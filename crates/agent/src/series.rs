//! Port of `tsdb/agent/series.go`: the in-memory series of the agent (no chunks) and their striped maps.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use model::exemplar::Exemplar;
use model::labels::{self, Labels};
use model::metadata::Handle;
use tsdb_format::chunks::HeadSeriesRef;

use crate::checkpoint::{ActiveSeries, DeletedSeries};

/// A hasher for keys that are already hashes or series references: one multiply (Fibonacci hashing) spreads
/// sequential references over all bits.
#[derive(Default, Clone, Copy)]
pub struct U64Hasher(u64);

impl Hasher for U64Hasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(0x100000001b3);
        }
    }
    fn write_u64(&mut self, i: u64) {
        self.0 = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}

/// A map keyed by a hash or a series reference.
pub type U64Map<V> = HashMap<u64, V, BuildHasherDefault<U64Hasher>>;

/// The mutable part of a series (Go: the fields guarded by `memSeries.Mutex`).
#[derive(Debug)]
pub struct SeriesState {
    /// Last recorded timestamp. Used by `StripeSeries::gc` to determine if a series is stale.
    pub last_ts: i64,
    /// The metadata of the series, or the zero handle.
    pub meta: Handle,
}

/// Go `memSeries`: a chunkless version of the head's series.
#[derive(Debug)]
pub struct MemSeries {
    pub r#ref: HeadSeriesRef,
    pub lset: Labels,
    state: Mutex<SeriesState>,
}

impl MemSeries {
    /// A series with the given reference, labels and last timestamp, and no metadata.
    pub fn new(r#ref: HeadSeriesRef, lset: Labels, last_ts: i64) -> Arc<MemSeries> {
        Arc::new(MemSeries {
            r#ref,
            lset,
            state: Mutex::new(SeriesState {
                last_ts,
                meta: Handle::default(),
            }),
        })
    }

    /// Go `memSeries.Lock`.
    pub fn lock(&self) -> MutexGuard<'_, SeriesState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Go `updateTimestamp`: sets the last timestamp unless `new_ts` is older; false if it is older.
    pub fn update_timestamp(&self, new_ts: i64) -> bool {
        let mut s = self.lock();
        if new_ts >= s.last_ts {
            s.last_ts = new_ts;
            return true;
        }
        false
    }

    pub fn r#ref(&self) -> HeadSeriesRef {
        self.r#ref
    }

    pub fn labels(&self) -> &Labels {
        &self.lset
    }

    /// Go `LastSampleTimestamp`.
    pub fn last_sample_timestamp(&self) -> i64 {
        self.lock().last_ts
    }

    /// Go `Metadata`: the metadata handle of the series, or the zero handle if none is set.
    pub fn metadata(&self) -> Handle {
        self.lock().meta.clone()
    }
}

/// Go `seriesHashmap`: finds a series by its label set via a 64-bit hash. One map for the common case where
/// the hash is unique, and a second map for series with the same hash. Each series is in only one of the maps.
#[derive(Default)]
pub struct SeriesHashmap {
    unique: U64Map<Arc<MemSeries>>,
    conflicts: Option<U64Map<Vec<Arc<MemSeries>>>>,
}

impl SeriesHashmap {
    pub fn get(&self, hash: u64, lset: &Labels) -> Option<&Arc<MemSeries>> {
        if let Some(s) = self.unique.get(&hash)
            && labels::equal(&s.lset, lset)
        {
            return Some(s);
        }
        self.conflicts.as_ref()?.get(&hash)?.iter().find(|s| labels::equal(&s.lset, lset))
    }

    pub fn set(&mut self, hash: u64, s: Arc<MemSeries>) {
        match self.unique.get(&hash) {
            Some(existing) if !labels::equal(&existing.lset, &s.lset) => {}
            _ => {
                self.unique.insert(hash, s);
                return;
            }
        }
        let series_set = self.conflicts.get_or_insert_with(Default::default).entry(hash).or_default();
        for prev in series_set.iter_mut() {
            if labels::equal(&prev.lset, &s.lset) {
                *prev = s;
                return;
            }
        }
        series_set.push(s);
    }

    pub fn delete(&mut self, hash: u64, r#ref: HeadSeriesRef) {
        let rem: Vec<Arc<MemSeries>>;
        let Some(unique) = self.unique.get(&hash) else {
            return; // Supplied hash is not stored.
        };
        if unique.r#ref == r#ref {
            let conflicts = self.conflicts.as_mut().and_then(|c| c.get_mut(&hash));
            match conflicts {
                Some(c) if !c.is_empty() => {
                    // First remaining series goes in 'unique'; keep the rest.
                    let first = c.remove(0);
                    self.unique.insert(hash, first);
                    rem = std::mem::take(c);
                }
                _ => {
                    // Exactly one series with this hash was stored.
                    self.unique.remove(&hash);
                    return;
                }
            }
        } else {
            // The series to delete is somewhere in 'conflicts'. Keep all the ones that don't match.
            rem = match self.conflicts.as_mut().and_then(|c| c.get_mut(&hash)) {
                Some(c) => std::mem::take(c).into_iter().filter(|s| s.r#ref != r#ref).collect(),
                None => Vec::new(),
            };
        }
        if let Some(c) = self.conflicts.as_mut() {
            if rem.is_empty() {
                c.remove(&hash);
            } else {
                c.insert(hash, rem);
            }
        }
    }
}

/// One stripe of [`StripeSeries`]: the series of the references and hashes that map to it.
#[derive(Default)]
pub struct Stripe {
    /// Series by reference (Go `series[i]`).
    pub series: U64Map<Arc<MemSeries>>,
    /// Series by label hash (Go `hashes[i]`).
    pub hashes: SeriesHashmap,
    /// The latest exemplar of each series by reference (Go `exemplars[i]`).
    pub exemplars: U64Map<Exemplar>,
}

/// Go `stripeSeries`: locks modulo ranges of references and hashes to reduce lock contention.
pub struct StripeSeries {
    pub size: usize,
    stripes: Vec<RwLock<Stripe>>,
    gc_mut: Mutex<()>,
}

/// The labels and metadata of a deleted series (Go `deletedSeries`).
#[derive(Clone, Debug, Default)]
pub struct DeletedSeriesEntry {
    pub r#ref: HeadSeriesRef,
    pub labels: Labels,
    pub meta: Handle,
}

impl DeletedSeries for DeletedSeriesEntry {
    fn r#ref(&self) -> HeadSeriesRef {
        self.r#ref
    }
    fn labels(&self) -> &Labels {
        &self.labels
    }
    fn metadata(&self) -> Option<&Handle> {
        Some(&self.meta)
    }
}

impl StripeSeries {
    /// Go `newStripeSeries`. `stripe_size` must be a power of two.
    pub fn new(stripe_size: usize) -> StripeSeries {
        StripeSeries {
            size: stripe_size,
            stripes: (0..stripe_size).map(|_| RwLock::new(Stripe::default())).collect(),
            gc_mut: Mutex::new(()),
        }
    }

    /// Read access to stripe `i` (Go `locks[i].RLock()` and the maps of stripe `i`).
    pub fn read(&self, i: usize) -> RwLockReadGuard<'_, Stripe> {
        self.stripes[i].read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self, i: usize) -> RwLockWriteGuard<'_, Stripe> {
        self.stripes[i].write().unwrap_or_else(|e| e.into_inner())
    }

    /// Go `GC`: removes the series that have not received a sample at or after `mint`. Returns the deleted
    /// series by reference; their labels and metadata are kept only when `retain_series` is true (they are
    /// used by the in-memory checkpoint).
    pub fn gc(&self, mint: i64, retain_series: bool) -> U64Map<DeletedSeriesEntry> {
        // gc_mut serializes GC calls. Within a single GC pass, the check holds the hash lock and then acquires
        // the ref lock; callers must never hold both at once, which set_unless_already_set satisfies.
        let _g = self.gc_mut.lock().unwrap_or_else(|e| e.into_inner());

        let mut deleted: U64Map<DeletedSeriesEntry> = U64Map::default();
        let mut candidates: Vec<(u64, Arc<MemSeries>)> = Vec::new();
        for hash_lock in 0..self.size {
            let mut hs = self.write(hash_lock);
            candidates.clear();
            if let Some(c) = &hs.hashes.conflicts {
                for (h, all) in c {
                    candidates.extend(all.iter().map(|s| (*h, s.clone())));
                }
            }
            candidates.extend(hs.hashes.unique.iter().map(|(h, s)| (*h, s.clone())));

            for (hash, series) in &candidates {
                let st = series.lock();
                // Any series that has received a write since mint is still alive.
                if st.last_ts >= mint {
                    continue;
                }
                let entry = if retain_series {
                    DeletedSeriesEntry {
                        r#ref: series.r#ref,
                        labels: series.lset.clone(),
                        meta: st.meta.clone(),
                    }
                } else {
                    DeletedSeriesEntry {
                        r#ref: series.r#ref,
                        ..Default::default()
                    }
                };
                deleted.insert(series.r#ref, entry);

                // The series is stale. We need a second lock for the ref if it differs from the hash lock.
                let ref_lock = self.ref_lock(series.r#ref);
                if ref_lock != hash_lock {
                    let mut rs = self.write(ref_lock);
                    rs.series.remove(&series.r#ref);
                    // Since the series is gone, also delete its latest stored exemplar.
                    rs.exemplars.remove(&series.r#ref);
                } else {
                    hs.series.remove(&series.r#ref);
                    hs.exemplars.remove(&series.r#ref);
                }
                hs.hashes.delete(*hash, series.r#ref);
                drop(st);
            }
        }
        deleted
    }

    /// Go `GetByID`.
    pub fn get_by_id(&self, id: HeadSeriesRef) -> Option<Arc<MemSeries>> {
        self.read(self.ref_lock(id)).series.get(&id).cloned()
    }

    /// Go `GetByHash`.
    pub fn get_by_hash(&self, hash: u64, lset: &Labels) -> Option<Arc<MemSeries>> {
        self.read(self.hash_lock(hash)).hashes.get(hash, lset).cloned()
    }

    /// Go `SetUnlessAlreadySet`: inserts `series` unless a series with the same label set exists. Returns the
    /// canonical series and whether it was newly inserted.
    ///
    /// Insertion order is refs before hashes. GC only discovers series via hashes, so anything it finds is
    /// already present in refs. The hash lock and the ref lock are never held at the same time.
    pub fn set_unless_already_set(&self, hash: u64, series: Arc<MemSeries>) -> (Arc<MemSeries>, bool) {
        let hash_lock = self.hash_lock(hash);

        // Fast path: series already exists.
        if let Some(prev) = self.write(hash_lock).hashes.get(hash, &series.lset) {
            return (prev.clone(), false);
        }

        // Insert into refs first. A series that is only in refs is invisible to GC.
        let ref_lock = self.ref_lock(series.r#ref);
        self.write(ref_lock).series.insert(series.r#ref, series.clone());

        // Re-acquire the hash lock. A concurrent caller may have inserted the same label set meanwhile.
        let mut hs = self.write(hash_lock);
        if let Some(prev) = hs.hashes.get(hash, &series.lset) {
            let prev = prev.clone();
            drop(hs);
            // We lost the race: clean up the ref we pre-inserted.
            self.write(ref_lock).series.remove(&series.r#ref);
            return (prev, false);
        }
        hs.hashes.set(hash, series.clone());
        drop(hs);
        (series, true)
    }

    /// Go `GetLatestExemplar`, as a comparison: whether the latest exemplar of `r#ref` equals `e`.
    pub fn latest_exemplar_equals(&self, r#ref: HeadSeriesRef, e: &Exemplar) -> bool {
        self.read(self.ref_lock(r#ref)).exemplars.get(&r#ref).is_some_and(|p| p.equals(e))
    }

    /// Go `GetLatestExemplar`.
    pub fn get_latest_exemplar(&self, r#ref: HeadSeriesRef) -> Option<Exemplar> {
        self.read(self.ref_lock(r#ref)).exemplars.get(&r#ref).cloned()
    }

    /// Go `SetLatestExemplar`: records the latest exemplar of an existing series.
    pub fn set_latest_exemplar(&self, r#ref: HeadSeriesRef, e: &Exemplar) {
        let mut s = self.write(self.ref_lock(r#ref));
        if s.series.contains_key(&r#ref) {
            s.exemplars.insert(r#ref, e.clone());
        }
    }

    pub fn hash_lock(&self, hash: u64) -> usize {
        (hash & (self.size as u64 - 1)) as usize
    }

    pub fn ref_lock(&self, r#ref: HeadSeriesRef) -> usize {
        (r#ref & (self.size as u64 - 1)) as usize
    }

    /// All series by reference, collected stripe by stripe (Go: ranging over `series[i]` under each lock).
    pub fn all_by_ref(&self) -> U64Map<Arc<MemSeries>> {
        let mut out = U64Map::default();
        for i in 0..self.size {
            for (r, s) in &self.read(i).series {
                out.insert(*r, s.clone());
            }
        }
        out
    }

    /// Go `allSeries`: a point-in-time snapshot of each series. Each stripe's series are collected under its
    /// read lock and snapshotted outside of it, so that slow consumers (checkpoint disk I/O) do not block
    /// appends.
    pub fn all_series(&self) -> AllSeries<'_> {
        AllSeries {
            s: self,
            stripe: 0,
            buf: Vec::new(),
            pos: 0,
        }
    }
}

/// The iterator of [`StripeSeries::all_series`].
pub struct AllSeries<'a> {
    s: &'a StripeSeries,
    stripe: usize,
    buf: Vec<Arc<MemSeries>>,
    pos: usize,
}

impl Iterator for AllSeries<'_> {
    type Item = SeriesSnapshot;

    fn next(&mut self) -> Option<SeriesSnapshot> {
        while self.pos >= self.buf.len() {
            if self.stripe >= self.s.size {
                return None;
            }
            self.buf.clear();
            self.pos = 0;
            self.buf.extend(self.s.read(self.stripe).series.values().cloned());
            self.stripe += 1;
        }
        let series = &self.buf[self.pos];
        self.pos += 1;
        let st = series.lock();
        Some(SeriesSnapshot {
            r#ref: series.r#ref,
            lset: series.lset.clone(),
            last_ts: st.last_ts,
            meta: st.meta.clone(),
        })
    }
}

/// Go `seriesSnapshot`: a point-in-time copy of a series' fields, used to avoid holding series locks during
/// checkpoint I/O.
#[derive(Clone, Debug)]
pub struct SeriesSnapshot {
    pub r#ref: HeadSeriesRef,
    pub lset: Labels,
    pub last_ts: i64,
    pub meta: Handle,
}

impl ActiveSeries for SeriesSnapshot {
    fn r#ref(&self) -> HeadSeriesRef {
        self.r#ref
    }
    fn labels(&self) -> &Labels {
        &self.lset
    }
    fn last_sample_timestamp(&self) -> i64 {
        self.last_ts
    }
    fn metadata(&self) -> Option<&Handle> {
        Some(&self.meta)
    }
}

/// Go `deletedSeriesIter`: the deleted series of `m` whose last segment is greater than `last`, matching the
/// filtering of the `wlog::checkpoint` keep function.
pub fn deleted_series_iter<'a, S: std::hash::BuildHasher>(
    m: &'a HashMap<HeadSeriesRef, crate::db::DeletedRefMeta, S>,
    last: i64,
) -> impl Iterator<Item = DeletedSeriesEntry> + 'a {
    m.iter().filter(move |(_, meta)| meta.last_segment > last).map(|(r, meta)| DeletedSeriesEntry {
        r#ref: *r,
        labels: meta.labels.clone(),
        meta: meta.meta.clone(),
    })
}
