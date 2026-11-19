//! The scrape cache (Go `scrapeCache`): series text to label set and storage reference, dropped series,
//! staleness tracking between scrapes, and the metadata of metric families.
//!
//! Entries live in a slab and are addressed by index, so the series map and the staleness maps share them
//! without reference counting. Lookups take the series bytes from the parser; a hit allocates nothing.

use std::sync::{Arc, Mutex, MutexGuard};

use model::labels::Labels;
use model::metadata::{Metadata, MetricType};
use storage::SeriesRef;

use crate::metrics::ScrapeMetrics;
use crate::st_synthesis::StCache;
use crate::target::{MetricMetadata, MetricMetadataStore};
use crate::util::FxHashMap;

/// One cached series (Go `cacheEntry`).
#[derive(Default)]
pub struct CacheEntry {
    pub ref_: SeriesRef,
    pub last_iter: u64,
    pub lset: Labels,
    /// Optional state for start timestamp synthesis.
    pub st: Option<Box<StCache>>,
}

/// The series part of the cache; the scrape loop owns it while it appends.
#[derive(Default)]
pub struct SeriesCache {
    /// Current scrape iteration.
    pub iter: u64,
    /// How many series and metadata entries there were at the last success.
    pub successful_count: usize,
    entries: Vec<CacheEntry>,
    free: Vec<u32>,
    series: FxHashMap<Box<[u8]>, u32>,
    dropped_series: FxHashMap<Box<[u8]>, u64>,
    series_cur: FxHashMap<SeriesRef, u32>,
    series_prev: FxHashMap<SeriesRef, u32>,
}

/// The index of a cache entry.
pub type EntryId = u32;

impl SeriesCache {
    /// The entry for an id.
    #[inline]
    pub fn entry(&self, id: EntryId) -> &CacheEntry {
        &self.entries[id as usize]
    }
    #[inline]
    pub fn entry_mut(&mut self, id: EntryId) -> &mut CacheEntry {
        &mut self.entries[id as usize]
    }

    /// Go `get`: the entry for the series text, whether it was found, and whether it was already seen in this
    /// scrape. Marks the entry as seen.
    #[inline]
    pub fn get(&mut self, met: &[u8]) -> (Option<EntryId>, bool) {
        match self.series.get(met) {
            None => (None, false),
            Some(&id) => {
                let iter = self.iter;
                let e = &mut self.entries[id as usize];
                let already = e.last_iter == iter;
                e.last_iter = iter;
                (Some(id), already)
            }
        }
    }

    /// Go `addRef`.
    pub fn add_ref(&mut self, met: &[u8], r: SeriesRef, lset: Labels) -> EntryId {
        let e = CacheEntry { ref_: r, last_iter: self.iter, lset, st: None };
        let id = match self.free.pop() {
            Some(id) => {
                self.entries[id as usize] = e;
                id
            }
            None => {
                self.entries.push(e);
                (self.entries.len() - 1) as u32
            }
        };
        if let Some(old) = self.series.insert(met.into(), id) {
            self.release(old);
        }
        id
    }

    fn release(&mut self, id: EntryId) {
        // An entry may still be tracked for staleness of the previous scrape; keep it alive until then.
        if self.series_prev.values().any(|&x| x == id) || self.series_cur.values().any(|&x| x == id) {
            return;
        }
        self.entries[id as usize] = CacheEntry::default();
        self.free.push(id);
    }

    /// Go `addDropped`.
    pub fn add_dropped(&mut self, met: &[u8]) {
        let iter = self.iter;
        self.dropped_series.insert(met.into(), iter);
    }

    /// Go `getDropped`.
    #[inline]
    pub fn get_dropped(&mut self, met: &[u8]) -> bool {
        if self.dropped_series.is_empty() {
            return false;
        }
        match self.dropped_series.get_mut(met) {
            Some(i) => {
                *i = self.iter;
                true
            }
            None => false,
        }
    }

    /// Go `updateRef`: points the entry at a new reference and moves its staleness tracking along.
    pub fn update_ref(&mut self, id: EntryId, r: SeriesRef) {
        let old = self.entries[id as usize].ref_;
        if old == r {
            return;
        }
        if old != 0 {
            move_staleness(&mut self.series_prev, id, old, r);
            move_staleness(&mut self.series_cur, id, old, r);
        }
        self.entries[id as usize].ref_ = r;
    }

    /// Go `trackStaleness`.
    #[inline]
    pub fn track_staleness(&mut self, r: SeriesRef, id: EntryId) {
        self.series_cur.insert(r, id);
    }

    /// Go `forEachStale`: calls `f` with the reference and labels of every series of the previous scrape that is
    /// not in the current one, until `f` returns false.
    pub fn for_each_stale(&self, mut f: impl FnMut(SeriesRef, &Labels) -> bool) {
        for (r, &id) in self.series_prev.iter() {
            if !self.series_cur.contains_key(r) {
                let e = &self.entries[id as usize];
                if !f(e.ref_, &e.lset) {
                    break;
                }
            }
        }
    }

    /// The number of cached series (Go `len(c.series)`).
    pub fn series_len(&self) -> usize {
        self.series.len()
    }
    /// The number of dropped series (Go `len(c.droppedSeries)`).
    pub fn dropped_len(&self) -> usize {
        self.dropped_series.len()
    }
    /// Whether the series text is cached.
    pub fn has_series(&self, met: &[u8]) -> bool {
        self.series.contains_key(met)
    }
    /// The entry of a series text without touching it.
    pub fn peek(&self, met: &[u8]) -> Option<&CacheEntry> {
        self.series.get(met).map(|&id| &self.entries[id as usize])
    }
    /// The references tracked in the current scrape (Go `c.seriesCur` keys).
    pub fn series_cur_refs(&self) -> Vec<SeriesRef> {
        self.series_cur.keys().copied().collect()
    }
    /// The references tracked in the previous scrape (Go `c.seriesPrev` keys).
    pub fn series_prev_refs(&self) -> Vec<SeriesRef> {
        self.series_prev.keys().copied().collect()
    }
    /// The entry tracked under a reference in the current scrape.
    pub fn series_cur_entry(&self, r: SeriesRef) -> Option<&CacheEntry> {
        self.series_cur.get(&r).map(|&id| &self.entries[id as usize])
    }
    /// The entry tracked under a reference in the previous scrape.
    pub fn series_prev_entry(&self, r: SeriesRef) -> Option<&CacheEntry> {
        self.series_prev.get(&r).map(|&id| &self.entries[id as usize])
    }
    /// All cached series texts and their entries.
    pub fn series_iter(&self) -> impl Iterator<Item = (&[u8], &CacheEntry)> {
        self.series.iter().map(|(k, &id)| (&**k, &self.entries[id as usize]))
    }
}

fn move_staleness(tracked: &mut FxHashMap<SeriesRef, u32>, id: EntryId, old: SeriesRef, new: SeriesRef) {
    if tracked.get(&old) != Some(&id) {
        return;
    }
    tracked.remove(&old);
    tracked.insert(new, id);
}

/// Metadata of a metric family with its scrape iterations (Go `metaEntry`).
#[derive(Clone, Debug, Default)]
pub struct MetaEntry {
    pub metadata: Metadata,
    /// Last scrape iteration the entry was observed at.
    pub last_iter: u64,
    /// Last scrape iteration the entry was changed at.
    pub last_iter_change: u64,
}

impl MetaEntry {
    fn size(&self) -> usize {
        self.metadata.help.len() + self.metadata.unit.len() + self.metadata.typ.as_str().len()
    }
}

/// The metadata part of the cache; the API reads it while scrapes run.
#[derive(Default)]
pub struct MetaCache {
    pub metadata: FxHashMap<Box<[u8]>, MetaEntry>,
    /// The total size of all entries.
    pub metadata_size: usize,
}

/// The scrape cache of one target (Go `scrapeCache`).
pub struct ScrapeCache {
    series: Mutex<SeriesCache>,
    meta: Mutex<MetaCache>,
    pub metrics: Arc<ScrapeMetrics>,
}

fn utf8(b: &[u8]) -> &str {
    std::str::from_utf8(b).unwrap_or("")
}

impl ScrapeCache {
    /// Go `newScrapeCache`.
    pub fn new(metrics: Arc<ScrapeMetrics>) -> Arc<ScrapeCache> {
        Arc::new(ScrapeCache { series: Mutex::new(SeriesCache::default()), meta: Mutex::new(MetaCache::default()), metrics })
    }

    /// Locks the series part.
    pub fn series(&self) -> MutexGuard<'_, SeriesCache> {
        self.series.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Locks the metadata part.
    pub fn meta(&self) -> MutexGuard<'_, MetaCache> {
        self.meta.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Go `iterDone`.
    pub fn iter_done(&self, s: &mut SeriesCache, mut flush_cache: bool) {
        let count = s.series.len() + s.dropped_series.len() + self.meta().metadata.len();
        if flush_cache {
            s.successful_count = count;
        } else if count > s.successful_count * 2 + 1000 {
            // A target with varying labels in failed scrapes would grow the caches forever.
            flush_cache = true;
            self.metrics.target_scrape_cache_flush_forced.get().inc();
        }
        if flush_cache {
            let iter = s.iter;
            let mut freed = Vec::new();
            s.series.retain(|_, id| {
                let keep = s.entries[*id as usize].last_iter == iter;
                if !keep {
                    freed.push(*id);
                }
                keep
            });
            s.dropped_series.retain(|_, i| *i == iter);
            // Entries still tracked as current stay alive until the swap below drops them from tracking.
            let mut m = self.meta();
            let mut removed = 0;
            m.metadata.retain(|_, e| {
                let keep = iter.wrapping_sub(e.last_iter) <= 10;
                if !keep {
                    removed += e.size();
                }
                keep
            });
            m.metadata_size -= removed;
            drop(m);
            let tracked: std::collections::HashSet<u32, crate::util::FxBuild> =
                if freed.is_empty() { Default::default() } else { s.series_cur.values().copied().collect() };
            for id in freed {
                if tracked.contains(&id) {
                    continue;
                }
                s.entries[id as usize] = CacheEntry::default();
                s.free.push(id);
            }
        }
        std::mem::swap(&mut s.series_prev, &mut s.series_cur);
        s.series_cur.clear();
        s.iter += 1;
    }

    fn set_meta(&self, iter: u64, mf: &[u8], f: impl FnOnce(&mut MetaEntry, u64)) -> (MetricType, u64) {
        let mut m = self.meta();
        let m = &mut *m;
        let (e, old_size) = match m.metadata.get_mut(mf) {
            Some(e) => {
                let s = e.size();
                (e, s)
            }
            None => {
                let e = MetaEntry { metadata: Metadata { typ: MetricType::Unknown, ..Default::default() }, ..Default::default() };
                m.metadata.insert(mf.into(), e);
                (m.metadata.get_mut(mf).unwrap(), 0)
            }
        };
        f(e, iter);
        e.last_iter = iter;
        let new_size = e.size();
        let r = (e.metadata.typ, e.last_iter_change);
        m.metadata_size = m.metadata_size + new_size - old_size;
        r
    }

    /// Go `setType`: returns the entry's type and last change iteration.
    pub fn set_type(&self, iter: u64, mf: &[u8], t: MetricType) -> (MetricType, u64) {
        self.set_meta(iter, mf, |e, iter| {
            if e.metadata.typ != t {
                e.metadata.typ = t;
                e.last_iter_change = iter;
            }
        })
    }

    /// Go `setHelp`.
    pub fn set_help(&self, iter: u64, mf: &[u8], help: &[u8]) -> (MetricType, u64) {
        self.set_meta(iter, mf, |e, iter| {
            if e.metadata.help.as_bytes() != help {
                e.metadata.help.clear();
                e.metadata.help.push_str(&String::from_utf8_lossy(help));
                e.last_iter_change = iter;
            }
        })
    }

    /// Go `setUnit`.
    pub fn set_unit(&self, iter: u64, mf: &[u8], unit: &[u8]) -> (MetricType, u64) {
        self.set_meta(iter, mf, |e, iter| {
            if e.metadata.unit.as_bytes() != unit {
                e.metadata.unit.clear();
                e.metadata.unit.push_str(&String::from_utf8_lossy(unit));
                e.last_iter_change = iter;
            }
        })
    }

    /// Copies the metadata of a family into `out` (reusing its strings); false when absent.
    pub fn copy_metadata(&self, mf: &[u8], out: &mut Metadata) -> bool {
        let m = self.meta();
        match m.metadata.get(mf) {
            Some(e) => {
                out.typ = e.metadata.typ;
                out.help.clone_from(&e.metadata.help);
                out.unit.clone_from(&e.metadata.unit);
                true
            }
            None => false,
        }
    }

    /// The metadata entry of a family (Go `c.metadata[name]`).
    pub fn meta_entry(&self, mf: &str) -> Option<MetaEntry> {
        self.meta().metadata.get(mf.as_bytes()).cloned()
    }
}

impl MetricMetadataStore for ScrapeCache {
    fn get_metadata(&self, mf_name: &str) -> Option<MetricMetadata> {
        let m = self.meta();
        m.metadata.get(mf_name.as_bytes()).map(|e| MetricMetadata {
            metric_family: mf_name.to_string(),
            typ: e.metadata.typ,
            help: e.metadata.help.clone(),
            unit: e.metadata.unit.clone(),
        })
    }

    fn list_metadata(&self) -> Vec<MetricMetadata> {
        let m = self.meta();
        m.metadata
            .iter()
            .map(|(k, e)| MetricMetadata {
                metric_family: utf8(k).to_string(),
                typ: e.metadata.typ,
                help: e.metadata.help.clone(),
                unit: e.metadata.unit.clone(),
            })
            .collect()
    }

    fn size_metadata(&self) -> usize {
        self.meta().metadata_size
    }

    fn length_metadata(&self) -> usize {
        self.meta().metadata.len()
    }
}
