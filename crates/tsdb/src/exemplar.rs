//! Circular in-memory exemplar storage (port of `tsdb/exemplar.go`).
//!
//! Layout: as in Go, the storage is a circular buffer of `length` slots; each stored exemplar is a node in a
//! doubly linked list per series, ordered by timestamp, and an index entry per series points to the oldest and
//! newest node. Slot numbers, `next_index`, eviction order, `Select` results and metrics are the same as Go's.
//!
//! Memory: Go allocates all `length` slots up front. Here only the slots that were written are materialized.
//! Empty slots always form one circular run (writes go to `next_index` and move forward), so the materialized
//! slots are kept as a rotation of the logical buffer: logical slot `i` lives at physical position
//! `(i - base) mod length`, and physical positions at or after `slots.len()` are empty. Resizing keeps Go's
//! logical slot numbers and moves a leading run of empty slots into `base` instead of materializing it.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use model::exemplar::{Exemplar, QueryResult, EXEMPLAR_MAX_LABEL_SET_LENGTH};
use model::labels::{self, Labels};
use model::matcher::Matcher;
use storage::{
    Context, Error, ExemplarQuerier, Sentinel, ERR_DUPLICATE_EXEMPLAR, ERR_EXEMPLARS_DISABLED,
    ERR_EXEMPLAR_LABEL_LENGTH, ERR_OUT_OF_ORDER_EXEMPLAR,
};

use crate::metrics::{Counter, Gauge};

static INVALID_EXEMPLAR: Sentinel = Sentinel::new("invalid exemplar");
/// Go `tsdb.ErrInvalidExemplar` (head.go): an appended exemplar is not valid and can't be ingested.
pub const ERR_INVALID_EXEMPLAR: Error = Error::Sentinel(&INVALID_EXEMPLAR);

/// Indicates that there is no index entry for an exemplar (Go `noExemplar`).
const NO_EXEMPLAR: i32 = -1;
/// No index entry (Go `nil` `*indexEntry`).
const NO_REF: u32 = u32::MAX;
/// Estimated number of exemplars per series, for sizing the index (Go `estimatedExemplarsPerSeries`).
const ESTIMATED_EXEMPLARS_PER_SERIES: usize = 16;
/// Upper bound of the index capacity reserved up front; the index grows with demand beyond it.
const MAX_INITIAL_INDEX_CAPACITY: usize = 1024;

/// Go `ExemplarMetrics`.
#[derive(Debug, Default)]
pub struct ExemplarMetrics {
    /// `prometheus_tsdb_exemplar_exemplars_appended_total`.
    pub exemplars_appended: Counter,
    /// `prometheus_tsdb_exemplar_exemplars_in_storage`.
    pub exemplars_in_storage: Gauge,
    /// `prometheus_tsdb_exemplar_series_with_exemplars_in_storage`.
    pub series_with_exemplars_in_storage: Gauge,
    /// `prometheus_tsdb_exemplar_last_exemplars_timestamp_seconds`.
    pub last_exemplars_ts: Gauge,
    /// `prometheus_tsdb_exemplar_max_exemplars`.
    pub max_exemplars: Gauge,
    /// `prometheus_tsdb_exemplar_out_of_order_exemplars_total`.
    pub out_of_order_exemplars: Counter,
}

impl ExemplarMetrics {
    /// Go `NewExemplarMetrics`.
    pub fn new() -> Arc<ExemplarMetrics> {
        Arc::new(ExemplarMetrics::default())
    }
}

/// Go `indexEntry`: the oldest and newest slot of a series.
#[derive(Clone, Debug)]
struct IndexEntry {
    oldest: i32,
    newest: i32,
    series_labels: Labels,
}

/// Go `circularBufferEntry`. `ref_` is the slot of the series in `Inner::entries` (`NO_REF` when empty).
#[derive(Clone, Debug)]
struct Entry {
    exemplar: Exemplar,
    next: i32,
    prev: i32,
    ref_: u32,
}

impl Entry {
    #[inline]
    fn empty() -> Entry {
        Entry { exemplar: Exemplar::default(), next: 0, prev: 0, ref_: NO_REF }
    }
}

/// Go `intRange`.
#[derive(Clone, Copy)]
struct IntRange {
    from: i32,
    to: i32,
}

impl IntRange {
    #[inline]
    fn contains(&self, i: i32) -> bool {
        i >= self.from && i < self.to
    }
}

struct Inner {
    /// Logical length of the buffer (Go `len(ce.exemplars)`).
    length: usize,
    /// Materialized slots, rotated by `base` (see the module docs).
    slots: Vec<Entry>,
    base: usize,
    next_index: i32,
    ooo_time_window_millis: i64,
    /// Index entries; `free` lists unused positions.
    entries: Vec<IndexEntry>,
    free: Vec<u32>,
    /// Series labels to position in `entries` (Go `index`, keyed by the label bytes).
    index: HashMap<Labels, u32>,
}

impl Inner {
    #[inline]
    fn phys(&self, i: i32) -> usize {
        let i = i as usize;
        if i >= self.base { i - self.base } else { i + self.length - self.base }
    }

    /// The slot `i`, or `None` when it was never written (an empty slot).
    #[inline]
    fn slot(&self, i: i32) -> Option<&Entry> {
        self.slots.get(self.phys(i))
    }

    /// The slot `i`, materializing it (and any gap before it) as empty slots.
    #[inline]
    fn slot_mut(&mut self, i: i32) -> &mut Entry {
        let p = self.phys(i);
        if p >= self.slots.len() {
            self.slots.resize(p + 1, Entry::empty());
        }
        &mut self.slots[p]
    }

    /// The reference of slot `i` (`NO_REF` when empty).
    #[inline]
    fn ref_at(&self, i: i32) -> u32 {
        self.slot(i).map_or(NO_REF, |e| e.ref_)
    }

    /// The timestamp of the exemplar in slot `i` (a stored slot).
    #[inline]
    fn ts_at(&self, i: i32) -> i64 {
        self.slot(i).map_or(0, |e| e.exemplar.ts)
    }

    fn lookup(&self, l: &Labels) -> u32 {
        self.index.get(l).copied().unwrap_or(NO_REF)
    }

    /// Go `validateExemplar`. Not thread safe. `appended` tells whether this is an internal validation of an
    /// `add_exemplar` call, which updates metrics.
    fn validate_exemplar(&self, idx: u32, e: &Exemplar, appended: bool, metrics: &ExemplarMetrics) -> Result<(), Error> {
        if self.length == 0 {
            return Err(ERR_EXEMPLARS_DISABLED);
        }

        if let Some(l) = e.labels.has_duplicate_label_names() {
            return Err(Error::wrapped(
                format!("label name {} is not unique: {}", model::unicode::quote(l), ERR_INVALID_EXEMPLAR),
                ERR_INVALID_EXEMPLAR,
            ));
        }

        // Exemplar label length does not include chars involved in text rendering such as quotes, equals sign,
        // or commas. See the definition of EXEMPLAR_MAX_LABEL_SET_LENGTH.
        let mut label_set_len = 0usize;
        e.labels.validate(|n, v| {
            label_set_len += n.chars().count();
            label_set_len += v.chars().count();
            if label_set_len > EXEMPLAR_MAX_LABEL_SET_LENGTH {
                return Err(ERR_EXEMPLAR_LABEL_LENGTH);
            }
            Ok(())
        })?;

        if idx == NO_REF {
            return Ok(());
        }

        // Check for duplicate vs last stored exemplar for this series. These are expected, and appending them
        // is a no-op. For floats and classic histograms there is only one exemplar per series, so this is
        // sufficient. For native histograms with multiple exemplars per series there is another check below.
        let newest = &self.slot(self.entries[idx as usize].newest).expect("newest exemplar").exemplar;
        if newest.equals(e) {
            return Err(ERR_DUPLICATE_EXEMPLAR);
        }

        // Reject exemplars older than the OOO time window relative to the newest exemplar. Exemplars with the
        // same timestamp are ordered by value then label hash to detect duplicates without iterating through all
        // stored exemplars. Exemplars with equal timestamps but different values or labels are allowed to support
        // multiple buckets of native histograms.
        if (e.ts < newest.ts && e.ts <= newest.ts.wrapping_sub(self.ooo_time_window_millis))
            || (e.ts == newest.ts && e.value < newest.value)
            || (e.ts == newest.ts && e.value == newest.value && e.labels.hash() < newest.labels.hash())
        {
            if appended {
                metrics.out_of_order_exemplars.inc();
            }
            return Err(ERR_OUT_OF_ORDER_EXEMPLAR);
        }
        Ok(())
    }

    /// Go `removeExemplar`: unlinks slot `i`. Returns true iff it was the last entry of its series.
    fn remove_exemplar(&mut self, i: i32) -> bool {
        let (r, prev, next) = match self.slot(i) {
            Some(e) if e.ref_ != NO_REF => (e.ref_, e.prev, e.next),
            _ => return false,
        };
        if prev != NO_EXEMPLAR {
            self.slot_mut(prev).next = next;
        } else {
            self.entries[r as usize].oldest = next;
        }
        if next != NO_EXEMPLAR {
            self.slot_mut(next).prev = prev;
        } else {
            self.entries[r as usize].newest = prev;
        }
        // Mark this item as deleted.
        self.slot_mut(i).ref_ = NO_REF;
        let ie = &self.entries[r as usize];
        ie.oldest == NO_EXEMPLAR && ie.newest == NO_EXEMPLAR
    }

    /// Go `removeIndex`.
    fn remove_index(&mut self, r: u32) {
        let ls = std::mem::take(&mut self.entries[r as usize].series_labels);
        self.index.remove(&ls);
        self.free.push(r);
    }

    fn new_index(&mut self, l: &Labels) -> u32 {
        let ie = IndexEntry { oldest: 0, newest: 0, series_labels: l.clone() };
        let r = match self.free.pop() {
            Some(r) => {
                self.entries[r as usize] = ie;
                r
            }
            None => {
                self.entries.push(ie);
                (self.entries.len() - 1) as u32
            }
        };
        self.index.insert(l.clone(), r);
        r
    }

    /// Go `findInsertionIndex`: walks from the newest entry back to the first one not newer than `e`.
    fn find_insertion_index(&self, e: &Exemplar, r: u32) -> i32 {
        let ie = &self.entries[r as usize];
        let mut i = ie.newest;
        while i != NO_EXEMPLAR {
            let cur = self.slot(i).expect("linked exemplar");
            if cur.exemplar.ts <= e.ts {
                return i;
            }
            i = cur.prev;
        }
        ie.oldest
    }

    /// Go `computeMetrics`.
    fn compute_metrics(&self, m: &ExemplarMetrics) {
        m.series_with_exemplars_in_storage.set(self.index.len() as f64);

        if self.length == 0 {
            m.exemplars_in_storage.set(0.0);
            m.last_exemplars_ts.set(0.0);
            return;
        }

        if self.ref_at(self.next_index) != NO_REF {
            m.exemplars_in_storage.set(self.length as f64);
            m.last_exemplars_ts.set(self.ts_at(self.next_index) as f64 / 1000.0);
            return;
        }

        // We did not yet fill the buffer.
        m.exemplars_in_storage.set(self.next_index as f64);
        if self.ref_at(0) != NO_REF {
            m.last_exemplars_ts.set(self.ts_at(0) as f64 / 1000.0);
        }
    }

    /// Go `copyExemplarRanges` followed by the slice swap: builds a buffer of logical length `new_len` from the
    /// ranges of the current one, fixes the list pointers and the index, and returns the number of slots copied
    /// and the number of non-empty entries migrated. Empty slots before the first entry and after the last one
    /// are not materialized.
    fn copy_ranges(&mut self, new_len: usize, ranges: &[IntRange]) -> (usize, usize) {
        let mut old = std::mem::take(&mut self.slots);
        let mut offsets = [0i32; 2];
        let mut dest: Vec<Entry> = Vec::new();
        let mut lead: Option<usize> = None;
        let mut pending_empty = 0usize;
        let mut n = 0usize;
        let mut migrated = 0usize;
        for (ri, rng) in ranges.iter().enumerate() {
            offsets[ri] = n as i32 - rng.from;
            let count = ((rng.to - rng.from).max(0) as usize).min(new_len - n);
            for k in 0..count {
                let p = self.phys(rng.from + k as i32);
                let d = n + k;
                match old.get_mut(p) {
                    Some(e) if e.ref_ != NO_REF => {
                        migrated += 1;
                        if lead.is_none() {
                            lead = Some(d);
                        } else {
                            dest.extend(std::iter::repeat_n(Entry::empty(), pending_empty));
                        }
                        pending_empty = 0;
                        dest.push(std::mem::replace(e, Entry::empty()));
                    }
                    _ => {
                        if lead.is_some() {
                            pending_empty += 1;
                        }
                    }
                }
            }
            n += count;
        }
        drop(old);

        let adjust = |x: &mut i32| {
            for (i, rng) in ranges.iter().enumerate() {
                if rng.contains(*x) {
                    *x += offsets[i];
                    break;
                }
            }
        };
        for e in dest.iter_mut() {
            if e.ref_ == NO_REF {
                continue;
            }
            adjust(&mut e.prev);
            adjust(&mut e.next);
        }
        for &r in self.index.values() {
            let ie = &mut self.entries[r as usize];
            adjust(&mut ie.oldest);
            adjust(&mut ie.newest);
        }

        self.length = new_len;
        self.slots = dest;
        self.base = match lead {
            Some(d) => d,
            None if new_len > 0 => n % new_len,
            None => 0,
        };
        (n, migrated)
    }

    /// Go `grow`.
    fn grow(&mut self, l: usize) -> usize {
        let old_size = self.length as i32;
        let ranges = [IntRange { from: self.next_index, to: old_size }, IntRange { from: 0, to: self.next_index }];
        let (total, migrated) = self.copy_ranges(l, &ranges);
        self.next_index = total as i32;
        migrated
    }

    /// Go `shrink`: deletes the oldest slots (in order of ingestion) to fit `l`.
    fn shrink(&mut self, l: usize) -> usize {
        let old_size = self.length;
        let diff = old_size - l;
        let delete_start = self.next_index as usize;
        let delete_end = (delete_start + diff) % old_size;

        // Remove items from the buffer starting from next_index. This drops older entries first in the order of
        // ingestion.
        for i in 0..diff {
            let idx = ((delete_start + i) % old_size) as i32;
            let r = self.ref_at(idx);
            if r == NO_REF {
                continue;
            }
            if self.remove_exemplar(idx) {
                self.remove_index(r);
            }
        }

        let (total, migrated) = if delete_start == delete_end {
            // The entire buffer was cleared (shrink to zero). The index is already empty (remove_exemplar
            // removed every entry).
            self.slots = Vec::new();
            self.length = l;
            self.base = 0;
            self.next_index = 0;
            return 0;
        } else if delete_start < delete_end {
            // We delete an "inner" section of the circular buffer.
            self.copy_ranges(
                l,
                &[
                    IntRange { from: delete_end as i32, to: old_size as i32 },
                    IntRange { from: 0, to: delete_start as i32 },
                ],
            )
        } else {
            // We keep an "inner" section of the circular buffer.
            self.copy_ranges(l, &[IntRange { from: delete_end as i32, to: delete_start as i32 }])
        };

        self.next_index = (total % l) as i32;
        migrated
    }
}

/// Go `CircularExemplarStorage`. Thread-safe (interior lock). The buffer grows with demand up to its length
/// instead of being preallocated.
pub struct CircularExemplarStorage {
    inner: RwLock<Inner>,
    metrics: Arc<ExemplarMetrics>,
}

impl CircularExemplarStorage {
    /// Go `NewCircularExemplarStorage`: `length <= 0` gives a storage that keeps nothing until resized;
    /// `ooo_time_window_millis <= 0` disables out-of-order exemplars.
    pub fn new(
        length: i64,
        metrics: Arc<ExemplarMetrics>,
        ooo_time_window_millis: i64,
    ) -> Result<Arc<CircularExemplarStorage>, Error> {
        let length = length.max(0) as usize;
        let ooo = ooo_time_window_millis.max(0);
        let cap = (length / ESTIMATED_EXEMPLARS_PER_SERIES).min(MAX_INITIAL_INDEX_CAPACITY);
        let inner = Inner {
            length,
            slots: Vec::new(),
            base: 0,
            next_index: 0,
            ooo_time_window_millis: ooo,
            entries: Vec::new(),
            free: Vec::new(),
            index: HashMap::with_capacity(cap),
        };
        metrics.max_exemplars.set(length as f64);
        Ok(Arc::new(CircularExemplarStorage { inner: RwLock::new(inner), metrics }))
    }

    fn read(&self) -> RwLockReadGuard<'_, Inner> {
        self.inner.read().unwrap_or_else(|p| p.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, Inner> {
        self.inner.write().unwrap_or_else(|p| p.into_inner())
    }

    /// The metrics of this storage.
    pub fn metrics(&self) -> &Arc<ExemplarMetrics> {
        &self.metrics
    }

    /// Go `Select`: the exemplars in [start, end] of the series that match some matcher set, sorted by series
    /// labels.
    pub fn select(&self, start: i64, end: i64, matchers: &[Vec<Matcher>]) -> Result<Vec<QueryResult>, Error> {
        let mut ret = Vec::new();
        let ce = self.read();
        if ce.length == 0 {
            return Ok(ret);
        }

        // Loop through each index entry, which points to the first and last exemplar of each series.
        for &r in ce.index.values() {
            let idx = &ce.entries[r as usize];
            let mut e = ce.slot(idx.oldest).expect("oldest exemplar");
            if e.exemplar.ts > end || ce.ts_at(idx.newest) < start {
                continue;
            }
            if !matches_some_matcher_set(&idx.series_labels, matchers) {
                continue;
            }
            let mut exemplars = Vec::new();
            // Loop through all exemplars in the circular buffer for the current series.
            while e.exemplar.ts <= end {
                if e.exemplar.ts >= start {
                    exemplars.push(e.exemplar.clone());
                }
                if e.next == NO_EXEMPLAR {
                    break;
                }
                e = ce.slot(e.next).expect("linked exemplar");
            }
            if !exemplars.is_empty() {
                ret.push(QueryResult { series_labels: idx.series_labels.clone(), exemplars });
            }
        }
        drop(ce);

        ret.sort_by(|a, b| labels::compare(&a.series_labels, &b.series_labels).cmp(&0));
        Ok(ret)
    }

    /// Go `ValidateExemplar`.
    pub fn validate_exemplar(&self, l: &Labels, e: &Exemplar) -> Result<(), Error> {
        let ce = self.read();
        ce.validate_exemplar(ce.lookup(l), e, false, &self.metrics)
    }

    /// Go `AddExemplar`.
    pub fn add_exemplar(&self, l: &Labels, e: &Exemplar) -> Result<(), Error> {
        let mut guard = self.write();
        let ce = &mut *guard;

        if ce.length == 0 {
            return Err(ERR_EXEMPLARS_DISABLED);
        }

        let mut idx = ce.lookup(l);
        let mut index_exists = idx != NO_REF;
        if let Err(err) = ce.validate_exemplar(idx, e, true, &self.metrics) {
            if err.is(&ERR_DUPLICATE_EXEMPLAR) {
                // Duplicate exemplar, noop.
                return Ok(());
            }
            return Err(err);
        }

        // If we insert an out-of-order exemplar, we preemptively find the insertion index to check for
        // duplicates.
        let mut insertion_index = 0i32;
        let mut out_of_order = false;
        if index_exists {
            let ie = &ce.entries[idx as usize];
            out_of_order = e.ts >= ce.ts_at(ie.oldest) && e.ts < ce.ts_at(ie.newest);
            if out_of_order {
                insertion_index = ce.find_insertion_index(e, idx);
                if ce.ts_at(insertion_index) == e.ts {
                    // Assume duplicate exemplar, noop. Native histograms exercise this path a lot due to having
                    // multiple exemplars per series, so checking the value and labels would be too expensive.
                    return Ok(());
                }
            }
        }

        // If the index didn't exist (new series), create one.
        if !index_exists {
            idx = ce.new_index(l);
        }

        let ni = ce.next_index;

        // Remove entries if the buffer is full.
        let prev_ref = ce.ref_at(ni);
        if prev_ref != NO_REF {
            if ce.remove_exemplar(ni) {
                if prev_ref == idx {
                    // Do not delete the index entry we're inserting to.
                    index_exists = false;
                } else {
                    ce.remove_index(prev_ref);
                }
            } else if out_of_order && insertion_index == ni && prev_ref == idx {
                // The entry we were going to insert after was removed from the same series. Recalculate the
                // insertion point in the updated linked list to avoid creating a self-referencing loop.
                insertion_index = ce.find_insertion_index(e, idx);
            }
        }

        // We create a new entry in the linked list.
        {
            let s = ce.slot_mut(ni);
            s.exemplar = e.clone();
            s.ref_ = idx;
        }

        let (oldest, newest) = {
            let ie = &ce.entries[idx as usize];
            (ie.oldest, ie.newest)
        };
        if !index_exists {
            // Add the first and only exemplar to the list.
            let ie = &mut ce.entries[idx as usize];
            ie.oldest = ni;
            ie.newest = ni;
            let s = ce.slot_mut(ni);
            s.prev = NO_EXEMPLAR;
            s.next = NO_EXEMPLAR;
        } else if e.ts >= ce.ts_at(newest) {
            // Add the exemplar at the tip (after newest).
            ce.slot_mut(newest).next = ni;
            let s = ce.slot_mut(ni);
            s.prev = newest;
            s.next = NO_EXEMPLAR;
            ce.entries[idx as usize].newest = ni;
        } else if e.ts < ce.ts_at(oldest) {
            // Add the exemplar at the tail (before oldest).
            ce.slot_mut(oldest).prev = ni;
            let s = ce.slot_mut(ni);
            s.prev = NO_EXEMPLAR;
            s.next = oldest;
            ce.entries[idx as usize].oldest = ni;
        } else {
            // Insert the exemplar into the list by finding the most recent in-order exemplar that precedes it,
            // and placing it after.
            let next_exemplar = ce.slot(insertion_index).expect("insertion exemplar").next;
            {
                let s = ce.slot_mut(ni);
                s.prev = insertion_index;
                s.next = next_exemplar;
            }
            ce.slot_mut(insertion_index).next = ni;
            if next_exemplar != NO_EXEMPLAR {
                ce.slot_mut(next_exemplar).prev = ni;
            }
        }

        ce.next_index = ((ni as usize + 1) % ce.length) as i32;

        self.metrics.exemplars_appended.inc();
        ce.compute_metrics(&self.metrics);
        Ok(())
    }

    /// Go `IterateExemplars`: calls `f` for every exemplar, from oldest to newest appended, until the end or the
    /// first error.
    pub fn iterate_exemplars(
        &self,
        f: &mut dyn FnMut(&Labels, &Exemplar) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let ce = self.read();
        if ce.length == 0 {
            return Ok(());
        }
        // Logical order from next_index is physical order from its position (the slots are a rotation).
        let start = ce.phys(ce.next_index);
        let n = ce.slots.len();
        for p in (start.min(n)..n).chain(0..start.min(n)) {
            let e = &ce.slots[p];
            if e.ref_ == NO_REF {
                continue;
            }
            f(&ce.entries[e.ref_ as usize].series_labels, &e.exemplar)?;
        }
        Ok(())
    }

    /// Go `Resize`: changes the size of the buffer, migrating the data. Exemplars are kept when possible;
    /// shrinking discards old data (in order of ingestion) as needed. Returns the number of migrated exemplars.
    pub fn resize(&self, l: i64) -> i32 {
        // Accept negative values as just 0 size.
        let l = l.max(0) as usize;
        let mut ce = self.write();
        let old_size = ce.length;
        let migrated = if l == old_size {
            return 0;
        } else if l > old_size {
            ce.grow(l)
        } else {
            ce.shrink(l)
        };
        ce.compute_metrics(&self.metrics);
        self.metrics.max_exemplars.set(l as f64);
        migrated as i32
    }

    /// Go `SetOutOfOrderTimeWindow`: exemplars older than `d` milliseconds relative to the newest one of their
    /// series are rejected.
    pub fn set_out_of_order_time_window(&self, d: i64) {
        self.write().ooo_time_window_millis = d;
    }

    /// Go `ExemplarQuerier(ctx)`: a querier over this storage.
    pub fn exemplar_querier(self: &Arc<Self>, _ctx: &Context) -> Result<Box<dyn ExemplarQuerier>, Error> {
        Ok(Box::new(CircularExemplarQuerier(self.clone())))
    }

    /// Go `Querier(ctx)`: same as [`Self::exemplar_querier`].
    pub fn querier(self: &Arc<Self>, ctx: &Context) -> Result<Box<dyn ExemplarQuerier>, Error> {
        self.exemplar_querier(ctx)
    }

    // Test helpers: Go tests read the unexported fields of the storage.

    /// Go `len(ce.exemplars)`.
    #[doc(hidden)]
    pub fn debug_len(&self) -> usize {
        self.read().length
    }

    /// Go `ce.nextIndex`.
    #[doc(hidden)]
    pub fn debug_next_index(&self) -> i32 {
        self.read().next_index
    }

    /// Go `ce.exemplars[i].exemplar` (the zero exemplar for a slot that was never written).
    #[doc(hidden)]
    pub fn debug_exemplar_at(&self, i: usize) -> Exemplar {
        let ce = self.read();
        assert!(i < ce.length, "index out of range [{}] with length {}", i, ce.length);
        ce.slot(i as i32).map_or_else(Exemplar::default, |e| e.exemplar.clone())
    }

    /// Go `ce.index[string(l.Bytes(nil))]` as `(oldest, newest, seriesLabels)`.
    #[doc(hidden)]
    pub fn debug_index_entry(&self, l: &Labels) -> Option<(i32, i32, Labels)> {
        let ce = self.read();
        let r = ce.lookup(l);
        if r == NO_REF {
            return None;
        }
        let ie = &ce.entries[r as usize];
        Some((ie.oldest, ie.newest, ie.series_labels.clone()))
    }

    /// Go `debugCircularBuffer` (exemplar_test.go): the stored slots with their list pointers.
    #[doc(hidden)]
    pub fn debug_circular_buffer(&self) -> String {
        let ce = self.read();
        let mut sb = String::new();
        for i in 0..ce.length as i32 {
            let e = match ce.slot(i) {
                Some(e) if e.ref_ != NO_REF => e,
                _ => continue,
            };
            let _ = write!(sb, "i: {}, ts: {}, next: {}, prev: {}", i, e.exemplar.ts, e.next, e.prev);
            for &r in ce.index.values() {
                let idx = &ce.entries[r as usize];
                if i == idx.newest {
                    let _ = write!(sb, " <- newest {}", idx.series_labels);
                }
                if i == idx.oldest {
                    let _ = write!(sb, " <- oldest {}", idx.series_labels);
                }
            }
            sb.push('\n');
        }
        let _ = writeln!(sb, "Next index: {}", ce.next_index);
        sb
    }
}

/// Go `matchesSomeMatcherSet`.
fn matches_some_matcher_set(lbls: &Labels, matchers: &[Vec<Matcher>]) -> bool {
    matchers.iter().any(|ms| ms.iter().all(|m| m.matches(lbls.get(&m.name))))
}

/// The querier over the storage (Go returns the storage itself).
pub struct CircularExemplarQuerier(pub Arc<CircularExemplarStorage>);

impl ExemplarQuerier for CircularExemplarQuerier {
    fn select(&self, start: i64, end: i64, matchers: &[Vec<Matcher>]) -> Result<Vec<QueryResult>, Error> {
        self.0.select(start, end, matchers)
    }
}
