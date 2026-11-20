//! Isolation between appends and reads of the head (port of `tsdb/isolation.go`).
//!
//! Rust notes: Go keeps the open appenders and the open reads in doubly linked lists with sentinel heads. Here
//! they are small vectors sorted by ID (IDs only grow, so new entries go to the end); the oldest entry is the
//! first one. A read state is an `Arc<IsolationState>` that removes itself from the isolation on `close` (and
//! on drop, if it was not closed).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

fn rl<T>(l: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(|e| e.into_inner())
}

fn wl<T>(l: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(|e| e.into_inner())
}

/// The part of an open read that the isolation keeps (Go fields of `isolationState` used by
/// `TraverseOpenReads` and the low watermark).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenRead {
    pub low_watermark: u64,
    /// Time range of the read.
    pub mint: i64,
    pub maxt: i64,
}

#[derive(Default)]
struct AppendState {
    /// The last appendID given out (Go keeps it in the sentinel of `appendsOpenList`).
    last_append_id: u64,
    /// Open appends: (appendID, minTime), sorted by appendID.
    open: Vec<(u64, i64)>,
}

#[derive(Default)]
struct ReadState {
    next_id: u64,
    /// Open reads by an internal sequence number (oldest first).
    open: Vec<(u64, OpenRead)>,
}

/// Go `isolation`: the global isolation state.
pub struct Isolation {
    /// Take before `reads` if taking both (Go `appendMtx`).
    appends: RwLock<AppendState>,
    /// Go `readMtx`.
    reads: RwLock<ReadState>,
    /// If true, writes are not tracked while reads are still tracked.
    disabled: bool,
}

impl Isolation {
    /// Go `newIsolation`.
    pub fn new(disabled: bool) -> Arc<Isolation> {
        Arc::new(Isolation {
            appends: RwLock::new(AppendState::default()),
            reads: RwLock::new(ReadState::default()),
            disabled,
        })
    }

    /// Clears all state, as a new isolation would have (Go replaces the isolation in `resetInMemoryState`).
    pub(crate) fn reset(&self) {
        let mut a = wl(&self.appends);
        let mut r = wl(&self.reads);
        *a = AppendState::default();
        r.open.clear();
    }

    pub fn disabled(&self) -> bool {
        self.disabled
    }

    /// Go `lowWatermark`: the appendID below which we no longer need to track which appends were from which
    /// appendID.
    pub fn low_watermark(&self) -> u64 {
        if self.disabled {
            return 0;
        }
        let a = rl(&self.appends);
        self.low_watermark_locked(&a)
    }

    fn low_watermark_locked(&self, a: &AppendState) -> u64 {
        if self.disabled {
            return 0;
        }
        let r = rl(&self.reads);
        if let Some((_, oldest)) = r.open.first() {
            return oldest.low_watermark;
        }
        // Lowest appendID from appenders, or lastAppendId.
        a.open.first().map_or(a.last_append_id, |x| x.0)
    }

    /// Go `lowestAppendTime`: the lowest minTime of any open appender, or `i64::MAX` if there is none.
    pub fn lowest_append_time(&self) -> i64 {
        let a = rl(&self.appends);
        a.open.iter().map(|x| x.1).min().unwrap_or(i64::MAX)
    }

    /// Go `State`: an object that controls isolation between a query and appends. Must be closed when
    /// complete.
    pub fn state(self: &Arc<Self>, mint: i64, maxt: i64) -> Arc<IsolationState> {
        let a = rl(&self.appends);
        // We need to track reads even when isolation is disabled, so that head truncation can wait till reads
        // overlapping that range have finished.
        let low_watermark = a.open.first().map_or(a.last_append_id, |x| x.0);
        let incomplete_appends: Vec<u64> = a.open.iter().map(|x| x.0).collect();
        let max_append_id = a.last_append_id;
        let mut r = wl(&self.reads);
        r.next_id += 1;
        let id = r.next_id;
        r.open.push((
            id,
            OpenRead {
                low_watermark,
                mint,
                maxt,
            },
        ));
        drop(r);
        drop(a);
        Arc::new(IsolationState {
            max_append_id,
            incomplete_appends,
            low_watermark,
            mint,
            maxt,
            isolation: self.clone(),
            id,
            closed: AtomicBool::new(false),
        })
    }

    /// Go `TraverseOpenReads`: calls `f` for the open reads, newest first, until it returns false.
    pub fn traverse_open_reads(&self, mut f: impl FnMut(&OpenRead) -> bool) {
        let r = rl(&self.reads);
        for (_, s) in r.open.iter().rev() {
            if !f(s) {
                return;
            }
        }
    }

    /// Go `newAppendID`: a new transaction ID (the first is 1) and the low watermark.
    pub fn new_append_id(&self, min_time: i64) -> (u64, u64) {
        if self.disabled {
            return (0, 0);
        }
        let mut a = wl(&self.appends);
        a.last_append_id += 1;
        let id = a.last_append_id;
        a.open.push((id, min_time));
        let lw = self.low_watermark_locked(&a);
        (id, lw)
    }

    /// Go `lastAppendID`.
    pub fn last_append_id(&self) -> u64 {
        if self.disabled {
            return 0;
        }
        rl(&self.appends).last_append_id
    }

    /// Go `committedAppendID`: the highest appendID guaranteed to be committed at the time of the call.
    pub fn committed_append_id(&self) -> u64 {
        if self.disabled {
            return 0;
        }
        let a = rl(&self.appends);
        match a.open.first() {
            None => a.last_append_id,
            Some(x) => x.0 - 1,
        }
    }

    /// Go `closeAppend`.
    pub fn close_append(&self, append_id: u64) {
        if self.disabled {
            return;
        }
        let mut a = wl(&self.appends);
        if let Ok(i) = a.open.binary_search_by_key(&append_id, |x| x.0) {
            a.open.remove(i);
        }
        if a.open.is_empty() && a.open.capacity() > 64 {
            a.open.shrink_to_fit();
        }
    }

    fn close_read(&self, id: u64) {
        let mut r = wl(&self.reads);
        if let Ok(i) = r.open.binary_search_by_key(&id, |x| x.0) {
            r.open.remove(i);
        }
        if r.open.is_empty() && r.open.capacity() > 64 {
            r.open.shrink_to_fit();
        }
    }
}

/// Go `isolationState`: the isolation information of a read.
pub struct IsolationState {
    /// We will ignore all appends above the max, or that are incomplete.
    pub max_append_id: u64,
    /// Sorted.
    pub incomplete_appends: Vec<u64>,
    /// Lowest of incompleteAppends/maxAppendID.
    pub low_watermark: u64,
    /// Time range of the read.
    pub mint: i64,
    pub maxt: i64,
    isolation: Arc<Isolation>,
    id: u64,
    closed: AtomicBool,
}

impl IsolationState {
    /// Go `Close`. Closing twice has no effect.
    pub fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.isolation.close_read(self.id);
        }
    }

    /// Go `IsolationDisabled`.
    pub fn isolation_disabled(&self) -> bool {
        self.isolation.disabled
    }

    /// Whether `append_id` is in the incomplete appends.
    #[inline]
    pub fn is_incomplete(&self, append_id: u64) -> bool {
        self.incomplete_appends.binary_search(&append_id).is_ok()
    }
}

impl Drop for IsolationState {
    fn drop(&mut self) {
        self.close();
    }
}

impl std::fmt::Debug for IsolationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IsolationState")
            .field("max_append_id", &self.max_append_id)
            .field("low_watermark", &self.low_watermark)
            .field("mint", &self.mint)
            .field("maxt", &self.maxt)
            .finish()
    }
}

/// The slots of a [`TxRing`]: none, four inline (the first size, so that most series need no allocation), or
/// more on the heap. It dereferences to the slots.
#[derive(Clone, Debug, Default)]
pub enum TxIds {
    #[default]
    Empty,
    Inline([u64; 4]),
    Heap(Vec<u64>),
}

impl TxIds {
    fn zeroed(n: usize) -> TxIds {
        match n {
            0 => TxIds::Empty,
            4 => TxIds::Inline([0; 4]),
            _ => TxIds::Heap(vec![0; n]),
        }
    }
}

impl std::ops::Deref for TxIds {
    type Target = [u64];
    #[inline]
    fn deref(&self) -> &[u64] {
        match self {
            TxIds::Empty => &[],
            TxIds::Inline(a) => a,
            TxIds::Heap(v) => v,
        }
    }
}

impl std::ops::DerefMut for TxIds {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u64] {
        match self {
            TxIds::Empty => &mut [],
            TxIds::Inline(a) => a,
            TxIds::Heap(v) => v,
        }
    }
}

/// Go `txRing`: the transaction ID ring buffer of a series.
#[derive(Clone, Debug, Default)]
pub struct TxRing {
    pub tx_ids: TxIds,
    /// Position of the first id in the ring.
    pub tx_id_first: u32,
    /// How many ids in the ring.
    pub tx_id_count: u32,
}

impl TxRing {
    /// Go `newTxRing`.
    pub fn new(capacity: usize) -> TxRing {
        TxRing {
            tx_ids: TxIds::zeroed(capacity),
            tx_id_first: 0,
            tx_id_count: 0,
        }
    }

    /// Go `add`.
    pub fn add(&mut self, append_id: u64) {
        if self.tx_id_count as usize == self.tx_ids.len() {
            // Ring buffer is full, expand by doubling.
            let mut new_len = self.tx_id_count * 2;
            if new_len == 0 {
                new_len = 4;
            }
            let mut new_ring = TxIds::zeroed(new_len as usize);
            let first = self.tx_id_first as usize;
            let idx = self.tx_ids.len() - first;
            new_ring[..idx].copy_from_slice(&self.tx_ids[first..]);
            new_ring[idx..idx + first].copy_from_slice(&self.tx_ids[..first]);
            self.tx_ids = new_ring;
            self.tx_id_first = 0;
        }
        let len = self.tx_ids.len();
        self.tx_ids[(self.tx_id_first + self.tx_id_count) as usize % len] = append_id;
        self.tx_id_count += 1;
    }

    /// Go `cleanupAppendIDsBelow`.
    pub fn cleanup_append_ids_below(&mut self, bound: u64) {
        if self.tx_ids.is_empty() {
            return;
        }
        let mut pos = self.tx_id_first as usize;
        while self.tx_id_count > 0 {
            if self.tx_ids[pos] >= bound {
                break;
            }
            self.tx_id_first += 1;
            self.tx_id_count -= 1;
            pos += 1;
            if pos == self.tx_ids.len() {
                pos = 0;
            }
        }
        self.tx_id_first %= self.tx_ids.len() as u32;
    }

    /// Go `iterator`: an iterator that does not terminate.
    pub fn iterator(&self) -> TxRingIterator<'_> {
        TxRingIterator {
            ids: &self.tx_ids,
            pos: self.tx_id_first,
        }
    }
}

/// Go `txRingIterator`. It DOESN'T terminate.
pub struct TxRingIterator<'a> {
    ids: &'a [u64],
    pos: u32,
}

impl TxRingIterator<'_> {
    #[inline]
    pub fn at(&self) -> u64 {
        self.ids[self.pos as usize]
    }
    #[inline]
    pub fn next(&mut self) {
        self.pos += 1;
        if self.pos as usize == self.ids.len() {
            self.pos = 0;
        }
    }
}
