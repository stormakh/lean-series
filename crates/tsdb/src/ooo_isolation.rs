//! Isolation of reads of out-of-order m-mapped chunks (port of `tsdb/ooo_isolation.go`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use tsdb_format::chunks::ChunkDiskMapperRef;

#[derive(Default)]
struct Reads {
    next_id: u64,
    /// Open reads (id, minRef), in the order they were added.
    open: Vec<(u64, ChunkDiskMapperRef)>,
}

/// Go `oooIsolation`.
#[derive(Default)]
pub struct OooIsolation {
    mtx: RwLock<Reads>,
}

impl OooIsolation {
    /// Go `newOOOIsolation`.
    pub fn new() -> Arc<OooIsolation> {
        Arc::new(OooIsolation::default())
    }

    pub(crate) fn reset(&self) {
        let mut g = self.mtx.write().unwrap_or_else(|e| e.into_inner());
        g.open.clear();
    }

    /// Go `HasOpenReadsAtOrBefore`: whether any read uses chunks with reference at or before `r`.
    pub fn has_open_reads_at_or_before(&self, r: ChunkDiskMapperRef) -> bool {
        let g = self.mtx.read().unwrap_or_else(|e| e.into_inner());
        g.open.iter().any(|(_, min_ref)| r.greater_than(*min_ref))
    }

    /// Go `TrackReadAfter`: records a read that uses chunks with reference after `min_ref`. The returned state
    /// must be closed when the read is complete (it also closes on drop).
    pub fn track_read_after(self: &Arc<Self>, min_ref: ChunkDiskMapperRef) -> Arc<OooIsolationState> {
        let mut g = self.mtx.write().unwrap_or_else(|e| e.into_inner());
        g.next_id += 1;
        let id = g.next_id;
        g.open.push((id, min_ref));
        drop(g);
        Arc::new(OooIsolationState {
            i: self.clone(),
            id,
            min_ref,
            closed: AtomicBool::new(false),
        })
    }
}

/// Go `oooIsolationState`.
pub struct OooIsolationState {
    i: Arc<OooIsolation>,
    id: u64,
    pub min_ref: ChunkDiskMapperRef,
    closed: AtomicBool,
}

impl OooIsolationState {
    /// Go `Close`; closing twice has no effect.
    pub fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut g = self.i.mtx.write().unwrap_or_else(|e| e.into_inner());
        if let Some(p) = g.open.iter().position(|x| x.0 == self.id) {
            g.open.remove(p);
        }
    }
}

impl Drop for OooIsolationState {
    fn drop(&mut self) {
        self.close();
    }
}
