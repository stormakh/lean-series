//! WAL/WBL replay, m-map markers, chunk snapshots and the series state file of the head (port of
//! `tsdb/head_wal.go`).
//!
//! STATUS: stubs with the signatures that `Head::init` and `Head::close` call. The WAL writing on commit is in
//! `head_append.rs`. A later port fills these bodies. Until then, replaying a WAL or WBL that holds records
//! fails with an error (an empty or missing WAL works), and the chunk snapshot functions panic (the snapshot is
//! off by default).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use storage::Error;
use tsdb_format::chunks::ChunkDiskMapperRef;
use tsdb_format::wlog::{self, SegmentBufReader};

use crate::head::{Head, HeadSeriesRef, MemSeries, MmappedChunk};

/// Go `errLoadWbl`: an error while loading the WBL. Find it with `Error::downcast_ref::<ErrLoadWbl>()`.
#[derive(Debug, Clone)]
pub struct ErrLoadWbl(pub Error);

impl std::fmt::Display for ErrLoadWbl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for ErrLoadWbl {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// Go `SeriesLifecycleState`: the content of `series_state.json`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SeriesLifecycleState {
    pub last_series_id: u64,
    pub last_wal_segment: i64,
    pub clean_shutdown: bool,
}

/// Go `ChunkSnapshotStats`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChunkSnapshotStats {
    pub total_series: usize,
    pub dir: String,
}

/// Go `chunkSnapshotDir`.
pub fn chunk_snapshot_dir(wlast: i64, woffset: i64) -> String {
    format!("chunk_snapshot.{:06}.{:010}", wlast, woffset)
}

/// Go `LastChunkSnapshot`: (directory, WAL index, WAL offset) of the newest snapshot; `ERR_NOT_FOUND` if none.
pub fn last_chunk_snapshot(dir: &Path) -> Result<(PathBuf, i64, i64), Error> {
    let _ = dir;
    todo!("head_wal port: LastChunkSnapshot")
}

/// Go `DeleteChunkSnapshots`: deletes snapshots below the given index and offset.
pub fn delete_chunk_snapshots(dir: &Path, max_index: i64, max_offset: i64) -> Result<(), Error> {
    let _ = (dir, max_index, max_offset);
    todo!("head_wal port: DeleteChunkSnapshots")
}

fn not_ported(what: &str) -> Error {
    Error::msg(format!("{what}: replay of a non-empty log is not ported yet"))
}

impl Head {
    /// Go `loadWAL`: replays the records of `r` into the head. `multi_ref` maps duplicate series references to
    /// the one kept; `mmapped_chunks`/`ooo_mmapped_chunks` are the m-mapped chunks of series not yet in the head.
    pub(crate) fn load_wal(
        &self,
        r: &mut wlog::Reader<SegmentBufReader>,
        multi_ref: &mut HashMap<HeadSeriesRef, HeadSeriesRef>,
        mmapped_chunks: &HashMap<HeadSeriesRef, Vec<MmappedChunk>>,
        ooo_mmapped_chunks: &HashMap<HeadSeriesRef, Vec<MmappedChunk>>,
        last_mmap_ref: ChunkDiskMapperRef,
    ) -> Result<(), Error> {
        let _ = (multi_ref, mmapped_chunks, ooo_mmapped_chunks, last_mmap_ref);
        if r.next() {
            return Err(not_ported("loadWAL"));
        }
        match r.err() {
            Some(e) => Err(crate::head::wlog_err(e)),
            None => Ok(()),
        }
    }

    /// Go `loadWBL`: replays the out-of-order records of `r`.
    pub(crate) fn load_wbl(
        &self,
        r: &mut wlog::Reader<SegmentBufReader>,
        multi_ref: &mut HashMap<HeadSeriesRef, HeadSeriesRef>,
        last_mmap_ref: ChunkDiskMapperRef,
    ) -> Result<(), Error> {
        let _ = (multi_ref, last_mmap_ref);
        if r.next() {
            return Err(not_ported("loadWBL"));
        }
        match r.err() {
            Some(e) => Err(crate::head::wlog_err(e)),
            None => Ok(()),
        }
    }

    /// Go `loadChunkSnapshot`: (WAL index, WAL offset, series by reference).
    #[allow(clippy::type_complexity)]
    pub(crate) fn load_chunk_snapshot(
        &self,
    ) -> Result<(i64, i64, HashMap<HeadSeriesRef, std::sync::Arc<MemSeries>>), Error> {
        todo!("head_wal port: loadChunkSnapshot")
    }

    /// Go `ChunkSnapshot`: writes a snapshot of the head chunks.
    pub fn chunk_snapshot(&self) -> Result<ChunkSnapshotStats, Error> {
        todo!("head_wal port: ChunkSnapshot")
    }

    /// Go `performChunkSnapshot`.
    pub(crate) fn perform_chunk_snapshot(&self) -> Result<(), Error> {
        todo!("head_wal port: performChunkSnapshot")
    }

    /// Go `readSeriesStateFile`.
    pub fn read_series_state_file(&self) -> Result<SeriesLifecycleState, Error> {
        Err(Error::msg("open series_state.json: no such file or directory"))
    }

    /// Go `findLastSeriesID`: scans the WAL from the state's segment to `end_segment`.
    pub fn find_last_series_id(&self, state: &SeriesLifecycleState, end_segment: i64) -> Result<u64, Error> {
        let _ = (state, end_segment);
        todo!("head_wal port: findLastSeriesID")
    }

    /// Go `runSeriesStateTicker` (Go starts it in a goroutine).
    pub(crate) fn run_series_state_ticker(&self) {}

    /// Go `writeSeriesState`.
    pub(crate) fn write_series_state(&self, clean_shutdown: bool) {
        let _ = clean_shutdown;
    }
}
