//! WAL/WBL replay, chunk snapshots and the series state file of the head (port of `tsdb/head_wal.go`).
//!
//! Replay threads: Go starts `WALReplayConcurrency` goroutines for every replayed segment. Here, the records
//! are decoded on the calling thread, and the samples go to `wal_replay_concurrency` worker threads (scoped
//! threads, one shard of the series references each) that end when the segment is replayed. With a
//! concurrency of 1 the samples are applied on the calling thread. No thread lives after the replay.
//!
//! Memory: the decode buffers are reused for the whole segment; batches to the workers hold at most 5000
//! samples (as Go) and the workers give the buffers back for reuse. The input queues are short (Go: 300).

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chunkenc::{Encoding, ValueType};
use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram};
use model::labels::Labels;
use model::metadata::{self, Metadata};
use model::value::is_stale_nan;
use storage::{Error, ERR_NOT_FOUND, ERR_OUT_OF_ORDER_EXEMPLAR};
use tsdb_format::chunks::ChunkDiskMapperRef;
use tsdb_format::encoding::Decbuf;
use tsdb_format::record::{
    self, RefExemplar, RefFloatHistogramSample, RefHistogramSample, RefMetadata, RefMmapMarker, RefSample,
    RefSeries, Type,
};
use tsdb_format::tombstones::{self, Stone};
use tsdb_format::wlog::{self, CorruptionErr, SegmentBufReader, WL};

use crate::head::{
    lock, overlaps_closed_interval, wlog_err, Head, HeadSeriesRef, MemChunk, MemSeries, MemSeriesInner,
    MemSeriesOooFields, MmappedChunk, SeriesAppender,
};
use crate::head_append::ChunkOpts;

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

/// Go `chunkSnapshotPrefix`.
pub const CHUNK_SNAPSHOT_PREFIX: &str = "chunk_snapshot.";
/// Go `seriesStateFilename`.
pub const SERIES_STATE_FILENAME: &str = "series_state.json";

const CHUNK_SNAPSHOT_RECORD_TYPE_SERIES: u8 = 1;
const CHUNK_SNAPSHOT_RECORD_TYPE_TOMBSTONES: u8 = 2;
const CHUNK_SNAPSHOT_RECORD_TYPE_EXEMPLARS: u8 = 3;

/// Samples per batch to a worker (Go splits records into parts of 5000 samples).
const MAX_BATCH: usize = 5000;
/// Length of the input queue of each worker (Go: 300).
const INPUT_QUEUE: usize = 32;
/// Length of the queue of buffers that a worker gives back (Go: 300).
const RETURN_QUEUE: usize = 8;

/// Go `chunkSnapshotDir`.
pub fn chunk_snapshot_dir(wlast: i64, woffset: i64) -> String {
    format!("{CHUNK_SNAPSHOT_PREFIX}{:06}.{:010}", wlast, woffset)
}

fn read_dir_err(dir: &Path, e: &std::io::Error) -> Error {
    Error::msg(format!("open {}: {}", dir.display(), crate::io_err_text(e)))
}

/// Go `strconv.Atoi` for the non-negative names of snapshot directories.
fn atoi(s: &str) -> Option<i64> {
    let (neg, d) = match s.strip_prefix('-') {
        Some(d) => (true, d),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    if d.is_empty() || !d.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let v: i64 = d.parse().ok()?;
    Some(if neg { -v } else { v })
}

/// The (index, offset) of a snapshot directory name, if it has the right form.
fn parse_snapshot_name(name: &str) -> Option<(i64, i64)> {
    let rest = name.strip_prefix(CHUNK_SNAPSHOT_PREFIX)?;
    let splits: Vec<&str> = rest.split('.').collect();
    if splits.len() != 2 {
        return None;
    }
    Some((atoi(splits[0])?, atoi(splits[1])?))
}

fn read_dir_sorted(dir: &Path) -> Result<Vec<std::fs::DirEntry>, Error> {
    let rd = std::fs::read_dir(dir).map_err(|e| read_dir_err(dir, &e))?;
    let mut files: Vec<std::fs::DirEntry> = rd.collect::<Result<_, _>>().map_err(|e| read_dir_err(dir, &e))?;
    // Go `os.ReadDir` sorts by name.
    files.sort_by_key(|f| f.file_name());
    Ok(files)
}

/// Go `LastChunkSnapshot`: (directory, WAL index, WAL offset) of the newest snapshot; `ERR_NOT_FOUND` if none.
pub fn last_chunk_snapshot(dir: &Path) -> Result<(PathBuf, i64, i64), Error> {
    let files = read_dir_sorted(dir)?;
    let (mut max_idx, mut max_offset) = (-1i64, -1i64);
    let mut max_file: Option<PathBuf> = None;
    for fi in &files {
        let name = fi.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with(CHUNK_SNAPSHOT_PREFIX) {
            continue;
        }
        if !fi.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            return Err(Error::msg(format!("chunk snapshot {} is not a directory", name)));
        }
        let Some((idx, offset)) = parse_snapshot_name(&name) else {
            // Chunk snapshots is not in the right format, we do not care about it.
            continue;
        };
        if idx > max_idx || (idx == max_idx && offset > max_offset) {
            max_idx = idx;
            max_offset = offset;
            max_file = Some(dir.join(&*name));
        }
    }
    match max_file {
        Some(f) => Ok((f, max_idx, max_offset)),
        None => Err(ERR_NOT_FOUND),
    }
}

/// Go `DeleteChunkSnapshots`: deletes snapshots below the given index and offset.
pub fn delete_chunk_snapshots(dir: &Path, max_index: i64, max_offset: i64) -> Result<(), Error> {
    let files = read_dir_sorted(dir)?;
    let mut errs = Vec::new();
    for fi in &files {
        let name = fi.file_name();
        let name = name.to_string_lossy();
        let Some((idx, offset)) = parse_snapshot_name(&name) else {
            continue;
        };
        if idx < max_index || (idx == max_index && offset < max_offset) {
            let p = dir.join(&*name);
            if let Err(e) = std::fs::remove_dir_all(&p) {
                errs.push(Some(Error::msg(format!("unlinkat {}: {}", p.display(), crate::io_err_text(&e)))));
            }
        }
    }
    match Error::join(errs) {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Go `mergeReplayMmappedChunks`: merges persisted chunks without modifying the replay inventories. In-order
/// chunks are sorted by time; OOO chunks by disk reference, which is the order expected by OOO truncation.
pub fn merge_replay_mmapped_chunks(
    existing: &[MmappedChunk],
    incoming: &[MmappedChunk],
    last_mmap_ref: ChunkDiskMapperRef,
    ooo: bool,
) -> Vec<MmappedChunk> {
    if existing.is_empty() {
        return incoming.to_vec();
    }
    let mut incoming = incoming;
    let mut merged = Vec::with_capacity(existing.len() + incoming.len());
    for c in existing {
        if c.r#ref.greater_than(last_mmap_ref) {
            continue;
        }
        while let Some(i) = incoming.first() {
            if (ooo && i.r#ref < c.r#ref) || (!ooo && i.max_time < c.min_time) {
                merged.push(*i);
                incoming = &incoming[1..];
            } else {
                break;
            }
        }
        if let Some(i) = incoming.first()
            && (c.r#ref == i.r#ref || (!ooo && c.max_time >= i.min_time))
        {
            // Deduplicate disk references. For different in-order chunks with overlapping ranges, retain the
            // preference for the later WAL definition.
            continue;
        }
        merged.push(*c);
    }
    merged.extend_from_slice(incoming);
    merged
}

// ---------------------------------------------------------------------------------------------------------------
// Replay workers.

/// Go `histogramRecord`: an integer or float histogram sample (exactly one is set).
#[derive(Debug, Default)]
struct HistogramRecord {
    r#ref: HeadSeriesRef,
    st: i64,
    t: i64,
    h: Option<Arc<Histogram>>,
    fh: Option<Arc<FloatHistogram>>,
}

/// Go `walSubsetProcessorInputItem` and `wblSubsetProcessorInputItem`.
enum ReplayItem {
    /// WAL: a series record (the series in the head and the reference of the record).
    Series { series: Arc<MemSeries>, wal_ref: HeadSeriesRef },
    /// WBL: an m-map marker of the series.
    Mmapped(Arc<MemSeries>),
    Samples(Vec<RefSample>),
    Histograms(Vec<HistogramRecord>),
    Deleted(Vec<HeadSeriesRef>),
}

trait ReplayProcessor: Send {
    type Stats: Send;
    fn process(&mut self, item: &mut ReplayItem);
    fn finish(self) -> Self::Stats;
}

/// The workers of one replay call: inline (concurrency 1) or one thread per shard.
enum Shards<'scope, P: ReplayProcessor> {
    Inline {
        proc: P,
        spare_samples: Vec<RefSample>,
        spare_hists: Vec<HistogramRecord>,
    },
    Threads {
        inputs: Vec<SyncSender<ReplayItem>>,
        ret_samples: Vec<Receiver<Vec<RefSample>>>,
        ret_hists: Vec<Receiver<Vec<HistogramRecord>>>,
        handles: Vec<std::thread::ScopedJoinHandle<'scope, P::Stats>>,
    },
}

impl<'scope, P: ReplayProcessor + 'scope> Shards<'scope, P> {
    fn new<'env>(scope: &'scope std::thread::Scope<'scope, 'env>, mut procs: Vec<P>) -> Shards<'scope, P> {
        if procs.len() == 1 {
            return Shards::Inline {
                proc: procs.pop().expect("one processor"),
                spare_samples: Vec::new(),
                spare_hists: Vec::new(),
            };
        }
        let n = procs.len();
        let mut inputs = Vec::with_capacity(n);
        let mut ret_samples = Vec::with_capacity(n);
        let mut ret_hists = Vec::with_capacity(n);
        let mut handles = Vec::with_capacity(n);
        for mut p in procs {
            let (in_tx, in_rx) = mpsc::sync_channel::<ReplayItem>(INPUT_QUEUE);
            let (rs_tx, rs_rx) = mpsc::sync_channel::<Vec<RefSample>>(RETURN_QUEUE);
            let (rh_tx, rh_rx) = mpsc::sync_channel::<Vec<HistogramRecord>>(RETURN_QUEUE);
            let h = std::thread::Builder::new()
                .name("wal-replay".into())
                .spawn_scoped(scope, move || {
                    for mut item in in_rx {
                        p.process(&mut item);
                        match item {
                            ReplayItem::Samples(mut v) => {
                                v.clear();
                                let _ = rs_tx.try_send(v);
                            }
                            ReplayItem::Histograms(mut v) => {
                                v.clear();
                                let _ = rh_tx.try_send(v);
                            }
                            _ => {}
                        }
                    }
                    p.finish()
                })
                .expect("spawn WAL replay thread");
            inputs.push(in_tx);
            ret_samples.push(rs_rx);
            ret_hists.push(rh_rx);
            handles.push(h);
        }
        Shards::Threads {
            inputs,
            ret_samples,
            ret_hists,
            handles,
        }
    }

}

impl<P: ReplayProcessor> Shards<'_, P> {
    fn len(&self) -> usize {
        match self {
            Shards::Inline { .. } => 1,
            Shards::Threads { inputs, .. } => inputs.len(),
        }
    }

    fn send(&mut self, i: usize, mut item: ReplayItem) {
        match self {
            Shards::Inline {
                proc,
                spare_samples,
                spare_hists,
            } => {
                proc.process(&mut item);
                match item {
                    ReplayItem::Samples(mut v) => {
                        v.clear();
                        *spare_samples = v;
                    }
                    ReplayItem::Histograms(mut v) => {
                        v.clear();
                        *spare_hists = v;
                    }
                    _ => {}
                }
            }
            Shards::Threads { inputs, .. } => {
                // A worker only ends after the sender is dropped, so this cannot fail.
                let _ = inputs[i].send(item);
            }
        }
    }

    /// Go `reuseBuf`: a buffer that worker `i` gave back, or an empty one.
    fn reuse_samples(&mut self, i: usize) -> Vec<RefSample> {
        match self {
            Shards::Inline { spare_samples, .. } => std::mem::take(spare_samples),
            Shards::Threads { ret_samples, .. } => ret_samples[i].try_recv().unwrap_or_default(),
        }
    }

    /// Go `reuseHistogramBuf`.
    fn reuse_hists(&mut self, i: usize) -> Vec<HistogramRecord> {
        match self {
            Shards::Inline { spare_hists, .. } => std::mem::take(spare_hists),
            Shards::Threads { ret_hists, .. } => ret_hists[i].try_recv().unwrap_or_default(),
        }
    }

    /// Go `closeAndDrain` and `wg.Wait`: the workers process what is queued and return their stats.
    fn finish(self) -> Vec<P::Stats> {
        match self {
            Shards::Inline { proc, .. } => vec![proc.finish()],
            Shards::Threads { inputs, handles, .. } => {
                drop(inputs);
                handles
                    .into_iter()
                    .map(|h| match h.join() {
                        Ok(s) => s,
                        Err(p) => std::panic::resume_unwind(p),
                    })
                    .collect()
            }
        }
    }

    /// Sends the samples to the workers by series reference in batches of at most 5000. `f` maps or drops a
    /// sample (false: drop).
    fn dispatch_samples(&mut self, samples: &[RefSample], mut f: impl FnMut(&mut RefSample) -> bool) {
        let n = self.len();
        let mut shards: Vec<Vec<RefSample>> = (0..n).map(|_| Vec::new()).collect();
        for part in samples.chunks(MAX_BATCH) {
            for (i, s) in shards.iter_mut().enumerate() {
                if s.capacity() == 0 {
                    *s = self.reuse_samples(i);
                }
            }
            for s in part {
                let mut s = *s;
                if !f(&mut s) {
                    continue;
                }
                shards[(s.r#ref % n as u64) as usize].push(s);
            }
            for (i, s) in shards.iter_mut().enumerate() {
                if !s.is_empty() {
                    let v = std::mem::take(s);
                    self.send(i, ReplayItem::Samples(v));
                }
            }
        }
    }

    /// Like `dispatch_samples` for histogram samples; moves the histograms out of `samples` (which is emptied).
    fn dispatch_hists<T>(
        &mut self,
        samples: &mut Vec<T>,
        mut f: impl FnMut(T) -> Option<HistogramRecord>,
    ) {
        let n = self.len();
        let mut shards: Vec<Vec<HistogramRecord>> = (0..n).map(|_| Vec::new()).collect();
        let mut in_part = 0usize;
        let total = samples.len();
        for (k, s) in samples.drain(..).enumerate() {
            if in_part == 0 {
                for (i, s) in shards.iter_mut().enumerate() {
                    if s.capacity() == 0 {
                        *s = self.reuse_hists(i);
                    }
                }
            }
            in_part += 1;
            if let Some(rec) = f(s) {
                shards[(rec.r#ref % n as u64) as usize].push(rec);
            }
            if in_part == MAX_BATCH || k + 1 == total {
                in_part = 0;
                for (i, s) in shards.iter_mut().enumerate() {
                    if !s.is_empty() {
                        let v = std::mem::take(s);
                        self.send(i, ReplayItem::Histograms(v));
                    }
                }
            }
        }
    }
}

fn replay_concurrency(h: &Head) -> usize {
    h.opts.wal_replay_concurrency.max(1) as usize
}

fn corruption(what: &str, e: impl std::fmt::Display, r: &wlog::Reader<SegmentBufReader>) -> Error {
    wlog_err(wlog::Error::Corruption(Box::new(CorruptionErr {
        dir: PathBuf::new(),
        segment: r.segment(),
        offset: r.offset(),
        err: wlog::Error::Msg(format!("{what}: {e}")),
    })))
}

/// Go `valueTypeForChunkEncoding`.
fn value_type_for_chunk_encoding(enc: Encoding) -> ValueType {
    match enc {
        chunkenc::ENC_XOR | chunkenc::ENC_XOR2 => ValueType::Float,
        chunkenc::ENC_HISTOGRAM | chunkenc::ENC_HISTOGRAM_ST => ValueType::Histogram,
        chunkenc::ENC_FLOAT_HISTOGRAM | chunkenc::ENC_FLOAT_HISTOGRAM_ST => ValueType::FloatHistogram,
        _ => ValueType::None,
    }
}

/// Go `latestInOrderValueType`: the value type of the series' most recent in-order sample, from the newest
/// materialized chunk.
fn latest_in_order_value_type(s: &MemSeriesInner) -> ValueType {
    if let Some(c) = s.head_chunks.last() {
        return value_type_for_chunk_encoding(c.chunk.encoding());
    }
    if let Some(c) = s.mmapped_chunks.last() {
        return value_type_for_chunk_encoding(c.encoding);
    }
    if s.last_histogram_value().is_some() {
        return ValueType::Histogram;
    }
    if s.last_float_histogram_value().is_some() {
        return ValueType::FloatHistogram;
    }
    ValueType::Float
}

/// Go `walSubsetProcessor`.
struct WalWorker<'a> {
    head: &'a Head,
    mmapped_chunks: &'a HashMap<HeadSeriesRef, Vec<MmappedChunk>>,
    ooo_mmapped_chunks: &'a HashMap<HeadSeriesRef, Vec<MmappedChunk>>,
    last_mmap_ref: ChunkDiskMapperRef,
    opts: ChunkOpts<'a>,
    min_valid_time: i64,
    missing: HashSet<HeadSeriesRef>,
    unknown_samples: u64,
    unknown_histograms: u64,
    overlapping: u64,
    mint: i64,
    maxt: i64,
}

struct WalStats {
    missing: HashSet<HeadSeriesRef>,
    unknown_samples: u64,
    unknown_histograms: u64,
    overlapping: u64,
}

impl ReplayProcessor for WalWorker<'_> {
    type Stats = WalStats;

    /// Go `processWALSamples` (one input item).
    fn process(&mut self, item: &mut ReplayItem) {
        let h = self.head;
        match item {
            ReplayItem::Series { series, wal_ref } => {
                let empty: &[MmappedChunk] = &[];
                let mmc = self.mmapped_chunks.get(wal_ref).map_or(empty, |v| v.as_slice());
                let ooo_mmc = self.ooo_mmapped_chunks.get(wal_ref).map_or(empty, |v| v.as_slice());
                if h.reset_series_with_mmapped_chunks(series, mmc, ooo_mmc, *wal_ref, self.last_mmap_ref) {
                    self.overlapping += 1;
                }
            }
            ReplayItem::Samples(samples) => {
                for s in samples.iter() {
                    let Some(ms) = h.series.get_by_id(s.r#ref) else {
                        self.unknown_samples += 1;
                        self.missing.insert(s.r#ref);
                        continue;
                    };
                    let mut g = ms.lock();
                    if s.t <= g.mm_max_time {
                        continue;
                    }
                    h.append_wal_float(&ms, &mut g, s, &self.opts);
                    drop(g);
                    self.maxt = self.maxt.max(s.t);
                    self.mint = self.mint.min(s.t);
                }
            }
            ReplayItem::Histograms(hists) => {
                for s in hists.iter_mut() {
                    if s.t < self.min_valid_time {
                        continue;
                    }
                    let Some(ms) = h.series.get_by_id(s.r#ref) else {
                        self.unknown_histograms += 1;
                        self.missing.insert(s.r#ref);
                        continue;
                    };
                    let mut g = ms.lock();
                    if s.t <= g.mm_max_time {
                        continue;
                    }
                    h.append_wal_histogram(&ms, &mut g, s.st, s.t, s.h.take(), s.fh.take(), &self.opts);
                    drop(g);
                    self.maxt = self.maxt.max(s.t);
                    self.mint = self.mint.min(s.t);
                }
            }
            ReplayItem::Deleted(refs) => {
                if !refs.is_empty() {
                    h.delete_series_by_id(refs);
                }
            }
            ReplayItem::Mmapped(_) => {}
        }
    }

    fn finish(self) -> WalStats {
        self.head.update_min_max_time(self.mint, self.maxt);
        WalStats {
            missing: self.missing,
            unknown_samples: self.unknown_samples,
            unknown_histograms: self.unknown_histograms,
            overlapping: self.overlapping,
        }
    }
}

/// Go `wblSubsetProcessor`.
struct WblWorker<'a> {
    head: &'a Head,
    opts: ChunkOpts<'a>,
    ooo_cap_max: i64,
    missing: HashSet<HeadSeriesRef>,
    unknown_samples: u64,
    unknown_histograms: u64,
    mint: i64,
    maxt: i64,
}

struct WblStats {
    missing: HashSet<HeadSeriesRef>,
    unknown_samples: u64,
    unknown_histograms: u64,
}

impl ReplayProcessor for WblWorker<'_> {
    type Stats = WblStats;

    /// Go `processWBLSamples` (one input item). We don't check for minValidTime for ooo samples.
    fn process(&mut self, item: &mut ReplayItem) {
        let h = self.head;
        match item {
            ReplayItem::Mmapped(ms) => {
                let mut g = ms.lock();
                if let Some(ooo) = g.ooo_mut() {
                    // All samples till now have been m-mapped. Hence clear out the headChunk.
                    ooo.ooo_head_chunk = None;
                }
            }
            ReplayItem::Samples(samples) => {
                for s in samples.iter() {
                    let Some(ms) = h.series.get_by_id(s.r#ref) else {
                        self.unknown_samples += 1;
                        self.missing.insert(s.r#ref);
                        continue;
                    };
                    let mut g = ms.lock();
                    let (ok, created, _) = g.insert(
                        s.st,
                        s.t,
                        s.v,
                        None,
                        None,
                        &self.opts,
                        self.ooo_cap_max,
                        &h.logger,
                        &ms.lset,
                        ms.r#ref,
                    );
                    drop(g);
                    if created {
                        h.metrics.chunks_created.inc();
                        h.metrics.chunks.inc();
                    }
                    if ok {
                        self.mint = self.mint.min(s.t);
                        self.maxt = self.maxt.max(s.t);
                    }
                }
            }
            ReplayItem::Histograms(hists) => {
                for s in hists.iter_mut() {
                    let Some(ms) = h.series.get_by_id(s.r#ref) else {
                        self.unknown_histograms += 1;
                        self.missing.insert(s.r#ref);
                        continue;
                    };
                    let hh = s.h.take().map(|x| Box::new(Arc::unwrap_or_clone(x)));
                    let fh = s.fh.take().map(|x| Box::new(Arc::unwrap_or_clone(x)));
                    let mut g = ms.lock();
                    let (ok, created, _) =
                        g.insert(s.st, s.t, 0.0, hh, fh, &self.opts, self.ooo_cap_max, &h.logger, &ms.lset, ms.r#ref);
                    drop(g);
                    if created {
                        h.metrics.chunks_created.inc();
                        h.metrics.chunks.inc();
                    }
                    if ok {
                        self.mint = self.mint.min(s.t);
                        self.maxt = self.maxt.max(s.t);
                    }
                }
            }
            ReplayItem::Series { .. } | ReplayItem::Deleted(_) => {}
        }
    }

    fn finish(self) -> WblStats {
        self.head.update_min_ooo_max_ooo_time(self.mint, self.maxt);
        WblStats {
            missing: self.missing,
            unknown_samples: self.unknown_samples,
            unknown_histograms: self.unknown_histograms,
        }
    }
}

fn add_non_zero(c: &crate::head::LabeledCounter, v: u64, label: &str) {
    if v > 0 {
        c.add(label, v as f64);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Chunk snapshot records.

/// Go `chunkSnapshotRecord`.
struct ChunkSnapshotRecord {
    r#ref: HeadSeriesRef,
    lset: Labels,
    mc: Option<(MemChunk, chunkenc::Appender)>,
    last_value: f64,
    last_histogram_value: Option<Arc<Histogram>>,
    last_float_histogram_value: Option<Arc<FloatHistogram>>,
}

/// Go `memSeries.encodeToSnapshotRecord`: appends the record to `b`.
fn encode_to_snapshot_record(s: &MemSeries, b: &mut Vec<u8>) {
    b.push(CHUNK_SNAPSHOT_RECORD_TYPE_SERIES);
    b.extend_from_slice(&s.r#ref.to_be_bytes());
    record::encode_labels(b, s.labels());
    b.extend_from_slice(&0i64.to_be_bytes()); // Backwards-compatibility; was chunkRange but now unused.

    let g = s.lock();
    let Some(hc) = g.head_chunks.last() else {
        tsdb_format::encoding::put_uvarint(b, 0);
        return;
    };
    let enc = hc.chunk.encoding();
    tsdb_format::encoding::put_uvarint(b, 1);
    b.extend_from_slice(&hc.min_time.to_be_bytes());
    b.extend_from_slice(&hc.max_time.to_be_bytes());
    b.push(enc.0);
    let bytes = hc.chunk.bytes();
    tsdb_format::encoding::put_uvarint(b, bytes.len() as u64);
    b.extend_from_slice(bytes);
    match enc {
        chunkenc::ENC_XOR | chunkenc::ENC_XOR2 => {
            // Backwards compatibility for old sampleBuf which had last 4 samples.
            for _ in 0..3 {
                b.extend_from_slice(&0i64.to_be_bytes());
                b.extend_from_slice(&0f64.to_bits().to_be_bytes());
            }
            b.extend_from_slice(&0i64.to_be_bytes());
            b.extend_from_slice(&g.last_value.to_bits().to_be_bytes());
        }
        chunkenc::ENC_HISTOGRAM | chunkenc::ENC_HISTOGRAM_ST => {
            let def = Histogram::default();
            record::encode_histogram(b, g.last_histogram_value().map_or(&def, |h| &**h));
        }
        chunkenc::ENC_FLOAT_HISTOGRAM | chunkenc::ENC_FLOAT_HISTOGRAM_ST => {
            let def = FloatHistogram::default();
            record::encode_float_histogram(b, g.last_float_histogram_value().map_or(&def, |h| &**h));
        }
        e => panic!("unknown chunk encoding: {}", e),
    }
}

/// Go `decodeSeriesFromChunkSnapshot`.
fn decode_series_from_chunk_snapshot(d: &mut record::Decoder, b: &[u8]) -> Result<ChunkSnapshotRecord, Error> {
    let mut dec = Decbuf::new(b);
    let flag = dec.byte();
    if flag != CHUNK_SNAPSHOT_RECORD_TYPE_SERIES {
        return Err(Error::msg(format!("invalid record type {:x}", flag)));
    }
    let mut csr = ChunkSnapshotRecord {
        r#ref: dec.be64(),
        lset: Labels::default(),
        mc: None,
        last_value: 0.0,
        last_histogram_value: None,
        last_float_histogram_value: None,
    };
    // The label set written to the disk is already sorted.
    csr.lset = d.decode_labels(&mut dec);
    let _ = dec.be64int64(); // Was chunkRange but now unused.
    if dec.uvarint() == 0 {
        return match dec.err() {
            Some(e) => Err(Error::msg(e.to_string())),
            None => Ok(csr),
        };
    }
    let min_time = dec.be64int64();
    let max_time = dec.be64int64();
    let enc = Encoding(dec.byte());
    // The underlying bytes gets re-used later, so make a copy.
    let data = dec.uvarint_bytes().to_vec();
    let mut chk = chunkenc::from_data(enc, data).map_err(|e| Error::msg(format!("chunk from data: {e}")))?;
    match enc {
        chunkenc::ENC_XOR | chunkenc::ENC_XOR2 => {
            // Backwards-compatibility for old sampleBuf which had last 4 samples.
            for _ in 0..3 {
                let _ = dec.be64int64();
                let _ = dec.be64_float64();
            }
            let _ = dec.be64int64();
            csr.last_value = dec.be64_float64();
        }
        chunkenc::ENC_HISTOGRAM | chunkenc::ENC_HISTOGRAM_ST => {
            let mut h = Histogram::default();
            record::decode_histogram(&mut dec, &mut h);
            csr.last_histogram_value = Some(Arc::new(h));
        }
        chunkenc::ENC_FLOAT_HISTOGRAM | chunkenc::ENC_FLOAT_HISTOGRAM_ST => {
            let mut h = FloatHistogram::default();
            record::decode_float_histogram(&mut dec, &mut h);
            csr.last_float_histogram_value = Some(Arc::new(h));
        }
        e => return Err(Error::msg(format!("chunk encoding {} has no decode case", e))),
    }
    if dec.err().is_some() && dec.len() > 0 {
        return Err(Error::msg(format!("unexpected {} bytes left in entry", dec.len())));
    }
    if let Some(e) = dec.err() {
        return Err(Error::msg(e.to_string()));
    }
    let app = chk.appender().map_err(|e| Error::msg(e.to_string()))?;
    csr.mc = Some((
        MemChunk {
            chunk: Arc::from(chk),
            min_time,
            max_time,
        },
        app,
    ));
    Ok(csr)
}

/// Go `encodeTombstonesToSnapshotRecord`.
fn encode_tombstones_to_snapshot_record(tr: &dyn tombstones::Reader) -> Result<Vec<u8>, Error> {
    let mut buf = vec![CHUNK_SNAPSHOT_RECORD_TYPE_TOMBSTONES];
    let b = tombstones::encode(tr).map_err(|e| Error::wrap("encode tombstones", crate::tomb_err(e)))?;
    tsdb_format::encoding::put_uvarint(&mut buf, b.len() as u64);
    buf.extend_from_slice(&b);
    Ok(buf)
}

/// Go `decodeTombstonesSnapshotRecord`.
fn decode_tombstones_snapshot_record(b: &[u8]) -> Result<tombstones::MemTombstones, Error> {
    let mut dec = Decbuf::new(b);
    let flag = dec.byte();
    if flag != CHUNK_SNAPSHOT_RECORD_TYPE_TOMBSTONES {
        return Err(Error::msg(format!("invalid record type {:x}", flag)));
    }
    tombstones::decode(dec.uvarint_bytes()).map_err(|e| Error::wrap("decode tombstones", crate::tomb_err(e)))
}

// ---------------------------------------------------------------------------------------------------------------
// Head methods.

impl Head {
    fn replay_chunk_opts(&self) -> ChunkOpts<'_> {
        // storeST must be passed so that appendPreprocessor cuts an in-progress XOR chunk immediately when
        // replaying into a head with ST storage enabled.
        ChunkOpts {
            chunk_disk_mapper: Some(&self.chunk_disk_mapper),
            chunk_range: self.chunk_range(),
            samples_per_chunk: self.opts.samples_per_chunk,
            use_xor2: self.opts.use_xor2_float_encoding(),
            use_histogram_st: self.opts.enable_histogram_st_encoding.load(Ordering::Relaxed),
            store_st: self.opts.enable_st_storage.load(Ordering::Relaxed),
        }
    }

    /// Go `appendChunkAndMmap` (tests): runs `append` on the locked series and m-maps as WAL replay does.
    #[doc(hidden)]
    pub fn test_append_chunk_and_mmap(
        &self,
        ms: &MemSeries,
        g: &mut MemSeriesInner,
        append: impl FnOnce(&mut MemSeriesInner) -> (bool, bool),
    ) -> (bool, bool) {
        let prev = g.head_chunk_count();
        let (in_order, created) = append(g);
        self.after_replay_append(ms, g, prev, created);
        (in_order, created)
    }

    /// Go `appendChunkAndMmap` (after the append): if a new head chunk was created, m-maps the completed ones.
    fn after_replay_append(&self, ms: &MemSeries, g: &mut MemSeriesInner, prev: u32, chunk_created: bool) {
        if chunk_created {
            self.metrics.chunks_created.inc();
            self.metrics.chunks.inc();
            g.mmap_chunks(&self.chunk_disk_mapper, ms.r#ref);
            if prev >= 2 {
                self.series.dec_mmap_ready(ms.r#ref);
            }
        }
    }

    /// Go `appendWALFloat`.
    fn append_wal_float(&self, ms: &MemSeries, g: &mut MemSeriesInner, s: &RefSample, o: &ChunkOpts<'_>) {
        let is_stale = is_stale_nan(s.v);
        if is_stale {
            // Mirror commitFloats: decide the marker type from the series' most recent in-order sample.
            match latest_in_order_value_type(g) {
                ValueType::Histogram => {
                    let h = Histogram {
                        sum: s.v,
                        ..Default::default()
                    };
                    self.append_wal_histogram(ms, g, s.st, s.t, Some(Arc::new(h)), None, o);
                    return;
                }
                ValueType::FloatHistogram => {
                    let fh = FloatHistogram {
                        sum: s.v,
                        ..Default::default()
                    };
                    self.append_wal_histogram(ms, g, s.st, s.t, None, Some(Arc::new(fh)), o);
                    return;
                }
                _ => {}
            }
        }
        let (was_stale, was_histogram, old_buckets) = g.sample_state();
        let prev = g.head_chunk_count();
        let (in_order, created) = g.append(s.st, s.t, s.v, 0, o, &ms.lset, ms.r#ref);
        self.after_replay_append(ms, g, prev, created);
        if in_order {
            self.update_stale_series_metric_on_append(was_stale, is_stale);
            if was_histogram {
                self.update_native_histogram_metrics_on_append(true, false, old_buckets, 0);
            }
        }
    }

    /// Go `appendWALHistogram`: exactly one of `h`/`fh` must be set.
    #[allow(clippy::too_many_arguments)]
    fn append_wal_histogram(
        &self,
        ms: &MemSeries,
        g: &mut MemSeriesInner,
        st: i64,
        t: i64,
        h: Option<Arc<Histogram>>,
        fh: Option<Arc<FloatHistogram>>,
        o: &ChunkOpts<'_>,
    ) {
        let (was_stale, was_histogram, old_buckets) = g.sample_state();
        let prev = g.head_chunk_count();
        let (is_stale, new_buckets, (in_order, created)) = if let Some(h) = h {
            let is_stale = is_stale_nan(h.sum);
            let nb = h.positive_buckets.len() + h.negative_buckets.len();
            (is_stale, nb, g.append_histogram(st, t, h, 0, o, &ms.lset, ms.r#ref))
        } else {
            let fh = fh.expect("histogram or float histogram");
            let is_stale = is_stale_nan(fh.sum);
            let nb = fh.positive_buckets.len() + fh.negative_buckets.len();
            (is_stale, nb, g.append_float_histogram(st, t, fh, 0, o, &ms.lset, ms.r#ref))
        };
        self.after_replay_append(ms, g, prev, created);
        if in_order {
            self.update_stale_series_metric_on_append(was_stale, is_stale);
            self.update_native_histogram_metrics_on_append(was_histogram, true, old_buckets, new_buckets);
        }
    }

    /// Go `resetSeriesWithMMappedChunks` (only used during the WAL replay). Returns whether the new m-mapped
    /// chunks overlap the existing ones of a duplicate series record.
    fn reset_series_with_mmapped_chunks(
        &self,
        ms: &MemSeries,
        mmc: &[MmappedChunk],
        ooo_mmc: &[MmappedChunk],
        wal_series_ref: HeadSeriesRef,
        last_mmap_ref: ChunkDiskMapperRef,
    ) -> bool {
        let mut g = ms.lock();
        let mut overlapped = false;
        if ms.r#ref != wal_series_ref
            && let (Some(of), Some(ol), Some(nf), Some(nl)) =
                (g.mmapped_chunks.first(), g.mmapped_chunks.last(), mmc.first(), mmc.last())
            && overlaps_closed_interval(of.min_time, ol.max_time, nf.min_time, nl.max_time)
        {
            self.logger.debug(
                "M-mapped chunks overlap on a duplicate series record",
                &[
                    ("series", &ms.lset),
                    ("oldref", &ms.r#ref),
                    ("oldmint", &of.min_time),
                    ("oldmaxt", &ol.max_time),
                    ("newref", &wal_series_ref),
                    ("newmint", &nf.min_time),
                    ("newmaxt", &nl.max_time),
                ],
            );
            overlapped = true;
        }

        // Preserve chunks that were already on disk at startup, including those attached through another WAL
        // reference. Chunks written during this replay belong to the obsolete generation and must be discarded.
        let mmc = merge_replay_mmapped_chunks(&g.mmapped_chunks, mmc, last_mmap_ref, false);
        let old_ooo_len = g.ooo().map(|o| o.ooo_mmapped_chunks.len());
        let ooo_mmc = match g.ooo() {
            Some(o) => merge_replay_mmapped_chunks(&o.ooo_mmapped_chunks, ooo_mmc, last_mmap_ref, true),
            None => ooo_mmc.to_vec(),
        };

        let old_len = g.mmapped_chunks.len();
        self.metrics.chunks_created.add((mmc.len() + ooo_mmc.len()) as f64);
        self.metrics.chunks_removed.add(old_len as f64);
        self.metrics.chunks.add(mmc.len() as f64 + ooo_mmc.len() as f64 - old_len as f64);
        if let Some(n) = old_ooo_len {
            self.metrics.chunks_removed.add(n as f64);
            self.metrics.chunks.sub(n as f64);
        }

        g.mmapped_chunks = mmc;
        if ooo_mmc.is_empty() {
            g.clear_ooo();
        } else {
            *g.ooo_or_init() = MemSeriesOooFields {
                ooo_mmapped_chunks: ooo_mmc,
                ..Default::default()
            };
        }
        // Cache the last mmapped chunk time, so we can skip calling append() for samples it will reject.
        match (g.mmapped_chunks.first().map(|c| c.min_time), g.mmapped_chunks.last().map(|c| c.max_time)) {
            (Some(mint), Some(maxt)) => {
                g.mm_max_time = maxt;
                self.update_min_max_time(mint, maxt);
            }
            _ => g.mm_max_time = i64::MIN,
        }
        if let Some(o) = g.ooo()
            && !o.ooo_mmapped_chunks.is_empty()
        {
            // Mint and maxt can be in any chunk, they are not sorted.
            let (mut mint, mut maxt) = (i64::MAX, i64::MIN);
            for c in &o.ooo_mmapped_chunks {
                mint = mint.min(c.min_time);
                maxt = maxt.max(c.max_time);
            }
            self.update_min_ooo_max_ooo_time(mint, maxt);
        }

        // Any samples replayed till now would already be compacted. Resetting the head chunk.
        g.next_at = 0;
        if g.head_chunk_count() >= 2 {
            self.series.dec_mmap_ready(ms.r#ref);
        }
        g.head_chunks = Vec::new();
        g.app = SeriesAppender::None;
        overlapped
    }

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
        let concurrency = replay_concurrency(self);
        let min_valid_time = self.min_valid_time.load(Ordering::SeqCst);
        let procs: Vec<WalWorker<'_>> = (0..concurrency)
            .map(|_| WalWorker {
                head: self,
                mmapped_chunks,
                ooo_mmapped_chunks,
                last_mmap_ref,
                opts: self.replay_chunk_opts(),
                min_valid_time,
                missing: HashSet::new(),
                unknown_samples: 0,
                unknown_histograms: 0,
                overlapping: 0,
                mint: i64::MAX,
                maxt: i64::MIN,
            })
            .collect();

        let mut unknown_exemplars = 0u64;
        let mut unknown_metadata = 0u64;
        let mut unknown_tombstones = 0u64;
        let mut missing: HashSet<HeadSeriesRef> = HashSet::new();

        let (res, stats) = std::thread::scope(|scope| {
            let mut shards = Shards::new(scope, procs);
            let res = self.load_wal_records(
                r,
                multi_ref,
                &mut shards,
                &mut missing,
                &mut unknown_exemplars,
                &mut unknown_metadata,
                &mut unknown_tombstones,
            );
            // Signal termination to each worker and wait for it to finish.
            (res, shards.finish())
        });
        res?;
        if let Some(e) = r.err() {
            return Err(Error::wrap("read records", wlog_err(e)));
        }

        let (mut unknown_samples, mut unknown_histograms, mut overlapping) = (0u64, 0u64, 0u64);
        for s in stats {
            missing.extend(s.missing);
            unknown_samples += s.unknown_samples;
            unknown_histograms += s.unknown_histograms;
            overlapping += s.overlapping;
        }
        if unknown_samples + unknown_exemplars + unknown_histograms + unknown_metadata + unknown_tombstones > 0 {
            self.logger.warn(
                "Unknown series references",
                &[
                    ("series", &missing.len()),
                    ("samples", &unknown_samples),
                    ("exemplars", &unknown_exemplars),
                    ("histograms", &unknown_histograms),
                    ("metadata", &unknown_metadata),
                    ("tombstones", &unknown_tombstones),
                ],
            );
            let c = &self.metrics.wal_replay_unknown_refs_total;
            add_non_zero(c, missing.len() as u64, "series");
            add_non_zero(c, unknown_samples, "samples");
            add_non_zero(c, unknown_exemplars, "exemplars");
            add_non_zero(c, unknown_histograms, "histograms");
            add_non_zero(c, unknown_metadata, "metadata");
            add_non_zero(c, unknown_tombstones, "tombstones");
        }
        if overlapping > 0 {
            self.logger.info("Overlapping m-map chunks on duplicate series records", &[("count", &overlapping)]);
        }
        Ok(())
    }

    /// The record loop of `loadWAL` (Go: the decoder goroutine and the main loop). The records are always
    /// replayed from the oldest to the newest.
    #[allow(clippy::too_many_arguments)]
    fn load_wal_records(
        &self,
        r: &mut wlog::Reader<SegmentBufReader>,
        multi_ref: &mut HashMap<HeadSeriesRef, HeadSeriesRef>,
        shards: &mut Shards<'_, WalWorker<'_>>,
        missing: &mut HashSet<HeadSeriesRef>,
        unknown_exemplars: &mut u64,
        unknown_metadata: &mut u64,
        unknown_tombstones: &mut u64,
    ) -> Result<(), Error> {
        let concurrency = shards.len() as u64;
        let mut dec = record::Decoder::new(self.logger.clone());
        let mut series_buf: Vec<RefSeries> = Vec::new();
        let mut samples_buf: Vec<RefSample> = Vec::new();
        let mut tstones_buf: Vec<Stone> = Vec::new();
        let mut exemplars_buf: Vec<RefExemplar> = Vec::new();
        let mut hists_buf: Vec<RefHistogramSample> = Vec::new();
        let mut fhists_buf: Vec<RefFloatHistogramSample> = Vec::new();
        let mut meta_buf: Vec<RefMetadata> = Vec::new();
        let exemplars = self.exemplars();

        while r.next() {
            let rec = r.record();
            match dec.r#type(rec) {
                Type::Series => {
                    series_buf.clear();
                    if let Err(e) = dec.series(rec, &mut series_buf) {
                        return Err(corruption("decode series", e, r));
                    }
                    for ws in series_buf.drain(..) {
                        let (ms, created) =
                            self.get_or_create_with_optional_id(ws.r#ref, ws.labels.hash(), &ws.labels, false)?;
                        if self.last_series_id.load(Ordering::SeqCst) < ws.r#ref {
                            self.last_series_id.store(ws.r#ref, Ordering::SeqCst);
                        }
                        if !created {
                            multi_ref.insert(ws.r#ref, ms.r#ref);
                        }
                        let idx = (ms.r#ref % concurrency) as usize;
                        shards.send(
                            idx,
                            ReplayItem::Series {
                                series: ms,
                                wal_ref: ws.r#ref,
                            },
                        );
                    }
                }
                Type::Samples | Type::SamplesV2 => {
                    samples_buf.clear();
                    if let Err(e) = dec.samples(rec, &mut samples_buf) {
                        return Err(corruption("decode samples", e, r));
                    }
                    let min_valid_time = self.min_valid_time.load(Ordering::SeqCst);
                    shards.dispatch_samples(&samples_buf, |s| {
                        if s.t < min_valid_time {
                            return false; // Before minValidTime: discard.
                        }
                        if let Some(&m) = multi_ref.get(&s.r#ref) {
                            // A sample for a duplicate series: keep the series record at least until this
                            // record's timestamp.
                            self.update_wal_expiry(s.r#ref, s.t);
                            s.r#ref = m;
                        }
                        true
                    });
                }
                Type::Tombstones => {
                    tstones_buf.clear();
                    if let Err(e) = dec.tombstones(rec, &mut tstones_buf) {
                        return Err(corruption("decode tombstones", e, r));
                    }
                    let mut delete_shards: Vec<Vec<HeadSeriesRef>> = vec![Vec::new(); concurrency as usize];
                    for s in tstones_buf.iter_mut() {
                        // A tombstone means this ref was previously allocated, even if its series record is no
                        // longer in the WAL. Advance lastSeriesID so the ref is not reissued.
                        if self.last_series_id.load(Ordering::SeqCst) < s.r#ref {
                            self.last_series_id.store(s.r#ref, Ordering::SeqCst);
                        }
                        if s.intervals.len() == 1 && s.intervals[0].mint == i64::MIN && s.intervals[0].maxt == i64::MAX
                        {
                            // This series was fully deleted at this point.
                            let mut sref = s.r#ref;
                            if let Some(&m) = multi_ref.get(&sref) {
                                sref = m;
                            }
                            if let Some(series) = self.series.get_by_id(sref) {
                                // Remove the series from the hash index so that a later series record with the
                                // same labels creates a fresh series. It stays in the by-ref map so already
                                // queued samples still resolve until the deletion applies.
                                self.series.unlink_hash(series.lset.hash(), sref);
                                delete_shards[(sref % concurrency) as usize].push(sref);
                            }
                            continue;
                        }
                        for itv in s.intervals.iter() {
                            if itv.maxt < self.min_valid_time.load(Ordering::SeqCst) {
                                continue;
                            }
                            if let Some(&m) = multi_ref.get(&s.r#ref) {
                                // A tombstone for a duplicate series: keep the series record at least until this
                                // record's timestamp.
                                self.update_wal_expiry(s.r#ref, itv.maxt);
                                s.r#ref = m;
                            }
                            if self.series.get_by_id(s.r#ref).is_none() {
                                *unknown_tombstones += 1;
                                missing.insert(s.r#ref);
                                continue;
                            }
                            self.tombstones.add_interval(s.r#ref, std::slice::from_ref(itv));
                        }
                    }
                    for (i, d) in delete_shards.into_iter().enumerate() {
                        if !d.is_empty() {
                            shards.send(i, ReplayItem::Deleted(d));
                        }
                    }
                }
                Type::Exemplars => {
                    exemplars_buf.clear();
                    if let Err(e) = dec.exemplars(rec, &mut exemplars_buf) {
                        return Err(corruption("decode exemplars", e, r));
                    }
                    let min_valid_time = self.min_valid_time.load(Ordering::SeqCst);
                    for e in exemplars_buf.drain(..) {
                        if e.t < min_valid_time {
                            continue;
                        }
                        let mut eref = e.r#ref;
                        if let Some(&m) = multi_ref.get(&eref) {
                            self.update_wal_expiry(eref, e.t);
                            eref = m;
                        }
                        let Some(ms) = self.series.get_by_id(eref) else {
                            *unknown_exemplars += 1;
                            missing.insert(eref);
                            continue;
                        };
                        // The only possible error here is out of order exemplars, which we shouldn't see when
                        // replaying the WAL, so lets just log the error if it's that type.
                        let ex = Exemplar {
                            labels: e.labels,
                            value: e.v,
                            ts: e.t,
                            has_ts: false,
                        };
                        match exemplars.add_exemplar(&ms.lset, &ex) {
                            Ok(()) => ms.update_exemplar_timestamp(e.t),
                            Err(err) if err.is(&ERR_OUT_OF_ORDER_EXEMPLAR) => self.logger.warn(
                                "Unexpected error when replaying WAL on exemplar record",
                                &[("err", &err)],
                            ),
                            Err(_) => {}
                        }
                    }
                }
                Type::HistogramSamples | Type::CustomBucketsHistogramSamples | Type::HistogramSamplesV2 => {
                    hists_buf.clear();
                    if let Err(e) = dec.histogram_samples(rec, &mut hists_buf) {
                        return Err(corruption("decode histograms", e, r));
                    }
                    let min_valid_time = self.min_valid_time.load(Ordering::SeqCst);
                    shards.dispatch_hists(&mut hists_buf, |s| {
                        if s.t < min_valid_time {
                            return None;
                        }
                        let mut sref = s.r#ref;
                        if let Some(&m) = multi_ref.get(&sref) {
                            self.update_wal_expiry(sref, s.t);
                            sref = m;
                        }
                        Some(HistogramRecord {
                            r#ref: sref,
                            st: s.st,
                            t: s.t,
                            h: Some(s.h),
                            fh: None,
                        })
                    });
                }
                Type::FloatHistogramSamples
                | Type::CustomBucketsFloatHistogramSamples
                | Type::FloatHistogramSamplesV2 => {
                    fhists_buf.clear();
                    if let Err(e) = dec.float_histogram_samples(rec, &mut fhists_buf) {
                        return Err(corruption("decode float histograms", e, r));
                    }
                    let min_valid_time = self.min_valid_time.load(Ordering::SeqCst);
                    shards.dispatch_hists(&mut fhists_buf, |s| {
                        if s.t < min_valid_time {
                            return None;
                        }
                        let mut sref = s.r#ref;
                        if let Some(&m) = multi_ref.get(&sref) {
                            self.update_wal_expiry(sref, s.t);
                            sref = m;
                        }
                        Some(HistogramRecord {
                            r#ref: sref,
                            st: s.st,
                            t: s.t,
                            h: None,
                            fh: Some(s.fh),
                        })
                    });
                }
                Type::Metadata => {
                    meta_buf.clear();
                    if let Err(e) = dec.metadata(rec, &mut meta_buf) {
                        return Err(corruption("decode metadata", e, r));
                    }
                    for m in meta_buf.drain(..) {
                        let mut mref = m.r#ref;
                        if let Some(&x) = multi_ref.get(&mref) {
                            mref = x;
                        }
                        let Some(s) = self.series.get_by_id(mref) else {
                            *unknown_metadata += 1;
                            missing.insert(mref);
                            continue;
                        };
                        s.lock().ext_mut().meta = metadata::Handle::make(Metadata {
                            typ: record::to_metric_type(m.r#type),
                            unit: m.unit,
                            help: m.help,
                        });
                    }
                }
                _ => {
                    // Unknown records are ignored, enabling users to roll back.
                }
            }
        }
        Ok(())
    }

    /// Go `loadWBL`: replays the out-of-order records of `r`.
    pub(crate) fn load_wbl(
        &self,
        r: &mut wlog::Reader<SegmentBufReader>,
        multi_ref: &mut HashMap<HeadSeriesRef, HeadSeriesRef>,
        last_mmap_ref: ChunkDiskMapperRef,
    ) -> Result<(), Error> {
        let concurrency = replay_concurrency(self);
        let ooo_cap_max = self.opts.out_of_order_cap_max.load(Ordering::Relaxed);
        let procs: Vec<WblWorker<'_>> = (0..concurrency)
            .map(|_| WblWorker {
                head: self,
                opts: self.replay_chunk_opts(),
                ooo_cap_max,
                missing: HashSet::new(),
                unknown_samples: 0,
                unknown_histograms: 0,
                mint: i64::MAX,
                maxt: i64::MIN,
            })
            .collect();
        let mut missing: HashSet<HeadSeriesRef> = HashSet::new();
        let mut pinned: HashSet<HeadSeriesRef> = HashSet::new();
        let mut unknown_markers = 0u64;

        let (res, stats) = std::thread::scope(|scope| {
            let mut shards = Shards::new(scope, procs);
            let res =
                self.load_wbl_records(r, multi_ref, last_mmap_ref, &mut shards, &mut missing, &mut pinned, &mut unknown_markers);
            (res, shards.finish())
        });
        // References that this WBL resolves through multiRef. Their series records must stay in the WAL after
        // the next checkpoint.
        self.pin_wbl_series_refs(&pinned);
        // We wrap a corruption to identify OOO WBL corruption.
        res.map_err(|e| Error::other(ErrLoadWbl(e)))?;
        if let Some(e) = r.err() {
            return Err(Error::wrap("read records", wlog_err(e)));
        }

        let (mut unknown_samples, mut unknown_histograms) = (0u64, 0u64);
        for s in stats {
            missing.extend(s.missing);
            unknown_samples += s.unknown_samples;
            unknown_histograms += s.unknown_histograms;
        }
        if unknown_samples + unknown_histograms + unknown_markers > 0 {
            self.logger.warn(
                "Unknown series references for ooo WAL replay",
                &[
                    ("series", &missing.len()),
                    ("samples", &unknown_samples),
                    ("histograms", &unknown_histograms),
                    ("mmap_markers", &unknown_markers),
                ],
            );
            let c = &self.metrics.wbl_replay_unknown_refs_total;
            add_non_zero(c, missing.len() as u64, "series");
            add_non_zero(c, unknown_samples, "samples");
            add_non_zero(c, unknown_histograms, "histograms");
            add_non_zero(c, unknown_markers, "mmap_markers");
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn load_wbl_records(
        &self,
        r: &mut wlog::Reader<SegmentBufReader>,
        multi_ref: &HashMap<HeadSeriesRef, HeadSeriesRef>,
        last_mmap_ref: ChunkDiskMapperRef,
        shards: &mut Shards<'_, WblWorker<'_>>,
        missing: &mut HashSet<HeadSeriesRef>,
        pinned: &mut HashSet<HeadSeriesRef>,
        unknown_markers: &mut u64,
    ) -> Result<(), Error> {
        let concurrency = shards.len() as u64;
        let (last_seq, last_off) = last_mmap_ref.unpack();
        let dec = record::Decoder::new(self.logger.clone());
        let mut samples_buf: Vec<RefSample> = Vec::new();
        let mut markers_buf: Vec<RefMmapMarker> = Vec::new();
        let mut hists_buf: Vec<RefHistogramSample> = Vec::new();
        let mut fhists_buf: Vec<RefFloatHistogramSample> = Vec::new();
        while r.next() {
            let rec = r.record();
            match dec.r#type(rec) {
                Type::Samples | Type::SamplesV2 => {
                    samples_buf.clear();
                    if let Err(e) = dec.samples(rec, &mut samples_buf) {
                        return Err(corruption("decode samples", e, r));
                    }
                    shards.dispatch_samples(&samples_buf, |s| {
                        if let Some(&m) = multi_ref.get(&s.r#ref) {
                            pinned.insert(s.r#ref);
                            s.r#ref = m;
                        }
                        true
                    });
                }
                Type::MmapMarkers => {
                    markers_buf.clear();
                    if let Err(e) = dec.mmap_markers(rec, &mut markers_buf) {
                        return Err(corruption("decode mmap markers", e, r));
                    }
                    for rm in markers_buf.iter() {
                        let (seq, off) = rm.mmap_ref.unpack();
                        if seq > last_seq || (seq == last_seq && off > last_off) {
                            // This m-map chunk from markers was not present during the load of mmapped chunks
                            // that happened in the head initialization.
                            continue;
                        }
                        let mut mref = rm.r#ref;
                        if let Some(&m) = multi_ref.get(&mref) {
                            pinned.insert(mref);
                            mref = m;
                        }
                        let Some(ms) = self.series.get_by_id(mref) else {
                            *unknown_markers += 1;
                            missing.insert(mref);
                            continue;
                        };
                        let idx = (ms.r#ref % concurrency) as usize;
                        shards.send(idx, ReplayItem::Mmapped(ms));
                    }
                }
                Type::HistogramSamples | Type::CustomBucketsHistogramSamples | Type::HistogramSamplesV2 => {
                    hists_buf.clear();
                    if let Err(e) = dec.histogram_samples(rec, &mut hists_buf) {
                        return Err(corruption("decode histograms", e, r));
                    }
                    shards.dispatch_hists(&mut hists_buf, |s| {
                        let mut sref = s.r#ref;
                        if let Some(&m) = multi_ref.get(&sref) {
                            pinned.insert(sref);
                            sref = m;
                        }
                        Some(HistogramRecord {
                            r#ref: sref,
                            st: s.st,
                            t: s.t,
                            h: Some(s.h),
                            fh: None,
                        })
                    });
                }
                Type::FloatHistogramSamples
                | Type::CustomBucketsFloatHistogramSamples
                | Type::FloatHistogramSamplesV2 => {
                    fhists_buf.clear();
                    if let Err(e) = dec.float_histogram_samples(rec, &mut fhists_buf) {
                        return Err(corruption("decode float histograms", e, r));
                    }
                    shards.dispatch_hists(&mut fhists_buf, |s| {
                        let mut sref = s.r#ref;
                        if let Some(&m) = multi_ref.get(&sref) {
                            pinned.insert(sref);
                            sref = m;
                        }
                        Some(HistogramRecord {
                            r#ref: sref,
                            st: s.st,
                            t: s.t,
                            h: None,
                            fh: Some(s.fh),
                        })
                    });
                }
                _ => {}
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------------------------------------------
    // Chunk snapshots.

    /// Go `ChunkSnapshot`: creates a snapshot of all the series and tombstones in the head. It deletes the old
    /// chunk snapshots if the chunk snapshot creation is successful.
    ///
    /// The snapshot is the directory `chunk_snapshot.N.M` written with the WAL format; N is the last WAL segment
    /// present during snapshotting and M is the offset in segment N up to which data was written. It contains
    /// all series (one record each, not sorted), then the tombstones (one record), then the exemplars (>= 1
    /// record) in the order they were written to the circular buffer.
    pub fn chunk_snapshot(&self) -> Result<ChunkSnapshotStats, Error> {
        let Some(wal) = &self.wal else {
            // If we are not storing any WAL, does not make sense to take a snapshot too.
            self.logger.warn("skipping chunk snapshotting as WAL is disabled", &[]);
            return Ok(ChunkSnapshotStats::default());
        };
        let _g = lock(&self.chunk_snapshot_mtx);
        let mut stats = ChunkSnapshotStats::default();

        let (wlast, woffset) = match wal.last_segment_and_offset() {
            Ok(x) => x,
            Err(e) if e.is_not_found() => (0, 0),
            Err(e) => return Err(Error::wrap("get last wal segment and offset", wlog_err(e))),
        };
        let (cslast, csoffset) = match last_chunk_snapshot(&self.opts.chunk_dir_root) {
            Ok((_, i, o)) => (i, o),
            Err(e) if e.is(&ERR_NOT_FOUND) => (0, 0),
            Err(e) => return Err(Error::wrap("find last chunk snapshot", e)),
        };
        if wlast == cslast && woffset == csoffset {
            // Nothing has been written to the WAL/Head since the last snapshot.
            return Ok(stats);
        }

        let cpdir = self.opts.chunk_dir_root.join(chunk_snapshot_dir(wlast, woffset));
        let mut tmp = cpdir.clone().into_os_string();
        tmp.push(".tmp");
        let cpdirtmp = PathBuf::from(tmp);
        stats.dir = cpdir.display().to_string();

        std::fs::create_dir_all(&cpdirtmp).map_err(|e| {
            Error::msg(format!("create chunk snapshot dir: mkdir {}: {}", cpdirtmp.display(), crate::io_err_text(&e)))
        })?;
        let res = self.write_chunk_snapshot(&cpdirtmp, &cpdir, wal.compression_type(), &mut stats);
        // Ensures that an early return caused by an error doesn't leave any tmp files.
        let _ = std::fs::remove_dir_all(&cpdirtmp);
        res?;

        if let Err(e) = delete_chunk_snapshots(&self.opts.chunk_dir_root, wlast, woffset) {
            // Leftover old chunk snapshots do not cause problems down the line beyond occupying disk space.
            self.logger.error("delete old chunk snapshots", &[("err", &e)]);
        }
        Ok(stats)
    }

    fn write_chunk_snapshot(
        &self,
        cpdirtmp: &Path,
        cpdir: &Path,
        compression: wlog::compression::Type,
        stats: &mut ChunkSnapshotStats,
    ) -> Result<(), Error> {
        let cp = WL::new(cpdirtmp, compression).map_err(|e| Error::wrap("open chunk snapshot", wlog_err(e)))?;
        let res = (|| -> Result<(), Error> {
            let flush = |recs: &mut Vec<Vec<u8>>| -> Result<(), Error> {
                cp.log_batch(recs).map_err(|e| Error::wrap("flush records", wlog_err(e)))?;
                recs.clear();
                Ok(())
            };
            // Add all series to the snapshot. The records are flushed in 10 MB increments.
            let mut recs: Vec<Vec<u8>> = Vec::new();
            let mut size = 0usize;
            for i in 0..self.series.size {
                let shard = self.series.shard_series(i);
                for s in &shard {
                    let mut b = Vec::new();
                    encode_to_snapshot_record(s, &mut b);
                    size += b.len();
                    recs.push(b);
                    if size > 10 * 1024 * 1024 {
                        flush(&mut recs)?;
                        size = 0;
                    }
                }
                stats.total_series += shard.len();
            }
            // Add tombstones to the snapshot.
            let tr = self.tombstones();
            let rec = encode_tombstones_to_snapshot_record(&*tr).map_err(|e| Error::wrap("encode tombstones", e))?;
            recs.push(rec);
            // Flush remaining series records and tombstones.
            flush(&mut recs)?;

            // Add exemplars in the snapshot, in batches of up to 10000 exemplars per record.
            const MAX_EXEMPLARS_PER_RECORD: usize = 10000;
            let mut batch: Vec<RefExemplar> = Vec::new();
            let enc = record::Encoder {
                enable_st_storage: self.opts.enable_st_storage.load(Ordering::Relaxed),
            };
            let mut buf: Vec<u8> = Vec::new();
            let mut flush_exemplars = |batch: &mut Vec<RefExemplar>| -> Result<(), Error> {
                if batch.is_empty() {
                    return Ok(());
                }
                buf.clear();
                buf.push(CHUNK_SNAPSHOT_RECORD_TYPE_EXEMPLARS);
                enc.encode_exemplars_into_buffer(batch, &mut buf);
                cp.log(&buf).map_err(|e| Error::wrap("log exemplars", wlog_err(e)))?;
                batch.clear();
                Ok(())
            };
            self.exemplars()
                .iterate_exemplars(&mut |lset, e| {
                    if batch.len() >= MAX_EXEMPLARS_PER_RECORD {
                        flush_exemplars(&mut batch).map_err(|e| Error::wrap("flush exemplars", e))?;
                    }
                    let Some(ms) = self.series.get_by_hash(lset.hash(), lset) else {
                        // It is possible that exemplar refers to some old series. We discard such exemplars.
                        return Ok(());
                    };
                    batch.push(RefExemplar {
                        r#ref: ms.r#ref,
                        t: e.ts,
                        v: e.value,
                        labels: e.labels.clone(),
                    });
                    Ok(())
                })
                .map_err(|e| Error::wrap("iterate exemplars", e))?;
            // Flush remaining exemplars.
            flush_exemplars(&mut batch).map_err(|e| Error::wrap("flush exemplars at the end", e))?;
            Ok(())
        })();
        let close = cp.close();
        res?;
        close.map_err(|e| Error::wrap("close chunk snapshot", wlog_err(e)))?;
        tsdb_format::fileutil::replace(cpdirtmp, cpdir).map_err(|e| {
            Error::msg(format!("rename chunk snapshot directory: {}", crate::io_err_text(&e)))
        })?;
        Ok(())
    }

    /// Go `performChunkSnapshot`.
    pub(crate) fn perform_chunk_snapshot(&self) -> Result<(), Error> {
        self.logger.info("creating chunk snapshot", &[]);
        let start = Instant::now();
        let stats = self.chunk_snapshot().map_err(|e| Error::wrap("chunk snapshot", e))?;
        self.logger.info(
            "chunk snapshot complete",
            &[
                ("duration", &format!("{:?}", start.elapsed())),
                ("num_series", &stats.total_series),
                ("dir", &stats.dir),
            ],
        );
        Ok(())
    }

    /// Go `loadChunkSnapshot`: replays the newest chunk snapshot and restores the head state from it. Returns
    /// (WAL index, WAL offset, series by reference). On error, the caller must clear the head.
    #[allow(clippy::type_complexity)]
    pub(crate) fn load_chunk_snapshot(&self) -> Result<(i64, i64, HashMap<HeadSeriesRef, Arc<MemSeries>>), Error> {
        let (dir, snap_idx, snap_offset) = match last_chunk_snapshot(&self.opts.chunk_dir_root) {
            Ok(x) => x,
            Err(e) if e.is(&ERR_NOT_FOUND) => return Ok((-1, 0, HashMap::new())),
            Err(e) => return Err(Error::wrap("find last chunk snapshot", e)),
        };
        let start = Instant::now();
        let sr = wlog::new_segments_reader(&dir).map_err(|e| Error::wrap("open chunk snapshot", wlog_err(e)))?;
        let mut r = wlog::Reader::new(sr);
        let mut dec = record::Decoder::new(self.logger.clone());
        let mut ref_series: HashMap<HeadSeriesRef, Arc<MemSeries>> = HashMap::new();
        let mut exemplar_buf: Vec<RefExemplar> = Vec::new();
        let mut num_series = 0usize;
        let mut unknown_refs = 0i64;
        let exemplars_enabled =
            self.opts.enable_exemplar_storage && self.opts.max_exemplars.load(Ordering::Relaxed) > 0;

        let mut loop_err: Option<Error> = None;
        while r.next() {
            let rec = r.record();
            let res: Result<(), Error> = match rec.first().copied().unwrap_or(0) {
                CHUNK_SNAPSHOT_RECORD_TYPE_SERIES => {
                    num_series += 1;
                    match decode_series_from_chunk_snapshot(&mut dec, rec) {
                        Ok(csr) => self
                            .load_snapshot_series(csr, &mut ref_series)
                            .map_err(|e| Error::wrap("record processing", e)),
                        Err(e) => Err(Error::wrap("decode loop", Error::wrap("decode series record", e))),
                    }
                }
                CHUNK_SNAPSHOT_RECORD_TYPE_TOMBSTONES => match decode_tombstones_snapshot_record(rec) {
                    Ok(tr) => {
                        let _ = tr.iter(&mut |r, ivs| {
                            self.tombstones.add_interval(r, ivs);
                            Ok(())
                        });
                        Ok(())
                    }
                    Err(e) => Err(Error::wrap("decode loop", Error::wrap("decode tombstones", e))),
                },
                CHUNK_SNAPSHOT_RECORD_TYPE_EXEMPLARS => {
                    // Exemplars are at the end of snapshot. So all series are loaded at this point.
                    if !exemplars_enabled {
                        continue;
                    }
                    let mut decbuf = Decbuf::new(&rec[1..]);
                    exemplar_buf.clear();
                    match dec.exemplars_from_buffer(&mut decbuf, &mut exemplar_buf) {
                        Err(e) => Err(Error::wrap(
                            "decode loop",
                            Error::msg(format!("exemplars from buffer: {e}")),
                        )),
                        Ok(()) => {
                            let es = self.exemplars();
                            let mut res = Ok(());
                            for e in exemplar_buf.drain(..) {
                                let Some(ms) = ref_series.get(&e.r#ref) else {
                                    unknown_refs += 1;
                                    continue;
                                };
                                let ex = Exemplar {
                                    labels: e.labels,
                                    value: e.v,
                                    ts: e.t,
                                    has_ts: false,
                                };
                                if let Err(err) = es.add_exemplar(&ms.lset, &ex) {
                                    res = Err(Error::wrap("decode loop", Error::wrap("add exemplar", err)));
                                    break;
                                }
                                ms.update_exemplar_timestamp(e.t);
                            }
                            res
                        }
                    }
                }
                b => {
                    // A record type we don't understand: an old format from earlier versions, or a new format
                    // and the code was rolled back to old version.
                    Err(Error::wrap("decode loop", Error::msg(format!("unsupported snapshot record type 0b{:b}", b))))
                }
            };
            if let Err(e) = res {
                loop_err = Some(e);
                break;
            }
        }
        if let Some(e) = loop_err {
            return Err(e);
        }
        if let Some(e) = r.err() {
            return Err(Error::wrap("read records", wlog_err(e)));
        }
        self.logger.info(
            "chunk snapshot loaded",
            &[
                ("dir", &dir.display()),
                ("num_series", &num_series),
                ("duration", &format!("{:?}", start.elapsed())),
            ],
        );
        if unknown_refs > 0 {
            self.logger.warn("unknown series references during chunk snapshot replay", &[("count", &unknown_refs)]);
        }
        Ok((snap_idx, snap_offset, ref_series))
    }

    fn load_snapshot_series(
        &self,
        csr: ChunkSnapshotRecord,
        ref_series: &mut HashMap<HeadSeriesRef, Arc<MemSeries>>,
    ) -> Result<(), Error> {
        let (series, _) = self.get_or_create_with_optional_id(csr.r#ref, csr.lset.hash(), &csr.lset, false)?;
        ref_series.insert(csr.r#ref, series.clone());
        self.last_series_id.fetch_max(series.r#ref, Ordering::SeqCst);
        let Some((mc, app)) = csr.mc else {
            return Ok(());
        };
        let (mint, maxt) = (mc.min_time, mc.max_time);
        let mut g = series.lock();
        g.next_at = mc.max_time; // This will create a new chunk on append.
        g.head_chunks = vec![mc];
        g.last_value = csr.last_value;
        g.set_last_histogram_values(csr.last_histogram_value, csr.last_float_histogram_value);
        let (stale, is_hist, buckets) = g.sample_state();
        if stale {
            self.num_stale_series.fetch_add(1, Ordering::SeqCst);
        }
        if is_hist {
            self.num_native_histogram_series.fetch_add(1, Ordering::SeqCst);
            self.num_native_histogram_buckets.fetch_add(buckets as u64, Ordering::SeqCst);
        }
        g.app = SeriesAppender::from_appender(app);
        drop(g);
        self.update_min_max_time(mint, maxt);
        Ok(())
    }

    // -----------------------------------------------------------------------------------------------------------
    // Series state file (the `fast-startup` option).

    /// Go `readSeriesStateFile`. A missing file gives an error with "no such file or directory".
    pub fn read_series_state_file(&self) -> Result<SeriesLifecycleState, Error> {
        let Some(wal) = &self.wal else {
            return Err(Error::msg("open series_state.json: no such file or directory"));
        };
        let path = wal.dir().join(SERIES_STATE_FILENAME);
        let b = std::fs::read(&path)
            .map_err(|e| Error::msg(format!("open {}: {}", path.display(), crate::io_err_text(&e))))?;
        let j = json::Json::parse_bytes(&b).map_err(|e| Error::msg(format!("decode series state: {e}")))?;
        let mut st = SeriesLifecycleState::default();
        if let Some(v) = j.get("last_series_id") {
            st.last_series_id = v.as_f64().map_or(0, |f| f as u64);
            if let Some(i) = v.as_i64() {
                st.last_series_id = i as u64;
            }
        }
        if let Some(v) = j.get("last_wal_segment").and_then(|v| v.as_i64()) {
            st.last_wal_segment = v;
        }
        if let Some(v) = j.get("clean_shutdown").and_then(|v| v.as_bool()) {
            st.clean_shutdown = v;
        }
        Ok(st)
    }

    /// Go `writeSeriesState`: atomically writes the current series state to disk.
    pub(crate) fn write_series_state(&self, clean_shutdown: bool) {
        let Some(wal) = &self.wal else {
            return;
        };
        // Find the last segment number by checking the wal/ directory.
        let last = match wal.last_segment_and_offset() {
            Ok((l, _)) => l,
            Err(e) => {
                self.logger.warn("Failed to get WAL segments for series state", &[("err", &e)]);
                -1
            }
        };
        let text = format!(
            "{{\"last_series_id\":{},\"last_wal_segment\":{},\"clean_shutdown\":{}}}\n",
            self.last_series_id.load(Ordering::SeqCst),
            last,
            clean_shutdown
        );
        let path = wal.dir().join(SERIES_STATE_FILENAME);
        let mut tmp = path.clone().into_os_string();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        if let Err(e) = std::fs::write(&tmp, text) {
            self.logger.error("Failed to create temp series state file", &[("err", &crate::io_err_text(&e))]);
            return;
        }
        if let Err(e) = std::fs::rename(&tmp, &path) {
            self.logger
                .error("Failed to rename the temporary series state file", &[("err", &crate::io_err_text(&e))]);
        }
    }

    /// Go `runSeriesStateTicker`: starts a thread that writes the series state to disk every second, until
    /// [`Head::stop_series_state_ticker`]. Only used with the `fast-startup` option.
    pub(crate) fn run_series_state_ticker(&self) {
        let (tx, rx) = mpsc::channel::<()>();
        let me = self.arc();
        let weak = Arc::downgrade(&me);
        drop(me);
        let handle = std::thread::Builder::new()
            .name("series-state".into())
            .spawn(move || {
                while let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(Duration::from_secs(1)) {
                    match weak.upgrade() {
                        Some(h) => h.write_series_state(false),
                        None => return,
                    }
                }
            })
            .expect("spawn series state thread");
        *lock(&self.series_state_quit) = Some((tx, handle));
    }

    /// Stops the series state thread (Go `close(h.seriesStateQuit); h.seriesStateWg.Wait()`). Returns whether
    /// it was running.
    pub(crate) fn stop_series_state_ticker(&self) -> bool {
        let Some((tx, handle)) = lock(&self.series_state_quit).take() else {
            return false;
        };
        drop(tx);
        if handle.thread().id() != std::thread::current().id() {
            let _ = handle.join();
        }
        true
    }

    /// Go `findLastSeriesID`: a bounded reverse scan of WAL segments to find the highest series ID.
    pub fn find_last_series_id(&self, state: &SeriesLifecycleState, end_segment: i64) -> Result<u64, Error> {
        let Some(wal) = &self.wal else {
            return Ok(state.last_series_id);
        };
        let start_segment = state.last_wal_segment.max(0);
        let mut dec = record::Decoder::new(self.logger.clone());
        let mut series: Vec<RefSeries> = Vec::new();
        // Iterate backwards from the newest segment to the oldest allowed segment.
        let mut i = end_segment;
        while i >= start_segment {
            let s = match wlog::open_read_segment(&wlog::segment_name(wal.dir(), i)) {
                Ok(s) => s,
                Err(e) if e.io_error().is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound) => {
                    i -= 1;
                    continue; // Segment might have been deleted, we skip it.
                }
                Err(e) => return Err(Error::wrap(format!("open WAL segment {i}"), wlog_err(e))),
            };
            let mut r = wlog::Reader::new(wlog::new_segment_buf_reader(vec![s]));
            let mut highest: HeadSeriesRef = 0;
            let mut found = false;
            // Read the segment forwards.
            while r.next() {
                let rec = r.record();
                if dec.r#type(rec) != Type::Series {
                    continue;
                }
                series.clear();
                dec.series(rec, &mut series)
                    .map_err(|e| Error::msg(format!("decode series in segment {i}: {e}")))?;
                for ws in &series {
                    highest = highest.max(ws.r#ref);
                    found = true;
                }
            }
            if let Some(e) = r.err() {
                return Err(Error::wrap(format!("read WAL segment {i}"), wlog_err(e)));
            }
            if found {
                return Ok(highest);
            }
            i -= 1;
        }
        // If we scanned the segments and found no series records, the ID from our state file has to be used.
        Ok(state.last_series_id)
    }
}
