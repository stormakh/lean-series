//! The head block: in-memory series, their chunks, garbage collection and truncation (port of `tsdb/head.go`).
//!
//! Memory design (see lib.rs):
//! - A series is an `Arc<MemSeries>`: its reference, its labels (one shared buffer) and a mutex around the
//!   mutable part ([`MemSeriesInner`]). Rarely used parts (out-of-order chunks, metadata, the last histogram
//!   values, exemplar times) are behind one `Option<Box<..>>`, so a plain float series stays small.
//! - Head chunks are a vector ordered oldest first (Go: a linked list, newest first). The open (newest) chunk is
//!   never shared with readers; readers get a copy of its bytes under the series lock. Closed chunks are shared
//!   through their `Arc`.
//! - The series maps are a small number of shards (a power of two near 4x the CPU count, at most
//!   `HeadOptions::stripe_size`) whose hash maps grow with demand.

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak};
use std::time::{Duration, Instant};

use chunkenc::{Chunk, Encoding, Pool};
use model::histogram::{FloatHistogram, Histogram};
use model::labels::Labels;
use model::matcher::Matcher;
use model::metadata;
use model::value::is_stale_nan;
use storage::error::Sentinel;
use storage::{Context, Error, SeriesRef};
use tsdb_format::chunks::{ChunkDiskMapper, ChunkDiskMapperRef};
use tsdb_format::index::{MemPostings, PostingsStats};
use tsdb_format::record;
use tsdb_format::tombstones::{self, Interval, MemTombstones, Stone};
use tsdb_format::wlog::{self, WL, WriteNotified};

use crate::exemplar::{CircularExemplarStorage, ExemplarMetrics};
use crate::isolation::{Isolation, TxRing};
use crate::meta::{BlockMeta, BlockStats, Ulid};
use crate::metrics::{Counter, Gauge, Histogram as MetricHistogram};
use crate::ooo_head::OooChunk;
use crate::ooo_isolation::OooIsolation;
use crate::reader::{BlockReader, ChunkReader, IndexReader};
use crate::Logger;

/// Go `chunks.HeadSeriesRef`.
pub type HeadSeriesRef = u64;
/// Go `chunks.HeadChunkID`.
pub type HeadChunkId = u64;

static INVALID_SAMPLE: Sentinel = Sentinel::new("invalid sample");
/// Go `ErrInvalidSample`: an appended sample is not valid and can't be ingested.
pub const ERR_INVALID_SAMPLE: Error = Error::Sentinel(&INVALID_SAMPLE);
static INVALID_EXEMPLAR: Sentinel = Sentinel::new("invalid exemplar");
/// Go `ErrInvalidExemplar`.
pub const ERR_INVALID_EXEMPLAR: Error = Error::Sentinel(&INVALID_EXEMPLAR);
static APPENDER_CLOSED: Sentinel = Sentinel::new("appender closed");
/// Go `ErrAppenderClosed`: the appender has already been committed or rolled back.
pub const ERR_APPENDER_CLOSED: Error = Error::Sentinel(&APPENDER_CLOSED);

/// Go `DefaultOutOfOrderCapMax`.
pub const DEFAULT_OUT_OF_ORDER_CAP_MAX: i64 = 32;
/// Go `DefaultSamplesPerChunk`.
pub const DEFAULT_SAMPLES_PER_CHUNK: usize = 120;
/// Go `DefaultStripeSize`. Here it is the upper bound of the number of shards of the series maps.
pub const DEFAULT_STRIPE_SIZE: usize = 1 << 14;
/// Go `DefaultBlockDuration` (2h in milliseconds).
pub const DEFAULT_BLOCK_DURATION: i64 = 2 * 60 * 60 * 1000;
/// Go `oooChunkIDMask`.
pub const OOO_CHUNK_ID_MASK: u64 = 1 << 23;

pub(crate) const SAMPLE_METRIC_TYPE_FLOAT: &str = "float";
pub(crate) const SAMPLE_METRIC_TYPE_HISTOGRAM: &str = "histogram";

const CARDINALITY_CACHE_EXPIRATION_TIME: Duration = Duration::from_secs(30);

pub(crate) fn chunks_err(e: tsdb_format::chunks::Error) -> Error {
    Error::other(e)
}

pub(crate) fn wlog_err(e: wlog::Error) -> Error {
    Error::other(e)
}

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn rlock<T>(m: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    m.read().unwrap_or_else(|e| e.into_inner())
}

fn wlock<T>(m: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    m.write().unwrap_or_else(|e| e.into_inner())
}

// ---------------------------------------------------------------------------------------------------------------
// Hash maps keyed by u64 (series references and label hashes).

/// A cheap hasher for u64 keys (one multiply, as FxHash).
#[derive(Default, Clone, Copy)]
pub struct U64Hasher(u64);

impl Hasher for U64Hasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0.rotate_left(5) ^ b as u64).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
        }
    }
    #[inline]
    fn write_u64(&mut self, x: u64) {
        self.0 = (self.0.rotate_left(5) ^ x).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
}

/// A hash map with u64 keys.
pub type U64Map<V> = HashMap<u64, V, BuildHasherDefault<U64Hasher>>;

// ---------------------------------------------------------------------------------------------------------------
// Options, stats and callbacks.

/// Go `SeriesLifecycleCallback`: callbacks during the life of a series. Always a no-op in Prometheus.
pub trait SeriesLifecycleCallback: Send + Sync {
    /// Called before creating a series; an error means the series must not be created.
    fn pre_creation(&self, l: &Labels) -> Result<(), Error>;
    /// Called after creating a series.
    fn post_creation(&self, l: &Labels);
    /// Called after deleting series.
    fn post_deletion(&self, deleted: &HashMap<HeadSeriesRef, Labels>);
}

/// Go `noopSeriesLifecycleCallback`.
pub struct NoopSeriesLifecycleCallback;

impl SeriesLifecycleCallback for NoopSeriesLifecycleCallback {
    fn pre_creation(&self, _: &Labels) -> Result<(), Error> {
        Ok(())
    }
    fn post_creation(&self, _: &Labels) {}
    fn post_deletion(&self, _: &HashMap<HeadSeriesRef, Labels>) {}
}

/// Go `HeadOptions`: parameters of the head block. Runtime reloadable options are atomics.
pub struct HeadOptions {
    pub max_exemplars: AtomicI64,
    pub out_of_order_time_window: AtomicI64,
    pub out_of_order_cap_max: AtomicI64,
    /// Whether the WAL/WBL store a start time per sample (the `st-storage` feature flag).
    pub enable_st_storage: AtomicBool,
    /// The encoding of new float chunks (a `chunkenc::Encoding` byte).
    pub float_chunk_encoding: AtomicU32,
    /// The ST-capable histogram encodings (`histograms-st-encoding` feature flag).
    pub enable_histogram_st_encoding: AtomicBool,

    pub chunk_range: i64,
    /// The parent directory of the `chunks_head` directory.
    pub chunk_dir_root: PathBuf,
    pub chunk_pool: Option<Arc<dyn Pool>>,
    /// The write buffer of the head chunks files. The default here is the smallest allowed size
    /// (`chunks::MIN_WRITE_BUFFER_SIZE`, 64 KiB) instead of Go's 4 MiB; the files are the same.
    pub chunk_write_buffer_size: usize,
    pub chunk_write_queue_size: usize,
    pub samples_per_chunk: usize,
    /// The maximum number of shards of the series maps (must be a power of 2). The maps themselves grow with
    /// demand.
    pub stripe_size: usize,
    pub series_callback: Option<Arc<dyn SeriesLifecycleCallback>>,
    pub enable_exemplar_storage: bool,
    pub enable_memory_snapshot_on_shutdown: bool,
    pub isolation_disabled: bool,
    /// Maximum number of threads for WAL replay; `<= 0` uses the number of CPUs.
    pub wal_replay_concurrency: i32,
    /// Enables `sharded_postings` support in the head.
    pub enable_sharding: bool,
    /// Stores the start timestamp as a zero sample before the sample (`created-timestamp-zero-ingestion`).
    pub enable_st_as_zero_sample: bool,
    /// The `metadata-wal-records` feature flag.
    pub enable_metadata_wal_records: bool,
    /// Scraping in parallel with WAL replay.
    pub enable_fast_startup: bool,
    /// The logger (Go passes it to `NewHead`).
    pub logger: Logger,
}

fn default_wal_replay_concurrency() -> i32 {
    std::thread::available_parallelism().map_or(1, |n| n.get() as i32)
}

impl Default for HeadOptions {
    /// Go `DefaultHeadOptions`.
    fn default() -> HeadOptions {
        HeadOptions {
            max_exemplars: AtomicI64::new(0),
            out_of_order_time_window: AtomicI64::new(0),
            out_of_order_cap_max: AtomicI64::new(DEFAULT_OUT_OF_ORDER_CAP_MAX),
            enable_st_storage: AtomicBool::new(false),
            float_chunk_encoding: AtomicU32::new(chunkenc::ENC_XOR.0 as u32),
            enable_histogram_st_encoding: AtomicBool::new(false),
            chunk_range: DEFAULT_BLOCK_DURATION,
            chunk_dir_root: PathBuf::new(),
            chunk_pool: None,
            chunk_write_buffer_size: tsdb_format::chunks::MIN_WRITE_BUFFER_SIZE,
            chunk_write_queue_size: tsdb_format::chunks::DEFAULT_WRITE_QUEUE_SIZE,
            samples_per_chunk: DEFAULT_SAMPLES_PER_CHUNK,
            stripe_size: DEFAULT_STRIPE_SIZE,
            series_callback: None,
            enable_exemplar_storage: false,
            enable_memory_snapshot_on_shutdown: false,
            isolation_disabled: false,
            wal_replay_concurrency: default_wal_replay_concurrency(),
            enable_sharding: false,
            enable_st_as_zero_sample: false,
            enable_metadata_wal_records: false,
            enable_fast_startup: false,
            logger: Logger::nop(),
        }
    }
}

impl HeadOptions {
    /// Go `DefaultHeadOptions`.
    pub fn new() -> HeadOptions {
        HeadOptions::default()
    }

    /// Go `UseXOR2FloatEncoding`.
    pub fn use_xor2_float_encoding(&self) -> bool {
        Encoding(self.float_chunk_encoding.load(Ordering::Relaxed) as u8) == chunkenc::ENC_XOR2
    }
}

/// Go `WALReplayStatus`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalReplayStatus {
    pub min: i64,
    pub max: i64,
    pub current: i64,
}

/// Go `HeadStats`.
#[derive(Debug, Default)]
pub struct HeadStats {
    pub wal_replay_status: RwLock<WalReplayStatus>,
}

impl HeadStats {
    /// Go `NewHeadStats`.
    pub fn new() -> Arc<HeadStats> {
        Arc::new(HeadStats::default())
    }

    /// Go `GetWALReplayStatus`.
    pub fn get_wal_replay_status(&self) -> WalReplayStatus {
        *rlock(&self.wal_replay_status)
    }
}

/// A counter with the label `type` (`float` or `histogram`), Go `*prometheus.CounterVec`.
#[derive(Debug, Default)]
pub struct TypeCounter {
    pub float: Counter,
    pub histogram: Counter,
}

impl TypeCounter {
    /// Go `WithLabelValues(typ)`.
    pub fn with_label_values(&self, typ: &str) -> &Counter {
        if typ == SAMPLE_METRIC_TYPE_HISTOGRAM {
            &self.histogram
        } else {
            &self.float
        }
    }
}

/// A counter vector with one label (Go `*prometheus.CounterVec`), for rarely used counters.
#[derive(Debug, Default)]
pub struct LabeledCounter(Mutex<Vec<(String, f64)>>);

impl LabeledCounter {
    pub fn add(&self, label: &str, v: f64) {
        let mut g = lock(&self.0);
        match g.iter_mut().find(|x| x.0 == label) {
            Some(x) => x.1 += v,
            None => g.push((label.to_string(), v)),
        }
    }
    pub fn get(&self, label: &str) -> f64 {
        lock(&self.0).iter().find(|x| x.0 == label).map_or(0.0, |x| x.1)
    }
    pub fn values(&self) -> Vec<(String, f64)> {
        lock(&self.0).clone()
    }
}

/// Go `headMetrics`. Gauges that Go computes with functions are methods of [`Head`].
#[derive(Debug)]
pub struct HeadMetrics {
    /// `prometheus_tsdb_head_active_appenders`.
    pub active_appenders: Gauge,
    /// `prometheus_tsdb_head_appenders_created_total`.
    pub appenders_created: Counter,
    /// `prometheus_tsdb_head_series_created_total`.
    pub series_created: Counter,
    /// `prometheus_tsdb_head_series_removed_total`.
    pub series_removed: Counter,
    /// `prometheus_tsdb_head_series_not_found_total`.
    pub series_not_found: Counter,
    /// `prometheus_tsdb_head_series_pending_commit_underflow_total`.
    pub pending_commit_underflow: Counter,
    /// `prometheus_tsdb_head_chunks`.
    pub chunks: Gauge,
    /// `prometheus_tsdb_head_chunks_created_total`.
    pub chunks_created: Counter,
    /// `prometheus_tsdb_head_chunks_removed_total`.
    pub chunks_removed: Counter,
    /// `prometheus_tsdb_head_gc_duration_seconds` (summary).
    pub gc_duration: MetricHistogram,
    /// `prometheus_tsdb_head_samples_appended_total{type}`.
    pub samples_appended: TypeCounter,
    /// `prometheus_tsdb_head_out_of_order_samples_appended_total{type}`.
    pub out_of_order_samples_appended: TypeCounter,
    /// `prometheus_tsdb_out_of_bound_samples_total{type}`.
    pub out_of_bound_samples: TypeCounter,
    /// `prometheus_tsdb_out_of_order_samples_total{type}`.
    pub out_of_order_samples: TypeCounter,
    /// `prometheus_tsdb_too_old_samples_total{type}`.
    pub too_old_samples: TypeCounter,
    /// `prometheus_tsdb_wal_truncate_duration_seconds` (summary).
    pub wal_truncate_duration: MetricHistogram,
    /// `prometheus_tsdb_wal_corruptions_total`.
    pub wal_corruptions_total: Counter,
    /// `prometheus_tsdb_data_replay_duration_seconds`.
    pub data_total_replay_duration: Gauge,
    /// `prometheus_tsdb_head_truncations_failed_total`.
    pub head_truncate_fail: Counter,
    /// `prometheus_tsdb_head_truncations_total`.
    pub head_truncate_total: Counter,
    /// `prometheus_tsdb_checkpoint_deletions_failed_total`.
    pub checkpoint_delete_fail: Counter,
    /// `prometheus_tsdb_checkpoint_deletions_total`.
    pub checkpoint_delete_total: Counter,
    /// `prometheus_tsdb_checkpoint_creations_failed_total`.
    pub checkpoint_creation_fail: Counter,
    /// `prometheus_tsdb_checkpoint_creations_total`.
    pub checkpoint_creation_total: Counter,
    /// `prometheus_tsdb_mmap_chunk_corruptions_total`.
    pub mmap_chunk_corruption_total: Counter,
    /// `prometheus_tsdb_snapshot_replay_error_total`.
    pub snapshot_replay_error_total: Counter,
    /// `prometheus_tsdb_sample_ooo_delta`.
    pub ooo_histogram: MetricHistogram,
    /// `prometheus_tsdb_mmap_chunks_total`.
    pub mmap_chunks_total: Counter,
    /// `prometheus_tsdb_wal_replay_unknown_refs_total{type}`.
    pub wal_replay_unknown_refs_total: LabeledCounter,
    /// `prometheus_tsdb_wbl_replay_unknown_refs_total{type}`.
    pub wbl_replay_unknown_refs_total: LabeledCounter,
}

impl HeadMetrics {
    fn new() -> HeadMetrics {
        HeadMetrics {
            active_appenders: Gauge::new(),
            appenders_created: Counter::new(),
            series_created: Counter::new(),
            series_removed: Counter::new(),
            series_not_found: Counter::new(),
            pending_commit_underflow: Counter::new(),
            chunks: Gauge::new(),
            chunks_created: Counter::new(),
            chunks_removed: Counter::new(),
            gc_duration: MetricHistogram::new(Vec::new()),
            samples_appended: TypeCounter::default(),
            out_of_order_samples_appended: TypeCounter::default(),
            out_of_bound_samples: TypeCounter::default(),
            out_of_order_samples: TypeCounter::default(),
            too_old_samples: TypeCounter::default(),
            wal_truncate_duration: MetricHistogram::new(Vec::new()),
            wal_corruptions_total: Counter::new(),
            data_total_replay_duration: Gauge::new(),
            head_truncate_fail: Counter::new(),
            head_truncate_total: Counter::new(),
            checkpoint_delete_fail: Counter::new(),
            checkpoint_delete_total: Counter::new(),
            checkpoint_creation_fail: Counter::new(),
            checkpoint_creation_total: Counter::new(),
            mmap_chunk_corruption_total: Counter::new(),
            snapshot_replay_error_total: Counter::new(),
            ooo_histogram: MetricHistogram::new(vec![
                60.0 * 10.0,
                60.0 * 30.0,
                60.0 * 60.0,
                60.0 * 60.0 * 2.0,
                60.0 * 60.0 * 3.0,
                60.0 * 60.0 * 6.0,
                60.0 * 60.0 * 12.0,
            ]),
            mmap_chunks_total: Counter::new(),
            wal_replay_unknown_refs_total: LabeledCounter::default(),
            wbl_replay_unknown_refs_total: LabeledCounter::default(),
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Chunks of a series.

/// Go `memChunk`: a head chunk in memory.
#[derive(Clone, Debug)]
pub struct MemChunk {
    pub chunk: Arc<dyn Chunk>,
    pub min_time: i64,
    pub max_time: i64,
}

impl MemChunk {
    /// Go `OverlapsClosedInterval`.
    #[inline]
    pub fn overlaps_closed_interval(&self, mint: i64, maxt: i64) -> bool {
        overlaps_closed_interval(self.min_time, self.max_time, mint, maxt)
    }

    /// The chunk for appending. The open chunk is never shared; if it is (which should not happen), its bytes
    /// are copied first so readers keep a consistent view.
    pub(crate) fn chunk_mut(&mut self) -> &mut dyn Chunk {
        if Arc::get_mut(&mut self.chunk).is_none() {
            let c = chunkenc::from_data(self.chunk.encoding(), self.chunk.bytes().to_vec())
                .expect("copy of a valid head chunk");
            self.chunk = Arc::from(c);
        }
        Arc::get_mut(&mut self.chunk).expect("unshared chunk")
    }
}

/// Go `overlapsClosedInterval`.
#[inline]
pub fn overlaps_closed_interval(mint1: i64, maxt1: i64, mint2: i64, maxt2: i64) -> bool {
    mint1 <= maxt2 && mint2 <= maxt1
}

/// Go `mmappedChunk`: a head chunk on disk that has been mmapped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MmappedChunk {
    pub r#ref: ChunkDiskMapperRef,
    pub num_samples: u16,
    pub encoding: Encoding,
    pub min_time: i64,
    pub max_time: i64,
}

impl MmappedChunk {
    #[inline]
    pub fn overlaps_closed_interval(&self, mint: i64, maxt: i64) -> bool {
        overlaps_closed_interval(self.min_time, self.max_time, mint, maxt)
    }
}

/// Go `oooHeadChunk`.
#[derive(Debug)]
pub struct OooHeadChunk {
    pub chunk: OooChunk,
    pub min_time: i64,
    pub max_time: i64,
}

impl OooHeadChunk {
    #[inline]
    pub fn overlaps_closed_interval(&self, mint: i64, maxt: i64) -> bool {
        overlaps_closed_interval(self.min_time, self.max_time, mint, maxt)
    }
}

/// Go `memSeriesOOOFields`: the fields needed for out-of-order data.
#[derive(Debug, Default)]
pub struct MemSeriesOooFields {
    /// Immutable chunks on disk containing OOO samples.
    pub ooo_mmapped_chunks: Vec<MmappedChunk>,
    /// Most recent chunk for ooo samples in memory that's still being built.
    pub ooo_head_chunk: Option<OooHeadChunk>,
    /// HeadOOOChunkID for oooMmappedChunks[0].
    pub first_ooo_chunk_id: HeadChunkId,
}

/// The rarely used parts of a series, allocated on first use.
#[derive(Debug, Default)]
pub struct MemSeriesExt {
    pub meta: metadata::Handle,
    /// Latest accepted exemplar timestamp, including exemplars awaiting commit.
    pub last_exemplar_ts: i64,
    /// Whether an exemplar timestamp has been recorded.
    pub has_exemplar: bool,
    pub ooo: Option<MemSeriesOooFields>,
    /// The last histogram value (to check for duplicates).
    pub last_histogram_value: Option<Arc<Histogram>>,
    pub last_float_histogram_value: Option<Arc<FloatHistogram>>,
}

impl MemSeriesExt {
    fn is_empty(&self) -> bool {
        metadata::is_zero_handle(&self.meta)
            && !self.has_exemplar
            && self.ooo.is_none()
            && self.last_histogram_value.is_none()
            && self.last_float_histogram_value.is_none()
    }
}

/// The appender of the open head chunk (Go `chunkenc.Appender` of `memSeries.app`). The XOR appender is held
/// inline; the others are boxed.
#[derive(Clone, Debug, Default)]
pub enum SeriesAppender {
    #[default]
    None,
    Xor(chunkenc::XorAppender),
    Other(Box<chunkenc::Appender>),
}

impl SeriesAppender {
    pub fn from_appender(a: chunkenc::Appender) -> SeriesAppender {
        match a {
            chunkenc::Appender::Xor(x) => SeriesAppender::Xor(x),
            o => SeriesAppender::Other(Box::new(o)),
        }
    }

    pub fn is_none(&self) -> bool {
        matches!(self, SeriesAppender::None)
    }

    /// The appender as `chunkenc::Appender` (for `prev` arguments), if it is not an XOR appender.
    pub fn as_appender(&self) -> Option<&chunkenc::Appender> {
        match self {
            SeriesAppender::Other(a) => Some(a),
            _ => None,
        }
    }

    /// Appends a float sample to `c`.
    #[inline]
    pub fn append(&mut self, c: &mut dyn Chunk, st: i64, t: i64, v: f64) {
        match self {
            SeriesAppender::Xor(a) => {
                let c = c
                    .as_any_mut()
                    .downcast_mut::<chunkenc::XorChunk>()
                    .expect("XOR appender used with another chunk");
                a.append(c, st, t, v)
            }
            SeriesAppender::Other(a) => a.append(c, st, t, v),
            SeriesAppender::None => panic!("append without an appender"),
        }
    }
}

// Layout of MemSeriesInner::state.
const SERIES_PENDING_COMMIT_MASK: u32 = (1 << 30) - 1;
const SERIES_HISTOGRAM_CHUNK_HAS_COMPUTED_END_TIME_FLAG: u32 = 1 << 30;
const SERIES_GCED_FLAG: u32 = 1 << 31;

/// Go `memSeries`: the in-memory representation of a series. The mutable part is behind the mutex.
pub struct MemSeries {
    pub r#ref: HeadSeriesRef,
    /// Series labels hash for sharding; 0 when sharding is not enabled.
    pub shard_hash: u64,
    pub lset: Labels,
    inner: Mutex<MemSeriesInner>,
}

impl std::fmt::Debug for MemSeries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemSeries")
            .field("ref", &self.r#ref)
            .field("lset", &self.lset)
            .finish()
    }
}

/// The mutable part of [`MemSeries`]; access it with [`MemSeries::lock`].
#[derive(Debug)]
pub struct MemSeriesInner {
    /// Immutable chunks on disk that have not yet gone into a block, in order of ascending time stamps.
    pub mmapped_chunks: Vec<MmappedChunk>,
    /// Chunks in memory that are still being built or waiting to be mmapped, oldest first. The last one is
    /// the open chunk.
    pub head_chunks: Vec<MemChunk>,
    /// HeadChunkID for mmapped_chunks[0].
    pub first_chunk_id: HeadChunkId,
    /// Timestamp at which to cut the next chunk.
    pub next_at: i64,
    /// Max time of any mmapped chunk, only used during WAL replay.
    pub mm_max_time: i64,
    /// The last value (to check for duplicates).
    pub last_value: f64,
    /// Pending-sample count and flags.
    pub state: u32,
    /// The appender of the open head chunk.
    pub app: SeriesAppender,
    /// None if isolation is disabled.
    pub txs: Option<TxRing>,
    /// Rarely used fields.
    pub ext: Option<Box<MemSeriesExt>>,
}

impl MemSeries {
    /// Go `newMemSeries`.
    pub fn new(
        lset: Labels,
        id: HeadSeriesRef,
        shard_hash: u64,
        isolation_disabled: bool,
        pending_commit: bool,
    ) -> MemSeries {
        let mut inner = MemSeriesInner {
            mmapped_chunks: Vec::new(),
            head_chunks: Vec::new(),
            first_chunk_id: 0,
            next_at: i64::MIN,
            mm_max_time: 0,
            last_value: 0.0,
            state: 0,
            app: SeriesAppender::None,
            txs: None,
            ext: None,
        };
        if pending_commit {
            inner.mark_pending_commit();
        }
        if !isolation_disabled {
            inner.txs = Some(TxRing::new(0));
        }
        MemSeries {
            r#ref: id,
            shard_hash,
            lset,
            inner: Mutex::new(inner),
        }
    }

    /// Locks the series (Go `s.Lock()`).
    #[inline]
    pub fn lock(&self) -> MutexGuard<'_, MemSeriesInner> {
        lock(&self.inner)
    }

    /// Go `TryLock` (tests): whether the lock is free; a lock poisoned by a panic counts as free.
    #[doc(hidden)]
    pub fn test_try_lock(&self) -> bool {
        match self.inner.try_lock() {
            Ok(_) => true,
            Err(std::sync::TryLockError::Poisoned(_)) => true,
            Err(std::sync::TryLockError::WouldBlock) => false,
        }
    }

    /// Go `labels()`.
    #[inline]
    pub fn labels(&self) -> &Labels {
        &self.lset
    }

    /// Go `updateExemplarTimestamp`: records the latest exemplar time while holding the series lock.
    pub fn update_exemplar_timestamp(&self, ts: i64) {
        let mut g = self.lock();
        let ext = g.ext_mut();
        if !ext.has_exemplar || ts > ext.last_exemplar_ts {
            ext.last_exemplar_ts = ts;
        }
        ext.has_exemplar = true;
    }
}

impl MemSeriesInner {
    /// The rarely used fields, allocated on first use.
    pub fn ext_mut(&mut self) -> &mut MemSeriesExt {
        self.ext.get_or_insert_with(Default::default)
    }

    /// Frees the rarely used fields when they are all empty.
    pub(crate) fn shrink_ext(&mut self) {
        if self.ext.as_ref().is_some_and(|e| e.is_empty()) {
            self.ext = None;
        }
    }

    /// Go `s.ooo`.
    #[inline]
    pub fn ooo(&self) -> Option<&MemSeriesOooFields> {
        self.ext.as_ref().and_then(|e| e.ooo.as_ref())
    }

    #[inline]
    pub fn ooo_mut(&mut self) -> Option<&mut MemSeriesOooFields> {
        self.ext.as_mut().and_then(|e| e.ooo.as_mut())
    }

    /// Go `if s.ooo == nil { s.ooo = &memSeriesOOOFields{} }`.
    pub fn ooo_or_init(&mut self) -> &mut MemSeriesOooFields {
        self.ext_mut().ooo.get_or_insert_with(Default::default)
    }

    /// Go `s.ooo = nil`.
    pub fn clear_ooo(&mut self) {
        if let Some(e) = self.ext.as_mut() {
            e.ooo = None;
        }
        self.shrink_ext();
    }

    #[inline]
    pub fn last_histogram_value(&self) -> Option<&Arc<Histogram>> {
        self.ext.as_ref().and_then(|e| e.last_histogram_value.as_ref())
    }

    #[inline]
    pub fn last_float_histogram_value(&self) -> Option<&Arc<FloatHistogram>> {
        self.ext.as_ref().and_then(|e| e.last_float_histogram_value.as_ref())
    }

    /// Sets the last histogram values (Go `s.lastHistogramValue = h; s.lastFloatHistogramValue = fh`).
    pub(crate) fn set_last_histogram_values(&mut self, h: Option<Arc<Histogram>>, fh: Option<Arc<FloatHistogram>>) {
        if h.is_none() && fh.is_none() {
            if let Some(e) = self.ext.as_mut() {
                e.last_histogram_value = None;
                e.last_float_histogram_value = None;
            }
            self.shrink_ext();
            return;
        }
        let e = self.ext_mut();
        e.last_histogram_value = h;
        e.last_float_histogram_value = fh;
    }

    /// The metadata handle (Go `s.meta`).
    pub fn meta(&self) -> Option<&metadata::Handle> {
        self.ext.as_ref().map(|e| &e.meta)
    }

    /// Go `s.hasExemplar`.
    pub fn has_exemplar(&self) -> bool {
        self.ext.as_ref().is_some_and(|e| e.has_exemplar)
    }

    /// Go `s.lastExemplarTs`.
    pub fn last_exemplar_ts(&self) -> i64 {
        self.ext.as_ref().map_or(0, |e| e.last_exemplar_ts)
    }

    /// Go `headChunkCount`.
    #[inline]
    pub fn head_chunk_count(&self) -> u32 {
        self.head_chunks.len() as u32
    }

    /// The open (newest) head chunk (Go `s.headChunks`).
    #[inline]
    pub fn head_chunk(&self) -> Option<&MemChunk> {
        self.head_chunks.last()
    }

    pub fn has_pending_commit(&self) -> bool {
        self.pending_commit_count() != 0
    }

    pub fn pending_commit_count(&self) -> u32 {
        self.state & SERIES_PENDING_COMMIT_MASK
    }

    pub fn mark_pending_commit(&mut self) {
        if self.pending_commit_count() == SERIES_PENDING_COMMIT_MASK {
            panic!("pending commit counter overflow");
        }
        self.state += 1;
    }

    /// Go `unmarkPendingCommit`: releases one pending-sample reservation, reporting whether there was one.
    pub fn unmark_pending_commit(&mut self) -> bool {
        if !self.has_pending_commit() {
            return false;
        }
        self.state -= 1;
        true
    }

    pub fn is_gced(&self) -> bool {
        self.state & SERIES_GCED_FLAG != 0
    }

    pub fn set_gced(&mut self) {
        self.state |= SERIES_GCED_FLAG;
    }

    pub fn has_computed_histogram_chunk_end_time(&self) -> bool {
        self.state & SERIES_HISTOGRAM_CHUNK_HAS_COMPUTED_END_TIME_FLAG != 0
    }

    pub fn set_computed_histogram_chunk_end_time(&mut self, computed: bool) {
        if computed {
            self.state |= SERIES_HISTOGRAM_CHUNK_HAS_COMPUTED_END_TIME_FLAG;
        } else {
            self.state &= !SERIES_HISTOGRAM_CHUNK_HAS_COMPUTED_END_TIME_FLAG;
        }
    }

    /// Go `sampleState`: the latest in-order sample's staleness, type and bucket count.
    pub fn sample_state(&self) -> (bool, bool, usize) {
        if let Some(h) = self.last_histogram_value() {
            return (is_stale_nan(h.sum), true, h.positive_buckets.len() + h.negative_buckets.len());
        }
        if let Some(h) = self.last_float_histogram_value() {
            return (is_stale_nan(h.sum), true, h.positive_buckets.len() + h.negative_buckets.len());
        }
        (is_stale_nan(self.last_value), false, 0)
    }

    /// Go `minTime`.
    pub fn min_time(&self) -> i64 {
        if let Some(c) = self.mmapped_chunks.first() {
            return c.min_time;
        }
        if let Some(c) = self.head_chunks.first() {
            return c.min_time;
        }
        i64::MIN
    }

    /// Go `maxTime`.
    pub fn max_time(&self) -> i64 {
        // The highest timestamps will always be in the regular (non-OOO) chunks, even if OOO is enabled.
        if let Some(c) = self.head_chunks.last() {
            return c.max_time;
        }
        if let Some(c) = self.mmapped_chunks.last() {
            return c.max_time;
        }
        i64::MIN
    }

    /// Go `pushHeadChunk`: adds `chk` as the newest head chunk.
    pub fn push_head_chunk(&mut self, chk: MemChunk, lset: &Labels, sref: HeadSeriesRef) -> &mut MemChunk {
        // Chunk IDs can only be 23 bits, so a series cannot hold more than 2^23 chunks without two of them
        // sharing an ID.
        if self.mmapped_chunks.len() + self.head_chunks.len() >= (OOO_CHUNK_ID_MASK - 1) as usize {
            panic!("too many in-order head chunks for series {} ({})", lset, sref);
        }
        self.head_chunks.push(chk);
        self.head_chunks.last_mut().expect("pushed")
    }

    /// Go `truncateChunksBefore`: removes all chunks that have no timestamp at or after `mint`. Chunk IDs
    /// remain unchanged. Returns the number of removed chunks.
    pub fn truncate_chunks_before(&mut self, mint: i64, min_ooo_mmap_ref: ChunkDiskMapperRef) -> usize {
        let mut removed_in_order = 0usize;
        if !self.head_chunks.is_empty() {
            // Walk newest to oldest; the first chunk that ends before mint removes itself and everything older.
            let n = self.head_chunks.len();
            for i in 0..n {
                let pos = n - 1 - i;
                if self.head_chunks[pos].max_time < mint {
                    // If any head chunk is truncated, we can truncate all mmapped chunks.
                    removed_in_order = (pos + 1) + self.mmapped_chunks.len();
                    self.first_chunk_id = wrap_chunk_id(self.first_chunk_id.wrapping_add(removed_in_order as u64));
                    self.head_chunks.drain(..=pos);
                    self.mmapped_chunks.clear();
                    break;
                }
            }
        }
        if !self.mmapped_chunks.is_empty() {
            removed_in_order = 0;
            for (i, c) in self.mmapped_chunks.iter().enumerate() {
                if c.max_time >= mint {
                    break;
                }
                removed_in_order = i + 1;
            }
            self.mmapped_chunks.drain(..removed_in_order);
            self.first_chunk_id = wrap_chunk_id(self.first_chunk_id.wrapping_add(removed_in_order as u64));
        }
        if self.head_chunks.is_empty() && self.head_chunks.capacity() > 0 {
            self.head_chunks = Vec::new();
        }

        let mut removed_ooo = 0usize;
        let mut clear = false;
        if let Some(ooo) = self.ooo_mut()
            && !ooo.ooo_mmapped_chunks.is_empty()
        {
            for (i, c) in ooo.ooo_mmapped_chunks.iter().enumerate() {
                if c.r#ref.greater_than(min_ooo_mmap_ref) {
                    break;
                }
                removed_ooo = i + 1;
            }
            ooo.ooo_mmapped_chunks.drain(..removed_ooo);
            ooo.first_ooo_chunk_id = wrap_chunk_id(ooo.first_ooo_chunk_id.wrapping_add(removed_ooo as u64));
            if ooo.ooo_mmapped_chunks.is_empty() && ooo.ooo_head_chunk.is_none() {
                clear = true;
            }
        }
        if clear {
            self.clear_ooo();
        }
        removed_in_order + removed_ooo
    }

    /// Go `cleanupAppendIDsBelow`.
    pub fn cleanup_append_ids_below(&mut self, bound: u64) {
        if let Some(t) = self.txs.as_mut() {
            t.cleanup_append_ids_below(bound);
        }
    }

    /// Go `headChunkID`: the HeadChunkID at the given position (`pos < len(mmapped_chunks)` are the mmapped
    /// chunks, the rest the head chunks, oldest first).
    #[inline]
    pub fn head_chunk_id(&self, pos: usize) -> HeadChunkId {
        wrap_chunk_id((pos as u64).wrapping_add(self.first_chunk_id))
    }

    /// Go `oooHeadChunkID`. The series must have OOO fields.
    pub fn ooo_head_chunk_id(&self, pos: usize) -> HeadChunkId {
        let first = self.ooo().map_or(0, |o| o.first_ooo_chunk_id);
        wrap_chunk_id((pos as u64).wrapping_add(first)) | OOO_CHUNK_ID_MASK
    }

    /// Go `mmapChunks`: m-maps all but the newest head chunk. Returns the number of m-mapped chunks.
    pub fn mmap_chunks(&mut self, cdm: &ChunkDiskMapper, sref: HeadSeriesRef) -> usize {
        if self.head_chunks.len() < 2 {
            // There is none or only one head chunk, so nothing to m-map here.
            return 0;
        }
        let n = self.head_chunks.len() - 1;
        for chk in self.head_chunks.drain(..n) {
            let num_samples = chk.chunk.num_samples() as u16;
            let encoding = chk.chunk.encoding();
            let chunk_ref = cdm.write_chunk(
                sref,
                chk.min_time,
                chk.max_time,
                chk.chunk,
                false,
                Some(Box::new(handle_chunk_write_error)),
            );
            self.mmapped_chunks.push(MmappedChunk {
                r#ref: chunk_ref,
                num_samples,
                encoding,
                min_time: chk.min_time,
                max_time: chk.max_time,
            });
        }
        if self.head_chunks.capacity() > 4 {
            self.head_chunks.shrink_to(1);
        }
        n
    }
}

/// Go `handleChunkWriteError`.
pub(crate) fn handle_chunk_write_error(r: Result<(), tsdb_format::chunks::Error>) {
    if let Err(e) = r
        && !matches!(e, tsdb_format::chunks::Error::Closed)
    {
        panic!("{}", e);
    }
}

/// Go `wrapChunkID`: reduces `id` into the 23-bit chunk ID space.
#[inline]
pub fn wrap_chunk_id(id: HeadChunkId) -> HeadChunkId {
    id & (OOO_CHUNK_ID_MASK - 1)
}

/// Go `unpackHeadChunkRef`: (series ID, chunk ID, is OOO).
#[inline]
pub fn unpack_head_chunk_ref(r: u64) -> (HeadSeriesRef, HeadChunkId, bool) {
    let (sid, cid) = tsdb_format::chunks::unpack_head_chunk_ref(r);
    (sid, cid & (OOO_CHUNK_ID_MASK - 1), cid & OOO_CHUNK_ID_MASK != 0)
}

/// Go `isSeriesWithoutOOO`.
pub fn is_series_without_ooo(s: &MemSeriesInner) -> bool {
    s.ooo().is_none()
}

/// Go `isStaleSeries`.
pub fn is_stale_series(s: &MemSeriesInner) -> bool {
    s.sample_state().0
}

// ---------------------------------------------------------------------------------------------------------------
// Series maps.

/// Go `seriesHashmap`: series by label hash, with a second map for hash collisions.
#[derive(Default)]
pub struct SeriesHashmap {
    pub unique: U64Map<Arc<MemSeries>>,
    pub conflicts: Option<U64Map<Vec<Arc<MemSeries>>>>,
}

impl SeriesHashmap {
    pub fn get(&self, hash: u64, lset: &Labels) -> Option<&Arc<MemSeries>> {
        if let Some(s) = self.unique.get(&hash)
            && s.lset == *lset
        {
            return Some(s);
        }
        self.conflicts
            .as_ref()
            .and_then(|c| c.get(&hash))
            .and_then(|l| l.iter().find(|s| s.lset == *lset))
    }

    pub fn set(&mut self, hash: u64, s: Arc<MemSeries>) {
        match self.unique.get(&hash) {
            None => {
                self.unique.insert(hash, s);
                return;
            }
            Some(existing) if existing.lset == s.lset => {
                self.unique.insert(hash, s);
                return;
            }
            _ => {}
        }
        let l = self.conflicts.get_or_insert_with(Default::default).entry(hash).or_default();
        for prev in l.iter_mut() {
            if prev.lset == s.lset {
                *prev = s;
                return;
            }
        }
        l.push(s);
    }

    pub fn del(&mut self, hash: u64, r: HeadSeriesRef) {
        let rem: Vec<Arc<MemSeries>>;
        match self.unique.get(&hash) {
            None => return,
            Some(u) if u.r#ref == r => {
                let conflicts = self.conflicts.as_mut().and_then(|c| c.remove(&hash)).unwrap_or_default();
                if conflicts.is_empty() {
                    self.unique.remove(&hash);
                    return;
                }
                let mut it = conflicts.into_iter();
                self.unique.insert(hash, it.next().expect("non-empty"));
                rem = it.collect();
            }
            Some(_) => {
                rem = self
                    .conflicts
                    .as_ref()
                    .and_then(|c| c.get(&hash))
                    .map(|l| l.iter().filter(|s| s.r#ref != r).cloned().collect())
                    .unwrap_or_default();
            }
        }
        if let Some(c) = self.conflicts.as_mut() {
            if rem.is_empty() {
                c.remove(&hash);
            } else {
                c.insert(hash, rem);
            }
            if c.is_empty() {
                self.conflicts = None;
            }
        }
    }
}

/// One shard of the series maps (Go: one stripe of `series`, `hashes` and `locks`).
#[derive(Default)]
pub struct Shard {
    /// Series by reference.
    pub series: U64Map<Arc<MemSeries>>,
    /// Series by label hash.
    pub hashes: SeriesHashmap,
}

/// Go `stripeSeries`: series by reference and by label hash, sharded.
pub struct StripeSeries {
    pub size: usize,
    shards: Box<[RwLock<Shard>]>,
    /// Per-shard count of series with at least two head chunks (ready for mmapping).
    mmap_ready: Box<[AtomicI32]>,
    series_lifecycle_callback: Arc<dyn SeriesLifecycleCallback>,
}

/// The result of [`StripeSeries::gc`].
#[derive(Debug, Default)]
pub struct GcResult {
    pub deleted: HashSet<SeriesRef>,
    pub affected: HashSet<model::labels::Label>,
    pub chunks_removed: usize,
    pub stale_series_deleted: usize,
    pub histogram_series_deleted: usize,
    pub histogram_buckets_deleted: usize,
    pub actual_mint: i64,
    pub min_ooo_time: i64,
    pub min_mmap_file: i64,
}

impl StripeSeries {
    /// Go `newStripeSeries`. The number of shards is the smaller of `stripe_size` and a power of two near 4x
    /// the CPU count; no map is preallocated.
    pub fn new(stripe_size: usize, callback: Arc<dyn SeriesLifecycleCallback>) -> StripeSeries {
        let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
        let want = (cpus * 4).next_power_of_two();
        let size = stripe_size.max(1).next_power_of_two().min(want);
        StripeSeries {
            size,
            shards: (0..size).map(|_| RwLock::new(Shard::default())).collect(),
            mmap_ready: (0..size).map(|_| AtomicI32::new(0)).collect(),
            series_lifecycle_callback: callback,
        }
    }

    /// Go `refStripe`.
    #[inline]
    pub fn ref_stripe(&self, r: HeadSeriesRef) -> usize {
        (r & (self.size as u64 - 1)) as usize
    }

    #[inline]
    fn hash_stripe(&self, hash: u64) -> usize {
        (hash & (self.size as u64 - 1)) as usize
    }

    /// Go `incMmapReady`.
    pub fn inc_mmap_ready(&self, r: HeadSeriesRef) {
        self.mmap_ready[self.ref_stripe(r)].fetch_add(1, Ordering::Relaxed);
    }

    /// Go `decMmapReady`.
    pub fn dec_mmap_ready(&self, r: HeadSeriesRef) {
        self.mmap_ready[self.ref_stripe(r)].fetch_sub(1, Ordering::Relaxed);
    }

    /// The mmap-ready count of shard `i` (tests).
    #[doc(hidden)]
    pub fn test_mmap_ready(&self, i: usize) -> i32 {
        self.mmap_ready[i].load(Ordering::Relaxed)
    }

    /// Go `getByID`.
    #[inline]
    pub fn get_by_id(&self, id: HeadSeriesRef) -> Option<Arc<MemSeries>> {
        rlock(&self.shards[self.ref_stripe(id)]).series.get(&id).cloned()
    }

    /// Go `getByHash`.
    #[inline]
    pub fn get_by_hash(&self, hash: u64, lset: &Labels) -> Option<Arc<MemSeries>> {
        rlock(&self.shards[self.hash_stripe(hash)]).hashes.get(hash, lset).cloned()
    }

    /// Go `unlinkHash`: removes the series from the hash index only.
    pub fn unlink_hash(&self, hash: u64, r: HeadSeriesRef) {
        wlock(&self.shards[self.hash_stripe(hash)]).hashes.del(hash, r);
    }

    /// Go `setUnlessAlreadySet`.
    pub fn set_unless_already_set(
        &self,
        hash: u64,
        lset: &Labels,
        series: Arc<MemSeries>,
    ) -> (Arc<MemSeries>, bool) {
        {
            let mut g = wlock(&self.shards[self.hash_stripe(hash)]);
            if let Some(prev) = g.hashes.get(hash, lset) {
                return (prev.clone(), false);
            }
            g.hashes.set(hash, series.clone());
        }
        wlock(&self.shards[self.ref_stripe(series.r#ref)])
            .series
            .insert(series.r#ref, series.clone());
        (series, true)
    }

    /// Go `postCreation`.
    pub fn post_creation(&self, lset: &Labels) {
        self.series_lifecycle_callback.post_creation(lset);
    }

    pub(crate) fn callback(&self) -> &Arc<dyn SeriesLifecycleCallback> {
        &self.series_lifecycle_callback
    }

    /// Removes a series from the reference map (with the hash map entry already removed).
    pub(crate) fn remove_by_ref(&self, r: HeadSeriesRef) -> Option<Arc<MemSeries>> {
        wlock(&self.shards[self.ref_stripe(r)]).series.remove(&r)
    }

    /// The series of shard `i` (by reference), for iteration without holding the shard lock.
    pub fn shard_series(&self, i: usize) -> Vec<Arc<MemSeries>> {
        rlock(&self.shards[i]).series.values().cloned().collect()
    }

    /// Calls `f` for every series (shard by shard, under the shard read lock).
    pub fn for_each(&self, mut f: impl FnMut(&Arc<MemSeries>)) {
        for sh in self.shards.iter() {
            for s in rlock(sh).series.values() {
                f(s);
            }
        }
    }

    /// Go `iterForDeletion`: calls `check(hash_shard, hash, series)` for every series; series for which it
    /// returns true (after it marked them GCed under the series lock) are removed from both maps. Calls the
    /// `post_deletion` hook per shard. Returns the number of deleted series.
    pub fn iter_for_deletion(&self, mut check: impl FnMut(usize, u64, &Arc<MemSeries>) -> bool) -> usize {
        let mut total = 0;
        for i in 0..self.size {
            let mut deleted_for_callback: HashMap<HeadSeriesRef, Labels> = HashMap::new();
            let mut other_stripes: Vec<HeadSeriesRef> = Vec::new();
            {
                let mut g = wlock(&self.shards[i]);
                // Iterate conflicts first so deletions don't move them to the unique map before they are seen.
                let mut candidates: Vec<(u64, Arc<MemSeries>)> = Vec::new();
                if let Some(c) = &g.hashes.conflicts {
                    for (h, l) in c {
                        for s in l {
                            candidates.push((*h, s.clone()));
                        }
                    }
                }
                for (h, s) in &g.hashes.unique {
                    candidates.push((*h, s.clone()));
                }
                for (hash, series) in candidates {
                    if !check(i, hash, &series) {
                        continue;
                    }
                    g.hashes.del(hash, series.r#ref);
                    let stripe = self.ref_stripe(series.r#ref);
                    if stripe == i {
                        g.series.remove(&series.r#ref);
                    } else {
                        other_stripes.push(series.r#ref);
                    }
                    deleted_for_callback.insert(series.r#ref, series.lset.clone());
                }
                if g.series.is_empty() && g.series.capacity() > 0 {
                    g.series = U64Map::default();
                }
                if g.hashes.unique.is_empty() && g.hashes.unique.capacity() > 0 {
                    g.hashes.unique = U64Map::default();
                }
            }
            for r in other_stripes {
                self.remove_by_ref(r);
            }
            self.series_lifecycle_callback.post_deletion(&deleted_for_callback);
            total += deleted_for_callback.len();
        }
        total
    }

    /// Go `stripeSeries.gc`: garbage collects old chunks that are strictly before `mint` and removes series
    /// that have no chunks left.
    pub fn gc(&self, mint: i64, min_ooo_mmap_ref: ChunkDiskMapperRef) -> GcResult {
        let mut res = GcResult {
            actual_mint: i64::MAX,
            min_ooo_time: i64::MAX,
            min_mmap_file: i32::MAX as i64,
            ..Default::default()
        };
        let deleted = std::cell::RefCell::new(&mut res);
        self.iter_for_deletion(|_hash_shard, _hash, series| {
            let mut res = deleted.borrow_mut();
            let mut s = series.lock();
            let was_mmap_ready = s.head_chunk_count() >= 2;
            res.chunks_removed += s.truncate_chunks_before(mint, min_ooo_mmap_ref);
            if was_mmap_ready && s.head_chunk_count() < 2 {
                self.dec_mmap_ready(series.r#ref);
            }
            // Replay merges in-order chunks by timestamp, which need not match disk file order.
            for ch in &s.mmapped_chunks {
                let (seq, _) = ch.r#ref.unpack();
                if (seq as i64) < res.min_mmap_file {
                    res.min_mmap_file = seq as i64;
                }
            }
            if let Some(ooo) = s.ooo() {
                if let Some(first) = ooo.ooo_mmapped_chunks.first() {
                    let (seq, _) = first.r#ref.unpack();
                    if (seq as i64) < res.min_mmap_file {
                        res.min_mmap_file = seq as i64;
                    }
                    for ch in &ooo.ooo_mmapped_chunks {
                        if ch.min_time < res.min_ooo_time {
                            res.min_ooo_time = ch.min_time;
                        }
                    }
                }
                if let Some(hc) = &ooo.ooo_head_chunk
                    && hc.min_time < res.min_ooo_time
                {
                    res.min_ooo_time = hc.min_time;
                }
            }
            if !s.mmapped_chunks.is_empty()
                || !s.head_chunks.is_empty()
                || s.has_pending_commit()
                || s.ooo().is_some_and(|o| !o.ooo_mmapped_chunks.is_empty() || o.ooo_head_chunk.is_some())
            {
                let series_mint = s.min_time();
                if series_mint < res.actual_mint {
                    res.actual_mint = series_mint;
                }
                return false;
            }
            // Exemplars are not persisted in blocks. Keep their series reference resolvable until WAL truncation
            // and replay can discard them by timestamp.
            if s.has_exemplar() && s.last_exemplar_ts() >= mint {
                // Clear the appender so it does not retain the removed sample chunk.
                s.app = SeriesAppender::None;
                return false;
            }
            let (stale, is_hist, buckets) = s.sample_state();
            if stale {
                res.stale_series_deleted += 1;
            }
            if is_hist {
                res.histogram_series_deleted += 1;
                res.histogram_buckets_deleted += buckets;
            }
            res.deleted.insert(series.r#ref);
            s.set_gced();
            for (n, v) in series.lset.iter() {
                res.affected.insert(model::labels::Label::new(n, v));
            }
            true
        });
        if res.actual_mint == i64::MAX {
            res.actual_mint = mint;
        }
        res
    }

    /// Go `stripeSeries.gcSeries`: removes the series in `series_refs` whose max time is <= `maxt` and for
    /// which `should_evict` returns true. Returns (deleted, affected, chunks removed, stale, histogram series,
    /// histogram buckets).
    pub fn gc_series(
        &self,
        series_refs: &[SeriesRef],
        maxt: i64,
        mut should_evict: impl FnMut(&MemSeries, &mut MemSeriesInner) -> bool,
    ) -> GcResult {
        let mut res = GcResult::default();
        let refs_set: HashSet<SeriesRef> = series_refs.iter().copied().collect();
        self.iter_for_deletion(|_hash_shard, _hash, series| {
            if !refs_set.contains(&series.r#ref) {
                // This series was not provided by the caller. Skip it.
                return false;
            }
            let mut s = series.lock();
            if s.has_pending_commit() || s.max_time() > maxt {
                return false;
            }
            if !should_evict(series, &mut s) {
                return false;
            }
            let head_chunk_count = s.head_chunk_count();
            res.chunks_removed += head_chunk_count as usize;
            res.chunks_removed += s.mmapped_chunks.len();
            if head_chunk_count >= 2 {
                self.dec_mmap_ready(series.r#ref);
            }
            res.deleted.insert(series.r#ref);
            // Keep head chunks intact for readers that still reference the series.
            s.set_gced();
            let (stale, is_hist, buckets) = s.sample_state();
            if stale {
                res.stale_series_deleted += 1;
            }
            if is_hist {
                res.histogram_series_deleted += 1;
                res.histogram_buckets_deleted += buckets;
            }
            for (n, v) in series.lset.iter() {
                res.affected.insert(model::labels::Label::new(n, v));
            }
            true
        });
        res
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Head.

struct CardinalityCache {
    cache: Option<Arc<PostingsStats>>,
    key: String,
    last_call: Option<Instant>,
}

/// Go `Head`: reads and writes of time series data within a time window.
pub struct Head {
    me: Weak<Head>,
    pub chunk_range: AtomicI64,
    pub(crate) num_series: AtomicU64,
    pub(crate) num_stale_series: AtomicU64,
    pub(crate) num_native_histogram_series: AtomicU64,
    pub(crate) num_native_histogram_buckets: AtomicU64,
    pub(crate) min_ooo_time: AtomicI64,
    pub(crate) max_ooo_time: AtomicI64,
    /// `i64::MAX` while the head is not initialized.
    pub min_time: AtomicI64,
    pub max_time: AtomicI64,
    /// Mint allowed to be added to the head.
    pub min_valid_time: AtomicI64,
    pub last_wal_truncation_time: AtomicI64,
    pub(crate) last_memory_truncation_time: AtomicI64,
    pub last_series_id: AtomicU64,
    /// All the ooo m-map chunks should be after this.
    pub(crate) min_ooo_mmap_ref: AtomicU64,

    pub metrics: HeadMetrics,
    pub opts: HeadOptions,
    pub(crate) wal: Option<Arc<WL>>,
    wbl: RwLock<Option<Arc<WL>>>,
    pub exemplar_metrics: Arc<ExemplarMetrics>,
    exemplars: RwLock<Option<Arc<CircularExemplarStorage>>>,
    pub(crate) logger: Logger,

    /// All series addressable by their ID or hash.
    pub series: StripeSeries,

    /// Series no longer in the head, and what time they must be kept until.
    pub(crate) wal_expiries: Mutex<HashMap<HeadSeriesRef, i64>>,
    /// Series references that the WBL uses through a mapping that WAL replay made.
    pub(crate) wbl_pinned_series_refs: Mutex<HashSet<HeadSeriesRef>>,

    /// Postings lists for terms.
    pub postings: MemPostings,
    pub tombstones: Arc<MemTombstones>,
    pub iso: Arc<Isolation>,
    pub ooo_iso: Arc<OooIsolation>,

    cardinality: Mutex<CardinalityCache>,

    /// Writes and reads head chunks to/from disk.
    pub chunk_disk_mapper: ChunkDiskMapper,

    pub(crate) chunk_snapshot_mtx: Mutex<()>,
    closed: Mutex<bool>,
    /// The series state thread (`fast-startup` option): its stop channel and handle.
    pub(crate) series_state_quit: Mutex<Option<(std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>)>>,

    pub stats: Arc<HeadStats>,
    write_notified: RwLock<Option<Arc<dyn WriteNotified>>>,

    pub(crate) mem_truncation_in_process: AtomicBool,
    /// For testing purposes.
    #[doc(hidden)]
    pub mem_truncation_call_back: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    has_test_after_series_lookup: AtomicBool,
    /// Invoked after the series lookup in appenders, before the series is locked (tests).
    test_after_series_lookup: Mutex<Option<Arc<dyn Fn(Option<&Arc<MemSeries>>) + Send + Sync>>>,
}

/// Go `headULID`.
pub fn head_ulid() -> Ulid {
    Ulid::parse("0000000000XXXXXXXXXXXXHEAD").expect("valid ULID")
}

/// Go `rangeHeadULID`.
pub fn range_head_ulid() -> Ulid {
    Ulid::parse("0000000000XXXXXXXRANGEHEAD").expect("valid ULID")
}

/// Go `selectedSeriesHeadULID`.
pub fn selected_series_head_ulid() -> Ulid {
    Ulid::parse("0000000000XXSELECTEDSERIES").expect("valid ULID")
}

/// Go `oooCompactionHeadULID`.
pub fn ooo_compaction_head_ulid() -> Ulid {
    Ulid::parse("0000000000XX000COMPACTHEAD").expect("valid ULID")
}

/// Go `mmappedChunksDir`.
pub fn mmapped_chunks_dir(dir: &Path) -> PathBuf {
    dir.join("chunks_head")
}

/// Go `Stats`: important current head statistics.
#[derive(Debug, Clone)]
pub struct Stats {
    pub num_series: u64,
    pub min_time: i64,
    pub max_time: i64,
    pub index_posting_stats: Arc<PostingsStats>,
}

/// Go `clampInterval`.
pub fn clamp_interval(a: i64, b: i64, mint: i64, maxt: i64) -> (i64, i64) {
    (a.max(mint), b.min(maxt))
}

impl Head {
    /// Go `NewHead`: opens the head block in `opts.chunk_dir_root`.
    pub fn new(
        wal: Option<Arc<WL>>,
        wbl: Option<Arc<WL>>,
        mut opts: HeadOptions,
        stats: Option<Arc<HeadStats>>,
    ) -> Result<Arc<Head>, Error> {
        if opts.out_of_order_time_window.load(Ordering::Relaxed) < 0 {
            opts.out_of_order_time_window.store(0, Ordering::Relaxed);
        }
        // Time window can be set on runtime. So the capMin and capMax should be valid even if ooo is not
        // enabled yet.
        let cap_max = opts.out_of_order_cap_max.load(Ordering::Relaxed);
        if cap_max <= 0 || cap_max > 255 {
            return Err(Error::msg(format!(
                "OOOCapMax of {} is invalid. must be > 0 and <= 255",
                cap_max
            )));
        }
        if opts.chunk_range < 1 {
            return Err(Error::msg(format!("invalid chunk range {}", opts.chunk_range)));
        }
        let callback = opts
            .series_callback
            .get_or_insert_with(|| Arc::new(NoopSeriesLifecycleCallback))
            .clone();
        let stats = stats.unwrap_or_else(HeadStats::new);
        if !opts.enable_exemplar_storage {
            opts.max_exemplars.store(0, Ordering::Relaxed);
        }
        if opts.wal_replay_concurrency <= 0 {
            opts.wal_replay_concurrency = default_wal_replay_concurrency();
        }
        let exemplar_metrics = ExemplarMetrics::new();
        let es = CircularExemplarStorage::new(
            opts.max_exemplars.load(Ordering::Relaxed),
            exemplar_metrics.clone(),
            opts.out_of_order_time_window.load(Ordering::Relaxed),
        )?;
        let chunk_disk_mapper = ChunkDiskMapper::new(
            &mmapped_chunks_dir(&opts.chunk_dir_root),
            opts.chunk_pool.clone(),
            opts.chunk_write_buffer_size,
            opts.chunk_write_queue_size,
        )
        .map_err(chunks_err)?;
        let logger = opts.logger.clone();
        let series = StripeSeries::new(opts.stripe_size, callback);
        let iso = Isolation::new(opts.isolation_disabled);
        Ok(Arc::new_cyclic(|me| Head {
            me: me.clone(),
            chunk_range: AtomicI64::new(opts.chunk_range),
            num_series: AtomicU64::new(0),
            num_stale_series: AtomicU64::new(0),
            num_native_histogram_series: AtomicU64::new(0),
            num_native_histogram_buckets: AtomicU64::new(0),
            min_ooo_time: AtomicI64::new(i64::MAX),
            max_ooo_time: AtomicI64::new(i64::MIN),
            min_time: AtomicI64::new(i64::MAX),
            max_time: AtomicI64::new(i64::MIN),
            min_valid_time: AtomicI64::new(0),
            last_wal_truncation_time: AtomicI64::new(i64::MIN),
            last_memory_truncation_time: AtomicI64::new(i64::MIN),
            last_series_id: AtomicU64::new(0),
            min_ooo_mmap_ref: AtomicU64::new(0),
            metrics: HeadMetrics::new(),
            opts,
            wal,
            wbl: RwLock::new(wbl),
            exemplar_metrics,
            exemplars: RwLock::new(Some(es)),
            logger,
            series,
            wal_expiries: Mutex::new(HashMap::new()),
            wbl_pinned_series_refs: Mutex::new(HashSet::new()),
            postings: MemPostings::new_unordered(),
            tombstones: Arc::new(MemTombstones::new()),
            iso,
            ooo_iso: OooIsolation::new(),
            cardinality: Mutex::new(CardinalityCache {
                cache: None,
                key: String::new(),
                last_call: None,
            }),
            chunk_disk_mapper,
            chunk_snapshot_mtx: Mutex::new(()),
            closed: Mutex::new(false),
            series_state_quit: Mutex::new(None),
            stats,
            write_notified: RwLock::new(None),
            mem_truncation_in_process: AtomicBool::new(false),
            mem_truncation_call_back: Mutex::new(None),
            has_test_after_series_lookup: AtomicBool::new(false),
            test_after_series_lookup: Mutex::new(None),
        }))
    }

    /// The head as an `Arc` (it is always created in one).
    pub fn arc(&self) -> Arc<Head> {
        self.me.upgrade().expect("head is alive")
    }

    /// Go `resetInMemoryState`: drops all series and in-memory state, calling the deletion hooks.
    pub(crate) fn reset_in_memory_state(&self) -> Result<(), Error> {
        let es = CircularExemplarStorage::new(
            self.opts.max_exemplars.load(Ordering::Relaxed),
            self.exemplar_metrics.clone(),
            self.opts.out_of_order_time_window.load(Ordering::Relaxed),
        )?;
        // Reset the existing series to make sure we call the appropriate hooks and increment the series
        // removed metrics.
        let mut deleted = HashSet::new();
        let mut affected = HashSet::new();
        let fs = self.series.iter_for_deletion(|_, _, s| {
            s.lock().set_gced();
            deleted.insert(s.r#ref);
            for (n, v) in s.lset.iter() {
                affected.insert(model::labels::Label::new(n, v));
            }
            true
        });
        self.metrics.series_removed.add(fs as f64);
        self.iso.reset();
        self.ooo_iso.reset();
        self.num_series.store(0, Ordering::Relaxed);
        self.num_stale_series.store(0, Ordering::Relaxed);
        self.num_native_histogram_series.store(0, Ordering::Relaxed);
        self.num_native_histogram_buckets.store(0, Ordering::Relaxed);
        *wlock(&self.exemplars) = Some(es);
        // Go makes new postings and tombstones; here they are emptied in place.
        let (all_name, all_value) = tsdb_format::index::all_postings_key();
        if let Ok(all) = tsdb_format::index::expand_postings(&mut self.postings.postings(
            tsdb_format::index::background(),
            all_name,
            &[all_value],
        )) {
            deleted.extend(all);
        }
        for l in self.postings.sorted_keys() {
            affected.insert(l);
        }
        self.postings.delete(&deleted, &affected);
        let mut stones = Vec::new();
        let _ = self.tombstones.iter(&mut |r, _| {
            stones.push(r);
            Ok(())
        });
        self.tombstones.delete_tombstones(stones.iter());
        lock(&self.wal_expiries).clear();
        lock(&self.wbl_pinned_series_refs).clear();
        self.chunk_range.store(self.opts.chunk_range, Ordering::Relaxed);
        self.min_time.store(i64::MAX, Ordering::Relaxed);
        self.max_time.store(i64::MIN, Ordering::Relaxed);
        self.min_ooo_time.store(i64::MAX, Ordering::Relaxed);
        self.max_ooo_time.store(i64::MIN, Ordering::Relaxed);
        self.last_wal_truncation_time.store(i64::MIN, Ordering::Relaxed);
        self.last_memory_truncation_time.store(i64::MIN, Ordering::Relaxed);
        Ok(())
    }

    /// The exemplar storage.
    pub fn exemplars(&self) -> Arc<CircularExemplarStorage> {
        rlock(&self.exemplars).clone().expect("exemplar storage")
    }

    /// The WBL, if any.
    pub fn wbl(&self) -> Option<Arc<WL>> {
        rlock(&self.wbl).clone()
    }

    /// The WAL, if any.
    pub fn wal(&self) -> Option<&Arc<WL>> {
        self.wal.as_ref()
    }

    /// The logger.
    pub fn logger(&self) -> &Logger {
        &self.logger
    }

    /// Sets the hook called on every write (Go `writeNotified`).
    pub fn set_write_notified(&self, wn: Option<Arc<dyn WriteNotified>>) {
        *wlock(&self.write_notified) = wn;
    }

    pub(crate) fn write_notified(&self) -> Option<Arc<dyn WriteNotified>> {
        rlock(&self.write_notified).clone()
    }

    /// Sets the hook that tests run after the series lookup of an append (Go `testAfterSeriesLookup`).
    #[doc(hidden)]
    pub fn test_set_after_series_lookup(&self, f: Option<Arc<dyn Fn(Option<&Arc<MemSeries>>) + Send + Sync>>) {
        self.has_test_after_series_lookup.store(f.is_some(), Ordering::Release);
        *lock(&self.test_after_series_lookup) = f;
    }

    #[inline]
    pub(crate) fn run_test_after_series_lookup(&self, s: Option<&Arc<MemSeries>>) {
        if self.has_test_after_series_lookup.load(Ordering::Acquire) {
            let f = lock(&self.test_after_series_lookup).clone();
            if let Some(f) = f {
                f(s);
            }
        }
    }

    /// Go `ApplyConfig`: the out-of-order window and the exemplar storage size.
    pub fn apply_config(&self, ooo_time_window: i64, max_exemplars: i64, wbl: Option<Arc<WL>>) {
        let ooo_time_window = ooo_time_window.max(0);
        self.set_out_of_order_time_window(ooo_time_window, wbl);
        if !self.opts.enable_exemplar_storage {
            return;
        }
        self.exemplars().set_out_of_order_time_window(ooo_time_window);
        // Head uses opts.MaxExemplars in combination with opts.EnableExemplarStorage to decide if it should pass
        // exemplars along to its exemplar storage.
        let prev_size = self.opts.max_exemplars.load(Ordering::Relaxed);
        self.opts.max_exemplars.store(max_exemplars, Ordering::Relaxed);
        let new_size = self.opts.max_exemplars.load(Ordering::Relaxed);
        if prev_size == new_size {
            return;
        }
        let migrated = self.exemplars().resize(new_size);
        self.logger.info(
            "Exemplar storage resized",
            &[("from", &prev_size), ("to", &new_size), ("migrated", &migrated)],
        );
    }

    /// Go `SetOutOfOrderTimeWindow`. If the head already has a WBL, `wbl` is ignored.
    pub fn set_out_of_order_time_window(&self, ooo_time_window: i64, wbl: Option<Arc<WL>>) {
        if ooo_time_window > 0 {
            let mut w = wlock(&self.wbl);
            if w.is_none() {
                *w = wbl;
            }
        }
        self.opts.out_of_order_time_window.store(ooo_time_window, Ordering::Relaxed);
    }

    /// Go `PostingsCardinalityStats`: highest cardinality stats by label and value names (cached for 30s).
    pub fn postings_cardinality_stats(&self, stats_by_label_name: &str, limit: usize) -> Arc<PostingsStats> {
        let cache_key = format!("{};{}", stats_by_label_name, limit);
        let mut c = lock(&self.cardinality);
        if c.key != cache_key {
            c.cache = None;
        } else if c.last_call.is_none_or(|t| t.elapsed() > CARDINALITY_CACHE_EXPIRATION_TIME) {
            c.cache = None;
        }
        if let Some(s) = &c.cache {
            return s.clone();
        }
        c.key = cache_key;
        let s = Arc::new(self.postings.stats(stats_by_label_name, limit, model::labels::size_of_labels));
        c.cache = Some(s.clone());
        c.last_call = Some(Instant::now());
        s
    }

    /// Go `updateMinMaxTime`: expands the head time bounds to include the in-order range.
    pub fn update_min_max_time(&self, mint: i64, maxt: i64) {
        self.min_time.fetch_min(mint, Ordering::AcqRel);
        self.max_time.fetch_max(maxt, Ordering::AcqRel);
    }

    /// Go `updateMinOOOMaxOOOTime`.
    pub(crate) fn update_min_ooo_max_ooo_time(&self, mint: i64, maxt: i64) {
        self.min_ooo_time.fetch_min(mint, Ordering::AcqRel);
        self.max_ooo_time.fetch_max(maxt, Ordering::AcqRel);
    }

    /// Go `SetMinValidTime`.
    pub fn set_min_valid_time(&self, min_valid_time: i64) {
        self.min_valid_time.store(min_valid_time, Ordering::Relaxed);
    }

    /// Go `minValidTime`.
    pub fn min_valid_time(&self) -> i64 {
        self.min_valid_time.load(Ordering::Relaxed)
    }

    /// Go `Truncate`: removes old data before `mint` from the head and the WAL.
    pub fn truncate(&self, mint: i64) -> Result<(), Error> {
        let initialized = self.initialized();
        self.truncate_memory(mint)?;
        if !initialized {
            return Ok(());
        }
        self.truncate_wal(mint)
    }

    /// Go `OverlapsClosedInterval`.
    pub fn overlaps_closed_interval(&self, mint: i64, maxt: i64) -> bool {
        self.min_time() <= maxt && mint <= self.max_time()
    }

    /// Go `truncateMemory`: removes old data before `mint` from the head.
    pub fn truncate_memory(&self, mint: i64) -> Result<(), Error> {
        let _g = lock(&self.chunk_snapshot_mtx);
        let res = self.truncate_memory_locked(mint);
        if res.is_err() {
            self.metrics.head_truncate_fail.inc();
        }
        res
    }

    fn truncate_memory_locked(&self, mint: i64) -> Result<(), Error> {
        let initialized = self.initialized();
        if self.min_time() >= mint && initialized {
            return Ok(());
        }
        // The order of these two stores should not be changed.
        self.last_memory_truncation_time.store(mint, Ordering::SeqCst);
        self.mem_truncation_in_process.store(true, Ordering::SeqCst);
        struct Reset<'a>(&'a AtomicBool);
        impl Drop for Reset<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::SeqCst);
            }
        }
        let _reset = Reset(&self.mem_truncation_in_process);

        if let Some(cb) = lock(&self.mem_truncation_call_back).as_ref() {
            cb();
        }
        // We wait for pending queries to end that overlap with this truncation.
        if initialized {
            self.wait_for_pending_readers_in_time_range(self.min_time(), mint);
        }
        self.min_time.store(mint, Ordering::SeqCst);
        self.min_valid_time.store(mint, Ordering::SeqCst);
        // Ensure that max time is at least as high as min time.
        self.max_time.fetch_max(mint, Ordering::SeqCst);

        // This was an initial call to Truncate after loading blocks on startup. We haven't read back the WAL
        // yet, so do not attempt to truncate it.
        if !initialized {
            return Ok(());
        }
        self.metrics.head_truncate_total.inc();
        self.truncate_series_and_chunk_disk_mapper("truncateMemory")
    }

    /// Go `truncateStaleSeries`.
    pub fn truncate_stale_series(
        &self,
        series_refs: &[SeriesRef],
        maxt: i64,
        fingerprints: &HashMap<SeriesRef, SeriesFingerprint>,
    ) -> Result<(), Error> {
        self.truncate_series(series_refs, maxt, |s, g| {
            is_series_without_ooo(g) && is_stale_series(g) && !has_mutated_since_snapshot(s, g, fingerprints)
        })
        .map(|_| ())
    }

    /// Go `truncateSelectedSeries`.
    pub fn truncate_selected_series(
        &self,
        series_refs: &[SeriesRef],
        maxt: i64,
        fingerprints: &HashMap<SeriesRef, SeriesFingerprint>,
    ) -> Result<(), Error> {
        self.truncate_series(series_refs, maxt, |s, g| {
            is_series_without_ooo(g) && !has_mutated_since_snapshot(s, g, fingerprints)
        })
        .map(|_| ())
    }

    /// Go `snapshotFingerprints`.
    pub fn snapshot_fingerprints(
        &self,
        series_refs: &[SeriesRef],
        append_id_watermark: u64,
    ) -> HashMap<SeriesRef, SeriesFingerprint> {
        let mut fps = HashMap::with_capacity(series_refs.len());
        for &r in series_refs {
            let Some(s) = self.series.get_by_id(r) else {
                continue;
            };
            let g = s.lock();
            fps.insert(r, snapshot_fingerprint(&g, append_id_watermark));
        }
        fps
    }

    /// Go `truncateSeries`: removes the given series (for which `should_evict` returns true) from the head and
    /// records WAL tombstones for them. Returns the number of evicted series.
    pub fn truncate_series(
        &self,
        series_refs: &[SeriesRef],
        maxt: i64,
        should_evict: impl FnMut(&MemSeries, &mut MemSeriesInner) -> bool,
    ) -> Result<usize, Error> {
        let _g = lock(&self.chunk_snapshot_mtx);
        if self.min_time() > maxt {
            return Ok(0);
        }
        self.wait_for_pending_readers_in_time_range(self.min_time(), maxt);
        let deleted = self.gc_series(series_refs, maxt, should_evict);
        // Record the deleted series refs in the WAL so that we can ignore them during replay.
        if let Some(wal) = &self.wal {
            let stones: Vec<Stone> = deleted
                .iter()
                .map(|&r| Stone {
                    r#ref: r,
                    intervals: vec![Interval {
                        mint: i64::MIN,
                        maxt: i64::MAX,
                    }],
                })
                .collect();
            let mut buf = Vec::new();
            record::Encoder::default().tombstones(&stones, &mut buf);
            wal.log(&buf).map_err(wlog_err)?;
        }
        Ok(deleted.len())
    }

    /// Go `WaitForPendingReadersInTimeRange`: waits for queries overlapping [mint, maxt) to finish.
    pub fn wait_for_pending_readers_in_time_range(&self, mint: i64, maxt: i64) {
        let maxt = maxt.wrapping_sub(1); // Making it inclusive before checking overlaps.
        let overlaps = || {
            let mut o = false;
            self.iso.traverse_open_reads(|s| {
                if s.mint <= maxt && mint <= s.maxt {
                    o = true;
                    return false;
                }
                true
            });
            o
        };
        while overlaps() {
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    /// Go `WaitForPendingReadersForOOOChunksAtOrBefore`.
    pub fn wait_for_pending_readers_for_ooo_chunks_at_or_before(&self, chunk: ChunkDiskMapperRef) {
        while self.ooo_iso.has_open_reads_at_or_before(chunk) {
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    /// Go `WaitForAppendersOverlapping`.
    pub fn wait_for_appenders_overlapping(&self, maxt: i64) {
        while maxt >= self.iso.lowest_append_time() {
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    /// Go `IsQuerierCollidingWithTruncation`: (should close, get new, new mint).
    pub fn is_querier_colliding_with_truncation(&self, querier_mint: i64, querier_maxt: i64) -> (bool, bool, i64) {
        if !self.mem_truncation_in_process.load(Ordering::SeqCst) {
            return (false, false, 0);
        }
        // Head truncation is in process. It also means that the block that was created for this truncation
        // range is also available.
        let mem_trunc_time = self.last_memory_truncation_time.load(Ordering::SeqCst);
        if querier_maxt < mem_trunc_time {
            // This query doesn't overlap with the Head any longer.
            return (true, false, mem_trunc_time);
        }
        if querier_mint < mem_trunc_time {
            // The truncation started after we got the querier.
            return (true, true, mem_trunc_time);
        }
        (false, false, 0)
    }

    /// Go `getWALExpiry`.
    pub fn get_wal_expiry(&self, id: HeadSeriesRef) -> Option<i64> {
        lock(&self.wal_expiries).get(&id).copied()
    }

    /// Go `updateWALExpiry`: keeps the higher of the current value and `keep_until`.
    #[allow(dead_code)] // Used by the WAL replay port.
    pub fn update_wal_expiry(&self, id: HeadSeriesRef, keep_until: i64) {
        let mut g = lock(&self.wal_expiries);
        let e = g.entry(id).or_insert(0);
        *e = (*e).max(keep_until);
    }

    /// Go `pinWBLSeriesRefs`.
    #[allow(dead_code)] // Used by the WAL replay port.
    pub fn pin_wbl_series_refs(&self, refs: &HashSet<HeadSeriesRef>) {
        lock(&self.wbl_pinned_series_refs).extend(refs.iter().copied());
    }

    /// Go `isWBLPinnedSeriesRef`.
    pub(crate) fn is_wbl_pinned_series_ref(&self, id: HeadSeriesRef) -> bool {
        lock(&self.wbl_pinned_series_refs).contains(&id)
    }

    /// Go `releaseWBLPinnedSeriesRefs`.
    pub fn release_wbl_pinned_series_refs(&self) {
        lock(&self.wbl_pinned_series_refs).clear();
    }

    /// Go `keepSeriesInWALCheckpointFn`.
    pub fn keep_series_in_wal_checkpoint_fn(&self, mint: i64) -> impl Fn(HeadSeriesRef) -> bool + '_ {
        move |id| {
            // Keep the record if the series exists in the head.
            if self.series.get_by_id(id).is_some() {
                return true;
            }
            // Keep the record if the WBL uses this reference.
            if self.is_wbl_pinned_series_ref(id) {
                return true;
            }
            // Keep the record if the series has an expiry set.
            self.get_wal_expiry(id).is_some_and(|k| k >= mint)
        }
    }

    /// Go `truncateWAL`: removes old data before `mint` from the WAL.
    pub fn truncate_wal(&self, mint: i64) -> Result<(), Error> {
        let _g = lock(&self.chunk_snapshot_mtx);
        let Some(wal) = &self.wal else {
            return Ok(());
        };
        if mint <= self.last_wal_truncation_time.load(Ordering::SeqCst) {
            return Ok(());
        }
        let start = Instant::now();
        self.last_wal_truncation_time.store(mint, Ordering::SeqCst);

        let (first, mut last) =
            wlog::segments(wal.dir()).map_err(|e| Error::wrap("get segment range", wlog_err(e)))?;
        // Start a new segment, so low ingestion volume TSDB don't have more WAL than needed.
        wal.next_segment().map_err(|e| Error::wrap("next segment", wlog_err(e)))?;
        last -= 1; // Never consider last segment for checkpoint.
        if last < 0 {
            return Ok(()); // no segments yet.
        }
        // The lower two thirds of segments should contain mostly obsolete samples. If we have less than two
        // segments, it's not worth checkpointing yet.
        last = first + (last - first) * 2 / 3;
        if last <= first {
            return Ok(());
        }

        self.metrics.checkpoint_creation_total.inc();
        let keep = self.keep_series_in_wal_checkpoint_fn(mint);
        if let Err(e) = wlog::checkpoint(
            wal,
            first,
            last,
            &keep,
            mint,
            self.opts.enable_st_storage.load(Ordering::Relaxed),
            true,
        ) {
            self.metrics.checkpoint_creation_fail.inc();
            if e.corruption().is_some() {
                self.metrics.wal_corruptions_total.inc();
            }
            return Err(Error::wrap("create checkpoint", wlog_err(e)));
        }
        if let Err(e) = wal.truncate(last + 1) {
            // If truncating fails, we'll just try again at the next checkpoint.
            self.logger.error("truncating segments failed", &[("err", &e)]);
        }
        // The checkpoint is written and data before mint is truncated, so stop tracking expired series.
        lock(&self.wal_expiries).retain(|_, keep_until| *keep_until >= mint);

        self.metrics.checkpoint_delete_total.inc();
        if let Err(e) = wlog::delete_checkpoints(wal.dir(), last) {
            // Leftover old checkpoints do not cause problems down the line beyond occupying disk space.
            self.logger.error("delete old checkpoints", &[("err", &e)]);
            self.metrics.checkpoint_delete_fail.inc();
        }
        self.metrics.wal_truncate_duration.observe(start.elapsed().as_secs_f64());
        self.logger.info(
            "WAL checkpoint complete",
            &[
                ("first", &first),
                ("last", &last),
                ("duration", &format!("{:?}", start.elapsed())),
            ],
        );
        Ok(())
    }

    /// Go `truncateOOO`: waits for reads of OOO chunks at or before `new_min_ooo_mmap_ref`, truncates the WBL
    /// files below `last_wbl_file` and garbage collects the old OOO m-map chunks.
    pub fn truncate_ooo(&self, last_wbl_file: i64, new_min_ooo_mmap_ref: ChunkDiskMapperRef) -> Result<(), Error> {
        let cur = ChunkDiskMapperRef(self.min_ooo_mmap_ref.load(Ordering::SeqCst));
        if new_min_ooo_mmap_ref.greater_than(cur) {
            self.wait_for_pending_readers_for_ooo_chunks_at_or_before(new_min_ooo_mmap_ref);
            self.min_ooo_mmap_ref.store(new_min_ooo_mmap_ref.0, Ordering::SeqCst);
            self.truncate_series_and_chunk_disk_mapper("truncateOOO")?;
        }
        let Some(wbl) = self.wbl() else {
            return Ok(());
        };
        wbl.truncate(last_wbl_file).map_err(wlog_err)?;
        // The truncated segments held every WBL record from before the last replay.
        self.release_wbl_pinned_series_refs();
        Ok(())
    }

    /// Go `truncateSeriesAndChunkDiskMapper`: runs GC and truncates the chunk disk mapper.
    fn truncate_series_and_chunk_disk_mapper(&self, caller: &str) -> Result<(), Error> {
        self.logger.info("Head GC started", &[("caller", &caller)]);
        let start = Instant::now();
        let head_maxt = self.max_time();
        let (actual_mint, mut min_ooo_time, min_mmap_file) = self.gc();
        self.logger.info(
            "Head GC completed",
            &[("caller", &caller), ("duration", &format!("{:?}", start.elapsed()))],
        );
        self.metrics.gc_duration.observe(start.elapsed().as_secs_f64());

        if actual_mint > self.min_time.load(Ordering::SeqCst) {
            // The actual mint of the head is higher than the one asked to truncate.
            let appendable_min_valid_time = self.appendable_min_valid_time();
            if actual_mint < appendable_min_valid_time {
                self.min_time.store(actual_mint, Ordering::SeqCst);
                self.min_valid_time.store(actual_mint, Ordering::SeqCst);
            } else {
                // The actual min time is in the appendable window. So we set the mint to the
                // appendableMinValidTime.
                self.min_time.store(appendable_min_valid_time, Ordering::SeqCst);
                self.min_valid_time.store(appendable_min_valid_time, Ordering::SeqCst);
            }
        }
        let window = self.opts.out_of_order_time_window.load(Ordering::Relaxed);
        if head_maxt.wrapping_sub(window) < min_ooo_time {
            // The allowed OOO window is lower than the min OOO time seen during GC.
            min_ooo_time = head_maxt.wrapping_sub(window);
        }
        self.min_ooo_time.store(min_ooo_time, Ordering::SeqCst);

        // Truncate the chunk m-mapper.
        self.chunk_disk_mapper
            .truncate(min_mmap_file as u32)
            .map_err(|e| Error::wrap("truncate chunks.HeadReadWriter by file number", chunks_err(e)))
    }

    /// Go `Stats`.
    pub fn stats(&self, stats_by_label_name: &str, limit: usize) -> Stats {
        Stats {
            num_series: self.num_series(),
            max_time: self.max_time(),
            min_time: self.min_time(),
            index_posting_stats: self.postings_cardinality_stats(stats_by_label_name, limit),
        }
    }

    /// Go `Delete`: deletes all samples in [mint, maxt] of the series that match the matchers.
    pub fn delete(&self, ctx: &Context, mint: i64, maxt: i64, ms: &[Matcher]) -> Result<(), Error> {
        // Do not delete anything beyond the currently valid range.
        let (mint, maxt) = clamp_interval(mint, maxt, self.min_time(), self.max_time());
        let ir = self.index_range(mint, maxt);
        let mut p = crate::querier::postings_for_matchers(ctx, &ir, ms)
            .map_err(|e| Error::wrap("select series", e))?;
        let mut stones: Vec<Stone> = Vec::new();
        while p.next() {
            if let Some(e) = ctx.err() {
                return Err(Error::wrap("select series", e));
            }
            let Some(series) = self.series.get_by_id(p.at()) else {
                self.logger.debug("Series not found in Head.Delete", &[]);
                continue;
            };
            let (t0, t1) = {
                let g = series.lock();
                (g.min_time(), g.max_time())
            };
            if t0 == i64::MIN || t1 == i64::MIN {
                continue;
            }
            // Delete only until the current values and not beyond.
            let (t0, t1) = clamp_interval(mint, maxt, t0, t1);
            stones.push(Stone {
                r#ref: p.at(),
                intervals: vec![Interval { mint: t0, maxt: t1 }],
            });
        }
        if let Some(e) = p.err() {
            return Err(Error::other(e));
        }
        drop(p);
        if let Some(e) = ctx.err() {
            return Err(Error::wrap("select series", e));
        }
        if let Some(wal) = &self.wal {
            let enc = record::Encoder {
                enable_st_storage: self.opts.enable_st_storage.load(Ordering::Relaxed),
            };
            let mut buf = Vec::new();
            enc.tombstones(&stones, &mut buf);
            wal.log(&buf).map_err(wlog_err)?;
        }
        for s in &stones {
            self.tombstones.add_interval(s.r#ref, &s.intervals[..1]);
        }
        Ok(())
    }

    /// Go `gc`: removes data before the minimum timestamp from the head. Returns the actual min time of the
    /// in-order chunks, the min OOO time seen, and the min mmap file number seen.
    pub fn gc(&self) -> (i64, i64, i64) {
        // Only data strictly lower than this timestamp must be deleted.
        let mint = self.min_time();
        // Only ooo m-map chunks strictly lower than or equal to this ref must be deleted.
        let min_ooo_mmap_ref = ChunkDiskMapperRef(self.min_ooo_mmap_ref.load(Ordering::SeqCst));
        let res = self.series.gc(mint, min_ooo_mmap_ref);
        let series_removed = res.deleted.len();
        self.metrics.series_removed.add(series_removed as f64);
        self.metrics.chunks_removed.add(res.chunks_removed as f64);
        self.metrics.chunks.sub(res.chunks_removed as f64);
        self.num_series.fetch_sub(series_removed as u64, Ordering::SeqCst);
        self.num_stale_series.fetch_sub(res.stale_series_deleted as u64, Ordering::SeqCst);
        self.num_native_histogram_series.fetch_sub(res.histogram_series_deleted as u64, Ordering::SeqCst);
        self.num_native_histogram_buckets.fetch_sub(res.histogram_buckets_deleted as u64, Ordering::SeqCst);

        // Remove deleted series IDs from the postings lists.
        self.postings.delete(&res.deleted, &res.affected);
        // Remove tombstones referring to the deleted series.
        self.tombstones.delete_tombstones(res.deleted.iter());
        self.tombstones.truncate_before(mint);

        if self.wal.is_some() {
            // Samples for deleted series are likely still in the WAL, so flag that the deleted series records
            // should be kept during WAL checkpointing while the WAL contains data through actualInOrderMint.
            let mut g = lock(&self.wal_expiries);
            for &r in &res.deleted {
                g.insert(r, res.actual_mint);
            }
        }
        (res.actual_mint, res.min_ooo_time, res.min_mmap_file)
    }

    /// Go `gcSeries`: removes eligible series from the head index and updates the metrics, postings,
    /// tombstones and WAL expiries. Returns the deleted references.
    pub(crate) fn gc_series(
        &self,
        series_refs: &[SeriesRef],
        maxt: i64,
        mut should_evict: impl FnMut(&MemSeries, &mut MemSeriesInner) -> bool,
    ) -> HashSet<SeriesRef> {
        let min_valid_time = self.min_valid_time.load(Ordering::SeqCst);
        let mut protected_chunks_removed = 0usize;
        let res = self.series.gc_series(series_refs, maxt, |series, s| {
            // Keep unpersisted samples before considering chunk removal or series eviction.
            if !should_evict(series, s) {
                return false;
            }
            // Delete the series if it has no exemplar at or after the WAL replay cutoff.
            if !s.has_exemplar() || s.last_exemplar_ts() < min_valid_time {
                return true;
            }
            // Free the persisted sample chunks, but keep the series identity for exemplar replay.
            let was_mmap_ready = s.head_chunk_count() >= 2;
            protected_chunks_removed += s.truncate_chunks_before(i64::MAX, ChunkDiskMapperRef(0));
            // Clear the appender because it references the removed head chunk.
            s.app = SeriesAppender::None;
            if was_mmap_ready && s.head_chunk_count() < 2 {
                self.series.dec_mmap_ready(series.r#ref);
            }
            false
        });
        let series_removed = res.deleted.len();
        let chunks_removed = res.chunks_removed + protected_chunks_removed;
        self.metrics.series_removed.add(series_removed as f64);
        self.metrics.chunks_removed.add(chunks_removed as f64);
        self.metrics.chunks.sub(chunks_removed as f64);
        self.num_series.fetch_sub(series_removed as u64, Ordering::SeqCst);
        self.num_stale_series.fetch_sub(res.stale_series_deleted as u64, Ordering::SeqCst);
        self.num_native_histogram_series.fetch_sub(res.histogram_series_deleted as u64, Ordering::SeqCst);
        self.num_native_histogram_buckets.fetch_sub(res.histogram_buckets_deleted as u64, Ordering::SeqCst);

        self.postings.delete(&res.deleted, &res.affected);
        self.tombstones.delete_tombstones(res.deleted.iter());

        if self.wal.is_some() {
            // Keep series records until we're past timestamp maxt because the WAL will still have samples
            // records with this ref ID.
            let mut g = lock(&self.wal_expiries);
            for &r in &res.deleted {
                g.insert(r, maxt);
            }
        }
        res.deleted
    }

    /// Go `deleteSeriesByID`: deletes the series with the given references. Only used for WAL replay.
    #[allow(dead_code)] // Used by the WAL replay port.
    pub fn delete_series_by_id(&self, refs: &[HeadSeriesRef]) {
        let mut deleted: HashSet<SeriesRef> = HashSet::new();
        let mut deleted_for_callback: HashMap<HeadSeriesRef, Labels> = HashMap::new();
        let mut affected = HashSet::new();
        let (mut stale_deleted, mut hist_deleted, mut buckets_deleted, mut chunks_removed) = (0usize, 0usize, 0usize, 0usize);
        for &r in refs {
            // Delete the reference from the series map.
            let Some(series) = self.series.remove_by_ref(r) else {
                continue;
            };
            // Delete the reference from the hash.
            self.series.unlink_hash(series.lset.hash(), series.r#ref);

            let mut s = series.lock();
            // Keep the series record in checkpoints until its last sample time.
            if self.wal.is_some() {
                let maxt = s.max_time();
                if maxt != i64::MIN {
                    self.update_wal_expiry(series.r#ref, maxt);
                }
            }
            let (stale, is_hist, buckets) = s.sample_state();
            if stale {
                stale_deleted += 1;
            }
            if is_hist {
                hist_deleted += 1;
                buckets_deleted += buckets;
            }
            let head_chunk_count = s.head_chunk_count();
            chunks_removed += s.mmapped_chunks.len();
            chunks_removed += head_chunk_count as usize;
            if head_chunk_count >= 2 {
                self.series.dec_mmap_ready(series.r#ref);
            }
            // Clear to prevent a double-subtraction from the chunksRemoved gauge.
            s.mmapped_chunks = Vec::new();
            drop(s);

            deleted.insert(series.r#ref);
            deleted_for_callback.insert(series.r#ref, series.lset.clone());
            for (n, v) in series.lset.iter() {
                affected.insert(model::labels::Label::new(n, v));
            }
        }
        self.metrics.series_removed.add(deleted.len() as f64);
        self.metrics.chunks_removed.add(chunks_removed as f64);
        self.metrics.chunks.sub(chunks_removed as f64);
        self.num_series.fetch_sub(deleted.len() as u64, Ordering::SeqCst);
        self.num_stale_series.fetch_sub(stale_deleted as u64, Ordering::SeqCst);
        self.num_native_histogram_series.fetch_sub(hist_deleted as u64, Ordering::SeqCst);
        self.num_native_histogram_buckets.fetch_sub(buckets_deleted as u64, Ordering::SeqCst);
        self.postings.delete(&deleted, &affected);
        self.tombstones.delete_tombstones(deleted.iter());
        if !deleted_for_callback.is_empty() {
            self.series.callback().post_deletion(&deleted_for_callback);
        }
    }

    /// Go `Tombstones`.
    pub fn tombstones(&self) -> Arc<dyn tombstones::Reader> {
        self.tombstones.clone()
    }

    /// Go `NumSeries`.
    pub fn num_series(&self) -> u64 {
        self.num_series.load(Ordering::SeqCst)
    }

    /// Go `NumStaleSeries`.
    pub fn num_stale_series(&self) -> u64 {
        self.num_stale_series.load(Ordering::SeqCst)
    }

    /// Go `updateStaleSeriesMetricOnAppend`.
    pub(crate) fn update_stale_series_metric_on_append(&self, was_stale: bool, is_stale: bool) {
        match (was_stale, is_stale) {
            (false, true) => {
                self.num_stale_series.fetch_add(1, Ordering::SeqCst);
            }
            (true, false) => {
                self.num_stale_series.fetch_sub(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }

    /// Go `NumNativeHistogramSeries`.
    pub fn num_native_histogram_series(&self) -> u64 {
        self.num_native_histogram_series.load(Ordering::SeqCst)
    }

    /// Go `NumNativeHistogramBuckets`.
    pub fn num_native_histogram_buckets(&self) -> u64 {
        self.num_native_histogram_buckets.load(Ordering::SeqCst)
    }

    /// Go `updateNativeHistogramMetricsOnAppend`.
    pub(crate) fn update_native_histogram_metrics_on_append(
        &self,
        was_histogram: bool,
        is_histogram: bool,
        old_buckets: usize,
        new_buckets: usize,
    ) {
        match (was_histogram, is_histogram) {
            (false, true) => {
                self.num_native_histogram_series.fetch_add(1, Ordering::SeqCst);
            }
            (true, false) => {
                self.num_native_histogram_series.fetch_sub(1, Ordering::SeqCst);
            }
            _ => {}
        }
        if new_buckets > old_buckets {
            self.num_native_histogram_buckets
                .fetch_add((new_buckets - old_buckets) as u64, Ordering::SeqCst);
        } else if old_buckets > new_buckets {
            self.num_native_histogram_buckets
                .fetch_sub((old_buckets - new_buckets) as u64, Ordering::SeqCst);
        }
    }

    /// Go `Meta`.
    pub fn meta(&self) -> BlockMeta {
        BlockMeta {
            min_time: self.min_time(),
            max_time: self.max_time(),
            ulid: head_ulid(),
            stats: BlockStats {
                num_series: self.num_series(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Go `MinTime`: the lowest time bound on visible data; `i64::MAX` if the head is not initialized.
    pub fn min_time(&self) -> i64 {
        self.min_time.load(Ordering::SeqCst)
    }

    /// Go `MaxTime`.
    pub fn max_time(&self) -> i64 {
        self.max_time.load(Ordering::SeqCst)
    }

    /// Go `MinOOOTime`.
    pub fn min_ooo_time(&self) -> i64 {
        self.min_ooo_time.load(Ordering::SeqCst)
    }

    /// Go `MaxOOOTime`.
    pub fn max_ooo_time(&self) -> i64 {
        self.max_ooo_time.load(Ordering::SeqCst)
    }

    /// Go `chunkRange`.
    pub fn chunk_range(&self) -> i64 {
        self.chunk_range.load(Ordering::SeqCst)
    }

    /// Sets the chunk range (Go `h.chunkRange.Store`).
    pub fn set_chunk_range(&self, r: i64) {
        self.chunk_range.store(r, Ordering::SeqCst);
    }

    /// Go `lastSeriesID`.
    pub fn last_series_id(&self) -> u64 {
        self.last_series_id.load(Ordering::SeqCst)
    }

    /// Go `minOOOMmapRef`.
    pub fn min_ooo_mmap_ref(&self) -> ChunkDiskMapperRef {
        ChunkDiskMapperRef(self.min_ooo_mmap_ref.load(Ordering::SeqCst))
    }

    /// Go `lastWALTruncationTime`.
    pub fn last_wal_truncation_time(&self) -> i64 {
        self.last_wal_truncation_time.load(Ordering::SeqCst)
    }

    /// Go `lastMemoryTruncationTime`.
    pub fn last_memory_truncation_time(&self) -> i64 {
        self.last_memory_truncation_time.load(Ordering::SeqCst)
    }

    /// Go `initialized`.
    pub fn initialized(&self) -> bool {
        self.min_time() != i64::MAX
    }

    /// Go `compactable`: whether the head time range is more than 1.5 times the chunk range.
    pub fn compactable(&self) -> bool {
        if !self.initialized() {
            return false;
        }
        self.max_time() - self.min_time() > self.chunk_range() / 2 * 3
    }

    /// Whether the head is closed.
    pub fn is_closed(&self) -> bool {
        *lock(&self.closed)
    }

    /// Go `Close`: flushes the WAL and closes the head (and takes a chunk snapshot if enabled).
    pub fn close(&self) -> Result<(), Error> {
        let mut closed = lock(&self.closed);
        *closed = true;
        // Stop the background series_state.json writer.
        if self.opts.enable_fast_startup && self.stop_series_state_ticker() {
            // Flush the final clean state.
            self.write_series_state(true);
        }
        // mmap all but last chunk in case we're performing snapshot since that only takes samples from most
        // recent head chunk.
        self.mmap_head_chunks();
        let mut errs = Vec::new();
        errs.push(self.chunk_disk_mapper.close().err().map(chunks_err));
        if let Some(wal) = &self.wal {
            errs.push(wal.close().err().map(wlog_err));
        }
        if let Some(wbl) = self.wbl() {
            errs.push(wbl.close().err().map(wlog_err));
        }
        let mut err = Error::join(errs);
        if err.is_none() && self.opts.enable_memory_snapshot_on_shutdown {
            err = Error::join([self.perform_chunk_snapshot().err()]);
        }
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Go `getOrCreate`.
    pub fn get_or_create(&self, hash: u64, lset: &Labels, pending_commit: bool) -> Result<(Arc<MemSeries>, bool), Error> {
        if let Some(s) = self.series.get_by_hash(hash, lset) {
            return Ok((s, false));
        }
        self.get_or_create_with_optional_id(0, hash, lset, pending_commit)
    }

    /// Go `getOrCreateWithOptionalID`: if `id` is zero, one is allocated.
    pub fn get_or_create_with_optional_id(
        &self,
        mut id: HeadSeriesRef,
        hash: u64,
        lset: &Labels,
        pending_commit: bool,
    ) -> Result<(Arc<MemSeries>, bool), Error> {
        self.series.callback().pre_creation(lset)?;
        if id == 0 {
            // Note this id is wasted in the case where a concurrent operation creates the same series first.
            id = self.last_series_id.fetch_add(1, Ordering::SeqCst) + 1;
        }
        let shard_hash = if self.opts.enable_sharding {
            model::labels::stable_hash(lset)
        } else {
            0
        };
        let optimistically_created =
            Arc::new(MemSeries::new(lset.clone(), id, shard_hash, self.opts.isolation_disabled, pending_commit));
        let (s, created) = self.series.set_unless_already_set(hash, lset, optimistically_created);
        if !created {
            return Ok((s, false));
        }
        self.metrics.series_created.inc();
        self.num_series.fetch_add(1, Ordering::SeqCst);
        self.postings.add(id, lset);
        // Adding the series in the postings marks the creation of series as any further calls to this and the
        // read methods would return that series.
        self.series.post_creation(lset);
        Ok((s, true))
    }

    /// Go `onChunkCreated`: bumps the chunk metrics; marks the series mmap-ready when it reached 2 head chunks.
    pub(crate) fn on_chunk_created(&self, sref: HeadSeriesRef, prev_head_chunk_count: u32, head_chunk_count: u32) {
        self.metrics.chunks.inc();
        self.metrics.chunks_created.inc();
        if prev_head_chunk_count < 2 && head_chunk_count == 2 {
            self.series.inc_mmap_ready(sref);
        }
    }

    /// Go `mmapHeadChunks`: m-maps all but the newest head chunk of every series that has more than one.
    pub fn mmap_head_chunks(&self) {
        let mut count = 0usize;
        for i in 0..self.series.size {
            count += self.mmap_head_chunks_in_stripe(i);
        }
        self.metrics.mmap_chunks_total.add(count as f64);
    }

    fn mmap_head_chunks_in_stripe(&self, i: usize) -> usize {
        if self.series.mmap_ready[i].load(Ordering::Relaxed) == 0 {
            return 0; // No series in this stripe need mmapping.
        }
        let mut count = 0;
        for series in self.series.shard_series(i) {
            let n = self.mmap_series_chunks(&series);
            if n > 0 {
                count += n;
                self.series.dec_mmap_ready(series.r#ref);
            }
        }
        count
    }

    fn mmap_series_chunks(&self, s: &MemSeries) -> usize {
        let mut g = s.lock();
        if g.is_gced() {
            return 0;
        }
        g.mmap_chunks(&self.chunk_disk_mapper, s.r#ref)
    }

    /// Go `Size`: the bytes of the WAL, WBL and head chunks on disk.
    pub fn size(&self) -> i64 {
        let wal_size = self.wal.as_ref().and_then(|w| w.size().ok()).unwrap_or(0);
        let wbl_size = self.wbl().and_then(|w| w.size().ok()).unwrap_or(0);
        let cdm_size = self.chunk_disk_mapper.size().unwrap_or(0);
        wal_size + wbl_size + cdm_size
    }

    /// Go `startWALReplayStatus`.
    pub(crate) fn start_wal_replay_status(&self, start_from: i64, last: i64) {
        let mut s = wlock(&self.stats.wal_replay_status);
        s.min = start_from;
        s.max = last;
        s.current = start_from;
    }

    /// Go `updateWALReplayStatusRead`.
    pub(crate) fn update_wal_replay_status_read(&self, current: i64) {
        wlock(&self.stats.wal_replay_status).current = current;
    }

    // -----------------------------------------------------------------------------------------------------------
    // Init and m-mapped chunk loading.

    /// Go `Init`: loads data from the WAL and prepares the head for writes. Call it before using an appender,
    /// so that it limits the ingested samples to the head min valid time.
    pub fn init(&self, min_valid_time: i64) -> Result<(), Error> {
        self.min_valid_time.store(min_valid_time, Ordering::SeqCst);
        let res = self.init_inner();
        // Loading of m-mapped chunks and snapshot can make the mint of the Head to go below minValidTime.
        if self.min_time() < self.min_valid_time.load(Ordering::SeqCst) {
            self.min_time.store(self.min_valid_time.load(Ordering::SeqCst), Ordering::SeqCst);
        }
        self.gc(); // After loading the wal remove the obsolete data from the head.
        self.postings.ensure_order(self.opts.wal_replay_concurrency);
        res
    }

    fn init_inner(&self) -> Result<(), Error> {
        self.logger.info("Replaying on-disk memory mappable chunks if any", &[]);
        let start = Instant::now();

        let mut snap_idx: i64 = -1;
        let mut snap_offset: i64 = 0;
        let mut ref_series: HashMap<HeadSeriesRef, Arc<MemSeries>> = HashMap::new();
        let mut snapshot_loaded = false;
        let mut chunk_snapshot_load_duration = Duration::ZERO;
        if self.opts.enable_memory_snapshot_on_shutdown {
            self.logger.info("Chunk snapshot is enabled, replaying from the snapshot", &[]);
            // If there are any WAL files, there should be at least one WAL file with an index that is current or
            // newer than the snapshot index. If the WAL index is behind the snapshot index somehow, the snapshot
            // is assumed to be outdated.
            let mut load_snapshot = true;
            if let Some(wal) = &self.wal {
                let (_, end_at) =
                    wlog::segments(wal.dir()).map_err(|e| Error::wrap("finding WAL segments", wlog_err(e)))?;
                match crate::head_wal::last_chunk_snapshot(&self.opts.chunk_dir_root) {
                    Ok((_, idx, _)) => {
                        if end_at < idx {
                            load_snapshot = false;
                            self.logger.warn("Last WAL file is behind snapshot, removing snapshots", &[]);
                            if let Err(e) =
                                crate::head_wal::delete_chunk_snapshots(&self.opts.chunk_dir_root, i64::MAX, i64::MAX)
                            {
                                self.logger.error("Error while deleting snapshot directories", &[("err", &e)]);
                            }
                        }
                    }
                    Err(e) => {
                        if !e.is(&storage::ERR_NOT_FOUND) {
                            self.logger.error("Could not find last snapshot", &[("err", &e)]);
                        }
                    }
                }
            }
            if load_snapshot {
                match self.load_chunk_snapshot() {
                    Ok((idx, off, rs)) => {
                        snap_idx = idx;
                        snap_offset = off;
                        ref_series = rs;
                        snapshot_loaded = true;
                        chunk_snapshot_load_duration = start.elapsed();
                        self.logger.info(
                            "Chunk snapshot loading time",
                            &[("duration", &format!("{:?}", chunk_snapshot_load_duration))],
                        );
                    }
                    Err(e) => {
                        snap_idx = -1;
                        snap_offset = 0;
                        ref_series = HashMap::new();
                        self.metrics.snapshot_replay_error_total.inc();
                        self.logger.error("Failed to load chunk snapshot", &[("err", &e)]);
                        // We clear the partially loaded data to replay fresh from the WAL.
                        self.reset_in_memory_state()?;
                    }
                }
            }
        }

        let mmap_chunk_replay_start = Instant::now();
        let mut mmapped_chunks: HashMap<HeadSeriesRef, Vec<MmappedChunk>> = HashMap::new();
        let mut ooo_mmapped_chunks: HashMap<HeadSeriesRef, Vec<MmappedChunk>> = HashMap::new();
        let mut last_mmap_ref = ChunkDiskMapperRef(0);
        let mut mmap_chunk_replay_duration = Duration::ZERO;
        if snapshot_loaded || self.wal.is_some() {
            // If snapshot was not loaded and if there is no WAL, then m-map chunks will be discarded anyway. So
            // we only load m-map chunks when it won't be discarded.
            match self.load_mmapped_chunks(&ref_series) {
                Ok((m, o, l)) => {
                    mmapped_chunks = m;
                    ooo_mmapped_chunks = o;
                    last_mmap_ref = l;
                }
                Err((e, _second_last)) => {
                    self.logger.error(
                        "Loading on-disk chunks failed",
                        &[("err", &format!("iterate on-disk chunks: {}", e))],
                    );
                    if matches!(e, tsdb_format::chunks::Error::Corruption(_)) {
                        self.metrics.mmap_chunk_corruption_total.inc();
                    }
                    // Discard snapshot data since we need to replay the WAL for the missed m-map chunks data.
                    snap_idx = -1;
                    snap_offset = 0;
                    // If this fails, data will be recovered from WAL.
                    let (m, o, l) = self.remove_corrupted_mmapped_chunks(&e)?;
                    mmapped_chunks = m;
                    ooo_mmapped_chunks = o;
                    last_mmap_ref = l;
                }
            }
            mmap_chunk_replay_duration = mmap_chunk_replay_start.elapsed();
            self.logger.info(
                "On-disk memory mappable chunks replay completed",
                &[("duration", &format!("{:?}", mmap_chunk_replay_duration))],
            );
        }

        let Some(wal) = self.wal.clone() else {
            self.logger.info("WAL not found", &[]);
            return Ok(());
        };

        self.logger.info("Replaying WAL, this may take a while", &[]);
        let checkpoint_replay_start = Instant::now();
        // Backfill the checkpoint first if it exists.
        let cp = wlog::last_checkpoint(wal.dir());
        let (dir, mut start_from) = match &cp {
            Ok((d, i)) => (Some(d.clone()), *i),
            Err(e) if e.is_not_found() => (None, 0),
            Err(e) => return Err(Error::wrap("find last checkpoint", wlog_err(e.clone()))),
        };
        // Find the last segment.
        let (_, end_at) = wlog::segments(wal.dir()).map_err(|e| Error::wrap("finding WAL segments", wlog_err(e)))?;

        if self.opts.enable_fast_startup {
            match self.read_series_state_file() {
                Ok(state) => {
                    if state.clean_shutdown {
                        self.last_series_id.store(state.last_series_id, Ordering::SeqCst);
                        self.logger.info(
                            "Fast startup: clean shutdown detected, restored last series ID",
                            &[("last_series_id", &state.last_series_id)],
                        );
                    } else {
                        self.logger.info(
                            "Fast startup: unclean shutdown detected, performing WAL scan",
                            &[("from_segment", &state.last_wal_segment), ("to_segment", &end_at)],
                        );
                        match self.find_last_series_id(&state, end_at) {
                            Ok(id) => {
                                self.last_series_id.store(id, Ordering::SeqCst);
                                self.logger.info("Fast startup: WAL scan completed", &[("last_series_id", &id)]);
                            }
                            Err(e) => self.logger.error(
                                "Fast startup: WAL scan failed, skipping fast startup",
                                &[("err", &e)],
                            ),
                        }
                    }
                }
                Err(e) => {
                    if !e.to_string().contains("no such file or directory") {
                        self.logger.warn(
                            "Failed to read series state file, skipping the fast startup",
                            &[("err", &e)],
                        );
                    }
                }
            }
        }

        self.start_wal_replay_status(start_from, end_at);

        let mut multi_ref: HashMap<HeadSeriesRef, HeadSeriesRef> = HashMap::new();
        if let Some(dir) = dir
            && start_from >= snap_idx
        {
            let sr = wlog::new_segments_reader(&dir).map_err(|e| Error::wrap("open checkpoint", wlog_err(e)))?;
            let mut r = wlog::Reader::new(sr);
            // A corrupted checkpoint is a hard error for now and requires user intervention.
            self.load_wal(&mut r, &mut multi_ref, &mmapped_chunks, &ooo_mmapped_chunks, last_mmap_ref)
                .map_err(|e| Error::wrap("backfill checkpoint", e))?;
            self.update_wal_replay_status_read(start_from);
            start_from += 1;
            self.logger.info("WAL checkpoint loaded", &[]);
        }
        let checkpoint_replay_duration = checkpoint_replay_start.elapsed();

        let wal_replay_start = Instant::now();
        if snap_idx > start_from {
            start_from = snap_idx;
        }
        // Backfill segments from the most recent checkpoint onwards.
        let mut i = start_from;
        while i <= end_at {
            let wal_segment_start = Instant::now();
            let s = wlog::open_read_segment(&wlog::segment_name(wal.dir(), i))
                .map_err(|e| Error::wrap(format!("open WAL segment: {}", i), wlog_err(e)))?;
            let offset = if i == snap_idx { snap_offset } else { 0 };
            let sr = match wlog::new_segment_buf_reader_with_offset(offset as usize, vec![s]) {
                Ok(sr) => sr,
                Err(e) if e.is_eof() => {
                    // File does not exist.
                    i += 1;
                    continue;
                }
                Err(e) => {
                    return Err(Error::wrap(format!("segment reader (offset={})", offset), wlog_err(e)));
                }
            };
            let mut r = wlog::Reader::new(sr);
            self.load_wal(&mut r, &mut multi_ref, &mmapped_chunks, &ooo_mmapped_chunks, last_mmap_ref)?;
            self.logger.info(
                "WAL segment loaded",
                &[
                    ("segment", &i),
                    ("maxSegment", &end_at),
                    ("duration", &format!("{:?}", wal_segment_start.elapsed())),
                ],
            );
            self.update_wal_replay_status_read(i);
            i += 1;
        }
        let wal_replay_duration = wal_replay_start.elapsed();

        let wbl_replay_start = Instant::now();
        if let Some(wbl) = self.wbl() {
            // Replay WBL.
            let (sf, ea) = wlog::segments(wbl.dir()).map_err(|e| {
                Error::other(crate::head_wal::ErrLoadWbl(Error::wrap("finding WBL segments", wlog_err(e))))
            })?;
            self.start_wal_replay_status(sf, ea);
            let mut i = sf;
            while i <= ea {
                let s = wlog::open_read_segment(&wlog::segment_name(wbl.dir(), i)).map_err(|e| {
                    Error::other(crate::head_wal::ErrLoadWbl(Error::wrap(
                        format!("open WBL segment: {}", i),
                        wlog_err(e),
                    )))
                })?;
                let sr = wlog::new_segment_buf_reader(vec![s]);
                let mut r = wlog::Reader::new(sr);
                self.load_wbl(&mut r, &mut multi_ref, last_mmap_ref)
                    .map_err(|e| Error::other(crate::head_wal::ErrLoadWbl(e)))?;
                self.logger.info("WBL segment loaded", &[("segment", &i), ("maxSegment", &ea)]);
                self.update_wal_replay_status_read(i);
                i += 1;
            }
        }
        let wbl_replay_duration = wbl_replay_start.elapsed();

        let total_replay_duration = start.elapsed();
        self.metrics.data_total_replay_duration.set(total_replay_duration.as_secs_f64());
        self.logger.info(
            "WAL replay completed",
            &[
                ("checkpoint_replay_duration", &format!("{:?}", checkpoint_replay_duration)),
                ("wal_replay_duration", &format!("{:?}", wal_replay_duration)),
                ("wbl_replay_duration", &format!("{:?}", wbl_replay_duration)),
                ("chunk_snapshot_load_duration", &format!("{:?}", chunk_snapshot_load_duration)),
                ("mmap_chunk_replay_duration", &format!("{:?}", mmap_chunk_replay_duration)),
                ("total_replay_duration", &format!("{:?}", total_replay_duration)),
            ],
        );
        if self.opts.enable_fast_startup {
            self.run_series_state_ticker();
        }
        Ok(())
    }

    /// Go `loadMmappedChunks`. On error, returns the error and the second-last chunk reference (the last one
    /// caused the error).
    #[allow(clippy::type_complexity)]
    pub(crate) fn load_mmapped_chunks(
        &self,
        ref_series: &HashMap<HeadSeriesRef, Arc<MemSeries>>,
    ) -> Result<
        (HashMap<HeadSeriesRef, Vec<MmappedChunk>>, HashMap<HeadSeriesRef, Vec<MmappedChunk>>, ChunkDiskMapperRef),
        (tsdb_format::chunks::Error, ChunkDiskMapperRef),
    > {
        let mut mmapped_chunks: HashMap<HeadSeriesRef, Vec<MmappedChunk>> = HashMap::new();
        let mut ooo_mmapped_chunks: HashMap<HeadSeriesRef, Vec<MmappedChunk>> = HashMap::new();
        let mut last_ref = ChunkDiskMapperRef(0);
        let mut second_last_ref = ChunkDiskMapperRef(0);
        let min_valid_time = self.min_valid_time.load(Ordering::SeqCst);
        let res = self.chunk_disk_mapper.iterate_all_chunks(
            |series_ref, chunk_ref, mint, maxt, num_samples, encoding, is_ooo| {
                second_last_ref = last_ref;
                last_ref = chunk_ref;
                if !is_ooo && maxt < min_valid_time {
                    return Ok(());
                }
                // We ignore any chunk that doesn't have a valid encoding.
                if !chunkenc::is_valid_encoding(encoding) {
                    return Ok(());
                }
                let mc = MmappedChunk {
                    r#ref: chunk_ref,
                    min_time: mint,
                    max_time: maxt,
                    num_samples,
                    encoding,
                };
                let ms = ref_series.get(&series_ref);
                if is_ooo {
                    let Some(ms) = ms else {
                        ooo_mmapped_chunks.entry(series_ref).or_default().push(mc);
                        return Ok(());
                    };
                    self.metrics.chunks.inc();
                    self.metrics.chunks_created.inc();
                    ms.lock().ooo_or_init().ooo_mmapped_chunks.push(mc);
                    self.update_min_ooo_max_ooo_time(mint, maxt);
                    return Ok(());
                }
                let Some(ms) = ms else {
                    let slice = mmapped_chunks.entry(series_ref).or_default();
                    if let Some(l) = slice.last()
                        && l.max_time >= mint
                    {
                        self.metrics.mmap_chunk_corruption_total.inc();
                        return Err(tsdb_format::chunks::Error::msg(format!(
                            "out of sequence m-mapped chunk for series ref {}, last chunk: [{}, {}], new: [{}, {}]",
                            series_ref, l.min_time, l.max_time, mint, maxt
                        )));
                    }
                    slice.push(mc);
                    return Ok(());
                };
                let mut g = ms.lock();
                if let Some(l) = g.mmapped_chunks.last()
                    && l.max_time >= mint
                {
                    self.metrics.mmap_chunk_corruption_total.inc();
                    return Err(tsdb_format::chunks::Error::msg(format!(
                        "out of sequence m-mapped chunk for series ref {}, last chunk: [{}, {}], new: [{}, {}]",
                        series_ref, l.min_time, l.max_time, mint, maxt
                    )));
                }
                self.metrics.chunks.inc();
                self.metrics.chunks_created.inc();
                g.mmapped_chunks.push(mc);
                self.update_min_max_time(mint, maxt);
                if let Some(hc) = g.head_chunks.last()
                    && maxt >= hc.min_time
                {
                    // The most recent head chunk was completed and m-mapped after the snapshot was taken, so its
                    // samples are now covered by the mmapped chunk we just loaded. Drop the head chunk chain.
                    g.next_at = 0;
                    if g.head_chunk_count() >= 2 {
                        self.series.dec_mmap_ready(ms.r#ref);
                    }
                    g.head_chunks = Vec::new();
                    g.app = SeriesAppender::None;
                }
                Ok(())
            },
        );
        match res {
            Ok(()) => Ok((mmapped_chunks, ooo_mmapped_chunks, last_ref)),
            Err(e) => Err((e, second_last_ref)),
        }
    }

    /// Go `removeCorruptedMmappedChunks`: deletes the corrupted mmapped chunks (or all of them if that fails).
    #[allow(clippy::type_complexity)]
    fn remove_corrupted_mmapped_chunks(
        &self,
        err: &tsdb_format::chunks::Error,
    ) -> Result<
        (HashMap<HeadSeriesRef, Vec<MmappedChunk>>, HashMap<HeadSeriesRef, Vec<MmappedChunk>>, ChunkDiskMapperRef),
        Error,
    > {
        self.logger.info("Deleting mmapped chunk files", &[]);
        // We never want to preserve the in-memory series from snapshots if we are repairing m-map chunks.
        self.reset_in_memory_state()?;
        self.logger.info("Deleting mmapped chunk files", &[]);
        if let Err(e) = self.chunk_disk_mapper.delete_corrupted(err) {
            self.logger.info(
                "Deletion of corrupted mmap chunk files failed, discarding chunk files completely",
                &[("err", &e)],
            );
            if let Err(e) = self.chunk_disk_mapper.truncate(u32::MAX) {
                self.logger.error("Deletion of all mmap chunk files failed", &[("err", &e)]);
            }
            return Ok((HashMap::new(), HashMap::new(), ChunkDiskMapperRef(0)));
        }
        self.logger.info("Deletion of mmap chunk files successful, reattempting m-mapping the on-disk chunks", &[]);
        match self.load_mmapped_chunks(&HashMap::new()) {
            Ok(r) => Ok(r),
            Err((e, last_ref)) => {
                self.logger.error(
                    "Loading on-disk chunks failed, discarding chunk files completely",
                    &[("err", &e)],
                );
                if let Err(e) = self.chunk_disk_mapper.truncate(u32::MAX) {
                    self.logger.error("Deletion of all mmap chunk files failed after failed loading", &[("err", &e)]);
                }
                Ok((HashMap::new(), HashMap::new(), last_ref))
            }
        }
    }
}

impl std::fmt::Display for Head {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("head")
    }
}

impl std::fmt::Debug for Head {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("head")
    }
}

impl BlockReader for Head {
    fn index(&self) -> Result<Box<dyn IndexReader>, Error> {
        Ok(Box::new(self.index_range(i64::MIN, i64::MAX)))
    }
    fn chunks(&self) -> Result<Box<dyn ChunkReader>, Error> {
        let iso = self.iso.state(i64::MIN, i64::MAX);
        Ok(Box::new(self.chunks_range(i64::MIN, i64::MAX, Some(iso))?))
    }
    fn tombstones(&self) -> Result<Arc<dyn tombstones::Reader>, Error> {
        Ok(self.tombstones.clone())
    }
    fn meta(&self) -> BlockMeta {
        Head::meta(self)
    }
    fn size(&self) -> i64 {
        Head::size(self)
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Fingerprints of series for selected-series compaction.

/// Go `seriesFingerprint`: an isolation-independent snapshot of a series' in-order head chunk shape.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SeriesFingerprint {
    pub current_chunk_id: HeadChunkId,
    pub last_chunk_samples: usize,
    pub watermark_violated_at_snapshot: bool,
}

/// Go `hasAppendIDAbove`: whether `s` contains a sample with an appendID greater than `watermark`.
pub fn has_append_id_above(s: &MemSeriesInner, watermark: u64) -> bool {
    let Some(txs) = &s.txs else {
        return false;
    };
    let mut it = txs.iterator();
    for _ in 0..txs.tx_id_count {
        if it.at() > watermark {
            return true;
        }
        it.next();
    }
    false
}

/// Go `currentChunkID`.
pub fn current_chunk_id(s: &MemSeriesInner) -> HeadChunkId {
    let total = s.mmapped_chunks.len() + s.head_chunks.len();
    if total == 0 {
        return 0;
    }
    s.head_chunk_id(total - 1)
}

/// Go `snapshotFingerprint`.
pub fn snapshot_fingerprint(s: &MemSeriesInner, append_id_watermark: u64) -> SeriesFingerprint {
    SeriesFingerprint {
        current_chunk_id: current_chunk_id(s),
        watermark_violated_at_snapshot: has_append_id_above(s, append_id_watermark),
        last_chunk_samples: s.head_chunks.last().map_or(0, |c| c.chunk.num_samples()),
    }
}

/// Go `fingerprintChanged`.
pub fn fingerprint_changed(s: &MemSeriesInner, fp: &SeriesFingerprint) -> bool {
    if current_chunk_id(s) != fp.current_chunk_id {
        return true;
    }
    s.head_chunks.last().map_or(0, |c| c.chunk.num_samples()) != fp.last_chunk_samples
}

/// Go `hasMutatedSinceSnapshot`. A missing fingerprint counts as mutated.
pub fn has_mutated_since_snapshot(
    series: &MemSeries,
    s: &MemSeriesInner,
    fingerprints: &HashMap<SeriesRef, SeriesFingerprint>,
) -> bool {
    let Some(fp) = fingerprints.get(&series.r#ref) else {
        return true;
    };
    if fp.watermark_violated_at_snapshot {
        return true;
    }
    fingerprint_changed(s, fp)
}

// ---------------------------------------------------------------------------------------------------------------
// RangeHead and SelectedSeriesHead.

/// Go `RangeHead`: the head restricted to [mint, maxt], for queries and compactions.
#[derive(Clone)]
pub struct RangeHead {
    pub(crate) head: Arc<Head>,
    pub(crate) mint: i64,
    pub(crate) maxt: i64,
    pub(crate) isolation_off: bool,
}

impl RangeHead {
    /// Go `NewRangeHead`.
    pub fn new(head: Arc<Head>, mint: i64, maxt: i64) -> RangeHead {
        RangeHead {
            head,
            mint,
            maxt,
            isolation_off: false,
        }
    }

    /// Go `NewRangeHeadWithIsolationDisabled`.
    pub fn new_with_isolation_disabled(head: Arc<Head>, mint: i64, maxt: i64) -> RangeHead {
        RangeHead {
            isolation_off: true,
            ..RangeHead::new(head, mint, maxt)
        }
    }

    pub fn head(&self) -> &Arc<Head> {
        &self.head
    }

    pub fn min_time(&self) -> i64 {
        self.mint
    }

    /// The max time of actual data fetch-able from the head.
    pub fn max_time(&self) -> i64 {
        self.maxt
    }

    /// Go `BlockMaxTime`: the max time of the potential block created from this head (+1, half-open).
    pub fn block_max_time(&self) -> i64 {
        self.max_time() + 1
    }

    pub fn num_series(&self) -> u64 {
        self.head.num_series()
    }
}

impl std::fmt::Display for RangeHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "range head (mint: {}, maxt: {})", self.mint, self.maxt)
    }
}

impl BlockReader for RangeHead {
    fn index(&self) -> Result<Box<dyn IndexReader>, Error> {
        Ok(Box::new(self.head.index_range(self.mint, self.maxt)))
    }
    fn chunks(&self) -> Result<Box<dyn ChunkReader>, Error> {
        let iso = if self.isolation_off {
            None
        } else {
            Some(self.head.iso.state(self.mint, self.maxt))
        };
        Ok(Box::new(self.head.chunks_range(self.mint, self.maxt, iso)?))
    }
    fn tombstones(&self) -> Result<Arc<dyn tombstones::Reader>, Error> {
        Ok(self.head.tombstones.clone())
    }
    fn meta(&self) -> BlockMeta {
        BlockMeta {
            min_time: self.mint,
            max_time: self.maxt,
            ulid: range_head_ulid(),
            stats: BlockStats {
                num_series: self.num_series(),
                ..Default::default()
            },
            ..Default::default()
        }
    }
    fn size(&self) -> i64 {
        self.head.size()
    }
}

/// Go `seriesRefs`: series references sorted by reference and by labels.
#[derive(Clone, Debug, Default)]
pub struct SeriesRefs {
    pub sorted_by_ref: Arc<Vec<SeriesRef>>,
    pub sorted_by_labels: Arc<Vec<SeriesRef>>,
}

/// Go `SelectedSeriesHead`: the head restricted to a list of series, for compactions.
#[derive(Clone)]
pub struct SelectedSeriesHead {
    pub range: RangeHead,
    pub selected_series_refs: SeriesRefs,
}

impl SelectedSeriesHead {
    /// Go `NewSelectedSeriesHead`.
    pub fn new(head: Arc<Head>, mint: i64, maxt: i64, selected_series_refs: SeriesRefs) -> SelectedSeriesHead {
        SelectedSeriesHead {
            range: RangeHead::new(head, mint, maxt),
            selected_series_refs,
        }
    }

    pub fn num_series(&self) -> u64 {
        self.selected_series_refs.sorted_by_ref.len() as u64
    }

    pub fn min_time(&self) -> i64 {
        self.range.mint
    }

    pub fn max_time(&self) -> i64 {
        self.range.maxt
    }
}

impl std::fmt::Display for SelectedSeriesHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "selected-series head (mint: {}, maxt: {})", self.range.mint, self.range.maxt)
    }
}

impl BlockReader for SelectedSeriesHead {
    fn index(&self) -> Result<Box<dyn IndexReader>, Error> {
        Ok(Box::new(self.range.head.selected_series_index(
            self.range.mint,
            self.range.maxt,
            self.selected_series_refs.clone(),
        )))
    }
    fn chunks(&self) -> Result<Box<dyn ChunkReader>, Error> {
        self.range.chunks()
    }
    fn tombstones(&self) -> Result<Arc<dyn tombstones::Reader>, Error> {
        self.range.tombstones()
    }
    fn meta(&self) -> BlockMeta {
        BlockMeta {
            min_time: self.range.mint,
            max_time: self.range.maxt,
            ulid: selected_series_head_ulid(),
            stats: BlockStats {
                num_series: self.num_series(),
                ..Default::default()
            },
            ..Default::default()
        }
    }
    fn size(&self) -> i64 {
        self.range.size()
    }
}
