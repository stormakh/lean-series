//! The time series database (port of `tsdb/db.go`): options, open, the compaction loop, retention, reload,
//! snapshots, deletion, queriers over blocks and the head, and the read-only DB.
//!
//! Rust notes:
//! - [`open`] returns an `Arc<DB>`. One background thread (Go `run`) waits on a condition variable with the
//!   block reload interval (one minute by default) as timeout, or for a compaction trigger (Go `compactc`). Nothing
//!   else wakes when the DB is idle. The thread holds only a weak reference to the DB while it waits.
//! - Go fields that tests read are public (`#[doc(hidden)]` where they are internals). Go's package-level testing
//!   callbacks are fields of the DB (Go tests set globals; Rust tests run in parallel).
//! - `ApplyConfig` takes plain values ([`ApplyConfig`]); this crate does not depend on the config crate.
//! - Go `HeadChunksWriteBufferSize` defaults to 4 MiB; here it defaults to the head's smallest allowed size
//!   (`chunks::MIN_WRITE_BUFFER_SIZE`, 64 KiB). The files are the same.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use chunkenc::{Encoding, Pool};
use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram as ModelHistogram};
use model::labels::Labels;
use model::matcher::Matcher;
use model::metadata::Metadata;
use storage::{
    AppendOptions, AppendV2Options, Appender, AppenderTransaction, AppenderV2, BoxChunkQuerier, BoxQuerier,
    CancelToken, Context, Error, ExemplarAppenderV2, GetRef, SampleAndChunkQueryable, SeriesRef, Sentinel,
};
use tsdb_format::chunks::{self, ChunkDiskMapperRef};
use tsdb_format::fileutil;
use tsdb_format::index;
use tsdb_format::wlog::{self, WL, compression};

use crate::block::{Block, PostingsDecoderFactory, open_block};
use crate::compact::{
    BlockExcludeFilterFunc, Compactor, LeveledCompactor, LeveledCompactorOptions, exponential_block_ranges,
    new_leveled_compactor, new_leveled_compactor_with_options,
};
use crate::head::{
    DEFAULT_OUT_OF_ORDER_CAP_MAX, DEFAULT_SAMPLES_PER_CHUNK, DEFAULT_STRIPE_SIZE, Head, HeadOptions, HeadStats,
    RangeHead, SelectedSeriesHead, SeriesLifecycleCallback, is_series_without_ooo, mmapped_chunks_dir,
};
use crate::head_wal::ErrLoadWbl;
use crate::meta::{BlockMeta, Ulid, read_meta_file};
use crate::metrics::{Counter, Gauge, Histogram};
use crate::ooo_head_read::{HeadAndOOOChunkQuerier, HeadAndOOOQuerier, OooCompactionHead};
use crate::querier::{BlockChunkQuerier, new_block_base_querier, new_block_querier};
use crate::query_cost::{CostSamplingQueryable, PinnedChunkQuerier};
use crate::reader::BlockReader;
use crate::tsdbutil::dir_locker::DirLocker;
use crate::tsdbutil::remove_tmp_dirs::remove_tmp_dirs;
use crate::{Logger, io_err_text};

/// Go `DefaultBlockDuration`: 2 hours in milliseconds.
pub const DEFAULT_BLOCK_DURATION: i64 = 2 * 60 * 60 * 1000;
/// Go `DefaultCompactionDelayMaxPercent`.
pub const DEFAULT_COMPACTION_DELAY_MAX_PERCENT: i32 = 10;
/// Go `MinBlockReloadInterval`.
pub const MIN_BLOCK_RELOAD_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_BLOCK_RELOAD_INTERVAL: Duration = Duration::from_secs(60);

/// Go `tmpForDeletionBlockDirSuffix`.
pub const TMP_FOR_DELETION_BLOCK_DIR_SUFFIX: &str = ".tmp-for-deletion";
/// Go `tmpForCreationBlockDirSuffix`.
pub const TMP_FOR_CREATION_BLOCK_DIR_SUFFIX: &str = ".tmp-for-creation";
/// Go `tmpLegacy`: the pre-2.21 tmp dir suffix.
pub const TMP_LEGACY: &str = ".tmp";
/// Go `chunkSnapshotPrefix`.
pub const CHUNK_SNAPSHOT_PREFIX: &str = "chunk_snapshot.";

static NOT_READY: Sentinel = Sentinel::new("TSDB not ready");
/// Go `ErrNotReady`: the underlying storage is not ready yet.
pub const ERR_NOT_READY: Error = Error::Sentinel(&NOT_READY);
static CLOSED: Sentinel = Sentinel::new("db already closed");
/// Go `ErrClosed`: the DB is closed.
pub const ERR_CLOSED: Error = Error::Sentinel(&CLOSED);

/// Go `config.FloatChunkEncodingXOR`.
pub const FLOAT_CHUNK_ENCODING_XOR: &str = "xor";
/// Go `config.FloatChunkEncodingXOR2`.
pub const FLOAT_CHUNK_ENCODING_XOR2: &str = "xor2";

fn err_xor_incompatible_with_st_storage() -> Error {
    Error::msg(format!(
        "float chunk encoding {:?} is incompatible with start-timestamp storage; XOR chunks do not store start timestamps, use {:?}",
        FLOAT_CHUNK_ENCODING_XOR, FLOAT_CHUNK_ENCODING_XOR2
    ))
}

// ---------------------------------------------------------------------------------------------------------------
// Options.

/// Go `NewCompactorFunc`.
pub type NewCompactorFunc =
    Arc<dyn Fn(&Context, Logger, &[i64], Arc<dyn Pool>, &Options) -> Result<Arc<dyn Compactor>, Error> + Send + Sync>;
/// Go `BlocksToDeleteFunc`: the blocks (by ULID) that can be deleted.
pub type BlocksToDeleteFunc = Arc<dyn Fn(&[Block]) -> HashSet<Ulid> + Send + Sync>;
/// Go `BlockQuerierFunc`.
pub type BlockQuerierFunc = Arc<dyn Fn(&dyn BlockReader, i64, i64) -> Result<BoxQuerier, Error> + Send + Sync>;
/// Go `BlockChunkQuerierFunc`.
pub type BlockChunkQuerierFunc =
    Arc<dyn Fn(&dyn BlockReader, i64, i64) -> Result<BoxChunkQuerier, Error> + Send + Sync>;
/// Go `FsSizeFunc`: the total disk size for a path.
pub type FsSizeFunc = Arc<dyn Fn(&Path) -> u64 + Send + Sync>;

/// Go `Options`: options of the DB storage. Durations are in milliseconds unless the type says otherwise.
#[derive(Clone)]
pub struct Options {
    /// WAL segment size: 0 = default (128 MiB), > 0 = that size, < 0 = WAL disabled.
    pub wal_segment_size: i64,
    /// Max size of block chunk segment files; <= 0 = default (512 MiB).
    pub max_block_chunk_segment_size: i64,
    /// Duration of persisted data to keep (0 = no time retention).
    pub retention_duration: i64,
    /// Maximum number of bytes in blocks (and WAL, head chunks) to retain; <= 0 = disabled.
    pub max_bytes: i64,
    /// Maximum % of disk space to use for blocks; <= 0 = disabled. Prevails over `max_bytes`.
    pub max_percentage: f64,
    /// Disables the lock file.
    pub no_lockfile: bool,
    /// The compression of WAL records.
    pub wal_compression: compression::Type,
    /// Maximum number of threads for WAL replay; <= 0 = the number of CPUs.
    pub wal_replay_concurrency: i32,
    /// The maximum number of shards of the head series maps.
    pub stripe_size: usize,
    /// The time range of head blocks after which they get persisted.
    pub min_block_duration: i64,
    /// The maximum time range of compacted blocks.
    pub max_block_duration: i64,
    /// The write buffer size of the head chunks mapper (<= 0 = the head default, 64 KiB).
    pub head_chunks_write_buffer_size: i64,
    /// The size of the chunk write queue of the head chunks mapper (< 0 = default).
    pub head_chunks_write_queue_size: i64,
    /// The target number of samples per chunk.
    pub samples_per_chunk: i64,
    /// Callbacks during the lifecycle of a series (no-op in Prometheus).
    pub series_lifecycle_callback: Option<Arc<dyn SeriesLifecycleCallback>>,
    /// The blocks that can be deleted; the default time and size retention when `None`.
    pub blocks_to_delete: Option<BlocksToDeleteFunc>,
    /// Enables the in-memory exemplar storage.
    pub enable_exemplar_storage: bool,
    /// Snapshot of the in-memory chunks on shutdown.
    pub enable_memory_snapshot_on_shutdown: bool,
    /// The size of the circular exemplar buffer.
    pub max_exemplars: i64,
    /// Disables isolation between reads and in-flight appends.
    pub isolation_disabled: bool,
    /// How much out of order is allowed (only used while opening; see [`DB::apply_config`]).
    pub out_of_order_time_window: i64,
    /// Maximum capacity of OOO chunks (samples); <= 0 = default.
    pub out_of_order_cap_max: i64,
    /// Compaction of overlapping blocks.
    pub enable_overlapping_compaction: bool,
    /// Query sharding support.
    pub enable_sharding: bool,
    /// Assigns a random value to `compaction_delay` when opening.
    pub enable_delayed_compaction: bool,
    /// Delays the start time of auto compactions.
    pub compaction_delay: Duration,
    /// The upper limit of `compaction_delay`, in percent of the head chunk range.
    pub compaction_delay_max_percent: i32,
    /// Creates the compactor (the leveled compactor when `None`).
    pub new_compactor_func: Option<NewCompactorFunc>,
    /// Creates block queriers (`new_block_querier` when `None`).
    pub block_querier_func: Option<BlockQuerierFunc>,
    /// Creates block chunk queriers (`new_block_chunk_querier` when `None`).
    pub block_chunk_querier_func: Option<BlockChunkQuerierFunc>,
    /// Customizes postings decoders per block.
    pub postings_decoder_factory: Option<PostingsDecoderFactory>,
    /// Bypass the page cache when appropriate.
    pub use_uncached_io: bool,
    /// The `created-timestamp-zero-ingestion` feature flag.
    pub enable_st_as_zero_sample: bool,
    /// The ST-capable histogram chunk encodings.
    pub enable_histogram_st_encoding: bool,
    /// Writes a start timestamp per sample to the WAL.
    pub enable_st_storage: bool,
    /// The `metadata-wal-records` feature flag.
    pub enable_metadata_wal_records: bool,
    /// Blocks that should not be compacted.
    pub block_compaction_exclude_func: Option<BlockExcludeFilterFunc>,
    /// The interval of block reloads (zero = one minute; values below one second are clamped).
    pub block_reload_interval: Duration,
    /// The encoding of new float chunks (XOR or XOR2; `ENC_NONE` = XOR).
    pub float_chunk_encoding: Encoding,
    /// The fraction (0.0 to 1.0) of stale head series above which stale series compaction runs.
    pub stale_series_compaction_threshold: f64,
    /// The total disk size of a path (statfs when `None`).
    pub fs_size_func: Option<FsSizeFunc>,
    /// Scraping in parallel with WAL replay.
    pub enable_fast_startup: bool,
}

impl Default for Options {
    /// Go `DefaultOptions`.
    fn default() -> Options {
        default_options()
    }
}

/// Go `DefaultOptions`: reasonable options for millisecond timestamps.
pub fn default_options() -> Options {
    Options {
        wal_segment_size: wlog::DEFAULT_SEGMENT_SIZE as i64,
        max_block_chunk_segment_size: chunks::DEFAULT_CHUNK_SEGMENT_SIZE,
        retention_duration: 15 * 24 * 60 * 60 * 1000,
        max_bytes: 0,
        max_percentage: 0.0,
        no_lockfile: false,
        wal_compression: compression::Type::None,
        wal_replay_concurrency: 0,
        stripe_size: DEFAULT_STRIPE_SIZE,
        min_block_duration: DEFAULT_BLOCK_DURATION,
        max_block_duration: DEFAULT_BLOCK_DURATION,
        head_chunks_write_buffer_size: chunks::MIN_WRITE_BUFFER_SIZE as i64,
        head_chunks_write_queue_size: chunks::DEFAULT_WRITE_QUEUE_SIZE as i64,
        samples_per_chunk: DEFAULT_SAMPLES_PER_CHUNK as i64,
        series_lifecycle_callback: None,
        blocks_to_delete: None,
        enable_exemplar_storage: false,
        enable_memory_snapshot_on_shutdown: false,
        max_exemplars: 0,
        isolation_disabled: false,
        out_of_order_time_window: 0,
        out_of_order_cap_max: DEFAULT_OUT_OF_ORDER_CAP_MAX,
        enable_overlapping_compaction: true,
        enable_sharding: false,
        enable_delayed_compaction: false,
        compaction_delay: Duration::ZERO,
        compaction_delay_max_percent: DEFAULT_COMPACTION_DELAY_MAX_PERCENT,
        new_compactor_func: None,
        block_querier_func: None,
        block_chunk_querier_func: None,
        postings_decoder_factory: None,
        use_uncached_io: false,
        enable_st_as_zero_sample: false,
        enable_histogram_st_encoding: false,
        enable_st_storage: false,
        enable_metadata_wal_records: false,
        block_compaction_exclude_func: None,
        block_reload_interval: DEFAULT_BLOCK_RELOAD_INTERVAL,
        float_chunk_encoding: chunkenc::ENC_XOR,
        stale_series_compaction_threshold: 0.0,
        fs_size_func: None,
        enable_fast_startup: false,
    }
}

/// An all-zero `Options` (Go `&tsdb.Options{}` literal): the values `validate_opts` fills in.
pub fn zero_options() -> Options {
    Options {
        wal_segment_size: 0,
        max_block_chunk_segment_size: 0,
        retention_duration: 0,
        stripe_size: 0,
        min_block_duration: 0,
        max_block_duration: 0,
        head_chunks_write_buffer_size: 0,
        head_chunks_write_queue_size: 0,
        samples_per_chunk: 0,
        out_of_order_cap_max: 0,
        enable_overlapping_compaction: false,
        compaction_delay_max_percent: 0,
        block_reload_interval: Duration::ZERO,
        float_chunk_encoding: chunkenc::ENC_NONE,
        ..default_options()
    }
}

/// The options that the `prometheus` server uses by default (`cmd/prometheus/main.go`, `ToTSDBOptions` with
/// the flag defaults): 15 days of retention, 2h head blocks, max block duration 10% of the retention (at most
/// 31 days), Snappy WAL compression, 128 MiB WAL segments, OOO window 0, no memory snapshot, exemplar storage off.
/// `enable_exemplar_storage` sets the `exemplar-storage` feature (100000 exemplars, the config default).
pub fn prometheus_options(retention_duration: i64, enable_exemplar_storage: bool) -> Options {
    let mut retention = retention_duration;
    if retention < 0 {
        // Limits the max retention to 100y.
        retention = 100 * 365 * 24 * 60 * 60 * 1000;
    }
    let mut max_block = 31 * 24 * 60 * 60 * 1000;
    if retention != 0 && retention / 10 < max_block {
        max_block = retention / 10;
    }
    Options {
        wal_segment_size: 0,
        max_block_chunk_segment_size: 0,
        retention_duration: retention,
        max_bytes: 0,
        max_percentage: 0.0,
        no_lockfile: false,
        wal_compression: compression::Type::Snappy,
        head_chunks_write_queue_size: 0,
        samples_per_chunk: 120,
        stripe_size: 0,
        min_block_duration: DEFAULT_BLOCK_DURATION,
        max_block_duration: max_block,
        enable_exemplar_storage,
        max_exemplars: if enable_exemplar_storage { 100_000 } else { 0 },
        enable_memory_snapshot_on_shutdown: false,
        out_of_order_time_window: 0,
        out_of_order_cap_max: 0,
        enable_delayed_compaction: false,
        compaction_delay_max_percent: DEFAULT_COMPACTION_DELAY_MAX_PERCENT,
        enable_overlapping_compaction: true,
        block_reload_interval: DEFAULT_BLOCK_RELOAD_INTERVAL,
        float_chunk_encoding: chunkenc::ENC_XOR,
        head_chunks_write_buffer_size: 0,
        ..zero_options()
    }
}

/// Go `validateOpts`: fills in defaults; computes the block ranges when `rngs` is empty.
pub fn validate_opts(opts: Option<Options>, rngs: Vec<i64>) -> Result<(Options, Vec<i64>), Error> {
    let mut opts = opts.unwrap_or_else(default_options);
    if opts.float_chunk_encoding == chunkenc::ENC_NONE {
        opts.float_chunk_encoding = chunkenc::ENC_XOR;
    }
    if opts.float_chunk_encoding != chunkenc::ENC_XOR && opts.float_chunk_encoding != chunkenc::ENC_XOR2 {
        return Err(Error::msg(format!(
            "unsupported float chunk encoding {:?}; valid values are {:?} and {:?}",
            opts.float_chunk_encoding.to_string().to_lowercase(),
            FLOAT_CHUNK_ENCODING_XOR,
            FLOAT_CHUNK_ENCODING_XOR2
        )));
    }
    if opts.enable_st_storage && opts.float_chunk_encoding == chunkenc::ENC_XOR {
        return Err(err_xor_incompatible_with_st_storage());
    }
    if opts.stripe_size == 0 {
        opts.stripe_size = DEFAULT_STRIPE_SIZE;
    }
    if opts.head_chunks_write_buffer_size <= 0 {
        opts.head_chunks_write_buffer_size = chunks::MIN_WRITE_BUFFER_SIZE as i64;
    }
    if opts.head_chunks_write_queue_size < 0 {
        opts.head_chunks_write_queue_size = chunks::DEFAULT_WRITE_QUEUE_SIZE as i64;
    }
    if opts.samples_per_chunk <= 0 {
        opts.samples_per_chunk = DEFAULT_SAMPLES_PER_CHUNK as i64;
    }
    if opts.max_block_chunk_segment_size <= 0 {
        opts.max_block_chunk_segment_size = chunks::DEFAULT_CHUNK_SEGMENT_SIZE;
    }
    if opts.min_block_duration <= 0 {
        opts.min_block_duration = DEFAULT_BLOCK_DURATION;
    }
    if opts.min_block_duration > opts.max_block_duration {
        opts.max_block_duration = opts.min_block_duration;
    }
    if opts.out_of_order_cap_max <= 0 {
        opts.out_of_order_cap_max = DEFAULT_OUT_OF_ORDER_CAP_MAX;
    }
    if opts.out_of_order_time_window < 0 {
        opts.out_of_order_time_window = 0;
    }
    if opts.block_reload_interval.is_zero() {
        opts.block_reload_interval = DEFAULT_BLOCK_RELOAD_INTERVAL;
    } else if opts.block_reload_interval < MIN_BLOCK_RELOAD_INTERVAL {
        opts.block_reload_interval = MIN_BLOCK_RELOAD_INTERVAL;
    }
    let rngs = if rngs.is_empty() {
        // Start with smallest block duration and create exponential buckets until the exceed the configured
        // maximum block duration.
        exponential_block_ranges(opts.min_block_duration, 10, 3)
    } else {
        rngs
    };
    Ok((opts, rngs))
}

// ---------------------------------------------------------------------------------------------------------------
// Metrics and stats.

/// Go `dbMetrics`. The gauge functions (`prometheus_tsdb_blocks_loaded`, `prometheus_tsdb_symbol_table_size_bytes`,
/// `prometheus_tsdb_lowest_timestamp`) are [`DB::loaded_blocks`], [`DB::symbol_table_size`] and
/// [`DB::lowest_timestamp`].
#[derive(Debug)]
pub struct DbMetrics {
    /// `prometheus_tsdb_reloads_total`
    pub reloads: Counter,
    /// `prometheus_tsdb_reloads_failures_total`
    pub reloads_failed: Counter,
    /// `prometheus_tsdb_compactions_failed_total`
    pub compactions_failed: Counter,
    /// `prometheus_tsdb_compactions_triggered_total`
    pub compactions_triggered: Counter,
    /// `prometheus_tsdb_compactions_skipped_total`
    pub compactions_skipped: Counter,
    /// `prometheus_tsdb_size_retentions_total`
    pub size_retention_count: Counter,
    /// `prometheus_tsdb_time_retentions_total`
    pub time_retention_count: Counter,
    /// `prometheus_tsdb_tombstone_cleanup_seconds` (a native histogram upstream)
    pub tomb_clean_timer: Histogram,
    /// `prometheus_tsdb_storage_blocks_bytes`
    pub blocks_bytes: Gauge,
    /// `prometheus_tsdb_retention_limit_bytes`
    pub max_bytes: Gauge,
    /// `prometheus_tsdb_retention_limit_percentage`
    pub max_percentage: Gauge,
    /// `prometheus_tsdb_retention_limit_seconds`
    pub retention_duration: Gauge,
    /// `prometheus_tsdb_stale_series_compactions_triggered_total`
    pub stale_series_compactions_triggered: Counter,
    /// `prometheus_tsdb_stale_series_compactions_failed_total`
    pub stale_series_compactions_failed: Counter,
    /// `prometheus_tsdb_stale_series_compaction_duration_seconds`
    pub stale_series_compaction_duration: Histogram,
    /// `prometheus_tsdb_selected_series_compactions_triggered_total`
    pub selected_series_compactions_triggered: Counter,
    /// `prometheus_tsdb_selected_series_compactions_failed_total`
    pub selected_series_compactions_failed: Counter,
    /// `prometheus_tsdb_selected_series_compaction_duration_seconds`
    pub selected_series_compaction_duration: Histogram,
}

impl DbMetrics {
    fn new() -> DbMetrics {
        DbMetrics {
            reloads: Counter::new(),
            reloads_failed: Counter::new(),
            compactions_failed: Counter::new(),
            compactions_triggered: Counter::new(),
            compactions_skipped: Counter::new(),
            size_retention_count: Counter::new(),
            time_retention_count: Counter::new(),
            tomb_clean_timer: Histogram::new(Vec::new()),
            blocks_bytes: Gauge::new(),
            max_bytes: Gauge::new(),
            max_percentage: Gauge::new(),
            retention_duration: Gauge::new(),
            stale_series_compactions_triggered: Counter::new(),
            stale_series_compactions_failed: Counter::new(),
            stale_series_compaction_duration: Histogram::new(Histogram::exponential_buckets(1.0, 2.0, 14)),
            selected_series_compactions_triggered: Counter::new(),
            selected_series_compactions_failed: Counter::new(),
            selected_series_compaction_duration: Histogram::new(Histogram::exponential_buckets(1.0, 2.0, 14)),
        }
    }
}

/// Go `DBStats`: statistics by component, available before the DB finished initializing.
#[derive(Clone)]
pub struct DbStats {
    pub head: Arc<HeadStats>,
}

impl DbStats {
    /// Go `NewDBStats`.
    pub fn new() -> DbStats {
        DbStats { head: HeadStats::new() }
    }
}

impl Default for DbStats {
    fn default() -> DbStats {
        DbStats::new()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// The DB.

/// The block layout (Go `db.blocks` and `db.lastGarbageCollectedMmapRef`, guarded by `db.mtx`).
#[derive(Default)]
pub struct BlocksState {
    pub blocks: Vec<Block>,
    /// The last OOO chunk that was compacted and written to disk. New queriers must not read chunks less than or
    /// equal to this reference, as these chunks could be garbage collected at any time.
    pub last_garbage_collected_mmap_ref: ChunkDiskMapperRef,
}

/// State guarded by the compaction mutex (Go `db.cmtx` and the fields it guards).
#[derive(Default)]
pub struct CompactionState {
    /// Helps delay the start time of compactions (Go `timeWhenCompactionDelayStarted`; `None` = zero time).
    pub time_when_compaction_delay_started: Option<Instant>,
    /// The last wall clock time when the head compaction was started (Go `lastHeadCompactionTime`).
    pub last_head_compaction_time: Option<Instant>,
}

#[derive(Default)]
struct RunState {
    /// Go `compactc` holds a value.
    compact: bool,
    /// Go `stopc` is closed.
    stopped: bool,
    /// The number of processed compaction triggers (for tests).
    processed: u64,
}

/// The wake-up state of the background thread (Go `compactc`, `stopc`).
#[derive(Default)]
struct RunCtl {
    state: Mutex<RunState>,
    cv: Condvar,
}

impl RunCtl {
    fn lock(&self) -> MutexGuard<'_, RunState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// A testing callback (Go package-level `func()` variables, called once and then cleared).
pub type TestingCallback = Mutex<Option<Box<dyn FnOnce() + Send>>>;

/// Go `DB`: reads and writes of time series data in a directory.
pub struct DB {
    me: Weak<DB>,
    dir: PathBuf,
    #[doc(hidden)]
    pub locker: Mutex<Option<DirLocker>>,
    #[doc(hidden)]
    pub logger: Logger,
    pub metrics: DbMetrics,
    /// The options (Go `db.opts`; the retention values are guarded by this lock, Go `retentionMtx`).
    #[doc(hidden)]
    pub opts: RwLock<Options>,
    stale_series_compaction_threshold: AtomicU64,
    #[doc(hidden)]
    pub chunk_pool: Arc<dyn Pool>,
    /// The compactor (tests replace it).
    #[doc(hidden)]
    pub compactor: RwLock<Arc<dyn Compactor>>,
    /// The leveled compactor, when the DB made it (tests read its metrics and ranges).
    #[doc(hidden)]
    pub leveled_compactor: Option<Arc<LeveledCompactor>>,
    /// Go `db.blocksToDelete` (the default retention when `None`).
    #[doc(hidden)]
    pub blocks_to_delete: RwLock<Option<BlocksToDeleteFunc>>,
    /// Go `db.mtx` with the block layout.
    #[doc(hidden)]
    pub mtx: RwLock<BlocksState>,
    #[doc(hidden)]
    pub head: Arc<Head>,
    run: Arc<RunCtl>,
    run_handle: Mutex<Option<JoinHandle<()>>>,
    closed: AtomicBool,
    /// Go `db.cmtx`: compactions and deletions don't run simultaneously.
    #[doc(hidden)]
    pub cmtx: Mutex<CompactionState>,
    /// Go `autoCompactMtx` with `autoCompact`.
    #[doc(hidden)]
    pub auto_compact: Mutex<bool>,
    compact_ctx: Context,
    compact_cancel: CancelToken,
    /// Go `oooWasEnabled`: out-of-order support was enabled at least once while the DB was up.
    #[doc(hidden)]
    pub ooo_was_enabled: AtomicBool,
    #[doc(hidden)]
    pub block_querier_func: RwLock<Option<BlockQuerierFunc>>,
    /// `None` gives native block chunk queriers (what cost sampling accepts).
    #[doc(hidden)]
    pub block_chunk_querier_func: RwLock<Option<BlockChunkQuerierFunc>>,
    fs_size_func: FsSizeFunc,
    cost_sampling: CostSamplingQueryable<Box<dyn Fn(i64, i64) -> Result<Vec<PinnedChunkQuerier>, Error> + Send + Sync>>,
    /// Go `compactOOOHeadTestingCallback`.
    #[doc(hidden)]
    pub compact_ooo_head_testing_callback: TestingCallback,
    /// Go `compactHeadViewBeforeEvictTestingCallback`.
    #[doc(hidden)]
    pub compact_head_view_before_evict_testing_callback: TestingCallback,
    /// Milliseconds added to the wall clock (tests that need to move time, Go `synctest`).
    #[doc(hidden)]
    pub test_time_offset_ms: AtomicI64,
}

fn rlock<T>(l: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(|p| p.into_inner())
}

fn wlock<T>(l: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(|p| p.into_inner())
}

fn lock<T>(l: &Mutex<T>) -> MutexGuard<'_, T> {
    l.lock().unwrap_or_else(|p| p.into_inner())
}

/// Go `prom_runtime.FsSize`: the total size of the file system of `path` (0 if unknown).
pub fn fs_size(path: &Path) -> u64 {
    use std::os::unix::ffi::OsStrExt;
    let Ok(c) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return 0;
    };
    // SAFETY: `c` is a valid NUL-terminated path and `st` is a zeroed statfs the call fills in.
    unsafe {
        let mut st: libc::statfs = std::mem::zeroed();
        if libc::statfs(c.as_ptr(), &mut st) != 0 {
            return 0;
        }
        (st.f_blocks as u64).wrapping_mul(st.f_bsize as u64)
    }
}

fn io_wrap(prefix: impl std::fmt::Display, e: std::io::Error) -> Error {
    Error::msg(format!("{}: {}", prefix, io_err_text(&e)))
}

/// Go `Open`: a new DB in the given directory; `None` options use [`default_options`].
pub fn open(dir: &Path, logger: Option<Logger>, opts: Option<Options>, stats: Option<DbStats>) -> Result<Arc<DB>, Error> {
    let (opts, rngs) = validate_opts(opts, Vec::new())?;
    open_with_ranges(dir, logger, opts, rngs, stats)
}

/// Go `open`: a new DB with the given (validated) options and block ranges. It initializes the lock file, the WAL,
/// the compactor and the head (by replaying the WAL), and runs the database. Do not open more than one DB in the
/// same directory.
pub fn open_with_ranges(
    dir: &Path,
    logger: Option<Logger>,
    opts: Options,
    mut rngs: Vec<i64>,
    stats: Option<DbStats>,
) -> Result<Arc<DB>, Error> {
    std::fs::create_dir_all(dir).map_err(|e| io_wrap(format!("mkdir {}", dir.display()), e))?;
    let l = logger.unwrap_or_else(Logger::nop);
    let stats = stats.unwrap_or_default();

    if let Some(i) = rngs.iter().position(|&v| v > opts.max_block_duration) {
        rngs.truncate(i);
    }

    // Fixup bad format written by Prometheus 2.1.
    crate::repair::repair_bad_index_version(&l, dir).map_err(|e| Error::wrap("repair bad index version", e))?;

    let wal_dir = dir.join("wal");
    let wbl_dir = dir.join(wlog::WBL_DIR_NAME);

    for tmp_dir in [&wal_dir, &dir.to_path_buf()] {
        // Remove tmp dirs.
        remove_tmp_dirs(&l, tmp_dir, is_tmp_dir).map_err(|e| Error::wrap("remove tmp dirs", e))?;
        // Remove any temporary checkpoints that might have been interrupted during creation.
        wlog::delete_temp_checkpoints(tmp_dir)
            .map_err(|e| Error::wrap("delete temp checkpoints", Error::other(e)))?;
    }

    let mut locker = DirLocker::new(dir, "tsdb", l.clone())?;
    if !opts.no_lockfile {
        locker.lock()?;
    }
    let release = |mut locker: DirLocker, e: Error| -> Error {
        match locker.release() {
            Ok(()) => e,
            Err(e2) => Error::join([Some(e), Some(e2)]).expect("errors"),
        }
    };

    let chunk_pool: Arc<dyn Pool> = Arc::new(chunkenc::new_pool());
    let (compact_ctx, compact_cancel) = Context::background().with_cancel();
    let head_cell: Arc<OnceLock<Arc<Head>>> = Arc::new(OnceLock::new());
    let startup_encoding = opts.float_chunk_encoding;
    let mut leveled_compactor = None;
    let compactor: Result<Arc<dyn Compactor>, Error> = match &opts.new_compactor_func {
        Some(f) => f(&compact_ctx, l.clone(), &rngs, chunk_pool.clone(), &opts),
        None => {
            let cell = head_cell.clone();
            let float_encoding: storage::FloatEncodingFn = Arc::new(move || match cell.get() {
                Some(h) => Encoding(h.opts.float_chunk_encoding.load(Ordering::Relaxed) as u8),
                None => startup_encoding,
            });
            new_leveled_compactor_with_options(
                &compact_ctx,
                Some(l.clone()),
                rngs.clone(),
                Some(chunk_pool.clone()),
                LeveledCompactorOptions {
                    max_block_chunk_segment_size: opts.max_block_chunk_segment_size,
                    enable_overlapping_compaction: opts.enable_overlapping_compaction,
                    pd: opts.postings_decoder_factory.clone(),
                    use_uncached_io: opts.use_uncached_io,
                    block_exclude_filter: opts.block_compaction_exclude_func.clone(),
                    float_chunk_encoding: Some(float_encoding),
                    ..Default::default()
                },
            )
            .map(|c| {
                let c = Arc::new(c);
                leveled_compactor = Some(c.clone());
                c as Arc<dyn Compactor>
            })
        }
    };
    let compactor = match compactor {
        Ok(c) => c,
        Err(e) => {
            compact_cancel.cancel();
            return Err(release(locker, Error::wrap("create compactor", e)));
        }
    };

    let fs_size_func: FsSizeFunc = opts.fs_size_func.clone().unwrap_or_else(|| Arc::new(fs_size));

    let mut wal = None;
    let mut wbl = None;
    // Wal is enabled.
    if opts.wal_segment_size >= 0 {
        let mut segment_size = wlog::DEFAULT_SEGMENT_SIZE;
        // Wal is set to a custom size.
        if opts.wal_segment_size > 0 {
            segment_size = opts.wal_segment_size as usize;
        }
        let w = match WL::new_size(&wal_dir, segment_size, opts.wal_compression) {
            Ok(w) => Arc::new(w),
            Err(e) => return Err(release(locker, Error::other(e))),
        };
        wal = Some(w);
        // Check if there is a WBL on disk, in which case we should replay that data.
        let wbl_size = match fileutil::dir_size(&wbl_dir) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => {
                let _ = wal.as_ref().map(|w| w.close());
                return Err(release(locker, Error::other(e)));
            }
        };
        if opts.out_of_order_time_window > 0 || wbl_size > 0 {
            match WL::new_size(&wbl_dir, segment_size, opts.wal_compression) {
                Ok(w) => wbl = Some(Arc::new(w)),
                Err(e) => {
                    let _ = wal.as_ref().map(|w| w.close());
                    return Err(release(locker, Error::other(e)));
                }
            }
        }
    }

    let head_opts = HeadOptions {
        chunk_range: rngs[0],
        chunk_dir_root: dir.to_path_buf(),
        chunk_pool: Some(chunk_pool.clone()),
        chunk_write_buffer_size: opts.head_chunks_write_buffer_size as usize,
        chunk_write_queue_size: opts.head_chunks_write_queue_size as usize,
        samples_per_chunk: opts.samples_per_chunk as usize,
        stripe_size: opts.stripe_size,
        series_callback: opts.series_lifecycle_callback.clone(),
        enable_exemplar_storage: opts.enable_exemplar_storage,
        enable_memory_snapshot_on_shutdown: std::sync::atomic::AtomicBool::new(opts.enable_memory_snapshot_on_shutdown),
        enable_sharding: opts.enable_sharding,
        enable_st_as_zero_sample: opts.enable_st_as_zero_sample,
        enable_metadata_wal_records: opts.enable_metadata_wal_records,
        enable_fast_startup: opts.enable_fast_startup,
        logger: l.clone(),
        ..HeadOptions::default()
    };
    head_opts.max_exemplars.store(opts.max_exemplars, Ordering::Relaxed);
    head_opts.out_of_order_time_window.store(opts.out_of_order_time_window, Ordering::Relaxed);
    head_opts.out_of_order_cap_max.store(opts.out_of_order_cap_max, Ordering::Relaxed);
    head_opts.enable_st_storage.store(opts.enable_st_storage, Ordering::Relaxed);
    head_opts.float_chunk_encoding.store(opts.float_chunk_encoding.0 as u32, Ordering::Relaxed);
    head_opts.enable_histogram_st_encoding.store(opts.enable_histogram_st_encoding, Ordering::Relaxed);
    let mut head_opts = head_opts;
    if opts.wal_replay_concurrency > 0 {
        head_opts.wal_replay_concurrency = opts.wal_replay_concurrency;
    }
    if opts.isolation_disabled {
        // We only override this flag if isolation is disabled at DB level. We use the default otherwise.
        head_opts.isolation_disabled = opts.isolation_disabled;
    }
    let head = match Head::new(wal.clone(), wbl.clone(), head_opts, Some(stats.head.clone())) {
        Ok(h) => h,
        Err(e) => {
            // If head was never initialized, WAL/WBL need explicit cleanup.
            let mut errs = vec![Some(e)];
            if let Some(w) = &wal {
                errs.push(w.close().err().map(Error::other));
            }
            if let Some(w) = &wbl {
                errs.push(w.close().err().map(Error::other));
            }
            compact_cancel.cancel();
            return Err(release(locker, Error::join(errs).expect("errors")));
        }
    };
    let _ = head_cell.set(head.clone());

    let ooo_enabled = opts.out_of_order_time_window > 0;
    let block_querier_func = opts.block_querier_func.clone();
    let block_chunk_querier_func = opts.block_chunk_querier_func.clone();
    let blocks_to_delete = opts.blocks_to_delete.clone();
    let threshold = opts.stale_series_compaction_threshold;
    let enable_delayed_compaction = opts.enable_delayed_compaction;

    let db = Arc::new_cyclic(|me: &Weak<DB>| {
        let weak = me.clone();
        let f: Box<dyn Fn(i64, i64) -> Result<Vec<PinnedChunkQuerier>, Error> + Send + Sync> =
            Box::new(move |mint, maxt| match weak.upgrade() {
                Some(db) => db.block_chunk_querier_for_range(mint, maxt),
                None => Err(ERR_CLOSED),
            });
        DB {
            me: me.clone(),
            dir: dir.to_path_buf(),
            locker: Mutex::new(Some(locker)),
            logger: l.clone(),
            metrics: DbMetrics::new(),
            opts: RwLock::new(opts),
            stale_series_compaction_threshold: AtomicU64::new(threshold.to_bits()),
            chunk_pool,
            compactor: RwLock::new(compactor),
            leveled_compactor,
            blocks_to_delete: RwLock::new(blocks_to_delete),
            mtx: RwLock::new(BlocksState::default()),
            head,
            run: Arc::new(RunCtl::default()),
            run_handle: Mutex::new(None),
            closed: AtomicBool::new(false),
            cmtx: Mutex::new(CompactionState::default()),
            auto_compact: Mutex::new(true),
            compact_ctx,
            compact_cancel,
            ooo_was_enabled: AtomicBool::new(ooo_enabled),
            block_querier_func: RwLock::new(block_querier_func),
            block_chunk_querier_func: RwLock::new(block_chunk_querier_func),
            fs_size_func,
            cost_sampling: CostSamplingQueryable(f),
            compact_ooo_head_testing_callback: Mutex::new(None),
            compact_head_view_before_evict_testing_callback: Mutex::new(None),
            test_time_offset_ms: AtomicI64::new(0),
        }
    });

    match db.open_rest(wal.as_ref(), wbl.as_ref(), enable_delayed_compaction) {
        Ok(()) => {}
        Err(e) => {
            // Close files if startup fails somewhere.
            let mut errs = vec![Some(e)];
            if let Some(mut locker) = lock(&db.locker).take() {
                errs.push(locker.release().err());
            }
            db.closed.store(true, Ordering::SeqCst);
            db.compact_cancel.cancel();
            errs.push(db.head.close().err().map(|e| Error::wrap("close DB after failed startup", e)));
            return Err(Error::join(errs).expect("errors"));
        }
    }

    let weak = Arc::downgrade(&db);
    let run = db.run.clone();
    let ctx = db.compact_ctx.clone();
    let handle = std::thread::Builder::new()
        .name("tsdb-compact".into())
        .spawn(move || run_loop(weak, run, ctx))
        .map_err(|e| Error::msg(format!("start compaction thread: {e}")))?;
    *lock(&db.run_handle) = Some(handle);
    Ok(db)
}

/// Go `run`: reloads blocks every block reload interval and compacts when triggered.
fn run_loop(db: Weak<DB>, run: Arc<RunCtl>, ctx: Context) {
    let mut backoff = Duration::ZERO;
    loop {
        {
            let g = run.lock();
            let (g, _) = run.cv.wait_timeout_while(g, backoff, |s| !s.stopped).unwrap_or_else(|p| p.into_inner());
            if g.stopped {
                return;
            }
        }
        let interval = match db.upgrade() {
            Some(d) => rlock(&d.opts).block_reload_interval,
            None => return,
        };
        let compact = {
            let g = run.lock();
            let (mut g, _) = run
                .cv
                .wait_timeout_while(g, interval, |s| !s.stopped && !s.compact)
                .unwrap_or_else(|p| p.into_inner());
            if g.stopped {
                return;
            }
            std::mem::replace(&mut g.compact, false)
        };
        let Some(db) = db.upgrade() else { return };
        if !compact {
            db.on_reload_tick();
        } else {
            db.metrics.compactions_triggered.inc();
            {
                let ac = lock(&db.auto_compact);
                if *ac {
                    match db.compact(&ctx) {
                        Err(e) => {
                            db.logger.error("compaction failed", &[("err", &e)]);
                            backoff = exponential(backoff, Duration::from_secs(1), Duration::from_secs(60));
                        }
                        Ok(()) => backoff = Duration::ZERO,
                    }
                } else {
                    db.metrics.compactions_skipped.inc();
                }
            }
            let mut g = run.lock();
            g.processed += 1;
            run.cv.notify_all();
        }
    }
}

/// Go `exponential`.
fn exponential(d: Duration, min_d: Duration, max_d: Duration) -> Duration {
    (d * 2).clamp(min_d, max_d)
}

/// Go `rangeForTimestamp`.
pub fn range_for_timestamp(t: i64, width: i64) -> i64 {
    (t / width) * width + width
}

impl DB {
    fn arc(&self) -> Arc<DB> {
        self.me.upgrade().expect("db is alive")
    }

    /// The current time (moved by `test_time_offset_ms` in tests).
    fn now(&self) -> Instant {
        let off = self.test_time_offset_ms.load(Ordering::Relaxed);
        Instant::now() + Duration::from_millis(off.max(0) as u64)
    }

    /// Opens the blocks, replays the WAL and finishes the setup of `open`.
    fn open_rest(&self, wal: Option<&Arc<WL>>, wbl: Option<&Arc<WL>>, enable_delayed_compaction: bool) -> Result<(), Error> {
        {
            let o = rlock(&self.opts);
            self.metrics.max_bytes.set(o.max_bytes.max(0) as f64);
            self.metrics.max_percentage.set(o.max_percentage.max(0.0));
            self.metrics.retention_duration.set(o.retention_duration as f64 / 1000.0);
        }
        {
            // Calling reload() calls reload_blocks() which requires cmtx to be locked.
            let _c = lock(&self.cmtx);
            self.reload()?;
        }

        // Set the min valid time for the ingested samples to be no lower than the maxt of the last block. We do
        // not consider blocks created from out-of-order samples for the head's minValidTime since minValidTime is
        // only for the in-order data and we do not want to discard unnecessary samples from the head.
        let mut min_valid_time = i64::MIN;
        if let Some(t) = self.in_order_blocks_max_time() {
            min_valid_time = t;
        }
        // Fall back to the truncation mint persisted by truncateWAL, and use whichever of the two is higher, so
        // minValidTime never regresses below a point the WAL was already truncated to.
        if let Some(w) = wal {
            match wlog::read_min_valid_time(w.dir()) {
                Err(e) => self.logger.warn(
                    "Failed to read persisted min valid time, falling back to the value computed from blocks",
                    &[("err", &e)],
                ),
                Ok(Some(t)) if t > min_valid_time => min_valid_time = t,
                Ok(_) => {}
            }
        }

        if let Err(init_err) = self.head.init(min_valid_time) {
            self.head.metrics.wal_corruptions_total.inc();
            if let Some(e) = init_err.downcast_ref::<ErrLoadWbl>() {
                self.logger.warn("Encountered WBL read error, attempting repair", &[("err", &init_err)]);
                let wbl = wbl.ok_or_else(|| Error::wrap("repair corrupted WBL", init_err.clone()))?;
                wbl.repair(&to_wlog_err(&e.0)).map_err(|e| Error::wrap("repair corrupted WBL", Error::other(e)))?;
                self.logger.info("Successfully repaired WBL", &[]);
            } else {
                self.logger.warn("Encountered WAL read error, attempting repair", &[("err", &init_err)]);
                let wal = wal.ok_or_else(|| Error::wrap("repair corrupted WAL", init_err.clone()))?;
                wal.repair(&to_wlog_err(&init_err))
                    .map_err(|e| Error::wrap("repair corrupted WAL", Error::other(e)))?;
                self.logger.info("Successfully repaired WAL", &[]);
            }
        }

        if self.head.min_ooo_time() != i64::MAX {
            // Some OOO data was replayed from the disk that needs compaction and cleanup.
            self.ooo_was_enabled.store(true, Ordering::SeqCst);
        }

        if enable_delayed_compaction {
            let d = self.generate_compaction_delay();
            wlock(&self.opts).compaction_delay = d;
        }
        Ok(())
    }

    /// The reload branch of Go `run`.
    fn on_reload_tick(&self) {
        {
            let _c = lock(&self.cmtx);
            if let Err(e) = self.reload_blocks() {
                self.logger.error("reloadBlocks", &[("err", &e)]);
            }
        }
        self.trigger_compaction();
        // We attempt mmapping of head chunks regularly.
        self.head.mmap_head_chunks();

        let num_stale_series = self.head.num_stale_series();
        let num_series = self.head.num_series();
        let threshold = self.stale_series_compaction_threshold();
        let auto_compact = *lock(&self.auto_compact);
        if auto_compact && num_series > 0 && threshold > 0.0 {
            let ratio = num_stale_series as f64 / num_series as f64;
            if ratio >= threshold {
                let mut next_compaction_is_soon = false;
                let last = lock(&self.cmtx).last_head_compaction_time;
                if let Some(last) = last {
                    let interval = Duration::from_millis(self.head.chunk_range().max(0) as u64);
                    let next_estimated = last + interval;
                    if self.now() + Duration::from_secs(600) > next_estimated {
                        // Next compaction is starting within next 10 mins.
                        next_compaction_is_soon = true;
                    }
                }
                if !next_compaction_is_soon {
                    if let Err(e) = self.compact_stale_head() {
                        self.logger.error("immediate stale series compaction failed", &[("err", &e)]);
                    }
                }
            }
        }
    }

    /// Go `select { case db.compactc <- struct{}{}: default: }`: triggers a compaction in the background.
    pub fn trigger_compaction(&self) {
        let mut g = self.run.lock();
        if !g.compact {
            g.compact = true;
            self.run.cv.notify_all();
        }
    }

    /// Triggers a compaction and waits until the background thread processed a trigger (Go tests: `db.compactc <-
    /// struct{}{}` followed by `synctest.Wait()`). Returns false if the DB is closed.
    #[doc(hidden)]
    pub fn trigger_compaction_and_wait(&self) -> bool {
        let mut g = self.run.lock();
        if g.stopped {
            return false;
        }
        let target = g.processed + 1;
        g.compact = true;
        self.run.cv.notify_all();
        while g.processed < target && !g.stopped {
            g = self.run.cv.wait(g).unwrap_or_else(|p| p.into_inner());
        }
        g.processed >= target
    }

    /// Whether a compaction trigger is pending (Go `len(db.compactc)`).
    #[doc(hidden)]
    pub fn compaction_triggered(&self) -> bool {
        self.run.lock().compact
    }

    fn stopped(&self) -> bool {
        self.run.lock().stopped
    }

    fn stale_series_compaction_threshold(&self) -> f64 {
        f64::from_bits(self.stale_series_compaction_threshold.load(Ordering::Relaxed))
    }

    /// Go `StartTime`: the oldest timestamp stored.
    pub fn start_time(&self) -> Result<i64, Error> {
        let s = rlock(&self.mtx);
        if let Some(b) = s.blocks.first() {
            return Ok(b.meta().min_time);
        }
        Ok(self.head.min_time())
    }

    /// Go `Dir`: the directory of the database.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Go `BlockMetas`: the metadata of all blocks.
    pub fn block_metas(&self) -> Vec<BlockMeta> {
        self.blocks().iter().map(|b| b.meta()).collect()
    }

    /// `prometheus_tsdb_blocks_loaded`.
    pub fn loaded_blocks(&self) -> f64 {
        rlock(&self.mtx).blocks.len() as f64
    }

    /// `prometheus_tsdb_symbol_table_size_bytes`.
    pub fn symbol_table_size(&self) -> f64 {
        let blocks = self.blocks();
        blocks.iter().map(|b| b.get_symbol_table_size()).sum::<u64>() as f64
    }

    /// `prometheus_tsdb_lowest_timestamp`.
    pub fn lowest_timestamp(&self) -> f64 {
        let s = rlock(&self.mtx);
        match s.blocks.first() {
            None => self.head.min_time() as f64,
            Some(b) => b.meta().min_time as f64,
        }
    }

    /// Go `Appender`: a new appender against the database.
    pub fn appender(&self, ctx: &Context) -> Box<dyn Appender> {
        Box::new(DbAppender { app: self.head.appender(ctx), db: self.arc() })
    }

    /// Go `AppenderV2`.
    pub fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
        Box::new(DbAppenderV2 { app: self.head.appender_v2(ctx), db: self.arc() })
    }

    /// Go `ApplyConfig`: applies a new config to the DB. See the Go docs for the out-of-order transitions.
    pub fn apply_config(&self, conf: &ApplyConfig) -> Result<(), Error> {
        let mut ooo_time_window = 0;
        let startup_encoding = rlock(&self.opts).float_chunk_encoding;
        let enable_st_storage = rlock(&self.opts).enable_st_storage;
        if let Some(tc) = &conf.tsdb {
            // Validate encoding config before updating the head encoding so that an invalid encoding in the config
            // does not change the active encoding. An absent chunk_encoding.floats keeps the startup encoding.
            let effective = match tc.float_chunk_encoding.as_str() {
                "" => startup_encoding,
                FLOAT_CHUNK_ENCODING_XOR => chunkenc::ENC_XOR,
                FLOAT_CHUNK_ENCODING_XOR2 => chunkenc::ENC_XOR2,
                floats => {
                    return Err(Error::msg(format!(
                        "unsupported float chunk encoding {:?}; valid values are {:?} and {:?}",
                        floats, FLOAT_CHUNK_ENCODING_XOR, FLOAT_CHUNK_ENCODING_XOR2
                    )));
                }
            };
            if enable_st_storage && effective == chunkenc::ENC_XOR {
                return Err(err_xor_incompatible_with_st_storage());
            }
            ooo_time_window = tc.out_of_order_time_window;
            self.stale_series_compaction_threshold
                .store(tc.stale_series_compaction_threshold.to_bits(), Ordering::Relaxed);
            // Update retention configuration if provided.
            if let Some(r) = &tc.retention {
                let mut o = wlock(&self.opts);
                o.retention_duration = r.time;
                self.metrics.retention_duration.set(o.retention_duration as f64 / 1000.0);
                o.max_bytes = r.size;
                self.metrics.max_bytes.set(o.max_bytes as f64);
                o.max_percentage = r.percentage;
                self.metrics.max_percentage.set(o.max_percentage);
            }
            self.head.opts.float_chunk_encoding.store(effective.0 as u32, Ordering::Relaxed);
        } else {
            self.stale_series_compaction_threshold.store(0f64.to_bits(), Ordering::Relaxed);
            self.head.opts.float_chunk_encoding.store(startup_encoding.0 as u32, Ordering::Relaxed);
        }
        if ooo_time_window < 0 {
            ooo_time_window = 0;
        }

        // Create WBL if it was not present and if OOO is enabled with WAL enabled.
        let wal_segment_size = rlock(&self.opts).wal_segment_size;
        let wblog = match self.head.wbl() {
            // The existing WBL from the disk might have been replayed while OOO was disabled.
            Some(w) => Some(w),
            None if !self.ooo_was_enabled.load(Ordering::SeqCst) && ooo_time_window > 0 && wal_segment_size >= 0 => {
                let mut segment_size = wlog::DEFAULT_SEGMENT_SIZE;
                // Wal is set to a custom size.
                if wal_segment_size > 0 {
                    segment_size = wal_segment_size as usize;
                }
                let compress = rlock(&self.opts).wal_compression;
                let w = WL::new_size(self.dir.join(wlog::WBL_DIR_NAME), segment_size, compress).map_err(Error::other)?;
                Some(Arc::new(w))
            }
            None => None,
        };

        wlock(&self.opts).out_of_order_time_window = ooo_time_window;
        self.head.apply_config(ooo_time_window, conf.max_exemplars, wblog);

        if !self.ooo_was_enabled.load(Ordering::SeqCst) {
            self.ooo_was_enabled.store(ooo_time_window > 0, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Go `waitingForCompactionDelay`: whether the DB is waiting for the head compaction delay.
    fn waiting_for_compaction_delay(&self, st: &CompactionState) -> bool {
        let delay = rlock(&self.opts).compaction_delay;
        match st.time_when_compaction_delay_started {
            // Go: time.Since(zero time) is huge.
            None => false,
            Some(t) => self.now().saturating_duration_since(t) < delay,
        }
    }

    /// Go `Compact`: compacts data if possible. After a successful compaction blocks are reloaded, which also
    /// deletes the blocks that fall out of the retention window.
    pub fn compact(&self, ctx: &Context) -> Result<(), Error> {
        let mut st = lock(&self.cmtx);
        let res = self.compact_locked(&mut st, ctx);
        if let Err(e) = &res {
            if !e.is(&storage::ERR_CANCELED) {
                // If we got an error because context was canceled then we're most likely shutting down TSDB and
                // we don't need to report this on metrics.
                self.metrics.compactions_failed.inc();
            }
        }
        res
    }

    fn compact_locked(&self, st: &mut CompactionState, ctx: &Context) -> Result<(), Error> {
        let mut last_block_maxt = i64::MIN;
        let res = self.compact_locked_inner(st, ctx, &mut last_block_maxt);
        let defer = self.head.truncate_wal(last_block_maxt).err();
        match (res, defer) {
            (r, None) => r,
            (Ok(()), Some(e)) => Err(Error::wrap("WAL truncation in Compact defer", e)),
            (Err(e), Some(e2)) => Err(Error::join([Some(e), Some(Error::wrap("WAL truncation in Compact defer", e2))])
                .expect("errors")),
        }
    }

    fn compact_locked_inner(
        &self,
        st: &mut CompactionState,
        ctx: &Context,
        last_block_maxt: &mut i64,
    ) -> Result<(), Error> {
        let start = Instant::now();
        // Check whether we have pending head blocks that are ready to be persisted. They have the highest priority.
        loop {
            if self.stopped() {
                return Ok(());
            }
            if !self.head.compactable() {
                // Reset the counter once the head compactions are done. This would also reset it if a manual
                // compaction was triggered while the auto compaction was in its delay period.
                st.time_when_compaction_delay_started = None;
                break;
            }
            if st.time_when_compaction_delay_started.is_none() {
                // Start counting for the delay.
                st.time_when_compaction_delay_started = Some(self.now());
            }
            if self.waiting_for_compaction_delay(st) {
                break;
            }
            let mint = self.head.min_time();
            let maxt = range_for_timestamp(mint, self.head.chunk_range());

            // Wrap head into a range that bounds all reads to it. We remove 1 millisecond from maxt because block
            // intervals are half-open: [b.MinTime, b.MaxTime). But chunk intervals are closed: [c.MinTime,
            // c.MaxTime]; so in order to make sure that overlaps are evaluated consistently, we explicitly remove
            // the last value from the block interval here.
            let rh = RangeHead::new_with_isolation_disabled(self.head.clone(), mint, maxt - 1);

            // Compaction runs with isolation disabled, because head.compactable() ensures that maxt is more than
            // chunkRange/2 back from now, and head.appendableMinValidTime() ensures that no new appends can start
            // within the compaction range. We do need to wait for any overlapping appenders that started
            // previously to finish.
            self.head.wait_for_appenders_overlapping(rh.max_time());

            self.compact_head_locked(st, &rh).map_err(|e| Error::wrap("compact head", e))?;
            // Consider only successful compactions for WAL truncation.
            *last_block_maxt = maxt;
        }

        // Clear some disk space before compacting blocks, especially important when Head compaction happened over
        // a long time range.
        self.head.truncate_wal(*last_block_maxt).map_err(|e| Error::wrap("WAL truncation in Compact", e))?;

        let compaction_duration = start.elapsed();
        if compaction_duration.as_millis() as i64 > self.head.chunk_range() {
            self.logger.warn(
                "Head compaction took longer than the block time range, compactions are falling behind and won't be able to catch up",
                &[("duration", &format!("{:?}", compaction_duration)), ("block_range", &self.head.chunk_range())],
            );
        }

        if *last_block_maxt != i64::MIN {
            // The head was compacted, so we compact OOO head as well.
            self.compact_ooo_head_locked(ctx).map_err(|e| Error::wrap("compact ooo head", e))?;
        }

        self.compact_blocks(st)
    }

    /// Go `CompactHead`: compacts the given range head.
    pub fn compact_head(&self, head: &RangeHead) -> Result<(), Error> {
        let mut st = lock(&self.cmtx);
        self.compact_head_locked(&mut st, head).map_err(|e| Error::wrap("compact head", e))?;
        self.head.truncate_wal(head.block_max_time()).map_err(|e| Error::wrap("WAL truncation", e))
    }

    /// Go `CompactOOOHead`.
    pub fn compact_ooo_head(&self, ctx: &Context) -> Result<(), Error> {
        let _st = lock(&self.cmtx);
        self.compact_ooo_head_locked(ctx)
    }

    /// Go `compactOOOHead`. The cmtx must be held.
    fn compact_ooo_head_locked(&self, ctx: &Context) -> Result<(), Error> {
        if !self.ooo_was_enabled.load(Ordering::SeqCst) {
            return Ok(());
        }
        let ooo_head =
            OooCompactionHead::new(ctx, self.head.clone()).map_err(|e| Error::wrap("get ooo compaction head", e))?;

        if let Some(cb) = lock(&self.compact_ooo_head_testing_callback).take() {
            cb();
        }

        let ulids = self.compact_ooo(&self.dir, &ooo_head).map_err(|e| Error::wrap("compact ooo head", e))?;
        if let Err(e) = self.reload_blocks() {
            let mut errs = vec![Some(e)];
            for uid in &ulids {
                if let Err(e) = std::fs::remove_dir_all(self.dir.join(uid.to_string())) {
                    errs.push(Some(Error::other(e)));
                }
            }
            return Err(Error::wrap(
                "reloadBlocks blocks after failed compact ooo head",
                Error::join(errs).expect("errors"),
            ));
        }

        let last_wbl_file = ooo_head.last_wbl_file();
        let min_ooo_mmap_ref = ooo_head.last_mmap_ref();
        if last_wbl_file != 0 || min_ooo_mmap_ref.0 != 0 {
            if min_ooo_mmap_ref.0 != 0 {
                // Ensure that no more queriers are created that will reference chunks we're about to garbage
                // collect. We take mtx to ensure that queriers don't miss blocks.
                wlock(&self.mtx).last_garbage_collected_mmap_ref = min_ooo_mmap_ref;
            }
            self.head
                .truncate_ooo(last_wbl_file, min_ooo_mmap_ref)
                .map_err(|e| Error::wrap("truncate ooo wbl", e))?;
        }
        Ok(())
    }

    /// Go `compactOOO`: a new block per possible block range from the OOO head. The cmtx must be held.
    fn compact_ooo(&self, dest: &Path, ooo_head: &OooCompactionHead) -> Result<Vec<Ulid>, Error> {
        self.logger.info("out-of-order compaction started", &[]);
        let start = Instant::now();
        let block_size = ooo_head.chunk_range();
        let (ooo_mint, ooo_maxt) = (ooo_head.min_time(), ooo_head.max_time());
        let mut ulids = Vec::new();
        let mut meta = BlockMeta::default();
        meta.compaction.set_out_of_order();
        let compactor = rlock(&self.compactor).clone();
        let mut t = block_size * (ooo_mint / block_size);
        while t <= ooo_maxt {
            let (mint, maxt) = (t, t + block_size);
            // Block intervals are half-open: [b.MinTime, b.MaxTime).
            match compactor.write(dest, &ooo_head.clone_for_time_range(mint, maxt - 1), mint, maxt, Some(&meta)) {
                Ok(uids) => ulids.extend(uids),
                Err(e) => {
                    // Best effort removal of created block on any error.
                    for uid in &ulids {
                        let _ = std::fs::remove_dir_all(self.dir.join(uid.to_string()));
                    }
                    return Err(e);
                }
            }
            t += block_size;
        }
        if ulids.is_empty() {
            self.logger
                .info("compact ooo head resulted in no blocks", &[("duration", &format!("{:?}", start.elapsed()))]);
            return Ok(Vec::new());
        }
        self.logger.info(
            "out-of-order compaction completed",
            &[("duration", &format!("{:?}", start.elapsed())), ("ulids", &format!("{:?}", ulids))],
        );
        Ok(ulids)
    }

    /// Go `compactHead`: compacts the given range head. The cmtx must be held.
    fn compact_head_locked(&self, st: &mut CompactionState, head: &RangeHead) -> Result<(), Error> {
        st.last_head_compaction_time = Some(self.now());
        let compactor = rlock(&self.compactor).clone();
        let uids = compactor
            .write(&self.dir, head, head.min_time(), head.block_max_time(), None)
            .map_err(|e| Error::wrap("persist head block", e))?;

        if let Err(e) = self.reload_blocks() {
            let mut errs = vec![Some(Error::wrap("reloadBlocks blocks", e))];
            for uid in &uids {
                if let Err(e) = std::fs::remove_dir_all(self.dir.join(uid.to_string())) {
                    errs.push(Some(Error::wrap(
                        format!("delete persisted head block after failed db reloadBlocks:{}", uid),
                        Error::other(e),
                    )));
                }
            }
            return Err(Error::join(errs).expect("errors"));
        }
        self.head.truncate_memory(head.block_max_time()).map_err(|e| Error::wrap("head memory truncate", e))?;
        // RebuildSymbolTable is a no-op without dedupelabels.
        Ok(())
    }

    /// Go `compactHeadViewLocked`: writes blocks (one per chunk range) for the restricted head view made by
    /// `view_factory`, then runs `evict`. The cmtx must be held.
    fn compact_head_view_locked(
        &self,
        view_factory: &dyn Fn(&Arc<Head>, i64, i64) -> SelectedSeriesHead,
        evict: &dyn Fn(i64) -> Result<(), Error>,
        configure: &dyn Fn(&mut BlockMeta),
    ) -> Result<(), Error> {
        let chunk_range = self.head.opts.chunk_range;
        let mut mint = chunk_range * (self.head.min_time() / chunk_range);
        let maxt = self.head.max_time();
        let compactor = rlock(&self.compactor).clone();
        // The bound is inclusive so that a sample sitting exactly on a chunk-range boundary (mint == maxt) still
        // gets a block written before its series is evicted.
        while mint <= maxt {
            let cr = self.head.chunk_range();
            let view = view_factory(&self.head, mint, mint + cr - 1);
            let mut meta = BlockMeta::default();
            configure(&mut meta);
            let vm = view.meta();
            let uids = compactor
                .write(&self.dir, &view, vm.min_time, vm.max_time + 1, Some(&meta))
                .map_err(|e| Error::wrap("persist head portion", e))?;
            // No UIDs when the view contained no chunks: there is nothing to reload or log.
            if !uids.is_empty() {
                self.logger.info(
                    "Head portion block created",
                    &[("ulids", &format!("{:?}", uids)), ("min_time", &mint), ("max_time", &(mint + cr - 1))],
                );
                if let Err(e) = self.reload_blocks() {
                    let mut errs = vec![Some(Error::wrap("reloadBlocks blocks", e))];
                    for uid in &uids {
                        if let Err(e) = std::fs::remove_dir_all(self.dir.join(uid.to_string())) {
                            errs.push(Some(Error::wrap(
                                format!("delete persisted head portion block after failed db reloadBlocks:{}", uid),
                                Error::other(e),
                            )));
                        }
                    }
                    return Err(Error::join(errs).expect("errors"));
                }
            }
            mint += self.head.chunk_range();
        }

        if let Some(cb) = lock(&self.compact_head_view_before_evict_testing_callback).take() {
            cb();
        }
        evict(maxt).map_err(|e| Error::wrap("head truncate", e))?;
        Ok(())
    }

    /// Go `CompactStaleHead`: writes stale series into blocks and evicts those that are still eligible.
    pub fn compact_stale_head(&self) -> Result<(), Error> {
        let _st = lock(&self.cmtx);
        let res = self.compact_stale_head_locked();
        if res.is_err() {
            self.metrics.stale_series_compactions_failed.inc();
        }
        res
    }

    fn compact_stale_head_locked(&self) -> Result<(), Error> {
        self.metrics.stale_series_compactions_triggered.inc();
        self.logger.info("Starting stale series compaction", &[]);
        let start = Instant::now();

        // We get the stale series reference first because this list can change during the compaction below.
        let refs = self.head.stale_series_refs_no_ooo_data(&Context::background())?;
        // Capture the committed append-ID watermark before snapshotting any series.
        let watermark = self.head.iso.committed_append_id();
        // Snapshot each stale series' in-memory shape before writing any blocks.
        let fingerprints = self.head.snapshot_fingerprints(&refs.sorted_by_ref, watermark);

        let r2 = refs.clone();
        self.compact_head_view_locked(
            &|h, mint, maxt| SelectedSeriesHead::new(h.clone(), mint, maxt, r2.clone()),
            &|maxt| self.head.truncate_stale_series(&refs.sorted_by_ref, maxt, &fingerprints),
            &|meta| meta.compaction.set_stale_series(),
        )?;

        let elapsed = start.elapsed();
        self.metrics.stale_series_compaction_duration.observe(elapsed.as_secs_f64());
        self.logger.info(
            "Ending stale series compaction",
            &[("num_series", &refs.sorted_by_ref.len()), ("duration", &format!("{:?}", elapsed))],
        );
        Ok(())
    }

    /// Go `CompactSelectedSeries`: writes the series into blocks (one per chunk range) and evicts them from the
    /// head without advancing the head min time. Series with out-of-order data are skipped.
    pub fn compact_selected_series(&self, series_refs: &[SeriesRef]) -> Result<(), Error> {
        if series_refs.is_empty() {
            return Ok(());
        }
        let _st = lock(&self.cmtx);
        let res = self.compact_selected_series_locked(series_refs);
        if res.is_err() {
            self.metrics.selected_series_compactions_failed.inc();
        }
        res
    }

    fn compact_selected_series_locked(&self, series_refs: &[SeriesRef]) -> Result<(), Error> {
        self.metrics.selected_series_compactions_triggered.inc();
        let start = Instant::now();
        let mut refs = series_refs.to_vec();
        if !refs.is_sorted() {
            refs.sort_unstable();
        }
        refs.dedup();
        let total_series = refs.len();
        let mut postings = index::new_list_postings(refs);
        // Skip series with out-of-order data: such series stay in the head and are picked up on a later cycle.
        let selected = self
            .head
            .filter_series_and_sort_postings(&mut postings, Some(&|_, s| is_series_without_ooo(s)))?;
        let skipped = total_series - selected.sorted_by_ref.len();
        self.logger.info(
            "Starting selected series compaction",
            &[
                ("num_series", &selected.sorted_by_ref.len()),
                ("num_skipped_ooo", &skipped),
                ("isolation_disabled", &self.head.opts.isolation_disabled),
            ],
        );
        if selected.sorted_by_ref.is_empty() {
            let elapsed = start.elapsed();
            self.metrics.selected_series_compaction_duration.observe(elapsed.as_secs_f64());
            self.logger.info(
                "Ending selected series compaction",
                &[("num_series", &0), ("num_skipped_ooo", &skipped), ("duration", &format!("{:?}", elapsed))],
            );
            return Ok(());
        }
        let watermark = self.head.iso.committed_append_id();
        let fingerprints = self.head.snapshot_fingerprints(&selected.sorted_by_ref, watermark);
        let s2 = selected.clone();
        self.compact_head_view_locked(
            &|h, mint, maxt| SelectedSeriesHead::new(h.clone(), mint, maxt, s2.clone()),
            &|maxt| self.head.truncate_selected_series(&selected.sorted_by_ref, maxt, &fingerprints),
            &|meta| meta.compaction.set_selected_series(),
        )?;
        let elapsed = start.elapsed();
        self.metrics.selected_series_compaction_duration.observe(elapsed.as_secs_f64());
        self.logger.info(
            "Ending selected series compaction",
            &[
                ("num_series", &selected.sorted_by_ref.len()),
                ("num_skipped_ooo", &skipped),
                ("duration", &format!("{:?}", elapsed)),
            ],
        );
        Ok(())
    }

    /// Go `compactBlocks`: compacts all the eligible on-disk blocks. The cmtx must be held.
    #[doc(hidden)]
    pub fn compact_blocks(&self, st: &CompactionState) -> Result<(), Error> {
        loop {
            // If we have a lot of blocks to compact the whole process might take long enough that we end up with
            // a head block that needs to be written. Stop compactions early in that case.
            if self.head.compactable() && !self.waiting_for_compaction_delay(st) {
                self.logger.warn("aborting block compactions to persist the head block", &[]);
                return Ok(());
            }
            let compactor = rlock(&self.compactor).clone();
            let plan = compactor.plan(&self.dir).map_err(|e| Error::wrap("plan compaction", e))?;
            if plan.is_empty() {
                break;
            }
            if self.stopped() {
                return Ok(());
            }
            let blocks = self.blocks();
            let uids = compactor
                .compact(&self.dir, &plan, &blocks)
                .map_err(|e| Error::wrap(format!("compact {}", fmt_paths(&plan)), e))?;
            if let Err(e) = self.reload_blocks() {
                let mut errs = vec![Some(Error::wrap("reloadBlocks blocks", e))];
                for uid in &uids {
                    if let Err(e) = std::fs::remove_dir_all(self.dir.join(uid.to_string())) {
                        errs.push(Some(Error::wrap(
                            format!("delete persisted block after failed db reloadBlocks:{}", uid),
                            Error::other(e),
                        )));
                    }
                }
                return Err(Error::join(errs).expect("errors"));
            }
        }
        Ok(())
    }

    /// Go `reload`: reloads blocks and truncates the head and its WAL. The cmtx must be held.
    #[doc(hidden)]
    pub fn reload(&self) -> Result<(), Error> {
        self.reload_blocks().map_err(|e| Error::wrap("reloadBlocks", e))?;
        let Some(maxt) = self.in_order_blocks_max_time() else {
            return Ok(());
        };
        self.head.truncate(maxt).map_err(|e| Error::wrap("head truncate", e))
    }

    /// Go `reloadBlocks`: reloads blocks without touching the head. Blocks that are obsolete due to replacement or
    /// retention are deleted. The cmtx must be held.
    #[doc(hidden)]
    pub fn reload_blocks(&self) -> Result<(), Error> {
        let res = self.reload_blocks_inner();
        if res.is_err() {
            self.metrics.reloads_failed.inc();
        }
        self.metrics.reloads.inc();
        res
    }

    fn reload_blocks_inner(&self) -> Result<(), Error> {
        let (mut loadable, mut corrupted) = {
            let s = rlock(&self.mtx);
            let pd = rlock(&self.opts).postings_decoder_factory.clone();
            open_blocks(&self.logger, &self.dir, &s.blocks, Some(self.chunk_pool.clone()), pd.as_ref())?
        };

        let deletable_ulids = {
            let f = rlock(&self.blocks_to_delete).clone();
            match f {
                Some(f) => f(&loadable),
                None => deletable_blocks(self, &mut loadable),
            }
        };
        let mut deletable: HashMap<Ulid, Option<Block>> = HashMap::with_capacity(deletable_ulids.len());

        // Mark all parents of loaded blocks as deletable (no matter if they exists). This makes it resilient
        // against the process crashing towards the end of a compaction but before deletions.
        for block in &loadable {
            let meta = block.meta();
            if deletable_ulids.contains(&meta.ulid) {
                deletable.insert(meta.ulid, Some(block.clone()));
            }
            for b in &meta.compaction.parents {
                if corrupted.remove(&b.ulid).is_some() {
                    self.logger.warn(
                        "Found corrupted block, but replaced by compacted one so it's safe to delete. This should not happen with atomic deletes.",
                        &[("block", &b.ulid)],
                    );
                }
                deletable.insert(b.ulid, None);
            }
        }

        if !corrupted.is_empty() {
            // Corrupted but no child loaded for it. Close all new blocks to release the lock for windows.
            {
                let s = rlock(&self.mtx);
                for block in &loadable {
                    if !s.blocks.iter().any(|b| b.ulid() == block.ulid()) {
                        let _ = block.close();
                    }
                }
            }
            let mut ids: Vec<_> = corrupted.into_iter().collect();
            ids.sort_by_key(|(u, _)| *u);
            let errs = ids
                .into_iter()
                .map(|(u, e)| Some(Error::wrap(format!("corrupted block {}", u), e)));
            return Err(Error::join(errs).expect("errors"));
        }

        let mut to_load = Vec::with_capacity(loadable.len());
        let mut blocks_size = 0i64;
        // All deletable blocks should be unloaded.
        for block in loadable {
            if let Some(slot) = deletable.get_mut(&block.ulid()) {
                *slot = Some(block);
                continue;
            }
            blocks_size += block.size();
            to_load.push(block);
        }
        self.metrics.blocks_bytes.set(blocks_size as f64);

        to_load.sort_by_key(|b| b.meta().min_time);

        // Swap new blocks first for subsequently created readers to be seen.
        let old_blocks = {
            let mut s = wlock(&self.mtx);
            std::mem::replace(&mut s.blocks, to_load.clone())
        };

        // Only check overlapping blocks when overlapping compaction is enabled.
        if rlock(&self.opts).enable_overlapping_compaction {
            let metas: Vec<BlockMeta> = to_load.iter().map(|b| b.meta()).collect();
            let overlaps = overlapping_blocks(&metas);
            if !overlaps.is_empty() {
                self.logger
                    .warn("Overlapping blocks found during reloadBlocks", &[("detail", &overlaps_string(&overlaps))]);
            }
        }

        // Append blocks to old, deletable blocks, so we can close them.
        for b in old_blocks {
            if let Some(slot) = deletable.get_mut(&b.ulid()) {
                *slot = Some(b);
            }
        }
        let n = deletable.len();
        self.delete_blocks(deletable).map_err(|e| Error::wrap(format!("delete {} blocks", n), e))
    }

    /// Go `deleteBlocks`: closes the blocks if loaded and deletes them from the disk if they exist.
    fn delete_blocks(&self, blocks: HashMap<Ulid, Option<Block>>) -> Result<(), Error> {
        for (ulid, block) in blocks {
            if let Some(b) = block {
                if let Err(e) = b.close() {
                    self.logger.warn("Closing block failed", &[("err", &e), ("block", &ulid)]);
                }
            }
            let to_delete = self.dir.join(ulid.to_string());
            match std::fs::symlink_metadata(&to_delete) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(io_wrap(format!("stat dir {}", to_delete.display()), e)),
                Ok(_) => {}
            }
            // Replace atomically to avoid partial block when process would crash during deletion.
            let tmp = self.dir.join(format!("{}{}", ulid, TMP_FOR_DELETION_BLOCK_DIR_SUFFIX));
            fileutil::replace(&to_delete, &tmp)
                .map_err(|e| io_wrap(format!("replace of obsolete block for deletion {}", ulid), e))?;
            std::fs::remove_dir_all(&tmp).map_err(|e| io_wrap(format!("delete obsolete block {}", ulid), e))?;
            self.logger.info("Deleting obsolete block", &[("block", &ulid)]);
        }
        Ok(())
    }

    /// Go `Blocks`: the persisted blocks of the database.
    pub fn blocks(&self) -> Vec<Block> {
        rlock(&self.mtx).blocks.clone()
    }

    /// Go `inOrderBlocksMaxTime`: the max time among the blocks not created only from out-of-order, stale or
    /// selected series data.
    pub fn in_order_blocks_max_time(&self) -> Option<i64> {
        let mut maxt = i64::MIN;
        let mut ok = false;
        // If blocks are overlapping, last block might not have the max time. So check all blocks.
        for b in self.blocks() {
            let m = b.meta();
            if !m.compaction.from_out_of_order()
                && !m.compaction.from_stale_series()
                && !m.compaction.from_selected_series()
                && m.max_time > maxt
            {
                ok = true;
                maxt = m.max_time;
            }
        }
        ok.then_some(maxt)
    }

    /// Go `Head`: the head of the database.
    pub fn head(&self) -> &Arc<Head> {
        &self.head
    }

    /// Go `Close`: closes the database. Closing twice returns `Ok`.
    pub fn close(&self) -> Result<(), Error> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        {
            let mut g = self.run.lock();
            g.stopped = true;
            self.run.cv.notify_all();
        }
        self.compact_cancel.cancel();
        if let Some(h) = lock(&self.run_handle).take() {
            if h.thread().id() != std::thread::current().id() {
                let _ = h.join();
            }
        }

        let s = wlock(&self.mtx);
        // Close the blocks in parallel (Go errgroup; the first error wins).
        let blocks_err = std::thread::scope(|sc| {
            let hs: Vec<_> = s.blocks.iter().map(|b| sc.spawn(move || b.close())).collect();
            let mut first = None;
            for h in hs {
                if let Ok(Err(e)) = h.join() {
                    first.get_or_insert(e);
                }
            }
            first
        });
        let mut errs = vec![blocks_err];
        if let Some(mut l) = lock(&self.locker).take() {
            errs.push(l.release().err());
        }
        errs.push(self.head.close().err());
        match Error::join(errs) {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Whether the DB is closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Go `DisableCompactions`: disables auto compactions.
    pub fn disable_compactions(&self) {
        *lock(&self.auto_compact) = false;
        self.logger.info("Compactions disabled", &[]);
    }

    /// Go `EnableCompactions`: enables auto compactions.
    pub fn enable_compactions(&self) {
        *lock(&self.auto_compact) = true;
        self.logger.info("Compactions enabled", &[]);
    }

    /// Go `generateCompactionDelay`.
    #[doc(hidden)]
    pub fn generate_compaction_delay(&self) -> Duration {
        let pct = rlock(&self.opts).compaction_delay_max_percent as i64;
        let n = self.head.chunk_range() * pct / 100;
        if n <= 0 {
            // Go rand.Int63n panics for n <= 0.
            panic!("invalid argument to Int63n");
        }
        let mut b = [0u8; 8];
        crypto::random_bytes(&mut b);
        Duration::from_millis((u64::from_le_bytes(b) % n as u64) as u64)
    }

    /// Go `ForceHeadMMap`: for tests and benchmarks.
    pub fn force_head_mmap(&self) {
        self.head.mmap_head_chunks();
    }

    /// Go `Snapshot`: writes the current data to `dir`. With `with_head`, also a block of the data in the head.
    pub fn snapshot(&self, dir: &Path, with_head: bool) -> Result<(), Error> {
        if dir == self.dir {
            return Err(Error::msg("cannot snapshot into base directory"));
        }
        if dir.to_str().is_some_and(|s| Ulid::parse(s).is_ok()) {
            return Err(Error::msg("dir must not be a valid ULID"));
        }
        let _c = lock(&self.cmtx);
        let s = rlock(&self.mtx);
        for b in &s.blocks {
            self.logger.info("Snapshotting block", &[("block", b)]);
            b.snapshot(dir)
                .map_err(|e| Error::wrap(format!("error snapshotting block: {}", b.dir().display()), e))?;
        }
        if !with_head {
            return Ok(());
        }
        let mint = self.head.min_time();
        let maxt = self.head.max_time();
        let head = RangeHead::new(self.head.clone(), mint, maxt);
        // Add +1 millisecond to block maxt because block intervals are half-open: [b.MinTime, b.MaxTime).
        let compactor = rlock(&self.compactor).clone();
        compactor.write(dir, &head, mint, maxt + 1, None).map_err(|e| Error::wrap("snapshot head block", e))?;
        Ok(())
    }

    /// Go `Querier`: a querier over the data of the time range. It is not safe for concurrent use.
    pub fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        let s = rlock(&self.mtx);
        let bq = rlock(&self.block_querier_func).clone();
        querier_over(&s.blocks, &self.head, s.last_garbage_collected_mmap_ref, bq.as_ref(), mint, maxt)
    }

    /// Go `blockChunkQuerierForRange`: the chunk queriers of the persistent blocks, the in-order head and the
    /// out-of-order head that overlap the time range. Native queriers are typed (for cost sampling).
    #[doc(hidden)]
    pub fn block_chunk_querier_for_range(&self, mint: i64, maxt: i64) -> Result<Vec<PinnedChunkQuerier>, Error> {
        let s = rlock(&self.mtx);
        let f = rlock(&self.block_chunk_querier_func).clone();
        let open = |b: &dyn BlockReader, mint: i64, maxt: i64| -> Result<PinnedChunkQuerier, Error> {
            match &f {
                None => Ok(PinnedChunkQuerier::Native(BlockChunkQuerier(new_block_base_querier(b, mint, maxt)?))),
                Some(f) => Ok(PinnedChunkQuerier::Other(f(b, mint, maxt)?)),
            }
        };
        let blocks: Vec<&Block> = s.blocks.iter().filter(|b| b.overlaps_closed_interval(mint, maxt)).collect();
        let mut queriers: Vec<PinnedChunkQuerier> = Vec::with_capacity(blocks.len() + 1);
        let res = (|| -> Result<(), Error> {
            let head = &self.head;
            let overlaps_ooo = crate::head::overlaps_closed_interval(mint, maxt, head.min_ooo_time(), head.max_ooo_time());
            let mut head_querier: Option<PinnedChunkQuerier> = None;
            let mut ino_mint = head.min_time().max(mint);
            if maxt >= head.min_time() || overlaps_ooo {
                let rh = RangeHead::new(head.clone(), mint, maxt);
                head_querier = Some(open(&rh, mint, maxt).map_err(|e| Error::wrap(format!("open querier for head {}", rh), e))?);
                // Getting the querier above registers itself in the queue that the truncation waits on. So if the
                // querier is currently not colliding with any truncation, we can continue to use it.
                let (should_close, get_new, new_mint) = head.is_querier_colliding_with_truncation(mint, maxt);
                if should_close {
                    ino_mint = new_mint;
                    if let Some(mut q) = head_querier.take() {
                        if let Some(e) = q.close() {
                            return Err(Error::wrap(format!("closing head querier {}", rh), e));
                        }
                    }
                }
                if get_new {
                    let rh = RangeHead::new(head.clone(), new_mint, maxt);
                    head_querier = Some(open(&rh, new_mint, maxt).map_err(|e| {
                        Error::wrap(format!("open querier for head while getting new querier {}", rh), e)
                    })?);
                }
            }
            if overlaps_ooo {
                // We need to fetch from in-order and out-of-order chunks: wrap the head querier.
                let iso_state = head.ooo_iso.track_read_after(s.last_garbage_collected_mmap_ref);
                let inner = head_querier.take().map(PinnedChunkQuerier::into_box);
                head_querier = Some(PinnedChunkQuerier::Other(Box::new(HeadAndOOOChunkQuerier::new(
                    ino_mint,
                    mint,
                    maxt,
                    head.clone(),
                    iso_state,
                    inner,
                ))));
            }
            if let Some(q) = head_querier {
                queriers.push(q);
            }
            for b in &blocks {
                let q = open(*b, mint, maxt).map_err(|e| Error::wrap(format!("open querier for block {}", b), e))?;
                queriers.push(q);
            }
            Ok(())
        })();
        if let Err(mut err) = res {
            // If we fail, all previously opened queriers must be closed.
            for q in &mut queriers {
                if let Some(ce) = q.close() {
                    err = Error::join([Some(err), Some(Error::wrap("close querier during cleanup", ce))])
                        .expect("errors");
                }
            }
            return Err(err);
        }
        Ok(queriers)
    }

    /// Go `floatChunkEncoding`: the float chunk encoding in effect.
    pub fn float_chunk_encoding(&self) -> Encoding {
        Encoding(self.head.opts.float_chunk_encoding.load(Ordering::Relaxed) as u8)
    }

    /// Go `ChunkQuerier`: a chunk querier over the data of the time range.
    pub fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        let qs = self.block_chunk_querier_for_range(mint, maxt)?;
        let head = self.head.clone();
        let enc: storage::FloatEncodingFn =
            Arc::new(move || Encoding(head.opts.float_chunk_encoding.load(Ordering::Relaxed) as u8));
        let merge = storage::new_compacting_chunk_series_merger_with_float_encoding(
            Arc::new(storage::chained_series_merge),
            Some(enc),
        );
        Ok(storage::new_merge_chunk_querier(qs.into_iter().map(PinnedChunkQuerier::into_box).collect(), Vec::new(), merge))
    }

    /// Go `ExemplarQuerier`.
    pub fn exemplar_querier(&self, ctx: &Context) -> Result<Box<dyn storage::ExemplarQuerier>, Error> {
        self.head.exemplar_querier(ctx)
    }

    /// Go `CostSamplingQueryable`: a source restricted to incremental native readers.
    pub fn cost_sampling_queryable(&self) -> (&dyn SampleAndChunkQueryable, bool) {
        (&self.cost_sampling, true)
    }

    /// Replaces the block chunk querier function (Go tests assign `db.blockChunkQuerierFunc`).
    #[doc(hidden)]
    pub fn set_block_chunk_querier_func(
        &self,
        f: Box<dyn Fn(&dyn BlockReader, i64, i64) -> Result<BoxChunkQuerier, Error> + Send + Sync>,
    ) {
        *wlock(&self.block_chunk_querier_func) = Some(Arc::from(f));
    }

    /// Go `Delete`: deletes the matching series in the time range. Atomic per block only.
    pub fn delete(&self, ctx: &Context, mint: i64, maxt: i64, ms: &[Matcher]) -> Result<(), Error> {
        let _c = lock(&self.cmtx);
        let s = rlock(&self.mtx);
        std::thread::scope(|sc| {
            let mut hs = Vec::new();
            for b in &s.blocks {
                if b.overlaps_closed_interval(mint, maxt) {
                    hs.push(sc.spawn(move || b.delete(ctx, mint, maxt, ms)));
                }
            }
            if self.head.overlaps_closed_interval(mint, maxt) {
                let head = &self.head;
                hs.push(sc.spawn(move || head.delete(ctx, mint, maxt, ms)));
            }
            let mut first = None;
            for h in hs {
                match h.join() {
                    Ok(Err(e)) => {
                        first.get_or_insert(e);
                    }
                    Ok(Ok(())) => {}
                    Err(p) => std::panic::resume_unwind(p),
                }
            }
            match first {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })
    }

    /// Go `CleanTombstones`: rewrites any blocks with tombstones.
    pub fn clean_tombstones(&self) -> Result<(), Error> {
        let _c = lock(&self.cmtx);
        let start = Instant::now();
        let res = self.clean_tombstones_locked();
        self.metrics.tomb_clean_timer.observe(start.elapsed().as_secs_f64());
        res
    }

    fn clean_tombstones_locked(&self) -> Result<(), Error> {
        let mut clean_up_completed = false;
        // Repeat cleanup until there is no tombstones left.
        while !clean_up_completed {
            clean_up_completed = true;
            for pb in self.blocks() {
                let compactor = rlock(&self.compactor).clone();
                let (uids, safe_to_delete) = pb
                    .clean_tombstones(&self.dir, &*compactor)
                    .map_err(|e| Error::wrap(format!("clean tombstones: {}", pb.dir().display()), e))?;
                if !safe_to_delete {
                    // There was nothing to clean.
                    continue;
                }
                // In case tombstones of the old block covers the whole block, then there would be no resultant
                // block to tell the parent. The lock protects against race conditions when deleting blocks
                // during an already running reload.
                {
                    let _s = wlock(&self.mtx);
                    pb.set_deletable_in_memory();
                }
                clean_up_completed = false;
                match self.reload_blocks() {
                    // Successful reload will change the existing blocks. We need to loop over the new set of blocks.
                    Ok(()) => break,
                    Err(e) => {
                        // Delete new block if it was created.
                        for uid in &uids {
                            let dir = self.dir.join(uid.to_string());
                            if let Err(err) = std::fs::remove_dir_all(&dir) {
                                self.logger.error(
                                    "failed to delete block after failed `CleanTombstones`",
                                    &[("dir", &dir.display()), ("err", &err)],
                                );
                            }
                        }
                        // This should only be reached if an error occurred.
                        return Err(Error::wrap("reload blocks", e));
                    }
                }
            }
        }
        Ok(())
    }

    /// Go `SetWriteNotified`.
    pub fn set_write_notified(&self, wn: Option<Arc<dyn wlog::WriteNotified>>) {
        self.head.set_write_notified(wn);
    }

    /// The fs size function (Go `db.fsSizeFunc`).
    #[doc(hidden)]
    pub fn fs_size_of_dir(&self) -> u64 {
        (self.fs_size_func)(&self.dir)
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        // Stops the background thread if the DB was not closed (it only holds a weak reference).
        let mut g = self.run.lock();
        g.stopped = true;
        self.run.cv.notify_all();
    }
}

impl std::fmt::Display for DB {
    /// Go `String`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HEAD")
    }
}

fn fmt_paths(p: &[PathBuf]) -> String {
    let v: Vec<String> = p.iter().map(|p| p.display().to_string()).collect();
    format!("[{}]", v.join(" "))
}

/// Finds a WAL error in the chain of `e` (or makes one with its text), for `WL::repair`.
fn to_wlog_err(e: &Error) -> wlog::Error {
    match e.downcast_ref::<wlog::Error>() {
        Some(w) => w.clone(),
        None => wlog::Error::msg(&e.to_string()),
    }
}

impl PinnedChunkQuerier {
    /// The querier as a plain boxed chunk querier.
    pub fn into_box(self) -> BoxChunkQuerier {
        match self {
            PinnedChunkQuerier::Native(q) => Box::new(q),
            PinnedChunkQuerier::Other(q) => q,
        }
    }
}

/// The querier of `DB::querier` over the given blocks and head (also used by the read-only DB).
fn querier_over(
    all_blocks: &[Block],
    head: &Arc<Head>,
    last_gc_ref: ChunkDiskMapperRef,
    bq: Option<&BlockQuerierFunc>,
    mint: i64,
    maxt: i64,
) -> Result<BoxQuerier, Error> {
    let open = |b: &dyn BlockReader, mint: i64, maxt: i64| -> Result<BoxQuerier, Error> {
        match bq {
            None => new_block_querier(b, mint, maxt),
            Some(f) => f(b, mint, maxt),
        }
    };
    let blocks: Vec<&Block> = all_blocks.iter().filter(|b| b.overlaps_closed_interval(mint, maxt)).collect();
    let mut queriers: Vec<BoxQuerier> = Vec::with_capacity(blocks.len() + 1);
    let res = (|| -> Result<(), Error> {
        let overlaps_ooo = crate::head::overlaps_closed_interval(mint, maxt, head.min_ooo_time(), head.max_ooo_time());
        let mut head_querier: Option<BoxQuerier> = None;
        let mut ino_mint = head.min_time().max(mint);
        if maxt >= head.min_time() || overlaps_ooo {
            let rh = RangeHead::new(head.clone(), mint, maxt);
            head_querier =
                Some(open(&rh, mint, maxt).map_err(|e| Error::wrap(format!("open block querier for head {}", rh), e))?);
            let (should_close, get_new, new_mint) = head.is_querier_colliding_with_truncation(mint, maxt);
            if should_close {
                ino_mint = new_mint;
                if let Some(mut q) = head_querier.take() {
                    if let Some(e) = q.close() {
                        return Err(Error::wrap(format!("closing head block querier {}", rh), e));
                    }
                }
            }
            if get_new {
                let rh = RangeHead::new(head.clone(), new_mint, maxt);
                head_querier = Some(open(&rh, new_mint, maxt).map_err(|e| {
                    Error::wrap(format!("open block querier for head while getting new querier {}", rh), e)
                })?);
            }
        }
        if overlaps_ooo {
            // We need to fetch from in-order and out-of-order chunks: wrap the head querier.
            let iso_state = head.ooo_iso.track_read_after(last_gc_ref);
            head_querier = Some(Box::new(HeadAndOOOQuerier::new(
                ino_mint,
                mint,
                maxt,
                head.clone(),
                iso_state,
                head_querier.take(),
            )));
        }
        if let Some(q) = head_querier {
            queriers.push(q);
        }
        for b in &blocks {
            let q = open(*b, mint, maxt).map_err(|e| Error::wrap(format!("open querier for block {}", b), e))?;
            queriers.push(q);
        }
        Ok(())
    })();
    if let Err(mut err) = res {
        // If we fail, all previously opened queriers must be closed.
        for q in &mut queriers {
            if let Some(ce) = q.close() {
                err = Error::join([Some(err), Some(Error::wrap("close querier during cleanup", ce))]).expect("errors");
            }
        }
        return Err(err);
    }
    Ok(storage::new_merge_querier(queriers, Vec::new(), Arc::new(storage::chained_series_merge)))
}

// ---------------------------------------------------------------------------------------------------------------
// ApplyConfig values.

/// The parts of `config.Config` that `DB::apply_config` reads.
#[derive(Clone, Debug, Default)]
pub struct ApplyConfig {
    /// `storage.tsdb` (Go `StorageConfig.TSDBConfig`; `None` when absent).
    pub tsdb: Option<TsdbConfig>,
    /// `storage.exemplars.max_exemplars`.
    pub max_exemplars: i64,
}

/// Go `config.TSDBConfig` (the fields the DB reads).
#[derive(Clone, Debug, Default)]
pub struct TsdbConfig {
    /// Milliseconds.
    pub out_of_order_time_window: i64,
    pub stale_series_compaction_threshold: f64,
    pub retention: Option<TsdbRetentionConfig>,
    /// `chunk_encoding.floats`: "", "xor" or "xor2".
    pub float_chunk_encoding: String,
}

/// Go `config.TSDBRetentionConfig`.
#[derive(Clone, Copy, Debug, Default)]
pub struct TsdbRetentionConfig {
    /// Milliseconds.
    pub time: i64,
    /// Bytes.
    pub size: i64,
    pub percentage: f64,
}

// ---------------------------------------------------------------------------------------------------------------
// Appenders.

/// Go `dbAppender`: wraps the head appender and triggers compactions on commit if necessary.
struct DbAppender {
    app: Box<dyn Appender>,
    db: Arc<DB>,
}

impl AppenderTransaction for DbAppender {
    fn commit(&mut self) -> Option<Error> {
        let err = self.app.commit();
        // We could just run this check every few minutes practically. But for benchmarks and high frequency use
        // cases this is the safer way.
        if self.db.head.compactable() {
            self.db.trigger_compaction();
        }
        err
    }
    fn rollback(&mut self) -> Option<Error> {
        self.app.rollback()
    }
}

impl Appender for DbAppender {
    fn append(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        self.app.append(r, l, t, v)
    }
    fn set_options(&mut self, opts: Option<&AppendOptions>) {
        self.app.set_options(opts)
    }
    fn append_exemplar(&mut self, r: SeriesRef, l: &Labels, e: &Exemplar) -> Result<SeriesRef, Error> {
        self.app.append_exemplar(r, l, e)
    }
    fn append_histogram(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        h: Option<&ModelHistogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.app.append_histogram(r, l, t, h, fh)
    }
    fn append_histogram_st_zero_sample(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        st: i64,
        h: Option<&ModelHistogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.app.append_histogram_st_zero_sample(r, l, t, st, h, fh)
    }
    fn update_metadata(&mut self, r: SeriesRef, l: &Labels, m: &Metadata) -> Result<SeriesRef, Error> {
        self.app.update_metadata(r, l, m)
    }
    fn append_st_zero_sample(&mut self, r: SeriesRef, l: &Labels, t: i64, st: i64) -> Result<SeriesRef, Error> {
        self.app.append_st_zero_sample(r, l, t, st)
    }
    fn as_get_ref(&mut self) -> Option<&mut dyn GetRef> {
        Some(self)
    }
}

impl GetRef for DbAppender {
    fn get_ref(&mut self, lset: &Labels, hash: u64) -> (SeriesRef, Labels) {
        match self.app.as_get_ref() {
            Some(g) => g.get_ref(lset, hash),
            None => (0, Labels::default()),
        }
    }
}

/// Go `dbAppenderV2`.
struct DbAppenderV2 {
    app: Box<dyn AppenderV2>,
    db: Arc<DB>,
}

impl AppenderTransaction for DbAppenderV2 {
    fn commit(&mut self) -> Option<Error> {
        let err = self.app.commit();
        if self.db.head.compactable() {
            self.db.trigger_compaction();
        }
        err
    }
    fn rollback(&mut self) -> Option<Error> {
        self.app.rollback()
    }
}

impl AppenderV2 for DbAppenderV2 {
    fn append(
        &mut self,
        r: SeriesRef,
        ls: &Labels,
        st: i64,
        t: i64,
        v: f64,
        h: Option<&ModelHistogram>,
        fh: Option<&FloatHistogram>,
        opts: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        self.app.append(r, ls, st, t, v, h, fh, opts)
    }
    fn as_exemplar_appender_v2(&mut self) -> Option<&mut dyn ExemplarAppenderV2> {
        Some(self)
    }
    fn type_name(&self) -> &'static str {
        "tsdb.dbAppenderV2"
    }
}

impl ExemplarAppenderV2 for DbAppenderV2 {
    fn append_exemplars(&mut self, r: SeriesRef, ls: &Labels, exemplars: &[Exemplar]) -> Result<SeriesRef, Error> {
        let name = self.app.type_name();
        match self.app.as_exemplar_appender_v2() {
            Some(ea) => ea.append_exemplars(r, ls, exemplars),
            None => Err(Error::msg(format!("appender {} does not implement storage.ExemplarAppenderV2", name))),
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Storage traits.

impl storage::Queryable for DB {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        DB::querier(self, mint, maxt)
    }
}

impl storage::ChunkQueryable for DB {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        DB::chunk_querier(self, mint, maxt)
    }
}

impl storage::Appendable for DB {
    fn appender(&self, ctx: &Context) -> Box<dyn Appender> {
        DB::appender(self, ctx)
    }
}

impl storage::AppendableV2 for DB {
    fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
        DB::appender_v2(self, ctx)
    }
}

impl storage::ExemplarQueryable for DB {
    fn exemplar_querier(&self, ctx: &Context) -> Result<Box<dyn storage::ExemplarQuerier>, Error> {
        DB::exemplar_querier(self, ctx)
    }
}

impl storage::CostSamplingQueryable for DB {
    fn cost_sampling_queryable(&self) -> (Option<&dyn SampleAndChunkQueryable>, bool) {
        (Some(&self.cost_sampling), true)
    }
}

impl storage::Storage for DB {
    fn start_time(&self) -> Result<i64, Error> {
        DB::start_time(self)
    }
    fn close(&self) -> Option<Error> {
        DB::close(self).err()
    }
    fn as_cost_sampling_queryable(&self) -> Option<&dyn storage::CostSamplingQueryable> {
        Some(self)
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Blocks, retention and overlaps.

/// Go `openBlocks`: the loadable blocks of `dir` (reusing the `loaded` ones) and the corrupted ones.
#[allow(clippy::type_complexity)]
pub fn open_blocks(
    l: &Logger,
    dir: &Path,
    loaded: &[Block],
    chunk_pool: Option<Arc<dyn Pool>>,
    postings_decoder_factory: Option<&PostingsDecoderFactory>,
) -> Result<(Vec<Block>, HashMap<Ulid, Error>), Error> {
    let bdirs = block_dirs(dir).map_err(|e| Error::wrap("find blocks", e))?;
    let mut corrupted = HashMap::new();
    let mut blocks = Vec::with_capacity(bdirs.len());
    for bdir in bdirs {
        let meta = match read_meta_file(&bdir) {
            Ok((m, _)) => m,
            Err(e) => {
                l.error(
                    "Failed to read meta.json for a block during reloadBlocks. Skipping",
                    &[("dir", &bdir.display()), ("err", &e)],
                );
                continue;
            }
        };
        // See if we already have the block in memory or open it otherwise.
        if let Some(b) = loaded.iter().find(|b| b.ulid() == meta.ulid) {
            blocks.push(b.clone());
            continue;
        }
        match open_block(Some(l.clone()), &bdir, chunk_pool.clone(), postings_decoder_factory) {
            Ok(b) => blocks.push(b),
            Err(e) => {
                corrupted.insert(meta.ulid, e);
            }
        }
    }
    Ok((blocks, corrupted))
}

/// Go `deletableBlocks`: the blocks past the retention policy or already compacted into a new block. Sorts
/// `blocks` from newest to oldest (by max time).
pub fn deletable_blocks(db: &DB, blocks: &mut [Block]) -> HashSet<Ulid> {
    let mut deletable = HashSet::new();
    // Sort the blocks by time - newest to oldest (largest to smallest timestamp). This ensures that the
    // retentions will remove the oldest blocks.
    blocks.sort_by_key(|b| std::cmp::Reverse(b.meta().max_time));
    for b in blocks.iter() {
        let m = b.meta();
        if m.compaction.deletable {
            deletable.insert(m.ulid);
        }
    }
    deletable.extend(beyond_time_retention(db, blocks));
    deletable.extend(beyond_size_retention(db, blocks));
    deletable
}

/// Go `BeyondTimeRetention`: the blocks beyond the time retention (blocks sorted newest first).
pub fn beyond_time_retention(db: &DB, blocks: &[Block]) -> HashSet<Ulid> {
    let mut deletable = HashSet::new();
    // Time retention is disabled or no blocks to work with.
    let retention = rlock(&db.opts).retention_duration;
    if blocks.is_empty() || retention == 0 {
        return deletable;
    }
    let first_max = blocks[0].meta().max_time;
    for (i, block) in blocks.iter().enumerate() {
        // The difference between the first block and this block is greater than or equal to the retention period
        // so any blocks after that are added as deletable.
        if i > 0 && first_max - block.meta().max_time >= retention {
            for b in &blocks[i..] {
                deletable.insert(b.ulid());
            }
            db.metrics.time_retention_count.inc();
            break;
        }
    }
    deletable
}

/// Go `BeyondSizeRetention`: the blocks beyond the size retention (blocks sorted newest first). The WAL, WBL and
/// head chunk sizes count too.
pub fn beyond_size_retention(db: &DB, blocks: &[Block]) -> HashSet<Ulid> {
    let mut deletable = HashSet::new();
    // No blocks to work with.
    if blocks.is_empty() {
        return deletable;
    }
    let (mut max_bytes, max_percentage) = {
        let o = rlock(&db.opts);
        (o.max_bytes, o.max_percentage)
    };
    // Max percentage prevails over max size.
    if max_percentage > 0.0 {
        let disk_size = (db.fs_size_func)(&db.dir);
        if disk_size == 0 {
            db.logger.warn(
                "Unable to retrieve filesystem size of database directory, skip percentage limitation and default to fixed size limitation",
                &[("dir", &db.dir.display())],
            );
        } else {
            max_bytes = (disk_size as f64 * max_percentage / 100.0) as i64;
        }
    }
    // Size retention is disabled.
    if max_bytes <= 0 {
        return deletable;
    }
    // Initializing size counter with WAL size and Head chunks written to disk, as that is part of the retention
    // strategy.
    let mut blocks_size = db.head.size();
    for (i, block) in blocks.iter().enumerate() {
        blocks_size += block.size();
        if blocks_size > max_bytes {
            // Add this and all following blocks for deletion.
            for b in &blocks[i..] {
                deletable.insert(b.ulid());
            }
            db.metrics.size_retention_count.inc();
            break;
        }
    }
    deletable
}

/// Go `TimeRange`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimeRange {
    pub min: i64,
    pub max: i64,
}

/// Go `Overlaps`: overlapping blocks aggregated by overlapping range (ordered by range; Go map order is random).
pub type Overlaps = BTreeMap<TimeRange, Vec<BlockMeta>>;

/// Go `time.Duration(secs * time.Second).String()` for whole seconds.
fn go_seconds_string(secs: i64) -> String {
    if secs == 0 {
        return "0s".to_string();
    }
    let neg = secs < 0;
    let s = secs.unsigned_abs();
    let (h, m, sec) = (s / 3600, (s / 60) % 60, s % 60);
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    if h > 0 {
        out.push_str(&format!("{h}h{m}m{sec}s"));
    } else if m > 0 {
        out.push_str(&format!("{m}m{sec}s"));
    } else {
        out.push_str(&format!("{sec}s"));
    }
    out
}

/// Go `Overlaps.String`.
pub fn overlaps_string(o: &Overlaps) -> String {
    let mut res = Vec::with_capacity(o.len());
    for (r, overlaps) in o {
        let groups: Vec<String> = overlaps
            .iter()
            .map(|m| {
                format!(
                    "<ulid: {}, mint: {}, maxt: {}, range: {}>",
                    m.ulid,
                    m.min_time,
                    m.max_time,
                    go_seconds_string((m.max_time - m.min_time) / 1000)
                )
            })
            .collect();
        res.push(format!(
            "[mint: {}, maxt: {}, range: {}, blocks: {}]: {}",
            r.min,
            r.max,
            go_seconds_string((r.max - r.min) / 1000),
            overlaps.len(),
            groups.join(", ")
        ));
    }
    res.join("\n")
}

/// Go `OverlappingBlocks`: all overlapping blocks of the metas (sorted by min time).
pub fn overlapping_blocks(bm: &[BlockMeta]) -> Overlaps {
    let mut groups = Overlaps::new();
    if bm.len() <= 1 {
        return groups;
    }
    let mut overlaps: Vec<Vec<BlockMeta>> = Vec::new();
    // Pending contains not ended blocks in regards to "current" timestamp.
    let mut pending: Vec<BlockMeta> = vec![bm[0].clone()];
    // Helps to aggregate same overlaps to single group.
    let mut continuous_pending = true;
    for b in &bm[1..] {
        let mut new_pending = Vec::new();
        for p in &pending {
            // "b.MinTime" is our current time.
            if b.min_time >= p.max_time {
                continuous_pending = false;
                continue;
            }
            // "p" overlaps with "b" and "p" is still pending.
            new_pending.push(p.clone());
        }
        // Our block "b" is now pending.
        let no_overlap = new_pending.is_empty();
        pending = new_pending.clone();
        pending.push(b.clone());
        if no_overlap {
            continue;
        }
        if continuous_pending && !overlaps.is_empty() {
            overlaps.last_mut().expect("non-empty").push(b.clone());
            continue;
        }
        let mut g = new_pending;
        g.push(b.clone());
        overlaps.push(g);
        // Start new pendings.
        continuous_pending = true;
    }
    // Fetch the critical overlapped time range foreach overlap groups.
    for overlap in overlaps {
        let mut r = TimeRange { min: 0, max: i64::MAX };
        for b in &overlap {
            if r.max > b.max_time {
                r.max = b.max_time;
            }
            if r.min < b.min_time {
                r.min = b.min_time;
            }
        }
        groups.insert(r, overlap);
    }
    groups
}

/// Go `isTmpDir`: a directory with a block ULID or chunk snapshot prefix and a tmp extension.
pub fn is_tmp_dir(fi: &std::fs::DirEntry) -> bool {
    if !fi.file_type().is_ok_and(|t| t.is_dir()) {
        return false;
    }
    let name = fi.file_name();
    let Some(fname) = name.to_str() else { return false };
    let ext = match fname.rfind('.') {
        Some(i) => &fname[i..],
        None => "",
    };
    if ext == TMP_FOR_DELETION_BLOCK_DIR_SUFFIX || ext == TMP_FOR_CREATION_BLOCK_DIR_SUFFIX || ext == TMP_LEGACY {
        if fname.starts_with(CHUNK_SNAPSHOT_PREFIX) {
            return true;
        }
        if Ulid::parse(&fname[..fname.len() - ext.len()]).is_ok() {
            return true;
        }
    }
    false
}

/// Go `blockDirs` (db.go): the block directories of `dir`, sorted by name.
pub fn block_dirs(dir: &Path) -> Result<Vec<PathBuf>, Error> {
    crate::compact::block_dirs(dir)
}

// ---------------------------------------------------------------------------------------------------------------
// The read-only DB.

/// Go `DBReadOnly`: read only operations on a database. Not for concurrent use.
pub struct DbReadOnly {
    logger: Logger,
    dir: PathBuf,
    sandbox_dir: PathBuf,
    closers: Vec<ReadOnlyCloser>,
    closed: bool,
}

enum ReadOnlyCloser {
    Block(Block),
    Head(Arc<Head>),
}

impl ReadOnlyCloser {
    fn close(&self) -> Result<(), Error> {
        match self {
            ReadOnlyCloser::Block(b) => b.close(),
            ReadOnlyCloser::Head(h) => h.close(),
        }
    }
}

fn make_temp_dir(root: &Path, prefix: &str) -> std::io::Result<PathBuf> {
    loop {
        let mut b = [0u8; 4];
        crypto::random_bytes(&mut b);
        let p = root.join(format!("{}{}", prefix, u32::from_le_bytes(b)));
        match std::fs::create_dir(&p) {
            Ok(()) => return Ok(p),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
}

/// A queryable over blocks and a head (Go: the `*DB` that `loadDataAsQueryable` returns).
pub struct ReadOnlyQueryable {
    blocks: Vec<Block>,
    head: Arc<Head>,
}

impl storage::Queryable for ReadOnlyQueryable {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        querier_over(&self.blocks, &self.head, ChunkDiskMapperRef(0), None, mint, maxt)
    }
}

impl storage::ChunkQueryable for ReadOnlyQueryable {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        let mut qs: Vec<BoxChunkQuerier> = Vec::new();
        let head = &self.head;
        let overlaps_ooo = crate::head::overlaps_closed_interval(mint, maxt, head.min_ooo_time(), head.max_ooo_time());
        let ino_mint = head.min_time().max(mint);
        let mut head_q: Option<BoxChunkQuerier> = None;
        if maxt >= head.min_time() || overlaps_ooo {
            let rh = RangeHead::new(head.clone(), mint, maxt);
            head_q = Some(
                crate::querier::new_block_chunk_querier(&rh, mint, maxt)
                    .map_err(|e| Error::wrap(format!("open querier for head {}", rh), e))?,
            );
        }
        if overlaps_ooo {
            let iso = head.ooo_iso.track_read_after(ChunkDiskMapperRef(0));
            head_q = Some(Box::new(HeadAndOOOChunkQuerier::new(ino_mint, mint, maxt, head.clone(), iso, head_q.take())));
        }
        if let Some(q) = head_q {
            qs.push(q);
        }
        for b in self.blocks.iter().filter(|b| b.overlaps_closed_interval(mint, maxt)) {
            match crate::querier::new_block_chunk_querier(b, mint, maxt) {
                Ok(q) => qs.push(q),
                Err(e) => {
                    for q in &mut qs {
                        let _ = q.close();
                    }
                    return Err(Error::wrap(format!("open querier for block {}", b), e));
                }
            }
        }
        let enc_head = head.clone();
        let enc: storage::FloatEncodingFn =
            Arc::new(move || Encoding(enc_head.opts.float_chunk_encoding.load(Ordering::Relaxed) as u8));
        let merge = storage::new_compacting_chunk_series_merger_with_float_encoding(
            Arc::new(storage::chained_series_merge),
            Some(enc),
        );
        Ok(storage::new_merge_chunk_querier(qs, Vec::new(), merge))
    }
}

impl DbReadOnly {
    /// Go `OpenDBReadOnly`: opens the DB in `dir` for read only operations. The sandbox (for head chunk files) is
    /// created in `sandbox_dir_root` (or `dir` when empty).
    pub fn open(dir: &Path, sandbox_dir_root: Option<&Path>, logger: Option<Logger>) -> Result<DbReadOnly, Error> {
        std::fs::metadata(dir).map_err(|e| io_wrap(format!("opening the db dir: stat {}", dir.display()), e))?;
        let root = sandbox_dir_root.filter(|p| !p.as_os_str().is_empty()).unwrap_or(dir);
        let sandbox_dir = make_temp_dir(root, "tmp_dbro_sandbox")
            .map_err(|e| io_wrap("setting up sandbox dir", e))?;
        Ok(DbReadOnly {
            logger: logger.unwrap_or_else(Logger::nop),
            dir: dir.to_path_buf(),
            sandbox_dir,
            closers: Vec::new(),
            closed: false,
        })
    }

    /// The sandbox directory.
    pub fn sandbox_dir(&self) -> &Path {
        &self.sandbox_dir
    }

    /// Go `FlushWAL`: a new block in `dir` with all the data in the WAL. Samples in existing blocks are not written.
    pub fn flush_wal(&mut self, dir: &Path) -> Result<(), Error> {
        let blocks = self.blocks().map_err(|e| Error::wrap("read blocks", e))?;
        let max_block_time = blocks.last().map_or(i64::MIN, |b| b.meta().max_time);
        let w = Arc::new(WL::open(self.dir.join("wal")).map_err(Error::other)?);
        let wbl_dir = self.dir.join(wlog::WBL_DIR_NAME);
        let wbl = if std::fs::metadata(&wbl_dir).is_ok() {
            Some(Arc::new(WL::open(&wbl_dir).map_err(Error::other)?))
        } else {
            None
        };
        let opts = HeadOptions { chunk_dir_root: self.dir.clone(), logger: self.logger.clone(), ..HeadOptions::default() };
        let head = Head::new(Some(w), wbl, opts, None)?;
        let res = (|| -> Result<(), Error> {
            // Set the min valid time for the ingested wal samples to be no lower than the maxt of the last block.
            head.init(max_block_time).map_err(|e| Error::wrap("read WAL", e))?;
            let mint = head.min_time();
            let maxt = head.max_time();
            let rh = RangeHead::new(head.clone(), mint, maxt);
            let compactor = new_leveled_compactor(
                &Context::background(),
                Some(self.logger.clone()),
                exponential_block_ranges(DEFAULT_BLOCK_DURATION, 3, 5),
                Some(Arc::new(chunkenc::new_pool())),
                None,
            )
            .map_err(|e| Error::wrap("create leveled compactor", e))?;
            // Add +1 millisecond to block maxt because block intervals are half-open: [b.MinTime, b.MaxTime).
            compactor.write(dir, &rh, mint, maxt + 1, None).map_err(|e| Error::wrap("writing WAL", e))?;
            Ok(())
        })();
        let close = head.close().err().map(|e| Error::wrap("closing Head", e));
        match (res, close) {
            (Ok(()), None) => Ok(()),
            (Ok(()), Some(e)) => Err(e),
            (Err(e), None) => Err(e),
            (Err(e), Some(c)) => Err(Error::join([Some(e), Some(c)]).expect("errors")),
        }
    }

    /// Go `loadDataAsQueryable`.
    fn load_data_as_queryable(&mut self, maxt: i64) -> Result<ReadOnlyQueryable, Error> {
        if self.closed {
            return Err(ERR_CLOSED);
        }
        let blocks = self.blocks()?;
        // Hard link the chunk files to a dir in the sandbox in case the head needs to truncate some of them or cut
        // new ones while replaying the WAL.
        chunks::hard_link_chunk_files(&mmapped_chunks_dir(&self.dir), &mmapped_chunks_dir(&self.sandbox_dir))
            .map_err(Error::other)?;
        let max_block_time = blocks.last().map_or(i64::MIN, |b| b.meta().max_time);
        let head;
        // Also add the WAL if the current blocks don't cover the requests time range.
        if max_block_time <= maxt {
            let w = Arc::new(WL::open(self.dir.join("wal")).map_err(Error::other)?);
            let wbl_dir = self.dir.join(wlog::WBL_DIR_NAME);
            let wbl = if std::fs::metadata(&wbl_dir).is_ok() {
                Some(Arc::new(WL::open(&wbl_dir).map_err(Error::other)?))
            } else {
                None
            };
            let opts = HeadOptions {
                chunk_dir_root: self.sandbox_dir.clone(),
                logger: self.logger.clone(),
                ..HeadOptions::default()
            };
            head = Head::new(Some(w), wbl, opts, None)?;
            // Set the min valid time for the ingested wal samples to be no lower than the maxt of the last block.
            if let Err(e) = head.init(max_block_time) {
                let _ = head.close();
                return Err(Error::wrap("read WAL", e));
            }
        } else {
            let opts = HeadOptions {
                chunk_dir_root: self.sandbox_dir.clone(),
                logger: self.logger.clone(),
                ..HeadOptions::default()
            };
            head = Head::new(None, None, opts, None)?;
        }
        self.closers.push(ReadOnlyCloser::Head(head.clone()));
        Ok(ReadOnlyQueryable { blocks, head })
    }

    /// Go `Querier`: loads the blocks and the WAL and returns a querier over the time range.
    pub fn querier(&mut self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        let q = self.load_data_as_queryable(maxt)?;
        storage::Queryable::querier(&q, mint, maxt)
    }

    /// Go `ChunkQuerier`.
    pub fn chunk_querier(&mut self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        let q = self.load_data_as_queryable(maxt)?;
        storage::ChunkQueryable::chunk_querier(&q, mint, maxt)
    }

    /// Go `Blocks`: the persisted blocks.
    pub fn blocks(&mut self) -> Result<Vec<Block>, Error> {
        if self.closed {
            return Err(ERR_CLOSED);
        }
        let (mut loadable, mut corrupted) = open_blocks(&self.logger, &self.dir, &[], None, None)?;
        // Corrupted blocks that have been superseded by a loadable block can be safely ignored.
        for b in &loadable {
            for p in &b.meta().compaction.parents {
                corrupted.remove(&p.ulid);
            }
        }
        if !corrupted.is_empty() {
            for b in &loadable {
                if let Err(e) = b.close() {
                    self.logger.warn("Closing block failed", &[("err", &e), ("block", b)]);
                }
            }
            let mut ids: Vec<_> = corrupted.into_iter().collect();
            ids.sort_by_key(|(u, _)| *u);
            let errs = ids.into_iter().map(|(u, e)| Some(Error::wrap(format!("corrupted block {}", u), e)));
            return Err(Error::join(errs).expect("errors"));
        }
        if loadable.is_empty() {
            return Ok(Vec::new());
        }
        loadable.sort_by_key(|b| b.meta().min_time);
        let metas: Vec<BlockMeta> = loadable.iter().map(|b| b.meta()).collect();
        let overlaps = overlapping_blocks(&metas);
        if !overlaps.is_empty() {
            self.logger.warn("Overlapping blocks found during opening", &[("detail", &overlaps_string(&overlaps))]);
        }
        // Close all previously open readers and add the new ones to the cache.
        for c in &self.closers {
            let _ = c.close();
        }
        self.closers = loadable.iter().map(|b| ReadOnlyCloser::Block(b.clone())).collect();
        Ok(loadable)
    }

    /// Go `LastBlockID`: the ID of the latest block.
    pub fn last_block_id(&self) -> Result<String, Error> {
        let entries = std::fs::read_dir(&self.dir).map_err(|e| io_wrap(format!("open {}", self.dir.display()), e))?;
        let mut names: Vec<String> =
            entries.filter_map(|e| e.ok()).filter_map(|e| e.file_name().to_str().map(String::from)).collect();
        names.sort();
        let mut max_t = 0u64;
        let mut last = String::new();
        for name in names {
            let Ok(u) = Ulid::parse(&name) else { continue };
            if u.time() > max_t {
                max_t = u.time();
                last = name;
            }
        }
        if last.is_empty() {
            return Err(Error::msg("no blocks found"));
        }
        Ok(last)
    }

    /// Go `Block`: a block reader by block ID.
    pub fn block(&mut self, block_id: &str, pd: Option<&PostingsDecoderFactory>) -> Result<Block, Error> {
        if self.closed {
            return Err(ERR_CLOSED);
        }
        let p = self.dir.join(block_id);
        if let Err(e) = std::fs::metadata(&p) {
            if e.kind() == std::io::ErrorKind::NotFound {
                return Err(Error::msg(format!("invalid block ID {}", block_id)));
            }
        }
        let b = open_block(Some(self.logger.clone()), &p, None, pd)?;
        self.closers.push(ReadOnlyCloser::Block(b.clone()));
        Ok(b)
    }

    /// Go `Close`: closes all block readers and deletes the sandbox directory.
    pub fn close(&mut self) -> Result<(), Error> {
        let res = if self.closed {
            Err(ERR_CLOSED)
        } else {
            self.closed = true;
            let errs: Vec<_> = self.closers.iter().map(|c| c.close().err()).collect();
            match Error::join(errs) {
                Some(e) => Err(e),
                None => Ok(()),
            }
        };
        // Delete the temporary sandbox directory that was created when opening the DB.
        if let Err(e) = std::fs::remove_dir_all(&self.sandbox_dir) {
            if e.kind() != std::io::ErrorKind::NotFound {
                self.logger.error("delete sandbox dir", &[("err", &e)]);
            }
        }
        res
    }
}
