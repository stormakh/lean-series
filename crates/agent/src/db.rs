//! Port of `tsdb/agent/db.go`: the WAL-only storage (options, metrics, open, WAL replay, series GC, WAL
//! truncation) and the V1 appender.

use std::collections::HashMap;
use std::io::Read;
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use model::exemplar::{EXEMPLAR_MAX_LABEL_SET_LENGTH, Exemplar};
use model::histogram::{FloatHistogram, Histogram};
use model::labels::Labels;
use model::metadata::{self, Handle, Metadata};
use storage::error::Sentinel;
use storage::{
    AppendOptions, Appendable, AppendableV2, Appender, AppenderTransaction, AppenderV2 as AppenderV2Trait,
    BoxChunkQuerier, BoxQuerier, ChunkQueryable, Context, ERR_DUPLICATE_EXEMPLAR, ERR_EXEMPLAR_LABEL_LENGTH,
    ERR_OUT_OF_ORDER_SAMPLE, ERR_OUT_OF_ORDER_ST, ERR_ST_NEWER_THAN_SAMPLE, Error, ExemplarQuerier,
    ExemplarQueryable, Queryable, SeriesRef, Storage,
};
use tsdb::head::{ERR_INVALID_EXEMPLAR, ERR_INVALID_SAMPLE};
use tsdb::metrics::{Counter, Gauge, Histogram as Summary};
use tsdb::tsdbutil::DirLocker;
use tsdb_format::chunks::HeadSeriesRef;
use tsdb_format::record::{
    self, Decoder, Encoder, RefExemplar, RefFloatHistogramSample, RefHistogramSample, RefMetadata, RefSample,
    RefSeries, Type,
};
use tsdb_format::wlog::{self, CorruptionErr, WL, WriteNotified, compression};

use crate::Logger;
use crate::series::{MemSeries, StripeSeries};

/// The `type` label value of float samples.
pub const SAMPLE_METRIC_TYPE_FLOAT: &str = "float";
/// The `type` label value of histogram samples.
pub const SAMPLE_METRIC_TYPE_HISTOGRAM: &str = "histogram";

static UNSUPPORTED: Sentinel = Sentinel::new("unsupported operation with WAL-only storage");
/// Go `ErrUnsupported`.
pub const ERR_UNSUPPORTED: Error = Error::Sentinel(&UNSUPPORTED);

/// Go `DefaultTruncateFrequency`.
pub const DEFAULT_TRUNCATE_FREQUENCY: Duration = Duration::from_secs(2 * 60 * 60);
/// Go `DefaultMinWALTime` (5 minutes in milliseconds).
pub const DEFAULT_MIN_WAL_TIME: i64 = 5 * 60 * 1000;
/// Go `DefaultMaxWALTime` (4 hours in milliseconds).
pub const DEFAULT_MAX_WAL_TIME: i64 = 4 * 60 * 60 * 1000;

/// Go `Options`: options of the WAL storage. The `Default` value is Go's zero value; see [`default_options`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Options {
    /// Segments (wal files) max size. <= 0: the default size.
    pub wal_segment_size: i64,
    /// The compression type of records in the WAL.
    pub wal_compression: compression::Type,
    /// The size (power of 2) in entries of the series hash map.
    pub stripe_size: usize,
    /// How frequently to truncate data from the WAL.
    pub truncate_frequency: Duration,
    /// Shortest amount of time data can exist in the WAL before being deleted.
    pub min_wal_time: i64,
    /// Longest amount of time data can exist in the WAL before being deleted.
    pub max_wal_time: i64,
    /// Disables creation and consideration of a lock file.
    pub no_lockfile: bool,
    /// How much out of order is allowed, if any.
    pub out_of_order_time_window: i64,
    /// The 'created-timestamp-zero-ingestion' feature flag: a start timestamp earlier than the sample
    /// timestamp is stored as a zero sample before the sample (best effort).
    pub enable_st_as_zero_sample: bool,
    /// The `st-storage` feature flag: write the start timestamp per sample to the WAL (V2 records).
    pub enable_st_storage: bool,
    /// The 'metadata-wal-records' feature flag.
    pub enable_metadata_wal_records: bool,
    /// Build checkpoints from the in-memory series only, without re-reading the previous checkpoint and
    /// segments.
    pub checkpoint_from_in_memory_series: bool,
    /// The number of series in one checkpoint WAL record batch. Only with `checkpoint_from_in_memory_series`.
    pub checkpoint_batch_size: usize,
}

/// Go `DefaultOptions`.
pub fn default_options() -> Options {
    Options {
        wal_segment_size: wlog::DEFAULT_SEGMENT_SIZE as i64,
        wal_compression: compression::Type::None,
        stripe_size: tsdb::head::DEFAULT_STRIPE_SIZE,
        truncate_frequency: DEFAULT_TRUNCATE_FREQUENCY,
        min_wal_time: DEFAULT_MIN_WAL_TIME,
        max_wal_time: DEFAULT_MAX_WAL_TIME,
        no_lockfile: false,
        out_of_order_time_window: 0,
        ..Default::default()
    }
}

/// Go `validateOptions`.
pub fn validate_options(opts: Option<Options>) -> Options {
    let mut opts = opts.unwrap_or_else(default_options);
    if opts.wal_segment_size <= 0 {
        opts.wal_segment_size = wlog::DEFAULT_SEGMENT_SIZE as i64;
    }
    // Revert the stripe size to the default if it is 0 or not a power of 2.
    if opts.stripe_size == 0 || !opts.stripe_size.is_power_of_two() {
        opts.stripe_size = tsdb::head::DEFAULT_STRIPE_SIZE;
    }
    if opts.truncate_frequency.is_zero() {
        opts.truncate_frequency = DEFAULT_TRUNCATE_FREQUENCY;
    }
    if opts.min_wal_time <= 0 {
        opts.min_wal_time = DEFAULT_MIN_WAL_TIME;
    }
    if opts.max_wal_time <= 0 {
        opts.max_wal_time = DEFAULT_MAX_WAL_TIME;
    }
    if opts.min_wal_time > opts.max_wal_time {
        opts.max_wal_time = opts.min_wal_time;
    }
    let t = opts.truncate_frequency.as_millis() as i64;
    if opts.max_wal_time < t {
        opts.max_wal_time = t;
    }
    opts
}

/// `prometheus_agent_samples_appended_total{type}`.
#[derive(Debug, Default)]
pub struct SamplesAppended {
    /// `type="float"`.
    pub float: Counter,
    /// `type="histogram"`.
    pub histogram: Counter,
}

impl SamplesAppended {
    /// Go `WithLabelValues(type)`.
    pub fn with_label_values(&self, t: &str) -> &Counter {
        if t == SAMPLE_METRIC_TYPE_HISTOGRAM { &self.histogram } else { &self.float }
    }
}

/// Go `dbMetrics`.
#[derive(Debug, Default)]
pub struct DbMetrics {
    /// `prometheus_agent_active_series`: number of active series being tracked by the WAL storage.
    pub num_active_series: Gauge,
    /// `prometheus_agent_deleted_series`: number of series pending deletion from the WAL.
    pub num_wal_series_pending_deletion: Gauge,
    /// `prometheus_agent_samples_appended_total`: total number of samples appended to the storage.
    pub total_appended_samples: SamplesAppended,
    /// `prometheus_agent_exemplars_appended_total`: total number of exemplars appended to the storage.
    pub total_appended_exemplars: Counter,
    /// `prometheus_agent_out_of_order_samples_total`: total number of out of order samples ingestion failed
    /// attempts.
    pub total_out_of_order_samples: Counter,
    /// `prometheus_agent_truncate_duration_seconds` (a summary): duration of WAL truncation.
    pub wal_truncate_duration: Summary,
    /// `prometheus_agent_corruptions_total`: total number of WAL corruptions.
    pub wal_corruptions_total: Counter,
    /// `prometheus_agent_data_replay_duration_seconds`: time taken to replay the data on disk.
    pub wal_total_replay_duration: Gauge,
    /// `prometheus_agent_checkpoint_deletions_failed_total`.
    pub checkpoint_delete_fail: Counter,
    /// `prometheus_agent_checkpoint_deletions_total`.
    pub checkpoint_delete_total: Counter,
    /// `prometheus_agent_checkpoint_creations_failed_total`.
    pub checkpoint_creation_fail: Counter,
    /// `prometheus_agent_checkpoint_creations_total`.
    pub checkpoint_creation_total: Counter,
}

/// Go `deletedRefMeta`: the WAL segment a deleted series must be kept around to, with its labels and
/// metadata (kept only for the in-memory checkpoint).
#[derive(Clone, Debug, Default)]
pub struct DeletedRefMeta {
    pub last_segment: i64,
    pub labels: Labels,
    pub meta: Handle,
}

/// The lowest timestamp remote write has sent (Go `remote.Storage.LowestSentTimestamp`). The agent uses it
/// to decide which series may be truncated from the WAL.
pub trait LowestSentTimestamp: Send + Sync {
    fn lowest_sent_timestamp(&self) -> i64;
}

/// The state of the WAL storage shared by the DB, its appenders and its truncation thread (Go `DB` without
/// the lock file and the run loop).
pub struct DbInner {
    pub logger: Logger,
    pub opts: Options,
    rs: Option<Arc<dyn LowestSentTimestamp>>,

    pub wal: WL,
    /// Go `db.mtx`: appenders log under the read lock; close takes the write lock.
    pub mtx: RwLock<()>,

    /// Go `bufPool`: encoding buffers of the appenders.
    pub buf_pool: Mutex<Vec<Vec<u8>>>,

    pub next_ref: AtomicU64,
    pub series: StripeSeries,
    /// Refs of series that should be deleted from the WAL, with the WAL segment they must be kept around to.
    pub deleted: Mutex<HashMap<HeadSeriesRef, DeletedRefMeta>>,

    write_notified: RwLock<Option<Arc<dyn WriteNotified>>>,

    pub metrics: DbMetrics,
}

/// Go `DB`: a WAL-only storage. It implements `storage::Storage`.
pub struct Db {
    inner: Arc<DbInner>,
    pub locker: Mutex<Option<DirLocker>>,
    run: Mutex<Option<(mpsc::Sender<()>, JoinHandle<()>)>>,
    closed: Mutex<bool>,
}

impl Deref for Db {
    type Target = DbInner;
    fn deref(&self) -> &DbInner {
        &self.inner
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// A wlog error as a storage error with Go's text `"<ctx>: <err>"` (the wlog error stays reachable by
/// downcast).
fn werr(ctx: &str, e: wlog::Error) -> Error {
    Error::other(e.wrap(ctx))
}

/// Go `Open`: opens (and replays) the agent storage in `dir`; the WAL is in `<dir>/wal`.
pub fn open(
    l: Logger,
    rs: Option<Arc<dyn LowestSentTimestamp>>,
    dir: &Path,
    opts: Option<Options>,
) -> Result<Db, Error> {
    let opts = validate_options(opts);

    let mut locker = DirLocker::new(dir, "agent", l.clone())?;
    if !opts.no_lockfile {
        locker.lock()?;
    }

    // Remote write expects the WAL to be stored in a "wal" subdirectory of the main storage.
    let dir = dir.join("wal");

    let w = WL::new_size(&dir, opts.wal_segment_size as usize, opts.wal_compression)
        .map_err(|e| werr("creating WAL", e))?
        .with_logger(l.clone());

    let inner = Arc::new(DbInner::new(l, opts, rs, w));

    if let Err(e) = inner.replay_wal() {
        inner.logger.warn("encountered WAL read error, attempting repair", &[("err", &e)]);
        if let Err(e) = inner.wal.repair(&e) {
            return Err(werr("repair corrupted WAL", e));
        }
        inner.logger.info("successfully repaired WAL", &[]);
    }

    let (tx, rx) = mpsc::channel::<()>();
    let db2 = inner.clone();
    let h = std::thread::Builder::new()
        .name("agent-truncate".into())
        .spawn(move || db2.run(rx))
        .map_err(Error::other)?;

    Ok(Db {
        inner,
        locker: Mutex::new(Some(locker)),
        run: Mutex::new(Some((tx, h))),
        closed: Mutex::new(false),
    })
}

/// Go `walTruncationTime`: the timestamp before which inactive series may be truncated from the WAL, keeping
/// between `min_wal_time` and `max_wal_time` of data.
pub fn wal_truncation_time(opts: &Options, lowest_sent_ts: i64, now_ts: i64) -> i64 {
    // Keep MinWALTime of data behind what remote write has sent. Floor at 0 since lowestSentTs is 0 until
    // remote write sends something.
    let mut ts = (lowest_sent_ts - opts.min_wal_time).max(0);
    // Keep at most MaxWALTime of data, even if remote write is stalled.
    ts = ts.max(now_ts - opts.max_wal_time);
    // Keep at least MinWALTime of data, even if lowestSentTs is in the future. Floor at 0.
    ts.min((now_ts - opts.min_wal_time).max(0))
}

impl DbInner {
    /// A DB around an open WAL, without replay, lock file or truncation thread (Go `&DB{wal: wal, opts: opts}`
    /// plus the fields `Open` sets).
    pub fn new(logger: Logger, opts: Options, rs: Option<Arc<dyn LowestSentTimestamp>>, wal: WL) -> DbInner {
        let stripe_size = if opts.stripe_size.is_power_of_two() { opts.stripe_size } else { 1 };
        DbInner {
            logger,
            rs,
            wal,
            mtx: RwLock::new(()),
            buf_pool: Mutex::new(Vec::new()),
            next_ref: AtomicU64::new(0),
            series: StripeSeries::new(stripe_size),
            deleted: Mutex::new(HashMap::new()),
            write_notified: RwLock::new(None),
            metrics: DbMetrics::default(),
            opts,
        }
    }

    /// Go `SetWriteNotified`: the instance to notify when a write happens.
    pub fn set_write_notified(&self, wn: Option<Arc<dyn WriteNotified>>) {
        *self.write_notified.write().unwrap_or_else(|e| e.into_inner()) = wn;
    }

    fn replay_wal(&self) -> Result<(), wlog::Error> {
        self.logger.info("replaying WAL, this may take a while", &[("dir", &self.wal.dir().display())]);
        let start = Instant::now();

        let mut duplicate_ref_to_valid_ref: HashMap<HeadSeriesRef, HeadSeriesRef> = HashMap::new();

        let mut start_from = 0i64;
        match wlog::last_checkpoint(self.wal.dir()) {
            Ok((dir, idx)) => {
                start_from = idx;
                let sr = wlog::new_segments_reader(&dir).map_err(|e| e.wrap("open checkpoint"))?;
                let mut r = wlog::Reader::new(sr);
                // A corrupted checkpoint is a hard error for now and requires user intervention. There's
                // likely little data that can be recovered anyway.
                let res = self.load_wal(&mut r, &mut duplicate_ref_to_valid_ref, start_from);
                if let Err(e) = r.into_inner().close() {
                    self.logger.warn("error while closing the wal segments reader", &[("err", &e)]);
                }
                res.map_err(|e| e.wrap("backfill checkpoint"))?;
                start_from += 1;
                self.logger.info("WAL checkpoint loaded", &[]);
            }
            Err(e) if e.is_not_found() => {}
            Err(e) => return Err(e.wrap("find last checkpoint")),
        }

        // Find the last segment.
        let (_, last) = wlog::segments(self.wal.dir()).map_err(|e| e.wrap("finding WAL segments"))?;

        // Backfill segments from the most recent checkpoint onwards.
        for i in start_from..=last {
            let seg = wlog::open_read_segment(&wlog::segment_name(self.wal.dir(), i))
                .map_err(|e| e.wrap(&format!("open WAL segment: {i}")))?;
            let mut r = wlog::Reader::new(wlog::new_segment_buf_reader(vec![seg]));
            let res = self.load_wal(&mut r, &mut duplicate_ref_to_valid_ref, i);
            if let Err(e) = r.into_inner().close() {
                self.logger.warn("error while closing the wal segments reader", &[("err", &e)]);
            }
            res?;
            self.logger.info("WAL segment loaded", &[("segment", &i), ("maxSegment", &last)]);
        }

        self.metrics.wal_total_replay_duration.set(start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Go `loadWAL`: replays the records of `r` (a checkpoint or segment with index
    /// `current_segment_or_checkpoint`).
    pub fn load_wal<R: Read + 'static>(
        &self,
        r: &mut wlog::Reader<R>,
        duplicate_ref_to_valid_ref: &mut HashMap<HeadSeriesRef, HeadSeriesRef>,
        current_segment_or_checkpoint: i64,
    ) -> Result<(), wlog::Error> {
        let mut last_ref = self.next_ref.load(Ordering::SeqCst);
        let mut non_existent_series_refs = 0u64;
        let res = self.load_wal_records(
            r,
            duplicate_ref_to_valid_ref,
            current_segment_or_checkpoint,
            &mut last_ref,
            &mut non_existent_series_refs,
        );

        if non_existent_series_refs > 0 {
            self.logger.warn(
                "found sample referencing non-existing series",
                &[("skipped_series", &non_existent_series_refs)],
            );
        }
        self.next_ref.store(last_ref, Ordering::SeqCst);

        res?;
        if let Some(e) = r.err() {
            return Err(e.wrap("read records"));
        }
        Ok(())
    }

    fn load_wal_records<R: Read + 'static>(
        &self,
        r: &mut wlog::Reader<R>,
        dup: &mut HashMap<HeadSeriesRef, HeadSeriesRef>,
        cur: i64,
        last_ref: &mut u64,
        non_existent: &mut u64,
    ) -> Result<(), wlog::Error> {
        let mut dec = Decoder::new(self.logger.clone());
        let mut series_buf: Vec<RefSeries> = Vec::new();
        let mut samples_buf: Vec<RefSample> = Vec::new();
        let mut hists_buf: Vec<RefHistogramSample> = Vec::new();
        let mut fhists_buf: Vec<RefFloatHistogramSample> = Vec::new();
        let mut meta_buf: Vec<RefMetadata> = Vec::new();
        let mut deleted = lock(&self.deleted);

        let corruption = |what: &str, e: record::Error, r: &wlog::Reader<R>| {
            wlog::Error::Corruption(Box::new(CorruptionErr {
                dir: Default::default(),
                segment: r.segment(),
                offset: r.offset(),
                err: wlog::Error::Msg(format!("{what}: {e}")),
            }))
        };

        // Remaps a duplicate ref to its valid ref and keeps the duplicate's series record in the checkpoints
        // until the largest segment it was seen in.
        fn remap(
            deleted: &mut HashMap<HeadSeriesRef, DeletedRefMeta>,
            dup: &HashMap<HeadSeriesRef, HeadSeriesRef>,
            r: &mut HeadSeriesRef,
            cur: i64,
        ) {
            if let Some(&valid) = dup.get(r) {
                if let Some(meta) = deleted.get_mut(r)
                    && meta.last_segment <= cur
                {
                    meta.last_segment = cur;
                }
                *r = valid;
            }
        }

        while r.next() {
            let rec = r.record();
            match dec.r#type(rec) {
                Type::Series => {
                    series_buf.clear();
                    if let Err(e) = dec.series(rec, &mut series_buf) {
                        return Err(corruption("decode series", e, r));
                    }
                    for entry in series_buf.drain(..) {
                        // Make sure we don't try to reuse a ref that already exists in the WAL.
                        if entry.r#ref > *last_ref {
                            *last_ref = entry.r#ref;
                        }
                        let hash = entry.labels.hash();
                        let (series, created) = self
                            .series
                            .set_unless_already_set(hash, MemSeries::new(entry.r#ref, entry.labels.clone(), 0));
                        if !created {
                            // set_unless_already_set is "first insertion wins": during single-threaded replay
                            // the first ref written for a label set is the canonical one.
                            dup.insert(entry.r#ref, series.r#ref);

                            // Track the largest segment where we encountered the duplicate ref, so it remains
                            // in the checkpoint until we get past that segment.
                            let meta = deleted.get(&entry.r#ref).cloned().unwrap_or_default();
                            if meta.last_segment <= cur {
                                let lbls = if self.opts.checkpoint_from_in_memory_series {
                                    entry.labels
                                } else {
                                    Labels::default()
                                };
                                deleted.insert(
                                    entry.r#ref,
                                    DeletedRefMeta {
                                        last_segment: cur,
                                        labels: lbls,
                                        meta: meta.meta,
                                    },
                                );
                            }
                        } else {
                            self.metrics.num_active_series.inc();
                        }
                    }
                }
                Type::Samples | Type::SamplesV2 => {
                    samples_buf.clear();
                    if let Err(e) = dec.samples(rec, &mut samples_buf) {
                        return Err(corruption("decode samples", e, r));
                    }
                    for entry in &samples_buf {
                        let mut sref = entry.r#ref;
                        remap(&mut deleted, dup, &mut sref, cur);
                        let Some(series) = self.series.get_by_id(sref) else {
                            *non_existent += 1;
                            continue;
                        };
                        // Update the lastTs for the series if this sample is newer.
                        let mut st = series.lock();
                        if entry.t > st.last_ts {
                            st.last_ts = entry.t;
                        }
                    }
                }
                Type::HistogramSamples | Type::CustomBucketsHistogramSamples | Type::HistogramSamplesV2 => {
                    hists_buf.clear();
                    if let Err(e) = dec.histogram_samples(rec, &mut hists_buf) {
                        return Err(corruption("decode histogram samples", e, r));
                    }
                    for entry in &hists_buf {
                        let mut sref = entry.r#ref;
                        remap(&mut deleted, dup, &mut sref, cur);
                        let Some(series) = self.series.get_by_id(sref) else {
                            *non_existent += 1;
                            continue;
                        };
                        let mut st = series.lock();
                        if entry.t > st.last_ts {
                            st.last_ts = entry.t;
                        }
                    }
                    hists_buf.clear();
                }
                Type::FloatHistogramSamples
                | Type::CustomBucketsFloatHistogramSamples
                | Type::FloatHistogramSamplesV2 => {
                    fhists_buf.clear();
                    if let Err(e) = dec.float_histogram_samples(rec, &mut fhists_buf) {
                        return Err(corruption("decode float histogram samples", e, r));
                    }
                    for entry in &fhists_buf {
                        let mut sref = entry.r#ref;
                        remap(&mut deleted, dup, &mut sref, cur);
                        let Some(series) = self.series.get_by_id(sref) else {
                            *non_existent += 1;
                            continue;
                        };
                        let mut st = series.lock();
                        if entry.t > st.last_ts {
                            st.last_ts = entry.t;
                        }
                    }
                    fhists_buf.clear();
                }
                Type::Metadata => {
                    meta_buf.clear();
                    if let Err(e) = dec.metadata(rec, &mut meta_buf) {
                        return Err(corruption("decode metadata", e, r));
                    }
                    // Metadata records are replayed regardless of enable_metadata_wal_records, to keep the
                    // existing metadata of the WAL (as the TSDB head replay does).
                    for entry in meta_buf.drain(..) {
                        let m = Handle::make(Metadata {
                            typ: record::to_metric_type(entry.r#type),
                            unit: entry.unit,
                            help: entry.help,
                        });
                        let orig_ref = entry.r#ref;
                        let mut sref = entry.r#ref;
                        if let Some(&valid) = dup.get(&sref) {
                            if let Some(meta) = deleted.get_mut(&sref)
                                && meta.last_segment <= cur
                            {
                                meta.last_segment = cur;
                                if self.opts.checkpoint_from_in_memory_series {
                                    meta.meta = m.clone();
                                }
                            }
                            sref = valid;
                        }
                        let Some(series) = self.series.get_by_id(sref) else {
                            *non_existent += 1;
                            continue;
                        };
                        let mut st = series.lock();
                        // The in-memory checkpoint writes active series before deleted series, so an older
                        // deleted series (orig_ref < sref) can be replayed after a recreated active series and
                        // must not overwrite its metadata.
                        if orig_ref < sref && !metadata::is_zero_handle(&st.meta) {
                            continue;
                        }
                        st.meta = m;
                    }
                }
                Type::Tombstones | Type::Exemplars => {
                    // We don't care about tombstones or exemplars during replay.
                }
                _ => {
                    // Unknown records are ignored, enabling users to roll back.
                }
            }
        }
        Ok(())
    }

    fn run(self: Arc<Self>, stop: mpsc::Receiver<()>) {
        loop {
            match stop.recv_timeout(self.opts.truncate_frequency) {
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // ts decides which series are not receiving samples and may be deleted from the WAL:
                    // series whose most recent append is older than ts are inactive.
                    let lowest = self.rs.as_ref().map_or(0, |rs| rs.lowest_sent_timestamp());
                    let ts = wal_truncation_time(&self.opts, lowest, model::timestamp::now());
                    self.logger.debug("truncating the WAL", &[("ts", &ts)]);
                    if let Err(e) = self.truncate(ts) {
                        self.logger.warn("failed to truncate WAL", &[("err", &e)]);
                    }
                }
                _ => return,
            }
        }
    }

    /// Go `keepSeriesInWALCheckpointFn`: whether a series record is kept in the checkpoint. `last` is the last
    /// WAL segment considered for checkpointing. Unlike the TSDB, the agent uses WAL segment numbers instead of
    /// timestamps.
    pub fn keep_series_in_wal_checkpoint_fn(&self, last: i64) -> impl Fn(HeadSeriesRef) -> bool + '_ {
        move |id| {
            // Keep the record if the series exists in the db.
            if self.series.get_by_id(id).is_some() {
                return true;
            }
            // Keep the record if the series was recently deleted.
            lock(&self.deleted).get(&id).is_some_and(|m| m.last_segment > last)
        }
    }

    /// Go `truncate`: garbage collects the series without samples since `mint`, writes a checkpoint of the
    /// lower two thirds of the segments and deletes those segments.
    pub fn truncate(&self, mint: i64) -> Result<(), Error> {
        self.logger.info("series GC started", &[]);
        let _g = self.mtx.read().unwrap_or_else(|e| e.into_inner());

        let start = Instant::now();

        self.gc(mint);
        self.logger.info("series GC completed", &[("duration", &format!("{:?}", start.elapsed()))]);

        let (first, mut last) = wlog::segments(self.wal.dir()).map_err(|e| werr("get segment range", e))?;

        // Start a new segment so low ingestion volume instances don't have more WAL than needed.
        self.wal.next_segment().map_err(|e| werr("next segment", e))?;

        last -= 1; // Never consider the most recent segment for checkpoint.
        if last < 0 {
            return Ok(()); // No segments yet.
        }

        // The lower two-thirds of segments should contain mostly obsolete samples. If we have less than two
        // segments, it's not worth checkpointing yet.
        last = first + (last - first) * 2 / 3;
        if last <= first {
            return Ok(());
        }

        self.metrics.checkpoint_creation_total.inc();

        let res = if self.opts.checkpoint_from_in_memory_series {
            let deleted = lock(&self.deleted);
            crate::checkpoint::checkpoint(
                &self.logger,
                &self.wal,
                last,
                self.opts.checkpoint_batch_size,
                self.series.all_series(),
                crate::series::deleted_series_iter(&deleted, last),
            )
        } else {
            // write_min_valid_time is false: the agent has no use for it, and an older agent would not know
            // the record.
            let keep = self.keep_series_in_wal_checkpoint_fn(last);
            wlog::checkpoint(&self.wal, first, last, &keep, mint, self.opts.enable_st_storage, false).map(|_| ())
        };
        if let Err(e) = res {
            self.metrics.checkpoint_creation_fail.inc();
            if e.corruption().is_some() {
                self.metrics.wal_corruptions_total.inc();
            }
            return Err(werr("create checkpoint", e));
        }
        if let Err(e) = self.wal.truncate(last + 1) {
            // If truncating fails, we'll just try it again at the next checkpoint. Leftover segments will be
            // ignored in the future if there's a checkpoint that supersedes them.
            self.logger.error("truncating segments failed", &[("err", &e)]);
        }

        // The checkpoint is written and segments before it are truncated, so we no longer need to track
        // deleted series that were being kept around.
        {
            let mut deleted = lock(&self.deleted);
            deleted.retain(|_, m| m.last_segment > last);
            self.metrics.checkpoint_delete_total.inc();
            self.metrics.num_wal_series_pending_deletion.set(deleted.len() as f64);
        }

        if let Err(e) = wlog::delete_checkpoints(self.wal.dir(), last) {
            // Leftover old checkpoints do not cause problems down the line beyond occupying disk space.
            self.logger.error("delete old checkpoints", &[("err", &e)]);
            self.metrics.checkpoint_delete_fail.inc();
        }

        self.metrics.wal_truncate_duration.observe(start.elapsed().as_secs_f64());

        self.logger.info(
            "WAL checkpoint complete",
            &[("first", &first), ("last", &last), ("duration", &format!("{:?}", start.elapsed()))],
        );
        Ok(())
    }

    /// Go `gc`: marks the series that have not received a sample since `mint` as deleted, with the segment
    /// where they got deleted.
    pub fn gc(&self, mint: i64) {
        let deleted_now = self.series.gc(mint, self.opts.checkpoint_from_in_memory_series);
        self.metrics.num_active_series.sub(deleted_now.len() as f64);

        let last = wlog::segments(self.wal.dir()).map(|(_, l)| l).unwrap_or(-1);

        // Keep series records for any newly deleted series until we've passed the last recorded segment. This
        // prevents the WAL having samples for series records that no longer exist.
        let mut deleted = lock(&self.deleted);
        for (r, ds) in deleted_now {
            deleted.insert(
                r,
                DeletedRefMeta {
                    last_segment: last,
                    labels: ds.labels,
                    meta: ds.meta,
                },
            );
        }
        self.metrics.num_wal_series_pending_deletion.set(deleted.len() as f64);
    }

    /// Go `StartTime`.
    pub fn start_time(&self) -> Result<i64, Error> {
        Ok(i64::MAX) // Go model.Latest.
    }

    fn take_buf(&self) -> Vec<u8> {
        lock(&self.buf_pool).pop().unwrap_or_else(|| Vec::with_capacity(1024))
    }

    fn put_buf(&self, mut b: Vec<u8>) {
        b.clear();
        lock(&self.buf_pool).push(b);
    }
}

impl Db {
    /// The shared state, for appenders created outside of the `Appendable` traits.
    pub fn inner(&self) -> &Arc<DbInner> {
        &self.inner
    }

    /// Go `Appender`.
    pub fn appender(&self) -> AppenderV1 {
        AppenderV1 {
            base: AppenderBase::new(self.inner.clone()),
            hints: None,
        }
    }

    /// Go `AppenderV2`.
    pub fn appender_v2(&self) -> crate::db_append_v2::AppenderV2 {
        crate::db_append_v2::AppenderV2 {
            base: AppenderBase::new(self.inner.clone()),
        }
    }

    /// Go `Close`: stops the truncation loop, releases the lock file and closes the WAL.
    pub fn close(&self) -> Result<(), Error> {
        if let Some((tx, h)) = lock(&self.run).take() {
            drop(tx);
            let _ = h.join();
        }
        let _g = self.mtx.write().unwrap_or_else(|e| e.into_inner());
        *lock(&self.closed) = true;
        let e1 = lock(&self.locker).take().and_then(|mut l| l.release().err());
        let e2 = self.wal.close().err().map(Error::other);
        match Error::join([e1, e2]) {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if !*lock(&self.closed) {
            let _ = self.close();
        }
    }
}

impl Queryable for Db {
    fn querier(&self, _mint: i64, _maxt: i64) -> Result<BoxQuerier, Error> {
        Err(ERR_UNSUPPORTED)
    }
}

impl ChunkQueryable for Db {
    fn chunk_querier(&self, _mint: i64, _maxt: i64) -> Result<BoxChunkQuerier, Error> {
        Err(ERR_UNSUPPORTED)
    }
}

impl ExemplarQueryable for Db {
    fn exemplar_querier(&self, _ctx: &Context) -> Result<Box<dyn ExemplarQuerier>, Error> {
        Err(ERR_UNSUPPORTED)
    }
}

impl Appendable for Db {
    fn appender(&self, _ctx: &Context) -> Box<dyn Appender> {
        Box::new(Db::appender(self))
    }
}

impl AppendableV2 for Db {
    fn appender_v2(&self, _ctx: &Context) -> Box<dyn AppenderV2Trait> {
        Box::new(Db::appender_v2(self))
    }
}

impl Storage for Db {
    fn start_time(&self) -> Result<i64, Error> {
        DbInner::start_time(self)
    }
    fn close(&self) -> Option<Error> {
        Db::close(self).err()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Appenders.

/// Go `appenderBase`: the pending data of a transaction, shared by the V1 and V2 appenders.
pub struct AppenderBase {
    pub db: Arc<DbInner>,

    pub pending_series: Vec<RefSeries>,
    pub pending_samples: Vec<RefSample>,
    pub pending_histograms: Vec<RefHistogramSample>,
    pub pending_float_histograms: Vec<RefFloatHistogramSample>,
    pub pending_exemplars: Vec<RefExemplar>,
    pub pending_metadata: Vec<RefMetadata>,

    /// The series of each element of `pending_samples` (series locks are not held).
    pub sample_series: Vec<Arc<MemSeries>>,
    /// The series of each element of `pending_histograms`.
    pub histogram_series: Vec<Arc<MemSeries>>,
    /// The series of each element of `pending_float_histograms`.
    pub float_histogram_series: Vec<Arc<MemSeries>>,
    /// The series of each element of `pending_metadata`.
    pub metadata_series: Vec<Arc<MemSeries>>,
}

/// Returns the encoding buffer to the pool, empty, on every return path (Go `defer bufPool.Put(buf[:0])`).
struct PooledBuf<'a> {
    db: &'a DbInner,
    buf: Option<Vec<u8>>,
}

impl PooledBuf<'_> {
    fn get(&mut self) -> &mut Vec<u8> {
        self.buf.as_mut().expect("buffer")
    }
}

impl Drop for PooledBuf<'_> {
    fn drop(&mut self) {
        if let Some(b) = self.buf.take() {
            self.db.put_buf(b);
        }
    }
}

impl AppenderBase {
    pub fn new(db: Arc<DbInner>) -> AppenderBase {
        AppenderBase {
            db,
            pending_series: Vec::new(),
            pending_samples: Vec::new(),
            pending_histograms: Vec::new(),
            pending_float_histograms: Vec::new(),
            pending_exemplars: Vec::new(),
            pending_metadata: Vec::new(),
            sample_series: Vec::new(),
            histogram_series: Vec::new(),
            float_histogram_series: Vec::new(),
            metadata_series: Vec::new(),
        }
    }

    /// Go `getOrCreate`.
    pub fn get_or_create(&mut self, r#ref: HeadSeriesRef, l: &Labels) -> Result<Arc<MemSeries>, Error> {
        // Fastest path: the caller already has a valid ref from a prior append.
        if r#ref != 0
            && let Some(s) = self.db.series.get_by_id(r#ref)
        {
            return Ok(s);
        }

        // Ensure no empty or duplicate labels have gotten through. This mirrors the TSDB head appender.
        let l = l.without_empty();
        if l.is_empty() {
            return Err(Error::wrap("empty labelset", ERR_INVALID_SAMPLE));
        }
        if let Some(lbl) = l.has_duplicate_label_names() {
            return Err(Error::wrap(format!("label name \"{lbl}\" is not unique"), ERR_INVALID_SAMPLE));
        }

        let hash = l.hash();

        // Fast path: the series already exists. This avoids burning a ref on every append.
        if let Some(s) = self.db.series.get_by_hash(hash, &l) {
            return Ok(s);
        }

        // This ref is wasted if a concurrent appender inserts the same series first.
        let new_ref = self.db.next_ref.fetch_add(1, Ordering::SeqCst) + 1;
        let (series, created) =
            self.db.series.set_unless_already_set(hash, MemSeries::new(new_ref, l.clone(), i64::MIN));
        if !created {
            // A concurrent appender inserted this series first; skip the WAL record and metric update.
            return Ok(series);
        }

        // Known limitation (as upstream): agent series have no pending-commit protection. Until the first
        // sample updates last_ts, GC may remove the series, and its WAL record would then reference a ref with
        // no in-memory series.
        self.pending_series.push(RefSeries {
            r#ref: series.r#ref,
            labels: l,
        });
        self.db.metrics.num_active_series.inc();
        Ok(series)
    }

    /// Go `validateExemplar`.
    pub fn validate_exemplar(&self, r#ref: HeadSeriesRef, e: &Exemplar) -> Result<(), Error> {
        if let Some(lbl) = e.labels.has_duplicate_label_names() {
            return Err(Error::wrap(format!("label name \"{lbl}\" is not unique"), ERR_INVALID_EXEMPLAR));
        }

        // Exemplar label length does not include chars involved in text rendering such as quotes, equals sign,
        // or commas. See EXEMPLAR_MAX_LABEL_SET_LENGTH.
        let mut label_set_len = 0usize;
        e.labels.validate(|n, v| {
            label_set_len += n.chars().count();
            label_set_len += v.chars().count();
            if label_set_len > EXEMPLAR_MAX_LABEL_SET_LENGTH {
                return Err(ERR_EXEMPLAR_LABEL_LENGTH);
            }
            Ok(())
        })?;

        // Discard a duplicate of the last stored exemplar of this series (the TSDB returns 0 for duplicates).
        if self.db.series.latest_exemplar_equals(r#ref, e) {
            return Err(ERR_DUPLICATE_EXEMPLAR);
        }
        Ok(())
    }

    /// Go `commit`.
    pub fn commit(&mut self) -> Result<(), Error> {
        let r = self.log();
        self.clear_data();
        r?;
        let wn = self.db.write_notified.read().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(wn) = wn {
            wn.notify();
        }
        Ok(())
    }

    /// Go `log`: logs all pending data to the WAL.
    pub fn log(&mut self) -> Result<(), Error> {
        let db = self.db.clone();
        let _g = db.mtx.read().unwrap_or_else(|e| e.into_inner());

        let encoder = Encoder {
            enable_st_storage: db.opts.enable_st_storage,
        };
        let mut pb = PooledBuf {
            db: &db,
            buf: Some(db.take_buf()),
        };
        let buf = pb.get();
        let log = |buf: &mut Vec<u8>| -> Result<(), Error> {
            let r = db.wal.log(buf).map_err(Error::other);
            buf.clear();
            r
        };

        if !self.pending_series.is_empty() {
            encoder.series(&self.pending_series, buf);
            log(buf)?;
        }
        if !self.pending_metadata.is_empty() {
            encoder.metadata(&self.pending_metadata, buf);
            log(buf)?;
        }
        if !self.pending_samples.is_empty() {
            encoder.samples(&self.pending_samples, buf);
            log(buf)?;
        }
        if !self.pending_histograms.is_empty() {
            let custom = encoder.histogram_samples(&self.pending_histograms, buf);
            if !buf.is_empty() {
                log(buf)?;
            }
            if !custom.is_empty() {
                buf.clear();
                encoder.custom_buckets_histogram_samples(&custom, buf);
                log(buf)?;
            }
        }
        if !self.pending_float_histograms.is_empty() {
            let custom = encoder.float_histogram_samples(&self.pending_float_histograms, buf);
            if !buf.is_empty() {
                log(buf)?;
            }
            if !custom.is_empty() {
                buf.clear();
                encoder.custom_buckets_float_histogram_samples(&custom, buf);
                log(buf)?;
            }
        }
        if !self.pending_exemplars.is_empty() {
            encoder.exemplars(&self.pending_exemplars, buf);
            log(buf)?;
        }
        drop(pb);

        for (s, series) in self.pending_samples.iter().zip(&self.sample_series) {
            if !series.update_timestamp(s.t) {
                db.metrics.total_out_of_order_samples.inc();
            }
        }
        for (s, series) in self.pending_histograms.iter().zip(&self.histogram_series) {
            if !series.update_timestamp(s.t) {
                db.metrics.total_out_of_order_samples.inc();
            }
        }
        for (s, series) in self.pending_float_histograms.iter().zip(&self.float_histogram_series) {
            if !series.update_timestamp(s.t) {
                db.metrics.total_out_of_order_samples.inc();
            }
        }
        for (m, series) in self.pending_metadata.iter().zip(&self.metadata_series) {
            let h = Handle::make(Metadata {
                typ: record::to_metric_type(m.r#type),
                unit: m.unit.clone(),
                help: m.help.clone(),
            });
            series.lock().meta = h;
        }
        Ok(())
    }

    /// Go `clearData`: clears all pending data.
    pub fn clear_data(&mut self) {
        self.pending_series.clear();
        self.pending_samples.clear();
        self.pending_histograms.clear();
        self.pending_float_histograms.clear();
        self.pending_exemplars.clear();
        self.pending_metadata.clear();
        self.sample_series.clear();
        self.histogram_series.clear();
        self.float_histogram_series.clear();
        self.metadata_series.clear();
    }

    /// Go `rollback`. Series are created in memory regardless of rollback, so they must be logged to the WAL;
    /// otherwise later commits may reference a series that was never written.
    pub fn rollback(&mut self) -> Result<(), Error> {
        let r = self.log_series();
        self.clear_data();
        r
    }

    /// Go `logSeries`: logs only the pending series records to the WAL.
    pub fn log_series(&mut self) -> Result<(), Error> {
        let db = self.db.clone();
        let _g = db.mtx.read().unwrap_or_else(|e| e.into_inner());
        if !self.pending_series.is_empty() {
            let mut pb = PooledBuf {
                db: &db,
                buf: Some(db.take_buf()),
            };
            let buf = pb.get();
            let encoder = Encoder {
                enable_st_storage: db.opts.enable_st_storage,
            };
            encoder.series(&self.pending_series, buf);
            db.wal.log(buf).map_err(Error::other)?;
        }
        Ok(())
    }

    /// Go `minValidTime`: the minimum timestamp a sample can have, without underflow.
    pub fn min_valid_time(&self, last_ts: i64) -> i64 {
        let w = self.db.opts.out_of_order_time_window;
        if last_ts < i64::MIN + w {
            return i64::MIN;
        }
        last_ts - w
    }
}

/// Go `appender`: the V1 appender of the agent.
pub struct AppenderV1 {
    pub base: AppenderBase,
    hints: Option<AppendOptions>,
}

impl AppenderV1 {
    /// The append options set with `set_options` (unused by the agent, as upstream).
    pub fn hints(&self) -> Option<&AppendOptions> {
        self.hints.as_ref()
    }
}

impl AppenderTransaction for AppenderV1 {
    fn commit(&mut self) -> Option<Error> {
        self.base.commit().err()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.base.rollback().err()
    }
}

impl Appender for AppenderV1 {
    fn append(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        let series = self.base.get_or_create(r, l)?;
        let st = series.lock();
        if t <= self.base.min_valid_time(st.last_ts) {
            self.base.db.metrics.total_out_of_order_samples.inc();
            return Err(ERR_OUT_OF_ORDER_SAMPLE);
        }
        drop(st);
        self.base.pending_samples.push(RefSample {
            r#ref: series.r#ref,
            st: 0,
            t,
            v,
        });
        let r = series.r#ref;
        self.base.sample_series.push(series);
        self.base.db.metrics.total_appended_samples.float.inc();
        Ok(r)
    }

    fn set_options(&mut self, opts: Option<&AppendOptions>) {
        self.hints = opts.copied();
    }

    fn append_exemplar(&mut self, r: SeriesRef, _l: &Labels, e: &Exemplar) -> Result<SeriesRef, Error> {
        // Series references and chunk references are identical for agent mode.
        let Some(s) = self.base.db.series.get_by_id(r) else {
            return Err(Error::msg(format!("unknown series ref when trying to add exemplar: {r}")));
        };

        // Ensure no empty labels have gotten through.
        let mut e = e.clone();
        e.labels = e.labels.without_empty();

        if let Err(err) = self.base.validate_exemplar(s.r#ref, &e) {
            if err.is(&ERR_DUPLICATE_EXEMPLAR) {
                // Duplicate: don't return an error but don't accept the exemplar.
                return Ok(0);
            }
            return Err(err);
        }

        self.base.db.series.set_latest_exemplar(s.r#ref, &e);
        self.base.pending_exemplars.push(RefExemplar {
            r#ref: s.r#ref,
            t: e.ts,
            v: e.value,
            labels: e.labels,
        });
        self.base.db.metrics.total_appended_exemplars.inc();
        Ok(s.r#ref)
    }

    fn append_histogram(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        if let Some(h) = h {
            h.validate()?;
        }
        if let Some(fh) = fh {
            fh.validate()?;
        }
        let series = self.base.get_or_create(r, l)?;
        let st = series.lock();
        if t <= self.base.min_valid_time(st.last_ts) {
            self.base.db.metrics.total_out_of_order_samples.inc();
            return Err(ERR_OUT_OF_ORDER_SAMPLE);
        }
        drop(st);
        let sref = series.r#ref;
        if let Some(h) = h {
            self.base.pending_histograms.push(RefHistogramSample {
                r#ref: sref,
                st: 0,
                t,
                h: Arc::new(h.clone()),
            });
            self.base.histogram_series.push(series);
        } else if let Some(fh) = fh {
            self.base.pending_float_histograms.push(RefFloatHistogramSample {
                r#ref: sref,
                st: 0,
                t,
                fh: Arc::new(fh.clone()),
            });
            self.base.float_histogram_series.push(series);
        }
        self.base.db.metrics.total_appended_samples.histogram.inc();
        Ok(sref)
    }

    fn append_histogram_st_zero_sample(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        st: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        if let Some(h) = h {
            h.validate()?;
        }
        if let Some(fh) = fh {
            fh.validate()?;
        }
        if st >= t {
            return Err(ERR_ST_NEWER_THAN_SAMPLE);
        }
        let series = self.base.get_or_create(r, l)?;
        let mut state = series.lock();
        if st <= self.base.min_valid_time(state.last_ts) {
            return Err(ERR_OUT_OF_ORDER_ST);
        }
        if st <= state.last_ts {
            // Discard the sample if it's out of order.
            return Err(ERR_OUT_OF_ORDER_ST);
        }
        // NOTE (upstream): this "commits" the pending sample timestamp as the last timestamp. It is a known
        // bug, kept until the V1 appender is removed.
        state.last_ts = st;
        drop(state);

        let sref = series.r#ref;
        if h.is_some() {
            self.base.pending_histograms.push(RefHistogramSample {
                r#ref: sref,
                st: 0,
                t: st,
                h: Arc::new(Histogram::default()),
            });
            self.base.histogram_series.push(series);
        } else if fh.is_some() {
            self.base.pending_float_histograms.push(RefFloatHistogramSample {
                r#ref: sref,
                st: 0,
                t: st,
                fh: Arc::new(FloatHistogram::default()),
            });
            self.base.float_histogram_series.push(series);
        }
        self.base.db.metrics.total_appended_samples.histogram.inc();
        Ok(sref)
    }

    fn update_metadata(&mut self, r: SeriesRef, l: &Labels, meta: &Metadata) -> Result<SeriesRef, Error> {
        if !self.base.db.opts.enable_metadata_wal_records || meta.is_empty() {
            return Ok(0);
        }
        let mut series = None;
        if r != 0 {
            series = self.base.db.series.get_by_id(r);
        }
        if series.is_none() {
            series = self.base.db.series.get_by_hash(l.hash(), l);
        }
        let Some(series) = series else {
            return Err(Error::msg(format!(
                "unknown series when trying to add metadata with HeadSeriesRef: {r} and labels: {l}"
            )));
        };

        let has_new_metadata = {
            let st = series.lock();
            metadata::is_zero_handle(&st.meta) || !st.meta.value().equals(meta)
        };
        let sref = series.r#ref;
        if has_new_metadata {
            self.base.pending_metadata.push(RefMetadata {
                r#ref: sref,
                r#type: record::get_metric_type(meta.typ),
                unit: meta.unit.clone(),
                help: meta.help.clone(),
            });
            self.base.metadata_series.push(series);
        }
        Ok(sref)
    }

    fn append_st_zero_sample(&mut self, r: SeriesRef, l: &Labels, t: i64, st: i64) -> Result<SeriesRef, Error> {
        if st >= t {
            return Err(ERR_ST_NEWER_THAN_SAMPLE);
        }
        let series = self.base.get_or_create(r, l)?;
        let mut state = series.lock();
        if t <= self.base.min_valid_time(state.last_ts) {
            self.base.db.metrics.total_out_of_order_samples.inc();
            return Err(ERR_OUT_OF_ORDER_SAMPLE);
        }
        if st <= state.last_ts {
            // Discard the sample if it's out of order.
            return Err(ERR_OUT_OF_ORDER_ST);
        }
        // NOTE (upstream): known bug, see append_histogram_st_zero_sample.
        state.last_ts = st;
        drop(state);

        let sref = series.r#ref;
        self.base.pending_samples.push(RefSample {
            r#ref: sref,
            st: 0,
            t: st,
            v: 0.0,
        });
        self.base.sample_series.push(series);
        self.base.db.metrics.total_appended_samples.float.inc();
        Ok(sref)
    }
}
