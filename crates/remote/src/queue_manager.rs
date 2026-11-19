//! Port of `storage/remote/queue_manager.go`: the remote write queue manager. It reads series, samples,
//! exemplars, histograms and metadata from the WAL watcher, relabels series with `write_relabel_configs`,
//! converts NHCB to classic histograms when asked, and sends batches through shards (one thread each) with
//! backoff, resharding by the same formulas as upstream.
//!
//! Go runs shards as goroutines that select on channels and timers. Here each shard is one thread that waits on
//! the condition variable of its queue (with the batch send deadline as timeout); there are no idle wake-ups
//! while a queue is empty. Requests are encoded straight from the queued items into a reused buffer (the bytes
//! are those of gogo `Marshal` for 1.0 and `OptimizedMarshal` for 2.0), then Snappy-compressed into a reused
//! output buffer, so the steady state allocates nothing per sample.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, RwLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use config::{MetadataConfig, QueueConfig, WRITE_V1_MESSAGE_TYPE, WRITE_V2_MESSAGE_TYPE};
use discovery::log::Logger;
use discovery::metrics::{Collector, CounterVec, GaugeVec, HistogramVec, MetricVec, Observer, Opts, Registry, Value};
use model::histogram::{CUSTOM_BUCKETS_SCHEMA, FloatHistogram, Histogram, HistogramRef, Span, classic_bucket_counts};
use model::labels::{Builder, Label, Labels, format_open_metrics_float};
use model::metadata::{Handle, Metadata, MetricType, is_zero_handle};
use model::relabel;
use model::value::{is_stale_nan, stale_nan};
use scrape::target::MetricMetadata;
use storage::{CancelToken, Context, ERR_CANCELED, Error};
use tsdb_format::chunks::HeadSeriesRef;
use tsdb_format::record::{
    BuffersPool, RefExemplar, RefFloatHistogramSample, RefHistogramSample, RefMetadata, RefSample, RefSeries,
    to_metric_type,
};
use tsdb_format::wlog::{LiveReaderMetrics, Watcher, WatcherMetrics, WriteTo};

use crate::client::{WriteClient, as_recoverable};
use crate::ewma::EwmaRate;
use crate::intern::Pool;
use crate::max_timestamp::MaxTimestamp;
use crate::metadata_watcher::{MetadataAppender, MetadataWatcher, ReadyScrapeManager};
use crate::prompb::proto::{
    double_field_len, int_field_len, message_len, packed_doubles_len, packed_sint64_len, packed_uint32_len, put_bytes_always,
    put_double_always, put_double_field, put_int_field, put_message_header, put_packed_doubles, put_packed_sint64,
    put_packed_uint32, put_sint32_field, put_uint_always, sint32_field_len, sov, tag_len,
};
use crate::prompb::types::{BucketSpan, Message, label_len, put_label};
use crate::prompb::{self, writev2};
use crate::stats::WriteResponseStats;

/// Go `namespace`.
pub const NAMESPACE: &str = "prometheus";
/// Go `subsystem`.
pub const SUBSYSTEM: &str = "remote_storage";
/// Go `remoteName`: the label of the queue name.
pub const REMOTE_NAME: &str = "remote_name";
/// Go `endpoint`: the label of the queue URL.
pub const ENDPOINT: &str = "url";

/// We track samples in/out and how long pushes take using an Exponentially Weighted Moving Average.
pub const EWMA_WEIGHT: f64 = 0.2;
pub const SHARD_UPDATE_DURATION: Duration = Duration::from_secs(10);
/// Allow 30% too many shards before scaling down.
pub const SHARD_TOLERANCE_FRACTION: f64 = 0.3;

pub const REASON_TOO_OLD: &str = "too_old";
pub const REASON_DROPPED_SERIES: &str = "dropped_series";
pub const REASON_UNINTENTIONAL_DROPPED_SERIES: &str = "unintentionally_dropped_series";
pub const REASON_NHCB_NOT_SUPPORTED: &str = "nhcb_in_rw1_not_supported";
pub const REASON_NHCB_CONVERSION_ERROR: &str = "nhcb_to_classic_conversion_error";
const REASONS: [&str; 5] = [
    REASON_TOO_OLD,
    REASON_DROPPED_SERIES,
    REASON_UNINTENTIONAL_DROPPED_SERIES,
    REASON_NHCB_NOT_SUPPORTED,
    REASON_NHCB_CONVERSION_ERROR,
];

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Nanoseconds since the Unix epoch.
fn ns_since_epoch(t: SystemTime) -> i128 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i128,
        Err(e) => -(e.duration().as_nanos() as i128),
    }
}

fn now_ns() -> i128 {
    ns_since_epoch(SystemTime::now())
}

/// Go `time.Now().Unix()`.
fn unix_now() -> i64 {
    now_ns().div_euclid(1_000_000_000) as i64
}

fn sleep_ns(ns: i64) {
    if ns > 0 {
        std::thread::sleep(Duration::from_nanos(ns as u64));
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Signals and contexts.

/// A one-shot event that threads can wait for (Go: a channel that is closed once).
#[derive(Debug, Default)]
pub struct Signal {
    flag: AtomicBool,
    m: Mutex<()>,
    c: Condvar,
}

impl Signal {
    pub fn new() -> Signal {
        Signal::default()
    }
    pub fn set(&self) {
        let _g = lock(&self.m);
        self.flag.store(true, Ordering::SeqCst);
        self.c.notify_all();
    }
    #[inline]
    pub fn is_set(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }
    /// Waits until set or `d` passed; true when set.
    pub fn wait_timeout(&self, d: Duration) -> bool {
        let deadline = Instant::now() + d;
        let mut g = lock(&self.m);
        loop {
            if self.is_set() {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            g = self.c.wait_timeout(g, deadline - now).unwrap_or_else(|e| e.into_inner()).0;
        }
    }
    pub fn wait(&self) {
        let mut g = lock(&self.m);
        while !self.is_set() {
            g = self.c.wait(g).unwrap_or_else(|e| e.into_inner());
        }
    }
}

/// A cancellable context (Go `context.Context` with `context.WithCancel`): a `storage::Context` for the
/// write client (which polls it) plus a signal that sleepers wait on, so a cancel wakes them at once.
#[derive(Clone, Debug)]
pub struct Ctx {
    pub ctx: Context,
    token: Option<CancelToken>,
    sig: Arc<Signal>,
}

impl Ctx {
    /// Go `context.Background()`.
    pub fn background() -> Ctx {
        Ctx { ctx: Context::background(), token: None, sig: Arc::new(Signal::new()) }
    }
    /// Go `context.WithCancel(context.Background())`.
    pub fn with_cancel() -> Ctx {
        let (ctx, token) = Context::background().with_cancel();
        Ctx { ctx, token: Some(token), sig: Arc::new(Signal::new()) }
    }
    pub fn cancel(&self) {
        if let Some(t) = &self.token {
            t.cancel();
        }
        self.sig.set();
    }
    pub fn is_done(&self) -> bool {
        self.sig.is_set() || self.ctx.is_done()
    }
    /// Go `ctx.Err()`.
    pub fn err(&self) -> Option<Error> {
        match self.ctx.err() {
            Some(e) => Some(e),
            None if self.sig.is_set() => Some(ERR_CANCELED),
            None => None,
        }
    }
    /// Sleeps `d` or until cancelled; true when cancelled.
    pub fn sleep(&self, d: Duration) -> bool {
        if self.token.is_none() {
            std::thread::sleep(d);
            return false;
        }
        self.sig.wait_timeout(d) || self.ctx.is_done()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Metrics.

/// Go `remoteapi.WriteMessageType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ProtoMsg {
    #[default]
    V1,
    V2,
}

impl ProtoMsg {
    /// From the config value (`prometheus.WriteRequest` or `io.prometheus.write.v2.Request`; empty is 1.0).
    pub fn from_config(s: &str) -> ProtoMsg {
        if s == WRITE_V2_MESSAGE_TYPE { ProtoMsg::V2 } else { ProtoMsg::V1 }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            ProtoMsg::V1 => WRITE_V1_MESSAGE_TYPE,
            ProtoMsg::V2 => WRITE_V2_MESSAGE_TYPE,
        }
    }
}

impl std::fmt::Display for ProtoMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Go `prometheus.DefBuckets` followed by 25, 60, 120, 300.
const SENT_BATCH_DURATION_BUCKETS: [f64; 15] =
    [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 60.0, 120.0, 300.0];

/// Go `queueManagerMetrics`.
pub struct QueueManagerMetrics {
    reg: Option<Arc<Registry>>,

    pub samples_total: Arc<CounterVec>,
    pub exemplars_total: Arc<CounterVec>,
    pub histograms_total: Arc<CounterVec>,
    pub metadata_total: Arc<CounterVec>,
    pub failed_samples_total: Arc<CounterVec>,
    pub failed_exemplars_total: Arc<CounterVec>,
    pub failed_histograms_total: Arc<CounterVec>,
    pub failed_metadata_total: Arc<CounterVec>,
    pub retried_samples_total: Arc<CounterVec>,
    pub retried_exemplars_total: Arc<CounterVec>,
    pub retried_histograms_total: Arc<CounterVec>,
    pub retried_metadata_total: Arc<CounterVec>,
    pub dropped_samples_total: Arc<CounterVec>,
    pub dropped_exemplars_total: Arc<CounterVec>,
    pub dropped_histograms_total: Arc<CounterVec>,
    pub enqueue_retries_total: Arc<CounterVec>,
    pub sent_batch_duration: Arc<HistogramVec>,
    pub highest_timestamp: Arc<MaxTimestamp>,
    pub highest_sent_timestamp: Arc<MaxTimestamp>,
    pub pending_samples: Arc<GaugeVec>,
    pub pending_exemplars: Arc<GaugeVec>,
    pub pending_histograms: Arc<GaugeVec>,
    pub shard_capacity: Arc<GaugeVec>,
    pub num_shards: Arc<GaugeVec>,
    pub max_num_shards: Arc<GaugeVec>,
    pub min_num_shards: Arc<GaugeVec>,
    pub desired_num_shards: Arc<GaugeVec>,
    pub sent_bytes_total: Arc<CounterVec>,
    pub metadata_bytes_total: Arc<CounterVec>,
    pub max_samples_per_send: Arc<GaugeVec>,

    // The single series of the metrics without variable labels, for the hot paths.
    c_samples: Arc<Value>,
    c_exemplars: Arc<Value>,
    c_histograms: Arc<Value>,
    c_retried_samples: Arc<Value>,
    c_retried_exemplars: Arc<Value>,
    c_retried_histograms: Arc<Value>,
    c_enqueue_retries: Arc<Value>,
    c_pending_samples: Arc<Value>,
    c_pending_exemplars: Arc<Value>,
    c_pending_histograms: Arc<Value>,
    c_sent_batch_duration: Arc<Observer>,
    // Children of the `reason` vectors, made on first use as Go's `WithLabelValues` does.
    dropped: [[OnceLock<Arc<Value>>; 5]; 3],
}

/// Which `*_dropped_total` vector.
#[derive(Clone, Copy)]
enum Dropped {
    Samples = 0,
    Exemplars = 1,
    Histograms = 2,
}

impl QueueManagerMetrics {
    /// Go `newQueueManagerMetrics`.
    pub fn new(r: Option<Arc<Registry>>, rn: &str, e: &str) -> Arc<QueueManagerMetrics> {
        let opts = |name: &str, help: &str| {
            Opts::new(&format!("{NAMESPACE}_{SUBSYSTEM}_{name}"), help).const_label(REMOTE_NAME, rn).const_label(ENDPOINT, e)
        };
        let counter = |name: &str, help: &str| MetricVec::new_counter(opts(name, help));
        let gauge = |name: &str, help: &str| MetricVec::new_gauge(opts(name, help));
        let reason_counter = |name: &str, help: &str| MetricVec::new_counter_vec(opts(name, help), &["reason"]);

        let samples_total = counter("samples_total", "Total number of samples sent to remote storage.");
        let exemplars_total = counter("exemplars_total", "Total number of exemplars sent to remote storage.");
        let histograms_total = counter("histograms_total", "Total number of histograms sent to remote storage.");
        let metadata_total = counter("metadata_total", "Total number of metadata entries sent to remote storage. Deprecated: This metric only applies to Remote Write 1.0.");
        let failed_samples_total = counter("samples_failed_total", "Total number of samples which failed on send to remote storage, non-recoverable errors.");
        let failed_exemplars_total = counter("exemplars_failed_total", "Total number of exemplars which failed on send to remote storage, non-recoverable errors.");
        let failed_histograms_total = counter("histograms_failed_total", "Total number of histograms which failed on send to remote storage, non-recoverable errors.");
        let failed_metadata_total = counter("metadata_failed_total", "Total number of metadata entries which failed on send to remote storage, non-recoverable errors. Deprecated: This metric only applies to Remote Write 1.0.");
        let retried_samples_total = counter("samples_retried_total", "Total number of samples which failed on send to remote storage but were retried because the send error was recoverable.");
        let retried_exemplars_total = counter("exemplars_retried_total", "Total number of exemplars which failed on send to remote storage but were retried because the send error was recoverable.");
        let retried_histograms_total = counter("histograms_retried_total", "Total number of histograms which failed on send to remote storage but were retried because the send error was recoverable.");
        let retried_metadata_total = counter("metadata_retried_total", "Total number of metadata entries which failed on send to remote storage but were retried because the send error was recoverable. Deprecated: This metric only applies to Remote Write 1.0.");
        let dropped_samples_total = reason_counter("samples_dropped_total", "Total number of samples which were dropped after being read from the WAL before being sent via remote write, either via relabelling, due to being too old or unintentionally because of an unknown reference ID.");
        let dropped_exemplars_total = reason_counter("exemplars_dropped_total", "Total number of exemplars which were dropped after being read from the WAL before being sent via remote write, either via relabelling, due to being too old or unintentionally because of an unknown reference ID.");
        let dropped_histograms_total = reason_counter("histograms_dropped_total", "Total number of histograms which were dropped after being read from the WAL before being sent via remote write, either via relabelling, due to being too old or unintentionally because of an unknown reference ID.");
        let enqueue_retries_total = counter("enqueue_retries_total", "Total number of times enqueue has failed because a shards queue was full.");
        // TODO(metrics): Go also exposes this as a native histogram (factor 1.1, 100 buckets, 1h reset).
        let sent_batch_duration = MetricVec::new_histogram_vec(
            opts("sent_batch_duration_seconds", "Duration of send calls to the remote storage."),
            &[],
            &SENT_BATCH_DURATION_BUCKETS,
        );
        let c_sent_batch_duration = sent_batch_duration.get();
        let highest_timestamp = Arc::new(MaxTimestamp::new(opts("queue_highest_timestamp_seconds", "Highest timestamp that was enqueued, in seconds since epoch. Initialized to 0 when no data has been received yet.")));
        let highest_sent_timestamp = Arc::new(MaxTimestamp::new(opts("queue_highest_sent_timestamp_seconds", "Highest timestamp successfully sent by this queue, in seconds since epoch. Initialized to 0 when no data has been sent yet.")));
        let pending_samples = gauge("samples_pending", "The number of samples pending in the queues shards to be sent to the remote storage.");
        let pending_exemplars = gauge("exemplars_pending", "The number of exemplars pending in the queues shards to be sent to the remote storage.");
        let pending_histograms = gauge("histograms_pending", "The number of histograms pending in the queues shards to be sent to the remote storage.");
        let shard_capacity = gauge("shard_capacity", "The capacity of each shard of the queue used for parallel sending to the remote storage.");
        let num_shards = gauge("shards", "The number of shards used for parallel sending to the remote storage.");
        let max_num_shards = gauge("shards_max", "The maximum number of shards that the queue is allowed to run.");
        let min_num_shards = gauge("shards_min", "The minimum number of shards that the queue is allowed to run.");
        let desired_num_shards = gauge("shards_desired", "The number of shards that the queues shard calculation wants to run based on the rate of samples in vs. samples out.");
        let sent_bytes_total = counter("bytes_total", "The total number of bytes of data sent by the queue after compression. For Remote Write 2.0, this includes metadata. Note that when exemplars over remote write is enabled the exemplars included in a remote write request count towards this metric.");
        let metadata_bytes_total = counter("metadata_bytes_total", "The total number of bytes of metadata sent by the queue after compression. Deprecated: This metric only applies to Remote Write 1.0.");
        let max_samples_per_send = gauge("max_samples_per_send", "The maximum number of samples to be sent, in a single request, to the remote storage. Note that, when sending of exemplars over remote write is enabled, exemplars count towards this limit.");

        Arc::new(QueueManagerMetrics {
            reg: r,
            c_samples: samples_total.get(),
            c_exemplars: exemplars_total.get(),
            c_histograms: histograms_total.get(),
            c_retried_samples: retried_samples_total.get(),
            c_retried_exemplars: retried_exemplars_total.get(),
            c_retried_histograms: retried_histograms_total.get(),
            c_enqueue_retries: enqueue_retries_total.get(),
            c_pending_samples: pending_samples.get(),
            c_pending_exemplars: pending_exemplars.get(),
            c_pending_histograms: pending_histograms.get(),
            c_sent_batch_duration,
            dropped: Default::default(),
            samples_total,
            exemplars_total,
            histograms_total,
            metadata_total,
            failed_samples_total,
            failed_exemplars_total,
            failed_histograms_total,
            failed_metadata_total,
            retried_samples_total,
            retried_exemplars_total,
            retried_histograms_total,
            retried_metadata_total,
            dropped_samples_total,
            dropped_exemplars_total,
            dropped_histograms_total,
            enqueue_retries_total,
            sent_batch_duration,
            highest_timestamp,
            highest_sent_timestamp,
            pending_samples,
            pending_exemplars,
            pending_histograms,
            shard_capacity,
            num_shards,
            max_num_shards,
            min_num_shards,
            desired_num_shards,
            sent_bytes_total,
            metadata_bytes_total,
            max_samples_per_send,
        })
    }

    fn dropped_child(&self, which: Dropped, reason: &'static str) -> &Arc<Value> {
        let i = REASONS.iter().position(|r| *r == reason).expect("known reason");
        self.dropped[which as usize][i].get_or_init(|| match which {
            Dropped::Samples => self.dropped_samples_total.with_label_values(&[reason]),
            Dropped::Exemplars => self.dropped_exemplars_total.with_label_values(&[reason]),
            Dropped::Histograms => self.dropped_histograms_total.with_label_values(&[reason]),
        })
    }

    fn collectors(&self) -> Vec<Arc<dyn Collector>> {
        vec![
            self.samples_total.clone(),
            self.exemplars_total.clone(),
            self.histograms_total.clone(),
            self.failed_samples_total.clone(),
            self.failed_exemplars_total.clone(),
            self.failed_histograms_total.clone(),
            self.retried_samples_total.clone(),
            self.retried_exemplars_total.clone(),
            self.retried_histograms_total.clone(),
            self.dropped_samples_total.clone(),
            self.dropped_exemplars_total.clone(),
            self.dropped_histograms_total.clone(),
            self.enqueue_retries_total.clone(),
            self.sent_batch_duration.clone(),
            self.highest_timestamp.collector(),
            self.highest_sent_timestamp.collector(),
            self.pending_samples.clone(),
            self.pending_exemplars.clone(),
            self.pending_histograms.clone(),
            self.shard_capacity.clone(),
            self.num_shards.clone(),
            self.max_num_shards.clone(),
            self.min_num_shards.clone(),
            self.desired_num_shards.clone(),
            self.sent_bytes_total.clone(),
            self.max_samples_per_send.clone(),
        ]
    }

    fn v1_collectors(&self) -> Vec<Arc<dyn Collector>> {
        vec![
            self.metadata_total.clone(),
            self.failed_metadata_total.clone(),
            self.retried_metadata_total.clone(),
            self.metadata_bytes_total.clone(),
        ]
    }

    /// Go `register` (`MustRegister`: panics on a duplicate registration).
    pub fn register(&self, proto_msg: ProtoMsg) {
        let Some(reg) = &self.reg else { return };
        let mut cs = self.collectors();
        if proto_msg == ProtoMsg::V1 {
            cs.extend(self.v1_collectors());
        }
        for c in cs {
            if let Err(e) = reg.register(c) {
                panic!("{e}");
            }
        }
    }

    /// Go `unregister`.
    pub fn unregister(&self, proto_msg: ProtoMsg) {
        let Some(reg) = &self.reg else { return };
        let mut cs = self.collectors();
        if proto_msg == ProtoMsg::V1 {
            cs.extend(self.v1_collectors());
        }
        for c in cs {
            reg.unregister(&c);
        }
    }

    fn record_batch_attempt(&self, sc: &SendBatchContext) {
        self.c_samples.add(sc.sample_count as f64);
        self.c_exemplars.add(sc.exemplar_count as f64);
        self.c_histograms.add(sc.histogram_count as f64);
    }

    fn record_latency(&self, begin: Instant) {
        self.c_sent_batch_duration.observe(begin.elapsed().as_secs_f64());
    }

    fn record_retry(&self, sc: &SendBatchContext) {
        self.c_retried_samples.add(sc.sample_count as f64);
        self.c_retried_exemplars.add(sc.exemplar_count as f64);
        self.c_retried_histograms.add(sc.histogram_count as f64);
    }
}

/// Go `sendBatchContext`.
#[derive(Clone, Copy, Debug, Default)]
struct SendBatchContext {
    sample_count: usize,
    exemplar_count: usize,
    histogram_count: usize,
}

// ---------------------------------------------------------------------------------------------------------------
// Queued items.

/// Go `seriesType`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SeriesType {
    #[default]
    Sample,
    Exemplar,
    Histogram,
    FloatHistogram,
    Metadata,
}

/// Go `timeSeries`: one queued sample, exemplar or histogram with its (relabeled) series labels.
#[derive(Clone, Debug, Default)]
pub struct TimeSeries {
    pub series_labels: Labels,
    pub value: f64,
    pub histogram: Option<Arc<Histogram>>,
    pub float_histogram: Option<Arc<FloatHistogram>>,
    pub metadata: Handle,
    pub start_timestamp: i64,
    pub timestamp: i64,
    pub exemplar_labels: Labels,
    /// The type of series: sample, exemplar, or histogram.
    pub s_type: SeriesType,
}

/// The first sample, exemplar and histogram timestamps of a series, for `build_time_series` and the age filter
/// (Go reads `ts.Samples[0]`, `ts.Exemplars[0]` and `ts.Histograms[0]`).
pub trait SeriesData {
    fn first_sample_ts(&self) -> Option<i64>;
    fn first_exemplar_ts(&self) -> Option<i64>;
    fn first_histogram_ts(&self) -> Option<i64>;
}

impl SeriesData for TimeSeries {
    fn first_sample_ts(&self) -> Option<i64> {
        (self.s_type == SeriesType::Sample).then_some(self.timestamp)
    }
    fn first_exemplar_ts(&self) -> Option<i64> {
        (self.s_type == SeriesType::Exemplar).then_some(self.timestamp)
    }
    fn first_histogram_ts(&self) -> Option<i64> {
        matches!(self.s_type, SeriesType::Histogram | SeriesType::FloatHistogram).then_some(self.timestamp)
    }
}

impl SeriesData for prompb::TimeSeries {
    fn first_sample_ts(&self) -> Option<i64> {
        self.samples.first().map(|s| s.timestamp)
    }
    fn first_exemplar_ts(&self) -> Option<i64> {
        self.exemplars.first().map(|e| e.timestamp)
    }
    fn first_histogram_ts(&self) -> Option<i64> {
        self.histograms.first().map(|h| h.timestamp)
    }
}

impl SeriesData for writev2::TimeSeries {
    fn first_sample_ts(&self) -> Option<i64> {
        self.samples.first().map(|s| s.timestamp)
    }
    fn first_exemplar_ts(&self) -> Option<i64> {
        self.exemplars.first().map(|e| e.timestamp)
    }
    fn first_histogram_ts(&self) -> Option<i64> {
        self.histograms.first().map(|h| h.timestamp)
    }
}

/// Go `isSampleOld`.
pub fn is_sample_old(base_time: SystemTime, sample_age_limit: Duration, ts: i64) -> bool {
    is_sample_old_ns(ns_since_epoch(base_time), sample_age_limit.as_nanos() as i64, ts)
}

fn is_sample_old_ns(base_ns: i128, sample_age_limit_ns: i64, ts: i64) -> bool {
    if sample_age_limit_ns == 0 {
        // If sampleAgeLimit is unset, then we never skip samples due to their age.
        return false;
    }
    let limit_ts = base_ns - sample_age_limit_ns as i128;
    (ts as i128) * 1_000_000 < limit_ts
}

/// Go `timeSeriesAgeChecker` with `isTimeSeriesOldFilter` / `isV2TimeSeriesOldFilter`: true when the series
/// is too old, counting the drop.
fn old_filter<T: SeriesData>(metrics: &QueueManagerMetrics, base_ns: i128, limit_ns: i64) -> impl FnMut(&T) -> bool + '_ {
    move |ts: &T| {
        let check = |t: i64, which: Dropped| {
            if limit_ns == 0 || !is_sample_old_ns(base_ns, limit_ns, t) {
                return false;
            }
            metrics.dropped_child(which, REASON_TOO_OLD).inc();
            true
        };
        // Only the first element should be set in the series, therefore we only check the first element.
        if let Some(t) = ts.first_sample_ts() {
            check(t, Dropped::Samples)
        } else if let Some(t) = ts.first_histogram_ts() {
            check(t, Dropped::Histograms)
        } else if let Some(t) = ts.first_exemplar_ts() {
            check(t, Dropped::Exemplars)
        } else {
            false
        }
    }
}

/// Go `timeSeriesStats`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeSeriesStats {
    pub highest: i64,
    pub lowest: i64,
    pub dropped_samples: usize,
    pub dropped_exemplars: usize,
    pub dropped_histograms: usize,
}

impl Default for TimeSeriesStats {
    /// Go `newTimeSeriesStats`: `lowest` starts at `MaxInt64`.
    fn default() -> Self {
        TimeSeriesStats { highest: 0, lowest: i64::MAX, dropped_samples: 0, dropped_exemplars: 0, dropped_histograms: 0 }
    }
}

impl TimeSeriesStats {
    fn update_timestamp(&mut self, t: i64) {
        if t > 0 {
            if t > self.highest {
                self.highest = t;
            }
            if t < self.lowest {
                self.lowest = t;
            }
        }
    }
    fn record_dropped<T: SeriesData>(&mut self, ts: &T) {
        if ts.first_sample_ts().is_some() {
            self.dropped_samples += 1;
        }
        if ts.first_exemplar_ts().is_some() {
            self.dropped_exemplars += 1;
        }
        if ts.first_histogram_ts().is_some() {
            self.dropped_histograms += 1;
        }
    }
    fn update<T: SeriesData>(&mut self, ts: &T) {
        // At the moment we only ever append a TimeSeries with a single sample or exemplar in it.
        if let Some(t) = ts.first_sample_ts() {
            self.update_timestamp(t);
        }
        if let Some(t) = ts.first_exemplar_ts() {
            self.update_timestamp(t);
        }
        if let Some(t) = ts.first_histogram_ts() {
            self.update_timestamp(t);
        }
    }
}

/// A series filter: true drops the series.
pub type Filter<'a, T> = Option<&'a mut dyn FnMut(&T) -> bool>;

/// Go `buildTimeSeries`: drops the series the filter selects (keeping the order) and returns the stats.
pub fn build_time_series<T: SeriesData>(time_series: &mut Vec<T>, mut filter: Filter<'_, T>) -> TimeSeriesStats {
    let mut stats = TimeSeriesStats::default();
    time_series.retain(|ts| {
        if let Some(f) = filter.as_mut()
            && f(ts)
        {
            stats.record_dropped(ts);
            return false;
        }
        stats.update(ts);
        true
    });
    stats
}

/// Go `buildV2TimeSeries`: moves the kept series to the front (swapping, so the dropped ones stay at the end
/// of the slices for later retries) and returns how many are kept. `pending` moves along with `batch`.
fn build_v2_time_series(batch: &mut [TimeSeries], pending: &mut [PendingV2], mut filter: Filter<'_, TimeSeries>) -> (usize, TimeSeriesStats) {
    let mut stats = TimeSeriesStats::default();
    let mut keep = 0;
    for i in 0..batch.len() {
        if let Some(f) = filter.as_mut()
            && f(&batch[i])
        {
            stats.record_dropped(&batch[i]);
            continue;
        }
        stats.update(&batch[i]);
        if i != keep {
            batch.swap(keep, i);
            pending.swap(keep, i);
        }
        keep += 1;
    }
    (keep, stats)
}

// ---------------------------------------------------------------------------------------------------------------
// Protobuf encoding straight from the queued items.

#[derive(Clone, Copy)]
enum HistData<'a> {
    Int(&'a Histogram),
    Float(&'a FloatHistogram),
}

impl<'a> HistData<'a> {
    fn of(d: &'a TimeSeries) -> Option<HistData<'a>> {
        match d.s_type {
            SeriesType::Histogram => d.histogram.as_deref().map(HistData::Int),
            SeriesType::FloatHistogram => d.float_histogram.as_deref().map(HistData::Float),
            _ => None,
        }
    }
    fn as_ref(self) -> HistogramRef<'a> {
        match self {
            HistData::Int(h) => HistogramRef::Int(h),
            HistData::Float(h) => HistogramRef::Float(h),
        }
    }
    fn custom_values(self) -> &'a [f64] {
        match self {
            HistData::Int(h) => h.custom_values_slice(),
            HistData::Float(h) => h.custom_values_slice(),
        }
    }
}

fn bucket_span(s: &Span) -> BucketSpan {
    BucketSpan { offset: s.offset, length: s.length }
}

fn spans_len(field: u32, spans: &[Span]) -> usize {
    spans.iter().map(|s| message_len(field, bucket_span(s).encoded_len())).sum()
}

fn put_spans(buf: &mut Vec<u8>, field: u32, spans: &[Span]) {
    for s in spans {
        let b = bucket_span(s);
        put_message_header(buf, field, b.encoded_len());
        b.encode_raw(buf);
    }
}

/// The body length of `prompb.Histogram` (`st`: the 2.0 start timestamp) made by Go `FromIntHistogram` /
/// `FromFloatHistogram`.
fn hist_len(h: HistData<'_>, ts: i64, st: Option<i64>) -> usize {
    let mut n = match h {
        HistData::Int(h) => {
            tag_len(1)
                + sov(h.count)
                + double_field_len(3, h.sum)
                + sint32_field_len(4, h.schema)
                + double_field_len(5, h.zero_threshold)
                + tag_len(6)
                + sov(h.zero_count)
                + spans_len(8, &h.negative_spans)
                + packed_sint64_len(9, &h.negative_buckets)
                + spans_len(11, &h.positive_spans)
                + packed_sint64_len(12, &h.positive_buckets)
                + int_field_len(14, h.counter_reset_hint as i32 as i64)
                + packed_doubles_len(16, h.custom_values_slice().len())
        }
        HistData::Float(h) => {
            tag_len(2)
                + 8
                + double_field_len(3, h.sum)
                + sint32_field_len(4, h.schema)
                + double_field_len(5, h.zero_threshold)
                + tag_len(7)
                + 8
                + spans_len(8, &h.negative_spans)
                + packed_doubles_len(10, h.negative_buckets.len())
                + spans_len(11, &h.positive_spans)
                + packed_doubles_len(13, h.positive_buckets.len())
                + int_field_len(14, h.counter_reset_hint as i32 as i64)
                + packed_doubles_len(16, h.custom_values_slice().len())
        }
    };
    n += int_field_len(15, ts);
    if let Some(st) = st {
        n += int_field_len(17, st);
    }
    n
}

fn put_hist(buf: &mut Vec<u8>, h: HistData<'_>, ts: i64, st: Option<i64>) {
    match h {
        HistData::Int(h) => {
            put_uint_always(buf, 1, h.count);
            put_double_field(buf, 3, h.sum);
            put_sint32_field(buf, 4, h.schema);
            put_double_field(buf, 5, h.zero_threshold);
            put_uint_always(buf, 6, h.zero_count);
            put_spans(buf, 8, &h.negative_spans);
            put_packed_sint64(buf, 9, &h.negative_buckets);
            put_spans(buf, 11, &h.positive_spans);
            put_packed_sint64(buf, 12, &h.positive_buckets);
            put_int_field(buf, 14, h.counter_reset_hint as i32 as i64);
            put_int_field(buf, 15, ts);
            put_packed_doubles(buf, 16, h.custom_values_slice());
        }
        HistData::Float(h) => {
            put_double_always(buf, 2, h.count);
            put_double_field(buf, 3, h.sum);
            put_sint32_field(buf, 4, h.schema);
            put_double_field(buf, 5, h.zero_threshold);
            put_double_always(buf, 7, h.zero_count);
            put_spans(buf, 8, &h.negative_spans);
            put_packed_doubles(buf, 10, &h.negative_buckets);
            put_spans(buf, 11, &h.positive_spans);
            put_packed_doubles(buf, 13, &h.positive_buckets);
            put_int_field(buf, 14, h.counter_reset_hint as i32 as i64);
            put_int_field(buf, 15, ts);
            put_packed_doubles(buf, 16, h.custom_values_slice());
        }
    }
    if let Some(st) = st {
        put_int_field(buf, 17, st);
    }
}

fn labels_msgs_len(field: u32, l: &Labels) -> usize {
    l.iter().map(|(n, v)| message_len(field, label_len(n, v))).sum()
}

fn put_labels(buf: &mut Vec<u8>, field: u32, l: &Labels) {
    for (n, v) in l.iter() {
        put_label(buf, field, n, v);
    }
}

/// Appends one remote write 1.0 `TimeSeries` (field 1 of `WriteRequest`), as Go `populateTimeSeries` and
/// gogo `Marshal` make it.
fn put_v1_series(buf: &mut Vec<u8>, d: &TimeSeries) {
    let inner = match d.s_type {
        SeriesType::Sample => message_len(2, prompb::Sample { value: d.value, timestamp: d.timestamp }.encoded_len()),
        SeriesType::Exemplar => message_len(3, v1_exemplar_len(d)),
        SeriesType::Histogram | SeriesType::FloatHistogram => match HistData::of(d) {
            Some(h) => message_len(4, hist_len(h, d.timestamp, None)),
            None => 0,
        },
        SeriesType::Metadata => 0,
    };
    put_message_header(buf, 1, labels_msgs_len(1, &d.series_labels) + inner);
    put_labels(buf, 1, &d.series_labels);
    match d.s_type {
        SeriesType::Sample => {
            let s = prompb::Sample { value: d.value, timestamp: d.timestamp };
            put_message_header(buf, 2, s.encoded_len());
            s.encode_raw(buf);
        }
        SeriesType::Exemplar => {
            put_message_header(buf, 3, v1_exemplar_len(d));
            put_labels(buf, 1, &d.exemplar_labels);
            put_double_field(buf, 2, d.value);
            put_int_field(buf, 3, d.timestamp);
        }
        SeriesType::Histogram | SeriesType::FloatHistogram => {
            if let Some(h) = HistData::of(d) {
                put_message_header(buf, 4, hist_len(h, d.timestamp, None));
                put_hist(buf, h, d.timestamp, None);
            }
        }
        SeriesType::Metadata => {}
    }
}

fn v1_exemplar_len(d: &TimeSeries) -> usize {
    labels_msgs_len(1, &d.exemplar_labels) + double_field_len(2, d.value) + int_field_len(3, d.timestamp)
}

/// Encodes a remote write 1.0 `WriteRequest` of `batch` into `buf` (cleared first).
pub fn encode_v1_request(buf: &mut Vec<u8>, batch: &[TimeSeries]) {
    buf.clear();
    for d in batch {
        put_v1_series(buf, d);
    }
}

/// Per-item remote write 2.0 state of a shard (Go keeps it in `pendingDataV2`): symbol references and metadata.
#[derive(Clone, Debug, Default)]
pub struct PendingV2 {
    pub labels_refs: Vec<u32>,
    pub exemplar_labels_refs: Vec<u32>,
    pub metadata: writev2::Metadata,
}

fn v2_exemplar_len(d: &TimeSeries, p: &PendingV2) -> usize {
    packed_uint32_len(1, &p.exemplar_labels_refs) + double_field_len(2, d.value) + int_field_len(3, d.timestamp)
}

fn v2_sample(d: &TimeSeries) -> writev2::Sample {
    writev2::Sample { value: d.value, timestamp: d.timestamp, start_timestamp: d.start_timestamp }
}

/// Appends one remote write 2.0 `TimeSeries` (field 5 of `Request`) in `OptimizedMarshal` order: label refs,
/// samples, exemplars, histograms, metadata.
fn put_v2_series(buf: &mut Vec<u8>, d: &TimeSeries, p: &PendingV2) {
    let body = match d.s_type {
        SeriesType::Sample => message_len(2, v2_sample(d).encoded_len()),
        SeriesType::Exemplar => message_len(4, v2_exemplar_len(d, p)),
        SeriesType::Histogram | SeriesType::FloatHistogram => match HistData::of(d) {
            Some(h) => message_len(3, hist_len(h, d.timestamp, Some(d.start_timestamp))),
            None => 0,
        },
        SeriesType::Metadata => 0,
    };
    let total = packed_uint32_len(1, &p.labels_refs) + body + message_len(5, p.metadata.encoded_len());
    put_message_header(buf, 5, total);
    put_packed_uint32(buf, 1, &p.labels_refs);
    match d.s_type {
        SeriesType::Sample => {
            let s = v2_sample(d);
            put_message_header(buf, 2, s.encoded_len());
            s.encode_raw(buf);
        }
        SeriesType::Exemplar => {
            put_message_header(buf, 4, v2_exemplar_len(d, p));
            put_packed_uint32(buf, 1, &p.exemplar_labels_refs);
            put_double_field(buf, 2, d.value);
            put_int_field(buf, 3, d.timestamp);
        }
        SeriesType::Histogram | SeriesType::FloatHistogram => {
            if let Some(h) = HistData::of(d) {
                put_message_header(buf, 3, hist_len(h, d.timestamp, Some(d.start_timestamp)));
                put_hist(buf, h, d.timestamp, Some(d.start_timestamp));
            }
        }
        SeriesType::Metadata => {}
    }
    put_message_header(buf, 5, p.metadata.encoded_len());
    p.metadata.encode_raw(buf);
}

/// Encodes a remote write 2.0 `Request` into `buf` (cleared first).
pub fn encode_v2_request(buf: &mut Vec<u8>, symbols: &SymbolTable, batch: &[TimeSeries], pending: &[PendingV2]) {
    buf.clear();
    for s in symbols.iter() {
        put_bytes_always(buf, 4, s.as_bytes());
    }
    for (d, p) in batch.iter().zip(pending) {
        put_v2_series(buf, d, p);
    }
}

/// Go `writev2.SymbolsTable`, without allocations in steady state: the strings live in one reused arena and
/// an open addressing table maps them to references. Reference 0 is the empty string.
#[derive(Debug, Clone)]
pub struct SymbolTable {
    arena: String,
    offs: Vec<(u32, u32)>,
    slots: Vec<u32>,
}

impl Default for SymbolTable {
    fn default() -> Self {
        SymbolTable::new()
    }
}

fn str_hash(s: &[u8]) -> u64 {
    const K: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut h: u64 = (s.len() as u64).wrapping_mul(K);
    let mut c = s.chunks_exact(8);
    for w in &mut c {
        h = (h ^ u64::from_le_bytes(w.try_into().unwrap())).wrapping_mul(K).rotate_left(31);
    }
    let mut last = [0u8; 8];
    let r = c.remainder();
    last[..r.len()].copy_from_slice(r);
    h = (h ^ u64::from_le_bytes(last)).wrapping_mul(K);
    h ^ (h >> 29)
}

impl SymbolTable {
    /// Go `NewSymbolTable`.
    pub fn new() -> SymbolTable {
        SymbolTable { arena: String::new(), offs: vec![(0, 0)], slots: vec![0; 64] }
    }

    fn get(&self, i: u32) -> &str {
        let (o, l) = self.offs[i as usize];
        &self.arena[o as usize..(o + l) as usize]
    }

    fn grow(&mut self) {
        let n = self.slots.len() * 2;
        self.slots.clear();
        self.slots.resize(n, 0);
        let mask = n - 1;
        for idx in 1..self.offs.len() as u32 {
            let mut i = str_hash(self.get(idx).as_bytes()) as usize & mask;
            while self.slots[i] != 0 {
                i = (i + 1) & mask;
            }
            self.slots[i] = idx;
        }
    }

    /// Adds (if new) a string and returns its reference.
    pub fn symbolize(&mut self, s: &str) -> u32 {
        if s.is_empty() {
            return 0;
        }
        if (self.offs.len() + 1) * 2 > self.slots.len() {
            self.grow();
        }
        let mask = self.slots.len() - 1;
        let mut i = str_hash(s.as_bytes()) as usize & mask;
        loop {
            let v = self.slots[i];
            if v == 0 {
                let idx = self.offs.len() as u32;
                let start = self.arena.len() as u32;
                self.arena.push_str(s);
                self.offs.push((start, s.len() as u32));
                self.slots[i] = idx;
                return idx;
            }
            if self.get(v) == s {
                return v;
            }
            i = (i + 1) & mask;
        }
    }

    /// Go `SymbolizeLabels`: clears `buf` and fills it with name/value references.
    pub fn symbolize_labels(&mut self, lbls: &Labels, buf: &mut Vec<u32>) {
        buf.clear();
        for (n, v) in lbls.iter() {
            let n = self.symbolize(n);
            buf.push(n);
            let v = self.symbolize(v);
            buf.push(v);
        }
    }

    /// The number of symbols (the empty string included).
    pub fn len(&self) -> usize {
        self.offs.len()
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    /// The symbol of a reference.
    pub fn symbol(&self, r: u32) -> &str {
        self.get(r)
    }

    /// Go `Symbols()`, in reference order.
    pub fn iter(&self) -> impl Iterator<Item = &str> + '_ {
        (0..self.offs.len() as u32).map(|i| self.get(i))
    }

    /// Go `Reset`: keeps only the empty symbol.
    pub fn reset(&mut self) {
        self.arena.clear();
        self.offs.truncate(1);
        self.slots.fill(0);
    }
}

/// Go `writev2.FromMetadataType` on a `model.MetricType` string.
fn metadata_type_from_str(t: &str) -> writev2::MetadataType {
    MetricType::parse(t).map(writev2::from_metadata_type).unwrap_or(writev2::MetadataType::UNSPECIFIED)
}

/// Go `populateV2TimeSeries`: fills `pending` (symbol references and metadata) for `batch` and returns the
/// counts of samples, exemplars, histograms and unexpected metadata items.
pub fn populate_v2_time_series(
    symbol_table: &mut SymbolTable,
    batch: &[TimeSeries],
    pending: &mut [PendingV2],
    _send_exemplars: bool,
    _send_native_histograms: bool,
    enable_type_and_unit_labels: bool,
) -> (usize, usize, usize, usize) {
    let (mut n_samples, mut n_exemplars, mut n_histograms, mut n_unexpected) = (0, 0, 0, 0);
    for (d, p) in batch.iter().zip(pending.iter_mut()) {
        if enable_type_and_unit_labels {
            // Go `schema.NewMetadataFromLabels`: an empty type is "unknown".
            p.metadata.r#type = metadata_type_from_str(d.series_labels.get(model::schema::METRIC_TYPE_LABEL));
            p.metadata.unit_ref = symbol_table.symbolize(d.series_labels.get(model::schema::METRIC_UNIT_LABEL));
            p.metadata.help_ref = 0; // Type and unit does not give us help.
            // Use Help from d.metadata if available.
            if !is_zero_handle(&d.metadata) {
                p.metadata.help_ref = symbol_table.symbolize(&d.metadata.value().help);
            }
        } else if !is_zero_handle(&d.metadata) {
            let meta = d.metadata.value();
            p.metadata.r#type = writev2::from_metadata_type(meta.typ);
            p.metadata.help_ref = symbol_table.symbolize(&meta.help);
            p.metadata.unit_ref = symbol_table.symbolize(&meta.unit);
        } else {
            // Safeguard against sending garbage in case of not having metadata for whatever reason.
            p.metadata.r#type = writev2::from_metadata_type(MetricType::Unknown);
            p.metadata.unit_ref = 0;
            p.metadata.help_ref = 0;
        }

        symbol_table.symbolize_labels(&d.series_labels, &mut p.labels_refs);
        match d.s_type {
            SeriesType::Sample => n_samples += 1,
            SeriesType::Exemplar => {
                symbol_table.symbolize_labels(&d.exemplar_labels, &mut p.exemplar_labels_refs);
                n_exemplars += 1;
            }
            SeriesType::Histogram | SeriesType::FloatHistogram => n_histograms += 1,
            SeriesType::Metadata => n_unexpected += 1,
        }
    }
    (n_samples, n_exemplars, n_histograms, n_unexpected)
}

/// Go `compression.Encode(Snappy, ...)`: Snappy block format into the reused `out`; returns the length.
fn snappy_encode(enc: &mut snap::raw::Encoder, input: &[u8], out: &mut Vec<u8>) -> Result<usize, Error> {
    let need = snap::raw::max_compress_len(input.len());
    if out.len() < need {
        out.resize(need, 0);
    }
    enc.compress(input, out).map_err(|e| Error::msg(e.to_string()))
}

/// Reused per-shard (or per-call) encode and compress buffers.
pub struct EncodeBuffers {
    pub pbuf: Vec<u8>,
    enc: snap::raw::Encoder,
    pub out: Vec<u8>,
}

impl Default for EncodeBuffers {
    fn default() -> Self {
        EncodeBuffers { pbuf: Vec::new(), enc: snap::raw::Encoder::new(), out: Vec::new() }
    }
}

/// Go `buildWriteRequest` for the queued items: filters, encodes and compresses `batch`. Returns the
/// compressed length (the request is `bufs.out[..len]`) and the highest and lowest timestamps.
pub fn build_write_request(
    logger: &Logger,
    batch: &mut Vec<TimeSeries>,
    filter: Filter<'_, TimeSeries>,
    bufs: &mut EncodeBuffers,
) -> (Result<usize, Error>, i64, i64) {
    let stats = build_time_series(batch, filter);
    log_dropped(logger, &stats);
    encode_v1_request(&mut bufs.pbuf, batch);
    let r = snappy_encode(&mut bufs.enc, &bufs.pbuf, &mut bufs.out);
    (r, stats.highest, stats.lowest)
}

/// Go `buildV2WriteRequest`: as `build_write_request`, for remote write 2.0. The kept items are moved to the
/// front of `batch` and `pending`.
pub fn build_v2_write_request(
    logger: &Logger,
    batch: &mut [TimeSeries],
    pending: &mut [PendingV2],
    symbols: &SymbolTable,
    filter: Filter<'_, TimeSeries>,
    bufs: &mut EncodeBuffers,
) -> (Result<usize, Error>, i64, i64) {
    let (keep, stats) = build_v2_time_series(batch, pending, filter);
    log_dropped(logger, &stats);
    encode_v2_request(&mut bufs.pbuf, symbols, &batch[..keep], &pending[..keep]);
    let r = snappy_encode(&mut bufs.enc, &bufs.pbuf, &mut bufs.out);
    (r, stats.highest, stats.lowest)
}

fn log_dropped(logger: &Logger, stats: &TimeSeriesStats) {
    if stats.dropped_samples > 0 || stats.dropped_exemplars > 0 || stats.dropped_histograms > 0 {
        logger.debug(
            "dropped data due to their age",
            &[
                ("droppedSamples", &stats.dropped_samples.to_string()),
                ("droppedExemplars", &stats.dropped_exemplars.to_string()),
                ("droppedHistograms", &stats.dropped_histograms.to_string()),
            ],
        );
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Queue.

struct QueueState {
    batch: Vec<TimeSeries>,
    /// Go `batchQueue`: full batches waiting for the shard (at most `batches`).
    full: VecDeque<Vec<TimeSeries>>,
    /// Go `batchPool`.
    pool: Vec<Vec<TimeSeries>>,
    /// Go: `batchQueue` closed.
    closed: bool,
    /// The shard waits for data without a deadline (its partial batch is empty).
    idle_waiting: bool,
}

/// Go `queue`: the partial batch and the full batches of one shard.
pub struct Queue {
    st: Mutex<QueueState>,
    cond: Condvar,
    batch_size: usize,
    batches: usize,
}

enum Next {
    Hard,
    Closed,
    Batch(Vec<TimeSeries>),
    Timer(Vec<TimeSeries>),
}

impl Queue {
    /// Go `newQueue`.
    pub fn new(batch_size: usize, capacity: usize) -> Queue {
        // Always create an unbuffered channel even if capacity is configured to be
        // less than max_samples_per_send.
        let batches = (capacity / batch_size.max(1)).max(1);
        Queue {
            st: Mutex::new(QueueState {
                batch: Vec::with_capacity(batch_size),
                full: VecDeque::with_capacity(batches),
                // batchPool should have capacity for everything in the channel + 1 for
                // the batch being processed.
                pool: Vec::with_capacity(batches + 1),
                closed: false,
                idle_waiting: false,
            }),
            cond: Condvar::new(),
            batch_size,
            batches,
        }
    }

    fn new_batch(&self, st: &mut QueueState) -> Vec<TimeSeries> {
        st.pool.pop().unwrap_or_else(|| Vec::with_capacity(self.batch_size))
    }

    /// Go `Append`: appends to the partial batch; gives the item back when it must be retried.
    pub fn append(&self, datum: TimeSeries) -> Result<(), TimeSeries> {
        let mut st = lock(&self.st);
        if st.closed {
            return Err(datum);
        }
        st.batch.push(datum);
        if st.batch.len() >= self.batch_size {
            if st.full.len() < self.batches {
                let nb = self.new_batch(&mut st);
                let b = std::mem::replace(&mut st.batch, nb);
                st.full.push_back(b);
                drop(st);
                self.cond.notify_all();
                return Ok(());
            }
            // Remove the sample we just appended. It will get retried.
            return Err(st.batch.pop().expect("just pushed"));
        }
        if st.idle_waiting && st.batch.len() == 1 {
            st.idle_waiting = false;
            drop(st);
            self.cond.notify_all();
        }
        Ok(())
    }

    /// Go `Batch`: the next full batch, or else the partial batch (replaced by a new one).
    pub fn batch(&self) -> Vec<TimeSeries> {
        let mut st = lock(&self.st);
        if let Some(b) = st.full.pop_front() {
            return b;
        }
        let nb = self.new_batch(&mut st);
        std::mem::replace(&mut st.batch, nb)
    }

    /// Go `ReturnForReuse`.
    pub fn return_for_reuse(&self, mut batch: Vec<TimeSeries>) {
        batch.clear();
        let mut st = lock(&self.st);
        if st.pool.len() < self.batches + 1 {
            st.pool.push(batch);
        }
    }

    /// Go `FlushAndShutdown`: stops the queue and flushes the partial batch. No appends can be made after this.
    pub fn flush_and_shutdown(&self, done: &Signal) {
        while self.try_enqueueing_batch(done) {
            if done.wait_timeout(Duration::from_secs(1)) {
                break;
            }
        }
        let mut st = lock(&self.st);
        st.batch = Vec::new();
        st.closed = true;
        drop(st);
        self.cond.notify_all();
    }

    /// Go `tryEnqueueingBatch`: true when the send must be retried.
    fn try_enqueueing_batch(&self, done: &Signal) -> bool {
        let mut st = lock(&self.st);
        if st.batch.is_empty() {
            return false;
        }
        if st.full.len() < self.batches {
            let b = std::mem::take(&mut st.batch);
            st.full.push_back(b);
            drop(st);
            self.cond.notify_all();
            return false;
        }
        // The shard has been hard shut down, so no more samples can be sent.
        // Otherwise the batchQueue is full, so we need to try again later.
        !done.is_set()
    }

    fn wake(&self) {
        let _st = lock(&self.st);
        self.cond.notify_all();
    }

    /// What the shard does next (Go: the `select` of `runShard`). `timer_start` is the last timer reset.
    fn next(&self, hard: &Ctx, timer_start: &mut Instant, deadline: Duration) -> Next {
        let deadline = deadline.max(Duration::from_nanos(1));
        let mut st = lock(&self.st);
        loop {
            if hard.is_done() {
                return Next::Hard;
            }
            if let Some(b) = st.full.pop_front() {
                return Next::Batch(b);
            }
            if st.closed {
                return Next::Closed;
            }
            let elapsed = timer_start.elapsed();
            if elapsed >= deadline {
                if st.batch.is_empty() {
                    // Go's timer fired with nothing to send; it is reset at each firing.
                    advance_timer(timer_start, deadline);
                    continue;
                }
                let nb = self.new_batch(&mut st);
                return Next::Timer(std::mem::replace(&mut st.batch, nb));
            }
            if st.batch.is_empty() {
                st.idle_waiting = true;
                st = self.cond.wait(st).unwrap_or_else(|e| e.into_inner());
                st.idle_waiting = false;
                // The timer fired (and was reset) while there was nothing to send.
                advance_timer(timer_start, deadline);
            } else {
                st = self.cond.wait_timeout(st, deadline - elapsed).unwrap_or_else(|e| e.into_inner()).0;
            }
        }
    }
}

fn advance_timer(timer_start: &mut Instant, d: Duration) {
    let elapsed = timer_start.elapsed();
    if elapsed >= d {
        let k = elapsed.as_nanos() / d.as_nanos();
        *timer_start += Duration::from_nanos((d.as_nanos() * k) as u64);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Shards.

struct ShardsInner {
    queues: Vec<Arc<Queue>>,
    hard: Ctx,
    done: Arc<Signal>,
    handles: Vec<JoinHandle<()>>,
    started: bool,
}

/// Go `shards`.
pub struct Shards {
    qm: Weak<QueueManager>,
    metrics: Arc<QueueManagerMetrics>,
    logger: Logger,
    max_samples_per_send: usize,
    capacity: usize,
    flush_deadline: Duration,

    inner: RwLock<ShardsInner>, // With the WAL, this is never actually contended.
    soft_shutdown: AtomicBool,
    // So we can accurately track how many of each are lost during shard shutdowns.
    pub enqueued_samples: AtomicI64,
    pub enqueued_exemplars: AtomicI64,
    pub enqueued_histograms: AtomicI64,
    running: AtomicI32,
    samples_dropped_on_hard_shutdown: AtomicU32,
    exemplars_dropped_on_hard_shutdown: AtomicU32,
    histograms_dropped_on_hard_shutdown: AtomicU32,
    metadata_dropped_on_hard_shutdown: AtomicU32,
}

impl Shards {
    /// Go `start`: starts `n` shards; must be called before any call to `enqueue`.
    pub fn start(&self, n: usize) {
        let qm = self.qm.upgrade().expect("queue manager alive");
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());

        self.metrics.c_pending_samples.set(0.0);
        self.metrics.num_shards.get().set(n as f64);

        let queues: Vec<Arc<Queue>> = (0..n).map(|_| Arc::new(Queue::new(self.max_samples_per_send, self.capacity))).collect();
        let hard = Ctx::with_cancel();
        let done = Arc::new(Signal::new());
        self.running.store(n as i32, Ordering::SeqCst);
        self.enqueued_samples.store(0, Ordering::SeqCst);
        self.enqueued_exemplars.store(0, Ordering::SeqCst);
        self.enqueued_histograms.store(0, Ordering::SeqCst);
        self.samples_dropped_on_hard_shutdown.store(0, Ordering::SeqCst);
        self.exemplars_dropped_on_hard_shutdown.store(0, Ordering::SeqCst);
        self.histograms_dropped_on_hard_shutdown.store(0, Ordering::SeqCst);
        self.metadata_dropped_on_hard_shutdown.store(0, Ordering::SeqCst);
        let mut handles = Vec::with_capacity(n);
        for (i, q) in queues.iter().enumerate() {
            let (qm, q, hard, done) = (qm.clone(), q.clone(), hard.clone(), done.clone());
            let h = std::thread::Builder::new()
                .name(format!("rw-shard-{i}"))
                .spawn(move || run_shard(qm, i, q, hard, done))
                .expect("spawn remote write shard");
            handles.push(h);
        }
        if n == 0 {
            done.set();
        }
        inner.queues = queues;
        inner.hard = hard;
        inner.done = done;
        inner.handles = handles;
        inner.started = true;
        self.soft_shutdown.store(false, Ordering::SeqCst);
    }

    /// Go `stop`: flushes the shards for at most the flush deadline, then forces them to stop. Subsequent calls
    /// to `enqueue` fail until the next `start`.
    pub fn stop(&self) {
        // Attempt a clean shutdown, but only wait flushDeadline for all the shards
        // to cleanly exit. As we're doing RPCs, enqueue can block indefinitely.
        {
            let _r = self.inner.read().unwrap_or_else(|e| e.into_inner());
            self.soft_shutdown.store(true, Ordering::SeqCst);
        }

        // Enqueue should now be unblocked, so we can take the write lock.
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if !inner.started {
            return;
        }
        let queues = inner.queues.clone();
        let done = inner.done.clone();
        let hard = inner.hard.clone();
        std::thread::scope(|sc| {
            for q in &queues {
                let done = &done;
                sc.spawn(move || q.flush_and_shutdown(done));
            }
            if done.wait_timeout(self.flush_deadline) {
                return;
            }
            // Force an unclean shutdown.
            hard.cancel();
            for q in &queues {
                q.wake();
            }
            done.wait();

            // Log error for any dropped samples, exemplars, or histograms.
            for (t, c) in [
                ("samples", &self.samples_dropped_on_hard_shutdown),
                ("exemplars", &self.exemplars_dropped_on_hard_shutdown),
                ("histograms", &self.histograms_dropped_on_hard_shutdown),
            ] {
                let dropped = c.load(Ordering::SeqCst);
                if dropped > 0 {
                    self.logger.error(&format!("Failed to flush all {t} on shutdown"), &[("count", &dropped.to_string())]);
                }
            }
        });
        for h in inner.handles.drain(..) {
            let _ = h.join();
        }
        inner.started = false;
    }

    /// Go `enqueue`: queues a sample, exemplar or histogram on its shard. Gives the item back when the shard is
    /// full, shutting down or resharding; then back off and retry.
    pub fn enqueue(&self, r: HeadSeriesRef, data: TimeSeries) -> Result<(), TimeSeries> {
        let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
        if self.soft_shutdown.load(Ordering::SeqCst) || inner.queues.is_empty() {
            return Err(data);
        }
        let shard = (r % inner.queues.len() as u64) as usize;
        let (s_type, ts) = (data.s_type, data.timestamp);
        inner.queues[shard].append(data)?;
        match s_type {
            SeriesType::Sample => {
                self.metrics.c_pending_samples.inc();
                self.enqueued_samples.fetch_add(1, Ordering::SeqCst);
            }
            SeriesType::Exemplar => {
                self.metrics.c_pending_exemplars.inc();
                self.enqueued_exemplars.fetch_add(1, Ordering::SeqCst);
            }
            SeriesType::Histogram | SeriesType::FloatHistogram => {
                self.metrics.c_pending_histograms.inc();
                self.enqueued_histograms.fetch_add(1, Ordering::SeqCst);
            }
            SeriesType::Metadata => return Ok(()),
        }
        self.metrics.highest_timestamp.set((ts / 1000) as f64);
        Ok(())
    }

    /// The queue of a shard, while running (tests).
    pub fn queue(&self, i: usize) -> Option<Arc<Queue>> {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).queues.get(i).cloned()
    }
}

/// Reused state of one shard thread.
struct ShardSender {
    bufs: EncodeBuffers,
    symbols: SymbolTable,
    pending_v2: Vec<PendingV2>,
}

/// Go `runShard`.
fn run_shard(qm: Arc<QueueManager>, shard_id: usize, queue: Arc<Queue>, hard: Ctx, done: Arc<Signal>) {
    let s = &qm.shards;
    // Send batches of at most MaxSamplesPerSend samples to the remote storage.
    // If we have fewer samples than that, flush them out after a deadline anyways.
    let mut max_count = qm.cfg.max_samples_per_send.max(0) as usize;
    if qm.send_exemplars {
        max_count += (max_count as f64 * 0.1) as usize;
    }
    let mut sender = ShardSender {
        bufs: EncodeBuffers::default(),
        symbols: SymbolTable::new(),
        pending_v2: if qm.proto_msg == ProtoMsg::V2 { vec![PendingV2::default(); max_count] } else { Vec::new() },
    };
    let deadline = qm.cfg.batch_send_deadline.to_std();
    let mut timer_start = Instant::now();

    loop {
        match queue.next(&hard, &mut timer_start, deadline) {
            Next::Hard => {
                // In this case we drop all samples in the buffer and the queue.
                // Remove them from pending and mark them as failed.
                let dropped_samples = s.enqueued_samples.load(Ordering::SeqCst);
                let dropped_exemplars = s.enqueued_exemplars.load(Ordering::SeqCst);
                let dropped_histograms = s.enqueued_histograms.load(Ordering::SeqCst);
                qm.metrics.c_pending_samples.sub(dropped_samples as f64);
                qm.metrics.c_pending_exemplars.sub(dropped_exemplars as f64);
                qm.metrics.c_pending_histograms.sub(dropped_histograms as f64);
                qm.metrics.failed_samples_total.get().add(dropped_samples as f64);
                qm.metrics.failed_exemplars_total.get().add(dropped_exemplars as f64);
                qm.metrics.failed_histograms_total.get().add(dropped_histograms as f64);
                s.samples_dropped_on_hard_shutdown.fetch_add(dropped_samples as u32, Ordering::SeqCst);
                s.exemplars_dropped_on_hard_shutdown.fetch_add(dropped_exemplars as u32, Ordering::SeqCst);
                s.histograms_dropped_on_hard_shutdown.fetch_add(dropped_histograms as u32, Ordering::SeqCst);
                break;
            }
            Next::Closed => break,
            Next::Batch(mut batch) => {
                sender.send_batch(&qm, &hard, &mut batch, shard_id, false);
                queue.return_for_reuse(batch);
                timer_start = Instant::now();
            }
            Next::Timer(mut batch) => {
                if !batch.is_empty() {
                    sender.send_batch(&qm, &hard, &mut batch, shard_id, true);
                }
                queue.return_for_reuse(batch);
                timer_start = Instant::now();
            }
        }
    }
    if s.running.fetch_sub(1, Ordering::SeqCst) == 1 {
        done.set();
    }
}

trait SubExt {
    fn sub(&self, v: f64);
}

impl SubExt for Value {
    fn sub(&self, v: f64) {
        self.add(-v);
    }
}

impl ShardSender {
    fn send_batch(&mut self, qm: &QueueManager, hard: &Ctx, batch: &mut Vec<TimeSeries>, shard_id: usize, timer: bool) {
        match qm.proto_msg {
            ProtoMsg::V1 => {
                let mut sc = SendBatchContext::default();
                for d in batch.iter() {
                    match d.s_type {
                        SeriesType::Sample => sc.sample_count += 1,
                        SeriesType::Exemplar => sc.exemplar_count += 1,
                        SeriesType::Histogram | SeriesType::FloatHistogram => sc.histogram_count += 1,
                        SeriesType::Metadata => {}
                    }
                }
                let n = sc.sample_count + sc.exemplar_count + sc.histogram_count;
                if timer {
                    qm.logger.debug(
                        "runShard timer ticked, sending buffered data",
                        &[
                            ("samples", &sc.sample_count.to_string()),
                            ("exemplars", &sc.exemplar_count.to_string()),
                            ("shard", &shard_id.to_string()),
                            ("histograms", &sc.histogram_count.to_string()),
                        ],
                    );
                }
                // Go sends `pendingData[:n]`.
                batch.truncate(n);
                let begin = Instant::now();
                let (rs, err) = self.send_samples_with_backoff(qm, hard, batch, sc);
                qm.update_metrics(&err, sc, rs, begin.elapsed());
            }
            ProtoMsg::V2 => {
                if self.pending_v2.len() < batch.len() {
                    self.pending_v2.resize(batch.len(), PendingV2::default());
                }
                let (ns, ne, nh, nu) = populate_v2_time_series(
                    &mut self.symbols,
                    batch,
                    &mut self.pending_v2,
                    qm.send_exemplars,
                    qm.send_native_histograms.load(Ordering::Relaxed),
                    qm.enable_type_and_unit_labels,
                );
                let n = ns + ne + nh;
                if nu > 0 {
                    qm.logger.warn("unexpected metadata sType in populateV2TimeSeries", &[("count", &nu.to_string())]);
                }
                let sc = SendBatchContext { sample_count: ns, exemplar_count: ne, histogram_count: nh };
                let begin = Instant::now();
                let (rs, err) = self.send_v2_samples_with_backoff(qm, hard, &mut batch[..n], sc);
                qm.update_metrics(&err, sc, rs, begin.elapsed());
                self.symbols.reset();
            }
        }
    }

    /// Go `sendSamplesWithBackoff`.
    fn send_samples_with_backoff(
        &mut self,
        qm: &QueueManager,
        ctx: &Ctx,
        batch: &mut Vec<TimeSeries>,
        sc: SendBatchContext,
    ) -> (WriteResponseStats, Option<Error>) {
        // Build the WriteRequest with no metadata.
        let (r, highest, lowest) = build_write_request(&qm.logger, batch, None, &mut self.bufs);
        qm.build_request_limit_timestamp.store(lowest, Ordering::SeqCst);
        let mut req_len = match r {
            Ok(n) => n,
            // Failing to build the write request is non-recoverable, since it will
            // only error if marshaling the proto to bytes fails.
            Err(e) => return (WriteResponseStats::default(), Some(e)),
        };
        let req_size = req_len;
        let limit = qm.cfg.sample_age_limit.0;
        let metrics = &qm.metrics;

        // Since we retry writes via attemptStore and sendWriteRequestWithBackoff we need
        // to track the total amount of accepted data across the various attempts.
        let mut accumulated = WriteResponseStats::default();
        let bufs = &mut self.bufs;
        let err = {
            let mut attempt = |try_: i32| -> Option<Error> {
                let now = now_ns();
                let lowest = qm.build_request_limit_timestamp.load(Ordering::SeqCst);
                if is_sample_old_ns(now, limit, lowest) {
                    // This will filter out old samples during retries.
                    let mut f = old_filter::<TimeSeries>(metrics, now, limit);
                    let (r, _, lowest) = build_write_request(&qm.logger, batch, Some(&mut f), bufs);
                    qm.build_request_limit_timestamp.store(lowest, Ordering::SeqCst);
                    match r {
                        Ok(n) => req_len = n,
                        Err(e) => return Some(e),
                    }
                }
                let cl = qm.client();
                metrics.record_batch_attempt(&sc);
                let begin = Instant::now();
                // Technically for v1, we will likely have empty response stats, but for
                // newer Receivers this might be not, so used it in a best effort.
                let r = cl.store(&ctx.ctx, &bufs.out[..req_len], try_);
                metrics.record_latency(begin);
                match r {
                    Ok(rs) => {
                        accumulated = accumulated.add(rs);
                        None
                    }
                    Err(e) => {
                        accumulated = accumulated.add(WriteResponseStats::default());
                        Some(e)
                    }
                }
            };
            let mut on_retry = || metrics.record_retry(&sc);
            qm.send_write_request_with_backoff(ctx, &mut attempt, &mut on_retry)
        };
        if let Some(e) = &err
            && e.is(&ERR_CANCELED)
        {
            // When there is resharding, we cancel the context for this queue, which means the data is not sent.
            // So we exit early to not update the metrics.
            return (accumulated, err);
        }

        metrics.sent_bytes_total.get().add(req_size as f64);
        metrics.highest_sent_timestamp.set((highest / 1000) as f64);

        if err.is_none() && !accumulated.confirmed {
            // No 2.0 response headers, and we sent v1 message, so likely it's 1.0 Receiver.
            // Assume success, don't rely on headers.
            return (
                WriteResponseStats {
                    samples: sc.sample_count as i64,
                    histograms: sc.histogram_count as i64,
                    exemplars: sc.exemplar_count as i64,
                    confirmed: false,
                },
                None,
            );
        }
        (accumulated, err)
    }

    /// Go `sendV2SamplesWithBackoff`.
    fn send_v2_samples_with_backoff(
        &mut self,
        qm: &QueueManager,
        ctx: &Ctx,
        batch: &mut [TimeSeries],
        sc: SendBatchContext,
    ) -> (WriteResponseStats, Option<Error>) {
        let n = batch.len();
        let (r, highest, lowest) =
            build_v2_write_request(&qm.logger, batch, &mut self.pending_v2[..n], &self.symbols, None, &mut self.bufs);
        qm.build_request_limit_timestamp.store(lowest, Ordering::SeqCst);
        let mut req_len = match r {
            Ok(n) => n,
            Err(e) => return (WriteResponseStats::default(), Some(e)),
        };
        let req_size = req_len;
        let limit = qm.cfg.sample_age_limit.0;
        let metrics = &qm.metrics;

        let mut accumulated = WriteResponseStats::default();
        let bufs = &mut self.bufs;
        let pending = &mut self.pending_v2[..n];
        let symbols = &self.symbols;
        let err = {
            let mut attempt = |try_: i32| -> Option<Error> {
                let now = now_ns();
                let lowest = qm.build_request_limit_timestamp.load(Ordering::SeqCst);
                if is_sample_old_ns(now, limit, lowest) {
                    // This will filter out old samples during retries.
                    let mut f = old_filter::<TimeSeries>(metrics, now, limit);
                    let (r, _, lowest) = build_v2_write_request(&qm.logger, batch, pending, symbols, Some(&mut f), bufs);
                    qm.build_request_limit_timestamp.store(lowest, Ordering::SeqCst);
                    match r {
                        Ok(n) => req_len = n,
                        Err(e) => return Some(e),
                    }
                }
                let cl = qm.client();
                metrics.record_batch_attempt(&sc);
                let begin = Instant::now();
                let r = cl.store(&ctx.ctx, &bufs.out[..req_len], try_);
                metrics.record_latency(begin);
                let (rs, err) = match r {
                    Ok(rs) => (rs, None),
                    Err(e) => (WriteResponseStats::default(), Some(e)),
                };
                accumulated = accumulated.add(rs);
                match err {
                    None => {
                        // Check the case mentioned in PRW 2.0
                        // https://prometheus.io/docs/specs/remote_write_spec_2_0/#required-written-response-headers.
                        if sc.sample_count + sc.histogram_count + sc.exemplar_count > 0 && rs.no_data_written() {
                            let e = Error::msg(format!(
                                "sent v2 request with {} samples, {} histograms and {} exemplars; got 2xx, but PRW 2.0 response header statistics indicate {} samples, {} histograms and {} exemplars were accepted; assuming failure e.g. the target only supports PRW 1.0 prometheus.WriteRequest, but does not check the Content-Type header correctly",
                                sc.sample_count, sc.histogram_count, sc.exemplar_count, rs.samples, rs.histograms, rs.exemplars,
                            ));
                            if qm.failed_request_logging {
                                qm.logger.debug("Failed to send remote write v2 request", &[("err", &e.to_string())]);
                            }
                            return Some(e);
                        }
                        None
                    }
                    Some(e) => {
                        if qm.failed_request_logging {
                            qm.logger.debug("Failed to send remote write v2 request", &[("err", &e.to_string())]);
                        }
                        Some(e)
                    }
                }
            };
            let mut on_retry = || metrics.record_retry(&sc);
            qm.send_write_request_with_backoff(ctx, &mut attempt, &mut on_retry)
        };
        if let Some(e) = &err
            && e.is(&ERR_CANCELED)
        {
            return (accumulated, err);
        }
        metrics.sent_bytes_total.get().add(req_size as f64);
        metrics.highest_sent_timestamp.set((highest / 1000) as f64);
        (accumulated, err)
    }
}

/// Go `setAtomicToNewer`: sets `value` to `new_value` when that is newer; returns the previous value and
/// whether it was updated.
pub fn set_atomic_to_newer(value: &AtomicI64, new_value: i64) -> (i64, bool) {
    let mut current = value.load(Ordering::SeqCst);
    loop {
        if current >= new_value {
            // If the current stored value is newer than newValue; abort.
            return (current, false);
        }
        match value.compare_exchange(current, new_value, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return (current, true),
            Err(c) => current = c,
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// QueueManager.

/// Go `nhcbLayout`: a bucket layout shared by the NHCB series that use it.
#[derive(Debug)]
pub struct NhcbLayout {
    pub bounds: Arc<[f64]>,
    /// Formatted bounds, followed by +Inf.
    pub le: Vec<String>,
    key: Vec<u8>,
    /// Series sharing the layout.
    pub refs: AtomicUsize,
}

/// The state Go guards with `seriesMtx`.
struct SeriesState {
    labels: HashMap<HeadSeriesRef, Labels>,
    metadata: HashMap<HeadSeriesRef, Handle>,
    dropped: HashSet<HeadSeriesRef>,
    /// Layout of each converted NHCB series.
    nhcb_series_bounds: HashMap<HeadSeriesRef, Arc<NhcbLayout>>,
    /// Keyed by the encoded bounds.
    nhcb_layouts: HashMap<Vec<u8>, Arc<NhcbLayout>>,
    /// Scratch space for layout keys.
    nhcb_layout_key: Vec<u8>,
    /// Scratch space for bucket counts.
    nhcb_counts: Vec<f64>,
    builder: Builder,
    relabel_configs: Vec<relabel::Config>,
}

/// Go `QueueManager`: manages a queue of samples to be sent to the storage behind a `WriteClient`. Fed by a
/// WAL watcher (it implements the watcher's `WriteTo` through an adapter).
pub struct QueueManager {
    pub last_send_timestamp: AtomicI64,
    pub build_request_limit_timestamp: AtomicI64,
    /// Time that reshard was disabled.
    pub reshard_disable_start_timestamp: AtomicI64,
    /// Time that reshard is disabled until.
    pub reshard_disable_end_timestamp: AtomicI64,

    logger: Logger,
    pub cfg: QueueConfig,
    mcfg: MetadataConfig,
    pub external_labels: Vec<Label>,
    send_exemplars: bool,
    send_native_histograms: AtomicBool,
    enable_type_and_unit_labels: bool,
    failed_request_logging: bool,
    convert_nhcb_to_classic: AtomicBool,
    watcher: Arc<Watcher>,
    metadata_watcher: Option<Arc<MetadataWatcher>>,

    store_client: RwLock<Arc<dyn WriteClient>>,
    proto_msg: ProtoMsg,

    series: Mutex<SeriesState>,
    /// Covers seriesSegmentIndexes - if you also lock series, take series first.
    series_segment_indexes: Mutex<HashMap<HeadSeriesRef, i64>>,

    pub shards: Shards,
    num_shards: AtomicI64,
    reshard_tx: SyncSender<Option<i64>>,
    reshard_rx: Mutex<Option<Receiver<Option<i64>>>>,
    quit: Signal,
    loops: Mutex<Vec<JoinHandle<()>>>,
    me: Weak<QueueManager>,

    pub data_in: Arc<EwmaRate>,
    pub data_in_expanded: EwmaRate,
    pub data_dropped: EwmaRate,
    pub data_out: EwmaRate,
    pub data_out_duration: EwmaRate,

    pub metrics: Arc<QueueManagerMetrics>,
    pub interner: Arc<Pool>,
    pub highest_recv_timestamp: Arc<MaxTimestamp>,
}

/// The WAL watcher's view of a queue manager (the watcher must not keep it alive).
struct WalSink(Weak<QueueManager>);

impl WriteTo for WalSink {
    fn append(&self, samples: &[RefSample]) -> bool {
        self.0.upgrade().is_some_and(|q| q.append(samples))
    }
    fn append_exemplars(&self, exemplars: &[RefExemplar]) -> bool {
        self.0.upgrade().is_some_and(|q| q.append_exemplars(exemplars))
    }
    fn append_histograms(&self, histograms: &[RefHistogramSample]) -> bool {
        self.0.upgrade().is_some_and(|q| q.append_histograms(histograms))
    }
    fn append_float_histograms(&self, float_histograms: &[RefFloatHistogramSample]) -> bool {
        self.0.upgrade().is_some_and(|q| q.append_float_histograms(float_histograms))
    }
    fn store_series(&self, series: &[RefSeries], index: i64) {
        if let Some(q) = self.0.upgrade() {
            q.store_series(series, index)
        }
    }
    fn store_metadata(&self, metadata: &[RefMetadata]) {
        if let Some(q) = self.0.upgrade() {
            q.store_metadata(metadata)
        }
    }
    fn update_series_segment(&self, series: &[RefSeries], index: i64) {
        if let Some(q) = self.0.upgrade() {
            q.update_series_segment(series, index)
        }
    }
    fn series_reset(&self, index: i64) {
        if let Some(q) = self.0.upgrade() {
            q.series_reset(index)
        }
    }
}

struct MetaSink(Weak<QueueManager>);

impl MetadataAppender for MetaSink {
    fn append_watcher_metadata(&self, ctx: &Ctx, metadata: &[MetricMetadata]) {
        if let Some(q) = self.0.upgrade() {
            q.append_watcher_metadata(ctx, metadata)
        }
    }
}

impl MetadataAppender for QueueManager {
    fn append_watcher_metadata(&self, ctx: &Ctx, metadata: &[MetricMetadata]) {
        QueueManager::append_watcher_metadata(self, ctx, metadata)
    }
}

/// Go `processExternalLabels`: merges `external_labels` into `b`. If `b` contains a label in
/// `external_labels`, the value in `b` wins.
pub fn process_external_labels(b: &mut Builder, external_labels: &[Label]) {
    for el in external_labels {
        if b.get(&el.name).is_empty() {
            b.set(&el.name, &el.value);
        }
    }
}

/// A histogram sample of either kind (Go passes `*Histogram` or `*FloatHistogram`).
#[derive(Clone, Copy)]
enum HistArc<'a> {
    Int(&'a Arc<Histogram>),
    Float(&'a Arc<FloatHistogram>),
}

impl<'a> HistArc<'a> {
    fn data(self) -> HistData<'a> {
        match self {
            HistArc::Int(h) => HistData::Int(h),
            HistArc::Float(h) => HistData::Float(h),
        }
    }
    fn schema(self) -> i32 {
        match self {
            HistArc::Int(h) => h.schema,
            HistArc::Float(h) => h.schema,
        }
    }
    fn sum(self) -> f64 {
        match self {
            HistArc::Int(h) => h.sum,
            HistArc::Float(h) => h.sum,
        }
    }
    fn item(self, lbls: Labels, meta: Handle, st: i64, t: i64) -> TimeSeries {
        match self {
            HistArc::Int(h) => TimeSeries {
                series_labels: lbls,
                metadata: meta,
                start_timestamp: st,
                timestamp: t,
                histogram: Some(h.clone()),
                s_type: SeriesType::Histogram,
                ..Default::default()
            },
            HistArc::Float(h) => TimeSeries {
                series_labels: lbls,
                metadata: meta,
                start_timestamp: st,
                timestamp: t,
                float_histogram: Some(h.clone()),
                s_type: SeriesType::FloatHistogram,
                ..Default::default()
            },
        }
    }
}

impl QueueManager {
    /// Go `NewQueueManager`: builds a queue manager and its WAL watcher (of `<dir>/wal`), with the queue
    /// manager as the watcher's `WriteTo`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metrics: Arc<QueueManagerMetrics>,
        watcher_metrics: Option<Arc<WatcherMetrics>>,
        reader_metrics: Option<Arc<LiveReaderMetrics>>,
        logger: Option<Logger>,
        dir: &Path,
        samples_in: Arc<EwmaRate>,
        cfg: QueueConfig,
        mcfg: MetadataConfig,
        external_labels: &Labels,
        relabel_configs: Vec<relabel::Config>,
        client: Arc<dyn WriteClient>,
        flush_deadline: Duration,
        interner: Arc<Pool>,
        highest_recv_timestamp: Arc<MaxTimestamp>,
        sm: Option<Arc<dyn ReadyScrapeManager>>,
        enable_exemplar_remote_write: bool,
        enable_native_histogram_remote_write: bool,
        enable_type_and_unit_labels: bool,
        proto_msg: ProtoMsg,
        record_buf: Option<Arc<BuffersPool>>,
        failed_request_logging: bool,
        convert_nhcb_to_classic: bool,
    ) -> Arc<QueueManager> {
        // Copy externalLabels into a slice, which we need for processExternalLabels.
        let ext_labels: Vec<Label> = external_labels.iter().map(|(n, v)| Label::new(n, v)).collect();
        let logger = logger.unwrap_or_default().with(&[(REMOTE_NAME, client.name()), (ENDPOINT, client.endpoint())]);
        let mut mcfg = mcfg;
        // The current MetadataWatcher implementation is mutually exclusive
        // with the new approach, which stores metadata as WAL records and
        // ships them alongside series. If both mechanisms are set, the new one
        // takes precedence by implicitly disabling the older one.
        if mcfg.send && proto_msg != ProtoMsg::V1 {
            logger.warn("'metadata_config.send' is ignored when using Remote Write 2.0; metadata is sent when --enable-feature=metadata-wal-records or --enable-feature=type-and-unit-labels is enabled", &[]);
            mcfg.send = false;
        }
        let (reshard_tx, reshard_rx) = sync_channel(0);
        let name = client.name().to_string();
        Arc::new_cyclic(|me: &Weak<QueueManager>| {
            let wal_metadata = proto_msg != ProtoMsg::V1;
            let watcher = Watcher::new(
                watcher_metrics,
                reader_metrics,
                &name,
                Arc::new(WalSink(me.clone())),
                dir,
                enable_exemplar_remote_write,
                enable_native_histogram_remote_write || convert_nhcb_to_classic,
                wal_metadata,
                record_buf,
            );
            let metadata_watcher = mcfg.send.then(|| {
                MetadataWatcher::new(
                    Some(logger.clone()),
                    sm,
                    &name,
                    Arc::new(MetaSink(me.clone())),
                    mcfg.send_interval.to_std(),
                    flush_deadline,
                )
            });
            let shards = Shards {
                qm: me.clone(),
                metrics: metrics.clone(),
                logger: logger.clone(),
                max_samples_per_send: cfg.max_samples_per_send.max(0) as usize,
                capacity: cfg.capacity.max(0) as usize,
                flush_deadline,
                inner: RwLock::new(ShardsInner {
                    queues: Vec::new(),
                    hard: Ctx::background(),
                    done: Arc::new(Signal::new()),
                    handles: Vec::new(),
                    started: false,
                }),
                soft_shutdown: AtomicBool::new(false),
                enqueued_samples: AtomicI64::new(0),
                enqueued_exemplars: AtomicI64::new(0),
                enqueued_histograms: AtomicI64::new(0),
                running: AtomicI32::new(0),
                samples_dropped_on_hard_shutdown: AtomicU32::new(0),
                exemplars_dropped_on_hard_shutdown: AtomicU32::new(0),
                histograms_dropped_on_hard_shutdown: AtomicU32::new(0),
                metadata_dropped_on_hard_shutdown: AtomicU32::new(0),
            };
            QueueManager {
                last_send_timestamp: AtomicI64::new(0),
                build_request_limit_timestamp: AtomicI64::new(0),
                reshard_disable_start_timestamp: AtomicI64::new(0),
                reshard_disable_end_timestamp: AtomicI64::new(0),
                logger: logger.clone(),
                num_shards: AtomicI64::new(cfg.min_shards),
                cfg,
                mcfg,
                external_labels: ext_labels,
                send_exemplars: enable_exemplar_remote_write,
                send_native_histograms: AtomicBool::new(enable_native_histogram_remote_write),
                enable_type_and_unit_labels,
                failed_request_logging,
                convert_nhcb_to_classic: AtomicBool::new(convert_nhcb_to_classic),
                watcher,
                metadata_watcher,
                store_client: RwLock::new(client),
                proto_msg,
                series: Mutex::new(SeriesState {
                    labels: HashMap::new(),
                    metadata: HashMap::new(),
                    dropped: HashSet::new(),
                    nhcb_series_bounds: HashMap::new(),
                    nhcb_layouts: HashMap::new(),
                    nhcb_layout_key: Vec::new(),
                    nhcb_counts: Vec::new(),
                    builder: Builder::new(Labels::empty()),
                    relabel_configs,
                }),
                series_segment_indexes: Mutex::new(HashMap::new()),
                shards,
                reshard_tx,
                reshard_rx: Mutex::new(Some(reshard_rx)),
                quit: Signal::new(),
                loops: Mutex::new(Vec::new()),
                me: me.clone(),
                data_in: samples_in,
                data_in_expanded: EwmaRate::new(EWMA_WEIGHT, SHARD_UPDATE_DURATION),
                data_dropped: EwmaRate::new(EWMA_WEIGHT, SHARD_UPDATE_DURATION),
                data_out: EwmaRate::new(EWMA_WEIGHT, SHARD_UPDATE_DURATION),
                data_out_duration: EwmaRate::new(EWMA_WEIGHT, SHARD_UPDATE_DURATION),
                metrics,
                interner,
                highest_recv_timestamp,
            }
        })
    }

    pub fn proto_msg(&self) -> ProtoMsg {
        self.proto_msg
    }

    /// The WAL watcher of this queue.
    pub fn watcher(&self) -> &Arc<Watcher> {
        &self.watcher
    }

    pub fn num_shards(&self) -> i64 {
        self.num_shards.load(Ordering::SeqCst)
    }

    /// Sets the current number of shards (Go tests assign `numShards`).
    pub fn set_num_shards(&self, n: i64) {
        self.num_shards.store(n, Ordering::SeqCst);
    }

    /// Go tests assign `sendNativeHistograms`.
    pub fn set_send_native_histograms(&self, v: bool) {
        self.send_native_histograms.store(v, Ordering::SeqCst);
    }

    /// Go tests assign `convertNHCBToClassic`.
    pub fn set_convert_nhcb_to_classic(&self, v: bool) {
        self.convert_nhcb_to_classic.store(v, Ordering::SeqCst);
    }

    /// Go tests assign `relabelConfigs`.
    pub fn set_relabel_configs(&self, cfgs: Vec<relabel::Config>) {
        lock(&self.series).relabel_configs = cfgs;
    }

    /// The number of known series (Go `len(seriesLabels)`).
    pub fn series_labels_len(&self) -> usize {
        lock(&self.series).labels.len()
    }

    /// Go `len(seriesMetadata)`.
    pub fn series_metadata_len(&self) -> usize {
        lock(&self.series).metadata.len()
    }

    /// Go `len(nhcbLayouts)`.
    pub fn nhcb_layouts_len(&self) -> usize {
        lock(&self.series).nhcb_layouts.len()
    }

    /// Go `len(nhcbSeriesBounds)`.
    pub fn nhcb_series_bounds_len(&self) -> usize {
        lock(&self.series).nhcb_series_bounds.len()
    }

    /// Go `nhcbSeriesBounds[ref]`.
    pub fn nhcb_series_layout(&self, r: HeadSeriesRef) -> Option<Arc<NhcbLayout>> {
        lock(&self.series).nhcb_series_bounds.get(&r).cloned()
    }

    /// Sends a reshard request and waits until the reshard loop takes it (Go `reshardChan <- n`).
    pub fn reshard_chan_send(&self, n: i64) {
        let _ = self.reshard_tx.send(Some(n));
    }

    fn dropped(&self, which: Dropped, reason: &'static str) {
        self.metrics.dropped_child(which, reason).inc();
    }

    /// Go `AppendWatcherMetadata`: sends metadata to the remote storage in batches, not parallelized. Only for
    /// `metadata_config.send` and remote write 1.0.
    pub fn append_watcher_metadata(&self, ctx: &Ctx, metadata: &[MetricMetadata]) {
        // no op for any newer proto format, which will cache metadata sent to it from the WAL watcher.
        if self.proto_msg != ProtoMsg::V1 {
            return;
        }

        // 1.X will still get metadata in batches.
        let mm: Vec<prompb::MetricMetadata> = metadata
            .iter()
            .map(|e| prompb::MetricMetadata {
                metric_family_name: e.metric_family.clone(),
                help: e.help.clone(),
                r#type: prompb::from_metadata_type(e.typ),
                unit: e.unit.clone(),
            })
            .collect();

        let per = self.mcfg.max_samples_per_send.max(1) as usize;
        let num_sends = (metadata.len() as f64 / per as f64).ceil() as usize;
        let mut bufs = EncodeBuffers::default();
        for i in 0..num_sends {
            let last = ((i + 1) * per).min(metadata.len());
            if let Some(err) = self.send_metadata_with_backoff(ctx, &mm[i * per..last], &mut bufs) {
                let count = last - i * per;
                self.metrics.failed_metadata_total.get().add(count as f64);
                self.logger.error(
                    "non-recoverable error while sending metadata",
                    &[("count", &count.to_string()), ("err", &err.to_string())],
                );
            }
        }
    }

    fn send_metadata_with_backoff(&self, ctx: &Ctx, metadata: &[prompb::MetricMetadata], bufs: &mut EncodeBuffers) -> Option<Error> {
        // Build the WriteRequest with no samples (v1 flow).
        let req = prompb::WriteRequest { timeseries: Vec::new(), metadata: metadata.to_vec() };
        req.marshal_into(&mut bufs.pbuf);
        let n = match snappy_encode(&mut bufs.enc, &bufs.pbuf, &mut bufs.out) {
            Ok(n) => n,
            Err(e) => return Some(e),
        };
        let out = &bufs.out[..n];
        let mut attempt = |try_: i32| -> Option<Error> {
            let cl = self.client();
            let begin = Instant::now();
            // Ignoring WriteResponseStats, because there is nothing for metadata, since it's
            // embedded in v2 calls now, and we do v1 here.
            let r = cl.store(&ctx.ctx, out, try_);
            self.metrics.c_sent_batch_duration.observe(begin.elapsed().as_secs_f64());
            r.err()
        };
        let mut on_retry = || self.metrics.retried_metadata_total.get().add(metadata.len() as f64);
        if let Some(e) = self.send_write_request_with_backoff(ctx, &mut attempt, &mut on_retry) {
            return Some(e);
        }
        self.metrics.metadata_total.get().add(metadata.len() as f64);
        self.metrics.metadata_bytes_total.get().add(n as f64);
        None
    }

    fn enqueue_with_backoff(&self, r: HeadSeriesRef, mut data: TimeSeries, initial_backoff: i64) -> bool {
        let mut backoff = initial_backoff;
        let max_backoff = self.cfg.max_backoff.0;
        loop {
            if self.quit.is_set() {
                return false;
            }
            match self.shards.enqueue(r, data) {
                Ok(()) => return true,
                Err(d) => data = d,
            }
            self.metrics.c_enqueue_retries.inc();
            sleep_ns(backoff);
            backoff *= 2;
            // It is reasonable to use t.cfg.MaxBackoff here, as if we have hit
            // the full backoff we are likely waiting for external resources.
            if backoff > max_backoff {
                backoff = max_backoff;
            }
        }
    }

    /// Records a sample, exemplar or histogram of a series that has no labels (dropped or unknown).
    fn record_unknown(&self, s: &SeriesState, r: HeadSeriesRef, which: Dropped, what: &str) {
        self.data_dropped.incr(1);
        if !s.dropped.contains(&r) {
            self.logger.info(
                &format!("Dropped {what} for series that was not explicitly dropped via relabelling"),
                &[("ref", &r.to_string())],
            );
            self.dropped(which, REASON_UNINTENTIONAL_DROPPED_SERIES);
        } else {
            self.dropped(which, REASON_DROPPED_SERIES);
        }
    }

    /// Go `Append`: queues samples to be sent to the remote storage. Blocks until all samples are enqueued on
    /// their shards or a shutdown signal is received.
    pub fn append(&self, samples: &[RefSample]) -> bool {
        let now = now_ns();
        let limit = self.cfg.sample_age_limit.0;
        for s in samples {
            if is_sample_old_ns(now, limit, s.t) {
                self.dropped(Dropped::Samples, REASON_TOO_OLD);
                continue;
            }
            let g = lock(&self.series);
            let Some(lbls) = g.labels.get(&s.r#ref).cloned() else {
                self.record_unknown(&g, s.r#ref, Dropped::Samples, "sample");
                continue;
            };
            // TODO(cstyan): Handle or at least log an error if no metadata is found.
            // See https://github.com/prometheus/prometheus/issues/14405
            let meta = g.metadata.get(&s.r#ref).cloned().unwrap_or_default();
            if self.convert_nhcb_to_classic.load(Ordering::Relaxed) && g.nhcb_series_bounds.contains_key(&s.r#ref) {
                // A float sample for a converted NHCB series is either its stale marker or
                // a change of sample type. Both retire the classic series; a type change
                // also forwards the float, whose start time stays off the retired series.
                let mut st = s.st;
                let mut replacement = None;
                if !is_stale_nan(s.v) {
                    st = 0;
                    replacement = Some(TimeSeries {
                        series_labels: lbls.clone(),
                        metadata: meta.clone(),
                        start_timestamp: s.st,
                        timestamp: s.t,
                        value: s.v,
                        s_type: SeriesType::Sample,
                        ..Default::default()
                    });
                }
                if !self.convert_and_enqueue_nhcb(g, s.r#ref, st, s.t, None, true, &lbls, &meta, replacement) {
                    return false;
                }
                continue;
            }
            drop(g);
            // Start with a very small backoff. This should not be t.cfg.MinBackoff
            // as it can happen without errors, and we want to pickup work after
            // filling a queue/resharding as quickly as possible.
            let item = TimeSeries {
                series_labels: lbls,
                metadata: meta,
                start_timestamp: s.st,
                timestamp: s.t,
                value: s.v,
                s_type: SeriesType::Sample,
                ..Default::default()
            };
            if !self.enqueue_with_backoff(s.r#ref, item, 5_000_000) {
                return false;
            }
        }
        true
    }

    /// Go `AppendExemplars`.
    pub fn append_exemplars(&self, exemplars: &[RefExemplar]) -> bool {
        if !self.send_exemplars {
            return true;
        }
        let now = now_ns();
        let limit = self.cfg.sample_age_limit.0;
        for e in exemplars {
            if is_sample_old_ns(now, limit, e.t) {
                self.dropped(Dropped::Exemplars, REASON_TOO_OLD);
                continue;
            }
            let mut g = lock(&self.series);
            let Some(mut lbls) = g.labels.get(&e.r#ref).cloned() else {
                // Track dropped exemplars in the same EWMA for sharding calc.
                self.record_unknown(&g, e.r#ref, Dropped::Exemplars, "exemplar");
                continue;
            };
            if self.convert_nhcb_to_classic.load(Ordering::Relaxed)
                && let Some(layout) = g.nhcb_series_bounds.get(&e.r#ref).cloned()
            {
                let base_name = lbls.get(model::labels::METRIC_NAME).to_string();
                if !base_name.is_empty() {
                    let mut le = &layout.le[layout.bounds.len()]; // +Inf, for values above every bound.
                    for (i, b) in layout.bounds.iter().enumerate() {
                        if e.v <= *b {
                            le = &layout.le[i];
                            break;
                        }
                    }
                    let b = &mut g.builder;
                    b.reset(lbls);
                    b.set(model::labels::METRIC_NAME, &format!("{base_name}_bucket"));
                    b.set(model::labels::BUCKET_LABEL, le);
                    lbls = b.labels();
                }
            }
            let meta = g.metadata.get(&e.r#ref).cloned().unwrap_or_default();
            drop(g);
            // This will only loop if the queues are being resharded.
            let item = TimeSeries {
                series_labels: lbls,
                metadata: meta,
                timestamp: e.t,
                value: e.v,
                exemplar_labels: e.labels.clone(),
                s_type: SeriesType::Exemplar,
                ..Default::default()
            };
            if !self.enqueue_with_backoff(e.r#ref, item, self.cfg.min_backoff.0) {
                return false;
            }
        }
        true
    }

    /// Go `acquireNHCBLayout`: a new reference to the shared layout for `bounds`. Release it with
    /// `release_nhcb_layout`.
    fn acquire_nhcb_layout(s: &mut SeriesState, bounds: &[f64]) -> Arc<NhcbLayout> {
        // The key is the bounds' exact bit patterns.
        let mut key = std::mem::take(&mut s.nhcb_layout_key);
        key.clear();
        for b in bounds {
            key.extend_from_slice(&b.to_bits().to_le_bytes());
        }
        let layout = match s.nhcb_layouts.get(&key[..]) {
            Some(l) => l.clone(),
            None => {
                let mut le: Vec<String> = Vec::with_capacity(bounds.len() + 1);
                for b in bounds {
                    le.push(format_open_metrics_float(*b));
                }
                le.push(format_open_metrics_float(f64::INFINITY));
                let l = Arc::new(NhcbLayout { bounds: Arc::from(bounds), le, key: key.clone(), refs: AtomicUsize::new(0) });
                s.nhcb_layouts.insert(key.clone(), l.clone());
                l
            }
        };
        s.nhcb_layout_key = key;
        layout.refs.fetch_add(1, Ordering::SeqCst);
        layout
    }

    /// Go `releaseNHCBLayout`: drops a reference, forgetting the layout once unused.
    fn release_nhcb_layout(s: &mut SeriesState, layout: &Arc<NhcbLayout>) {
        if layout.refs.fetch_sub(1, Ordering::SeqCst) == 1 {
            s.nhcb_layouts.remove(&layout.key[..]);
        }
    }

    /// Go `convertAndEnqueueNHCB`: converts an NHCB histogram (using the already-relabeled `lbls`) into classic
    /// `_bucket`, `_count` and `_sum` samples while holding the series lock, then enqueues them after releasing
    /// it. `nhcb` None is a stale marker for the series' current layout. A replacement follows the converted
    /// stale markers when changing sample type.
    #[allow(clippy::too_many_arguments)]
    fn convert_and_enqueue_nhcb(
        &self,
        mut g: MutexGuard<'_, SeriesState>,
        r: HeadSeriesRef,
        st: i64,
        ts: i64,
        nhcb: Option<HistData<'_>>,
        is_stale: bool,
        lbls: &Labels,
        meta: &Handle,
        replacement: Option<TimeSeries>,
    ) -> bool {
        let s = &mut *g;
        let prev = s.nhcb_series_bounds.get(&r).cloned();
        let was_active = prev.is_some();
        let prev_bounds: &[f64] = prev.as_deref().map(|l| &l.bounds[..]).unwrap_or(&[]);
        let stale_h;
        let (nhcb, custom_values): (HistData<'_>, &[f64]) = match nhcb {
            Some(h) => (h, h.custom_values()),
            None => {
                stale_h = Histogram {
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    custom_values: prev.as_ref().map(|l| l.bounds.clone()),
                    sum: stale_nan(),
                    ..Default::default()
                };
                (HistData::Int(&stale_h), prev_bounds)
            }
        };

        let same_bounds = was_active && prev_bounds == custom_values;
        let mut capacity = custom_values.len() + 3;
        if was_active && !same_bounds {
            capacity += prev_bounds.len();
        }
        if replacement.is_some() {
            capacity += 1;
        }
        let mut classic_series: Vec<TimeSeries> = Vec::with_capacity(capacity);
        let emit = |out: &mut Vec<TimeSeries>, bucket_labels: Labels, value: f64| {
            out.push(TimeSeries {
                series_labels: bucket_labels,
                metadata: meta.clone(),
                start_timestamp: st,
                timestamp: ts,
                value,
                s_type: SeriesType::Sample,
                ..Default::default()
            });
        };
        let base_name = lbls.get(model::labels::METRIC_NAME);
        let mut err: Option<String> = None;
        let (mut count, mut sum) = (0.0, 0.0);
        let mut counts = std::mem::take(&mut s.nhcb_counts);
        if base_name.is_empty() {
            err = Some("metric name label '__name__' is missing".to_string());
        } else {
            match classic_bucket_counts(Some(nhcb.as_ref()), counts) {
                Ok((c, n, sm)) => {
                    counts = c;
                    count = n;
                    sum = sm;
                }
                Err(e) => {
                    counts = Vec::new();
                    err = Some(e.to_string());
                }
            }
        }
        let mut layout = prev.clone();
        if err.is_none() {
            if !same_bounds {
                layout = Some(Self::acquire_nhcb_layout(s, custom_values));
            }
            let l = layout.as_ref().expect("layout");
            let bucket_name = format!("{base_name}_bucket");
            for (i, c) in counts.iter().enumerate() {
                s.builder.reset(lbls.clone());
                s.builder.set(model::labels::METRIC_NAME, &bucket_name);
                s.builder.set(model::labels::BUCKET_LABEL, &l.le[i]);
                emit(&mut classic_series, s.builder.labels(), *c);
            }
            s.builder.reset(lbls.clone());
            s.builder.set(model::labels::METRIC_NAME, &format!("{base_name}_count"));
            emit(&mut classic_series, s.builder.labels(), count);
            s.builder.reset(lbls.clone());
            s.builder.set(model::labels::METRIC_NAME, &format!("{base_name}_sum"));
            emit(&mut classic_series, s.builder.labels(), sum);
        }
        s.nhcb_counts = counts;
        if err.is_none()
            && was_active
            && !same_bounds
            && let Some(p) = &prev
        {
            let stale_val = stale_nan();
            for (i, prev_bound) in prev_bounds.iter().enumerate() {
                if custom_values.contains(prev_bound) {
                    continue;
                }
                s.builder.reset(lbls.clone());
                s.builder.set(model::labels::METRIC_NAME, &format!("{base_name}_bucket"));
                s.builder.set(model::labels::BUCKET_LABEL, &p.le[i]);
                // Retired buckets get no start time; st belongs to the new layout.
                classic_series.push(TimeSeries {
                    series_labels: s.builder.labels(),
                    metadata: meta.clone(),
                    start_timestamp: 0,
                    timestamp: ts,
                    value: stale_val,
                    s_type: SeriesType::Sample,
                    ..Default::default()
                });
            }
        }
        if err.is_none() {
            let layout = layout.expect("layout");
            if is_stale {
                if !same_bounds {
                    // A stale marker needed the new layout only for its labels.
                    Self::release_nhcb_layout(s, &layout);
                }
                if let Some(p) = &prev {
                    Self::release_nhcb_layout(s, p);
                }
                s.nhcb_series_bounds.remove(&r);
            } else if !same_bounds {
                if let Some(p) = &prev {
                    Self::release_nhcb_layout(s, p);
                }
                s.nhcb_series_bounds.insert(r, layout);
            }
            if let Some(rep) = replacement {
                classic_series.push(rep);
            }
        }
        drop(g);
        if let Some(e) = err {
            self.logger.error("Conversion error", &[("err", &e)]);
            self.dropped(Dropped::Histograms, REASON_NHCB_CONVERSION_ERROR);
            self.data_dropped.incr(1);
            return true;
        }
        if classic_series.len() > 1 {
            // WriteStorage counts the incoming sample as a single item in dataIn;
            // account for the extra classic series produced by NHCB expansion here.
            self.data_in_expanded.incr(classic_series.len() as i64 - 1);
        }
        for item in classic_series {
            if !self.enqueue_with_backoff(r, item, 5_000_000) {
                return false;
            }
        }
        true
    }

    /// Go `AppendHistograms`.
    pub fn append_histograms(&self, histograms: &[RefHistogramSample]) -> bool {
        self.append_histograms_common(histograms.iter().map(|h| (h.r#ref, h.st, h.t, HistArc::Int(&h.h))), false)
    }

    /// Go `AppendFloatHistograms`.
    pub fn append_float_histograms(&self, float_histograms: &[RefFloatHistogramSample]) -> bool {
        self.append_histograms_common(float_histograms.iter().map(|h| (h.r#ref, h.st, h.t, HistArc::Float(&h.fh))), true)
    }

    fn append_histograms_common<'a>(&self, items: impl Iterator<Item = (HeadSeriesRef, i64, i64, HistArc<'a>)>, float: bool) -> bool {
        let send_native = self.send_native_histograms.load(Ordering::Relaxed);
        let convert = self.convert_nhcb_to_classic.load(Ordering::Relaxed);
        if !send_native && !convert {
            return true;
        }
        let now = now_ns();
        let limit = self.cfg.sample_age_limit.0;
        for (r, st, t, h) in items {
            let is_nhcb = h.schema() == CUSTOM_BUCKETS_SCHEMA;
            let is_stale = is_stale_nan(h.sum());
            let g = lock(&self.series);
            let is_tracked_nhcb = g.nhcb_series_bounds.contains_key(&r);
            let convert_nhcb = convert && (is_nhcb || (is_stale && is_tracked_nhcb));
            let transition = convert && is_tracked_nhcb && !is_nhcb && !is_stale;
            if !send_native && !convert_nhcb && !transition {
                continue;
            }
            if is_sample_old_ns(now, limit, t) {
                drop(g);
                self.dropped(Dropped::Histograms, REASON_TOO_OLD);
                continue;
            }
            if !convert && self.proto_msg == ProtoMsg::V1 && is_nhcb {
                drop(g);
                // We cannot send native histograms with custom buckets (NHCB) via remote write v1.
                self.dropped(Dropped::Histograms, REASON_NHCB_NOT_SUPPORTED);
                let msg = if float {
                    "Dropped float native histogram with custom buckets (NHCB) as remote write v1 does not support it"
                } else {
                    "Dropped native histogram with custom buckets (NHCB) as remote write v1 does not support it"
                };
                self.logger.warn(msg, &[("ref", &r.to_string())]);
                continue;
            }
            let Some(lbls) = g.labels.get(&r).cloned() else {
                self.record_unknown(&g, r, Dropped::Histograms, "histogram");
                continue;
            };
            let meta = g.metadata.get(&r).cloned().unwrap_or_default();
            if transition {
                let replacement = send_native.then(|| h.item(lbls.clone(), meta.clone(), st, t));
                if !self.convert_and_enqueue_nhcb(g, r, 0, t, None, true, &lbls, &meta, replacement) {
                    return false;
                }
                continue;
            }
            // Check if `convert_nhcb_to_classic` flag is enabled to convert NHCB histograms to classic histograms.
            if convert_nhcb {
                let nhcb = is_nhcb.then(|| h.data());
                // A stale marker without a layout converts with the series' current layout.
                if !self.convert_and_enqueue_nhcb(g, r, st, t, nhcb, is_stale, &lbls, &meta, None) {
                    return false;
                }
                continue;
            }
            drop(g);
            if !self.enqueue_with_backoff(r, h.item(lbls, meta, st, t), 5_000_000) {
                return false;
            }
        }
        true
    }

    /// Go `Start`: starts sending samples to the remote storage. Does not block.
    pub fn start(self: &Arc<Self>) {
        // Register and initialise some metrics.
        self.metrics.register(self.proto_msg);
        self.metrics.shard_capacity.get().set(self.cfg.capacity as f64);
        self.metrics.max_num_shards.get().set(self.cfg.max_shards as f64);
        self.metrics.min_num_shards.get().set(self.cfg.min_shards as f64);
        self.metrics.desired_num_shards.get().set(self.cfg.min_shards as f64);
        self.metrics.max_samples_per_send.get().set(self.cfg.max_samples_per_send as f64);

        self.shards.start(self.num_shards().max(0) as usize);
        self.watcher.start();
        if let Some(mw) = &self.metadata_watcher {
            mw.start();
        }

        let rx = lock(&self.reshard_rx).take();
        let mut loops = lock(&self.loops);
        let qm = self.clone();
        loops.push(std::thread::Builder::new().name("rw-update-shards".into()).spawn(move || qm.update_shards_loop()).expect("spawn"));
        if let Some(rx) = rx {
            let qm = self.clone();
            loops.push(std::thread::Builder::new().name("rw-reshard".into()).spawn(move || qm.reshard_loop(rx)).expect("spawn"));
        }
    }

    /// Go `Stop`: stops sending samples to the remote storage and waits for pending sends to complete.
    pub fn stop(&self) {
        self.logger.info("Stopping remote storage...", &[]);
        self.quit.set();
        // Wait for all QueueManager routines to end before stopping shards, metadata watcher, and WAL watcher.
        // This is to ensure we don't end up executing a reshard and shards.stop() at the same time.
        let loops: Vec<JoinHandle<()>> = lock(&self.loops).drain(..).collect();
        if !loops.is_empty() {
            let _ = self.reshard_tx.send(None);
        }
        for h in loops {
            let _ = h.join();
        }
        self.shards.stop();
        self.watcher.stop();
        if let Some(mw) = &self.metadata_watcher {
            mw.stop();
        }
        self.metrics.unregister(self.proto_msg);
        self.logger.info("Remote storage stopped.", &[]);
    }

    /// Go `StoreSeries`: keeps track of which series we know about for lookups when sending samples to remote.
    pub fn store_series(&self, series: &[RefSeries], index: i64) {
        let mut g = lock(&self.series);
        let mut seg = lock(&self.series_segment_indexes);
        let s = &mut *g;
        for rs in series {
            // Just make sure all the Refs of Series will insert into seriesSegmentIndexes map for tracking.
            seg.insert(rs.r#ref, index);

            s.builder.reset(rs.labels.clone());
            process_external_labels(&mut s.builder, &self.external_labels);
            let keep = relabel::process_builder(&mut s.builder, s.relabel_configs.iter());
            if !keep {
                s.dropped.insert(rs.r#ref);
                continue;
            }
            let lbls = s.builder.labels();
            s.labels.insert(rs.r#ref, lbls);
        }
    }

    /// Go `StoreMetadata`: keeps track of known series' metadata for lookups when sending samples to remote.
    pub fn store_metadata(&self, meta: &[RefMetadata]) {
        if self.proto_msg == ProtoMsg::V1 {
            return;
        }
        let mut g = lock(&self.series);
        for m in meta {
            let h = Handle::make(Metadata { typ: to_metric_type(m.r#type), unit: m.unit.clone(), help: m.help.clone() });
            g.metadata.insert(m.r#ref, h);
        }
    }

    /// Go `UpdateSeriesSegment`: updates the segment number held against the series, so we can trim older ones
    /// in `series_reset`.
    pub fn update_series_segment(&self, series: &[RefSeries], index: i64) {
        let mut seg = lock(&self.series_segment_indexes);
        for s in series {
            seg.insert(s.r#ref, index);
        }
    }

    /// Go `SeriesReset`: used when reading a checkpoint; deletes the series of segments older than `index`.
    pub fn series_reset(&self, index: i64) {
        let mut g = lock(&self.series);
        let mut seg = lock(&self.series_segment_indexes);
        let s = &mut *g;
        // Check for series that are in segments older than the checkpoint
        // that were not also present in the checkpoint.
        seg.retain(|k, v| {
            if *v >= index {
                return true;
            }
            s.labels.remove(k);
            s.metadata.remove(k);
            s.dropped.remove(k);
            if let Some(layout) = s.nhcb_series_bounds.remove(k) {
                Self::release_nhcb_layout(s, &layout);
            }
            false
        });
    }

    /// Go `SetClient`: updates the client used by a queue. Used when only client specific fields are updated
    /// to avoid restarting the queue.
    pub fn set_client(&self, c: Arc<dyn WriteClient>) {
        *self.store_client.write().unwrap_or_else(|e| e.into_inner()) = c;
    }

    /// The current client.
    pub fn client(&self) -> Arc<dyn WriteClient> {
        self.store_client.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn update_shards_loop(&self) {
        let mut next = Instant::now() + SHARD_UPDATE_DURATION;
        loop {
            let now = Instant::now();
            if now < next && self.quit.wait_timeout(next - now) {
                return;
            }
            if self.quit.is_set() {
                return;
            }
            next += SHARD_UPDATE_DURATION;
            let desired_shards = self.calculate_desired_shards();
            if !self.should_reshard(desired_shards) {
                continue;
            }
            // Resharding can take some time, and we want this loop
            // to stay close to shardUpdateDuration.
            match self.reshard_tx.try_send(Some(desired_shards)) {
                Ok(()) => {
                    self.logger.info(
                        "Remote storage resharding",
                        &[("from", &self.num_shards().to_string()), ("to", &desired_shards.to_string())],
                    );
                    self.set_num_shards(desired_shards);
                }
                Err(TrySendError::Full(_)) => self.logger.info("Currently resharding, skipping.", &[]),
                Err(TrySendError::Disconnected(_)) => return,
            }
        }
    }

    /// Go `shouldReshard`.
    pub fn should_reshard(&self, desired_shards: i64) -> bool {
        if desired_shards == self.num_shards() {
            return false;
        }
        // We shouldn't reshard if Prometheus hasn't been able to send
        // since the last time it checked if it should reshard.
        let min_send_timestamp = (now_ns() - SHARD_UPDATE_DURATION.as_nanos() as i128).div_euclid(1_000_000_000) as i64;
        let lsts = self.last_send_timestamp.load(Ordering::SeqCst);
        if lsts < min_send_timestamp {
            self.logger.warn(
                "Skipping resharding, last successful send was beyond threshold",
                &[("lastSendTimestamp", &lsts.to_string()), ("minSendTimestamp", &min_send_timestamp.to_string())],
            );
            return false;
        }
        let disable_timestamp = self.reshard_disable_end_timestamp.load(Ordering::SeqCst);
        if unix_now() < disable_timestamp {
            let disabled_at = self.reshard_disable_start_timestamp.load(Ordering::SeqCst);
            let disabled_for = disable_timestamp - unix_now();
            self.logger.warn(
                "Skipping resharding, resharding is disabled while waiting for recoverable errors",
                &[("disabled_at", &disabled_at.to_string()), ("disabled_for", &format!("{disabled_for}s"))],
            );
            return false;
        }
        true
    }

    /// Go `calculateDesiredShards`: the number of shards wanted, which is the current number when resharding
    /// should not occur.
    pub fn calculate_desired_shards(&self) -> i64 {
        self.data_out.tick();
        self.data_dropped.tick();
        self.data_out_duration.tick();
        self.data_in_expanded.tick();

        // We use the number of incoming samples as a prediction of how much work we
        // will need to do next iteration.  We add to this any pending samples
        // (received - send) so we can catch up with any backlog. We use the average
        // outgoing batch latency to work out how many shards we need.
        let data_in_rate = self.data_in.rate() + self.data_in_expanded.rate();
        let data_out_rate = self.data_out.rate();
        let data_kept_ratio = data_out_rate / (self.data_dropped.rate() + data_out_rate);
        let data_out_duration = self.data_out_duration.rate() / 1e9;
        let data_pending_rate = data_in_rate * data_kept_ratio - data_out_rate;
        let highest_sent = self.metrics.highest_sent_timestamp.get();
        let highest_recv = self.highest_recv_timestamp.get();
        let delay = (highest_recv - highest_sent).max(0.0); // Delay must never be negative.
        let data_pending = delay * data_in_rate * data_kept_ratio;

        let num_shards = self.num_shards();
        if data_out_rate <= 0.0 {
            return num_shards;
        }

        // When behind we will try to catch up on 5% of samples per second.
        let backlog_catchup = 0.05 * data_pending;
        // Calculate Time to send one sample, averaged across all sends done this tick.
        let time_per_sample = data_out_duration / data_out_rate;
        let mut desired_shards = time_per_sample * (data_in_rate * data_kept_ratio + backlog_catchup);
        self.metrics.desired_num_shards.get().set(desired_shards);
        self.logger.debug(
            "QueueManager.calculateDesiredShards",
            &[
                ("dataInRate", &data_in_rate.to_string()),
                ("dataOutRate", &data_out_rate.to_string()),
                ("dataKeptRatio", &data_kept_ratio.to_string()),
                ("dataPendingRate", &data_pending_rate.to_string()),
                ("dataPending", &data_pending.to_string()),
                ("dataOutDuration", &data_out_duration.to_string()),
                ("timePerSample", &time_per_sample.to_string()),
                ("desiredShards", &desired_shards.to_string()),
                ("highestSent", &highest_sent.to_string()),
                ("highestRecv", &highest_recv.to_string()),
            ],
        );

        // Changes in the number of shards must be greater than shardToleranceFraction.
        let lower_bound = num_shards as f64 * (1.0 - SHARD_TOLERANCE_FRACTION);
        let upper_bound = num_shards as f64 * (1.0 + SHARD_TOLERANCE_FRACTION);

        desired_shards = desired_shards.ceil(); // Round up to be on the safe side.
        if lower_bound <= desired_shards && desired_shards <= upper_bound {
            return num_shards;
        }

        let mut n = desired_shards as i64;
        // Do not downshard if we are more than ten seconds back.
        if n < num_shards && delay > 10.0 {
            self.logger.debug("Not downsharding due to being too far behind", &[]);
            return num_shards;
        }

        if n > self.cfg.max_shards {
            n = self.cfg.max_shards;
        } else if n < self.cfg.min_shards {
            n = self.cfg.min_shards;
        }
        n
    }

    fn reshard_loop(&self, rx: Receiver<Option<i64>>) {
        while let Ok(Some(num_shards)) = rx.recv() {
            // We start the newShards after we have stopped (the therefore completely
            // flushed) the oldShards, to guarantee we only every deliver samples in
            // order.
            self.shards.stop();
            self.shards.start(num_shards.max(0) as usize);
        }
    }

    /// Go `updateMetrics` (of `shards`).
    fn update_metrics(&self, err: &Option<Error>, sc: SendBatchContext, rs: WriteResponseStats, duration: Duration) {
        // Partial errors may happen -- account for that.
        let sample_diff = sc.sample_count as i64 - rs.samples;
        if sample_diff > 0 {
            self.metrics.failed_samples_total.get().add(sample_diff as f64);
        }
        let histogram_diff = sc.histogram_count as i64 - rs.histograms;
        if histogram_diff > 0 {
            self.metrics.failed_histograms_total.get().add(histogram_diff as f64);
        }
        let exemplar_diff = sc.exemplar_count as i64 - rs.exemplars;
        if exemplar_diff > 0 {
            self.metrics.failed_exemplars_total.get().add(exemplar_diff as f64);
        }
        if let Some(e) = err {
            self.logger.error(
                "non-recoverable error",
                &[
                    ("failedSampleCount", &sample_diff.to_string()),
                    ("failedHistogramCount", &histogram_diff.to_string()),
                    ("failedExemplarCount", &exemplar_diff.to_string()),
                    ("err", &e.to_string()),
                ],
            );
        } else if sample_diff + exemplar_diff + histogram_diff > 0 {
            self.logger.error(
                "we got 2xx status code from the Receiver yet statistics indicate some data was not written; investigation needed",
                &[
                    ("failedSampleCount", &sample_diff.to_string()),
                    ("failedHistogramCount", &histogram_diff.to_string()),
                    ("failedExemplarCount", &exemplar_diff.to_string()),
                ],
            );
        }

        // These counters are used to calculate the dynamic sharding, and as such
        // should be maintained irrespective of success or failure.
        // Count only queueable items so dataOut remains comparable with dataIn.
        self.data_out.incr((sc.sample_count + sc.exemplar_count + sc.histogram_count) as i64);
        self.data_out_duration.incr(duration.as_nanos() as i64);
        self.last_send_timestamp.store(unix_now(), Ordering::SeqCst);

        // Pending samples/exemplars/histograms also should be subtracted, as an error means
        // they will not be retried.
        self.metrics.c_pending_samples.sub(sc.sample_count as f64);
        self.metrics.c_pending_exemplars.sub(sc.exemplar_count as f64);
        self.metrics.c_pending_histograms.sub(sc.histogram_count as f64);
        self.shards.enqueued_samples.fetch_sub(sc.sample_count as i64, Ordering::SeqCst);
        self.shards.enqueued_exemplars.fetch_sub(sc.exemplar_count as i64, Ordering::SeqCst);
        self.shards.enqueued_histograms.fetch_sub(sc.histogram_count as i64, Ordering::SeqCst);
    }

    /// Go `sendWriteRequestWithBackoff`: calls `attempt` until it succeeds, fails with an error that is not
    /// recoverable, or `ctx` is cancelled.
    pub fn send_write_request_with_backoff(
        &self,
        ctx: &Ctx,
        attempt: &mut dyn FnMut(i32) -> Option<Error>,
        on_retry: &mut dyn FnMut(),
    ) -> Option<Error> {
        let mut backoff = self.cfg.min_backoff.0;
        let mut try_ = 0;
        loop {
            if ctx.is_done() {
                return ctx.err();
            }
            let err = attempt(try_)?;

            // If the error is unrecoverable, we should not retry.
            let Some(backoff_err) = as_recoverable(&err) else {
                return Some(err);
            };

            let mut sleep_duration = backoff;
            if backoff_err.retry_after > 0 {
                sleep_duration = backoff_err.retry_after;
                self.logger.info(
                    "Retrying after duration specified by Retry-After header",
                    &[("duration", &format!("{}ns", sleep_duration))],
                );
            } else if backoff_err.retry_after < 0 {
                self.logger.debug("retry-after cannot be in past, retrying using default backoff mechanism", &[]);
            }

            // We should never reshard for a recoverable error; increasing shards could
            // make the problem worse, particularly if we're getting rate limited.
            //
            // reshardDisableTimestamp holds the unix timestamp until which resharding
            // is disabled. We'll update that timestamp if the period we were just told
            // to sleep for is newer than the existing disabled timestamp.
            let reshard_wait_period = (now_ns() + sleep_duration as i128 * 2).div_euclid(1_000_000_000) as i64;
            let (old_ts, updated) = set_atomic_to_newer(&self.reshard_disable_end_timestamp, reshard_wait_period);
            if updated {
                // If the old timestamp was in the past, then resharding was previously
                // enabled. We want to track the time where it initially got disabled for
                // logging purposes.
                let disable_time = unix_now();
                if old_ts < disable_time {
                    self.reshard_disable_start_timestamp.store(disable_time, Ordering::SeqCst);
                }
            }

            if sleep_duration > 0 {
                ctx.sleep(Duration::from_nanos(sleep_duration as u64));
            }

            // If we make it this far, we've encountered a recoverable error and will retry.
            on_retry();
            self.logger.warn("Failed to send batch, retrying", &[("err", &err.to_string())]);

            backoff = (sleep_duration * 2).min(self.cfg.max_backoff.0);
            try_ += 1;
        }
    }

    /// The `Weak` handle of this queue manager.
    pub fn weak(&self) -> Weak<QueueManager> {
        self.me.clone()
    }
}
