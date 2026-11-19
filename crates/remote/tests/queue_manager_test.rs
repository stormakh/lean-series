//! Port of `storage/remote/queue_manager_test.go`.
//!
//! adapted (whole file): Go's `synctest` fake clock is not available; tests that use it run on the real clock
//! and wait for the condition `synctest.Wait` would reach (with a timeout) instead.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use config::{
    Config, MetadataConfig, QueueConfig, RemoteWriteConfig, WriteMessageType, default_global_config,
    default_metadata_config, default_queue_config, default_remote_write_config,
};
use discovery::metrics::{MetricType as FamilyType, Opts, Registry};
use model::histogram::{CUSTOM_BUCKETS_SCHEMA, FloatHistogram, Histogram, Span, custom_values};
use model::labels::{self, Builder, Label, Labels, ScratchBuilder, ValidationScheme, format_open_metrics_float};
use model::metadata::{Handle, Metadata, MetricType};
use model::relabel::{self, Action, Regexp};
use model::schema;
use model::value::{is_stale_nan, stale_nan};
use remote::client::{RecoverableError, WriteClient};
use remote::ewma::EwmaRate;
use remote::intern::{Pool, no_reference_releases};
use remote::max_timestamp::MaxTimestamp;
use remote::prompb::{self, Message, writev2};
use remote::queue_manager::*;
use remote::stats::WriteResponseStats;
use remote::write::{WriteStorage, to_hash};
use scrape::target::MetricMetadata;
use storage::{AppendV2Options, AppendableV2, Context, ERR_DEADLINE_EXCEEDED, Error};
use tsdb_format::chunks::HeadSeriesRef;
use tsdb_format::record::{
    MetricType as RecMetricType, RefExemplar, RefFloatHistogramSample, RefHistogramSample, RefMetadata, RefSample,
    RefSeries, to_metric_type,
};
use yaml::{Duration as YDuration, Url};

const DEFAULT_FLUSH_DEADLINE: Duration = Duration::from_secs(60);
const V1: ProtoMsg = ProtoMsg::V1;
const V2: ProtoMsg = ProtoMsg::V2;
const PROTOS: [ProtoMsg; 2] = [V1, V2];

// ---------------------------------------------------------------------------------------------------------------
// Helpers.

fn temp_dir() -> PathBuf {
    use std::sync::atomic::AtomicUsize;
    static N: AtomicUsize = AtomicUsize::new(0);
    let d = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../bench/tmp/w3-remote/qm")
        .join(format!("{}-{}", std::process::id(), N.fetch_add(1, Ordering::SeqCst)));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

fn new_highest_timestamp_metric() -> Arc<MaxTimestamp> {
    Arc::new(MaxTimestamp::new(Opts::new(
        "prometheus_remote_storage_highest_timestamp_in_seconds",
        "Highest timestamp that has come into the remote storage via the Appender interface, in seconds since epoch. Initialized to 0 when no data has been received yet",
    )))
}

fn ydur(d: Duration) -> YDuration {
    YDuration(d.as_nanos() as i64)
}

fn counter(v: &discovery::metrics::CounterVec) -> f64 {
    v.get().get()
}

fn dropped(v: &discovery::metrics::CounterVec, reason: &str) -> f64 {
    v.with_label_values(&[reason]).get()
}

/// Go `testwal.RecordsCase`.
#[derive(Default)]
struct RecCase {
    #[allow(dead_code)]
    name: &'static str,
    series: usize,
    samples_per_series: usize,
    histograms_per_series: usize,
    float_histograms_per_series: usize,
    exemplars_per_series: usize,
    ts_fn: Option<Box<dyn Fn(usize, usize) -> i64>>,
    labels_fn: Option<Box<dyn FnMut(&mut ScratchBuilder, usize) -> Labels>>,
    no_st: bool,
}

/// Go `testwal.Records`.
#[derive(Default, Clone)]
struct Records {
    series: Vec<RefSeries>,
    samples: Vec<RefSample>,
    histograms: Vec<RefHistogramSample>,
    float_histograms: Vec<RefFloatHistogramSample>,
    exemplars: Vec<RefExemplar>,
    metadata: Vec<RefMetadata>,
}

/// Go `testwal.newTestHist`.
fn new_test_hist(i: usize) -> Histogram {
    Histogram {
        schema: 2,
        zero_threshold: 1e-128,
        zero_count: 0,
        count: 2,
        sum: 0.0,
        positive_spans: vec![Span { offset: 0, length: 1 }],
        positive_buckets: vec![i as i64 + 1],
        negative_spans: vec![Span { offset: 0, length: 1 }],
        negative_buckets: vec![-(i as i64) - 1],
        ..Default::default()
    }
}

/// Go `testwal.GenerateRecords`.
fn generate_records(mut c: RecCase) -> Records {
    let mut ret = Records::default();
    let st_for_ts = |ts: i64| if c.no_st { 0 } else { ts - 1 };
    let mut lb = ScratchBuilder::new(1);
    for i in 0..c.series {
        let r = i;
        let hr = r as HeadSeriesRef;
        let lbls = match c.labels_fn.as_mut() {
            Some(f) => f(&mut lb, r),
            None => labels::from_strings(&["__name__", &format!("test_metric_{r}")]),
        };
        ret.series.push(RefSeries { r#ref: hr, labels: lbls });
        ret.metadata.push(RefMetadata {
            r#ref: hr,
            r#type: RecMetricType::Counter as u8,
            unit: "unit text".to_string(),
            help: format!("help text for {r}"),
        });
        let ts_fn = |r: usize, j: usize| match &c.ts_fn {
            Some(f) => f(r, j),
            None => j as i64,
        };
        for j in 0..c.samples_per_series {
            let ts = ts_fn(r, j);
            ret.samples.push(RefSample { r#ref: hr, st: st_for_ts(ts), t: ts, v: r as f64 });
        }
        let h = Arc::new(new_test_hist(r));
        for j in 0..c.histograms_per_series {
            let ts = ts_fn(r, j);
            ret.histograms.push(RefHistogramSample { r#ref: hr, st: st_for_ts(ts), t: ts, h: h.clone() });
        }
        for j in 0..c.float_histograms_per_series {
            let ts = ts_fn(r, j);
            ret.float_histograms.push(RefFloatHistogramSample { r#ref: hr, st: st_for_ts(ts), t: ts, fh: Arc::new(h.to_float()) });
        }
        for j in 0..c.exemplars_per_series {
            ret.exemplars.push(RefExemplar {
                r#ref: hr,
                t: ts_fn(r, j),
                v: r as f64,
                labels: labels::from_strings(&["trace_id", &format!("trace-{r}")]),
            });
        }
    }
    ret
}

fn get_series_id_from_ref(r: &RefSeries) -> String {
    r.labels.to_string()
}

/// Go `TestWriteClient`: re-implements a fake write handler.
#[derive(Default)]
struct TwcState {
    received_samples: HashMap<String, Vec<writev2::Sample>>,
    expected_samples: HashMap<String, Vec<writev2::Sample>>,
    received_exemplars: HashMap<String, Vec<prompb::Exemplar>>,
    expected_exemplars: HashMap<String, Vec<prompb::Exemplar>>,
    received_histograms: HashMap<String, Vec<writev2::Histogram>>,
    received_float_histograms: HashMap<String, Vec<writev2::Histogram>>,
    expected_histograms: HashMap<String, Vec<writev2::Histogram>>,
    expected_float_histograms: HashMap<String, Vec<writev2::Histogram>>,
    received_metadata: HashMap<String, Vec<prompb::MetricMetadata>>,
    expected_metadata: HashMap<String, Vec<prompb::MetricMetadata>>,
    writes_received: usize,
    injected_errs: Vec<Option<Error>>,
    curr_err: i64,
    store_wait: Duration,
    return_error: Option<Error>,
}

struct TestWriteClient {
    mtx: Mutex<TwcState>,
    proto_msg: ProtoMsg,
}

fn deep_len<M>(ms: &[&HashMap<String, Vec<M>>]) -> usize {
    ms.iter().map(|m| m.values().map(|v| v.len()).sum::<usize>()).sum()
}

impl TestWriteClient {
    fn new(proto_msg: ProtoMsg) -> Arc<TestWriteClient> {
        Arc::new(TestWriteClient { mtx: Mutex::new(TwcState::default()), proto_msg })
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, TwcState> {
        self.mtx.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn inject_errors(&self, errs: Vec<Option<Error>>) {
        let mut s = self.lock();
        s.injected_errs = errs;
        s.curr_err = -1;
    }
    fn expect_samples(&self, ss: &[RefSample], series: &[RefSeries]) {
        let mut c = self.lock();
        c.expected_samples.clear();
        c.received_samples.clear();
        for s in ss {
            let id = get_series_id_from_ref(&series[s.r#ref as usize]);
            c.expected_samples.entry(id).or_default().push(writev2::Sample { start_timestamp: s.st, timestamp: s.t, value: s.v });
        }
    }
    fn expect_exemplars(&self, ss: &[RefExemplar], series: &[RefSeries]) {
        let mut c = self.lock();
        c.expected_exemplars.clear();
        c.received_exemplars.clear();
        for s in ss {
            let id = get_series_id_from_ref(&series[s.r#ref as usize]);
            c.expected_exemplars.entry(id).or_default().push(prompb::Exemplar {
                labels: prompb::labels_to_label_protos(&s.labels),
                timestamp: s.t,
                value: s.v,
            });
        }
    }
    fn expect_histograms(&self, hh: &[RefHistogramSample], series: &[RefSeries]) {
        let mut c = self.lock();
        c.expected_histograms.clear();
        c.received_histograms.clear();
        for h in hh {
            let id = get_series_id_from_ref(&series[h.r#ref as usize]);
            c.expected_histograms.entry(id).or_default().push(writev2::from_int_histogram(h.st, h.t, &h.h));
        }
    }
    fn expect_float_histograms(&self, fhs: &[RefFloatHistogramSample], series: &[RefSeries]) {
        let mut c = self.lock();
        c.expected_float_histograms.clear();
        c.received_float_histograms.clear();
        for fh in fhs {
            let id = get_series_id_from_ref(&series[fh.r#ref as usize]);
            c.expected_float_histograms.entry(id).or_default().push(writev2::from_float_histogram(fh.st, fh.t, &fh.fh));
        }
    }
    fn expect_metadata_for_batch(
        &self,
        metadata: &[RefMetadata],
        series: &[RefSeries],
        samples: &[RefSample],
        exemplars: &[RefExemplar],
        histograms: &[RefHistogramSample],
        float_histograms: &[RefFloatHistogramSample],
    ) {
        let mut c = self.lock();
        c.expected_metadata.clear();
        c.received_metadata.clear();
        // Collect refs that have data in this batch.
        let mut refs_with_data = HashSet::new();
        refs_with_data.extend(samples.iter().map(|s| s.r#ref));
        refs_with_data.extend(exemplars.iter().map(|s| s.r#ref));
        refs_with_data.extend(histograms.iter().map(|s| s.r#ref));
        refs_with_data.extend(float_histograms.iter().map(|s| s.r#ref));
        // Only expect metadata for series that have data in this batch.
        for m in metadata {
            if !refs_with_data.contains(&m.r#ref) {
                continue;
            }
            let id = get_series_id_from_ref(&series[m.r#ref as usize]);
            c.expected_metadata.entry(id.clone()).or_default().push(prompb::MetricMetadata {
                metric_family_name: id,
                r#type: prompb::from_metadata_type(to_metric_type(m.r#type)),
                help: m.help.clone(),
                unit: m.unit.clone(),
            });
        }
    }
    fn add_expected_sample(&self, id: String, s: writev2::Sample) {
        self.lock().expected_samples.entry(id).or_default().push(s);
    }
    fn wait_for_expected_data(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            let (exp, got) = {
                let c = self.lock();
                let exp = deep_len(&[&c.expected_samples])
                    + deep_len(&[&c.expected_exemplars])
                    + deep_len(&[&c.expected_histograms, &c.expected_float_histograms])
                    + c.expected_metadata.len();
                let got = deep_len(&[&c.received_samples])
                    + deep_len(&[&c.received_exemplars])
                    + deep_len(&[&c.received_histograms, &c.received_float_histograms])
                    + if c.received_metadata.is_empty() { 0 } else { c.expected_metadata.len() };
                (exp, got)
            };
            if got >= exp {
                break;
            }
            if Instant::now() >= deadline {
                // Go `tb.Error`: reported, then the comparisons below run.
                eprintln!("expected {exp} samples/exemplars/histograms/floathistograms/metadata, got {got}");
                break;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        let c = self.lock();
        for (ts, e) in &c.expected_samples {
            assert_eq!(Some(e), c.received_samples.get(ts), "{ts}");
        }
        for (ts, e) in &c.expected_exemplars {
            assert_eq!(Some(e), c.received_exemplars.get(ts), "{ts}");
        }
        for (ts, e) in &c.expected_histograms {
            assert_eq!(Some(e), c.received_histograms.get(ts), "{ts}");
        }
        for (ts, e) in &c.expected_float_histograms {
            assert_eq!(Some(e), c.received_float_histograms.get(ts), "{ts}");
        }
        for (ts, e) in &c.expected_metadata {
            let got = c.received_metadata.get(ts);
            assert!(got.is_some_and(|g| !g.is_empty()), "No metadata received for series {ts}");
            // For metadata, we only check that we got at least one entry with the right content
            // since v2 protocol sends metadata with each data point
            assert_eq!(e[0], got.unwrap()[0], "{ts}");
        }
    }
    fn set_store_wait(&self, w: Duration) {
        self.lock().store_wait = w;
    }
    fn set_return_error(&self, e: Option<Error>) {
        self.lock().return_error = e;
    }
}

/// Go `v2RequestToWriteRequest`.
fn v2_request_to_write_request(v2: &writev2::Request) -> Result<prompb::WriteRequest, String> {
    let mut req = prompb::WriteRequest { timeseries: vec![Default::default(); v2.timeseries.len()], metadata: vec![] };
    let mut b = ScratchBuilder::new(0);
    for (i, rts) in v2.timeseries.iter().enumerate() {
        let lbls = rts.to_labels(&mut b, &v2.symbols).map_err(|e| format!("failed to convert labels: {e}"))?;
        for (n, v) in lbls.iter() {
            req.timeseries[i].labels.push(prompb::Label::new(n, v));
        }
        let mut exemplars = vec![prompb::Exemplar::default(); rts.exemplars.len()];
        for (j, e) in rts.exemplars.iter().enumerate() {
            exemplars[j].value = e.value;
            exemplars[j].timestamp = e.timestamp;
            let ex = e.to_exemplar(&mut b, &v2.symbols).map_err(|e| format!("failed to convert exemplar: {e}"))?;
            for (n, v) in ex.labels.iter() {
                exemplars[j].labels.push(prompb::Label::new(n, v));
            }
        }
        req.timeseries[i].exemplars = exemplars;
        req.timeseries[i].samples =
            rts.samples.iter().map(|s| prompb::Sample { timestamp: s.timestamp, value: s.value }).collect();
        req.timeseries[i].histograms = rts
            .histograms
            .iter()
            .map(|h| {
                if h.is_float_histogram() {
                    prompb::from_float_histogram(h.timestamp, &h.to_float_histogram())
                } else {
                    prompb::from_int_histogram(h.timestamp, &h.to_int_histogram().unwrap())
                }
            })
            .collect();
        // Convert v2 metadata to v1 format.
        if rts.metadata.r#type != writev2::MetadataType::UNSPECIFIED {
            let labels = rts.to_labels(&mut b, &v2.symbols).map_err(|e| format!("failed to convert metadata labels: {e}"))?;
            let md = rts.to_metadata(&v2.symbols).map_err(|e| format!("failed to convert metadata: {e}"))?;
            req.metadata.push(prompb::MetricMetadata {
                metric_family_name: labels.to_string(),
                r#type: prompb::from_metadata_type(md.typ),
                help: md.help,
                unit: md.unit,
            });
        }
    }
    Ok(req)
}

impl WriteClient for TestWriteClient {
    fn store(&self, _ctx: &Context, req: &[u8], _retry: i32) -> Result<WriteResponseStats, Error> {
        let mut c = self.lock();
        if c.store_wait > Duration::ZERO {
            std::thread::sleep(c.store_wait);
        }
        if let Some(e) = &c.return_error {
            return Err(e.clone());
        }
        let req_buf = snap::raw::Decoder::new().decompress_vec(req).map_err(|e| Error::msg(e.to_string()))?;

        // Check if we've been told to inject err for this call.
        if !c.injected_errs.is_empty() {
            c.curr_err += 1;
            if let Some(e) = c.injected_errs[c.curr_err as usize].clone() {
                return Err(e);
            }
        }

        let (req_proto, req_v2) = match self.proto_msg {
            ProtoMsg::V1 => (prompb::WriteRequest::unmarshal(&req_buf).map_err(|e| Error::msg(e.to_string()))?, None),
            ProtoMsg::V2 => {
                // NOTE(bwplotka): v1 msg can be unmarshaled to v2 sometimes, without errors.
                let v2 = writev2::Request::unmarshal(&req_buf).map_err(|e| Error::msg(e.to_string()))?;
                (v2_request_to_write_request(&v2).map_err(Error::msg)?, Some(v2))
            }
        };

        let mut rs = WriteResponseStats::default();
        let mut b = ScratchBuilder::new(0);
        for (i, ts) in req_proto.timeseries.iter().enumerate() {
            let id = ts.to_labels(&mut b).to_string();
            for (j, s) in ts.samples.iter().enumerate() {
                let st = req_v2.as_ref().map_or(0, |v| v.timeseries[i].samples[j].start_timestamp);
                c.received_samples.entry(id.clone()).or_default().push(writev2::Sample {
                    start_timestamp: st,
                    timestamp: s.timestamp,
                    value: s.value,
                });
            }
            rs.samples += ts.samples.len() as i64;
            if !ts.exemplars.is_empty() {
                c.received_exemplars.entry(id.clone()).or_default().extend(ts.exemplars.iter().cloned());
            }
            rs.exemplars += ts.exemplars.len() as i64;
            for (j, h) in ts.histograms.iter().enumerate() {
                let st = req_v2.as_ref().map_or(0, |v| v.timeseries[i].histograms[j].start_timestamp);
                if h.is_float_histogram() {
                    let fh = h.to_float_histogram();
                    c.received_float_histograms.entry(id.clone()).or_default().push(writev2::from_float_histogram(st, h.timestamp, &fh));
                } else {
                    let ih = h.to_int_histogram().unwrap();
                    c.received_histograms.entry(id.clone()).or_default().push(writev2::from_int_histogram(st, h.timestamp, &ih));
                }
            }
            rs.histograms += ts.histograms.len() as i64;
        }
        for m in req_proto.metadata {
            c.received_metadata.entry(m.metric_family_name.clone()).or_default().push(m);
        }
        c.writes_received += 1;
        Ok(rs)
    }
    fn name(&self) -> &str {
        "testwriteclient"
    }
    fn endpoint(&self) -> &str {
        "http://test-remote.com/1234"
    }
}

/// Go `TestBlockingWriteClient`: blocks until the request's context is cancelled.
#[derive(Default)]
struct TestBlockingWriteClient {
    num_calls: AtomicU64,
}

impl WriteClient for TestBlockingWriteClient {
    fn store(&self, ctx: &Context, _req: &[u8], _retry: i32) -> Result<WriteResponseStats, Error> {
        self.num_calls.fetch_add(1, Ordering::SeqCst);
        while !ctx.is_done() {
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(WriteResponseStats::default())
    }
    fn name(&self) -> &str {
        "testblockingwriteclient"
    }
    fn endpoint(&self) -> &str {
        "http://test-remote-blocking.com/1234"
    }
}

/// Go `NopWriteClient`.
struct NopWriteClient;

impl WriteClient for NopWriteClient {
    fn store(&self, _ctx: &Context, _req: &[u8], _retry: i32) -> Result<WriteResponseStats, Error> {
        Ok(WriteResponseStats::default())
    }
    fn name(&self) -> &str {
        "nopwriteclient"
    }
    fn endpoint(&self) -> &str {
        "http://test-remote.com/1234"
    }
}

type StoreFn = Box<dyn Fn(&Context, &[u8], i32) -> Result<WriteResponseStats, Error> + Send + Sync>;

/// Go `MockWriteClient`.
struct MockWriteClient {
    store_func: StoreFn,
    name: String,
    endpoint: String,
}

impl WriteClient for MockWriteClient {
    fn store(&self, ctx: &Context, req: &[u8], n: i32) -> Result<WriteResponseStats, Error> {
        (self.store_func)(ctx, req, n)
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

fn new_test_queue_manager(
    cfg: QueueConfig,
    mcfg: MetadataConfig,
    deadline: Duration,
    c: Arc<dyn WriteClient>,
    proto_msg: ProtoMsg,
) -> Arc<QueueManager> {
    let dir = temp_dir();
    let metrics = QueueManagerMetrics::new(None, "", "");
    QueueManager::new(
        metrics,
        None,
        None,
        None,
        &dir,
        Arc::new(EwmaRate::new(EWMA_WEIGHT, SHARD_UPDATE_DURATION)),
        cfg,
        mcfg,
        &Labels::empty(),
        vec![],
        c,
        deadline,
        Arc::new(Pool::new()),
        new_highest_timestamp_metric(),
        None,
        false,
        false,
        false,
        proto_msg,
        None,
        false,
        false,
    )
}

fn new_test_client_and_queue_manager(flush_deadline: Duration, proto_msg: ProtoMsg) -> (Arc<TestWriteClient>, Arc<QueueManager>) {
    let c = TestWriteClient::new(proto_msg);
    let m = new_test_queue_manager(default_queue_config(), default_metadata_config(), flush_deadline, c.clone(), proto_msg);
    (c, m)
}

fn test_default_queue_config() -> QueueConfig {
    let mut cfg = default_queue_config();
    // For faster unit tests we don't wait default 5 seconds.
    cfg.batch_send_deadline = YDuration::from_millis(100);
    cfg
}

/// Go `baseRemoteWriteConfig`.
fn base_remote_write_config(host: &str) -> RemoteWriteConfig {
    let mut cfg = default_remote_write_config();
    cfg.url = Url::parse(host).unwrap();
    cfg
}

fn wm(p: ProtoMsg) -> WriteMessageType {
    WriteMessageType(p.as_str().to_string())
}

/// adapted: Go tests use `NewStorage` (storage.go) and read `s.rws`; here the `WriteStorage` is used directly,
/// with clients that are replaced by `set_client` before any data is sent.
fn new_write_storage(dir: &PathBuf) -> Arc<WriteStorage> {
    WriteStorage::new(None, None, dir, DEFAULT_FLUSH_DEADLINE, None, false, None)
}

/// Polls `f` until it is true or `timeout` passed (Go `require.Eventually`).
fn eventually(timeout: Duration, tick: Duration, mut f: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if f() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(tick);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Tests.

#[test]
fn test_basic_content_negotiation() {
    let mut queue_config = default_queue_config();
    queue_config.batch_send_deadline = YDuration::from_millis(100);
    queue_config.max_shards = 1;

    // We need to set URL's so that metric creation doesn't panic.
    let mut write_config = base_remote_write_config("http://test-storage.com");
    write_config.queue_config = queue_config;

    let pretend_500 = || Some(RecoverableError::new(Error::msg("pretend 500"), 1));
    let unrecoverable = || Some(Error::msg("pretend unrecoverable err"));
    let cases: Vec<(&str, ProtoMsg, ProtoMsg, Vec<Option<Error>>, bool)> = vec![
        ("v2 happy path", V2, V2, vec![None], false),
        ("v1 happy path", V1, V1, vec![None], false),
        // Test a case where the v1 request has a temporary delay but goes through on retry.
        ("v1 happy path with one 5xx retry", V1, V1, vec![pretend_500(), None], false),
        // Repeat the above test but with v2. The request has a temporary delay but goes through on retry.
        ("v2 happy path with one 5xx retry", V2, V2, vec![pretend_500(), None], false),
        // A few error cases of v2 talking to v1.
        ("v2 talks to v1 that gives 400 or 415", V2, V1, vec![unrecoverable()], true),
        // We detect this thanks to https://github.com/prometheus/prometheus/issues/14359
        ("v2 talks to (broken) v1 that tries to unmarshal v2 payload with v1 proto", V2, V1, vec![None], true),
        // Opposite, v1 talking to v2 only server.
        ("v1 talks to v2 that gives 400 or 415", V1, V2, vec![unrecoverable()], true),
    ];
    for (name, sender, receiver, inject_errs, expect_fail) in cases {
        let dir = temp_dir();
        let s = new_write_storage(&dir);

        let recs = generate_records(RecCase { no_st: sender == V1, series: 1, samples_per_series: 1, ..Default::default() });

        write_config.protobuf_message = wm(sender);
        let conf = Config {
            global_config: default_global_config(),
            remote_write_configs: vec![write_config.clone()],
            ..Default::default()
        };
        s.apply_config(&conf).unwrap();
        let hash = to_hash(&write_config).unwrap();
        let qm = s.queue(&hash).unwrap();

        let c = TestWriteClient::new(receiver);
        c.inject_errors(inject_errs.clone());
        qm.set_client(c.clone());

        qm.store_series(&recs.series, 0);
        qm.store_metadata(&recs.metadata);

        // Do we expect some data back?
        if !expect_fail {
            c.expect_samples(&recs.samples, &recs.series);
        } else {
            c.expect_samples(&[], &[]);
        }

        // Schedule send.
        qm.append(&recs.samples);

        if !expect_fail {
            // No error expected, so wait for data.
            c.wait_for_expected_data(Duration::from_secs(5));
            assert_eq!(0.0, counter(&qm.metrics.failed_samples_total), "{name}");
        } else {
            // Wait for failure to be recorded in metrics.
            assert!(
                eventually(Duration::from_secs(5), Duration::from_millis(500), || counter(&qm.metrics.failed_samples_total) == 1.0),
                "{name}: expected one sample failed in qm metrics; got {}",
                counter(&qm.metrics.failed_samples_total)
            );
        }

        // samplesTotal means attempts.
        assert_eq!(inject_errs.len() as f64, counter(&qm.metrics.samples_total), "{name}");
        assert_eq!((inject_errs.len() - 1) as f64, counter(&qm.metrics.retried_samples_total), "{name}");
        s.close().unwrap();
    }
}

#[test]
fn test_sample_delivery() {
    let n = 3;

    let mut queue_config = default_queue_config();
    queue_config.batch_send_deadline = YDuration::from_millis(100);
    queue_config.max_shards = 1;

    // We need to set URL's so that metric creation doesn't panic.
    let mut write_config = base_remote_write_config("http://test-storage.com");
    write_config.queue_config = queue_config.clone();
    write_config.send_exemplars = true;
    write_config.send_native_histograms = true;

    let mut handles = Vec::new();
    for proto_msg in PROTOS {
        let cases = [
            (n, 0, 0, 0, "samples only"),
            (0, n, 0, 0, "histograms only"),
            (0, 0, n, 0, "float histograms only"),
            (0, 0, 0, n, "exemplars only"),
            (n, n, n, n, "all"),
        ];
        for (sps, hps, fhps, eps, case_name) in cases {
            let mut write_config = write_config.clone();
            handles.push(std::thread::spawn(move || {
                let dir = temp_dir();
                let s = new_write_storage(&dir);
                let recs = generate_records(RecCase {
                    name: case_name,
                    series: n,
                    samples_per_series: sps,
                    histograms_per_series: hps,
                    float_histograms_per_series: fhps,
                    exemplars_per_series: eps,
                    no_st: proto_msg == V1, // RW1 does not support ST.
                    ..Default::default()
                });
                let (series, metadata, samples, exemplars, histograms, float_histograms) =
                    (&recs.series, &recs.metadata, &recs.samples, &recs.exemplars, &recs.histograms, &recs.float_histograms);

                // Apply new config.
                // adapted note: Go changes `queueConfig.Capacity` and `MaxSamplesPerSend` here, but on a copy
                // that is not used any more (the write config already holds its own copy), so they stay default.
                write_config.protobuf_message = wm(proto_msg);
                let conf = Config {
                    global_config: default_global_config(),
                    remote_write_configs: vec![write_config.clone()],
                    ..Default::default()
                };
                s.apply_config(&conf).unwrap();
                let hash = to_hash(&write_config).unwrap();
                let qm = s.queue(&hash).unwrap();

                let c = TestWriteClient::new(proto_msg);
                c.set_store_wait(Duration::from_millis(10));
                qm.set_client(c.clone());

                qm.store_series(series, 0);
                qm.store_metadata(metadata);

                let halves = [
                    (&samples[..samples.len() / 2], &exemplars[..exemplars.len() / 2], &histograms[..histograms.len() / 2], &float_histograms[..float_histograms.len() / 2]),
                    (&samples[samples.len() / 2..], &exemplars[exemplars.len() / 2..], &histograms[histograms.len() / 2..], &float_histograms[float_histograms.len() / 2..]),
                ];
                for (ss, es, hs, fhs) in halves {
                    c.expect_samples(ss, series);
                    c.expect_exemplars(es, series);
                    c.expect_histograms(hs, series);
                    c.expect_float_histograms(fhs, series);
                    if proto_msg == V2 && !metadata.is_empty() {
                        c.expect_metadata_for_batch(metadata, series, ss, es, hs, fhs);
                    }
                    qm.append(ss);
                    qm.append_exemplars(es);
                    qm.append_histograms(hs);
                    qm.append_float_histograms(fhs);
                    c.wait_for_expected_data(Duration::from_secs(30));
                }

                // Ensure the queue metrics match what was sent
                assert_eq!(samples.len() as f64, counter(&qm.metrics.samples_total), "{proto_msg}/{case_name}");
                assert_eq!(exemplars.len() as f64, counter(&qm.metrics.exemplars_total), "{proto_msg}/{case_name}");
                assert_eq!((histograms.len() + float_histograms.len()) as f64, counter(&qm.metrics.histograms_total), "{proto_msg}/{case_name}");
                assert!(qm.metrics.sent_batch_duration.get().sum() >= 0.010, "{proto_msg}/{case_name}");
                s.close().unwrap();
            }));
        }
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_metadata_delivery() {
    let (c, m) = new_test_client_and_queue_manager(DEFAULT_FLUSH_DEADLINE, V1);
    m.start();

    let num_metadata = 1532;
    let metadata: Vec<MetricMetadata> = (0..num_metadata)
        .map(|i| MetricMetadata {
            metric_family: format!("prometheus_remote_storage_sent_metadata_bytes_{i}"),
            typ: MetricType::Counter,
            help: "a nice help text".to_string(),
            unit: String::new(),
        })
        .collect();

    m.append_watcher_metadata(&Ctx::background(), &metadata);

    assert_eq!(num_metadata as f64, counter(&m.metrics.metadata_total));
    assert_eq!(0.0, counter(&m.metrics.failed_metadata_total));
    assert!(counter(&m.metrics.metadata_bytes_total) > 0.0);
    {
        let st = c.lock();
        assert_eq!(num_metadata, st.received_metadata.len());
        // One more write than the rounded quotient should be performed in order to get samples that didn't
        // fit into MaxSamplesPerSend.
        assert_eq!(num_metadata / default_metadata_config().max_samples_per_send as usize + 1, st.writes_received);
        // Make sure the last samples were sent.
        let last = &metadata[metadata.len() - 1].metric_family;
        assert_eq!(&st.received_metadata[last][0].metric_family_name, last);
    }
    m.stop();
}

#[test]
fn test_wal_metadata_delivery() {
    let dir = temp_dir();
    let s = new_write_storage(&dir);

    let mut cfg = default_queue_config();
    cfg.batch_send_deadline = YDuration::from_millis(100);
    cfg.max_shards = 1;

    let mut write_config = base_remote_write_config("http://test-storage.com");
    write_config.queue_config = cfg;
    write_config.protobuf_message = wm(V2);

    let conf = Config { global_config: default_global_config(), remote_write_configs: vec![write_config.clone()], ..Default::default() };

    let n = 3;
    let recs = generate_records(RecCase { series: n, samples_per_series: n, ..Default::default() });

    s.apply_config(&conf).unwrap();
    let hash = to_hash(&write_config).unwrap();
    let qm = s.queue(&hash).unwrap();

    let c = TestWriteClient::new(V2);
    qm.set_client(c.clone());

    qm.store_series(&recs.series, 0);
    qm.store_metadata(&recs.metadata);

    assert_eq!(n, qm.series_labels_len());
    assert_eq!(n, qm.series_metadata_len());

    c.expect_samples(&recs.samples, &recs.series);
    c.expect_metadata_for_batch(&recs.metadata, &recs.series, &recs.samples, &[], &[], &[]);
    qm.append(&recs.samples);
    c.wait_for_expected_data(Duration::from_secs(30));

    // Metadata is cached state, not a queue item used for shard scaling.
    assert_eq!(recs.samples.len() as i64, qm.data_out.new_events.load(Ordering::SeqCst));
    s.close().unwrap();
}

#[test]
fn test_sample_delivery_timeout() {
    for proto_msg in PROTOS {
        let recs = generate_records(RecCase { no_st: proto_msg == V1, series: 10, samples_per_series: 10, ..Default::default() });
        let mut cfg = test_default_queue_config();
        let mcfg = default_metadata_config();
        cfg.max_shards = 1;

        let c = TestWriteClient::new(proto_msg);
        let m = new_test_queue_manager(cfg, mcfg, DEFAULT_FLUSH_DEADLINE, c.clone(), proto_msg);
        m.store_series(&recs.series, 0);
        m.start();

        // Send the samples twice, waiting for the samples in the meantime.
        c.expect_samples(&recs.samples, &recs.series);
        m.append(&recs.samples);
        c.wait_for_expected_data(Duration::from_secs(30));

        c.expect_samples(&recs.samples, &recs.series);
        m.append(&recs.samples);
        c.wait_for_expected_data(Duration::from_secs(30));
        m.stop();
    }
}

#[test]
fn test_sample_delivery_order() {
    for proto_msg in PROTOS {
        let ts = 10;
        let n = default_queue_config().max_samples_per_send as usize * ts;
        let recs = generate_records(RecCase { no_st: proto_msg == V1, series: n, samples_per_series: 1, ..Default::default() });

        let (c, m) = new_test_client_and_queue_manager(DEFAULT_FLUSH_DEADLINE, proto_msg);
        c.expect_samples(&recs.samples, &recs.series);
        m.store_series(&recs.series, 0);

        m.start();
        // These should be received by the client.
        m.append(&recs.samples);
        c.wait_for_expected_data(Duration::from_secs(30));
        m.stop();
    }
}

#[test]
fn test_shutdown() {
    // adapted: runs on the real clock (Go uses synctest); the two protocols run in parallel.
    let handles: Vec<_> = PROTOS
        .into_iter()
        .map(|proto_msg| {
            std::thread::spawn(move || {
                let deadline = Duration::from_secs(15);
                let c = Arc::new(TestBlockingWriteClient::default());

                let m = new_test_queue_manager(default_queue_config(), default_metadata_config(), deadline, c, proto_msg);
                // Send 2x batch size, so we know it will need at least two sends.
                let n = 2 * default_queue_config().max_samples_per_send as usize;
                let recs = generate_records(RecCase {
                    no_st: proto_msg == V1,
                    series: n / 1000,
                    samples_per_series: 1000,
                    ..Default::default()
                });
                m.store_series(&recs.series, 0);
                m.start();

                // Append blocks to guarantee delivery, so we do it in the background.
                let m2 = m.clone();
                let samples = recs.samples.clone();
                let bg = std::thread::spawn(move || {
                    m2.append(&samples);
                });
                // adapted: synctest.Wait — wait until every sample is queued (pending) or being sent.
                assert!(eventually(Duration::from_secs(10), Duration::from_millis(1), || {
                    m.metrics.pending_samples.get().get() == n as f64
                }));

                // Test to ensure that Stop doesn't block.
                let start = Instant::now();
                m.stop();
                // The samples will never be delivered, so duration should
                // be at least equal to deadline, otherwise the flush deadline
                // was not respected.
                let elapsed = start.elapsed();
                assert!(elapsed >= deadline && elapsed < deadline + Duration::from_secs(2), "{elapsed:?}");
                bg.join().unwrap();
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_series_reset() {
    for proto_msg in PROTOS {
        let c = Arc::new(TestBlockingWriteClient::default());
        let deadline = Duration::from_secs(5);
        let num_segments = 4;
        let num_series = 25;

        let m = new_test_queue_manager(default_queue_config(), default_metadata_config(), deadline, c, proto_msg);
        for i in 0..num_segments {
            let mut series = Vec::new();
            let mut metadata = Vec::new();
            for j in 0..num_series {
                let r = (i * 100 + j) as HeadSeriesRef;
                series.push(RefSeries { r#ref: r, labels: labels::from_strings(&["a", "a"]) });
                metadata.push(RefMetadata { r#ref: r, r#type: 1, unit: String::new(), help: "test".to_string() });
            }
            m.store_series(&series, i as i64);
            m.store_metadata(&metadata);
        }
        assert_eq!(num_segments * num_series, m.series_labels_len());
        // V2 stores metadata in seriesMetadata map for inline sending.
        // V1 sends metadata separately via MetadataWatcher, so seriesMetadata is not populated.
        if proto_msg == V2 {
            assert_eq!(num_segments * num_series, m.series_metadata_len());
        }

        m.series_reset(2);
        assert_eq!(num_segments * num_series / 2, m.series_labels_len());
        // Verify metadata is also reset for V2
        if proto_msg == V2 {
            assert_eq!(num_segments * num_series / 2, m.series_metadata_len());
        }
    }
}

#[test]
fn test_reshard() {
    let handles: Vec<_> = PROTOS
        .into_iter()
        .map(|proto_msg| {
            std::thread::spawn(move || {
                let size = 10; // Make bigger to find more races.
                let n_series = 6;
                let capacity = default_queue_config().capacity as usize;
                let samples_per_series = capacity * size;
                let recs = generate_records(RecCase {
                    no_st: proto_msg == V1,
                    series: n_series,
                    samples_per_series,
                    ..Default::default()
                });

                let mut cfg = default_queue_config();
                cfg.max_shards = 1;

                let c = TestWriteClient::new(proto_msg);
                let m = new_test_queue_manager(cfg, default_metadata_config(), DEFAULT_FLUSH_DEADLINE, c.clone(), proto_msg);
                c.expect_samples(&recs.samples, &recs.series);
                m.store_series(&recs.series, 0);

                m.start();

                let m2 = m.clone();
                let samples = recs.samples.clone();
                let appender = std::thread::spawn(move || {
                    let mut i = 0;
                    while i < samples.len() {
                        let sent = m2.append(&samples[i..i + capacity]);
                        assert!(sent, "samples not sent");
                        std::thread::sleep(Duration::from_millis(100));
                        i += capacity;
                    }
                });

                for i in 1..recs.samples.len() / capacity {
                    m.shards.stop();
                    m.shards.start(i);
                    std::thread::sleep(Duration::from_millis(100));
                }

                c.wait_for_expected_data(Duration::from_secs(30));
                appender.join().unwrap();
                m.stop();
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_reshard_race_with_stop() {
    for proto_msg in PROTOS {
        let c = TestWriteClient::new(proto_msg);
        let cfg = test_default_queue_config();
        let mcfg = default_metadata_config();
        // adapted: Go hands the queue manager over with a mutex; here two channels do the same handoff.
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Arc<QueueManager>>();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let worker = std::thread::spawn(move || {
            loop {
                let m = new_test_queue_manager(cfg.clone(), mcfg.clone(), DEFAULT_FLUSH_DEADLINE, c.clone(), proto_msg);
                m.start();
                if ready_tx.send(m.clone()).is_err() {
                    m.stop();
                    return;
                }
                let more = go_rx.recv().is_ok();
                m.stop();
                if !more {
                    return;
                }
            }
        });
        for i in 1..100 {
            let m = ready_rx.recv().unwrap();
            m.reshard_chan_send(i);
            go_tx.send(()).unwrap();
        }
        drop(go_tx);
        drop(ready_rx);
        worker.join().unwrap();
    }
}

/// TestSetClientRace runs SetClient concurrently with sample and metadata sends.
#[test]
fn test_set_client_race() {
    for proto_msg in PROTOS {
        let c = TestWriteClient::new(proto_msg);
        let mut cfg = test_default_queue_config();
        // More than one shard keeps multiple send goroutines in flight.
        cfg.max_shards = 4;
        let m = new_test_queue_manager(cfg, default_metadata_config(), DEFAULT_FLUSH_DEADLINE, c.clone(), proto_msg);

        // Series and samples only need to be large enough to fill a batch and hit Store.
        let recs = generate_records(RecCase { no_st: proto_msg == V1, series: 8, samples_per_series: 20, ..Default::default() });
        m.store_series(&recs.series, 0);
        m.store_metadata(&recs.metadata);
        m.start();

        let stop = Arc::new(AtomicBool::new(false));
        let (m1, c1, stop1) = (m.clone(), c.clone(), stop.clone());
        let setter = std::thread::spawn(move || {
            while !stop1.load(Ordering::SeqCst) {
                m1.set_client(c1.clone());
            }
        });
        // Repeated appends keep shards sending; the exact count is not load-bearing.
        for _ in 0..50 {
            m.append(&recs.samples);
            if proto_msg == V1 {
                m.append_watcher_metadata(
                    &Ctx::background(),
                    &[MetricMetadata { metric_family: "test_metric".into(), typ: MetricType::Counter, help: "help".into(), unit: String::new() }],
                );
            }
        }
        stop.store(true, Ordering::SeqCst);
        setter.join().unwrap();
        m.stop();
    }
}

#[test]
fn test_reshard_partial_batch() {
    for proto_msg in PROTOS {
        let recs = generate_records(RecCase { no_st: proto_msg == V1, series: 1, samples_per_series: 10, ..Default::default() });

        let c = Arc::new(TestBlockingWriteClient::default());

        let mut cfg = test_default_queue_config();
        let mcfg = default_metadata_config();
        cfg.max_shards = 1;
        let batch_send_deadline = Duration::from_millis(1);
        let flush_deadline = Duration::from_millis(10);
        cfg.batch_send_deadline = ydur(batch_send_deadline);

        let m = new_test_queue_manager(cfg, mcfg, flush_deadline, c, proto_msg);
        m.store_series(&recs.series, 0);

        m.start();

        for _ in 0..100 {
            let (tx, rx) = std::sync::mpsc::channel();
            let (m2, samples) = (m.clone(), recs.samples.clone());
            std::thread::spawn(move || {
                m2.append(&samples);
                std::thread::sleep(batch_send_deadline);
                m2.shards.stop();
                m2.shards.start(1);
                let _ = tx.send(());
            });
            if rx.recv_timeout(Duration::from_secs(2)).is_err() {
                panic!("Deadlock between sending and stopping detected");
            }
        }
        // We can only call stop if there was not a deadlock.
        m.stop();
    }
}

/// TestQueueFilledDeadlock makes sure the code does not deadlock in the case
/// where a large scrape (> capacity + max samples per send) is appended at the
/// same time as a batch times out according to the batch send deadline.
#[test]
fn test_queue_filled_deadlock() {
    for proto_msg in PROTOS {
        let recs = generate_records(RecCase { no_st: proto_msg == V1, series: 50, samples_per_series: 1, ..Default::default() });

        let c = Arc::new(NopWriteClient);

        let mut cfg = test_default_queue_config();
        let mcfg = default_metadata_config();
        cfg.max_shards = 1;
        cfg.max_samples_per_send = 10;
        cfg.capacity = 20;
        let flush_deadline = Duration::from_secs(1);
        let batch_send_deadline = Duration::from_millis(1);
        cfg.batch_send_deadline = ydur(batch_send_deadline);

        let m = new_test_queue_manager(cfg, mcfg, flush_deadline, c, proto_msg);
        m.store_series(&recs.series, 0);
        m.start();

        for _ in 0..100 {
            let (tx, rx) = std::sync::mpsc::channel();
            let (m2, samples) = (m.clone(), recs.samples.clone());
            std::thread::spawn(move || {
                std::thread::sleep(batch_send_deadline);
                m2.append(&samples);
                let _ = tx.send(());
            });
            if rx.recv_timeout(Duration::from_secs(2)).is_err() {
                panic!("Deadlock between sending and appending detected");
            }
        }
        m.stop();
    }
}

#[test]
fn test_release_noninterned_string() {
    for proto_msg in PROTOS {
        let (_, m) = new_test_client_and_queue_manager(DEFAULT_FLUSH_DEADLINE, proto_msg);
        m.start();
        for i in 1..1000 {
            m.store_series(&[RefSeries { r#ref: i as HeadSeriesRef, labels: labels::from_strings(&["asdf", &i.to_string()]) }], 0);
            m.series_reset(1);
        }

        let metric = no_reference_releases().get().get();
        assert_eq!(0.0, metric, "expected there to be no calls to release for strings that were not already interned: {}", metric as i64);
        m.stop();
    }
}

#[test]
fn test_should_reshard() {
    struct Case {
        starting_shards: i64,
        samples_in: i64,
        samples_out: i64,
        last_send_timestamp: i64,
        expected_to_reshard: bool,
        send_deadline: Duration,
    }
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let cases = [
        // resharding shouldn't take place if we haven't successfully sent
        // since the last shardUpdateDuration, even if the send deadline is very low
        Case {
            starting_shards: 10,
            samples_in: 1000,
            samples_out: 10,
            last_send_timestamp: now - SHARD_UPDATE_DURATION.as_nanos() as i64,
            expected_to_reshard: false,
            send_deadline: Duration::from_millis(100),
        },
        Case {
            starting_shards: 10,
            samples_in: 1000,
            samples_out: 10,
            last_send_timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            expected_to_reshard: true,
            send_deadline: default_queue_config().batch_send_deadline.to_std(),
        },
    ];

    for c in cases {
        let (_, m) = new_test_client_and_queue_manager(c.send_deadline, V1);
        m.set_num_shards(c.starting_shards);
        m.data_in.incr(c.samples_in);
        m.data_out.incr(c.samples_out);
        m.last_send_timestamp.store(c.last_send_timestamp, Ordering::SeqCst);
        m.start();

        let desired_shards = m.calculate_desired_shards();
        let should_reshard = m.should_reshard(desired_shards);

        m.stop();

        assert_eq!(c.expected_to_reshard, should_reshard);
    }
}

/// TestDisableReshardOnRetry asserts that resharding should be disabled when a
/// recoverable error is returned from remote_write.
#[test]
fn test_disable_reshard_on_retry() {
    let on_store_called = Arc::new(AtomicBool::new(false));

    let recs = generate_records(RecCase { series: 100, samples_per_series: 100, ..Default::default() });
    let cfg = default_queue_config();
    let mcfg = default_metadata_config();
    let retry_after = Duration::from_secs(1);

    let metrics = QueueManagerMetrics::new(None, "", "");

    let called = on_store_called.clone();
    let client = Arc::new(MockWriteClient {
        store_func: Box::new(move |_, _, _| {
            called.store(true, Ordering::SeqCst);
            Err(RecoverableError::new(Error::msg("fake error"), retry_after.as_nanos() as i64))
        }),
        name: "mock".to_string(),
        endpoint: "http://fake:9090/api/v1/write".to_string(),
    });

    let m = QueueManager::new(
        metrics,
        None,
        None,
        None,
        std::path::Path::new(""),
        Arc::new(EwmaRate::new(EWMA_WEIGHT, SHARD_UPDATE_DURATION)),
        cfg,
        mcfg,
        &Labels::empty(),
        vec![],
        client,
        Duration::ZERO,
        Arc::new(Pool::new()),
        new_highest_timestamp_metric(),
        None,
        false,
        false,
        false,
        V1,
        None,
        false,
        false,
    );
    m.store_series(&recs.series, 0);

    // Attempt to samples while the manager is running. We immediately stop the
    // manager after the recoverable error is generated to prevent the manager
    // from resharding itself.
    m.start();
    m.append(&recs.samples);
    assert!(
        eventually(Duration::from_secs(60), Duration::from_millis(1), || on_store_called.load(Ordering::SeqCst)),
        "timed out waiting for client to be sent metrics"
    );
    m.stop();

    let unix = || SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    assert!(
        eventually(Duration::from_secs(60), Duration::from_millis(10), || {
            // Force m.lastSendTimestamp to be current so the last send timestamp isn't
            // the reason resharding is disabled.
            m.last_send_timestamp.store(unix(), Ordering::SeqCst);
            !m.should_reshard(m.num_shards() + 1)
        }),
        "shouldReshard was never disabled"
    );

    // After 2x retryAfter, resharding should be enabled again.
    assert!(
        eventually(Duration::from_secs(60), retry_after, || {
            m.last_send_timestamp.store(unix(), Ordering::SeqCst);
            m.should_reshard(m.num_shards() + 1)
        }),
        "shouldReshard should have been re-enabled"
    );
}

#[test]
fn test_process_external_labels() {
    let mut b = Builder::new(Labels::empty());
    let l = |n: &str, v: &str| Label::new(n, v);
    let cases: Vec<(Labels, Vec<Label>, Labels)> = vec![
        // Test adding labels at the end.
        (labels::from_strings(&["a", "b"]), vec![l("c", "d")], labels::from_strings(&["a", "b", "c", "d"])),
        // Test adding labels at the beginning.
        (labels::from_strings(&["c", "d"]), vec![l("a", "b")], labels::from_strings(&["a", "b", "c", "d"])),
        // Test we don't override existing labels.
        (labels::from_strings(&["a", "b"]), vec![l("a", "c")], labels::from_strings(&["a", "b"])),
        // Test empty externalLabels.
        (labels::from_strings(&["a", "b"]), vec![], labels::from_strings(&["a", "b"])),
        // Test empty labels.
        (Labels::empty(), vec![l("a", "b")], labels::from_strings(&["a", "b"])),
        // Test labels is longer than externalLabels.
        (labels::from_strings(&["a", "b", "c", "d"]), vec![l("e", "f")], labels::from_strings(&["a", "b", "c", "d", "e", "f"])),
        // Test externalLabels is longer than labels.
        (labels::from_strings(&["c", "d"]), vec![l("a", "b"), l("e", "f")], labels::from_strings(&["a", "b", "c", "d", "e", "f"])),
        // Adding with and without clashing labels.
        (
            labels::from_strings(&["a", "b", "c", "d"]),
            vec![l("a", "xxx"), l("c", "yyy"), l("e", "f")],
            labels::from_strings(&["a", "b", "c", "d", "e", "f"]),
        ),
    ];
    for (i, (lbls, ext, expected)) in cases.into_iter().enumerate() {
        b.reset(lbls);
        process_external_labels(&mut b, &ext);
        assert_eq!(expected, b.labels(), "test {i}");
    }
}

#[test]
fn test_calculate_desired_shards() {
    let cfg = default_queue_config();
    let (_, m) = new_test_client_and_queue_manager(DEFAULT_FLUSH_DEADLINE, V1);
    let samples_in = m.data_in.clone();

    // Need to start the queue manager so the proper metrics are initialized.
    // However we can stop it right away since we don't need to do any actual
    // processing.
    m.start();
    m.stop();

    let input_rate: i64 = 50000;
    let mut pending_samples: i64 = 0;

    // Two minute startup, no samples are sent.
    let started_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64 - 120 * 1_000_000_000;
    let unix_at = |ts_ns: i64| (started_at + ts_ns).div_euclid(1_000_000_000) as f64;
    let sud_ns = SHARD_UPDATE_DURATION.as_nanos() as i64;
    let sud_s = SHARD_UPDATE_DURATION.as_secs() as i64;

    let mut ts: i64 = 0;
    while ts < 120 * 1_000_000_000 {
        // addSamples
        let s = input_rate * sud_s;
        pending_samples += s;
        samples_in.incr(s);
        samples_in.tick();
        m.highest_recv_timestamp.set(unix_at(ts));

        let n = m.calculate_desired_shards();
        m.set_num_shards(n);
        assert_eq!(1, m.num_shards());
        ts += sud_ns;
    }

    // Assume 100ms per request, or 10 requests per second per shard.
    // Shard calculation should never drop below barely keeping up.
    let min_shards = input_rate / cfg.max_samples_per_send / 10;
    // This test should never go above 200 shards, that would be more resources than needed.
    let max_shards = 200;

    while ts < 15 * 60 * 1_000_000_000 {
        let sin = input_rate * sud_s;
        pending_samples += sin;
        samples_in.incr(sin);
        samples_in.tick();
        m.highest_recv_timestamp.set(unix_at(ts));

        // You can't send samples that don't exist so cap at the number of pending samples.
        let sout = (m.num_shards() * cfg.max_samples_per_send * (sud_ns / 100_000_000)).min(pending_samples);
        // sendSamples
        pending_samples -= sout;
        m.data_out.incr(sout);
        m.data_out_duration.incr(m.num_shards() * sud_ns);
        // highest sent is how far back pending samples would be at our input rate.
        let highest_sent = unix_at(ts - (pending_samples / input_rate) * 1_000_000_000);
        m.metrics.highest_sent_timestamp.set(highest_sent);
        m.last_send_timestamp.store(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64, Ordering::SeqCst);

        let n = m.calculate_desired_shards();
        m.set_num_shards(n);
        assert!(m.num_shards() >= min_shards, "Shards are too low. desiredShards={}, minShards={min_shards}, t_seconds={}", m.num_shards(), ts / 1_000_000_000);
        assert!(m.num_shards() <= max_shards, "Shards are too high. desiredShards={}, maxShards={max_shards}, t_seconds={}", m.num_shards(), ts / 1_000_000_000);
        ts += sud_ns;
    }
    assert_eq!(0, pending_samples, "Remote write never caught up, there are still {pending_samples} pending samples.");
}

/// Go `forceEMWA`.
fn force_ewma(r: &EwmaRate, rate: i64) {
    r.state.lock().unwrap().init = false;
    r.new_events.store(rate, Ordering::SeqCst);
}

/// Go `nhcbQueueTestOptions`.
#[derive(Default, Clone)]
struct NhcbOpts {
    proto_msg: ProtoMsg,
    send_native_histograms: bool,
    send_exemplars: bool,
    num_shards: usize,
    samples_in: Option<Arc<EwmaRate>>,
    highest_recv_timestamp: Option<Arc<MaxTimestamp>>,
    sample_age_limit: Duration,
    batch_send_deadline: Duration,
}

/// The `stop` of `newTestNHCBQueueManager` (Go `sync.OnceFunc(m.shards.stop)`, also run at cleanup).
struct NhcbStop {
    m: Arc<QueueManager>,
    once: Once,
}

impl NhcbStop {
    fn stop(&self) {
        self.once.call_once(|| self.m.shards.stop());
    }
}

impl Drop for NhcbStop {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Go `newTestNHCBQueueManager`.
fn new_test_nhcb_queue_manager(c: Arc<dyn WriteClient>, mut opts: NhcbOpts) -> (Arc<QueueManager>, NhcbStop) {
    let mut cfg = default_queue_config();
    cfg.max_samples_per_send = 1000;
    cfg.batch_send_deadline = YDuration::from_secs(60);
    if opts.batch_send_deadline != Duration::ZERO {
        cfg.batch_send_deadline = ydur(opts.batch_send_deadline);
    }
    cfg.sample_age_limit = ydur(opts.sample_age_limit);
    let mut mcfg = default_metadata_config();
    mcfg.send = false;
    if opts.num_shards == 0 {
        opts.num_shards = 1;
    }
    let samples_in = opts.samples_in.unwrap_or_else(|| Arc::new(EwmaRate::new(EWMA_WEIGHT, SHARD_UPDATE_DURATION)));
    let highest = opts.highest_recv_timestamp.unwrap_or_else(new_highest_timestamp_metric);
    let m = QueueManager::new(
        QueueManagerMetrics::new(None, "", ""),
        None,
        None,
        None,
        &temp_dir(),
        samples_in,
        cfg,
        mcfg,
        &Labels::empty(),
        vec![],
        c,
        DEFAULT_FLUSH_DEADLINE,
        Arc::new(Pool::new()),
        highest,
        None,
        opts.send_exemplars,
        opts.send_native_histograms,
        false,
        opts.proto_msg,
        None,
        false,
        true,
    );
    // Start the production senders without the WAL watcher or automatic resharding loop.
    m.set_num_shards(opts.num_shards as i64);
    m.shards.start(opts.num_shards);
    let stop = NhcbStop { m: m.clone(), once: Once::new() };
    (m, stop)
}

#[test]
fn test_calculate_desired_shards_detail() {
    let (_, m) = new_test_client_and_queue_manager(DEFAULT_FLUSH_DEADLINE, V1);
    let samples_in = m.data_in.clone();
    let sud_s = SHARD_UPDATE_DURATION.as_secs() as i64;

    struct Case {
        name: &'static str,
        prev_shards: i64,
        data_in: i64, // Quantities normalised to seconds.
        data_out: i64,
        data_dropped: i64,
        data_out_duration: f64,
        backlog: f64,
        expected_shards: i64,
    }
    let c = |name, prev_shards, data_in, data_out, data_out_duration, backlog, expected_shards| Case {
        name,
        prev_shards,
        data_in,
        data_out,
        data_dropped: 0,
        data_out_duration,
        backlog,
        expected_shards,
    };
    let cases = [
        c("nothing in or out 1", 1, 0, 0, 0.0, 0.0, 1),     // Shards stays the same.
        c("nothing in or out 10", 10, 0, 0, 0.0, 0.0, 10), // Shards stays the same.
        c("steady throughput", 1, 10, 10, 1.0, 0.0, 1),
        c("sent timestamp in the future", 40, 1000, 1000, 40.0, -(5.0 * 3600.0), 40),
        c("scale down", 10, 10, 10, 5.0, 0.0, 5),
        c("scale down constrained", 7, 10, 10, 5.0, 0.0, 7),
        c("scale up", 1, 10, 10, 10.0, 0.0, 10),
        c("scale up constrained", 8, 10, 10, 10.0, 0.0, 8),
        c("backlogged 20s", 2, 10, 10, 2.0, 20.0, 4),
        c("backlogged 90s", 4, 10, 10, 4.0, 90.0, 22),
        c("backlog reduced", 22, 10, 20, 4.0, 10.0, 3),
        c("backlog eliminated", 3, 10, 10, 2.0, 0.0, 2), // Shard back down.
        c("slight slowdown", 1, 10, 10, 1.2, 0.0, 2),    // 1.2 is rounded up to 2.
        c("bigger slowdown", 1, 10, 10, 1.4, 0.0, 2),
        c("speed up", 2, 10, 10, 1.2, 0.0, 2), // No reaction - 1.2 is rounded up to 2.
        c("speed up more", 2, 10, 10, 0.9, 0.0, 1),
        c("marginal decision A", 3, 10, 10, 2.01, 0.0, 3), // 2.01 rounds up to 3.
        c("marginal decision B", 3, 10, 10, 1.99, 0.0, 2), // 1.99 rounds up to 2.
    ];
    for tc in cases {
        m.set_num_shards(tc.prev_shards);
        force_ewma(&samples_in, tc.data_in * sud_s);
        samples_in.tick();
        force_ewma(&m.data_out, tc.data_out * sud_s);
        force_ewma(&m.data_dropped, tc.data_dropped * sud_s);
        force_ewma(&m.data_out_duration, (tc.data_out_duration * SHARD_UPDATE_DURATION.as_nanos() as f64) as i64);
        m.highest_recv_timestamp.force_value(tc.backlog); // Not Set() because it can only increase value.

        assert_eq!(tc.expected_shards, m.calculate_desired_shards(), "{}", tc.name);
    }

    // NHCB conversion. adapted: real clock, the four cases in parallel.
    let nhcb_cases: [(&str, usize, usize, bool); 4] = [
        ("ordinary samples", 0, 10000, false),
        ("integer histograms", 1000, 0, false),
        ("float histograms", 1000, 0, true),
        ("mixed samples", 500, 5000, false),
    ];
    let handles: Vec<_> = nhcb_cases
        .into_iter()
        .map(|(name, n_hist, n_samples, as_float)| {
            std::thread::spawn(move || {
                let c = TestWriteClient::new(V1);
                let in_flight = Arc::new(AtomicI64::new(0));
                let peak_in_flight = Arc::new(AtomicI64::new(0));
                let calls = Arc::new(AtomicI64::new(0));
                let (cc, inf, peak, calls2) = (c.clone(), in_flight.clone(), peak_in_flight.clone(), calls.clone());
                let client = Arc::new(MockWriteClient {
                    name: c.name().to_string(),
                    endpoint: c.endpoint().to_string(),
                    store_func: Box::new(move |ctx, req, attempt| {
                        let active = inf.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(active, Ordering::SeqCst);
                        calls2.fetch_add(1, Ordering::SeqCst);
                        // Delay outside the recording client's mutex so all ten shards can send concurrently.
                        let deadline = Instant::now() + SHARD_UPDATE_DURATION;
                        let r = loop {
                            if ctx.is_done() {
                                break Err(ctx.err().unwrap());
                            }
                            if Instant::now() >= deadline {
                                break cc.store(ctx, req, attempt);
                            }
                            std::thread::sleep(Duration::from_millis(5));
                        };
                        inf.fetch_sub(1, Ordering::SeqCst);
                        r
                    }),
                });
                let rws = WriteStorage::new(None, None, &temp_dir(), DEFAULT_FLUSH_DEADLINE, None, false, None);
                let (m, stop) = new_test_nhcb_queue_manager(
                    client,
                    NhcbOpts {
                        proto_msg: V1,
                        num_shards: 10,
                        samples_in: Some(rws.samples_in.clone()),
                        highest_recv_timestamp: Some(rws.highest_timestamp.clone()),
                        ..Default::default()
                    },
                );

                let h = Histogram {
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
                    positive_spans: vec![Span { offset: 0, length: 8 }],
                    positive_buckets: vec![1, 0, 0, 0, 0, 0, 0, 0],
                    count: 8,
                    sum: 30.0,
                    ..Default::default()
                };
                h.validate().unwrap();
                let h = Arc::new(h);
                let fh = Arc::new(h.to_float());
                let mut app = rws.appender_v2(&Context::background());
                let (mut series, mut histograms, mut float_histograms, mut samples) = (vec![], vec![], vec![], vec![]);
                let ts = 1000i64;
                for i in 0..n_hist + n_samples {
                    let r = (i + 1) as HeadSeriesRef;
                    let mut name = "requests_total";
                    let (mut int_h, mut float_h) = (None, None);
                    if i < n_hist {
                        name = "latency";
                        if as_float {
                            float_h = Some(&*fh);
                            float_histograms.push(RefFloatHistogramSample { r#ref: r, st: 0, t: ts, fh: fh.clone() });
                        } else {
                            int_h = Some(&*h);
                            histograms.push(RefHistogramSample { r#ref: r, st: 0, t: ts, h: h.clone() });
                        }
                    } else {
                        samples.push(RefSample { r#ref: r, st: 0, t: ts, v: 1.0 });
                    }
                    let lbls = labels::from_strings(&["__name__", name, "series", &i.to_string()]);
                    series.push(RefSeries { r#ref: r, labels: lbls.clone() });
                    app.append(r, &lbls, 0, ts, 1.0, int_h, float_h, &AppendV2Options::default()).unwrap();
                }
                assert!(app.commit().is_none());
                m.store_series(&series, 0);
                assert!(m.append_histograms(&histograms));
                assert!(m.append_float_histograms(&float_histograms));
                assert!(m.append(&samples));

                // adapted: synctest.Wait.
                assert!(eventually(Duration::from_secs(5), Duration::from_millis(1), || in_flight.load(Ordering::SeqCst) == 10), "{name}");
                std::thread::sleep(SHARD_UPDATE_DURATION);
                assert!(eventually(Duration::from_secs(5), Duration::from_millis(1), || in_flight.load(Ordering::SeqCst) == 0), "{name}");
                assert_eq!(0, in_flight.load(Ordering::SeqCst));
                assert_eq!(10, peak_in_flight.load(Ordering::SeqCst), "{name}");
                assert_eq!(10, calls.load(Ordering::SeqCst), "{name}");
                assert_eq!(10000, deep_len(&[&c.lock().received_samples]), "{name}");
                assert_eq!(0.0, counter(&m.metrics.failed_samples_total));
                assert_eq!(0.0, m.metrics.pending_samples.get().get());
                assert_eq!(m.highest_recv_timestamp.get(), m.metrics.highest_sent_timestamp.get());
                // adapted: wait for the WriteStorage's 10s tick of samples in (Go's fake clock has run it).
                assert!(eventually(Duration::from_secs(5), Duration::from_millis(10), || m.data_in.rate() > 0.0));
                // Each workload filled ten batches and kept ten shards busy for the whole interval.
                assert_eq!(10, m.calculate_desired_shards(), "{name}: NHCB expansion must not reduce the estimated shard demand");
                stop.stop();
                rws.close().unwrap();
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_queue_manager_metrics() {
    // These legacy metadata metrics are deprecated and not registered for Remote Write 2.0.
    let metadata_metric_names = [
        "prometheus_remote_storage_metadata_total",
        "prometheus_remote_storage_metadata_failed_total",
        "prometheus_remote_storage_metadata_retried_total",
        "prometheus_remote_storage_metadata_bytes_total",
    ];
    for (name, proto_msg, expect_metadata_metric) in [("remote write 1.0", V1, true), ("remote write 2.0", V2, false)] {
        let reg = Registry::new();
        let metrics = QueueManagerMetrics::new(Some(reg.clone()), "name", "http://localhost:1234");
        metrics.register(proto_msg);

        // adapted: Go lints with `testutil.GatherAndLint`; here the lint rules this registry can break are
        // checked: help text present and counters ending in `_total`.
        let families = reg.gather();
        for f in &families {
            assert!(!f.help.is_empty(), "{name}: {} has no help", f.name);
            if f.typ == FamilyType::Counter {
                assert!(f.name.ends_with("_total"), "{name}: counter {} must end in _total", f.name);
            }
        }
        let registered: HashSet<&str> = families.iter().map(|f| f.name.as_str()).collect();
        for metric_name in metadata_metric_names {
            assert_eq!(expect_metadata_metric, registered.contains(metric_name), "{name}: {metric_name}");
        }

        // Make sure all metrics were unregistered. A failure here means you need
        // unregister a metric in `queueManagerMetrics.unregister()`.
        metrics.unregister(proto_msg);
        assert!(reg.gather().is_empty(), "{name}");
    }
}

#[test]
fn test_queue_flush_and_shutdown_does_not_deadlock() {
    let capacity = 100;
    let batch_size = 10;
    let queue = Arc::new(Queue::new(batch_size, capacity));
    for _ in 0..capacity + batch_size {
        let _ = queue.append(TimeSeries::default());
    }

    let done = Arc::new(Signal::new());
    let (q1, d1) = (queue.clone(), done.clone());
    let flusher = std::thread::spawn(move || q1.flush_and_shutdown(&d1));
    let (q2, d2) = (queue.clone(), done.clone());
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        // Give enough time for FlushAndShutdown to acquire the lock. queue.Batch()
        // should not block forever even if the lock is acquired.
        std::thread::sleep(Duration::from_millis(10));
        q2.batch();
        d2.set();
        let _ = tx.send(());
    });
    if rx.recv_timeout(Duration::from_secs(2)).is_err() {
        panic!("Deadlock in FlushAndShutdown detected");
    }
    flusher.join().unwrap();
}

#[test]
fn test_drop_old_time_series() {
    let handles: Vec<_> = PROTOS
        .into_iter()
        .map(|proto_msg| {
            std::thread::spawn(move || {
                let size = 10;
                let n_series = 6;
                let n_samples = default_queue_config().capacity as usize * size;
                let no_st = proto_msg == V1; // RW1
                let past_recs = generate_records(RecCase {
                    no_st,
                    series: n_series,
                    samples_per_series: (n_samples / n_series) / 2, // Half data is past.
                    ts_fn: Some(Box::new(|_, j| now_ms() - 5 * 60 * 1000 + j as i64)),
                    ..Default::default()
                });
                let new_recs = generate_records(RecCase {
                    no_st,
                    series: n_series,
                    samples_per_series: (n_samples / n_series) / 2,
                    ts_fn: Some(Box::new(|_, j| now_ms() + j as i64)),
                    ..Default::default()
                });

                let series = &past_recs.series; // Series is the same for both old and new.
                let new_samples = &new_recs.samples;
                let mut samples = past_recs.samples.clone();
                samples.extend_from_slice(&new_recs.samples);

                let c = TestWriteClient::new(proto_msg);
                c.expect_samples(new_samples, series);

                let mut cfg = default_queue_config();
                let mcfg = default_metadata_config();
                cfg.max_shards = 1;
                cfg.sample_age_limit = YDuration::from_secs(60);
                let m = new_test_queue_manager(cfg, mcfg, DEFAULT_FLUSH_DEADLINE, c.clone(), proto_msg);
                m.store_series(series, 0);

                m.start();
                m.append(&samples);
                c.wait_for_expected_data(Duration::from_secs(30));
                m.stop();
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_is_sample_old() {
    let current_time = SystemTime::now();
    let ms = |t: SystemTime| t.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    assert!(is_sample_old(current_time, Duration::from_secs(60), ms(current_time - Duration::from_secs(61))));
    assert!(!is_sample_old(current_time, Duration::from_secs(60), ms(current_time - Duration::from_secs(59))));
}

/// Simulates scenario in which remote write endpoint is down and a subset of samples is dropped due to age
/// limit while backoffing.
#[test]
fn test_send_samples_with_backoff_with_sample_age_limit() {
    const MAX_SAMPLES_PER_SEND: usize = 10;
    const MAX_LABELS: u64 = 9;

    let handles: Vec<_> = PROTOS
        .into_iter()
        .map(|proto_msg| {
            std::thread::spawn(move || {
                let sample_age_limit = Duration::from_secs(2);

                let mut cfg = default_queue_config();
                cfg.max_shards = 1;
                cfg.sample_age_limit = ydur(sample_age_limit);
                // Set the batch send deadline to 5 minutes to effectively disable it.
                cfg.batch_send_deadline = YDuration::from_secs(300);
                cfg.capacity = 10 * MAX_SAMPLES_PER_SEND as i64; // more than the amount of data we append in the test
                cfg.max_backoff = YDuration::from_millis(100);
                cfg.min_backoff = YDuration::from_millis(100);
                cfg.max_samples_per_send = MAX_SAMPLES_PER_SEND as i64;
                let mut metadata_cfg = default_metadata_config();
                metadata_cfg.send = true;
                metadata_cfg.send_interval = YDuration::from_secs(60);
                metadata_cfg.max_samples_per_send = MAX_SAMPLES_PER_SEND as i64;
                let c = TestWriteClient::new(proto_msg);
                let m = new_test_queue_manager(cfg, metadata_cfg, Duration::from_secs(1), c.clone(), proto_msg);

                m.start();

                let mut batch_id = 0usize;
                let mut append_data = |number_of_series: usize, time_add_ms: i64, should_be_dropped: bool| {
                    // Use a fixed rand source so tests are consistent.
                    // adapted: a fixed LCG in place of Go's math/rand source 99 (only the label count varies).
                    let mut rng: u64 = 99;
                    let bid = batch_id;
                    let recs = generate_records(RecCase {
                        no_st: proto_msg == V1, // RW1 does not support ST.
                        series: number_of_series,
                        samples_per_series: 1,
                        ts_fn: Some(Box::new(move |_, _| now_ms() + time_add_ms)),
                        labels_fn: Some(Box::new(move |lb, i| {
                            lb.reset();
                            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                            let labels_count = (rng >> 33) % MAX_LABELS;
                            lb.add("__name__", &format!("batch_{bid}_id_{i}"));
                            for j in 1..labels_count + 1 {
                                // same for both name and value
                                let label = format!("batch_{bid}_label_{j}");
                                lb.add(&label, &label);
                            }
                            lb.labels()
                        })),
                        ..Default::default()
                    });

                    m.store_series(&recs.series, batch_id as i64);
                    let sent = m.append(&recs.samples);
                    assert!(sent, "samples not sent");
                    if !should_be_dropped {
                        for s in &recs.samples {
                            let id = get_series_id_from_ref(&recs.series[s.r#ref as usize]);
                            c.add_expected_sample(id, writev2::Sample { start_timestamp: s.st, timestamp: s.t, value: s.v });
                        }
                    }
                    batch_id += 1;
                };
                let time_shift = -5;

                // Inject RW error.
                c.set_return_error(Some(RecoverableError::new(ERR_DEADLINE_EXCEEDED, 0)));
                // Send current samples in various intervals.
                append_data(MAX_SAMPLES_PER_SEND / 2, time_shift, true);
                std::thread::sleep(sample_age_limit);
                append_data(MAX_SAMPLES_PER_SEND / 2, time_shift, true);
                std::thread::sleep(sample_age_limit / 10);
                append_data(MAX_SAMPLES_PER_SEND / 2, time_shift, true);
                std::thread::sleep(2 * sample_age_limit);
                // Eventually all the above data must be ignored as 2x sampleAgeLimit passed.

                // Now send, quickly re-enable RW target and send another batch.
                // We should expect all the data from those two below batches.
                append_data(2 * MAX_SAMPLES_PER_SEND, time_shift, false);
                std::thread::sleep(sample_age_limit / 2);
                c.set_return_error(None);
                append_data(5, time_shift, false);
                m.stop();

                let st = c.lock();
                assert_eq!(st.expected_samples, st.received_samples, "{proto_msg}");
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

fn filter_ts_limit(limit: i64, ts: &prompb::TimeSeries) -> bool {
    limit > ts.samples[0].timestamp
}

#[test]
fn test_build_time_series() {
    let s = |t: i64, v: f64| prompb::TimeSeries { samples: vec![prompb::Sample { timestamp: t, value: v }], ..Default::default() };
    struct Case {
        name: &'static str,
        ts: Vec<prompb::TimeSeries>,
        filter: Option<i64>,
        lowest_ts: i64,
        highest_ts: i64,
        dropped_samples: usize,
        response_len: usize,
    }
    let cases = vec![
        Case {
            name: "No filter applied",
            ts: vec![s(1234567890, 1.23), s(1234567891, 2.34), s(1234567892, 3.34)],
            filter: None,
            response_len: 3,
            lowest_ts: 1234567890,
            highest_ts: 1234567892,
            dropped_samples: 0,
        },
        Case {
            name: "Filter applied, samples in order",
            ts: vec![s(1234567890, 1.23), s(1234567891, 2.34), s(1234567892, 3.45), s(1234567893, 3.45)],
            filter: Some(1234567892),
            response_len: 2,
            lowest_ts: 1234567892,
            highest_ts: 1234567893,
            dropped_samples: 2,
        },
        Case {
            name: "Filter applied, samples out of order",
            ts: vec![s(1234567892, 3.45), s(1234567890, 1.23), s(1234567893, 3.45), s(1234567891, 2.34)],
            filter: Some(1234567892),
            response_len: 2,
            lowest_ts: 1234567892,
            highest_ts: 1234567893,
            dropped_samples: 2,
        },
        Case {
            name: "Filter applied, samples not consecutive",
            ts: vec![s(1234567890, 1.23), s(1234567892, 3.45), s(1234567895, 6.78), s(1234567897, 6.78)],
            filter: Some(1234567895),
            response_len: 2,
            lowest_ts: 1234567895,
            highest_ts: 1234567897,
            dropped_samples: 2,
        },
    ];
    for mut tc in cases {
        let stats = match tc.filter {
            Some(limit) => {
                let mut f = |ts: &prompb::TimeSeries| filter_ts_limit(limit, ts);
                build_time_series(&mut tc.ts, Some(&mut f))
            }
            None => build_time_series(&mut tc.ts, None),
        };
        assert_eq!(tc.response_len, tc.ts.len(), "{}", tc.name);
        assert_eq!(tc.highest_ts, stats.highest, "{}", tc.name);
        assert_eq!(tc.lowest_ts, stats.lowest, "{}", tc.name);
        assert_eq!(tc.dropped_samples, stats.dropped_samples, "{}", tc.name);
    }
}

#[test]
fn test_populate_v2_time_series_unexpected_metadata() {
    let mut symbol_table = SymbolTable::new();
    let mut pending_data = vec![PendingV2::default(); 4];

    let item = |t: SeriesType, name: &str| TimeSeries { s_type: t, series_labels: labels::from_strings(&["__name__", name]), ..Default::default() };
    let batch = vec![
        item(SeriesType::Sample, "metric1"),
        item(SeriesType::Metadata, "metric2"),
        item(SeriesType::Sample, "metric3"),
        item(SeriesType::Metadata, "metric4"),
    ];

    let (n_samples, n_exemplars, n_histograms, n_unexpected) =
        populate_v2_time_series(&mut symbol_table, &batch, &mut pending_data, false, false, false);

    assert_eq!(2, n_samples, "Should count 2 samples");
    assert_eq!(0, n_exemplars, "Should count 0 exemplars");
    assert_eq!(0, n_histograms, "Should count 0 histograms");
    assert_eq!(2, n_unexpected, "Should count 2 unexpected metadata");
}

#[test]
fn test_populate_v2_time_series_type_and_unit_labels() {
    let mut symbol_table = SymbolTable::new();
    use writev2::MetadataType as MT;
    let cases = [
        ("counter_with_unit", "counter", "operations", MT::COUNTER, "Counter metric with operations unit"),
        ("gauge_with_bytes", "gauge", "bytes", MT::GAUGE, "Gauge metric with bytes unit"),
        ("histogram_with_seconds", "histogram", "seconds", MT::HISTOGRAM, "Histogram metric with seconds unit"),
        ("summary_with_ratio", "summary", "ratio", MT::SUMMARY, "Summary metric with ratio unit"),
        ("info_no_unit", "info", "", MT::INFO, "Info metric without unit"),
        ("stateset_no_unit", "stateset", "", MT::STATESET, "Stateset metric without unit"),
        ("unknown_type", "unknown_type", "meters", MT::UNSPECIFIED, "Unknown type defaults to unspecified"),
        ("empty_type_with_unit", "", "watts", MT::UNSPECIFIED, "Empty type with unit"),
        ("type_no_unit", "gauge", "", MT::GAUGE, "Type without unit"),
        ("no_type_no_unit", "", "", MT::UNSPECIFIED, "No type and no unit"),
    ];
    for (name, type_label, unit_label, expected_type, description) in cases {
        let mut builder = ScratchBuilder::new(2);
        let metadata = schema::Metadata { name: format!("test_metric_{name}"), typ: type_label.to_string(), unit: unit_label.to_string() };
        metadata.add_to_labels(&mut builder);
        let batch = vec![TimeSeries {
            series_labels: builder.labels(),
            value: 123.45,
            timestamp: now_ms(),
            s_type: SeriesType::Sample,
            ..Default::default()
        }];
        let mut pending_data = vec![PendingV2::default(); 1];

        symbol_table.reset();
        let (n_samples, n_exemplars, n_histograms, _) =
            populate_v2_time_series(&mut symbol_table, &batch, &mut pending_data, false, false, true);

        assert_eq!(1, n_samples, "Should have 1 sample");
        assert_eq!(0, n_exemplars, "Should have 0 exemplars");
        assert_eq!(0, n_histograms, "Should have 0 histograms");
        assert_eq!(expected_type, pending_data[0].metadata.r#type, "Type should match expected for {description}");
        let unit_ref = pending_data[0].metadata.unit_ref;
        assert_eq!(unit_label, symbol_table.symbol(unit_ref), "Unit should match");
    }
}

/// TestPopulateV2TimeSeries_MetadataAndTypeAndUnit verifies that type and unit labels are properly
/// extracted from labels even when d.metadata is not nil (agent mode scenario).
#[test]
fn test_populate_v2_time_series_metadata_and_type_and_unit() {
    let mut symbol_table = SymbolTable::new();
    use writev2::MetadataType as MT;
    let gauge_meta = || Some(Metadata { typ: MetricType::Gauge, unit: "bytes".into(), help: "Test metric".into() });
    let cases: Vec<(&str, &str, &str, Option<Metadata>, MT, &str, bool)> = vec![
        ("type_and_unit_no_meta", "gauge", "bytes", None, MT::GAUGE, "bytes", true),
        ("type_no_unit_no_meta", "counter", "", None, MT::COUNTER, "", true),
        ("no_type_and_unit_no_meta", "gauge", "bytes", None, MT::UNSPECIFIED, "", false),
        ("type_and_unit_and_meta", "gauge", "bytes", gauge_meta(), MT::GAUGE, "bytes", true),
        ("type-and-unit-overrides-meta", "counter", "requests", gauge_meta(), MT::COUNTER, "requests", true),
    ];
    for (name, type_label, unit_label, meta, expected_type, expected_unit, enable) in cases {
        let mut builder = ScratchBuilder::new(3);
        // Simulate labels with __type__ and __unit__ as scraped with type-and-unit-labels feature.
        builder.add(labels::METRIC_NAME, "test_metric");
        if !type_label.is_empty() {
            builder.add("__type__", type_label);
        }
        if !unit_label.is_empty() {
            builder.add("__unit__", unit_label);
        }
        builder.sort();

        let meta_handle = meta.clone().map(Handle::make).unwrap_or_default();
        let batch = vec![TimeSeries {
            series_labels: builder.labels(),
            value: 123.45,
            timestamp: now_ms(),
            s_type: SeriesType::Sample,
            metadata: meta_handle,
            ..Default::default()
        }];
        let mut pending_data = vec![PendingV2::default(); 1];

        symbol_table.reset();
        let (n_samples, n_exemplars, n_histograms, _) =
            populate_v2_time_series(&mut symbol_table, &batch, &mut pending_data, false, false, enable);

        assert_eq!(1, n_samples, "Should have 1 sample");
        assert_eq!(0, n_exemplars, "Should have 0 exemplars");
        assert_eq!(0, n_histograms, "Should have 0 histograms");

        // Verify type is correctly extracted.
        assert_eq!(expected_type, pending_data[0].metadata.r#type, "Type should match expected for {name}");

        // Verify unit is correctly extracted.
        let unit_ref = pending_data[0].metadata.unit_ref;
        let mut actual_unit = "";
        if unit_ref > 0 && (unit_ref as usize) < symbol_table.len() {
            actual_unit = symbol_table.symbol(unit_ref);
        }
        assert_eq!(expected_unit, actual_unit, "Unit should match for {name}");
        if let Some(m) = &meta
            && enable
        {
            assert_eq!(m.help, symbol_table.symbol(pending_data[0].metadata.help_ref));
        }
    }
}

#[test]
fn test_highest_timestamp_on_append() {
    for proto_msg in PROTOS {
        let n_samples = 11 * default_queue_config().capacity as usize;
        let n_series = 3;
        let recs = generate_records(RecCase { no_st: proto_msg == V1, series: n_series, samples_per_series: n_samples / n_series, ..Default::default() });

        let (_, m) = new_test_client_and_queue_manager(DEFAULT_FLUSH_DEADLINE, proto_msg);
        m.start();

        assert_eq!(0.0, m.metrics.highest_timestamp.get());

        m.store_series(&recs.series, 0);
        assert!(m.append(&recs.samples));

        // Check that Append sets the highest timestamp correctly.
        // NOTE: testwal.GenerateRecords yields nSamples/nSeries samples (36666), with <i for samplesPerSeries> timestamp.
        // This gives the highest timestamp of 36666/1000 (seconds).
        let expected_highest_ts_seconds = 36.0;
        assert_eq!(expected_highest_ts_seconds, m.metrics.highest_timestamp.get());
        m.stop();
    }
}

fn exp_hist(count: u64, sum: f64, pos: Vec<i64>, neg: Option<Vec<i64>>) -> Histogram {
    Histogram {
        schema: 0,
        zero_threshold: 1e-128,
        zero_count: 0,
        count,
        sum,
        positive_spans: vec![Span { offset: 0, length: 1 }],
        positive_buckets: pos,
        negative_spans: if neg.is_some() { vec![Span { offset: 0, length: 1 }] } else { vec![] },
        negative_buckets: neg.unwrap_or_default(),
        ..Default::default()
    }
}

#[test]
fn test_append_histogram_schema_validation() {
    for proto_msg in PROTOS {
        let c = TestWriteClient::new(proto_msg);
        let mut cfg = test_default_queue_config();
        let mcfg = default_metadata_config();
        cfg.max_shards = 1;

        let m = new_test_queue_manager(cfg, mcfg, DEFAULT_FLUSH_DEADLINE, c.clone(), proto_msg);
        m.set_send_native_histograms(true);

        // Create series for the histograms
        let series = vec![RefSeries { r#ref: 0, labels: labels::from_strings(&["__name__", "test_histogram"]) }];
        m.store_series(&series, 0);

        let valid = || exp_hist(2, 5.0, vec![2], Some(vec![1]));
        let nhcb = Histogram {
            schema: CUSTOM_BUCKETS_SCHEMA, // Not valid for version 1.0.
            zero_threshold: 1e-128,
            zero_count: 0,
            count: 1,
            sum: 3.0,
            positive_spans: vec![Span { offset: 0, length: 1 }],
            positive_buckets: vec![1],
            custom_values: custom_values(&[2.0]),
            ..Default::default()
        };
        let histograms = vec![
            RefHistogramSample { r#ref: 0, st: 0, t: 1234567890, h: Arc::new(valid()) },
            RefHistogramSample { r#ref: 0, st: 0, t: 1234567891, h: Arc::new(nhcb) },
            RefHistogramSample { r#ref: 0, st: 0, t: 1234567892, h: Arc::new(valid()) },
        ];
        let fvalid = || FloatHistogram {
            schema: 0,
            zero_threshold: 1e-128,
            zero_count: 0.0,
            count: 2.0,
            sum: 5.0,
            positive_spans: vec![Span { offset: 0, length: 1 }],
            positive_buckets: vec![2.0],
            negative_spans: vec![Span { offset: 0, length: 1 }],
            negative_buckets: vec![1.0],
            ..Default::default()
        };
        let fnhcb = FloatHistogram {
            schema: CUSTOM_BUCKETS_SCHEMA, // Not valid for version 1.0.
            zero_threshold: 1e-128,
            zero_count: 0.0,
            count: 1.0,
            sum: 3.0,
            positive_spans: vec![Span { offset: 0, length: 1 }],
            positive_buckets: vec![1.0],
            custom_values: custom_values(&[2.0]),
            ..Default::default()
        };
        let float_histograms = vec![
            RefFloatHistogramSample { r#ref: 0, st: 0, t: 1234567890, fh: Arc::new(fvalid()) },
            RefFloatHistogramSample { r#ref: 0, st: 0, t: 1234567891, fh: Arc::new(fnhcb) },
            RefFloatHistogramSample { r#ref: 0, st: 0, t: 1234567892, fh: Arc::new(fvalid()) },
        ];

        if proto_msg == V1 {
            c.expect_histograms(&[histograms[0].clone(), histograms[2].clone()], &series);
            c.expect_float_histograms(&[float_histograms[0].clone(), float_histograms[2].clone()], &series);
        } else {
            c.expect_histograms(&histograms, &series);
            c.expect_float_histograms(&float_histograms, &series);
        }

        m.start();

        // Get initial dropped histograms count
        let initial_dropped = dropped(&m.metrics.dropped_histograms_total, REASON_NHCB_NOT_SUPPORTED);

        assert!(m.append_histograms(&histograms));
        assert!(m.append_float_histograms(&float_histograms));

        // Wait for the valid histogram to be received
        c.wait_for_expected_data(Duration::from_secs(30));

        // Verify that one histogram was dropped due to invalid schema
        let final_dropped = dropped(&m.metrics.dropped_histograms_total, REASON_NHCB_NOT_SUPPORTED);
        if proto_msg == V1 {
            assert_eq!(initial_dropped + 2.0, final_dropped, "Expected exactly two histograms to be dropped due to invalid schema");
        } else {
            assert_eq!(initial_dropped, final_dropped, "No histograms should be dropped");
        }

        // Verify no failed histograms (this would indicate a different type of error)
        assert_eq!(0.0, counter(&m.metrics.failed_histograms_total));
        m.stop();
    }
}

/// TestAppendHistogramsWithStartTimestamp verifies that AppendHistograms and
/// AppendFloatHistograms propagate the per-sample start timestamp end-to-end
/// through the queue manager into a Remote Write 2.0 request.
#[test]
fn test_append_histograms_with_start_timestamp() {
    let proto_msg = V2;

    let c = TestWriteClient::new(proto_msg);
    let mut cfg = test_default_queue_config();
    let mcfg = default_metadata_config();
    cfg.max_shards = 1;

    let m = new_test_queue_manager(cfg, mcfg, DEFAULT_FLUSH_DEADLINE, c.clone(), proto_msg);
    m.set_send_native_histograms(true);

    let series = vec![RefSeries { r#ref: 0, labels: labels::from_strings(&["__name__", "test_histogram"]) }];
    m.store_series(&series, 0);

    let int_hist = |seed: i64| {
        Arc::new(Histogram {
            schema: 0,
            zero_threshold: 1e-128,
            count: (seed + 1) as u64,
            sum: seed as f64,
            positive_spans: vec![Span { offset: 0, length: 1 }],
            positive_buckets: vec![seed + 1],
            ..Default::default()
        })
    };
    let float_hist = |seed: i64| {
        Arc::new(FloatHistogram {
            schema: 0,
            zero_threshold: 1e-128,
            count: (seed + 1) as f64,
            sum: seed as f64,
            positive_spans: vec![Span { offset: 0, length: 1 }],
            positive_buckets: vec![(seed + 1) as f64],
            ..Default::default()
        })
    };

    let histograms = vec![
        RefHistogramSample { r#ref: 0, st: 100, t: 200, h: int_hist(0) },
        RefHistogramSample { r#ref: 0, st: 100, t: 300, h: int_hist(1) },
        RefHistogramSample { r#ref: 0, st: 250, t: 400, h: int_hist(2) },
    ];
    let float_histograms = vec![
        RefFloatHistogramSample { r#ref: 0, st: 101, t: 201, fh: float_hist(0) },
        RefFloatHistogramSample { r#ref: 0, st: 101, t: 301, fh: float_hist(1) },
        RefFloatHistogramSample { r#ref: 0, st: 251, t: 401, fh: float_hist(2) },
    ];

    c.expect_histograms(&histograms, &series);
    c.expect_float_histograms(&float_histograms, &series);

    m.start();

    assert!(m.append_histograms(&histograms));
    assert!(m.append_float_histograms(&float_histograms));

    c.wait_for_expected_data(Duration::from_secs(30));
    m.stop();
}

fn received_samples(c: &TestWriteClient, key: &str) -> Vec<writev2::Sample> {
    c.lock().received_samples.get(key).cloned().unwrap_or_default()
}

#[test]
fn test_append_to_convert_nhcb_to_classic() {
    let spans = || vec![Span { offset: 0, length: 2 }, Span { offset: 4, length: 1 }, Span { offset: 1, length: 2 }];
    let cv: Vec<f64> = (1..=10).map(|i| i as f64).collect();
    let h = Arc::new(Histogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        custom_values: custom_values(&cv),
        positive_spans: spans(),
        positive_buckets: vec![1, 2, 3, 4, 5],
        count: 35,
        sum: 123.0,
        ..Default::default()
    });
    let fh = Arc::new(FloatHistogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        custom_values: custom_values(&cv),
        positive_spans: spans(),
        positive_buckets: vec![1.0, 2.0, 3.0, 4.0, 5.0],
        count: 15.0,
        sum: 123.0,
        ..Default::default()
    });
    let exp_h = Arc::new(Histogram {
        schema: 0,
        zero_threshold: 1e-128,
        count: 10,
        sum: 20.0,
        positive_spans: vec![Span { offset: 0, length: 1 }],
        positive_buckets: vec![10],
        ..Default::default()
    });
    let exp_fh = Arc::new(FloatHistogram {
        schema: 0,
        zero_threshold: 1e-128,
        count: 10.0,
        sum: 20.0,
        positive_spans: vec![Span { offset: 0, length: 1 }],
        positive_buckets: vec![10.0],
        ..Default::default()
    });

    let rename = relabel::Config {
        source_labels: Some(vec!["__name__".to_string()]),
        regex: Regexp::must_new("test_histogram"),
        target_label: "__name__".to_string(),
        replacement: "relabeled_histogram".to_string(),
        action: Action::Replace,
        name_validation_scheme: ValidationScheme::Utf8,
        ..Default::default()
    };
    let drop_cfg = relabel::Config {
        source_labels: Some(vec!["__name__".to_string()]),
        regex: Regexp::must_new("test_histogram"),
        action: Action::Drop,
        name_validation_scheme: ValidationScheme::Utf8,
        ..Default::default()
    };

    struct Case {
        name: &'static str,
        proto_msg: ProtoMsg,
        is_float_histogram: bool,
        send_native_histograms: bool,
        convert_flag: bool,
        relabel_configs: Vec<relabel::Config>,
        expected_metric_name: &'static str,
        expected_samples_count: f64,
        expected_histograms_count: f64,
        expected_dropped_series: f64,
    }
    let case = |name, proto_msg, is_float_histogram, send_native_histograms, convert_flag, expected_samples_count, expected_histograms_count| Case {
        name,
        proto_msg,
        is_float_histogram,
        send_native_histograms,
        convert_flag,
        relabel_configs: vec![],
        expected_metric_name: "test_histogram",
        expected_samples_count,
        expected_histograms_count,
        expected_dropped_series: 0.0,
    };
    let cases = vec![
        case("convert nhcb to classic v1", V1, false, true, true, 13.0, 0.0),
        case("convert float nhcb to classic v1", V1, true, true, true, 13.0, 0.0),
        case("convert nhcb to classic v2", V2, false, true, true, 13.0, 0.0),
        case("convert float nhcb to classic v2", V2, true, true, true, 13.0, 0.0),
        case("convert nhcb to classic with send_native_histograms=false", V1, false, false, true, 13.0, 0.0),
        case("convert float nhcb to classic with send_native_histograms=false", V1, true, false, true, 13.0, 0.0),
        Case {
            relabel_configs: vec![rename],
            expected_metric_name: "relabeled_histogram",
            ..case("convert nhcb to classic after write_relabel_configs rename", V2, false, true, true, 13.0, 0.0)
        },
        Case {
            relabel_configs: vec![drop_cfg],
            expected_metric_name: "",
            expected_dropped_series: 1.0,
            ..case("drop nhcb via write_relabel_configs before conversion", V1, false, false, true, 0.0, 0.0)
        },
        Case { expected_metric_name: "", ..case("no nhcb to classic conversion v2", V2, false, true, false, 0.0, 1.0) },
        Case { expected_metric_name: "", ..case("no float nhcb to classic conversion v2", V2, true, true, false, 0.0, 1.0) },
    ];

    for tc in cases {
        let c = TestWriteClient::new(tc.proto_msg);
        let mut cfg = test_default_queue_config();
        let mcfg = default_metadata_config();
        cfg.max_shards = 1;

        let m = new_test_queue_manager(cfg, mcfg, DEFAULT_FLUSH_DEADLINE, c.clone(), tc.proto_msg);
        m.set_convert_nhcb_to_classic(tc.convert_flag);
        m.set_send_native_histograms(tc.send_native_histograms);
        m.set_relabel_configs(tc.relabel_configs.clone());

        let series = vec![
            RefSeries { r#ref: 0, labels: labels::from_strings(&["__name__", "test_histogram"]) },
            RefSeries { r#ref: 1, labels: labels::from_strings(&["__name__", "exp_histogram"]) },
        ];
        m.store_series(&series, 0);

        m.start();

        let initial_conv_err = dropped(&m.metrics.dropped_histograms_total, REASON_NHCB_CONVERSION_ERROR);

        let (st, ts) = (1234560000i64, 1234567890i64);
        if !tc.is_float_histogram {
            let mut samples = vec![RefHistogramSample { r#ref: 0, st, t: ts, h: h.clone() }];
            if !tc.send_native_histograms {
                // Non-NHCB histograms must be ignored when send_native_histograms is false.
                samples.push(RefHistogramSample { r#ref: 1, st, t: ts, h: exp_h.clone() });
            }
            assert!(m.append_histograms(&samples));
        } else {
            let mut samples = vec![RefFloatHistogramSample { r#ref: 0, st, t: ts, fh: fh.clone() }];
            if !tc.send_native_histograms {
                samples.push(RefFloatHistogramSample { r#ref: 1, st, t: ts, fh: exp_fh.clone() });
            }
            assert!(m.append_float_histograms(&samples));
        }

        assert!(
            eventually(Duration::from_secs(5), Duration::from_millis(10), || {
                counter(&m.metrics.samples_total) == tc.expected_samples_count
                    && counter(&m.metrics.histograms_total) == tc.expected_histograms_count
                    && dropped(&m.metrics.dropped_histograms_total, REASON_DROPPED_SERIES) == tc.expected_dropped_series
            }),
            "{}",
            tc.name
        );

        let final_conv_err = dropped(&m.metrics.dropped_histograms_total, REASON_NHCB_CONVERSION_ERROR);
        assert_eq!(initial_conv_err, final_conv_err, "No conversion errors should occur");
        assert_eq!(0.0, counter(&m.metrics.failed_histograms_total));

        if tc.expected_samples_count > 0.0 {
            let expected_st = if tc.proto_msg == V2 { st } else { 0 };
            let expected_count = if tc.is_float_histogram { fh.count as u64 } else { h.count };
            let n = tc.expected_metric_name;
            assert_eq!(
                vec![writev2::Sample { start_timestamp: expected_st, timestamp: ts, value: expected_count as f64 }],
                received_samples(&c, &format!("{{__name__=\"{n}_count\"}}")),
                "{}",
                tc.name
            );
            assert_eq!(
                vec![writev2::Sample { start_timestamp: expected_st, timestamp: ts, value: 123.0 }],
                received_samples(&c, &format!("{{__name__=\"{n}_sum\"}}")),
                "{}",
                tc.name
            );
            assert_eq!(
                vec![writev2::Sample { start_timestamp: expected_st, timestamp: ts, value: expected_count as f64 }],
                received_samples(&c, &format!("{{__name__=\"{n}_bucket\", le=\"+Inf\"}}")),
                "{}",
                tc.name
            );
        }
        m.stop();
    }

    let base_series = RefSeries { r#ref: 1, labels: labels::from_strings(&["__name__", "latency", "job", "api"]) };
    let classic_series = vec![
        RefSeries { r#ref: 1, labels: labels::from_strings(&["__name__", "latency_bucket", "job", "api", "le", "1.0"]) },
        RefSeries { r#ref: 2, labels: labels::from_strings(&["__name__", "latency_bucket", "job", "api", "le", "2.0"]) },
        RefSeries { r#ref: 3, labels: labels::from_strings(&["__name__", "latency_bucket", "job", "api", "le", "+Inf"]) },
        RefSeries { r#ref: 4, labels: labels::from_strings(&["__name__", "latency_count", "job", "api"]) },
        RefSeries { r#ref: 5, labels: labels::from_strings(&["__name__", "latency_sum", "job", "api"]) },
    ];
    let classic_values = [2.0, 5.0, 9.0, 9.0, 13.0];
    let new_histogram = || Histogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        custom_values: custom_values(&[1.0, 2.0]),
        positive_spans: vec![Span { offset: 0, length: 3 }],
        positive_buckets: vec![2, 1, 1],
        count: 9,
        sum: 13.0,
        ..Default::default()
    };
    let append_histogram = |m: &QueueManager, h: Histogram, ts: i64, as_float: bool| {
        h.validate().unwrap();
        if as_float {
            assert!(m.append_float_histograms(&[RefFloatHistogramSample { r#ref: base_series.r#ref, st: 0, t: ts, fh: Arc::new(h.to_float()) }]));
        } else {
            assert!(m.append_histograms(&[RefHistogramSample { r#ref: base_series.r#ref, st: 0, t: ts, h: Arc::new(h) }]));
        }
    };
    let require_initial_samples = |c: &TestWriteClient| {
        for (i, series) in classic_series.iter().enumerate() {
            let got = received_samples(c, &series.labels.to_string());
            assert!(!got.is_empty(), "normal sample for {}", series.labels);
            assert_eq!(writev2::Sample { start_timestamp: 0, timestamp: 1000, value: classic_values[i] }, got[0], "normal sample for {}", series.labels);
        }
    };
    let stale = stale_nan();

    // sample type transitions
    for proto_msg in PROTOS {
        for (target_name, kind, send_native) in [
            ("ordinary float", "float", true),
            ("integer exponential histogram", "integer", true),
            ("float exponential histogram", "histogram", true),
            ("integer exponential without forwarding", "integer", false),
            ("float exponential without forwarding", "histogram", false),
        ] {
            if !send_native && proto_msg == V2 {
                continue;
            }
            for prior in ["ordinary control", "after integer NHCB", "after float NHCB"] {
                let what = format!("{proto_msg}/{target_name}/{prior}");
                let c = TestWriteClient::new(proto_msg);
                let (m, stop) = new_test_nhcb_queue_manager(
                    c.clone(),
                    NhcbOpts { proto_msg, send_native_histograms: send_native, send_exemplars: send_native, ..Default::default() },
                );
                m.store_series(&[base_series.clone()], 0);
                let converted = prior != "ordinary control";
                if converted {
                    append_histogram(&m, new_histogram(), 1000, prior == "after float NHCB");
                }
                let before = m.data_in_expanded.new_events.load(Ordering::SeqCst);
                let mut h = new_histogram();
                h.schema = 0;
                h.custom_values = None;
                h.validate().unwrap();
                let h = Arc::new(h);
                match kind {
                    "float" => assert!(m.append(&[RefSample { r#ref: base_series.r#ref, st: 1500, t: 2000, v: 17.0 }])),
                    "integer" => assert!(m.append_histograms(&[RefHistogramSample { r#ref: base_series.r#ref, st: 1500, t: 2000, h: h.clone() }])),
                    _ => assert!(m.append_float_histograms(&[RefFloatHistogramSample { r#ref: base_series.r#ref, st: 1500, t: 2000, fh: Arc::new(h.to_float()) }])),
                }
                let mut extra = 0i64;
                if converted {
                    extra = 5;
                    if !send_native {
                        extra -= 1;
                    }
                }
                let expanded = m.data_in_expanded.new_events.load(Ordering::SeqCst) - before;
                if send_native {
                    assert!(m.append_exemplars(&[RefExemplar { r#ref: base_series.r#ref, t: 2500, v: 1.5, labels: labels::from_strings(&["trace_id", "abc"]) }]));
                }
                let before_stale = m.data_in_expanded.new_events.load(Ordering::SeqCst);
                match kind {
                    "float" => assert!(m.append(&[RefSample { r#ref: base_series.r#ref, st: 0, t: 3000, v: stale }])),
                    "integer" => assert!(m.append_histograms(&[RefHistogramSample {
                        r#ref: base_series.r#ref,
                        st: 0,
                        t: 3000,
                        h: Arc::new(Histogram { sum: stale, ..Default::default() })
                    }])),
                    _ => assert!(m.append_float_histograms(&[RefFloatHistogramSample {
                        r#ref: base_series.r#ref,
                        st: 0,
                        t: 3000,
                        fh: Arc::new(FloatHistogram { sum: stale, ..Default::default() })
                    }])),
                }
                let after_stale = m.data_in_expanded.new_events.load(Ordering::SeqCst);
                stop.stop();
                // expansion accounting
                assert_eq!(extra, expanded, "{what}: retirement and replacement must be accounted together");
                assert_eq!(before_stale, after_stale, "{what}: the replacement's marker must not expand into old classic series");
                if converted {
                    // converted series retirement
                    require_initial_samples(&c);
                    for series in &classic_series {
                        let got = received_samples(&c, &series.labels.to_string());
                        assert_eq!(2, got.len(), "{what}: retired series {}", series.labels);
                        assert_eq!(2000, got[1].timestamp);
                        assert_eq!(0, got[1].start_timestamp, "{what}: retirement must not borrow the replacement's start time");
                        assert!(is_stale_nan(got[1].value));
                    }
                }
                if !send_native {
                    let st = c.lock();
                    assert!(st.received_histograms.is_empty(), "{what}");
                    assert!(st.received_float_histograms.is_empty(), "{what}");
                    continue;
                }
                let expected_st = if proto_msg == V2 { 1500 } else { 0 };
                let key = base_series.labels.to_string();
                // replacement identity
                if kind == "float" {
                    let got = received_samples(&c, &key);
                    assert_eq!(2, got.len(), "{what}: the replacement float must receive its own stale marker");
                    assert_eq!(writev2::Sample { start_timestamp: expected_st, timestamp: 2000, value: 17.0 }, got[0]);
                    assert_eq!(3000, got[1].timestamp);
                    assert!(is_stale_nan(got[1].value));
                } else {
                    let st = c.lock();
                    let (got, want) = if kind == "histogram" {
                        (st.received_float_histograms.get(&key).cloned().unwrap_or_default(), writev2::from_float_histogram(expected_st, 2000, &h.to_float()))
                    } else {
                        (st.received_histograms.get(&key).cloned().unwrap_or_default(), writev2::from_int_histogram(expected_st, 2000, &h))
                    };
                    assert_eq!(2, got.len(), "{what}: the replacement histogram must receive its own stale marker");
                    assert_eq!(want, got[0], "{what}");
                    assert_eq!(3000, got[1].timestamp);
                    assert!(is_stale_nan(got[1].sum));
                }
                // exemplar identity
                let st = c.lock();
                let want: HashMap<String, Vec<prompb::Exemplar>> = HashMap::from([(
                    key.clone(),
                    vec![prompb::Exemplar { timestamp: 2500, value: 1.5, labels: vec![prompb::Label::new("trace_id", "abc")] }],
                )]);
                assert_eq!(want, st.received_exemplars, "{what}: replacement exemplars must not retain bucket identity");
            }
        }
    }

    // age filtered replacements preserve conversion. adapted: real clock (Go uses synctest).
    {
        // A type change dropped by sample_age_limit must neither retire the classic series
        // nor stop their conversion; otherwise the series' later stale marker would miss them.
        let c = TestWriteClient::new(V1);
        // Native forwarding is on, so a wrongly accepted histogram replacement would be sent.
        let (m, stop) = new_test_nhcb_queue_manager(
            c.clone(),
            NhcbOpts {
                proto_msg: V1,
                send_native_histograms: true,
                sample_age_limit: Duration::from_secs(1),
                batch_send_deadline: Duration::from_millis(1),
                ..Default::default()
            },
        );
        m.store_series(&[base_series.clone()], 0);
        let first_t = now_ms();
        append_histogram(&m, new_histogram(), first_t, false);
        std::thread::sleep(Duration::from_secs(2));
        // Each sample type change is now too old to send.
        assert!(m.append(&[RefSample { r#ref: base_series.r#ref, st: 0, t: first_t + 1, v: 17.0 }]));
        let mut h = new_histogram();
        h.schema = 0;
        h.custom_values = None;
        assert!(m.append_histograms(&[RefHistogramSample { r#ref: base_series.r#ref, st: 0, t: first_t + 2, h: Arc::new(h.clone()) }]));
        assert!(m.append_float_histograms(&[RefFloatHistogramSample { r#ref: base_series.r#ref, st: 0, t: first_t + 3, fh: Arc::new(h.to_float()) }]));
        // A fresh stale marker must still reach every classic series.
        let stale_t = now_ms();
        assert!(m.append(&[RefSample { r#ref: base_series.r#ref, st: 0, t: stale_t, v: stale }]));
        stop.stop();
        for series in &classic_series {
            let got = received_samples(&c, &series.labels.to_string());
            assert_eq!(2, got.len());
            assert_eq!(stale_t, got[1].timestamp);
            assert!(is_stale_nan(got[1].value));
        }
        // The dropped samples must not have been forwarded.
        let st = c.lock();
        assert_eq!(classic_series.len(), st.received_samples.len());
        assert!(st.received_histograms.is_empty());
        assert!(st.received_float_histograms.is_empty());
    }

    // staleness
    for proto_msg in PROTOS {
        for as_float in [false, true] {
            for send_native in [false, true] {
                for marker in ["float", "histogram without layout", "custom bucket histogram"] {
                    let what = format!("{proto_msg}/float={as_float}/send native histograms={send_native}/{marker}");
                    let c = TestWriteClient::new(proto_msg);
                    let (m, stop) =
                        new_test_nhcb_queue_manager(c.clone(), NhcbOpts { proto_msg, send_native_histograms: send_native, ..Default::default() });
                    m.store_series(&[base_series.clone()], 0);
                    let h = new_histogram();
                    append_histogram(&m, h.clone(), 1000, as_float);
                    match marker {
                        // Separate scrape transactions produce a float WAL staleness marker.
                        "float" => assert!(m.append(&[RefSample { r#ref: base_series.r#ref, st: 0, t: 2000, v: stale }])),
                        "histogram without layout" => append_histogram(&m, Histogram { sum: stale, ..Default::default() }, 2000, as_float),
                        _ => {
                            let mut h = h.clone();
                            h.sum = stale;
                            append_histogram(&m, h, 2000, as_float);
                        }
                    }
                    stop.stop();
                    require_initial_samples(&c);
                    for series in &classic_series {
                        let got = received_samples(&c, &series.labels.to_string());
                        assert_eq!(2, got.len(), "{what}: converted series {} must receive a stale marker", series.labels);
                        assert_eq!(2000, got[1].timestamp);
                        assert!(is_stale_nan(got[1].value), "{what}: converted series {} must be stale", series.labels);
                    }
                    assert_eq!(classic_series.len(), c.lock().received_samples.len(), "{what}");
                }
            }
        }
    }
    // ordinary float control
    {
        let c = TestWriteClient::new(V1);
        let (m, stop) = new_test_nhcb_queue_manager(c.clone(), NhcbOpts { proto_msg: V1, ..Default::default() });
        m.store_series(&[base_series.clone()], 0);
        assert!(m.append(&[
            RefSample { r#ref: base_series.r#ref, st: 0, t: 1000, v: 13.0 },
            RefSample { r#ref: base_series.r#ref, st: 0, t: 2000, v: stale },
        ]));
        stop.stop();
        let got = received_samples(&c, &base_series.labels.to_string());
        assert_eq!(2, got.len());
        assert_eq!(writev2::Sample { start_timestamp: 0, timestamp: 1000, value: 13.0 }, got[0]);
        assert_eq!(2000, got[1].timestamp);
        assert!(is_stale_nan(got[1].value));
    }

    // removed boundaries
    for proto_msg in PROTOS {
        for as_float in [false, true] {
            let what = format!("{proto_msg}/float={as_float}");
            let c = TestWriteClient::new(proto_msg);
            let (m, stop) = new_test_nhcb_queue_manager(c.clone(), NhcbOpts { proto_msg, ..Default::default() });
            m.store_series(&[base_series.clone()], 0);
            // Start with finite boundaries 1 and 2, then remove 1 while the histogram stays active.
            append_histogram(&m, new_histogram(), 1000, as_float);
            append_histogram(
                &m,
                Histogram {
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    custom_values: custom_values(&[2.0]),
                    positive_spans: vec![Span { offset: 0, length: 2 }],
                    positive_buckets: vec![5, 0],
                    count: 10,
                    sum: 15.0,
                    ..Default::default()
                },
                2000,
                as_float,
            );
            stop.stop();
            require_initial_samples(&c);
            for (i, v) in [5.0, 10.0, 10.0, 15.0].into_iter().enumerate() {
                let series = &classic_series[i + 1];
                assert_eq!(
                    vec![
                        writev2::Sample { start_timestamp: 0, timestamp: 1000, value: classic_values[i + 1] },
                        writev2::Sample { start_timestamp: 0, timestamp: 2000, value: v },
                    ],
                    received_samples(&c, &series.labels.to_string()),
                    "{what}: retained series {}",
                    series.labels
                );
            }
            // The removed le="1.0" bucket must become stale while the retained series keep updating.
            let got = received_samples(&c, &classic_series[0].labels.to_string());
            assert_eq!(2, got.len(), "{what}: removed bucket must receive a stale marker");
            assert_eq!(2000, got[1].timestamp);
            assert!(is_stale_nan(got[1].value));
        }
    }

    // boundary layouts
    // When an NHCB series changes bucket layout, every bucket of the new layout is sent, and
    // each finite bucket it dropped gets one stale marker without a start time. +Inf, _count
    // and _sum exist in every layout, so they are never retired.
    for proto_msg in PROTOS {
        for as_float in [false, true] {
            let layouts: [(&str, Vec<f64>, Vec<f64>); 6] = [
                ("unchanged", vec![1.0, 2.0], vec![1.0, 2.0]),
                ("added boundary", vec![1.0, 2.0], vec![0.5, 1.0, 2.0]),
                ("removed trailing boundary", vec![1.0, 2.0], vec![1.0]),
                ("interleaved replacement", vec![1.0, 2.0], vec![1.5, 2.5]),
                ("all finite boundaries removed", vec![1.0, 2.0], vec![]),
                ("first finite boundary added", vec![], vec![1.0]),
            ];
            for (name, before, after) in layouts {
                let what = format!("{proto_msg}/float={as_float}/{name}");
                let c = TestWriteClient::new(proto_msg);
                let (m, stop) = new_test_nhcb_queue_manager(c.clone(), NhcbOpts { proto_msg, ..Default::default() });
                m.store_series(&[base_series.clone()], 0);
                let mut expected: HashMap<String, Vec<writev2::Sample>> = HashMap::new();
                for (i, bounds) in [&before, &after].into_iter().enumerate() {
                    let mut pb = vec![0i64; bounds.len() + 1];
                    // One observation per bucket gives cumulative counts 1, 2, ... including +Inf.
                    pb[0] = 1;
                    let h = Histogram {
                        schema: CUSTOM_BUCKETS_SCHEMA,
                        custom_values: custom_values(bounds),
                        positive_spans: vec![Span { offset: 0, length: (bounds.len() + 1) as u32 }],
                        positive_buckets: pb,
                        count: (bounds.len() + 1) as u64,
                        sum: 13.0,
                        ..Default::default()
                    };
                    h.validate().unwrap();
                    let ts = 1000 * (i as i64 + 1);
                    let mut st = ts - 500;
                    if as_float {
                        assert!(m.append_float_histograms(&[RefFloatHistogramSample { r#ref: base_series.r#ref, st, t: ts, fh: Arc::new(h.to_float()) }]));
                    } else {
                        assert!(m.append_histograms(&[RefHistogramSample { r#ref: base_series.r#ref, st, t: ts, h: Arc::new(h.clone()) }]));
                    }
                    // Only remote write v2 carries start times.
                    if proto_msg == V1 {
                        st = 0;
                    }
                    let mut all: Vec<f64> = bounds.clone();
                    all.push(f64::INFINITY);
                    for (j, bound) in all.into_iter().enumerate() {
                        let ls = labels::from_strings(&["__name__", "latency_bucket", "job", "api", "le", &format_open_metrics_float(bound)]);
                        expected.entry(ls.to_string()).or_default().push(writev2::Sample { start_timestamp: st, timestamp: ts, value: (j + 1) as f64 });
                    }
                    for (n, v) in [("latency_count", h.count as f64), ("latency_sum", h.sum)] {
                        let ls = labels::from_strings(&["__name__", n, "job", "api"]);
                        expected.entry(ls.to_string()).or_default().push(writev2::Sample { start_timestamp: st, timestamp: ts, value: v });
                    }
                }
                // Buckets missing from the new layout are retired at its timestamp.
                for bound in &before {
                    if !after.contains(bound) {
                        let ls = labels::from_strings(&["__name__", "latency_bucket", "job", "api", "le", &format_open_metrics_float(*bound)]);
                        expected.entry(ls.to_string()).or_default().push(writev2::Sample { start_timestamp: 0, timestamp: 2000, value: stale });
                    }
                }
                stop.stop();
                // Nothing beyond the expected series and samples may be sent.
                let st = c.lock();
                assert_eq!(expected.len(), st.received_samples.len(), "{what}");
                for (key, want) in &expected {
                    let got = st.received_samples.get(key).cloned().unwrap_or_default();
                    assert_eq!(want.len(), got.len(), "{what}: {key}");
                    for (i, sample) in want.iter().enumerate() {
                        assert_eq!(sample.timestamp, got[i].timestamp);
                        if is_stale_nan(sample.value) {
                            assert!(is_stale_nan(got[i].value), "{what}: {key}");
                            assert_eq!(0, got[i].start_timestamp, "{what}: retired bucket {key} must not borrow the new layout's start time");
                        } else {
                            assert_eq!(*sample, got[i], "{what}: {key}");
                        }
                    }
                }
            }
        }
    }

    // shared bucket layouts
    {
        let (m, stop) = new_test_nhcb_queue_manager(TestWriteClient::new(V1), NhcbOpts { proto_msg: V1, ..Default::default() });
        let other = RefSeries { r#ref: 2, labels: labels::from_strings(&["__name__", "latency", "job", "web"]) };
        m.store_series(&[base_series.clone(), other.clone()], 0);
        let append_to = |r: HeadSeriesRef, h: Histogram, ts: i64| {
            assert!(m.append_histograms(&[RefHistogramSample { r#ref: r, st: 0, t: ts, h: Arc::new(h) }]));
        };
        append_to(base_series.r#ref, new_histogram(), 1000);
        append_to(other.r#ref, new_histogram(), 1000);
        assert_eq!(1, m.nhcb_layouts_len());
        let shared = m.nhcb_series_layout(base_series.r#ref).unwrap();
        assert!(Arc::ptr_eq(&shared, &m.nhcb_series_layout(other.r#ref).unwrap()));
        assert_eq!(2, shared.refs.load(Ordering::SeqCst));

        let mut changed = new_histogram();
        changed.custom_values = custom_values(&[1.0, 3.0]);
        changed.validate().unwrap();
        append_to(other.r#ref, changed, 2000);
        assert_eq!(2, m.nhcb_layouts_len());
        assert_eq!(1, shared.refs.load(Ordering::SeqCst));

        append_to(other.r#ref, Histogram { sum: stale, ..Default::default() }, 3000);
        assert_eq!(1, m.nhcb_layouts_len(), "a stale marker must release its layout");
        assert!(m.nhcb_series_layout(other.r#ref).is_none());

        m.series_reset(1);
        assert_eq!(0, m.nhcb_series_bounds_len());
        assert_eq!(0, m.nhcb_layouts_len(), "garbage collection must release the last reference");
        stop.stop();
    }

    // TODO(tsdb): the "exemplars" subtest needs a TSDB head (tsdb.NewHead) behind the remote write handler;
    // it waits for the tsdb crate.
}
