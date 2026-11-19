//! `promtool push metrics` (metrics.go `PushMetrics`), with util/fmtutil `MetricTextToWriteRequest` and the
//! sending side of client_golang `exp/api/remote` (remote_api.go `NewAPI`, `API.Write`).

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use net::{Client, Request, Response, RoundTripper};
use remote::prompb::{self, Message};
use remote::remoteapi::{
    SNAPPY_BLOCK_COMPRESSION, VERSION_HEADER, VERSION1_HEADER_VALUE, VERSION20_HEADER_VALUE,
    WriteMessageType,
};
use remote::stats::{WriteResponseStats, parse_write_response_stats};
use yaml::url::GoUrl;

use crate::expfmt::{self, Metric, MetricFamily, ValidationScheme};
use crate::{FAILURE_EXIT_CODE, Io, SUCCESS_EXIT_CODE};

/// Go `PushMetrics(url, roundTripper, headers, timeout, protoMsg, apiPath, labels, files...)`: pushes the
/// metrics files (or stdin when `files` is empty) to a remote write endpoint.
#[allow(clippy::too_many_arguments)]
pub fn push_metrics(
    url: &GoUrl,
    round_tripper: Arc<dyn RoundTripper>,
    headers: &BTreeMap<String, String>,
    timeout: Duration,
    proto_msg: &str,
    api_path: &str,
    labels: &BTreeMap<String, String>,
    files: &[String],
    io: &mut Io,
) -> i32 {
    let address_url = match yaml::url::parse(&url.to_url_string()) {
        Ok(u) => u,
        Err(e) => {
            let _ = writeln!(io.stderr, "{e}");
            return FAILURE_EXIT_CODE;
        }
    };

    // Build HTTP client with custom transport for headers.
    let transport: Arc<dyn RoundTripper> =
        Arc::new(SetHeadersTransport { round_tripper, headers: headers.clone() });
    let mut http_client = Client::new(transport);
    // Go http.Client: a zero Timeout means none.
    http_client.timeout = if timeout.is_zero() { None } else { Some(timeout) };

    // Create remote write API client.
    let write_api = match Api::new(
        &address_url.to_url_string(),
        ApiOpts { path: api_path.to_string(), client: http_client, ..ApiOpts::default() },
    ) {
        Ok(a) => a,
        Err(e) => {
            let _ = writeln!(io.stderr, "{e}");
            return FAILURE_EXIT_CODE;
        }
    };

    // Determine message type based on protobuf_message flag.
    let message_type = match proto_msg {
        "io.prometheus.write.v2.Request" => WriteMessageType::V2,
        "prometheus.WriteRequest" => WriteMessageType::V1,
        _ => {
            let _ = writeln!(
                io.stderr,
                "  FAILED: invalid protobuf message {}, must be prometheus.WriteRequest or io.prometheus.write.v2.Request",
                textparse::strconv::quote_bytes(proto_msg.as_bytes())
            );
            return FAILURE_EXIT_CODE;
        }
    };

    if files.is_empty() {
        let mut data = Vec::new();
        if let Err(e) = io.stdin.read_to_end(&mut data) {
            let _ = writeln!(io.stderr, "  FAILED: {}", net::types::go_io_error(&e));
            return FAILURE_EXIT_CODE;
        }
        let _ = writeln!(io.stdout, "Parsing standard input");
        if parse_and_push_metrics(&write_api, message_type, &data, labels, io) {
            let _ = writeln!(io.stdout, "  SUCCESS: metrics pushed to remote write.");
            return SUCCESS_EXIT_CODE;
        }
        return FAILURE_EXIT_CODE;
    }

    let mut failed = false;
    for file in files {
        let data = match read_file(file) {
            Ok(d) => d,
            Err(e) => {
                let _ = writeln!(io.stderr, "  FAILED: {e}");
                failed = true;
                continue;
            }
        };
        let _ = writeln!(io.stdout, "Parsing metrics file {file}");
        if parse_and_push_metrics(&write_api, message_type, &data, labels, io) {
            let _ = writeln!(io.stdout, "  SUCCESS: metrics file {file} pushed to remote write.");
            continue;
        }
        failed = true;
    }

    if failed {
        return FAILURE_EXIT_CODE;
    }
    SUCCESS_EXIT_CODE
}

/// Go `os.ReadFile` error texts.
fn read_file(path: &str) -> Result<Vec<u8>, String> {
    let mut f = std::fs::File::open(path).map_err(|e| format!("open {path}: {}", net::types::go_io_error(&e)))?;
    let mut data = Vec::new();
    f.read_to_end(&mut data).map_err(|e| {
        let msg = if e.raw_os_error() == Some(21) { "is a directory".to_string() } else { net::types::go_io_error(&e) };
        format!("read {path}: {msg}")
    })?;
    Ok(data)
}

fn parse_and_push_metrics(
    write_api: &Api,
    message_type: WriteMessageType,
    data: &[u8],
    labels: &BTreeMap<String, String>,
    io: &mut Io,
) -> bool {
    let metrics_data = match metric_text_to_write_request(data, labels) {
        Ok(m) => m,
        Err(e) => {
            let _ = writeln!(io.stderr, "  FAILED: {e}");
            return false;
        }
    };
    // Write handles marshaling and compression; the message is always a prompb.WriteRequest (1.0), also when
    // sent as a 2.0 message type, as in Go.
    if let Err(e) = write_api.write(message_type, &metrics_data) {
        let _ = writeln!(io.stderr, "  FAILED: {e}");
        return false;
    }
    true
}

/// Go `setHeadersTransport`: sets the configured headers on every request.
struct SetHeadersTransport {
    round_tripper: Arc<dyn RoundTripper>,
    headers: BTreeMap<String, String>,
}

impl RoundTripper for SetHeadersTransport {
    fn round_trip(&self, mut req: Request) -> Result<Response, net::Error> {
        for (k, v) in &self.headers {
            req.header.set(k, v);
        }
        self.round_tripper.round_trip(req)
    }
}

// ---------------------------------------------------------------------------------------------------------------
// util/fmtutil.

/// Go `fmtutil.MetricMetadataTypeValue[mf.Type.String()]` (missing names are 0).
fn metric_metadata_type_value(t: expfmt::MetricType) -> i32 {
    match &*t.name() {
        "UNKNOWN" => 0,
        "COUNTER" => 1,
        "GAUGE" => 2,
        "HISTOGRAM" => 3,
        "GAUGEHISTOGRAM" => 4,
        "SUMMARY" => 5,
        "INFO" => 6,
        "STATESET" => 7,
        _ => 0,
    }
}

/// Go `fmtutil.MetricTextToWriteRequest`.
pub fn metric_text_to_write_request(
    input: &[u8],
    labels: &BTreeMap<String, String>,
) -> Result<prompb::WriteRequest, String> {
    // promtool keeps model.NameValidationScheme at its default, UTF8Validation.
    let (mf, err) = expfmt::text_to_metric_families(ValidationScheme::Utf8, input);
    if let Some(e) = err {
        return Err(e.to_string());
    }
    metric_families_to_write_request(&mf, labels)
}

/// Go `fmtutil.MetricFamiliesToWriteRequest`.
pub fn metric_families_to_write_request(
    mf: &BTreeMap<String, MetricFamily>,
    extra_labels: &BTreeMap<String, String>,
) -> Result<prompb::WriteRequest, String> {
    let mut wr = prompb::WriteRequest::default();
    // BTreeMap iterates in sorted name order (Go sorts the names).
    for (metric_name, fam) in mf {
        // Go `mf.Type.String()` on a nil type is "COUNTER" too (the enum's zero value).
        let mtype = metric_metadata_type_value(fam.get_type());
        wr.metadata.push(prompb::MetricMetadata {
            r#type: prompb::MetricType(mtype),
            metric_family_name: fam.name.clone(),
            help: fam.get_help().to_string(),
            unit: String::new(),
        });
        for metric in &fam.metric {
            let labels = make_labels_map(metric, metric_name, extra_labels);
            make_timeseries(&mut wr, labels, metric)?;
        }
    }
    Ok(wr)
}

fn to_timeseries(wr: &mut prompb::WriteRequest, labels: &BTreeMap<String, String>, timestamp: i64, value: f64) {
    wr.timeseries.push(prompb::TimeSeries {
        labels: make_labels(labels),
        samples: vec![prompb::Sample { value, timestamp }],
        ..Default::default()
    });
}

fn now_millis() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn make_timeseries(
    wr: &mut prompb::WriteRequest,
    mut labels: BTreeMap<String, String>,
    m: &Metric,
) -> Result<(), String> {
    let mut timestamp = m.get_timestamp_ms();
    if timestamp == 0 {
        timestamp = now_millis();
    }
    let fmt_g = textparse::strconv::format_g;

    if let Some(v) = m.gauge {
        to_timeseries(wr, &labels, timestamp, v);
    } else if let Some(v) = m.counter {
        to_timeseries(wr, &labels, timestamp, v);
    } else if let Some(s) = &m.summary {
        let metric_name = labels.get("__name__").cloned().unwrap_or_default();
        // Quantile series first, then the _sum and _count series.
        let mut quantile_labels = labels.clone();
        for q in &s.quantile {
            quantile_labels.insert("quantile".to_string(), fmt_g(q.quantile));
            to_timeseries(wr, &quantile_labels, timestamp, q.value);
        }
        labels.insert("__name__".to_string(), format!("{metric_name}_sum"));
        to_timeseries(wr, &labels, timestamp, s.get_sample_sum());
        labels.insert("__name__".to_string(), format!("{metric_name}_count"));
        to_timeseries(wr, &labels, timestamp, s.get_sample_count() as f64);
    } else if let Some(h) = &m.histogram {
        let metric_name = labels.get("__name__").cloned().unwrap_or_default();
        // Bucket series first, then the _sum and _count series.
        let mut bucket_labels = labels.clone();
        let mut has_inf = false;
        for b in &h.bucket {
            if b.upper_bound == f64::INFINITY {
                has_inf = true;
            }
            bucket_labels.insert("__name__".to_string(), format!("{metric_name}_bucket"));
            bucket_labels.insert("le".to_string(), fmt_g(b.upper_bound));
            to_timeseries(wr, &bucket_labels, timestamp, b.get_cumulative_count() as f64);
        }
        // Add +Inf bucket if not present.
        if !has_inf {
            bucket_labels.insert("__name__".to_string(), format!("{metric_name}_bucket"));
            bucket_labels.insert("le".to_string(), "+Inf".to_string());
            to_timeseries(wr, &bucket_labels, timestamp, h.get_sample_count() as f64);
        }
        labels.insert("__name__".to_string(), format!("{metric_name}_sum"));
        to_timeseries(wr, &labels, timestamp, h.get_sample_sum());
        labels.insert("__name__".to_string(), format!("{metric_name}_count"));
        to_timeseries(wr, &labels, timestamp, h.get_sample_count() as f64);
    } else if let Some(v) = m.untyped {
        to_timeseries(wr, &labels, timestamp, v);
    } else {
        return Err("unsupported metric type".to_string());
    }
    Ok(())
}

fn make_labels(labels_map: &BTreeMap<String, String>) -> Vec<prompb::Label> {
    labels_map.iter().map(|(k, v)| prompb::Label { name: k.clone(), value: v.clone() }).collect()
}

fn make_labels_map(m: &Metric, metric_name: &str, extra_labels: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert("__name__".to_string(), metric_name.to_string());
    for (k, v) in extra_labels {
        labels.insert(k.clone(), v.clone());
    }
    for l in &m.label {
        let name = if l.name == "job" { format!("exported_{}", l.name) } else { l.name.clone() };
        labels.insert(name, l.value.clone());
    }
    labels
}

// ---------------------------------------------------------------------------------------------------------------
// client_golang exp/api/remote: the client side.

/// Go `BackoffConfig`.
#[derive(Clone, Copy, Debug)]
pub struct BackoffConfig {
    pub min: Duration,
    pub max: Duration,
    /// Zero means infinite retries.
    pub max_retries: usize,
}

/// Go `apiOpts` (without the logger: nothing is logged by default).
#[derive(Clone)]
pub struct ApiOpts {
    pub client: Client,
    pub backoff_config: BackoffConfig,
    pub path: String,
    pub retry_on_rate_limit: bool,
}

impl Default for ApiOpts {
    /// Go `defaultAPIOpts` (the client is `http.DefaultClient`).
    fn default() -> Self {
        ApiOpts {
            client: Client::new(Arc::new(net::Transport::default_transport())),
            backoff_config: BackoffConfig {
                min: Duration::from_secs(1),
                max: Duration::from_secs(10),
                max_retries: 10,
            },
            path: "api/v1/write".to_string(),
            retry_on_rate_limit: true,
        }
    }
}

/// Go `API`: a client for the Prometheus remote write protocols.
pub struct Api {
    base_url: GoUrl,
    opts: ApiOpts,
}

/// Go `retryableError`.
struct RetryableError {
    msg: String,
    retry_after: Duration,
}

enum AttemptError {
    Retryable(RetryableError),
    Fatal(String),
}

impl Api {
    /// Go `NewAPI(baseURL, opts...)` (options are the fields of `opts`).
    pub fn new(base_url: &str, opts: ApiOpts) -> Result<Api, String> {
        let mut parsed = yaml::url::parse(base_url).map_err(|e| format!("invalid base URL: {e}"))?;
        parsed.path = path_join(&parsed.path, &opts.path);
        // Go sets Path only; a RawPath that no longer matches is ignored by EscapedPath.
        parsed.raw_path = String::new();
        Ok(Api { base_url: parsed, opts })
    }

    /// Go `API.Write`: marshals, compresses and sends the message with retries. Returns the accumulated
    /// response statistics.
    pub fn write(&self, msg_type: WriteMessageType, msg: &prompb::WriteRequest) -> Result<WriteResponseStats, String> {
        let buf = msg.marshal();
        let payload = remote::codec::snappy_encode(&buf);

        let mut accumulated = WriteResponseStats::default();
        let mut b = Backoff::new(self.opts.backoff_config);
        loop {
            let (rs, res) = self.attempt_write(msg_type, &payload, b.num_retries);
            accumulated = accumulated.add(rs);
            let err = match res {
                Ok(()) => {
                    // Check the case mentioned in PRW 2.0.
                    if msg_type == WriteMessageType::V2 && !accumulated.confirmed && accumulated.no_data_written() {
                        return Err(format!(
                            "sent v2 request; got 2xx, but PRW 2.0 response header statistics indicate {} samples, {} histograms and {} exemplars were accepted; assuming failure e.g. the target only supports PRW 1.0 prometheus.WriteRequest, but does not check the Content-Type header correctly",
                            accumulated.samples, accumulated.histograms, accumulated.exemplars
                        ));
                    }
                    return Ok(accumulated);
                }
                Err(AttemptError::Fatal(e)) => return Err(e),
                Err(AttemptError::Retryable(e)) => e,
            };
            if !b.ongoing() {
                return Err(err.msg);
            }
            let delay = b.next_delay() + err.retry_after;
            std::thread::sleep(delay);
        }
    }

    fn attempt_write(
        &self,
        msg_type: WriteMessageType,
        payload: &[u8],
        attempt: usize,
    ) -> (WriteResponseStats, Result<(), AttemptError>) {
        let mut req = match Request::new("POST", &self.base_url.to_url_string(), payload.to_vec()) {
            Ok(r) => r,
            Err(e) => return (WriteResponseStats::default(), Err(AttemptError::Fatal(e.to_string()))),
        };
        req.header.add("Content-Encoding", SNAPPY_BLOCK_COMPRESSION);
        req.header.set("Content-Type", msg_type.content_type_header());
        if msg_type == WriteMessageType::V1 {
            req.header.set(VERSION_HEADER, VERSION1_HEADER_VALUE);
        } else {
            req.header.set(VERSION_HEADER, VERSION20_HEADER_VALUE);
        }
        if attempt > 0 {
            req.header.set("Retry-Attempt", &attempt.to_string());
        }

        let resp = match self.opts.client.do_request(req) {
            Ok(r) => r,
            Err(e) => {
                // Errors from Client.Do are likely network errors, so recoverable.
                return (
                    WriteResponseStats::default(),
                    Err(AttemptError::Retryable(RetryableError { msg: e.to_string(), retry_after: Duration::ZERO })),
                );
            }
        };

        let mut rs = WriteResponseStats::default();
        if msg_type == WriteMessageType::V2 {
            // A parse error is only logged in Go (nothing is logged by default).
            rs = parse_write_response_stats(&resp.header).0;
        }

        if resp.status / 100 == 2 {
            return (rs, Ok(()));
        }

        let err = format!("server returned HTTP status {}: {}", resp.status_text, String::from_utf8_lossy(&resp.body));
        if resp.status / 100 == 5 || (self.opts.retry_on_rate_limit && resp.status == 429) {
            let retry_after = retry_after_duration(resp.header.get_str("Retry-After"));
            return (rs, Err(AttemptError::Retryable(RetryableError { msg: err, retry_after })));
        }
        (rs, Err(AttemptError::Fatal(err)))
    }
}

/// Go `retryAfterDuration`: seconds (an HTTP date is not parsed here and counts as 0, as an invalid value does
/// in Go).
fn retry_after_duration(t: &str) -> Duration {
    match t.parse::<i64>() {
        Ok(d) if d > 0 => Duration::from_secs(d as u64),
        _ => Duration::ZERO,
    }
}

/// Go `path.Join(a, b)`.
fn path_join(a: &str, b: &str) -> String {
    let parts: Vec<&str> = [a, b].into_iter().filter(|p| !p.is_empty()).collect();
    if parts.is_empty() {
        return String::new();
    }
    net::config::clean_path(&parts.join("/"))
}

/// efficientgo `backoff.Backoff` (without a context: the write is never cancelled).
struct Backoff {
    cfg: BackoffConfig,
    num_retries: usize,
    next_delay_min: Duration,
    next_delay_max: Duration,
    rng: u64,
}

fn double_duration(value: Duration, max: Duration) -> Duration {
    let v = value * 2;
    if v <= max { v } else { max }
}

impl Backoff {
    fn new(cfg: BackoffConfig) -> Backoff {
        use std::hash::{BuildHasher, Hasher};
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u64(now_millis() as u64);
        Backoff {
            cfg,
            num_retries: 0,
            next_delay_min: cfg.min,
            next_delay_max: double_duration(cfg.min, cfg.max),
            rng: h.finish() | 1,
        }
    }

    fn ongoing(&self) -> bool {
        self.cfg.max_retries == 0 || self.num_retries < self.cfg.max_retries
    }

    fn rand_below(&mut self, n: u64) -> u64 {
        // xorshift64*.
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        self.rng.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }

    fn next_delay(&mut self) -> Duration {
        self.num_retries += 1;
        if self.next_delay_min >= self.next_delay_max {
            return self.next_delay_min;
        }
        let span = (self.next_delay_max - self.next_delay_min).as_nanos() as u64;
        let sleep_time = self.next_delay_min + Duration::from_nanos(self.rand_below(span));
        if self.next_delay_max < self.cfg.max {
            self.next_delay_min = double_duration(self.next_delay_min, self.cfg.max);
            self.next_delay_max = double_duration(self.next_delay_max, self.cfg.max);
        }
        sleep_time
    }
}
