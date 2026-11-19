//! Self-instrumentation: the metric family model (the parts of `io_prometheus_client.MetricFamily` the server
//! exposes), the `Gatherer` interface (Go `prometheus.Gatherer`), and their encodings.

/// Go `dto.MetricType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    Summary,
    Untyped,
    Histogram,
}

impl MetricType {
    /// The protobuf enum name (protojson).
    pub fn proto_name(self) -> &'static str {
        match self {
            MetricType::Counter => "COUNTER",
            MetricType::Gauge => "GAUGE",
            MetricType::Summary => "SUMMARY",
            MetricType::Untyped => "UNTYPED",
            MetricType::Histogram => "HISTOGRAM",
        }
    }

    /// The text exposition name.
    pub fn text_name(self) -> &'static str {
        match self {
            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge",
            MetricType::Summary => "summary",
            MetricType::Untyped => "untyped",
            MetricType::Histogram => "histogram",
        }
    }
}

/// The value of one metric.
#[derive(Clone, Debug, PartialEq)]
pub enum MetricValue {
    Counter(f64),
    Gauge(f64),
    Untyped(f64),
    /// Quantiles are `(quantile, value)`.
    Summary { count: u64, sum: f64, quantiles: Vec<(f64, f64)> },
    /// Buckets are cumulative `(upper bound, count)`, without +Inf.
    Histogram { count: u64, sum: f64, buckets: Vec<(f64, u64)> },
}

/// One metric of a family (Go `dto.Metric`).
#[derive(Clone, Debug, PartialEq)]
pub struct Metric {
    /// Label pairs sorted by name.
    pub labels: Vec<(String, String)>,
    pub value: MetricValue,
    pub timestamp_ms: Option<i64>,
}

/// One metric family (Go `dto.MetricFamily`).
#[derive(Clone, Debug, PartialEq)]
pub struct MetricFamily {
    pub name: String,
    pub help: String,
    pub typ: MetricType,
    pub metrics: Vec<Metric>,
}

/// Go `prometheus.Gatherer`: families sorted by name.
pub trait Gatherer: Send + Sync {
    fn gather(&self) -> Result<Vec<MetricFamily>, String>;
}

fn protojson_f64(f: f64, out: &mut String) {
    if f.is_nan() {
        out.push_str("\"NaN\"");
    } else if f.is_infinite() {
        out.push_str(if f > 0.0 { "\"Infinity\"" } else { "\"-Infinity\"" });
    } else {
        json::write_f64(f, out);
    }
}

/// The family as protojson (Go `protojson.Marshal(mf)`, compact).
pub fn protojson(mf: &MetricFamily) -> String {
    let mut out = String::with_capacity(128);
    out.push_str("{\"name\":");
    json::write_str(&mf.name, &mut out);
    if !mf.help.is_empty() {
        out.push_str(",\"help\":");
        json::write_str(&mf.help, &mut out);
    }
    out.push_str(",\"type\":\"");
    out.push_str(mf.typ.proto_name());
    out.push_str("\",\"metric\":[");
    for (i, m) in mf.metrics.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('{');
        if !m.labels.is_empty() {
            out.push_str("\"label\":[");
            for (j, (n, v)) in m.labels.iter().enumerate() {
                if j > 0 {
                    out.push(',');
                }
                out.push_str("{\"name\":");
                json::write_str(n, &mut out);
                out.push_str(",\"value\":");
                json::write_str(v, &mut out);
                out.push('}');
            }
            out.push_str("],");
        }
        match &m.value {
            MetricValue::Counter(v) => {
                out.push_str("\"counter\":{\"value\":");
                protojson_f64(*v, &mut out);
                out.push('}');
            }
            MetricValue::Gauge(v) => {
                out.push_str("\"gauge\":{\"value\":");
                protojson_f64(*v, &mut out);
                out.push('}');
            }
            MetricValue::Untyped(v) => {
                out.push_str("\"untyped\":{\"value\":");
                protojson_f64(*v, &mut out);
                out.push('}');
            }
            MetricValue::Summary { count, sum, quantiles } => {
                out.push_str("\"summary\":{\"sampleCount\":\"");
                out.push_str(&count.to_string());
                out.push_str("\",\"sampleSum\":");
                protojson_f64(*sum, &mut out);
                if !quantiles.is_empty() {
                    out.push_str(",\"quantile\":[");
                    for (j, (q, v)) in quantiles.iter().enumerate() {
                        if j > 0 {
                            out.push(',');
                        }
                        out.push_str("{\"quantile\":");
                        protojson_f64(*q, &mut out);
                        out.push_str(",\"value\":");
                        protojson_f64(*v, &mut out);
                        out.push('}');
                    }
                    out.push(']');
                }
                out.push('}');
            }
            MetricValue::Histogram { count, sum, buckets } => {
                out.push_str("\"histogram\":{\"sampleCount\":\"");
                out.push_str(&count.to_string());
                out.push_str("\",\"sampleSum\":");
                protojson_f64(*sum, &mut out);
                if !buckets.is_empty() {
                    out.push_str(",\"bucket\":[");
                    for (j, (ub, c)) in buckets.iter().enumerate() {
                        if j > 0 {
                            out.push(',');
                        }
                        out.push_str("{\"cumulativeCount\":\"");
                        out.push_str(&c.to_string());
                        out.push_str("\",\"upperBound\":");
                        protojson_f64(*ub, &mut out);
                        out.push('}');
                    }
                    out.push(']');
                }
                out.push('}');
            }
        }
        if let Some(ts) = m.timestamp_ms {
            out.push_str(",\"timestampMs\":\"");
            out.push_str(&ts.to_string());
            out.push('"');
        }
        out.push('}');
    }
    out.push_str("]}");
    out
}

// ---------------------------------------------------------------------------------------------------------------
// Registry, default gatherer and exposition.

use std::sync::{Arc, OnceLock};

use discovery::metrics::{self as reg, Collector, CounterVec, GaugeVec, Opts, Registry};

use crate::expfmt::{DtoBucket, DtoFamily, DtoHistogram, DtoMetric, DtoSummary, DtoType, DtoValue};

/// Converts the families of a [`Registry`] (Go client_golang collectors) into the gatherer model.
pub fn from_registry(fams: Vec<reg::Family>) -> Vec<MetricFamily> {
    fams.into_iter()
        .map(|f| MetricFamily {
            name: f.name,
            help: f.help,
            typ: match f.typ {
                reg::MetricType::Counter => MetricType::Counter,
                reg::MetricType::Gauge => MetricType::Gauge,
                reg::MetricType::Summary => MetricType::Summary,
                reg::MetricType::Histogram => MetricType::Histogram,
            },
            metrics: f
                .metrics
                .into_iter()
                .map(|m| Metric {
                    labels: m.labels,
                    value: match m.value {
                        reg::MetricValue::Counter(v) => MetricValue::Counter(v),
                        reg::MetricValue::Gauge(v) => MetricValue::Gauge(v),
                        reg::MetricValue::Summary { count, sum } => MetricValue::Summary { count, sum, quantiles: Vec::new() },
                        reg::MetricValue::Histogram { count, sum, buckets } => MetricValue::Histogram { count, sum, buckets },
                    },
                    timestamp_ms: None,
                })
                .collect(),
        })
        .collect()
}

/// The family in the exposition model (Go client_golang always sets `Help`).
pub fn to_dto(mf: &MetricFamily) -> DtoFamily {
    DtoFamily {
        name: mf.name.clone(),
        help: Some(mf.help.clone()),
        typ: match mf.typ {
            MetricType::Counter => DtoType::Counter,
            MetricType::Gauge => DtoType::Gauge,
            MetricType::Summary => DtoType::Summary,
            MetricType::Untyped => DtoType::Untyped,
            MetricType::Histogram => DtoType::Histogram,
        },
        unit: None,
        metrics: mf
            .metrics
            .iter()
            .map(|m| DtoMetric {
                labels: m.labels.clone(),
                value: match &m.value {
                    MetricValue::Counter(v) => DtoValue::Counter(*v),
                    MetricValue::Gauge(v) => DtoValue::Gauge(*v),
                    MetricValue::Untyped(v) => DtoValue::Untyped(*v),
                    MetricValue::Summary { count, sum, quantiles } => {
                        DtoValue::Summary(DtoSummary { sample_count: *count, sample_sum: *sum, quantiles: quantiles.clone() })
                    }
                    MetricValue::Histogram { count, sum, buckets } => DtoValue::Histogram(DtoHistogram {
                        sample_count: Some(*count),
                        sample_sum: Some(*sum),
                        buckets: buckets
                            .iter()
                            .map(|(ub, c)| DtoBucket { cumulative_count: Some(*c), cumulative_count_float: None, upper_bound: *ub })
                            .collect(),
                        ..Default::default()
                    }),
                },
                timestamp_ms: m.timestamp_ms,
            })
            .collect(),
    }
}

/// A gatherer over a [`Registry`] (Go `prometheus.Registry` as a `Gatherer`).
pub struct RegistryGatherer(pub Arc<Registry>);

impl Gatherer for RegistryGatherer {
    fn gather(&self) -> Result<Vec<MetricFamily>, String> {
        Ok(from_registry(self.0.gather()))
    }
}

/// Go `prometheus.DefaultRegisterer`: a process-wide registry with the process collector, the Go-compatible
/// runtime collector and promhttp's handler metrics.
pub fn default_registry() -> &'static Arc<Registry> {
    static R: OnceLock<Arc<Registry>> = OnceLock::new();
    R.get_or_init(|| {
        let r = Registry::new();
        let _ = r.register(Arc::new(ProcessCollector));
        let _ = r.register(Arc::new(GoCollector));
        let h = promhttp_metrics();
        let _ = r.register(h.requests.clone());
        let _ = r.register(h.in_flight.clone());
        r
    })
}

/// Go `prometheus.DefaultGatherer`.
pub struct DefaultGatherer;

impl Gatherer for DefaultGatherer {
    fn gather(&self) -> Result<Vec<MetricFamily>, String> {
        Ok(from_registry(default_registry().gather()))
    }
}

/// Go `prometheus.DefaultGatherer` as a shared gatherer (includes the `go_*` compatibility families).
pub fn default_gatherer() -> Arc<dyn Gatherer> {
    Arc::new(DefaultGatherer)
}

/// promhttp's `InstrumentMetricHandler` metrics.
pub struct PromhttpMetrics {
    pub requests: Arc<CounterVec>,
    pub in_flight: Arc<GaugeVec>,
}

/// The promhttp handler metrics of the default registry.
pub fn promhttp_metrics() -> &'static PromhttpMetrics {
    static M: OnceLock<PromhttpMetrics> = OnceLock::new();
    M.get_or_init(|| {
        let requests = CounterVec::new_counter_vec(
            Opts::new("promhttp_metric_handler_requests_total", "Total number of scrapes by HTTP status code."),
            &["code"],
        );
        // Go initializes the common status codes.
        for c in ["200", "500", "503"] {
            requests.with_label_values(&[c]);
        }
        let in_flight = GaugeVec::new_gauge(Opts::new(
            "promhttp_metric_handler_requests_in_flight",
            "Current number of scrapes being served.",
        ));
        PromhttpMetrics { requests, in_flight }
    })
}

/// Serves `/metrics` from `g` as promhttp's `HandlerFor` does with default options: format negotiation out of
/// `accepted`, gzip when accepted, `name[]` filtering, and HTTP 500 with the error when gathering fails.
pub fn serve_metrics(g: &dyn Gatherer, req: &http::Request, accepted: &[&str]) -> http::Response {
    let mfs = match g.gather() {
        Ok(m) => m,
        Err(e) => {
            return crate::route::http_error(&format!("An error has occurred while serving metrics:\n\n{}", e), 500);
        }
    };
    let format = crate::expfmt::negotiate_accept(req.header("accept").unwrap_or(""), accepted);
    let enc = crate::expfmt::Encoder::new(&format);
    let filter = req.query_all("name[]");
    let mut body = Vec::with_capacity(16 * 1024);
    for mf in &mfs {
        if !filter.is_empty() && !filter.contains(&mf.name) {
            continue;
        }
        if enc.encode(&mut body, &to_dto(mf)).is_err() {
            break;
        }
    }
    enc.close(&mut body);
    let mut resp = http::Response::new(200);
    resp.headers.push(("Content-Type".to_string(), format.0.clone()));
    resp.headers.push(("Vary".to_string(), "Accept".to_string()));
    resp.headers.push(("Vary".to_string(), "Accept-Encoding".to_string()));
    if negotiate_gzip(req) {
        use std::io::Write;
        let mut w = flate2::write::GzEncoder::new(Vec::with_capacity(body.len() / 4 + 64), flate2::Compression::default());
        let _ = w.write_all(&body);
        body = w.finish().unwrap_or_default();
        resp.headers.push(("Content-Encoding".to_string(), "gzip".to_string()));
    }
    resp.body = http::Body::Bytes(body);
    resp
}

/// promhttp's `NegotiateContentEncoding` over the offers identity and gzip: true when gzip wins.
fn negotiate_gzip(req: &http::Request) -> bool {
    let Some(h) = req.header("accept-encoding") else { return false };
    let mut best = ("", -1.0f64);
    for spec in h.split(',') {
        let mut parts = spec.split(';');
        let enc = parts.next().unwrap_or("").trim().to_ascii_lowercase();
        let mut q = 1.0f64;
        for p in parts {
            if let Some(v) = p.trim().strip_prefix("q=") {
                q = v.trim().parse().unwrap_or(0.0);
            }
        }
        for offer in ["identity", "gzip"] {
            if (enc == offer || enc == "*") && q > best.1 {
                best = (offer, q);
            }
        }
    }
    best.0 == "gzip" && best.1 > 0.0
}

// ---- Process and runtime collectors ----

/// Linux `USER_HZ` and page size (the x86_64 and aarch64 values; Go reads them with sysconf).
const CLK_TCK: f64 = 100.0;
const PAGE_SIZE: f64 = 4096.0;

fn gauge(name: &str, help: &str, v: f64) -> reg::Family {
    reg::Family {
        name: name.to_string(),
        help: help.to_string(),
        typ: reg::MetricType::Gauge,
        metrics: vec![reg::Metric { labels: Vec::new(), value: reg::MetricValue::Gauge(v) }],
    }
}

fn counter(name: &str, help: &str, v: f64) -> reg::Family {
    reg::Family {
        name: name.to_string(),
        help: help.to_string(),
        typ: reg::MetricType::Counter,
        metrics: vec![reg::Metric { labels: Vec::new(), value: reg::MetricValue::Counter(v) }],
    }
}

/// The fields of `/proc/self/stat` after the command name (proc(5) field 3 is index 0).
fn proc_stat() -> Option<Vec<String>> {
    let s = std::fs::read_to_string("/proc/self/stat").ok()?;
    let rest = &s[s.rfind(')')? + 1..];
    Some(rest.split_whitespace().map(|x| x.to_string()).collect())
}

fn boot_time() -> Option<f64> {
    let s = std::fs::read_to_string("/proc/stat").ok()?;
    s.lines().find_map(|l| l.strip_prefix("btime ")).and_then(|v| v.trim().parse().ok())
}

/// A soft limit from `/proc/self/limits` ("unlimited" is the max uint64, as procfs reports it).
fn proc_limit(name: &str) -> Option<f64> {
    let s = std::fs::read_to_string("/proc/self/limits").ok()?;
    let line = s.lines().find(|l| l.starts_with(name))?;
    let soft = line[name.len()..].split_whitespace().next()?;
    if soft == "unlimited" { Some(u64::MAX as f64) } else { soft.parse().ok() }
}

fn net_octets() -> Option<(f64, f64)> {
    let s = std::fs::read_to_string("/proc/self/net/netstat").ok()?;
    let mut lines = s.lines().filter(|l| l.starts_with("IpExt:"));
    let names: Vec<&str> = lines.next()?.split_whitespace().collect();
    let vals: Vec<&str> = lines.next()?.split_whitespace().collect();
    let get = |n: &str| names.iter().position(|x| *x == n).and_then(|i| vals.get(i)).and_then(|v| v.parse::<f64>().ok());
    Some((get("InOctets")?, get("OutOctets")?))
}

/// The number of OS threads of this process.
pub fn thread_count() -> usize {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find_map(|l| l.strip_prefix("Threads:")).and_then(|v| v.trim().parse().ok()))
        .unwrap_or(1)
}

/// Go client_golang's process collector (`process_*` metrics from `/proc`).
pub struct ProcessCollector;

impl Collector for ProcessCollector {
    fn describe(&self) -> Vec<String> {
        vec!["process_cpu_seconds_total".to_string()]
    }
    fn collect(&self, out: &mut Vec<reg::Family>) {
        if let Some(st) = proc_stat() {
            // proc(5) fields: utime 14, stime 15, starttime 22, vsize 23, rss 24.
            let f = |i: usize| st.get(i - 3).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
            out.push(counter("process_cpu_seconds_total", "Total user and system CPU time spent in seconds.", (f(14) + f(15)) / CLK_TCK));
            if let Ok(rd) = std::fs::read_dir("/proc/self/fd") {
                out.push(gauge("process_open_fds", "Number of open file descriptors.", rd.count() as f64));
            }
            if let Some(m) = proc_limit("Max open files") {
                out.push(gauge("process_max_fds", "Maximum number of open file descriptors.", m));
            }
            out.push(gauge("process_virtual_memory_bytes", "Virtual memory size in bytes.", f(23)));
            if let Some(m) = proc_limit("Max address space") {
                out.push(gauge("process_virtual_memory_max_bytes", "Maximum amount of virtual memory available in bytes.", m));
            }
            out.push(gauge("process_resident_memory_bytes", "Resident memory size in bytes.", f(24) * PAGE_SIZE));
            if let Some(bt) = boot_time() {
                out.push(gauge("process_start_time_seconds", "Start time of the process since unix epoch in seconds.", bt + f(22) / CLK_TCK));
            }
        }
        if let Some((rx, tx)) = net_octets() {
            out.push(counter("process_network_receive_bytes_total", "Number of bytes received by the process over the network.", rx));
            out.push(counter("process_network_transmit_bytes_total", "Number of bytes sent by the process over the network.", tx));
        }
    }
}

/// The Go runtime collector's metrics that mean something for this Rust process: `go_goroutines` and
/// `go_threads` are the OS thread count (each unit of concurrency here is a thread), `go_sched_gomaxprocs_threads`
/// is the available parallelism, and `go_info` names the runtime. Go's GC and memstats metrics have no
/// equivalent and are left out.
pub struct GoCollector;

impl Collector for GoCollector {
    fn describe(&self) -> Vec<String> {
        vec!["go_goroutines".to_string()]
    }
    fn collect(&self, out: &mut Vec<reg::Family>) {
        let threads = thread_count() as f64;
        out.push(gauge("go_goroutines", "Number of goroutines that currently exist.", threads));
        out.push(reg::Family {
            name: "go_info".to_string(),
            help: "Information about the Go environment.".to_string(),
            typ: reg::MetricType::Gauge,
            metrics: vec![reg::Metric { labels: vec![("version".to_string(), "rust".to_string())], value: reg::MetricValue::Gauge(1.0) }],
        });
        let procs = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1) as f64;
        out.push(gauge(
            "go_sched_gomaxprocs_threads",
            "The current runtime.GOMAXPROCS setting, or the number of operating system threads that can execute user-level Go code simultaneously. Sourced from /sched/gomaxprocs:threads.",
            procs,
        ));
        out.push(gauge("go_threads", "Number of OS threads created.", threads));
    }
}
