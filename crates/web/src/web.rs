//! Port of `web/web.go`: the web [`Handler`] of the Prometheus server. It owns the top-level routing (route prefix,
//! redirects, the UI, lifecycle and health endpoints, `/version`, `/metrics`, `/federate`, `/consoles`, `/debug`,
//! `/user`), mounts the API v1 under `<prefix>/api/v1`, instruments every route with the
//! `prometheus_http_*` metrics, tracks the ready state, and serves on one or more listen addresses.
//!
//! Build it with [`Handler::new`]`(`[`Options`]`)`; the API is built from the options as Go's `New` builds it
//! (the retriever traits, storages and engine come from the caller). Then call [`Handler::listeners`] and
//! [`Handler::run`] with a [`CancelToken`].
//!
//! Differences from Go:
//! - `/debug/pprof/*` is Go-only: `cmdline` is real, `goroutine` and `threadcreate` answer with the OS thread
//!   count, the other known profiles answer with a short text, `profile`, `trace`, `symbol` and `fgprof` answer
//!   501. Unknown profiles give Go's 404 "Unknown profile".
//! - `/consoles/*` needs the template engine: a console file that exists is expanded by
//!   [`Options::console_expander`]; without one the answer is 500.
//! - No TLS, HTTP/2 or basic auth (`web_config_file` must be empty), no connection tracking metrics, no tracing.
//! - `--web.max-connections` limits connections per listener (Go shares one limit across listeners).

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard, Weak};
use std::time::{Duration, Instant};

use discovery::log::Logger;
use discovery::metrics::{CounterVec, GaugeVec, HistogramVec, Opts, Registry};
use http::{Body, Request, Response, StreamWriter};
use promql::parser::Parser;
use storage::{Context, Error, ExemplarQueryable, Queryable, SampleAndChunkQueryable};

use crate::api::{
    self, AlertmanagerRetriever, Api, ApiOptions, GlobalUrlOptions, HttpHandler, NotificationsSub, PrometheusVersion,
    QueryEngine, RulesRetriever, RuntimeInfo, ScrapePoolsRetriever, TargetRetriever, TsdbAdminStats, WalReplayStatus,
};
use crate::gotime::Time;
use crate::httputil::CorsOrigin;
use crate::metrics::{Gatherer, MetricFamily, MetricValue};
use crate::notifications::Notification;
use crate::route::{HandlerFn, InstrumentFn, Router, http_error, not_found};
use crate::ui::{Asset, Assets, IndexPlaceholders, replace_placeholders};

/// Paths handled by the React router of the old UI (agent and server mode).
pub const OLD_UI_REACT_ROUTER_PATHS: [&str; 5] = ["/config", "/flags", "/service-discovery", "/status", "/targets"];
/// Paths handled by the React router of the new UI (agent and server mode).
pub const NEW_UI_REACT_ROUTER_PATHS: [&str; 6] =
    ["/config", "/flags", "/service-discovery", "/alertmanager-discovery", "/status", "/targets"];
/// Paths handled by the React router in agent mode.
pub const REACT_ROUTER_AGENT_PATHS: [&str; 1] = ["/agent"];
/// Paths handled by the React router of the old UI when not in agent mode.
pub const OLD_UI_REACT_ROUTER_SERVER_PATHS: [&str; 4] = ["/alerts", "/graph", "/rules", "/tsdb-status"];
/// Paths handled by the React router of the new UI when not in agent mode (the old `/graph` redirects to
/// `/query`).
pub const NEW_UI_REACT_ROUTER_SERVER_PATHS: [&str; 4] = ["/alerts", "/query", "/rules", "/tsdb-status"];

/// Go `ReadyStatus`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum ReadyStatus {
    NotReady = 0,
    Ready = 1,
    Stopping = 2,
}

/// The external URL (Go `*url.URL` as the web handler uses it).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExternalUrl {
    pub scheme: String,
    pub host: String,
    /// The path without a trailing slash ("" for the root), as `cmd/prometheus` computes it.
    pub path: String,
}

impl ExternalUrl {
    /// Parses "scheme://host/path".
    pub fn parse(s: &str) -> ExternalUrl {
        match yaml::url::parse(s) {
            Ok(u) => ExternalUrl { scheme: u.scheme, host: u.host, path: u.path },
            Err(_) => ExternalUrl::default(),
        }
    }
}

impl std::fmt::Display for ExternalUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.scheme.is_empty() {
            write!(f, "{}:", self.scheme)?;
        }
        if !self.scheme.is_empty() || !self.host.is_empty() {
            write!(f, "//{}", self.host)?;
        }
        if !self.path.is_empty() && !self.path.starts_with('/') && !self.host.is_empty() {
            f.write_str("/")?;
        }
        f.write_str(&self.path)
    }
}

/// What a console template expansion gets (Go's `consoles` data). The expander returns the HTML.
pub struct ConsoleRequest<'a> {
    /// The template text (with Go's convenience definitions not yet prepended).
    pub text: &'a str,
    /// The template name (`__console_<name>`).
    pub name: String,
    pub raw_params: Vec<(String, Vec<String>)>,
    pub params: BTreeMap<String, String>,
    pub path: String,
    pub external_labels: BTreeMap<String, String>,
    /// The `*.lib` files of the console libraries path.
    pub library_files: Vec<PathBuf>,
    pub now_ms: i64,
}

/// Expands a console template (the server wires the template engine here).
pub type ConsoleExpander = Arc<dyn Fn(&ConsoleRequest<'_>) -> Result<String, String> + Send + Sync>;

/// Options for the web handler (Go `Options`).
#[derive(Clone)]
pub struct Options {
    pub tsdb_retention_duration: yaml::Duration,
    pub tsdb_dir: String,
    pub tsdb_max_bytes: yaml::Base2Bytes,
    pub tsdb_max_percentage: f64,
    /// Go `LocalStorage` for federation (`Querier`).
    pub local_storage: Option<Arc<dyn Queryable>>,
    /// Go `LocalStorage` as `api_v1.TSDBAdminStats`.
    pub tsdb_admin: Option<Arc<dyn TsdbAdminStats>>,
    /// Go `Storage` (what the API queries).
    pub storage: Option<Arc<dyn SampleAndChunkQueryable>>,
    pub exemplar_storage: Option<Arc<dyn ExemplarQueryable>>,
    /// The cost sampling view of `storage`, if it has one.
    pub cost_sampling: Option<Arc<dyn storage::CostSamplingQueryable + Send + Sync>>,
    pub query_engine: Option<Arc<dyn QueryEngine>>,
    pub lookback_delta: Duration,
    pub query_timeout: Duration,
    /// Go `ScrapeManager` (as the API's retrievers).
    pub scrape_pools_retriever: Option<Arc<dyn ScrapePoolsRetriever>>,
    pub target_retriever: Option<Arc<dyn TargetRetriever>>,
    /// Go `RuleManager`.
    pub rules_retriever: Option<Arc<dyn RulesRetriever>>,
    /// Go `Notifier`.
    pub alertmanager_retriever: Option<Arc<dyn AlertmanagerRetriever>>,
    pub version: Option<PrometheusVersion>,
    pub notifications_getter: Option<Arc<dyn Fn() -> Vec<Notification> + Send + Sync>>,
    pub notifications_sub: Option<NotificationsSub>,
    pub flags: BTreeMap<String, String>,

    pub listen_addresses: Vec<String>,
    pub cors_origin: Option<CorsOrigin>,
    /// Go `ReadTimeout` (0: no limit; the server then uses 5 minutes between requests).
    pub read_timeout: Duration,
    /// 0: no limit (the server then allows 1024 per listener).
    pub max_connections: usize,
    pub external_url: ExternalUrl,
    pub route_prefix: String,
    pub user_assets_path: String,
    pub console_templates_path: String,
    pub console_libraries_path: String,
    pub use_old_ui: bool,
    pub enable_lifecycle: bool,
    pub enable_admin_api: bool,
    pub enable_search: bool,
    pub enable_query_cost: bool,
    pub max_search_limit: i64,
    pub page_title: String,
    pub enable_remote_write_receiver: bool,
    pub enable_otlp_write_receiver: bool,
    pub convert_otlp_delta: bool,
    pub native_otlp_delta_ingestion: bool,
    pub is_agent: bool,
    pub app_name: String,
    /// The remote read, remote write and OTLP handlers (built by the caller from the remote storage).
    pub remote_read_handler: Option<HttpHandler>,
    pub remote_write_handler: Option<HttpHandler>,
    pub otlp_write_handler: Option<HttpHandler>,

    /// Go `Gatherer` (None: the default gatherer).
    pub gatherer: Option<Arc<dyn Gatherer>>,
    /// Go `Registerer`: where the handler's own metrics are registered (None: not registered).
    pub registerer: Option<Arc<Registry>>,
    pub feature_registry: Option<Arc<dyn promql::features::Collector + Send + Sync>>,
    /// The PromQL parser (None: a parser with default options).
    pub parser: Option<Arc<Parser>>,

    /// The UI assets (default: the embedded build).
    pub ui_assets: Assets,
    pub console_expander: Option<ConsoleExpander>,
    pub logger: Logger,
    /// Overrides the clock in milliseconds (tests; Go sets `h.now`).
    pub now: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            tsdb_retention_duration: yaml::Duration(0),
            tsdb_dir: String::new(),
            tsdb_max_bytes: yaml::Base2Bytes(0),
            tsdb_max_percentage: 0.0,
            local_storage: None,
            tsdb_admin: None,
            storage: None,
            exemplar_storage: None,
            cost_sampling: None,
            query_engine: None,
            lookback_delta: Duration::ZERO,
            query_timeout: Duration::ZERO,
            scrape_pools_retriever: None,
            target_retriever: None,
            rules_retriever: None,
            alertmanager_retriever: None,
            version: None,
            notifications_getter: None,
            notifications_sub: None,
            flags: BTreeMap::new(),
            listen_addresses: Vec::new(),
            cors_origin: None,
            read_timeout: Duration::ZERO,
            max_connections: 0,
            external_url: ExternalUrl::default(),
            route_prefix: "/".to_string(),
            user_assets_path: String::new(),
            console_templates_path: String::new(),
            console_libraries_path: String::new(),
            use_old_ui: false,
            enable_lifecycle: false,
            enable_admin_api: false,
            enable_search: false,
            enable_query_cost: false,
            max_search_limit: 0,
            page_title: String::new(),
            enable_remote_write_receiver: false,
            enable_otlp_write_receiver: false,
            convert_otlp_delta: false,
            native_otlp_delta_ingestion: false,
            is_agent: false,
            app_name: String::new(),
            remote_read_handler: None,
            remote_write_handler: None,
            otlp_write_handler: None,
            gatherer: None,
            registerer: None,
            feature_registry: None,
            parser: None,
            ui_assets: Assets::embedded(),
            console_expander: None,
            logger: Logger::nop(),
            now: None,
        }
    }
}

/// The handler's metrics (Go `metrics` plus the federation counters).
pub struct WebMetrics {
    pub request_counter: Arc<CounterVec>,
    pub request_duration: Arc<HistogramVec>,
    pub response_size: Arc<HistogramVec>,
    pub ready_status: Arc<GaugeVec>,
    pub federation_errors: Arc<discovery::metrics::Value>,
    pub federation_warnings: Arc<discovery::metrics::Value>,
    federation_errors_vec: Arc<CounterVec>,
    federation_warnings_vec: Arc<CounterVec>,
}

impl WebMetrics {
    /// Go `newMetrics` (with `registerFederationMetrics`).
    fn new(r: Option<&Arc<Registry>>) -> WebMetrics {
        let request_counter =
            CounterVec::new_counter_vec(Opts::new("prometheus_http_requests_total", "Counter of HTTP requests."), &["handler", "code"]);
        let request_duration = HistogramVec::new_histogram_vec(
            Opts::new("prometheus_http_request_duration_seconds", "Histogram of latencies for HTTP requests."),
            &["handler"],
            &[0.1, 0.2, 0.4, 1.0, 3.0, 8.0, 20.0, 60.0, 120.0],
        );
        // Go: prometheus.ExponentialBuckets(100, 10, 8).
        let size_buckets: Vec<f64> = (0..8).map(|i| 100.0 * 10f64.powi(i)).collect();
        let response_size = HistogramVec::new_histogram_vec(
            Opts::new("prometheus_http_response_size_bytes", "Histogram of response size for HTTP requests."),
            &["handler"],
            &size_buckets,
        );
        let ready_status = GaugeVec::new_gauge(Opts::new(
            "prometheus_ready",
            "Whether Prometheus startup was fully completed and the server is ready for normal operation.",
        ));
        let federation_errors_vec = CounterVec::new_counter(Opts::new(
            "prometheus_web_federation_errors_total",
            "Total number of errors that occurred while sending federation responses.",
        ));
        let federation_warnings_vec = CounterVec::new_counter(Opts::new(
            "prometheus_web_federation_warnings_total",
            "Total number of warnings that occurred while sending federation responses.",
        ));
        if let Some(r) = r {
            let _ = r.register(request_counter.clone());
            let _ = r.register(request_duration.clone());
            let _ = r.register(response_size.clone());
            let _ = r.register(ready_status.clone());
            let _ = r.register(federation_warnings_vec.clone());
            let _ = r.register(federation_errors_vec.clone());
        }
        WebMetrics {
            federation_errors: federation_errors_vec.get(),
            federation_warnings: federation_warnings_vec.get(),
            request_counter,
            request_duration,
            response_size,
            ready_status,
            federation_errors_vec,
            federation_warnings_vec,
        }
    }

    /// Go `instrumentHandler`: counts requests by status code, and observes the duration and the response size.
    pub fn instrument_handler(self: &Arc<Self>, name: &str, h: HandlerFn) -> HandlerFn {
        self.request_counter.with_label_values(&[name, "200"]);
        let m = self.clone();
        let name = name.to_string();
        Arc::new(move |req: Request| {
            let start = Instant::now();
            let head = req.method == "HEAD";
            let mut resp = h(req);
            let code = resp.status.to_string();
            m.request_counter.with_label_values(&[&name, &code]).inc();
            match std::mem::replace(&mut resp.body, Body::Bytes(Vec::new())) {
                Body::Stream(f) => {
                    let m2 = m.clone();
                    let name2 = name.clone();
                    resp.body = Body::Stream(Box::new(move |w: &mut dyn StreamWriter| {
                        let mut cw = CountingWriter { inner: w, n: 0 };
                        let r = f(&mut cw);
                        m2.request_duration.with_label_values(&[&name2]).observe(start.elapsed().as_secs_f64());
                        m2.response_size.with_label_values(&[&name2]).observe(cw.n as f64);
                        r
                    }));
                }
                b => {
                    let n = if head {
                        0
                    } else {
                        match &b {
                            Body::Bytes(v) => v.len() as u64,
                            Body::File(_, n) => *n,
                            Body::Stream(_) => 0,
                        }
                    };
                    resp.body = b;
                    m.request_duration.with_label_values(&[&name]).observe(start.elapsed().as_secs_f64());
                    m.response_size.with_label_values(&[&name]).observe(n as f64);
                }
            }
            resp
        })
    }

    /// The `(prometheus_web_federation_errors_total, prometheus_web_federation_warnings_total)` collectors.
    pub fn federation_collectors(&self) -> (Arc<CounterVec>, Arc<CounterVec>) {
        (self.federation_errors_vec.clone(), self.federation_warnings_vec.clone())
    }
}

struct CountingWriter<'a> {
    inner: &'a mut dyn StreamWriter,
    n: u64,
}

impl StreamWriter for CountingWriter<'_> {
    fn write_all(&mut self, b: &[u8]) -> std::io::Result<()> {
        self.n += b.len() as u64;
        self.inner.write_all(b)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
    fn peer_closed(&mut self) -> bool {
        self.inner.peer_closed()
    }
}

/// The mutable part of the handler (Go fields guarded by `h.mtx`).
pub struct State {
    pub config: Arc<config::Config>,
    pub tsdb_retention_duration: yaml::Duration,
    pub tsdb_max_bytes: yaml::Base2Bytes,
    pub tsdb_max_percentage: f64,
}

/// Go's quit channel: set once, waited on by any number of threads.
#[derive(Default)]
pub struct QuitSignal {
    set: Mutex<bool>,
    cv: Condvar,
}

impl QuitSignal {
    pub fn is_set(&self) -> bool {
        *self.set.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Blocks until quit is requested.
    pub fn wait(&self) {
        let mut g = self.set.lock().unwrap_or_else(|e| e.into_inner());
        while !*g {
            g = self.cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
    }
    /// Blocks until quit is requested or `d` passed; true if requested.
    pub fn wait_timeout(&self, d: Duration) -> bool {
        let g = self.set.lock().unwrap_or_else(|e| e.into_inner());
        let (g, _) = self.cv.wait_timeout_while(g, d, |s| !*s).unwrap_or_else(|e| e.into_inner());
        *g
    }
    fn fire(&self) -> bool {
        let mut g = self.set.lock().unwrap_or_else(|e| e.into_inner());
        let was = *g;
        *g = true;
        self.cv.notify_all();
        !was
    }
}

/// A reload request: the receiver sends the reload result back on this sender (Go `chan error`).
pub type ReloadRequest = Sender<Result<(), String>>;

/// Go's `context.Context` for [`Handler::run`]: cancel it to stop serving.
#[derive(Clone, Default)]
pub struct CancelToken(Arc<(Mutex<bool>, Condvar)>);

impl CancelToken {
    pub fn new() -> CancelToken {
        CancelToken::default()
    }
    pub fn cancel(&self) {
        let mut g = self.0.0.lock().unwrap_or_else(|e| e.into_inner());
        *g = true;
        self.0.1.notify_all();
    }
    pub fn is_cancelled(&self) -> bool {
        *self.0.0.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn wait(&self) {
        let mut g = self.0.0.lock().unwrap_or_else(|e| e.into_inner());
        while !*g {
            g = self.0.1.wait(g).unwrap_or_else(|e| e.into_inner());
        }
    }
}

/// The web handler (Go `Handler`).
pub struct Handler {
    pub(crate) options: Options,
    pub(crate) logger: Logger,
    pub(crate) metrics: Arc<WebMetrics>,
    pub(crate) parser: Arc<Parser>,
    pub(crate) now: Arc<dyn Fn() -> i64 + Send + Sync>,
    state: RwLock<State>,
    ready: AtomicU32,
    quit: QuitSignal,
    reload_tx: Mutex<Sender<ReloadRequest>>,
    reload_rx: Mutex<Option<Receiver<ReloadRequest>>>,
    api: Arc<Api>,
    router: Router,
    api_router: Router,
    api_path: String,
    birth: Time,
    cwd: String,
}

fn text(code: u16, body: impl Into<Vec<u8>>) -> Response {
    // Go handlers write without a Content-Type; net/http sniffs plain text.
    Response::text(code, "text/plain; charset=utf-8", body)
}

/// Go `path.Clean`.
pub fn go_path_clean(p: &str) -> String {
    if p.is_empty() {
        return ".".to_string();
    }
    let rooted = p.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                if out.last().is_some_and(|l| *l != "..") {
                    out.pop();
                } else if !rooted {
                    out.push("..");
                }
            }
            s => out.push(s),
        }
    }
    let s = out.join("/");
    if rooted {
        format!("/{}", s)
    } else if s.is_empty() {
        ".".to_string()
    } else {
        s
    }
}

/// Go `path.Join`.
pub fn go_path_join(elems: &[&str]) -> String {
    let parts: Vec<&str> = elems.iter().copied().filter(|e| !e.is_empty()).collect();
    if parts.is_empty() {
        return String::new();
    }
    go_path_clean(&parts.join("/"))
}

/// Go `http.Redirect` for a path URL: relative URLs are resolved against the request path, and the path is cleaned
/// (keeping a trailing slash).
pub fn go_redirect(req: &Request, url: &str, code: u16) -> Response {
    let mut url = url.to_string();
    if !url.contains("://") {
        if url.is_empty() || !url.starts_with('/') {
            let old = if req.path.is_empty() { "/" } else { req.path.as_str() };
            let dir = match old.rfind('/') {
                Some(i) => &old[..=i],
                None => "/",
            };
            url = format!("{}{}", dir, url);
        }
        let (p, q) = match url.find('?') {
            Some(i) => (url[..i].to_string(), url[i..].to_string()),
            None => (url.clone(), String::new()),
        };
        let trailing = p.ends_with('/');
        let mut p = go_path_clean(&p);
        if trailing && !p.ends_with('/') {
            p.push('/');
        }
        url = p + &q;
    }
    crate::route::redirect(req, &url, code)
}

/// Go `url.ParseQuery`: the pairs in order, or the first error (Go's text).
pub fn go_parse_query(q: &str) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    let mut first_err: Option<String> = None;
    for part in q.split('&') {
        if part.is_empty() {
            continue;
        }
        if part.contains(';') {
            first_err.get_or_insert_with(|| "invalid semicolon separator in query".to_string());
            continue;
        }
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        let k = match query_unescape(k) {
            Ok(k) => k,
            Err(e) => {
                first_err.get_or_insert(e);
                continue;
            }
        };
        let v = match query_unescape(v) {
            Ok(v) => v,
            Err(e) => {
                first_err.get_or_insert(e);
                continue;
            }
        };
        out.push((k, v));
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(out),
    }
}

fn query_unescape(s: &str) -> Result<String, String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                let hex = |c: u8| (c as char).to_digit(16);
                if i + 2 >= b.len() || hex(b[i + 1]).is_none() || hex(b[i + 2]).is_none() {
                    let end = (i + 3).min(b.len());
                    return Err(format!("invalid URL escape {}", crate::gotime::go_quote(&String::from_utf8_lossy(&b[i..end]))));
                }
                out.push((hex(b[i + 1]).unwrap_or(0) * 16 + hex(b[i + 2]).unwrap_or(0)) as u8);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

fn read_hostname() -> Result<String, String> {
    match std::fs::read_to_string("/proc/sys/kernel/hostname") {
        Ok(s) => Ok(s.trim().to_string()),
        Err(e) => Err(e.to_string()),
    }
}

/// `toFloat64` of the first metric of a family.
fn to_float64(f: &MetricFamily) -> f64 {
    match f.metrics.first().map(|m| &m.value) {
        Some(MetricValue::Gauge(v)) | Some(MetricValue::Counter(v)) | Some(MetricValue::Untyped(v)) => *v,
        _ => f64::NAN,
    }
}

type Weak2 = Weak<Handler>;

/// Wraps a handler that needs the `Handler` (routes are built before the `Arc` exists).
fn with_h(w: &Weak2, f: impl Fn(&Handler, Request) -> Response + Send + Sync + 'static) -> HandlerFn {
    let w = w.clone();
    Arc::new(move |req: Request| match w.upgrade() {
        Some(h) => f(&h, req),
        None => http_error("handler gone", 503),
    })
}

/// Go `testReady`: the handler when ready, else 503 (or 500 for an unknown state).
fn ready_wrap(w: &Weak2, f: HandlerFn) -> HandlerFn {
    let w = w.clone();
    Arc::new(move |req: Request| match w.upgrade() {
        Some(h) => match h.not_ready_response() {
            Some(r) => r,
            None => f(req),
        },
        None => http_error("handler gone", 503),
    })
}

impl Handler {
    /// Go `New`.
    pub fn new(o: Options) -> Arc<Handler> {
        let logger = o.logger.clone();
        let parser = o.parser.clone().unwrap_or_else(|| Arc::new(Parser::new(Default::default())));
        let metrics = Arc::new(WebMetrics::new(o.registerer.as_ref()));
        let cwd = match std::env::current_dir() {
            Ok(p) => p.to_string_lossy().into_owned(),
            Err(_) => "<error retrieving current working directory>".to_string(),
        };
        let now: Arc<dyn Fn() -> i64 + Send + Sync> = match &o.now {
            Some(f) => f.clone(),
            None => Arc::new(|| Time::now().unix_milli()),
        };
        let (reload_tx, reload_rx) = channel();
        let state = State {
            config: Arc::new(config::Config::default()),
            tsdb_retention_duration: o.tsdb_retention_duration,
            tsdb_max_bytes: o.tsdb_max_bytes,
            tsdb_max_percentage: o.tsdb_max_percentage,
        };

        if let Some(r) = &o.feature_registry {
            use promql::features::API;
            r.set(API, "lifecycle", o.enable_lifecycle);
            r.set(API, "admin", o.enable_admin_api);
            r.set(API, "remote_write_receiver", o.enable_remote_write_receiver);
            r.set(API, "otlp_write_receiver", o.enable_otlp_write_receiver);
            r.set(API, "query_cost", o.enable_query_cost);
            r.set(API, "search", o.enable_search);
            for alg in crate::api::search::fuzz_algorithms() {
                r.enable(API, &format!("search_fuzz_alg_{}", alg));
            }
            r.set("otlp_receiver", "delta_conversion", o.convert_otlp_delta);
            r.set("otlp_receiver", "native_delta_ingestion", o.native_otlp_delta_ingestion);
            for f in ["label_values_match", "query_warnings", "query_stats", "time_range_series", "time_range_labels", "exclude_alerts", "openapi_3.1", "openapi_3.2"] {
                r.enable(API, f);
            }
            r.set("ui", "ui_v3", !o.use_old_ui);
            r.set("ui", "ui_v2", o.use_old_ui);
        }

        let h = Arc::new_cyclic(|w: &Weak2| {
            let api = Arc::new(Api::new(Self::api_options(&o, w, &parser)));
            let api_path = if o.route_prefix != "/" { format!("{}/api", o.route_prefix) } else { "/api".to_string() };
            let mut api_router = Router::new();
            let m = metrics.clone();
            let instr: InstrumentFn = Arc::new(move |name: &str, h: HandlerFn| m.instrument_handler(name, h));
            api_router.set_instrumentation(&api_path[..api_path.len() - 4], instr.clone());
            api.register(&mut api_router, &format!("{}/v1", api_path));
            let router = Self::build_router(&o, w, instr);
            Handler {
                logger,
                metrics: metrics.clone(),
                parser: parser.clone(),
                now,
                state: RwLock::new(state),
                ready: AtomicU32::new(ReadyStatus::NotReady as u32),
                quit: QuitSignal::default(),
                reload_tx: Mutex::new(reload_tx),
                reload_rx: Mutex::new(Some(reload_rx)),
                api,
                router,
                api_router,
                api_path,
                birth: Time::now(),
                cwd,
                options: o,
            }
        });
        h.set_ready(ReadyStatus::NotReady);
        h
    }

    fn api_options(o: &Options, w: &Weak2, parser: &Arc<Parser>) -> ApiOptions {
        let wr = w.clone();
        let ready: api::ReadyFn = Arc::new(move || wr.upgrade().and_then(|h| h.not_ready_response()));
        let wc = w.clone();
        let config: Arc<dyn Fn() -> Arc<config::Config> + Send + Sync> =
            Arc::new(move || wc.upgrade().map(|h| h.state().config.clone()).unwrap_or_default());
        let wi = w.clone();
        let runtime_info: Arc<dyn Fn() -> Result<RuntimeInfo, Error> + Send + Sync> = Arc::new(move || match wi.upgrade() {
            Some(h) => h.runtime_info(),
            None => Err(Error::msg("handler gone")),
        });
        ApiOptions {
            query_engine: o.query_engine.clone(),
            queryable: o.storage.clone(),
            exemplar_queryable: o.exemplar_storage.clone(),
            scrape_pools_retriever: o.scrape_pools_retriever.clone(),
            target_retriever: o.target_retriever.clone(),
            alertmanager_retriever: o.alertmanager_retriever.clone(),
            rules_retriever: o.rules_retriever.clone(),
            config: Some(config),
            flags: o.flags.clone(),
            global_url_options: GlobalUrlOptions {
                listen_address: o.listen_addresses.first().cloned().unwrap_or_default(),
                host: o.external_url.host.clone(),
                scheme: o.external_url.scheme.clone(),
            },
            ready: Some(ready),
            db: o.tsdb_admin.clone(),
            db_dir: o.tsdb_dir.clone(),
            enable_admin: o.enable_admin_api,
            enable_search: o.enable_search,
            max_search_limit: o.max_search_limit,
            is_agent: o.is_agent,
            cors_origin: o.cors_origin.clone(),
            runtime_info: Some(runtime_info),
            build_info: o.version.clone().unwrap_or_default(),
            notifications_getter: o.notifications_getter.clone(),
            notifications_sub: o.notifications_sub.clone(),
            gatherer: Some(o.gatherer.clone().unwrap_or_else(crate::metrics::default_gatherer)),
            stats_renderer: None,
            remote_read_handler: o.remote_read_handler.clone(),
            remote_write_handler: o.remote_write_handler.clone(),
            otlp_write_handler: o.otlp_write_handler.clone(),
            lookback_delta: o.lookback_delta.as_nanos() as i64,
            override_error_code: None,
            feature_registry: o.feature_registry.clone(),
            openapi_options: crate::api::openapi::OpenApiOptions {
                include_paths: Vec::new(),
                external_url: o.external_url.to_string(),
                version: o.version.as_ref().map(|v| v.version.clone()).unwrap_or_default(),
                max_search_limit: o.max_search_limit,
            },
            parser: Some(parser.clone()),
            enable_query_cost: o.enable_query_cost,
            query_timeout_flag: o.query_timeout.as_nanos() as i64,
            cost_sampling: o.cost_sampling.clone(),
            now: None,
        }
    }

    fn build_router(o: &Options, w: &Weak2, instr: InstrumentFn) -> Router {
        let mut r = Router::new();
        r.set_instrumentation("", instr.clone());
        let prefix = if o.route_prefix != "/" { o.route_prefix.clone() } else { String::new() };
        if !prefix.is_empty() {
            // If the prefix is missing for the root path, prepend it.
            let target = o.route_prefix.clone();
            r.get("/", Arc::new(move |req: Request| go_redirect(&req, &target, 302)));
            r.set_instrumentation(&prefix, instr);
        }
        let p = |s: &str| format!("{}{}", prefix, s);

        let mut home = "/query";
        if o.use_old_ui {
            home = "/graph";
        }
        if o.is_agent {
            home = "/agent";
        }
        let ext_path = o.external_url.path.clone();
        {
            let target = go_path_join(&[&ext_path, home]);
            r.get(&p("/"), Arc::new(move |req: Request| go_redirect(&req, &target, 302)));
        }
        if !o.use_old_ui {
            let ep = ext_path.clone();
            r.get(
                &p("/graph"),
                Arc::new(move |req: Request| {
                    let target = go_path_join(&[&ep, &format!("/query?{}", req.query)]);
                    go_redirect(&req, &target, 302)
                }),
            );
        }

        let react_assets_root = if o.use_old_ui { "/static/react-app" } else { "/static/mantine-ui" };

        r.get(&p("/version"), with_h(w, |h, _| h.version()));
        r.get(&p("/metrics"), with_h(w, |h, req| h.serve_metrics(&req)));
        r.get(
            &p("/federate"),
            ready_wrap(
                w,
                with_h(w, |h, req| {
                    let mut resp = h.federation(&req);
                    crate::httputil::compress(&mut resp, &req);
                    resp
                }),
            ),
        );
        r.get(&p("/consoles/*filepath"), ready_wrap(w, with_h(w, |h, req| h.consoles(&req))));

        let serve_react: HandlerFn = {
            let root = react_assets_root.to_string();
            with_h(w, move |h, _| h.serve_react_app(&root))
        };
        let (paths, server_paths): (&[&str], &[&str]) = if o.use_old_ui {
            (&OLD_UI_REACT_ROUTER_PATHS, &OLD_UI_REACT_ROUTER_SERVER_PATHS)
        } else {
            (&NEW_UI_REACT_ROUTER_PATHS, &NEW_UI_REACT_ROUTER_SERVER_PATHS)
        };
        for path in paths {
            r.get(&p(path), serve_react.clone());
        }
        let extra: &[&str] = if o.is_agent { &REACT_ROUTER_AGENT_PATHS } else { server_paths };
        for path in extra {
            r.get(&p(path), serve_react.clone());
        }

        // The favicon and manifest are bundled with the React app, but served on the root.
        for f in ["/favicon.svg", "/favicon.ico", "/manifest.json"] {
            let asset = format!("{}{}", react_assets_root, f);
            r.get(&p(f), with_h(w, move |h, req| h.options.ui_assets.serve(&req, &asset)));
        }
        let static_dir = if o.use_old_ui { "/static" } else { "/assets" };
        {
            let base = format!("{}{}", react_assets_root, static_dir);
            r.get(
                &p(&format!("{}/*filepath", static_dir)),
                with_h(w, move |h, req| {
                    let path = go_path_join(&[&base, &format!("/{}", req.param("filepath"))]);
                    h.options.ui_assets.serve(&req, &path)
                }),
            );
        }
        if !o.user_assets_path.is_empty() {
            let dir = PathBuf::from(&o.user_assets_path);
            r.get(&p("/user/*filepath"), Arc::new(move |req: Request| file_serve(&req, &dir)));
        }

        if o.enable_lifecycle {
            for m in ["POST", "PUT"] {
                r.handle(m, &p("/-/quit"), with_h(w, |h, _| h.quit()));
                r.handle(m, &p("/-/reload"), with_h(w, |h, _| h.reload()));
            }
        } else {
            let forbidden: HandlerFn = Arc::new(|_| text(403, "Lifecycle API is not enabled."));
            for m in ["POST", "PUT"] {
                r.handle(m, &p("/-/quit"), forbidden.clone());
                r.handle(m, &p("/-/reload"), forbidden.clone());
            }
        }
        let only_post: HandlerFn = Arc::new(|_| text(405, "Only POST or PUT requests allowed"));
        r.get(&p("/-/quit"), only_post.clone());
        r.get(&p("/-/reload"), only_post);

        r.get(&p("/debug/*subpath"), Arc::new(serve_debug));
        r.post(&p("/debug/*subpath"), Arc::new(serve_debug));

        let app = o.app_name.clone();
        r.get(&p("/-/healthy"), Arc::new(move |_| text(200, format!("{} is Healthy.\n", app))));
        r.head(&p("/-/healthy"), Arc::new(|_| Response::new(200)));
        let app = o.app_name.clone();
        r.get(&p("/-/ready"), ready_wrap(w, Arc::new(move |_| text(200, format!("{} is Ready.\n", app)))));
        r.head(&p("/-/ready"), ready_wrap(w, Arc::new(|_| Response::new(200))));
        r
    }

    /// The API (Go `h.apiV1`).
    pub fn api(&self) -> &Arc<Api> {
        &self.api
    }

    /// The main router (Go `h.router`: everything but the API).
    pub fn router(&self) -> &Router {
        &self.router
    }

    /// The handler's metrics.
    pub fn metrics(&self) -> &Arc<WebMetrics> {
        &self.metrics
    }

    /// The options the handler was built with.
    pub fn options(&self) -> &Options {
        &self.options
    }

    /// The state guarded by Go's `h.mtx` (read lock).
    pub fn state(&self) -> RwLockReadGuard<'_, State> {
        self.state.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Go `ApplyConfig`.
    pub fn apply_config(&self, conf: Arc<config::Config>) {
        let mut s = self.state.write().unwrap_or_else(|e| e.into_inner());
        if let Some(r) = conf.storage_config.tsdb_config.as_ref().and_then(|t| t.retention.as_ref()) {
            s.tsdb_retention_duration = r.time;
            s.tsdb_max_bytes = r.size;
            s.tsdb_max_percentage = r.percentage;
        }
        s.config = conf;
    }

    /// Go `SetReady`.
    pub fn set_ready(&self, v: ReadyStatus) {
        self.ready.store(v as u32, Ordering::SeqCst);
        self.metrics.ready_status.get().set(if v == ReadyStatus::Ready { 1.0 } else { 0.0 });
    }

    /// Go `isReady`.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst) == ReadyStatus::Ready as u32
    }

    /// Go `testReady` without the wrapped handler: `None` when ready, else the response to send.
    pub fn not_ready_response(&self) -> Option<Response> {
        match self.ready.load(Ordering::SeqCst) {
            1 => None,
            0 => Some(text(503, "Service Unavailable").with_header("X-Prometheus-Stopping", "false")),
            2 => Some(text(503, "Service Unavailable").with_header("X-Prometheus-Stopping", "true")),
            _ => Some(text(500, "Unknown state")),
        }
    }

    /// Go `Quit`: the signal set by `/-/quit`.
    pub fn quit_signal(&self) -> &QuitSignal {
        &self.quit
    }

    /// Go `Reload`: the reload requests of `/-/reload` (taken once; later calls return `None`).
    pub fn reload_requests(&self) -> Option<Receiver<ReloadRequest>> {
        self.reload_rx.lock().unwrap_or_else(|e| e.into_inner()).take()
    }

    fn quit(&self) -> Response {
        if self.quit.fire() {
            text(200, "Requesting termination... Goodbye!")
        } else {
            text(200, "Termination already in progress.")
        }
    }

    fn reload(&self) -> Response {
        let (tx, rx) = channel();
        let sent = self.reload_tx.lock().unwrap_or_else(|e| e.into_inner()).send(tx).is_ok();
        let res = if sent { rx.recv().unwrap_or(Ok(())) } else { Ok(()) };
        match res {
            Ok(()) => Response::new(200),
            Err(e) => http_error(&format!("failed to reload config: {}", e), 500),
        }
    }

    fn version(&self) -> Response {
        let mut s = String::new();
        match &self.options.version {
            Some(v) => v.write_json(&mut s),
            None => s.push_str("null"),
        }
        s.push('\n');
        text(200, s)
    }

    /// `/metrics`: promhttp's handler over the gatherer, with promhttp's own handler metrics.
    pub fn serve_metrics(&self, req: &Request) -> Response {
        let pm = crate::metrics::promhttp_metrics();
        let in_flight = pm.in_flight.get();
        in_flight.inc();
        let g = self.options.gatherer.clone().unwrap_or_else(crate::metrics::default_gatherer);
        let resp = crate::metrics::serve_metrics(&*g, req, &crate::expfmt::DEFAULT_ACCEPTED_FORMATS);
        in_flight.dec();
        pm.requests.with_label_values(&[&resp.status.to_string()]).inc();
        resp
    }

    /// Go `consolesPath`.
    fn consoles_path(&self) -> String {
        let o = &self.options;
        if std::path::Path::new(&format!("{}/index.html", o.console_templates_path)).exists() {
            return format!("{}/consoles/index.html", o.external_url.path);
        }
        if !o.user_assets_path.is_empty() && std::path::Path::new(&format!("{}/index.html", o.user_assets_path)).exists() {
            return format!("{}/user/index.html", o.external_url.path);
        }
        String::new()
    }

    fn serve_react_app(&self, root: &str) -> Response {
        let index = format!("{}/index.html", root);
        let idx = match self.options.ui_assets.open(&index) {
            Some(Asset::File(b)) => b,
            _ => {
                return text(500, format!("Error opening React index.html: open {}: file does not exist", index.trim_start_matches('/')));
            }
        };
        let lookback = yaml::Duration(self.options.lookback_delta.as_nanos() as i64).to_string();
        let out = replace_placeholders(
            &idx,
            &IndexPlaceholders {
                consoles_link: &self.consoles_path(),
                title: &self.options.page_title,
                agent_mode: self.options.is_agent,
                ready: self.is_ready(),
                lookback_delta: &lookback,
            },
        );
        Response::text(200, "text/html; charset=utf-8", out)
    }

    fn consoles(&self, req: &Request) -> Response {
        let name = format!("/{}", req.param("filepath"));
        let base = &self.options.console_templates_path;
        let dir = if base.is_empty() { "." } else { base.as_str() };
        // Go http.Dir.Open: the cleaned path under the directory.
        let full = go_path_join(&[dir, &go_path_clean(&name)]);
        let text_bytes = match std::fs::read(&full) {
            Ok(b) => b,
            Err(e) => {
                let msg = if e.kind() == std::io::ErrorKind::NotFound {
                    format!("open {}: no such file or directory", full)
                } else if e.kind() == std::io::ErrorKind::IsADirectory || std::path::Path::new(&full).is_dir() {
                    format!("read {}: is a directory", full)
                } else {
                    format!("open {}: {}", full, e)
                };
                let code = if msg.starts_with("read ") { 500 } else { 404 };
                return http_error(&msg, code);
            }
        };
        let raw = match go_parse_query(&req.query) {
            Ok(r) => r,
            Err(e) => return http_error(&e, 400),
        };
        let mut raw_params: Vec<(String, Vec<String>)> = Vec::new();
        for (k, v) in raw {
            match raw_params.iter_mut().find(|(n, _)| *n == k) {
                Some(e) => e.1.push(v),
                None => raw_params.push((k, vec![v])),
            }
        }
        let params: BTreeMap<String, String> = raw_params.iter().map(|(k, v)| (k.clone(), v[0].clone())).collect();
        let mut external_labels = BTreeMap::new();
        self.state().config.global_config.external_labels.range(|n, v| {
            external_labels.insert(n.to_string(), v.to_string());
        });
        let mut library_files: Vec<PathBuf> = std::fs::read_dir(&self.options.console_libraries_path)
            .map(|rd| rd.filter_map(|e| e.ok().map(|e| e.path())).filter(|p| p.extension().is_some_and(|x| x == "lib")).collect())
            .unwrap_or_default();
        library_files.sort();
        let Some(exp) = &self.options.console_expander else {
            return http_error("console templates are not supported: no template engine configured", 500);
        };
        let text = String::from_utf8_lossy(&text_bytes);
        let cr = ConsoleRequest {
            text: &text,
            name: format!("__console_{}", name),
            raw_params,
            params,
            path: name.trim_start_matches('/').to_string(),
            external_labels,
            library_files,
            now_ms: (self.now)(),
        };
        match exp(&cr) {
            Ok(html) => text_html(html),
            Err(e) => http_error(&e, 500),
        }
    }

    /// Go `runtimeInfo`. Go-only fields get the closest equivalents: `goroutineCount` is the OS thread count,
    /// `GOMAXPROCS` the available parallelism, `GOMEMLIMIT` Go's "no limit" (max int64), `GOGC` and `GODEBUG`
    /// the environment variables.
    pub fn runtime_info(&self) -> Result<RuntimeInfo, Error> {
        let mut status = RuntimeInfo {
            start_time: self.birth,
            cwd: self.cwd.clone(),
            goroutine_count: crate::metrics::thread_count() as i64,
            gomaxprocs: std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1) as i64,
            gomemlimit: i64::MAX,
            gogc: std::env::var("GOGC").unwrap_or_default(),
            godebug: std::env::var("GODEBUG").unwrap_or_default(),
            ..Default::default()
        };
        match read_hostname() {
            Ok(hn) => status.hostname = hn,
            Err(e) => return Err(Error::msg(format!("error getting hostname: {}", e))),
        }
        status.server_time = Time::now();
        {
            let s = self.state();
            if s.tsdb_retention_duration.0 != 0 {
                status.storage_retention = s.tsdb_retention_duration.to_string();
            }
            if s.tsdb_max_bytes.0 != 0 {
                if !status.storage_retention.is_empty() {
                    status.storage_retention.push_str(" or ");
                }
                status.storage_retention.push_str(&s.tsdb_max_bytes.go_string());
            }
            if s.tsdb_max_percentage != 0.0 {
                if !status.storage_retention.is_empty() {
                    status.storage_retention.push_str(" or ");
                }
                model::histogram::gofmt::write_g(&mut status.storage_retention, s.tsdb_max_percentage);
                status.storage_retention.push('%');
            }
        }
        let g = self.options.gatherer.clone().unwrap_or_else(crate::metrics::default_gatherer);
        let mfs = g.gather().map_err(|e| Error::msg(format!("error gathering runtime status: {}", e)))?;
        for mf in &mfs {
            match mf.name.as_str() {
                "prometheus_tsdb_wal_corruptions_total" => status.corruption_count = to_float64(mf) as i64,
                "prometheus_config_last_reload_successful" => status.reload_config_success = to_float64(mf) != 0.0,
                "prometheus_config_last_reload_success_timestamp_seconds" => {
                    status.last_config_time = Time::unix(to_float64(mf) as i64, 0)
                }
                _ => {}
            }
        }
        Ok(status)
    }

    /// Serves one request (Go's mux: the API under `<prefix>/api/v1/`, everything else on the main router).
    pub fn serve(&self, req: Request) -> Response {
        // Go's ServeMux redirects to the cleaned path.
        if req.method != "CONNECT" {
            let cleaned = clean_mux_path(&req.path);
            if cleaned != req.path {
                let mut url = cleaned;
                if !req.query.is_empty() {
                    url.push('?');
                    url.push_str(&req.query);
                }
                let mut r = Response::new(301);
                r.set_header("Location", &url);
                if req.method == "GET" || req.method == "HEAD" {
                    r.set_header("Content-Type", "text/html; charset=utf-8");
                    r.body = Body::Bytes(format!("<a href=\"{}\">Moved Permanently</a>.\n\n", crate::route::html_escape(&url)).into_bytes());
                }
                return r;
            }
        }
        let api_v1 = format!("{}/v1", self.api_path);
        if req.path == api_v1 {
            // Go's ServeMux redirects a subtree root without the slash with 307.
            return go_redirect(&req, &format!("{}/", api_v1), 307);
        }
        if req.path.starts_with(&format!("{}/", api_v1)) {
            if req.path.len() == api_v1.len() + 1 {
                // StripPrefix leaves "/", which the API router does not route.
                return not_found();
            }
            return self.api_router.serve(req);
        }
        self.router.serve(req)
    }

    /// Go `Listeners`: binds every listen address (":port" binds all interfaces, as in Go).
    pub fn listeners(&self) -> Result<Vec<http::Server>, String> {
        let mut out = Vec::new();
        for a in &self.options.listen_addresses {
            out.push(self.listener(a)?);
        }
        Ok(out)
    }

    /// Go `Listener`.
    pub fn listener(&self, address: &str) -> Result<http::Server, String> {
        self.logger.info("Start listening for connections", &[("address", address)]);
        let addr = if address.starts_with(':') { format!("0.0.0.0{}", address) } else { address.to_string() };
        let cfg = http::ServerConfig {
            max_conns: if self.options.max_connections == 0 { 1024 } else { self.options.max_connections },
            idle_timeout: if self.options.read_timeout.is_zero() { Duration::from_secs(300) } else { self.options.read_timeout },
            gzip: false,
            // Queries and federation recurse deeper than the server's 64 KiB default (Go stacks grow). Only the
            // touched pages of a stack use memory.
            stack_size: 4 << 20,
            ..Default::default()
        };
        http::Server::bind(&addr, cfg).map_err(|e| format!("listen tcp {}: {}", address, e))
    }

    /// Go `Run`: serves on `listeners` (or on new ones for the listen addresses when empty) until `ctx` is
    /// cancelled. `web_config_file` (TLS, basic auth) is not supported and must be empty.
    pub fn run(self: &Arc<Self>, ctx: &CancelToken, listeners: Vec<http::Server>, web_config_file: &str) -> Result<(), String> {
        if !web_config_file.is_empty() {
            return Err("web config files (TLS, basic auth) are not supported".to_string());
        }
        let listeners = if listeners.is_empty() { self.listeners()? } else { listeners };
        if self.options.route_prefix != "/" {
            self.logger.info("Router prefix", &[("prefix", &self.options.route_prefix)]);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let mut addrs: Vec<SocketAddr> = Vec::new();
        let mut threads = Vec::new();
        for l in listeners {
            if let Some(a) = l.local_addr() {
                addrs.push(a);
            }
            let svc: Arc<dyn http::Service> = Arc::new(Service(self.clone()));
            let stop = stop.clone();
            threads.push(std::thread::spawn(move || l.run_until(svc, stop)));
        }
        ctx.wait();
        stop.store(true, Ordering::SeqCst);
        for a in &addrs {
            let mut a = *a;
            if a.ip().is_unspecified() {
                a.set_ip(std::net::Ipv4Addr::LOCALHOST.into());
            }
            let _ = TcpStream::connect_timeout(&a, Duration::from_secs(1));
        }
        for t in threads {
            let _ = t.join();
        }
        Ok(())
    }
}

fn text_html(s: String) -> Response {
    Response::text(200, "text/html; charset=utf-8", s)
}

/// Go ServeMux `cleanPath`: path.Clean, keeping a trailing slash.
fn clean_mux_path(p: &str) -> String {
    if p.is_empty() {
        return "/".to_string();
    }
    let mut p2 = if p.starts_with('/') { p.to_string() } else { format!("/{}", p) };
    let np = go_path_clean(&p2);
    if p2.ends_with('/') && np != "/" {
        if p2.len() == np.len() + 1 && p2.starts_with(&np) {
            return p2;
        }
        p2 = format!("{}/", np);
        return p2;
    }
    np
}

/// The HTTP service of a handler.
struct Service(Arc<Handler>);

impl http::Service for Service {
    fn handle(&self, req: Request) -> Response {
        let h = self.0.clone();
        with_stack_tracer(&h.logger, req, |r| h.serve(r))
    }
}

/// Go `http.ErrAbortHandler`: a panic payload that aborts the response without logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrAbortHandler;

/// Go `withStackTracer`: logs the stack trace when the request panics, then re-raises the panic. An
/// [`ErrAbortHandler`] payload is re-raised without logging.
pub fn with_stack_tracer(l: &Logger, req: Request, h: impl FnOnce(Request) -> Response) -> Response {
    let client = req.remote_ip.clone();
    let url = req.uri();
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| h(req))) {
        Ok(r) => r,
        Err(p) => {
            if p.downcast_ref::<ErrAbortHandler>().is_none() {
                let msg = if let Some(s) = p.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = p.downcast_ref::<&str>() {
                    s.to_string()
                } else {
                    "panic".to_string()
                };
                let stack = std::backtrace::Backtrace::force_capture().to_string();
                l.error("panic while serving request", &[("client", &client), ("url", &url), ("err", &msg), ("stack", &stack)]);
            }
            std::panic::resume_unwind(p)
        }
    }
}

/// Go `serveDebug`. pprof is Go-only (see the module comment).
fn serve_debug(req: Request) -> Response {
    let subpath = format!("/{}", req.param("subpath"));
    let subpath = if req.path.ends_with('/') && !subpath.ends_with('/') { format!("{}/", subpath) } else { subpath };
    if subpath == "/pprof" {
        return go_redirect(&req, &format!("{}/", req.path), 301);
    }
    let Some(sub) = subpath.strip_prefix("/pprof/") else {
        return not_found();
    };
    match sub {
        "cmdline" => {
            let args: Vec<String> = std::env::args().collect();
            Response::text(200, "text/plain; charset=utf-8", args.join("\u{0}")).with_header("X-Content-Type-Options", "nosniff")
        }
        "profile" | "symbol" | "trace" | "fgprof" => http_error("Go profiling is not supported by this server", 501),
        "" => {
            let mut b = String::from("<html>\n<head>\n<title>/debug/pprof/</title>\n</head>\n<body>\n/debug/pprof/\n<br>\n<p>Profiles:</p>\n<table>\n");
            for p in ["allocs", "block", "goroutine", "heap", "mutex", "threadcreate"] {
                b.push_str(&format!("<tr><td><a href='{p}?debug=1'>{p}</a></td></tr>\n"));
            }
            b.push_str("</table>\n</body>\n</html>\n");
            Response::text(200, "text/html; charset=utf-8", b)
        }
        "goroutine" | "threadcreate" => {
            let n = crate::metrics::thread_count();
            Response::text(200, "text/plain; charset=utf-8", format!("{} profile: total {}\n", sub, n)).with_header("X-Content-Type-Options", "nosniff")
        }
        "allocs" | "block" | "heap" | "mutex" => Response::text(
            200,
            "text/plain; charset=utf-8",
            format!("{} profile: not available (Go runtime profile)\n", sub),
        )
        .with_header("X-Content-Type-Options", "nosniff"),
        _ => http_error("Unknown profile", 404).with_header("X-Go-Pprof", "1"),
    }
}

/// Go `route.FileServe(dir)` (`http.FileServer(http.Dir(dir))` at the `filepath` parameter).
fn file_serve(req: &Request, dir: &std::path::Path) -> Response {
    let mut name = format!("/{}", req.param("filepath"));
    if req.path.ends_with('/') && !name.ends_with('/') {
        name.push('/');
    }
    if name.ends_with("/index.html") {
        return go_redirect(req, "./", 301);
    }
    let clean = go_path_clean(&name);
    let Some(full) = http::staticfile::safe_join(dir, clean.trim_start_matches('/')) else {
        return not_found();
    };
    let meta = match std::fs::metadata(&full) {
        Ok(m) => m,
        Err(_) => return not_found(),
    };
    let base = clean.rsplit('/').next().unwrap_or("").to_string();
    if meta.is_dir() {
        if !name.ends_with('/') {
            let mut r = Response::new(301);
            r.set_header("Location", &format!("{}/", base));
            return r;
        }
        let index = full.join("index.html");
        if index.is_file() {
            return http::staticfile::serve(req, &full, "index.html", "").unwrap_or_else(not_found);
        }
        let mut names: Vec<String> = std::fs::read_dir(&full)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| {
                        let mut n = e.file_name().to_string_lossy().into_owned();
                        if e.path().is_dir() {
                            n.push('/');
                        }
                        n
                    })
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        let mut b = String::from("<!doctype html>\n<meta name=\"viewport\" content=\"width=device-width\">\n<pre>\n");
        for n in names {
            b.push_str(&format!("<a href=\"{}\">{}</a>\n", http::url_encode(&n).replace("%2F", "/"), crate::route::html_escape(&n)));
        }
        b.push_str("</pre>\n");
        return Response::text(200, "text/html; charset=utf-8", b);
    }
    if name.ends_with('/') {
        return go_redirect(req, &format!("../{}", base), 301);
    }
    http::staticfile::serve(req, dir, clean.trim_start_matches('/'), "").unwrap_or_else(not_found)
}

// ---------------------------------------------------------------------------------------------------------------
// TSDB adapter.

/// The TSDB as the web handler uses it (Go's `readyStorage`/test `dbAdapter`): local storage for federation and
/// the admin and status operations of the API.
pub struct TsdbAdapter(pub Arc<tsdb::db::DB>);

impl Queryable for TsdbAdapter {
    fn querier(&self, mint: i64, maxt: i64) -> Result<storage::BoxQuerier, Error> {
        self.0.querier(mint, maxt)
    }
}

impl storage::ChunkQueryable for TsdbAdapter {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<storage::BoxChunkQuerier, Error> {
        storage::ChunkQueryable::chunk_querier(&*self.0, mint, maxt)
    }
}

impl TsdbAdminStats for TsdbAdapter {
    fn clean_tombstones(&self) -> Result<(), Error> {
        self.0.clean_tombstones()
    }
    fn delete(&self, ctx: &Context, mint: i64, maxt: i64, ms: &[model::labels::Matcher]) -> Result<(), Error> {
        self.0.delete(ctx, mint, maxt, ms)
    }
    fn snapshot(&self, dir: &std::path::Path, with_head: bool) -> Result<(), Error> {
        self.0.snapshot(dir, with_head)
    }
    fn stats(&self, stats_by_label_name: &str, limit: usize) -> Result<tsdb::head::Stats, Error> {
        Ok(self.0.head().stats(stats_by_label_name, limit))
    }
    fn wal_replay_status(&self) -> Result<WalReplayStatus, Error> {
        let s = self.0.head().stats.get_wal_replay_status();
        Ok(WalReplayStatus { min: s.min, max: s.max, current: s.current })
    }
    fn block_metas(&self) -> Result<Vec<tsdb::BlockMeta>, Error> {
        Ok(self.0.block_metas())
    }
}
