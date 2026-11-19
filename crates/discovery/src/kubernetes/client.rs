//! A small Kubernetes API client (the part of client-go the discovery uses): list and watch of one resource
//! kind in one namespace, over HTTP/1.1 with JSON, with Kubernetes `Status` errors.
//!
//! Differences from client-go: JSON instead of protobuf (`application/vnd.kubernetes.protobuf`), HTTP/1.1 (one
//! connection per watch) instead of HTTP/2, no client-side rate limiter (client-go: 5 QPS, burst 10), and no
//! watch-list streaming (`sendInitialEvents`); the reflector always lists and then watches.
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use json::Json;

use super::api::{Kind, Object};

/// metav1.ListOptions (the fields the reflector sets).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListOptions {
    pub label_selector: String,
    pub field_selector: String,
    pub resource_version: String,
    pub timeout_seconds: Option<i64>,
    pub allow_watch_bookmarks: bool,
    pub limit: i64,
    pub continue_: String,
}

impl ListOptions {
    /// The URL query (Go `url.Values.Encode`: sorted by key).
    pub fn query(&self, watch: bool) -> String {
        let mut q: Vec<(&str, String)> = Vec::new();
        if self.allow_watch_bookmarks {
            q.push(("allowWatchBookmarks", "true".into()));
        }
        if !self.continue_.is_empty() {
            q.push(("continue", self.continue_.clone()));
        }
        if !self.field_selector.is_empty() {
            q.push(("fieldSelector", self.field_selector.clone()));
        }
        if !self.label_selector.is_empty() {
            q.push(("labelSelector", self.label_selector.clone()));
        }
        if self.limit > 0 {
            q.push(("limit", self.limit.to_string()));
        }
        if !self.resource_version.is_empty() {
            q.push(("resourceVersion", self.resource_version.clone()));
        }
        if let Some(t) = self.timeout_seconds {
            // the request timeout (Go `Request.Timeout`), then the server-side watch timeout
            q.push(("timeout", go_duration_secs(t)));
            q.push(("timeoutSeconds", t.to_string()));
        }
        if watch {
            q.push(("watch", "true".into()));
        }
        q.iter().map(|(k, v)| format!("{}={}", k, yaml::url::query_escape(v))).collect::<Vec<_>>().join("&")
    }
}

/// Go `time.Duration.String` of whole seconds ("5m0s").
fn go_duration_secs(s: i64) -> String {
    let (h, m, sec) = (s / 3600, (s % 3600) / 60, s % 60);
    if h > 0 {
        format!("{h}h{m}m{sec}s")
    } else if m > 0 {
        format!("{m}m{sec}s")
    } else {
        format!("{sec}s")
    }
}

/// An API error: a Kubernetes `Status` (Go `apierrors.StatusError`) or a transport error.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ApiError {
    /// HTTP status code (0 for transport errors).
    pub code: i32,
    /// metav1.StatusReason ("Expired", "Gone", "TooManyRequests", ...).
    pub reason: String,
    pub message: String,
    /// details.causes[].reason
    pub causes: Vec<String>,
    /// The connection was refused (Go `utilnet.IsConnectionRefused`).
    pub connection_refused: bool,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

const KNOWN_REASONS: &[&str] = &[
    "Unauthorized", "Forbidden", "NotFound", "AlreadyExists", "Conflict", "Gone", "Invalid", "ServerTimeout", "StoreReadError", "Timeout",
    "TooManyRequests", "BadRequest", "MethodNotAllowed", "NotAcceptable", "RequestEntityTooLarge", "UnsupportedMediaType", "InternalError",
    "Expired", "ServiceUnavailable",
];

impl ApiError {
    pub fn new(code: i32, reason: &str, message: impl Into<String>) -> ApiError {
        ApiError { code, reason: reason.to_string(), message: message.into(), ..Default::default() }
    }
    /// Go `apierrors.NewUnauthorized`.
    pub fn unauthorized(msg: &str) -> ApiError {
        let m = if msg.is_empty() { "not authorized" } else { msg };
        ApiError::new(401, "Unauthorized", m)
    }
    /// Go `apierrors.NewInternalError`.
    pub fn internal(msg: &str) -> ApiError {
        ApiError::new(500, "InternalError", format!("Internal error occurred: {msg}"))
    }
    fn transport(e: &net::Error) -> ApiError {
        ApiError { message: e.msg.clone(), connection_refused: e.msg.contains("connection refused"), ..Default::default() }
    }
    fn unknown_reason(&self) -> bool {
        !KNOWN_REASONS.contains(&self.reason.as_str())
    }
    /// Go `isExpiredError`: `apierrors.IsResourceExpired || apierrors.IsGone`.
    pub fn is_expired(&self) -> bool {
        self.reason == "Expired" || self.reason == "Gone" || (self.unknown_reason() && self.code == 410)
    }
    /// Go `apierrors.IsTooManyRequests`.
    pub fn is_too_many_requests(&self) -> bool {
        self.reason == "TooManyRequests" || (self.unknown_reason() && self.code == 429)
    }
    /// Go `apierrors.IsInternalError`.
    pub fn is_internal(&self) -> bool {
        self.reason == "InternalError" || (self.unknown_reason() && self.code == 500)
    }
    /// Go `isTooLargeResourceVersionError`.
    pub fn is_too_large_resource_version(&self) -> bool {
        if self.causes.iter().any(|c| c == "ResourceVersionTooLarge") {
            return true;
        }
        let timeout = self.reason == "Timeout" || (self.unknown_reason() && self.code == 504);
        timeout && self.message.contains("Too large resource version")
    }
    /// Go `isWatchErrorRetriable`.
    pub fn is_watch_retriable(&self) -> bool {
        self.connection_refused || self.is_too_many_requests()
    }

    /// Go `apierrors.FromObject` of a `Status` object.
    pub fn from_status(v: &Json) -> ApiError {
        let causes = v.get("details").and_then(|d| d.get("causes")).and_then(|c| c.as_arr()).map(|a| a.iter().map(|c| c.str_of("reason").to_string()).collect()).unwrap_or_default();
        ApiError { code: v.get("code").and_then(|c| c.as_i64()).unwrap_or(0) as i32, reason: v.str_of("reason").to_string(), message: v.str_of("message").to_string(), causes, connection_refused: false }
    }

    /// The error of a non-2xx response (Go `rest.Request.transformResponse` / `newUnstructuredResponseError`).
    fn from_response(code: u16, body: &[u8], kind: Kind, verb: &str) -> ApiError {
        if let Ok(v) = Json::parse_bytes(body)
            && v.str_of("kind") == "Status"
        {
            let mut e = ApiError::from_status(&v);
            if e.code == 0 {
                e.code = code as i32;
            }
            return e;
        }
        let (reason, msg) = match code {
            409 => ("Conflict", "the server reported a conflict".to_string()),
            404 => ("NotFound", "the server could not find the requested resource".to_string()),
            400 => ("BadRequest", "the server rejected our request for an unknown reason".to_string()),
            401 => ("Unauthorized", "the server has asked for the client to provide credentials".to_string()),
            403 => ("Forbidden", String::from_utf8_lossy(body).trim().to_string()),
            405 => ("MethodNotAllowed", "the server does not allow this method on the requested resource".to_string()),
            410 => ("Gone", "the server has asked for the client to retry the request".to_string()),
            413 => ("RequestEntityTooLarge", "the server has rejected the request due to its size being too large".to_string()),
            415 => ("UnsupportedMediaType", "the server was unable to respond with a content type that the client supports".to_string()),
            429 => ("TooManyRequests", "the server has received too many requests and has asked us to try again later".to_string()),
            500 => ("InternalError", "an error on the server has prevented the request from succeeding".to_string()),
            503 => ("ServiceUnavailable", "the server is currently unable to handle the request".to_string()),
            504 => ("Timeout", "the server was unable to return a response in the time allotted, but may still be processing the request".to_string()),
            _ => ("", format!("an error on the server ({}) has prevented the request from succeeding", String::from_utf8_lossy(body).trim())),
        };
        let msg = if code == 404 || code == 405 { format!("{msg} ({verb} {})", kind.resource()) } else { msg };
        ApiError::new(code as i32, reason, msg)
    }
}

/// One watch event (Go `watch.Event`).
#[derive(Clone, Debug, PartialEq)]
pub enum WatchEvent {
    Added(Object),
    Modified(Object),
    Deleted(Object),
    /// A bookmark: only the resource version and the annotations of the object.
    Bookmark(String),
    Error(ApiError),
}

impl WatchEvent {
    /// The resource version of the event object.
    pub fn resource_version(&self) -> &str {
        match self {
            WatchEvent::Added(o) | WatchEvent::Modified(o) | WatchEvent::Deleted(o) => &o.meta().resource_version,
            WatchEvent::Bookmark(rv) => rv,
            WatchEvent::Error(_) => "",
        }
    }

    /// Decodes a JSON watch event (`{"type": ..., "object": ...}`).
    pub fn decode(kind: Kind, v: &Json) -> Result<WatchEvent, ApiError> {
        let obj = v.get("object").unwrap_or(&Json::Null);
        Ok(match v.str_of("type") {
            "ADDED" => WatchEvent::Added(kind.decode(obj)),
            "MODIFIED" => WatchEvent::Modified(kind.decode(obj)),
            "DELETED" => WatchEvent::Deleted(kind.decode(obj)),
            "BOOKMARK" => WatchEvent::Bookmark(obj.get("metadata").map(|m| m.str_of("resourceVersion").to_string()).unwrap_or_default()),
            "ERROR" => WatchEvent::Error(ApiError::from_status(obj)),
            t => return Err(ApiError::internal(&format!("got invalid watch event type: {t}"))),
        })
    }
}

/// The result of a list (one page).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ObjectList {
    pub resource_version: String,
    pub continue_: String,
    pub items: Vec<Object>,
}

/// Stops a list or watch in flight from another thread (Go: cancelling the request context).
#[derive(Clone, Default)]
pub struct Stopper(Arc<Mutex<StopState>>);

#[derive(Default)]
struct StopState {
    stopped: bool,
    action: Option<Box<dyn FnOnce() + Send>>,
}

impl Stopper {
    pub fn new() -> Stopper {
        Stopper::default()
    }
    /// Stops the current request and every later one.
    pub fn stop(&self) {
        let a = {
            let mut s = self.0.lock().unwrap_or_else(|e| e.into_inner());
            s.stopped = true;
            s.action.take()
        };
        if let Some(a) = a {
            a();
        }
    }
    pub fn is_stopped(&self) -> bool {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).stopped
    }
    /// Sets what `stop` does for the current request; false (and `f` is dropped) when already stopped.
    pub fn set(&self, f: Box<dyn FnOnce() + Send>) -> bool {
        let mut s = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if s.stopped {
            return false;
        }
        s.action = Some(f);
        true
    }
    pub fn clear(&self) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).action = None;
    }
}

/// Receives watch events; returning false ends the watch.
pub type EventHandler = Box<dyn FnMut(WatchEvent) -> bool + Send>;

/// The API calls the reflector makes (Go `cache.ListerWatcher` for each resource).
pub trait Client: Send + Sync {
    fn list(&self, kind: Kind, namespace: &str, opts: &ListOptions, stop: &Stopper) -> Result<ObjectList, ApiError>;
    /// Opens a watch and hands every event to `on_event` until the stream ends (Ok), `on_event` returns false
    /// or `stop` is stopped. Errors while opening the watch are returned; errors in the stream are
    /// `WatchEvent::Error` events.
    fn watch(&self, kind: Kind, namespace: &str, opts: &ListOptions, stop: &Stopper, on_event: EventHandler) -> Result<(), ApiError>;
}

/// The client of a real API server over HTTP.
pub struct HttpClient {
    /// scheme://host[:port][/path prefix], without a trailing slash.
    base: String,
    client: net::Client,
    user_agent: String,
}

/// Splits a stream of JSON values (Go `json.Decoder` framing of a watch body).
#[derive(Default)]
struct JsonFramer {
    buf: Vec<u8>,
    depth: usize,
    in_str: bool,
    esc: bool,
    /// bytes of `buf` already scanned
    scanned: usize,
}

impl JsonFramer {
    /// Adds bytes and returns the complete values.
    fn push(&mut self, b: &[u8], out: &mut Vec<Vec<u8>>) {
        self.buf.extend_from_slice(b);
        let mut start = 0;
        let mut i = self.scanned;
        while i < self.buf.len() {
            let c = self.buf[i];
            if self.in_str {
                if self.esc {
                    self.esc = false;
                } else if c == b'\\' {
                    self.esc = true;
                } else if c == b'"' {
                    self.in_str = false;
                }
            } else {
                match c {
                    b'"' => self.in_str = true,
                    b'{' | b'[' => self.depth += 1,
                    b'}' | b']' => {
                        self.depth = self.depth.saturating_sub(1);
                        if self.depth == 0 {
                            out.push(self.buf[start..=i].to_vec());
                            start = i + 1;
                        }
                    }
                    _ => {
                        if self.depth == 0 && c.is_ascii_whitespace() {
                            start = i + 1;
                        }
                    }
                }
            }
            i += 1;
        }
        self.buf.drain(..start);
        self.scanned = self.buf.len();
    }
}

impl HttpClient {
    /// A client for `host` (a URL; without a scheme, https when TLS is configured as client-go does) with the
    /// HTTP client configuration.
    pub fn new(host: &str, cfg: &net::HttpClientConfig, user_agent: &str) -> Result<HttpClient, String> {
        let mut base = host.to_string();
        if !base.contains("://") {
            let tls = cfg.tls_config != net::TlsConfig::default();
            base = format!("{}://{}", if tls { "https" } else { "http" }, base);
        }
        let u = yaml::url::parse(&base).map_err(|e| format!("host must be a URL or a host:port pair: {e}"))?;
        if u.host.is_empty() {
            return Err(format!("host must be a URL or a host:port pair: {}", yaml::go_quote(host)));
        }
        let base = format!("{}://{}{}", u.scheme, u.host, u.path.trim_end_matches('/'));
        let client = net::new_client_from_config(cfg, &net::HttpClientOptions::default()).map_err(|e| e.to_string())?;
        Ok(HttpClient { base, client, user_agent: user_agent.to_string() })
    }

    fn request(&self, kind: Kind, namespace: &str, query: &str, stop: &Stopper) -> Result<net::Request, ApiError> {
        let mut url = format!("{}{}", self.base, kind.path(namespace));
        if !query.is_empty() {
            url.push('?');
            url.push_str(query);
        }
        let mut req = net::Request::get(&url).map_err(|e| ApiError::transport(&e))?;
        req.header.set("Accept", "application/json");
        req.header.set("User-Agent", &self.user_agent);
        let abort = net::Cancel::new();
        let a = abort.clone();
        if !stop.set(Box::new(move || a.cancel())) {
            return Err(ApiError::new(0, "", "context canceled"));
        }
        req.cancel = Some(abort);
        Ok(req)
    }
}

impl Client for HttpClient {
    fn list(&self, kind: Kind, namespace: &str, opts: &ListOptions, stop: &Stopper) -> Result<ObjectList, ApiError> {
        let req = self.request(kind, namespace, &opts.query(false), stop)?;
        let r = self.client.do_request(req);
        stop.clear();
        let resp = r.map_err(|e| ApiError::transport(&e))?;
        if !(200..300).contains(&resp.status) {
            return Err(ApiError::from_response(resp.status, &resp.body, kind, "get"));
        }
        let v = Json::parse_bytes(&resp.body).map_err(|e| ApiError::internal(&e.to_string()))?;
        drop(resp);
        let meta = v.get("metadata");
        let mut out = ObjectList {
            resource_version: meta.map(|m| m.str_of("resourceVersion").to_string()).unwrap_or_default(),
            continue_: meta.map(|m| m.str_of("continue").to_string()).unwrap_or_default(),
            items: Vec::new(),
        };
        if let Some(items) = v.get("items").and_then(|i| i.as_arr()) {
            out.items = items.iter().map(|i| kind.decode(i)).collect();
        }
        Ok(out)
    }

    fn watch(&self, kind: Kind, namespace: &str, opts: &ListOptions, stop: &Stopper, mut on_event: EventHandler) -> Result<(), ApiError> {
        let mut req = self.request(kind, namespace, &opts.query(true), stop)?;
        if let Some(t) = opts.timeout_seconds {
            // client-go sets the same client-side timeout; reaching it ends the watch like a server close
            req.timeout = Some(Duration::from_secs(t.max(0) as u64));
        }
        let ended = Arc::new(Mutex::new(false));
        let ended2 = ended.clone();
        let mut framer = JsonFramer::default();
        let mut values = Vec::new();
        req.body_sink = Some(net::BodySink::new(move |b| {
            framer.push(b, &mut values);
            for v in values.drain(..) {
                let ev = match Json::parse_bytes(&v) {
                    Ok(j) => WatchEvent::decode(kind, &j).unwrap_or_else(WatchEvent::Error),
                    Err(e) => WatchEvent::Error(ApiError::internal(&format!("unable to decode an event from the watch stream: {e}"))),
                };
                if !on_event(ev) {
                    *ended2.lock().unwrap_or_else(|e| e.into_inner()) = true;
                    return false;
                }
            }
            true
        }));
        let r = self.client.do_request(req);
        stop.clear();
        match r {
            Ok(resp) if (200..300).contains(&resp.status) => Ok(()),
            Ok(resp) => Err(ApiError::from_response(resp.status, &resp.body, kind, "watch")),
            // the stream broke after it started, or was stopped: the watch ends
            Err(e) if stop.is_stopped() || e.is_timeout() || *ended.lock().unwrap_or_else(|e| e.into_inner()) => Ok(()),
            Err(e) => Err(ApiError::transport(&e)),
        }
    }
}

/// How to reach the API server: its URL and the HTTP client configuration (TLS, credentials, proxy).
#[derive(Clone, Debug, PartialEq)]
pub struct RestConfig {
    pub host: String,
    pub http: net::HttpClientConfig,
}

pub const SERVICE_ACCOUNT_TOKEN_FILE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
pub const SERVICE_ACCOUNT_CA_FILE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
pub const SERVICE_ACCOUNT_NAMESPACE_FILE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";

/// Go `rest.InClusterConfig`: the API server from `KUBERNETES_SERVICE_HOST`/`_PORT`, the service account token
/// (re-read from its file) and CA.
pub fn in_cluster_config() -> Result<RestConfig, String> {
    let host = std::env::var("KUBERNETES_SERVICE_HOST").unwrap_or_default();
    let port = std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_default();
    if host.is_empty() || port.is_empty() {
        return Err("unable to load in-cluster configuration, KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT must be defined".to_string());
    }
    std::fs::read(SERVICE_ACCOUNT_TOKEN_FILE).map_err(|e| format!("open {SERVICE_ACCOUNT_TOKEN_FILE}: {}", net::types::go_io_error(&e)))?;
    let mut http = net::HttpClientConfig::default_config();
    // the CA is used only when it loads (client-go logs the error otherwise)
    if std::fs::read(SERVICE_ACCOUNT_CA_FILE).is_ok_and(|b| String::from_utf8_lossy(&b).contains("-----BEGIN CERTIFICATE-----")) {
        http.tls_config.ca_file = SERVICE_ACCOUNT_CA_FILE.to_string();
    }
    http.authorization = Some(net::Authorization { type_: "Bearer".to_string(), credentials_file: SERVICE_ACCOUNT_TOKEN_FILE.to_string(), ..Default::default() });
    http.proxy_config.proxy_from_environment = true;
    let hp = if host.contains(':') { format!("[{host}]:{port}") } else { format!("{host}:{port}") };
    Ok(RestConfig { host: format!("https://{hp}"), http })
}
