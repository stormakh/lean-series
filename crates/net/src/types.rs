//! Request, response, header map and error types (the parts of Go net/http the client needs).
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use yaml::url::GoUrl;

/// Go `textproto.CanonicalMIMEHeaderKey`: "content-type" -> "Content-Type". Keys with invalid bytes are kept.
pub fn canonical_header_key(s: &str) -> String {
    let b = s.as_bytes();
    let valid = b.iter().all(|&c| c.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&c));
    if !valid || b.is_empty() {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut upper = true;
    for &c in b {
        let c = if upper { c.to_ascii_uppercase() } else { c.to_ascii_lowercase() };
        out.push(c as char);
        upper = c == b'-';
    }
    out
}

/// Go `http.Header`: canonical keys, several values per key, in insertion order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Header(pub Vec<(String, String)>);

impl Header {
    pub fn new() -> Header {
        Header(Vec::new())
    }
    pub fn get(&self, k: &str) -> Option<&str> {
        self.0.iter().find(|(n, _)| n.eq_ignore_ascii_case(k)).map(|(_, v)| v.as_str())
    }
    /// Go `Header.Get`: "" when absent.
    pub fn get_str(&self, k: &str) -> &str {
        self.get(k).unwrap_or("")
    }
    pub fn values(&self, k: &str) -> Vec<&str> {
        self.0.iter().filter(|(n, _)| n.eq_ignore_ascii_case(k)).map(|(_, v)| v.as_str()).collect()
    }
    pub fn has(&self, k: &str) -> bool {
        self.0.iter().any(|(n, _)| n.eq_ignore_ascii_case(k))
    }
    pub fn add(&mut self, k: &str, v: &str) {
        self.0.push((canonical_header_key(k), v.to_string()));
    }
    pub fn set(&mut self, k: &str, v: &str) {
        let key = canonical_header_key(k);
        if let Some(i) = self.0.iter().position(|(n, _)| n.eq_ignore_ascii_case(k)) {
            self.0[i] = (key, v.to_string());
            let mut j = i + 1;
            while j < self.0.len() {
                if self.0[j].0.eq_ignore_ascii_case(k) {
                    self.0.remove(j);
                } else {
                    j += 1;
                }
            }
        } else {
            self.0.push((key, v.to_string()));
        }
    }
    pub fn del(&mut self, k: &str) {
        self.0.retain(|(n, _)| !n.eq_ignore_ascii_case(k));
    }
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// An HTTP request. `url` must be absolute (http or https).
#[derive(Debug, Clone)]
pub struct Request {
    pub method: String,
    pub url: GoUrl,
    pub header: Header,
    /// Go `req.Host`: overrides the Host header ("" = from the URL).
    pub host: String,
    pub body: Vec<u8>,
    /// Deadline for the whole exchange (connect, TLS, write, read), like a Go context deadline.
    pub timeout: Option<Duration>,
    /// Response bodies larger than this fail with `ErrorKind::BodyTooLarge` (after decompression too).
    pub max_body_bytes: Option<usize>,
    /// Decode `Content-Encoding: gzip` / `deflate` bodies (Go's transport with compression on; Prometheus
    /// clients run with it off and decode themselves).
    pub decompress: bool,
    /// Hostnames of the earlier requests in a redirect chain, original first (Go's `req.Response` chain).
    pub redirect_hosts: Vec<String>,
    /// Lets another thread close the connection of this request while it is in flight (Go: cancelling the
    /// request's context).
    pub abort: Option<Abort>,
    /// Receives the body of a 2xx response piece by piece as it arrives instead of `Response::body` (Go:
    /// reading `resp.Body` while the server still sends, as watches do).
    pub body_sink: Option<BodySink>,
}

#[derive(Default)]
struct AbortState {
    aborted: bool,
    conn: Option<TcpStream>,
}

/// Aborts a request in flight from another thread: the connection is shut down, so a blocked read returns.
#[derive(Clone, Default)]
pub struct Abort(Arc<Mutex<AbortState>>);

impl std::fmt::Debug for Abort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Abort(aborted={})", self.is_aborted())
    }
}

impl Abort {
    pub fn new() -> Abort {
        Abort::default()
    }
    /// Closes the current connection (if any) and makes later requests with this handle fail at once.
    pub fn abort(&self) {
        let mut s = self.0.lock().unwrap_or_else(|e| e.into_inner());
        s.aborted = true;
        if let Some(c) = s.conn.take() {
            let _ = c.shutdown(std::net::Shutdown::Both);
        }
    }
    pub fn is_aborted(&self) -> bool {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).aborted
    }
    /// The transport registers the socket of the exchange; false when already aborted.
    pub(crate) fn register(&self, s: Option<TcpStream>) -> bool {
        let mut st = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if st.aborted {
            return false;
        }
        st.conn = s;
        true
    }
}

/// A receiver of body bytes: called with each piece; returning false stops reading (the connection closes).
#[derive(Clone)]
pub struct BodySink(pub Arc<Mutex<dyn FnMut(&[u8]) -> bool + Send>>);

impl BodySink {
    pub fn new(f: impl FnMut(&[u8]) -> bool + Send + 'static) -> BodySink {
        BodySink(Arc::new(Mutex::new(f)))
    }
    pub(crate) fn call(&self, b: &[u8]) -> bool {
        (self.0.lock().unwrap_or_else(|e| e.into_inner()))(b)
    }
}

impl std::fmt::Debug for BodySink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BodySink")
    }
}

impl Request {
    /// Go `http.NewRequest`.
    pub fn new(method: &str, url: &str, body: Vec<u8>) -> Result<Request, Error> {
        let u = yaml::url::parse(url).map_err(Error::other)?;
        Ok(Request { method: method.to_string(), url: u, header: Header::new(), host: String::new(), body, timeout: None, max_body_bytes: None, decompress: false, redirect_hosts: Vec::new(), abort: None, body_sink: None })
    }
    pub fn get(url: &str) -> Result<Request, Error> {
        Request::new("GET", url, Vec::new())
    }
    /// Go `req.SetBasicAuth`.
    pub fn set_basic_auth(&mut self, user: &str, pass: &str) {
        let v = crypto::base64(format!("{user}:{pass}").as_bytes());
        self.header.set("Authorization", &format!("Basic {v}"));
    }
    /// Go `req.BasicAuth`.
    pub fn basic_auth(&self) -> Option<(String, String)> {
        let a = self.header.get("Authorization")?;
        if a.len() < 6 || !a[..6].eq_ignore_ascii_case("basic ") {
            return None;
        }
        let d = crypto::base64_decode(&a[6..])?;
        let s = String::from_utf8(d).ok()?;
        let (u, p) = s.split_once(':')?;
        Some((u.to_string(), p.to_string()))
    }
    /// Go `req.UserAgent()`.
    pub fn user_agent(&self) -> &str {
        self.header.get_str("User-Agent")
    }
}

/// An HTTP response with the whole body read.
#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    /// Go `resp.Status`, for example "200 OK".
    pub status_text: String,
    pub header: Header,
    pub body: Vec<u8>,
    /// The request as the transport sent it (body dropped): Go `resp.Request`.
    pub request: Request,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// The deadline passed (Go: "context deadline exceeded").
    Timeout,
    /// The body is bigger than `max_body_bytes`.
    BodyTooLarge,
    Other,
}

/// An error with Go's text: the message users see in target pages and logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub kind: ErrorKind,
    pub msg: String,
}

impl Error {
    pub fn other(msg: impl Into<String>) -> Error {
        Error { kind: ErrorKind::Other, msg: msg.into() }
    }
    pub fn timeout() -> Error {
        Error { kind: ErrorKind::Timeout, msg: "context deadline exceeded".to_string() }
    }
    pub fn too_large() -> Error {
        Error { kind: ErrorKind::BodyTooLarge, msg: "body size limit exceeded".to_string() }
    }
    /// Prefixes the message ("prefix: msg"), keeping the kind (Go `fmt.Errorf("prefix: %w", err)`).
    pub fn wrap(self, prefix: &str) -> Error {
        Error { kind: self.kind, msg: format!("{prefix}: {}", self.msg) }
    }
    pub fn is_timeout(&self) -> bool {
        self.kind == ErrorKind::Timeout
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}
impl std::error::Error for Error {}

/// Go's text for an I/O error (the part after "read tcp a->b: ").
pub fn go_io_error(e: &std::io::Error) -> String {
    use std::io::ErrorKind as K;
    match e.kind() {
        K::ConnectionRefused => "connection refused".to_string(),
        K::ConnectionReset => "connection reset by peer".to_string(),
        K::BrokenPipe => "broken pipe".to_string(),
        K::TimedOut | K::WouldBlock => "i/o timeout".to_string(),
        K::UnexpectedEof => "unexpected EOF".to_string(),
        K::NotFound => "no such file or directory".to_string(),
        K::PermissionDenied => "permission denied".to_string(),
        K::HostUnreachable => "no route to host".to_string(),
        K::NetworkUnreachable => "network is unreachable".to_string(),
        K::AddrNotAvailable => "cannot assign requested address".to_string(),
        _ => {
            let s = e.to_string();
            // "Something (os error N)" -> "something"
            let s = match s.find(" (os error") {
                Some(i) => s[..i].to_string(),
                None => s,
            };
            let mut c = s.chars();
            match c.next() {
                Some(f) => f.to_lowercase().collect::<String>() + c.as_str(),
                None => s,
            }
        }
    }
}

/// Go `os.ReadFile` error text: "open <path>: no such file or directory".
pub fn read_file(path: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|e| format!("open {}: {}", path, go_io_error(&e)))
}
