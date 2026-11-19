//! The subset of the Docker Engine API client (`github.com/moby/moby/client` v0.6.2) that the Docker and Docker
//! Swarm discoverers use: host parsing, API version negotiation with `/_ping`, the versioned path prefix, the
//! `filters` query parameter, the list calls, and the error texts of `doRequest` and `checkResponseErr`.
//!
//! Transports: `unix://` hosts speak HTTP/1.1 over a Unix socket (a small client in this file); `http://` and
//! `https://` hosts use `net` with the SD config's HTTP client settings; every other scheme (`tcp://`, ...) speaks
//! plain HTTP over TCP with Go's default transport settings (proxy from the environment, no redirects).
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cancel::CancelToken;

/// Go `client.DefaultDockerHost` (unix).
pub const DEFAULT_DOCKER_HOST: &str = "unix:///var/run/docker.sock";
/// Go `client.DummyHost`: the Host header for unix sockets.
pub const DUMMY_HOST: &str = "api.moby.localhost";
/// Go `client.MaxAPIVersion`.
pub const MAX_API_VERSION: &str = "1.56";
/// Go `client.MinAPIVersion`.
pub const MIN_API_VERSION: &str = "1.40";
/// The version of the moby client module that upstream builds with (Go `mod.Version`).
const MODULE_VERSION: &str = "v0.6.2";
/// Go `maxUnixSocketPathSize` on Linux (`len(syscall.RawSockaddrUnix{}.Path)`).
const MAX_UNIX_SOCKET_PATH_SIZE: usize = 108;

/// Go `client.Filters`: term -> set of values. Kept sorted as Go's JSON encoding sorts map keys.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Filters(pub std::collections::BTreeMap<String, std::collections::BTreeSet<String>>);

impl Filters {
    /// Go `Filters.Add`.
    pub fn add(&mut self, term: &str, value: &str) {
        self.0.entry(term.to_string()).or_default().insert(value.to_string());
    }

    /// Go `json.Marshal(f)`: `{"term":{"value":true}}`.
    pub fn to_json(&self) -> String {
        let mut s = String::from("{");
        for (i, (term, values)) in self.0.iter().enumerate() {
            if i > 0 {
                s.push(',');
            }
            json::write_str(term, &mut s);
            s.push_str(":{");
            for (j, v) in values.iter().enumerate() {
                if j > 0 {
                    s.push(',');
                }
                json::write_str(v, &mut s);
                s.push_str(":true");
            }
            s.push('}');
        }
        s.push('}');
        s
    }

    /// Go `Filters.updateURLValues`.
    pub(crate) fn update_url_values(&self, query: &mut Vec<(String, String)>) {
        query.retain(|(k, _)| k != "filters");
        if !self.0.is_empty() {
            query.push(("filters".to_string(), self.to_json()));
        }
    }
}

/// Go `client.ParseHostURL`: (proto, addr, base path).
pub fn parse_host_url(host: &str) -> Result<(String, String, String), String> {
    let (proto, addr) = match host.split_once("://") {
        Some((p, a)) if !a.is_empty() => (p, a),
        _ => return Err(format!("unable to parse docker host `{host}`")),
    };
    if proto == "tcp" {
        let u = yaml::url::parse(&format!("tcp://{addr}"))?;
        return Ok((proto.to_string(), u.host, u.path));
    }
    Ok((proto.to_string(), addr.to_string(), String::new()))
}

/// Go `path.Join` for absolute API paths (clean: no empty, "." or ".." elements).
fn path_join(parts: &[&str]) -> String {
    let mut out: Vec<&str> = Vec::new();
    for p in parts {
        for seg in p.split('/') {
            match seg {
                "" | "." => {}
                ".." => {
                    out.pop();
                }
                s => out.push(s),
            }
        }
    }
    format!("/{}", out.join("/"))
}

/// Go `versions.compare`.
fn compare_versions(v1: &str, v2: &str) -> std::cmp::Ordering {
    if v1 == v2 {
        return std::cmp::Ordering::Equal;
    }
    let a: Vec<&str> = v1.split('.').collect();
    let b: Vec<&str> = v2.split('.').collect();
    for i in 0..a.len().max(b.len()) {
        let x: i64 = a.get(i).and_then(|s| s.parse().ok()).unwrap_or(0);
        let y: i64 = b.get(i).and_then(|s| s.parse().ok()).unwrap_or(0);
        if x != y {
            return x.cmp(&y);
        }
    }
    std::cmp::Ordering::Equal
}

fn version_less_than(v: &str, other: &str) -> bool {
    compare_versions(v, other) == std::cmp::Ordering::Less
}

/// Go `parseAPIVersion`: "major.minor" with leading zeros dropped.
fn parse_api_version(version: &str) -> Result<String, String> {
    let t = version.trim();
    if t.strip_prefix('v').unwrap_or(t).is_empty() {
        return Err("value is empty".to_string());
    }
    if version.starts_with('v') {
        return Err("must be formatted <major>.<minor>".to_string());
    }
    let (maj, min) = version.split_once('.').ok_or_else(|| "must be formatted <major>.<minor>".to_string())?;
    let maj: i64 = go_atoi(maj).ok_or_else(|| "invalid major version: must be formatted <major>.<minor>".to_string())?;
    let min: i64 = go_atoi(min).ok_or_else(|| "invalid minor version: must be formatted <major>.<minor>".to_string())?;
    Ok(format!("{maj}.{min}"))
}

/// Go `strconv.Atoi` (optional sign, decimal digits).
fn go_atoi(s: &str) -> Option<i64> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// The default User-Agent of the moby client (Go `userAgent()`).
pub fn default_user_agent() -> String {
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "x86" => "386",
        "powerpc64" => "ppc64",
        "loongarch64" => "loong64",
        a => a,
    };
    format!("moby-client/{MODULE_VERSION} {}/{arch}", std::env::consts::OS)
}

/// A response with its whole body (Go `*http.Response` after reading).
pub struct Response {
    pub status: u16,
    /// Go `resp.Status` ("404 Not Found").
    pub status_text: String,
    pub header: net::Header,
    pub body: Vec<u8>,
    /// Go `resp.Request.URL.String()` (the last request when redirects were followed).
    pub request_url: String,
    pub request_method: String,
}

/// How requests reach the daemon.
enum Transport {
    /// HTTP over a Unix socket at this path.
    Unix(String),
    /// `net` client (http/https hosts with the SD HTTP config, or plain TCP).
    Net(net::Client),
}

/// Go `client.Options` that the discoverers set.
pub struct ClientOptions {
    pub host: String,
    /// Some for http(s) hosts: the round tripper from the SD's HTTP client config (Go `WithHTTPClient`).
    pub http_client: Option<net::Client>,
    /// Go `WithScheme`.
    pub scheme: String,
    /// Go `WithHTTPHeaders`.
    pub http_headers: Vec<(String, String)>,
    /// Go `WithTimeout` (0 = none).
    pub timeout: Duration,
}

/// Go `client.Client` (the parts used here).
pub struct Client {
    host: String,
    proto: String,
    addr: String,
    base_path: String,
    scheme: String,
    version: String,
    negotiated: bool,
    custom_headers: Vec<(String, String)>,
    timeout: Option<Duration>,
    /// Go's default transport asks for gzip and decodes it (not the Prometheus round tripper, nor unix).
    decompress: bool,
    transport: Transport,
}

impl Client {
    /// Go `client.New(WithHost(host), [WithHTTPClient, WithScheme, WithHTTPHeaders], WithTimeout(t))`.
    pub fn new(opts: ClientOptions) -> Result<Client, String> {
        let (proto, addr, base_path) = parse_host_url(&opts.host)?;
        // sockets.ConfigureTransport
        if proto == "unix" && addr.len() > MAX_UNIX_SOCKET_PATH_SIZE {
            return Err(format!("unix socket path {} is too long", yaml::go_quote(&addr)));
        }
        if proto == "npipe" {
            return Err("protocol not available".to_string());
        }
        let decompress = opts.http_client.is_none() && proto != "unix";
        let transport = match opts.http_client {
            Some(c) => Transport::Net(c),
            None if proto == "unix" => Transport::Unix(addr.clone()),
            None => {
                // Go's default client: proxy from the environment, redirects not followed.
                let proxy = net::proxy::from_config(&net::ProxyConfig { proxy_from_environment: true, ..Default::default() });
                let t = net::Transport::new(
                    Default::default(),
                    proxy,
                    net::Header::new(),
                    net::TransportOptions { idle_conn_timeout: Duration::from_secs(30), max_idle_per_host: 6, ..Default::default() },
                )?;
                let mut c = net::Client::new(Arc::new(t));
                c.follow_redirects = false;
                Transport::Net(c)
            }
        };
        let mut headers: Vec<(String, String)> = Vec::new();
        for (k, v) in &opts.http_headers {
            let k = net::types::canonical_header_key(k);
            if headers.iter().any(|(n, _)| *n == k) {
                return Err(format!("duplicate custom HTTP header ({k})"));
            }
            headers.push((k, v.clone()));
        }
        let scheme = if opts.scheme.is_empty() { "http".to_string() } else { opts.scheme };
        Ok(Client {
            host: opts.host,
            proto,
            addr,
            base_path,
            scheme,
            version: MAX_API_VERSION.to_string(),
            negotiated: false,
            custom_headers: headers,
            timeout: if opts.timeout.is_zero() { None } else { Some(opts.timeout) },
            decompress,
            transport,
        })
    }

    /// Go `ClientVersion`.
    pub fn client_version(&self) -> &str {
        &self.version
    }

    /// Go `checkVersion`: negotiates once (errors are dropped by the callers, as in Go).
    fn check_version(&mut self, ctx: &CancelToken) -> Result<(), String> {
        if self.negotiated {
            return Ok(());
        }
        self.ping_negotiate(ctx).map(|_| ())
    }

    /// Go `Ping(ctx, PingOptions{NegotiateAPIVersion: true})`; returns the daemon's API version header.
    pub fn ping_negotiate(&mut self, ctx: &CancelToken) -> Result<String, String> {
        if self.negotiated {
            return self.ping(ctx);
        }
        let v = self.ping(ctx)?;
        if v.is_empty() {
            self.set_api_version(MAX_API_VERSION);
            return Ok(v);
        }
        self.negotiate_api_version(&v)?;
        Ok(v)
    }

    /// Go `ping`: HEAD /_ping, then GET /_ping when HEAD did not answer 200.
    fn ping(&mut self, ctx: &CancelToken) -> Result<String, String> {
        let p = path_join(&[&self.base_path, "/_ping"]);
        if let Ok(r) = self.do_request(ctx, "HEAD", &p)
            && r.status == 200
        {
            return Ok(r.header.get_str("Api-Version").to_string());
        }
        let r = self.do_request(ctx, "GET", &p)?;
        check_response_err(&r)?;
        Ok(r.header.get_str("Api-Version").to_string())
    }

    /// Go `negotiateAPIVersion`.
    fn negotiate_api_version(&mut self, ping_version: &str) -> Result<(), String> {
        let pv = parse_api_version(ping_version)?;
        if version_less_than(&pv, MIN_API_VERSION) {
            return Err(format!("API version {pv} is not supported by this client: the minimum supported API version is {MIN_API_VERSION}"));
        }
        let mut negotiated = if self.version.is_empty() { MAX_API_VERSION.to_string() } else { self.version.clone() };
        if version_less_than(&pv, &negotiated) {
            negotiated = pv;
        }
        self.set_api_version(&negotiated);
        Ok(())
    }

    fn set_api_version(&mut self, v: &str) {
        self.version = v.to_string();
        self.negotiated = true;
    }

    /// Go `getAPIPath`: the versioned path with the encoded query.
    fn get_api_path(&mut self, ctx: &CancelToken, p: &str, query: &[(String, String)]) -> String {
        let _ = self.check_version(ctx);
        let path = if !self.version.is_empty() {
            let v = format!("/v{}", self.version.trim_start_matches('v'));
            path_join(&[&self.base_path, &v, p])
        } else {
            path_join(&[&self.base_path, p])
        };
        let u = yaml::url::GoUrl { path, raw_query: yaml::url::encode_query(query), ..Default::default() };
        u.to_url_string()
    }

    /// Go `get`: GET of an API path; the response must not be an error status.
    pub fn get(&mut self, ctx: &CancelToken, p: &str, query: &[(String, String)]) -> Result<Response, String> {
        let path = self.get_api_path(ctx, p, query);
        let r = self.do_request(ctx, "GET", &path)?;
        check_response_err(&r)?;
        Ok(r)
    }

    /// The request URL as Go prints it (`req.URL.String()`).
    fn request_url(&self, path_and_query: &str) -> String {
        let (p, q) = path_and_query.split_once('?').unwrap_or((path_and_query, ""));
        let u = yaml::url::GoUrl { scheme: self.scheme.clone(), host: self.addr.clone(), path: p.to_string(), raw_query: q.to_string(), ..Default::default() };
        u.to_url_string()
    }

    fn headers(&self) -> net::Header {
        let mut h = net::Header::new();
        for (k, v) in &self.custom_headers {
            h.set(k, v);
        }
        if h.get_str("User-Agent").is_empty() {
            h.set("User-Agent", &default_user_agent());
        }
        h
    }

    /// Go `buildRequest` + `doRequest`: sends the request and maps transport errors to the client's texts.
    fn do_request(&mut self, ctx: &CancelToken, method: &str, path_and_query: &str) -> Result<Response, String> {
        let url = self.request_url(path_and_query);
        let op = go_op(method);
        if ctx.is_cancelled() {
            return Err(format!("{op} {}: context canceled", yaml::go_quote(&url)));
        }
        let header = self.headers();
        let res =
            match &self.transport {
                Transport::Unix(sock) => unix_round_trip(sock, method, path_and_query, &header, self.timeout).map(|(status, status_text, header, body)| {
                    Response { status, status_text, header, body, request_url: url.clone(), request_method: method.to_string() }
                }),
                Transport::Net(c) => {
                    let mut req = net::Request::new(method, &url, Vec::new()).map_err(|e| e.to_string())?;
                    req.header = header;
                    req.timeout = self.timeout;
                    req.decompress = self.decompress;
                    match c.do_request(req) {
                        Ok(r) => {
                            if method == "HEAD" && (300..400).contains(&r.status) && !c.follow_redirects && !r.header.get_str("Location").is_empty() {
                                // Go CheckRedirect: a redirect of a non-GET request is an error.
                                Err(ConnErr { msg: format!("{op} {}: unexpected redirect in response", yaml::go_quote(&url)), kind: ConnErrKind::Other })
                            } else {
                                Ok(Response {
                                    status: r.status,
                                    status_text: r.status_text,
                                    header: r.header,
                                    body: r.body,
                                    request_url: r.request.url.to_url_string(),
                                    request_method: r.request.method,
                                })
                            }
                        }
                        Err(e) => Err(ConnErr::from_net(e)),
                    }
                }
            };
        res.map_err(|e| self.connection_error(e))
    }

    /// The error mapping of Go `doRequest`.
    fn connection_error(&self, e: ConnErr) -> String {
        let s = &e.msg;
        if self.scheme != "https" && s.contains("malformed HTTP response") {
            return format!("{s}.\n* Are you trying to connect to a TLS-enabled daemon without TLS?");
        }
        if self.scheme == "https" && (s.contains("handshake failure") || s.contains("bad certificate")) {
            return format!("the server probably has client authentication (--tlsverify) enabled; check your TLS client certification settings: {s}");
        }
        match e.kind {
            ConnErrKind::Timeout => return format!("{s} (Client.Timeout exceeded while awaiting headers)"),
            ConnErrKind::Permission => return format!("permission denied while trying to connect to the docker API at {}", self.host),
            ConnErrKind::NotExist(ref inner) => {
                return format!("failed to connect to the docker API at {}; check if the path is correct and if the daemon is running: {inner}", self.host);
            }
            ConnErrKind::Dial => return connection_failed(&self.host),
            ConnErrKind::Other => {}
        }
        if let Some(i) = s.find("lookup ")
            && (s.contains("no such host") || s.contains("server misbehaving"))
        {
            return format!("failed to connect to the docker API at {}: {}", self.host, &s[i..]);
        }
        if s.contains("connection refused") || s.contains("dial unix") || s.contains("i/o timeout") {
            return connection_failed(&self.host);
        }
        format!("error during connect: {s}")
    }

    /// The protocol of the host ("unix", "http", "tcp", ...).
    pub fn proto(&self) -> &str {
        &self.proto
    }
}

/// Go `connectionFailed`.
fn connection_failed(host: &str) -> String {
    if host.is_empty() {
        "Cannot connect to the Docker daemon. Is the docker daemon running on this host?".to_string()
    } else {
        format!("Cannot connect to the Docker daemon at {host}. Is the docker daemon running?")
    }
}

/// Go's `url.Error` op for a method ("Get", "Head").
fn go_op(method: &str) -> String {
    let m = method.to_ascii_lowercase();
    let mut c = m.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ConnErrKind {
    /// The client timeout passed (Go `context.DeadlineExceeded`).
    Timeout,
    /// `os.ErrPermission` while dialing.
    Permission,
    /// `os.ErrNotExist` while dialing; the unwrapped dial error text.
    NotExist(String),
    /// Another dial error ("dial unix ...").
    Dial,
    Other,
}

/// A transport error: Go's `url.Error` text and what kind of error it wraps.
struct ConnErr {
    msg: String,
    kind: ConnErrKind,
}

impl ConnErr {
    fn from_net(e: net::Error) -> ConnErr {
        let kind = if e.is_timeout() { ConnErrKind::Timeout } else { ConnErrKind::Other };
        ConnErr { msg: e.msg, kind }
    }
}

/// Reads a CRLF line (without the line end).
fn read_line<R: BufRead>(r: &mut R) -> std::io::Result<String> {
    let mut b = Vec::new();
    let n = r.read_until(b'\n', &mut b)?;
    if n == 0 {
        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "unexpected EOF"));
    }
    while b.last().is_some_and(|c| *c == b'\n' || *c == b'\r') {
        b.pop();
    }
    Ok(String::from_utf8_lossy(&b).into_owned())
}

/// One HTTP/1.1 exchange over a Unix socket (Go's transport with `DialContext` to the socket). Returns
/// (status, status text, headers, body). The connection is not reused.
fn unix_round_trip(
    sock: &str,
    method: &str,
    path_and_query: &str,
    header: &net::Header,
    timeout: Option<Duration>,
) -> Result<(u16, String, net::Header, Vec<u8>), ConnErr> {
    let url = yaml::url::GoUrl {
        scheme: "http".into(),
        host: sock.to_string(),
        path: path_and_query.split('?').next().unwrap_or("").to_string(),
        raw_query: path_and_query.split_once('?').map(|(_, q)| q.to_string()).unwrap_or_default(),
        ..Default::default()
    };
    let url_err = |kind: ConnErrKind, reason: &str| ConnErr { msg: format!("{} {}: {}", go_op(method), yaml::go_quote(&url.to_url_string()), reason), kind };
    let deadline = timeout.map(|t| Instant::now() + t);
    let mut s = match UnixStream::connect(sock) {
        Ok(s) => s,
        Err(e) => {
            let reason = format!("dial unix {sock}: connect: {}", net::types::go_io_error(&e));
            let kind = match e.kind() {
                std::io::ErrorKind::NotFound => ConnErrKind::NotExist(reason.clone()),
                std::io::ErrorKind::PermissionDenied => ConnErrKind::Permission,
                _ => ConnErrKind::Dial,
            };
            return Err(url_err(kind, &reason));
        }
    };
    let left = |d: Option<Instant>| -> Result<Option<Duration>, ()> {
        match d {
            None => Ok(None),
            Some(d) => {
                let now = Instant::now();
                if now >= d { Err(()) } else { Ok(Some(d - now)) }
            }
        }
    };
    let timeout_err = || url_err(ConnErrKind::Timeout, "context deadline exceeded");
    let mut req = format!("{method} {path_and_query} HTTP/1.1\r\nHost: {DUMMY_HOST}\r\n");
    for (k, v) in header.iter() {
        req.push_str(k);
        req.push_str(": ");
        req.push_str(v);
        req.push_str("\r\n");
    }
    req.push_str("\r\n");
    let t = left(deadline).map_err(|_| timeout_err())?;
    let _ = s.set_write_timeout(t);
    if let Err(e) = s.write_all(req.as_bytes()) {
        if matches!(e.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock) {
            return Err(timeout_err());
        }
        return Err(url_err(ConnErrKind::Other, &format!("write unix ->{sock}: {}", net::types::go_io_error(&e))));
    }
    // A reader that re-arms the read timeout with the time left before the deadline.
    struct Timed {
        s: UnixStream,
        deadline: Option<Instant>,
    }
    impl Read for Timed {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if let Some(d) = self.deadline {
                let now = Instant::now();
                if now >= d {
                    return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "i/o timeout"));
                }
                self.s.set_read_timeout(Some(d - now))?;
            }
            self.s.read(buf)
        }
    }
    let mut r = BufReader::new(Timed { s, deadline });
    let io_err = |e: std::io::Error| -> ConnErr {
        match e.kind() {
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => timeout_err(),
            std::io::ErrorKind::UnexpectedEof => url_err(ConnErrKind::Other, "EOF"),
            _ => url_err(ConnErrKind::Other, &format!("read unix ->{sock}: {}", net::types::go_io_error(&e))),
        }
    };
    let status_line = read_line(&mut r).map_err(io_err)?;
    let mut parts = status_line.splitn(3, ' ');
    let proto = parts.next().unwrap_or("");
    let code = parts.next().unwrap_or("");
    let status: u16 = match (proto.starts_with("HTTP/"), code.len() == 3, code.parse::<u16>()) {
        (true, true, Ok(c)) => c,
        _ => {
            return Err(url_err(
                ConnErrKind::Other,
                &format!("net/http: HTTP/1.x transport connection broken: malformed HTTP response {}", yaml::go_quote(&status_line)),
            ));
        }
    };
    let status_text = status_line.split_once(' ').map(|(_, s)| s.to_string()).unwrap_or_default();
    let mut h = net::Header::new();
    loop {
        let line = read_line(&mut r).map_err(io_err)?;
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            h.add(k.trim(), v.trim());
        }
    }
    let mut body = Vec::new();
    let no_body = method == "HEAD" || status / 100 == 1 || status == 204 || status == 304;
    if !no_body {
        if h.get_str("Transfer-Encoding").eq_ignore_ascii_case("chunked") {
            loop {
                let sz = read_line(&mut r).map_err(io_err)?;
                let n = usize::from_str_radix(sz.split(';').next().unwrap_or("").trim(), 16)
                    .map_err(|_| url_err(ConnErrKind::Other, "malformed chunked encoding"))?;
                if n == 0 {
                    // trailers
                    while !read_line(&mut r).map_err(io_err)?.is_empty() {}
                    break;
                }
                let start = body.len();
                body.resize(start + n, 0);
                r.read_exact(&mut body[start..]).map_err(io_err)?;
                let mut crlf = [0u8; 2];
                r.read_exact(&mut crlf).map_err(io_err)?;
            }
        } else if let Some(n) = h.get("Content-Length").and_then(|v| v.trim().parse::<usize>().ok()) {
            body.resize(n, 0);
            r.read_exact(&mut body).map_err(io_err)?;
        } else {
            r.read_to_end(&mut body).map_err(io_err)?;
        }
    }
    Ok((status, status_text, h, body))
}

/// Go `checkResponseErr`.
pub fn check_response_err(resp: &Response) -> Result<(), String> {
    if (200..400).contains(&resp.status) {
        return Ok(());
    }
    let status_msg = if resp.status_text.is_empty() { net::transport::status_text(resp.status).to_string() } else { resp.status_text.clone() };
    const BODY_MAX: usize = 1024 * 1024;
    let body: &[u8] = if resp.request_method != "HEAD" { &resp.body } else { &[] };
    if body.len() >= BODY_MAX {
        if !resp.request_url.is_empty() {
            return Err(format!(
                "request returned {status_msg} with a message (> {BODY_MAX} bytes) for API route and version {}, check if the server supports the requested API version",
                resp.request_url
            ));
        }
        return Err(format!("request returned {status_msg} with a message (> {BODY_MAX} bytes); check if the server supports the requested API version"));
    }
    if body.is_empty() {
        if !resp.request_url.is_empty() {
            return Err(format!(
                "request returned {status_msg} for API route and version {}, check if the server supports the requested API version",
                resp.request_url
            ));
        }
        return Err(format!("request returned {status_msg}; check if the server supports the requested API version"));
    }
    let daemon_err = if resp.header.get_str("Content-Type") == "application/json" {
        let v = json::Json::parse_bytes(body).map_err(|e| format!("error reading JSON: {}", super::types::go_json_error(&e)))?;
        let msg = match &v {
            json::Json::Obj(_) => match super::types::get(&v, "message") {
                Some(json::Json::Str(s)) => s.clone(),
                Some(json::Json::Null) | None => String::new(),
                Some(other) => {
                    return Err(format!(
                        "error reading JSON: json: cannot unmarshal {} into Go struct field ErrorResponse.message of type string",
                        super::types::json_kind(other)
                    ));
                }
            },
            json::Json::Null => String::new(),
            other => {
                return Err(format!("error reading JSON: json: cannot unmarshal {} into Go value of type common.ErrorResponse", super::types::json_kind(other)));
            }
        };
        if msg.is_empty() {
            format!("API returned a {} ({}) but provided no error-message", resp.status, net::transport::status_text(resp.status))
        } else {
            msg.trim().to_string()
        }
    } else {
        String::from_utf8_lossy(body).trim().to_string()
    };
    Err(format!("Error response from daemon: {daemon_err}"))
}
