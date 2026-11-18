//! The HTTP/1.1 transport (Go `http.Transport`): plain and TLS connections, a pool of idle keep-alive
//! connections, HTTP proxies (absolute-form requests for http, CONNECT tunnels for https), deadlines, optional
//! gzip/deflate decoding and body size limits. HTTP/2 is not spoken: `enable_http2` only changes the config.
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use yaml::url::GoUrl;

use crate::proxy::ProxyFunc;
use crate::roundtrip::RoundTripper;
use crate::tls::{TlsClientConfig, io_error_text};
use crate::types::{Error, Header, Request, Response, go_io_error};

/// Custom dialer (Go `WithDialContextFunc`): network ("tcp") and "host:port"; the deadline, if any.
pub type DialFunc = Arc<dyn Fn(&str, &str, Option<Instant>) -> std::io::Result<TcpStream> + Send + Sync>;

pub enum Conn {
    Plain(TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Conn {
    fn tcp(&self) -> &TcpStream {
        match self {
            Conn::Plain(s) => s,
            Conn::Tls(s) => &s.sock,
        }
    }
    fn set_deadline(&self, deadline: Option<Instant>) -> Result<(), Error> {
        let left = match deadline {
            None => None,
            Some(d) => {
                let now = Instant::now();
                if now >= d {
                    return Err(Error::timeout());
                }
                Some(d - now)
            }
        };
        let _ = self.tcp().set_read_timeout(left);
        let _ = self.tcp().set_write_timeout(left);
        Ok(())
    }
}

impl Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Conn::Plain(s) => s.read(buf),
            Conn::Tls(s) => match s.read(buf) {
                // a peer closing without close_notify ends the stream like Go does
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(0),
                r => r,
            },
        }
    }
}
impl Write for Conn {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Conn::Plain(s) => s.write(buf),
            Conn::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Conn::Plain(s) => s.flush(),
            Conn::Tls(s) => s.flush(),
        }
    }
}

struct Idle {
    key: String,
    conn: Conn,
    since: Instant,
}

#[derive(Clone)]
pub struct TransportOptions {
    pub keep_alives: bool,
    pub idle_conn_timeout: Duration,
    pub max_idle_per_host: usize,
    pub dial: Option<DialFunc>,
}

impl Default for TransportOptions {
    fn default() -> Self {
        TransportOptions { keep_alives: true, idle_conn_timeout: Duration::from_secs(300), max_idle_per_host: 1000, dial: None }
    }
}

pub struct Transport {
    pub tls: Arc<TlsClientConfig>,
    rustls: Arc<rustls::ClientConfig>,
    pub proxy: Option<ProxyFunc>,
    pub proxy_connect_header: Header,
    pub opts: TransportOptions,
    idle: Mutex<Vec<Idle>>,
}

fn timeout_or(e: &std::io::Error, f: impl FnOnce() -> String) -> Error {
    match e.kind() {
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => Error::timeout(),
        _ => Error::other(f()),
    }
}

fn default_port(scheme: &str) -> &'static str {
    if scheme == "https" { "443" } else { "80" }
}

fn host_port(u: &GoUrl) -> String {
    let port = if u.port().is_empty() { default_port(&u.scheme) } else { u.port() };
    let h = u.hostname();
    if h.contains(':') { format!("[{h}]:{port}") } else { format!("{h}:{port}") }
}

/// Go `http.StatusText`.
pub fn status_text(code: u16) -> &'static str {
    match code {
        100 => "Continue",
        101 => "Switching Protocols",
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        206 => "Partial Content",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        407 => "Proxy Authentication Required",
        408 => "Request Timeout",
        409 => "Conflict",
        410 => "Gone",
        413 => "Request Entity Too Large",
        415 => "Unsupported Media Type",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "",
    }
}

impl Transport {
    pub fn new(tls: TlsClientConfig, proxy: Option<ProxyFunc>, proxy_connect_header: Header, opts: TransportOptions) -> Result<Transport, String> {
        let rustls = tls.to_rustls()?;
        Ok(Transport { tls: Arc::new(tls), rustls, proxy, proxy_connect_header, opts, idle: Mutex::new(Vec::new()) })
    }

    /// A transport with default TLS (system roots) and no proxy.
    pub fn default_transport() -> Transport {
        Transport::new(TlsClientConfig::default(), None, Header::new(), TransportOptions::default()).expect("default TLS config")
    }

    fn take_idle(&self, key: &str) -> Option<Conn> {
        let mut v = self.idle.lock().unwrap_or_else(|p| p.into_inner());
        let ttl = self.opts.idle_conn_timeout;
        if !ttl.is_zero() {
            v.retain(|i| i.since.elapsed() < ttl);
        }
        let pos = v.iter().rposition(|i| i.key == key)?;
        Some(v.swap_remove(pos).conn)
    }

    fn put_idle(&self, key: String, conn: Conn) {
        if !self.opts.keep_alives {
            return;
        }
        let mut v = self.idle.lock().unwrap_or_else(|p| p.into_inner());
        let n = v.iter().filter(|i| i.key == key).count();
        if n >= self.opts.max_idle_per_host {
            return;
        }
        v.push(Idle { key, conn, since: Instant::now() });
    }

    fn dial(&self, addr: &str, deadline: Option<Instant>) -> Result<TcpStream, Error> {
        if let Some(d) = &self.opts.dial {
            return d("tcp", addr, deadline).map_err(|e| timeout_or(&e, || format!("dial tcp {addr}: {}", go_io_error(&e))));
        }
        let addrs: Vec<_> = match addr.to_socket_addrs() {
            Ok(a) => a.collect(),
            Err(_) => {
                let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);
                return Err(Error::other(format!("dial tcp: lookup {}: no such host", host.trim_start_matches('[').trim_end_matches(']'))));
            }
        };
        let mut last = Error::other(format!("dial tcp {addr}: no address"));
        for a in addrs {
            let r = match deadline {
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        return Err(Error::timeout());
                    }
                    TcpStream::connect_timeout(&a, d - now)
                }
                None => TcpStream::connect(a),
            };
            match r {
                Ok(s) => {
                    let _ = s.set_nodelay(true);
                    return Ok(s);
                }
                Err(e) => last = timeout_or(&e, || format!("dial tcp {a}: connect: {}", go_io_error(&e))),
            }
        }
        Err(last)
    }

    /// A new connection to the target (through the proxy when there is one); TLS for https.
    fn connect(&self, u: &GoUrl, proxy: Option<&GoUrl>, deadline: Option<Instant>) -> Result<Conn, Error> {
        let target = host_port(u);
        let tcp = match proxy {
            None => self.dial(&target, deadline)?,
            Some(p) => {
                if p.scheme != "http" && !p.scheme.is_empty() {
                    return Err(Error::other(format!("proxyconnect tcp: unsupported proxy scheme {}", yaml::go_quote(&p.scheme))));
                }
                let paddr = host_port(p);
                let tcp = self.dial(&paddr, deadline).map_err(|e| if e.is_timeout() { e } else { e.wrap("proxyconnect tcp") })?;
                if u.scheme == "https" {
                    self.connect_tunnel(tcp, p, &target, deadline)?
                } else {
                    tcp
                }
            }
        };
        if u.scheme != "https" {
            return Ok(Conn::Plain(tcp));
        }
        let name = self.tls.server_name_for(u.hostname()).map_err(Error::other)?;
        let cc = rustls::ClientConnection::new(self.rustls.clone(), name).map_err(|e| Error::other(crate::tls::tls_error_text(&e)))?;
        let conn = Conn::Tls(Box::new(rustls::StreamOwned::new(cc, tcp)));
        conn.set_deadline(deadline)?;
        Ok(conn)
    }

    fn connect_tunnel(&self, mut tcp: TcpStream, p: &GoUrl, target: &str, deadline: Option<Instant>) -> Result<TcpStream, Error> {
        let c = Conn::Plain(tcp.try_clone().map_err(|e| Error::other(go_io_error(&e)))?);
        c.set_deadline(deadline)?;
        let mut h = format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n");
        if let Some(ui) = &p.user {
            let v = crypto::base64(format!("{}:{}", ui.username, ui.password).as_bytes());
            h.push_str(&format!("Proxy-Authorization: Basic {v}\r\n"));
        }
        for (k, v) in self.proxy_connect_header.iter() {
            h.push_str(&format!("{k}: {v}\r\n"));
        }
        h.push_str("\r\n");
        tcp.write_all(h.as_bytes()).map_err(|e| timeout_or(&e, || format!("proxyconnect tcp: {}", go_io_error(&e))))?;
        // read the response head byte by byte so no tunneled bytes are buffered away
        let mut head = Vec::new();
        let mut b = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            let n = tcp.read(&mut b).map_err(|e| timeout_or(&e, || format!("proxyconnect tcp: {}", go_io_error(&e))))?;
            if n == 0 {
                return Err(Error::other("proxyconnect tcp: unexpected EOF"));
            }
            head.push(b[0]);
            if head.len() > 64 * 1024 {
                return Err(Error::other("proxyconnect tcp: response header too long"));
            }
        }
        let text = String::from_utf8_lossy(&head);
        let line = text.lines().next().unwrap_or("");
        let mut parts = line.splitn(3, ' ');
        let _ = parts.next();
        let code: u16 = parts.next().and_then(|c| c.parse().ok()).unwrap_or(0);
        if code != 200 {
            let rest = parts.next().unwrap_or("");
            return Err(Error::other(format!("{code} {rest}").trim_end().to_string()));
        }
        Ok(tcp)
    }

    fn write_request(&self, conn: &mut Conn, req: &Request, proxy: Option<&GoUrl>) -> std::io::Result<()> {
        let u = &req.url;
        let mut h = String::with_capacity(256);
        h.push_str(&req.method);
        h.push(' ');
        if proxy.is_some() && u.scheme == "http" {
            let mut abs = u.clone();
            abs.fragment.clear();
            abs.raw_fragment.clear();
            abs.user = None;
            h.push_str(&abs.to_url_string());
        } else {
            h.push_str(&u.request_uri());
        }
        h.push_str(" HTTP/1.1\r\nHost: ");
        let host = if req.host.is_empty() { u.host.as_str() } else { req.host.as_str() };
        h.push_str(host);
        h.push_str("\r\n");
        let ua = req.header.get("User-Agent");
        match ua {
            Some(v) => {
                h.push_str("User-Agent: ");
                h.push_str(v);
                h.push_str("\r\n");
            }
            None => h.push_str("User-Agent: Go-http-client/1.1\r\n"),
        }
        if !req.body.is_empty() || matches!(req.method.as_str(), "POST" | "PUT" | "PATCH") {
            h.push_str(&format!("Content-Length: {}\r\n", req.body.len()));
        }
        if let (Some(p), "http") = (proxy, u.scheme.as_str())
            && let Some(ui) = &p.user
                && !req.header.has("Proxy-Authorization") {
                    let v = crypto::base64(format!("{}:{}", ui.username, ui.password).as_bytes());
                    h.push_str(&format!("Proxy-Authorization: Basic {v}\r\n"));
                }
        for (k, v) in req.header.iter() {
            if k.eq_ignore_ascii_case("host") || k.eq_ignore_ascii_case("user-agent") || k.eq_ignore_ascii_case("content-length") || k.eq_ignore_ascii_case("connection") || k.eq_ignore_ascii_case("transfer-encoding") {
                continue;
            }
            h.push_str(k);
            h.push_str(": ");
            h.push_str(v);
            h.push_str("\r\n");
        }
        if req.decompress && !req.header.has("Accept-Encoding") && req.method != "HEAD" {
            h.push_str("Accept-Encoding: gzip\r\n");
        }
        if !self.opts.keep_alives {
            h.push_str("Connection: close\r\n");
        }
        h.push_str("\r\n");
        let mut all = h.into_bytes();
        all.extend_from_slice(&req.body);
        conn.write_all(&all)?;
        conn.flush()
    }

    /// One request on one connection. Returns the response and the connection when it can be reused, or
    /// (error, whether any response byte arrived).
    fn exchange(&self, mut conn: Conn, req: &Request, proxy: Option<&GoUrl>, deadline: Option<Instant>) -> Result<(Response, Option<Conn>), (Error, bool)> {
        conn.set_deadline(deadline).map_err(|e| (e, false))?;
        let io = |e: std::io::Error| timeout_or(&e, || io_error_text(&e));
        self.write_request(&mut conn, req, proxy).map_err(|e| (io(e), false))?;
        let mut r = BufReader::with_capacity(16 * 1024, conn);
        let mut line = String::new();
        let mut got_any = false;
        loop {
            line.clear();
            let n = r.read_line(&mut line).map_err(|e| (io(e), got_any))?;
            if n == 0 {
                return Err((Error::other("EOF"), got_any));
            }
            got_any = true;
            let (status, http10) = parse_status(&line).map_err(|e| (e, true))?;
            let mut header = Header::new();
            loop {
                line.clear();
                let n = r.read_line(&mut line).map_err(|e| (io(e), true))?;
                if n == 0 {
                    return Err((Error::other("unexpected EOF"), true));
                }
                let t = line.trim_end_matches(['\r', '\n']);
                if t.is_empty() {
                    break;
                }
                if let Some((k, v)) = t.split_once(':') {
                    header.add(k.trim(), v.trim());
                }
            }
            if (100..200).contains(&status) && status != 101 {
                continue;
            }
            let conn_hdr = header.get_str("Connection").to_ascii_lowercase();
            let mut reusable = self.opts.keep_alives && !conn_hdr.contains("close") && (!http10 || conn_hdr.contains("keep-alive"));
            let chunked = header.get_str("Transfer-Encoding").to_ascii_lowercase().contains("chunked");
            let clen = header.get("Content-Length").and_then(|v| v.trim().parse::<usize>().ok());
            let limit = req.max_body_bytes.unwrap_or(usize::MAX);
            let mut body = Vec::new();
            if req.method == "HEAD" || status == 204 || status == 304 {
            } else if chunked {
                body = read_chunked(&mut r, limit).map_err(|e| (e, true))?;
            } else if let Some(n) = clen {
                if n > limit {
                    return Err((Error::too_large(), true));
                }
                body = vec![0u8; n];
                r.read_exact(&mut body).map_err(|e| (if e.kind() == std::io::ErrorKind::UnexpectedEof { Error::other("unexpected EOF") } else { io(e) }, true))?;
            } else {
                let mut buf = [0u8; 16 * 1024];
                loop {
                    let n = r.read(&mut buf).map_err(|e| (io(e), true))?;
                    if n == 0 {
                        break;
                    }
                    if body.len() + n > limit {
                        return Err((Error::too_large(), true));
                    }
                    body.extend_from_slice(&buf[..n]);
                }
                reusable = false;
            }
            if req.decompress && req.header.get("Accept-Encoding").is_none() {
                let enc = header.get_str("Content-Encoding").to_ascii_lowercase();
                if enc == "gzip" || enc == "deflate" {
                    body = decode_body(&enc, &body, limit).map_err(|e| (e, true))?;
                    header.del("Content-Encoding");
                    header.del("Content-Length");
                }
            }
            if !r.buffer().is_empty() {
                reusable = false;
            }
            let mut sent = req.clone();
            sent.body = Vec::new();
            let resp = Response { status, status_text: format!("{} {}", status, status_text(status)).trim_end().to_string(), header, body, request: sent };
            let keep = if reusable { Some(r.into_inner()) } else { None };
            return Ok((resp, keep));
        }
    }
}

/// Decodes a gzip or deflate body (zlib-wrapped, or raw deflate as some servers send) with a size limit.
pub fn decode_body(enc: &str, body: &[u8], limit: usize) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    let read_limited = |rd: &mut dyn Read, out: &mut Vec<u8>| -> Result<(), Error> {
        let mut buf = [0u8; 16 * 1024];
        loop {
            let n = rd.read(&mut buf).map_err(|e| Error::other(format!("{enc}: {e}")))?;
            if n == 0 {
                return Ok(());
            }
            if out.len() + n > limit {
                return Err(Error::too_large());
            }
            out.extend_from_slice(&buf[..n]);
        }
    };
    if enc == "gzip" {
        read_limited(&mut flate2::read::MultiGzDecoder::new(body), &mut out)?;
    } else if read_limited(&mut flate2::read::ZlibDecoder::new(body), &mut out).is_err() {
        out.clear();
        read_limited(&mut flate2::read::DeflateDecoder::new(body), &mut out)?;
    }
    Ok(out)
}

fn parse_status(line: &str) -> Result<(u16, bool), Error> {
    let t = line.trim_end_matches(['\r', '\n']);
    let mut it = t.splitn(3, ' ');
    let v = it.next().unwrap_or("");
    if !v.starts_with("HTTP/1.") {
        return Err(Error::other(format!("net/http: HTTP/1.x transport connection broken: malformed HTTP response {}", yaml::go_quote(t))));
    }
    match it.next().unwrap_or("").parse::<u16>() {
        Ok(s) => Ok((s, v == "HTTP/1.0")),
        Err(_) => Err(Error::other(format!("malformed HTTP status code {}", yaml::go_quote(t)))),
    }
}

fn read_chunked<R: BufRead>(r: &mut R, max: usize) -> Result<Vec<u8>, Error> {
    let io = |e: std::io::Error| timeout_or(&e, || io_error_text(&e));
    let mut body = Vec::new();
    let mut line = String::new();
    loop {
        line.clear();
        if r.read_line(&mut line).map_err(io)? == 0 {
            return Err(Error::other("unexpected EOF"));
        }
        let hex = line.trim().split(';').next().unwrap_or("").trim();
        let n = usize::from_str_radix(hex, 16).map_err(|_| Error::other("invalid byte in chunk length"))?;
        if n == 0 {
            loop {
                line.clear();
                let k = r.read_line(&mut line).map_err(io)?;
                if k == 0 || line.trim().is_empty() {
                    return Ok(body);
                }
            }
        }
        if body.len() + n > max {
            return Err(Error::too_large());
        }
        let st = body.len();
        body.resize(st + n, 0);
        r.read_exact(&mut body[st..]).map_err(io)?;
        let mut crlf = [0u8; 2];
        r.read_exact(&mut crlf).map_err(io)?;
    }
}

impl RoundTripper for Transport {
    fn round_trip(&self, req: Request) -> Result<Response, Error> {
        let u = &req.url;
        if u.scheme != "http" && u.scheme != "https" {
            return Err(Error::other(format!("unsupported protocol scheme {}", yaml::go_quote(&u.scheme))));
        }
        if u.host.is_empty() {
            return Err(Error::other("http: no Host in request URL"));
        }
        let deadline = req.timeout.map(|t| Instant::now() + t);
        let proxy = match &self.proxy {
            Some(f) => f(u).map_err(Error::other)?,
            None => None,
        };
        let key = format!("{}|{}|{}", u.scheme, host_port(u), proxy.as_ref().map(|p| p.to_url_string()).unwrap_or_default());
        if let Some(conn) = self.take_idle(&key) {
            match self.exchange(conn, &req, proxy.as_ref(), deadline) {
                Ok((resp, keep)) => {
                    if let Some(c) = keep {
                        self.put_idle(key, c);
                    }
                    return Ok(resp);
                }
                // a pooled connection closed by the server: retry on a new one when nothing came back
                Err((e, false)) if !e.is_timeout() => {}
                Err((e, _)) => return Err(e),
            }
        }
        let fresh = |e: Error| -> Error {
            // a client certificate that failed to load during the handshake explains the TLS failure
            if let Some(src) = &self.tls.client_cert
                && let Some(ce) = src.take_error() {
                    return Error::other(ce);
                }
            e
        };
        let conn = self.connect(u, proxy.as_ref(), deadline).map_err(fresh)?;
        match self.exchange(conn, &req, proxy.as_ref(), deadline) {
            Ok((resp, keep)) => {
                if let Some(c) = keep {
                    self.put_idle(key, c);
                }
                Ok(resp)
            }
            Err((e, _)) => Err(fresh(e)),
        }
    }

    fn close_idle_connections(&self) {
        if let Ok(mut v) = self.idle.lock() {
            v.clear();
        }
    }
}
