//! Shared test helpers for the SD provider tests: Go's `httptest.NewServer` as a small HTTP/1.1 server on an
//! ephemeral local port (one thread per connection, keep-alive, chunked request bodies), fixture paths, and the
//! metrics setup that Go tests repeat (`NewRefreshMetrics` + `NewDiscovererMetrics` + `Register`).
//!
//! Use from a test file with `mod common;`.
#![allow(dead_code)]
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use discovery::{Config, DiscovererMetrics, DiscovererOptions, Registry};

/// A request as the handler sees it (Go `*http.Request`).
#[derive(Clone, Debug, Default)]
pub struct Req {
    pub method: String,
    /// The request target as sent ("/v1/x?a=b").
    pub uri: String,
    /// The path, percent-decoded (Go `r.URL.Path`).
    pub path: String,
    /// The raw query without '?' (Go `r.URL.RawQuery`).
    pub raw_query: String,
    /// The query, decoded, in order (Go `r.URL.Query()`).
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Req {
    /// Go `r.Header.Get` (case-insensitive; "" when absent).
    pub fn header(&self, k: &str) -> &str {
        self.headers.iter().find(|(n, _)| n.eq_ignore_ascii_case(k)).map(|(_, v)| v.as_str()).unwrap_or("")
    }
    /// Go `r.URL.Query().Get`.
    pub fn query_get(&self, k: &str) -> &str {
        self.query.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str()).unwrap_or("")
    }
    /// Go `r.URL.Query()[k]`.
    pub fn query_values(&self, k: &str) -> Vec<&str> {
        self.query.iter().filter(|(n, _)| n == k).map(|(_, v)| v.as_str()).collect()
    }
    pub fn has_query(&self, k: &str) -> bool {
        self.query.iter().any(|(n, _)| n == k)
    }
    /// Go `r.BasicAuth()`.
    pub fn basic_auth(&self) -> Option<(String, String)> {
        let a = self.header("Authorization");
        let rest = a.strip_prefix("Basic ")?;
        let d = crypto::base64_decode(rest)?;
        let s = String::from_utf8(d).ok()?;
        let (u, p) = s.split_once(':')?;
        Some((u.to_string(), p.to_string()))
    }
}

/// A response (status, headers in order, body).
#[derive(Clone, Debug)]
pub struct Resp {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Sleep this long before answering (unresponsive servers).
    pub delay: Option<std::time::Duration>,
}

impl Resp {
    pub fn new(status: u16, body: impl Into<Vec<u8>>) -> Resp {
        Resp { status, headers: Vec::new(), body: body.into(), delay: None }
    }
    /// 200 with `Content-Type: application/json`.
    pub fn json(body: impl Into<Vec<u8>>) -> Resp {
        Resp::new(200, body).header("Content-Type", "application/json")
    }
    /// Go `http.Error(w, msg, code)`: text/plain body "msg\n".
    pub fn error(code: u16, msg: &str) -> Resp {
        Resp::new(code, format!("{msg}\n")).header("Content-Type", "text/plain; charset=utf-8").header("X-Content-Type-Options", "nosniff")
    }
    /// Go `http.NotFound`.
    pub fn not_found() -> Resp {
        Resp::error(404, "404 page not found")
    }
    pub fn header(mut self, k: &str, v: &str) -> Resp {
        self.headers.push((k.to_string(), v.to_string()));
        self
    }
    pub fn with_delay(mut self, d: std::time::Duration) -> Resp {
        self.delay = Some(d);
        self
    }
}

pub type Handler = Arc<dyn Fn(&Req) -> Resp + Send + Sync>;

/// Go `httptest.NewServer`: serves `handler` until dropped (Go `ts.Close()`).
pub struct TestServer {
    /// "http://127.0.0.1:<port>" (Go `ts.URL`).
    pub url: String,
    /// "127.0.0.1:<port>".
    pub addr: String,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    /// Every request served, in order.
    pub requests: Arc<Mutex<Vec<Req>>>,
}

pub fn status_text(c: u16) -> &'static str {
    net::transport::status_text(c)
}

fn pct_decode(s: &str, plus_space: bool) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                let h = std::str::from_utf8(&b[i + 1..i + 3]).ok().and_then(|h| u8::from_str_radix(h, 16).ok());
                match h {
                    Some(v) => {
                        out.push(v);
                        i += 3;
                    }
                    None => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' if plus_space => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Go `url.ParseQuery` (pairs in order; invalid pairs are skipped).
pub fn parse_query(q: &str) -> Vec<(String, String)> {
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (pct_decode(k, true), pct_decode(v, true)),
            None => (pct_decode(p, true), String::new()),
        })
        .collect()
}

fn read_request(r: &mut BufReader<TcpStream>) -> Option<Req> {
    let mut line = String::new();
    if r.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let mut it = line.split_whitespace();
    let method = it.next()?.to_string();
    let uri = it.next()?.to_string();
    let mut headers = Vec::new();
    loop {
        let mut h = String::new();
        if r.read_line(&mut h).ok()? == 0 {
            return None;
        }
        let h = h.trim_end_matches(['\r', '\n']);
        if h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    let get = |k: &str| headers.iter().find(|(n, _): &&(String, String)| n.eq_ignore_ascii_case(k)).map(|(_, v)| v.clone());
    let mut body = Vec::new();
    if get("Transfer-Encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked")) {
        loop {
            let mut sz = String::new();
            r.read_line(&mut sz).ok()?;
            let n = usize::from_str_radix(sz.trim().split(';').next()?.trim(), 16).ok()?;
            let mut chunk = vec![0u8; n + 2];
            r.read_exact(&mut chunk).ok()?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..n]);
        }
    } else if let Some(n) = get("Content-Length").and_then(|v| v.parse::<usize>().ok()) {
        body.resize(n, 0);
        r.read_exact(&mut body).ok()?;
    }
    let (p, q) = match uri.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (uri.clone(), String::new()),
    };
    // absolute-form (proxies)
    let p = match p.find("://") {
        Some(i) => p[i + 3..].find('/').map(|j| p[i + 3 + j..].to_string()).unwrap_or_else(|| "/".to_string()),
        None => p,
    };
    Some(Req { method, path: pct_decode(&p, false), query: parse_query(&q), raw_query: q, uri, headers, body })
}

fn serve_conn(c: TcpStream, handler: Handler, requests: Arc<Mutex<Vec<Req>>>, stop: Arc<AtomicBool>) {
    let mut w = match c.try_clone() {
        Ok(w) => w,
        Err(_) => return,
    };
    let mut r = BufReader::new(c);
    while let Some(req) = read_request(&mut r) {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        requests.lock().unwrap().push(req.clone());
        let close = req.header("Connection").eq_ignore_ascii_case("close");
        let resp = handler(&req);
        if let Some(d) = resp.delay {
            std::thread::sleep(d);
        }
        let mut out = format!("HTTP/1.1 {} {}\r\n", resp.status, status_text(resp.status));
        let mut has_ct = false;
        for (k, v) in &resp.headers {
            has_ct |= k.eq_ignore_ascii_case("Content-Type");
            out.push_str(&format!("{k}: {v}\r\n"));
        }
        if !has_ct && !resp.body.is_empty() {
            // Go sniffs the content type; tests that care set it.
            out.push_str("Content-Type: text/plain; charset=utf-8\r\n");
        }
        out.push_str(&format!("Content-Length: {}\r\n", resp.body.len()));
        if close {
            out.push_str("Connection: close\r\n");
        }
        out.push_str("\r\n");
        if w.write_all(out.as_bytes()).is_err() || (req.method != "HEAD" && w.write_all(&resp.body).is_err()) {
            return;
        }
        let _ = w.flush();
        if close {
            return;
        }
    }
}

impl TestServer {
    pub fn new(handler: impl Fn(&Req) -> Resp + Send + Sync + 'static) -> TestServer {
        TestServer::with_handler(Arc::new(handler))
    }

    pub fn with_handler(handler: Handler) -> TestServer {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let url = format!("http://{addr}");
        let stop = Arc::new(AtomicBool::new(false));
        let requests: Arc<Mutex<Vec<Req>>> = Arc::default();
        let (st, rq) = (stop.clone(), requests.clone());
        let thread = std::thread::spawn(move || {
            for conn in l.incoming() {
                if st.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(c) = conn else { continue };
                let (h, rq, st) = (handler.clone(), rq.clone(), st.clone());
                std::thread::spawn(move || serve_conn(c, h, rq, st));
            }
        });
        TestServer { url, addr, stop, thread: Some(thread), requests }
    }

    /// Serves files under `dir` (Go `http.FileServer(http.Dir(dir))` for plain files): JSON files as
    /// application/json, others as text/plain; 404 otherwise.
    pub fn file_server(dir: &str) -> TestServer {
        let dir = dir.to_string();
        TestServer::new(move |r: &Req| match std::fs::read(format!("{dir}{}", r.path)) {
            Ok(b) => {
                let ct = if r.path.ends_with(".json") { "application/json" } else { "text/plain; charset=utf-8" };
                Resp::new(200, b).header("Content-Type", ct)
            }
            Err(_) => Resp::not_found(),
        })
    }

    pub fn requests(&self) -> Vec<Req> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(&self.addr);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Absolute path of an upstream file: `upstream("discovery/moby/testdata")`.
pub fn upstream(rel: &str) -> String {
    format!("{}/../../upstream/{rel}", env!("CARGO_MANIFEST_DIR"))
}

/// Reads an upstream file (panics with the path when missing).
pub fn read_upstream(rel: &str) -> Vec<u8> {
    let p = upstream(rel);
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {p}: {e}"))
}

/// The Go test preamble: `reg := prometheus.NewRegistry(); refreshMetrics := discovery.NewRefreshMetrics(reg);
/// metrics := cfg.NewDiscovererMetrics(reg, refreshMetrics); require.NoError(t, metrics.Register())`.
/// Returns the options to pass to the provider constructor (logger nop, set name "test") and the registry.
pub fn new_options<C: Config>(cfg: &C) -> (DiscovererOptions, Arc<Registry>) {
    let reg = Registry::new();
    let refresh_metrics = discovery::new_refresh_metrics(&reg);
    refresh_metrics.register().unwrap();
    let metrics = cfg.new_discoverer_metrics(&reg, refresh_metrics);
    metrics.register().unwrap();
    (DiscovererOptions { logger: discovery::Logger::nop(), metrics: Some(metrics), set_name: "test".to_string(), ..Default::default() }, reg)
}

/// `model.LabelSet` literal.
pub fn ls(p: &[(&str, &str)]) -> model::labelset::LabelSet {
    model::labelset::LabelSet::from_pairs(p)
}
