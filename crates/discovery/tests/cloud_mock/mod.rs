//! A tiny HTTP/1.1 server on an ephemeral local port for the cloud SD tests (Go tests mock the SDK clients;
//! here the same data is served as API responses). Use with `mod cloud_mock;` in a test file.
#![allow(dead_code)]
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// A request as the handler sees it.
#[derive(Clone, Debug, Default)]
pub struct MockRequest {
    pub method: String,
    /// The path without the query, still escaped.
    pub path: String,
    /// The raw query (without '?').
    pub query: String,
    /// Header names in lower case.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl MockRequest {
    pub fn header(&self, name: &str) -> &str {
        let n = name.to_ascii_lowercase();
        self.headers.iter().find(|(k, _)| *k == n).map(|(_, v)| v.as_str()).unwrap_or("")
    }
    pub fn body_str(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
    /// The decoded query parameters (or form body parameters with `form_params`).
    pub fn query_params(&self) -> Vec<(String, String)> {
        parse_form(&self.query)
    }
    pub fn form_params(&self) -> Vec<(String, String)> {
        parse_form(&self.body_str())
    }
    /// The first value of a query or form parameter.
    pub fn param(&self, k: &str) -> Option<String> {
        self.query_params().into_iter().chain(self.form_params()).find(|(n, _)| n == k).map(|(_, v)| v)
    }
}

fn unescape(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                if let Some(v) = std::str::from_utf8(&b[i + 1..i + 3]).ok().and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    out.push(v);
                    i += 3;
                    continue;
                }
                out.push(b'%');
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn parse_form(s: &str) -> Vec<(String, String)> {
    s.split('&').filter(|p| !p.is_empty()).map(|p| match p.split_once('=') {
        Some((k, v)) => (unescape(k), unescape(v)),
        None => (unescape(p), String::new()),
    }).collect()
}

/// A response: status, headers, body.
pub struct MockResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl MockResponse {
    pub fn new(status: u16, content_type: &str, body: impl Into<Vec<u8>>) -> MockResponse {
        MockResponse { status, headers: vec![("Content-Type".to_string(), content_type.to_string())], body: body.into() }
    }
    pub fn json(body: impl Into<Vec<u8>>) -> MockResponse {
        MockResponse::new(200, "application/json", body)
    }
    pub fn xml(body: impl Into<Vec<u8>>) -> MockResponse {
        MockResponse::new(200, "text/xml;charset=UTF-8", body)
    }
    pub fn status(status: u16, body: impl Into<Vec<u8>>) -> MockResponse {
        MockResponse::new(status, "text/plain; charset=utf-8", body)
    }
    pub fn header(mut self, k: &str, v: &str) -> MockResponse {
        self.headers.push((k.to_string(), v.to_string()));
        self
    }
}

pub type Handler = Arc<dyn Fn(&MockRequest) -> MockResponse + Send + Sync>;

/// Go `httptest.NewServer`: serves `handler` on 127.0.0.1:<ephemeral> until dropped. Every request is recorded.
pub struct MockServer {
    /// "http://127.0.0.1:<port>"
    pub url: String,
    pub requests: Arc<Mutex<Vec<MockRequest>>>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

fn status_text(c: u16) -> &'static str {
    match c {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

fn read_request(c: &TcpStream) -> Option<MockRequest> {
    let mut r = BufReader::new(c.try_clone().ok()?);
    let mut line = String::new();
    if r.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next().unwrap_or("/").to_string();
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target, String::new()),
    };
    let mut headers = Vec::new();
    loop {
        let mut h = String::new();
        if r.read_line(&mut h).ok()? == 0 || h == "\r\n" || h == "\n" {
            break;
        }
        if let Some((k, v)) = h.trim_end().split_once(':') {
            headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
        }
    }
    let len: usize = headers.iter().find(|(k, _)| k == "content-length").and_then(|(_, v)| v.parse().ok()).unwrap_or(0);
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).ok()?;
    Some(MockRequest { method, path, query, headers, body })
}

impl MockServer {
    pub fn new(handler: Handler) -> MockServer {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let requests: Arc<Mutex<Vec<MockRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let (st, reqs) = (stop.clone(), requests.clone());
        let thread = std::thread::spawn(move || {
            for conn in l.incoming() {
                if st.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(mut c) = conn else { continue };
                let (h, reqs) = (handler.clone(), reqs.clone());
                // one thread per connection so that concurrent client requests are served
                std::thread::spawn(move || {
                    let Some(req) = read_request(&c) else { return };
                    reqs.lock().unwrap().push(req.clone());
                    let resp = h(&req);
                    let mut out = format!("HTTP/1.1 {} {}\r\n", resp.status, status_text(resp.status));
                    for (k, v) in &resp.headers {
                        out.push_str(&format!("{k}: {v}\r\n"));
                    }
                    out.push_str(&format!("Content-Length: {}\r\nConnection: close\r\n\r\n", resp.body.len()));
                    let _ = c.write_all(out.as_bytes());
                    let _ = c.write_all(&resp.body);
                    let _ = c.flush();
                });
            }
        });
        MockServer { url, requests, stop, thread: Some(thread) }
    }

    pub fn from_fn(f: impl Fn(&MockRequest) -> MockResponse + Send + Sync + 'static) -> MockServer {
        MockServer::new(Arc::new(f))
    }

    /// The requests served so far.
    pub fn requests(&self) -> Vec<MockRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.url.trim_start_matches("http://"));
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}
