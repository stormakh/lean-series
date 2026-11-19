//! A test HTTP server on an ephemeral port and a raw HTTP/1.1 client that does not decode bodies (Go's
//! `http.Transport{DisableCompression: true}`).
#![allow(dead_code)]
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

/// Starts `svc` on 127.0.0.1 with an ephemeral port and returns the port. The server thread lives until the test
/// process ends (the http crate's server has no stop).
pub fn serve(svc: Arc<dyn http::Service>) -> u16 {
    // 1 MiB stacks: the CORS test handler compiles its regex per request (as the Go test does), which needs more
    // than the default 64 KiB in debug builds.
    let cfg = http::ServerConfig { gzip: false, stack_size: 1 << 20, ..http::ServerConfig::default() };
    let srv = http::Server::bind("127.0.0.1:0", cfg).expect("bind");
    let port = srv.local_port();
    std::thread::spawn(move || srv.run(svc));
    port
}

pub struct RawResponse {
    pub status: u16,
    /// Lowercase names, in order.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl RawResponse {
    pub fn header(&self, name: &str) -> &str {
        for (k, v) in &self.headers {
            if k.eq_ignore_ascii_case(name) {
                return v;
            }
        }
        ""
    }
    /// Go `resp.ContentLength`: -1 when unknown.
    pub fn content_length(&self) -> i64 {
        self.header("content-length").parse().unwrap_or(-1)
    }
}

/// Sends one request with `Connection: close` and reads the whole response.
pub fn request(method: &str, port: u16, path: &str, headers: &[(&str, &str)]) -> Result<RawResponse, String> {
    let mut c = TcpStream::connect(("127.0.0.1", port)).map_err(|e| e.to_string())?;
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n");
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    c.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
    let mut raw = Vec::new();
    c.read_to_end(&mut raw).map_err(|e| e.to_string())?;
    let end = raw.windows(4).position(|w| w == b"\r\n\r\n").ok_or("no header end")?;
    let head = String::from_utf8_lossy(&raw[..end]).into_owned();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().ok_or("no status line")?;
    let status: u16 = status_line.split(' ').nth(1).ok_or("no status")?.parse().map_err(|_| "bad status")?;
    let mut headers = Vec::new();
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
        }
    }
    let body = raw[end + 4..].to_vec();
    Ok(RawResponse { status, headers, body })
}

/// A service from a closure.
pub struct Func<F>(pub F);
impl<F: Fn(http::Request) -> http::Response + Send + Sync> http::Service for Func<F> {
    fn handle(&self, req: http::Request) -> http::Response {
        (self.0)(req)
    }
}
