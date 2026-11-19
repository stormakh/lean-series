// Test helpers shared by the notifier tests: a small HTTP test server (Go httptest.Server) and Go's
// alertsEqual / JSON decoding of alerts.
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use model::labels::{self, Labels};
use notifier::Alert;

/// One received request.
pub struct TestRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl TestRequest {
    pub fn header(&self, name: &str) -> &str {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str()).unwrap_or("")
    }
    /// Go `r.BasicAuth()` (empty strings when absent).
    pub fn basic_auth(&self) -> (String, String) {
        let h = self.header("Authorization");
        let Some(enc) = h.strip_prefix("Basic ") else { return (String::new(), String::new()) };
        let dec = String::from_utf8(base64_decode(enc)).unwrap_or_default();
        match dec.split_once(':') {
            Some((u, p)) => (u.to_string(), p.to_string()),
            None => (String::new(), String::new()),
        }
    }
}

fn base64_decode(s: &str) -> Vec<u8> {
    let val = |c: u8| -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a' + 26) as u32),
            b'0'..=b'9' => Some((c - b'0' + 52) as u32),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    };
    let mut out = Vec::new();
    let (mut acc, mut bits) = (0u32, 0);
    for c in s.bytes() {
        let Some(v) = val(c) else { continue };
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    out
}

type Handler = dyn Fn(&TestRequest) -> u16 + Send + Sync;

/// Go `httptest.Server`: serves each connection on its own thread; the handler returns the status code.
pub struct TestServer {
    pub url: String,
    pub addr: String,
    stop: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
}

impl TestServer {
    pub fn new(handler: impl Fn(&TestRequest) -> u16 + Send + Sync + 'static) -> TestServer {
        let l = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = l.local_addr().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let h: Arc<Handler> = Arc::new(handler);
        let st = stop.clone();
        let accept = std::thread::spawn(move || {
            for c in l.incoming() {
                if st.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(c) = c else { continue };
                let h = h.clone();
                std::thread::spawn(move || serve(c, &*h));
            }
        });
        TestServer { url: format!("http://{addr}"), addr, stop, accept: Some(accept) }
    }

    /// Go `server.Close()`: stops accepting connections.
    pub fn close(&mut self) {
        if let Some(a) = self.accept.take() {
            self.stop.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(&self.addr);
            let _ = a.join();
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.close();
    }
}

fn serve(c: TcpStream, h: &Handler) {
    let _ = c.set_read_timeout(Some(Duration::from_secs(30)));
    let mut r = BufReader::new(match c.try_clone() {
        Ok(c) => c,
        Err(_) => return,
    });
    let mut w = c;
    let mut line = String::new();
    if r.read_line(&mut line).unwrap_or(0) == 0 {
        return;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();
    let mut headers = Vec::new();
    loop {
        let mut hl = String::new();
        if r.read_line(&mut hl).unwrap_or(0) == 0 {
            return;
        }
        let hl = hl.trim_end();
        if hl.is_empty() {
            break;
        }
        if let Some((k, v)) = hl.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    let len: usize = headers.iter().find(|(k, _)| k.eq_ignore_ascii_case("Content-Length")).and_then(|(_, v)| v.parse().ok()).unwrap_or(0);
    let mut body = vec![0u8; len];
    if r.read_exact(&mut body).is_err() {
        return;
    }
    let req = TestRequest { method, path, headers, body };
    let status = h(&req);
    let resp = format!("HTTP/1.1 {status} X\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
    let _ = w.write_all(resp.as_bytes());
    let _ = w.flush();
}

/// Go `json.Unmarshal(b, &[]*Alert)`: the alerts' labels and annotations from a request body.
pub fn decode_alerts(b: &[u8]) -> Result<Vec<Alert>, String> {
    let v = json::Json::parse_bytes(b).map_err(|e| format!("{e:?}"))?;
    let arr = v.as_arr().ok_or("not an array")?;
    let mut out = Vec::new();
    for a in arr {
        let get = |k: &str| -> Labels {
            match a.get(k).and_then(|o| o.as_obj()) {
                Some(o) => labels::from_map(o.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))),
                None => Labels::empty(),
            }
        };
        out.push(Alert { labels: get("labels"), annotations: get("annotations"), generator_url: a.str_of("generatorURL").to_string(), ..Default::default() });
    }
    Ok(out)
}

/// Go `alertsEqual`.
pub fn alerts_equal<A: AsRef<Alert>, B: AsRef<Alert>>(a: &[A], b: &[B]) -> Result<(), String> {
    if a.len() != b.len() {
        return Err(format!("length mismatch: {} != {}", a.len(), b.len()));
    }
    for (i, (x, y)) in a.iter().zip(b).enumerate() {
        if x.as_ref().labels != y.as_ref().labels {
            return Err(format!("label mismatch at index {i}: {} != {}", x.as_ref().labels, y.as_ref().labels));
        }
    }
    Ok(())
}

pub fn alert(pairs: &[&str]) -> Alert {
    Alert { labels: labels::from_strings(pairs), ..Default::default() }
}

/// Go `require.Eventually(cond, wait, tick)`.
pub fn eventually(wait: Duration, tick: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let end = std::time::Instant::now() + wait;
    loop {
        if cond() {
            return true;
        }
        if std::time::Instant::now() >= end {
            return false;
        }
        std::thread::sleep(tick);
    }
}
