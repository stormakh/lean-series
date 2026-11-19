//! Shared helpers of the discovery/xds tests (Go xds_test.go): `sdConf` and `createTestHTTPServer`, a TLS
//! test server (Go `httptest.NewTLSServer`; here with prometheus/common's testdata server.crt) that answers
//! protojson xDS discovery requests.
#![allow(dead_code)]
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use discovery::xds::{DiscoveryRequest, DiscoveryResponse, Message, SdConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Go `sdConf`.
pub fn sd_conf() -> SdConfig {
    SdConfig { server: "http://127.0.0.1".to_string(), refresh_interval: yaml::Duration::from_secs(10), client_id: "test-id".to_string(), ..Default::default() }
}

/// Go `discoveryResponder`: `Ok(None)` answers 304, `Err` answers 500.
pub type DiscoveryResponder = Arc<dyn Fn(&DiscoveryRequest) -> Result<Option<DiscoveryResponse>, String> + Send + Sync>;

/// A running test server. `close` stops it and fails the test if a request check failed (Go's `require` calls in
/// the handler).
pub struct TestServer {
    pub url: String,
    port: u16,
    stop: Arc<AtomicBool>,
    errors: Arc<Mutex<Vec<String>>>,
    accept: Option<JoinHandle<()>>,
}

impl TestServer {
    pub fn close(mut self) {
        self.shutdown();
        let errs = self.errors.lock().unwrap().clone();
        assert!(errs.is_empty(), "test server checks failed: {errs:?}");
    }
    fn shutdown(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(h) = self.accept.take() {
            let _ = h.join();
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

const COMMON_TESTDATA: &str = "../../upstream/../tools/gopath/pkg/mod/github.com/prometheus/common@v0.72.1/config/testdata";

fn read(name: &str) -> Vec<u8> {
    let p = format!("{}/{COMMON_TESTDATA}/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {p}: {e}"))
}

fn tls_config() -> Arc<rustls::ServerConfig> {
    let certs: Vec<CertificateDer<'static>> = net::tls::pem_blocks(&read("server.crt")).into_iter().filter(|b| b.type_ == "CERTIFICATE").map(|b| CertificateDer::from(b.der)).collect();
    let key = PrivateKeyDer::try_from(net::tls::pem_blocks(&read("server.key")).remove(0).der).unwrap();
    let cfg = rustls::ServerConfig::builder_with_provider(net::tls::provider()).with_safe_default_protocol_versions().unwrap().with_no_client_auth().with_single_cert(certs, key).unwrap();
    Arc::new(cfg)
}

/// Go `createTestHTTPServer`.
pub fn create_test_http_server(responder: impl Fn(&DiscoveryRequest) -> Result<Option<DiscoveryResponse>, String> + Send + Sync + 'static) -> TestServer {
    let responder: DiscoveryResponder = Arc::new(responder);
    let tls = tls_config();
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let stop = Arc::new(AtomicBool::new(false));
    let errors = Arc::new(Mutex::new(Vec::new()));
    let (st, errs) = (stop.clone(), errors.clone());
    let accept = std::thread::spawn(move || {
        for c in l.incoming() {
            if st.load(Ordering::SeqCst) {
                break;
            }
            let Ok(c) = c else { continue };
            let (tls, r, errs, st) = (tls.clone(), responder.clone(), errs.clone(), st.clone());
            std::thread::spawn(move || {
                let _ = c.set_read_timeout(Some(Duration::from_secs(5)));
                let Ok(sc) = rustls::ServerConnection::new(tls) else { return };
                serve(rustls::StreamOwned::new(sc, c), &r, &errs, &st);
            });
        }
    });
    TestServer { url: format!("https://127.0.0.1:{port}"), port, stop, errors, accept: Some(accept) }
}

/// The Go handler: checks the MIME headers and the body, decodes the DiscoveryRequest and answers.
fn handle(header: &net::Header, body: &[u8], responder: &DiscoveryResponder) -> Result<(u16, Vec<u8>), String> {
    if header.get_str("Content-Type") != "application/json" {
        return Err(format!("Content-Type: {:?}", header.get_str("Content-Type")));
    }
    if header.get_str("Accept") != "application/json" {
        return Err(format!("Accept: {:?}", header.get_str("Accept")));
    }
    if body.is_empty() {
        return Err("empty body".to_string());
    }
    let req = DiscoveryRequest::from_proto_json(body)?;
    let res = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| responder(&req))) {
        Ok(r) => r,
        Err(p) => {
            let msg = p.downcast_ref::<String>().cloned().or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string())).unwrap_or_default();
            return Err(format!("responder panicked: {msg}"));
        }
    };
    match res {
        Err(_) => Ok((500, Vec::new())),
        Ok(None) => Ok((304, Vec::new())),
        Ok(Some(r)) => Ok((200, r.to_proto_json()?.into_bytes())),
    }
}

fn serve<S: Read + Write>(s: S, responder: &DiscoveryResponder, errors: &Mutex<Vec<String>>, stop: &AtomicBool) {
    let mut r = BufReader::new(s);
    loop {
        let mut line = String::new();
        if r.read_line(&mut line).unwrap_or(0) == 0 || stop.load(Ordering::SeqCst) {
            return;
        }
        let mut header = net::Header::new();
        loop {
            let mut l = String::new();
            if r.read_line(&mut l).unwrap_or(0) == 0 {
                return;
            }
            let t = l.trim_end();
            if t.is_empty() {
                break;
            }
            if let Some((k, v)) = t.split_once(':') {
                header.add(k.trim(), v.trim());
            }
        }
        let n: usize = header.get("Content-Length").and_then(|v| v.parse().ok()).unwrap_or(0);
        let mut body = vec![0u8; n];
        if r.read_exact(&mut body).is_err() {
            return;
        }
        let (status, out) = match handle(&header, &body, responder) {
            Ok(x) => x,
            Err(e) => {
                errors.lock().unwrap().push(e);
                (500, Vec::new())
            }
        };
        let mut head = format!("HTTP/1.1 {} {}\r\n", status, net::transport::status_text(status));
        if status != 304 {
            head.push_str(&format!("Content-Length: {}\r\n", out.len()));
        }
        head.push_str("\r\n");
        let w = r.get_mut();
        if w.write_all(head.as_bytes()).is_err() || w.write_all(&out).is_err() || w.flush().is_err() {
            return;
        }
    }
}
