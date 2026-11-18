//! Test helpers: an HTTP/1.1 test server (plain, or TLS with a required client certificate as Go's
//! `newTestServer`), the prometheus/common testdata directory as working directory, and temp dirs.
#![allow(dead_code)]
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once};
use std::thread::JoinHandle;
use std::time::Duration;

use net::Header;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub const COMMON_CONFIG_DIR: &str = "../../upstream/../tools/gopath/pkg/mod/github.com/prometheus/common@v0.72.1/config";

pub const TLS_CA_CHAIN_PATH: &str = "testdata/tls-ca-chain.pem";
pub const SERVER_CERTIFICATE_PATH: &str = "testdata/server.crt";
pub const SERVER_KEY_PATH: &str = "testdata/server.key";
pub const CLIENT_CERTIFICATE_PATH: &str = "testdata/client.crt";
pub const CLIENT_KEY_NO_PASS_PATH: &str = "testdata/client.key";
pub const INVALID_CA: &str = "testdata/client.key";
pub const WRONG_CLIENT_CERT_PATH: &str = "testdata/self-signed-client.crt";
pub const WRONG_CLIENT_KEY_PATH: &str = "testdata/self-signed-client.key";
pub const EMPTY_FILE: &str = "testdata/empty";
pub const MISSING_CA: &str = "missing/ca.crt";
pub const MISSING_CERT: &str = "missing/cert.crt";
pub const MISSING_KEY: &str = "missing/secret.key";

pub const EXPECTED_MESSAGE: &str = "I'm here to serve you!!!";
pub const EXPECTED_ERROR: &str = "expected error";
pub const AUTHORIZATION_CREDENTIALS: &str = "theanswertothegreatquestionoflifetheuniverseandeverythingisfortytwo";
pub const AUTHORIZATION_CREDENTIALS_FILE: &str = "testdata/bearer.token";
pub const AUTHORIZATION_TYPE: &str = "APIKEY";
pub const BEARER_TOKEN: &str = AUTHORIZATION_CREDENTIALS;
pub const BEARER_TOKEN_FILE: &str = AUTHORIZATION_CREDENTIALS_FILE;
pub const MISSING_BEARER_TOKEN_FILE: &str = "missing/bearer.token";
pub const EXPECTED_USERNAME: &str = "arthurdent";
pub const EXPECTED_PASSWORD: &str = "42";
pub const EXPECTED_ACCESS_TOKEN: &str = "12345";

pub fn expected_bearer() -> String {
    format!("Bearer {BEARER_TOKEN}")
}
pub fn expected_authentication_credentials() -> String {
    format!("{AUTHORIZATION_TYPE} {BEARER_TOKEN}")
}

/// Runs the tests from prometheus/common's config directory, so that "testdata/..." paths work as in Go.
pub fn setup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let dir = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), COMMON_CONFIG_DIR);
        std::env::set_current_dir(&dir).unwrap_or_else(|e| panic!("chdir {dir}: {e}"));
    });
}

/// A fresh temp dir under cargo's target tmp dir.
pub fn temp_dir(name: &str) -> String {
    let mut b = [0u8; 6];
    crypto::random_bytes(&mut b);
    let d = format!("{}/{}-{}", env!("CARGO_TARGET_TMPDIR"), name, crypto::hex(&b));
    std::fs::create_dir_all(&d).unwrap();
    d
}

pub fn read_file(p: &str) -> String {
    std::fs::read_to_string(p).unwrap_or_else(|e| panic!("Failed to read file {p:?}: {e}"))
}

#[derive(Debug, Clone)]
pub struct SReq {
    pub method: String,
    pub path: String,
    pub query: String,
    pub host: String,
    pub header: Header,
    pub body: Vec<u8>,
}

impl SReq {
    /// Go `r.BasicAuth()`.
    pub fn basic_auth(&self) -> Option<(String, String)> {
        let a = self.header.get("Authorization")?;
        let rest = a.strip_prefix("Basic ")?;
        let d = String::from_utf8(crypto::base64_decode(rest)?).ok()?;
        let (u, p) = d.split_once(':')?;
        Some((u.to_string(), p.to_string()))
    }
    /// Go `r.FormValue` for an urlencoded body.
    pub fn form_value(&self, k: &str) -> String {
        let (pairs, _) = yaml::url::parse_query(&String::from_utf8_lossy(&self.body));
        pairs.into_iter().find(|(n, _)| n == k).map(|(_, v)| v).unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
pub struct SResp {
    pub status: u16,
    pub header: Header,
    pub body: Vec<u8>,
}

impl SResp {
    pub fn text(s: &str) -> SResp {
        SResp { status: 200, header: Header::new(), body: s.as_bytes().to_vec() }
    }
    pub fn status(code: u16, s: &str) -> SResp {
        SResp { status: code, header: Header::new(), body: s.as_bytes().to_vec() }
    }
    /// Go `http.Redirect(w, r, url, 302)`.
    pub fn redirect(loc: &str) -> SResp {
        let mut h = Header::new();
        h.set("Location", loc);
        SResp { status: 302, header: h, body: format!("<a href=\"{loc}\">Found</a>.\n").into_bytes() }
    }
    pub fn json(s: &str) -> SResp {
        let mut h = Header::new();
        h.add("Content-Type", "application/json");
        SResp { status: 200, header: h, body: s.as_bytes().to_vec() }
    }
}

pub type Handler = Arc<dyn Fn(&SReq) -> SResp + Send + Sync>;

pub struct TestServer {
    pub url: String,
    pub port: u16,
    stop: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(h) = self.accept.take() {
            let _ = h.join();
        }
    }
}

fn load_certs(p: &str) -> Vec<CertificateDer<'static>> {
    net::tls::pem_blocks(read_file(p).as_bytes()).into_iter().filter(|b| b.type_ == "CERTIFICATE").map(|b| CertificateDer::from(b.der)).collect()
}

/// Go `newTestServer`: TLS with server.crt, client certificates required and verified with tls-ca-chain.pem.
pub fn tls_server_config() -> Arc<rustls::ServerConfig> {
    setup();
    let provider = net::tls::provider();
    let mut roots = rustls::RootCertStore::empty();
    for c in load_certs(TLS_CA_CHAIN_PATH) {
        roots.add(c).unwrap();
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone()).build().unwrap();
    let key = net::tls::pem_blocks(read_file(SERVER_KEY_PATH).as_bytes()).remove(0);
    let key = PrivateKeyDer::try_from(key.der).unwrap();
    let cfg = rustls::ServerConfig::builder_with_provider(provider).with_safe_default_protocol_versions().unwrap().with_client_cert_verifier(verifier).with_single_cert(load_certs(SERVER_CERTIFICATE_PATH), key).unwrap();
    Arc::new(cfg)
}

pub fn new_server(handler: impl Fn(&SReq) -> SResp + Send + Sync + 'static) -> TestServer {
    start(None, Arc::new(handler))
}

pub fn new_tls_server(handler: impl Fn(&SReq) -> SResp + Send + Sync + 'static) -> TestServer {
    start(Some(tls_server_config()), Arc::new(handler))
}

fn start(tls: Option<Arc<rustls::ServerConfig>>, handler: Handler) -> TestServer {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let stop = Arc::new(AtomicBool::new(false));
    let st = stop.clone();
    let scheme = if tls.is_some() { "https" } else { "http" };
    let accept = std::thread::spawn(move || {
        for c in l.incoming() {
            if st.load(Ordering::SeqCst) {
                break;
            }
            let Ok(c) = c else { continue };
            let h = handler.clone();
            let tls = tls.clone();
            let st2 = st.clone();
            std::thread::spawn(move || {
                let _ = c.set_read_timeout(Some(Duration::from_secs(5)));
                match tls {
                    None => serve(c, &h, &st2),
                    Some(cfg) => {
                        let Ok(sc) = rustls::ServerConnection::new(cfg) else { return };
                        serve(rustls::StreamOwned::new(sc, c), &h, &st2);
                    }
                }
            });
        }
    });
    TestServer { url: format!("{scheme}://127.0.0.1:{port}"), port, stop, accept: Some(accept) }
}

fn serve<S: Read + Write>(s: S, h: &Handler, stop: &AtomicBool) {
    let mut r = BufReader::new(s);
    loop {
        let mut line = String::new();
        if r.read_line(&mut line).unwrap_or(0) == 0 || stop.load(Ordering::SeqCst) {
            return;
        }
        let parts: Vec<&str> = line.trim_end().split(' ').collect();
        if parts.len() < 3 {
            return;
        }
        let (method, target) = (parts[0].to_string(), parts[1].to_string());
        let mut header = Header::new();
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
        // absolute-form targets (proxy requests) keep the full URL in path
        let (path, query) = match target.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (target.clone(), String::new()),
        };
        let host = header.get_str("Host").to_string();
        let req = SReq { method, path, query, host, header, body };
        let resp = h(&req);
        let mut out = format!("HTTP/1.1 {} {}\r\n", resp.status, net::transport::status_text(resp.status));
        for (k, v) in resp.header.iter() {
            out.push_str(&format!("{k}: {v}\r\n"));
        }
        out.push_str(&format!("Content-Length: {}\r\n\r\n", resp.body.len()));
        let w = r.get_mut();
        if w.write_all(out.as_bytes()).is_err() || w.write_all(&resp.body).is_err() || w.flush().is_err() {
            return;
        }
    }
}
