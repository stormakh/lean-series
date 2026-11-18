//! Port of discovery/http/http_test.go. Go's httptest server is a small HTTP/1.1 server on a local port here.
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use discovery::http::{Discovery, SdConfig, match_content_type, url_source};
use discovery::{CancelToken, Config, DiscovererMetrics, DiscovererOptions, Group, Registry};
use model::labelset::LabelSet;

type Handler = Arc<dyn Fn(&str) -> (u16, Vec<(String, String)>, Vec<u8>) + Send + Sync>;

/// Go `httptest.NewServer`: serves `handler(path)` until dropped.
struct TestServer {
    url: String,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

fn status_text(c: u16) -> &'static str {
    match c {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "",
    }
}

impl TestServer {
    fn new(handler: Handler) -> TestServer {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let st = stop.clone();
        let thread = std::thread::spawn(move || {
            for conn in l.incoming() {
                if st.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(mut c) = conn else { continue };
                let mut r = BufReader::new(c.try_clone().unwrap());
                let mut line = String::new();
                if r.read_line(&mut line).is_err() {
                    continue;
                }
                let path = line.split_whitespace().nth(1).unwrap_or("/").to_string();
                loop {
                    let mut h = String::new();
                    if r.read_line(&mut h).is_err() || h == "\r\n" || h.is_empty() {
                        break;
                    }
                }
                let (code, headers, body) = handler(&path);
                let mut resp = format!("HTTP/1.1 {} {}\r\n", code, status_text(code));
                for (k, v) in headers {
                    resp.push_str(&format!("{k}: {v}\r\n"));
                }
                resp.push_str(&format!("Content-Length: {}\r\nConnection: close\r\n\r\n", body.len()));
                let _ = c.write_all(resp.as_bytes());
                let _ = c.write_all(&body);
            }
        });
        TestServer { url, stop, thread: Some(thread) }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = std::net::TcpStream::connect(self.url.trim_start_matches("http://"));
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Go `http.FileServer(http.Dir("./fixtures"))` for the files used here.
fn file_server() -> Handler {
    Arc::new(|path: &str| {
        let f = format!("{}/../../upstream/discovery/http/fixtures{}", env!("CARGO_MANIFEST_DIR"), path);
        match std::fs::read(&f) {
            Ok(b) => {
                let ct = if path.ends_with(".json") { "application/json" } else { "text/plain; charset=utf-8" };
                (200, vec![("Content-Type".to_string(), ct.to_string())], b)
            }
            Err(_) => (404, vec![("Content-Type".to_string(), "text/plain; charset=utf-8".to_string())], b"404 page not found\n".to_vec()),
        }
    })
}

fn new_discovery(cfg: &SdConfig) -> (Discovery, Arc<Registry>) {
    let reg = Registry::new();
    let refresh_metrics = discovery::new_refresh_metrics(&reg);
    refresh_metrics.register().unwrap();
    let metrics = cfg.new_discoverer_metrics(&reg, refresh_metrics);
    metrics.register().unwrap();
    let d = Discovery::new(cfg, DiscovererOptions { logger: discovery::Logger::nop(), metrics: Some(metrics), set_name: "http".to_string(), ..Default::default() }).unwrap();
    (d, reg)
}

fn ls(p: &[(&str, &str)]) -> LabelSet {
    LabelSet::from_pairs(p)
}

#[test]
fn test_http_valid_refresh() {
    let ts = TestServer::new(file_server());
    let cfg = SdConfig { http_client_config: net::HttpClientConfig::default_config(), url: format!("{}/http_sd.good.json", ts.url), refresh_interval: yaml::Duration::from_secs(30) };
    let (mut d, _reg) = new_discovery(&cfg);
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    let url = format!("{}/http_sd.good.json", ts.url);
    let expected = vec![Arc::new(Group {
        targets: vec![ls(&[("__address__", "127.0.0.1:9090")])],
        labels: ls(&[("__meta_datacenter", "bru1"), ("__meta_url", &url)]),
        source: url_source(&url, 0),
    })];
    assert_eq!(expected, tgs);
    assert_eq!(0.0, d.failures_count());
}

#[test]
fn test_http_invalid_code() {
    let ts = TestServer::new(Arc::new(|_: &str| (400, vec![], vec![])));
    let cfg = SdConfig { http_client_config: net::HttpClientConfig::default_config(), url: ts.url.clone(), refresh_interval: yaml::Duration::from_secs(30) };
    let (mut d, _reg) = new_discovery(&cfg);
    let err = d.refresh(&CancelToken::new()).unwrap_err();
    assert_eq!("server returned HTTP status 400 Bad Request", err);
    assert_eq!(1.0, d.failures_count());
}

#[test]
fn test_http_invalid_format() {
    // Go's ResponseWriter sniffs "text/plain; charset=utf-8" for "{}\n".
    let ts = TestServer::new(Arc::new(|_: &str| (200, vec![("Content-Type".to_string(), "text/plain; charset=utf-8".to_string())], b"{}\n".to_vec())));
    let cfg = SdConfig { http_client_config: net::HttpClientConfig::default_config(), url: ts.url.clone(), refresh_interval: yaml::Duration::from_secs(30) };
    let (mut d, _reg) = new_discovery(&cfg);
    let err = d.refresh(&CancelToken::new()).unwrap_err();
    assert_eq!(r#"unsupported content type "text/plain; charset=utf-8""#, err);
    assert_eq!(1.0, d.failures_count());
}

#[test]
fn test_content_type_regex() {
    let cases = [
        ("application/json;charset=utf-8", true),
        ("application/json;charset=UTF-8", true),
        ("Application/JSON;Charset=\"utf-8\"", true),
        ("application/json; charset=\"utf-8\"", true),
        ("application/json", true),
        ("application/jsonl; charset=\"utf-8\"", false),
        ("application/json;charset=UTF-9", false),
        ("application /json;charset=UTF-8", false),
        ("application/ json;charset=UTF-8", false),
        ("application/json;", false),
        ("charset=UTF-8", false),
    ];
    for (header, m) in cases {
        assert_eq!(m, match_content_type(header), "{header}");
    }
}

#[test]
fn test_source_disappeared() {
    let stub: Arc<Mutex<String>> = Arc::default();
    let s2 = stub.clone();
    let ts = TestServer::new(Arc::new(move |_: &str| (200, vec![("Content-Type".to_string(), "application/json".to_string())], format!("{}\n", s2.lock().unwrap()).into_bytes())));
    let u = ts.url.clone();
    let g = |k: &str, addr: &str, i: usize| Group { targets: vec![ls(&[("__address__", addr)])], labels: ls(&[("k", k), ("__meta_url", &u)]), source: url_source(&u, i) };
    let empty = |i: usize| Group { targets: vec![], labels: LabelSet::new(), source: url_source(&u, i) };
    let cases: Vec<(Vec<&str>, Vec<Vec<Group>>)> = vec![
        (vec!["[]", "[]"], vec![vec![], vec![]]),
        (
            vec![r#"[{"labels": {"k": "1"}, "targets": ["127.0.0.1"]}]"#, r#"[{"labels": {"k": "1"}, "targets": ["127.0.0.1"]}, {"labels": {"k": "2"}, "targets": ["127.0.0.1"]}]"#],
            vec![vec![g("1", "127.0.0.1", 0)], vec![g("1", "127.0.0.1", 0), g("2", "127.0.0.1", 1)]],
        ),
        (
            vec![r#"[{"labels": {"k": "1"}, "targets": ["127.0.0.1"]}, {"labels": {"k": "2"}, "targets": ["127.0.0.1"]}]"#, r#"[{"labels": {"k": "1"}, "targets": ["127.0.0.1"]}]"#],
            vec![vec![g("1", "127.0.0.1", 0), g("2", "127.0.0.1", 1)], vec![g("1", "127.0.0.1", 0), empty(1)]],
        ),
        (
            vec![
                r#"[{"labels": {"k": "1"}, "targets": ["127.0.0.1"]}, {"labels": {"k": "2"}, "targets": ["127.0.0.1"]}, {"labels": {"k": "3"}, "targets": ["127.0.0.1"]}]"#,
                r#"[{"labels": {"k": "1"}, "targets": ["127.0.0.1"]}]"#,
                r#"[{"labels": {"k": "v"}, "targets": ["127.0.0.2"]}, {"labels": {"k": "vv"}, "targets": ["127.0.0.3"]}]"#,
            ],
            vec![
                vec![g("1", "127.0.0.1", 0), g("2", "127.0.0.1", 1), g("3", "127.0.0.1", 2)],
                vec![g("1", "127.0.0.1", 0), empty(1), empty(2)],
                vec![g("v", "127.0.0.2", 0), g("vv", "127.0.0.3", 1)],
            ],
        ),
    ];
    let cfg = SdConfig { http_client_config: net::HttpClientConfig::default_config(), url: ts.url.clone(), refresh_interval: yaml::Duration::from_secs(1) };
    let (mut d, _reg) = new_discovery(&cfg);
    for (responses, expected) in cases {
        for (i, res) in responses.iter().enumerate() {
            *stub.lock().unwrap() = res.to_string();
            let tgs = d.refresh(&CancelToken::new()).unwrap();
            let got: Vec<Group> = tgs.iter().map(|g| (**g).clone()).collect();
            assert_eq!(expected[i], got);
        }
    }
}
