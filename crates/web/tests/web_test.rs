//! Port of `web/web_test.go`.
//!
//! adapted: Go listens on ":<random port>" (all interfaces); these tests listen on fixed ports of this agent's
//! range on 127.0.0.1, one port per test, so parallel tests do not collide.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use discovery::log::{Level, Logger};
use http::{Client, ClientResponse, Request, Response};
use web::api::PrometheusVersion;
use web::web::{CancelToken, ErrAbortHandler, ExternalUrl, Handler, Options, ReadyStatus, TsdbAdapter, with_stack_tracer};

// Go's http.Get has no timeout; a TSDB snapshot fsyncs, which can take seconds on a busy disk.
const TIMEOUT: Duration = Duration::from_secs(120);

fn client() -> &'static Client {
    static C: std::sync::OnceLock<Client> = std::sync::OnceLock::new();
    C.get_or_init(Client::new)
}

fn do_req(method: &str, url: &str, body: &[u8]) -> ClientResponse {
    client().request(method, url, &[], body, TIMEOUT).unwrap_or_else(|e| panic!("{} {}: {}", method, url, e))
}

fn get(url: &str) -> ClientResponse {
    do_req("GET", url, b"")
}

fn head(url: &str) -> ClientResponse {
    do_req("HEAD", url, b"")
}

fn post(url: &str, body: &str) -> ClientResponse {
    do_req("POST", url, body.as_bytes())
}

fn temp_dir(name: &str) -> PathBuf {
    let d = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("web_test").join(name);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).expect("create temp dir");
    d
}

/// Go `tsdb.Open(dbDir, nil, nil, nil, nil)` wrapped in `dbAdapter`.
fn open_db(dir: &std::path::Path) -> (Arc<tsdb::db::DB>, Arc<TsdbAdapter>) {
    let db = tsdb::db::open(dir, None, None, None).expect("open tsdb");
    (db.clone(), Arc::new(TsdbAdapter(db)))
}

fn base_options(addr: &str, db_dir: &std::path::Path, adapter: Option<Arc<TsdbAdapter>>) -> Options {
    let mut o = Options {
        listen_addresses: vec![addr.to_string()],
        read_timeout: Duration::from_secs(30),
        max_connections: 512,
        tsdb_dir: db_dir.to_string_lossy().into_owned(),
        route_prefix: "/".to_string(),
        enable_admin_api: true,
        external_url: ExternalUrl { scheme: "http".to_string(), host: format!("localhost{}", &addr[addr.rfind(':').unwrap_or(0)..]), path: "/".to_string() },
        version: Some(PrometheusVersion::default()),
        gatherer: Some(web::metrics::default_gatherer()),
        flags: BTreeMap::new(),
        ..Default::default()
    };
    if let Some(a) = adapter {
        o.local_storage = Some(a.clone());
        o.tsdb_admin = Some(a);
    }
    o
}

/// Starts `Run` in a thread; the returned token stops it, the handle returns its result.
fn start(h: &Arc<Handler>) -> (CancelToken, std::thread::JoinHandle<Result<(), String>>) {
    let l = h.listeners().unwrap_or_else(|e| panic!("Unable to start web listeners: {}", e));
    let ctx = CancelToken::new();
    let (c2, h2) = (ctx.clone(), h.clone());
    let t = std::thread::spawn(move || h2.run(&c2, l, ""));
    (ctx, t)
}

fn stop(ctx: CancelToken, t: std::thread::JoinHandle<Result<(), String>>) {
    ctx.cancel();
    let r = t.join().expect("run thread");
    assert!(r.is_ok(), "Can't start web handler:{:?}", r);
}

/// Go `waitForServerReady`.
fn wait_for_server_ready(base_url: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(r) = client().request("GET", &format!("{}/-/healthy", base_url), &[], b"", TIMEOUT) {
            if r.status == 200 {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("Server did not become ready within {:?}", timeout);
}

/// Go `cleanupSnapshot`.
fn cleanup_snapshot(db_dir: &std::path::Path, resp: &ClientResponse) {
    let v = json::Json::parse_bytes(&resp.body).expect("snapshot response JSON");
    let name = v.get("data").and_then(|d| d.get("name")).and_then(|n| n.as_str()).unwrap_or("").to_string();
    assert!(!name.is_empty(), "snapshot directory not returned");
    std::fs::remove_dir(db_dir.join("snapshots").join(&name)).expect("remove snapshot");
    std::fs::remove_dir(db_dir.join("snapshots")).expect("remove snapshots");
}

#[test]
fn test_with_stack_tracer() {
    // adapted: Go's otelhttp wrapper is left out (no tracing). A Rust panic payload stands for Go's panic value;
    // `ErrAbortHandler` is Go's `http.ErrAbortHandler`, and a wrapped abort is a different value.
    #[derive(Clone)]
    enum Panic {
        None,
        Abort,
        Msg(String),
    }
    let cases = [
        ("success", Panic::None, false),
        ("abort", Panic::Abort, false),
        ("ordinary panic", Panic::Msg("unexpected failure".to_string()), true),
        ("wrapped abort", Panic::Msg("wrapped: net/http: abort Handler".to_string()), true),
    ];
    for (name, panic_value, want_log) in cases {
        let logs = Arc::new(Mutex::new(String::new()));
        let l2 = logs.clone();
        let logger = Logger::new(Arc::new(move |level: Level, msg: &str, kv: &[(String, String)]| {
            let mut s = l2.lock().unwrap();
            s.push_str(&format!("level={} msg=\"{}\"", level.as_str(), msg));
            for (k, v) in kv {
                s.push_str(&format!(" {}={}", k, v));
            }
            s.push('\n');
        }));
        let req = Request { method: "GET".to_string(), path: "/".to_string(), ..Default::default() };
        let pv = panic_value.clone();
        let handler = move |_: Request| -> Response {
            match &pv {
                Panic::None => Response::new(204),
                Panic::Abort => std::panic::panic_any(ErrAbortHandler),
                Panic::Msg(m) => std::panic::panic_any(m.clone()),
            }
        };
        match panic_value {
            Panic::None => {
                let resp = with_stack_tracer(&logger, req, handler);
                assert_eq!(204, resp.status, "{}", name);
            }
            Panic::Abort => {
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| with_stack_tracer(&logger, req, handler)));
                let p = r.err().unwrap_or_else(|| panic!("{}: expected a panic", name));
                assert_eq!(Some(&ErrAbortHandler), p.downcast_ref::<ErrAbortHandler>(), "{}", name);
            }
            Panic::Msg(m) => {
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| with_stack_tracer(&logger, req, handler)));
                let p = r.err().unwrap_or_else(|| panic!("{}: expected a panic", name));
                assert_eq!(Some(&m), p.downcast_ref::<String>(), "{}", name);
            }
        }
        let out = logs.lock().unwrap().clone();
        if want_log {
            assert!(out.contains("panic while serving request"), "{}: {}", name, out);
            assert!(out.contains("stack="), "{}: {}", name, out);
        } else {
            assert!(out.is_empty(), "{}: {}", name, out);
        }
    }
}

#[test]
fn test_ready_and_healthy() {
    let db_dir = temp_dir("ready_and_healthy");
    let (db, adapter) = open_db(&db_dir);
    let addr = "127.0.0.1:9981";
    let opts = base_options(addr, &db_dir, Some(adapter));
    let h = Handler::new(opts);
    h.apply_config(Arc::new(config::Config::default()));
    let (ctx, t) = start(&h);

    let base_url = format!("http://{}", addr);
    wait_for_server_ready(&base_url, Duration::from_secs(5));

    let resp = get(&format!("{}/-/healthy", base_url));
    assert_eq!(200, resp.status);
    let resp = head(&format!("{}/-/healthy", base_url));
    assert_eq!(200, resp.status);

    for u in [format!("{}/-/ready", base_url)] {
        let resp = get(&u);
        assert_eq!(503, resp.status);
        assert_eq!(Some("false"), resp.header("X-Prometheus-Stopping"));
        let resp = head(&u);
        assert_eq!(503, resp.status);
        assert_eq!(Some("false"), resp.header("X-Prometheus-Stopping"));
    }

    // Set to stopping.
    h.set_ready(ReadyStatus::Stopping);

    for u in [format!("{}/-/ready", base_url)] {
        let resp = get(&u);
        assert_eq!(503, resp.status);
        assert_eq!(Some("true"), resp.header("X-Prometheus-Stopping"));
        let resp = head(&u);
        assert_eq!(503, resp.status);
        assert_eq!(Some("true"), resp.header("X-Prometheus-Stopping"));
    }

    let resp = post(&format!("{}/api/v1/admin/tsdb/snapshot", base_url), "");
    assert_eq!(503, resp.status);
    let resp = post(&format!("{}/api/v1/admin/tsdb/delete_series", base_url), "{}");
    assert_eq!(503, resp.status);

    // Set to ready.
    h.set_ready(ReadyStatus::Ready);

    for u in [format!("{}/-/healthy", base_url), format!("{}/-/ready", base_url)] {
        let resp = get(&u);
        assert_eq!(200, resp.status);
        let resp = head(&u);
        assert_eq!(200, resp.status);
    }

    let resp = post(&format!("{}/api/v1/admin/tsdb/snapshot", base_url), "");
    assert_eq!(200, resp.status);
    cleanup_snapshot(&db_dir, &resp);

    let resp = post(&format!("{}/api/v1/admin/tsdb/delete_series?match[]=up", base_url), "");
    assert_eq!(204, resp.status);

    stop(ctx, t);
    db.close().expect("close db");
}

#[test]
fn test_route_prefix() {
    let db_dir = temp_dir("route_prefix");
    let (db, adapter) = open_db(&db_dir);
    let addr = "127.0.0.1:9982";
    let mut opts = base_options(addr, &db_dir, Some(adapter));
    opts.route_prefix = "/prometheus".to_string();
    opts.external_url = ExternalUrl { scheme: "http".to_string(), host: "localhost.localdomain:9982".to_string(), path: String::new() };
    opts.version = None;
    opts.gatherer = None;
    let prefix = opts.route_prefix.clone();
    let h = Handler::new(opts);
    let (ctx, t) = start(&h);

    let base_url = format!("http://{}", addr);
    wait_for_server_ready(&format!("{}{}", base_url, prefix), Duration::from_secs(5));

    let resp = get(&format!("{}{}/-/healthy", base_url, prefix));
    assert_eq!(200, resp.status);
    let resp = get(&format!("{}{}/-/ready", base_url, prefix));
    assert_eq!(503, resp.status);
    let resp = post(&format!("{}{}/api/v1/admin/tsdb/snapshot", base_url, prefix), "");
    assert_eq!(503, resp.status);
    let resp = post(&format!("{}{}/api/v1/admin/tsdb/delete_series", base_url, prefix), "{}");
    assert_eq!(503, resp.status);

    // Set to ready.
    h.set_ready(ReadyStatus::Ready);

    let resp = get(&format!("{}{}/-/healthy", base_url, prefix));
    assert_eq!(200, resp.status);
    let resp = get(&format!("{}{}/-/ready", base_url, prefix));
    assert_eq!(200, resp.status);
    let resp = post(&format!("{}{}/api/v1/admin/tsdb/snapshot", base_url, prefix), "");
    assert_eq!(200, resp.status);
    cleanup_snapshot(&db_dir, &resp);
    let resp = post(&format!("{}{}/api/v1/admin/tsdb/delete_series?match[]=up", base_url, prefix), "");
    assert_eq!(204, resp.status);

    stop(ctx, t);
    db.close().expect("close db");
}

fn router_get(h: &Handler, path: &str) -> Response {
    let (p, q) = path.split_once('?').unwrap_or((path, ""));
    h.router().serve(Request { method: "GET".to_string(), path: p.to_string(), query: q.to_string(), ..Default::default() })
}

#[test]
fn test_debug_handler() {
    // adapted: pprof is Go-only; the debug handler answers cmdline and the goroutine profile (as the thread
    // count) with 200 and unknown profiles with Go's 404.
    for (prefix, url, code) in [
        ("/", "/debug/pprof/cmdline", 200),
        ("/foo", "/foo/debug/pprof/cmdline", 200),
        ("/", "/debug/pprof/goroutine", 200),
        ("/foo", "/foo/debug/pprof/goroutine", 200),
        ("/", "/debug/pprof/foo", 404),
        ("/foo", "/bar/debug/pprof/goroutine", 404),
    ] {
        let opts = Options {
            route_prefix: prefix.to_string(),
            listen_addresses: vec!["somehost:9090".to_string()],
            external_url: ExternalUrl { scheme: "http".to_string(), host: "localhost.localdomain:9090".to_string(), path: String::new() },
            version: Some(PrometheusVersion::default()),
            ..Default::default()
        };
        let h = Handler::new(opts);
        h.set_ready(ReadyStatus::Ready);
        let resp = router_get(&h, url);
        assert_eq!(code, resp.status, "{} {}", prefix, url);
    }
}

#[test]
fn test_http_metrics() {
    let h = Handler::new(Options {
        route_prefix: "/".to_string(),
        listen_addresses: vec!["somehost:9090".to_string()],
        external_url: ExternalUrl { scheme: "http".to_string(), host: "localhost.localdomain:9090".to_string(), path: String::new() },
        version: Some(PrometheusVersion::default()),
        ..Default::default()
    });
    let get_ready = || router_get(&h, "/-/ready").status;

    let code = get_ready();
    assert_eq!(503, code);
    let ready = h.metrics().ready_status.get();
    assert_eq!(0, ready.get() as i64);
    let counter = &h.metrics().request_counter;
    assert_eq!(1, counter.with_label_values(&["/-/ready", "503"]).get() as i64);

    h.set_ready(ReadyStatus::Ready);
    for _ in 0..2 {
        assert_eq!(200, get_ready());
    }
    assert_eq!(1, ready.get() as i64);
    assert_eq!(2, counter.with_label_values(&["/-/ready", "200"]).get() as i64);
    assert_eq!(1, counter.with_label_values(&["/-/ready", "503"]).get() as i64);

    h.set_ready(ReadyStatus::NotReady);
    for _ in 0..2 {
        assert_eq!(503, get_ready());
    }
    assert_eq!(0, ready.get() as i64);
    assert_eq!(2, counter.with_label_values(&["/-/ready", "200"]).get() as i64);
    assert_eq!(3, counter.with_label_values(&["/-/ready", "503"]).get() as i64);
}

#[test]
fn test_shutdown_with_stale_connection() {
    let db_dir = temp_dir("shutdown_with_stale_connection");
    let (db, adapter) = open_db(&db_dir);
    let timeout = Duration::from_secs(10);
    let addr = "127.0.0.1:9983";
    let mut opts = base_options(addr, &db_dir, Some(adapter));
    opts.read_timeout = timeout;
    opts.enable_admin_api = false;
    let h = Handler::new(opts);
    h.apply_config(Arc::new(config::Config::default()));
    let (ctx, t) = start(&h);

    let base_url = format!("http://{}", addr);
    wait_for_server_ready(&base_url, Duration::from_secs(5));

    // Open a socket, and don't use it. This connection should then be closed after the ReadTimeout.
    let c = std::net::TcpStream::connect(addr).expect("dial");

    // Stop the web handler.
    ctx.cancel();
    let deadline = Instant::now() + timeout + Duration::from_secs(5);
    while !t.is_finished() {
        assert!(Instant::now() < deadline, "Server still running after read timeout.");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(t.join().expect("run thread").is_ok());
    drop(c);
    db.close().expect("close db");
}

#[test]
fn test_handle_multiple_quit_requests() {
    let addr = "127.0.0.1:9984";
    let opts = Options {
        listen_addresses: vec![addr.to_string()],
        max_connections: 512,
        enable_lifecycle: true,
        route_prefix: "/".to_string(),
        external_url: ExternalUrl { scheme: "http".to_string(), host: addr.to_string(), path: "/".to_string() },
        ..Default::default()
    };
    let h = Handler::new(opts);
    h.apply_config(Arc::new(config::Config::default()));
    let (ctx, t) = start(&h);

    let base_url = format!("{}://{}", h.options().external_url.scheme, h.options().external_url.host);
    wait_for_server_ready(&base_url, Duration::from_secs(5));

    let start_gate = Arc::new(std::sync::Barrier::new(3));
    let mut ws = Vec::new();
    for _ in 0..3 {
        let g = start_gate.clone();
        let u = format!("{}/-/quit", base_url);
        ws.push(std::thread::spawn(move || {
            g.wait();
            let resp = post(&u, "");
            assert_eq!(200, resp.status);
        }));
    }
    for w in ws {
        w.join().expect("quit request");
    }

    // Stop the web handler.
    ctx.cancel();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !t.is_finished() {
        assert!(Instant::now() < deadline, "Server still running after 5 seconds.");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(t.join().expect("run thread").is_ok());
}

/// Test for availability of API endpoints in Prometheus Agent mode.
#[test]
fn test_agent_api_end_points() {
    let addr = "127.0.0.1:9985";
    let mut opts = base_options(addr, std::path::Path::new(""), None);
    opts.tsdb_dir = String::new();
    opts.is_agent = true;
    // adapted: Go uses a zero `scrape.Manager{}`; the Rust manager needs an appendable.
    let app: Arc<dyn storage::AppendableV2> = Arc::new(util::teststorage::Appendable::new());
    let sm = scrape::Manager::new(None, None, None, None, Some(app), None).expect("scrape manager");
    opts.scrape_pools_retriever = Some(sm.clone());
    opts.target_retriever = Some(sm);
    let h = Handler::new(opts);
    h.set_ready(ReadyStatus::Ready);
    h.apply_config(Arc::new(config::Config::default()));
    let (ctx, t) = start(&h);

    let base_url = format!("http://{}/api/v1", addr);
    wait_for_server_ready(&format!("http://{}", addr), Duration::from_secs(5));

    // Test for non-available endpoints in the Agent mode.
    let unavailable: [(&str, &[&str]); 14] = [
        ("/labels", &["GET", "POST"]),
        ("/label/:name/values", &["GET"]),
        ("/series", &["GET", "POST"]),
        ("/alertmanagers", &["GET"]),
        ("/query", &["GET", "POST"]),
        ("/query_range", &["GET", "POST"]),
        ("/query_exemplars", &["GET", "POST"]),
        ("/status/tsdb", &["GET"]),
        ("/status/tsdb/blocks", &["GET"]),
        ("/alerts", &["GET"]),
        ("/rules", &["GET"]),
        ("/admin/tsdb/delete_series", &["POST", "PUT"]),
        ("/admin/tsdb/clean_tombstones", &["POST", "PUT"]),
        ("/admin/tsdb/snapshot", &["POST", "PUT"]),
    ];
    for (path, methods) in unavailable {
        for m in methods {
            let resp = do_req(m, &format!("{}{}", base_url, path), b"");
            assert_eq!(422, resp.status, "{} {}", m, path);
        }
    }

    // Test for some of available endpoints in the Agent mode.
    for (path, methods) in [
        ("/targets", ["GET"]),
        ("/targets/metadata", ["GET"]),
        ("/metadata", ["GET"]),
        ("/status/config", ["GET"]),
        ("/status/runtimeinfo", ["GET"]),
        ("/status/flags", ["GET"]),
    ] {
        for m in methods {
            let resp = do_req(m, &format!("{}{}", base_url, path), b"");
            assert_eq!(200, resp.status, "{} {}: {}", m, path, String::from_utf8_lossy(&resp.body));
        }
    }
    stop(ctx, t);
}

#[test]
fn test_multiple_listen_addresses() {
    let db_dir = temp_dir("multiple_listen_addresses");
    let (db, adapter) = open_db(&db_dir);
    // Create multiple ports for testing multiple ListenAddresses.
    let (addr1, addr2) = ("127.0.0.1:9986", "127.0.0.1:9987");
    let mut opts = base_options(addr1, &db_dir, Some(adapter));
    opts.listen_addresses = vec![addr1.to_string(), addr2.to_string()];
    let h = Handler::new(opts);
    h.apply_config(Arc::new(config::Config::default()));
    let (ctx, t) = start(&h);

    wait_for_server_ready(&format!("http://{}", addr1), Duration::from_secs(5));

    // Set to ready.
    h.set_ready(ReadyStatus::Ready);

    for addr in [addr1, addr2] {
        let base_url = format!("http://{}", addr);
        let resp = get(&format!("{}/-/healthy", base_url));
        assert_eq!(200, resp.status);
        let resp = get(&format!("{}/-/ready", base_url));
        assert_eq!(200, resp.status);
    }
    stop(ctx, t);
    db.close().expect("close db");
}
