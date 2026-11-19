//! Shared helpers of the promtool tests: the testdata path, a mock HTTP server (Go `httptest.NewServer`),
//! the real TSDB as test storage (Go `teststorage.New` / `promqltest.LoadedStorage`) and temp dirs.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// The promtool binary (Go runs the test binary itself with `-test.main`).
pub const PROMTOOL: &str = env!("CARGO_BIN_EXE_promtool");

/// `upstream/cmd/promtool` (Go tests run there).
pub fn promtool_dir() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../upstream/cmd/promtool"))
}

static N: AtomicU64 = AtomicU64::new(0);

/// A new empty temporary directory (Go `t.TempDir()`), under the cargo target tmp dir.
pub fn temp_dir() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("promtool-test-{}-{}", std::process::id(), N.fetch_add(1, Ordering::SeqCst)));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

/// What the mock server saw (Go `*http.Request` after `ParseForm`).
#[derive(Clone, Debug, Default)]
pub struct SeenRequest {
    pub path: String,
    pub form: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
}

impl SeenRequest {
    /// Go `Form.Get`.
    pub fn form_get(&self, k: &str) -> String {
        self.form.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone()).unwrap_or_default()
    }
    /// Go `Header.Get`.
    pub fn header_get(&self, k: &str) -> String {
        self.headers.iter().find(|(n, _)| n.eq_ignore_ascii_case(k)).map(|(_, v)| v.clone()).unwrap_or_default()
    }
}

struct FnService<F>(F);

impl<F: Fn(&http::Request) -> http::Response + Send + Sync> http::server::Service for FnService<F> {
    fn handle(&self, req: http::Request) -> http::Response {
        (self.0)(&req)
    }
}

/// Go `mockServer(code, body)`: returns the server URL and a function giving the last request.
/// adapted: the server thread is not closed at the end of the test (the crates/http server runs forever).
pub fn mock_server(code: u16, body: &str) -> (String, impl Fn() -> SeenRequest) {
    let seen: Arc<Mutex<SeenRequest>> = Arc::new(Mutex::new(SeenRequest::default()));
    let s2 = seen.clone();
    let body = format!("{body}\n");
    let srv = http::server::Server::bind("127.0.0.1:0", http::server::ServerConfig::default()).expect("bind");
    let port = srv.local_port();
    let svc = FnService(move |r: &http::Request| {
        let mut form = r.query_pairs();
        if r.header("content-type").is_some_and(|c| c.starts_with("application/x-www-form-urlencoded")) {
            // Go ParseForm: body values first, then the query values.
            let mut f = r.form();
            f.extend(form);
            form = f;
        }
        *s2.lock().unwrap() = SeenRequest { path: http::url_decode(&r.path), form, headers: r.headers.clone() };
        http::Response::text(code, "text/plain; charset=utf-8", body.clone().into_bytes())
    });
    std::thread::spawn(move || srv.run(Arc::new(svc)));
    (format!("http://127.0.0.1:{port}"), move || seen.lock().unwrap().clone())
}

/// Go `teststorage.TestStorage`: the real TSDB in a temporary directory, closed and removed on drop.
pub struct TestStorage {
    pub db: Arc<tsdb::db::DB>,
    pub dir: PathBuf,
}

impl Drop for TestStorage {
    fn drop(&mut self) {
        let _ = self.db.close();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Go `promqltest.LoadedStorage(t, input)` over `teststorage.New(t)`.
pub fn loaded_storage(input: &str) -> TestStorage {
    let mut opts = tsdb::db::default_options();
    opts.min_block_duration = 24 * 3600 * 1000;
    opts.max_block_duration = 24 * 3600 * 1000;
    opts.retention_duration = 0;
    opts.out_of_order_time_window = 0;
    opts.enable_exemplar_storage = true;
    opts.max_exemplars = 100_000;
    let dir = temp_dir();
    let db = tsdb::db::open(&dir, None, Some(opts), None).expect("opening test storage");
    promql::promqltest::load_into(input, db.clone()).expect("loading storage");
    TestStorage { db, dir }
}
