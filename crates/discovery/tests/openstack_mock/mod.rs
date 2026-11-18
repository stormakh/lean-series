//! Port of discovery/openstack/mock_test.go: an OpenStack API mock on a local port. The JSON bodies are read from
//! the upstream mock_test.go (its Go string literals), so they are the same bytes. Use with `mod cloud_mock;` and
//! `mod openstack_mock;` in a test file.
#![allow(dead_code)]
use std::sync::{Arc, Mutex};

use crate::cloud_mock::{MockRequest, MockResponse, MockServer};

pub const TOKEN_ID: &str = "cbc36478b0bd8e67e89469c7749d4127";

type Route = Arc<dyn Fn(&MockRequest) -> MockResponse + Send + Sync>;

/// Go `SDMock`: a server with an `http.ServeMux` (exact paths, "/" catches the rest).
pub struct SdMock {
    server: Option<MockServer>,
    routes: Arc<Mutex<Vec<(String, Route)>>>,
    /// Failed checks of `testMethod` / `testHeader` in handlers (the test fails when the mock is dropped).
    failures: Arc<Mutex<Vec<String>>>,
}

fn mock_source() -> String {
    std::fs::read_to_string(format!("{}/../../upstream/discovery/openstack/mock_test.go", env!("CARGO_MANIFEST_DIR"))).unwrap()
}

/// The raw string literal (between backticks) that follows `marker` in mock_test.go.
fn go_literal(marker: &str) -> String {
    let src = mock_source();
    let at = src.find(marker).unwrap_or_else(|| panic!("{marker} not in mock_test.go"));
    let rest = &src[at..];
    let start = rest.find('`').unwrap() + 1;
    let end = start + rest[start..].find('`').unwrap();
    rest[start..end].to_string()
}

/// Go `fmt.Sprintf` with `%s` verbs only.
fn sprintf(f: &str, args: &[&str]) -> String {
    let mut out = String::new();
    let mut it = args.iter();
    let mut parts = f.split("%s");
    out.push_str(parts.next().unwrap());
    for p in parts {
        out.push_str(it.next().unwrap());
        out.push_str(p);
    }
    out
}

impl SdMock {
    /// Go `NewSDMock` + `Setup`.
    pub fn new() -> SdMock {
        let routes: Arc<Mutex<Vec<(String, Route)>>> = Arc::new(Mutex::new(Vec::new()));
        let r = routes.clone();
        let server = MockServer::from_fn(move |req| {
            let routes = r.lock().unwrap();
            let h = routes.iter().find(|(p, _)| *p == req.path).or_else(|| routes.iter().find(|(p, _)| p == "/")).map(|(_, h)| h.clone());
            drop(routes);
            match h {
                Some(h) => h(req),
                None => MockResponse::status(404, "404 page not found\n"),
            }
        });
        SdMock { server: Some(server), routes, failures: Arc::new(Mutex::new(Vec::new())) }
    }

    /// Go `Endpoint`.
    pub fn endpoint(&self) -> String {
        format!("{}/", self.server.as_ref().unwrap().url)
    }

    fn handle(&self, path: &str, h: impl Fn(&MockRequest) -> MockResponse + Send + Sync + 'static) {
        self.routes.lock().unwrap().push((path.to_string(), Arc::new(h)));
    }

    /// A GET handler that checks the method and the token (Go `testMethod`, `testHeader`) and serves `body` as JSON.
    fn handle_list(&self, path: &str, body: String) {
        let failures = self.failures.clone();
        self.handle(path, move |r| {
            if r.method != "GET" {
                failures.lock().unwrap().push(format!("Unexpected request method: {}", r.method));
            }
            if r.header("X-Auth-Token") != TOKEN_ID {
                failures.lock().unwrap().push(format!("Unexpected value for request header X-Auth-Token: {:?}", r.header("X-Auth-Token")));
            }
            MockResponse::json(body.clone())
        });
    }

    /// Go `HandleVersionsSuccessfully`.
    pub fn handle_versions_successfully(&self) {
        let ep = self.endpoint();
        let body = sprintf(&go_literal("m.Mux.HandleFunc(\"/\","), &[&format!("{ep}v3/"), &format!("{ep}v2.0/")]);
        // Go's ResponseWriter sniffs the type of this body as text/plain.
        self.handle("/", move |_| MockResponse::new(200, "text/plain; charset=utf-8", body.clone()));
    }

    /// Go `HandleAuthSuccessfully`.
    pub fn handle_auth_successfully(&self) {
        let ep = self.endpoint();
        let body = sprintf(&go_literal("m.Mux.HandleFunc(\"/v3/auth/tokens\","), &[&ep, &ep, &ep]);
        self.handle("/v3/auth/tokens", move |_| MockResponse::new(201, "text/plain; charset=utf-8", body.clone()).header("X-Subject-Token", TOKEN_ID));
    }

    pub fn handle_hypervisor_list_successfully(&self) {
        self.handle_list("/os-hypervisors/detail", go_literal("const hypervisorListBody ="));
    }

    pub fn handle_server_list_successfully(&self) {
        self.handle_list("/servers/detail", go_literal("const serverListBody ="));
    }

    pub fn handle_floating_ip_list_successfully(&self) {
        self.handle_list("/v2.0/floatingips", go_literal("const listOutput ="));
    }

    pub fn handle_ports_list_successfully(&self) {
        self.handle_list("/v2.0/ports", go_literal("const portsListBody ="));
    }

    pub fn handle_load_balancer_list_successfully(&self) {
        self.handle_list("/v2.0/lbaas/loadbalancers", go_literal("const lbListBody ="));
    }

    pub fn handle_listeners_list_successfully(&self) {
        self.handle_list("/v2.0/lbaas/listeners", go_literal("const listenerListBody ="));
    }
}

impl Drop for SdMock {
    fn drop(&mut self) {
        // Go `t.Cleanup(m.Server.Close)`
        self.server.take();
        let f = self.failures.lock().unwrap();
        if !f.is_empty() && !std::thread::panicking() {
            panic!("mock handler checks failed: {f:?}");
        }
    }
}
