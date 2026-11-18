//! Port of discovery/hetzner/mock_test.go: an httptest server with the hcloud and robot handlers.
//!
//! The response bodies are the raw string literals of the Go file, read from `upstream/` (so they stay
//! byte-identical to upstream).
#![allow(dead_code)]
use std::sync::{Arc, Mutex};

use crate::common::{self, Req, Resp, TestServer};

/// The raw string literals (`` `...` ``) of a Go source file, in order.
pub fn go_raw_strings(rel: &str) -> Vec<String> {
    let src = String::from_utf8(common::read_upstream(rel)).unwrap();
    src.split('`').skip(1).step_by(2).map(str::to_string).collect()
}

/// Go `path.Clean` as ServeMux uses it (keeps a trailing slash).
pub fn clean_path(p: &str) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for s in p.split('/') {
        match s {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            s => parts.push(s),
        }
    }
    let mut out = format!("/{}", parts.join("/"));
    if p.ends_with('/') && out != "/" {
        out.push('/');
    }
    out
}

type Route =(String, Arc<dyn Fn(&Req) -> Resp + Send + Sync>);

/// Go `SDMock`: a server with a mux of exact paths (Go `http.ServeMux`).
pub struct SdMock {
    pub server: TestServer,
    routes: Arc<Mutex<Vec<Route>>>,
    bodies: Vec<String>,
}

pub const HCLOUD_TEST_TOKEN: &str = "LRK9DAWQ1ZAEFSrCNEEzLCUwhYX1U3g7wMg4dTlkkDC96fyDuyJ39nVbVjCKSDfj";
pub const ROBOT_TEST_USERNAME: &str = "my-hetzner";
pub const ROBOT_TEST_PASSWORD: &str = "my-password";

impl SdMock {
    /// Go `NewSDMock` + `Setup`.
    pub fn new() -> SdMock {
        let routes: Arc<Mutex<Vec<Route>>> = Arc::default();
        let r2 = routes.clone();
        let server = TestServer::new(move |r: &Req| {
            // Go ServeMux: a path that is not clean gets a 301 to the clean path.
            let clean = clean_path(&r.path);
            if clean != r.path {
                let loc = if r.raw_query.is_empty() { clean } else { format!("{clean}?{}", r.raw_query) };
                return Resp::new(301, "").header("Location", &loc);
            }
            let h =r2.lock().unwrap().iter().find(|(p, _)| *p == r.path).map(|(_, h)| h.clone());
            match h {
                Some(h) => h(r),
                None => Resp::not_found(),
            }
        });
        SdMock { server, routes, bodies: go_raw_strings("discovery/hetzner/mock_test.go") }
    }

    /// Go `Endpoint`.
    pub fn endpoint(&self) -> String {
        format!("{}/", self.server.url)
    }

    fn handle(&self, path: &str, h: impl Fn(&Req) -> Resp + Send + Sync + 'static) {
        self.routes.lock().unwrap().push((path.to_string(), Arc::new(h)));
    }

    fn hcloud(&self, path: &str, body: String) {
        self.handle(path, move |r: &Req| {
            if r.header("Authorization") != format!("Bearer {HCLOUD_TEST_TOKEN}") {
                return Resp::new(401, "");
            }
            Resp::new(200, body.clone()).header("Content-Type", "application/json; charset=utf-8")
        });
    }

    /// Go `HandleHcloudServers`.
    pub fn handle_hcloud_servers(&self) {
        self.hcloud("/servers", self.bodies[0].clone());
    }

    /// Go `HandleHcloudNetworks`.
    pub fn handle_hcloud_networks(&self) {
        self.hcloud("/networks", self.bodies[1].clone());
    }

    /// Go `HandleRobotServers`.
    pub fn handle_robot_servers(&self) {
        let body = self.bodies[2].clone();
        self.handle("/server", move |r: &Req| {
            let (username, password, ok) = match r.basic_auth() {
                Some((u, p)) => (u, p, true),
                None => (String::new(), String::new(), false),
            };
            if username != ROBOT_TEST_USERNAME && password != ROBOT_TEST_PASSWORD && !ok {
                return Resp::new(401, "");
            }
            Resp::new(200, body.clone()).header("Content-Type", "application/json; charset=utf-8")
        });
    }
}
