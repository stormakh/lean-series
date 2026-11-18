//! Port of discovery/digitalocean/mock_test.go: an httptest server with the droplets and databases handlers.
//!
//! The response bodies are the raw string literals of the Go file, read from `upstream/` (byte-identical).
#![allow(dead_code)]
use std::sync::{Arc, Mutex};

use crate::common::{self, Req, Resp, TestServer};

/// The raw string literals (`` `...` ``) of a Go source file, in order.
pub fn go_raw_strings(rel: &str) -> Vec<String> {
    let src = String::from_utf8(common::read_upstream(rel)).unwrap();
    src.split('`')
        .skip(1)
        .step_by(2)
        .map(str::to_string)
        .collect()
}

type Route = (String, Arc<dyn Fn(&Req) -> Resp + Send + Sync>);

pub const TOKEN_ID: &str = "3c9a75a2-24fd-4508-b4f2-11f18aa97411";

/// Go `SDMock`: a server with a mux of exact paths (Go `http.ServeMux`).
pub struct SdMock {
    pub server: Option<TestServer>,
    routes: Arc<Mutex<Vec<Route>>>,
    bodies: Vec<String>,
}

impl SdMock {
    /// Go `NewSDMock` + `Setup`.
    pub fn new() -> SdMock {
        let routes: Arc<Mutex<Vec<Route>>> = Arc::default();
        let r2 = routes.clone();
        let server = TestServer::new(move |r: &Req| {
            let h = r2
                .lock()
                .unwrap()
                .iter()
                .find(|(p, _)| *p == r.path)
                .map(|(_, h)| h.clone());
            match h {
                Some(h) => h(r),
                None => Resp::not_found(),
            }
        });
        SdMock {
            server: Some(server),
            routes,
            bodies: go_raw_strings("discovery/digitalocean/mock_test.go"),
        }
    }

    /// Go `Endpoint`.
    pub fn endpoint(&self) -> String {
        format!("{}/", self.server.as_ref().unwrap().url)
    }

    /// Go `ShutdownServer`.
    pub fn shutdown_server(&mut self) {
        self.server = None;
    }

    fn handle(&self, path: &str, h: impl Fn(&Req) -> Resp + Send + Sync + 'static) {
        self.routes
            .lock()
            .unwrap()
            .push((path.to_string(), Arc::new(h)));
    }

    /// Go `HandleDropletsList`.
    pub fn handle_droplets_list(&self) {
        let pages = [self.bodies[0].clone(), self.bodies[1].clone()];
        self.handle("/v2/droplets", move |r: &Req| {
            if r.header("Authorization") != format!("Bearer {TOKEN_ID}") {
                return Resp::new(403, "");
            }
            let mut page = 1usize;
            if r.has_query("page") {
                page = r.query_values("page")[0].parse().unwrap();
            }
            Resp::new(202, pages[page - 1].clone())
                .header("Content-Type", "application/json; charset=utf-8")
                .header("Ratelimit-Limit", "1200")
                .header("Ratelimit-Remaining", "965")
                .header("Ratelimit-Reset", "1415984218")
        });
    }

    /// Go `HandleDatabasesList`.
    pub fn handle_databases_list(&self) {
        let bodies = [
            self.bodies[2].clone(),
            self.bodies[3].clone(),
            self.bodies[4].clone(),
        ];
        self.handle("/v2/databases", move |r: &Req| match r.query_get("page") {
            "1" | "" => Resp::new(200, bodies[0].clone()),
            "2" => Resp::new(200, bodies[1].clone()),
            _ => Resp::new(200, bodies[2].clone()),
        });
    }
}
