//! Port of discovery/moby/mock_test.go: a mock Docker daemon that serves the fixtures under
//! `upstream/discovery/moby/testdata/<directory>` with Go `http.ServeMux` routing (including its redirect of a
//! subtree root without the trailing slash), and answers 429 to a second call of the same request URI.
//!
//! Use from a test file with `mod common; mod moby_mock;`.
#![allow(dead_code)]
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::common::{Req, Resp, TestServer, upstream};

/// Go `SDMock`.
pub struct SdMock {
    pub server: TestServer,
    pub directory: String,
}

/// Go `yaml.Unmarshal(rawHeaders, &map[string]string{})` for the flat `headers.yml` fixtures.
fn parse_headers(text: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for line in text.lines() {
        let Some((k, v)) = line.split_once(':') else { continue };
        let v = v.trim();
        let v = v.strip_prefix('"').and_then(|v| v.strip_suffix('"')).unwrap_or(v);
        out.push((k.trim().to_string(), v.to_string()));
    }
    out
}

/// Go `strutil.SanitizeLabelName`.
fn sanitize_label_name(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect()
}

/// Go `filepath.Join(a, b)` for the relative paths used here.
fn join(a: &str, b: &str) -> String {
    if b.is_empty() { a.to_string() } else { format!("{a}/{b}") }
}

impl SdMock {
    /// Go `NewSDMock(t, directory)` + `Setup()`.
    pub fn new(directory: &str) -> SdMock {
        let dir = upstream(&format!("discovery/moby/testdata/{directory}"));
        let headers = parse_headers(&std::fs::read_to_string(format!("{dir}/headers.yml")).unwrap());
        let mut prefix = "/".to_string();
        if let Some((_, v)) = headers.iter().find(|(k, _)| k == "Api-Version") {
            prefix.push_str(&format!("v{v}/"));
        }
        // (pattern, path element) in registration order.
        let patterns: Vec<(String, String)> = ["_ping", "networks/", "services/", "nodes/", "nodes", "services", "tasks", "containers/"]
            .iter()
            .map(|p| (if *p == "_ping" { format!("/{p}") } else { format!("{prefix}{p}") }, p.to_string()))
            .collect();
        let calls: Arc<Mutex<HashMap<String, usize>>> = Arc::default();
        let server = TestServer::new(move |r: &Req| {
            // Go ServeMux: an exact pattern, else the longest subtree pattern; a subtree root without its slash
            // redirects to the root.
            let path = r.path.as_str();
            let mut found: Option<&(String, String)> = patterns.iter().find(|(pat, _)| !pat.ends_with('/') && pat == path);
            if found.is_none() {
                if patterns.iter().any(|(pat, _)| pat.ends_with('/') && *pat == format!("{path}/")) {
                    let mut loc = format!("{path}/");
                    if !r.raw_query.is_empty() {
                        loc.push('?');
                        loc.push_str(&r.raw_query);
                    }
                    let body = if r.method == "GET" || r.method == "HEAD" { format!("<a href=\"{loc}\">Moved Permanently</a>.\n\n") } else { String::new() };
                    return Resp::new(301, body).header("Location", &loc).header("Content-Type", "text/html; charset=utf-8");
                }
                found = patterns.iter().filter(|(pat, _)| pat.ends_with('/') && path.starts_with(pat.as_str())).max_by_key(|(pat, _)| pat.len());
            }
            let Some((_, p)) = found else { return Resp::not_found() };

            // The discovery should only call each API endpoint once.
            {
                let mut c = calls.lock().unwrap();
                let n = c.entry(r.uri.clone()).or_insert(0);
                *n += 1;
                if *n != 1 {
                    return Resp::new(429, Vec::new());
                }
            }
            let parts: Vec<&str> = r.uri.split('/').collect();
            let last = parts[parts.len() - 1];
            let f = if let Some(stem) = p.strip_suffix('/') {
                join(stem, &sanitize_label_name(last))
            } else {
                let query: Vec<&str> = last.split('?').collect();
                let mut f = format!("{}.json", query[0]);
                if query.len() == 2 {
                    let h = crypto::sha1(query[1].as_bytes());
                    // Avoiding long filenames for Windows.
                    f.push_str("__");
                    f.push_str(&crypto::base64url(&h)[..10]);
                }
                f
            };
            if let Ok(b) = std::fs::read(format!("{dir}/{f}.json")) {
                let mut resp = Resp::new(200, b);
                for (k, v) in &headers {
                    resp = resp.header(k, v);
                }
                resp.header("content-type", "application/json")
            } else if let Ok(b) = std::fs::read(format!("{dir}/{f}")) {
                let mut resp = Resp::new(200, b);
                for (k, v) in &headers {
                    resp = resp.header(k, v);
                }
                resp
            } else {
                let mut resp = Resp::new(500, Vec::new());
                for (k, v) in &headers {
                    resp = resp.header(k, v);
                }
                resp
            }
        });
        SdMock { server, directory: directory.to_string() }
    }

    /// Go `Endpoint`: the server URL with a trailing slash.
    pub fn endpoint(&self) -> String {
        format!("{}/", self.server.url)
    }
}
