//! Port of `github.com/prometheus/common/route` on top of `julienschmidt/httprouter` v1.3.0 semantics: one route
//! tree per method (HEAD does not match GET routes), trailing-slash and cleaned-path redirects (301 for GET, 307
//! otherwise), automatic `OPTIONS` answers with `Allow`, 405 with `Allow`, and Go's `http.NotFound`.
//!
//! Difference: the fixed-path redirect only cleans the path (`CleanPath`); it does not search case-insensitively.

use std::collections::HashMap;
use std::sync::Arc;

use http::{Found, Request, Response};

/// A route handler.
pub type HandlerFn = Arc<dyn Fn(Request) -> Response + Send + Sync>;

/// Go `http.Error(w, msg, code)`.
pub fn http_error(msg: &str, code: u16) -> Response {
    let mut body = String::with_capacity(msg.len() + 1);
    body.push_str(msg);
    body.push('\n');
    Response::text(code, "text/plain; charset=utf-8", body).with_header("X-Content-Type-Options", "nosniff")
}

/// Go `http.NotFound`.
pub fn not_found() -> Response {
    http_error("404 page not found", 404)
}

/// Go `http.Redirect` for `req` to `url` (absolute path or full URL).
pub fn redirect(req: &Request, url: &str, code: u16) -> Response {
    let mut resp = Response::new(code);
    resp.set_header("Location", url);
    if req.method == "GET" || req.method == "HEAD" {
        let mut body = String::from("<a href=\"");
        body.push_str(&html_escape(url));
        body.push_str("\">");
        body.push_str(http::reason(code));
        body.push_str("</a>.\n\n");
        resp.set_header("Content-Type", "text/html; charset=utf-8");
        resp.body = http::Body::Bytes(body.into_bytes());
    }
    resp
}

/// Go `html.EscapeString` (the five characters `htmlEscaper` replaces).
pub fn html_escape(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => o.push_str("&amp;"),
            '\'' => o.push_str("&#39;"),
            '<' => o.push_str("&lt;"),
            '>' => o.push_str("&gt;"),
            '"' => o.push_str("&#34;"),
            c => o.push(c),
        }
    }
    o
}

/// httprouter `CleanPath`.
pub fn clean_path(p: &str) -> String {
    if p.is_empty() {
        return "/".to_string();
    }
    let trailing = p.len() > 1 && p.ends_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            s => parts.push(s),
        }
    }
    let mut out = String::with_capacity(p.len() + 1);
    out.push('/');
    out.push_str(&parts.join("/"));
    if trailing && out.len() > 1 {
        out.push('/');
    }
    out
}

/// Go `route.Router.WithInstrumentation`'s function: wraps a handler, given its handler name.
pub type InstrumentFn = Arc<dyn Fn(&str, HandlerFn) -> HandlerFn + Send + Sync>;

/// A router with a path prefix (Go `route.Router`).
#[derive(Default, Clone)]
pub struct Router {
    trees: HashMap<String, Arc<http::Router<HandlerFn>>>,
    pending: HashMap<String, Vec<(String, HandlerFn)>>,
    instr: Option<(String, InstrumentFn)>,
}

impl Router {
    pub fn new() -> Router {
        Router::default()
    }

    /// Go `WithInstrumentation`: routes added from now on are wrapped with `f`. The handler name is the route path
    /// without `strip` (Go's route prefix is not part of handler names).
    pub fn set_instrumentation(&mut self, strip: &str, f: InstrumentFn) {
        self.instr = Some((strip.to_string(), f));
    }

    /// Adds a route; `path` is the full path including any prefix.
    pub fn handle(&mut self, method: &str, path: &str, h: HandlerFn) {
        let h = match &self.instr {
            Some((strip, f)) => f(path.strip_prefix(strip.as_str()).unwrap_or(path), h),
            None => h,
        };
        self.pending.entry(method.to_string()).or_default().push((path.to_string(), h));
        let mut r = http::Router::new();
        for (p, h) in &self.pending[method] {
            r.add("GET", p, h.clone());
        }
        self.trees.insert(method.to_string(), Arc::new(r));
    }

    pub fn get(&mut self, path: &str, h: HandlerFn) {
        self.handle("GET", path, h);
    }
    pub fn post(&mut self, path: &str, h: HandlerFn) {
        self.handle("POST", path, h);
    }
    pub fn put(&mut self, path: &str, h: HandlerFn) {
        self.handle("PUT", path, h);
    }
    pub fn head(&mut self, path: &str, h: HandlerFn) {
        self.handle("HEAD", path, h);
    }
    pub fn options(&mut self, path: &str, h: HandlerFn) {
        self.handle("OPTIONS", path, h);
    }
    pub fn del(&mut self, path: &str, h: HandlerFn) {
        self.handle("DELETE", path, h);
    }

    fn lookup(&self, method: &str, path: &str) -> Option<(HandlerFn, Vec<(String, String)>)> {
        let t = self.trees.get(method)?;
        match t.find("GET", path) {
            Found::Ok(h, params) => Some((h.clone(), params)),
            _ => None,
        }
    }

    fn allowed(&self, path: &str, req_method: &str) -> String {
        let mut allowed: Vec<&str> = Vec::new();
        if path == "*" {
            if req_method.is_empty() {
                for m in self.trees.keys() {
                    if m != "OPTIONS" {
                        allowed.push(m);
                    }
                }
            } else {
                // Go returns the precomputed global list here.
                for m in self.trees.keys() {
                    if m != "OPTIONS" {
                        allowed.push(m);
                    }
                }
            }
        } else {
            for m in self.trees.keys() {
                if m == req_method || m == "OPTIONS" {
                    continue;
                }
                if self.lookup(m, path).is_some() {
                    allowed.push(m);
                }
            }
        }
        if allowed.is_empty() {
            return String::new();
        }
        allowed.push("OPTIONS");
        allowed.sort();
        allowed.join(", ")
    }

    /// Serves a request (Go `ServeHTTP`); `req.path` is the raw path.
    pub fn serve(&self, mut req: Request) -> Response {
        let path = http::util::path_decode(&req.path);
        if let Some((h, params)) = self.lookup(&req.method, &req.path) {
            req.params = params;
            return h(req);
        }
        if self.trees.contains_key(&req.method) && req.method != "CONNECT" && path != "/" {
            let code = if req.method == "GET" { 301 } else { 307 };
            let alt = if path.len() > 1 && path.ends_with('/') { path[..path.len() - 1].to_string() } else { format!("{}/", path) };
            if self.lookup(&req.method, &alt).is_some() {
                return redirect(&req, &with_query(&alt, &req.query), code);
            }
            let cleaned = clean_path(&path);
            if cleaned != path {
                if self.lookup(&req.method, &cleaned).is_some() {
                    return redirect(&req, &with_query(&cleaned, &req.query), code);
                }
                let alt2 = if cleaned.len() > 1 && cleaned.ends_with('/') { cleaned[..cleaned.len() - 1].to_string() } else { format!("{}/", cleaned) };
                if self.lookup(&req.method, &alt2).is_some() {
                    return redirect(&req, &with_query(&alt2, &req.query), code);
                }
            }
        }
        if req.method == "OPTIONS" {
            let allow = self.allowed(&path, "OPTIONS");
            if !allow.is_empty() {
                let mut r = Response::new(200);
                r.set_header("Allow", &allow);
                return r;
            }
        } else {
            let allow = self.allowed(&path, &req.method);
            if !allow.is_empty() {
                let mut r = http_error("Method Not Allowed", 405);
                r.set_header("Allow", &allow);
                return r;
            }
        }
        not_found()
    }
}

fn with_query(path: &str, query: &str) -> String {
    let mut p = path.replace(' ', "%20");
    if !query.is_empty() {
        p.push('?');
        p.push_str(query);
    }
    p
}
