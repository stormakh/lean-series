//! Port of `web/ui` (the asset file system, Go `ui.Assets`) and the static file serving of `web.go`
//! (`server.StaticFileServer` over `http.FileServer`).
//!
//! The upstream `web/ui/mantine-ui` build (made by `scripts/build-ui.sh`) is embedded at compile time from
//! `crates/web/ui/static`, under the same paths as Go's asset file system ("/static/mantine-ui/..."). A directory
//! can replace the embedded files (Go builds without `builtinassets` read `web/ui/static` from disk).
//!
//! Differences: no `Range` support (Go's embedded assets answer ranges with 416 anyway) and no `Last-Modified`
//! (Go's embedded assets have no modification time).

use std::path::PathBuf;

use http::{Request, Response};

use crate::route::{not_found, redirect};

mod embedded {
    include!(concat!(env!("OUT_DIR"), "/ui_assets.rs"));
}

/// The assets the UI is served from (Go `ui.Assets`).
#[derive(Clone, Debug, Default)]
pub struct Assets {
    /// When set, files are read from this directory (it holds "static/..."), else the embedded files are used.
    pub dir: Option<PathBuf>,
}

/// A looked-up asset path.
pub enum Asset {
    File(Vec<u8>),
    Dir,
}

impl Assets {
    /// The embedded assets.
    pub fn embedded() -> Assets {
        Assets { dir: None }
    }

    /// Assets read from `dir` (which holds "static/mantine-ui/...").
    pub fn from_dir(dir: impl Into<PathBuf>) -> Assets {
        Assets { dir: Some(dir.into()) }
    }

    /// Opens `path` ("/static/mantine-ui/index.html"). `None` when it does not exist.
    pub fn open(&self, path: &str) -> Option<Asset> {
        let path = clean(path);
        match &self.dir {
            Some(d) => {
                let p = http::staticfile::safe_join(d, path.trim_start_matches('/'))?;
                let meta = std::fs::metadata(&p).ok()?;
                if meta.is_dir() {
                    return Some(Asset::Dir);
                }
                // Go's filter leaves out source maps and the old bootstrap files.
                if path.ends_with("map.js") {
                    return None;
                }
                std::fs::read(&p).ok().map(Asset::File)
            }
            None => {
                if let Some((_, b)) = embedded::ASSETS.iter().find(|(p, _)| *p == path) {
                    return Some(Asset::File(b.to_vec()));
                }
                let dir_prefix = if path.ends_with('/') { path.clone() } else { format!("{}/", path) };
                if path == "/" || embedded::ASSETS.iter().any(|(p, _)| p.starts_with(&dir_prefix)) {
                    return Some(Asset::Dir);
                }
                None
            }
        }
    }

    /// Whether any file is embedded or the directory exists.
    pub fn is_empty(&self) -> bool {
        match &self.dir {
            Some(d) => !d.exists(),
            None => embedded::ASSETS.is_empty(),
        }
    }

    /// Go `server.StaticFileServer(ui.Assets)` for the file at `path`.
    pub fn serve(&self, req: &Request, path: &str) -> Response {
        // Go's FileServer redirects ".../index.html" to "./".
        if path.ends_with("/index.html") {
            return redirect(req, "./", 301);
        }
        match self.open(path) {
            None => not_found(),
            Some(Asset::Dir) => {
                // A directory without a trailing slash: Go redirects to "<base>/" (relative).
                let base = path.trim_end_matches('/').rsplit('/').next().unwrap_or("");
                let mut r = Response::new(301);
                r.set_header("Location", &format!("{}/", base));
                r
            }
            Some(Asset::File(b)) => {
                let mut r = Response::new(200);
                r.headers.push(("Accept-Ranges".to_string(), "bytes".to_string()));
                r.headers.push(("Content-Type".to_string(), content_type(path).to_string()));
                r.body = http::Body::Bytes(b);
                r
            }
        }
    }
}

/// Go `path.Clean` for a rooted path.
fn clean(p: &str) -> String {
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
    format!("/{}", parts.join("/"))
}

/// The content type: `server.StaticFileServer`'s table, then Go's `mime.TypeByExtension` for the rest.
pub fn content_type(path: &str) -> &'static str {
    let ext = match path.rsplit_once('.') {
        Some((_, e)) if !e.contains('/') => e,
        _ => "",
    };
    match ext {
        "cjs" | "js" => "application/javascript",
        "css" => "text/css",
        "eot" => "font/eot",
        "gif" => "image/gif",
        "ico" => "image/x-icon",
        "jpg" => "image/jpeg",
        "json" | "map" => "application/json",
        "less" | "txt" => "text/plain",
        "otf" => "font/otf",
        "png" => "image/png",
        "svg" => "image/svg+xml",
        "ttf" => "font/ttf",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "html" | "htm" => "text/html; charset=utf-8",
        "webp" => "image/webp",
        "wasm" => "application/wasm",
        "xml" => "text/xml; charset=utf-8",
        "pdf" => "application/pdf",
        _ => "application/octet-stream",
    }
}

/// Go `serveReactApp`'s placeholder replacements in `index.html`.
pub struct IndexPlaceholders<'a> {
    pub consoles_link: &'a str,
    pub title: &'a str,
    pub agent_mode: bool,
    pub ready: bool,
    pub lookback_delta: &'a str,
}

/// Replaces the placeholders of `index.html` as `web.go` does (in the same order).
pub fn replace_placeholders(idx: &[u8], p: &IndexPlaceholders<'_>) -> Vec<u8> {
    let s = String::from_utf8_lossy(idx);
    let s = s.replace("CONSOLES_LINK_PLACEHOLDER", p.consoles_link);
    let s = s.replace("TITLE_PLACEHOLDER", p.title);
    let s = s.replace("AGENT_MODE_PLACEHOLDER", if p.agent_mode { "true" } else { "false" });
    let s = s.replace("READY_PLACEHOLDER", if p.ready { "true" } else { "false" });
    let s = s.replace("LOOKBACKDELTA_PLACEHOLDER", p.lookback_delta);
    s.into_bytes()
}
