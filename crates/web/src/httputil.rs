//! Port of the parts of `util/httputil` the web layer uses: CORS headers and response compression.

use std::io::Write;

use http::{Body, Request, Response};

const CORS_HEADERS: [(&str, &str); 3] = [
    ("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type, Origin"),
    ("Access-Control-Allow-Methods", "GET, POST, OPTIONS"),
    ("Access-Control-Expose-Headers", "Date"),
];

/// A CORS origin pattern (Go `*regexp.Regexp` anchored as `^(?:...)$`).
#[derive(Clone, Debug)]
pub struct CorsOrigin {
    /// The anchored source (Go `o.String()`).
    pub source: String,
    re: model::regexp::Regexp,
}

impl CorsOrigin {
    /// Compiles `^(?:pattern)$` with Go regexp semantics.
    pub fn new(pattern: &str) -> Result<CorsOrigin, String> {
        let source = format!("^(?:{})$", pattern);
        let re = model::regexp::Regexp::compile(&source).map_err(|e| e.to_string())?;
        Ok(CorsOrigin { source, re })
    }

    pub fn is_match(&self, s: &str) -> bool {
        self.re.is_match(s)
    }
}

/// Go `httputil.SetCORS`: CORS headers for `req` added to `resp`.
pub fn set_cors(resp: &mut Response, o: Option<&CorsOrigin>, req: &Request) {
    resp.headers.push(("Vary".to_string(), "Origin".to_string()));
    let origin = req.header("origin").unwrap_or("");
    if origin.is_empty() {
        return;
    }
    for (k, v) in CORS_HEADERS {
        resp.set_header(k, v);
    }
    let Some(o) = o else {
        // Go would panic on a nil regexp; treat it as no match.
        return;
    };
    if o.source == "^(?:.*)$" {
        resp.set_header("Access-Control-Allow-Origin", "*");
        return;
    }
    if o.is_match(origin) {
        resp.set_header("Access-Control-Allow-Origin", origin);
    }
}

/// The encoding Go's `newCompressedResponseWriter` picks: the first of "gzip" or "deflate" in Accept-Encoding.
pub fn pick_encoding(req: &Request) -> Option<&'static str> {
    let raw = req.header("accept-encoding").unwrap_or("");
    for enc in raw.split(',') {
        match enc.trim() {
            "gzip" => return Some("gzip"),
            "deflate" => return Some("deflate"),
            _ => {}
        }
    }
    None
}

/// Go `httputil.CompressionHandler`: compresses the body with gzip or deflate when the client accepts it, and adds
/// `Vary: Accept-Encoding`. Streaming bodies are left as they are.
pub fn compress(resp: &mut Response, req: &Request) {
    resp.headers.push(("Vary".to_string(), "Accept-Encoding".to_string()));
    let Some(enc) = pick_encoding(req) else { return };
    if resp.header("content-encoding").is_some() {
        return;
    }
    let data = match &resp.body {
        Body::Bytes(b) => b,
        _ => return,
    };
    let out = match enc {
        "gzip" => {
            let mut w = flate2::write::GzEncoder::new(Vec::with_capacity(data.len() / 4 + 64), flate2::Compression::default());
            let _ = w.write_all(data);
            w.finish().unwrap_or_default()
        }
        _ => {
            let mut w = flate2::write::ZlibEncoder::new(Vec::with_capacity(data.len() / 4 + 64), flate2::Compression::default());
            let _ = w.write_all(data);
            w.finish().unwrap_or_default()
        }
    };
    resp.set_header("Content-Encoding", enc);
    resp.body = Body::Bytes(out);
}

/// Go `httputil.ContextFromRequest`: a child of `ctx` with the "httpRequest" query origin (client IP, method and
/// path) that the query logger writes.
pub fn context_from_request(ctx: &storage::Context, client_ip: &str, method: &str, path: &str) -> storage::Context {
    use promql::LogValue;
    promql::OriginContext::attach(
        ctx,
        vec![(
            "httpRequest".to_string(),
            LogValue::Map(vec![
                ("clientIP".to_string(), LogValue::Str(client_ip.to_string())),
                ("method".to_string(), LogValue::Str(method.to_string())),
                ("path".to_string(), LogValue::Str(path.to_string())),
            ]),
        )],
    )
}
