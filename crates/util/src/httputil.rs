//! Port of `util/httputil`: response compression (`compression.go`), CORS headers (`cors.go`) and the request
//! origin that the query log records (`context.go`).
use http::{Body, Request, Response, Service};
use std::sync::Arc;

pub const ACCEPT_ENCODING_HEADER: &str = "Accept-Encoding";
pub const CONTENT_ENCODING_HEADER: &str = "Content-Encoding";
pub const GZIP_ENCODING: &str = "gzip";
pub const DEFLATE_ENCODING: &str = "deflate";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Encoding {
    Gzip,
    Deflate,
    Identity,
}

/// The first of `gzip` or `deflate` in `Accept-Encoding`, as Go's `newCompressedResponseWriter` picks it (names
/// must match exactly after trimming spaces; `q` values are not read).
fn pick_encoding(raw: &str) -> Encoding {
    for enc in raw.split(',') {
        match enc.trim() {
            GZIP_ENCODING => return Encoding::Gzip,
            DEFLATE_ENCODING => return Encoding::Deflate,
            _ => {}
        }
    }
    Encoding::Identity
}

fn remove_header(resp: &mut Response, name: &str) {
    resp.headers.retain(|h| !h.0.eq_ignore_ascii_case(name));
}

/// Compresses a response for a request as Go's `CompressionHandler` does: adds `Vary: Accept-Encoding`, and for
/// `gzip` or `deflate` (zlib) sets `Content-Encoding`, drops `Content-Length`, and compresses the body (an empty
/// body becomes an empty compressed stream).
/// `accept_encoding` is the request's `Accept-Encoding` header ("" when absent).
pub fn compress_response(accept_encoding: &str, mut resp: Response) -> Response {
    resp.headers.push(("Vary".to_string(), ACCEPT_ENCODING_HEADER.to_string()));
    let enc = pick_encoding(accept_encoding);
    if enc == Encoding::Identity {
        return resp;
    }
    let raw = match std::mem::replace(&mut resp.body, Body::Bytes(Vec::new())) {
        Body::Bytes(b) => b,
        Body::File(mut f, n) => {
            let mut b = Vec::with_capacity(n as usize);
            let _ = std::io::Read::read_to_end(&mut f, &mut b);
            b
        }
    };
    let out = match enc {
        // the server's own small encoder (it also fits the server's small thread stacks)
        Encoding::Gzip => http::gzip::gzip_vec(&raw),
        _ => zlib(&raw),
    };
    resp.set_header(CONTENT_ENCODING_HEADER, if enc == Encoding::Gzip { GZIP_ENCODING } else { DEFLATE_ENCODING });
    remove_header(&mut resp, "Content-Length");
    resp.body = Body::Bytes(out);
    resp
}

/// Go `CompressionHandler`: wraps a service and compresses its responses.
pub struct CompressionHandler {
    pub handler: Arc<dyn Service>,
}

impl Service for CompressionHandler {
    fn handle(&self, req: Request) -> Response {
        let accept = req.header(ACCEPT_ENCODING_HEADER).unwrap_or("").to_string();
        let resp = self.handler.handle(req);
        compress_response(&accept, resp)
    }
}

const CORS_HEADERS: [(&str, &str); 3] = [
    ("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type, Origin"),
    ("Access-Control-Allow-Methods", "GET, POST, OPTIONS"),
    ("Access-Control-Expose-Headers", "Date"),
];

/// Go `SetCORS`: sets the CORS headers when the request has an `Origin`. `o` is the anchored origin regex
/// (`^(?:<cfg>)$`); for `^(?:.*)$` the allowed origin is `*`.
pub fn set_cors(resp: &mut Response, o: &regex::Regex, req: &Request) {
    resp.headers.push(("Vary".to_string(), "Origin".to_string()));
    let origin = req.header("Origin").unwrap_or("");
    if origin.is_empty() {
        return;
    }
    for (k, v) in CORS_HEADERS {
        resp.set_header(k, v);
    }
    if o.as_str() == "^(?:.*)$" {
        resp.set_header("Access-Control-Allow-Origin", "*");
        return;
    }
    if o.is_match(origin) {
        resp.set_header("Access-Control-Allow-Origin", origin);
    }
}

/// Go `ContextFromRequest`: the origin the query log writes,
/// `{"httpRequest":{"clientIP":..,"method":..,"path":..}}`. `path` is the route path set with `ContextWithPath`
/// ("" when none).
pub fn origin_from_request(req: &Request, path: &str) -> json::Json {
    json::Json::obj().with(
        "httpRequest",
        json::Json::obj().with("clientIP", req.remote_ip.as_str()).with("method", req.method.as_str()).with("path", path),
    )
}

/// `data` as a zlib stream (RFC 1950: header, deflate, Adler-32), as Go's `zlib.NewWriter` writes it.
fn zlib(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() / 4 + 32);
    // CMF: deflate with a 32 KiB window; FLG: default level, check bits.
    out.extend_from_slice(&[0x78, 0x9c]);
    http::gzip::deflate(data, &mut out);
    let (mut a, mut b) = (1u32, 0u32);
    for chunk in data.chunks(5552) {
        for &x in chunk {
            a += x as u32;
            b += a;
        }
        a %= 65521;
        b %= 65521;
    }
    out.extend_from_slice(&((b << 16) | a).to_be_bytes());
    out
}
