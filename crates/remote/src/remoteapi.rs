//! Port of the receiving side of client_golang `exp/api/remote` (`remote_api.go` write handler and
//! `remote_headers.go`): message type negotiation from the content type, the snappy decode middleware, the
//! written-stats response headers and the status code rules. Also Go's `http.Error`.

use std::fmt;

use discovery::log::Logger;
use storage::{Context, Error};

use crate::codec::{snappy_decode, snappy_decoded_len};
use crate::stats::WriteResponseStats;

pub const VERSION_HEADER: &str = "X-Prometheus-Remote-Write-Version";
pub const VERSION1_HEADER_VALUE: &str = "0.1.0";
pub const VERSION20_HEADER_VALUE: &str = "2.0.0";
pub const APP_PROTO_CONTENT_TYPE: &str = "application/x-protobuf";

/// Go `SnappyBlockCompression`.
pub const SNAPPY_BLOCK_COMPRESSION: &str = "snappy";

/// Go `maxDecodedSize`: the limit of compressed and decompressed request bodies.
pub const MAX_DECODED_SIZE: usize = 32 * 1024 * 1024;

/// Go `WriteMessageType`: the protobuf message of a remote write request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WriteMessageType {
    /// `prometheus.WriteRequest` (remote write 1.0).
    V1,
    /// `io.prometheus.write.v2.Request` (remote write 2.0).
    V2,
}

impl WriteMessageType {
    pub fn as_str(self) -> &'static str {
        match self {
            WriteMessageType::V1 => "prometheus.WriteRequest",
            WriteMessageType::V2 => "io.prometheus.write.v2.Request",
        }
    }

    /// Go `WriteMessageType(s).Validate()`: the type with this name.
    pub fn parse(s: &str) -> Result<WriteMessageType, String> {
        match s {
            "prometheus.WriteRequest" => Ok(WriteMessageType::V1),
            "io.prometheus.write.v2.Request" => Ok(WriteMessageType::V2),
            _ => Err(format!(
                "unknown type for remote write protobuf message {}, supported: {}",
                s,
                MessageTypes(vec![WriteMessageType::V1, WriteMessageType::V2])
            )),
        }
    }

    /// Go `contentTypeHeader`.
    pub fn content_type_header(self) -> &'static str {
        match self {
            WriteMessageType::V1 => APP_PROTO_CONTENT_TYPE,
            WriteMessageType::V2 => "application/x-protobuf;proto=io.prometheus.write.v2.Request",
        }
    }
}

impl fmt::Display for WriteMessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Go `MessageTypes`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MessageTypes(pub Vec<WriteMessageType>);

impl MessageTypes {
    pub fn contains(&self, t: WriteMessageType) -> bool {
        self.0.contains(&t)
    }
    pub fn strings(&self) -> Vec<String> {
        self.0.iter().map(|t| t.as_str().to_string()).collect()
    }
}

impl fmt::Display for MessageTypes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.strings().join(", "))
    }
}

/// Go `ParseProtoMsg`: the message type from a content type such as
/// `application/x-protobuf;proto=io.prometheus.write.v2.Request` (no `proto` parameter means 1.0).
pub fn parse_proto_msg(content_type: &str) -> Result<WriteMessageType, String> {
    let content_type = content_type.trim();
    let parts: Vec<&str> = content_type.split(';').collect();
    if parts[0] != APP_PROTO_CONTENT_TYPE {
        return Err(format!(
            "expected {} as the first (media) part, got {} content-type",
            APP_PROTO_CONTENT_TYPE, content_type
        ));
    }
    for p in &parts[1..] {
        let pair: Vec<&str> = p.split('=').collect();
        if pair.len() != 2 {
            return Err(format!(
                "as per https://www.rfc-editor.org/rfc/rfc9110#parameter expected parameters to be key-values, got {} in {} content-type",
                p, content_type
            ));
        }
        if pair[0] == "proto" {
            return WriteMessageType::parse(pair[1]).map_err(|e| format!("got {} content type; {}", content_type, e));
        }
    }
    // No "proto=" parameter, assuming v1.
    Ok(WriteMessageType::V1)
}

/// Go `WriteResponse`: the stats, the status code (204 by default) and extra headers of a write response.
#[derive(Clone, Debug)]
pub struct WriteResponse {
    pub stats: WriteResponseStats,
    status_code: u16,
    extra_headers: Vec<(String, String)>,
}

impl Default for WriteResponse {
    fn default() -> Self {
        WriteResponse::new()
    }
}

impl WriteResponse {
    /// Go `NewWriteResponse`.
    pub fn new() -> WriteResponse {
        WriteResponse { stats: WriteResponseStats::default(), status_code: 204, extra_headers: Vec::new() }
    }
    /// Go `Stats`.
    pub fn stats(&self) -> WriteResponseStats {
        self.stats
    }
    /// Go `Add` (on the embedded stats).
    pub fn add(&mut self, rs: WriteResponseStats) {
        self.stats = self.stats.add(rs);
    }
    /// Go `SetStatusCode`. With a store error, a status below 400 becomes 500.
    pub fn set_status_code(&mut self, code: u16) {
        self.status_code = code;
    }
    pub fn status_code(&self) -> u16 {
        self.status_code
    }
    /// Go `SetExtraHeader` (replaces a header of the same name).
    pub fn set_extra_header(&mut self, key: &str, value: &str) {
        self.extra_headers.retain(|h| !h.0.eq_ignore_ascii_case(key));
        self.extra_headers.push((key.to_string(), value.to_string()));
    }

    /// Go `writeHeaders`: stats headers (not for 1.0 messages) and extra headers.
    fn write_headers(&self, msg_type: WriteMessageType, resp: &mut http::Response) {
        if msg_type != WriteMessageType::V1 {
            for (k, v) in self.stats.headers() {
                resp.set_header(k, &v);
            }
        }
        for (k, v) in &self.extra_headers {
            resp.headers.push((k.clone(), v.clone()));
        }
    }
}

/// Go `writeStorage`: stores a decoded request. The request body is decompressed and `Content-Encoding` is
/// removed by the snappy middleware.
pub trait WriteStorage: Send + Sync {
    fn store(&self, ctx: &Context, req: &http::Request, msg_type: WriteMessageType) -> (Option<WriteResponse>, Option<Error>);
}

/// Go `http.Error`: a text/plain error response (the message plus a newline). Headers already in `resp` stay,
/// except Content-Length.
pub fn http_error_into(resp: &mut http::Response, msg: &str, code: u16) {
    resp.headers.retain(|h| !h.0.eq_ignore_ascii_case("Content-Length"));
    resp.set_header("Content-Type", "text/plain; charset=utf-8");
    resp.set_header("X-Content-Type-Options", "nosniff");
    resp.status = code;
    let mut body = Vec::with_capacity(msg.len() + 1);
    body.extend_from_slice(msg.as_bytes());
    body.push(b'\n');
    resp.body = http::Body::Bytes(body);
}

/// Go `http.Error` on a fresh response.
pub fn http_error(msg: &str, code: u16) -> http::Response {
    let mut r = http::Response::new(code);
    http_error_into(&mut r, msg, code);
    r
}

/// Go `writeHandler` with the default middleware (`SnappyDecodeMiddleware`).
pub struct WriteHandler {
    store: Box<dyn WriteStorage>,
    accepted_message_types: MessageTypes,
    logger: Logger,
    middleware_logger: Logger,
}

/// Go `NewWriteHandler(store, acceptedMessageTypes, WithWriteHandlerLogger(logger))`. The default snappy
/// middleware logs nothing, as in Go.
pub fn new_write_handler(store: Box<dyn WriteStorage>, accepted_message_types: MessageTypes, logger: Logger) -> WriteHandler {
    WriteHandler { store, accepted_message_types, logger, middleware_logger: Logger::nop() }
}

impl WriteHandler {
    /// Serves one request: the snappy middleware, then the handler.
    pub fn serve(&self, ctx: &Context, mut r: http::Request) -> http::Response {
        // SnappyDecodeMiddleware.
        let logger = &self.middleware_logger;
        let enc = r.header("content-encoding").unwrap_or("").to_string();
        if !enc.is_empty() && enc != SNAPPY_BLOCK_COMPRESSION {
            let err = format!(
                "{} encoding (compression) is not accepted by this server; only {} is acceptable",
                enc, SNAPPY_BLOCK_COMPRESSION
            );
            logger.error("Error decoding remote write request", &[("err", &err)]);
            return http_error(&err, 415);
        }
        if r.body.len() > MAX_DECODED_SIZE {
            logger.error("Request body exceeds the limit", &[("limitBytes", &MAX_DECODED_SIZE.to_string())]);
            return http_error(&format!("request body exceeds the {} bytes limit", MAX_DECODED_SIZE), 413);
        }
        let decoded_size = match snappy_decoded_len(&r.body) {
            Ok(n) => n,
            Err(e) => {
                logger.error("Error snappy decoding request body length", &[("err", &e.to_string())]);
                return http_error(&e.to_string(), 400);
            }
        };
        if decoded_size > MAX_DECODED_SIZE {
            logger.error(
                "Snappy decoded size exceeds the limit",
                &[("sizeBytes", &decoded_size.to_string()), ("limitBytes", &MAX_DECODED_SIZE.to_string())],
            );
            return http_error(&format!("decoded size exceeds the {} bytes limit", MAX_DECODED_SIZE), 400);
        }
        match snappy_decode(&r.body) {
            Ok(d) => r.body = d,
            Err(e) => {
                logger.error("Error snappy decoding remote write request", &[("err", &e.to_string())]);
                return http_error(&e.to_string(), 400);
            }
        }
        r.headers.retain(|h| !h.0.eq_ignore_ascii_case("content-encoding"));

        self.serve_decoded(ctx, &r)
    }

    /// Go `writeHandler.ServeHTTP` (after the middleware).
    fn serve_decoded(&self, ctx: &Context, r: &http::Request) -> http::Response {
        if r.method != "POST" {
            return http_error("Method not allowed", 405);
        }
        let mut content_type = r.header("content-type").unwrap_or("");
        if content_type.is_empty() {
            content_type = APP_PROTO_CONTENT_TYPE;
        }
        let msg_type = match parse_proto_msg(content_type) {
            Ok(t) => t,
            Err(e) => {
                self.logger.error("Error decoding remote write request", &[("err", &e)]);
                return http_error(&e, 415);
            }
        };
        if !self.accepted_message_types.contains(msg_type) {
            let err = format!(
                "{} protobuf message is not accepted by this server; only accepts {}",
                msg_type, self.accepted_message_types
            );
            self.logger.error("Unaccepted message type", &[("msgType", msg_type.as_str()), ("err", &err)]);
            return http_error(&err, 415);
        }

        let (write_response, store_err) = self.store.store(ctx, r, msg_type);
        // A missing write response means that nothing was written.
        let mut write_response = write_response.unwrap_or_default();

        let mut resp = http::Response::new(204);
        write_response.write_headers(msg_type, &mut resp);

        if let Some(err) = store_err {
            // A storage error must never be reported as success, else the sender drops the data.
            if write_response.status_code < 400 {
                write_response.set_status_code(500);
            }
            if write_response.status_code / 100 == 5 {
                self.logger.error("Error while storing the remote write request", &[("err", &err.to_string())]);
            }
            http_error_into(&mut resp, &err.to_string(), write_response.status_code);
            return resp;
        }
        if write_response.status_code == 0 {
            write_response.set_status_code(204);
        }
        resp.status = write_response.status_code;
        resp
    }
}
