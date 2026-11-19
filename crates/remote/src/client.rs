//! Port of `storage/remote/client.go`: the remote write and remote read HTTP clients.

use std::fmt;
use std::sync::Arc;

use storage::{BoxSeriesSet, Context, Error};

use crate::prompb::{Query, ResponseType};
pub use crate::stats::{parse_write_response_stats, WriteResponseStats};

pub const MAX_ERR_MSG_LEN: usize = 1024;
pub const REMOTE_WRITE_VERSION_HEADER: &str = "X-Prometheus-Remote-Write-Version";
pub const REMOTE_WRITE_VERSION1_HEADER_VALUE: &str = "0.1.0";
pub const REMOTE_WRITE_VERSION20_HEADER_VALUE: &str = "2.0.0";
pub const APP_PROTO_CONTENT_TYPE: &str = "application/x-protobuf";

/// Go `AcceptedResponseTypes`.
pub const ACCEPTED_RESPONSE_TYPES: [ResponseType; 2] = [ResponseType::STREAMED_XOR_CHUNKS, ResponseType::SAMPLES];

/// Go `RecoverableError`: the send may be retried, after `retry_after` when it is positive.
/// It travels as `storage::Error::Other`; find it with [`as_recoverable`] (Go `errors.As`).
#[derive(Clone, Debug)]
pub struct RecoverableError {
    pub err: Error,
    /// Go `model.Duration` in nanoseconds; may be negative (a Retry-After date in the past).
    pub retry_after: i64,
}

impl fmt::Display for RecoverableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.err)
    }
}

impl std::error::Error for RecoverableError {}

impl RecoverableError {
    pub fn new(err: Error, retry_after: i64) -> Error {
        Error::Other(Arc::new(RecoverableError { err, retry_after }))
    }
}

/// Go `errors.As(err, &RecoverableError{})`.
pub fn as_recoverable(err: &Error) -> Option<&RecoverableError> {
    let mut cur = err;
    loop {
        match cur {
            Error::Other(e) => return e.downcast_ref::<RecoverableError>(),
            Error::Wrapped(w) => cur = &w.1,
            _ => return None,
        }
    }
}

/// Go `WriteClient` (declared in `queue_manager.go`).
pub trait WriteClient: Send + Sync {
    /// Stores the encoded (compressed) request.
    fn store(&self, ctx: &Context, req: &[u8], retry_attempt: i32) -> Result<WriteResponseStats, Error>;
    /// Uniquely identifies the remote storage.
    fn name(&self) -> &str;
    /// The remote read or write endpoint.
    fn endpoint(&self) -> &str;
}

/// Go `ReadClient`.
pub trait ReadClient: Send + Sync {
    fn read(&self, ctx: &Context, query: &Query, sort_series: bool) -> Result<BoxSeriesSet, Error>;
    fn read_multiple(&self, ctx: &Context, queries: &[Query], sort_series: bool) -> Result<BoxSeriesSet, Error>;
}
