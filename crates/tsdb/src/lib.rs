//! Port of the Prometheus TSDB (`tsdb/*.go` of v3.16.0): head block, appenders, isolation, out-of-order
//! ingestion, exemplars, WAL/WBL replay, querier, blocks, compaction and the DB.
//!
//! The byte formats (index, chunks, `chunks_head`, WAL, records, tombstones) are in the `tsdb-format` crate; the
//! storage interfaces (Queryable, Appender, series sets, errors, Context) are in `storage`.
//!
//! Design rules (memory and CPU are the goal; see PORTING.md):
//! - Errors are `storage::Error` with Go's texts. Cancellation is `storage::Context`.
//! - Head series are compact structs with u64 refs, `model::labels::Labels` (one shared buffer) and a mutex;
//!   the series maps grow with demand (no fixed 16k stripes), postings are `tsdb_format::index::MemPostings`.
//! - No per-sample allocation on append. No preallocated large buffers (size by demand).
//! - No background threads that wake when idle, except the DB's one-minute compaction timer.
//! - Logging goes through [`Logger`] (Go `*slog.Logger`); the default discards everything.
//! - Self-instrumentation values are plain atomics ([`metrics`]) with upstream metric names in comments.

pub mod block;
pub mod blockwriter;
pub mod compact;
pub mod db;
pub mod exemplar;
pub mod head;
pub mod head_append;
pub mod head_append_v2;
pub mod head_read;
pub mod head_wal;
pub mod isolation;
pub mod meta;
pub mod metrics;
pub mod ooo_head;
pub mod ooo_head_read;
pub mod ooo_isolation;
pub mod querier;
pub mod query_cost;
pub mod reader;
pub mod repair;
pub mod teststorage;
pub mod tsdbblockutil;
pub mod tsdbutil;

pub use meta::*;
pub use reader::*;

/// The logger of the TSDB (Go `*slog.Logger`).
pub use tsdb_format::record::Logger;

/// Converts an index error to a storage error with the same text. Context errors keep `errors.Is` semantics
/// (they wrap `storage::ERR_CANCELED` or `storage::ERR_DEADLINE_EXCEEDED`).
pub(crate) fn index_err(e: tsdb_format::index::Error) -> storage::Error {
    if e.is_context() {
        let target = if e.to_string().ends_with("context deadline exceeded") {
            storage::ERR_DEADLINE_EXCEEDED
        } else {
            storage::ERR_CANCELED
        };
        if e.to_string() == target.to_string() {
            return target;
        }
        return storage::Error::wrapped(e.to_string(), target);
    }
    storage::Error::other(e)
}

/// Converts a storage error to an index error with the same text (for postings that carry an index error).
pub(crate) fn to_index_err(e: &storage::Error) -> tsdb_format::index::Error {
    use tsdb_format::index::Error as IErr;
    if let Some(ie) = e.downcast_ref::<IErr>() {
        if ie.to_string() == e.to_string() {
            return ie.clone();
        }
        return IErr::with_cause(e.to_string(), ie.clone());
    }
    let ctx = if e.is(&storage::ERR_CANCELED) {
        Some("context canceled")
    } else if e.is(&storage::ERR_DEADLINE_EXCEEDED) {
        Some("context deadline exceeded")
    } else {
        None
    };
    match ctx {
        Some(c) if e.to_string() == c => IErr::context(c),
        Some(c) => IErr::with_cause(e.to_string(), IErr::context(c)),
        None => IErr::new(e.to_string()),
    }
}

/// Converts a tombstones error to a storage error with the same text.
pub(crate) fn tomb_err(e: tsdb_format::tombstones::Error) -> storage::Error {
    storage::Error::other(e)
}

/// Converts a chunks error to a storage error with the same text.
pub(crate) fn chunks_err(e: tsdb_format::chunks::Error) -> storage::Error {
    storage::Error::other(e)
}

/// The OS part of an I/O error, as Go prints it (for example "no such file or directory").
pub(crate) fn io_err_text(e: &std::io::Error) -> String {
    let s = e.to_string();
    // Rust appends " (os error N)"; Go does not.
    match s.find(" (os error ") {
        Some(i) => {
            let mut t = s[..i].to_string();
            if let Some(c) = t.get_mut(0..1) {
                c.make_ascii_lowercase();
            }
            t
        }
        None => s,
    }
}
