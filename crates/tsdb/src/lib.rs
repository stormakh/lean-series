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

pub mod meta;
pub mod metrics;
pub mod reader;

pub use meta::*;
pub use reader::*;

/// The logger of the TSDB (Go `*slog.Logger`).
pub use tsdb_format::record::Logger;

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
