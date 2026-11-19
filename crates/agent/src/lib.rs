//! Port of `tsdb/agent` (Prometheus v3.16.0): the WAL-only storage of agent mode.
//!
//! The agent keeps only the series (labels, last sample timestamp, metadata) in memory and writes every sample
//! to the WAL, in the same format as the TSDB head (`tsdb-format`), so that remote write can read it. Inactive
//! series are garbage collected and the WAL is truncated behind a checkpoint every `truncate_frequency`.
//!
//! Self-instrumentation values are plain atomics (`tsdb::metrics`) with the upstream metric names in comments.
//! Logging goes through [`Logger`] (Go `*slog.Logger`).

pub mod checkpoint;
pub mod db;
pub mod db_append_v2;
pub mod series;

pub use checkpoint::{ActiveSeries, DEFAULT_BATCH_SIZE, DeletedSeries, checkpoint};
pub use db::*;
pub use db_append_v2::AppenderV2;
pub use series::{DeletedSeriesEntry, MemSeries, SeriesSnapshot, StripeSeries, deleted_series_iter};

/// The logger of the agent (Go `*slog.Logger`).
pub use tsdb_format::record::Logger;
