//! Port of the Prometheus TSDB byte formats (`tsdb/encoding`, `tsdb/fileutil`, `tsdb/wlog`, `tsdb/record`,
//! `tsdb/tombstones`, `tsdb/chunks`, `tsdb/index`). Real Prometheus reads what this crate writes and the reverse.

pub mod chunks;
pub mod encoding;
pub mod fileutil;
pub mod index;
pub mod record;
pub mod testwal;
pub mod tombstones;
pub mod wlog;

/// Go `storage.SeriesRef`: a series reference, opaque to callers. In an index file it is the series offset / 16.
pub use storage::SeriesRef;
