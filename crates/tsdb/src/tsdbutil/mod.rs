//! Port of `tsdb/tsdbutil`: the data directory lock and the removal of temporary directories. (The histogram
//! test generators of `tsdbutil/histogram.go` live in `storage::tsdbutil`.)

pub mod dir_locker;
pub mod remove_tmp_dirs;

pub use dir_locker::*;
pub use remove_tmp_dirs::*;
