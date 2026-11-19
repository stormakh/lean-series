//! Port of Prometheus `util/` packages.

pub mod logging;
pub mod stats;

/// Port of `util/annotations`. The code lives in `storage::annotations`, because storage needs it and cannot
/// depend on this crate.
pub mod annotations {
    pub use storage::annotations::*;
}

pub mod teststorage;

pub mod compression;
pub mod httputil;
pub mod junitxml;
pub mod logging;
pub mod netconnlimit;
pub mod notifications;
pub mod runtime;
pub mod tracing;
pub mod almost;
pub mod convertnhcb;
pub mod fmtutil;
pub mod kahansum;
pub mod namevalidationutil;
pub mod pool;
pub mod strutil;
pub mod zeropool;
