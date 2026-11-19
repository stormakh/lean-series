//! Port of Prometheus `util/` packages.

pub mod stats;

/// Port of `util/annotations`. The code lives in `storage::annotations`, because storage needs it and cannot
/// depend on this crate.
pub mod annotations {
    pub use storage::annotations::*;
}

pub mod teststorage;

pub mod almost;
pub mod convertnhcb;
pub mod fmtutil;
pub mod kahansum;
pub mod namevalidationutil;
pub mod pool;
pub mod strutil;
pub mod zeropool;
