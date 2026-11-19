//! Port of Prometheus `cmd/prometheus` (main.go, reload, query log, features, warnings, upload): the
//! `prometheus` server binary. `main.rs` only calls [`run::main`].

pub mod consoles;
pub mod features;
pub mod flags;
pub mod helpers;
pub use kingpin;
pub mod metrics;
pub mod ready;
pub mod reload;
pub mod run;
pub mod rungroup;
pub mod selfmetrics;
pub mod sys;

/// Go `os` error text of an I/O error.
pub fn io_err_text(e: &std::io::Error) -> String {
    net::types::go_io_error(e)
}
