//! Port of Prometheus `cmd/promtool` (v3.16.0).
//!
//! The binary (`src/main.rs`) calls [`app::run`]. The Go functions of `package main` are public functions of
//! the modules below, with the same names in snake_case. Where Go writes to `os.Stdout`/`os.Stderr` or reads
//! `os.Stdin`, the Rust function takes [`Io`] (tests pass buffers; the binary passes the process streams).
//! Errors carry Go's `err.Error()` text.
//!
//! Modules (one per Go file):
//! - [`app`] main.go: flags, `check config/web-config/rules/metrics/healthy/ready`, promql editing.
//! - [`cli`] kingpin-compatible flag parser, help text and `write-documentation` (util/documentcli).
//! - [`query`], [`api`], [`client_model`] query.go and the client_golang API client and value types.
//! - [`analyze`] analyze.go (`query analyze`).
//! - [`metrics`] metrics.go (`push metrics`), [`debug`] + [`archive`] debug.go/archive.go, [`sd`] sd.go.
//! - [`promlint`] client_golang `promlint`.
//! - [`tsdb_cmd`] tsdb.go, [`backfill`] backfill.go, [`rules_cmd`] rules.go, [`unittest`] unittest.go.

pub mod api;
pub mod client_model;

pub mod backfill;
pub mod rules_cmd;
pub mod tsdb_cmd;
pub mod unittest;

pub mod check_metrics;
pub mod expfmt;
pub mod metrics;
pub mod promlint;
pub mod web_config;

pub mod app;

use std::io::{Read, Write};

pub const SUCCESS_EXIT_CODE: i32 = 0;
pub const FAILURE_EXIT_CODE: i32 = 1;
/// Exit code 3 is used for "one or more lint issues detected".
pub const LINT_ERR_EXIT_CODE: i32 = 3;

/// The process streams of a command (Go `os.Stdin`, `os.Stdout`, `os.Stderr`).
pub struct Io<'a> {
    pub stdin: &'a mut dyn Read,
    pub stdout: &'a mut dyn Write,
    pub stderr: &'a mut dyn Write,
}

/// A boxed error with Go's error text.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Go `checkErr`: prints the error to stderr and returns 1, or returns 0.
pub fn check_err<T>(r: Result<T, Error>, stderr: &mut dyn Write) -> i32 {
    match r {
        Ok(_) => SUCCESS_EXIT_CODE,
        Err(e) => {
            let _ = writeln!(stderr, "{e}");
            FAILURE_EXIT_CODE
        }
    }
}
