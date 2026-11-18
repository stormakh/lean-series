//! Go test files named instance_test.go of several SD packages (one module per package; the ledger keys tests by
//! file stem and test name only).
mod common;

#[path = "instance_test/scaleway.rs"]
mod scaleway;
