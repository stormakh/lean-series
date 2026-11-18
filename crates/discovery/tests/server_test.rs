//! Go test files named server_test.go of several SD packages (one module per package).
mod common;

#[path = "server_test/ionos.rs"]
mod ionos;
#[path = "server_test/stackit.rs"]
mod stackit;
