//! Go test files named server_test.go of several SD packages (one module per package).
mod common;

#[path = "server_test/ionos.rs"]
mod ionos;
#[path = "server_test/stackit.rs"]
mod stackit;
#[path = "server_test/stackit_mock.rs"]
mod stackit_mock;
