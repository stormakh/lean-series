//! Go test files named client_test.go of several SD packages (one module per package).
mod common;

#[path = "client_test/eureka.rs"]
mod eureka;
