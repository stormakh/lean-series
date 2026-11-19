//! Go test files named instance_test.go of several SD packages (one module per package; the ledger keys tests by
//! file stem and test name only).
mod cloud_mock;
mod common;
mod openstack_mock;

#[path = "instance_test/scaleway.rs"]
mod scaleway;

#[path = "instance_test/openstack.rs"]
mod openstack;
