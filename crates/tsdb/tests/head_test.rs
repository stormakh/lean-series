//! Port of `tsdb/head_test.go`, split by line range of the Go file into parts (one per porter):
//! part1: lines 1..3805, part2: 3806..7602, part3: 7603..end. Each part has its own helpers.

#[allow(dead_code)]
mod common;

#[path = "head_test/part1.rs"]
mod part1;
#[path = "head_test/part2.rs"]
mod part2;
#[path = "head_test/part3.rs"]
mod part3;
