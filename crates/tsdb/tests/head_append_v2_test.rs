//! Port of `tsdb/head_append_v2_test.go`, split by line range of the Go file into parts (one per porter):
//! part1: lines 1..2831, part2: 2832..end. Each part has its own helpers.

#[allow(dead_code)]
mod common;

#[path = "head_append_v2_test/part1.rs"]
mod part1;
#[path = "head_append_v2_test/part2.rs"]
mod part2;
