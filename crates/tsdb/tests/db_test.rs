//! Port of `tsdb/db_test.go`, split by line range of the Go file into parts (one per porter):
//! part1: lines 1..3398, part2: 3399..6614, part3: 6615..9966, part4: 9967..end. Each part has its own helpers.

#[allow(dead_code)]
mod common;

#[path = "db_test/part1.rs"]
mod part1;
#[path = "db_test/part2.rs"]
mod part2;
#[path = "db_test/part3.rs"]
mod part3;
#[path = "db_test/part4.rs"]
mod part4;
