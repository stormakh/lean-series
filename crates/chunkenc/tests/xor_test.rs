//! Port of `tsdb/chunkenc/xor_test.go` (BenchmarkXorRead is a Go benchmark, not a test).

mod common;

use chunkenc::{ENC_XOR, ValueType};

#[test]
fn test_xor_chunk_over_flow_panics() {
    common::test_chunk_over_flow_panics(ENC_XOR, ValueType::Float);
}
