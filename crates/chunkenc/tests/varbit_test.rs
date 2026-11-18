//! Port of `tsdb/chunkenc/varbit_test.go`.

use chunkenc::varbit::{put_varbit_int, put_varbit_int_fast, put_varbit_uint, read_varbit_int, read_varbit_uint};
use chunkenc::{Bstream, BstreamReader};

fn test_varbit_int_boundary_values() -> Vec<i64> {
    vec![
        i64::MIN,
        -36028797018963968,
        -36028797018963967,
        -16777216,
        -16777215,
        -131072,
        -131071,
        -2048,
        -2047,
        -256,
        -255,
        -32,
        -31,
        -4,
        -3,
        -1,
        0,
        1,
        4,
        5,
        32,
        33,
        256,
        257,
        2048,
        2049,
        131072,
        131073,
        16777216,
        16777217,
        36028797018963968,
        36028797018963969,
        i64::MAX,
    ]
}

#[test]
fn test_varbit_int() {
    let numbers = test_varbit_int_boundary_values();
    let mut bs = Bstream::default();
    for &n in &numbers {
        put_varbit_int(&mut bs, n);
    }
    let mut bsr = BstreamReader::new(bs.bytes());
    for &want in &numbers {
        let got = read_varbit_int(&mut bsr);
        assert_eq!(Ok(want), got);
    }
}

#[test]
fn test_varbit_int_fast() {
    let numbers = test_varbit_int_boundary_values();
    let mut bs = Bstream::default();
    for &n in &numbers {
        put_varbit_int_fast(&mut bs, n);
    }
    let mut bsr = BstreamReader::new(bs.bytes());
    for &want in &numbers {
        let got = read_varbit_int(&mut bsr);
        assert_eq!(Ok(want), got);
    }
}

#[test]
fn test_varbit_int_and_fast_produce_identical_output() {
    let numbers = test_varbit_int_boundary_values();
    let mut slow = Bstream::default();
    let mut fast = Bstream::default();
    for &n in &numbers {
        put_varbit_int(&mut slow, n);
        put_varbit_int_fast(&mut fast, n);
    }
    assert_eq!(slow.bytes(), fast.bytes());
}

#[test]
fn test_varbit_uint() {
    let numbers: [u64; 17] = [
        0,
        1,
        7,
        8,
        63,
        64,
        511,
        512,
        4095,
        4096,
        262143,
        262144,
        33554431,
        33554432,
        72057594037927935,
        72057594037927936,
        u64::MAX,
    ];
    let mut bs = Bstream::default();
    for &n in &numbers {
        put_varbit_uint(&mut bs, n);
    }
    let mut bsr = BstreamReader::new(bs.bytes());
    for &want in &numbers {
        let got = read_varbit_uint(&mut bsr);
        assert_eq!(Ok(want), got);
    }
}
