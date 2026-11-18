//! Port of `tsdb/chunkenc/bstream_test.go` (the Go benchmarks are not tests).

use chunkenc::{Bstream, BstreamReader};

#[test]
fn test_bstream_reset() {
    let mut bs = Bstream { stream: b"test".to_vec(), count: 10 };
    bs.reset(b"was reset".to_vec());

    assert_eq!(Bstream { stream: b"was reset".to_vec(), count: 0 }, bs);
}

#[test]
fn test_bstream_reader() {
    // Write to the bit stream.
    let mut w = Bstream::default();
    for bit in [true, false] {
        w.write_bit(bit);
    }
    for nbits in 1..=64 {
        w.write_bits(nbits as u64, nbits);
    }
    let mut v = 1;
    while v < 10000 {
        w.write_bits(v as u64, 29);
        v += 123;
    }

    // Read back.
    let mut r = BstreamReader::new(w.bytes());
    for bit in [true, false] {
        let v = match r.read_bit_fast() {
            Some(v) => Some(v),
            None => r.read_bit(),
        };
        assert_eq!(Some(bit), v);
    }
    for nbits in 1..=64u8 {
        let v = match r.read_bits_fast(nbits) {
            Some(v) => Some(v),
            None => r.read_bits(nbits),
        };
        assert_eq!(Some(nbits as u64), v, "nbits={nbits}");
    }
    let mut v = 1u64;
    while v < 10000 {
        let actual = match r.read_bits_fast(29) {
            Some(a) => Some(a),
            None => r.read_bits(29),
        };
        assert_eq!(Some(v), actual, "v={v}");
        v += 123;
    }
}
