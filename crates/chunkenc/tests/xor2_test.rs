//! Port of `tsdb/chunkenc/xor2_test.go`.

mod common;

use chunkenc::ChunkIterator as _;
use chunkenc::{Bstream, BstreamReader, Chunk, ENC_XOR2, Error, ValueType, Xor2Chunk, Xor2Iterator, from_data};
use common::{Rand, SampleCase, Triple, triple};

use model::value::STALE_NAN;

fn stale_nan() -> f64 {
    f64::from_bits(STALE_NAN)
}

fn is_stale_nan(v: f64) -> bool {
    v.to_bits() == STALE_NAN
}

/// Go `maxFirstSTChangeOn` (st.go).
const MAX_FIRST_ST_CHANGE_ON: usize = 0x7F;

/// Go `newXOR2IteratorForPayload`: the stream bytes (padding, payload, 64 tail bits).
fn xor2_payload_bytes(padding: usize, payload: &dyn Fn(&mut Bstream)) -> Vec<u8> {
    let mut bs = Bstream::default();
    if padding > 0 {
        bs.write_bits_fast(0, padding as i32);
    }
    payload(&mut bs);
    // Add tail bytes so the reader initially fills a full 64-bit buffer.
    bs.write_bits_fast(0, 64);
    bs.stream
}

/// Go `newXOR2IteratorForPayload`: an iterator over `bytes` with `padding` bits already read.
fn new_xor2_iterator_for_payload<'a>(
    bytes: &'a [u8],
    padding: usize,
    setup: &dyn Fn(&mut Xor2Iterator<'a>),
) -> Xor2Iterator<'a> {
    let mut it = Xor2Iterator::default();
    setup(&mut it);
    it.br = BstreamReader::new(bytes);
    if padding > 0 {
        assert!(it.br.read_bits(padding as u8).is_some());
    }
    it
}

/// Go `writeXOR2NewWindowPayload`.
fn write_xor2_new_window_payload(bs: &mut Bstream, delta: u64) -> (u8, u8) {
    let (leading, trailing, sigbits) = xor2_delta_window(delta);
    let encoded_sigbits = if sigbits == 64 { 0 } else { sigbits };
    bs.write_bits_fast(leading as u64, 5);
    bs.write_bits_fast(encoded_sigbits as u64, 6);
    bs.write_bits_fast(delta >> trailing, sigbits as i32);
    (leading, trailing)
}

/// Go `xor2DeltaWindow`.
fn xor2_delta_window(delta: u64) -> (u8, u8, u8) {
    let mut leading = delta.leading_zeros() as u8;
    let trailing = delta.trailing_zeros() as u8;
    if leading >= 32 {
        leading = 31;
    }
    (leading, trailing, 64 - leading - trailing)
}

/// Go `xor2SampleCase`.
struct Xor2SampleCase {
    case: SampleCase,
    want_bytes: usize,
}

type ValueFn = Box<dyn Fn(usize, &mut Rand) -> f64>;

/// Go `xor2WindowSamples`.
fn xor2_window_samples() -> Vec<Xor2SampleCase> {
    let mut r = Rand::new_pcg(10, 20);
    let one = 1.0f64.to_bits();
    // Expected sizes indexed by timestamp jitter, then start timestamp presence.
    let patterns: Vec<(&str, ValueFn, [[usize; 2]; 2])> = vec![
        ("constant", Box::new(|_, _| 1e9), [[34, 217], [252, 435]]),
        ("counter", Box::new(|i, _| 1e9 + i as f64), [[214, 397], [419, 602]]),
        ("alternating-bool", Box::new(|i, _| (i % 2) as f64), [[214, 397], [419, 602]]),
        // Alternate shifted XOR windows at and beyond the expansion limits.
        (
            "shifted-window-5",
            Box::new(move |i, _| {
                let deltas: [u64; 4] = [0, 0xff << 27, (0xff << 27) ^ (0xff << 32), 0xff << 32];
                f64::from_bits(one ^ deltas[i % 4])
            }),
            [[260, 443], [464, 647]],
        ),
        (
            "shifted-window-6",
            Box::new(move |i, _| {
                let deltas: [u64; 4] = [0, 0xff << 27, (0xff << 27) ^ (0xff << 33), 0xff << 33];
                f64::from_bits(one ^ deltas[i % 4])
            }),
            [[347, 530], [479, 662]],
        ),
        (
            "shifted-window-7",
            Box::new(move |i, _| {
                let deltas: [u64; 4] = [0, 0xff << 27, (0xff << 27) ^ (0xff << 34), 0xff << 34];
                f64::from_bits(one ^ deltas[i % 4])
            }),
            [[347, 530], [565, 748]],
        ),
        (
            "full-width-union",
            Box::new(move |i, _| {
                let deltas: [u64; 4] =
                    [0, (1 << 63) | (1 << 5), (1 << 63) | (1 << 58) | (1 << 5) | 1, (1 << 58) | 1];
                f64::from_bits(one ^ deltas[i % 4])
            }),
            [[1018, 1201], [1223, 1406]],
        ),
        (
            "sign-excursion",
            Box::new(|i, _| {
                let v = 1e9 + i as f64;
                if i == 2 { -v } else { v }
            }),
            [[225, 408], [429, 612]],
        ),
        (
            "zero-excursion",
            Box::new(|i, _| if i == 2 { 0.0 } else { 1000.0 + i as f64 * 0.125 }),
            [[175, 358], [380, 563]],
        ),
        ("random-integer", Box::new(|_, r| r.int_n(1001) as f64), [[260, 443], [464, 647]]),
        ("random-float", Box::new(|_, r| r.float64() * 1000.0), [[900, 1083], [1104, 1287]]),
        (
            "fractional",
            Box::new(|i, _| {
                let f = i as f64;
                f + f / 10.0 + f / 100.0 + f / 1000.0
            }),
            [[883, 1066], [1088, 1271]],
        ),
        (
            "special-floats",
            Box::new(|i, _| match i % 31 {
                0 => stale_nan(),
                1 => f64::from_bits(0x7ff8000000000012),
                2 => f64::INFINITY,
                3 => f64::NEG_INFINITY,
                4 => -0.0,
                5 => 0.0,
                _ => 1e9 + i as f64,
            }),
            [[301, 484], [508, 691]],
        ),
    ];

    let mut cases = Vec::new();
    for (name, value, want_bytes) in &patterns {
        let values: Vec<f64> = (0..120).map(|i| value(i, &mut r)).collect();
        for (jitter_index, jitter) in [false, true].into_iter().enumerate() {
            for (st_index, with_st) in [false, true].into_iter().enumerate() {
                let mut timestamps = Rand::new_pcg(1, 2);
                let samples: Vec<Triple> = values
                    .iter()
                    .enumerate()
                    .map(|(i, &v)| {
                        let mut t = 1700000000000i64 + i as i64 * 15000;
                        if jitter {
                            t += timestamps.int_n(11) - 5;
                        }
                        let st = if with_st { 1699999990000i64 + (i as i64 / 60) * 900000 } else { 0 };
                        triple(st, t, v)
                    })
                    .collect();
                cases.push(Xor2SampleCase {
                    case: SampleCase { name: format!("{name}/jitter={jitter}/st={with_st}"), samples },
                    want_bytes: want_bytes[jitter_index][st_index],
                });
            }
        }
    }
    cases
}

#[test]
fn test_xor2_window_selection() {
    for tc in xor2_window_samples() {
        let name = &tc.case.name;
        let mut c = Xor2Chunk::new();
        let mut app = c.appender().unwrap();
        for (i, s) in tc.case.samples.iter().enumerate() {
            if i == 5 {
                // Resume appending after narrowing the window.
                c.reset(c.bytes().to_vec());
                app = c.appender().unwrap();
            }
            app.append(&mut c, s.st, s.t, s.v);
        }
        assert_eq!(c.bytes().len(), tc.want_bytes, "{name}");
        let decoded = from_data(ENC_XOR2, c.bytes().to_vec()).unwrap();
        let mut it = decoded.iterator(None);
        for s in &tc.case.samples {
            assert_eq!(ValueType::Float, it.next(), "{name}");
            let (ts, v) = it.at();
            assert_eq!(s.t, ts, "{name}");
            assert_eq!(s.st, it.at_st(), "{name}");
            assert_eq!(s.v.to_bits(), v.to_bits(), "{name}");
        }
        assert_eq!(ValueType::None, it.next(), "{name}");
        assert!(it.err().is_none(), "{name}: {:?}", it.err());
    }
}

/// Go `requireXOR2Samples`.
fn require_xor2_samples(samples: &[Triple]) {
    let mut chunk = Xor2Chunk::new();
    let mut app = chunk.appender().unwrap();
    for s in samples {
        app.append(&mut chunk, s.st, s.t, s.v);
    }

    let mut it = chunk.iterator();
    for want in samples {
        assert_eq!(ValueType::Float, it.next());
        assert_eq!(want.st, it.at_st());
        let (ts, v) = it.at();
        assert_eq!(want.t, ts);
        assert_eq!(want.v, v);
    }
    assert_eq!(ValueType::None, it.next());
    assert!(it.err().is_none());
}

#[test]
fn test_xor2_basic() {
    let mut c = Xor2Chunk::new();
    let mut app = c.appender().unwrap();

    let samples: [(i64, f64); 5] = [(1000, 1.0), (2000, 2.0), (3000, 3.0), (4000, 4.0), (5000, 5.0)];
    for &(t, v) in &samples {
        app.append(&mut c, 0, t, v);
    }

    let mut it = c.iterator();
    for &(et, ev) in &samples {
        assert_eq!(ValueType::Float, it.next());
        let (ts, v) = it.at();
        assert_eq!(et, ts);
        assert_eq!(ev, v);
    }
    assert_eq!(ValueType::None, it.next());
}

fn check_stale_samples(samples: &[(i64, f64, bool)]) {
    let mut c = Xor2Chunk::new();
    let mut app = c.appender().unwrap();
    for &(t, v, _) in samples {
        app.append(&mut c, 0, t, v);
    }

    let mut it = c.iterator();
    for &(et, ev, stale) in samples {
        assert_eq!(ValueType::Float, it.next());
        let (ts, v) = it.at();
        assert_eq!(et, ts);
        if stale {
            assert!(is_stale_nan(v), "Expected stale NaN at ts={ts}");
        } else {
            assert_eq!(ev, v);
        }
    }
    assert_eq!(ValueType::None, it.next());
}

#[test]
fn test_xor2_with_staleness() {
    check_stale_samples(&[
        (1000, 1.0, false),
        (2000, 2.0, false),
        (3000, stale_nan(), true),
        (4000, 4.0, false),
        (5000, stale_nan(), true),
        (6000, 6.0, false),
    ]);
}

#[test]
fn test_xor2_stale_with_dod_non_zero() {
    // Stale NaN samples where the timestamp dod is non-zero, exercising the `111` value encoding path inside
    // writeVDelta.
    check_stale_samples(&[
        (1000, 1.0, false),
        (2000, 2.0, false),
        // dod = (1050 - 1000) - (2000 - 1000) = 50 - 1000 = -950: stale with dod!=0.
        (3050, stale_nan(), true),
        (4050, 4.0, false),
        (5050, 5.0, false),
    ]);
}

fn check_timestamps(timestamps: &[i64]) {
    let mut c = Xor2Chunk::new();
    let mut app = c.appender().unwrap();
    for &ts in timestamps {
        app.append(&mut c, 0, ts, 1.0);
    }

    let mut it = c.iterator();
    for &expected in timestamps {
        assert_eq!(ValueType::Float, it.next());
        let (ts, _) = it.at();
        assert_eq!(expected, ts);
    }
    assert_eq!(ValueType::None, it.next());
}

#[test]
fn test_xor2_irregular_timestamps() {
    // Timestamps with dod values spanning multiple encoding ranges.
    check_timestamps(&[
        1000,
        2000,
        3000,
        // dod in 13-bit range.
        3050,
        4050,
        5050,
        // dod in 20-bit range (large jitter).
        5050 + 100000,
        5050 + 200000,
        5050 + 300000,
        // Back to regular.
        5050 + 301000,
    ]);
}

#[test]
fn test_xor2_large_dod() {
    // Force the 64-bit escape path with a very large dod.
    check_timestamps(&[0, 1000, 2000, 2000 + (1 << 20)]);
}

#[test]
fn test_xor2_large_dod_with_active_st() {
    require_xor2_samples(&[
        triple(0, 0, 1.0),
        triple(900, 1000, 2.0),
        triple(1000, 2000, 3.0),
        triple(1047576, 1050576, 4.0),
    ]);
}

#[test]
fn test_xor2_active_st_fast_path_boundaries() {
    require_xor2_samples(&[
        triple(0, 1000, 1.0),
        triple(1990, 2000, 1.0),
        triple(2986, 3000, 1.0),
        triple(3954, 4000, 1.0),
        triple(4698, 5000, 1.0),
    ]);
}

#[test]
fn test_xor2_active_st13_bit_dod_value_unchanged_st_delta_branches() {
    require_xor2_samples(&[
        triple(0, 1000, 1.0),
        triple(0, 2000, 1.0),
        triple(1500, 3000, 1.0), // First ST change: stDiff=500.
        triple(2500, 4001, 1.0), // Active ST, dod=1, deltaStDiff=0.
        triple(3497, 5003, 1.0), // Active ST, dod=1, deltaStDiff=4.
        triple(4467, 6004, 1.0), // Active ST, dod=-1, deltaStDiff=32.
        triple(5212, 7007, 1.0), // Active ST, dod=2, deltaStDiff=256.
        triple(5915, 8008, 1.0), // Active ST, dod=-2, deltaStDiff=300.
    ]);
}

#[test]
fn test_xor2_encode_joint_value_unchanged_then_changed() {
    require_xor2_samples(&[
        triple(0, 1000, 1.0),
        triple(0, 2000, 2.0),
        triple(0, 7096, 2.0),
        triple(0, 12192, 3.0),
    ]);
}

#[test]
fn test_xor2_constant_non_zero_st_fast_path() {
    require_xor2_samples(&[
        triple(500, 1000, 1.0),
        triple(500, 2000, 2.0),
        triple(500, 3000, 2.0),
        triple(500, 4050, 2.0),
        triple(500, 5100, 3.0),
    ]);
}

#[test]
fn test_xor2_active_st_dod_zero_value_change() {
    require_xor2_samples(&[
        triple(0, 1000, 1.0),
        triple(500, 2000, 2.0),
        triple(500, 3000, 3.0), // dod=0, value changed.
        triple(500, 4000, 4.0), // dod=0, value changed.
    ]);
}

#[test]
fn test_xor2_active_st_dod_zero_value_unchanged_zero_st_delta() {
    require_xor2_samples(&[
        triple(0, 1000, 1.0),
        triple(0, 2000, 1.0),
        triple(1500, 3000, 1.0), // First ST change: stDiff=500.
        triple(2500, 4000, 1.0), // Active ST, dod=0, value unchanged, deltaStDiff=0.
        triple(3500, 5000, 1.0), // Repeat to keep the path live on consecutive samples.
    ]);
}

#[test]
fn test_xor2_active_st_value_change_inline_st_delta_branches() {
    require_xor2_samples(&[
        triple(0, 1000, 1.0),
        triple(0, 2000, 2.0),
        triple(1500, 3000, 3.0), // First ST change: stDiff=500.
        triple(2500, 4000, 4.0), // Active ST default case, deltaStDiff=0.
        triple(3497, 5000, 5.0), // Active ST default case, deltaStDiff=3.
        triple(4466, 6000, 6.0), // Active ST default case, deltaStDiff=31.
        triple(5211, 7000, 7.0), // Active ST default case, deltaStDiff=255.
    ]);
}

#[test]
fn test_xor2_chunk_st() {
    common::test_chunk_st_handling(ValueType::Float, || Box::new(Xor2Chunk::new()) as Box<dyn Chunk>);
}

#[test]
fn test_xor2_chunk_more_than127_samples() {
    let after_max = MAX_FIRST_ST_CHANGE_ON + 3;

    // "zero ST".
    {
        let mut chunk = Xor2Chunk::new();
        let mut app = chunk.appender().unwrap();
        for i in 0..after_max {
            app.append(&mut chunk, 0, (i * 10 + 1) as i64, i as f64 * 1.5);
        }

        let mut it = chunk.iterator();
        for i in 0..after_max {
            assert_eq!(ValueType::Float, it.next());
            let st = it.at_st();
            let (ts, v) = it.at();
            assert_eq!(0, st);
            assert_eq!((i * 10 + 1) as i64, ts);
            assert_eq!(i as f64 * 1.5, v);
        }
        assert_eq!(ValueType::None, it.next());
        assert!(it.err().is_none());
    }

    // "non-zero ST after 127".
    {
        let mut chunk = Xor2Chunk::new();
        let mut app = chunk.appender().unwrap();
        for i in 0..after_max {
            let st = if i == after_max - 1 { ((after_max - 1) * 10) as i64 } else { 0 };
            app.append(&mut chunk, st, (i * 10 + 1) as i64, i as f64 * 1.5);
        }

        let mut it = chunk.iterator();
        for i in 0..after_max {
            assert_eq!(ValueType::Float, it.next());
            let st = it.at_st();
            let (ts, v) = it.at();
            if i == after_max - 1 {
                assert_eq!(((after_max - 1) * 10) as i64, st);
            } else {
                assert_eq!(0, st);
            }
            assert_eq!((i * 10 + 1) as i64, ts);
            assert_eq!(i as f64 * 1.5, v);
        }
        assert_eq!(ValueType::None, it.next());
        assert!(it.err().is_none());
    }
}

struct DecodeCase {
    name: &'static str,
    payload: Box<dyn Fn(&mut Bstream)>,
    setup: Box<dyn for<'a> Fn(&mut Xor2Iterator<'a>)>,
    assert: Box<dyn for<'a> Fn(&str, &Xor2Iterator<'a>)>,
}

fn run_cases(group: &str, cases: &[DecodeCase], f: for<'a, 'b> fn(&'b mut Xor2Iterator<'a>) -> Result<(), Error>) {
    for tc in cases {
        for padding in 0..64 {
            let ctx = format!("{group}/{}/padding={padding}", tc.name);
            let bytes = xor2_payload_bytes(padding, &*tc.payload);
            let mut it = new_xor2_iterator_for_payload(&bytes, padding, &*tc.setup);
            let res = f(&mut it);
            assert!(res.is_ok(), "{ctx}: {res:?}");
            (tc.assert)(&ctx, &it);
        }
    }
}

/// TestXOR2DecodeFunctionsAcrossPadding exercises decode_value, decode_value_known_non_zero, and
/// decode_new_leading_trailing across all logical cases x all 64 bit-buffer alignments (padding 0..63), so both
/// the fast path (enough bits buffered) and the slow path (bits span a buffer refill) run for every case.
#[test]
fn test_xor2_decode_functions_across_padding() {
    const BASELINE: f64 = 1234.5;
    let xored = |d: u64| f64::from_bits(BASELINE.to_bits() ^ d);

    // decode_value: `0`=unchanged, `10`=reuse window, `110`=new window, `111`=stale NaN.
    {
        let reuse_d: u64 = 0x000ABCDE000000;
        let (r_l, r_t, r_s) = xor2_delta_window(reuse_d);
        // Two new-window variants: full-width sigbits (encoded as 0) and small sigbits.
        let new_d_full: u64 = 0xFEDCBA9876543211;
        let (n_l_full, n_t_full, _) = xor2_delta_window(new_d_full);
        let new_d_small: u64 = 0x000ABCDE000000;
        let (n_l_small, n_t_small, _) = xor2_delta_window(new_d_small);

        run_cases(
            "decodeValue",
            &[
                DecodeCase {
                    name: "unchanged",
                    payload: Box::new(|bs| bs.write_bit(false)),
                    setup: Box::new(|it| it.baseline_v = BASELINE),
                    assert: Box::new(|ctx, it| {
                        assert_eq!(BASELINE, it.val, "{ctx}");
                        assert_eq!(BASELINE, it.baseline_v, "{ctx}");
                    }),
                },
                DecodeCase {
                    name: "reuse_window",
                    payload: Box::new(move |bs| {
                        bs.write_bits_fast(0b10, 2);
                        bs.write_bits_fast(reuse_d >> r_t, r_s as i32);
                    }),
                    setup: Box::new(move |it| {
                        it.baseline_v = BASELINE;
                        it.leading = r_l;
                        it.trailing = r_t;
                    }),
                    assert: Box::new(move |ctx, it| {
                        let expected = xored(reuse_d);
                        assert_eq!(expected, it.val, "{ctx}");
                        assert_eq!(expected, it.baseline_v, "{ctx}");
                        assert_eq!(r_l, it.leading, "{ctx}");
                        assert_eq!(r_t, it.trailing, "{ctx}");
                    }),
                },
                DecodeCase {
                    name: "new_window_full_sigbits",
                    payload: Box::new(move |bs| {
                        bs.write_bits_fast(0b110, 3);
                        write_xor2_new_window_payload(bs, new_d_full);
                    }),
                    setup: Box::new(|it| it.baseline_v = BASELINE),
                    assert: Box::new(move |ctx, it| {
                        let expected = xored(new_d_full);
                        assert_eq!(expected, it.val, "{ctx}");
                        assert_eq!(expected, it.baseline_v, "{ctx}");
                        assert_eq!(n_l_full, it.leading, "{ctx}");
                        assert_eq!(n_t_full, it.trailing, "{ctx}");
                    }),
                },
                DecodeCase {
                    name: "new_window_small_sigbits",
                    payload: Box::new(move |bs| {
                        bs.write_bits_fast(0b110, 3);
                        write_xor2_new_window_payload(bs, new_d_small);
                    }),
                    setup: Box::new(|it| it.baseline_v = BASELINE),
                    assert: Box::new(move |ctx, it| {
                        let expected = xored(new_d_small);
                        assert_eq!(expected, it.val, "{ctx}");
                        assert_eq!(expected, it.baseline_v, "{ctx}");
                        assert_eq!(n_l_small, it.leading, "{ctx}");
                        assert_eq!(n_t_small, it.trailing, "{ctx}");
                    }),
                },
                DecodeCase {
                    name: "stale_nan",
                    payload: Box::new(|bs| bs.write_bits_fast(0b111, 3)),
                    setup: Box::new(|it| it.baseline_v = BASELINE),
                    assert: Box::new(|ctx, it| {
                        assert!(is_stale_nan(it.val), "{ctx}");
                        assert_eq!(BASELINE, it.baseline_v, "{ctx}");
                    }),
                },
            ],
            |it| it.decode_value(),
        );
    }

    // decode_value_known_non_zero: `0`=reuse window, `1`=new window. The new_window case uses real
    // leading/trailing (not 0xff) so that sz is small enough for the fast path (valid >= 1+sz) to be reached with
    // ctrlBit=1.
    {
        let delta: u64 = 0x000ABCDE000000;
        let (d_l, d_t, d_s) = xor2_delta_window(delta);

        run_cases(
            "decodeValueKnownNonZero",
            &[
                DecodeCase {
                    name: "reuse_window",
                    payload: Box::new(move |bs| {
                        bs.write_bit(false);
                        bs.write_bits_fast(delta >> d_t, d_s as i32);
                    }),
                    setup: Box::new(move |it| {
                        it.baseline_v = BASELINE;
                        it.leading = d_l;
                        it.trailing = d_t;
                    }),
                    assert: Box::new(move |ctx, it| {
                        let expected = xored(delta);
                        assert_eq!(expected, it.val, "{ctx}");
                        assert_eq!(expected, it.baseline_v, "{ctx}");
                    }),
                },
                DecodeCase {
                    name: "new_window",
                    payload: Box::new(move |bs| {
                        bs.write_bit(true);
                        write_xor2_new_window_payload(bs, delta);
                    }),
                    setup: Box::new(move |it| {
                        it.baseline_v = BASELINE;
                        it.leading = d_l;
                        it.trailing = d_t;
                    }),
                    assert: Box::new(move |ctx, it| {
                        let expected = xored(delta);
                        assert_eq!(expected, it.val, "{ctx}");
                        assert_eq!(expected, it.baseline_v, "{ctx}");
                        assert_eq!(d_l, it.leading, "{ctx}");
                        assert_eq!(d_t, it.trailing, "{ctx}");
                    }),
                },
            ],
            |it| it.decode_value_known_non_zero(),
        );
    }

    // decode_new_leading_trailing: the 11-bit header fast path, the value-bits fast path (small sigbits), and
    // full-width sigbits (encoded as 0).
    {
        let small_d: u64 = 0x000ABCDE000000;
        let (s_l, s_t, _) = xor2_delta_window(small_d);
        let full_d: u64 = 0xFEDCBA9876543211;
        let (f_l, f_t, _) = xor2_delta_window(full_d);

        run_cases(
            "decodeNewLeadingTrailing",
            &[
                DecodeCase {
                    name: "small_sigbits",
                    payload: Box::new(move |bs| {
                        write_xor2_new_window_payload(bs, small_d);
                    }),
                    setup: Box::new(|it| it.baseline_v = BASELINE),
                    assert: Box::new(move |ctx, it| {
                        assert_eq!(s_l, it.leading, "{ctx}");
                        assert_eq!(s_t, it.trailing, "{ctx}");
                        let expected = xored(small_d);
                        assert_eq!(expected, it.val, "{ctx}");
                        assert_eq!(expected, it.baseline_v, "{ctx}");
                    }),
                },
                DecodeCase {
                    name: "full_width_sigbits",
                    payload: Box::new(move |bs| {
                        write_xor2_new_window_payload(bs, full_d);
                    }),
                    setup: Box::new(|it| it.baseline_v = BASELINE),
                    assert: Box::new(move |ctx, it| {
                        assert_eq!(f_l, it.leading, "{ctx}");
                        assert_eq!(f_t, it.trailing, "{ctx}");
                        let expected = xored(full_d);
                        assert_eq!(expected, it.val, "{ctx}");
                        assert_eq!(expected, it.baseline_v, "{ctx}");
                    }),
                },
            ],
            |it| it.decode_new_leading_trailing(),
        );
    }
}

#[test]
fn test_xor2_chunk_over_flow_panics() {
    common::test_chunk_over_flow_panics(ENC_XOR2, ValueType::Float);
}
