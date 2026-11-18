//! Shared helpers of the chunkenc tests (ports of helpers in `chunk_test.go`, `benchmark_test.go`,
//! `st_helper_test.go`, and Go's `math/rand/v2` PCG source so random test data matches Go exactly).
#![allow(dead_code)]

use chunkenc::Iterator as _;
use chunkenc::{Chunk, Encoding, ValueType, new_empty_chunk};
use model::histogram::{FloatHistogram, Histogram};
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Go `triple` (chunk_test.go).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Triple {
    pub st: i64,
    pub t: i64,
    pub v: f64,
}

pub fn triple(st: i64, t: i64, v: f64) -> Triple {
    Triple { st, t, v }
}

/// Go `sampleCase` (benchmark_test.go).
#[derive(Clone, Debug)]
pub struct SampleCase {
    pub name: String,
    pub samples: Vec<Triple>,
}

/// Go `math/rand/v2.PCG` with the `Rand` methods the tests use (64-bit platform behavior).
pub struct Rand {
    hi: u64,
    lo: u64,
}

impl Rand {
    /// Go `rand.New(rand.NewPCG(seed1, seed2))`.
    pub fn new_pcg(seed1: u64, seed2: u64) -> Rand {
        Rand { hi: seed1, lo: seed2 }
    }

    fn next(&mut self) -> (u64, u64) {
        const MUL_HI: u64 = 2549297995355413924;
        const MUL_LO: u64 = 4865540595714422341;
        const INC_HI: u64 = 6364136223846793005;
        const INC_LO: u64 = 1442695040888963407;
        let m = (self.lo as u128) * (MUL_LO as u128);
        let mut hi = (m >> 64) as u64;
        let lo = m as u64;
        hi = hi.wrapping_add(self.hi.wrapping_mul(MUL_LO)).wrapping_add(self.lo.wrapping_mul(MUL_HI));
        let (lo, c) = lo.overflowing_add(INC_LO);
        let hi = hi.wrapping_add(INC_HI).wrapping_add(c as u64);
        self.lo = lo;
        self.hi = hi;
        (hi, lo)
    }

    /// Go `PCG.Uint64`.
    pub fn uint64(&mut self) -> u64 {
        let (mut hi, lo) = self.next();
        const CHEAP_MUL: u64 = 0xda942042e4dd58b5;
        hi ^= hi >> 32;
        hi = hi.wrapping_mul(CHEAP_MUL);
        hi ^= hi >> 48;
        hi.wrapping_mul(lo | 1)
    }

    fn uint64n(&mut self, n: u64) -> u64 {
        if n & (n - 1) == 0 {
            return self.uint64() & (n - 1);
        }
        let mul = |x: u64| {
            let m = (x as u128) * (n as u128);
            ((m >> 64) as u64, m as u64)
        };
        let (mut hi, mut lo) = mul(self.uint64());
        if lo < n {
            let thresh = n.wrapping_neg() % n;
            while lo < thresh {
                (hi, lo) = mul(self.uint64());
            }
        }
        hi
    }

    /// Go `Rand.IntN`.
    pub fn int_n(&mut self, n: i64) -> i64 {
        assert!(n > 0, "invalid argument to IntN");
        self.uint64n(n as u64) as i64
    }

    /// Go `Rand.Float64`.
    pub fn float64(&mut self) -> f64 {
        ((self.uint64() << 11) >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Runs `f` and returns the panic message, or None if it did not panic.
pub fn panic_message(f: impl FnOnce()) -> Option<String> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(()) => None,
        Err(p) => Some(if let Some(s) = p.downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = p.downcast_ref::<String>() {
            s.clone()
        } else {
            "<non-string panic>".to_string()
        }),
    }
}

/// Go `testChunkOverFlowPanics` (chunk_test.go).
pub fn test_chunk_over_flow_panics(e: Encoding, vt: ValueType) {
    let mut chunk = new_empty_chunk(e).expect("new empty chunk");
    let mut app = chunk.appender().expect("appender");

    // Go: require.PanicsWithValue(t, "chunk capacity exceeded", ...).
    let msg = panic_message(|| {
        for i in 0..1_000_000i64 {
            match vt {
                ValueType::Float => app.append(&mut *chunk, 0, i, i as f64),
                ValueType::Histogram => {
                    let mut h = Histogram {
                        count: i as u64,
                        zero_threshold: 1e-128,
                        zero_count: i as u64,
                        ..Default::default()
                    };
                    let _ = app.append_histogram(&mut *chunk, None, 0, i, &mut h, true);
                }
                ValueType::FloatHistogram => {
                    let mut h = FloatHistogram {
                        count: i as f64,
                        zero_threshold: 1e-128,
                        zero_count: i as f64,
                        ..Default::default()
                    };
                    let _ = app.append_float_histogram(&mut *chunk, None, 0, i, &mut h, true);
                }
                ValueType::None => {}
            }
        }
    });
    assert_eq!(msg.as_deref(), Some("chunk capacity exceeded"));
}

/// Go `testChunkSTHandling` (st_helper_test.go): handling of start times in chunks. It uses 0-4 samples with
/// timestamps 1000, 2000, 3000, 4000 and monotonically increasing start times chosen from 0-(ts-500) for each
/// sample. All combinations of start times are tested for each number of samples.
pub fn test_chunk_st_handling(vt: ValueType, chunk_factory: fn() -> Box<dyn Chunk>) {
    let sample_append = |app: &mut chunkenc::Appender, c: &mut dyn Chunk, ctx: &str, st: i64, ts: i64, v: f64| match vt
    {
        ValueType::Float => app.append(c, st, ts, v),
        ValueType::Histogram => {
            let mut h = Histogram { sum: v, count: (v * 10.0) as u64, ..Default::default() };
            let (_, recoded) = app
                .append_histogram(c, None, st, ts, &mut h, false)
                .unwrap_or_else(|e| panic!("{ctx}: append histogram: {e}"));
            assert!(!recoded, "{ctx}: recoded");
        }
        ValueType::FloatHistogram => {
            let mut h = FloatHistogram { sum: v, count: v * 10.0, ..Default::default() };
            let (_, recoded) = app
                .append_float_histogram(c, None, st, ts, &mut h, false)
                .unwrap_or_else(|e| panic!("{ctx}: append float histogram: {e}"));
            assert!(!recoded, "{ctx}: recoded");
        }
        ValueType::None => panic!("unsupported value type {vt}"),
    };

    let get = |it: &chunkenc::ChunkIterator<'_>| -> (i64, i64, f64) {
        match vt {
            ValueType::Float => {
                let (ts, v) = it.at();
                (it.at_st(), ts, v)
            }
            ValueType::Histogram => {
                let (ts, h) = it.at_histogram(None);
                (it.at_st(), ts, h.sum)
            }
            ValueType::FloatHistogram => {
                let (ts, fh) = it.at_float_histogram(None);
                (it.at_st(), ts, fh.sum)
            }
            ValueType::None => panic!("unsupported value type {vt}"),
        }
    };

    let run_case = |ctx: &str, samples: &[Triple]| {
        let mut chunk = chunk_factory();
        let mut app = chunk.appender().unwrap_or_else(|e| panic!("{ctx}: appender: {e}"));
        let mut clone: Vec<u8> = Vec::new();
        for (i, s) in samples.iter().enumerate() {
            if i == samples.len() - 1 {
                clone.extend_from_slice(chunk.bytes());
            }
            sample_append(&mut app, &mut *chunk, ctx, s.st, s.t, s.v);
        }
        let mut chunks_to_test: Vec<Box<dyn Chunk>> = vec![chunk];

        if !samples.is_empty() {
            // If there are samples, also test that appending to a chunk cloned from the original chunk works
            // correctly. This tests resuming the appender from a previous chunk.
            let mut clone_chunk = chunk_factory();
            clone_chunk.reset(clone);
            let mut clone_app = clone_chunk.appender().unwrap_or_else(|e| panic!("{ctx}: clone appender: {e}"));
            let last = samples[samples.len() - 1];
            sample_append(&mut clone_app, &mut *clone_chunk, ctx, last.st, last.t, last.v);
            chunks_to_test.push(clone_chunk);
        }

        for (ci, chk) in chunks_to_test.iter().enumerate() {
            let name = if ci == 0 { "original" } else { "cloned" };
            assert_eq!(samples.len(), chk.num_samples(), "{ctx}: {name} chunk: number of samples mismatch");
            let mut it = chk.iterator(None);
            for (i, s) in samples.iter().enumerate() {
                assert_eq!(vt, it.next(), "{ctx}: {name}[{i}]: value type mismatch");
                let (st, ts, f) = get(&it);
                assert_eq!(s.t, ts, "{ctx}: {name}[{i}]: timestamp mismatch");
                assert_eq!(s.st, st, "{ctx}: {name}[{i}]: start time mismatch");
                assert!((s.v - f).abs() <= 1e-9, "{ctx}: {name}[{i}]: value mismatch: {} vs {}", s.v, f);
            }
            assert_eq!(ValueType::None, it.next(), "{ctx}: {name}");
            assert!(it.err().is_none(), "{ctx}: {name}: {:?}", it.err());
        }
    };

    // "manual for debugging".
    run_case(
        "manual for debugging",
        &[triple(0, 1000, 1.5), triple(0, 2000, 2.5), triple(0, 3000, 3.5), triple(0, 4000, 4.5)],
    );

    let st_times: [i64; 9] = [0, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000];
    let ts = |j: usize| -> i64 { 1000 * (j as i64 + 1) };

    for number_of_samples in 0..5usize {
        let mut samples = vec![triple(0, 0, 0.0); number_of_samples];
        let mut sample_st_idx = vec![0usize; number_of_samples];
        loop {
            for j in 0..number_of_samples {
                samples[j] = triple(st_times[sample_st_idx[j]], ts(j), j as f64 + 0.5);
            }

            let ctx = format!("{samples:?}");
            run_case(&ctx, &samples);

            let mut exhausted = true;
            for j in (0..number_of_samples).rev() {
                if st_times[sample_st_idx[j]] < ts(j) {
                    sample_st_idx[j] += 1;
                    exhausted = false;
                    break;
                }
                sample_st_idx[j] = 0;
            }
            if exhausted {
                break;
            }
        }
    }
}

/// Go `assertSampleCount` (histogram_test.go): counts the samples of value type `vtype` in the chunk.
pub fn assert_sample_count(c: &dyn Chunk, exp: i64, vtype: ValueType) {
    let mut count = 0i64;
    let mut it = c.iterator(None);
    assert!(it.err().is_none(), "{:?}", it.err());
    while it.next() == vtype {
        count += 1;
    }
    assert!(it.err().is_none(), "{:?}", it.err());
    assert_eq!(exp, count);
}

/// Go `tsdbutil.GenerateTestHistograms`.
pub fn generate_test_histograms(n: usize) -> Vec<Histogram> {
    (0..n)
        .map(|i| {
            let mut h = generate_test_histogram(i as i64);
            if i > 0 {
                h.counter_reset_hint = model::histogram::CounterResetHint::NotCounterReset;
            }
            h
        })
        .collect()
}

/// Go `tsdbutil.GenerateTestHistogram`.
pub fn generate_test_histogram(i: i64) -> Histogram {
    use model::histogram::Span;
    Histogram {
        count: 12 + (i * 9) as u64,
        zero_count: 2 + i as u64,
        zero_threshold: 0.001,
        sum: 18.4 * (i + 1) as f64,
        schema: 1,
        positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
        positive_buckets: vec![i + 1, 1, -1, 0],
        negative_spans: vec![Span::new(0, 2), Span::new(1, 2)],
        negative_buckets: vec![i + 1, 1, -1, 0],
        ..Default::default()
    }
}

/// Go `tsdbutil.GenerateTestCustomBucketsHistogram`.
pub fn generate_test_custom_buckets_histogram(i: i64) -> Histogram {
    use model::histogram::{CUSTOM_BUCKETS_SCHEMA, Span, custom_values};
    Histogram {
        count: 5 + (i * 4) as u64,
        sum: 18.4 * (i + 1) as f64,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
        positive_buckets: vec![i + 1, 1, -1, 0],
        custom_values: custom_values(&[0.0, 1.0, 2.0, 3.0, 4.0]),
        ..Default::default()
    }
}

/// Go `tsdbutil.GenerateTestFloatHistograms`.
pub fn generate_test_float_histograms(n: usize) -> Vec<model::histogram::FloatHistogram> {
    (0..n)
        .map(|i| {
            let mut h = generate_test_float_histogram(i as i64);
            if i > 0 {
                h.counter_reset_hint = model::histogram::CounterResetHint::NotCounterReset;
            }
            h
        })
        .collect()
}

/// Go `tsdbutil.GenerateTestFloatHistogram`.
pub fn generate_test_float_histogram(i: i64) -> model::histogram::FloatHistogram {
    use model::histogram::{FloatHistogram, Span};
    FloatHistogram {
        count: 12.0 + (i * 9) as f64,
        zero_count: 2.0 + i as f64,
        zero_threshold: 0.001,
        sum: 18.4 * (i + 1) as f64,
        schema: 1,
        positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
        positive_buckets: vec![(i + 1) as f64, (i + 2) as f64, (i + 1) as f64, (i + 1) as f64],
        negative_spans: vec![Span::new(0, 2), Span::new(1, 2)],
        negative_buckets: vec![(i + 1) as f64, (i + 2) as f64, (i + 1) as f64, (i + 1) as f64],
        ..Default::default()
    }
}

/// Go `tsdbutil.GenerateTestCustomBucketsFloatHistogram`.
pub fn generate_test_custom_buckets_float_histogram(i: i64) -> model::histogram::FloatHistogram {
    use model::histogram::{CUSTOM_BUCKETS_SCHEMA, FloatHistogram, Span, custom_values};
    FloatHistogram {
        count: 5.0 + (i * 4) as f64,
        sum: 18.4 * (i + 1) as f64,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
        positive_buckets: vec![(i + 1) as f64, (i + 2) as f64, (i + 1) as f64, (i + 1) as f64],
        custom_values: custom_values(&[0.0, 1.0, 2.0, 3.0, 4.0]),
        ..Default::default()
    }
}
