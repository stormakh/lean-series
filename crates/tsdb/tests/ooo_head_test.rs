//! Port of `tsdb/ooo_head_test.go`.

use chunkenc::{ChunkIterator, Encoding, ValueType};
use model::histogram::{CounterResetHint, Span};
use storage::tsdbutil::{generate_test_float_histogram, generate_test_histogram};
use storage::Sample;
use tsdb::ooo_head::OooChunk;

const TEST_MAX_SIZE: usize = 32;

// Formulas chosen to make testing easy.
fn val_even(pos: usize) -> i64 {
    (pos * 2 + 2) as i64
}
fn val_odd(pos: usize) -> i64 {
    (pos * 2 + 1) as i64
}

type SampleFunc = fn(i64, i64) -> Sample;

fn make_even_sample_slice(n: usize, f: SampleFunc) -> Vec<Sample> {
    (0..n)
        .map(|i| {
            let ts = val_even(i);
            f(ts, ts)
        })
        .collect()
}

fn float_sample(st: i64, t: i64) -> Sample {
    Sample::float(st, t, t as f64)
}

fn scenarios() -> Vec<(&'static str, SampleFunc)> {
    vec![
        ("float st=0", |_, ts| float_sample(0, ts)),
        ("float st=ts", |_, ts| float_sample(ts, ts)),
        ("float st=ts-100", |_, ts| float_sample(ts - 100, ts)),
        ("float st descending while t ascending", |_, ts| float_sample(1000 - ts, ts)),
        ("integer histogram st=0", |_, ts| Sample::histogram(0, ts, generate_test_histogram(ts))),
        ("integer histogram st=ts", |_, ts| Sample::histogram(ts, ts, generate_test_histogram(ts))),
        ("float histogram st=0", |_, ts| Sample::float_histogram(0, ts, generate_test_float_histogram(ts))),
        ("float histogram st=ts", |_, ts| Sample::float_histogram(ts, ts, generate_test_float_histogram(ts))),
    ]
}

fn insert(chunk: &mut OooChunk, s: Sample) -> bool {
    chunk.insert(s.st, s.t, s.f, s.h, s.fh)
}

#[test]
fn test_ooo_insert() {
    for (name, sample_func) in scenarios() {
        for num_pre_existing in 0..=TEST_MAX_SIZE {
            for insert_pos in 0..=num_pre_existing {
                let mut chunk = OooChunk::new();
                chunk.samples = make_even_sample_slice(num_pre_existing, sample_func);
                let ts = val_odd(insert_pos);
                let new_sample = sample_func(ts, ts);
                insert(&mut chunk, new_sample.clone());

                let mut exp_samples = Vec::new();
                for i in 0..insert_pos {
                    let ts = val_even(i);
                    exp_samples.push(sample_func(ts, ts));
                }
                exp_samples.push(new_sample);
                for i in insert_pos..num_pre_existing {
                    let ts = val_even(i);
                    exp_samples.push(sample_func(ts, ts));
                }
                assert_eq!(
                    exp_samples, chunk.samples,
                    "{name}: numPreExisting {num_pre_existing}, insertPos {insert_pos}"
                );
            }
        }
    }
}

#[test]
fn test_ooo_insert_duplicate() {
    for (name, sample_func) in scenarios() {
        for num in 1..=TEST_MAX_SIZE {
            for dup_pos in 0..num {
                let mut chunk = OooChunk::new();
                chunk.samples = make_even_sample_slice(num, sample_func);
                let mut dup_sample = chunk.samples[dup_pos].clone();
                dup_sample.f = 0.123;
                let ok = insert(&mut chunk, dup_sample);
                let exp_samples = make_even_sample_slice(num, sample_func); // We expect no change.
                assert!(!ok);
                assert_eq!(exp_samples, chunk.samples, "{name}: num {num}, dupPos {dup_pos}");
            }
        }
    }
}

struct ChunkVerify {
    encoding: Encoding,
    min_time: i64,
    max_time: i64,
}

fn cv(encoding: Encoding, min_time: i64, max_time: i64) -> ChunkVerify {
    ChunkVerify {
        encoding,
        min_time,
        max_time,
    }
}

#[test]
fn test_ooo_chunks_to_encoded_chunks() {
    use chunkenc::{ENC_FLOAT_HISTOGRAM, ENC_HISTOGRAM, ENC_XOR};
    use CounterResetHint::*;
    let h1 = generate_test_histogram(1);
    // Make h2 appendable but with more buckets, to trigger recoding.
    let mut h2 = h1.clone();
    h2.positive_spans.push(Span { offset: 1, length: 1 });
    h2.positive_buckets.push(12);
    let mut h2explicit = h2.clone();
    h2explicit.counter_reset_hint = CounterReset;

    let f = |t: i64, v: f64| Sample::float(0, t, v);
    let h = |t: i64, h: &model::histogram::Histogram| Sample::histogram(0, t, h.clone());

    let cases: Vec<(&str, Vec<Sample>, Vec<CounterResetHint>, Vec<ChunkVerify>)> = vec![
        ("empty", vec![], vec![], vec![]),
        (
            "has floats",
            vec![f(1000, 43.0), f(1100, 42.0)],
            vec![UnknownCounterReset, UnknownCounterReset],
            vec![cv(ENC_XOR, 1000, 1100)],
        ),
        (
            "mix of floats and histograms",
            vec![f(1000, 43.0), h(1100, &h1), f(1200, 42.0)],
            vec![UnknownCounterReset, UnknownCounterReset, UnknownCounterReset],
            vec![cv(ENC_XOR, 1000, 1000), cv(ENC_HISTOGRAM, 1100, 1100), cv(ENC_XOR, 1200, 1200)],
        ),
        (
            "has an implicit counter reset",
            vec![h(1000, &h2), h(1100, &h1)],
            vec![UnknownCounterReset, UnknownCounterReset],
            vec![cv(ENC_HISTOGRAM, 1000, 1000), cv(ENC_HISTOGRAM, 1100, 1100)],
        ),
        (
            "has an explicit counter reset",
            vec![h(1100, &h2explicit)],
            vec![UnknownCounterReset],
            vec![cv(ENC_HISTOGRAM, 1100, 1100)],
        ),
        (
            "has an explicit counter reset inside",
            vec![h(1000, &h1), h(1100, &h2explicit)],
            vec![UnknownCounterReset, UnknownCounterReset],
            vec![cv(ENC_HISTOGRAM, 1000, 1000), cv(ENC_HISTOGRAM, 1100, 1100)],
        ),
        (
            "has a recoded histogram",
            vec![h(0, &h1), h(1, &h2)],
            vec![UnknownCounterReset, NotCounterReset],
            vec![cv(ENC_HISTOGRAM, 0, 1)],
        ),
    ];

    for (name, samples, expected_counter_resets, expected_chunks) in cases {
        // Sanity check.
        assert_eq!(expected_counter_resets.len(), samples.len(), "{name}: number of samples and counter resets");
        let mut ooo_chunk = OooChunk::default();
        for s in &samples {
            match s.typ() {
                ValueType::Float => {
                    ooo_chunk.insert(s.st, s.t, s.f, None, None);
                }
                ValueType::Histogram => {
                    ooo_chunk.insert(s.st, s.t, 0.0, s.h.clone(), None);
                }
                ValueType::FloatHistogram => {
                    ooo_chunk.insert(s.st, s.t, 0.0, None, s.fh.clone());
                }
                t => panic!("unexpected sample type {t}"),
            }
        }
        let chunks = ooo_chunk.to_encoded_chunks(i64::MIN, i64::MAX, false, false).expect("encode");
        assert_eq!(expected_chunks.len(), chunks.len(), "{name}: number of chunks");
        let mut sample_index = 0usize;
        for (i, c) in chunks.iter().enumerate() {
            assert_eq!(expected_chunks[i].encoding, c.chunk.encoding(), "{name}: chunk {i} encoding");
            assert_eq!(expected_chunks[i].min_time, c.min_time, "{name}: chunk {i} minTime");
            assert_eq!(expected_chunks[i].max_time, c.max_time, "{name}: chunk {i} maxTime");
            let mut it = c.chunk.iterator(None);
            let got = storage::expand_samples(&mut it);
            let got = got.expect("expand");
            assert!(samples.len() - sample_index >= got.len(), "{name}: too many samples in chunk {i}");
            if got.is_empty() {
                // Ignore empty chunks.
                continue;
            }
            match c.chunk.encoding() {
                ENC_XOR => {
                    for (j, s) in got.iter().enumerate() {
                        assert_eq!(ValueType::Float, s.typ());
                        // XOR chunks don't have counter reset hints.
                        assert_eq!(UnknownCounterReset, expected_counter_resets[sample_index + j]);
                        assert_eq!(samples[sample_index + j].f, s.f, "{name}: sample {}", sample_index + j);
                    }
                }
                ENC_HISTOGRAM => {
                    for (j, s) in got.iter().enumerate() {
                        assert_eq!(ValueType::Histogram, s.typ());
                        let gh = s.h().expect("histogram");
                        assert_eq!(expected_counter_resets[sample_index + j], gh.counter_reset_hint);
                        let mut compare_to = samples[sample_index + j].h.as_deref().expect("h").clone();
                        compare_to.counter_reset_hint = expected_counter_resets[sample_index + j];
                        let mut got_h = gh.clone();
                        got_h.compact(0);
                        assert_eq!(compare_to, got_h, "{name}: sample {}", sample_index + j);
                    }
                }
                ENC_FLOAT_HISTOGRAM => {
                    for (j, s) in got.iter().enumerate() {
                        assert_eq!(ValueType::FloatHistogram, s.typ());
                        let gh = s.fh().expect("float histogram");
                        assert_eq!(expected_counter_resets[sample_index + j], gh.counter_reset_hint);
                        let mut compare_to = samples[sample_index + j].fh.as_deref().expect("fh").clone();
                        compare_to.counter_reset_hint = expected_counter_resets[sample_index + j];
                        let mut got_h = gh.clone();
                        got_h.compact(0);
                        assert_eq!(compare_to, got_h, "{name}: sample {}", sample_index + j);
                    }
                }
                _ => {}
            }
            sample_index += got.len();
        }
        assert_eq!(samples.len(), sample_index, "{name}: number of samples");
    }
}

#[test]
fn test_ooo_chunks_to_encoded_chunks_with_st() {
    use chunkenc::{
        ENC_FLOAT_HISTOGRAM, ENC_FLOAT_HISTOGRAM_ST, ENC_HISTOGRAM, ENC_HISTOGRAM_ST, ENC_XOR, ENC_XOR2,
    };
    let h1 = generate_test_histogram(1);
    let h2 = generate_test_histogram(2);
    let h3 = generate_test_histogram(3);
    let fh1 = generate_test_float_histogram(1);
    let fh2 = generate_test_float_histogram(2);
    let fh3 = generate_test_float_histogram(3);
    let f = Sample::float;
    let h = |st, t, h: &model::histogram::Histogram| Sample::histogram(st, t, h.clone());
    let fh = |st, t, h: &model::histogram::FloatHistogram| Sample::float_histogram(st, t, h.clone());

    let cases: Vec<(&str, Vec<Sample>, Encoding, Encoding)> = vec![
        ("floats with st=0", vec![f(0, 1000, 43.0), f(0, 1100, 42.0)], ENC_XOR2, ENC_XOR),
        ("floats with st=t", vec![f(1000, 1000, 43.0), f(1100, 1100, 42.0)], ENC_XOR2, ENC_XOR),
        ("floats with st=t-100", vec![f(900, 1000, 43.0), f(1000, 1100, 42.0)], ENC_XOR2, ENC_XOR),
        (
            "floats with varying st",
            vec![f(500, 1000, 43.0), f(1100, 1100, 42.0), f(0, 1200, 41.0)],
            ENC_XOR2,
            ENC_XOR,
        ),
        ("histograms with st=0", vec![h(0, 1000, &h1), h(0, 1100, &h2)], ENC_HISTOGRAM_ST, ENC_HISTOGRAM),
        ("histograms with st=t-100", vec![h(900, 1000, &h1), h(1000, 1100, &h2)], ENC_HISTOGRAM_ST, ENC_HISTOGRAM),
        (
            "histograms with varying st",
            vec![h(500, 1000, &h1), h(1100, 1100, &h2), h(0, 1200, &h3)],
            ENC_HISTOGRAM_ST,
            ENC_HISTOGRAM,
        ),
        (
            "float histograms with st=0",
            vec![fh(0, 1000, &fh1), fh(0, 1100, &fh2)],
            ENC_FLOAT_HISTOGRAM_ST,
            ENC_FLOAT_HISTOGRAM,
        ),
        (
            "float histograms with st=t-100",
            vec![fh(900, 1000, &fh1), fh(1000, 1100, &fh2)],
            ENC_FLOAT_HISTOGRAM_ST,
            ENC_FLOAT_HISTOGRAM,
        ),
        (
            "float histograms with varying st",
            vec![fh(500, 1000, &fh1), fh(1100, 1100, &fh2), fh(0, 1200, &fh3)],
            ENC_FLOAT_HISTOGRAM_ST,
            ENC_FLOAT_HISTOGRAM,
        ),
    ];

    for (name, samples, st_encoding, regular_encoding) in cases {
        for store_st in [true, false] {
            let mut ooo_chunk = OooChunk::default();
            for s in &samples {
                ooo_chunk.insert(s.st, s.t, s.f, s.h.clone(), s.fh.clone());
            }
            let chunks = ooo_chunk.to_encoded_chunks(i64::MIN, i64::MAX, store_st, store_st).expect("encode");
            assert_eq!(1, chunks.len(), "{name}/{store_st}: number of chunks");
            let c = &chunks[0];
            let expected_enc = if store_st { st_encoding } else { regular_encoding };
            assert_eq!(expected_enc, c.chunk.encoding(), "{name}/{store_st}: chunk encoding");
            assert_eq!(samples[0].t, c.min_time, "chunk minTime");
            assert_eq!(samples[samples.len() - 1].t, c.max_time, "chunk maxTime");

            // Verify samples can be read back.
            let mut it = c.chunk.iterator(None);
            let mut sample_index = 0;
            loop {
                let vt = it.next();
                if vt == ValueType::None {
                    break;
                }
                let exp = &samples[sample_index];
                match vt {
                    ValueType::Float => {
                        let (got_t, got_f) = it.at();
                        assert_eq!(exp.t, got_t, "sample {sample_index} t");
                        assert_eq!(exp.f, got_f, "sample {sample_index} f");
                    }
                    ValueType::Histogram => {
                        let (got_t, mut got_h) = it.at_histogram(None);
                        assert_eq!(exp.t, got_t, "sample {sample_index} t");
                        let mut exp_h = exp.h.as_deref().expect("h").clone();
                        exp_h.counter_reset_hint = got_h.counter_reset_hint;
                        got_h.compact(0);
                        assert_eq!(exp_h, got_h, "sample {sample_index} h");
                    }
                    ValueType::FloatHistogram => {
                        let (got_t, mut got_fh) = it.at_float_histogram(None);
                        assert_eq!(exp.t, got_t, "sample {sample_index} t");
                        let mut exp_fh = exp.fh.as_deref().expect("fh").clone();
                        exp_fh.counter_reset_hint = got_fh.counter_reset_hint;
                        got_fh.compact(0);
                        assert_eq!(exp_fh, got_fh, "sample {sample_index} fh");
                    }
                    ValueType::None => unreachable!(),
                }
                let got_st = it.at_st();
                if store_st {
                    assert_eq!(exp.st, got_st, "sample {sample_index} st");
                } else {
                    assert_eq!(0, got_st, "sample {sample_index} st should be 0 when useXOR2=false");
                }
                sample_index += 1;
            }
            assert!(it.err().is_none());
            assert_eq!(samples.len(), sample_index, "number of samples");
        }
    }
}
