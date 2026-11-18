//! Port of `storage/series_test.go`.

use chunkenc::{
    AnyIterator, ChunkIterator, CounterResetHeader, ENC_FLOAT_HISTOGRAM, ENC_FLOAT_HISTOGRAM_ST,
    ENC_HISTOGRAM, ENC_HISTOGRAM_ST, ENC_XOR, ENC_XOR2, Encoding, FloatHistogramChunk,
    HistogramChunk, ValueType,
};
use model::histogram::{CounterResetHint, FloatHistogram, Histogram, Span};
use model::labels::{self, Labels};
use model::value::{STALE_NAN, is_stale_nan};
use storage::chunks::chunk_metas_to_samples;
use storage::tsdbutil::*;
use storage::*;

#[test]
fn test_list_series_iterator() {
    let samples = vec![
        Sample::float(-10, 0, 0.0),
        Sample::float(-9, 1, 1.0),
        Sample::float(-8, 1, 1.5),
        Sample::float(-7, 2, 2.0),
        Sample::float(-6, 3, 3.0),
    ];
    let mut it = new_list_series_iterator(&samples);

    // Seek to the first sample with ts=1.
    assert_eq!(it.seek(1), ValueType::Float);
    assert_eq!(it.at(), (1, 1.0));
    assert_eq!(it.at_st(), -9);

    // Seek one further, next sample still has ts=1.
    assert_eq!(it.next(), ValueType::Float);
    assert_eq!(it.at(), (1, 1.5));
    assert_eq!(it.at_st(), -8);

    // Seek again to 1 and make sure we stay where we are.
    assert_eq!(it.seek(1), ValueType::Float);
    assert_eq!(it.at(), (1, 1.5));
    assert_eq!(it.at_st(), -8);

    // Another seek.
    assert_eq!(it.seek(3), ValueType::Float);
    assert_eq!(it.at(), (3, 3.0));
    assert_eq!(it.at_st(), -6);

    // And we don't go back.
    assert_eq!(it.seek(2), ValueType::Float);
    assert_eq!(it.at(), (3, 3.0));
    assert_eq!(it.at_st(), -6);

    // Seek beyond the end.
    assert_eq!(it.seek(5), ValueType::None);
    // And we don't go back. (This exposes issue #10027.)
    assert_eq!(it.seek(2), ValueType::None);
}

/// Go `mockChunkSeriesSet` (from merge_test.go).
struct MockChunkSeriesSet {
    idx: isize,
    series: Vec<ChunkSeriesArc>,
}

fn new_mock_chunk_series_set(series: Vec<ChunkSeriesArc>) -> BoxChunkSeriesSet {
    Box::new(MockChunkSeriesSet { idx: -1, series })
}

impl ChunkSeriesSet for MockChunkSeriesSet {
    fn next(&mut self) -> bool {
        self.idx += 1;
        (self.idx as usize) < self.series.len()
    }
    fn at(&self) -> &ChunkSeriesArc {
        &self.series[self.idx as usize]
    }
    fn err(&self) -> Option<Error> {
        None
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

/// The property of SeriesSet that returned series are iterable even after Next is called.
#[test]
fn test_chunk_series_set_to_series_set() {
    let series: Vec<(Labels, Vec<Sample>)> = vec![
        (
            labels::from_strings(&["__name__", "up", "instance", "localhost:8080"]),
            vec![
                Sample::float(0, 1, 1.0),
                Sample::float(0, 2, 2.0),
                Sample::float(0, 3, 3.0),
                Sample::float(0, 4, 4.0),
            ],
        ),
        (
            labels::from_strings(&["__name__", "up", "instance", "localhost:8081"]),
            vec![
                Sample::float(0, 1, 2.0),
                Sample::float(0, 2, 3.0),
                Sample::float(0, 3, 4.0),
                Sample::float(0, 4, 5.0),
                Sample::float(0, 5, 6.0),
                Sample::float(0, 6, 7.0),
            ],
        ),
    ];
    let chunk_series: Vec<ChunkSeriesArc> = series
        .iter()
        .map(|(l, s)| new_list_chunk_series_from_samples(l.clone(), &[s.as_slice()]))
        .collect();
    let css = new_mock_chunk_series_set(chunk_series);

    let mut ss = new_series_set_from_chunk_series_set(css);
    let mut ss_slice: Vec<SeriesArc> = Vec::new();
    while ss.next() {
        ss_slice.push(ss.at().clone());
    }
    assert_eq!(ss_slice.len(), 2);
    let mut iter: Option<AnyIterator<'_>> = None;
    for (i, s) in ss_slice.iter().enumerate() {
        assert_eq!(s.labels(), &series[i].0);
        let mut it = s.iterator(iter.take());
        let mut j = 0;
        while it.next() == ValueType::Float {
            let (ts, v) = it.at();
            assert_eq!(Sample::float(0, ts, v), series[i].1[j]);
            j += 1;
        }
        iter = Some(it);
    }
}

fn histogram_sample(ts: i64, hint: CounterResetHint) -> Sample {
    let mut h = generate_test_histogram(ts + 1);
    h.counter_reset_hint = hint;
    Sample::histogram(0, ts, h)
}

fn float_histogram_sample(ts: i64, hint: CounterResetHint) -> Sample {
    let mut fh = generate_test_float_histogram(ts + 1);
    fh.counter_reset_hint = hint;
    Sample::float_histogram(0, ts, fh)
}

const UK: CounterResetHint = CounterResetHint::UnknownCounterReset;

struct HistogramTest {
    samples: Vec<Sample>,
    expected_samples: Vec<Sample>,
    expected_counter_reset_headers: Vec<CounterResetHeader>,
}

fn spans(s: &[(i32, u32)]) -> Vec<Span> {
    s.iter()
        .map(|&(offset, length)| Span { offset, length })
        .collect()
}

fn hist(hint: CounterResetHint, count: u64, sp: &[(i32, u32)], buckets: &[i64]) -> Histogram {
    Histogram {
        counter_reset_hint: hint,
        count,
        zero_count: 2,
        zero_threshold: 0.001,
        sum: 100.0,
        schema: 0,
        positive_spans: spans(sp),
        positive_buckets: buckets.to_vec(),
        ..Default::default()
    }
}

fn fhist(hint: CounterResetHint, count: f64, sp: &[(i32, u32)], buckets: &[f64]) -> FloatHistogram {
    FloatHistogram {
        counter_reset_hint: hint,
        count,
        zero_count: 2.0,
        zero_threshold: 0.001,
        sum: 100.0,
        schema: 0,
        positive_spans: spans(sp),
        positive_buckets: buckets.to_vec(),
        ..Default::default()
    }
}

#[test]
fn test_histogram_series_to_chunks() {
    let ga = CounterResetHint::GaugeType;
    let h1 = hist(UK, 7, &[(0, 2)], &[2, 1]); // Abs: 2, 3
    // h1 but with an extra empty bucket at offset -10.
    // This can happen if h1 is from a recoded chunk, where a later histogram had a bucket at offset -10.
    let h1_extra_buckets = hist(UK, 7, &[(-10, 1), (9, 2)], &[0, 2, 1]); // Abs: 0, 2, 3
    let h1_recoded = hist(UK, 7, &[(0, 2), (1, 2)], &[2, 1, -3, 0]); // Abs: 2, 3, 0, 0
    // Appendable to h1.
    let h2 = hist(UK, 12, &[(0, 2), (1, 2)], &[2, 1, -2, 3]); // Abs: 2, 3, 1, 4
    // Implicit counter reset by reduction in buckets, not appendable.
    let h2down = hist(UK, 10, &[(0, 2), (1, 2)], &[1, 1, -1, 3]); // Abs: 1, 2, 1, 4

    let fh1 = fhist(UK, 6.0, &[(0, 2)], &[3.0, 1.0]);
    // fh1 but with an extra empty bucket at offset -10.
    let fh1_extra_buckets = fhist(UK, 6.0, &[(-10, 1), (9, 2)], &[0.0, 3.0, 1.0]);
    let fh1_recoded = fhist(UK, 6.0, &[(0, 2), (1, 2)], &[3.0, 1.0, 0.0, 0.0]);
    // Appendable to fh1.
    let fh2 = fhist(UK, 17.0, &[(0, 2), (1, 2)], &[4.0, 2.0, 7.0, 2.0]);
    // Implicit counter reset by reduction in buckets, not appendable.
    let fh2down = fhist(UK, 15.0, &[(0, 2), (1, 2)], &[2.0, 2.0, 7.0, 2.0]);

    // Gauge histogram.
    let gh1 = hist(ga, 7, &[(0, 2)], &[2, 1]); // Abs: 2, 3
    // gh1 recoded to add extra empty buckets at end.
    let gh1_recoded = hist(ga, 7, &[(0, 2), (1, 2)], &[2, 1, -3, 0]); // Abs: 2, 3, 0, 0
    let gh2 = hist(ga, 12, &[(0, 2), (1, 2)], &[2, 1, -2, 3]); // Abs: 2, 3, 1, 4

    // Float gauge histogram.
    let gfh1 = fhist(ga, 6.0, &[(0, 2)], &[3.0, 1.0]);
    // gfh1 recoded to add an extra empty buckets at end.
    let gfh1_recoded = fhist(ga, 6.0, &[(0, 2), (1, 2)], &[3.0, 1.0, 0.0, 0.0]);
    let gfh2 = fhist(ga, 17.0, &[(0, 2), (1, 2)], &[4.0, 2.0, 7.0, 2.0]);

    let stale_histogram = Histogram {
        sum: f64::from_bits(STALE_NAN),
        ..Default::default()
    };
    let stale_float_histogram = FloatHistogram {
        sum: f64::from_bits(STALE_NAN),
        ..Default::default()
    };

    let h = |t: i64, h: &Histogram| Sample::histogram(0, t, h.clone());
    let fh = |t: i64, fh: &FloatHistogram| Sample::float_histogram(0, t, fh.clone());
    use CounterResetHeader as C;
    let unk = C::UNKNOWN_COUNTER_RESET;
    let cr = C::COUNTER_RESET;
    let gauge = C::GAUGE_TYPE;

    let tests: Vec<(&str, HistogramTest)> = vec![
        (
            "single histogram to single chunk",
            HistogramTest {
                samples: vec![h(1, &h1)],
                expected_samples: vec![h(1, &h1)],
                expected_counter_reset_headers: vec![unk],
            },
        ),
        (
            "two histograms encoded to a single chunk",
            HistogramTest {
                samples: vec![h(1, &h1), h(2, &h2)],
                expected_samples: vec![h(1, &h1_recoded), h(2, &h2)],
                expected_counter_reset_headers: vec![unk],
            },
        ),
        (
            "two histograms encoded to two chunks",
            HistogramTest {
                samples: vec![h(1, &h2), h(2, &h1)],
                expected_samples: vec![h(1, &h2), h(2, &h1)],
                expected_counter_reset_headers: vec![unk, cr],
            },
        ),
        (
            "histogram and stale sample encoded to two chunks",
            HistogramTest {
                samples: vec![h(1, &stale_histogram), h(2, &h1)],
                expected_samples: vec![h(1, &stale_histogram), h(2, &h1)],
                expected_counter_reset_headers: vec![unk, unk],
            },
        ),
        (
            "histogram and reduction in bucket encoded to two chunks",
            HistogramTest {
                samples: vec![h(1, &h1), h(2, &h2down)],
                expected_samples: vec![h(1, &h1), h(2, &h2down)],
                expected_counter_reset_headers: vec![unk, cr],
            },
        ),
        // Float histograms.
        (
            "single float histogram to single chunk",
            HistogramTest {
                samples: vec![fh(1, &fh1)],
                expected_samples: vec![fh(1, &fh1)],
                expected_counter_reset_headers: vec![unk],
            },
        ),
        (
            "two float histograms encoded to a single chunk",
            HistogramTest {
                samples: vec![fh(1, &fh1), fh(2, &fh2)],
                expected_samples: vec![fh(1, &fh1_recoded), fh(2, &fh2)],
                expected_counter_reset_headers: vec![unk],
            },
        ),
        (
            "two float histograms encoded to two chunks",
            HistogramTest {
                samples: vec![fh(1, &fh2), fh(2, &fh1)],
                expected_samples: vec![fh(1, &fh2), fh(2, &fh1)],
                expected_counter_reset_headers: vec![unk, cr],
            },
        ),
        (
            "float histogram and stale sample encoded to two chunks",
            HistogramTest {
                samples: vec![fh(1, &stale_float_histogram), fh(2, &fh1)],
                expected_samples: vec![fh(1, &stale_float_histogram), fh(2, &fh1)],
                expected_counter_reset_headers: vec![unk, unk],
            },
        ),
        (
            "float histogram and reduction in bucket encoded to two chunks",
            HistogramTest {
                samples: vec![fh(1, &fh1), fh(2, &fh2down)],
                expected_samples: vec![fh(1, &fh1), fh(2, &fh2down)],
                expected_counter_reset_headers: vec![unk, cr],
            },
        ),
        // Mixed.
        (
            "histogram and float histogram encoded to two chunks",
            HistogramTest {
                samples: vec![h(1, &h1), fh(2, &fh2)],
                expected_samples: vec![h(1, &h1), fh(2, &fh2)],
                expected_counter_reset_headers: vec![unk, unk],
            },
        ),
        (
            "float histogram and histogram encoded to two chunks",
            HistogramTest {
                samples: vec![fh(1, &fh1), h(2, &h2)],
                expected_samples: vec![fh(1, &fh1), h(2, &h2)],
                expected_counter_reset_headers: vec![unk, unk],
            },
        ),
        (
            "histogram and stale float histogram encoded to two chunks",
            HistogramTest {
                samples: vec![h(1, &h1), fh(2, &stale_float_histogram)],
                expected_samples: vec![h(1, &h1), fh(2, &stale_float_histogram)],
                expected_counter_reset_headers: vec![unk, unk],
            },
        ),
        (
            "single gauge histogram encoded to one chunk",
            HistogramTest {
                samples: vec![h(1, &gh1)],
                expected_samples: vec![h(1, &gh1)],
                expected_counter_reset_headers: vec![gauge],
            },
        ),
        (
            "two gauge histograms encoded to one chunk when counter increases",
            HistogramTest {
                samples: vec![h(1, &gh1), h(2, &gh2)],
                expected_samples: vec![h(1, &gh1_recoded), h(2, &gh2)],
                expected_counter_reset_headers: vec![gauge],
            },
        ),
        (
            "two gauge histograms encoded to one chunk when counter decreases",
            HistogramTest {
                samples: vec![h(1, &gh2), h(2, &gh1)],
                expected_samples: vec![h(1, &gh2), h(2, &gh1_recoded)],
                expected_counter_reset_headers: vec![gauge],
            },
        ),
        (
            "single gauge float histogram encoded to one chunk",
            HistogramTest {
                samples: vec![fh(1, &gfh1)],
                expected_samples: vec![fh(1, &gfh1)],
                expected_counter_reset_headers: vec![gauge],
            },
        ),
        (
            "two float gauge histograms encoded to one chunk when counter increases",
            HistogramTest {
                samples: vec![fh(1, &gfh1), fh(2, &gfh2)],
                expected_samples: vec![fh(1, &gfh1_recoded), fh(2, &gfh2)],
                expected_counter_reset_headers: vec![gauge],
            },
        ),
        (
            "two float gauge histograms encoded to one chunk when counter decreases",
            HistogramTest {
                samples: vec![fh(1, &gfh2), fh(2, &gfh1)],
                expected_samples: vec![fh(1, &gfh2), fh(2, &gfh1_recoded)],
                expected_counter_reset_headers: vec![gauge],
            },
        ),
        (
            "histogram with extra empty bucket followed by histogram encodes to one chunk",
            HistogramTest {
                samples: vec![h(1, &h1_extra_buckets), h(2, &h1)],
                // The second one is recoded to add the missing buckets.
                expected_samples: vec![h(1, &h1_extra_buckets), h(2, &h1_extra_buckets)],
                expected_counter_reset_headers: vec![unk],
            },
        ),
        (
            "float histogram with extra empty bucket followed by float histogram encodes to one chunk",
            HistogramTest {
                samples: vec![fh(1, &fh1_extra_buckets), fh(2, &fh1)],
                // The second one is recoded to add the missing buckets.
                expected_samples: vec![fh(1, &fh1_extra_buckets), fh(2, &fh1_extra_buckets)],
                expected_counter_reset_headers: vec![unk],
            },
        ),
    ];

    for (name, test) in &tests {
        test_histograms_series_to_chunks(name, test);
    }
}

fn test_histograms_series_to_chunks(name: &str, test: &HistogramTest) {
    let lbs = labels::from_strings(&["__name__", "up", "instance", "localhost:8080"]);
    let copied_samples: Vec<Sample> = test
        .samples
        .iter()
        .map(|s| match s.typ() {
            ValueType::Histogram => Sample::histogram(0, s.t, s.h().expect("histogram").copy()),
            ValueType::FloatHistogram => {
                Sample::float_histogram(0, s.t, s.fh().expect("float histogram").copy())
            }
            _ => panic!("{}: internal error, unexpected type", name),
        })
        .collect();
    let series = new_list_series(lbs.clone(), copied_samples);
    let encoder = new_series_to_chunk_encoder(series);
    assert_eq!(encoder.labels(), &lbs, "{}", name);

    let chks = expand_chunks(&mut *encoder.iterator(None)).expect("no error");
    assert_eq!(
        chks.len(),
        test.expected_counter_reset_headers.len(),
        "{}",
        name
    );

    // Decode all encoded samples and assert they are equal to the original ones.
    let encoded_samples = chunk_metas_to_samples(&chks);
    assert_eq!(
        encoded_samples.len(),
        test.expected_samples.len(),
        "{}",
        name
    );

    for (i, expected) in test.expected_samples.iter().enumerate() {
        let encoded = &encoded_samples[i];
        match expected.typ() {
            ValueType::Histogram => {
                assert_eq!(
                    encoded.typ(),
                    ValueType::Histogram,
                    "{}: expect histogram at idx {}",
                    name,
                    i
                );
                let eh = expected.h().expect("histogram");
                let mut h = encoded.h().expect("histogram").clone();
                // Ignore counter reset if not gauge here, will check on chunk level.
                if eh.counter_reset_hint != CounterResetHint::GaugeType {
                    h.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                }
                if is_stale_nan(eh.sum) {
                    assert!(is_stale_nan(h.sum), "{}: at idx {}", name, i);
                    continue;
                }
                assert_eq!(*eh, h, "{}: at idx {}", name, i);
            }
            ValueType::FloatHistogram => {
                assert_eq!(
                    encoded.typ(),
                    ValueType::FloatHistogram,
                    "{}: expect float histogram at idx {}",
                    name,
                    i
                );
                let efh = expected.fh().expect("float histogram");
                let mut fh = encoded.fh().expect("float histogram").clone();
                // Ignore counter reset if not gauge here, will check on chunk level.
                if efh.counter_reset_hint != CounterResetHint::GaugeType {
                    fh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                }
                if is_stale_nan(efh.sum) {
                    assert!(is_stale_nan(fh.sum), "{}: at idx {}", name, i);
                    continue;
                }
                assert_eq!(*efh, fh, "{}: at idx {}", name, i);
            }
            _ => panic!("{}: internal error, unexpected type", name),
        }
    }

    for (i, expected) in test.expected_counter_reset_headers.iter().enumerate() {
        assert_eq!(
            *expected,
            get_counter_reset_hint(&chks[i]),
            "{}: chunk at index {}",
            name,
            i
        );
    }
}

#[test]
fn test_series_to_chunk_encoder_float_encoding() {
    const SPLIT: i64 = SERIES_TO_CHUNK_ENCODER_SPLIT as i64;
    let lbs = labels::from_strings(&["__name__", "up"]);

    let many_floats =
        |n: i64| -> Vec<Sample> { (0..n).map(|i| Sample::float(0, i, i as f64)).collect() };
    let many_floats_with_st = |n: i64| -> Vec<Sample> {
        (0..n)
            .map(|i| Sample::float(i * 10 + 5, (i + 1) * 10, i as f64))
            .collect()
    };
    let histogram_sample_with_st = |st: i64, ts: i64| {
        let mut s = histogram_sample(ts, UK);
        s.st = st;
        s
    };
    let float_histogram_sample_with_st = |st: i64, ts: i64| {
        let mut s = float_histogram_sample(ts, UK);
        s.st = st;
        s
    };

    let floats_no_st = vec![
        Sample::float(0, 10, 1.0),
        Sample::float(0, 20, 2.0),
        Sample::float(0, 30, 3.0),
    ];
    let floats_all_st = vec![
        Sample::float(5, 10, 1.0),
        Sample::float(15, 20, 2.0),
        Sample::float(25, 30, 3.0),
    ];
    let floats_mixed_st = vec![
        Sample::float(0, 10, 1.0),
        Sample::float(0, 20, 2.0),
        Sample::float(15, 30, 3.0),
        Sample::float(0, 40, 4.0),
    ];

    struct Case {
        name: &'static str,
        use_default_constructor: bool,
        float_encoding: Encoding,
        samples: Vec<Sample>,
        /// The expected chunks: the encoding and the samples after a round trip through the chunk.
        expected: Vec<(Encoding, Vec<Sample>)>,
    }
    let d = |name, samples, expected| Case {
        name,
        use_default_constructor: true,
        float_encoding: Encoding(0),
        samples,
        expected,
    };
    let e = |name, float_encoding, samples, expected| Case {
        name,
        use_default_constructor: false,
        float_encoding,
        samples,
        expected,
    };

    let mut xor2_st_last = many_floats(SPLIT - 1);
    xor2_st_last.push(Sample::float(5, SPLIT, SPLIT as f64));
    let xor2_st_last_first_chunk = xor2_st_last.clone();
    xor2_st_last.push(Sample::float(15, SPLIT + 1, (SPLIT + 1) as f64));

    let cases = vec![
        d(
            "default, no ST",
            floats_no_st.clone(),
            vec![(ENC_XOR, floats_no_st.clone())],
        ),
        d(
            "default, all ST",
            floats_all_st.clone(),
            vec![(ENC_XOR2, floats_all_st.clone())],
        ),
        d(
            "default, mixed ST cuts XOR chunk on first ST only",
            floats_mixed_st.clone(),
            vec![
                (ENC_XOR, floats_mixed_st[..2].to_vec()),
                (ENC_XOR2, floats_mixed_st[2..].to_vec()),
            ],
        ),
        e(
            "xor, no ST",
            ENC_XOR,
            floats_no_st.clone(),
            vec![(ENC_XOR, floats_no_st.clone())],
        ),
        e(
            "xor, all ST",
            ENC_XOR,
            floats_all_st.clone(),
            vec![(ENC_XOR2, floats_all_st.clone())],
        ),
        e(
            "xor, mixed ST cuts XOR chunk on first ST only",
            ENC_XOR,
            floats_mixed_st.clone(),
            vec![
                (ENC_XOR, floats_mixed_st[..2].to_vec()),
                (ENC_XOR2, floats_mixed_st[2..].to_vec()),
            ],
        ),
        e(
            "xor2, no ST",
            ENC_XOR2,
            floats_no_st.clone(),
            vec![(ENC_XOR2, floats_no_st.clone())],
        ),
        e(
            "xor2, all ST",
            ENC_XOR2,
            floats_all_st.clone(),
            vec![(ENC_XOR2, floats_all_st.clone())],
        ),
        e(
            "xor2, mixed ST does not cut",
            ENC_XOR2,
            floats_mixed_st.clone(),
            vec![(ENC_XOR2, floats_mixed_st.clone())],
        ),
        d(
            "default, type flips cut",
            vec![
                Sample::float(0, 10, 1.0),
                histogram_sample(20, UK),
                Sample::float(0, 30, 3.0),
            ],
            vec![
                (ENC_XOR, vec![Sample::float(0, 10, 1.0)]),
                (ENC_HISTOGRAM, vec![histogram_sample(20, UK)]),
                (ENC_XOR, vec![Sample::float(0, 30, 3.0)]),
            ],
        ),
        e(
            "xor2, type flips cut",
            ENC_XOR2,
            vec![
                Sample::float(0, 10, 1.0),
                histogram_sample(20, UK),
                Sample::float(0, 30, 3.0),
            ],
            vec![
                (ENC_XOR2, vec![Sample::float(0, 10, 1.0)]),
                (ENC_HISTOGRAM, vec![histogram_sample(20, UK)]),
                (ENC_XOR2, vec![Sample::float(0, 30, 3.0)]),
            ],
        ),
        d(
            "default, histogram ST flips cut",
            vec![
                histogram_sample(10, UK),
                histogram_sample_with_st(15, 20),
                histogram_sample(30, UK),
            ],
            vec![
                (ENC_HISTOGRAM, vec![histogram_sample(10, UK)]),
                (ENC_HISTOGRAM_ST, vec![histogram_sample_with_st(15, 20)]),
                (ENC_HISTOGRAM, vec![histogram_sample(30, UK)]),
            ],
        ),
        e(
            "xor2, histogram ST flips cut",
            ENC_XOR2,
            vec![
                histogram_sample(10, UK),
                histogram_sample_with_st(15, 20),
                histogram_sample(30, UK),
            ],
            vec![
                (ENC_HISTOGRAM, vec![histogram_sample(10, UK)]),
                (ENC_HISTOGRAM_ST, vec![histogram_sample_with_st(15, 20)]),
                (ENC_HISTOGRAM, vec![histogram_sample(30, UK)]),
            ],
        ),
        e(
            "xor2, float histogram ST flips cut",
            ENC_XOR2,
            vec![
                float_histogram_sample(10, UK),
                float_histogram_sample_with_st(15, 20),
                float_histogram_sample(30, UK),
            ],
            vec![
                (ENC_FLOAT_HISTOGRAM, vec![float_histogram_sample(10, UK)]),
                (
                    ENC_FLOAT_HISTOGRAM_ST,
                    vec![float_histogram_sample_with_st(15, 20)],
                ),
                (ENC_FLOAT_HISTOGRAM, vec![float_histogram_sample(30, UK)]),
            ],
        ),
        d(
            "default, 150 samples split at 120",
            many_floats(150),
            vec![
                (ENC_XOR, many_floats(150)[..120].to_vec()),
                (ENC_XOR, many_floats(150)[120..].to_vec()),
            ],
        ),
        e(
            "xor2, 150 samples split at 120",
            ENC_XOR2,
            many_floats(150),
            vec![
                (ENC_XOR2, many_floats(150)[..120].to_vec()),
                (ENC_XOR2, many_floats(150)[120..].to_vec()),
            ],
        ),
        d(
            "default, 150 samples all ST split at 120",
            many_floats_with_st(150),
            vec![
                (ENC_XOR2, many_floats_with_st(150)[..120].to_vec()),
                (ENC_XOR2, many_floats_with_st(150)[120..].to_vec()),
            ],
        ),
        e(
            "xor2, 150 samples all ST split at 120",
            ENC_XOR2,
            many_floats_with_st(150),
            vec![
                (ENC_XOR2, many_floats_with_st(150)[..120].to_vec()),
                (ENC_XOR2, many_floats_with_st(150)[120..].to_vec()),
            ],
        ),
        e(
            "xor2, ST appearing on the last sample before the split does not cut",
            ENC_XOR2,
            xor2_st_last,
            vec![
                (ENC_XOR2, xor2_st_last_first_chunk),
                (
                    ENC_XOR2,
                    vec![Sample::float(15, SPLIT + 1, (SPLIT + 1) as f64)],
                ),
            ],
        ),
    ];

    for tc in cases {
        let name = tc.name;
        let series = new_list_series(lbs.clone(), tc.samples);
        let encoder = if tc.use_default_constructor {
            new_series_to_chunk_encoder(series)
        } else {
            new_series_to_chunk_encoder_with_float_encoding(series, tc.float_encoding)
        };

        let chks = expand_chunks(&mut *encoder.iterator(None)).expect("no error");
        assert_eq!(chks.len(), tc.expected.len(), "{}", name);

        for (i, (enc, exp_samples)) in tc.expected.iter().enumerate() {
            assert_eq!(
                *enc,
                chks[i].chunk().encoding(),
                "{}: chunk {} encoding",
                name,
                i
            );
            let act_samples =
                expand_samples(&mut chks[i].chunk().iterator(None)).expect("no error");
            assert_eq!(
                act_samples.len(),
                exp_samples.len(),
                "{}: chunk {} sample count",
                name,
                i
            );
            for (j, exp) in exp_samples.iter().enumerate() {
                let act = &act_samples[j];
                assert_eq!(
                    exp.typ(),
                    act.typ(),
                    "{}: chunk {} sample {} type",
                    name,
                    i,
                    j
                );
                assert_eq!(
                    exp.t(),
                    act.t(),
                    "{}: chunk {} sample {} timestamp",
                    name,
                    i,
                    j
                );
                assert_eq!(
                    exp.st(),
                    act.st(),
                    "{}: chunk {} sample {} start timestamp",
                    name,
                    i,
                    j
                );
                if exp.typ() == ValueType::Float {
                    assert_eq!(exp.f(), act.f(), "{}: chunk {} sample {} value", name, i, j);
                }
            }
        }
    }
}

fn get_counter_reset_hint(chunk: &Meta) -> CounterResetHeader {
    let c = chunk.chunk();
    if let Some(chk) = c.as_any().downcast_ref::<HistogramChunk>() {
        return chk.get_counter_reset_header();
    }
    if let Some(chk) = c.as_any().downcast_ref::<FloatHistogramChunk>() {
        return chk.get_counter_reset_header();
    }
    CounterResetHeader::UNKNOWN_COUNTER_RESET
}
