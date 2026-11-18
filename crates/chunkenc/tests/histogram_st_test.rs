//! Port of `tsdb/chunkenc/histogram_st_test.go`.

mod common;

use chunkenc::Iterator as _;
use chunkenc::st::MAX_FIRST_ST_CHANGE_ON;
use chunkenc::{Chunk, CounterResetHeader, ENC_HISTOGRAM_ST, HistogramStChunk, ValueType};
use common::{generate_test_histogram, generate_test_histograms};
use model::histogram::{CounterResetHint, Histogram, Span};

/// Go `value.StaleNaN`.
use model::value::STALE_NAN;

fn is_stale_nan(f: f64) -> bool {
    f.to_bits() == STALE_NAN
}

/// Go `histogramSTSample`.
struct HistogramStSample {
    st: i64,
    t: i64,
    h: Histogram,
}

fn s(st: i64, t: i64, h: &Histogram) -> HistogramStSample {
    HistogramStSample { st, t, h: h.clone() }
}

/// Go `requireHistogramSTSamples`: appends the given histogram samples to a new HistogramSTChunk, then verifies
/// all samples round-trip correctly through the iterator.
fn require_histogram_st_samples(samples: Vec<HistogramStSample>) {
    let mut c = HistogramStChunk::new();
    let mut app = c.appender().unwrap();

    for s in &samples {
        let mut h = s.h.clone();
        let (new, _) = app.append_histogram(&mut c, None, s.st, s.t, &mut h, false).unwrap();
        // adapted: Go keeps appending through the returned appender (`_, _, app, err = ...`) and then checks the
        // sample count of c; a new chunk here would make that check fail in Go, so it must not happen.
        assert!(new.is_none());
    }

    assert_eq!(samples.len(), c.num_samples());

    let mut it = c.iterator();
    for s in &samples {
        assert_eq!(ValueType::Histogram, it.next());
        assert_eq!(s.t, it.at_t());
        assert_eq!(s.st, it.at_st());
    }
    assert_eq!(ValueType::None, it.next());
    assert!(it.err().is_none());
}

#[test]
fn test_histogram_st_chunk_st() {
    common::test_chunk_st_handling(ValueType::Histogram, || Box::new(HistogramStChunk::new()) as Box<dyn Chunk>);
}

#[test]
fn test_histogram_st_basic() {
    let hs = generate_test_histograms(5);
    require_histogram_st_samples(vec![
        s(0, 1000, &hs[0]),
        s(0, 2000, &hs[1]),
        s(0, 3000, &hs[2]),
        s(0, 4000, &hs[3]),
        s(0, 5000, &hs[4]),
    ]);
}

#[test]
fn test_histogram_st_chunk_append_and_iterate() {
    let hs = generate_test_histograms(5);
    require_histogram_st_samples(vec![
        s(100, 1000, &hs[0]),
        s(100, 2000, &hs[1]),
        s(200, 3000, &hs[2]),
        s(200, 4000, &hs[3]),
        s(300, 5000, &hs[4]),
    ]);
}

#[test]
fn test_histogram_st_chunk_counter_reset() {
    let mut c = HistogramStChunk::new();
    let mut app = c.appender().unwrap();

    let mut h1 = Histogram {
        count: 10,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![Span::new(0, 2)],
        positive_buckets: vec![6, -3],
        ..Default::default()
    };

    let (new, _) = app.append_histogram(&mut c, None, 100, 1000, &mut h1, false).unwrap();
    assert!(new.is_none());

    let mut h2 = h1.copy();
    h2.count = 3;
    h2.sum = 5.0;
    h2.positive_buckets = vec![2, -1];
    h2.counter_reset_hint = CounterResetHint::CounterReset;

    let (new_chunk, recoded) = app.append_histogram(&mut c, None, 200, 2000, &mut h2, false).unwrap();
    let (mut st_chunk, mut new_app) = new_chunk.expect("new chunk");
    assert!(!recoded);

    // adapted: Go type-asserts newChunk.(*HistogramSTChunk); the Rust result is already that concrete type.
    assert_eq!(CounterResetHeader::COUNTER_RESET, st_chunk.get_counter_reset_header());
    assert_eq!(1, st_chunk.num_samples());

    // Verify ST is preserved in the new chunk.
    let mut it = st_chunk.iterator();
    assert_eq!(ValueType::Histogram, it.next());
    assert_eq!(2000, it.at_t());
    assert_eq!(200, it.at_st());
    assert_eq!(ValueType::None, it.next());
    assert!(it.err().is_none());

    let mut h3 = h2.copy();
    h3.counter_reset_hint = CounterResetHint::UnknownCounterReset;
    h3.count = 8;
    h3.sum = 10.0;
    h3.positive_buckets = vec![4, -2];
    new_app.append_histogram(&mut st_chunk, None, 300, 3000, &mut h3, false).unwrap();
    assert_eq!(2, st_chunk.num_samples());
}

#[test]
fn test_histogram_st_chunk_recode() {
    let mut c = HistogramStChunk::new();
    let mut app = c.appender().unwrap();

    let mut h1 = Histogram {
        count: 27,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![Span::new(0, 2), Span::new(2, 1), Span::new(3, 2), Span::new(3, 1), Span::new(1, 1)],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4],
        negative_spans: vec![Span::new(1, 1)],
        negative_buckets: vec![1],
        ..Default::default()
    };

    let (new, _) = app.append_histogram(&mut c, None, 100, 1000, &mut h1, false).unwrap();
    assert!(new.is_none());

    // Force recode with expanded bucket layout.
    let mut h2 = h1.copy();
    h2.positive_spans = vec![Span::new(0, 3), Span::new(1, 1), Span::new(1, 4), Span::new(3, 3)];
    h2.negative_spans = vec![Span::new(0, 2)];
    h2.count = 35;
    h2.zero_count += 1;
    h2.sum = 30.0;
    h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1];
    h2.negative_buckets = vec![2, -1];

    // Ensure recode produced a new chunk
    let (new_chunk, recoded) = app.append_histogram(&mut c, None, 200, 2000, &mut h2, false).unwrap();
    let (mut st_chunk, mut new_app) = new_chunk.expect("new chunk");
    assert!(recoded);

    assert_eq!(2, st_chunk.num_samples());

    let mut it = st_chunk.iterator();
    assert_eq!(ValueType::Histogram, it.next());
    assert_eq!(1000, it.at_t());
    assert_eq!(100, it.at_st());

    assert_eq!(ValueType::Histogram, it.next());
    assert_eq!(2000, it.at_t());
    assert_eq!(200, it.at_st());

    assert_eq!(ValueType::None, it.next());
    assert!(it.err().is_none());

    let mut h3 = h2.copy();
    h3.count = 40;
    h3.sum = 35.0;
    for b in h3.positive_buckets.iter_mut() {
        *b += 1;
    }
    new_app.append_histogram(&mut st_chunk, None, 300, 3000, &mut h3, false).unwrap();
    assert_eq!(3, st_chunk.num_samples());
}

#[test]
fn test_histogram_st_more_than127_samples() {
    let mut c = HistogramStChunk::new();
    let mut app = c.appender().unwrap();

    let h = Histogram {
        count: 5,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![Span::new(0, 2)],
        positive_buckets: vec![6, -3],
        ..Default::default()
    };

    let num_samples = MAX_FIRST_ST_CHANGE_ON as i64 + 3; // 130

    for i in 0..num_samples {
        let mut hi = h.copy();
        hi.count = (5 + i) as u64;
        hi.sum = (18 + i) as f64;
        let (new, _) = app.append_histogram(&mut c, None, 500, 1000 + i * 1000, &mut hi, false).unwrap();
        // adapted: Go reassigns app from the result; no new chunk means the same appender continues.
        assert!(new.is_none());
    }
    assert_eq!(num_samples as usize, c.num_samples());

    let mut it = c.iterator();
    for i in 0..num_samples {
        assert_eq!(ValueType::Histogram, it.next());
        assert_eq!(1000 + i * 1000, it.at_t());
        assert_eq!(500, it.at_st());
    }
    assert_eq!(ValueType::None, it.next());
    assert!(it.err().is_none());

    // Test ST changing after the boundary.
    let mut c2 = HistogramStChunk::new();
    let mut app2 = c2.appender().unwrap();

    for i in 0..(MAX_FIRST_ST_CHANGE_ON as i64 + 1) {
        let mut hi = h.copy();
        hi.count = (5 + i) as u64;
        hi.sum = (18 + i) as f64;
        let (new, _) = app2.append_histogram(&mut c2, None, 0, 1000 + i * 1000, &mut hi, false).unwrap();
        // adapted: Go reassigns app2 from the result; no new chunk means the same appender continues.
        assert!(new.is_none());
    }

    for i in 0..3i64 {
        let mut hi = h.copy();
        hi.count = (200 + i) as u64;
        hi.sum = (200 + i) as f64;
        let (new, _) = app2.append_histogram(&mut c2, None, 100, 200000 + i * 1000, &mut hi, false).unwrap();
        // adapted: Go reassigns app2 from the result; no new chunk means the same appender continues.
        assert!(new.is_none());
    }

    let mut it2 = c2.iterator();
    for i in 0..(MAX_FIRST_ST_CHANGE_ON as i64 + 1) {
        assert_eq!(ValueType::Histogram, it2.next());
        assert_eq!(1000 + i * 1000, it2.at_t());
        assert_eq!(0, it2.at_st());
    }
    for i in 0..3i64 {
        assert_eq!(ValueType::Histogram, it2.next());
        assert_eq!(200000 + i * 1000, it2.at_t());
        assert_eq!(100, it2.at_st());
    }
    assert_eq!(ValueType::None, it2.next());
    assert!(it2.err().is_none());
}

#[test]
fn test_histogram_st_chunk_mixed_st() {
    let hs = generate_test_histograms(5);
    require_histogram_st_samples(vec![
        s(0, 1000, &hs[0]),
        s(0, 2000, &hs[1]),
        s(100, 3000, &hs[2]),
        s(0, 4000, &hs[3]),
        s(200, 5000, &hs[4]),
    ]);
}

#[test]
fn test_histogram_st_chunk_seek() {
    let hs = generate_test_histograms(5);
    let mut c = HistogramStChunk::new();
    let mut app = c.appender().unwrap();

    let samples = vec![
        s(0, 1000, &hs[0]),
        s(0, 2000, &hs[1]),
        s(100, 3000, &hs[2]),
        s(100, 4000, &hs[3]),
        s(200, 5000, &hs[4]),
    ];
    for smp in &samples {
        let mut h = smp.h.clone();
        let (new, _) = app.append_histogram(&mut c, None, smp.st, smp.t, &mut h, false).unwrap();
        // adapted: Go reassigns app from the result; no new chunk means the same appender continues.
        assert!(new.is_none());
    }

    let mut it = c.iterator();
    assert_eq!(ValueType::Histogram, it.seek(2000));
    assert_eq!(2000, it.at_t());
    assert_eq!(0, it.at_st());

    assert_eq!(ValueType::Histogram, it.seek(3000));
    assert_eq!(3000, it.at_t());
    assert_eq!(100, it.at_st());

    assert_eq!(ValueType::Histogram, it.seek(3000));
    assert_eq!(3000, it.at_t());
    assert_eq!(100, it.at_st());

    assert_eq!(ValueType::Histogram, it.seek(4000));
    assert_eq!(4000, it.at_t());
    assert_eq!(100, it.at_st());

    assert_eq!(ValueType::Histogram, it.seek(5000));
    assert_eq!(5000, it.at_t());
    assert_eq!(200, it.at_st());

    assert_eq!(ValueType::None, it.seek(5001));
    assert!(it.err().is_none());
}

#[test]
fn test_histogram_st_chunk_stale_samples() {
    let mut c = HistogramStChunk::new();
    let mut app = c.appender().unwrap();

    let mut h1 = generate_test_histogram(0);
    let mut stale1 = Histogram { sum: f64::from_bits(STALE_NAN), ..Default::default() };
    let mut stale2 = Histogram { sum: f64::from_bits(STALE_NAN), ..Default::default() };

    // adapted: Go reassigns app from each result; no new chunk means the same appender continues.
    let (new, _) = app.append_histogram(&mut c, None, 100, 1000, &mut h1, false).unwrap();
    assert!(new.is_none());
    let (new, _) = app.append_histogram(&mut c, None, 200, 2000, &mut stale1, false).unwrap();
    assert!(new.is_none());
    app.append_histogram(&mut c, None, 0, 3000, &mut stale2, false).unwrap();

    let mut it = c.iterator();

    assert_eq!(ValueType::Histogram, it.next());
    assert_eq!(1000, it.at_t());
    assert_eq!(100, it.at_st());
    let (_, h) = it.at_histogram(None);
    assert!(!is_stale_nan(h.sum));

    assert_eq!(ValueType::Histogram, it.next());
    assert_eq!(2000, it.at_t());
    assert_eq!(200, it.at_st());
    let (_, h) = it.at_histogram(None);
    assert!(is_stale_nan(h.sum));

    assert_eq!(ValueType::Histogram, it.next());
    assert_eq!(3000, it.at_t());
    assert_eq!(0, it.at_st());
    let (_, h) = it.at_histogram(None);
    assert!(is_stale_nan(h.sum));

    assert_eq!(ValueType::None, it.next());
    assert!(it.err().is_none());
}

#[test]
fn test_histogram_st_append_only_errors_preserve_chunk() {
    // adapted: in Go the erroring AppendHistogram also returns a nil chunk and recoded false; the Rust call
    // returns only the error.
    {
        // "schema change".
        let mut c = HistogramStChunk::new();
        let mut app = c.appender().unwrap();

        let mut h = generate_test_histogram(0);
        let (new, recoded) = app.append_histogram(&mut c, None, 100, 1000, &mut h, true).unwrap();
        assert!(new.is_none());
        assert!(!recoded);

        let before = c.bytes().to_vec();
        let before_samples = c.num_samples();

        let mut h2 = h.copy();
        h2.schema += 1;
        let err = app.append_histogram(&mut c, None, 200, 2000, &mut h2, true).unwrap_err();
        assert_eq!("histogram schema change", err.to_string());
        assert_eq!(before_samples, c.num_samples());
        assert_eq!(before, c.bytes());
    }

    {
        // "counter reset".
        let mut c = HistogramStChunk::new();
        let mut app = c.appender().unwrap();

        let mut h = generate_test_histogram(0);
        let (new, recoded) = app.append_histogram(&mut c, None, 100, 1000, &mut h, true).unwrap();
        assert!(new.is_none());
        assert!(!recoded);

        let before = c.bytes().to_vec();
        let before_samples = c.num_samples();

        let mut h2 = h.copy();
        h2.counter_reset_hint = CounterResetHint::CounterReset;
        let err = app.append_histogram(&mut c, None, 200, 2000, &mut h2, true).unwrap_err();
        assert_eq!("histogram counter reset", err.to_string());
        assert_eq!(before_samples, c.num_samples());
        assert_eq!(before, c.bytes());
    }
}

#[test]
fn test_histogram_st_chunk_gauge() {
    let mut c = HistogramStChunk::new();
    let mut app = c.appender().unwrap();

    let mut h1 = Histogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        count: 5,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![Span::new(0, 2), Span::new(2, 1), Span::new(3, 2), Span::new(3, 1), Span::new(1, 1)],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4],
        ..Default::default()
    };

    let (new, _) = app.append_histogram(&mut c, None, 100, 1000, &mut h1, false).unwrap();
    assert!(new.is_none());
    assert_eq!(CounterResetHeader::GAUGE_TYPE, c.get_counter_reset_header());

    let mut h2 = h1.copy();
    h2.count += 9;
    h2.zero_count += 1;
    h2.sum = 30.0;
    h2.positive_spans = vec![Span::new(0, 3), Span::new(1, 1), Span::new(1, 4), Span::new(3, 3)];
    h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1];

    let (new_chunk, recoded) = app.append_histogram(&mut c, None, 200, 2000, &mut h2, false).unwrap();
    assert!(recoded);

    let (mut st_chunk, mut new_app) = new_chunk.expect("new chunk");
    assert_eq!(CounterResetHeader::GAUGE_TYPE, st_chunk.get_counter_reset_header());

    let mut it = st_chunk.iterator();
    assert_eq!(ValueType::Histogram, it.next());
    assert_eq!(1000, it.at_t());
    assert_eq!(100, it.at_st());

    assert_eq!(ValueType::Histogram, it.next());
    assert_eq!(2000, it.at_t());
    assert_eq!(200, it.at_st());
    assert_eq!(ValueType::None, it.next());
    assert!(it.err().is_none());

    let mut h3 = h2.copy();
    h3.sum = 35.0;
    for b in h3.positive_buckets.iter_mut() {
        *b += 1;
    }
    new_app.append_histogram(&mut st_chunk, None, 300, 3000, &mut h3, false).unwrap();
    assert_eq!(3, st_chunk.num_samples());
}

#[test]
fn test_histogram_st_chunk_counter_reset_header() {
    let mut c = HistogramStChunk::new();
    assert_eq!(3, c.bytes().len());

    let mut happ = c.appender().unwrap();

    let mut h = generate_test_histograms(1).remove(0);
    happ.append_histogram(&mut c, None, 100, 1000, &mut h, false).unwrap();

    let byte2_before = c.bytes()[2];
    for cr in [
        CounterResetHeader::UNKNOWN_COUNTER_RESET,
        CounterResetHeader::COUNTER_RESET,
        CounterResetHeader::NOT_COUNTER_RESET,
        CounterResetHeader::GAUGE_TYPE,
    ] {
        happ.set_counter_reset_header(&mut c, cr);
        assert_eq!(cr, c.get_counter_reset_header());
        assert_eq!(1, c.num_samples());
        assert_eq!(1, happ.num_samples());
        assert_eq!(byte2_before, c.bytes()[2]);
    }
}

#[test]
fn test_histogram_st_chunk_over_flow_panics() {
    common::test_chunk_over_flow_panics(ENC_HISTOGRAM_ST, ValueType::Histogram);
}
