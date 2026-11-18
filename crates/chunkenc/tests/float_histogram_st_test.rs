//! Port of `tsdb/chunkenc/float_histogram_st_test.go`. The Go benchmarks are not ported.

mod common;
use common::{generate_test_float_histogram, generate_test_float_histograms};

use chunkenc::ChunkIterator as _;
use chunkenc::st::MAX_FIRST_ST_CHANGE_ON;
use chunkenc::{
    Chunk, CounterResetHeader, ENC_FLOAT_HISTOGRAM_ST, Error, FloatHistogramStAppender, FloatHistogramStChunk,
    ValueType,
};
use model::histogram::{CounterResetHint, FloatHistogram, Histogram, Span};

use model::value::STALE_NAN;

fn is_stale_nan(f: f64) -> bool {
    f.to_bits() == STALE_NAN
}

fn sp(offset: i32, length: u32) -> Span {
    Span::new(offset, length)
}

/// Go `floatHistogramSTSample`.
struct FloatHistogramStSample {
    st: i64,
    t: i64,
    fh: FloatHistogram,
}

fn sample(st: i64, t: i64, fh: &FloatHistogram) -> FloatHistogramStSample {
    FloatHistogramStSample { st, t, fh: fh.clone() }
}

/// Go `_, _, app, err = app.AppendFloatHistogram(...)`: appends to the chunk the appender writes to (`c`, or the
/// last new chunk returned) and continues with the returned appender, as Go does when it reassigns `app`.
fn go_append(
    c: &mut FloatHistogramStChunk,
    cur: &mut Option<FloatHistogramStChunk>,
    app: &mut FloatHistogramStAppender,
    st: i64,
    t: i64,
    h: &mut FloatHistogram,
    append_only: bool,
) -> Result<bool, Error> {
    let target = match cur.as_mut() {
        Some(n) => n,
        None => c,
    };
    let (new, recoded) = app.append_float_histogram(target, None, st, t, h, append_only)?;
    if let Some((nc, na)) = new {
        *cur = Some(nc);
        *app = na;
    }
    Ok(recoded)
}

/// Go `requireFloatHistogramSTSamples`: appends the given float histogram samples to a new FloatHistogramSTChunk,
/// then verifies all samples round-trip correctly through the iterator.
fn require_float_histogram_st_samples(samples: Vec<FloatHistogramStSample>) {
    let mut c = FloatHistogramStChunk::new();
    let mut app = c.appender().unwrap();
    let mut cur = None;

    for mut s in samples.iter().map(|s| FloatHistogramStSample { st: s.st, t: s.t, fh: s.fh.clone() }) {
        go_append(&mut c, &mut cur, &mut app, s.st, s.t, &mut s.fh, false).unwrap();
    }

    assert_eq!(samples.len(), c.num_samples());

    let mut it = c.iterator();
    for (i, s) in samples.iter().enumerate() {
        assert_eq!(ValueType::FloatHistogram, it.next(), "sample {i}");
        assert_eq!(s.t, it.at_t(), "sample {i}: timestamp");
        assert_eq!(s.st, it.at_st(), "sample {i}: start timestamp");
    }
    assert_eq!(ValueType::None, it.next());
    assert!(it.err().is_none(), "{:?}", it.err());
}

#[test]
fn test_float_histogram_st_chunk_st() {
    common::test_chunk_st_handling(ValueType::FloatHistogram, || Box::new(FloatHistogramStChunk::new()));
}

#[test]
fn test_float_histogram_st_basic() {
    let hs = generate_test_float_histograms(5);
    require_float_histogram_st_samples(vec![
        sample(0, 1000, &hs[0]),
        sample(0, 2000, &hs[1]),
        sample(0, 3000, &hs[2]),
        sample(0, 4000, &hs[3]),
        sample(0, 5000, &hs[4]),
    ]);
}

#[test]
fn test_float_histogram_st_chunk_append_and_iterate() {
    let fhs = generate_test_float_histograms(5);
    require_float_histogram_st_samples(vec![
        sample(100, 1000, &fhs[0]),
        sample(100, 2000, &fhs[1]),
        sample(200, 3000, &fhs[2]),
        sample(200, 4000, &fhs[3]),
        sample(300, 5000, &fhs[4]),
    ]);
}

#[test]
fn test_float_histogram_st_chunk_counter_reset() {
    let mut c = FloatHistogramStChunk::new();
    let mut app = c.appender().unwrap();

    let mut fh1 = FloatHistogram {
        count: 10.0,
        zero_count: 2.0,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![sp(0, 2)],
        positive_buckets: vec![6.0, 3.0],
        ..Default::default()
    };

    let (new, _) = app.append_float_histogram(&mut c, None, 100, 1000, &mut fh1, false).unwrap();
    // Go reassigns `app`; no new chunk was cut, so the same appender continues.
    assert!(new.is_none());

    let mut fh2 = fh1.copy();
    fh2.count = 3.0;
    fh2.sum = 5.0;
    fh2.positive_buckets = vec![2.0, 1.0];
    fh2.counter_reset_hint = CounterResetHint::CounterReset;

    // Ensure counter reset produces new chunk.
    let (new_chunk, recoded) = app.append_float_histogram(&mut c, None, 200, 2000, &mut fh2, false).unwrap();
    let (mut st_chunk, mut new_app) = new_chunk.expect("new chunk");
    assert!(!recoded);

    // adapted: Go type-asserts the new chunk to *FloatHistogramSTChunk; the Rust type is static.
    assert_eq!(ENC_FLOAT_HISTOGRAM_ST, Chunk::encoding(&st_chunk));
    assert_eq!(CounterResetHeader::COUNTER_RESET, st_chunk.get_counter_reset_header());
    assert_eq!(1, st_chunk.num_samples());

    // Verify ST is preserved in the new chunk.
    {
        let mut it = st_chunk.iterator();
        assert_eq!(ValueType::FloatHistogram, it.next());
        assert_eq!(2000, it.at_t());
        assert_eq!(200, it.at_st());
        assert_eq!(ValueType::None, it.next());
        assert!(it.err().is_none(), "{:?}", it.err());
    }

    let mut fh3 = fh2.copy();
    fh3.counter_reset_hint = CounterResetHint::UnknownCounterReset;
    fh3.count = 8.0;
    fh3.sum = 10.0;
    fh3.positive_buckets = vec![4.0, 2.0];
    new_app.append_float_histogram(&mut st_chunk, None, 300, 3000, &mut fh3, false).unwrap();
    assert_eq!(2, st_chunk.num_samples());
}

#[test]
fn test_float_histogram_st_chunk_recode() {
    let mut c = FloatHistogramStChunk::new();
    let mut app = c.appender().unwrap();

    let h1 = Histogram {
        count: 27,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![sp(0, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4],
        negative_spans: vec![sp(1, 1)],
        negative_buckets: vec![1],
        ..Default::default()
    };
    let mut fh1 = h1.to_float();

    let (new, _) = app.append_float_histogram(&mut c, None, 100, 1000, &mut fh1, false).unwrap();
    // Go reassigns `app`; no new chunk was cut, so the same appender continues.
    assert!(new.is_none());

    // Force recode with expanded bucket layout
    let mut h2 = h1.copy();
    h2.positive_spans = vec![sp(0, 3), sp(1, 1), sp(1, 4), sp(3, 3)];
    h2.negative_spans = vec![sp(0, 2)];
    h2.count = 35;
    h2.zero_count += 1;
    h2.sum = 30.0;
    h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1];
    h2.negative_buckets = vec![2, -1];
    let mut fh2 = h2.to_float();

    // Ensure recode produces a new chunk.
    let (new_chunk, recoded) = app.append_float_histogram(&mut c, None, 200, 2000, &mut fh2, false).unwrap();
    let (mut st_chunk, mut new_app) = new_chunk.expect("new chunk");
    assert!(recoded);

    // adapted: Go type-asserts the new chunk to *FloatHistogramSTChunk; the Rust type is static.
    assert_eq!(ENC_FLOAT_HISTOGRAM_ST, Chunk::encoding(&st_chunk));
    assert_eq!(2, st_chunk.num_samples());

    {
        let mut it = st_chunk.iterator();
        assert_eq!(ValueType::FloatHistogram, it.next());
        assert_eq!(1000, it.at_t());
        assert_eq!(100, it.at_st());

        assert_eq!(ValueType::FloatHistogram, it.next());
        assert_eq!(2000, it.at_t());
        assert_eq!(200, it.at_st());

        assert_eq!(ValueType::None, it.next());
        assert!(it.err().is_none(), "{:?}", it.err());
    }

    let mut fh3 = fh2.copy();
    fh3.count = 40.0;
    fh3.sum = 35.0;
    for b in fh3.positive_buckets.iter_mut() {
        *b += 1.0;
    }
    new_app.append_float_histogram(&mut st_chunk, None, 300, 3000, &mut fh3, false).unwrap();
    assert_eq!(3, st_chunk.num_samples());
}

#[test]
fn test_float_histogram_st_more_than127_samples() {
    let mut c = FloatHistogramStChunk::new();
    let mut app = c.appender().unwrap();
    let mut cur = None;

    let fh = FloatHistogram {
        count: 5.0,
        zero_count: 2.0,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![sp(0, 2)],
        positive_buckets: vec![6.0, 3.0],
        ..Default::default()
    };

    let num_samples = MAX_FIRST_ST_CHANGE_ON as usize + 3; // 130

    for i in 0..num_samples {
        let mut fhi = fh.copy();
        fhi.count = (5 + i) as f64;
        fhi.sum = (18 + i) as f64;
        go_append(&mut c, &mut cur, &mut app, 500, (1000 + i * 1000) as i64, &mut fhi, false).unwrap();
    }
    assert_eq!(num_samples, c.num_samples());

    // Verify all samples round-trip correctly.
    let mut it = c.iterator();
    for i in 0..num_samples {
        assert_eq!(ValueType::FloatHistogram, it.next());
        assert_eq!((1000 + i * 1000) as i64, it.at_t());
        assert_eq!(500, it.at_st());
    }
    assert_eq!(ValueType::None, it.next());
    assert!(it.err().is_none(), "{:?}", it.err());

    let mut c2 = FloatHistogramStChunk::new();
    let mut app2 = c2.appender().unwrap();
    let mut cur2 = None;

    for i in 0..MAX_FIRST_ST_CHANGE_ON as usize + 1 {
        let mut fhi = fh.copy();
        fhi.count = (5 + i) as f64;
        fhi.sum = (18 + i) as f64;
        go_append(&mut c2, &mut cur2, &mut app2, 0, (1000 + i * 1000) as i64, &mut fhi, false).unwrap();
    }

    for i in 0..3usize {
        let mut fhi = fh.copy();
        fhi.count = (200 + i) as f64;
        fhi.sum = (200 + i) as f64;
        go_append(&mut c2, &mut cur2, &mut app2, 100, (200000 + i * 1000) as i64, &mut fhi, false).unwrap();
    }

    let mut it2 = c2.iterator();
    for i in 0..MAX_FIRST_ST_CHANGE_ON as usize + 1 {
        assert_eq!(ValueType::FloatHistogram, it2.next());
        assert_eq!((1000 + i * 1000) as i64, it2.at_t());
        assert_eq!(0, it2.at_st());
    }
    for i in 0..3usize {
        assert_eq!(ValueType::FloatHistogram, it2.next());
        assert_eq!((200000 + i * 1000) as i64, it2.at_t());
        assert_eq!(100, it2.at_st());
    }
    assert_eq!(ValueType::None, it2.next());
    assert!(it2.err().is_none(), "{:?}", it2.err());
}

#[test]
fn test_float_histogram_st_chunk_mixed_st() {
    let fhs = generate_test_float_histograms(5);
    require_float_histogram_st_samples(vec![
        sample(0, 1000, &fhs[0]),
        sample(0, 2000, &fhs[1]),
        sample(100, 3000, &fhs[2]),
        sample(0, 4000, &fhs[3]),
        sample(200, 5000, &fhs[4]),
    ]);
}

#[test]
fn test_float_histogram_st_chunk_seek() {
    let fhs = generate_test_float_histograms(5);
    let mut c = FloatHistogramStChunk::new();
    let mut app = c.appender().unwrap();
    let mut cur = None;

    let mut samples = vec![
        sample(0, 1000, &fhs[0]),
        sample(0, 2000, &fhs[1]),
        sample(100, 3000, &fhs[2]),
        sample(100, 4000, &fhs[3]),
        sample(200, 5000, &fhs[4]),
    ];
    for s in samples.iter_mut() {
        go_append(&mut c, &mut cur, &mut app, s.st, s.t, &mut s.fh, false).unwrap();
    }

    let mut it = c.iterator();
    assert_eq!(ValueType::FloatHistogram, it.seek(2000));
    assert_eq!(2000, it.at_t());
    assert_eq!(0, it.at_st());

    assert_eq!(ValueType::FloatHistogram, it.seek(3000));
    assert_eq!(3000, it.at_t());
    assert_eq!(100, it.at_st());

    assert_eq!(ValueType::FloatHistogram, it.seek(3000));
    assert_eq!(3000, it.at_t());
    assert_eq!(100, it.at_st());

    assert_eq!(ValueType::FloatHistogram, it.seek(4000));
    assert_eq!(4000, it.at_t());
    assert_eq!(100, it.at_st());

    assert_eq!(ValueType::FloatHistogram, it.seek(5000));
    assert_eq!(5000, it.at_t());
    assert_eq!(200, it.at_st());

    assert_eq!(ValueType::None, it.seek(5001));
    assert!(it.err().is_none(), "{:?}", it.err());
}

#[test]
fn test_float_histogram_st_chunk_stale_samples() {
    let mut c = FloatHistogramStChunk::new();
    let mut app = c.appender().unwrap();
    let mut cur = None;

    let mut fh1 = generate_test_float_histogram(0);
    let mut stale1 = FloatHistogram { sum: f64::from_bits(STALE_NAN), ..Default::default() };
    let mut stale2 = FloatHistogram { sum: f64::from_bits(STALE_NAN), ..Default::default() };

    go_append(&mut c, &mut cur, &mut app, 100, 1000, &mut fh1, false).unwrap();
    go_append(&mut c, &mut cur, &mut app, 200, 2000, &mut stale1, false).unwrap();
    go_append(&mut c, &mut cur, &mut app, 0, 3000, &mut stale2, false).unwrap();

    let mut it = c.iterator();

    assert_eq!(ValueType::FloatHistogram, it.next());
    assert_eq!(1000, it.at_t());
    assert_eq!(100, it.at_st());
    let (_, fh) = it.at_float_histogram(None);
    assert!(!is_stale_nan(fh.sum));

    assert_eq!(ValueType::FloatHistogram, it.next());
    assert_eq!(2000, it.at_t());
    assert_eq!(200, it.at_st());
    let (_, fh) = it.at_float_histogram(None);
    assert!(is_stale_nan(fh.sum));

    assert_eq!(ValueType::FloatHistogram, it.next());
    assert_eq!(3000, it.at_t());
    assert_eq!(0, it.at_st());
    let (_, fh) = it.at_float_histogram(None);
    assert!(is_stale_nan(fh.sum));

    assert_eq!(ValueType::None, it.next());
    assert!(it.err().is_none(), "{:?}", it.err());
}

#[test]
fn test_float_histogram_st_append_only_errors_preserve_chunk() {
    // adapted: on error Go also returns a nil chunk and recoded=false; the Rust Err carries neither, so those
    // checks hold by the type.
    {
        // "schema change"
        let mut c = FloatHistogramStChunk::new();
        let mut app = c.appender().unwrap();

        let mut h = generate_test_float_histogram(0);
        let (new, recoded) = app.append_float_histogram(&mut c, None, 100, 1000, &mut h, true).unwrap();
        assert!(!recoded);
        // Go continues with the returned appender; no new chunk was cut, so it is `app`.
        assert!(new.is_none());
        let next_app = &mut app;

        let before = c.bytes().to_vec();
        let before_samples = c.num_samples();

        let mut h2 = h.copy();
        h2.schema += 1;
        let err = next_app.append_float_histogram(&mut c, None, 200, 2000, &mut h2, true).unwrap_err();
        assert_eq!("float histogram schema change", err.to_string());
        assert_eq!(before_samples, c.num_samples());
        assert_eq!(before, c.bytes());
    }

    {
        // "counter reset"
        let mut c = FloatHistogramStChunk::new();
        let mut app = c.appender().unwrap();

        let mut h = generate_test_float_histogram(0);
        let (new, recoded) = app.append_float_histogram(&mut c, None, 100, 1000, &mut h, true).unwrap();
        assert!(!recoded);
        // Go continues with the returned appender; no new chunk was cut, so it is `app`.
        assert!(new.is_none());
        let next_app = &mut app;

        let before = c.bytes().to_vec();
        let before_samples = c.num_samples();

        let mut h2 = h.copy();
        h2.counter_reset_hint = CounterResetHint::CounterReset;
        let err = next_app.append_float_histogram(&mut c, None, 200, 2000, &mut h2, true).unwrap_err();
        assert_eq!("float histogram counter reset", err.to_string());
        assert_eq!(before_samples, c.num_samples());
        assert_eq!(before, c.bytes());
    }
}

#[test]
fn test_float_histogram_st_chunk_gauge_recode_preserves_st() {
    let mut c = FloatHistogramStChunk::new();
    let mut app = c.appender().unwrap();

    let mut fh1 = FloatHistogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        count: 5.0,
        zero_count: 2.0,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![sp(0, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)],
        positive_buckets: vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 1.0],
        ..Default::default()
    };

    let (new, _) = app.append_float_histogram(&mut c, None, 100, 1000, &mut fh1, false).unwrap();
    // Go reassigns `app`; no new chunk was cut, so the same appender continues.
    assert!(new.is_none());
    assert_eq!(CounterResetHeader::GAUGE_TYPE, c.get_counter_reset_header());

    let mut fh2 = fh1.copy();
    fh2.count += 9.0;
    fh2.zero_count += 1.0;
    fh2.sum = 30.0;
    fh2.positive_spans = vec![sp(0, 3), sp(1, 1), sp(1, 4), sp(3, 3)];
    fh2.positive_buckets = vec![7.0, 5.0, 1.0, 3.0, 1.0, 0.0, 2.0, 5.0, 5.0, 0.0, 1.0];

    let (new_chunk, recoded) = app.append_float_histogram(&mut c, None, 200, 2000, &mut fh2, false).unwrap();
    assert!(recoded);

    let (mut st_chunk, mut new_app) = new_chunk.expect("new chunk");
    // adapted: Go type-asserts the new chunk to *FloatHistogramSTChunk; the Rust type is static.
    assert_eq!(ENC_FLOAT_HISTOGRAM_ST, Chunk::encoding(&st_chunk));
    assert_eq!(CounterResetHeader::GAUGE_TYPE, st_chunk.get_counter_reset_header());

    {
        let mut it = st_chunk.iterator();
        assert_eq!(ValueType::FloatHistogram, it.next());
        assert_eq!(1000, it.at_t());
        assert_eq!(100, it.at_st());

        assert_eq!(ValueType::FloatHistogram, it.next());
        assert_eq!(2000, it.at_t());
        assert_eq!(200, it.at_st());
        assert_eq!(ValueType::None, it.next());
        assert!(it.err().is_none(), "{:?}", it.err());
    }

    let mut fh3 = fh2.copy();
    fh3.sum = 35.0;
    for b in fh3.positive_buckets.iter_mut() {
        *b += 1.0;
    }
    new_app.append_float_histogram(&mut st_chunk, None, 300, 3000, &mut fh3, false).unwrap();
    assert_eq!(3, st_chunk.num_samples());
}

#[test]
fn test_float_histogram_st_chunk_counter_reset_header() {
    let mut c = FloatHistogramStChunk::new();
    assert_eq!(3, c.bytes().len());

    // adapted: Go type-asserts the appender to *FloatHistogramSTAppender; the Rust type is static.
    let mut happ = c.appender().unwrap();

    let mut fh = generate_test_float_histograms(1).remove(0);
    happ.append_float_histogram(&mut c, None, 100, 1000, &mut fh, false).unwrap();

    let byte2_before = c.bytes()[2];
    for cr in [
        CounterResetHeader::UNKNOWN_COUNTER_RESET,
        CounterResetHeader::COUNTER_RESET,
        CounterResetHeader::NOT_COUNTER_RESET,
        CounterResetHeader::GAUGE_TYPE,
    ] {
        happ.set_counter_reset_header(&mut c, cr);
        assert_eq!(cr, c.get_counter_reset_header());
        assert_eq!(1, c.num_samples(), "NumSamples must not include CR bits");
        assert_eq!(1, happ.num_samples(), "NumSamples must not include CR bits");
        assert_eq!(byte2_before, c.bytes()[2], "setting CR must not disturb byte 2 (ST header)");
    }
}

#[test]
fn test_float_histogram_st_chunk_over_flow_panics() {
    common::test_chunk_over_flow_panics(ENC_FLOAT_HISTOGRAM_ST, ValueType::FloatHistogram);
}
