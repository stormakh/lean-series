//! Port of `tsdb/chunkenc/histogram_test.go`.

mod common;

use chunkenc::ChunkIterator as _;
use chunkenc::{
    Chunk, CounterResetHeader, ENC_HISTOGRAM, HistogramAppendable, HistogramAppender, HistogramChunk, ValueType,
};
use common::{assert_sample_count, generate_test_custom_buckets_histogram, generate_test_histogram};
use model::histogram::{
    CUSTOM_BUCKETS_SCHEMA, CounterResetHint, EXPONENTIAL_SCHEMA_MAX, ErrorKind, FloatHistogram, Histogram, Span,
    custom_values,
};

/// Go `result`.
#[derive(Clone, Debug, PartialEq)]
struct TestResult {
    t: i64,
    h: Histogram,
    fh: FloatHistogram,
}

fn sp(offset: i32, length: u32) -> Span {
    Span::new(offset, length)
}

#[test]
fn test_first_histogram_explicit_counter_reset() {
    struct Case {
        name: &'static str,
        hint: CounterResetHint,
        exp_header: CounterResetHeader,
        exp_hint: CounterResetHint,
    }
    let tests = [
        Case {
            name: "CounterReset",
            hint: CounterResetHint::CounterReset,
            exp_header: CounterResetHeader::COUNTER_RESET,
            exp_hint: CounterResetHint::UnknownCounterReset,
        },
        Case {
            name: "NotCounterReset",
            hint: CounterResetHint::NotCounterReset,
            exp_header: CounterResetHeader::UNKNOWN_COUNTER_RESET,
            exp_hint: CounterResetHint::UnknownCounterReset,
        },
        Case {
            name: "UnknownCounterReset",
            hint: CounterResetHint::UnknownCounterReset,
            exp_header: CounterResetHeader::UNKNOWN_COUNTER_RESET,
            exp_hint: CounterResetHint::UnknownCounterReset,
        },
        Case {
            name: "Gauge",
            hint: CounterResetHint::GaugeType,
            exp_header: CounterResetHeader::GAUGE_TYPE,
            exp_hint: CounterResetHint::GaugeType,
        },
    ];
    for test in tests {
        let mut h = Histogram { counter_reset_hint: test.hint, ..Default::default() };
        let mut chk = HistogramChunk::new();
        let mut app = chk.appender().unwrap();
        let (new_chk, recoded) = app.append_histogram(&mut chk, None, 0, 0, &mut h, false).unwrap();
        // adapted: Go returns a nil chunk and the same appender (require.Equal(app, newApp)); in Rust "no new
        // chunk" means the same appender continues.
        assert!(new_chk.is_none(), "{}", test.name);
        assert!(!recoded, "{}", test.name);
        assert_eq!(test.exp_header, chk.get_counter_reset_header(), "{}", test.name);
        assert_first_int_histogram_sample_hint(&chk, test.exp_hint);
    }
}

#[test]
fn test_histogram_chunk_same_buckets() {
    let mut c = HistogramChunk::new();
    let mut exp: Vec<TestResult> = Vec::new();

    // Create fresh appender and add the first histogram.
    let mut app = c.appender().unwrap();
    assert_eq!(0, c.num_samples());

    let mut ts = 1234567890i64;
    let mut h = Histogram {
        count: 15,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-100,
        schema: 1,
        positive_spans: vec![sp(0, 2), sp(1, 2)],
        positive_buckets: vec![1, 1, -1, 0], // counts: 1, 2, 1, 1 (total 5)
        negative_spans: vec![sp(1, 1), sp(2, 3)],
        negative_buckets: vec![2, 1, -1, -1], // counts: 2, 3, 2, 1 (total 8)
        ..Default::default()
    };
    let (chk, _) = app.append_histogram(&mut c, None, 0, ts, &mut h, false).unwrap();
    assert!(chk.is_none());
    exp.push(TestResult { t: ts, h: h.clone(), fh: h.to_float() });
    assert_eq!(1, c.num_samples());

    // Add an updated histogram.
    ts += 16;
    h = h.copy();
    h.count = 32;
    h.zero_count += 1;
    h.sum = 24.4;
    h.positive_buckets = vec![5, -2, 1, -2]; // counts: 5, 3, 4, 2 (total 14)
    h.negative_buckets = vec![4, -1, 1, -1]; // counts: 4, 3, 4, 4 (total 15)
    let (chk, _) = app.append_histogram(&mut c, None, 0, ts, &mut h, false).unwrap();
    assert!(chk.is_none());
    let mut h_exp = h.copy();
    h_exp.counter_reset_hint = CounterResetHint::NotCounterReset;
    exp.push(TestResult { t: ts, h: h_exp.clone(), fh: h_exp.to_float() });
    assert_eq!(2, c.num_samples());

    // Add update with new appender.
    let mut app = c.appender().unwrap();

    ts += 14;
    h = h.copy();
    h.count = 54;
    h.zero_count += 2;
    h.sum = 24.4;
    h.positive_buckets = vec![6, 1, -3, 6]; // counts: 6, 7, 4, 10 (total 27)
    h.negative_buckets = vec![5, 1, -2, 3]; // counts: 5, 6, 4, 7 (total 22)
    let (chk, _) = app.append_histogram(&mut c, None, 0, ts, &mut h, false).unwrap();
    assert!(chk.is_none());
    let mut h_exp = h.copy();
    h_exp.counter_reset_hint = CounterResetHint::NotCounterReset;
    exp.push(TestResult { t: ts, h: h_exp.clone(), fh: h_exp.to_float() });
    assert_eq!(3, c.num_samples());

    // 1. Expand iterator in simple case.
    let mut it = Chunk::iterator(&c, None);
    assert!(it.err().is_none());
    let mut act = Vec::new();
    while it.next() == ValueType::Histogram {
        let (ts, h) = it.at_histogram(None);
        let (fts, fh) = it.at_float_histogram(None);
        assert_eq!(ts, fts);
        act.push(TestResult { t: ts, h, fh });
    }
    assert!(it.err().is_none());
    assert_eq!(exp, act);

    // 2. Expand second iterator while reusing first one.
    let mut it2 = Chunk::iterator(&c, Some(it));
    let mut act2 = Vec::new();
    while it2.next() == ValueType::Histogram {
        let (ts, h) = it2.at_histogram(None);
        let (fts, fh) = it2.at_float_histogram(None);
        assert_eq!(ts, fts);
        act2.push(TestResult { t: ts, h, fh });
    }
    assert!(it2.err().is_none());
    assert_eq!(exp, act2);

    // 3. Now recycle an iterator that was never used to access anything.
    let mut it_x = Chunk::iterator(&c, None);
    while it_x.next() == ValueType::Histogram {
        // Just iterate through without accessing anything.
    }
    let mut it3 = Chunk::iterator(&c, Some(it_x));
    let mut act3 = Vec::new();
    while it3.next() == ValueType::Histogram {
        let (ts, h) = it3.at_histogram(None);
        let (fts, fh) = it3.at_float_histogram(None);
        assert_eq!(ts, fts);
        act3.push(TestResult { t: ts, h, fh });
    }
    assert!(it3.err().is_none());
    assert_eq!(exp, act3);

    // 4. Test iterator Seek.
    let mid = exp.len() / 2;
    let mut it4 = Chunk::iterator(&c, None);
    let mut act4 = Vec::new();
    assert_eq!(ValueType::Histogram, it4.seek(exp[mid].t));
    // Below ones should not matter.
    assert_eq!(ValueType::Histogram, it4.seek(exp[mid].t));
    assert_eq!(ValueType::Histogram, it4.seek(exp[mid].t));
    let (ts, h) = it4.at_histogram(None);
    let (fts, fh) = it4.at_float_histogram(None);
    assert_eq!(ts, fts);
    act4.push(TestResult { t: ts, h, fh });
    while it4.next() == ValueType::Histogram {
        let (ts, h) = it4.at_histogram(None);
        let (fts, fh) = it4.at_float_histogram(None);
        assert_eq!(ts, fts);
        act4.push(TestResult { t: ts, h, fh });
    }
    assert!(it4.err().is_none());
    assert_eq!(exp[mid..].to_vec(), act4);
    assert_eq!(ValueType::None, it4.seek(exp[exp.len() - 1].t + 1));
}

// Mimics the scenario described for expandIntSpansAndBuckets.
#[test]
fn test_histogram_chunk_bucket_changes() {
    let mut c = HistogramChunk::new();

    // Create fresh appender and add the first histogram.
    let mut app = c.appender().unwrap();
    assert_eq!(0, c.num_samples());

    let ts1 = 1234567890i64;
    let mut h1 = Histogram {
        count: 27,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![sp(0, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4], // counts: 6, 3, 3, 2, 4, 5, 1 (total 24)
        negative_spans: vec![sp(1, 1)],
        negative_buckets: vec![1],
        ..Default::default()
    };

    let (chk, _) = app.append_histogram(&mut c, None, 0, ts1, &mut h1, false).unwrap();
    assert!(chk.is_none());
    assert_eq!(1, c.num_samples());

    // Add a new histogram that has expanded buckets.
    let ts2 = ts1 + 16;
    let mut h2 = h1.copy();
    h2.positive_spans = vec![sp(0, 3), sp(1, 1), sp(1, 4), sp(3, 3)];
    h2.negative_spans = vec![sp(0, 2)];
    h2.count = 35;
    h2.zero_count += 1;
    h2.sum = 30.0;
    // Existing histogram should get values converted from the above to:
    //   6 3 0 3 0 0 2 4 5 0 1 (previous values with some new empty buckets in between)
    // so the new histogram should have new counts >= these per-bucket counts, e.g.:
    h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1]; // 7 5 1 3 1 0 2 5 5 0 1 (total 30)
    // Existing histogram should get values converted from the above to:
    //   0 1 (previous values with some new empty buckets in between)
    // so the new histogram should have new counts >= these per-bucket counts, e.g.:
    h2.negative_buckets = vec![2, -1]; // 2 1 (total 3)
    // This is how span changes will be handled.
    let h_app = &app;
    let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
        h_app.appendable(&h2);
    assert!(!pos_interjections.is_empty());
    assert!(!neg_interjections.is_empty());
    assert!(backward_positive_inserts.is_empty());
    assert!(backward_negative_inserts.is_empty());
    assert!(ok); // Only new buckets came in.
    assert_eq!(CounterResetHeader::NOT_COUNTER_RESET, cr);
    let (mut c, mut app) =
        h_app.recode(&c, &pos_interjections, &neg_interjections, &h2.positive_spans, &h2.negative_spans);
    let (chk, _) = app.append_histogram(&mut c, None, 0, ts2, &mut h2, false).unwrap();
    assert!(chk.is_none());

    assert_eq!(2, c.num_samples());

    // Because the 2nd histogram has expanded buckets, we should expect all histograms (in particular the first)
    // to come back using the new spans metadata as well as the expanded buckets.
    h1.positive_spans = h2.positive_spans.clone();
    h1.positive_buckets = vec![6, -3, -3, 3, -3, 0, 2, 2, 1, -5, 1];
    h1.negative_spans = h2.negative_spans.clone();
    h1.negative_buckets = vec![0, 1];
    let mut h_exp = h2.copy();
    h_exp.counter_reset_hint = CounterResetHint::NotCounterReset;
    let exp = vec![
        TestResult { t: ts1, h: h1.clone(), fh: h1.to_float() },
        TestResult { t: ts2, h: h_exp.clone(), fh: h_exp.to_float() },
    ];
    let mut it = c.iterator();
    let mut act = Vec::new();
    while it.next() == ValueType::Histogram {
        let (ts, h) = it.at_histogram(None);
        let (fts, fh) = it.at_float_histogram(None);
        assert_eq!(ts, fts);
        act.push(TestResult { t: ts, h, fh });
    }
    assert!(it.err().is_none());
    assert_eq!(exp, act);
}

/// The `setup` closure of TestHistogramChunkAppendable / TestHistogramChunkAppendableGauge.
fn setup(h: &Histogram, exp_header: CounterResetHeader) -> (HistogramChunk, HistogramAppender, i64, Histogram) {
    let mut c = HistogramChunk::new();

    // Create fresh appender and add the first histogram.
    let mut app = c.appender().unwrap();
    assert_eq!(0, c.num_samples());

    let ts = 1234567890i64;

    let (chk, _) = app.append_histogram(&mut c, None, 0, ts, &mut h.copy(), false).unwrap();
    assert!(chk.is_none());
    assert_eq!(1, c.num_samples());
    assert_eq!(exp_header, c.get_counter_reset_header());
    (c, app, ts, h.clone())
}

#[test]
fn test_histogram_chunk_appendable() {
    let eh = Histogram {
        count: 5,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![sp(0, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4], // counts: 6, 3, 3, 2, 4, 5, 1 (total 24)
        ..Default::default()
    };

    let cbh = Histogram {
        count: 24,
        sum: 18.4,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![sp(0, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4], // counts: 6, 3, 3, 2, 4, 5, 1 (total 24)
        custom_values: custom_values(&[
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0,
            20.0,
        ]),
        ..Default::default()
    };

    let setup = |h: &Histogram| setup(h, CounterResetHeader::UNKNOWN_COUNTER_RESET);

    {
        // Schema change.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.schema += 1;
        let (_, _, _, _, ok, _) = h_app.appendable(&h2);
        assert!(!ok);

        assert_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
            CounterResetHint::UnknownCounterReset,
        );
    }

    {
        // Zero threshold change.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.zero_threshold += 0.1;
        let (_, _, _, _, ok, _) = h_app.appendable(&h2);
        assert!(!ok);

        assert_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
            CounterResetHint::UnknownCounterReset,
        );
    }

    {
        // New histogram that has more buckets.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(0, 3), sp(1, 1), sp(1, 4), sp(3, 3)];
        h2.count += 9;
        h2.zero_count += 1;
        h2.sum = 30.0;
        // Existing histogram should get values converted from the above to:
        //   6 3 0 3 0 0 2 4 5 0 1 (previous values with some new empty buckets in between)
        // so the new histogram should have new counts >= these per-bucket counts, e.g.:
        h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1]; // 7 5 1 3 1 0 2 5 5 0 1 (total 30)

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(!pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(ok); // Only new buckets came in.
        assert_eq!(CounterResetHeader::NOT_COUNTER_RESET, cr);

        assert_recoded_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
        );
    }

    {
        // New histogram that has a bucket missing.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(0, 2), sp(5, 2), sp(3, 1), sp(1, 1)];
        h2.sum = 21.0;
        h2.positive_buckets = vec![6, -3, -1, 2, 1, -4]; // counts: 6, 3, 2, 4, 5, 1 (total 21)

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(!ok); // Need to cut a new chunk.
        assert_eq!(CounterResetHeader::COUNTER_RESET, cr);

        assert_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::COUNTER_RESET,
            CounterResetHint::UnknownCounterReset,
        );
    }

    {
        // New histogram that has buckets missing but the buckets missing were empty.
        let mut empty_bucket_h = eh.copy();
        empty_bucket_h.positive_buckets = vec![6, -6, 1, 1, -2, 1, 1]; // counts: 6, 0, 1, 2, 0, 1, 2 (total 12)
        let (mut c, mut h_app, ts, h1) = setup(&empty_bucket_h);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![
            // Missing buckets at offset 1 and 9.
            sp(0, 1),
            sp(3, 1),
            sp(3, 1),
            sp(4, 1),
            sp(1, 1),
        ];
        let saved_h2_spans = h2.positive_spans.clone();
        h2.positive_buckets = vec![7, -5, 1, 0, 1]; // counts: 7, 2, 3, 3, 4 (total 18)

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(!backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(ok);
        assert_eq!(CounterResetHeader::NOT_COUNTER_RESET, cr);

        assert_no_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
        );

        // Check that h2 was recoded.
        assert_eq!(vec![7i64, -7, 2, 1, -3, 3, 1], h2.positive_buckets); // counts: 7, 0, 2, 3 , 0, 3, 4 (total 18)
        assert_eq!(empty_bucket_h.positive_spans, h2.positive_spans);
        assert_ne!(saved_h2_spans, h2.positive_spans, "recoding must make a copy");
    }

    {
        // New histogram that has new buckets AND buckets missing but the buckets missing were empty.
        let mut empty_bucket_h = eh.copy();
        empty_bucket_h.positive_buckets = vec![6, -6, 1, 1, -2, 1, 1]; // counts: 6, 0, 1, 2, 0, 1, 2 (total 12)
        let (mut c, mut h_app, ts, h1) = setup(&empty_bucket_h);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![
            // Missing buckets at offset 1 and 9.
            sp(0, 1),
            sp(3, 1),
            sp(3, 1),
            sp(4, 1),
            sp(1, 2),
        ];
        let saved_h2_spans = h2.positive_spans.clone();
        h2.positive_buckets = vec![7, -5, 1, 0, 1, 1]; // counts: 7, 2, 3, 3, 4, 5 (total 23)

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(!pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(!backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(ok);
        assert_eq!(CounterResetHeader::NOT_COUNTER_RESET, cr);

        assert_recoded_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
        );

        // Check that h2 was recoded.
        assert_eq!(vec![7i64, -7, 2, 1, -3, 3, 1, 1], h2.positive_buckets); // counts: 7, 0, 2, 3 , 0, 3, 5 (total 23)
        assert_eq!(
            vec![
                sp(0, 2), // Added empty bucket.
                sp(2, 1), // Existing - offset adjusted.
                sp(3, 2), // Added empty bucket.
                sp(3, 1), // Existing - offset adjusted.
                sp(1, 2), // Existing.
            ],
            h2.positive_spans
        );
        assert_ne!(saved_h2_spans, h2.positive_spans, "recoding must make a copy");
    }

    {
        // New histogram that has a counter reset while buckets are same.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.sum = 23.0;
        h2.positive_buckets = vec![6, -4, 1, -1, 2, 1, -4]; // counts: 6, 2, 3, 2, 4, 5, 1 (total 23)

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(!ok); // Need to cut a new chunk.
        assert_eq!(CounterResetHeader::COUNTER_RESET, cr);

        assert_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::COUNTER_RESET,
            CounterResetHint::UnknownCounterReset,
        );
    }

    {
        // New histogram that has a counter reset while new buckets were added.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(0, 3), sp(1, 1), sp(1, 4), sp(3, 3)];
        h2.sum = 29.0;
        // Existing histogram should get values converted from the above to:
        //   6 3 0 3 0 0 2 4 5 0 1 (previous values with some new empty buckets in between)
        // so the new histogram should have new counts >= these per-bucket counts, e.g.:
        h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 0]; // 7 5 1 3 1 0 2 5 5 0 0 (total 29)

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(!ok); // Need to cut a new chunk.
        assert_eq!(CounterResetHeader::COUNTER_RESET, cr);

        assert_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::COUNTER_RESET,
            CounterResetHint::UnknownCounterReset,
        );
    }

    {
        // New histogram that has a counter reset while new buckets were added before the first bucket and reset
        // on first bucket. (to catch the edge case where the new bucket should be forwarded ahead until first old
        // bucket at start)
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(-3, 2), sp(1, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)];
        h2.sum = 26.0;
        // Existing histogram should get values converted from the above to:
        //   0, 0, 6, 3, 3, 2, 4, 5, 1
        // so the new histogram should have new counts >= these per-bucket counts, e.g.:
        h2.positive_buckets = vec![1, 1, 3, -2, 0, -1, 2, 1, -4]; // counts: 1, 2, 5, 3, 3, 2, 4, 5, 1 (total 26)

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(!ok); // Need to cut a new chunk.
        assert_eq!(CounterResetHeader::COUNTER_RESET, cr);

        assert_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::COUNTER_RESET,
            CounterResetHint::UnknownCounterReset,
        );
    }

    {
        // New histogram that has an explicit counter reset.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.counter_reset_hint = CounterResetHint::CounterReset;

        assert_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::COUNTER_RESET,
            CounterResetHint::UnknownCounterReset,
        );
    }

    {
        // Start new chunk explicitly, and append a new histogram that is considered appendable to the previous
        // chunk.
        let (_, h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy(); // Identity is appendable.

        let mut next_chunk = HistogramChunk::new();
        let mut app = next_chunk.appender().unwrap();
        let (new_chunk, recoded) = app
            .append_histogram(&mut next_chunk, Some(&h_app as &dyn HistogramAppendable), 0, ts + 1, &mut h2, false)
            .unwrap();
        // adapted: Go also checks require.Equal(app, newApp); no new chunk means the same appender continues.
        assert!(new_chunk.is_none());
        assert!(!recoded);
        assert_sample_count(&next_chunk, 1, ValueType::Histogram);
        assert_eq!(CounterResetHeader::NOT_COUNTER_RESET, next_chunk.get_counter_reset_header());
        assert_first_int_histogram_sample_hint(&next_chunk, CounterResetHint::UnknownCounterReset);
    }

    {
        // Start new chunk explicitly, and append a new histogram that is not considered appendable to the
        // previous chunk.
        let (_, h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.count -= 1; // Make this not appendable due to counter reset.

        let mut next_chunk = HistogramChunk::new();
        let mut app = next_chunk.appender().unwrap();
        let (new_chunk, recoded) = app
            .append_histogram(&mut next_chunk, Some(&h_app as &dyn HistogramAppendable), 0, ts + 1, &mut h2, false)
            .unwrap();
        // adapted: Go also checks require.Equal(app, newApp); no new chunk means the same appender continues.
        assert!(new_chunk.is_none());
        assert!(!recoded);
        assert_sample_count(&next_chunk, 1, ValueType::Histogram);
        assert_eq!(CounterResetHeader::COUNTER_RESET, next_chunk.get_counter_reset_header());
        assert_first_int_histogram_sample_hint(&next_chunk, CounterResetHint::UnknownCounterReset);
    }

    {
        // Start new chunk explicitly, and append a new histogram that would need recoding if we added it to the
        // chunk.
        let (_, h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(0, 3), sp(1, 1), sp(1, 4), sp(3, 3)];
        h2.count += 9;
        h2.zero_count += 1;
        h2.sum = 30.0;
        // Existing histogram should get values converted from the above to:
        //   6 3 0 3 0 0 2 4 5 0 1 (previous values with some new empty buckets in between)
        // so the new histogram should have new counts >= these per-bucket counts, e.g.:
        h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1]; // 7 5 1 3 1 0 2 5 5 0 1 (total 30)

        let mut next_chunk = HistogramChunk::new();
        let mut app = next_chunk.appender().unwrap();
        let (new_chunk, recoded) = app
            .append_histogram(&mut next_chunk, Some(&h_app as &dyn HistogramAppendable), 0, ts + 1, &mut h2, false)
            .unwrap();
        // adapted: Go also checks require.Equal(app, newApp); no new chunk means the same appender continues.
        assert!(new_chunk.is_none());
        assert!(!recoded);
        assert_sample_count(&next_chunk, 1, ValueType::Histogram);
        assert_eq!(CounterResetHeader::NOT_COUNTER_RESET, next_chunk.get_counter_reset_header());
        assert_first_int_histogram_sample_hint(&next_chunk, CounterResetHint::UnknownCounterReset);
    }

    {
        // Start a new chunk with a histogram that has an empty bucket.
        // Add a histogram that has the same bucket missing.
        // This should be appendable and can happen if we are merging from chunks where the first sample came from
        // a recoded chunk that added the empty bucket.
        let mut h1 = eh.copy();
        // Add a bucket that is empty -10 offsets from the first bucket.
        h1.positive_spans = vec![Span::default(); eh.positive_spans.len() + 1];
        h1.positive_spans[0] = sp(eh.positive_spans[0].offset - 10, 1);
        h1.positive_spans[1] = sp(eh.positive_spans[0].offset + 9, eh.positive_spans[0].length);
        for (i, v) in eh.positive_spans[1..].iter().enumerate() {
            h1.positive_spans[i + 2] = *v;
        }
        h1.positive_buckets = vec![0; eh.positive_buckets.len() + 1];
        h1.positive_buckets[0] = 0;
        for (i, v) in eh.positive_buckets.iter().enumerate() {
            h1.positive_buckets[i + 1] = *v;
        }

        let (mut c, mut h_app, ts, _) = setup(&h1);
        let mut h2 = eh.copy();

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(!backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(ok);
        assert_eq!(CounterResetHeader::NOT_COUNTER_RESET, cr);

        assert_no_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
        );
    }

    {
        // Custom buckets, no change.
        let (mut c, mut h_app, ts, h1) = setup(&cbh);
        let mut h2 = h1.copy();
        let (_, _, _, _, ok, _) = h_app.appendable(&h2);
        assert!(ok);

        assert_no_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
        );
    }

    {
        // Custom buckets, increase in bucket counts but no change in layout.
        let (mut c, mut h_app, ts, h1) = setup(&cbh);
        let mut h2 = h1.copy();
        h2.count += 1;
        h2.positive_buckets = vec![6, -3, 0, -1, 2, 1, -3];
        let (_, _, _, _, ok, _) = h_app.appendable(&h2);
        assert!(ok);

        assert_no_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
        );
    }

    {
        // Custom buckets, decrease in bucket counts but no change in layout.
        let (mut c, mut h_app, ts, h1) = setup(&cbh);
        let mut h2 = h1.copy();
        h2.count -= 1;
        h2.positive_buckets = vec![6, -3, 0, -1, 2, 1, -5];
        let (_, _, _, _, ok, _) = h_app.appendable(&h2);
        assert!(!ok);

        assert_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::COUNTER_RESET,
            CounterResetHint::UnknownCounterReset,
        );
    }

    {
        // Custom buckets, change only in custom bounds.
        let (mut c, mut h_app, ts, h1) = setup(&cbh);
        let mut h2 = h1.copy();
        h2.custom_values = custom_values(&[
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0,
            20.0, 21.0,
        ]);
        let (_, _, _, _, ok, _) = h_app.appendable(&h2);
        assert!(!ok);

        assert_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::COUNTER_RESET,
            CounterResetHint::UnknownCounterReset,
        );
    }

    {
        // Custom buckets, with more buckets.
        let (mut c, mut h_app, ts, h1) = setup(&cbh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(0, 3), sp(1, 1), sp(1, 4), sp(3, 3)];
        h2.count += 6;
        h2.sum = 30.0;
        // Existing histogram should get values converted from the above to:
        //   6 3 0 3 0 0 2 4 5 0 1 (previous values with some new empty buckets in between)
        // so the new histogram should have new counts >= these per-bucket counts, e.g.:
        h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1]; // 7 5 1 3 1 0 2 5 5 0 1 (total 30)

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(!pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(ok); // Only new buckets came in.
        assert_eq!(CounterResetHeader::NOT_COUNTER_RESET, cr);

        assert_recoded_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
        );
    }

    {
        // New histogram with a different schema.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.schema = 2;

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(!ok); // Need to cut a new chunk.
        assert_eq!(CounterResetHeader::UNKNOWN_COUNTER_RESET, cr);

        assert_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
            CounterResetHint::UnknownCounterReset,
        );
    }

    {
        // New histogram with a different schema.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.zero_threshold = 1e-120;

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(!ok); // Need to cut a new chunk.
        assert_eq!(CounterResetHeader::UNKNOWN_COUNTER_RESET, cr);

        assert_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
            CounterResetHint::UnknownCounterReset,
        );
    }
}

/// Go `assertNewHistogramChunkOnAppend`.
fn assert_new_histogram_chunk_on_append(
    old_chunk: &mut HistogramChunk,
    h_app: &mut HistogramAppender,
    ts: i64,
    h: &mut Histogram,
    expect_header: CounterResetHeader,
    expect_hint: CounterResetHint,
) {
    let old_chunk_bytes = old_chunk.bytes().to_vec();
    let res = h_app.append_histogram(old_chunk, None, 0, ts, h, false);
    assert_eq!(old_chunk_bytes, old_chunk.bytes()); // Sanity check that previous chunk is untouched.
    let (new, recoded) = res.unwrap();
    let (new_chunk, new_appender) = new.expect("new chunk");
    assert!(!recoded);
    assert_ne!(*old_chunk, new_chunk);
    assert_eq!(expect_header, new_chunk.get_counter_reset_header());
    assert_ne!(*h_app, new_appender);
    assert_sample_count(&new_chunk, 1, ValueType::Histogram);
    assert_first_int_histogram_sample_hint(&new_chunk, expect_hint);
}

/// Go `assertNoNewHistogramChunkOnAppend`.
fn assert_no_new_histogram_chunk_on_append(
    curr_chunk: &mut HistogramChunk,
    h_app: &mut HistogramAppender,
    ts: i64,
    h: &mut Histogram,
    expect_header: CounterResetHeader,
) {
    let prev_chunk_bytes = curr_chunk.bytes().to_vec();
    let res = h_app.append_histogram(curr_chunk, None, 0, ts, h, false);
    assert!(curr_chunk.bytes().len() > prev_chunk_bytes.len()); // Check that current chunk is bigger than previously.
    let (new, recoded) = res.unwrap();
    // adapted: Go also checks require.Equal(hApp, newAppender); no new chunk means the same appender continues.
    assert!(new.is_none());
    assert!(!recoded);
    assert_eq!(expect_header, curr_chunk.get_counter_reset_header());
    assert_sample_count(curr_chunk, 2, ValueType::Histogram);
}

/// Go `assertRecodedHistogramChunkOnAppend`.
fn assert_recoded_histogram_chunk_on_append(
    prev_chunk: &mut HistogramChunk,
    h_app: &mut HistogramAppender,
    ts: i64,
    h: &mut Histogram,
    expect_header: CounterResetHeader,
) {
    let prev_chunk_bytes = prev_chunk.bytes().to_vec();
    let res = h_app.append_histogram(prev_chunk, None, 0, ts, h, false);
    // Sanity check that previous chunk is untouched. This may change in the future if we implement in-place
    // recoding.
    assert_eq!(prev_chunk_bytes, prev_chunk.bytes());
    let (new, recoded) = res.unwrap();
    let (new_chunk, new_appender) = new.expect("new chunk");
    assert!(recoded);
    assert_ne!(*prev_chunk, new_chunk);
    assert_eq!(expect_header, new_chunk.get_counter_reset_header());
    assert_ne!(*h_app, new_appender);
    assert_sample_count(&new_chunk, 2, ValueType::Histogram);
}

#[test]
fn test_histogram_chunk_appendable_with_empty_span() {
    let tests: Vec<(&str, Histogram, Histogram)> = vec![
        (
            "empty span in old and new histogram",
            Histogram {
                schema: 0,
                count: 21,
                sum: 1234.5,
                zero_threshold: 0.001,
                zero_count: 4,
                positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
                positive_buckets: vec![1, 1, -1, 0, 0, 0, 0],
                negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
                negative_buckets: vec![1, 1, -1, 1, 0, 0, 0],
                ..Default::default()
            },
            Histogram {
                schema: 0,
                count: 37,
                sum: 2345.6,
                zero_threshold: 0.001,
                zero_count: 5,
                positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
                negative_buckets: vec![1, 3, -2, 5, -2, 0, -3],
                ..Default::default()
            },
        ),
        (
            "empty span in old histogram",
            Histogram {
                schema: 0,
                count: 21,
                sum: 1234.5,
                zero_threshold: 0.001,
                zero_count: 4,
                positive_spans: vec![
                    sp(1, 0), // This span will disappear.
                    sp(2, 4),
                    sp(0, 3),
                ],
                positive_buckets: vec![1, 1, -1, 0, 0, 0, 0],
                negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
                negative_buckets: vec![1, 1, -1, 1, 0, 0, 0],
                ..Default::default()
            },
            Histogram {
                schema: 0,
                count: 37,
                sum: 2345.6,
                zero_threshold: 0.001,
                zero_count: 5,
                positive_spans: vec![sp(3, 4), sp(0, 3)],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
                negative_buckets: vec![1, 3, -2, 5, -2, 0, -3],
                ..Default::default()
            },
        ),
        (
            "empty span in new histogram",
            Histogram {
                schema: 0,
                count: 21,
                sum: 1234.5,
                zero_threshold: 0.001,
                zero_count: 4,
                positive_spans: vec![sp(0, 4), sp(3, 3)],
                positive_buckets: vec![1, 1, -1, 0, 0, 0, 0],
                negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
                negative_buckets: vec![1, 1, -1, 1, 0, 0, 0],
                ..Default::default()
            },
            Histogram {
                schema: 0,
                count: 37,
                sum: 2345.6,
                zero_threshold: 0.001,
                zero_count: 5,
                positive_spans: vec![
                    sp(0, 4),
                    sp(1, 0), // This span is new.
                    sp(2, 3),
                ],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
                negative_buckets: vec![1, 3, -2, 5, -2, 0, -3],
                ..Default::default()
            },
        ),
        (
            "two empty spans mixing offsets",
            Histogram {
                schema: 0,
                count: 21,
                sum: 1234.5,
                zero_threshold: 0.001,
                zero_count: 4,
                positive_spans: vec![sp(0, 4), sp(1, 0), sp(3, 0), sp(4, 3)],
                positive_buckets: vec![1, 1, -1, 0, 0, 0, 0],
                negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
                negative_buckets: vec![1, 1, -1, 1, 0, 0, 0],
                ..Default::default()
            },
            Histogram {
                schema: 0,
                count: 37,
                sum: 2345.6,
                zero_threshold: 0.001,
                zero_count: 5,
                positive_spans: vec![sp(0, 4), sp(3, 0), sp(1, 0), sp(4, 3)],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
                negative_buckets: vec![1, 3, -2, 5, -2, 0, -3],
                ..Default::default()
            },
        ),
        (
            "empty span in old and new custom buckets histogram",
            Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 7,
                sum: 1234.5,
                positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
                positive_buckets: vec![1, 1, -1, 0, 0, 0, 0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                ..Default::default()
            },
            Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 10,
                sum: 2345.6,
                positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                ..Default::default()
            },
        ),
    ];

    for (name, mut h1, h2) in tests {
        let mut c = HistogramChunk::new();

        // Create fresh appender and add the first histogram.
        let mut app = c.appender().unwrap();
        assert_eq!(0, c.num_samples(), "{name}");

        app.append_histogram(&mut c, None, 1, 0, &mut h1, true).unwrap();
        assert_eq!(1, c.num_samples(), "{name}");
        let h_app = &app;

        let (p_i, n_i, bp_i, bn_i, ok_to_append, counter_reset) = h_app.appendable(&h2);
        assert!(p_i.is_empty(), "{name}");
        assert!(n_i.is_empty(), "{name}");
        assert!(bp_i.is_empty(), "{name}");
        assert!(bn_i.is_empty(), "{name}");
        assert!(ok_to_append, "{name}");
        assert_eq!(CounterResetHeader::NOT_COUNTER_RESET, counter_reset, "{name}");
    }
}

#[test]
fn test_at_float_histogram() {
    let mut input = vec![
        Histogram {
            schema: 0,
            count: 21,
            sum: 1234.5,
            zero_threshold: 0.001,
            zero_count: 4,
            positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
            positive_buckets: vec![1, 1, -1, 0, 0, 0, 0],
            negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
            negative_buckets: vec![1, 1, -1, 1, 0, 0, 0],
            ..Default::default()
        },
        Histogram {
            schema: 0,
            count: 36,
            sum: 2345.6,
            zero_threshold: 0.001,
            zero_count: 5,
            positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
            positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
            negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
            negative_buckets: vec![1, 3, -2, 5, -2, 0, -3],
            ..Default::default()
        },
        Histogram {
            schema: 0,
            count: 36,
            sum: 1111.1,
            zero_threshold: 0.001,
            zero_count: 5,
            positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
            positive_buckets: vec![1, 2, -2, 2, -1, 0, 0],
            negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
            negative_buckets: vec![1, 3, -2, 5, -1, 0, -3],
            ..Default::default()
        },
    ];

    let exp_output = vec![
        FloatHistogram {
            schema: 0,
            count: 21.0,
            sum: 1234.5,
            zero_threshold: 0.001,
            zero_count: 4.0,
            positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
            positive_buckets: vec![1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
            negative_buckets: vec![1.0, 2.0, 1.0, 2.0, 2.0, 2.0, 2.0],
            ..Default::default()
        },
        FloatHistogram {
            counter_reset_hint: CounterResetHint::NotCounterReset,
            schema: 0,
            count: 36.0,
            sum: 2345.6,
            zero_threshold: 0.001,
            zero_count: 5.0,
            positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
            positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 1.0],
            negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
            negative_buckets: vec![1.0, 4.0, 2.0, 7.0, 5.0, 5.0, 2.0],
            ..Default::default()
        },
        FloatHistogram {
            counter_reset_hint: CounterResetHint::NotCounterReset,
            schema: 0,
            count: 36.0,
            sum: 1111.1,
            zero_threshold: 0.001,
            zero_count: 5.0,
            positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
            positive_buckets: vec![1.0, 3.0, 1.0, 3.0, 2.0, 2.0, 2.0],
            negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
            negative_buckets: vec![1.0, 4.0, 2.0, 7.0, 6.0, 6.0, 3.0],
            ..Default::default()
        },
    ];

    let mut chk = HistogramChunk::new();
    let mut app = chk.appender().unwrap();
    for (i, h) in input.iter_mut().enumerate() {
        let (newc, _) = app.append_histogram(&mut chk, None, 0, i as i64, h, false).unwrap();
        assert!(newc.is_none());
    }
    let mut it = chk.iterator();
    let mut i = 0i64;
    while it.next() != ValueType::None {
        let (ts, h) = it.at_float_histogram(None);
        assert_eq!(i, ts);
        assert_eq!(exp_output[i as usize], h, "histogram {i} unequal");
        i += 1;
    }
}

#[test]
fn test_histogram_chunk_appendable_gauge() {
    let eh = Histogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        count: 5,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![sp(0, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4], // {6, 3, 3, 2, 4, 5, 1}
        ..Default::default()
    };

    let cbh = Histogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        count: 24,
        sum: 18.4,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![sp(0, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4], // {6, 3, 3, 2, 4, 5, 1}
        custom_values: custom_values(&[
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0,
            20.0,
        ]),
        ..Default::default()
    };

    let setup = |h: &Histogram| setup(h, CounterResetHeader::GAUGE_TYPE);

    {
        // Schema change.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.schema += 1;
        let (_, _, _, _, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(!ok);

        assert_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::GAUGE_TYPE,
            CounterResetHint::GaugeType,
        );
    }

    {
        // Zero threshold change.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.zero_threshold += 0.1;
        let (_, _, _, _, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(!ok);

        assert_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::GAUGE_TYPE,
            CounterResetHint::GaugeType,
        );
    }

    {
        // New histogram that has more buckets.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(0, 3), sp(1, 1), sp(1, 4), sp(3, 3)];
        h2.count += 9;
        h2.zero_count += 1;
        h2.sum = 30.0;
        h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1]; // {7, 5, 1, 3, 1, 0, 2, 5, 5, 0, 1}

        let (p_i, n_i, p_backward_i, n_backward_i, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(!p_i.is_empty());
        assert!(n_i.is_empty());
        assert!(p_backward_i.is_empty());
        assert!(n_backward_i.is_empty());
        assert!(ok);

        assert_recoded_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // New histogram that has buckets missing.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(0, 2), sp(2, 1), sp(3, 1), sp(4, 1), sp(1, 1)];
        h2.count -= 4;
        h2.sum -= 1.0;
        h2.positive_buckets = vec![6, -3, 0, -1, 3, -4]; // {6, 3, 3, 2, 5, 1}

        let (p_i, n_i, p_backward_i, n_backward_i, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(p_i.is_empty());
        assert!(n_i.is_empty());
        assert!(!p_backward_i.is_empty());
        assert!(n_backward_i.is_empty());
        assert!(ok);

        assert_no_new_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // New histogram that has a bucket missing and new buckets.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(0, 2), sp(5, 2), sp(3, 1), sp(1, 1)];
        h2.sum = 21.0;
        h2.positive_buckets = vec![6, -3, -1, 2, 1, -4]; // {6, 3, 2, 4, 5, 1}

        let (p_i, n_i, p_backward_i, n_backward_i, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(!p_i.is_empty());
        assert!(!p_backward_i.is_empty());
        assert!(n_i.is_empty());
        assert!(n_backward_i.is_empty());
        assert!(ok);

        assert_recoded_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // New histogram that has a counter reset while buckets are same.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.sum = 23.0;
        h2.positive_buckets = vec![6, -4, 1, -1, 2, 1, -4]; // {6, 2, 3, 2, 4, 5, 1}

        let (p_i, n_i, p_backward_i, n_backward_i, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(p_i.is_empty());
        assert!(n_i.is_empty());
        assert!(p_backward_i.is_empty());
        assert!(n_backward_i.is_empty());
        assert!(ok);

        assert_no_new_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // New histogram that has a counter reset while new buckets were added.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(0, 3), sp(1, 1), sp(1, 4), sp(3, 3)];
        h2.sum = 29.0;
        h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 0]; // {7, 5, 1, 3, 1, 0, 2, 5, 5, 0, 0}

        let (p_i, n_i, p_backward_i, n_backward_i, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(!p_i.is_empty());
        assert!(n_i.is_empty());
        assert!(p_backward_i.is_empty());
        assert!(n_backward_i.is_empty());
        assert!(ok);

        assert_recoded_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // New histogram that has a counter reset while new buckets were added before the first bucket and reset
        // on first bucket.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(-3, 2), sp(1, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)];
        h2.sum = 26.0;
        h2.positive_buckets = vec![1, 1, 3, -2, 0, -1, 2, 1, -4]; // {1, 2, 5, 3, 3, 2, 4, 5, 1}

        let (p_i, n_i, p_backward_i, n_backward_i, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(!p_i.is_empty());
        assert!(n_i.is_empty());
        assert!(p_backward_i.is_empty());
        assert!(n_backward_i.is_empty());
        assert!(ok);

        assert_recoded_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // Custom buckets, no change.
        let (mut c, mut h_app, ts, h1) = setup(&cbh);
        let mut h2 = h1.copy();
        let (_, _, _, _, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(ok);

        assert_no_new_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // Custom buckets, increase in bucket counts but no change in layout.
        let (mut c, mut h_app, ts, h1) = setup(&cbh);
        let mut h2 = h1.copy();
        h2.count += 1;
        h2.positive_buckets = vec![6, -3, 0, -1, 2, 1, -3];
        let (_, _, _, _, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(ok);

        assert_no_new_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // Custom buckets, decrease in bucket counts but no change in layout.
        let (mut c, mut h_app, ts, h1) = setup(&cbh);
        let mut h2 = h1.copy();
        h2.count -= 1;
        h2.positive_buckets = vec![6, -3, 0, -1, 2, 1, -5];
        let (_, _, _, _, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(ok);

        assert_no_new_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // Custom buckets, change only in custom bounds.
        let (mut c, mut h_app, ts, h1) = setup(&cbh);
        let mut h2 = h1.copy();
        h2.custom_values = custom_values(&[
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0,
            20.0, 21.0,
        ]);
        let (_, _, _, _, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(!ok);

        assert_new_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::GAUGE_TYPE,
            CounterResetHint::GaugeType,
        );
    }

    {
        // Custom buckets, with more buckets.
        let (mut c, mut h_app, ts, h1) = setup(&cbh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(0, 3), sp(1, 1), sp(1, 4), sp(3, 3)];
        h2.count += 6;
        h2.sum = 30.0;
        // Existing histogram should get values converted from the above to:
        //   6 3 0 3 0 0 2 4 5 0 1 (previous values with some new empty buckets in between)
        // so the new histogram should have new counts >= these per-bucket counts, e.g.:
        h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1]; // 7 5 1 3 1 0 2 5 5 0 1 (total 30)

        let (pos_interjections, neg_interjections, p_backward_i, n_backward_i, _, _, ok) =
            h_app.appendable_gauge(&h2);
        assert!(!pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(p_backward_i.is_empty());
        assert!(n_backward_i.is_empty());
        assert!(ok); // Only new buckets came in.

        assert_recoded_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }
}

#[test]
fn test_histogram_append_only_errors() {
    // adapted: in Go the erroring AppendHistogram also returns a nil chunk and isRecoded false; the Rust call
    // returns only the error.
    {
        // "schema change error".
        let mut c = HistogramChunk::new();

        // Create fresh appender and add the first histogram.
        let mut app = c.appender().unwrap();

        let mut h = generate_test_histogram(0);
        let (nc, is_recoded) = app.append_histogram(&mut c, None, 0, 1, &mut h, true).unwrap();
        assert!(nc.is_none());
        assert!(!is_recoded);

        // Add erroring histogram.
        let mut h2 = h.copy();
        h2.schema += 1;
        let err = app.append_histogram(&mut c, None, 0, 2, &mut h2, true).unwrap_err();
        assert_eq!("histogram schema change", err.to_string());
    }
    {
        // "counter reset error".
        let mut c = HistogramChunk::new();

        // Create fresh appender and add the first histogram.
        let mut app = c.appender().unwrap();

        let mut h = generate_test_histogram(0);
        let (nc, is_recoded) = app.append_histogram(&mut c, None, 0, 1, &mut h, true).unwrap();
        assert!(nc.is_none());
        assert!(!is_recoded);

        // Add erroring histogram.
        let mut h2 = h.copy();
        h2.counter_reset_hint = CounterResetHint::CounterReset;
        let err = app.append_histogram(&mut c, None, 0, 2, &mut h2, true).unwrap_err();
        assert_eq!("histogram counter reset", err.to_string());
    }
    {
        // "counter reset error with custom buckets".
        let mut c = HistogramChunk::new();

        // Create fresh appender and add the first histogram.
        let mut app = c.appender().unwrap();

        let mut h = generate_test_custom_buckets_histogram(0);
        let (nc, is_recoded) = app.append_histogram(&mut c, None, 0, 1, &mut h, true).unwrap();
        assert!(nc.is_none());
        assert!(!is_recoded);

        // Add erroring histogram.
        let mut h2 = h.copy();
        h2.custom_values = custom_values(&[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]);
        let err = app.append_histogram(&mut c, None, 0, 2, &mut h2, true).unwrap_err();
        assert_eq!("histogram counter reset", err.to_string());
    }
}

fn two_sample_chunk(h1: &Histogram) -> HistogramChunk {
    let mut h1 = h1.clone();
    let mut h2 = h1.copy();

    // Create a chunk and append both histograms.
    let mut c = HistogramChunk::new();
    let mut app = c.appender().unwrap();

    app.append_histogram(&mut c, None, 0, 0, &mut h1, false).unwrap();
    app.append_histogram(&mut c, None, 0, 1, &mut h2, false).unwrap();
    c
}

fn unique_spans_histogram() -> Histogram {
    // Create two histograms with the same schema and spans.
    Histogram {
        schema: 1,
        zero_threshold: 1e-100,
        count: 10,
        zero_count: 2,
        sum: 15.0,
        positive_spans: vec![sp(0, 2), sp(1, 2)],
        positive_buckets: vec![1, 2, 3, 4],
        negative_spans: vec![sp(1, 1)],
        negative_buckets: vec![2],
        ..Default::default()
    }
}

#[test]
fn test_histogram_unique_spans_after_next_with_at_histogram() {
    let h1 = unique_spans_histogram();
    let c = two_sample_chunk(&h1);

    // Create an iterator and advance to the first histogram.
    let mut it = c.iterator();
    assert_eq!(ValueType::Histogram, it.next());
    let (_, rh1) = it.at_histogram(None);

    // Advance to the second histogram and retrieve it.
    assert_eq!(ValueType::Histogram, it.next());
    let (_, rh2) = it.at_histogram(None);

    assert_eq!(rh1.positive_spans, h1.positive_spans, "Returned positive spans are as expected");
    assert_eq!(rh1.negative_spans, h1.negative_spans, "Returned negative spans are as expected");
    assert_eq!(rh2.positive_spans, h1.positive_spans, "Returned positive spans are as expected");
    assert_eq!(rh2.negative_spans, h1.negative_spans, "Returned negative spans are as expected");

    // Check that the spans for h1 and h2 are unique slices.
    assert!(
        !std::ptr::eq(&rh1.positive_spans[0], &rh2.positive_spans[0]),
        "PositiveSpans should be unique between histograms"
    );
    assert!(
        !std::ptr::eq(&rh1.negative_spans[0], &rh2.negative_spans[0]),
        "NegativeSpans should be unique between histograms"
    );
}

#[test]
fn test_histogram_unique_spans_after_next_with_at_float_histogram() {
    let h1 = unique_spans_histogram();
    let c = two_sample_chunk(&h1);

    // Create an iterator and advance to the first histogram.
    let mut it = c.iterator();
    assert_eq!(ValueType::Histogram, it.next());
    let (_, rh1) = it.at_float_histogram(None);

    // Advance to the second histogram and retrieve it.
    assert_eq!(ValueType::Histogram, it.next());
    let (_, rh2) = it.at_float_histogram(None);

    assert_eq!(rh1.positive_spans, h1.positive_spans, "Returned positive spans are as expected");
    assert_eq!(rh1.negative_spans, h1.negative_spans, "Returned negative spans are as expected");
    assert_eq!(rh2.positive_spans, h1.positive_spans, "Returned positive spans are as expected");
    assert_eq!(rh2.negative_spans, h1.negative_spans, "Returned negative spans are as expected");

    // Check that the spans for h1 and h2 are unique slices.
    assert!(
        !std::ptr::eq(&rh1.positive_spans[0], &rh2.positive_spans[0]),
        "PositiveSpans should be unique between histograms"
    );
    assert!(
        !std::ptr::eq(&rh1.negative_spans[0], &rh2.negative_spans[0]),
        "NegativeSpans should be unique between histograms"
    );
}

fn custom_values_histogram() -> Histogram {
    // Create two histograms with the same schema and custom values.
    Histogram {
        schema: -53,
        count: 10,
        sum: 15.0,
        positive_spans: vec![sp(0, 2), sp(1, 2)],
        positive_buckets: vec![1, 2, 3, 4],
        custom_values: custom_values(&[10.0, 11.0, 12.0, 13.0]),
        ..Default::default()
    }
}

#[test]
fn test_histogram_custom_values_interned_after_next_with_at_histogram() {
    let h1 = custom_values_histogram();
    let c = two_sample_chunk(&h1);

    // Create an iterator and advance to the first histogram.
    let mut it = c.iterator();
    assert_eq!(ValueType::Histogram, it.next());
    let (_, rh1) = it.at_histogram(None);

    // Advance to the second histogram and retrieve it.
    assert_eq!(ValueType::Histogram, it.next());
    let (_, rh2) = it.at_histogram(None);

    assert_eq!(rh1.positive_spans, h1.positive_spans, "Returned positive spans are as expected");
    assert_eq!(rh1.custom_values, h1.custom_values, "Returned custom values are as expected");
    assert_eq!(rh2.positive_spans, h1.positive_spans, "Returned positive spans are as expected");
    assert_eq!(rh2.custom_values, h1.custom_values, "Returned custom values are as expected");

    // Check that the spans and custom values for h1 and h2 are unique slices.
    assert!(
        !std::ptr::eq(&rh1.positive_spans[0], &rh2.positive_spans[0]),
        "PositiveSpans should be unique between histograms"
    );
    assert!(
        std::ptr::eq(&rh1.custom_values_slice()[0], &rh2.custom_values_slice()[0]),
        "CustomValues should be unique between histograms"
    );
}

#[test]
fn test_histogram_custom_values_interned_after_next_with_at_float_histogram() {
    let h1 = custom_values_histogram();
    let c = two_sample_chunk(&h1);

    // Create an iterator and advance to the first histogram.
    let mut it = c.iterator();
    assert_eq!(ValueType::Histogram, it.next());
    let (_, rh1) = it.at_float_histogram(None);

    // Advance to the second histogram and retrieve it.
    assert_eq!(ValueType::Histogram, it.next());
    let (_, rh2) = it.at_float_histogram(None);

    assert_eq!(rh1.positive_spans, h1.positive_spans, "Returned positive spans are as expected");
    assert_eq!(rh1.custom_values, h1.custom_values, "Returned custom values are as expected");
    assert_eq!(rh2.positive_spans, h1.positive_spans, "Returned positive spans are as expected");
    assert_eq!(rh2.custom_values, h1.custom_values, "Returned custom values are as expected");

    // Check that the spans and custom values for h1 and h2 are unique slices.
    assert!(
        !std::ptr::eq(&rh1.positive_spans[0], &rh2.positive_spans[0]),
        "PositiveSpans should be unique between histograms"
    );
    assert!(
        std::ptr::eq(&rh1.custom_values_slice()[0], &rh2.custom_values_slice()[0]),
        "CustomValues should be unique between histograms"
    );
}

/// Go `assertFirstIntHistogramSampleHint`.
fn assert_first_int_histogram_sample_hint(chunk: &dyn Chunk, expected: CounterResetHint) {
    let mut it = chunk.iterator(None);
    assert_eq!(ValueType::Histogram, it.next());
    let (_, v) = it.at_histogram(None);
    assert_eq!(expected, v.counter_reset_hint);
}

#[test]
fn test_int_histogram_empty_buckets_with_gaps() {
    let mut h1 = Histogram {
        positive_spans: vec![sp(-19, 2), sp(1, 2)],
        positive_buckets: vec![0, 0, 0, 0],
        ..Default::default()
    };
    h1.validate().unwrap();

    let mut c = HistogramChunk::new();
    let mut app = c.appender().unwrap();
    app.append_histogram(&mut c, None, 0, 1, &mut h1, false).unwrap();

    let mut h2 = Histogram {
        positive_spans: vec![sp(-19, 1), sp(4, 1), sp(3, 1)],
        positive_buckets: vec![0, 0, 0],
        ..Default::default()
    };
    h2.validate().unwrap();

    let (new_c, recoded) = app.append_histogram(&mut c, None, 0, 2, &mut h2, false).unwrap();
    assert!(recoded);
    let (new_c, _) = new_c.expect("new chunk");

    let mut it = new_c.iterator();
    assert_eq!(ValueType::Histogram, it.next());
    let (_, h) = it.at_float_histogram(None);
    h.validate().unwrap();
    assert_eq!(ValueType::Histogram, it.next());
    let (_, h) = it.at_float_histogram(None);
    h.validate().unwrap();
    assert_eq!(ValueType::None, it.next());
    assert!(it.err().is_none());
}

#[test]
fn test_histogram_iterator_fail_if_schema_in_valid() {
    for schema in [-101i32, 101] {
        let mut h = Histogram {
            schema,
            count: 10,
            sum: 15.0,
            zero_threshold: 1e-100,
            positive_spans: vec![sp(0, 2), sp(1, 2)],
            positive_buckets: vec![1, 2, 3, 4],
            ..Default::default()
        };

        let mut c = HistogramChunk::new();
        let mut app = c.appender().unwrap();

        app.append_histogram(&mut c, None, 0, 1, &mut h, false).unwrap();

        let mut it = c.iterator();
        assert_eq!(ValueType::None, it.next(), "schema {schema}");
        let err = it.err().expect("error");
        assert!(err.is_histogram(ErrorKind::HistogramsUnknownSchema), "schema {schema}: {err}");
    }
}

#[test]
fn test_histogram_iterator_reduce_schema() {
    for schema in [9i32, 52] {
        let mut h = Histogram {
            schema,
            count: 10,
            sum: 15.0,
            zero_threshold: 1e-100,
            positive_spans: vec![sp(0, 2), sp(1, 2)],
            positive_buckets: vec![1, 2, 3, 4],
            ..Default::default()
        };

        let mut c = HistogramChunk::new();
        let mut app = c.appender().unwrap();

        app.append_histogram(&mut c, None, 0, 1, &mut h, false).unwrap();

        let mut it = c.iterator();
        assert_eq!(ValueType::Histogram, it.next(), "schema {schema}");
        let (_, rh) = it.at_histogram(None);
        assert_eq!(EXPONENTIAL_SCHEMA_MAX, rh.schema, "schema {schema}");

        let (_, rfh) = it.at_float_histogram(None);
        assert_eq!(EXPONENTIAL_SCHEMA_MAX, rfh.schema, "schema {schema}");
    }
}

#[test]
fn test_histogram_chunk_over_flow_panics() {
    common::test_chunk_over_flow_panics(ENC_HISTOGRAM, ValueType::Histogram);
}
