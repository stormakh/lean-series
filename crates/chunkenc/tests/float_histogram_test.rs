//! Port of `tsdb/chunkenc/float_histogram_test.go`.

mod common;
use common::{generate_test_custom_buckets_float_histogram, generate_test_float_histogram};

use chunkenc::Iterator as _;
use chunkenc::{
    Chunk, ChunkIterator, CounterResetHeader, ENC_FLOAT_HISTOGRAM, FloatHistogramAppender, FloatHistogramChunk,
    ValueType,
};
use model::histogram::{
    CUSTOM_BUCKETS_SCHEMA, CounterResetHint, EXPONENTIAL_SCHEMA_MAX, ErrorKind, FloatHistogram, Histogram, Span,
    custom_values,
};

/// Go `floatResult`.
#[derive(Clone, Debug, PartialEq)]
struct FloatResult {
    t: i64,
    h: FloatHistogram,
}

fn sp(offset: i32, length: u32) -> Span {
    Span::new(offset, length)
}

/// Go `assertSampleCount` (histogram_test.go).
fn assert_sample_count(c: &dyn Chunk, exp: i64, vtype: ValueType) {
    let mut count = 0i64;
    let mut it = c.iterator(None);
    assert!(it.err().is_none(), "{:?}", it.err());
    while it.next() == vtype {
        count += 1;
    }
    assert!(it.err().is_none(), "{:?}", it.err());
    assert_eq!(exp, count);
}

/// Go `assertFirstFloatHistogramSampleHint`.
fn assert_first_float_histogram_sample_hint(c: &dyn Chunk, expected: CounterResetHint) {
    let mut it = c.iterator(None);
    assert_eq!(ValueType::FloatHistogram, it.next());
    let (_, v) = it.at_float_histogram(None);
    assert_eq!(expected, v.counter_reset_hint);
}

fn collect(it: &mut ChunkIterator<'_>) -> Vec<FloatResult> {
    let mut act = Vec::new();
    while it.next() == ValueType::FloatHistogram {
        let (t, h) = it.at_float_histogram(None);
        act.push(FloatResult { t, h });
    }
    act
}

#[test]
fn test_first_float_histogram_explicit_counter_reset() {
    let tests: [(&str, CounterResetHint, CounterResetHeader, CounterResetHint); 4] = [
        (
            "CounterReset",
            CounterResetHint::CounterReset,
            CounterResetHeader::COUNTER_RESET,
            CounterResetHint::UnknownCounterReset,
        ),
        (
            "NotCounterReset",
            CounterResetHint::NotCounterReset,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
            CounterResetHint::UnknownCounterReset,
        ),
        (
            "UnknownCounterReset",
            CounterResetHint::UnknownCounterReset,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
            CounterResetHint::UnknownCounterReset,
        ),
        ("Gauge", CounterResetHint::GaugeType, CounterResetHeader::GAUGE_TYPE, CounterResetHint::GaugeType),
    ];
    for (name, hint, exp_header, exp_hint) in tests {
        let mut h = FloatHistogram { counter_reset_hint: hint, ..Default::default() };
        let mut chk = FloatHistogramChunk::new();
        let mut app = chk.appender().unwrap();
        let (new_chk, recoded) = app.append_float_histogram(&mut chk, None, 0, 0, &mut h, false).unwrap();
        assert!(new_chk.is_none(), "{name}");
        assert!(!recoded, "{name}");
        // adapted: Go checks that the returned appender is `app`; in Rust no new chunk means `app` continues.
        assert_eq!(exp_header, chk.get_counter_reset_header(), "{name}");
        assert_first_float_histogram_sample_hint(&chk, exp_hint);
    }
}

#[test]
fn test_float_histogram_chunk_same_buckets() {
    let mut c = FloatHistogramChunk::new();
    let mut exp: Vec<FloatResult> = Vec::new();

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
    let (chk, _) = app.append_float_histogram(&mut c, None, 0, ts, &mut h.to_float(), false).unwrap();
    assert!(chk.is_none());
    exp.push(FloatResult { t: ts, h: h.to_float() });
    assert_eq!(1, c.num_samples());

    // Add an updated histogram.
    ts += 16;
    h = h.copy();
    h.count = 32;
    h.zero_count += 1;
    h.sum = 24.4;
    h.positive_buckets = vec![5, -2, 1, -2]; // counts: 5, 3, 4, 2 (total 14)
    h.negative_buckets = vec![4, -1, 1, -1]; // counts: 4, 3, 4, 4 (total 15)
    let (chk, _) = app.append_float_histogram(&mut c, None, 0, ts, &mut h.to_float(), false).unwrap();
    assert!(chk.is_none());
    let mut exp_h = h.to_float();
    exp_h.counter_reset_hint = CounterResetHint::NotCounterReset;
    exp.push(FloatResult { t: ts, h: exp_h });
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
    let (chk, _) = app.append_float_histogram(&mut c, None, 0, ts, &mut h.to_float(), false).unwrap();
    assert!(chk.is_none());
    let mut exp_h = h.to_float();
    exp_h.counter_reset_hint = CounterResetHint::NotCounterReset;
    exp.push(FloatResult { t: ts, h: exp_h });
    assert_eq!(3, c.num_samples());

    // 1. Expand iterator in simple case.
    let mut it = Chunk::iterator(&c, None);
    assert!(it.err().is_none());
    let act = collect(&mut it);
    assert!(it.err().is_none(), "{:?}", it.err());
    assert_eq!(exp, act);

    // 2. Expand second iterator while reusing first one.
    let mut it2 = Chunk::iterator(&c, Some(it));
    let act2 = collect(&mut it2);
    assert!(it2.err().is_none(), "{:?}", it2.err());
    assert_eq!(exp, act2);

    // 3. Now recycle an iterator that was never used to access anything.
    let mut it_x = Chunk::iterator(&c, None);
    while it_x.next() == ValueType::FloatHistogram {
        // Just iterate through without accessing anything.
    }
    let mut it3 = Chunk::iterator(&c, Some(it_x));
    let act3 = collect(&mut it3);
    assert!(it3.err().is_none(), "{:?}", it3.err());
    assert_eq!(exp, act3);

    // 4. Test iterator Seek.
    let mid = exp.len() / 2;
    let mut it4 = Chunk::iterator(&c, None);
    let mut act4 = Vec::new();
    assert_eq!(ValueType::FloatHistogram, it4.seek(exp[mid].t));
    // Below ones should not matter.
    assert_eq!(ValueType::FloatHistogram, it4.seek(exp[mid].t));
    assert_eq!(ValueType::FloatHistogram, it4.seek(exp[mid].t));
    let (fts, fh) = it4.at_float_histogram(None);
    act4.push(FloatResult { t: fts, h: fh });
    act4.extend(collect(&mut it4));
    assert!(it4.err().is_none(), "{:?}", it4.err());
    assert_eq!(exp[mid..], act4[..]);
    assert_eq!(ValueType::None, it4.seek(exp[exp.len() - 1].t + 1));
}

// Mimics the scenario described for expandFloatSpansAndBuckets.
#[test]
fn test_float_histogram_chunk_bucket_changes() {
    let mut c = FloatHistogramChunk::new();

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

    let (chk, _) = app.append_float_histogram(&mut c, None, 0, ts1, &mut h1.to_float(), false).unwrap();
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
        h_app.appendable(&h2.to_float());
    assert!(!pos_interjections.is_empty());
    assert!(!neg_interjections.is_empty());
    assert!(backward_positive_inserts.is_empty());
    assert!(backward_negative_inserts.is_empty());
    assert!(ok); // Only new buckets came in.
    assert!(!cr);
    let (mut c, mut app) =
        h_app.recode(&c, &pos_interjections, &neg_interjections, &h2.positive_spans, &h2.negative_spans);
    let (chk, _) = app.append_float_histogram(&mut c, None, 0, ts2, &mut h2.to_float(), false).unwrap();
    assert!(chk.is_none());
    assert_eq!(2, c.num_samples());

    // Because the 2nd histogram has expanded buckets, we should expect all
    // histograms (in particular the first) to come back using the new spans
    // metadata as well as the expanded buckets.
    h1.positive_spans = h2.positive_spans.clone();
    h1.positive_buckets = vec![6, -3, -3, 3, -3, 0, 2, 2, 1, -5, 1];
    h1.negative_spans = h2.negative_spans.clone();
    h1.negative_buckets = vec![0, 1];
    let mut exp_h2 = h2.to_float();
    exp_h2.counter_reset_hint = CounterResetHint::NotCounterReset;
    let exp = vec![FloatResult { t: ts1, h: h1.to_float() }, FloatResult { t: ts2, h: exp_h2 }];
    let mut it = Chunk::iterator(&c, None);
    let act = collect(&mut it);
    assert!(it.err().is_none(), "{:?}", it.err());
    assert_eq!(exp, act);
}

/// The `setup` closure of TestFloatHistogramChunkAppendable(Gauge).
fn setup(h: &FloatHistogram, exp_header: CounterResetHeader) -> (FloatHistogramChunk, FloatHistogramAppender, i64, FloatHistogram) {
    let mut c = FloatHistogramChunk::new();

    // Create fresh appender and add the first histogram.
    let mut app = c.appender().unwrap();
    assert_eq!(0, c.num_samples());

    let ts = 1234567890i64;

    let (chk, _) = app.append_float_histogram(&mut c, None, 0, ts, &mut h.copy(), false).unwrap();
    assert!(chk.is_none());
    assert_eq!(1, c.num_samples());
    assert_eq!(exp_header, c.get_counter_reset_header());
    (c, app, ts, h.clone())
}

#[test]
fn test_float_histogram_chunk_appendable() {
    let eh = FloatHistogram {
        count: 5.0,
        zero_count: 2.0,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![sp(0, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)],
        positive_buckets: vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 1.0],
        ..Default::default()
    };

    let cbh = FloatHistogram {
        count: 24.0,
        sum: 18.4,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![sp(0, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)],
        positive_buckets: vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 1.0],
        custom_values: custom_values(&[
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0,
            20.0,
        ]),
        ..Default::default()
    };

    let setup = |h: &FloatHistogram| setup(h, CounterResetHeader::UNKNOWN_COUNTER_RESET);

    {
        // Schema change.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.schema += 1;
        let (_, _, _, _, ok, _) = h_app.appendable(&h2);
        assert!(!ok);

        assert_new_float_histogram_chunk_on_append(
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

        assert_new_float_histogram_chunk_on_append(
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
        h2.count += 9.0;
        h2.zero_count += 1.0;
        h2.sum = 30.0;
        h2.positive_buckets = vec![7.0, 5.0, 1.0, 3.0, 1.0, 0.0, 2.0, 5.0, 5.0, 0.0, 1.0];

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(!pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(ok); // Only new buckets came in.
        assert!(!cr);

        assert_recoded_float_histogram_chunk_on_append(
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
        h2.positive_buckets = vec![6.0, 3.0, 2.0, 4.0, 5.0, 1.0];

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(!ok); // Need to cut a new chunk.
        assert!(cr);

        assert_new_float_histogram_chunk_on_append(
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
        empty_bucket_h.positive_buckets = vec![6.0, 0.0, 3.0, 2.0, 4.0, 0.0, 1.0];
        let (mut c, mut h_app, ts, h1) = setup(&empty_bucket_h);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(0, 1), sp(3, 1), sp(3, 2), sp(5, 1)];
        let saved_h2_spans = h2.positive_spans.clone();
        h2.positive_buckets = vec![7.0, 4.0, 3.0, 5.0, 2.0];

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(!backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(ok);
        assert!(!cr);

        assert_no_new_float_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
        );

        // Check that h2 was recoded.
        assert_eq!(vec![7.0, 0.0, 4.0, 3.0, 5.0, 0.0, 2.0], h2.positive_buckets);
        assert_eq!(empty_bucket_h.positive_spans, h2.positive_spans);
        assert_ne!(saved_h2_spans, h2.positive_spans, "recoding must make a copy");
    }

    {
        // New histogram that has new buckets AND buckets missing but the buckets missing were empty.
        let mut empty_bucket_h = eh.copy();
        empty_bucket_h.positive_buckets = vec![6.0, 0.0, 3.0, 2.0, 4.0, 0.0, 1.0];
        let (mut c, mut h_app, ts, h1) = setup(&empty_bucket_h);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(0, 1), sp(3, 1), sp(3, 2), sp(5, 2)];
        let saved_h2_spans = h2.positive_spans.clone();
        h2.positive_buckets = vec![7.0, 4.0, 3.0, 5.0, 2.0, 3.0];

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(!pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(!backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(ok);
        assert!(!cr);

        assert_recoded_float_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
        );

        // Check that h2 was recoded.
        assert_eq!(vec![7.0, 0.0, 4.0, 3.0, 5.0, 0.0, 2.0, 3.0], h2.positive_buckets);
        assert_eq!(
            vec![
                sp(0, 2), // Added empty bucket.
                sp(2, 1), // Existing - offset adjusted.
                sp(3, 2), // Existing.
                sp(3, 1), // Added empty bucket.
                sp(1, 2), // Existing + the extra bucket.
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
        h2.positive_buckets = vec![6.0, 2.0, 3.0, 2.0, 4.0, 5.0, 1.0];

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(!ok); // Need to cut a new chunk.
        assert!(cr);

        assert_new_float_histogram_chunk_on_append(
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
        h2.positive_buckets = vec![7.0, 5.0, 1.0, 3.0, 1.0, 0.0, 2.0, 5.0, 5.0, 0.0, 0.0];

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(!ok); // Need to cut a new chunk.
        assert!(cr);

        assert_new_float_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::COUNTER_RESET,
            CounterResetHint::UnknownCounterReset,
        );
    }

    {
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        // New histogram that has a counter reset while new buckets were
        // added before the first bucket and reset on first bucket.  (to
        // catch the edge case where the new bucket should be forwarded
        // ahead until first old bucket at start)
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(-3, 2), sp(1, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)];
        h2.sum = 26.0;
        h2.positive_buckets = vec![1.0, 2.0, 5.0, 3.0, 3.0, 2.0, 4.0, 5.0, 1.0];

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(!ok); // Need to cut a new chunk.
        assert!(cr);

        assert_new_float_histogram_chunk_on_append(
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

        assert_new_float_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::COUNTER_RESET,
            CounterResetHint::UnknownCounterReset,
        );
    }

    {
        // Start new chunk explicitly, and append a new histogram that is considered appendable to the previous chunk.
        let (_, h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy(); // Identity is appendable.

        let mut next_chunk = FloatHistogramChunk::new();
        let mut app = next_chunk.appender().unwrap();
        let (new_chunk, recoded) =
            app.append_float_histogram(&mut next_chunk, Some(&h_app), 0, ts + 1, &mut h2, false).unwrap();
        assert!(new_chunk.is_none());
        assert!(!recoded);
        // adapted: Go checks that the returned appender is `app`; in Rust no new chunk means `app` continues.
        assert_sample_count(&next_chunk, 1, ValueType::FloatHistogram);
        assert_eq!(CounterResetHeader::NOT_COUNTER_RESET, next_chunk.get_counter_reset_header());
        assert_first_float_histogram_sample_hint(&next_chunk, CounterResetHint::UnknownCounterReset);
    }

    {
        // Start new chunk explicitly, and append a new histogram that is not considered appendable to the previous chunk.
        let (_, h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.count -= 1.0; // Make this not appendable due to counter reset.

        let mut next_chunk = FloatHistogramChunk::new();
        let mut app = next_chunk.appender().unwrap();
        let (new_chunk, recoded) =
            app.append_float_histogram(&mut next_chunk, Some(&h_app), 0, ts + 1, &mut h2, false).unwrap();
        assert!(new_chunk.is_none());
        assert!(!recoded);
        // adapted: Go checks that the returned appender is `app`; in Rust no new chunk means `app` continues.
        assert_sample_count(&next_chunk, 1, ValueType::FloatHistogram);
        assert_eq!(CounterResetHeader::COUNTER_RESET, next_chunk.get_counter_reset_header());
        assert_first_float_histogram_sample_hint(&next_chunk, CounterResetHint::UnknownCounterReset);
    }

    {
        // Start new chunk explicitly, and append a new histogram that would need recoding if we added it to the chunk.
        let (_, h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(0, 3), sp(1, 1), sp(1, 4), sp(3, 3)];
        h2.count += 9.0;
        h2.zero_count += 1.0;
        h2.sum = 30.0;
        h2.positive_buckets = vec![7.0, 5.0, 1.0, 3.0, 1.0, 0.0, 2.0, 5.0, 5.0, 0.0, 1.0];

        let mut next_chunk = FloatHistogramChunk::new();
        let mut app = next_chunk.appender().unwrap();
        let (new_chunk, recoded) =
            app.append_float_histogram(&mut next_chunk, Some(&h_app), 0, ts + 1, &mut h2, false).unwrap();
        assert!(new_chunk.is_none());
        assert!(!recoded);
        // adapted: Go checks that the returned appender is `app`; in Rust no new chunk means `app` continues.
        assert_sample_count(&next_chunk, 1, ValueType::FloatHistogram);
        assert_eq!(CounterResetHeader::NOT_COUNTER_RESET, next_chunk.get_counter_reset_header());
        assert_first_float_histogram_sample_hint(&next_chunk, CounterResetHint::UnknownCounterReset);
    }

    {
        // Start a new chunk with a histogram that has an empty bucket.
        // Add a histogram that has the same bucket missing.
        // This should be appendable and can happen if we are merging from chunks
        // where the first sample came from a recoded chunk that added the
        // empty bucket.
        let mut h1 = eh.copy();
        // Add a bucket that is empty -10 offsets from the first bucket.
        h1.positive_spans = vec![Span::default(); eh.positive_spans.len() + 1];
        h1.positive_spans[0] = sp(eh.positive_spans[0].offset - 10, 1);
        h1.positive_spans[1] = sp(eh.positive_spans[0].offset + 9, eh.positive_spans[0].length);
        for (i, v) in eh.positive_spans[1..].iter().enumerate() {
            h1.positive_spans[i + 2] = *v;
        }
        h1.positive_buckets = vec![0.0; eh.positive_buckets.len() + 1];
        h1.positive_buckets[0] = 0.0;
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
        assert!(!cr);

        assert_no_new_float_histogram_chunk_on_append(
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

        assert_no_new_float_histogram_chunk_on_append(
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
        h2.count += 1.0;
        h2.positive_buckets = vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 2.0];
        let (_, _, _, _, ok, _) = h_app.appendable(&h2);
        assert!(ok);

        assert_no_new_float_histogram_chunk_on_append(
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
        h2.count -= 1.0;
        h2.positive_buckets = vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 0.0];
        let (_, _, _, _, ok, _) = h_app.appendable(&h2);
        assert!(!ok);

        assert_new_float_histogram_chunk_on_append(
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

        assert_new_float_histogram_chunk_on_append(
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
        h2.count += 6.0;
        h2.sum = 30.0;
        // Existing histogram should get values converted from the above to:
        //   6 3 0 3 0 0 2 4 5 0 1 (previous values with some new empty buckets in between)
        // so the new histogram should have new counts >= these per-bucket counts, e.g.:
        h2.positive_buckets = vec![7.0, 5.0, 1.0, 3.0, 1.0, 0.0, 2.0, 5.0, 5.0, 0.0, 1.0]; // (total 30)

        let (pos_interjections, neg_interjections, backward_positive_inserts, backward_negative_inserts, ok, cr) =
            h_app.appendable(&h2);
        assert!(!pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(backward_positive_inserts.is_empty());
        assert!(backward_negative_inserts.is_empty());
        assert!(ok); // Only new buckets came in.
        assert!(!cr);

        assert_recoded_float_histogram_chunk_on_append(
            &mut c,
            &mut h_app,
            ts + 1,
            &mut h2,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
        );
    }
}

/// Go `assertNewFloatHistogramChunkOnAppend`.
fn assert_new_float_histogram_chunk_on_append(
    old_chunk: &mut FloatHistogramChunk,
    h_app: &mut FloatHistogramAppender,
    ts: i64,
    h: &mut FloatHistogram,
    expect_header: CounterResetHeader,
    expect_hint: CounterResetHint,
) {
    let old_chunk_bytes = old_chunk.bytes().to_vec();
    let res = h_app.append_float_histogram(old_chunk, None, 0, ts, h, false);
    assert_eq!(old_chunk_bytes, old_chunk.bytes()); // Sanity check that previous chunk is untouched.
    let (new_chunk, recoded) = res.expect("append");
    let (new_chunk, new_appender) = new_chunk.expect("new chunk");
    assert!(!recoded);
    assert_ne!(*old_chunk, new_chunk);
    assert_eq!(expect_header, new_chunk.get_counter_reset_header());
    assert_ne!(*h_app, new_appender);
    assert_sample_count(&new_chunk, 1, ValueType::FloatHistogram);
    assert_first_float_histogram_sample_hint(&new_chunk, expect_hint);
}

/// Go `assertNoNewFloatHistogramChunkOnAppend`.
fn assert_no_new_float_histogram_chunk_on_append(
    old_chunk: &mut FloatHistogramChunk,
    h_app: &mut FloatHistogramAppender,
    ts: i64,
    h: &mut FloatHistogram,
    expect_header: CounterResetHeader,
) {
    let old_chunk_len = old_chunk.bytes().len();
    let res = h_app.append_float_histogram(old_chunk, None, 0, ts, h, false);
    assert!(old_chunk.bytes().len() > old_chunk_len); // Check that current chunk is bigger than previously.
    let (new_chunk, recoded) = res.expect("append");
    assert!(new_chunk.is_none());
    assert!(!recoded);
    assert_eq!(expect_header, old_chunk.get_counter_reset_header());
    // adapted: Go checks that the returned appender is `hApp`; in Rust no new chunk means `hApp` continues.
    assert_sample_count(old_chunk, 2, ValueType::FloatHistogram);
}

/// Go `assertRecodedFloatHistogramChunkOnAppend`.
fn assert_recoded_float_histogram_chunk_on_append(
    prev_chunk: &mut FloatHistogramChunk,
    h_app: &mut FloatHistogramAppender,
    ts: i64,
    h: &mut FloatHistogram,
    expect_header: CounterResetHeader,
) {
    let prev_chunk_bytes = prev_chunk.bytes().to_vec();
    let res = h_app.append_float_histogram(prev_chunk, None, 0, ts, h, false);
    // Sanity check that previous chunk is untouched. This may change in the future if we implement in-place recoding.
    assert_eq!(prev_chunk_bytes, prev_chunk.bytes());
    let (new_chunk, recoded) = res.expect("append");
    let (new_chunk, new_appender) = new_chunk.expect("new chunk");
    assert!(recoded);
    assert_ne!(*prev_chunk, new_chunk);
    assert_eq!(expect_header, new_chunk.get_counter_reset_header());
    assert_ne!(*h_app, new_appender);
    assert_sample_count(&new_chunk, 2, ValueType::FloatHistogram);
}

#[test]
fn test_float_histogram_chunk_appendable_with_empty_span() {
    let tests: Vec<(&str, FloatHistogram, FloatHistogram)> = vec![
        (
            "empty span in old and new histogram",
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
                schema: 0,
                count: 37.0,
                sum: 2345.6,
                zero_threshold: 0.001,
                zero_count: 5.0,
                positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
                positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 1.0],
                negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
                negative_buckets: vec![1.0, 4.0, 2.0, 7.0, 5.0, 5.0, 2.0],
                ..Default::default()
            },
        ),
        (
            "empty span in old histogram",
            FloatHistogram {
                schema: 0,
                count: 21.0,
                sum: 1234.5,
                zero_threshold: 0.001,
                zero_count: 4.0,
                positive_spans: vec![
                    sp(1, 0), // This span will disappear.
                    sp(2, 4),
                    sp(0, 3),
                ],
                positive_buckets: vec![1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
                negative_buckets: vec![1.0, 2.0, 1.0, 2.0, 2.0, 2.0, 2.0],
                ..Default::default()
            },
            FloatHistogram {
                schema: 0,
                count: 37.0,
                sum: 2345.6,
                zero_threshold: 0.001,
                zero_count: 5.0,
                positive_spans: vec![sp(3, 4), sp(0, 3)],
                positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 1.0],
                negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
                negative_buckets: vec![1.0, 4.0, 2.0, 7.0, 5.0, 5.0, 2.0],
                ..Default::default()
            },
        ),
        (
            "empty span in new histogram",
            FloatHistogram {
                schema: 0,
                count: 21.0,
                sum: 1234.5,
                zero_threshold: 0.001,
                zero_count: 4.0,
                positive_spans: vec![sp(0, 4), sp(3, 3)],
                positive_buckets: vec![1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
                negative_buckets: vec![1.0, 2.0, 1.0, 2.0, 2.0, 2.0, 2.0],
                ..Default::default()
            },
            FloatHistogram {
                schema: 0,
                count: 37.0,
                sum: 2345.6,
                zero_threshold: 0.001,
                zero_count: 5.0,
                positive_spans: vec![
                    sp(0, 4),
                    sp(1, 0), // This span is new.
                    sp(2, 3),
                ],
                positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 1.0],
                negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
                negative_buckets: vec![1.0, 4.0, 2.0, 7.0, 5.0, 5.0, 2.0],
                ..Default::default()
            },
        ),
        (
            "two empty spans mixing offsets",
            FloatHistogram {
                schema: 0,
                count: 21.0,
                sum: 1234.5,
                zero_threshold: 0.001,
                zero_count: 4.0,
                positive_spans: vec![sp(0, 4), sp(1, 0), sp(3, 0), sp(4, 3)],
                positive_buckets: vec![1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
                negative_buckets: vec![1.0, 2.0, 1.0, 2.0, 2.0, 2.0, 2.0],
                ..Default::default()
            },
            FloatHistogram {
                schema: 0,
                count: 37.0,
                sum: 2345.6,
                zero_threshold: 0.001,
                zero_count: 5.0,
                positive_spans: vec![sp(0, 4), sp(3, 0), sp(1, 0), sp(4, 3)],
                positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 1.0],
                negative_spans: vec![sp(1, 4), sp(2, 0), sp(2, 3)],
                negative_buckets: vec![1.0, 4.0, 2.0, 7.0, 5.0, 5.0, 2.0],
                ..Default::default()
            },
        ),
        (
            "empty span in old and new custom buckets histogram",
            FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 7.0,
                sum: 1234.5,
                positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
                positive_buckets: vec![1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                ..Default::default()
            },
            FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 10.0,
                sum: 2345.6,
                positive_spans: vec![sp(0, 4), sp(0, 0), sp(0, 3)],
                positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 1.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                ..Default::default()
            },
        ),
    ];

    for (name, h1, h2) in tests {
        let mut c = FloatHistogramChunk::new();

        // Create fresh appender and add the first histogram.
        let mut app = c.appender().unwrap();
        assert_eq!(0, c.num_samples(), "{name}");

        let mut h1 = h1;
        app.append_float_histogram(&mut c, None, 0, 1, &mut h1, true).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(1, c.num_samples(), "{name}");
        let h_app = &app;

        let (p_i, n_i, bp_i, bn_i, ok_to_append, counter_reset) = h_app.appendable(&h2);
        assert!(p_i.is_empty(), "{name}");
        assert!(n_i.is_empty(), "{name}");
        assert!(bp_i.is_empty(), "{name}");
        assert!(bn_i.is_empty(), "{name}");
        assert!(ok_to_append, "{name}");
        assert!(!counter_reset, "{name}");
    }
}

#[test]
fn test_float_histogram_chunk_appendable_gauge() {
    let eh = FloatHistogram {
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

    let cbh = FloatHistogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        count: 24.0,
        sum: 18.4,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![sp(0, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)],
        positive_buckets: vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 1.0],
        custom_values: custom_values(&[
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0,
            20.0,
        ]),
        ..Default::default()
    };

    let setup = |h: &FloatHistogram| setup(h, CounterResetHeader::GAUGE_TYPE);

    {
        // Schema change.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.schema += 1;
        let (_, _, _, _, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(!ok);

        assert_new_float_histogram_chunk_on_append(
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

        assert_new_float_histogram_chunk_on_append(
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
        h2.count += 9.0;
        h2.zero_count += 1.0;
        h2.sum = 30.0;
        h2.positive_buckets = vec![7.0, 5.0, 1.0, 3.0, 1.0, 0.0, 2.0, 5.0, 5.0, 0.0, 1.0];

        let (p_i, n_i, p_backward_i, n_backward_i, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(!p_i.is_empty());
        assert!(n_i.is_empty());
        assert!(p_backward_i.is_empty());
        assert!(n_backward_i.is_empty());
        assert!(ok);

        assert_recoded_float_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // New histogram that has buckets missing.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(0, 2), sp(2, 1), sp(3, 1), sp(4, 1), sp(1, 1)];
        h2.count -= 4.0;
        h2.sum -= 1.0;
        h2.positive_buckets = vec![6.0, 3.0, 3.0, 2.0, 5.0, 1.0];

        let (p_i, n_i, p_backward_i, n_backward_i, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(p_i.is_empty());
        assert!(n_i.is_empty());
        assert!(!p_backward_i.is_empty());
        assert!(n_backward_i.is_empty());
        assert!(ok);

        assert_no_new_float_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // New histogram that has a bucket missing and new buckets.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(0, 2), sp(5, 2), sp(3, 1), sp(1, 1)];
        h2.sum = 21.0;
        h2.positive_buckets = vec![6.0, 3.0, 2.0, 4.0, 5.0, 1.0];

        let (p_i, n_i, p_backward_i, n_backward_i, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(!p_i.is_empty());
        assert!(!p_backward_i.is_empty());
        assert!(n_i.is_empty());
        assert!(n_backward_i.is_empty());
        assert!(ok);

        assert_recoded_float_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // New histogram that has a counter reset while buckets are same.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.sum = 23.0;
        h2.positive_buckets = vec![6.0, 2.0, 3.0, 2.0, 4.0, 5.0, 1.0];

        let (p_i, n_i, p_backward_i, n_backward_i, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(p_i.is_empty());
        assert!(n_i.is_empty());
        assert!(p_backward_i.is_empty());
        assert!(n_backward_i.is_empty());
        assert!(ok);

        assert_no_new_float_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // New histogram that has a counter reset while new buckets were added.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(0, 3), sp(1, 1), sp(1, 4), sp(3, 3)];
        h2.sum = 29.0;
        h2.positive_buckets = vec![7.0, 5.0, 1.0, 3.0, 1.0, 0.0, 2.0, 5.0, 5.0, 0.0, 0.0];

        let (p_i, n_i, p_backward_i, n_backward_i, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(!p_i.is_empty());
        assert!(n_i.is_empty());
        assert!(p_backward_i.is_empty());
        assert!(n_backward_i.is_empty());
        assert!(ok);

        assert_recoded_float_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // New histogram that has a counter reset while new buckets were
        // added before the first bucket and reset on first bucket.
        let (mut c, mut h_app, ts, h1) = setup(&eh);
        let mut h2 = h1.copy();
        h2.positive_spans = vec![sp(-3, 2), sp(1, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)];
        h2.sum = 26.0;
        h2.positive_buckets = vec![1.0, 2.0, 5.0, 3.0, 3.0, 2.0, 4.0, 5.0, 1.0];

        let (p_i, n_i, p_backward_i, n_backward_i, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(!p_i.is_empty());
        assert!(n_i.is_empty());
        assert!(p_backward_i.is_empty());
        assert!(n_backward_i.is_empty());
        assert!(ok);

        assert_recoded_float_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // Custom buckets, no change.
        let (mut c, mut h_app, ts, h1) = setup(&cbh);
        let mut h2 = h1.copy();
        let (_, _, _, _, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(ok);

        assert_no_new_float_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // Custom buckets, increase in bucket counts but no change in layout.
        let (mut c, mut h_app, ts, h1) = setup(&cbh);
        let mut h2 = h1.copy();
        h2.count += 1.0;
        h2.positive_buckets = vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 2.0];
        let (_, _, _, _, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(ok);

        assert_no_new_float_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }

    {
        // Custom buckets, decrease in bucket counts but no change in layout.
        let (mut c, mut h_app, ts, h1) = setup(&cbh);
        let mut h2 = h1.copy();
        h2.count -= 1.0;
        h2.positive_buckets = vec![6.0, 3.0, 3.0, 2.0, 4.0, 5.0, 0.0];
        let (_, _, _, _, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(ok);

        assert_no_new_float_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
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

        assert_new_float_histogram_chunk_on_append(
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
        h2.count += 6.0;
        h2.sum = 30.0;
        // Existing histogram should get values converted from the above to:
        //   6 3 0 3 0 0 2 4 5 0 1 (previous values with some new empty buckets in between)
        // so the new histogram should have new counts >= these per-bucket counts, e.g.:
        h2.positive_buckets = vec![7.0, 5.0, 1.0, 3.0, 1.0, 0.0, 2.0, 5.0, 5.0, 0.0, 1.0]; // (total 30)

        let (pos_interjections, neg_interjections, p_backward_i, n_backward_i, _, _, ok) = h_app.appendable_gauge(&h2);
        assert!(!pos_interjections.is_empty());
        assert!(neg_interjections.is_empty());
        assert!(p_backward_i.is_empty());
        assert!(n_backward_i.is_empty());
        assert!(ok); // Only new buckets came in.

        assert_recoded_float_histogram_chunk_on_append(&mut c, &mut h_app, ts + 1, &mut h2, CounterResetHeader::GAUGE_TYPE);
    }
}

#[test]
fn test_float_histogram_append_only_errors() {
    // adapted: on error Go also returns a nil chunk and recoded=false; the Rust Err carries neither, so those
    // checks hold by the type.
    {
        // "schema change error"
        let mut c = FloatHistogramChunk::new();

        // Create fresh appender and add the first histogram.
        let mut app = c.appender().unwrap();

        let mut h = generate_test_float_histogram(0);
        let (new_c, is_recoded) = app.append_float_histogram(&mut c, None, 0, 1, &mut h, true).unwrap();
        assert!(new_c.is_none());
        assert!(!is_recoded);

        // Add erroring histogram.
        let mut h2 = h.copy();
        h2.schema += 1;
        let err = app.append_float_histogram(&mut c, None, 0, 2, &mut h2, true).unwrap_err();
        assert_eq!("float histogram schema change", err.to_string());
    }
    {
        // "counter reset error"
        let mut c = FloatHistogramChunk::new();

        // Create fresh appender and add the first histogram.
        let mut app = c.appender().unwrap();

        let mut h = generate_test_float_histogram(0);
        let (new_c, is_recoded) = app.append_float_histogram(&mut c, None, 0, 1, &mut h, true).unwrap();
        assert!(new_c.is_none());
        assert!(!is_recoded);

        // Add erroring histogram.
        let mut h2 = h.copy();
        h2.counter_reset_hint = CounterResetHint::CounterReset;
        let err = app.append_float_histogram(&mut c, None, 0, 2, &mut h2, true).unwrap_err();
        assert_eq!("float histogram counter reset", err.to_string());
    }
    {
        // "counter reset error with custom buckets"
        let mut c = FloatHistogramChunk::new();

        // Create fresh appender and add the first histogram.
        let mut app = c.appender().unwrap();

        let mut h = generate_test_custom_buckets_float_histogram(0);
        let (new_c, is_recoded) = app.append_float_histogram(&mut c, None, 0, 1, &mut h, true).unwrap();
        assert!(new_c.is_none());
        assert!(!is_recoded);

        // Add erroring histogram.
        let mut h2 = h.copy();
        h2.custom_values = custom_values(&[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]);
        let err = app.append_float_histogram(&mut c, None, 0, 2, &mut h2, true).unwrap_err();
        assert_eq!("float histogram counter reset", err.to_string());
    }
}

#[test]
fn test_float_histogram_unique_spans_after_next() {
    // Create two histograms with the same schema and spans.
    let mut h1 = FloatHistogram {
        schema: 1,
        zero_threshold: 1e-100,
        count: 10.0,
        zero_count: 2.0,
        sum: 15.0,
        positive_spans: vec![sp(0, 2), sp(1, 2)],
        positive_buckets: vec![1.0, 2.0, 3.0, 4.0],
        negative_spans: vec![sp(1, 1)],
        negative_buckets: vec![2.0],
        ..Default::default()
    };

    let mut h2 = h1.copy();

    // Create a chunk and append both histograms.
    let mut c = FloatHistogramChunk::new();
    let mut app = c.appender().unwrap();

    app.append_float_histogram(&mut c, None, 0, 0, &mut h1, false).unwrap();
    app.append_float_histogram(&mut c, None, 0, 1, &mut h2, false).unwrap();

    // Create an iterator and advance to the first histogram.
    let mut it = c.iterator();
    assert_eq!(ValueType::FloatHistogram, it.next());
    let (_, rh1) = it.at_float_histogram(None);

    // Advance to the second histogram and retrieve it.
    assert_eq!(ValueType::FloatHistogram, it.next());
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

#[test]
fn test_float_histogram_unique_custom_values_after_next() {
    // Create two histograms with the same schema and custom values.
    let mut h1 = FloatHistogram {
        schema: -53,
        count: 10.0,
        sum: 15.0,
        positive_spans: vec![sp(0, 2), sp(1, 2)],
        positive_buckets: vec![1.0, 2.0, 3.0, 4.0],
        custom_values: custom_values(&[10.0, 11.0, 12.0, 13.0]),
        ..Default::default()
    };

    let mut h2 = h1.copy();

    // Create a chunk and append both histograms.
    let mut c = FloatHistogramChunk::new();
    let mut app = c.appender().unwrap();

    app.append_float_histogram(&mut c, None, 0, 0, &mut h1, false).unwrap();
    app.append_float_histogram(&mut c, None, 0, 1, &mut h2, false).unwrap();

    // Create an iterator and advance to the first histogram.
    let mut it = c.iterator();
    assert_eq!(ValueType::FloatHistogram, it.next());
    let (_, mut rh1) = it.at_float_histogram(None);

    // Advance to the second histogram and retrieve it.
    assert_eq!(ValueType::FloatHistogram, it.next());
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
    // adapted: custom values are an immutable interned `Arc<[f64]>` in Rust, so two histograms may share them by
    // design. Go's check guards against a write through one histogram showing up in the other; check that the
    // shared values cannot be written in place and that replacing them in one histogram leaves the other unchanged.
    let cv1 = rh1.custom_values.as_mut().expect("custom values");
    assert!(
        std::sync::Arc::get_mut(cv1).is_none() || !std::ptr::eq(&cv1[0], &rh2.custom_values.as_ref().unwrap()[0]),
        "CustomValues should be unique between histograms"
    );
    rh1.custom_values = custom_values(&[0.0, 11.0, 12.0, 13.0]);
    assert_eq!(rh2.custom_values, h1.custom_values, "CustomValues should be unique between histograms");
}

#[test]
fn test_float_histogram_empty_buckets_with_gaps() {
    let mut h1 = FloatHistogram {
        positive_spans: vec![sp(-19, 2), sp(1, 2)],
        positive_buckets: vec![0.0, 0.0, 0.0, 0.0],
        ..Default::default()
    };
    h1.validate().unwrap();

    let mut c = FloatHistogramChunk::new();
    let mut app = c.appender().unwrap();
    app.append_float_histogram(&mut c, None, 0, 1, &mut h1, false).unwrap();

    let mut h2 = FloatHistogram {
        positive_spans: vec![sp(-19, 1), sp(4, 1), sp(3, 1)],
        positive_buckets: vec![0.0, 0.0, 0.0],
        ..Default::default()
    };
    h2.validate().unwrap();

    let (new_c, recoded) = app.append_float_histogram(&mut c, None, 0, 2, &mut h2, false).unwrap();
    assert!(recoded);
    let (new_c, _) = new_c.expect("new chunk");

    let mut it = new_c.iterator();
    assert_eq!(ValueType::FloatHistogram, it.next());
    let (_, h) = it.at_float_histogram(None);
    h.validate().unwrap();
    assert_eq!(ValueType::FloatHistogram, it.next());
    let (_, h) = it.at_float_histogram(None);
    h.validate().unwrap();
    assert_eq!(ValueType::None, it.next());
    assert!(it.err().is_none(), "{:?}", it.err());
}

#[test]
fn test_float_histogram_iterator_fail_if_schema_in_valid() {
    for schema in [-101, 101] {
        let mut h = FloatHistogram {
            schema,
            count: 10.0,
            sum: 15.0,
            zero_threshold: 1e-100,
            positive_spans: vec![sp(0, 2), sp(1, 2)],
            positive_buckets: vec![1.0, 2.0, 3.0, 4.0],
            ..Default::default()
        };

        let mut c = FloatHistogramChunk::new();
        let mut app = c.appender().unwrap();

        app.append_float_histogram(&mut c, None, 0, 1, &mut h, false).unwrap();

        let mut it = c.iterator();
        assert_eq!(ValueType::None, it.next(), "schema {schema}");
        let err = it.err().unwrap_or_else(|| panic!("schema {schema}: no error"));
        assert!(err.is_histogram(ErrorKind::HistogramsUnknownSchema), "schema {schema}: {err}");
    }
}

#[test]
fn test_float_histogram_iterator_reduce_schema() {
    for schema in [9, 52] {
        let mut h = FloatHistogram {
            schema,
            count: 10.0,
            sum: 15.0,
            zero_threshold: 1e-100,
            positive_spans: vec![sp(0, 2), sp(1, 2)],
            positive_buckets: vec![1.0, 2.0, 3.0, 4.0],
            ..Default::default()
        };

        let mut c = FloatHistogramChunk::new();
        let mut app = c.appender().unwrap();

        app.append_float_histogram(&mut c, None, 0, 1, &mut h, false).unwrap();

        let mut it = c.iterator();
        assert_eq!(ValueType::FloatHistogram, it.next(), "schema {schema}");
        let (_, rh) = it.at_float_histogram(None);
        assert_eq!(EXPONENTIAL_SCHEMA_MAX, rh.schema, "schema {schema}");
    }
}

#[test]
fn test_float_histogram_chunk_over_flow_panics() {
    common::test_chunk_over_flow_panics(ENC_FLOAT_HISTOGRAM, ValueType::FloatHistogram);
}
