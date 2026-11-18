//! Port of `scrape/st_synthesis_test.go`.
//!
//! The Rust synthesis functions return `None` for "the input, unchanged" (Go returns the same pointer); `pick`
//! turns that back into the value Go's tests compare.

use model::histogram::{FloatHistogram, Histogram, Span};
use scrape::st_synthesis::StCache;

fn pick_fh(r: Option<FloatHistogram>, input: &FloatHistogram) -> FloatHistogram {
    r.unwrap_or_else(|| input.clone())
}

fn pick_h(r: Option<Histogram>, input: &Histogram) -> Histogram {
    r.unwrap_or_else(|| input.clone())
}

#[test]
fn test_synthesize_float_valid_counter() {
    let mut st = StCache::default();

    // Scrape loop anchors first sample.
    let (v, ct, skip) = st.synthesize_float(10.0, 1000);
    assert_eq!(10.0, v);
    assert_eq!(1000i64, ct);
    assert!(skip);
    assert_eq!(10.0, st.f.as_ref().unwrap().prev);
    assert_eq!(10.0, st.f.as_ref().unwrap().starting);

    // Second scrape, no reset.
    let (v, ct, skip) = st.synthesize_float(15.0, 2000);
    assert_eq!(5.0, v);
    assert_eq!(15.0, st.f.as_ref().unwrap().prev);
    assert_eq!(1000i64, ct);
    assert!(!skip);

    // Third scrape, no reset.
    let (v, ct, skip) = st.synthesize_float(20.0, 3000);
    assert_eq!(10.0, v);
    assert_eq!(20.0, st.f.as_ref().unwrap().prev);
    assert_eq!(1000i64, ct);
    assert!(!skip);
}

#[test]
fn test_synthesize_float_counter_reset() {
    let mut st = StCache::default();

    // Scrape loop anchors first sample.
    let (v, ct, skip) = st.synthesize_float(100.0, 1000);
    assert_eq!(100.0, v);
    assert_eq!(1000i64, ct);
    assert!(skip);
    assert_eq!(100.0, st.f.as_ref().unwrap().prev);
    assert_eq!(100.0, st.f.as_ref().unwrap().starting);

    // First reset (value goes down).
    let (v, ct, skip) = st.synthesize_float(5.0, 2000);
    assert_eq!(5.0, v);
    assert_eq!(1999i64, ct);
    assert!(!skip);
    assert_eq!(5.0, st.f.as_ref().unwrap().prev);
    assert_eq!(0.0, st.f.as_ref().unwrap().starting);

    // Increment.
    let (v, ct, skip) = st.synthesize_float(15.0, 3000);
    assert_eq!(15.0, v);
    assert_eq!(1999i64, ct);
    assert!(!skip);
}

#[test]
fn test_synthesize_float_histogram_valid_and_reset() {
    let mut st = StCache::default();

    let fh1 = FloatHistogram { count: 10.0, sum: 50.5, zero_count: 2.0, ..Default::default() };

    // Scrape loop anchors first sample.
    let (v, ct, skip) = st.synthesize_float_histogram(&fh1, 1000);
    assert_eq!(fh1, pick_fh(v, &fh1));
    assert_eq!(1000i64, ct);
    assert!(skip);
    assert!(st.h.is_some());
    assert!(st.h.as_ref().unwrap().prev.is_some());
    assert!(st.h.as_ref().unwrap().starting.is_some());

    // Next scrape.
    let fh2 = FloatHistogram { count: 25.0, sum: 120.0, zero_count: 5.0, ..Default::default() };
    let (v, ct, skip) = st.synthesize_float_histogram(&fh2, 2000);
    let v = pick_fh(v, &fh2);
    assert_eq!(15.0, v.count);
    assert_eq!(69.5, v.sum);
    assert_eq!(3.0, v.zero_count);
    assert_eq!(1000i64, ct);
    assert!(!skip);

    // Reset scrape (values go down).
    let fh3 = FloatHistogram { count: 5.0, sum: 12.0, zero_count: 1.0, ..Default::default() };
    let (v, ct, skip) = st.synthesize_float_histogram(&fh3, 3000);
    let v = pick_fh(v, &fh3);
    assert_eq!(5.0, v.count);
    assert_eq!(12.0, v.sum);
    assert_eq!(1.0, v.zero_count);
    assert_eq!(2999i64, ct);
    assert!(!skip);
    assert!(st.h.as_ref().unwrap().starting.is_none());

    // Next scrape.
    let fh4 = FloatHistogram { count: 25.0, sum: 120.0, zero_count: 5.0, ..Default::default() };
    let (v, ct, skip) = st.synthesize_float_histogram(&fh4, 4000);
    let v = pick_fh(v, &fh4);
    assert_eq!(25.0, v.count);
    assert_eq!(120.0, v.sum);
    assert_eq!(5.0, v.zero_count);
    assert_eq!(2999i64, ct);
    assert!(!skip);
}

#[test]
fn test_synthesize_histogram_valid_and_reset() {
    let mut st = StCache::default();

    let h1 = Histogram { count: 10, sum: 50.5, zero_count: 2, ..Default::default() };

    // Scrape loop anchors first sample.
    let (v, ct, skip) = st.synthesize_histogram(&h1, 1000);
    assert_eq!(h1, pick_h(v, &h1));
    assert_eq!(1000i64, ct);
    assert!(skip);
    assert!(st.h.is_some());

    // Next scrape.
    let h2 = Histogram { count: 25, sum: 120.0, zero_count: 5, ..Default::default() };
    let (v, ct, skip) = st.synthesize_histogram(&h2, 2000);
    let v = pick_h(v, &h2);
    assert_eq!(15u64, v.count);
    assert_eq!(69.5, v.sum);
    assert_eq!(3u64, v.zero_count);
    assert_eq!(1000i64, ct);
    assert!(!skip);

    // Reset scrape (values go down).
    let h3 = Histogram { count: 5, sum: 12.0, zero_count: 1, ..Default::default() };
    let (v, ct, skip) = st.synthesize_histogram(&h3, 3000);
    let v = pick_h(v, &h3);
    assert_eq!(5u64, v.count);
    assert_eq!(12.0, v.sum);
    assert_eq!(1u64, v.zero_count);
    assert_eq!(2999i64, ct);
    assert!(!skip);
    assert!(st.h.as_ref().unwrap().starting.is_none());

    // Next scrape.
    let h4 = Histogram { count: 25, sum: 120.0, zero_count: 5, ..Default::default() };
    let (v, ct, skip) = st.synthesize_histogram(&h4, 4000);
    let v = pick_h(v, &h4);
    assert_eq!(25u64, v.count);
    assert_eq!(120.0, v.sum);
    assert_eq!(5u64, v.zero_count);
    assert_eq!(2999i64, ct);
    assert!(!skip);
}

#[test]
fn test_synthesize_float_histogram_subtraction_mapping() {
    let mut st = StCache::default();

    // Create an anchor.
    let fh1 = FloatHistogram {
        count: 10.0,
        sum: 50.5,
        zero_count: 2.0,
        positive_spans: vec![Span { offset: 0, length: 2 }], // Buckets at index 0, 1
        positive_buckets: vec![3.0, 5.0],                     // total 8
        ..Default::default()
    };

    // Scrape loop anchors first sample.
    let (v, ct, skip) = st.synthesize_float_histogram(&fh1, 1000);
    assert_eq!(fh1, pick_fh(v, &fh1));
    assert_eq!(1000i64, ct);
    assert!(skip);

    // Next scrape has a NEW span layout because a previous 0 bucket got populated.
    // We expect NO reset, just proper subtraction.
    let fh2 = FloatHistogram {
        count: 25.0,
        sum: 120.0,
        zero_count: 3.0,
        positive_spans: vec![Span { offset: 0, length: 3 }], // Buckets at index 0, 1, 2
        positive_buckets: vec![4.0, 7.0, 6.0],                // The previous 0 bucket is now index 2 with 6.0 counts!
        ..Default::default()
    };

    // Result: [4-3, 7-5, 6-0] -> [1.0, 2.0, 6.0]
    let (v, ct, skip) = st.synthesize_float_histogram(&fh2, 2000);
    let v = pick_fh(v, &fh2);

    assert!(!skip);
    assert_eq!(1000i64, ct);
    assert_eq!(15.0, v.count);
    assert_eq!(69.5, v.sum);
    assert_eq!(1.0, v.zero_count);
    assert_eq!(1, v.positive_spans.len());
    assert_eq!(3u32, v.positive_spans[0].length);
    assert_eq!(vec![1.0, 2.0, 6.0], v.positive_buckets);
}

#[test]
fn test_synthesize_histogram_subtraction_mapping() {
    let mut st = StCache::default();

    // Create an anchor.
    let h1 = Histogram {
        count: 10,
        sum: 50.5,
        zero_count: 2,
        positive_spans: vec![Span { offset: 0, length: 2 }], // Buckets at index 0, 1
        positive_buckets: vec![3, 2],                         // Absolute: 3, 5
        ..Default::default()
    };

    // Scrape loop anchors first sample.
    let (v, ct, skip) = st.synthesize_histogram(&h1, 1000);
    assert_eq!(h1, pick_h(v, &h1));
    assert_eq!(1000i64, ct);
    assert!(skip);

    // Next scrape has a NEW span layout because a previous 0 bucket got populated.
    let h2 = Histogram {
        count: 25,
        sum: 120.0,
        zero_count: 3,
        positive_spans: vec![Span { offset: 0, length: 3 }], // Buckets at index 0, 1, 2
        positive_buckets: vec![4, 3, -1],                     // Absolute: 4, 7, 6
        ..Default::default()
    };

    let (v, ct, skip) = st.synthesize_histogram(&h2, 2000);
    let v = pick_h(v, &h2);

    assert!(!skip);
    assert_eq!(1000i64, ct);
    assert_eq!(15u64, v.count);
    assert_eq!(69.5, v.sum);
    assert_eq!(1u64, v.zero_count);
    assert_eq!(1, v.positive_spans.len());
}

#[test]
fn test_synthesize_float_histogram_bucket_reset() {
    let mut st = StCache::default();

    let fh1 = FloatHistogram {
        count: 10.0,
        sum: 50.5,
        zero_count: 2.0,
        positive_spans: vec![Span { offset: 0, length: 2 }], // Buckets at index 0, 1
        positive_buckets: vec![3.0, 5.0],                     // total 8
        ..Default::default()
    };

    let (_v, _ct, skip) = st.synthesize_float_histogram(&fh1, 1000);
    assert!(skip);

    // Next scrape: total count up to 12. Sum up to 60.0. Zero count stable.
    // BUT Bucket 0 goes from 3.0 down to 2.0. Bucket 1 goes from 5.0 to 8.0.
    let fh2 = FloatHistogram {
        count: 12.0,
        sum: 60.0,
        zero_count: 2.0,
        positive_spans: vec![Span { offset: 0, length: 2 }],
        positive_buckets: vec![2.0, 8.0],
        ..Default::default()
    };

    let (v, ct, skip) = st.synthesize_float_histogram(&fh2, 2000);
    let v = pick_fh(v, &fh2);

    assert!(!skip);
    // Since it resets, ct should be 1999.
    assert_eq!(1999i64, ct);
    // v should equal fh2 exactly since it resets.
    assert_eq!(12.0, v.count);
    assert_eq!(60.0, v.sum);
    assert_eq!(2.0, v.zero_count);
    assert_eq!(vec![2.0, 8.0], v.positive_buckets);
    assert!(st.h.as_ref().unwrap().starting.is_none());
}

#[test]
fn test_synthesize_histogram_bucket_reset() {
    let mut st = StCache::default();

    let h1 = Histogram {
        count: 10,
        sum: 50.5,
        zero_count: 2,
        positive_spans: vec![Span { offset: 0, length: 2 }], // Buckets at index 0, 1
        positive_buckets: vec![3, 2],                         // Absolute: 3, 5
        ..Default::default()
    };

    let (_v, _ct, skip) = st.synthesize_histogram(&h1, 1000);
    assert!(skip);

    let h2 = Histogram {
        count: 12,
        sum: 60.0,
        zero_count: 2,
        positive_spans: vec![Span { offset: 0, length: 2 }],
        positive_buckets: vec![2, 6], // Absolute: 2, 8. Bucket 0 went from 3 to 2 -> Reset!
        ..Default::default()
    };

    let (v, ct, skip) = st.synthesize_histogram(&h2, 2000);
    let v = pick_h(v, &h2);

    assert!(!skip);
    assert_eq!(1999i64, ct);
    assert_eq!(12u64, v.count);
    assert_eq!(60.0, v.sum);
    assert_eq!(vec![2i64, 6], v.positive_buckets);
    assert!(st.h.as_ref().unwrap().starting.is_none());
}
