//! Port of `tsdb/tsdbutil/histogram.go`: test histograms that the storage, TSDB and PromQL tests share.

use model::histogram::{CUSTOM_BUCKETS_SCHEMA, CounterResetHint, FloatHistogram, Histogram, Span, custom_values};

fn spans() -> Vec<Span> {
    vec![Span { offset: 0, length: 2 }, Span { offset: 1, length: 2 }]
}

/// Go `GenerateTestHistograms`: `n` histograms; all but the first are marked as not a counter reset.
pub fn generate_test_histograms(n: usize) -> Vec<Histogram> {
    (0..n)
        .map(|i| {
            let mut h = generate_test_histogram(i as i64);
            if i > 0 {
                h.counter_reset_hint = CounterResetHint::NotCounterReset;
            }
            h
        })
        .collect()
}

/// Go `GenerateTestHistogramWithHint`.
pub fn generate_test_histogram_with_hint(n: i64, hint: CounterResetHint) -> Histogram {
    let mut h = generate_test_histogram(n);
    h.counter_reset_hint = hint;
    h
}

/// Go `GenerateTestHistogram`; the caller sets any known counter reset hint.
pub fn generate_test_histogram(i: i64) -> Histogram {
    Histogram {
        count: (12 + i * 9) as u64,
        zero_count: (2 + i) as u64,
        zero_threshold: 0.001,
        sum: 18.4 * (i + 1) as f64,
        schema: 1,
        positive_spans: spans(),
        positive_buckets: vec![i + 1, 1, -1, 0],
        negative_spans: spans(),
        negative_buckets: vec![i + 1, 1, -1, 0],
        ..Default::default()
    }
}

/// Go `GenerateTestCustomBucketsHistograms`.
pub fn generate_test_custom_buckets_histograms(n: usize) -> Vec<Histogram> {
    (0..n)
        .map(|i| {
            let mut h = generate_test_custom_buckets_histogram(i as i64);
            if i > 0 {
                h.counter_reset_hint = CounterResetHint::NotCounterReset;
            }
            h
        })
        .collect()
}

/// Go `GenerateTestCustomBucketsHistogram`.
pub fn generate_test_custom_buckets_histogram(i: i64) -> Histogram {
    Histogram {
        count: (5 + i * 4) as u64,
        sum: 18.4 * (i + 1) as f64,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: spans(),
        positive_buckets: vec![i + 1, 1, -1, 0],
        custom_values: custom_values(&[0.0, 1.0, 2.0, 3.0, 4.0]),
        ..Default::default()
    }
}

/// Go `GenerateTestGaugeHistograms`.
pub fn generate_test_gauge_histograms(n: usize) -> Vec<Histogram> {
    (0..n).map(|x| generate_test_gauge_histogram(((x as f64).sin() * 100.0) as i64 + 100)).collect()
}

/// Go `GenerateTestGaugeHistogram`.
pub fn generate_test_gauge_histogram(i: i64) -> Histogram {
    let mut h = generate_test_histogram(i);
    h.counter_reset_hint = CounterResetHint::GaugeType;
    h
}

/// Go `GenerateTestFloatHistograms`.
pub fn generate_test_float_histograms(n: usize) -> Vec<FloatHistogram> {
    (0..n)
        .map(|i| {
            let mut h = generate_test_float_histogram(i as i64);
            if i > 0 {
                h.counter_reset_hint = CounterResetHint::NotCounterReset;
            }
            h
        })
        .collect()
}

/// Go `GenerateTestFloatHistogram`; the caller sets any known counter reset hint.
pub fn generate_test_float_histogram(i: i64) -> FloatHistogram {
    let f = i as f64;
    FloatHistogram {
        count: 12.0 + (i * 9) as f64,
        zero_count: 2.0 + f,
        zero_threshold: 0.001,
        sum: 18.4 * (i + 1) as f64,
        schema: 1,
        positive_spans: spans(),
        positive_buckets: vec![f + 1.0, f + 2.0, f + 1.0, f + 1.0],
        negative_spans: spans(),
        negative_buckets: vec![f + 1.0, f + 2.0, f + 1.0, f + 1.0],
        ..Default::default()
    }
}

/// Go `GenerateTestCustomBucketsFloatHistograms`.
pub fn generate_test_custom_buckets_float_histograms(n: usize) -> Vec<FloatHistogram> {
    (0..n)
        .map(|i| {
            let mut h = generate_test_custom_buckets_float_histogram(i as i64);
            if i > 0 {
                h.counter_reset_hint = CounterResetHint::NotCounterReset;
            }
            h
        })
        .collect()
}

/// Go `GenerateTestCustomBucketsFloatHistogram`.
pub fn generate_test_custom_buckets_float_histogram(i: i64) -> FloatHistogram {
    let f = i as f64;
    FloatHistogram {
        count: 5.0 + (i * 4) as f64,
        sum: 18.4 * (i + 1) as f64,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: spans(),
        positive_buckets: vec![f + 1.0, f + 2.0, f + 1.0, f + 1.0],
        custom_values: custom_values(&[0.0, 1.0, 2.0, 3.0, 4.0]),
        ..Default::default()
    }
}

/// Go `GenerateTestGaugeFloatHistograms`.
pub fn generate_test_gauge_float_histograms(n: usize) -> Vec<FloatHistogram> {
    (0..n).map(|x| generate_test_gauge_float_histogram(((x as f64).sin() * 100.0) as i64 + 100)).collect()
}

/// Go `GenerateTestGaugeFloatHistogram`.
pub fn generate_test_gauge_float_histogram(i: i64) -> FloatHistogram {
    let mut h = generate_test_float_histogram(i);
    h.counter_reset_hint = CounterResetHint::GaugeType;
    h
}

/// Go `SetHistogramNotCounterReset`.
pub fn set_histogram_not_counter_reset(mut h: Histogram) -> Histogram {
    h.counter_reset_hint = CounterResetHint::NotCounterReset;
    h
}

/// Go `SetHistogramCounterReset`.
pub fn set_histogram_counter_reset(mut h: Histogram) -> Histogram {
    h.counter_reset_hint = CounterResetHint::CounterReset;
    h
}

/// Go `SetFloatHistogramNotCounterReset`.
pub fn set_float_histogram_not_counter_reset(mut h: FloatHistogram) -> FloatHistogram {
    h.counter_reset_hint = CounterResetHint::NotCounterReset;
    h
}

/// Go `SetFloatHistogramCounterReset`.
pub fn set_float_histogram_counter_reset(mut h: FloatHistogram) -> FloatHistogram {
    h.counter_reset_hint = CounterResetHint::CounterReset;
    h
}
