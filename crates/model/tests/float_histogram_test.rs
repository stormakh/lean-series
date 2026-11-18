//! Port of upstream `model/histogram/float_histogram_test.go`. Go benchmarks (and the `createRandom*` helpers they
//! use) are not ported.

use model::histogram::*;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Builds a [`FloatHistogram`] from the given fields; all other fields keep their zero value (like a Go struct
/// literal).
macro_rules! fh {
    ($($f:ident : $v:expr),* $(,)?) => {
        FloatHistogram { $($f: $v,)* ..Default::default() }
    };
}

/// Port of Go `expandSpans` (histogram_test.go): appends one zero span.
// adapted: Go reslices into spare capacity when there is some (sharing the backing array); a Rust Vec never
// shares its buffer, so appending a zero span gives the same observable result.
fn expand_spans(mut spans: Vec<Span>) -> Vec<Span> {
    spans.push(Span::default());
    spans
}

/// Port of Go `assertDeepCopyFHSpans`.
// adapted: in Go this checks that the copy does not share the spans backing array with the original. Rust
// Vec clones never share buffers; the check still verifies that changing the original does not change the copy.
fn assert_deep_copy_fh_spans(
    name: &str,
    orig: &mut FloatHistogram,
    h_copy: &mut FloatHistogram,
    expected: &mut FloatHistogram,
) {
    // Do an in-place expansion of an original spans slice.
    orig.positive_spans = expand_spans(std::mem::take(&mut orig.positive_spans));
    let n = orig.positive_spans.len();
    orig.positive_spans[n - 1] = Span::new(1, 2);

    h_copy.positive_spans = expand_spans(std::mem::take(&mut h_copy.positive_spans));
    expected.positive_spans = expand_spans(std::mem::take(&mut expected.positive_spans));
    // Expand the copy spans and assert that modifying the original has not affected the copy.
    assert_eq!(*expected, *h_copy, "{name}");
}

#[test]
fn test_float_histogram_mul() {
    struct Case {
        name: &'static str,
        in_: FloatHistogram,
        scale: f64,
        expected: FloatHistogram,
    }
    let cases = vec![
        Case {
            name: "zero value",
            in_: fh! {},
            scale: 3.1415,
            expected: fh! {},
        },
        Case {
            name: "zero multiplier",
            in_: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            scale: 0.0,
            expected: fh! {
                zero_threshold: 0.01,
                zero_count: 0.0,
                count: 0.0,
                sum: 0.0,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![0.0, 0.0, 0.0, 0.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![0.0, 0.0, 0.0, 0.0],
            },
        },
        Case {
            name: "no-op",
            in_: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            scale: 1.0,
            expected: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
        },
        Case {
            name: "double",
            in_: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            scale: 2.0,
            expected: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 3493.3 * 2.0,
                sum: 2349209.324 * 2.0,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![2.0, 6.6, 8.4, 0.2],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![6.2, 6.0, 1.234e5 * 2.0, 2000.0],
            },
        },
        Case {
            name: "triple",
            in_: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 23.0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
            },
            scale: 3.0,
            expected: fh! {
                zero_threshold: 0.01,
                zero_count: 33.0,
                count: 90.0,
                sum: 69.0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![3.0, 0.0, 9.0, 12.0, 21.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![9.0, 3.0, 15.0, 18.0],
            },
        },
        Case {
            name: "negation",
            in_: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 23.0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
            },
            scale: -1.0,
            expected: fh! {
                zero_threshold: 0.01,
                zero_count: -11.0,
                count: -30.0,
                sum: -23.0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![-1.0, 0.0, -3.0, -4.0, -7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![-3.0, -1.0, -5.0, -6.0],
                counter_reset_hint: CounterResetHint::GaugeType,
            },
        },
        Case {
            name: "negative multiplier",
            in_: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 23.0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
            },
            scale: -2.0,
            expected: fh! {
                zero_threshold: 0.01,
                zero_count: -22.0,
                count: -60.0,
                sum: -46.0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![-2.0, 0.0, -6.0, -8.0, -14.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![-6.0, -2.0, -10.0, -12.0],
                counter_reset_hint: CounterResetHint::GaugeType,
            },
        },
        Case {
            name: "no-op with custom buckets",
            in_: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
            scale: 1.0,
            expected: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
        },
        Case {
            name: "triple with custom buckets",
            in_: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 30.0,
                sum: 23.0,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
            scale: 3.0,
            expected: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 90.0,
                sum: 69.0,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![3.0, 0.0, 9.0, 12.0, 21.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
        },
    ];

    for mut c in cases {
        assert_eq!(c.expected, *c.in_.mul(c.scale), "{}", c.name);
        // Has it also happened in-place?
        assert_eq!(c.expected, c.in_, "{}", c.name);
    }
}

#[test]
fn test_float_histogram_copy() {
    struct Case {
        name: &'static str,
        orig: FloatHistogram,
        expected: FloatHistogram,
    }
    let cases = vec![
        Case {
            name: "without buckets",
            orig: fh! {},
            expected: fh! {},
        },
        Case {
            name: "with buckets",
            orig: fh! {
                positive_spans: vec![Span::new(-2, 1)],
                positive_buckets: vec![1.0, 3.0, -3.0, 42.0],
                negative_spans: vec![Span::new(3, 2)],
                negative_buckets: vec![5.0, 3.0, 1.234e5, 1000.0],
            },
            expected: fh! {
                positive_spans: vec![Span::new(-2, 1)],
                positive_buckets: vec![1.0, 3.0, -3.0, 42.0],
                negative_spans: vec![Span::new(3, 2)],
                negative_buckets: vec![5.0, 3.0, 1.234e5, 1000.0],
            },
        },
        Case {
            name: "with empty buckets and non empty capacity",
            orig: fh! {
                positive_spans: Vec::with_capacity(1),
                positive_buckets: Vec::with_capacity(1),
                negative_spans: Vec::with_capacity(1),
                negative_buckets: Vec::with_capacity(1),
            },
            expected: fh! {},
        },
        Case {
            name: "with custom buckets",
            orig: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 1)],
                positive_buckets: vec![1.0, 3.0, -3.0, 42.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
            expected: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 1)],
                positive_buckets: vec![1.0, 3.0, -3.0, 42.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
        },
    ];

    for mut tcase in cases {
        let mut h_copy = tcase.orig.copy();

        // Modify a primitive value in the original histogram.
        tcase.orig.sum += 1.0;
        assert_eq!(tcase.expected, h_copy, "{}", tcase.name);
        assert_deep_copy_fh_spans(tcase.name, &mut tcase.orig, &mut h_copy, &mut tcase.expected);
    }
}

#[test]
fn test_float_histogram_copy_to() {
    struct Case {
        name: &'static str,
        orig: FloatHistogram,
        expected: FloatHistogram,
    }
    let cases = vec![
        Case {
            name: "without buckets",
            orig: fh! {},
            expected: fh! {},
        },
        Case {
            name: "with buckets",
            orig: fh! {
                positive_spans: vec![Span::new(-2, 1)],
                positive_buckets: vec![1.0, 3.0, -3.0, 42.0],
                negative_spans: vec![Span::new(3, 2)],
                negative_buckets: vec![5.0, 3.0, 1.234e5, 1000.0],
            },
            expected: fh! {
                positive_spans: vec![Span::new(-2, 1)],
                positive_buckets: vec![1.0, 3.0, -3.0, 42.0],
                negative_spans: vec![Span::new(3, 2)],
                negative_buckets: vec![5.0, 3.0, 1.234e5, 1000.0],
            },
        },
        Case {
            name: "with empty buckets and non empty capacity",
            orig: fh! {
                positive_spans: Vec::with_capacity(1),
                positive_buckets: Vec::with_capacity(1),
                negative_spans: Vec::with_capacity(1),
                negative_buckets: Vec::with_capacity(1),
            },
            expected: fh! {},
        },
        Case {
            name: "with custom buckets",
            orig: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 1)],
                positive_buckets: vec![1.0, 3.0, -3.0, 42.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
            expected: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 1)],
                positive_buckets: vec![1.0, 3.0, -3.0, 42.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
        },
    ];

    for mut tcase in cases {
        let mut h_copy = FloatHistogram::default();
        tcase.orig.copy_to(&mut h_copy);

        // Modify a primitive value in the original histogram.
        tcase.orig.sum += 1.0;
        assert_eq!(tcase.expected, h_copy, "{}", tcase.name);
        assert_deep_copy_fh_spans(tcase.name, &mut tcase.orig, &mut h_copy, &mut tcase.expected);
    }
}

#[test]
fn test_float_histogram_div() {
    struct Case {
        name: &'static str,
        fh: FloatHistogram,
        s: f64,
        expected: FloatHistogram,
    }
    let cases = vec![
        Case {
            name: "zero value",
            fh: fh! {},
            s: 3.1415,
            expected: fh! {},
        },
        Case {
            name: "zero divisor",
            fh: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            s: 0.0,
            expected: fh! {
                zero_threshold: 0.01,
                count: f64::INFINITY,
                sum: f64::INFINITY,
                zero_count: f64::INFINITY,
            },
        },
        Case {
            name: "no-op",
            fh: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            s: 1.0,
            expected: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
        },
        Case {
            name: "half",
            fh: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 23.0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
            },
            s: 2.0,
            expected: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 15.0,
                sum: 11.5,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![0.5, 0.0, 1.5, 2.0, 3.5],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![1.5, 0.5, 2.5, 3.0],
            },
        },
        Case {
            name: "negation",
            fh: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            s: -1.0,
            expected: fh! {
                zero_threshold: 0.01,
                zero_count: -5.5,
                count: -3493.3,
                sum: -2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![-1.0, -3.3, -4.2, -0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![-3.1, -3.0, -1.234e5, -1000.0],
                counter_reset_hint: CounterResetHint::GaugeType,
            },
        },
        Case {
            name: "negative half",
            fh: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 23.0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
            },
            s: -2.0,
            expected: fh! {
                zero_threshold: 0.01,
                zero_count: -5.5,
                count: -15.0,
                sum: -11.5,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![-0.5, 0.0, -1.5, -2.0, -3.5],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![-1.5, -0.5, -2.5, -3.0],
                counter_reset_hint: CounterResetHint::GaugeType,
            },
        },
        Case {
            name: "no-op with custom buckets",
            fh: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
            s: 1.0,
            expected: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
        },
        Case {
            name: "half with custom buckets",
            fh: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 30.0,
                sum: 23.0,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
            s: 2.0,
            expected: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 15.0,
                sum: 11.5,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![0.5, 0.0, 1.5, 2.0, 3.5],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
        },
    ];

    for mut c in cases {
        assert_eq!(c.expected, *c.fh.div(c.s), "{}", c.name);
        // Has it also happened in-place?
        assert_eq!(c.expected, c.fh, "{}", c.name);
    }
}

#[test]
fn test_float_histogram_detect_reset() {
    struct Case {
        name: &'static str,
        previous: FloatHistogram,
        current: FloatHistogram,
        reset_expected: bool,
    }
    let cases = vec![
        Case {
            name: "zero values",
            previous: fh! {},
            current: fh! {},
            reset_expected: false,
        },
        Case {
            name: "no buckets to some buckets",
            previous: fh! {},
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: false,
        },
        Case {
            name: "some buckets to no buckets",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {},
            reset_expected: true,
        },
        Case {
            name: "one bucket appears, nothing else changes",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 1.23, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: false,
        },
        Case {
            name: "one bucket disappears, nothing else changes",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 1.23, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: true,
        },
        Case {
            name: "an unpopulated bucket disappears, nothing else changes",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: false,
        },
        Case {
            name: "an unpopulated bucket at the end disappears, nothing else changes",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(1, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(1, 2)],
                positive_buckets: vec![1.0, 3.3, 4.2],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: false,
        },
        Case {
            name: "an unpopulated bucket disappears in a histogram with nothing else",
            previous: fh! {
                positive_spans: vec![Span::new(23, 1)],
                positive_buckets: vec![0.0],
            },
            current: fh! {},
            reset_expected: false,
        },
        Case {
            name: "zero count goes up",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.6,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: false,
        },
        Case {
            name: "zero count goes down",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.4,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: true,
        },
        Case {
            name: "count goes up",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.4,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: false,
        },
        Case {
            name: "count goes down",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.2,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: true,
        },
        Case {
            name: "sum goes up",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349210.0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: false,
        },
        Case {
            name: "sum goes down",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349200.0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: false,
        },
        Case {
            name: "one positive bucket goes up",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.3, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: false,
        },
        Case {
            name: "one positive bucket goes down",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.1, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: true,
        },
        Case {
            name: "one negative bucket goes up",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.1, 1.234e5, 1000.0],
            },
            reset_expected: false,
        },
        Case {
            name: "one negative bucket goes down",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 2.9, 1.234e5, 1000.0],
            },
            reset_expected: true,
        },
        Case {
            name: "zero threshold decreases",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 1.23, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.009,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 1.23, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: true,
        },
        Case {
            name: "zero threshold increases without touching any existing buckets",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 1.23, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.011,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 1.23, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: false,
        },
        Case {
            name: "zero threshold increases enough to cover existing buckets",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 1.23, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 1.0,
                zero_count: 7.73,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(1, 3)],
                positive_buckets: vec![3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: false,
        },
        Case {
            name: "zero threshold increases into the middle of an existing buckets",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 1.23, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.3,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 1.23, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: true,
        },
        Case {
            name: "schema increases without any other changes",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                schema: 0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 1.23, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                schema: 1,
                positive_spans: vec![Span::new(-5, 4), Span::new(2, 6)],
                positive_buckets: vec![0.4, 0.6, 1.0, 0.23, 2.0, 1.3, 1.2, 3.0, 0.05, 0.05],
                negative_spans: vec![Span::new(5, 4), Span::new(6, 4)],
                negative_buckets: vec![2.0, 1.1, 2.0, 1.0, 0.234e5, 1e5, 500.0, 500.0],
            },
            reset_expected: true,
        },
        Case {
            name: "schema decreases without any other changes",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                schema: 1,
                positive_spans: vec![Span::new(-5, 4), Span::new(2, 6)],
                positive_buckets: vec![0.4, 0.6, 1.0, 0.23, 2.0, 1.3, 1.2, 3.0, 0.05, 0.05],
                negative_spans: vec![Span::new(5, 4), Span::new(6, 4)],
                negative_buckets: vec![2.0, 1.1, 2.0, 1.0, 0.234e5, 1e5, 500.0, 500.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                schema: 0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 1.23, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: false,
        },
        Case {
            name: "schema decreases and a bucket goes up",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                schema: 1,
                positive_spans: vec![Span::new(-5, 4), Span::new(2, 6)],
                positive_buckets: vec![0.4, 0.6, 1.0, 0.23, 2.0, 1.3, 1.2, 3.0, 0.05, 0.05],
                negative_spans: vec![Span::new(5, 4), Span::new(6, 4)],
                negative_buckets: vec![2.0, 1.1, 2.0, 1.0, 0.234e5, 1e5, 500.0, 500.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                schema: 0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 1.23, 4.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: false,
        },
        Case {
            name: "schema decreases and a bucket goes down",
            previous: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                schema: 1,
                positive_spans: vec![Span::new(-5, 4), Span::new(2, 6)],
                positive_buckets: vec![0.4, 0.6, 1.0, 0.23, 2.0, 1.3, 1.2, 3.0, 0.05, 0.05],
                negative_spans: vec![Span::new(5, 4), Span::new(6, 4)],
                negative_buckets: vec![2.0, 1.1, 2.0, 1.0, 0.234e5, 1e5, 500.0, 500.0],
            },
            current: fh! {
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                schema: 0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 1.23, 2.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            reset_expected: true,
        },
        Case {
            name: "no buckets to some buckets with custom bounds",
            previous: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
            current: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
            reset_expected: false,
        },
        Case {
            name: "some buckets to no buckets with custom bounds",
            previous: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
            current: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
            reset_expected: true,
        },
        Case {
            name: "one bucket appears, nothing else changes with custom bounds",
            previous: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
            current: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 1.23, 3.3, 4.2, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
            reset_expected: false,
        },
        Case {
            name: "one bucket disappears, nothing else changes with custom bounds",
            previous: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 1.23, 3.3, 4.2, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
            current: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
            reset_expected: true,
        },
        Case {
            name: "an unpopulated bucket disappears, nothing else changes with custom bounds",
            previous: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
            current: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
            reset_expected: false,
        },
        Case {
            name: "one positive bucket goes up with custom bounds",
            previous: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
            current: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.3, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
            reset_expected: false,
        },
        Case {
            name: "one positive bucket goes down with custom bounds",
            previous: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.2, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
            current: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.3, 4.1, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
            reset_expected: true,
        },
        Case {
            name: "mismatched custom bounds - no reset when all buckets increase",
            previous: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 200.0,
                sum: 1000.0,
                positive_spans: vec![Span::new(0, 4)],
                positive_buckets: vec![20.0, 35.0, 40.0, 50.0], // Previous: buckets for: (-Inf,0], (0,1], (1,2], (2,3], then (3,+Inf]
                custom_values: custom_values(&[0.0, 1.0, 2.0, 3.0]),
            },
            current: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 200.0,
                sum: 1000.0,
                positive_spans: vec![Span::new(0, 5)],
                positive_buckets: vec![25.0, 15.0, 40.0, 50.0, 70.0], // Current: buckets for: (-Inf,0], (0,0.5], (0.5,1], (1,2], (2,3], then (3,+Inf]
                custom_values: custom_values(&[0.0, 0.5, 1.0, 2.0, 3.0]),
            },
            reset_expected: false, // No reset: (-Inf,0] increases from 20 to 25, (0,1] increases from 35 to 55, (1,2] increases from 40 to 50, (2,3] increases from 50 to 70
        },
        Case {
            name: "mismatched custom bounds - reset in middle bucket",
            previous: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 100.0,
                sum: 500.0,
                positive_spans: vec![Span::new(1, 4)],
                positive_buckets: vec![10.0, 15.0, 20.0, 25.0], // Buckets for: [0,1], [1,3], [3,5], [5,+Inf]
                custom_values: custom_values(&[0.0, 1.0, 3.0, 5.0]),
            },
            current: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 100.0,
                sum: 500.0,
                positive_spans: vec![Span::new(1, 5)],
                positive_buckets: vec![10.0, 16.0, 10.0, 5.0, 25.0], // Buckets for: [0,1], [1,2], [2,3], [3,5], [5,+Inf]
                custom_values: custom_values(&[0.0, 1.0, 2.0, 3.0, 5.0]),
            },
            reset_expected: true, // Reset detected: [1,3] bucket decreased from 20 to 15
        },
        Case {
            name: "mismatched custom bounds - reset in last bucket",
            previous: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 100.0,
                sum: 500.0,
                positive_spans: vec![Span::new(1, 3)],
                positive_buckets: vec![10.0, 20.0, 20.0], // Buckets for: [0,1], [1,2], [2,+Inf]
                custom_values: custom_values(&[0.0, 1.0, 2.0]),
            },
            current: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 100.0,
                sum: 500.0,
                positive_spans: vec![Span::new(1, 4)],
                positive_buckets: vec![100.0, 200.0, 8.0, 7.0], // Buckets for: [0,1], [1,2], [2,3], [3,+Inf]
                custom_values: custom_values(&[0.0, 1.0, 2.0, 3.0]),
            },
            reset_expected: true, // Reset detected: [2,+Inf] bucket decreased from 20 to 15
        },
        Case {
            name: "mismatched custom bounds - no common bounds",
            previous: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 100.0,
                sum: 500.0,
                positive_spans: vec![Span::new(1, 3)],
                positive_buckets: vec![10.0, 20.0, 30.0], // Buckets for: [1,2], [2,3], [3,+Inf]
                custom_values: custom_values(&[4.0, 5.0, 6.0]),
            },
            current: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 100.0,
                sum: 500.0,
                positive_spans: vec![Span::new(1, 3)],
                positive_buckets: vec![15.0, 25.0, 35.0], // Buckets for: [4,5], [5,6], [6,+Inf]
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
            reset_expected: false, // no decrease in aggregated single +Inf bounded bucket
        },
        Case {
            name: "mismatched custom bounds - sparse common bounds",
            previous: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 200.0,
                sum: 1000.0,
                positive_spans: vec![Span::new(0, 4)],
                positive_buckets: vec![10.0, 20.0, 30.0, 40.0], // Previous: buckets for: (-Inf,0], (0,1], (1,3], (3,5], then (5,+Inf]
                custom_values: custom_values(&[0.0, 1.0, 3.0, 5.0]),
            },
            current: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 200.0,
                sum: 1000.0,
                positive_spans: vec![Span::new(0, 5)],
                positive_buckets: vec![15.0, 25.0, 70.0, 50.0, 100.0], // Current: buckets for: (-Inf,0], (0,2], (2,3], (3,4], (4,5], then (5,+Inf]
                custom_values: custom_values(&[0.0, 2.0, 3.0, 4.0, 5.0]),
            },
            reset_expected: false, // No reset: common bounds [0,3,5] all increase when mapped
        },
        Case {
            name: "reset detected with mismatched custom bounds and split bucket spans",
            previous: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 200.0,
                sum: 1000.0,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)], // Split spans: buckets at indices 0,1 and 3,4,5
                positive_buckets: vec![10.0, 20.0, 30.0, 40.0, 50.0], // Buckets for: (-Inf,0], (0,1], skip (1,2], then (2,3], (3,4], (4,5]
                custom_values: custom_values(&[0.0, 1.0, 2.0, 3.0, 4.0, 5.0]),
            },
            current: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 200.0,
                sum: 1000.0,
                positive_spans: vec![Span::new(0, 3), Span::new(1, 2)], // Split spans: buckets at indices 0,1,2 and 4,5
                positive_buckets: vec![15.0, 25.0, 35.0, 25.0, 60.0], // Buckets for: (-Inf,0], (0,1], (1,3], skip (3,4], then (4,5], (5,7]
                custom_values: custom_values(&[0.0, 1.0, 3.0, 4.0, 5.0, 7.0]),
            },
            reset_expected: true, // Reset detected: bucket (3,4] goes from 40 to 0 (missing in current histogram)
        },
        Case {
            name: "no reset with mismatched custom bounds and split bucket spans",
            previous: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 300.0,
                sum: 1500.0,
                positive_spans: vec![Span::new(0, 2), Span::new(2, 3)], // Split spans: buckets at indices 0,1 and 4,5,6
                positive_buckets: vec![10.0, 20.0, 30.0, 40.0, 50.0], // Buckets for: (-Inf,0], (0,1], skip (1,2], (2,3], then (3,4], (4,5], (5,6]
                custom_values: custom_values(&[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0]),
            },
            current: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 300.0,
                sum: 1500.0,
                positive_spans: vec![Span::new(0, 3), Span::new(1, 2)], // Split spans: buckets at indices 0,1,2 and 4,5
                positive_buckets: vec![12.0, 25.0, 45.0, 75.0, 95.0], // Buckets for: (-Inf,0], (0,0.5], (0.5,1], skip (1,3], then (3,5], (5,7]
                custom_values: custom_values(&[0.0, 0.5, 1.0, 3.0, 5.0, 7.0]),
            },
            reset_expected: false, // No reset: all mapped buckets increase
        },
        Case {
            // The previous histogram's first negative bucket is empty and a
            // later one is populated. The current histogram has no negative
            // buckets at all, so the populated previous bucket disappeared,
            // which is a reset.
            name: "reset when a populated bucket following an empty one disappears",
            previous: fh! {
                zero_threshold: 0.001,
                count: 10.0,
                positive_spans: vec![Span::new(0, 1)],
                positive_buckets: vec![5.0],
                negative_spans: vec![Span::new(1, 2)],
                negative_buckets: vec![0.0, 5.0],
            },
            current: fh! {
                zero_threshold: 0.001,
                count: 20.0,
                positive_spans: vec![Span::new(0, 1)],
                positive_buckets: vec![20.0],
            },
            reset_expected: true,
        },
        Case {
            // The current histogram runs out of buckets before a later
            // populated bucket of the previous histogram, which sits behind an
            // empty one. That populated bucket disappeared, so it is a reset.
            name: "reset when current ends before a later populated previous bucket",
            previous: fh! {
                zero_threshold: 0.001,
                count: 7.0,
                positive_spans: vec![Span::new(0, 3)],
                positive_buckets: vec![2.0, 0.0, 5.0],
            },
            current: fh! {
                zero_threshold: 0.001,
                count: 100.0,
                positive_spans: vec![Span::new(0, 1)],
                positive_buckets: vec![100.0],
            },
            reset_expected: true,
        },
    ];

    for c in cases {
        assert_eq!(c.reset_expected, c.current.detect_reset(&c.previous), "{}", c.name);
    }
}

#[test]
fn test_float_histogram_compact() {
    struct Case {
        name: &'static str,
        in_: FloatHistogram,
        max_empty_buckets: usize,
        expected: FloatHistogram,
    }
    let cases = vec![
        Case {
            name: "empty histogram",
            in_: fh! {},
            max_empty_buckets: 0,
            expected: fh! {},
        },
        Case {
            name: "nothing should happen",
            in_: fh! {
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
            max_empty_buckets: 0,
            expected: fh! {
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
            },
        },
        Case {
            name: "eliminate zero offsets",
            in_: fh! {
                positive_spans: vec![Span::new(-2, 1), Span::new(0, 3), Span::new(0, 1)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3],
                negative_spans: vec![Span::new(0, 2), Span::new(0, 2), Span::new(2, 1), Span::new(0, 1)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 3.0, 4.0],
            },
            max_empty_buckets: 0,
            expected: fh! {
                positive_spans: vec![Span::new(-2, 5)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3],
                negative_spans: vec![Span::new(0, 4), Span::new(2, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 3.0, 4.0],
            },
        },
        Case {
            name: "eliminate zero length",
            in_: fh! {
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 0), Span::new(3, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3],
                negative_spans: vec![Span::new(0, 2), Span::new(0, 0), Span::new(2, 0), Span::new(1, 4)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 3.0, 4.0],
            },
            max_empty_buckets: 0,
            expected: fh! {
                positive_spans: vec![Span::new(-2, 2), Span::new(5, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 4)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 3.0, 4.0],
            },
        },
        Case {
            name: "eliminate multiple zero length spans",
            in_: fh! {
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 0), Span::new(2, 0), Span::new(2, 0), Span::new(3, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3],
            },
            max_empty_buckets: 0,
            expected: fh! {
                positive_spans: vec![Span::new(-2, 2), Span::new(9, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3],
            },
        },
        Case {
            name: "cut empty buckets at start or end",
            in_: fh! {
                positive_spans: vec![Span::new(-4, 4), Span::new(5, 3)],
                positive_buckets: vec![0.0, 0.0, 1.0, 3.3, 4.2, 0.1, 3.3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 5)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 3.0, 4.0, 0.0],
            },
            max_empty_buckets: 0,
            expected: fh! {
                positive_spans: vec![Span::new(-2, 2), Span::new(5, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 4)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 3.0, 4.0],
            },
        },
        Case {
            name: "cut empty buckets at start and end",
            in_: fh! {
                positive_spans: vec![Span::new(-4, 4), Span::new(5, 6)],
                positive_buckets: vec![0.0, 0.0, 1.0, 3.3, 4.2, 0.1, 3.3, 0.0, 0.0, 0.0],
                negative_spans: vec![Span::new(-2, 4), Span::new(3, 5)],
                negative_buckets: vec![0.0, 0.0, 3.1, 3.0, 1.234e5, 1000.0, 3.0, 4.0, 0.0],
            },
            max_empty_buckets: 0,
            expected: fh! {
                positive_spans: vec![Span::new(-2, 2), Span::new(5, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 4)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 3.0, 4.0],
            },
        },
        Case {
            name: "cut empty buckets in the middle",
            in_: fh! {
                positive_spans: vec![Span::new(5, 4)],
                positive_buckets: vec![1.0, 3.0, 0.0, 2.0],
            },
            max_empty_buckets: 0,
            expected: fh! {
                positive_spans: vec![Span::new(5, 2), Span::new(1, 1)],
                positive_buckets: vec![1.0, 3.0, 2.0],
            },
        },
        Case {
            name: "cut empty buckets at start or end of spans, even in the middle",
            in_: fh! {
                positive_spans: vec![Span::new(-4, 6), Span::new(3, 6)],
                positive_buckets: vec![0.0, 0.0, 1.0, 3.3, 0.0, 0.0, 4.2, 0.1, 3.3, 0.0, 0.0, 0.0],
                negative_spans: vec![Span::new(0, 2), Span::new(2, 6)],
                negative_buckets: vec![3.1, 3.0, 0.0, 1.234e5, 1000.0, 3.0, 4.0, 0.0],
            },
            max_empty_buckets: 0,
            expected: fh! {
                positive_spans: vec![Span::new(-2, 2), Span::new(5, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 4)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 3.0, 4.0],
            },
        },
        Case {
            name: "cut empty buckets at start and end - also merge spans due to maxEmptyBuckets",
            in_: fh! {
                positive_spans: vec![Span::new(-4, 4), Span::new(5, 3)],
                positive_buckets: vec![0.0, 0.0, 1.0, 3.3, 4.2, 0.1, 3.3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 5)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 3.0, 4.0, 0.0],
            },
            max_empty_buckets: 10,
            expected: fh! {
                positive_spans: vec![Span::new(-2, 10)],
                positive_buckets: vec![1.0, 3.3, 0.0, 0.0, 0.0, 0.0, 0.0, 4.2, 0.1, 3.3],
                negative_spans: vec![Span::new(0, 9)],
                negative_buckets: vec![3.1, 3.0, 0.0, 0.0, 0.0, 1.234e5, 1000.0, 3.0, 4.0],
            },
        },
        Case {
            name: "cut empty buckets from the middle of a span",
            in_: fh! {
                positive_spans: vec![Span::new(-4, 6), Span::new(3, 3)],
                positive_buckets: vec![0.0, 0.0, 1.0, 0.0, 0.0, 3.3, 4.2, 0.1, 3.3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 5)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 0.0, 3.0, 4.0],
            },
            max_empty_buckets: 0,
            expected: fh! {
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 1), Span::new(3, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 2), Span::new(1, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 3.0, 4.0],
            },
        },
        Case {
            name: "cut out a span containing only empty buckets",
            in_: fh! {
                positive_spans: vec![Span::new(-4, 3), Span::new(2, 2), Span::new(3, 4)],
                positive_buckets: vec![0.0, 0.0, 1.0, 0.0, 0.0, 3.3, 4.2, 0.1, 3.3],
            },
            max_empty_buckets: 0,
            expected: fh! {
                positive_spans: vec![Span::new(-2, 1), Span::new(7, 4)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3],
            },
        },
        Case {
            name: "cut empty buckets from the middle of a span, avoiding none due to maxEmptyBuckets",
            in_: fh! {
                positive_spans: vec![Span::new(-2, 4)],
                positive_buckets: vec![1.0, 0.0, 0.0, 3.3],
            },
            max_empty_buckets: 1,
            expected: fh! {
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 1)],
                positive_buckets: vec![1.0, 3.3],
            },
        },
        Case {
            name: "cut empty buckets and merge spans due to maxEmptyBuckets",
            in_: fh! {
                positive_spans: vec![Span::new(-2, 4), Span::new(3, 1)],
                positive_buckets: vec![1.0, 0.0, 0.0, 3.3, 4.2],
            },
            max_empty_buckets: 1,
            expected: fh! {
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 1), Span::new(3, 1)],
                positive_buckets: vec![1.0, 3.3, 4.2],
            },
        },
        Case {
            name: "cut empty buckets from the middle of a span, avoiding some due to maxEmptyBuckets",
            in_: fh! {
                positive_spans: vec![Span::new(-4, 6), Span::new(3, 3), Span::new(10, 2)],
                positive_buckets: vec![0.0, 0.0, 1.0, 0.0, 0.0, 3.3, 4.2, 0.1, 3.3, 2.0, 3.0],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 5)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 0.0, 3.0, 4.0],
            },
            max_empty_buckets: 1,
            expected: fh! {
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 1), Span::new(3, 3), Span::new(10, 2)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3, 2.0, 3.0],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 5)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 0.0, 3.0, 4.0],
            },
        },
        Case {
            name: "avoiding all cutting of empty buckets from the middle of a chunk due to maxEmptyBuckets",
            in_: fh! {
                positive_spans: vec![Span::new(-4, 6), Span::new(3, 3)],
                positive_buckets: vec![0.0, 0.0, 1.0, 0.0, 0.0, 3.3, 4.2, 0.1, 3.3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 5)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 0.0, 3.0, 4.0],
            },
            max_empty_buckets: 2,
            expected: fh! {
                positive_spans: vec![Span::new(-2, 4), Span::new(3, 3)],
                positive_buckets: vec![1.0, 0.0, 0.0, 3.3, 4.2, 0.1, 3.3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 5)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 0.0, 3.0, 4.0],
            },
        },
        Case {
            name: "everything merged into one span due to maxEmptyBuckets",
            in_: fh! {
                positive_spans: vec![Span::new(-4, 6), Span::new(3, 3)],
                positive_buckets: vec![0.0, 0.0, 1.0, 0.0, 0.0, 3.3, 4.2, 0.1, 3.3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 5)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0, 0.0, 3.0, 4.0],
            },
            max_empty_buckets: 3,
            expected: fh! {
                positive_spans: vec![Span::new(-2, 10)],
                positive_buckets: vec![1.0, 0.0, 0.0, 3.3, 0.0, 0.0, 0.0, 4.2, 0.1, 3.3],
                negative_spans: vec![Span::new(0, 10)],
                negative_buckets: vec![3.1, 3.0, 0.0, 0.0, 0.0, 1.234e5, 1000.0, 0.0, 3.0, 4.0],
            },
        },
        Case {
            name: "more than one span merged in the same pass",
            in_: fh! {
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 2), Span::new(2, 2)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3, 2.0],
                negative_spans: vec![Span::new(0, 1), Span::new(1, 1), Span::new(1, 1)],
                negative_buckets: vec![3.1, 1000.0, 4.0],
            },
            max_empty_buckets: 3,
            expected: fh! {
                positive_spans: vec![Span::new(-2, 10)],
                positive_buckets: vec![1.0, 3.3, 0.0, 0.0, 4.2, 0.1, 0.0, 0.0, 3.3, 2.0],
                negative_spans: vec![Span::new(0, 5)],
                negative_buckets: vec![3.1, 0.0, 1000.0, 0.0, 4.0],
            },
        },
        Case {
            name: "only empty buckets and maxEmptyBuckets greater zero",
            in_: fh! {
                positive_spans: vec![Span::new(-4, 6), Span::new(3, 3)],
                positive_buckets: vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                negative_spans: vec![Span::new(0, 7)],
                negative_buckets: vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            },
            max_empty_buckets: 3,
            expected: fh! {
                positive_spans: vec![],
                positive_buckets: vec![],
                negative_spans: vec![],
                negative_buckets: vec![],
            },
        },
        Case {
            name: "multiple spans of only empty buckets",
            in_: fh! {
                positive_spans: vec![Span::new(-10, 2), Span::new(2, 1), Span::new(3, 3)],
                positive_buckets: vec![0.0, 0.0, 0.0, 0.0, 2.0, 3.0],
                negative_spans: vec![Span::new(-10, 2), Span::new(2, 1), Span::new(3, 3)],
                negative_buckets: vec![2.0, 3.0, 0.0, 0.0, 0.0, 0.0],
            },
            max_empty_buckets: 0,
            expected: fh! {
                positive_spans: vec![Span::new(-1, 2)],
                positive_buckets: vec![2.0, 3.0],
                negative_spans: vec![Span::new(-10, 2)],
                negative_buckets: vec![2.0, 3.0],
            },
        },
        Case {
            name: "nothing should happen with custom buckets",
            in_: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
            max_empty_buckets: 0,
            expected: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
        },
        Case {
            name: "eliminate zero offsets with custom buckets",
            in_: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 1), Span::new(0, 3), Span::new(0, 1)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
            max_empty_buckets: 0,
            expected: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 5)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
        },
        Case {
            name: "eliminate multiple zero length spans with custom buckets",
            in_: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 2), Span::new(2, 0), Span::new(2, 0), Span::new(2, 0), Span::new(3, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
            max_empty_buckets: 0,
            expected: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 2), Span::new(9, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
            },
        },
        Case {
            name: "cut empty buckets at start and end with custom buckets",
            in_: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 4), Span::new(5, 6)],
                positive_buckets: vec![0.0, 0.0, 1.0, 3.3, 4.2, 0.1, 3.3, 0.0, 0.0, 0.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
            },
            max_empty_buckets: 0,
            expected: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(2, 2), Span::new(5, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 3.3],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
            },
        },
    ];

    for mut c in cases {
        let was_valid = c.in_.validate().is_ok();
        assert_eq!(c.expected, *c.in_.compact(c.max_empty_buckets), "{}", c.name);
        // Compact has happened in-place, too.
        assert_eq!(c.expected, c.in_, "{}", c.name);
        if was_valid {
            if let Err(err) = c.in_.validate() {
                panic!("{}: Compact turned a valid histogram into an invalid one: {err}", c.name);
            }
        }
    }
}

/// TestFloatHistogramKahanCompactSpanMerge makes sure the Kahan compensation terms stay aligned with the buckets
/// they belong to when more than one span is merged in the same pass.
#[test]
fn test_float_histogram_kahan_compact_span_merge() {
    let mut h = fh! {
        positive_spans: vec![Span::new(0, 1), Span::new(1, 1), Span::new(1, 1)],
        positive_buckets: vec![1.0, 2.0, 3.0],
    };
    let mut c = fh! {
        positive_spans: vec![Span::new(0, 1), Span::new(1, 1), Span::new(1, 1)],
        positive_buckets: vec![0.1, 0.2, 0.3],
    };

    h.kahan_compact(3, &mut c);

    assert_eq!(vec![Span::new(0, 5)], h.positive_spans);
    assert_eq!(vec![1.0, 0.0, 2.0, 0.0, 3.0], h.positive_buckets);
    assert_eq!(vec![0.1, 0.0, 0.2, 0.0, 0.3], c.positive_buckets);
}

#[test]
fn test_float_histogram_add() {
    #[derive(Default)]
    struct Case {
        name: &'static str,
        in1: FloatHistogram,
        in2: FloatHistogram,
        expected: Option<FloatHistogram>,
        exp_err_msg: &'static str,
        exp_counter_reset_collision: bool,
        exp_nhcb_bounds_reconciled: bool,
    }
    let cases = vec![
        Case {
            name: "same bucket layout",
            in1: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
            },
            in2: fh! {
                zero_threshold: 0.01,
                zero_count: 8.0,
                count: 21.0,
                sum: 1.234,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![0.0, 0.0, 2.0, 3.0, 6.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0],
            },
            expected: Some(fh! {
                zero_threshold: 0.01,
                zero_count: 19.0,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 5.0, 7.0, 13.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![4.0, 2.0, 9.0, 10.0],
            }),
            ..Default::default()
        },
        Case {
            name: "same bucket layout, defined differently",
            in1: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 1), Span::new(0, 2)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
            },
            in2: fh! {
                zero_threshold: 0.01,
                zero_count: 8.0,
                count: 21.0,
                sum: 1.234,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 2), Span::new(0, 1)],
                positive_buckets: vec![0.0, 0.0, 2.0, 3.0, 6.0],
                negative_spans: vec![Span::new(3, 7)],
                negative_buckets: vec![1.0, 1.0, 0.0, 0.0, 0.0, 4.0, 4.0],
            },
            expected: Some(fh! {
                zero_threshold: 0.01,
                zero_count: 19.0,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 1), Span::new(0, 2)],
                positive_buckets: vec![1.0, 0.0, 5.0, 7.0, 13.0],
                negative_spans: vec![Span::new(3, 5), Span::new(0, 2)],
                negative_buckets: vec![4.0, 2.0, 0.0, 0.0, 0.0, 9.0, 10.0],
            }),
            ..Default::default()
        },
        Case {
            name: "non-overlapping spans",
            in1: fh! {
                zero_threshold: 0.001,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
            },
            in2: fh! {
                zero_threshold: 0.001,
                zero_count: 8.0,
                count: 21.0,
                sum: 1.234,
                positive_spans: vec![Span::new(0, 2), Span::new(3, 3)],
                positive_buckets: vec![5.0, 4.0, 2.0, 3.0, 6.0],
                negative_spans: vec![Span::new(-9, 2), Span::new(3, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0],
            },
            expected: Some(fh! {
                zero_threshold: 0.001,
                zero_count: 19.0,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(-2, 4), Span::new(0, 6)],
                positive_buckets: vec![1.0, 0.0, 5.0, 4.0, 3.0, 4.0, 7.0, 2.0, 3.0, 6.0],
                negative_spans: vec![Span::new(-9, 2), Span::new(3, 2), Span::new(5, 2), Span::new(3, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0, 3.0, 1.0, 5.0, 6.0],
            }),
            ..Default::default()
        },
        Case {
            name: "non-overlapping spans inverted order",
            in1: fh! {
                zero_threshold: 0.01,
                zero_count: 8.0,
                count: 21.0,
                sum: 1.234,
                positive_spans: vec![Span::new(0, 2), Span::new(3, 3)],
                positive_buckets: vec![5.0, 4.0, 2.0, 3.0, 6.0],
                negative_spans: vec![Span::new(-6, 2), Span::new(1, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0],
            },
            in2: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
            },
            expected: Some(fh! {
                zero_threshold: 0.01,
                zero_count: 19.0,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(-2, 2), Span::new(0, 5), Span::new(0, 3)],
                positive_buckets: vec![1.0, 0.0, 5.0, 4.0, 3.0, 4.0, 7.0, 2.0, 3.0, 6.0],
                negative_spans: vec![Span::new(-6, 2), Span::new(1, 2), Span::new(4, 2), Span::new(3, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0, 3.0, 1.0, 5.0, 6.0],
            }),
            ..Default::default()
        },
        Case {
            name: "overlapping spans",
            in1: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
            },
            in2: fh! {
                zero_threshold: 0.01,
                zero_count: 8.0,
                count: 21.0,
                sum: 1.234,
                positive_spans: vec![Span::new(-1, 4), Span::new(0, 3)],
                positive_buckets: vec![5.0, 4.0, 2.0, 3.0, 6.0, 2.0, 5.0],
                negative_spans: vec![Span::new(4, 2), Span::new(1, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0],
            },
            expected: Some(fh! {
                zero_threshold: 0.01,
                zero_count: 19.0,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(-2, 4), Span::new(0, 4)],
                positive_buckets: vec![1.0, 5.0, 4.0, 2.0, 6.0, 10.0, 9.0, 5.0],
                negative_spans: vec![Span::new(3, 3), Span::new(1, 3)],
                negative_buckets: vec![3.0, 2.0, 1.0, 4.0, 9.0, 6.0],
            }),
            ..Default::default()
        },
        Case {
            name: "overlapping spans inverted order",
            in1: fh! {
                zero_threshold: 0.01,
                zero_count: 8.0,
                count: 21.0,
                sum: 1.234,
                positive_spans: vec![Span::new(-1, 4), Span::new(0, 3)],
                positive_buckets: vec![5.0, 4.0, 2.0, 3.0, 6.0, 2.0, 5.0],
                negative_spans: vec![Span::new(4, 2), Span::new(1, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0],
            },
            in2: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
            },
            expected: Some(fh! {
                zero_threshold: 0.01,
                zero_count: 19.0,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(-2, 5), Span::new(0, 3)],
                positive_buckets: vec![1.0, 5.0, 4.0, 2.0, 6.0, 10.0, 9.0, 5.0],
                negative_spans: vec![Span::new(3, 3), Span::new(1, 3)],
                negative_buckets: vec![3.0, 2.0, 1.0, 4.0, 9.0, 6.0],
            }),
            ..Default::default()
        },
        Case {
            name: "schema change",
            in1: fh! {
                zero_threshold: 0.01,
                zero_count: 8.0,
                count: 21.0,
                sum: 1.234,
                schema: 0,
                positive_spans: vec![Span::new(-1, 4), Span::new(0, 3)],
                positive_buckets: vec![5.0, 4.0, 2.0, 3.0, 6.0, 2.0, 5.0],
                negative_spans: vec![Span::new(4, 2), Span::new(1, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0],
            },
            in2: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                schema: 1,
                positive_spans: vec![Span::new(-4, 3), Span::new(5, 5)],
                positive_buckets: vec![1.0, 0.0, 0.0, 3.0, 2.0, 2.0, 3.0, 4.0],
                negative_spans: vec![Span::new(6, 3), Span::new(6, 4)],
                negative_buckets: vec![3.0, 0.5, 0.5, 2.0, 3.0, 2.0, 4.0],
            },
            expected: Some(fh! {
                zero_threshold: 0.01,
                zero_count: 19.0,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(-2, 5), Span::new(0, 3)],
                positive_buckets: vec![1.0, 5.0, 4.0, 2.0, 6.0, 10.0, 9.0, 5.0],
                negative_spans: vec![Span::new(3, 3), Span::new(1, 3)],
                negative_buckets: vec![3.0, 2.0, 1.0, 4.0, 9.0, 6.0],
            }),
            ..Default::default()
        },
        Case {
            name: "larger zero bucket in first histogram",
            in1: fh! {
                zero_threshold: 1.0,
                zero_count: 17.0,
                count: 21.0,
                sum: 1.234,
                positive_spans: vec![Span::new(1, 2), Span::new(0, 3)],
                positive_buckets: vec![2.0, 3.0, 6.0, 2.0, 5.0],
                negative_spans: vec![Span::new(4, 2), Span::new(1, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0],
            },
            in2: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
            },
            expected: Some(fh! {
                zero_threshold: 1.0,
                zero_count: 29.0,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(1, 2), Span::new(0, 3)],
                positive_buckets: vec![2.0, 6.0, 10.0, 9.0, 5.0],
                negative_spans: vec![Span::new(3, 3), Span::new(1, 3)],
                negative_buckets: vec![3.0, 2.0, 1.0, 4.0, 9.0, 6.0],
            }),
            ..Default::default()
        },
        Case {
            name: "larger zero bucket in second histogram",
            in1: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
            },
            in2: fh! {
                zero_threshold: 1.0,
                zero_count: 17.0,
                count: 21.0,
                sum: 1.234,
                positive_spans: vec![Span::new(1, 2), Span::new(0, 3)],
                positive_buckets: vec![2.0, 3.0, 6.0, 2.0, 5.0],
                negative_spans: vec![Span::new(4, 2), Span::new(1, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0],
            },
            expected: Some(fh! {
                zero_threshold: 1.0,
                zero_count: 29.0,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(1, 5)],
                positive_buckets: vec![2.0, 6.0, 10.0, 9.0, 5.0],
                negative_spans: vec![Span::new(3, 3), Span::new(1, 3)],
                negative_buckets: vec![3.0, 2.0, 1.0, 4.0, 9.0, 6.0],
            }),
            ..Default::default()
        },
        Case {
            name: "larger zero threshold in first histogram ends up inside a populated bucket of second histogram",
            in1: fh! {
                zero_threshold: 0.2,
                zero_count: 17.0,
                count: 21.0,
                sum: 1.234,
                positive_spans: vec![Span::new(1, 2), Span::new(0, 3)],
                positive_buckets: vec![2.0, 3.0, 6.0, 2.0, 5.0],
                negative_spans: vec![Span::new(4, 2), Span::new(1, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0],
            },
            in2: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
            },
            expected: Some(fh! {
                zero_threshold: 0.25,
                zero_count: 29.0,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(-1, 1), Span::new(1, 5)],
                positive_buckets: vec![0.0, 2.0, 6.0, 10.0, 9.0, 5.0],
                negative_spans: vec![Span::new(3, 3), Span::new(1, 3)],
                negative_buckets: vec![3.0, 2.0, 1.0, 4.0, 9.0, 6.0],
            }),
            ..Default::default()
        },
        Case {
            name: "larger zero threshold in second histogram ends up inside a populated bucket of first histogram",
            in1: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
            },
            in2: fh! {
                zero_threshold: 0.2,
                zero_count: 17.0,
                count: 21.0,
                sum: 1.234,
                positive_spans: vec![Span::new(1, 2), Span::new(0, 3)],
                positive_buckets: vec![2.0, 3.0, 6.0, 2.0, 5.0],
                negative_spans: vec![Span::new(4, 2), Span::new(1, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0],
            },
            expected: Some(fh! {
                zero_threshold: 0.25,
                zero_count: 29.0,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(1, 5)],
                positive_buckets: vec![2.0, 6.0, 10.0, 9.0, 5.0],
                negative_spans: vec![Span::new(3, 3), Span::new(1, 3)],
                negative_buckets: vec![3.0, 2.0, 1.0, 4.0, 9.0, 6.0],
            }),
            ..Default::default()
        },
        Case {
            name: "schema change combined with larger zero bucket in second histogram",
            in1: fh! {
                zero_threshold: 0.01,
                zero_count: 8.0,
                count: 21.0,
                sum: 1.234,
                schema: 0,
                positive_spans: vec![Span::new(-2, 5), Span::new(0, 3)],
                positive_buckets: vec![2.0, 5.0, 4.0, 2.0, 3.0, 6.0, 2.0, 5.0],
                negative_spans: vec![Span::new(4, 2), Span::new(1, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0],
            },
            in2: fh! {
                zero_threshold: 0.25,
                zero_count: 12.0,
                count: 30.0,
                sum: 2.345,
                schema: 1,
                positive_spans: vec![Span::new(-3, 2), Span::new(5, 5)],
                positive_buckets: vec![1.0, 0.0, 3.0, 2.0, 2.0, 3.0, 4.0],
                negative_spans: vec![Span::new(6, 3), Span::new(6, 4)],
                negative_buckets: vec![3.0, 0.5, 0.5, 2.0, 3.0, 2.0, 4.0],
            },
            expected: Some(fh! {
                zero_threshold: 0.25,
                zero_count: 22.0,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(-1, 7)],
                positive_buckets: vec![6.0, 4.0, 2.0, 6.0, 10.0, 9.0, 5.0],
                negative_spans: vec![Span::new(3, 3), Span::new(1, 3)],
                negative_buckets: vec![3.0, 2.0, 1.0, 4.0, 9.0, 6.0],
            }),
            ..Default::default()
        },
        Case {
            name: "schema change combined with larger zero bucket in first histogram",
            in1: fh! {
                zero_threshold: 0.25,
                zero_count: 8.0,
                count: 21.0,
                sum: 1.234,
                schema: 0,
                positive_spans: vec![Span::new(-1, 4), Span::new(0, 3)],
                positive_buckets: vec![5.0, 4.0, 2.0, 3.0, 6.0, 2.0, 5.0],
                negative_spans: vec![Span::new(4, 2), Span::new(1, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0],
            },
            in2: fh! {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                schema: 1,
                positive_spans: vec![Span::new(-4, 3), Span::new(5, 5)],
                positive_buckets: vec![1.0, 0.0, 0.0, 3.0, 2.0, 2.0, 3.0, 4.0],
                negative_spans: vec![Span::new(6, 3), Span::new(6, 4)],
                negative_buckets: vec![3.0, 0.5, 0.5, 2.0, 3.0, 2.0, 4.0],
            },
            expected: Some(fh! {
                zero_threshold: 0.25,
                zero_count: 20.0,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(-1, 4), Span::new(0, 3)],
                positive_buckets: vec![5.0, 4.0, 2.0, 6.0, 10.0, 9.0, 5.0],
                negative_spans: vec![Span::new(3, 3), Span::new(1, 3)],
                negative_buckets: vec![3.0, 2.0, 1.0, 4.0, 9.0, 6.0],
            }),
            ..Default::default()
        },
        Case {
            name: "same custom bucket layout",
            in1: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 15.0,
                sum: 2.345,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0]),
            },
            in2: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 11.0,
                sum: 1.234,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![0.0, 0.0, 2.0, 3.0, 6.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0]),
            },
            expected: Some(fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 26.0,
                sum: 3.579,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 5.0, 7.0, 13.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0]),
            }),
            ..Default::default()
        },
        Case {
            name: "same custom bucket layout, defined differently",
            in1: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 15.0,
                sum: 2.345,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 1), Span::new(0, 2)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0]),
            },
            in2: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 11.0,
                sum: 1.234,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 2), Span::new(0, 1)],
                positive_buckets: vec![0.0, 0.0, 2.0, 3.0, 6.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0]),
            },
            expected: Some(fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 26.0,
                sum: 3.579,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 1), Span::new(0, 2)],
                positive_buckets: vec![1.0, 0.0, 5.0, 7.0, 13.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0]),
            }),
            ..Default::default()
        },
        Case {
            name: "non-overlapping spans with custom buckets",
            in1: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 15.0,
                sum: 2.345,
                positive_spans: vec![Span::new(0, 2), Span::new(2, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
            },
            in2: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 20.0,
                sum: 1.234,
                positive_spans: vec![Span::new(2, 2), Span::new(3, 3)],
                positive_buckets: vec![5.0, 4.0, 2.0, 3.0, 6.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
            },
            expected: Some(fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 35.0,
                sum: 3.579,
                positive_spans: vec![Span::new(0, 4), Span::new(0, 6)],
                positive_buckets: vec![1.0, 0.0, 5.0, 4.0, 3.0, 4.0, 7.0, 2.0, 3.0, 6.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
            }),
            ..Default::default()
        },
        Case {
            name: "non-overlapping spans inverted order with custom buckets",
            in1: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 20.0,
                sum: 1.234,
                positive_spans: vec![Span::new(2, 2), Span::new(3, 3)],
                positive_buckets: vec![5.0, 4.0, 2.0, 3.0, 6.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
            },
            in2: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 15.0,
                sum: 2.345,
                positive_spans: vec![Span::new(0, 2), Span::new(2, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
            },
            expected: Some(fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 35.0,
                sum: 3.579,
                positive_spans: vec![Span::new(0, 4), Span::new(0, 6)],
                positive_buckets: vec![1.0, 0.0, 5.0, 4.0, 3.0, 4.0, 7.0, 2.0, 3.0, 6.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
            }),
            ..Default::default()
        },
        Case {
            name: "overlapping spans with custom buckets",
            in1: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 15.0,
                sum: 2.345,
                positive_spans: vec![Span::new(0, 2), Span::new(2, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
            },
            in2: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 27.0,
                sum: 1.234,
                positive_spans: vec![Span::new(1, 4), Span::new(0, 3)],
                positive_buckets: vec![5.0, 4.0, 2.0, 3.0, 6.0, 2.0, 5.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
            },
            expected: Some(fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 42.0,
                sum: 3.579,
                positive_spans: vec![Span::new(0, 4), Span::new(0, 4)],
                positive_buckets: vec![1.0, 5.0, 4.0, 2.0, 6.0, 10.0, 9.0, 5.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
            }),
            ..Default::default()
        },
        Case {
            name: "overlapping spans inverted order with custom buckets",
            in1: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 27.0,
                sum: 1.234,
                positive_spans: vec![Span::new(1, 4), Span::new(0, 3)],
                positive_buckets: vec![5.0, 4.0, 2.0, 3.0, 6.0, 2.0, 5.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
            },
            in2: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 15.0,
                sum: 2.345,
                positive_spans: vec![Span::new(0, 2), Span::new(2, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
            },
            expected: Some(fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 42.0,
                sum: 3.579,
                positive_spans: vec![Span::new(0, 4), Span::new(0, 4)],
                positive_buckets: vec![1.0, 5.0, 4.0, 2.0, 6.0, 10.0, 9.0, 5.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
            }),
            ..Default::default()
        },
        Case {
            name: "custom buckets with partial intersection",
            in1: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 10.0,
                sum: 100.0,
                positive_spans: vec![Span::new(0, 3)],
                positive_buckets: vec![2.0, 3.0, 5.0],
                custom_values: custom_values(&[1.0, 2.5]),
            },
            in2: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 8.0,
                sum: 80.0,
                positive_spans: vec![Span::new(0, 4)],
                positive_buckets: vec![1.0, 2.0, 3.0, 2.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
            expected: Some(fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 18.0,
                sum: 180.0,
                positive_spans: vec![Span::new(0, 2)],
                positive_buckets: vec![3.0, 15.0],
                custom_values: custom_values(&[1.0]),
            }),
            exp_nhcb_bounds_reconciled: true,
            ..Default::default()
        },
        Case {
            name: "different custom bucket layout - intersection and rollup",
            in1: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 6.0,
                sum: 2.345,
                positive_spans: vec![Span::new(0, 1), Span::new(1, 1)],
                positive_buckets: vec![1.0, 5.0],
                custom_values: custom_values(&[2.0, 4.0]),
            },
            in2: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 220.0,
                sum: 1.234,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 1), Span::new(0, 2)],
                positive_buckets: vec![10.0, 20.0, 40.0, 50.0, 100.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0]),
            },
            expected: Some(fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 226.0,
                sum: 3.579,
                positive_spans: vec![Span::new(0, 3)],
                positive_buckets: vec![1.0 + 10.0 + 20.0, 40.0, 5.0 + 50.0 + 100.0],
                custom_values: custom_values(&[2.0, 4.0]),
            }),
            exp_nhcb_bounds_reconciled: true,
            ..Default::default()
        },
        Case {
            name: "custom buckets with no common boundaries except +Inf",
            in1: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3.0,
                sum: 50.0,
                positive_spans: vec![Span::new(0, 2)],
                positive_buckets: vec![1.0, 2.0],
                custom_values: custom_values(&[1.5]),
            },
            in2: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 30.0,
                sum: 40.0,
                positive_spans: vec![Span::new(0, 2)],
                positive_buckets: vec![10.0, 20.0],
                custom_values: custom_values(&[2.5]),
            },
            expected: Some(fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 33.0,
                sum: 90.0,
                positive_spans: vec![Span::new(0, 1)],
                positive_buckets: vec![1.0 + 2.0 + 10.0 + 20.0],
                custom_values: None,
            }),
            exp_nhcb_bounds_reconciled: true,
            ..Default::default()
        },
        Case {
            name: "mix exponential and custom buckets histograms",
            in1: fh! {
                zero_threshold: 0.01,
                zero_count: 8.0,
                count: 59.0,
                sum: 1.234,
                schema: 0,
                positive_spans: vec![Span::new(-2, 5), Span::new(0, 3)],
                positive_buckets: vec![2.0, 5.0, 4.0, 2.0, 3.0, 6.0, 7.0, 5.0],
                negative_spans: vec![Span::new(3, 3), Span::new(1, 3)],
                negative_buckets: vec![4.0, 10.0, 1.0, 4.0, 14.0, 7.0],
            },
            in2: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 11.0,
                sum: 12.0,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![0.0, 0.0, 2.0, 3.0, 6.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0]),
            },
            exp_err_msg: "cannot apply this operation on histograms with a mix of exponential and custom bucket schemas",
            ..Default::default()
        },
        Case {
            name: "warn on counter reset hint collision",
            in1: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                counter_reset_hint: CounterResetHint::CounterReset,
            },
            in2: fh! {
                schema: CUSTOM_BUCKETS_SCHEMA,
                counter_reset_hint: CounterResetHint::NotCounterReset,
            },
            exp_err_msg: "",
            exp_counter_reset_collision: true,
            ..Default::default()
        },
    ];

    for c in cases {
        let e = c.expected.as_ref();
        let (msg, crc, nbr) = (c.exp_err_msg, c.exp_counter_reset_collision, c.exp_nhcb_bounds_reconciled);
        test_histogram_add(c.name, &c.in1, &c.in2, e, msg, crc, nbr);
        test_histogram_add(c.name, &c.in2, &c.in1, e, msg, crc, nbr);
        test_histogram_kahan_add(c.name, &c.in1, None, &c.in2, e, msg, crc, nbr);
        test_histogram_kahan_add(c.name, &c.in2, None, &c.in1, e, msg, crc, nbr);
    }
}

/// Asserts the error result of an operation: `require.EqualError` if `exp_err_msg` is not empty, else
/// `require.NoError`.
fn check_err<T>(name: &str, res: &Result<T, Error>, exp_err_msg: &str) {
    if !exp_err_msg.is_empty() {
        match res {
            Err(err) => assert_eq!(exp_err_msg, err.to_string(), "{name}"),
            Ok(_) => panic!("{name}: expected error {exp_err_msg:?}, got none"),
        }
    } else if let Err(err) = res {
        panic!("{name}: unexpected error: {err}");
    }
}

/// Port of Go `testHistogramAdd`.
fn test_histogram_add(
    name: &str,
    a: &FloatHistogram,
    b: &FloatHistogram,
    expected: Option<&FloatHistogram>,
    exp_err_msg: &str,
    exp_counter_reset_collision: bool,
    exp_nhcb_bounds_reconciled: bool,
) {
    if let Err(err) = a.validate() {
        panic!("{name}: a: {err}");
    }
    if let Err(err) = b.validate() {
        panic!("{name}: b: {err}");
    }

    let mut a_copy = a.copy();
    let b_copy = b.copy();
    let mut expected_copy = expected.map(|e| e.copy());

    let r = a_copy.add(&b_copy);
    check_err(name, &r, exp_err_msg);
    // Go returns false for both warnings together with an error.
    let (counter_reset_collision, nhcb_bounds_reconciled) = r.unwrap_or((false, false));

    // Check that the warnings are correct.
    assert_eq!(exp_counter_reset_collision, counter_reset_collision, "{name}");
    assert_eq!(exp_nhcb_bounds_reconciled, nhcb_bounds_reconciled, "{name}");

    if let Some(expected_copy) = expected_copy.as_mut() {
        // adapted: Go's Add returns the receiver as `res`; in Rust the result is `a_copy` itself, so the
        // "result" and "in-place" checks below compare the same value.
        let res = &mut a_copy;
        res.compact(0);
        expected_copy.compact(0);

        assert_eq!(*expected_copy, *res, "{name}");

        // Has it also happened in-place?
        assert_eq!(*expected_copy, a_copy, "{name}");

        // Check that the argument was not mutated.
        assert_eq!(*b, b_copy, "{name}");
    }
}

/// Port of Go `testHistogramKahanAdd`.
#[allow(clippy::too_many_arguments)]
fn test_histogram_kahan_add(
    name: &str,
    a: &FloatHistogram,
    c: Option<&FloatHistogram>,
    b: &FloatHistogram,
    expected_sum: Option<&FloatHistogram>,
    exp_err_msg: &str,
    exp_counter_reset_collision: bool,
    exp_nhcb_bounds_reconciled: bool,
) {
    let mut a_copy = a.copy();
    let b_copy = b.copy();
    let c_copy = c.map(|c| c.copy());
    let mut expected_sum_copy = expected_sum.map(|e| e.copy());

    let r = a_copy.kahan_add(&b_copy, c_copy);
    check_err(name, &r, exp_err_msg);
    // Go returns a nil compensation histogram and false for both warnings together with an error.
    let (comp, counter_reset_collision, nhcb_bounds_reconciled) = match r {
        Ok((comp, crc, nbr)) => (Some(comp), crc, nbr),
        Err(_) => (None, false, false),
    };

    let mut has_res = false;
    if let Some(comp) = comp {
        // Check that a_copy and its compensation histogram layouts match after addition.
        assert_eq!(a_copy.schema, comp.schema, "{name}");
        assert_eq!(a_copy.zero_threshold, comp.zero_threshold, "{name}");
        assert_eq!(a_copy.positive_spans, comp.positive_spans, "{name}");
        assert_eq!(a_copy.negative_spans, comp.negative_spans, "{name}");
        assert_eq!(a_copy.custom_values_slice().len(), comp.custom_values_slice().len(), "{name}");
        assert_eq!(a_copy.positive_buckets.len(), comp.positive_buckets.len(), "{name}");
        assert_eq!(a_copy.negative_buckets.len(), comp.negative_buckets.len(), "{name}");

        let r = a_copy.add(&comp);
        check_err(name, &r, exp_err_msg);
        has_res = true;
    }

    // Check that the warnings are correct.
    assert_eq!(exp_counter_reset_collision, counter_reset_collision, "{name}");
    assert_eq!(exp_nhcb_bounds_reconciled, nhcb_bounds_reconciled, "{name}");

    if let Some(expected_sum_copy) = expected_sum_copy.as_mut() {
        // Go dereferences a nil `res` here (a panic) when KahanAdd returned no compensation histogram.
        assert!(has_res, "{name}: no result");
        // adapted: Go's Add returns the receiver as `res`; in Rust the result is `a_copy` itself.
        let res = &mut a_copy;
        res.compact(0);
        expected_sum_copy.compact(0);

        assert_eq!(*expected_sum_copy, *res, "{name}");

        // Has it also happened in-place?
        assert_eq!(*expected_sum_copy, a_copy, "{name}");

        // Check that the argument was not mutated.
        assert_eq!(*b, b_copy, "{name}");
    }
}

#[test]
fn test_kahan_add_with_comp_histogram() {
    #[derive(Default)]
    struct Case {
        name: &'static str,
        in1: Option<FloatHistogram>,
        comp: Option<FloatHistogram>,
        in2: Option<FloatHistogram>,
        expected_sum: Option<FloatHistogram>,
        exp_err_msg: &'static str,
        exp_counter_reset_collision: bool,
        exp_nhcb_bounds_reconciled: bool,
    }
    let cases = vec![
        Case {
            name: "larger zero bucket in first histogram",
            in1: Some(FloatHistogram {
                zero_threshold: 1.0,
                zero_count: 17.0,
                count: 21.0,
                sum: 1.234,
                positive_spans: vec![Span::new(1, 2), Span::new(0, 3)],
                positive_buckets: vec![2.0, 3.0, 6.0, 2.0, 5.0],
                negative_spans: vec![Span::new(4, 2), Span::new(1, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0],
                ..Default::default()
            }),
            comp: Some(FloatHistogram {
                zero_threshold: 1.0,
                positive_spans: vec![Span::new(1, 2), Span::new(0, 3)],
                positive_buckets: vec![0.02, 0.03, 0.06, 0.02, 0.05],
                negative_spans: vec![Span::new(4, 2), Span::new(1, 2)],
                negative_buckets: vec![0.01, 0.01, 0.04, 0.04],
                ..Default::default()
            }),
            in2: Some(FloatHistogram {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
                ..Default::default()
            }),
            expected_sum: Some(FloatHistogram {
                zero_threshold: 1.0,
                zero_count: 29.0,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(1, 2), Span::new(0, 3)],
                positive_buckets: vec![2.02, 6.03, 10.06, 9.02, 5.05],
                negative_spans: vec![Span::new(3, 3), Span::new(1, 3)],
                negative_buckets: vec![3.0, 2.01, 1.01, 4.04, 9.04, 6.0],
                ..Default::default()
            }),
            exp_err_msg: "",
            exp_counter_reset_collision: false,
            exp_nhcb_bounds_reconciled: false,
        },
        Case {
            name: "smaller zero bucket in first histogram",
            in1: Some(FloatHistogram {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 40.0,
                sum: 2.345,
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 3)],
                positive_buckets: vec![1.0, 2.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
                ..Default::default()
            }),
            comp: Some(FloatHistogram {
                zero_threshold: 0.01,
                zero_count: 0.0,
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 3)],
                positive_buckets: vec![0.02, 0.03, 0.06, 0.07, 0.05],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![0.01, 0.01, 0.04, 0.04],
                ..Default::default()
            }),
            in2: Some(FloatHistogram {
                zero_threshold: 1.0,
                zero_count: 17.0,
                count: 11.0,
                sum: 1.234,
                positive_spans: vec![Span::new(1, 2), Span::new(0, 3)],
                positive_buckets: vec![2.0, 3.0, 6.0, 2.0, 5.0],
                negative_spans: vec![Span::new(4, 2), Span::new(1, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0],
                ..Default::default()
            }),
            expected_sum: Some(FloatHistogram {
                zero_threshold: 1.0,
                zero_count: 31.05,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(1, 5)],
                positive_buckets: vec![2.0, 6.06, 10.07, 9.05, 5.0],
                negative_spans: vec![Span::new(3, 3), Span::new(1, 3)],
                negative_buckets: vec![3.01, 2.01, 1.0, 4.0, 9.04, 6.04],
                ..Default::default()
            }),
            exp_err_msg: "",
            exp_counter_reset_collision: false,
            exp_nhcb_bounds_reconciled: false,
        },
        Case {
            name: "first histogram contains zero buckets and Compact is called",
            in1: Some(FloatHistogram {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 1), Span::new(1, 3)],
                positive_buckets: vec![1.0, 3.0, 3.0, 0.0, 7.0, -6.0],
                ..Default::default()
            }),
            comp: Some(FloatHistogram {
                zero_threshold: 0.01,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 1), Span::new(1, 3)],
                positive_buckets: vec![7.0, 2.0, 0.03, 0.0, 0.05, 0.06],
                ..Default::default()
            }),
            in2: Some(FloatHistogram {
                zero_threshold: 1.0,
                zero_count: 17.0,
                count: 21.0,
                sum: 1.234,
                positive_spans: vec![Span::new(1, 2), Span::new(1, 2)],
                positive_buckets: vec![2.0, 3.0, 2.0, 5.0],
                ..Default::default()
            }),
            expected_sum: Some(FloatHistogram {
                zero_threshold: 1.0,
                zero_count: 41.0,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(1, 2), Span::new(1, 2)],
                positive_buckets: vec![5.03, 3.0, 9.05, -0.94],
                ..Default::default()
            }),
            exp_err_msg: "",
            exp_counter_reset_collision: false,
            exp_nhcb_bounds_reconciled: false,
        },
        Case {
            name: "reduce resolution",
            in1: Some(FloatHistogram {
                schema: 2,
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 1), Span::new(1, 3)],
                positive_buckets: vec![1.0, 3.0, 1e100, 0.0, 7.0, -6.0],
                ..Default::default()
            }),
            comp: Some(FloatHistogram {
                schema: 2,
                zero_threshold: 0.01,
                zero_count: 1.0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 1), Span::new(1, 3)],
                positive_buckets: vec![7.0, 2.0, 0.03, 0.0, 0.05, 0.06],
                ..Default::default()
            }),
            in2: Some(FloatHistogram {
                schema: 1,
                zero_threshold: 1.0,
                zero_count: 17.0,
                count: 21.0,
                sum: 1.234,
                positive_spans: vec![Span::new(1, 2), Span::new(1, 2)],
                positive_buckets: vec![-1e100, 3.0, 2.0, 5.0],
                ..Default::default()
            }),
            expected_sum: Some(FloatHistogram {
                schema: 1,
                zero_threshold: 1.0,
                zero_count: 42.0,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(1, 5)],
                positive_buckets: vec![0.03, 10.05, -5.94, 2.0, 5.0],
                ..Default::default()
            }),
            exp_err_msg: "",
            exp_counter_reset_collision: false,
            exp_nhcb_bounds_reconciled: false,
        },
        Case {
            name: "reduce resolution of 'other' histogram",
            in1: Some(FloatHistogram {
                schema: 0,
                zero_threshold: 1.0,
                zero_count: 17.0,
                count: 21.0,
                sum: 1.234,
                positive_spans: vec![Span::new(1, 2), Span::new(1, 2)],
                positive_buckets: vec![2.0, 3.0, 2.0, 5.0],
                ..Default::default()
            }),
            comp: Some(FloatHistogram {
                schema: 0,
                zero_threshold: 1.0,
                zero_count: 1.0,
                positive_spans: vec![Span::new(1, 2), Span::new(1, 2)],
                positive_buckets: vec![17.0, 2.0, 0.03, 0.0],
                ..Default::default()
            }),
            in2: Some(FloatHistogram {
                schema: 2,
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                positive_spans: vec![Span::new(-2, 3), Span::new(1, 1), Span::new(1, 3)],
                positive_buckets: vec![1e100, 4.1, -1e100, 2.1, 0.0, 7.0, -6.0],
                ..Default::default()
            }),
            expected_sum: Some(FloatHistogram {
                schema: 0,
                zero_threshold: 1.0,
                zero_count: 33.1,
                count: 51.0,
                sum: 3.579,
                positive_spans: vec![Span::new(1, 2), Span::new(1, 2)],
                positive_buckets: vec![21.1, 6.0, 2.03, 5.0],
                ..Default::default()
            }),
            exp_err_msg: "",
            exp_counter_reset_collision: false,
            exp_nhcb_bounds_reconciled: false,
        },
        Case {
            name: "warn on counter reset hint collision",
            in1: Some(FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                counter_reset_hint: CounterResetHint::CounterReset,
                ..Default::default()
            }),
            in2: Some(FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                counter_reset_hint: CounterResetHint::NotCounterReset,
                ..Default::default()
            }),
            exp_err_msg: "",
            exp_counter_reset_collision: true,
            exp_nhcb_bounds_reconciled: false,
            ..Default::default()
        },
    ];

    for c in &cases {
        test_histogram_kahan_add(
            c.name,
            c.in1.as_ref().unwrap(),
            c.comp.as_ref(),
            c.in2.as_ref().unwrap(),
            c.expected_sum.as_ref(),
            c.exp_err_msg,
            c.exp_counter_reset_collision,
            c.exp_nhcb_bounds_reconciled,
        );
    }
}

#[test]
fn test_float_histogram_sub() {
    // This has fewer test cases than TestFloatHistogramAdd because Add and
    // Sub share most of the trickier code.
    #[derive(Default)]
    struct Case {
        name: &'static str,
        in1: Option<FloatHistogram>,
        in2: Option<FloatHistogram>,
        expected: Option<FloatHistogram>,
        exp_err_msg: &'static str,
        exp_counter_reset_collision: bool,
        exp_nhcb_bounds_reconciled: bool,
    }
    let cases = vec![
        Case {
            name: "same bucket layout",
            in1: Some(FloatHistogram {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 23.0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
                ..Default::default()
            }),
            in2: Some(FloatHistogram {
                zero_threshold: 0.01,
                zero_count: 8.0,
                count: 21.0,
                sum: 12.0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![0.0, 0.0, 2.0, 3.0, 6.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![1.0, 1.0, 4.0, 4.0],
                ..Default::default()
            }),
            expected: Some(FloatHistogram {
                zero_threshold: 0.01,
                zero_count: 3.0,
                count: 9.0,
                sum: 11.0,
                positive_spans: vec![Span::new(-2, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 1.0, 1.0, 1.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![2.0, 0.0, 1.0, 2.0],
                ..Default::default()
            }),
            ..Default::default()
        },
        Case {
            name: "schema change",
            in1: Some(FloatHistogram {
                zero_threshold: 0.01,
                zero_count: 8.0,
                count: 59.0,
                sum: 1.234,
                schema: 0,
                positive_spans: vec![Span::new(-2, 5), Span::new(0, 3)],
                positive_buckets: vec![2.0, 5.0, 4.0, 2.0, 3.0, 6.0, 7.0, 5.0],
                negative_spans: vec![Span::new(3, 3), Span::new(1, 3)],
                negative_buckets: vec![4.0, 10.0, 1.0, 4.0, 14.0, 7.0],
                ..Default::default()
            }),
            in2: Some(FloatHistogram {
                zero_threshold: 0.01,
                zero_count: 2.0,
                count: 19.0,
                sum: 0.345,
                schema: 1,
                positive_spans: vec![Span::new(-4, 3), Span::new(5, 5)],
                positive_buckets: vec![1.0, 0.0, 0.0, 1.0, 2.0, 2.0, 3.0, 4.0],
                negative_spans: vec![Span::new(6, 3), Span::new(6, 4)],
                negative_buckets: vec![3.0, 0.5, 0.5, 2.0, 3.0, 2.0, 4.0],
                ..Default::default()
            }),
            expected: Some(FloatHistogram {
                zero_threshold: 0.01,
                zero_count: 6.0,
                count: 40.0,
                sum: 0.889,
                positive_spans: vec![Span::new(-2, 5), Span::new(0, 3)],
                positive_buckets: vec![1.0, 5.0, 4.0, 2.0, 2.0, 2.0, 0.0, 5.0],
                negative_spans: vec![Span::new(3, 3), Span::new(1, 3)],
                negative_buckets: vec![1.0, 9.0, 1.0, 4.0, 9.0, 1.0],
                ..Default::default()
            }),
            ..Default::default()
        },
        Case {
            name: "same custom bucket layout",
            in1: Some(FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 15.0,
                sum: 23.0,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0]),
                ..Default::default()
            }),
            in2: Some(FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 11.0,
                sum: 12.0,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![0.0, 0.0, 2.0, 3.0, 6.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0]),
                ..Default::default()
            }),
            expected: Some(FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 4.0,
                sum: 11.0,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![1.0, 0.0, 1.0, 1.0, 1.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0]),
                ..Default::default()
            }),
            ..Default::default()
        },
        Case {
            name: "different custom bucket layout -with intersection and rollup",
            in1: Some(FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 220.0,
                sum: 9.9,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 1), Span::new(0, 2)],
                positive_buckets: vec![10.0, 20.0, 40.0, 50.0, 100.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0]),
                counter_reset_hint: CounterResetHint::GaugeType,
                ..Default::default()
            }),
            in2: Some(FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 6.0,
                sum: 4.4,
                positive_spans: vec![Span::new(0, 1), Span::new(1, 1)],
                positive_buckets: vec![1.0, 5.0],
                custom_values: custom_values(&[2.0, 4.0]),
                counter_reset_hint: CounterResetHint::GaugeType,
                ..Default::default()
            }),
            expected: Some(FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 214.0,
                sum: 5.5,
                positive_spans: vec![Span::new(0, 3)],
                positive_buckets: vec![10.0 + 20.0 - 1.0, 40.0, 50.0 + 100.0 - 5.0],
                custom_values: custom_values(&[2.0, 4.0]),
                counter_reset_hint: CounterResetHint::GaugeType,
                ..Default::default()
            }),
            exp_nhcb_bounds_reconciled: true,
            ..Default::default()
        },
        Case {
            name: "mix exponential and custom buckets histograms",
            in1: Some(FloatHistogram {
                zero_threshold: 0.01,
                zero_count: 8.0,
                count: 59.0,
                sum: 1.234,
                schema: 0,
                positive_spans: vec![Span::new(-2, 5), Span::new(0, 3)],
                positive_buckets: vec![2.0, 5.0, 4.0, 2.0, 3.0, 6.0, 7.0, 5.0],
                negative_spans: vec![Span::new(3, 3), Span::new(1, 3)],
                negative_buckets: vec![4.0, 10.0, 1.0, 4.0, 14.0, 7.0],
                ..Default::default()
            }),
            in2: Some(FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 11.0,
                sum: 12.0,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![0.0, 0.0, 2.0, 3.0, 6.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]),
                ..Default::default()
            }),
            exp_err_msg:
                "cannot apply this operation on histograms with a mix of exponential and custom bucket schemas",
            ..Default::default()
        },
        Case {
            name: "warn on counter reset hint collision",
            in1: Some(FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                counter_reset_hint: CounterResetHint::CounterReset,
                ..Default::default()
            }),
            in2: Some(FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                counter_reset_hint: CounterResetHint::NotCounterReset,
                ..Default::default()
            }),
            exp_err_msg: "",
            exp_counter_reset_collision: true,
            ..Default::default()
        },
    ];

    for c in &cases {
        eprintln!("case: {}", c.name);
        let in1 = c.in1.as_ref().unwrap();
        let in2 = c.in2.as_ref().unwrap();
        test_float_histogram_sub_helper(
            in1,
            in2,
            c.expected.as_ref(),
            c.exp_err_msg,
            c.exp_counter_reset_collision,
            c.exp_nhcb_bounds_reconciled,
        );

        let expected_negative = c.expected.as_ref().map(|e| {
            let mut n = e.copy();
            n.mul(-1.0);
            // Mul(-1) sets the counter reset hint to
            // GaugeType, but we want to retain the original
            // counter reset hint for this test.
            n.counter_reset_hint = e.counter_reset_hint;
            n
        });
        test_float_histogram_sub_helper(
            in2,
            in1,
            expected_negative.as_ref(),
            c.exp_err_msg,
            c.exp_counter_reset_collision,
            c.exp_nhcb_bounds_reconciled,
        );
    }
}

/// Go `testFloatHistogramSub`.
fn test_float_histogram_sub_helper(
    a: &FloatHistogram,
    b: &FloatHistogram,
    expected: Option<&FloatHistogram>,
    exp_err_msg: &str,
    exp_counter_reset_collision: bool,
    exp_nhcb_bounds_reconciled: bool,
) {
    assert!(a.validate().is_ok(), "a: {:?}", a.validate());
    assert!(b.validate().is_ok(), "b: {:?}", b.validate());

    let mut a_copy = a.copy();
    let b_copy = b.copy();
    let mut expected_copy = expected.map(|e| e.copy());

    let res = a_copy.sub(&b_copy);
    if !exp_err_msg.is_empty() {
        assert_eq!(res.as_ref().err().map(|e| e.to_string()).as_deref(), Some(exp_err_msg));
    } else {
        assert!(res.is_ok(), "unexpected error: {:?}", res.as_ref().err());
    }
    let (counter_reset_collision, nhcb_bounds_reconciled) = res.unwrap_or((false, false));

    if let Some(expected_copy) = expected_copy.as_mut() {
        // adapted: Go's `res` is the receiver `aCopy` itself (Sub works in place and returns it).
        a_copy.compact(0);
        expected_copy.compact(0);
        let res = &a_copy;

        assert_eq!(*expected_copy, *res);

        // Has it also happened in-place?
        assert_eq!(*expected_copy, a_copy);

        // Check that the argument was not mutated.
        assert_eq!(*b, b_copy);

        // Check that the warnings are correct.
        assert_eq!(exp_counter_reset_collision, counter_reset_collision);
        assert_eq!(exp_nhcb_bounds_reconciled, nhcb_bounds_reconciled);
    }
}

#[test]
fn test_float_histogram_copy_to_schema() {
    struct Case {
        name: &'static str,
        target_schema: i32,
        input: FloatHistogram,
        expected: FloatHistogram,
    }
    let cases = vec![
        Case {
            name: "no schema change",
            target_schema: 1,
            input: FloatHistogram {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                schema: 1,
                positive_spans: vec![Span::new(-4, 3), Span::new(5, 5)],
                positive_buckets: vec![1.0, 0.0, 0.0, 3.0, 2.0, 2.0, 3.0, 4.0],
                negative_spans: vec![Span::new(6, 3), Span::new(6, 4)],
                negative_buckets: vec![3.0, 0.5, 0.5, 2.0, 3.0, 2.0, 4.0],
                ..Default::default()
            },
            expected: FloatHistogram {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                schema: 1,
                positive_spans: vec![Span::new(-4, 3), Span::new(5, 5)],
                positive_buckets: vec![1.0, 0.0, 0.0, 3.0, 2.0, 2.0, 3.0, 4.0],
                negative_spans: vec![Span::new(6, 3), Span::new(6, 4)],
                negative_buckets: vec![3.0, 0.5, 0.5, 2.0, 3.0, 2.0, 4.0],
                ..Default::default()
            },
        },
        Case {
            name: "schema change",
            target_schema: 0,
            input: FloatHistogram {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                schema: 1,
                positive_spans: vec![Span::new(-4, 3), Span::new(5, 5)],
                positive_buckets: vec![1.0, 0.0, 0.0, 3.0, 2.0, 2.0, 3.0, 4.0],
                negative_spans: vec![Span::new(6, 3), Span::new(6, 4)],
                negative_buckets: vec![3.0, 0.5, 0.5, 2.0, 3.0, 2.0, 4.0],
                ..Default::default()
            },
            expected: FloatHistogram {
                zero_threshold: 0.01,
                zero_count: 11.0,
                count: 30.0,
                sum: 2.345,
                schema: 0,
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 3)],
                positive_buckets: vec![1.0, 0.0, 3.0, 4.0, 7.0],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.0, 1.0, 5.0, 6.0],
                ..Default::default()
            },
        },
        Case {
            name: "no schema change for custom buckets",
            target_schema: CUSTOM_BUCKETS_SCHEMA,
            input: FloatHistogram {
                count: 30.0,
                sum: 2.345,
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 3), Span::new(5, 5)],
                positive_buckets: vec![1.0, 0.0, 0.0, 3.0, 2.0, 2.0, 3.0, 4.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
                ..Default::default()
            },
            expected: FloatHistogram {
                count: 30.0,
                sum: 2.345,
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 3), Span::new(5, 5)],
                positive_buckets: vec![1.0, 0.0, 0.0, 3.0, 2.0, 2.0, 3.0, 4.0],
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
                ..Default::default()
            },
        },
    ];

    for c in &cases {
        eprintln!("case: {}", c.name);
        for hint in [
            CounterResetHint::UnknownCounterReset,
            CounterResetHint::CounterReset,
            CounterResetHint::NotCounterReset,
            CounterResetHint::GaugeType,
        ] {
            let mut input = c.input.copy();
            input.counter_reset_hint = hint;
            let mut expected = c.expected.copy();
            expected.counter_reset_hint = hint;
            let in_copy = input.copy();
            let res = input.copy_to_schema(c.target_schema);
            assert!(res.is_ok(), "unexpected error: {:?}", res.as_ref().err());
            assert_eq!(expected, res.unwrap());
            // Check that the receiver histogram was not mutated:
            assert_eq!(in_copy, input);
        }
    }
}

type Operation = fn(&mut FloatHistogram, &mut FloatHistogram) -> Result<(), Error>;

/// Go `TestFloatHistogramInvalidBucketLayoutInResolutionReduction`: checks that Add, Sub, and CopyToSchema return
/// an error instead of panicking when the histogram whose resolution they reduce has an invalid bucket layout, as
/// can happen with invalid stored data, and that they leave both histograms unmodified.
#[test]
fn test_float_histogram_invalid_bucket_layout_in_resolution_reduction() {
    let lower_schema = FloatHistogram {
        schema: 0,
        count: 5.0,
        sum: 10.0,
        positive_spans: vec![Span::new(0, 2)],
        positive_buckets: vec![2.0, 3.0],
        ..Default::default()
    };

    struct Case {
        name: &'static str,
        // invalid has a higher schema than lowerSchema, so every operation below
        // reduces its resolution.
        invalid: FloatHistogram,
        exp_err: ErrorKind,
    }
    let cases = vec![
        Case {
            name: "positive span with a negative offset",
            invalid: FloatHistogram {
                schema: 1,
                count: 5.0,
                sum: 10.0,
                positive_spans: vec![Span::new(0, 1), Span::new(-2, 1)],
                positive_buckets: vec![2.0, 3.0],
                ..Default::default()
            },
            exp_err: ErrorKind::HistogramSpanNegativeOffset,
        },
        Case {
            name: "negative span with a negative offset",
            invalid: FloatHistogram {
                schema: 1,
                count: 5.0,
                sum: 10.0,
                negative_spans: vec![Span::new(0, 1), Span::new(-2, 1)],
                negative_buckets: vec![2.0, 3.0],
                ..Default::default()
            },
            exp_err: ErrorKind::HistogramSpanNegativeOffset,
        },
        Case {
            name: "fewer buckets than the spans need",
            invalid: FloatHistogram {
                schema: 1,
                count: 5.0,
                sum: 10.0,
                positive_spans: vec![Span::new(0, 3)],
                positive_buckets: vec![2.0, 3.0],
                ..Default::default()
            },
            exp_err: ErrorKind::HistogramSpansBucketsMismatch,
        },
    ];

    let operations: Vec<(&str, Operation)> = vec![
        ("invalid.Add(valid)", |invalid, valid| invalid.add(valid).map(|_| ())),
        ("valid.Add(invalid)", |invalid, valid| valid.add(invalid).map(|_| ())),
        ("invalid.Sub(valid)", |invalid, valid| invalid.sub(valid).map(|_| ())),
        ("valid.Sub(invalid)", |invalid, valid| valid.sub(invalid).map(|_| ())),
        ("invalid.CopyToSchema(valid.Schema)", |invalid, valid| {
            invalid.copy_to_schema(valid.schema).map(|_| ())
        }),
    ];

    for c in &cases {
        for (op_name, op) in &operations {
            eprintln!("case: {}/{}", c.name, op_name);
            let mut invalid = c.invalid.copy();
            let mut valid = lower_schema.copy();

            let err = op(&mut invalid, &mut valid);
            assert!(
                matches!(&err, Err(e) if e.is(c.exp_err)),
                "{}/{}: got {:?}, want {:?}",
                c.name,
                op_name,
                err,
                c.exp_err
            );
            assert_eq!(c.invalid, invalid, "the invalid histogram must not be modified");
            assert_eq!(lower_schema, valid, "the valid histogram must not be modified");
        }
    }

    // Add and Sub check the bucket layout before they modify either histogram
    // and then reduce it with mustReduceResolution, while CopyToSchema returns
    // the error of reduceResolution directly. If the check and reduceResolution
    // drift apart, Add and Sub either panic or reject a layout they could
    // reduce. Try all small layouts, valid or not, to catch that.
    // Subtest: "all operations agree on which bucket layouts are invalid".
    let mut layouts: Vec<Vec<Span>> = vec![vec![]];
    let mut i = 0;
    while i < layouts.len() {
        if layouts[i].len() == 3 {
            i += 1;
            continue;
        }
        for offset in [-2i32, -1, 0, 1] {
            for length in [0u32, 1, 2] {
                let mut l = layouts[i].clone();
                l.push(Span::new(offset, length));
                layouts.push(l);
            }
        }
        i += 1;
    }

    for spans in &layouts {
        for num_buckets in 0..=4usize {
            let buckets = vec![0.0f64; num_buckets];
            for negative in [false, true] {
                let mut h = FloatHistogram {
                    schema: 1,
                    count: num_buckets as f64,
                    sum: 10.0,
                    ..Default::default()
                };
                if negative {
                    h.negative_spans = spans.clone();
                    h.negative_buckets = buckets.clone();
                } else {
                    h.positive_spans = spans.clone();
                    h.positive_buckets = buckets.clone();
                }
                let exp_err = h.copy().copy_to_schema(lower_schema.schema).err();

                for (op_name, op) in &operations {
                    let res = catch_unwind(AssertUnwindSafe(|| op(&mut h.copy(), &mut lower_schema.copy())));
                    assert!(
                        res.is_ok(),
                        "{} with spans {:?} and {} buckets (negative: {}) panicked",
                        op_name,
                        spans,
                        num_buckets,
                        negative
                    );
                    let err = res.unwrap().err();
                    assert_eq!(
                        exp_err.is_some(),
                        err.is_some(),
                        "{} with spans {:?} and {} buckets (negative: {}): got error {:?}, CopyToSchema got {:?}",
                        op_name,
                        spans,
                        num_buckets,
                        negative,
                        err,
                        exp_err
                    );
                }
            }
        }
    }
}

#[test]
fn test_reverse_float_bucket_iterator() {
    let h = FloatHistogram {
        count: 405.0,
        zero_count: 102.0,
        zero_threshold: 0.001,
        sum: 1008.4,
        schema: 1,
        positive_spans: vec![
            Span::new(0, 4),
            Span::new(1, 0),
            Span::new(3, 3),
            Span::new(3, 0),
            Span::new(2, 0),
            Span::new(5, 3),
        ],
        positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0, 33.0],
        negative_spans: vec![
            Span::new(0, 3),
            Span::new(1, 0),
            Span::new(3, 0),
            Span::new(3, 4),
            Span::new(2, 0),
            Span::new(5, 3),
        ],
        negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0, 33.0],
        ..Default::default()
    };

    // Assuming that the regular iterator is correct.

    // Positive buckets.
    let mut exp_buckets: Vec<Bucket<f64>> = Vec::new();
    let mut act_buckets: Vec<Bucket<f64>> = Vec::new();
    let mut it = h.positive_bucket_iterator();
    while it.next() {
        // Append in reverse to check reversed list.
        exp_buckets.insert(0, it.at());
    }
    let mut it = h.positive_reverse_bucket_iterator();
    while it.next() {
        act_buckets.push(it.at());
    }
    assert!(!exp_buckets.is_empty());
    assert!(!act_buckets.is_empty());
    assert_eq!(exp_buckets, act_buckets);

    // Negative buckets.
    exp_buckets.clear();
    act_buckets.clear();
    let mut it = h.negative_bucket_iterator();
    while it.next() {
        // Append in reverse to check reversed list.
        exp_buckets.insert(0, it.at());
    }
    let mut it = h.negative_reverse_bucket_iterator();
    while it.next() {
        act_buckets.push(it.at());
    }
    assert!(!exp_buckets.is_empty());
    assert!(!act_buckets.is_empty());
    assert_eq!(exp_buckets, act_buckets);
}

#[derive(Default)]
struct AllIterCase {
    h: FloatHistogram,
    // To determine the expected buckets.
    include_neg: bool,
    include_zero: bool,
    include_pos: bool,
}

fn zero_bucket_of(h: &FloatHistogram) -> Bucket<f64> {
    Bucket {
        lower: -h.zero_threshold,
        upper: h.zero_threshold,
        lower_inclusive: true,
        upper_inclusive: true,
        count: h.zero_count,
        index: 0,
    }
}

#[test]
fn test_all_float_bucket_iterator() {
    type Case = AllIterCase;
    let cases = vec![
        Case {
            h: FloatHistogram {
                count: 405.0,
                zero_count: 102.0,
                zero_threshold: 0.001,
                sum: 1008.4,
                schema: 1,
                positive_spans: vec![
                    Span::new(0, 4),
                    Span::new(1, 0),
                    Span::new(3, 3),
                    Span::new(3, 0),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0, 33.0],
                negative_spans: vec![
                    Span::new(0, 3),
                    Span::new(1, 0),
                    Span::new(3, 0),
                    Span::new(3, 4),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0, 33.0],
                ..Default::default()
            },
            include_neg: true,
            include_zero: true,
            include_pos: true,
        },
        Case {
            h: FloatHistogram {
                count: 405.0,
                zero_count: 102.0,
                zero_threshold: 0.001,
                sum: 1008.4,
                schema: 1,
                negative_spans: vec![
                    Span::new(0, 3),
                    Span::new(1, 0),
                    Span::new(3, 0),
                    Span::new(3, 4),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0, 33.0],
                ..Default::default()
            },
            include_neg: true,
            include_zero: true,
            include_pos: false,
        },
        Case {
            h: FloatHistogram {
                count: 405.0,
                zero_count: 102.0,
                zero_threshold: 0.001,
                sum: 1008.4,
                schema: 1,
                positive_spans: vec![
                    Span::new(0, 4),
                    Span::new(1, 0),
                    Span::new(3, 3),
                    Span::new(3, 0),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0, 33.0],
                ..Default::default()
            },
            include_neg: false,
            include_zero: true,
            include_pos: true,
        },
        Case {
            h: FloatHistogram {
                count: 405.0,
                zero_count: 102.0,
                zero_threshold: 0.001,
                sum: 1008.4,
                schema: 1,
                ..Default::default()
            },
            include_neg: false,
            include_zero: true,
            include_pos: false,
        },
        Case {
            h: FloatHistogram {
                count: 405.0,
                zero_count: 0.0,
                zero_threshold: 0.001,
                sum: 1008.4,
                schema: 1,
                positive_spans: vec![
                    Span::new(0, 4),
                    Span::new(1, 0),
                    Span::new(3, 3),
                    Span::new(3, 0),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0, 33.0],
                negative_spans: vec![
                    Span::new(0, 3),
                    Span::new(1, 0),
                    Span::new(3, 0),
                    Span::new(3, 4),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0, 33.0],
                ..Default::default()
            },
            include_neg: true,
            include_zero: false,
            include_pos: true,
        },
        Case {
            h: FloatHistogram {
                count: 447.0,
                zero_count: 42.0,
                zero_threshold: 0.5,
                sum: 1008.4,
                schema: 0,
                positive_spans: vec![
                    Span::new(0, 4),
                    Span::new(1, 0),
                    Span::new(3, 3),
                    Span::new(3, 0),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0, 33.0],
                negative_spans: vec![
                    Span::new(0, 3),
                    Span::new(1, 0),
                    Span::new(3, 0),
                    Span::new(3, 4),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0, 33.0],
                ..Default::default()
            },
            include_neg: true,
            include_zero: true,
            include_pos: true,
        },
        Case {
            h: FloatHistogram {
                count: 447.0,
                zero_count: 42.0,
                zero_threshold: 0.6,
                sum: 1008.4,
                schema: 0,
                positive_spans: vec![
                    Span::new(0, 4),
                    Span::new(1, 0),
                    Span::new(3, 3),
                    Span::new(3, 0),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0, 33.0],
                negative_spans: vec![
                    Span::new(0, 3),
                    Span::new(1, 0),
                    Span::new(3, 0),
                    Span::new(3, 4),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0, 33.0],
                ..Default::default()
            },
            include_neg: true,
            include_zero: true,
            include_pos: true,
        },
    ];

    for (i, c) in cases.iter().enumerate() {
        eprintln!("case: {}", i);
        let mut exp_buckets: Vec<Bucket<f64>> = Vec::new();
        let mut act_buckets: Vec<Bucket<f64>> = Vec::new();

        if c.include_neg {
            let mut it = c.h.negative_reverse_bucket_iterator();
            while it.next() {
                let mut b = it.at();
                if c.include_zero && b.upper > -c.h.zero_threshold {
                    b.upper = -c.h.zero_threshold;
                }
                exp_buckets.push(b);
            }
        }
        if c.include_zero {
            exp_buckets.push(zero_bucket_of(&c.h));
        }
        if c.include_pos {
            let mut it = c.h.positive_bucket_iterator();
            while it.next() {
                let mut b = it.at();
                if c.include_zero && b.lower < c.h.zero_threshold {
                    b.lower = c.h.zero_threshold;
                }
                exp_buckets.push(b);
            }
        }

        let mut it = c.h.all_bucket_iterator();
        while it.next() {
            act_buckets.push(it.at());
        }

        assert_eq!(exp_buckets, act_buckets, "case {}", i);
    }
}

#[test]
fn test_all_reverse_float_bucket_iterator() {
    type Case = AllIterCase;
    let cases = vec![
        Case {
            h: FloatHistogram {
                count: 405.0,
                zero_count: 102.0,
                zero_threshold: 0.001,
                sum: 1008.4,
                schema: 1,
                positive_spans: vec![
                    Span::new(0, 4),
                    Span::new(1, 0),
                    Span::new(3, 3),
                    Span::new(3, 0),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0, 33.0],
                negative_spans: vec![
                    Span::new(0, 3),
                    Span::new(1, 0),
                    Span::new(3, 0),
                    Span::new(3, 4),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0, 33.0],
                ..Default::default()
            },
            include_neg: true,
            include_zero: true,
            include_pos: true,
        },
        Case {
            h: FloatHistogram {
                count: 405.0,
                zero_count: 102.0,
                zero_threshold: 0.001,
                sum: 1008.4,
                schema: 1,
                negative_spans: vec![
                    Span::new(0, 3),
                    Span::new(1, 0),
                    Span::new(3, 0),
                    Span::new(3, 4),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0, 33.0],
                ..Default::default()
            },
            include_neg: true,
            include_zero: true,
            include_pos: false,
        },
        Case {
            h: FloatHistogram {
                count: 405.0,
                zero_count: 102.0,
                zero_threshold: 0.001,
                sum: 1008.4,
                schema: 1,
                positive_spans: vec![
                    Span::new(0, 4),
                    Span::new(1, 0),
                    Span::new(3, 3),
                    Span::new(3, 0),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0, 33.0],
                ..Default::default()
            },
            include_neg: false,
            include_zero: true,
            include_pos: true,
        },
        Case {
            h: FloatHistogram {
                count: 405.0,
                zero_count: 102.0,
                zero_threshold: 0.001,
                sum: 1008.4,
                schema: 1,
                ..Default::default()
            },
            include_neg: false,
            include_zero: true,
            include_pos: false,
        },
        Case {
            h: FloatHistogram {
                count: 405.0,
                zero_count: 0.0,
                zero_threshold: 0.001,
                sum: 1008.4,
                schema: 1,
                positive_spans: vec![
                    Span::new(0, 4),
                    Span::new(1, 0),
                    Span::new(3, 3),
                    Span::new(3, 0),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0, 33.0],
                negative_spans: vec![
                    Span::new(0, 3),
                    Span::new(1, 0),
                    Span::new(3, 0),
                    Span::new(3, 4),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0, 33.0],
                ..Default::default()
            },
            include_neg: true,
            include_zero: false,
            include_pos: true,
        },
        Case {
            h: FloatHistogram {
                count: 447.0,
                zero_count: 42.0,
                zero_threshold: 0.5,
                sum: 1008.4,
                schema: 0,
                positive_spans: vec![
                    Span::new(0, 4),
                    Span::new(1, 0),
                    Span::new(3, 3),
                    Span::new(3, 0),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0, 33.0],
                negative_spans: vec![
                    Span::new(0, 3),
                    Span::new(1, 0),
                    Span::new(3, 0),
                    Span::new(3, 4),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0, 33.0],
                ..Default::default()
            },
            include_neg: true,
            include_zero: true,
            include_pos: true,
        },
        Case {
            h: FloatHistogram {
                count: 447.0,
                zero_count: 42.0,
                zero_threshold: 0.6,
                sum: 1008.4,
                schema: 0,
                positive_spans: vec![
                    Span::new(0, 4),
                    Span::new(1, 0),
                    Span::new(3, 3),
                    Span::new(3, 0),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0, 33.0],
                negative_spans: vec![
                    Span::new(0, 3),
                    Span::new(1, 0),
                    Span::new(3, 0),
                    Span::new(3, 4),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0, 33.0],
                ..Default::default()
            },
            include_neg: true,
            include_zero: true,
            include_pos: true,
        },
        Case {
            h: FloatHistogram {
                count: 405.0,
                zero_count: 102.0,
                zero_threshold: 0.001,
                sum: 1008.4,
                schema: 1,
                positive_spans: vec![
                    Span::new(0, 4),
                    Span::new(1, 0),
                    Span::new(3, 3),
                    Span::new(3, 0),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0],
                negative_spans: vec![
                    Span::new(0, 3),
                    Span::new(1, 0),
                    Span::new(3, 0),
                    Span::new(3, 4),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0],
                ..Default::default()
            },
            include_neg: true,
            include_zero: true,
            include_pos: true,
        },
        Case {
            h: FloatHistogram {
                count: 447.0,
                zero_count: 42.0,
                zero_threshold: 0.6,
                sum: 1008.4,
                schema: 0,
                positive_spans: vec![
                    Span::new(0, 4),
                    Span::new(1, 0),
                    Span::new(3, 3),
                    Span::new(3, 0),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0, 33.0, 42.0],
                negative_spans: vec![
                    Span::new(0, 3),
                    Span::new(1, 0),
                    Span::new(3, 0),
                    Span::new(3, 4),
                    Span::new(2, 0),
                    Span::new(5, 3),
                ],
                negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0, 33.0, 42.0],
                ..Default::default()
            },
            include_neg: true,
            include_zero: true,
            include_pos: true,
        },
    ];

    for (i, c) in cases.iter().enumerate() {
        eprintln!("case: {}", i);
        let mut exp_buckets: Vec<Bucket<f64>> = Vec::new();
        let mut act_buckets: Vec<Bucket<f64>> = Vec::new();

        if c.include_pos {
            let mut it = c.h.positive_reverse_bucket_iterator();
            while it.next() {
                let mut b = it.at();
                if c.include_zero && b.lower < c.h.zero_threshold {
                    b.lower = c.h.zero_threshold;
                }
                exp_buckets.push(b);
            }
        }
        if c.include_zero {
            exp_buckets.push(zero_bucket_of(&c.h));
        }
        if c.include_neg {
            let mut it = c.h.negative_bucket_iterator();
            while it.next() {
                let mut b = it.at();
                if c.include_zero && b.upper > -c.h.zero_threshold {
                    b.upper = -c.h.zero_threshold;
                }
                exp_buckets.push(b);
            }
        }

        let mut it = c.h.all_reverse_bucket_iterator();
        while it.next() {
            act_buckets.push(it.at());
        }

        assert_eq!(exp_buckets, act_buckets, "case {}", i);
    }
}

#[test]
fn test_float_bucket_iterator_target_schema() {
    struct Case {
        h: FloatHistogram,
        exp_positive_buckets: Vec<Bucket<f64>>,
        exp_negative_buckets: Vec<Bucket<f64>>,
    }
    let cases: Vec<(&str, Case)> = vec![
        (
            "regular",
            Case {
                h: FloatHistogram {
                    count: 405.0,
                    sum: 1008.4,
                    schema: 1,
                    positive_spans: vec![Span::new(0, 4), Span::new(1, 3), Span::new(2, 3)],
                    positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0, 33.0],
                    negative_spans: vec![Span::new(0, 3), Span::new(7, 4), Span::new(1, 3)],
                    negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0, 33.0],
                    ..Default::default()
                },
                exp_positive_buckets: vec![
                    Bucket::<f64> {
                        lower: 0.25,
                        upper: 1.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 100.0,
                        index: 0,
                    },
                    Bucket::<f64> {
                        lower: 1.0,
                        upper: 4.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 522.0,
                        index: 1,
                    },
                    Bucket::<f64> {
                        lower: 4.0,
                        upper: 16.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 68.0,
                        index: 2,
                    },
                    Bucket::<f64> {
                        lower: 16.0,
                        upper: 64.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 322.0,
                        index: 3,
                    },
                ],
                exp_negative_buckets: vec![
                    Bucket::<f64> {
                        lower: -1.0,
                        upper: -0.25,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 10.0,
                        index: 0,
                    },
                    Bucket::<f64> {
                        lower: -4.0,
                        upper: -1.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 1264.0,
                        index: 1,
                    },
                    Bucket::<f64> {
                        lower: -64.0,
                        upper: -16.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 184.0,
                        index: 3,
                    },
                    Bucket::<f64> {
                        lower: -256.0,
                        upper: -64.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 791.0,
                        index: 4,
                    },
                    Bucket::<f64> {
                        lower: -1024.0,
                        upper: -256.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 33.0,
                        index: 5,
                    },
                ],
            },
        ),
        (
            "missing buckets",
            Case {
                h: FloatHistogram {
                    count: 405.0,
                    sum: 1008.4,
                    schema: 1,
                    positive_spans: vec![Span::new(0, 4), Span::new(1, 3), Span::new(2, 3)],
                    positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0],
                    negative_spans: vec![Span::new(0, 3), Span::new(7, 4), Span::new(1, 3)],
                    negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0],
                    ..Default::default()
                },
                exp_positive_buckets: vec![
                    Bucket::<f64> {
                        lower: 0.25,
                        upper: 1.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 100.0,
                        index: 0,
                    },
                    Bucket::<f64> {
                        lower: 1.0,
                        upper: 4.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 522.0,
                        index: 1,
                    },
                    Bucket::<f64> {
                        lower: 4.0,
                        upper: 16.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 68.0,
                        index: 2,
                    },
                    Bucket::<f64> {
                        lower: 16.0,
                        upper: 64.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 289.0,
                        index: 3,
                    },
                ],
                exp_negative_buckets: vec![
                    Bucket::<f64> {
                        lower: -1.0,
                        upper: -0.25,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 10.0,
                        index: 0,
                    },
                    Bucket::<f64> {
                        lower: -4.0,
                        upper: -1.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 1264.0,
                        index: 1,
                    },
                    Bucket::<f64> {
                        lower: -64.0,
                        upper: -16.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 184.0,
                        index: 3,
                    },
                    Bucket::<f64> {
                        lower: -256.0,
                        upper: -64.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 791.0,
                        index: 4,
                    },
                ],
            },
        ),
        (
            "spurious bucket",
            Case {
                h: FloatHistogram {
                    count: 405.0,
                    sum: 1008.4,
                    schema: 1,
                    positive_spans: vec![Span::new(0, 4), Span::new(1, 3), Span::new(2, 3)],
                    positive_buckets: vec![100.0, 344.0, 123.0, 55.0, 3.0, 63.0, 2.0, 54.0, 235.0, 33.0, 42.0],
                    negative_spans: vec![Span::new(0, 3), Span::new(7, 4), Span::new(1, 3)],
                    negative_buckets: vec![10.0, 34.0, 1230.0, 54.0, 67.0, 63.0, 2.0, 554.0, 235.0, 33.0, 42.0],
                    ..Default::default()
                },
                exp_positive_buckets: vec![
                    Bucket::<f64> {
                        lower: 0.25,
                        upper: 1.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 100.0,
                        index: 0,
                    },
                    Bucket::<f64> {
                        lower: 1.0,
                        upper: 4.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 522.0,
                        index: 1,
                    },
                    Bucket::<f64> {
                        lower: 4.0,
                        upper: 16.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 68.0,
                        index: 2,
                    },
                    Bucket::<f64> {
                        lower: 16.0,
                        upper: 64.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 322.0,
                        index: 3,
                    },
                ],
                exp_negative_buckets: vec![
                    Bucket::<f64> {
                        lower: -1.0,
                        upper: -0.25,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 10.0,
                        index: 0,
                    },
                    Bucket::<f64> {
                        lower: -4.0,
                        upper: -1.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 1264.0,
                        index: 1,
                    },
                    Bucket::<f64> {
                        lower: -64.0,
                        upper: -16.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 184.0,
                        index: 3,
                    },
                    Bucket::<f64> {
                        lower: -256.0,
                        upper: -64.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 791.0,
                        index: 4,
                    },
                    Bucket::<f64> {
                        lower: -1024.0,
                        upper: -256.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 33.0,
                        index: 5,
                    },
                ],
            },
        ),
        (
            "no schema change",
            Case {
                h: FloatHistogram {
                    count: 405.0,
                    sum: 1008.4,
                    schema: -1,
                    positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    positive_buckets: vec![100.0, 522.0, 68.0, 322.0],
                    negative_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    negative_buckets: vec![100.0, 522.0, 68.0, 322.0],
                    ..Default::default()
                },
                exp_positive_buckets: vec![
                    Bucket::<f64> {
                        lower: 0.25,
                        upper: 1.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 100.0,
                        index: 0,
                    },
                    Bucket::<f64> {
                        lower: 1.0,
                        upper: 4.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 522.0,
                        index: 1,
                    },
                    Bucket::<f64> {
                        lower: 16.0,
                        upper: 64.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 68.0,
                        index: 3,
                    },
                    Bucket::<f64> {
                        lower: 64.0,
                        upper: 256.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 322.0,
                        index: 4,
                    },
                ],
                exp_negative_buckets: vec![
                    Bucket::<f64> {
                        lower: -1.0,
                        upper: -0.25,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 100.0,
                        index: 0,
                    },
                    Bucket::<f64> {
                        lower: -4.0,
                        upper: -1.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 522.0,
                        index: 1,
                    },
                    Bucket::<f64> {
                        lower: -64.0,
                        upper: -16.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 68.0,
                        index: 3,
                    },
                    Bucket::<f64> {
                        lower: -256.0,
                        upper: -64.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 322.0,
                        index: 4,
                    },
                ],
            },
        ),
        (
            "no schema change, missing bucket",
            Case {
                h: FloatHistogram {
                    count: 405.0,
                    sum: 1008.4,
                    schema: -1,
                    positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    positive_buckets: vec![100.0, 522.0, 68.0],
                    negative_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    negative_buckets: vec![100.0, 522.0, 68.0],
                    ..Default::default()
                },
                exp_positive_buckets: vec![
                    Bucket::<f64> {
                        lower: 0.25,
                        upper: 1.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 100.0,
                        index: 0,
                    },
                    Bucket::<f64> {
                        lower: 1.0,
                        upper: 4.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 522.0,
                        index: 1,
                    },
                    Bucket::<f64> {
                        lower: 16.0,
                        upper: 64.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 68.0,
                        index: 3,
                    },
                ],
                exp_negative_buckets: vec![
                    Bucket::<f64> {
                        lower: -1.0,
                        upper: -0.25,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 100.0,
                        index: 0,
                    },
                    Bucket::<f64> {
                        lower: -4.0,
                        upper: -1.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 522.0,
                        index: 1,
                    },
                    Bucket::<f64> {
                        lower: -64.0,
                        upper: -16.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 68.0,
                        index: 3,
                    },
                ],
            },
        ),
        (
            "no schema change, spurious bucket",
            Case {
                h: FloatHistogram {
                    count: 405.0,
                    sum: 1008.4,
                    schema: -1,
                    positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    positive_buckets: vec![100.0, 522.0, 68.0, 322.0, 42.0],
                    negative_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    negative_buckets: vec![100.0, 522.0, 68.0, 322.0, 42.0],
                    ..Default::default()
                },
                exp_positive_buckets: vec![
                    Bucket::<f64> {
                        lower: 0.25,
                        upper: 1.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 100.0,
                        index: 0,
                    },
                    Bucket::<f64> {
                        lower: 1.0,
                        upper: 4.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 522.0,
                        index: 1,
                    },
                    Bucket::<f64> {
                        lower: 16.0,
                        upper: 64.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 68.0,
                        index: 3,
                    },
                    Bucket::<f64> {
                        lower: 64.0,
                        upper: 256.0,
                        lower_inclusive: false,
                        upper_inclusive: true,
                        count: 322.0,
                        index: 4,
                    },
                ],
                exp_negative_buckets: vec![
                    Bucket::<f64> {
                        lower: -1.0,
                        upper: -0.25,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 100.0,
                        index: 0,
                    },
                    Bucket::<f64> {
                        lower: -4.0,
                        upper: -1.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 522.0,
                        index: 1,
                    },
                    Bucket::<f64> {
                        lower: -64.0,
                        upper: -16.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 68.0,
                        index: 3,
                    },
                    Bucket::<f64> {
                        lower: -256.0,
                        upper: -64.0,
                        lower_inclusive: true,
                        upper_inclusive: false,
                        count: 322.0,
                        index: 4,
                    },
                ],
            },
        ),
    ];
    for (tn, tc) in &cases {
        eprintln!("case: {}", tn);
        let mut it = tc.h.float_bucket_iterator(true, 0.0, -1);
        for (i, b) in tc.exp_positive_buckets.iter().enumerate() {
            assert!(it.next(), "positive iterator exhausted too early");
            assert_eq!(*b, it.at(), "bucket {}", i);
        }
        assert!(!it.next(), "positive iterator not exhausted");

        let mut it = tc.h.float_bucket_iterator(false, 0.0, -1);
        for (i, b) in tc.exp_negative_buckets.iter().enumerate() {
            assert!(it.next(), "negative iterator exhausted too early");
            assert_eq!(*b, it.at(), "bucket {}", i);
        }
        assert!(!it.next(), "negative iterator not exhausted");
    }
}

#[test]
fn test_float_custom_buckets_iterators() {
    struct Case {
        h: FloatHistogram,
        exp_positive_buckets: Vec<Bucket<f64>>,
    }
    let cases = vec![
        Case {
            h: FloatHistogram {
                count: 622.0,
                sum: 10008.4,
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 1), Span::new(1, 1)],
                positive_buckets: vec![100.0, 344.0, 123.0, 55.0],
                custom_values: custom_values(&[10.0, 25.0, 50.0, 100.0, 500.0]),
                ..Default::default()
            },
            exp_positive_buckets: vec![
                Bucket::<f64> {
                    lower: f64::NEG_INFINITY,
                    upper: 10.0,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    count: 100.0,
                    index: 0,
                },
                Bucket::<f64> {
                    lower: 10.0,
                    upper: 25.0,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    count: 344.0,
                    index: 1,
                },
                Bucket::<f64> {
                    lower: 50.0,
                    upper: 100.0,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    count: 123.0,
                    index: 3,
                },
                Bucket::<f64> {
                    lower: 500.0,
                    upper: f64::INFINITY,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    count: 55.0,
                    index: 5,
                },
            ],
        },
        Case {
            h: FloatHistogram {
                count: 622.0,
                sum: 10008.4,
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 1), Span::new(1, 1)],
                positive_buckets: vec![100.0, 344.0, 123.0, 55.0],
                custom_values: custom_values(&[-10.0, -5.0, 0.0, 10.0, 25.0]),
                ..Default::default()
            },
            exp_positive_buckets: vec![
                Bucket::<f64> {
                    lower: f64::NEG_INFINITY,
                    upper: -10.0,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    count: 100.0,
                    index: 0,
                },
                Bucket::<f64> {
                    lower: -10.0,
                    upper: -5.0,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    count: 344.0,
                    index: 1,
                },
                Bucket::<f64> {
                    lower: 0.0,
                    upper: 10.0,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    count: 123.0,
                    index: 3,
                },
                Bucket::<f64> {
                    lower: 25.0,
                    upper: f64::INFINITY,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    count: 55.0,
                    index: 5,
                },
            ],
        },
    ];

    for (case_idx, c) in cases.iter().enumerate() {
        eprintln!("case: {}", case_idx);
        {
            let length = c.exp_positive_buckets.len();

            let mut it = c.h.all_bucket_iterator();
            for (i, b) in c.exp_positive_buckets.iter().enumerate() {
                assert!(it.next(), "all bucket iterator exhausted too early");
                assert_eq!(*b, it.at(), "bucket {}", i);
            }
            assert!(!it.next(), "all bucket iterator not exhausted");

            let mut it = c.h.all_reverse_bucket_iterator();
            for j in 0..length {
                let i = length - j - 1;
                let b = c.exp_positive_buckets[i];
                assert!(it.next(), "all reverse bucket iterator exhausted too early");
                assert_eq!(b, it.at(), "bucket {}", i);
            }
            assert!(!it.next(), "all reverse bucket iterator not exhausted");

            let mut it = c.h.positive_bucket_iterator();
            for (i, b) in c.exp_positive_buckets.iter().enumerate() {
                assert!(it.next(), "positive bucket iterator exhausted too early");
                assert_eq!(*b, it.at(), "bucket {}", i);
            }
            assert!(!it.next(), "positive bucket iterator not exhausted");

            let mut it = c.h.positive_reverse_bucket_iterator();
            for j in 0..length {
                let i = length - j - 1;
                let b = c.exp_positive_buckets[i];
                assert!(it.next(), "positive reverse bucket iterator exhausted too early");
                assert_eq!(b, it.at(), "bucket {}", i);
            }
            assert!(!it.next(), "positive reverse bucket iterator not exhausted");

            let mut it = c.h.negative_bucket_iterator();
            assert!(!it.next(), "negative bucket iterator not exhausted");

            let mut it = c.h.negative_reverse_bucket_iterator();
            assert!(!it.next(), "negative reverse bucket iterator not exhausted");
        }
        {
            let mut it = c.h.float_bucket_iterator(true, 0.0, CUSTOM_BUCKETS_SCHEMA);
            for (i, b) in c.exp_positive_buckets.iter().enumerate() {
                assert!(it.next(), "positive iterator exhausted too early");
                assert_eq!(*b, it.at(), "bucket {}", i);
            }
            assert!(!it.next(), "positive iterator not exhausted");

            let mut it = c.h.float_bucket_iterator(false, 0.0, CUSTOM_BUCKETS_SCHEMA);
            assert!(!it.next(), "negative iterator not exhausted");
        }
    }
}

/// Go `TestFloatHistogramEquals`: tests FloatHistogram with float-specific cases that cannot be covered by
/// TestHistogramEquals.
#[test]
fn test_float_histogram_equals() {
    let h1 = FloatHistogram {
        schema: 3,
        count: 2.2,
        sum: 9.7,
        zero_threshold: 0.1,
        zero_count: 1.1,
        positive_buckets: vec![3.0],
        negative_buckets: vec![4.0],
        ..Default::default()
    };

    let equals = |h1: &FloatHistogram, h2: &FloatHistogram| {
        assert!(h1.equals(h2));
        assert!(h2.equals(h1));
    };
    let not_equals = |h1: &FloatHistogram, h2: &FloatHistogram| {
        assert!(!h1.equals(h2));
        assert!(!h2.equals(h1));
    };

    let h2 = h1.copy();
    equals(&h1, &h2);

    // Count is NaN (but not a StaleNaN).
    let mut h_count_nan = h1.copy();
    h_count_nan.count = f64::NAN;
    not_equals(&h1, &h_count_nan);
    equals(&h_count_nan, &h_count_nan);

    // ZeroCount is NaN (but not a StaleNaN).
    let mut h_zero_count_nan = h1.copy();
    h_zero_count_nan.zero_count = f64::NAN;
    not_equals(&h1, &h_zero_count_nan);
    equals(&h_zero_count_nan, &h_zero_count_nan);

    // Positive bucket value is NaN.
    let mut h_pos_bucket_nan = h1.copy();
    h_pos_bucket_nan.positive_buckets[0] = f64::NAN;
    not_equals(&h1, &h_pos_bucket_nan);
    equals(&h_pos_bucket_nan, &h_pos_bucket_nan);

    // Negative bucket value is NaN.
    let mut h_neg_bucket_nan = h1.copy();
    h_neg_bucket_nan.negative_buckets[0] = f64::NAN;
    not_equals(&h1, &h_neg_bucket_nan);
    equals(&h_neg_bucket_nan, &h_neg_bucket_nan);

    // Custom bounds are defined for exponential schema.
    let mut h_custom = h1.copy();
    h_custom.custom_values = custom_values(&[1.0, 2.0, 3.0]);
    equals(&h1, &h_custom);

    let cbh1 = FloatHistogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count: 2.2,
        sum: 9.7,
        positive_spans: vec![Span::new(0, 1)],
        positive_buckets: vec![3.0],
        custom_values: custom_values(&[1.0, 2.0, 3.0]),
        ..Default::default()
    };

    assert!(cbh1.validate().is_ok(), "{:?}", cbh1.validate());

    let cbh2 = cbh1.copy();
    equals(&cbh1, &cbh2);

    // Has different custom bounds for custom buckets schema.
    let mut cbh2 = cbh1.copy();
    cbh2.custom_values = custom_values(&[1.0, 2.0, 3.0, 4.0]);
    not_equals(&cbh1, &cbh2);

    // Has non-empty negative spans and buckets for custom buckets schema.
    let mut cbh2 = cbh1.copy();
    cbh2.negative_spans = vec![Span::new(0, 1)];
    cbh2.negative_buckets = vec![1.0];
    not_equals(&cbh1, &cbh2);

    // Has non-zero zero count and threshold for custom buckets schema.
    let mut cbh2 = cbh1.copy();
    cbh2.zero_threshold = 0.1;
    cbh2.zero_count = 10.0;
    not_equals(&cbh1, &cbh2);
}

#[test]
fn test_float_histogram_size() {
    // adapted: the expected sizes are Go's 64-bit struct and slice sizes, which `size()` reports as upstream does.
    struct Case {
        name: &'static str,
        fh: FloatHistogram,
        expected: usize,
    }
    let cases = vec![
        Case {
            name: "without spans and buckets",
            fh: FloatHistogram {
                counter_reset_hint: CounterResetHint::UnknownCounterReset,
                schema: 1,
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![],
                positive_buckets: vec![],
                negative_spans: vec![],
                negative_buckets: vec![],
                custom_values: None,
            },
            expected: 8 + 4 + 4 + 8 + 8 + 8 + 8 + 24 + 24 + 24 + 24 + 24,
        },
        Case {
            name: "complete struct",
            fh: FloatHistogram {
                counter_reset_hint: CounterResetHint::UnknownCounterReset,
                schema: 1,
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
                custom_values: None,
            },
            expected: 8
                + 4
                + 4
                + 8
                + 8
                + 8
                + 8
                + (24 + 2 * 4 + 2 * 4)
                + (24 + 2 * 4 + 2 * 4)
                + (24 + 4 * 8)
                + (24 + 4 * 8)
                + 24,
        },
        Case {
            name: "complete struct with custom buckets",
            fh: FloatHistogram {
                counter_reset_hint: CounterResetHint::UnknownCounterReset,
                schema: CUSTOM_BUCKETS_SCHEMA,
                zero_threshold: 0.0,
                zero_count: 0.0,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![],
                negative_buckets: vec![],
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
            },
            expected: 8 + 4 + 4 + 8 + 8 + 8 + 8 + (24 + 2 * 4 + 2 * 4) + (24 + 4 * 8) + 24 + 24 + (24 + 3 * 8),
        },
    ];

    for c in &cases {
        assert_eq!(c.expected, c.fh.size(), "{}", c.name);
    }
}

#[test]
fn test_float_histogram_string() {
    struct Case {
        name: &'static str,
        fh: FloatHistogram,
        expected: &'static str,
    }
    let cases = vec![
        Case {
            name: "exponential histogram",
            fh: FloatHistogram {
                schema: 1,
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
                ..Default::default()
            },
            expected: r#"{count:3493.3, sum:2.349209324e+06, [-22.62741699796952,-16):1000, [-16,-11.31370849898476):123400, [-4,-2.82842712474619):3, [-2.82842712474619,-2):3.1, [-0.01,0.01]:5.5, (0.35355339059327373,0.5]:1, (1,1.414213562373095]:3.3, (1.414213562373095,2]:4.2, (2,2.82842712474619]:0.1}"#,
        },
        Case {
            name: "custom buckets histogram",
            fh: FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(0, 1), Span::new(2, 4)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1, 5.0],
                custom_values: custom_values(&[1.0, 2.0, 5.0, 10.0, 15.0, 20.0]),
                ..Default::default()
            },
            expected: r#"{count:3493.3, sum:2.349209324e+06, [-Inf,1]:1, (5,10]:3.3, (10,15]:4.2, (15,20]:0.1, (20,+Inf]:5}"#,
        },
    ];

    for c in &cases {
        assert!(c.fh.validate().is_ok(), "{}: {:?}", c.name, c.fh.validate());
        assert_eq!(c.expected, c.fh.string(), "{}", c.name);
    }
}

#[test]
fn test_float_histogram_validate_negative_histogram() {
    struct Case {
        name: &'static str,
        fh: FloatHistogram,
    }
    let cases = vec![
        Case {
            name: "positive bucket with negative population",
            fh: FloatHistogram {
                schema: 1,
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, -4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
                ..Default::default()
            },
        },
        Case {
            name: "negative bucket with negative population",
            fh: FloatHistogram {
                schema: 1,
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, -3.0, 1.234e5, 1000.0],
                ..Default::default()
            },
        },
        Case {
            name: "negative count",
            fh: FloatHistogram {
                schema: 1,
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: -3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
                ..Default::default()
            },
        },
        Case {
            name: "zero bucket with negative population",
            fh: FloatHistogram {
                schema: 1,
                zero_threshold: 0.01,
                zero_count: -5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
                ..Default::default()
            },
        },
    ];

    for c in &cases {
        assert!(c.fh.validate().is_err(), "{}", c.name);
    }
}

#[test]
fn test_float_histogram_reduce_resolution() {
    #[derive(Default)]
    struct Case {
        origin: FloatHistogram,
        target_schema: i32,
        target: Option<FloatHistogram>,
        error_msg: &'static str,
    }
    let tcs: Vec<(&str, Case)> = vec![
        ("valid float histogram", Case { origin: FloatHistogram { schema: 0, positive_spans: vec![Span::new(-2, 4), Span::new(0, 0), Span::new(3, 2)], positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0], negative_spans: vec![Span::new(0, 4), Span::new(0, 0), Span::new(3, 2)], negative_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0], ..Default::default() }, target_schema: -1, target: Some(FloatHistogram { schema: -1, positive_spans: vec![Span::new(-1, 3), Span::new(1, 1)], positive_buckets: vec![1.0, 4.0, 2.0, 2.0], negative_spans: vec![Span::new(0, 3), Span::new(1, 1)], negative_buckets: vec![1.0, 4.0, 2.0, 2.0], ..Default::default() }), ..Default::default() }),
        ("not enough buckets", Case { origin: FloatHistogram { schema: 0, positive_spans: vec![Span::new(-2, 4), Span::new(0, 0), Span::new(3, 2)], positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0], ..Default::default() }, target_schema: -1, error_msg: "have 5 buckets but spans need more: histogram spans specify different number of buckets than provided", ..Default::default() }),
        ("too many buckets", Case { origin: FloatHistogram { schema: 0, positive_spans: vec![Span::new(-2, 4), Span::new(0, 0), Span::new(3, 2)], positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 5.0], ..Default::default() }, target_schema: -1, error_msg: "spans need 6 buckets, have 7 buckets: histogram spans specify different number of buckets than provided", ..Default::default() }),
        ("negative offset", Case { origin: FloatHistogram { schema: 0, positive_spans: vec![Span::new(-2, 4), Span::new(-1, 0), Span::new(3, 2)], positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0], ..Default::default() }, target_schema: -1, error_msg: "span number 2 with offset -1: histogram has a span whose offset is negative", ..Default::default() }),
    ];

    for (tn, mut tc) in tcs {
        eprintln!("case: {}", tn);
        let err = tc.origin.reduce_resolution(tc.target_schema);
        if !tc.error_msg.is_empty() {
            let err = err.expect_err("expected an error");
            assert_eq!(tc.error_msg, err.to_string());
            // The returned error should be a histogram.Error.
            assert!(err.is_histogram_error());
            continue;
        }
        assert!(err.is_ok(), "unexpected error: {:?}", err);
        assert_eq!(tc.target.as_ref(), Some(&tc.origin));
    }
}

#[test]
fn test_float_histogram_trim_buckets_in_place() {
    // Schema 0 native histogram with buckets straddling a trim point of 4.
    // Bucket upper bounds: 1, 2, 4, 8.
    let new_hist = || FloatHistogram {
        schema: 0,
        count: 30.0,
        sum: 100.0,
        positive_spans: vec![Span::new(0, 4)],
        positive_buckets: vec![5.0, 5.0, 10.0, 10.0],
        ..Default::default()
    };

    // Subtest: "mutates the receiver and returns it".
    {
        let mut h = new_hist();
        let h_ptr: *const FloatHistogram = &h;
        let got: *const FloatHistogram = h.trim_buckets(4.0, true);
        // The returned histogram is the receiver, not a copy.
        // adapted: Go require.Same (pointer identity) -> std::ptr::eq on the returned reference.
        assert!(std::ptr::eq(h_ptr, got));
        // The bucket entirely above the trim point (upper bound 8) is dropped.
        assert_ne!(new_hist(), h);
    }

    // Subtest: "Copy preserves the original".
    {
        let h = new_hist();
        let mut trimmed = h.copy();
        trimmed.trim_buckets(4.0, true);
        // The original is untouched.
        assert_eq!(new_hist(), h);
        // The copy was actually trimmed.
        assert_ne!(h, trimmed);
    }
}
