//! Port of `promql/functions_internal_test.go`.

use std::sync::Arc;

use model::histogram::{kahansum, CounterResetHint, FloatHistogram};
use model::labels::{self, METRIC_NAME};
use promql::functions::{histogram_rate, interpolate, interpolate_histograms};
use promql::value::{FPoint, HPoint};
use storage::annotations::PositionRange;
use storage::Annotations;

#[test]
fn test_histogram_rate_counter_reset_hint() {
    let points = vec![
        HPoint {
            t: 0,
            h: Arc::new(FloatHistogram {
                counter_reset_hint: CounterResetHint::CounterReset,
                count: 5.0,
                sum: 5.0,
                ..Default::default()
            }),
        },
        HPoint {
            t: 1,
            h: Arc::new(FloatHistogram {
                counter_reset_hint: CounterResetHint::UnknownCounterReset,
                count: 10.0,
                sum: 10.0,
                ..Default::default()
            }),
        },
    ];
    let labels = labels::from_strings(&[METRIC_NAME, "foo"]);
    let (fh, _) = histogram_rate(&points, &[], false, &labels, PositionRange::default()).unwrap();
    assert_eq!(CounterResetHint::GaugeType, fh.unwrap().counter_reset_hint);

    let (fh, _) = histogram_rate(&points, &[], true, &labels, PositionRange::default()).unwrap();
    assert_eq!(CounterResetHint::GaugeType, fh.unwrap().counter_reset_hint);
}

#[test]
fn test_kahan_sum_inc() {
    let test_cases = [
        ("+Inf + anything = +Inf", f64::INFINITY, 2.0, f64::INFINITY),
        ("-Inf + anything = -Inf", f64::NEG_INFINITY, 2.0, f64::NEG_INFINITY),
        ("+Inf + -Inf = NaN", f64::INFINITY, f64::NEG_INFINITY, f64::NAN),
        ("NaN + anything = NaN", f64::NAN, 2.0, f64::NAN),
        ("NaN + Inf = NaN", f64::NAN, f64::INFINITY, f64::NAN),
        ("NaN + -Inf = NaN", f64::NAN, f64::NEG_INFINITY, f64::NAN),
    ];

    let run_test = |name: &str, a: f64, b: f64, expected: f64| {
        let (sum, c) = kahansum::inc(b, a, 0.0);
        let result = sum + c;
        if expected.is_nan() {
            assert!(result.is_nan(), "{}: expected result to be NaN, but got {} (from {} + {})", name, result, sum, c);
        } else {
            assert_eq!(expected, result, "{}: expected result to be {}, but got {} (from {} + {})", name, expected, result, sum, c);
        }
    };

    for (name, first, second, expected) in test_cases {
        run_test(name, first, second, expected);
        run_test(name, second, first, expected);
    }
}

#[test]
fn test_interpolate() {
    let tests = [
        (FPoint { t: 1, f: 100.0 }, FPoint { t: 2, f: 200.0 }, 1, false, 100.0),
        (FPoint { t: 0, f: 100.0 }, FPoint { t: 2, f: 200.0 }, 1, false, 150.0),
        (FPoint { t: 0, f: 200.0 }, FPoint { t: 2, f: 100.0 }, 1, false, 150.0),
        (FPoint { t: 0, f: 200.0 }, FPoint { t: 2, f: 0.0 }, 1, true, 0.0),
        (FPoint { t: 0, f: 200.0 }, FPoint { t: 2, f: 100.0 }, 1, true, 50.0),
        (FPoint { t: 0, f: 500.0 }, FPoint { t: 2, f: 100.0 }, 1, true, 50.0),
        (FPoint { t: 0, f: 500.0 }, FPoint { t: 10, f: 0.0 }, 1, true, 0.0),
    ];
    for (p1, p2, t, is_counter, expected) in tests {
        let result = interpolate(p1, p2, t, is_counter);
        assert_eq!(expected, result);
    }
}

#[test]
fn test_interpolate_histograms() {
    let h1 = FloatHistogram {
        count: 1.0,
        sum: 1.0,
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        ..Default::default()
    };
    let h2 = FloatHistogram {
        count: 3.0,
        sum: 3.0,
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        ..Default::default()
    };
    let h2_reset =
        FloatHistogram { count: 1.0, sum: 1.0, counter_reset_hint: CounterResetHint::CounterReset, ..Default::default() };
    let pos = PositionRange::default();

    struct Case<'a> {
        name: &'a str,
        h1: &'a FloatHistogram,
        h2: &'a FloatHistogram,
        t1: i64,
        t2: i64,
        t: i64,
        is_counter: bool,
        want_count: f64,
    }
    let tests = [
        Case { name: "exact match t1", h1: &h1, h2: &h2, t1: 0, t2: 20, t: 0, is_counter: false, want_count: 1.0 },
        Case { name: "exact match t2", h1: &h1, h2: &h2, t1: 0, t2: 20, t: 20, is_counter: false, want_count: 3.0 },
        Case { name: "midpoint no reset", h1: &h1, h2: &h2, t1: 0, t2: 20, t: 10, is_counter: false, want_count: 2.0 },
        Case {
            name: "counter midpoint no reset",
            h1: &h1,
            h2: &h2,
            t1: 0,
            t2: 20,
            t: 10,
            is_counter: true,
            want_count: 2.0,
        },
        Case {
            name: "counter midpoint with reset: scale from zero",
            h1: &h1,
            h2: &h2_reset,
            t1: 0,
            t2: 20,
            t: 10,
            // h2Reset * (10/20) = count:1 * 0.5 = 0.5.
            is_counter: true,
            want_count: 0.5,
        },
        Case {
            name: "quarter point",
            h1: &h1,
            h2: &h2,
            t1: 0,
            t2: 20,
            t: 5,
            // h1 + (h2-h1)*0.25 = 1 + 2*0.25 = 1.5.
            is_counter: false,
            want_count: 1.5,
        },
    ];
    for tc in &tests {
        let mut annos = Annotations::new();
        let result = interpolate_histograms(tc.h1, tc.t1, tc.h2, tc.t2, tc.t, tc.is_counter, &mut annos, pos)
            .unwrap_or_else(|e| panic!("{}: {}", tc.name, e));
        assert_eq!(tc.want_count, result.count, "{}", tc.name);
    }
}
