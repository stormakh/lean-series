//! Port of `promql/quantile_test.go`.

use model::histogram::{custom_values, BucketIterator, FloatHistogram, Span, CUSTOM_BUCKETS_SCHEMA};
use promql::quantile::{bucket_fraction, bucket_quantile, histogram_fraction, Bucket, Buckets};
use storage::annotations::PositionRange;

const INF: f64 = f64::INFINITY;
const NEG_INF: f64 = f64::NEG_INFINITY;

/// testify `require.InEpsilon`.
fn require_in_epsilon(expected: f64, actual: f64, eps: f64, msg: &str) {
    assert!(!expected.is_nan() && !actual.is_nan(), "{}: NaN: expected {}, actual {}", msg, expected, actual);
    assert!(expected != 0.0, "{}: expected value must not be zero", msg);
    let rel = ((expected - actual) / expected).abs();
    assert!(rel <= eps, "{}: relative error is too high: {} (expected) < {} (actual): expected {}, actual {}", msg, eps, rel, expected, actual);
}

fn b(upper_bound: f64, count: f64) -> Bucket {
    Bucket { upper_bound, count }
}

#[test]
fn test_bucket_quantile_forced_monotonicity() {
    // empty buckets
    for (name, buckets) in [("nil", Buckets::new()), ("empty", Buckets::new())] {
        for (tname, q, want) in [
            ("zero", 0.0, f64::NAN),
            ("middle", 0.5, f64::NAN),
            ("one", 1.0, f64::NAN),
            ("NaN", f64::NAN, f64::NAN),
            ("below zero", -1.0, NEG_INF),
            ("above one", 2.0, INF),
        ] {
            let mut bk = buckets.clone();
            let r = bucket_quantile(q, &mut bk);
            if want.is_nan() {
                assert!(r.quantile.is_nan(), "{}/{}", name, tname);
            } else {
                assert_eq!(want, r.quantile, "{}/{}", name, tname);
            }
            assert!(!r.forced_monotonic, "{}/{}", name, tname);
            assert!(!r.fixed_precision, "{}/{}", name, tname);
            assert_eq!(r.min_bucket, 0.0);
            assert_eq!(r.max_bucket, 0.0);
            assert_eq!(r.max_diff, 0.0);
        }
    }

    let eps = 1e-12;

    struct Case {
        name: &'static str,
        get_input: fn() -> Buckets,
        expected_forced: bool,
        expected_fixed: bool,
        expected_values: &'static [(f64, f64)],
    }

    let cases = [
        Case {
            name: "simple - monotonic",
            get_input: || vec![b(10.0, 10.0), b(15.0, 15.0), b(20.0, 15.0), b(30.0, 15.0), b(INF, 15.0)],
            expected_forced: false,
            expected_fixed: false,
            expected_values: &[(1.0, 15.0), (0.99, 14.85), (0.9, 13.5), (0.5, 7.5)],
        },
        Case {
            name: "simple - non-monotonic middle",
            get_input: || {
                vec![
                    b(10.0, 10.0),
                    b(15.0, 15.0),
                    b(20.0, 15.00000000001), // Simulate the case there's a small imprecision in float64.
                    b(30.0, 15.0),
                    b(INF, 15.0),
                ]
            },
            expected_forced: false,
            expected_fixed: true,
            expected_values: &[(1.0, 15.0), (0.99, 14.85), (0.9, 13.5), (0.5, 7.5)],
        },
        Case {
            name: "real example - monotonic",
            get_input: || {
                vec![
                    b(1.0, 6454661.3014166197),
                    b(5.0, 8339611.2001912938),
                    b(10.0, 14118319.2444762159),
                    b(25.0, 14130031.5272856522),
                    b(50.0, 46001270.3030008152),
                    b(64.0, 46008473.8585563600),
                    b(80.0, 46008473.8585563600),
                    b(100.0, 46008473.8585563600),
                    b(250.0, 46008473.8585563600),
                    b(1000.0, 46008473.8585563600),
                    b(INF, 46008473.8585563600),
                ]
            },
            expected_forced: false,
            expected_fixed: false,
            expected_values: &[
                (1.0, 64.0),
                (0.99, 49.64475715376406),
                (0.9, 46.39671690938454),
                (0.5, 31.96098248992002),
            ],
        },
        Case {
            name: "real example - non-monotonic",
            get_input: || {
                vec![
                    b(1.0, 6454661.3014166225),
                    b(5.0, 8339611.2001912957),
                    b(10.0, 14118319.2444762159),
                    b(25.0, 14130031.5272856504),
                    b(50.0, 46001270.3030008227),
                    b(64.0, 46008473.8585563824),
                    b(80.0, 46008473.8585563898),
                    b(100.0, 46008473.8585563824),
                    b(250.0, 46008473.8585563824),
                    b(1000.0, 46008473.8585563898),
                    b(INF, 46008473.8585563824),
                ]
            },
            expected_forced: false,
            expected_fixed: true,
            expected_values: &[
                (1.0, 64.0),
                (0.99, 49.64475715376406),
                (0.9, 46.39671690938454),
                (0.5, 31.96098248992002),
            ],
        },
        Case {
            name: "real example 2 - monotonic",
            get_input: || {
                vec![
                    b(0.005, 9.6),
                    b(0.01, 9.688888889),
                    b(0.025, 9.755555556),
                    b(0.05, 9.844444444),
                    b(0.1, 9.888888889),
                    b(0.25, 9.888888889),
                    b(0.5, 9.888888889),
                    b(1.0, 9.888888889),
                    b(2.5, 9.888888889),
                    b(5.0, 9.888888889),
                    b(10.0, 9.888888889),
                    b(25.0, 9.888888889),
                    b(50.0, 9.888888889),
                    b(100.0, 9.888888889),
                    b(INF, 9.888888889),
                ]
            },
            expected_forced: false,
            expected_fixed: false,
            expected_values: &[
                (1.0, 0.1),
                (0.99, 0.03468750000281261),
                (0.9, 0.00463541666671875),
                (0.5, 0.0025752314815104174),
            ],
        },
        Case {
            name: "real example 2 - non-monotonic",
            get_input: || {
                vec![
                    b(0.005, 9.6),
                    b(0.01, 9.688888889),
                    b(0.025, 9.755555556),
                    b(0.05, 9.844444444),
                    b(0.1, 9.888888889),
                    b(0.25, 9.888888889),
                    b(0.5, 9.888888889),
                    b(1.0, 9.888888889),
                    b(2.5, 9.888888889),
                    b(5.0, 9.888888889),
                    b(10.0, 9.888888889001), // Simulate the case there's a small imprecision in float64.
                    b(25.0, 9.888888889),
                    b(50.0, 9.888888888999), // Simulate the case there's a small imprecision in float64.
                    b(100.0, 9.888888889),
                    b(INF, 9.888888889),
                ]
            },
            expected_forced: false,
            expected_fixed: true,
            expected_values: &[
                (1.0, 0.1),
                (0.99, 0.03468750000281261),
                (0.9, 0.00463541666671875),
                (0.5, 0.0025752314815104174),
            ],
        },
    ];

    for tc in &cases {
        for &(q, v) in tc.expected_values {
            let mut input = (tc.get_input)();
            let r = bucket_quantile(q, &mut input);
            assert_eq!(tc.expected_forced, r.forced_monotonic, "{} q={}", tc.name, q);
            assert_eq!(tc.expected_fixed, r.fixed_precision, "{} q={}", tc.name, q);
            require_in_epsilon(v, r.quantile, eps, tc.name);
        }
    }
}

#[test]
fn test_bucket_fraction() {
    // Counts are cumulative, with all observations above zero.
    let buckets: Buckets = vec![b(1.0, 10.0), b(INF, 30.0)];

    let cases: Vec<(&str, Buckets, f64, f64, f64)> = vec![
        ("nil/finite bounds", Buckets::new(), 0.0, 1.0, f64::NAN),
        ("nil/reversed bounds", Buckets::new(), 1.0, 0.0, f64::NAN),
        ("nil/infinite bounds", Buckets::new(), NEG_INF, INF, f64::NAN),
        ("nil/NaN lower bound", Buckets::new(), f64::NAN, 1.0, f64::NAN),
        ("nil/NaN upper bound", Buckets::new(), 0.0, f64::NAN, f64::NAN),
        ("empty/finite bounds", Buckets::new(), 0.0, 1.0, f64::NAN),
        ("empty/reversed bounds", Buckets::new(), 1.0, 0.0, f64::NAN),
        ("empty/infinite bounds", Buckets::new(), NEG_INF, INF, f64::NAN),
        ("empty/NaN lower bound", Buckets::new(), f64::NAN, 1.0, f64::NAN),
        ("empty/NaN upper bound", Buckets::new(), 0.0, f64::NAN, f64::NAN),
        ("zero count", vec![b(INF, 0.0)], 0.0, 1.0, f64::NAN),
        // Interpolation in the first bucket gives 10 * (0.5 - 0) / (1 - 0) = 5 out of 30 observations.
        ("interpolated fraction", buckets.clone(), 0.0, 0.5, 5.0 / 30.0),
        ("full range", buckets.clone(), 0.0, INF, 1.0),
        ("reversed bounds", buckets.clone(), 1.0, 0.0, 0.0),
    ];
    for (name, bk, lower, upper, want) in cases {
        let mut bk = bk.clone();
        let got = bucket_fraction(lower, upper, &mut bk);
        if want.is_nan() {
            assert!(got.is_nan(), "{}", name);
        } else if want == 0.0 {
            assert_eq!(got, 0.0, "{}", name);
        } else {
            require_in_epsilon(want, got, 1e-12, name);
        }
    }
}

fn spans(s: &[(i32, u32)]) -> Vec<Span> {
    s.iter().map(|&(offset, length)| Span { offset, length }).collect()
}

/// Checks that `h </ x` and `h >/ x` match histogram_fraction(-Inf, x, h) and histogram_fraction(x, +Inf, h)
/// times histogram_count(h).
#[test]
fn test_trim_buckets_histogram_fraction_cross_check() {
    let pos = PositionRange::default();
    let cases: Vec<(&str, FloatHistogram, Vec<f64>)> = vec![
        (
            // Bucket bounds: (-64,-32] (-32,-16] (-16,-8] (-4,-2] (-2,-1] (-0.5,0.5] (0.5,1] (1,2] (2,4] (16,32]
            // (32,64] (64,128].
            "exponential, positive and negative buckets, zero bucket, multiple spans, empty buckets",
            FloatHistogram {
                schema: 0,
                count: 56.0,
                sum: 113.0,
                zero_threshold: 0.5,
                zero_count: 7.0,
                positive_spans: spans(&[(0, 3), (2, 3)]),
                positive_buckets: vec![3.0, 11.0, 2.0, 9.0, 0.0, 6.0],
                negative_spans: spans(&[(1, 2), (1, 3)]),
                negative_buckets: vec![4.0, 1.0, 8.0, 0.0, 5.0],
                ..Default::default()
            },
            vec![
                NEG_INF, -100.0, -64.0, -32.0, -16.0, -8.0, -4.0, -2.0, -1.0, -0.5, // Bucket boundaries.
                -48.0, -12.0, -6.0, -3.0, -0.75, -0.3, // Inside buckets and inside gaps between spans.
                0.0, 0.3, 0.73, 1.87, 3.3, 12.0, 100.0, // Inside buckets and inside gaps between spans.
                0.5, 1.0, 2.0, 4.0, 16.0, 32.0, 64.0, 128.0, // Bucket boundaries.
                200.0, INF,
            ],
        ),
        (
            // Bucket bounds: (-Inf,1] (1,2] (2,4] (8,16] (16,+Inf].
            "custom buckets (NHCB), underflow bucket to -Inf and overflow bucket to +Inf",
            FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 36.0,
                sum: 181.0,
                positive_spans: spans(&[(0, 3), (1, 2)]),
                positive_buckets: vec![2.0, 7.0, 5.0, 13.0, 9.0],
                custom_values: custom_values(&[1.0, 2.0, 4.0, 8.0, 16.0]),
                ..Default::default()
            },
            vec![
                NEG_INF, 0.0, 1.0, 2.0, 4.0, 8.0, 16.0, // Bucket boundaries.
                0.3, 0.73, 1.87, 3.3, 6.0, 12.0, 20.0, // Inside buckets, the gap and the overflow bucket.
                INF,
            ],
        ),
        (
            // Bucket bounds: (0.5,0.5946] (0.5946,0.7071] (0.7071,0.8409] (0.8409,1] (1.6818,2] (2,2.3784]
            // (2.3784,2.8284].
            "only positive buckets, no zero bucket, multiple spans, empty bucket",
            FloatHistogram {
                schema: 2,
                count: 36.0,
                sum: 49.0,
                positive_spans: spans(&[(-3, 4), (3, 3)]),
                positive_buckets: vec![1.0, 6.0, 0.0, 9.0, 4.0, 11.0, 5.0],
                ..Default::default()
            },
            vec![
                NEG_INF, -12.0, -1.7, 0.0, 0.5, 1.0, 2.0, // Bucket boundaries.
                0.6, 0.9, 1.5, 2.5, 3.0, // Inside buckets and inside the gap between spans.
                INF,
            ],
        ),
        (
            // Bucket bounds: (-8,-5.6569] (-5.6569,-4] (-2,-1.4142] (-1.4142,-1] (-1,-0.7071].
            "only negative buckets, no zero bucket, multiple spans",
            FloatHistogram {
                schema: 1,
                count: 29.0,
                sum: -87.0,
                negative_spans: spans(&[(0, 3), (2, 2)]),
                negative_buckets: vec![4.0, 9.0, 3.0, 8.0, 5.0],
                ..Default::default()
            },
            vec![
                NEG_INF, -20.0, -8.0, -4.0, -2.0, -1.0, // Bucket boundaries.
                -6.0, -3.0, -1.7, -0.9, -0.5, // Inside buckets and inside the gap between spans.
                0.0, 0.5, INF,
            ],
        ),
        (
            "zero bucket only",
            FloatHistogram { schema: 0, count: 9.0, sum: 0.4, zero_threshold: 0.25, zero_count: 9.0, ..Default::default() },
            vec![NEG_INF, -1.0, -0.25, 0.25, -0.1, 0.0, 0.1, 1.0, INF],
        ),
    ];

    for (name, h, thresholds) in &cases {
        let total = h.count;
        // Guard against a fixture whose Count does not match its buckets.
        let mut bucket_count = 0.0;
        let mut it = h.all_bucket_iterator();
        while it.next() {
            bucket_count += it.at().count;
        }
        assert_eq!(total, bucket_count, "{}: test case has an inconsistent Count", name);

        for &x in thresholds {
            let ctx = format!("{} x={}", name, x);
            // TrimBuckets mutates its receiver, so it needs a copy; HistogramFraction does not.
            let mut lower = h.copy();
            lower.trim_buckets(x, true); // h </ x
            let (fraction_below, annos) = histogram_fraction(NEG_INF, x, h, "", pos);
            assert!(annos.is_empty(), "{}", ctx);
            if fraction_below == 0.0 {
                assert_eq!(lower.count, 0.0, "{}", ctx);
            } else {
                require_in_epsilon(fraction_below * total, lower.count, 1e-9, &ctx);
            }

            let mut upper = h.copy();
            upper.trim_buckets(x, false); // h >/ x
            let (fraction_above, annos) = histogram_fraction(x, INF, h, "", pos);
            assert!(annos.is_empty(), "{}", ctx);
            if fraction_above == 0.0 {
                assert_eq!(upper.count, 0.0, "{}", ctx);
            } else {
                require_in_epsilon(fraction_above * total, upper.count, 1e-9, &ctx);
            }
        }
    }

    // zero-count histogram: NaN fraction vs empty trim
    {
        let h = FloatHistogram::default();
        let (fraction, annos) = histogram_fraction(NEG_INF, 0.5, &h, "", pos);
        assert!(annos.is_empty());
        assert!(fraction.is_nan());
        let mut trimmed = h.copy();
        trimmed.trim_buckets(0.5, true);
        assert_eq!(trimmed.count, 0.0);
    }

    // Known, pre-existing divergences between the two operations.
    struct Divergence {
        name: &'static str,
        h: FloatHistogram,
        x: f64,
        is_upper_trim: bool,
        expected_count: f64,
        expected_fraction: f64,
        why: &'static str,
    }
    let divergences = vec![
        Divergence {
            name: "threshold inside an infinite-bound bucket",
            h: FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 20.0,
                sum: 50.0,
                positive_spans: spans(&[(0, 5)]),
                positive_buckets: vec![2.0, 3.0, 5.0, 4.0, 6.0],
                custom_values: custom_values(&[1.0, 2.0, 4.0, 8.0, 16.0]),
                ..Default::default()
            },
            x: -6.0,
            is_upper_trim: false,
            expected_count: 18.0,   // TrimBuckets drops the whole underflow bucket.
            expected_fraction: 1.0, // HistogramFraction credits the whole underflow bucket as being above x.
            why: "underflow bucket",
        },
        Divergence {
            name: "empty positive bucket, non-empty negative bucket, non-empty zero bucket",
            h: FloatHistogram {
                schema: 0,
                zero_threshold: 0.001,
                zero_count: 3.0,
                count: 8.0,
                sum: -10.0,
                negative_spans: spans(&[(0, 1)]),
                negative_buckets: vec![5.0],
                positive_spans: spans(&[(0, 1)]),
                positive_buckets: vec![0.0], // Present but empty.
                ..Default::default()
            },
            x: 0.0,
            is_upper_trim: true,
            expected_count: 8.0,         // TrimBuckets keeps the whole zero bucket.
            expected_fraction: 0.8125,   // HistogramFraction splits the zero bucket in half: (5+1.5)/8.
            why: "zero bucket",
        },
    ];
    for tc in &divergences {
        let mut trimmed = tc.h.copy();
        trimmed.trim_buckets(tc.x, tc.is_upper_trim);
        assert_eq!(tc.expected_count, trimmed.count, "{}: TrimBuckets handles the {} differently", tc.name, tc.why);
        let (lower, upper) = if tc.is_upper_trim { (NEG_INF, tc.x) } else { (tc.x, INF) };
        let (fraction, annos) = histogram_fraction(lower, upper, &tc.h, "", pos);
        assert!(annos.is_empty(), "{}", tc.name);
        assert_eq!(tc.expected_fraction, fraction, "{}: HistogramFraction handles the {} differently", tc.name, tc.why);
    }
}
