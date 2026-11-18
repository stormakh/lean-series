//! Port of upstream `model/histogram/generic_test.go`.

use model::histogram::*;

#[derive(Default)]
struct BoundCase {
    idx: i32,
    schema: i32,
    want: f64,
}

#[test]
fn test_get_bound_exponential() {
    let scenarios: Vec<BoundCase> = vec![
        BoundCase {
            idx: -1,
            schema: -1,
            want: 0.25,
        },
        BoundCase {
            idx: 0,
            schema: -1,
            want: 1.0,
        },
        BoundCase {
            idx: 1,
            schema: -1,
            want: 4.0,
        },
        BoundCase {
            idx: 512,
            schema: -1,
            want: f64::MAX,
        },
        BoundCase {
            idx: 513,
            schema: -1,
            want: f64::INFINITY,
        },
        BoundCase {
            idx: -1,
            schema: 0,
            want: 0.5,
        },
        BoundCase {
            idx: 0,
            schema: 0,
            want: 1.0,
        },
        BoundCase {
            idx: 1,
            schema: 0,
            want: 2.0,
        },
        BoundCase {
            idx: 1024,
            schema: 0,
            want: f64::MAX,
        },
        BoundCase {
            idx: 1025,
            schema: 0,
            want: f64::INFINITY,
        },
        BoundCase {
            idx: -1,
            schema: 2,
            want: 0.8408964152537144,
        },
        BoundCase {
            idx: 0,
            schema: 2,
            want: 1.0,
        },
        BoundCase {
            idx: 1,
            schema: 2,
            want: 1.189207115002721,
        },
        BoundCase {
            idx: 4096,
            schema: 2,
            want: f64::MAX,
        },
        BoundCase {
            idx: 4097,
            schema: 2,
            want: f64::INFINITY,
        },
    ];
    for s in scenarios {
        let got = get_bound_exponential(s.idx, s.schema);
        if s.want != got {
            assert_eq!(s.want, got, "idx {}, schema {}", s.idx, s.schema);
        }
    }
}

#[derive(Default)]
struct FractionCase {
    name: &'static str,
    bucket: Bucket<f64>,
    v: f64,
    linear: bool,
    want: f64,
}

#[test]
fn test_bucket_fraction_below() {
    let scenarios: Vec<FractionCase> = vec![
        FractionCase {
            name: "linear, midpoint",
            bucket: Bucket::<f64> {
                lower: 0.0,
                upper: 10.0,
                count: 100.0,
                ..Default::default()
            },
            v: 5.0,
            linear: true,
            want: 0.5,
        },
        FractionCase {
            name: "linear, quarter",
            bucket: Bucket::<f64> {
                lower: 2.0,
                upper: 6.0,
                count: 100.0,
                ..Default::default()
            },
            v: 3.0,
            linear: true,
            want: 0.25,
        },
        FractionCase {
            name: "linear, negative bucket",
            bucket: Bucket::<f64> {
                lower: -10.0,
                upper: -5.0,
                count: 100.0,
                ..Default::default()
            },
            v: -6.0,
            linear: true,
            want: 0.8,
        },
        FractionCase {
            name: "exponential, positive bucket at geometric midpoint",
            bucket: Bucket::<f64> {
                lower: 2.0,
                upper: 8.0,
                count: 100.0,
                ..Default::default()
            },
            v: 4.0,
            linear: false,
            want: 0.5,
        },
        FractionCase {
            name: "exponential, positive bucket lower quarter of log scale",
            bucket: Bucket::<f64> {
                lower: 1.0,
                upper: 16.0,
                count: 100.0,
                ..Default::default()
            },
            v: 2.0,
            linear: false,
            want: 0.25,
        },
        FractionCase {
            name: "exponential, negative bucket at geometric midpoint",
            bucket: Bucket::<f64> {
                lower: -8.0,
                upper: -2.0,
                count: 100.0,
                ..Default::default()
            },
            v: -4.0,
            linear: false,
            want: 0.5,
        },
        FractionCase {
            name: "exponential, negative bucket",
            bucket: Bucket::<f64> {
                lower: -4.0,
                upper: -2.0,
                count: 100.0,
                ..Default::default()
            },
            v: -3.0,
            linear: false,
            want: 0.41503749927884383,
        },
    ];
    for s in scenarios {
        let got = s.bucket.fraction_below(s.v, s.linear);
        // require.InDelta(want, got, 1e-12).
        let dt = s.want - got;
        assert!(
            !got.is_nan() && (-1e-12..=1e-12).contains(&dt),
            "{}: want {}, got {}",
            s.name,
            s.want,
            got
        );
    }
}

#[derive(Default)]
struct ReduceCaseI64 {
    spans: Vec<Span>,
    buckets: Vec<i64>,
    schema: i32,
    target_schema: i32,
    expected_spans: Vec<Span>,
    expected_buckets: Vec<i64>,
}

#[test]
fn test_reduce_resolution_histogram() {
    let cases: Vec<ReduceCaseI64> = vec![ReduceCaseI64 {
        spans: vec![Span::new(0, 4), Span::new(0, 0), Span::new(3, 2)],
        buckets: vec![1, 2, -2, 1, -1, 0],
        schema: 0,
        target_schema: -1,
        expected_spans: vec![Span::new(0, 3), Span::new(1, 1)],
        expected_buckets: vec![1, 3, -2, 0],
    }];
    for mut tc in cases {
        let (spans_copy, buckets_copy) = (tc.spans.clone(), tc.buckets.clone());
        let (spans, buckets) =
            must_reduce_resolution(&tc.spans, &tc.buckets, tc.schema, tc.target_schema, true);
        assert_eq!(tc.expected_spans, spans);
        assert_eq!(tc.expected_buckets, buckets);
        // Verify inputs were not mutated:
        assert_eq!(spans_copy, tc.spans);
        assert_eq!(buckets_copy, tc.buckets);

        // Output slices reuse input slices:
        // adapted: Go's inplace=true returns subslices of the inputs; the Rust in-place variant writes the result
        // into the input vectors themselves (truncated to the result length), so the result is read from them.
        must_reduce_resolution_in_place(
            &mut tc.spans,
            &mut tc.buckets,
            tc.schema,
            tc.target_schema,
            true,
        );
        let (spans, buckets) = (tc.spans.clone(), tc.buckets.clone());
        assert_eq!(tc.expected_spans, spans);
        assert_eq!(tc.expected_buckets, buckets);
        // Verify inputs were mutated which is now expected:
        assert_eq!(spans, tc.spans[..spans.len()]);
        assert_eq!(buckets, tc.buckets[..buckets.len()]);
    }
}

#[derive(Default)]
struct ReduceCaseF64 {
    spans: Vec<Span>,
    buckets: Vec<f64>,
    schema: i32,
    target_schema: i32,
    expected_spans: Vec<Span>,
    expected_buckets: Vec<f64>,
}

#[test]
fn test_reduce_resolution_float_histogram() {
    let cases: Vec<ReduceCaseF64> = vec![ReduceCaseF64 {
        spans: vec![Span::new(0, 4), Span::new(0, 0), Span::new(3, 2)],
        buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0],
        schema: 0,
        target_schema: -1,
        expected_spans: vec![Span::new(0, 3), Span::new(1, 1)],
        expected_buckets: vec![1.0, 4.0, 2.0, 2.0],
    }];
    for mut tc in cases {
        let (spans_copy, buckets_copy) = (tc.spans.clone(), tc.buckets.clone());
        let (spans, buckets) =
            must_reduce_resolution(&tc.spans, &tc.buckets, tc.schema, tc.target_schema, false);
        assert_eq!(tc.expected_spans, spans);
        assert_eq!(tc.expected_buckets, buckets);
        // Verify inputs were not mutated:
        assert_eq!(spans_copy, tc.spans);
        assert_eq!(buckets_copy, tc.buckets);

        // Output slices reuse input slices:
        // adapted: Go's inplace=true returns subslices of the inputs; the Rust in-place variant writes the result
        // into the input vectors themselves (truncated to the result length), so the result is read from them.
        must_reduce_resolution_in_place(
            &mut tc.spans,
            &mut tc.buckets,
            tc.schema,
            tc.target_schema,
            false,
        );
        let (spans, buckets) = (tc.spans.clone(), tc.buckets.clone());
        assert_eq!(tc.expected_spans, spans);
        assert_eq!(tc.expected_buckets, buckets);
        // Verify inputs were mutated which is now expected:
        assert_eq!(spans, tc.spans[..spans.len()]);
        assert_eq!(buckets, tc.buckets[..buckets.len()]);
    }
}

#[derive(Default)]
struct BoundsMatchCase {
    name: &'static str,
    c1: Vec<f64>,
    c2: Vec<f64>,
    want: bool,
}

// adapted: Go's nil and empty slices are both an empty slice in Rust; the "both nil" and "both empty" rows stay.
#[test]
fn test_custom_bucket_bounds_match() {
    let tests: Vec<BoundsMatchCase> = vec![
        BoundsMatchCase {
            name: "both nil",
            c1: vec![],
            c2: vec![],
            want: true,
        },
        BoundsMatchCase {
            name: "both empty",
            c1: vec![],
            c2: vec![],
            want: true,
        },
        BoundsMatchCase {
            name: "one empty one non-empty",
            c1: vec![],
            c2: vec![1.0],
            want: false,
        },
        BoundsMatchCase {
            name: "different lengths",
            c1: vec![1.0, 2.0],
            c2: vec![1.0, 2.0, 3.0],
            want: false,
        },
        BoundsMatchCase {
            name: "same single value",
            c1: vec![1.5],
            c2: vec![1.5],
            want: true,
        },
        BoundsMatchCase {
            name: "different single value",
            c1: vec![1.5],
            c2: vec![2.5],
            want: false,
        },
        BoundsMatchCase {
            name: "same multiple values",
            c1: vec![1.0, 2.0, 3.0, 4.0, 5.0],
            c2: vec![1.0, 2.0, 3.0, 4.0, 5.0],
            want: true,
        },
        BoundsMatchCase {
            name: "different values",
            c1: vec![1.0, 2.1, 3.0],
            c2: vec![1.0, 2.0, 3.0],
            want: false,
        },
    ];
    for tt in tests {
        let got = custom_bucket_bounds_match(&tt.c1, &tt.c2);
        assert_eq!(tt.want, got, "{}", tt.name);

        // Test commutativity (should be symmetric)
        let got_reverse = custom_bucket_bounds_match(&tt.c2, &tt.c1);
        assert_eq!(got, got_reverse, "{}", tt.name);
    }
}
