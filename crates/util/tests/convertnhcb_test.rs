//! Port of `util/convertnhcb/convertnhcb_test.go`.

use std::sync::Arc;

use model::histogram::{CUSTOM_BUCKETS_SCHEMA, FloatHistogram, Histogram, Span};
use util::convertnhcb::{Error, TempHistogram};

#[derive(Clone, Copy, Debug, PartialEq)]
enum ErrKind {
    MissingCount,
    CountNotCumulative,
    NegativeCount,
    NaNBucket,
}

/// Go `require.ErrorIs` against the sentinel errors (the variants carry the wrapped details).
fn is(err: &Error, kind: ErrKind) -> bool {
    matches!(
        (err, kind),
        (Error::MissingCount, ErrKind::MissingCount)
            | (Error::CountNotCumulative(_), ErrKind::CountNotCumulative)
            | (Error::NegativeCount(_), ErrKind::NegativeCount)
            | (Error::NaNBucket, ErrKind::NaNBucket)
    )
}

struct Case {
    name: &'static str,
    setup: fn() -> TempHistogram,
    expected_err: Option<ErrKind>,
    expected_h: Option<Histogram>,
    expected_fh: Option<FloatHistogram>,
}

fn span(length: u32) -> Vec<Span> {
    vec![Span { offset: 0, length }]
}

#[test]
fn test_nhcb_convert() {
    let tests = vec![
        Case { name: "empty", setup: TempHistogram::new, expected_err: Some(ErrKind::MissingCount), expected_h: None, expected_fh: None },
        Case {
            name: "sum only",
            setup: || {
                let mut h = TempHistogram::new();
                let _ = h.set_sum(1000.25);
                h
            },
            expected_err: Some(ErrKind::MissingCount),
            expected_h: None,
            expected_fh: None,
        },
        Case {
            name: "count and sum without buckets",
            setup: || {
                let mut h = TempHistogram::new();
                let _ = h.set_count(1000.0);
                let _ = h.set_sum(1000.25);
                h
            },
            expected_err: None,
            expected_h: Some(Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 1000,
                sum: 1000.25,
                positive_spans: span(1),
                positive_buckets: vec![1000],
                ..Default::default()
            }),
            expected_fh: None,
        },
        Case {
            name: "single integer bucket",
            setup: || {
                let mut h = TempHistogram::new();
                let _ = h.set_sum(1000.25);
                let _ = h.set_bucket_count(0.5, 1000.0);
                h
            },
            expected_err: None,
            expected_h: Some(Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 1000,
                sum: 1000.25,
                positive_spans: span(1),
                positive_buckets: vec![1000],
                custom_values: Some(Arc::from(&[0.5][..])),
                ..Default::default()
            }),
            expected_fh: None,
        },
        Case {
            name: "single float bucket",
            setup: || {
                let mut h = TempHistogram::new();
                let _ = h.set_sum(1000.25);
                let _ = h.set_bucket_count(0.5, 1337.42);
                h
            },
            expected_err: None,
            expected_h: None,
            expected_fh: Some(FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 1337.42,
                sum: 1000.25,
                positive_spans: span(1),
                positive_buckets: vec![1337.42],
                custom_values: Some(Arc::from(&[0.5][..])),
                ..Default::default()
            }),
        },
        Case {
            name: "happy case integer bucket",
            setup: || {
                let mut h = TempHistogram::new();
                let _ = h.set_count(1000.0);
                let _ = h.set_sum(1000.25);
                let _ = h.set_bucket_count(0.5, 50.0);
                let _ = h.set_bucket_count(1.0, 950.0);
                let _ = h.set_bucket_count(f64::INFINITY, 1000.0);
                h
            },
            expected_err: None,
            expected_h: Some(Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 1000,
                sum: 1000.25,
                positive_spans: span(3),
                positive_buckets: vec![50, 850, -850],
                custom_values: Some(Arc::from(&[0.5, 1.0][..])),
                ..Default::default()
            }),
            expected_fh: None,
        },
        Case {
            name: "happy case float bucket",
            setup: || {
                let mut h = TempHistogram::new();
                let _ = h.set_count(1000.0);
                let _ = h.set_sum(1000.25);
                let _ = h.set_bucket_count(0.5, 50.0);
                let _ = h.set_bucket_count(1.0, 950.5);
                let _ = h.set_bucket_count(f64::INFINITY, 1000.0);
                h
            },
            expected_err: None,
            expected_h: None,
            expected_fh: Some(FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 1000.0,
                sum: 1000.25,
                positive_spans: span(3),
                positive_buckets: vec![50.0, 900.5, 49.5],
                custom_values: Some(Arc::from(&[0.5, 1.0][..])),
                ..Default::default()
            }),
        },
        Case {
            name: "non cumulative bucket",
            setup: || {
                let mut h = TempHistogram::new();
                let _ = h.set_count(1000.0);
                let _ = h.set_sum(1000.25);
                let _ = h.set_bucket_count(0.5, 50.0);
                let _ = h.set_bucket_count(1.0, 950.0);
                let _ = h.set_bucket_count(f64::INFINITY, 900.0);
                h
            },
            expected_err: Some(ErrKind::CountNotCumulative),
            expected_h: None,
            expected_fh: None,
        },
        Case {
            name: "negative count",
            setup: || {
                let mut h = TempHistogram::new();
                let _ = h.set_count(-1000.0);
                let _ = h.set_sum(1000.25);
                let _ = h.set_bucket_count(0.5, 50.0);
                let _ = h.set_bucket_count(1.0, 950.0);
                let _ = h.set_bucket_count(f64::INFINITY, 900.0);
                h
            },
            expected_err: Some(ErrKind::NegativeCount),
            expected_h: None,
            expected_fh: None,
        },
        Case {
            name: "NaN bucket upper bound",
            setup: || {
                let mut h = TempHistogram::new();
                // A real bucket first, so that the NaN call is not the first bucket.
                let _ = h.set_bucket_count(1.0, 5.0);
                let _ = h.set_bucket_count(f64::NAN, 10.0);
                h
            },
            expected_err: Some(ErrKind::NaNBucket),
            expected_h: None,
            expected_fh: None,
        },
        Case {
            name: "mixed order",
            setup: || {
                let mut h = TempHistogram::new();
                let _ = h.set_bucket_count(0.5, 50.0);
                let _ = h.set_bucket_count(f64::INFINITY, 1000.0);
                let _ = h.set_bucket_count(1.0, 950.0);
                let _ = h.set_count(1000.0);
                let _ = h.set_sum(1000.25);
                h
            },
            expected_err: None,
            expected_h: Some(Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 1000,
                sum: 1000.25,
                positive_spans: span(3),
                positive_buckets: vec![50, 850, -850],
                custom_values: Some(Arc::from(&[0.5, 1.0][..])),
                ..Default::default()
            }),
            expected_fh: None,
        },
    ];
    for test in tests {
        let th = (test.setup)();
        let res = th.convert();
        if let Some(kind) = test.expected_err {
            let err = res.expect_err(test.name);
            assert!(is(&err, kind), "{}: error {err:?} is not {kind:?}", test.name);
            continue;
        }
        let (h, fh) = res.unwrap_or_else(|e| panic!("{}: {e}", test.name));
        assert_eq!(test.expected_h, h, "{}", test.name);
        if let Some(h) = &h {
            h.validate().unwrap_or_else(|e| panic!("{}: {e}", test.name));
        }
        assert_eq!(test.expected_fh, fh, "{}", test.name);
        if let Some(fh) = &fh {
            fh.validate().unwrap_or_else(|e| panic!("{}: {e}", test.name));
        }
    }
}
