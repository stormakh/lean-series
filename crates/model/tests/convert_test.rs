//! Port of upstream `model/histogram/convert_test.go`.

use model::histogram::*;
use model::labels::{self, Builder, Labels};
use model::value::{STALE_NAN, is_stale_nan};

#[derive(Clone, Debug)]
struct Sample {
    lset: Labels,
    val: f64,
}

/// Go's `nhcb any`: a histogram, a float histogram, or nil.
enum Nhcb {
    H(Histogram),
    F(FloatHistogram),
    Nil,
}

impl Nhcb {
    fn as_ref(&self) -> Option<HistogramRef<'_>> {
        match self {
            Nhcb::H(h) => Some(HistogramRef::Int(h)),
            Nhcb::F(h) => Some(HistogramRef::Float(h)),
            Nhcb::Nil => None,
        }
    }
}

struct Case {
    name: &'static str,
    nhcb: Nhcb,
    labels: Labels,
    expect_err: bool,
    expected: Vec<Sample>,
}

fn s(ls: &[&str], val: f64) -> Sample {
    Sample { lset: labels::from_strings(ls), val }
}

fn sp(offset: i32, length: u32) -> Span {
    Span::new(offset, length)
}

fn stale() -> f64 {
    f64::from_bits(STALE_NAN)
}

#[test]
fn test_convert_nhcb_to_classic_histogram() {
    let tests = vec![
        Case {
            name: "valid histogram",
            nhcb: Nhcb::H(Histogram {
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
                positive_buckets: vec![10, 20, 30],
                positive_spans: vec![sp(0, 3)],
                count: 100,
                sum: 100.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            labels: labels::from_strings(&["__name__", "test_metric"]),
            expect_err: false,
            expected: vec![
                s(&["__name__", "test_metric_bucket", "le", "1.0"], 10.0),
                s(&["__name__", "test_metric_bucket", "le", "2.0"], 40.0),
                s(&["__name__", "test_metric_bucket", "le", "3.0"], 100.0),
                s(&["__name__", "test_metric_bucket", "le", "+Inf"], 100.0),
                s(&["__name__", "test_metric_count"], 100.0),
                s(&["__name__", "test_metric_sum"], 100.0),
            ],
        },
        Case {
            name: "valid floatHistogram",
            nhcb: Nhcb::F(FloatHistogram {
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
                positive_buckets: vec![20.0, 40.0, 60.0], // 20 -> 60 ->120
                positive_spans: vec![sp(0, 3)],
                count: 120.0,
                sum: 100.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            labels: labels::from_strings(&["__name__", "test_metric"]),
            expect_err: false,
            expected: vec![
                s(&["__name__", "test_metric_bucket", "le", "1.0"], 20.0),
                s(&["__name__", "test_metric_bucket", "le", "2.0"], 60.0),
                s(&["__name__", "test_metric_bucket", "le", "3.0"], 120.0),
                s(&["__name__", "test_metric_bucket", "le", "+Inf"], 120.0),
                s(&["__name__", "test_metric_count"], 120.0),
                s(&["__name__", "test_metric_sum"], 100.0),
            ],
        },
        Case {
            name: "empty histogram",
            nhcb: Nhcb::H(Histogram {
                custom_values: custom_values(&[]),
                positive_buckets: vec![],
                positive_spans: vec![],
                count: 0,
                sum: 0.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            labels: labels::from_strings(&["__name__", "test_metric"]),
            expect_err: false,
            expected: vec![
                s(&["__name__", "test_metric_bucket", "le", "+Inf"], 0.0),
                s(&["__name__", "test_metric_count"], 0.0),
                s(&["__name__", "test_metric_sum"], 0.0),
            ],
        },
        Case {
            name: "missing __name__ label",
            nhcb: Nhcb::H(Histogram {
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
                positive_buckets: vec![10, 20, 30],
                count: 100,
                sum: 100.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            labels: labels::from_strings(&["job", "test_job"]),
            expect_err: true,
            expected: vec![],
        },
        Case {
            name: "unsupported histogram type",
            nhcb: Nhcb::Nil,
            labels: labels::from_strings(&["__name__", "test_metric"]),
            expect_err: true,
            expected: vec![],
        },
        Case {
            name: "histogram with zero bucket counts",
            nhcb: Nhcb::H(Histogram {
                custom_values: custom_values(&[1.0, 2.0, 3.0]),
                positive_buckets: vec![0, 10, 0],
                positive_spans: vec![sp(0, 3)],
                count: 20,
                sum: 50.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            labels: labels::from_strings(&["__name__", "test_metric"]),
            expect_err: false,
            expected: vec![
                s(&["__name__", "test_metric_bucket", "le", "1.0"], 0.0),
                s(&["__name__", "test_metric_bucket", "le", "2.0"], 10.0),
                s(&["__name__", "test_metric_bucket", "le", "3.0"], 20.0),
                s(&["__name__", "test_metric_bucket", "le", "+Inf"], 20.0),
                s(&["__name__", "test_metric_count"], 20.0),
                s(&["__name__", "test_metric_sum"], 50.0),
            ],
        },
        Case {
            name: "extra bucket counts than custom values",
            nhcb: Nhcb::H(Histogram {
                custom_values: custom_values(&[1.0, 2.0]),
                positive_buckets: vec![10, 20, 30],
                positive_spans: vec![sp(0, 3)],
                count: 100,
                sum: 100.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            labels: labels::from_strings(&["__name__", "test_metric"]),
            expect_err: false,
            expected: vec![
                s(&["__name__", "test_metric_bucket", "le", "1.0"], 10.0),
                s(&["__name__", "test_metric_bucket", "le", "2.0"], 40.0),
                s(&["__name__", "test_metric_bucket", "le", "+Inf"], 100.0),
                s(&["__name__", "test_metric_count"], 100.0),
                s(&["__name__", "test_metric_sum"], 100.0),
            ],
        },
        Case {
            name: "mismatched bucket lengths with less filled bucket count",
            nhcb: Nhcb::H(Histogram {
                custom_values: custom_values(&[1.0, 2.0]),
                positive_buckets: vec![10],
                positive_spans: vec![sp(0, 2)],
                count: 100,
                sum: 100.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            labels: labels::from_strings(&["__name__", "test_metric_bucket"]),
            expect_err: true,
            expected: vec![],
        },
        Case {
            name: "single series Histogram",
            nhcb: Nhcb::H(Histogram {
                custom_values: custom_values(&[1.0]),
                positive_buckets: vec![10],
                positive_spans: vec![sp(0, 1)],
                count: 10,
                sum: 20.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            labels: labels::from_strings(&["__name__", "test_metric"]),
            expect_err: false,
            expected: vec![
                s(&["__name__", "test_metric_bucket", "le", "1.0"], 10.0),
                s(&["__name__", "test_metric_bucket", "le", "+Inf"], 10.0),
                s(&["__name__", "test_metric_count"], 10.0),
                s(&["__name__", "test_metric_sum"], 20.0),
            ],
        },
        Case {
            name: "multiset label histogram",
            nhcb: Nhcb::H(Histogram {
                custom_values: custom_values(&[1.0]),
                positive_buckets: vec![10],
                positive_spans: vec![sp(0, 1)],
                count: 10,
                sum: 20.0,
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            labels: labels::from_strings(&["__name__", "test_metric", "job", "test_job", "instance", "localhost:9090"]),
            expect_err: false,
            expected: vec![
                s(
                    &["__name__", "test_metric_bucket", "job", "test_job", "instance", "localhost:9090", "le", "1.0"],
                    10.0,
                ),
                s(
                    &["__name__", "test_metric_bucket", "job", "test_job", "instance", "localhost:9090", "le", "+Inf"],
                    10.0,
                ),
                s(&["__name__", "test_metric_count", "job", "test_job", "instance", "localhost:9090"], 10.0),
                s(&["__name__", "test_metric_sum", "job", "test_job", "instance", "localhost:9090"], 20.0),
            ],
        },
        Case {
            name: "exponential histogram",
            nhcb: Nhcb::F(FloatHistogram {
                schema: 1,
                zero_threshold: 0.01,
                zero_count: 5.5,
                count: 3493.3,
                sum: 2349209.324,
                positive_spans: vec![sp(-2, 1), sp(2, 3)],
                positive_buckets: vec![1.0, 3.3, 4.2, 0.1],
                negative_spans: vec![sp(3, 2), sp(3, 2)],
                negative_buckets: vec![3.1, 3.0, 1.234e5, 1000.0],
                ..Default::default()
            }),
            labels: labels::from_strings(&["__name__", "test_metric_bucket"]),
            expect_err: true,
            expected: vec![],
        },
        Case {
            name: "sparse histogram",
            nhcb: Nhcb::H(Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                positive_spans: vec![sp(0, 2), sp(4, 1), sp(1, 2)],
                positive_buckets: vec![1, 2, 3, 4, 5], // 1 -> 3 -> 0 -> 0 -> 0 -> 0 -> 6 -> 0 -> 10 -> 15
                count: 35,                             // 1 -> 4 -> 4 -> 4 -> 4 -> 4 -> 10 -> 10 -> 20 -> 35
                sum: 123.0,
                ..Default::default()
            }),
            labels: labels::from_strings(&["__name__", "test_metric"]),
            expect_err: false,
            expected: vec![
                s(&["__name__", "test_metric_bucket", "le", "1.0"], 1.0),
                s(&["__name__", "test_metric_bucket", "le", "2.0"], 4.0),
                s(&["__name__", "test_metric_bucket", "le", "3.0"], 4.0),
                s(&["__name__", "test_metric_bucket", "le", "4.0"], 4.0),
                s(&["__name__", "test_metric_bucket", "le", "5.0"], 4.0),
                s(&["__name__", "test_metric_bucket", "le", "6.0"], 4.0),
                s(&["__name__", "test_metric_bucket", "le", "7.0"], 10.0),
                s(&["__name__", "test_metric_bucket", "le", "8.0"], 10.0),
                s(&["__name__", "test_metric_bucket", "le", "9.0"], 20.0),
                s(&["__name__", "test_metric_bucket", "le", "10.0"], 35.0),
                s(&["__name__", "test_metric_bucket", "le", "+Inf"], 35.0),
                s(&["__name__", "test_metric_count"], 35.0),
                s(&["__name__", "test_metric_sum"], 123.0),
            ],
        },
        Case {
            name: "sparse float histogram",
            nhcb: Nhcb::F(FloatHistogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                positive_spans: vec![sp(0, 2), sp(4, 1), sp(1, 2)],
                positive_buckets: vec![1.0, 2.0, 3.0, 4.0, 5.0], // 1 -> 2 -> 0 -> 0 -> 0 -> 0 -> 3 -> 0 -> 4 -> 5
                count: 15.0,                                     // 1 -> 3 -> 3 -> 3 -> 3 -> 3 -> 6 -> 6 -> 10 -> 15
                sum: 123.0,
                ..Default::default()
            }),
            labels: labels::from_strings(&["__name__", "test_metric"]),
            expect_err: false,
            expected: vec![
                s(&["__name__", "test_metric_bucket", "le", "1.0"], 1.0),
                s(&["__name__", "test_metric_bucket", "le", "2.0"], 3.0),
                s(&["__name__", "test_metric_bucket", "le", "3.0"], 3.0),
                s(&["__name__", "test_metric_bucket", "le", "4.0"], 3.0),
                s(&["__name__", "test_metric_bucket", "le", "5.0"], 3.0),
                s(&["__name__", "test_metric_bucket", "le", "6.0"], 3.0),
                s(&["__name__", "test_metric_bucket", "le", "7.0"], 6.0),
                s(&["__name__", "test_metric_bucket", "le", "8.0"], 6.0),
                s(&["__name__", "test_metric_bucket", "le", "9.0"], 10.0),
                s(&["__name__", "test_metric_bucket", "le", "10.0"], 15.0),
                s(&["__name__", "test_metric_bucket", "le", "+Inf"], 15.0),
                s(&["__name__", "test_metric_count"], 15.0),
                s(&["__name__", "test_metric_sum"], 123.0),
            ],
        },
        Case {
            name: "stale histogram",
            nhcb: Nhcb::H(Histogram {
                custom_values: custom_values(&[1.0, 2.0]),
                positive_buckets: vec![2, 1, 1],
                positive_spans: vec![sp(0, 3)],
                count: 9,
                sum: stale(),
                schema: CUSTOM_BUCKETS_SCHEMA,
                ..Default::default()
            }),
            labels: labels::from_strings(&["__name__", "test_metric"]),
            expect_err: false,
            expected: vec![
                s(&["__name__", "test_metric_bucket", "le", "1.0"], stale()),
                s(&["__name__", "test_metric_bucket", "le", "2.0"], stale()),
                s(&["__name__", "test_metric_bucket", "le", "+Inf"], stale()),
                s(&["__name__", "test_metric_count"], stale()),
                s(&["__name__", "test_metric_sum"], stale()),
            ],
        },
    ];

    let mut label_builder = Builder::new(labels::from_strings::<&str>(&[]));
    for tt in &tests {
        let mut emitted: Vec<Sample> = Vec::new();
        let err = convert_nhcb_to_classic::<Error, _>(tt.nhcb.as_ref(), &tt.labels, &mut label_builder, |lbls, val| {
            emitted.push(Sample { lset: lbls, val });
            Ok(())
        });
        assert_eq!(tt.expect_err, err.is_err(), "{}: unexpected error: {:?}", tt.name, err.err());
        if tt.expect_err {
            continue;
        }
        assert_eq!(tt.expected.len(), emitted.len(), "{}", tt.name);
        for (i, exp) in tt.expected.iter().enumerate() {
            assert!(
                labels::equal(&exp.lset, &emitted[i].lset),
                "{}: labels mismatch at index {}: expected {}, got {}",
                tt.name,
                i,
                exp.lset,
                emitted[i].lset
            );
            if is_stale_nan(exp.val) {
                assert!(is_stale_nan(emitted[i].val), "{}: expected stale NaN at index {}", tt.name, i);
            } else {
                assert_eq!(exp.val, emitted[i].val, "{}: value mismatch at index {}", tt.name, i);
            }
        }

        // A reused buffer must not leak earlier values into skipped buckets.
        let (buckets, count, sum) = classic_bucket_counts(tt.nhcb.as_ref(), vec![42.0; 16]).unwrap();
        let mut counts = buckets;
        counts.push(count);
        counts.push(sum);
        assert_eq!(emitted.len(), counts.len(), "{}", tt.name);
        for (i, s) in emitted.iter().enumerate() {
            if is_stale_nan(s.val) {
                assert!(is_stale_nan(counts[i]), "{}: expected stale NaN count at index {}", tt.name, i);
            } else {
                assert_eq!(s.val, counts[i], "{}: count mismatch at index {}", tt.name, i);
            }
        }
    }
}

/// Checks that an integer NHCB and its exact FloatHistogram representation convert to the same classic series,
/// and that the +Inf bucket matches the count.
#[test]
fn test_convert_nhcb_to_classic_int_float_agreement() {
    let h = Histogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
        positive_spans: vec![sp(0, 2), sp(4, 1), sp(1, 2)],
        positive_buckets: vec![1, 2, 3, 4, 5],
        count: 35,
        sum: 123.0,
        ..Default::default()
    };
    h.validate().unwrap();

    let convert = |nhcb: HistogramRef<'_>| -> Vec<Sample> {
        let mut got = Vec::new();
        let mut lb = Builder::new(labels::from_strings::<&str>(&[]));
        convert_nhcb_to_classic::<Error, _>(
            Some(nhcb),
            &labels::from_strings(&["__name__", "test_metric"]),
            &mut lb,
            |l, v| {
                got.push(Sample { lset: l, val: v });
                Ok(())
            },
        )
        .unwrap();
        got
    };

    let from_int = convert(HistogramRef::Int(&h));
    let fh = h.to_float();
    let from_float = convert(HistogramRef::Float(&fh));

    assert_eq!(from_float.len(), from_int.len());
    for i in 0..from_int.len() {
        assert!(labels::equal(&from_int[i].lset, &from_float[i].lset), "labels mismatch at index {}", i);
        assert_eq!(from_float[i].val, from_int[i].val, "value mismatch at index {} for {}", i, from_int[i].lset);
    }

    // In a classic histogram the +Inf bucket holds every observation.
    let (mut inf_bucket, mut count) = (0.0, 0.0);
    for s in &from_int {
        match s.lset.get(labels::METRIC_NAME) {
            "test_metric_bucket" => {
                if s.lset.get(labels::BUCKET_LABEL) == "+Inf" {
                    inf_bucket = s.val;
                }
            }
            "test_metric_count" => count = s.val,
            _ => {}
        }
    }
    assert_eq!(count, inf_bucket);
}
