//! Port of upstream `model/histogram/histogram_test.go` (benchmarks are not ported).

use model::histogram::*;

/// Go `value.StaleNaN` (model/value): the bit pattern of the stale marker.
const STALE_NAN: u64 = 0x7ff0000000000002;

fn collect<B, I: BucketIterator<B>>(mut it: I) -> Vec<Bucket<B>> {
    let mut v = Vec::new();
    while it.next() {
        v.push(it.at());
    }
    v
}

#[derive(Default)]
struct StringCase {
    histogram: Histogram,
    expected_string: &'static str,
}

#[test]
fn test_histogram_string() {
    let cases: Vec<StringCase> = vec![
        StringCase {
            histogram: Histogram {
                schema: 0,
                ..Default::default()
            },
            expected_string: "{count:0, sum:0}",
        },
        StringCase {
            histogram: Histogram {
                schema: 0,
                count: 9,
                sum: -3.1415,
                zero_count: 12,
                zero_threshold: 0.001,
                negative_spans: vec![Span::new(0, 5), Span::new(1, 1)],
                negative_buckets: vec![1, 2, -2, 1, -1, 0],
                ..Default::default()
            },
            expected_string: "{count:9, sum:-3.1415, [-64,-32):1, [-16,-8):1, [-8,-4):2, [-4,-2):1, [-2,-1):3, [-1,-0.5):1, [-0.001,0.001]:12}",
        },
        StringCase {
            histogram: Histogram {
                schema: 0,
                count: 19,
                sum: 2.7,
                positive_spans: vec![Span::new(0, 4), Span::new(0, 0), Span::new(0, 3)],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                negative_spans: vec![Span::new(0, 5), Span::new(1, 0), Span::new(0, 1)],
                negative_buckets: vec![1, 2, -2, 1, -1, 0],
                ..Default::default()
            },
            expected_string: "{count:19, sum:2.7, [-64,-32):1, [-16,-8):1, [-8,-4):2, [-4,-2):1, [-2,-1):3, [-1,-0.5):1, (0.5,1]:1, (1,2]:3, (2,4]:1, (4,8]:2, (8,16]:1, (16,32]:1, (32,64]:1}",
        },
        StringCase {
            histogram: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 19,
                sum: 2.7,
                positive_spans: vec![Span::new(0, 4), Span::new(0, 0), Span::new(0, 3)],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                custom_values: custom_values(&[1.0, 2.0, 5.0, 10.0, 15.0, 20.0, 25.0, 50.0]),
                ..Default::default()
            },
            expected_string: "{count:19, sum:2.7, [-Inf,1]:1, (1,2]:3, (2,5]:1, (5,10]:2, (10,15]:1, (15,20]:1, (20,25]:1}",
        },
    ];
    for (i, c) in cases.iter().enumerate() {
        let actual_string = c.histogram.string();
        assert_eq!(c.expected_string, actual_string, "case {}", i);
    }
}

#[derive(Default)]
struct CumCase {
    histogram: Histogram,
    expected_buckets: Vec<Bucket<u64>>,
}

#[test]
fn test_cumulative_bucket_iterator() {
    let cases: Vec<CumCase> = vec![
        CumCase {
            histogram: Histogram {
                schema: 0,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                positive_buckets: vec![1, 1, -1, 0],
                ..Default::default()
            },
            expected_buckets: vec![
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 1.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 2.0,
                    count: 3,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 4.0,
                    count: 3,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 2,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 8.0,
                    count: 4,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 16.0,
                    count: 5,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 4,
                },
            ],
        },
        CumCase {
            histogram: Histogram {
                schema: 0,
                positive_spans: vec![Span::new(0, 5), Span::new(1, 1)],
                positive_buckets: vec![1, 2, -2, 1, -1, 0],
                ..Default::default()
            },
            expected_buckets: vec![
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 1.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 2.0,
                    count: 4,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 4.0,
                    count: 5,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 2,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 8.0,
                    count: 7,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 16.0,
                    count: 8,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 4,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 32.0,
                    count: 8,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 5,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 64.0,
                    count: 9,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 6,
                },
            ],
        },
        CumCase {
            histogram: Histogram {
                schema: 0,
                positive_spans: vec![Span::new(0, 7)],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                ..Default::default()
            },
            expected_buckets: vec![
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 1.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 2.0,
                    count: 4,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 4.0,
                    count: 5,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 2,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 8.0,
                    count: 7,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 16.0,
                    count: 8,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 4,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 32.0,
                    count: 9,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 5,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 64.0,
                    count: 10,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 6,
                },
            ],
        },
        CumCase {
            histogram: Histogram {
                schema: 3,
                positive_spans: vec![Span::new(-5, 2), Span::new(2, 3), Span::new(2, 2)],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 3],
                ..Default::default()
            },
            expected_buckets: vec![
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 0.6484197773255048,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: -5,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 0.7071067811865475,
                    count: 4,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: -4,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 0.7711054127039704,
                    count: 4,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: -3,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 0.8408964152537144,
                    count: 4,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: -2,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 0.9170040432046711,
                    count: 5,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: -1,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 1.0,
                    count: 7,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 1.0905077326652577,
                    count: 8,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 1.189207115002721,
                    count: 8,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 2,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 1.2968395546510096,
                    count: 8,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 1.414213562373095,
                    count: 9,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 4,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 1.5422108254079407,
                    count: 13,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 5,
                },
            ],
        },
        CumCase {
            histogram: Histogram {
                schema: -2,
                positive_spans: vec![Span::new(-2, 4), Span::new(2, 2)],
                positive_buckets: vec![1, 2, -2, 1, -1, 0],
                ..Default::default()
            },
            expected_buckets: vec![
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 0.00390625,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: -2,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 0.0625,
                    count: 4,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: -1,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 1.0,
                    count: 5,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 16.0,
                    count: 7,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 256.0,
                    count: 7,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 2,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 4096.0,
                    count: 7,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 65536.0,
                    count: 8,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 4,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 1048576.0,
                    count: 9,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 5,
                },
            ],
        },
        CumCase {
            histogram: Histogram {
                schema: -1,
                positive_spans: vec![Span::new(-2, 5)],
                positive_buckets: vec![1, 2, -2, 1, -1],
                ..Default::default()
            },
            expected_buckets: vec![
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 0.0625,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: -2,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 0.25,
                    count: 4,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: -1,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 1.0,
                    count: 5,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 4.0,
                    count: 7,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 16.0,
                    count: 8,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 2,
                },
            ],
        },
        CumCase {
            histogram: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                positive_buckets: vec![1, 1, -1, 0],
                custom_values: custom_values(&[5.0, 10.0, 20.0, 50.0]),
                ..Default::default()
            },
            expected_buckets: vec![
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 5.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 10.0,
                    count: 3,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 20.0,
                    count: 3,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 2,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 50.0,
                    count: 4,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: f64::INFINITY,
                    count: 5,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 4,
                },
            ],
        },
        CumCase {
            histogram: Histogram {
                schema: 0,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                positive_buckets: vec![1, 1, -1, 0, 2],
                ..Default::default()
            },
            expected_buckets: vec![
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 1.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 2.0,
                    count: 3,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 4.0,
                    count: 3,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 2,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 8.0,
                    count: 4,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 16.0,
                    count: 5,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 4,
                },
            ],
        },
        CumCase {
            histogram: Histogram {
                schema: 0,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 3)],
                positive_buckets: vec![1, 1, -1, 0],
                ..Default::default()
            },
            expected_buckets: vec![
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 1.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 2.0,
                    count: 3,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 4.0,
                    count: 3,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 2,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 8.0,
                    count: 4,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 16.0,
                    count: 5,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 4,
                },
            ],
        },
    ];
    for (i, c) in cases.iter().enumerate() {
        let actual_buckets = collect(c.histogram.cumulative_bucket_iterator());
        assert_eq!(c.expected_buckets, actual_buckets, "case {}", i);
    }
}

#[derive(Default)]
struct RegCase {
    histogram: Histogram,
    expected_positive_buckets: Vec<Bucket<u64>>,
    expected_negative_buckets: Vec<Bucket<u64>>,
}

#[test]
fn test_regular_bucket_iterator() {
    let cases: Vec<RegCase> = vec![
        RegCase {
            histogram: Histogram {
                schema: 0,
                ..Default::default()
            },
            expected_positive_buckets: vec![],
            expected_negative_buckets: vec![],
        },
        RegCase {
            histogram: Histogram {
                schema: 0,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                positive_buckets: vec![1, 1, -1, 0],
                ..Default::default()
            },
            expected_positive_buckets: vec![
                Bucket::<u64> {
                    lower: 0.5,
                    upper: 1.0,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: 1.0,
                    upper: 2.0,
                    count: 2,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: 4.0,
                    upper: 8.0,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: 8.0,
                    upper: 16.0,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 4,
                },
            ],
            expected_negative_buckets: vec![],
        },
        RegCase {
            histogram: Histogram {
                schema: 0,
                negative_spans: vec![Span::new(0, 5), Span::new(1, 1)],
                negative_buckets: vec![1, 2, -2, 1, -1, 0],
                ..Default::default()
            },
            expected_positive_buckets: vec![],
            expected_negative_buckets: vec![
                Bucket::<u64> {
                    lower: -1.0,
                    upper: -0.5,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: -2.0,
                    upper: -1.0,
                    count: 3,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: -4.0,
                    upper: -2.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 2,
                },
                Bucket::<u64> {
                    lower: -8.0,
                    upper: -4.0,
                    count: 2,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: -16.0,
                    upper: -8.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 4,
                },
                Bucket::<u64> {
                    lower: -64.0,
                    upper: -32.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 6,
                },
            ],
        },
        RegCase {
            histogram: Histogram {
                schema: 0,
                positive_spans: vec![Span::new(0, 4), Span::new(0, 0), Span::new(0, 3)],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                negative_spans: vec![Span::new(0, 5), Span::new(1, 0), Span::new(0, 1)],
                negative_buckets: vec![1, 2, -2, 1, -1, 0],
                ..Default::default()
            },
            expected_positive_buckets: vec![
                Bucket::<u64> {
                    lower: 0.5,
                    upper: 1.0,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: 1.0,
                    upper: 2.0,
                    count: 3,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: 2.0,
                    upper: 4.0,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 2,
                },
                Bucket::<u64> {
                    lower: 4.0,
                    upper: 8.0,
                    count: 2,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: 8.0,
                    upper: 16.0,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 4,
                },
                Bucket::<u64> {
                    lower: 16.0,
                    upper: 32.0,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 5,
                },
                Bucket::<u64> {
                    lower: 32.0,
                    upper: 64.0,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 6,
                },
            ],
            expected_negative_buckets: vec![
                Bucket::<u64> {
                    lower: -1.0,
                    upper: -0.5,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: -2.0,
                    upper: -1.0,
                    count: 3,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: -4.0,
                    upper: -2.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 2,
                },
                Bucket::<u64> {
                    lower: -8.0,
                    upper: -4.0,
                    count: 2,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: -16.0,
                    upper: -8.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 4,
                },
                Bucket::<u64> {
                    lower: -64.0,
                    upper: -32.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 6,
                },
            ],
        },
        RegCase {
            histogram: Histogram {
                schema: 3,
                positive_spans: vec![Span::new(-5, 2), Span::new(2, 3), Span::new(2, 2)],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 3],
                ..Default::default()
            },
            expected_positive_buckets: vec![
                Bucket::<u64> {
                    lower: 0.5946035575013605,
                    upper: 0.6484197773255048,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: -5,
                },
                Bucket::<u64> {
                    lower: 0.6484197773255048,
                    upper: 0.7071067811865475,
                    count: 3,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: -4,
                },
                Bucket::<u64> {
                    lower: 0.8408964152537144,
                    upper: 0.9170040432046711,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: -1,
                },
                Bucket::<u64> {
                    lower: 0.9170040432046711,
                    upper: 1.0,
                    count: 2,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: 1.0,
                    upper: 1.0905077326652577,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: 1.2968395546510096,
                    upper: 1.414213562373095,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 4,
                },
                Bucket::<u64> {
                    lower: 1.414213562373095,
                    upper: 1.5422108254079407,
                    count: 4,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 5,
                },
            ],
            expected_negative_buckets: vec![],
        },
        RegCase {
            histogram: Histogram {
                schema: -2,
                positive_spans: vec![Span::new(-2, 4), Span::new(2, 2)],
                positive_buckets: vec![1, 2, -2, 1, -1, 0],
                ..Default::default()
            },
            expected_positive_buckets: vec![
                Bucket::<u64> {
                    lower: 0.000244140625,
                    upper: 0.00390625,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: -2,
                },
                Bucket::<u64> {
                    lower: 0.00390625,
                    upper: 0.0625,
                    count: 3,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: -1,
                },
                Bucket::<u64> {
                    lower: 0.0625,
                    upper: 1.0,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: 1.0,
                    upper: 16.0,
                    count: 2,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: 4096.0,
                    upper: 65536.0,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 4,
                },
                Bucket::<u64> {
                    lower: 65536.0,
                    upper: 1048576.0,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 5,
                },
            ],
            expected_negative_buckets: vec![],
        },
        RegCase {
            histogram: Histogram {
                schema: -1,
                positive_spans: vec![Span::new(-2, 5)],
                positive_buckets: vec![1, 2, -2, 1, -1],
                ..Default::default()
            },
            expected_positive_buckets: vec![
                Bucket::<u64> {
                    lower: 0.015625,
                    upper: 0.0625,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: -2,
                },
                Bucket::<u64> {
                    lower: 0.0625,
                    upper: 0.25,
                    count: 3,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: -1,
                },
                Bucket::<u64> {
                    lower: 0.25,
                    upper: 1.0,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: 1.0,
                    upper: 4.0,
                    count: 2,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: 4.0,
                    upper: 16.0,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 2,
                },
            ],
            expected_negative_buckets: vec![],
        },
        RegCase {
            histogram: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                positive_buckets: vec![1, 1, -1, 0],
                custom_values: custom_values(&[5.0, 10.0, 20.0, 50.0]),
                ..Default::default()
            },
            expected_positive_buckets: vec![
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 5.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: 5.0,
                    upper: 10.0,
                    count: 2,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: 20.0,
                    upper: 50.0,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: 50.0,
                    upper: f64::INFINITY,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 4,
                },
            ],
            expected_negative_buckets: vec![],
        },
        RegCase {
            histogram: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                positive_buckets: vec![1, 1, -1, 0],
                custom_values: custom_values(&[0.0, 10.0, 20.0, 50.0]),
                ..Default::default()
            },
            expected_positive_buckets: vec![
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: 0.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: 0.0,
                    upper: 10.0,
                    count: 2,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: 20.0,
                    upper: 50.0,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: 50.0,
                    upper: f64::INFINITY,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 4,
                },
            ],
            expected_negative_buckets: vec![],
        },
        RegCase {
            histogram: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(0, 5)],
                positive_buckets: vec![1, 1, 0, -1, 0],
                custom_values: custom_values(&[-5.0, 0.0, 20.0, 50.0]),
                ..Default::default()
            },
            expected_positive_buckets: vec![
                Bucket::<u64> {
                    lower: f64::NEG_INFINITY,
                    upper: -5.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: true,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: -5.0,
                    upper: 0.0,
                    count: 2,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: 0.0,
                    upper: 20.0,
                    count: 2,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 2,
                },
                Bucket::<u64> {
                    lower: 20.0,
                    upper: 50.0,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: 50.0,
                    upper: f64::INFINITY,
                    count: 1,
                    lower_inclusive: false,
                    upper_inclusive: true,
                    index: 4,
                },
            ],
            expected_negative_buckets: vec![],
        },
        RegCase {
            histogram: Histogram {
                schema: 0,
                negative_spans: vec![Span::new(0, 5), Span::new(1, 1)],
                negative_buckets: vec![1, 2, -2, 1, -1, 0, 3],
                ..Default::default()
            },
            expected_positive_buckets: vec![],
            expected_negative_buckets: vec![
                Bucket::<u64> {
                    lower: -1.0,
                    upper: -0.5,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: -2.0,
                    upper: -1.0,
                    count: 3,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: -4.0,
                    upper: -2.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 2,
                },
                Bucket::<u64> {
                    lower: -8.0,
                    upper: -4.0,
                    count: 2,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: -16.0,
                    upper: -8.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 4,
                },
                Bucket::<u64> {
                    lower: -64.0,
                    upper: -32.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 6,
                },
            ],
        },
        RegCase {
            histogram: Histogram {
                schema: 0,
                negative_spans: vec![Span::new(0, 5), Span::new(1, 1)],
                negative_buckets: vec![1, 2, -2, 1, -1],
                ..Default::default()
            },
            expected_positive_buckets: vec![],
            expected_negative_buckets: vec![
                Bucket::<u64> {
                    lower: -1.0,
                    upper: -0.5,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 0,
                },
                Bucket::<u64> {
                    lower: -2.0,
                    upper: -1.0,
                    count: 3,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 1,
                },
                Bucket::<u64> {
                    lower: -4.0,
                    upper: -2.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 2,
                },
                Bucket::<u64> {
                    lower: -8.0,
                    upper: -4.0,
                    count: 2,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 3,
                },
                Bucket::<u64> {
                    lower: -16.0,
                    upper: -8.0,
                    count: 1,
                    lower_inclusive: true,
                    upper_inclusive: false,
                    index: 4,
                },
            ],
        },
    ];
    for (i, c) in cases.iter().enumerate() {
        let actual_positive_buckets = collect(c.histogram.positive_bucket_iterator());
        assert_eq!(
            c.expected_positive_buckets, actual_positive_buckets,
            "case {}",
            i
        );
        let actual_negative_buckets = collect(c.histogram.negative_bucket_iterator());
        assert_eq!(
            c.expected_negative_buckets, actual_negative_buckets,
            "case {}",
            i
        );
    }
}

#[derive(Default)]
struct ToFloatCase {
    name: &'static str,
    fh: Option<FloatHistogram>,
}

#[test]
fn test_histogram_to_float() {
    let h = Histogram {
        schema: 3,
        count: 61,
        sum: 2.7,
        zero_threshold: 0.1,
        zero_count: 42,
        positive_spans: vec![Span::new(0, 4), Span::new(0, 0), Span::new(0, 3)],
        positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
        negative_spans: vec![Span::new(0, 5), Span::new(1, 0), Span::new(0, 1)],
        negative_buckets: vec![1, 2, -2, 1, -1, 0],
        ..Default::default()
    };
    let cases: Vec<ToFloatCase> = vec![
        ToFloatCase {
            name: "without prior float histogram",
            ..Default::default()
        },
        ToFloatCase {
            name: "prior float histogram with more buckets",
            fh: Some(FloatHistogram {
                schema: 2,
                count: 3.0,
                sum: 5.0,
                zero_threshold: 4.0,
                zero_count: 1.0,
                positive_spans: vec![Span::new(1, 2), Span::new(1, 2), Span::new(1, 2)],
                positive_buckets: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
                negative_spans: vec![Span::new(20, 6), Span::new(12, 7), Span::new(33, 10)],
                negative_buckets: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
                ..Default::default()
            }),
        },
        ToFloatCase {
            name: "prior float histogram with fewer buckets",
            fh: Some(FloatHistogram {
                schema: 2,
                count: 3.0,
                sum: 5.0,
                zero_threshold: 4.0,
                zero_count: 1.0,
                positive_spans: vec![Span::new(1, 2), Span::new(1, 2), Span::new(1, 2)],
                positive_buckets: vec![1.0, 2.0],
                negative_spans: vec![Span::new(20, 6), Span::new(12, 7), Span::new(33, 10)],
                negative_buckets: vec![1.0, 2.0],
                ..Default::default()
            }),
        },
    ];
    for c in cases {
        let fh = match c.fh {
            None => h.to_float(),
            Some(mut fh) => {
                h.to_float_into(&mut fh);
                fh
            }
        };
        assert_eq!(h.string(), fh.string(), "{}", c.name);
    }
}

#[test]
fn test_custom_buckets_histogram_to_float() {
    let h = Histogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count: 10,
        sum: 2.7,
        positive_spans: vec![Span::new(0, 4), Span::new(0, 0), Span::new(0, 3)],
        positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
        custom_values: custom_values(&[5.0, 10.0, 20.0, 50.0, 100.0, 500.0]),
        ..Default::default()
    };
    let cases: Vec<ToFloatCase> = vec![
        ToFloatCase {
            name: "without prior float histogram",
            ..Default::default()
        },
        ToFloatCase {
            name: "prior float histogram with more buckets",
            fh: Some(FloatHistogram {
                schema: 2,
                count: 3.0,
                sum: 5.0,
                zero_threshold: 4.0,
                zero_count: 1.0,
                positive_spans: vec![Span::new(1, 2), Span::new(1, 2), Span::new(1, 2)],
                positive_buckets: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
                negative_spans: vec![Span::new(20, 6), Span::new(12, 7), Span::new(33, 10)],
                negative_buckets: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
                ..Default::default()
            }),
        },
        ToFloatCase {
            name: "prior float histogram with fewer buckets",
            fh: Some(FloatHistogram {
                schema: 2,
                count: 3.0,
                sum: 5.0,
                zero_threshold: 4.0,
                zero_count: 1.0,
                positive_spans: vec![Span::new(1, 2), Span::new(1, 2), Span::new(1, 2)],
                positive_buckets: vec![1.0, 2.0],
                negative_spans: vec![Span::new(20, 6), Span::new(12, 7), Span::new(33, 10)],
                negative_buckets: vec![1.0, 2.0],
                ..Default::default()
            }),
        },
    ];
    h.validate().unwrap();
    for c in cases {
        let h_str = h.string();
        let fh = match c.fh {
            None => h.to_float(),
            Some(mut fh) => {
                h.to_float_into(&mut fh);
                fh
            }
        };
        fh.validate()
            .unwrap_or_else(|e| panic!("{}: {}", c.name, e));
        assert_eq!(h_str, h.string(), "{}", c.name);
        assert_eq!(h_str, fh.string(), "{}", c.name);
    }
}

fn equals(h1: &Histogram, h2: &Histogram) {
    assert!(h1.equals(h2));
    assert!(h2.equals(h1));
    let (h1f, h2f) = (h1.to_float(), h2.to_float());
    assert!(h1f.equals(&h2f));
    assert!(h2f.equals(&h1f));
}

fn not_equals(h1: &Histogram, h2: &Histogram) {
    assert!(!h1.equals(h2));
    assert!(!h2.equals(h1));
    let (h1f, h2f) = (h1.to_float(), h2.to_float());
    assert!(!h1f.equals(&h2f));
    assert!(!h2f.equals(&h1f));
}

fn not_equals_until_float_conv(h1: &Histogram, h2: &Histogram) {
    assert!(!h1.equals(h2));
    assert!(!h2.equals(h1));
    let (h1f, h2f) = (h1.to_float(), h2.to_float());
    assert!(h1f.equals(&h2f));
    assert!(h2f.equals(&h1f));
}

/// Tests both Histogram and FloatHistogram.
#[test]
fn test_histogram_equals() {
    let h1 = Histogram {
        schema: 3,
        count: 62,
        sum: 2.7,
        zero_threshold: 0.1,
        zero_count: 42,
        positive_spans: vec![Span::new(0, 4), Span::new(10, 3)],
        positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
        negative_spans: vec![Span::new(0, 4), Span::new(10, 3)],
        negative_buckets: vec![1, 2, -2, 1, -1, 0, 0],
        ..Default::default()
    };

    h1.validate().unwrap();

    let mut h2 = h1.copy();
    equals(&h1, &h2);

    // Changed spans but same layout.
    h2.positive_spans.push(Span::new(5, 0));
    h2.negative_spans.push(Span::new(2, 0));
    equals(&h1, &h2);
    // Adding empty spans in between.
    h2.positive_spans[1].offset = 6;
    h2.positive_spans = vec![
        h2.positive_spans[0],
        Span::new(1, 0),
        Span::new(3, 0),
        h2.positive_spans[1],
        h2.positive_spans[2],
    ];
    h2.negative_spans[1].offset = 5;
    h2.negative_spans = vec![
        h2.negative_spans[0],
        Span::new(2, 0),
        Span::new(3, 0),
        h2.negative_spans[1],
        h2.negative_spans[2],
    ];
    equals(&h1, &h2);

    // All mismatches.
    not_equals(&h1, &Histogram::default());

    h2.schema = 1;
    not_equals(&h1, &h2);

    let mut h2 = h1.copy();
    h2.count += 1;
    not_equals(&h1, &h2);

    let mut h2 = h1.copy();
    h2.sum += 1.0;
    not_equals(&h1, &h2);

    let mut h2 = h1.copy();
    h2.zero_threshold += 1.0;
    not_equals(&h1, &h2);

    let mut h2 = h1.copy();
    h2.zero_count += 1;
    not_equals(&h1, &h2);

    // Changing value of buckets.
    let mut h2 = h1.copy();
    *h2.positive_buckets.last_mut().unwrap() += 1;
    not_equals(&h1, &h2);
    let mut h2 = h1.copy();
    *h2.negative_buckets.last_mut().unwrap() += 1;
    not_equals(&h1, &h2);

    // Changing bucket layout.
    let mut h2 = h1.copy();
    h2.positive_spans[1].offset += 1;
    not_equals(&h1, &h2);
    let mut h2 = h1.copy();
    h2.negative_spans[1].offset += 1;
    not_equals(&h1, &h2);

    // Adding an empty bucket.
    let mut h2 = h1.copy();
    h2.positive_spans[0].offset -= 1;
    h2.positive_spans[0].length += 1;
    h2.positive_buckets.insert(0, 0);
    not_equals(&h1, &h2);
    let mut h2 = h1.copy();
    h2.negative_spans[0].offset -= 1;
    h2.negative_spans[0].length += 1;
    h2.negative_buckets.insert(0, 0);
    not_equals(&h1, &h2);

    // Adding new bucket.
    let mut h2 = h1.copy();
    h2.positive_spans.push(Span::new(1, 1));
    h2.positive_buckets.push(1);
    not_equals(&h1, &h2);
    let mut h2 = h1.copy();
    h2.negative_spans.push(Span::new(1, 1));
    h2.negative_buckets.push(1);
    not_equals(&h1, &h2);

    // Sum is StaleNaN.
    let mut h_stale = h1.copy();
    h_stale.sum = f64::from_bits(STALE_NAN);
    not_equals(&h1, &h_stale);
    equals(&h_stale, &h_stale);

    // Sum is NaN (but not a StaleNaN).
    let mut h_nan = h1.copy();
    h_nan.sum = f64::NAN;
    not_equals(&h1, &h_nan);
    equals(&h_nan, &h_nan);

    // Sum StaleNaN vs regular NaN.
    not_equals(&h_stale, &h_nan);

    // Has non-empty custom bounds for exponential schema.
    let mut h_custom = h1.copy();
    h_custom.custom_values = custom_values(&[1.0, 2.0, 3.0]);
    equals(&h1, &h_custom);

    let cbh1 = Histogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count: 10,
        sum: 2.7,
        positive_spans: vec![Span::new(0, 4), Span::new(10, 3)],
        positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
        custom_values: custom_values(&[
            0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 15.0, 20.0, 25.0, 50.0, 75.0, 100.0, 200.0, 250.0,
            500.0, 1000.0,
        ]),
        ..Default::default()
    };

    cbh1.validate().unwrap();

    let cbh2 = cbh1.copy();
    equals(&cbh1, &cbh2);

    // Has different custom bounds for custom buckets schema.
    let mut cbh2 = cbh1.copy();
    cbh2.custom_values = custom_values(&[0.1, 0.2, 0.5]);
    not_equals(&cbh1, &cbh2);

    // Has non-empty negative spans and buckets for custom buckets schema.
    let mut cbh2 = cbh1.copy();
    cbh2.negative_spans = vec![Span::new(0, 1)];
    cbh2.negative_buckets = vec![1];
    not_equals_until_float_conv(&cbh1, &cbh2);

    // Has non-zero zero count and threshold for custom buckets schema.
    let mut cbh2 = cbh1.copy();
    cbh2.zero_threshold = 0.1;
    cbh2.zero_count = 10;
    not_equals_until_float_conv(&cbh1, &cbh2);
}

#[derive(Default)]
struct CopyCase {
    name: &'static str,
    orig: Histogram,
    expected: Histogram,
}

#[test]
fn test_histogram_copy() {
    let cases: Vec<CopyCase> = vec![
        CopyCase {
            name: "without buckets",
            orig: Histogram {
                ..Default::default()
            },
            expected: Histogram {
                ..Default::default()
            },
        },
        CopyCase {
            name: "with buckets",
            orig: Histogram {
                positive_spans: vec![Span::new(-2, 1)],
                positive_buckets: vec![1, 3, -3, 42],
                negative_spans: vec![Span::new(3, 2)],
                negative_buckets: vec![5, 3, 123400, 1000],
                ..Default::default()
            },
            expected: Histogram {
                positive_spans: vec![Span::new(-2, 1)],
                positive_buckets: vec![1, 3, -3, 42],
                negative_spans: vec![Span::new(3, 2)],
                negative_buckets: vec![5, 3, 123400, 1000],
                ..Default::default()
            },
        },
        CopyCase {
            name: "with empty buckets and non empty capacity",
            orig: Histogram {
                positive_spans: Vec::<Span>::with_capacity(1),
                positive_buckets: Vec::<i64>::with_capacity(1),
                negative_spans: Vec::<Span>::with_capacity(1),
                negative_buckets: Vec::<i64>::with_capacity(1),
                ..Default::default()
            },
            expected: Histogram {
                ..Default::default()
            },
        },
        CopyCase {
            name: "with custom buckets",
            orig: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(-2, 1)],
                positive_buckets: vec![1, 3, -3, 42],
                custom_values: custom_values(&[5.0, 10.0, 15.0]),
                ..Default::default()
            },
            expected: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(-2, 1)],
                positive_buckets: vec![1, 3, -3, 42],
                custom_values: custom_values(&[5.0, 10.0, 15.0]),
                ..Default::default()
            },
        },
    ];
    for mut tcase in cases {
        let mut h_copy = tcase.orig.copy();

        // Modify a primitive value in the original histogram.
        tcase.orig.sum += 1.0;
        assert_eq!(tcase.expected, h_copy, "{}", tcase.name);
        assert_deep_copy_h_spans(
            tcase.name,
            &mut tcase.orig,
            &mut h_copy,
            &mut tcase.expected,
        );
    }
}

#[test]
fn test_histogram_copy_to() {
    let cases: Vec<CopyCase> = vec![
        CopyCase {
            name: "without buckets",
            orig: Histogram {
                ..Default::default()
            },
            expected: Histogram {
                ..Default::default()
            },
        },
        CopyCase {
            name: "with buckets",
            orig: Histogram {
                positive_spans: vec![Span::new(-2, 1)],
                positive_buckets: vec![1, 3, -3, 42],
                negative_spans: vec![Span::new(3, 2)],
                negative_buckets: vec![5, 3, 123400, 1000],
                ..Default::default()
            },
            expected: Histogram {
                positive_spans: vec![Span::new(-2, 1)],
                positive_buckets: vec![1, 3, -3, 42],
                negative_spans: vec![Span::new(3, 2)],
                negative_buckets: vec![5, 3, 123400, 1000],
                ..Default::default()
            },
        },
        CopyCase {
            name: "with empty buckets and non empty capacity",
            orig: Histogram {
                positive_spans: Vec::<Span>::with_capacity(1),
                positive_buckets: Vec::<i64>::with_capacity(1),
                negative_spans: Vec::<Span>::with_capacity(1),
                negative_buckets: Vec::<i64>::with_capacity(1),
                ..Default::default()
            },
            expected: Histogram {
                ..Default::default()
            },
        },
        CopyCase {
            name: "with custom buckets",
            orig: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(-2, 1)],
                positive_buckets: vec![1, 3, -3, 42],
                custom_values: custom_values(&[5.0, 10.0, 15.0]),
                ..Default::default()
            },
            expected: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(-2, 1)],
                positive_buckets: vec![1, 3, -3, 42],
                custom_values: custom_values(&[5.0, 10.0, 15.0]),
                ..Default::default()
            },
        },
    ];
    for mut tcase in cases {
        let mut h_copy = Histogram::default();
        tcase.orig.copy_to(&mut h_copy);
        // Modify a primitive value in the original histogram.
        tcase.orig.sum += 1.0;
        assert_eq!(tcase.expected, h_copy, "{}", tcase.name);
        assert_deep_copy_h_spans(
            tcase.name,
            &mut tcase.orig,
            &mut h_copy,
            &mut tcase.expected,
        );
    }
}

fn assert_deep_copy_h_spans(
    name: &str,
    orig: &mut Histogram,
    h_copy: &mut Histogram,
    expected: &mut Histogram,
) {
    // Do an in-place expansion of an original spans slice.
    expand_spans(&mut orig.positive_spans);
    *orig.positive_spans.last_mut().unwrap() = Span::new(1, 2);
    expand_spans(&mut h_copy.positive_spans);
    expand_spans(&mut expected.positive_spans);
    // Expand the copy spans and assert that modifying the original has not affected the copy.
    assert_eq!(expected, h_copy, "{}", name);
}

// adapted: Go reslices into spare capacity (which holds a zero Span here) or appends; Vec has no shared backing
// array, so both cases are a push of a zero Span. The check that the copy does not alias the original stays.
fn expand_spans(spans: &mut Vec<Span>) {
    spans.push(Span::default());
}

#[derive(Default)]
struct CompactCase {
    name: &'static str,
    input: Histogram,
    max_empty_buckets: usize,
    expected: Histogram,
}

// adapted: Go's require.Equal tells a nil slice from an empty one ([]Span{} in some expected values); a Vec has no
// nil state, so the expected values compare by content.
#[test]
fn test_histogram_compact() {
    let cases: Vec<CompactCase> = vec![
        CompactCase {
            name: "empty histogram",
            input: Histogram {
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                ..Default::default()
            },
        },
        CompactCase {
            name: "nothing should happen",
            input: Histogram {
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1, 3, -3, 42],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![5, 3, 123400, 1000],
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1, 3, -3, 42],
                negative_spans: vec![Span::new(3, 2), Span::new(3, 2)],
                negative_buckets: vec![5, 3, 123400, 1000],
                ..Default::default()
            },
        },
        CompactCase {
            name: "eliminate zero offsets",
            input: Histogram {
                positive_spans: vec![Span::new(-2, 1), Span::new(0, 3), Span::new(0, 1)],
                positive_buckets: vec![1, 3, -3, 42, 3],
                negative_spans: vec![
                    Span::new(0, 2),
                    Span::new(0, 2),
                    Span::new(2, 1),
                    Span::new(0, 1),
                ],
                negative_buckets: vec![5, 3, 123400, 1000, 3, 4],
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                positive_spans: vec![Span::new(-2, 5)],
                positive_buckets: vec![1, 3, -3, 42, 3],
                negative_spans: vec![Span::new(0, 4), Span::new(2, 2)],
                negative_buckets: vec![5, 3, 123400, 1000, 3, 4],
                ..Default::default()
            },
        },
        CompactCase {
            name: "eliminate zero length",
            input: Histogram {
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 0), Span::new(3, 3)],
                positive_buckets: vec![1, 3, -3, 42, 3],
                negative_spans: vec![
                    Span::new(0, 2),
                    Span::new(0, 0),
                    Span::new(2, 0),
                    Span::new(1, 4),
                ],
                negative_buckets: vec![5, 3, 123400, 1000, 3, 4],
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                positive_spans: vec![Span::new(-2, 2), Span::new(5, 3)],
                positive_buckets: vec![1, 3, -3, 42, 3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 4)],
                negative_buckets: vec![5, 3, 123400, 1000, 3, 4],
                ..Default::default()
            },
        },
        CompactCase {
            name: "eliminate multiple zero length spans",
            input: Histogram {
                positive_spans: vec![
                    Span::new(-2, 2),
                    Span::new(2, 0),
                    Span::new(2, 0),
                    Span::new(2, 0),
                    Span::new(3, 3),
                ],
                positive_buckets: vec![1, 3, -3, 42, 3],
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                positive_spans: vec![Span::new(-2, 2), Span::new(9, 3)],
                positive_buckets: vec![1, 3, -3, 42, 3],
                ..Default::default()
            },
        },
        CompactCase {
            name: "cut empty buckets at start or end",
            input: Histogram {
                positive_spans: vec![Span::new(-4, 4), Span::new(5, 3)],
                positive_buckets: vec![0, 0, 1, 3, -3, 42, 3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 5)],
                negative_buckets: vec![5, 3, -4, -2, 3, 4, -9],
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                positive_spans: vec![Span::new(-2, 2), Span::new(5, 3)],
                positive_buckets: vec![1, 3, -3, 42, 3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 4)],
                negative_buckets: vec![5, 3, -4, -2, 3, 4],
                ..Default::default()
            },
        },
        CompactCase {
            name: "cut empty buckets at start and end",
            input: Histogram {
                positive_spans: vec![Span::new(-4, 4), Span::new(5, 6)],
                positive_buckets: vec![0, 0, 1, 3, -3, 42, 3, -46, 0, 0],
                negative_spans: vec![Span::new(-2, 4), Span::new(3, 5)],
                negative_buckets: vec![0, 0, 5, 3, -4, -2, 3, 4, -9],
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                positive_spans: vec![Span::new(-2, 2), Span::new(5, 3)],
                positive_buckets: vec![1, 3, -3, 42, 3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 4)],
                negative_buckets: vec![5, 3, -4, -2, 3, 4],
                ..Default::default()
            },
        },
        CompactCase {
            name: "cut empty buckets at start or end of spans, even in the middle",
            input: Histogram {
                positive_spans: vec![Span::new(-4, 6), Span::new(3, 6)],
                positive_buckets: vec![0, 0, 1, 3, -4, 0, 1, 42, 3, -46, 0, 0],
                negative_spans: vec![Span::new(0, 2), Span::new(2, 6)],
                negative_buckets: vec![5, 3, -8, 4, -2, 3, 4, -9],
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                positive_spans: vec![Span::new(-2, 2), Span::new(5, 3)],
                positive_buckets: vec![1, 3, -3, 42, 3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 4)],
                negative_buckets: vec![5, 3, -4, -2, 3, 4],
                ..Default::default()
            },
        },
        CompactCase {
            name: "cut empty buckets at start or end but merge spans due to maxEmptyBuckets",
            input: Histogram {
                positive_spans: vec![Span::new(-4, 4), Span::new(5, 3)],
                positive_buckets: vec![0, 0, 1, 3, -3, 42, 3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 5)],
                negative_buckets: vec![5, 3, -4, -2, 3, 4, -9],
                ..Default::default()
            },
            max_empty_buckets: 10,
            expected: Histogram {
                positive_spans: vec![Span::new(-2, 10)],
                positive_buckets: vec![1, 3, -4, 0, 0, 0, 0, 1, 42, 3],
                negative_spans: vec![Span::new(0, 9)],
                negative_buckets: vec![5, 3, -8, 0, 0, 4, -2, 3, 4],
                ..Default::default()
            },
        },
        CompactCase {
            name: "cut empty buckets from the middle of a span",
            input: Histogram {
                positive_spans: vec![Span::new(-4, 6), Span::new(3, 3)],
                positive_buckets: vec![0, 0, 1, -1, 0, 3, -2, 42, 3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 5)],
                negative_buckets: vec![5, 3, -4, -2, -2, 3, 4],
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 1), Span::new(3, 3)],
                positive_buckets: vec![1, 2, -2, 42, 3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 2), Span::new(1, 2)],
                negative_buckets: vec![5, 3, -4, -2, 1, 4],
                ..Default::default()
            },
        },
        CompactCase {
            name: "cut out a span containing only empty buckets",
            input: Histogram {
                positive_spans: vec![Span::new(-4, 3), Span::new(2, 2), Span::new(3, 4)],
                positive_buckets: vec![0, 0, 1, -1, 0, 3, -2, 42, 3],
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                positive_spans: vec![Span::new(-2, 1), Span::new(7, 4)],
                positive_buckets: vec![1, 2, -2, 42, 3],
                ..Default::default()
            },
        },
        CompactCase {
            name: "cut empty buckets from the middle of a span, avoiding some due to maxEmptyBuckets",
            input: Histogram {
                positive_spans: vec![Span::new(-4, 6), Span::new(3, 3)],
                positive_buckets: vec![0, 0, 1, -1, 0, 3, -2, 42, 3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 5)],
                negative_buckets: vec![5, 3, -4, -2, -2, 3, 4],
                ..Default::default()
            },
            max_empty_buckets: 1,
            expected: Histogram {
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 1), Span::new(3, 3)],
                positive_buckets: vec![1, 2, -2, 42, 3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 5)],
                negative_buckets: vec![5, 3, -4, -2, -2, 3, 4],
                ..Default::default()
            },
        },
        CompactCase {
            name: "avoiding all cutting of empty buckets from the middle of a chunk due to maxEmptyBuckets",
            input: Histogram {
                positive_spans: vec![Span::new(-4, 6), Span::new(3, 3)],
                positive_buckets: vec![0, 0, 1, -1, 0, 3, -2, 42, 3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 5)],
                negative_buckets: vec![5, 3, -4, -2, -2, 3, 4],
                ..Default::default()
            },
            max_empty_buckets: 2,
            expected: Histogram {
                positive_spans: vec![Span::new(-2, 4), Span::new(3, 3)],
                positive_buckets: vec![1, -1, 0, 3, -2, 42, 3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 5)],
                negative_buckets: vec![5, 3, -4, -2, -2, 3, 4],
                ..Default::default()
            },
        },
        CompactCase {
            name: "everything merged into one span due to maxEmptyBuckets",
            input: Histogram {
                positive_spans: vec![Span::new(-4, 6), Span::new(3, 3)],
                positive_buckets: vec![0, 0, 1, -1, 0, 3, -2, 42, 3],
                negative_spans: vec![Span::new(0, 2), Span::new(3, 5)],
                negative_buckets: vec![5, 3, -4, -2, -2, 3, 4],
                ..Default::default()
            },
            max_empty_buckets: 3,
            expected: Histogram {
                positive_spans: vec![Span::new(-2, 10)],
                positive_buckets: vec![1, -1, 0, 3, -3, 0, 0, 1, 42, 3],
                negative_spans: vec![Span::new(0, 10)],
                negative_buckets: vec![5, 3, -8, 0, 0, 4, -2, -2, 3, 4],
                ..Default::default()
            },
        },
        CompactCase {
            name: "more than one span merged in the same pass",
            input: Histogram {
                count: 30,
                positive_spans: vec![Span::new(0, 2), Span::new(2, 2), Span::new(2, 2)],
                positive_buckets: vec![1, 1, 1, 1, 1, 1],
                negative_spans: vec![Span::new(0, 1), Span::new(1, 1), Span::new(1, 1)],
                negative_buckets: vec![2, 1, 1],
                ..Default::default()
            },
            max_empty_buckets: 3,
            expected: Histogram {
                count: 30,
                positive_spans: vec![Span::new(0, 10)],
                positive_buckets: vec![1, 1, -2, 0, 3, 1, -4, 0, 5, 1],
                negative_spans: vec![Span::new(0, 5)],
                negative_buckets: vec![2, -2, 3, -3, 4],
                ..Default::default()
            },
        },
        CompactCase {
            name: "only empty buckets and maxEmptyBuckets greater zero",
            input: Histogram {
                positive_spans: vec![Span::new(-4, 6), Span::new(3, 3)],
                positive_buckets: vec![0, 0, 0, 0, 0, 0, 0, 0, 0],
                negative_spans: vec![Span::new(0, 7)],
                negative_buckets: vec![0, 0, 0, 0, 0, 0, 0],
                ..Default::default()
            },
            max_empty_buckets: 3,
            expected: Histogram {
                positive_spans: vec![],
                positive_buckets: vec![],
                negative_spans: vec![],
                negative_buckets: vec![],
                ..Default::default()
            },
        },
        CompactCase {
            name: "multiple spans of only empty buckets",
            input: Histogram {
                positive_spans: vec![Span::new(-10, 2), Span::new(2, 1), Span::new(3, 3)],
                positive_buckets: vec![0, 0, 0, 0, 2, 3],
                negative_spans: vec![Span::new(-10, 2), Span::new(2, 1), Span::new(3, 3)],
                negative_buckets: vec![2, 3, -5, 0, 0, 0],
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                positive_spans: vec![Span::new(-1, 2)],
                positive_buckets: vec![2, 3],
                negative_spans: vec![Span::new(-10, 2)],
                negative_buckets: vec![2, 3],
                ..Default::default()
            },
        },
        CompactCase {
            name: "nothing should happen with custom buckets",
            input: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1, 3, -3, 42],
                custom_values: custom_values(&[5.0, 10.0, 15.0]),
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(-2, 1), Span::new(2, 3)],
                positive_buckets: vec![1, 3, -3, 42],
                custom_values: custom_values(&[5.0, 10.0, 15.0]),
                ..Default::default()
            },
        },
        CompactCase {
            name: "eliminate zero offsets with custom buckets",
            input: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(-2, 1), Span::new(0, 3), Span::new(0, 1)],
                positive_buckets: vec![1, 3, -3, 42, 3],
                custom_values: custom_values(&[5.0, 10.0, 15.0, 20.0]),
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(-2, 5)],
                positive_buckets: vec![1, 3, -3, 42, 3],
                custom_values: custom_values(&[5.0, 10.0, 15.0, 20.0]),
                ..Default::default()
            },
        },
        CompactCase {
            name: "eliminate zero length with custom buckets",
            input: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(-2, 2), Span::new(2, 0), Span::new(3, 3)],
                positive_buckets: vec![1, 3, -3, 42, 3],
                custom_values: custom_values(&[5.0, 10.0, 15.0, 20.0]),
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(-2, 2), Span::new(5, 3)],
                positive_buckets: vec![1, 3, -3, 42, 3],
                custom_values: custom_values(&[5.0, 10.0, 15.0, 20.0]),
                ..Default::default()
            },
        },
        CompactCase {
            name: "all zero-length spans with non-empty buckets",
            input: Histogram {
                positive_spans: vec![Span::new(0, 0), Span::new(2, 0)],
                positive_buckets: vec![1, 3],
                negative_spans: vec![Span::new(1, 0)],
                negative_buckets: vec![2],
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                positive_spans: vec![],
                positive_buckets: vec![],
                negative_spans: vec![],
                negative_buckets: vec![],
                ..Default::default()
            },
        },
        CompactCase {
            name: "more buckets than spans account for",
            input: Histogram {
                positive_spans: vec![Span::new(0, 1), Span::new(2, 1)],
                positive_buckets: vec![1, 2, 3, 4],
                negative_spans: vec![Span::new(0, 1)],
                negative_buckets: vec![5, 6],
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                positive_spans: vec![Span::new(0, 1), Span::new(2, 1)],
                positive_buckets: vec![1, 2, 3, 4],
                negative_spans: vec![Span::new(0, 1)],
                negative_buckets: vec![5, 6],
                ..Default::default()
            },
        },
        CompactCase {
            name: "eliminate multiple zero length spans with custom buckets",
            input: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![
                    Span::new(-2, 2),
                    Span::new(2, 0),
                    Span::new(2, 0),
                    Span::new(2, 0),
                    Span::new(3, 3),
                ],
                positive_buckets: vec![1, 3, -3, 42, 3],
                custom_values: custom_values(&[5.0, 10.0, 15.0, 20.0]),
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(-2, 2), Span::new(9, 3)],
                positive_buckets: vec![1, 3, -3, 42, 3],
                custom_values: custom_values(&[5.0, 10.0, 15.0, 20.0]),
                ..Default::default()
            },
        },
        CompactCase {
            name: "cut empty buckets at start or end of spans, even in the middle, with custom buckets",
            input: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(-4, 6), Span::new(3, 6)],
                positive_buckets: vec![0, 0, 1, 3, -4, 0, 1, 42, 3, -46, 0, 0],
                custom_values: custom_values(&[5.0, 10.0, 15.0, 20.0]),
                ..Default::default()
            },
            max_empty_buckets: 0,
            expected: Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                positive_spans: vec![Span::new(-2, 2), Span::new(5, 3)],
                positive_buckets: vec![1, 3, -3, 42, 3],
                custom_values: custom_values(&[5.0, 10.0, 15.0, 20.0]),
                ..Default::default()
            },
        },
    ];
    for mut c in cases {
        let was_valid = c.input.validate().is_ok();
        assert_eq!(
            &c.expected,
            c.input.compact(c.max_empty_buckets),
            "{}",
            c.name
        );
        // Compact has happened in-place, too.
        assert_eq!(c.expected, c.input, "{}", c.name);
        if was_valid {
            if let Err(e) = c.input.validate() {
                panic!(
                    "{}: Compact turned a valid histogram into an invalid one: {}",
                    c.name, e
                );
            }
        }
    }
}

#[derive(Default)]
struct ValidationCase {
    h: Histogram,
    err_msg: &'static str,
    skip_float: bool,
}

#[test]
fn test_histogram_validation() {
    let tests: Vec<(&str, ValidationCase)> = vec![
        (
            "valid histogram",
            ValidationCase {
                h: Histogram {
                    count: 12,
                    zero_count: 2,
                    zero_threshold: 0.001,
                    sum: 19.4,
                    schema: 1,
                    positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    positive_buckets: vec![1, 1, -1, 0],
                    negative_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    negative_buckets: vec![1, 1, -1, 0],
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "valid histogram with NaN observations that has its Count (4) higher than the actual total of buckets (2 + 1)",
            ValidationCase {
                h: Histogram {
                    zero_count: 2,
                    count: 4,
                    sum: f64::NAN,
                    positive_spans: vec![Span::new(0, 1)],
                    positive_buckets: vec![1],
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "rejects histogram without NaN observations that has its Count (4) higher than the actual total of buckets (2 + 1)",
            ValidationCase {
                h: Histogram {
                    zero_count: 2,
                    count: 4,
                    sum: 333.0,
                    positive_spans: vec![Span::new(0, 1)],
                    positive_buckets: vec![1],
                    ..Default::default()
                },
                err_msg: r#"3 observations found in buckets, but the Count field is 4: histogram's observation count should equal the number of observations found in the buckets (in absence of NaN)"#,
                skip_float: true,
            },
        ),
        (
            "rejects histogram that has too few negative buckets",
            ValidationCase {
                h: Histogram {
                    negative_spans: vec![Span::new(0, 1)],
                    negative_buckets: vec![],
                    ..Default::default()
                },
                err_msg: r#"negative side: spans need 1 buckets, have 0 buckets: histogram spans specify different number of buckets than provided"#,
                ..Default::default()
            },
        ),
        (
            "rejects histogram that has too few positive buckets",
            ValidationCase {
                h: Histogram {
                    positive_spans: vec![Span::new(0, 1)],
                    positive_buckets: vec![],
                    ..Default::default()
                },
                err_msg: r#"positive side: spans need 1 buckets, have 0 buckets: histogram spans specify different number of buckets than provided"#,
                ..Default::default()
            },
        ),
        (
            "rejects histogram that has too many negative buckets",
            ValidationCase {
                h: Histogram {
                    negative_spans: vec![Span::new(0, 1)],
                    negative_buckets: vec![1, 2],
                    ..Default::default()
                },
                err_msg: r#"negative side: spans need 1 buckets, have 2 buckets: histogram spans specify different number of buckets than provided"#,
                ..Default::default()
            },
        ),
        (
            "rejects histogram that has too many positive buckets",
            ValidationCase {
                h: Histogram {
                    positive_spans: vec![Span::new(0, 1)],
                    positive_buckets: vec![1, 2],
                    ..Default::default()
                },
                err_msg: r#"positive side: spans need 1 buckets, have 2 buckets: histogram spans specify different number of buckets than provided"#,
                ..Default::default()
            },
        ),
        (
            "rejects a histogram that has a negative span with a negative offset",
            ValidationCase {
                h: Histogram {
                    negative_spans: vec![Span::new(-1, 1), Span::new(-1, 1)],
                    negative_buckets: vec![1, 2],
                    ..Default::default()
                },
                err_msg: r#"negative side: span number 2 with offset -1: histogram has a span whose offset is negative"#,
                ..Default::default()
            },
        ),
        (
            "rejects a histogram which has a positive span with a negative offset",
            ValidationCase {
                h: Histogram {
                    positive_spans: vec![Span::new(-1, 1), Span::new(-1, 1)],
                    positive_buckets: vec![1, 2],
                    ..Default::default()
                },
                err_msg: r#"positive side: span number 2 with offset -1: histogram has a span whose offset is negative"#,
                ..Default::default()
            },
        ),
        (
            "rejects a histogram that has negative spans whose bucket index overflows int32",
            ValidationCase {
                h: Histogram {
                    negative_spans: vec![Span::new(0, 1), Span::new(i32::MAX, 1)],
                    negative_buckets: vec![1, 0],
                    ..Default::default()
                },
                err_msg: r#"negative side: span number 2 ends at bucket index 2147483648: histogram has a span whose bucket index is out of range"#,
                ..Default::default()
            },
        ),
        (
            "rejects a histogram that has positive spans whose bucket index overflows int32",
            ValidationCase {
                h: Histogram {
                    positive_spans: vec![
                        Span::new(0, 1),
                        Span::new(i32::MAX, 1),
                        Span::new(i32::MAX, 1),
                    ],
                    positive_buckets: vec![1, 0, 0],
                    ..Default::default()
                },
                err_msg: r#"positive side: span number 2 ends at bucket index 2147483648: histogram has a span whose bucket index is out of range"#,
                ..Default::default()
            },
        ),
        (
            "valid histogram with a bucket at the largest int32 index",
            ValidationCase {
                h: Histogram {
                    count: 1,
                    positive_spans: vec![Span::new(i32::MAX, 1)],
                    positive_buckets: vec![1],
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "rejects a histogram whose positive span offsets each fit in int32 but add up past it",
            ValidationCase {
                h: Histogram {
                    positive_spans: vec![
                        Span::new(i32::MAX / 2, 1),
                        Span::new(i32::MAX / 2, 1),
                        Span::new(1, 1),
                    ],
                    positive_buckets: vec![1, 0, 0],
                    ..Default::default()
                },
                err_msg: r#"positive side: span number 3 ends at bucket index 2147483649: histogram has a span whose bucket index is out of range"#,
                ..Default::default()
            },
        ),
        (
            "rejects a histogram that has a negative span starting at the smallest int32 index",
            ValidationCase {
                h: Histogram {
                    negative_spans: vec![Span::new(i32::MIN, 1)],
                    negative_buckets: vec![1],
                    ..Default::default()
                },
                err_msg: r#"negative side: span number 1 starts at bucket index -2147483648: histogram has a span whose bucket index is out of range"#,
                ..Default::default()
            },
        ),
        (
            "rejects a histogram that has a positive span starting at the smallest int32 index",
            ValidationCase {
                h: Histogram {
                    positive_spans: vec![Span::new(i32::MIN, 1)],
                    positive_buckets: vec![1],
                    ..Default::default()
                },
                err_msg: r#"positive side: span number 1 starts at bucket index -2147483648: histogram has a span whose bucket index is out of range"#,
                ..Default::default()
            },
        ),
        (
            "valid histogram with a bucket at the smallest allowed index",
            ValidationCase {
                h: Histogram {
                    count: 1,
                    positive_spans: vec![Span::new(i32::MIN + 1, 1)],
                    positive_buckets: vec![1],
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "rejects a histogram that has a negative bucket with a negative count",
            ValidationCase {
                h: Histogram {
                    negative_spans: vec![Span::new(-1, 1)],
                    negative_buckets: vec![-1],
                    ..Default::default()
                },
                err_msg: r#"negative side: bucket number 1 has observation count of -1: histogram has a bucket whose observation count is negative"#,
                ..Default::default()
            },
        ),
        (
            "rejects a histogram that has a positive bucket with a negative count",
            ValidationCase {
                h: Histogram {
                    positive_spans: vec![Span::new(-1, 1)],
                    positive_buckets: vec![-1],
                    ..Default::default()
                },
                err_msg: r#"positive side: bucket number 1 has observation count of -1: histogram has a bucket whose observation count is negative"#,
                ..Default::default()
            },
        ),
        (
            "rejects a histogram that has a lower count than count in buckets",
            ValidationCase {
                h: Histogram {
                    count: 0,
                    negative_spans: vec![Span::new(-1, 1)],
                    positive_spans: vec![Span::new(-1, 1)],
                    negative_buckets: vec![1],
                    positive_buckets: vec![1],
                    ..Default::default()
                },
                err_msg: r#"2 observations found in buckets, but the Count field is 0: histogram's observation count should equal the number of observations found in the buckets (in absence of NaN)"#,
                skip_float: true,
            },
        ),
        (
            "rejects a histogram that doesn't count the zero bucket in its count",
            ValidationCase {
                h: Histogram {
                    count: 2,
                    zero_count: 1,
                    negative_spans: vec![Span::new(-1, 1)],
                    positive_spans: vec![Span::new(-1, 1)],
                    negative_buckets: vec![1],
                    positive_buckets: vec![1],
                    ..Default::default()
                },
                err_msg: r#"3 observations found in buckets, but the Count field is 2: histogram's observation count should equal the number of observations found in the buckets (in absence of NaN)"#,
                skip_float: true,
            },
        ),
        (
            "rejects an exponential histogram with custom buckets schema",
            ValidationCase {
                h: Histogram {
                    count: 12,
                    zero_count: 2,
                    zero_threshold: 0.001,
                    sum: 19.4,
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    positive_buckets: vec![1, 1, -1, 0],
                    negative_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    negative_buckets: vec![1, 1, -1, 0],
                    ..Default::default()
                },
                err_msg: r#"custom buckets: only 0 custom bounds defined which is insufficient to cover total span length of 5: histogram custom bounds are too few"#,
                ..Default::default()
            },
        ),
        (
            "rejects a custom buckets histogram with exponential schema",
            ValidationCase {
                h: Histogram {
                    count: 5,
                    sum: 19.4,
                    schema: 0,
                    positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    positive_buckets: vec![1, 1, -1, 0],
                    custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
                    ..Default::default()
                },
                err_msg: r#"histogram with exponential schema must not have custom bounds"#,
                skip_float: true,
            },
        ),
        (
            "rejects a custom buckets histogram with zero/negative buckets",
            ValidationCase {
                h: Histogram {
                    count: 12,
                    zero_count: 2,
                    zero_threshold: 0.001,
                    sum: 19.4,
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    positive_buckets: vec![1, 1, -1, 0],
                    negative_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    negative_buckets: vec![1, 1, -1, 0],
                    custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
                    ..Default::default()
                },
                err_msg: r#"custom buckets: must have zero count of 0"#,
                skip_float: true,
            },
        ),
        (
            "rejects a custom buckets histogram with negative offset in first span",
            ValidationCase {
                h: Histogram {
                    count: 5,
                    sum: 19.4,
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    positive_spans: vec![Span::new(-1, 2), Span::new(1, 2)],
                    positive_buckets: vec![1, 1, -1, 0],
                    custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
                    ..Default::default()
                },
                err_msg: r#"custom buckets: span number 1 with offset -1: histogram has a span whose offset is negative"#,
                ..Default::default()
            },
        ),
        (
            "rejects a custom buckets histogram with negative offset in subsequent spans",
            ValidationCase {
                h: Histogram {
                    count: 5,
                    sum: 19.4,
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    positive_spans: vec![Span::new(0, 2), Span::new(-1, 2)],
                    positive_buckets: vec![1, 1, -1, 0],
                    custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
                    ..Default::default()
                },
                err_msg: r#"custom buckets: span number 2 with offset -1: histogram has a span whose offset is negative"#,
                ..Default::default()
            },
        ),
        (
            "rejects a custom buckets histogram with non-matching bucket counts",
            ValidationCase {
                h: Histogram {
                    count: 5,
                    sum: 19.4,
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    positive_buckets: vec![1, 1, -1],
                    custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
                    ..Default::default()
                },
                err_msg: r#"custom buckets: spans need 4 buckets, have 3 buckets: histogram spans specify different number of buckets than provided"#,
                ..Default::default()
            },
        ),
        (
            "rejects a custom buckets histogram with too few bounds",
            ValidationCase {
                h: Histogram {
                    count: 5,
                    sum: 19.4,
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    positive_buckets: vec![1, 1, -1, 0],
                    custom_values: custom_values(&[1.0, 2.0, 3.0]),
                    ..Default::default()
                },
                err_msg: r#"custom buckets: only 3 custom bounds defined which is insufficient to cover total span length of 5: histogram custom bounds are too few"#,
                ..Default::default()
            },
        ),
        (
            "valid custom buckets histogram",
            ValidationCase {
                h: Histogram {
                    count: 5,
                    sum: 19.4,
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    positive_buckets: vec![1, 1, -1, 0],
                    custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0]),
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "valid custom buckets histogram with extra bounds",
            ValidationCase {
                h: Histogram {
                    count: 5,
                    sum: 19.4,
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
                    positive_buckets: vec![1, 1, -1, 0],
                    custom_values: custom_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]),
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "reject custom buckets histogram with non-increasing bound",
            ValidationCase {
                h: Histogram {
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    custom_values: custom_values(&[0.0, 0.0]),
                    ..Default::default()
                },
                err_msg: "custom buckets: previous bound is 0.000000 and current is 0.000000: histogram custom bounds must be in strictly increasing order",
                ..Default::default()
            },
        ),
        (
            "reject custom buckets histogram with explicit +Inf bound",
            ValidationCase {
                h: Histogram {
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    custom_values: custom_values(&[1.0, f64::INFINITY]),
                    ..Default::default()
                },
                err_msg: "custom buckets: last +Inf bound must not be explicitly defined: histogram custom bounds must be finite",
                ..Default::default()
            },
        ),
        (
            "valid custom buckets histogram with explicit -Inf bound",
            ValidationCase {
                h: Histogram {
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    custom_values: custom_values(&[f64::NEG_INFINITY, 1.0]),
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "reject custom buckets histogram with NaN bound",
            ValidationCase {
                h: Histogram {
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    custom_values: custom_values(&[1.0, f64::NAN, 3.0]),
                    ..Default::default()
                },
                err_msg: "custom buckets: histogram custom bounds must not be NaN",
                ..Default::default()
            },
        ),
        (
            "schema too high",
            ValidationCase {
                h: Histogram {
                    schema: 10,
                    ..Default::default()
                },
                err_msg: r#"histogram has an invalid schema, which must be between -4 and 8 for exponential buckets, or -53 for custom buckets, got schema 10"#,
                ..Default::default()
            },
        ),
        (
            "schema too low",
            ValidationCase {
                h: Histogram {
                    schema: -10,
                    ..Default::default()
                },
                err_msg: r#"histogram has an invalid schema, which must be between -4 and 8 for exponential buckets, or -53 for custom buckets, got schema -10"#,
                ..Default::default()
            },
        ),
    ];
    for (test_name, tc) in tests {
        match tc.h.validate() {
            Err(err) if !tc.err_msg.is_empty() => {
                assert!(
                    err.is_histogram_error(),
                    "{}: not a histogram.Error: {}",
                    test_name,
                    err
                );
                assert_eq!(tc.err_msg, err.to_string(), "{}", test_name);
            }
            Ok(()) if !tc.err_msg.is_empty() => {
                panic!("{}: expected error {:?}", test_name, tc.err_msg)
            }
            r => assert!(r.is_ok(), "{}: unexpected error {:?}", test_name, r),
        }
        if tc.skip_float {
            continue;
        }

        let fh = tc.h.to_float();
        match fh.validate() {
            Err(err) if !tc.err_msg.is_empty() => {
                assert!(
                    err.is_histogram_error(),
                    "{} (float): not a histogram.Error: {}",
                    test_name,
                    err
                );
                assert_eq!(tc.err_msg, err.to_string(), "{} (float)", test_name);
            }
            Ok(()) if !tc.err_msg.is_empty() => {
                panic!("{} (float): expected error {:?}", test_name, tc.err_msg)
            }
            r => assert!(r.is_ok(), "{} (float): unexpected error {:?}", test_name, r),
        }
    }
}

#[derive(Default)]
struct ReduceCase {
    origin: Histogram,
    target_schema: i32,
    target: Histogram,
    error_msg: &'static str,
}

#[test]
fn test_histogram_reduce_resolution() {
    let tcs: Vec<(&str, ReduceCase)> = vec![
        (
            "valid histogram",
            ReduceCase {
                origin: Histogram {
                    schema: 0,
                    positive_spans: vec![Span::new(-2, 4), Span::new(0, 0), Span::new(3, 2)],
                    positive_buckets: vec![1, 2, -2, 1, -1, 0],
                    negative_spans: vec![Span::new(0, 4), Span::new(0, 0), Span::new(3, 2)],
                    negative_buckets: vec![1, 2, -2, 1, -1, 0],
                    ..Default::default()
                },
                target_schema: -1,
                target: Histogram {
                    schema: -1,
                    positive_spans: vec![Span::new(-1, 3), Span::new(1, 1)],
                    positive_buckets: vec![1, 3, -2, 0],
                    negative_spans: vec![Span::new(0, 3), Span::new(1, 1)],
                    negative_buckets: vec![1, 3, -2, 0],
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "not enough buckets",
            ReduceCase {
                origin: Histogram {
                    schema: 0,
                    positive_spans: vec![Span::new(-2, 4), Span::new(0, 0), Span::new(3, 2)],
                    positive_buckets: vec![1, 2, -2, 1, -1],
                    ..Default::default()
                },
                target_schema: -1,
                error_msg: "have 5 buckets but spans need more: histogram spans specify different number of buckets than provided",
                ..Default::default()
            },
        ),
        (
            "too many buckets",
            ReduceCase {
                origin: Histogram {
                    schema: 0,
                    positive_spans: vec![Span::new(-2, 4), Span::new(0, 0), Span::new(3, 2)],
                    positive_buckets: vec![1, 2, -2, 1, -1, 0, 3],
                    ..Default::default()
                },
                target_schema: -1,
                error_msg: "spans need 6 buckets, have 7 buckets: histogram spans specify different number of buckets than provided",
                ..Default::default()
            },
        ),
        (
            "negative offset",
            ReduceCase {
                origin: Histogram {
                    schema: 0,
                    positive_spans: vec![Span::new(-2, 4), Span::new(-1, 0), Span::new(3, 2)],
                    positive_buckets: vec![1, 2, -2, 1, -1, 0],
                    ..Default::default()
                },
                target_schema: -1,
                error_msg: "span number 2 with offset -1: histogram has a span whose offset is negative",
                ..Default::default()
            },
        ),
    ];
    for (tn, mut tc) in tcs {
        let res = tc.origin.reduce_resolution(tc.target_schema);
        if !tc.error_msg.is_empty() {
            let err = res.expect_err(tn);
            assert_eq!(tc.error_msg, err.to_string(), "{}", tn);
            // The returned error should be a histogram.Error.
            assert!(err.is_histogram_error(), "{}", tn);
            continue;
        }
        res.unwrap_or_else(|e| panic!("{}: {}", tn, e));
        assert_eq!(tc.target, tc.origin, "{}", tn);
    }
}
