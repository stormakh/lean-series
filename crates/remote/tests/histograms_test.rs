//! Port of `storage/remote/otlptranslator/prometheusremotewrite/histograms_test.go`.

mod otlp_testutil;

use model::exemplar::Exemplar;
use model::histogram::{Histogram, Span, custom_values};
use model::labels::{Labels, METRIC_NAME};
use otlp_testutil::*;
use remote::otlptranslator::namer::MetricNamer;
use remote::otlptranslator::pdata::{
    AggregationTemporality, AppendEmpty, ExponentialHistogramDataPoint, HistogramDataPoint, Metric, Resource,
    new_timestamp_from_time,
};
use remote::otlptranslator::prometheusremotewrite::{
    AppOpts, DEFAULT_ZERO_THRESHOLD, PrometheusConverter, Scope, Settings, convert_buckets_layout,
    explicit_histogram_to_custom_buckets_histogram, exponential_to_native_histogram, get_bucket_offset,
    translator_metric_from_otel_metric,
};
use storage::{AppendableV2, Context};
use util::teststorage::{Appendable, Sample, require_equal};

fn span(offset: i32, length: u32) -> Span {
    Span { offset, length }
}

struct LayoutCase {
    name: &'static str,
    offset: i32,
    counts: Vec<u64>,
    /// scale down -> (spans, deltas)
    want: Vec<(i32, Vec<Span>, Vec<i64>)>,
}

#[test]
fn test_convert_buckets_layout() {
    let tests = vec![
        LayoutCase {
            name: "zero offset",
            offset: 0,
            counts: vec![4, 3, 2, 1],
            want: vec![
                (0, vec![span(1, 4)], vec![4, -1, -1, -1]),
                // 4+3, 2+1 = 7, 3 =delta= 7, -4
                (1, vec![span(1, 2)], vec![7, -4]),
                // 4+3+2+1 = 10 =delta= 10
                (2, vec![span(1, 1)], vec![10]),
            ],
        },
        LayoutCase {
            name: "offset 1",
            offset: 1,
            counts: vec![4, 3, 2, 1],
            want: vec![
                (0, vec![span(2, 4)], vec![4, -1, -1, -1]),
                (1, vec![span(1, 3)], vec![4, 1, -4]), // 0+4, 3+2, 1+0 = 4, 5, 1
                (2, vec![span(1, 2)], vec![9, -8]),    // 0+4+3+2, 1+0+0+0 = 9, 1
            ],
        },
        LayoutCase {
            name: "positive offset",
            offset: 4,
            counts: vec![4, 2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            want: vec![
                (0, vec![span(5, 4), span(12, 1)], vec![4, -2, -2, 2, -1]),
                (1, vec![span(3, 2), span(6, 1)], vec![6, -4, -1]),
                (2, vec![span(2, 1), span(3, 1)], vec![8, -7]),
            ],
        },
        LayoutCase {
            name: "scaledown merges spans",
            offset: 4,
            counts: vec![4, 2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            want: vec![
                (0, vec![span(5, 4), span(8, 1)], vec![4, -2, -2, 2, -1]),
                (1, vec![span(3, 2), span(4, 1)], vec![6, -4, -1]),
                (2, vec![span(2, 4)], vec![8, -8, 0, 1]),
            ],
        },
        LayoutCase {
            name: "negative offset",
            offset: -2,
            counts: vec![3, 1, 0, 0, 0, 1],
            want: vec![
                (0, vec![span(-1, 2), span(3, 1)], vec![3, -2, 0]),
                (1, vec![span(0, 3)], vec![4, -4, 1]),
                (2, vec![span(0, 2)], vec![4, -3]),
            ],
        },
        LayoutCase {
            name: "buckets with gaps of size 1",
            offset: -2,
            counts: vec![3, 1, 0, 1, 0, 1],
            want: vec![
                (0, vec![span(-1, 6)], vec![3, -2, -1, 1, -1, 1]),
                (1, vec![span(0, 3)], vec![4, -3, 0]),
                (2, vec![span(0, 2)], vec![4, -2]),
            ],
        },
        LayoutCase {
            name: "buckets with gaps of size 2",
            offset: -2,
            counts: vec![3, 0, 0, 1, 0, 0, 1],
            want: vec![
                (0, vec![span(-1, 7)], vec![3, -3, 0, 1, -1, 0, 1]),
                (1, vec![span(0, 4)], vec![3, -2, -1, 1]),
                (2, vec![span(0, 3)], vec![3, -2, 0]),
            ],
        },
        LayoutCase {
            name: "empty bucket before merged buckets",
            offset: 0,
            counts: vec![0, 0, 5, 3],
            want: vec![
                (0, vec![span(1, 4)], vec![0, 0, 5, -2]),
                (1, vec![span(1, 2)], vec![0, 8]),
                (2, vec![span(1, 1)], vec![8]),
            ],
        },
        LayoutCase {
            name: "empty bucket between merged buckets",
            offset: 0,
            counts: vec![1, 0, 0, 0, 5, 3],
            want: vec![
                (0, vec![span(1, 1), span(3, 2)], vec![1, 4, -2]),
                (1, vec![span(1, 3)], vec![1, -1, 8]),
                (2, vec![span(1, 2)], vec![1, 7]),
            ],
        },
        LayoutCase {
            name: "zero buckets",
            offset: 0,
            counts: vec![],
            want: vec![(0, vec![], vec![]), (1, vec![], vec![]), (2, vec![], vec![])],
        },
    ];
    for tt in tests {
        for (scale_down, want_spans, want_deltas) in tt.want {
            let (got_spans, got_deltas) = convert_buckets_layout(&tt.counts, tt.offset, scale_down, true);
            assert_eq!(want_spans, got_spans, "{}-scaleby-{}", tt.name, scale_down);
            assert_eq!(want_deltas, got_deltas, "{}-scaleby-{}", tt.name, scale_down);
        }
    }
}

fn validate_histogram_count(h: &HistogramDataPoint) {
    let actual: u64 = h.bucket_counts.iter().sum();
    assert_eq!(h.count, actual, "histogram count mismatch");
}

fn validate_exponential_histogram_count(h: &ExponentialHistogramDataPoint) {
    let actual: u64 = h.positive.bucket_counts.iter().sum::<u64>() + h.negative.bucket_counts.iter().sum::<u64>();
    assert_eq!(h.count, actual, "exponential histogram count mismatch");
}

fn validate_native_histogram_count(h: &Histogram) {
    let mut actual = 0u64;
    let mut prev = 0i64;
    for d in &h.positive_buckets {
        prev += d;
        actual += prev as u64;
    }
    prev = 0;
    for d in &h.negative_buckets {
        prev += d;
        actual += prev as u64;
    }
    assert_eq!(h.count, actual, "native histogram count mismatch");
}

fn ms(t: u64) -> u64 {
    new_timestamp_from_time(std::time::UNIX_EPOCH + std::time::Duration::from_millis(t))
}

#[test]
fn test_exponential_to_native_histogram() {
    let base = || {
        let mut pt = ExponentialHistogramDataPoint::new();
        pt.start_timestamp = ms(100);
        pt.timestamp = ms(500);
        pt.count = 4;
        pt.scale = 1;
        pt.zero_count = 1;
        pt.positive.bucket_counts = vec![1, 1];
        pt.positive.offset = 1;
        pt.negative.bucket_counts = vec![1, 1];
        pt.negative.offset = 1;
        pt
    };
    let scaled = |scale: i32, count: u64, pos: Vec<u64>, pos_off: i32, neg: Vec<u64>, neg_off: i32| {
        let mut pt = ExponentialHistogramDataPoint::new();
        pt.timestamp = ms(500);
        pt.count = count;
        pt.set_sum(10.1);
        pt.scale = scale;
        pt.zero_count = 1;
        pt.positive.bucket_counts = pos;
        pt.positive.offset = pos_off;
        pt.negative.bucket_counts = neg;
        pt.negative.offset = neg_off;
        pt
    };
    let tests: Vec<(&str, ExponentialHistogramDataPoint, Option<Histogram>, &str)> = vec![
        (
            "convert exp. to native histogram",
            {
                let mut pt = base();
                pt.set_sum(10.1);
                pt
            },
            Some(Histogram {
                count: 4,
                sum: 10.1,
                schema: 1,
                zero_threshold: DEFAULT_ZERO_THRESHOLD,
                zero_count: 1,
                negative_spans: vec![span(2, 2)],
                negative_buckets: vec![1, 0],
                positive_spans: vec![span(2, 2)],
                positive_buckets: vec![1, 0],
                ..Default::default()
            }),
            "",
        ),
        (
            "convert exp. to native histogram with no sum",
            base(),
            Some(Histogram {
                count: 4,
                schema: 1,
                zero_threshold: DEFAULT_ZERO_THRESHOLD,
                zero_count: 1,
                negative_spans: vec![span(2, 2)],
                negative_buckets: vec![1, 0],
                positive_spans: vec![span(2, 2)],
                positive_buckets: vec![1, 0],
                ..Default::default()
            }),
            "",
        ),
        (
            "invalid negative scale",
            {
                let mut pt = ExponentialHistogramDataPoint::new();
                pt.scale = -10;
                pt
            },
            None,
            "cannot convert exponential to native histogram. Scale must be >= -4, was -10",
        ),
        (
            "no downscaling at scale 8",
            scaled(8, 6, vec![1, 1, 1], 1, vec![1, 1, 1], 2),
            Some(Histogram {
                count: 6,
                sum: 10.1,
                schema: 8,
                zero_threshold: DEFAULT_ZERO_THRESHOLD,
                zero_count: 1,
                positive_spans: vec![span(2, 3)],
                positive_buckets: vec![1, 0, 0], // 1, 1, 1
                negative_spans: vec![span(3, 3)],
                negative_buckets: vec![1, 0, 0], // 1, 1, 1
                ..Default::default()
            }),
            "",
        ),
        (
            "downsample if scale is more than 8",
            scaled(9, 6, vec![1, 1, 1], 1, vec![1, 1, 1], 2),
            Some(Histogram {
                count: 6,
                sum: 10.1,
                schema: 8,
                zero_threshold: DEFAULT_ZERO_THRESHOLD,
                zero_count: 1,
                positive_spans: vec![span(1, 2)],
                positive_buckets: vec![1, 1], // 0+1, 1+1 = 1, 2
                negative_spans: vec![span(2, 2)],
                negative_buckets: vec![2, -1], // 1+1, 1+0 = 2, 1
                ..Default::default()
            }),
            "",
        ),
        (
            "downsample with empty buckets before merged buckets",
            scaled(9, 17, vec![0, 0, 5, 3], 0, vec![1, 0, 0, 0, 5, 3], 0),
            Some(Histogram {
                count: 17,
                sum: 10.1,
                schema: 8,
                zero_threshold: DEFAULT_ZERO_THRESHOLD,
                zero_count: 1,
                positive_spans: vec![span(1, 2)],
                positive_buckets: vec![0, 8], // 0+0, 5+3 = 0, 8
                negative_spans: vec![span(1, 3)],
                negative_buckets: vec![1, -1, 8], // 1+0, 0+0, 5+3 = 1, 0, 8
                ..Default::default()
            }),
            "",
        ),
    ];
    for (name, pt, want, want_err) in tests {
        validate_exponential_histogram_count(&pt); // Sanity check.
        let res = exponential_to_native_histogram(&pt, AggregationTemporality::CUMULATIVE);
        if !want_err.is_empty() {
            let e = res.err().unwrap_or_else(|| panic!("{name}: expected an error"));
            assert!(e.to_string().contains(want_err), "{name}: {e}");
            continue;
        }
        let (got, annots) = res.unwrap();
        assert!(annots.is_empty(), "{name}");
        assert_eq!(want.unwrap(), got, "{name}");
        validate_native_histogram_count(&got);
    }
}

fn exp_metric(points: Vec<(u64, Vec<u64>, bool, f64, &'static str)>) -> Metric {
    // (count, buckets, negative, exemplar value, attr)
    let mut metric = Metric::new();
    metric.name = "test_hist".into();
    metric.set_empty_exponential_histogram().aggregation_temporality = AggregationTemporality::CUMULATIVE;
    for (count, buckets, negative, ex, attr) in points {
        let pt = metric.exponential_histogram_mut().data_points.append_empty();
        pt.count = count;
        pt.scale = 1;
        let b = if negative { &mut pt.negative } else { &mut pt.positive };
        b.offset = -1;
        b.bucket_counts = buckets;
        pt.exemplars.append_empty().set_double_value(ex);
        pt.attributes.put_str("attr", attr);
    }
    metric
}

fn exp_hist(count: u64, spans: Vec<Span>, buckets: Vec<i64>, negative: bool) -> Histogram {
    let mut h = Histogram { count, schema: 1, zero_threshold: DEFAULT_ZERO_THRESHOLD, ..Default::default() };
    if negative {
        h.negative_spans = spans;
        h.negative_buckets = buckets;
    } else {
        h.positive_spans = spans;
        h.positive_buckets = buckets;
    }
    h
}

fn hsample(mf: &str, l: Labels, h: Histogram, es: Vec<Exemplar>) -> Sample {
    Sample { mf: mf.into(), l, h: Some(h), es, ..Default::default() }
}

fn ex(value: f64) -> Exemplar {
    Exemplar { value, ..Default::default() }
}

fn ex_ts(value: f64, ts: i64) -> Exemplar {
    Exemplar { value, ts, has_ts: true, ..Default::default() }
}

fn scoped(name: &str, attr: &str, promote: bool) -> Labels {
    if !promote {
        return ls(&[METRIC_NAME, name, "attr", attr]);
    }
    let s = default_scope();
    ls(&[
        METRIC_NAME,
        name,
        "attr",
        attr,
        "otel_scope_name",
        &s.name,
        "otel_scope_schema_url",
        &s.schema_url,
        "otel_scope_version",
        &s.version,
        "otel_scope_attr1",
        "value1",
        "otel_scope_attr2",
        "value2",
    ])
}

fn run_native_cases(tests: Vec<(&str, Metric, bool, Vec<Sample>)>, nhcb: bool) {
    for (name, metric, promote_scope, want) in tests {
        let app_test = Appendable::new();
        let app = app_test.appender_v2(&Context::background());
        let mut converter = PrometheusConverter::new(Some(app));
        let namer = MetricNamer { with_metric_suffixes: true, ..Default::default() };
        let prom_name = namer.build(&translator_metric_from_otel_metric(&metric)).unwrap();
        let settings =
            Settings { convert_histograms_to_nhcb: nhcb, promote_scope_metadata: promote_scope, ..Default::default() };
        let scope: Scope = default_scope();
        converter.set_resource_context(&Resource::new(), &settings).unwrap();
        converter.set_scope_context(&scope, &settings).unwrap();
        let opts = AppOpts { metric_family_name: prom_name, ..Default::default() };
        let (annots, res) = if nhcb {
            converter.add_custom_buckets_histogram_data_points(
                &Context::background(),
                &metric.histogram().data_points,
                &settings,
                AggregationTemporality::CUMULATIVE,
                &opts,
            )
        } else {
            converter.add_exponential_histogram_data_points(
                &Context::background(),
                &metric.exponential_histogram().data_points,
                &settings,
                AggregationTemporality::CUMULATIVE,
                &opts,
            )
        };
        res.unwrap();
        assert!(annots.is_empty(), "{name}");
        assert!(converter.appender_mut().unwrap().commit().is_none(), "{name}");
        require_equal(&want, &app_test.result_samples());
    }
}

#[test]
fn test_prometheus_converter_add_exponential_histogram_data_points() {
    let same_labels = || {
        exp_metric(vec![(7, vec![4, 2], false, 1.0, "test_attr"), (4, vec![4, 2, 1], false, 2.0, "test_attr")])
    };
    let tests = vec![
        (
            "histogram data points with same labels and without scope promotion",
            same_labels(),
            false,
            vec![
                hsample("test_hist", scoped("test_hist", "test_attr", false), exp_hist(7, vec![span(0, 2)], vec![4, -2], false), vec![ex(1.0)]),
                hsample("test_hist", scoped("test_hist", "test_attr", false), exp_hist(4, vec![span(0, 3)], vec![4, -2, -1], false), vec![ex(2.0)]),
            ],
        ),
        (
            "histogram data point with exemplars out of timestamp order",
            {
                let mut metric = Metric::new();
                metric.name = "test_hist".into();
                metric.set_empty_exponential_histogram().aggregation_temporality = AggregationTemporality::CUMULATIVE;
                let pt = metric.exponential_histogram_mut().data_points.append_empty();
                pt.count = 7;
                pt.scale = 1;
                pt.positive.offset = -1;
                pt.positive.bucket_counts = vec![4, 2];
                let newer = pt.exemplars.append_empty();
                newer.timestamp = ms(2000);
                newer.set_double_value(1.0);
                let older = pt.exemplars.append_empty();
                older.timestamp = ms(1000);
                older.set_double_value(2.0);
                pt.attributes.put_str("attr", "test_attr");
                metric
            },
            false,
            vec![hsample(
                "test_hist",
                scoped("test_hist", "test_attr", false),
                exp_hist(7, vec![span(0, 2)], vec![4, -2], false),
                vec![ex_ts(2.0, 1000), ex_ts(1.0, 2000)],
            )],
        ),
        (
            "histogram data points with same labels",
            same_labels(),
            true,
            vec![
                hsample("test_hist", scoped("test_hist", "test_attr", true), exp_hist(7, vec![span(0, 2)], vec![4, -2], false), vec![ex(1.0)]),
                hsample("test_hist", scoped("test_hist", "test_attr", true), exp_hist(4, vec![span(0, 3)], vec![4, -2, -1], false), vec![ex(2.0)]),
            ],
        ),
        (
            "histogram data points with different labels and without scope promotion",
            exp_metric(vec![(7, vec![4, 2], false, 1.0, "test_attr"), (4, vec![4, 2, 1], true, 2.0, "test_attr_two")]),
            false,
            vec![
                hsample("test_hist", scoped("test_hist", "test_attr", false), exp_hist(7, vec![span(0, 2)], vec![4, -2], false), vec![ex(1.0)]),
                hsample(
                    "test_hist",
                    scoped("test_hist", "test_attr_two", false),
                    exp_hist(4, vec![span(0, 3)], vec![4, -2, -1], true),
                    vec![ex(2.0)],
                ),
            ],
        ),
    ];
    run_native_cases(tests, false);
}

#[test]
fn test_convert_explicit_histogram_buckets_to_nhcb_layout() {
    let tests: Vec<(&str, Vec<u64>, Vec<Span>, Vec<i64>)> = vec![
        ("zero offset", vec![4, 3, 2, 1], vec![span(0, 4)], vec![4, -1, -1, -1]),
        ("leading empty buckets", vec![0, 0, 1, 1, 2, 3], vec![span(2, 4)], vec![1, 0, 1, 1]),
        ("trailing empty buckets", vec![0, 0, 1, 1, 2, 3, 0, 0], vec![span(2, 6)], vec![1, 0, 1, 1, -3, 0]),
        ("bucket gap of 2", vec![1, 2, 0, 0, 2], vec![span(0, 5)], vec![1, 1, -2, 0, 2]),
        ("bucket gap > 2", vec![1, 2, 0, 0, 0, 2, 4, 4], vec![span(0, 2), span(3, 3)], vec![1, 1, 0, 2, 0]),
        (
            "multiple bucket gaps",
            vec![0, 0, 1, 2, 0, 0, 0, 2, 4, 4, 0, 0],
            vec![span(2, 2), span(3, 5)],
            vec![1, 1, 0, 2, 0, -4, 0],
        ),
    ];
    for (name, buckets, want_spans, want_deltas) in tests {
        let offset = get_bucket_offset(&buckets);
        let (got_spans, got_deltas) = convert_buckets_layout(&buckets[offset..], offset as i32, 0, false);
        assert_eq!(want_spans, got_spans, "{name}");
        assert_eq!(want_deltas, got_deltas, "{name}");
    }
}

#[test]
fn test_histogram_to_custom_buckets_histogram() {
    let pt = |count: u64, sum: Option<f64>, buckets: Vec<u64>| {
        let mut pt = HistogramDataPoint::new();
        pt.start_timestamp = ms(100);
        pt.timestamp = ms(500);
        pt.count = count;
        pt.sum = sum;
        pt.bucket_counts = buckets;
        pt.explicit_bounds = vec![0.0, 1.0];
        pt
    };
    let tests = vec![
        (
            "convert hist to custom buckets hist",
            pt(2, Some(10.1), vec![1, 1]),
            Histogram {
                count: 2,
                sum: 10.1,
                schema: -53,
                positive_spans: vec![span(0, 2)],
                positive_buckets: vec![1, 0],
                custom_values: custom_values(&[0.0, 1.0]),
                ..Default::default()
            },
        ),
        (
            "convert hist to custom buckets hist with no sum",
            pt(4, None, vec![2, 2]),
            Histogram {
                count: 4,
                schema: -53,
                positive_spans: vec![span(0, 2)],
                positive_buckets: vec![2, 0],
                custom_values: custom_values(&[0.0, 1.0]),
                ..Default::default()
            },
        ),
    ];
    for (name, hist, want) in tests {
        validate_histogram_count(&hist);
        let (got, annots) = explicit_histogram_to_custom_buckets_histogram(&hist, AggregationTemporality::CUMULATIVE);
        assert!(annots.is_empty(), "{name}");
        assert_eq!(want, got, "{name}");
        validate_native_histogram_count(&got);
    }
}

fn nhcb_metric(points: Vec<(u64, f64, Vec<u64>, Vec<f64>, f64, &'static str)>) -> Metric {
    let mut metric = Metric::new();
    metric.name = "test_hist_to_nhcb".into();
    metric.set_empty_histogram().aggregation_temporality = AggregationTemporality::CUMULATIVE;
    for (count, sum, buckets, bounds, ex, attr) in points {
        let pt = metric.histogram_mut().data_points.append_empty();
        pt.count = count;
        pt.set_sum(sum);
        pt.bucket_counts = buckets;
        pt.explicit_bounds = bounds;
        pt.exemplars.append_empty().set_double_value(ex);
        pt.attributes.put_str("attr", attr);
    }
    metric
}

fn nhcb(count: u64, sum: f64, spans: Vec<Span>, buckets: Vec<i64>, cv: &[f64]) -> Histogram {
    Histogram {
        count,
        sum,
        schema: -53,
        positive_spans: spans,
        positive_buckets: buckets,
        custom_values: custom_values(cv),
        ..Default::default()
    }
}

#[test]
fn test_prometheus_converter_add_custom_buckets_histogram_data_points() {
    const N: &str = "test_hist_to_nhcb";
    let same_labels = || {
        nhcb_metric(vec![
            (3, 3.0, vec![2, 0, 1], vec![5.0, 10.0], 1.0, "test_attr"),
            (11, 5.0, vec![3, 8, 0], vec![0.0, 1.0], 2.0, "test_attr"),
        ])
    };
    let tests = vec![
        (
            "histogram data points with same labels and without scope promotion",
            same_labels(),
            false,
            vec![
                hsample(N, scoped(N, "test_attr", false), nhcb(3, 3.0, vec![span(0, 3)], vec![2, -2, 1], &[5.0, 10.0]), vec![ex(1.0)]),
                hsample(N, scoped(N, "test_attr", false), nhcb(11, 5.0, vec![span(0, 3)], vec![3, 5, -8], &[0.0, 1.0]), vec![ex(2.0)]),
            ],
        ),
        (
            "histogram data point with exemplars out of timestamp order",
            {
                let mut metric = Metric::new();
                metric.name = N.into();
                metric.set_empty_histogram().aggregation_temporality = AggregationTemporality::CUMULATIVE;
                let pt = metric.histogram_mut().data_points.append_empty();
                pt.count = 3;
                pt.set_sum(3.0);
                pt.bucket_counts = vec![2, 0, 1];
                pt.explicit_bounds = vec![5.0, 10.0];
                let newer = pt.exemplars.append_empty();
                newer.timestamp = ms(2000);
                newer.set_double_value(1.0);
                let older = pt.exemplars.append_empty();
                older.timestamp = ms(1000);
                older.set_double_value(12.0);
                pt.attributes.put_str("attr", "test_attr");
                metric
            },
            false,
            vec![hsample(
                N,
                scoped(N, "test_attr", false),
                nhcb(3, 3.0, vec![span(0, 3)], vec![2, -2, 1], &[5.0, 10.0]),
                vec![ex_ts(12.0, 1000), ex_ts(1.0, 2000)],
            )],
        ),
        (
            "histogram data points with same labels",
            same_labels(),
            true,
            vec![
                hsample(N, scoped(N, "test_attr", true), nhcb(3, 3.0, vec![span(0, 3)], vec![2, -2, 1], &[5.0, 10.0]), vec![ex(1.0)]),
                hsample(N, scoped(N, "test_attr", true), nhcb(11, 5.0, vec![span(0, 3)], vec![3, 5, -8], &[0.0, 1.0]), vec![ex(2.0)]),
            ],
        ),
        (
            "histogram data points with different labels and without scope promotion",
            nhcb_metric(vec![
                (6, 3.0, vec![4, 2], vec![0.0, 1.0], 1.0, "test_attr"),
                (11, 5.0, vec![3, 8], vec![0.0, 1.0], 2.0, "test_attr_two"),
            ]),
            false,
            vec![
                hsample(N, scoped(N, "test_attr", false), nhcb(6, 3.0, vec![span(0, 2)], vec![4, -2], &[0.0, 1.0]), vec![ex(1.0)]),
                hsample(N, scoped(N, "test_attr_two", false), nhcb(11, 5.0, vec![span(0, 2)], vec![3, 5], &[0.0, 1.0]), vec![ex(2.0)]),
            ],
        ),
    ];
    run_native_cases(tests, true);
}
