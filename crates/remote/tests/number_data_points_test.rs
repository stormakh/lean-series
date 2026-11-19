//! Port of `storage/remote/otlptranslator/prometheusremotewrite/number_data_points_test.go`.

mod otlp_testutil;


use model::exemplar::Exemplar;
use model::labels::METRIC_NAME;
use model::metadata::Metadata;
use otlp_testutil::*;
use remote::otlptranslator::pdata::{AggregationTemporality, AppendEmpty, Map, Metric, Resource};
use remote::otlptranslator::prometheusremotewrite::{AppOpts, PrometheusConverter, Scope, Settings, convert_time_stamp};
use storage::{AppendableV2, Context};
use util::teststorage::{Appendable, Sample, require_equal};

struct Case {
    name: &'static str,
    metric: Box<dyn Fn() -> Metric>,
    scope: Scope,
    promote_scope: bool,
    want: Box<dyn Fn() -> Vec<Sample>>,
}

fn run(tests: Vec<Case>, sum: bool) {
    for tt in tests {
        let metric = (tt.metric)();
        let app_test = Appendable::new();
        let app = app_test.appender_v2(&Context::background());
        let mut converter = PrometheusConverter::new(Some(app));
        let settings = Settings { promote_scope_metadata: tt.promote_scope, ..Default::default() };
        let resource = Resource::new();

        // Initialize resource and scope context as FromMetrics would.
        converter.set_resource_context(&resource, &settings).unwrap();
        converter.set_scope_context(&tt.scope, &settings).unwrap();

        let opts = AppOpts { metric_family_name: metric.name.clone(), ..Default::default() };
        let ctx = Context::background();
        let _ = if sum {
            converter.add_sum_number_data_points(&ctx, &metric.sum().data_points, &settings, &opts)
        } else {
            converter.add_gauge_number_data_points(&ctx, &metric.gauge().data_points, &settings, &opts)
        };
        assert!(converter.appender_mut().unwrap().commit().is_none(), "{}", tt.name);
        require_equal(&(tt.want)(), &app_test.result_samples());
    }
}

fn scope_labels(name: &str) -> model::labels::Labels {
    let s = default_scope();
    ls(&[
        METRIC_NAME,
        name,
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

#[test]
fn test_prometheus_converter_add_gauge_number_data_points() {
    let ts = now_nanos();
    let tests = vec![
        Case {
            name: "gauge without scope promotion",
            metric: Box::new(move || get_int_gauge_metric("test", &Map::new(), 1, ts)),
            scope: default_scope(),
            promote_scope: false,
            want: Box::new(move || {
                vec![Sample {
                    mf: "test".into(),
                    l: ls(&[METRIC_NAME, "test"]),
                    m: Metadata::default(),
                    t: convert_time_stamp(ts),
                    v: 1.0,
                    ..Default::default()
                }]
            }),
        },
        Case {
            name: "gauge with scope promotion",
            metric: Box::new(move || get_int_gauge_metric("test", &Map::new(), 1, ts)),
            scope: default_scope(),
            promote_scope: true,
            want: Box::new(move || {
                vec![Sample {
                    mf: "test".into(),
                    l: scope_labels("test"),
                    m: Metadata::default(),
                    t: convert_time_stamp(ts),
                    v: 1.0,
                    ..Default::default()
                }]
            }),
        },
    ];
    run(tests, false);
}

#[test]
fn test_prometheus_converter_add_sum_number_data_points() {
    let ts = now_nanos();
    let second = 1_000_000_000u64;
    let tests = vec![
        Case {
            name: "sum without scope promotion",
            metric: Box::new(move || get_int_sum_metric("test", &Map::new(), 1, ts)),
            scope: default_scope(),
            promote_scope: false,
            want: Box::new(move || {
                vec![Sample {
                    mf: "test".into(),
                    l: ls(&[METRIC_NAME, "test"]),
                    t: convert_time_stamp(ts),
                    v: 1.0,
                    ..Default::default()
                }]
            }),
        },
        Case {
            name: "sum with scope promotion",
            metric: Box::new(move || get_int_sum_metric("test", &Map::new(), 1, ts)),
            scope: default_scope(),
            promote_scope: true,
            want: Box::new(move || {
                vec![Sample {
                    mf: "test".into(),
                    l: scope_labels("test"),
                    t: convert_time_stamp(ts),
                    v: 1.0,
                    ..Default::default()
                }]
            }),
        },
        Case {
            name: "sum with exemplars and without scope promotion",
            metric: Box::new(move || {
                let mut m = get_int_sum_metric("test", &Map::new(), 1, ts);
                m.sum_mut().data_points[0].exemplars.append_empty().set_double_value(2.0);
                m
            }),
            scope: default_scope(),
            promote_scope: false,
            want: Box::new(move || {
                vec![Sample {
                    mf: "test".into(),
                    l: ls(&[METRIC_NAME, "test"]),
                    t: convert_time_stamp(ts),
                    v: 1.0,
                    es: vec![Exemplar { value: 2.0, ..Default::default() }],
                    ..Default::default()
                }]
            }),
        },
        Case {
            name: "sum with exemplars out of timestamp order",
            metric: Box::new(move || {
                let mut m = get_int_sum_metric("test", &Map::new(), 1, ts);
                let exemplars = &mut m.sum_mut().data_points[0].exemplars;
                let newer = exemplars.append_empty();
                newer.timestamp = ts;
                newer.set_double_value(2.0);
                let older = exemplars.append_empty();
                older.timestamp = ts - second;
                older.set_double_value(1.0);
                m
            }),
            scope: default_scope(),
            promote_scope: false,
            want: Box::new(move || {
                vec![Sample {
                    mf: "test".into(),
                    l: ls(&[METRIC_NAME, "test"]),
                    t: convert_time_stamp(ts),
                    v: 1.0,
                    es: vec![
                        Exemplar { value: 1.0, ts: convert_time_stamp(ts - second), has_ts: true, ..Default::default() },
                        Exemplar { value: 2.0, ts: convert_time_stamp(ts), has_ts: true, ..Default::default() },
                    ],
                    ..Default::default()
                }]
            }),
        },
        Case {
            name: "monotonic cumulative sum with start timestamp and without scope promotion",
            metric: Box::new(move || {
                let mut metric = Metric::new();
                metric.name = "test_sum".into();
                metric.set_empty_sum().aggregation_temporality = AggregationTemporality::CUMULATIVE;
                // Go calls SetEmptySum again, which resets the temporality.
                metric.set_empty_sum().is_monotonic = true;
                let dp = metric.sum_mut().data_points.append_empty();
                dp.set_double_value(1.0);
                dp.timestamp = ts;
                dp.start_timestamp = ts;
                metric
            }),
            scope: default_scope(),
            promote_scope: false,
            want: Box::new(move || {
                vec![Sample {
                    mf: "test_sum".into(),
                    l: ls(&[METRIC_NAME, "test_sum"]),
                    t: convert_time_stamp(ts),
                    st: convert_time_stamp(ts),
                    v: 1.0,
                    ..Default::default()
                }]
            }),
        },
        Case {
            name: "monotonic cumulative sum with no start time and without scope promotion",
            metric: Box::new(move || {
                let mut metric = Metric::new();
                metric.name = "test_sum".into();
                metric.set_empty_sum().aggregation_temporality = AggregationTemporality::CUMULATIVE;
                metric.set_empty_sum().is_monotonic = true;
                metric.sum_mut().data_points.append_empty().timestamp = ts;
                metric
            }),
            scope: default_scope(),
            promote_scope: false,
            want: Box::new(move || {
                vec![Sample {
                    mf: "test_sum".into(),
                    l: ls(&[METRIC_NAME, "test_sum"]),
                    t: convert_time_stamp(ts),
                    v: 0.0,
                    ..Default::default()
                }]
            }),
        },
        Case {
            name: "non-monotonic cumulative sum with start time and without scope promotion",
            metric: Box::new(move || {
                let mut metric = Metric::new();
                metric.name = "test_sum".into();
                metric.set_empty_sum().aggregation_temporality = AggregationTemporality::CUMULATIVE;
                metric.set_empty_sum().is_monotonic = false;
                metric.sum_mut().data_points.append_empty().timestamp = ts;
                metric
            }),
            scope: default_scope(),
            promote_scope: false,
            want: Box::new(move || {
                vec![Sample {
                    mf: "test_sum".into(),
                    l: ls(&[METRIC_NAME, "test_sum"]),
                    t: convert_time_stamp(ts),
                    v: 0.0,
                    ..Default::default()
                }]
            }),
        },
    ];
    run(tests, true);
}
