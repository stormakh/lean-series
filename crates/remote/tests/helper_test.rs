//! Port of `storage/remote/otlptranslator/prometheusremotewrite/helper_test.go`.

mod otlp_testutil;

use std::collections::HashMap;

use model::exemplar::Exemplar;
use model::labels::{BUCKET_LABEL, Labels, METRIC_NAME};
use model::metadata::{Metadata, MetricType};
use model::schema::{METRIC_TYPE_LABEL, METRIC_UNIT_LABEL};
use otlp_testutil::*;
use remote::otlptranslator::namer::UnitNamer;
use remote::otlptranslator::pdata::{AggregationTemporality, AppendEmpty, Exemplar as OtlpExemplar, Map, Metric, Resource};
use remote::otlptranslator::prometheusremotewrite::{
    AppOpts, BUCKET_STR, COUNT_STR, MAX_EXEMPLAR_RUNES, PrometheusConverter, QUANTILE_STR, RESERVED_LABEL_NAMES,
    SUM_STR, Scope, Settings, WarningCategory, convert_time_stamp, count_warnings_by_category,
    new_promote_resource_attributes,
};
use remote::prompb::types::{Label as PromLabel, MetricMetadata, MetricType as PromMetricType};
use storage::{AppendableV2, Context};
use util::teststorage::{Appendable, Sample, require_equal};

fn new_converter() -> (Appendable, PrometheusConverter) {
    let app_test = Appendable::new();
    let app = app_test.appender_v2(&Context::background());
    (app_test, PrometheusConverter::new(Some(app)))
}

fn smp(mf: &str, l: Labels, t: i64, st: i64, v: f64) -> Sample {
    Sample { mf: mf.into(), l, t, st, v, ..Default::default() }
}

struct CreateAttrsCase {
    name: &'static str,
    resource: Option<Resource>,
    attrs: Option<Map>,
    scope: Scope,
    promote_all_resource_attributes: bool,
    promote_resource_attributes: Vec<&'static str>,
    promote_scope: bool,
    ignore_resource_attributes: Vec<&'static str>,
    ignore_attrs: Vec<&'static str>,
    label_name_underscore_sanitization: bool,
    label_name_preserve_multiple_underscores: bool,
    expected_labels: Labels,
    expected_collision_warnings: Vec<&'static str>,
}

impl Default for CreateAttrsCase {
    fn default() -> Self {
        CreateAttrsCase {
            name: "",
            resource: None,
            attrs: None,
            scope: Scope::default(),
            promote_all_resource_attributes: false,
            promote_resource_attributes: vec![],
            promote_scope: false,
            ignore_resource_attributes: vec![],
            ignore_attrs: vec![],
            label_name_underscore_sanitization: false,
            label_name_preserve_multiple_underscores: false,
            expected_labels: Labels::default(),
            expected_collision_warnings: vec![],
        }
    }
}

fn map_of(pairs: &[(&str, &str)]) -> Map {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.put_str(k, v);
    }
    m
}

fn resource_of(pairs: &[(&str, &str)]) -> Resource {
    let mut r = Resource::new();
    for (k, v) in pairs {
        r.attributes.put_str(k, v);
    }
    r
}

#[test]
fn test_prometheus_converter_create_attributes() {
    let resource = resource_of(&[
        ("service.name", "service name"),
        ("service.instance.id", "service ID"),
        ("existent-attr", "resource value"),
        // Conflicts with a metric attribute.
        ("metric-attr", "resource value"),
        // Conflicts with the generated job label.
        ("job", "resource value"),
        // Conflicts with the generated instance label.
        ("instance", "resource value"),
    ]);
    let default_scope = default_scope();
    let attrs = map_of(&[("metric-attr", "metric value"), ("metric-attr-other", "metric value other")]);

    // Resources with underscores for the sanitization tests.
    let resource_with_underscores = resource_of(&[
        ("service.name", "service name"),
        ("service.instance.id", "service ID"),
        ("_private", "private value"),
        ("__reserved__", "reserved value"),
        ("label___multi", "multi value"),
    ]);
    let attrs_with_underscores = map_of(&[("_metric_private", "private metric"), ("metric___multi", "multi metric")]);

    // Attribute names that collide after sanitization.
    let colliding_attrs = map_of(&[("a.b", "1"), ("a_b", "2")]);
    let triple_colliding_attrs = map_of(&[("a-b", "1"), ("a.b", "2"), ("a_b", "3")]);
    let multi_group_colliding_attrs = map_of(&[("a.b", "1"), ("a_b", "2"), ("c.d", "3"), ("c_d", "4")]);
    let collision_with_extra_attrs = map_of(&[("a.b", "1"), ("a_b", "2"), ("unrelated", "9")]);

    let scope_labels = |extra: &[&str]| -> Labels {
        let mut v: Vec<&str> = extra.to_vec();
        v.extend_from_slice(&[
            "otel_scope_name",
            "test-scope",
            "otel_scope_schema_url",
            "https://schema.com",
            "otel_scope_version",
            "1.0.0",
            "otel_scope_attr1",
            "value1",
            "otel_scope_attr2",
            "value2",
        ]);
        ls(&v)
    };

    let test_cases = vec![
        CreateAttrsCase {
            name: "Successful conversion without resource attribute promotion and without scope promotion",
            scope: default_scope.clone(),
            expected_labels: ls(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "metric_attr", "metric value",
                "metric_attr_other", "metric value other",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Successful conversion without resource attribute promotion and with scope promotion",
            scope: default_scope.clone(),
            promote_scope: true,
            expected_labels: scope_labels(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "metric_attr", "metric value",
                "metric_attr_other", "metric value other",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Successful conversion without resource attribute promotion and with scope promotion, but without scope",
            scope: Scope::default(),
            promote_scope: true,
            expected_labels: ls(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "metric_attr", "metric value",
                "metric_attr_other", "metric value other",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Successful conversion with some attributes ignored",
            ignore_attrs: vec!["metric-attr-other"],
            expected_labels: ls(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "metric_attr", "metric value",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Successful conversion with some attributes ignored and with scope promotion",
            scope: default_scope.clone(),
            promote_scope: true,
            ignore_attrs: vec!["metric-attr-other"],
            expected_labels: scope_labels(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "metric_attr", "metric value",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Successful conversion with resource attribute promotion and with scope promotion",
            scope: default_scope.clone(),
            promote_resource_attributes: vec!["non-existent-attr", "existent-attr"],
            promote_scope: true,
            expected_labels: scope_labels(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "metric_attr", "metric value",
                "metric_attr_other", "metric value other", "existent_attr", "resource value",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Successful conversion with resource attribute promotion and with scope promotion, conflicting resource attributes are ignored",
            scope: default_scope.clone(),
            promote_scope: true,
            promote_resource_attributes: vec!["non-existent-attr", "existent-attr", "metric-attr", "job", "instance"],
            expected_labels: scope_labels(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "existent_attr",
                "resource value", "metric_attr", "metric value", "metric_attr_other", "metric value other",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Successful conversion with resource attribute promotion and with scope promotion, attributes are only promoted once",
            scope: default_scope.clone(),
            promote_scope: true,
            promote_resource_attributes: vec!["existent-attr", "existent-attr"],
            expected_labels: scope_labels(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "existent_attr",
                "resource value", "metric_attr", "metric value", "metric_attr_other", "metric value other",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Successful conversion promoting all resource attributes and with scope promotion",
            scope: default_scope.clone(),
            promote_all_resource_attributes: true,
            promote_scope: true,
            expected_labels: scope_labels(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "existent_attr",
                "resource value", "metric_attr", "metric value", "metric_attr_other", "metric value other",
                "service_name", "service name", "service_instance_id", "service ID",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Successful conversion promoting all resource attributes and with scope promotion, ignoring 'service.instance.id'",
            scope: default_scope.clone(),
            promote_scope: true,
            promote_all_resource_attributes: true,
            ignore_resource_attributes: vec!["service.instance.id"],
            expected_labels: scope_labels(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "existent_attr",
                "resource value", "metric_attr", "metric value", "metric_attr_other", "metric value other",
                "service_name", "service name",
            ]),
            ..Default::default()
        },
        // Label sanitization test cases
        CreateAttrsCase {
            name: "Underscore sanitization enabled - prepends key_ to labels starting with single _",
            resource: Some(resource_with_underscores.clone()),
            attrs: Some(attrs_with_underscores.clone()),
            promote_resource_attributes: vec!["_private"],
            label_name_underscore_sanitization: true,
            label_name_preserve_multiple_underscores: true,
            expected_labels: ls(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "key_private",
                "private value", "key_metric_private", "private metric", "metric___multi", "multi metric",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Underscore sanitization disabled - keeps labels with _ as-is",
            resource: Some(resource_with_underscores.clone()),
            attrs: Some(attrs_with_underscores.clone()),
            promote_resource_attributes: vec!["_private"],
            label_name_underscore_sanitization: false,
            label_name_preserve_multiple_underscores: true,
            expected_labels: ls(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "_private", "private value",
                "_metric_private", "private metric", "metric___multi", "multi metric",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Multiple underscores preserved - keeps consecutive underscores",
            resource: Some(resource_with_underscores.clone()),
            attrs: Some(attrs_with_underscores.clone()),
            promote_resource_attributes: vec!["label___multi"],
            label_name_underscore_sanitization: false,
            label_name_preserve_multiple_underscores: true,
            expected_labels: ls(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "label___multi",
                "multi value", "_metric_private", "private metric", "metric___multi", "multi metric",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Multiple underscores collapsed - collapses to single underscore",
            resource: Some(resource_with_underscores.clone()),
            attrs: Some(attrs_with_underscores.clone()),
            promote_resource_attributes: vec!["label___multi"],
            label_name_underscore_sanitization: false,
            label_name_preserve_multiple_underscores: false,
            expected_labels: ls(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "label_multi",
                "multi value", "_metric_private", "private metric", "metric_multi", "multi metric",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Both sanitization options enabled",
            resource: Some(resource_with_underscores.clone()),
            attrs: Some(attrs_with_underscores.clone()),
            promote_resource_attributes: vec!["_private", "label___multi"],
            label_name_underscore_sanitization: true,
            label_name_preserve_multiple_underscores: true,
            expected_labels: ls(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "key_private",
                "private value", "label___multi", "multi value", "key_metric_private", "private metric",
                "metric___multi", "multi metric",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Both sanitization options disabled",
            resource: Some(resource_with_underscores.clone()),
            attrs: Some(attrs_with_underscores.clone()),
            promote_resource_attributes: vec!["_private", "label___multi"],
            label_name_underscore_sanitization: false,
            label_name_preserve_multiple_underscores: false,
            expected_labels: ls(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "_private", "private value",
                "label_multi", "multi value", "_metric_private", "private metric", "metric_multi", "multi metric",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Reserved labels (starting with __) are never modified",
            resource: Some(resource_with_underscores.clone()),
            attrs: Some(attrs_with_underscores.clone()),
            promote_resource_attributes: vec!["__reserved__"],
            label_name_underscore_sanitization: true,
            label_name_preserve_multiple_underscores: false,
            expected_labels: ls(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "__reserved__",
                "reserved value", "key_metric_private", "private metric", "metric_multi", "multi metric",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "__name__ attribute is filtered when passed in ignoreAttrs",
            ignore_attrs: vec![METRIC_NAME],
            expected_labels: ls(&[
                "__name__", "test_metric", "instance", "service ID", "job", "service name", "metric_attr", "metric value",
                "metric_attr_other", "metric value other",
            ]),
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Attributes colliding after sanitization are concatenated and recorded as a warning",
            attrs: Some(colliding_attrs),
            expected_labels: ls(&["__name__", "test_metric", "a_b", "1;2", "instance", "service ID", "job", "service name"]),
            expected_collision_warnings: vec![
                r#"OTLP data point attributes "a.b", "a_b" collide as label "a_b" after name sanitization, values are concatenated with ';'"#,
            ],
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Three colliding attributes report all names in concatenation order",
            attrs: Some(triple_colliding_attrs),
            expected_labels: ls(&["__name__", "test_metric", "a_b", "1;2;3", "instance", "service ID", "job", "service name"]),
            expected_collision_warnings: vec![
                r#"OTLP data point attributes "a-b", "a.b", "a_b" collide as label "a_b" after name sanitization, values are concatenated with ';'"#,
            ],
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Multiple independent collision groups each produce a distinct warning",
            attrs: Some(multi_group_colliding_attrs),
            expected_labels: ls(&[
                "__name__", "test_metric", "a_b", "1;2", "c_d", "3;4", "instance", "service ID", "job", "service name",
            ]),
            expected_collision_warnings: vec![
                r#"OTLP data point attributes "a.b", "a_b" collide as label "a_b" after name sanitization, values are concatenated with ';'"#,
                r#"OTLP data point attributes "c.d", "c_d" collide as label "c_d" after name sanitization, values are concatenated with ';'"#,
            ],
            ..Default::default()
        },
        CreateAttrsCase {
            name: "Attributes not participating in a collision are excluded from the warning",
            attrs: Some(collision_with_extra_attrs),
            expected_labels: ls(&[
                "__name__", "test_metric", "a_b", "1;2", "instance", "service ID", "job", "service name", "unrelated", "9",
            ]),
            expected_collision_warnings: vec![
                r#"OTLP data point attributes "a.b", "a_b" collide as label "a_b" after name sanitization, values are concatenated with ';'"#,
            ],
            ..Default::default()
        },
    ];
    for tc in test_cases {
        let (_, mut c) = new_converter();
        let settings = Settings {
            promote_resource_attributes: Some(new_promote_resource_attributes(&config::OtlpConfig {
                promote_all_resource_attributes: tc.promote_all_resource_attributes,
                promote_resource_attributes: tc.promote_resource_attributes.iter().map(|s| s.to_string()).collect(),
                ignore_resource_attributes: tc.ignore_resource_attributes.iter().map(|s| s.to_string()).collect(),
                ..Default::default()
            })),
            promote_scope_metadata: tc.promote_scope,
            label_name_underscore_sanitization: tc.label_name_underscore_sanitization,
            label_name_preserve_multiple_underscores: tc.label_name_preserve_multiple_underscores,
            ..Default::default()
        };
        // Use the test case's resource and attributes when set.
        let test_resource = tc.resource.as_ref().unwrap_or(&resource);
        let test_attrs = tc.attrs.as_ref().unwrap_or(&attrs);
        // Initialize resource and scope context as FromMetrics would.
        c.set_resource_context(test_resource, &settings).unwrap();
        c.set_scope_context(&tc.scope, &settings).unwrap();

        let lbls = c
            .create_attributes(test_attrs, &settings, &tc.ignore_attrs, false, &Metadata::default(), &[METRIC_NAME, "test_metric"])
            .unwrap();
        assert_eq!(tc.expected_labels, lbls, "{}", tc.name);
        let mut want: Vec<String> = tc.expected_collision_warnings.iter().map(|s| s.to_string()).collect();
        let mut got = collision_warnings(&c.collision_annots);
        want.sort();
        got.sort();
        assert_eq!(want, got, "{}", tc.name);
    }

    // __name__ attributes in OTLP data are filtered out to prevent duplicate labels.
    {
        let resource = resource_of(&[("service.name", "test-service"), ("service.instance.id", "test-instance")]);
        let attrs_with_name_label = map_of(&[("__name__", "wrong_metric_name"), ("other_attr", "value")]);
        let (_, mut c) = new_converter();
        let settings = Settings::default();
        c.set_resource_context(&resource, &settings).unwrap();
        c.set_scope_context(&Scope::default(), &settings).unwrap();
        let lbls = c
            .create_attributes(
                &attrs_with_name_label,
                &settings,
                RESERVED_LABEL_NAMES,
                true,
                &Metadata::default(),
                &[METRIC_NAME, "correct_metric_name"],
            )
            .unwrap();
        let names: Vec<(&str, &str)> = lbls.iter().filter(|(n, _)| *n == METRIC_NAME).collect();
        assert_eq!(1, names.len());
        assert_eq!("correct_metric_name", names[0].1);
        assert_eq!("value", lbls.get("other_attr"));
    }

    // __type__ and __unit__ attributes are overwritten by metadata when EnableTypeAndUnitLabels is true.
    {
        let resource = resource_of(&[("service.name", "test-service"), ("service.instance.id", "test-instance")]);
        let attrs_with_type_and_unit =
            map_of(&[(METRIC_TYPE_LABEL, "wrong_type"), (METRIC_UNIT_LABEL, "wrong_unit"), ("other_attr", "value")]);
        let (_, mut c) = new_converter();
        let settings = Settings { enable_type_and_unit_labels: true, ..Default::default() };
        c.set_resource_context(&resource, &settings).unwrap();
        c.set_scope_context(&Scope::default(), &settings).unwrap();
        let lbls = c
            .create_attributes(
                &attrs_with_type_and_unit,
                &settings,
                RESERVED_LABEL_NAMES,
                true,
                &Metadata { typ: MetricType::Gauge, unit: "seconds".into(), ..Default::default() },
                &[METRIC_NAME, "test_metric"],
            )
            .unwrap();
        let types: Vec<(&str, &str)> = lbls.iter().filter(|(n, _)| *n == METRIC_TYPE_LABEL).collect();
        assert_eq!(1, types.len());
        assert_eq!("gauge", types[0].1);
        let units: Vec<(&str, &str)> = lbls.iter().filter(|(n, _)| *n == METRIC_UNIT_LABEL).collect();
        assert_eq!(1, units.len());
        assert_eq!("seconds", units[0].1);
        assert_eq!("value", lbls.get("other_attr"));
    }
}

#[test]
fn test_convert_time_stamp() {
    let tests: [(&str, u64, i64); 3] = [("zero", 0, 0), ("1ms", 1_000_000, 1), ("1s", 1_000_000_000, 1000)];
    for (name, arg, want) in tests {
        assert_eq!(want, convert_time_stamp(arg), "{name}");
    }
}

fn with_scope_labels(scope: &Scope, rest: &[&str]) -> Labels {
    let mut v = vec![
        "otel_scope_attr1",
        "value1",
        "otel_scope_attr2",
        "value2",
        "otel_scope_name",
        &scope.name,
        "otel_scope_schema_url",
        &scope.schema_url,
        "otel_scope_version",
        &scope.version,
    ];
    v.extend_from_slice(rest);
    ls(&v)
}

#[test]
fn test_prometheus_converter_add_summary_data_points() {
    let default_scope = default_scope();
    let ts = now_nanos();
    let t = convert_time_stamp(ts);
    let sum_name = format!("test_summary{SUM_STR}");
    let count_name = format!("test_summary{COUNT_STR}");

    let summary = |start: bool| {
        let mut metric = Metric::new();
        metric.name = "test_summary".into();
        let dp = metric.set_empty_summary().data_points.append_empty();
        dp.timestamp = ts;
        if start {
            dp.start_timestamp = ts;
        }
        metric
    };
    let tests: Vec<(&str, Metric, bool, Vec<Sample>)> = vec![
        (
            "summary with start time and without scope promotion",
            summary(true),
            false,
            vec![
                smp("test_summary", ls(&[METRIC_NAME, &sum_name]), t, t, 0.0),
                smp("test_summary", ls(&[METRIC_NAME, &count_name]), t, t, 0.0),
            ],
        ),
        (
            "summary with start time and with scope promotion",
            summary(true),
            true,
            vec![
                smp("test_summary", with_scope_labels(&default_scope, &[METRIC_NAME, &sum_name]), t, t, 0.0),
                smp("test_summary", with_scope_labels(&default_scope, &[METRIC_NAME, &count_name]), t, t, 0.0),
            ],
        ),
        (
            "summary without start time and without scope promotion",
            summary(false),
            false,
            vec![
                smp("test_summary", ls(&[METRIC_NAME, &sum_name]), t, 0, 0.0),
                smp("test_summary", ls(&[METRIC_NAME, &count_name]), t, 0, 0.0),
            ],
        ),
        (
            "summary without start time and without scope promotion and some quantiles",
            {
                let mut metric = summary(false);
                let dp = &mut metric.summary_mut().data_points[0];
                dp.count = 50;
                dp.sum = 100.0;
                let h = dp.quantile_values.append_empty();
                h.quantile = 0.5;
                h.value = 30.0;
                let n = dp.quantile_values.append_empty();
                n.quantile = 0.9;
                n.value = 40.0;
                metric
            },
            false,
            vec![
                smp("test_summary", ls(&[METRIC_NAME, &sum_name]), t, 0, 100.0),
                smp("test_summary", ls(&[METRIC_NAME, &count_name]), t, 0, 50.0),
                smp("test_summary", ls(&[METRIC_NAME, "test_summary", QUANTILE_STR, "0.5"]), t, 0, 30.0),
                smp("test_summary", ls(&[METRIC_NAME, "test_summary", QUANTILE_STR, "0.9"]), t, 0, 40.0),
            ],
        ),
    ];
    for (name, metric, promote_scope, want) in tests {
        let (app_test, mut converter) = new_converter();
        let settings = Settings { promote_scope_metadata: promote_scope, ..Default::default() };
        converter.set_resource_context(&Resource::new(), &settings).unwrap();
        converter.set_scope_context(&default_scope, &settings).unwrap();
        converter
            .add_summary_data_points(
                &Context::background(),
                &metric.summary().data_points,
                &settings,
                &AppOpts { metric_family_name: metric.name.clone(), ..Default::default() },
            )
            .unwrap();
        assert!(converter.appender_mut().unwrap().commit().is_none(), "{name}");
        require_equal(&want, &app_test.result_samples());
    }
}

fn hist_metric(f: impl FnOnce(&mut remote::otlptranslator::pdata::HistogramDataPoint)) -> Metric {
    let mut metric = Metric::new();
    metric.name = "test_hist".into();
    metric.set_empty_histogram().aggregation_temporality = AggregationTemporality::CUMULATIVE;
    f(metric.histogram_mut().data_points.append_empty());
    metric
}

#[test]
fn test_prometheus_converter_add_histogram_data_points() {
    let default_scope = default_scope();
    let ts = now_nanos();
    let t = convert_time_stamp(ts);
    let count_name = format!("test_hist{COUNT_STR}");
    let sum_name = format!("test_hist{SUM_STR}");
    let bucket_name = format!("test_hist{BUCKET_STR}");
    let second = 1_000_000_000u64;
    let plus_inf = HashMap::from([(WarningCategory::HistogramPlusInfBound, 1usize)]);

    struct Case {
        name: &'static str,
        metric: Metric,
        scope: Scope,
        promote_scope: bool,
        want: Vec<Sample>,
        want_warnings: HashMap<WarningCategory, usize>,
    }
    let newer = convert_time_stamp(ts);
    let older = convert_time_stamp(ts - second);
    let ex = |value: f64, ts: i64| Exemplar { value, ts, has_ts: true, ..Default::default() };

    let explicit_inf = |count: u64, buckets: Vec<u64>| {
        hist_metric(|pt| {
            pt.timestamp = ts;
            pt.count = count;
            pt.set_sum(count as f64);
            pt.bucket_counts = buckets;
            pt.explicit_bounds = vec![1.0, 2.0, f64::INFINITY];
        })
    };
    let explicit_inf_want = |v: f64| {
        vec![
            smp("test_hist", ls(&[METRIC_NAME, &sum_name]), t, 0, v),
            smp("test_hist", ls(&[METRIC_NAME, &count_name]), t, 0, v),
            smp("test_hist", ls(&[METRIC_NAME, &bucket_name, BUCKET_LABEL, "1"]), t, 0, 5.0),
            smp("test_hist", ls(&[METRIC_NAME, &bucket_name, BUCKET_LABEL, "2"]), t, 0, 15.0),
            smp("test_hist", ls(&[METRIC_NAME, &bucket_name, BUCKET_LABEL, "+Inf"]), t, 0, v),
        ]
    };

    let tests = vec![
        Case {
            name: "histogram with start time and without scope promotion",
            metric: hist_metric(|pt| {
                pt.timestamp = ts;
                pt.start_timestamp = ts;
            }),
            scope: default_scope.clone(),
            promote_scope: false,
            want: vec![
                smp("test_hist", ls(&[METRIC_NAME, &count_name]), t, t, 0.0),
                smp("test_hist", ls(&[METRIC_NAME, "test_hist_bucket", BUCKET_LABEL, "+Inf"]), t, t, 0.0),
            ],
            want_warnings: HashMap::new(),
        },
        Case {
            name: "histogram with start time and with scope promotion",
            metric: hist_metric(|pt| {
                pt.timestamp = ts;
                pt.start_timestamp = ts;
            }),
            scope: default_scope.clone(),
            promote_scope: true,
            want: vec![
                smp("test_hist", with_scope_labels(&default_scope, &[METRIC_NAME, &count_name]), t, t, 0.0),
                smp(
                    "test_hist",
                    with_scope_labels(&default_scope, &[METRIC_NAME, "test_hist_bucket", BUCKET_LABEL, "+Inf"]),
                    t,
                    t,
                    0.0,
                ),
            ],
            want_warnings: HashMap::new(),
        },
        Case {
            name: "histogram without start time",
            metric: hist_metric(|pt| pt.timestamp = ts),
            scope: Scope::default(),
            promote_scope: false,
            want: vec![
                smp("test_hist", ls(&[METRIC_NAME, &count_name]), t, 0, 0.0),
                smp("test_hist", ls(&[METRIC_NAME, "test_hist_bucket", BUCKET_LABEL, "+Inf"]), t, 0, 0.0),
            ],
            want_warnings: HashMap::new(),
        },
        Case {
            name: "histogram with exemplars out of timestamp order",
            metric: hist_metric(|pt| {
                pt.timestamp = ts;
                pt.count = 4;
                pt.explicit_bounds = vec![5.0];
                pt.bucket_counts = vec![2, 2];
                // Each bucket gets a newer exemplar followed by an older one.
                for (value, ets) in [(1.0, ts), (2.0, ts - second), (7.0, ts), (8.0, ts - second)] {
                    let e = pt.exemplars.append_empty();
                    e.set_double_value(value);
                    e.timestamp = ets;
                }
            }),
            scope: Scope::default(),
            promote_scope: false,
            want: vec![
                smp("test_hist", ls(&[METRIC_NAME, &count_name]), newer, 0, 4.0),
                Sample {
                    es: vec![ex(2.0, older), ex(1.0, newer)],
                    ..smp("test_hist", ls(&[METRIC_NAME, "test_hist_bucket", BUCKET_LABEL, "5"]), newer, 0, 2.0)
                },
                Sample {
                    es: vec![ex(8.0, older), ex(7.0, newer)],
                    ..smp("test_hist", ls(&[METRIC_NAME, "test_hist_bucket", BUCKET_LABEL, "+Inf"]), newer, 0, 4.0)
                },
            ],
            want_warnings: HashMap::new(),
        },
        Case {
            // An explicit +Inf bound leaves the implicit last bucket spanning (+Inf, +infinity), so its count
            // can only be 0.
            name: "histogram with an explicit +Inf bound and an empty trailing bucket",
            metric: explicit_inf(35, vec![5, 10, 20, 0]),
            scope: Scope::default(),
            promote_scope: false,
            want: explicit_inf_want(35.0),
            want_warnings: plus_inf.clone(),
        },
        Case {
            // Invalid input: a trailing count of 7 could not have been observed.
            name: "histogram with an explicit +Inf bound and a non-empty trailing bucket",
            metric: explicit_inf(42, vec![5, 10, 20, 7]),
            scope: Scope::default(),
            promote_scope: false,
            want: explicit_inf_want(42.0),
            want_warnings: plus_inf.clone(),
        },
        Case {
            // Invalid the other way: count is not the sum of the bucket counts.
            name: "histogram with an explicit +Inf bound and a count above its bucket sum",
            metric: explicit_inf(99, vec![5, 10, 20, 0]),
            scope: Scope::default(),
            promote_scope: false,
            want: explicit_inf_want(99.0),
            want_warnings: plus_inf.clone(),
        },
    ];
    for tt in tests {
        let (app_test, mut converter) = new_converter();
        let settings = Settings { promote_scope_metadata: tt.promote_scope, ..Default::default() };
        converter.set_resource_context(&Resource::new(), &settings).unwrap();
        converter.set_scope_context(&tt.scope, &settings).unwrap();
        converter
            .add_histogram_data_points(
                &Context::background(),
                &tt.metric.histogram().data_points,
                &settings,
                &AppOpts { metric_family_name: tt.metric.name.clone(), ..Default::default() },
            )
            .unwrap();
        assert!(converter.appender_mut().unwrap().commit().is_none(), "{}", tt.name);
        require_equal(&tt.want, &app_test.result_samples());
        assert_eq!(tt.want_warnings, count_warnings_by_category(&converter.plus_inf_annots), "{}", tt.name);
    }
}

/// Exemplars of a previous data point must not leak into `_sum` and `_count` of the next one.
#[test]
fn test_add_histogram_data_points_exemplar_leak_across_data_points() {
    let ts = now_nanos();
    let ex_ts = ts + 1_000_000_000;

    let mut metric = Metric::new();
    metric.name = "test_hist".into();
    metric.set_empty_histogram().aggregation_temporality = AggregationTemporality::CUMULATIVE;
    // First data point: buckets and an exemplar with value 200 (above the bound 100, so in +Inf).
    let pt1 = metric.histogram_mut().data_points.append_empty();
    pt1.timestamp = ts;
    pt1.start_timestamp = ts;
    pt1.set_sum(42.0);
    pt1.count = 10;
    pt1.explicit_bounds = vec![100.0];
    pt1.bucket_counts = vec![7, 3];
    let ex = pt1.exemplars.append_empty();
    ex.timestamp = ex_ts;
    ex.set_double_value(200.0);
    // Second data point: no exemplars.
    let pt2 = metric.histogram_mut().data_points.append_empty();
    pt2.timestamp = ts;
    pt2.start_timestamp = ts;
    pt2.set_sum(84.0);
    pt2.count = 20;
    pt2.explicit_bounds = vec![100.0];
    pt2.bucket_counts = vec![14, 6];

    let (app_test, mut converter) = new_converter();
    let settings = Settings::default();
    converter.set_resource_context(&Resource::new(), &settings).unwrap();
    converter.set_scope_context(&Scope::default(), &settings).unwrap();
    converter
        .add_histogram_data_points(
            &Context::background(),
            &metric.histogram().data_points,
            &settings,
            &AppOpts { metric_family_name: metric.name.clone(), ..Default::default() },
        )
        .unwrap();
    assert!(converter.appender_mut().unwrap().commit().is_none());

    let ex_converted = Exemplar { value: 200.0, ts: convert_time_stamp(ex_ts), has_ts: true, ..Default::default() };
    let t = convert_time_stamp(ts);
    let want = vec![
        // First data point: _sum and _count without exemplars, le=100 without, le=+Inf with the exemplar.
        smp("test_hist", ls(&[METRIC_NAME, "test_hist_sum"]), t, t, 42.0),
        smp("test_hist", ls(&[METRIC_NAME, "test_hist_count"]), t, t, 10.0),
        smp("test_hist", ls(&[METRIC_NAME, "test_hist_bucket", BUCKET_LABEL, "100"]), t, t, 7.0),
        Sample {
            es: vec![ex_converted],
            ..smp("test_hist", ls(&[METRIC_NAME, "test_hist_bucket", BUCKET_LABEL, "+Inf"]), t, t, 10.0)
        },
        // Second data point: no exemplars anywhere (the regression check).
        smp("test_hist", ls(&[METRIC_NAME, "test_hist_sum"]), t, t, 84.0),
        smp("test_hist", ls(&[METRIC_NAME, "test_hist_count"]), t, t, 20.0),
        smp("test_hist", ls(&[METRIC_NAME, "test_hist_bucket", BUCKET_LABEL, "100"]), t, t, 14.0),
        smp("test_hist", ls(&[METRIC_NAME, "test_hist_bucket", BUCKET_LABEL, "+Inf"]), t, t, 20.0),
    ];
    require_equal(&want, &app_test.result_samples());
}

#[test]
fn test_get_prom_exemplars() {
    let ctx = Context::background();
    let (_, mut c) = new_converter();

    // Exemplars with int value.
    {
        let mut e = OtlpExemplar::new();
        e.timestamp = now_nanos();
        e.set_int_value(42);
        let exemplars = c.get_prom_exemplars(&ctx, &[e]).unwrap();
        assert_eq!(1, exemplars.len());
        assert_eq!(42.0, exemplars[0].value);
    }
    // Exemplars with double value.
    {
        let mut e = OtlpExemplar::new();
        e.timestamp = now_nanos();
        e.set_double_value(69.420);
        let exemplars = c.get_prom_exemplars(&ctx, &[e]).unwrap();
        assert_eq!(1, exemplars.len());
        assert_eq!(69.420, exemplars[0].value);
    }
    // Exemplars with unsupported value type.
    {
        let mut e = OtlpExemplar::new();
        e.timestamp = now_nanos();
        assert!(c.get_prom_exemplars(&ctx, &[e]).is_err());
    }
    // Exemplars with filtered attributes colliding with trace context.
    let trace_id: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let span_id: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
    const TRACE_ID_HEX: &str = "0102030405060708090a0b0c0d0e0f10";
    const SPAN_ID_HEX: &str = "0102030405060708";
    let long = "a".repeat(MAX_EXEMPLAR_RUNES);
    let cases: Vec<(&str, [u8; 16], [u8; 8], Vec<(&str, &str)>, Labels)> = vec![
        (
            "trace and span ID override filtered attributes",
            trace_id,
            span_id,
            vec![("trace_id", "attr-trace"), ("span_id", "attr-span"), ("foo", "bar")],
            ls(&["trace_id", TRACE_ID_HEX, "span_id", SPAN_ID_HEX, "foo", "bar"]),
        ),
        (
            "only trace ID overrides filtered attribute",
            trace_id,
            [0; 8],
            vec![("trace_id", "attr-trace"), ("span_id", "attr-span")],
            ls(&["trace_id", TRACE_ID_HEX, "span_id", "attr-span"]),
        ),
        (
            "only span ID overrides filtered attribute",
            [0; 16],
            span_id,
            vec![("trace_id", "attr-trace"), ("span_id", "attr-span")],
            ls(&["trace_id", "attr-trace", "span_id", SPAN_ID_HEX]),
        ),
        (
            "filtered attributes are kept without trace context",
            [0; 16],
            [0; 8],
            vec![("trace_id", "attr-trace"), ("span_id", "attr-span")],
            ls(&["trace_id", "attr-trace", "span_id", "attr-span"]),
        ),
        (
            "overridden filtered attributes do not count towards rune limit",
            trace_id,
            span_id,
            vec![("trace_id", &long), ("foo", "bar")],
            ls(&["trace_id", TRACE_ID_HEX, "span_id", SPAN_ID_HEX, "foo", "bar"]),
        ),
    ];
    for (name, tid, sid, attrs, expected) in cases {
        let mut e = OtlpExemplar::new();
        e.timestamp = now_nanos();
        e.set_double_value(1.0);
        e.trace_id = tid;
        e.span_id = sid;
        e.filtered_attributes = map_of(&attrs);
        let exemplars = c.get_prom_exemplars(&ctx, &[e]).unwrap();
        assert_eq!(1, exemplars.len(), "{name}");
        assert_eq!(expected, exemplars[0].labels, "{name}");
    }
}

/// The test's own `addTypeAndUnitLabels`: appends type and unit labels to the labels.
fn add_type_and_unit_labels(labels: Vec<PromLabel>, metadata: &MetricMetadata, settings: &Settings) -> Vec<PromLabel> {
    let unit_namer = UnitNamer { utf8_allowed: settings.allow_utf8 };
    let mut labels: Vec<PromLabel> =
        labels.into_iter().filter(|l| l.name != "__type__" && l.name != "__unit__").collect();
    labels.push(PromLabel::new("__type__", metadata.r#type.name().to_lowercase()));
    labels.push(PromLabel::new("__unit__", unit_namer.build(&metadata.unit)));
    labels
}

#[test]
fn test_add_type_and_unit_labels() {
    let l = |n: &str, v: &str| PromLabel::new(n, v);
    let test_cases = vec![
        (
            "overwrites existing type and unit labels and preserves other labels",
            vec![
                l("job", "test-job"),
                l("__type__", "old_type"),
                l("instance", "test-instance"),
                l("__unit__", "old_unit"),
                l("custom_label", "custom_value"),
            ],
            MetricMetadata { r#type: PromMetricType::COUNTER, unit: "seconds".into(), ..Default::default() },
            vec![
                l("job", "test-job"),
                l("instance", "test-instance"),
                l("custom_label", "custom_value"),
                l("__type__", "counter"),
                l("__unit__", "seconds"),
            ],
        ),
        (
            "adds type and unit labels when missing",
            vec![l("job", "test-job"), l("instance", "test-instance")],
            MetricMetadata { r#type: PromMetricType::GAUGE, unit: "bytes".into(), ..Default::default() },
            vec![l("job", "test-job"), l("instance", "test-instance"), l("__type__", "gauge"), l("__unit__", "bytes")],
        ),
    ];
    for (name, input, metadata, mut expected) in test_cases {
        let mut result = add_type_and_unit_labels(input, &metadata, &Settings { allow_utf8: false, ..Default::default() });
        // require.ElementsMatch
        result.sort();
        expected.sort();
        assert_eq!(expected, result, "{name}");
    }
}
