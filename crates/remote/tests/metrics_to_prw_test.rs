//! Port of `storage/remote/otlptranslator/prometheusremotewrite/metrics_to_prw_test.go`. The benchmarks are
//! not ported.

mod otlp_testutil;

use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use model::histogram::{CounterResetHint, FloatHistogram, Histogram, Span, custom_values};
use model::labels::{Labels, METRIC_NAME};
use model::metadata::{Metadata, MetricType};
use otlp_testutil::*;
use remote::otlptranslator::namer::{self, LabelNamer};
use remote::otlptranslator::pdata::{
    AggregationTemporality, AppendEmpty, Exemplar, Map, Metric, Metrics, new_timestamp_from_time, timestamp_unix_milli,
};
use remote::otlptranslator::prometheusremotewrite::*;
use storage::annotations::Annotations;
use storage::{
    AppendV2Options, AppendableV2, AppenderTransaction, AppenderV2, Context, ERR_CANCELED, ERR_DEADLINE_EXCEEDED,
    Error, SeriesRef,
};
use util::teststorage::{Appendable, Sample, require_equal};

/// Go `noOpAppender`.
#[derive(Default)]
struct NoOpAppender {
    samples: usize,
    histograms: usize,
    metadata: usize,
}

impl AppenderTransaction for NoOpAppender {
    fn commit(&mut self) -> Option<Error> {
        None
    }
    fn rollback(&mut self) -> Option<Error> {
        None
    }
}

impl AppenderV2 for NoOpAppender {
    fn append(
        &mut self,
        _: SeriesRef,
        _: &Labels,
        _: i64,
        _: i64,
        _: f64,
        h: Option<&Histogram>,
        _: Option<&FloatHistogram>,
        opts: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        if opts.metadata.is_some_and(|m| !m.is_empty()) {
            self.metadata += 1;
        }
        if h.is_some() {
            self.histograms += 1;
        } else {
            self.samples += 1;
        }
        Ok(1)
    }
}

fn new_converter() -> (Appendable, PrometheusConverter) {
    let app_test = Appendable::new();
    let app = app_test.appender_v2(&Context::background());
    (app_test, PrometheusConverter::new(Some(app)))
}

fn commit(c: &mut PrometheusConverter) {
    assert!(c.appender_mut().unwrap().commit().is_none());
}

fn as_strings(a: &Annotations) -> (Vec<String>, Vec<String>) {
    a.as_strings("", 0, 0)
}

#[test]
fn test_prometheus_converter_reset() {
    // isolates requests
    {
        let mut request = Metrics::new();
        let metric = request.resource_metrics.append_empty().scope_metrics.append_empty().metrics.append_empty();
        metric.name = "test_gauge".into();
        let dp = metric.set_empty_gauge().data_points.append_empty();
        dp.timestamp = 0;
        dp.set_int_value(1);
        dp.attributes.put_str("foo.bar", "value");

        let mut converter = PrometheusConverter::new(None);
        for (name, allow_utf8, label_name, other_label_name) in
            [("escaped labels", false, "foo_bar", "foo.bar"), ("UTF-8 labels", true, "foo.bar", "foo_bar")]
        {
            let appendable = Appendable::new();
            converter.reset(Some(appendable.appender_v2(&Context::background())));
            let (annots, err) = converter.from_metrics(
                &Context::background(),
                &request,
                &Settings { allow_utf8, disable_target_info: true, ..Default::default() },
            );
            assert!(err.is_none(), "{name}");
            assert!(annots.is_empty(), "{name}");
            commit(&mut converter);
            let samples = appendable.result_samples();
            assert_eq!(1, samples.len(), "{name}");
            assert_eq!("value", samples[0].l.get(label_name), "{name}");
            assert!(!samples[0].l.has(other_label_name), "{name}");
            converter.reset(None);
        }
    }

    // clears request state
    {
        let mut converter = PrometheusConverter::new(Some(Box::new(NoOpAppender::default())));
        converter.every_n = EveryNTimes { n: 128, i: 1, err: Some(ERR_CANCELED) };
        converter.scratch_builder.add("request_label", "request_value");
        converter.builder.set("request_label", "request_value");
        converter.seen_target_info = HashSet::from([TargetInfoKey::default()]);
        converter.resource_labels = Some(CachedResourceLabels { job_label: "request_job".into(), ..Default::default() });
        converter.scope_labels = Some(CachedScopeLabels { scope_name: "request_scope".into(), ..Default::default() });
        converter.label_namer = LabelNamer { utf8_allowed: true, ..Default::default() };
        converter.sanitized_labels.as_mut().unwrap().insert("request_label".into(), "request_label".into());
        converter.collision_annots = Annotations::new().with(Error::msg("request annotation"));
        converter.recorded_collisions = Some(HashSet::from(["request_label".to_string()]));
        converter.collision_source = CollisionAttrSource::Resource;

        converter.reset(None);

        // adapted: Go compares the whole struct with PrometheusConverter{sanitizedLabels: map[string]string{}};
        // here every field is compared with its zero value.
        assert_eq!(converter.every_n, EveryNTimes::default());
        assert!(converter.scratch_builder.is_empty());
        assert!(converter.builder.labels().is_empty());
        assert!(converter.appender.is_none());
        assert!(converter.seen_target_info.is_empty());
        assert!(converter.resource_labels.is_none());
        assert!(converter.scope_labels.is_none());
        assert_eq!(converter.label_namer, LabelNamer::default());
        assert_eq!(converter.sanitized_labels, Some(HashMap::new()));
        assert!(converter.plus_inf_annots.is_empty());
        assert!(converter.collision_annots.is_empty());
        assert!(converter.recorded_collisions.is_none());
        assert_eq!(converter.collision_source, CollisionAttrSource::DataPoint);
    }

    // drops large label cache
    {
        let mut converter = PrometheusConverter::new(Some(Box::new(NoOpAppender::default())));
        for i in 0..MAX_SANITIZED_LABELS + 1 {
            let label = format!("request_label_{i}");
            converter.sanitized_labels.as_mut().unwrap().insert(label.clone(), label);
        }
        converter.reset(None);
        assert!(converter.sanitized_labels.is_none());
        converter.reset(Some(Box::new(NoOpAppender::default())));
        assert!(converter.sanitized_labels.as_ref().is_some_and(|m| m.is_empty()));
    }
}

/// Go `generateAttributes`.
fn generate_attributes(m: &mut Map, prefix: &str, count: usize) {
    for i in 1..=count {
        m.put_str(&format!("{prefix}-name-{i}"), &format!("value-{i}"));
    }
}

/// Go `generateExemplars`.
fn generate_exemplars(exemplars: &mut Vec<Exemplar>, count: usize, ts: u64) {
    for _ in 1..=count {
        let e = exemplars.append_empty();
        e.timestamp = ts;
        e.set_double_value(2.22);
        e.span_id = [1, 2, 3, 4, 5, 6, 7, 8];
        e.trace_id = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    }
}

/// Go `wantPrometheusMetric`.
#[allow(dead_code)]
struct WantPrometheusMetric {
    name: String,
    family_name: String,
    metric_type: MetricType,
    description: String,
    unit: String,
}

/// Go `createExportRequest`.
fn create_export_request(
    resource_attribute_count: usize,
    histogram_count: usize,
    non_histogram_count: usize,
    labels_per_metric: usize,
    exemplars_per_series: usize,
    settings: &Settings,
    temporality: AggregationTemporality,
) -> (Metrics, Vec<WantPrometheusMetric>) {
    let mut request = Metrics::new();
    let rm = request.resource_metrics.append_empty();
    generate_attributes(&mut rm.resource.attributes, "resource", resource_attribute_count);
    for (k, v) in [("service.name", "test-service"), ("service.namespace", "test-namespace"), ("service.instance.id", "id1234")] {
        rm.resource.attributes.put_str(k, v);
    }
    let metrics = &mut rm.scope_metrics.append_empty().metrics;
    let ts = new_timestamp_from_time(SystemTime::now());
    let suffix = if settings.add_metric_suffixes { "_unit" } else { "" };
    let cumulative = temporality == AggregationTemporality::CUMULATIVE;
    let mut want = Vec::new();
    let mut push = |name: String, family: String, t: MetricType, desc: &str| {
        want.push(WantPrometheusMetric { name, family_name: family, metric_type: t, description: desc.into(), unit: "unit".into() })
    };
    for i in 1..=histogram_count {
        let m = metrics.append_empty();
        m.set_empty_histogram();
        m.name = format!("histogram-{i}");
        m.description = "histogram".into();
        m.unit = "unit".into();
        m.histogram_mut().aggregation_temporality = temporality;
        let h = m.histogram_mut().data_points.append_empty();
        h.timestamp = ts;
        // 50 samples, 10 each with values 0.5, 1, 2, 4, and 8.
        h.count = 50;
        h.set_sum(155.0);
        h.bucket_counts = vec![10, 10, 10, 10, 10, 0];
        h.explicit_bounds = vec![0.5, 1.0, 2.0, 4.0, 8.0, 16.0];
        generate_attributes(&mut h.attributes, "series", labels_per_metric);
        generate_exemplars(&mut h.exemplars, exemplars_per_series, ts);
        let t = if cumulative { MetricType::Histogram } else { MetricType::Unknown };
        for s in ["_bucket", "_count", "_sum"] {
            push(format!("histogram_{i}{suffix}{s}"), format!("histogram_{i}{suffix}"), t, "histogram");
        }
    }
    for i in 1..=non_histogram_count {
        let m = metrics.append_empty();
        m.set_empty_sum();
        m.name = format!("non.monotonic.sum-{i}");
        m.description = "sum".into();
        m.unit = "unit".into();
        m.sum_mut().aggregation_temporality = temporality;
        let p = m.sum_mut().data_points.append_empty();
        p.timestamp = ts;
        p.set_double_value(1.23);
        generate_attributes(&mut p.attributes, "series", labels_per_metric);
        generate_exemplars(&mut p.exemplars, exemplars_per_series, ts);
        let t = if cumulative { MetricType::Gauge } else { MetricType::Unknown };
        push(format!("non_monotonic_sum_{i}{suffix}"), format!("non_monotonic_sum_{i}{suffix}"), t, "sum");
    }
    for i in 1..=non_histogram_count {
        let m = metrics.append_empty();
        m.set_empty_sum();
        m.name = format!("monotonic.sum-{i}");
        m.description = "sum".into();
        m.unit = "unit".into();
        m.sum_mut().aggregation_temporality = temporality;
        m.sum_mut().is_monotonic = true;
        let p = m.sum_mut().data_points.append_empty();
        p.timestamp = ts;
        p.set_double_value(1.23);
        generate_attributes(&mut p.attributes, "series", labels_per_metric);
        generate_exemplars(&mut p.exemplars, exemplars_per_series, ts);
        let counter_suffix = if settings.add_metric_suffixes { format!("{suffix}_total") } else { String::new() };
        let t = if cumulative { MetricType::Counter } else { MetricType::Unknown };
        push(format!("monotonic_sum_{i}{counter_suffix}"), format!("monotonic_sum_{i}{counter_suffix}"), t, "sum");
    }
    for i in 1..=non_histogram_count {
        let m = metrics.append_empty();
        m.set_empty_gauge();
        m.name = format!("gauge-{i}");
        m.description = "gauge".into();
        m.unit = "unit".into();
        let p = m.gauge_mut().data_points.append_empty();
        p.timestamp = ts;
        p.set_double_value(1.23);
        generate_attributes(&mut p.attributes, "series", labels_per_metric);
        generate_exemplars(&mut p.exemplars, exemplars_per_series, ts);
        push(format!("gauge_{i}{suffix}"), format!("gauge_{i}{suffix}"), MetricType::Gauge, "gauge");
    }
    push("target_info".into(), "target_info".into(), MetricType::Unknown, "");
    (request, want)
}

fn hist_sample(mf: &str, l: Labels, t: i64, typ: MetricType, h: Histogram) -> Sample {
    Sample { mf: mf.into(), l, t, m: Metadata { typ, ..Default::default() }, h: Some(h), ..Default::default() }
}

fn float_sample(mf: &str, l: Labels, t: i64, typ: MetricType, v: f64) -> Sample {
    Sample { mf: mf.into(), l, t, m: Metadata { typ, ..Default::default() }, v, ..Default::default() }
}

#[test]
fn test_from_metrics() {
    // Successful
    for (name, settings, temporality) in [
        ("Default with cumulative temporality", Settings::default(), AggregationTemporality::CUMULATIVE),
        (
            "Default with delta temporality",
            Settings { allow_delta_temporality: true, ..Default::default() },
            AggregationTemporality::DELTA,
        ),
        (
            "Keep identifying attributes",
            Settings { keep_identifying_resource_attributes: true, ..Default::default() },
            AggregationTemporality::CUMULATIVE,
        ),
        (
            "Add metric suffixes with cumulative temporality",
            Settings { add_metric_suffixes: true, ..Default::default() },
            AggregationTemporality::CUMULATIVE,
        ),
        (
            "Add metric suffixes with delta temporality",
            Settings { add_metric_suffixes: true, allow_delta_temporality: true, ..Default::default() },
            AggregationTemporality::DELTA,
        ),
    ] {
        let (app_test, mut converter) = new_converter();
        let (payload, want_prom_metrics) = create_export_request(5, 128, 128, 2, 0, &settings, temporality);
        let mut seen_family_names = HashSet::new();
        for w in &want_prom_metrics {
            if w.family_name == "target_info" {
                continue;
            }
            seen_family_names.insert(w.family_name.clone());
        }
        let (annots, err) = converter.from_metrics(&Context::background(), &payload, &settings);
        assert!(err.is_none(), "{name}: {err:?}");
        assert!(annots.is_empty(), "{name}");
        commit(&mut converter);
        let got = app_test.result_samples();
        assert_eq!(1536 + 1, got.len(), "{name}"); // +1 for the target_info.
        let mut tgt_info_count = 0;
        for s in &got {
            let l = &s.l;
            if l.get(METRIC_NAME) == "target_info" {
                tgt_info_count += 1;
                assert_eq!("test-namespace/test-service", l.get("job"));
                assert_eq!("id1234", l.get("instance"));
                if settings.keep_identifying_resource_attributes {
                    assert_eq!("test-service", l.get("service_name"));
                    assert_eq!("test-namespace", l.get("service_namespace"));
                    assert_eq!("id1234", l.get("service_instance_id"));
                } else {
                    assert!(!l.has("service_name"));
                    assert!(!l.has("service_namespace"));
                    assert!(!l.has("service_instance_id"));
                }
            }
        }
        assert_eq!(1, tgt_info_count, "{name}");
    }

    for convert_histograms_to_nhcb in [false, true] {
        let mut request = Metrics::new();
        let rm = request.resource_metrics.append_empty();
        generate_attributes(&mut rm.resource.attributes, "resource", 10);
        let metrics = &mut rm.scope_metrics.append_empty().metrics;
        let ts = new_timestamp_from_time(SystemTime::now());
        let m = metrics.append_empty();
        m.set_empty_histogram();
        m.name = "histogram-1".into();
        m.histogram_mut().aggregation_temporality = AggregationTemporality::CUMULATIVE;
        let h = m.histogram_mut().data_points.append_empty();
        h.timestamp = ts;
        h.count = 15;
        h.set_sum(155.0);
        // The last bucket is the (1.123, +Inf] overflow bucket.
        h.bucket_counts = vec![3, 11, 1];
        h.explicit_bounds = vec![0.124, 1.123];
        generate_attributes(&mut h.attributes, "series", 1);

        let (app_test, mut converter) = new_converter();
        let (annots, err) = converter.from_metrics(
            &Context::background(),
            &request,
            &Settings { convert_histograms_to_nhcb, ..Default::default() },
        );
        assert!(err.is_none());
        assert!(annots.is_empty());
        commit(&mut converter);

        let t = timestamp_unix_milli(ts);
        let ht = MetricType::Histogram;
        let expected = if convert_histograms_to_nhcb {
            vec![hist_sample(
                "histogram_1",
                ls(&["__name__", "histogram_1", "series_name_1", "value-1"]),
                t,
                ht,
                Histogram {
                    schema: -53,
                    count: 15,
                    sum: 155.0,
                    positive_spans: vec![Span { offset: 0, length: 3 }],
                    positive_buckets: vec![3, 8, -10],
                    custom_values: custom_values(&[0.124, 1.123]),
                    ..Default::default()
                },
            )]
        } else {
            vec![
                float_sample("histogram_1", ls(&["__name__", "histogram_1_sum", "series_name_1", "value-1"]), t, ht, 155.0),
                float_sample("histogram_1", ls(&["__name__", "histogram_1_count", "series_name_1", "value-1"]), t, ht, 15.0),
                float_sample(
                    "histogram_1",
                    ls(&["__name__", "histogram_1_bucket", "le", "0.124", "series_name_1", "value-1"]),
                    t,
                    ht,
                    3.0,
                ),
                float_sample(
                    "histogram_1",
                    ls(&["__name__", "histogram_1_bucket", "le", "1.123", "series_name_1", "value-1"]),
                    t,
                    ht,
                    14.0,
                ),
                float_sample(
                    "histogram_1",
                    ls(&["__name__", "histogram_1_bucket", "le", "+Inf", "series_name_1", "value-1"]),
                    t,
                    ht,
                    15.0,
                ),
            ]
        };
        // Every expected native histogram must be one the appender would accept.
        for es in &expected {
            if let Some(h) = &es.h {
                assert!(h.validate().is_ok(), "expected histogram for {} does not validate", es.l);
            }
        }
        require_equal(&expected, &app_test.result_samples());
    }

    // context cancellation
    {
        let settings = Settings::default();
        let (_, mut converter) = new_converter();
        let (ctx, cancel) = Context::background().with_cancel();
        cancel.cancel();
        let (payload, _) = create_export_request(5, 128, 128, 2, 0, &settings, AggregationTemporality::CUMULATIVE);
        let (annots, err) = converter.from_metrics(&ctx, &payload, &settings);
        assert!(err.unwrap().is(&ERR_CANCELED));
        assert!(annots.is_empty());
    }

    // context timeout
    {
        let settings = Settings::default();
        let (_, mut converter) = new_converter();
        let (ctx, _cancel) = Context::background().with_timeout(Duration::ZERO);
        let (payload, _) = create_export_request(5, 128, 128, 2, 0, &settings, AggregationTemporality::CUMULATIVE);
        let (annots, err) = converter.from_metrics(&ctx, &payload, &settings);
        assert!(err.unwrap().is(&ERR_DEADLINE_EXCEEDED));
        assert!(annots.is_empty());
    }

    // exponential histogram and explicit histogram (NHCB) warnings for zero count and non-zero sum
    for (exponential, want) in [
        (true, "exponential histogram data point has zero count, but non-zero sum: 155.000000"),
        (false, "histogram data point has zero count, but non-zero sum: 155.000000"),
    ] {
        let mut request = Metrics::new();
        let rm = request.resource_metrics.append_empty();
        generate_attributes(&mut rm.resource.attributes, "resource", 10);
        let metrics = &mut rm.scope_metrics.append_empty().metrics;
        let ts = new_timestamp_from_time(SystemTime::now());
        for i in 1..=10 {
            let m = metrics.append_empty();
            m.name = format!("histogram-{i}");
            if exponential {
                m.set_empty_exponential_histogram().aggregation_temporality = AggregationTemporality::CUMULATIVE;
                let h = m.exponential_histogram_mut().data_points.append_empty();
                h.timestamp = ts;
                h.count = 0;
                h.set_sum(155.0);
                generate_attributes(&mut h.attributes, "series", 10);
            } else {
                m.set_empty_histogram().aggregation_temporality = AggregationTemporality::CUMULATIVE;
                let h = m.histogram_mut().data_points.append_empty();
                h.timestamp = ts;
                h.count = 0;
                h.set_sum(155.0);
                generate_attributes(&mut h.attributes, "series", 10);
            }
        }
        let (_, mut converter) = new_converter();
        let settings = Settings { convert_histograms_to_nhcb: !exponential, ..Default::default() };
        let (annots, err) = converter.from_metrics(&Context::background(), &request, &settings);
        assert!(err.is_none());
        assert!(!annots.is_empty());
        let (ws, infos) = as_strings(&annots);
        assert!(infos.is_empty());
        assert_eq!(vec![want.to_string()], ws);
    }

    // empty data points are surfaced as warnings
    let builders: Vec<(&str, fn(&mut Metric))> = vec![
        ("gauge", |m| {
            m.set_empty_gauge();
        }),
        ("sum", |m| m.set_empty_sum().aggregation_temporality = AggregationTemporality::CUMULATIVE),
        ("histogram", |m| m.set_empty_histogram().aggregation_temporality = AggregationTemporality::CUMULATIVE),
        ("exponential histogram", |m| {
            m.set_empty_exponential_histogram().aggregation_temporality = AggregationTemporality::CUMULATIVE
        }),
        ("summary", |m| {
            m.set_empty_summary();
        }),
    ];
    for (name, build_empty) in builders {
        let mut request = Metrics::new();
        let m = request.resource_metrics.append_empty().scope_metrics.append_empty().metrics.append_empty();
        m.name = "test_empty".into();
        build_empty(m);
        let (_, mut converter) = new_converter();
        let (annots, err) = converter.from_metrics(&Context::background(), &request, &Settings::default());
        assert!(err.is_none(), "{name}");
        assert_eq!(HashMap::from([(WarningCategory::EmptyDataPoints, 1)]), count_warnings_by_category(&annots), "{name}");
        let (ws, infos) = as_strings(&annots);
        assert!(infos.is_empty());
        assert_eq!(vec!["empty data points. test_empty is dropped".to_string()], ws, "{name}");
    }

    let gauge_request = |values: &[&str], resource: &[(&str, &str)]| {
        let mut request = Metrics::new();
        let rm = request.resource_metrics.append_empty();
        for (k, v) in resource {
            rm.resource.attributes.put_str(k, v);
        }
        let m = rm.scope_metrics.append_empty().metrics.append_empty();
        m.name = "test_gauge".into();
        let g = m.set_empty_gauge();
        let ts = new_timestamp_from_time(SystemTime::now());
        for (i, v) in values.iter().enumerate() {
            let dp = g.data_points.append_empty();
            dp.timestamp = ts;
            dp.set_int_value(i as i64 + if values.len() == 1 { 1 } else { 0 });
            if !v.is_empty() {
                dp.attributes.put_str("a.b", "x");
                dp.attributes.put_str("a_b", v);
            }
        }
        request
    };

    // attribute collision is surfaced as a warning
    {
        let request = gauge_request(&["y"], &[]);
        let (_, mut converter) = new_converter();
        let (annots, err) = converter.from_metrics(&Context::background(), &request, &Settings::default());
        assert!(err.is_none());
        let want = vec![
            r#"OTLP data point attributes "a.b", "a_b" collide as label "a_b" after name sanitization, values are concatenated with ';'"#
                .to_string(),
        ];
        let (ws, infos) = as_strings(&annots);
        assert!(infos.is_empty());
        assert_eq!(want, ws);
        assert_eq!(want, collision_warnings(&annots));
    }

    // resource attribute collision is surfaced as a warning
    {
        let request = gauge_request(&[""], &[("k8s.pod.name", "foo"), ("k8s_pod_name", "bar")]);
        let (_, mut converter) = new_converter();
        let (annots, err) = converter.from_metrics(&Context::background(), &request, &Settings::default());
        assert!(err.is_none());
        let (ws, _) = as_strings(&annots);
        assert!(ws.contains(
            &r#"OTLP resource attributes "k8s.pod.name", "k8s_pod_name" collide as label "k8s_pod_name" after name sanitization, values are concatenated with ';'"#
                .to_string()
        ));
    }

    // repeated collision across data points is recorded once
    {
        let request = gauge_request(&["y", "z"], &[]);
        let (_, mut converter) = new_converter();
        let (annots, err) = converter.from_metrics(&Context::background(), &request, &Settings::default());
        assert!(err.is_none());
        let (ws, _) = as_strings(&annots);
        assert_eq!(
            vec![
                r#"OTLP data point attributes "a.b", "a_b" collide as label "a_b" after name sanitization, values are concatenated with ';'"#
                    .to_string()
            ],
            ws
        );
    }

    // no collision warning when UTF-8 is allowed
    {
        let request = gauge_request(&["y"], &[]);
        let (_, mut converter) = new_converter();
        let (annots, err) =
            converter.from_metrics(&Context::background(), &request, &Settings { allow_utf8: true, ..Default::default() });
        assert!(err.is_none());
        assert!(collision_warnings(&annots).is_empty());
        let (ws, _) = as_strings(&annots);
        assert!(ws.is_empty());
    }

    let lookback = DEFAULT_LOOKBACK_DELTA;
    let target_info_meta = Metadata { typ: MetricType::Gauge, help: "Target metadata".into(), ..Default::default() };
    let ti_sample = |t: i64, l: &Labels| Sample {
        mf: "target_info".into(),
        v: 1.0,
        t,
        l: l.clone(),
        m: target_info_meta.clone(),
        ..Default::default()
    };
    let service_attrs = [("service.name", "test-service"), ("service.namespace", "test-namespace"), ("service.instance.id", "id1234")];

    // target_info samples start at the earliest metric sample timestamp and end at the latest one of the
    // resource, with one sample every lookback delta/2 between.
    {
        let mut request = Metrics::new();
        let rm = request.resource_metrics.append_empty();
        generate_attributes(&mut rm.resource.attributes, "resource", 5);
        for (k, v) in service_attrs {
            rm.resource.attributes.put_str(k, v);
        }
        let metrics = &mut rm.scope_metrics.append_empty().metrics;
        let now = SystemTime::now();
        let ts = new_timestamp_from_time(now);
        for i in 0..3 {
            let m = metrics.append_empty();
            m.set_empty_gauge();
            m.name = format!("gauge-{}", i + 1);
            m.description = "gauge".into();
            m.unit = "unit".into();
            // Samples every lookback delta / 4.
            let mut cur = now;
            for _ in 0..6 {
                let p = m.gauge_mut().data_points.append_empty();
                p.timestamp = new_timestamp_from_time(cur);
                p.set_double_value(1.23);
                generate_attributes(&mut p.attributes, "series", 2);
                cur += lookback / 4;
            }
        }
        let (app_test, mut converter) = new_converter();
        let (annots, err) = converter.from_metrics(
            &Context::background(),
            &request,
            &Settings { lookback_delta: lookback, ..Default::default() },
        );
        assert!(err.is_none());
        assert!(annots.is_empty());
        commit(&mut converter);
        let got = app_test.result_samples();
        assert_eq!(22, got.len());
        let target_info_labels = ls(&[
            "__name__", "target_info", "instance", "id1234", "job", "test-namespace/test-service", "resource_name_1",
            "value-1", "resource_name_2", "value-2", "resource_name_3", "value-3", "resource_name_4", "value-4",
            "resource_name_5", "value-5",
        ]);
        let ms = |d: Duration| timestamp_unix_milli(new_timestamp_from_time(now + d));
        let _ = ts;
        require_equal(
            &[
                ti_sample(ms(Duration::ZERO), &target_info_labels),
                ti_sample(ms(lookback / 2), &target_info_labels),
                ti_sample(ms(lookback), &target_info_labels),
                ti_sample(ms(lookback + lookback / 4), &target_info_labels),
            ],
            &got[got.len() - 4..],
        );
    }

    // target_info deduplication across multiple resources with the same labels
    {
        let mut request = Metrics::new();
        let now = SystemTime::now();
        let ts = new_timestamp_from_time(now);
        for _ in 0..2 {
            let rm = request.resource_metrics.append_empty();
            generate_attributes(&mut rm.resource.attributes, "resource", 3);
            for (k, v) in service_attrs {
                rm.resource.attributes.put_str(k, v);
            }
            let m = rm.scope_metrics.append_empty().metrics.append_empty();
            m.set_empty_gauge();
            m.name = "gauge-1".into();
            m.description = "gauge".into();
            m.unit = "unit".into();
            let p1 = m.gauge_mut().data_points.append_empty();
            p1.timestamp = ts;
            p1.set_double_value(1.23);
            generate_attributes(&mut p1.attributes, "series", 1);
            let p2 = m.gauge_mut().data_points.append_empty();
            p2.timestamp = new_timestamp_from_time(now + lookback / 2);
            p2.set_double_value(2.34);
            generate_attributes(&mut p2.attributes, "series", 1);
        }
        let (app_test, mut converter) = new_converter();
        let (annots, err) = converter.from_metrics(
            &Context::background(),
            &request,
            &Settings { lookback_delta: lookback, ..Default::default() },
        );
        assert!(err.is_none());
        assert!(annots.is_empty());
        commit(&mut converter);
        let ti: Vec<Sample> =
            app_test.result_samples().into_iter().filter(|s| s.l.get(METRIC_NAME) == "target_info").collect();
        // Two samples (at ts and ts + lookback/2), not four.
        assert_eq!(2, ti.len());
        let target_info_labels = ls(&[
            "__name__", "target_info", "instance", "id1234", "job", "test-namespace/test-service", "resource_name_1",
            "value-1", "resource_name_2", "value-2", "resource_name_3", "value-3",
        ]);
        require_equal(
            &[
                ti_sample(timestamp_unix_milli(ts), &target_info_labels),
                ti_sample(timestamp_unix_milli(new_timestamp_from_time(now + lookback / 2)), &target_info_labels),
            ],
            &ti,
        );
    }

    // target_info does not include scope labels when PromoteScopeMetadata is enabled
    {
        let mut request = Metrics::new();
        let rm = request.resource_metrics.append_empty();
        rm.resource.attributes.put_str("service.name", "test-service");
        rm.resource.attributes.put_str("service.instance.id", "instance-1");
        generate_attributes(&mut rm.resource.attributes, "resource", 2);
        let sm = rm.scope_metrics.append_empty();
        sm.scope.name = "my-scope".into();
        sm.scope.version = "1.0.0".into();
        sm.scope.attributes.put_str("scope-attr", "scope-value");
        let m = sm.metrics.append_empty();
        m.set_empty_gauge();
        m.name = "test_gauge".into();
        m.description = "test gauge".into();
        let p = m.gauge_mut().data_points.append_empty();
        p.timestamp = new_timestamp_from_time(SystemTime::now());
        p.set_double_value(1.0);
        let (app_test, mut converter) = new_converter();
        let (annots, err) = converter.from_metrics(
            &Context::background(),
            &request,
            &Settings { promote_scope_metadata: true, lookback_delta: lookback, ..Default::default() },
        );
        assert!(err.is_none());
        assert!(annots.is_empty());
        commit(&mut converter);
        let res = app_test.result_samples();
        let ti: Vec<&Sample> = res.iter().filter(|s| s.l.get(METRIC_NAME) == "target_info").collect();
        assert!(!ti.is_empty(), "expected target_info samples");
        for s in ti {
            assert!(s.l.get("otel_scope_name").is_empty());
            assert!(s.l.get("otel_scope_version").is_empty());
            assert!(s.l.get("otel_scope_schema_url").is_empty());
            assert!(s.l.get("otel_scope_scope_attr").is_empty());
        }
        let ms: Vec<&Sample> = res.iter().filter(|s| s.l.get(METRIC_NAME) == "test_gauge").collect();
        assert!(!ms.is_empty(), "expected metric samples");
        assert_eq!("my-scope", ms[0].l.get("otel_scope_name"));
        assert_eq!("1.0.0", ms[0].l.get("otel_scope_version"));
    }

    // target_info includes promoted resource attributes, also with KeepIdentifyingResourceAttributes
    for keep in [false, true] {
        let mut request = Metrics::new();
        let rm = request.resource_metrics.append_empty();
        rm.resource.attributes.put_str("service.name", "test-service");
        if keep {
            rm.resource.attributes.put_str("service.namespace", "test-namespace");
        }
        rm.resource.attributes.put_str("service.instance.id", "instance-1");
        rm.resource.attributes.put_str("custom.promoted.attr", "promoted-value");
        rm.resource.attributes.put_str("another.resource.attr", "another-value");
        let m = rm.scope_metrics.append_empty().metrics.append_empty();
        m.set_empty_gauge();
        m.name = "test_gauge".into();
        m.description = "test gauge".into();
        let p = m.gauge_mut().data_points.append_empty();
        p.timestamp = new_timestamp_from_time(SystemTime::now());
        p.set_double_value(1.0);
        let (app_test, mut converter) = new_converter();
        let settings = Settings {
            promote_resource_attributes: Some(new_promote_resource_attributes(&config::OtlpConfig {
                promote_resource_attributes: vec!["custom.promoted.attr".into()],
                ..Default::default()
            })),
            keep_identifying_resource_attributes: keep,
            lookback_delta: lookback,
            ..Default::default()
        };
        let (annots, err) = converter.from_metrics(&Context::background(), &request, &settings);
        assert!(err.is_none());
        assert!(annots.is_empty());
        commit(&mut converter);
        let res = app_test.result_samples();
        let ti: Vec<&Sample> = res.iter().filter(|s| s.l.get(METRIC_NAME) == "target_info").collect();
        assert!(!ti.is_empty(), "expected target_info samples");
        for s in ti {
            assert_eq!("promoted-value", s.l.get("custom_promoted_attr"));
            if keep {
                assert_eq!("test-service", s.l.get("service_name"));
                assert_eq!("test-namespace", s.l.get("service_namespace"));
                assert_eq!("instance-1", s.l.get("service_instance_id"));
            }
            assert_eq!("another-value", s.l.get("another_resource_attr"));
        }
        let ms: Vec<&Sample> = res.iter().filter(|s| s.l.get(METRIC_NAME) == "test_gauge").collect();
        assert!(!ms.is_empty(), "expected metric samples");
        assert_eq!("promoted-value", ms[0].l.get("custom_promoted_attr"));
    }
}

// TestTemporality helpers.

fn unix_ts(t: SystemTime) -> u64 {
    new_timestamp_from_time(t)
}

fn millis(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

fn test_labels(name: &str) -> Labels {
    ls(&["__name__", name, "test_label", "test_value"])
}

fn create_otel_sum(name: &str, temporality: AggregationTemporality, ts: SystemTime) -> Metric {
    let mut m = Metric::new();
    m.name = name.into();
    let sum = m.set_empty_sum();
    sum.aggregation_temporality = temporality;
    sum.is_monotonic = true;
    let dp = sum.data_points.append_empty();
    dp.set_double_value(5.0);
    dp.timestamp = unix_ts(ts);
    dp.attributes.put_str("test_label", "test_value");
    m
}

fn create_prom_float_series(name: &str, ts: SystemTime, typ: MetricType) -> Sample {
    float_sample(name, test_labels(name), millis(ts), typ, 5.0)
}

fn create_otel_gauge(name: &str, ts: SystemTime) -> Metric {
    let mut m = Metric::new();
    m.name = name.into();
    let dp = m.set_empty_gauge().data_points.append_empty();
    dp.set_double_value(5.0);
    dp.timestamp = unix_ts(ts);
    dp.attributes.put_str("test_label", "test_value");
    m
}

fn create_otel_exponential_histogram(name: &str, temporality: AggregationTemporality, ts: SystemTime) -> Metric {
    let mut m = Metric::new();
    m.name = name.into();
    let hist = m.set_empty_exponential_histogram();
    hist.aggregation_temporality = temporality;
    let dp = hist.data_points.append_empty();
    dp.count = 1;
    dp.set_sum(5.0);
    dp.positive.bucket_counts = vec![1];
    dp.timestamp = unix_ts(ts);
    dp.attributes.put_str("test_label", "test_value");
    m
}

fn create_prom_native_histogram_series(name: &str, hint: CounterResetHint, ts: SystemTime, typ: MetricType) -> Sample {
    hist_sample(
        name,
        test_labels(name),
        millis(ts),
        typ,
        Histogram {
            count: 1,
            sum: 5.0,
            schema: 0,
            zero_threshold: 1e-128,
            zero_count: 0,
            positive_spans: vec![Span { offset: 1, length: 1 }],
            positive_buckets: vec![1],
            counter_reset_hint: hint,
            ..Default::default()
        },
    )
}

fn create_otel_explicit_histogram(name: &str, temporality: AggregationTemporality, ts: SystemTime) -> Metric {
    let mut m = Metric::new();
    m.name = name.into();
    let hist = m.set_empty_histogram();
    hist.aggregation_temporality = temporality;
    let dp = hist.data_points.append_empty();
    dp.count = 20;
    dp.set_sum(30.0);
    dp.bucket_counts = vec![10, 10, 0];
    dp.explicit_bounds = vec![1.0, 2.0];
    dp.timestamp = unix_ts(ts);
    dp.attributes.put_str("test_label", "test_value");
    m
}

fn create_prom_nhcb_series(name: &str, hint: CounterResetHint, ts: SystemTime, typ: MetricType) -> Sample {
    hist_sample(
        name,
        test_labels(name),
        millis(ts),
        typ,
        Histogram {
            count: 20,
            sum: 30.0,
            schema: -53,
            zero_threshold: 0.0,
            positive_spans: vec![Span { offset: 0, length: 3 }],
            positive_buckets: vec![10, 0, -10],
            custom_values: custom_values(&[1.0, 2.0]),
            counter_reset_hint: hint,
            ..Default::default()
        },
    )
}

fn create_prom_classic_histogram_series(name: &str, ts: SystemTime, typ: MetricType) -> Vec<Sample> {
    let t = millis(ts);
    let b = |le: &str| ls(&["__name__", &format!("{name}_bucket"), "le", le, "test_label", "test_value"]);
    vec![
        float_sample(name, ls(&["__name__", &format!("{name}_sum"), "test_label", "test_value"]), t, typ, 30.0),
        float_sample(name, ls(&["__name__", &format!("{name}_count"), "test_label", "test_value"]), t, typ, 20.0),
        float_sample(name, b("1"), t, typ, 10.0),
        float_sample(name, b("2"), t, typ, 20.0),
        float_sample(name, b("+Inf"), t, typ, 20.0),
    ]
}

fn create_otel_summary(name: &str, ts: SystemTime) -> Metric {
    let mut m = Metric::new();
    m.name = name.into();
    let dp = m.set_empty_summary().data_points.append_empty();
    dp.count = 9;
    dp.sum = 18.0;
    let qv = dp.quantile_values.append_empty();
    qv.quantile = 0.5;
    qv.value = 2.0;
    dp.timestamp = unix_ts(ts);
    dp.attributes.put_str("test_label", "test_value");
    m
}

fn create_prom_summary_series(name: &str, ts: SystemTime) -> Vec<Sample> {
    let t = millis(ts);
    let st = MetricType::Summary;
    vec![
        float_sample(name, ls(&["__name__", &format!("{name}_sum"), "test_label", "test_value"]), t, st, 18.0),
        float_sample(name, ls(&["__name__", &format!("{name}_count"), "test_label", "test_value"]), t, st, 9.0),
        float_sample(name, ls(&["__name__", name, "quantile", "0.5", "test_label", "test_value"]), t, st, 2.0),
    ]
}

fn create_otel_empty_type(name: &str) -> Metric {
    let mut m = Metric::new();
    m.name = name.into();
    m
}

#[test]
fn test_temporality() {
    let ts = UNIX_EPOCH + Duration::from_secs(100);
    let cum = AggregationTemporality::CUMULATIVE;
    let delta = AggregationTemporality::DELTA;
    let unknown_hint = CounterResetHint::UnknownCounterReset;
    let gauge_hint = CounterResetHint::GaugeType;
    struct Case {
        name: &'static str,
        allow_delta: bool,
        convert_to_nhcb: bool,
        input_series: Vec<Metric>,
        expected_samples: Vec<Sample>,
        expected_error: &'static str,
    }
    let c = |name, allow_delta, convert_to_nhcb, input_series, expected_samples, expected_error| Case {
        name,
        allow_delta,
        convert_to_nhcb,
        input_series,
        expected_samples,
        expected_error,
    };
    let tests = vec![
        c(
            "all cumulative when delta not allowed",
            false,
            false,
            vec![create_otel_sum("test_metric_1", cum, ts), create_otel_sum("test_metric_2", cum, ts)],
            vec![
                create_prom_float_series("test_metric_1", ts, MetricType::Counter),
                create_prom_float_series("test_metric_2", ts, MetricType::Counter),
            ],
            "",
        ),
        c(
            "all delta when allowed",
            true,
            false,
            vec![create_otel_sum("test_metric_1", delta, ts), create_otel_sum("test_metric_2", delta, ts)],
            vec![
                create_prom_float_series("test_metric_1", ts, MetricType::Unknown),
                create_prom_float_series("test_metric_2", ts, MetricType::Unknown),
            ],
            "",
        ),
        c(
            "mixed temporality when delta allowed",
            true,
            false,
            vec![create_otel_sum("test_metric_1", delta, ts), create_otel_sum("test_metric_2", cum, ts)],
            vec![
                create_prom_float_series("test_metric_1", ts, MetricType::Unknown),
                create_prom_float_series("test_metric_2", ts, MetricType::Counter),
            ],
            "",
        ),
        c(
            "delta rejected when not allowed",
            false,
            false,
            vec![create_otel_sum("test_metric_1", cum, ts), create_otel_sum("test_metric_2", delta, ts)],
            vec![create_prom_float_series("test_metric_1", ts, MetricType::Counter)],
            r#"invalid temporality and type combination for metric "test_metric_2""#,
        ),
        c(
            "unspecified temporality not allowed",
            true,
            false,
            vec![
                create_otel_sum("test_metric_1", cum, ts),
                create_otel_sum("test_metric_2", AggregationTemporality::UNSPECIFIED, ts),
            ],
            vec![create_prom_float_series("test_metric_1", ts, MetricType::Counter)],
            r#"invalid temporality and type combination for metric "test_metric_2""#,
        ),
        c(
            "cumulative histogram",
            false,
            false,
            vec![create_otel_exponential_histogram("test_histogram", cum, ts)],
            vec![create_prom_native_histogram_series("test_histogram", unknown_hint, ts, MetricType::Histogram)],
            "",
        ),
        c(
            "delta histogram when allowed",
            true,
            false,
            vec![
                create_otel_exponential_histogram("test_histogram_1", delta, ts),
                create_otel_exponential_histogram("test_histogram_2", cum, ts),
            ],
            vec![
                create_prom_native_histogram_series("test_histogram_1", gauge_hint, ts, MetricType::Unknown),
                create_prom_native_histogram_series("test_histogram_2", unknown_hint, ts, MetricType::Histogram),
            ],
            "",
        ),
        c(
            "delta histogram when not allowed",
            false,
            false,
            vec![
                create_otel_exponential_histogram("test_histogram_1", delta, ts),
                create_otel_exponential_histogram("test_histogram_2", cum, ts),
            ],
            vec![create_prom_native_histogram_series("test_histogram_2", unknown_hint, ts, MetricType::Histogram)],
            r#"invalid temporality and type combination for metric "test_histogram_1""#,
        ),
        c(
            "cumulative histogram with buckets",
            false,
            true,
            vec![create_otel_explicit_histogram("test_histogram", cum, ts)],
            vec![create_prom_nhcb_series("test_histogram", unknown_hint, ts, MetricType::Histogram)],
            "",
        ),
        c(
            "delta histogram with buckets when allowed",
            true,
            true,
            vec![
                create_otel_explicit_histogram("test_histogram_1", delta, ts),
                create_otel_explicit_histogram("test_histogram_2", cum, ts),
            ],
            vec![
                create_prom_nhcb_series("test_histogram_1", gauge_hint, ts, MetricType::Unknown),
                create_prom_nhcb_series("test_histogram_2", unknown_hint, ts, MetricType::Histogram),
            ],
            "",
        ),
        c(
            "delta histogram with buckets when not allowed",
            false,
            true,
            vec![
                create_otel_explicit_histogram("test_histogram_1", delta, ts),
                create_otel_explicit_histogram("test_histogram_2", cum, ts),
            ],
            vec![create_prom_nhcb_series("test_histogram_2", unknown_hint, ts, MetricType::Histogram)],
            r#"invalid temporality and type combination for metric "test_histogram_1""#,
        ),
        c(
            "delta histogram with buckets and convertToNHCB=false when not allowed",
            false,
            false,
            vec![
                create_otel_explicit_histogram("test_histogram_1", delta, ts),
                create_otel_explicit_histogram("test_histogram_2", cum, ts),
            ],
            create_prom_classic_histogram_series("test_histogram_2", ts, MetricType::Histogram),
            r#"invalid temporality and type combination for metric "test_histogram_1""#,
        ),
        c(
            "delta histogram with buckets and convertToNHCB=false when allowed",
            true,
            false,
            vec![
                create_otel_explicit_histogram("test_histogram_1", delta, ts),
                create_otel_explicit_histogram("test_histogram_2", cum, ts),
            ],
            {
                let mut v = create_prom_classic_histogram_series("test_histogram_1", ts, MetricType::Unknown);
                v.extend(create_prom_classic_histogram_series("test_histogram_2", ts, MetricType::Histogram));
                v
            },
            "",
        ),
        c(
            "summary does not have temporality",
            false,
            false,
            vec![create_otel_summary("test_summary_1", ts)],
            create_prom_summary_series("test_summary_1", ts),
            "",
        ),
        c(
            "gauge does not have temporality",
            false,
            false,
            vec![create_otel_gauge("test_gauge_1", ts)],
            vec![create_prom_float_series("test_gauge_1", ts, MetricType::Gauge)],
            "",
        ),
        c(
            "empty metric type errors",
            false,
            false,
            vec![create_otel_empty_type("test_empty")],
            vec![],
            "could not get aggregation temporality for test_empty as it has unsupported metric type Empty",
        ),
    ];
    for tc in tests {
        let mut metrics = Metrics::new();
        let sm = metrics.resource_metrics.append_empty().scope_metrics.append_empty();
        for s in &tc.input_series {
            sm.metrics.push(s.clone());
        }
        let (app_test, mut converter) = new_converter();
        let settings = Settings {
            allow_delta_temporality: tc.allow_delta,
            convert_histograms_to_nhcb: tc.convert_to_nhcb,
            ..Default::default()
        };
        let (_, err) = converter.from_metrics(&Context::background(), &metrics, &settings);
        if !tc.expected_error.is_empty() {
            assert_eq!(tc.expected_error, err.map(|e| e.to_string()).unwrap_or_default(), "{}", tc.name);
        } else {
            assert!(err.is_none(), "{}: {err:?}", tc.name);
        }
        commit(&mut converter);
        for es in &tc.expected_samples {
            if let Some(h) = &es.h {
                assert!(h.validate().is_ok(), "expected histogram for {} does not validate", es.l);
            }
        }
        require_equal(&tc.expected_samples, &app_test.result_samples());
    }
}

fn create_otel_metric_for_translator(name: &str, unit: &str, description: &str) -> Metric {
    Metric { name: name.into(), unit: unit.into(), description: description.into(), ..Default::default() }
}

#[test]
fn test_translator_metric_from_otel_metric() {
    let gauge = |n, u, d| {
        let mut m = create_otel_metric_for_translator(n, u, d);
        m.set_empty_gauge();
        m
    };
    let sum = |n, u, d, mono| {
        let mut m = create_otel_metric_for_translator(n, u, d);
        m.set_empty_sum().is_monotonic = mono;
        m
    };
    let hist = {
        let mut m = create_otel_metric_for_translator("test_histogram", "seconds", "Test histogram metric");
        m.set_empty_histogram();
        m
    };
    let exp = {
        let mut m = create_otel_metric_for_translator("test_exp_histogram", "milliseconds", "Test exponential histogram metric");
        m.set_empty_exponential_histogram();
        m
    };
    let summary = {
        let mut m = create_otel_metric_for_translator("test_summary", "duration", "Test summary metric");
        m.set_empty_summary();
        m
    };
    let empty = Metric { name: "test_empty".into(), ..Default::default() };
    let want = |n: &str, u: &str, t| namer::Metric { name: n.into(), unit: u.into(), typ: t };
    use namer::MetricType as T;
    let tests = vec![
        ("gauge metric", gauge("test_gauge", "bytes", "Test gauge metric"), want("test_gauge", "bytes", T::Gauge)),
        (
            "monotonic sum metric",
            sum("test_sum", "count", "Test sum metric", true),
            want("test_sum", "count", T::MonotonicCounter),
        ),
        (
            "non-monotonic sum metric",
            sum("test_sum", "count", "Test sum metric", false),
            want("test_sum", "count", T::NonMonotonicCounter),
        ),
        ("histogram metric", hist, want("test_histogram", "seconds", T::Histogram)),
        ("exponential histogram metric", exp, want("test_exp_histogram", "milliseconds", T::ExponentialHistogram)),
        ("summary metric", summary, want("test_summary", "duration", T::Summary)),
        ("empty metric name and unit", gauge("", "", ""), want("", "", T::Gauge)),
        ("empty metric type defaults to unknown", empty, want("test_empty", "", T::Unknown)),
    ];
    for (name, input, expected) in tests {
        assert_eq!(expected, translator_metric_from_otel_metric(&input), "{name}");
    }
}
