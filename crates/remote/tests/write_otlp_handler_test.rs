//! Port of `storage/remote/write_otlp_handler_test.go`. The benchmarks are not ported.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use discovery::log::Logger;
use discovery::metrics::Registry;
use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram, Span};
use model::labels::{self, Labels, METRIC_NAME};
use model::matcher::{MatchType, Matcher};
use model::metadata::{Metadata, MetricType};
use remote::otlptranslator::namer;
use remote::otlptranslator::pdata::{
    AggregationTemporality, AppendEmpty, Metrics, new_timestamp_from_time,
};
use remote::otlptranslator::pdata_proto::marshal_export_request;
use remote::write_otlp_handler::*;
use storage::{
    AppendV2Options, AppendableV2, AppenderTransaction, AppenderV2, Context, ERR_EXEMPLAR_LABEL_LENGTH,
    ERR_OUT_OF_ORDER_EXEMPLAR, Error, SeriesRef,
};
use util::teststorage::{Appendable, Sample, require_equal};

fn ls(pairs: &[&str]) -> Labels {
    labels::from_strings(pairs)
}

fn ms(t: SystemTime) -> i64 {
    model::timestamp::from_time(t)
}

fn cfg_fn(c: config::OtlpConfig) -> OtlpConfigFn {
    Arc::new(move || c.clone())
}

fn proto_request(md: &Metrics) -> http::Request {
    http::Request {
        method: "POST".into(),
        headers: vec![("content-type".into(), PB_CONTENT_TYPE.into())],
        body: marshal_export_request(md),
        ..Default::default()
    }
}

/// Go `handleOTLP`.
fn handle_otlp(export_request: &Metrics, otlp_cfg: config::OtlpConfig, otlp_opts: OtlpOptions) -> Appendable {
    let appendable = Appendable::new();
    let handler = new_otlp_write_handler(Logger::nop(), None, Arc::new(appendable.clone()), cfg_fn(otlp_cfg), otlp_opts);
    let resp = handler.serve_http(&proto_request(export_request));
    assert_eq!(200, resp.status, "{:?}", String::from_utf8_lossy(resp.body_bytes()));
    appendable
}

/// Go `generateOTLPWriteRequest`: a counter, a gauge, a histogram and an exponential histogram with resource
/// attributes service.name, service.instance.id and host.name and the metric attribute foo.bar="baz".
fn generate_otlp_write_request(timestamp: SystemTime, start_time: SystemTime) -> Metrics {
    let ts = new_timestamp_from_time(timestamp);
    let st = new_timestamp_from_time(start_time);
    let mut d = Metrics::new();
    let rm = d.resource_metrics.append_empty();
    rm.resource.attributes.put_str("service.name", "test-service");
    rm.resource.attributes.put_str("service.instance.id", "test-instance");
    rm.resource.attributes.put_str("host.name", "test-host");
    let sm = rm.scope_metrics.append_empty();

    let m = sm.metrics.append_empty();
    m.name = "test.counter".into();
    m.description = "test-counter-description".into();
    m.unit = "By".into();
    let s = m.set_empty_sum();
    s.aggregation_temporality = AggregationTemporality::CUMULATIVE;
    s.is_monotonic = true;
    let dp = s.data_points.append_empty();
    dp.timestamp = ts;
    dp.start_timestamp = st;
    dp.set_double_value(10.0);
    dp.attributes.put_str("foo.bar", "baz");
    let e = dp.exemplars.append_empty();
    e.timestamp = ts;
    e.set_double_value(10.0);
    e.span_id = [0, 1, 2, 3, 4, 5, 6, 7];
    e.trace_id = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

    let m = sm.metrics.append_empty();
    m.name = "test.gauge".into();
    m.description = "test-gauge-description".into();
    m.unit = "By".into();
    let dp = m.set_empty_gauge().data_points.append_empty();
    dp.timestamp = ts;
    dp.start_timestamp = st;
    dp.set_double_value(10.0);
    dp.attributes.put_str("foo.bar", "baz");

    let m = sm.metrics.append_empty();
    m.name = "test.histogram".into();
    m.description = "test-histogram-description".into();
    m.unit = "By".into();
    let h = m.set_empty_histogram();
    h.aggregation_temporality = AggregationTemporality::CUMULATIVE;
    let dp = h.data_points.append_empty();
    dp.timestamp = ts;
    dp.start_timestamp = st;
    dp.explicit_bounds = vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0];
    dp.bucket_counts = vec![2, 2, 2, 2, 2, 2];
    dp.count = 12;
    dp.set_sum(30.0);
    dp.attributes.put_str("foo.bar", "baz");

    let m = sm.metrics.append_empty();
    m.name = "test.exponential.histogram".into();
    m.description = "test-exponential-histogram-description".into();
    m.unit = "By".into();
    let h = m.set_empty_exponential_histogram();
    h.aggregation_temporality = AggregationTemporality::CUMULATIVE;
    let dp = h.data_points.append_empty();
    dp.timestamp = ts;
    dp.start_timestamp = st;
    dp.scale = 2;
    dp.positive.bucket_counts = vec![2, 2, 2, 2, 2];
    dp.zero_count = 2;
    dp.count = 10;
    dp.set_sum(30.0);
    dp.attributes.put_str("foo.bar", "baz");
    d
}

fn strategy(s: &str) -> config::OtlpConfig {
    config::OtlpConfig { translation_strategy: s.into(), ..Default::default() }
}

#[test]
fn test_otlp_write_handler() {
    let ts = SystemTime::now();
    let st = ts - Duration::from_millis(1);
    let (t, stt) = (ms(ts), ms(st));

    // Expected samples without the labels and family names, which depend on the translation options.
    let expected_samples_without_labels = || -> Vec<Sample> {
        let meta = |typ, help: &str| Metadata { typ, unit: "bytes".into(), help: help.into() };
        let hist = |v: f64| Sample {
            m: meta(MetricType::Histogram, "test-histogram-description"),
            v,
            st: stt,
            t,
            ..Default::default()
        };
        vec![
            Sample {
                m: meta(MetricType::Counter, "test-counter-description"),
                v: 10.0,
                st: stt,
                t,
                es: vec![Exemplar {
                    labels: ls(&["span_id", "0001020304050607", "trace_id", "000102030405060708090a0b0c0d0e0f"]),
                    value: 10.0,
                    ts: t,
                    has_ts: true,
                }],
                ..Default::default()
            },
            Sample { m: meta(MetricType::Gauge, "test-gauge-description"), v: 10.0, st: stt, t, ..Default::default() },
            hist(30.0),
            hist(12.0),
            hist(2.0),
            hist(4.0),
            hist(6.0),
            hist(8.0),
            hist(10.0),
            hist(12.0),
            hist(12.0),
            Sample {
                m: meta(MetricType::Histogram, "test-exponential-histogram-description"),
                h: Some(Histogram {
                    count: 10,
                    sum: 30.0,
                    schema: 2,
                    zero_threshold: 1e-128,
                    zero_count: 2,
                    positive_spans: vec![Span { offset: 1, length: 5 }],
                    positive_buckets: vec![2, 0, 0, 0, 0],
                    ..Default::default()
                }),
                st: stt,
                t,
                ..Default::default()
            },
            Sample {
                m: Metadata { typ: MetricType::Gauge, unit: String::new(), help: "Target metadata".into() },
                v: 1.0,
                t,
                ..Default::default()
            },
        ]
    };

    // (metric family, labels) per expected sample, built from the name parts of a case.
    struct Names {
        counter: &'static str,
        gauge: &'static str,
        hist: &'static str,
        exp: &'static str,
        attr: &'static str,
        host: &'static str,
    }
    let build = |n: &Names, type_unit: bool| -> Vec<(String, Labels)> {
        let tu = |typ: &str| -> Vec<String> {
            if type_unit { vec!["__type__".into(), typ.into(), "__unit__".into(), "bytes".into()] } else { vec![] }
        };
        let mk = |name: &str, typ: &str, extra: &[&str]| -> Labels {
            let mut v: Vec<String> = vec![METRIC_NAME.into(), name.into()];
            v.extend(tu(typ));
            for s in [n.attr, "baz", "instance", "test-instance", "job", "test-service"] {
                v.push(s.into());
            }
            v.extend(extra.iter().map(|s| s.to_string()));
            let r: Vec<&str> = v.iter().map(|s| s.as_str()).collect();
            ls(&r)
        };
        let mut out = vec![
            (n.counter.to_string(), mk(n.counter, "counter", &[])),
            (n.gauge.to_string(), mk(n.gauge, "gauge", &[])),
            (n.hist.to_string(), mk(&format!("{}_sum", n.hist), "histogram", &[])),
            (n.hist.to_string(), mk(&format!("{}_count", n.hist), "histogram", &[])),
        ];
        for le in ["0", "1", "2", "3", "4", "5", "+Inf"] {
            out.push((n.hist.to_string(), mk(&format!("{}_bucket", n.hist), "histogram", &["le", le])));
        }
        out.push((n.exp.to_string(), mk(n.exp, "histogram", &[])));
        out.push((
            "target_info".into(),
            ls(&[METRIC_NAME, "target_info", n.host, "test-host", "instance", "test-instance", "job", "test-service"]),
        ));
        out
    };
    let utf8_names = Names {
        counter: "test.counter",
        gauge: "test.gauge",
        hist: "test.histogram",
        exp: "test.exponential.histogram",
        attr: "foo.bar",
        host: "host.name",
    };
    let cases: Vec<(&str, config::OtlpConfig, bool, Vec<(String, Labels)>)> = vec![
        ("NoTranslation/NoTypeAndUnitLabels", strategy(namer::NO_TRANSLATION), false, build(&utf8_names, false)),
        ("NoTranslation/WithTypeAndUnitLabels", strategy(namer::NO_TRANSLATION), true, build(&utf8_names, true)),
        (
            "UnderscoreEscapingWithSuffixes",
            strategy(namer::UNDERSCORE_ESCAPING_WITH_SUFFIXES),
            false,
            build(
                &Names {
                    counter: "test_counter_bytes_total",
                    gauge: "test_gauge_bytes",
                    hist: "test_histogram_bytes",
                    exp: "test_exponential_histogram_bytes",
                    attr: "foo_bar",
                    host: "host_name",
                },
                false,
            ),
        ),
        (
            "UnderscoreEscapingWithoutSuffixes",
            strategy(namer::UNDERSCORE_ESCAPING_WITHOUT_SUFFIXES),
            false,
            build(
                &Names {
                    counter: "test_counter",
                    gauge: "test_gauge",
                    hist: "test_histogram",
                    exp: "test_exponential_histogram",
                    attr: "foo_bar",
                    host: "host_name",
                },
                false,
            ),
        ),
        (
            "NoUTF8EscapingWithSuffixes",
            strategy(namer::NO_UTF8_ESCAPING_WITH_SUFFIXES),
            false,
            build(
                &Names {
                    counter: "test.counter_bytes_total",
                    gauge: "test.gauge_bytes",
                    hist: "test.histogram_bytes",
                    exp: "test.exponential.histogram_bytes",
                    attr: "foo.bar",
                    host: "host.name",
                },
                false,
            ),
        ),
    ];
    let export_request = generate_otlp_write_request(ts, st);
    for (name, otlp_cfg, type_and_unit, expected_labels_and_mfs) in cases {
        let appendable = handle_otlp(
            &export_request,
            otlp_cfg,
            OtlpOptions { enable_type_and_unit_labels: type_and_unit, ..Default::default() },
        );
        let mut expected = expected_samples_without_labels();
        for (i, (mf, l)) in expected_labels_and_mfs.into_iter().enumerate() {
            expected[i].l = l;
            expected[i].mf = mf;
        }
        let got = appendable.result_samples();
        assert_eq!(expected.len(), got.len(), "{name}");
        require_equal(&expected, &got);
    }

    // translation warnings metric
    {
        let new_handler = || {
            new_otlp_write_handler(
                Logger::nop(),
                Some(&Registry::new()),
                Arc::new(Appendable::new()),
                cfg_fn(strategy(namer::UNDERSCORE_ESCAPING_WITH_SUFFIXES)),
                OtlpOptions::default(),
            )
        };
        let count = |h: &OtlpWriteHandler, c: &str| h.default_consumer.translation_warnings.with_label_values(&[c]).get();
        let now = new_timestamp_from_time(SystemTime::now());

        // label name collision: `a.b` and `a_b` both map to `a_b` (escaping is on).
        let mut request = Metrics::new();
        let m = request.resource_metrics.append_empty().scope_metrics.append_empty().metrics.append_empty();
        m.name = "test_gauge".into();
        let dp = m.set_empty_gauge().data_points.append_empty();
        dp.timestamp = now;
        dp.set_int_value(1);
        dp.attributes.put_str("a.b", "x");
        dp.attributes.put_str("a_b", "y");
        let ex = new_handler();
        assert_eq!(0.0, count(&ex, "label_name_collision"));
        ex.default_consumer.consume_metrics(&Context::background(), &request).unwrap();
        assert_eq!(1.0, count(&ex, "label_name_collision"));

        // histogram zero count non-zero sum
        let mut request = Metrics::new();
        let m = request.resource_metrics.append_empty().scope_metrics.append_empty().metrics.append_empty();
        m.name = "test_exponential_histogram".into();
        m.set_empty_exponential_histogram().aggregation_temporality = AggregationTemporality::CUMULATIVE;
        let h = m.exponential_histogram_mut().data_points.append_empty();
        h.timestamp = now;
        h.count = 0;
        h.set_sum(155.0);
        let ex = new_handler();
        assert_eq!(0.0, count(&ex, "histogram_zero_count_non_zero_sum"));
        ex.default_consumer.consume_metrics(&Context::background(), &request).unwrap();
        assert_eq!(1.0, count(&ex, "histogram_zero_count_non_zero_sum"));

        // empty data points
        let mut request = Metrics::new();
        let m = request.resource_metrics.append_empty().scope_metrics.append_empty().metrics.append_empty();
        m.name = "test_empty_gauge".into();
        m.set_empty_gauge();
        let ex = new_handler();
        assert_eq!(0.0, count(&ex, "empty_data_points"));
        ex.default_consumer.consume_metrics(&Context::background(), &request).unwrap();
        assert_eq!(1.0, count(&ex, "empty_data_points"));
    }

    // empty metric does not reject healthy metrics
    {
        let mut request = Metrics::new();
        let metrics = &mut request.resource_metrics.append_empty().scope_metrics.append_empty().metrics;
        let healthy = metrics.append_empty();
        healthy.name = "healthy_metric".into();
        let dp = healthy.set_empty_gauge().data_points.append_empty();
        dp.timestamp = new_timestamp_from_time(ts);
        dp.set_int_value(1);
        let empty = metrics.append_empty();
        empty.name = "empty_metric".into();
        empty.set_empty_gauge();
        let appendable = handle_otlp(&request, config::OtlpConfig::default(), OtlpOptions::default());
        let samples = appendable.result_samples();
        assert_eq!(1, samples.len());
        assert_eq!("healthy_metric", samples[0].l.get(METRIC_NAME));
    }

    // future exemplar is dropped without failing the request
    for ex_offset in [Duration::ZERO, Duration::from_secs(3600)] {
        let now = SystemTime::now();
        let mut request = Metrics::new();
        let m = request.resource_metrics.append_empty().scope_metrics.append_empty().metrics.append_empty();
        m.name = "test_histogram".into();
        let eh = m.set_empty_exponential_histogram();
        eh.aggregation_temporality = AggregationTemporality::CUMULATIVE;
        let dp = eh.data_points.append_empty();
        dp.timestamp = new_timestamp_from_time(now);
        dp.count = 1;
        dp.set_sum(1.0);
        dp.scale = 0;
        dp.positive.bucket_counts = vec![1];
        let ex = dp.exemplars.append_empty();
        ex.timestamp = new_timestamp_from_time(now + ex_offset);
        ex.set_double_value(1.0);
        ex.trace_id = [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let appendable = handle_otlp(&request, config::OtlpConfig::default(), OtlpOptions::default());
        let samples = appendable.result_samples();
        assert_eq!(1, samples.len(), "histogram sample should be stored");
        if ex_offset.is_zero() {
            assert_eq!(1, samples[0].es.len());
        } else {
            assert!(samples[0].es.is_empty());
        }
    }

    // exemplars out of timestamp order are all stored (real TSDB)
    {
        let dir = temp_dir("exemplars-order");
        let mut opts = tsdb::db::default_options();
        opts.enable_exemplar_storage = true;
        opts.max_exemplars = 10;
        let db = tsdb::db::open(&dir, None, Some(opts), None).unwrap();

        let now = SystemTime::now();
        let mut request = Metrics::new();
        let m = request.resource_metrics.append_empty().scope_metrics.append_empty().metrics.append_empty();
        m.name = "test_counter".into();
        let sum = m.set_empty_sum();
        sum.aggregation_temporality = AggregationTemporality::CUMULATIVE;
        sum.is_monotonic = true;
        let dp = sum.data_points.append_empty();
        dp.timestamp = new_timestamp_from_time(now);
        dp.set_double_value(1.0);
        // The newer exemplar comes first.
        for (i, offset) in [0i64, -1].into_iter().enumerate() {
            let ex = dp.exemplars.append_empty();
            let t = if offset < 0 { now - Duration::from_secs(1) } else { now };
            ex.timestamp = new_timestamp_from_time(t);
            ex.set_double_value(i as f64);
            let mut tid = [0u8; 16];
            tid[0] = i as u8 + 1;
            ex.trace_id = tid;
        }
        let handler = new_otlp_write_handler(
            Logger::nop(),
            None,
            db.clone(),
            cfg_fn(config::OtlpConfig::default()),
            OtlpOptions::default(),
        );
        let resp = handler.serve_http(&proto_request(&request));
        assert_eq!(200, resp.status);

        let q = db.exemplar_querier(&Context::background()).unwrap();
        let res = q
            .select(i64::MIN, i64::MAX, &[vec![Matcher::must_new(MatchType::Regexp, METRIC_NAME, ".+")]])
            .unwrap();
        assert_eq!(1, res.len());
        assert_eq!(2, res[0].exemplars.len());
        drop(handler);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // concurrent requests
    {
        let (handler, payload) = new_otlp_write_handler_fixture();
        let handler = Arc::new(handler);
        let threads: Vec<_> = (0..64)
            .map(|_| {
                let h = handler.clone();
                let p = payload.clone();
                std::thread::spawn(move || h.serve_http(&new_otlp_write_handler_request(p)).status)
            })
            .collect();
        for t in threads {
            assert_eq!(200, t.join().unwrap());
        }
    }
}

/// A temporary directory under bench/tmp/w3-remote/otlp/.
fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let d = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../bench/tmp/w3-remote/otlp")
        .join(format!("{name}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Go `discardAppendable`.
struct DiscardAppendable;
struct DiscardAppender;

impl AppendableV2 for DiscardAppendable {
    fn appender_v2(&self, _: &Context) -> Box<dyn AppenderV2> {
        Box::new(DiscardAppender)
    }
}

impl AppenderTransaction for DiscardAppender {
    fn commit(&mut self) -> Option<Error> {
        None
    }
    fn rollback(&mut self) -> Option<Error> {
        None
    }
}

impl AppenderV2 for DiscardAppender {
    fn append(
        &mut self,
        _: SeriesRef,
        _: &Labels,
        _: i64,
        _: i64,
        _: f64,
        _: Option<&Histogram>,
        _: Option<&FloatHistogram>,
        _: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        Ok(0)
    }
}

/// Go `newOTLPWriteHandlerFixture`.
fn new_otlp_write_handler_fixture() -> (OtlpWriteHandler, Vec<u8>) {
    let request = generate_otlp_write_request(UNIX_EPOCH, UNIX_EPOCH);
    let payload = marshal_export_request(&request);
    let handler = new_otlp_write_handler(
        Logger::nop(),
        None,
        Arc::new(DiscardAppendable),
        cfg_fn(config::default_otlp_config()),
        OtlpOptions::default(),
    );
    (handler, payload)
}

/// Go `newOTLPWriteHandlerRequest`.
fn new_otlp_write_handler_request(payload: Vec<u8>) -> http::Request {
    http::Request {
        method: "POST".into(),
        path: "/".into(),
        headers: vec![("content-type".into(), PB_CONTENT_TYPE.into())],
        body: payload,
        ..Default::default()
    }
}

#[test]
fn test_otlp_delta() {
    let appendable = Appendable::new();
    let handler = new_otlp_write_handler(
        Logger::nop(),
        None,
        Arc::new(appendable.clone()),
        cfg_fn(config::default_otlp_config()),
        OtlpOptions { convert_delta: true, ..Default::default() },
    );
    let mut md = Metrics::new();
    let m = md.resource_metrics.append_empty().scope_metrics.append_empty().metrics.append_empty();
    m.name = "some.delta.total".into();
    let sum = m.set_empty_sum();
    sum.aggregation_temporality = AggregationTemporality::DELTA;
    // 2000-01-02T03:04:00Z
    let base = UNIX_EPOCH + Duration::from_secs(946_782_240);
    for i in 0..3u64 {
        let dp = sum.data_points.append_empty();
        dp.set_int_value(i as i64);
        dp.timestamp = new_timestamp_from_time(base + Duration::from_secs(i));
    }
    let resp = handler.serve_http(&proto_request(&md));
    assert_eq!(200, resp.status);

    let l = ls(&["__name__", "some_delta_total"]);
    let milli = |sec: u64| ms(base + Duration::from_secs(sec));
    let gauge = Metadata { typ: MetricType::Gauge, ..Default::default() };
    let s = |t: i64, v: f64| Sample { mf: "some_delta_total".into(), m: gauge.clone(), t, l: l.clone(), v, ..Default::default() };
    let want = vec![s(milli(0), 0.0), s(milli(1), 1.0), s(milli(2), 3.0)];
    require_equal(&want, &appendable.result_samples());
}

#[test]
fn test_otlp_instrumented_appendable() {
    let app_with = |a: &Appendable| -> Arc<dyn AppendableV2> { Arc::new(a.clone()) };
    let ooo = |oa: &OtlpInstrumentedAppendable| oa.out_of_order_exemplars.get().get();
    let no_meta = |oa: &OtlpInstrumentedAppendable| oa.samples_appended_without_metadata.get().get();
    let help = Metadata { help: "yo".into(), ..Default::default() };

    // no problems
    {
        let app_test = Appendable::new();
        let oa = new_otlp_instrumented_appendable(Some(&Registry::new()), app_with(&app_test));
        assert_eq!(0.0, ooo(&oa));
        assert_eq!(0.0, no_meta(&oa));
        let mut app = oa.appender_v2(&Context::background());
        app.append(0, &Labels::default(), -1, 1, 2.0, None, None, &AppendV2Options { metadata: Some(&help), ..Default::default() })
            .unwrap();
        assert!(app.commit().is_none());
        assert_eq!(1, app_test.result_samples().len());
        assert_eq!(0.0, ooo(&oa));
        assert_eq!(0.0, no_meta(&oa));
    }
    // without metadata
    {
        let app_test = Appendable::new();
        let oa = new_otlp_instrumented_appendable(Some(&Registry::new()), app_with(&app_test));
        assert_eq!(0.0, ooo(&oa));
        assert_eq!(0.0, no_meta(&oa));
        let mut app = oa.appender_v2(&Context::background());
        app.append(0, &Labels::default(), -1, 1, 2.0, None, None, &AppendV2Options::default()).unwrap();
        assert!(app.commit().is_none());
        assert_eq!(1, app_test.result_samples().len());
        assert_eq!(0.0, ooo(&oa));
        assert_eq!(1.0, no_meta(&oa));
    }
    let two = [Exemplar::default(), Exemplar::default()];
    // without metadata; 2 exemplar OOO errors
    {
        let app_test = Appendable::new().with_errs(None, Some(ERR_OUT_OF_ORDER_EXEMPLAR), None);
        let oa = new_otlp_instrumented_appendable(Some(&Registry::new()), app_with(&app_test));
        assert_eq!(0.0, ooo(&oa));
        assert_eq!(0.0, no_meta(&oa));
        let mut app = oa.appender_v2(&Context::background());
        // Partial errors are handled in the middleware: the OTLP converter does not handle them.
        app.append(0, &Labels::default(), -1, 1, 2.0, None, None, &AppendV2Options { exemplars: &two, ..Default::default() })
            .unwrap();
        assert!(app.commit().is_none());
        assert_eq!(1, app_test.result_samples().len());
        assert_eq!(2.0, ooo(&oa));
        assert_eq!(1.0, no_meta(&oa));
    }
    // exemplar errors other than out of order are not counted
    {
        let app_test = Appendable::new().with_errs(None, Some(ERR_EXEMPLAR_LABEL_LENGTH), None);
        let oa = new_otlp_instrumented_appendable(Some(&Registry::new()), app_with(&app_test));
        let mut app = oa.appender_v2(&Context::background());
        app.append(0, &Labels::default(), -1, 1, 2.0, None, None, &AppendV2Options { exemplars: &two, ..Default::default() })
            .unwrap();
        assert!(app.commit().is_none());
        assert_eq!(1, app_test.result_samples().len());
        assert_eq!(0.0, ooo(&oa));
    }
    // exemplars too far in the future are not counted
    {
        let app_test = Appendable::new();
        let oa = new_otlp_instrumented_appendable(Some(&Registry::new()), app_with(&app_test));
        let future = ms(SystemTime::now() + Duration::from_secs(3600));
        let ex = [Exemplar { ts: future, has_ts: true, ..Default::default() }];
        let mut app = oa.appender_v2(&Context::background());
        app.append(0, &Labels::default(), -1, 1, 2.0, None, None, &AppendV2Options { exemplars: &ex, ..Default::default() })
            .unwrap();
        assert!(app.commit().is_none());
        assert_eq!(1, app_test.result_samples().len());
        assert_eq!(0.0, ooo(&oa));
    }
}
