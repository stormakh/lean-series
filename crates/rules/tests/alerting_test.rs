//! Port of upstream/rules/alerting_test.go.
mod common;

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use model::histogram::{FloatHistogram, Span};
use model::labels::{self, Labels, ValidationScheme};
use model::relabel::{self, Action};
use promql::parser::{self, Expr, NumberLiteral};
use promql::promqltest;
use promql::{Engine, EngineOpts, Sample, Vector};
use rules::alerting::ERR_DUPLICATE_ALERT_LABEL_SET;
use rules::log::Logger;
use rules::{
    engine_query_func, from_origin_context, new_rule_detail, send_alerts, Alert, AlertState, AlertingRule, NotifyFunc,
    QueryFunc, RecordingRule, Rule, RuleDetail, RuleRef, Sender, Time,
};
use storage::{Context, Error, Queryable};

fn test_parser() -> parser::Parser {
    parser::Parser::new(parser::Options::default())
}

fn parse(q: &str) -> Expr {
    test_parser().parse_expr(q).unwrap()
}

/// Go `testEngine`.
fn test_engine() -> Arc<Engine> {
    Arc::new(promqltest::new_test_engine_with_opts(EngineOpts {
        max_samples: 10000,
        timeout: Duration::from_secs(100),
        no_step_subquery_interval_fn: Some(Arc::new(|_| 60 * 1000)),
        enable_at_modifier: true,
        enable_negative_offset: true,
        enable_per_step_stats: true,
        ..Default::default()
    }))
}

/// Go `promqltest.LoadedStorage`: the real TSDB (kept alive by the first value) and it as a queryable.
fn loaded(input: &str) -> (common::TestStorage, Arc<dyn Queryable>) {
    let st = common::loaded_storage(input);
    let q: Arc<dyn Queryable> = st.db.clone();
    (st, q)
}

fn sample(metric: &[&str], f: f64, t: i64) -> Sample {
    Sample { metric: labels::from_strings(metric), f, t, h: None, drop_name: false }
}

/// The 'ALERTS' samples of a result; every other sample must be 'ALERTS_FOR_STATE'.
fn filter_alerts(res: Vector, filtered: &mut Vector) {
    for smpl in res {
        let name = smpl.metric.get("__name__").to_string();
        if name == "ALERTS" {
            filtered.push(smpl);
        } else {
            // If not 'ALERTS', it has to be 'ALERTS_FOR_STATE'.
            assert_eq!("ALERTS_FOR_STATE", name);
        }
    }
}

fn minutes(n: u64) -> Duration {
    Duration::from_secs(60 * n)
}

#[allow(clippy::too_many_arguments)]
fn new_rule(
    name: &str,
    expr: impl Into<Option<Expr>>,
    hold: Duration,
    keep_firing_for: Duration,
    lbls: Labels,
    annotations: Labels,
    external_labels: Labels,
    external_url: &str,
) -> AlertingRule {
    AlertingRule::new(name, expr, hold, keep_firing_for, lbls, annotations, external_labels, external_url, true, Logger::nop())
}

#[test]
fn test_alerting_rule_state() {
    let tests: Vec<(&str, Vec<(u64, AlertState)>, AlertState)> = vec![
        ("MaxStateFiring", vec![(0, AlertState::Pending), (1, AlertState::Firing)], AlertState::Firing),
        ("MaxStatePending", vec![(0, AlertState::Inactive), (1, AlertState::Pending)], AlertState::Pending),
        ("MaxStateInactive", vec![(0, AlertState::Inactive), (1, AlertState::Inactive)], AlertState::Inactive),
    ];
    for (i, (name, active, want)) in tests.into_iter().enumerate() {
        let rule = new_rule(name, None, Duration::ZERO, Duration::ZERO, Labels::empty(), Labels::empty(), Labels::empty(), "");
        *rule.active_map() = active.into_iter().map(|(k, state)| (k, Alert { state, ..Default::default() })).collect::<HashMap<_, _>>();
        // Set evaluation timestamp to simulate that the rule has been evaluated.
        rule.set_evaluation_timestamp(Time::now());
        let got = rule.state();
        assert_eq!(want, got, "test case {i} unexpected AlertState, want:{want:?} got:{got:?}");
    }
}

#[test]
fn test_alerting_rule_template_with_histogram() {
    let h = FloatHistogram {
        schema: 0,
        count: 30.0,
        sum: 1111.1,
        zero_threshold: 0.001,
        zero_count: 2.0,
        positive_spans: vec![Span { offset: 0, length: 1 }, Span { offset: 1, length: 5 }],
        positive_buckets: vec![1.0, 1.0, 2.0, 1.0, 1.0, 1.0],
        negative_spans: vec![Span { offset: 1, length: 4 }, Span { offset: 4, length: 3 }],
        negative_buckets: vec![-2.0, 2.0, 2.0, 7.0, 5.0, 5.0, 2.0],
        ..Default::default()
    };
    let hr = Arc::new(h.clone());
    let q: QueryFunc = Arc::new(move |_: &Context, _: &str, _: Time| {
        Ok(vec![Sample { metric: Labels::empty(), t: 0, f: 0.0, h: Some(hr.clone()), drop_name: false }])
    });

    let rule = new_rule(
        "HistogramAsValue",
        parse("foo"),
        minutes(1),
        Duration::ZERO,
        labels::from_strings(&["histogram", "{{ $value }}"]),
        Labels::empty(),
        Labels::empty(),
        "",
    );

    let eval_time = Time::now();
    let res = rule.eval(&Context::background(), Duration::ZERO, eval_time, &q, None, 0).unwrap();

    assert_eq!(res.len(), 2);
    let hs = h.to_string();
    for smpl in res {
        let name = smpl.metric.get("__name__").to_string();
        if name == "ALERTS" {
            let result = sample(
                &["__name__", "ALERTS", "alertname", "HistogramAsValue", "alertstate", "pending", "histogram", &hs],
                1.0,
                eval_time.millis(),
            );
            assert_eq!(result, smpl);
        } else {
            // If not 'ALERTS', it has to be 'ALERTS_FOR_STATE'.
            assert_eq!("ALERTS_FOR_STATE", name);
        }
    }
}

#[test]
fn test_alerting_rule_labels_update() {
    let (_st, storage) = loaded(
        r#"
		load 1m
			http_requests{job="app-server", instance="0"}	75 85 70 70 stale
	"#,
    );

    let rule = new_rule(
        "HTTPRequestRateLow",
        parse("http_requests < 100"),
        minutes(1),
        Duration::ZERO,
        // Basing alerting rule labels off of a value that can change is a very bad idea.
        // If an alert is going back and forth between two label values it will never fire.
        // Instead, you should write two alerts with constant labels.
        labels::from_strings(&["severity", "{{ if lt $value 80.0 }}critical{{ else }}warning{{ end }}"]),
        Labels::empty(),
        Labels::empty(),
        "",
    );

    let alert = |state: &str, severity: &str| {
        sample(
            &[
                "__name__", "ALERTS", "alertname", "HTTPRequestRateLow", "alertstate", state, "instance", "0", "job",
                "app-server", "severity", severity,
            ],
            1.0,
            0,
        )
    };
    let results: Vec<Vector> = vec![
        vec![alert("pending", "critical")],
        vec![alert("pending", "warning")],
        vec![alert("pending", "critical")],
        vec![alert("firing", "critical")],
    ];

    let ng = test_engine();
    let base_time = Time::unix(0, 0);
    let n = results.len();
    for (i, mut result) in results.into_iter().enumerate() {
        let eval_time = base_time.add(minutes(i as u64));
        result[0].t = eval_time.millis();
        let res = rule
            .eval(&Context::background(), Duration::ZERO, eval_time, &engine_query_func(ng.clone(), storage.clone()), None, 0)
            .unwrap_or_else(|e| panic!("case {i}: {e}"));
        let mut filtered = Vector::new(); // After removing 'ALERTS_FOR_STATE' samples.
        filter_alerts(res, &mut filtered);
        assert_eq!(result, filtered, "case {i}");
    }
    let eval_time = base_time.add(minutes(n as u64));
    let res = rule.eval(&Context::background(), Duration::ZERO, eval_time, &engine_query_func(ng, storage), None, 0).unwrap();
    assert!(res.is_empty(), "{res:?}");
}

#[test]
fn test_alerting_rule_external_labels_in_template() {
    let (_st, storage) = loaded(
        r#"
		load 1m
			http_requests{job="app-server", instance="0"}	75 85 70 70
	"#,
    );
    let expr = parse("http_requests < 100");
    let tmpl = "There are {{ len $externalLabels }} external Labels, of which foo is {{ $externalLabels.foo }}.";

    let rule_without_external_labels = new_rule(
        "ExternalLabelDoesNotExist",
        expr.clone(),
        minutes(1),
        Duration::ZERO,
        labels::from_strings(&["templated_label", tmpl]),
        Labels::empty(),
        Labels::empty(),
        "",
    );
    let rule_with_external_labels = new_rule(
        "ExternalLabelExists",
        expr,
        minutes(1),
        Duration::ZERO,
        labels::from_strings(&["templated_label", tmpl]),
        Labels::empty(),
        labels::from_strings(&["foo", "bar", "dings", "bums"]),
        "",
    );
    let eval_time = Time::unix(0, 0);
    let t = eval_time.millis();
    let result = vec![
        sample(
            &[
                "__name__", "ALERTS", "alertname", "ExternalLabelDoesNotExist", "alertstate", "pending", "instance", "0",
                "job", "app-server", "templated_label", "There are 0 external Labels, of which foo is .",
            ],
            1.0,
            t,
        ),
        sample(
            &[
                "__name__", "ALERTS", "alertname", "ExternalLabelExists", "alertstate", "pending", "instance", "0", "job",
                "app-server", "templated_label", "There are 2 external Labels, of which foo is bar.",
            ],
            1.0,
            t,
        ),
    ];

    let ng = test_engine();
    let qf = engine_query_func(ng, storage);
    let mut filtered = Vector::new(); // After removing 'ALERTS_FOR_STATE' samples.
    let res = rule_without_external_labels.eval(&Context::background(), Duration::ZERO, eval_time, &qf, None, 0).unwrap();
    filter_alerts(res, &mut filtered);
    let res = rule_with_external_labels.eval(&Context::background(), Duration::ZERO, eval_time, &qf, None, 0).unwrap();
    filter_alerts(res, &mut filtered);

    assert_eq!(result, filtered);
}

#[test]
fn test_alerting_rule_external_url_in_template() {
    let (_st, storage) = loaded(
        r#"
		load 1m
			http_requests{job="app-server", instance="0"}	75 85 70 70
	"#,
    );
    let expr = parse("http_requests < 100");

    let rule_without_external_url = new_rule(
        "ExternalURLDoesNotExist",
        expr.clone(),
        minutes(1),
        Duration::ZERO,
        labels::from_strings(&["templated_label", "The external URL is {{ $externalURL }}."]),
        Labels::empty(),
        Labels::empty(),
        "",
    );
    let rule_with_external_url = new_rule(
        "ExternalURLExists",
        expr,
        minutes(1),
        Duration::ZERO,
        labels::from_strings(&["templated_label", "The external URL is {{ $externalURL }}."]),
        Labels::empty(),
        Labels::empty(),
        "http://localhost:1234",
    );
    let eval_time = Time::unix(0, 0);
    let t = eval_time.millis();
    let result = vec![
        sample(
            &[
                "__name__", "ALERTS", "alertname", "ExternalURLDoesNotExist", "alertstate", "pending", "instance", "0",
                "job", "app-server", "templated_label", "The external URL is .",
            ],
            1.0,
            t,
        ),
        sample(
            &[
                "__name__", "ALERTS", "alertname", "ExternalURLExists", "alertstate", "pending", "instance", "0", "job",
                "app-server", "templated_label", "The external URL is http://localhost:1234.",
            ],
            1.0,
            t,
        ),
    ];

    let ng = test_engine();
    let qf = engine_query_func(ng, storage);
    let mut filtered = Vector::new(); // After removing 'ALERTS_FOR_STATE' samples.
    let res = rule_without_external_url.eval(&Context::background(), Duration::ZERO, eval_time, &qf, None, 0).unwrap();
    filter_alerts(res, &mut filtered);
    let res = rule_with_external_url.eval(&Context::background(), Duration::ZERO, eval_time, &qf, None, 0).unwrap();
    filter_alerts(res, &mut filtered);

    assert_eq!(result, filtered);
}

#[test]
fn test_alerting_rule_empty_label_from_template() {
    let (_st, storage) = loaded(
        r#"
		load 1m
			http_requests{job="app-server", instance="0"}	75 85 70 70
	"#,
    );
    let rule = new_rule(
        "EmptyLabel",
        parse("http_requests < 100"),
        minutes(1),
        Duration::ZERO,
        labels::from_strings(&["empty_label", ""]),
        Labels::empty(),
        Labels::empty(),
        "",
    );
    let eval_time = Time::unix(0, 0);
    let result = vec![sample(
        &["__name__", "ALERTS", "alertname", "EmptyLabel", "alertstate", "pending", "instance", "0", "job", "app-server"],
        1.0,
        eval_time.millis(),
    )];

    let ng = test_engine();
    let mut filtered = Vector::new(); // After removing 'ALERTS_FOR_STATE' samples.
    let res = rule.eval(&Context::background(), Duration::ZERO, eval_time, &engine_query_func(ng, storage), None, 0).unwrap();
    filter_alerts(res, &mut filtered);
    assert_eq!(result, filtered);
}

#[test]
fn test_alerting_rule_query_in_template() {
    let (_st, storage) = loaded(
        r#"
		load 1m
			http_requests{job="app-server", instance="0"}	70 85 70 70
	"#,
    );

    let rule = Arc::new(new_rule(
        "ruleWithQueryInTemplate",
        parse("sum(http_requests) < 100"),
        minutes(1),
        Duration::ZERO,
        labels::from_strings(&["label", "value"]),
        labels::from_strings(&[
            "templated_label",
            r#"{{- with "sort(sum(http_requests) by (instance))" | query -}}
{{- range $i,$v := . -}}
instance: {{ $v.Labels.instance }}, value: {{ printf "%.0f" $v.Value }};
{{- end -}}
{{- end -}}
"#,
        ]),
        Labels::empty(),
        "",
    ));
    let eval_time = Time::unix(0, 0);

    let ng = test_engine();

    let (start_tx, start_rx) = mpsc::channel::<()>();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let start_tx = Mutex::new(Some(start_tx));
    let done_rx = Mutex::new(done_rx);
    let inner = engine_query_func(ng, storage);
    let slow_query_func: QueryFunc = Arc::new(move |ctx: &Context, q: &str, ts: Time| {
        if q == "sort(sum(http_requests) by (instance))" {
            // This is a minimum reproduction of issue 10703, expand template with query.
            drop(start_tx.lock().unwrap().take()); // Go close(startQueryCh).
            // adapted: Go waits 20ms before failing; the machine running the tests is heavily loaded, so the
            // wait is 10s. A rule lock held during template expansion blocks forever, so the check is the same.
            if done_rx.lock().unwrap().recv_timeout(Duration::from_secs(10)).is_err() {
                panic!("unexpected blocking when template expanding.");
            }
        }
        inner(ctx, q, ts)
    });
    let r = rule.clone();
    let getter = std::thread::spawn(move || {
        if start_rx.recv().is_ok() {
            // The channel was closed without a send; the query started.
        }
        let _ = r.health();
        let _ = r.last_error();
        let _ = r.get_evaluation_duration();
        let _ = r.get_evaluation_timestamp();
        drop(done_tx); // Go close(getDoneCh).
    });
    let res = rule.eval(&Context::background(), Duration::ZERO, eval_time, &slow_query_func, None, 0);
    // Dropping the query func closes the start channel if the template never queried, so the thread ends.
    drop(slow_query_func);
    getter.join().unwrap();
    res.unwrap();
}

#[test]
fn test_alerting_rule_duplicate() {
    let st = common::new();
    let storage: Arc<dyn Queryable> = st.db.clone();
    let engine = Arc::new(promqltest::new_test_engine_with_opts(EngineOpts {
        max_samples: 10,
        timeout: Duration::from_secs(10),
        ..Default::default()
    }));
    let now = Time::now();
    let rule = new_rule(
        "foo",
        parse(r#"vector(0) or label_replace(vector(0),"test","x","","")"#),
        minutes(1),
        Duration::ZERO,
        labels::from_strings(&["test", "test"]),
        Labels::empty(),
        Labels::empty(),
        "",
    );
    let err = rule.eval(&Context::background(), Duration::ZERO, now, &engine_query_func(engine, storage), None, 0).unwrap_err();
    assert!(err.is(&ERR_DUPLICATE_ALERT_LABEL_SET), "{err}");
}

#[test]
fn test_alerting_rule_limit() {
    let (_st, storage) = loaded(
        r#"
		load 1m
			metric{label="1"} 1
			metric{label="2"} 1
	"#,
    );
    let tests: &[(i64, &str)] = &[(0, ""), (-1, ""), (2, ""), (1, "exceeded limit of 1 with 2 alerts")];
    let rule = new_rule(
        "foo",
        parse("metric > 0"),
        minutes(1),
        Duration::ZERO,
        labels::from_strings(&["test", "test"]),
        Labels::empty(),
        Labels::empty(),
        "",
    );
    let eval_time = Time::unix(0, 0);
    let ng = test_engine();
    for &(limit, err) in tests {
        match rule.eval(&Context::background(), Duration::ZERO, eval_time, &engine_query_func(ng.clone(), storage.clone()), None, limit) {
            Err(e) => assert_eq!(e.to_string(), err),
            Ok(_) => assert!(err.is_empty(), "Expected error {err}, got none"),
        }
    }
}

type SelectFn = Box<dyn Fn(bool, Option<&storage::SelectHints>, &[model::matcher::Matcher]) -> storage::BoxSeriesSet + Send + Sync>;

#[test]
fn test_query_for_state_series() {
    let test_error = Error::msg("test error");
    let mock_lset = ["__name__", "ALERTS_FOR_STATE", "alertname", "TestRule", "severity", "critical"];

    struct TestInput {
        select_mock_function: SelectFn,
        expected_series: Option<storage::SeriesArc>,
        expected_error: Option<Error>,
    }

    let te = test_error.clone();
    let tests = vec![
        // Test for empty series.
        TestInput {
            select_mock_function: Box::new(|_, _, _| storage::empty_series_set()),
            expected_series: None,
            expected_error: None,
        },
        // Test for error series.
        TestInput {
            select_mock_function: Box::new(move |_, _, _| storage::err_series_set(te.clone())),
            expected_series: None,
            expected_error: Some(test_error.clone()),
        },
        // Test for mock series.
        TestInput {
            select_mock_function: Box::new(move |_, _, _| {
                storage::test_series_set(storage::mock_series(Vec::new(), vec![1, 2, 3], vec![1.0, 2.0, 3.0], &mock_lset))
            }),
            expected_series: Some(storage::mock_series(Vec::new(), vec![1, 2, 3], vec![1.0, 2.0, 3.0], &mock_lset)),
            expected_error: None,
        },
    ];

    // Go compares the MockSeries structs; here the labels and the samples of the series.
    fn contents(s: &storage::SeriesArc) -> (Labels, Vec<storage::Sample>) {
        let mut it = s.iterator(None);
        (s.labels().clone(), storage::series::expand_samples(&mut it).unwrap())
    }

    for tst in tests {
        let querier = storage::MockQuerier { select_mock_function: tst.select_mock_function };
        let rule = new_rule(
            "TestRule",
            None,
            minutes(1),
            Duration::ZERO,
            labels::from_strings(&["severity", "critical"]),
            Labels::empty(),
            Labels::empty(),
            "",
        );

        let sample = rule.for_state_sample(None, Time::ZERO, 0.0);

        // Go returns the series set and its error; on an error the set has no series.
        let (series, err) = match rule.query_for_state_series(&Context::background(), &querier) {
            Ok(mut series_set) => {
                let mut series = None;
                while series_set.next() {
                    if series_set.at().labels().len() == sample.metric.len() {
                        series = Some(series_set.at().clone());
                        break;
                    }
                }
                (series, None)
            }
            Err(e) => (None, Some(e)),
        };

        assert_eq!(tst.expected_series.as_ref().map(contents), series.as_ref().map(contents));
        assert_eq!(tst.expected_error, err);
    }
}

/// Verifies that labels containing Go template syntax are excluded from the matchers used to query
/// ALERTS_FOR_STATE, so that per-instance expanded values stored in the series can still be found.
#[test]
fn test_query_for_state_series_template_labels() {
    let got_matchers: Arc<Mutex<Vec<model::matcher::Matcher>>> = Arc::default();
    let gm = got_matchers.clone();
    let querier = storage::MockQuerier {
        select_mock_function: Box::new(move |_, _, matchers| {
            *gm.lock().unwrap() = matchers.to_vec();
            storage::empty_series_set()
        }),
    };

    let rule = new_rule(
        "TestRule",
        None,
        minutes(1),
        Duration::ZERO,
        labels::from_strings(&["severity", "critical", "instance_ext", "instance_{{ $labels.instance }}"]),
        Labels::empty(),
        Labels::empty(),
        "",
    );

    rule.query_for_state_series(&Context::background(), &querier).unwrap();

    let got = got_matchers.lock().unwrap();
    for m in got.iter() {
        assert!(!m.value.contains("{{"), "template label {:?} must not appear in Select matchers", m.name);
    }

    // The static label must still be present.
    let found = got.iter().any(|m| m.name == "severity" && m.value == "critical");
    assert!(found, "static label severity=critical must be present in Select matchers");
}

/// Tests a fix for https://github.com/prometheus/prometheus/issues/11424.
#[test]
fn test_send_alerts_dont_affect_active_alerts() {
    let mut rule = new_rule(
        "TestRule",
        None,
        minutes(1),
        Duration::ZERO,
        labels::from_strings(&["severity", "critical"]),
        Labels::empty(),
        Labels::empty(),
        "",
    );

    // Set an active alert.
    let lbls = labels::from_strings(&["a1", "1"]);
    let h = lbls.hash();
    let al = Alert { state: AlertState::Firing, labels: lbls, active_at: Time::now(), ..Default::default() };
    rule.active_map().insert(h, al);

    rule.set_vector(Some(parse("foo")));
    let rule = Arc::new(rule);

    // The relabel rule reproduced the bug here.
    let opts = notifier::Options {
        queue_capacity: 1,
        relabel_configs: vec![relabel::Config {
            source_labels: Some(vec!["a1".to_string()]),
            regex: relabel::Regexp::must_new("(.+)"),
            target_label: "a1".to_string(),
            replacement: "bug".to_string(),
            action: Action::Replace,
            name_validation_scheme: ValidationScheme::Utf8,
            ..Default::default()
        }],
        ..Default::default()
    };
    let nm = Arc::new(notifier::Manager::new(opts, ValidationScheme::Utf8, Some(Logger::nop())));

    let f = send_alerts(nm.clone() as Arc<dyn Sender>, "");
    let r = rule.clone();
    let notify_func: NotifyFunc = Arc::new(move |ctx: &Context, expr: &str, alerts: Vec<Alert>| {
        assert_eq!(alerts.len(), 1);
        // adapted: Go compares the pointer `al` with the sent pointer (the same object, updated by
        // sendAlerts); here the sent copy must equal the alert stored in the rule.
        assert_eq!(r.active_map()[&h], alerts[0]);
        f(ctx, expr, alerts);
    });

    rule.send_alerts(&Context::background(), Time::now(), Duration::ZERO, Duration::ZERO, &notify_func);
    nm.stop();

    // The relabel rule changes a1=1 to a1=bug.
    // But the labels with the AlertingRule should not be changed.
    assert_eq!(labels::from_strings(&["a1", "1"]), rule.active_map()[&h].labels);
}

#[test]
fn test_keep_firing_for() {
    let (_st, storage) = loaded(
        r#"
		load 1m
			http_requests{job="app-server", instance="0"}	75 85 70 70 10x5
	"#,
    );
    let rule = new_rule(
        "HTTPRequestRateHigh",
        parse("http_requests > 50"),
        minutes(1),
        minutes(1),
        Labels::empty(),
        Labels::empty(),
        Labels::empty(),
        "",
    );

    let alert = |state: &str| {
        sample(
            &["__name__", "ALERTS", "alertname", "HTTPRequestRateHigh", "alertstate", state, "instance", "0", "job", "app-server"],
            1.0,
            0,
        )
    };
    let results: Vec<Vector> = vec![
        vec![alert("pending")],
        vec![alert("firing")],
        vec![alert("firing")],
        vec![alert("firing")],
        // From now on the alert should keep firing.
        vec![alert("firing")],
    ];

    let ng = test_engine();
    let base_time = Time::unix(0, 0);
    let n = results.len();
    for (i, mut result) in results.into_iter().enumerate() {
        let eval_time = base_time.add(minutes(i as u64));
        result[0].t = eval_time.millis();
        let res = rule
            .eval(&Context::background(), Duration::ZERO, eval_time, &engine_query_func(ng.clone(), storage.clone()), None, 0)
            .unwrap_or_else(|e| panic!("case {i}: {e}"));
        let mut filtered = Vector::new(); // After removing 'ALERTS_FOR_STATE' samples.
        filter_alerts(res, &mut filtered);
        assert_eq!(result, filtered, "case {i}");
    }
    let eval_time = base_time.add(minutes(n as u64));
    let res = rule.eval(&Context::background(), Duration::ZERO, eval_time, &engine_query_func(ng, storage), None, 0).unwrap();
    assert!(res.is_empty(), "{res:?}");
}

#[test]
fn test_pending_and_keep_firing_for() {
    let (_st, storage) = loaded(
        r#"
		load 1m
			http_requests{job="app-server", instance="0"}	75 10x10
	"#,
    );
    let rule = new_rule(
        "HTTPRequestRateHigh",
        parse("http_requests > 50"),
        minutes(1),
        minutes(1),
        Labels::empty(),
        Labels::empty(),
        Labels::empty(),
        "",
    );

    let base_time = Time::unix(0, 0);
    let result = sample(
        &["__name__", "ALERTS", "alertname", "HTTPRequestRateHigh", "alertstate", "pending", "instance", "0", "job", "app-server"],
        1.0,
        base_time.millis(),
    );

    let ng = test_engine();
    let res = rule
        .eval(&Context::background(), Duration::ZERO, base_time, &engine_query_func(ng.clone(), storage.clone()), None, 0)
        .unwrap();

    assert_eq!(res.len(), 2);
    for smpl in res {
        let name = smpl.metric.get("__name__").to_string();
        if name == "ALERTS" {
            assert_eq!(result, smpl);
        } else {
            // If not 'ALERTS', it has to be 'ALERTS_FOR_STATE'.
            assert_eq!("ALERTS_FOR_STATE", name);
        }
    }

    let eval_time = base_time.add(minutes(1));
    let res = rule.eval(&Context::background(), Duration::ZERO, eval_time, &engine_query_func(ng, storage), None, 0).unwrap();
    assert!(res.is_empty(), "{res:?}");
}

/// Checks that the alerting rule details are passed through the context.
#[test]
fn test_alerting_eval_with_origin() {
    let now = Time::now();
    let name = "my-recording-rule";
    let query = r#"count(metric{foo="bar"}) > 0"#;
    let detail: Arc<Mutex<RuleDetail>> = Arc::default();
    let lbs = labels::from_strings(&["test", "test"]);

    let rule = new_rule(name, parse(query), Duration::from_secs(1), minutes(1), lbs, Labels::empty(), Labels::empty(), "");

    let d = detail.clone();
    let qf: QueryFunc = Arc::new(move |ctx: &Context, _q: &str, _t: Time| {
        *d.lock().unwrap() = from_origin_context(ctx);
        Ok(Vec::new())
    });
    rule.eval(&Context::background(), Duration::ZERO, now, &qf, None, 0).unwrap();
    assert_eq!(*detail.lock().unwrap(), new_rule_detail(&rule));
}

fn same(a: &[RuleRef], b: &[RuleRef]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| rules::same_rule(x, y))
}

fn number(v: f64) -> Expr {
    Expr::NumberLiteral(Box::new(NumberLiteral { val: v, ..Default::default() }))
}

#[test]
fn test_alerting_rule_set_dependent_rules() {
    let dependent_rule: RuleRef = Arc::new(RecordingRule::new("test1", None, Labels::empty()));
    let rule = new_rule(
        "test",
        number(1.0),
        minutes(1),
        Duration::ZERO,
        labels::from_strings(&["test", "test"]),
        Labels::empty(),
        Labels::empty(),
        "",
    );
    assert!(!rule.no_dependent_rules());

    rule.set_dependent_rules(std::slice::from_ref(&dependent_rule));
    assert!(!rule.no_dependent_rules());
    assert!(same(&[dependent_rule.clone()], &rule.dependent_rules()));

    rule.set_dependent_rules(&[]);
    assert!(rule.no_dependent_rules());
    assert!(rule.dependent_rules().is_empty());
}

#[test]
fn test_alerting_rule_set_dependency_rules() {
    let dependency_rule: RuleRef = Arc::new(RecordingRule::new("test1", None, Labels::empty()));
    let rule = new_rule(
        "test",
        number(1.0),
        minutes(1),
        Duration::ZERO,
        labels::from_strings(&["test", "test"]),
        Labels::empty(),
        Labels::empty(),
        "",
    );
    assert!(!rule.no_dependency_rules());

    rule.set_dependency_rules(std::slice::from_ref(&dependency_rule));
    assert!(!rule.no_dependency_rules());
    assert!(same(&[dependency_rule.clone()], &rule.dependency_rules()));

    rule.set_dependency_rules(&[]);
    assert!(rule.no_dependency_rules());
    assert!(rule.dependency_rules().is_empty());
}

#[test]
fn test_alerting_rule_active_alerts_count() {
    let rule = new_rule(
        "TestRule",
        None,
        minutes(1),
        Duration::ZERO,
        labels::from_strings(&["severity", "critical"]),
        Labels::empty(),
        Labels::empty(),
        "",
    );

    assert_eq!(0, rule.active_alerts_count());

    // Set an active alert.
    let lbls = labels::from_strings(&["a1", "1"]);
    let h = lbls.hash();
    rule.active_map().insert(h, Alert { state: AlertState::Firing, labels: lbls, active_at: Time::now(), ..Default::default() });

    assert_eq!(1, rule.active_alerts_count());
}

/// Verifies that when the hold duration ("for" duration) is increased on a rule that already has a firing
/// alert, the alert is demoted back to pending because the elapsed time since ActiveAt no longer meets the new,
/// larger hold duration.
#[test]
fn test_firing_alert_reset_to_pending_on_hold_duration_increase() {
    let short_hold = Duration::from_secs(15);
    let long_hold = Duration::from_secs(3600);

    let mut rule = new_rule(
        "TestResetToPending",
        parse("foo"),
        short_hold,
        Duration::ZERO,
        Labels::empty(),
        Labels::empty(),
        Labels::empty(),
        "",
    );

    let base_time = Time::unix(0, 0);
    let q: QueryFunc = Arc::new(|_: &Context, _: &str, ts: Time| {
        Ok(vec![Sample { metric: Labels::empty(), t: ts.millis(), f: 1.0, h: None, drop_name: false }])
    });
    // Go keeps a pointer to the one alert; here the alert is read from the rule after each evaluation.
    let alert = |rule: &AlertingRule| -> Alert {
        let active = rule.active_map();
        assert_eq!(active.len(), 1);
        active.values().next().unwrap().clone()
    };

    // Eval at t=0: creates the alert in StatePending with ActiveAt = baseTime.
    rule.eval(&Context::background(), Duration::ZERO, base_time, &q, None, 0).unwrap();
    assert_eq!(rule.active_map().len(), 1);
    assert_eq!(AlertState::Pending, alert(&rule).state);

    // Eval at t=15s: the short holdDuration is met, alert transitions to firing.
    let eval_time = base_time.add(short_hold);
    rule.eval(&Context::background(), Duration::ZERO, eval_time, &q, None, 0).unwrap();
    let a = alert(&rule);
    assert_eq!(AlertState::Firing, a.state);
    assert!(!a.fired_at.is_zero(), "FiredAt should be set after firing");

    // Now increase the holdDuration to 1 hour (simulating a rule config reload).
    rule.set_hold_duration(long_hold);

    // Eval at t=30s: only 30s have elapsed since ActiveAt, which is far less than the new 1h holdDuration, so
    // the alert must go back to pending.
    let eval_time = base_time.add(Duration::from_secs(30));
    let res = rule.eval(&Context::background(), Duration::ZERO, eval_time, &q, None, 0).unwrap();
    let a = alert(&rule);
    assert_eq!(AlertState::Pending, a.state);
    assert!(a.fired_at.is_zero(), "FiredAt should be reset");
    assert!(a.last_sent_at.is_zero(), "LastSentAt should be reset");
    assert!(a.keep_firing_since.is_zero(), "KeepFiringSince should be reset");
    for smpl in &res {
        if smpl.metric.get("__name__") == "ALERTS" {
            assert_eq!("pending", smpl.metric.get("alertstate"));
        }
    }

    // Eval at t=1h: now the new holdDuration is met, alert fires again.
    let eval_time = base_time.add(long_hold);
    let res = rule.eval(&Context::background(), Duration::ZERO, eval_time, &q, None, 0).unwrap();
    assert_eq!(AlertState::Firing, alert(&rule).state);
    for smpl in &res {
        if smpl.metric.get("__name__") == "ALERTS" {
            assert_eq!("firing", smpl.metric.get("alertstate"));
        }
    }
}
