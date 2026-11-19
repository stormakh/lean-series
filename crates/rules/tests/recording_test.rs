//! Port of upstream/rules/recording_test.go.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use model::labels::{self, Labels};
use promql::parser::{self, Expr, NumberLiteral};
use promql::promqltest::{self, TestStorage};
use promql::{Engine, EngineOpts, Sample, Vector};
use rules::recording::ERR_DUPLICATE_RECORDING_LABEL_SET;
use rules::{engine_query_func, from_origin_context, new_rule_detail, QueryFunc, RecordingRule, Rule, RuleDetail, RuleRef, Time};
use storage::{Context, Queryable};

fn test_parser() -> parser::Parser {
    parser::Parser::new(parser::Options::default())
}

fn parse(q: &str) -> Expr {
    test_parser().parse_expr(q).unwrap()
}

/// Go `testEngine` (alerting_test.go).
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

fn rule_evaluation_time() -> Time {
    Time::unix(0, 0)
}

struct Scenario {
    name: &'static str,
    rule_labels: Labels,
    expr: Expr,
    expected: Vector,
}

fn sample(metric: &[&str], f: f64, t: i64) -> Sample {
    Sample { metric: labels::from_strings(metric), f, t, h: None, drop_name: false }
}

fn rule_eval_test_scenarios() -> Vec<Scenario> {
    let expr_with_metric_name = parse("sort(metric)");
    let expr_without_metric_name = parse("sort(metric + metric)");
    let t = rule_evaluation_time().millis();
    vec![
        Scenario {
            name: "no labels in recording rule, metric name in query result",
            rule_labels: Labels::empty(),
            expr: expr_with_metric_name.clone(),
            expected: vec![
                sample(&["__name__", "test_rule", "label_a", "1", "label_b", "3"], 1.0, t),
                sample(&["__name__", "test_rule", "label_a", "2", "label_b", "4"], 10.0, t),
            ],
        },
        Scenario {
            name: "only new labels in recording rule, metric name in query result",
            rule_labels: labels::from_strings(&["extra_from_rule", "foo"]),
            expr: expr_with_metric_name.clone(),
            expected: vec![
                sample(&["__name__", "test_rule", "label_a", "1", "label_b", "3", "extra_from_rule", "foo"], 1.0, t),
                sample(&["__name__", "test_rule", "label_a", "2", "label_b", "4", "extra_from_rule", "foo"], 10.0, t),
            ],
        },
        Scenario {
            name: "some replacement labels in recording rule, metric name in query result",
            rule_labels: labels::from_strings(&["label_a", "from_rule"]),
            expr: expr_with_metric_name,
            expected: vec![
                sample(&["__name__", "test_rule", "label_a", "from_rule", "label_b", "3"], 1.0, t),
                sample(&["__name__", "test_rule", "label_a", "from_rule", "label_b", "4"], 10.0, t),
            ],
        },
        Scenario {
            name: "no labels in recording rule, no metric name in query result",
            rule_labels: Labels::empty(),
            expr: expr_without_metric_name,
            expected: vec![
                sample(&["__name__", "test_rule", "label_a", "1", "label_b", "3"], 2.0, t),
                sample(&["__name__", "test_rule", "label_a", "2", "label_b", "4"], 20.0, t),
            ],
        },
    ]
}

fn set_up_rule_eval_test() -> Arc<dyn Queryable> {
    Arc::new(
        promqltest::loaded_storage(
            r#"
		load 1m
			metric{label_a="1",label_b="3"} 1
			metric{label_a="2",label_b="4"} 10
	"#,
        )
        .unwrap(),
    )
}

#[test]
fn test_rule_eval() {
    let storage = set_up_rule_eval_test();
    let ng = test_engine();
    for scenario in rule_eval_test_scenarios() {
        let rule = RecordingRule::new("test_rule", scenario.expr, scenario.rule_labels);
        let result = rule
            .eval(&Context::background(), Duration::ZERO, rule_evaluation_time(), &engine_query_func(ng.clone(), storage.clone()), None, 0)
            .unwrap_or_else(|e| panic!("{}: {e}", scenario.name));
        assert_eq!(scenario.expected, result, "{}", scenario.name);
    }
}

#[test]
fn test_rule_eval_duplicate() {
    let storage: Arc<dyn Queryable> = Arc::new(TestStorage::new());
    let engine = Arc::new(promqltest::new_test_engine_with_opts(EngineOpts {
        max_samples: 10,
        timeout: Duration::from_secs(10),
        ..Default::default()
    }));
    let now = Time::now();
    let expr = parse(r#"vector(0) or label_replace(vector(0),"test","x","","")"#);
    let rule = RecordingRule::new("foo", expr, labels::from_strings(&["test", "test"]));
    let err = rule.eval(&Context::background(), Duration::ZERO, now, &engine_query_func(engine, storage), None, 0).unwrap_err();
    assert!(err.is(&ERR_DUPLICATE_RECORDING_LABEL_SET), "{err}");
}

#[test]
fn test_recording_rule_limit() {
    let storage: Arc<dyn Queryable> = Arc::new(
        promqltest::loaded_storage(
            r#"
		load 1m
			metric{label="1"} 1
			metric{label="2"} 1
	"#,
        )
        .unwrap(),
    );
    let tests: &[(i64, &str)] = &[(0, ""), (-1, ""), (2, ""), (1, "exceeded limit of 1 with 2 series")];
    let rule = RecordingRule::new("foo", parse("metric > 0"), labels::from_strings(&["test", "test"]));
    let ng = test_engine();
    let eval_time = Time::unix(0, 0);
    for &(limit, err) in tests {
        match rule.eval(&Context::background(), Duration::ZERO, eval_time, &engine_query_func(ng.clone(), storage.clone()), None, limit) {
            Err(e) => assert_eq!(e.to_string(), err),
            Ok(_) => assert!(err.is_empty(), "Expected error {err}, got none"),
        }
    }
}

#[test]
fn test_recording_eval_with_origin() {
    let now = Time::now();
    let name = "my-recording-rule";
    let query = r#"count(metric{foo="bar"})"#;
    let detail: Arc<Mutex<RuleDetail>> = Arc::default();
    let lbs = labels::from_strings(&["foo", "bar"]);
    let expr = test_parser().parse_expr(query).unwrap();
    let rule = RecordingRule::new(name, expr, lbs);
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
fn test_recording_rule_set_dependent_rules() {
    let dependent_rule: RuleRef = Arc::new(RecordingRule::new("test1", None, Labels::empty()));
    let rule = RecordingRule::new("1", number(1.0), Labels::empty());
    assert!(!rule.no_dependent_rules());

    rule.set_dependent_rules(std::slice::from_ref(&dependent_rule));
    assert!(!rule.no_dependent_rules());
    assert!(same(&[dependent_rule.clone()], &rule.dependent_rules()));

    rule.set_dependent_rules(&[]);
    assert!(rule.no_dependent_rules());
    assert!(rule.dependent_rules().is_empty());
}

#[test]
fn test_recording_rule_set_dependency_rules() {
    let dependency_rule: RuleRef = Arc::new(RecordingRule::new("test1", None, Labels::empty()));
    let rule = RecordingRule::new("1", number(1.0), Labels::empty());
    assert!(!rule.no_dependency_rules());

    rule.set_dependency_rules(std::slice::from_ref(&dependency_rule));
    assert!(!rule.no_dependency_rules());
    assert!(same(&[dependency_rule.clone()], &rule.dependency_rules()));

    rule.set_dependency_rules(&[]);
    assert!(rule.no_dependency_rules());
    assert!(rule.dependency_rules().is_empty());
}
