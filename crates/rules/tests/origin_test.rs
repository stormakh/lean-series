//! Port of upstream/rules/origin_test.go.
use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use model::labels::{self, Labels};
use promql::parser::{Expr, NumberLiteral};
use promql::Vector;
use rules::log::Logger;
use rules::{from_origin_context, new_rule_detail, AlertingRule, QueryFunc, RecordingRule, Rule, RuleDetail, RuleHealth, RuleRef, Time};
use storage::{Context, Error};

struct UnknownRule;

impl Rule for UnknownRule {
    fn name(&self) -> &str {
        ""
    }
    fn labels(&self) -> &Labels {
        static EMPTY: std::sync::OnceLock<Labels> = std::sync::OnceLock::new();
        EMPTY.get_or_init(Labels::empty)
    }
    fn eval(&self, _: &Context, _: Duration, _: Time, _: &QueryFunc, _: Option<&yaml::url::GoUrl>, _: i64) -> Result<Vector, Error> {
        Ok(Vec::new())
    }
    fn string(&self) -> String {
        String::new()
    }
    fn query(&self) -> Option<&Expr> {
        None
    }
    fn set_last_error(&self, _: Option<Error>) {}
    fn last_error(&self) -> Option<Error> {
        None
    }
    fn set_health(&self, _: RuleHealth) {}
    fn health(&self) -> RuleHealth {
        RuleHealth::Unknown
    }
    fn set_evaluation_duration(&self, _: Duration) {}
    fn get_evaluation_duration(&self) -> Duration {
        Duration::ZERO
    }
    fn set_evaluation_timestamp(&self, _: Time) {}
    fn get_evaluation_timestamp(&self) -> Time {
        Time::ZERO
    }
    fn set_dependent_rules(&self, _: &[RuleRef]) {}
    fn no_dependent_rules(&self) -> bool {
        false
    }
    fn dependent_rules(&self) -> Vec<RuleRef> {
        Vec::new()
    }
    fn set_dependency_rules(&self, _: &[RuleRef]) {}
    fn no_dependency_rules(&self) -> bool {
        false
    }
    fn dependency_rules(&self) -> Vec<RuleRef> {
        Vec::new()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[test]
fn test_new_rule_detail_panics() {
    let r = std::panic::catch_unwind(|| new_rule_detail(&UnknownRule)).unwrap_err();
    let msg = r.downcast_ref::<String>().cloned().unwrap_or_default();
    // adapted: Go's %T prints "rules.unknownRule" (package.Type); Rust prints the type path of the test type.
    assert_eq!(msg, format!("unknown rule type \"{}\"", std::any::type_name::<UnknownRule>()));
}

#[test]
fn test_from_origin_context() {
    // should return zero value if RuleDetail is missing in the context
    let detail = from_origin_context(&Context::background());
    assert_eq!(detail, RuleDetail::default());
    // The zero value for no_dependent_rules must be the most conservative option.
    assert!(!detail.no_dependent_rules);
    // The zero value for no_dependency_rules must be the most conservative option.
    assert!(!detail.no_dependency_rules);
}

fn number(v: f64) -> Expr {
    Expr::NumberLiteral(Box::new(NumberLiteral { val: v, ..Default::default() }))
}

#[test]
fn test_new_rule_detail() {
    // should populate NoDependentRules and NoDependencyRules for a RecordingRule
    let rule = RecordingRule::new("test", number(1.0), Labels::empty());
    let detail = new_rule_detail(&rule);
    assert!(!detail.no_dependent_rules);
    assert!(!detail.no_dependency_rules);

    rule.set_dependent_rules(&[]);
    let detail = new_rule_detail(&rule);
    assert!(detail.no_dependent_rules);
    assert!(!detail.no_dependency_rules);

    rule.set_dependency_rules(&[]);
    let detail = new_rule_detail(&rule);
    assert!(detail.no_dependent_rules);
    assert!(detail.no_dependency_rules);

    // should populate NoDependentRules and NoDependencyRules for a AlertingRule
    let rule = AlertingRule::new(
        "test",
        number(1.0),
        Duration::from_secs(60),
        Duration::ZERO,
        labels::from_strings(&["test", "test"]),
        Labels::empty(),
        Labels::empty(),
        "",
        true,
        Logger::nop(),
    );
    let detail = new_rule_detail(&rule);
    assert!(!detail.no_dependent_rules);
    assert!(!detail.no_dependency_rules);

    rule.set_dependent_rules(&[]);
    let detail = new_rule_detail(&rule);
    assert!(detail.no_dependent_rules);
    assert!(!detail.no_dependency_rules);

    rule.set_dependency_rules(&[]);
    let detail = new_rule_detail(&rule);
    assert!(detail.no_dependent_rules);
    assert!(detail.no_dependency_rules);
    let _ = Arc::new(0);
}
