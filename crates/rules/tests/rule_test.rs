//! Port of upstream/rules/rule_test.go.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use model::labels::Labels;
use promql::parser;
use rules::log::Logger;
use rules::{AlertingRule, RecordingRule, RuleRef};

#[test]
fn test_rule_log_value() {
    // adapted: Go checks `slog.LogValuer` output (and JSON marshaling of the rule) through promslog in four
    // placements and two formats, and nil rule pointers. The Rust logger takes string attributes: the rule's log
    // value is `Rule::string()`, checked here directly and through a logger with the attribute given directly
    // and with `with`. Nil rule pointers do not exist in Rust.
    let expr = parser::Parser::new(parser::Options::default()).parse_expr("up").unwrap();
    let cases: Vec<(&str, RuleRef, &str)> = vec![
        ("recording", Arc::new(RecordingRule::new("recorded_metric", expr.clone(), Labels::empty())), "record: recorded_metric\nexpr: up\n"),
        (
            "alerting",
            Arc::new(AlertingRule::new(
                "TestAlert",
                expr.clone(),
                Duration::ZERO,
                Duration::ZERO,
                Labels::empty(),
                Labels::empty(),
                Labels::empty(),
                "",
                false,
                Logger::nop(),
            )),
            "alert: TestAlert\nexpr: up\n",
        ),
    ];
    for (name, rule, want) in cases {
        assert_eq!(want, rule.string(), "{name}");
        for placement in ["direct", "with"] {
            let out: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
            let o = out.clone();
            let logger = Logger::new(Arc::new(move |_l, _m: &str, kv: &[(String, String)]| o.lock().unwrap().extend(kv.iter().cloned())));
            match placement {
                "direct" => logger.info("test", &[("rule", &rule.string())]),
                _ => logger.with(&[("rule", &rule.string())]).info("test", &[]),
            }
            let got = out.lock().unwrap();
            let v = got.iter().find(|(k, _)| k == "rule").map(|(_, v)| v.as_str());
            assert_eq!(Some(want), v, "{name}/{placement}");
        }
    }
}
