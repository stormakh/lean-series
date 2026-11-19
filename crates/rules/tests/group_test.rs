//! Port of upstream/rules/group_test.go.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use model::labels::Labels;
use promql::parser;
use rules::log::{Level, Logger};
use rules::{AlertingRule, Group, GroupOptions, ManagerOptions, QueryFunc, RecordingRule, RuleRef, Time};
use storage::{Context, Error};

#[test]
fn test_group_eval_json_logger_rule() {
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
        // adapted: Go logs JSON with promslog; the Rust logger hands records to a sink, which captures the "rule"
        // attribute of the record.
        let output: Arc<Mutex<Vec<(Level, String, Vec<(String, String)>)>>> = Arc::default();
        let o = output.clone();
        let logger = Logger::new(Arc::new(move |l, m: &str, kv: &[(String, String)]| o.lock().unwrap().push((l, m.to_string(), kv.to_vec()))));

        let query_err = Error::msg("query failed");
        let qe = query_err.clone();
        let qf: QueryFunc = Arc::new(move |_: &Context, _: &str, _: Time| Err(qe.clone()));
        let group = Group::new(GroupOptions {
            name: "test-group".into(),
            file: "test.rules".into(),
            interval: Duration::from_secs(60),
            rules: vec![rule.clone()],
            opts: Some(Arc::new(ManagerOptions { logger, query_func: Some(qf), ..Default::default() })),
            ..Default::default()
        });

        group.eval(&Context::background(), Time::unix(0, 0));
        let last = rule.last_error().unwrap_or_else(|| panic!("{name}: no last error"));
        assert!(last.is(&query_err), "{name}: {last}");

        let out = output.lock().unwrap();
        assert_eq!(out.len(), 1, "{name}: one record");
        let entry_rule = out[0].2.iter().find(|(k, _)| k == "rule").map(|(_, v)| v.clone()).unwrap();
        assert_eq!(want, entry_rule, "{name}");
    }
}

fn group(query_offset: Option<Duration>) -> Group {
    Group::new(GroupOptions {
        name: "group-1".into(),
        file: "file-1".into(),
        interval: Duration::from_secs(60),
        query_offset,
        ..Default::default()
    })
}

#[test]
fn test_group_equals() {
    let m = Duration::from_secs(60);
    let tests: Vec<(&str, Group, Group, bool)> = vec![
        ("no query offset set on both groups", group(None), group(None), true),
        ("query offset set only on the first group", group(Some(m)), group(None), false),
        ("query offset set on both groups to the same value", group(Some(m)), group(Some(m)), true),
        ("query offset set on both groups to different value", group(Some(m)), group(Some(2 * m)), false),
    ];
    for (name, first, second, expected) in tests {
        assert_eq!(expected, first.equals(&second), "{name}");
        assert_eq!(expected, second.equals(&first), "{name}");
    }
}
