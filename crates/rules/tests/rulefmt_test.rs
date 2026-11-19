//! Port of upstream/model/rulefmt/rulefmt_test.go.
use std::sync::{Arc, Mutex};

use model::labels::ValidationScheme;
use promql::parser::{Options, Parser};
use rules::log::Logger;
use rules::rulefmt::{self, chain_any, Error, Msg, Pos, WrappedError};

fn testdata(f: &str) -> String {
    format!("{}/../../upstream/model/rulefmt/testdata/{f}", env!("CARGO_MANIFEST_DIR"))
}

fn test_parser() -> Parser {
    Parser::new(Options::default())
}

fn nop() -> Logger {
    Logger::nop()
}

fn err(s: &str) -> rulefmt::BoxError {
    Box::new(Msg(s.to_string()))
}

#[test]
fn test_parse_file_success() {
    let p = test_parser();
    let (_, errs) = rulefmt::parse_file(&testdata("test.yaml"), false, ValidationScheme::Utf8, &p, &nop());
    assert!(errs.is_empty(), "unexpected errors parsing file: {errs:?}");
    let (_, errs) = rulefmt::parse_file(&testdata("utf-8_lname.good.yaml"), false, ValidationScheme::Utf8, &p, &nop());
    assert!(errs.is_empty(), "unexpected errors parsing file: {errs:?}");
    let (_, errs) = rulefmt::parse_file(&testdata("utf-8_annotation.good.yaml"), false, ValidationScheme::Utf8, &p, &nop());
    assert!(errs.is_empty(), "unexpected errors parsing file: {errs:?}");
    let (_, errs) =
        rulefmt::parse_file(&testdata("legacy_validation_annotation.good.yaml"), false, ValidationScheme::Legacy, &p, &nop());
    assert!(errs.is_empty(), "unexpected errors parsing file: {errs:?}");
}

#[test]
fn test_parse_file_success_with_aliases() {
    let expr_string = "sum without(instance) (rate(errors_total[5m]))\n/\nsum without(instance) (rate(requests_total[5m]))\n";
    let p = test_parser();
    let (rgs, errs) = rulefmt::parse_file(&testdata("test_aliases.yaml"), false, ValidationScheme::Utf8, &p, &nop());
    assert!(errs.is_empty(), "unexpected errors parsing file: {errs:?}");
    for rg in &rgs.unwrap().groups {
        assert_eq!("HighAlert", rg.rules[0].alert);
        assert_eq!("critical", rg.rules[0].labels["severity"]);
        assert_eq!("stuff's happening with {{ $.labels.service }}", rg.rules[0].annotations["description"]);

        assert_eq!("new_metric", rg.rules[1].record);

        assert_eq!("HighAlert", rg.rules[2].alert);
        assert_eq!("critical", rg.rules[2].labels["severity"]);
        assert_eq!("stuff's happening with {{ $.labels.service }}", rg.rules[2].annotations["description"]);

        assert_eq!("HighAlert2", rg.rules[3].alert);
        assert_eq!("critical", rg.rules[3].labels["severity"]);

        for rule in &rg.rules {
            assert_eq!(expr_string, rule.expr);
        }
    }
}

#[test]
fn test_parse_file_failure() {
    let cases: &[(&str, &str, ValidationScheme)] = &[
        ("duplicate_grp.bad.yaml", "groupname: \"yolo\" is repeated in the same file", ValidationScheme::Unset),
        ("bad_expr.bad.yaml", "parse error", ValidationScheme::Unset),
        ("record_and_alert.bad.yaml", "only one of 'record' and 'alert' must be set", ValidationScheme::Unset),
        ("no_rec_alert.bad.yaml", "one of 'record' or 'alert' must be set", ValidationScheme::Unset),
        ("noexpr.bad.yaml", "field 'expr' must be set in rule", ValidationScheme::Unset),
        (
            "invalid_record_name.bad.yaml",
            "braces present in the recording rule name; should it be in expr?: strawberry{flavor=\"sweet\"}",
            ValidationScheme::Unset,
        ),
        ("bad_field.bad.yaml", "field annotation not found", ValidationScheme::Unset),
        ("invalid_label_name.bad.yaml", "invalid label name", ValidationScheme::Unset),
        ("record_and_for.bad.yaml", "invalid field 'for' in recording rule", ValidationScheme::Unset),
        ("record_and_keep_firing_for.bad.yaml", "invalid field 'keep_firing_for' in recording rule", ValidationScheme::Unset),
        ("legacy_validation_annotation.bad.yaml", "invalid annotation name: ins-tance", ValidationScheme::Legacy),
    ];
    let p = test_parser();
    for &(filename, err_msg, scheme) in cases {
        let scheme = if scheme == ValidationScheme::Unset { ValidationScheme::Utf8 } else { scheme };
        let (_, errs) = rulefmt::parse_file(&testdata(filename), false, scheme, &p, &nop());
        assert!(!errs.is_empty(), "Expected error parsing {filename} but got none");
        let got = errs[0].to_string();
        assert!(got.contains(err_msg), "Expected error for {filename}: {got:?} does not contain {err_msg:?}");
    }
}

#[test]
fn test_template_parsing() {
    let tests: &[(&str, bool)] = &[
        (
            r#"
groups:
- name: example
  rules:
  - alert: InstanceDown
    expr: up == 0
    for: 5m
    labels:
      severity: "page"
    annotations:
      summary: "Instance {{ $labels.instance }} down"
"#,
            true,
        ),
        (
            r#"
groups:
- name: example
  labels:
    team: myteam
  rules:
  - alert: InstanceDown
    expr: up == 0
    for: 5m
    labels:
      severity: "page"
    annotations:
      summary: "Instance {{ $labels.instance }} down"
"#,
            true,
        ),
        (
            // `$label` instead of `$labels`.
            r#"
groups:
- name: example
  rules:
  - alert: InstanceDown
    expr: up == 0
    for: 5m
    labels:
      severity: "page"
    annotations:
      summary: "Instance {{ $label.instance }} down"
"#,
            false,
        ),
        (
            // `$this_is_wrong`.
            r#"
groups:
- name: example
  rules:
  - alert: InstanceDown
    expr: up == 0
    for: 5m
    labels:
      severity: "{{$this_is_wrong}}"
    annotations:
      summary: "Instance {{ $labels.instance }} down"
"#,
            false,
        ),
        (
            // `$labels.quantile * 100`.
            r#"
groups:
- name: example
  rules:
  - alert: InstanceDown
    expr: up == 0
    for: 5m
    labels:
      severity: "page"
    annotations:
      summary: "Instance {{ $labels.instance }} down"
      description: "{{$labels.quantile * 100}}"
"#,
            false,
        ),
    ];
    let p = test_parser();
    for &(rule_string, should_pass) in tests {
        let (rgs, errs) = rulefmt::parse(rule_string.as_bytes(), false, ValidationScheme::Utf8, &p, &nop());
        assert!(rgs.is_some(), "Rule parsing, rule=\n{rule_string}");
        let passed = (should_pass && errs.is_empty()) || (!should_pass && !errs.is_empty());
        assert!(passed, "Rule validation failed, rule=\n{rule_string}\nerrs: {errs:?}");
    }
}

#[test]
fn test_unique_error_nodes() {
    let group = r#"
groups:
- name: example
  rules:
  - alert: InstanceDown
    expr: up ===== 0
    for: 5m
    labels:
      severity: "page"
    annotations:
      summary: "Instance {{ $labels.instance }} down"
  - alert: InstanceUp
    expr: up ===== 1
    for: 5m
    labels:
      severity: "page"
    annotations:
      summary: "Instance {{ $labels.instance }} up"
"#;
    let p = test_parser();
    let (_, errs) = rulefmt::parse(group.as_bytes(), false, ValidationScheme::Utf8, &p, &nop());
    assert_eq!(errs.len(), 2, "Expected two errors");
    let err00 = errs[0].downcast_ref::<Error>().expect("errors[0] is a rulefmt.Error");
    let err0 = err00.err.node;
    let err01 = errs[1].downcast_ref::<Error>().expect("errors[1] is a rulefmt.Error");
    let err1 = err01.err.node;
    assert_ne!(err0, err1, "Error nodes should not be the same");
}

#[test]
fn test_error() {
    let tests: Vec<(&str, Error, &str)> = vec![
        (
            "with alternative node provided in WrappedError",
            Error {
                group: "some group".into(),
                rule: 1,
                rule_name: "some rule name".into(),
                err: WrappedError::new(err("some error"), Some(Pos { line: 10, column: 20 }), Some(Pos { line: 11, column: 21 })),
            },
            r#"10:20: 11:21: group "some group", rule 1, "some rule name": some error"#,
        ),
        (
            "with node provided in WrappedError",
            Error {
                group: "some group".into(),
                rule: 1,
                rule_name: "some rule name".into(),
                err: WrappedError::new(err("some error"), Some(Pos { line: 10, column: 20 }), None),
            },
            r#"10:20: group "some group", rule 1, "some rule name": some error"#,
        ),
        (
            "with only err provided in WrappedError",
            Error {
                group: "some group".into(),
                rule: 1,
                rule_name: "some rule name".into(),
                err: WrappedError::new(err("some error"), None, None),
            },
            r#"group "some group", rule 1, "some rule name": some error"#,
        ),
    ];
    for (name, e, want) in tests {
        assert_eq!(e.to_string(), want, "{name}");
    }
}

#[test]
fn test_wrapped_error() {
    let tests: Vec<(&str, WrappedError, &str)> = vec![
        (
            "with alternative node provided",
            WrappedError::new(err("some error"), Some(Pos { line: 10, column: 20 }), Some(Pos { line: 11, column: 21 })),
            "10:20: 11:21: some error",
        ),
        ("with node provided", WrappedError::new(err("some error"), Some(Pos { line: 10, column: 20 }), None), "10:20: some error"),
        ("with only err provided", WrappedError::new(err("some error"), None, None), "some error"),
    ];
    for (name, e, want) in tests {
        assert_eq!(e.to_string(), want, "{name}");
    }
}

/// A distinct error value, compared by identity like Go's `errors.New` sentinels.
#[derive(Debug)]
struct Sentinel(&'static str, u32);
impl std::fmt::Display for Sentinel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Sentinel {}

#[test]
fn test_error_unwrap() {
    // err1 := errors.New("test error"); io.ErrClosedPipe.
    let tests: Vec<(Error, u32)> = vec![
        (Error { group: String::new(), rule: 0, rule_name: String::new(), err: WrappedError::new(Box::new(Sentinel("test error", 1)), None, None) }, 1),
        (
            Error {
                group: String::new(),
                rule: 0,
                rule_name: String::new(),
                err: WrappedError::new(Box::new(Sentinel("io: read/write on closed pipe", 2)), None, None),
            },
            2,
        ),
    ];
    for (wrapped, id) in tests {
        let is = chain_any(&wrapped, |e| e.downcast_ref::<Sentinel>().is_some_and(|s| s.1 == id));
        assert!(is, "{wrapped}");
        // Go errors.Is on a different sentinel fails.
        assert!(!chain_any(&wrapped, |e| e.downcast_ref::<Sentinel>().is_some_and(|s| s.1 == id + 10)));
    }
}

#[test]
fn test_multi_doc_parse() {
    let valid = r#"
groups:
- name: example
  rules:
  - alert: InstanceDown
    expr: up == 0
"#;
    let multi = r#"
groups:
- name: example
  rules:
  - alert: InstanceDown
    expr: up == 0
---
groups:
- name: example2
  rules:
  - alert: InstanceDown2
    expr: up == 0
"#;
    let multi_empty = r#"
groups:
- name: example
  rules:
  - alert: InstanceDown
    expr: up == 0
---
"#;
    let buf = Arc::new(Mutex::new(String::new()));
    let b2 = buf.clone();
    let logger = Logger::new(Arc::new(move |_lvl, m: &str, _kv: &[(String, String)]| {
        b2.lock().unwrap().push_str(m);
        b2.lock().unwrap().push('\n');
    }));
    let p = test_parser();

    let (rgs, errs) = rulefmt::parse(valid.as_bytes(), false, ValidationScheme::Utf8, &p, &logger);
    assert!(errs.is_empty());
    assert_eq!(rgs.unwrap().groups.len(), 1);
    assert!(!buf.lock().unwrap().contains("Multiple document yaml rules files are not supported"));
    buf.lock().unwrap().clear();

    let (rgs, errs) = rulefmt::parse(multi.as_bytes(), false, ValidationScheme::Utf8, &p, &logger);
    assert!(errs.is_empty());
    assert_eq!(rgs.unwrap().groups.len(), 1);
    assert!(buf.lock().unwrap().contains("Multiple document yaml rules files are not supported"));
    buf.lock().unwrap().clear();

    let (rgs, errs) = rulefmt::parse(multi_empty.as_bytes(), false, ValidationScheme::Utf8, &p, &logger);
    assert!(errs.is_empty());
    assert_eq!(rgs.unwrap().groups.len(), 1);
    assert!(buf.lock().unwrap().contains("Multiple document yaml rules files are not supported"));
}
