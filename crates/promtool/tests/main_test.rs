//! Port of cmd/promtool/main_test.go.
//!
//! The Go tests run in `cmd/promtool` (relative testdata paths); these tests set the working directory to
//! `upstream/cmd/promtool` (the same directory for every test, so tests may run in parallel). Where Go runs the
//! test binary with `-test.main`, these tests run the promtool binary.

mod common;

use std::process::Command;
use std::sync::Once;

use common::{PROMTOOL, loaded_storage, mock_server, promtool_dir, temp_dir};
use model::labels::{Labels, ValidationScheme};
use promql::parser::{Options, Parser};
use promtool::app::{
    self, CHECK_HEALTH, CompareRuleType, LINT_OPTION_DUPLICATE_RULES, LINT_OPTION_NONE, LINT_OPTION_TOO_LONG_SCRAPE_INTERVAL,
    check_config, check_config_file, check_duplicates, check_rules_cmd, check_sd_file, check_server_status,
    new_config_lint_config, new_rules_lint_config,
};
use promtool::query::{Printer, query_instant, query_range};
use promtool::{FAILURE_EXIT_CODE, Io, LINT_ERR_EXIT_CODE, SUCCESS_EXIT_CODE};

static CWD: Once = Once::new();

fn chdir() {
    CWD.call_once(|| std::env::set_current_dir(promtool_dir()).expect("chdir"));
}

fn parser() -> Parser {
    Parser::new(Options::default())
}

fn rt() -> std::sync::Arc<dyn net::RoundTripper> {
    promtool::api::default_round_tripper()
}

fn url(s: &str) -> yaml::url::GoUrl {
    yaml::url::parse(s).expect("url")
}

/// Runs `f` with an Io over the given stdin and sink stdout/stderr.
fn with_io<T>(stdin: &[u8], f: impl FnOnce(&mut Io) -> T) -> T {
    let mut input = stdin;
    let mut out = Vec::new();
    let mut err = Vec::new();
    let mut io = Io { stdin: &mut input, stdout: &mut out, stderr: &mut err };
    f(&mut io)
}

#[test]
fn test_query_range() {
    let (s, get_request) = mock_server(200, r#"{"status": "success", "data": {"resultType": "matrix", "result": []}}"#);
    let u = url(&s);
    let p = Printer::Promql;
    let mut out = Vec::new();
    let mut err = Vec::new();
    let exit_code = query_range(&u, rt(), &Default::default(), "up", "0", "300", 0, p, &mut out, &mut err);
    assert_eq!("/api/v1/query_range", get_request().path);
    let form = get_request();
    assert_eq!("up", form.form_get("query"));
    assert_eq!("1", form.form_get("step"));
    assert_eq!(0, exit_code);

    let exit_code = query_range(&u, rt(), &Default::default(), "up", "0", "300", 10_000_000, p, &mut out, &mut err);
    assert_eq!("/api/v1/query_range", get_request().path);
    let form = get_request();
    assert_eq!("up", form.form_get("query"));
    assert_eq!("0.01", form.form_get("step"));
    assert_eq!(0, exit_code);
}

#[test]
fn test_query_instant() {
    let (s, get_request) = mock_server(200, r#"{"status": "success", "data": {"resultType": "vector", "result": []}}"#);
    let u = url(&s);
    let mut out = Vec::new();
    let mut err = Vec::new();
    let exit_code = query_instant(&u, rt(), &Default::default(), "up", "300", Printer::Promql, &mut out, &mut err);
    assert_eq!("/api/v1/query", get_request().path);
    let form = get_request();
    assert_eq!("up", form.form_get("query"));
    assert_eq!("300", form.form_get("time"));
    assert_eq!(0, exit_code);
}

#[test]
fn test_query_instant_headers() {
    let (s, get_request) = mock_server(200, r#"{"status": "success", "data": {"resultType": "vector", "result": []}}"#);
    let u = url(&s);
    let mut headers = std::collections::BTreeMap::new();
    headers.insert("X-Scope-OrgID".to_string(), "prom".to_string());
    headers.insert("X-Custom".to_string(), "value".to_string());
    let mut out = Vec::new();
    let mut err = Vec::new();
    let exit_code = query_instant(&u, rt(), &headers, "up", "300", Printer::Promql, &mut out, &mut err);
    assert_eq!(0, exit_code);
    assert_eq!("prom", get_request().header_get("X-Scope-OrgID"));
    assert_eq!("value", get_request().header_get("X-Custom"));
}

#[test]
fn test_check_server_status() {
    for (name, url_suffix) in [("no trailing slash", ""), ("trailing slash", "/")] {
        let (s, get_request) = mock_server(200, "Prometheus is Healthy.\n");
        let mut u = url(&format!("{s}{url_suffix}"));
        let mut err = Vec::new();
        let r = check_server_status(&mut u, CHECK_HEALTH, rt(), &mut err);
        assert!(r.is_ok(), "{name}: {r:?}");
        // The endpoint path must be requested verbatim regardless of a trailing slash in the user-provided URL.
        assert_eq!(CHECK_HEALTH, get_request().path, "{name}");
    }
}

#[test]
fn test_check_sd_file() {
    chdir();
    let cases = [
        ("good .yml", "./testdata/good-sd-file.yml", ""),
        ("good .yaml", "./testdata/good-sd-file.yaml", ""),
        ("good .json", "./testdata/good-sd-file.json", ""),
        ("bad file extension", "./testdata/bad-sd-file-extension.nonexistent", "invalid file extension: \".nonexistent\""),
        (
            "bad format",
            "./testdata/bad-sd-file-format.yml",
            "yaml: unmarshal errors:\n  line 1: field targats not found in type struct { Targets []string \"yaml:\\\"targets\\\"\"; Labels model.LabelSet \"yaml:\\\"labels\\\"\" }",
        ),
    ];
    for (name, file, want) in cases {
        let r = check_sd_file(file);
        if !want.is_empty() {
            assert_eq!(Some(want.to_string()), r.err(), "{name}: Expected error {want:?}");
            continue;
        }
        assert!(r.is_ok(), "{name}: {r:?}");
    }
}

fn parse_rule_file(f: &str) -> rules::rulefmt::RuleGroups {
    let (rgs, errs) =
        rules::rulefmt::parse_file(f, false, ValidationScheme::Utf8, &parser(), &discovery::Logger::nop());
    assert!(errs.is_empty(), "{errs:?}");
    rgs.unwrap()
}

#[test]
fn test_check_duplicates() {
    chdir();
    let cases: Vec<(&str, &str, Vec<CompareRuleType>)> = vec![
        ("no duplicates", "./testdata/rules.yml", vec![]),
        (
            "duplicate in other group",
            "./testdata/rules_duplicates.yml",
            vec![CompareRuleType { metric: "job:test:count_over_time1m".to_string(), label: Labels::default() }],
        ),
    ];
    for (name, rule_file, expected) in cases {
        let rgs = parse_rule_file(rule_file);
        let dups = check_duplicates(&rgs.groups);
        assert_eq!(expected, dups, "{name}");
    }
}

/// adapted: Go `BenchmarkCheckDuplicates` is a benchmark; this checks that the large rule file parses and runs
/// through `check_duplicates`.
#[test]
fn bench_check_duplicates_runs() {
    chdir();
    let rgs = parse_rule_file("./testdata/rules_large.yml");
    let _ = check_duplicates(&rgs.groups);
}

fn check_config_quiet(file: &str, syntax_only: bool) -> Result<(), String> {
    let mut out = Vec::new();
    check_config_file(false, file, syntax_only, &mut out).map(|_| ())
}

#[test]
fn test_check_target_config() {
    chdir();
    let cases = [
        ("url_in_scrape_targetgroup_with_relabel_config.good", "url_in_scrape_targetgroup_with_relabel_config.good.yml", ""),
        ("url_in_alert_targetgroup_with_relabel_config.good", "url_in_alert_targetgroup_with_relabel_config.good.yml", ""),
        (
            "url_in_scrape_targetgroup_with_relabel_config.bad",
            "url_in_scrape_targetgroup_with_relabel_config.bad.yml",
            "instance 0 in group 0: \"http://bad\" is not a valid hostname",
        ),
        (
            "url_in_alert_targetgroup_with_relabel_config.bad",
            "url_in_alert_targetgroup_with_relabel_config.bad.yml",
            "\"http://bad\" is not a valid hostname",
        ),
    ];
    for (name, file, want) in cases {
        let r = check_config_quiet(&format!("testdata/{file}"), false);
        if !want.is_empty() {
            assert_eq!(Err(want.to_string()), r, "{name}");
            continue;
        }
        assert_eq!(Ok(()), r, "{name}");
    }
}

#[test]
fn test_check_config_syntax() {
    chdir();
    let cases = [
        ("check with syntax only succeeds with nonexistent rule files", "config_with_rule_files.yml", true, ""),
        (
            "check without syntax only fails with nonexistent rule files",
            "config_with_rule_files.yml",
            false,
            "\"testdata/non-existent-file.yml\" does not point to an existing file",
        ),
        ("check with syntax only succeeds with nonexistent service discovery files", "config_with_service_discovery_files.yml", true, ""),
        // The file verification for ServiceDiscoveryConfigs doesn't fail the check if the file isn't found; it only
        // outputs a warning message.
        ("check without syntax only succeeds with nonexistent service discovery files", "config_with_service_discovery_files.yml", false, ""),
        ("check with syntax only succeeds with nonexistent TLS files", "config_with_tls_files.yml", true, ""),
        (
            "check without syntax only fails with nonexistent TLS files",
            "config_with_tls_files.yml",
            false,
            "error checking client cert file \"testdata/nonexistent_cert_file.yml\": stat testdata/nonexistent_cert_file.yml: no such file or directory",
        ),
        ("check with syntax only succeeds with nonexistent credentials file", "authorization_credentials_file.bad.yml", true, ""),
        (
            "check without syntax only fails with nonexistent credentials file",
            "authorization_credentials_file.bad.yml",
            false,
            "error checking authorization credentials or bearer token file \"/random/file/which/does/not/exist.yml\": stat /random/file/which/does/not/exist.yml: no such file or directory",
        ),
    ];
    for (name, file, syntax_only, want) in cases {
        let r = check_config_quiet(&format!("testdata/{file}"), syntax_only);
        if !want.is_empty() {
            assert_eq!(Err(want.to_string()), r, "{name}");
            continue;
        }
        assert_eq!(Ok(()), r, "{name}");
    }
}

#[test]
fn test_authorization_config() {
    chdir();
    let cases = [
        ("authorization_credentials_file.bad", "authorization_credentials_file.bad.yml", "error checking authorization credentials or bearer token file"),
        ("authorization_credentials_file.good", "authorization_credentials_file.good.yml", ""),
    ];
    for (name, file, want) in cases {
        let r = check_config_quiet(&format!("testdata/{file}"), false);
        if !want.is_empty() {
            let e = r.expect_err(name);
            assert!(e.contains(want), "{name}: Expected error to contain {want:?}, got {e:?}");
            continue;
        }
        assert_eq!(Ok(()), r, "{name}");
    }
}

#[test]
fn test_exit_codes() {
    chdir();
    let cases = [
        ("prometheus-config.good.yml", 0, false),
        // Regression test for https://github.com/prometheus/prometheus/issues/6092:
        // AWS SDs with empty region must parse without IMDS calls.
        ("prometheus-aws-sd-empty-region.good.yml", 0, false),
        ("prometheus-config.bad.yml", 1, false),
        ("prometheus-config.nonexistent.yml", 1, false),
        ("prometheus-config.lint.yml", 3, true),
    ];
    for (file, exit_code, lint_issue) in cases {
        for lint_fatal in [true, false] {
            let mut args = vec!["check".to_string(), "config".to_string(), format!("testdata/{file}")];
            if lint_fatal {
                args.push("--lint-fatal".to_string());
            }
            let status = Command::new(PROMTOOL).args(&args).current_dir(promtool_dir()).output().expect("run promtool").status;
            if exit_code == 0 || (lint_issue && !lint_fatal) {
                assert!(status.success(), "{file} lint_fatal={lint_fatal}: {status:?}");
                continue;
            }
            assert!(!status.success(), "{file} lint_fatal={lint_fatal}");
            assert_eq!(Some(exit_code), status.code(), "{file} lint_fatal={lint_fatal}");
        }
    }
}

#[test]
fn test_documentation() {
    let out = Command::new(PROMTOOL).arg("write-documentation").output().expect("run promtool");
    if !out.status.success() {
        println!("Command failed with non-zero exit code");
    }
    let generated = String::from_utf8(out.stdout).unwrap();
    let expected = std::fs::read_to_string(promtool_dir().join("../../docs/command-line/promtool.md")).unwrap();
    assert_eq!(expected, generated, "Generated content does not match documentation. Hint: run `make cli-documentation`.");
}

fn rules_lint(fatal: bool) -> app::RulesLintConfig {
    new_rules_lint_config(LINT_OPTION_DUPLICATE_RULES, fatal, false, ValidationScheme::Utf8, &mut std::io::sink())
}

#[test]
fn test_check_rules() {
    chdir();
    // rules-good
    let data = std::fs::read("./testdata/rules.yml").unwrap();
    let exit_code = with_io(&data, |io| check_rules_cmd(&rules_lint(false), &parser(), &[], io));
    assert_eq!(SUCCESS_EXIT_CODE, exit_code);
    // rules-bad
    let data = std::fs::read("./testdata/rules-bad.yml").unwrap();
    let exit_code = with_io(&data, |io| check_rules_cmd(&rules_lint(false), &parser(), &[], io));
    assert_eq!(FAILURE_EXIT_CODE, exit_code);
    // rules-lint-fatal
    let data = std::fs::read("./testdata/prometheus-rules.lint.yml").unwrap();
    let exit_code = with_io(&data, |io| check_rules_cmd(&rules_lint(true), &parser(), &[], io));
    assert_eq!(LINT_ERR_EXIT_CODE, exit_code);
}

#[test]
fn test_check_rules_with_feature_flag() {
    // As opposed to TestCheckRules calling CheckRules directly we run promtool so the feature flag parsing can be
    // tested.
    let status = Command::new(PROMTOOL)
        .args([
            "--enable-feature=promql-experimental-functions",
            "--enable-feature=promql-extended-range-selectors",
            "--enable-feature=promql-binop-fill-modifiers",
            "check",
            "rules",
            "testdata/features.yml",
        ])
        .current_dir(promtool_dir())
        .status()
        .expect("run promtool");
    assert!(status.success());
}

#[test]
fn test_check_rules_with_rule_files() {
    chdir();
    let files = |f: &str| vec![f.to_string()];
    let exit_code = with_io(b"", |io| check_rules_cmd(&rules_lint(false), &parser(), &files("./testdata/rules.yml"), io));
    assert_eq!(SUCCESS_EXIT_CODE, exit_code);
    let exit_code = with_io(b"", |io| check_rules_cmd(&rules_lint(false), &parser(), &files("./testdata/rules-bad.yml"), io));
    assert_eq!(FAILURE_EXIT_CODE, exit_code);
    let exit_code =
        with_io(b"", |io| check_rules_cmd(&rules_lint(true), &parser(), &files("./testdata/prometheus-rules.lint.yml"), io));
    assert_eq!(LINT_ERR_EXIT_CODE, exit_code);
}

#[test]
fn test_check_scrape_configs() {
    chdir();
    const MIN: i64 = 60_000_000_000;
    let file = vec!["./testdata/prometheus-config.lint.too_long_scrape_interval.yml".to_string()];
    for (name, lookback_delta, expect_error) in [
        ("scrape interval less than lookback delta", 11 * MIN, false),
        ("scrape interval greater than lookback delta", 5 * MIN, true),
        ("scrape interval same as lookback delta", 10 * MIN, true),
    ] {
        let p = parser();
        let cfg = |opts: &str, fatal: bool| {
            new_config_lint_config(opts, fatal, false, ValidationScheme::Utf8, lookback_delta, &mut std::io::sink())
        };
        // Non-fatal linting.
        let code = with_io(b"", |io| check_config(false, false, &cfg(LINT_OPTION_TOO_LONG_SCRAPE_INTERVAL, false), &p, &file, io));
        assert_eq!(SUCCESS_EXIT_CODE, code, "{name}: Non-fatal linting should return success");
        // Fatal linting.
        let code = with_io(b"", |io| check_config(false, false, &cfg(LINT_OPTION_TOO_LONG_SCRAPE_INTERVAL, true), &p, &file, io));
        if expect_error {
            assert_eq!(LINT_ERR_EXIT_CODE, code, "{name}: Fatal linting should return error");
        } else {
            assert_eq!(SUCCESS_EXIT_CODE, code, "{name}: Fatal linting should return success when there are no problems");
        }
        // Check syntax only, no linting.
        let code = with_io(b"", |io| check_config(false, true, &cfg(LINT_OPTION_TOO_LONG_SCRAPE_INTERVAL, true), &p, &file, io));
        assert_eq!(SUCCESS_EXIT_CODE, code, "{name}: Fatal linting should return success when checking syntax only");
        // Lint option "none" should disable linting.
        let opts = format!("{LINT_OPTION_NONE},{LINT_OPTION_TOO_LONG_SCRAPE_INTERVAL}");
        let code = with_io(b"", |io| check_config(false, false, &cfg(&opts, true), &p, &file, io));
        assert_eq!(SUCCESS_EXIT_CODE, code, "{name}: Fatal linting should return success when lint option \"none\" is specified");
    }
}

#[test]
fn test_tsdb_dump_command() {
    let storage = loaded_storage(
        "
	load 1m
		metric{foo=\"bar\"} 1 2 3
	",
    );
    let sandbox1 = temp_dir();
    let sandbox2 = temp_dir();
    let cases: Vec<(&str, &str, Option<&std::path::Path>)> = vec![
        ("dump", "dump", None),
        ("dump with sandbox dir root", "dump", Some(&sandbox1)),
        ("dump-openmetrics", "dump-openmetrics", None),
        ("dump-openmetrics with sandbox dir root", "dump-openmetrics", Some(&sandbox2)),
    ];
    for (name, sub_cmd, sandbox) in cases {
        // The Go test builds `sandboxDirRoot` but does not pass it to the command; neither does this port.
        let _ = sandbox;
        let status = Command::new(PROMTOOL)
            .args(["tsdb", sub_cmd, storage.dir.to_str().unwrap()])
            .stdout(std::process::Stdio::null())
            .status()
            .expect("run promtool");
        assert!(status.success(), "{name}");
    }
}

#[test]
fn test_create_blocks_from_max_block_duration() {
    // The samples are within one 18h compaction range, but either side of a day boundary.
    let input = temp_dir().join("input.om");
    std::fs::write(
        &input,
        "# TYPE http_requests_total counter
http_requests_total{code=\"200\"} 1 1624474800.000
http_requests_total{code=\"200\"} 2 1624528800.000
# EOF
",
    )
    .unwrap();
    for (name, args, expected_blocks) in [
        ("rounded down to a compaction range", vec![], 1usize),
        ("incompatible block duration allowed", vec!["--allow-incompatible-block-duration"], 2),
    ] {
        let output_dir = temp_dir();
        let mut a = vec!["tsdb", "create-blocks-from", "--max-block-duration=24h"];
        a.extend(args);
        a.extend(["openmetrics", input.to_str().unwrap(), output_dir.to_str().unwrap()]);
        let out = Command::new(PROMTOOL).args(&a).output().expect("run promtool");
        assert!(out.status.success(), "{name}: {}", String::from_utf8_lossy(&out.stderr));
        let mut db = tsdb::db::DbReadOnly::open(&output_dir, None, None).expect("open");
        let blocks = db.blocks().expect("blocks");
        assert_eq!(expected_blocks, blocks.len(), "{name}");
        db.close().expect("close");
    }
}
