//! Port of cmd/promtool/unittest_test.go.

use promql::parser::{Options as ParserOptions, Parser};
use promql::promqltest::LazyLoaderOpts;
use promtool::unittest::junitxml::JUnitXml;
use promtool::unittest::{rules_unit_test, rules_unit_test_result};

/// Go `./testdata/<name>` (the Go test runs in cmd/promtool).
fn testdata(name: &str) -> String {
    format!("{}/../../upstream/cmd/promtool/testdata/{name}", env!("CARGO_MANIFEST_DIR"))
}

fn opts(at: bool, neg: bool) -> LazyLoaderOpts {
    LazyLoaderOpts { enable_at_modifier: at, enable_negative_offset: neg, ..Default::default() }
}

#[test]
fn test_rules_unit_test() {
    struct Case {
        name: &'static str,
        files: Vec<String>,
        query_opts: LazyLoaderOpts,
        parser_opts: ParserOptions,
        want: i32,
    }
    let case = |name, file: &str, query_opts, parser_opts, want| Case {
        name,
        files: vec![testdata(file)],
        query_opts,
        parser_opts,
        want,
    };
    let exp = ParserOptions { enable_experimental_functions: true, ..Default::default() };
    let tests = vec![
        case("Passing Unit Tests", "unittest.yml", opts(false, false), ParserOptions::default(), 0),
        case("Long evaluation interval", "long-period.yml", opts(false, false), ParserOptions::default(), 0),
        case("Bad input series", "bad-input-series.yml", opts(false, false), ParserOptions::default(), 1),
        case("Bad PromQL", "bad-promql.yml", opts(false, false), ParserOptions::default(), 1),
        case("Bad rules (syntax error)", "bad-rules-syntax-test.yml", opts(false, false), ParserOptions::default(), 1),
        case("Bad rules (error evaluating)", "bad-rules-error-test.yml", opts(false, false), ParserOptions::default(), 1),
        case("Simple failing test", "failing.yml", opts(false, false), ParserOptions::default(), 1),
        case("Disabled feature (@ modifier)", "at-modifier-test.yml", opts(false, false), ParserOptions::default(), 1),
        case("Enabled feature (@ modifier)", "at-modifier-test.yml", opts(true, false), ParserOptions::default(), 0),
        case("Disabled feature (negative offset)", "negative-offset-test.yml", opts(false, false), ParserOptions::default(), 1),
        case("Enabled feature (negative offset)", "negative-offset-test.yml", opts(false, true), ParserOptions::default(), 0),
        case(
            "Disabled feature (experimental functions)",
            "experimental-functions-test.yml",
            opts(false, false),
            ParserOptions::default(),
            1,
        ),
        case("Enabled feature (experimental functions)", "experimental-functions-test.yml", opts(false, false), exp, 0),
        case("No test group interval", "no-test-group-interval.yml", opts(false, true), ParserOptions::default(), 0),
        case("Start time tests", "start-time-test.yml", opts(true, false), ParserOptions::default(), 0),
    ];
    let mut reuse_files: Vec<String> = Vec::new();
    let mut reuse_count = [0usize; 2];
    // adapted: Go runs the subtests in parallel; here they run on threads.
    let mut handles = Vec::new();
    for tt in tests {
        let q = &tt.query_opts;
        let is_neg_only = q.enable_negative_offset && !q.enable_at_modifier && !q.enable_delayed_name_removal;
        let is_at_only = q.enable_at_modifier && !q.enable_negative_offset && !q.enable_delayed_name_removal;
        if q.start_time.is_none() && q.parser.is_none() && (is_neg_only || is_at_only) {
            reuse_files.extend(tt.files.iter().cloned());
            reuse_count[tt.want as usize] += tt.files.len();
        }
        handles.push(std::thread::spawn(move || {
            let got = rules_unit_test(
                tt.query_opts,
                Parser::new(tt.parser_opts),
                None,
                false,
                false,
                false,
                &tt.files,
                &mut std::io::sink(),
                &mut std::io::sink(),
            );
            if got != tt.want {
                return Err(format!("{}: RulesUnitTest() = {}, want {}", tt.name, got, tt.want));
            }
            Ok(())
        }));
    }
    let mut failures = Vec::new();
    for h in handles {
        if let Err(e) = h.join().unwrap() {
            failures.push(e);
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));

    // "Junit xml output "
    let mut buf: Vec<u8> = Vec::new();
    let got = rules_unit_test_result(
        &mut buf,
        LazyLoaderOpts::default(),
        Parser::new(ParserOptions::default()),
        None,
        false,
        false,
        false,
        &reuse_files,
        &mut std::io::sink(),
        &mut std::io::sink(),
    );
    assert_eq!(got, 1, "RulesUnitTestResults() = {got}, want 1");
    let output = String::from_utf8(buf).unwrap();
    let test = match JUnitXml::parse(&output) {
        Ok(t) => t,
        Err(err) => {
            // As in Go, a decoding error ends the subtest without failing it.
            println!("error in decoding XML: {err}");
            return;
        }
    };
    let total = test.suites.len();
    let (mut passes, mut failures, mut cases) = (0, 0, 0);
    assert_eq!(total, reuse_files.len(), "JUnit output had {} testsuite elements; expected {}", total, reuse_files.len());
    for i in &test.suites {
        if i.failure_count == 0 {
            passes += 1;
        } else {
            failures += 1;
        }
        cases += i.cases.len();
    }
    assert_eq!(
        total,
        passes + failures,
        "JUnit output mismatch: Total testsuites ({total}) does not equal the sum of passes ({passes}) and failures ({failures})."
    );
    assert!(cases >= total, "JUnit output had {} suites without test cases", total - cases);
}

#[test]
fn test_rules_unit_test_run() {
    struct Case {
        name: &'static str,
        run: Option<Vec<String>>,
        files: Vec<String>,
        want: i32,
        ignore_unknown_fields: bool,
    }
    let run = |v: &[&str]| Some(v.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    let tests = vec![
        Case { name: "Test all without run arg", run: None, files: vec![testdata("rules_run.yml")], want: 1, ignore_unknown_fields: false },
        Case {
            name: "Test all with run arg",
            run: run(&["correct", "wrong"]),
            files: vec![testdata("rules_run.yml")],
            want: 1,
            ignore_unknown_fields: false,
        },
        Case { name: "Test correct", run: run(&["correct"]), files: vec![testdata("rules_run.yml")], want: 0, ignore_unknown_fields: false },
        Case { name: "Test wrong", run: run(&["wrong"]), files: vec![testdata("rules_run.yml")], want: 1, ignore_unknown_fields: false },
        Case {
            name: "Test all with extra fields",
            run: None,
            files: vec![testdata("rules_run_extrafields.yml")],
            want: 0,
            ignore_unknown_fields: true,
        },
        Case {
            name: "Test precise floating point comparison expected failure",
            run: None,
            files: vec![testdata("rules_run_no_fuzzy.yml")],
            want: 1,
            ignore_unknown_fields: false,
        },
        Case {
            name: "Test fuzzy floating point comparison correct match",
            run: run(&["correct"]),
            files: vec![testdata("rules_run_fuzzy.yml")],
            want: 0,
            ignore_unknown_fields: false,
        },
        Case {
            name: "Test fuzzy floating point comparison wrong match",
            run: run(&["wrong"]),
            files: vec![testdata("rules_run_fuzzy.yml")],
            want: 1,
            ignore_unknown_fields: false,
        },
    ];
    // adapted: Go runs the subtests in parallel; here they run on threads.
    let handles: Vec<_> = tests
        .into_iter()
        .map(|tt| {
            std::thread::spawn(move || {
                let got = rules_unit_test(
                    LazyLoaderOpts::default(),
                    Parser::new(ParserOptions::default()),
                    tt.run.as_deref(),
                    false,
                    false,
                    tt.ignore_unknown_fields,
                    &tt.files,
                    &mut std::io::sink(),
                    &mut std::io::sink(),
                );
                (tt.name, tt.want, got)
            })
        })
        .collect();
    let mut failures = Vec::new();
    for h in handles {
        let (name, want, got) = h.join().unwrap();
        if want != got {
            failures.push(format!("{name}: want {want}, got {got}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
