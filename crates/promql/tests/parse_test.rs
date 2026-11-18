// Port of promql/parser/parse_test.go. The big tables (testExpr, testSeries) are read from fixtures that
// scripts/gen/promql_parser_fixtures.sh dumps from the Go test tables.

mod common;

use common::*;
use json::Json;
use promql::parser::*;

fn opts_parser() -> Parser {
    Parser::new(Options { enable_experimental_functions: true, ..Default::default() })
}

fn readable(s: &str) -> String {
    if s.len() < 40 {
        return s.to_string();
    }
    let mut end = 40;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &s[..end])
}

#[test]
fn test_parse_expressions() {
    // adapted: one case nests 20000 binary expressions. Go stacks grow as needed; a Rust thread needs a stack
    // that is big enough for the recursive passes (type check, printing, drop).
    std::thread::Builder::new().stack_size(1 << 30).spawn(parse_expressions).unwrap().join().unwrap();
}

fn parse_expressions() {
    let p = opts_parser();
    let cases = fixture("parse_test_expr.json");
    let cases = cases.as_arr().expect("array");
    let mut failures = Vec::new();
    let mut skipped = Vec::new();
    for case in cases {
        let input = match fixture_str(case.get("input").unwrap()) {
            Ok(s) => s,
            Err(bytes) => {
                // adapted: the Go input holds bytes that are not valid UTF-8, which a Rust &str cannot hold.
                skipped.push(String::from_utf8_lossy(&bytes).into_owned());
                continue;
            }
        };
        let fail = case.get("fail").and_then(|f| f.as_bool()).unwrap_or(false);
        let (expr, err) = p.parse_expr_partial(&input);
        let name = readable(&input);

        // Unexpected errors are always caused by a bug.
        if err == Some(Error::Unexpected) {
            failures.push(format!("{}: unexpected error occurred", name));
            continue;
        }

        if !fail {
            if let Some(e) = err {
                failures.push(format!("{}: unexpected error: {}", name, e));
                continue;
            }
            let want = lossy(case.get("expected").unwrap());
            let got = dump_expr(expr.as_ref().expect("expr"));
            if let Some(d) = json_diff(&want, &got, "expr") {
                failures.push(format!("{}: {}", name, d));
            }
            continue;
        }

        let errs = match err {
            Some(Error::Parse(errs)) => errs,
            Some(other) => {
                failures.push(format!("{}: unexpected error type: {:?}", name, other));
                continue;
            }
            None => {
                failures.push(format!("{}: expected an error", name));
                continue;
            }
        };
        let want = lossy(case.get("errors").unwrap());
        let got = Json::Arr(errs.0.iter().map(|e| dump_parse_err(e, &input)).collect());
        if let Some(d) = json_diff(&want, &got, "errors") {
            failures.push(format!("{}: {}", name, d));
        }

        // A failed parse may still return a partially-built AST. It must be printable.
        // (A Rust AST cannot hold a nil label matcher, which Go also checks here.)
        if let Some(expr) = &expr {
            let printed = std::panic::catch_unwind(|| expr.to_string());
            match printed {
                Err(_) => failures.push(format!("{}: String() panicked on partial AST", name)),
                Ok(got) => {
                    let want = fixture_str(case.get("printed").unwrap()).unwrap();
                    if !want.is_empty() && want != got {
                        failures.push(format!(
                            "{}: unexpected String() of partial AST: want {:?} got {:?}",
                            name, want, got
                        ));
                    }
                }
            }
        }

        for e in &errs.0 {
            let pr = e.position_range;
            if !(0 <= pr.start && pr.start <= pr.end && pr.end as usize <= input.len()) {
                failures.push(format!("{}: bad error position {:?}: {}", name, pr, e));
            }
        }
    }
    assert!(skipped.len() <= 2, "skipped {:?}", skipped);
    assert!(failures.is_empty(), "{} of {} cases failed:\n{}", failures.len(), cases.len(), failures.join("\n"));
}

#[test]
fn test_duration_expr_position_range() {
    let cases = [
        ("foo[min_of(1m, 2m)]", "min_of(1m, 2m)"),
        ("foo[max_of(1m, 2m):]", "max_of(1m, 2m)"),
        ("foo[1h:min_of(1m, 2m)]", "min_of(1m, 2m)"),
        ("foo offset min_of(1m, 2m)", "min_of(1m, 2m)"),
        ("foo offset +min_of(1m, 2m)", "+min_of(1m, 2m)"),
        ("foo offset -min_of(1m, 2m)", "-min_of(1m, 2m)"),
        ("foo offset +max_of(1m, 2m)", "+max_of(1m, 2m)"),
        ("foo offset -max_of(1m, 2m)", "-max_of(1m, 2m)"),
        ("foo[min_of(1m, max_of(2m, 3m)) + 1m]", "min_of(1m, max_of(2m, 3m)) + 1m"),
        ("foo[1m + max_of(2m, 3m)]", "1m + max_of(2m, 3m)"),
        ("foo[1m + 2m]", "1m + 2m"),
        ("foo offset -step()", "-step()"),
        ("foo[range()]", "range()"),
    ];
    let p = Parser::default();
    for (input, span) in cases {
        let expr = p.parse_expr(input).unwrap_or_else(|e| panic!("{}: {}", input, e));
        let duration = match &expr {
            Expr::MatrixSelector(m) => m.range_expr.as_deref(),
            Expr::Subquery(s) => s.step_expr.as_deref().or(s.range_expr.as_deref()),
            Expr::VectorSelector(v) => v.original_offset_expr.as_deref(),
            _ => None,
        };
        let duration = duration.unwrap_or_else(|| panic!("{}: no duration expression", input));
        let start = input.find(span).unwrap() as Pos;
        assert_eq!(PositionRange { start, end: start + span.len() as Pos }, duration.position_range(), "{}", input);
    }
}

#[test]
fn test_parse_series_desc() {
    struct Case {
        name: &'static str,
        input: &'static str,
        expected_labels: Vec<&'static str>,
        expected_values: Vec<SequenceValue>,
        expect_error: &'static str,
    }
    let sv = |v: f64| SequenceValue { value: v, ..Default::default() };
    let cases = vec![
        Case { name: "empty string", input: "", expected_labels: vec![], expected_values: vec![], expect_error: "" },
        Case {
            name: "simple line",
            input: r#"http_requests{job="api-server", instance="0", group="production"}"#,
            expected_labels: vec![
                "__name__",
                "http_requests",
                "group",
                "production",
                "instance",
                "0",
                "job",
                "api-server",
            ],
            expected_values: vec![],
            expect_error: "",
        },
        Case {
            name: "label name characters that require quoting",
            input: "{\"http.requests\", \"service.name\"=\"api-server\", instance=\"0\", group=\"canary\"}\t\t0+50x2",
            expected_labels: vec![
                "__name__",
                "http.requests",
                "group",
                "canary",
                "instance",
                "0",
                "service.name",
                "api-server",
            ],
            expected_values: vec![sv(0.0), sv(50.0), sv(100.0)],
            expect_error: "",
        },
        Case {
            name: "confirm failure on junk after identifier",
            input: "{\"http.requests\"xx}\t\t0+50x2",
            expected_labels: vec![],
            expected_values: vec![],
            expect_error: r#"parse error: unexpected identifier "xx" in label set, expected "," or "}""#,
        },
        Case {
            name: "confirm failure on bare operator after identifier",
            input: "{\"http.requests\"=, x=\"y\"}\t\t0+50x2",
            expected_labels: vec![],
            expected_values: vec![],
            expect_error: r#"parse error: unexpected "," in label set, expected string"#,
        },
        Case {
            name: "confirm failure on unterminated string identifier",
            input: "{\"http.requests}\t\t0+50x2",
            expected_labels: vec![],
            expected_values: vec![],
            expect_error: "parse error: unterminated quoted string",
        },
    ];
    let p = Parser::default();
    for tc in cases {
        let res = p.parse_series_desc(tc.input);
        if !tc.expect_error.is_empty() {
            let err = res.err().unwrap_or_else(|| panic!("{}: expected error", tc.name));
            assert!(err.to_string().contains(tc.expect_error), "{}: {}", tc.name, err);
        } else {
            let (l, v) = res.unwrap_or_else(|e| panic!("{}: {}", tc.name, e));
            assert!(model::labels::equal(&model::labels::from_strings(&tc.expected_labels), &l), "{}", tc.name);
            assert_eq!(tc.expected_values, v, "{}", tc.name);
        }
    }
}

// NaN has no equality. Thus, we need a separate test for it.
#[test]
fn test_na_n_expression() {
    let expr = Parser::default().parse_expr("NaN").unwrap();
    match expr {
        Expr::NumberLiteral(nl) => assert!(nl.val.is_nan(), "expected 'NaN' in number literal but got {}", nl.val),
        other => panic!("expected number literal but got {:?}", other),
    }
}

/// Go `newSeq`, with `none` (math.SmallestNonzeroFloat64) as an omitted value.
fn seq_from_fixture(j: &Json) -> Vec<SequenceValue> {
    j.as_arr()
        .unwrap()
        .iter()
        .map(|v| {
            let val: f64 = v.get("Value").unwrap().as_str().unwrap().parse().unwrap();
            assert!(v.get("Histogram").unwrap().is_null());
            SequenceValue {
                value: val,
                omitted: v.get("Omitted").unwrap().as_bool().unwrap(),
                histogram: None,
                counter_reset_hint_set: v.get("CounterResetHintSet").unwrap().as_bool().unwrap(),
            }
        })
        .collect()
}

#[test]
fn test_parse_series() {
    let p = Parser::default();
    let cases = fixture("parse_test_series.json");
    for case in cases.as_arr().unwrap() {
        let input = fixture_str(case.get("input").unwrap()).unwrap();
        let res = p.parse_series_desc(&input);
        // Unexpected errors are always caused by a bug.
        assert!(!matches!(res, Err(Error::Unexpected)), "unexpected error occurred");
        if !case.get("fail").unwrap().as_bool().unwrap() {
            let (metric, vals) = res.unwrap_or_else(|e| panic!("error on input '{}': {}", input, e));
            let want: Vec<String> = case
                .get("expectedMetric")
                .unwrap()
                .as_arr()
                .unwrap()
                .iter()
                .flat_map(|pair| pair.as_arr().unwrap().iter().map(|s| s.as_str().unwrap().to_string()))
                .collect();
            assert!(
                model::labels::equal(&model::labels::from_strings(&want), &metric),
                "error on input '{}': {:?}",
                input,
                metric
            );
            assert_eq!(seq_from_fixture(case.get("expectedValues").unwrap()), vals, "error in input '{}'", input);
        } else {
            assert!(res.is_err(), "input '{}' should fail", input);
        }
    }
}

fn span(offset: i32, length: u32) -> model::histogram::Span {
    model::histogram::Span { offset, length }
}

#[test]
fn test_parse_histogram_series() {
    use model::histogram::{CounterResetHint, FloatHistogram};
    struct Case {
        name: &'static str,
        input: &'static str,
        expected: Vec<FloatHistogram>,
        expected_error: &'static str,
    }
    let fh = |schema: i32, pb: &[f64], ps: Vec<model::histogram::Span>| FloatHistogram {
        schema,
        positive_buckets: pb.to_vec(),
        positive_spans: ps,
        ..Default::default()
    };
    let all = |sum: f64, count: f64, zc: f64, zt: f64, pb: &[f64], po: i32, nb: &[f64], no: i32| FloatHistogram {
        schema: 1,
        sum,
        count,
        zero_count: zc,
        zero_threshold: zt,
        positive_buckets: pb.to_vec(),
        positive_spans: vec![span(po, pb.len() as u32)],
        negative_buckets: nb.to_vec(),
        negative_spans: vec![span(no, nb.len() as u32)],
        counter_reset_hint: CounterResetHint::GaugeType,
        ..Default::default()
    };
    let sum = |s: f64| FloatHistogram { sum: s, ..Default::default() };
    let hint = |h: CounterResetHint| FloatHistogram { counter_reset_hint: h, ..Default::default() };
    let cases = vec![
        Case {
            name: "empty histogram",
            input: "{} {{}}",
            expected: vec![FloatHistogram::default()],
            expected_error: "",
        },
        Case {
            name: "empty histogram with space",
            input: "{} {{ }}",
            expected: vec![FloatHistogram::default()],
            expected_error: "",
        },
        Case {
            name: "all properties used",
            input: "{} {{schema:1 sum:0.3 count:3.1 z_bucket:7.1 z_bucket_w:0.05 buckets:[5.1 10 7] offset:3 n_buckets:[4.1 5] n_offset:5 counter_reset_hint:gauge}}",
            expected: vec![all(0.3, 3.1, 7.1, 0.05, &[5.1, 10.0, 7.0], 3, &[4.1, 5.0], 5)],
            expected_error: "",
        },
        Case {
            name: "all properties used - with spaces",
            input: "{} {{schema:1  sum:0.3  count:3  z_bucket:7 z_bucket_w:5 buckets:[5 10  7  ] offset:-3  n_buckets:[4 5]  n_offset:5  counter_reset_hint:gauge  }}",
            expected: vec![all(0.3, 3.0, 7.0, 5.0, &[5.0, 10.0, 7.0], -3, &[4.0, 5.0], 5)],
            expected_error: "",
        },
        Case {
            name: "all properties used, with negative values where supported",
            input: "{} {{schema:1 sum:-0.3 count:-3.1 z_bucket:-7.1 z_bucket_w:0.05 buckets:[-5.1 -10 -7] offset:-3 n_buckets:[-4.1 -5] n_offset:-5 counter_reset_hint:gauge}}",
            expected: vec![all(-0.3, -3.1, -7.1, 0.05, &[-5.1, -10.0, -7.0], -3, &[-4.1, -5.0], -5)],
            expected_error: "",
        },
        Case {
            name: "static series",
            input: "{} {{buckets:[5 10 7] schema:1}}x2",
            expected: vec![
                fh(1, &[5.0, 10.0, 7.0], vec![span(0, 3)]),
                fh(1, &[5.0, 10.0, 7.0], vec![span(0, 3)]),
                fh(1, &[5.0, 10.0, 7.0], vec![span(0, 3)]),
            ],
            expected_error: "",
        },
        Case {
            name: "static series - x0",
            input: "{} {{buckets:[5 10 7] schema:1}}x0",
            expected: vec![fh(1, &[5.0, 10.0, 7.0], vec![span(0, 3)])],
            expected_error: "",
        },
        Case {
            name: "2 histograms stated explicitly",
            input: "{} {{buckets:[5 10 7] schema:1}} {{buckets:[1 2 3] schema:1}}",
            expected: vec![fh(1, &[5.0, 10.0, 7.0], vec![span(0, 3)]), fh(1, &[1.0, 2.0, 3.0], vec![span(0, 3)])],
            expected_error: "",
        },
        Case {
            name: "series with increment - with different schemas",
            input: "{} {{buckets:[5] schema:0}}+{{buckets:[1 2] schema:1}}x2",
            expected: vec![
                fh(0, &[5.0], vec![span(0, 1)]),
                fh(0, &[6.0, 2.0], vec![span(0, 2)]),
                fh(0, &[7.0, 4.0], vec![span(0, 2)]),
            ],
            expected_error: "",
        },
        Case {
            name: "series with two different increments",
            input: "{} {{sum:1}}+{{sum:1}}x2 {{sum:2}}+{{sum:2}}x2",
            expected: vec![sum(1.0), sum(2.0), sum(3.0), sum(2.0), sum(4.0), sum(6.0)],
            expected_error: "",
        },
        Case {
            name: "series with decrement",
            input: "{} {{buckets:[5 10 7] schema:1}}-{{buckets:[1 2 3] schema:1}}x2",
            expected: vec![
                fh(1, &[5.0, 10.0, 7.0], vec![span(0, 3)]),
                fh(1, &[4.0, 8.0, 4.0], vec![span(0, 3)]),
                fh(1, &[3.0, 6.0, 1.0], vec![span(0, 3)]),
            ],
            expected_error: "",
        },
        Case {
            name: "series with increment - 0x",
            input: "{} {{buckets:[5 10 7] schema:1}}+{{buckets:[1 2 3] schema:1}}x0",
            expected: vec![fh(1, &[5.0, 10.0, 7.0], vec![span(0, 3)])],
            expected_error: "",
        },
        Case {
            name: "series with different schemas - second one is smaller",
            input: "{} {{buckets:[5 10 7] schema:1}}+{{buckets:[1 2 3] schema:0}}x2",
            expected: vec![],
            expected_error: "1:63: parse error: error combining histograms: cannot merge from schema 0 to 1",
        },
        Case {
            name: "different order",
            input: "{} {{buckets:[5 10 7] schema:1}}",
            expected: vec![fh(1, &[5.0, 10.0, 7.0], vec![span(0, 3)])],
            expected_error: "",
        },
        Case {
            name: "double property",
            input: "{} {{schema:1 schema:1}}",
            expected: vec![],
            expected_error: "1:1: parse error: duplicate key \"schema\" in histogram",
        },
        Case {
            name: "unknown property",
            input: "{} {{foo:1}}",
            expected: vec![],
            expected_error: "1:6: parse error: bad histogram descriptor found: \"foo\"",
        },
        Case {
            name: "space before :",
            input: "{} {{schema :1}}",
            expected: vec![],
            expected_error: "1:6: parse error: missing `:` for histogram descriptor",
        },
        Case {
            name: "space after :",
            input: "{} {{schema: 1}}",
            expected: vec![],
            expected_error: "1:13: parse error: unexpected \" \" in series values",
        },
        Case {
            name: "space after [",
            input: "{} {{buckets:[ 1]}}",
            expected: vec![],
            expected_error: "1:15: parse error: unexpected \" \" in series values",
        },
        Case {
            name: "space after {{",
            input: "{} {{ schema:1}}",
            expected: vec![],
            expected_error: "1:7: parse error: unexpected \"<Item 57372>\" \"schema\" in series values",
        },
        Case {
            name: "invalid counter reset hint value",
            input: "{} {{counter_reset_hint:foo}}",
            expected: vec![],
            expected_error: "1:25: parse error: bad histogram descriptor found: \"foo\"",
        },
        Case {
            name: "'unknown' counter reset hint value",
            input: "{} {{counter_reset_hint:unknown}}",
            expected: vec![hint(CounterResetHint::UnknownCounterReset)],
            expected_error: "",
        },
        Case {
            name: "'reset' counter reset hint value",
            input: "{} {{counter_reset_hint:reset}}",
            expected: vec![hint(CounterResetHint::CounterReset)],
            expected_error: "",
        },
        Case {
            name: "'not_reset' counter reset hint value",
            input: "{} {{counter_reset_hint:not_reset}}",
            expected: vec![hint(CounterResetHint::NotCounterReset)],
            expected_error: "",
        },
        Case {
            name: "'gauge' counter reset hint value",
            input: "{} {{counter_reset_hint:gauge}}",
            expected: vec![hint(CounterResetHint::GaugeType)],
            expected_error: "",
        },
    ];
    let p = Parser::default();
    for tc in cases {
        let res = p.parse_series_desc(tc.input);
        if !tc.expected_error.is_empty() {
            match res {
                Err(e) => assert_eq!(tc.expected_error, e.to_string(), "{}", tc.name),
                Ok(_) => panic!("{}: expected error", tc.name),
            }
            continue;
        }
        let (_, vals) = res.unwrap_or_else(|e| panic!("{}: {}", tc.name, e));
        let got: Vec<FloatHistogram> = vals.into_iter().map(|v| *v.histogram.expect("histogram")).collect();
        assert_eq!(tc.expected, got, "{}", tc.name);
    }
}

#[test]
fn test_histogram_test_expression() {
    use model::histogram::{CounterResetHint, FloatHistogram};
    let cases = vec![
        (
            "single positive and negative span",
            FloatHistogram {
                schema: 1,
                sum: -0.3,
                count: 3.1,
                zero_count: 7.1,
                zero_threshold: 0.05,
                positive_buckets: vec![5.1, 10.0, 7.0],
                positive_spans: vec![span(-3, 3)],
                negative_buckets: vec![4.1, 5.0],
                negative_spans: vec![span(-5, 2)],
                ..Default::default()
            },
            "{{schema:1 count:3.1 sum:-0.3 z_bucket:7.1 z_bucket_w:0.05 offset:-3 buckets:[5.1 10 7] n_offset:-5 n_buckets:[4.1 5]}}",
        ),
        (
            "multiple positive and negative spans",
            FloatHistogram {
                positive_buckets: vec![5.1, 10.0, 7.0],
                positive_spans: vec![span(-3, 1), span(4, 2)],
                negative_buckets: vec![4.1, 5.0, 7.0, 8.0, 9.0],
                negative_spans: vec![span(-1, 2), span(2, 3)],
                ..Default::default()
            },
            "{{offset:-3 buckets:[5.1 0 0 0 0 10 7] n_offset:-1 n_buckets:[4.1 5 0 0 7 8 9]}}",
        ),
        (
            "known counter reset hint",
            FloatHistogram {
                schema: 1,
                sum: -0.3,
                count: 3.1,
                zero_count: 7.1,
                zero_threshold: 0.05,
                positive_buckets: vec![5.1, 10.0, 7.0],
                positive_spans: vec![span(-3, 3)],
                negative_buckets: vec![4.1, 5.0],
                negative_spans: vec![span(-5, 2)],
                counter_reset_hint: CounterResetHint::CounterReset,
                ..Default::default()
            },
            "{{schema:1 count:3.1 sum:-0.3 z_bucket:7.1 z_bucket_w:0.05 counter_reset_hint:reset offset:-3 buckets:[5.1 10 7] n_offset:-5 n_buckets:[4.1 5]}}",
        ),
    ];
    let p = Parser::default();
    for (name, input, expected) in cases {
        let expression = input.test_expression();
        assert_eq!(expected, expression, "{}", name);
        let (_, vals) =
            p.parse_series_desc(&format!("{{}} {}", expression)).unwrap_or_else(|e| panic!("{}: {}", name, e));
        assert_eq!(1, vals.len(), "{}", name);
        let canonical = vals[0].histogram.as_ref().expect("histogram");
        assert_eq!(expected, canonical.test_expression(), "{}", name);
    }
}

#[test]
fn test_recover_parser_runtime() {
    // adapted: Go causes a runtime panic (index out of range) inside the deferred recover.
    let res: Result<(), Error> = recover(|| {
        let a: Vec<i32> = Vec::new();
        let i = a.len() + 123;
        #[allow(unconditional_panic)]
        let _ = a[i];
        Ok(())
    });
    assert_eq!(Err(Error::Unexpected), res);
}

#[test]
fn test_recover_parser_error() {
    let e = Error::Custom("custom error".to_string());
    let res: Result<(), Error> = recover(|| std::panic::panic_any(e.clone()));
    assert_eq!(e.to_string(), res.unwrap_err().to_string());
}

#[test]
fn test_extract_selectors() {
    let cases: Vec<(&str, Vec<&str>)> = vec![
        ("foo", vec![r#"{__name__="foo"}"#]),
        (r#"foo{bar="baz"}"#, vec![r#"{bar="baz", __name__="foo"}"#]),
        (
            r#"foo{bar="baz"} / flip{flop="flap"}"#,
            vec![r#"{bar="baz", __name__="foo"}"#, r#"{flop="flap", __name__="flip"}"#],
        ),
        ("rate(foo[5m])", vec![r#"{__name__="foo"}"#]),
        ("vector(1)", vec![]),
    ];
    let p = Parser::default();
    for (input, expected) in cases {
        let expr = p.parse_expr(input).unwrap();
        let mut want = Vec::new();
        for s in expected {
            want.push(p.parse_metric_selector(s).unwrap());
        }
        assert_eq!(want, extract_selectors(&expr), "{}", input);
    }
}

#[test]
fn test_parse_custom_functions() {
    use std::sync::Arc;
    let custom_func = Arc::new(Function::new("custom_func", vec![ValueType::Matrix], 0, ValueType::Vector, false));
    let mut with_custom_func = functions().clone();
    with_custom_func.insert(custom_func.name.to_string(), custom_func.clone());

    // custom function is parsed
    let expr = Parser::new(Options { functions: Some(with_custom_func), ..Default::default() })
        .parse_expr("custom_func(metric[1m])")
        .unwrap();
    match expr {
        Expr::Call(call) => assert!(Arc::ptr_eq(&custom_func, &call.func)),
        other => panic!("not a call: {:?}", other),
    }

    // default parser does not accept the custom function
    let err = Parser::new(Options::default()).parse_expr("custom_func(metric[1m])").unwrap_err();
    assert!(err.to_string().contains(r#"unknown function with name "custom_func""#), "{}", err);
    assert!(!functions().contains_key("custom_func"));

    // custom functions replace the default set
    let only: FunctionMap = [(custom_func.name.to_string(), custom_func.clone())].into_iter().collect();
    let err = Parser::new(Options { functions: Some(only.clone()), ..Default::default() })
        .parse_expr("rate(metric[1m])")
        .unwrap_err();
    assert!(err.to_string().contains(r#"unknown function with name "rate""#), "{}", err);

    // empty functions map accepts no functions
    let err = Parser::new(Options { functions: Some(FunctionMap::new()), ..Default::default() })
        .parse_expr("rate(metric[1m])")
        .unwrap_err();
    assert!(err.to_string().contains(r#"unknown function with name "rate""#), "{}", err);

    // experimental custom function must be enabled
    let experimental_func =
        Arc::new(Function::new("experimental_func", vec![ValueType::Matrix], 0, ValueType::Vector, true));
    let funcs: FunctionMap = [(experimental_func.name.to_string(), experimental_func.clone())].into_iter().collect();
    let err = Parser::new(Options { functions: Some(funcs.clone()), ..Default::default() })
        .parse_expr("experimental_func(metric[1m])")
        .unwrap_err();
    assert!(err.to_string().contains(r#"function "experimental_func" is not enabled"#), "{}", err);
    Parser::new(Options { functions: Some(funcs), enable_experimental_functions: true, ..Default::default() })
        .parse_expr("experimental_func(metric[1m])")
        .unwrap();

    // changing the functions map after creating the parser has no effect
    let mut funcs = only;
    let p = Parser::new(Options { functions: Some(funcs.clone()), ..Default::default() });
    funcs.remove("custom_func");
    p.parse_expr("custom_func(metric[1m])").unwrap();
}

#[test]
fn test_new_parser() {
    let p = Parser::new(Options { enable_experimental_functions: true, ..Default::default() });

    // ParseExpr should work.
    p.parse_expr("up").unwrap();

    // ParseMetricSelector should work.
    let matchers = p.parse_metric_selector(r#"{job="prometheus"}"#).unwrap();
    assert_eq!(1, matchers.len());

    // ParseMetricSelectors should work.
    let sets = p.parse_metric_selectors(&[r#"{job="prometheus"}"#, r#"{job="grafana"}"#]).unwrap();
    assert_eq!(2, sets.len());

    // Invalid input should return errors.
    assert!(p.parse_expr("===").is_err());
}
