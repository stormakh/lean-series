// Port of promql/parser/printer_test.go. The string tables are read from a fixture that
// scripts/gen/promql_parser_fixtures.sh extracts from the Go test source.

mod common;

use common::*;
use model::labels::{METRIC_NAME, MatchType, Matcher};
use promql::parser::*;

fn rows(func: &str) -> Vec<json::Json> {
    fixture("printer_test.json").get(func).unwrap_or_else(|| panic!("no table {}", func)).as_arr().unwrap().clone()
}

fn s(row: &json::Json, k: &str) -> String {
    row.get(k).map(|v| fixture_str(v).unwrap()).unwrap_or_default()
}

#[test]
fn test_expr_string() {
    let p = Parser::new(Options { enable_binop_fill_modifiers: true, ..Default::default() });
    // A list of valid expressions that are expected to be returned as out when calling String(). If out is empty
    // the output is expected to equal the input.
    let rows = rows("TestExprString");
    assert!(rows.len() > 80);
    for row in &rows {
        let input = s(row, "in");
        let out = s(row, "out");
        let expr = p.parse_expr(&input).unwrap_or_else(|e| panic!("{}: {}", input, e));
        let exp = if out.is_empty() { input.clone() } else { out };
        assert_eq!(exp, expr.to_string(), "{}", input);
    }
}

#[test]
fn test_vector_selector_string() {
    let m = |t: MatchType, n: &str, v: &str| Matcher::must_new(t, n, v);
    let vs = |name: &str, label_matchers: Vec<Matcher>| VectorSelector {
        name: name.to_string(),
        label_matchers,
        ..Default::default()
    };
    let cases = vec![
        ("empty value", vs("", vec![])),
        ("no matchers with name", vs("foobar", vec![])),
        ("one matcher with name", vs("foobar", vec![m(MatchType::Equal, "a", "x")])),
        ("two matchers with name", vs("foobar", vec![m(MatchType::Equal, "a", "x"), m(MatchType::Equal, "b", "y")])),
        ("two matchers without name", vs("", vec![m(MatchType::Equal, "a", "x"), m(MatchType::Equal, "b", "y")])),
        ("name matcher and name", vs("foobar", vec![m(MatchType::Equal, METRIC_NAME, "foobar")])),
        ("name matcher only", vs("", vec![m(MatchType::Equal, METRIC_NAME, "foobar")])),
        ("empty name matcher", vs("", vec![m(MatchType::Equal, METRIC_NAME, ""), m(MatchType::Equal, "a", "x")])),
    ];
    let rows = rows("TestVectorSelector_String");
    assert_eq!(cases.len(), rows.len());
    for ((name, v), row) in cases.into_iter().zip(&rows) {
        assert_eq!(s(row, "name"), name);
        assert_eq!(s(row, "expected"), v.to_string(), "{}", name);
    }
}

#[test]
fn test_matrix_selector_string() {
    let rows = rows("TestMatrixSelector_String");
    let cases = vec![
        (
            "vector selector",
            Some(MatrixSelector {
                vector_selector: Expr::VectorSelector(Box::new(VectorSelector {
                    name: "foobar".into(),
                    ..Default::default()
                })),
                range: 5 * 60 * 1_000_000_000,
                range_expr: None,
                end_pos: 0,
            }),
        ),
        (
            // A failed parse can leave a non-vector expression as the operand.
            "non-vector operand",
            Some(MatrixSelector {
                vector_selector: Expr::NumberLiteral(Box::new(NumberLiteral { val: 1.0, ..Default::default() })),
                range: 5 * 60 * 1_000_000_000,
                range_expr: None,
                end_pos: 0,
            }),
        ),
        // adapted: Go builds a MatrixSelector without an operand (nil VectorSelector) by hand. The Rust AST
        // always has an operand, so that node cannot be built; the parser never builds it either.
        ("no operand", None),
    ];
    assert_eq!(cases.len(), rows.len());
    for ((name, ms), row) in cases.into_iter().zip(&rows) {
        assert_eq!(s(row, "name"), name);
        let Some(ms) = ms else { continue };
        assert_eq!(s(row, "expected"), ms.to_string(), "{}", name);
        assert_eq!(s(row, "expectedShort"), ms.short_string(), "{}", name);
    }
}

#[test]
fn test_binary_expr_utf8_labels() {
    let p = Parser::default();
    let rows = rows("TestBinaryExprUTF8Labels");
    assert_eq!(6, rows.len());
    for row in &rows {
        let input = s(row, "input");
        let expr = p.parse_expr(&input).unwrap_or_else(|e| panic!("Failed to parse: {}", e));
        assert_eq!(s(row, "expected"), expr.to_string(), "{}", s(row, "name"));
    }
}
