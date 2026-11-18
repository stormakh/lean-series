// Port of promql/parser/prettier_test.go. The tables are read from a fixture that
// scripts/gen/promql_parser_fixtures.sh extracts from the Go test source. Every Go test sets
// maxCharactersPerLine to 10; so does every test here.

mod common;

use common::*;
use promql::parser::*;

fn run(func: &str, p: &Parser) {
    set_max_characters_per_line(10);
    let fx = fixture("prettier_test.json");
    let rows = fx.get(func).unwrap_or_else(|| panic!("no table {}", func)).as_arr().unwrap();
    assert!(!rows.is_empty());
    for row in rows {
        let input = fixture_str(row.get("in").unwrap()).unwrap();
        let out = fixture_str(row.get("out").unwrap()).unwrap();
        let expr = p.parse_expr(&input).unwrap_or_else(|e| panic!("{}: {}", input, e));
        assert_eq!(out, prettify(&expr), "{}", input);
    }
}

#[test]
fn test_aggregate_expr_pretty() {
    run("TestAggregateExprPretty", &Parser::default());
}

#[test]
fn test_binary_expr_pretty() {
    run("TestBinaryExprPretty", &Parser::default());
}

#[test]
fn test_call_expr_pretty() {
    run("TestCallExprPretty", &Parser::default());
}

#[test]
fn test_paren_expr_pretty() {
    run("TestParenExprPretty", &Parser::default());
}

#[test]
fn test_step_invariant_expr() {
    run("TestStepInvariantExpr", &Parser::default());
}

#[test]
fn test_expr_pretty() {
    run("TestExprPretty", &Parser::default());
}

#[test]
fn test_unary_pretty() {
    run("TestUnaryPretty", &Parser::default());
}

#[test]
fn test_duration_expr_pretty() {
    run("TestDurationExprPretty", &Parser::new(Options::default()));
}
