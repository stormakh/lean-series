//! Port of `promql/engine_internal_test.go`.

use std::panic::{AssertUnwindSafe, catch_unwind};

use model::histogram::{CUSTOM_BUCKETS_SCHEMA, CounterResetHint, FloatHistogram, custom_values};
use promql::parser::{self, ADD, Parser, SUB};
use promql::{ErrWithWarnings, preprocess_expr, recover_evaluator, subquery_time_range_for, vector_elem_binop};
use storage::annotations::{
    HISTOGRAM_COUNTER_RESET_COLLISION_WARNING, MISMATCHED_CUSTOM_BUCKETS_HISTOGRAMS_INFO, PositionRange,
};
use storage::{Annotations, Error};

fn test_parser() -> Parser {
    Parser::new(parser::Options::default())
}

#[test]
fn test_recover_evaluator_runtime() {
    let mut output = String::new();
    let expr = test_parser().parse_expr("sum(up)").unwrap();
    // Cause a runtime panic.
    let res = catch_unwind(AssertUnwindSafe(|| -> Result<(), Error> {
        let a: Vec<i32> = Vec::new();
        let i = std::hint::black_box(123usize);
        let _ = a[i];
        Ok(())
    }));
    let (_, _, err) = recover_evaluator(Some(&expr), res, &mut |m| output.push_str(m));
    assert_eq!(
        "unexpected error: runtime error: index out of range [123] with length 0",
        err.unwrap().to_string()
    );
    assert!(output.contains("sum(up)"));
}

#[test]
fn test_recover_evaluator_error() {
    let e = Error::msg("custom error");
    let ec = e.clone();
    let res = catch_unwind(AssertUnwindSafe(|| -> Result<(), Error> { std::panic::panic_any(ec) }));
    let (_, _, err) = recover_evaluator(None, res, &mut |_| {});
    assert_eq!(e.to_string(), err.unwrap().to_string());
}

#[test]
fn test_recover_evaluator_error_with_warnings() {
    let warnings = Annotations::new().with(Error::msg("custom warning"));
    let e = ErrWithWarnings { err: Error::msg("custom error"), warnings: warnings.clone() };
    let ec = Error::other(e.clone());
    let res = catch_unwind(AssertUnwindSafe(|| -> Result<(), Error> { std::panic::panic_any(ec) }));
    let (_, ws, err) = recover_evaluator(None, res, &mut |_| {});
    assert_eq!(e.to_string(), err.unwrap().to_string());
    assert_eq!(warnings, ws, "wrong warning message");
}

#[test]
fn test_subquery_time_range() {
    let no_step_interval = |_: i64| 60_000i64;
    const SECOND: i64 = 1000;
    struct Case {
        name: &'static str,
        parent_start: i64,
        parent_end: i64,
        parent_interval: i64,
        query: &'static str,
        want: (i64, i64, i64),
    }
    let cases = [
        Case { name: "instant query", parent_start: 1000 * SECOND, parent_end: 1000 * SECOND, parent_interval: 0, query: "metric[60s:5s]", want: (945 * SECOND, 1000 * SECOND, 5 * SECOND) },
        Case { name: "range query, end already step-aligned", parent_start: 201 * SECOND, parent_end: 216 * SECOND, parent_interval: 5 * SECOND, query: "metric[60s:5s]", want: (145 * SECOND, 216 * SECOND, 5 * SECOND) },
        Case { name: "range query, end past last step is aligned down", parent_start: 201 * SECOND, parent_end: 220 * SECOND, parent_interval: 5 * SECOND, query: "metric[60s:5s]", want: (145 * SECOND, 216 * SECOND, 5 * SECOND) },
        Case { name: "range query with offset, end aligned down", parent_start: 201 * SECOND, parent_end: 220 * SECOND, parent_interval: 5 * SECOND, query: "metric[60s:5s] offset 30s", want: (115 * SECOND, 186 * SECOND, 5 * SECOND) },
        Case { name: "range query, subquery without explicit step", parent_start: 201 * SECOND, parent_end: 220 * SECOND, parent_interval: 5 * SECOND, query: "metric[100s:]", want: (120 * SECOND, 216 * SECOND, 60 * SECOND) },
        Case { name: "range query with offset, start bumped onto the grid", parent_start: 201 * SECOND, parent_end: 216 * SECOND, parent_interval: 5 * SECOND, query: "metric[60s:5s] offset 1s", want: (145 * SECOND, 215 * SECOND, 5 * SECOND) },
        Case { name: "instant query, negative boundary skips the start bump", parent_start: 30 * SECOND, parent_end: 30 * SECOND, parent_interval: 0, query: "metric[61s:5s]", want: (-30 * SECOND, 30 * SECOND, 5 * SECOND) },
    ];
    for tc in cases {
        let expr = test_parser().parse_expr(tc.query).unwrap();
        let expr = preprocess_expr(expr, tc.parent_start, tc.parent_end, tc.parent_interval).unwrap();
        let got = subquery_time_range_for(tc.parent_start, tc.parent_end, tc.parent_interval, &expr, &no_step_interval)
            .expect("query must parse to a subquery expression");
        assert_eq!(tc.want.0, got.0, "{}: subquery start timestamp", tc.name);
        assert_eq!(tc.want.1, got.1, "{}: subquery end timestamp", tc.name);
        assert_eq!(tc.want.2, got.2, "{}: subquery interval", tc.name);
    }
}

#[test]
fn test_vector_elem_binop_histograms() {
    let nhcb = |cv: &[f64]| FloatHistogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_buckets: vec![],
        custom_values: custom_values(cv),
        ..Default::default()
    };
    // Go's want values have nil custom values.
    let nhcb_nil = || FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, positive_buckets: vec![], ..Default::default() };
    let hint = |h: CounterResetHint| FloatHistogram { counter_reset_hint: h, ..Default::default() };
    struct Case {
        name: &'static str,
        op: parser::ItemType,
        hlhs: FloatHistogram,
        hrhs: FloatHistogram,
        want: FloatHistogram,
        want_err: Option<Error>,
        want_info: Option<Error>,
    }
    let cases = vec![
        Case { name: "ADD with unknown counter reset hints", op: ADD, hlhs: FloatHistogram::default(), hrhs: FloatHistogram::default(), want: FloatHistogram::default(), want_err: None, want_info: None },
        Case { name: "ADD with mixed counter reset hints", op: ADD, hlhs: hint(CounterResetHint::CounterReset), hrhs: hint(CounterResetHint::NotCounterReset), want: FloatHistogram::default(), want_err: Some(Error::Sentinel(&HISTOGRAM_COUNTER_RESET_COLLISION_WARNING)), want_info: None },
        Case { name: "SUB with unknown counter reset hints", op: SUB, hlhs: FloatHistogram::default(), hrhs: FloatHistogram::default(), want: hint(CounterResetHint::GaugeType), want_err: None, want_info: None },
        Case { name: "SUB with mixed counter reset hints", op: SUB, hlhs: hint(CounterResetHint::CounterReset), hrhs: hint(CounterResetHint::NotCounterReset), want: FloatHistogram::default(), want_err: Some(Error::Sentinel(&HISTOGRAM_COUNTER_RESET_COLLISION_WARNING)), want_info: None },
        Case { name: "ADD with mismatched NHCB bounds", op: ADD, hlhs: nhcb(&[1.0]), hrhs: nhcb(&[2.0]), want: nhcb_nil(), want_err: None, want_info: Some(Error::Sentinel(&MISMATCHED_CUSTOM_BUCKETS_HISTOGRAMS_INFO)) },
        Case { name: "SUB with mismatched NHCB bounds", op: SUB, hlhs: nhcb(&[1.0]), hrhs: nhcb(&[2.0]), want: FloatHistogram { counter_reset_hint: CounterResetHint::GaugeType, ..nhcb_nil() }, want_err: None, want_info: Some(Error::Sentinel(&MISMATCHED_CUSTOM_BUCKETS_HISTOGRAMS_INFO)) },
    ];
    for tc in cases {
        let l = std::sync::Arc::new(tc.hlhs);
        let r = std::sync::Arc::new(tc.hrhs);
        let res = vector_elem_binop(tc.op, 0.0, 0.0, Some(&l), Some(&r), PositionRange::default());
        if let Some(want_err) = &tc.want_err {
            assert!(res.err.as_ref().is_some_and(|e| e.is(want_err)), "{}: {:?}", tc.name, res.err);
            continue;
        }
        assert!(res.err.is_none(), "{}: {:?}", tc.name, res.err);
        match &tc.want_info {
            Some(wi) => assert!(res.info.as_ref().is_some_and(|e| e.is(wi)), "{}: {:?}", tc.name, res.info),
            None => assert!(res.info.is_none(), "{}", tc.name),
        }
        assert_eq!(Some(&tc.want), res.h.as_deref(), "{}", tc.name);
    }
}

#[test]
fn test_cost_fallback_sample_window() {
    // Intervals are nanoseconds (Go time.Duration); timestamps milliseconds.
    for (name, end, interval, want_start) in [
        ("default", 0i64, 0i64, -300000i64),
        ("negative interval", 0, -1_000_000_000, -300000),
        ("minimum duration", 0, i64::MIN, -300000),
        ("eight intervals", 0, 60_000_000_000, -480000),
        ("maximum duration", 0, i64::MAX, -1800000),
        ("timestamp underflow", i64::MIN + 1, 60_000_000_000, i64::MIN),
    ] {
        let (start, got_end) = promql::estimate::fallback_sample_window(end, interval);
        assert_eq!(want_start, start, "{name}");
        assert_eq!(end, got_end, "{name}");
    }
}
