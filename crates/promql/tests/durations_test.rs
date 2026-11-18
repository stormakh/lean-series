//! Port of `promql/durations_test.go`.

use promql::durations::DurationVisitor;
use promql::parser::{self, DurationExpr, Expr, NumberLiteral, Parser};

const SECOND: i64 = 1_000_000_000;
const MINUTE: i64 = 60 * SECOND;
const HOUR: i64 = 60 * MINUTE;

fn num(v: f64) -> Expr {
    Expr::NumberLiteral(Box::new(NumberLiteral { val: v, ..Default::default() }))
}

fn dur(op: parser::ItemType, lhs: Option<Expr>, rhs: Option<Expr>) -> Expr {
    Expr::Duration(Box::new(DurationExpr { op, lhs, rhs, ..Default::default() }))
}

#[test]
fn test_duration_visitor() {
    let p = Parser::new(parser::Options::default());
    let complex_expr = "sum_over_time(
		rate(metric[5m] offset 1h)[10m:30s] offset 2h
	) +
	avg_over_time(
		metric[1h + 30m] offset -1h
	) *
	count_over_time(
		metric[2h * 0.5]
	)";
    let mut expr = p.parse_expr(complex_expr).unwrap();
    DurationVisitor::default().walk(&mut expr).unwrap();

    let Expr::Binary(bin) = &expr else { panic!("Expected binary expression at top level") };
    let Expr::Call(left) = &bin.lhs else { panic!("Expected call expression on left side") };
    assert_eq!("sum_over_time", left.func.name);
    let Expr::Subquery(sq) = &left.args[0] else { panic!("Expected subquery in sum_over_time") };
    assert_eq!(10 * MINUTE, sq.range);
    assert_eq!(30 * SECOND, sq.step);
    assert_eq!(2 * HOUR, sq.original_offset);
    let Expr::Call(rate) = &sq.expr else { panic!("Expected rate call in subquery") };
    assert_eq!("rate", rate.func.name);
    let Expr::MatrixSelector(rm) = &rate.args[0] else { panic!("Expected matrix selector in rate") };
    assert_eq!(5 * MINUTE, rm.range);
    assert_eq!(HOUR, rm.vector_selector.as_vector_selector().unwrap().original_offset);

    let Expr::Binary(right) = &bin.rhs else { panic!("Expected binary expression on right side") };
    let Expr::Call(avg) = &right.lhs else { panic!("Expected call expression on left side of multiplication") };
    assert_eq!("avg_over_time", avg.func.name);
    let Expr::MatrixSelector(am) = &avg.args[0] else { panic!("Expected matrix selector in avg_over_time") };
    assert_eq!(90 * MINUTE, am.range);
    assert_eq!(-HOUR, am.vector_selector.as_vector_selector().unwrap().original_offset);
    let Expr::Call(count) = &right.rhs else { panic!("Expected call expression on right side of multiplication") };
    assert_eq!("count_over_time", count.func.name);
    let Expr::MatrixSelector(cm) = &count.args[0] else { panic!("Expected matrix selector in count_over_time") };
    assert_eq!(HOUR, cm.range);
}

#[test]
fn test_calculate_duration() {
    use parser::{ADD, DIV, MAX_OF, MOD, MUL, POW, RANGE, STEP, SUB};
    struct Case {
        name: &'static str,
        expr: Expr,
        expected: i64,
        error_message: &'static str,
        allowed_negative: bool,
    }
    let c = |name, expr, expected| Case { name, expr, expected, error_message: "", allowed_negative: false };
    let e = |name, expr, msg| Case { name, expr, expected: 0, error_message: msg, allowed_negative: false };
    let tests = vec![
        c("addition", dur(ADD, Some(num(5.0)), Some(num(10.0))), 15 * SECOND),
        c("subtraction", dur(SUB, Some(num(15.0)), Some(num(5.0))), 10 * SECOND),
        e("subtraction with negative", dur(SUB, Some(num(5.0)), Some(num(10.0))), "duration must be greater than 0"),
        c("multiplication", dur(MUL, Some(num(5.0)), Some(num(3.0))), 15 * SECOND),
        c("division", dur(DIV, Some(num(15.0)), Some(num(3.0))), 5 * SECOND),
        c("modulo with numbers", dur(MOD, Some(num(17.0)), Some(num(5.0))), 2 * SECOND),
        c("power", dur(POW, Some(num(2.0)), Some(num(3.0))), 8 * SECOND),
        c(
            "complex expression",
            dur(SUB, Some(dur(MUL, Some(num(2.0)), Some(dur(ADD, Some(num(3.0)), Some(num(4.0)))))), Some(num(1.0))),
            13 * SECOND,
        ),
        Case {
            name: "unary negative",
            expr: dur(SUB, None, Some(num(5.0))),
            expected: -5 * SECOND,
            error_message: "",
            allowed_negative: true,
        },
        c("step", dur(STEP, None, None), SECOND),
        c("step multiplication", dur(MUL, Some(dur(STEP, None, None)), Some(num(3.0))), 3 * SECOND),
        c("range", dur(RANGE, None, None), 5 * MINUTE),
        c("range division", dur(DIV, Some(dur(RANGE, None, None)), Some(num(2.0))), 150 * SECOND),
        c("max_of of step and range", dur(MAX_OF, Some(dur(STEP, None, None)), Some(dur(RANGE, None, None))), 5 * MINUTE),
        e("division by zero", dur(DIV, Some(num(5.0)), Some(dur(SUB, Some(num(5.0)), Some(num(5.0))))), "division by zero"),
        e("modulo by zero", dur(MOD, Some(num(5.0)), Some(dur(SUB, Some(num(5.0)), Some(num(5.0))))), "modulo by zero"),
        Case {
            name: "NaN from negative base raised to fractional power",
            expr: dur(POW, Some(num(-1.0)), Some(num(0.5))),
            expected: 0,
            error_message: "duration is NaN or infinite",
            allowed_negative: true,
        },
        e("infinity from huge power", dur(POW, Some(num(2.0)), Some(num(1e10))), "duration is NaN or infinite"),
        e("duration exceeds time.Duration range", num(1e10), "duration is out of range"),
        c("duration just below the upper bound is accepted", num(1e9), 1_000_000_000 * SECOND),
    ];
    for tt in tests {
        let v = DurationVisitor { step: SECOND, query_range: 5 * MINUTE };
        let r = v.calculate_duration(&tt.expr, tt.allowed_negative);
        if !tt.error_message.is_empty() {
            let err = r.expect_err(tt.name);
            assert!(err.contains(tt.error_message), "{}: {}", tt.name, err);
            continue;
        }
        assert_eq!(tt.expected, r.unwrap(), "{}", tt.name);
    }
}
