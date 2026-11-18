//! Port of `promql/durations.go`: evaluates duration expressions (`offset (1h / 2)`, `[step()]`) into the
//! concrete durations of the AST nodes.

use crate::parser::{
    ADD, DIV, DurationExpr, Expr, MAX_OF, MIN_OF, MOD, MUL, POW, RANGE, STEP, SUB,
};

/// Evaluates duration expressions (Go `durationVisitor`). Durations are nanoseconds.
#[derive(Clone, Copy, Debug, Default)]
pub struct DurationVisitor {
    pub step: i64,
    pub query_range: i64,
}

impl DurationVisitor {
    /// Visits `expr` and its children (Go `parser.Walk` with `Visit`), setting the evaluated durations.
    pub fn walk(&self, expr: &mut Expr) -> Result<(), String> {
        self.visit(expr)?;
        crate::engine::for_each_child_mut(expr, &mut |c| self.walk(c))
    }

    /// Go `Visit` for one node.
    pub fn visit(&self, node: &mut Expr) -> Result<(), String> {
        match node {
            Expr::VectorSelector(n) => {
                if let Some(e) = &n.original_offset_expr {
                    n.original_offset = self.calculate_duration_expr(e, true)?;
                }
            }
            Expr::MatrixSelector(n) => {
                if let Some(e) = &n.range_expr {
                    n.range = self.calculate_duration_expr(e, false)?;
                }
            }
            Expr::Subquery(n) => {
                if let Some(e) = &n.original_offset_expr {
                    n.original_offset = self.calculate_duration_expr(e, true)?;
                }
                if let Some(e) = &n.step_expr {
                    n.step = self.calculate_duration_expr(e, false)?;
                }
                if let Some(e) = &n.range_expr {
                    n.range = self.calculate_duration_expr(e, false)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Go `calculateDuration` for a duration expression node.
    pub fn calculate_duration_expr(&self, expr: &DurationExpr, allowed_negative: bool) -> Result<i64, String> {
        let duration = self.evaluate_duration(expr)?;
        Self::to_duration(duration, expr.position_range(), allowed_negative)
    }

    /// Go `calculateDuration`: the duration of a duration expression in nanoseconds.
    pub fn calculate_duration(&self, expr: &Expr, allowed_negative: bool) -> Result<i64, String> {
        let duration = self.evaluate_duration_expr(expr)?;
        Self::to_duration(duration, expr.position_range(), allowed_negative)
    }

    fn to_duration(duration: f64, pos: crate::parser::PositionRange, allowed_negative: bool) -> Result<i64, String> {
        if duration.is_nan() || duration.is_infinite() {
            return Err(format!("{}:{}: duration is NaN or infinite", pos.start, pos.end));
        }
        if duration <= 0.0 && !allowed_negative {
            return Err(format!("{}:{}: duration must be greater than 0", pos.start, pos.end));
        }
        let lim = 9223372036854775808.0 / 1e9;
        if duration > lim || duration < -lim {
            return Err(format!("{}:{}: duration is out of range", pos.start, pos.end));
        }
        // Go: time.Duration(duration*1000) * time.Millisecond, with wrapping int64 arithmetic.
        Ok(((duration * 1000.0) as i64).wrapping_mul(1_000_000))
    }

    /// Go `evaluateDurationExpr` for an expression node.
    pub fn evaluate_duration_expr(&self, expr: &Expr) -> Result<f64, String> {
        match expr {
            Expr::NumberLiteral(n) => Ok(n.val),
            Expr::Duration(n) => self.evaluate_duration(n),
            other => Err(format!("unexpected duration expression type {}", go_type_name(other))),
        }
    }

    fn evaluate_duration(&self, n: &DurationExpr) -> Result<f64, String> {
        let mut lhs = 0.0;
        let mut rhs = 0.0;
        if let Some(l) = &n.lhs {
            lhs = self.evaluate_duration_expr(l)?;
        }
        if let Some(r) = &n.rhs {
            rhs = self.evaluate_duration_expr(r)?;
        }
        let pos = n.position_range();
        match n.op {
            STEP => Ok(self.step as f64 / 1e9),
            RANGE => Ok(self.query_range as f64 / 1e9),
            MIN_OF => Ok(go_min(lhs, rhs)),
            MAX_OF => Ok(go_max(lhs, rhs)),
            ADD => Ok(if n.lhs.is_none() { rhs } else { lhs + rhs }),
            SUB => Ok(if n.lhs.is_none() { -rhs } else { lhs - rhs }),
            MUL => Ok(lhs * rhs),
            DIV => {
                if rhs == 0.0 {
                    return Err(format!("{}:{}: division by zero", pos.start, pos.end));
                }
                Ok(lhs / rhs)
            }
            MOD => {
                if rhs == 0.0 {
                    return Err(format!("{}:{}: modulo by zero", pos.start, pos.end));
                }
                Ok(lhs % rhs)
            }
            POW => Ok(crate::engine::go_pow(lhs, rhs)),
            op => Err(format!("unexpected duration expression operator {}", crate::parser::quote::go_quote(&op.to_string()))),
        }
    }
}

/// Go `math.Min`.
pub fn go_min(x: f64, y: f64) -> f64 {
    if x.is_infinite() && x < 0.0 || y.is_infinite() && y < 0.0 {
        return f64::NEG_INFINITY;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 && x == y {
        return if x.is_sign_negative() { x } else { y };
    }
    if x < y { x } else { y }
}

/// Go `math.Max`.
pub fn go_max(x: f64, y: f64) -> f64 {
    if x.is_infinite() && x > 0.0 || y.is_infinite() && y > 0.0 {
        return f64::INFINITY;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 && x == y {
        return if x.is_sign_negative() { y } else { x };
    }
    if x > y { x } else { y }
}

/// The Go type name of a node, for error messages (`%T`).
pub fn go_type_name(e: &Expr) -> &'static str {
    match e {
        Expr::Aggregate(_) => "*parser.AggregateExpr",
        Expr::Binary(_) => "*parser.BinaryExpr",
        Expr::Call(_) => "*parser.Call",
        Expr::MatrixSelector(_) => "*parser.MatrixSelector",
        Expr::Subquery(_) => "*parser.SubqueryExpr",
        Expr::NumberLiteral(_) => "*parser.NumberLiteral",
        Expr::Paren(_) => "*parser.ParenExpr",
        Expr::StringLiteral(_) => "*parser.StringLiteral",
        Expr::Unary(_) => "*parser.UnaryExpr",
        Expr::VectorSelector(_) => "*parser.VectorSelector",
        Expr::StepInvariant(_) => "*parser.StepInvariantExpr",
        Expr::Duration(_) => "*parser.DurationExpr",
    }
}
