//! Port of `promql/info.go`: the `info` function.

use storage::{Annotations, Error};

use crate::engine::Evaluator;
fn dur_ms(ns: i64) -> i64 {
    ns / 1_000_000
}
use crate::parser::{Expr, ValueType};
use crate::value::Value;

#[derive(Clone, Copy, Debug, Default)]
struct InfoSeriesReference {
    timestamp: Option<i64>,
    offset: i64,
}

impl InfoSeriesReference {
    fn equal(&self, other: &InfoSeriesReference) -> bool {
        match (self.timestamp, other.timestamp) {
            (Some(a), Some(b)) => a - dur_ms(self.offset) == b - dur_ms(other.offset),
            (None, None) => dur_ms(self.offset) == dur_ms(other.offset),
            _ => false,
        }
    }
}

/// Go `infoSelectTimestampAndOffset`: the first vector-producing selector's reference (@ timestamp and offset in
/// nanoseconds) and whether every vector-producing path has a selector with the same effective reference time.
pub fn info_select_timestamp_and_offset(expr: &Expr) -> (Option<i64>, i64, bool) {
    struct St {
        first: InfoSeriesReference,
        found: bool,
        reference_free: bool,
        uniform: bool,
    }
    fn inspect<'a>(expr: &'a Expr, path: &mut Vec<&'a Expr>, st: &mut St) -> bool {
        let t = expr.value_type();
        if t != ValueType::Vector && t != ValueType::Matrix {
            return false;
        }
        if let Expr::VectorSelector(n) = expr {
            let mut r = InfoSeriesReference { timestamp: n.timestamp, offset: n.original_offset };
            let mut i = path.len();
            while r.timestamp.is_none() && i > 0 {
                i -= 1;
                if let Expr::Subquery(sq) = path[i] {
                    r.offset += sq.original_offset;
                    r.timestamp = sq.timestamp;
                }
            }
            if !st.found {
                st.first = r;
                st.found = true;
            } else if !st.first.equal(&r) {
                st.uniform = false;
            }
            return true;
        }
        path.push(expr);
        let res = if let Expr::Call(c) = expr
            && c.func.name == "info"
        {
            inspect(&c.args[0], path, st)
        } else {
            let mut has_selector = false;
            expr.for_each_child(|child| {
                if inspect(child, path, st) {
                    has_selector = true;
                }
                true
            });
            if !has_selector {
                st.reference_free = true;
            }
            has_selector
        };
        path.pop();
        res
    }
    let mut st = St { first: InfoSeriesReference::default(), found: false, reference_free: false, uniform: true };
    inspect(expr, &mut Vec::new(), &mut st);
    if !st.found {
        return (None, 0, false);
    }
    (st.first.timestamp, st.first.offset, st.uniform && !st.reference_free)
}

/// Go `infoSeriesSelectTimestampAndOffset`.
pub fn info_series_select_timestamp_and_offset(expr: &Expr) -> (Option<i64>, i64) {
    let (ts, off, uniform) = info_select_timestamp_and_offset(expr);
    if !uniform {
        return (None, 0);
    }
    (ts, off)
}

/// Go `evalInfo`.
pub(crate) fn eval_info(_ev: &mut Evaluator<'_>, _args: &[Expr]) -> Result<(Value, Annotations), Error> {
    Err(Error::msg("info() is not implemented yet"))
}
