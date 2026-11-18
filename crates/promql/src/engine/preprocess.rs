//! Preparation of a statement (Go `PreprocessExpr`, `FindMinMaxTime`, `populateSeries`,
//! `setOffsetForAtModifier`, `detectHistogramStatsDecoding`).

use storage::{Context, Querier, SelectHints};

use super::EvalStmt;
use super::eval::{QueryState, addr, dur_ms};
use crate::durations::DurationVisitor;
use crate::functions;
use crate::info::{info_select_timestamp_and_offset, info_series_select_timestamp_and_offset};
use crate::parser::{
    COUNT_VALUES, EQLC, END, Expr, NEQ, NumberLiteral, START, StepInvariantExpr, TRIM_LOWER, TRIM_UPPER,
    VectorSelector,
};

/// Calls `f` on each child of `expr`, mutably, in Go `ChildrenIter` order.
pub fn for_each_child_mut<E>(expr: &mut Expr, f: &mut impl FnMut(&mut Expr) -> Result<(), E>) -> Result<(), E> {
    match expr {
        Expr::Aggregate(n) => {
            if let Some(e) = &mut n.expr {
                f(e)?;
            }
            if let Some(p) = &mut n.param {
                f(p)?;
            }
        }
        Expr::Binary(n) => {
            f(&mut n.lhs)?;
            f(&mut n.rhs)?;
        }
        Expr::Call(n) => {
            for a in n.args.iter_mut() {
                f(a)?;
            }
        }
        Expr::Subquery(n) => f(&mut n.expr)?,
        Expr::Paren(n) => f(&mut n.expr)?,
        Expr::Unary(n) => f(&mut n.expr)?,
        Expr::MatrixSelector(n) => f(&mut n.vector_selector)?,
        Expr::StepInvariant(n) => f(&mut n.expr)?,
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::VectorSelector(_) | Expr::Duration(_) => {}
    }
    Ok(())
}

/// Go `subqueryTimes`: the sum of offsets and ranges (nanoseconds) of the subqueries in the path, and the
/// closest `@` timestamp.
pub fn subquery_times(path: &[&Expr]) -> (i64, i64, Option<i64>) {
    let (mut off, mut rng) = (0i64, 0i64);
    let mut ts = None;
    for node in path {
        if let Expr::Subquery(n) = node {
            off += n.original_offset;
            rng += n.range;
            if let Some(t) = n.timestamp {
                off = n.original_offset;
                rng = n.range;
                ts = Some(t);
            }
        }
    }
    (off, rng, ts)
}

/// Go `getTimeRangesForSelector`. `eval_range` is in nanoseconds; the result in milliseconds.
pub fn get_time_ranges_for_selector(
    s: &EvalStmt,
    n: &VectorSelector,
    path: &[&Expr],
    eval_range: i64,
) -> (i64, i64) {
    get_time_ranges(s, n.timestamp, n.original_offset, n.anchored, n.smoothed, path, eval_range)
}

fn get_time_ranges(
    s: &EvalStmt,
    timestamp: Option<i64>,
    original_offset: i64,
    anchored: bool,
    smoothed: bool,
    path: &[&Expr],
    eval_range: i64,
) -> (i64, i64) {
    let (mut start, mut end) = (s.start, s.end);
    let (subq_offset, subq_range, subq_ts) = subquery_times(path);
    if let Some(t) = subq_ts {
        start = t;
        end = t;
    }
    if let Some(t) = timestamp {
        start = t;
        end = t;
    } else {
        let off = dur_ms(subq_offset);
        start = start - off - dur_ms(subq_range);
        end -= off;
    }
    let lb = s.lookback_delta.as_nanos() as i64;
    if eval_range == 0 {
        start -= dur_ms(lb) - 1;
        if smoothed {
            end += dur_ms(lb);
        }
    } else if anchored {
        start -= dur_ms(lb + eval_range) - 1;
    } else if smoothed {
        start -= dur_ms(lb + eval_range) - 1;
        end += dur_ms(lb);
    } else {
        start -= dur_ms(eval_range) - 1;
    }
    let off = dur_ms(original_offset);
    (start - off, end - off)
}

/// Walks the tree with the path of ancestors, like `parser.Inspect`, tracking the range of the enclosing matrix
/// selector as Go's `evalRange` variable does.
fn inspect_selectors<'a>(expr: &'a Expr, f: &mut impl FnMut(&'a Expr, &[&'a Expr])) {
    let _ = crate::parser::inspect(expr, |node, path| -> Result<(), ()> {
        f(node, path);
        Ok(())
    });
}

/// Go `FindMinMaxTime`: the earliest and latest time (ms) the statement reads; (0, 0) without selectors.
pub fn find_min_max_time(s: &EvalStmt) -> (i64, i64) {
    let (mut min_ts, mut max_ts) = (i64::MAX, i64::MIN);
    let mut eval_range = 0i64;
    inspect_selectors(&s.expr, &mut |node, path| match node {
        Expr::Call(n) if n.func.name == "info" => {
            let (ts, off) = info_series_select_timestamp_and_offset(&n.args[0]);
            let (start, end) = get_time_ranges(s, ts, off, false, false, path, 0);
            min_ts = min_ts.min(start);
            max_ts = max_ts.max(end);
        }
        Expr::VectorSelector(n) => {
            let (start, end) = get_time_ranges_for_selector(s, n, path, eval_range);
            min_ts = min_ts.min(start);
            max_ts = max_ts.max(end);
            eval_range = 0;
        }
        Expr::MatrixSelector(n) => eval_range = n.range,
        _ => {}
    });
    if max_ts == i64::MIN {
        return (0, 0);
    }
    (min_ts, max_ts)
}

/// Go `extractFuncFromPath`.
fn extract_func_from_path(p: &[&Expr]) -> String {
    for n in p.iter().rev() {
        match n {
            Expr::Aggregate(a) => return a.op.to_string(),
            Expr::Call(c) => return c.func.name.to_string(),
            Expr::Binary(_) => return String::new(),
            _ => {}
        }
    }
    String::new()
}

/// Go `extractGroupsFromPath`.
fn extract_groups_from_path(p: &[&Expr]) -> (bool, Vec<String>) {
    match p.last() {
        Some(Expr::Aggregate(a)) => (!a.without, a.grouping.clone()),
        _ => (false, Vec::new()),
    }
}

/// Go `getLastSubqueryInterval` (nanoseconds).
fn get_last_subquery_interval(path: &[&Expr], no_step: &dyn Fn(i64) -> i64) -> i64 {
    let mut interval = 0;
    for node in path {
        if let Expr::Subquery(n) = node {
            interval = n.step;
            if n.step == 0 {
                interval = no_step(dur_ms(n.range)) * 1_000_000;
            }
        }
    }
    interval
}

/// Go `populateSeries`: selects the series of every vector selector.
pub(crate) fn populate_series(
    querier: &dyn Querier,
    s: &EvalStmt,
    ctx: &Context,
    state: &QueryState,
    no_step: &(dyn Fn(i64) -> i64 + Send + Sync),
) {
    let mut eval_range = 0i64;
    inspect_selectors(&s.expr, &mut |node, path| match node {
        Expr::VectorSelector(n) => {
            let (start, end) = get_time_ranges_for_selector(s, n, path, eval_range);
            let mut interval = get_last_subquery_interval(path, no_step);
            if interval == 0 {
                interval = s.interval * 1_000_000;
            }
            let (by, grouping) = extract_groups_from_path(path);
            let hints = SelectHints {
                start,
                end,
                step: dur_ms(interval),
                range: dur_ms(eval_range),
                func: extract_func_from_path(path),
                by,
                grouping,
                ..Default::default()
            };
            eval_range = 0;
            let set = querier.select(ctx, false, Some(&hints), &n.label_matchers);
            state.set_unexpanded(n, set);
        }
        Expr::MatrixSelector(n) => eval_range = n.range,
        _ => {}
    });
}

/// Go `setOffsetForAtModifier`: the offsets of selectors and subqueries with `@`, relative to `eval_time`.
pub fn set_offset_for_at_modifier(eval_time: i64, expr: &Expr, state: &QueryState) {
    let get_offset = |ts: Option<i64>, original_offset: i64, path: &[&Expr]| -> Option<i64> {
        let ts = ts?;
        let (mut subq_offset, _, subq_ts) = subquery_times(path);
        if let Some(t) = subq_ts {
            subq_offset += (eval_time - t) * 1_000_000;
        }
        let offset_for_ts = (eval_time - ts) * 1_000_000;
        Some(original_offset + offset_for_ts - subq_offset)
    };
    inspect_selectors(expr, &mut |node, path| {
        let (key, off) = match node {
            Expr::VectorSelector(n) => (addr(&**n), get_offset(n.timestamp, n.original_offset, path)),
            Expr::MatrixSelector(m) => match &m.vector_selector {
                Expr::VectorSelector(n) => (addr(&**n), get_offset(n.timestamp, n.original_offset, path)),
                _ => return,
            },
            Expr::Subquery(n) => (addr(&**n), get_offset(n.timestamp, n.original_offset, path)),
            _ => return,
        };
        if let Some(off) = off {
            state.set_offset(key, off);
        }
    });
}

/// Go `subqueryEvaluationTimes`.
pub fn subquery_evaluation_times(
    start: i64,
    mut end: i64,
    interval: i64,
    offset: i64,
    range_ms: i64,
    subq_interval: i64,
    fixed: bool,
) -> (i64, i64) {
    if fixed {
        end = start;
    } else if interval > 0 {
        end = start + ((end - start) / interval) * interval;
    }
    let mut subq_start = subq_interval * ((start - offset - range_ms) / subq_interval);
    if subq_start <= start - offset - range_ms {
        subq_start += subq_interval;
    }
    (subq_start, end - offset)
}

// -----------------------------------------------------------------------------------------------------------------
// PreprocessExpr.

/// Go `PreprocessExpr`: marks histogram selectors that only need statistics, folds `start()`, `end()`,
/// `range()` and `step()`, evaluates duration expressions, resolves `@ start()`/`@ end()` and wraps
/// step-invariant parts in `StepInvariantExpr`. Times and step are milliseconds.
pub fn preprocess_expr(mut expr: Expr, start: i64, end: i64, step: i64) -> Result<Expr, String> {
    detect_histogram_stats_decoding(&mut expr);
    fold_query_context_functions(&mut expr, start, end, step);
    let v = DurationVisitor { step: step * 1_000_000, query_range: (end - start) * 1_000_000 };
    v.walk(&mut expr)?;
    let (_, should_wrap) = preprocess_expr_helper(&mut expr, start, end);
    if should_wrap {
        return Ok(new_step_invariant_expr(expr));
    }
    Ok(expr)
}

fn new_step_invariant_expr(expr: Expr) -> Expr {
    Expr::StepInvariant(Box::new(StepInvariantExpr { expr }))
}

fn wrap_in_place(e: &mut Expr) {
    let inner = std::mem::replace(e, Expr::NumberLiteral(Box::default()));
    *e = new_step_invariant_expr(inner);
}

/// Go `unwrapParenExpr`.
fn unwrap_paren_expr(e: &mut Expr) {
    while let Expr::Paren(p) = e {
        let inner = std::mem::replace(&mut p.expr, Expr::NumberLiteral(Box::default()));
        *e = inner;
    }
}

/// Go `foldQueryContextFunctions`.
fn fold_query_context_functions(expr: &mut Expr, start: i64, end: i64, step: i64) {
    if let Expr::Call(call) = expr {
        let val = match &*call.func.name {
            "start" => Some(start as f64 / 1000.0),
            "end" => Some(end as f64 / 1000.0),
            "range" => Some((end - start) as f64 / 1000.0),
            "step" => Some(if start != end { step as f64 / 1000.0 } else { 0.0 }),
            _ => None,
        };
        if let Some(val) = val {
            *expr = Expr::NumberLiteral(Box::new(NumberLiteral { val, duration: false, pos_range: call.pos_range }));
            return;
        }
    }
    match expr {
        Expr::Binary(n) => {
            fold_query_context_functions(&mut n.lhs, start, end, step);
            fold_query_context_functions(&mut n.rhs, start, end, step);
        }
        Expr::Call(n) => {
            for a in n.args.iter_mut() {
                fold_query_context_functions(a, start, end, step);
            }
        }
        Expr::Aggregate(n) => {
            if let Some(e) = &mut n.expr {
                fold_query_context_functions(e, start, end, step);
            }
            if let Some(p) = &mut n.param {
                fold_query_context_functions(p, start, end, step);
            }
        }
        Expr::Unary(n) => fold_query_context_functions(&mut n.expr, start, end, step),
        Expr::Paren(n) => fold_query_context_functions(&mut n.expr, start, end, step),
        Expr::Subquery(n) => fold_query_context_functions(&mut n.expr, start, end, step),
        Expr::MatrixSelector(_) | Expr::VectorSelector(_) | Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
        other => panic!("foldQueryContextFunctions: unhandled node type {}", crate::durations::go_type_name(other)),
    }
}

/// Go `preprocessExprHelper`: returns (isStepInvariant, shouldWrap).
fn preprocess_expr_helper(expr: &mut Expr, start: i64, end: i64) -> (bool, bool) {
    match expr {
        Expr::VectorSelector(n) => {
            match n.start_or_end {
                START => n.timestamp = Some(start),
                END => n.timestamp = Some(end),
                _ => {}
            }
            (n.timestamp.is_some(), n.timestamp.is_some())
        }
        Expr::Aggregate(n) => {
            if let Some(e) = &mut n.expr {
                unwrap_paren_expr(e);
            }
            if let Some(p) = &mut n.param {
                unwrap_paren_expr(p);
            }
            match &mut n.expr {
                Some(e) => preprocess_expr_helper(e, start, end),
                None => (false, false),
            }
        }
        Expr::Binary(n) => {
            let (inv_l, wrap_l) = preprocess_expr_helper(&mut n.lhs, start, end);
            let (inv_r, wrap_r) = preprocess_expr_helper(&mut n.rhs, start, end);
            if inv_l && inv_r {
                return (true, true);
            }
            if wrap_l {
                wrap_in_place(&mut n.lhs);
            }
            if wrap_r {
                wrap_in_place(&mut n.rhs);
            }
            (false, false)
        }
        Expr::Call(n) => {
            let name = n.func.name.to_string();
            let mut is_step_invariant = !functions::is_at_modifier_unsafe(&name);
            let mut ts_safe = name == "timestamp";
            let mut should_wrap = vec![false; n.args.len()];
            for i in 0..n.args.len() {
                unwrap_paren_expr(&mut n.args[i]);
                let (arg_inv, wrap) = preprocess_expr_helper(&mut n.args[i], start, end);
                should_wrap[i] = wrap;
                if name == "info" && i == 1 {
                    should_wrap[i] = false;
                    continue;
                }
                is_step_invariant = is_step_invariant && arg_inv;
                let is_vs = matches!(n.args[i], Expr::VectorSelector(_));
                if !arg_inv || !is_vs {
                    ts_safe = false;
                }
            }
            if name == "info" {
                let (_, _, uniform) = info_select_timestamp_and_offset(&n.args[0]);
                is_step_invariant = is_step_invariant && uniform;
            }
            if is_step_invariant || ts_safe {
                return (true, true);
            }
            for (i, w) in should_wrap.into_iter().enumerate() {
                if w {
                    wrap_in_place(&mut n.args[i]);
                }
            }
            (false, false)
        }
        Expr::MatrixSelector(n) => {
            let (inv, _) = preprocess_expr_helper(&mut n.vector_selector, start, end);
            (inv, false)
        }
        Expr::Subquery(n) => {
            let (inv, _) = preprocess_expr_helper(&mut n.expr, start, end);
            if inv {
                wrap_in_place(&mut n.expr);
            }
            match n.start_or_end {
                START => n.timestamp = Some(start),
                END => n.timestamp = Some(end),
                _ => {}
            }
            (n.timestamp.is_some(), false)
        }
        Expr::Paren(n) => preprocess_expr_helper(&mut n.expr, start, end),
        Expr::Unary(n) => preprocess_expr_helper(&mut n.expr, start, end),
        Expr::StringLiteral(_) | Expr::NumberLiteral(_) => (true, false),
        other => panic!("found unexpected node {}", crate::durations::go_type_name(other)),
    }
}

/// Go `detectHistogramStatsDecoding`: sets `skip_histogram_buckets` on selectors whose histograms only need
/// count and sum.
pub fn detect_histogram_stats_decoding(expr: &mut Expr) {
    #[derive(Clone, Copy)]
    enum Anc {
        Subquery,
        SkipFn,
        NeedFn,
        CountValues,
        NeedBinop,
        Other,
    }
    fn walk(e: &mut Expr, path: &mut Vec<Anc>) {
        if let Expr::VectorSelector(n) = e {
            for a in path.iter().rev() {
                match a {
                    Anc::Subquery | Anc::NeedFn | Anc::CountValues | Anc::NeedBinop => {
                        n.skip_histogram_buckets = false;
                        break;
                    }
                    Anc::SkipFn => n.skip_histogram_buckets = true,
                    Anc::Other => {}
                }
            }
            return;
        }
        let anc = match &*e {
            Expr::Subquery(_) => Anc::Subquery,
            Expr::Call(c) => match &*c.func.name {
                "histogram_count" | "histogram_sum" | "histogram_avg" | "timestamp" | "start_timestamp" | "absent" => {
                    Anc::SkipFn
                }
                "histogram_quantile" | "histogram_quantiles" | "histogram_fraction" | "histogram_stddev"
                | "histogram_stdvar" | "changes" => Anc::NeedFn,
                _ => Anc::Other,
            },
            Expr::Aggregate(a) if a.op == COUNT_VALUES => Anc::CountValues,
            Expr::Binary(b) if matches!(b.op, TRIM_UPPER | TRIM_LOWER | EQLC | NEQ) => Anc::NeedBinop,
            _ => Anc::Other,
        };
        path.push(anc);
        let _ = for_each_child_mut(e, &mut |c| -> Result<(), ()> {
            walk(c, path);
            Ok(())
        });
        path.pop();
    }
    walk(expr, &mut Vec::new());
}
