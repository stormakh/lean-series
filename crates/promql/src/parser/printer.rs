//! Port of `promql/parser/printer.go`: `String()` of the AST nodes. The output parses back to the same AST.

use std::fmt;

use model::labels::{METRIC_NAME, MatchType};

use super::ast::*;
use super::duration::{go_f64_to_i64, write_duration};
use super::generated::*;
use super::lex::label_name_requires_quoting;
use super::quote::go_quote;

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::with_capacity(64);
        self.write_to(&mut s);
        f.write_str(&s)
    }
}

/// Go `strconv.FormatFloat(f, 'f', -1, 64)`.
pub(crate) fn write_float_f(out: &mut String, f: f64) {
    use std::fmt::Write;
    if f.is_infinite() {
        out.push_str(if f > 0.0 { "+Inf" } else { "-Inf" });
    } else if f.is_nan() {
        out.push_str("NaN");
    } else {
        let _ = write!(out, "{}", f);
    }
}

/// Go `fmt` `%.3f` of a timestamp in milliseconds, as seconds.
fn write_timestamp(out: &mut String, ts: i64) {
    use std::fmt::Write;
    let _ = write!(out, "{:.3}", ts as f64 / 1000.0);
}

fn write_labels(out: &mut String, ss: &[String], unquoted_utf8_names: bool) {
    for (i, s) in ss.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        if label_name_requires_quoting(s, unquoted_utf8_names) {
            model::unicode::append_quote(out, s);
        } else {
            out.push_str(s);
        }
    }
}

fn write_offset(out: &mut String, expr: &Option<Box<DurationExpr>>, offset: i64) {
    if let Some(e) = expr {
        out.push_str(" offset ");
        e.write_to(out);
    } else if offset > 0 {
        out.push_str(" offset ");
        write_duration(out, offset);
    } else if offset < 0 {
        out.push_str(" offset -");
        write_duration(out, offset.wrapping_neg());
    }
}

fn write_at(out: &mut String, timestamp: Option<i64>, start_or_end: super::lex::ItemType) {
    if let Some(ts) = timestamp {
        out.push_str(" @ ");
        write_timestamp(out, ts);
    } else if start_or_end == START {
        out.push_str(" @ start()");
    } else if start_or_end == END {
        out.push_str(" @ end()");
    }
}

impl Expr {
    /// Appends the String() form of the expression.
    pub fn write_to(&self, out: &mut String) {
        match self {
            Expr::Aggregate(n) => n.write_to(out),
            Expr::Binary(n) => {
                n.lhs.write_to(out);
                out.push(' ');
                write_op(out, n.op);
                n.write_bool_and_matching(out);
                out.push(' ');
                n.rhs.write_to(out);
            }
            Expr::Call(n) => {
                out.push_str(&n.func.name);
                out.push('(');
                write_expressions(out, &n.args);
                out.push(')');
            }
            Expr::MatrixSelector(n) => n.write_to(out),
            Expr::Subquery(n) => {
                n.expr.write_to(out);
                n.write_time_suffix(out);
            }
            Expr::NumberLiteral(n) => n.write_to(out),
            Expr::Paren(n) => {
                out.push('(');
                n.expr.write_to(out);
                out.push(')');
            }
            Expr::StringLiteral(n) => match &n.invalid_utf8 {
                Some(b) => out.push_str(&super::quote::go_quote_bytes(b)),
                None => model::unicode::append_quote(out, &n.val),
            },
            Expr::Unary(n) => {
                write_op(out, n.op);
                n.expr.write_to(out);
            }
            Expr::VectorSelector(n) => n.write_to(out),
            Expr::StepInvariant(n) => n.expr.write_to(out),
            Expr::Duration(n) => n.write_to(out),
        }
    }

    /// The String() form of the expression without its children, where Go has `ShortString`.
    pub fn short_string(&self) -> String {
        let mut s = String::new();
        match self {
            Expr::Aggregate(n) => n.write_agg_op_str(&mut s),
            Expr::Binary(n) => {
                write_op(&mut s, n.op);
                n.write_bool_and_matching(&mut s);
            }
            Expr::Call(n) => s.push_str(&n.func.name),
            Expr::MatrixSelector(n) => s = n.short_string(),
            Expr::Subquery(n) => n.write_time_suffix(&mut s),
            Expr::Unary(n) => write_op(&mut s, n.op),
            Expr::Duration(n) => write_op(&mut s, n.op),
            _ => self.write_to(&mut s),
        }
        s
    }
}

/// Go `Expressions.String`.
pub fn write_expressions(out: &mut String, es: &[Expr]) {
    for (i, e) in es.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        e.write_to(out);
    }
}

impl AggregateExpr {
    pub fn write_to(&self, out: &mut String) {
        self.write_agg_op_str(out);
        out.push('(');
        // A failed parse (e.g. missing or too few arguments) can leave Param or Expr nil. Print only the parts
        // that were parsed.
        if self.op.is_aggregator_with_param() {
            if let Some(p) = &self.param {
                p.write_to(out);
                if self.expr.is_some() {
                    out.push_str(", ");
                }
            }
        }
        if let Some(e) = &self.expr {
            e.write_to(out);
        }
        out.push(')');
    }

    pub(crate) fn write_agg_op_str(&self, out: &mut String) {
        write_op(out, self.op);
        if self.without {
            out.push_str(" without (");
            write_labels(out, &self.grouping, self.unquoted_utf8_names);
            out.push_str(") ");
        } else if !self.grouping.is_empty() {
            out.push_str(" by (");
            write_labels(out, &self.grouping, self.unquoted_utf8_names);
            out.push_str(") ");
        }
    }
}

impl BinaryExpr {
    pub(crate) fn write_bool_and_matching(&self, out: &mut String) {
        if self.return_bool {
            out.push_str(" bool");
        }
        self.write_matching(out);
    }

    /// Go `getMatchingStr`.
    pub(crate) fn write_matching(&self, out: &mut String) {
        let Some(vm) = &self.vector_matching else { return };
        let grouped = vm.card == VectorMatchCardinality::ManyToOne || vm.card == VectorMatchCardinality::OneToMany;
        if !vm.matching_labels.is_empty() || vm.on || grouped {
            out.push_str(if vm.on { " on (" } else { " ignoring (" });
            write_labels(out, &vm.matching_labels, self.unquoted_utf8_names);
            out.push(')');
        }
        if grouped {
            out.push_str(if vm.card == VectorMatchCardinality::ManyToOne { " group_left (" } else { " group_right (" });
            write_labels(out, &vm.include, self.unquoted_utf8_names);
            out.push(')');
        }
        let fv = vm.fill_values;
        match (fv.lhs, fv.rhs) {
            (Some(l), Some(r)) if l == r => {
                out.push_str(" fill (");
                model::histogram::gofmt::write_g(out, l);
                out.push(')');
            }
            (l, r) => {
                if let Some(l) = l {
                    out.push_str(" fill_left (");
                    model::histogram::gofmt::write_g(out, l);
                    out.push(')');
                }
                if let Some(r) = r {
                    out.push_str(" fill_right (");
                    model::histogram::gofmt::write_g(out, r);
                    out.push(')');
                }
            }
        }
    }
}

impl DurationExpr {
    pub fn write_to(&self, out: &mut String) {
        if self.wrapped {
            out.push('(');
        }
        let lhs_rhs = |out: &mut String| {
            if let Some(l) = &self.lhs {
                l.write_to(out);
            }
            out.push_str(", ");
            if let Some(r) = &self.rhs {
                r.write_to(out);
            }
        };
        if self.op == STEP {
            out.push_str("step()");
        } else if self.op == RANGE {
            out.push_str("range()");
        } else if self.op == MIN_OF {
            out.push_str("min_of(");
            lhs_rhs(out);
            out.push(')');
        } else if self.op == MAX_OF {
            out.push_str("max_of(");
            lhs_rhs(out);
            out.push(')');
        } else if self.lhs.is_none() {
            // A unary duration expression.
            if self.op == SUB {
                write_op(out, self.op);
            } else if self.op != ADD {
                panic!("unexpected unary duration expression: {}", self.op);
            }
            if let Some(r) = &self.rhs {
                r.write_to(out);
            }
        } else {
            if let Some(l) = &self.lhs {
                l.write_to(out);
            }
            out.push(' ');
            write_op(out, self.op);
            out.push(' ');
            if let Some(r) = &self.rhs {
                r.write_to(out);
            }
        }
        if self.wrapped {
            out.push(')');
        }
    }
}

impl fmt::Display for DurationExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::new();
        self.write_to(&mut s);
        f.write_str(&s)
    }
}

fn write_range(out: &mut String, range: i64, range_expr: &Option<Box<DurationExpr>>) {
    match range_expr {
        Some(e) => e.write_to(out),
        None => write_duration(out, range),
    }
}

impl MatrixSelector {
    fn write_at_offset(&self, out: &mut String) {
        // A failed parse can leave a non-vector expression here (e.g. `1[5m]`). Such an operand cannot carry
        // @ or offset modifiers, so there is nothing to print.
        if let Expr::VectorSelector(vs) = &self.vector_selector {
            write_at(out, vs.timestamp, vs.start_or_end);
            write_offset(out, &vs.original_offset_expr, vs.original_offset);
        }
    }

    pub fn write_to(&self, out: &mut String) {
        let Expr::VectorSelector(vs) = &self.vector_selector else {
            self.vector_selector.write_to(out);
            out.push('[');
            write_range(out, self.range, &self.range_expr);
            out.push(']');
            return;
        };
        // Print the vector selector without its @, offset and other modifiers, then them after the range.
        vs.write_base(out);
        out.push('[');
        write_range(out, self.range, &self.range_expr);
        out.push(']');
        if vs.anchored {
            out.push_str(" anchored");
        } else if vs.smoothed {
            out.push_str(" smoothed");
        }
        self.write_at_offset(out);
    }

    /// Go `MatrixSelector.ShortString`.
    pub fn short_string(&self) -> String {
        let mut out = String::from("[");
        write_range(&mut out, self.range, &self.range_expr);
        out.push(']');
        self.write_at_offset(&mut out);
        out
    }
}

impl SubqueryExpr {
    /// Go `getSubqueryTimeSuffix`: `[<range>:<step>] @ <timestamp> offset <offset>`.
    pub fn write_time_suffix(&self, out: &mut String) {
        out.push('[');
        write_range(out, self.range, &self.range_expr);
        out.push(':');
        if let Some(e) = &self.step_expr {
            e.write_to(out);
        } else if self.step != 0 {
            write_duration(out, self.step);
        }
        out.push(']');
        write_at(out, self.timestamp, self.start_or_end);
        write_offset(out, &self.original_offset_expr, self.original_offset);
    }
}

impl NumberLiteral {
    pub fn write_to(&self, out: &mut String) {
        if self.duration {
            if self.val < 0.0 {
                out.push('-');
                write_duration(out, go_f64_to_i64(-self.val * 1e9));
            } else {
                write_duration(out, go_f64_to_i64(self.val * 1e9));
            }
            return;
        }
        write_float_f(out, self.val);
    }
}

impl VectorSelector {
    /// The name and matchers.
    fn write_base(&self, out: &mut String) {
        let mut label_strings: Vec<String> = Vec::with_capacity(self.label_matchers.len());
        for m in &self.label_matchers {
            // Only include the __name__ label if its equality matching and matches the name, but don't skip if
            // it's an explicit empty name matcher.
            if m.name == METRIC_NAME && m.typ == MatchType::Equal && m.value == self.name && !m.value.is_empty() {
                continue;
            }
            if !label_name_requires_quoting(&m.name, self.unquoted_utf8_names) {
                let mut s = String::with_capacity(m.name.len() + m.value.len() + 4);
                s.push_str(&m.name);
                s.push_str(m.typ.as_str());
                model::unicode::append_quote(&mut s, &m.value);
                label_strings.push(s);
            } else {
                label_strings.push(m.to_string());
            }
        }
        out.push_str(&self.name);
        if !label_strings.is_empty() {
            out.push('{');
            label_strings.sort();
            for (i, s) in label_strings.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(s);
            }
            out.push('}');
        }
    }

    pub fn write_to(&self, out: &mut String) {
        self.write_base(out);
        write_at(out, self.timestamp, self.start_or_end);
        if self.anchored {
            out.push_str(" anchored");
        } else if self.smoothed {
            out.push_str(" smoothed");
        }
        write_offset(out, &self.original_offset_expr, self.original_offset);
    }
}

macro_rules! display_via_write_to {
    ($($t:ty),*) => {$(
        impl fmt::Display for $t {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut s = String::new();
                self.write_to(&mut s);
                f.write_str(&s)
            }
        }
    )*};
}

display_via_write_to!(AggregateExpr, MatrixSelector, NumberLiteral, VectorSelector);

/// A string of the tree structure of the node (Go `Tree`).
pub fn tree(node: &Expr) -> String {
    let mut out = String::new();
    tree_into(&mut out, node, "");
    out
}

fn type_name(e: &Expr) -> &'static str {
    match e {
        Expr::Aggregate(_) => "AggregateExpr",
        Expr::Binary(_) => "BinaryExpr",
        Expr::Call(_) => "Call",
        Expr::MatrixSelector(_) => "MatrixSelector",
        Expr::Subquery(_) => "SubqueryExpr",
        Expr::NumberLiteral(_) => "NumberLiteral",
        Expr::Paren(_) => "ParenExpr",
        Expr::StringLiteral(_) => "StringLiteral",
        Expr::Unary(_) => "UnaryExpr",
        Expr::VectorSelector(_) => "VectorSelector",
        Expr::StepInvariant(_) => "StepInvariantExpr",
        Expr::Duration(_) => "DurationExpr",
    }
}

fn tree_into(out: &mut String, node: &Expr, level: &str) {
    out.push_str(level);
    out.push_str(" |---- ");
    out.push_str(type_name(node));
    out.push_str(" :: ");
    node.write_to(out);
    out.push('\n');
    let level = format!("{} · · ·", level);
    node.for_each_child(|e| {
        tree_into(out, e, &level);
        true
    });
}

/// Go `strconv.Quote` of a string literal value.
pub fn quote_string(s: &str) -> String {
    go_quote(s)
}

/// Appends the String() form of an operator or keyword without allocating.
pub(crate) fn write_op(out: &mut String, op: super::lex::ItemType) {
    match op.type_str() {
        Some(s) => out.push_str(s),
        None => out.push_str(&op.to_string()),
    }
}
