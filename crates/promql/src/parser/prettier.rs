//! Port of `promql/parser/prettier.go`: splits long expressions over several lines.
//!
//! A node that is longer than [`max_characters_per_line`] when printed puts its children on their own lines,
//! indented by two spaces per level.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::ast::*;
use super::printer::write_op;

static MAX_CHARACTERS_PER_LINE: AtomicUsize = AtomicUsize::new(100);

/// The line length above which the prettier splits a node (Go `maxCharactersPerLine`, 100).
pub fn max_characters_per_line() -> usize {
    MAX_CHARACTERS_PER_LINE.load(Ordering::Relaxed)
}

/// Sets the line length for the prettier. Go tests change the package variable; this is the same knob.
pub fn set_max_characters_per_line(n: usize) {
    MAX_CHARACTERS_PER_LINE.store(n, Ordering::Relaxed);
}

/// The prettified form of the expression (Go `Prettify`).
pub fn prettify(e: &Expr) -> String {
    e.pretty(0)
}

fn indent(n: usize) -> String {
    "  ".repeat(n)
}

fn needs_split(e: &Expr) -> bool {
    e.to_string().len() > max_characters_per_line()
}

impl Expr {
    /// The prettified form of the node at depth `level` (Go `Pretty`).
    pub fn pretty(&self, level: usize) -> String {
        match self {
            Expr::Aggregate(e) => {
                let mut s = indent(level);
                if !needs_split(self) {
                    self.write_to(&mut s);
                    return s;
                }
                e.write_agg_op_str(&mut s);
                s.push_str("(\n");
                if e.op.is_aggregator_with_param() {
                    if let Some(p) = &e.param {
                        s.push_str(&p.pretty(level + 1));
                    }
                    s.push_str(",\n");
                }
                if let Some(x) = &e.expr {
                    s.push_str(&x.pretty(level + 1));
                }
                s.push('\n');
                s.push_str(&indent(level));
                s.push(')');
                s
            }
            Expr::Binary(e) => {
                let mut s = indent(level);
                if !needs_split(self) {
                    self.write_to(&mut s);
                    return s;
                }
                let mut s = e.lhs.pretty(level + 1);
                s.push('\n');
                s.push_str(&indent(level));
                write_op(&mut s, e.op);
                e.write_bool_and_matching(&mut s);
                s.push('\n');
                s.push_str(&e.rhs.pretty(level + 1));
                s
            }
            Expr::Duration(e) => e.pretty(),
            Expr::Call(e) => {
                let mut s = indent(level);
                if !needs_split(self) {
                    self.write_to(&mut s);
                    return s;
                }
                s.push_str(&e.func.name);
                s.push_str("(\n");
                s.push_str(&pretty_expressions(&e.args, level + 1));
                s.push('\n');
                s.push_str(&indent(level));
                s.push(')');
                s
            }
            Expr::Paren(e) => {
                let mut s = indent(level);
                if !needs_split(self) {
                    self.write_to(&mut s);
                    return s;
                }
                s.push_str("(\n");
                s.push_str(&e.expr.pretty(level + 1));
                s.push('\n');
                s.push_str(&indent(level));
                s.push(')');
                s
            }
            Expr::StepInvariant(e) => e.expr.pretty(level),
            Expr::Subquery(e) => {
                if !needs_split(self) {
                    return self.to_string();
                }
                let mut s = e.expr.pretty(level);
                e.write_time_suffix(&mut s);
                s
            }
            Expr::Unary(e) => {
                let child = e.expr.pretty(level);
                // Remove the indent prefix from child since we attach the prefix indent before Op.
                let mut s = indent(level);
                write_op(&mut s, e.op);
                s.push_str(child.trim_matches(|c: char| c.is_whitespace()));
                s
            }
            Expr::MatrixSelector(_) | Expr::VectorSelector(_) | Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {
                let mut s = indent(level);
                self.write_to(&mut s);
                s
            }
        }
    }
}

/// Go `Expressions.Pretty`.
pub fn pretty_expressions(es: &[Expr], level: usize) -> String {
    let parts: Vec<String> = es.iter().map(|e| e.pretty(level)).collect();
    parts.join(",\n")
}

impl DurationExpr {
    /// Go `DurationExpr.Pretty`.
    pub fn pretty(&self) -> String {
        let rhs = self.rhs.as_ref().map(|r| r.pretty(0)).unwrap_or_default();
        let mut s = match &self.lhs {
            None => {
                // Unary plus is not printed, matching String(), so a parenthesised number lifted into a
                // unary-plus DurationExpr round-trips as (5).
                if self.op == super::generated::ADD && self.rhs.is_some() { rhs } else { format!("{}{}", self.op, rhs) }
            }
            Some(l) => format!("{} {} {}", l.pretty(0), self.op, rhs),
        };
        if self.wrapped {
            s = format!("({})", s);
        }
        s
    }
}
