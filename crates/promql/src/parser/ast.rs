//! Port of `promql/parser/ast.go`: the expression tree.
//!
//! An [`Expr`] is a 16-byte enum whose variants box their node, so a node costs one allocation (as a Go pointer
//! does) and child expressions are stored inline in their parent. Functions are resolved at parse time: a
//! [`Call`] holds an `Arc<Function>` with a [`super::FunctionId`] the engine can dispatch on.

use std::sync::Arc;

use model::labels::{METRIC_NAME, Matcher};

use super::functions::Function;
use super::generated::*;
use super::lex::ItemType;
use super::posrange::{Pos, PositionRange};
use super::value::ValueType;

/// An expression node.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Aggregate(Box<AggregateExpr>),
    Binary(Box<BinaryExpr>),
    Call(Box<Call>),
    MatrixSelector(Box<MatrixSelector>),
    Subquery(Box<SubqueryExpr>),
    NumberLiteral(Box<NumberLiteral>),
    Paren(Box<ParenExpr>),
    StringLiteral(Box<StringLiteral>),
    Unary(Box<UnaryExpr>),
    VectorSelector(Box<VectorSelector>),
    StepInvariant(Box<StepInvariantExpr>),
    Duration(Box<DurationExpr>),
}

/// An aggregation operation on a vector.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct AggregateExpr {
    /// The aggregation operator.
    pub op: ItemType,
    /// The vector expression over which is aggregated. `None` only in a partial AST after a parse error.
    pub expr: Option<Expr>,
    /// The parameter of `topk`, `quantile` and the like.
    pub param: Option<Expr>,
    /// The labels by which to group the vector.
    pub grouping: Vec<String>,
    /// Whether to drop the given labels rather than keep them.
    pub without: bool,
    /// Whether the printer may emit unquoted Unicode letters and dots in names.
    pub unquoted_utf8_names: bool,
    pub pos_range: PositionRange,
}

/// A binary expression between two child expressions.
#[derive(Clone, Debug, PartialEq)]
pub struct BinaryExpr {
    /// The operation of the expression.
    pub op: ItemType,
    pub lhs: Expr,
    pub rhs: Expr,
    /// The matching behavior for the operation if both operands are vectors; `None` otherwise.
    pub vector_matching: Option<Box<VectorMatching>>,
    /// If a comparison operator, return 0/1 rather than filtering.
    pub return_bool: bool,
    pub unquoted_utf8_names: bool,
}

/// An arithmetic expression on durations, or `step()`, `range()`, `min_of()`, `max_of()`.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct DurationExpr {
    pub op: ItemType,
    pub lhs: Option<Expr>,
    pub rhs: Option<Expr>,
    /// Set when the duration is wrapped in parentheses.
    pub wrapped: bool,
    /// The start position of unary operations and duration function calls.
    pub start_pos: Pos,
    /// The end position of duration function calls, including the closing parenthesis.
    pub end_pos: Pos,
}

/// A function call.
#[derive(Clone, Debug, PartialEq)]
pub struct Call {
    pub func: Arc<Function>,
    pub args: Vec<Expr>,
    pub pos_range: PositionRange,
}

/// A range vector selection.
#[derive(Clone, Debug, PartialEq)]
pub struct MatrixSelector {
    /// A vector selector if the parser returned no error.
    pub vector_selector: Expr,
    /// The range in nanoseconds.
    pub range: i64,
    pub range_expr: Option<Box<DurationExpr>>,
    pub end_pos: Pos,
}

/// A subquery.
#[derive(Clone, Debug, PartialEq)]
pub struct SubqueryExpr {
    pub expr: Expr,
    /// The range in nanoseconds.
    pub range: i64,
    pub range_expr: Option<Box<DurationExpr>>,
    /// The offset that was set in the query, in nanoseconds.
    pub original_offset: i64,
    /// The offset expression that was set in the query.
    pub original_offset_expr: Option<Box<DurationExpr>>,
    /// The offset used during query execution, in nanoseconds (set by the engine).
    pub offset: i64,
    /// The `@` timestamp in milliseconds.
    pub timestamp: Option<i64>,
    /// `START` or `END` when `@ start()` or `@ end()` is used, else `ItemType(0)`.
    pub start_or_end: ItemType,
    /// The step in nanoseconds (0: default step).
    pub step: i64,
    pub step_expr: Option<Box<DurationExpr>>,
    pub end_pos: Pos,
}

/// A number.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct NumberLiteral {
    pub val: f64,
    /// Format the number as a duration.
    pub duration: bool,
    pub pos_range: PositionRange,
}

/// An expression in parentheses.
#[derive(Clone, Debug, PartialEq)]
pub struct ParenExpr {
    pub expr: Expr,
    pub pos_range: PositionRange,
}

/// A string.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct StringLiteral {
    pub val: String,
    pub pos_range: PositionRange,
}

/// A unary operation (`+` or `-`) on another expression.
#[derive(Clone, Debug, PartialEq)]
pub struct UnaryExpr {
    pub op: ItemType,
    pub expr: Expr,
    pub start_pos: Pos,
}

/// An expression that evaluates to the same result at every step. Only the engine creates it.
#[derive(Clone, Debug, PartialEq)]
pub struct StepInvariantExpr {
    pub expr: Expr,
}

/// An instant vector selection.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct VectorSelector {
    pub name: String,
    /// The offset that was set in the query, in nanoseconds.
    pub original_offset: i64,
    /// The offset expression that was set in the query.
    pub original_offset_expr: Option<Box<DurationExpr>>,
    /// The offset used during query execution, in nanoseconds (set by the engine).
    pub offset: i64,
    /// The `@` timestamp in milliseconds.
    pub timestamp: Option<i64>,
    /// Set when decoding native histogram buckets is not needed for query evaluation.
    pub skip_histogram_buckets: bool,
    /// `START` or `END` when `@ start()` or `@ end()` is used, else `ItemType(0)`.
    pub start_or_end: ItemType,
    pub label_matchers: Vec<Matcher>,
    /// True when the selector does not need a matcher that rejects the empty string (the second argument of
    /// `info`).
    pub bypass_empty_matcher_check: bool,
    pub anchored: bool,
    pub smoothed: bool,
    pub unquoted_utf8_names: bool,
    pub pos_range: PositionRange,
}

/// The cardinality relationship of two vectors in a binary operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum VectorMatchCardinality {
    #[default]
    OneToOne,
    ManyToOne,
    OneToMany,
    ManyToMany,
}

impl VectorMatchCardinality {
    pub fn as_str(self) -> &'static str {
        match self {
            VectorMatchCardinality::OneToOne => "one-to-one",
            VectorMatchCardinality::ManyToOne => "many-to-one",
            VectorMatchCardinality::OneToMany => "one-to-many",
            VectorMatchCardinality::ManyToMany => "many-to-many",
        }
    }
}

/// How elements of two vectors in a binary operation are matched.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct VectorMatching {
    pub card: VectorMatchCardinality,
    /// The labels which define equality of a pair of elements.
    pub matching_labels: Vec<String>,
    /// Whether `matching_labels` are included (`on`) rather than excluded (`ignoring`).
    pub on: bool,
    /// Labels to include from the side with the lower cardinality.
    pub include: Vec<String>,
    pub fill_values: VectorMatchFillValues,
}

/// The values for a series of one side that has no match on the other side; `None` means no fill.
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct VectorMatchFillValues {
    pub rhs: Option<f64>,
    pub lhs: Option<f64>,
}

impl Expr {
    /// The type the expression evaluates to.
    pub fn value_type(&self) -> ValueType {
        match self {
            Expr::Aggregate(_) => ValueType::Vector,
            Expr::Call(c) => c.func.return_type,
            Expr::MatrixSelector(_) | Expr::Subquery(_) => ValueType::Matrix,
            Expr::NumberLiteral(_) | Expr::Duration(_) => ValueType::Scalar,
            Expr::Paren(p) => p.expr.value_type(),
            Expr::StringLiteral(_) => ValueType::String,
            Expr::Unary(u) => u.expr.value_type(),
            Expr::VectorSelector(_) => ValueType::Vector,
            Expr::Binary(b) => {
                if b.lhs.value_type() == ValueType::Scalar && b.rhs.value_type() == ValueType::Scalar {
                    ValueType::Scalar
                } else {
                    ValueType::Vector
                }
            }
            Expr::StepInvariant(s) => s.expr.value_type(),
        }
    }

    /// The position of the node in the query string.
    pub fn position_range(&self) -> PositionRange {
        match self {
            Expr::Aggregate(e) => e.pos_range,
            Expr::Binary(e) => merge_ranges(&e.lhs, &e.rhs),
            Expr::Call(e) => e.pos_range,
            Expr::MatrixSelector(e) => e.position_range(),
            Expr::Subquery(e) => PositionRange { start: e.expr.position_range().start, end: e.end_pos },
            Expr::NumberLiteral(e) => e.pos_range,
            Expr::Paren(e) => e.pos_range,
            Expr::StringLiteral(e) => e.pos_range,
            Expr::Unary(e) => PositionRange { start: e.start_pos, end: e.expr.position_range().end },
            Expr::VectorSelector(e) => e.pos_range,
            Expr::StepInvariant(e) => e.expr.position_range(),
            Expr::Duration(e) => e.position_range(),
        }
    }

    /// Calls `f` for each child node, in Go `ChildrenIter` order. `f` returns false to stop.
    pub fn for_each_child<'a>(&'a self, mut f: impl FnMut(&'a Expr) -> bool) {
        match self {
            Expr::Aggregate(n) => {
                if let Some(e) = &n.expr {
                    if !f(e) {
                        return;
                    }
                }
                if let Some(p) = &n.param {
                    f(p);
                }
            }
            Expr::Binary(n) => {
                if f(&n.lhs) {
                    f(&n.rhs);
                }
            }
            Expr::Call(n) => {
                for a in &n.args {
                    if !f(a) {
                        return;
                    }
                }
            }
            Expr::Subquery(n) => {
                f(&n.expr);
            }
            Expr::Paren(n) => {
                f(&n.expr);
            }
            Expr::Unary(n) => {
                f(&n.expr);
            }
            Expr::MatrixSelector(n) => {
                f(&n.vector_selector);
            }
            Expr::StepInvariant(n) => {
                f(&n.expr);
            }
            // Go's ChildrenIter has no case for DurationExpr; it is a leaf here.
            Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::VectorSelector(_) | Expr::Duration(_) => {}
        }
    }

    /// The child nodes (Go `Children`).
    pub fn children(&self) -> Vec<&Expr> {
        let mut v = Vec::new();
        self.for_each_child(|e| {
            v.push(e);
            true
        });
        v
    }

    /// The vector selector, if this is one.
    pub fn as_vector_selector(&self) -> Option<&VectorSelector> {
        match self {
            Expr::VectorSelector(v) => Some(v),
            _ => None,
        }
    }

    /// The mutable vector selector, if this is one.
    pub fn as_vector_selector_mut(&mut self) -> Option<&mut VectorSelector> {
        match self {
            Expr::VectorSelector(v) => Some(v),
            _ => None,
        }
    }
}

impl MatrixSelector {
    pub fn position_range(&self) -> PositionRange {
        PositionRange { start: self.vector_selector.position_range().start, end: self.end_pos }
    }
}

impl DurationExpr {
    pub fn position_range(&self) -> PositionRange {
        if self.op == MIN_OF || self.op == MAX_OF || (self.rhs.is_none() && self.lhs.is_none()) {
            return PositionRange { start: self.start_pos, end: self.end_pos };
        }
        match (&self.lhs, &self.rhs) {
            (Some(l), None) => PositionRange { start: self.start_pos, end: l.position_range().end },
            (None, Some(r)) => PositionRange { start: self.start_pos, end: r.position_range().end },
            (Some(l), Some(r)) => merge_ranges(l, r),
            (None, None) => PositionRange { start: self.start_pos, end: self.end_pos },
        }
    }
}

/// Merges the position ranges of two nodes, given in input order.
pub(crate) fn merge_ranges(first: &Expr, last: &Expr) -> PositionRange {
    PositionRange { start: first.position_range().start, end: last.position_range().end }
}

/// The position range of a list of expressions (Go `Expressions.PositionRange`): -1, -1 when empty.
pub fn expressions_position_range(e: &[Expr]) -> PositionRange {
    match (e.first(), e.last()) {
        (Some(f), Some(l)) => merge_ranges(f, l),
        _ => PositionRange { start: -1, end: -1 },
    }
}

/// Visits a node and its children (Go `Visitor`).
pub trait Visitor<'a> {
    type Error;

    /// Called for each node with the path of its ancestors, and with `None` after the children of a node were
    /// visited. Returns whether to visit the children of `node`.
    fn visit(&mut self, node: Option<&'a Expr>, path: &[&'a Expr]) -> Result<bool, Self::Error>;
}

/// Traverses an AST in depth-first order (Go `Walk`). The first error stops the walk.
pub fn walk<'a, V: Visitor<'a>>(v: &mut V, node: &'a Expr, path: &mut Vec<&'a Expr>) -> Result<(), V::Error> {
    if !v.visit(Some(node), path)? {
        return Ok(());
    }
    path.push(node);
    let mut res = Ok(());
    node.for_each_child(|e| {
        res = walk(v, e, path);
        res.is_ok()
    });
    path.pop();
    res?;
    v.visit(None, &[])?;
    Ok(())
}

struct Inspector<F>(F);

impl<'a, E, F: FnMut(&'a Expr, &[&'a Expr]) -> Result<(), E>> Visitor<'a> for Inspector<F> {
    type Error = E;

    fn visit(&mut self, node: Option<&'a Expr>, path: &[&'a Expr]) -> Result<bool, E> {
        if let Some(n) = node {
            (self.0)(n, path)?;
        }
        Ok(true)
    }
}

/// Calls `f(node, path)` for each node in depth-first order (Go `Inspect`). An error from `f` stops the walk.
pub fn inspect<'a, E>(node: &'a Expr, f: impl FnMut(&'a Expr, &[&'a Expr]) -> Result<(), E>) -> Result<(), E> {
    let mut path = Vec::new();
    walk(&mut Inspector(f), node, &mut path)
}

/// The label matchers of every vector selector in the expression (Go `ExtractSelectors`).
pub fn extract_selectors(expr: &Expr) -> Vec<Vec<Matcher>> {
    let mut selectors = Vec::new();
    let _ = inspect(expr, |node, _| -> Result<(), ()> {
        if let Expr::VectorSelector(vs) = node {
            selectors.push(vs.label_matchers.clone());
        }
        Ok(())
    });
    selectors
}

/// Go `MustLabelMatcher`.
pub fn must_label_matcher(mt: model::labels::MatchType, name: &str, val: &str) -> Matcher {
    Matcher::must_new(mt, name, val)
}

impl VectorSelector {
    /// Whether the selector has an explicit metric name matcher for `name` (helper for engine use).
    pub fn name_matcher(&self) -> Option<&Matcher> {
        self.label_matchers.iter().find(|m| m.name == METRIC_NAME)
    }
}
