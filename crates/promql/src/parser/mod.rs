//! Port of `promql/parser`: the PromQL lexer, parser, AST, type checker, printer and prettier.
//!
//! The parser runs upstream's goyacc LALR tables ([`generated`], produced by `scripts/gen/promql_yacc_tables.py`
//! from `generated_parser.y.go`) with a port of the goyacc driver, so the accepted language, the AST, the error
//! texts and their positions, and the partial AST after an error are the same as in Go.

mod actions;
pub mod ast;
mod check;
pub mod duration;
pub mod functions;
mod functions_table;
pub mod generated;
pub mod lex;
pub mod posrange;
pub mod prettier;
pub mod printer;
pub mod quote;
pub mod value;

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};

use model::histogram::FloatHistogram;
use model::labels::{Labels, Matcher};

pub use ast::*;
pub use functions::{Function, FunctionId, FunctionMap, function_by_id, functions, must_get_function};
pub use generated::*;
pub use lex::{Item, ItemType, Lexer, keywords};
pub use posrange::{Pos, PositionRange};
pub use prettier::{max_characters_per_line, prettify, set_max_characters_per_line};
pub use printer::tree;
pub use value::{ValueType, documented_type};

use crate::features::{self, Collector};

/// The configuration of the parser.
#[derive(Clone, Debug, Default)]
pub struct Options {
    pub enable_experimental_functions: bool,
    pub enable_binop_fill_modifiers: bool,
    /// Allows Unicode letters and non-leading dots in unquoted names.
    pub enable_unquoted_utf8_names: bool,
    /// The functions the parser accepts, keyed by name. `None` uses [`functions()`]; otherwise it replaces the
    /// default set (to extend it, start from a clone of [`functions()`]). An empty map accepts no functions.
    pub functions: Option<FunctionMap>,
}

impl Options {
    /// The set of functions the parser accepts.
    pub fn functions(&self) -> &FunctionMap {
        self.functions.as_ref().unwrap_or_else(|| functions())
    }
}

/// A PromQL parser with its options (Go `Parser` made by `NewParser`).
#[derive(Clone, Debug, Default)]
pub struct Parser {
    options: Options,
}

/// A parse error with its position (Go `ParseErr`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseErr {
    pub position_range: PositionRange,
    /// The error text (Go `Err.Error()`).
    pub err: String,
    pub query: String,
    /// An additional line offset; only used inside unit tests.
    pub line_offset: i32,
}

impl fmt::Display for ParseErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: parse error: {}", self.position_range.start_pos_input(&self.query, self.line_offset), self.err)
    }
}

/// The errors of one parse (Go `ParseErrors`). Only the first is in the error text.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParseErrors(pub Vec<ParseErr>);

impl fmt::Display for ParseErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.first() {
            Some(e) => e.fmt(f),
            None => f.write_str("error contains no error message"),
        }
    }
}

/// An error of the parser.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// Syntax or type errors.
    Parse(ParseErrors),
    /// The parser recovered from an internal panic (Go `ErrUnexpected`).
    Unexpected,
    /// An error value the parser panicked with (Go `recover` returns it as is).
    Custom(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Parse(e) => e.fmt(f),
            Error::Unexpected => f.write_str("unexpected error"),
            Error::Custom(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    /// The parse errors, if this is a parse error.
    pub fn parse_errors(&self) -> Option<&ParseErrors> {
        match self {
            Error::Parse(e) => Some(e),
            _ => None,
        }
    }
}

/// Enriches a parse error or a list of them (Go `EnrichParseError`, used by promtool and tests).
pub fn enrich_parse_error(err: &mut Error, mut enrich: impl FnMut(&mut ParseErr)) {
    if let Error::Parse(errs) = err {
        for e in &mut errs.0 {
            enrich(e);
        }
    }
}

/// Runs `f` and turns a panic into an error (Go `parser.recover`): an [`Error`] payload is returned as is, any
/// other panic gives [`Error::Unexpected`].
pub fn recover<T>(f: impl FnOnce() -> Result<T, Error>) -> Result<T, Error> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(payload) => match payload.downcast::<Error>() {
            Ok(e) => Err(*e),
            Err(p) => {
                let msg = p
                    .downcast_ref::<String>()
                    .map(|s| s.as_str())
                    .or_else(|| p.downcast_ref::<&str>().copied())
                    .unwrap_or("");
                eprintln!("parser panic: {}", msg);
                Err(Error::Unexpected)
            }
        },
    }
}

/// An omittable value in a sequence of time series values.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct SequenceValue {
    pub value: f64,
    pub omitted: bool,
    pub histogram: Option<Box<FloatHistogram>>,
    /// True if the counter reset hint was set explicitly with `counter_reset_hint:...`.
    pub counter_reset_hint_set: bool,
}

impl fmt::Display for SequenceValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.omitted {
            return f.write_str("_");
        }
        if let Some(h) = &self.histogram {
            return f.write_str(&h.string());
        }
        f.write_str(&model::histogram::gofmt::f6(self.value))
    }
}

/// What the generated parser produced.
pub(crate) enum ParseResult {
    Expr(Expr),
    Labels(Labels),
    SeriesDesc(Labels, Vec<SequenceValue>),
}

/// The start symbols of the grammar.
#[derive(Clone, Copy)]
pub(crate) enum Start {
    Metric,
    SeriesDescription,
    Expression,
    MetricSelector,
}

impl Parser {
    /// A parser with the given options (Go `NewParser`).
    pub fn new(options: Options) -> Parser {
        Parser { options }
    }

    /// The options of the parser.
    pub fn options(&self) -> &Options {
        &self.options
    }

    /// Parses the input into an expression AST.
    pub fn parse_expr(&self, input: &str) -> Result<Expr, Error> {
        match self.parse_expr_partial(input) {
            (Some(e), None) => Ok(e),
            (_, Some(err)) => Err(err),
            (None, None) => Err(Error::Unexpected),
        }
    }

    /// Parses the input into an expression AST. On error the returned AST may still hold what was parsed, so
    /// that tooling can inspect or print it; it must not be evaluated.
    pub fn parse_expr_partial(&self, input: &str) -> (Option<Expr>, Option<Error>) {
        let mut expr = None;
        let res = recover(|| {
            let mut p = actions::YyParser::new(input, &self.options, false);
            let result = p.parse_generated(Start::Expression);
            if let Some(ParseResult::Expr(mut e)) = result {
                // Only typecheck when there are no syntax errors.
                if p.parse_errors.is_empty() {
                    p.check_ast(&mut e);
                }
                expr = Some(e);
            }
            if p.parse_errors.is_empty() {
                Ok(())
            } else {
                Err(Error::Parse(ParseErrors(std::mem::take(&mut p.parse_errors))))
            }
        });
        (expr, res.err())
    }

    /// Parses a metric with labels, such as `foo{a="b"}`, into labels.
    pub fn parse_metric(&self, input: &str) -> Result<Labels, Error> {
        recover(|| {
            let mut p = actions::YyParser::new(input, &self.options, false);
            let result = p.parse_generated(Start::Metric);
            if !p.parse_errors.is_empty() {
                return Err(Error::Parse(ParseErrors(std::mem::take(&mut p.parse_errors))));
            }
            match result {
                Some(ParseResult::Labels(l)) => Ok(l),
                _ => Ok(Labels::empty()),
            }
        })
    }

    /// Parses a metric selector, such as `foo{a="b"}`, into label matchers.
    pub fn parse_metric_selector(&self, input: &str) -> Result<Vec<Matcher>, Error> {
        recover(|| {
            let mut p = actions::YyParser::new(input, &self.options, false);
            let result = p.parse_generated(Start::MetricSelector);
            if !p.parse_errors.is_empty() {
                return Err(Error::Parse(ParseErrors(std::mem::take(&mut p.parse_errors))));
            }
            match result {
                Some(ParseResult::Expr(Expr::VectorSelector(vs))) => Ok(vs.label_matchers),
                _ => Ok(Vec::new()),
            }
        })
    }

    /// Parses several metric selectors.
    pub fn parse_metric_selectors<S: AsRef<str>>(&self, matchers: &[S]) -> Result<Vec<Vec<Matcher>>, Error> {
        let mut sets = Vec::with_capacity(matchers.len());
        for s in matchers {
            sets.push(self.parse_metric_selector(s.as_ref())?);
        }
        Ok(sets)
    }

    /// Parses a series description of promqltest and promtool, such as `foo{a="b"} 1+1x10 _ {{schema:0}}`.
    pub fn parse_series_desc(&self, input: &str) -> Result<(Labels, Vec<SequenceValue>), Error> {
        recover(|| {
            let mut p = actions::YyParser::new(input, &self.options, true);
            let result = p.parse_generated(Start::SeriesDescription);
            if !p.parse_errors.is_empty() {
                return Err(Error::Parse(ParseErrors(std::mem::take(&mut p.parse_errors))));
            }
            match result {
                Some(ParseResult::SeriesDesc(l, v)) => Ok((l, v)),
                _ => Ok((Labels::empty(), Vec::new())),
            }
        })
    }

    /// Registers all PromQL features with the feature registry (Go `RegisterFeatures`).
    pub fn register_features(&self, r: &dyn Collector) {
        // Core PromQL language keywords.
        for (keyword, t) in lex::KEYWORDS.iter() {
            if t.is_keyword() {
                match *keyword {
                    "fill" | "fill_left" | "fill_right" => {
                        r.set(features::PROMQL, keyword, self.options.enable_binop_fill_modifiers)
                    }
                    _ => r.enable(features::PROMQL, keyword),
                }
            }
        }
        // Operators.
        for o in (operatorsStart.0 + 1)..operatorsEnd.0 {
            let o = ItemType(o);
            if o.is_operator() {
                r.set(features::PROMQL_OPERATORS, &o.to_string(), true);
            }
        }
        // Aggregators.
        for a in (aggregatorsStart.0 + 1)..aggregatorsEnd.0 {
            let a = ItemType(a);
            if a.is_aggregator() {
                let experimental = a.is_experimental_aggregator() && !self.options.enable_experimental_functions;
                r.set(features::PROMQL_OPERATORS, &a.to_string(), !experimental);
            }
        }
        // Functions.
        for (name, f) in self.options.functions() {
            r.set(features::PROMQL_FUNCTIONS, name, !f.experimental || self.options.enable_experimental_functions);
        }
        r.enable(features::PROMQL, "duration_expr");
        r.set(features::PROMQL, "unquoted_utf8_names", self.options.enable_unquoted_utf8_names);
    }
}

/// Parses an expression with default options.
pub fn parse_expr(input: &str) -> Result<Expr, Error> {
    Parser::default().parse_expr(input)
}

/// Parses a metric selector with default options.
pub fn parse_metric_selector(input: &str) -> Result<Vec<Matcher>, Error> {
    Parser::default().parse_metric_selector(input)
}
