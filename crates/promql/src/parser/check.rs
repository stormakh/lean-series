//! Port of `checkAST` and `expectType` from `promql/parser/parse.go`: type checks of a parsed AST.

use model::labels::METRIC_NAME;

use super::actions::YyParser;
use super::ast::merge_ranges;
use super::ast::*;
use super::generated::*;
use super::lex::is_space_byte;
use super::posrange::PositionRange;
use super::quote::go_quote;
use super::value::{ValueType, documented_type};

impl YyParser<'_, '_> {
    /// Checks the type of the node and records an error if it is not of the expected type.
    fn expect_type(&mut self, node: &mut Expr, want: ValueType, context: &str) {
        let t = self.check_ast(node);
        if t != want {
            self.add_parse_err(
                node.position_range(),
                format!("expected type {} in {}, got {}", documented_type(want), context, documented_type(t)),
            );
        }
    }

    /// Checks the validity of the AST, including types, and returns the type of the node.
    pub(crate) fn check_ast(&mut self, node: &mut Expr) -> ValueType {
        let typ = node.value_type();
        // Position ranges are computed only for errors: the range of a deep left-nested binary expression walks
        // its left spine.
        if let Expr::StepInvariant(_) | Expr::Duration(_) = node {
            let t =
                if let Expr::StepInvariant(_) = node { "*parser.StepInvariantExpr" } else { "*parser.DurationExpr" };
            let pr = node.position_range();
            self.add_parse_err(pr, format!("unknown node type: {}", t));
            return typ;
        }
        match node {
            Expr::Aggregate(n) => {
                if !n.op.is_aggregator() {
                    self.add_parse_err(
                        n.pos_range,
                        format!(
                            "aggregation operator expected in aggregation expression but got {}",
                            go_quote(&n.op.to_string())
                        ),
                    );
                }
                if let Some(e) = &mut n.expr {
                    self.expect_type(e, ValueType::Vector, "aggregation expression");
                }
                if matches!(n.op, TOPK | BOTTOMK | QUANTILE | LIMITK | LIMIT_RATIO) {
                    if let Some(p) = &mut n.param {
                        self.expect_type(p, ValueType::Scalar, "aggregation parameter");
                    }
                }
                if n.op == COUNT_VALUES {
                    if let Some(p) = &mut n.param {
                        self.expect_type(p, ValueType::String, "aggregation parameter");
                    }
                }
            }
            Expr::Binary(n) => {
                let lt = self.check_ast(&mut n.lhs);
                let rt = self.check_ast(&mut n.rhs);
                let input = self.input().as_bytes();
                // The position range of the operator, without the surrounding spaces.
                let op_range = |n: &BinaryExpr| {
                    let mut r =
                        PositionRange { start: n.lhs.position_range().end, end: n.rhs.position_range().start - 1 };
                    while is_space_byte(input[r.start as usize]) {
                        r.start += 1;
                    }
                    while is_space_byte(input[r.end as usize]) {
                        r.end -= 1;
                    }
                    r
                };

                if n.return_bool && !n.op.is_comparison_operator() {
                    self.add_parse_err(op_range(n), "bool modifier can only be used on comparison operators".into());
                }
                if n.op.is_comparison_operator()
                    && !n.return_bool
                    && n.rhs.value_type() == ValueType::Scalar
                    && n.lhs.value_type() == ValueType::Scalar
                {
                    self.add_parse_err(op_range(n), "comparisons between scalars must use BOOL modifier".into());
                }
                if let Some(vm) = &mut n.vector_matching {
                    if n.op.is_set_operator() && vm.card == VectorMatchCardinality::OneToOne {
                        vm.card = VectorMatchCardinality::ManyToMany;
                    }
                }
                if let Some(vm) = &n.vector_matching {
                    for l1 in &vm.matching_labels {
                        for l2 in &vm.include {
                            if l1 == l2 && vm.on {
                                self.add_parse_err(
                                    op_range(n),
                                    format!("label {} must not occur in ON and GROUP clause at once", go_quote(l1)),
                                );
                            }
                        }
                    }
                }
                if !n.op.is_operator() {
                    self.add_parse_err(
                        merge_ranges(&n.lhs, &n.rhs),
                        format!("binary expression does not support operator {}", go_quote(&n.op.to_string())),
                    );
                }
                if lt != ValueType::Scalar && lt != ValueType::Vector {
                    self.add_parse_err(
                        n.lhs.position_range(),
                        "binary expression must contain only scalar and instant vector types".into(),
                    );
                }
                if rt != ValueType::Scalar && rt != ValueType::Vector {
                    self.add_parse_err(
                        n.rhs.position_range(),
                        "binary expression must contain only scalar and instant vector types".into(),
                    );
                }

                if (lt != ValueType::Vector || rt != ValueType::Vector) && n.vector_matching.is_some() {
                    let vm = n.vector_matching.take().unwrap_or_default();
                    if !vm.matching_labels.is_empty() {
                        self.add_parse_err(
                            merge_ranges(&n.lhs, &n.rhs),
                            "vector matching only allowed between instant vectors".into(),
                        );
                    }
                    if vm.fill_values.lhs.is_some() || vm.fill_values.rhs.is_some() {
                        self.add_parse_err(
                            merge_ranges(&n.lhs, &n.rhs),
                            "filling in missing series only allowed between instant vectors".into(),
                        );
                    }
                } else if n.op.is_set_operator() {
                    // Both operands are vectors.
                    if let Some(vm) = &n.vector_matching {
                        let card = vm.card;
                        let fill = vm.fill_values;
                        if card == VectorMatchCardinality::OneToMany || card == VectorMatchCardinality::ManyToOne {
                            self.add_parse_err(
                                merge_ranges(&n.lhs, &n.rhs),
                                format!("no grouping allowed for {} operation", go_quote(&n.op.to_string())),
                            );
                        }
                        if card != VectorMatchCardinality::ManyToMany {
                            self.add_parse_err(
                                merge_ranges(&n.lhs, &n.rhs),
                                "set operations must always be many-to-many".into(),
                            );
                        }
                        if fill.lhs.is_some() || fill.rhs.is_some() {
                            self.add_parse_err(
                                merge_ranges(&n.lhs, &n.rhs),
                                "filling in missing series not allowed for set operators".into(),
                            );
                        }
                    }
                }

                if (lt == ValueType::Scalar || rt == ValueType::Scalar) && n.op.is_set_operator() {
                    self.add_parse_err(
                        merge_ranges(&n.lhs, &n.rhs),
                        format!("set operator {} not allowed in binary scalar expression", go_quote(&n.op.to_string())),
                    );
                }
            }
            Expr::Call(n) => {
                let nargs = n.func.arg_types.len();
                let variadic = n.func.variadic;
                if variadic == 0 {
                    if nargs != n.args.len() {
                        self.add_parse_err(
                            n.pos_range,
                            format!(
                                "expected {} argument(s) in call to {}, got {}",
                                nargs,
                                go_quote(&n.func.name),
                                n.args.len()
                            ),
                        );
                    }
                } else {
                    let na = nargs as i64 - 1;
                    if na > n.args.len() as i64 {
                        self.add_parse_err(
                            n.pos_range,
                            format!(
                                "expected at least {} argument(s) in call to {}, got {}",
                                na,
                                go_quote(&n.func.name),
                                n.args.len()
                            ),
                        );
                    } else {
                        let nargsmax = na + variadic as i64;
                        if variadic > 0 && nargsmax < n.args.len() as i64 {
                            self.add_parse_err(
                                n.pos_range,
                                format!(
                                    "expected at most {} argument(s) in call to {}, got {}",
                                    nargsmax,
                                    go_quote(&n.func.name),
                                    n.args.len()
                                ),
                            );
                        }
                    }
                }

                if n.func.name == "info" && n.args.len() > 1 {
                    // Check the type is correct first.
                    let t1 = n.args[1].value_type();
                    if t1 != ValueType::Vector {
                        self.add_parse_err(
                            n.pos_range,
                            format!(
                                "expected type {} in call to function {}, got {}",
                                documented_type(ValueType::Vector),
                                go_quote(&n.func.name),
                                documented_type(t1)
                            ),
                        );
                    }
                    // Check the vector selector in the input doesn't contain a metric name.
                    let arg_range = n.args[1].position_range();
                    match &mut n.args[1] {
                        Expr::VectorSelector(vs) if !vs.name.is_empty() => {
                            self.add_parse_err(
                                arg_range,
                                "expected label selectors only, got vector selector instead".into(),
                            );
                        }
                        // Set the flag to bypass the empty matcher check.
                        Expr::VectorSelector(vs) => vs.bypass_empty_matcher_check = true,
                        _ => self.add_parse_err(arg_range, "expected label selectors only".into()),
                    }
                }

                let func = n.func.clone();
                for (i, arg) in n.args.iter_mut().enumerate() {
                    let mut i = i;
                    if i >= func.arg_types.len() {
                        if func.variadic == 0 {
                            // This is not a vararg function so we should not check the type of the extra
                            // arguments.
                            break;
                        }
                        i = func.arg_types.len() - 1;
                    }
                    let t = self.check_ast(arg);
                    if t != func.arg_types[i] {
                        self.add_parse_err(
                            arg.position_range(),
                            format!(
                                "expected type {} in call to function {}, got {}",
                                documented_type(func.arg_types[i]),
                                go_quote(&func.name),
                                documented_type(t)
                            ),
                        );
                    }
                }
            }
            Expr::Paren(n) => {
                self.check_ast(&mut n.expr);
            }
            Expr::Unary(n) => {
                if n.op != ADD && n.op != SUB {
                    self.add_parse_err(unary_range(n), "only + and - operators allowed for unary expressions".into());
                }
                let t = self.check_ast(&mut n.expr);
                if t != ValueType::Scalar && t != ValueType::Vector {
                    self.add_parse_err(
                        unary_range(n),
                        format!(
                            "unary expression only allowed on expressions of type scalar or instant vector, got {}",
                            go_quote(documented_type(t))
                        ),
                    );
                }
            }
            Expr::Subquery(n) => {
                let ty = self.check_ast(&mut n.expr);
                if ty != ValueType::Vector {
                    self.add_parse_err(
                        PositionRange { start: n.expr.position_range().start, end: n.end_pos },
                        format!("subquery is only allowed on instant vector, got {} instead", ty),
                    );
                }
            }
            Expr::MatrixSelector(n) => {
                self.check_ast(&mut n.vector_selector);
            }
            Expr::VectorSelector(n) => {
                if !n.name.is_empty() {
                    // In this case the last label matcher is checking for the metric name set outside the
                    // braces. This checks if the name has already been set previously.
                    let k = n.label_matchers.len().saturating_sub(1);
                    for m in &n.label_matchers[..k] {
                        if m.name == METRIC_NAME {
                            self.add_parse_err(
                                n.pos_range,
                                format!(
                                    "metric name must not be set twice: {} or {}",
                                    go_quote(&n.name),
                                    go_quote(&m.value)
                                ),
                            );
                        }
                    }
                    // Skip the check for non-empty matchers because an explicit metric name is a non-empty
                    // matcher.
                } else if !n.bypass_empty_matcher_check {
                    // A vector selector must contain at least one non-empty matcher to prevent implicit
                    // selection of all metrics (e.g. by a typo).
                    if !n.label_matchers.iter().any(|lm| !lm.matches("")) {
                        self.add_parse_err(
                            n.pos_range,
                            "vector selector must contain at least one non-empty matcher".into(),
                        );
                    }
                }
            }
            Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::StepInvariant(_) | Expr::Duration(_) => {}
        }
        typ
    }
}

fn unary_range(n: &UnaryExpr) -> PositionRange {
    PositionRange { start: n.start_pos, end: n.expr.position_range().end }
}
