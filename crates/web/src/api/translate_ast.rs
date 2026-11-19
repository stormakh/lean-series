//! Port of `web/api/v1/translate_ast.go`: the PromQL AST as JSON for the UI's tree view. Go builds maps, so the
//! keys of every object are sorted.

use json::Json;
use model::labels::Matcher;
use promql::parser::{DurationExpr, Expr, ItemType};

use super::jsonutil::write_float_f;
use super::types::value_type_name;

fn obj(mut pairs: Vec<(&str, Json)>) -> Json {
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    Json::Obj(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

fn float_f(v: f64) -> Json {
    let mut s = String::new();
    write_float_f(v, &mut s);
    Json::Str(s)
}

fn strs(l: &[String]) -> Json {
    Json::Arr(l.iter().map(|s| Json::Str(s.clone())).collect())
}

/// Go `time.Duration.Milliseconds()` of nanoseconds.
fn millis(ns: i64) -> Json {
    Json::Int(ns / 1_000_000)
}

fn timestamp(t: Option<i64>) -> Json {
    match t {
        Some(v) => Json::Int(v),
        None => Json::Null,
    }
}

fn start_or_end(t: ItemType) -> Json {
    if t == ItemType(0) { Json::Null } else { Json::Str(t.to_string()) }
}

fn matchers(ms: &[Matcher]) -> Json {
    Json::Arr(
        ms.iter()
            .map(|m| obj(vec![("name", Json::Str(m.name.clone())), ("value", Json::Str(m.value.clone())), ("type", Json::Str(m.typ.as_str().to_string()))]))
            .collect(),
    )
}

fn opt(e: Option<&Expr>) -> Json {
    match e {
        Some(e) => translate_ast(e),
        None => Json::Null,
    }
}

fn duration_expr(d: &DurationExpr) -> Json {
    obj(vec![
        ("type", Json::Str("durationExpr".to_string())),
        ("op", Json::Str(d.op.to_string())),
        ("lhs", opt_duration(d.lhs.as_ref())),
        ("rhs", opt_duration(d.rhs.as_ref())),
        ("wrapped", Json::Bool(d.wrapped)),
    ])
}

/// Go `translateDurationExpr`.
fn opt_duration(e: Option<&Expr>) -> Json {
    match e {
        None => Json::Null,
        Some(Expr::Duration(d)) => duration_expr(d),
        Some(Expr::NumberLiteral(n)) => obj(vec![
            ("type", Json::Str("numberLiteral".to_string())),
            ("val", float_f(n.val)),
            ("duration", Json::Bool(n.duration)),
        ]),
        Some(e) => translate_ast(e),
    }
}

fn opt_duration_box(d: Option<&Box<DurationExpr>>) -> Json {
    match d {
        Some(d) => duration_expr(d),
        None => Json::Null,
    }
}

/// Go `translateAST`.
pub fn translate_ast(node: &Expr) -> Json {
    match node {
        Expr::Aggregate(n) => obj(vec![
            ("type", Json::Str("aggregation".to_string())),
            ("op", Json::Str(n.op.to_string())),
            ("expr", opt(n.expr.as_ref())),
            ("param", opt(n.param.as_ref())),
            ("grouping", strs(&n.grouping)),
            ("without", Json::Bool(n.without)),
        ]),
        Expr::Binary(n) => {
            let matching = match &n.vector_matching {
                Some(m) => {
                    let fv = |v: Option<f64>| match v {
                        Some(f) => Json::Float(f),
                        None => Json::Null,
                    };
                    obj(vec![
                        ("card", Json::Str(m.card.as_str().to_string())),
                        ("labels", strs(&m.matching_labels)),
                        ("on", Json::Bool(m.on)),
                        ("include", strs(&m.include)),
                        ("fillValues", obj(vec![("lhs", fv(m.fill_values.lhs)), ("rhs", fv(m.fill_values.rhs))])),
                    ])
                }
                None => Json::Null,
            };
            obj(vec![
                ("type", Json::Str("binaryExpr".to_string())),
                ("op", Json::Str(n.op.to_string())),
                ("lhs", translate_ast(&n.lhs)),
                ("rhs", translate_ast(&n.rhs)),
                ("matching", matching),
                ("bool", Json::Bool(n.return_bool)),
            ])
        }
        Expr::Call(n) => obj(vec![
            ("type", Json::Str("call".to_string())),
            (
                "func",
                obj(vec![
                    ("name", Json::Str(n.func.name.to_string())),
                    ("argTypes", Json::Arr(n.func.arg_types.iter().map(|t| Json::Str(value_type_name(*t).to_string())).collect())),
                    ("variadic", Json::Int(n.func.variadic as i64)),
                    ("returnType", Json::Str(value_type_name(n.func.return_type).to_string())),
                ]),
            ),
            ("args", Json::Arr(n.args.iter().map(translate_ast).collect())),
        ]),
        Expr::MatrixSelector(n) => {
            let Expr::VectorSelector(vs) = &n.vector_selector else { return Json::Null };
            obj(vec![
                ("type", Json::Str("matrixSelector".to_string())),
                ("name", Json::Str(vs.name.clone())),
                ("range", millis(n.range)),
                ("rangeExpr", opt_duration_box(n.range_expr.as_ref())),
                ("offset", millis(vs.original_offset)),
                ("offsetExpr", opt_duration_box(vs.original_offset_expr.as_ref())),
                ("matchers", matchers(&vs.label_matchers)),
                ("timestamp", timestamp(vs.timestamp)),
                ("startOrEnd", start_or_end(vs.start_or_end)),
                ("anchored", Json::Bool(vs.anchored)),
                ("smoothed", Json::Bool(vs.smoothed)),
            ])
        }
        Expr::Subquery(n) => obj(vec![
            ("type", Json::Str("subquery".to_string())),
            ("expr", translate_ast(&n.expr)),
            ("range", millis(n.range)),
            ("rangeExpr", opt_duration_box(n.range_expr.as_ref())),
            ("offset", millis(n.original_offset)),
            ("offsetExpr", opt_duration_box(n.original_offset_expr.as_ref())),
            ("step", millis(n.step)),
            ("stepExpr", opt_duration_box(n.step_expr.as_ref())),
            ("timestamp", timestamp(n.timestamp)),
            ("startOrEnd", start_or_end(n.start_or_end)),
        ]),
        Expr::Duration(d) => duration_expr(d),
        Expr::NumberLiteral(n) => obj(vec![("type", Json::Str("numberLiteral".to_string())), ("val", float_f(n.val))]),
        Expr::Paren(n) => obj(vec![("type", Json::Str("parenExpr".to_string())), ("expr", translate_ast(&n.expr))]),
        Expr::StringLiteral(n) => obj(vec![("type", Json::Str("stringLiteral".to_string())), ("val", Json::Str(n.val.clone()))]),
        Expr::Unary(n) => obj(vec![
            ("type", Json::Str("unaryExpr".to_string())),
            ("op", Json::Str(n.op.to_string())),
            ("expr", translate_ast(&n.expr)),
        ]),
        Expr::VectorSelector(n) => obj(vec![
            ("type", Json::Str("vectorSelector".to_string())),
            ("name", Json::Str(n.name.clone())),
            ("offset", millis(n.original_offset)),
            ("offsetExpr", opt_duration_box(n.original_offset_expr.as_ref())),
            ("matchers", matchers(&n.label_matchers)),
            ("timestamp", timestamp(n.timestamp)),
            ("startOrEnd", start_or_end(n.start_or_end)),
            ("anchored", Json::Bool(n.anchored)),
            ("smoothed", Json::Bool(n.smoothed)),
        ]),
        Expr::StepInvariant(_) => panic!("unsupported node type"),
    }
}
