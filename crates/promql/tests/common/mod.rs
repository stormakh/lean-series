//! Helpers for the promql/parser tests: fixtures dumped from the upstream test tables
//! (`scripts/gen/promql_parser_fixtures.sh`) and a dump of the Rust AST in the same JSON form.

#![allow(dead_code)]

use json::Json;
use promql::parser::*;

/// Reads a fixture from `tests-data/promql/parser/`.
pub fn fixture(name: &str) -> Json {
    let path = format!("{}/../../tests-data/promql/parser/{}", env!("CARGO_MANIFEST_DIR"), name);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {}", path, e));
    Json::parse(&text).unwrap_or_else(|e| panic!("parse {}: {}", path, e))
}

/// A string of a fixture. Go strings that are not valid UTF-8 are dumped as `{"$bytes": hex}`; they come back
/// as `Err(bytes)`.
pub fn fixture_str(j: &Json) -> Result<String, Vec<u8>> {
    match j {
        Json::Str(s) => Ok(s.clone()),
        Json::Obj(_) => {
            let hex = j.get("$bytes").and_then(|h| h.as_str()).expect("$bytes");
            let bytes = (0..hex.len() / 2).map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap()).collect();
            Err(bytes)
        }
        other => panic!("not a string: {:?}", other),
    }
}

/// Replaces every `{"$bytes": hex}` in an expected value by the string Rust can hold: a Rust `String` is UTF-8,
/// so the bytes that are not valid UTF-8 become U+FFFD (as `String::from_utf8_lossy` does).
pub fn lossy(j: &Json) -> Json {
    match j {
        Json::Obj(o) if o.len() == 1 && o[0].0 == "$bytes" => match fixture_str(j) {
            Err(b) => Json::Str(String::from_utf8_lossy(&b).into_owned()),
            Ok(s) => Json::Str(s),
        },
        Json::Obj(o) => Json::Obj(o.iter().map(|(k, v)| (k.clone(), lossy(v))).collect()),
        Json::Arr(a) => Json::Arr(a.iter().map(lossy).collect()),
        other => other.clone(),
    }
}

/// Go `strconv.FormatFloat(f, 'g', -1, 64)`, with -0 as "0" (Go's deep equality treats them as equal).
pub fn float_str(f: f64) -> Json {
    if f == 0.0 {
        return Json::Str("0".into());
    }
    Json::Str(model::histogram::gofmt::g(f))
}

fn pos_range(p: PositionRange) -> Json {
    Json::obj().with("$t", "PositionRange").with("Start", p.start as i64).with("End", p.end as i64)
}

fn opt_expr(e: &Option<Expr>) -> Json {
    e.as_ref().map(dump_expr).unwrap_or(Json::Null)
}

fn opt_duration(e: &Option<Box<DurationExpr>>) -> Json {
    e.as_ref().map(|d| dump_duration(d)).unwrap_or(Json::Null)
}

fn strings(v: &[String]) -> Json {
    Json::Arr(v.iter().map(|s| Json::Str(s.clone())).collect())
}

fn opt_i64(v: Option<i64>) -> Json {
    v.map(Json::Int).unwrap_or(Json::Null)
}

fn opt_f64(v: Option<f64>) -> Json {
    v.map(float_str).unwrap_or(Json::Null)
}

pub fn dump_matcher(m: &model::labels::Matcher) -> Json {
    let t = match m.typ {
        model::labels::MatchType::Equal => 0,
        model::labels::MatchType::NotEqual => 1,
        model::labels::MatchType::Regexp => 2,
        model::labels::MatchType::NotRegexp => 3,
    };
    Json::obj().with("Type", t as i64).with("Name", m.name.as_str()).with("Value", m.value.as_str())
}

fn dump_duration(d: &DurationExpr) -> Json {
    Json::obj()
        .with("$t", "DurationExpr")
        .with("Op", d.op.0 as i64)
        .with("LHS", opt_expr(&d.lhs))
        .with("RHS", opt_expr(&d.rhs))
        .with("Wrapped", d.wrapped)
        .with("StartPos", d.start_pos as i64)
        .with("EndPos", d.end_pos as i64)
}

pub fn dump_vector_selector(v: &VectorSelector) -> Json {
    Json::obj()
        .with("$t", "VectorSelector")
        .with("Name", v.name.as_str())
        .with("OriginalOffset", v.original_offset)
        .with("OriginalOffsetExpr", opt_duration(&v.original_offset_expr))
        .with("Offset", v.offset)
        .with("Timestamp", opt_i64(v.timestamp))
        .with("SkipHistogramBuckets", v.skip_histogram_buckets)
        .with("StartOrEnd", v.start_or_end.0 as i64)
        .with("LabelMatchers", Json::Arr(v.label_matchers.iter().map(dump_matcher).collect()))
        .with("BypassEmptyMatcherCheck", v.bypass_empty_matcher_check)
        .with("Anchored", v.anchored)
        .with("Smoothed", v.smoothed)
        .with("unquotedUTF8Names", v.unquoted_utf8_names)
        .with("PosRange", pos_range(v.pos_range))
}

fn value_type(t: ValueType) -> Json {
    Json::Str(t.as_str().to_string())
}

/// The Rust AST in the JSON form of the Go dumper (field names of the Go structs).
pub fn dump_expr(e: &Expr) -> Json {
    match e {
        Expr::Aggregate(a) => Json::obj()
            .with("$t", "AggregateExpr")
            .with("Op", a.op.0 as i64)
            .with("Expr", opt_expr(&a.expr))
            .with("Param", opt_expr(&a.param))
            .with("Grouping", strings(&a.grouping))
            .with("Without", a.without)
            .with("unquotedUTF8Names", a.unquoted_utf8_names)
            .with("PosRange", pos_range(a.pos_range)),
        Expr::Binary(b) => Json::obj()
            .with("$t", "BinaryExpr")
            .with("Op", b.op.0 as i64)
            .with("LHS", dump_expr(&b.lhs))
            .with("RHS", dump_expr(&b.rhs))
            .with(
                "VectorMatching",
                match &b.vector_matching {
                    None => Json::Null,
                    Some(vm) => Json::obj()
                        .with("$t", "VectorMatching")
                        .with(
                            "Card",
                            match vm.card {
                                VectorMatchCardinality::OneToOne => 0i64,
                                VectorMatchCardinality::ManyToOne => 1,
                                VectorMatchCardinality::OneToMany => 2,
                                VectorMatchCardinality::ManyToMany => 3,
                            },
                        )
                        .with("MatchingLabels", strings(&vm.matching_labels))
                        .with("On", vm.on)
                        .with("Include", strings(&vm.include))
                        .with(
                            "FillValues",
                            Json::obj()
                                .with("$t", "VectorMatchFillValues")
                                .with("RHS", opt_f64(vm.fill_values.rhs))
                                .with("LHS", opt_f64(vm.fill_values.lhs)),
                        ),
                },
            )
            .with("ReturnBool", b.return_bool)
            .with("unquotedUTF8Names", b.unquoted_utf8_names),
        Expr::Call(c) => Json::obj()
            .with("$t", "Call")
            .with(
                "Func",
                Json::obj()
                    .with("$t", "Function")
                    .with("Name", c.func.name.as_ref())
                    .with("ArgTypes", Json::Arr(c.func.arg_types.iter().map(|t| value_type(*t)).collect()))
                    .with("Variadic", c.func.variadic as i64)
                    .with("ReturnType", value_type(c.func.return_type))
                    .with("Experimental", c.func.experimental),
            )
            .with("Args", Json::Arr(c.args.iter().map(dump_expr).collect()))
            .with("PosRange", pos_range(c.pos_range)),
        Expr::MatrixSelector(m) => Json::obj()
            .with("$t", "MatrixSelector")
            .with("VectorSelector", dump_expr(&m.vector_selector))
            .with("Range", m.range)
            .with("RangeExpr", opt_duration(&m.range_expr))
            .with("EndPos", m.end_pos as i64),
        Expr::Subquery(s) => Json::obj()
            .with("$t", "SubqueryExpr")
            .with("Expr", dump_expr(&s.expr))
            .with("Range", s.range)
            .with("RangeExpr", opt_duration(&s.range_expr))
            .with("OriginalOffset", s.original_offset)
            .with("OriginalOffsetExpr", opt_duration(&s.original_offset_expr))
            .with("Offset", s.offset)
            .with("Timestamp", opt_i64(s.timestamp))
            .with("StartOrEnd", s.start_or_end.0 as i64)
            .with("Step", s.step)
            .with("StepExpr", opt_duration(&s.step_expr))
            .with("EndPos", s.end_pos as i64),
        Expr::NumberLiteral(n) => Json::obj()
            .with("$t", "NumberLiteral")
            .with("Val", float_str(n.val))
            .with("Duration", n.duration)
            .with("PosRange", pos_range(n.pos_range)),
        Expr::Paren(p) => Json::obj()
            .with("$t", "ParenExpr")
            .with("Expr", dump_expr(&p.expr))
            .with("PosRange", pos_range(p.pos_range)),
        Expr::StringLiteral(s) => {
            Json::obj().with("$t", "StringLiteral").with("Val", s.val.as_str()).with("PosRange", pos_range(s.pos_range))
        }
        Expr::Unary(u) => Json::obj()
            .with("$t", "UnaryExpr")
            .with("Op", u.op.0 as i64)
            .with("Expr", dump_expr(&u.expr))
            .with("StartPos", u.start_pos as i64),
        Expr::VectorSelector(v) => dump_vector_selector(v),
        Expr::StepInvariant(s) => Json::obj().with("$t", "StepInvariantExpr").with("Expr", dump_expr(&s.expr)),
        Expr::Duration(d) => dump_duration(d),
    }
}

/// Compares two JSON values with object keys in any order. Returns the path of the first difference.
pub fn json_diff(want: &Json, got: &Json, path: &str) -> Option<String> {
    match (want, got) {
        (Json::Obj(a), Json::Obj(b)) => {
            if a.len() != b.len() {
                let ka: Vec<&str> = a.iter().map(|(k, _)| k.as_str()).collect();
                let kb: Vec<&str> = b.iter().map(|(k, _)| k.as_str()).collect();
                return Some(format!("{}: keys {:?} != {:?}", path, ka, kb));
            }
            for (k, va) in a {
                match got.get(k) {
                    None => return Some(format!("{}.{}: missing", path, k)),
                    Some(vb) => {
                        if let Some(d) = json_diff(va, vb, &format!("{}.{}", path, k)) {
                            return Some(d);
                        }
                    }
                }
            }
            None
        }
        (Json::Arr(a), Json::Arr(b)) => {
            if a.len() != b.len() {
                return Some(format!(
                    "{}: len {} != {}: want {} got {}",
                    path,
                    a.len(),
                    b.len(),
                    want.stringify(),
                    got.stringify()
                ));
            }
            for (i, (va, vb)) in a.iter().zip(b).enumerate() {
                if let Some(d) = json_diff(va, vb, &format!("{}[{}]", path, i)) {
                    return Some(d);
                }
            }
            None
        }
        _ if want == got => None,
        _ => Some(format!("{}: want {} got {}", path, want.stringify(), got.stringify())),
    }
}

/// A parse error in the JSON form of the fixtures.
pub fn dump_parse_err(e: &ParseErr, input: &str) -> Json {
    let query = if e.query == input { Json::obj().with("$input", true) } else { Json::Str(e.query.clone()) };
    Json::obj()
        .with("Start", e.position_range.start as i64)
        .with("End", e.position_range.end as i64)
        .with("Err", e.err.as_str())
        .with("Query", query)
        .with("LineOffset", e.line_offset as i64)
}
