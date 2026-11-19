//! Port of `web/api/v1/translate_ast_test.go`.

use json::Json;
use promql::parser::Parser;
use web::api::translate_ast::translate_ast;

fn sorted(mut pairs: Vec<(&str, Json)>) -> Json {
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    Json::Obj(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

fn duration_expr(op: &str, lhs: Json, rhs: Json, wrapped: bool) -> Json {
    sorted(vec![
        ("type", Json::Str("durationExpr".into())),
        ("op", Json::Str(op.into())),
        ("lhs", lhs),
        ("rhs", rhs),
        ("wrapped", Json::Bool(wrapped)),
    ])
}

fn duration_number(val: &str, duration: bool) -> Json {
    sorted(vec![("type", Json::Str("numberLiteral".into())), ("val", Json::Str(val.into())), ("duration", Json::Bool(duration))])
}

fn int(v: i64) -> Json {
    Json::Int(v)
}

#[test]
fn test_translate_ast_duration_expressions() {
    let p = Parser::new(Default::default());
    let n = || Json::Null;
    let cases: Vec<(&str, &str, &str, Vec<(&str, Json)>)> = vec![
        (
            "regular matrix selector",
            "foo[5m]",
            "matrixSelector",
            vec![("range", int(5 * 60 * 1000)), ("rangeExpr", n()), ("offset", int(0)), ("offsetExpr", n())],
        ),
        (
            "regular matrix selector with offset",
            "foo[5m] offset 1m",
            "matrixSelector",
            vec![("range", int(5 * 60 * 1000)), ("rangeExpr", n()), ("offset", int(60 * 1000)), ("offsetExpr", n())],
        ),
        (
            "matrix selector range expression",
            "foo[5m+1m]",
            "matrixSelector",
            vec![("range", int(0)), ("rangeExpr", duration_expr("+", duration_number("300", true), duration_number("60", true), false))],
        ),
        (
            "matrix selector range expression with offset expression",
            "foo[5m+1m] offset (10m/2)",
            "matrixSelector",
            vec![
                ("rangeExpr", duration_expr("+", duration_number("300", true), duration_number("60", true), false)),
                ("offset", int(0)),
                ("offsetExpr", duration_expr("/", duration_number("600", true), duration_number("2", false), true)),
            ],
        ),
        (
            "complex matrix selector range expression",
            "foo[max_of(step(),5m+3m) ]",
            "matrixSelector",
            vec![
                ("range", int(0)),
                (
                    "rangeExpr",
                    duration_expr(
                        "max_of",
                        duration_expr("step", n(), n(), false),
                        duration_expr("+", duration_number("300", true), duration_number("180", true), false),
                        false,
                    ),
                ),
            ],
        ),
        (
            "nested min_of and max_of matrix selector range expression",
            "foo[min_of(max_of(step(),5m+3m),10m-2m)]",
            "matrixSelector",
            vec![
                ("range", int(0)),
                (
                    "rangeExpr",
                    duration_expr(
                        "min_of",
                        duration_expr(
                            "max_of",
                            duration_expr("step", n(), n(), false),
                            duration_expr("+", duration_number("300", true), duration_number("180", true), false),
                            false,
                        ),
                        duration_expr("-", duration_number("600", true), duration_number("120", true), false),
                        false,
                    ),
                ),
            ],
        ),
        ("range preprocessor expression", "foo[range()]", "matrixSelector", vec![("range", int(0)), ("rangeExpr", duration_expr("range", n(), n(), false))]),
        (
            "regular subquery selector",
            "foo[5m:1m]",
            "subquery",
            vec![
                ("range", int(5 * 60 * 1000)),
                ("rangeExpr", n()),
                ("step", int(60 * 1000)),
                ("stepExpr", n()),
                ("offset", int(0)),
                ("offsetExpr", n()),
            ],
        ),
        (
            "subquery selector duration expressions",
            "foo[4s+4s:1s*2] offset (5s-8)",
            "subquery",
            vec![
                ("range", int(0)),
                ("rangeExpr", duration_expr("+", duration_number("4", true), duration_number("4", true), false)),
                ("step", int(0)),
                ("stepExpr", duration_expr("*", duration_number("1", true), duration_number("2", false), false)),
                ("offset", int(0)),
                ("offsetExpr", duration_expr("-", duration_number("5", true), duration_number("8", false), true)),
            ],
        ),
        ("regular vector selector offset", "foo offset 5m", "vectorSelector", vec![("offset", int(5 * 60 * 1000)), ("offsetExpr", n())]),
        (
            "vector selector offset expression",
            "foo offset -min_of(5s,step()+8s)",
            "vectorSelector",
            vec![
                ("offset", int(0)),
                (
                    "offsetExpr",
                    duration_expr(
                        "-",
                        n(),
                        duration_expr(
                            "min_of",
                            duration_number("5", true),
                            duration_expr("+", duration_expr("step", n(), n(), false), duration_number("8", true), false),
                            false,
                        ),
                        false,
                    ),
                ),
            ],
        ),
    ];
    for (name, query, want_type, fields) in cases {
        let expr = p.parse_expr(query).unwrap_or_else(|e| panic!("{}: {}", name, e));
        let got = translate_ast(&expr);
        assert_eq!(Some(&Json::Str(want_type.to_string())), got.get("type"), "{}", name);
        for (field, want) in fields {
            let g = got.get(field).unwrap_or_else(|| panic!("{}: missing field {}", name, field));
            assert_eq!(&want, g, "{}: {}", name, field);
        }
    }
}
