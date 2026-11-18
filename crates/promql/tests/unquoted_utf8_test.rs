// Port of promql/parser/unquoted_utf8_test.go.

use promql::features::{self, Collector, Registry};
use promql::parser::*;

fn go_quote(s: &str) -> String {
    promql::parser::quote::go_quote(s)
}

#[test]
fn test_unquoted_utf8_names() {
    let enabled = Parser::new(Options { enable_unquoted_utf8_names: true, ..Default::default() });
    let disabled = Parser::new(Options::default());
    for name in [
        "Björn",
        "温度",
        "θερμοκρασία",
        "температура",
        "حرارة",
        "𐐀",
        "a𐐀9",
        "http.server.request.duration",
        "foo..bar",
        "foo.",
        "_:温度.total",
        "rate.total",
        "Inf.total",
        "NaN.total",
        "on.total",
        "sum.total",
        "resource.k8s.namespace",
        "target.info",
        "histogram.count",
        "İnf",
        "а",
        "ö",
    ] {
        let expr = enabled.parse_expr(name).unwrap_or_else(|e| panic!("{}: {}", name, e));
        match &expr {
            Expr::VectorSelector(vs) => assert_eq!(name, vs.name),
            other => panic!("{}: not a vector selector: {:?}", name, other),
        }
        assert_eq!(name, expr.to_string());
        let actual = enabled.parse_metric_selector(name).unwrap();
        let expected = disabled.parse_metric_selector(&format!("{{{}}}", go_quote(name))).unwrap();
        assert_eq!(expected, actual, "{}", name);
        assert!(disabled.parse_expr(name).is_err(), "{}", name);
        enabled.parse_expr(&expr.to_string()).unwrap();
    }

    for input in [
        "foo{service.name=\"api\"}",
        "温度{場所=\"東京\"}",
        "sum by (service.name, équipe) (http.server.requests)",
        "sum without (service.name, équipe) (http.server.requests)",
        "requests_total + on (service.name) group_left (équipe) service_info",
        "requests_total + ignoring (service.name) group_right (équipe) service_info",
        "foo{a..=\"b\", __名=~\"東京.*\"}",
        "rate(http.server.requests[5m])",
        "http.server.requests[5m:1m]",
        "http.server.requests offset 5m",
        "http.server.requests @ 0.5",
        "http.server.requests + .5",
        "Inf.total + Inf",
        "NaN.total or foo",
    ] {
        let expr = enabled.parse_expr(input).unwrap_or_else(|e| panic!("{}: {}", input, e));
        let printed = expr.to_string();
        let round_trip = enabled.parse_expr(&printed).unwrap_or_else(|e| panic!("{}: {}", printed, e));
        assert_eq!(printed, round_trip.to_string());
        assert!(disabled.parse_expr(input).is_err(), "{}", input);
    }

    for input in [
        ".foo",
        "5foo",
        "foo{.label=\"x\"}",
        "foo{5label=\"x\"}",
        "foo{a:b=\"x\"}",
        "foo😀",
        "foo١",
        "foo²",
        "o\u{0308}",
        "foo·bar",
        "foo\u{200d}bar",
        "foo\u{fffd}",
        // adapted: Go also tests "foo\xff" (an invalid UTF-8 byte), which a Rust &str cannot hold; Go's lexer
        // sees it as U+FFFD, the case above.
        "sum by (o\u{0308}) (foo)",
        "foo{é😀=\"x\"}",
        "rate.total(foo)",
        "温度(foo)",
        "on",
        "bool",
        "foo[5m.foo]",
        r#"my.metric.name{"my.metric.name"}"#,
        r#"my.metric.name{__name__="my.metric.name"}"#,
    ] {
        assert!(enabled.parse_expr(input).is_err(), "{}", input);
    }

    for input in ["sum by (a:b) (foo)", "foo + on (a:b) bar"] {
        disabled.parse_expr(input).unwrap();
        let err = enabled.parse_expr(input).unwrap_err();
        assert!(err.to_string().contains(r#"invalid label name for grouping: "a:b""#), "{}", err);
    }

    for input in [".5", "5.", "1.5", "1e3", "0x10", "Inf", "inf", "NaN", "nan", "5m"] {
        let expr = enabled.parse_expr(input).unwrap();
        assert!(matches!(expr, Expr::NumberLiteral(_)), "{}", input);
        let expected = disabled.parse_expr(input).unwrap();
        assert_eq!(expected.to_string(), expr.to_string());
    }

    for name in [".5", "5", "5foo", ".foo", "Inf", "inf", "NaN", "nan", "5m"] {
        assert!(enabled.parse_metric_selector(name).is_err(), "{}", name);
        let matchers = enabled.parse_metric_selector(&format!("{{{}}}", go_quote(name))).unwrap();
        assert_eq!(name, matchers[0].value);
    }

    let metric = enabled.parse_metric(r#"温度{場所="東京"}"#).unwrap();
    assert!(model::labels::equal(&model::labels::from_strings(&["__name__", "温度", "場所", "東京"]), &metric));
    let (series, values) = enabled.parse_series_desc(r#"温度{場所="東京"} 1+1x2"#).unwrap();
    assert!(model::labels::equal(&metric, &series));
    let sv = |v: f64| SequenceValue { value: v, ..Default::default() };
    assert_eq!(vec![sv(1.0), sv(2.0), sv(3.0)], values);

    for on in [false, true] {
        let registry = Registry::new();
        Parser::new(Options { enable_unquoted_utf8_names: on, ..Default::default() }).register_features(&registry);
        assert_eq!(Some(&on), registry.get()[features::PROMQL].get("unquoted_utf8_names"));
    }
}

#[test]
fn test_unquoted_utf8_names_printing() {
    let cases = [
        (r#"foo{"service.name"="api"}"#, r#"foo{service.name="api"}"#, r#"foo{"service.name"="api"}"#),
        (
            r#"sum by ("service.name", "équipe") (foo)"#,
            r#"sum by (service.name, équipe) (foo)"#,
            r#"sum by ("service.name", "équipe") (foo)"#,
        ),
        (
            r#"foo + on ("service.name") group_left ("équipe") bar"#,
            r#"foo + on (service.name) group_left (équipe) bar"#,
            r#"foo + on ("service.name") group_left ("équipe") bar"#,
        ),
        // "o\u{0308}" is "ö" decomposed, as in the Go source.
        (
            "foo{\".name\"=\"x\", \"o\u{0308}\"=\"y\", \"😀\"=\"z\"}",
            "foo{\".name\"=\"x\",\"o\u{0308}\"=\"y\",\"😀\"=\"z\"}",
            "foo{\".name\"=\"x\",\"o\u{0308}\"=\"y\",\"😀\"=\"z\"}",
        ),
    ];
    for (input, en, dis) in cases {
        for on in [false, true] {
            let p = Parser::new(Options { enable_unquoted_utf8_names: on, ..Default::default() });
            let expr = p.parse_expr(input).unwrap_or_else(|e| panic!("{}: {}", input, e));
            let expected = if on { en } else { dis };
            assert_eq!(expected, expr.to_string());
            let round_trip = p.parse_expr(&expr.to_string()).unwrap();
            assert_eq!(expected, round_trip.to_string());
        }
    }
}
