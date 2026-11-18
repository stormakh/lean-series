//! Port of `model/textparse/promparse_test.go`.

mod common;

use common::{todo_detect_family_switch, type_and_unit_labels};
use model::metadata::MetricType;
use textparse::testing::{require_entries, test_parse, ParsedEntry};
use textparse::{Entry, Error, Parser, PromParser};

#[test]
fn test_prom_parse() {
    let mut input = String::from(concat!(
        r#"# HELP go_gc_duration_seconds A summary of the GC invocation durations.
#"#,
        " \t",
        r#"TYPE go_gc_duration_seconds summary
go_gc_duration_seconds{quantile="0"} 4.9351e-05
go_gc_duration_seconds{quantile="0.25",} 7.424100000000001e-05
go_gc_duration_seconds{quantile="0.5",a="b"} 8.3835e-05
go_gc_duration_seconds{quantile="0.8", a="b"} 8.3835e-05
go_gc_duration_seconds{ quantile="0.9", a="b"} 8.3835e-05
# HELP prometheus_http_request_duration_seconds Histogram of latencies for HTTP requests.
# TYPE prometheus_http_request_duration_seconds histogram
prometheus_http_request_duration_seconds_bucket{handler="/",le="1"} 423
prometheus_http_request_duration_seconds_bucket{handler="/",le="2"} 1423
prometheus_http_request_duration_seconds_bucket{handler="/",le="+Inf"} 1423
prometheus_http_request_duration_seconds_sum{handler="/"} 2000
prometheus_http_request_duration_seconds_count{handler="/"} 1423
# Hrandom comment starting with prefix of HELP
#
wind_speed{A="2",c="3"} 12345
# comment with escaped \n newline
# comment with escaped \ escape character
# HELP nohelp1
# HELP nohelp2 
go_gc_duration_seconds{ quantile="1.0", a="b" } 8.3835e-05
go_gc_duration_seconds { quantile="1.0", a="b" } 8.3835e-05
go_gc_duration_seconds { quantile= "1.0", a= "b", } 8.3835e-05
go_gc_duration_seconds { quantile = "1.0", a = "b" } 8.3835e-05
go_gc_duration_seconds { quantile = "2.0" a = "b" } 8.3835e-05
go_gc_duration_seconds_count 99
some:aggregate:rate5m{a_b="c"}"#,
        "\t",
        r#"1
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 33  "#,
        "\t",
        r#"123123
# TYPE some_counter_total counter
# HELP some_counter_total Help after type.
some_counter_total 12
# HELP nohelp3
_metric_starting_with_underscore 1
testmetric{_label_starting_with_underscore="foo"} 1
testmetric{label="\"bar\""} 1
testmetric{le="10"} 1
# HELP type_and_unit_test1 Type specified in metadata overrides.
# TYPE type_and_unit_test1 gauge
type_and_unit_test1{__type__="counter"} 123
# HELP type_and_unit_test2 Type specified in label.
type_and_unit_test2{__type__="counter"} 123"#
    ));
    input += "\n# HELP metric foo\x00bar";
    input += "\nnull_byte_metric{a=\"abc\x00\"} 1";

    for type_and_unit_enabled in [false, true] {
        let exp: Vec<ParsedEntry> = vec![
            pe! {
                m: "go_gc_duration_seconds",
                help: "A summary of the GC invocation durations.",
            },
            pe! {
                m: "go_gc_duration_seconds",
                typ: MetricType::Summary,
            },
            pe! {
                m: r#"go_gc_duration_seconds{quantile="0"}"#,
                v: 4.9351e-05,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "go_gc_duration_seconds", "__type__", MetricType::Summary.as_str(), "quantile", "0.0"),
                    ls!("__name__", "go_gc_duration_seconds", "quantile", "0.0"),
                ),
            },
            pe! {
                m: r#"go_gc_duration_seconds{quantile="0.25",}"#,
                v: 7.424100000000001e-05,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "go_gc_duration_seconds", "__type__", MetricType::Summary.as_str(), "quantile", "0.25"),
                    ls!("__name__", "go_gc_duration_seconds", "quantile", "0.25"),
                ),
            },
            pe! {
                m: r#"go_gc_duration_seconds{quantile="0.5",a="b"}"#,
                v: 8.3835e-05,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "go_gc_duration_seconds", "__type__", MetricType::Summary.as_str(), "quantile", "0.5", "a", "b"),
                    ls!("__name__", "go_gc_duration_seconds", "quantile", "0.5", "a", "b"),
                ),
            },
            pe! {
                m: r#"go_gc_duration_seconds{quantile="0.8", a="b"}"#,
                v: 8.3835e-05,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "go_gc_duration_seconds", "__type__", MetricType::Summary.as_str(), "quantile", "0.8", "a", "b"),
                    ls!("__name__", "go_gc_duration_seconds", "quantile", "0.8", "a", "b"),
                ),
            },
            pe! {
                m: r#"go_gc_duration_seconds{ quantile="0.9", a="b"}"#,
                v: 8.3835e-05,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "go_gc_duration_seconds", "__type__", MetricType::Summary.as_str(), "quantile", "0.9", "a", "b"),
                    ls!("__name__", "go_gc_duration_seconds", "quantile", "0.9", "a", "b"),
                ),
            },
            pe! {
                m: "prometheus_http_request_duration_seconds",
                help: "Histogram of latencies for HTTP requests.",
            },
            pe! {
                m: "prometheus_http_request_duration_seconds",
                typ: MetricType::Histogram,
            },
            pe! {
                m: r#"prometheus_http_request_duration_seconds_bucket{handler="/",le="1"}"#,
                v: 423.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "prometheus_http_request_duration_seconds_bucket", "__type__", MetricType::Histogram.as_str(), "handler", "/", "le", "1.0"),
                    ls!("__name__", "prometheus_http_request_duration_seconds_bucket", "handler", "/", "le", "1.0"),
                ),
            },
            pe! {
                m: r#"prometheus_http_request_duration_seconds_bucket{handler="/",le="2"}"#,
                v: 1423.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "prometheus_http_request_duration_seconds_bucket", "__type__", MetricType::Histogram.as_str(), "handler", "/", "le", "2.0"),
                    ls!("__name__", "prometheus_http_request_duration_seconds_bucket", "handler", "/", "le", "2.0"),
                ),
            },
            pe! {
                m: r#"prometheus_http_request_duration_seconds_bucket{handler="/",le="+Inf"}"#,
                v: 1423.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "prometheus_http_request_duration_seconds_bucket", "__type__", MetricType::Histogram.as_str(), "handler", "/", "le", "+Inf"),
                    ls!("__name__", "prometheus_http_request_duration_seconds_bucket", "handler", "/", "le", "+Inf"),
                ),
            },
            pe! {
                m: r#"prometheus_http_request_duration_seconds_sum{handler="/"}"#,
                v: 2000.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "prometheus_http_request_duration_seconds_sum", "__type__", MetricType::Histogram.as_str(), "handler", "/"),
                    ls!("__name__", "prometheus_http_request_duration_seconds_sum", "handler", "/"),
                ),
            },
            pe! {
                m: r#"prometheus_http_request_duration_seconds_count{handler="/"}"#,
                v: 1423.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "prometheus_http_request_duration_seconds_count", "__type__", MetricType::Histogram.as_str(), "handler", "/"),
                    ls!("__name__", "prometheus_http_request_duration_seconds_count", "handler", "/"),
                ),
            },
            pe! {
                comment: "# Hrandom comment starting with prefix of HELP",
            },
            pe! {
                comment: "#",
            },
            pe! {
                m: r#"wind_speed{A="2",c="3"}"#,
                v: 12345.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    // NOTE(bwplotka): This is knowingly broken, inheriting old type when TYPE was not specified on a new metric.
                    // This was broken forever on a case for a broken exposition. Don't fix for now (expensive).
                    ls!("A", "2", "__name__", "wind_speed", "__type__", MetricType::Histogram.as_str(), "c", "3"),
                    ls!("A", "2", "__name__", "wind_speed", "c", "3"),
                ),
            },
            pe! {
                comment: "# comment with escaped \\n newline",
            },
            pe! {
                comment: "# comment with escaped \\ escape character",
            },
            pe! {
                m: "nohelp1",
                help: "",
            },
            pe! {
                m: "nohelp2",
                help: "",
            },
            pe! {
                m: r#"go_gc_duration_seconds{ quantile="1.0", a="b" }"#,
                v: 8.3835e-05,
                lset: todo_detect_family_switch(type_and_unit_enabled, ls!("__name__", "go_gc_duration_seconds", "quantile", "1.0", "a", "b"), MetricType::Histogram),
            },
            pe! {
                m: r#"go_gc_duration_seconds { quantile="1.0", a="b" }"#,
                v: 8.3835e-05,
                lset: todo_detect_family_switch(type_and_unit_enabled, ls!("__name__", "go_gc_duration_seconds", "quantile", "1.0", "a", "b"), MetricType::Histogram),
            },
            pe! {
                m: r#"go_gc_duration_seconds { quantile= "1.0", a= "b", }"#,
                v: 8.3835e-05,
                lset: todo_detect_family_switch(type_and_unit_enabled, ls!("__name__", "go_gc_duration_seconds", "quantile", "1.0", "a", "b"), MetricType::Histogram),
            },
            pe! {
                m: r#"go_gc_duration_seconds { quantile = "1.0", a = "b" }"#,
                v: 8.3835e-05,
                lset: todo_detect_family_switch(type_and_unit_enabled, ls!("__name__", "go_gc_duration_seconds", "quantile", "1.0", "a", "b"), MetricType::Histogram),
            },
            // NOTE: Unlike OpenMetrics, PromParser allows spaces between label terms. This appears to be unintended and should probably be fixed.
            pe! {
                m: r#"go_gc_duration_seconds { quantile = "2.0" a = "b" }"#,
                v: 8.3835e-05,
                lset: todo_detect_family_switch(type_and_unit_enabled, ls!("__name__", "go_gc_duration_seconds", "quantile", "2.0", "a", "b"), MetricType::Histogram),
            },
            pe! {
                m: "go_gc_duration_seconds_count",
                v: 99.0,
                lset: todo_detect_family_switch(type_and_unit_enabled, ls!("__name__", "go_gc_duration_seconds_count"), MetricType::Histogram),
            },
            pe! {
                m: r#"some:aggregate:rate5m{a_b="c"}"#,
                v: 1.0,
                lset: todo_detect_family_switch(type_and_unit_enabled, ls!("__name__", "some:aggregate:rate5m", "a_b", "c"), MetricType::Histogram),
            },
            pe! {
                m: "go_goroutines",
                help: "Number of goroutines that currently exist.",
            },
            pe! {
                m: "go_goroutines",
                typ: MetricType::Gauge,
            },
            pe! {
                m: "go_goroutines",
                v: 33.0,
                t: Some(123123),
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "go_goroutines", "__type__", MetricType::Gauge.as_str()),
                    ls!("__name__", "go_goroutines"),
                ),
            },
            pe! {
                m: "some_counter_total",
                typ: MetricType::Counter,
            },
            pe! {
                m: "some_counter_total",
                help: "Help after type.",
            },
            pe! {
                m: "some_counter_total",
                v: 12.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "some_counter_total", "__type__", MetricType::Counter.as_str()),
                    ls!("__name__", "some_counter_total"),
                ),
            },
            pe! {
                m: "nohelp3",
                help: "",
            },
            pe! {
                m: "_metric_starting_with_underscore",
                v: 1.0,
                lset: todo_detect_family_switch(type_and_unit_enabled, ls!("__name__", "_metric_starting_with_underscore"), MetricType::Counter),
            },
            pe! {
                m: "testmetric{_label_starting_with_underscore=\"foo\"}",
                v: 1.0,
                lset: todo_detect_family_switch(type_and_unit_enabled, ls!("__name__", "testmetric", "_label_starting_with_underscore", "foo"), MetricType::Counter),
            },
            pe! {
                m: "testmetric{label=\"\\\"bar\\\"\"}",
                v: 1.0,
                lset: todo_detect_family_switch(type_and_unit_enabled, ls!("__name__", "testmetric", "label", r#""bar""#), MetricType::Counter),
            },
            pe! {
                m: r#"testmetric{le="10"}"#,
                v: 1.0,
                lset: todo_detect_family_switch(type_and_unit_enabled, ls!("__name__", "testmetric", "le", "10"), MetricType::Counter),
            },
            pe! {
                m: "type_and_unit_test1",
                help: "Type specified in metadata overrides.",
            },
            pe! {
                m: "type_and_unit_test1",
                typ: MetricType::Gauge,
            },
            pe! {
                m: "type_and_unit_test1{__type__=\"counter\"}",
                v: 123.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "type_and_unit_test1", "__type__", MetricType::Gauge.as_str()),
                    ls!("__name__", "type_and_unit_test1", "__type__", MetricType::Counter.as_str()),
                ),
            },
            pe! {
                m: "type_and_unit_test2",
                help: "Type specified in label.",
            },
            pe! {
                m: "type_and_unit_test2{__type__=\"counter\"}",
                v: 123.0,
                lset: todo_detect_family_switch(type_and_unit_enabled, ls!("__name__", "type_and_unit_test2", "__type__", MetricType::Counter.as_str()), MetricType::Gauge),
            },
            pe! {
                m: "metric",
                help: "foo\x00bar",
            },
            pe! {
                m: "null_byte_metric{a=\"abc\x00\"}",
                v: 1.0,
                lset: todo_detect_family_switch(type_and_unit_enabled, ls!("__name__", "null_byte_metric", "a", "abc\x00"), MetricType::Gauge),
            },
        ];

        for suffix in ["", "\n"] {
            let data = format!("{input}{suffix}");
            let mut p = PromParser::new(data.as_bytes(), type_and_unit_enabled);
            let got = test_parse(&mut p);
            require_entries(&exp, &got);
        }
    }
}

#[test]
fn test_utf8_prom_parse() {
    let input = concat!(
        r#"# HELP "go.gc_duration_seconds" A summary of the GC invocation durations.
#"#,
        " \t",
        r#"TYPE "go.gc_duration_seconds" summary
{"go.gc_duration_seconds",quantile="0"} 4.9351e-05
{"go.gc_duration_seconds",quantile="0.25",} 7.424100000000001e-05
{"go.gc_duration_seconds",quantile="0.5",a="b"} 8.3835e-05
{"go.gc_duration_seconds",quantile="0.8", a="b"} 8.3835e-05
{"go.gc_duration_seconds", quantile="0.9", a="b"} 8.3835e-05
{"go.gc_duration_seconds", quantile="1.0", a="b" } 8.3835e-05
{ "go.gc_duration_seconds", quantile="1.0", a="b" } 8.3835e-05
{ "go.gc_duration_seconds", quantile= "1.0", a= "b", } 8.3835e-05
{ "go.gc_duration_seconds", quantile = "1.0", a = "b" } 8.3835e-05
{"go.gc_duration_seconds_count"} 99
{"Heizölrückstoßabdämpfung 10€ metric with \"interesting\" {character\nchoices}","strange©™\n'quoted' \"name\""="6"} 10.0"#
    );

    let exp: Vec<ParsedEntry> = vec![
        pe! {
            m: "go.gc_duration_seconds",
            help: "A summary of the GC invocation durations.",
        },
        pe! {
            m: "go.gc_duration_seconds",
            typ: MetricType::Summary,
        },
        pe! {
            m: r#"{"go.gc_duration_seconds",quantile="0"}"#,
            v: 4.9351e-05,
            lset: ls!("__name__", "go.gc_duration_seconds", "quantile", "0.0"),
        },
        pe! {
            m: r#"{"go.gc_duration_seconds",quantile="0.25",}"#,
            v: 7.424100000000001e-05,
            lset: ls!("__name__", "go.gc_duration_seconds", "quantile", "0.25"),
        },
        pe! {
            m: r#"{"go.gc_duration_seconds",quantile="0.5",a="b"}"#,
            v: 8.3835e-05,
            lset: ls!("__name__", "go.gc_duration_seconds", "quantile", "0.5", "a", "b"),
        },
        pe! {
            m: r#"{"go.gc_duration_seconds",quantile="0.8", a="b"}"#,
            v: 8.3835e-05,
            lset: ls!("__name__", "go.gc_duration_seconds", "quantile", "0.8", "a", "b"),
        },
        pe! {
            m: r#"{"go.gc_duration_seconds", quantile="0.9", a="b"}"#,
            v: 8.3835e-05,
            lset: ls!("__name__", "go.gc_duration_seconds", "quantile", "0.9", "a", "b"),
        },
        pe! {
            m: r#"{"go.gc_duration_seconds", quantile="1.0", a="b" }"#,
            v: 8.3835e-05,
            lset: ls!("__name__", "go.gc_duration_seconds", "quantile", "1.0", "a", "b"),
        },
        pe! {
            m: r#"{ "go.gc_duration_seconds", quantile="1.0", a="b" }"#,
            v: 8.3835e-05,
            lset: ls!("__name__", "go.gc_duration_seconds", "quantile", "1.0", "a", "b"),
        },
        pe! {
            m: r#"{ "go.gc_duration_seconds", quantile= "1.0", a= "b", }"#,
            v: 8.3835e-05,
            lset: ls!("__name__", "go.gc_duration_seconds", "quantile", "1.0", "a", "b"),
        },
        pe! {
            m: r#"{ "go.gc_duration_seconds", quantile = "1.0", a = "b" }"#,
            v: 8.3835e-05,
            lset: ls!("__name__", "go.gc_duration_seconds", "quantile", "1.0", "a", "b"),
        },
        pe! {
            m: r#"{"go.gc_duration_seconds_count"}"#,
            v: 99.0,
            lset: ls!("__name__", "go.gc_duration_seconds_count"),
        },
        pe! {
            m: r#"{"Heizölrückstoßabdämpfung 10€ metric with \"interesting\" {character\nchoices}","strange©™\n'quoted' \"name\""="6"}"#,
            v: 10.0,
            lset: ls!("__name__", r#"Heizölrückstoßabdämpfung 10€ metric with "interesting" {character
choices}"#, "strange©™\n'quoted' \"name\"", "6"),
        },
    ];

    let mut p = PromParser::new(input.as_bytes(), false);
    let got = test_parse(&mut p);
    require_entries(&exp, &got);
}

/// Reads entries until the first error and returns it.
fn first_error(p: &mut dyn Parser) -> Error {
    loop {
        if let Err(e) = p.next() {
            return e;
        }
    }
}

#[test]
fn test_prom_parse_errors() {
    struct Case {
        input: &'static [u8],
        err: &'static str,
    }
    let cases = [
        Case {
            input: b"a",
            err: "expected value after metric, got \"\\n\" (\"INVALID\") while parsing: \"a\\n\"",
        },
        Case {
            input: b"a{b='c'} 1\n",
            err: "expected label value, got \"'\" (\"INVALID\") while parsing: \"a{b='\"",
        },
        Case {
            input: b"a{b=\n",
            err: "expected label value, got \"\\n\" (\"INVALID\") while parsing: \"a{b=\\n\"",
        },
        Case {
            input: b"a{\xff=\"foo\"} 1\n",
            err: "expected label name, got \"\\xff\" (\"INVALID\") while parsing: \"a{\\xff\"",
        },
        Case {
            input: b"a{b=\"\xff\"} 1\n",
            err: "invalid UTF-8 label value: \"\\\"\\xff\\\"\"",
        },
        Case {
            input: br#"{"a", "b = "c"}"#,
            err: "expected equal, got \"c\\\"\" (\"LNAME\") while parsing: \"{\\\"a\\\", \\\"b = \\\"c\\\"\"",
        },
        Case {
            input: br#"{"a",b\nc="d"} 1"#,
            err: "expected equal, got \"\\\\\" (\"INVALID\") while parsing: \"{\\\"a\\\",b\\\\\"",
        },
        Case {
            input: b"a true\n",
            err: "strconv.ParseFloat: parsing \"true\": invalid syntax while parsing: \"a true\"",
        },
        Case {
            input: b"something_weird{problem=\"",
            err: "expected label value, got \"\\\"\\n\" (\"INVALID\") while parsing: \"something_weird{problem=\\\"\\n\"",
        },
        Case {
            input: b"empty_label_name{=\"\"} 0",
            err: "expected label name, got \"=\\\"\" (\"EQUAL\") while parsing: \"empty_label_name{=\\\"\"",
        },
        Case {
            input: b"{A}0",
            err: "expected equal, got \"}0\" (\"BCLOSE\") while parsing: \"{A}0\"",
        },
        Case {
            input: b"foo 1_2\n",
            err: "unsupported character in float while parsing: \"foo 1_2\"",
        },
        Case {
            input: b"foo 0x1p-3\n",
            err: "unsupported character in float while parsing: \"foo 0x1p-3\"",
        },
        Case {
            input: b"foo 0x1P-3\n",
            err: "unsupported character in float while parsing: \"foo 0x1P-3\"",
        },
        Case {
            input: b"foo 0 1_2\n",
            err: "expected next entry after timestamp, got \"_\" (\"INVALID\") while parsing: \"foo 0 1_\"",
        },
        Case {
            input: br#"{a="ok"} 1"#,
            err: "metric name not set while parsing: \"{a=\\\"ok\\\"} 1\"",
        },
        Case {
            input: b"# TYPE #\n#EOF\n",
            err: "expected metric name after TYPE, got \"#\" (\"INVALID\") while parsing: \"# TYPE #\"",
        },
        Case {
            input: b"# HELP #\n#EOF\n",
            err: "expected metric name after HELP, got \"#\" (\"INVALID\") while parsing: \"# HELP #\"",
        },
    ];

    for (i, c) in cases.iter().enumerate() {
        let mut p = PromParser::new(c.input, false);
        let err = first_error(&mut p);
        assert_eq!(err.to_string(), c.err, "test {i}");
    }
}

#[test]
fn test_prom_parse_bare_identifier_in_braces() {
    // Go: require.NotPanics. A panic fails the Rust test.
    let mut p = PromParser::new(b"{A} 0\n", false);
    while let Ok(et) = p.next() {
        if et == Entry::Series {
            let mut lset = model::labels::Labels::default();
            p.labels(&mut lset);
        }
    }
}

#[test]
fn test_prom_null_byte_handling() {
    struct Case {
        input: &'static [u8],
        err: &'static str,
    }
    let cases = [
        Case {
            input: b"null_byte_metric{a=\"abc\x00\"} 1",
            err: "",
        },
        Case {
            input: b"a{b=\"\x00ss\"} 1\n",
            err: "",
        },
        Case {
            input: b"a{b=\"\x00\"} 1\n",
            err: "",
        },
        Case {
            input: b"a{b=\"\x00\"} 1\n",
            err: "",
        },
        Case {
            input: b"a{b=\x00\"ssss\"} 1\n",
            err: "expected label value, got \"\\x00\" (\"INVALID\") while parsing: \"a{b=\\x00\"",
        },
        Case {
            input: b"a{b=\"\x00",
            err: "expected label value, got \"\\\"\\x00\\n\" (\"INVALID\") while parsing: \"a{b=\\\"\\x00\\n\"",
        },
        Case {
            input: b"a{b\x00=\"hiih\"}\t1",
            err: "expected equal, got \"\\x00\" (\"INVALID\") while parsing: \"a{b\\x00\"",
        },
        Case {
            input: b"a\x00{b=\"ddd\"} 1",
            err: "expected value after metric, got \"\\x00\" (\"INVALID\") while parsing: \"a\\x00\"",
        },
        Case {
            input: b"a 0 1\x00",
            err: "expected next entry after timestamp, got \"\\x00\" (\"INVALID\") while parsing: \"a 0 1\\x00\"",
        },
    ];

    for (i, c) in cases.iter().enumerate() {
        let mut p = PromParser::new(c.input, false);
        let err = first_error(&mut p);

        if c.err.is_empty() {
            assert_eq!(err, Error::Eof, "test {i}");
            continue;
        }

        assert_eq!(err.to_string(), c.err, "test {i}");
    }
}

#[test]
fn test_prom_parse_empty() {
    // Go: nil, []byte{} and {'\n'}; nil and empty are the same slice in Rust.
    for input in [&[][..], &[][..], &b"\n"[..]] {
        let mut p = PromParser::new(input, false);
        let err = p.next().unwrap_err();
        assert!(err.is_eof(), "{err}");
    }
}
