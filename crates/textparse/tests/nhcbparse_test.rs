//! Port of `model/textparse/nhcbparse_test.go`. Protobuf inputs are the binaries generated from the Go test's
//! text-format messages by `scripts/gen/textparse` (under `tests-data/textparse/`).

use model::exemplar::Exemplar;
use model::histogram::{custom_values, Histogram, Span, CUSTOM_BUCKETS_SCHEMA};
use model::labels::{from_strings, Labels};
use model::metadata::MetricType;
use textparse::testing::{require_entries, test_parse, ParsedEntry};
use textparse::{new, Entry, Parser, ParserOptions};

fn ls(s: &[&str]) -> Labels {
    from_strings(s)
}

fn pe() -> ParsedEntry {
    ParsedEntry::default()
}

fn help(m: &str, h: &str) -> ParsedEntry {
    ParsedEntry { m: m.into(), help: h.into(), ..pe() }
}

fn typ(m: &str, t: MetricType) -> ParsedEntry {
    ParsedEntry { m: m.into(), typ: t, ..pe() }
}

fn unit(m: &str, u: &str) -> ParsedEntry {
    ParsedEntry { m: m.into(), unit: u.into(), ..pe() }
}

fn series(m: &str, v: f64, lset: Labels) -> ParsedEntry {
    ParsedEntry { m: m.into(), v, lset, ..pe() }
}

fn ex(l: &[&str], value: f64) -> Exemplar {
    Exemplar { labels: ls(l), value, ..Default::default() }
}

fn ex_ts(l: &[&str], value: f64, ts: i64) -> Exemplar {
    Exemplar { labels: ls(l), value, ts, has_ts: true }
}

fn nhcb(count: u64, sum: f64, spans: &[(i32, u32)], buckets: &[i64], cv: Option<&[f64]>) -> Histogram {
    Histogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count,
        sum,
        positive_spans: spans.iter().map(|&(o, l)| Span::new(o, l)).collect(),
        positive_buckets: buckets.to_vec(),
        custom_values: cv.and_then(custom_values),
        ..Default::default()
    }
}

fn hist_entry(m: &str, h: Histogram, lset: Labels) -> ParsedEntry {
    ParsedEntry { m: m.into(), shs: Some(h), lset, ..pe() }
}

fn tdata(name: &str) -> Vec<u8> {
    let p = format!("{}/../../tests-data/textparse/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {p}: {e}"))
}

fn parse_all(b: &[u8], content_type: &str, opts: ParserOptions) -> Vec<ParsedEntry> {
    let (p, err) = new(b, content_type, &opts);
    assert!(err.is_none(), "unexpected error: {err:?}");
    let mut p = p.expect("parser");
    test_parse(p.as_mut())
}

#[test]
fn test_nhcb_parser_on_om_parser() {
    // The input is taken originally from TestOpenMetricsParse, with additional tests for the NHCBParser.
    let mut input = String::from(
        r#"# HELP go_gc_duration_seconds A summary of the GC invocation durations.
# TYPE go_gc_duration_seconds summary
# UNIT go_gc_duration_seconds seconds
go_gc_duration_seconds{quantile="0"} 4.9351e-05
go_gc_duration_seconds{quantile="0.25"} 7.424100000000001e-05
go_gc_duration_seconds{quantile="0.5",a="b"} 8.3835e-05
# HELP nohelp1 
# HELP help2 escape \ \n \\ \" \x chars
# UNIT nounit 
go_gc_duration_seconds{quantile="1.0",a="b"} 8.3835e-05
go_gc_duration_seconds_count 99
some:aggregate:rate5m{a_b="c"} 1
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 33 123.123
# TYPE hh histogram
hh_bucket{le="+Inf"} 1
# TYPE gh gaugehistogram
gh_bucket{le="+Inf"} 1
# TYPE hhh histogram
hhh_bucket{le="+Inf"} 1 # {id="histogram-bucket-test"} 4
hhh_count 1 # {id="histogram-count-test"} 4
# TYPE ggh gaugehistogram
ggh_bucket{le="+Inf"} 1 # {id="gaugehistogram-bucket-test",xx="yy"} 4 123.123
ggh_count 1 # {id="gaugehistogram-count-test",xx="yy"} 4 123.123
# TYPE smr_seconds summary
smr_seconds_count 2.0 # {id="summary-count-test"} 1 123.321
smr_seconds_sum 42.0 # {id="summary-sum-test"} 1 123.321
# TYPE ii info
ii{foo="bar"} 1
# TYPE ss stateset
ss{ss="foo"} 1
ss{ss="bar"} 0
ss{A="a"} 0
# TYPE un unknown
_metric_starting_with_underscore 1
testmetric{_label_starting_with_underscore="foo"} 1
testmetric{label="\"bar\""} 1
# HELP foo Counter with and without labels to certify ST is parsed for both cases
# TYPE foo counter
foo_total 17.0 1520879607.789 # {id="counter-test"} 5
foo_created 1520872607.123
foo_total{a="b"} 17.0 1520879607.789 # {id="counter-test"} 5
foo_created{a="b"} 1520872607.123
# HELP bar Summary with ST at the end, making sure we find ST even if it's multiple lines a far
# TYPE bar summary
bar_count 17.0
bar_sum 324789.3
bar{quantile="0.95"} 123.7
bar{quantile="0.99"} 150.0
bar_created 1520872608.124
# HELP baz Histogram with the same objective as above's summary
# TYPE baz histogram
baz_bucket{le="0.0"} 0
baz_bucket{le="+Inf"} 17
baz_count 17
baz_sum 324789.3
baz_created 1520872609.125
# HELP fizz_created Gauge which shouldn't be parsed as ST
# TYPE fizz_created gauge
fizz_created 17.0
# HELP something Histogram with _created between buckets and summary
# TYPE something histogram
something_count 18
something_sum 324789.4
something_created 1520430001
something_bucket{le="0.0"} 1
something_bucket{le="+Inf"} 18
something_count{a="b"} 9
something_sum{a="b"} 42123.0
something_bucket{a="b",le="0.0"} 8
something_bucket{a="b",le="+Inf"} 9
something_created{a="b"} 1520430002
# HELP yum Summary with _created between sum and quantiles
# TYPE yum summary
yum_count 20
yum_sum 324789.5
yum_created 1520430003
yum{quantile="0.95"} 123.7
yum{quantile="0.99"} 150.0
# HELP foobar Summary with _created as the first line
# TYPE foobar summary
foobar_count 21
foobar_created 1520430004
foobar_sum 324789.6
foobar{quantile="0.95"} 123.8
foobar{quantile="0.99"} 150.1"#,
    );
    input += "\n# HELP metric foo\x00bar";
    input += "\nnull_byte_metric{a=\"abc\x00\"} 1";
    input += "\n# EOF\n";

    let exp = vec![
        help("go_gc_duration_seconds", "A summary of the GC invocation durations."),
        typ("go_gc_duration_seconds", MetricType::Summary),
        unit("go_gc_duration_seconds", "seconds"),
        series(
            r#"go_gc_duration_seconds{quantile="0"}"#,
            4.9351e-05,
            ls(&["__name__", "go_gc_duration_seconds", "quantile", "0.0"]),
        ),
        series(
            r#"go_gc_duration_seconds{quantile="0.25"}"#,
            7.424100000000001e-05,
            ls(&["__name__", "go_gc_duration_seconds", "quantile", "0.25"]),
        ),
        series(
            r#"go_gc_duration_seconds{quantile="0.5",a="b"}"#,
            8.3835e-05,
            ls(&["__name__", "go_gc_duration_seconds", "quantile", "0.5", "a", "b"]),
        ),
        help("nohelp1", ""),
        help("help2", "escape \\ \n \\ \" \\x chars"),
        unit("nounit", ""),
        series(
            r#"go_gc_duration_seconds{quantile="1.0",a="b"}"#,
            8.3835e-05,
            ls(&["__name__", "go_gc_duration_seconds", "quantile", "1.0", "a", "b"]),
        ),
        series("go_gc_duration_seconds_count", 99.0, ls(&["__name__", "go_gc_duration_seconds_count"])),
        series(r#"some:aggregate:rate5m{a_b="c"}"#, 1.0, ls(&["__name__", "some:aggregate:rate5m", "a_b", "c"])),
        help("go_goroutines", "Number of goroutines that currently exist."),
        typ("go_goroutines", MetricType::Gauge),
        ParsedEntry { t: Some(123123), ..series("go_goroutines", 33.0, ls(&["__name__", "go_goroutines"])) },
        typ("hh", MetricType::Histogram),
        // Custom values are empty as we do not store the +Inf boundary.
        hist_entry("hh", nhcb(1, 0.0, &[(0, 1)], &[1], None), ls(&["__name__", "hh"])),
        typ("gh", MetricType::GaugeHistogram),
        series(r#"gh_bucket{le="+Inf"}"#, 1.0, ls(&["__name__", "gh_bucket", "le", "+Inf"])),
        typ("hhh", MetricType::Histogram),
        ParsedEntry {
            es: vec![ex(&["id", "histogram-bucket-test"], 4.0), ex(&["id", "histogram-count-test"], 4.0)],
            ..hist_entry("hhh", nhcb(1, 0.0, &[(0, 1)], &[1], None), ls(&["__name__", "hhh"]))
        },
        typ("ggh", MetricType::GaugeHistogram),
        ParsedEntry {
            es: vec![ex_ts(&["id", "gaugehistogram-bucket-test", "xx", "yy"], 4.0, 123123)],
            ..series(r#"ggh_bucket{le="+Inf"}"#, 1.0, ls(&["__name__", "ggh_bucket", "le", "+Inf"]))
        },
        ParsedEntry {
            es: vec![ex_ts(&["id", "gaugehistogram-count-test", "xx", "yy"], 4.0, 123123)],
            ..series("ggh_count", 1.0, ls(&["__name__", "ggh_count"]))
        },
        typ("smr_seconds", MetricType::Summary),
        ParsedEntry {
            es: vec![ex_ts(&["id", "summary-count-test"], 1.0, 123321)],
            ..series("smr_seconds_count", 2.0, ls(&["__name__", "smr_seconds_count"]))
        },
        ParsedEntry {
            es: vec![ex_ts(&["id", "summary-sum-test"], 1.0, 123321)],
            ..series("smr_seconds_sum", 42.0, ls(&["__name__", "smr_seconds_sum"]))
        },
        typ("ii", MetricType::Info),
        series(r#"ii{foo="bar"}"#, 1.0, ls(&["__name__", "ii", "foo", "bar"])),
        typ("ss", MetricType::StateSet),
        series(r#"ss{ss="foo"}"#, 1.0, ls(&["__name__", "ss", "ss", "foo"])),
        series(r#"ss{ss="bar"}"#, 0.0, ls(&["__name__", "ss", "ss", "bar"])),
        series(r#"ss{A="a"}"#, 0.0, ls(&["A", "a", "__name__", "ss"])),
        typ("un", MetricType::Unknown),
        series("_metric_starting_with_underscore", 1.0, ls(&["__name__", "_metric_starting_with_underscore"])),
        series(
            "testmetric{_label_starting_with_underscore=\"foo\"}",
            1.0,
            ls(&["__name__", "testmetric", "_label_starting_with_underscore", "foo"]),
        ),
        series("testmetric{label=\"\\\"bar\\\"\"}", 1.0, ls(&["__name__", "testmetric", "label", r#""bar""#])),
        help("foo", "Counter with and without labels to certify ST is parsed for both cases"),
        typ("foo", MetricType::Counter),
        ParsedEntry {
            t: Some(1520879607789),
            es: vec![ex(&["id", "counter-test"], 5.0)],
            st: 1520872607123,
            ..series("foo_total", 17.0, ls(&["__name__", "foo_total"]))
        },
        ParsedEntry {
            t: Some(1520879607789),
            es: vec![ex(&["id", "counter-test"], 5.0)],
            st: 1520872607123,
            ..series(r#"foo_total{a="b"}"#, 17.0, ls(&["__name__", "foo_total", "a", "b"]))
        },
        help("bar", "Summary with ST at the end, making sure we find ST even if it's multiple lines a far"),
        typ("bar", MetricType::Summary),
        ParsedEntry { st: 1520872608124, ..series("bar_count", 17.0, ls(&["__name__", "bar_count"])) },
        ParsedEntry { st: 1520872608124, ..series("bar_sum", 324789.3, ls(&["__name__", "bar_sum"])) },
        ParsedEntry {
            st: 1520872608124,
            ..series(r#"bar{quantile="0.95"}"#, 123.7, ls(&["__name__", "bar", "quantile", "0.95"]))
        },
        ParsedEntry {
            st: 1520872608124,
            ..series(r#"bar{quantile="0.99"}"#, 150.0, ls(&["__name__", "bar", "quantile", "0.99"]))
        },
        help("baz", "Histogram with the same objective as above's summary"),
        typ("baz", MetricType::Histogram),
        ParsedEntry {
            st: 1520872609125,
            // The first bucket has 0 count so we don't store it and Offset is 1.
            ..hist_entry("baz", nhcb(17, 324789.3, &[(1, 1)], &[17], Some(&[0.0])), ls(&["__name__", "baz"]))
        },
        help("fizz_created", "Gauge which shouldn't be parsed as ST"),
        typ("fizz_created", MetricType::Gauge),
        series("fizz_created", 17.0, ls(&["__name__", "fizz_created"])),
        help("something", "Histogram with _created between buckets and summary"),
        typ("something", MetricType::Histogram),
        ParsedEntry {
            st: 1520430001000,
            ..hist_entry(
                "something",
                nhcb(18, 324789.4, &[(0, 2)], &[1, 16], Some(&[0.0])),
                ls(&["__name__", "something"]),
            )
        },
        ParsedEntry {
            st: 1520430002000,
            ..hist_entry(
                r#"something{a="b"}"#,
                nhcb(9, 42123.0, &[(0, 2)], &[8, -7], Some(&[0.0])),
                ls(&["__name__", "something", "a", "b"]),
            )
        },
        help("yum", "Summary with _created between sum and quantiles"),
        typ("yum", MetricType::Summary),
        ParsedEntry { st: 1520430003000, ..series("yum_count", 20.0, ls(&["__name__", "yum_count"])) },
        ParsedEntry { st: 1520430003000, ..series("yum_sum", 324789.5, ls(&["__name__", "yum_sum"])) },
        ParsedEntry {
            st: 1520430003000,
            ..series(r#"yum{quantile="0.95"}"#, 123.7, ls(&["__name__", "yum", "quantile", "0.95"]))
        },
        ParsedEntry {
            st: 1520430003000,
            ..series(r#"yum{quantile="0.99"}"#, 150.0, ls(&["__name__", "yum", "quantile", "0.99"]))
        },
        help("foobar", "Summary with _created as the first line"),
        typ("foobar", MetricType::Summary),
        ParsedEntry { st: 1520430004000, ..series("foobar_count", 21.0, ls(&["__name__", "foobar_count"])) },
        ParsedEntry { st: 1520430004000, ..series("foobar_sum", 324789.6, ls(&["__name__", "foobar_sum"])) },
        ParsedEntry {
            st: 1520430004000,
            ..series(r#"foobar{quantile="0.95"}"#, 123.8, ls(&["__name__", "foobar", "quantile", "0.95"]))
        },
        ParsedEntry {
            st: 1520430004000,
            ..series(r#"foobar{quantile="0.99"}"#, 150.1, ls(&["__name__", "foobar", "quantile", "0.99"]))
        },
        help("metric", "foo\x00bar"),
        series("null_byte_metric{a=\"abc\x00\"}", 1.0, ls(&["__name__", "null_byte_metric", "a", "abc\x00"])),
    ];

    let got = parse_all(
        input.as_bytes(),
        "application/openmetrics-text",
        ParserOptions { convert_classic_histograms_to_nhcb: true, open_metrics_skip_st_series: true, ..Default::default() },
    );
    require_entries(&exp, &got);
}

#[test]
fn test_nhcb_parser_om_parser_multiple_histograms() {
    // The input is taken originally from TestOpenMetricsParse, with additional tests for the NHCBParser.
    let input = r#"# HELP something Histogram with _created between buckets and summary
# TYPE something histogram
something_count 18
something_sum 324789.4
something_bucket{le="0.0"} 1 # {id="something-test"} -2.0
something_bucket{le="1.0"} 16 # {id="something-test"} 0.5
something_bucket{le="+Inf"} 18 # {id="something-test"} 8
something_count{a="b"} 9
something_sum{a="b"} 42123.0
something_bucket{a="b",le="0.0"} 8 # {id="something-test"} 0.0 123.321
something_bucket{a="b",le="1.0"} 8
something_bucket{a="b",le="+Inf"} 9 # {id="something-test"} 2e100 123.000
# EOF
"#;
    let exp = vec![
        help("something", "Histogram with _created between buckets and summary"),
        typ("something", MetricType::Histogram),
        ParsedEntry {
            es: vec![
                ex(&["id", "something-test"], -2.0),
                ex(&["id", "something-test"], 0.5),
                ex(&["id", "something-test"], 8.0),
            ],
            ..hist_entry(
                "something",
                nhcb(18, 324789.4, &[(0, 3)], &[1, 14, -13], Some(&[0.0, 1.0])),
                ls(&["__name__", "something"]),
            )
        },
        ParsedEntry {
            es: vec![ex_ts(&["id", "something-test"], 0.0, 123321), ex_ts(&["id", "something-test"], 2e100, 123000)],
            ..hist_entry(
                r#"something{a="b"}"#,
                nhcb(9, 42123.0, &[(0, 3)], &[8, -8, 1], Some(&[0.0, 1.0])),
                ls(&["__name__", "something", "a", "b"]),
            )
        },
    ];
    let got = parse_all(
        input.as_bytes(),
        "application/openmetrics-text",
        ParserOptions { convert_classic_histograms_to_nhcb: true, ..Default::default() },
    );
    require_entries(&exp, &got);
}

#[test]
fn test_nhcb_parser_does_not_convert_sum_only_histogram() {
    struct Case {
        name: &'static str,
        input: &'static str,
        keep_classic: bool,
        expected: Vec<ParsedEntry>,
    }
    let tests = vec![
        Case {
            name: "drop classic histogram",
            input: "# TYPE sum_only histogram\nsum_only_sum 123.5\n",
            keep_classic: false,
            expected: vec![typ("sum_only", MetricType::Histogram)],
        },
        Case {
            name: "keep classic histogram",
            input: "# TYPE sum_only histogram\nsum_only_sum 123.5\n",
            keep_classic: true,
            expected: vec![
                typ("sum_only", MetricType::Histogram),
                series("sum_only_sum", 123.5, ls(&["__name__", "sum_only_sum"])),
            ],
        },
        Case {
            name: "convert histogram with count and sum",
            input: "# TYPE bucketless histogram\nbucketless_sum 123.5\nbucketless_count 42\n",
            keep_classic: false,
            expected: vec![
                typ("bucketless", MetricType::Histogram),
                hist_entry("bucketless", nhcb(42, 123.5, &[(0, 1)], &[42], None), ls(&["__name__", "bucketless"])),
            ],
        },
    ];
    for test in tests {
        eprintln!("case {}", test.name);
        let got = parse_all(
            test.input.as_bytes(),
            "text/plain",
            ParserOptions {
                convert_classic_histograms_to_nhcb: true,
                keep_classic_on_classic_and_native_histograms: test.keep_classic,
                ..Default::default()
            },
        );
        require_entries(&test.expected, &got);
    }
}

fn create_test_open_metrics_histogram() -> &'static str {
    r#"# HELP test_histogram1 Test histogram 1
# TYPE test_histogram1 histogram
test_histogram1_count 175 1234.568
test_histogram1_sum 0.0008280461746287094 1234.568
test_histogram1_bucket{le="-0.0004899999999999998"} 2 1234.568
test_histogram1_bucket{le="-0.0003899999999999998"} 4 1234.568
test_histogram1_bucket{le="-0.0002899999999999998"} 16 1234.568
test_histogram1_bucket{le="+Inf"} 175 1234.568
test_histogram1_created 1
# EOF"#
}

fn create_test_prom_histogram() -> &'static str {
    r#"# HELP test_histogram1 Test histogram 1
# TYPE test_histogram1 histogram
test_histogram1_count 175 1234568
test_histogram1_sum 0.0008280461746287094 1234568
test_histogram1_bucket{le="-0.0004899999999999998"} 2 1234568
test_histogram1_bucket{le="-0.0003899999999999998"} 4 1234568
test_histogram1_bucket{le="-0.0002899999999999998"} 16 1234568
test_histogram1_bucket{le="+Inf"} 175 1234568"#
}

/// Verify the requirement tables from https://github.com/prometheus/prometheus/issues/13532 (see the Go test).
#[test]
fn test_nhcb_parser_no_nhcb_when_exponential() {
    #[derive(Clone, Copy)]
    struct Requirement {
        expect_classic: bool,
        expect_exponential: bool,
        expect_nhcb: bool,
    }
    const fn r(expect_classic: bool, expect_exponential: bool, expect_nhcb: bool) -> Requirement {
        Requirement { expect_classic, expect_exponential, expect_nhcb }
    }
    let cases: [[(&str, Requirement); 4]; 3] = [
        // Case 1.
        [
            ("classic=false, nhcb=false", r(true, false, false)),
            ("classic=true, nhcb=false", r(true, false, false)),
            ("classic=false, nhcb=true", r(false, false, true)),
            ("classic=true, nhcb=true", r(true, false, true)),
        ],
        // Case 2.
        [
            ("classic=false, nhcb=false", r(false, true, false)),
            ("classic=true, nhcb=false", r(true, true, false)),
            ("classic=false, nhcb=true", r(false, true, false)),
            ("classic=true, nhcb=true", r(true, true, false)),
        ],
        // Case 3.
        [
            ("classic=false, nhcb=false", r(false, true, false)),
            ("classic=true, nhcb=false", r(false, true, false)),
            ("classic=false, nhcb=true", r(false, true, false)),
            ("classic=true, nhcb=true", r(false, true, false)),
        ],
    ];

    struct ParserDef {
        name: &'static str,
        input: Vec<u8>,
        content_type: &'static str,
        skip_st: bool,
        supported_cases: &'static [usize],
        use_utf8_sep: bool,
        has_start_timestamp: bool,
    }
    let parsers = [
        ParserDef {
            name: "ProtoBuf",
            input: tdata("nhcbparse_test/createTestProtoBufHistogram.bin"),
            content_type: "application/vnd.google.protobuf",
            skip_st: false,
            supported_cases: &[1, 2, 3],
            use_utf8_sep: true,
            has_start_timestamp: true,
        },
        ParserDef {
            name: "OpenMetrics",
            input: create_test_open_metrics_histogram().as_bytes().to_vec(),
            content_type: "application/openmetrics-text",
            skip_st: true,
            supported_cases: &[1],
            use_utf8_sep: false,
            has_start_timestamp: true,
        },
        ParserDef {
            name: "Prometheus",
            input: create_test_prom_histogram().as_bytes().to_vec(),
            content_type: "text/plain",
            skip_st: false,
            supported_cases: &[1],
            use_utf8_sep: false,
            has_start_timestamp: false,
        },
    ];

    for parser in &parsers {
        for classic in [false, true] {
            for nhcb_on in [false, true] {
                let requirement_name = format!("classic={classic}, nhcb={nhcb_on}");
                let name = format!("parser={}, {requirement_name}", parser.name);
                let mut exp: Vec<ParsedEntry> = Vec::new();
                for &case_number in parser.supported_cases {
                    let case_i = &cases[case_number - 1];
                    let req = case_i
                        .iter()
                        .find(|(n, _)| *n == requirement_name)
                        .unwrap_or_else(|| panic!("Case {case_number} does not have requirement {requirement_name}"))
                        .1;
                    let metric = format!("test_histogram{case_number}");
                    exp.push(help(&metric, &format!("Test histogram {case_number}")));
                    exp.push(typ(&metric, MetricType::Histogram));
                    let st = if parser.has_start_timestamp { 1000 } else { 0 };
                    let bucket_for_metric = |s: &str| -> Vec<u8> {
                        if parser.use_utf8_sep {
                            [b"_bucket\xffle\xff".as_slice(), s.as_bytes()].concat()
                        } else {
                            format!("_bucket{{le=\"{s}\"}}").into_bytes()
                        }
                    };
                    if req.expect_exponential {
                        // Always expect exponential histogram first.
                        exp.push(ParsedEntry {
                            m: metric.clone().into_bytes(),
                            shs: Some(Histogram {
                                schema: 3,
                                count: 175,
                                sum: 0.0008280461746287094,
                                zero_threshold: 2.938735877055719e-39,
                                zero_count: 2,
                                positive_spans: vec![Span::new(-161, 1), Span::new(8, 3)],
                                negative_spans: vec![Span::new(-162, 1), Span::new(23, 4)],
                                positive_buckets: vec![1, 2, -1, -1],
                                negative_buckets: vec![1, 3, -2, -1, 1],
                                ..Default::default()
                            }),
                            lset: ls(&["__name__", &metric]),
                            t: Some(1234568),
                            st,
                            ..pe()
                        });
                    }
                    if req.expect_classic {
                        // Always expect classic histogram series after exponential.
                        let mut classic_series = |m: Vec<u8>, v: f64, lset: Labels| {
                            exp.push(ParsedEntry { m, v, lset, t: Some(1234568), st, ..pe() });
                        };
                        classic_series(format!("{metric}_count").into_bytes(), 175.0, ls(&["__name__", &format!("{metric}_count")]));
                        classic_series(
                            format!("{metric}_sum").into_bytes(),
                            0.0008280461746287094,
                            ls(&["__name__", &format!("{metric}_sum")]),
                        );
                        for (le, v) in [
                            ("-0.0004899999999999998", 2.0),
                            ("-0.0003899999999999998", 4.0),
                            ("-0.0002899999999999998", 16.0),
                            ("+Inf", 175.0),
                        ] {
                            classic_series(
                                [metric.as_bytes(), &bucket_for_metric(le)].concat(),
                                v,
                                ls(&["__name__", &format!("{metric}_bucket"), "le", le]),
                            );
                        }
                    }
                    if req.expect_nhcb {
                        // Always expect NHCB series after classic.
                        exp.push(ParsedEntry {
                            m: metric.clone().into_bytes(),
                            shs: Some(nhcb(
                                175,
                                0.0008280461746287094,
                                &[(0, 4)],
                                &[2, 0, 10, 147],
                                Some(&[-0.0004899999999999998, -0.0003899999999999998, -0.0002899999999999998]),
                            )),
                            lset: ls(&["__name__", &metric]),
                            t: Some(1234568),
                            st,
                            ..pe()
                        });
                    }
                }
                eprintln!("case {name}");
                let got = parse_all(
                    &parser.input,
                    parser.content_type,
                    ParserOptions {
                        keep_classic_on_classic_and_native_histograms: classic,
                        convert_classic_histograms_to_nhcb: nhcb_on,
                        open_metrics_skip_st_series: parser.skip_st,
                        ..Default::default()
                    },
                );
                require_entries(&exp, &got);
            }
        }
    }
}

#[test]
fn test_nhcb_parser_error_handling() {
    let input = r#"# HELP something Histogram with non cumulative buckets
# TYPE something histogram
something_count 18
something_sum 324789.4
something_created 1520430001
something_bucket{le="0.0"} 18
something_bucket{le="+Inf"} 1
something_count{a="b"} 9
something_sum{a="b"} 42123
something_created{a="b"} 1520430002
something_bucket{a="b",le="0.0"} 1
something_bucket{a="b",le="+Inf"} 9
# EOF"#;
    let exp = vec![
        help("something", "Histogram with non cumulative buckets"),
        typ("something", MetricType::Histogram),
        // The parser should skip the series with non-cumulative buckets.
        ParsedEntry {
            st: 1520430002000,
            ..hist_entry(
                r#"something{a="b"}"#,
                nhcb(9, 42123.0, &[(0, 2)], &[1, 7], Some(&[0.0])),
                ls(&["__name__", "something", "a", "b"]),
            )
        },
    ];
    let got = parse_all(
        input.as_bytes(),
        "application/openmetrics-text",
        ParserOptions { convert_classic_histograms_to_nhcb: true, open_metrics_skip_st_series: true, ..Default::default() },
    );
    require_entries(&exp, &got);
}

#[test]
fn test_nhcb_parser_reset_last_exponential() {
    let buf = tdata("nhcbparse_test/TestNHCBParserResetLastExponential.bin");
    let exp = vec![
        help("test_histogram1", "Test histogram 1"),
        typ("test_histogram1", MetricType::Histogram),
        // The parser should skip the series with non-cumulative buckets.
        ParsedEntry {
            m: "test_histogram1".into(),
            shs: Some(Histogram {
                schema: 3,
                count: 175,
                sum: 0.0008280461746287094,
                zero_threshold: 2.938735877055719e-39,
                zero_count: 2,
                positive_spans: vec![Span::new(-161, 1), Span::new(8, 3)],
                negative_spans: vec![Span::new(-162, 1), Span::new(23, 4)],
                positive_buckets: vec![1, 2, -1, -1],
                negative_buckets: vec![1, 3, -2, -1, 1],
                ..Default::default()
            }),
            lset: ls(&["__name__", "test_histogram1"]),
            t: Some(1234568),
            st: 1000,
            ..pe()
        },
        help("test_histogram2", "Test histogram 2"),
        typ("test_histogram2", MetricType::Histogram),
        ParsedEntry {
            t: Some(1234568),
            st: 1000,
            ..hist_entry(
                "test_histogram2",
                nhcb(
                    175,
                    0.0008280461746287094,
                    &[(0, 4)],
                    &[2, 0, 10, 147],
                    Some(&[-0.0004899999999999998, -0.0003899999999999998, -0.0002899999999999998]),
                ),
                ls(&["__name__", "test_histogram2"]),
            )
        },
    ];
    let got = parse_all(
        &buf,
        "application/vnd.google.protobuf",
        ParserOptions { convert_classic_histograms_to_nhcb: true, open_metrics_skip_st_series: true, ..Default::default() },
    );
    require_entries(&exp, &got);
}

/// Regression test for https://github.com/prometheus/prometheus/issues/17075.
#[test]
fn test_nhcb_not_corrupt_metric_name_after_read() {
    let input_om = r#"# HELP test_histogram_seconds Just a test histogram
# TYPE test_histogram_seconds histogram
test_histogram_seconds_count 10
test_histogram_seconds_sum 100
test_histogram_seconds_bucket{le="10"} 10
test_histogram_seconds_bucket{le="+Inf"} 10
# HELP different_metric Just a different metric
# TYPE different_metric histogram
different_metric_count 5
different_metric_sum 50
different_metric_bucket{le="10"} 5
different_metric_bucket{le="+Inf"} 5
# EOF"#;
    let buf = tdata("nhcbparse_test/TestNHCBNotCorruptMetricNameAfterRead.bin");
    let cases: [(&[u8], &str); 3] = [
        (&buf, "application/vnd.google.protobuf"),
        (input_om.as_bytes(), "text/plain"),
        (input_om.as_bytes(), "application/openmetrics-text"),
    ];
    for (input, ct) in cases {
        eprintln!("case {ct}");
        let (p, err) = new(input, ct, &ParserOptions { convert_classic_histograms_to_nhcb: true, ..Default::default() });
        assert!(err.is_none());
        let mut p = p.expect("parser");
        let get_next = |p: &mut Box<dyn Parser + '_>| p.next().expect("no error");

        assert_eq!(Entry::Help, get_next(&mut p));
        let (last_mf_name, last_help) = p.help();
        assert_eq!(b"test_histogram_seconds", last_mf_name);
        assert_eq!(b"Just a test histogram", last_help);

        assert_eq!(Entry::Type, get_next(&mut p));
        let (last_mf_name, last_type) = p.typ();
        let last_mf_name = last_mf_name.to_vec();
        assert_eq!(b"test_histogram_seconds", last_mf_name.as_slice());
        assert_eq!(MetricType::Histogram, last_type);

        assert_eq!(Entry::Histogram, get_next(&mut p));
        let (_, _, h, _) = p.histogram();
        assert!(h.is_some());
        // adapted: Go checks that the bytes it kept from Type() were not overwritten; Rust's borrow rules make the
        // returned slice unusable after Next, so the kept copy and the parser's current type name are checked.
        assert_eq!(b"test_histogram_seconds", last_mf_name.as_slice());
        assert_eq!(b"test_histogram_seconds", p.typ().0);
    }
}

/// Documents a known limitation: NHCBParser is streaming and expects all classic histogram series of one label
/// set to be contiguous, so ungrouped input yields incomplete NHCB histograms.
#[test]
fn test_nhcb_ungrouped_histogram_series() {
    let input = r#"# TYPE test_histogram histogram
test_histogram_bucket{a="1",le="1"} 1
test_histogram_bucket{a="1",le="+Inf"} 1
test_histogram_bucket{a="2",le="1"} 1
test_histogram_bucket{a="2",le="+Inf"} 2
test_histogram_count{a="1"} 1
test_histogram_count{a="2"} 2
test_histogram_sum{a="1"} 1.5
test_histogram_sum{a="2"} 2.5
"#;
    let got = parse_all(
        input.as_bytes(),
        "text/plain",
        ParserOptions { convert_classic_histograms_to_nhcb: true, ..Default::default() },
    );
    let exp = vec![
        typ("test_histogram", MetricType::Histogram),
        // Discrepancy 1: emitted prematurely from _bucket lines only.
        hist_entry(
            r#"test_histogram{a="1"}"#,
            nhcb(1, 0.0, &[(0, 1)], &[1], Some(&[1.0])),
            ls(&["__name__", "test_histogram", "a", "1"]),
        ),
        hist_entry(
            r#"test_histogram{a="2"}"#,
            nhcb(2, 0.0, &[(0, 2)], &[1, 0], Some(&[1.0])),
            ls(&["__name__", "test_histogram", "a", "2"]),
        ),
        // Discrepancy 2: duplicate series emitted from _count lines only.
        hist_entry(
            r#"test_histogram{a="1"}"#,
            nhcb(1, 0.0, &[(0, 1)], &[1], Some(&[])),
            ls(&["__name__", "test_histogram", "a", "1"]),
        ),
        hist_entry(
            r#"test_histogram{a="2"}"#,
            nhcb(2, 0.0, &[(0, 1)], &[2], Some(&[])),
            ls(&["__name__", "test_histogram", "a", "2"]),
        ),
        // Discrepancy 3: the trailing _sum lines are dropped.
    ];
    require_entries(&exp, &got);
}
