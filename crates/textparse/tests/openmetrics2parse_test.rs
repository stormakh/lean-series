//! Port of `model/textparse/openmetrics2parse_test.go`.

mod common;

use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram, Span, CounterResetHint, CUSTOM_BUCKETS_SCHEMA};
use model::labels::Labels;
use model::metadata::MetricType;
use model::value::NORMAL_NAN;
use textparse::testing::{require_entries, test_parse, ParsedEntry};
use textparse::{Error, OpenMetrics2Parser, Parser, ParserOptions};

/// `exemplar.Exemplar{Labels: l, Value: v, HasTs: true, Ts: ts}`.
fn ex(l: Labels, v: f64, ts: i64) -> Exemplar {
    Exemplar { labels: l, value: v, has_ts: true, ts }
}

fn parse(input: &[u8], opts: ParserOptions) -> Vec<ParsedEntry> {
    let mut p = OpenMetrics2Parser::new(input, &opts);
    test_parse(&mut p)
}

/// Calls `Next` until it returns an error and returns that error.
fn first_error(input: &[u8], opts: ParserOptions) -> Error {
    let mut p = OpenMetrics2Parser::new(input, &opts);
    loop {
        if let Err(e) = p.next() {
            return e;
        }
    }
}

#[track_caller]
fn require_error_contains(err: &Error, want: &str) {
    let s = err.to_string();
    assert!(s.contains(want), "error {s:?} does not contain {want:?}");
}

fn spans(s: &[(i32, u32)]) -> Vec<Span> {
    s.iter().map(|&(o, l)| Span::new(o, l)).collect()
}

fn type_and_unit() -> ParserOptions {
    ParserOptions { enable_type_and_unit_labels: true, ..Default::default() }
}

#[test]
fn test_open_metrics2_parse() {
    let input = r##"# HELP go_gc_duration_seconds A summary of GC invocation durations.
# TYPE go_gc_duration_seconds summary
# UNIT go_gc_duration_seconds seconds
go_gc_duration_seconds {count:99,sum:0.004,quantile:[0:4.9351e-05,0.5:8.3835e-05]}
# TYPE go_goroutines gauge
go_goroutines 33 123456.789
# TYPE hh histogram
hh {count:1,sum:1.0,bucket:[+Inf:1]}
# TYPE ii_info info
ii_info{foo="bar"} 1
# TYPE ss stateset
ss{ss="foo"} 1
ss{ss="bar"} 0
# TYPE un unknown
un 1
# TYPE foo_total counter
foo_total 17.0 1520879607.789 # {id="counter-test"} 5 1520879600.789
# EOF
"##;

    let exp = vec![
        pe! { m: "go_gc_duration_seconds", help: "A summary of GC invocation durations." },
        pe! { m: "go_gc_duration_seconds", typ: MetricType::Summary },
        pe! { m: "go_gc_duration_seconds", unit: "seconds" },
        pe! { m: "go_gc_duration_seconds_count", v: 99.0, lset: ls!("__name__", "go_gc_duration_seconds_count") },
        pe! { m: "go_gc_duration_seconds_sum", v: 0.004, lset: ls!("__name__", "go_gc_duration_seconds_sum") },
        pe! {
            m: b"go_gc_duration_seconds\xffquantile\xff0.0",
            v: 4.9351e-05,
            lset: ls!("__name__", "go_gc_duration_seconds", "quantile", "0.0"),
        },
        pe! {
            m: b"go_gc_duration_seconds\xffquantile\xff0.5",
            v: 8.3835e-05,
            lset: ls!("__name__", "go_gc_duration_seconds", "quantile", "0.5"),
        },
        pe! { m: "go_goroutines", typ: MetricType::Gauge },
        pe! { m: "go_goroutines", v: 33.0, t: Some(123456789), lset: ls!("__name__", "go_goroutines") },
        pe! { m: "hh", typ: MetricType::Histogram },
        pe! { m: "hh_count", v: 1.0, lset: ls!("__name__", "hh_count") },
        pe! { m: "hh_sum", v: 1.0, lset: ls!("__name__", "hh_sum") },
        pe! { m: b"hh_bucket\xffle\xff+Inf", v: 1.0, lset: ls!("__name__", "hh_bucket", "le", "+Inf") },
        pe! { m: "ii_info", typ: MetricType::Info },
        pe! { m: r#"ii_info{foo="bar"}"#, v: 1.0, lset: ls!("__name__", "ii_info", "foo", "bar") },
        pe! { m: "ss", typ: MetricType::StateSet },
        pe! { m: r#"ss{ss="foo"}"#, v: 1.0, lset: ls!("__name__", "ss", "ss", "foo") },
        pe! { m: r#"ss{ss="bar"}"#, v: 0.0, lset: ls!("__name__", "ss", "ss", "bar") },
        pe! { m: "un", typ: MetricType::Unknown },
        pe! { m: "un", v: 1.0, lset: ls!("__name__", "un") },
        pe! { m: "foo_total", typ: MetricType::Counter },
        pe! {
            m: "foo_total",
            v: 17.0,
            t: Some(1520879607789),
            lset: ls!("__name__", "foo_total"),
            es: vec![ex(ls!("id", "counter-test"), 5.0, 1520879600789)],
        },
    ];

    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_start_timestamp() {
    let input = "# TYPE requests_total counter\nrequests_total 42.0 1000000.0 st@500000.0\n# EOF\n";
    let exp = vec![
        pe! { m: "requests_total", typ: MetricType::Counter },
        pe! {
            m: "requests_total",
            v: 42.0,
            t: Some(1000000000),
            st: 500000000,
            lset: ls!("__name__", "requests_total"),
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_start_timestamp_no_main_timestamp() {
    let input = "# TYPE requests_total counter\nrequests_total 42.0 st@500000.0\n# EOF\n";
    let exp = vec![
        pe! { m: "requests_total", typ: MetricType::Counter },
        pe! { m: "requests_total", v: 42.0, st: 500000000, lset: ls!("__name__", "requests_total") },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_multiple_exemplars() {
    let input = r##"# TYPE foo_total counter
foo_total 17.0 # {id="a"} 1.0 1234566.0 # {id="b"} 2.0 1234567.0
# EOF
"##;
    let exp = vec![
        pe! { m: "foo_total", typ: MetricType::Counter },
        pe! {
            m: "foo_total",
            v: 17.0,
            lset: ls!("__name__", "foo_total"),
            es: vec![ex(ls!("id", "a"), 1.0, 1234566000), ex(ls!("id", "b"), 2.0, 1234567000)],
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_exemplar_multiple_labels() {
    let input = r##"# TYPE http_requests_total counter
http_requests_total 1027 1710000000.0 # {trace_id="abc123",span_id="def456"} 1.0 1709999999.0
# EOF
"##;
    let exp = vec![
        pe! { m: "http_requests_total", typ: MetricType::Counter },
        pe! {
            m: "http_requests_total",
            v: 1027.0,
            t: Some(1710000000000),
            lset: ls!("__name__", "http_requests_total"),
            es: vec![ex(ls!("span_id", "def456", "trace_id", "abc123"), 1.0, 1709999999000)],
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_exemplar_exceeds_om1_limit() {
    // OM1 capped exemplar LabelSets at 128 UTF-8 code points (sum of all label names and values). OM2 removes
    // that cap; this exemplar is well over the old limit (4 + 200 = 204 code points).
    let long_value = "x".repeat(200);
    let input = format!("# TYPE foo_total counter\nfoo_total 1.0 # {{data=\"{long_value}\"}} 1.0 1234567.0\n# EOF\n");
    let exp = vec![
        pe! { m: "foo_total", typ: MetricType::Counter },
        pe! {
            m: "foo_total",
            v: 1.0,
            lset: ls!("__name__", "foo_total"),
            es: vec![ex(ls!("data", long_value.as_str()), 1.0, 1234567000)],
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_exemplar_quoted_utf8_label_key() {
    // OM2 label-key allows DQUOTE escaped-string-non-empty DQUOTE, permitting label names that contain
    // characters invalid in plain identifiers (e.g. ".").
    let input = r##"# TYPE hits gauge
hits 17.0 # {"my.id"="abc"} 5 1520879600.789
# EOF
"##;
    let exp = vec![
        pe! { m: "hits", typ: MetricType::Gauge },
        pe! {
            m: "hits",
            v: 17.0,
            lset: ls!("__name__", "hits"),
            es: vec![ex(ls!("my.id", "abc"), 5.0, 1520879600789)],
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

/// A classic GaugeHistogram composite value exposes its count/sum as gcount/gsum, not count/sum, per the OM2
/// spec: https://prometheus.io/docs/specs/om/open_metrics_spec_2_0/#gaugehistogram-1
#[test]
fn test_open_metrics2_parse_composite_classic_gauge_histogram() {
    let input = r##"# TYPE foo gaugehistogram
foo {gcount:42,gsum:3289.3,bucket:[0.01:20,0.1:25,1:34,+Inf:42]}
# EOF
"##;
    let exp = vec![
        pe! { m: "foo", typ: MetricType::GaugeHistogram },
        pe! { m: "foo_gcount", v: 42.0, lset: ls!("__name__", "foo_gcount") },
        pe! { m: "foo_gsum", v: 3289.3, lset: ls!("__name__", "foo_gsum") },
        pe! { m: b"foo_bucket\xffle\xff0.01", v: 20.0, lset: ls!("__name__", "foo_bucket", "le", "0.01") },
        pe! { m: b"foo_bucket\xffle\xff0.1", v: 25.0, lset: ls!("__name__", "foo_bucket", "le", "0.1") },
        pe! { m: b"foo_bucket\xffle\xff1.0", v: 34.0, lset: ls!("__name__", "foo_bucket", "le", "1.0") },
        pe! { m: b"foo_bucket\xffle\xff+Inf", v: 42.0, lset: ls!("__name__", "foo_bucket", "le", "+Inf") },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_composite_summary() {
    let input = r##"# HELP rpc_duration_seconds RPC duration.
# TYPE rpc_duration_seconds summary
rpc_duration_seconds {count:100,sum:30000.0,quantile:[0.5:100.0,0.9:200.0,0.99:300.0]}
# EOF
"##;
    let exp = vec![
        pe! { m: "rpc_duration_seconds", help: "RPC duration." },
        pe! { m: "rpc_duration_seconds", typ: MetricType::Summary },
        // Exploded series: _count, _sum, quantile entries.
        pe! { m: "rpc_duration_seconds_count", v: 100.0, lset: ls!("__name__", "rpc_duration_seconds_count") },
        pe! { m: "rpc_duration_seconds_sum", v: 30000.0, lset: ls!("__name__", "rpc_duration_seconds_sum") },
        pe! {
            m: b"rpc_duration_seconds\xffquantile\xff0.5",
            v: 100.0,
            lset: ls!("__name__", "rpc_duration_seconds", "quantile", "0.5"),
        },
        pe! {
            m: b"rpc_duration_seconds\xffquantile\xff0.9",
            v: 200.0,
            lset: ls!("__name__", "rpc_duration_seconds", "quantile", "0.9"),
        },
        pe! {
            m: b"rpc_duration_seconds\xffquantile\xff0.99",
            v: 300.0,
            lset: ls!("__name__", "rpc_duration_seconds", "quantile", "0.99"),
        },
    ];

    let mut p = OpenMetrics2Parser::new(input.as_bytes(), &ParserOptions::default());
    let got = test_parse(&mut p);
    require_entries(&exp, &got);

    // test_parse drained the input to EOF, so the pooled composite buffers must be detached from the parser and
    // a stray next() must not touch them.
    // adapted: Go checks that the scratch, pending and seriesBuf fields are nil; the Rust parser reports the same
    // state (no pooled scratch held, both buffers without capacity).
    assert!(p.composite_scratch_released());
    assert!(p.next().is_err());
}

#[test]
fn test_open_metrics2_parse_composite_classic_histogram() {
    let input = r##"# HELP req_duration Request duration histogram.
# TYPE req_duration histogram
req_duration {count:3,sum:6.0,bucket:[0.1:1,1.0:2,+Inf:3]}
# EOF
"##;
    let exp = vec![
        pe! { m: "req_duration", help: "Request duration histogram." },
        pe! { m: "req_duration", typ: MetricType::Histogram },
        pe! { m: "req_duration_count", v: 3.0, lset: ls!("__name__", "req_duration_count") },
        pe! { m: "req_duration_sum", v: 6.0, lset: ls!("__name__", "req_duration_sum") },
        pe! { m: b"req_duration_bucket\xffle\xff0.1", v: 1.0, lset: ls!("__name__", "req_duration_bucket", "le", "0.1") },
        pe! { m: b"req_duration_bucket\xffle\xff1.0", v: 2.0, lset: ls!("__name__", "req_duration_bucket", "le", "1.0") },
        pe! {
            m: b"req_duration_bucket\xffle\xff+Inf",
            v: 3.0,
            lset: ls!("__name__", "req_duration_bucket", "le", "+Inf"),
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

/// -Inf is accepted as the first classic histogram bucket threshold, as permitted by the OM2 ABNF.
#[test]
fn test_open_metrics2_parse_classic_histogram_neg_inf_bucket() {
    let input = r##"# TYPE req_duration histogram
req_duration {count:3,sum:6.0,bucket:[-Inf:1,1.0:2,+Inf:3]}
# EOF
"##;
    let exp = vec![
        pe! { m: "req_duration", typ: MetricType::Histogram },
        pe! { m: "req_duration_count", v: 3.0, lset: ls!("__name__", "req_duration_count") },
        pe! { m: "req_duration_sum", v: 6.0, lset: ls!("__name__", "req_duration_sum") },
        pe! {
            m: b"req_duration_bucket\xffle\xff-Inf",
            v: 1.0,
            lset: ls!("__name__", "req_duration_bucket", "le", "-Inf"),
        },
        pe! { m: b"req_duration_bucket\xffle\xff1.0", v: 2.0, lset: ls!("__name__", "req_duration_bucket", "le", "1.0") },
        pe! {
            m: b"req_duration_bucket\xffle\xff+Inf",
            v: 3.0,
            lset: ls!("__name__", "req_duration_bucket", "le", "+Inf"),
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_composite_native_and_classic_histogram() {
    // A composite value exposing both representations in one line: native histogram fields AND a classic bucket
    // list. With keep_classic off only the native histogram is emitted; with it on the classic
    // _count/_sum/_bucket series follow, and exemplars come with both the native histogram and the matching
    // classic _bucket series.
    let input = r##"# HELP req_duration Request duration histogram with both representations.
# TYPE req_duration histogram
req_duration {count:3,sum:6.0,schema:0,zero_threshold:0.001,zero_count:0,positive_spans:[0:2],positive_buckets:[1,2],bucket:[0.1:1,1.0:2,+Inf:3]} # {id="req-1"} 0.8 1234566.0 # {id="req-2"} 5.0 1234567.0
# EOF
"##;
    let ex1 = ex(ls!("id", "req-1"), 0.8, 1234566000);
    let ex2 = ex(ls!("id", "req-2"), 5.0, 1234567000);
    let header = vec![
        pe! { m: "req_duration", help: "Request duration histogram with both representations." },
        pe! { m: "req_duration", typ: MetricType::Histogram },
    ];
    let native_entry = pe! {
        m: "req_duration",
        lset: ls!("__name__", "req_duration"),
        es: vec![ex1.clone(), ex2.clone()],
        shs: Some(Histogram {
            schema: 0,
            zero_threshold: 0.001,
            zero_count: 0,
            count: 3,
            sum: 6.0,
            positive_spans: spans(&[(0, 2)]),
            positive_buckets: vec![1, 1],
            ..Default::default()
        }),
    };
    let classic_series = [
        pe! { m: "req_duration_count", v: 3.0, lset: ls!("__name__", "req_duration_count") },
        pe! { m: "req_duration_sum", v: 6.0, lset: ls!("__name__", "req_duration_sum") },
        pe! { m: b"req_duration_bucket\xffle\xff0.1", v: 1.0, lset: ls!("__name__", "req_duration_bucket", "le", "0.1") },
        pe! {
            m: b"req_duration_bucket\xffle\xff1.0",
            v: 2.0,
            lset: ls!("__name__", "req_duration_bucket", "le", "1.0"),
            es: vec![ex1.clone()],
        },
        pe! {
            m: b"req_duration_bucket\xffle\xff+Inf",
            v: 3.0,
            lset: ls!("__name__", "req_duration_bucket", "le", "+Inf"),
            es: vec![ex2.clone()],
        },
    ];
    let with_native = || {
        let mut v = header.clone();
        v.push(native_entry.clone());
        v
    };
    let with_classic = || {
        let mut v = with_native();
        v.extend(classic_series.iter().cloned());
        v
    };

    let cases: Vec<(&str, ParserOptions, Vec<ParsedEntry>)> = vec![
        ("keep_classic_off", ParserOptions::default(), with_native()),
        (
            "keep_classic_on",
            ParserOptions { keep_classic_on_classic_and_native_histograms: true, ..Default::default() },
            with_classic(),
        ),
        (
            "convert_nhcb_keep_classic_off",
            ParserOptions { convert_classic_histograms_to_nhcb: true, ..Default::default() },
            with_native(),
        ),
        (
            "convert_nhcb_keep_classic_on",
            ParserOptions {
                convert_classic_histograms_to_nhcb: true,
                keep_classic_on_classic_and_native_histograms: true,
                ..Default::default()
            },
            with_classic(),
        ),
    ];
    for (name, opts, exp) in cases {
        eprintln!("case {name}");
        let got = parse(input.as_bytes(), opts);
        require_entries(&exp, &got);
    }
}

#[test]
fn test_open_metrics2_parse_convert_classic_histogram_to_nhcb() {
    let ts = 1234567000i64;
    let cases: Vec<(&str, &str, ParserOptions, Vec<ParsedEntry>)> = vec![
        (
            "classic_int_histogram_to_nhcb",
            r##"# HELP req_duration Request duration.
# TYPE req_duration histogram
# UNIT req_duration seconds
req_duration{job="api"} {count:3,sum:6.0,bucket:[0.1:1,1.0:2,+Inf:3]} 1234567.0 st@1000.0 # {id="req-1"} 0.8 1234566.0
# EOF
"##,
            ParserOptions { convert_classic_histograms_to_nhcb: true, enable_type_and_unit_labels: true, ..Default::default() },
            vec![
                pe! { m: "req_duration", help: "Request duration." },
                pe! { m: "req_duration", typ: MetricType::Histogram },
                pe! { m: "req_duration", unit: "seconds" },
                pe! {
                    m: r#"req_duration{job="api"}"#,
                    lset: ls!("__name__", "req_duration", "__type__", "histogram", "__unit__", "seconds", "job", "api"),
                    t: Some(ts),
                    st: 1000000,
                    es: vec![ex(ls!("id", "req-1"), 0.8, 1234566000)],
                    shs: Some(Histogram {
                        schema: CUSTOM_BUCKETS_SCHEMA,
                        count: 3,
                        sum: 6.0,
                        positive_spans: spans(&[(0, 3)]),
                        positive_buckets: vec![1, 0, 0],
                        custom_values: Some(vec![0.1, 1.0].into()),
                        ..Default::default()
                    }),
                },
            ],
        ),
        (
            // Corresponds to convert_classic_histograms_to_nhcb: true and always_scrape_classic_histograms: true
            // in scrape config (see #13532), emitting both the converted NHCB and the classic histogram series.
            "classic_int_histogram_to_nhcb_with_keep_classic",
            r##"# TYPE req_duration histogram
req_duration {count:3,sum:6.0,bucket:[0.1:1,1.0:2,+Inf:3]} 1234567.0 st@1000.0 # {id="req-1"} 0.8 1234566.0
# EOF
"##,
            ParserOptions {
                convert_classic_histograms_to_nhcb: true,
                keep_classic_on_classic_and_native_histograms: true,
                ..Default::default()
            },
            vec![
                pe! { m: "req_duration", typ: MetricType::Histogram },
                pe! {
                    m: "req_duration",
                    lset: ls!("__name__", "req_duration"),
                    t: Some(ts),
                    st: 1000000,
                    es: vec![ex(ls!("id", "req-1"), 0.8, 1234566000)],
                    shs: Some(Histogram {
                        schema: CUSTOM_BUCKETS_SCHEMA,
                        count: 3,
                        sum: 6.0,
                        positive_spans: spans(&[(0, 3)]),
                        positive_buckets: vec![1, 0, 0],
                        custom_values: Some(vec![0.1, 1.0].into()),
                        ..Default::default()
                    }),
                },
                pe! { m: "req_duration_count", v: 3.0, t: Some(ts), st: 1000000, lset: ls!("__name__", "req_duration_count") },
                pe! { m: "req_duration_sum", v: 6.0, t: Some(ts), st: 1000000, lset: ls!("__name__", "req_duration_sum") },
                pe! {
                    m: b"req_duration_bucket\xffle\xff0.1",
                    v: 1.0,
                    t: Some(ts),
                    st: 1000000,
                    lset: ls!("__name__", "req_duration_bucket", "le", "0.1"),
                },
                pe! {
                    m: b"req_duration_bucket\xffle\xff1.0",
                    v: 2.0,
                    t: Some(ts),
                    st: 1000000,
                    lset: ls!("__name__", "req_duration_bucket", "le", "1.0"),
                    es: vec![ex(ls!("id", "req-1"), 0.8, 1234566000)],
                },
                pe! {
                    m: b"req_duration_bucket\xffle\xff+Inf",
                    v: 3.0,
                    t: Some(ts),
                    st: 1000000,
                    lset: ls!("__name__", "req_duration_bucket", "le", "+Inf"),
                },
            ],
        ),
        (
            "classic_float_histogram_to_nhcb",
            r##"# TYPE req_duration histogram
req_duration {count:3.5,sum:6.0,bucket:[0.1:1.5,1.0:2.5,+Inf:3.5]}
# EOF
"##,
            ParserOptions { convert_classic_histograms_to_nhcb: true, ..Default::default() },
            vec![
                pe! { m: "req_duration", typ: MetricType::Histogram },
                pe! {
                    m: "req_duration",
                    lset: ls!("__name__", "req_duration"),
                    fhs: Some(FloatHistogram {
                        schema: CUSTOM_BUCKETS_SCHEMA,
                        count: 3.5,
                        sum: 6.0,
                        positive_spans: spans(&[(0, 3)]),
                        positive_buckets: vec![1.5, 1.0, 1.0],
                        custom_values: Some(vec![0.1, 1.0].into()),
                        ..Default::default()
                    }),
                },
            ],
        ),
        (
            "classic_gaugehistogram_to_nhcb",
            r##"# TYPE queue_size gaugehistogram
queue_size {gcount:4,gsum:100.0,bucket:[10:1,50:3,+Inf:4]}
# EOF
"##,
            ParserOptions { convert_classic_histograms_to_nhcb: true, ..Default::default() },
            vec![
                pe! { m: "queue_size", typ: MetricType::GaugeHistogram },
                pe! {
                    m: "queue_size",
                    lset: ls!("__name__", "queue_size"),
                    shs: Some(Histogram {
                        counter_reset_hint: CounterResetHint::GaugeType,
                        schema: CUSTOM_BUCKETS_SCHEMA,
                        count: 4,
                        sum: 100.0,
                        positive_spans: spans(&[(0, 3)]),
                        positive_buckets: vec![1, 1, -1],
                        custom_values: Some(vec![10.0, 50.0].into()),
                        ..Default::default()
                    }),
                },
            ],
        ),
    ];
    for (name, input, opts, exp) in cases {
        eprintln!("case {name}");
        let got = parse(input.as_bytes(), opts);
        require_entries(&exp, &got);
    }
}

#[test]
fn test_open_metrics2_parse_native_histogram() {
    let input = r##"# HELP test_histogram Native histogram.
# TYPE test_histogram histogram
test_histogram {count:6,sum:12.1,schema:0,zero_threshold:0.001,zero_count:2,negative_spans:[],negative_buckets:[],positive_spans:[0:3],positive_buckets:[1,2,1]}
# EOF
"##;
    let exp = vec![
        pe! { m: "test_histogram", help: "Native histogram." },
        pe! { m: "test_histogram", typ: MetricType::Histogram },
        pe! {
            m: "test_histogram",
            lset: ls!("__name__", "test_histogram"),
            shs: Some(Histogram {
                schema: 0,
                zero_threshold: 0.001,
                zero_count: 2,
                count: 6,
                sum: 12.1,
                positive_spans: spans(&[(0, 3)]),
                positive_buckets: vec![1, 1, -1],
                ..Default::default()
            }),
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_native_histogram_omitted_empty_fields() {
    // The OM2 spec says empty spans/buckets SHOULD be omitted; absent keys are treated like explicit empty lists.
    let input = r##"# TYPE test_histogram histogram
test_histogram {count:6,sum:12.1,schema:0,zero_threshold:0.001,zero_count:2,positive_spans:[0:3],positive_buckets:[1,2,1]}
# EOF
"##;
    let exp = vec![
        pe! { m: "test_histogram", typ: MetricType::Histogram },
        pe! {
            m: "test_histogram",
            lset: ls!("__name__", "test_histogram"),
            shs: Some(Histogram {
                schema: 0,
                zero_threshold: 0.001,
                zero_count: 2,
                count: 6,
                sum: 12.1,
                positive_spans: spans(&[(0, 3)]),
                positive_buckets: vec![1, 1, -1],
                ..Default::default()
            }),
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_native_histogram_omitted_positive_fields() {
    // Histogram with only negative buckets; positive_spans and positive_buckets are omitted.
    let input = r##"# TYPE test_histogram histogram
test_histogram {count:6,sum:12.1,schema:0,zero_threshold:0.001,zero_count:2,negative_spans:[0:3],negative_buckets:[1,2,1]}
# EOF
"##;
    let exp = vec![
        pe! { m: "test_histogram", typ: MetricType::Histogram },
        pe! {
            m: "test_histogram",
            lset: ls!("__name__", "test_histogram"),
            shs: Some(Histogram {
                schema: 0,
                zero_threshold: 0.001,
                zero_count: 2,
                count: 6,
                sum: 12.1,
                negative_spans: spans(&[(0, 3)]),
                negative_buckets: vec![1, 1, -1],
                ..Default::default()
            }),
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_utf8_metric_name() {
    let input = r##"# TYPE "metric.name" gauge
{"metric.name",label="value"} 1.0
# EOF
"##;
    let exp = vec![
        pe! { m: "metric.name", typ: MetricType::Gauge },
        pe! {
            m: r#"{"metric.name",label="value"}"#,
            v: 1.0,
            lset: ls!("__name__", "metric.name", "label", "value"),
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_unknown() {
    let cases: Vec<(&str, &str, Vec<ParsedEntry>)> = vec![
        (
            "explicit help and type",
            "# HELP un Some unknown metric.\n# TYPE un unknown\nun 1\n# EOF\n",
            vec![
                pe! { m: "un", help: "Some unknown metric." },
                pe! { m: "un", typ: MetricType::Unknown },
                pe! { m: "un", v: 1.0, lset: ls!("__name__", "un") },
            ],
        ),
        (
            "no type declaration",
            "bare_metric 42\n# EOF\n",
            vec![pe! { m: "bare_metric", v: 42.0, lset: ls!("__name__", "bare_metric") }],
        ),
        (
            "explicit type and unit",
            "# TYPE un_seconds unknown\n# UNIT un_seconds seconds\nun_seconds 1.5\n# EOF\n",
            vec![
                pe! { m: "un_seconds", typ: MetricType::Unknown },
                pe! { m: "un_seconds", unit: "seconds" },
                pe! { m: "un_seconds", v: 1.5, lset: ls!("__name__", "un_seconds") },
            ],
        ),
    ];
    for (name, input, exp) in cases {
        eprintln!("case {name}");
        let got = parse(input.as_bytes(), ParserOptions::default());
        require_entries(&exp, &got);
    }
}

#[test]
fn test_open_metrics2_parse_errors() {
    let cases: &[(&[u8], &str)] = &[
        (b"metric 1.0\n", "does not end with # EOF"),
        (b"# EOF\nextra\n", "unexpected data after # EOF"),
        // Composite value with unknown type (no # TYPE before).
        (b"foo {count:1,sum:1.0}\n# EOF\n", "composite value not supported"),
        // OM2 requires every exemplar to carry a timestamp.
        (b"# TYPE foo_total counter\nfoo_total 1.0 # {id=\"x\"} 1.0\n# EOF\n", "expected exemplar timestamp"),
        (
            b"# TYPE foo histogram\nfoo {sum:1.0,schema:0,zero_threshold:0,zero_count:0}\n# EOF\n",
            "missing required field: count",
        ),
        (
            b"# TYPE foo histogram\nfoo {count:1,schema:0,zero_threshold:0,zero_count:0}\n# EOF\n",
            "missing required field: sum",
        ),
        (
            b"# TYPE foo histogram\nfoo {count:1,sum:1.0,schema:0,zero_count:0}\n# EOF\n",
            "missing required field: zero_threshold",
        ),
        (
            b"# TYPE foo histogram\nfoo {count:1,sum:1.0,schema:0,zero_threshold:0}\n# EOF\n",
            "missing required field: zero_count",
        ),
        (
            b"# TYPE requests_total counter\nrequests_total 42.0 1000000.0 2000000.0\n# EOF\n",
            "duplicate timestamp",
        ),
        (
            b"# TYPE requests_total counter\nrequests_total 42.0 1000000.0 st@500000.0 st@600000.0\n# EOF\n",
            "duplicate start timestamp",
        ),
        (
            b"# TYPE requests_total counter\nrequests_total 42.0 st@500000.0 1000000.0\n# EOF\n",
            "timestamp must precede start timestamp",
        ),
        (
            b"# TYPE foo summary\nfoo {count:3,sum:6.0,quantile:[0.99:1.0,0.5:2.0]}\n# EOF\n",
            "quantiles must be sorted in increasing order",
        ),
        (b"{\"a\xffb\"} 1.0\n# EOF\n", "invalid UTF-8 metric name"),
        (b"# TYPE \"a\xffb\" counter\n# EOF\n", "invalid UTF-8 metric name"),
        (b"# TYPE foo counter\nfoo{\"a\xffb\"=\"v\"} 1.0\n# EOF\n", "invalid UTF-8 label name"),
        (b"{\"\",label=\"v\"} 1.0\n# EOF\n", "metric name must not be empty"),
        (b"# TYPE \"\" counter\n# EOF\n", "metric name must not be empty"),
        (b"{label=\"v\",\"m.n\"} 1.0\n# EOF\n", "metric name must be the first item in the label set"),
        (
            b"# TYPE foo histogram\nfoo {count:3,sum:6.0,bucket:[+Inf:3,0.1:1,1.0:2]}\n# EOF\n",
            "classic histogram buckets must be sorted in increasing order",
        ),
        (b"# TYPE foo histogram\nfoo {count:1,count:99,sum:2.0,bucket:[+Inf:1]}\n# EOF\n", "duplicate composite field"),
        (b"# TYPE foo histogram\nfoo {count:5,sum:2.0,buckets:[1:3,+Inf:5]}\n# EOF\n", "unknown composite field"),
        (b"# TYPE foo summary\nfoo {count:1,sum:2.0,quantiles:[0.5:1.0]}\n# EOF\n", "unknown composite field"),
        (
            b"# TYPE foo histogram\nfoo {count:1,sum:1.0,schema:9,zero_threshold:0,zero_count:0}\n# EOF\n",
            "schema must be between -4 and 8",
        ),
        (
            b"# TYPE foo histogram\nfoo {count:1,sum:1.0,schema:-5,zero_threshold:0,zero_count:0}\n# EOF\n",
            "schema must be between -4 and 8",
        ),
        (
            b"# TYPE foo histogram\nfoo {count:1,sum:1.0,schema:4294967296,zero_threshold:0,zero_count:0}\n# EOF\n",
            "schema must be between -4 and 8",
        ),
        (
            b"# TYPE foo histogram\nfoo {count:1,sum:1.0,schema:0,zero_threshold:-1,zero_count:0}\n# EOF\n",
            "zero_threshold must be a non-negative, finite number",
        ),
        (
            b"# TYPE foo histogram\nfoo {count:1,sum:1.0,schema:0,zero_threshold:NaN,zero_count:0}\n# EOF\n",
            "zero_threshold must be a non-negative, finite number",
        ),
        (
            b"# TYPE foo histogram\nfoo {count:1,sum:1.0,schema:0,zero_threshold:+Inf,zero_count:0}\n# EOF\n",
            "zero_threshold must be a non-negative, finite number",
        ),
        (b"# TYPE foo histogram\nfoo 5\n# EOF\n", "composite value required for metric type"),
        (b"# TYPE foo gaugehistogram\nfoo 5\n# EOF\n", "composite value required for metric type"),
        (b"# TYPE foo summary\nfoo 5\n# EOF\n", "composite value required for metric type"),
        (b"# TYPE foo summary\nfoo {sum:2.0,quantile:[0.5:1.0]}\n# EOF\n", "missing required field: count"),
        (b"# TYPE foo summary\nfoo {count:1,quantile:[0.5:1.0]}\n# EOF\n", "missing required field: sum"),
        (b"# TYPE foo summary\nfoo {count:1,sum:2.0}\n# EOF\n", "missing required field: quantile"),
        (b"# TYPE foo histogram\nfoo {sum:2.0,bucket:[+Inf:1]}\n# EOF\n", "missing required field: count"),
        (b"# TYPE foo histogram\nfoo {count:1,bucket:[+Inf:1]}\n# EOF\n", "missing required field: sum"),
        (b"# TYPE foo histogram\nfoo {count:1,sum:2.0}\n# EOF\n", "missing required field: bucket"),
        (
            b"# TYPE foo histogram\nfoo {count:1,sum:2.0,bucket:[0.1:1]}\n# EOF\n",
            "classic histogram buckets must include a +Inf threshold",
        ),
        (b"# TYPE ss stateset\nss{ss=\"foo\"} 2\n# EOF\n", "stateset sample value must be 0 or 1"),
        (
            b"# TYPE ss stateset\nss{other=\"foo\"} 1\n# EOF\n",
            "stateset sample must have a label matching the metric family name",
        ),
        (b"# TYPE foo_total counter\nfoo_total NaN\n# EOF\n", "counter sample value must not be NaN"),
        (
            // A NaN sum relaxes the Validate check to "count is at least the bucket total", so the wrapped count
            // was accepted as 18446744073709551615.
            b"# TYPE foo histogram\nfoo {count:-1,sum:NaN,schema:0,zero_threshold:0,zero_count:0,positive_spans:[0:2],positive_buckets:[2,1]}\n# EOF\n",
            "count must not be negative",
        ),
        (
            // A gauge histogram takes the same integer path, and no histogram type can hold a negative count, so
            // gcount is checked too.
            b"# TYPE foo gaugehistogram\nfoo {gcount:-1,gsum:NaN,schema:0,zero_threshold:0,zero_count:0,positive_spans:[0:2],positive_buckets:[2,1]}\n# EOF\n",
            "gcount must not be negative",
        ),
        (
            b"# TYPE foo histogram\nfoo {count:1,sum:1.0,schema:0,zero_threshold:0,zero_count:-1}\n# EOF\n",
            "zero_count must not be negative",
        ),
        (
            // Histogram bucket values are counters, so they must not be negative.
            b"# TYPE foo histogram\nfoo {count:3,sum:6.0,bucket:[1.0:-1,+Inf:3]}\n# EOF\n",
            "value must not be negative",
        ),
        (b"# TYPE foo histogram\nfoo {count:3,sum:6.0,bucket:[abc:1,+Inf:3]}\n# EOF\n", "invalid bucket threshold"),
        (
            b"# TYPE foo histogram\nfoo {count:3,sum:6.0,bucket:[NaN:1,+Inf:3]}\n# EOF\n",
            "bucket threshold must not be NaN",
        ),
        (
            // -Inf is only permitted as the first threshold.
            b"# TYPE foo histogram\nfoo {count:3,sum:6.0,bucket:[1.0:1,-Inf:2,+Inf:3]}\n# EOF\n",
            "classic histogram buckets must be sorted in increasing order",
        ),
        (b"# TYPE foo summary\nfoo {count:1,sum:2.0,quantile:[abc:1.0]}\n# EOF\n", "invalid quantile"),
        (b"# TYPE foo summary\nfoo {count:1,sum:2.0,quantile:[1.5:1.0]}\n# EOF\n", "quantile must be between 0.0 and 1.0"),
        (b"# TYPE foo summary\nfoo {count:1,sum:2.0,quantile:[NaN:1.0]}\n# EOF\n", "quantile must not be NaN"),
    ];
    for &(input, want) in cases {
        let err = first_error(input, ParserOptions::default());
        require_error_contains(&err, want);
    }
}

/// A composite carrying zero_threshold/zero_count but no schema falls through to classic histogram parsing
/// instead of failing with "missing required field: schema".
#[test]
fn test_open_metrics2_parse_histogram_native_detection_requires_schema() {
    let input = "# TYPE foo histogram\nfoo {count:1,sum:1.0,zero_threshold:0,zero_count:0,bucket:[+Inf:1]}\n# EOF\n";
    let exp = vec![
        pe! { m: "foo", typ: MetricType::Histogram },
        pe! { m: "foo_count", v: 1.0, lset: ls!("__name__", "foo_count") },
        pe! { m: "foo_sum", v: 1.0, lset: ls!("__name__", "foo_sum") },
        pe! { m: b"foo_bucket\xffle\xff+Inf", v: 1.0, lset: ls!("__name__", "foo_bucket", "le", "+Inf") },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

/// The parser returns EOF when the input ends with "# EOF\n".
#[test]
fn test_open_metrics2_parse_eof_handling() {
    let mut p = OpenMetrics2Parser::new(b"# EOF\n", &ParserOptions::default());
    // adapted: Go returns (EntryInvalid, io.EOF); the Rust `next` returns `Err(Error::Eof)`, which carries no entry.
    assert_eq!(p.next(), Err(Error::Eof));
}

#[test]
fn test_open_metrics2_parse_composite_extra_labels_utf8() {
    let input = r##"# TYPE "req.duration" histogram
{"req.duration",job="api"} {count:3,sum:6.0,bucket:[0.1:1,+Inf:3]}
# EOF
"##;
    let exp = vec![
        pe! { m: "req.duration", typ: MetricType::Histogram },
        pe! { m: b"req.duration_count\xffjob\xffapi", v: 3.0, lset: ls!("__name__", "req.duration_count", "job", "api") },
        pe! { m: b"req.duration_sum\xffjob\xffapi", v: 6.0, lset: ls!("__name__", "req.duration_sum", "job", "api") },
        pe! {
            m: b"req.duration_bucket\xffjob\xffapi\xffle\xff0.1",
            v: 1.0,
            lset: ls!("__name__", "req.duration_bucket", "job", "api", "le", "0.1"),
        },
        pe! {
            m: b"req.duration_bucket\xffjob\xffapi\xffle\xff+Inf",
            v: 3.0,
            lset: ls!("__name__", "req.duration_bucket", "job", "api", "le", "+Inf"),
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_composite_extra_labels() {
    let input = r##"# TYPE req_duration histogram
req_duration{job="api",env="prod"} {count:3,sum:6.0,bucket:[0.1:1,1.0:2,+Inf:3]}
# EOF
"##;
    let exp = vec![
        pe! { m: "req_duration", typ: MetricType::Histogram },
        pe! {
            m: b"req_duration_count\xffjob\xffapi\xffenv\xffprod",
            v: 3.0,
            lset: ls!("__name__", "req_duration_count", "env", "prod", "job", "api"),
        },
        pe! {
            m: b"req_duration_sum\xffjob\xffapi\xffenv\xffprod",
            v: 6.0,
            lset: ls!("__name__", "req_duration_sum", "env", "prod", "job", "api"),
        },
        pe! {
            m: b"req_duration_bucket\xffjob\xffapi\xffenv\xffprod\xffle\xff0.1",
            v: 1.0,
            lset: ls!("__name__", "req_duration_bucket", "env", "prod", "job", "api", "le", "0.1"),
        },
        pe! {
            m: b"req_duration_bucket\xffjob\xffapi\xffenv\xffprod\xffle\xff1.0",
            v: 2.0,
            lset: ls!("__name__", "req_duration_bucket", "env", "prod", "job", "api", "le", "1.0"),
        },
        pe! {
            m: b"req_duration_bucket\xffjob\xffapi\xffenv\xffprod\xffle\xff+Inf",
            v: 3.0,
            lset: ls!("__name__", "req_duration_bucket", "env", "prod", "job", "api", "le", "+Inf"),
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

/// Quoted metric and label names as well as escaped label values on a composite line are extracted and
/// unescaped with the expected Series() bytes and Labels().
#[test]
fn test_open_metrics2_parse_composite_extra_labels_quoted() {
    let cases: Vec<(&str, &str, Vec<ParsedEntry>)> = vec![
        (
            "utf8 label name",
            r##"# TYPE foo summary
foo{"user.id"="x"} {count:1,sum:2.0,quantile:[0.5:1.0]}
# EOF
"##,
            vec![
                pe! { m: "foo", typ: MetricType::Summary },
                pe! { m: b"foo_count\xffuser.id\xffx", v: 1.0, lset: ls!("__name__", "foo_count", "user.id", "x") },
                pe! { m: b"foo_sum\xffuser.id\xffx", v: 2.0, lset: ls!("__name__", "foo_sum", "user.id", "x") },
                pe! {
                    m: b"foo\xffuser.id\xffx\xffquantile\xff0.5",
                    v: 1.0,
                    lset: ls!("__name__", "foo", "quantile", "0.5", "user.id", "x"),
                },
            ],
        ),
        (
            "escaped values and quoted names on histogram",
            r##"# TYPE "my\"metric" histogram
{"my\"metric","a\"k"="x\"y\n",b="\\",c=""} {count:2,sum:3.0,bucket:[1.0:1,+Inf:2]}
# EOF
"##,
            vec![
                pe! { m: r#"my\"metric"#, typ: MetricType::Histogram },
                pe! {
                    m: b"my\\\"metric_count\xffa\\\"k\xffx\\\"y\\n\xffb\xff\\\\\xffc\xff",
                    v: 2.0,
                    lset: ls!("__name__", r#"my"metric_count"#, r#"a"k"#, "x\"y\n", "b", r"\", "c", ""),
                },
                pe! {
                    m: b"my\\\"metric_sum\xffa\\\"k\xffx\\\"y\\n\xffb\xff\\\\\xffc\xff",
                    v: 3.0,
                    lset: ls!("__name__", r#"my"metric_sum"#, r#"a"k"#, "x\"y\n", "b", r"\", "c", ""),
                },
                pe! {
                    m: b"my\\\"metric_bucket\xffa\\\"k\xffx\\\"y\\n\xffb\xff\\\\\xffc\xff\xffle\xff1.0",
                    v: 1.0,
                    lset: ls!("__name__", r#"my"metric_bucket"#, r#"a"k"#, "x\"y\n", "b", r"\", "c", "", "le", "1.0"),
                },
                pe! {
                    m: b"my\\\"metric_bucket\xffa\\\"k\xffx\\\"y\\n\xffb\xff\\\\\xffc\xff\xffle\xff+Inf",
                    v: 2.0,
                    lset: ls!("__name__", r#"my"metric_bucket"#, r#"a"k"#, "x\"y\n", "b", r"\", "c", "", "le", "+Inf"),
                },
            ],
        ),
        (
            "escaped values and quoted names on summary",
            r##"# TYPE "my\"metric" summary
{"my\"metric","a\"k"="x\"y\n",b="\\",c=""} {count:2,sum:3.0,quantile:[0.5:1.5]}
# EOF
"##,
            vec![
                pe! { m: r#"my\"metric"#, typ: MetricType::Summary },
                pe! {
                    m: b"my\\\"metric_count\xffa\\\"k\xffx\\\"y\\n\xffb\xff\\\\\xffc\xff",
                    v: 2.0,
                    lset: ls!("__name__", r#"my"metric_count"#, r#"a"k"#, "x\"y\n", "b", r"\", "c", ""),
                },
                pe! {
                    m: b"my\\\"metric_sum\xffa\\\"k\xffx\\\"y\\n\xffb\xff\\\\\xffc\xff",
                    v: 3.0,
                    lset: ls!("__name__", r#"my"metric_sum"#, r#"a"k"#, "x\"y\n", "b", r"\", "c", ""),
                },
                pe! {
                    m: b"my\\\"metric\xffa\\\"k\xffx\\\"y\\n\xffb\xff\\\\\xffc\xff\xffquantile\xff0.5",
                    v: 1.5,
                    lset: ls!("__name__", r#"my"metric"#, r#"a"k"#, "x\"y\n", "b", r"\", "c", "", "quantile", "0.5"),
                },
            ],
        ),
    ];
    for (name, input, exp) in cases {
        eprintln!("case {name}");
        let got = parse(input.as_bytes(), ParserOptions::default());
        require_entries(&exp, &got);
    }
}

/// A quoted extra label whose name itself contains "=" (valid per the OM2 UTF-8 label-name grammar) does not
/// confuse the key/value split for a composite line's extra labels.
#[test]
fn test_open_metrics2_parse_composite_extra_labels_quoted_with_equals() {
    let input = r##"# TYPE foo summary
foo{"a=b"="v",job="api"} {count:1,sum:2.0,quantile:[0.5:1.0]}
# EOF
"##;
    let exp = vec![
        pe! { m: "foo", typ: MetricType::Summary },
        pe! {
            m: b"foo_count\xffa=b\xffv\xffjob\xffapi",
            v: 1.0,
            lset: ls!("__name__", "foo_count", "a=b", "v", "job", "api"),
        },
        pe! {
            m: b"foo_sum\xffa=b\xffv\xffjob\xffapi",
            v: 2.0,
            lset: ls!("__name__", "foo_sum", "a=b", "v", "job", "api"),
        },
        pe! {
            m: b"foo\xffa=b\xffv\xffjob\xffapi\xffquantile\xff0.5",
            v: 1.0,
            lset: ls!("__name__", "foo", "a=b", "v", "job", "api", "quantile", "0.5"),
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

/// Series() never returns the same bytes for two entries with different label sets; the scrape loop uses those
/// bytes as a cache key, so a collision would corrupt or drop samples.
#[test]
fn test_open_metrics2_parse_composite_series_unique() {
    let cases: &[(&str, &str)] = &[
        ("classic histogram buckets", "# TYPE foo histogram\nfoo {count:5,sum:12.5,bucket:[0.5:1,1:3,+Inf:5]}\n# EOF\n"),
        (
            "classic histogram with differing extra labels",
            "# TYPE foo histogram\nfoo{path=\"/a\"} {count:1,sum:1.0,bucket:[1.0:1,+Inf:1]}\nfoo{path=\"/b\"} {count:2,sum:2.0,bucket:[1.0:1,+Inf:2]}\n# EOF\n",
        ),
        ("summary quantiles", "# TYPE foo summary\nfoo {count:5,sum:12.5,quantile:[0.5:1.0,0.9:2.0,0.99:3.0]}\n# EOF\n"),
    ];
    for &(name, input) in cases {
        let got = parse(input.as_bytes(), ParserOptions::default());
        let mut seen: std::collections::HashMap<Vec<u8>, Labels> = std::collections::HashMap::new();
        for e in &got {
            if let Some(prev) = seen.get(&e.m) {
                assert!(
                    *prev == e.lset,
                    "{name}: series key {:?} reused for a different label set: first saw {prev}, now {}",
                    String::from_utf8_lossy(&e.m),
                    e.lset
                );
            }
            seen.insert(e.m.clone(), e.lset.clone());
        }
    }
}

#[test]
fn test_open_metrics2_parse_composite_with_timestamp() {
    let input = "# TYPE req_duration histogram\nreq_duration {count:2,sum:4.0,bucket:[+Inf:2]} 1234567.0 st@1000.0\n# EOF\n";
    let exp = vec![
        pe! { m: "req_duration", typ: MetricType::Histogram },
        pe! { m: "req_duration_count", v: 2.0, t: Some(1234567000), st: 1000000, lset: ls!("__name__", "req_duration_count") },
        pe! { m: "req_duration_sum", v: 4.0, t: Some(1234567000), st: 1000000, lset: ls!("__name__", "req_duration_sum") },
        pe! {
            m: b"req_duration_bucket\xffle\xff+Inf",
            v: 2.0,
            t: Some(1234567000),
            st: 1000000,
            lset: ls!("__name__", "req_duration_bucket", "le", "+Inf"),
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_float_histogram() {
    let input = r##"# TYPE test_histogram histogram
test_histogram {count:5.5,sum:12.1,schema:0,zero_threshold:0.001,zero_count:2.5,negative_spans:[],negative_buckets:[],positive_spans:[0:2],positive_buckets:[2.0,1.0]}
# EOF
"##;
    let exp = vec![
        pe! { m: "test_histogram", typ: MetricType::Histogram },
        pe! {
            m: "test_histogram",
            lset: ls!("__name__", "test_histogram"),
            fhs: Some(FloatHistogram {
                schema: 0,
                zero_threshold: 0.001,
                zero_count: 2.5,
                count: 5.5,
                sum: 12.1,
                positive_spans: spans(&[(0, 2)]),
                positive_buckets: vec![2.0, 1.0],
                ..Default::default()
            }),
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

/// Each input that can flip the parser between Histogram and FloatHistogram. Per the OM2 spec a native histogram
/// is integer only if count, zero_count, and every bucket value are integers; sum must not discriminate.
#[test]
fn test_open_metrics2_parse_native_histogram_int_vs_float_discriminator() {
    let ih = |count: u64, zero_count: u64, sum: f64, pos: Vec<i64>| Histogram {
        schema: 0,
        zero_threshold: 0.001,
        zero_count,
        count,
        sum,
        positive_spans: spans(&[(0, 2)]),
        positive_buckets: pos,
        ..Default::default()
    };
    let fhp = |count: f64, zero_count: f64, pos: Vec<f64>| FloatHistogram {
        schema: 0,
        zero_threshold: 0.001,
        zero_count,
        count,
        sum: 12.1,
        positive_spans: spans(&[(0, 2)]),
        positive_buckets: pos,
        ..Default::default()
    };
    let cases: Vec<(&str, &str, Option<Histogram>, Option<FloatHistogram>)> = vec![
        (
            "all integer fields -> Histogram",
            "h {count:5,sum:12.1,schema:0,zero_threshold:0.001,zero_count:2,positive_spans:[0:2],positive_buckets:[2,1]}",
            Some(ih(5, 2, 12.1, vec![2, -1])),
            None,
        ),
        (
            "fractional count -> FloatHistogram",
            "h {count:5.5,sum:12.1,schema:0,zero_threshold:0.001,zero_count:2,positive_spans:[0:2],positive_buckets:[2,1]}",
            None,
            Some(fhp(5.5, 2.0, vec![2.0, 1.0])),
        ),
        (
            "fractional zero_count -> FloatHistogram",
            "h {count:5,sum:12.1,schema:0,zero_threshold:0.001,zero_count:2.5,positive_spans:[0:2],positive_buckets:[2,1]}",
            None,
            Some(fhp(5.0, 2.5, vec![2.0, 1.0])),
        ),
        (
            "fractional positive bucket -> FloatHistogram",
            "h {count:5,sum:12.1,schema:0,zero_threshold:0.001,zero_count:2,positive_spans:[0:2],positive_buckets:[2.0,1.0]}",
            None,
            Some(fhp(5.0, 2.0, vec![2.0, 1.0])),
        ),
        (
            "fractional negative bucket -> FloatHistogram",
            "h {count:5,sum:12.1,schema:0,zero_threshold:0.001,zero_count:2,negative_spans:[0:2],negative_buckets:[2.0,1.0]}",
            None,
            Some(FloatHistogram {
                schema: 0,
                zero_threshold: 0.001,
                zero_count: 2.0,
                count: 5.0,
                sum: 12.1,
                negative_spans: spans(&[(0, 2)]),
                negative_buckets: vec![2.0, 1.0],
                ..Default::default()
            }),
        ),
        (
            "exponent notation in bucket -> FloatHistogram",
            "h {count:5,sum:12.1,schema:0,zero_threshold:0.001,zero_count:2,positive_spans:[0:2],positive_buckets:[1e2,1]}",
            None,
            Some(fhp(5.0, 2.0, vec![1e2, 1.0])),
        ),
        (
            "count with no uint64 representation -> FloatHistogram",
            "h {count:1e20,sum:12.1,schema:0,zero_threshold:0.001,zero_count:2,positive_spans:[0:2],positive_buckets:[2,1]}",
            None,
            Some(fhp(1e20, 2.0, vec![2.0, 1.0])),
        ),
        (
            "infinite count -> FloatHistogram",
            "h {count:+Inf,sum:12.1,schema:0,zero_threshold:0.001,zero_count:2,positive_spans:[0:2],positive_buckets:[2,1]}",
            None,
            Some(fhp(f64::INFINITY, 2.0, vec![2.0, 1.0])),
        ),
        (
            "zero_count with no uint64 representation -> FloatHistogram",
            "h {count:5,sum:12.1,schema:0,zero_threshold:0.001,zero_count:1e20,positive_spans:[0:2],positive_buckets:[2,1]}",
            None,
            Some(fhp(5.0, 1e20, vec![2.0, 1.0])),
        ),
        (
            "fractional sum only -> Histogram (sum does not discriminate)",
            "h {count:5,sum:12.5,schema:0,zero_threshold:0.001,zero_count:2,positive_spans:[0:2],positive_buckets:[2,1]}",
            Some(ih(5, 2, 12.5, vec![2, -1])),
            None,
        ),
    ];
    for (name, line, shs, fhs) in cases {
        eprintln!("case {name}");
        let input = format!("# TYPE h histogram\n{line}\n# EOF\n");
        let exp = vec![
            pe! { m: "h", typ: MetricType::Histogram },
            pe! { m: "h", lset: ls!("__name__", "h"), shs: shs, fhs: fhs },
        ];
        let got = parse(input.as_bytes(), ParserOptions::default());
        require_entries(&exp, &got);
    }
}

#[test]
fn test_open_metrics2_parse_composite_gauge_histogram() {
    let input = r##"# TYPE req_size gaugehistogram
req_size {gcount:6,gsum:100.0,schema:0,zero_threshold:0.001,zero_count:1,negative_spans:[],negative_buckets:[],positive_spans:[0:2],positive_buckets:[3,2]}
# EOF
"##;
    let exp = vec![
        pe! { m: "req_size", typ: MetricType::GaugeHistogram },
        pe! {
            m: "req_size",
            lset: ls!("__name__", "req_size"),
            shs: Some(Histogram {
                schema: 0,
                zero_threshold: 0.001,
                zero_count: 1,
                count: 6,
                sum: 100.0,
                positive_spans: spans(&[(0, 2)]),
                positive_buckets: vec![3, -1],
                counter_reset_hint: CounterResetHint::GaugeType,
                ..Default::default()
            }),
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions::default());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_type_and_unit_labels() {
    let input = r##"# TYPE process_open_fds gauge
# UNIT process_open_fds fds
process_open_fds 8.0
# TYPE rpc_duration_seconds summary
# UNIT rpc_duration_seconds seconds
rpc_duration_seconds {count:10,sum:5.0,quantile:[0.5:1.0]}
# TYPE rpc_duration_seconds_count gauge
rpc_duration_seconds_count 99.0
# EOF
"##;
    let exp = vec![
        pe! { m: "process_open_fds", typ: MetricType::Gauge },
        pe! { m: "process_open_fds", unit: "fds" },
        pe! {
            m: "process_open_fds",
            v: 8.0,
            lset: ls!("__name__", "process_open_fds", "__type__", "gauge", "__unit__", "fds"),
        },
        pe! { m: "rpc_duration_seconds", typ: MetricType::Summary },
        pe! { m: "rpc_duration_seconds", unit: "seconds" },
        pe! {
            m: b"rpc_duration_seconds_count\xff__type__\xffsummary\xff__unit__\xffseconds",
            v: 10.0,
            lset: ls!("__name__", "rpc_duration_seconds_count", "__type__", "summary", "__unit__", "seconds"),
        },
        pe! {
            m: b"rpc_duration_seconds_sum\xff__type__\xffsummary\xff__unit__\xffseconds",
            v: 5.0,
            lset: ls!("__name__", "rpc_duration_seconds_sum", "__type__", "summary", "__unit__", "seconds"),
        },
        pe! {
            m: b"rpc_duration_seconds\xff__type__\xffsummary\xff__unit__\xffseconds\xffquantile\xff0.5",
            v: 1.0,
            lset: ls!("__name__", "rpc_duration_seconds", "__type__", "summary", "__unit__", "seconds", "quantile", "0.5"),
        },
        pe! { m: "rpc_duration_seconds_count", typ: MetricType::Gauge },
        pe! {
            m: "rpc_duration_seconds_count",
            v: 99.0,
            lset: ls!("__name__", "rpc_duration_seconds_count", "__type__", "gauge"),
        },
    ];
    let got = parse(input.as_bytes(), type_and_unit());
    require_entries(&exp, &got);
    // Summary _count and flat gauge with the same metric name must have distinct Series() cache keys so the
    // scrape cache does not conflate them.
    assert_ne!(got[5].m, got[9].m);
}

#[test]
fn test_open_metrics2_parse_type_and_unit_labels_scalar_types() {
    let input = r##"# TYPE http_requests_total counter
# UNIT http_requests_total requests
http_requests_total{method="get"} 5.0
# TYPE build_info info
build_info{version="1.0"} 1.0
# TYPE subsystem_enabled stateset
subsystem_enabled{subsystem_enabled="auth"} 0.0
# TYPE unknown_metric unknown
unknown_metric 42.0
# EOF
"##;
    let exp = vec![
        pe! { m: "http_requests_total", typ: MetricType::Counter },
        pe! { m: "http_requests_total", unit: "requests" },
        pe! {
            m: r#"http_requests_total{method="get"}"#,
            v: 5.0,
            lset: ls!("__name__", "http_requests_total", "__type__", "counter", "__unit__", "requests", "method", "get"),
        },
        pe! { m: "build_info", typ: MetricType::Info },
        pe! {
            m: r#"build_info{version="1.0"}"#,
            v: 1.0,
            lset: ls!("__name__", "build_info", "__type__", "info", "version", "1.0"),
        },
        pe! { m: "subsystem_enabled", typ: MetricType::StateSet },
        pe! {
            m: r#"subsystem_enabled{subsystem_enabled="auth"}"#,
            v: 0.0,
            lset: ls!("__name__", "subsystem_enabled", "__type__", "stateset", "subsystem_enabled", "auth"),
        },
        pe! { m: "unknown_metric", typ: MetricType::Unknown },
        pe! { m: "unknown_metric", v: 42.0, lset: ls!("__name__", "unknown_metric") },
    ];
    let got = parse(input.as_bytes(), type_and_unit());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_type_and_unit_labels_classic_histogram() {
    let input = r##"# TYPE http_request_duration_seconds histogram
# UNIT http_request_duration_seconds seconds
http_request_duration_seconds {count:3,sum:6.0,bucket:[0.1:1,0.5:2,+Inf:3]}
http_request_duration_seconds{__type__="counter",__unit__="bytes",x="y"} {count:1,sum:2.0,bucket:[+Inf:1]}
# EOF
"##;
    let exp = vec![
        pe! { m: "http_request_duration_seconds", typ: MetricType::Histogram },
        pe! { m: "http_request_duration_seconds", unit: "seconds" },
        pe! {
            m: b"http_request_duration_seconds_count\xff__type__\xffhistogram\xff__unit__\xffseconds",
            v: 3.0,
            lset: ls!("__name__", "http_request_duration_seconds_count", "__type__", "histogram", "__unit__", "seconds"),
        },
        pe! {
            m: b"http_request_duration_seconds_sum\xff__type__\xffhistogram\xff__unit__\xffseconds",
            v: 6.0,
            lset: ls!("__name__", "http_request_duration_seconds_sum", "__type__", "histogram", "__unit__", "seconds"),
        },
        pe! {
            m: b"http_request_duration_seconds_bucket\xff__type__\xffhistogram\xff__unit__\xffseconds\xffle\xff0.1",
            v: 1.0,
            lset: ls!(
                "__name__", "http_request_duration_seconds_bucket", "__type__", "histogram", "__unit__", "seconds",
                "le", "0.1"
            ),
        },
        pe! {
            m: b"http_request_duration_seconds_bucket\xff__type__\xffhistogram\xff__unit__\xffseconds\xffle\xff0.5",
            v: 2.0,
            lset: ls!(
                "__name__", "http_request_duration_seconds_bucket", "__type__", "histogram", "__unit__", "seconds",
                "le", "0.5"
            ),
        },
        pe! {
            m: b"http_request_duration_seconds_bucket\xff__type__\xffhistogram\xff__unit__\xffseconds\xffle\xff+Inf",
            v: 3.0,
            lset: ls!(
                "__name__", "http_request_duration_seconds_bucket", "__type__", "histogram", "__unit__", "seconds",
                "le", "+Inf"
            ),
        },
        pe! {
            m: b"http_request_duration_seconds_count\xff__type__\xffhistogram\xff__unit__\xffseconds\xff__type__\xffcounter\xff__unit__\xffbytes\xffx\xffy",
            v: 1.0,
            lset: ls!(
                "__name__", "http_request_duration_seconds_count", "__type__", "histogram", "__unit__", "seconds",
                "x", "y"
            ),
        },
        pe! {
            m: b"http_request_duration_seconds_sum\xff__type__\xffhistogram\xff__unit__\xffseconds\xff__type__\xffcounter\xff__unit__\xffbytes\xffx\xffy",
            v: 2.0,
            lset: ls!(
                "__name__", "http_request_duration_seconds_sum", "__type__", "histogram", "__unit__", "seconds",
                "x", "y"
            ),
        },
        pe! {
            m: b"http_request_duration_seconds_bucket\xff__type__\xffhistogram\xff__unit__\xffseconds\xff__type__\xffcounter\xff__unit__\xffbytes\xffx\xffy\xffle\xff+Inf",
            v: 1.0,
            lset: ls!(
                "__name__", "http_request_duration_seconds_bucket", "__type__", "histogram", "__unit__", "seconds",
                "le", "+Inf", "x", "y"
            ),
        },
    ];
    let got = parse(input.as_bytes(), type_and_unit());
    require_entries(&exp, &got);

    // Changing TYPE or UNIT on a later scrape must produce distinct Series() bytes for shared series like _bucket
    // so the scrape cache refreshes Labels().
    let input_gauge_hist = r##"# TYPE http_request_duration_seconds gaugehistogram
http_request_duration_seconds {gcount:3,gsum:6.0,bucket:[0.1:1,0.5:2,+Inf:3]}
# EOF
"##;
    let got2 = parse(input_gauge_hist.as_bytes(), type_and_unit());
    assert_ne!(got[4].m, got2[3].m);
}

#[test]
fn test_open_metrics2_parse_type_and_unit_labels_gauge_histogram() {
    let input = r##"# TYPE response_size_bytes gaugehistogram
# UNIT response_size_bytes bytes
response_size_bytes {gcount:4,gsum:1024.0,bucket:[100:1,500:3,+Inf:4]}
# EOF
"##;
    let exp = vec![
        pe! { m: "response_size_bytes", typ: MetricType::GaugeHistogram },
        pe! { m: "response_size_bytes", unit: "bytes" },
        pe! {
            m: b"response_size_bytes_gcount\xff__type__\xffgaugehistogram\xff__unit__\xffbytes",
            v: 4.0,
            lset: ls!("__name__", "response_size_bytes_gcount", "__type__", "gaugehistogram", "__unit__", "bytes"),
        },
        pe! {
            m: b"response_size_bytes_gsum\xff__type__\xffgaugehistogram\xff__unit__\xffbytes",
            v: 1024.0,
            lset: ls!("__name__", "response_size_bytes_gsum", "__type__", "gaugehistogram", "__unit__", "bytes"),
        },
        pe! {
            m: b"response_size_bytes_bucket\xff__type__\xffgaugehistogram\xff__unit__\xffbytes\xffle\xff100.0",
            v: 1.0,
            lset: ls!(
                "__name__", "response_size_bytes_bucket", "__type__", "gaugehistogram", "__unit__", "bytes", "le",
                "100.0"
            ),
        },
        pe! {
            m: b"response_size_bytes_bucket\xff__type__\xffgaugehistogram\xff__unit__\xffbytes\xffle\xff500.0",
            v: 3.0,
            lset: ls!(
                "__name__", "response_size_bytes_bucket", "__type__", "gaugehistogram", "__unit__", "bytes", "le",
                "500.0"
            ),
        },
        pe! {
            m: b"response_size_bytes_bucket\xff__type__\xffgaugehistogram\xff__unit__\xffbytes\xffle\xff+Inf",
            v: 4.0,
            lset: ls!(
                "__name__", "response_size_bytes_bucket", "__type__", "gaugehistogram", "__unit__", "bytes", "le",
                "+Inf"
            ),
        },
    ];
    let got = parse(input.as_bytes(), type_and_unit());
    require_entries(&exp, &got);
}

/// Classic histogram buckets out of increasing order are rejected: "Classic Buckets MUST be sorted in number
/// increasing order".
#[test]
fn test_open_metrics2_parse_unsorted_classic_buckets_rejected() {
    let input = "# TYPE req_duration histogram\nreq_duration {count:144,sum:53.4,bucket:[+Inf:144,0.1:24,0.2:33,0.4:100,1.0:144]}\n# EOF\n";
    let err = first_error(input.as_bytes(), ParserOptions::default());
    require_error_contains(&err, "classic histogram buckets must be sorted in increasing order");
}

#[test]
fn test_open_metrics2_parse_exemplar_on_composite_line() {
    let input = r##"# TYPE req_duration histogram
req_duration {count:5,sum:10.0,bucket:[-Inf:1,1.0:3,2.5:4,+Inf:5]} # {id="inf-ex"} 3.8 9999999.0 # {id="neginf-ex"} -Inf 9999995.0 # {id="b1-ex1"} 1.0 9999996.0 # {id="b1-ex2"} 0.5 9999997.0 # {id="nan-ex"} NaN 9999998.0
# TYPE queue_size gaugehistogram
queue_size {gcount:2,gsum:3.0,bucket:[1.0:1,+Inf:2]} # {id="g-ex"} 0.9 9999999.0
# TYPE native_ignored histogram
native_ignored {count:2,sum:4.0,schema:0,zero_threshold:0.001,zero_count:0,positive_spans:[0:1],positive_buckets:[2]} # {id="ni-ex"} 1.5 9999999.0
# EOF
"##;
    let exp = vec![
        pe! { m: "req_duration", typ: MetricType::Histogram },
        pe! { m: "req_duration_count", v: 5.0, lset: ls!("__name__", "req_duration_count") },
        pe! { m: "req_duration_sum", v: 10.0, lset: ls!("__name__", "req_duration_sum") },
        pe! {
            m: b"req_duration_bucket\xffle\xff-Inf",
            v: 1.0,
            lset: ls!("__name__", "req_duration_bucket", "le", "-Inf"),
            es: vec![ex(ls!("id", "neginf-ex"), f64::NEG_INFINITY, 9999995000)],
        },
        pe! {
            m: b"req_duration_bucket\xffle\xff1.0",
            v: 3.0,
            lset: ls!("__name__", "req_duration_bucket", "le", "1.0"),
            es: vec![ex(ls!("id", "b1-ex1"), 1.0, 9999996000), ex(ls!("id", "b1-ex2"), 0.5, 9999997000)],
        },
        pe! { m: b"req_duration_bucket\xffle\xff2.5", v: 4.0, lset: ls!("__name__", "req_duration_bucket", "le", "2.5") },
        pe! {
            m: b"req_duration_bucket\xffle\xff+Inf",
            v: 5.0,
            lset: ls!("__name__", "req_duration_bucket", "le", "+Inf"),
            es: vec![
                ex(ls!("id", "inf-ex"), 3.8, 9999999000),
                ex(ls!("id", "nan-ex"), f64::from_bits(NORMAL_NAN), 9999998000),
            ],
        },
        pe! { m: "queue_size", typ: MetricType::GaugeHistogram },
        pe! { m: "queue_size_gcount", v: 2.0, lset: ls!("__name__", "queue_size_gcount") },
        pe! { m: "queue_size_gsum", v: 3.0, lset: ls!("__name__", "queue_size_gsum") },
        pe! {
            m: b"queue_size_bucket\xffle\xff1.0",
            v: 1.0,
            lset: ls!("__name__", "queue_size_bucket", "le", "1.0"),
            es: vec![ex(ls!("id", "g-ex"), 0.9, 9999999000)],
        },
        pe! { m: b"queue_size_bucket\xffle\xff+Inf", v: 2.0, lset: ls!("__name__", "queue_size_bucket", "le", "+Inf") },
        pe! { m: "native_ignored", typ: MetricType::Histogram },
        pe! { m: "native_ignored_count", v: 2.0, lset: ls!("__name__", "native_ignored_count") },
        pe! { m: "native_ignored_sum", v: 4.0, lset: ls!("__name__", "native_ignored_sum") },
        pe! {
            m: b"native_ignored_bucket\xffle\xff+Inf",
            v: 2.0,
            lset: ls!("__name__", "native_ignored_bucket", "le", "+Inf"),
            es: vec![ex(ls!("id", "ni-ex"), 1.5, 9999999000)],
        },
    ];
    let got = parse(input.as_bytes(), ParserOptions { ignore_native_histograms: true, ..Default::default() });
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_descriptor_order_type_before_help() {
    // TYPE before HELP (and UNIT after HELP) must not corrupt the parsed type or unit.
    let input = r##"# TYPE open_fds gauge
# HELP open_fds Number of open file descriptors.
# UNIT open_fds fds
open_fds 8.0
# EOF
"##;
    let exp = vec![
        pe! { m: "open_fds", typ: MetricType::Gauge },
        pe! { m: "open_fds", help: "Number of open file descriptors." },
        pe! { m: "open_fds", unit: "fds" },
        pe! { m: "open_fds", v: 8.0, lset: ls!("__name__", "open_fds", "__type__", "gauge", "__unit__", "fds") },
    ];
    let got = parse(input.as_bytes(), type_and_unit());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_descriptor_order_unit_before_type() {
    // UNIT before TYPE: the unit must not be erased by the TYPE line.
    let input = r##"# UNIT http_requests requests
# TYPE http_requests counter
http_requests 1.0
# EOF
"##;
    let exp = vec![
        pe! { m: "http_requests", unit: "requests" },
        pe! { m: "http_requests", typ: MetricType::Counter },
        pe! {
            m: "http_requests",
            v: 1.0,
            lset: ls!("__name__", "http_requests", "__type__", "counter", "__unit__", "requests"),
        },
    ];
    let got = parse(input.as_bytes(), type_and_unit());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_family_reset_on_undeclared_metric() {
    // A metric family with no descriptor lines at all must not inherit mtype/unit from the previous family.
    let input = r##"# TYPE http_requests counter
# UNIT http_requests requests
http_requests 1.0
bare_metric 2.0
# EOF
"##;
    let exp = vec![
        pe! { m: "http_requests", typ: MetricType::Counter },
        pe! { m: "http_requests", unit: "requests" },
        pe! {
            m: "http_requests",
            v: 1.0,
            lset: ls!("__name__", "http_requests", "__type__", "counter", "__unit__", "requests"),
        },
        pe! { m: "bare_metric", v: 2.0, lset: ls!("__name__", "bare_metric") },
    ];
    let got = parse(input.as_bytes(), type_and_unit());
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_family_reset_on_suffixed_metric() {
    // OM2 removes the implicit suffix stripping of OM1: a Metric name must exactly match its MetricFamily name.
    let cases: Vec<(&str, &str, Vec<ParsedEntry>)> = vec![
        (
            "suffix mismatch reverts to unknown",
            "# TYPE foo counter\nfoo_total 1.0\n# EOF\n",
            vec![
                pe! { m: "foo", typ: MetricType::Counter },
                pe! { m: "foo_total", v: 1.0, lset: ls!("__name__", "foo_total") },
            ],
        ),
        (
            "exact match keeps counter type",
            "# TYPE foo_total counter\nfoo_total 1.0\n# EOF\n",
            vec![
                pe! { m: "foo_total", typ: MetricType::Counter },
                pe! { m: "foo_total", v: 1.0, lset: ls!("__name__", "foo_total", "__type__", "counter") },
            ],
        ),
    ];
    for (name, input, exp) in cases {
        eprintln!("case {name}");
        let got = parse(input.as_bytes(), type_and_unit());
        require_entries(&exp, &got);
    }
}

#[test]
fn test_open_metrics2_parse_exemplar_escaped_label_names_and_null_bytes() {
    let input = b"foo_total 1.0 1700000000.0 # {\"trace\\\\id\\n\\\"1\\\"\"=\"a\x00b\\nc\"} 1.0 1700000000.5\n# EOF\n";
    let got = parse(input, ParserOptions::default());
    let exp = vec![pe! {
        m: "foo_total",
        v: 1.0,
        t: Some(1700000000000),
        lset: ls!("__name__", "foo_total"),
        es: vec![ex(ls!("trace\\id\n\"1\"", "a\x00b\nc"), 1.0, 1700000000500)],
    }];
    require_entries(&exp, &got);
}

#[test]
fn test_open_metrics2_parse_ignore_native_histograms() {
    // native-only histogram emits count sum and +Inf bucket.
    {
        let input = "# TYPE req_seconds histogram\nreq_seconds{service=\"api\"} {count:12,sum:5.5,schema:0,zero_threshold:0.001,zero_count:2,positive_spans:[0:2],positive_buckets:[3,7]} 1700000000.0\n# EOF\n";
        let got = parse(input.as_bytes(), ParserOptions { ignore_native_histograms: true, ..Default::default() });
        let exp = vec![
            pe! { m: "req_seconds", typ: MetricType::Histogram },
            pe! {
                m: b"req_seconds_count\xffservice\xffapi",
                v: 12.0,
                t: Some(1700000000000),
                lset: ls!("__name__", "req_seconds_count", "service", "api"),
            },
            pe! {
                m: b"req_seconds_sum\xffservice\xffapi",
                v: 5.5,
                t: Some(1700000000000),
                lset: ls!("__name__", "req_seconds_sum", "service", "api"),
            },
            pe! {
                m: b"req_seconds_bucket\xffservice\xffapi\xffle\xff+Inf",
                v: 12.0,
                t: Some(1700000000000),
                lset: ls!("__name__", "req_seconds_bucket", "le", "+Inf", "service", "api"),
            },
        ];
        require_entries(&exp, &got);
    }

    // dual classic and native histogram emits classic series only.
    {
        let input = "# TYPE req_seconds histogram\nreq_seconds {count:12,sum:5.5,schema:0,zero_threshold:0.001,zero_count:2,positive_spans:[0:2],positive_buckets:[3,7],bucket:[1.0:3,2.0:7,+Inf:12]}\n# EOF\n";
        let got = parse(input.as_bytes(), ParserOptions { ignore_native_histograms: true, ..Default::default() });
        let exp = vec![
            pe! { m: "req_seconds", typ: MetricType::Histogram },
            pe! { m: "req_seconds_count", v: 12.0, lset: ls!("__name__", "req_seconds_count") },
            pe! { m: "req_seconds_sum", v: 5.5, lset: ls!("__name__", "req_seconds_sum") },
            pe! { m: b"req_seconds_bucket\xffle\xff1.0", v: 3.0, lset: ls!("__name__", "req_seconds_bucket", "le", "1.0") },
            pe! { m: b"req_seconds_bucket\xffle\xff2.0", v: 7.0, lset: ls!("__name__", "req_seconds_bucket", "le", "2.0") },
            pe! { m: b"req_seconds_bucket\xffle\xff+Inf", v: 12.0, lset: ls!("__name__", "req_seconds_bucket", "le", "+Inf") },
        ];
        require_entries(&exp, &got);
    }

    // native-only histogram with NHCB conversion emits single +Inf custom bucket.
    {
        let input = "# TYPE req_seconds histogram\nreq_seconds{service=\"api\"} {count:12,sum:5.5,schema:0,zero_threshold:0.001,zero_count:2,positive_spans:[0:2],positive_buckets:[3,7]} 1700000000.0\n# EOF\n";
        let got = parse(
            input.as_bytes(),
            ParserOptions { ignore_native_histograms: true, convert_classic_histograms_to_nhcb: true, ..Default::default() },
        );
        let exp = vec![
            pe! { m: "req_seconds", typ: MetricType::Histogram },
            pe! {
                m: r#"req_seconds{service="api"}"#,
                t: Some(1700000000000),
                lset: ls!("__name__", "req_seconds", "service", "api"),
                shs: Some(Histogram {
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    count: 12,
                    sum: 5.5,
                    positive_spans: spans(&[(0, 1)]),
                    positive_buckets: vec![12],
                    ..Default::default()
                }),
            },
        ];
        require_entries(&exp, &got);
    }

    // dual classic and native histogram with native ignored and with NHCB conversion emits NHCB.
    {
        let input = "# TYPE req_seconds histogram\nreq_seconds {count:12,sum:5.5,schema:0,zero_threshold:0.001,zero_count:2,positive_spans:[0:2],positive_buckets:[3,7],bucket:[1.0:3,2.0:7,+Inf:12]}\n# EOF\n";
        let got = parse(
            input.as_bytes(),
            ParserOptions { ignore_native_histograms: true, convert_classic_histograms_to_nhcb: true, ..Default::default() },
        );
        let exp = vec![
            pe! { m: "req_seconds", typ: MetricType::Histogram },
            pe! {
                m: "req_seconds",
                lset: ls!("__name__", "req_seconds"),
                shs: Some(Histogram {
                    schema: CUSTOM_BUCKETS_SCHEMA,
                    count: 12,
                    sum: 5.5,
                    positive_spans: spans(&[(0, 3)]),
                    positive_buckets: vec![3, 1, 1],
                    custom_values: Some(vec![1.0, 2.0].into()),
                    ..Default::default()
                }),
            },
        ];
        require_entries(&exp, &got);
    }
}

#[test]
fn test_open_metrics2_parse_spec_validation_errors() {
    let cases: &[(&str, &str, &str)] = &[
        ("empty quoted label name in sample", "foo{\"\"=\"val\"} 1.0\n# EOF\n", "label name must not be empty"),
        (
            "empty quoted label name in exemplar",
            "foo 1.0 # {\"\"=\"val\"} 1.0 1700000000.0\n# EOF\n",
            "label name must not be empty",
        ),
        (
            "info metric with non-empty unit (TYPE before UNIT)",
            "# TYPE build_info info\n# UNIT build_info seconds\nbuild_info{v=\"1\"} 1\n# EOF\n",
            "info metric \"build_info\" must have an empty unit",
        ),
        (
            "info metric with non-empty unit (UNIT before TYPE)",
            "# UNIT build_info seconds\n# TYPE build_info info\nbuild_info{v=\"1\"} 1\n# EOF\n",
            "info metric \"build_info\" must have an empty unit",
        ),
        (
            "stateset metric with non-empty unit (TYPE before UNIT)",
            "# TYPE flags stateset\n# UNIT flags seconds\nflags{flags=\"a\"} 1\n# EOF\n",
            "stateset metric \"flags\" must have an empty unit",
        ),
        (
            "stateset metric with non-empty unit (UNIT before TYPE)",
            "# UNIT flags seconds\n# TYPE flags stateset\nflags{flags=\"a\"} 1\n# EOF\n",
            "stateset metric \"flags\" must have an empty unit",
        ),
        (
            "info metric family name missing _info suffix",
            "# TYPE build info\nbuild{v=\"1\"} 1\n# EOF\n",
            "info metric family name \"build\" must end with _info",
        ),
        (
            "info metric sample value not 1 (zero)",
            "# TYPE build_info info\nbuild_info{v=\"1\"} 0\n# EOF\n",
            "info sample value must be 1",
        ),
        (
            "info metric sample value not 1 (two)",
            "# TYPE build_info info\nbuild_info{v=\"1\"} 2.0\n# EOF\n",
            "info sample value must be 1",
        ),
        ("carriage return in HELP line", "# HELP foo help text\r\nfoo 1.0\n# EOF\n", "unexpected carriage return"),
        ("carriage return in TYPE line", "# TYPE foo gauge\r\nfoo 1.0\n# EOF\n", "unexpected carriage return"),
        ("carriage return in UNIT line", "# UNIT foo seconds\r\nfoo 1.0\n# EOF\n", "unexpected carriage return"),
        ("carriage return in scalar sample line", "foo 1.0\r\n# EOF\n", "while parsing"),
        (
            "carriage return in composite sample line",
            "# TYPE foo histogram\nfoo {count:1,sum:1,bucket:[+Inf:1]}\r\n# EOF\n",
            "unexpected carriage return",
        ),
        ("carriage return in EOF line", "foo 1.0\n# EOF\r\n", "unexpected data after # EOF"),
    ];
    for &(name, input, want) in cases {
        eprintln!("case {name}");
        let err = first_error(input.as_bytes(), ParserOptions::default());
        assert!(!err.is_eof(), "{name}: got EOF");
        require_error_contains(&err, want);
    }
}

#[test]
fn test_open_metrics2_parse_carriage_return_in_help() {
    let input = "# HELP foo first\rsecond\nfoo 1\n# EOF\n";
    let got = parse(input.as_bytes(), ParserOptions::default());
    let exp = vec![
        pe! { m: "foo", help: "first\rsecond" },
        pe! { m: "foo", v: 1.0, lset: ls!("__name__", "foo") },
    ];
    require_entries(&exp, &got);
}
