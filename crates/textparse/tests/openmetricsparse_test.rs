//! Port of `model/textparse/openmetricsparse_test.go`.

mod common;

use common::{reset_val_and_lset, todo_detect_family_switch, type_and_unit_labels};
use model::exemplar::Exemplar;
use model::labels::Labels;
use model::metadata::MetricType;
use model::value::NORMAL_NAN;
use textparse::testing::{require_entries, test_parse, ParsedEntry};
use textparse::{Entry, Error, OpenMetricsOptions, OpenMetricsParser, Parser};

/// `exemplar.Exemplar{Labels: l, Value: v}`.
fn ex(l: Labels, v: f64) -> Exemplar {
    Exemplar { labels: l, value: v, ..Default::default() }
}

/// `exemplar.Exemplar{Labels: l, Value: v, HasTs: true, Ts: ts}`.
fn ex_ts(l: Labels, v: f64, ts: i64) -> Exemplar {
    Exemplar { labels: l, value: v, has_ts: true, ts }
}

/// `WithOMParserSTSeriesSkipped()`.
fn st_skipped() -> OpenMetricsOptions {
    OpenMetricsOptions { skip_st_series: true, ..Default::default() }
}

#[test]
fn test_open_metrics_parse() {
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
foo_total{le="c"} 21.0
foo_created{le="c"} 1520872621.123
foo_total{le="1"} 10.0
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
something_bucket{le="1"} 2
something_bucket{le="+Inf"} 18
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
foobar{quantile="0.99"} 150.1
# HELP nansum Summary with NaN value
# TYPE nansum summary
nansum_count 0
nansum_sum 0.0
nansum{quantile="0.95"} nan
nansum{quantile="0.99"} NaN"#,
    );

    input += "\n# HELP metric foo\x00bar";
    input += "\nnull_byte_metric{a=\"abc\x00\"} 1";
    input += "\n# EOF\n";

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
                m: "go_gc_duration_seconds",
                unit: "seconds",
            },
            pe! {
                m: r#"go_gc_duration_seconds{quantile="0"}"#,
                v: 4.9351e-05,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "go_gc_duration_seconds", "__type__", MetricType::Summary.as_str(), "__unit__", "seconds", "quantile", "0.0"),
                    ls!("__name__", "go_gc_duration_seconds", "quantile", "0.0"),
                ),
            },
            pe! {
                m: r#"go_gc_duration_seconds{quantile="0.25"}"#,
                v: 7.424100000000001e-05,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "go_gc_duration_seconds", "__type__", MetricType::Summary.as_str(), "__unit__", "seconds", "quantile", "0.25"),
                    ls!("__name__", "go_gc_duration_seconds", "quantile", "0.25"),
                ),
            },
            pe! {
                m: r#"go_gc_duration_seconds{quantile="0.5",a="b"}"#,
                v: 8.3835e-05,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "go_gc_duration_seconds", "__type__", MetricType::Summary.as_str(), "__unit__", "seconds", "quantile", "0.5", "a", "b"),
                    ls!("__name__", "go_gc_duration_seconds", "quantile", "0.5", "a", "b"),
                ),
            },
            pe! {
                m: "nohelp1",
                help: "",
            },
            pe! {
                m: "help2",
                help: "escape \\ \n \\ \" \\x chars",
            },
            pe! {
                m: "nounit",
                unit: "",
            },
            pe! {
                m: r#"go_gc_duration_seconds{quantile="1.0",a="b"}"#,
                v: 8.3835e-05,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "go_gc_duration_seconds", "__type__", MetricType::Summary.as_str(), "quantile", "1.0", "a", "b"),
                    ls!("__name__", "go_gc_duration_seconds", "quantile", "1.0", "a", "b"),
                ),
            },
            pe! {
                m: "go_gc_duration_seconds_count",
                v: 99.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "go_gc_duration_seconds_count", "__type__", MetricType::Summary.as_str()),
                    ls!("__name__", "go_gc_duration_seconds_count"),
                ),
            },
            pe! {
                m: r#"some:aggregate:rate5m{a_b="c"}"#,
                v: 1.0,
                lset: todo_detect_family_switch(type_and_unit_enabled, ls!("__name__", "some:aggregate:rate5m", "a_b", "c"), MetricType::Summary),
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
                m: "hh",
                typ: MetricType::Histogram,
            },
            pe! {
                m: r#"hh_bucket{le="+Inf"}"#,
                v: 1.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "hh_bucket", "__type__", MetricType::Histogram.as_str(), "le", "+Inf"),
                    ls!("__name__", "hh_bucket", "le", "+Inf"),
                ),
            },
            pe! {
                m: "gh",
                typ: MetricType::GaugeHistogram,
            },
            pe! {
                m: r#"gh_bucket{le="+Inf"}"#,
                v: 1.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "gh_bucket", "__type__", MetricType::GaugeHistogram.as_str(), "le", "+Inf"),
                    ls!("__name__", "gh_bucket", "le", "+Inf"),
                ),
            },
            pe! {
                m: "hhh",
                typ: MetricType::Histogram,
            },
            pe! {
                m: r#"hhh_bucket{le="+Inf"}"#,
                v: 1.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "hhh_bucket", "__type__", MetricType::Histogram.as_str(), "le", "+Inf"),
                    ls!("__name__", "hhh_bucket", "le", "+Inf"),
                ),
                es: vec![ex(ls!("id", "histogram-bucket-test"), 4.0)],
            },
            pe! {
                m: "hhh_count",
                v: 1.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "hhh_count", "__type__", MetricType::Histogram.as_str()),
                    ls!("__name__", "hhh_count"),
                ),
                es: vec![ex(ls!("id", "histogram-count-test"), 4.0)],
            },
            pe! {
                m: "ggh",
                typ: MetricType::GaugeHistogram,
            },
            pe! {
                m: r#"ggh_bucket{le="+Inf"}"#,
                v: 1.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "ggh_bucket", "__type__", MetricType::GaugeHistogram.as_str(), "le", "+Inf"),
                    ls!("__name__", "ggh_bucket", "le", "+Inf"),
                ),
                es: vec![ex_ts(ls!("id", "gaugehistogram-bucket-test", "xx", "yy"), 4.0, 123123)],
            },
            pe! {
                m: "ggh_count",
                v: 1.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "ggh_count", "__type__", MetricType::GaugeHistogram.as_str()),
                    ls!("__name__", "ggh_count"),
                ),
                es: vec![ex_ts(ls!("id", "gaugehistogram-count-test", "xx", "yy"), 4.0, 123123)],
            },
            pe! {
                m: "smr_seconds",
                typ: MetricType::Summary,
            },
            pe! {
                m: "smr_seconds_count",
                v: 2.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "smr_seconds_count", "__type__", MetricType::Summary.as_str()),
                    ls!("__name__", "smr_seconds_count"),
                ),
                es: vec![ex_ts(ls!("id", "summary-count-test"), 1.0, 123321)],
            },
            pe! {
                m: "smr_seconds_sum",
                v: 42.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "smr_seconds_sum", "__type__", MetricType::Summary.as_str()),
                    ls!("__name__", "smr_seconds_sum"),
                ),
                es: vec![ex_ts(ls!("id", "summary-sum-test"), 1.0, 123321)],
            },
            pe! {
                m: "ii",
                typ: MetricType::Info,
            },
            pe! {
                m: r#"ii{foo="bar"}"#,
                v: 1.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "ii", "__type__", MetricType::Info.as_str(), "foo", "bar"),
                    ls!("__name__", "ii", "foo", "bar"),
                ),
            },
            pe! {
                m: "ss",
                typ: MetricType::StateSet,
            },
            pe! {
                m: r#"ss{ss="foo"}"#,
                v: 1.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "ss", "__type__", MetricType::StateSet.as_str(), "ss", "foo"),
                    ls!("__name__", "ss", "ss", "foo"),
                ),
            },
            pe! {
                m: r#"ss{ss="bar"}"#,
                v: 0.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "ss", "__type__", MetricType::StateSet.as_str(), "ss", "bar"),
                    ls!("__name__", "ss", "ss", "bar"),
                ),
            },
            pe! {
                m: r#"ss{A="a"}"#,
                v: 0.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "ss", "__type__", MetricType::StateSet.as_str(), "A", "a"),
                    ls!("__name__", "ss", "A", "a"),
                ),
            },
            pe! {
                m: "un",
                typ: MetricType::Unknown,
            },
            pe! {
                m: "_metric_starting_with_underscore",
                v: 1.0,
                lset: ls!("__name__", "_metric_starting_with_underscore"),
            },
            pe! {
                m: "testmetric{_label_starting_with_underscore=\"foo\"}",
                v: 1.0,
                lset: ls!("__name__", "testmetric", "_label_starting_with_underscore", "foo"),
            },
            pe! {
                m: "testmetric{label=\"\\\"bar\\\"\"}",
                v: 1.0,
                lset: ls!("__name__", "testmetric", "label", r#""bar""#),
            },
            pe! {
                m: "foo",
                help: "Counter with and without labels to certify ST is parsed for both cases",
            },
            pe! {
                m: "foo",
                typ: MetricType::Counter,
            },
            pe! {
                m: "foo_total",
                v: 17.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "foo_total", "__type__", MetricType::Counter.as_str()),
                    ls!("__name__", "foo_total"),
                ),
                t: Some(1520879607789),
                es: vec![ex(ls!("id", "counter-test"), 5.0)],
                st: 1520872607123,
            },
            pe! {
                m: r#"foo_total{a="b"}"#,
                v: 17.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "foo_total", "__type__", MetricType::Counter.as_str(), "a", "b"),
                    ls!("__name__", "foo_total", "a", "b"),
                ),
                t: Some(1520879607789),
                es: vec![ex(ls!("id", "counter-test"), 5.0)],
                st: 1520872607123,
            },
            pe! {
                m: r#"foo_total{le="c"}"#,
                v: 21.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "foo_total", "__type__", MetricType::Counter.as_str(), "le", "c"),
                    ls!("__name__", "foo_total", "le", "c"),
                ),
                st: 1520872621123,
            },
            pe! {
                m: r#"foo_total{le="1"}"#,
                v: 10.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "foo_total", "__type__", MetricType::Counter.as_str(), "le", "1"),
                    ls!("__name__", "foo_total", "le", "1"),
                ),
            },
            pe! {
                m: "bar",
                help: "Summary with ST at the end, making sure we find ST even if it's multiple lines a far",
            },
            pe! {
                m: "bar",
                typ: MetricType::Summary,
            },
            pe! {
                m: "bar_count",
                v: 17.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "bar_count", "__type__", MetricType::Summary.as_str()),
                    ls!("__name__", "bar_count"),
                ),
                st: 1520872608124,
            },
            pe! {
                m: "bar_sum",
                v: 324789.3,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "bar_sum", "__type__", MetricType::Summary.as_str()),
                    ls!("__name__", "bar_sum"),
                ),
                st: 1520872608124,
            },
            pe! {
                m: r#"bar{quantile="0.95"}"#,
                v: 123.7,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "bar", "__type__", MetricType::Summary.as_str(), "quantile", "0.95"),
                    ls!("__name__", "bar", "quantile", "0.95"),
                ),
                st: 1520872608124,
            },
            pe! {
                m: r#"bar{quantile="0.99"}"#,
                v: 150.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "bar", "__type__", MetricType::Summary.as_str(), "quantile", "0.99"),
                    ls!("__name__", "bar", "quantile", "0.99"),
                ),
                st: 1520872608124,
            },
            pe! {
                m: "baz",
                help: "Histogram with the same objective as above's summary",
            },
            pe! {
                m: "baz",
                typ: MetricType::Histogram,
            },
            pe! {
                m: r#"baz_bucket{le="0.0"}"#,
                v: 0.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "baz_bucket", "__type__", MetricType::Histogram.as_str(), "le", "0.0"),
                    ls!("__name__", "baz_bucket", "le", "0.0"),
                ),
                st: 1520872609125,
            },
            pe! {
                m: r#"baz_bucket{le="+Inf"}"#,
                v: 17.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "baz_bucket", "__type__", MetricType::Histogram.as_str(), "le", "+Inf"),
                    ls!("__name__", "baz_bucket", "le", "+Inf"),
                ),
                st: 1520872609125,
            },
            pe! {
                m: "baz_count",
                v: 17.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "baz_count", "__type__", MetricType::Histogram.as_str()),
                    ls!("__name__", "baz_count"),
                ),
                st: 1520872609125,
            },
            pe! {
                m: "baz_sum",
                v: 324789.3,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "baz_sum", "__type__", MetricType::Histogram.as_str()),
                    ls!("__name__", "baz_sum"),
                ),
                st: 1520872609125,
            },
            pe! {
                m: "fizz_created",
                help: "Gauge which shouldn't be parsed as ST",
            },
            pe! {
                m: "fizz_created",
                typ: MetricType::Gauge,
            },
            pe! {
                m: "fizz_created",
                v: 17.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "fizz_created", "__type__", MetricType::Gauge.as_str()),
                    ls!("__name__", "fizz_created"),
                ),
            },
            pe! {
                m: "something",
                help: "Histogram with _created between buckets and summary",
            },
            pe! {
                m: "something",
                typ: MetricType::Histogram,
            },
            pe! {
                m: "something_count",
                v: 18.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "something_count", "__type__", MetricType::Histogram.as_str()),
                    ls!("__name__", "something_count"),
                ),
                st: 1520430001000,
            },
            pe! {
                m: "something_sum",
                v: 324789.4,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "something_sum", "__type__", MetricType::Histogram.as_str()),
                    ls!("__name__", "something_sum"),
                ),
                st: 1520430001000,
            },
            pe! {
                m: r#"something_bucket{le="0.0"}"#,
                v: 1.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "something_bucket", "__type__", MetricType::Histogram.as_str(), "le", "0.0"),
                    ls!("__name__", "something_bucket", "le", "0.0"),
                ),
                st: 1520430001000,
            },
            pe! {
                m: r#"something_bucket{le="1"}"#,
                v: 2.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "something_bucket", "__type__", MetricType::Histogram.as_str(), "le", "1.0"),
                    ls!("__name__", "something_bucket", "le", "1.0"),
                ),
                st: 1520430001000,
            },
            pe! {
                m: r#"something_bucket{le="+Inf"}"#,
                v: 18.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "something_bucket", "__type__", MetricType::Histogram.as_str(), "le", "+Inf"),
                    ls!("__name__", "something_bucket", "le", "+Inf"),
                ),
                st: 1520430001000,
            },
            pe! {
                m: "yum",
                help: "Summary with _created between sum and quantiles",
            },
            pe! {
                m: "yum",
                typ: MetricType::Summary,
            },
            pe! {
                m: "yum_count",
                v: 20.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "yum_count", "__type__", MetricType::Summary.as_str()),
                    ls!("__name__", "yum_count"),
                ),
                st: 1520430003000,
            },
            pe! {
                m: "yum_sum",
                v: 324789.5,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "yum_sum", "__type__", MetricType::Summary.as_str()),
                    ls!("__name__", "yum_sum"),
                ),
                st: 1520430003000,
            },
            pe! {
                m: r#"yum{quantile="0.95"}"#,
                v: 123.7,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "yum", "__type__", MetricType::Summary.as_str(), "quantile", "0.95"),
                    ls!("__name__", "yum", "quantile", "0.95"),
                ),
                st: 1520430003000,
            },
            pe! {
                m: r#"yum{quantile="0.99"}"#,
                v: 150.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "yum", "__type__", MetricType::Summary.as_str(), "quantile", "0.99"),
                    ls!("__name__", "yum", "quantile", "0.99"),
                ),
                st: 1520430003000,
            },
            pe! {
                m: "foobar",
                help: "Summary with _created as the first line",
            },
            pe! {
                m: "foobar",
                typ: MetricType::Summary,
            },
            pe! {
                m: "foobar_count",
                v: 21.0,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "foobar_count", "__type__", MetricType::Summary.as_str()),
                    ls!("__name__", "foobar_count"),
                ),
                st: 1520430004000,
            },
            pe! {
                m: "foobar_sum",
                v: 324789.6,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "foobar_sum", "__type__", MetricType::Summary.as_str()),
                    ls!("__name__", "foobar_sum"),
                ),
                st: 1520430004000,
            },
            pe! {
                m: r#"foobar{quantile="0.95"}"#,
                v: 123.8,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "foobar", "__type__", MetricType::Summary.as_str(), "quantile", "0.95"),
                    ls!("__name__", "foobar", "quantile", "0.95"),
                ),
                st: 1520430004000,
            },
            pe! {
                m: r#"foobar{quantile="0.99"}"#,
                v: 150.1,
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "foobar", "__type__", MetricType::Summary.as_str(), "quantile", "0.99"),
                    ls!("__name__", "foobar", "quantile", "0.99"),
                ),
                st: 1520430004000,
            },
            pe! {
                m: "nansum",
                help: "Summary with NaN value",
            },
            pe! {
                m: "nansum",
                typ: MetricType::Summary,
            },
            pe! {
                m: "nansum_count",
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "nansum_count", "__type__", MetricType::Summary.as_str()),
                    ls!("__name__", "nansum_count"),
                ),
            },
            pe! {
                m: "nansum_sum",
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "nansum_sum", "__type__", MetricType::Summary.as_str()),
                    ls!("__name__", "nansum_sum"),
                ),
            },
            pe! {
                m: r#"nansum{quantile="0.95"}"#,
                v: f64::from_bits(NORMAL_NAN),
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "nansum", "__type__", MetricType::Summary.as_str(), "quantile", "0.95"),
                    ls!("__name__", "nansum", "quantile", "0.95"),
                ),
            },
            pe! {
                m: r#"nansum{quantile="0.99"}"#,
                v: f64::from_bits(NORMAL_NAN),
                lset: type_and_unit_labels(
                    type_and_unit_enabled,
                    ls!("__name__", "nansum", "__type__", MetricType::Summary.as_str(), "quantile", "0.99"),
                    ls!("__name__", "nansum", "quantile", "0.99"),
                ),
            },
            pe! {
                m: "metric",
                help: "foo\x00bar",
            },
            pe! {
                m: "null_byte_metric{a=\"abc\x00\"}",
                v: 1.0,
                lset: todo_detect_family_switch(type_and_unit_enabled, ls!("__name__", "null_byte_metric", "a", "abc\x00"), MetricType::Summary),
            },
        ];
        let opts = OpenMetricsOptions { skip_st_series: true, enable_type_and_unit_labels: type_and_unit_enabled };
        let mut p = OpenMetricsParser::new(input.as_bytes(), opts);
        let got = test_parse(&mut p);
        require_entries(&exp, &got);
    }
}

#[test]
fn test_open_metrics_parse_utf8() {
    let mut input = String::from(
        r#"# HELP "go.gc_duration_seconds" A summary of the GC invocation durations.
# TYPE "go.gc_duration_seconds" summary
# UNIT "go.gc_duration_seconds" seconds
{"go.gc_duration_seconds",quantile="0"} 4.9351e-05
{"go.gc_duration_seconds",quantile="0.25"} 7.424100000000001e-05
{"go.gc_duration_seconds_created"} 1520872607.123
{"go.gc_duration_seconds",quantile="0.5",a="b"} 8.3835e-05
{"http.status",q="0.9",a="b"} 8.3835e-05
{"http.status",q="0.9",a="b"} 8.3835e-05
{q="0.9","http.status",a="b"} 8.3835e-05
{"go.gc_duration_seconds_sum"} 0.004304266
{"Heizölrückstoßabdämpfung 10€ metric with \"interesting\" {character\nchoices}","strange©™\n'quoted' \"name\""="6"} 10.0
quotedexemplar_count 1 # {"id.thing"="histogram-count-test"} 4
quotedexemplar2_count 1 # {"id.thing"="histogram-count-test",other="hello"} 4
"#,
    );

    input += "# EOF\n";

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
            m: "go.gc_duration_seconds",
            unit: "seconds",
        },
        pe! {
            m: r#"{"go.gc_duration_seconds",quantile="0"}"#,
            v: 4.9351e-05,
            lset: ls!("__name__", "go.gc_duration_seconds", "quantile", "0.0"),
            st: 1520872607123,
        },
        pe! {
            m: r#"{"go.gc_duration_seconds",quantile="0.25"}"#,
            v: 7.424100000000001e-05,
            lset: ls!("__name__", "go.gc_duration_seconds", "quantile", "0.25"),
            st: 1520872607123,
        },
        pe! {
            m: r#"{"go.gc_duration_seconds",quantile="0.5",a="b"}"#,
            v: 8.3835e-05,
            lset: ls!("__name__", "go.gc_duration_seconds", "quantile", "0.5", "a", "b"),
        },
        pe! {
            m: r#"{"http.status",q="0.9",a="b"}"#,
            v: 8.3835e-05,
            lset: ls!("__name__", "http.status", "q", "0.9", "a", "b"),
        },
        pe! {
            m: r#"{"http.status",q="0.9",a="b"}"#,
            v: 8.3835e-05,
            lset: ls!("__name__", "http.status", "q", "0.9", "a", "b"),
        },
        pe! {
            m: r#"{q="0.9","http.status",a="b"}"#,
            v: 8.3835e-05,
            lset: ls!("__name__", "http.status", "q", "0.9", "a", "b"),
        },
        pe! {
            m: r#"{"go.gc_duration_seconds_sum"}"#,
            v: 0.004304266,
            lset: ls!("__name__", "go.gc_duration_seconds_sum"),
        },
        pe! {
            m: r#"{"Heizölrückstoßabdämpfung 10€ metric with \"interesting\" {character\nchoices}","strange©™\n'quoted' \"name\""="6"}"#,
            v: 10.0,
            lset: ls!("__name__", r#"Heizölrückstoßabdämpfung 10€ metric with "interesting" {character
choices}"#, "strange©™\n'quoted' \"name\"", "6"),
        },
        pe! {
            m: "quotedexemplar_count",
            v: 1.0,
            lset: ls!("__name__", "quotedexemplar_count"),
            es: vec![ex(ls!("id.thing", "histogram-count-test"), 4.0)],
        },
        pe! {
            m: "quotedexemplar2_count",
            v: 1.0,
            lset: ls!("__name__", "quotedexemplar2_count"),
            es: vec![ex(ls!("id.thing", "histogram-count-test", "other", "hello"), 4.0)],
        },
    ];

    let mut p = OpenMetricsParser::new(input.as_bytes(), st_skipped());
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

struct Case {
    input: &'static [u8],
    err: &'static str,
}

#[test]
fn test_open_metrics_parse_errors() {
    let cases = [
        // Happy cases. EOF is returned by the parser at the end of valid
        // data.
        Case {
            input: b"# EOF",
            err: "EOF",
        },
        Case {
            input: b"# EOF\n",
            err: "EOF",
        },
        // Unhappy cases.
        Case {
            input: b"",
            err: "data does not end with # EOF",
        },
        Case {
            input: b"\n",
            err: "expected a valid start token, got \"\\n\" (\"INVALID\") while parsing: \"\\n\"",
        },
        Case {
            input: b"metric",
            err: "expected value after metric, got \"metric\" (\"EOF\") while parsing: \"metric\"",
        },
        Case {
            input: b"metric 1",
            err: "data does not end with # EOF",
        },
        Case {
            input: b"metric 1\n",
            err: "data does not end with # EOF",
        },
        Case {
            input: b"metric_total 1 # {aa=\"bb\"} 4",
            err: "data does not end with # EOF",
        },
        Case {
            input: b"a\n#EOF\n",
            err: "expected value after metric, got \"\\n\" (\"INVALID\") while parsing: \"a\\n\"",
        },
        Case {
            input: b"\n\n#EOF\n",
            err: "expected a valid start token, got \"\\n\" (\"INVALID\") while parsing: \"\\n\"",
        },
        Case {
            input: b" a 1\n#EOF\n",
            err: "expected a valid start token, got \" \" (\"INVALID\") while parsing: \" \"",
        },
        Case {
            input: b"9\n#EOF\n",
            err: "expected a valid start token, got \"9\" (\"INVALID\") while parsing: \"9\"",
        },
        Case {
            input: b"# TYPE u untyped\n#EOF\n",
            err: "invalid metric type \"untyped\"",
        },
        Case {
            input: b"# TYPE c counter \n#EOF\n",
            err: "invalid metric type \"counter \"",
        },
        Case {
            input: b"#  TYPE c counter\n#EOF\n",
            err: "expected a valid start token, got \"#  \" (\"INVALID\") while parsing: \"#  \"",
        },
        Case {
            input: b"# TYPE \n#EOF\n",
            err: "expected metric name after TYPE, got \"\\n\" (\"INVALID\") while parsing: \"# TYPE \\n\"",
        },
        Case {
            input: b"# TYPE m\n#EOF\n",
            err: "expected text in TYPE",
        },
        Case {
            input: b"# UNIT metric suffix\n#EOF\n",
            err: "unit \"suffix\" not a suffix of metric \"metric\"",
        },
        Case {
            input: b"# UNIT metricsuffix suffix\n#EOF\n",
            err: "unit \"suffix\" not a suffix of metric \"metricsuffix\"",
        },
        Case {
            input: b"# UNIT m suffix\n#EOF\n",
            err: "unit \"suffix\" not a suffix of metric \"m\"",
        },
        Case {
            input: b"# UNIT \n#EOF\n",
            err: "expected metric name after UNIT, got \"\\n\" (\"INVALID\") while parsing: \"# UNIT \\n\"",
        },
        Case {
            input: b"# UNIT m\n#EOF\n",
            err: "expected text in UNIT",
        },
        Case {
            input: b"# HELP \n#EOF\n",
            err: "expected metric name after HELP, got \"\\n\" (\"INVALID\") while parsing: \"# HELP \\n\"",
        },
        Case {
            input: b"# HELP m\n#EOF\n",
            err: "expected text in HELP",
        },
        Case {
            input: b"a\t1\n#EOF\n",
            err: "expected value after metric, got \"\\t\" (\"INVALID\") while parsing: \"a\\t\"",
        },
        Case {
            input: b"a 1\t2\n#EOF\n",
            err: "strconv.ParseFloat: parsing \"1\\t2\": invalid syntax while parsing: \"a 1\\t2\"",
        },
        Case {
            input: b"a 1 2 \n#EOF\n",
            err: "expected next entry after timestamp, got \" \\n\" (\"INVALID\") while parsing: \"a 1 2 \\n\"",
        },
        Case {
            input: b"a 1 2 #\n#EOF\n",
            err: "expected next entry after timestamp, got \" #\\n\" (\"TIMESTAMP\") while parsing: \"a 1 2 #\\n\"",
        },
        Case {
            input: b"a 1 1z\n#EOF\n",
            err: "strconv.ParseFloat: parsing \"1z\": invalid syntax while parsing: \"a 1 1z\"",
        },
        Case {
            input: b" # EOF\n",
            err: "expected a valid start token, got \" \" (\"INVALID\") while parsing: \" \"",
        },
        Case {
            input: b"# EOF\na 1",
            err: "unexpected data after # EOF",
        },
        Case {
            input: b"# EOF\n\n",
            err: "unexpected data after # EOF",
        },
        Case {
            input: b"# EOFa 1",
            err: "unexpected data after # EOF",
        },
        Case {
            input: b"#\tTYPE c counter\n",
            err: "expected a valid start token, got \"#\\t\" (\"INVALID\") while parsing: \"#\\t\"",
        },
        Case {
            input: b"# TYPE c  counter\n",
            err: "invalid metric type \" counter\"",
        },
        Case {
            input: b"a 1 1 1\n# EOF\n",
            err: "expected next entry after timestamp, got \" 1\\n\" (\"TIMESTAMP\") while parsing: \"a 1 1 1\\n\"",
        },
        Case {
            input: b"a{b='c'} 1\n# EOF\n",
            err: "expected label value, got \"'\" (\"INVALID\") while parsing: \"a{b='\"",
        },
        Case {
            input: b"a{,b=\"c\"} 1\n# EOF\n",
            err: "expected label name, got \",b\" (\"COMMA\") while parsing: \"a{,b\"",
        },
        Case {
            input: b"a{b=\"c\"d=\"e\"} 1\n# EOF\n",
            err: "expected comma or brace close, got \"d=\" (\"LNAME\") while parsing: \"a{b=\\\"c\\\"d=\"",
        },
        Case {
            input: b"a{b=\"c\",,d=\"e\"} 1\n# EOF\n",
            err: "expected label name, got \",d\" (\"COMMA\") while parsing: \"a{b=\\\"c\\\",,d\"",
        },
        Case {
            input: b"a{b=\n# EOF\n",
            err: "expected label value, got \"\\n\" (\"INVALID\") while parsing: \"a{b=\\n\"",
        },
        Case {
            input: b"a{\xff=\"foo\"} 1\n# EOF\n",
            err: "expected label name, got \"\\xff\" (\"INVALID\") while parsing: \"a{\\xff\"",
        },
        Case {
            input: b"a{b=\"\xff\"} 1\n# EOF\n",
            err: "invalid UTF-8 label value: \"\\\"\\xff\\\"\"",
        },
        Case {
            input: br#"{"a","b = "c"}
# EOF
"#,
            err: "expected equal, got \"c\\\"\" (\"LNAME\") while parsing: \"{\\\"a\\\",\\\"b = \\\"c\\\"\"",
        },
        Case {
            input: br#"{"a",b\nc="d"} 1
# EOF
"#,
            err: "expected equal, got \"\\\\\" (\"INVALID\") while parsing: \"{\\\"a\\\",b\\\\\"",
        },
        Case {
            input: b"a true\n",
            err: "strconv.ParseFloat: parsing \"true\": invalid syntax while parsing: \"a true\"",
        },
        Case {
            input: b"something_weird{problem=\"\n# EOF\n",
            err: "expected label value, got \"\\\"\\n\" (\"INVALID\") while parsing: \"something_weird{problem=\\\"\\n\"",
        },
        Case {
            input: b"empty_label_name{=\"\"} 0\n# EOF\n",
            err: "expected label name, got \"=\\\"\" (\"EQUAL\") while parsing: \"empty_label_name{=\\\"\"",
        },
        Case {
            input: b"{A}0\n# EOF\n",
            err: "expected equal, got \"}0\" (\"BCLOSE\") while parsing: \"{A}0\"",
        },
        Case {
            input: b"foo 1_2\n\n# EOF\n",
            err: "unsupported character in float while parsing: \"foo 1_2\"",
        },
        Case {
            input: b"foo 0x1p-3\n\n# EOF\n",
            err: "unsupported character in float while parsing: \"foo 0x1p-3\"",
        },
        Case {
            input: b"foo 0x1P-3\n\n# EOF\n",
            err: "unsupported character in float while parsing: \"foo 0x1P-3\"",
        },
        Case {
            input: b"foo 0 1_2\n\n# EOF\n",
            err: "unsupported character in float while parsing: \"foo 0 1_2\"",
        },
        Case {
            input: b"custom_metric_total 1 # {aa=bb}\n# EOF\n",
            err: "expected label value, got \"b\" (\"INVALID\") while parsing: \"custom_metric_total 1 # {aa=b\"",
        },
        Case {
            input: b"custom_metric_total 1 # {aa=\"bb\"}\n# EOF\n",
            err: "expected value after exemplar labels, got \"\\n\" (\"INVALID\") while parsing: \"custom_metric_total 1 # {aa=\\\"bb\\\"}\\n\"",
        },
        Case {
            input: br#"custom_metric_total 1 # {aa="bb"}"#,
            err: "expected value after exemplar labels, got \"}\" (\"EOF\") while parsing: \"custom_metric_total 1 # {aa=\\\"bb\\\"}\"",
        },
        Case {
            input: br#"custom_metric_total 1 # {bb}"#,
            err: "expected equal, got \"}\" (\"BCLOSE\") while parsing: \"custom_metric_total 1 # {bb}\"",
        },
        Case {
            input: br#"custom_metric_total 1 # {bb, a="dd"}"#,
            err: "expected equal, got \", \" (\"COMMA\") while parsing: \"custom_metric_total 1 # {bb, \"",
        },
        Case {
            input: br#"custom_metric_total 1 # {aa="bb",,cc="dd"} 1"#,
            err: "expected label name, got \",c\" (\"COMMA\") while parsing: \"custom_metric_total 1 # {aa=\\\"bb\\\",,c\"",
        },
        Case {
            input: br#"custom_metric_total 1 # {aa="bb"} 1_2"#,
            err: "unsupported character in float while parsing: \"custom_metric_total 1 # {aa=\\\"bb\\\"} 1_2\"",
        },
        Case {
            input: br#"custom_metric_total 1 # {aa="bb"} 0x1p-3"#,
            err: "unsupported character in float while parsing: \"custom_metric_total 1 # {aa=\\\"bb\\\"} 0x1p-3\"",
        },
        Case {
            input: br#"custom_metric_total 1 # {aa="bb"} true"#,
            err: "strconv.ParseFloat: parsing \"true\": invalid syntax while parsing: \"custom_metric_total 1 # {aa=\\\"bb\\\"} true\"",
        },
        Case {
            input: br#"custom_metric_total 1 # {aa="bb",cc=}"#,
            err: "expected label value, got \"}\" (\"INVALID\") while parsing: \"custom_metric_total 1 # {aa=\\\"bb\\\",cc=}\"",
        },
        Case {
            // Go: a raw string with the escape `\xff` as four literal characters.
            input: br#"custom_metric_total 1 # {aa=\"\xff\"} 9.0"#,
            err: "expected label value, got \"\\\\\" (\"INVALID\") while parsing: \"custom_metric_total 1 # {aa=\\\\\"",
        },
        Case {
            input: br#"{b="c",} 1"#,
            err: "metric name not set while parsing: \"{b=\\\"c\\\",} 1\"",
        },
        Case {
            input: b"a 1 NaN",
            err: "invalid timestamp NaN",
        },
        Case {
            input: b"a 1 -Inf",
            err: "invalid timestamp -Inf",
        },
        Case {
            input: b"a 1 Inf",
            err: "invalid timestamp +Inf",
        },
        Case {
            input: b"# TYPE hhh histogram\nhhh_bucket{le=\"+Inf\"} 1 # {aa=\"bb\"} 4 NaN",
            err: "invalid exemplar timestamp NaN",
        },
        Case {
            input: b"# TYPE hhh histogram\nhhh_bucket{le=\"+Inf\"} 1 # {aa=\"bb\"} 4 -Inf",
            err: "invalid exemplar timestamp -Inf",
        },
    ];

    for (i, c) in cases.iter().enumerate() {
        let mut p = OpenMetricsParser::new(c.input, st_skipped());
        let err = first_error(&mut p);
        assert_eq!(c.err, err.to_string(), "test {i}: {}", String::from_utf8_lossy(c.input));
    }
}

#[test]
fn test_open_metrics_parse_bare_identifier_in_braces() {
    // Go: require.NotPanics. A panic fails the Rust test.
    let mut p = OpenMetricsParser::new(b"{A} 0\n# EOF\n", st_skipped());
    while let Ok(et) = p.next() {
        if et == Entry::Series {
            let mut lset = Labels::default();
            p.labels(&mut lset);
        }
    }
}

#[test]
fn test_om_null_byte_handling() {
    let cases = [
        Case {
            input: b"null_byte_metric{a=\"abc\x00\"} 1\n# EOF\n",
            err: "",
        },
        Case {
            input: b"a{b=\"\x00ss\"} 1\n# EOF\n",
            err: "",
        },
        Case {
            input: b"a{b=\"\x00\"} 1\n# EOF\n",
            err: "",
        },
        Case {
            input: b"a{b=\"\x00\"} 1\n# EOF",
            err: "",
        },
        Case {
            input: b"a{b=\x00\"ssss\"} 1\n# EOF\n",
            err: "expected label value, got \"\\x00\" (\"INVALID\") while parsing: \"a{b=\\x00\"",
        },
        Case {
            input: b"a{b=\"\x00",
            err: "expected label value, got \"\\\"\\x00\" (\"INVALID\") while parsing: \"a{b=\\\"\\x00\"",
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
            input: b"#",
            err: "expected a valid start token, got \"#\" (\"INVALID\") while parsing: \"#\"",
        },
        Case {
            input: b"# H",
            err: "expected a valid start token, got \"# H\" (\"INVALID\") while parsing: \"# H\"",
        },
        Case {
            input: b"custom_metric_total 1 # {b=\x00\"ssss\"} 1\n",
            err: "expected label value, got \"\\x00\" (\"INVALID\") while parsing: \"custom_metric_total 1 # {b=\\x00\"",
        },
        Case {
            input: b"custom_metric_total 1 # {b=\"\x00ss\"} 1\n",
            err: "expected label value, got \"\\\"\\x00\" (\"INVALID\") while parsing: \"custom_metric_total 1 # {b=\\\"\\x00\"",
        },
    ];

    for (i, c) in cases.iter().enumerate() {
        let mut p = OpenMetricsParser::new(c.input, st_skipped());
        let err = first_error(&mut p);

        if c.err.is_empty() {
            assert!(err.is_eof(), "test {i}: {err}");
            continue;
        }

        assert_eq!(err.to_string(), c.err, "test {i}");
    }
}

/// Tests known failure edge cases, we know does not work due current OM spec limitations or clients with broken
/// OM format.
// TODO(maniktherana): Make sure OM 1.1/2.0 pass ST via metadata or exemplar-like to avoid this.
#[test]
fn test_st_parse_failures() {
    struct TCase {
        name: &'static str,
        input: &'static str,
        expected: Vec<ParsedEntry>,
    }
    let tcases = vec![
        TCase {
            name: "_created line is a first one",
            input: r#"# HELP thing histogram with _created as first line
# TYPE thing histogram
thing_created 1520872607.123
thing_count 17
thing_sum 324789.3
thing_bucket{le="0.0"} 0
thing_bucket{le="+Inf"} 17
# HELP thing_c counter with _created as first line
# TYPE thing_c counter
thing_c_created 1520872607.123
thing_c_total 14123.232
# EOF
"#,
            expected: vec![
                pe! {
                    m: "thing",
                    help: "histogram with _created as first line",
                },
                pe! {
                    m: "thing",
                    typ: MetricType::Histogram,
                },
                pe! {
                    m: "thing_count",
                    st: 0, // Should be int64p(1520872607123).
                },
                pe! {
                    m: "thing_sum",
                    st: 0, // Should be int64p(1520872607123).
                },
                pe! {
                    m: r#"thing_bucket{le="0.0"}"#,
                    st: 0, // Should be int64p(1520872607123).
                },
                pe! {
                    m: r#"thing_bucket{le="+Inf"}"#,
                    st: 0, // Should be int64p(1520872607123),
                },
                pe! {
                    m: "thing_c",
                    help: "counter with _created as first line",
                },
                pe! {
                    m: "thing_c",
                    typ: MetricType::Counter,
                },
                pe! {
                    m: "thing_c_total",
                    st: 0, // Should be int64p(1520872607123).
                },
            ],
        },
        TCase {
            // TODO(bwplotka): Kind of correct bevaviour? If yes, let's move to the OK tests above.
            name: "maybe counter with no meta",
            input: r#"foo_total 17.0
foo_created 1520872607.123
foo_total{a="b"} 17.0
foo_created{a="b"} 1520872608.123
# EOF
"#,
            expected: vec![
                pe! {
                    m: "foo_total",
                },
                pe! {
                    m: "foo_created",
                },
                pe! {
                    m: r#"foo_total{a="b"}"#,
                },
                pe! {
                    m: r#"foo_created{a="b"}"#,
                },
            ],
        },
    ];
    for tcase in tcases {
        let mut p = OpenMetricsParser::new(tcase.input.as_bytes(), st_skipped());
        let mut got = test_parse(&mut p);
        reset_val_and_lset(&mut got); // Keep this test focused on metric, basic entries and ST only.
        eprintln!("case={}", tcase.name);
        require_entries(&tcase.expected, &got);
    }
}
