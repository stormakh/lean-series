//! Port of upstream/template/template_test.go.

use std::sync::Arc;

use model::histogram::FloatHistogram;
use model::labels;
use promql::{Sample, Vector};
use template::{Expander, GoUrl, IntKind, QueryFunc, Type, UintKind, Value, float_to_time, new_sample};

struct Scenario {
    text: &'static str,
    output: String,
    input: Value,
    options: Option<Vec<String>>,
    query_result: Vector,
    should_fail: bool,
    html: bool,
    error_msg: &'static str,
}

impl Default for Scenario {
    fn default() -> Scenario {
        Scenario {
            text: "",
            output: String::new(),
            input: Value::Invalid,
            options: None,
            query_result: Vector::new(),
            should_fail: false,
            html: false,
            error_msg: "",
        }
    }
}

fn hist(count: f64, sum: f64) -> FloatHistogram {
    FloatHistogram { count, sum, ..Default::default() }
}

fn smpl(lbls: &[&str], f: f64) -> Sample {
    Sample { metric: labels::from_strings(lbls), t: 0, f, ..Default::default() }
}

fn floats(v: &[f64]) -> Value {
    Value::slice(Type::Float64, v.iter().map(|&f| Value::Float(f)).collect())
}

fn strings(v: &[&str]) -> Value {
    Value::slice(Type::String(template::StrKind::String), v.iter().map(|&s| Value::string(s)).collect())
}

fn ints(v: &[i64], k: IntKind) -> Value {
    Value::slice(Type::Int(k), v.iter().map(|&i| Value::Int(i, k)).collect())
}

fn uints(v: &[u64], k: UintKind) -> Value {
    Value::slice(Type::Uint(k), v.iter().map(|&u| Value::Uint(u, k)).collect())
}

fn test_template_expansion_scenarios(scenarios: Vec<Scenario>) {
    let ext_url = yaml::url::parse("http://testhost:9090/path/prefix").expect("url");
    let ext_url: GoUrl = ext_url;
    for s in scenarios {
        let qr = s.query_result.clone();
        let query_func: QueryFunc = Arc::new(move |_, _, _| Ok(qr.clone()));
        let expander = Expander::new(
            storage::Context::default(),
            s.text,
            "test",
            s.input.clone(),
            1353755652000,
            Some(query_func),
            Some(&ext_url),
            s.options.as_deref(),
        );
        let result = if s.html { expander.expand_html(&[]) } else { expander.expand() };
        if s.should_fail {
            match result {
                Ok(r) => panic!("{}: expected error, got {:?}", s.text, r),
                Err(e) => assert_eq!(e, s.error_msg, "{}", s.text),
            }
            continue;
        }
        match result {
            Ok(r) => assert_eq!(r, s.output, "{}", s.text),
            Err(e) => panic!("{}: unexpected error {}", s.text, e),
        }
    }
}

#[test]
fn test_template_expansion() {
    let h = hist(3.0, 10.0);
    let metric_a = |f: f64| smpl(&["__name__", "metric", "instance", "a"], f);
    test_template_expansion_scenarios(vec![
        // No template.
        Scenario { text: "plain text", output: "plain text".into(), ..Default::default() },
        // Simple value.
        Scenario { text: "{{ 1 }}", output: "1".into(), ..Default::default() },
        // Native histogram value.
        Scenario {
            text: "{{ . | value }}",
            input: new_sample(Vec::<(String, String)>::new(), Value::ptr(Value::Histogram(Arc::new(h.clone())))),
            output: h.string(),
            ..Default::default()
        },
        // Non-ASCII space (not allowed in text/template).
        Scenario {
            text: "{{\u{a0}}}",
            should_fail: true,
            error_msg: "error parsing template test: template: test:1: unrecognized character in action: U+00A0",
            ..Default::default()
        },
        // HTML escaping.
        Scenario { text: "{{ \"<b>\" }}", output: "&lt;b&gt;".into(), html: true, ..Default::default() },
        // Disabling HTML escaping.
        Scenario { text: "{{ \"<b>\" | safeHtml }}", output: "<b>".into(), html: true, ..Default::default() },
        // HTML escaping doesn't apply to non-html.
        Scenario { text: "{{ \"<b>\" }}", output: "<b>".into(), ..Default::default() },
        // Pass multiple arguments to templates.
        Scenario {
            text: "{{define \"x\"}}{{.arg0}} {{.arg1}}{{end}}{{template \"x\" (args 1 \"2\")}}",
            output: "1 2".into(),
            ..Default::default()
        },
        Scenario {
            text: "{{ query \"1.5\" | first | value }}",
            output: "1.5".into(),
            query_result: vec![Sample { t: 0, f: 1.5, ..Default::default() }],
            ..Default::default()
        },
        // Get value from query.
        Scenario {
            text: "{{ query \"metric{instance='a'}\" | first | value }}",
            query_result: vec![metric_a(11.0)],
            output: "11".into(),
            ..Default::default()
        },
        // Get value of a native histogram from query.
        Scenario {
            text: "{{ query \"metric{instance='a'}\" | first | value }}",
            query_result: vec![Sample {
                metric: labels::from_strings(&["__name__", "metric", "instance", "a"]),
                t: 0,
                h: Some(Arc::new(h.clone())),
                ..Default::default()
            }],
            output: h.string(),
            ..Default::default()
        },
        // Get label from query.
        Scenario {
            text: "{{ query \"metric{instance='a'}\" | first | label \"instance\" }}",
            query_result: vec![metric_a(11.0)],
            output: "a".into(),
            ..Default::default()
        },
        // Get label "__value__" from query.
        Scenario {
            text: "{{ query \"metric{__value__='a'}\" | first | strvalue }}",
            query_result: vec![smpl(&["__name__", "metric", "__value__", "a"], 11.0)],
            output: "a".into(),
            ..Default::default()
        },
        // Missing label is empty when using label function.
        Scenario {
            text: "{{ query \"metric{instance='a'}\" | first | label \"foo\" }}",
            query_result: vec![metric_a(11.0)],
            output: "".into(),
            ..Default::default()
        },
        // Missing label is empty when not using label function.
        Scenario {
            text: "{{ $x := query \"metric\" | first }}{{ $x.Labels.foo }}",
            query_result: vec![metric_a(11.0)],
            output: "".into(),
            ..Default::default()
        },
        Scenario {
            text: "{{ $x := query \"metric\" | first }}{{ $x.Labels.foo }}",
            query_result: vec![metric_a(11.0)],
            output: "".into(),
            html: true,
            ..Default::default()
        },
        // Range over query and sort by label.
        Scenario {
            text: "{{ range query \"metric\" | sortByLabel \"instance\" }}{{.Labels.instance}}:{{.Value}}: {{end}}",
            query_result: vec![smpl(&["__name__", "metric", "instance", "b"], 21.0), metric_a(11.0)],
            output: "a:11: b:21: ".into(),
            ..Default::default()
        },
        // Simple hostname.
        Scenario { text: "{{ \"foo.example.com\" | stripPort }}", output: "foo.example.com".into(), ..Default::default() },
        // Hostname with port.
        Scenario { text: "{{ \"foo.example.com:12345\" | stripPort }}", output: "foo.example.com".into(), ..Default::default() },
        // Simple IPv4 address.
        Scenario { text: "{{ \"192.0.2.1\" | stripPort }}", output: "192.0.2.1".into(), ..Default::default() },
        // IPv4 address with port.
        Scenario { text: "{{ \"192.0.2.1:12345\" | stripPort }}", output: "192.0.2.1".into(), ..Default::default() },
        // Simple IPv6 address.
        Scenario { text: "{{ \"2001:0DB8::1\" | stripPort }}", output: "2001:0DB8::1".into(), ..Default::default() },
        // IPv6 address with port.
        Scenario { text: "{{ \"[2001:0DB8::1]:12345\" | stripPort }}", output: "2001:0DB8::1".into(), ..Default::default() },
        // Value can't be split into host and port.
        Scenario {
            text: "{{ \"[2001:0DB8::1]::12345\" | stripPort }}",
            output: "[2001:0DB8::1]::12345".into(),
            ..Default::default()
        },
        // Missing value is no value for nil options.
        Scenario { text: "{{ .Foo }}", output: "<no value>".into(), ..Default::default() },
        // Missing value is no value for no options.
        Scenario { text: "{{ .Foo }}", options: Some(vec![]), output: "<no value>".into(), ..Default::default() },
        // Assert that missing value returns error with missingkey=error.
        Scenario {
            text: "{{ .Foo }}",
            options: Some(vec!["missingkey=error".into()]),
            should_fail: true,
            error_msg: "error executing template test: template: test:1:3: executing \"test\" at <.Foo>: nil data; no entry for key \"Foo\"",
            ..Default::default()
        },
        // Missing value is "" for nil options in ExpandHTML.
        Scenario { text: "{{ .Foo }}", output: "".into(), html: true, ..Default::default() },
        // Missing value is "" for no options in ExpandHTML.
        Scenario { text: "{{ .Foo }}", options: Some(vec![]), output: "".into(), html: true, ..Default::default() },
        // Assert that missing value returns error with missingkey=error in ExpandHTML.
        Scenario {
            text: "{{ .Foo }}",
            options: Some(vec!["missingkey=error".into()]),
            should_fail: true,
            error_msg: "error executing template test: template: test:1:3: executing \"test\" at <.Foo>: nil data; no entry for key \"Foo\"",
            html: true,
            ..Default::default()
        },
        // Unparsable template.
        Scenario {
            text: "{{",
            should_fail: true,
            error_msg: "error parsing template test: template: test:1: unclosed action",
            ..Default::default()
        },
        // Error in function.
        Scenario {
            text: "{{ query \"missing\" | first }}",
            query_result: vec![],
            should_fail: true,
            error_msg: "error executing template test: template: test:1:21: executing \"test\" at <first>: error calling first: first() called on vector with no elements",
            ..Default::default()
        },
        // Panic.
        Scenario {
            text: "{{ (query \"missing\").banana }}",
            query_result: vec![],
            should_fail: true,
            error_msg: "error executing template test: template: test:1:10: executing \"test\" at <\"missing\">: can't evaluate field banana in type template.queryResult",
            ..Default::default()
        },
        // Regex replacement.
        Scenario { text: "{{ reReplaceAll \"(a)b\" \"x$1\" \"ab\" }}", output: "xa".into(), ..Default::default() },
        // Humanize - float64.
        Scenario {
            text: "{{ range . }}{{ humanize . }}:{{ end }}",
            input: floats(&[0.0, 1.0, 1234567.0, 0.12]),
            output: "0:1:1.235M:120m:".into(),
            ..Default::default()
        },
        // Humanize - string.
        Scenario {
            text: "{{ range . }}{{ humanize . }}:{{ end }}",
            input: strings(&["0.0", "1.0", "1234567.0", ".12"]),
            output: "0:1:1.235M:120m:".into(),
            ..Default::default()
        },
        // Humanize - string with error.
        Scenario {
            text: "{{ humanize \"one\" }}",
            should_fail: true,
            error_msg: "error executing template test: template: test:1:3: executing \"test\" at <humanize \"one\">: error calling humanize: strconv.ParseFloat: parsing \"one\": invalid syntax",
            ..Default::default()
        },
        // Humanize - int.
        Scenario {
            text: "{{ range . }}{{ humanize . }}:{{ end }}",
            input: ints(&[0, -1, 1, 1234567, i64::MAX], IntKind::Int64),
            output: "0:-1:1:1.235M:9.223E:".into(),
            ..Default::default()
        },
        // Humanize - uint.
        Scenario {
            text: "{{ range . }}{{ humanize . }}:{{ end }}",
            input: uints(&[0, 1, 1234567, u64::MAX], UintKind::Uint64),
            output: "0:1:1.235M:18.45E:".into(),
            ..Default::default()
        },
        // Humanize1024 - float64.
        Scenario {
            text: "{{ range . }}{{ humanize1024 . }}:{{ end }}",
            input: floats(&[0.0, 1.0, 1048576.0, 0.12]),
            output: "0:1:1Mi:0.12:".into(),
            ..Default::default()
        },
        // Humanize1024 - string.
        Scenario {
            text: "{{ range . }}{{ humanize1024 . }}:{{ end }}",
            input: strings(&["0.0", "1.0", "1048576.0", ".12"]),
            output: "0:1:1Mi:0.12:".into(),
            ..Default::default()
        },
        // Humanize1024 - string with error.
        Scenario {
            text: "{{ humanize1024 \"one\" }}",
            should_fail: true,
            error_msg: "error executing template test: template: test:1:3: executing \"test\" at <humanize1024 \"one\">: error calling humanize1024: strconv.ParseFloat: parsing \"one\": invalid syntax",
            ..Default::default()
        },
        // Humanize1024 - int.
        Scenario {
            text: "{{ range . }}{{ humanize1024 . }}:{{ end }}",
            input: ints(&[0, -1, 1, 1234567, i64::MAX], IntKind::Int64),
            output: "0:-1:1:1.177Mi:8Ei:".into(),
            ..Default::default()
        },
        // Humanize1024 - uint.
        Scenario {
            text: "{{ range . }}{{ humanize1024 . }}:{{ end }}",
            input: uints(&[0, 1, 1234567, u64::MAX], UintKind::Uint64),
            output: "0:1:1.177Mi:16Ei:".into(),
            ..Default::default()
        },
        // HumanizeDuration - seconds - float64.
        Scenario {
            text: "{{ range . }}{{ humanizeDuration . }}:{{ end }}",
            input: floats(&[0.0, 1.0, 60.0, 3600.0, 86400.0, 86400.0 + 3600.0, -(86400.0 * 2.0 + 3600.0 * 3.0 + 60.0 * 4.0 + 5.0), 899.99]),
            output: "0s:1s:1m 0s:1h 0m 0s:1d 0h 0m 0s:1d 1h 0m 0s:-2d 3h 4m 5s:14m 59s:".into(),
            ..Default::default()
        },
        // HumanizeDuration - seconds - string.
        Scenario {
            text: "{{ range . }}{{ humanizeDuration . }}:{{ end }}",
            input: strings(&["0", "1", "60", "3600", "86400"]),
            output: "0s:1s:1m 0s:1h 0m 0s:1d 0h 0m 0s:".into(),
            ..Default::default()
        },
        // HumanizeDuration - subsecond and fractional seconds - float64.
        Scenario {
            text: "{{ range . }}{{ humanizeDuration . }}:{{ end }}",
            input: floats(&[0.1, 0.0001, 0.12345, 60.1, 60.5, 1.2345, 12.345]),
            output: "100ms:100us:123.5ms:1m 0s:1m 0s:1.234s:12.35s:".into(),
            ..Default::default()
        },
        // HumanizeDuration - subsecond and fractional seconds - string.
        Scenario {
            text: "{{ range . }}{{ humanizeDuration . }}:{{ end }}",
            input: strings(&[".1", ".0001", ".12345", "60.1", "60.5", "1.2345", "12.345"]),
            output: "100ms:100us:123.5ms:1m 0s:1m 0s:1.234s:12.35s:".into(),
            ..Default::default()
        },
        // HumanizeDuration - string with error.
        Scenario {
            text: "{{ humanizeDuration \"one\" }}",
            should_fail: true,
            error_msg: "error executing template test: template: test:1:3: executing \"test\" at <humanizeDuration \"one\">: error calling humanizeDuration: strconv.ParseFloat: parsing \"one\": invalid syntax",
            ..Default::default()
        },
        // HumanizeDuration - int.
        Scenario {
            text: "{{ range . }}{{ humanizeDuration . }}:{{ end }}",
            input: ints(&[0, -1, 1, 1234567], IntKind::Int),
            output: "0s:-1s:1s:14d 6h 56m 7s:".into(),
            ..Default::default()
        },
        // HumanizeDuration - uint.
        Scenario {
            text: "{{ range . }}{{ humanizeDuration . }}:{{ end }}",
            input: uints(&[0, 1, 1234567], UintKind::Uint),
            output: "0s:1s:14d 6h 56m 7s:".into(),
            ..Default::default()
        },
        // Humanize* Inf and NaN - float64.
        Scenario {
            text: "{{ range . }}{{ humanize . }}:{{ humanize1024 . }}:{{ humanizeDuration . }}:{{humanizeTimestamp .}}:{{ end }}",
            input: floats(&[f64::INFINITY, f64::NEG_INFINITY, f64::NAN]),
            output: "+Inf:+Inf:+Inf:+Inf:-Inf:-Inf:-Inf:-Inf:NaN:NaN:NaN:NaN:".into(),
            ..Default::default()
        },
        // Humanize* Inf and NaN - string.
        Scenario {
            text: "{{ range . }}{{ humanize . }}:{{ humanize1024 . }}:{{ humanizeDuration . }}:{{humanizeTimestamp .}}:{{ end }}",
            input: strings(&["+Inf", "-Inf", "NaN"]),
            output: "+Inf:+Inf:+Inf:+Inf:-Inf:-Inf:-Inf:-Inf:NaN:NaN:NaN:NaN:".into(),
            ..Default::default()
        },
        // HumanizePercentage - model.SampleValue input - float64.
        Scenario {
            text: "{{ -0.22222 | humanizePercentage }}:{{ 0.0 | humanizePercentage }}:{{ 0.1234567 | humanizePercentage }}:{{ 1.23456 | humanizePercentage }}",
            output: "-22.22%:0%:12.35%:123.5%".into(),
            ..Default::default()
        },
        // HumanizePercentage - int.
        Scenario {
            text: "{{ range . }}{{ humanizePercentage . }}:{{ end }}",
            input: ints(&[0, -1, 1, 1234567, i64::MAX], IntKind::Int64),
            output: "0%:-100%:100%:1.235e+08%:9.223e+20%:".into(),
            ..Default::default()
        },
        // HumanizePercentage - uint.
        Scenario {
            text: "{{ range . }}{{ humanizePercentage . }}:{{ end }}",
            input: uints(&[0, 1, 1234567, u64::MAX], UintKind::Uint64),
            output: "0%:100%:1.235e+08%:1.845e+21%:".into(),
            ..Default::default()
        },
        // HumanizePercentage - model.SampleValue input - string.
        Scenario {
            text: "{{ \"-0.22222\" | humanizePercentage }}:{{ \"0.0\" | humanizePercentage }}:{{ \"0.1234567\" | humanizePercentage }}:{{ \"1.23456\" | humanizePercentage }}",
            output: "-22.22%:0%:12.35%:123.5%".into(),
            ..Default::default()
        },
        // HumanizePercentage - model.SampleValue input - string with error.
        Scenario {
            text: "{{ \"one\" | humanizePercentage }}",
            should_fail: true,
            error_msg: "error executing template test: template: test:1:11: executing \"test\" at <humanizePercentage>: error calling humanizePercentage: strconv.ParseFloat: parsing \"one\": invalid syntax",
            ..Default::default()
        },
        // HumanizeTimestamp - int.
        Scenario {
            text: "{{ range . }}{{ humanizeTimestamp . }}:{{ end }}",
            input: ints(&[0, -1, 1, 1234567, 9223372036], IntKind::Int64),
            output: "1970-01-01 00:00:00 +0000 UTC:1969-12-31 23:59:59 +0000 UTC:1970-01-01 00:00:01 +0000 UTC:1970-01-15 06:56:07 +0000 UTC:2262-04-11 23:47:16 +0000 UTC:".into(),
            ..Default::default()
        },
        // HumanizeTimestamp - uint.
        Scenario {
            text: "{{ range . }}{{ humanizeTimestamp . }}:{{ end }}",
            input: uints(&[0, 1, 1234567, 9223372036], UintKind::Uint64),
            output: "1970-01-01 00:00:00 +0000 UTC:1970-01-01 00:00:01 +0000 UTC:1970-01-15 06:56:07 +0000 UTC:2262-04-11 23:47:16 +0000 UTC:".into(),
            ..Default::default()
        },
        // HumanizeTimestamp - int with error.
        Scenario {
            text: "{{ range . }}{{ humanizeTimestamp . }}:{{ end }}",
            input: ints(&[i64::MIN, i64::MAX], IntKind::Int64),
            should_fail: true,
            error_msg: "error executing template test: template: test:1:16: executing \"test\" at <humanizeTimestamp .>: error calling humanizeTimestamp: -9.223372036854776e+18 cannot be represented as a nanoseconds timestamp since it overflows int64",
            ..Default::default()
        },
        // HumanizeTimestamp - uint with error.
        Scenario {
            text: "{{ range . }}{{ humanizeTimestamp . }}:{{ end }}",
            input: uints(&[u64::MAX], UintKind::Uint64),
            should_fail: true,
            error_msg: "error executing template test: template: test:1:16: executing \"test\" at <humanizeTimestamp .>: error calling humanizeTimestamp: 1.8446744073709552e+19 cannot be represented as a nanoseconds timestamp since it overflows int64",
            ..Default::default()
        },
        // HumanizeTimestamp - model.SampleValue input - float64.
        Scenario {
            text: "{{ 1435065584.128 | humanizeTimestamp }}",
            output: "2015-06-23 13:19:44.128 +0000 UTC".into(),
            ..Default::default()
        },
        // HumanizeTimestamp - model.SampleValue input - string.
        Scenario {
            text: "{{ \"1435065584.128\" | humanizeTimestamp }}",
            output: "2015-06-23 13:19:44.128 +0000 UTC".into(),
            ..Default::default()
        },
        // ToTime - model.SampleValue input - float64.
        Scenario { text: "{{ (1435065584.128 | toTime).Format \"2006\" }}", output: "2015".into(), ..Default::default() },
        // ToTime - model.SampleValue input - string.
        Scenario { text: "{{ (\"1435065584.128\" | toTime).Format \"2006\" }}", output: "2015".into(), ..Default::default() },
        // toDuration - input as float64 seconds, returns *time.Duration.
        Scenario { text: "{{ (1800 | toDuration).String }}", output: "30m0s".into(), ..Default::default() },
        // toDuration - input as string seconds, returns *time.Duration.
        Scenario { text: "{{ (\"1800\" | toDuration).String }}", output: "30m0s".into(), ..Default::default() },
        // now - returns fixed timestamp as float64 seconds.
        Scenario { text: "{{ now }}", output: "1.353755652e+09".into(), ..Default::default() },
        // now - returns fixed timestamp converted to formatted time string.
        Scenario {
            text: "{{ (now | toTime).Format \"Mon Jan 2 15:04:05 2006\" }}",
            output: "Sat Nov 24 11:14:12 2012".into(),
            ..Default::default()
        },
        // returns Unix milliseconds timestamp for 30 minutes ago.
        Scenario {
            text: "{{ (\"-30m\" | parseDuration | toDuration | (now | toTime).Add).UnixMilli }}",
            output: "1353753852000".into(),
            ..Default::default()
        },
        // Title.
        Scenario { text: "{{ \"aa bb CC\" | title }}", output: "Aa Bb CC".into(), ..Default::default() },
        // toUpper.
        Scenario { text: "{{ \"aa bb CC\" | toUpper }}", output: "AA BB CC".into(), ..Default::default() },
        // toLower.
        Scenario { text: "{{ \"aA bB CC\" | toLower }}", output: "aa bb cc".into(), ..Default::default() },
        // Match.
        Scenario { text: "{{ match \"a+\" \"aa\" }} {{ match \"a+\" \"b\" }}", output: "true false".into(), ..Default::default() },
        // graphLink.
        Scenario { text: "{{ graphLink \"up\" }}", output: "/graph?g0.expr=up&g0.tab=0".into(), ..Default::default() },
        // tableLink.
        Scenario { text: "{{ tableLink \"up\" }}", output: "/graph?g0.expr=up&g0.tab=1".into(), ..Default::default() },
        // tmpl.
        Scenario {
            text: "{{ define \"a\" }}x{{ end }}{{ $name := \"a\"}}{{ tmpl $name . }}",
            output: "x".into(),
            html: true,
            ..Default::default()
        },
        // pathPrefix.
        Scenario { text: "{{ pathPrefix }}", output: "/path/prefix".into(), ..Default::default() },
        // externalURL.
        Scenario { text: "{{ externalURL }}", output: "http://testhost:9090/path/prefix".into(), ..Default::default() },
        // parseDuration with positive duration (using printf to ensure the return is a string).
        Scenario { text: "{{ printf \"%0.2f\" (parseDuration \"1h2m10ms\") }}", output: "3720.01".into(), ..Default::default() },
        // parseDuration with negative duration (using printf to ensure the return is a string).
        Scenario { text: "{{ printf \"%0.2f\" (parseDuration \"-1h2m10ms\") }}", output: "-3720.01".into(), ..Default::default() },
        // Simple hostname.
        Scenario { text: "{{ \"foo.example.com\" | stripDomain }}", output: "foo".into(), ..Default::default() },
        // Hostname with port.
        Scenario { text: "{{ \"foo.example.com:12345\" | stripDomain }}", output: "foo:12345".into(), ..Default::default() },
        // Simple IPv4 address.
        Scenario { text: "{{ \"192.0.2.1\" | stripDomain }}", output: "192.0.2.1".into(), ..Default::default() },
        // IPv4 address with port.
        Scenario { text: "{{ \"192.0.2.1:12345\" | stripDomain }}", output: "192.0.2.1:12345".into(), ..Default::default() },
        // Simple IPv6 address.
        Scenario { text: "{{ \"2001:0DB8::1\" | stripDomain }}", output: "2001:0DB8::1".into(), ..Default::default() },
        // IPv6 address with port.
        Scenario { text: "{{ \"[2001:0DB8::1]:12345\" | stripDomain }}", output: "[2001:0DB8::1]:12345".into(), ..Default::default() },
        // Value can't be split into host and port.
        Scenario {
            text: "{{ \"[2001:0DB8::1]::12345\" | stripDomain }}",
            output: "[2001:0DB8::1]::12345".into(),
            ..Default::default()
        },
        // IPv6 address with a zone identifier containing dots.
        Scenario { text: "{{ \"fe80::1%eth0.100\" | stripDomain }}", output: "fe80::1%eth0.100".into(), ..Default::default() },
        // IPv6 address with a zone identifier containing dots and a port.
        Scenario {
            text: "{{ \"[fe80::1%eth0.100]:9090\" | stripDomain }}",
            output: "[fe80::1%eth0.100]:9090".into(),
            ..Default::default()
        },
        // IPv4-mapped IPv6 address with a zone identifier containing dots.
        Scenario {
            text: "{{ \"::ffff:192.0.2.1%eth0.100\" | stripDomain }}",
            output: "::ffff:192.0.2.1%eth0.100".into(),
            ..Default::default()
        },
    ]);
}

#[test]
fn test_float_to_time() {
    struct Case {
        name: &'static str,
        v: f64,
        want: Option<template::gotime::GoTime>,
        want_err: bool,
    }
    let tests = [
        Case {
            name: "happy path",
            v: 1657155181.0,
            // time.Date(2022, 7, 7, 0, 53, 1, 0, time.UTC)
            want: Some(template::gotime::GoTime { sec: 1657155181, nsec: 0 }),
            want_err: false,
        },
        Case { name: "more than math.MaxInt64", v: 1.79769313486231570814527423731704356798070e+300, want: None, want_err: true },
        Case { name: "less than math.MinInt64", v: -1.79769313486231570814527423731704356798070e+300, want: None, want_err: true },
    ];
    for tt in tests {
        let got = float_to_time(tt.v);
        assert_eq!(got.is_err(), tt.want_err, "{}: floatToTime() error = {:?}, wantErr {}", tt.name, got, tt.want_err);
        if let Ok(t) = got {
            assert_eq!(Some(t), tt.want, "{}", tt.name);
            assert_eq!(t.format("2006-01-02 15:04:05"), "2022-07-07 00:53:01", "{}", tt.name);
        }
    }
}
