//! Port of `util/fmtutil/format_test.go`.

use std::collections::BTreeMap;

use textparse::expfmt::{Bucket, Histogram, Metric};
use util::fmtutil::{Label, MetricMetadata, Sample, TimeSeries, WriteRequest, make_timeseries, metric_text_to_write_request};

fn ts(labels: &[(&str, &str)], value: f64, timestamp: i64) -> TimeSeries {
    TimeSeries {
        labels: labels.iter().map(|(n, v)| Label { name: n.to_string(), value: v.to_string() }).collect(),
        samples: vec![Sample { value, timestamp }],
    }
}

fn md(name: &str, t: i32, help: &str) -> MetricMetadata {
    MetricMetadata { metric_family_name: name.to_string(), r#type: t, help: help.to_string(), unit: String::new() }
}

fn write_request_fixture() -> WriteRequest {
    let n = "__name__";
    WriteRequest {
        metadata: vec![
            md("http_request_duration_seconds", 3, "A histogram of the request duration."),
            md("http_requests_total", 1, "The total number of HTTP requests."),
            md("rpc_duration_seconds", 5, "A summary of the RPC duration in seconds."),
            md("test_metric1", 2, "This is a test metric."),
        ],
        timeseries: vec![
            ts(&[(n, "http_request_duration_seconds_bucket"), ("job", "promtool"), ("le", "0.1")], 33444.0, 1),
            ts(&[(n, "http_request_duration_seconds_bucket"), ("job", "promtool"), ("le", "0.5")], 129389.0, 1),
            ts(&[(n, "http_request_duration_seconds_bucket"), ("job", "promtool"), ("le", "1")], 133988.0, 1),
            ts(&[(n, "http_request_duration_seconds_bucket"), ("job", "promtool"), ("le", "+Inf")], 144320.0, 1),
            ts(&[(n, "http_request_duration_seconds_sum"), ("job", "promtool")], 53423.0, 1),
            ts(&[(n, "http_request_duration_seconds_count"), ("job", "promtool")], 144320.0, 1),
            ts(&[(n, "http_requests_total"), ("code", "200"), ("job", "promtool"), ("method", "post")], 1027.0, 1395066363000),
            ts(&[(n, "http_requests_total"), ("code", "400"), ("job", "promtool"), ("method", "post")], 3.0, 1395066363000),
            ts(&[(n, "rpc_duration_seconds"), ("job", "promtool"), ("quantile", "0.01")], 3102.0, 1),
            ts(&[(n, "rpc_duration_seconds"), ("job", "promtool"), ("quantile", "0.5")], 4773.0, 1),
            ts(&[(n, "rpc_duration_seconds"), ("job", "promtool"), ("quantile", "0.99")], 76656.0, 1),
            ts(&[(n, "rpc_duration_seconds_sum"), ("job", "promtool")], 1.7560473e+07, 1),
            ts(&[(n, "rpc_duration_seconds_count"), ("job", "promtool")], 2693.0, 1),
            ts(&[(n, "test_metric1"), ("b", "c"), ("baz", "qux"), ("d", "e"), ("foo", "bar"), ("job", "promtool")], 1.0, 1),
            ts(&[(n, "test_metric1"), ("b", "c"), ("baz", "qux"), ("d", "e"), ("foo", "bar"), ("job", "promtool")], 2.0, 1),
        ],
    }
}

fn job_labels() -> BTreeMap<String, String> {
    BTreeMap::from([("job".to_string(), "promtool".to_string())])
}

#[test]
fn test_parse_and_push_metrics_text_and_format() {
    let input = "
\t# HELP http_request_duration_seconds A histogram of the request duration.
\t# TYPE http_request_duration_seconds histogram
\thttp_request_duration_seconds_bucket{le=\"0.1\"} 33444 1
\thttp_request_duration_seconds_bucket{le=\"0.5\"} 129389 1
\thttp_request_duration_seconds_bucket{le=\"1\"} 133988 1
\thttp_request_duration_seconds_bucket{le=\"+Inf\"} 144320 1
\thttp_request_duration_seconds_sum 53423 1
\thttp_request_duration_seconds_count 144320 1
\t# HELP http_requests_total The total number of HTTP requests.
\t# TYPE http_requests_total counter
\thttp_requests_total{method=\"post\",code=\"200\"} 1027 1395066363000
\thttp_requests_total{method=\"post\",code=\"400\"}    3 1395066363000
\t# HELP rpc_duration_seconds A summary of the RPC duration in seconds.
\t# TYPE rpc_duration_seconds summary
\trpc_duration_seconds{quantile=\"0.01\"} 3102 1
\trpc_duration_seconds{quantile=\"0.5\"} 4773 1
\trpc_duration_seconds{quantile=\"0.99\"} 76656 1
\trpc_duration_seconds_sum 1.7560473e+07 1
\trpc_duration_seconds_count 2693 1
\t# HELP test_metric1 This is a test metric.
\t# TYPE test_metric1 gauge
\ttest_metric1{b=\"c\",baz=\"qux\",d=\"e\",foo=\"bar\"} 1 1
\ttest_metric1{b=\"c\",baz=\"qux\",d=\"e\",foo=\"bar\"} 2 1
\t";
    let expected = metric_text_to_write_request(input.as_bytes(), &job_labels()).expect("MetricTextToWriteRequest");
    assert_eq!(expected, write_request_fixture());
}

#[test]
fn test_metric_text_to_write_request_error_parsing_float_value() {
    let input = "
\t# HELP http_requests_total The total number of HTTP requests.
\t# TYPE http_requests_total counter
\thttp_requests_total{method=\"post\",code=\"200\"} 1027Error 1395066363000
\thttp_requests_total{method=\"post\",code=\"400\"}    3 1395066363000
\t";
    let err = metric_text_to_write_request(input.as_bytes(), &job_labels()).expect_err("error");
    assert_eq!("text format parsing error in line 4: expected float as value, got \"1027Error\"", err);
}

#[test]
fn test_metric_text_to_write_request_error_parsing_metric_type() {
    let input = "
\t# HELP node_info node info summary.
\t# TYPE node_info info
\tnode_info{test=\"summary\"} 1 1395066363000
\t";
    let err = metric_text_to_write_request(input.as_bytes(), &job_labels()).expect_err("error");
    assert_eq!("text format parsing error in line 3: unknown metric type \"info\"", err);
}

#[test]
fn test_make_timeseries_histogram_inf_bucket() {
    let bucket = |count: u64, upper: f64| Bucket { cumulative_count: Some(count), upper_bound: Some(upper), ..Default::default() };
    let tests: Vec<(&str, Histogram)> = vec![
        (
            "Histogram missing +Inf bucket",
            Histogram { bucket: vec![bucket(5, 1.0), bucket(10, 5.0)], sample_count: Some(15), ..Default::default() },
        ),
        (
            "Histogram already has +Inf bucket",
            Histogram {
                bucket: vec![bucket(5, 1.0), bucket(10, 5.0), bucket(15, f64::INFINITY)],
                sample_count: Some(15),
                ..Default::default()
            },
        ),
    ];
    for (name, histogram) in tests {
        let mut wr = WriteRequest::default();
        let mut labels = BTreeMap::from([("__name__".to_string(), "test_histogram".to_string())]);
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
        let metric = Metric { histogram: Some(histogram), timestamp_ms: Some(now), ..Default::default() };
        make_timeseries(&mut wr, &mut labels, &metric).expect(name);
        let has_inf = wr.timeseries.iter().any(|ts| ts.labels.iter().any(|l| l.name == "le" && l.value == "+Inf"));
        assert!(has_inf, "{name}: expected +Inf bucket in histogram");
    }
}
