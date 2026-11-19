//! Port of `util/strutil/strconv_test.go`.

use util::strutil::{graph_link_for_expression, sanitize_full_label_name, sanitize_label_name, table_link_for_expression};

#[test]
fn test_link() {
    let link_tests: &[(&str, &str, &str)] = &[
        (
            "sum(incoming_http_requests_total) by (system)",
            "/graph?g0.expr=sum%28incoming_http_requests_total%29+by+%28system%29&g0.tab=0",
            "/graph?g0.expr=sum%28incoming_http_requests_total%29+by+%28system%29&g0.tab=1",
        ),
        (
            "sum(incoming_http_requests_total{system=\"trackmetadata\"})",
            "/graph?g0.expr=sum%28incoming_http_requests_total%7Bsystem%3D%22trackmetadata%22%7D%29&g0.tab=0",
            "/graph?g0.expr=sum%28incoming_http_requests_total%7Bsystem%3D%22trackmetadata%22%7D%29&g0.tab=1",
        ),
        ("up", "/graph?g0.expr=up&g0.tab=0", "/graph?g0.expr=up&g0.tab=1"),
        (
            "rate(http_requests_total[5m])",
            "/graph?g0.expr=rate%28http_requests_total%5B5m%5D%29&g0.tab=0",
            "/graph?g0.expr=rate%28http_requests_total%5B5m%5D%29&g0.tab=1",
        ),
        ("", "/graph?g0.expr=&g0.tab=0", "/graph?g0.expr=&g0.tab=1"),
        (
            "metric_name{label=\"value with spaces\"}",
            "/graph?g0.expr=metric_name%7Blabel%3D%22value+with+spaces%22%7D&g0.tab=0",
            "/graph?g0.expr=metric_name%7Blabel%3D%22value+with+spaces%22%7D&g0.tab=1",
        ),
    ];
    for &(expression, graph, table) in link_tests {
        assert_eq!(graph, graph_link_for_expression(expression), "GraphLinkForExpression failed for expression ({expression})");
        assert_eq!(table, table_link_for_expression(expression), "TableLinkForExpression failed for expression ({expression})");
    }
}

#[test]
fn test_sanitize_label_name() {
    let tests: &[(&str, &str, &str)] = &[
        ("valid label name", "fooClientLABEL", "fooClientLABEL"),
        ("label with special characters", "barClient.LABEL$$##", "barClient_LABEL____"),
        ("label starting with digit", "123label", "123label"),
        ("label with dashes", "my-label-name", "my_label_name"),
        ("label with spaces", "my label name", "my_label_name"),
        ("label with mixed case and numbers", "Test123Label456", "Test123Label456"),
        ("label with unicode characters", "test-ñ-ü-label", "test_____label"),
        ("empty string", "", ""),
        ("only underscores", "___", "___"),
        ("label with colons", "namespace:metric_name", "namespace_metric_name"),
    ];
    for &(name, input, expected) in tests {
        assert_eq!(expected, sanitize_label_name(input), "{name}: SanitizeLabelName({input:?})");
    }
}

#[test]
fn test_sanitize_full_label_name() {
    let tests: &[(&str, &str, &str)] = &[
        ("valid label name", "fooClientLABEL", "fooClientLABEL"),
        ("label with special characters", "barClient.LABEL$$##", "barClient_LABEL____"),
        ("label starting with digit", "0zerothClient1LABEL", "_zerothClient1LABEL"),
        ("empty string", "", "_"),
        ("label starting with multiple digits", "123abc", "_23abc"),
        ("label with dashes", "my-label-name", "my_label_name"),
        ("label with spaces", "my label name", "my_label_name"),
        ("label with numbers in middle", "Test123Label456", "Test123Label456"),
        ("single underscore", "_", "_"),
        ("label starting with underscore", "_validLabel", "_validLabel"),
        ("label with colons", "namespace:metric_name", "namespace_metric_name"),
        ("label with unicode characters", "test-ñ-ü-label", "test_____label"),
        ("only digits", "12345", "_2345"),
        ("label with mixed invalid characters at start", "!@#test", "___test"),
        ("label with consecutive digits at start", "0123test", "_123test"),
    ];
    for &(name, input, expected) in tests {
        assert_eq!(expected, sanitize_full_label_name(input), "{name}: SanitizeFullLabelName({input:?})");
    }
}
