//! Port of `web/api/v1/api_scenarios_test.go`.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use common::testhelpers::*;
use json::Json;

/// Go `time.Now().Unix()`.
fn now_unix() -> i64 {
    now_millis() / 1000
}

fn name_is(group: &Json, name: &str) -> bool {
    matches!(group.get("name"), Some(Json::Str(n)) if n == name)
}

/// The API with no metrics and no rules.
#[test]
fn test_api_empty() {
    // Create an API with empty defaults (no series, no rules).
    let api = new_test_api(ApiConfig::default());

    // GET /api/v1/labels returns success with empty array.
    get(&api, "/api/v1/labels", &[])
        .require_success()
        .validate_openapi()
        .require_json_array("$.data");

    // GET /api/v1/query?query=up returns success (empty result ok).
    get(&api, "/api/v1/query", &["query", "up"])
        .validate_openapi()
        .require_success()
        .require_equals("$.data.resultType", "vector");

    // GET /api/v1/query_range?query=up returns success.
    get(
        &api,
        "/api/v1/query_range",
        &["query", "up", "start", "0", "end", "100", "step", "10"],
    )
    .require_success()
    .validate_openapi()
    .require_equals("$.data.resultType", "matrix");

    // GET /api/v1/series returns success with empty result.
    get(
        &api,
        "/api/v1/series",
        &["match[]", "up", "start", "0", "end", "100"],
    )
    .require_success()
    .validate_openapi()
    .require_json_array("$.data");

    // GET /api/v1/label/__name__/values returns success with empty array.
    get(&api, "/api/v1/label/__name__/values", &[])
        .require_success()
        .validate_openapi()
        .require_json_array("$.data");

    // GET /api/v1/targets returns success.
    get(&api, "/api/v1/targets", &[])
        .require_success()
        .require_json_path_exists("$.data.activeTargets");

    // GET /api/v1/scrape_pools/config requires a pool.
    get(&api, "/api/v1/scrape_pools/config", &[])
        .require_status_code(400)
        .require_error()
        .require_equals("$.errorType", "bad_data");

    // GET /api/v1/scrape_pools/config returns effective config.
    {
        let target_retriever = FakeTargetRetriever {
            scrape_config: HashMap::from([(
                "prometheus".to_string(),
                Arc::new(config::ScrapeConfig {
                    job_name: "prometheus".to_string(),
                    ..Default::default()
                }),
            )]),
            ..new_empty_target_retriever()
        };
        let config_api = new_test_api(ApiConfig {
            target_retriever: Some(Arc::new(target_retriever)),
            ..Default::default()
        });
        get(
            &config_api,
            "/api/v1/scrape_pools/config",
            &["scrapePool", "prometheus"],
        )
        .require_success()
        .validate_openapi()
        .require_json_path_exists("$.data.yaml");
    }

    // GET /api/v1/rules returns success with empty groups.
    get(&api, "/api/v1/rules", &[])
        .require_success()
        .validate_openapi()
        .require_json_path_exists("$.data.groups");

    // GET /api/v1/alerts returns success with empty alerts.
    get(&api, "/api/v1/alerts", &[])
        .require_success()
        .validate_openapi()
        .require_json_path_exists("$.data.alerts");

    // GET /api/v1/alertmanagers returns success.
    get(&api, "/api/v1/alertmanagers", &[])
        .require_success()
        .validate_openapi()
        .require_json_path_exists("$.data.activeAlertmanagers");

    // GET /api/v1/metadata returns success.
    get(&api, "/api/v1/metadata", &[])
        .require_success()
        .validate_openapi()
        .require_json_path_exists("$.data");

    // GET /api/v1/status/config returns success.
    get(&api, "/api/v1/status/config", &[])
        .require_success()
        .validate_openapi()
        .require_json_path_exists("$.data.yaml");

    // GET /api/v1/status/flags returns success.
    get(&api, "/api/v1/status/flags", &[])
        .require_success()
        .validate_openapi()
        .require_json_path_exists("$.data");

    // GET /api/v1/status/runtimeinfo returns success.
    get(&api, "/api/v1/status/runtimeinfo", &[])
        .require_success()
        .validate_openapi()
        .require_json_path_exists("$.data");

    // GET /api/v1/status/buildinfo returns success.
    get(&api, "/api/v1/status/buildinfo", &[])
        .require_success()
        .validate_openapi()
        .require_json_path_exists("$.data");

    // POST /api/v1/query with form data returns success.
    post(&api, "/api/v1/query", &["query", "up"])
        .require_success()
        .validate_openapi()
        .require_equals("$.data.resultType", "vector");
}

/// The API with metrics/series data.
#[test]
fn test_api_with_series() {
    // Create an API with sample series data.
    let api = new_test_api(ApiConfig {
        queryable: Some(Arc::new(new_queryable_with_series(
            fixture_multiple_series(),
        ))),
        ..Default::default()
    });

    // GET /api/v1/query returns vector with >= 1 sample.
    get(&api, "/api/v1/query", &["query", "up"])
        .require_success()
        .validate_openapi()
        .require_equals("$.data.resultType", "vector")
        .require_len_at_least("$.data.result", 1);

    // GET /api/v1/query_range returns matrix result type.
    {
        // Use relative timestamps to match our fixtures.
        let now = now_unix();
        let (start, end) = ((now - 120).to_string(), now.to_string());
        get(
            &api,
            "/api/v1/query_range",
            &["query", "up", "start", &start, "end", &end, "step", "60"],
        )
        .require_success()
        .validate_openapi()
        .require_equals("$.data.resultType", "matrix");
        // Note: Result may be empty if timestamps don't align perfectly with samples.
    }

    // GET /api/v1/labels returns non-empty array.
    get(&api, "/api/v1/labels", &[])
        .require_success()
        .validate_openapi()
        .require_json_array("$.data")
        .require_len_at_least("$.data", 1);

    // GET /api/v1/label/__name__/values contains expected metric names.
    get(&api, "/api/v1/label/__name__/values", &[])
        .require_success()
        .validate_openapi()
        .require_array_contains("$.data", "up")
        .require_array_contains("$.data", "http_requests_total");

    // GET /api/v1/label/job/values contains expected jobs.
    get(&api, "/api/v1/label/job/values", &[])
        .require_success()
        .validate_openapi()
        .require_json_array("$.data")
        .require_array_contains("$.data", "prometheus")
        .require_array_contains("$.data", "node")
        .require_array_contains("$.data", "api");

    // GET /api/v1/series with match returns results.
    get(
        &api,
        "/api/v1/series",
        &["match[]", "up", "start", "0", "end", "120"],
    )
    .require_success()
    .validate_openapi()
    .require_json_array("$.data")
    .require_len_at_least("$.data", 1);

    // GET /api/v1/query with specific job returns filtered results.
    get(&api, "/api/v1/query", &["query", r#"up{job="prometheus"}"#])
        .require_success()
        .validate_openapi()
        .require_equals("$.data.resultType", "vector")
        .require_len_at_least("$.data.result", 1);

    // GET /api/v1/query with aggregation returns result.
    get(&api, "/api/v1/query", &["query", "sum(up)"])
        .require_success()
        .validate_openapi()
        .require_equals("$.data.resultType", "vector");

    // POST /api/v1/query returns vector with data.
    post(&api, "/api/v1/query", &["query", "up"])
        .require_success()
        .validate_openapi()
        .require_equals("$.data.resultType", "vector")
        .require_len_at_least("$.data.result", 1);
}

/// The API with rules configured.
#[test]
fn test_api_with_rules() {
    // Create an API with rule groups.
    let api = new_test_api(ApiConfig {
        rules_retriever: Some(Arc::new(new_rules_retriever_with_groups(
            fixture_rule_groups(),
        ))),
        ..Default::default()
    });

    // GET /api/v1/rules returns groups with rules.
    get(&api, "/api/v1/rules", &[])
        .require_success()
        .validate_openapi()
        .require_json_path_exists("$.data.groups")
        .require_len_at_least("$.data.groups", 1)
        .require_some("$.data.groups", |g| name_is(g, "example"))
        .require_some("$.data.groups", |g| {
            // Check that the group has rules.
            name_is(g, "example") && matches!(g.get("rules"), Some(Json::Arr(r)) if !r.is_empty())
        });

    // GET /api/v1/alerts returns alerts array.
    get(&api, "/api/v1/alerts", &[])
        .require_success()
        .validate_openapi()
        .require_json_path_exists("$.data.alerts")
        .require_json_array("$.data.alerts");

    // GET /api/v1/rules with rule_name filter.
    get(&api, "/api/v1/rules", &["rule_name[]", "InstanceDown"])
        .require_success()
        .validate_openapi()
        .require_json_path_exists("$.data.groups");
}

/// The API when the TSDB is not ready (e.g., during WAL replay). TSDB not ready errors become errorUnavailable
/// (`set_unavail_status_on_tsdb_not_ready`), which answers HTTP 500.
#[test]
fn test_apitsdb_not_ready() {
    // Create an API with a queryable that returns tsdb.ErrNotReady.
    let api = new_test_api(ApiConfig {
        queryable: Some(Arc::new(TsdbNotReadyQueryable)),
        ..Default::default()
    });

    // GET /api/v1/query returns 500 when TSDB not ready.
    get(&api, "/api/v1/query", &["query", "up"])
        .require_status_code(500)
        .validate_openapi()
        .require_error();

    // POST /api/v1/query returns 500 when TSDB not ready.
    post(&api, "/api/v1/query", &["query", "up"])
        .require_status_code(500)
        .validate_openapi()
        .require_error();

    // GET /api/v1/query_range returns 500 when TSDB not ready.
    get(
        &api,
        "/api/v1/query_range",
        &["query", "up", "start", "0", "end", "100", "step", "10"],
    )
    .require_status_code(500)
    .validate_openapi()
    .require_error();

    // GET /api/v1/series returns 500 when TSDB not ready.
    get(
        &api,
        "/api/v1/series",
        &["match[]", "up", "start", "0", "end", "100"],
    )
    .require_status_code(500)
    .validate_openapi()
    .require_error();

    // GET /api/v1/labels returns 500 when TSDB not ready.
    get(&api, "/api/v1/labels", &[])
        .require_status_code(500)
        .validate_openapi()
        .require_error();

    // GET /api/v1/label/{name}/values returns 500 when TSDB not ready.
    get(&api, "/api/v1/label/__name__/values", &[])
        .require_status_code(500)
        .validate_openapi()
        .require_error();
}

fn has_histogram(item: &Json) -> bool {
    // Check that the sample has a histogram field (not a value field).
    matches!(item, Json::Obj(_)) && item.get("histogram").is_some()
}

/// The API with native histogram data.
#[test]
fn test_api_with_native_histograms() {
    // Create an API with histogram series data.
    let api = new_test_api(ApiConfig {
        queryable: Some(Arc::new(new_queryable_with_series(
            fixture_histogram_series(),
        ))),
        ..Default::default()
    });

    // GET /api/v1/query returns vector with native histogram.
    get(&api, "/api/v1/query", &["query", "test_histogram"])
        .require_success()
        .validate_openapi()
        .require_equals("$.data.resultType", "vector")
        .require_len_at_least("$.data.result", 1)
        .require_some("$.data.result", has_histogram);

    // POST /api/v1/query returns vector with native histogram.
    post(&api, "/api/v1/query", &["query", "test_histogram"])
        .require_success()
        .validate_openapi()
        .require_equals("$.data.resultType", "vector")
        .require_len_at_least("$.data.result", 1)
        .require_some("$.data.result", has_histogram);

    // GET /api/v1/query_range returns matrix with native histogram.
    {
        // Use relative timestamps to match our fixtures.
        let now = now_unix();
        let (start, end) = ((now - 120).to_string(), now.to_string());
        get(
            &api,
            "/api/v1/query_range",
            &[
                "query",
                "test_histogram",
                "start",
                &start,
                "end",
                &end,
                "step",
                "60",
            ],
        )
        .require_success()
        .validate_openapi()
        .require_equals("$.data.resultType", "matrix");
    }

    // GET /api/v1/query with histogram selector.
    get(
        &api,
        "/api/v1/query",
        &["query", r#"test_histogram{job="prometheus"}"#],
    )
    .require_success()
    .validate_openapi()
    .require_equals("$.data.resultType", "vector")
    .require_len_at_least("$.data.result", 1);

    // GET /api/v1/series returns histogram metric series.
    let end = now_unix().to_string();
    get(
        &api,
        "/api/v1/series",
        &["match[]", "test_histogram", "start", "0", "end", &end],
    )
    .require_success()
    .validate_openapi()
    .require_json_array("$.data")
    .require_len_at_least("$.data", 1);
}

/// The API with the stats query parameter.
#[test]
fn test_api_with_stats() {
    // Create an API with sample series data.
    let api = new_test_api(ApiConfig {
        queryable: Some(Arc::new(new_queryable_with_series(
            fixture_multiple_series(),
        ))),
        ..Default::default()
    });

    let now = now_unix();
    let (start, end) = ((now - 120).to_string(), now.to_string());

    // Test combinations of methods, endpoints, and stats values. Values outside the supported enum ("true",
    // "all") keep the historical behaviour (any non-empty value enables basic statistics) but attach a
    // deprecation warning to the response; they will be rejected in the next major release.
    let methods = ["GET", "POST"];
    // (value, expect stats, expect warning)
    let stats_values = [
        ("true", true, false),
        ("all", true, false),
        ("1", true, true),
        ("", false, false),
    ];
    let deprecation_warning = r#"value "1" for parameter "stats" is deprecated and will be rejected in the next major release, use "true" or "all""#;

    for method in methods {
        for (value, expect_stats, expect_warning) in stats_values {
            let check = |resp: Response, name: &str| {
                // `name` is the Go subtest name.
                let _ = name;
                resp.require_success().validate_openapi();
                if expect_warning {
                    resp.require_array_contains("$.warnings", deprecation_warning);
                } else {
                    resp.require_json_path_not_exists("$.warnings");
                }
                if expect_stats {
                    resp.require_json_path_exists("$.data.stats")
                        .require_json_path_exists("$.data.stats.timings")
                        .require_json_path_exists("$.data.stats.samples");
                } else {
                    resp.require_json_path_not_exists("$.data.stats");
                }
            };
            let send = |path: &str, params: &[&str]| {
                if method == "GET" {
                    get(&api, path, params)
                } else {
                    post(&api, path, params)
                }
            };

            // <method> /api/v1/query with stats=<value>.
            let mut params = vec!["query", "up"];
            if !value.is_empty() {
                params.extend(["stats", value]);
            }
            check(
                send("/api/v1/query", &params),
                &format!("{method} /api/v1/query with stats={value}"),
            );

            // <method> /api/v1/query_range with stats=<value>.
            let mut params = vec!["query", "up", "start", &start, "end", &end, "step", "60"];
            if !value.is_empty() {
                params.extend(["stats", value]);
            }
            check(
                send("/api/v1/query_range", &params),
                &format!("{method} /api/v1/query_range with stats={value}"),
            );
        }
    }
}

/// Not in Go: the test validator rejects wrong shapes, so a passing `validate_openapi` means something.
#[test]
fn test_openapi_validator_rejects_wrong_shapes() {
    let ok = r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1767436620,"1"]}]}}"#;
    let bad = [
        // A sample value with one element.
        r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1767436620]}]}}"#,
        // An unknown result type.
        r#"{"status":"success","data":{"resultType":"table","result":[]}}"#,
        // A field the response body does not have.
        r#"{"status":"success","data":{"resultType":"vector","result":[]},"extra":1}"#,
        // A wrong status.
        r#"{"status":"ok","data":{"resultType":"vector","result":[]}}"#,
        // No data.
        r#"{"status":"success"}"#,
    ];
    for spec in common::testhelpers::openapi::specs() {
        let errs = spec.validate_response("GET", "/api/v1/query", 200, "application/json", ok);
        assert!(errs.is_empty(), "OpenAPI {}: {errs:?}", spec.version);
        for b in bad {
            let errs = spec.validate_response("GET", "/api/v1/query", 200, "application/json", b);
            assert!(
                !errs.is_empty(),
                "OpenAPI {}: {b} should fail validation",
                spec.version
            );
        }
        // An undeclared content type, an undeclared path, a missing required parameter, a bad parameter type.
        assert!(
            !spec
                .validate_response("GET", "/api/v1/query", 200, "text/plain", ok)
                .is_empty()
        );
        assert!(
            !spec
                .validate_request("GET", "/api/v1/nope", "", "", b"")
                .is_empty()
        );
        assert!(
            !spec
                .validate_request("GET", "/api/v1/query", "time=0", "", b"")
                .is_empty()
        );
        assert!(
            !spec
                .validate_request("GET", "/api/v1/query", "query=up&limit=x", "", b"")
                .is_empty()
        );
        assert!(
            !spec
                .validate_request(
                    "GET",
                    "/api/v1/query_range",
                    "query=up&start=0&end=1&step=abc",
                    "",
                    b""
                )
                .is_empty()
        );
        assert!(
            spec.validate_request(
                "GET",
                "/api/v1/query_range",
                "query=up&start=0&end=1&step=1m30s",
                "",
                b""
            )
            .is_empty()
        );
        // A form body with an unknown field.
        assert!(
            !spec
                .validate_request(
                    "POST",
                    "/api/v1/query",
                    "",
                    "application/x-www-form-urlencoded",
                    b"query=up&bogus=1"
                )
                .is_empty()
        );
    }
}
