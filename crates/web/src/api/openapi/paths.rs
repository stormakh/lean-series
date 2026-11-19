//! Port of `web/api/v1/openapi_paths.go`: the path items of every endpoint registered in `Api::register`.

use super::emit::{Node, str_seq};
use super::examples::*;
use super::model::*;
use super::{DEFAULT_SEARCH_BATCH_SIZE, FUZZ_ALGORITHMS, OpenApiBuilder};

/// One hour before `EXAMPLE_TIME`.
const EXAMPLE_TIME_MINUS_1H: i64 = EXAMPLE_TIME - 3600;

const TIMEOUT_DESC: &str = "Evaluation timeout. Optional. Must be positive. Defaults to and is capped by the effective server-side maximum: global.query_max_duration when the query-cost feature is enabled and the value is set, otherwise the -query.timeout flag.";
const STATS_DESC: &str = "Include query statistics in the response. Supported values: 'true' (basic statistics) and 'all' (basic plus per-step statistics). Other non-empty values are deprecated (they behave like 'true') and will be rejected in the next major release.";
const COST_DESC: &str = "When true, include an estimated-vs-actual cost comparison in the response. Any non-boolean value is an error. Requires the query-cost feature flag (--enable-feature=query-cost).";
const MAX_SERIES_DESC: &str = "Lower the maximum number of series this query may load. Optional. Honored only with the query-cost feature flag (--enable-feature=query-cost). It may only lower the query_max_series ceiling, never raise it: a higher value is rejected.";
const MAX_SAMPLES_READ_DESC: &str = "Lower the maximum number of samples this query may read. Optional. Honored only with the query-cost feature flag (--enable-feature=query-cost). It may only lower the query_max_samples_read ceiling, never raise it: a higher value is rejected.";
const MAX_QUERY_DURATION_DESC: &str = "Lower the maximum execution duration of this query. Optional. Honored only with the query-cost feature flag (--enable-feature=query-cost). It may only lower the effective duration ceiling (query_max_duration when set, otherwise the -query.timeout flag), never raise it: a higher value is rejected.";

fn qp(
    name: &'static str,
    description: &'static str,
    required: bool,
    schema: Schema,
    examples: Vec<ExampleArg>,
) -> Parameter {
    query_param_with_example(name, description, required, schema, examples)
}

fn duration_examples(duration: &'static str, number: &'static str) -> Vec<ExampleArg> {
    vec![
        ("duration", Node::from(duration)),
        ("number", Node::from(number)),
    ]
}

/// Go `base.CreateSchemaProxy(&base.Schema{Type: []string{"array"}, Items: stringSchema()})`.
fn string_array() -> Schema {
    array_of(string_schema())
}

fn op(operation_id: &'static str, summary: &'static str, tag: &'static str) -> Operation {
    Operation {
        operation_id,
        summary,
        tags: vec![tag],
        ..Default::default()
    }
}

fn timeout_param() -> Parameter {
    qp(
        "timeout",
        TIMEOUT_DESC,
        false,
        duration_schema(),
        duration_examples("1m30s", "90"),
    )
}

fn lookback_delta_param() -> Parameter {
    qp(
        "lookback_delta",
        "Override the lookback period for this query. Optional.",
        false,
        duration_schema(),
        duration_examples("5m", "300"),
    )
}

/// The `max_series`, `max_samples_read` and `max_query_duration` parameters.
fn cost_limit_params() -> [Parameter; 3] {
    [
        qp(
            "max_series",
            MAX_SERIES_DESC,
            false,
            integer_schema(),
            one(1000),
        ),
        qp(
            "max_samples_read",
            MAX_SAMPLES_READ_DESC,
            false,
            integer_schema(),
            one(100000),
        ),
        qp(
            "max_query_duration",
            MAX_QUERY_DURATION_DESC,
            false,
            duration_schema(),
            duration_examples("30s", "30"),
        ),
    ]
}

impl OpenApiBuilder {
    pub(super) fn query_path(&self) -> PathItem {
        let mut params = vec![
            qp(
                "limit",
                "The maximum number of metrics to return.",
                false,
                integer_schema(),
                one(100),
            ),
            qp(
                "time",
                "The evaluation timestamp (optional, defaults to current time).",
                false,
                timestamp_schema(),
                timestamp_examples(EXAMPLE_TIME),
            ),
            qp(
                "query",
                "The PromQL query to execute.",
                true,
                string_schema(),
                one("up"),
            ),
            timeout_param(),
            lookback_delta_param(),
            qp("stats", STATS_DESC, false, string_schema(), one("all")),
            qp("cost", COST_DESC, false, boolean_schema(), one(true)),
        ];
        params.extend(cost_limit_params());
        PathItem {
            get: Some(Operation {
                parameters: params,
                responses: responses_with_error_examples(
                    "QueryOutputBody",
                    query_response_examples(),
                    error_response_examples(),
                    "Query executed successfully.",
                    "Error executing query.",
                ),
                ..op("query", "Evaluate an instant query", "query")
            }),
            post: Some(Operation {
                request_body: Some(form_request_body_with_examples(
                    "QueryPostInputBody",
                    query_post_examples(),
                    "Submit an instant query. This endpoint accepts the same parameters as the GET version.",
                )),
                responses: responses_with_error_examples(
                    "QueryOutputBody",
                    query_response_examples(),
                    error_response_examples(),
                    "Instant query executed successfully.",
                    "Error executing instant query.",
                ),
                ..op("query-post", "Evaluate an instant query", "query")
            }),
            ..Default::default()
        }
    }

    pub(super) fn query_range_path(&self) -> PathItem {
        let mut params = vec![
            qp(
                "limit",
                "The maximum number of metrics to return.",
                false,
                integer_schema(),
                one(100),
            ),
            qp(
                "start",
                "The start time of the query.",
                true,
                timestamp_schema(),
                timestamp_examples(EXAMPLE_TIME_MINUS_1H),
            ),
            qp(
                "end",
                "The end time of the query.",
                true,
                timestamp_schema(),
                timestamp_examples(EXAMPLE_TIME),
            ),
            qp(
                "step",
                "The step size of the query.",
                true,
                duration_schema(),
                duration_examples("15s", "15"),
            ),
            qp(
                "query",
                "The query to execute.",
                true,
                string_schema(),
                one("rate(prometheus_http_requests_total{handler=\"/api/v1/query\"}[5m])"),
            ),
            timeout_param(),
            lookback_delta_param(),
            qp("stats", STATS_DESC, false, string_schema(), one("all")),
            qp("cost", COST_DESC, false, boolean_schema(), one(true)),
        ];
        params.extend(cost_limit_params());
        PathItem {
            get: Some(Operation {
                parameters: params,
                responses: responses_with_error_examples(
                    "QueryRangeOutputBody",
                    query_range_response_examples(),
                    error_response_examples(),
                    "Range query executed successfully.",
                    "Error executing range query.",
                ),
                ..op("query-range", "Evaluate a range query", "query")
            }),
            post: Some(Operation {
                request_body: Some(form_request_body_with_examples(
                    "QueryRangePostInputBody",
                    query_range_post_examples(),
                    "Submit a range query. This endpoint accepts the same parameters as the GET version.",
                )),
                responses: responses_with_error_examples(
                    "QueryRangeOutputBody",
                    query_range_response_examples(),
                    error_response_examples(),
                    "Range query executed successfully.",
                    "Error executing range query.",
                ),
                ..op("query-range-post", "Evaluate a range query", "query")
            }),
            ..Default::default()
        }
    }

    /// The `/query_cost` endpoint, which estimates the cost of an instant query without executing it. It is
    /// only registered when the query-cost feature is enabled.
    pub(super) fn query_cost_path(&self) -> PathItem {
        let mut params = vec![
            qp(
                "time",
                "The evaluation timestamp (optional, defaults to current time).",
                false,
                timestamp_schema(),
                timestamp_examples(EXAMPLE_TIME),
            ),
            qp(
                "query",
                "The PromQL query to estimate.",
                true,
                string_schema(),
                one("up"),
            ),
            timeout_param(),
            lookback_delta_param(),
            qp(
                "limit",
                "The maximum number of metrics to return. Validated identically to /api/v1/query but ignored, as no series are returned.",
                false,
                integer_schema(),
                one(100),
            ),
            qp(
                "stats",
                "Validated identically to /api/v1/query but ignored, as no query statistics are returned.",
                false,
                string_schema(),
                one("all"),
            ),
            qp(
                "cost",
                "Validated identically to /api/v1/query but ignored, as the response is already a cost estimate. Any non-boolean value is an error.",
                false,
                boolean_schema(),
                one(true),
            ),
        ];
        params.extend(cost_limit_params());
        let description = "Estimates the resource cost of an instant query without executing it. Requires the query-cost feature flag (--enable-feature=query-cost).";
        PathItem {
            get: Some(Operation {
                description,
                parameters: params,
                responses: responses_with_error_examples(
                    "QueryCostOutputBody",
                    query_cost_response_examples(),
                    error_response_examples(),
                    "Query cost estimated successfully.",
                    "Error estimating query cost.",
                ),
                ..op(
                    "query-cost",
                    "Estimate the cost of an instant query",
                    "query",
                )
            }),
            post: Some(Operation {
                description,
                request_body: Some(form_request_body_with_examples(
                    "QueryCostPostInputBody",
                    query_cost_post_examples(),
                    "Submit an instant query for cost estimation. This endpoint accepts the same parameters as the GET version.",
                )),
                responses: responses_with_error_examples(
                    "QueryCostOutputBody",
                    query_cost_response_examples(),
                    error_response_examples(),
                    "Query cost estimated successfully.",
                    "Error estimating query cost.",
                ),
                ..op(
                    "query-cost-post",
                    "Estimate the cost of an instant query",
                    "query",
                )
            }),
            ..Default::default()
        }
    }

    /// The `/query_range_cost` endpoint, which estimates the cost of a range query without executing it. It
    /// is only registered when the query-cost feature is enabled.
    pub(super) fn query_range_cost_path(&self) -> PathItem {
        let mut params = vec![
            qp(
                "start",
                "The start time of the query.",
                true,
                timestamp_schema(),
                timestamp_examples(EXAMPLE_TIME_MINUS_1H),
            ),
            qp(
                "end",
                "The end time of the query.",
                true,
                timestamp_schema(),
                timestamp_examples(EXAMPLE_TIME),
            ),
            qp(
                "step",
                "The step size of the query.",
                true,
                duration_schema(),
                duration_examples("15s", "15"),
            ),
            qp(
                "query",
                "The PromQL query to estimate.",
                true,
                string_schema(),
                one("rate(prometheus_http_requests_total{handler=\"/api/v1/query\"}[5m])"),
            ),
            timeout_param(),
            lookback_delta_param(),
            qp(
                "limit",
                "The maximum number of metrics to return. Validated identically to /api/v1/query but ignored, as no series are returned.",
                false,
                integer_schema(),
                one(100),
            ),
            qp(
                "stats",
                "Validated identically to /api/v1/query but ignored, as no query statistics are returned.",
                false,
                string_schema(),
                one("all"),
            ),
            qp(
                "cost",
                "Validated identically to /api/v1/query but ignored, as the response is already a cost estimate. Any non-boolean value is an error.",
                false,
                boolean_schema(),
                one(true),
            ),
        ];
        params.extend(cost_limit_params());
        let description = "Estimates the resource cost of a range query without executing it. Requires the query-cost feature flag (--enable-feature=query-cost).";
        PathItem {
            get: Some(Operation {
                description,
                parameters: params,
                responses: responses_with_error_examples(
                    "QueryCostOutputBody",
                    query_cost_response_examples(),
                    error_response_examples(),
                    "Range query cost estimated successfully.",
                    "Error estimating range query cost.",
                ),
                ..op(
                    "query-range-cost",
                    "Estimate the cost of a range query",
                    "query",
                )
            }),
            post: Some(Operation {
                description,
                request_body: Some(form_request_body_with_examples(
                    "QueryRangeCostPostInputBody",
                    query_range_cost_post_examples(),
                    "Submit a range query for cost estimation. This endpoint accepts the same parameters as the GET version.",
                )),
                responses: responses_with_error_examples(
                    "QueryCostOutputBody",
                    query_cost_response_examples(),
                    error_response_examples(),
                    "Range query cost estimated successfully.",
                    "Error estimating range query cost.",
                ),
                ..op(
                    "query-range-cost-post",
                    "Estimate the cost of a range query",
                    "query",
                )
            }),
            ..Default::default()
        }
    }

    pub(super) fn query_exemplars_path(&self) -> PathItem {
        let params = vec![
            qp(
                "start",
                "Start timestamp for exemplars query.",
                false,
                timestamp_schema(),
                timestamp_examples(EXAMPLE_TIME_MINUS_1H),
            ),
            qp(
                "end",
                "End timestamp for exemplars query.",
                false,
                timestamp_schema(),
                timestamp_examples(EXAMPLE_TIME),
            ),
            qp(
                "query",
                "PromQL query to extract exemplars for.",
                true,
                string_schema(),
                one("prometheus_http_requests_total"),
            ),
        ];
        PathItem {
            get: Some(Operation {
                parameters: params,
                responses: responses_with_error_examples(
                    "QueryExemplarsOutputBody",
                    query_exemplars_response_examples(),
                    error_response_examples(),
                    "Exemplars retrieved successfully.",
                    "Error retrieving exemplars.",
                ),
                ..op("query-exemplars", "Query exemplars", "query")
            }),
            post: Some(Operation {
                request_body: Some(form_request_body_with_examples(
                    "QueryExemplarsPostInputBody",
                    query_exemplars_post_examples(),
                    "Submit an exemplars query. This endpoint accepts the same parameters as the GET version.",
                )),
                responses: responses_with_error_examples(
                    "QueryExemplarsOutputBody",
                    query_exemplars_response_examples(),
                    error_response_examples(),
                    "Exemplars query completed successfully.",
                    "Error processing exemplars query.",
                ),
                ..op("query-exemplars-post", "Query exemplars", "query")
            }),
            ..Default::default()
        }
    }

    pub(super) fn format_query_path(&self) -> PathItem {
        let params = vec![qp(
            "query",
            "PromQL expression to format.",
            true,
            string_schema(),
            one("sum(rate(http_requests_total[5m])) by (job)"),
        )];
        PathItem {
            get: Some(Operation {
                parameters: params,
                responses: responses_with_error_examples(
                    "FormatQueryOutputBody",
                    format_query_response_examples(),
                    error_response_examples(),
                    "Query formatted successfully.",
                    "Error formatting query.",
                ),
                ..op("format-query", "Format a PromQL query", "query")
            }),
            post: Some(Operation {
                request_body: Some(form_request_body_with_examples(
                    "FormatQueryPostInputBody",
                    format_query_post_examples(),
                    "Submit a PromQL query to format. This endpoint accepts the same parameters as the GET version.",
                )),
                responses: responses_with_error_examples(
                    "FormatQueryOutputBody",
                    format_query_response_examples(),
                    error_response_examples(),
                    "Query formatting completed successfully.",
                    "Error formatting query.",
                ),
                ..op("format-query-post", "Format a PromQL query", "query")
            }),
            ..Default::default()
        }
    }

    pub(super) fn parse_query_path(&self) -> PathItem {
        let params = vec![qp(
            "query",
            "PromQL expression to parse.",
            true,
            string_schema(),
            one("up{job=\"prometheus\"}"),
        )];
        PathItem {
            get: Some(Operation {
                parameters: params,
                responses: responses_with_error_examples(
                    "ParseQueryOutputBody",
                    parse_query_response_examples(),
                    error_response_examples(),
                    "Query parsed successfully.",
                    "Error parsing query.",
                ),
                ..op("parse-query", "Parse a PromQL query", "query")
            }),
            post: Some(Operation {
                request_body: Some(form_request_body_with_examples(
                    "ParseQueryPostInputBody",
                    parse_query_post_examples(),
                    "Submit a PromQL query to parse. This endpoint accepts the same parameters as the GET version.",
                )),
                responses: responses_with_error_examples(
                    "ParseQueryOutputBody",
                    parse_query_response_examples(),
                    error_response_examples(),
                    "Query parsed successfully via POST.",
                    "Error parsing query via POST.",
                ),
                ..op("parse-query-post", "Parse a PromQL query", "query")
            }),
            ..Default::default()
        }
    }

    pub(super) fn labels_path(&self) -> PathItem {
        let params = vec![
            qp(
                "start",
                "Start timestamp for label names query.",
                false,
                timestamp_schema(),
                timestamp_examples(EXAMPLE_TIME_MINUS_1H),
            ),
            qp(
                "end",
                "End timestamp for label names query.",
                false,
                timestamp_schema(),
                timestamp_examples(EXAMPLE_TIME),
            ),
            qp(
                "match[]",
                "Series selector argument.",
                false,
                string_array(),
                one(str_seq(&["{job=\"prometheus\"}"])),
            ),
            qp(
                "limit",
                "Maximum number of label names to return.",
                false,
                integer_schema(),
                one(100),
            ),
        ];
        PathItem {
            get: Some(Operation {
                parameters: params,
                responses: responses_with_error_examples(
                    "LabelsOutputBody",
                    labels_response_examples(),
                    error_response_examples(),
                    "Label names retrieved successfully.",
                    "Error retrieving label names.",
                ),
                ..op("labels", "Get label names", "labels")
            }),
            post: Some(Operation {
                request_body: Some(form_request_body_with_examples(
                    "LabelsPostInputBody",
                    labels_post_examples(),
                    "Submit a label names query. This endpoint accepts the same parameters as the GET version.",
                )),
                responses: responses_with_error_examples(
                    "LabelsOutputBody",
                    labels_response_examples(),
                    error_response_examples(),
                    "Label names retrieved successfully via POST.",
                    "Error retrieving label names via POST.",
                ),
                ..op("labels-post", "Get label names", "labels")
            }),
            ..Default::default()
        }
    }

    pub(super) fn label_values_path(&self) -> PathItem {
        let params = vec![
            path_param("name", "Label name.", string_schema()),
            qp(
                "start",
                "Start timestamp for label values query.",
                false,
                timestamp_schema(),
                timestamp_examples(EXAMPLE_TIME_MINUS_1H),
            ),
            qp(
                "end",
                "End timestamp for label values query.",
                false,
                timestamp_schema(),
                timestamp_examples(EXAMPLE_TIME),
            ),
            qp(
                "match[]",
                "Series selector argument.",
                false,
                string_array(),
                one(str_seq(&["{job=\"prometheus\"}"])),
            ),
            qp(
                "limit",
                "Maximum number of label values to return.",
                false,
                integer_schema(),
                one(1000),
            ),
        ];
        PathItem {
            get: Some(Operation {
                parameters: params,
                responses: responses_with_error_examples(
                    "LabelValuesOutputBody",
                    label_values_response_examples(),
                    error_response_examples(),
                    "Label values retrieved successfully.",
                    "Error retrieving label values.",
                ),
                ..op("label-values", "Get label values", "labels")
            }),
            ..Default::default()
        }
    }

    /// Go `commonSearchParams`: the query parameters shared by all three search endpoints.
    fn common_search_params() -> Vec<Parameter> {
        vec![
            qp(
                "fuzz_threshold",
                "Fuzzy threshold in the range 0-100. A value of 0 is the lowest fuzzy threshold.",
                false,
                integer_schema(),
                one(80),
            ),
            qp(
                "fuzz_alg",
                "Fuzzy algorithm. Supported values are subsequence (default) and jarowinkler.",
                false,
                enum_string_schema(FUZZ_ALGORITHMS),
                Vec::new(),
            ),
            qp(
                "case_sensitive",
                "Whether matching is case-sensitive.",
                false,
                boolean_schema(),
                one(true),
            ),
            qp(
                "sort_by",
                "Sort mode. Supported values are alpha and score.",
                false,
                enum_string_schema(&["alpha", "score"]),
                Vec::new(),
            ),
            qp(
                "sort_dir",
                "Sort direction. Only valid with sort_by=alpha. Supported values are asc and dsc.",
                false,
                enum_string_schema(&["asc", "dsc"]),
                Vec::new(),
            ),
            qp(
                "include_score",
                "Include the relevance score in each result.",
                false,
                boolean_schema(),
                one(true),
            ),
        ]
    }

    pub(super) fn search_metric_names_path(&self) -> PathItem {
        let mut params = vec![
            qp(
                "match[]",
                "Series selector argument used to scope metric discovery.",
                false,
                string_array(),
                one(str_seq(&["{job=\"prometheus\"}"])),
            ),
            qp(
                "search[]",
                "One or more search terms matched against metric names (OR logic).",
                false,
                string_array(),
                one(str_seq(&["http_req"])),
            ),
        ];
        params.extend(Self::common_search_params());
        params.extend([
            qp("include_metadata", "Include metric metadata in each result.", false, boolean_schema(), one(true)),
            qp("start", "Start timestamp for metric name search.", false, timestamp_schema(), timestamp_examples(EXAMPLE_TIME_MINUS_1H)),
            qp("end", "End timestamp for metric name search.", false, timestamp_schema(), timestamp_examples(EXAMPLE_TIME)),
            qp("limit", "Maximum number of metric names to return.", false, integer_schema_with_default(self.search_default_limit()), one(20)),
            qp(
                "batch_size",
                "Preferred number of results per NDJSON batch. Clamped to 1000 and the effective result limit.",
                false,
                integer_schema_with_default(DEFAULT_SEARCH_BATCH_SIZE),
                one(20),
            ),
        ]);
        PathItem {
            get: Some(Operation {
                parameters: params,
                responses: ndjson_responses_with_error_examples(
                    search_metric_names_response_examples(),
                    error_response_examples(),
                    "Metric names streamed successfully.",
                    "Error searching metric names.",
                ),
                ..op("search-metric-names", "Search metric names", "metadata")
            }),
            post: Some(Operation {
                request_body: Some(form_request_body_with_examples(
                    "SearchMetricNamesPostInputBody",
                    search_metric_names_post_examples(),
                    "Submit a metric name search. This endpoint accepts the same parameters as the GET version.",
                )),
                responses: ndjson_responses_with_error_examples(
                    search_metric_names_response_examples(),
                    error_response_examples(),
                    "Metric names streamed successfully via POST.",
                    "Error searching metric names via POST.",
                ),
                ..op(
                    "search-metric-names-post",
                    "Search metric names",
                    "metadata",
                )
            }),
            ..Default::default()
        }
    }

    pub(super) fn search_label_names_path(&self) -> PathItem {
        let mut params = vec![
            qp(
                "match[]",
                "Series selector argument used to scope label discovery.",
                false,
                string_array(),
                one(str_seq(&["{__name__=\"up\"}"])),
            ),
            qp(
                "search[]",
                "One or more search terms matched against label names (OR logic).",
                false,
                string_array(),
                one(str_seq(&["inst"])),
            ),
        ];
        params.extend(Self::common_search_params());
        params.extend([
            qp("start", "Start timestamp for label name search.", false, timestamp_schema(), timestamp_examples(EXAMPLE_TIME_MINUS_1H)),
            qp("end", "End timestamp for label name search.", false, timestamp_schema(), timestamp_examples(EXAMPLE_TIME)),
            qp("limit", "Maximum number of label names to return.", false, integer_schema_with_default(self.search_default_limit()), one(20)),
            qp(
                "batch_size",
                "Preferred number of results per NDJSON batch. Clamped to 1000 and the effective result limit.",
                false,
                integer_schema_with_default(DEFAULT_SEARCH_BATCH_SIZE),
                one(20),
            ),
        ]);
        PathItem {
            get: Some(Operation {
                parameters: params,
                responses: ndjson_responses_with_error_examples(
                    search_label_names_response_examples(),
                    error_response_examples(),
                    "Label names streamed successfully.",
                    "Error searching label names.",
                ),
                ..op("search-label-names", "Search label names", "labels")
            }),
            post: Some(Operation {
                request_body: Some(form_request_body_with_examples(
                    "SearchLabelNamesPostInputBody",
                    search_label_names_post_examples(),
                    "Submit a label name search. This endpoint accepts the same parameters as the GET version.",
                )),
                responses: ndjson_responses_with_error_examples(
                    search_label_names_response_examples(),
                    error_response_examples(),
                    "Label names streamed successfully via POST.",
                    "Error searching label names via POST.",
                ),
                ..op("search-label-names-post", "Search label names", "labels")
            }),
            ..Default::default()
        }
    }

    pub(super) fn search_label_values_path(&self) -> PathItem {
        let mut params = vec![
            qp(
                "label",
                "Label name whose values should be searched.",
                true,
                string_schema(),
                one("instance"),
            ),
            qp(
                "match[]",
                "Series selector argument used to scope label value discovery.",
                false,
                string_array(),
                one(str_seq(&["up"])),
            ),
            qp(
                "search[]",
                "One or more search terms matched against label values (OR logic).",
                false,
                string_array(),
                one(str_seq(&["909"])),
            ),
        ];
        params.extend(Self::common_search_params());
        params.extend([
            qp("start", "Start timestamp for label value search.", false, timestamp_schema(), timestamp_examples(EXAMPLE_TIME_MINUS_1H)),
            qp("end", "End timestamp for label value search.", false, timestamp_schema(), timestamp_examples(EXAMPLE_TIME)),
            qp("limit", "Maximum number of label values to return.", false, integer_schema_with_default(self.search_default_limit()), one(10)),
            qp(
                "batch_size",
                "Preferred number of results per NDJSON batch. Clamped to 1000 and the effective result limit.",
                false,
                integer_schema_with_default(DEFAULT_SEARCH_BATCH_SIZE),
                one(10),
            ),
        ]);
        PathItem {
            get: Some(Operation {
                parameters: params,
                responses: ndjson_responses_with_error_examples(
                    search_label_values_response_examples(),
                    error_response_examples(),
                    "Label values streamed successfully.",
                    "Error searching label values.",
                ),
                ..op("search-label-values", "Search label values", "labels")
            }),
            post: Some(Operation {
                request_body: Some(form_request_body_with_examples(
                    "SearchLabelValuesPostInputBody",
                    search_label_values_post_examples(),
                    "Submit a label value search. This endpoint accepts the same parameters as the GET version.",
                )),
                responses: ndjson_responses_with_error_examples(
                    search_label_values_response_examples(),
                    error_response_examples(),
                    "Label values streamed successfully via POST.",
                    "Error searching label values via POST.",
                ),
                ..op("search-label-values-post", "Search label values", "labels")
            }),
            ..Default::default()
        }
    }

    pub(super) fn series_path(&self) -> PathItem {
        let params = vec![
            qp(
                "start",
                "Start timestamp for series query.",
                false,
                timestamp_schema(),
                timestamp_examples(EXAMPLE_TIME_MINUS_1H),
            ),
            qp(
                "end",
                "End timestamp for series query.",
                false,
                timestamp_schema(),
                timestamp_examples(EXAMPLE_TIME),
            ),
            qp(
                "match[]",
                "Series selector argument.",
                true,
                string_array(),
                one(str_seq(&["{job=\"prometheus\"}"])),
            ),
            qp(
                "limit",
                "Maximum number of series to return.",
                false,
                integer_schema(),
                one(100),
            ),
        ];
        PathItem {
            get: Some(Operation {
                parameters: params,
                responses: responses_with_error_examples(
                    "SeriesOutputBody",
                    series_response_examples(),
                    error_response_examples(),
                    "Series returned matching the provided label matchers.",
                    "Error retrieving series.",
                ),
                ..op("series", "Find series by label matchers", "series")
            }),
            post: Some(Operation {
                request_body: Some(form_request_body_with_examples(
                    "SeriesPostInputBody",
                    series_post_examples(),
                    "Submit a series query. This endpoint accepts the same parameters as the GET version.",
                )),
                responses: responses_with_error_examples(
                    "SeriesOutputBody",
                    series_response_examples(),
                    error_response_examples(),
                    "Series returned matching the provided label matchers via POST.",
                    "Error retrieving series via POST.",
                ),
                ..op("series-post", "Find series by label matchers", "series")
            }),
            ..Default::default()
        }
    }

    pub(super) fn metadata_path(&self) -> PathItem {
        let params = vec![
            qp(
                "limit",
                "The maximum number of metrics to return.",
                false,
                integer_schema(),
                one(100),
            ),
            qp(
                "limit_per_metric",
                "The maximum number of metadata entries per metric.",
                false,
                integer_schema(),
                one(10),
            ),
            qp(
                "metric",
                "A metric name to filter metadata for.",
                false,
                string_schema(),
                one("http_requests_total"),
            ),
        ];
        get_only(
            Operation {
                parameters: params,
                ..op("get-metadata", "Get metadata", "metadata")
            },
            "MetadataOutputBody",
            metadata_response_examples(),
            "Metric metadata retrieved successfully.",
            "Error retrieving metadata.",
        )
    }

    pub(super) fn scrape_pools_path(&self) -> PathItem {
        get_only(
            op("get-scrape-pools", "Get scrape pools", "targets"),
            "ScrapePoolsOutputBody",
            scrape_pools_response_examples(),
            "Scrape pools retrieved successfully.",
            "Error retrieving scrape pools.",
        )
    }

    pub(super) fn scrape_pool_config_path(&self) -> PathItem {
        let params = vec![qp(
            "scrapePool",
            "Name of the scrape pool.",
            true,
            string_schema(),
            one("prometheus"),
        )];
        get_only(
            Operation {
                parameters: params,
                ..op(
                    "get-scrape-pool-config",
                    "Get scrape pool configuration",
                    "targets",
                )
            },
            "ScrapePoolConfigOutputBody",
            scrape_pool_config_response_examples(),
            "Scrape pool configuration retrieved successfully.",
            "Error retrieving scrape pool configuration.",
        )
    }

    pub(super) fn targets_path(&self) -> PathItem {
        let params = vec![
            qp(
                "scrapePool",
                "Filter targets by scrape pool name.",
                false,
                string_schema(),
                one("prometheus"),
            ),
            qp(
                "state",
                "Filter by state: active, dropped, or any.",
                false,
                string_schema(),
                one("active"),
            ),
        ];
        get_only(
            Operation {
                parameters: params,
                ..op("get-targets", "Get targets", "targets")
            },
            "TargetsOutputBody",
            targets_response_examples(),
            "Target discovery information retrieved successfully.",
            "Error retrieving targets.",
        )
    }

    pub(super) fn targets_metadata_path(&self) -> PathItem {
        let params = vec![
            qp(
                "match_target",
                "Label selector to filter targets.",
                false,
                string_schema(),
                one("{job=\"prometheus\"}"),
            ),
            qp(
                "metric",
                "Metric name to retrieve metadata for.",
                false,
                string_schema(),
                one("http_requests_total"),
            ),
            qp(
                "limit",
                "Maximum number of targets to match.",
                false,
                integer_schema(),
                one(10),
            ),
        ];
        get_only(
            Operation {
                parameters: params,
                ..op("get-targets-metadata", "Get targets metadata", "targets")
            },
            "TargetMetadataOutputBody",
            targets_metadata_response_examples(),
            "Target metadata retrieved successfully.",
            "Error retrieving target metadata.",
        )
    }

    pub(super) fn targets_relabel_steps_path(&self) -> PathItem {
        let params = vec![
            qp(
                "scrapePool",
                "Name of the scrape pool.",
                true,
                string_schema(),
                one("prometheus"),
            ),
            qp(
                "labels",
                "JSON-encoded labels to apply relabel rules to.",
                true,
                string_schema(),
                one("{\"__address__\":\"localhost:9090\",\"job\":\"prometheus\"}"),
            ),
        ];
        get_only(
            Operation {
                parameters: params,
                ..op(
                    "get-targets-relabel-steps",
                    "Get targets relabel steps",
                    "targets",
                )
            },
            "TargetRelabelStepsOutputBody",
            targets_relabel_steps_response_examples(),
            "Relabel steps retrieved successfully.",
            "Error retrieving relabel steps.",
        )
    }

    pub(super) fn rules_path(&self) -> PathItem {
        let params = vec![
            qp(
                "type",
                "Filter by rule type: alert or record.",
                false,
                string_schema(),
                one("alert"),
            ),
            qp(
                "rule_name[]",
                "Filter by rule name.",
                false,
                string_array(),
                one(str_seq(&["HighErrorRate"])),
            ),
            qp(
                "rule_group[]",
                "Filter by rule group name.",
                false,
                string_array(),
                one(str_seq(&["example_alerts"])),
            ),
            qp(
                "file[]",
                "Filter by file path.",
                false,
                string_array(),
                one(str_seq(&["/etc/prometheus/rules.yml"])),
            ),
            qp(
                "match[]",
                "Label matchers to filter rules.",
                false,
                string_array(),
                one(str_seq(&["{severity=\"critical\"}"])),
            ),
            qp(
                "exclude_alerts",
                "Exclude active alerts from response.",
                false,
                string_schema(),
                one("false"),
            ),
            qp(
                "group_limit",
                "Maximum number of rule groups to return.",
                false,
                integer_schema(),
                one(100),
            ),
            qp(
                "group_next_token",
                "Pagination token for next page.",
                false,
                string_schema(),
                one("abc123"),
            ),
        ];
        get_only(
            Operation {
                parameters: params,
                ..op("rules", "Get alerting and recording rules", "rules")
            },
            "RulesOutputBody",
            rules_response_examples(),
            "Rules retrieved successfully.",
            "Error retrieving rules.",
        )
    }

    pub(super) fn alerts_path(&self) -> PathItem {
        get_only(
            op("alerts", "Get active alerts", "alerts"),
            "AlertsOutputBody",
            alerts_response_examples(),
            "Active alerts retrieved successfully.",
            "Error retrieving alerts.",
        )
    }

    pub(super) fn alertmanagers_path(&self) -> PathItem {
        get_only(
            op("alertmanagers", "Get Alertmanager discovery", "alerts"),
            "AlertmanagersOutputBody",
            alertmanagers_response_examples(),
            "Alertmanager targets retrieved successfully.",
            "Error retrieving Alertmanager targets.",
        )
    }

    pub(super) fn status_config_path(&self) -> PathItem {
        get_only(
            op("get-status-config", "Get status config", "status"),
            "StatusConfigOutputBody",
            status_config_response_examples(),
            "Configuration retrieved successfully.",
            "Error retrieving configuration.",
        )
    }

    pub(super) fn status_runtime_info_path(&self) -> PathItem {
        get_only(
            op("get-status-runtimeinfo", "Get status runtimeinfo", "status"),
            "StatusRuntimeInfoOutputBody",
            status_runtime_info_response_examples(),
            "Runtime information retrieved successfully.",
            "Error retrieving runtime information.",
        )
    }

    pub(super) fn status_build_info_path(&self) -> PathItem {
        get_only(
            op("get-status-buildinfo", "Get status buildinfo", "status"),
            "StatusBuildInfoOutputBody",
            status_build_info_response_examples(),
            "Build information retrieved successfully.",
            "Error retrieving build information.",
        )
    }

    pub(super) fn status_flags_path(&self) -> PathItem {
        get_only(
            op("get-status-flags", "Get status flags", "status"),
            "StatusFlagsOutputBody",
            status_flags_response_examples(),
            "Command-line flags retrieved successfully.",
            "Error retrieving flags.",
        )
    }

    pub(super) fn status_tsdb_path(&self) -> PathItem {
        let params = vec![qp(
            "limit",
            "The maximum number of items to return per category.",
            false,
            integer_schema(),
            one(10),
        )];
        get_only(
            Operation {
                parameters: params,
                ..op("status-tsdb", "Get TSDB status", "status")
            },
            "StatusTSDBOutputBody",
            status_tsdb_response_examples(),
            "TSDB status retrieved successfully.",
            "Error retrieving TSDB status.",
        )
    }

    pub(super) fn status_tsdb_blocks_path(&self) -> PathItem {
        get_only(
            op(
                "status-tsdb-blocks",
                "Get TSDB blocks information",
                "status",
            ),
            "StatusTSDBBlocksOutputBody",
            status_tsdb_blocks_response_examples(),
            "TSDB blocks information retrieved successfully.",
            "Error retrieving TSDB blocks.",
        )
    }

    pub(super) fn status_wal_replay_path(&self) -> PathItem {
        get_only(
            op("get-status-walreplay", "Get status walreplay", "status"),
            "StatusWALReplayOutputBody",
            status_wal_replay_response_examples(),
            "WAL replay status retrieved successfully.",
            "Error retrieving WAL replay status.",
        )
    }

    pub(super) fn status_self_metrics_path(&self) -> PathItem {
        let params = vec![qp(
            "metric_name_pattern",
            "Regular expression filter for metric names (fully anchored, like PromQL label matchers). Only metric families whose names fully match the pattern are returned.",
            false,
            string_schema(),
            one("prometheus_tsdb_.*"),
        )];
        get_only(
            Operation {
                description: "Returns Prometheus' own instrumentation metrics from its internal client registry, as structured JSON. Supports optional regex filtering via the metric_name_pattern parameter.",
                parameters: params,
                ..op(
                    "get-status-self-metrics",
                    "Get Prometheus self-instrumentation metrics",
                    "status",
                )
            },
            "StatusSelfMetricsOutputBody",
            status_self_metrics_response_examples(),
            "Self metrics retrieved successfully.",
            "Error retrieving self metrics.",
        )
    }

    pub(super) fn admin_delete_series_path(&self) -> PathItem {
        let params = vec![
            qp(
                "match[]",
                "Series selectors to identify series to delete.",
                true,
                string_array(),
                one(str_seq(&["{__name__=~\"test.*\"}"])),
            ),
            qp(
                "start",
                "Start timestamp for deletion.",
                false,
                timestamp_schema(),
                timestamp_examples(EXAMPLE_TIME_MINUS_1H),
            ),
            qp(
                "end",
                "End timestamp for deletion.",
                false,
                timestamp_schema(),
                timestamp_examples(EXAMPLE_TIME),
            ),
        ];
        PathItem {
            post: Some(Operation {
                description: "Deletes data for a selection of series in a time range.",
                parameters: params.clone(),
                responses: responses_with_error_examples(
                    "DeleteSeriesOutputBody",
                    delete_series_response_examples(),
                    error_response_examples(),
                    "Series deleted successfully.",
                    "Error deleting series.",
                ),
                ..op(
                    "deleteSeriesPost",
                    "Delete series matching selectors",
                    "admin",
                )
            }),
            put: Some(Operation {
                description: "Deletes data for a selection of series in a time range using PUT method.",
                parameters: params,
                responses: responses_with_error_examples(
                    "DeleteSeriesOutputBody",
                    delete_series_response_examples(),
                    error_response_examples(),
                    "Series deleted successfully via PUT.",
                    "Error deleting series via PUT.",
                ),
                ..op(
                    "deleteSeriesPut",
                    "Delete series matching selectors via PUT",
                    "admin",
                )
            }),
            ..Default::default()
        }
    }

    pub(super) fn admin_clean_tombstones_path(&self) -> PathItem {
        PathItem {
            post: Some(Operation {
                description: "Removes deleted data from disk and cleans up existing tombstones.",
                responses: responses_with_error_examples(
                    "CleanTombstonesOutputBody",
                    clean_tombstones_response_examples(),
                    error_response_examples(),
                    "Tombstones cleaned successfully.",
                    "Error cleaning tombstones.",
                ),
                ..op(
                    "cleanTombstonesPost",
                    "Clean tombstones in the TSDB",
                    "admin",
                )
            }),
            put: Some(Operation {
                description: "Removes deleted data from disk and cleans up existing tombstones using PUT method.",
                responses: responses_with_error_examples(
                    "CleanTombstonesOutputBody",
                    clean_tombstones_response_examples(),
                    error_response_examples(),
                    "Tombstones cleaned successfully via PUT.",
                    "Error cleaning tombstones via PUT.",
                ),
                ..op(
                    "cleanTombstonesPut",
                    "Clean tombstones in the TSDB via PUT",
                    "admin",
                )
            }),
            ..Default::default()
        }
    }

    pub(super) fn admin_snapshot_path(&self) -> PathItem {
        let params = vec![qp(
            "skip_head",
            "If true, do not snapshot data in the head block.",
            false,
            string_schema(),
            one("false"),
        )];
        PathItem {
            post: Some(Operation {
                description: "Creates a snapshot of all current data.",
                parameters: params.clone(),
                responses: responses_with_error_examples(
                    "SnapshotOutputBody",
                    snapshot_response_examples(),
                    error_response_examples(),
                    "Snapshot created successfully.",
                    "Error creating snapshot.",
                ),
                ..op("snapshotPost", "Create a snapshot of the TSDB", "admin")
            }),
            put: Some(Operation {
                description: "Creates a snapshot of all current data using PUT method.",
                parameters: params,
                responses: responses_with_error_examples(
                    "SnapshotOutputBody",
                    snapshot_response_examples(),
                    error_response_examples(),
                    "Snapshot created successfully via PUT.",
                    "Error creating snapshot via PUT.",
                ),
                ..op(
                    "snapshotPut",
                    "Create a snapshot of the TSDB via PUT",
                    "admin",
                )
            }),
            ..Default::default()
        }
    }

    pub(super) fn remote_read_path(&self) -> PathItem {
        PathItem {
            post: Some(Operation {
                description: "Prometheus remote read endpoint for federated queries. Accepts and returns Protocol Buffer encoded data.",
                responses: responses_no_content(),
                ..op("remoteRead", "Remote read endpoint", "remote")
            }),
            ..Default::default()
        }
    }

    pub(super) fn remote_write_path(&self) -> PathItem {
        PathItem {
            post: Some(Operation {
                description: "Prometheus remote write endpoint for sending metrics. Accepts Protocol Buffer encoded write requests.",
                responses: responses_no_content(),
                ..op("remoteWrite", "Remote write endpoint", "remote")
            }),
            ..Default::default()
        }
    }

    pub(super) fn otlp_write_path(&self) -> PathItem {
        PathItem {
            post: Some(Operation {
                description: "OpenTelemetry Protocol metrics ingestion endpoint. Accepts OTLP/HTTP metrics in Protocol Buffer format.",
                responses: responses_no_content(),
                ..op("otlpWrite", "OTLP metrics write endpoint", "otlp")
            }),
            ..Default::default()
        }
    }

    pub(super) fn notifications_path(&self) -> PathItem {
        get_only(
            op("get-notifications", "Get notifications", "notifications"),
            "NotificationsOutputBody",
            notifications_response_examples(),
            "Notifications retrieved successfully.",
            "Error retrieving notifications.",
        )
    }

    /// The `/notifications/live` endpoint. It uses OpenAPI 3.2's `itemSchema` to document the SSE stream, so
    /// the OpenAPI 3.1 document leaves it out.
    pub(super) fn notifications_live_path(&self) -> PathItem {
        let item_schema = Schema {
            typ: "object",
            title: "Server Sent Event Message".into(),
            description:
                "A single SSE message. The data field contains a JSON-encoded Notification object."
                    .into(),
            properties: vec![(
                "data".into(),
                Schema {
                    typ: "string",
                    description: "SSE data field containing JSON-encoded notification.".into(),
                    content_media_type: "application/json",
                    content_schema: Some(Box::new(schema_ref("#/components/schemas/Notification"))),
                    ..Default::default()
                },
            )],
            required: vec!["data"],
            additional_properties: Some(AdditionalProperties::Bool(false)),
            ..Default::default()
        };
        let content = vec![(
            "text/event-stream",
            MediaType {
                item_schema: Some(item_schema),
                examples: Some(notification_live_examples()),
                ..Default::default()
            },
        )];
        PathItem {
            get: Some(Operation {
                description: "Subscribe to real-time server notifications using SSE. Each event contains a JSON-encoded Notification object in the data field.",
                responses: vec![
                    (
                        "200",
                        Response {
                            description: "Server-sent events stream established.",
                            content,
                        },
                    ),
                    ("default", error_response()),
                ],
                ..op(
                    "notifications-live",
                    "Stream live notifications via Server-Sent Events",
                    "notifications",
                )
            }),
            ..Default::default()
        }
    }

    pub(super) fn features_path(&self) -> PathItem {
        get_only(
            op("get-features", "Get features", "features"),
            "FeaturesOutputBody",
            features_response_examples(),
            "Feature flags retrieved successfully.",
            "Error retrieving features.",
        )
    }
}

/// A path item with only a GET operation answering with `responsesWithErrorExamples`.
fn get_only(
    o: Operation,
    ok_schema: &str,
    examples: Examples,
    ok: &'static str,
    err: &'static str,
) -> PathItem {
    PathItem {
        get: Some(Operation {
            responses: responses_with_error_examples(
                ok_schema,
                examples,
                error_response_examples(),
                ok,
                err,
            ),
            ..o
        }),
        ..Default::default()
    }
}
