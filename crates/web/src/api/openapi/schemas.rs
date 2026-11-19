//! Port of `web/api/v1/openapi_schemas.go`: the component schemas of request and response types.

use std::borrow::Cow;

use super::emit::{Node, Str, go_map, str_seq};
use super::model::*;
use super::{DEFAULT_SEARCH_BATCH_SIZE, FUZZ_ALGORITHMS, OpenApiBuilder};

/// Go `&base.DynamicValue[*base.SchemaProxy, bool]{N: 1, B: false}`.
fn closed() -> Option<AdditionalProperties> {
    Some(AdditionalProperties::Bool(false))
}

/// Go `orderedmap` of properties.
fn props(pairs: Vec<(&'static str, Schema)>) -> Vec<(Str, Schema)> {
    pairs
        .into_iter()
        .map(|(k, v)| (Cow::Borrowed(k), v))
        .collect()
}

/// A closed object schema (the common shape of the component schemas).
fn object(
    description: &'static str,
    required: Vec<&'static str>,
    properties: Vec<(&'static str, Schema)>,
) -> Schema {
    Schema {
        typ: "object",
        description: description.into(),
        additional_properties: closed(),
        required,
        properties: props(properties),
        ..Default::default()
    }
}

fn number() -> Schema {
    schema_from_type("number")
}

/// `oneOf: [number, other]`.
fn number_or(other: Schema) -> Schema {
    Schema {
        one_of: vec![number(), other],
        ..Default::default()
    }
}

/// An array of exactly two items.
fn pair_of(items: Schema) -> Schema {
    Schema {
        min_items: Some(2),
        max_items: Some(2),
        ..array_of(items)
    }
}

const FORM_TIMEOUT_DESC: &str = "Form field: Evaluation timeout. Optional. Must be positive. Defaults to and is capped by the effective server-side maximum: global.query_max_duration when the query-cost feature is enabled and the value is set, otherwise the -query.timeout flag.";
const FORM_MAX_SERIES_DESC: &str = "Form field: Lower the maximum number of series this query may load. Optional. Honored only with the query-cost feature flag (--enable-feature=query-cost). It may only lower the query_max_series ceiling, never raise it: a higher value is rejected.";
const FORM_MAX_SAMPLES_READ_DESC: &str = "Form field: Lower the maximum number of samples this query may read. Optional. Honored only with the query-cost feature flag (--enable-feature=query-cost). It may only lower the query_max_samples_read ceiling, never raise it: a higher value is rejected.";
const FORM_MAX_QUERY_DURATION_DESC: &str = "Form field: Lower the maximum execution duration of this query. Optional. Honored only with the query-cost feature flag (--enable-feature=query-cost). It may only lower the effective duration ceiling (query_max_duration when set, otherwise the -query.timeout flag), never raise it: a higher value is rejected.";
const FORM_STATS_DESC: &str = "Form field: When provided, include query statistics in the response (the special value 'all' enables more comprehensive statistics).";
const FORM_COST_DESC: &str = "Form field: When true, include an estimated-vs-actual cost comparison in the response. Any non-boolean value is an error. Requires the query-cost feature flag (--enable-feature=query-cost).";
const FORM_COST_LIMIT_DESC: &str = "Form field: The maximum number of metrics to return. Validated identically to /api/v1/query but ignored, as no series are returned.";
const FORM_COST_STATS_DESC: &str = "Form field: Validated identically to /api/v1/query but ignored, as no query statistics are returned.";
const FORM_COST_COST_DESC: &str = "Form field: Validated identically to /api/v1/query but ignored, as the response is already a cost estimate. Any non-boolean value is an error.";
const FORM_LOOKBACK_DESC: &str =
    "Form field: Override the lookback period for this query (optional).";

/// The `max_series`, `max_samples_read` and `max_query_duration` form fields.
fn form_cost_limits() -> [(&'static str, Schema); 3] {
    [
        (
            "max_series",
            integer_schema_with_description_and_example(FORM_MAX_SERIES_DESC, 1000),
        ),
        (
            "max_samples_read",
            integer_schema_with_description_and_example(FORM_MAX_SAMPLES_READ_DESC, 100000),
        ),
        (
            "max_query_duration",
            string_schema_with_description_and_example(FORM_MAX_QUERY_DURATION_DESC, "30s"),
        ),
    ]
}

impl OpenApiBuilder {
    /// Go `buildComponents` (its schemas).
    pub(super) fn build_component_schemas(&self) -> Vec<(&'static str, Schema)> {
        vec![
            // Core schemas.
            ("Error", error_schema()),
            ("Labels", labels_schema()),
            // Query schemas.
            (
                "QueryOutputBody",
                response_body_schema("QueryData", "Response body for instant query."),
            ),
            (
                "QueryRangeOutputBody",
                response_body_schema("QueryData", "Response body for range query."),
            ),
            ("QueryPostInputBody", query_post_input_body_schema()),
            (
                "QueryRangePostInputBody",
                query_range_post_input_body_schema(),
            ),
            ("QueryExemplarsOutputBody", simple_response_body_schema()),
            (
                "QueryExemplarsPostInputBody",
                query_exemplars_post_input_body_schema(),
            ),
            ("FormatQueryOutputBody", format_query_output_body_schema()),
            (
                "FormatQueryPostInputBody",
                format_query_post_input_body_schema(),
            ),
            ("ParseQueryOutputBody", simple_response_body_schema()),
            (
                "ParseQueryPostInputBody",
                parse_query_post_input_body_schema(),
            ),
            ("QueryData", query_data_schema()),
            ("QueryStats", query_stats_schema()),
            ("CostEstimate", cost_estimate_schema()),
            ("ActualCost", actual_cost_schema()),
            ("QueryCostComparison", query_cost_comparison_schema()),
            ("QueryCostData", query_cost_data_schema()),
            (
                "QueryCostOutputBody",
                response_body_schema("QueryCostData", "Response body for query cost estimation."),
            ),
            (
                "QueryCostPostInputBody",
                query_cost_post_input_body_schema(),
            ),
            (
                "QueryRangeCostPostInputBody",
                query_range_cost_post_input_body_schema(),
            ),
            ("FloatSample", float_sample_schema()),
            ("HistogramSample", histogram_sample_schema()),
            ("FloatSeries", float_series_schema()),
            ("HistogramSeries", histogram_series_schema()),
            ("HistogramValue", histogram_value_schema()),
            // Label schemas.
            ("LabelsOutputBody", string_array_response_body_schema()),
            ("LabelsPostInputBody", labels_post_input_body_schema()),
            ("LabelValuesOutputBody", string_array_response_body_schema()),
            (
                "SearchMetricNamesPostInputBody",
                self.search_metric_names_post_input_body_schema(),
            ),
            (
                "SearchLabelNamesPostInputBody",
                self.search_label_names_post_input_body_schema(),
            ),
            (
                "SearchLabelValuesPostInputBody",
                self.search_label_values_post_input_body_schema(),
            ),
            // Series schemas.
            ("SeriesOutputBody", labels_array_response_body_schema()),
            ("SeriesPostInputBody", series_post_input_body_schema()),
            // Metadata schemas.
            ("Metadata", metadata_schema()),
            ("MetadataOutputBody", metadata_output_body_schema()),
            ("MetricMetadata", metric_metadata_schema()),
            // Target schemas.
            ("Target", target_schema()),
            ("DroppedTarget", dropped_target_schema()),
            ("TargetDiscovery", target_discovery_schema()),
            (
                "TargetsOutputBody",
                response_body_schema("TargetDiscovery", "Response body for targets endpoint."),
            ),
            (
                "TargetMetadataOutputBody",
                metric_metadata_array_response_body_schema(),
            ),
            ("ScrapePoolsDiscovery", scrape_pools_discovery_schema()),
            (
                "ScrapePoolsOutputBody",
                response_body_schema(
                    "ScrapePoolsDiscovery",
                    "Response body for scrape pools endpoint.",
                ),
            ),
            ("ScrapePoolConfigData", scrape_pool_config_data_schema()),
            (
                "ScrapePoolConfigOutputBody",
                response_body_schema(
                    "ScrapePoolConfigData",
                    "Response body for scrape pool config endpoint.",
                ),
            ),
            // Relabel schemas.
            ("Config", config_schema()),
            ("RelabelStep", relabel_step_schema()),
            ("RelabelStepsResponse", relabel_steps_response_schema()),
            (
                "TargetRelabelStepsOutputBody",
                response_body_schema(
                    "RelabelStepsResponse",
                    "Response body for target relabel steps endpoint.",
                ),
            ),
            // Rule schemas.
            ("RuleGroup", rule_group_schema()),
            ("RuleDiscovery", rule_discovery_schema()),
            (
                "RulesOutputBody",
                response_body_schema("RuleDiscovery", "Response body for rules endpoint."),
            ),
            // Alert schemas.
            ("Alert", alert_schema()),
            ("AlertDiscovery", alert_discovery_schema()),
            (
                "AlertsOutputBody",
                response_body_schema("AlertDiscovery", "Response body for alerts endpoint."),
            ),
            ("AlertmanagerTarget", alertmanager_target_schema()),
            ("AlertmanagerDiscovery", alertmanager_discovery_schema()),
            (
                "AlertmanagersOutputBody",
                response_body_schema(
                    "AlertmanagerDiscovery",
                    "Response body for alertmanagers endpoint.",
                ),
            ),
            // Status schemas.
            ("StatusConfigData", status_config_data_schema()),
            (
                "StatusConfigOutputBody",
                response_body_schema(
                    "StatusConfigData",
                    "Response body for status config endpoint.",
                ),
            ),
            ("RuntimeInfo", runtime_info_schema()),
            (
                "StatusRuntimeInfoOutputBody",
                response_body_schema(
                    "RuntimeInfo",
                    "Response body for status runtime info endpoint.",
                ),
            ),
            ("PrometheusVersion", prometheus_version_schema()),
            (
                "StatusBuildInfoOutputBody",
                response_body_schema(
                    "PrometheusVersion",
                    "Response body for status build info endpoint.",
                ),
            ),
            ("StatusFlagsOutputBody", status_flags_output_body_schema()),
            ("HeadStats", head_stats_schema()),
            ("TSDBStat", tsdb_stat_schema()),
            ("TSDBStatus", tsdb_status_schema()),
            (
                "StatusTSDBOutputBody",
                response_body_schema("TSDBStatus", "Response body for status TSDB endpoint."),
            ),
            ("BlockDesc", block_desc_schema()),
            ("BlockStats", block_stats_schema()),
            ("BlockMetaCompaction", block_meta_compaction_schema()),
            ("BlockMeta", block_meta_schema()),
            ("StatusTSDBBlocksData", status_tsdb_blocks_data_schema()),
            (
                "StatusTSDBBlocksOutputBody",
                response_body_schema(
                    "StatusTSDBBlocksData",
                    "Response body for status TSDB blocks endpoint.",
                ),
            ),
            ("StatusWALReplayData", status_wal_replay_data_schema()),
            (
                "StatusWALReplayOutputBody",
                response_body_schema(
                    "StatusWALReplayData",
                    "Response body for status WAL replay endpoint.",
                ),
            ),
            ("StatusSelfMetricsOutputBody", simple_response_body_schema()),
            // Admin schemas.
            ("DeleteSeriesOutputBody", status_only_response_body_schema()),
            (
                "CleanTombstonesOutputBody",
                status_only_response_body_schema(),
            ),
            ("DataStruct", data_struct_schema()),
            (
                "SnapshotOutputBody",
                response_body_schema("DataStruct", "Response body for snapshot endpoint."),
            ),
            // Notification schemas.
            ("Notification", notification_schema()),
            (
                "NotificationsOutputBody",
                notification_array_response_body_schema(),
            ),
            // Features schema.
            ("FeaturesOutputBody", simple_response_body_schema()),
        ]
    }

    /// Go `commonSearchPostProps`: the properties shared by all three search POST bodies.
    fn common_search_post_props(&self) -> Vec<(&'static str, Schema)> {
        vec![
            (
                "fuzz_threshold",
                integer_schema_with_description_and_example(
                    "Form field: Fuzzy threshold in the range 0-100. Default is 0, the lowest fuzzy threshold.",
                    80,
                ),
            ),
            (
                "fuzz_alg",
                enum_string_schema_with_description(
                    "Form field: Fuzzy algorithm. Supported values are subsequence (default) and jarowinkler.",
                    FUZZ_ALGORITHMS,
                ),
            ),
            (
                "case_sensitive",
                boolean_schema_with_description("Form field: Whether matching is case-sensitive."),
            ),
            (
                "sort_by",
                enum_string_schema_with_description(
                    "Form field: Sort mode. Supported values are alpha and score. If unset, results are returned in natural order.",
                    &["alpha", "score"],
                ),
            ),
            (
                "sort_dir",
                enum_string_schema_with_description(
                    "Form field: Sort direction. Only valid with sort_by=alpha. Supported values are asc and dsc.",
                    &["asc", "dsc"],
                ),
            ),
            (
                "include_score",
                boolean_schema_with_description(
                    "Form field: Include the relevance score in each result record.",
                ),
            ),
            (
                "start",
                string_schema_with_description_and_example(
                    "Form field: The start time of the query.",
                    "2026-01-02T12:37:00.000Z",
                ),
            ),
            (
                "end",
                string_schema_with_description_and_example(
                    "Form field: The end time of the query.",
                    "2026-01-02T13:37:00.000Z",
                ),
            ),
            (
                "limit",
                integer_schema_with_description_default_and_example(
                    "Form field: The maximum number of results to return.",
                    self.search_default_limit(),
                    20,
                ),
            ),
            (
                "batch_size",
                integer_schema_with_description_default_and_example(
                    "Form field: Preferred number of results per NDJSON batch. Clamped to 1000 and the effective result limit.",
                    DEFAULT_SEARCH_BATCH_SIZE,
                    20,
                ),
            ),
        ]
    }

    fn search_metric_names_post_input_body_schema(&self) -> Schema {
        let mut p = vec![
            (
                "match[]",
                string_array_schema_with_description_and_example(
                    "Form field: Series selector argument used to scope metric discovery.",
                    str_seq(&["{job=\"prometheus\"}"]),
                ),
            ),
            (
                "search[]",
                string_array_schema_with_description_and_example(
                    "Form field: One or more search terms matched against metric names (OR logic).",
                    str_seq(&["http_req"]),
                ),
            ),
        ];
        p.extend(self.common_search_post_props());
        p.push((
            "include_metadata",
            boolean_schema_with_description("Form field: Include metric metadata in each result."),
        ));
        object("POST request body for metric name search.", Vec::new(), p)
    }

    fn search_label_names_post_input_body_schema(&self) -> Schema {
        let mut p = vec![
            (
                "match[]",
                string_array_schema_with_description_and_example(
                    "Form field: Series selector argument used to scope label discovery.",
                    str_seq(&["{__name__=\"up\"}"]),
                ),
            ),
            (
                "search[]",
                string_array_schema_with_description_and_example(
                    "Form field: One or more search terms matched against label names (OR logic).",
                    str_seq(&["inst"]),
                ),
            ),
        ];
        p.extend(self.common_search_post_props());
        object("POST request body for label name search.", Vec::new(), p)
    }

    fn search_label_values_post_input_body_schema(&self) -> Schema {
        let mut p = vec![
            (
                "label",
                string_schema_with_description_and_example(
                    "Form field: Label name whose values should be searched.",
                    "instance",
                ),
            ),
            (
                "match[]",
                string_array_schema_with_description_and_example(
                    "Form field: Series selector argument used to scope label value discovery.",
                    str_seq(&["up"]),
                ),
            ),
            (
                "search[]",
                string_array_schema_with_description_and_example(
                    "Form field: One or more search terms matched against label values (OR logic).",
                    str_seq(&["909"]),
                ),
            ),
        ];
        p.extend(self.common_search_post_props());
        object(
            "POST request body for label value search.",
            vec!["label"],
            p,
        )
    }
}

fn error_schema() -> Schema {
    object(
        "Error response.",
        vec!["status", "errorType", "error"],
        vec![
            ("status", status_schema()),
            (
                "errorType",
                string_schema_with_description_and_example(
                    "Type of error that occurred.",
                    "bad_data",
                ),
            ),
            (
                "error",
                string_schema_with_description_and_example(
                    "Human-readable error message.",
                    "invalid parameter",
                ),
            ),
        ],
    )
}

fn labels_schema() -> Schema {
    Schema {
        typ: "object",
        description: "Label set represented as a key-value map.".into(),
        additional_properties: Some(AdditionalProperties::Bool(true)),
        ..Default::default()
    }
}

/// Go `responseBodySchema` (and `refResponseBodySchema`).
fn response_body_schema(data_schema_ref: &str, description: &'static str) -> Schema {
    object(
        description,
        vec!["status", "data"],
        vec![
            ("status", status_schema()),
            ("data", component_ref(data_schema_ref)),
            ("warnings", warnings_schema()),
            ("infos", infos_schema()),
        ],
    )
}

/// A response body whose data is `data`.
fn data_response_body_schema(description: &'static str, data: Schema) -> Schema {
    object(
        description,
        vec!["status", "data"],
        vec![
            ("status", status_schema()),
            ("data", data),
            ("warnings", warnings_schema()),
            ("infos", infos_schema()),
        ],
    )
}

fn simple_response_body_schema() -> Schema {
    data_response_body_schema(
        "Generic response body.",
        Schema {
            description: "Response data (structure varies by endpoint).".into(),
            example: Some(go_map(vec![("result", Node::from("ok"))])),
            ..Default::default()
        },
    )
}

fn status_only_response_body_schema() -> Schema {
    object(
        "Response body containing only status.",
        vec!["status"],
        vec![
            ("status", status_schema()),
            ("warnings", warnings_schema()),
            ("infos", infos_schema()),
        ],
    )
}

fn string_array_response_body_schema() -> Schema {
    data_response_body_schema(
        "Response body with an array of strings.",
        Schema {
            example: Some(str_seq(&["__name__", "job", "instance"])),
            ..array_of(string_schema())
        },
    )
}

fn labels_array_response_body_schema() -> Schema {
    data_response_body_schema(
        "Response body with an array of label sets.",
        Schema {
            example: Some(Node::Seq(vec![str_map(&[
                ("__name__", "up"),
                ("job", "prometheus"),
                ("instance", "localhost:9090"),
            ])])),
            ..array_of(component_ref("Labels"))
        },
    )
}

fn metric_metadata_array_response_body_schema() -> Schema {
    data_response_body_schema(
        "Response body with an array of metric metadata.",
        Schema {
            example: Some(Node::Seq(vec![go_map(vec![
                (
                    "target",
                    str_map(&[("instance", "localhost:9090"), ("job", "prometheus")]),
                ),
                ("metric", Node::from("up")),
                ("type", Node::from("gauge")),
                (
                    "help",
                    Node::from("The current health status of the target"),
                ),
                ("unit", Node::from("")),
            ])])),
            ..array_of(component_ref("MetricMetadata"))
        },
    )
}

fn notification_array_response_body_schema() -> Schema {
    data_response_body_schema(
        "Response body with an array of notifications.",
        Schema {
            example: Some(Node::Seq(vec![go_map(vec![
                ("text", Node::from("Server is running")),
                ("date", Node::from("2023-07-21T20:00:00.000Z")),
                ("active", Node::Bool(true)),
            ])])),
            ..array_of(component_ref("Notification"))
        },
    )
}

fn float_sample_schema() -> Schema {
    object(
        "A sample with a float value.",
        vec!["metric", "value"],
        vec![
            ("metric", component_ref("Labels")),
            (
                "value",
                Schema {
                    description: "Timestamp and float value as [unixTimestamp, stringValue]."
                        .into(),
                    example: Some(Node::Seq(vec![Node::Int(1767436620), Node::from("1")])),
                    ..pair_of(number_or(string_schema()))
                },
            ),
        ],
    )
}

fn histogram_value_schema() -> Schema {
    object(
        "Native histogram value representation.",
        vec!["count", "sum"],
        vec![
            (
                "count",
                string_schema_with_description("Total count of observations."),
            ),
            (
                "sum",
                string_schema_with_description("Sum of all observed values."),
            ),
            (
                "buckets",
                Schema {
                    description: "Histogram buckets as [boundary_rule, lower, upper, count]."
                        .into(),
                    ..array_of(array_of(number_or(string_schema())))
                },
            ),
        ],
    )
}

fn histogram_sample_schema() -> Schema {
    object(
        "A sample with a native histogram value.",
        vec!["metric", "histogram"],
        vec![
            ("metric", component_ref("Labels")),
            (
                "histogram",
                Schema {
                    description:
                        "Timestamp and histogram value as [unixTimestamp, histogramObject].".into(),
                    example: Some(Node::Seq(vec![
                        Node::Int(1767436620),
                        go_map(vec![
                            ("count", Node::from("60")),
                            ("sum", Node::from("120")),
                            ("buckets", Node::Seq(Vec::new())),
                        ]),
                    ])),
                    ..pair_of(number_or(component_ref("HistogramValue")))
                },
            ),
        ],
    )
}

fn float_series_schema() -> Schema {
    object(
        "A time series with float values.",
        vec!["metric", "values"],
        vec![
            ("metric", component_ref("Labels")),
            (
                "values",
                Schema {
                    description: "Array of [timestamp, stringValue] pairs for float values.".into(),
                    ..array_of(pair_of(number_or(string_schema())))
                },
            ),
        ],
    )
}

fn histogram_series_schema() -> Schema {
    object(
        "A time series with native histogram values.",
        vec!["metric", "histograms"],
        vec![
            ("metric", component_ref("Labels")),
            (
                "histograms",
                Schema {
                    description:
                        "Array of [timestamp, histogramObject] pairs for histogram values.".into(),
                    ..array_of(pair_of(number_or(component_ref("HistogramValue"))))
                },
            ),
        ],
    )
}

/// One `resultType` variant of `QueryData`.
fn query_result_variant(result_type: &'static str, result: Schema) -> Schema {
    Schema {
        typ: "object",
        required: vec!["resultType", "result"],
        additional_properties: closed(),
        properties: props(vec![
            ("resultType", string_schema_with_const_value(result_type)),
            ("result", result),
            ("stats", component_ref("QueryStats")),
            ("cost", component_ref("QueryCostComparison")),
        ]),
        ..Default::default()
    }
}

fn query_data_schema() -> Schema {
    let vector = Schema {
        description: "Array of samples (either float or histogram).".into(),
        ..array_of(Schema {
            any_of: vec![
                component_ref("FloatSample"),
                component_ref("HistogramSample"),
            ],
            ..Default::default()
        })
    };
    let matrix = Schema {
        description: "Array of time series (either float or histogram).".into(),
        ..array_of(Schema {
            any_of: vec![
                component_ref("FloatSeries"),
                component_ref("HistogramSeries"),
            ],
            ..Default::default()
        })
    };
    let scalar = Schema {
        description: "Scalar value as [timestamp, stringValue].".into(),
        ..pair_of(number_or(string_schema()))
    };
    let string = Schema {
        description: "String value as [timestamp, stringValue].".into(),
        ..pair_of(string_schema())
    };
    Schema {
        description: "Query result data. The structure of 'result' depends on 'resultType'.".into(),
        any_of: vec![
            query_result_variant("vector", vector),
            query_result_variant("matrix", matrix),
            query_result_variant("scalar", scalar),
            query_result_variant("string", string),
        ],
        example: Some(go_map(vec![
            ("resultType", Node::from("vector")),
            (
                "result",
                Node::Seq(vec![go_map(vec![
                    (
                        "metric",
                        str_map(&[("__name__", "up"), ("job", "prometheus")]),
                    ),
                    (
                        "value",
                        Node::Seq(vec![Node::Int(1627845600), Node::from("1")]),
                    ),
                ])]),
            ),
        ])),
        ..Default::default()
    }
}

fn described(typ: &'static str, description: &'static str) -> Schema {
    Schema {
        typ,
        description: description.into(),
        ..Default::default()
    }
}

fn per_step(description: &'static str) -> Schema {
    Schema {
        description: description.into(),
        ..array_of(Schema {
            description: "Timestamp and sample count as [timestamp, count].".into(),
            ..pair_of(number())
        })
    }
}

fn query_stats_schema() -> Schema {
    let timings = props(vec![
        (
            "evalTotalTime",
            described("number", "Total evaluation time in seconds."),
        ),
        (
            "resultSortTime",
            described("number", "Time spent sorting results in seconds."),
        ),
        (
            "queryPreparationTime",
            described("number", "Query preparation time in seconds."),
        ),
        (
            "innerEvalTime",
            described("number", "Inner evaluation time in seconds."),
        ),
        (
            "execQueueTime",
            described("number", "Execution queue wait time in seconds."),
        ),
        (
            "execTotalTime",
            described("number", "Total execution time in seconds."),
        ),
    ]);
    let samples = props(vec![
        (
            "totalQueryableSamples",
            described(
                "integer",
                "Logical underlying sample consumption, including subqueries but excluding their materialized results. Overlapping windows can count the same sample repeatedly.",
            ),
        ),
        (
            "peakSamples",
            described("integer", "Peak number of samples in memory."),
        ),
        (
            "totalQueryableSamplesPerStep",
            per_step(
                "Logical underlying sample consumption per step (only included with stats=all).",
            ),
        ),
        (
            "seriesTouched",
            integer_schema_with_description(
                "Number of series consumed during execution, summed across selectors. Repeated selectors can count the same series more than once.",
            ),
        ),
        (
            "samplesRead",
            described(
                "integer",
                "Evaluator input samples, counting only new points across overlapping range-vector windows. With query-cost enabled, includes all evaluated subquery steps; otherwise only consumed windows. Not physical storage I/O.",
            ),
        ),
        (
            "samplesReadPerStep",
            per_step(
                "Samples read per step (only included with stats=all when per-step stats enabled).",
            ),
        ),
    ]);
    Schema {
        typ: "object",
        description:
            "Query execution statistics (included when the stats query parameter is provided)."
                .into(),
        properties: props(vec![
            (
                "timings",
                Schema {
                    typ: "object",
                    properties: timings,
                    ..Default::default()
                },
            ),
            (
                "samples",
                Schema {
                    typ: "object",
                    properties: samples,
                    ..Default::default()
                },
            ),
        ]),
        ..Default::default()
    }
}

/// A cost estimate computed without executing the query.
fn cost_estimate_schema() -> Schema {
    object(
        "Estimated storage input. Values may overestimate or underestimate actual cost; warnings identify incomplete estimates.",
        vec!["seriesTouched", "samplesRead"],
        vec![
            (
                "seriesTouched",
                integer_schema_with_description(
                    "Estimated series reads, summed across explicit selectors. Shared series may be counted repeatedly; implicit info() reads are omitted with a warning.",
                ),
            ),
            (
                "samplesRead",
                integer_schema_with_description(
                    "Storage input in sample units: one per float, weighted by in-memory size for native histograms.",
                ),
            ),
        ],
    )
}

/// The resource cost measured while the query ran.
fn actual_cost_schema() -> Schema {
    object(
        "Actual resource cost measured while the query executed.",
        vec!["seriesTouched", "samplesRead"],
        vec![
            (
                "seriesTouched",
                integer_schema_with_description(
                    "Number of series the query loaded, summed per selector. Series shared between selectors may be counted more than once, so this is an upper bound.",
                ),
            ),
            (
                "samplesRead",
                integer_schema_with_description(
                    "Samples read by the evaluator in the engine's sample units; the same quantity as the samplesRead query statistic.",
                ),
            ),
            (
                "peakSamples",
                integer_schema_with_description(
                    "Highest number of samples held in memory at once. Omitted when zero.",
                ),
            ),
        ],
    )
}

fn query_cost_comparison_schema() -> Schema {
    object(
        "Estimated storage input paired with the actual cost measured during execution. The estimated cost has no peakSamples; the actual cost includes it. Requires the query-cost feature flag (--enable-feature=query-cost).",
        vec!["estimated", "actual"],
        vec![
            ("estimated", component_ref("CostEstimate")),
            ("actual", component_ref("ActualCost")),
        ],
    )
}

fn query_cost_data_schema() -> Schema {
    object(
        "Estimated cost of a query computed without executing it.",
        vec!["estimate"],
        vec![("estimate", component_ref("CostEstimate"))],
    )
}

fn query_cost_post_input_body_schema() -> Schema {
    let mut p = vec![
        (
            "query",
            string_schema_with_description_and_example(
                "Form field: The PromQL query to estimate.",
                "up",
            ),
        ),
        (
            "time",
            string_schema_with_description_and_example(
                "Form field: The evaluation timestamp (optional, defaults to current time).",
                "2023-07-21T20:10:51.781Z",
            ),
        ),
        (
            "timeout",
            string_schema_with_description_and_example(FORM_TIMEOUT_DESC, "30s"),
        ),
        (
            "lookback_delta",
            string_schema_with_description_and_example(FORM_LOOKBACK_DESC, "5m"),
        ),
        (
            "limit",
            integer_schema_with_description_and_example(FORM_COST_LIMIT_DESC, 100),
        ),
        (
            "stats",
            string_schema_with_description_and_example(FORM_COST_STATS_DESC, "all"),
        ),
        ("cost", boolean_schema_with_description(FORM_COST_COST_DESC)),
    ];
    p.extend(form_cost_limits());
    object(
        "POST request body for instant query cost estimation.",
        vec!["query"],
        p,
    )
}

fn query_range_cost_post_input_body_schema() -> Schema {
    let mut p = vec![
        (
            "query",
            string_schema_with_description_and_example(
                "Form field: The PromQL query to estimate.",
                "rate(http_requests_total[5m])",
            ),
        ),
        (
            "start",
            string_schema_with_description_and_example(
                "Form field: The start time of the query.",
                "2023-07-21T20:10:30.781Z",
            ),
        ),
        (
            "end",
            string_schema_with_description_and_example(
                "Form field: The end time of the query.",
                "2023-07-21T20:20:30.781Z",
            ),
        ),
        (
            "step",
            string_schema_with_description_and_example(
                "Form field: The step size of the query.",
                "15s",
            ),
        ),
        (
            "timeout",
            string_schema_with_description_and_example(FORM_TIMEOUT_DESC, "30s"),
        ),
        (
            "lookback_delta",
            string_schema_with_description_and_example(FORM_LOOKBACK_DESC, "5m"),
        ),
        (
            "limit",
            integer_schema_with_description_and_example(FORM_COST_LIMIT_DESC, 100),
        ),
        (
            "stats",
            string_schema_with_description_and_example(FORM_COST_STATS_DESC, "all"),
        ),
        ("cost", boolean_schema_with_description(FORM_COST_COST_DESC)),
    ];
    p.extend(form_cost_limits());
    object(
        "POST request body for range query cost estimation.",
        vec!["query", "start", "end", "step"],
        p,
    )
}

fn query_post_input_body_schema() -> Schema {
    let mut p = vec![
        (
            "query",
            string_schema_with_description_and_example(
                "Form field: The PromQL query to execute.",
                "up",
            ),
        ),
        (
            "time",
            string_schema_with_description_and_example(
                "Form field: The evaluation timestamp (optional, defaults to current time).",
                "2023-07-21T20:10:51.781Z",
            ),
        ),
        (
            "limit",
            integer_schema_with_description_and_example(
                "Form field: The maximum number of metrics to return.",
                100,
            ),
        ),
        (
            "timeout",
            string_schema_with_description_and_example(FORM_TIMEOUT_DESC, "30s"),
        ),
        (
            "lookback_delta",
            string_schema_with_description_and_example(FORM_LOOKBACK_DESC, "5m"),
        ),
        (
            "stats",
            string_schema_with_description_and_example(FORM_STATS_DESC, "all"),
        ),
        ("cost", boolean_schema_with_description(FORM_COST_DESC)),
    ];
    p.extend(form_cost_limits());
    object("POST request body for instant query.", vec!["query"], p)
}

fn query_range_post_input_body_schema() -> Schema {
    let mut p = vec![
        (
            "query",
            string_schema_with_description_and_example(
                "Form field: The query to execute.",
                "rate(http_requests_total[5m])",
            ),
        ),
        (
            "start",
            string_schema_with_description_and_example(
                "Form field: The start time of the query.",
                "2023-07-21T20:10:30.781Z",
            ),
        ),
        (
            "end",
            string_schema_with_description_and_example(
                "Form field: The end time of the query.",
                "2023-07-21T20:20:30.781Z",
            ),
        ),
        (
            "step",
            string_schema_with_description_and_example(
                "Form field: The step size of the query.",
                "15s",
            ),
        ),
        (
            "limit",
            integer_schema_with_description_and_example(
                "Form field: The maximum number of metrics to return.",
                100,
            ),
        ),
        (
            "timeout",
            string_schema_with_description_and_example(FORM_TIMEOUT_DESC, "30s"),
        ),
        (
            "lookback_delta",
            string_schema_with_description_and_example(FORM_LOOKBACK_DESC, "5m"),
        ),
        (
            "stats",
            string_schema_with_description_and_example(FORM_STATS_DESC, "all"),
        ),
        ("cost", boolean_schema_with_description(FORM_COST_DESC)),
    ];
    p.extend(form_cost_limits());
    object(
        "POST request body for range query.",
        vec!["query", "start", "end", "step"],
        p,
    )
}

fn query_exemplars_post_input_body_schema() -> Schema {
    object(
        "POST request body for exemplars query.",
        vec!["query"],
        vec![
            (
                "query",
                string_schema_with_description_and_example(
                    "Form field: The query to execute.",
                    "http_requests_total",
                ),
            ),
            (
                "start",
                string_schema_with_description_and_example(
                    "Form field: The start time of the query.",
                    "2023-07-21T20:00:00.000Z",
                ),
            ),
            (
                "end",
                string_schema_with_description_and_example(
                    "Form field: The end time of the query.",
                    "2023-07-21T21:00:00.000Z",
                ),
            ),
        ],
    )
}

fn format_query_output_body_schema() -> Schema {
    data_response_body_schema(
        "Response body for format query endpoint.",
        string_schema_with_description_and_example(
            "Formatted query string.",
            "sum by(status) (rate(http_requests_total[5m]))",
        ),
    )
}

fn format_query_post_input_body_schema() -> Schema {
    object(
        "POST request body for format query.",
        vec!["query"],
        vec![(
            "query",
            string_schema_with_description_and_example(
                "Form field: The query to format.",
                "sum(rate(http_requests_total[5m])) by (status)",
            ),
        )],
    )
}

fn parse_query_post_input_body_schema() -> Schema {
    object(
        "POST request body for parse query.",
        vec!["query"],
        vec![(
            "query",
            string_schema_with_description_and_example(
                "Form field: The query to parse.",
                "sum(rate(http_requests_total[5m]))",
            ),
        )],
    )
}

fn labels_post_input_body_schema() -> Schema {
    object(
        "POST request body for labels query.",
        Vec::new(),
        vec![
            (
                "start",
                string_schema_with_description_and_example(
                    "Form field: The start time of the query.",
                    "2023-07-21T20:00:00.000Z",
                ),
            ),
            (
                "end",
                string_schema_with_description_and_example(
                    "Form field: The end time of the query.",
                    "2023-07-21T21:00:00.000Z",
                ),
            ),
            (
                "match[]",
                string_array_schema_with_description_and_example(
                    "Form field: Series selector argument that selects the series from which to read the label names.",
                    str_seq(&["{job=\"prometheus\"}"]),
                ),
            ),
            (
                "limit",
                integer_schema_with_description_and_example(
                    "Form field: The maximum number of label names to return.",
                    100,
                ),
            ),
        ],
    )
}

fn series_post_input_body_schema() -> Schema {
    object(
        "POST request body for series query.",
        vec!["match[]"],
        vec![
            (
                "start",
                string_schema_with_description_and_example(
                    "Form field: The start time of the query.",
                    "2023-07-21T20:00:00.000Z",
                ),
            ),
            (
                "end",
                string_schema_with_description_and_example(
                    "Form field: The end time of the query.",
                    "2023-07-21T21:00:00.000Z",
                ),
            ),
            (
                "match[]",
                string_array_schema_with_description_and_example(
                    "Form field: Series selector argument that selects the series to return.",
                    str_seq(&["{job=\"prometheus\"}"]),
                ),
            ),
            (
                "limit",
                integer_schema_with_description_and_example(
                    "Form field: The maximum number of series to return.",
                    100,
                ),
            ),
        ],
    )
}

fn metadata_schema() -> Schema {
    object(
        "Metric metadata.",
        vec!["type", "unit", "help"],
        vec![
            (
                "type",
                string_schema_with_description(
                    "Metric type (counter, gauge, histogram, summary, or untyped).",
                ),
            ),
            (
                "unit",
                string_schema_with_description("Unit of the metric."),
            ),
            (
                "help",
                string_schema_with_description("Help text describing the metric."),
            ),
        ],
    )
}

fn metadata_output_body_schema() -> Schema {
    object(
        "Response body for metadata endpoint.",
        vec!["status", "data"],
        vec![
            ("status", status_schema()),
            (
                "data",
                Schema {
                    typ: "object",
                    additional_properties: Some(AdditionalProperties::Schema(Box::new(array_of(
                        component_ref("Metadata"),
                    )))),
                    ..Default::default()
                },
            ),
            ("warnings", warnings_schema()),
            ("infos", infos_schema()),
        ],
    )
}

fn metric_metadata_schema() -> Schema {
    object(
        "Target metric metadata.",
        vec!["target", "type", "help", "unit"],
        vec![
            ("target", component_ref("Labels")),
            ("metric", string_schema_with_description("Metric name.")),
            (
                "type",
                string_schema_with_description(
                    "Metric type (counter, gauge, histogram, summary, or untyped).",
                ),
            ),
            (
                "help",
                string_schema_with_description("Help text describing the metric."),
            ),
            (
                "unit",
                string_schema_with_description("Unit of the metric."),
            ),
        ],
    )
}

fn target_schema() -> Schema {
    object(
        "Scrape target information.",
        vec![
            "discoveredLabels",
            "labels",
            "scrapePool",
            "scrapeUrl",
            "globalUrl",
            "lastError",
            "lastScrape",
            "lastScrapeDuration",
            "health",
            "scrapeInterval",
            "scrapeTimeout",
        ],
        vec![
            ("discoveredLabels", component_ref("Labels")),
            ("labels", component_ref("Labels")),
            (
                "scrapePool",
                string_schema_with_description("Name of the scrape pool."),
            ),
            (
                "scrapeUrl",
                string_schema_with_description("URL of the target."),
            ),
            (
                "globalUrl",
                string_schema_with_description("Global URL of the target."),
            ),
            (
                "lastError",
                string_schema_with_description("Last error message from scraping."),
            ),
            (
                "lastScrape",
                date_time_schema_with_description("Timestamp of the last scrape."),
            ),
            (
                "lastScrapeDuration",
                number_schema_with_description("Duration of the last scrape in seconds."),
            ),
            (
                "health",
                string_schema_with_description(
                    "Health status of the target (up, down, or unknown).",
                ),
            ),
            (
                "scrapeInterval",
                string_schema_with_description("Scrape interval for this target."),
            ),
            (
                "scrapeTimeout",
                string_schema_with_description("Scrape timeout for this target."),
            ),
        ],
    )
}

fn dropped_target_schema() -> Schema {
    object(
        "Dropped target information.",
        vec!["discoveredLabels", "scrapePool"],
        vec![
            ("discoveredLabels", component_ref("Labels")),
            (
                "scrapePool",
                string_schema_with_description("Name of the scrape pool."),
            ),
        ],
    )
}

fn target_discovery_schema() -> Schema {
    object(
        "Target discovery information including active and dropped targets.",
        vec!["activeTargets", "droppedTargets", "droppedTargetCounts"],
        vec![
            ("activeTargets", array_of(component_ref("Target"))),
            ("droppedTargets", array_of(component_ref("DroppedTarget"))),
            (
                "droppedTargetCounts",
                Schema {
                    typ: "object",
                    additional_properties: Some(AdditionalProperties::Schema(Box::new(
                        integer_schema(),
                    ))),
                    ..Default::default()
                },
            ),
        ],
    )
}

fn scrape_pools_discovery_schema() -> Schema {
    object(
        "List of all configured scrape pools.",
        vec!["scrapePools"],
        vec![("scrapePools", array_of(string_schema()))],
    )
}

fn scrape_pool_config_data_schema() -> Schema {
    object(
        "Effective configuration for a scrape pool.",
        vec!["yaml"],
        vec![(
            "yaml",
            string_schema_with_description("Effective scrape pool configuration in YAML format."),
        )],
    )
}

fn config_schema() -> Schema {
    object(
        "Relabel configuration.",
        Vec::new(),
        vec![
            (
                "source_labels",
                string_array_schema_with_description("Source labels for relabeling."),
            ),
            (
                "separator",
                string_schema_with_description("Separator for source label values."),
            ),
            (
                "regex",
                string_schema_with_description("Regular expression for matching."),
            ),
            (
                "modulus",
                integer_schema_with_description("Modulus for hash-based relabeling."),
            ),
            (
                "target_label",
                string_schema_with_description("Target label name."),
            ),
            (
                "replacement",
                string_schema_with_description("Replacement value."),
            ),
            ("action", string_schema_with_description("Relabel action.")),
        ],
    )
}

fn relabel_step_schema() -> Schema {
    object(
        "Relabel step showing the rule, output, and whether the target was kept.",
        vec!["rule", "output", "keep"],
        vec![
            ("rule", component_ref("Config")),
            ("output", component_ref("Labels")),
            ("keep", boolean_schema()),
        ],
    )
}

fn relabel_steps_response_schema() -> Schema {
    object(
        "Relabeling steps response.",
        vec!["steps"],
        vec![("steps", array_of(component_ref("RelabelStep")))],
    )
}

fn rule_group_schema() -> Schema {
    object(
        "Rule group information.",
        vec![
            "name",
            "file",
            "rules",
            "interval",
            "limit",
            "evaluationTime",
            "lastEvaluation",
        ],
        vec![
            (
                "name",
                string_schema_with_description("Name of the rule group."),
            ),
            (
                "file",
                string_schema_with_description("File containing the rule group."),
            ),
            (
                "rules",
                Schema {
                    description: "Rules in this group.".into(),
                    ..array_of(described("object", "Rule definition."))
                },
            ),
            (
                "interval",
                number_schema_with_description("Evaluation interval in seconds."),
            ),
            (
                "limit",
                integer_schema_with_description("Maximum number of alerts for this group."),
            ),
            (
                "evaluationTime",
                number_schema_with_description("Time taken to evaluate the group in seconds."),
            ),
            (
                "lastEvaluation",
                date_time_schema_with_description("Timestamp of the last evaluation."),
            ),
        ],
    )
}

fn rule_discovery_schema() -> Schema {
    object(
        "Rule discovery information containing all rule groups.",
        vec!["groups"],
        vec![
            ("groups", array_of(component_ref("RuleGroup"))),
            (
                "groupNextToken",
                string_schema_with_description("Pagination token for the next page of groups."),
            ),
        ],
    )
}

fn alert_schema() -> Schema {
    object(
        "Alert information.",
        vec!["labels", "annotations", "state", "value"],
        vec![
            ("labels", component_ref("Labels")),
            ("annotations", component_ref("Labels")),
            (
                "state",
                string_schema_with_description(
                    "State of the alert (pending, firing, or inactive).",
                ),
            ),
            (
                "value",
                string_schema_with_description("Value of the alert expression."),
            ),
            (
                "activeAt",
                date_time_schema_with_description("Timestamp when the alert became active."),
            ),
            (
                "keepFiringSince",
                date_time_schema_with_description(
                    "Timestamp since the alert has been kept firing.",
                ),
            ),
        ],
    )
}

fn alert_discovery_schema() -> Schema {
    object(
        "Alert discovery information containing all active alerts.",
        vec!["alerts"],
        vec![("alerts", array_of(component_ref("Alert")))],
    )
}

fn alertmanager_target_schema() -> Schema {
    object(
        "Alertmanager target information.",
        vec!["url"],
        vec![(
            "url",
            string_schema_with_description("URL of the Alertmanager instance."),
        )],
    )
}

fn alertmanager_discovery_schema() -> Schema {
    object(
        "Alertmanager discovery information including active and dropped instances.",
        vec!["activeAlertmanagers", "droppedAlertmanagers"],
        vec![
            (
                "activeAlertmanagers",
                array_of(component_ref("AlertmanagerTarget")),
            ),
            (
                "droppedAlertmanagers",
                array_of(component_ref("AlertmanagerTarget")),
            ),
        ],
    )
}

fn status_config_data_schema() -> Schema {
    object(
        "Prometheus configuration.",
        vec!["yaml"],
        vec![(
            "yaml",
            string_schema_with_description("Prometheus configuration in YAML format."),
        )],
    )
}

fn date_time() -> Schema {
    Schema {
        typ: "string",
        format: "date-time",
        ..Default::default()
    }
}

fn runtime_info_schema() -> Schema {
    object(
        "Prometheus runtime information.",
        vec![
            "startTime",
            "CWD",
            "hostname",
            "serverTime",
            "reloadConfigSuccess",
            "lastConfigTime",
            "corruptionCount",
            "goroutineCount",
            "GOMAXPROCS",
            "GOMEMLIMIT",
            "GOGC",
            "GODEBUG",
            "storageRetention",
        ],
        vec![
            ("startTime", date_time()),
            ("CWD", string_schema()),
            ("hostname", string_schema()),
            ("serverTime", date_time()),
            ("reloadConfigSuccess", boolean_schema()),
            ("lastConfigTime", date_time()),
            ("corruptionCount", integer_schema()),
            ("goroutineCount", integer_schema()),
            ("GOMAXPROCS", integer_schema()),
            ("GOMEMLIMIT", integer_schema()),
            ("GOGC", string_schema()),
            ("GODEBUG", string_schema()),
            ("storageRetention", string_schema()),
        ],
    )
}

fn prometheus_version_schema() -> Schema {
    object(
        "Prometheus version information.",
        vec![
            "version",
            "revision",
            "branch",
            "buildUser",
            "buildDate",
            "goVersion",
        ],
        vec![
            ("version", string_schema()),
            ("revision", string_schema()),
            ("branch", string_schema()),
            ("buildUser", string_schema()),
            ("buildDate", string_schema()),
            ("goVersion", string_schema()),
        ],
    )
}

fn status_flags_output_body_schema() -> Schema {
    data_response_body_schema(
        "Response body for status flags endpoint.",
        Schema {
            typ: "object",
            additional_properties: Some(AdditionalProperties::Schema(Box::new(string_schema()))),
            ..Default::default()
        },
    )
}

fn head_stats_schema() -> Schema {
    object(
        "TSDB head statistics.",
        vec![
            "numSeries",
            "numLabelPairs",
            "chunkCount",
            "minTime",
            "maxTime",
        ],
        vec![
            ("numSeries", integer_schema()),
            ("numLabelPairs", integer_schema()),
            ("chunkCount", integer_schema()),
            ("minTime", integer_schema()),
            ("maxTime", integer_schema()),
        ],
    )
}

fn tsdb_stat_schema() -> Schema {
    object(
        "TSDB statistic.",
        vec!["name", "value"],
        vec![("name", string_schema()), ("value", integer_schema())],
    )
}

fn tsdb_status_schema() -> Schema {
    object(
        "TSDB status information.",
        vec![
            "headStats",
            "seriesCountByMetricName",
            "labelValueCountByLabelName",
            "memoryInBytesByLabelName",
            "seriesCountByLabelValuePair",
        ],
        vec![
            ("headStats", component_ref("HeadStats")),
            (
                "seriesCountByMetricName",
                array_of(component_ref("TSDBStat")),
            ),
            (
                "labelValueCountByLabelName",
                array_of(component_ref("TSDBStat")),
            ),
            (
                "memoryInBytesByLabelName",
                array_of(component_ref("TSDBStat")),
            ),
            (
                "seriesCountByLabelValuePair",
                array_of(component_ref("TSDBStat")),
            ),
        ],
    )
}

fn block_desc_schema() -> Schema {
    object(
        "Block descriptor.",
        vec!["ulid", "minTime", "maxTime"],
        vec![
            ("ulid", string_schema()),
            ("minTime", integer_schema()),
            ("maxTime", integer_schema()),
        ],
    )
}

fn block_stats_schema() -> Schema {
    object(
        "Block statistics.",
        Vec::new(),
        vec![
            ("numSamples", integer_schema()),
            ("numSeries", integer_schema()),
            ("numChunks", integer_schema()),
            ("numTombstones", integer_schema()),
            ("numFloatSamples", integer_schema()),
            ("numHistogramSamples", integer_schema()),
        ],
    )
}

fn block_meta_compaction_schema() -> Schema {
    object(
        "Block compaction metadata.",
        vec!["level"],
        vec![
            ("level", integer_schema()),
            ("sources", array_of(string_schema())),
            ("parents", array_of(component_ref("BlockDesc"))),
            ("failed", boolean_schema()),
            ("deletable", boolean_schema()),
            ("hints", array_of(string_schema())),
        ],
    )
}

fn block_meta_schema() -> Schema {
    object(
        "Block metadata.",
        vec!["ulid", "minTime", "maxTime", "compaction", "version"],
        vec![
            ("ulid", string_schema()),
            ("minTime", integer_schema()),
            ("maxTime", integer_schema()),
            ("stats", component_ref("BlockStats")),
            ("compaction", component_ref("BlockMetaCompaction")),
            ("version", integer_schema()),
        ],
    )
}

fn status_tsdb_blocks_data_schema() -> Schema {
    object(
        "TSDB blocks information.",
        vec!["blocks"],
        vec![("blocks", array_of(component_ref("BlockMeta")))],
    )
}

fn status_wal_replay_data_schema() -> Schema {
    object(
        "WAL replay status.",
        vec!["min", "max", "current"],
        vec![
            ("min", integer_schema()),
            ("max", integer_schema()),
            ("current", integer_schema()),
        ],
    )
}

fn data_struct_schema() -> Schema {
    object(
        "Generic data structure with a name field.",
        vec!["name"],
        vec![("name", string_schema())],
    )
}

fn notification_schema() -> Schema {
    object(
        "Server notification.",
        vec!["text", "date", "active"],
        vec![
            ("text", string_schema()),
            ("date", date_time()),
            ("active", boolean_schema()),
        ],
    )
}
