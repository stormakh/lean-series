//! Port of `web/api/v1/openapi_examples.go`: example request and response bodies. Go maps are written with
//! `go_map` (sorted as go-yaml sorts them); Go slices with `Node::Seq`.

use std::sync::Arc;

use model::histogram::{FloatHistogram, Span};
use promql::{FPoint, HPoint, Sample, Series};

use super::emit::{Node, go_map, str_seq};
use super::model::{Examples, ex, labels, matrix_example, str_map, vector_example};

macro_rules! gm {
    ($($k:expr => $v:expr),* $(,)?) => { go_map(vec![$(($k, Node::from($v))),*]) };
}

macro_rules! seq {
    ($($v:expr),* $(,)?) => { Node::Seq(vec![$(Node::from($v)),*]) };
}

// Example builders for request bodies.

pub fn query_post_examples() -> Examples {
    vec![
        ex("simpleQuery", "Simple instant query", gm! {"query" => "up"}),
        ex(
            "queryWithTime",
            "Query with specific timestamp",
            gm! {"query" => "up{job=\"prometheus\"}", "time" => "2026-01-02T13:37:00.000Z"},
        ),
        ex(
            "queryWithLimit",
            "Query with limit and statistics",
            gm! {"query" => "rate(prometheus_http_requests_total{handler=\"/api/v1/query\"}[5m])", "limit" => 100, "stats" => "all"},
        ),
    ]
}

pub fn query_range_post_examples() -> Examples {
    vec![
        ex(
            "basicRange",
            "Basic range query",
            gm! {"query" => "up", "start" => "2026-01-02T12:37:00.000Z", "end" => "2026-01-02T13:37:00.000Z", "step" => "15s"},
        ),
        ex(
            "rateQuery",
            "Rate calculation over time range",
            gm! {
                "query" => "rate(prometheus_http_requests_total{handler=\"/api/v1/query\"}[5m])",
                "start" => "2026-01-02T12:37:00.000Z",
                "end" => "2026-01-02T13:37:00.000Z",
                "step" => "30s",
                "timeout" => "30s",
            },
        ),
    ]
}

pub fn query_cost_post_examples() -> Examples {
    vec![
        ex(
            "simpleQuery",
            "Estimate the cost of an instant query",
            gm! {"query" => "up"},
        ),
        ex(
            "queryWithTime",
            "Estimate the cost at a specific timestamp",
            gm! {"query" => "rate(prometheus_http_requests_total[5m])", "time" => "2026-01-02T13:37:00.000Z"},
        ),
    ]
}

pub fn query_range_cost_post_examples() -> Examples {
    vec![ex(
        "rangeQuery",
        "Estimate the cost of a range query",
        gm! {
            "query" => "rate(prometheus_http_requests_total[5m])",
            "start" => "2026-01-02T12:37:00.000Z",
            "end" => "2026-01-02T13:37:00.000Z",
            "step" => "30s",
        },
    )]
}

pub fn query_cost_response_examples() -> Examples {
    vec![ex(
        "estimate",
        "Cost estimate for a query",
        gm! {"status" => "success", "data" => gm! {"estimate" => gm! {"seriesTouched" => 42, "samplesRead" => 5040}}},
    )]
}

pub fn query_exemplars_post_examples() -> Examples {
    vec![
        ex(
            "basicExemplar",
            "Query exemplars for a metric",
            gm! {"query" => "prometheus_http_requests_total"},
        ),
        ex(
            "exemplarWithTimeRange",
            "Exemplars within specific time range",
            gm! {
                "query" => "prometheus_http_requests_total{job=\"prometheus\"}",
                "start" => "2026-01-02T12:37:00.000Z",
                "end" => "2026-01-02T13:37:00.000Z",
            },
        ),
    ]
}

pub fn format_query_post_examples() -> Examples {
    vec![
        ex(
            "simpleFormat",
            "Format a simple query",
            gm! {"query" => "up{job=\"prometheus\"}"},
        ),
        ex(
            "complexFormat",
            "Format a complex query",
            gm! {"query" => "sum(rate(http_requests_total[5m])) by (job, status)"},
        ),
    ]
}

pub fn parse_query_post_examples() -> Examples {
    vec![
        ex("simpleParse", "Parse a simple query", gm! {"query" => "up"}),
        ex(
            "complexParse",
            "Parse a complex query",
            gm! {"query" => "rate(http_requests_total{job=\"api\"}[5m])"},
        ),
    ]
}

pub fn labels_post_examples() -> Examples {
    vec![
        ex("allLabels", "Get all label names", gm! {}),
        ex(
            "labelsWithTimeRange",
            "Get label names within time range",
            gm! {"start" => "2026-01-02T12:37:00.000Z", "end" => "2026-01-02T13:37:00.000Z"},
        ),
        ex(
            "labelsWithMatch",
            "Get label names matching series selector",
            gm! {"match[]" => str_seq(&["up", "process_start_time_seconds{job=\"prometheus\"}"])},
        ),
    ]
}

pub fn search_metric_names_post_examples() -> Examples {
    vec![ex(
        "metricAutocomplete",
        "Search metric names for autocomplete",
        gm! {"search[]" => str_seq(&["http_req"]), "include_metadata" => true, "sort_by" => "score", "limit" => 20},
    )]
}

pub fn search_label_names_post_examples() -> Examples {
    vec![ex(
        "labelsForMetric",
        "Search label names for a metric",
        gm! {
            "match[]" => str_seq(&["{__name__=\"http_requests_total\"}"]),
            "search[]" => str_seq(&["sta"]),
            "sort_by" => "score",
            "limit" => 20,
        },
    )]
}

pub fn search_label_values_post_examples() -> Examples {
    vec![ex(
        "valuesForLabel",
        "Search values for a label",
        gm! {
            "label" => "instance",
            "match[]" => str_seq(&["up"]),
            "search[]" => str_seq(&["909"]),
            "sort_by" => "score",
            "limit" => 10,
        },
    )]
}

pub fn series_post_examples() -> Examples {
    vec![
        ex(
            "seriesMatch",
            "Find series by label matchers",
            gm! {"match[]" => str_seq(&["up"])},
        ),
        ex(
            "seriesWithTimeRange",
            "Find series with time range",
            gm! {
                "match[]" => str_seq(&["up", "process_cpu_seconds_total{job=\"prometheus\"}"]),
                "start" => "2026-01-02T12:37:00.000Z",
                "end" => "2026-01-02T13:37:00.000Z",
            },
        ),
    ]
}

// Example builders for response bodies.

fn example_histogram() -> Arc<FloatHistogram> {
    Arc::new(FloatHistogram {
        schema: 1,
        zero_threshold: 1e-128,
        count: 12.5,
        sum: 42.1,
        positive_spans: vec![Span {
            offset: 0,
            length: 2,
        }],
        positive_buckets: vec![5.0, 7.5],
        ..Default::default()
    })
}

pub fn query_response_examples() -> Examples {
    let vector_result = vec![
        Sample {
            metric: labels(&[
                "__name__",
                "up",
                "job",
                "prometheus",
                "instance",
                "demo.prometheus.io:9090",
            ]),
            t: 1767436620000,
            f: 1.0,
            ..Default::default()
        },
        Sample {
            metric: labels(&[
                "__name__",
                "up",
                "env",
                "demo",
                "job",
                "alertmanager",
                "instance",
                "demo.prometheus.io:9093",
            ]),
            t: 1767436620000,
            f: 1.0,
            ..Default::default()
        },
    ];
    let matrix_result = vec![Series {
        metric: labels(&[
            "__name__",
            "up",
            "job",
            "prometheus",
            "instance",
            "demo.prometheus.io:9090",
        ]),
        floats: vec![
            FPoint {
                t: 1767436320000,
                f: 1.0,
            },
            FPoint {
                t: 1767436620000,
                f: 1.0,
            },
        ],
        ..Default::default()
    }];
    let histogram_result = vec![Sample {
        metric: labels(&[
            "__name__",
            "http_request_duration_seconds",
            "job",
            "prometheus",
            "instance",
            "demo.prometheus.io:9090",
        ]),
        t: 1767436620000,
        h: Some(example_histogram()),
        ..Default::default()
    }];
    vec![
        ex(
            "vectorResult",
            "Instant vector query: up",
            vector_example(&vector_result),
        ),
        ex(
            "scalarResult",
            "Scalar query: scalar(42)",
            gm! {"status" => "success", "data" => gm! {"resultType" => "scalar", "result" => seq![1767436620, "42"]}},
        ),
        ex(
            "matrixResult",
            "Range vector query: up[5m]",
            matrix_example(&matrix_result),
        ),
        ex(
            "histogramResult",
            "Instant vector query with native histograms",
            vector_example(&histogram_result),
        ),
    ]
}

pub fn query_range_response_examples() -> Examples {
    let matrix_result = vec![Series {
        metric: labels(&[
            "__name__",
            "up",
            "job",
            "prometheus",
            "instance",
            "demo.prometheus.io:9090",
        ]),
        floats: vec![
            FPoint {
                t: 1767433020000,
                f: 1.0,
            },
            FPoint {
                t: 1767434820000,
                f: 1.0,
            },
            FPoint {
                t: 1767436620000,
                f: 1.0,
            },
        ],
        ..Default::default()
    }];
    let histogram_result = vec![Series {
        metric: labels(&[
            "__name__",
            "http_request_duration_seconds",
            "job",
            "prometheus",
            "instance",
            "demo.prometheus.io:9090",
        ]),
        histograms: vec![HPoint {
            t: 1767433020000,
            h: example_histogram(),
        }],
        ..Default::default()
    }];
    vec![
        ex(
            "matrixResult",
            "Range query: rate(prometheus_http_requests_total[5m])",
            matrix_example(&matrix_result),
        ),
        ex(
            "histogramResult",
            "Range query with native histograms",
            matrix_example(&histogram_result),
        ),
    ]
}

pub fn labels_response_examples() -> Examples {
    vec![ex(
        "labelNames",
        "List of label names",
        gm! {
            "status" => "success",
            "data" => str_seq(&[
                "__name__", "active", "address", "alertmanager", "alertname", "alertstate",
                "backend", "branch", "code", "collector", "component", "device",
                "env", "endpoint", "fstype", "handler", "instance", "job",
                "le", "method", "mode", "name",
            ]),
        },
    )]
}

pub fn series_response_examples() -> Examples {
    vec![ex(
        "seriesList",
        "List of series matching the selector",
        gm! {
            "status" => "success",
            "data" => seq![
                str_map(&[("__name__", "up"), ("env", "demo"), ("instance", "demo.prometheus.io:8080"), ("job", "cadvisor")]),
                str_map(&[("__name__", "up"), ("env", "demo"), ("instance", "demo.prometheus.io:9093"), ("job", "alertmanager")]),
                str_map(&[("__name__", "up"), ("env", "demo"), ("instance", "demo.prometheus.io:9100"), ("job", "node")]),
                str_map(&[("__name__", "up"), ("instance", "demo.prometheus.io:3000"), ("job", "grafana")]),
                str_map(&[("__name__", "up"), ("instance", "demo.prometheus.io:8996"), ("job", "random")]),
            ],
        },
    )]
}

pub fn targets_response_examples() -> Examples {
    vec![ex(
        "targetsList",
        "Active and dropped targets",
        gm! {
            "status" => "success",
            "data" => gm! {
                "activeTargets" => seq![gm! {
                    "discoveredLabels" => str_map(&[
                        ("__address__", "demo.prometheus.io:9093"),
                        ("__meta_filepath", "/etc/prometheus/file_sd/alertmanager.yml"),
                        ("__metrics_path__", "/metrics"),
                        ("__scheme__", "http"),
                        ("env", "demo"),
                        ("job", "alertmanager"),
                    ]),
                    "labels" => str_map(&[("env", "demo"), ("instance", "demo.prometheus.io:9093"), ("job", "alertmanager")]),
                    "scrapePool" => "alertmanager",
                    "scrapeUrl" => "http://demo.prometheus.io:9093/metrics",
                    "globalUrl" => "http://demo.prometheus.io:9093/metrics",
                    "lastError" => "",
                    "lastScrape" => "2026-01-02T13:36:40.200Z",
                    "lastScrapeDuration" => 0.006576866,
                    "health" => "up",
                    "scrapeInterval" => "15s",
                    "scrapeTimeout" => "10s",
                }],
                "droppedTargets" => seq![],
                "droppedTargetCounts" => gm! {
                    "alertmanager" => 0,
                    "blackbox" => 0,
                    "caddy" => 0,
                    "cadvisor" => 0,
                    "grafana" => 0,
                    "node" => 0,
                    "prometheus" => 0,
                    "random" => 0,
                },
            },
        },
    )]
}

const WATCHDOG_DESCRIPTION: &str = "This is an alert meant to ensure that the entire alerting pipeline is functional. This alert is always firing, therefore it should always be firing in Alertmanager and always fire against a receiver. There are integrations with various notification mechanisms that send a notification when this alert is not firing. For example the \"DeadMansSnitch\" integration in PagerDuty.";

pub fn rules_response_examples() -> Examples {
    vec![ex(
        "ruleGroups",
        "Alerting and recording rules",
        gm! {
            "status" => "success",
            "data" => gm! {
                "groups" => seq![gm! {
                    "name" => "ansible managed alert rules",
                    "file" => "/etc/prometheus/rules/ansible_managed.yml",
                    "interval" => 15,
                    "limit" => 0,
                    "rules" => seq![gm! {
                        "state" => "firing",
                        "name" => "Watchdog",
                        "query" => "vector(1)",
                        "duration" => 600,
                        "keepFiringFor" => 0,
                        "labels" => str_map(&[("severity", "warning")]),
                        "annotations" => str_map(&[("description", WATCHDOG_DESCRIPTION), ("summary", "Ensure entire alerting pipeline is functional")]),
                        "health" => "ok",
                        "evaluationTime" => 0.000356688,
                        "lastEvaluation" => "2026-01-02T13:36:56.874Z",
                        "type" => "alerting",
                    }],
                    "evaluationTime" => 0.000561635,
                    "lastEvaluation" => "2026-01-02T13:36:56.874Z",
                }],
            },
        },
    )]
}

pub fn alerts_response_examples() -> Examples {
    vec![ex(
        "activeAlerts",
        "Currently active alerts",
        gm! {
            "status" => "success",
            "data" => gm! {
                "alerts" => seq![gm! {
                    "labels" => str_map(&[("alertname", "Watchdog"), ("severity", "warning")]),
                    "annotations" => str_map(&[("description", WATCHDOG_DESCRIPTION), ("summary", "Ensure entire alerting pipeline is functional")]),
                    "state" => "firing",
                    "activeAt" => "2026-01-02T13:30:00.000Z",
                    "value" => "1e+00",
                }],
            },
        },
    )]
}

pub fn query_exemplars_response_examples() -> Examples {
    vec![ex(
        "exemplarsResult",
        "Exemplars for a metric with trace IDs",
        gm! {
            "status" => "success",
            "data" => seq![gm! {
                "seriesLabels" => str_map(&[("__name__", "http_requests_total"), ("job", "api-server"), ("method", "GET")]),
                "exemplars" => seq![gm! {
                    "labels" => str_map(&[("traceID", "abc123def456")]),
                    "value" => "1.5",
                    "timestamp" => 1689956451.781,
                }],
            }],
        },
    )]
}

pub fn format_query_response_examples() -> Examples {
    vec![ex(
        "formattedQuery",
        "Formatted PromQL query",
        gm! {"status" => "success", "data" => "sum by(job, status) (rate(http_requests_total[5m]))"},
    )]
}

pub fn parse_query_response_examples() -> Examples {
    vec![ex(
        "parsedQuery",
        "Parsed PromQL expression tree",
        gm! {"status" => "success", "data" => gm! {"resultType" => "vector"}},
    )]
}

pub fn label_values_response_examples() -> Examples {
    vec![ex(
        "labelValues",
        "List of values for a label",
        gm! {
            "status" => "success",
            "data" => str_seq(&["alertmanager", "blackbox", "caddy", "cadvisor", "grafana", "node", "prometheus", "random"]),
        },
    )]
}

pub fn metadata_response_examples() -> Examples {
    vec![ex(
        "metricMetadata",
        "Metadata for metrics",
        gm! {
            "status" => "success",
            "data" => gm! {
                "prometheus_rule_group_iterations_missed_total" => seq![gm! {
                    "type" => "counter",
                    "help" => "The total number of rule group evaluations missed due to slow rule group evaluation.",
                    "unit" => "",
                }],
                "prometheus_sd_updates_total" => seq![gm! {
                    "type" => "counter",
                    "help" => "Total number of update events sent to the SD consumers.",
                    "unit" => "",
                }],
                "go_gc_stack_starting_size_bytes" => seq![gm! {
                    "type" => "gauge",
                    "help" => "The stack size of new goroutines. Sourced from /gc/stack/starting-size:bytes.",
                    "unit" => "",
                }],
            },
        },
    )]
}

pub fn scrape_pools_response_examples() -> Examples {
    vec![ex(
        "scrapePoolsList",
        "List of scrape pool names",
        gm! {
            "status" => "success",
            "data" => gm! {
                "scrapePools" => str_seq(&["alertmanager", "blackbox", "caddy", "cadvisor", "grafana", "node", "prometheus", "random"]),
            },
        },
    )]
}

pub fn scrape_pool_config_response_examples() -> Examples {
    vec![ex(
        "scrapePoolConfig",
        "Effective configuration for a scrape pool",
        gm! {
            "status" => "success",
            "data" => gm! {
                "yaml" => "job_name: prometheus\nscrape_interval: 15s\nscrape_timeout: 10s\nmetrics_path: /metrics\nscheme: http\n",
            },
        },
    )]
}

pub fn targets_metadata_response_examples() -> Examples {
    vec![ex(
        "targetMetadata",
        "Metadata for targets",
        gm! {
            "status" => "success",
            "data" => seq![gm! {
                "target" => str_map(&[("instance", "localhost:9090"), ("job", "prometheus")]),
                "type" => "gauge",
                "help" => "The current health status of the target",
                "unit" => "",
                "metric" => "up",
            }],
        },
    )]
}

pub fn targets_relabel_steps_response_examples() -> Examples {
    vec![ex(
        "relabelSteps",
        "Relabel steps for a target",
        gm! {
            "status" => "success",
            "data" => gm! {
                "steps" => seq![gm! {
                    "rule" => gm! {
                        "source_labels" => str_seq(&["__address__"]),
                        "target_label" => "instance",
                        "action" => "replace",
                        "regex" => "(.*)",
                        "replacement" => "$1",
                    },
                    "output" => str_map(&[("__address__", "localhost:9090"), ("instance", "localhost:9090"), ("job", "prometheus")]),
                    "keep" => true,
                }],
            },
        },
    )]
}

pub fn alertmanagers_response_examples() -> Examples {
    vec![ex(
        "alertmanagerDiscovery",
        "Alertmanager discovery results",
        gm! {
            "status" => "success",
            "data" => gm! {
                "activeAlertmanagers" => seq![gm! {"url" => "http://demo.prometheus.io:9093/api/v2/alerts"}],
                "droppedAlertmanagers" => seq![],
            },
        },
    )]
}

pub fn status_config_response_examples() -> Examples {
    vec![ex(
        "configYAML",
        "Prometheus configuration",
        gm! {
            "status" => "success",
            "data" => gm! {
                "yaml" => "global:\n  scrape_interval: 15s\n  scrape_timeout: 10s\n  evaluation_interval: 15s\n  external_labels:\n    environment: demo-prometheus-io\nalerting:\n  alertmanagers:\n  - scheme: http\n    static_configs:\n    - targets:\n      - demo.prometheus.io:9093\nrule_files:\n- /etc/prometheus/rules/*.yml\n",
            },
        },
    )]
}

pub fn status_runtime_info_response_examples() -> Examples {
    vec![ex(
        "runtimeInfo",
        "Runtime information",
        gm! {
            "status" => "success",
            "data" => gm! {
                "startTime" => "2026-01-01T13:37:00.000Z",
                "CWD" => "/",
                "hostname" => "demo-prometheus-io",
                "serverTime" => "2026-01-02T13:37:00.000Z",
                "reloadConfigSuccess" => true,
                "lastConfigTime" => "2026-01-01T13:37:00.000Z",
                "corruptionCount" => 0,
                "goroutineCount" => 88,
                "GOMAXPROCS" => 2,
                "GOMEMLIMIT" => 3703818240i64,
                "GOGC" => "75",
                "GODEBUG" => "",
                "storageRetention" => "31d",
            },
        },
    )]
}

pub fn status_build_info_response_examples() -> Examples {
    vec![ex(
        "buildInfo",
        "Build information",
        gm! {
            "status" => "success",
            "data" => gm! {
                "version" => "3.7.3",
                "revision" => "0a41f0000705c69ab8e0f9a723fc73e39ed62b07",
                "branch" => "HEAD",
                "buildUser" => "root@08c890a84441",
                "buildDate" => "20251030-07:26:10",
                "goVersion" => "go1.25.3",
            },
        },
    )]
}

pub fn status_flags_response_examples() -> Examples {
    vec![ex(
        "flags",
        "Command-line flags",
        gm! {
            "status" => "success",
            "data" => str_map(&[
                ("agent", "false"),
                ("alertmanager.notification-queue-capacity", "10000"),
                ("config.file", "/etc/prometheus/prometheus.yml"),
                ("enable-feature", "exemplar-storage,native-histograms"),
                ("query.max-concurrency", "20"),
                ("query.timeout", "2m"),
                ("storage.tsdb.path", "/prometheus"),
                ("storage.tsdb.retention.time", "15d"),
                ("web.console.libraries", "/usr/share/prometheus/console_libraries"),
                ("web.console.templates", "/usr/share/prometheus/consoles"),
                ("web.enable-admin-api", "true"),
                ("web.enable-lifecycle", "true"),
                ("web.listen-address", "0.0.0.0:9090"),
                ("web.page-title", "Prometheus Time Series Collection and Processing Server"),
            ]),
        },
    )]
}

fn name_value(name: &'static str, value: i64) -> Node {
    gm! {"name" => name, "value" => value}
}

pub fn status_tsdb_response_examples() -> Examples {
    vec![ex(
        "tsdbStats",
        "TSDB statistics",
        gm! {
            "status" => "success",
            "data" => gm! {
                "headStats" => gm! {
                    "numSeries" => 9925,
                    "numLabelPairs" => 2512,
                    "chunkCount" => 37525,
                    "minTime" => 1767362400712i64,
                    "maxTime" => 1767436620000i64,
                },
                "seriesCountByMetricName" => seq![name_value("up", 100), name_value("http_requests_total", 500)],
                "labelValueCountByLabelName" => seq![name_value("__name__", 5), name_value("job", 3)],
                "memoryInBytesByLabelName" => seq![name_value("__name__", 1024), name_value("job", 512)],
                "seriesCountByLabelValuePair" => seq![name_value("job=prometheus", 100), name_value("instance=localhost:9090", 100)],
            },
        },
    )]
}

pub fn status_tsdb_blocks_response_examples() -> Examples {
    vec![ex(
        "tsdbBlocks",
        "TSDB block information",
        gm! {
            "status" => "success",
            "data" => gm! {
                "blocks" => seq![gm! {
                    "ulid" => "01KC4D6GXQA4CRHYKV78NEBVAE",
                    "minTime" => 1764568801099i64,
                    "maxTime" => 1764763200000i64,
                    "stats" => gm! {"numSamples" => 129505582, "numSeries" => 10661, "numChunks" => 1073962},
                    "compaction" => gm! {
                        "level" => 4,
                        "sources" => str_seq(&["01KBCJ7TR8A4QAJ3AA1J651P5S", "01KBCS3J0E34567YPB8Y5W0E24", "01KBCZZ9KRTYGG3E7HVQFGC3S3"]),
                    },
                    "version" => 1,
                }],
            },
        },
    )]
}

pub fn status_wal_replay_response_examples() -> Examples {
    vec![ex(
        "walReplay",
        "WAL replay status",
        gm! {"status" => "success", "data" => gm! {"min" => 3209, "max" => 3214, "current" => 3214}},
    )]
}

pub fn status_self_metrics_response_examples() -> Examples {
    let label =
        |name: &'static str, value: &'static str| str_map(&[("name", name), ("value", value)]);
    vec![ex(
        "selfMetrics",
        "Prometheus self-instrumentation metrics in ProtoJSON format",
        gm! {
            "status" => "success",
            "data" => seq![
                gm! {
                    "name" => "prometheus_build_info",
                    "help" => "A metric with a constant '1' value labeled by version, revision, branch, goversion from which prometheus was built, and the goos and goarch for the build.",
                    "type" => "GAUGE",
                    "metric" => seq![gm! {
                        "label" => seq![
                            label("branch", "HEAD"),
                            label("goarch", "amd64"),
                            label("goos", "linux"),
                            label("goversion", "go1.23.0"),
                            label("revision", "abc1234"),
                            label("tags", "netgo,builtinassets,stringlabels"),
                            label("version", "3.0.0"),
                        ],
                        "gauge" => gm! {"value" => 1},
                    }],
                },
                gm! {
                    "name" => "prometheus_tsdb_head_chunks",
                    "help" => "Total number of chunks in the head block.",
                    "type" => "GAUGE",
                    "metric" => seq![gm! {"gauge" => gm! {"value" => 1024}}],
                },
            ],
        },
    )]
}

pub fn delete_series_response_examples() -> Examples {
    vec![ex(
        "deletionSuccess",
        "Successful series deletion",
        gm! {"status" => "success"},
    )]
}

pub fn clean_tombstones_response_examples() -> Examples {
    vec![ex(
        "tombstonesCleaned",
        "Tombstones cleaned successfully",
        gm! {"status" => "success"},
    )]
}

pub fn snapshot_response_examples() -> Examples {
    vec![ex(
        "snapshotCreated",
        "Snapshot created successfully",
        gm! {"status" => "success", "data" => gm! {"name" => "20260102T133700Z-a1b2c3d4e5f67890"}},
    )]
}

pub fn notifications_response_examples() -> Examples {
    vec![ex(
        "notifications",
        "Server notifications",
        gm! {
            "status" => "success",
            "data" => seq![gm! {"text" => "Configuration reload has failed.", "date" => "2026-01-02T16:14:50.046Z", "active" => true}],
        },
    )]
}

pub fn notification_live_examples() -> Examples {
    let mut e = ex(
        "activeNotification",
        "Active notification SSE message",
        gm! {"data" => "{\"text\":\"Configuration reload has failed.\",\"date\":\"2026-01-02T16:14:50.046Z\",\"active\":true}"},
    );
    e.1.description = "An SSE message containing an active server notification.".into();
    vec![e]
}

pub fn features_response_examples() -> Examples {
    vec![ex(
        "enabledFeatures",
        "Enabled feature flags",
        gm! {"status" => "success", "data" => str_seq(&["exemplar-storage", "remote-write-receiver"])},
    )]
}

pub fn search_metric_names_response_examples() -> Examples {
    vec![ex(
        "metricNamesStream",
        "NDJSON stream of metric names",
        Node::from(
            "{\"results\":[{\"name\":\"http_requests_total\",\"type\":\"counter\",\"help\":\"Total HTTP requests.\"}]}\n{\"status\":\"success\",\"has_more\":false}\n",
        ),
    )]
}

pub fn search_label_names_response_examples() -> Examples {
    vec![ex(
        "labelNamesStream",
        "NDJSON stream of label names",
        Node::from(
            "{\"results\":[{\"name\":\"instance\"},{\"name\":\"job\"}]}\n{\"status\":\"success\",\"has_more\":false}\n",
        ),
    )]
}

pub fn search_label_values_response_examples() -> Examples {
    vec![ex(
        "labelValuesStream",
        "NDJSON stream of label values",
        Node::from(
            "{\"results\":[{\"value\":\"localhost:9090\"},{\"value\":\"localhost:9091\"}]}\n{\"status\":\"success\",\"has_more\":true}\n",
        ),
    )]
}

pub fn error_response_examples() -> Examples {
    vec![ex(
        "tsdbNotReady",
        "TSDB not ready",
        gm! {"status" => "error", "errorType" => "internal", "error" => "TSDB not ready"},
    )]
}
