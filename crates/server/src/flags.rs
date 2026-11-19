//! The command line of `prometheus` (Go `main()` flag definitions and `flagConfig`).

use crate::kingpin::{App, Flag, Kind, Parsed};

/// Go `defaultRetentionString`.
pub const DEFAULT_RETENTION_STRING: &str = "15d";

/// The version as Go's `version.Print("prometheus")` prints it.
pub fn version_print(app_name: &str) -> String {
    format!(
        "{app_name}, version {} (branch: {}, revision: {})\n  build user:       {}\n  build date:       {}\n  go version:       {}\n  platform:         {}\n  tags:             {}",
        VERSION,
        BRANCH,
        REVISION,
        BUILD_USER,
        BUILD_DATE,
        GO_VERSION,
        PLATFORM,
        TAGS
    )
}

/// Go `version.Version` and friends. The build may set them with environment variables at compile time.
pub const VERSION: &str = "3.16.0";
pub const BRANCH: &str = match option_env!("PROMETHEUS_RS_BRANCH") {
    Some(b) => b,
    None => "HEAD",
};
pub const REVISION: &str = match option_env!("PROMETHEUS_RS_REVISION") {
    Some(r) => r,
    None => "unknown",
};
pub const BUILD_USER: &str = match option_env!("PROMETHEUS_RS_BUILD_USER") {
    Some(u) => u,
    None => "prometheus-rs",
};
pub const BUILD_DATE: &str = match option_env!("PROMETHEUS_RS_BUILD_DATE") {
    Some(d) => d,
    None => "unknown",
};
/// Go's `go version` line names the toolchain: here the Rust port.
pub const GO_VERSION: &str = "rust";
pub const PLATFORM: &str = concat!("linux/", "amd64");
pub const TAGS: &str = "builtinassets,stringlabels";

/// Go `version.Info()`.
pub fn version_info() -> String {
    format!("(version={VERSION}, branch={BRANCH}, revision={REVISION})")
}

/// Go `version.BuildContext()`.
pub fn build_context() -> String {
    format!("(go={GO_VERSION}, platform={PLATFORM}, user={BUILD_USER}, date={BUILD_DATE}, tags={TAGS})")
}

/// The remote write protobuf messages (Go `remoteapi.WriteV1MessageType`, `WriteV2MessageType`).
pub const WRITE_V1_MESSAGE_TYPE: &str = "prometheus.WriteRequest";
pub const WRITE_V2_MESSAGE_TYPE: &str = "io.prometheus.write.v2.Request";

/// Go `rwProtoMsgFlagParser.Set`: validates a message type and refuses duplicates.
pub fn rw_proto_msg_set(prev: &[String], opt: &str) -> Result<(), String> {
    if opt != WRITE_V1_MESSAGE_TYPE && opt != WRITE_V2_MESSAGE_TYPE {
        return Err(format!(
            "unknown type for remote write protobuf message {opt}, supported: {WRITE_V1_MESSAGE_TYPE}, {WRITE_V2_MESSAGE_TYPE}"
        ));
    }
    if prev.iter().any(|p| p == opt) {
        return Err(format!("duplicated {opt} flag value, got {} already", prev.join(", ")));
    }
    Ok(())
}

fn log_level_set(_: &[String], v: &str) -> Result<(), String> {
    util::logging::Level::default().set(v)
}

fn log_format_set(_: &[String], v: &str) -> Result<(), String> {
    util::logging::Format::parse(v).map(|_| ())
}

/// The flags that `serverOnlyFlag` defines.
pub const SERVER_ONLY_FLAGS: &[&str] = &[
    "storage.tsdb.path",
    "storage.tsdb.min-block-duration",
    "storage.tsdb.max-block-duration",
    "storage.tsdb.max-block-chunk-segment-size",
    "storage.tsdb.wal-segment-size",
    "storage.tsdb.retention.time",
    "storage.tsdb.retention.size",
    "storage.tsdb.no-lockfile",
    "storage.tsdb.allow-overlapping-compaction",
    "storage.tsdb.wal-compression",
    "storage.tsdb.wal-compression-type",
    "storage.tsdb.head-chunks-write-queue-size",
    "storage.tsdb.samples-per-chunk",
    "storage.tsdb.delayed-compaction.max-percent",
    "storage.tsdb.delay-compact-file.path",
    "storage.tsdb.block-reload-interval",
    "storage.remote.read-sample-limit",
    "storage.remote.read-concurrent-limit",
    "storage.remote.read-max-bytes-in-frame",
    "web.search.max-limit",
    "rules.alert.for-outage-tolerance",
    "rules.alert.for-grace-period",
    "rules.alert.resend-delay",
    "rules.max-concurrent-evals",
    "alertmanager.notification-queue-capacity",
    "alertmanager.notification-batch-size",
    "alertmanager.drain-notification-queue-on-shutdown",
    "query.lookback-delta",
    "query.timeout",
    "query.max-concurrency",
    "query.max-samples",
];

/// The flags that `agentOnlyFlag` defines.
pub const AGENT_ONLY_FLAGS: &[&str] = &[
    "storage.agent.path",
    "storage.agent.wal-segment-size",
    "storage.agent.wal-compression",
    "storage.agent.wal-compression-type",
    "storage.agent.wal-truncate-frequency",
    "storage.agent.retention.min-time",
    "storage.agent.retention.max-time",
    "storage.agent.checkpoint-from-in-memory-series",
    "storage.agent.checkpoint-batch-size",
    "storage.agent.no-lockfile",
];

fn server_only(name: &'static str, help: &str, kind: Kind) -> Flag {
    Flag::new(name, format!("{help} Use with server mode only."), kind)
}

fn agent_only(name: &'static str, help: &str, kind: Kind) -> Flag {
    Flag::new(name, format!("{help} Use with agent mode only."), kind)
}

/// The `--enable-feature` help (its list is what TestFeatureFlagsDocumented checks against the docs).
pub const ENABLE_FEATURE_HELP: &str = "Comma separated feature names to enable. Valid options: concurrent-rule-eval, created-timestamp-zero-ingestion, delayed-compaction, exemplar-storage, extra-scrape-metrics, histograms-st-encoding, memory-snapshot-on-shutdown, metadata-wal-records, old-ui, openmetrics2, otlp-deltatocumulative, otlp-native-delta-ingestion, promql-binop-fill-modifiers, promql-delayed-name-removal, promql-experimental-functions, promql-per-step-stats, promql-unquoted-utf8-names, query-cost, search-api, st-storage, st-synthesis, type-and-unit-labels, use-start-timestamps, use-uncached-io, xor2-encoding, zstd-scrape. See https://prometheus.io/docs/prometheus/latest/feature_flags/ for more details.";

/// The agent's default WAL retention bounds (Go `agent.DefaultMinWALTime`, `agent.DefaultMaxWALTime`, in ms).
pub const AGENT_DEFAULT_MIN_WAL_TIME_MS: i64 = 5 * 60 * 1000;
pub const AGENT_DEFAULT_MAX_WAL_TIME_MS: i64 = 4 * 60 * 60 * 1000;

/// The application with every flag of Go's `main()`, in the same order.
pub fn app(name: &str) -> App {
    use Kind::*;
    let min_wal = yaml::types::Duration::from_millis(AGENT_DEFAULT_MIN_WAL_TIME_MS).go_string();
    let max_wal = yaml::types::Duration::from_millis(AGENT_DEFAULT_MAX_WAL_TIME_MS).go_string();
    let flags = vec![
        Flag::new("config.file", "Prometheus configuration file path.", String).default("prometheus.yml"),
        Flag::new("config.auto-reload", "Enable automatic configuration file reloading. See also --config.auto-reload-interval.", Bool).default("false"),
        Flag::new("config.auto-reload-interval", "Specifies the interval for checking and automatically reloading the Prometheus configuration file upon detecting changes. Only used when --config.auto-reload is set.", ModelDuration).default("30s"),
        Flag::new("web.listen-address", "Address to listen on for UI, API, and telemetry. Can be repeated.", Strings).default("0.0.0.0:9090"),
        Flag::new("auto-gomaxprocs", "Automatically set GOMAXPROCS to match Linux container CPU quota", Bool).default("true"),
        Flag::new("auto-gomemlimit", "Automatically set GOMEMLIMIT to match Linux container or system memory limit", Bool).default("true"),
        Flag::new("auto-gomemlimit.ratio", "The ratio of reserved GOMEMLIMIT memory to the detected maximum container or system memory", Float).default("0.9"),
        Flag::new("auto-gomemlimit.refresh-interval", "Interval at which to re-detect the container or system memory limit and update GOMEMLIMIT accordingly. Useful when the limit can change at runtime, e.g. with a Vertical Pod Autoscaler. Set to 0 to detect the limit only once at startup. Note that a downward change in the limit can cause a temporary increase in garbage collection activity. Only used when --auto-gomemlimit is set.", ModelDuration).default("0s"),
        Flag::new("web.config.file", "[EXPERIMENTAL] Path to configuration file that can enable TLS or authentication.", String).default(""),
        Flag::new("web.read-timeout", "Maximum duration before timing out read of the request, and closing idle connections.", ModelDuration).default("5m"),
        Flag::new("web.max-connections", "Maximum number of simultaneous connections across all listeners.", Int).default("512"),
        Flag::new("web.max-notifications-subscribers", "Limits the maximum number of subscribers that can concurrently receive live notifications. If the limit is reached, new subscription requests will be denied until existing connections close.", Int).default("16"),
        Flag::new("web.external-url", "The URL under which Prometheus is externally reachable (for example, if Prometheus is served via a reverse proxy). Used for generating relative and absolute links back to Prometheus itself. If the URL has a path portion, it will be used to prefix all HTTP endpoints served by Prometheus. If omitted, relevant URL components will be derived automatically.", String).placeholder("<URL>"),
        Flag::new("web.route-prefix", "Prefix for the internal routes of web endpoints. Defaults to path of --web.external-url.", String).placeholder("<path>"),
        Flag::new("web.user-assets", "Path to static asset directory, available at /user.", String).placeholder("<path>"),
        Flag::new("web.enable-lifecycle", "Enable shutdown and reload via HTTP request.", Bool).default("false"),
        Flag::new("web.enable-admin-api", "Enable API endpoints for admin control actions.", Bool).default("false"),
        Flag::new("web.enable-remote-write-receiver", "Enable API endpoint accepting remote write requests.", Bool).default("false"),
        {
            let mut f = Flag::new(
                "web.remote-write-receiver.accepted-protobuf-messages",
                format!("List of the remote write protobuf messages to accept when receiving the remote writes. Supported values: {WRITE_V1_MESSAGE_TYPE}, {WRITE_V2_MESSAGE_TYPE}"),
                Custom(rw_proto_msg_set, true),
            );
            f.default = vec![WRITE_V1_MESSAGE_TYPE.to_string(), WRITE_V2_MESSAGE_TYPE.to_string()];
            f
        },
        Flag::new("web.enable-otlp-receiver", "Enable API endpoint accepting OTLP write requests.", Bool).default("false"),
        Flag::new("web.console.templates", "Path to the console template directory, available at /consoles.", String).default("consoles"),
        Flag::new("web.console.libraries", "Path to the console library directory.", String).default("console_libraries"),
        Flag::new("web.page-title", "Document title of Prometheus instance.", String).default("Prometheus Time Series Collection and Processing Server"),
        Flag::new("web.cors.origin", "Regex for CORS origin. It is fully anchored. Example: 'https?://(domain1|domain2)\\.com'", String).default(".*"),
        server_only("storage.tsdb.path", "Base path for metrics storage.", String).default("data/"),
        server_only("storage.tsdb.min-block-duration", "Minimum duration of a data block before being persisted. For use in testing.", ModelDuration).hidden().default("2h"),
        server_only("storage.tsdb.max-block-duration", "Maximum duration compacted blocks may span. For use in testing. (Defaults to 10% of the retention period.)", ModelDuration).hidden().placeholder("<duration>"),
        server_only("storage.tsdb.max-block-chunk-segment-size", "The maximum size for a single chunk segment in a block. Example: 512MB", Bytes).hidden().placeholder("<bytes>"),
        server_only("storage.tsdb.wal-segment-size", "Size at which to split the tsdb WAL segment files. Example: 100MB", Bytes).hidden().placeholder("<bytes>"),
        server_only("storage.tsdb.retention.time", &format!("[DEPRECATED] How long to retain samples in storage. If neither this flag nor \"storage.tsdb.retention.size\" is set, the retention time defaults to {DEFAULT_RETENTION_STRING}. Units Supported: y, w, d, h, m, s, ms. This flag has been deprecated, use the storage.tsdb.retention.time field in the config file instead."), ModelDuration),
        server_only("storage.tsdb.retention.size", "[DEPRECATED] Maximum number of bytes that can be stored for blocks. A unit is required, supported units: B, KB, MB, GB, TB, PB, EB. Ex: \"512MB\". Based on powers-of-2, so 1KB is 1024B. This flag has been deprecated, use the storage.tsdb.retention.size field in the config file instead.", Bytes),
        server_only("storage.tsdb.no-lockfile", "Do not create lockfile in data directory.", Bool).default("false"),
        server_only("storage.tsdb.allow-overlapping-compaction", "Allow compaction of overlapping blocks. If set to false, TSDB stops vertical compaction and leaves overlapping blocks there. The use case is to let another component handle the compaction of overlapping blocks.", Bool).default("true").hidden(),
        server_only("storage.tsdb.wal-compression", "Compress the tsdb WAL. If false, the --storage.tsdb.wal-compression-type flag is ignored.", Bool).hidden().default("true"),
        server_only("storage.tsdb.wal-compression-type", "Compression algorithm for the tsdb WAL, used when --storage.tsdb.wal-compression is true.", Enum(vec!["snappy", "zstd"])).hidden().default("snappy"),
        server_only("storage.tsdb.head-chunks-write-queue-size", "Size of the queue through which head chunks are written to the disk to be m-mapped, 0 disables the queue completely. Experimental.", Int).default("0"),
        server_only("storage.tsdb.samples-per-chunk", "Target number of samples per chunk.", Int).default("120").hidden(),
        server_only("storage.tsdb.delayed-compaction.max-percent", "Sets the upper limit for the random compaction delay, specified as a percentage of the head chunk range. 100 means the compaction can be delayed by up to the entire head chunk range. Only effective when the delayed-compaction feature flag is enabled.", Int).default("10").hidden(),
        server_only("storage.tsdb.delay-compact-file.path", "Path to a JSON file with uploaded TSDB blocks e.g. Thanos shipper meta file. If set TSDB will only compact 1 level blocks that are marked as uploaded in that file, improving external storage integrations e.g. with Thanos sidecar. 1+ level compactions won't be delayed.", String).default(""),
        server_only("storage.tsdb.block-reload-interval", "Interval at which to check for new or removed blocks in storage. Users who manually backfill or drop blocks must wait up to this duration before changes become available.", ModelDuration).default("1m").hidden(),
        agent_only("storage.agent.path", "Base path for metrics storage.", String).default("data-agent/"),
        agent_only("storage.agent.wal-segment-size", "Size at which to split WAL segment files. Example: 100MB", Bytes).hidden().placeholder("<bytes>"),
        agent_only("storage.agent.wal-compression", "Compress the agent WAL. If false, the --storage.agent.wal-compression-type flag is ignored.", Bool).default("true"),
        agent_only("storage.agent.wal-compression-type", "Compression algorithm for the agent WAL, used when --storage.agent.wal-compression is true.", Enum(vec!["snappy", "zstd"])).hidden().default("snappy"),
        agent_only("storage.agent.wal-truncate-frequency", "The frequency at which to truncate the WAL and remove old data.", ModelDuration).hidden().placeholder("<duration>"),
        agent_only("storage.agent.retention.min-time", "Minimum age samples may be before being considered for deletion when the WAL is truncated", ModelDuration).default(&min_wal),
        agent_only("storage.agent.retention.max-time", "Maximum age samples may be before being forcibly deleted when the WAL is truncated", ModelDuration).default(&max_wal),
        agent_only("storage.agent.checkpoint-from-in-memory-series", "Use only in-memory series data when building a checkpoint.", Bool).default("false"),
        agent_only("storage.agent.checkpoint-batch-size", "Size of a single WAL log entry chunk to be flushed. Has no effect without --storage.agent.checkpoint-from-in-memory-series flag.", Int).default("1000"),
        agent_only("storage.agent.no-lockfile", "Do not create lockfile in data directory.", Bool).default("false"),
        Flag::new("storage.remote.flush-deadline", "How long to wait flushing sample on shutdown or config reload.", ModelDuration).default("1m").placeholder("<duration>"),
        server_only("storage.remote.read-sample-limit", "Maximum overall number of samples to return via the remote read interface, in a single query. 0 means no limit. This limit is ignored for streamed response types.", Int).default("5e7"),
        server_only("storage.remote.read-concurrent-limit", "Maximum number of concurrent remote read calls. 0 means no limit.", Int).default("10"),
        server_only("storage.remote.read-max-bytes-in-frame", "Maximum number of bytes in a single frame for streaming remote read response types before marshalling. Note that client might have limit on frame size as well. 1MB as recommended by protobuf by default.", Int).default("1048576"),
        server_only("web.search.max-limit", "Hard upper bound on the \"limit\" query parameter accepted by the experimental search API (--enable-feature=search-api). Requests with a higher limit are rejected with HTTP 400. 0 disables the cap.", Int).default("10000"),
        server_only("rules.alert.for-outage-tolerance", "Max time to tolerate prometheus outage for restoring \"for\" state of alert.", ModelDuration).default("1h"),
        server_only("rules.alert.for-grace-period", "Minimum duration between alert and restored \"for\" state. This is maintained only for alerts with configured \"for\" time greater than grace period.", ModelDuration).default("10m"),
        server_only("rules.alert.resend-delay", "Minimum amount of time to wait before resending an alert to Alertmanager.", ModelDuration).default("1m"),
        server_only("rules.max-concurrent-evals", "Global concurrency limit for independent rules that can run concurrently. When set, \"query.max-concurrency\" may need to be adjusted accordingly.", Int64).default("4"),
        Flag::new("scrape.adjust-timestamps", "Adjust scrape timestamps by up to `scrape.timestamp-tolerance` to align them to the intended schedule. See https://github.com/prometheus/prometheus/issues/7846 for more context. Experimental. This flag will be removed in a future release.", Bool).hidden().default("true"),
        Flag::new("scrape.timestamp-tolerance", "Timestamp tolerance. See https://github.com/prometheus/prometheus/issues/7846 for more context. Experimental. This flag will be removed in a future release.", Duration).hidden().default("2ms"),
        server_only("alertmanager.notification-queue-capacity", "The capacity of the queue for pending Alertmanager notifications.", Int).default("10000"),
        server_only("alertmanager.notification-batch-size", "The maximum number of notifications per batch to send to the Alertmanager.", Int).default(&notifier::DEFAULT_MAX_BATCH_SIZE.to_string()),
        server_only("alertmanager.drain-notification-queue-on-shutdown", "Send any outstanding Alertmanager notifications when shutting down. If false, any outstanding Alertmanager notifications will be dropped when shutting down.", Bool).default("true"),
        server_only("query.lookback-delta", "The maximum lookback duration for retrieving metrics during expression evaluations and federation.", ModelDuration).default("5m"),
        server_only("query.timeout", "Maximum time a query may take before being aborted.", ModelDuration).default("2m"),
        server_only("query.max-concurrency", "Maximum number of queries executed concurrently.", Int).default("20"),
        server_only("query.max-samples", "Maximum number of samples a single query can load into memory. Note that queries will fail if they try to load more samples than this into memory, so this also limits the number of samples a query can return.", Int).default("50000000"),
        Flag::new("scrape.discovery-reload-interval", "Interval used by scrape manager to throttle target groups updates.", ModelDuration).hidden().default("5s"),
        Flag::new("enable-feature", ENABLE_FEATURE_HELP, Strings),
        Flag::new("agent", "Run Prometheus in 'Agent mode'.", Bool),
        Flag::new("log.level", format!("{} Deprecated: set runtime.log_level in the configuration file instead.", util::logging::LEVEL_FLAG_HELP), Custom(log_level_set, false)).default("info"),
        Flag::new("log.format", util::logging::FORMAT_FLAG_HELP, Custom(log_format_set, false)).default("logfmt"),
        Flag::new("write-documentation", "Generate command line documentation. Internal use.", Bool).hidden(),
    ];
    App::new(name, "The Prometheus monitoring server", &version_print("prometheus"), flags, Vec::new())
}

/// The values of every flag (Go `flagConfig` plus the local flag variables of `main`). Durations are
/// nanoseconds, sizes bytes.
#[derive(Clone, Debug, Default)]
pub struct FlagConfig {
    pub config_file: String,
    pub enable_auto_reload: bool,
    pub auto_reload_interval: i64,
    pub listen_addresses: Vec<String>,
    pub maxprocs_enable: bool,
    pub memlimit_enable: bool,
    pub memlimit_ratio: f64,
    pub memlimit_refresh_interval: i64,
    pub web_config_file: String,
    pub web_timeout: i64,
    pub max_connections: i64,
    pub max_notifications_subscribers: i64,
    pub prometheus_url: String,
    pub route_prefix: String,
    pub user_assets_path: String,
    pub enable_lifecycle: bool,
    pub enable_admin_api: bool,
    pub enable_remote_write_receiver: bool,
    pub accept_remote_write_proto_msgs: Vec<String>,
    pub enable_otlp_write_receiver: bool,
    pub console_templates_path: String,
    pub console_libraries_path: String,
    pub page_title: String,
    pub cors_regex_string: String,
    pub server_storage_path: String,
    pub agent_storage_path: String,
    pub tsdb: TsdbFlags,
    pub agent: AgentFlags,
    pub tsdb_wal_compression: bool,
    pub tsdb_wal_compression_type: String,
    pub tsdb_delay_compact_file_path: String,
    pub agent_wal_compression: bool,
    pub agent_wal_compression_type: String,
    pub remote_flush_deadline: i64,
    pub remote_read_sample_limit: i64,
    pub remote_read_concurrency_limit: i64,
    pub remote_read_bytes_in_frame: i64,
    pub max_search_limit: i64,
    pub outage_tolerance: i64,
    pub for_grace_period: i64,
    pub resend_delay: i64,
    pub max_concurrent_evals: i64,
    pub scrape_adjust_timestamps: bool,
    pub scrape_timestamp_tolerance: i64,
    pub notifier_queue_capacity: i64,
    pub notifier_max_batch_size: i64,
    pub notifier_drain_on_shutdown: bool,
    pub lookback_delta: i64,
    pub query_timeout: i64,
    pub query_concurrency: i64,
    pub query_max_samples: i64,
    pub discovery_reload_interval: i64,
    pub feature_list: Vec<String>,
    pub agent_mode: bool,
    pub log_level: String,
    pub log_format: String,
    pub write_documentation: bool,
    /// The flags the user gave that only server mode accepts (`--name`), in order.
    pub server_only_flags: Vec<String>,
    /// The flags the user gave that only agent mode accepts.
    pub agent_only_flags: Vec<String>,
    /// Every flag value as Go's `a.Model().Flags` `f.Value.String()` gives it (for `/api/v1/status/flags`).
    pub flag_values: std::collections::BTreeMap<String, String>,
    pub features: crate::features::FeatureOptions,
}

/// The TSDB flags (Go `tsdbOptions` as the flags set them).
#[derive(Clone, Debug, Default)]
pub struct TsdbFlags {
    pub wal_segment_size: i64,
    pub max_block_chunk_segment_size: i64,
    pub retention_duration: i64,
    pub max_bytes: i64,
    pub no_lockfile: bool,
    pub enable_overlapping_compaction: bool,
    pub head_chunks_write_queue_size: i64,
    pub samples_per_chunk: i64,
    pub compaction_delay_max_percent: i64,
    pub min_block_duration: i64,
    pub max_block_duration: i64,
    pub block_reload_interval: i64,
}

/// The agent flags (Go `agentOptions` as the flags set them).
#[derive(Clone, Debug, Default)]
pub struct AgentFlags {
    pub wal_segment_size: i64,
    pub truncate_frequency: i64,
    pub min_wal_time: i64,
    pub max_wal_time: i64,
    pub no_lockfile: bool,
    pub checkpoint_from_in_memory_series: bool,
    pub checkpoint_batch_size: i64,
}

/// Go `kingpin.Value.String()` of a flag after parsing, as `/api/v1/status/flags` shows it.
fn flag_value_string(p: &Parsed, f: &Flag) -> String {
    match &f.kind {
        Kind::Strings | Kind::Custom(_, true) => {
            let v = p.strings(f.name);
            match &f.kind {
                // Go `rwProtoMsgFlagParser.String()` joins with ","; kingpin's strings value prints `[a b]`.
                Kind::Custom(..) => v.join(","),
                // kingpin `stringsValue.String()` joins with ",".
                _ => v.join(","),
            }
        }
        Kind::Bool => {
            if p.is_set(f.name) {
                p.bool(f.name).to_string()
            } else {
                "false".to_string()
            }
        }
        Kind::Bytes => {
            if p.is_set(f.name) {
                yaml::types::Base2Bytes(p.bytes(f.name)).go_string()
            } else {
                "0B".to_string()
            }
        }
        Kind::ModelDuration => yaml::types::Duration(p.model_duration(f.name)).go_string(),
        Kind::Duration => crate::helpers::go_duration_string(p.duration(f.name)),
        Kind::Int | Kind::Int64 => p.int(f.name).to_string(),
        Kind::Float => crate::helpers::format_float_g(p.float(f.name)),
        _ => p.str(f.name),
    }
}

impl FlagConfig {
    /// Reads the parsed values (Go: the `...Var` targets after `a.Parse`).
    pub fn from_parsed(app: &App, p: &Parsed) -> FlagConfig {
        let mut c = FlagConfig {
            config_file: p.str("config.file"),
            enable_auto_reload: p.bool("config.auto-reload"),
            auto_reload_interval: p.model_duration("config.auto-reload-interval"),
            listen_addresses: p.strings("web.listen-address"),
            maxprocs_enable: p.bool("auto-gomaxprocs"),
            memlimit_enable: p.bool("auto-gomemlimit"),
            memlimit_ratio: p.float("auto-gomemlimit.ratio"),
            memlimit_refresh_interval: p.model_duration("auto-gomemlimit.refresh-interval"),
            web_config_file: p.str("web.config.file"),
            web_timeout: p.model_duration("web.read-timeout"),
            max_connections: p.int("web.max-connections"),
            max_notifications_subscribers: p.int("web.max-notifications-subscribers"),
            prometheus_url: p.str("web.external-url"),
            route_prefix: p.str("web.route-prefix"),
            user_assets_path: p.str("web.user-assets"),
            enable_lifecycle: p.bool("web.enable-lifecycle"),
            enable_admin_api: p.bool("web.enable-admin-api"),
            enable_remote_write_receiver: p.bool("web.enable-remote-write-receiver"),
            accept_remote_write_proto_msgs: p.strings("web.remote-write-receiver.accepted-protobuf-messages"),
            enable_otlp_write_receiver: p.bool("web.enable-otlp-receiver"),
            console_templates_path: p.str("web.console.templates"),
            console_libraries_path: p.str("web.console.libraries"),
            page_title: p.str("web.page-title"),
            cors_regex_string: p.str("web.cors.origin"),
            server_storage_path: p.str("storage.tsdb.path"),
            agent_storage_path: p.str("storage.agent.path"),
            tsdb: TsdbFlags {
                wal_segment_size: p.bytes("storage.tsdb.wal-segment-size"),
                max_block_chunk_segment_size: p.bytes("storage.tsdb.max-block-chunk-segment-size"),
                retention_duration: p.model_duration("storage.tsdb.retention.time"),
                max_bytes: p.bytes("storage.tsdb.retention.size"),
                no_lockfile: p.bool("storage.tsdb.no-lockfile"),
                enable_overlapping_compaction: p.bool("storage.tsdb.allow-overlapping-compaction"),
                head_chunks_write_queue_size: p.int("storage.tsdb.head-chunks-write-queue-size"),
                samples_per_chunk: p.int("storage.tsdb.samples-per-chunk"),
                compaction_delay_max_percent: p.int("storage.tsdb.delayed-compaction.max-percent"),
                min_block_duration: p.model_duration("storage.tsdb.min-block-duration"),
                max_block_duration: p.model_duration("storage.tsdb.max-block-duration"),
                block_reload_interval: p.model_duration("storage.tsdb.block-reload-interval"),
            },
            agent: AgentFlags {
                wal_segment_size: p.bytes("storage.agent.wal-segment-size"),
                truncate_frequency: p.model_duration("storage.agent.wal-truncate-frequency"),
                min_wal_time: p.model_duration("storage.agent.retention.min-time"),
                max_wal_time: p.model_duration("storage.agent.retention.max-time"),
                no_lockfile: p.bool("storage.agent.no-lockfile"),
                checkpoint_from_in_memory_series: p.bool("storage.agent.checkpoint-from-in-memory-series"),
                checkpoint_batch_size: p.int("storage.agent.checkpoint-batch-size"),
            },
            tsdb_wal_compression: p.bool("storage.tsdb.wal-compression"),
            tsdb_wal_compression_type: p.str("storage.tsdb.wal-compression-type"),
            tsdb_delay_compact_file_path: p.str("storage.tsdb.delay-compact-file.path"),
            agent_wal_compression: p.bool("storage.agent.wal-compression"),
            agent_wal_compression_type: p.str("storage.agent.wal-compression-type"),
            remote_flush_deadline: p.model_duration("storage.remote.flush-deadline"),
            remote_read_sample_limit: p.int("storage.remote.read-sample-limit"),
            remote_read_concurrency_limit: p.int("storage.remote.read-concurrent-limit"),
            remote_read_bytes_in_frame: p.int("storage.remote.read-max-bytes-in-frame"),
            max_search_limit: p.int("web.search.max-limit"),
            outage_tolerance: p.model_duration("rules.alert.for-outage-tolerance"),
            for_grace_period: p.model_duration("rules.alert.for-grace-period"),
            resend_delay: p.model_duration("rules.alert.resend-delay"),
            max_concurrent_evals: p.int64("rules.max-concurrent-evals"),
            scrape_adjust_timestamps: p.bool("scrape.adjust-timestamps"),
            scrape_timestamp_tolerance: p.duration("scrape.timestamp-tolerance"),
            notifier_queue_capacity: p.int("alertmanager.notification-queue-capacity"),
            notifier_max_batch_size: p.int("alertmanager.notification-batch-size"),
            notifier_drain_on_shutdown: p.bool("alertmanager.drain-notification-queue-on-shutdown"),
            lookback_delta: p.model_duration("query.lookback-delta"),
            query_timeout: p.model_duration("query.timeout"),
            query_concurrency: p.int("query.max-concurrency"),
            query_max_samples: p.int("query.max-samples"),
            discovery_reload_interval: p.model_duration("scrape.discovery-reload-interval"),
            feature_list: p.strings("enable-feature"),
            agent_mode: p.bool("agent"),
            log_level: p.str("log.level"),
            log_format: p.str("log.format"),
            write_documentation: p.bool("write-documentation"),
            ..Default::default()
        };
        for name in &p.user_flags {
            if SERVER_ONLY_FLAGS.contains(&name.as_str()) {
                c.server_only_flags.push(format!("--{name}"));
            }
            if AGENT_ONLY_FLAGS.contains(&name.as_str()) {
                c.agent_only_flags.push(format!("--{name}"));
            }
        }
        // Go: every flag except kingpin's own (help, version and the hidden help/completion flags).
        let boilerplate = ["help", "help-long", "help-man", "completion-bash", "completion-script-bash", "completion-script-zsh", "version"];
        for f in &app.flags {
            if boilerplate.contains(&f.name) {
                continue;
            }
            c.flag_values.insert(f.name.to_string(), flag_value_string(p, f));
        }
        c
    }
}
