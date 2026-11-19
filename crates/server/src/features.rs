//! `--enable-feature` (Go `flagConfig.setFeatureListOptions`) and the process-wide feature registry
//! (Go `features.DefaultRegistry`).

use std::sync::{Arc, OnceLock};

use discovery::Logger;
use promql::features::{Collector, Registry};

/// Go feature categories that the server sets (the PromQL ones are in `promql::features`).
pub const PROMETHEUS: &str = "prometheus";
pub const TSDB: &str = "tsdb";
pub const SCRAPE: &str = "scrape";

/// Go `features.DefaultRegistry`.
pub fn default_registry() -> &'static Arc<Registry> {
    static R: OnceLock<Arc<Registry>> = OnceLock::new();
    R.get_or_init(|| Arc::new(Registry::new()))
}

/// The options that feature flags set (the fields of Go's `flagConfig` sub-options they change).
#[derive(Clone, Debug, Default)]
pub struct FeatureOptions {
    // tsdb (Go c.tsdb.*)
    pub tsdb_enable_exemplar_storage: bool,
    pub tsdb_enable_memory_snapshot_on_shutdown: bool,
    pub tsdb_enable_metadata_wal_records: bool,
    pub tsdb_enable_st_as_zero_sample: bool,
    /// "xor2" when a feature selects XOR2 (Go `c.tsdb.FloatChunkEncoding = chunkenc.EncXOR2`); "" keeps XOR.
    pub tsdb_float_chunk_encoding_xor2: bool,
    pub tsdb_enable_histogram_st_encoding: bool,
    pub tsdb_enable_st_storage: bool,
    pub tsdb_enable_delayed_compaction: bool,
    pub tsdb_use_uncached_io: bool,
    pub tsdb_enable_fast_startup: bool,
    // agent (Go c.agent.*)
    pub agent_enable_metadata_wal_records: bool,
    pub agent_enable_st_as_zero_sample: bool,
    pub agent_enable_st_storage: bool,
    // scrape (Go c.scrape.*)
    pub scrape_append_metadata: bool,
    pub scrape_enable_start_timestamp_zero_ingestion: bool,
    pub scrape_parse_st: bool,
    pub scrape_synthesize_st: bool,
    pub scrape_enable_open_metrics2: bool,
    pub scrape_enable_type_and_unit_labels: bool,
    pub scrape_enable_zstd_scrape: bool,
    // web (Go c.web.*)
    pub web_append_metadata: bool,
    pub web_use_old_ui: bool,
    pub web_convert_otlp_delta: bool,
    pub web_native_otlp_delta_ingestion: bool,
    pub web_enable_type_and_unit_labels: bool,
    pub web_enable_search: bool,
    pub web_enable_query_cost: bool,
    // parser (Go c.parserOpts.*)
    pub parser_enable_unquoted_utf8_names: bool,
    pub parser_enable_experimental_functions: bool,
    pub parser_enable_binop_fill_modifiers: bool,
    // the rest of flagConfig
    pub enable_per_step_stats: bool,
    pub enable_auto_reload: bool,
    pub enable_concurrent_rule_eval: bool,
    pub use_start_timestamps: bool,
    pub enable_query_cost: bool,
    pub promql_enable_delayed_name_removal: bool,
}

impl FeatureOptions {
    /// The PromQL parser options.
    pub fn parser_options(&self) -> promql::parser::Options {
        promql::parser::Options {
            enable_experimental_functions: self.parser_enable_experimental_functions,
            enable_binop_fill_modifiers: self.parser_enable_binop_fill_modifiers,
            enable_unquoted_utf8_names: self.parser_enable_unquoted_utf8_names,
            functions: None,
        }
    }
}

/// Go `setFeatureListOptions`: applies each comma separated feature of `feature_list`, logging as Go does.
/// Changes the global config defaults where Go does (`extra-scrape-metrics`, `created-timestamp-zero-ingestion`,
/// `st-storage`).
pub fn set_feature_list_options(c: &mut FeatureOptions, feature_list: &[String], logger: &Logger) -> Result<(), String> {
    for f in feature_list {
        for o in f.split(',') {
            match o {
                "exemplar-storage" => {
                    c.tsdb_enable_exemplar_storage = true;
                    logger.info("Experimental in-memory exemplar storage enabled", &[]);
                }
                "memory-snapshot-on-shutdown" => {
                    c.tsdb_enable_memory_snapshot_on_shutdown = true;
                    logger.info("Experimental memory snapshot on shutdown enabled", &[]);
                }
                "extra-scrape-metrics" => {
                    config::set_default_global_config(|g| g.extra_scrape_metrics = Some(true));
                    logger.warn("This option for --enable-feature is being phased out. It currently changes the default for the extra_scrape_metrics config setting to true, but will become a no-op in a future version. Stop using this option and set extra_scrape_metrics in the config instead.", &[("option", o)]);
                }
                "metadata-wal-records" => {
                    c.scrape_append_metadata = true;
                    c.web_append_metadata = true;
                    c.tsdb_enable_metadata_wal_records = true;
                    c.agent_enable_metadata_wal_records = true;
                    default_registry().enable(TSDB, "metadata_wal_records");
                    logger.info("Experimental metadata records in WAL enabled", &[]);
                }
                "promql-per-step-stats" => {
                    c.enable_per_step_stats = true;
                    logger.info("Experimental per-step statistics reporting", &[]);
                }
                "auto-reload-config" => {
                    c.enable_auto_reload = true;
                    logger.warn("This option for --enable-feature is deprecated. Use --config.auto-reload instead.", &[("option", o)]);
                }
                "concurrent-rule-eval" => {
                    c.enable_concurrent_rule_eval = true;
                    logger.info("Experimental concurrent rule evaluation enabled.", &[]);
                }
                "promql-unquoted-utf8-names" => {
                    c.parser_enable_unquoted_utf8_names = true;
                    logger.info("Experimental unquoted UTF-8 names enabled.", &[]);
                }
                "promql-experimental-functions" => {
                    c.parser_enable_experimental_functions = true;
                    logger.info("Experimental PromQL functions enabled.", &[]);
                }
                "promql-duration-expr" => {
                    logger.warn("This option for --enable-feature is now permanently enabled and therefore a no-op.", &[("option", o)]);
                }
                "native-histograms" => {
                    logger.warn("This option for --enable-feature is a no-op. To scrape native histograms, set the scrape_native_histograms scrape config setting to true.", &[("option", o)]);
                }
                "ooo-native-histograms" => {
                    logger.warn("This option for --enable-feature is now permanently enabled and therefore a no-op.", &[("option", o)]);
                }
                "created-timestamp-zero-ingestion" => {
                    c.scrape_enable_start_timestamp_zero_ingestion = true;
                    c.scrape_parse_st = true;
                    c.tsdb_enable_st_as_zero_sample = true;
                    c.agent_enable_st_as_zero_sample = true;
                    let protos = proto_first_protocols();
                    logger.info("Experimental start timestamp zero ingestion enabled. OpenMetrics 1.0 parsing will parse <metric>_created metrics as ST instead of normal sample. Changed default scrape_protocols to prefer PrometheusProto format.", &[("global.scrape_protocols", &protos)]);
                }
                "xor2-encoding" => {
                    c.tsdb_float_chunk_encoding_xor2 = true;
                    logger.warn("This option for --enable-feature is being phased out. It currently changes the default for the storage.tsdb.chunk_encoding.floats config setting to xor2, but will become a no-op in a future major version. Stop using this option and set storage.tsdb.chunk_encoding.floats in the config instead.", &[("option", o)]);
                }
                "histograms-st-encoding" => {
                    c.tsdb_enable_histogram_st_encoding = true;
                    logger.info("Experimental ST-capable histogram chunk encoding enabled.", &[]);
                }
                "st-synthesis" => {
                    c.scrape_synthesize_st = true;
                    default_registry().enable(SCRAPE, "st-synthesis");
                    logger.info("Experimental start timestamp synthesis enabled.", &[]);
                }
                "st-storage" => {
                    c.scrape_parse_st = true;
                    c.tsdb_enable_st_storage = true;
                    c.tsdb_float_chunk_encoding_xor2 = true;
                    c.tsdb_enable_histogram_st_encoding = true;
                    c.agent_enable_st_storage = true;
                    let protos = proto_first_protocols();
                    logger.info("Experimental start timestamp storage enabled. XOR2 and ST-capable histogram chunk encodings enabled. OpenMetrics 1.0 parsing will parse <metric>_created metrics as ST instead of normal sample. Changed default scrape_protocols to prefer PrometheusProto format.", &[("global.scrape_protocols", &protos)]);
                }
                "use-start-timestamps" => {
                    c.use_start_timestamps = true;
                    logger.info("Experimental usage of start timestamps in PromQL engine is enabled.", &[]);
                }
                "delayed-compaction" => {
                    c.tsdb_enable_delayed_compaction = true;
                    logger.info("Experimental delayed compaction is enabled.", &[]);
                }
                "promql-delayed-name-removal" => {
                    c.promql_enable_delayed_name_removal = true;
                    logger.info("Experimental PromQL delayed name removal enabled.", &[]);
                }
                "promql-extended-range-selectors" => {
                    logger.warn("This option for --enable-feature is now permanently enabled and therefore a no-op.", &[("option", o)]);
                }
                "promql-binop-fill-modifiers" => {
                    c.parser_enable_binop_fill_modifiers = true;
                    logger.info("Experimental PromQL binary operator fill modifiers enabled.", &[]);
                }
                "old-ui" => {
                    c.web_use_old_ui = true;
                    logger.info("Serving previous version of the Prometheus web UI.", &[]);
                }
                "otlp-deltatocumulative" => {
                    c.web_convert_otlp_delta = true;
                    logger.info("Converting delta OTLP metrics to cumulative", &[]);
                }
                "otlp-native-delta-ingestion" => {
                    c.web_native_otlp_delta_ingestion = true;
                    logger.info("Enabling native ingestion of delta OTLP metrics, storing the raw sample values without conversion. WARNING: Delta support is in an early stage of development. The ingestion and querying process is likely to change over time.", &[]);
                }
                "openmetrics2" => {
                    c.scrape_enable_open_metrics2 = true;
                    logger.info("Experimental OpenMetrics 2.0 scrape format enabled. WARNING: OpenMetrics 2.0 is not stable yet, the parser will be made stricter before stabilization and the exposition format may still change.", &[]);
                }
                "type-and-unit-labels" => {
                    c.scrape_enable_type_and_unit_labels = true;
                    c.web_enable_type_and_unit_labels = true;
                    logger.info("Experimental type and unit labels enabled", &[]);
                }
                "zstd-scrape" => {
                    c.scrape_enable_zstd_scrape = true;
                    logger.info("Experimental zstd scrape compression enabled", &[]);
                }
                "use-uncached-io" => {
                    if !tsdb_format::fileutil::uncached_io_supported() {
                        return Err("experimental Uncached IO is not supported".to_string());
                    }
                    c.tsdb_use_uncached_io = true;
                    logger.info("Experimental Uncached IO is enabled.", &[]);
                }
                "fast-startup" => {
                    c.tsdb_enable_fast_startup = true;
                    logger.info("Experimental fast startup is enabled.", &[]);
                }
                "search-api" => {
                    c.web_enable_search = true;
                    logger.info("Experimental search API enabled.", &[]);
                }
                "query-cost" => {
                    c.enable_query_cost = true;
                    c.web_enable_query_cost = true;
                    logger.info("Experimental query cost guardrails enabled.", &[]);
                }
                _ => {
                    logger.warn("Unknown option for --enable-feature", &[("option", o)]);
                }
            }
        }
    }
    if c.web_convert_otlp_delta && c.web_native_otlp_delta_ingestion {
        return Err("cannot enable otlp-deltatocumulative and otlp-native-delta-ingestion features at the same time".to_string());
    }
    Ok(())
}

/// Sets the default scrape protocols to the proto-first list (Go assigns `config.DefaultProtoFirstScrapeProtocols`
/// to the global defaults) and returns Go's `fmt.Sprintf("%v", ...)` of the list.
fn proto_first_protocols() -> String {
    let protos = config::default_proto_first_scrape_protocols();
    config::set_default_global_config(|g| g.scrape_protocols = Some(protos.clone()));
    format!("[{}]", protos.iter().map(|p| p.as_str().to_string()).collect::<Vec<_>>().join(" "))
}
