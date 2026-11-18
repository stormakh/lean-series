//! Port of Prometheus `config/`: the configuration file (`config.go`) and its checksum (`reload.go`).
//!
//! Load with [`load`] or [`load_file`]; get the validated scrape configs (with `scrape_config_files`) with
//! [`Config::get_scrape_configs`]. `Config::to_string` is Go's `Config.String()` (YAML, secrets hidden).
pub mod azuread;
pub mod reload;

use std::collections::BTreeMap;
use std::sync::{OnceLock, RwLock};

use discovery::{Configs, Logger, StaticConfig};
use model::labels::{self, Labels, ValidationScheme};
use model::labelset::{ADDRESS_LABEL, LabelSet};
use model::relabel;
use net::{HttpClientConfig, SigV4Config, TlsConfig};
use yaml::{Base2Bytes, Decoder, Duration, Error as YError, Marshal, Node, Struct, Unmarshal, Url, Value};

pub use azuread::{AzureAdConfig, GoogleIamConfig};
pub use reload::generate_checksum;

fn msg(s: impl Into<String>) -> YError {
    YError::Msg(s.into())
}

/// Go `%v` of a string slice: `[a b c]`.
fn go_list<S: AsRef<str>>(v: &[S]) -> String {
    let parts: Vec<&str> = v.iter().map(|s| s.as_ref()).collect();
    format!("[{}]", parts.join(" "))
}

// ---------------- constants ----------------

/// prometheus/common model escaping scheme names.
pub const ALLOW_UTF8: &str = "allow-utf-8";
pub const ESCAPE_UNDERSCORES: &str = "underscores";
pub const ESCAPE_DOTS: &str = "dots";
pub const ESCAPE_VALUES: &str = "values";
/// Go `model.EscapingKey`.
pub const ESCAPING_KEY: &str = "escaping";
/// Go `UTF8NamesHeader`.
pub const UTF8_NAMES_HEADER: &str = "escaping=allow-utf-8";

pub const DEFAULT_GO_GC_PERCENTAGE: i64 = 75;
/// Go `DefaultChunkedReadLimit` (5e+7).
pub const DEFAULT_CHUNKED_READ_LIMIT: u64 = 50_000_000;

pub const FLOAT_CHUNK_ENCODING_XOR: &str = "xor";
pub const FLOAT_CHUNK_ENCODING_XOR2: &str = "xor2";

pub const TRACING_CLIENT_HTTP: &str = "http";
pub const TRACING_CLIENT_GRPC: &str = "grpc";
pub const GZIP_COMPRESSION: &str = "gzip";

/// otlptranslator translation strategies.
pub const UNDERSCORE_ESCAPING_WITH_SUFFIXES: &str = "UnderscoreEscapingWithSuffixes";
pub const UNDERSCORE_ESCAPING_WITHOUT_SUFFIXES: &str = "UnderscoreEscapingWithoutSuffixes";
pub const NO_UTF8_ESCAPING_WITH_SUFFIXES: &str = "NoUTF8EscapingWithSuffixes";
pub const NO_TRANSLATION: &str = "NoTranslation";

/// Remote write protobuf messages (client_golang exp/api/remote).
pub const WRITE_V1_MESSAGE_TYPE: &str = "prometheus.WriteRequest";
pub const WRITE_V2_MESSAGE_TYPE: &str = "io.prometheus.write.v2.Request";

/// Headers that `remote_write`/`remote_read` `headers` may not set (lower case; authorization is checked apart).
pub const RESERVED_HEADERS: [&str; 16] = [
    "host",
    "content-encoding",
    "content-length",
    "content-type",
    "user-agent",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "www-authenticate",
    "accept-encoding",
    "x-prometheus-remote-write-version",
    "x-prometheus-remote-read-version",
    "x-amz-date",
    "x-amz-security-token",
    "x-amz-content-sha256",
];

// ---------------- ScrapeProtocol ----------------

/// A scrape protocol name (Go `ScrapeProtocol`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ScrapeProtocol(pub String);

pub const PROMETHEUS_PROTO: &str = "PrometheusProto";
pub const PROMETHEUS_TEXT_0_0_4: &str = "PrometheusText0.0.4";
pub const PROMETHEUS_TEXT_1_0_0: &str = "PrometheusText1.0.0";
pub const OPEN_METRICS_TEXT_0_0_1: &str = "OpenMetricsText0.0.1";
pub const OPEN_METRICS_TEXT_1_0_0: &str = "OpenMetricsText1.0.0";
pub const OPEN_METRICS_TEXT_2_0_0: &str = "OpenMetricsText2.0.0";

/// Go `ScrapeProtocolsHeaders`, sorted by protocol name.
pub const SCRAPE_PROTOCOLS_HEADERS: [(&str, &str); 6] = [
    (OPEN_METRICS_TEXT_0_0_1, "application/openmetrics-text;version=0.0.1"),
    (OPEN_METRICS_TEXT_1_0_0, "application/openmetrics-text;version=1.0.0"),
    (OPEN_METRICS_TEXT_2_0_0, "application/openmetrics-text;version=2.0.0"),
    (PROMETHEUS_PROTO, "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited"),
    (PROMETHEUS_TEXT_0_0_4, "text/plain;version=0.0.4"),
    (PROMETHEUS_TEXT_1_0_0, "text/plain;version=1.0.0"),
];

impl ScrapeProtocol {
    pub fn new(s: &str) -> ScrapeProtocol {
        ScrapeProtocol(s.to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// The Accept header value of the protocol (Go `ScrapeProtocolsHeaders[s]`).
    pub fn header(&self) -> Option<&'static str> {
        SCRAPE_PROTOCOLS_HEADERS.iter().find(|(k, _)| *k == self.0).map(|(_, v)| *v)
    }
    /// Go `Validate`.
    pub fn validate(&self) -> Result<(), String> {
        if self.header().is_none() {
            let names: Vec<&str> = SCRAPE_PROTOCOLS_HEADERS.iter().map(|(k, _)| *k).collect();
            return Err(format!("unknown scrape protocol {}, supported: {}", self.0, go_list(&names)));
        }
        Ok(())
    }
    /// Go `HeaderMediaType`: the media type of the header without parameters.
    pub fn header_media_type(&self) -> String {
        match self.header() {
            None => String::new(),
            Some(h) => h.split(';').next().unwrap_or("").trim().to_lowercase(),
        }
    }
}

impl std::fmt::Display for ScrapeProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Unmarshal for ScrapeProtocol {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        yaml::types::unmarshal_string_kind(&mut self.0, n, d, "config.ScrapeProtocol")
    }
    fn go_type() -> String {
        "config.ScrapeProtocol".to_string()
    }
}
impl Marshal for ScrapeProtocol {
    fn marshal_yaml(&self) -> Result<Value, String> {
        Ok(Value::Str(self.0.clone()))
    }
    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }
}

fn protos(v: &[&str]) -> Vec<ScrapeProtocol> {
    v.iter().map(|s| ScrapeProtocol::new(s)).collect()
}

/// Go `DefaultScrapeProtocols`.
pub fn default_scrape_protocols() -> Vec<ScrapeProtocol> {
    protos(&[OPEN_METRICS_TEXT_1_0_0, OPEN_METRICS_TEXT_0_0_1, PROMETHEUS_TEXT_1_0_0, PROMETHEUS_TEXT_0_0_4])
}

/// Go `DefaultProtoFirstScrapeProtocols`.
pub fn default_proto_first_scrape_protocols() -> Vec<ScrapeProtocol> {
    protos(&[PROMETHEUS_PROTO, OPEN_METRICS_TEXT_1_0_0, OPEN_METRICS_TEXT_0_0_1, PROMETHEUS_TEXT_1_0_0, PROMETHEUS_TEXT_0_0_4])
}

/// Go `validateAcceptScrapeProtocols`.
fn validate_accept_scrape_protocols(sps: &[ScrapeProtocol]) -> Result<(), String> {
    if sps.is_empty() {
        return Err("scrape_protocols cannot be empty".to_string());
    }
    let mut dups: Vec<String> = Vec::new();
    for sp in sps {
        let l = sp.0.to_lowercase();
        if dups.contains(&l) {
            return Err(format!("duplicated protocol in scrape_protocols, got {}", go_list(&sps.iter().map(|s| s.0.as_str()).collect::<Vec<_>>())));
        }
        sp.validate().map_err(|e| format!("scrape_protocols: {e}"))?;
        dups.push(l);
    }
    Ok(())
}

// ---------------- GlobalConfig ----------------

/// Values used across other configuration objects (Go `GlobalConfig`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GlobalConfig {
    pub scrape_interval: Duration,
    pub scrape_timeout: Duration,
    /// None: not set (the per-job default depends on native histograms).
    pub scrape_protocols: Option<Vec<ScrapeProtocol>>,
    pub evaluation_interval: Duration,
    pub rule_query_offset: Duration,
    pub query_log_file: String,
    pub query_max_series: u64,
    pub query_max_samples_read: u64,
    pub query_max_duration: Duration,
    pub scrape_failure_log_file: String,
    pub external_labels: Labels,
    pub body_size_limit: Base2Bytes,
    pub sample_limit: usize,
    pub target_limit: usize,
    pub label_limit: usize,
    pub label_name_length_limit: usize,
    pub label_value_length_limit: usize,
    pub keep_dropped_targets: usize,
    pub metric_name_validation_scheme: ValidationScheme,
    pub metric_name_escaping_scheme: String,
    pub scrape_native_histograms: Option<bool>,
    pub convert_classic_histograms_to_nhcb: bool,
    pub always_scrape_classic_histograms: bool,
    pub extra_scrape_metrics: Option<bool>,
}

yaml::yaml_struct!(GlobalConfig, "config.plain", [
    (scrape_interval: "scrape_interval", omitempty)
    (scrape_timeout: "scrape_timeout", omitempty)
    (scrape_protocols: "scrape_protocols", omitempty)
    (evaluation_interval: "evaluation_interval", omitempty)
    (rule_query_offset: "rule_query_offset", omitempty)
    (query_log_file: "query_log_file", omitempty)
    (query_max_series: "query_max_series", omitempty)
    (query_max_samples_read: "query_max_samples_read", omitempty)
    (query_max_duration: "query_max_duration", omitempty)
    (scrape_failure_log_file: "scrape_failure_log_file", omitempty)
    (external_labels: "external_labels", omitempty)
    (body_size_limit: "body_size_limit", omitempty)
    (sample_limit: "sample_limit", omitempty)
    (target_limit: "target_limit", omitempty)
    (label_limit: "label_limit", omitempty)
    (label_name_length_limit: "label_name_length_limit", omitempty)
    (label_value_length_limit: "label_value_length_limit", omitempty)
    (keep_dropped_targets: "keep_dropped_targets", omitempty)
    (metric_name_validation_scheme: "metric_name_validation_scheme", omitempty)
    (metric_name_escaping_scheme: "metric_name_escaping_scheme", omitempty)
    (scrape_native_histograms: "scrape_native_histograms", omitempty)
    (convert_classic_histograms_to_nhcb: "convert_classic_histograms_to_nhcb", omitempty)
    (always_scrape_classic_histograms: "always_scrape_classic_histograms", omitempty)
    (extra_scrape_metrics: "extra_scrape_metrics", omitempty)
]);

fn default_global() -> &'static RwLock<GlobalConfig> {
    static D: OnceLock<RwLock<GlobalConfig>> = OnceLock::new();
    D.get_or_init(|| {
        RwLock::new(GlobalConfig {
            scrape_interval: Duration::from_secs(60),
            scrape_timeout: Duration::from_secs(10),
            evaluation_interval: Duration::from_secs(60),
            rule_query_offset: Duration(0),
            scrape_protocols: None,
            scrape_native_histograms: Some(false),
            convert_classic_histograms_to_nhcb: false,
            always_scrape_classic_histograms: false,
            extra_scrape_metrics: Some(false),
            metric_name_validation_scheme: ValidationScheme::Utf8,
            metric_name_escaping_scheme: ALLOW_UTF8.to_string(),
            ..Default::default()
        })
    })
}

/// Go `DefaultGlobalConfig` (a variable: feature flags change it with `set_default_global_config`).
pub fn default_global_config() -> GlobalConfig {
    default_global().read().unwrap_or_else(|e| e.into_inner()).clone()
}

/// Changes `DefaultGlobalConfig` (Go assigns fields of the variable when feature flags are set, for example
/// `ScrapeProtocols` for created-timestamp-zero-ingestion or `ScrapeNativeHistograms`).
pub fn set_default_global_config(f: impl FnOnce(&mut GlobalConfig)) {
    f(&mut default_global().write().unwrap_or_else(|e| e.into_inner()));
}

impl Unmarshal for GlobalConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut gc = GlobalConfig::default();
            d.inner_struct(n, &mut gc)?;
            gc.apply_defaults().map_err(YError::Msg)?;
            *self = gc;
            Ok(())
        })
    }
    fn go_type() -> String {
        "config.plain".to_string()
    }
}
impl Marshal for GlobalConfig {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        self.fields_zero()
    }
}

impl GlobalConfig {
    /// The part of Go's `UnmarshalYAML` after decoding: validation and defaults.
    fn apply_defaults(&mut self) -> Result<(), String> {
        let def = default_global_config();
        match self.metric_name_validation_scheme {
            ValidationScheme::Utf8 | ValidationScheme::Legacy => {}
            ValidationScheme::Unset => self.metric_name_validation_scheme = def.metric_name_validation_scheme,
        }
        let scheme = self.metric_name_validation_scheme;
        self.external_labels.validate(|n, v| {
            if !scheme.is_valid_label_name(n) {
                return Err(format!("{} is not a valid label name", model::unicode::quote(n)));
            }
            let _ = v; // label values are valid UTF-8 (Rust strings)
            Ok(())
        })?;
        if self.scrape_interval.0 == 0 {
            self.scrape_interval = def.scrape_interval;
        }
        if self.scrape_timeout > self.scrape_interval {
            return Err("global scrape timeout greater than scrape interval".to_string());
        }
        if self.scrape_timeout.0 == 0 {
            self.scrape_timeout = def.scrape_timeout.min(self.scrape_interval);
        }
        if self.evaluation_interval.0 == 0 {
            self.evaluation_interval = def.evaluation_interval;
        }
        if self.scrape_native_histograms.is_none() {
            self.scrape_native_histograms = def.scrape_native_histograms;
        }
        if self.extra_scrape_metrics.is_none() {
            self.extra_scrape_metrics = def.extra_scrape_metrics;
        }
        if self.scrape_protocols.is_none() && def.scrape_protocols.is_some() {
            self.scrape_protocols = def.scrape_protocols.clone();
        }
        if let Some(sp) = &self.scrape_protocols {
            validate_accept_scrape_protocols(sp).map_err(|e| format!("{e} for global config"))?;
        }
        Ok(())
    }

    /// Go `SetDirectory`.
    pub fn set_directory(&mut self, dir: &str) {
        self.query_log_file = net::config::join_dir(dir, &self.query_log_file);
        self.scrape_failure_log_file = net::config::join_dir(dir, &self.scrape_failure_log_file);
    }

    /// Go `isZero`.
    pub fn is_zero(&self) -> bool {
        self.external_labels.is_empty()
            && self.scrape_interval.0 == 0
            && self.scrape_timeout.0 == 0
            && self.evaluation_interval.0 == 0
            && self.rule_query_offset.0 == 0
            && self.query_log_file.is_empty()
            && self.query_max_series == 0
            && self.query_max_samples_read == 0
            && self.query_max_duration.0 == 0
            && self.scrape_failure_log_file.is_empty()
            && self.scrape_protocols.is_none()
            && self.scrape_native_histograms.is_none()
            && !self.convert_classic_histograms_to_nhcb
            && !self.always_scrape_classic_histograms
            && self.body_size_limit.0 == 0
            && self.sample_limit == 0
            && self.target_limit == 0
            && self.label_limit == 0
            && self.label_name_length_limit == 0
            && self.label_value_length_limit == 0
            && self.keep_dropped_targets == 0
            && self.metric_name_validation_scheme == ValidationScheme::Unset
            && self.metric_name_escaping_scheme.is_empty()
            && self.extra_scrape_metrics.is_none()
    }
}

// ---------------- RuntimeConfig ----------------

/// A logging level, normalized (Go `LogLevel`): "debug", "info", "warn" or "error".
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogLevel(pub String);

impl Unmarshal for LogLevel {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut s = String::new();
            let ok = yaml::types::unmarshal_string_kind(&mut s, n, d, "promslog.plain")?;
            if !ok {
                // the type error is already recorded
                return Ok(());
            }
            // promslog.Level starts at info; "" keeps it
            let level = match s.to_lowercase().as_str() {
                "" | "info" => "info",
                "debug" => "debug",
                "warn" => "warn",
                "error" => "error",
                _ => return Err(msg(format!("unrecognized log level {s}"))),
            };
            self.0 = level.to_string();
            Ok(())
        })
    }
    fn go_type() -> String {
        "config.LogLevel".to_string()
    }
}
impl Marshal for LogLevel {
    fn marshal_yaml(&self) -> Result<Value, String> {
        Ok(Value::Str(self.0.clone()))
    }
    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }
}

/// Process behavior (Go `RuntimeConfig`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeConfig {
    pub gogc: i64,
    pub log_level: LogLevel,
}
yaml::yaml_struct!(RuntimeConfig, "config.RuntimeConfig", [
    (gogc: "gogc", omitempty)
    (log_level: "log_level", omitempty)
]);
yaml::impl_plain_struct!(RuntimeConfig);

impl RuntimeConfig {
    pub fn is_zero(&self) -> bool {
        self.gogc == 0 && self.log_level.0.is_empty()
    }
}

/// Go `getGoGC`: the GOGC environment variable ("off" is -1), else 75.
fn get_go_gc() -> i64 {
    if let Ok(v) = std::env::var("GOGC")
        && !v.is_empty()
    {
        if v.to_lowercase() == "off" {
            return -1;
        }
        if let Ok(i) = go_atoi(&v) {
            return i;
        }
    }
    DEFAULT_GO_GC_PERCENTAGE
}

fn go_atoi(s: &str) -> Result<i64, ()> {
    let t = s.strip_prefix('+').unwrap_or(s);
    if t.starts_with('+') {
        return Err(());
    }
    s.trim_start_matches('+').parse::<i64>().map_err(|_| ())
}

/// Go `DefaultRuntimeConfig`.
pub fn default_runtime_config() -> RuntimeConfig {
    static G: OnceLock<i64> = OnceLock::new();
    RuntimeConfig { gogc: *G.get_or_init(get_go_gc), log_level: LogLevel("info".to_string()) }
}

// ---------------- ScrapeConfig ----------------

/// A scraping unit (Go `ScrapeConfig`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ScrapeConfig {
    pub job_name: String,
    pub honor_labels: bool,
    pub honor_timestamps: bool,
    pub track_timestamps_staleness: bool,
    /// Go `url.Values`.
    pub params: BTreeMap<String, Vec<String>>,
    pub scrape_interval: Duration,
    pub scrape_timeout: Duration,
    pub scrape_protocols: Option<Vec<ScrapeProtocol>>,
    pub scrape_fallback_protocol: ScrapeProtocol,
    pub scrape_native_histograms: Option<bool>,
    pub always_scrape_classic_histograms: Option<bool>,
    pub convert_classic_histograms_to_nhcb: Option<bool>,
    pub scrape_failure_log_file: String,
    pub metrics_path: String,
    pub scheme: String,
    pub enable_compression: bool,
    pub body_size_limit: Base2Bytes,
    pub sample_limit: usize,
    pub target_limit: usize,
    pub label_limit: usize,
    pub label_name_length_limit: usize,
    pub label_value_length_limit: usize,
    pub native_histogram_bucket_limit: usize,
    pub native_histogram_min_bucket_factor: f64,
    pub keep_dropped_targets: usize,
    pub metric_name_validation_scheme: ValidationScheme,
    pub metric_name_escaping_scheme: String,
    pub extra_scrape_metrics: Option<bool>,
    pub service_discovery_configs: Configs,
    pub http_client_config: HttpClientConfig,
    pub relabel_configs: Vec<relabel::Config>,
    pub metric_relabel_configs: Vec<relabel::Config>,
}

yaml::yaml_struct!(ScrapeConfig, "config.ScrapeConfig", [
    (job_name: "job_name")
    (honor_labels: "honor_labels", omitempty)
    (honor_timestamps: "honor_timestamps")
    (track_timestamps_staleness: "track_timestamps_staleness")
    (params: "params", omitempty)
    (scrape_interval: "scrape_interval", omitempty)
    (scrape_timeout: "scrape_timeout", omitempty)
    (scrape_protocols: "scrape_protocols", omitempty)
    (scrape_fallback_protocol: "fallback_scrape_protocol", omitempty)
    (scrape_native_histograms: "scrape_native_histograms", omitempty)
    (always_scrape_classic_histograms: "always_scrape_classic_histograms", omitempty)
    (convert_classic_histograms_to_nhcb: "convert_classic_histograms_to_nhcb", omitempty)
    (scrape_failure_log_file: "scrape_failure_log_file", omitempty)
    (metrics_path: "metrics_path", omitempty)
    (scheme: "scheme", omitempty)
    (enable_compression: "enable_compression")
    (body_size_limit: "body_size_limit", omitempty)
    (sample_limit: "sample_limit", omitempty)
    (target_limit: "target_limit", omitempty)
    (label_limit: "label_limit", omitempty)
    (label_name_length_limit: "label_name_length_limit", omitempty)
    (label_value_length_limit: "label_value_length_limit", omitempty)
    (native_histogram_bucket_limit: "native_histogram_bucket_limit", omitempty)
    (native_histogram_min_bucket_factor: "native_histogram_min_bucket_factor", omitempty)
    (keep_dropped_targets: "keep_dropped_targets", omitempty)
    (metric_name_validation_scheme: "metric_name_validation_scheme", omitempty)
    (metric_name_escaping_scheme: "metric_name_escaping_scheme", omitempty)
    (extra_scrape_metrics: "extra_scrape_metrics", omitempty)
    (inline http_client_config)
    (ptrs relabel_configs: "relabel_configs" as "[]*relabel.Config", omitempty)
    (ptrs metric_relabel_configs: "metric_relabel_configs" as "[]*relabel.Config", omitempty)
]);

/// Go `DefaultScrapeConfig`. Call `validate` on a config made from it.
pub fn default_scrape_config() -> ScrapeConfig {
    ScrapeConfig {
        metrics_path: "/metrics".to_string(),
        scheme: "http".to_string(),
        honor_labels: false,
        honor_timestamps: true,
        http_client_config: HttpClientConfig::default_config(),
        enable_compression: true,
        ..Default::default()
    }
}

fn first_null_error(nulls: &[(&'static str, usize)], key: &str) -> Option<usize> {
    nulls.iter().find(|(k, _)| *k == key).map(|(_, i)| *i)
}

impl Unmarshal for ScrapeConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_scrape_config();
            let mut sd = Configs::default();
            let mut nulls = Vec::new();
            discovery::unmarshal_yaml_with_inline_configs_nulls(n, d, self, &mut sd, &mut nulls)?;
            self.service_discovery_configs = sd;
            if self.job_name.is_empty() {
                return Err(msg("job_name is empty"));
            }
            self.http_client_config.validate().map_err(YError::Msg)?;
            if self.relabel_configs.is_empty() && first_null_error(&nulls, "relabel_configs").is_none() {
                check_static_targets(&self.service_discovery_configs).map_err(YError::Msg)?;
            }
            if first_null_error(&nulls, "relabel_configs").is_some() {
                return Err(msg("empty or null target relabeling rule in scrape config"));
            }
            if first_null_error(&nulls, "metric_relabel_configs").is_some() {
                return Err(msg("empty or null metric relabeling rule in scrape config"));
            }
            Ok(())
        })
    }
    fn go_type() -> String {
        "config.ScrapeConfig".to_string()
    }
}

impl Marshal for ScrapeConfig {
    fn marshal_yaml(&self) -> Result<Value, String> {
        discovery::marshal_yaml_with_inline_configs(self, &self.service_discovery_configs)
    }
    fn is_zero(&self) -> bool {
        false
    }
}

impl ScrapeConfig {
    /// Go `SetDirectory`.
    pub fn set_directory(&mut self, dir: &str) {
        self.service_discovery_configs.set_directory(dir);
        self.http_client_config.set_directory(dir);
        self.scrape_failure_log_file = net::config::join_dir(dir, &self.scrape_failure_log_file);
    }

    /// Go `Validate`: checks the config and fills in defaults from the global config.
    pub fn validate(&mut self, global: &GlobalConfig) -> Result<(), String> {
        let mut global = global.clone();
        if self.scrape_interval.0 == 0 {
            self.scrape_interval = global.scrape_interval;
        }
        if self.scrape_timeout > self.scrape_interval {
            return Err(format!("scrape timeout greater than scrape interval for scrape config with job name {}", yaml::go_quote(&self.job_name)));
        }
        if self.scrape_timeout.0 == 0 {
            self.scrape_timeout = global.scrape_timeout.min(self.scrape_interval);
        }
        if self.body_size_limit.0 == 0 {
            self.body_size_limit = global.body_size_limit;
        }
        if self.sample_limit == 0 {
            self.sample_limit = global.sample_limit;
        }
        if self.target_limit == 0 {
            self.target_limit = global.target_limit;
        }
        if self.label_limit == 0 {
            self.label_limit = global.label_limit;
        }
        if self.label_name_length_limit == 0 {
            self.label_name_length_limit = global.label_name_length_limit;
        }
        if self.label_value_length_limit == 0 {
            self.label_value_length_limit = global.label_value_length_limit;
        }
        if self.keep_dropped_targets == 0 {
            self.keep_dropped_targets = global.keep_dropped_targets;
        }
        if self.scrape_failure_log_file.is_empty() {
            self.scrape_failure_log_file = global.scrape_failure_log_file.clone();
        }
        if self.scrape_native_histograms.is_none() {
            self.scrape_native_histograms = global.scrape_native_histograms;
        }
        if self.extra_scrape_metrics.is_none() {
            self.extra_scrape_metrics = global.extra_scrape_metrics;
        }
        if self.scrape_protocols.is_none() {
            self.scrape_protocols = Some(if let Some(g) = &global.scrape_protocols {
                g.clone()
            } else if self.scrape_native_histograms_enabled() {
                default_proto_first_scrape_protocols()
            } else {
                default_scrape_protocols()
            });
        }
        validate_accept_scrape_protocols(self.scrape_protocols.as_deref().unwrap_or(&[])).map_err(|e| format!("{e} for scrape config with job name {}", yaml::go_quote(&self.job_name)))?;
        if !self.scrape_fallback_protocol.0.is_empty() {
            self.scrape_fallback_protocol.validate().map_err(|e| format!("invalid fallback_scrape_protocol for scrape config with job name {}: {e}", yaml::go_quote(&self.job_name)))?;
        }
        match global.metric_name_validation_scheme {
            ValidationScheme::Legacy | ValidationScheme::Utf8 => {}
            ValidationScheme::Unset => return Err("global name validation method must be set".to_string()),
        }
        let mut local_validation_unset = false;
        if self.metric_name_validation_scheme == ValidationScheme::Unset {
            local_validation_unset = true;
            self.metric_name_validation_scheme = global.metric_name_validation_scheme;
        }
        let bad_escaping = |what: &str, got: &str| format!("unknown {what} name escaping method specified, must be one of '{ALLOW_UTF8}', '{ESCAPE_UNDERSCORES}', '{ESCAPE_DOTS}', or '{ESCAPE_VALUES}', got {}", yaml::go_quote(got));
        match global.metric_name_escaping_scheme.as_str() {
            "" => {
                global.metric_name_escaping_scheme = if global.metric_name_validation_scheme == ValidationScheme::Legacy { ESCAPE_UNDERSCORES } else { ALLOW_UTF8 }.to_string();
            }
            ALLOW_UTF8 | ESCAPE_UNDERSCORES | ESCAPE_DOTS | ESCAPE_VALUES => {}
            other => return Err(bad_escaping("global", other)),
        }
        if self.metric_name_escaping_scheme.is_empty() {
            self.metric_name_escaping_scheme = if local_validation_unset {
                global.metric_name_escaping_scheme.clone()
            } else if self.metric_name_validation_scheme == ValidationScheme::Legacy {
                ESCAPE_UNDERSCORES.to_string()
            } else {
                ALLOW_UTF8.to_string()
            };
        }
        match self.metric_name_escaping_scheme.as_str() {
            ALLOW_UTF8 => {
                if self.metric_name_validation_scheme != ValidationScheme::Utf8 {
                    return Err("utf8 metric names requested but validation scheme is not set to UTF8".to_string());
                }
            }
            ESCAPE_UNDERSCORES | ESCAPE_DOTS | ESCAPE_VALUES => {}
            other => return Err(bad_escaping("scrape config", other)),
        }
        if self.convert_classic_histograms_to_nhcb.is_none() {
            self.convert_classic_histograms_to_nhcb = Some(global.convert_classic_histograms_to_nhcb);
        }
        if self.always_scrape_classic_histograms.is_none() {
            self.always_scrape_classic_histograms = Some(global.always_scrape_classic_histograms);
        }
        for rc in self.relabel_configs.iter_mut() {
            rc.validate(self.metric_name_validation_scheme)?;
        }
        for rc in self.metric_relabel_configs.iter_mut() {
            rc.validate(self.metric_name_validation_scheme)?;
        }
        Ok(())
    }

    /// Go `ScrapeNativeHistogramsEnabled`.
    pub fn scrape_native_histograms_enabled(&self) -> bool {
        self.scrape_native_histograms == Some(true)
    }
    /// Go `ConvertClassicHistogramsToNHCBEnabled`.
    pub fn convert_classic_histograms_to_nhcb_enabled(&self) -> bool {
        self.convert_classic_histograms_to_nhcb == Some(true)
    }
    /// Go `AlwaysScrapeClassicHistogramsEnabled`.
    pub fn always_scrape_classic_histograms_enabled(&self) -> bool {
        self.always_scrape_classic_histograms == Some(true)
    }
    /// Go `ExtraScrapeMetricsEnabled`.
    pub fn extra_scrape_metrics_enabled(&self) -> bool {
        self.extra_scrape_metrics == Some(true)
    }
}

/// prometheus/common `model.EscapingScheme`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EscapingScheme {
    NoEscaping,
    UnderscoreEscaping,
    DotsEscaping,
    ValueEncodingEscaping,
}

/// Go `ToEscapingScheme`: "" maps from the validation scheme.
pub fn to_escaping_scheme(s: &str, v: ValidationScheme) -> Result<EscapingScheme, String> {
    if s.is_empty() {
        return match v {
            ValidationScheme::Utf8 => Ok(EscapingScheme::NoEscaping),
            ValidationScheme::Legacy => Ok(EscapingScheme::UnderscoreEscaping),
            ValidationScheme::Unset => Err(format!("ValidationScheme is unset: {}", v.as_str())),
        };
    }
    match s {
        ALLOW_UTF8 => Ok(EscapingScheme::NoEscaping),
        ESCAPE_UNDERSCORES => Ok(EscapingScheme::UnderscoreEscaping),
        ESCAPE_DOTS => Ok(EscapingScheme::DotsEscaping),
        ESCAPE_VALUES => Ok(EscapingScheme::ValueEncodingEscaping),
        _ => Err(format!("unknown model.EscapingScheme: {s}")),
    }
}

/// Go `checkStaticTargets`.
fn check_static_targets(configs: &Configs) -> Result<(), String> {
    for c in configs.iter() {
        let Some(sc) = c.as_any().downcast_ref::<StaticConfig>() else { continue };
        for tg in &sc.0 {
            for t in &tg.targets {
                check_target_address(t.get(ADDRESS_LABEL).map(|s| s.as_str()).unwrap_or(""))?;
            }
        }
    }
    Ok(())
}

/// Go `CheckTargetAddress`.
pub fn check_target_address(address: &str) -> Result<(), String> {
    if address.contains('/') {
        return Err(format!("{} is not a valid hostname", yaml::go_quote(address)));
    }
    Ok(())
}

/// The `scrape_config_files` file format (Go `ScrapeConfigs`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ScrapeConfigs {
    pub scrape_configs: Vec<ScrapeConfig>,
}
yaml::yaml_struct!(ScrapeConfigs, "config.ScrapeConfigs", [
    (ptrs scrape_configs: "scrape_configs" as "[]*config.ScrapeConfig", omitempty)
]);

/// Decoding of a `scrape_config_files` file: null scrape configs are an error when validated.
struct ScrapeConfigsFile {
    cfg: ScrapeConfigs,
    first_null: Option<usize>,
}
impl Default for ScrapeConfigsFile {
    fn default() -> Self {
        ScrapeConfigsFile { cfg: ScrapeConfigs::default(), first_null: None }
    }
}
impl Unmarshal for ScrapeConfigsFile {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        let nulls = match d.inner_struct_nulls(n, &mut self.cfg) {
            Ok(n) => n,
            Err(YError::Type(v)) => {
                for e in v {
                    d.push_type_error(e);
                }
                return Ok(false);
            }
            Err(e) => return Err(e),
        };
        self.first_null = first_null_error(&nulls, "scrape_configs");
        Ok(true)
    }
    fn go_type() -> String {
        "config.ScrapeConfigs".to_string()
    }
}

// ---------------- Storage ----------------

/// Go `TSDBRetentionConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TsdbRetentionConfig {
    pub time: Duration,
    pub size: Base2Bytes,
    pub percentage: f64,
}
yaml::yaml_struct!(TsdbRetentionConfig, "config.plain", [
    (time: "time", omitempty)
    (size: "size", omitempty)
    (percentage: "percentage", omitempty)
]);
impl Unmarshal for TsdbRetentionConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = TsdbRetentionConfig::default();
            d.inner_struct(n, self)?;
            if self.size.0 < 0 {
                return Err(msg(format!("'storage.tsdb.retention.size' must be greater than or equal to 0, got {}", self.size.go_string())));
            }
            if self.percentage < 0.0 || self.percentage > 100.0 {
                return Err(msg(format!("'storage.tsdb.retention.percentage' must be in the range [0, 100], got {}", yaml::format_float_g(self.percentage))));
            }
            Ok(())
        })
    }
    fn go_type() -> String {
        "config.plain".to_string()
    }
}
impl Marshal for TsdbRetentionConfig {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        self.fields_zero()
    }
}

/// Go `ChunkEncodingConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ChunkEncodingConfig {
    pub floats: String,
}
yaml::yaml_struct!(ChunkEncodingConfig, "config.ChunkEncodingConfig", [
    (floats: "floats", omitempty)
]);
yaml::impl_plain_struct!(ChunkEncodingConfig);

/// Go `TSDBConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TsdbConfig {
    /// In milliseconds, from `out_of_order_time_window_flag` (Go has no yaml tag: the key is the lower-case
    /// field name).
    pub out_of_order_time_window: i64,
    pub out_of_order_time_window_flag: Duration,
    pub stale_series_compaction_threshold: f64,
    pub chunk_encoding: ChunkEncodingConfig,
    pub retention: Option<TsdbRetentionConfig>,
}
yaml::yaml_struct!(TsdbConfig, "config.plain", [
    (out_of_order_time_window: "outofordertimewindow")
    (out_of_order_time_window_flag: "out_of_order_time_window", omitempty)
    (stale_series_compaction_threshold: "stale_series_compaction_threshold", omitempty)
    (chunk_encoding: "chunk_encoding", omitempty)
    (retention: "retention", omitempty)
]);
impl Unmarshal for TsdbConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = TsdbConfig::default();
            d.inner_struct(n, self)?;
            self.out_of_order_time_window = self.out_of_order_time_window_flag.millis();
            match self.chunk_encoding.floats.as_str() {
                "" | FLOAT_CHUNK_ENCODING_XOR | FLOAT_CHUNK_ENCODING_XOR2 => {}
                f => return Err(msg(format!("'storage.tsdb.chunk_encoding.floats' must be 'xor' or 'xor2', or the field must be omitted entirely, got {}", yaml::go_quote(f)))),
            }
            if self.retention.is_none() {
                self.retention = Some(TsdbRetentionConfig::default());
            }
            Ok(())
        })
    }
    fn go_type() -> String {
        "config.plain".to_string()
    }
}
impl Marshal for TsdbConfig {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        self.fields_zero()
    }
}

/// Go `ExemplarsConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExemplarsConfig {
    pub max_exemplars: i64,
}
yaml::yaml_struct!(ExemplarsConfig, "config.ExemplarsConfig", [
    (max_exemplars: "max_exemplars", omitempty)
]);
yaml::impl_plain_struct!(ExemplarsConfig);

/// Go `DefaultExemplarsConfig`.
pub fn default_exemplars_config() -> ExemplarsConfig {
    ExemplarsConfig { max_exemplars: 100000 }
}

/// Go `StorageConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageConfig {
    pub tsdb_config: Option<TsdbConfig>,
    pub exemplars_config: Option<ExemplarsConfig>,
}
yaml::yaml_struct!(StorageConfig, "config.StorageConfig", [
    (tsdb_config: "tsdb", omitempty)
    (exemplars_config: "exemplars", omitempty)
]);
yaml::impl_plain_struct!(StorageConfig);

/// Go `DefaultStorageConfig`.
pub fn default_storage_config() -> StorageConfig {
    StorageConfig { tsdb_config: None, exemplars_config: Some(default_exemplars_config()) }
}

// ---------------- Tracing ----------------

/// Go `TracingClientType`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TracingClientType(pub String);

impl Unmarshal for TracingClientType {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            self.0.clear();
            let mut s = String::new();
            let ok = yaml::types::unmarshal_string_kind(&mut s, n, d, "config.plain")?;
            if !ok {
                return Ok(());
            }
            self.0 = s;
            if self.0 != TRACING_CLIENT_HTTP && self.0 != TRACING_CLIENT_GRPC {
                return Err(msg(format!("expected tracing client type to be to be {TRACING_CLIENT_HTTP} or {TRACING_CLIENT_GRPC}, but got {}", self.0)));
            }
            Ok(())
        })
    }
    fn go_type() -> String {
        "config.TracingClientType".to_string()
    }
}
impl Marshal for TracingClientType {
    fn marshal_yaml(&self) -> Result<Value, String> {
        Ok(Value::Str(self.0.clone()))
    }
    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }
}

/// Go `TracingConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TracingConfig {
    pub client_type: TracingClientType,
    pub endpoint: String,
    pub sampling_fraction: f64,
    pub insecure: bool,
    pub tls_config: TlsConfig,
    pub headers: BTreeMap<String, String>,
    pub compression: String,
    pub timeout: Duration,
}
yaml::yaml_struct!(TracingConfig, "config.plain", [
    (client_type: "client_type", omitempty)
    (endpoint: "endpoint", omitempty)
    (sampling_fraction: "sampling_fraction", omitempty)
    (insecure: "insecure", omitempty)
    (tls_config: "tls_config", omitempty)
    (headers: "headers", omitempty)
    (compression: "compression", omitempty)
    (timeout: "timeout", omitempty)
]);
impl Unmarshal for TracingConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = TracingConfig { client_type: TracingClientType(TRACING_CLIENT_GRPC.to_string()), ..Default::default() };
            d.inner_struct(n, self)?;
            validate_headers_for_tracing(&self.headers).map_err(YError::Msg)?;
            if self.endpoint.is_empty() {
                return Err(msg("tracing endpoint must be set"));
            }
            if !self.compression.is_empty() && self.compression != GZIP_COMPRESSION {
                return Err(msg(format!("invalid compression type {} provided, valid options: {GZIP_COMPRESSION}", self.compression)));
            }
            Ok(())
        })
    }
    fn go_type() -> String {
        "config.plain".to_string()
    }
}
impl Marshal for TracingConfig {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        self.fields_zero()
    }
}
impl TracingConfig {
    pub fn set_directory(&mut self, dir: &str) {
        self.tls_config.set_directory(dir);
    }
}

// Go iterates the header map in random order; the first offending header is reported. Here: sorted order.
fn validate_headers_for_tracing(headers: &BTreeMap<String, String>) -> Result<(), String> {
    for h in headers.keys() {
        let l = h.to_lowercase();
        if l == "authorization" {
            return Err("custom authorization header configuration is not yet supported".to_string());
        }
        if RESERVED_HEADERS.contains(&l.as_str()) {
            return Err(format!("{h} is a reserved header. It must not be changed"));
        }
    }
    Ok(())
}

fn validate_headers(headers: &BTreeMap<String, String>) -> Result<(), String> {
    for h in headers.keys() {
        let l = h.to_lowercase();
        if l == "authorization" {
            return Err("authorization header must be changed via the basic_auth, authorization, oauth2, sigv4, azuread or google_iam parameter".to_string());
        }
        if RESERVED_HEADERS.contains(&l.as_str()) {
            return Err(format!("{h} is a reserved header. It must not be changed"));
        }
    }
    Ok(())
}

// ---------------- Alerting ----------------

/// Go `AlertmanagerAPIVersion`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AlertmanagerApiVersion(pub String);

pub const ALERTMANAGER_API_VERSION_V1: &str = "v1";
pub const ALERTMANAGER_API_VERSION_V2: &str = "v2";
/// Go `SupportedAlertmanagerAPIVersions`.
pub const SUPPORTED_ALERTMANAGER_API_VERSIONS: [&str; 1] = [ALERTMANAGER_API_VERSION_V2];

impl Unmarshal for AlertmanagerApiVersion {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            self.0.clear();
            let mut s = String::new();
            let ok = yaml::types::unmarshal_string_kind(&mut s, n, d, "config.plain")?;
            if !ok {
                return Ok(());
            }
            self.0 = s;
            if !SUPPORTED_ALERTMANAGER_API_VERSIONS.contains(&self.0.as_str()) {
                return Err(msg(format!("expected Alertmanager api version to be one of {} but got {}", go_list(&SUPPORTED_ALERTMANAGER_API_VERSIONS), self.0)));
            }
            Ok(())
        })
    }
    fn go_type() -> String {
        "config.AlertmanagerAPIVersion".to_string()
    }
}
impl Marshal for AlertmanagerApiVersion {
    fn marshal_yaml(&self) -> Result<Value, String> {
        Ok(Value::Str(self.0.clone()))
    }
    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }
}

/// How Alertmanagers are discovered and talked to (Go `AlertmanagerConfig`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AlertmanagerConfig {
    pub service_discovery_configs: Configs,
    pub http_client_config: HttpClientConfig,
    pub sigv4_config: Option<SigV4Config>,
    pub scheme: String,
    pub path_prefix: String,
    pub timeout: Duration,
    pub api_version: AlertmanagerApiVersion,
    pub relabel_configs: Vec<relabel::Config>,
    pub alert_relabel_configs: Vec<relabel::Config>,
}
yaml::yaml_struct!(AlertmanagerConfig, "config.AlertmanagerConfig", [
    (inline http_client_config)
    (sigv4_config: "sigv4", omitempty)
    (scheme: "scheme", omitempty)
    (path_prefix: "path_prefix", omitempty)
    (timeout: "timeout", omitempty)
    (api_version: "api_version")
    (ptrs relabel_configs: "relabel_configs" as "[]*relabel.Config", omitempty)
    (ptrs alert_relabel_configs: "alert_relabel_configs" as "[]*relabel.Config", omitempty)
]);

/// Go `DefaultAlertmanagerConfig`.
pub fn default_alertmanager_config() -> AlertmanagerConfig {
    AlertmanagerConfig {
        scheme: "http".to_string(),
        timeout: Duration::from_secs(10),
        api_version: AlertmanagerApiVersion(ALERTMANAGER_API_VERSION_V2.to_string()),
        http_client_config: HttpClientConfig::default_config(),
        ..Default::default()
    }
}

impl Unmarshal for AlertmanagerConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_alertmanager_config();
            let mut sd = Configs::default();
            let mut nulls = Vec::new();
            discovery::unmarshal_yaml_with_inline_configs_nulls(n, d, self, &mut sd, &mut nulls)?;
            self.service_discovery_configs = sd;
            self.http_client_config.validate().map_err(YError::Msg)?;
            let h = &self.http_client_config;
            if (h.basic_auth.is_some() || h.authorization.is_some() || h.oauth2.is_some()) && self.sigv4_config.is_some() {
                return Err(msg("at most one of basic_auth, authorization, oauth2, & sigv4 must be configured"));
            }
            if self.relabel_configs.is_empty() && first_null_error(&nulls, "relabel_configs").is_none() {
                check_static_targets(&self.service_discovery_configs).map_err(YError::Msg)?;
            }
            if first_null_error(&nulls, "relabel_configs").is_some() {
                return Err(msg("empty or null Alertmanager target relabeling rule"));
            }
            if first_null_error(&nulls, "alert_relabel_configs").is_some() {
                return Err(msg("empty or null Alertmanager alert relabeling rule"));
            }
            Ok(())
        })
    }
    fn go_type() -> String {
        "config.AlertmanagerConfig".to_string()
    }
}
impl Marshal for AlertmanagerConfig {
    fn marshal_yaml(&self) -> Result<Value, String> {
        discovery::marshal_yaml_with_inline_configs(self, &self.service_discovery_configs)
    }
    fn is_zero(&self) -> bool {
        false
    }
}

impl AlertmanagerConfig {
    pub fn set_directory(&mut self, dir: &str) {
        self.service_discovery_configs.set_directory(dir);
        self.http_client_config.set_directory(dir);
    }
    pub fn validate(&mut self, scheme: ValidationScheme) -> Result<(), String> {
        for rc in self.alert_relabel_configs.iter_mut() {
            rc.validate(scheme)?;
        }
        for rc in self.relabel_configs.iter_mut() {
            rc.validate(scheme)?;
        }
        Ok(())
    }
}

/// Go `AlertingConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AlertingConfig {
    pub alert_relabel_configs: Vec<relabel::Config>,
    pub alertmanager_configs: Vec<AlertmanagerConfig>,
}
yaml::yaml_struct!(AlertingConfig, "config.plain", [
    (ptrs alert_relabel_configs: "alert_relabel_configs" as "[]*relabel.Config", omitempty)
    (ptrs alertmanager_configs: "alertmanagers" as "config.AlertmanagerConfigs", omitempty)
]);
impl Unmarshal for AlertingConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = AlertingConfig::default();
            let nulls = d.inner_struct_nulls(n, self)?;
            if first_null_error(&nulls, "alert_relabel_configs").is_some() {
                return Err(msg("empty or null alert relabeling rule"));
            }
            // Go panics on a null Alertmanager config (nil pointer); here it is left out.
            Ok(())
        })
    }
    fn go_type() -> String {
        "config.plain".to_string()
    }
}
impl Marshal for AlertingConfig {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        self.fields_zero()
    }
}
impl AlertingConfig {
    pub fn validate(&mut self, scheme: ValidationScheme) -> Result<(), String> {
        for rc in self.alert_relabel_configs.iter_mut() {
            rc.validate(scheme)?;
        }
        for rc in self.alertmanager_configs.iter_mut() {
            rc.validate(scheme)?;
        }
        Ok(())
    }
    pub fn set_directory(&mut self, dir: &str) {
        for c in self.alertmanager_configs.iter_mut() {
            c.set_directory(dir);
        }
    }
    /// Go `AlertmanagerConfigs.ToMap`: "config-0", "config-1", ...
    pub fn alertmanager_configs_map(&self) -> BTreeMap<String, &AlertmanagerConfig> {
        self.alertmanager_configs.iter().enumerate().map(|(i, c)| (format!("config-{i}"), c)).collect()
    }
}

// ---------------- Remote write / read ----------------

/// Go `QueueConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueueConfig {
    pub capacity: i64,
    pub max_shards: i64,
    pub min_shards: i64,
    pub max_samples_per_send: i64,
    pub batch_send_deadline: Duration,
    pub min_backoff: Duration,
    pub max_backoff: Duration,
    pub retry_on_rate_limit: bool,
    pub sample_age_limit: Duration,
}
yaml::yaml_struct!(QueueConfig, "config.QueueConfig", [
    (capacity: "capacity", omitempty)
    (max_shards: "max_shards", omitempty)
    (min_shards: "min_shards", omitempty)
    (max_samples_per_send: "max_samples_per_send", omitempty)
    (batch_send_deadline: "batch_send_deadline", omitempty)
    (min_backoff: "min_backoff", omitempty)
    (max_backoff: "max_backoff", omitempty)
    (retry_on_rate_limit: "retry_on_http_429", omitempty)
    (sample_age_limit: "sample_age_limit", omitempty)
]);
yaml::impl_plain_struct!(QueueConfig);

/// Go `DefaultQueueConfig`.
pub fn default_queue_config() -> QueueConfig {
    QueueConfig {
        max_shards: 50,
        min_shards: 1,
        max_samples_per_send: 2000,
        capacity: 10000,
        batch_send_deadline: Duration::from_secs(5),
        min_backoff: Duration::from_millis(30),
        max_backoff: Duration::from_secs(5),
        ..Default::default()
    }
}

impl QueueConfig {
    /// Go `Validate`.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_shards <= 0 {
            return Err("remote write queue max_shards must be positive".into());
        }
        if self.min_shards <= 0 {
            return Err("remote write queue min_shards must be positive".into());
        }
        if self.min_shards > self.max_shards {
            return Err("remote write queue min_shards must not be greater than max_shards".into());
        }
        if self.max_samples_per_send <= 0 {
            return Err("remote write queue max_samples_per_send must be positive".into());
        }
        if self.capacity <= 0 {
            return Err("remote write queue capacity must be positive".into());
        }
        if self.max_backoff < self.min_backoff {
            return Err("remote write queue max_backoff must not be less than min_backoff".into());
        }
        Ok(())
    }
}

/// Go `MetadataConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetadataConfig {
    pub send: bool,
    pub send_interval: Duration,
    pub max_samples_per_send: i64,
}
yaml::yaml_struct!(MetadataConfig, "config.MetadataConfig", [
    (send: "send")
    (send_interval: "send_interval")
    (max_samples_per_send: "max_samples_per_send", omitempty)
]);
yaml::impl_plain_struct!(MetadataConfig);

/// Go `DefaultMetadataConfig`.
pub fn default_metadata_config() -> MetadataConfig {
    MetadataConfig { send: true, send_interval: Duration::from_secs(60), max_samples_per_send: 2000 }
}

/// Go `remoteapi.WriteMessageType`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteMessageType(pub String);

impl WriteMessageType {
    pub fn validate(&self) -> Result<(), String> {
        match self.0.as_str() {
            WRITE_V1_MESSAGE_TYPE | WRITE_V2_MESSAGE_TYPE => Ok(()),
            _ => Err(format!("unknown type for remote write protobuf message {}, supported: {WRITE_V1_MESSAGE_TYPE}, {WRITE_V2_MESSAGE_TYPE}", self.0)),
        }
    }
}
impl Unmarshal for WriteMessageType {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        yaml::types::unmarshal_string_kind(&mut self.0, n, d, "remote.WriteMessageType")
    }
    fn go_type() -> String {
        "remote.WriteMessageType".to_string()
    }
}
impl Marshal for WriteMessageType {
    fn marshal_yaml(&self) -> Result<Value, String> {
        Ok(Value::Str(self.0.clone()))
    }
    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }
}

/// Go `RemoteWriteConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RemoteWriteConfig {
    pub url: Url,
    pub remote_timeout: Duration,
    pub headers: BTreeMap<String, String>,
    pub write_relabel_configs: Vec<relabel::Config>,
    pub name: String,
    pub send_exemplars: bool,
    pub send_native_histograms: bool,
    pub round_robin_dns: bool,
    pub protobuf_message: WriteMessageType,
    pub failed_request_logging: bool,
    pub http_client_config: HttpClientConfig,
    pub queue_config: QueueConfig,
    pub metadata_config: MetadataConfig,
    pub sigv4_config: Option<SigV4Config>,
    pub azure_ad_config: Option<AzureAdConfig>,
    pub google_iam_config: Option<GoogleIamConfig>,
    pub convert_nhcb_to_classic: bool,
}
yaml::yaml_struct!(RemoteWriteConfig, "config.plain", [
    (url: "url")
    (remote_timeout: "remote_timeout", omitempty)
    (headers: "headers", omitempty)
    (ptrs write_relabel_configs: "write_relabel_configs" as "[]*relabel.Config", omitempty)
    (name: "name", omitempty)
    (send_exemplars: "send_exemplars", omitempty)
    (send_native_histograms: "send_native_histograms", omitempty)
    (round_robin_dns: "round_robin_dns", omitempty)
    (protobuf_message: "protobuf_message", omitempty)
    (failed_request_logging: "failed_request_logging", omitempty)
    (inline http_client_config)
    (queue_config: "queue_config", omitempty)
    (metadata_config: "metadata_config", omitempty)
    (sigv4_config: "sigv4", omitempty)
    (azure_ad_config: "azuread", omitempty)
    (google_iam_config: "google_iam", omitempty)
    (convert_nhcb_to_classic: "convert_nhcb_to_classic", omitempty)
]);

/// Go `DefaultRemoteWriteHTTPClientConfig`.
pub fn default_remote_write_http_client_config() -> HttpClientConfig {
    HttpClientConfig { follow_redirects: true, enable_http2: false, ..Default::default() }
}

/// Go `DefaultRemoteWriteConfig`.
pub fn default_remote_write_config() -> RemoteWriteConfig {
    RemoteWriteConfig {
        remote_timeout: Duration::from_secs(30),
        protobuf_message: WriteMessageType(WRITE_V1_MESSAGE_TYPE.to_string()),
        queue_config: default_queue_config(),
        metadata_config: default_metadata_config(),
        http_client_config: default_remote_write_http_client_config(),
        ..Default::default()
    }
}

impl Unmarshal for RemoteWriteConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_remote_write_config();
            let nulls = d.inner_struct_nulls(n, self)?;
            if self.url.is_none() {
                return Err(msg("url for remote_write is empty"));
            }
            if first_null_error(&nulls, "write_relabel_configs").is_some() {
                return Err(msg("empty or null relabeling rule in remote write config"));
            }
            validate_headers(&self.headers).map_err(YError::Msg)?;
            self.protobuf_message.validate().map_err(|e| msg(format!("invalid protobuf_message value: {e}")))?;
            self.http_client_config.validate().map_err(YError::Msg)?;
            self.queue_config.validate().map_err(YError::Msg)?;
            self.validate_auth_configs().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "config.plain".to_string()
    }
}
impl Marshal for RemoteWriteConfig {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        false
    }
}

impl RemoteWriteConfig {
    pub fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
    pub fn validate(&mut self, scheme: ValidationScheme) -> Result<(), String> {
        for rc in self.write_relabel_configs.iter_mut() {
            rc.validate(scheme)?;
        }
        Ok(())
    }
    /// Go `validateAuthConfigs`.
    fn validate_auth_configs(&self) -> Result<(), String> {
        let mut a: Vec<&str> = Vec::new();
        let h = &self.http_client_config;
        if h.basic_auth.is_some() {
            a.push("basic_auth");
        }
        if h.authorization.is_some() {
            a.push("authorization");
        }
        if h.oauth2.is_some() {
            a.push("oauth2");
        }
        if self.sigv4_config.is_some() {
            a.push("sigv4");
        }
        if self.azure_ad_config.is_some() {
            a.push("azuread");
        }
        if self.google_iam_config.is_some() {
            a.push("google_iam");
        }
        if a.len() > 1 {
            return Err(format!("at most one of basic_auth, authorization, oauth2, sigv4, azuread or google_iam must be configured. Currently configured: {}", go_list(&a)));
        }
        Ok(())
    }
}

/// Go `RemoteReadConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RemoteReadConfig {
    pub url: Url,
    pub remote_timeout: Duration,
    pub chunked_read_limit: u64,
    pub headers: BTreeMap<String, String>,
    pub read_recent: bool,
    pub name: String,
    pub http_client_config: HttpClientConfig,
    pub required_matchers: LabelSet,
    pub filter_external_labels: bool,
}
yaml::yaml_struct!(RemoteReadConfig, "config.plain", [
    (url: "url")
    (remote_timeout: "remote_timeout", omitempty)
    (chunked_read_limit: "chunked_read_limit", omitempty)
    (headers: "headers", omitempty)
    (read_recent: "read_recent", omitempty)
    (name: "name", omitempty)
    (inline http_client_config)
    (required_matchers: "required_matchers", omitempty)
    (filter_external_labels: "filter_external_labels", omitempty)
]);

/// Go `DefaultRemoteReadConfig`.
pub fn default_remote_read_config() -> RemoteReadConfig {
    RemoteReadConfig {
        remote_timeout: Duration::from_secs(60),
        chunked_read_limit: DEFAULT_CHUNKED_READ_LIMIT,
        http_client_config: HttpClientConfig::default_config(),
        filter_external_labels: true,
        ..Default::default()
    }
}

impl Unmarshal for RemoteReadConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_remote_read_config();
            d.inner_struct(n, self)?;
            if self.url.is_none() {
                return Err(msg("url for remote_read is empty"));
            }
            validate_headers(&self.headers).map_err(YError::Msg)?;
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "config.plain".to_string()
    }
}
impl Marshal for RemoteReadConfig {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        false
    }
}
impl RemoteReadConfig {
    pub fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

// ---------------- OTLP ----------------

/// Go `OTLPConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OtlpConfig {
    pub promote_all_resource_attributes: bool,
    pub promote_resource_attributes: Vec<String>,
    pub ignore_resource_attributes: Vec<String>,
    pub translation_strategy: String,
    pub keep_identifying_resource_attributes: bool,
    pub convert_histograms_to_nhcb: bool,
    pub promote_scope_metadata: bool,
    pub label_name_underscore_sanitization: bool,
    pub label_name_preserve_multiple_underscores: bool,
}
yaml::yaml_struct!(OtlpConfig, "config.plain", [
    (promote_all_resource_attributes: "promote_all_resource_attributes", omitempty)
    (promote_resource_attributes: "promote_resource_attributes", omitempty)
    (ignore_resource_attributes: "ignore_resource_attributes", omitempty)
    (translation_strategy: "translation_strategy", omitempty)
    (keep_identifying_resource_attributes: "keep_identifying_resource_attributes", omitempty)
    (convert_histograms_to_nhcb: "convert_histograms_to_nhcb", omitempty)
    (promote_scope_metadata: "promote_scope_metadata", omitempty)
    (label_name_underscore_sanitization: "label_name_underscore_sanitization", omitempty)
    (label_name_preserve_multiple_underscores: "label_name_preserve_multiple_underscores", omitempty)
]);

/// Go `DefaultOTLPConfig`.
pub fn default_otlp_config() -> OtlpConfig {
    OtlpConfig {
        translation_strategy: UNDERSCORE_ESCAPING_WITH_SUFFIXES.to_string(),
        label_name_underscore_sanitization: true,
        label_name_preserve_multiple_underscores: true,
        ..Default::default()
    }
}

impl Unmarshal for OtlpConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_otlp_config();
            d.inner_struct(n, self)?;
            if self.promote_all_resource_attributes {
                if !self.promote_resource_attributes.is_empty() {
                    return Err(msg("'promote_all_resource_attributes' and 'promote_resource_attributes' cannot be configured simultaneously"));
                }
                sanitize_attributes(&mut self.ignore_resource_attributes, "ignored").map_err(|e| msg(format!("invalid 'ignore_resource_attributes': {e}")))?;
            } else {
                if !self.ignore_resource_attributes.is_empty() {
                    return Err(msg("'ignore_resource_attributes' cannot be configured unless 'promote_all_resource_attributes' is true"));
                }
                sanitize_attributes(&mut self.promote_resource_attributes, "promoted").map_err(|e| msg(format!("invalid 'promote_resource_attributes': {e}")))?;
            }
            Ok(())
        })
    }
    fn go_type() -> String {
        "config.plain".to_string()
    }
}
impl Marshal for OtlpConfig {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        self.fields_zero()
    }
}

/// Go `sanitizeAttributes`: trims names in place; errors are joined with newlines (`errors.Join`).
fn sanitize_attributes(attrs: &mut [String], adjective: &str) -> Result<(), String> {
    let mut seen: Vec<String> = Vec::new();
    let mut errs: Vec<String> = Vec::new();
    for a in attrs.iter_mut() {
        let t = a.trim().to_string();
        if t.is_empty() {
            errs.push(format!("empty {adjective} OTel resource attribute"));
            continue;
        }
        if seen.contains(&t) {
            errs.push(format!("duplicated {adjective} OTel resource attribute {}", yaml::go_quote(&t)));
            continue;
        }
        seen.push(t.clone());
        *a = t;
    }
    if errs.is_empty() { Ok(()) } else { Err(errs.join("\n")) }
}

// ---------------- Config ----------------

/// The configuration file (Go `Config`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Config {
    pub global_config: GlobalConfig,
    pub runtime: RuntimeConfig,
    pub alerting_config: AlertingConfig,
    pub rule_files: Vec<String>,
    pub scrape_config_files: Vec<String>,
    pub scrape_configs: Vec<ScrapeConfig>,
    pub storage_config: StorageConfig,
    pub tracing_config: TracingConfig,
    pub remote_write_configs: Vec<RemoteWriteConfig>,
    pub remote_read_configs: Vec<RemoteReadConfig>,
    pub otlp_config: OtlpConfig,
    /// Set by `load`; `get_scrape_configs` needs it.
    pub loaded: bool,
}
yaml::yaml_struct!(Config, "config.plain", [
    (global_config: "global")
    (runtime: "runtime", omitempty)
    (alerting_config: "alerting", omitempty)
    (rule_files: "rule_files", omitempty)
    (scrape_config_files: "scrape_config_files", omitempty)
    (ptrs scrape_configs: "scrape_configs" as "[]*config.ScrapeConfig", omitempty)
    (storage_config: "storage", omitempty)
    (tracing_config: "tracing", omitempty)
    (ptrs remote_write_configs: "remote_write" as "[]*config.RemoteWriteConfig", omitempty)
    (ptrs remote_read_configs: "remote_read" as "[]*config.RemoteReadConfig", omitempty)
    (otlp_config: "otlp", omitempty)
]);

/// Go `DefaultConfig`.
pub fn default_config() -> Config {
    Config { global_config: default_global_config(), runtime: default_runtime_config(), otlp_config: default_otlp_config(), ..Default::default() }
}

/// `^[^*]*(\*[^/]*)?$`
fn pat_rule_path(s: &str) -> bool {
    match s.find('*') {
        None => true,
        Some(i) => !s[i + 1..].contains(['*', '/']),
    }
}

impl Unmarshal for Config {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_config();
            let nulls = d.inner_struct_nulls(n, self)?;
            if self.global_config.is_zero() {
                self.global_config = default_global_config();
            }
            if self.runtime.is_zero() {
                self.runtime = default_runtime_config();
            }
            if self.storage_config.tsdb_config.is_none() {
                self.storage_config.tsdb_config = Some(TsdbConfig { retention: Some(TsdbRetentionConfig::default()), ..Default::default() });
            }
            for rf in &self.rule_files {
                if !pat_rule_path(rf) {
                    return Err(msg(format!("invalid rule file path {}", yaml::go_quote(rf))));
                }
            }
            for sf in &self.scrape_config_files {
                if !pat_rule_path(sf) {
                    return Err(msg(format!("invalid scrape config file path {}", yaml::go_quote(sf))));
                }
            }
            let scrape_null = first_null_error(&nulls, "scrape_configs");
            let mut job_names: Vec<String> = Vec::new();
            let global = self.global_config.clone();
            for (i, scfg) in self.scrape_configs.iter_mut().enumerate() {
                if scrape_null == Some(i) {
                    return Err(msg("empty or null scrape config section"));
                }
                scfg.validate(&global).map_err(YError::Msg)?;
                if job_names.contains(&scfg.job_name) {
                    return Err(msg(format!("found multiple scrape configs with job name {}", yaml::go_quote(&scfg.job_name))));
                }
                job_names.push(scfg.job_name.clone());
            }
            if scrape_null == Some(self.scrape_configs.len()) {
                return Err(msg("empty or null scrape config section"));
            }
            self.alerting_config.validate(global.metric_name_validation_scheme).map_err(YError::Msg)?;
            let rw_null = first_null_error(&nulls, "remote_write");
            let mut rw_names: Vec<String> = Vec::new();
            for (i, rw) in self.remote_write_configs.iter_mut().enumerate() {
                if rw_null == Some(i) {
                    return Err(msg("empty or null remote write config section"));
                }
                if !rw.name.is_empty() && rw_names.contains(&rw.name) {
                    return Err(msg(format!("found multiple remote write configs with job name {}", yaml::go_quote(&rw.name))));
                }
                rw.validate(global.metric_name_validation_scheme).map_err(YError::Msg)?;
                rw_names.push(rw.name.clone());
            }
            if rw_null == Some(self.remote_write_configs.len()) {
                return Err(msg("empty or null remote write config section"));
            }
            let rr_null = first_null_error(&nulls, "remote_read");
            let mut rr_names: Vec<String> = Vec::new();
            for (i, rr) in self.remote_read_configs.iter().enumerate() {
                if rr_null == Some(i) {
                    return Err(msg("empty or null remote read config section"));
                }
                if !rr.name.is_empty() && rr_names.contains(&rr.name) {
                    return Err(msg(format!("found multiple remote read configs with job name {}", yaml::go_quote(&rr.name))));
                }
                rr_names.push(rr.name.clone());
            }
            if rr_null == Some(self.remote_read_configs.len()) {
                return Err(msg("empty or null remote read config section"));
            }
            Ok(())
        })
    }
    fn go_type() -> String {
        "config.plain".to_string()
    }
}

impl Marshal for Config {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        self.fields_zero()
    }
}

/// Go `os.Expand`.
pub fn expand_env(s: &str, mut mapping: impl FnMut(&str) -> String) -> String {
    fn special(c: u8) -> bool {
        matches!(c, b'*' | b'#' | b'$' | b'@' | b'!' | b'?' | b'-' | b'0'..=b'9')
    }
    fn alnum(c: u8) -> bool {
        c == b'_' || c.is_ascii_alphanumeric()
    }
    fn shell_name(s: &[u8]) -> (&[u8], usize) {
        if s[0] == b'{' {
            if s.len() > 2 && special(s[1]) && s[2] == b'}' {
                return (&s[1..2], 3);
            }
            for i in 1..s.len() {
                if s[i] == b'}' {
                    if i == 1 {
                        return (&[], 2);
                    }
                    return (&s[1..i], i + 1);
                }
            }
            return (&[], 1);
        }
        if special(s[0]) {
            return (&s[0..1], 1);
        }
        let mut i = 0;
        while i < s.len() && alnum(s[i]) {
            i += 1;
        }
        (&s[..i], i)
    }
    let b = s.as_bytes();
    let mut buf: Option<Vec<u8>> = None;
    let mut i = 0;
    let mut j = 0;
    while j < b.len() {
        if b[j] == b'$' && j + 1 < b.len() {
            let out = buf.get_or_insert_with(|| Vec::with_capacity(2 * b.len()));
            out.extend_from_slice(&b[i..j]);
            let (name, w) = shell_name(&b[j + 1..]);
            if name.is_empty() && w > 0 {
                // invalid syntax: eat the characters
            } else if name.is_empty() {
                out.push(b[j]);
            } else {
                out.extend_from_slice(mapping(&String::from_utf8_lossy(name)).as_bytes());
            }
            j += w;
            i = j + 1;
        }
        j += 1;
    }
    match buf {
        None => s.to_string(),
        Some(mut out) => {
            if i < b.len() {
                out.extend_from_slice(&b[i..]);
            }
            String::from_utf8_lossy(&out).into_owned()
        }
    }
}

/// Go `Load`: parses YAML into a Config (strict), expands `$VAR` in external labels and checks the OTLP
/// translation strategy.
pub fn load(s: &str, logger: &Logger) -> Result<Config, String> {
    let mut cfg = default_config();
    yaml::unmarshal_into(s, &mut cfg, true).map_err(|e| e.to_string())?;
    if cfg.storage_config.tsdb_config.is_none() {
        cfg.storage_config.tsdb_config = Some(TsdbConfig { retention: Some(TsdbRetentionConfig::default()), ..Default::default() });
    }
    let mut b = labels::ScratchBuilder::new(0);
    cfg.global_config.external_labels.range(|name, value| {
        let new_v = expand_env(value, |s| {
            if s == "$" {
                return "$".to_string();
            }
            match std::env::var(s) {
                Ok(v) if !v.is_empty() => v,
                _ => {
                    logger.warn("Empty environment variable", &[("name", s)]);
                    String::new()
                }
            }
        });
        if new_v != value {
            logger.debug("External label replaced", &[("label", name), ("input", value), ("output", &new_v)]);
        }
        b.add(name, &new_v);
    });
    let l = b.labels();
    if !l.is_empty() {
        cfg.global_config.external_labels = l;
    }
    match cfg.otlp_config.translation_strategy.as_str() {
        UNDERSCORE_ESCAPING_WITH_SUFFIXES | UNDERSCORE_ESCAPING_WITHOUT_SUFFIXES | "" => {}
        NO_TRANSLATION | NO_UTF8_ESCAPING_WITH_SUFFIXES => {
            if cfg.global_config.metric_name_validation_scheme == ValidationScheme::Legacy {
                return Err(format!("OTLP translation strategy {} is not allowed when UTF8 is disabled", yaml::go_quote(&cfg.otlp_config.translation_strategy)));
            }
        }
        other => return Err(format!("unsupported OTLP translation strategy {}", yaml::go_quote(other))),
    }
    cfg.loaded = true;
    Ok(cfg)
}

/// Go `LoadFile`: loads the file and resolves relative paths against its directory.
pub fn load_file(filename: &str, agent_mode: bool, logger: &Logger) -> Result<Config, String> {
    let content = net::types::read_file(filename)?;
    let mut cfg = load(&String::from_utf8_lossy(&content), logger).map_err(|e| format!("parsing YAML file {filename}: {e}"))?;
    if agent_mode {
        if !cfg.alerting_config.alertmanager_configs.is_empty() || !cfg.alerting_config.alert_relabel_configs.is_empty() {
            return Err("field alerting is not allowed in agent mode".to_string());
        }
        if !cfg.rule_files.is_empty() {
            return Err("field rule_files is not allowed in agent mode".to_string());
        }
        if !cfg.remote_read_configs.is_empty() {
            return Err("field remote_read is not allowed in agent mode".to_string());
        }
    }
    cfg.set_directory(&net::config::dir_of(filename));
    Ok(cfg)
}

/// Go `os.Getwd`: $PWD when it names the current directory (keeps symlinks), else getcwd.
pub fn getwd() -> Option<String> {
    use std::os::unix::fs::MetadataExt;
    if let Ok(pwd) = std::env::var("PWD")
        && pwd.starts_with('/')
        && let (Ok(a), Ok(b)) = (std::fs::metadata("."), std::fs::metadata(&pwd))
        && a.dev() == b.dev()
        && a.ino() == b.ino()
    {
        return Some(pwd);
    }
    std::env::current_dir().ok().map(|d| d.to_string_lossy().into_owned())
}

/// Go `filePath`: the absolute path (`filepath.Abs`).
fn file_path(filename: &str) -> String {
    if filename.starts_with('/') {
        return net::config::clean_path(filename);
    }
    match getwd() {
        Some(d) => net::config::clean_path(&format!("{d}/{filename}")),
        None => filename.to_string(),
    }
}

fn file_err(filename: &str, err: &str) -> String {
    format!("{}: {}", yaml::go_quote(&file_path(filename)), err)
}

impl Config {
    /// Go `SetDirectory`.
    pub fn set_directory(&mut self, dir: &str) {
        self.global_config.set_directory(dir);
        self.alerting_config.set_directory(dir);
        self.tracing_config.set_directory(dir);
        for f in self.rule_files.iter_mut() {
            *f = net::config::join_dir(dir, f);
        }
        for f in self.scrape_config_files.iter_mut() {
            *f = net::config::join_dir(dir, f);
        }
        for c in self.scrape_configs.iter_mut() {
            c.set_directory(dir);
        }
        for c in self.remote_write_configs.iter_mut() {
            c.set_directory(dir);
        }
        for c in self.remote_read_configs.iter_mut() {
            c.set_directory(dir);
        }
    }

    /// Go `GetScrapeConfigs`: the validated scrape configs, including those of `scrape_config_files`.
    pub fn get_scrape_configs(&self) -> Result<Vec<ScrapeConfig>, String> {
        if !self.loaded {
            return Err("scrape config cannot be fetched, main config was not validated and loaded correctly; should not happen".to_string());
        }
        let mut scfgs: Vec<ScrapeConfig> = self.scrape_configs.clone();
        let mut job_names: BTreeMap<String, String> = BTreeMap::new();
        for s in &self.scrape_configs {
            job_names.insert(s.job_name.clone(), "main config file".to_string());
        }
        for pat in &self.scrape_config_files {
            let fs = discovery::glob::glob(pat).map_err(|e| format!("error retrieving scrape config files for {}: {e}", yaml::go_quote(pat)))?;
            for filename in fs {
                let content = std::fs::read(&filename).map_err(|e| file_err(&filename, &format!("open {}: {}", filename, net::types::go_io_error(&e))))?;
                let mut f = ScrapeConfigsFile::default();
                yaml::unmarshal_into(&String::from_utf8_lossy(&content), &mut f, true).map_err(|e| file_err(&filename, &e.to_string()))?;
                let count = f.cfg.scrape_configs.len();
                for (i, mut scfg) in f.cfg.scrape_configs.into_iter().enumerate() {
                    if f.first_null == Some(i) {
                        return Err(file_err(&filename, "empty or null scrape config section"));
                    }
                    scfg.validate(&self.global_config).map_err(|e| file_err(&filename, &e))?;
                    if let Some(first) = job_names.get(&scfg.job_name) {
                        return Err(file_err(&filename, &format!("found multiple scrape configs with job name {}, first found in {}", yaml::go_quote(&scfg.job_name), first)));
                    }
                    job_names.insert(scfg.job_name.clone(), yaml::go_quote(&file_path(&filename)));
                    scfg.set_directory(&net::config::dir_of(&filename));
                    scfgs.push(scfg);
                }
                if f.first_null == Some(count) {
                    return Err(file_err(&filename, "empty or null scrape config section"));
                }
            }
        }
        Ok(scfgs)
    }

    /// Go `Config.String()`: the config as YAML, secrets as `<secret>`.
    pub fn to_yaml_string(&self) -> String {
        match yaml::marshal(self) {
            Ok(s) => s,
            Err(e) => format!("<error creating config string: {e}>"),
        }
    }
}

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_yaml_string())
    }
}
