//! The response payloads of API v1 (Go's result structs) and their JSON, with Go's field names, order and
//! `omitempty` rules.

use std::collections::BTreeMap;

use model::exemplar::QueryResult as ExemplarQueryResult;
use model::labels::Labels;
use promql::Value;
use promql::parser::ValueType;
use tsdb::BlockMeta;

use super::jsonutil::{write_exemplar_query_result, write_labels, write_strings, write_value};
use crate::gotime::Time;
use crate::notifications::Notification;

fn wstr(s: &str, out: &mut String) {
    json::write_str(s, out);
}

fn wf64(f: f64, out: &mut String) {
    json::write_f64(f, out);
}

fn wbool(b: bool, out: &mut String) {
    out.push_str(if b { "true" } else { "false" });
}

/// Go `parser.ValueType` as JSON.
pub fn value_type_name(t: ValueType) -> &'static str {
    match t {
        ValueType::None => "none",
        ValueType::Vector => "vector",
        ValueType::Scalar => "scalar",
        ValueType::Matrix => "matrix",
        ValueType::String => "string",
    }
}

/// Go `CostEstimate`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CostEstimate {
    pub series_touched: i64,
    pub samples_read: i64,
    /// Omitted when zero.
    pub peak_samples: i64,
}

impl CostEstimate {
    pub fn write_json(&self, out: &mut String) {
        out.push_str("{\"seriesTouched\":");
        json::write_i64(self.series_touched, out);
        out.push_str(",\"samplesRead\":");
        json::write_i64(self.samples_read, out);
        if self.peak_samples != 0 {
            out.push_str(",\"peakSamples\":");
            json::write_i64(self.peak_samples, out);
        }
        out.push('}');
    }
}

/// Go `QueryCostComparison`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryCostComparison {
    pub estimated: CostEstimate,
    pub actual: CostEstimate,
}

/// Go `QueryCostData`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryCostData {
    pub estimate: CostEstimate,
}

/// Query statistics as rendered by a stats renderer (Go `stats.QueryStats`): JSON text, or none.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryStatsJson(pub String);

/// Go `QueryData`.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryData {
    pub result_type: ValueType,
    pub result: Value,
    pub stats: Option<QueryStatsJson>,
    pub cost: Option<QueryCostComparison>,
}

impl QueryData {
    pub fn write_json(&self, out: &mut String) {
        out.push_str("{\"resultType\":\"");
        out.push_str(value_type_name(self.result_type));
        out.push_str("\",\"result\":");
        write_value(&self.result, out);
        if let Some(s) = &self.stats {
            out.push_str(",\"stats\":");
            out.push_str(&s.0);
        }
        if let Some(c) = &self.cost {
            out.push_str(",\"cost\":{\"estimated\":");
            c.estimated.write_json(out);
            out.push_str(",\"actual\":");
            c.actual.write_json(out);
            out.push('}');
        }
        out.push('}');
    }
}

/// Go `Target`.
#[derive(Clone, Debug, PartialEq)]
pub struct Target {
    pub discovered_labels: Labels,
    pub labels: Labels,
    pub scrape_pool: String,
    pub scrape_url: String,
    pub global_url: String,
    pub last_error: String,
    pub last_scrape: Time,
    pub last_scrape_duration: f64,
    /// "up", "down" or "unknown".
    pub health: String,
    pub scrape_interval: String,
    pub scrape_timeout: String,
}

impl Target {
    pub fn write_json(&self, out: &mut String) {
        out.push_str("{\"discoveredLabels\":");
        write_labels(&self.discovered_labels, out);
        out.push_str(",\"labels\":");
        write_labels(&self.labels, out);
        out.push_str(",\"scrapePool\":");
        wstr(&self.scrape_pool, out);
        out.push_str(",\"scrapeUrl\":");
        wstr(&self.scrape_url, out);
        out.push_str(",\"globalUrl\":");
        wstr(&self.global_url, out);
        out.push_str(",\"lastError\":");
        wstr(&self.last_error, out);
        out.push_str(",\"lastScrape\":");
        self.last_scrape.write_json(out);
        out.push_str(",\"lastScrapeDuration\":");
        wf64(self.last_scrape_duration, out);
        out.push_str(",\"health\":");
        wstr(&self.health, out);
        out.push_str(",\"scrapeInterval\":");
        wstr(&self.scrape_interval, out);
        out.push_str(",\"scrapeTimeout\":");
        wstr(&self.scrape_timeout, out);
        out.push('}');
    }
}

/// Go `DroppedTarget`.
#[derive(Clone, Debug, PartialEq)]
pub struct DroppedTarget {
    pub discovered_labels: Labels,
    pub scrape_pool: String,
}

/// Go `TargetDiscovery`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TargetDiscovery {
    pub active_targets: Vec<Target>,
    pub dropped_targets: Vec<DroppedTarget>,
    /// `None` is Go's nil map (`null`).
    pub dropped_target_counts: Option<BTreeMap<String, usize>>,
}

impl TargetDiscovery {
    pub fn write_json(&self, out: &mut String) {
        out.push_str("{\"activeTargets\":[");
        for (i, t) in self.active_targets.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            t.write_json(out);
        }
        out.push_str("],\"droppedTargets\":[");
        for (i, t) in self.dropped_targets.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str("{\"discoveredLabels\":");
            write_labels(&t.discovered_labels, out);
            out.push_str(",\"scrapePool\":");
            wstr(&t.scrape_pool, out);
            out.push('}');
        }
        out.push_str("],\"droppedTargetCounts\":");
        match &self.dropped_target_counts {
            None => out.push_str("null"),
            Some(m) => {
                out.push('{');
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    wstr(k, out);
                    out.push(':');
                    json::write_i64(*v as i64, out);
                }
                out.push('}');
            }
        }
        out.push('}');
    }
}

/// Go `metricMetadata` (targets/metadata items).
#[derive(Clone, Debug, PartialEq)]
pub struct MetricMetadataItem {
    pub target: Labels,
    /// Omitted when empty.
    pub metric_family: String,
    pub typ: String,
    pub help: String,
    pub unit: String,
}

/// Go `metadata.Metadata`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Metadata {
    pub typ: String,
    pub help: String,
    pub unit: String,
}

/// Go `RelabelStep`.
#[derive(Clone, Debug, PartialEq)]
pub struct RelabelStep {
    /// The rule as JSON (Go `json.Marshal(*relabel.Config)`).
    pub rule_json: String,
    pub output: Labels,
    pub keep: bool,
}

/// Go `Alert`.
#[derive(Clone, Debug, PartialEq)]
pub struct Alert {
    pub labels: Labels,
    pub annotations: Labels,
    pub state: String,
    pub active_at: Option<Time>,
    pub keep_firing_since: Option<Time>,
    /// Go `strconv.FormatFloat(v, 'e', -1, 64)`.
    pub value: String,
}

impl Alert {
    pub fn write_json(&self, out: &mut String) {
        out.push_str("{\"labels\":");
        write_labels(&self.labels, out);
        out.push_str(",\"annotations\":");
        write_labels(&self.annotations, out);
        out.push_str(",\"state\":");
        wstr(&self.state, out);
        if let Some(t) = &self.active_at {
            out.push_str(",\"activeAt\":");
            t.write_json(out);
        }
        if let Some(t) = &self.keep_firing_since {
            out.push_str(",\"keepFiringSince\":");
            t.write_json(out);
        }
        out.push_str(",\"value\":");
        wstr(&self.value, out);
        out.push('}');
    }
}

fn write_alerts(alerts: &[Alert], out: &mut String) {
    out.push('[');
    for (i, a) in alerts.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        a.write_json(out);
    }
    out.push(']');
}

/// Go `AlertingRule` (API view).
#[derive(Clone, Debug, PartialEq)]
pub struct AlertingRule {
    pub state: String,
    pub name: String,
    pub query: String,
    pub duration: f64,
    pub keep_firing_for: f64,
    pub labels: Labels,
    pub annotations: Labels,
    /// `None` is Go's nil slice (`null`).
    pub alerts: Option<Vec<Alert>>,
    pub health: String,
    /// Omitted when empty.
    pub last_error: String,
    pub evaluation_time: f64,
    pub last_evaluation: Time,
}

/// Go `RecordingRule` (API view).
#[derive(Clone, Debug, PartialEq)]
pub struct RecordingRule {
    pub name: String,
    pub query: String,
    pub labels: Labels,
    pub health: String,
    pub last_error: String,
    pub evaluation_time: f64,
    pub last_evaluation: Time,
}

/// Go `Rule` (an alerting or a recording rule).
#[derive(Clone, Debug, PartialEq)]
pub enum Rule {
    Alerting(AlertingRule),
    Recording(RecordingRule),
}

impl Rule {
    pub fn write_json(&self, out: &mut String) {
        match self {
            Rule::Alerting(r) => {
                out.push_str("{\"state\":");
                wstr(&r.state, out);
                out.push_str(",\"name\":");
                wstr(&r.name, out);
                out.push_str(",\"query\":");
                wstr(&r.query, out);
                out.push_str(",\"duration\":");
                wf64(r.duration, out);
                out.push_str(",\"keepFiringFor\":");
                wf64(r.keep_firing_for, out);
                out.push_str(",\"labels\":");
                write_labels(&r.labels, out);
                out.push_str(",\"annotations\":");
                write_labels(&r.annotations, out);
                out.push_str(",\"alerts\":");
                match &r.alerts {
                    None => out.push_str("null"),
                    Some(a) => write_alerts(a, out),
                }
                out.push_str(",\"health\":");
                wstr(&r.health, out);
                if !r.last_error.is_empty() {
                    out.push_str(",\"lastError\":");
                    wstr(&r.last_error, out);
                }
                out.push_str(",\"evaluationTime\":");
                wf64(r.evaluation_time, out);
                out.push_str(",\"lastEvaluation\":");
                r.last_evaluation.write_json(out);
                out.push_str(",\"type\":\"alerting\"}");
            }
            Rule::Recording(r) => {
                out.push_str("{\"name\":");
                wstr(&r.name, out);
                out.push_str(",\"query\":");
                wstr(&r.query, out);
                out.push_str(",\"labels\":");
                write_labels(&r.labels, out);
                out.push_str(",\"health\":");
                wstr(&r.health, out);
                if !r.last_error.is_empty() {
                    out.push_str(",\"lastError\":");
                    wstr(&r.last_error, out);
                }
                out.push_str(",\"evaluationTime\":");
                wf64(r.evaluation_time, out);
                out.push_str(",\"lastEvaluation\":");
                r.last_evaluation.write_json(out);
                out.push_str(",\"type\":\"recording\"}");
            }
        }
    }
}

/// Go `RuleGroup` (API view).
#[derive(Clone, Debug, PartialEq)]
pub struct RuleGroup {
    pub name: String,
    pub file: String,
    pub rules: Vec<Rule>,
    pub interval: f64,
    pub limit: i64,
    pub evaluation_time: f64,
    pub last_evaluation: Time,
}

/// Go `RuleDiscovery`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuleDiscovery {
    pub rule_groups: Vec<RuleGroup>,
    /// Omitted when empty.
    pub group_next_token: String,
}

impl RuleDiscovery {
    pub fn write_json(&self, out: &mut String) {
        out.push_str("{\"groups\":[");
        for (i, g) in self.rule_groups.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str("{\"name\":");
            wstr(&g.name, out);
            out.push_str(",\"file\":");
            wstr(&g.file, out);
            out.push_str(",\"rules\":[");
            for (j, r) in g.rules.iter().enumerate() {
                if j > 0 {
                    out.push(',');
                }
                r.write_json(out);
            }
            out.push_str("],\"interval\":");
            wf64(g.interval, out);
            out.push_str(",\"limit\":");
            json::write_i64(g.limit, out);
            out.push_str(",\"evaluationTime\":");
            wf64(g.evaluation_time, out);
            out.push_str(",\"lastEvaluation\":");
            g.last_evaluation.write_json(out);
            out.push('}');
        }
        out.push(']');
        if !self.group_next_token.is_empty() {
            out.push_str(",\"groupNextToken\":");
            wstr(&self.group_next_token, out);
        }
        out.push('}');
    }
}

/// Go `RuntimeInfo`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeInfo {
    pub start_time: Time,
    pub cwd: String,
    pub hostname: String,
    pub server_time: Time,
    pub reload_config_success: bool,
    pub last_config_time: Time,
    pub corruption_count: i64,
    pub goroutine_count: i64,
    pub gomaxprocs: i64,
    pub gomemlimit: i64,
    pub gogc: String,
    pub godebug: String,
    pub storage_retention: String,
}

impl RuntimeInfo {
    pub fn write_json(&self, out: &mut String) {
        out.push_str("{\"startTime\":");
        self.start_time.write_json(out);
        out.push_str(",\"CWD\":");
        wstr(&self.cwd, out);
        out.push_str(",\"hostname\":");
        wstr(&self.hostname, out);
        out.push_str(",\"serverTime\":");
        self.server_time.write_json(out);
        out.push_str(",\"reloadConfigSuccess\":");
        wbool(self.reload_config_success, out);
        out.push_str(",\"lastConfigTime\":");
        self.last_config_time.write_json(out);
        out.push_str(",\"corruptionCount\":");
        json::write_i64(self.corruption_count, out);
        out.push_str(",\"goroutineCount\":");
        json::write_i64(self.goroutine_count, out);
        out.push_str(",\"GOMAXPROCS\":");
        json::write_i64(self.gomaxprocs, out);
        out.push_str(",\"GOMEMLIMIT\":");
        json::write_i64(self.gomemlimit, out);
        out.push_str(",\"GOGC\":");
        wstr(&self.gogc, out);
        out.push_str(",\"GODEBUG\":");
        wstr(&self.godebug, out);
        out.push_str(",\"storageRetention\":");
        wstr(&self.storage_retention, out);
        out.push('}');
    }
}

/// Go `PrometheusVersion`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PrometheusVersion {
    pub version: String,
    pub revision: String,
    pub branch: String,
    pub build_user: String,
    pub build_date: String,
    pub go_version: String,
}

impl PrometheusVersion {
    pub fn write_json(&self, out: &mut String) {
        out.push_str("{\"version\":");
        wstr(&self.version, out);
        out.push_str(",\"revision\":");
        wstr(&self.revision, out);
        out.push_str(",\"branch\":");
        wstr(&self.branch, out);
        out.push_str(",\"buildUser\":");
        wstr(&self.build_user, out);
        out.push_str(",\"buildDate\":");
        wstr(&self.build_date, out);
        out.push_str(",\"goVersion\":");
        wstr(&self.go_version, out);
        out.push('}');
    }
}

/// Go `TSDBStat`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TsdbStat {
    pub name: String,
    pub value: u64,
}

/// Go `HeadStats`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HeadStats {
    pub num_series: u64,
    pub num_label_pairs: i64,
    pub chunk_count: i64,
    pub min_time: i64,
    pub max_time: i64,
}

/// Go `TSDBStatus`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TsdbStatus {
    pub head_stats: HeadStats,
    pub series_count_by_metric_name: Vec<TsdbStat>,
    pub label_value_count_by_label_name: Vec<TsdbStat>,
    pub memory_in_bytes_by_label_name: Vec<TsdbStat>,
    pub series_count_by_label_value_pair: Vec<TsdbStat>,
}

fn write_tsdb_stats(v: &[TsdbStat], out: &mut String) {
    out.push('[');
    for (i, s) in v.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"name\":");
        wstr(&s.name, out);
        out.push_str(",\"value\":");
        out.push_str(&s.value.to_string());
        out.push('}');
    }
    out.push(']');
}

impl TsdbStatus {
    pub fn write_json(&self, out: &mut String) {
        let h = &self.head_stats;
        out.push_str("{\"headStats\":{\"numSeries\":");
        out.push_str(&h.num_series.to_string());
        out.push_str(",\"numLabelPairs\":");
        json::write_i64(h.num_label_pairs, out);
        out.push_str(",\"chunkCount\":");
        json::write_i64(h.chunk_count, out);
        out.push_str(",\"minTime\":");
        json::write_i64(h.min_time, out);
        out.push_str(",\"maxTime\":");
        json::write_i64(h.max_time, out);
        out.push_str("},\"seriesCountByMetricName\":");
        write_tsdb_stats(&self.series_count_by_metric_name, out);
        out.push_str(",\"labelValueCountByLabelName\":");
        write_tsdb_stats(&self.label_value_count_by_label_name, out);
        out.push_str(",\"memoryInBytesByLabelName\":");
        write_tsdb_stats(&self.memory_in_bytes_by_label_name, out);
        out.push_str(",\"seriesCountByLabelValuePair\":");
        write_tsdb_stats(&self.series_count_by_label_value_pair, out);
        out.push('}');
    }
}

/// The `data` of an API response.
#[derive(Clone, Debug, PartialEq)]
pub enum Data {
    Query(QueryData),
    QueryCost(QueryCostData),
    /// A JSON string (format_query).
    String(String),
    /// A JSON array of strings (labels, label values).
    Strings(Vec<String>),
    /// A JSON array of label sets (series).
    Series(Vec<Labels>),
    Exemplars(Vec<ExemplarQueryResult>),
    /// `{"scrapePools":[...]}`.
    ScrapePools(Vec<String>),
    /// `{"yaml":"..."}` (status/config, scrape_pools/config).
    Config(String),
    Targets(TargetDiscovery),
    TargetMetadata(Vec<MetricMetadataItem>),
    /// `{"steps":[...]}`.
    RelabelSteps(Vec<RelabelStep>),
    /// `{"activeAlertmanagers":[{"url":..}],"droppedAlertmanagers":[...]}`.
    Alertmanagers { active: Vec<String>, dropped: Vec<String> },
    /// `{"alerts":[...]}`.
    Alerts(Vec<Alert>),
    /// Metric metadata by metric name (Go map: keys sorted in JSON).
    Metadata(BTreeMap<String, Vec<Metadata>>),
    Rules(RuleDiscovery),
    RuntimeInfo(RuntimeInfo),
    BuildInfo(PrometheusVersion),
    /// Go `map[string]string` (keys sorted).
    Flags(BTreeMap<String, String>),
    /// Features by category (keys sorted, no HTML escaping).
    Features(BTreeMap<String, BTreeMap<String, bool>>),
    TsdbStatus(TsdbStatus),
    /// `{"blocks":[...]}`.
    Blocks(Vec<BlockMeta>),
    /// `{"min":..,"max":..,"current":..}`.
    WalReplay { min: i64, max: i64, current: i64 },
    Notifications(Vec<Notification>),
    /// `{"name":"..."}` (snapshot).
    Snapshot(String),
    /// Raw JSON messages (self metrics).
    RawList(Vec<String>),
    /// A JSON value (parse_query).
    Json(json::Json),
    /// A single float point `[ts, "v"]`.
    FPoint(promql::FPoint),
}

impl Data {
    pub fn write_json(&self, out: &mut String) {
        match self {
            Data::Query(q) => q.write_json(out),
            Data::QueryCost(c) => {
                out.push_str("{\"estimate\":");
                c.estimate.write_json(out);
                out.push('}');
            }
            Data::String(s) => wstr(s, out),
            Data::Strings(v) => write_strings(v, out),
            Data::Series(v) => {
                out.push('[');
                for (i, l) in v.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_labels(l, out);
                }
                out.push(']');
            }
            Data::Exemplars(v) => {
                out.push('[');
                for (i, r) in v.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_exemplar_query_result(r, out);
                }
                out.push(']');
            }
            Data::ScrapePools(v) => {
                out.push_str("{\"scrapePools\":");
                write_strings(v, out);
                out.push('}');
            }
            Data::Config(y) => {
                out.push_str("{\"yaml\":");
                wstr(y, out);
                out.push('}');
            }
            Data::Targets(t) => t.write_json(out),
            Data::TargetMetadata(v) => {
                out.push('[');
                for (i, m) in v.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str("{\"target\":");
                    write_labels(&m.target, out);
                    if !m.metric_family.is_empty() {
                        out.push_str(",\"metric\":");
                        wstr(&m.metric_family, out);
                    }
                    out.push_str(",\"type\":");
                    wstr(&m.typ, out);
                    out.push_str(",\"help\":");
                    wstr(&m.help, out);
                    out.push_str(",\"unit\":");
                    wstr(&m.unit, out);
                    out.push('}');
                }
                out.push(']');
            }
            Data::RelabelSteps(steps) => {
                out.push_str("{\"steps\":[");
                for (i, s) in steps.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str("{\"rule\":");
                    out.push_str(&s.rule_json);
                    out.push_str(",\"output\":");
                    write_labels(&s.output, out);
                    out.push_str(",\"keep\":");
                    wbool(s.keep, out);
                    out.push('}');
                }
                out.push_str("]}");
            }
            Data::Alertmanagers { active, dropped } => {
                out.push_str("{\"activeAlertmanagers\":[");
                for (i, u) in active.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str("{\"url\":");
                    wstr(u, out);
                    out.push('}');
                }
                out.push_str("],\"droppedAlertmanagers\":[");
                for (i, u) in dropped.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str("{\"url\":");
                    wstr(u, out);
                    out.push('}');
                }
                out.push_str("]}");
            }
            Data::Alerts(a) => {
                out.push_str("{\"alerts\":");
                write_alerts(a, out);
                out.push('}');
            }
            Data::Metadata(m) => {
                out.push('{');
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    wstr(k, out);
                    out.push_str(":[");
                    for (j, md) in v.iter().enumerate() {
                        if j > 0 {
                            out.push(',');
                        }
                        out.push_str("{\"type\":");
                        wstr(&md.typ, out);
                        out.push_str(",\"help\":");
                        wstr(&md.help, out);
                        out.push_str(",\"unit\":");
                        wstr(&md.unit, out);
                        out.push('}');
                    }
                    out.push(']');
                }
                out.push('}');
            }
            Data::Rules(r) => r.write_json(out),
            Data::RuntimeInfo(r) => r.write_json(out),
            Data::BuildInfo(b) => b.write_json(out),
            Data::Flags(m) => {
                out.push('{');
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    wstr(k, out);
                    out.push(':');
                    wstr(v, out);
                }
                out.push('}');
            }
            Data::Features(m) => {
                out.push('{');
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_str_no_html(k, out);
                    out.push_str(":{");
                    for (j, (n, b)) in v.iter().enumerate() {
                        if j > 0 {
                            out.push(',');
                        }
                        write_str_no_html(n, out);
                        out.push(':');
                        wbool(*b, out);
                    }
                    out.push('}');
                }
                out.push('}');
            }
            Data::TsdbStatus(s) => s.write_json(out),
            Data::Blocks(b) => {
                out.push_str("{\"blocks\":");
                if b.is_empty() {
                    out.push_str("[]");
                } else {
                    out.push('[');
                    for (i, m) in b.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        m.to_json().write(out);
                    }
                    out.push(']');
                }
                out.push('}');
            }
            Data::WalReplay { min, max, current } => {
                out.push_str("{\"min\":");
                json::write_i64(*min, out);
                out.push_str(",\"max\":");
                json::write_i64(*max, out);
                out.push_str(",\"current\":");
                json::write_i64(*current, out);
                out.push('}');
            }
            Data::Notifications(v) => {
                out.push('[');
                for (i, n) in v.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    n.write_json(out);
                }
                out.push(']');
            }
            Data::Snapshot(name) => {
                out.push_str("{\"name\":");
                wstr(name, out);
                out.push('}');
            }
            Data::RawList(v) => {
                out.push('[');
                for (i, r) in v.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(r);
                }
                out.push(']');
            }
            Data::Json(j) => j.write(out),
            Data::FPoint(p) => super::jsonutil::write_fpoint(p, out),
        }
    }
}

/// A JSON string without HTML escaping (jsoniter with `EscapeHTML: false`).
pub fn write_str_no_html(s: &str, out: &mut String) {
    let mut tmp = String::with_capacity(s.len() + 2);
    json::write_str(s, &mut tmp);
    out.push_str(&tmp.replace("\\u003c", "<").replace("\\u003e", ">").replace("\\u0026", "&"));
}
