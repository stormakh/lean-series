//! Test doubles of `api_test.go` and `errors_test.go`.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use config::ScrapeConfig;
use model::labels::{Labels, Matcher};
use model::labelset::LabelSet;
use model::relabel::{Action, Config as RelabelConfig, Regexp};
use scrape::{MetricMetadata, MetricMetadataStore, Target};
use storage::{
    Annotations, BoxChunkQuerier, BoxQuerier, BoxSeriesSet, ChunkQueryable, Context, Error, ExemplarQuerier, ExemplarQueryable, LabelHints,
    LabelQuerier, LabelResult, Querier, Queryable, SelectHints, SeriesArc, SeriesSet,
};
use tsdb::BlockMeta;
use web::api::{
    AlertingRuleView, AlertmanagerRetriever, RuleAlert, RuleGroupView, RuleRef, RuleView, RulesRetriever, TargetRetriever, TsdbAdminStats,
    WalReplayStatus,
};
use web::gotime::{GoDuration, SECOND, Time};

/// Go `errorTestQueryable`.
#[derive(Clone)]
pub struct ErrorTestQueryable {
    pub q: Option<ErrorTestQuerier>,
    pub err: Option<Error>,
}

impl ErrorTestQueryable {
    pub fn err(e: Error) -> ErrorTestQueryable {
        ErrorTestQueryable { q: None, err: Some(e) }
    }
}

impl Queryable for ErrorTestQueryable {
    fn querier(&self, _mint: i64, _maxt: i64) -> Result<BoxQuerier, Error> {
        if let Some(q) = &self.q {
            return Ok(Box::new(q.clone()));
        }
        Err(self.err.clone().unwrap())
    }
}

impl ChunkQueryable for ErrorTestQueryable {
    fn chunk_querier(&self, _mint: i64, _maxt: i64) -> Result<BoxChunkQuerier, Error> {
        Err(self.err.clone().unwrap())
    }
}

impl ExemplarQueryable for ErrorTestQueryable {
    fn exemplar_querier(&self, _ctx: &Context) -> Result<Box<dyn ExemplarQuerier>, Error> {
        Err(self.err.clone().unwrap())
    }
}

/// Go `errorTestQuerier`.
#[derive(Clone)]
pub struct ErrorTestQuerier {
    pub s: Option<ErrorTestSeriesSet>,
    pub err: Option<Error>,
}

impl LabelQuerier for ErrorTestQuerier {
    fn label_values(&self, _: &Context, _: &str, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), self.err.clone())
    }
    fn label_names(&self, _: &Context, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), self.err.clone())
    }
    fn close(&mut self) -> Option<Error> {
        None
    }
}

impl Querier for ErrorTestQuerier {
    fn select(&self, _: &Context, _: bool, _: Option<&SelectHints>, _: &[Matcher]) -> BoxSeriesSet {
        if let Some(s) = &self.s {
            return Box::new(s.clone());
        }
        storage::err_series_set(self.err.clone().unwrap())
    }
}

/// Go `errorTestSeriesSet`.
#[derive(Clone)]
pub struct ErrorTestSeriesSet {
    pub err: Error,
}

impl SeriesSet for ErrorTestSeriesSet {
    fn next(&mut self) -> bool {
        false
    }
    fn at(&self) -> &SeriesArc {
        unreachable!()
    }
    fn err(&self) -> Option<Error> {
        Some(self.err.clone())
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

/// Go `testMetaStore`.
#[derive(Default)]
pub struct TestMetaStore {
    pub metadata: Vec<MetricMetadata>,
}

impl MetricMetadataStore for TestMetaStore {
    fn list_metadata(&self) -> Vec<MetricMetadata> {
        self.metadata.clone()
    }
    fn get_metadata(&self, metric: &str) -> Option<MetricMetadata> {
        self.metadata.iter().find(|m| m.metric_family == metric).cloned()
    }
    fn size_metadata(&self) -> usize {
        0
    }
    fn length_metadata(&self) -> usize {
        0
    }
}

/// Go `testReport`.
pub struct TestReport {
    pub start: SystemTime,
    pub duration: Duration,
    pub error: Option<Error>,
}

/// Go `testTargetParams`.
pub struct TestTargetParams {
    pub identifier: &'static str,
    pub labels: Labels,
    pub target_labels: LabelSet,
    pub params: BTreeMap<String, Vec<String>>,
    pub reports: Vec<TestReport>,
    pub active: bool,
}

/// Go `testTargetRetriever`.
#[derive(Default)]
pub struct TestTargetRetriever {
    pub active_targets: HashMap<String, Vec<Arc<Target>>>,
    pub dropped_targets: HashMap<String, Vec<Arc<Target>>>,
    pub scrape_pool_config_err: Option<Error>,
}

impl TestTargetRetriever {
    /// Go `newTestTargetRetriever`.
    pub fn new(targets: Vec<TestTargetParams>) -> TestTargetRetriever {
        let mut r = TestTargetRetriever::default();
        for t in targets {
            let cfg = Arc::new(ScrapeConfig { params: t.params, ..Default::default() });
            let nt = Arc::new(Target::new(t.labels, cfg, t.target_labels, LabelSet::new()));
            for rep in &t.reports {
                nt.report(rep.start, rep.duration, rep.error.as_ref());
            }
            if t.active {
                r.active_targets.insert(t.identifier.to_string(), vec![nt]);
            } else {
                r.dropped_targets.insert(t.identifier.to_string(), vec![nt]);
            }
        }
        r
    }

    /// Go `SetMetadataStoreForTargets`.
    pub fn set_metadata_store_for_targets(&self, identifier: &str, md: Arc<dyn MetricMetadataStore>) -> Result<(), String> {
        let Some(ts) = self.active_targets.get(identifier) else { return Err(format!("no active target for {}", identifier)) };
        for t in ts {
            t.set_metadata_store(md.clone());
        }
        Ok(())
    }

    /// Go `ResetMetadataStore`.
    pub fn reset_metadata_store(&self) {
        for ts in self.active_targets.values() {
            for t in ts {
                t.set_metadata_store(Arc::new(TestMetaStore::default()));
            }
        }
    }
}

fn relabel(action: Action, source: Option<Vec<&str>>, target: &str, regex: &str, replacement: &str, legacy: bool) -> RelabelConfig {
    RelabelConfig {
        source_labels: source.map(|v| v.into_iter().map(String::from).collect()),
        separator: String::new(),
        regex: Regexp::must_new(regex),
        modulus: 0,
        target_label: target.to_string(),
        replacement: replacement.to_string(),
        action,
        name_validation_scheme: if legacy { model::labels::ValidationScheme::Legacy } else { model::labels::ValidationScheme::Unset },
    }
}

impl TargetRetriever for TestTargetRetriever {
    fn targets_active(&self) -> HashMap<String, Vec<Arc<Target>>> {
        self.active_targets.clone()
    }
    fn targets_dropped(&self) -> HashMap<String, Vec<Arc<Target>>> {
        self.dropped_targets.clone()
    }
    fn targets_dropped_counts(&self) -> HashMap<String, usize> {
        self.dropped_targets.iter().map(|(k, v)| (k.clone(), v.len())).collect()
    }
    fn scrape_pool_config(&self, pool: &str) -> Result<Arc<ScrapeConfig>, Error> {
        if let Some(e) = &self.scrape_pool_config_err {
            return Err(e.clone());
        }
        let mut cfg = ScrapeConfig { job_name: pool.to_string(), ..Default::default() };
        cfg.http_client_config.authorization =
            Some(net::config::Authorization { credentials: yaml::types::Secret("credential helper secret".to_string()), ..Default::default() });
        cfg.relabel_configs = vec![
            relabel(Action::Replace, None, "__address__", "", "example.com:443", true),
            relabel(Action::Drop, Some(vec!["__address__"]), "", r"example\.com:.*", "", false),
        ];
        if pool == "testpool3" {
            cfg.relabel_configs.push(relabel(Action::Replace, None, "job", ".*", "should_not_apply", false));
        }
        Ok(Arc::new(cfg))
    }
}

/// Go `testAlertmanagerRetriever`.
pub struct TestAlertmanagerRetriever;

impl AlertmanagerRetriever for TestAlertmanagerRetriever {
    fn alertmanagers(&self) -> Vec<String> {
        vec!["http://alertmanager.example.com:8080/api/v1/alerts".to_string()]
    }
    fn dropped_alertmanagers(&self) -> Vec<String> {
        vec!["http://dropped.alertmanager.example.com:8080/api/v1/alerts".to_string()]
    }
}

/// Go `fakeDB`.
#[derive(Default)]
pub struct FakeDb {
    pub err: Option<Error>,
    pub block_metas: Vec<BlockMeta>,
}

impl TsdbAdminStats for FakeDb {
    fn clean_tombstones(&self) -> Result<(), Error> {
        self.err.clone().map_or(Ok(()), Err)
    }
    fn delete(&self, _ctx: &Context, _mint: i64, _maxt: i64, _ms: &[Matcher]) -> Result<(), Error> {
        self.err.clone().map_or(Ok(()), Err)
    }
    fn snapshot(&self, _dir: &Path, _with_head: bool) -> Result<(), Error> {
        self.err.clone().map_or(Ok(()), Err)
    }
    fn stats(&self, stats_by_label_name: &str, limit: usize) -> Result<tsdb::head::Stats, Error> {
        let dir = super::temp_dir("tsdb-api-ready");
        let mut opts = tsdb::head::HeadOptions::new();
        opts.chunk_range = 1000;
        opts.chunk_dir_root = dir.clone();
        let h = tsdb::head::Head::new(None, None, opts, None)?;
        let s = h.stats(stats_by_label_name, limit);
        let _ = std::fs::remove_dir_all(&dir);
        Ok(s)
    }
    fn wal_replay_status(&self) -> Result<WalReplayStatus, Error> {
        Ok(WalReplayStatus::default())
    }
    fn block_metas(&self) -> Result<Vec<BlockMeta>, Error> {
        Ok(self.block_metas.clone())
    }
}

/// Go `scrapeStart = time.Now().Add(-11 * time.Second)` (a package variable: one value for the whole run).
pub fn scrape_start() -> SystemTime {
    static S: std::sync::OnceLock<SystemTime> = std::sync::OnceLock::new();
    *S.get_or_init(|| SystemTime::now() - Duration::from_secs(11))
}

/// Go `setupTestTargetRetriever`.
pub fn setup_test_target_retriever() -> TestTargetRetriever {
    let lbls = |scheme: &str, addr: &str, path: &str, job: &str, interval: &str, timeout: &str| {
        model::labels::from_map([
            ("__scheme__", scheme),
            ("__address__", addr),
            ("__metrics_path__", path),
            ("job", job),
            ("__scrape_interval__", interval),
            ("__scrape_timeout__", timeout),
        ])
    };
    let targets = vec![
        TestTargetParams {
            identifier: "test",
            labels: lbls("http", "example.com:8080", "/metrics", "test", "15s", "5s"),
            target_labels: LabelSet::new(),
            params: BTreeMap::new(),
            reports: vec![TestReport { start: scrape_start(), duration: Duration::from_millis(70), error: None }],
            active: true,
        },
        TestTargetParams {
            identifier: "blackbox",
            labels: lbls("http", "localhost:9115", "/probe", "blackbox", "20s", "10s"),
            target_labels: LabelSet::new(),
            params: BTreeMap::from([("target".to_string(), vec!["example.com".to_string()])]),
            reports: vec![TestReport { start: scrape_start(), duration: Duration::from_millis(100), error: Some(Error::msg("failed")) }],
            active: true,
        },
        TestTargetParams {
            identifier: "blackbox",
            labels: Labels::empty(),
            target_labels: LabelSet::from_pairs(&[
                ("__scheme__", "http"),
                ("__address__", "http://dropped.example.com:9115"),
                ("__metrics_path__", "/probe"),
                ("job", "blackbox"),
                ("__scrape_interval__", "30s"),
                ("__scrape_timeout__", "15s"),
            ]),
            params: BTreeMap::new(),
            reports: vec![],
            active: false,
        },
    ];
    TestTargetRetriever::new(targets)
}

/// The relabel configs that `TestTargetRetriever::scrape_pool_config` builds (for expected values).
pub fn test_relabel_config(action: Action, source: Option<Vec<&str>>, target: &str, regex: &str, replacement: &str, legacy: bool) -> RelabelConfig {
    relabel(action, source, target, regex, replacement, legacy)
}

// adapted: rules are a double with the state of the real rules after one evaluation; the rules crate is not on master.
// Go's `rulesRetrieverMock` builds real `rules.AlertingRule`/`rules.RecordingRule` objects in two `rules.Group`s
// ("grp" with all seven rules, "grp2" with the first alerting rule) and evaluates "grp" once against an empty test
// storage. After that one evaluation: every rule has health "ok" and no error; `absent(test_metric3) != 1` and
// `up == 1` return nothing (inactive, no alerts); `vector(1)` (test_metric5, hold 1s) has one pending alert
// {alertname="test_metric5", name="tm5"} with value 1; "grp2" shares the evaluated rule object of test_metric3 and
// was not evaluated itself.

/// A rule with the state of Go's `rules.AlertingRule` / `rules.RecordingRule` after one evaluation.
pub struct MockRule {
    pub name: String,
    pub query: String,
    pub labels: Labels,
    pub annotations: Labels,
    pub hold: GoDuration,
    pub health: String,
    pub eval_duration: GoDuration,
    pub eval_timestamp: Time,
    pub alerts: Vec<RuleAlert>,
}

impl RuleView for MockRule {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn query(&self) -> String {
        self.query.clone()
    }
    fn labels(&self) -> Labels {
        self.labels.clone()
    }
    fn health(&self) -> String {
        self.health.clone()
    }
    fn last_error(&self) -> Option<String> {
        None
    }
    fn evaluation_duration(&self) -> GoDuration {
        self.eval_duration
    }
    fn evaluation_timestamp(&self) -> Time {
        self.eval_timestamp
    }
}

impl AlertingRuleView for MockRule {
    /// Go `AlertingRule.State()`: the highest state of the active alerts.
    fn state(&self) -> String {
        if self.alerts.iter().any(|a| a.state == "firing") {
            "firing".to_string()
        } else if self.alerts.iter().any(|a| a.state == "pending") {
            "pending".to_string()
        } else {
            "inactive".to_string()
        }
    }
    fn hold_duration(&self) -> GoDuration {
        self.hold
    }
    fn keep_firing_for(&self) -> GoDuration {
        0
    }
    fn annotations(&self) -> Labels {
        self.annotations.clone()
    }
    /// Go `ActiveAlerts`: the alerts that are not inactive.
    fn active_alerts(&self) -> Vec<RuleAlert> {
        self.alerts.iter().filter(|a| a.state != "inactive").cloned().collect()
    }
}

/// A group with the state of Go's `rules.Group`.
pub struct MockGroup {
    pub name: String,
    pub file: String,
    pub interval: GoDuration,
    pub eval_time: GoDuration,
    pub last_eval: Time,
    pub rules: Vec<RuleRef>,
}

impl RuleGroupView for MockGroup {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn file(&self) -> String {
        self.file.clone()
    }
    fn interval(&self) -> GoDuration {
        self.interval
    }
    fn limit(&self) -> i64 {
        0
    }
    fn evaluation_time(&self) -> GoDuration {
        self.eval_time
    }
    fn last_evaluation(&self) -> Time {
        self.last_eval
    }
    /// Go `Group.Rules(matcherSets...)`: matchers of a set are ANDed, the sets are ORed, on the rule labels.
    fn rules(&self, matcher_sets: &[Vec<Matcher>]) -> Vec<RuleRef> {
        if matcher_sets.is_empty() {
            return self.rules.clone();
        }
        self.rules
            .iter()
            .filter(|r| {
                let l = r.as_rule().labels();
                matcher_sets.iter().any(|ms| ms.iter().all(|m| m.matches(l.get(&m.name))))
            })
            .cloned()
            .collect()
    }
}

/// Go `rulesRetrieverMock`.
pub struct RulesRetrieverMock {
    pub alerting_rules: Vec<Arc<MockRule>>,
    pub rule_groups: Vec<Arc<MockGroup>>,
}

impl RulesRetrieverMock {
    /// Go `CreateAlertingRules`, `CreateRuleGroups` and `g[0].Eval(ctx, time.Now())`.
    pub fn new() -> RulesRetrieverMock {
        let now = Time::now();
        let eval_d: GoDuration = 1_000_000;
        let alerting = |name: &str, query: &str, labels: Labels, alerts: Vec<RuleAlert>| {
            Arc::new(MockRule {
                name: name.to_string(),
                query: query.to_string(),
                labels,
                annotations: Labels::empty(),
                hold: SECOND,
                health: "ok".to_string(),
                eval_duration: eval_d,
                eval_timestamp: now,
                alerts,
            })
        };
        let from = |s: &[&str]| model::labels::from_strings(s);
        let rule1 = alerting("test_metric3", "absent(test_metric3) != 1", Labels::empty(), vec![]);
        let rule2 = alerting("test_metric4", "up == 1", Labels::empty(), vec![]);
        let rule3 = alerting(
            "test_metric5",
            "vector(1)",
            from(&["name", "tm5"]),
            vec![RuleAlert {
                labels: from(&["alertname", "test_metric5", "name", "tm5"]),
                annotations: Labels::empty(),
                state: "pending".to_string(),
                active_at: now,
                keep_firing_since: Time::ZERO,
                value: 1.0,
            }],
        );
        let rule4 = alerting("test_metric6", "up == 1", from(&["testlabel", "rule"]), vec![]);
        let rule5 = alerting("test_metric7", "up == 1", from(&["templatedlabel", "{{ $externalURL }}"]), vec![]);
        let alerting_rules = vec![rule1, rule2, rule3, rule4, rule5];
        let recording = |name: &str, labels: Labels| -> Arc<dyn RuleView> {
            Arc::new(MockRule {
                name: name.to_string(),
                query: "vector(1)".to_string(),
                labels,
                annotations: Labels::empty(),
                hold: 0,
                health: "ok".to_string(),
                eval_duration: eval_d,
                eval_timestamp: now,
                alerts: vec![],
            })
        };
        let mut r: Vec<RuleRef> = alerting_rules.iter().map(|a| RuleRef::Alerting(a.clone() as Arc<dyn AlertingRuleView>)).collect();
        r.push(RuleRef::Recording(recording("recording-rule-1", Labels::empty())));
        r.push(RuleRef::Recording(recording("recording-rule-2", from(&["testlabel", "rule"]))));
        let group = Arc::new(MockGroup {
            name: "grp".to_string(),
            file: "/path/to/file".to_string(),
            interval: SECOND,
            eval_time: eval_d,
            last_eval: now,
            rules: r.clone(),
        });
        let group2 = Arc::new(MockGroup {
            name: "grp2".to_string(),
            file: "/path/to/file".to_string(),
            interval: SECOND,
            eval_time: 0,
            last_eval: Time::ZERO,
            rules: vec![r[0].clone()],
        });
        RulesRetrieverMock { alerting_rules, rule_groups: vec![group, group2] }
    }
}

impl RulesRetriever for RulesRetrieverMock {
    fn rule_groups(&self) -> Vec<Arc<dyn RuleGroupView>> {
        self.rule_groups.iter().map(|g| g.clone() as Arc<dyn RuleGroupView>).collect()
    }
    fn alerting_rules(&self) -> Vec<Arc<dyn AlertingRuleView>> {
        self.alerting_rules.iter().map(|a| a.clone() as Arc<dyn AlertingRuleView>).collect()
    }
}
