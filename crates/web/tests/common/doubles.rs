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
use web::api::{AlertmanagerRetriever, TargetRetriever, TsdbAdminStats, WalReplayStatus};

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
