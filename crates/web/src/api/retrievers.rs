//! The interfaces the API reads its data through (Go `ScrapePoolsRetriever`, `TargetRetriever`,
//! `AlertmanagerRetriever`, `RulesRetriever`, `TSDBAdminStats`, `promql.QueryEngine`). The server wires the real
//! implementations; tests use doubles.

use std::collections::HashMap;
use std::sync::Arc;

use config::ScrapeConfig;
use model::labels::{Labels, Matcher};
use promql::{QueryOpts, QueryResult};
use scrape::Target;
use storage::{Context, Error, Queryable};
use tsdb::BlockMeta;
use util::stats::Statistics;

use crate::gotime::{GoDuration, Time};

/// Go `ScrapePoolsRetriever`.
pub trait ScrapePoolsRetriever: Send + Sync {
    fn scrape_pools(&self) -> Vec<String>;
}

/// Go `TargetRetriever`.
pub trait TargetRetriever: Send + Sync {
    fn targets_active(&self) -> HashMap<String, Vec<Arc<Target>>>;
    fn targets_dropped(&self) -> HashMap<String, Vec<Arc<Target>>>;
    fn targets_dropped_counts(&self) -> HashMap<String, usize>;
    fn scrape_pool_config(&self, pool: &str) -> Result<Arc<ScrapeConfig>, Error>;
}

impl ScrapePoolsRetriever for scrape::Manager {
    fn scrape_pools(&self) -> Vec<String> {
        scrape::Manager::scrape_pools(self)
    }
}

impl TargetRetriever for scrape::Manager {
    fn targets_active(&self) -> HashMap<String, Vec<Arc<Target>>> {
        scrape::Manager::targets_active(self)
    }
    fn targets_dropped(&self) -> HashMap<String, Vec<Arc<Target>>> {
        scrape::Manager::targets_dropped(self)
    }
    fn targets_dropped_counts(&self) -> HashMap<String, usize> {
        scrape::Manager::targets_dropped_counts(self)
    }
    fn scrape_pool_config(&self, pool: &str) -> Result<Arc<ScrapeConfig>, Error> {
        scrape::Manager::scrape_pool_config(self, pool)
    }
}

/// Go `AlertmanagerRetriever`. URLs are returned as their `url.URL.String()` text.
pub trait AlertmanagerRetriever: Send + Sync {
    fn alertmanagers(&self) -> Vec<String>;
    fn dropped_alertmanagers(&self) -> Vec<String>;
}

/// One active alert of an alerting rule (the fields of Go `rules.Alert` the API reads).
#[derive(Clone, Debug, PartialEq)]
pub struct RuleAlert {
    pub labels: Labels,
    pub annotations: Labels,
    /// "pending", "firing" or "inactive" (Go `AlertState.String()`).
    pub state: String,
    pub active_at: Time,
    /// Zero when not keeping firing.
    pub keep_firing_since: Time,
    pub value: f64,
}

/// What the API reads from any rule (Go `rules.Rule`).
pub trait RuleView: Send + Sync {
    fn name(&self) -> String;
    /// Go `Query().String()`.
    fn query(&self) -> String;
    fn labels(&self) -> Labels;
    /// "ok", "err" or "unknown" (Go `RuleHealth`).
    fn health(&self) -> String;
    fn last_error(&self) -> Option<String>;
    fn evaluation_duration(&self) -> GoDuration;
    fn evaluation_timestamp(&self) -> Time;
}

/// What the API reads from an alerting rule (Go `*rules.AlertingRule`).
pub trait AlertingRuleView: RuleView {
    /// "pending", "firing" or "inactive".
    fn state(&self) -> String;
    fn hold_duration(&self) -> GoDuration;
    fn keep_firing_for(&self) -> GoDuration;
    fn annotations(&self) -> Labels;
    fn active_alerts(&self) -> Vec<RuleAlert>;
}

/// A rule of a group: alerting or recording (Go's type switch on `rules.Rule`).
#[derive(Clone)]
pub enum RuleRef {
    Alerting(Arc<dyn AlertingRuleView>),
    Recording(Arc<dyn RuleView>),
}

impl RuleRef {
    pub fn as_rule(&self) -> &dyn RuleView {
        match self {
            RuleRef::Alerting(r) => r.as_ref(),
            RuleRef::Recording(r) => r.as_ref(),
        }
    }
}

/// What the API reads from a rule group (Go `*rules.Group`).
pub trait RuleGroupView: Send + Sync {
    fn name(&self) -> String;
    fn file(&self) -> String;
    fn interval(&self) -> GoDuration;
    fn limit(&self) -> i64;
    fn evaluation_time(&self) -> GoDuration;
    fn last_evaluation(&self) -> Time;
    /// Go `Group.Rules(matcherSets...)`: the rules whose labels match any of the matcher sets (all rules when
    /// there are none).
    fn rules(&self, matcher_sets: &[Vec<Matcher>]) -> Vec<RuleRef>;
}

/// Go `RulesRetriever`.
pub trait RulesRetriever: Send + Sync {
    fn rule_groups(&self) -> Vec<Arc<dyn RuleGroupView>>;
    fn alerting_rules(&self) -> Vec<Arc<dyn AlertingRuleView>>;
}

/// Go `tsdb.WALReplayStatus`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalReplayStatus {
    pub min: i64,
    pub max: i64,
    pub current: i64,
}

/// Go `TSDBAdminStats`: the TSDB operations of the admin and status endpoints.
pub trait TsdbAdminStats: Send + Sync {
    fn clean_tombstones(&self) -> Result<(), Error>;
    fn delete(&self, ctx: &Context, mint: i64, maxt: i64, ms: &[Matcher]) -> Result<(), Error>;
    fn snapshot(&self, dir: &std::path::Path, with_head: bool) -> Result<(), Error>;
    fn stats(&self, stats_by_label_name: &str, limit: usize) -> Result<tsdb::head::Stats, Error>;
    fn wal_replay_status(&self) -> Result<WalReplayStatus, Error>;
    fn block_metas(&self) -> Result<Vec<BlockMeta>, Error>;
}

/// The TSDB as the admin and status endpoints use it (Go's `readyStorage` methods over `*tsdb.DB`).
impl TsdbAdminStats for tsdb::db::DB {
    fn clean_tombstones(&self) -> Result<(), Error> {
        tsdb::db::DB::clean_tombstones(self)
    }
    fn delete(&self, ctx: &Context, mint: i64, maxt: i64, ms: &[Matcher]) -> Result<(), Error> {
        tsdb::db::DB::delete(self, ctx, mint, maxt, ms)
    }
    fn snapshot(&self, dir: &std::path::Path, with_head: bool) -> Result<(), Error> {
        tsdb::db::DB::snapshot(self, dir, with_head)
    }
    fn stats(&self, stats_by_label_name: &str, limit: usize) -> Result<tsdb::head::Stats, Error> {
        Ok(self.head().stats(stats_by_label_name, limit))
    }
    fn wal_replay_status(&self) -> Result<WalReplayStatus, Error> {
        let s = self.head().stats.get_wal_replay_status();
        Ok(WalReplayStatus { min: s.min, max: s.max, current: s.current })
    }
    fn block_metas(&self) -> Result<Vec<BlockMeta>, Error> {
        Ok(tsdb::db::DB::block_metas(self))
    }
}

/// A query built by a [`QueryEngine`] (Go `promql.Query`).
pub trait ApiQuery: Send {
    fn exec(&mut self, ctx: &Context) -> QueryResult;
    fn close(&mut self);
    fn stats(&self) -> Option<Statistics>;
}

impl ApiQuery for promql::Query {
    fn exec(&mut self, ctx: &Context) -> QueryResult {
        promql::Query::exec(self, ctx)
    }
    fn close(&mut self) {
        promql::Query::close(self)
    }
    fn stats(&self) -> Option<Statistics> {
        Some(promql::Query::stats(self))
    }
}

/// A held query slot; dropping it releases the slot.
pub type QuerySlotGuard = Box<dyn Send>;

/// Go `promql.QueryEngine` (with `QuerySlotAcquirer`). Times are milliseconds.
pub trait QueryEngine: Send + Sync {
    fn new_instant_query(
        &self,
        ctx: &Context,
        q: Arc<dyn Queryable>,
        opts: Option<&QueryOpts>,
        qs: &str,
        ts: i64,
    ) -> Result<Box<dyn ApiQuery>, Error>;
    #[allow(clippy::too_many_arguments)]
    fn new_range_query(
        &self,
        ctx: &Context,
        q: Arc<dyn Queryable>,
        opts: Option<&QueryOpts>,
        qs: &str,
        start: i64,
        end: i64,
        step: i64,
    ) -> Result<Box<dyn ApiQuery>, Error>;
    /// Go `QuerySlotAcquirer.AcquireQuerySlot`; `None` when the engine does not implement it.
    fn acquire_query_slot(&self, _ctx: &Context, _qs: &str) -> Option<Result<QuerySlotGuard, Error>> {
        None
    }
}

impl QueryEngine for promql::Engine {
    fn new_instant_query(
        &self,
        ctx: &Context,
        q: Arc<dyn Queryable>,
        opts: Option<&QueryOpts>,
        qs: &str,
        ts: i64,
    ) -> Result<Box<dyn ApiQuery>, Error> {
        promql::Engine::new_instant_query(self, ctx, q, opts, qs, ts).map(|q| Box::new(q) as Box<dyn ApiQuery>)
    }
    fn new_range_query(
        &self,
        ctx: &Context,
        q: Arc<dyn Queryable>,
        opts: Option<&QueryOpts>,
        qs: &str,
        start: i64,
        end: i64,
        step: i64,
    ) -> Result<Box<dyn ApiQuery>, Error> {
        promql::Engine::new_range_query(self, ctx, q, opts, qs, start, end, step).map(|q| Box::new(q) as Box<dyn ApiQuery>)
    }
    fn acquire_query_slot(&self, ctx: &Context, qs: &str) -> Option<Result<QuerySlotGuard, Error>> {
        Some(promql::Engine::acquire_query_slot(self, ctx, qs).map(|s| Box::new(s) as QuerySlotGuard))
    }
}
