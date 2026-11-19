//! Go `readyStorage`, `notReadyAppender`, `readyScrapeManager` and the TSDB metric gauges of
//! `openDBWithMetrics`.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};

use discovery::metrics::{Collector, Family, Metric, MetricType, MetricValue, Registry};
use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{Labels, Matcher};
use model::metadata::Metadata;
use storage::{
    AppendOptions, AppendV2Options, Appendable, AppendableV2, Appender, AppenderTransaction, AppenderV2, BoxChunkQuerier,
    BoxQuerier, ChunkQueryable, Context, CostSamplingQueryable, Error, ExemplarAppenderV2, ExemplarQuerier,
    ExemplarQueryable, Queryable, SampleAndChunkQueryable, SeriesRef, Storage,
};
use tsdb::db::{DB, ERR_NOT_READY};

/// The storage behind [`ReadyStorage`].
#[derive(Clone)]
pub enum LocalDb {
    /// The TSDB (server mode).
    Tsdb(Arc<DB>),
    /// Any other storage (the agent WAL storage, tests). TSDB-only operations give "not supported".
    Other(Arc<dyn Storage>),
}

impl LocalDb {
    fn storage(&self) -> &dyn Storage {
        match self {
            LocalDb::Tsdb(db) => &**db,
            LocalDb::Other(s) => &**s,
        }
    }
}

/// Go `agent.ErrUnsupported`.
pub fn err_unsupported() -> Error {
    agent::ERR_UNSUPPORTED
}

/// Go `readyStorage`: a storage whose database is set once it is open.
#[derive(Default)]
pub struct ReadyStorage {
    db: OnceLock<LocalDb>,
    start_time_margin: AtomicI64,
    stats: tsdb::db::DbStats,
}

impl ReadyStorage {
    pub fn new() -> ReadyStorage {
        ReadyStorage { db: OnceLock::new(), start_time_margin: AtomicI64::new(0), stats: tsdb::db::DbStats::new() }
    }

    /// Go `Set`. `None` keeps the storage not ready (Go `Set(nil, ...)`). Set at most once.
    pub fn set(&self, db: Option<LocalDb>, start_time_margin: i64) {
        self.start_time_margin.store(start_time_margin, Ordering::SeqCst);
        if let Some(db) = db {
            let _ = self.db.set(db);
        }
    }

    pub fn get(&self) -> Option<&LocalDb> {
        self.db.get()
    }

    /// The TSDB, when set.
    pub fn tsdb(&self) -> Option<&Arc<DB>> {
        match self.db.get() {
            Some(LocalDb::Tsdb(db)) => Some(db),
            _ => None,
        }
    }

    /// Go `getStats`: the stats the TSDB is opened with.
    pub fn stats(&self) -> tsdb::db::DbStats {
        self.stats.clone()
    }

    /// Go `ApplyConfig`: applies the storage part of the config to the TSDB.
    pub fn apply_config(&self, conf: &config::Config) -> Result<(), Error> {
        match self.tsdb() {
            Some(db) => db.apply_config(&tsdb_apply_config(conf)),
            None => Ok(()),
        }
    }

    /// Go `CleanTombstones` (api TSDBAdminStats).
    pub fn clean_tombstones(&self) -> Result<(), Error> {
        match self.db.get() {
            Some(LocalDb::Tsdb(db)) => db.clean_tombstones(),
            Some(LocalDb::Other(_)) => Err(err_unsupported()),
            None => Err(ERR_NOT_READY),
        }
    }
}

/// The TSDB view of the config (Go `tsdb.DB.ApplyConfig(*config.Config)`).
pub fn tsdb_apply_config(conf: &config::Config) -> tsdb::db::ApplyConfig {
    tsdb::db::ApplyConfig {
        tsdb: conf.storage_config.tsdb_config.as_ref().map(|t| tsdb::db::TsdbConfig {
            out_of_order_time_window: t.out_of_order_time_window,
            stale_series_compaction_threshold: t.stale_series_compaction_threshold,
            retention: t.retention.as_ref().map(|r| tsdb::db::TsdbRetentionConfig {
                time: r.time.millis(),
                size: r.size.0,
                percentage: r.percentage,
            }),
            float_chunk_encoding: t.chunk_encoding.floats.clone(),
        }),
        max_exemplars: conf.storage_config.exemplars_config.as_ref().map(|e| e.max_exemplars).unwrap_or(0),
    }
}

impl Queryable for ReadyStorage {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        match self.db.get() {
            Some(db) => db.storage().querier(mint, maxt),
            None => Err(ERR_NOT_READY),
        }
    }
}

impl ChunkQueryable for ReadyStorage {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        match self.db.get() {
            Some(db) => db.storage().chunk_querier(mint, maxt),
            None => Err(ERR_NOT_READY),
        }
    }
}

impl CostSamplingQueryable for ReadyStorage {
    /// Go `CostSamplingQueryable`: forwards to the ready storage when it has the capability.
    fn cost_sampling_queryable(&self) -> (Option<&dyn SampleAndChunkQueryable>, bool) {
        match self.db.get().and_then(|db| db.storage().as_cost_sampling_queryable()) {
            Some(p) => p.cost_sampling_queryable(),
            None => (None, false),
        }
    }
}

impl ExemplarQueryable for ReadyStorage {
    fn exemplar_querier(&self, ctx: &Context) -> Result<Box<dyn ExemplarQuerier>, Error> {
        match self.db.get() {
            Some(LocalDb::Tsdb(db)) => db.exemplar_querier(ctx),
            Some(LocalDb::Other(_)) => Err(err_unsupported()),
            None => Err(ERR_NOT_READY),
        }
    }
}

impl Appendable for ReadyStorage {
    fn appender(&self, ctx: &Context) -> Box<dyn Appender> {
        match self.db.get() {
            Some(db) => db.storage().appender(ctx),
            None => Box::new(NotReadyAppender),
        }
    }
}

impl AppendableV2 for ReadyStorage {
    fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
        match self.db.get() {
            Some(db) => db.storage().appender_v2(ctx),
            None => Box::new(NotReadyAppenderV2),
        }
    }
}

impl Storage for ReadyStorage {
    /// Go `StartTime`: the first block's min time (now without blocks) plus the margin; `i64::MAX` and
    /// `ErrNotReady` before the storage is set.
    fn start_time(&self) -> Result<i64, Error> {
        match self.db.get() {
            Some(LocalDb::Tsdb(db)) => {
                let blocks = db.blocks();
                let start = match blocks.first() {
                    Some(b) => b.min_time(),
                    None => now_millis() / 1000 * 1000,
                };
                Ok(start + self.start_time_margin.load(Ordering::SeqCst))
            }
            Some(LocalDb::Other(s)) => s.start_time(),
            None => Err(ERR_NOT_READY),
        }
    }
    fn close(&self) -> Option<Error> {
        match self.db.get() {
            Some(db) => db.storage().close(),
            None => None,
        }
    }
    fn as_cost_sampling_queryable(&self) -> Option<&dyn CostSamplingQueryable> {
        Some(self)
    }
}

impl web::api::TsdbAdminStats for ReadyStorage {
    fn clean_tombstones(&self) -> Result<(), Error> {
        ReadyStorage::clean_tombstones(self)
    }
    fn delete(&self, ctx: &Context, mint: i64, maxt: i64, ms: &[Matcher]) -> Result<(), Error> {
        match self.db.get() {
            Some(LocalDb::Tsdb(db)) => db.delete(ctx, mint, maxt, ms),
            Some(LocalDb::Other(_)) => Err(err_unsupported()),
            None => Err(ERR_NOT_READY),
        }
    }
    fn snapshot(&self, dir: &std::path::Path, with_head: bool) -> Result<(), Error> {
        match self.db.get() {
            Some(LocalDb::Tsdb(db)) => db.snapshot(dir, with_head),
            Some(LocalDb::Other(_)) => Err(err_unsupported()),
            None => Err(ERR_NOT_READY),
        }
    }
    fn stats(&self, stats_by_label_name: &str, limit: usize) -> Result<tsdb::head::Stats, Error> {
        match self.db.get() {
            Some(LocalDb::Tsdb(db)) => Ok(db.head().stats(stats_by_label_name, limit)),
            Some(LocalDb::Other(_)) => Err(err_unsupported()),
            None => Err(ERR_NOT_READY),
        }
    }
    /// Go `WALReplayStatus`: read from the stats the TSDB is opened with, so it works during the replay.
    fn wal_replay_status(&self) -> Result<web::api::WalReplayStatus, Error> {
        let s = self.stats.head.get_wal_replay_status();
        Ok(web::api::WalReplayStatus { min: s.min, max: s.max, current: s.current })
    }
    fn block_metas(&self) -> Result<Vec<tsdb::BlockMeta>, Error> {
        match self.db.get() {
            Some(LocalDb::Tsdb(db)) => Ok(db.block_metas()),
            Some(LocalDb::Other(_)) => Err(err_unsupported()),
            None => Err(ERR_NOT_READY),
        }
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// Go `notReadyAppender`: every call fails with `ErrNotReady`.
pub struct NotReadyAppender;

impl AppenderTransaction for NotReadyAppender {
    fn commit(&mut self) -> Option<Error> {
        Some(ERR_NOT_READY)
    }
    fn rollback(&mut self) -> Option<Error> {
        Some(ERR_NOT_READY)
    }
}

impl Appender for NotReadyAppender {
    fn append(&mut self, _: SeriesRef, _: &Labels, _: i64, _: f64) -> Result<SeriesRef, Error> {
        Err(ERR_NOT_READY)
    }
    fn set_options(&mut self, _: Option<&AppendOptions>) {}
    fn append_exemplar(&mut self, _: SeriesRef, _: &Labels, _: &Exemplar) -> Result<SeriesRef, Error> {
        Err(ERR_NOT_READY)
    }
    fn append_histogram(&mut self, _: SeriesRef, _: &Labels, _: i64, _: Option<&Histogram>, _: Option<&FloatHistogram>) -> Result<SeriesRef, Error> {
        Err(ERR_NOT_READY)
    }
    fn append_histogram_st_zero_sample(
        &mut self,
        _: SeriesRef,
        _: &Labels,
        _: i64,
        _: i64,
        _: Option<&Histogram>,
        _: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        Err(ERR_NOT_READY)
    }
    fn update_metadata(&mut self, _: SeriesRef, _: &Labels, _: &Metadata) -> Result<SeriesRef, Error> {
        Err(ERR_NOT_READY)
    }
    fn append_st_zero_sample(&mut self, _: SeriesRef, _: &Labels, _: i64, _: i64) -> Result<SeriesRef, Error> {
        Err(ERR_NOT_READY)
    }
}

/// Go `notReadyAppenderV2`.
pub struct NotReadyAppenderV2;

impl AppenderTransaction for NotReadyAppenderV2 {
    fn commit(&mut self) -> Option<Error> {
        Some(ERR_NOT_READY)
    }
    fn rollback(&mut self) -> Option<Error> {
        Some(ERR_NOT_READY)
    }
}

impl AppenderV2 for NotReadyAppenderV2 {
    fn append(
        &mut self,
        _: SeriesRef,
        _: &Labels,
        _: i64,
        _: i64,
        _: f64,
        _: Option<&Histogram>,
        _: Option<&FloatHistogram>,
        _: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        Err(ERR_NOT_READY)
    }
    fn as_exemplar_appender_v2(&mut self) -> Option<&mut dyn ExemplarAppenderV2> {
        Some(self)
    }
}

impl ExemplarAppenderV2 for NotReadyAppenderV2 {
    fn append_exemplars(&mut self, _: SeriesRef, _: &Labels, _: &[Exemplar]) -> Result<SeriesRef, Error> {
        Err(ERR_NOT_READY)
    }
}

/// Go `ErrNotReady` of the scrape manager.
pub const ERR_SCRAPE_NOT_READY: &str = "scrape manager not ready";

/// Go `readyScrapeManager`.
#[derive(Default)]
pub struct ReadyScrapeManager {
    m: OnceLock<Arc<scrape::Manager>>,
}

impl ReadyScrapeManager {
    pub fn set(&self, m: Arc<scrape::Manager>) {
        let _ = self.m.set(m);
    }
    pub fn get(&self) -> Result<Arc<scrape::Manager>, String> {
        self.m.get().cloned().ok_or_else(|| ERR_SCRAPE_NOT_READY.to_string())
    }
}

impl remote::metadata_watcher::ReadyScrapeManager for ReadyScrapeManager {
    fn get(&self) -> Result<Arc<dyn remote::metadata_watcher::Watchable>, String> {
        ReadyScrapeManager::get(self).map(|m| m as Arc<dyn remote::metadata_watcher::Watchable>)
    }
}

/// The gauges that Go's `openDBWithMetrics` registers: the lowest timestamp and the head bounds in seconds.
pub struct DbTimeGauges(pub Arc<DB>);

fn gauge(name: &str, help: &str, v: f64) -> Family {
    Family {
        name: name.to_string(),
        help: help.to_string(),
        typ: MetricType::Gauge,
        metrics: vec![Metric { labels: Vec::new(), value: MetricValue::Gauge(v) }],
    }
}

impl Collector for DbTimeGauges {
    fn describe(&self) -> Vec<String> {
        vec![
            "prometheus_tsdb_lowest_timestamp_seconds".to_string(),
            "prometheus_tsdb_head_min_time_seconds".to_string(),
            "prometheus_tsdb_head_max_time_seconds".to_string(),
        ]
    }
    fn collect(&self, out: &mut Vec<Family>) {
        let db = &self.0;
        // Go divides int64 milliseconds by 1000 (integer division) before the float conversion.
        let lowest = match db.blocks().first() {
            Some(b) => b.min_time() / 1000,
            None => db.head().min_time() / 1000,
        };
        out.push(gauge("prometheus_tsdb_lowest_timestamp_seconds", "Lowest timestamp value stored in the database.", lowest as f64));
        out.push(gauge("prometheus_tsdb_head_min_time_seconds", "Minimum time bound of the head block.", (db.head().min_time() / 1000) as f64));
        out.push(gauge("prometheus_tsdb_head_max_time_seconds", "Maximum timestamp of the head block.", (db.head().max_time() / 1000) as f64));
    }
}

/// Go `openDBWithMetrics`: opens the TSDB and registers the time gauges.
pub fn open_db_with_metrics(
    dir: &str,
    logger: Option<tsdb::Logger>,
    reg: &Arc<Registry>,
    opts: Option<tsdb::db::Options>,
    stats: Option<tsdb::db::DbStats>,
) -> Result<Arc<DB>, Error> {
    let db = tsdb::db::open(std::path::Path::new(dir), logger, opts, stats)?;
    if let Err(e) = reg.register(Arc::new(DbTimeGauges(db.clone()))) {
        // Go panics in MustRegister.
        panic!("{e}");
    }
    Ok(db)
}
