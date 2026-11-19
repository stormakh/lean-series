//! Self-instrumentation collectors for the TSDB and the query engine.
//!
//! The TSDB and the engine keep their metrics as plain atomics; the collectors here read them at gather time
//! and expose the families that upstream registers for a server-mode `tsdb.DB` (`tsdb/db.go` newDBMetrics,
//! `tsdb/head.go` newHeadMetrics, `tsdb/compact.go`, `tsdb/exemplar.go`, `tsdb/wlog/wlog.go`,
//! `tsdb/tsdbutil/dir_locker.go`) and for `promql.NewEngine`, with the same names, help, types, labels and
//! buckets. Nothing here runs on the append or query paths.
//!
//! Known differences: summaries are exposed with count and sum only (the registry has no quantiles), so the
//! `quantile` series of `prometheus_tsdb_wal_fsync_duration_seconds` and
//! `prometheus_engine_query_duration_seconds` are missing. The `type` children of the head sample counters
//! appear once the head has created an appender (Go creates them on the first commit).

use std::sync::Arc;

use discovery::metrics::{Collector, Family, Metric, MetricType, MetricValue, Registry};
use tsdb::reader::BlockReader;
use tsdb::db::DB;
use tsdb::metrics::{Counter, Gauge, Histogram};
use tsdb_format::wlog::compression::Type as CompressionType;
use tsdb_format::wlog::WL;

/// Registers the TSDB self-instrumentation of `db` (Go `newDBMetrics`, `newHeadMetrics` and the metrics of
/// the compactor, exemplar storage, WAL, WBL and lock file). The `*_seconds` time gauges that Go's
/// `openDBWithMetrics` adds in main are not included.
pub fn register_tsdb_metrics(reg: &Arc<Registry>, db: &Arc<DB>) -> Result<(), String> {
    reg.register(Arc::new(TsdbCollector { db: db.clone() }))
}

/// Registers the PromQL engine metrics (Go `NewEngine`); the query-cost families only when that feature is on.
pub fn register_engine_metrics(reg: &Arc<Registry>, engine: &promql::Engine) -> Result<(), String> {
    reg.register(Arc::new(EngineCollector { engine: engine.clone() }))
}

// ---------------- family helpers ----------------

fn family(name: &str, help: &str, typ: MetricType, metrics: Vec<Metric>) -> Family {
    Family { name: name.to_string(), help: help.to_string(), typ, metrics }
}

fn one(value: MetricValue) -> Vec<Metric> {
    vec![Metric { labels: Vec::new(), value }]
}

fn labeled(label: &str, values: Vec<(String, MetricValue)>) -> Vec<Metric> {
    values.into_iter().map(|(v, value)| Metric { labels: vec![(label.to_string(), v)], value }).collect()
}

struct Out<'a>(&'a mut Vec<Family>);

impl Out<'_> {
    fn counter(&mut self, name: &str, help: &str, v: f64) {
        self.0.push(family(name, help, MetricType::Counter, one(MetricValue::Counter(v))));
    }
    fn gauge(&mut self, name: &str, help: &str, v: f64) {
        self.0.push(family(name, help, MetricType::Gauge, one(MetricValue::Gauge(v))));
    }
    fn c(&mut self, name: &str, help: &str, c: &Counter) {
        self.counter(name, help, c.get());
    }
    fn g(&mut self, name: &str, help: &str, g: &Gauge) {
        self.gauge(name, help, g.get());
    }
    fn hist(&mut self, name: &str, help: &str, h: &Histogram) {
        let buckets = h.buckets.iter().copied().zip(h.cumulative_counts()).collect();
        let v = MetricValue::Histogram { count: h.sample_count(), sum: h.sample_sum(), buckets };
        self.0.push(family(name, help, MetricType::Histogram, one(v)));
    }
    fn summary(&mut self, name: &str, help: &str, count: u64, sum: f64) {
        self.0.push(family(name, help, MetricType::Summary, one(MetricValue::Summary { count, sum })));
    }
    fn counter_vec(&mut self, name: &str, help: &str, label: &str, values: Vec<(String, f64)>) {
        let vals = values.into_iter().map(|(l, v)| (l, MetricValue::Counter(v))).collect();
        self.0.push(family(name, help, MetricType::Counter, labeled(label, vals)));
    }
}

/// The children of a `type` counter vector: `float` and `histogram` once Go would have created them
/// (`committed`), else only the ones with a value.
fn type_children(t: &tsdb::head::TypeCounter, committed: bool, histogram_on_commit: bool) -> Vec<(String, f64)> {
    let mut v = Vec::new();
    if committed || t.float.get() != 0.0 {
        v.push(("float".to_string(), t.float.get()));
    }
    if (committed && histogram_on_commit) || t.histogram.get() != 0.0 {
        v.push(("histogram".to_string(), t.histogram.get()));
    }
    v
}

// ---------------- TSDB ----------------

struct TsdbCollector {
    db: Arc<DB>,
}

/// The upstream family names, for the registry's duplicate check.
const TSDB_FAMILIES: &[&str] = &[
    "prometheus_tsdb_blocks_loaded",
    "prometheus_tsdb_symbol_table_size_bytes",
    "prometheus_tsdb_reloads_total",
    "prometheus_tsdb_lowest_timestamp",
    "prometheus_tsdb_head_series",
    "prometheus_tsdb_head_max_time",
    "prometheus_tsdb_head_min_time",
    "prometheus_tsdb_clean_start",
    "prometheus_tsdb_compactions_total",
    "prometheus_tsdb_exemplar_exemplars_appended_total",
    "prometheus_tsdb_wal_page_flushes_total",
];

impl Collector for TsdbCollector {
    fn describe(&self) -> Vec<String> {
        TSDB_FAMILIES.iter().map(|s| s.to_string()).collect()
    }

    fn collect(&self, fams: &mut Vec<Family>) {
        let db = &*self.db;
        let head = db.head();
        let mut o = Out(fams);

        // tsdb/db.go newDBMetrics.
        let blocks = db.blocks();
        let m = &db.metrics;
        o.gauge("prometheus_tsdb_blocks_loaded", "Number of currently loaded data blocks", blocks.len() as f64);
        let sym: u64 = blocks.iter().map(|b| b.get_symbol_table_size()).sum();
        o.gauge("prometheus_tsdb_symbol_table_size_bytes", "Size of symbol table in memory for loaded blocks", sym as f64);
        o.c("prometheus_tsdb_reloads_total", "Number of times the database reloaded block data from disk.", &m.reloads);
        o.c(
            "prometheus_tsdb_reloads_failures_total",
            "Number of times the database failed to reloadBlocks block data from disk.",
            &m.reloads_failed,
        );
        o.c("prometheus_tsdb_compactions_failed_total", "Total number of compactions that failed for the partition.", &m.compactions_failed);
        o.c(
            "prometheus_tsdb_compactions_triggered_total",
            "Total number of triggered compactions for the partition.",
            &m.compactions_triggered,
        );
        o.c(
            "prometheus_tsdb_compactions_skipped_total",
            "Total number of skipped compactions due to disabled auto compaction.",
            &m.compactions_skipped,
        );
        o.c(
            "prometheus_tsdb_size_retentions_total",
            "The number of times that blocks were deleted because the maximum number of bytes was exceeded.",
            &m.size_retention_count,
        );
        o.c(
            "prometheus_tsdb_time_retentions_total",
            "The number of times that blocks were deleted because the maximum time limit was exceeded.",
            &m.time_retention_count,
        );
        let lowest = match blocks.first() {
            Some(b) => b.meta().min_time,
            None => head.min_time(),
        };
        o.gauge(
            "prometheus_tsdb_lowest_timestamp",
            "Lowest timestamp value stored in the database. The unit is decided by the library consumer.",
            lowest as f64,
        );
        drop(blocks);
        o.hist("prometheus_tsdb_tombstone_cleanup_seconds", "The time taken to recompact blocks to remove tombstones.", &m.tomb_clean_timer);
        o.g("prometheus_tsdb_storage_blocks_bytes", "The number of bytes that are currently used for local storage by all blocks.", &m.blocks_bytes);
        o.g(
            "prometheus_tsdb_retention_limit_bytes",
            "Max number of bytes to be retained in the tsdb blocks, configured 0 means disabled",
            &m.max_bytes,
        );
        o.g(
            "prometheus_tsdb_retention_limit_percentage",
            "Max percentage of total storage space to be retained in the tsdb blocks, configured 0 means disabled",
            &m.max_percentage,
        );
        o.g("prometheus_tsdb_retention_limit_seconds", "How long to retain samples in storage.", &m.retention_duration);
        o.c(
            "prometheus_tsdb_stale_series_compactions_triggered_total",
            "Total number of triggered stale series compactions.",
            &m.stale_series_compactions_triggered,
        );
        o.c(
            "prometheus_tsdb_stale_series_compactions_failed_total",
            "Total number of stale series compactions that failed.",
            &m.stale_series_compactions_failed,
        );
        o.hist(
            "prometheus_tsdb_stale_series_compaction_duration_seconds",
            "Duration of stale series compaction runs.",
            &m.stale_series_compaction_duration,
        );
        o.c(
            "prometheus_tsdb_selected_series_compactions_triggered_total",
            "Total number of compactions triggered for an explicit caller-provided list of series references.",
            &m.selected_series_compactions_triggered,
        );
        o.c(
            "prometheus_tsdb_selected_series_compactions_failed_total",
            "Total number of compactions triggered for an explicit caller-provided list of series references that failed.",
            &m.selected_series_compactions_failed,
        );
        o.hist(
            "prometheus_tsdb_selected_series_compaction_duration_seconds",
            "Duration of compactions triggered for an explicit caller-provided list of series references.",
            &m.selected_series_compaction_duration,
        );

        // tsdb/tsdbutil/dir_locker.go: -1 when there is no lock file.
        let clean = db.locker.lock().unwrap_or_else(|e| e.into_inner()).as_ref().map_or(-1.0, |l| l.created_cleanly.get());
        o.gauge(
            "prometheus_tsdb_clean_start",
            "-1: lockfile is disabled. 0: a lockfile from a previous execution was replaced. 1: lockfile creation was clean",
            clean,
        );

        // tsdb/compact.go NewCompactorMetrics.
        if let Some(c) = &db.leveled_compactor {
            let cm = c.metrics();
            o.c("prometheus_tsdb_compactions_total", "Total number of compactions that were executed for the partition.", &cm.ran);
            o.g(
                "prometheus_tsdb_compaction_populating_block",
                "Set to 1 when a block is currently being written to the disk.",
                &cm.populating_blocks,
            );
            o.c("prometheus_tsdb_vertical_compactions_total", "Total number of compactions done on overlapping blocks.", &cm.overlapping_blocks);
            o.hist("prometheus_tsdb_compaction_duration_seconds", "Duration of compaction runs", &cm.duration);
            o.hist("prometheus_tsdb_compaction_chunk_size_bytes", "Final size of chunks on their first compaction", &cm.chunk_size);
            o.hist("prometheus_tsdb_compaction_chunk_samples", "Final number of samples on their first compaction", &cm.chunk_samples);
            o.hist("prometheus_tsdb_compaction_chunk_range_seconds", "Final time range of chunks on their first compaction", &cm.chunk_range);
        }

        // tsdb/head.go newHeadMetrics.
        let hm = &head.metrics;
        o.g("prometheus_tsdb_head_active_appenders", "Number of currently active appender transactions", &hm.active_appenders);
        o.c("prometheus_tsdb_head_appenders_created_total", "Total number of appender transactions created.", &hm.appenders_created);
        o.gauge("prometheus_tsdb_head_series", "Total number of series in the head block.", head.num_series() as f64);
        o.gauge("prometheus_tsdb_head_stale_series", "Total number of stale series in the head block.", head.num_stale_series() as f64);
        o.gauge(
            "prometheus_tsdb_head_native_histogram_series",
            "Number of series whose most recent in-order sample is a native histogram.",
            head.num_native_histogram_series() as f64,
        );
        o.gauge(
            "prometheus_tsdb_head_native_histogram_buckets",
            "Number of positive and negative bucket entries in the most recent in-order native histogram sample of each series, including entries carried by staleness markers.",
            head.num_native_histogram_buckets() as f64,
        );
        o.g("prometheus_tsdb_head_chunks", "Total number of chunks in the head block.", &hm.chunks);
        o.c("prometheus_tsdb_head_chunks_created_total", "Total number of chunks created in the head", &hm.chunks_created);
        o.c("prometheus_tsdb_head_chunks_removed_total", "Total number of chunks removed in the head", &hm.chunks_removed);
        o.c("prometheus_tsdb_head_series_created_total", "Total number of series created in the head", &hm.series_created);
        o.c("prometheus_tsdb_head_series_removed_total", "Total number of series removed in the head", &hm.series_removed);
        o.c("prometheus_tsdb_head_series_not_found_total", "Total number of requests for series that were not found.", &hm.series_not_found);
        o.c(
            "prometheus_tsdb_head_series_pending_commit_underflow_total",
            "Total number of attempts to release a pending-sample reservation when none was held.",
            &hm.pending_commit_underflow,
        );
        o.summary(
            "prometheus_tsdb_head_gc_duration_seconds",
            "Runtime of garbage collection in the head block.",
            hm.gc_duration.sample_count(),
            hm.gc_duration.sample_sum(),
        );
        o.summary(
            "prometheus_tsdb_wal_truncate_duration_seconds",
            "Duration of WAL truncation.",
            hm.wal_truncate_duration.sample_count(),
            hm.wal_truncate_duration.sample_sum(),
        );
        o.c("prometheus_tsdb_wal_corruptions_total", "Total number of WAL corruptions.", &hm.wal_corruptions_total);
        o.g("prometheus_tsdb_data_replay_duration_seconds", "Time taken to replay the data on disk.", &hm.data_total_replay_duration);
        // Go's commit creates the float and histogram children of these vectors (head_append.go).
        let committed = hm.appenders_created.get() > 0.0;
        o.counter_vec(
            "prometheus_tsdb_head_samples_appended_total",
            "Total number of appended samples.",
            "type",
            type_children(&hm.samples_appended, committed, true),
        );
        o.counter_vec(
            "prometheus_tsdb_head_out_of_order_samples_appended_total",
            "Total number of appended out of order samples.",
            "type",
            type_children(&hm.out_of_order_samples_appended, committed, true),
        );
        o.counter_vec(
            "prometheus_tsdb_out_of_bound_samples_total",
            "Total number of out of bound samples ingestion failed attempts with out of order support disabled.",
            "type",
            type_children(&hm.out_of_bound_samples, committed, false),
        );
        o.counter_vec(
            "prometheus_tsdb_out_of_order_samples_total",
            "Total number of out of order samples ingestion failed attempts due to out of order being disabled.",
            "type",
            type_children(&hm.out_of_order_samples, committed, true),
        );
        o.counter_vec(
            "prometheus_tsdb_too_old_samples_total",
            "Total number of out of order samples ingestion failed attempts with out of support enabled, but sample outside of time window.",
            "type",
            type_children(&hm.too_old_samples, committed, false),
        );
        o.c("prometheus_tsdb_head_truncations_failed_total", "Total number of head truncations that failed.", &hm.head_truncate_fail);
        o.c("prometheus_tsdb_head_truncations_total", "Total number of head truncations attempted.", &hm.head_truncate_total);
        o.c("prometheus_tsdb_checkpoint_deletions_failed_total", "Total number of checkpoint deletions that failed.", &hm.checkpoint_delete_fail);
        o.c("prometheus_tsdb_checkpoint_deletions_total", "Total number of checkpoint deletions attempted.", &hm.checkpoint_delete_total);
        o.c("prometheus_tsdb_checkpoint_creations_failed_total", "Total number of checkpoint creations that failed.", &hm.checkpoint_creation_fail);
        o.c("prometheus_tsdb_checkpoint_creations_total", "Total number of checkpoint creations attempted.", &hm.checkpoint_creation_total);
        o.hist(
            "prometheus_tsdb_sample_ooo_delta",
            "Delta in seconds by which a sample is considered out of order (reported regardless of OOO time window and whether sample is accepted or not).",
            &hm.ooo_histogram,
        );
        o.c("prometheus_tsdb_mmap_chunks_total", "Total number of chunks that were memory-mapped.", &hm.mmap_chunks_total);
        o.c("prometheus_tsdb_mmap_chunk_corruptions_total", "Total number of memory-mapped chunk corruptions.", &hm.mmap_chunk_corruption_total);
        o.c("prometheus_tsdb_snapshot_replay_error_total", "Total number snapshot replays that failed.", &hm.snapshot_replay_error_total);
        o.gauge(
            "prometheus_tsdb_head_max_time",
            "Maximum timestamp of the head block. The unit is decided by the library consumer.",
            head.max_time() as f64,
        );
        o.gauge(
            "prometheus_tsdb_head_min_time",
            "Minimum time bound of the head block. The unit is decided by the library consumer.",
            head.min_time() as f64,
        );
        o.gauge("prometheus_tsdb_isolation_low_watermark", "The lowest TSDB append ID that is still referenced.", head.iso.low_watermark() as f64);
        o.gauge("prometheus_tsdb_isolation_high_watermark", "The highest TSDB append ID that has been given out.", head.iso.last_append_id() as f64);
        o.gauge(
            "prometheus_tsdb_head_chunks_storage_size_bytes",
            "Size of the chunks_head directory.",
            head.chunk_disk_mapper.size().unwrap_or(0) as f64,
        );
        o.counter_vec(
            "prometheus_tsdb_wal_replay_unknown_refs_total",
            "Total number of unknown series references encountered during WAL replay.",
            "type",
            hm.wal_replay_unknown_refs_total.values(),
        );
        o.counter_vec(
            "prometheus_tsdb_wbl_replay_unknown_refs_total",
            "Total number of unknown series references encountered during WBL replay.",
            "type",
            hm.wbl_replay_unknown_refs_total.values(),
        );

        // tsdb/exemplar.go NewExemplarMetrics.
        let em = &head.exemplar_metrics;
        o.c("prometheus_tsdb_exemplar_exemplars_appended_total", "Total number of appended exemplars.", &em.exemplars_appended);
        o.g("prometheus_tsdb_exemplar_exemplars_in_storage", "Number of exemplars currently in circular storage.", &em.exemplars_in_storage);
        o.g(
            "prometheus_tsdb_exemplar_series_with_exemplars_in_storage",
            "Number of series with exemplars currently in circular storage.",
            &em.series_with_exemplars_in_storage,
        );
        o.g(
            "prometheus_tsdb_exemplar_last_exemplars_timestamp_seconds",
            "The timestamp of the oldest exemplar stored in circular storage. Useful to check for what timerange the current exemplar buffer limit allows. This usually means the last timestampfor all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.",
            &em.last_exemplars_ts,
        );
        o.c(
            "prometheus_tsdb_exemplar_out_of_order_exemplars_total",
            "Total number of out of order exemplar ingestion failed attempts.",
            &em.out_of_order_exemplars,
        );
        o.g(
            "prometheus_tsdb_exemplar_max_exemplars",
            "Total number of exemplars the exemplar storage can store, resizeable.",
            &em.max_exemplars,
        );

        // tsdb/wlog/wlog.go newWLMetrics, for the WAL and the WBL.
        if let Some(w) = head.wal() {
            wl_families(&mut o, w);
        }
        if let Some(w) = head.wbl() {
            wl_families(&mut o, &w);
        }
    }
}

/// The families of one write log, named with its prefix (`prometheus_tsdb_wal_` or
/// `prometheus_tsdb_out_of_order_wbl_`).
fn wl_families(o: &mut Out, w: &WL) {
    use std::sync::atomic::Ordering::Relaxed;
    let m = w.metrics();
    let p = m.prefix;
    let n = |s: &str| format!("{p}{s}");
    let u = |a: &std::sync::atomic::AtomicU64| a.load(Relaxed) as f64;
    o.summary(
        &n("fsync_duration_seconds"),
        "Duration of write log fsync.",
        m.fsync_duration_count.load(Relaxed),
        m.fsync_duration_sum_nanos.load(Relaxed) as f64 / 1e9,
    );
    o.counter(&n("page_flushes_total"), "Total number of page flushes.", u(&m.page_flushes));
    o.counter(&n("completed_pages_total"), "Total number of completed pages.", u(&m.page_completions));
    o.counter(&n("truncations_failed_total"), "Total number of write log truncations that failed.", u(&m.truncate_fail));
    o.counter(&n("truncations_total"), "Total number of write log truncations attempted.", u(&m.truncate_total));
    o.gauge(&n("segment_current"), "Write log segment index that TSDB is currently writing to.", m.current_segment.load(Relaxed) as f64);
    o.counter(&n("writes_failed_total"), "Total number of write log writes that failed.", u(&m.writes_failed));
    o.gauge(&n("storage_size_bytes"), "Size of the write log directory.", w.size().unwrap_or(0) as f64);
    o.counter(&n("record_part_writes_total"), "Total number of record parts written before flushing.", u(&m.record_part_writes));
    o.counter(
        &n("record_parts_bytes_written_total"),
        "Total number of record part bytes written before flushing, including CRC and compression headers.",
        u(&m.record_part_bytes),
    );
    // Go creates the child on the first compressed record.
    let saved = u(&m.record_bytes_saved);
    let c = w.compression_type();
    let children = if c != CompressionType::None && (saved != 0.0 || m.record_part_writes.load(Relaxed) > 0) {
        vec![(c.as_str().to_string(), saved)]
    } else {
        Vec::new()
    };
    o.counter_vec(
        &n("record_bytes_saved_total"),
        "Total number of bytes saved by the optional record compression. Use this metric to learn about the effectiveness compression.",
        "compression",
        children,
    );
}

// ---------------- engine ----------------

struct EngineCollector {
    engine: promql::Engine,
}

impl Collector for EngineCollector {
    fn describe(&self) -> Vec<String> {
        vec!["prometheus_engine_queries".to_string(), "prometheus_engine_query_duration_seconds".to_string()]
    }

    fn collect(&self, fams: &mut Vec<Family>) {
        use promql::engine::{
            QUERY_DURATION_BUCKETS, QUERY_DURATION_SLICES, QUERY_SAMPLES_READ_BUCKETS, QUERY_SERIES_TOUCHED_BUCKETS,
        };
        use std::sync::atomic::Ordering::Relaxed;
        let m = self.engine.metrics();
        let mut o = Out(fams);
        o.gauge("prometheus_engine_queries", "The current number of queries being executed or waiting.", m.current_queries.load(Relaxed) as f64);
        o.gauge(
            "prometheus_engine_queries_concurrent_max",
            "The max number of concurrent queries.",
            m.max_concurrent_queries.load(Relaxed) as f64,
        );
        o.gauge("prometheus_engine_query_log_enabled", "State of the query log.", m.query_log_enabled.load(Relaxed) as f64);
        o.counter("prometheus_engine_query_log_failures_total", "The number of query log failures.", m.query_log_failures.load(Relaxed) as f64);
        o.counter(
            "prometheus_engine_query_samples_total",
            "The total number of samples loaded by all queries (full window per step for range-vector).",
            m.query_samples.load(Relaxed) as f64,
        );
        o.counter(
            "prometheus_engine_query_samples_read_total",
            "The total number of samples read by all queries (only new points per step for range-vector).",
            m.query_samples_read.load(Relaxed) as f64,
        );
        let slice = |i: usize| vec![("slice".to_string(), QUERY_DURATION_SLICES[i].to_string())];
        let mut summaries = Vec::new();
        let mut histograms = Vec::new();
        for (i, h) in m.query_duration.iter().enumerate() {
            summaries.push(Metric { labels: slice(i), value: MetricValue::Summary { count: h.count(), sum: h.sum() } });
            histograms.push(Metric {
                labels: slice(i),
                value: MetricValue::Histogram { count: h.count(), sum: h.sum(), buckets: h.cumulative(&QUERY_DURATION_BUCKETS) },
            });
        }
        o.0.push(family("prometheus_engine_query_duration_seconds", "Query timings", MetricType::Summary, summaries));
        o.0.push(family(
            "prometheus_engine_query_duration_histogram_seconds",
            "The duration of various parts of PromQL query execution.",
            MetricType::Histogram,
            histograms,
        ));
        if self.engine.query_cost_enabled() {
            let rejected: Vec<(String, f64)> = {
                let g = m.query_rejected_total.lock().unwrap_or_else(|e| e.into_inner());
                g.iter().map(|(k, v)| (k.to_string(), *v as f64)).collect()
            };
            o.counter_vec(
                "prometheus_engine_query_rejected_total",
                "The total number of queries rejected by cost guardrails, by reason.",
                "reason",
                rejected,
            );
            for (name, help, h, b) in [
                (
                    "prometheus_engine_query_series_touched",
                    "The number of series touched by a query (per-selector sum, an upper bound on distinct series).",
                    &m.query_series_touched,
                    &QUERY_SERIES_TOUCHED_BUCKETS[..],
                ),
                (
                    "prometheus_engine_query_samples_read",
                    "The number of samples read by a query.",
                    &m.query_samples_read_histogram,
                    &QUERY_SAMPLES_READ_BUCKETS[..],
                ),
            ] {
                let v = MetricValue::Histogram { count: h.count(), sum: h.sum(), buckets: h.cumulative(b) };
                o.0.push(family(name, help, MetricType::Histogram, one(v)));
            }
        }
    }
}
