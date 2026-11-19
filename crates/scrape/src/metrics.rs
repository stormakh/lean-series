//! Port of `scrape/metrics.go`: the scrape manager's own metrics, with upstream names and help texts.
//! Histograms are classic histograms (the registry has no native histograms).

use std::sync::{Arc, Mutex, Weak};

use discovery::metrics::{
    Collector, CounterVec, Family, GaugeVec, HistogramVec, Metric, MetricType, MetricValue, MetricVec, Opts,
    Registry, SummaryVec,
};

use crate::target::Target;

/// The active targets by scrape pool (Go `TargetsGatherer`).
pub trait TargetsGatherer: Send + Sync {
    fn targets_active(&self) -> std::collections::HashMap<String, Vec<Arc<Target>>>;
}

/// Go `MetadataMetricsCollector`: metadata cache entries and bytes per scrape job.
#[derive(Default)]
pub struct MetadataMetricsCollector {
    pub targets_gatherer: Mutex<Option<Weak<dyn TargetsGatherer>>>,
}

impl Collector for MetadataMetricsCollector {
    fn describe(&self) -> Vec<String> {
        vec![
            "prometheus_target_metadata_cache_entries".to_string(),
            "prometheus_target_metadata_cache_bytes".to_string(),
        ]
    }
    fn collect(&self, out: &mut Vec<Family>) {
        let g = self.targets_gatherer.lock().unwrap().as_ref().and_then(|w| w.upgrade());
        let Some(g) = g else { return };
        let mut entries = Family {
            name: "prometheus_target_metadata_cache_entries".to_string(),
            help: "Total number of metric metadata entries in the cache".to_string(),
            typ: MetricType::Gauge,
            metrics: Vec::new(),
        };
        let mut bytes = Family {
            name: "prometheus_target_metadata_cache_bytes".to_string(),
            help: "The number of bytes that are currently used for storing metric metadata in the cache".to_string(),
            typ: MetricType::Gauge,
            metrics: Vec::new(),
        };
        for (tset, targets) in g.targets_active() {
            let (mut size, mut length) = (0usize, 0usize);
            for t in &targets {
                size += t.size_metadata();
                length += t.length_metadata();
            }
            let labels = vec![("scrape_job".to_string(), tset.clone())];
            entries.metrics.push(Metric { labels: labels.clone(), value: MetricValue::Gauge(length as f64) });
            bytes.metrics.push(Metric { labels, value: MetricValue::Gauge(size as f64) });
        }
        out.push(entries);
        out.push(bytes);
    }
}

const SYNC_BUCKETS: [f64; 4] = [0.01, 0.1, 1.0, 10.0];
const DEFAULT_BUCKETS: [f64; 11] = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

/// Go `scrapeMetrics`.
pub struct ScrapeMetrics {
    pub reg: Option<Arc<Registry>>,
    pub target_metadata_cache: Arc<MetadataMetricsCollector>,
    pub target_scrape_pools: Arc<CounterVec>,
    pub target_scrape_pools_failed: Arc<CounterVec>,
    pub target_reload_interval_length: Arc<SummaryVec>,
    pub target_scrape_pool_reloads: Arc<CounterVec>,
    pub target_scrape_pool_reloads_failed: Arc<CounterVec>,
    pub target_scrape_pool_syncs_counter: Arc<CounterVec>,
    pub target_scrape_pool_exceeded_target_limit: Arc<CounterVec>,
    pub target_scrape_pool_target_limit: Arc<GaugeVec>,
    pub target_scrape_pool_targets_added: Arc<GaugeVec>,
    pub target_scrape_pool_symbol_table_items: Arc<GaugeVec>,
    pub target_sync_interval_length: Arc<SummaryVec>,
    pub target_sync_interval_length_histogram: Arc<HistogramVec>,
    pub target_sync_failed: Arc<CounterVec>,
    pub target_scrape_exceeded_body_size_limit: Arc<CounterVec>,
    pub target_scrape_cache_flush_forced: Arc<CounterVec>,
    pub target_interval_length: Arc<SummaryVec>,
    pub target_interval_length_histogram: Arc<HistogramVec>,
    pub target_scrape_sample_limit: Arc<CounterVec>,
    pub target_scrape_sample_duplicate: Arc<CounterVec>,
    pub target_scrape_sample_out_of_order: Arc<CounterVec>,
    pub target_scrape_sample_out_of_bounds: Arc<CounterVec>,
    pub target_scrape_exemplar_out_of_order: Arc<CounterVec>,
    pub target_scrape_pool_exceeded_label_limits: Arc<CounterVec>,
    pub target_scrape_native_histogram_bucket_limit: Arc<CounterVec>,
    pub target_scrape_duration: Arc<HistogramVec>,
}

impl ScrapeMetrics {
    /// Go `newScrapeMetrics`: creates the metrics and registers them with `reg` (if any).
    pub fn new(reg: Option<Arc<Registry>>) -> Result<Arc<ScrapeMetrics>, String> {
        let c = |n: &str, h: &str| MetricVec::new_counter(Opts::new(n, h));
        let cv = |n: &str, h: &str, l: &[&str]| MetricVec::new_counter_vec(Opts::new(n, h), l);
        let gv = |n: &str, h: &str, l: &[&str]| MetricVec::new_gauge_vec(Opts::new(n, h), l);
        let sv = |n: &str, h: &str, l: &[&str]| MetricVec::new_summary_vec(Opts::new(n, h), l);
        let hv = |n: &str, h: &str, l: &[&str], b: &[f64]| MetricVec::new_histogram_vec(Opts::new(n, h), l, b);
        let sm = ScrapeMetrics {
            reg: reg.clone(),
            target_metadata_cache: Arc::new(MetadataMetricsCollector::default()),
            target_scrape_pools: c("prometheus_target_scrape_pools_total", "Total number of scrape pool creation attempts."),
            target_scrape_pools_failed: c(
                "prometheus_target_scrape_pools_failed_total",
                "Total number of scrape pool creations that failed.",
            ),
            target_reload_interval_length: sv(
                "prometheus_target_reload_length_seconds",
                "Actual interval to reload the scrape pool with a given configuration.",
                &["interval"],
            ),
            target_scrape_pool_reloads: c("prometheus_target_scrape_pool_reloads_total", "Total number of scrape pool reloads."),
            target_scrape_pool_reloads_failed: c(
                "prometheus_target_scrape_pool_reloads_failed_total",
                "Total number of failed scrape pool reloads.",
            ),
            target_scrape_pool_exceeded_target_limit: c(
                "prometheus_target_scrape_pool_exceeded_target_limit_total",
                "Total number of times scrape pools hit the target limit, during sync or config reload.",
            ),
            target_scrape_pool_target_limit: gv(
                "prometheus_target_scrape_pool_target_limit",
                "Maximum number of targets allowed in this scrape pool.",
                &["scrape_job"],
            ),
            target_scrape_pool_targets_added: gv(
                "prometheus_target_scrape_pool_targets",
                "Current number of targets in this scrape pool.",
                &["scrape_job"],
            ),
            target_scrape_pool_symbol_table_items: gv(
                "prometheus_target_scrape_pool_symboltable_items",
                "Current number of symbols in table for this scrape pool.",
                &["scrape_job"],
            ),
            target_scrape_pool_syncs_counter: cv(
                "prometheus_target_scrape_pool_sync_total",
                "Total number of syncs that were executed on a scrape pool.",
                &["scrape_job"],
            ),
            target_sync_interval_length: sv(
                "prometheus_target_sync_length_seconds",
                "Actual interval to sync the scrape pool.",
                &["scrape_job"],
            ),
            target_sync_interval_length_histogram: hv(
                "prometheus_target_sync_length_histogram_seconds",
                "Actual interval to sync the scrape pool.",
                &["scrape_job"],
                &SYNC_BUCKETS,
            ),
            target_sync_failed: cv("prometheus_target_sync_failed_total", "Total number of target sync failures.", &["scrape_job"]),
            target_scrape_exceeded_body_size_limit: c(
                "prometheus_target_scrapes_exceeded_body_size_limit_total",
                "Total number of scrapes that hit the body size limit",
            ),
            target_scrape_cache_flush_forced: c(
                "prometheus_target_scrapes_cache_flush_forced_total",
                "How many times a scrape cache was flushed due to getting big while scrapes are failing.",
            ),
            target_interval_length: sv("prometheus_target_interval_length_seconds", "Actual intervals between scrapes.", &["interval"]),
            target_interval_length_histogram: hv(
                "prometheus_target_interval_length_histogram_seconds",
                "Actual intervals between scrapes.",
                &["interval"],
                &SYNC_BUCKETS,
            ),
            target_scrape_sample_limit: c(
                "prometheus_target_scrapes_exceeded_sample_limit_total",
                "Total number of scrapes that hit the sample limit and were rejected.",
            ),
            target_scrape_sample_duplicate: c(
                "prometheus_target_scrapes_sample_duplicate_timestamp_total",
                "Total number of samples rejected due to duplicate timestamps but different values.",
            ),
            target_scrape_sample_out_of_order: c(
                "prometheus_target_scrapes_sample_out_of_order_total",
                "Total number of samples rejected due to not being out of the expected order.",
            ),
            target_scrape_sample_out_of_bounds: c(
                "prometheus_target_scrapes_sample_out_of_bounds_total",
                "Total number of samples rejected due to timestamp falling outside of the time bounds.",
            ),
            target_scrape_pool_exceeded_label_limits: c(
                "prometheus_target_scrape_pool_exceeded_label_limits_total",
                "Total number of times scrape pools hit the label limits, during sync or config reload.",
            ),
            target_scrape_native_histogram_bucket_limit: c(
                "prometheus_target_scrapes_exceeded_native_histogram_bucket_limit_total",
                "Total number of scrapes that hit the native histogram bucket limit and were rejected.",
            ),
            target_scrape_exemplar_out_of_order: c(
                "prometheus_target_scrapes_exemplar_out_of_order_total",
                "Total number of exemplar rejected due to not being out of the expected order.",
            ),
            target_scrape_duration: {
                let v = hv(
                    "prometheus_target_scrape_duration_seconds",
                    "Total duration of the scrape from start to commit completion in seconds.",
                    &[],
                    &DEFAULT_BUCKETS,
                );
                v.get();
                v
            },
        };
        if let Some(reg) = &reg {
            for col in sm.collectors() {
                reg.register(col).map_err(|e| format!("failed to register scrape metrics: {e}"))?;
            }
        }
        Ok(Arc::new(sm))
    }

    fn collectors(&self) -> Vec<Arc<dyn Collector>> {
        vec![
            self.target_metadata_cache.clone(),
            self.target_scrape_pools.clone(),
            self.target_scrape_pools_failed.clone(),
            self.target_reload_interval_length.clone(),
            self.target_scrape_pool_reloads.clone(),
            self.target_scrape_pool_reloads_failed.clone(),
            self.target_sync_interval_length.clone(),
            self.target_sync_interval_length_histogram.clone(),
            self.target_scrape_pool_syncs_counter.clone(),
            self.target_scrape_pool_exceeded_target_limit.clone(),
            self.target_scrape_pool_target_limit.clone(),
            self.target_scrape_pool_targets_added.clone(),
            self.target_scrape_pool_symbol_table_items.clone(),
            self.target_sync_failed.clone(),
            self.target_scrape_exceeded_body_size_limit.clone(),
            self.target_scrape_cache_flush_forced.clone(),
            self.target_interval_length.clone(),
            self.target_interval_length_histogram.clone(),
            self.target_scrape_sample_limit.clone(),
            self.target_scrape_sample_duplicate.clone(),
            self.target_scrape_sample_out_of_order.clone(),
            self.target_scrape_sample_out_of_bounds.clone(),
            self.target_scrape_exemplar_out_of_order.clone(),
            self.target_scrape_pool_exceeded_label_limits.clone(),
            self.target_scrape_native_histogram_bucket_limit.clone(),
            self.target_scrape_duration.clone(),
        ]
    }

    /// Go `setTargetMetadataCacheGatherer`.
    pub fn set_target_metadata_cache_gatherer(&self, g: Weak<dyn TargetsGatherer>) {
        *self.target_metadata_cache.targets_gatherer.lock().unwrap() = Some(g);
    }

    /// Go `Unregister`.
    pub fn unregister(&self) {
        if let Some(reg) = &self.reg {
            for c in self.collectors() {
                reg.unregister(&c);
            }
        }
    }
}
