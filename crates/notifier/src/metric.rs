//! Port of `notifier/metric.go`: the notifier's metrics, with upstream's names, labels and help texts.

use std::sync::Arc;

use discovery::metrics::{Collector, CounterVec, Family, GaugeVec, HistogramVec, Metric, MetricType, MetricValue, MetricVec, Opts, Registry, SummaryVec};

pub const NAMESPACE: &str = "prometheus";
pub const SUBSYSTEM: &str = "notifications";
pub const ALERTMANAGER_LABEL: &str = "alertmanager";

fn name(n: &str) -> String {
    format!("{NAMESPACE}_{SUBSYSTEM}_{n}")
}

/// A gauge whose value comes from a function (Go `prometheus.NewGaugeFunc`).
pub struct GaugeFunc {
    opts: Opts,
    f: Box<dyn Fn() -> f64 + Send + Sync>,
}

impl GaugeFunc {
    pub fn new(opts: Opts, f: Box<dyn Fn() -> f64 + Send + Sync>) -> GaugeFunc {
        GaugeFunc { opts, f }
    }
    pub fn get(&self) -> f64 {
        (self.f)()
    }
}

impl Collector for GaugeFunc {
    fn describe(&self) -> Vec<String> {
        vec![self.opts.name.clone()]
    }
    fn collect(&self, out: &mut Vec<Family>) {
        out.push(Family {
            name: self.opts.name.clone(),
            help: self.opts.help.clone(),
            typ: MetricType::Gauge,
            metrics: vec![Metric { labels: Vec::new(), value: MetricValue::Gauge(self.get()) }],
        });
    }
}

/// Go `alertMetrics`.
pub struct AlertMetrics {
    /// Count and sum only (the registry has no quantiles).
    pub latency_summary: Arc<SummaryVec>,
    /// Classic buckets only (no native histogram).
    pub latency_histogram: Arc<HistogramVec>,
    pub errors: Arc<CounterVec>,
    pub sent: Arc<CounterVec>,
    pub dropped: Arc<CounterVec>,
    pub queue_length: Arc<GaugeVec>,
    pub queue_capacity: Arc<GaugeVec>,
    pub alertmanagers_discovered: Arc<GaugeFunc>,
}

impl AlertMetrics {
    /// Go `newAlertMetrics`: registers the metrics when `r` is set. A registration error panics, as Go's
    /// `MustRegister` does.
    pub fn new(r: Option<&Arc<Registry>>, alertmanagers_discovered: Box<dyn Fn() -> f64 + Send + Sync>) -> AlertMetrics {
        let l = &[ALERTMANAGER_LABEL];
        let m = AlertMetrics {
            latency_summary: MetricVec::new_summary_vec(Opts::new(&name("latency_seconds"), "Latency quantiles for sending alert notifications."), l),
            latency_histogram: MetricVec::new_histogram_vec(
                Opts::new(&name("latency_histogram_seconds"), "Latency histogram for sending alert notifications."),
                l,
                &[0.01, 0.1, 1.0, 10.0],
            ),
            errors: MetricVec::new_counter_vec(Opts::new(&name("errors_total"), "Total number of sent alerts affected by errors."), l),
            sent: MetricVec::new_counter_vec(Opts::new(&name("sent_total"), "Total number of alerts sent."), l),
            dropped: MetricVec::new_counter_vec(Opts::new(&name("dropped_total"), "Total number of alerts dropped due to errors when sending to Alertmanager."), l),
            queue_length: MetricVec::new_gauge_vec(Opts::new(&name("queue_length"), "The number of alert notifications in the queue."), l),
            queue_capacity: MetricVec::new_gauge(Opts::new(&name("queue_capacity"), "The capacity of the alert notifications queue.")),
            alertmanagers_discovered: Arc::new(GaugeFunc::new(
                Opts::new("prometheus_notifications_alertmanagers_discovered", "The number of alertmanagers discovered and active."),
                alertmanagers_discovered,
            )),
        };
        if let Some(r) = r {
            let cs: [Arc<dyn Collector>; 8] = [
                m.latency_summary.clone(),
                m.latency_histogram.clone(),
                m.errors.clone(),
                m.sent.clone(),
                m.dropped.clone(),
                m.queue_length.clone(),
                m.queue_capacity.clone(),
                m.alertmanagers_discovered.clone(),
            ];
            for c in cs {
                if let Err(e) = r.register(c) {
                    panic!("{e}");
                }
            }
        }
        m
    }
}
