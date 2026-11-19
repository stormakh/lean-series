//! Process-wide families that Go's main and packages register: `prometheus_build_info` (version collector),
//! the notification metrics (`util/notifications`) and the template expansion counters (`template`).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use discovery::metrics::{Collector, Family, Metric, MetricType, MetricValue};

fn one(name: &str, help: &str, typ: MetricType, labels: Vec<(String, String)>, value: MetricValue) -> Family {
    Family { name: name.to_string(), help: help.to_string(), typ, metrics: vec![Metric { labels, value }] }
}

/// Go `versioncollector.NewCollector("prometheus")`.
pub struct BuildInfo;

impl Collector for BuildInfo {
    fn describe(&self) -> Vec<String> {
        vec!["prometheus_build_info".to_string()]
    }
    fn collect(&self, out: &mut Vec<Family>) {
        let mut labels: Vec<(String, String)> = [
            ("branch", crate::flags::BRANCH),
            ("goarch", "amd64"),
            ("goos", "linux"),
            ("goversion", crate::flags::GO_VERSION),
            ("revision", crate::flags::REVISION),
            ("tags", crate::flags::TAGS),
            ("version", crate::flags::VERSION),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        labels.sort();
        out.push(one(
            "prometheus_build_info",
            "A metric with a constant '1' value labeled by version, revision, branch, goversion from which prometheus was built, and the goos and goarch for the build.",
            MetricType::Gauge,
            labels,
            MetricValue::Gauge(1.0),
        ));
    }
}

/// Go `notifications.NewNotifications(..., prometheus.DefaultRegisterer)`.
pub struct NotificationMetrics(pub Arc<web::notifications::Notifications>);

impl Collector for NotificationMetrics {
    fn describe(&self) -> Vec<String> {
        vec!["prometheus_api_notification_active_subscribers".to_string()]
    }
    fn collect(&self, out: &mut Vec<Family>) {
        let n = &self.0;
        out.push(one(
            "prometheus_api_notification_active_subscribers",
            "The current number of active notification subscribers.",
            MetricType::Gauge,
            Vec::new(),
            MetricValue::Gauge(n.active_subscribers.load(Ordering::Relaxed) as f64),
        ));
        out.push(one(
            "prometheus_api_notification_updates_sent_total",
            "Total number of notification updates sent.",
            MetricType::Counter,
            Vec::new(),
            MetricValue::Counter(n.updates_sent.load(Ordering::Relaxed) as f64),
        ));
        out.push(one(
            "prometheus_api_notification_updates_dropped_total",
            "Total number of notification updates dropped.",
            MetricType::Counter,
            Vec::new(),
            MetricValue::Counter(n.updates_dropped.load(Ordering::Relaxed) as f64),
        ));
    }
}

/// The template package's expansion counters.
pub struct TemplateMetrics;

impl Collector for TemplateMetrics {
    fn describe(&self) -> Vec<String> {
        vec!["prometheus_template_text_expansions_total".to_string()]
    }
    fn collect(&self, out: &mut Vec<Family>) {
        out.push(one(
            "prometheus_template_text_expansion_failures_total",
            "The total number of template text expansion failures.",
            MetricType::Counter,
            Vec::new(),
            MetricValue::Counter(template::text_expansion_failures_total() as f64),
        ));
        out.push(one(
            "prometheus_template_text_expansions_total",
            "The total number of template text expansions.",
            MetricType::Counter,
            Vec::new(),
            MetricValue::Counter(template::text_expansions_total() as f64),
        ));
    }
}

/// Registers the families above and the ZooKeeper tree cache metrics (Go registers those at package init).
pub fn register(reg: &discovery::metrics::Registry, notifs: &Arc<web::notifications::Notifications>) -> Result<(), String> {
    reg.register(Arc::new(BuildInfo))?;
    reg.register(Arc::new(NotificationMetrics(notifs.clone())))?;
    reg.register(Arc::new(TemplateMetrics))?;
    for c in discovery::zookeeper::treecache::treecache_metrics() {
        reg.register(c)?;
    }
    Ok(())
}
