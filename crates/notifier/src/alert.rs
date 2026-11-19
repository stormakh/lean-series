//! Port of `notifier/alert.go`.

use std::sync::Arc;
use std::time::SystemTime;

use model::labels::{self, Builder, Labels};
use model::relabel;

/// A point in time like Go `time.Time`; `None` is the zero time (`IsZero()`).
pub type Time = Option<SystemTime>;

/// Alert is a generic representation of an alert in the Prometheus eco-system.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Alert {
    /// Label value pairs for purpose of aggregation, matching, and disposition dispatching.
    /// This must minimally include an "alertname" label.
    pub labels: Labels,
    /// Extra key/value information which does not define alert identity.
    pub annotations: Labels,
    /// The known time range for this alert. Both ends are optional (`None` is Go's zero time).
    pub starts_at: Time,
    pub ends_at: Time,
    pub generator_url: String,
}

impl Alert {
    /// Returns the name of the alert. It is equivalent to the "alertname" label.
    pub fn name(&self) -> &str {
        self.labels.get(labels::ALERT_NAME)
    }

    /// Returns a hash over the alert. It is equivalent to the alert labels hash.
    pub fn hash(&self) -> u64 {
        self.labels.hash()
    }

    /// Go `String()`: `name[hash7][resolved|active]`.
    pub fn string(&self) -> String {
        let h = format!("{:016x}", self.hash());
        let s = format!("{}[{}]", self.name(), &h[..7]);
        if self.resolved() { s + "[resolved]" } else { s + "[active]" }
    }

    /// Returns true iff the activity interval ended in the past.
    pub fn resolved(&self) -> bool {
        self.resolved_at(SystemTime::now())
    }

    /// Returns true iff the activity interval ended before the given timestamp.
    pub fn resolved_at(&self, ts: SystemTime) -> bool {
        match self.ends_at {
            None => false,
            Some(e) => e <= ts,
        }
    }
}

impl std::fmt::Display for Alert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.string())
    }
}

/// Go `relabelAlerts`: adds the external labels that the alert does not set, then applies the relabel configs.
/// Alerts whose labels change are copied; unchanged alerts keep their `Arc`.
pub fn relabel_alerts(relabel_configs: &[relabel::Config], external_labels: &Labels, alerts: &[Arc<Alert>]) -> Vec<Arc<Alert>> {
    let mut lb = Builder::new(Labels::empty());
    let mut out = Vec::with_capacity(alerts.len());
    for a in alerts {
        lb.reset(a.labels.clone());
        for (n, v) in external_labels.iter() {
            if a.labels.get(n).is_empty() {
                lb.set(n, v);
            }
        }
        if !relabel::process_builder(&mut lb, relabel_configs) {
            continue;
        }
        let ls = lb.labels();
        if a.labels != ls {
            out.push(Arc::new(Alert {
                labels: ls,
                annotations: a.annotations.clone(),
                starts_at: a.starts_at,
                ends_at: a.ends_at,
                generator_url: a.generator_url.clone(),
            }));
        } else {
            out.push(a.clone());
        }
    }
    out
}
