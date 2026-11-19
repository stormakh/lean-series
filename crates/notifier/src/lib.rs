//! Port of Prometheus `notifier/`: sends alerts to Alertmanagers.
//!
//! ```ignore
//! let m = Arc::new(notifier::Manager::new(notifier::Options { queue_capacity: 10_000, registerer: Some(reg), ..Default::default() }, ValidationScheme::Utf8, Some(logger)));
//! m.apply_config(&cfg)?;                                     // on every config (re)load
//! let m2 = m.clone();
//! std::thread::spawn(move || m2.run(Some(Box::new(notifier_discovery.sync_ch()))));
//! m.send(alerts);                                            // from rule evaluation
//! m.alertmanagers(); m.dropped_alertmanagers();              // /api/v1/alertmanagers
//! m.stop();                                                  // run() returns after the send loops stopped
//! ```

pub mod alert;
pub mod alertmanager;
pub mod alertmanager_set;
pub mod manager;
pub mod metric;
pub mod send_loop;
pub mod util;

pub use alert::{relabel_alerts, Alert, Time};
pub use alertmanager::{alertmanager_from_group, post_path, Alertmanager};
pub use alertmanager_set::AlertmanagerSet;
pub use manager::{Manager, Options, TargetSetsRecv, DEFAULT_MAX_BATCH_SIZE};
pub use metric::AlertMetrics;
pub use send_loop::{Context, DoFn, SendLoop};
