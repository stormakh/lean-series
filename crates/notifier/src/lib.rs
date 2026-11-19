//! Port of Prometheus `notifier/`: sends alerts to Alertmanagers.

pub mod alert;
pub mod manager;

pub use alert::{relabel_alerts, Alert, Time};
pub use manager::Manager;
