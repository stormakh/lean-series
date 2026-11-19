//! Port of `notifier/manager.go` (work in progress: API only).

use crate::alert::Alert;

/// Manager is responsible for dispatching alert notifications to an alert manager service.
#[derive(Default)]
pub struct Manager {}

impl Manager {
    /// Go `Send`: queues the given alerts for processing.
    pub fn send(&self, alerts: Vec<Alert>) {
        let _ = alerts;
    }
}
