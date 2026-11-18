//! Circular in-memory exemplar storage (port of `tsdb/exemplar.go`).
//!
//! API fixed by the lead (the head uses it); the bodies are filled by the exemplar port.

use std::sync::Arc;

use model::exemplar::{Exemplar, QueryResult};
use model::labels::Labels;
use model::matcher::Matcher;
use storage::{Context, Error, ExemplarQuerier};

use crate::metrics::{Counter, Gauge};

/// Go `ExemplarMetrics`.
#[derive(Debug, Default)]
pub struct ExemplarMetrics {
    /// `prometheus_tsdb_exemplar_exemplars_appended_total`.
    pub exemplars_appended: Counter,
    /// `prometheus_tsdb_exemplar_exemplars_in_storage`.
    pub exemplars_in_storage: Gauge,
    /// `prometheus_tsdb_exemplar_series_with_exemplars_in_storage`.
    pub series_with_exemplars_in_storage: Gauge,
    /// `prometheus_tsdb_exemplar_last_exemplars_timestamp_seconds`.
    pub last_exemplars_ts: Gauge,
    /// `prometheus_tsdb_exemplar_max_exemplars`.
    pub max_exemplars: Gauge,
    /// `prometheus_tsdb_exemplar_out_of_order_exemplars_total`.
    pub out_of_order_exemplars: Counter,
}

impl ExemplarMetrics {
    /// Go `NewExemplarMetrics`.
    pub fn new() -> Arc<ExemplarMetrics> {
        Arc::new(ExemplarMetrics::default())
    }
}

/// Go `CircularExemplarStorage`. Thread-safe (interior lock). The buffer grows with demand up to its length
/// instead of being preallocated.
pub struct CircularExemplarStorage {
    _todo: (),
}

impl CircularExemplarStorage {
    /// Go `NewCircularExemplarStorage`: `length <= 0` gives a storage that keeps nothing until resized;
    /// `ooo_time_window_millis <= 0` disables out-of-order exemplars.
    pub fn new(
        length: i64,
        metrics: Arc<ExemplarMetrics>,
        ooo_time_window_millis: i64,
    ) -> Result<Arc<CircularExemplarStorage>, Error> {
        let _ = (length, metrics, ooo_time_window_millis);
        todo!("exemplar port")
    }

    /// Go `Select`: the exemplars in [start, end] of the series that match some matcher set.
    pub fn select(&self, start: i64, end: i64, matchers: &[Vec<Matcher>]) -> Result<Vec<QueryResult>, Error> {
        let _ = (start, end, matchers);
        todo!("exemplar port")
    }

    /// Go `ValidateExemplar`.
    pub fn validate_exemplar(&self, l: &Labels, e: &Exemplar) -> Result<(), Error> {
        let _ = (l, e);
        todo!("exemplar port")
    }

    /// Go `AddExemplar`.
    pub fn add_exemplar(&self, l: &Labels, e: &Exemplar) -> Result<(), Error> {
        let _ = (l, e);
        todo!("exemplar port")
    }

    /// Go `IterateExemplars`: calls `f` for every exemplar, oldest first.
    pub fn iterate_exemplars(
        &self,
        f: &mut dyn FnMut(&Labels, &Exemplar) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let _ = f;
        todo!("exemplar port")
    }

    /// Go `Resize`: sets the length; returns the number of exemplars migrated.
    pub fn resize(&self, l: i64) -> i32 {
        let _ = l;
        todo!("exemplar port")
    }

    /// Go `SetOutOfOrderTimeWindow`.
    pub fn set_out_of_order_time_window(&self, d: i64) {
        let _ = d;
        todo!("exemplar port")
    }

    /// Go `ExemplarQuerier(ctx)`: a querier over this storage.
    pub fn exemplar_querier(self: &Arc<Self>, _ctx: &Context) -> Result<Box<dyn ExemplarQuerier>, Error> {
        Ok(Box::new(CircularExemplarQuerier(self.clone())))
    }
}

/// The querier over the storage (Go returns the storage itself).
pub struct CircularExemplarQuerier(pub Arc<CircularExemplarStorage>);

impl ExemplarQuerier for CircularExemplarQuerier {
    fn select(&self, start: i64, end: i64, matchers: &[Vec<Matcher>]) -> Result<Vec<QueryResult>, Error> {
        self.0.select(start, end, matchers)
    }
}
