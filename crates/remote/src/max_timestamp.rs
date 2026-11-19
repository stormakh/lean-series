//! Port of `storage/remote/max_timestamp.go`: a gauge that only goes up.

use std::sync::{Arc, Mutex};

use discovery::metrics::{Collector, GaugeVec, MetricVec, Opts, Value};

/// Go `maxTimestamp`: a gauge whose value is only ever raised.
pub struct MaxTimestamp {
    value: Mutex<f64>,
    gauge: Arc<GaugeVec>,
    g: Arc<Value>,
}

impl MaxTimestamp {
    /// A max timestamp backed by a new gauge.
    pub fn new(opts: Opts) -> MaxTimestamp {
        let gauge = MetricVec::new_gauge(opts);
        let g = gauge.get();
        MaxTimestamp { value: Mutex::new(0.0), gauge, g }
    }

    /// Raises the value (and the gauge) when `value` is higher.
    pub fn set(&self, value: f64) {
        let mut v = self.value.lock().unwrap_or_else(|e| e.into_inner());
        if value > *v {
            *v = value;
            self.g.set(value);
        }
    }

    pub fn get(&self) -> f64 {
        *self.value.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Overwrites the stored value without touching the gauge (Go tests write the `value` field directly).
    pub fn force_value(&self, value: f64) {
        *self.value.lock().unwrap_or_else(|e| e.into_inner()) = value;
    }

    /// The gauge, for registration.
    pub fn collector(&self) -> Arc<dyn Collector> {
        self.gauge.clone()
    }

    /// The exported gauge value (Go `testutil.ToFloat64`).
    pub fn gauge_value(&self) -> f64 {
        self.g.get()
    }
}
