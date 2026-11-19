//! Port of `storage/remote/ewma.go`: an exponentially weighted moving average of an event rate.

use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

/// Go `ewmaRate`: tracks an exponentially weighted moving average of a per-second rate.
#[derive(Debug)]
pub struct EwmaRate {
    /// Events since the last `tick` (Go `newEvents`).
    pub new_events: AtomicI64,
    alpha: f64,
    interval: Duration,
    /// Go `lastRate` and `init`, under Go's `mutex`.
    pub state: Mutex<EwmaState>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct EwmaState {
    pub last_rate: f64,
    pub init: bool,
}

impl EwmaRate {
    /// Go `newEWMARate`.
    pub fn new(alpha: f64, interval: Duration) -> EwmaRate {
        EwmaRate { new_events: AtomicI64::new(0), alpha, interval, state: Mutex::new(EwmaState::default()) }
    }

    /// The current rate.
    pub fn rate(&self) -> f64 {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).last_rate
    }

    /// Updates the rate; call it every `interval`.
    pub fn tick(&self) {
        let new_events = self.new_events.swap(0, Ordering::Relaxed);
        let instant_rate = new_events as f64 / self.interval.as_secs_f64();

        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if s.init {
            s.last_rate += self.alpha * (instant_rate - s.last_rate);
        } else if new_events > 0 {
            s.init = true;
            s.last_rate = instant_rate;
        }
    }

    /// Counts `incr` new events.
    #[inline]
    pub fn incr(&self, incr: i64) {
        self.new_events.fetch_add(incr, Ordering::Relaxed);
    }
}
