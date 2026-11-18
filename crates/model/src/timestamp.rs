//! Port of `model/timestamp`: millisecond timestamps.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Returns a millisecond timestamp from a time (Go `t.UnixMilli()`, which rounds towards negative infinity).
pub fn from_time(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as i64) * 1000 + (d.subsec_nanos() / 1_000_000) as i64,
        Err(e) => {
            // Before the epoch: Go keeps a non-negative nanosecond part and floors the seconds.
            let d = e.duration();
            let secs = -(d.as_secs() as i64);
            let nanos = d.subsec_nanos() as i64;
            if nanos == 0 { secs * 1000 } else { (secs - 1) * 1000 + (1_000_000_000 - nanos) / 1_000_000 }
        }
    }
}

/// Returns the time for a millisecond timestamp.
pub fn time(ts: i64) -> SystemTime {
    if ts >= 0 {
        UNIX_EPOCH + Duration::from_millis(ts as u64)
    } else {
        UNIX_EPOCH - Duration::from_millis(ts.unsigned_abs())
    }
}

/// Returns a millisecond timestamp from float seconds (Go `int64(math.Round(ts * 1000))`).
pub fn from_float_seconds(ts: f64) -> i64 {
    (ts * 1000.0).round() as i64
}

/// The current time as a millisecond timestamp.
pub fn now() -> i64 {
    from_time(SystemTime::now())
}
