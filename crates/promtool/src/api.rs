//! A port of the parts of `github.com/prometheus/client_golang/api` and `api/prometheus/v1` that promtool uses.
//!
//! Times are [`Time`]: nanoseconds since the Unix epoch (Go `time.Time` in UTC).

use std::time::Duration;

use crate::client_model::Value;

/// Nanoseconds since the Unix epoch (Go `time.Time`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Time(pub i64);

impl Time {
    pub fn now() -> Time {
        let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
        Time(d.as_nanos() as i64)
    }
    pub fn from_unix(sec: i64, nsec: i64) -> Time {
        Time(sec * 1_000_000_000 + nsec)
    }
    /// Go `t.Unix()`.
    pub fn unix(self) -> i64 {
        self.0.div_euclid(1_000_000_000)
    }
    /// Go `t.Nanosecond()`.
    pub fn nanosecond(self) -> i64 {
        self.0.rem_euclid(1_000_000_000)
    }
    /// Go `t.UnixMilli()` / `timestamp.FromTime(t)`.
    pub fn unix_milli(self) -> i64 {
        self.0.div_euclid(1_000_000)
    }
    pub fn add(self, d: Duration) -> Time {
        Time(self.0 + d.as_nanos() as i64)
    }
    pub fn sub_dur(self, d: Duration) -> Time {
        Time(self.0 - d.as_nanos() as i64)
    }
}

/// Go `v1.Range`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Range {
    pub start: Time,
    pub end: Time,
    pub step: Duration,
}

/// The result of a query: the value, warnings and infos (Go `(model.Value, v1.Warnings, v1.Infos, error)`).
pub type QueryResult = Result<(Value, Vec<String>, Vec<String>), crate::Error>;

/// Go `queryRangeAPI` of rules.go: what the rule importer needs from the API client.
pub trait QueryRangeApi {
    fn query_range(&self, query: &str, r: Range) -> QueryResult;
}
