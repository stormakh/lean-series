//! Port of `model/exemplar`.

use crate::labels::{self, Labels};

/// Maximum combined length (in UTF-8 characters) of an exemplar's label names and values (OpenMetrics).
pub const EXEMPLAR_MAX_LABEL_SET_LENGTH: usize = 128;

/// Additional information associated with a time series.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Exemplar {
    pub labels: Labels,
    pub value: f64,
    pub ts: i64,
    pub has_ts: bool,
}

/// The exemplars of one series.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryResult {
    pub series_labels: Labels,
    pub exemplars: Vec<Exemplar>,
}

impl Exemplar {
    /// Whether `self` is the same exemplar as `e2`. Timestamps are ignored when neither exemplar has one.
    pub fn equals(&self, e2: &Exemplar) -> bool {
        if !labels::equal(&self.labels, &e2.labels) {
            return false;
        }
        if (self.has_ts || e2.has_ts) && self.ts != e2.ts {
            return false;
        }
        self.value == e2.value
    }
}

/// Compares timestamps, then values, then labels.
pub fn compare(a: &Exemplar, b: &Exemplar) -> i32 {
    if a.ts < b.ts {
        return -1;
    } else if a.ts > b.ts {
        return 1;
    }
    if a.value < b.value {
        return -1;
    } else if a.value > b.value {
        return 1;
    }
    labels::compare(&a.labels, &b.labels)
}
