//! Port of `promql/quantile.go`: quantiles of classic and native histograms.
//!
//! STUB: being ported.

use model::labels::Labels;

/// A classic histogram bucket (Go `Bucket`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Bucket {
    pub upper_bound: f64,
    pub count: f64,
}

/// Go `Buckets`.
pub type Buckets = Vec<Bucket>;

/// A classic histogram: the metric without `le` and its buckets (Go `metricWithBuckets`).
#[derive(Clone, Debug, Default)]
pub struct MetricWithBuckets {
    pub metric: Labels,
    pub buckets: Buckets,
}

/// Go `quantile`: the q-quantile of the values (used by the `quantile` aggregation). May reorder `values`.
pub fn quantile(_q: f64, _values: &mut [f64]) -> f64 {
    unimplemented!("quantile")
}
