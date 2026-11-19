//! Port of `otlp_to_openmetrics_metadata.go`.

use model::metadata::MetricType;

use super::super::pdata::{AggregationTemporality, Metric, MetricData};

/// Go `otelMetricTypeToPromMetricType`. Delta sums and histograms are `Unknown`: there is no delta type yet
/// (proposal https://github.com/prometheus/proposals/pull/48).
pub fn otel_metric_type_to_prom_metric_type(m: &Metric) -> MetricType {
    match &m.data {
        MetricData::Gauge(_) => MetricType::Gauge,
        MetricData::Sum(s) => {
            if s.aggregation_temporality == AggregationTemporality::DELTA {
                MetricType::Unknown
            } else if s.is_monotonic {
                MetricType::Counter
            } else {
                MetricType::Gauge
            }
        }
        MetricData::Histogram(h) => {
            if h.aggregation_temporality == AggregationTemporality::DELTA {
                MetricType::Unknown
            } else {
                MetricType::Histogram
            }
        }
        MetricData::Summary(_) => MetricType::Summary,
        MetricData::ExponentialHistogram(h) => {
            if h.aggregation_temporality == AggregationTemporality::DELTA {
                MetricType::Unknown
            } else {
                MetricType::Histogram
            }
        }
        MetricData::Empty => MetricType::Unknown,
    }
}
