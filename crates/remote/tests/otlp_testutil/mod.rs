//! Port of `storage/remote/otlptranslator/prometheusremotewrite/testutil_test.go` and small shared helpers of
//! the OTLP translator tests.
#![allow(dead_code)]

use std::time::{SystemTime, UNIX_EPOCH};

use remote::otlptranslator::pdata::{
    AggregationTemporality, AppendEmpty, DataPointFlags, Map, Metric, Timestamp,
};
use remote::otlptranslator::prometheusremotewrite::{Scope, WarningCategory, warning_category_of};
use storage::Annotations;

/// Go `collisionWarnings`: the label name collision warnings of `annots`.
pub fn collision_warnings(annots: &Annotations) -> Vec<String> {
    annots
        .iter()
        .filter(|e| warning_category_of(e) == WarningCategory::LabelNameCollision)
        .map(|e| e.to_string())
        .collect()
}

/// Go `getIntGaugeMetric`.
pub fn get_int_gauge_metric(name: &str, attributes: &Map, value: i64, ts: u64) -> Metric {
    let mut metric = Metric::new();
    metric.name = name.to_string();
    let dp = metric.set_empty_gauge().data_points.append_empty();
    if name.starts_with("staleNaN") {
        dp.flags = DataPointFlags::DEFAULT.with_no_recorded_value(true);
    }
    dp.set_int_value(value);
    attributes.copy_to(&mut dp.attributes);
    dp.start_timestamp = 0;
    dp.timestamp = ts;
    metric
}

/// Go `getIntSumMetric`.
pub fn get_int_sum_metric(name: &str, attributes: &Map, value: i64, ts: u64) -> Metric {
    let mut metric = Metric::new();
    metric.name = name.to_string();
    metric.set_empty_sum().aggregation_temporality = AggregationTemporality::CUMULATIVE;
    let dp = metric.sum_mut().data_points.append_empty();
    if name.starts_with("staleNaN") {
        dp.flags = DataPointFlags::DEFAULT.with_no_recorded_value(true);
    }
    dp.set_int_value(value);
    attributes.copy_to(&mut dp.attributes);
    dp.start_timestamp = 0;
    dp.timestamp = ts;
    metric
}

/// The `defaultScope` of several tests.
pub fn default_scope() -> Scope {
    Scope {
        name: "test-scope".into(),
        version: "1.0.0".into(),
        schema_url: "https://schema.com".into(),
        attributes: Map::from_pairs(&[("attr1", "value1"), ("attr2", "value2")]),
    }
}

/// Go `time.Now().UnixNano()` as a pdata timestamp.
pub fn now_nanos() -> Timestamp {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// Go `labels.FromStrings`.
pub fn ls(pairs: &[&str]) -> model::labels::Labels {
    model::labels::from_strings(pairs)
}
