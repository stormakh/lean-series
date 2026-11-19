//! Port of `prompb/codec.go`: conversions between remote write 1.0 messages and the model types.
//! The histogram conversions are shared with remote write 2.0 (same fields).

use std::sync::Arc;

use model::exemplar::Exemplar as ModelExemplar;
use model::histogram::{CounterResetHint, FloatHistogram, Histogram as ModelHistogram, Span};
use model::labels::{Labels as ModelLabels, ScratchBuilder};
use model::metadata::MetricType as ModelMetricType;

use super::types::*;

fn label_protos_to_labels(b: &mut ScratchBuilder, pairs: &[Label]) -> ModelLabels {
    b.reset();
    for l in pairs {
        b.add(&l.name, &l.value);
    }
    b.sort();
    b.labels()
}

impl TimeSeries {
    /// Go `TimeSeries.ToLabels`.
    pub fn to_labels(&self, b: &mut ScratchBuilder) -> ModelLabels {
        label_protos_to_labels(b, &self.labels)
    }
}

impl ChunkedSeries {
    /// Go `ChunkedSeries.ToLabels`.
    pub fn to_labels(&self, b: &mut ScratchBuilder) -> ModelLabels {
        label_protos_to_labels(b, &self.labels)
    }
}

impl Exemplar {
    /// Go `Exemplar.ToExemplar`.
    pub fn to_exemplar(&self, b: &mut ScratchBuilder) -> ModelExemplar {
        ModelExemplar {
            labels: label_protos_to_labels(b, &self.labels),
            value: self.value,
            ts: self.timestamp,
            has_ts: self.timestamp != 0,
        }
    }
}

/// Go `FromLabels`: fills `buf` (cleared first) with the labels.
pub fn from_labels(lbls: &ModelLabels, buf: &mut Vec<Label>) {
    buf.clear();
    for l in lbls.iter() {
        buf.push(Label { name: l.0.to_string(), value: l.1.to_string() });
    }
}

/// Go `FromLabels(lbls, nil)`.
pub fn labels_to_label_protos(lbls: &ModelLabels) -> Vec<Label> {
    let mut v = Vec::with_capacity(lbls.len());
    from_labels(lbls, &mut v);
    v
}

/// Go `FromMetadataType`.
pub fn from_metadata_type(t: ModelMetricType) -> MetricType {
    from_metadata_type_str(t.as_str())
}

/// Go `FromMetadataType` on a raw `model.MetricType` string (Go's type is a string, so any text is allowed).
pub fn from_metadata_type_str(t: &str) -> MetricType {
    MetricType::from_name(&t.to_uppercase()).unwrap_or(MetricType::UNKNOWN)
}

fn reset_hint_to_model(h: ResetHint) -> CounterResetHint {
    CounterResetHint::from_u8(h.0 as u8)
}

fn spans_proto_to_spans(s: &[BucketSpan]) -> Vec<Span> {
    s.iter().map(|s| Span { offset: s.offset, length: s.length }).collect()
}

fn spans_to_spans_proto(s: &[Span]) -> Vec<BucketSpan> {
    s.iter().map(|s| BucketSpan { offset: s.offset, length: s.length }).collect()
}

fn deltas_to_counts(deltas: &[i64]) -> Vec<f64> {
    let mut cur = 0.0;
    deltas
        .iter()
        .map(|&d| {
            cur += d as f64;
            cur
        })
        .collect()
}

fn custom_values(cv: &[f64]) -> Option<Arc<[f64]>> {
    if cv.is_empty() { None } else { Some(Arc::from(cv)) }
}

impl Histogram {
    /// Go `IsFloatHistogram`.
    pub fn is_float_histogram(&self) -> bool {
        matches!(self.count, Some(Count::Float(_)))
    }

    /// Go `GetResetHint`.
    pub fn get_reset_hint(&self) -> ResetHint {
        self.reset_hint
    }

    /// Go `ToIntHistogram`: None for a float histogram.
    pub fn to_int_histogram(&self) -> Option<ModelHistogram> {
        if self.is_float_histogram() {
            return None;
        }
        Some(ModelHistogram {
            counter_reset_hint: reset_hint_to_model(self.reset_hint),
            schema: self.schema,
            zero_threshold: self.zero_threshold,
            zero_count: self.get_zero_count_int(),
            count: self.get_count_int(),
            sum: self.sum,
            positive_spans: spans_proto_to_spans(&self.positive_spans),
            positive_buckets: self.positive_deltas.clone(),
            negative_spans: spans_proto_to_spans(&self.negative_spans),
            negative_buckets: self.negative_deltas.clone(),
            custom_values: custom_values(&self.custom_values),
        })
    }

    /// Go `ToFloatHistogram`: converts an integer histogram.
    pub fn to_float_histogram(&self) -> FloatHistogram {
        if self.is_float_histogram() {
            return FloatHistogram {
                counter_reset_hint: reset_hint_to_model(self.reset_hint),
                schema: self.schema,
                zero_threshold: self.zero_threshold,
                zero_count: self.get_zero_count_float(),
                count: self.get_count_float(),
                sum: self.sum,
                positive_spans: spans_proto_to_spans(&self.positive_spans),
                positive_buckets: self.positive_counts.clone(),
                negative_spans: spans_proto_to_spans(&self.negative_spans),
                negative_buckets: self.negative_counts.clone(),
                custom_values: custom_values(&self.custom_values),
            };
        }
        FloatHistogram {
            counter_reset_hint: reset_hint_to_model(self.reset_hint),
            schema: self.schema,
            zero_threshold: self.zero_threshold,
            zero_count: self.get_zero_count_int() as f64,
            count: self.get_count_int() as f64,
            sum: self.sum,
            positive_spans: spans_proto_to_spans(&self.positive_spans),
            positive_buckets: deltas_to_counts(&self.positive_deltas),
            negative_spans: spans_proto_to_spans(&self.negative_spans),
            negative_buckets: deltas_to_counts(&self.negative_deltas),
            custom_values: custom_values(&self.custom_values),
        }
    }
}

/// Go `FromIntHistogram`.
pub fn from_int_histogram(timestamp: i64, h: &ModelHistogram) -> Histogram {
    Histogram {
        count: Some(Count::Int(h.count)),
        sum: h.sum,
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        zero_count: Some(Count::Int(h.zero_count)),
        negative_spans: spans_to_spans_proto(&h.negative_spans),
        negative_deltas: h.negative_buckets.clone(),
        positive_spans: spans_to_spans_proto(&h.positive_spans),
        positive_deltas: h.positive_buckets.clone(),
        reset_hint: ResetHint(h.counter_reset_hint as i32),
        timestamp,
        custom_values: h.custom_values.as_deref().map(|c| c.to_vec()).unwrap_or_default(),
        ..Default::default()
    }
}

/// Go `FromFloatHistogram`.
pub fn from_float_histogram(timestamp: i64, fh: &FloatHistogram) -> Histogram {
    Histogram {
        count: Some(Count::Float(fh.count)),
        sum: fh.sum,
        schema: fh.schema,
        zero_threshold: fh.zero_threshold,
        zero_count: Some(Count::Float(fh.zero_count)),
        negative_spans: spans_to_spans_proto(&fh.negative_spans),
        negative_counts: fh.negative_buckets.clone(),
        positive_spans: spans_to_spans_proto(&fh.positive_spans),
        positive_counts: fh.positive_buckets.clone(),
        reset_hint: ResetHint(fh.counter_reset_hint as i32),
        timestamp,
        custom_values: fh.custom_values.as_deref().map(|c| c.to_vec()).unwrap_or_default(),
        ..Default::default()
    }
}
