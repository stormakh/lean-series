//! The value types of `github.com/prometheus/common/model` that the Prometheus HTTP API client returns
//! (`model.Value`, `model.Matrix`, `model.Vector`, ...). Times are milliseconds (Go `model.Time`).

use std::collections::BTreeMap;

/// Go `model.LabelSet` / `model.Metric` (a map; Go sorts it when printing).
pub type LabelSet = BTreeMap<String, String>;

/// Go `model.SamplePair`.
#[derive(Clone, Debug, PartialEq)]
pub struct SamplePair {
    pub timestamp: i64,
    pub value: f64,
}

/// Go `model.HistogramBucket`.
#[derive(Clone, Debug, PartialEq)]
pub struct HistogramBucket {
    pub boundaries: i32,
    pub lower: f64,
    pub upper: f64,
    pub count: f64,
}

/// Go `model.SampleHistogram`.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct SampleHistogram {
    pub count: f64,
    pub sum: f64,
    pub buckets: Vec<HistogramBucket>,
}

/// Go `model.SampleHistogramPair`.
#[derive(Clone, Debug, PartialEq)]
pub struct SampleHistogramPair {
    pub timestamp: i64,
    pub histogram: SampleHistogram,
}

/// Go `model.SampleStream`.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct SampleStream {
    pub metric: LabelSet,
    pub values: Vec<SamplePair>,
    pub histograms: Vec<SampleHistogramPair>,
}

/// Go `model.Sample`.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Sample {
    pub metric: LabelSet,
    pub value: f64,
    pub timestamp: i64,
    pub histogram: Option<SampleHistogram>,
}

/// Go `model.Scalar`.
#[derive(Clone, Debug, PartialEq)]
pub struct Scalar {
    pub value: f64,
    pub timestamp: i64,
}

/// Go `model.String`.
#[derive(Clone, Debug, PartialEq)]
pub struct StringValue {
    pub value: String,
    pub timestamp: i64,
}

pub type Matrix = Vec<SampleStream>;
pub type Vector = Vec<Sample>;

/// Go `model.Value`.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Scalar(Scalar),
    Vector(Vector),
    Matrix(Matrix),
    String(StringValue),
}

impl Value {
    /// Go `Value.Type().String()`.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Scalar(_) => "scalar",
            Value::Vector(_) => "vector",
            Value::Matrix(_) => "matrix",
            Value::String(_) => "string",
        }
    }
}
