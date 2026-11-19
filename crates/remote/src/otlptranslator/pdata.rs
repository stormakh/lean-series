//! A minimal OTLP metrics data model: the parts of `go.opentelemetry.io/collector/pdata` (`pcommon`, `pmetric`,
//! `pmetricotlp`) that the OTLP translator, the OTLP write handler and their tests use.
//!
//! Go's pdata types are handles to protobuf structs; here they are plain owned structs with public fields, plus
//! a few Go-named helpers (`set_empty_gauge`, `put_str`, `append_empty`, ...) so that code reads like the Go
//! code. Optional proto3 fields (`sum`, `min`, `max` of histograms) are `Option`s (Go `HasSum`, `RemoveSum`).
//!
//! The protobuf codec is in `pdata_proto.rs` and the JSON decoder in `pdata_json.rs`.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// Go `pcommon.Timestamp`: nanoseconds since the Unix epoch.
pub type Timestamp = u64;

/// Go `pcommon.NewTimestampFromTime`.
pub fn new_timestamp_from_time(t: SystemTime) -> Timestamp {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as u64,
        // Go: uint64(t.UnixNano()) of a negative value.
        Err(e) => (-(e.duration().as_nanos() as i128)) as i64 as u64,
    }
}

/// Go `Timestamp.AsTime().UnixMilli()`: the time is `time.Unix(0, int64(ts))`, so milliseconds round down.
pub fn timestamp_unix_milli(ts: Timestamp) -> i64 {
    (ts as i64).div_euclid(1_000_000)
}

/// Go `Timestamp.AsTime()`.
pub fn timestamp_as_time(ts: Timestamp) -> SystemTime {
    let n = ts as i64;
    if n >= 0 {
        UNIX_EPOCH + std::time::Duration::from_nanos(n as u64)
    } else {
        UNIX_EPOCH - std::time::Duration::from_nanos(n.unsigned_abs())
    }
}

/// Go `pcommon.TraceID`.
pub type TraceId = [u8; 16];
/// Go `pcommon.SpanID`.
pub type SpanId = [u8; 8];

/// Go `TraceID.IsEmpty` / `SpanID.IsEmpty`: all bytes are zero.
pub fn id_is_empty(id: &[u8]) -> bool {
    id.iter().all(|&b| b == 0)
}

/// Appends a default element and returns it (Go `Slice.AppendEmpty`).
pub trait AppendEmpty<T> {
    fn append_empty(&mut self) -> &mut T;
}

impl<T: Default> AppendEmpty<T> for Vec<T> {
    fn append_empty(&mut self) -> &mut T {
        self.push(T::default());
        self.last_mut().unwrap()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// pcommon.Value and pcommon.Map

/// Go `pcommon.ValueType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueType {
    Empty,
    Str,
    Int,
    Double,
    Bool,
    Map,
    Slice,
    Bytes,
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ValueType::Empty => "Empty",
            ValueType::Str => "Str",
            ValueType::Int => "Int",
            ValueType::Double => "Double",
            ValueType::Bool => "Bool",
            ValueType::Map => "Map",
            ValueType::Slice => "Slice",
            ValueType::Bytes => "Bytes",
        })
    }
}

/// Go `pcommon.Value` (an OTLP `AnyValue`).
#[derive(Clone, Debug, Default, PartialEq)]
pub enum Value {
    #[default]
    Empty,
    Str(String),
    Bool(bool),
    Int(i64),
    Double(f64),
    Map(Map),
    Slice(Vec<Value>),
    Bytes(Vec<u8>),
}

impl From<&str> for Value {
    fn from(s: &str) -> Value {
        Value::Str(s.to_string())
    }
}
impl From<String> for Value {
    fn from(s: String) -> Value {
        Value::Str(s)
    }
}
impl From<i64> for Value {
    fn from(v: i64) -> Value {
        Value::Int(v)
    }
}
impl From<f64> for Value {
    fn from(v: f64) -> Value {
        Value::Double(v)
    }
}
impl From<bool> for Value {
    fn from(v: bool) -> Value {
        Value::Bool(v)
    }
}

impl Value {
    pub fn typ(&self) -> ValueType {
        match self {
            Value::Empty => ValueType::Empty,
            Value::Str(_) => ValueType::Str,
            Value::Bool(_) => ValueType::Bool,
            Value::Int(_) => ValueType::Int,
            Value::Double(_) => ValueType::Double,
            Value::Map(_) => ValueType::Map,
            Value::Slice(_) => ValueType::Slice,
            Value::Bytes(_) => ValueType::Bytes,
        }
    }

    /// Go `Value.Str` ("" when the value is not a string).
    pub fn str(&self) -> &str {
        match self {
            Value::Str(s) => s,
            _ => "",
        }
    }

    /// Go `Value.AsString`: a string form of any value. Maps and slices are JSON without HTML escaping, bytes are
    /// base64, doubles follow encoding/json's float format with `NaN`, `Infinity` and `-Infinity`.
    pub fn as_string(&self) -> String {
        match self {
            Value::Empty => String::new(),
            Value::Str(s) => s.clone(),
            Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
            Value::Double(f) => float64_as_string(*f),
            Value::Int(i) => i.to_string(),
            Value::Bytes(b) => base64_std(b),
            Value::Map(_) | Value::Slice(_) => {
                let mut out = String::new();
                if write_raw_json(self, &mut out) {
                    out
                } else {
                    // encoding/json fails on NaN and infinities; AsString ignores the error and returns "".
                    String::new()
                }
            }
        }
    }
}

/// pcommon `float64AsString`.
pub fn float64_as_string(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f == f64::INFINITY {
        return "Infinity".to_string();
    }
    if f == f64::NEG_INFINITY {
        return "-Infinity".to_string();
    }
    let mut s = String::new();
    json::write_f64(f, &mut s);
    s
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Go `base64.StdEncoding.EncodeToString`.
pub fn base64_std(b: &[u8]) -> String {
    let mut out = String::with_capacity(b.len().div_ceil(3) * 4);
    for c in b.chunks(3) {
        let n = (c[0] as u32) << 16 | (*c.get(1).unwrap_or(&0) as u32) << 8 | *c.get(2).unwrap_or(&0) as u32;
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if c.len() > 1 { B64[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if c.len() > 2 { B64[n as usize & 63] as char } else { '=' });
    }
    out
}

/// Go `base64.StdEncoding.Decode` (padding required).
pub fn base64_std_decode(s: &[u8]) -> Result<Vec<u8>, String> {
    if s.len() % 4 != 0 {
        return Err(format!("illegal base64 data at input byte {}", s.len() / 4 * 4));
    }
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    for (ci, c) in s.chunks(4).enumerate() {
        let mut n = 0u32;
        let mut pad = 0;
        for (i, &ch) in c.iter().enumerate() {
            let v = match ch {
                b'A'..=b'Z' => ch - b'A',
                b'a'..=b'z' => ch - b'a' + 26,
                b'0'..=b'9' => ch - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                b'=' if i >= 2 && ci * 4 + 4 == s.len() => {
                    pad += 1;
                    0
                }
                _ => return Err(format!("illegal base64 data at input byte {}", ci * 4 + i)),
            };
            if pad > 0 && ch != b'=' {
                return Err(format!("illegal base64 data at input byte {}", ci * 4 + i));
            }
            n = n << 6 | v as u32;
        }
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    Ok(out)
}

/// Writes `AsRaw()` of a value as encoding/json does with HTML escaping off. Returns false on NaN or infinity
/// (encoding/json fails there).
fn write_raw_json(v: &Value, out: &mut String) -> bool {
    match v {
        Value::Empty => out.push_str("null"),
        Value::Str(s) => write_json_str_no_html(s, out),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Int(i) => json::write_i64(*i, out),
        Value::Double(f) => {
            if !f.is_finite() {
                return false;
            }
            json::write_f64(*f, out);
        }
        Value::Bytes(b) => {
            // AsRaw gives []byte, which encoding/json writes as base64; nil (empty) bytes write as null.
            if b.is_empty() {
                out.push_str("null");
            } else {
                out.push('"');
                out.push_str(&base64_std(b));
                out.push('"');
            }
        }
        Value::Map(m) => {
            // A Go map: keys sorted, the last duplicate key wins.
            let mut keys: Vec<(&str, &Value)> = Vec::with_capacity(m.0.len());
            for (k, v) in m.0.iter() {
                if let Some(e) = keys.iter_mut().find(|e| e.0 == k) {
                    e.1 = v;
                } else {
                    keys.push((k, v));
                }
            }
            keys.sort_by(|a, b| a.0.cmp(b.0));
            out.push('{');
            for (i, (k, v)) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_str_no_html(k, out);
                out.push(':');
                if !write_raw_json(v, out) {
                    return false;
                }
            }
            out.push('}');
        }
        Value::Slice(s) => {
            out.push('[');
            for (i, v) in s.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                if !write_raw_json(v, out) {
                    return false;
                }
            }
            out.push(']');
        }
    }
    true
}

/// encoding/json string escaping with `SetEscapeHTML(false)` (U+2028 and U+2029 are still escaped).
fn write_json_str_no_html(s: &str, out: &mut String) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => {
                out.push_str("\\u00");
                out.push(HEX[(c as usize) >> 4] as char);
                out.push(HEX[(c as usize) & 15] as char);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Go `pcommon.Map`: attributes in insertion order. Keys are unique when built with the `put_*` methods; a
/// decoded map keeps duplicates as they came (as Go does).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Map(pub Vec<(String, Value)>);

impl Map {
    /// Go `pcommon.NewMap`.
    pub fn new() -> Map {
        Map(Vec::new())
    }

    /// A map from pairs in order (the closest form of Go's `FromRaw(map[string]any)`).
    pub fn from_pairs<V: Into<Value> + Clone>(pairs: &[(&str, V)]) -> Map {
        let mut m = Map::new();
        for (k, v) in pairs {
            m.put(k, v.clone().into());
        }
        m
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Go `Map.Get`: the first entry with the key.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    /// Go `Map.Put*`: updates the first entry with the key or appends a new one.
    pub fn put(&mut self, key: &str, v: Value) {
        if let Some(e) = self.0.iter_mut().find(|(k, _)| k == key) {
            e.1 = v;
        } else {
            self.0.push((key.to_string(), v));
        }
    }

    pub fn put_str(&mut self, key: &str, v: &str) {
        self.put(key, Value::Str(v.to_string()));
    }
    pub fn put_int(&mut self, key: &str, v: i64) {
        self.put(key, Value::Int(v));
    }
    pub fn put_double(&mut self, key: &str, v: f64) {
        self.put(key, Value::Double(v));
    }
    pub fn put_bool(&mut self, key: &str, v: bool) {
        self.put(key, Value::Bool(v));
    }

    /// Go `Map.Remove`: removes the first entry with the key, moving the last entry into its place.
    pub fn remove(&mut self, key: &str) -> bool {
        match self.0.iter().position(|(k, _)| k == key) {
            Some(i) => {
                self.0.swap_remove(i);
                true
            }
            None => false,
        }
    }

    /// Go `Map.Range` in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Go `Map.CopyTo`: replaces `dest` with a copy of `self`.
    pub fn copy_to(&self, dest: &mut Map) {
        dest.0.clone_from(&self.0);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// pmetric

/// Go `pmetric.AggregationTemporality` (the proto enum; unknown numbers are kept).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct AggregationTemporality(pub i32);

impl AggregationTemporality {
    pub const UNSPECIFIED: AggregationTemporality = AggregationTemporality(0);
    pub const DELTA: AggregationTemporality = AggregationTemporality(1);
    pub const CUMULATIVE: AggregationTemporality = AggregationTemporality(2);
}

impl fmt::Display for AggregationTemporality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self.0 {
            0 => "Unspecified",
            1 => "Delta",
            2 => "Cumulative",
            _ => "",
        })
    }
}

/// Go `pmetric.DataPointFlags`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct DataPointFlags(pub u32);

impl DataPointFlags {
    /// Go `DefaultDataPointFlags`.
    pub const DEFAULT: DataPointFlags = DataPointFlags(0);
    const NO_RECORDED_VALUE_MASK: u32 = 1;

    pub fn no_recorded_value(self) -> bool {
        self.0 & Self::NO_RECORDED_VALUE_MASK != 0
    }

    pub fn with_no_recorded_value(self, b: bool) -> DataPointFlags {
        if b {
            DataPointFlags(self.0 | Self::NO_RECORDED_VALUE_MASK)
        } else {
            DataPointFlags(self.0 & !Self::NO_RECORDED_VALUE_MASK)
        }
    }
}

/// Go `pmetric.Metrics` (and `pmetricotlp.ExportRequest`, which only wraps it).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Metrics {
    pub resource_metrics: Vec<ResourceMetrics>,
}

impl Metrics {
    pub fn new() -> Metrics {
        Metrics::default()
    }

    /// Go `Metrics.MetricCount`.
    pub fn metric_count(&self) -> usize {
        self.resource_metrics.iter().flat_map(|rm| rm.scope_metrics.iter()).map(|sm| sm.metrics.len()).sum()
    }

    /// Go `Metrics.DataPointCount`.
    pub fn data_point_count(&self) -> usize {
        self.resource_metrics
            .iter()
            .flat_map(|rm| rm.scope_metrics.iter())
            .flat_map(|sm| sm.metrics.iter())
            .map(|m| m.data_points_len())
            .sum()
    }
}

/// Go `pmetric.ResourceMetrics`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResourceMetrics {
    pub resource: Resource,
    pub scope_metrics: Vec<ScopeMetrics>,
    pub schema_url: String,
}

/// Go `pcommon.Resource`. Entity references are not kept.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Resource {
    pub attributes: Map,
    pub dropped_attributes_count: u32,
}

impl Resource {
    pub fn new() -> Resource {
        Resource::default()
    }
}

/// Go `pmetric.ScopeMetrics`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ScopeMetrics {
    pub scope: InstrumentationScope,
    pub metrics: Vec<Metric>,
    pub schema_url: String,
}

/// Go `pcommon.InstrumentationScope`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InstrumentationScope {
    pub name: String,
    pub version: String,
    pub attributes: Map,
    pub dropped_attributes_count: u32,
}

/// Go `pmetric.MetricType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetricType {
    Empty,
    Gauge,
    Sum,
    Histogram,
    ExponentialHistogram,
    Summary,
}

impl fmt::Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            MetricType::Empty => "Empty",
            MetricType::Gauge => "Gauge",
            MetricType::Sum => "Sum",
            MetricType::Histogram => "Histogram",
            MetricType::ExponentialHistogram => "ExponentialHistogram",
            MetricType::Summary => "Summary",
        })
    }
}

/// The data of a metric (the proto `oneof data`).
#[derive(Clone, Debug, Default, PartialEq)]
pub enum MetricData {
    #[default]
    Empty,
    Gauge(Gauge),
    Sum(Sum),
    Histogram(Histogram),
    ExponentialHistogram(ExponentialHistogram),
    Summary(Summary),
}

/// Go `pmetric.Metric`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Metric {
    pub name: String,
    pub description: String,
    pub unit: String,
    pub data: MetricData,
    pub metadata: Map,
}

macro_rules! metric_data_accessors {
    ($variant:ident, $ty:ident, $get:ident, $get_mut:ident, $set:ident) => {
        /// Go `Metric.<Type>()`. Panics if the metric has another type (Go returns a detached value).
        pub fn $get(&self) -> &$ty {
            match &self.data {
                MetricData::$variant(d) => d,
                _ => panic!(concat!("metric is not a ", stringify!($variant))),
            }
        }
        pub fn $get_mut(&mut self) -> &mut $ty {
            match &mut self.data {
                MetricData::$variant(d) => d,
                _ => panic!(concat!("metric is not a ", stringify!($variant))),
            }
        }
        /// Go `Metric.SetEmpty<Type>`.
        pub fn $set(&mut self) -> &mut $ty {
            self.data = MetricData::$variant($ty::default());
            self.$get_mut()
        }
    };
}

impl Metric {
    pub fn new() -> Metric {
        Metric::default()
    }

    /// Go `Metric.Type`.
    pub fn typ(&self) -> MetricType {
        match self.data {
            MetricData::Empty => MetricType::Empty,
            MetricData::Gauge(_) => MetricType::Gauge,
            MetricData::Sum(_) => MetricType::Sum,
            MetricData::Histogram(_) => MetricType::Histogram,
            MetricData::ExponentialHistogram(_) => MetricType::ExponentialHistogram,
            MetricData::Summary(_) => MetricType::Summary,
        }
    }

    metric_data_accessors!(Gauge, Gauge, gauge, gauge_mut, set_empty_gauge);
    metric_data_accessors!(Sum, Sum, sum, sum_mut, set_empty_sum);
    metric_data_accessors!(Histogram, Histogram, histogram, histogram_mut, set_empty_histogram);
    metric_data_accessors!(
        ExponentialHistogram,
        ExponentialHistogram,
        exponential_histogram,
        exponential_histogram_mut,
        set_empty_exponential_histogram
    );
    metric_data_accessors!(Summary, Summary, summary, summary_mut, set_empty_summary);

    /// The number of data points of the metric.
    pub fn data_points_len(&self) -> usize {
        match &self.data {
            MetricData::Empty => 0,
            MetricData::Gauge(g) => g.data_points.len(),
            MetricData::Sum(s) => s.data_points.len(),
            MetricData::Histogram(h) => h.data_points.len(),
            MetricData::ExponentialHistogram(h) => h.data_points.len(),
            MetricData::Summary(s) => s.data_points.len(),
        }
    }
}

/// Go `pmetric.Gauge`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Gauge {
    pub data_points: Vec<NumberDataPoint>,
}

/// Go `pmetric.Sum`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Sum {
    pub data_points: Vec<NumberDataPoint>,
    pub aggregation_temporality: AggregationTemporality,
    pub is_monotonic: bool,
}

/// Go `pmetric.Histogram`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Histogram {
    pub data_points: Vec<HistogramDataPoint>,
    pub aggregation_temporality: AggregationTemporality,
}

/// Go `pmetric.ExponentialHistogram`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExponentialHistogram {
    pub data_points: Vec<ExponentialHistogramDataPoint>,
    pub aggregation_temporality: AggregationTemporality,
}

/// Go `pmetric.Summary`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Summary {
    pub data_points: Vec<SummaryDataPoint>,
}

/// Go `pmetric.NumberDataPointValueType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NumberDataPointValueType {
    Empty,
    Int,
    Double,
}

/// The value of a number data point or an exemplar (the proto `oneof value`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum NumberValue {
    #[default]
    Empty,
    Int(i64),
    Double(f64),
}

impl NumberValue {
    pub fn value_type(&self) -> NumberDataPointValueType {
        match self {
            NumberValue::Empty => NumberDataPointValueType::Empty,
            NumberValue::Int(_) => NumberDataPointValueType::Int,
            NumberValue::Double(_) => NumberDataPointValueType::Double,
        }
    }
    /// Go `IntValue()` (0 when not an int).
    pub fn int_value(&self) -> i64 {
        match self {
            NumberValue::Int(v) => *v,
            _ => 0,
        }
    }
    /// Go `DoubleValue()` (0 when not a double).
    pub fn double_value(&self) -> f64 {
        match self {
            NumberValue::Double(v) => *v,
            _ => 0.0,
        }
    }
}

/// Go `pmetric.NumberDataPoint`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NumberDataPoint {
    pub attributes: Map,
    pub start_timestamp: Timestamp,
    pub timestamp: Timestamp,
    pub value: NumberValue,
    pub exemplars: Vec<Exemplar>,
    pub flags: DataPointFlags,
}

impl NumberDataPoint {
    pub fn new() -> NumberDataPoint {
        NumberDataPoint::default()
    }
    pub fn set_int_value(&mut self, v: i64) {
        self.value = NumberValue::Int(v);
    }
    pub fn set_double_value(&mut self, v: f64) {
        self.value = NumberValue::Double(v);
    }
    pub fn int_value(&self) -> i64 {
        self.value.int_value()
    }
    pub fn double_value(&self) -> f64 {
        self.value.double_value()
    }
    pub fn value_type(&self) -> NumberDataPointValueType {
        self.value.value_type()
    }
}

/// Go `pmetric.HistogramDataPoint`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HistogramDataPoint {
    pub attributes: Map,
    pub start_timestamp: Timestamp,
    pub timestamp: Timestamp,
    pub count: u64,
    /// `None` when unset (Go `HasSum`).
    pub sum: Option<f64>,
    pub bucket_counts: Vec<u64>,
    pub explicit_bounds: Vec<f64>,
    pub exemplars: Vec<Exemplar>,
    pub flags: DataPointFlags,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

impl HistogramDataPoint {
    pub fn new() -> HistogramDataPoint {
        HistogramDataPoint::default()
    }
    pub fn has_sum(&self) -> bool {
        self.sum.is_some()
    }
    /// Go `Sum()` (0 when unset).
    pub fn sum(&self) -> f64 {
        self.sum.unwrap_or(0.0)
    }
    pub fn set_sum(&mut self, v: f64) {
        self.sum = Some(v);
    }
}

/// Go `pmetric.ExponentialHistogramDataPointBuckets`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Buckets {
    pub offset: i32,
    pub bucket_counts: Vec<u64>,
}

/// Go `pmetric.ExponentialHistogramDataPoint`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExponentialHistogramDataPoint {
    pub attributes: Map,
    pub start_timestamp: Timestamp,
    pub timestamp: Timestamp,
    pub count: u64,
    /// `None` when unset (Go `HasSum`).
    pub sum: Option<f64>,
    pub scale: i32,
    pub zero_count: u64,
    pub positive: Buckets,
    pub negative: Buckets,
    pub flags: DataPointFlags,
    pub exemplars: Vec<Exemplar>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub zero_threshold: f64,
}

impl ExponentialHistogramDataPoint {
    pub fn new() -> ExponentialHistogramDataPoint {
        ExponentialHistogramDataPoint::default()
    }
    pub fn has_sum(&self) -> bool {
        self.sum.is_some()
    }
    /// Go `Sum()` (0 when unset).
    pub fn sum(&self) -> f64 {
        self.sum.unwrap_or(0.0)
    }
    pub fn set_sum(&mut self, v: f64) {
        self.sum = Some(v);
    }
}

/// Go `pmetric.SummaryDataPointValueAtQuantile`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ValueAtQuantile {
    pub quantile: f64,
    pub value: f64,
}

/// Go `pmetric.SummaryDataPoint`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SummaryDataPoint {
    pub attributes: Map,
    pub start_timestamp: Timestamp,
    pub timestamp: Timestamp,
    pub count: u64,
    pub sum: f64,
    pub quantile_values: Vec<ValueAtQuantile>,
    pub flags: DataPointFlags,
}

/// Go `pmetric.ExemplarValueType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExemplarValueType {
    Empty,
    Int,
    Double,
}

impl fmt::Display for ExemplarValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ExemplarValueType::Empty => "Empty",
            ExemplarValueType::Int => "Int",
            ExemplarValueType::Double => "Double",
        })
    }
}

/// Go `pmetric.Exemplar`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Exemplar {
    pub filtered_attributes: Map,
    pub timestamp: Timestamp,
    pub value: NumberValue,
    pub span_id: SpanId,
    pub trace_id: TraceId,
}

impl Exemplar {
    pub fn new() -> Exemplar {
        Exemplar::default()
    }
    pub fn value_type(&self) -> ExemplarValueType {
        match self.value {
            NumberValue::Empty => ExemplarValueType::Empty,
            NumberValue::Int(_) => ExemplarValueType::Int,
            NumberValue::Double(_) => ExemplarValueType::Double,
        }
    }
    pub fn set_int_value(&mut self, v: i64) {
        self.value = NumberValue::Int(v);
    }
    pub fn set_double_value(&mut self, v: f64) {
        self.value = NumberValue::Double(v);
    }
    pub fn int_value(&self) -> i64 {
        self.value.int_value()
    }
    pub fn double_value(&self) -> f64 {
        self.value.double_value()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_string_forms() {
        assert_eq!(Value::Double(1.5).as_string(), "1.5");
        assert_eq!(Value::Double(1e21).as_string(), "1e+21");
        assert_eq!(Value::Double(1e-7).as_string(), "1e-7");
        assert_eq!(Value::Double(f64::INFINITY).as_string(), "Infinity");
        assert_eq!(Value::Bytes(vec![1, 2, 3, 4]).as_string(), "AQIDBA==");
        let m = Map::from_pairs(&[("b", Value::Int(1)), ("a", Value::Str("<x>".into()))]);
        assert_eq!(Value::Map(m).as_string(), r#"{"a":"<x>","b":1}"#);
        assert_eq!(base64_std_decode(b"AQIDBA==").unwrap(), vec![1, 2, 3, 4]);
    }
}
