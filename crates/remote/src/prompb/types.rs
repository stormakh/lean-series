//! Port of `prompb/types.proto` and `prompb/remote.proto` (package `prometheus`): the remote write 1.0 and
//! remote read messages, with hand-written gogo-compatible encoding and decoding.

use super::proto::*;

/// A protobuf message with gogo-compatible encoding.
pub trait Message: Default {
    /// Go `Size()`.
    fn encoded_len(&self) -> usize;
    /// Appends the encoded fields (no length prefix).
    fn encode_raw(&self, buf: &mut Vec<u8>);
    /// Go `Unmarshal` on an existing value: repeated fields append, scalars overwrite.
    fn merge(&mut self, b: &[u8]) -> Result<()>;

    /// Go `Marshal()`.
    fn marshal(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_raw(&mut buf);
        buf
    }
    /// Marshals into `buf`, reusing its capacity (Go `MarshalToSizedBuffer` on a pooled buffer).
    fn marshal_into(&self, buf: &mut Vec<u8>) {
        buf.clear();
        buf.reserve(self.encoded_len());
        self.encode_raw(buf);
    }
    /// Go `proto.Unmarshal`: decodes into a fresh value.
    fn unmarshal(b: &[u8]) -> Result<Self> {
        let mut m = Self::default();
        m.merge(b)?;
        Ok(m)
    }
}

#[inline]
pub(crate) fn put_msg<M: Message>(buf: &mut Vec<u8>, field: u32, m: &M) {
    put_message_header(buf, field, m.encoded_len());
    m.encode_raw(buf);
}

#[inline]
fn msg_len<M: Message>(field: u32, m: &M) -> usize {
    message_len(field, m.encoded_len())
}

#[inline]
fn msgs_len<M: Message>(field: u32, ms: &[M]) -> usize {
    ms.iter().map(|m| msg_len(field, m)).sum()
}

#[inline]
fn push_msg<M: Message>(d: &mut Decoder<'_>, out: &mut Vec<M>) -> Result<()> {
    let b = d.bytes()?;
    let mut m = M::default();
    m.merge(b)?;
    out.push(m);
    Ok(())
}

/// Go `MetricMetadata_MetricType`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct MetricType(pub i32);

impl MetricType {
    pub const UNKNOWN: MetricType = MetricType(0);
    pub const COUNTER: MetricType = MetricType(1);
    pub const GAUGE: MetricType = MetricType(2);
    pub const HISTOGRAM: MetricType = MetricType(3);
    pub const GAUGEHISTOGRAM: MetricType = MetricType(4);
    pub const SUMMARY: MetricType = MetricType(5);
    pub const INFO: MetricType = MetricType(6);
    pub const STATESET: MetricType = MetricType(7);

    /// The enum name (Go `MetricMetadata_MetricType_name`), or the number.
    pub fn name(self) -> String {
        match self.0 {
            0 => "UNKNOWN".into(),
            1 => "COUNTER".into(),
            2 => "GAUGE".into(),
            3 => "HISTOGRAM".into(),
            4 => "GAUGEHISTOGRAM".into(),
            5 => "SUMMARY".into(),
            6 => "INFO".into(),
            7 => "STATESET".into(),
            n => n.to_string(),
        }
    }

    /// Go `MetricMetadata_MetricType_value`.
    pub fn from_name(s: &str) -> Option<MetricType> {
        Some(MetricType(match s {
            "UNKNOWN" => 0,
            "COUNTER" => 1,
            "GAUGE" => 2,
            "HISTOGRAM" => 3,
            "GAUGEHISTOGRAM" => 4,
            "SUMMARY" => 5,
            "INFO" => 6,
            "STATESET" => 7,
            _ => return None,
        }))
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetricMetadata {
    pub r#type: MetricType,
    pub metric_family_name: String,
    pub help: String,
    pub unit: String,
}

impl Message for MetricMetadata {
    fn encoded_len(&self) -> usize {
        int_field_len(1, self.r#type.0 as i64)
            + bytes_field_len(2, self.metric_family_name.len())
            + bytes_field_len(4, self.help.len())
            + bytes_field_len(5, self.unit.len())
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        put_int_field(buf, 1, self.r#type.0 as i64);
        put_bytes_field(buf, 2, self.metric_family_name.as_bytes());
        put_bytes_field(buf, 4, self.help.as_bytes());
        put_bytes_field(buf, 5, self.unit.as_bytes());
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("MetricMetadata")?;
            match f {
                1 => {
                    d.expect(wt, 0, "Type")?;
                    self.r#type = MetricType(d.varint()? as i32);
                }
                2 => {
                    d.expect(wt, 2, "MetricFamilyName")?;
                    self.metric_family_name = d.string()?;
                }
                4 => {
                    d.expect(wt, 2, "Help")?;
                    self.help = d.string()?;
                }
                5 => {
                    d.expect(wt, 2, "Unit")?;
                    self.unit = d.string()?;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Sample {
    pub value: f64,
    pub timestamp: i64,
}

impl Message for Sample {
    fn encoded_len(&self) -> usize {
        double_field_len(1, self.value) + int_field_len(2, self.timestamp)
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        put_double_field(buf, 1, self.value);
        put_int_field(buf, 2, self.timestamp);
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("Sample")?;
            match f {
                1 => {
                    d.expect(wt, 1, "Value")?;
                    self.value = d.double()?;
                }
                2 => {
                    d.expect(wt, 0, "Timestamp")?;
                    self.timestamp = d.varint()? as i64;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

impl Sample {
    pub fn t(&self) -> i64 {
        self.timestamp
    }
    pub fn v(&self) -> f64 {
        self.value
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Label {
    pub name: String,
    pub value: String,
}

impl Label {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Label {
        Label { name: name.into(), value: value.into() }
    }
}

/// Encoded length of a `Label` message body from its name and value.
#[inline]
pub fn label_len(name: &str, value: &str) -> usize {
    bytes_field_len(1, name.len()) + bytes_field_len(2, value.len())
}

/// Appends one `Label` as field `field` of the parent (used by encoders that write straight from
/// `model::labels::Labels` without building `Label` values).
#[inline]
pub fn put_label(buf: &mut Vec<u8>, field: u32, name: &str, value: &str) {
    put_message_header(buf, field, label_len(name, value));
    put_bytes_field(buf, 1, name.as_bytes());
    put_bytes_field(buf, 2, value.as_bytes());
}

impl Message for Label {
    fn encoded_len(&self) -> usize {
        label_len(&self.name, &self.value)
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        put_bytes_field(buf, 1, self.name.as_bytes());
        put_bytes_field(buf, 2, self.value.as_bytes());
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("Label")?;
            match f {
                1 => {
                    d.expect(wt, 2, "Name")?;
                    self.name = d.string()?;
                }
                2 => {
                    d.expect(wt, 2, "Value")?;
                    self.value = d.string()?;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Exemplar {
    pub labels: Vec<Label>,
    pub value: f64,
    pub timestamp: i64,
}

impl Message for Exemplar {
    fn encoded_len(&self) -> usize {
        msgs_len(1, &self.labels) + double_field_len(2, self.value) + int_field_len(3, self.timestamp)
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        for l in &self.labels {
            put_msg(buf, 1, l);
        }
        put_double_field(buf, 2, self.value);
        put_int_field(buf, 3, self.timestamp);
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("Exemplar")?;
            match f {
                1 => {
                    d.expect(wt, 2, "Labels")?;
                    push_msg(&mut d, &mut self.labels)?;
                }
                2 => {
                    d.expect(wt, 1, "Value")?;
                    self.value = d.double()?;
                }
                3 => {
                    d.expect(wt, 0, "Timestamp")?;
                    self.timestamp = d.varint()? as i64;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

/// The `count` and `zero_count` oneofs of `Histogram`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Count {
    Int(u64),
    Float(f64),
}

/// Go `Histogram_ResetHint` (and writev2 `Histogram_ResetHint`, same numbers).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ResetHint(pub i32);

impl ResetHint {
    pub const UNKNOWN: ResetHint = ResetHint(0);
    pub const YES: ResetHint = ResetHint(1);
    pub const NO: ResetHint = ResetHint(2);
    pub const GAUGE: ResetHint = ResetHint(3);
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct BucketSpan {
    pub offset: i32,
    pub length: u32,
}

impl Message for BucketSpan {
    fn encoded_len(&self) -> usize {
        sint32_field_len(1, self.offset) + uint_field_len(2, self.length as u64)
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        put_sint32_field(buf, 1, self.offset);
        put_uint_field(buf, 2, self.length as u64);
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("BucketSpan")?;
            match f {
                1 => {
                    d.expect(wt, 0, "Offset")?;
                    let v = d.varint()? as u32;
                    self.offset = ((v >> 1) as i32) ^ (((v & 1) as i32) << 31 >> 31);
                }
                2 => {
                    d.expect(wt, 0, "Length")?;
                    self.length = d.varint()? as u32;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

/// A native histogram (remote write 1.0 `prometheus.Histogram` and 2.0 `io.prometheus.write.v2.Histogram`,
/// which have the same fields; `start_timestamp` (field 17) exists only in 2.0 and is not written by the
/// 1.0 encoder).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Histogram {
    pub count: Option<Count>,
    pub sum: f64,
    pub schema: i32,
    pub zero_threshold: f64,
    pub zero_count: Option<Count>,
    pub negative_spans: Vec<BucketSpan>,
    pub negative_deltas: Vec<i64>,
    pub negative_counts: Vec<f64>,
    pub positive_spans: Vec<BucketSpan>,
    pub positive_deltas: Vec<i64>,
    pub positive_counts: Vec<f64>,
    pub reset_hint: ResetHint,
    pub timestamp: i64,
    pub custom_values: Vec<f64>,
    /// Only remote write 2.0.
    pub start_timestamp: i64,
}

fn count_len(c: &Option<Count>, int_field: u32) -> usize {
    match c {
        None => 0,
        Some(Count::Int(v)) => tag_len(int_field) + sov(*v),
        Some(Count::Float(_)) => tag_len(int_field + 1) + 8,
    }
}

fn put_count(buf: &mut Vec<u8>, c: &Option<Count>, int_field: u32) {
    match c {
        None => {}
        Some(Count::Int(v)) => put_uint_always(buf, int_field, *v),
        Some(Count::Float(v)) => put_double_always(buf, int_field + 1, *v),
    }
}

impl Histogram {
    pub(crate) fn len_v(&self, v2: bool) -> usize {
        count_len(&self.count, 1)
            + double_field_len(3, self.sum)
            + sint32_field_len(4, self.schema)
            + double_field_len(5, self.zero_threshold)
            + count_len(&self.zero_count, 6)
            + msgs_len(8, &self.negative_spans)
            + packed_sint64_len(9, &self.negative_deltas)
            + packed_doubles_len(10, self.negative_counts.len())
            + msgs_len(11, &self.positive_spans)
            + packed_sint64_len(12, &self.positive_deltas)
            + packed_doubles_len(13, self.positive_counts.len())
            + int_field_len(14, self.reset_hint.0 as i64)
            + int_field_len(15, self.timestamp)
            + packed_doubles_len(16, self.custom_values.len())
            + if v2 { int_field_len(17, self.start_timestamp) } else { 0 }
    }

    pub(crate) fn encode_v(&self, buf: &mut Vec<u8>, v2: bool) {
        put_count(buf, &self.count, 1);
        put_double_field(buf, 3, self.sum);
        put_sint32_field(buf, 4, self.schema);
        put_double_field(buf, 5, self.zero_threshold);
        put_count(buf, &self.zero_count, 6);
        for s in &self.negative_spans {
            put_msg(buf, 8, s);
        }
        put_packed_sint64(buf, 9, &self.negative_deltas);
        put_packed_doubles(buf, 10, &self.negative_counts);
        for s in &self.positive_spans {
            put_msg(buf, 11, s);
        }
        put_packed_sint64(buf, 12, &self.positive_deltas);
        put_packed_doubles(buf, 13, &self.positive_counts);
        put_int_field(buf, 14, self.reset_hint.0 as i64);
        put_int_field(buf, 15, self.timestamp);
        put_packed_doubles(buf, 16, &self.custom_values);
        if v2 {
            put_int_field(buf, 17, self.start_timestamp);
        }
    }

    pub(crate) fn merge_v(&mut self, b: &[u8], v2: bool) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("Histogram")?;
            match f {
                1 => {
                    d.expect(wt, 0, "CountInt")?;
                    self.count = Some(Count::Int(d.varint()?));
                }
                2 => {
                    d.expect(wt, 1, "CountFloat")?;
                    self.count = Some(Count::Float(d.double()?));
                }
                3 => {
                    d.expect(wt, 1, "Sum")?;
                    self.sum = d.double()?;
                }
                4 => {
                    d.expect(wt, 0, "Schema")?;
                    let v = d.varint()? as u32;
                    self.schema = ((v >> 1) as i32) ^ (((v & 1) as i32) << 31 >> 31);
                }
                5 => {
                    d.expect(wt, 1, "ZeroThreshold")?;
                    self.zero_threshold = d.double()?;
                }
                6 => {
                    d.expect(wt, 0, "ZeroCountInt")?;
                    self.zero_count = Some(Count::Int(d.varint()?));
                }
                7 => {
                    d.expect(wt, 1, "ZeroCountFloat")?;
                    self.zero_count = Some(Count::Float(d.double()?));
                }
                8 => {
                    d.expect(wt, 2, "NegativeSpans")?;
                    push_msg(&mut d, &mut self.negative_spans)?;
                }
                9 => {
                    let out = &mut self.negative_deltas;
                    d.repeated_varint(wt, "NegativeDeltas", |v| out.push(((v >> 1) as i64) ^ -((v & 1) as i64)))?;
                }
                10 => d.repeated_double(wt, "NegativeCounts", &mut self.negative_counts)?,
                11 => {
                    d.expect(wt, 2, "PositiveSpans")?;
                    push_msg(&mut d, &mut self.positive_spans)?;
                }
                12 => {
                    let out = &mut self.positive_deltas;
                    d.repeated_varint(wt, "PositiveDeltas", |v| out.push(((v >> 1) as i64) ^ -((v & 1) as i64)))?;
                }
                13 => d.repeated_double(wt, "PositiveCounts", &mut self.positive_counts)?,
                14 => {
                    d.expect(wt, 0, "ResetHint")?;
                    self.reset_hint = ResetHint(d.varint()? as i32);
                }
                15 => {
                    d.expect(wt, 0, "Timestamp")?;
                    self.timestamp = d.varint()? as i64;
                }
                16 => d.repeated_double(wt, "CustomValues", &mut self.custom_values)?,
                17 if v2 => {
                    d.expect(wt, 0, "StartTimestamp")?;
                    self.start_timestamp = d.varint()? as i64;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }

    /// Go `GetCountInt` (0 when the count is a float or unset).
    pub fn get_count_int(&self) -> u64 {
        match self.count {
            Some(Count::Int(v)) => v,
            _ => 0,
        }
    }
    pub fn get_count_float(&self) -> f64 {
        match self.count {
            Some(Count::Float(v)) => v,
            _ => 0.0,
        }
    }
    pub fn get_zero_count_int(&self) -> u64 {
        match self.zero_count {
            Some(Count::Int(v)) => v,
            _ => 0,
        }
    }
    pub fn get_zero_count_float(&self) -> f64 {
        match self.zero_count {
            Some(Count::Float(v)) => v,
            _ => 0.0,
        }
    }
}

impl Message for Histogram {
    fn encoded_len(&self) -> usize {
        self.len_v(false)
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        self.encode_v(buf, false)
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        self.merge_v(b, false)
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TimeSeries {
    pub labels: Vec<Label>,
    pub samples: Vec<Sample>,
    pub exemplars: Vec<Exemplar>,
    pub histograms: Vec<Histogram>,
}

impl Message for TimeSeries {
    fn encoded_len(&self) -> usize {
        msgs_len(1, &self.labels) + msgs_len(2, &self.samples) + msgs_len(3, &self.exemplars) + msgs_len(4, &self.histograms)
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        for m in &self.labels {
            put_msg(buf, 1, m);
        }
        for m in &self.samples {
            put_msg(buf, 2, m);
        }
        for m in &self.exemplars {
            put_msg(buf, 3, m);
        }
        for m in &self.histograms {
            put_msg(buf, 4, m);
        }
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("TimeSeries")?;
            match f {
                1 => {
                    d.expect(wt, 2, "Labels")?;
                    push_msg(&mut d, &mut self.labels)?;
                }
                2 => {
                    d.expect(wt, 2, "Samples")?;
                    push_msg(&mut d, &mut self.samples)?;
                }
                3 => {
                    d.expect(wt, 2, "Exemplars")?;
                    push_msg(&mut d, &mut self.exemplars)?;
                }
                4 => {
                    d.expect(wt, 2, "Histograms")?;
                    push_msg(&mut d, &mut self.histograms)?;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

/// `prometheus.Labels`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Labels {
    pub labels: Vec<Label>,
}

impl Message for Labels {
    fn encoded_len(&self) -> usize {
        msgs_len(1, &self.labels)
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        for m in &self.labels {
            put_msg(buf, 1, m);
        }
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("Labels")?;
            match f {
                1 => {
                    d.expect(wt, 2, "Labels")?;
                    push_msg(&mut d, &mut self.labels)?;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

/// Go `LabelMatcher_Type`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct MatcherType(pub i32);

impl MatcherType {
    pub const EQ: MatcherType = MatcherType(0);
    pub const NEQ: MatcherType = MatcherType(1);
    pub const RE: MatcherType = MatcherType(2);
    pub const NRE: MatcherType = MatcherType(3);
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LabelMatcher {
    pub r#type: MatcherType,
    pub name: String,
    pub value: String,
}

impl Message for LabelMatcher {
    fn encoded_len(&self) -> usize {
        int_field_len(1, self.r#type.0 as i64) + bytes_field_len(2, self.name.len()) + bytes_field_len(3, self.value.len())
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        put_int_field(buf, 1, self.r#type.0 as i64);
        put_bytes_field(buf, 2, self.name.as_bytes());
        put_bytes_field(buf, 3, self.value.as_bytes());
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("LabelMatcher")?;
            match f {
                1 => {
                    d.expect(wt, 0, "Type")?;
                    self.r#type = MatcherType(d.varint()? as i32);
                }
                2 => {
                    d.expect(wt, 2, "Name")?;
                    self.name = d.string()?;
                }
                3 => {
                    d.expect(wt, 2, "Value")?;
                    self.value = d.string()?;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReadHints {
    pub step_ms: i64,
    pub func: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub grouping: Vec<String>,
    pub by: bool,
    pub range_ms: i64,
}

impl Message for ReadHints {
    fn encoded_len(&self) -> usize {
        int_field_len(1, self.step_ms)
            + bytes_field_len(2, self.func.len())
            + int_field_len(3, self.start_ms)
            + int_field_len(4, self.end_ms)
            + self.grouping.iter().map(|s| bytes_always_len(5, s.len())).sum::<usize>()
            + bool_field_len(6, self.by)
            + int_field_len(7, self.range_ms)
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        put_int_field(buf, 1, self.step_ms);
        put_bytes_field(buf, 2, self.func.as_bytes());
        put_int_field(buf, 3, self.start_ms);
        put_int_field(buf, 4, self.end_ms);
        for s in &self.grouping {
            put_bytes_always(buf, 5, s.as_bytes());
        }
        put_bool_field(buf, 6, self.by);
        put_int_field(buf, 7, self.range_ms);
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("ReadHints")?;
            match f {
                1 => {
                    d.expect(wt, 0, "StepMs")?;
                    self.step_ms = d.varint()? as i64;
                }
                2 => {
                    d.expect(wt, 2, "Func")?;
                    self.func = d.string()?;
                }
                3 => {
                    d.expect(wt, 0, "StartMs")?;
                    self.start_ms = d.varint()? as i64;
                }
                4 => {
                    d.expect(wt, 0, "EndMs")?;
                    self.end_ms = d.varint()? as i64;
                }
                5 => {
                    d.expect(wt, 2, "Grouping")?;
                    self.grouping.push(d.string()?);
                }
                6 => {
                    d.expect(wt, 0, "By")?;
                    self.by = d.varint()? != 0;
                }
                7 => {
                    d.expect(wt, 0, "RangeMs")?;
                    self.range_ms = d.varint()? as i64;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

/// Go `Chunk_Encoding`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ChunkEncoding(pub i32);

impl ChunkEncoding {
    pub const UNKNOWN: ChunkEncoding = ChunkEncoding(0);
    pub const XOR: ChunkEncoding = ChunkEncoding(1);
    pub const HISTOGRAM: ChunkEncoding = ChunkEncoding(2);
    pub const FLOAT_HISTOGRAM: ChunkEncoding = ChunkEncoding(3);
    pub const XOR2: ChunkEncoding = ChunkEncoding(4);
    pub const HISTOGRAM_ST: ChunkEncoding = ChunkEncoding(5);
    pub const FLOAT_HISTOGRAM_ST: ChunkEncoding = ChunkEncoding(6);
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Chunk {
    pub min_time_ms: i64,
    pub max_time_ms: i64,
    pub r#type: ChunkEncoding,
    pub data: Vec<u8>,
}

impl Message for Chunk {
    fn encoded_len(&self) -> usize {
        int_field_len(1, self.min_time_ms)
            + int_field_len(2, self.max_time_ms)
            + int_field_len(3, self.r#type.0 as i64)
            + bytes_field_len(4, self.data.len())
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        put_int_field(buf, 1, self.min_time_ms);
        put_int_field(buf, 2, self.max_time_ms);
        put_int_field(buf, 3, self.r#type.0 as i64);
        put_bytes_field(buf, 4, &self.data);
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("Chunk")?;
            match f {
                1 => {
                    d.expect(wt, 0, "MinTimeMs")?;
                    self.min_time_ms = d.varint()? as i64;
                }
                2 => {
                    d.expect(wt, 0, "MaxTimeMs")?;
                    self.max_time_ms = d.varint()? as i64;
                }
                3 => {
                    d.expect(wt, 0, "Type")?;
                    self.r#type = ChunkEncoding(d.varint()? as i32);
                }
                4 => {
                    d.expect(wt, 2, "Data")?;
                    // gogo: `append(m.Data[:0], ...)`, and a non-nil empty slice for an empty field.
                    let b = d.bytes()?;
                    self.data.clear();
                    self.data.extend_from_slice(b);
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ChunkedSeries {
    pub labels: Vec<Label>,
    pub chunks: Vec<Chunk>,
}

impl Message for ChunkedSeries {
    fn encoded_len(&self) -> usize {
        msgs_len(1, &self.labels) + msgs_len(2, &self.chunks)
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        for m in &self.labels {
            put_msg(buf, 1, m);
        }
        for m in &self.chunks {
            put_msg(buf, 2, m);
        }
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("ChunkedSeries")?;
            match f {
                1 => {
                    d.expect(wt, 2, "Labels")?;
                    push_msg(&mut d, &mut self.labels)?;
                }
                2 => {
                    d.expect(wt, 2, "Chunks")?;
                    push_msg(&mut d, &mut self.chunks)?;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------- remote.proto

#[derive(Clone, Debug, Default, PartialEq)]
pub struct WriteRequest {
    pub timeseries: Vec<TimeSeries>,
    pub metadata: Vec<MetricMetadata>,
}

impl Message for WriteRequest {
    fn encoded_len(&self) -> usize {
        msgs_len(1, &self.timeseries) + msgs_len(3, &self.metadata)
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        for m in &self.timeseries {
            put_msg(buf, 1, m);
        }
        for m in &self.metadata {
            put_msg(buf, 3, m);
        }
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("WriteRequest")?;
            match f {
                1 => {
                    d.expect(wt, 2, "Timeseries")?;
                    push_msg(&mut d, &mut self.timeseries)?;
                }
                3 => {
                    d.expect(wt, 2, "Metadata")?;
                    push_msg(&mut d, &mut self.metadata)?;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

/// Go `ReadRequest_ResponseType`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ResponseType(pub i32);

impl ResponseType {
    pub const SAMPLES: ResponseType = ResponseType(0);
    pub const STREAMED_XOR_CHUNKS: ResponseType = ResponseType(1);
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReadRequest {
    pub queries: Vec<Query>,
    pub accepted_response_types: Vec<ResponseType>,
}

impl Message for ReadRequest {
    fn encoded_len(&self) -> usize {
        let types: usize = self.accepted_response_types.iter().map(|t| sov(t.0 as i64 as u64)).sum();
        msgs_len(1, &self.queries) + if types > 0 { tag_len(2) + sov(types as u64) + types } else { 0 }
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        for m in &self.queries {
            put_msg(buf, 1, m);
        }
        if !self.accepted_response_types.is_empty() {
            let n: usize = self.accepted_response_types.iter().map(|t| sov(t.0 as i64 as u64)).sum();
            put_tag(buf, 2, 2);
            put_varint(buf, n as u64);
            for t in &self.accepted_response_types {
                put_varint(buf, t.0 as i64 as u64);
            }
        }
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("ReadRequest")?;
            match f {
                1 => {
                    d.expect(wt, 2, "Queries")?;
                    push_msg(&mut d, &mut self.queries)?;
                }
                2 => {
                    let out = &mut self.accepted_response_types;
                    d.repeated_varint(wt, "AcceptedResponseTypes", |v| out.push(ResponseType(v as i32)))?;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReadResponse {
    pub results: Vec<QueryResult>,
}

impl Message for ReadResponse {
    fn encoded_len(&self) -> usize {
        msgs_len(1, &self.results)
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        for m in &self.results {
            put_msg(buf, 1, m);
        }
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("ReadResponse")?;
            match f {
                1 => {
                    d.expect(wt, 2, "Results")?;
                    push_msg(&mut d, &mut self.results)?;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Query {
    pub start_timestamp_ms: i64,
    pub end_timestamp_ms: i64,
    pub matchers: Vec<LabelMatcher>,
    pub hints: Option<ReadHints>,
}

impl Message for Query {
    fn encoded_len(&self) -> usize {
        int_field_len(1, self.start_timestamp_ms)
            + int_field_len(2, self.end_timestamp_ms)
            + msgs_len(3, &self.matchers)
            + self.hints.as_ref().map_or(0, |h| msg_len(4, h))
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        put_int_field(buf, 1, self.start_timestamp_ms);
        put_int_field(buf, 2, self.end_timestamp_ms);
        for m in &self.matchers {
            put_msg(buf, 3, m);
        }
        if let Some(h) = &self.hints {
            put_msg(buf, 4, h);
        }
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("Query")?;
            match f {
                1 => {
                    d.expect(wt, 0, "StartTimestampMs")?;
                    self.start_timestamp_ms = d.varint()? as i64;
                }
                2 => {
                    d.expect(wt, 0, "EndTimestampMs")?;
                    self.end_timestamp_ms = d.varint()? as i64;
                }
                3 => {
                    d.expect(wt, 2, "Matchers")?;
                    push_msg(&mut d, &mut self.matchers)?;
                }
                4 => {
                    d.expect(wt, 2, "Hints")?;
                    let b = d.bytes()?;
                    self.hints.get_or_insert_with(ReadHints::default).merge(b)?;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryResult {
    pub timeseries: Vec<TimeSeries>,
}

impl Message for QueryResult {
    fn encoded_len(&self) -> usize {
        msgs_len(1, &self.timeseries)
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        for m in &self.timeseries {
            put_msg(buf, 1, m);
        }
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("QueryResult")?;
            match f {
                1 => {
                    d.expect(wt, 2, "Timeseries")?;
                    push_msg(&mut d, &mut self.timeseries)?;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ChunkedReadResponse {
    pub chunked_series: Vec<ChunkedSeries>,
    pub query_index: i64,
}

impl Message for ChunkedReadResponse {
    fn encoded_len(&self) -> usize {
        msgs_len(1, &self.chunked_series) + int_field_len(2, self.query_index)
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        for m in &self.chunked_series {
            put_msg(buf, 1, m);
        }
        put_int_field(buf, 2, self.query_index);
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("ChunkedReadResponse")?;
            match f {
                1 => {
                    d.expect(wt, 2, "ChunkedSeries")?;
                    push_msg(&mut d, &mut self.chunked_series)?;
                }
                2 => {
                    d.expect(wt, 0, "QueryIndex")?;
                    self.query_index = d.varint()? as i64;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}
