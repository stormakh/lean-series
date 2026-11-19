//! Port of `prompb/io/prometheus/client`: the `io.prometheus.client` metric model (`metrics.proto`) and the
//! streaming decoder of `decoder.go` (`MetricStreamingDecoder`), with a hand-written protobuf decoder that
//! follows the gogo-generated `Unmarshal` code (same merge semantics and error texts).
//!
//! The decoder reuses its structs between metrics. Strings (names, help, label pairs) are byte ranges into the
//! input buffer, so decoding allocates nothing once the reused vectors are warm.

use std::borrow::Cow;
use std::ops::Range;

/// A decoding error with Go's text.
pub type DecodeError = Cow<'static, str>;

pub const ERR_INVALID_LENGTH: &str = "proto: negative length found during unmarshaling";
pub const ERR_INT_OVERFLOW: &str = "proto: integer overflow";
pub const ERR_UNEXPECTED_END_OF_GROUP: &str = "proto: unexpected end of group";
pub const ERR_UNEXPECTED_EOF: &str = "unexpected EOF";
pub const ERR_INVALID_VARINT: &str = "clientpb: invalid varint encountered";

/// Go `MetricType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct MetricType(pub i32);

impl MetricType {
    pub const COUNTER: MetricType = MetricType(0);
    pub const GAUGE: MetricType = MetricType(1);
    pub const SUMMARY: MetricType = MetricType(2);
    pub const UNTYPED: MetricType = MetricType(3);
    pub const HISTOGRAM: MetricType = MetricType(4);
    pub const GAUGE_HISTOGRAM: MetricType = MetricType(5);

    /// Go `MetricType.String()` (the enum name, or the number for unknown values).
    pub fn name(self) -> Cow<'static, str> {
        match self.0 {
            0 => "COUNTER".into(),
            1 => "GAUGE".into(),
            2 => "SUMMARY".into(),
            3 => "UNTYPED".into(),
            4 => "HISTOGRAM".into(),
            5 => "GAUGE_HISTOGRAM".into(),
            n => n.to_string().into(),
        }
    }
}

/// `google.protobuf.Timestamp`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Timestamp {
    pub seconds: i64,
    pub nanos: i32,
}

/// A label pair as byte ranges into the input.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LabelPair {
    pub name: Range<usize>,
    pub value: Range<usize>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Exemplar {
    pub label: Vec<LabelPair>,
    pub value: f64,
    pub timestamp: Option<Timestamp>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Gauge {
    pub value: f64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Counter {
    pub value: f64,
    pub exemplar: Option<Exemplar>,
    pub start_timestamp: Option<Timestamp>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Quantile {
    pub quantile: f64,
    pub value: f64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Summary {
    pub sample_count: u64,
    pub sample_sum: f64,
    pub quantile: Vec<Quantile>,
    pub start_timestamp: Option<Timestamp>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Untyped {
    pub value: f64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Bucket {
    pub cumulative_count: u64,
    pub cumulative_count_float: f64,
    pub upper_bound: f64,
    pub exemplar: Option<Exemplar>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BucketSpan {
    pub offset: i32,
    pub length: u32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Histogram {
    pub sample_count: u64,
    pub sample_count_float: f64,
    pub sample_sum: f64,
    pub bucket: Vec<Bucket>,
    pub start_timestamp: Option<Timestamp>,
    pub schema: i32,
    pub zero_threshold: f64,
    pub zero_count: u64,
    pub zero_count_float: f64,
    pub negative_span: Vec<BucketSpan>,
    pub negative_delta: Vec<i64>,
    pub negative_count: Vec<f64>,
    pub positive_span: Vec<BucketSpan>,
    pub positive_delta: Vec<i64>,
    pub positive_count: Vec<f64>,
    pub exemplars: Vec<Exemplar>,
}

/// Receives the labels of a metric (Go `unsafeLabelAdder`).
pub trait LabelAdder {
    fn add(&mut self, name: &str, value: &str);
}

impl LabelAdder for model::labels::ScratchBuilder {
    fn add(&mut self, name: &str, value: &str) {
        model::labels::ScratchBuilder::add(self, name, value);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Wire reader.

struct Rd<'a> {
    b: &'a [u8],
    i: usize,
    /// Absolute offset of `b[0]` in the whole input.
    base: usize,
}

impl<'a> Rd<'a> {
    fn new(b: &'a [u8], base: usize) -> Rd<'a> {
        Rd { b, i: 0, base }
    }

    #[inline]
    fn more(&self) -> bool {
        self.i < self.b.len()
    }

    #[inline]
    fn varint(&mut self) -> Result<u64, DecodeError> {
        let mut x: u64 = 0;
        let mut shift = 0u32;
        loop {
            if shift >= 64 {
                return Err(ERR_INT_OVERFLOW.into());
            }
            if self.i >= self.b.len() {
                return Err(ERR_UNEXPECTED_EOF.into());
            }
            let c = self.b[self.i];
            self.i += 1;
            x |= ((c & 0x7F) as u64) << shift;
            if c < 0x80 {
                return Ok(x);
            }
            shift += 7;
        }
    }

    /// Reads a field key. Returns (field number, wire type).
    #[inline]
    fn key(&mut self, msg: &str) -> Result<(i32, u8), DecodeError> {
        let wire = self.varint()?;
        let field = (wire >> 3) as i32;
        let wt = (wire & 7) as u8;
        if wt == 4 {
            return Err(format!("proto: {msg}: wiretype end group for non-group").into());
        }
        if field <= 0 {
            return Err(format!("proto: {msg}: illegal tag {field} (wire type {wire})").into());
        }
        Ok((field, wt))
    }

    /// Length-delimited payload; returns its range relative to `b`.
    #[inline]
    fn len_delim(&mut self) -> Result<Range<usize>, DecodeError> {
        let n = self.varint()? as i64;
        if n < 0 {
            return Err(ERR_INVALID_LENGTH.into());
        }
        let post = (self.i as i64).checked_add(n).ok_or(Cow::Borrowed(ERR_INVALID_LENGTH))?;
        if post as u64 > self.b.len() as u64 {
            return Err(ERR_UNEXPECTED_EOF.into());
        }
        let r = self.i..post as usize;
        self.i = post as usize;
        Ok(r)
    }

    #[inline]
    fn fixed64(&mut self) -> Result<u64, DecodeError> {
        if self.i + 8 > self.b.len() {
            return Err(ERR_UNEXPECTED_EOF.into());
        }
        let v = u64::from_le_bytes(self.b[self.i..self.i + 8].try_into().unwrap());
        self.i += 8;
        Ok(v)
    }

    #[inline]
    fn double(&mut self) -> Result<f64, DecodeError> {
        Ok(f64::from_bits(self.fixed64()?))
    }

    /// Skips an unknown field whose key started at `pre` (Go `skipMetrics`).
    fn skip(&mut self, pre: usize) -> Result<(), DecodeError> {
        self.i = pre;
        let n = skip_metrics(&self.b[pre..])?;
        if pre + n > self.b.len() {
            return Err(ERR_UNEXPECTED_EOF.into());
        }
        self.i += n;
        Ok(())
    }

    fn wrong(wt: u8, field: &str) -> DecodeError {
        format!("proto: wrong wireType = {wt} for field {field}").into()
    }

    #[inline]
    fn abs(&self, r: Range<usize>) -> Range<usize> {
        self.base + r.start..self.base + r.end
    }

    fn sub(&self, r: &Range<usize>) -> Rd<'a> {
        Rd::new(&self.b[r.clone()], self.base + r.start)
    }
}

fn skip_metrics(b: &[u8]) -> Result<usize, DecodeError> {
    let l = b.len();
    let mut i = 0usize;
    let mut depth = 0;
    while i < l {
        let mut wire: u64 = 0;
        let mut shift = 0;
        loop {
            if shift >= 64 {
                return Err(ERR_INT_OVERFLOW.into());
            }
            if i >= l {
                return Err(ERR_UNEXPECTED_EOF.into());
            }
            let c = b[i];
            i += 1;
            wire |= ((c & 0x7F) as u64) << shift;
            if c < 0x80 {
                break;
            }
            shift += 7;
        }
        match wire & 7 {
            0 => {
                let mut shift = 0;
                loop {
                    if shift >= 64 {
                        return Err(ERR_INT_OVERFLOW.into());
                    }
                    if i >= l {
                        return Err(ERR_UNEXPECTED_EOF.into());
                    }
                    i += 1;
                    if b[i - 1] < 0x80 {
                        break;
                    }
                    shift += 7;
                }
            }
            1 => i += 8,
            2 => {
                let mut length: u64 = 0;
                let mut shift = 0;
                loop {
                    if shift >= 64 {
                        return Err(ERR_INT_OVERFLOW.into());
                    }
                    if i >= l {
                        return Err(ERR_UNEXPECTED_EOF.into());
                    }
                    let c = b[i];
                    i += 1;
                    length |= ((c & 0x7F) as u64) << shift;
                    if c < 0x80 {
                        break;
                    }
                    shift += 7;
                }
                if (length as i64) < 0 {
                    return Err(ERR_INVALID_LENGTH.into());
                }
                i = i.checked_add(length as usize).ok_or(Cow::Borrowed(ERR_INVALID_LENGTH))?;
            }
            3 => depth += 1,
            4 => {
                if depth == 0 {
                    return Err(ERR_UNEXPECTED_END_OF_GROUP.into());
                }
                depth -= 1;
            }
            5 => i += 4,
            wt => return Err(format!("proto: illegal wireType {wt}").into()),
        }
        if depth == 0 {
            return Ok(i);
        }
    }
    Err(ERR_UNEXPECTED_EOF.into())
}

#[inline]
fn zigzag64(v: u64) -> i64 {
    ((v >> 1) ^ (((v & 1) as i64) << 63 >> 63) as u64) as i64
}

#[inline]
fn zigzag32(v: u64) -> i32 {
    let v = v as u32;
    ((v >> 1) ^ ((((v & 1) as i32) << 31) >> 31) as u32) as i32
}

// ---------------------------------------------------------------------------------------------------------------
// Message decoders (merge into existing values, like gogo's Unmarshal methods).

fn unmarshal_timestamp(r: &mut Rd<'_>, m: &mut Timestamp) -> Result<(), DecodeError> {
    while r.more() {
        let pre = r.i;
        let (f, wt) = r.key("Timestamp")?;
        match f {
            1 => {
                if wt != 0 {
                    return Err(Rd::wrong(wt, "Seconds"));
                }
                m.seconds = r.varint()? as i64;
            }
            2 => {
                if wt != 0 {
                    return Err(Rd::wrong(wt, "Nanos"));
                }
                m.nanos = r.varint()? as i32;
            }
            _ => r.skip(pre)?,
        }
    }
    Ok(())
}

fn unmarshal_label_pair(r: &mut Rd<'_>, m: &mut LabelPair) -> Result<(), DecodeError> {
    while r.more() {
        let pre = r.i;
        let (f, wt) = r.key("LabelPair")?;
        match f {
            1 => {
                if wt != 2 {
                    return Err(Rd::wrong(wt, "Name"));
                }
                let s = r.len_delim()?;
                m.name = r.abs(s);
            }
            2 => {
                if wt != 2 {
                    return Err(Rd::wrong(wt, "Value"));
                }
                let s = r.len_delim()?;
                m.value = r.abs(s);
            }
            _ => r.skip(pre)?,
        }
    }
    Ok(())
}

fn unmarshal_exemplar(r: &mut Rd<'_>, m: &mut Exemplar) -> Result<(), DecodeError> {
    while r.more() {
        let pre = r.i;
        let (f, wt) = r.key("Exemplar")?;
        match f {
            1 => {
                if wt != 2 {
                    return Err(Rd::wrong(wt, "Label"));
                }
                let s = r.len_delim()?;
                m.label.push(LabelPair::default());
                let n = m.label.len();
                unmarshal_label_pair(&mut r.sub(&s), &mut m.label[n - 1])?;
            }
            2 => {
                if wt != 1 {
                    return Err(Rd::wrong(wt, "Value"));
                }
                m.value = r.double()?;
            }
            3 => {
                if wt != 2 {
                    return Err(Rd::wrong(wt, "Timestamp"));
                }
                let s = r.len_delim()?;
                unmarshal_timestamp(&mut r.sub(&s), m.timestamp.get_or_insert_with(Timestamp::default))?;
            }
            _ => r.skip(pre)?,
        }
    }
    Ok(())
}

fn unmarshal_value(r: &mut Rd<'_>, v: &mut f64, msg: &str) -> Result<(), DecodeError> {
    while r.more() {
        let pre = r.i;
        let (f, wt) = r.key(msg)?;
        match f {
            1 => {
                if wt != 1 {
                    return Err(Rd::wrong(wt, "Value"));
                }
                *v = r.double()?;
            }
            _ => r.skip(pre)?,
        }
    }
    Ok(())
}

fn unmarshal_counter(r: &mut Rd<'_>, m: &mut Counter, pool: &mut ExPool) -> Result<(), DecodeError> {
    while r.more() {
        let pre = r.i;
        let (f, wt) = r.key("Counter")?;
        match f {
            1 => {
                if wt != 1 {
                    return Err(Rd::wrong(wt, "Value"));
                }
                m.value = r.double()?;
            }
            2 => {
                if wt != 2 {
                    return Err(Rd::wrong(wt, "Exemplar"));
                }
                let s = r.len_delim()?;
                unmarshal_exemplar(&mut r.sub(&s), m.exemplar.get_or_insert_with(|| pool.take()))?;
            }
            3 => {
                if wt != 2 {
                    return Err(Rd::wrong(wt, "StartTimestamp"));
                }
                let s = r.len_delim()?;
                unmarshal_timestamp(&mut r.sub(&s), m.start_timestamp.get_or_insert_with(Timestamp::default))?;
            }
            _ => r.skip(pre)?,
        }
    }
    Ok(())
}

fn unmarshal_quantile(r: &mut Rd<'_>, m: &mut Quantile) -> Result<(), DecodeError> {
    while r.more() {
        let pre = r.i;
        let (f, wt) = r.key("Quantile")?;
        match f {
            1 => {
                if wt != 1 {
                    return Err(Rd::wrong(wt, "Quantile"));
                }
                m.quantile = r.double()?;
            }
            2 => {
                if wt != 1 {
                    return Err(Rd::wrong(wt, "Value"));
                }
                m.value = r.double()?;
            }
            _ => r.skip(pre)?,
        }
    }
    Ok(())
}

fn unmarshal_summary(r: &mut Rd<'_>, m: &mut Summary) -> Result<(), DecodeError> {
    while r.more() {
        let pre = r.i;
        let (f, wt) = r.key("Summary")?;
        match f {
            1 => {
                if wt != 0 {
                    return Err(Rd::wrong(wt, "SampleCount"));
                }
                m.sample_count = r.varint()?;
            }
            2 => {
                if wt != 1 {
                    return Err(Rd::wrong(wt, "SampleSum"));
                }
                m.sample_sum = r.double()?;
            }
            3 => {
                if wt != 2 {
                    return Err(Rd::wrong(wt, "Quantile"));
                }
                let s = r.len_delim()?;
                m.quantile.push(Quantile::default());
                let n = m.quantile.len();
                unmarshal_quantile(&mut r.sub(&s), &mut m.quantile[n - 1])?;
            }
            4 => {
                if wt != 2 {
                    return Err(Rd::wrong(wt, "StartTimestamp"));
                }
                let s = r.len_delim()?;
                unmarshal_timestamp(&mut r.sub(&s), m.start_timestamp.get_or_insert_with(Timestamp::default))?;
            }
            _ => r.skip(pre)?,
        }
    }
    Ok(())
}

fn unmarshal_bucket(r: &mut Rd<'_>, m: &mut Bucket, pool: &mut ExPool) -> Result<(), DecodeError> {
    while r.more() {
        let pre = r.i;
        let (f, wt) = r.key("Bucket")?;
        match f {
            1 => {
                if wt != 0 {
                    return Err(Rd::wrong(wt, "CumulativeCount"));
                }
                m.cumulative_count = r.varint()?;
            }
            2 => {
                if wt != 1 {
                    return Err(Rd::wrong(wt, "UpperBound"));
                }
                m.upper_bound = r.double()?;
            }
            3 => {
                if wt != 2 {
                    return Err(Rd::wrong(wt, "Exemplar"));
                }
                let s = r.len_delim()?;
                unmarshal_exemplar(&mut r.sub(&s), m.exemplar.get_or_insert_with(|| pool.take()))?;
            }
            4 => {
                if wt != 1 {
                    return Err(Rd::wrong(wt, "CumulativeCountFloat"));
                }
                m.cumulative_count_float = r.double()?;
            }
            _ => r.skip(pre)?,
        }
    }
    Ok(())
}

fn unmarshal_span(r: &mut Rd<'_>, m: &mut BucketSpan) -> Result<(), DecodeError> {
    while r.more() {
        let pre = r.i;
        let (f, wt) = r.key("BucketSpan")?;
        match f {
            1 => {
                if wt != 0 {
                    return Err(Rd::wrong(wt, "Offset"));
                }
                m.offset = zigzag32(r.varint()?);
            }
            2 => {
                if wt != 0 {
                    return Err(Rd::wrong(wt, "Length"));
                }
                m.length = r.varint()? as u32;
            }
            _ => r.skip(pre)?,
        }
    }
    Ok(())
}

fn packed_sint64(r: &mut Rd<'_>, wt: u8, out: &mut Vec<i64>, field: &str) -> Result<(), DecodeError> {
    match wt {
        0 => out.push(zigzag64(r.varint()?)),
        2 => {
            let s = r.len_delim()?;
            let mut sub = r.sub(&s);
            while sub.more() {
                out.push(zigzag64(sub.varint()?));
            }
        }
        _ => return Err(Rd::wrong(wt, field)),
    }
    Ok(())
}

fn packed_double(r: &mut Rd<'_>, wt: u8, out: &mut Vec<f64>, field: &str) -> Result<(), DecodeError> {
    match wt {
        1 => out.push(r.double()?),
        2 => {
            let s = r.len_delim()?;
            let mut sub = r.sub(&s);
            while sub.more() {
                out.push(sub.double()?);
            }
        }
        _ => return Err(Rd::wrong(wt, field)),
    }
    Ok(())
}

fn unmarshal_histogram(r: &mut Rd<'_>, m: &mut Histogram, pool: &mut ExPool) -> Result<(), DecodeError> {
    while r.more() {
        let pre = r.i;
        let (f, wt) = r.key("Histogram")?;
        match f {
            1 => {
                if wt != 0 {
                    return Err(Rd::wrong(wt, "SampleCount"));
                }
                m.sample_count = r.varint()?;
            }
            2 => {
                if wt != 1 {
                    return Err(Rd::wrong(wt, "SampleSum"));
                }
                m.sample_sum = r.double()?;
            }
            3 => {
                if wt != 2 {
                    return Err(Rd::wrong(wt, "Bucket"));
                }
                let s = r.len_delim()?;
                m.bucket.push(Bucket::default());
                let n = m.bucket.len();
                unmarshal_bucket(&mut r.sub(&s), &mut m.bucket[n - 1], pool)?;
            }
            4 => {
                if wt != 1 {
                    return Err(Rd::wrong(wt, "SampleCountFloat"));
                }
                m.sample_count_float = r.double()?;
            }
            5 => {
                if wt != 0 {
                    return Err(Rd::wrong(wt, "Schema"));
                }
                m.schema = zigzag32(r.varint()?);
            }
            6 => {
                if wt != 1 {
                    return Err(Rd::wrong(wt, "ZeroThreshold"));
                }
                m.zero_threshold = r.double()?;
            }
            7 => {
                if wt != 0 {
                    return Err(Rd::wrong(wt, "ZeroCount"));
                }
                m.zero_count = r.varint()?;
            }
            8 => {
                if wt != 1 {
                    return Err(Rd::wrong(wt, "ZeroCountFloat"));
                }
                m.zero_count_float = r.double()?;
            }
            9 => {
                if wt != 2 {
                    return Err(Rd::wrong(wt, "NegativeSpan"));
                }
                let s = r.len_delim()?;
                m.negative_span.push(BucketSpan::default());
                let n = m.negative_span.len();
                unmarshal_span(&mut r.sub(&s), &mut m.negative_span[n - 1])?;
            }
            10 => packed_sint64(r, wt, &mut m.negative_delta, "NegativeDelta")?,
            11 => packed_double(r, wt, &mut m.negative_count, "NegativeCount")?,
            12 => {
                if wt != 2 {
                    return Err(Rd::wrong(wt, "PositiveSpan"));
                }
                let s = r.len_delim()?;
                m.positive_span.push(BucketSpan::default());
                let n = m.positive_span.len();
                unmarshal_span(&mut r.sub(&s), &mut m.positive_span[n - 1])?;
            }
            13 => packed_sint64(r, wt, &mut m.positive_delta, "PositiveDelta")?,
            14 => packed_double(r, wt, &mut m.positive_count, "PositiveCount")?,
            15 => {
                if wt != 2 {
                    return Err(Rd::wrong(wt, "StartTimestamp"));
                }
                let s = r.len_delim()?;
                unmarshal_timestamp(&mut r.sub(&s), m.start_timestamp.get_or_insert_with(Timestamp::default))?;
            }
            16 => {
                if wt != 2 {
                    return Err(Rd::wrong(wt, "Exemplars"));
                }
                let s = r.len_delim()?;
                m.exemplars.push(pool.take());
                let n = m.exemplars.len();
                unmarshal_exemplar(&mut r.sub(&s), &mut m.exemplars[n - 1])?;
            }
            _ => r.skip(pre)?,
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------------------------
// MetricStreamingDecoder.

/// Decodes varint-delimited `MetricFamily` messages one family and one metric at a time (Go
/// `MetricStreamingDecoder`). Like Go, the per-type values are reused: `counter()` still returns the zeroed
/// counter of an earlier metric for a gauge metric, and `untyped` is never reset.
#[derive(Default)]
pub struct MetricStreamingDecoder<'a> {
    input: &'a [u8],
    in_pos: usize,

    // MetricFamily (without metrics).
    pub name: Range<usize>,
    pub help: Range<usize>,
    pub typ: MetricType,
    pub unit: Range<usize>,

    // Metric (without labels).
    pub gauge: Gauge,
    pub counter: Counter,
    pub summary: Summary,
    pub untyped: Untyped,
    pub histogram: Histogram,
    pub timestamp_ms: i64,

    mf_data: Range<usize>,
    metrics: Vec<Range<usize>>,
    metric_index: usize,
    labels: Vec<Range<usize>>,
    ex_pool: ExPool,
}

/// Exemplars kept for reuse, so decoding allocates nothing once warm (Go allocates new ones).
#[derive(Default)]
struct ExPool(Vec<Exemplar>);

impl ExPool {
    #[inline]
    fn take(&mut self) -> Exemplar {
        self.0.pop().unwrap_or_default()
    }

    #[inline]
    fn give(&mut self, mut e: Exemplar) {
        e.label.clear();
        e.value = 0.0;
        e.timestamp = None;
        self.0.push(e);
    }
}

impl<'a> MetricStreamingDecoder<'a> {
    /// Go `NewMetricStreamingDecoder`.
    pub fn new(data: &'a [u8]) -> MetricStreamingDecoder<'a> {
        MetricStreamingDecoder { input: data, metrics: Vec::with_capacity(100), ..Default::default() }
    }

    /// The input bytes.
    pub fn input(&self) -> &'a [u8] {
        self.input
    }

    #[inline]
    fn str_at(&self, r: &Range<usize>) -> &'a [u8] {
        &self.input[r.clone()]
    }

    pub fn get_name(&self) -> &'a [u8] {
        self.str_at(&self.name)
    }

    pub fn get_help(&self) -> &'a [u8] {
        self.str_at(&self.help)
    }

    pub fn get_unit(&self) -> &'a [u8] {
        self.str_at(&self.unit)
    }

    pub fn get_type(&self) -> MetricType {
        self.typ
    }

    /// Bytes of a range returned in decoded structs (label names and values).
    pub fn bytes(&self, r: &Range<usize>) -> &'a [u8] {
        self.str_at(r)
    }

    /// Go `NextMetricFamily`: decodes the next family without its metrics. Err("EOF") is `Err(None)`.
    pub fn next_metric_family(&mut self) -> Result<(), Option<DecodeError>> {
        let b = &self.input[self.in_pos..];
        if b.is_empty() {
            return Err(None);
        }
        // gogo proto.DecodeVarint: up to 10 bytes, (0, 0) on failure.
        let mut x: u64 = 0;
        let mut n = 0usize;
        let mut shift = 0;
        let mut ok = false;
        while shift < 64 {
            if n >= b.len() {
                break;
            }
            let c = b[n] as u64;
            n += 1;
            x |= (c & 0x7F) << shift;
            if c & 0x80 == 0 {
                ok = true;
                break;
            }
            shift += 7;
        }
        if !ok || n > 5 {
            return Err(Some(ERR_INVALID_VARINT.into()));
        }
        let total = n + x as usize;
        if total > b.len() {
            return Err(Some(
                format!(
                    "clientpb: insufficient length of buffer, expected at least {total} bytes, got {} bytes",
                    b.len()
                )
                .into(),
            ));
        }
        self.metrics.clear();
        self.metric_index = 0;
        self.name = 0..0;
        self.help = 0..0;
        self.typ = MetricType(0);
        self.unit = 0..0;
        let start = self.in_pos + n;
        self.mf_data = start..self.in_pos + total;
        self.in_pos += total;
        self.unmarshal_without_metrics().map_err(Some)
    }

    fn unmarshal_without_metrics(&mut self) -> Result<(), DecodeError> {
        let mut r = Rd::new(&self.input[self.mf_data.clone()], self.mf_data.start);
        while r.more() {
            let pre = r.i;
            let (f, wt) = r.key("MetricFamily")?;
            match f {
                1 => {
                    if wt != 2 {
                        return Err(Rd::wrong(wt, "Name"));
                    }
                    let s = r.len_delim()?;
                    self.name = r.abs(s);
                }
                2 => {
                    if wt != 2 {
                        return Err(Rd::wrong(wt, "Help"));
                    }
                    let s = r.len_delim()?;
                    self.help = r.abs(s);
                }
                3 => {
                    if wt != 0 {
                        return Err(Rd::wrong(wt, "Type"));
                    }
                    self.typ = MetricType(r.varint()? as i32);
                }
                4 => {
                    if wt != 2 {
                        return Err(Rd::wrong(wt, "Metric"));
                    }
                    let s = r.len_delim()?;
                    self.metrics.push(r.abs(s));
                }
                5 => {
                    if wt != 2 {
                        return Err(Rd::wrong(wt, "Unit"));
                    }
                    let s = r.len_delim()?;
                    self.unit = r.abs(s);
                }
                _ => r.skip(pre)?,
            }
        }
        Ok(())
    }

    /// Go `NextMetric`: decodes the next metric of the family (without labels). `Err(None)` is EOF.
    pub fn next_metric(&mut self) -> Result<(), Option<DecodeError>> {
        if self.metric_index >= self.metrics.len() {
            return Err(None);
        }
        self.reset_metric();
        let m = self.metrics[self.metric_index].clone();
        self.unmarshal_without_labels(m).map_err(Some)?;
        self.metric_index += 1;
        Ok(())
    }

    fn reset_metric(&mut self) {
        self.labels.clear();
        self.timestamp_ms = 0;
        self.counter.value = 0.0;
        self.counter.start_timestamp = None;
        if let Some(e) = self.counter.exemplar.take() {
            self.ex_pool.give(e);
        }
        self.gauge.value = 0.0;
        let h = &mut self.histogram;
        h.sample_count = 0;
        h.sample_count_float = 0.0;
        h.sample_sum = 0.0;
        for b in h.bucket.drain(..) {
            if let Some(e) = b.exemplar {
                self.ex_pool.give(e);
            }
        }
        h.start_timestamp = None;
        h.schema = 0;
        h.zero_threshold = 0.0;
        h.zero_count = 0;
        h.zero_count_float = 0.0;
        h.negative_span.clear();
        h.negative_delta.clear();
        h.negative_count.clear();
        h.positive_span.clear();
        h.positive_delta.clear();
        h.positive_count.clear();
        for e in h.exemplars.drain(..) {
            self.ex_pool.give(e);
        }
        let s = &mut self.summary;
        s.sample_count = 0;
        s.sample_sum = 0.0;
        s.quantile.clear();
        s.start_timestamp = None;
    }

    fn unmarshal_without_labels(&mut self, m: Range<usize>) -> Result<(), DecodeError> {
        let mut r = Rd::new(&self.input[m.clone()], m.start);
        while r.more() {
            let pre = r.i;
            let (f, wt) = r.key("Metric")?;
            match f {
                1 => {
                    if wt != 2 {
                        return Err(Rd::wrong(wt, "Label"));
                    }
                    let s = r.len_delim()?;
                    self.labels.push(r.abs(s));
                }
                2 => {
                    if wt != 2 {
                        return Err(Rd::wrong(wt, "Gauge"));
                    }
                    let s = r.len_delim()?;
                    unmarshal_value(&mut r.sub(&s), &mut self.gauge.value, "Gauge")?;
                }
                3 => {
                    if wt != 2 {
                        return Err(Rd::wrong(wt, "Counter"));
                    }
                    let s = r.len_delim()?;
                    unmarshal_counter(&mut r.sub(&s), &mut self.counter, &mut self.ex_pool)?;
                }
                4 => {
                    if wt != 2 {
                        return Err(Rd::wrong(wt, "Summary"));
                    }
                    let s = r.len_delim()?;
                    unmarshal_summary(&mut r.sub(&s), &mut self.summary)?;
                }
                5 => {
                    if wt != 2 {
                        return Err(Rd::wrong(wt, "Untyped"));
                    }
                    let s = r.len_delim()?;
                    unmarshal_value(&mut r.sub(&s), &mut self.untyped.value, "Untyped")?;
                }
                6 => {
                    if wt != 0 {
                        return Err(Rd::wrong(wt, "TimestampMs"));
                    }
                    self.timestamp_ms = r.varint()? as i64;
                }
                7 => {
                    if wt != 2 {
                        return Err(Rd::wrong(wt, "Histogram"));
                    }
                    let s = r.len_delim()?;
                    unmarshal_histogram(&mut r.sub(&s), &mut self.histogram, &mut self.ex_pool)?;
                }
                _ => r.skip(pre)?,
            }
        }
        Ok(())
    }

    /// Go `Label`: parses the labels of the current metric into `b` (the metric name is not among them).
    pub fn label(&self, b: &mut dyn LabelAdder) -> Result<(), DecodeError> {
        for l in &self.labels {
            parse_label(self.input, l.clone(), b)?;
        }
        Ok(())
    }

    /// Number of labels of the current metric.
    pub fn num_labels(&self) -> usize {
        self.labels.len()
    }
}

/// Go `parseLabel`: `LabelPair.Unmarshal` directly into the adder, validating name and value.
fn parse_label(input: &[u8], r: Range<usize>, b: &mut dyn LabelAdder) -> Result<(), DecodeError> {
    let mut rd = Rd::new(&input[r.clone()], r.start);
    let mut name: &str = "";
    let mut value: &str = "";
    while rd.more() {
        let pre = rd.i;
        let (f, wt) = rd.key("LabelPair")?;
        match f {
            1 => {
                if wt != 2 {
                    return Err(Rd::wrong(wt, "Name"));
                }
                let s = rd.len_delim()?;
                let raw = &input[rd.abs(s)];
                match std::str::from_utf8(raw) {
                    Ok(n) if !n.is_empty() => name = n,
                    _ => {
                        return Err(format!("invalid label name: {}", String::from_utf8_lossy(raw)).into());
                    }
                }
            }
            2 => {
                if wt != 2 {
                    return Err(Rd::wrong(wt, "Value"));
                }
                let s = rd.len_delim()?;
                let raw = &input[rd.abs(s)];
                match std::str::from_utf8(raw) {
                    Ok(v) => value = v,
                    Err(_) => {
                        return Err(format!("invalid label value: {}", String::from_utf8_lossy(raw)).into());
                    }
                }
            }
            _ => rd.skip(pre)?,
        }
    }
    b.add(name, value);
    Ok(())
}
