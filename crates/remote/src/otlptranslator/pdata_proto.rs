//! Hand-written protobuf codec for OTLP metrics (`opentelemetry/proto/collector/metrics/v1`
//! `ExportMetricsServiceRequest` and the `metrics/v1`, `resource/v1` and `common/v1` messages it contains).
//!
//! Decoding follows pdata's generated `UnmarshalProto` code (v1.69.0): the same accepted wire types (packed and
//! unpacked repeated scalars), the same error texts (`proto: wrong wireType = %d for field X`, `unexpected EOF`,
//! ...), unknown fields skipped, and the migration of the deprecated `instrumentation_library_metrics` field
//! (1000) to `scope_metrics`. Encoding writes fields in field-number order, skips proto3 zero scalars and always
//! writes set `oneof`/`optional` fields.

use std::borrow::Cow;

use crate::prompb::proto::{DecodeError, Decoder, put_tag, put_varint};

use super::pdata::*;

type Result<T> = std::result::Result<T, DecodeError>;

const WT_VARINT: u8 = 0;
const WT_I64: u8 = 1;
const WT_LEN: u8 = 2;
const WT_START_GROUP: u8 = 3;
const WT_END_GROUP: u8 = 4;
const WT_I32: u8 = 5;

fn err(msg: String) -> DecodeError {
    DecodeError(Cow::Owned(msg))
}

fn wrong_wire_type(wt: u8, field: &str) -> DecodeError {
    err(format!("proto: wrong wireType = {wt} for field {field}"))
}

/// pdata `proto.ConsumeTag`.
fn tag(d: &mut Decoder<'_>) -> Result<(i32, u8)> {
    let t = d.varint()?;
    let field = (t >> 3) as i32;
    let wt = (t & 7) as u8;
    if field <= 0 {
        return Err(err(format!("proto: Link: illegal field={} (tag={}, pos={})", field, t, d.i)));
    }
    Ok((field, wt))
}

fn fixed32(d: &mut Decoder<'_>) -> Result<u32> {
    if d.i + 4 > d.b.len() {
        return Err(DecodeError::eof());
    }
    let mut a = [0u8; 4];
    a.copy_from_slice(&d.b[d.i..d.i + 4]);
    d.i += 4;
    Ok(u32::from_le_bytes(a))
}

/// pdata `proto.ConsumeUnknown`.
fn consume_unknown(d: &mut Decoder<'_>, mut wt: u8) -> Result<()> {
    let mut depth = 0;
    while !d.done() {
        match wt {
            WT_VARINT => {
                d.varint()?;
                return Ok(());
            }
            WT_I64 => {
                d.fixed64()?;
                return Ok(());
            }
            WT_LEN => {
                d.bytes()?;
                return Ok(());
            }
            WT_START_GROUP => depth += 1,
            WT_END_GROUP => {
                if depth == 0 {
                    return Err(err("proto: unexpected end of group".to_string()));
                }
                depth -= 1;
            }
            WT_I32 => {
                fixed32(d)?;
                return Ok(());
            }
            _ => return Err(err(format!("proto: illegal wireType {wt}"))),
        }
        if depth == 0 {
            return Ok(());
        }
        let (_, w) = tag(d)?;
        wt = w;
    }
    Err(DecodeError::eof())
}

fn want(wt: u8, want: u8, field: &str) -> Result<()> {
    if wt != want { Err(wrong_wire_type(wt, field)) } else { Ok(()) }
}

fn string(d: &mut Decoder<'_>, wt: u8, field: &str) -> Result<String> {
    want(wt, WT_LEN, field)?;
    d.string()
}

fn message<'a>(d: &mut Decoder<'a>, wt: u8, field: &str) -> Result<&'a [u8]> {
    want(wt, WT_LEN, field)?;
    d.bytes()
}

fn u64_varint(d: &mut Decoder<'_>, wt: u8, field: &str) -> Result<u64> {
    want(wt, WT_VARINT, field)?;
    d.varint()
}

fn i64_fixed(d: &mut Decoder<'_>, wt: u8, field: &str) -> Result<u64> {
    want(wt, WT_I64, field)?;
    d.fixed64()
}

fn zigzag32(v: u64) -> i32 {
    ((v >> 1) as u32 ^ (((v & 1) << 31) as i32 >> 31) as u32) as i32
}

/// A repeated fixed64 or double field: packed replaces the slice (as pdata does), unpacked appends.
fn repeated_fixed64(d: &mut Decoder<'_>, wt: u8, field: &str, out: &mut Vec<u64>) -> Result<()> {
    match wt {
        WT_LEN => {
            let b = d.bytes()?;
            let size = b.len() / 8;
            out.clear();
            out.reserve(size);
            for c in b.chunks_exact(8) {
                out.push(u64::from_le_bytes(c.try_into().unwrap()));
            }
            if size * 8 != b.len() {
                return Err(err(format!("proto: invalid field len = {} for field {}", b.len() - size * 8, field)));
            }
            Ok(())
        }
        WT_I64 => {
            out.push(d.fixed64()?);
            Ok(())
        }
        _ => Err(wrong_wire_type(wt, field)),
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Decoding

/// Decodes an `ExportMetricsServiceRequest` (Go `pmetricotlp.ExportRequest.UnmarshalProto`).
pub fn unmarshal_export_request(buf: &[u8]) -> Result<Metrics> {
    let mut md = Metrics::new();
    let mut d = Decoder::new(buf);
    while !d.done() {
        let (f, wt) = tag(&mut d)?;
        match f {
            1 => {
                let b = message(&mut d, wt, "ResourceMetrics")?;
                md.resource_metrics.push(resource_metrics(b)?);
            }
            _ => consume_unknown(&mut d, wt)?,
        }
    }
    Ok(md)
}

fn resource_metrics(buf: &[u8]) -> Result<ResourceMetrics> {
    let mut rm = ResourceMetrics::default();
    let mut deprecated: Vec<ScopeMetrics> = Vec::new();
    let mut d = Decoder::new(buf);
    while !d.done() {
        let (f, wt) = tag(&mut d)?;
        match f {
            1 => rm.resource = resource(message(&mut d, wt, "Resource")?)?,
            2 => rm.scope_metrics.push(scope_metrics(message(&mut d, wt, "ScopeMetrics")?)?),
            3 => rm.schema_url = string(&mut d, wt, "SchemaUrl")?,
            1000 => deprecated.push(scope_metrics(message(&mut d, wt, "DeprecatedScopeMetrics")?)?),
            _ => consume_unknown(&mut d, wt)?,
        }
    }
    // otlp.MigrateMetrics
    if rm.scope_metrics.is_empty() {
        rm.scope_metrics = deprecated;
    }
    Ok(rm)
}

fn resource(buf: &[u8]) -> Result<Resource> {
    let mut r = Resource::default();
    let mut d = Decoder::new(buf);
    while !d.done() {
        let (f, wt) = tag(&mut d)?;
        match f {
            1 => key_value_into(message(&mut d, wt, "Attributes")?, &mut r.attributes)?,
            2 => r.dropped_attributes_count = u64_varint(&mut d, wt, "DroppedAttributesCount")? as u32,
            3 => {
                message(&mut d, wt, "EntityRefs")?;
            }
            _ => consume_unknown(&mut d, wt)?,
        }
    }
    Ok(r)
}

fn scope_metrics(buf: &[u8]) -> Result<ScopeMetrics> {
    let mut sm = ScopeMetrics::default();
    let mut d = Decoder::new(buf);
    while !d.done() {
        let (f, wt) = tag(&mut d)?;
        match f {
            1 => sm.scope = scope(message(&mut d, wt, "Scope")?)?,
            2 => sm.metrics.push(metric(message(&mut d, wt, "Metrics")?)?),
            3 => sm.schema_url = string(&mut d, wt, "SchemaUrl")?,
            _ => consume_unknown(&mut d, wt)?,
        }
    }
    Ok(sm)
}

fn scope(buf: &[u8]) -> Result<InstrumentationScope> {
    let mut s = InstrumentationScope::default();
    let mut d = Decoder::new(buf);
    while !d.done() {
        let (f, wt) = tag(&mut d)?;
        match f {
            1 => s.name = string(&mut d, wt, "Name")?,
            2 => s.version = string(&mut d, wt, "Version")?,
            3 => key_value_into(message(&mut d, wt, "Attributes")?, &mut s.attributes)?,
            4 => s.dropped_attributes_count = u64_varint(&mut d, wt, "DroppedAttributesCount")? as u32,
            _ => consume_unknown(&mut d, wt)?,
        }
    }
    Ok(s)
}

fn key_value_into(buf: &[u8], m: &mut Map) -> Result<()> {
    let mut key = String::new();
    let mut value = Value::Empty;
    let mut d = Decoder::new(buf);
    while !d.done() {
        let (f, wt) = tag(&mut d)?;
        match f {
            1 => key = string(&mut d, wt, "Key")?,
            2 => value = any_value(message(&mut d, wt, "Value")?)?,
            3 => {
                u64_varint(&mut d, wt, "KeyStrindex")?;
            }
            _ => consume_unknown(&mut d, wt)?,
        }
    }
    m.0.push((key, value));
    Ok(())
}

fn any_value(buf: &[u8]) -> Result<Value> {
    let mut v = Value::Empty;
    let mut d = Decoder::new(buf);
    while !d.done() {
        let (f, wt) = tag(&mut d)?;
        match f {
            1 => v = Value::Str(string(&mut d, wt, "StringValue")?),
            2 => v = Value::Bool(u64_varint(&mut d, wt, "BoolValue")? != 0),
            3 => v = Value::Int(u64_varint(&mut d, wt, "IntValue")? as i64),
            4 => v = Value::Double(f64::from_bits(i64_fixed(&mut d, wt, "DoubleValue")?)),
            5 => {
                let b = message(&mut d, wt, "ArrayValue")?;
                let mut items = Vec::new();
                let mut ad = Decoder::new(b);
                while !ad.done() {
                    let (f, wt) = tag(&mut ad)?;
                    match f {
                        1 => items.push(any_value(message(&mut ad, wt, "Values")?)?),
                        _ => consume_unknown(&mut ad, wt)?,
                    }
                }
                v = Value::Slice(items);
            }
            6 => {
                let b = message(&mut d, wt, "KvlistValue")?;
                let mut m = Map::new();
                let mut kd = Decoder::new(b);
                while !kd.done() {
                    let (f, wt) = tag(&mut kd)?;
                    match f {
                        1 => key_value_into(message(&mut kd, wt, "Values")?, &mut m)?,
                        _ => consume_unknown(&mut kd, wt)?,
                    }
                }
                v = Value::Map(m);
            }
            7 => v = Value::Bytes(message(&mut d, wt, "BytesValue")?.to_vec()),
            8 => {
                u64_varint(&mut d, wt, "StringValueStrindex")?;
            }
            _ => consume_unknown(&mut d, wt)?,
        }
    }
    Ok(v)
}

fn metric(buf: &[u8]) -> Result<Metric> {
    let mut m = Metric::default();
    let mut d = Decoder::new(buf);
    while !d.done() {
        let (f, wt) = tag(&mut d)?;
        match f {
            1 => m.name = string(&mut d, wt, "Name")?,
            2 => m.description = string(&mut d, wt, "Description")?,
            3 => m.unit = string(&mut d, wt, "Unit")?,
            5 => {
                let b = message(&mut d, wt, "Gauge")?;
                let mut g = Gauge::default();
                let mut gd = Decoder::new(b);
                while !gd.done() {
                    let (f, wt) = tag(&mut gd)?;
                    match f {
                        1 => g.data_points.push(number_data_point(message(&mut gd, wt, "DataPoints")?)?),
                        _ => consume_unknown(&mut gd, wt)?,
                    }
                }
                m.data = MetricData::Gauge(g);
            }
            7 => {
                let b = message(&mut d, wt, "Sum")?;
                let mut s = Sum::default();
                let mut sd = Decoder::new(b);
                while !sd.done() {
                    let (f, wt) = tag(&mut sd)?;
                    match f {
                        1 => s.data_points.push(number_data_point(message(&mut sd, wt, "DataPoints")?)?),
                        2 => {
                            s.aggregation_temporality =
                                AggregationTemporality(u64_varint(&mut sd, wt, "AggregationTemporality")? as i32)
                        }
                        3 => s.is_monotonic = u64_varint(&mut sd, wt, "IsMonotonic")? != 0,
                        _ => consume_unknown(&mut sd, wt)?,
                    }
                }
                m.data = MetricData::Sum(s);
            }
            9 => {
                let b = message(&mut d, wt, "Histogram")?;
                let mut h = Histogram::default();
                let mut hd = Decoder::new(b);
                while !hd.done() {
                    let (f, wt) = tag(&mut hd)?;
                    match f {
                        1 => h.data_points.push(histogram_data_point(message(&mut hd, wt, "DataPoints")?)?),
                        2 => {
                            h.aggregation_temporality =
                                AggregationTemporality(u64_varint(&mut hd, wt, "AggregationTemporality")? as i32)
                        }
                        _ => consume_unknown(&mut hd, wt)?,
                    }
                }
                m.data = MetricData::Histogram(h);
            }
            10 => {
                let b = message(&mut d, wt, "ExponentialHistogram")?;
                let mut h = ExponentialHistogram::default();
                let mut hd = Decoder::new(b);
                while !hd.done() {
                    let (f, wt) = tag(&mut hd)?;
                    match f {
                        1 => h.data_points.push(exp_histogram_data_point(message(&mut hd, wt, "DataPoints")?)?),
                        2 => {
                            h.aggregation_temporality =
                                AggregationTemporality(u64_varint(&mut hd, wt, "AggregationTemporality")? as i32)
                        }
                        _ => consume_unknown(&mut hd, wt)?,
                    }
                }
                m.data = MetricData::ExponentialHistogram(h);
            }
            11 => {
                let b = message(&mut d, wt, "Summary")?;
                let mut s = Summary::default();
                let mut sd = Decoder::new(b);
                while !sd.done() {
                    let (f, wt) = tag(&mut sd)?;
                    match f {
                        1 => s.data_points.push(summary_data_point(message(&mut sd, wt, "DataPoints")?)?),
                        _ => consume_unknown(&mut sd, wt)?,
                    }
                }
                m.data = MetricData::Summary(s);
            }
            12 => key_value_into(message(&mut d, wt, "Metadata")?, &mut m.metadata)?,
            _ => consume_unknown(&mut d, wt)?,
        }
    }
    Ok(m)
}

fn number_data_point(buf: &[u8]) -> Result<NumberDataPoint> {
    let mut p = NumberDataPoint::default();
    let mut d = Decoder::new(buf);
    while !d.done() {
        let (f, wt) = tag(&mut d)?;
        match f {
            7 => key_value_into(message(&mut d, wt, "Attributes")?, &mut p.attributes)?,
            2 => p.start_timestamp = i64_fixed(&mut d, wt, "StartTimeUnixNano")?,
            3 => p.timestamp = i64_fixed(&mut d, wt, "TimeUnixNano")?,
            4 => p.value = NumberValue::Double(f64::from_bits(i64_fixed(&mut d, wt, "AsDouble")?)),
            6 => p.value = NumberValue::Int(i64_fixed(&mut d, wt, "AsInt")? as i64),
            5 => p.exemplars.push(exemplar(message(&mut d, wt, "Exemplars")?)?),
            8 => p.flags = DataPointFlags(u64_varint(&mut d, wt, "Flags")? as u32),
            _ => consume_unknown(&mut d, wt)?,
        }
    }
    Ok(p)
}

fn histogram_data_point(buf: &[u8]) -> Result<HistogramDataPoint> {
    let mut p = HistogramDataPoint::default();
    let mut d = Decoder::new(buf);
    while !d.done() {
        let (f, wt) = tag(&mut d)?;
        match f {
            9 => key_value_into(message(&mut d, wt, "Attributes")?, &mut p.attributes)?,
            2 => p.start_timestamp = i64_fixed(&mut d, wt, "StartTimeUnixNano")?,
            3 => p.timestamp = i64_fixed(&mut d, wt, "TimeUnixNano")?,
            4 => p.count = i64_fixed(&mut d, wt, "Count")?,
            5 => p.sum = Some(f64::from_bits(i64_fixed(&mut d, wt, "Sum")?)),
            6 => repeated_fixed64(&mut d, wt, "BucketCounts", &mut p.bucket_counts)?,
            7 => {
                let mut bits: Vec<u64> = p.explicit_bounds.iter().map(|f| f.to_bits()).collect();
                repeated_fixed64(&mut d, wt, "ExplicitBounds", &mut bits)?;
                p.explicit_bounds = bits.into_iter().map(f64::from_bits).collect();
            }
            8 => p.exemplars.push(exemplar(message(&mut d, wt, "Exemplars")?)?),
            10 => p.flags = DataPointFlags(u64_varint(&mut d, wt, "Flags")? as u32),
            11 => p.min = Some(f64::from_bits(i64_fixed(&mut d, wt, "Min")?)),
            12 => p.max = Some(f64::from_bits(i64_fixed(&mut d, wt, "Max")?)),
            _ => consume_unknown(&mut d, wt)?,
        }
    }
    Ok(p)
}

fn buckets(buf: &[u8]) -> Result<Buckets> {
    let mut b = Buckets::default();
    let mut d = Decoder::new(buf);
    while !d.done() {
        let (f, wt) = tag(&mut d)?;
        match f {
            1 => b.offset = zigzag32(u64_varint(&mut d, wt, "Offset")?),
            2 => match wt {
                WT_LEN => {
                    let p = d.bytes()?;
                    let mut pd = Decoder::new(p);
                    while !pd.done() {
                        b.bucket_counts.push(pd.varint()?);
                    }
                }
                WT_VARINT => b.bucket_counts.push(d.varint()?),
                _ => return Err(wrong_wire_type(wt, "BucketCounts")),
            },
            _ => consume_unknown(&mut d, wt)?,
        }
    }
    Ok(b)
}

fn exp_histogram_data_point(buf: &[u8]) -> Result<ExponentialHistogramDataPoint> {
    let mut p = ExponentialHistogramDataPoint::default();
    let mut d = Decoder::new(buf);
    while !d.done() {
        let (f, wt) = tag(&mut d)?;
        match f {
            1 => key_value_into(message(&mut d, wt, "Attributes")?, &mut p.attributes)?,
            2 => p.start_timestamp = i64_fixed(&mut d, wt, "StartTimeUnixNano")?,
            3 => p.timestamp = i64_fixed(&mut d, wt, "TimeUnixNano")?,
            4 => p.count = i64_fixed(&mut d, wt, "Count")?,
            5 => p.sum = Some(f64::from_bits(i64_fixed(&mut d, wt, "Sum")?)),
            6 => p.scale = zigzag32(u64_varint(&mut d, wt, "Scale")?),
            7 => p.zero_count = i64_fixed(&mut d, wt, "ZeroCount")?,
            8 => p.positive = buckets(message(&mut d, wt, "Positive")?)?,
            9 => p.negative = buckets(message(&mut d, wt, "Negative")?)?,
            10 => p.flags = DataPointFlags(u64_varint(&mut d, wt, "Flags")? as u32),
            11 => p.exemplars.push(exemplar(message(&mut d, wt, "Exemplars")?)?),
            12 => p.min = Some(f64::from_bits(i64_fixed(&mut d, wt, "Min")?)),
            13 => p.max = Some(f64::from_bits(i64_fixed(&mut d, wt, "Max")?)),
            14 => p.zero_threshold = f64::from_bits(i64_fixed(&mut d, wt, "ZeroThreshold")?),
            _ => consume_unknown(&mut d, wt)?,
        }
    }
    Ok(p)
}

fn summary_data_point(buf: &[u8]) -> Result<SummaryDataPoint> {
    let mut p = SummaryDataPoint::default();
    let mut d = Decoder::new(buf);
    while !d.done() {
        let (f, wt) = tag(&mut d)?;
        match f {
            7 => key_value_into(message(&mut d, wt, "Attributes")?, &mut p.attributes)?,
            2 => p.start_timestamp = i64_fixed(&mut d, wt, "StartTimeUnixNano")?,
            3 => p.timestamp = i64_fixed(&mut d, wt, "TimeUnixNano")?,
            4 => p.count = i64_fixed(&mut d, wt, "Count")?,
            5 => p.sum = f64::from_bits(i64_fixed(&mut d, wt, "Sum")?),
            6 => {
                let b = message(&mut d, wt, "QuantileValues")?;
                let mut q = ValueAtQuantile::default();
                let mut qd = Decoder::new(b);
                while !qd.done() {
                    let (f, wt) = tag(&mut qd)?;
                    match f {
                        1 => q.quantile = f64::from_bits(i64_fixed(&mut qd, wt, "Quantile")?),
                        2 => q.value = f64::from_bits(i64_fixed(&mut qd, wt, "Value")?),
                        _ => consume_unknown(&mut qd, wt)?,
                    }
                }
                p.quantile_values.push(q);
            }
            8 => p.flags = DataPointFlags(u64_varint(&mut d, wt, "Flags")? as u32),
            _ => consume_unknown(&mut d, wt)?,
        }
    }
    Ok(p)
}

fn exemplar(buf: &[u8]) -> Result<Exemplar> {
    let mut e = Exemplar::default();
    let mut d = Decoder::new(buf);
    while !d.done() {
        let (f, wt) = tag(&mut d)?;
        match f {
            7 => key_value_into(message(&mut d, wt, "FilteredAttributes")?, &mut e.filtered_attributes)?,
            2 => e.timestamp = i64_fixed(&mut d, wt, "TimeUnixNano")?,
            3 => e.value = NumberValue::Double(f64::from_bits(i64_fixed(&mut d, wt, "AsDouble")?)),
            6 => e.value = NumberValue::Int(i64_fixed(&mut d, wt, "AsInt")? as i64),
            5 => {
                let b = message(&mut d, wt, "TraceId")?;
                if b.is_empty() {
                    e.trace_id = [0; 16];
                } else if b.len() != 16 {
                    return Err(err("unmarshal: invalid TraceID length".to_string()));
                } else {
                    e.trace_id.copy_from_slice(b);
                }
            }
            4 => {
                let b = message(&mut d, wt, "SpanId")?;
                if b.is_empty() {
                    e.span_id = [0; 8];
                } else if b.len() != 8 {
                    return Err(err("unmarshal: invalid SpanID length".to_string()));
                } else {
                    e.span_id.copy_from_slice(b);
                }
            }
            _ => consume_unknown(&mut d, wt)?,
        }
    }
    Ok(e)
}

// ---------------------------------------------------------------------------------------------------------------
// Encoding

fn put_fixed64(buf: &mut Vec<u8>, field: u32, v: u64) {
    put_tag(buf, field, WT_I64);
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_fixed64_nz(buf: &mut Vec<u8>, field: u32, v: u64) {
    if v != 0 {
        put_fixed64(buf, field, v);
    }
}

fn put_double_nz(buf: &mut Vec<u8>, field: u32, v: f64) {
    if v != 0.0 {
        put_fixed64(buf, field, v.to_bits());
    }
}

fn put_varint_nz(buf: &mut Vec<u8>, field: u32, v: u64) {
    if v != 0 {
        put_tag(buf, field, WT_VARINT);
        put_varint(buf, v);
    }
}

fn put_str(buf: &mut Vec<u8>, field: u32, s: &str) {
    if !s.is_empty() {
        put_bytes(buf, field, s.as_bytes());
    }
}

fn put_bytes(buf: &mut Vec<u8>, field: u32, b: &[u8]) {
    put_tag(buf, field, WT_LEN);
    put_varint(buf, b.len() as u64);
    buf.extend_from_slice(b);
}

/// Writes a sub-message built by `f` (length-prefixed; the length is patched after writing).
fn put_msg(buf: &mut Vec<u8>, field: u32, f: impl FnOnce(&mut Vec<u8>)) {
    let mut inner = Vec::new();
    f(&mut inner);
    put_bytes(buf, field, &inner);
}

fn zigzag32_enc(v: i32) -> u64 {
    ((v << 1) ^ (v >> 31)) as u32 as u64
}

/// Encodes an `ExportMetricsServiceRequest` (Go `pmetricotlp.ExportRequest.MarshalProto`).
pub fn marshal_export_request(md: &Metrics) -> Vec<u8> {
    let mut buf = Vec::new();
    for rm in &md.resource_metrics {
        put_msg(&mut buf, 1, |b| enc_resource_metrics(b, rm));
    }
    buf
}

fn enc_attrs(buf: &mut Vec<u8>, field: u32, m: &Map) {
    for (k, v) in &m.0 {
        put_msg(buf, field, |b| {
            put_str(b, 1, k);
            put_msg(b, 2, |b| enc_any_value(b, v));
        });
    }
}

fn enc_any_value(buf: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Empty => {}
        Value::Str(s) => put_bytes(buf, 1, s.as_bytes()),
        Value::Bool(x) => {
            put_tag(buf, 2, WT_VARINT);
            put_varint(buf, *x as u64);
        }
        Value::Int(i) => {
            put_tag(buf, 3, WT_VARINT);
            put_varint(buf, *i as u64);
        }
        Value::Double(f) => put_fixed64(buf, 4, f.to_bits()),
        Value::Slice(items) => put_msg(buf, 5, |b| {
            for it in items {
                put_msg(b, 1, |b| enc_any_value(b, it));
            }
        }),
        Value::Map(m) => put_msg(buf, 6, |b| enc_attrs(b, 1, m)),
        Value::Bytes(x) => put_bytes(buf, 7, x),
    }
}

fn enc_resource_metrics(buf: &mut Vec<u8>, rm: &ResourceMetrics) {
    put_msg(buf, 1, |b| {
        enc_attrs(b, 1, &rm.resource.attributes);
        put_varint_nz(b, 2, rm.resource.dropped_attributes_count as u64);
    });
    for sm in &rm.scope_metrics {
        put_msg(buf, 2, |b| {
            put_msg(b, 1, |b| {
                put_str(b, 1, &sm.scope.name);
                put_str(b, 2, &sm.scope.version);
                enc_attrs(b, 3, &sm.scope.attributes);
                put_varint_nz(b, 4, sm.scope.dropped_attributes_count as u64);
            });
            for m in &sm.metrics {
                put_msg(b, 2, |b| enc_metric(b, m));
            }
            put_str(b, 3, &sm.schema_url);
        });
    }
    put_str(buf, 3, &rm.schema_url);
}

fn enc_metric(buf: &mut Vec<u8>, m: &Metric) {
    put_str(buf, 1, &m.name);
    put_str(buf, 2, &m.description);
    put_str(buf, 3, &m.unit);
    match &m.data {
        MetricData::Empty => {}
        MetricData::Gauge(g) => put_msg(buf, 5, |b| {
            for p in &g.data_points {
                put_msg(b, 1, |b| enc_number_data_point(b, p));
            }
        }),
        MetricData::Sum(s) => put_msg(buf, 7, |b| {
            for p in &s.data_points {
                put_msg(b, 1, |b| enc_number_data_point(b, p));
            }
            put_varint_nz(b, 2, s.aggregation_temporality.0 as i64 as u64);
            put_varint_nz(b, 3, s.is_monotonic as u64);
        }),
        MetricData::Histogram(h) => put_msg(buf, 9, |b| {
            for p in &h.data_points {
                put_msg(b, 1, |b| enc_histogram_data_point(b, p));
            }
            put_varint_nz(b, 2, h.aggregation_temporality.0 as i64 as u64);
        }),
        MetricData::ExponentialHistogram(h) => put_msg(buf, 10, |b| {
            for p in &h.data_points {
                put_msg(b, 1, |b| enc_exp_histogram_data_point(b, p));
            }
            put_varint_nz(b, 2, h.aggregation_temporality.0 as i64 as u64);
        }),
        MetricData::Summary(s) => put_msg(buf, 11, |b| {
            for p in &s.data_points {
                put_msg(b, 1, |b| enc_summary_data_point(b, p));
            }
        }),
    }
    enc_attrs(buf, 12, &m.metadata);
}

fn enc_number_data_point(buf: &mut Vec<u8>, p: &NumberDataPoint) {
    put_fixed64_nz(buf, 2, p.start_timestamp);
    put_fixed64_nz(buf, 3, p.timestamp);
    match p.value {
        NumberValue::Empty => {}
        NumberValue::Double(v) => put_fixed64(buf, 4, v.to_bits()),
        NumberValue::Int(v) => put_fixed64(buf, 6, v as u64),
    }
    for e in &p.exemplars {
        put_msg(buf, 5, |b| enc_exemplar(b, e));
    }
    enc_attrs(buf, 7, &p.attributes);
    put_varint_nz(buf, 8, p.flags.0 as u64);
}

fn enc_packed_fixed64(buf: &mut Vec<u8>, field: u32, v: impl ExactSizeIterator<Item = u64>) {
    if v.len() == 0 {
        return;
    }
    put_tag(buf, field, WT_LEN);
    put_varint(buf, v.len() as u64 * 8);
    for x in v {
        buf.extend_from_slice(&x.to_le_bytes());
    }
}

fn enc_histogram_data_point(buf: &mut Vec<u8>, p: &HistogramDataPoint) {
    put_fixed64_nz(buf, 2, p.start_timestamp);
    put_fixed64_nz(buf, 3, p.timestamp);
    put_fixed64_nz(buf, 4, p.count);
    if let Some(s) = p.sum {
        put_fixed64(buf, 5, s.to_bits());
    }
    enc_packed_fixed64(buf, 6, p.bucket_counts.iter().copied());
    enc_packed_fixed64(buf, 7, p.explicit_bounds.iter().map(|f| f.to_bits()));
    for e in &p.exemplars {
        put_msg(buf, 8, |b| enc_exemplar(b, e));
    }
    enc_attrs(buf, 9, &p.attributes);
    put_varint_nz(buf, 10, p.flags.0 as u64);
    if let Some(v) = p.min {
        put_fixed64(buf, 11, v.to_bits());
    }
    if let Some(v) = p.max {
        put_fixed64(buf, 12, v.to_bits());
    }
}

fn enc_buckets(buf: &mut Vec<u8>, field: u32, bk: &Buckets) {
    put_msg(buf, field, |b| {
        put_varint_nz(b, 1, zigzag32_enc(bk.offset));
        if !bk.bucket_counts.is_empty() {
            let mut packed = Vec::new();
            for &c in &bk.bucket_counts {
                put_varint(&mut packed, c);
            }
            put_bytes(b, 2, &packed);
        }
    });
}

fn enc_exp_histogram_data_point(buf: &mut Vec<u8>, p: &ExponentialHistogramDataPoint) {
    enc_attrs(buf, 1, &p.attributes);
    put_fixed64_nz(buf, 2, p.start_timestamp);
    put_fixed64_nz(buf, 3, p.timestamp);
    put_fixed64_nz(buf, 4, p.count);
    if let Some(s) = p.sum {
        put_fixed64(buf, 5, s.to_bits());
    }
    put_varint_nz(buf, 6, zigzag32_enc(p.scale));
    put_fixed64_nz(buf, 7, p.zero_count);
    enc_buckets(buf, 8, &p.positive);
    enc_buckets(buf, 9, &p.negative);
    put_varint_nz(buf, 10, p.flags.0 as u64);
    for e in &p.exemplars {
        put_msg(buf, 11, |b| enc_exemplar(b, e));
    }
    if let Some(v) = p.min {
        put_fixed64(buf, 12, v.to_bits());
    }
    if let Some(v) = p.max {
        put_fixed64(buf, 13, v.to_bits());
    }
    put_double_nz(buf, 14, p.zero_threshold);
}

fn enc_summary_data_point(buf: &mut Vec<u8>, p: &SummaryDataPoint) {
    put_fixed64_nz(buf, 2, p.start_timestamp);
    put_fixed64_nz(buf, 3, p.timestamp);
    put_fixed64_nz(buf, 4, p.count);
    put_double_nz(buf, 5, p.sum);
    for q in &p.quantile_values {
        put_msg(buf, 6, |b| {
            put_double_nz(b, 1, q.quantile);
            put_double_nz(b, 2, q.value);
        });
    }
    enc_attrs(buf, 7, &p.attributes);
    put_varint_nz(buf, 8, p.flags.0 as u64);
}

fn enc_exemplar(buf: &mut Vec<u8>, e: &Exemplar) {
    put_fixed64_nz(buf, 2, e.timestamp);
    match e.value {
        NumberValue::Empty => {}
        NumberValue::Double(v) => put_fixed64(buf, 3, v.to_bits()),
        NumberValue::Int(v) => put_fixed64(buf, 6, v as u64),
    }
    if !id_is_empty(&e.span_id) {
        put_bytes(buf, 4, &e.span_id);
    }
    if !id_is_empty(&e.trace_id) {
        put_bytes(buf, 5, &e.trace_id);
    }
    enc_attrs(buf, 7, &e.filtered_attributes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let mut md = Metrics::new();
        let rm = md.resource_metrics.append_empty();
        rm.resource.attributes.put_str("service.name", "svc");
        rm.resource.attributes.put("nested", Value::Map(Map::from_pairs(&[("a", Value::Int(-3))])));
        let sm = rm.scope_metrics.append_empty();
        sm.scope.name = "scope".into();
        let m = sm.metrics.append_empty();
        m.name = "eh".into();
        let eh = m.set_empty_exponential_histogram();
        eh.aggregation_temporality = AggregationTemporality::DELTA;
        let p = eh.data_points.append_empty();
        p.scale = -3;
        p.positive.offset = -2;
        p.positive.bucket_counts = vec![1, 0, 300];
        p.set_sum(0.0);
        p.zero_threshold = 1e-9;
        let e = p.exemplars.append_empty();
        e.trace_id = [1; 16];
        e.set_int_value(-7);
        let m = sm.metrics.append_empty();
        m.name = "h".into();
        let h = m.set_empty_histogram();
        let p = h.data_points.append_empty();
        p.bucket_counts = vec![1, 2];
        p.explicit_bounds = vec![0.5];
        p.min = Some(-1.0);
        let b = marshal_export_request(&md);
        assert_eq!(unmarshal_export_request(&b).unwrap(), md);
        assert!(unmarshal_export_request(&b[..b.len() - 1]).is_err());
    }
}
