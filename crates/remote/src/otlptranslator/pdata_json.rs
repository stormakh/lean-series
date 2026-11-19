//! OTLP/JSON decoding of `ExportMetricsServiceRequest` (Go `pmetricotlp.ExportRequest.UnmarshalJSON`).
//!
//! As pdata does, it accepts both lowerCamelCase and snake_case field names, 64-bit integers as numbers or
//! decimal strings, enums as numbers or names, trace and span ids as hex, bytes as base64, and `null` for empty
//! objects and arrays; unknown fields are skipped. Error texts name the failing operation like json-iterator's
//! (`ReadUint64: unsupported value type`), but not its byte context.

use json::Json;

use super::pdata::*;

type Result<T> = std::result::Result<T, String>;

/// Decodes an OTLP/JSON export request.
pub fn unmarshal_export_request_json(buf: &[u8]) -> Result<Metrics> {
    let v = Json::parse_bytes(buf).map_err(|e| format!("ReadObject: {e}"))?;
    let mut md = Metrics::new();
    for (k, v) in obj(&v)? {
        match k.as_str() {
            "resourceMetrics" | "resource_metrics" => {
                for it in arr(v)? {
                    md.resource_metrics.push(resource_metrics(it)?);
                }
            }
            _ => {}
        }
    }
    Ok(md)
}

static EMPTY_OBJ: Vec<(String, Json)> = Vec::new();
static EMPTY_ARR: Vec<Json> = Vec::new();

fn obj(v: &Json) -> Result<&Vec<(String, Json)>> {
    match v {
        Json::Obj(o) => Ok(o),
        Json::Null => Ok(&EMPTY_OBJ),
        _ => Err("ReadObject: expect { or n".to_string()),
    }
}

fn arr(v: &Json) -> Result<&Vec<Json>> {
    match v {
        Json::Arr(a) => Ok(a),
        Json::Null => Ok(&EMPTY_ARR),
        _ => Err("ReadArray: expect [ or n".to_string()),
    }
}

fn string(v: &Json) -> Result<String> {
    match v {
        Json::Str(s) => Ok(s.clone()),
        Json::Null => Ok(String::new()),
        _ => Err("ReadString: expects \" or n".to_string()),
    }
}

fn boolean(v: &Json) -> Result<bool> {
    match v {
        Json::Bool(b) => Ok(*b),
        _ => Err("ReadBool: expect t or f".to_string()),
    }
}

fn u64_of(v: &Json, op: &str) -> Result<u64> {
    match v {
        Json::Int(i) if *i >= 0 => Ok(*i as u64),
        Json::Float(f) if *f >= 0.0 && f.fract() == 0.0 && *f < 1.8446744073709552e19 => Ok(*f as u64),
        Json::Str(s) => s.parse::<u64>().map_err(|e| format!("{op}: {e}")),
        Json::Int(_) | Json::Float(_) => Err(format!("{op}: invalid number")),
        _ => Err(format!("{op}: unsupported value type")),
    }
}

fn i64_of(v: &Json, op: &str) -> Result<i64> {
    match v {
        Json::Int(i) => Ok(*i),
        Json::Str(s) => s.parse::<i64>().map_err(|e| format!("{op}: {e}")),
        Json::Float(_) => Err(format!("{op}: invalid number")),
        _ => Err(format!("{op}: unsupported value type")),
    }
}

fn u32_of(v: &Json) -> Result<u32> {
    let x = u64_of(v, "ReadUint32")?;
    u32::try_from(x).map_err(|_| "ReadUint32: overflow".to_string())
}

fn i32_of(v: &Json) -> Result<i32> {
    let x = i64_of(v, "ReadInt32")?;
    i32::try_from(x).map_err(|_| "ReadInt32: overflow".to_string())
}

fn f64_of(v: &Json) -> Result<f64> {
    match v {
        Json::Int(i) => Ok(*i as f64),
        Json::Float(f) => Ok(*f),
        Json::Str(s) => match s.as_str() {
            // strconv.ParseFloat accepts these spellings.
            "NaN" => Ok(f64::NAN),
            "Infinity" | "+Infinity" | "Inf" | "+Inf" => Ok(f64::INFINITY),
            "-Infinity" | "-Inf" => Ok(f64::NEG_INFINITY),
            _ => s.parse::<f64>().map_err(|e| format!("ReadUint64: {e}")),
        },
        _ => Err("ReadUint64: unsupported value type".to_string()),
    }
}

fn temporality(v: &Json) -> Result<AggregationTemporality> {
    match v {
        Json::Str(s) => match s.as_str() {
            "AGGREGATION_TEMPORALITY_UNSPECIFIED" => Ok(AggregationTemporality::UNSPECIFIED),
            "AGGREGATION_TEMPORALITY_DELTA" => Ok(AggregationTemporality::DELTA),
            "AGGREGATION_TEMPORALITY_CUMULATIVE" => Ok(AggregationTemporality::CUMULATIVE),
            _ => Err("ReadEnumValue: unknown string value".to_string()),
        },
        Json::Int(_) | Json::Float(_) => Ok(AggregationTemporality(i32_of(v)?)),
        _ => Err("ReadEnumValue: unsupported value type".to_string()),
    }
}

fn hex_id(v: &Json, dst: &mut [u8]) -> Result<()> {
    let s = string(v)?;
    dst.iter_mut().for_each(|b| *b = 0);
    if s.is_empty() {
        return Ok(());
    }
    if dst.len() != s.len() / 2 {
        return Err("ID.UnmarshalJSONIter: length mismatch".to_string());
    }
    let b = s.as_bytes();
    if b.len() % 2 == 1 {
        return Err("ID.UnmarshalJSONIter: encoding/hex: odd length hex string".to_string());
    }
    for (i, c) in b.chunks(2).enumerate() {
        let h = |x: u8| match x {
            b'0'..=b'9' => Ok(x - b'0'),
            b'a'..=b'f' => Ok(x - b'a' + 10),
            b'A'..=b'F' => Ok(x - b'A' + 10),
            _ => Err(format!("ID.UnmarshalJSONIter: encoding/hex: invalid byte: U+{:04X} {:?}", x, x as char)),
        };
        dst[i] = h(c[0])? << 4 | h(c[1])?;
    }
    Ok(())
}

fn attributes(v: &Json, m: &mut Map) -> Result<()> {
    for kv in arr(v)? {
        let mut key = String::new();
        let mut value = Value::Empty;
        for (k, v) in obj(kv)? {
            match k.as_str() {
                "key" => key = string(v)?,
                "value" => value = any_value(v)?,
                _ => {}
            }
        }
        m.0.push((key, value));
    }
    Ok(())
}

fn any_value(v: &Json) -> Result<Value> {
    let mut out = Value::Empty;
    for (k, v) in obj(v)? {
        match k.as_str() {
            "stringValue" | "string_value" => out = Value::Str(string(v)?),
            "boolValue" | "bool_value" => out = Value::Bool(boolean(v)?),
            "intValue" | "int_value" => out = Value::Int(i64_of(v, "ReadInt64")?),
            "doubleValue" | "double_value" => out = Value::Double(f64_of(v)?),
            "arrayValue" | "array_value" => {
                let mut items = Vec::new();
                for (k, v) in obj(v)? {
                    if k == "values" {
                        for it in arr(v)? {
                            items.push(any_value(it)?);
                        }
                    }
                }
                out = Value::Slice(items);
            }
            "kvlistValue" | "kvlist_value" => {
                let mut m = Map::new();
                for (k, v) in obj(v)? {
                    if k == "values" {
                        attributes(v, &mut m)?;
                    }
                }
                out = Value::Map(m);
            }
            "bytesValue" | "bytes_value" => {
                let s = string(v)?;
                out = Value::Bytes(base64_std_decode(s.as_bytes()).map_err(|e| format!("base64.Decode: {e}"))?);
            }
            _ => {}
        }
    }
    Ok(out)
}

fn resource_metrics(v: &Json) -> Result<ResourceMetrics> {
    let mut rm = ResourceMetrics::default();
    for (k, v) in obj(v)? {
        match k.as_str() {
            "resource" => {
                for (k, v) in obj(v)? {
                    match k.as_str() {
                        "attributes" => attributes(v, &mut rm.resource.attributes)?,
                        "droppedAttributesCount" | "dropped_attributes_count" => {
                            rm.resource.dropped_attributes_count = u32_of(v)?
                        }
                        _ => {}
                    }
                }
            }
            "scopeMetrics" | "scope_metrics" => {
                for it in arr(v)? {
                    rm.scope_metrics.push(scope_metrics(it)?);
                }
            }
            "schemaUrl" | "schema_url" => rm.schema_url = string(v)?,
            _ => {}
        }
    }
    Ok(rm)
}

fn scope_metrics(v: &Json) -> Result<ScopeMetrics> {
    let mut sm = ScopeMetrics::default();
    for (k, v) in obj(v)? {
        match k.as_str() {
            "scope" => {
                for (k, v) in obj(v)? {
                    match k.as_str() {
                        "name" => sm.scope.name = string(v)?,
                        "version" => sm.scope.version = string(v)?,
                        "attributes" => attributes(v, &mut sm.scope.attributes)?,
                        "droppedAttributesCount" | "dropped_attributes_count" => {
                            sm.scope.dropped_attributes_count = u32_of(v)?
                        }
                        _ => {}
                    }
                }
            }
            "metrics" => {
                for it in arr(v)? {
                    sm.metrics.push(metric(it)?);
                }
            }
            "schemaUrl" | "schema_url" => sm.schema_url = string(v)?,
            _ => {}
        }
    }
    Ok(sm)
}

fn data_points<T>(v: &Json, f: impl Fn(&Json) -> Result<T>) -> Result<Vec<T>> {
    let mut out = Vec::new();
    for it in arr(v)? {
        out.push(f(it)?);
    }
    Ok(out)
}

fn metric(v: &Json) -> Result<Metric> {
    let mut m = Metric::default();
    for (k, v) in obj(v)? {
        match k.as_str() {
            "name" => m.name = string(v)?,
            "description" => m.description = string(v)?,
            "unit" => m.unit = string(v)?,
            "gauge" => {
                let mut g = Gauge::default();
                for (k, v) in obj(v)? {
                    if k == "dataPoints" || k == "data_points" {
                        g.data_points = data_points(v, number_data_point)?;
                    }
                }
                m.data = MetricData::Gauge(g);
            }
            "sum" => {
                let mut s = Sum::default();
                for (k, v) in obj(v)? {
                    match k.as_str() {
                        "dataPoints" | "data_points" => s.data_points = data_points(v, number_data_point)?,
                        "aggregationTemporality" | "aggregation_temporality" => {
                            s.aggregation_temporality = temporality(v)?
                        }
                        "isMonotonic" | "is_monotonic" => s.is_monotonic = boolean(v)?,
                        _ => {}
                    }
                }
                m.data = MetricData::Sum(s);
            }
            "histogram" => {
                let mut h = Histogram::default();
                for (k, v) in obj(v)? {
                    match k.as_str() {
                        "dataPoints" | "data_points" => h.data_points = data_points(v, histogram_data_point)?,
                        "aggregationTemporality" | "aggregation_temporality" => {
                            h.aggregation_temporality = temporality(v)?
                        }
                        _ => {}
                    }
                }
                m.data = MetricData::Histogram(h);
            }
            "exponentialHistogram" | "exponential_histogram" => {
                let mut h = ExponentialHistogram::default();
                for (k, v) in obj(v)? {
                    match k.as_str() {
                        "dataPoints" | "data_points" => h.data_points = data_points(v, exp_histogram_data_point)?,
                        "aggregationTemporality" | "aggregation_temporality" => {
                            h.aggregation_temporality = temporality(v)?
                        }
                        _ => {}
                    }
                }
                m.data = MetricData::ExponentialHistogram(h);
            }
            "summary" => {
                let mut s = Summary::default();
                for (k, v) in obj(v)? {
                    if k == "dataPoints" || k == "data_points" {
                        s.data_points = data_points(v, summary_data_point)?;
                    }
                }
                m.data = MetricData::Summary(s);
            }
            "metadata" => attributes(v, &mut m.metadata)?,
            _ => {}
        }
    }
    Ok(m)
}

fn number_data_point(v: &Json) -> Result<NumberDataPoint> {
    let mut p = NumberDataPoint::default();
    for (k, v) in obj(v)? {
        match k.as_str() {
            "attributes" => attributes(v, &mut p.attributes)?,
            "startTimeUnixNano" | "start_time_unix_nano" => p.start_timestamp = u64_of(v, "ReadUint64")?,
            "timeUnixNano" | "time_unix_nano" => p.timestamp = u64_of(v, "ReadUint64")?,
            "asDouble" | "as_double" => p.value = NumberValue::Double(f64_of(v)?),
            "asInt" | "as_int" => p.value = NumberValue::Int(i64_of(v, "ReadInt64")?),
            "exemplars" => p.exemplars = data_points(v, exemplar)?,
            "flags" => p.flags = DataPointFlags(u32_of(v)?),
            _ => {}
        }
    }
    Ok(p)
}

fn u64_list(v: &Json) -> Result<Vec<u64>> {
    data_points(v, |x| u64_of(x, "ReadUint64"))
}

fn histogram_data_point(v: &Json) -> Result<HistogramDataPoint> {
    let mut p = HistogramDataPoint::default();
    for (k, v) in obj(v)? {
        match k.as_str() {
            "attributes" => attributes(v, &mut p.attributes)?,
            "startTimeUnixNano" | "start_time_unix_nano" => p.start_timestamp = u64_of(v, "ReadUint64")?,
            "timeUnixNano" | "time_unix_nano" => p.timestamp = u64_of(v, "ReadUint64")?,
            "count" => p.count = u64_of(v, "ReadUint64")?,
            "sum" => p.sum = Some(f64_of(v)?),
            "bucketCounts" | "bucket_counts" => p.bucket_counts.extend(u64_list(v)?),
            "explicitBounds" | "explicit_bounds" => p.explicit_bounds.extend(data_points(v, f64_of)?),
            "exemplars" => p.exemplars.extend(data_points(v, exemplar)?),
            "flags" => p.flags = DataPointFlags(u32_of(v)?),
            "min" => p.min = Some(f64_of(v)?),
            "max" => p.max = Some(f64_of(v)?),
            _ => {}
        }
    }
    Ok(p)
}

fn buckets(v: &Json) -> Result<Buckets> {
    let mut b = Buckets::default();
    for (k, v) in obj(v)? {
        match k.as_str() {
            "offset" => b.offset = i32_of(v)?,
            "bucketCounts" | "bucket_counts" => b.bucket_counts.extend(u64_list(v)?),
            _ => {}
        }
    }
    Ok(b)
}

fn exp_histogram_data_point(v: &Json) -> Result<ExponentialHistogramDataPoint> {
    let mut p = ExponentialHistogramDataPoint::default();
    for (k, v) in obj(v)? {
        match k.as_str() {
            "attributes" => attributes(v, &mut p.attributes)?,
            "startTimeUnixNano" | "start_time_unix_nano" => p.start_timestamp = u64_of(v, "ReadUint64")?,
            "timeUnixNano" | "time_unix_nano" => p.timestamp = u64_of(v, "ReadUint64")?,
            "count" => p.count = u64_of(v, "ReadUint64")?,
            "sum" => p.sum = Some(f64_of(v)?),
            "scale" => p.scale = i32_of(v)?,
            "zeroCount" | "zero_count" => p.zero_count = u64_of(v, "ReadUint64")?,
            "positive" => p.positive = buckets(v)?,
            "negative" => p.negative = buckets(v)?,
            "flags" => p.flags = DataPointFlags(u32_of(v)?),
            "exemplars" => p.exemplars.extend(data_points(v, exemplar)?),
            "min" => p.min = Some(f64_of(v)?),
            "max" => p.max = Some(f64_of(v)?),
            "zeroThreshold" | "zero_threshold" => p.zero_threshold = f64_of(v)?,
            _ => {}
        }
    }
    Ok(p)
}

fn summary_data_point(v: &Json) -> Result<SummaryDataPoint> {
    let mut p = SummaryDataPoint::default();
    for (k, v) in obj(v)? {
        match k.as_str() {
            "attributes" => attributes(v, &mut p.attributes)?,
            "startTimeUnixNano" | "start_time_unix_nano" => p.start_timestamp = u64_of(v, "ReadUint64")?,
            "timeUnixNano" | "time_unix_nano" => p.timestamp = u64_of(v, "ReadUint64")?,
            "count" => p.count = u64_of(v, "ReadUint64")?,
            "sum" => p.sum = f64_of(v)?,
            "quantileValues" | "quantile_values" => {
                for it in arr(v)? {
                    let mut q = ValueAtQuantile::default();
                    for (k, v) in obj(it)? {
                        match k.as_str() {
                            "quantile" => q.quantile = f64_of(v)?,
                            "value" => q.value = f64_of(v)?,
                            _ => {}
                        }
                    }
                    p.quantile_values.push(q);
                }
            }
            "flags" => p.flags = DataPointFlags(u32_of(v)?),
            _ => {}
        }
    }
    Ok(p)
}

fn exemplar(v: &Json) -> Result<Exemplar> {
    let mut e = Exemplar::default();
    for (k, v) in obj(v)? {
        match k.as_str() {
            "filteredAttributes" | "filtered_attributes" => attributes(v, &mut e.filtered_attributes)?,
            "timeUnixNano" | "time_unix_nano" => e.timestamp = u64_of(v, "ReadUint64")?,
            "asDouble" | "as_double" => e.value = NumberValue::Double(f64_of(v)?),
            "asInt" | "as_int" => e.value = NumberValue::Int(i64_of(v, "ReadInt64")?),
            "traceId" | "trace_id" => hex_id(v, &mut e.trace_id)?,
            "spanId" | "span_id" => hex_id(v, &mut e.span_id)?,
            _ => {}
        }
    }
    Ok(e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_json() {
        let body = br#"{"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"svc"}}]},
            "scopeMetrics":[{"scope":{"name":"s"},"metrics":[{"name":"c","unit":"By","sum":{"aggregationTemporality":"AGGREGATION_TEMPORALITY_CUMULATIVE","isMonotonic":true,
            "dataPoints":[{"timeUnixNano":"1000000","asInt":"5","exemplars":[{"traceId":"000102030405060708090a0b0c0d0e0f","asDouble":1.5}]}]}}]}]}]}"#;
        let md = unmarshal_export_request_json(body).unwrap();
        let m = &md.resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(m.sum().aggregation_temporality, AggregationTemporality::CUMULATIVE);
        assert!(m.sum().is_monotonic);
        let p = &m.sum().data_points[0];
        assert_eq!(p.timestamp, 1_000_000);
        assert_eq!(p.int_value(), 5);
        assert_eq!(p.exemplars[0].trace_id[15], 15);
        assert!(unmarshal_export_request_json(b"{\"resourceMetrics\":5}").is_err());
    }
}
