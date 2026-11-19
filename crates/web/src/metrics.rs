//! Self-instrumentation: the metric family model (the parts of `io_prometheus_client.MetricFamily` the server
//! exposes), the `Gatherer` interface (Go `prometheus.Gatherer`), and their encodings.

/// Go `dto.MetricType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    Summary,
    Untyped,
    Histogram,
}

impl MetricType {
    /// The protobuf enum name (protojson).
    pub fn proto_name(self) -> &'static str {
        match self {
            MetricType::Counter => "COUNTER",
            MetricType::Gauge => "GAUGE",
            MetricType::Summary => "SUMMARY",
            MetricType::Untyped => "UNTYPED",
            MetricType::Histogram => "HISTOGRAM",
        }
    }

    /// The text exposition name.
    pub fn text_name(self) -> &'static str {
        match self {
            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge",
            MetricType::Summary => "summary",
            MetricType::Untyped => "untyped",
            MetricType::Histogram => "histogram",
        }
    }
}

/// The value of one metric.
#[derive(Clone, Debug, PartialEq)]
pub enum MetricValue {
    Counter(f64),
    Gauge(f64),
    Untyped(f64),
    /// Quantiles are `(quantile, value)`.
    Summary { count: u64, sum: f64, quantiles: Vec<(f64, f64)> },
    /// Buckets are cumulative `(upper bound, count)`, without +Inf.
    Histogram { count: u64, sum: f64, buckets: Vec<(f64, u64)> },
}

/// One metric of a family (Go `dto.Metric`).
#[derive(Clone, Debug, PartialEq)]
pub struct Metric {
    /// Label pairs sorted by name.
    pub labels: Vec<(String, String)>,
    pub value: MetricValue,
    pub timestamp_ms: Option<i64>,
}

/// One metric family (Go `dto.MetricFamily`).
#[derive(Clone, Debug, PartialEq)]
pub struct MetricFamily {
    pub name: String,
    pub help: String,
    pub typ: MetricType,
    pub metrics: Vec<Metric>,
}

/// Go `prometheus.Gatherer`: families sorted by name.
pub trait Gatherer: Send + Sync {
    fn gather(&self) -> Result<Vec<MetricFamily>, String>;
}

fn protojson_f64(f: f64, out: &mut String) {
    if f.is_nan() {
        out.push_str("\"NaN\"");
    } else if f.is_infinite() {
        out.push_str(if f > 0.0 { "\"Infinity\"" } else { "\"-Infinity\"" });
    } else {
        json::write_f64(f, out);
    }
}

/// The family as protojson (Go `protojson.Marshal(mf)`, compact).
pub fn protojson(mf: &MetricFamily) -> String {
    let mut out = String::with_capacity(128);
    out.push_str("{\"name\":");
    json::write_str(&mf.name, &mut out);
    if !mf.help.is_empty() {
        out.push_str(",\"help\":");
        json::write_str(&mf.help, &mut out);
    }
    out.push_str(",\"type\":\"");
    out.push_str(mf.typ.proto_name());
    out.push_str("\",\"metric\":[");
    for (i, m) in mf.metrics.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('{');
        if !m.labels.is_empty() {
            out.push_str("\"label\":[");
            for (j, (n, v)) in m.labels.iter().enumerate() {
                if j > 0 {
                    out.push(',');
                }
                out.push_str("{\"name\":");
                json::write_str(n, &mut out);
                out.push_str(",\"value\":");
                json::write_str(v, &mut out);
                out.push('}');
            }
            out.push_str("],");
        }
        match &m.value {
            MetricValue::Counter(v) => {
                out.push_str("\"counter\":{\"value\":");
                protojson_f64(*v, &mut out);
                out.push('}');
            }
            MetricValue::Gauge(v) => {
                out.push_str("\"gauge\":{\"value\":");
                protojson_f64(*v, &mut out);
                out.push('}');
            }
            MetricValue::Untyped(v) => {
                out.push_str("\"untyped\":{\"value\":");
                protojson_f64(*v, &mut out);
                out.push('}');
            }
            MetricValue::Summary { count, sum, quantiles } => {
                out.push_str("\"summary\":{\"sampleCount\":\"");
                out.push_str(&count.to_string());
                out.push_str("\",\"sampleSum\":");
                protojson_f64(*sum, &mut out);
                if !quantiles.is_empty() {
                    out.push_str(",\"quantile\":[");
                    for (j, (q, v)) in quantiles.iter().enumerate() {
                        if j > 0 {
                            out.push(',');
                        }
                        out.push_str("{\"quantile\":");
                        protojson_f64(*q, &mut out);
                        out.push_str(",\"value\":");
                        protojson_f64(*v, &mut out);
                        out.push('}');
                    }
                    out.push(']');
                }
                out.push('}');
            }
            MetricValue::Histogram { count, sum, buckets } => {
                out.push_str("\"histogram\":{\"sampleCount\":\"");
                out.push_str(&count.to_string());
                out.push_str("\",\"sampleSum\":");
                protojson_f64(*sum, &mut out);
                if !buckets.is_empty() {
                    out.push_str(",\"bucket\":[");
                    for (j, (ub, c)) in buckets.iter().enumerate() {
                        if j > 0 {
                            out.push(',');
                        }
                        out.push_str("{\"cumulativeCount\":\"");
                        out.push_str(&c.to_string());
                        out.push_str("\",\"upperBound\":");
                        protojson_f64(*ub, &mut out);
                        out.push('}');
                    }
                    out.push(']');
                }
                out.push('}');
            }
        }
        if let Some(ts) = m.timestamp_ms {
            out.push_str(",\"timestampMs\":\"");
            out.push_str(&ts.to_string());
            out.push('"');
        }
        out.push('}');
    }
    out.push_str("]}");
    out
}
