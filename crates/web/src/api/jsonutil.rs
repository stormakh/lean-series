//! JSON writers for API values: a port of `util/jsonutil` and the jsoniter encoders of
//! `web/api/v1/json_codec.go`. Everything writes straight into a `String` (no JSON tree, no per-sample allocation).

use std::fmt::Write;

use model::exemplar::{Exemplar, QueryResult as ExemplarQueryResult};
use model::histogram::{BucketIterator, FloatHistogram};
use model::labels::Labels;
use promql::{FPoint, HPoint, Sample, Series, Value};

/// Appends Go `strconv.FormatFloat(f, 'f', -1, 64)`.
pub fn write_float_f(f: f64, out: &mut String) {
    if f.is_nan() {
        out.push_str("NaN");
    } else if f.is_infinite() {
        out.push_str(if f > 0.0 { "+Inf" } else { "-Inf" });
    } else {
        let _ = write!(out, "{}", f);
    }
}

/// Appends Go `strconv.FormatFloat(f, 'e', -1, 64)`.
pub fn write_float_e(f: f64, out: &mut String) {
    if f.is_nan() {
        out.push_str("NaN");
        return;
    }
    if f.is_infinite() {
        out.push_str(if f > 0.0 { "+Inf" } else { "-Inf" });
        return;
    }
    let start = out.len();
    let _ = write!(out, "{:e}", f);
    // Rust: "1.5e21", "1e-7"; Go: "1.5e+21", "1e-07".
    let Some(epos) = out[start..].find('e').map(|i| start + i) else { return };
    let exp: String = out[epos + 1..].to_string();
    out.truncate(epos + 1);
    let (sign, digits) = match exp.strip_prefix('-') {
        Some(d) => ('-', d),
        None => ('+', exp.as_str()),
    };
    out.push(sign);
    if digits.len() < 2 {
        out.push('0');
    }
    out.push_str(digits);
}

/// Go `jsonutil.MarshalFloat`: a quoted float, in exponent form below 1e-6 and from 1e21.
#[inline]
pub fn write_marshal_float(f: f64, out: &mut String) {
    out.push('"');
    let a = f.abs();
    if a != 0.0 && (a < 1e-6 || a >= 1e21) {
        write_float_e(f, out);
    } else {
        write_float_f(f, out);
    }
    out.push('"');
}

/// Go `jsonutil.MarshalTimestamp`: milliseconds written as seconds with up to 3 decimals.
#[inline]
pub fn write_timestamp(t: i64, out: &mut String) {
    let mut t = t;
    if t < 0 {
        out.push('-');
        t = t.wrapping_neg();
    }
    json::write_i64(t / 1000, out);
    let fraction = t % 1000;
    if fraction != 0 {
        out.push('.');
        if fraction < 100 {
            out.push('0');
        }
        if fraction < 10 {
            out.push('0');
        }
        json::write_i64(fraction, out);
    }
}

/// Go `jsonutil.MarshalHistogram`.
pub fn write_histogram(h: &FloatHistogram, out: &mut String) {
    out.push_str("{\"count\":");
    write_marshal_float(h.count, out);
    out.push_str(",\"sum\":");
    write_marshal_float(h.sum, out);
    let mut found = false;
    let mut it = h.all_bucket_iterator();
    while it.next() {
        let b = it.at();
        if b.count == 0.0 {
            continue;
        }
        out.push(',');
        if !found {
            out.push_str("\"buckets\":[");
        }
        found = true;
        let boundaries = match (b.lower_inclusive, b.upper_inclusive) {
            (true, true) => 3,
            (true, false) => 1,
            (false, true) => 0,
            (false, false) => 2,
        };
        out.push('[');
        json::write_i64(boundaries, out);
        out.push(',');
        write_marshal_float(b.lower, out);
        out.push(',');
        write_marshal_float(b.upper, out);
        out.push(',');
        write_marshal_float(b.count, out);
        out.push(']');
    }
    if found {
        out.push(']');
    }
    out.push('}');
}

/// Labels as a JSON object, in label order.
#[inline]
pub fn write_labels(l: &Labels, out: &mut String) {
    out.push('{');
    let mut first = true;
    l.range(|n, v| {
        if !first {
            out.push(',');
        }
        first = false;
        json::write_str(n, out);
        out.push(':');
        json::write_str(v, out);
    });
    out.push('}');
}

/// `[ts, "1.234"]`.
#[inline]
pub fn write_fpoint(p: &FPoint, out: &mut String) {
    out.push('[');
    write_timestamp(p.t, out);
    out.push(',');
    write_marshal_float(p.f, out);
    out.push(']');
}

/// `[ts, {histogram}]`.
pub fn write_hpoint(p: &HPoint, out: &mut String) {
    out.push('[');
    write_timestamp(p.t, out);
    out.push(',');
    write_histogram(&p.h, out);
    out.push(']');
}

/// A series: `{"metric":{...},"values":[...],"histograms":[...]}`.
pub fn write_series(s: &Series, out: &mut String) {
    out.push_str("{\"metric\":");
    write_labels(&s.metric, out);
    if !s.floats.is_empty() {
        out.push_str(",\"values\":[");
        for (i, p) in s.floats.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            write_fpoint(p, out);
        }
        out.push(']');
    }
    if !s.histograms.is_empty() {
        out.push_str(",\"histograms\":[");
        for (i, p) in s.histograms.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            write_hpoint(p, out);
        }
        out.push(']');
    }
    out.push('}');
}

/// A sample: `{"metric":{...},"value":[ts,"v"]}` or `{"metric":{...},"histogram":[ts,{...}]}`.
pub fn write_sample(s: &Sample, out: &mut String) {
    out.push_str("{\"metric\":");
    write_labels(&s.metric, out);
    match &s.h {
        None => {
            out.push_str(",\"value\":[");
            write_timestamp(s.t, out);
            out.push(',');
            write_marshal_float(s.f, out);
        }
        Some(h) => {
            out.push_str(",\"histogram\":[");
            write_timestamp(s.t, out);
            out.push(',');
            write_histogram(h, out);
        }
    }
    out.push_str("]}");
}

/// Go `encoding/json` of `float64(t) / 1000`.
fn write_json_seconds(t: i64, out: &mut String) {
    json::write_f64(t as f64 / 1000.0, out);
}

/// A query result value as the API writes it.
pub fn write_value(v: &Value, out: &mut String) {
    match v {
        Value::Matrix(m) => {
            out.push('[');
            for (i, s) in m.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_series(s, out);
            }
            out.push(']');
        }
        Value::Vector(vec) => {
            out.push('[');
            for (i, s) in vec.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_sample(s, out);
            }
            out.push(']');
        }
        Value::Scalar(s) => {
            // Go `Scalar.MarshalJSON`: `[float64(T)/1000, FormatFloat(V, 'f', -1, 64)]`.
            out.push('[');
            write_json_seconds(s.t, out);
            out.push_str(",\"");
            write_float_f(s.v, out);
            out.push_str("\"]");
        }
        Value::String(s) => {
            out.push('[');
            write_json_seconds(s.t, out);
            out.push(',');
            json::write_str(&s.v, out);
            out.push(']');
        }
    }
}

/// Go `marshalExemplarJSON`.
pub fn write_exemplar(e: &Exemplar, out: &mut String) {
    out.push_str("{\"labels\":");
    write_labels(&e.labels, out);
    out.push_str(",\"value\":");
    write_marshal_float(e.value, out);
    out.push_str(",\"timestamp\":");
    write_timestamp(e.ts, out);
    out.push('}');
}

/// `exemplar.QueryResult`: `{"seriesLabels":{...},"exemplars":[...]}`.
pub fn write_exemplar_query_result(r: &ExemplarQueryResult, out: &mut String) {
    out.push_str("{\"seriesLabels\":");
    write_labels(&r.series_labels, out);
    out.push_str(",\"exemplars\":");
    if r.exemplars.is_empty() {
        // Go: a nil slice is `null`; the exemplar storage never returns a series without exemplars.
        out.push_str("null");
    } else {
        out.push('[');
        for (i, e) in r.exemplars.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            write_exemplar(e, out);
        }
        out.push(']');
    }
    out.push('}');
}

/// A JSON array of strings.
pub fn write_strings(v: &[String], out: &mut String) {
    out.push('[');
    for (i, s) in v.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        json::write_str(s, out);
    }
    out.push(']');
}
