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

// ---------------------------------------------------------------------------------------------------------------
// Printing (Go `String()` methods of prometheus/common/model).

/// Go `strconv.FormatFloat(f, 'f', -1, 64)`.
pub fn fmt_f(f: f64) -> String {
    promql::format_float(f)
}

/// Go `model.Time.String()`: seconds with up to 3 decimals.
pub fn time_string(ms: i64) -> String {
    fmt_f(ms as f64 / 1000.0)
}

/// Go `strconv.Quote`.
fn quote(s: &str) -> String {
    promql::parser::quote::go_quote(s)
}

/// Go `model.Metric.String()`: `name{a="b", c="d"}`.
pub fn metric_string(m: &LabelSet) -> String {
    let name = m.get("__name__");
    let labels: Vec<String> =
        m.iter().filter(|(k, _)| k.as_str() != "__name__").map(|(k, v)| format!("{k}={}", quote(v))).collect();
    if labels.is_empty() {
        return match name {
            Some(n) => n.clone(),
            None => "{}".to_string(),
        };
    }
    format!("{}{{{}}}", name.map(|s| s.as_str()).unwrap_or(""), labels.join(", "))
}

/// Go `model.LabelSet.String()`: `{a="b", c="d"}` (the name is a label like the others).
pub fn labelset_string(m: &LabelSet) -> String {
    let labels: Vec<String> = m.iter().map(|(k, v)| format!("{k}={}", quote(v))).collect();
    format!("{{{}}}", labels.join(", "))
}

/// Go `%f`.
fn fmt_percent_f(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f > 0.0 { "+Inf" } else { "-Inf" }.to_string()
    } else {
        format!("{f:.6}")
    }
}

/// Go fmt `%g` of a float64.
fn fmt_percent_g(f: f64) -> String {
    model::labels::format_float_g(f)
}

impl std::fmt::Display for HistogramBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let lower_inclusive = self.boundaries == 1 || self.boundaries == 3;
        let upper_inclusive = self.boundaries == 0 || self.boundaries == 3;
        write!(
            f,
            "{}{},{}{}:{}",
            if lower_inclusive { '[' } else { '(' },
            fmt_percent_g(self.lower),
            fmt_percent_g(self.upper),
            if upper_inclusive { ']' } else { ')' },
            fmt_f(self.count)
        )
    }
}

impl std::fmt::Display for SampleHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let b: Vec<String> = self.buckets.iter().map(|b| b.to_string()).collect();
        write!(f, "Count: {}, Sum: {}, Buckets: [{}]", fmt_percent_f(self.count), fmt_percent_f(self.sum), b.join(" "))
    }
}

impl std::fmt::Display for SamplePair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} @[{}]", fmt_f(self.value), time_string(self.timestamp))
    }
}

impl std::fmt::Display for SampleHistogramPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} @[{}]", self.histogram, time_string(self.timestamp))
    }
}

impl std::fmt::Display for Sample {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.histogram {
            Some(h) => write!(
                f,
                "{} => {}",
                metric_string(&self.metric),
                SampleHistogramPair { timestamp: self.timestamp, histogram: h.clone() }
            ),
            None => write!(
                f,
                "{} => {}",
                metric_string(&self.metric),
                SamplePair { timestamp: self.timestamp, value: self.value }
            ),
        }
    }
}

impl std::fmt::Display for SampleStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut vals: Vec<String> = self.values.iter().map(|v| v.to_string()).collect();
        vals.extend(self.histograms.iter().map(|v| v.to_string()));
        write!(f, "{} =>\n{}", metric_string(&self.metric), vals.join("\n"))
    }
}

/// Go `LabelSet.Before`.
pub fn labelset_before(a: &LabelSet, b: &LabelSet) -> bool {
    if a.len() != b.len() {
        return a.len() < b.len();
    }
    let mut names: Vec<&String> = a.keys().chain(b.keys()).collect();
    names.sort();
    for n in names {
        let Some(av) = a.get(n) else { return true };
        let Some(bv) = b.get(n) else { return false };
        if av < bv {
            return true;
        }
        if av > bv {
            return false;
        }
    }
    false
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Scalar(s) => write!(f, "scalar: {} @[{}]", fmt_f(s.value), time_string(s.timestamp)),
            Value::String(s) => write!(f, "{}", s.value),
            Value::Vector(v) => {
                let e: Vec<String> = v.iter().map(|s| s.to_string()).collect();
                write!(f, "{}", e.join("\n"))
            }
            Value::Matrix(m) => {
                let mut c: Vec<&SampleStream> = m.iter().collect();
                c.sort_by(|a, b| {
                    if labelset_before(&a.metric, &b.metric) {
                        std::cmp::Ordering::Less
                    } else if labelset_before(&b.metric, &a.metric) {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Equal
                    }
                });
                let e: Vec<String> = c.iter().map(|s| s.to_string()).collect();
                write!(f, "{}", e.join("\n"))
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// JSON encoding (Go encoding/json with the model's MarshalJSON methods).

fn jstr(s: &str, out: &mut String) {
    json::write_str(s, out)
}

fn json_metric(m: &LabelSet, out: &mut String) {
    out.push('{');
    for (i, (k, v)) in m.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        jstr(k, out);
        out.push(':');
        jstr(v, out);
    }
    out.push('}');
}

fn json_pair(ts: i64, v: f64, out: &mut String) {
    out.push('[');
    out.push_str(&time_string(ts));
    out.push(',');
    jstr(&fmt_f(v), out);
    out.push(']');
}

fn json_hist(h: &SampleHistogram, out: &mut String) {
    out.push_str("{\"count\":");
    jstr(&fmt_f(h.count), out);
    out.push_str(",\"sum\":");
    jstr(&fmt_f(h.sum), out);
    out.push_str(",\"buckets\":");
    if h.buckets.is_empty() {
        out.push_str("null");
    } else {
        out.push('[');
        for (i, b) in h.buckets.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(&format!("[{},", b.boundaries));
            jstr(&fmt_f(b.lower), out);
            out.push(',');
            jstr(&fmt_f(b.upper), out);
            out.push(',');
            jstr(&fmt_f(b.count), out);
            out.push(']');
        }
        out.push(']');
    }
    out.push('}');
}

fn json_hist_pair(ts: i64, h: &SampleHistogram, out: &mut String) {
    out.push('[');
    out.push_str(&time_string(ts));
    out.push(',');
    json_hist(h, out);
    out.push(']');
}

/// Go `json.Marshal(value)`.
pub fn value_to_json(v: &Value) -> String {
    let mut out = String::new();
    match v {
        Value::Scalar(s) => json_pair(s.timestamp, s.value, &mut out),
        Value::String(s) => {
            out.push('[');
            out.push_str(&time_string(s.timestamp));
            out.push(',');
            jstr(&s.value, &mut out);
            out.push(']');
        }
        Value::Vector(vec) => {
            out.push('[');
            for (i, s) in vec.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str("{\"metric\":");
                json_metric(&s.metric, &mut out);
                match &s.histogram {
                    Some(h) => {
                        out.push_str(",\"histogram\":");
                        json_hist_pair(s.timestamp, h, &mut out);
                    }
                    None => {
                        out.push_str(",\"value\":");
                        json_pair(s.timestamp, s.value, &mut out);
                    }
                }
                out.push('}');
            }
            out.push(']');
        }
        Value::Matrix(m) => {
            out.push('[');
            for (i, ss) in m.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str("{\"metric\":");
                json_metric(&ss.metric, &mut out);
                let has_h = !ss.histograms.is_empty();
                let has_v = !ss.values.is_empty();
                if has_v || !has_h {
                    out.push_str(",\"values\":");
                    if ss.values.is_empty() {
                        out.push_str("null");
                    } else {
                        out.push('[');
                        for (j, p) in ss.values.iter().enumerate() {
                            if j > 0 {
                                out.push(',');
                            }
                            json_pair(p.timestamp, p.value, &mut out);
                        }
                        out.push(']');
                    }
                }
                if has_h {
                    out.push_str(",\"histograms\":[");
                    for (j, p) in ss.histograms.iter().enumerate() {
                        if j > 0 {
                            out.push(',');
                        }
                        json_hist_pair(p.timestamp, &p.histogram, &mut out);
                    }
                    out.push(']');
                }
                out.push('}');
            }
            out.push(']');
        }
    }
    out
}

/// Go `json.Marshal([]model.LabelSet)`.
pub fn labelsets_to_json(v: &[LabelSet]) -> String {
    if v.is_empty() {
        return "null".to_string();
    }
    let mut out = String::from("[");
    for (i, m) in v.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        json_metric(m, &mut out);
    }
    out.push(']');
    out
}

/// Go `json.Marshal(model.LabelValues)`.
pub fn strings_to_json(v: &[String]) -> String {
    if v.is_empty() {
        return "null".to_string();
    }
    let mut out = String::from("[");
    for (i, s) in v.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        jstr(s, &mut out);
    }
    out.push(']');
    out
}

// ---------------------------------------------------------------------------------------------------------------
// JSON decoding.

use json::Json;

fn type_err(what: &str, j: &Json) -> String {
    let kind = match j {
        Json::Null => "null",
        Json::Bool(_) => "bool",
        Json::Int(_) | Json::Float(_) => "number",
        Json::Str(_) => "string",
        Json::Arr(_) => "array",
        Json::Obj(_) => "object",
    };
    format!("json: cannot unmarshal {kind} into Go value of type {what}")
}

fn dec_time(j: &Json) -> Result<i64, String> {
    match j {
        Json::Int(i) => Ok(i * 1000),
        Json::Float(f) => Ok((f * 1000.0).round() as i64),
        _ => Err(type_err("model.Time", j)),
    }
}

fn dec_float_string(j: &Json, msg: &str) -> Result<f64, String> {
    match j {
        Json::Str(s) => go_parse_float(s),
        _ => Err(msg.to_string()),
    }
}

/// Go `strconv.ParseFloat(s, 64)` with its error text.
pub fn go_parse_float(s: &str) -> Result<f64, String> {
    let l = s.to_ascii_lowercase();
    let neg = l.starts_with('-');
    let t = l.strip_prefix(['+', '-']).unwrap_or(&l);
    match t {
        "inf" | "infinity" => return Ok(if neg { f64::NEG_INFINITY } else { f64::INFINITY }),
        "nan" => return Ok(f64::NAN),
        _ => {}
    }
    if !t.is_empty() && t.bytes().all(|c| c.is_ascii_digit() || matches!(c, b'.' | b'e' | b'+' | b'-')) {
        if let Ok(f) = s.parse::<f64>() {
            if f.is_infinite() {
                return Err(format!("strconv.ParseFloat: parsing {}: value out of range", quote(s)));
            }
            return Ok(f);
        }
    }
    Err(format!("strconv.ParseFloat: parsing {}: invalid syntax", quote(s)))
}

fn dec_metric(j: &Json) -> Result<LabelSet, String> {
    match j {
        Json::Null => Ok(LabelSet::new()),
        Json::Obj(o) => {
            let mut m = LabelSet::new();
            for (k, v) in o {
                if k.is_empty() {
                    return Err(format!("{} is not a valid label name", quote(k)));
                }
                match v {
                    Json::Str(s) => {
                        m.insert(k.clone(), s.clone());
                    }
                    _ => return Err(type_err("model.LabelValue", v)),
                }
            }
            Ok(m)
        }
        _ => Err(type_err("model.Metric", j)),
    }
}

fn dec_pair(j: &Json) -> Result<SamplePair, String> {
    let Json::Arr(a) = j else { return Err(type_err("[2]json.Unmarshaler", j)) };
    let ts = a.first().map(dec_time).transpose()?.unwrap_or(0);
    let v = match a.get(1) {
        Some(x) => dec_float_string(x, "sample value must be a quoted string")?,
        None => 0.0,
    };
    Ok(SamplePair { timestamp: ts, value: v })
}

fn dec_hist(j: &Json) -> Result<SampleHistogram, String> {
    let Json::Obj(_) = j else { return Err(type_err("model.SampleHistogram", j)) };
    let mut h = SampleHistogram::default();
    if let Some(c) = j.get("count") {
        h.count = dec_float_string(c, "float value must be a quoted string")?;
    }
    if let Some(s) = j.get("sum") {
        h.sum = dec_float_string(s, "float value must be a quoted string")?;
    }
    if let Some(Json::Arr(bs)) = j.get("buckets") {
        for b in bs {
            let Json::Arr(f) = b else { return Err(type_err("model.HistogramBucket", b)) };
            if f.len() != 4 {
                return Err(format!("wrong number of fields: {} != 4", f.len()));
            }
            let boundaries = f[0].as_i64().ok_or_else(|| type_err("int32", &f[0]))? as i32;
            h.buckets.push(HistogramBucket {
                boundaries,
                lower: dec_float_string(&f[1], "float value must be a quoted string")?,
                upper: dec_float_string(&f[2], "float value must be a quoted string")?,
                count: dec_float_string(&f[3], "float value must be a quoted string")?,
            });
        }
    }
    Ok(h)
}

fn dec_hist_pair(j: &Json) -> Result<SampleHistogramPair, String> {
    let Json::Arr(a) = j else { return Err(type_err("[]interface {}", j)) };
    if a.len() != 2 {
        return Err(format!("wrong number of fields: {} != 2", a.len()));
    }
    let ts = dec_time(&a[0])?;
    if a[1].is_null() {
        return Err("histogram is null".to_string());
    }
    Ok(SampleHistogramPair { timestamp: ts, histogram: dec_hist(&a[1])? })
}

/// Go `queryResult.UnmarshalJSON` (client_golang api/prometheus/v1, go1.27 build).
pub fn decode_query_result(j: &Json) -> Result<Value, String> {
    let t = j.get("resultType").and_then(|t| t.as_str()).unwrap_or("");
    let r = j.get("result").cloned().unwrap_or(Json::Null);
    match t {
        "scalar" => {
            let Json::Arr(a) = &r else { return Err(type_err("[2]interface {}", &r)) };
            let ts = a.first().map(dec_time).transpose()?.unwrap_or(0);
            let v = match a.get(1) {
                Some(Json::Str(s)) => go_parse_float(s).map_err(|e| format!("error parsing sample value: {e}"))?,
                Some(x) => return Err(type_err("string", x)),
                None => 0.0,
            };
            Ok(Value::Scalar(Scalar { value: v, timestamp: ts }))
        }
        "vector" => {
            let mut out = Vec::new();
            if let Json::Arr(a) = &r {
                for s in a {
                    let metric = dec_metric(s.get("metric").unwrap_or(&Json::Null))?;
                    if let Some(h) = s.get("histogram").filter(|h| !h.is_null()) {
                        let p = dec_hist_pair(h)?;
                        out.push(Sample { metric, value: 0.0, timestamp: p.timestamp, histogram: Some(p.histogram) });
                    } else {
                        let p = match s.get("value") {
                            Some(v) => dec_pair(v)?,
                            None => SamplePair { timestamp: 0, value: 0.0 },
                        };
                        out.push(Sample { metric, value: p.value, timestamp: p.timestamp, histogram: None });
                    }
                }
            } else if !r.is_null() {
                return Err(type_err("model.Vector", &r));
            }
            Ok(Value::Vector(out))
        }
        "matrix" => {
            let mut out = Vec::new();
            if let Json::Arr(a) = &r {
                for s in a {
                    let metric = dec_metric(s.get("metric").unwrap_or(&Json::Null))?;
                    let mut ss = SampleStream { metric, ..Default::default() };
                    if let Some(Json::Arr(vs)) = s.get("values") {
                        for v in vs {
                            ss.values.push(dec_pair(v)?);
                        }
                    }
                    if let Some(Json::Arr(hs)) = s.get("histograms") {
                        for h in hs {
                            ss.histograms.push(dec_hist_pair(h)?);
                        }
                    }
                    out.push(ss);
                }
            } else if !r.is_null() {
                return Err(type_err("model.Matrix", &r));
            }
            Ok(Value::Matrix(out))
        }
        "string" | "<ValNone>" | "" => {
            Err(format!("unexpected value type {}", quote(if t.is_empty() { "<ValNone>" } else { t })))
        }
        other => Err(format!("unknown value type {}", quote(other))),
    }
}

/// Go `json.Unmarshal(data, &[]model.LabelSet)`.
pub fn decode_labelsets(j: &Json) -> Result<Vec<LabelSet>, String> {
    match j {
        Json::Null => Ok(Vec::new()),
        Json::Arr(a) => a.iter().map(dec_metric).collect(),
        _ => Err(type_err("[]model.LabelSet", j)),
    }
}

/// Go `json.Unmarshal(data, &model.LabelValues)`.
pub fn decode_strings(j: &Json) -> Result<Vec<String>, String> {
    match j {
        Json::Null => Ok(Vec::new()),
        Json::Arr(a) => a
            .iter()
            .map(|v| match v {
                Json::Str(s) => Ok(s.clone()),
                _ => Err(type_err("model.LabelValue", v)),
            })
            .collect(),
        _ => Err(type_err("model.LabelValues", j)),
    }
}
