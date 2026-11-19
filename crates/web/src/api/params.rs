//! Request parameter parsing of API v1: times, durations, limits, matchers (Go `parseTime`, `parseDuration`,
//! `parseTimeParam`, `parseLimitParam`, `parseMatchersParam`, `toHintLimit`), and the request view the handlers
//! read (Go `r.FormValue`, `r.Form`, `r.URL.Query()`).

use model::labels::Matcher;
use storage::Context;

use crate::gotime::{GoDuration, Time, go_parse_float, go_quote};

/// Go `MinTime`: the default start of optional time ranges.
pub fn min_time() -> Time {
    Time::unix(i64::MIN / 1000 + 62135596801, 0)
}

/// Go `MaxTime`: the default end of optional time ranges.
pub fn max_time() -> Time {
    Time::unix(i64::MAX / 1000 - 62135596801, 999999999)
}

/// The parts of an HTTP request the API handlers read.
#[derive(Clone, Debug, Default)]
pub struct ApiRequest {
    pub method: String,
    /// The decoded URL query pairs, in order.
    pub url_query: Vec<(String, String)>,
    /// The decoded form body pairs (POST, PUT and PATCH with `application/x-www-form-urlencoded`).
    pub post_form: Vec<(String, String)>,
    /// Header names are lowercase.
    pub headers: Vec<(String, String)>,
    /// Route params (Go `route.Param`).
    pub params: Vec<(String, String)>,
    /// Cancellation of the request (Go `r.Context()`).
    pub ctx: Context,
    /// The client IP without the port (Go `r.RemoteAddr` host part); IPv6 without brackets.
    pub remote_ip: String,
    /// The full decoded request path, route prefix included (Go `httputil.ContextWithPath`).
    pub path: String,
}

impl ApiRequest {
    /// From an HTTP request: parses the query and, for form posts, the body (Go `ParseForm`).
    pub fn from_http(r: &http::Request) -> ApiRequest {
        let url_query = http::parse_query(&r.query);
        let mut post_form = Vec::new();
        if matches!(r.method.as_str(), "POST" | "PUT" | "PATCH") {
            let ct = r.header("content-type").unwrap_or("");
            let ct = ct.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
            if ct == "application/x-www-form-urlencoded" {
                post_form = r.form();
            }
        }
        ApiRequest {
            method: r.method.clone(),
            url_query,
            post_form,
            headers: r.headers.clone(),
            params: r.params.clone(),
            ctx: Context::background(),
            remote_ip: r.remote_ip.clone(),
            path: http::util::path_decode(&r.path),
        }
    }

    /// A GET request with these query pairs.
    pub fn get(query: &[(&str, &str)]) -> ApiRequest {
        ApiRequest {
            method: "GET".to_string(),
            url_query: query.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            ctx: Context::background(),
            ..Default::default()
        }
    }

    /// A request with this method, URL query and form body.
    pub fn new(method: &str, query: &[(&str, &str)], form: &[(&str, &str)]) -> ApiRequest {
        ApiRequest {
            method: method.to_string(),
            url_query: query.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            post_form: form.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            ctx: Context::background(),
            ..Default::default()
        }
    }

    pub fn with_param(mut self, name: &str, value: &str) -> ApiRequest {
        self.params.push((name.to_string(), value.to_string()));
        self
    }

    pub fn with_header(mut self, name: &str, value: &str) -> ApiRequest {
        self.headers.push((name.to_ascii_lowercase(), value.to_string()));
        self
    }

    pub fn with_ctx(mut self, ctx: Context) -> ApiRequest {
        self.ctx = ctx;
        self
    }

    /// Go `r.FormValue(k)`: the first body value, then the first query value, or "".
    pub fn form_value(&self, k: &str) -> &str {
        for (n, v) in self.post_form.iter().chain(self.url_query.iter()) {
            if n == k {
                return v;
            }
        }
        ""
    }

    /// Go `r.Form[k]`: body values first, then query values.
    pub fn form_values(&self, k: &str) -> Vec<String> {
        self.post_form.iter().chain(self.url_query.iter()).filter(|(n, _)| n == k).map(|(_, v)| v.clone()).collect()
    }

    /// Go `r.URL.Query().Get(k)`.
    pub fn query_value(&self, k: &str) -> &str {
        for (n, v) in self.url_query.iter() {
            if n == k {
                return v;
            }
        }
        ""
    }

    /// Go `r.Header.Get(k)`.
    pub fn header(&self, k: &str) -> &str {
        for (n, v) in self.headers.iter() {
            if n.eq_ignore_ascii_case(k) {
                return v;
            }
        }
        ""
    }

    /// Go `route.Param(ctx, name)`.
    pub fn param(&self, k: &str) -> &str {
        for (n, v) in self.params.iter() {
            if n == k {
                return v;
            }
        }
        ""
    }
}

/// Go `parseTime`.
pub fn parse_time(s: &str) -> Result<Time, String> {
    if let Some(t) = go_parse_float(s) {
        let sec = t.trunc();
        let mut ns = t - sec;
        ns = (ns * 1000.0).round() / 1000.0;
        return Ok(Time::unix(go_f64_to_i64(sec), go_f64_to_i64(ns * 1e9)));
    }
    if let Ok(t) = Time::parse_rfc3339(s) {
        return Ok(t);
    }
    let min = min_time();
    let max = max_time();
    if s == min.format_rfc3339_nano() {
        return Ok(min);
    }
    if s == max.format_rfc3339_nano() {
        return Ok(max);
    }
    Err(format!("cannot parse {} to a valid timestamp", go_quote(s)))
}

/// Go's `int64(f)` conversion on amd64 (out of range gives the minimum int64).
pub fn go_f64_to_i64(f: f64) -> i64 {
    if f.is_nan() || f >= 9223372036854775808.0 || f < -9223372036854775808.0 {
        i64::MIN
    } else {
        f as i64
    }
}

/// Go `parseDuration`: float seconds or a Prometheus duration.
pub fn parse_duration(s: &str) -> Result<GoDuration, String> {
    if let Some(d) = go_parse_float(s) {
        let ts = d * 1e9;
        if ts.is_nan() || ts >= 9223372036854775807.0 || ts < -9223372036854775808.0 {
            return Err(format!("cannot parse {} to a valid duration. It overflows int64", go_quote(s)));
        }
        return Ok(ts as i64);
    }
    if let Ok(d) = promql::parser::duration::parse_duration(s) {
        return Ok(d);
    }
    Err(format!("cannot parse {} to a valid duration", go_quote(s)))
}

/// Go `parseTimeParam`.
pub fn parse_time_param(r: &ApiRequest, name: &str, default: Time) -> Result<Time, String> {
    let val = r.form_value(name);
    if val.is_empty() {
        return Ok(default);
    }
    parse_time(val).map_err(|e| format!("invalid time value for '{}': {}", name, e))
}

/// Go `strconv.Atoi` error text.
pub fn atoi(s: &str) -> Result<i64, String> {
    match s.parse::<i64>() {
        Ok(v) => Ok(v),
        Err(e) => {
            let b = s.strip_prefix(['+', '-']).unwrap_or(s);
            if !b.is_empty() && b.bytes().all(|c| c.is_ascii_digit()) && matches!(e.kind(), std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow) {
                Err(format!("strconv.Atoi: parsing {}: value out of range", go_quote(s)))
            } else {
                Err(format!("strconv.Atoi: parsing {}: invalid syntax", go_quote(s)))
            }
        }
    }
}

/// Go `parseLimitParam`: 0 means no limit.
pub fn parse_limit_param(s: &str) -> Result<i64, String> {
    if s.is_empty() {
        return Ok(0);
    }
    let limit = atoi(s)?;
    if limit < 0 {
        return Err("limit must be non-negative".to_string());
    }
    Ok(limit)
}

/// Go `toHintLimit`.
pub fn to_hint_limit(limit: i64) -> i64 {
    if limit > 0 && limit < i64::MAX { limit + 1 } else { limit }
}

/// Go `strconv.ParseBool`.
pub fn parse_bool(s: &str) -> Result<bool, String> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(format!("strconv.ParseBool: parsing {}: invalid syntax", go_quote(s))),
    }
}

/// Go `parseMatchersParam`.
pub fn parse_matchers_param(p: &promql::parser::Parser, matchers: &[String]) -> Result<Vec<Vec<Matcher>>, String> {
    let sets = p.parse_metric_selectors(matchers).map_err(|e| e.to_string())?;
    'outer: for ms in &sets {
        for m in ms {
            if !m.matches("") {
                continue 'outer;
            }
        }
        return Err("match[] must contain at least one non-empty matcher".to_string());
    }
    Ok(sets)
}

/// Go `model.UnescapeName(name, model.ValueEncodingEscaping)`.
pub fn unescape_name(name: &str) -> String {
    let Some(escaped) = name.strip_prefix("U__") else { return name.to_string() };
    let b = escaped.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(b.len());
    let mut i = 0;
    'top: while i < b.len() {
        if b[i] != b'_' {
            out.push(b[i]);
            i += 1;
            continue;
        }
        i += 1;
        if i >= b.len() {
            return name.to_string();
        }
        if b[i] == b'_' {
            out.push(b'_');
            i += 1;
            continue;
        }
        let mut val: u32 = 0;
        let mut j = 0;
        while i < b.len() {
            if j >= 6 {
                return name.to_string();
            }
            if b[i] == b'_' {
                match char::from_u32(val) {
                    Some(c) => {
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                    }
                    None => return name.to_string(),
                }
                i += 1;
                continue 'top;
            }
            let r = b[i].to_ascii_lowercase();
            val *= 16;
            match r {
                b'0'..=b'9' => val += (r - b'0') as u32,
                b'a'..=b'f' => val += (r - b'a' + 10) as u32,
                _ => return name.to_string(),
            }
            i += 1;
            j += 1;
        }
        return name.to_string();
    }
    String::from_utf8_lossy(&out).into_owned()
}
