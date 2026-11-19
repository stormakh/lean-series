//! A port of the parts of `github.com/prometheus/client_golang/api` (v1.25.0) and `api/prometheus/v1` that
//! promtool uses: the HTTP client, `Query`, `QueryRange`, `Series`, `LabelValues` and the API error type.
//!
//! Times are [`Time`]: nanoseconds since the Unix epoch (Go `time.Time` in UTC).

use std::sync::Arc;
use std::time::Duration;

use json::Json;
use yaml::url::GoUrl;

use crate::client_model::{self, LabelSet, Value};

/// Nanoseconds since the Unix epoch (Go `time.Time`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Time(pub i64);

impl Time {
    pub fn now() -> Time {
        let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
        Time(d.as_nanos() as i64)
    }
    pub fn from_unix(sec: i64, nsec: i64) -> Time {
        Time(sec * 1_000_000_000 + nsec)
    }
    /// Go `t.Unix()`.
    pub fn unix(self) -> i64 {
        self.0.div_euclid(1_000_000_000)
    }
    /// Go `t.Nanosecond()`.
    pub fn nanosecond(self) -> i64 {
        self.0.rem_euclid(1_000_000_000)
    }
    /// Go `t.UnixMilli()` / `timestamp.FromTime(t)`.
    pub fn unix_milli(self) -> i64 {
        self.0.div_euclid(1_000_000)
    }
    pub fn add(self, d: Duration) -> Time {
        Time(self.0 + d.as_nanos() as i64)
    }
    pub fn sub_dur(self, d: Duration) -> Time {
        Time(self.0 - d.as_nanos() as i64)
    }
    /// Adds a signed number of nanoseconds (Go `t.Add(d)`).
    pub fn add_nanos(self, d: i64) -> Time {
        Time(self.0 + d)
    }
}

/// Go `v1.Range`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Range {
    pub start: Time,
    pub end: Time,
    pub step: Duration,
}

/// The result of a query: the value, warnings and infos (Go `(model.Value, v1.Warnings, v1.Infos, error)`).
pub type QueryResult = Result<(Value, Vec<String>, Vec<String>), crate::Error>;

/// Go `queryRangeAPI` of rules.go: what the rule importer needs from the API client.
pub trait QueryRangeApi {
    fn query_range(&self, query: &str, r: Range) -> QueryResult;
}

/// Go `v1.Error`.
#[derive(Debug, Clone, PartialEq)]
pub struct ApiError {
    pub typ: String,
    pub msg: String,
    pub detail: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.typ, self.msg)
    }
}

impl std::error::Error for ApiError {}

/// Go `formatTime` of api/prometheus/v1.
pub fn format_time(t: Time) -> String {
    client_model::fmt_f(t.unix() as f64 + t.nanosecond() as f64 / 1e9)
}

/// Go `path.Clean`.
pub fn path_clean(p: &str) -> String {
    if p.is_empty() {
        return ".".to_string();
    }
    let rooted = p.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                if out.last().is_some_and(|l| *l != "..") {
                    out.pop();
                } else if !rooted {
                    out.push("..");
                }
            }
            s => out.push(s),
        }
    }
    let s = out.join("/");
    if rooted {
        format!("/{s}")
    } else if s.is_empty() {
        ".".to_string()
    } else {
        s
    }
}

/// Go `path.Join`.
pub fn path_join(elems: &[&str]) -> String {
    let non_empty: Vec<&str> = elems.iter().copied().filter(|e| !e.is_empty()).collect();
    if non_empty.is_empty() {
        return String::new();
    }
    path_clean(&non_empty.join("/"))
}

/// Go `(*url.URL).JoinPath`.
pub fn url_join_path(u: &GoUrl, elem: &[&str]) -> GoUrl {
    let first = u.escaped_path();
    let mut elems: Vec<String> = vec![first];
    elems.extend(elem.iter().map(|s| s.to_string()));
    let p = if !elems[0].starts_with('/') {
        elems[0] = format!("/{}", elems[0]);
        let refs: Vec<&str> = elems.iter().map(|s| s.as_str()).collect();
        path_join(&refs)[1..].to_string()
    } else {
        let refs: Vec<&str> = elems.iter().map(|s| s.as_str()).collect();
        path_join(&refs)
    };
    let mut p = p;
    if elems.last().is_some_and(|e| e.ends_with('/')) && !p.ends_with('/') {
        p.push('/');
    }
    let mut out = u.clone();
    out.path = p;
    out.raw_path = String::new();
    out
}

/// The default round tripper (Go `api.DefaultRoundTripper`).
pub fn default_round_tripper() -> Arc<dyn net::RoundTripper> {
    net::new_round_tripper_from_config(&net::HttpClientConfig::default_config(), &net::HttpClientOptions::default())
        .expect("default round tripper")
}

/// Go `api.Client` + `v1.API`.
pub struct HttpApi {
    endpoint: GoUrl,
    client: net::Client,
    headers: Vec<(String, String)>,
    /// The context timeout of each call (promtool uses 2 minutes).
    pub timeout: Option<Duration>,
}

/// Go `newAPI` of query.go.
pub fn new_api(
    url: &mut GoUrl,
    rt: Arc<dyn net::RoundTripper>,
    headers: &std::collections::BTreeMap<String, String>,
) -> Result<HttpApi, crate::Error> {
    if url.scheme.is_empty() {
        url.scheme = "http".to_string();
    }
    new_client(&url.to_url_string(), rt, headers.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
}

/// Go `api.NewClient(api.Config{Address, RoundTripper})` with a header-adding round tripper.
pub fn new_client(address: &str, rt: Arc<dyn net::RoundTripper>, headers: Vec<(String, String)>) -> Result<HttpApi, crate::Error> {
    let mut u = yaml::url::parse(address)?;
    u.path = u.path.trim_end_matches('/').to_string();
    u.raw_path = String::new();
    Ok(HttpApi { endpoint: u, client: net::Client::new(rt), headers, timeout: Some(Duration::from_secs(120)) })
}

/// Go `apiResponse`.
struct ApiResponse {
    status: String,
    data: Json,
    error_type: String,
    error: String,
    warnings: Vec<String>,
    infos: Vec<String>,
}

fn strings_of(j: Option<&Json>) -> Vec<String> {
    match j {
        Some(Json::Arr(a)) => a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect(),
        _ => Vec::new(),
    }
}

fn parse_api_response(body: &[u8]) -> Result<ApiResponse, String> {
    let j = Json::parse_bytes(body).map_err(|e| e.to_string())?;
    if !matches!(j, Json::Obj(_)) && !j.is_null() {
        return Err(format!(
            "json: cannot unmarshal {} into Go value of type v1.apiResponse",
            match j {
                Json::Bool(_) => "bool",
                Json::Str(_) => "string",
                Json::Arr(_) => "array",
                _ => "number",
            }
        ));
    }
    Ok(ApiResponse {
        status: j.get("status").and_then(|s| s.as_str()).unwrap_or("").to_string(),
        data: j.get("data").cloned().unwrap_or(Json::Null),
        error_type: j.get("errorType").and_then(|s| s.as_str()).unwrap_or("").to_string(),
        error: j.get("error").and_then(|s| s.as_str()).unwrap_or("").to_string(),
        warnings: strings_of(j.get("warnings")),
        infos: strings_of(j.get("infos")),
    })
}

fn error_type_and_msg_for(code: u16) -> (String, String) {
    match code / 100 {
        4 => ("client_error".to_string(), format!("client error: {code}")),
        5 => ("server_error".to_string(), format!("server error: {code}")),
        _ => ("bad_response".to_string(), format!("bad response code {code}")),
    }
}

/// The decoded result of a call, with the status code of the response (if any).
type DoResult<T> = (Option<u16>, Vec<String>, Vec<String>, Result<T, crate::Error>);

impl HttpApi {
    /// Go `httpClient.URL(ep, args)`.
    fn url(&self, ep: &str, args: &[(&str, &str)]) -> GoUrl {
        let joined = path_join(&[&self.endpoint.path, ep]);
        let comps: Vec<String> = joined
            .split('/')
            .map(|c| match c.strip_prefix(':') {
                Some(name) => args.iter().find(|(k, _)| *k == name).map(|(_, v)| v.to_string()).unwrap_or(c.to_string()),
                None => c.to_string(),
            })
            .collect();
        let mut u = self.endpoint.clone();
        u.path = comps.join("/");
        u
    }

    fn send(&self, method: &str, u: &GoUrl, body: Vec<u8>, form: bool) -> Result<net::Response, crate::Error> {
        let mut req = net::Request::new(method, &u.to_url_string(), body)?;
        if form {
            req.header.set("Content-Type", "application/x-www-form-urlencoded");
        }
        for (k, v) in &self.headers {
            req.header.add(k, v);
        }
        req.timeout = self.timeout;
        req.decompress = true;
        Ok(self.client.do_request(req)?)
    }

    /// Go `apiClientImpl.Do`.
    fn do_call<T>(&self, method: &str, u: &GoUrl, body: Vec<u8>, form: bool, decode: &dyn Fn(&Json) -> Result<T, String>) -> DoResult<T> {
        let resp = match self.send(method, u, body, form) {
            Ok(r) => r,
            Err(e) => return (None, Vec::new(), Vec::new(), Err(e)),
        };
        let code = resp.status;
        let body = resp.body;
        let api_err = |typ: String, msg: String, detail: String| -> crate::Error { Box::new(ApiError { typ, msg, detail }) };
        if code / 100 != 2 {
            if !(code == 422 || code == 400) {
                let (t, m) = error_type_and_msg_for(code);
                return (Some(code), Vec::new(), Vec::new(), Err(api_err(t, m, String::from_utf8_lossy(&body).to_string())));
            }
            let r = match parse_api_response(&body) {
                Ok(r) => r,
                Err(_) => {
                    let (t, m) = error_type_and_msg_for(code);
                    return (Some(code), Vec::new(), Vec::new(), Err(api_err(t, m, String::from_utf8_lossy(&body).to_string())));
                }
            };
            if r.status == "success" {
                return (
                    Some(code),
                    r.warnings,
                    r.infos,
                    Err(api_err("bad_response".into(), "inconsistent body for response code".into(), String::new())),
                );
            }
            let t = if r.error_type.is_empty() { "client_error".to_string() } else { r.error_type };
            let m = if r.error.is_empty() { format!("client error: {code}") } else { r.error };
            return (Some(code), r.warnings, r.infos, Err(api_err(t, m, String::new())));
        }
        let r = match parse_api_response(&body) {
            Ok(r) => r,
            Err(e) => return (Some(code), Vec::new(), Vec::new(), Err(api_err("bad_response".into(), e, String::new()))),
        };
        let decoded = decode(&r.data);
        let v = match decoded {
            Ok(v) => v,
            Err(e) => return (Some(code), r.warnings, r.infos, Err(api_err("bad_response".into(), e, String::new()))),
        };
        if r.status == "error" {
            return (Some(code), r.warnings, r.infos, Err(api_err(r.error_type, r.error, String::new())));
        }
        (Some(code), r.warnings, r.infos, Ok(v))
    }

    /// Go `apiClientImpl.DoGetFallback`.
    fn do_get_fallback<T>(&self, u: &GoUrl, args: &[(String, String)], decode: &dyn Fn(&Json) -> Result<T, String>) -> DoResult<T> {
        let encoded = yaml::url::encode_query(args);
        let r = self.do_call("POST", u, encoded.clone().into_bytes(), true, decode);
        if matches!(r.0, Some(403) | Some(405) | Some(501)) {
            let mut u = u.clone();
            u.raw_query = encoded;
            return self.do_call("GET", &u, Vec::new(), false, decode);
        }
        r
    }

    /// Go `API.Query`.
    pub fn query(&self, query: &str, ts: Option<Time>) -> QueryResult {
        let u = self.url("/api/v1/query", &[]);
        let mut q = vec![("query".to_string(), query.to_string())];
        if let Some(t) = ts {
            q.push(("time".to_string(), format_time(t)));
        }
        let (_, w, i, r) = self.do_get_fallback(&u, &q, &client_model::decode_query_result);
        r.map(|v| (v, w, i))
    }

    /// Go `API.Series`. `None` times are Go's zero time (not sent).
    pub fn series(&self, matches: &[String], start: Option<Time>, end: Option<Time>) -> Result<(Vec<LabelSet>, Vec<String>), crate::Error> {
        let u = self.url("/api/v1/series", &[]);
        let mut q: Vec<(String, String)> = matches.iter().map(|m| ("match[]".to_string(), m.clone())).collect();
        if let Some(t) = start {
            q.push(("start".to_string(), format_time(t)));
        }
        if let Some(t) = end {
            q.push(("end".to_string(), format_time(t)));
        }
        let (_, w, _, r) = self.do_get_fallback(&u, &q, &client_model::decode_labelsets);
        r.map(|v| (v, w))
    }

    /// Go `API.LabelValues`.
    pub fn label_values(
        &self,
        label: &str,
        matches: &[String],
        start: Option<Time>,
        end: Option<Time>,
    ) -> (Vec<String>, Vec<String>, Result<(), crate::Error>) {
        let mut u = self.url("/api/v1/label/:name/values", &[("name", label)]);
        let mut q: Vec<(String, String)> = Vec::new();
        if let Some(t) = start {
            q.push(("start".to_string(), format_time(t)));
        }
        if let Some(t) = end {
            q.push(("end".to_string(), format_time(t)));
        }
        for m in matches {
            q.push(("match[]".to_string(), m.clone()));
        }
        u.raw_query = yaml::url::encode_query(&q);
        let (_, w, _, r) = self.do_call("GET", &u, Vec::new(), false, &client_model::decode_strings);
        match r {
            Ok(v) => (v, w, Ok(())),
            Err(e) => (Vec::new(), w, Err(e)),
        }
    }

    /// Go `API.QueryRange`.
    pub fn query_range_full(&self, query: &str, r: Range) -> QueryResult {
        let u = self.url("/api/v1/query_range", &[]);
        let q = vec![
            ("query".to_string(), query.to_string()),
            ("start".to_string(), format_time(r.start)),
            ("end".to_string(), format_time(r.end)),
            ("step".to_string(), client_model::fmt_f(r.step.as_secs_f64())),
        ];
        let (_, w, i, res) = self.do_get_fallback(&u, &q, &client_model::decode_query_result);
        res.map(|v| (v, w, i))
    }
}

impl QueryRangeApi for HttpApi {
    fn query_range(&self, query: &str, r: Range) -> QueryResult {
        self.query_range_full(query, r)
    }
}
