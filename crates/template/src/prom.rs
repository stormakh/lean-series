//! Port of Prometheus `template/template.go`: the template functions, [`Expander`] and
//! [`alert_template_data`].

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use model::histogram::FloatHistogram;
use model::regexp::Regexp;
use yaml::url::GoUrl;

use crate::gofmt::{append_float, quote, sprintf};
use crate::gotime::{GoTime, SECOND};
use crate::html;
use crate::text::funcs::query_escape;
use crate::text::{self, FuncMap};
use crate::value::{Func, IntKind, StrKind, StructType, Type, UintKind, Value};

/// Go `QueryFunc`: runs a PromQL query at a time (Unix milliseconds).
pub type QueryFunc =
    Arc<dyn Fn(&storage::Context, &str, i64) -> Result<promql::Vector, promql::Error> + Send + Sync>;

static TEXT_EXPANSIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static TEXT_EXPANSION_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// `prometheus_template_text_expansions_total`.
pub fn text_expansions_total() -> u64 {
    TEXT_EXPANSIONS_TOTAL.load(Ordering::Relaxed)
}

/// `prometheus_template_text_expansion_failures_total`.
pub fn text_expansion_failures_total() -> u64 {
    TEXT_EXPANSION_FAILURES_TOTAL.load(Ordering::Relaxed)
}

/// Go's message when a nil pointer or nil function is used (a recovered runtime panic).
const NIL_DEREF: &str = "runtime error: invalid memory address or nil pointer dereference";

fn string_t() -> Type {
    Type::String(StrKind::String)
}

fn string_map_t() -> Type {
    Type::map_of(string_t())
}

/// The struct type `template.sample` (`Labels map[string]string`, `Value any`).
pub fn sample_type() -> Arc<StructType> {
    static T: OnceLock<Arc<StructType>> = OnceLock::new();
    T.get_or_init(|| {
        Arc::new(StructType {
            name: "template.sample".to_string(),
            fields: vec![("Labels".to_string(), string_map_t()), ("Value".to_string(), Type::Interface)],
        })
    })
    .clone()
}

/// `*template.sample`.
fn sample_ptr_t() -> Type {
    Type::ptr_to(Type::Struct(sample_type()))
}

/// `template.queryResult` (`[]*sample`).
pub fn query_result_type() -> Type {
    Type::Slice(Arc::new(sample_ptr_t()), Some(Arc::from("template.queryResult")))
}

/// A `*template.sample` with the given labels and value (a float, or a pointer to a histogram).
pub fn new_sample<K: Into<String>, V: Into<Arc<str>>>(labels: impl IntoIterator<Item = (K, V)>, value: Value) -> Value {
    Value::ptr(Value::new_struct(sample_type(), vec![Value::string_map(labels), value]))
}

/// The template value of a sample's value: the float, or `*histogram.FloatHistogram`.
fn sample_value(f: f64, h: Option<&Arc<FloatHistogram>>) -> Value {
    match h {
        Some(h) => Value::ptr(Value::Histogram(h.clone())),
        None => Value::Float(f),
    }
}

/// Go `query`: runs the query and converts the vector.
fn query(ctx: &storage::Context, q: &str, ts: i64, query_fn: &Option<QueryFunc>) -> Result<Value, String> {
    let Some(f) = query_fn else { return Err(NIL_DEREF.to_string()) };
    let vector = f(ctx, q, ts).map_err(|e| e.to_string())?;
    let items = vector
        .iter()
        .map(|v| {
            let labels: BTreeMap<String, String> = v.metric.map().into_iter().collect();
            new_sample(labels, sample_value(v.f, v.h.as_ref()))
        })
        .collect();
    Ok(Value::slice_typed(query_result_type(), items))
}

/// The struct of a `*sample` argument, or Go's nil dereference error.
fn sample_struct(v: &Value) -> Result<Arc<crate::value::StructValue>, String> {
    match v {
        Value::Ptr(inner) => match &**inner {
            Value::Struct(s) => Ok(s.clone()),
            _ => Err(NIL_DEREF.to_string()),
        },
        _ => Err(NIL_DEREF.to_string()),
    }
}

fn sample_label(s: &crate::value::StructValue, name: &str) -> String {
    match s.field("Labels") {
        Some(Value::Map(m)) => m.entries.get(name).and_then(|v| v.as_str()).unwrap_or("").to_string(),
        _ => String::new(),
    }
}

/// Go `strconv.ParseFloat(s, 64)` with its error text.
pub fn parse_float(s: &str) -> Result<f64, String> {
    let syntax = || format!("strconv.ParseFloat: parsing {}: invalid syntax", quote(s));
    let (sign, body) = match s.as_bytes().first() {
        Some(b'+') => (1.0, &s[1..]),
        Some(b'-') => (-1.0, &s[1..]),
        _ => (1.0, s),
    };
    let lower = body.to_ascii_lowercase();
    if lower == "inf" || lower == "infinity" {
        return Ok(sign * f64::INFINITY);
    }
    if lower == "nan" && body.len() == s.len() {
        return Ok(f64::NAN);
    }
    match crate::parse::number_parse_float(s) {
        Some(v) => Ok(v),
        None => {
            // Distinguish out of range from syntax errors.
            let clean: String = s.chars().filter(|&c| c != '_').collect();
            match clean.parse::<f64>() {
                Ok(v) if v.is_infinite() => Err(format!("strconv.ParseFloat: parsing {}: value out of range", quote(s))),
                _ => Err(syntax()),
            }
        }
    }
}

/// Go `common_templates.ConvertToFloat`.
pub fn convert_to_float(i: &Value) -> Result<f64, String> {
    match i {
        Value::Float(f) => Ok(*f),
        Value::String(s, StrKind::String) => parse_float(s),
        Value::Int(v, IntKind::Int | IntKind::Int64) => Ok(*v as f64),
        Value::Uint(v, UintKind::Uint | UintKind::Uint64) => Ok(*v as f64),
        Value::Int(d, IntKind::Duration) => {
            let sec = d / SECOND;
            let nsec = d % SECOND;
            Ok(sec as f64 + nsec as f64 / 1e9)
        }
        other => Err(format!("can't convert {} to float", other.type_name())),
    }
}

/// Go's `int64(f)` on amd64: out-of-range values and NaN become math.MinInt64.
fn go_f64_to_i64(f: f64) -> i64 {
    if f.is_nan() || f >= 9.223372036854775808e18 || f < -9.223372036854775808e18 {
        i64::MIN
    } else {
        f as i64
    }
}

/// Go `fmt.Sprintf("%.4g", v)`.
fn g4(v: f64) -> String {
    sprintf("%.4g", &[Value::Float(v)])
}

/// Prometheus `floatToTime` (and `common_templates.FloatToTime`).
pub fn float_to_time(v: f64) -> Result<GoTime, String> {
    if v.is_nan() || v.is_infinite() {
        return Err("value is NaN or Inf".to_string());
    }
    let timestamp = v * 1e9;
    if timestamp > i64::MAX as f64 || timestamp < i64::MIN as f64 {
        let mut s = String::new();
        append_float(&mut s, v, b'g', -1);
        return Err(format!("{} cannot be represented as a nanoseconds timestamp since it overflows int64", s));
    }
    // model.TimeFromUnixNano truncates to milliseconds.
    let ms = go_f64_to_i64(timestamp) / 1_000_000;
    Ok(GoTime::from_unix_milli(ms))
}

/// Go `common_templates.HumanizeDuration`.
pub fn humanize_duration(i: &Value) -> Result<String, String> {
    let mut v = convert_to_float(i)?;
    if v.is_nan() || v.is_infinite() {
        return Ok(g4(v));
    }
    if v == 0.0 {
        return Ok(format!("{}s", g4(v)));
    }
    if v.abs() >= 1.0 {
        let mut sign = "";
        if v < 0.0 {
            sign = "-";
            v = -v;
        }
        let duration = go_f64_to_i64(v);
        let seconds = duration % 60;
        let minutes = (duration / 60) % 60;
        let hours = (duration / 60 / 60) % 24;
        let days = duration / 60 / 60 / 24;
        if days != 0 {
            return Ok(format!("{}{}d {}h {}m {}s", sign, days, hours, minutes, seconds));
        }
        if hours != 0 {
            return Ok(format!("{}{}h {}m {}s", sign, hours, minutes, seconds));
        }
        if minutes != 0 {
            return Ok(format!("{}{}m {}s", sign, minutes, seconds));
        }
        return Ok(format!("{}{}s", sign, g4(v)));
    }
    let mut prefix = "";
    for p in ["m", "u", "n", "p", "f", "a", "z", "y"] {
        if v.abs() >= 1.0 {
            break;
        }
        prefix = p;
        v *= 1000.0;
    }
    Ok(format!("{}{}s", g4(v), prefix))
}

/// Go `common_templates.HumanizeTimestamp`.
pub fn humanize_timestamp(i: &Value) -> Result<String, String> {
    let v = convert_to_float(i)?;
    if v.is_nan() || v.is_infinite() {
        return Ok(g4(v));
    }
    Ok(float_to_time(v)?.string())
}

/// Go `humanize`.
pub fn humanize(i: &Value) -> Result<String, String> {
    let mut v = convert_to_float(i)?;
    if v == 0.0 || v.is_nan() || v.is_infinite() {
        return Ok(g4(v));
    }
    if v.abs() >= 1.0 {
        let mut prefix = "";
        for p in ["k", "M", "G", "T", "P", "E", "Z", "Y"] {
            if v.abs() < 1000.0 {
                break;
            }
            prefix = p;
            v /= 1000.0;
        }
        return Ok(format!("{}{}", g4(v), prefix));
    }
    let mut prefix = "";
    for p in ["m", "u", "n", "p", "f", "a", "z", "y"] {
        if v.abs() >= 1.0 {
            break;
        }
        prefix = p;
        v *= 1000.0;
    }
    Ok(format!("{}{}", g4(v), prefix))
}

/// Go `humanize1024`.
pub fn humanize1024(i: &Value) -> Result<String, String> {
    let mut v = convert_to_float(i)?;
    if v.abs() <= 1.0 || v.is_nan() || v.is_infinite() {
        return Ok(g4(v));
    }
    let mut prefix = "";
    for p in ["ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi", "Yi"] {
        if v.abs() < 1024.0 {
            break;
        }
        prefix = p;
        v /= 1024.0;
    }
    Ok(format!("{}{}", g4(v), prefix))
}

/// Go `net.SplitHostPort`.
pub fn split_host_port(hostport: &str) -> Result<(String, String), String> {
    let err = |why: &str| Err(format!("address {}: {}", hostport, why));
    let Some(i) = hostport.rfind(':') else { return err("missing port in address") };
    let (host, j, k);
    if hostport.starts_with('[') {
        let Some(end) = hostport.find(']') else { return err("missing ']' in address") };
        if end + 1 == hostport.len() {
            return err("missing port in address");
        } else if end + 1 != i {
            if hostport.as_bytes()[end + 1] == b':' {
                return err("too many colons in address");
            }
            return err("missing port in address");
        }
        host = hostport[1..end].to_string();
        j = 1;
        k = end + 1;
    } else {
        host = hostport[..i].to_string();
        if host.contains(':') {
            return err("too many colons in address");
        }
        j = 0;
        k = 0;
    }
    if hostport[j..].contains('[') {
        return err("unexpected '[' in address");
    }
    if hostport[k..].contains(']') {
        return err("unexpected ']' in address");
    }
    Ok((host, hostport[i + 1..].to_string()))
}

/// Go `net.JoinHostPort`.
pub fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') { format!("[{}]:{}", host, port) } else { format!("{}:{}", host, port) }
}

/// Go `netip.ParseAddr(s) == nil`.
fn is_ip_addr(s: &str) -> bool {
    if let Some((addr, zone)) = s.split_once('%') {
        return !zone.is_empty() && addr.contains(':') && addr.parse::<Ipv6Addr>().is_ok();
    }
    if s.contains(':') {
        return s.parse::<Ipv6Addr>().is_ok();
    }
    s.parse::<Ipv4Addr>().is_ok()
}

/// Go `stripPort`.
pub fn strip_port(host_port: &str) -> String {
    match split_host_port(host_port) {
        Ok((host, _)) => host,
        Err(_) => host_port.to_string(),
    }
}

/// Go `stripDomain`.
pub fn strip_domain(host_port: &str) -> String {
    let (mut host, port) = match split_host_port(host_port) {
        Ok(hp) => hp,
        Err(_) => (host_port.to_string(), String::new()),
    };
    if is_ip_addr(&host) {
        return host_port.to_string();
    }
    host = host.split('.').next().unwrap_or("").to_string();
    if !port.is_empty() {
        return join_host_port(&host, &port);
    }
    host
}

/// `cases.Title(language.AmericanEnglish, cases.NoLower).String`: the first letter of each word in title case,
/// the rest unchanged. Words are runs of letters, digits, marks and `_`, joined by `'`, `.`, `:` and `’`
/// between letters (Unicode word boundaries, simplified).
pub fn title(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::with_capacity(s.len());
    let is_word = |c: char| c.is_alphanumeric() || c == '_';
    let mut in_word = false;
    for (i, &c) in chars.iter().enumerate() {
        if is_word(c) {
            if !in_word {
                if c.is_alphabetic() {
                    // Title case of the first letter (Unicode title case equals upper case except for a few
                    // digraphs).
                    let t: String = match c {
                        'ǆ' | 'ǅ' | 'Ǆ' => "ǅ".to_string(),
                        'ǉ' | 'ǈ' | 'Ǉ' => "ǈ".to_string(),
                        'ǌ' | 'ǋ' | 'Ǌ' => "ǋ".to_string(),
                        'ǳ' | 'ǲ' | 'Ǳ' => "ǲ".to_string(),
                        'ß' => "Ss".to_string(),
                        _ => c.to_uppercase().collect(),
                    };
                    out.push_str(&t);
                } else {
                    out.push(c);
                }
                in_word = true;
            } else {
                out.push(c);
            }
        } else {
            let joins = matches!(c, '\'' | '.' | ':' | '’' | '\u{b7}')
                && in_word
                && i > 0
                && chars[i - 1].is_alphabetic()
                && chars.get(i + 1).is_some_and(|n| n.is_alphabetic());
            out.push(c);
            if !joins {
                in_word = false;
            }
        }
    }
    out
}

/// Go `strutil.GraphLinkForExpression`.
pub fn graph_link_for_expression(expr: &str) -> String {
    format!("/graph?g0.expr={}&g0.tab=0", query_escape(expr))
}

/// Go `strutil.TableLinkForExpression`.
pub fn table_link_for_expression(expr: &str) -> String {
    format!("/graph?g0.expr={}&g0.tab=1", query_escape(expr))
}

fn arg_str(v: &Value) -> &str {
    v.as_str().unwrap_or("")
}

fn str_fn(f: impl Fn(&str) -> String + Send + Sync + 'static) -> Func {
    Func::new(vec![string_t()], string_t(), false, move |a| Ok(Value::string(f(arg_str(&a[0])))))
}

fn any_str_err_fn(f: impl Fn(&Value) -> Result<String, String> + Send + Sync + 'static) -> Func {
    Func::new(vec![Type::Interface], string_t(), true, move |a| f(&a[0]).map(Value::string))
}

/// Builds the Prometheus template function map (Go `NewTemplateExpander`'s funcMap).
fn func_map(ctx: storage::Context, timestamp_ms: i64, query_fn: Option<QueryFunc>, external_url: Option<GoUrl>) -> FuncMap {
    let mut m = FuncMap::new();
    let qr = query_result_type();
    let sp = sample_ptr_t();
    {
        let ctx = ctx.clone();
        m.insert(
            "query".into(),
            Func::new(vec![string_t()], qr.clone(), true, move |a| query(&ctx, arg_str(&a[0]), timestamp_ms, &query_fn)),
        );
    }
    m.insert(
        "first".into(),
        Func::new(vec![qr.clone()], sp.clone(), true, |a| match &a[0] {
            Value::Slice(s) if !s.items.is_empty() => Ok(s.items[0].clone()),
            _ => Err("first() called on vector with no elements".to_string()),
        }),
    );
    m.insert(
        "label".into(),
        Func::new(vec![string_t(), sp.clone()], string_t(), false, |a| {
            let s = sample_struct(&a[1])?;
            Ok(Value::string(sample_label(&s, arg_str(&a[0]))))
        }),
    );
    m.insert(
        "value".into(),
        Func::new(vec![sp.clone()], Type::Interface, false, |a| {
            let s = sample_struct(&a[0])?;
            Ok(s.field("Value").cloned().unwrap_or(Value::Invalid))
        }),
    );
    m.insert(
        "strvalue".into(),
        Func::new(vec![sp.clone()], string_t(), false, |a| {
            let s = sample_struct(&a[0])?;
            Ok(Value::string(sample_label(&s, "__value__")))
        }),
    );
    m.insert(
        "args".into(),
        Func::variadic(vec![Type::Interface], Type::map_of(Type::Interface), false, |a| {
            let entries = a.iter().enumerate().map(|(i, v)| (format!("arg{}", i), v.clone())).collect();
            Ok(Value::map(Type::Interface, entries))
        }),
    );
    m.insert(
        "reReplaceAll".into(),
        Func::new(vec![string_t(), string_t(), string_t()], string_t(), false, |a| {
            let pattern = arg_str(&a[0]);
            let re = Regexp::compile(pattern).map_err(|e| format!("regexp: Compile({}): {}", quote(pattern), e))?;
            Ok(Value::string(re.replace_all_string(arg_str(&a[2]), arg_str(&a[1]))))
        }),
    );
    m.insert(
        "safeHtml".into(),
        Func::new(vec![string_t()], Type::String(StrKind::Html), false, |a| Ok(Value::html(arg_str(&a[0])))),
    );
    m.insert(
        "match".into(),
        Func::new(vec![string_t(), string_t()], Type::Bool, true, |a| {
            let re = Regexp::compile(arg_str(&a[0])).map_err(|e| e.to_string())?;
            Ok(Value::Bool(re.is_match(arg_str(&a[1]))))
        }),
    );
    m.insert("title".into(), str_fn(title));
    m.insert("toUpper".into(), str_fn(|s| model::unicode::str_to_upper(s).into_owned()));
    m.insert("toLower".into(), str_fn(|s| model::unicode::str_to_lower(s).into_owned()));
    m.insert("graphLink".into(), str_fn(graph_link_for_expression));
    m.insert("tableLink".into(), str_fn(table_link_for_expression));
    m.insert(
        "sortByLabel".into(),
        Func::new(vec![string_t(), qr.clone()], qr.clone(), false, |a| {
            let label = arg_str(&a[0]);
            let Value::Slice(s) = &a[1] else { return Ok(a[1].clone()) };
            let mut items: Vec<(String, Value)> = s
                .items
                .iter()
                .map(|v| {
                    let key = sample_struct(v).map(|st| sample_label(&st, label));
                    (key.unwrap_or_default(), v.clone())
                })
                .collect();
            items.sort_by(|x, y| x.0.cmp(&y.0));
            Ok(Value::slice_typed(s.ty.clone(), items.into_iter().map(|(_, v)| v).collect()))
        }),
    );
    m.insert("stripPort".into(), str_fn(strip_port));
    m.insert("stripDomain".into(), str_fn(strip_domain));
    m.insert("humanize".into(), any_str_err_fn(humanize));
    m.insert("humanize1024".into(), any_str_err_fn(humanize1024));
    m.insert("humanizeDuration".into(), any_str_err_fn(humanize_duration));
    m.insert(
        "humanizePercentage".into(),
        any_str_err_fn(|i| {
            let v = convert_to_float(i)?;
            Ok(sprintf("%.4g%%", &[Value::Float(v * 100.0)]))
        }),
    );
    m.insert("humanizeTimestamp".into(), any_str_err_fn(humanize_timestamp));
    m.insert(
        "toTime".into(),
        Func::new(vec![Type::Interface], Type::ptr_to(Type::Time), true, |a| {
            let v = convert_to_float(&a[0])?;
            Ok(Value::ptr(Value::Time(float_to_time(v)?)))
        }),
    );
    m.insert(
        "toDuration".into(),
        Func::new(vec![Type::Interface], Type::ptr_to(Type::Int(IntKind::Duration)), true, |a| {
            let v = convert_to_float(&a[0])?;
            Ok(Value::ptr(Value::duration(go_f64_to_i64(v * SECOND as f64))))
        }),
    );
    m.insert(
        "now".into(),
        Func::new(vec![], Type::Float64, false, move |_| Ok(Value::Float(timestamp_ms as f64 / 1000.0))),
    );
    {
        let u = external_url.clone();
        m.insert(
            "pathPrefix".into(),
            Func::new(vec![], string_t(), false, move |_| match &u {
                Some(u) => Ok(Value::string(u.path.as_str())),
                None => Err(NIL_DEREF.to_string()),
            }),
        );
    }
    {
        let u = external_url;
        m.insert(
            "externalURL".into(),
            Func::new(vec![], string_t(), false, move |_| match &u {
                Some(u) => Ok(Value::string(u.to_url_string())),
                None => Err(NIL_DEREF.to_string()),
            }),
        );
    }
    m.insert(
        "parseDuration".into(),
        Func::new(vec![string_t()], Type::Float64, true, |a| {
            let s = arg_str(&a[0]);
            let d = if let Some(rest) = s.strip_prefix('-') {
                -promql::parser::duration::parse_duration(rest)?
            } else {
                promql::parser::duration::parse_duration(s)?
            };
            Ok(Value::Float(d as f64 / SECOND as f64))
        }),
    );
    m.insert("urlQueryEscape".into(), str_fn(query_escape));
    m
}

/// The names of all template functions (Go `templateFunctions`, for the feature registry).
pub fn template_function_names() -> Vec<String> {
    let mut v: Vec<String> = func_map(storage::Context::default(), 0, None, Some(GoUrl::default())).into_keys().collect();
    v.sort();
    v
}

/// Executes templates in text or HTML mode with the Prometheus template functions (Go `Expander`).
pub struct Expander {
    text: String,
    name: String,
    data: Value,
    func_map: FuncMap,
    options: Vec<String>,
}

impl Expander {
    /// Go `NewTemplateExpander`. `options` `None` means Go's nil (`missingkey=zero`); `Some(&[])` sets no option.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: storage::Context,
        text: &str,
        name: &str,
        data: Value,
        timestamp_ms: i64,
        query_func: Option<QueryFunc>,
        external_url: Option<&GoUrl>,
        options: Option<&[String]>,
    ) -> Expander {
        let options = match options {
            None => vec!["missingkey=zero".to_string()],
            Some(o) => o.to_vec(),
        };
        Expander {
            text: text.to_string(),
            name: name.to_string(),
            data,
            func_map: func_map(ctx, timestamp_ms, query_func, external_url.cloned()),
            options,
        }
    }

    /// Go `Funcs`: adds functions, replacing existing ones with the same name.
    pub fn funcs(&mut self, fm: FuncMap) {
        self.func_map.extend(fm);
    }

    /// Go `Expand`: expands the template in text mode.
    pub fn expand(&self) -> Result<String, String> {
        TEXT_EXPANSIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
        let r = self.expand_inner();
        if r.is_err() {
            TEXT_EXPANSION_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        r
    }

    fn expand_inner(&self) -> Result<String, String> {
        let tmpl = text::Template::new(&self.name);
        tmpl.funcs(&self.func_map);
        tmpl.option(&self.options).map_err(|p| format!("panic expanding template {}: {}", self.name, p))?;
        tmpl.parse(&self.text).map_err(|e| format!("error parsing template {}: {}", self.name, e))?;
        tmpl.execute(&self.data).map_err(|e| format!("error executing template {}: {}", self.name, e))
    }

    /// Go `ExpandHTML`: expands the template with HTML escaping, with templates read from `template_files`.
    pub fn expand_html(&self, template_files: &[String]) -> Result<String, String> {
        let tmpl = html::Template::new(&self.name);
        tmpl.funcs(&self.func_map);
        tmpl.option(&self.options).map_err(|p| format!("panic expanding template {}: {}", self.name, p))?;
        let weak = tmpl.downgrade();
        tmpl.func(
            "tmpl",
            Func::new(vec![string_t(), Type::Interface], Type::String(StrKind::Html), true, move |a| {
                let Some(t) = weak.upgrade() else { return Err(NIL_DEREF.to_string()) };
                t.execute_template(arg_str(&a[0]), &a[1]).map(Value::html)
            }),
        );
        tmpl.parse(&self.text).map_err(|e| format!("error parsing template {}: {}", self.name, e))?;
        if !template_files.is_empty() {
            tmpl.parse_files(template_files)
                .map_err(|e| format!("error parsing template files for {}: {}", self.name, e))?;
        }
        tmpl.execute(&self.data).map_err(|e| format!("error executing template {}: {}", self.name, e))
    }

    /// Go `ParseTest`: parses the template and returns the error, if any.
    pub fn parse_test(&self) -> Result<(), String> {
        let tmpl = text::Template::new(&self.name);
        tmpl.funcs(&self.func_map);
        tmpl.option(&["missingkey=zero"])?;
        tmpl.parse(&self.text)?;
        Ok(())
    }
}

/// The struct type of [`alert_template_data`].
pub fn alert_template_data_type() -> Arc<StructType> {
    static T: OnceLock<Arc<StructType>> = OnceLock::new();
    T.get_or_init(|| {
        Arc::new(StructType {
            name: "struct { Labels map[string]string; ExternalLabels map[string]string; ExternalURL string; Value interface {} }"
                .to_string(),
            fields: vec![
                ("Labels".to_string(), string_map_t()),
                ("ExternalLabels".to_string(), string_map_t()),
                ("ExternalURL".to_string(), string_t()),
                ("Value".to_string(), Type::Interface),
            ],
        })
    })
    .clone()
}

/// Go `AlertTemplateData`: the data of alert templates (`.Labels`, `.ExternalLabels`, `.ExternalURL`,
/// `.Value`; the value is the float, or the `*FloatHistogram` of a histogram sample).
pub fn alert_template_data<K1, V1, K2, V2>(
    labels: impl IntoIterator<Item = (K1, V1)>,
    external_labels: impl IntoIterator<Item = (K2, V2)>,
    external_url: &str,
    smpl: &promql::Sample,
) -> Value
where
    K1: Into<String>,
    V1: Into<Arc<str>>,
    K2: Into<String>,
    V2: Into<Arc<str>>,
{
    Value::new_struct(
        alert_template_data_type(),
        vec![
            Value::string_map(labels),
            Value::string_map(external_labels),
            Value::string(external_url),
            sample_value(smpl.f, smpl.h.as_ref()),
        ],
    )
}
