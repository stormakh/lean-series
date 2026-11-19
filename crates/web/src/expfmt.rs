//! Port of the writing half of `github.com/prometheus/common/expfmt` (v0.72.1) with the `io.prometheus.client`
//! metric model it encodes (`client_model` v0.6.3 `metrics.proto`): content negotiation (`NegotiateAccept`,
//! `goautoneg`), name escaping (`model.EscapeMetricFamily`), and the encoders for the Prometheus text format 0.0.4,
//! OpenMetrics 1.0, delimited protobuf, protobuf text and compact protobuf text.
//!
//! Differences: Go's `prototext` adds an extra space after separators, chosen once per Go build (`detrand`);
//! this port writes what the Prometheus 3.16.0 release writes (two spaces); exemplars and created timestamps are not modeled; the
//! OpenMetrics 2.0 encoder is not ported (no server path negotiates it).

use model::histogram::gofmt::write_g;

// ---------------------------------------------------------------------------------------------------------------
// The metric model (Go `dto`).

/// Go `dto.MetricType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DtoType {
    Counter = 0,
    Gauge = 1,
    Summary = 2,
    Untyped = 3,
    Histogram = 4,
    GaugeHistogram = 5,
}

impl DtoType {
    fn proto_name(self) -> &'static str {
        match self {
            DtoType::Counter => "COUNTER",
            DtoType::Gauge => "GAUGE",
            DtoType::Summary => "SUMMARY",
            DtoType::Untyped => "UNTYPED",
            DtoType::Histogram => "HISTOGRAM",
            DtoType::GaugeHistogram => "GAUGE_HISTOGRAM",
        }
    }
}

/// Go `dto.MetricFamily`. `help` and `unit` are optional fields (None is Go's nil).
#[derive(Clone, Debug, PartialEq)]
pub struct DtoFamily {
    pub name: String,
    pub help: Option<String>,
    pub typ: DtoType,
    pub unit: Option<String>,
    pub metrics: Vec<DtoMetric>,
}

/// Go `dto.Metric`.
#[derive(Clone, Debug, PartialEq)]
pub struct DtoMetric {
    pub labels: Vec<(String, String)>,
    pub value: DtoValue,
    pub timestamp_ms: Option<i64>,
}

/// The value field of a `dto.Metric` that is set.
#[derive(Clone, Debug, PartialEq)]
pub enum DtoValue {
    Counter(f64),
    Gauge(f64),
    Untyped(f64),
    Summary(DtoSummary),
    Histogram(DtoHistogram),
}

/// Go `dto.Summary`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DtoSummary {
    pub sample_count: u64,
    pub sample_sum: f64,
    /// `(quantile, value)`.
    pub quantiles: Vec<(f64, f64)>,
}

/// Go `dto.Histogram`: optional scalar fields are `Option` (proto2 presence).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DtoHistogram {
    pub sample_count: Option<u64>,
    pub sample_count_float: Option<f64>,
    pub sample_sum: Option<f64>,
    pub buckets: Vec<DtoBucket>,
    pub schema: Option<i32>,
    pub zero_threshold: Option<f64>,
    pub zero_count: Option<u64>,
    pub zero_count_float: Option<f64>,
    /// `(offset, length)`.
    pub negative_spans: Vec<(i32, u32)>,
    pub negative_deltas: Vec<i64>,
    pub negative_counts: Vec<f64>,
    pub positive_spans: Vec<(i32, u32)>,
    pub positive_deltas: Vec<i64>,
    pub positive_counts: Vec<f64>,
}

impl DtoHistogram {
    fn count_float(&self) -> f64 {
        self.sample_count_float.unwrap_or(0.0)
    }
    fn count(&self) -> u64 {
        self.sample_count.unwrap_or(0)
    }
}

/// Go `dto.Bucket`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DtoBucket {
    pub cumulative_count: Option<u64>,
    pub cumulative_count_float: Option<f64>,
    pub upper_bound: f64,
}

// ---------------------------------------------------------------------------------------------------------------
// Formats and negotiation.

pub const TEXT_VERSION: &str = "0.0.4";
pub const PROTO_TYPE: &str = "application/vnd.google.protobuf";
pub const PROTO_PROTOCOL: &str = "io.prometheus.client.MetricFamily";
pub const OPENMETRICS_TYPE: &str = "application/openmetrics-text";

pub const FMT_TEXT: &str = "text/plain; version=0.0.4; charset=utf-8";
pub const FMT_PROTO_DELIM: &str = "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited";
pub const FMT_PROTO_TEXT: &str = "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=text";
pub const FMT_PROTO_COMPACT: &str =
    "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=compact-text";
pub const FMT_OPENMETRICS_1_0_0: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";
pub const FMT_OPENMETRICS_0_0_1: &str = "application/openmetrics-text; version=0.0.1; charset=utf-8";

/// Go `expfmt.DefaultAcceptedFormats()` (what promhttp negotiates by default).
pub const DEFAULT_ACCEPTED_FORMATS: [&str; 4] = [FMT_PROTO_DELIM, FMT_PROTO_TEXT, FMT_PROTO_COMPACT, FMT_TEXT];
/// Go `expfmt.DefaultOpenMetricsAcceptedFormats()`.
pub const DEFAULT_OPENMETRICS_ACCEPTED_FORMATS: [&str; 6] =
    [FMT_OPENMETRICS_1_0_0, FMT_OPENMETRICS_0_0_1, FMT_PROTO_DELIM, FMT_PROTO_TEXT, FMT_PROTO_COMPACT, FMT_TEXT];

/// Go `expfmt.FormatType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormatType {
    Unknown,
    ProtoCompact,
    ProtoDelim,
    ProtoText,
    TextPlain,
    OpenMetrics,
}

/// Go `model.EscapingScheme`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EscapingScheme {
    NoEscaping,
    Underscores,
    Dots,
    Values,
}

impl EscapingScheme {
    pub fn as_str(self) -> &'static str {
        match self {
            EscapingScheme::NoEscaping => "allow-utf-8",
            EscapingScheme::Underscores => "underscores",
            EscapingScheme::Dots => "dots",
            EscapingScheme::Values => "values",
        }
    }
    fn parse(s: &str) -> Option<EscapingScheme> {
        match s {
            "allow-utf-8" => Some(EscapingScheme::NoEscaping),
            "underscores" => Some(EscapingScheme::Underscores),
            "dots" => Some(EscapingScheme::Dots),
            "values" => Some(EscapingScheme::Values),
            _ => None,
        }
    }
}

/// Go `model.NameEscapingScheme` (the default).
pub const NAME_ESCAPING_SCHEME: EscapingScheme = EscapingScheme::Underscores;

/// A negotiated format (Go `expfmt.Format`, the Content-Type value).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Format(pub String);

fn split_params(s: &str) -> (String, Vec<(String, String)>) {
    let mut toks = s.split(';');
    let first = toks.next().unwrap_or("").trim().to_string();
    let mut params = Vec::new();
    for t in toks {
        let args: Vec<&str> = t.split('=').collect();
        if args.len() != 2 {
            continue;
        }
        params.push((args[0].trim().to_string(), args[1].trim().to_string()));
    }
    (first, params)
}

fn param<'a>(ps: &'a [(String, String)], k: &str) -> Option<&'a str> {
    // Go fills a map, so the last value wins.
    ps.iter().rev().find(|(n, _)| n == k).map(|(_, v)| v.as_str())
}

impl Format {
    /// Go `Format.FormatType`.
    pub fn format_type(&self) -> FormatType {
        let (t, params) = split_params(&self.0);
        match t.as_str() {
            PROTO_TYPE => {
                if param(&params, "proto") != Some(PROTO_PROTOCOL) {
                    return FormatType::Unknown;
                }
                match param(&params, "encoding").unwrap_or("") {
                    "delimited" => FormatType::ProtoDelim,
                    "text" => FormatType::ProtoText,
                    "compact-text" => FormatType::ProtoCompact,
                    _ => FormatType::Unknown,
                }
            }
            OPENMETRICS_TYPE => {
                if param(&params, "charset") != Some("utf-8") {
                    return FormatType::Unknown;
                }
                FormatType::OpenMetrics
            }
            "text/plain" => match param(&params, "version") {
                None => FormatType::TextPlain,
                Some(TEXT_VERSION) => FormatType::TextPlain,
                Some(_) => FormatType::Unknown,
            },
            _ => FormatType::Unknown,
        }
    }

    /// Go `Format.ToEscapingScheme`.
    pub fn to_escaping_scheme(&self) -> EscapingScheme {
        for p in self.0.split(';') {
            let toks: Vec<&str> = p.split('=').collect();
            if toks.len() != 2 {
                continue;
            }
            if toks[0].trim() == "escaping" {
                return EscapingScheme::parse(toks[1].trim()).unwrap_or(NAME_ESCAPING_SCHEME);
            }
        }
        NAME_ESCAPING_SCHEME
    }
}

/// One clause of an Accept header (Go `goautoneg.Accept`).
#[derive(Clone, Debug, PartialEq)]
pub struct Accept {
    pub typ: String,
    pub sub_type: String,
    pub q: f64,
    pub params: Vec<(String, String)>,
}

fn next_split<'a>(s: &'a str, sep: char) -> (&'a str, &'a str) {
    match s.find(sep) {
        Some(i) => (&s[..i], &s[i + 1..]),
        None => (s, ""),
    }
}

fn trim_spaces(s: &str) -> &str {
    s.trim_matches(' ')
}

/// Go `goautoneg.ParseAccept`: the clauses sorted by preference.
pub fn parse_accept(header: &str) -> Vec<Accept> {
    let mut accept: Vec<Accept> = Vec::new();
    let mut remaining = header;
    while !remaining.is_empty() {
        let (part, rest) = next_split(remaining, ',');
        remaining = rest;
        let part = trim_spaces(part);
        let mut a = Accept { typ: String::new(), sub_type: String::new(), q: 1.0, params: Vec::new() };
        let (sp, remaining_part) = next_split(part, ';');
        let (sp0, sp_rem) = next_split(sp, '/');
        a.typ = trim_spaces(sp0).to_string();
        if sp_rem.is_empty() {
            if a.typ == "*" {
                a.sub_type = "*".to_string();
            } else {
                continue;
            }
        } else {
            let (sp1, sp_rem2) = next_split(sp_rem, '/');
            if !sp_rem2.is_empty() {
                continue;
            }
            a.sub_type = trim_spaces(sp1).to_string();
        }
        let mut rp = remaining_part;
        while !rp.is_empty() {
            let (sp, r) = next_split(rp, ';');
            rp = r;
            let (k, v_rem) = next_split(sp, '=');
            if v_rem.is_empty() {
                continue;
            }
            let (v, extra) = next_split(v_rem, '=');
            if !extra.is_empty() {
                continue;
            }
            let token = trim_spaces(k);
            if token == "q" {
                // Go: strconv.ParseFloat(sp1, 32), errors give 0.
                a.q = v.parse::<f32>().map(|f| f as f64).unwrap_or(0.0);
            } else {
                let token = token.to_string();
                let val = trim_spaces(v).to_string();
                match a.params.iter_mut().find(|(n, _)| *n == token) {
                    Some(p) => p.1 = val,
                    None => a.params.push((token, val)),
                }
            }
        }
        accept.push(a);
    }
    // Go sort.Sort with this Less; for the short slices of real headers Go's pdqsort is an insertion sort.
    let less = |a: &Accept, b: &Accept| -> bool {
        if a.q > b.q {
            return true;
        }
        if a.typ != "*" && b.typ == "*" {
            return true;
        }
        a.sub_type != "*" && b.sub_type == "*"
    };
    for i in 1..accept.len() {
        let mut j = i;
        while j > 0 && less(&accept[j], &accept[j - 1]) {
            accept.swap(j, j - 1);
            j -= 1;
        }
    }
    accept
}

fn ac_param<'a>(a: &'a Accept, k: &str) -> &'a str {
    a.params.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str()).unwrap_or("")
}

/// Go `matchFormat`.
fn match_format(ac: &Accept, f: &str) -> bool {
    let parsed = parse_accept(f);
    let Some(target) = parsed.first() else { return false };
    if ac.typ != "*" && ac.typ != target.typ {
        return false;
    }
    if ac.sub_type != "*" && ac.sub_type != target.sub_type {
        return false;
    }
    let mut ac_version = ac_param(ac, "version").to_string();
    if ac_version.is_empty() && format!("{}/{}", ac.typ, ac.sub_type) == OPENMETRICS_TYPE {
        ac_version = "0.0.1".to_string();
    }
    if ac_version.is_empty() && ac.typ == "text" && ac.sub_type == "plain" {
        ac_version = TEXT_VERSION.to_string();
    }
    for (k, v) in &target.params {
        if k == "charset" {
            continue;
        }
        let ac_val = if k == "version" { ac_version.as_str() } else { ac_param(ac, k) };
        if ac_val != v {
            return false;
        }
    }
    true
}

/// Go `expfmt.NegotiateAccept`: the format for the request's Accept header, out of `accepted` (in order of
/// preference), with the escaping parameter appended.
pub fn negotiate_accept(accept_header: &str, accepted: &[&str]) -> Format {
    let mut escaping = format!("; escaping={}", NAME_ESCAPING_SCHEME.as_str());
    for ac in parse_accept(accept_header) {
        let e = ac_param(&ac, "escaping");
        if !e.is_empty() && EscapingScheme::parse(e).is_some() {
            escaping = format!("; escaping={}", e);
        }
        for f in accepted {
            if match_format(&ac, f) {
                return Format(format!("{}{}", f, escaping));
            }
        }
    }
    for f in accepted {
        if Format(f.to_string()).format_type() == FormatType::TextPlain {
            return Format(format!("{}{}", f, escaping));
        }
    }
    if let Some(f) = accepted.first() {
        return Format(format!("{}{}", f, escaping));
    }
    Format(format!("{}{}", FMT_TEXT, escaping))
}

// ---------------------------------------------------------------------------------------------------------------
// Escaping.

fn is_valid_legacy_rune(c: char, i: usize) -> bool {
    c.is_ascii_alphabetic() || c == '_' || c == ':' || (c.is_ascii_digit() && i > 0)
}

/// Go `model.IsValidLegacyMetricName`.
pub fn is_valid_legacy_metric_name(n: &str) -> bool {
    !n.is_empty() && n.char_indices().all(|(i, c)| is_valid_legacy_rune(c, i))
}

/// Go `model.EscapeName`.
pub fn escape_name(name: &str, scheme: EscapingScheme) -> String {
    if name.is_empty() {
        return String::new();
    }
    let mut out = String::with_capacity(name.len() + 4);
    match scheme {
        EscapingScheme::NoEscaping => return name.to_string(),
        EscapingScheme::Underscores => {
            if is_valid_legacy_metric_name(name) {
                return name.to_string();
            }
            for (i, c) in name.char_indices() {
                out.push(if is_valid_legacy_rune(c, i) { c } else { '_' });
            }
        }
        EscapingScheme::Dots => {
            for (i, c) in name.char_indices() {
                if c == '_' {
                    out.push_str("__");
                } else if c == '.' {
                    out.push_str("_dot_");
                } else if is_valid_legacy_rune(c, i) {
                    out.push(c);
                } else {
                    out.push_str("__");
                }
            }
        }
        EscapingScheme::Values => {
            if is_valid_legacy_metric_name(name) {
                return name.to_string();
            }
            out.push_str("U__");
            for (i, c) in name.char_indices() {
                if c == '_' {
                    out.push_str("__");
                } else if is_valid_legacy_rune(c, i) {
                    out.push(c);
                } else if c == '\u{FFFD}' {
                    // Go: an invalid UTF-8 byte decodes to RuneError; Rust strings are valid UTF-8.
                    out.push_str("_FFFD_");
                } else {
                    out.push('_');
                    out.push_str(&format!("{:x}", c as u32));
                    out.push('_');
                }
            }
        }
    }
    out
}

/// Go `model.EscapeMetricFamily` (returns the input unchanged when nothing needs escaping).
pub fn escape_metric_family(v: &DtoFamily, scheme: EscapingScheme) -> std::borrow::Cow<'_, DtoFamily> {
    if scheme == EscapingScheme::NoEscaping {
        return std::borrow::Cow::Borrowed(v);
    }
    let name_ok = is_valid_legacy_metric_name(&v.name) || v.name.is_empty();
    let needs = |m: &DtoMetric| {
        m.labels.iter().any(|(n, val)| {
            if n == "__name__" { !is_valid_legacy_metric_name(val) } else { !is_valid_legacy_metric_name(n) }
        })
    };
    if name_ok && !v.metrics.iter().any(needs) {
        return std::borrow::Cow::Borrowed(v);
    }
    let mut out = v.clone();
    if !name_ok {
        out.name = escape_name(&v.name, scheme);
    }
    for m in out.metrics.iter_mut() {
        for (n, val) in m.labels.iter_mut() {
            if n == "__name__" {
                if !is_valid_legacy_metric_name(val) {
                    *val = escape_name(val, scheme);
                }
            } else if !is_valid_legacy_metric_name(n) {
                *n = escape_name(n, scheme);
            }
        }
    }
    std::borrow::Cow::Owned(out)
}

// ---------------------------------------------------------------------------------------------------------------
// Encoders.

/// An encoder for one format (Go `expfmt.NewEncoder`). Call `encode` per family, then `close`.
pub struct Encoder {
    typ: FormatType,
    escaping: EscapingScheme,
    openmetrics: bool,
}

impl Encoder {
    /// Go `expfmt.NewEncoder(w, format)`. An unknown format panics in Go; here it falls back to the text format.
    pub fn new(format: &Format) -> Encoder {
        let typ = format.format_type();
        Encoder { typ, escaping: format.to_escaping_scheme(), openmetrics: typ == FormatType::OpenMetrics }
    }

    /// Encodes one family into `out`.
    pub fn encode(&self, out: &mut Vec<u8>, mf: &DtoFamily) -> Result<(), String> {
        let mf = escape_metric_family(mf, self.escaping);
        match self.typ {
            FormatType::ProtoDelim => {
                let mut msg = Vec::with_capacity(128);
                proto_family(&mut msg, &mf);
                put_varint(out, msg.len() as u64);
                out.extend_from_slice(&msg);
                Ok(())
            }
            FormatType::ProtoText => {
                let mut s = String::new();
                prototext_family(&mut s, &mf, true);
                out.extend_from_slice(s.as_bytes());
                // Multi-line prototext ends with a newline, and Go adds one more (Fprintln).
                out.extend_from_slice(b"\n\n");
                Ok(())
            }
            FormatType::ProtoCompact => {
                let mut s = String::new();
                prototext_family(&mut s, &mf, false);
                out.extend_from_slice(s.as_bytes());
                out.push(b'\n');
                Ok(())
            }
            FormatType::OpenMetrics => metric_family_to_openmetrics(out, &mf),
            FormatType::TextPlain | FormatType::Unknown => metric_family_to_text(out, &mf),
        }
    }

    /// Go `Closer.Close`: the final `# EOF` line for OpenMetrics.
    pub fn close(&self, out: &mut Vec<u8>) {
        if self.openmetrics {
            out.extend_from_slice(b"# EOF\n");
        }
    }
}

// ---- Text format 0.0.4 ----

fn write_escaped(out: &mut String, v: &str, quote: bool) {
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '"' if quote => out.push_str("\\\""),
            c => out.push(c),
        }
    }
}

fn write_name(out: &mut String, name: &str) {
    if is_valid_legacy_metric_name(name) {
        out.push_str(name);
        return;
    }
    out.push('"');
    write_escaped(out, name, true);
    out.push('"');
}

/// Go `writeFloat` (strconv 'g', -1).
pub fn write_float(out: &mut String, f: f64) {
    if f == 1.0 {
        out.push('1');
    } else if f == 0.0 {
        out.push('0');
    } else if f == -1.0 {
        out.push_str("-1");
    } else {
        write_g(out, f);
    }
}

fn write_om_float(out: &mut String, f: f64) {
    if f == 1.0 {
        out.push_str("1.0");
    } else if f == 0.0 {
        out.push_str("0.0");
    } else if f == -1.0 {
        out.push_str("-1.0");
    } else if f.is_nan() || f.is_infinite() {
        write_g(out, f);
    } else {
        let start = out.len();
        write_g(out, f);
        if !out[start..].contains(['e', '.']) {
            out.push_str(".0");
        }
    }
}

fn write_name_and_labels(out: &mut String, name: &str, labels: &[(String, String)], extra: Option<(&str, f64)>, om: bool) {
    let mut sep = '{';
    let mut inside = false;
    if !name.is_empty() {
        if !is_valid_legacy_metric_name(name) {
            inside = true;
            out.push(sep);
            sep = ',';
        }
        write_name(out, name);
    }
    if labels.is_empty() && extra.is_none() {
        if inside {
            out.push('}');
        }
        return;
    }
    for (n, v) in labels {
        out.push(sep);
        write_name(out, n);
        out.push_str("=\"");
        write_escaped(out, v, true);
        out.push('"');
        sep = ',';
    }
    if let Some((ln, lv)) = extra {
        out.push(sep);
        out.push_str(ln);
        out.push_str("=\"");
        if om {
            write_om_float(out, lv);
        } else {
            write_float(out, lv);
        }
        out.push('"');
    }
    out.push('}');
}

fn write_sample(out: &mut String, name: &str, suffix: &str, m: &DtoMetric, extra: Option<(&str, f64)>, v: f64) {
    let mut full = String::with_capacity(name.len() + suffix.len());
    full.push_str(name);
    full.push_str(suffix);
    write_name_and_labels(out, &full, &m.labels, extra, false);
    out.push(' ');
    write_float(out, v);
    if let Some(ts) = m.timestamp_ms {
        out.push(' ');
        out.push_str(&ts.to_string());
    }
    out.push('\n');
}

fn type_mismatch(kind: &str, name: &str) -> String {
    format!("expected {} in metric {}", kind, name)
}

/// Go `expfmt.MetricFamilyToText`.
pub fn metric_family_to_text(o: &mut Vec<u8>, mf: &DtoFamily) -> Result<(), String> {
    if mf.metrics.is_empty() {
        return Err(format!("MetricFamily has no metrics: {}", mf.name));
    }
    if mf.name.is_empty() {
        return Err("MetricFamily has no name".to_string());
    }
    let name = mf.name.as_str();
    let mut out = String::with_capacity(64 * (mf.metrics.len() + 1));
    if let Some(h) = &mf.help {
        out.push_str("# HELP ");
        write_name(&mut out, name);
        out.push(' ');
        write_escaped(&mut out, h, false);
        out.push('\n');
    }
    out.push_str("# TYPE ");
    write_name(&mut out, name);
    out.push_str(match mf.typ {
        DtoType::Counter => " counter\n",
        DtoType::Gauge => " gauge\n",
        DtoType::Summary => " summary\n",
        DtoType::Untyped => " untyped\n",
        DtoType::Histogram | DtoType::GaugeHistogram => " histogram\n",
    });
    let mut res = Ok(());
    for m in &mf.metrics {
        match (mf.typ, &m.value) {
            (DtoType::Counter, DtoValue::Counter(v)) | (DtoType::Gauge, DtoValue::Gauge(v)) | (DtoType::Untyped, DtoValue::Untyped(v)) => {
                write_sample(&mut out, name, "", m, None, *v)
            }
            (DtoType::Summary, DtoValue::Summary(s)) => {
                for (q, v) in &s.quantiles {
                    write_sample(&mut out, name, "", m, Some(("quantile", *q)), *v);
                }
                write_sample(&mut out, name, "_sum", m, None, s.sample_sum);
                write_sample(&mut out, name, "_count", m, None, s.sample_count as f64);
            }
            (DtoType::Histogram | DtoType::GaugeHistogram, DtoValue::Histogram(h)) => {
                let mut inf_seen = false;
                for b in &h.buckets {
                    let mut v = b.cumulative_count_float.unwrap_or(0.0);
                    if v == 0.0 {
                        v = b.cumulative_count.unwrap_or(0) as f64;
                    }
                    write_sample(&mut out, name, "_bucket", m, Some(("le", b.upper_bound)), v);
                    if b.upper_bound == f64::INFINITY {
                        inf_seen = true;
                    }
                }
                let mut cnt = h.count_float();
                if cnt == 0.0 {
                    cnt = h.count() as f64;
                }
                if !inf_seen {
                    write_sample(&mut out, name, "_bucket", m, Some(("le", f64::INFINITY)), cnt);
                }
                write_sample(&mut out, name, "_sum", m, None, h.sample_sum.unwrap_or(0.0));
                write_sample(&mut out, name, "_count", m, None, cnt);
            }
            (t, _) => {
                let kind = match t {
                    DtoType::Counter => "counter",
                    DtoType::Gauge => "gauge",
                    DtoType::Untyped => "untyped",
                    DtoType::Summary => "summary",
                    _ => "histogram",
                };
                res = Err(type_mismatch(kind, name));
                break;
            }
        }
    }
    o.extend_from_slice(out.as_bytes());
    res
}

// ---- OpenMetrics 1.0 ----

fn write_om_sample(out: &mut String, name: &str, suffix: &str, m: &DtoMetric, extra: Option<(&str, f64)>, v: Result<f64, u64>) {
    let mut full = String::with_capacity(name.len() + suffix.len());
    full.push_str(name);
    full.push_str(suffix);
    write_name_and_labels(out, &full, &m.labels, extra, true);
    out.push(' ');
    match v {
        Ok(f) => write_om_float(out, f),
        Err(u) => out.push_str(&u.to_string()),
    }
    if let Some(ts) = m.timestamp_ms {
        out.push(' ');
        write_om_float(out, ts as f64 / 1000.0);
    }
    out.push('\n');
}

/// Go `expfmt.MetricFamilyToOpenMetrics` (without exemplars and created lines).
pub fn metric_family_to_openmetrics(o: &mut Vec<u8>, mf: &DtoFamily) -> Result<(), String> {
    if mf.name.is_empty() {
        return Err("MetricFamily has no name".to_string());
    }
    let name = mf.name.as_str();
    let is_total_counter = mf.typ == DtoType::Counter && name.ends_with("_total");
    let compliant = if is_total_counter { &name[..name.len() - 6] } else { name };
    let mut out = String::with_capacity(64 * (mf.metrics.len() + 1));
    if let Some(h) = &mf.help {
        out.push_str("# HELP ");
        write_name(&mut out, compliant);
        out.push(' ');
        write_escaped(&mut out, h, true);
        out.push('\n');
    }
    out.push_str("# TYPE ");
    write_name(&mut out, compliant);
    out.push_str(match mf.typ {
        DtoType::Counter if is_total_counter => " counter\n",
        DtoType::Counter => " unknown\n",
        DtoType::Gauge => " gauge\n",
        DtoType::Summary => " summary\n",
        DtoType::Untyped => " unknown\n",
        DtoType::Histogram => " histogram\n",
        DtoType::GaugeHistogram => " gaugehistogram\n",
    });
    if let Some(u) = &mf.unit {
        out.push_str("# UNIT ");
        write_name(&mut out, compliant);
        out.push(' ');
        write_escaped(&mut out, u, true);
        out.push('\n');
    }
    let sample_name = if is_total_counter { name } else { compliant };
    let mut res = Ok(());
    for m in &mf.metrics {
        match (mf.typ, &m.value) {
            (DtoType::Counter, DtoValue::Counter(v)) | (DtoType::Gauge, DtoValue::Gauge(v)) | (DtoType::Untyped, DtoValue::Untyped(v)) => {
                write_om_sample(&mut out, sample_name, "", m, None, Ok(*v))
            }
            (DtoType::Summary, DtoValue::Summary(s)) => {
                for (q, v) in &s.quantiles {
                    write_om_sample(&mut out, sample_name, "", m, Some(("quantile", *q)), Ok(*v));
                }
                write_om_sample(&mut out, sample_name, "_sum", m, None, Ok(s.sample_sum));
                write_om_sample(&mut out, sample_name, "_count", m, None, Err(s.sample_count));
            }
            (DtoType::Histogram | DtoType::GaugeHistogram, DtoValue::Histogram(h)) => {
                let float_err = || format!("OpenMetrics v1.0 does not support float histogram {}", sample_name);
                let mut inf_seen = false;
                for b in &h.buckets {
                    if b.cumulative_count_float.unwrap_or(0.0) > 0.0 {
                        res = Err(float_err());
                        break;
                    }
                    write_om_sample(&mut out, sample_name, "_bucket", m, Some(("le", b.upper_bound)), Err(b.cumulative_count.unwrap_or(0)));
                    if b.upper_bound == f64::INFINITY {
                        inf_seen = true;
                    }
                }
                if res.is_err() {
                    break;
                }
                if !inf_seen {
                    write_om_sample(&mut out, sample_name, "_bucket", m, Some(("le", f64::INFINITY)), Err(h.count()));
                }
                write_om_sample(&mut out, sample_name, "_sum", m, None, Ok(h.sample_sum.unwrap_or(0.0)));
                if h.count_float() > 0.0 {
                    res = Err(float_err());
                    break;
                }
                write_om_sample(&mut out, sample_name, "_count", m, None, Err(h.count()));
            }
            _ => {
                res = Err(format!("unexpected type in metric {}", sample_name));
                break;
            }
        }
    }
    o.extend_from_slice(out.as_bytes());
    res
}

// ---- Protobuf (binary) ----

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn put_tag(out: &mut Vec<u8>, field: u32, wire: u8) {
    put_varint(out, ((field as u64) << 3) | wire as u64);
}

fn put_bytes(out: &mut Vec<u8>, field: u32, b: &[u8]) {
    put_tag(out, field, 2);
    put_varint(out, b.len() as u64);
    out.extend_from_slice(b);
}

fn put_double(out: &mut Vec<u8>, field: u32, f: f64) {
    put_tag(out, field, 1);
    out.extend_from_slice(&f.to_bits().to_le_bytes());
}

fn put_uint(out: &mut Vec<u8>, field: u32, v: u64) {
    put_tag(out, field, 0);
    put_varint(out, v);
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn put_sub(out: &mut Vec<u8>, field: u32, f: impl FnOnce(&mut Vec<u8>)) {
    let mut b = Vec::with_capacity(32);
    f(&mut b);
    put_bytes(out, field, &b);
}

fn proto_spans(out: &mut Vec<u8>, field: u32, spans: &[(i32, u32)]) {
    for (off, len) in spans {
        put_sub(out, field, |b| {
            put_uint(b, 1, zigzag(*off as i64) & 0xffff_ffff);
            put_uint(b, 2, *len as u64);
        });
    }
}

fn proto_histogram(b: &mut Vec<u8>, h: &DtoHistogram) {
    if let Some(c) = h.sample_count {
        put_uint(b, 1, c);
    }
    if let Some(s) = h.sample_sum {
        put_double(b, 2, s);
    }
    for bk in &h.buckets {
        put_sub(b, 3, |x| {
            if let Some(c) = bk.cumulative_count {
                put_uint(x, 1, c);
            }
            put_double(x, 2, bk.upper_bound);
            if let Some(c) = bk.cumulative_count_float {
                put_double(x, 4, c);
            }
        });
    }
    if let Some(c) = h.sample_count_float {
        put_double(b, 4, c);
    }
    if let Some(s) = h.schema {
        put_uint(b, 5, zigzag(s as i64) & 0xffff_ffff);
    }
    if let Some(z) = h.zero_threshold {
        put_double(b, 6, z);
    }
    if let Some(z) = h.zero_count {
        put_uint(b, 7, z);
    }
    if let Some(z) = h.zero_count_float {
        put_double(b, 8, z);
    }
    proto_spans(b, 9, &h.negative_spans);
    for d in &h.negative_deltas {
        put_uint(b, 10, zigzag(*d));
    }
    for c in &h.negative_counts {
        put_double(b, 11, *c);
    }
    proto_spans(b, 12, &h.positive_spans);
    for d in &h.positive_deltas {
        put_uint(b, 13, zigzag(*d));
    }
    for c in &h.positive_counts {
        put_double(b, 14, *c);
    }
}

/// The family as a protobuf message (fields in field-number order, as Go's marshaler writes them).
pub fn proto_family(out: &mut Vec<u8>, mf: &DtoFamily) {
    put_bytes(out, 1, mf.name.as_bytes());
    if let Some(h) = &mf.help {
        put_bytes(out, 2, h.as_bytes());
    }
    put_uint(out, 3, mf.typ as u64);
    for m in &mf.metrics {
        put_sub(out, 4, |b| {
            for (n, v) in &m.labels {
                put_sub(b, 1, |x| {
                    put_bytes(x, 1, n.as_bytes());
                    put_bytes(x, 2, v.as_bytes());
                });
            }
            match &m.value {
                DtoValue::Gauge(v) => put_sub(b, 2, |x| put_double(x, 1, *v)),
                DtoValue::Counter(v) => put_sub(b, 3, |x| put_double(x, 1, *v)),
                DtoValue::Summary(s) => put_sub(b, 4, |x| {
                    put_uint(x, 1, s.sample_count);
                    put_double(x, 2, s.sample_sum);
                    for (q, v) in &s.quantiles {
                        put_sub(x, 3, |y| {
                            put_double(y, 1, *q);
                            put_double(y, 2, *v);
                        });
                    }
                }),
                DtoValue::Untyped(v) => put_sub(b, 5, |x| put_double(x, 1, *v)),
                DtoValue::Histogram(_) => {}
            }
            if let Some(ts) = m.timestamp_ms {
                put_uint(b, 6, ts as u64);
            }
            if let DtoValue::Histogram(h) = &m.value {
                put_sub(b, 7, |x| proto_histogram(x, h));
            }
        });
    }
    if let Some(u) = &mf.unit {
        put_bytes(out, 5, u.as_bytes());
    }
}

// ---- Protobuf text ----

struct TextW<'a> {
    out: &'a mut String,
    multi: bool,
    depth: usize,
    first: bool,
}

impl TextW<'_> {
    fn sep(&mut self) {
        if self.multi {
            if !self.first || self.depth > 0 {
                self.out.push('\n');
            }
            for _ in 0..self.depth {
                self.out.push_str("  ");
            }
        } else if !self.first {
            self.out.push_str("  ");
        }
        self.first = false;
    }
    fn field(&mut self, name: &str, value: &str) {
        self.sep();
        self.out.push_str(name);
        self.out.push(':');
        if self.multi {
            self.out.push_str("  ");
        }
        self.out.push_str(value);
    }
    fn open(&mut self, name: &str) {
        self.sep();
        self.out.push_str(name);
        self.out.push_str(if self.multi { ":  {" } else { ":{" });
        self.depth += 1;
        self.first = true;
    }
    fn close(&mut self) {
        self.depth -= 1;
        if self.multi {
            if !self.first {
                self.out.push('\n');
                for _ in 0..self.depth {
                    self.out.push_str("  ");
                }
            }
        }
        self.out.push('}');
        self.first = false;
    }
}

fn pt_str(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('"');
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\t' => o.push_str("\\t"),
            c if (c as u32) < 0x20 => o.push_str(&format!("\\x{:02x}", c as u32)),
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

fn pt_f64(f: f64) -> String {
    if f.is_nan() {
        return "nan".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "inf".to_string() } else { "-inf".to_string() };
    }
    let mut s = String::new();
    write_g(&mut s, f);
    s
}

fn prototext_family(out: &mut String, mf: &DtoFamily, multi: bool) {
    let mut w = TextW { out, multi, depth: 0, first: true };
    w.field("name", &pt_str(&mf.name));
    if let Some(h) = &mf.help {
        w.field("help", &pt_str(h));
    }
    w.field("type", mf.typ.proto_name());
    for m in &mf.metrics {
        w.open("metric");
        for (n, v) in &m.labels {
            w.open("label");
            w.field("name", &pt_str(n));
            w.field("value", &pt_str(v));
            w.close();
        }
        match &m.value {
            DtoValue::Gauge(v) | DtoValue::Counter(v) | DtoValue::Untyped(v) => {
                w.open(match &m.value {
                    DtoValue::Gauge(_) => "gauge",
                    DtoValue::Counter(_) => "counter",
                    _ => "untyped",
                });
                w.field("value", &pt_f64(*v));
                w.close();
            }
            DtoValue::Summary(s) => {
                w.open("summary");
                w.field("sample_count", &s.sample_count.to_string());
                w.field("sample_sum", &pt_f64(s.sample_sum));
                for (q, v) in &s.quantiles {
                    w.open("quantile");
                    w.field("quantile", &pt_f64(*q));
                    w.field("value", &pt_f64(*v));
                    w.close();
                }
                w.close();
            }
            DtoValue::Histogram(_) => {}
        }
        if let Some(ts) = m.timestamp_ms {
            w.field("timestamp_ms", &ts.to_string());
        }
        if let DtoValue::Histogram(h) = &m.value {
            w.open("histogram");
            if let Some(c) = h.sample_count {
                w.field("sample_count", &c.to_string());
            }
            if let Some(c) = h.sample_count_float {
                w.field("sample_count_float", &pt_f64(c));
            }
            if let Some(s) = h.sample_sum {
                w.field("sample_sum", &pt_f64(s));
            }
            for b in &h.buckets {
                w.open("bucket");
                if let Some(c) = b.cumulative_count {
                    w.field("cumulative_count", &c.to_string());
                }
                if let Some(c) = b.cumulative_count_float {
                    w.field("cumulative_count_float", &pt_f64(c));
                }
                w.field("upper_bound", &pt_f64(b.upper_bound));
                w.close();
            }
            if let Some(s) = h.schema {
                w.field("schema", &s.to_string());
            }
            if let Some(z) = h.zero_threshold {
                w.field("zero_threshold", &pt_f64(z));
            }
            if let Some(z) = h.zero_count {
                w.field("zero_count", &z.to_string());
            }
            if let Some(z) = h.zero_count_float {
                w.field("zero_count_float", &pt_f64(z));
            }
            for (name, spans, deltas, counts) in [
                ("negative", &h.negative_spans, &h.negative_deltas, &h.negative_counts),
                ("positive", &h.positive_spans, &h.positive_deltas, &h.positive_counts),
            ] {
                for (off, len) in spans.iter() {
                    w.open(&format!("{}_span", name));
                    w.field("offset", &off.to_string());
                    w.field("length", &len.to_string());
                    w.close();
                }
                for d in deltas.iter() {
                    w.field(&format!("{}_delta", name), &d.to_string());
                }
                for c in counts.iter() {
                    w.field(&format!("{}_count", name), &pt_f64(*c));
                }
            }
            w.close();
        }
        w.close();
    }
}
