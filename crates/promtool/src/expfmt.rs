//! Port of prometheus/common `expfmt` (v0.72.1) `TextParser` (text_parse.go): the Prometheus text format to
//! `io.prometheus.client` metric families, with Go's exact error texts. Also the `dto` model it fills
//! (client_model `metrics.proto`, only the fields the text format sets).
//!
//! Go returns a map; this port returns a `BTreeMap` (sorted by name). Go callers that iterate the map see a
//! random order; every order is a valid Go result.

use std::collections::{BTreeMap, HashMap};

use textparse::strconv::{go_f64_to_i64, parse_float as go_parse_float, parse_int, quote_bytes};

pub use textparse::client_pb::MetricType;

/// Go `model.ValidationScheme`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValidationScheme {
    Legacy,
    Utf8,
}

impl ValidationScheme {
    /// Go `ValidationScheme.IsValidMetricName`.
    pub fn is_valid_metric_name(self, name: &[u8]) -> bool {
        if name.is_empty() {
            return false;
        }
        match self {
            ValidationScheme::Legacy => {
                // Go ranges over runes: a non-ASCII rune is never valid, so checking bytes is the same.
                name.iter().enumerate().all(|(i, &b)| {
                    b.is_ascii_alphabetic() || b == b'_' || b == b':' || (b.is_ascii_digit() && i > 0)
                })
            }
            ValidationScheme::Utf8 => std::str::from_utf8(name).is_ok(),
        }
    }

    /// Go `ValidationScheme.IsValidLabelName`.
    pub fn is_valid_label_name(self, name: &[u8]) -> bool {
        if name.is_empty() {
            return false;
        }
        match self {
            ValidationScheme::Legacy => name
                .iter()
                .enumerate()
                .all(|(i, &b)| b.is_ascii_alphabetic() || b == b'_' || (b.is_ascii_digit() && i > 0)),
            ValidationScheme::Utf8 => std::str::from_utf8(name).is_ok(),
        }
    }
}

/// Go `dto.LabelPair`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LabelPair {
    pub name: String,
    pub value: String,
}

/// Go `dto.Quantile`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Quantile {
    pub quantile: f64,
    pub value: f64,
}

/// Go `dto.Summary`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Summary {
    pub sample_count: Option<u64>,
    pub sample_sum: Option<f64>,
    pub quantile: Vec<Quantile>,
}

impl Summary {
    pub fn get_sample_count(&self) -> u64 {
        self.sample_count.unwrap_or(0)
    }
    pub fn get_sample_sum(&self) -> f64 {
        self.sample_sum.unwrap_or(0.0)
    }
}

/// Go `dto.Bucket`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Bucket {
    pub cumulative_count: Option<u64>,
    pub cumulative_count_float: Option<f64>,
    pub upper_bound: f64,
}

impl Bucket {
    pub fn get_cumulative_count(&self) -> u64 {
        self.cumulative_count.unwrap_or(0)
    }
    pub fn get_cumulative_count_float(&self) -> f64 {
        self.cumulative_count_float.unwrap_or(0.0)
    }
}

/// Go `dto.Histogram` (classic buckets only; the text format has no native histograms).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Histogram {
    pub sample_count: Option<u64>,
    pub sample_count_float: Option<f64>,
    pub sample_sum: Option<f64>,
    pub bucket: Vec<Bucket>,
}

impl Histogram {
    pub fn get_sample_count(&self) -> u64 {
        self.sample_count.unwrap_or(0)
    }
    pub fn get_sample_count_float(&self) -> f64 {
        self.sample_count_float.unwrap_or(0.0)
    }
    pub fn get_sample_sum(&self) -> f64 {
        self.sample_sum.unwrap_or(0.0)
    }
}

/// Go `dto.Metric`. `gauge`, `counter` and `untyped` hold the `Value` of the Go message of the same name.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Metric {
    pub label: Vec<LabelPair>,
    pub gauge: Option<f64>,
    pub counter: Option<f64>,
    pub summary: Option<Summary>,
    pub untyped: Option<f64>,
    pub histogram: Option<Histogram>,
    pub timestamp_ms: Option<i64>,
}

impl Metric {
    pub fn get_timestamp_ms(&self) -> i64 {
        self.timestamp_ms.unwrap_or(0)
    }
}

/// Go `dto.MetricFamily`. `help` is the HELP text (Go keeps invalid UTF-8; it is replaced here).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetricFamily {
    pub name: String,
    pub help: Option<String>,
    pub r#type: Option<MetricType>,
    pub metric: Vec<Metric>,
}

impl MetricFamily {
    /// Go `GetType()`: COUNTER (0) when unset.
    pub fn get_type(&self) -> MetricType {
        self.r#type.unwrap_or(MetricType::COUNTER)
    }
    pub fn get_help(&self) -> &str {
        self.help.as_deref().unwrap_or("")
    }
}

/// Go `ParseError`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseError {
    pub line: usize,
    pub msg: String,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "text format parsing error in line {}: {}", self.line, self.msg)
    }
}

impl std::error::Error for ParseError {}

/// The errors of [`TextParser::text_to_metric_families`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TextError {
    Parse(ParseError),
    /// A plain error (Go `fmt.Errorf`).
    Other(String),
}

impl std::fmt::Display for TextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TextError::Parse(e) => e.fmt(f),
            TextError::Other(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for TextError {}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    StartOfLine,
    StartComment,
    ReadingMetricName,
    ReadingLabels,
    StartLabelName,
    StartLabelValue,
    ReadingValue,
    StartTimestamp,
    ReadingHelp,
    ReadingType,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Err {
    Eof,
    Other,
}

/// Where `currentMetric` lives: not yet appended to its family, or appended at an index.
enum CurMetric {
    None,
    New(Metric),
    At(usize, usize),
}

/// Go `TextParser`.
pub struct TextParser<'a> {
    scheme: ValidationScheme,
    input: &'a [u8],
    pos: usize,
    families: Vec<MetricFamily>,
    by_name: HashMap<String, usize>,
    err: Option<Err>,
    err_value: Option<TextError>,
    line_count: usize,
    current_byte: u8,
    current_token: Vec<u8>,
    current_mf: Option<usize>,
    current_metric: CurMetric,
    current_label_pair: LabelPair,
    current_label_pairs: Vec<LabelPair>,
    /// Whether `current_label_pair` was appended to `current_label_pairs` (Go appends the pointer).
    pending_pair: bool,
    current_labels: BTreeMap<String, String>,
    summaries: HashMap<Vec<(String, String)>, (usize, usize)>,
    current_quantile: f64,
    histograms: HashMap<Vec<(String, String)>, (usize, usize)>,
    current_bucket: f64,
    current_is_summary_count: bool,
    current_is_summary_sum: bool,
    current_is_histogram_count: bool,
    current_is_histogram_sum: bool,
    current_metric_is_inside_braces: bool,
    current_metric_inside_braces_is_present: bool,
}

/// Go `TextParser.TextToMetricFamilies` with a parser made by `NewTextParser(scheme)`. On error, Go returns the
/// families parsed so far with the error; so does this function.
pub fn text_to_metric_families(
    scheme: ValidationScheme,
    input: &[u8],
) -> (BTreeMap<String, MetricFamily>, Option<TextError>) {
    TextParser::new(scheme, input).text_to_metric_families()
}

/// Go `uint64(f)` on amd64.
pub fn go_f64_to_u64(f: f64) -> u64 {
    const TWO63: f64 = 9_223_372_036_854_775_808.0;
    if f < TWO63 {
        go_f64_to_i64(f) as u64
    } else {
        (go_f64_to_i64(f - TWO63) as u64) | (1 << 63)
    }
}

/// Go `value == float64(uint64(value))`: an integer in [0, 2^64).
fn is_uint(v: f64) -> bool {
    (0.0..18_446_744_073_709_551_616.0).contains(&v) && v.fract() == 0.0
}

fn quote_byte(b: u8) -> String {
    // Go `%q` of a byte formats it as the rune U+00XX.
    let c = char::from(b);
    let mut out = String::from("'");
    match c {
        '\'' | '\\' => {
            out.push('\\');
            out.push(c);
        }
        '"' => out.push('"'),
        _ => {
            let q = model::unicode::quote(c.encode_utf8(&mut [0u8; 4]));
            out.push_str(&q[1..q.len() - 1]);
        }
    }
    out.push('\'');
    out
}

fn quote_str(s: &str) -> String {
    quote_bytes(s.as_bytes())
}

fn is_blank_or_tab(b: u8) -> bool {
    b == b' ' || b == b'\t'
}

fn is_valid_label_name_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b == b'"'
}

fn is_valid_label_name_continuation(b: u8, quoted: bool) -> bool {
    // Go `utf8.ValidString(string(b))` converts the byte to a rune, which is always valid.
    is_valid_label_name_start(b) || b.is_ascii_digit() || quoted
}

fn is_valid_metric_name_start(b: u8) -> bool {
    is_valid_label_name_start(b) || b == b':'
}

fn is_valid_metric_name_continuation(b: u8, quoted: bool) -> bool {
    is_valid_label_name_continuation(b, quoted) || b == b':'
}

fn is_count(name: &str) -> bool {
    name.len() > 6 && name.ends_with("_count")
}

fn is_sum(name: &str) -> bool {
    name.len() > 4 && name.ends_with("_sum")
}

fn is_bucket(name: &str) -> bool {
    name.len() > 7 && name.ends_with("_bucket")
}

fn summary_metric_name(name: &str) -> &str {
    if is_count(name) {
        &name[..name.len() - 6]
    } else if is_sum(name) {
        &name[..name.len() - 4]
    } else {
        name
    }
}

fn histogram_metric_name(name: &str) -> &str {
    if is_count(name) {
        &name[..name.len() - 6]
    } else if is_sum(name) {
        &name[..name.len() - 4]
    } else if is_bucket(name) {
        &name[..name.len() - 7]
    } else {
        name
    }
}

/// Go `parseFloat` of text_parse.go.
fn parse_float(s: &[u8]) -> Option<f64> {
    if s.iter().any(|&c| c == b'p' || c == b'P' || c == b'_') {
        return None;
    }
    go_parse_float(s).ok()
}

/// Go `dto.MetricType_value` plus "GAUGEHISTOGRAM".
fn metric_type_from_upper(s: &str) -> Option<MetricType> {
    Some(match s {
        "COUNTER" => MetricType::COUNTER,
        "GAUGE" => MetricType::GAUGE,
        "SUMMARY" => MetricType::SUMMARY,
        "UNTYPED" => MetricType::UNTYPED,
        "HISTOGRAM" => MetricType::HISTOGRAM,
        "GAUGE_HISTOGRAM" | "GAUGEHISTOGRAM" => MetricType::GAUGE_HISTOGRAM,
        _ => return None,
    })
}

impl<'a> TextParser<'a> {
    pub fn new(scheme: ValidationScheme, input: &'a [u8]) -> TextParser<'a> {
        TextParser {
            scheme,
            input,
            pos: 0,
            families: Vec::new(),
            by_name: HashMap::new(),
            err: None,
            err_value: None,
            line_count: 0,
            current_byte: 0,
            current_token: Vec::new(),
            current_mf: None,
            current_metric: CurMetric::None,
            current_label_pair: LabelPair::default(),
            current_label_pairs: Vec::new(),
            pending_pair: false,
            current_labels: BTreeMap::new(),
            summaries: HashMap::new(),
            current_quantile: f64::NAN,
            histograms: HashMap::new(),
            current_bucket: f64::NAN,
            current_is_summary_count: false,
            current_is_summary_sum: false,
            current_is_histogram_count: false,
            current_is_histogram_sum: false,
            current_metric_is_inside_braces: false,
            current_metric_inside_braces_is_present: false,
        }
    }

    /// Go `TextToMetricFamilies`.
    pub fn text_to_metric_families(mut self) -> (BTreeMap<String, MetricFamily>, Option<TextError>) {
        let mut state = Some(State::StartOfLine);
        while let Some(s) = state {
            state = self.step(s);
        }
        if self.err == Some(Err::Eof) {
            self.parse_error("unexpected end of input stream".to_string());
        }
        // Normalize histograms.
        let hs: Vec<(usize, usize)> = self.histograms.values().copied().collect();
        for (fi, mi) in hs {
            if let Some(h) = self.families[fi].metric[mi].histogram.as_mut() {
                normalize_histogram(h);
            }
        }
        let mut out = BTreeMap::new();
        for mf in self.families {
            if !mf.metric.is_empty() {
                out.insert(mf.name.clone(), mf);
            }
        }
        (out, self.err_value)
    }

    fn step(&mut self, s: State) -> Option<State> {
        match s {
            State::StartOfLine => self.start_of_line(),
            State::StartComment => self.start_comment(),
            State::ReadingMetricName => self.reading_metric_name(),
            State::ReadingLabels => self.reading_labels(),
            State::StartLabelName => self.start_label_name(),
            State::StartLabelValue => self.start_label_value(),
            State::ReadingValue => self.reading_value(),
            State::StartTimestamp => self.start_timestamp(),
            State::ReadingHelp => self.reading_help(),
            State::ReadingType => self.reading_type(),
        }
    }

    fn read_byte(&mut self) {
        if self.pos < self.input.len() {
            self.current_byte = self.input[self.pos];
            self.pos += 1;
            self.err = None;
        } else {
            // bufio.Reader.ReadByte returns 0 with io.EOF.
            self.current_byte = 0;
            self.err = Some(Err::Eof);
        }
    }

    fn parse_error(&mut self, msg: String) {
        self.err = Some(Err::Other);
        self.err_value = Some(TextError::Parse(ParseError { line: self.line_count, msg }));
    }

    fn mf(&self) -> Option<&MetricFamily> {
        self.current_mf.map(|i| &self.families[i])
    }

    fn mf_type(&self) -> MetricType {
        self.mf().map(|m| m.get_type()).unwrap_or(MetricType::COUNTER)
    }

    fn mf_name(&self) -> String {
        self.mf().map(|m| m.name.clone()).unwrap_or_default()
    }

    fn is_summary_or_histogram(t: MetricType) -> bool {
        t == MetricType::SUMMARY || t == MetricType::HISTOGRAM || t == MetricType::GAUGE_HISTOGRAM
    }

    fn metric_mut(&mut self) -> &mut Metric {
        match &mut self.current_metric {
            CurMetric::New(m) => m,
            CurMetric::At(f, i) => &mut self.families[*f].metric[*i],
            CurMetric::None => unreachable!("no current metric"),
        }
    }

    fn fix_untyped(&mut self) {
        if let Some(i) = self.current_mf
            && self.families[i].r#type.is_none()
        {
            self.families[i].r#type = Some(MetricType::UNTYPED);
        }
    }

    fn start_of_line(&mut self) -> Option<State> {
        self.line_count += 1;
        self.current_metric = CurMetric::None;
        self.current_metric_is_inside_braces = false;
        self.current_metric_inside_braces_is_present = false;
        self.skip_blank_tab();
        if self.err.is_some() {
            if self.err == Some(Err::Eof) {
                self.err = None;
            }
            return None;
        }
        match self.current_byte {
            b'#' => Some(State::StartComment),
            b'\n' => Some(State::StartOfLine),
            b'{' => {
                self.current_metric_is_inside_braces = true;
                Some(State::ReadingLabels)
            }
            _ => Some(State::ReadingMetricName),
        }
    }

    fn start_comment(&mut self) -> Option<State> {
        self.skip_blank_tab();
        if self.err.is_some() {
            return None;
        }
        if self.current_byte == b'\n' {
            return Some(State::StartOfLine);
        }
        self.read_token_until_whitespace();
        if self.err.is_some() {
            return None;
        }
        if self.current_byte == b'\n' {
            return Some(State::StartOfLine);
        }
        let keyword = self.current_token.clone();
        if keyword != b"HELP" && keyword != b"TYPE" {
            while self.current_byte != b'\n' {
                self.read_byte();
                if self.err.is_some() {
                    return None;
                }
            }
            return Some(State::StartOfLine);
        }
        self.skip_blank_tab();
        if self.err.is_some() {
            return None;
        }
        self.read_token_as_metric_name();
        if self.err.is_some() {
            return None;
        }
        if self.current_byte == b'\n' {
            return Some(State::StartOfLine);
        }
        if !is_blank_or_tab(self.current_byte) {
            self.parse_error("invalid metric name in comment".to_string());
            return None;
        }
        self.set_or_create_current_mf();
        if self.err.is_some() {
            return None;
        }
        self.skip_blank_tab();
        if self.err.is_some() {
            return None;
        }
        if self.current_byte == b'\n' {
            return Some(State::StartOfLine);
        }
        if keyword == b"HELP" { Some(State::ReadingHelp) } else { Some(State::ReadingType) }
    }

    fn reading_metric_name(&mut self) -> Option<State> {
        self.read_token_as_metric_name();
        if self.err.is_some() {
            return None;
        }
        if self.current_token.is_empty() {
            self.parse_error("invalid metric name".to_string());
            return None;
        }
        self.set_or_create_current_mf();
        if self.err.is_some() {
            return None;
        }
        self.fix_untyped();
        self.current_metric = CurMetric::New(Metric::default());
        self.skip_blank_tab_if_current_blank_tab();
        if self.err.is_some() {
            return None;
        }
        Some(State::ReadingLabels)
    }

    fn reading_labels(&mut self) -> Option<State> {
        if Self::is_summary_or_histogram(self.mf_type()) {
            self.current_labels = BTreeMap::new();
            let name = self.mf_name();
            self.current_labels.insert("__name__".to_string(), name);
            self.current_quantile = f64::NAN;
            self.current_bucket = f64::NAN;
        }
        if self.current_byte != b'{' {
            return Some(State::ReadingValue);
        }
        Some(State::StartLabelName)
    }

    fn start_label_name(&mut self) -> Option<State> {
        self.skip_blank_tab();
        if self.err.is_some() {
            return None;
        }
        if self.current_byte == b'}' {
            if matches!(self.current_metric, CurMetric::None) {
                self.parse_error("invalid metric name".to_string());
                self.current_label_pairs.clear();
                return None;
            }
            let pairs = std::mem::take(&mut self.current_label_pairs);
            self.metric_mut().label.extend(pairs);
            self.skip_blank_tab();
            if self.err.is_some() {
                return None;
            }
            return Some(State::ReadingValue);
        }
        self.read_token_as_label_name();
        if self.err.is_some() {
            return None;
        }
        if self.current_token.is_empty() {
            let msg = format!("invalid label name for metric {}", quote_str(&self.mf_name()));
            self.parse_error(msg);
            return None;
        }
        self.skip_blank_tab_if_current_blank_tab();
        if self.err.is_some() {
            return None;
        }
        if self.current_byte != b'=' {
            if self.current_metric_is_inside_braces {
                if self.current_metric_inside_braces_is_present {
                    let msg = format!("multiple metric names for metric {}", quote_str(&self.mf_name()));
                    self.parse_error(msg);
                    return None;
                }
                match self.current_byte {
                    b',' => {
                        self.set_or_create_current_mf();
                        if self.err.is_some() {
                            return None;
                        }
                        self.fix_untyped();
                        self.current_metric = CurMetric::New(Metric::default());
                        self.current_metric_inside_braces_is_present = true;
                        return Some(State::StartLabelName);
                    }
                    b'}' => {
                        self.set_or_create_current_mf();
                        if self.err.is_some() {
                            self.current_label_pairs.clear();
                            return None;
                        }
                        self.fix_untyped();
                        let pairs = std::mem::take(&mut self.current_label_pairs);
                        self.current_metric = CurMetric::New(Metric { label: pairs, ..Default::default() });
                        self.skip_blank_tab();
                        if self.err.is_some() {
                            return None;
                        }
                        return Some(State::ReadingValue);
                    }
                    b => {
                        self.parse_error(format!("unexpected end of metric name {}", quote_byte(b)));
                        return None;
                    }
                }
            }
            self.parse_error(format!("expected '=' after label name, found {}", quote_byte(self.current_byte)));
            self.current_label_pairs.clear();
            return None;
        }
        if self.current_token == b"__name__" {
            self.parse_error(format!("label name {} is reserved", quote_str("__name__")));
            self.current_label_pairs.clear();
            return None;
        }
        if !self.scheme.is_valid_label_name(&self.current_token) {
            let msg = format!("invalid label name {}", quote_bytes(&self.current_token));
            self.parse_error(msg);
            self.current_label_pairs.clear();
            return None;
        }
        // Valid UTF-8 after the check above (both schemes).
        let name = String::from_utf8_lossy(&self.current_token).into_owned();
        self.current_label_pair = LabelPair { name: name.clone(), value: String::new() };
        let t = self.mf_type();
        if (t != MetricType::SUMMARY || name != "quantile")
            && ((t != MetricType::HISTOGRAM && t != MetricType::GAUGE_HISTOGRAM) || name != "le")
        {
            // The value is filled in by startLabelValue (Go appends the pointer); index it by position.
            self.current_label_pairs.push(self.current_label_pair.clone());
            self.pending_pair = true;
        } else {
            self.pending_pair = false;
        }
        // Check for duplicate label names.
        let mut seen = std::collections::HashSet::new();
        for l in &self.current_label_pairs {
            if !seen.insert(l.name.as_str()) {
                let msg = format!("duplicate label names for metric {}", quote_str(&self.mf_name()));
                self.parse_error(msg);
                self.current_label_pairs.clear();
                return None;
            }
        }
        Some(State::StartLabelValue)
    }

    fn start_label_value(&mut self) -> Option<State> {
        self.skip_blank_tab();
        if self.err.is_some() {
            return None;
        }
        if self.current_byte != b'"' {
            self.parse_error(format!("expected '\"' at start of label value, found {}", quote_byte(self.current_byte)));
            return None;
        }
        self.read_token_as_label_value();
        if self.err.is_some() {
            return None;
        }
        let Ok(value) = String::from_utf8(self.current_token.clone()) else {
            let msg = format!("invalid label value {}", quote_bytes(&self.current_token));
            self.parse_error(msg);
            return None;
        };
        self.current_label_pair.value = value.clone();
        if self.pending_pair
            && let Some(last) = self.current_label_pairs.last_mut()
        {
            last.value = value.clone();
        }
        let t = self.mf_type();
        let name = self.current_label_pair.name.clone();
        if t == MetricType::SUMMARY {
            if name == "quantile" {
                match parse_float(value.as_bytes()) {
                    Some(q) => self.current_quantile = q,
                    None => {
                        // Go assigns the failed parse result (0) before the error.
                        self.current_quantile = 0.0;
                        self.parse_error(format!("expected float as value for 'quantile' label, got {}", quote_str(&value)));
                        self.current_label_pairs.clear();
                        return None;
                    }
                }
            } else {
                self.current_labels.insert(name.clone(), value.clone());
            }
        }
        if t == MetricType::HISTOGRAM || t == MetricType::GAUGE_HISTOGRAM {
            if name == "le" {
                match parse_float(value.as_bytes()) {
                    Some(b) => self.current_bucket = b,
                    None => {
                        self.current_bucket = 0.0;
                        self.parse_error(format!("expected float as value for 'le' label, got {}", quote_str(&value)));
                        return None;
                    }
                }
            } else {
                self.current_labels.insert(name, value.clone());
            }
        }
        self.skip_blank_tab();
        if self.err.is_some() {
            return None;
        }
        match self.current_byte {
            b',' => Some(State::StartLabelName),
            b'}' => {
                if matches!(self.current_metric, CurMetric::None) {
                    self.parse_error("invalid metric name".to_string());
                    return None;
                }
                let pairs = std::mem::take(&mut self.current_label_pairs);
                self.metric_mut().label.extend(pairs);
                self.skip_blank_tab();
                if self.err.is_some() {
                    return None;
                }
                Some(State::ReadingValue)
            }
            _ => {
                self.parse_error(format!("unexpected end of label value {}", quote_str(&value)));
                self.current_label_pairs.clear();
                None
            }
        }
    }

    fn reading_value(&mut self) -> Option<State> {
        let fi = self.current_mf.expect("current metric family");
        let t = self.families[fi].get_type();
        let cur = std::mem::replace(&mut self.current_metric, CurMetric::None);
        let new_metric = match cur {
            CurMetric::New(m) => m,
            CurMetric::At(f, i) => {
                // Not reachable from the text format; keep the existing metric.
                self.current_metric = CurMetric::At(f, i);
                return self.reading_value_rest(fi, t);
            }
            CurMetric::None => unreachable!("no current metric"),
        };
        if Self::is_summary_or_histogram(t) {
            let sig: Vec<(String, String)> =
                self.current_labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            let map = if t == MetricType::SUMMARY { &mut self.summaries } else { &mut self.histograms };
            if let Some(&(f, i)) = map.get(&sig) {
                self.current_metric = CurMetric::At(f, i);
            } else {
                let idx = self.families[fi].metric.len();
                map.insert(sig, (fi, idx));
                self.families[fi].metric.push(new_metric);
                self.current_metric = CurMetric::At(fi, idx);
            }
        } else {
            let idx = self.families[fi].metric.len();
            self.families[fi].metric.push(new_metric);
            self.current_metric = CurMetric::At(fi, idx);
        }
        self.reading_value_rest(fi, t)
    }

    fn reading_value_rest(&mut self, fi: usize, t: MetricType) -> Option<State> {
        self.read_token_until_whitespace();
        if self.err.is_some() {
            return None;
        }
        let Some(value) = parse_float(&self.current_token) else {
            let msg = format!("expected float as value, got {}", quote_bytes(&self.current_token));
            self.parse_error(msg);
            return None;
        };
        let name = self.families[fi].name.clone();
        let (is_sc, is_ss, is_hc, is_hs) = (
            self.current_is_summary_count,
            self.current_is_summary_sum,
            self.current_is_histogram_count,
            self.current_is_histogram_sum,
        );
        let (q, bucket) = (self.current_quantile, self.current_bucket);
        match t {
            MetricType::COUNTER => self.metric_mut().counter = Some(value),
            MetricType::GAUGE => self.metric_mut().gauge = Some(value),
            MetricType::UNTYPED => self.metric_mut().untyped = Some(value),
            MetricType::SUMMARY => {
                let s = self.metric_mut().summary.get_or_insert_with(Summary::default);
                if is_sc {
                    s.sample_count = Some(go_f64_to_u64(value));
                } else if is_ss {
                    s.sample_sum = Some(value);
                } else if !q.is_nan() {
                    s.quantile.push(Quantile { quantile: q, value });
                }
            }
            MetricType::HISTOGRAM | MetricType::GAUGE_HISTOGRAM => {
                let h = self.metric_mut().histogram.get_or_insert_with(Histogram::default);
                if is_hc {
                    if is_uint(value) {
                        h.sample_count = Some(value as u64);
                    } else {
                        if value < 0.0 {
                            self.parse_error(format!("negative count for histogram {}", quote_str(&name)));
                            return None;
                        }
                        h.sample_count_float = Some(value);
                    }
                } else if is_hs {
                    h.sample_sum = Some(value);
                } else if !bucket.is_nan() {
                    let mut b = Bucket { upper_bound: bucket, ..Default::default() };
                    if is_uint(value) {
                        b.cumulative_count = Some(value as u64);
                    } else {
                        if value < 0.0 {
                            self.parse_error(format!("negative bucket population for histogram {}", quote_str(&name)));
                            return None;
                        }
                        b.cumulative_count_float = Some(value);
                    }
                    h.bucket.push(b);
                }
            }
            _ => {
                self.err = Some(Err::Other);
                self.err_value = Some(TextError::Other(format!("unexpected type for metric name {}", quote_str(&name))));
            }
        }
        if self.current_byte == b'\n' {
            return Some(State::StartOfLine);
        }
        Some(State::StartTimestamp)
    }

    fn start_timestamp(&mut self) -> Option<State> {
        self.skip_blank_tab();
        if self.err.is_some() {
            return None;
        }
        self.read_token_until_whitespace();
        if self.err.is_some() {
            return None;
        }
        let ts = match parse_int(&self.current_token) {
            Ok(ts) => ts,
            Err(_) => {
                let msg = format!("expected integer as timestamp, got {}", quote_bytes(&self.current_token));
                self.parse_error(msg);
                return None;
            }
        };
        self.metric_mut().timestamp_ms = Some(ts);
        self.read_token_until_newline(false);
        if self.err.is_some() {
            return None;
        }
        if !self.current_token.is_empty() {
            let msg = format!("spurious string after timestamp: {}", quote_bytes(&self.current_token));
            self.parse_error(msg);
            return None;
        }
        Some(State::StartOfLine)
    }

    fn reading_help(&mut self) -> Option<State> {
        let fi = self.current_mf.expect("current metric family");
        if self.families[fi].help.is_some() {
            let msg = format!("second HELP line for metric name {}", quote_str(&self.families[fi].name));
            self.parse_error(msg);
            return None;
        }
        self.read_token_until_newline(true);
        if self.err.is_some() {
            return None;
        }
        self.families[fi].help = Some(String::from_utf8_lossy(&self.current_token).into_owned());
        Some(State::StartOfLine)
    }

    fn reading_type(&mut self) -> Option<State> {
        let fi = self.current_mf.expect("current metric family");
        if self.families[fi].r#type.is_some() {
            let msg = format!(
                "second TYPE line for metric name {}, or TYPE reported after samples",
                quote_str(&self.families[fi].name)
            );
            self.parse_error(msg);
            return None;
        }
        self.read_token_until_newline(false);
        if self.err.is_some() {
            return None;
        }
        // Go strings.ToUpper (Unicode); invalid UTF-8 never names a type.
        let typ = String::from_utf8_lossy(&self.current_token).to_uppercase();
        match metric_type_from_upper(&typ) {
            Some(t) => self.families[fi].r#type = Some(t),
            None => {
                let msg = format!("unknown metric type {}", quote_bytes(&self.current_token));
                self.parse_error(msg);
                return None;
            }
        }
        Some(State::StartOfLine)
    }

    fn skip_blank_tab(&mut self) {
        loop {
            self.read_byte();
            if self.err.is_some() || !is_blank_or_tab(self.current_byte) {
                return;
            }
        }
    }

    fn skip_blank_tab_if_current_blank_tab(&mut self) {
        if is_blank_or_tab(self.current_byte) {
            self.skip_blank_tab();
        }
    }

    fn read_token_until_whitespace(&mut self) {
        self.current_token.clear();
        while self.err.is_none() && !is_blank_or_tab(self.current_byte) && self.current_byte != b'\n' {
            self.current_token.push(self.current_byte);
            self.read_byte();
        }
    }

    fn read_token_until_newline(&mut self, recognize_escape_sequence: bool) {
        self.current_token.clear();
        let mut escaped = false;
        while self.err.is_none() {
            if recognize_escape_sequence && escaped {
                match self.current_byte {
                    b'\\' => self.current_token.push(b'\\'),
                    b'n' => self.current_token.push(b'\n'),
                    b'"' => self.current_token.push(b'"'),
                    c => {
                        self.parse_error(format!("invalid escape sequence '\\{}'", char::from(c)));
                        return;
                    }
                }
                escaped = false;
            } else {
                match self.current_byte {
                    b'\n' => return,
                    b'\\' => escaped = true,
                    c => self.current_token.push(c),
                }
            }
            self.read_byte();
        }
    }

    /// Shared body of readTokenAsMetricName and readTokenAsLabelName.
    fn read_token_as_name(&mut self, label: bool) {
        self.current_token.clear();
        let mut quoted = false;
        let mut escaped = false;
        let start_ok =
            if label { is_valid_label_name_start(self.current_byte) } else { is_valid_metric_name_start(self.current_byte) };
        if !start_ok {
            return;
        }
        while self.err.is_none() {
            if escaped {
                match self.current_byte {
                    b'\\' => self.current_token.push(b'\\'),
                    b'n' => self.current_token.push(b'\n'),
                    b'"' => self.current_token.push(b'"'),
                    c => {
                        self.parse_error(format!("invalid escape sequence '\\{}'", char::from(c)));
                        return;
                    }
                }
                escaped = false;
            } else {
                match self.current_byte {
                    b'"' => {
                        quoted = !quoted;
                        if !quoted {
                            self.read_byte();
                            return;
                        }
                    }
                    b'\n' => {
                        let what = if label { "label name" } else { "metric name" };
                        let msg = format!("{what} {} contains unescaped new-line", quote_bytes(&self.current_token));
                        self.parse_error(msg);
                        return;
                    }
                    b'\\' => escaped = true,
                    c => self.current_token.push(c),
                }
            }
            self.read_byte();
            let b = self.current_byte;
            if label {
                if !is_valid_label_name_continuation(b, quoted) || (!quoted && b == b'=') {
                    return;
                }
            } else if !is_valid_metric_name_continuation(b, quoted) || (!quoted && b == b' ') {
                return;
            }
        }
    }

    fn read_token_as_metric_name(&mut self) {
        self.read_token_as_name(false);
    }

    fn read_token_as_label_name(&mut self) {
        self.read_token_as_name(true);
    }

    fn read_token_as_label_value(&mut self) {
        self.current_token.clear();
        let mut escaped = false;
        loop {
            self.read_byte();
            if self.err.is_some() {
                return;
            }
            if escaped {
                match self.current_byte {
                    b'"' | b'\\' => self.current_token.push(self.current_byte),
                    b'n' => self.current_token.push(b'\n'),
                    c => {
                        self.parse_error(format!("invalid escape sequence '\\{}'", char::from(c)));
                        self.current_label_pairs.clear();
                        return;
                    }
                }
                escaped = false;
                continue;
            }
            match self.current_byte {
                b'"' => return,
                b'\n' => {
                    let msg = format!("label value {} contains unescaped new-line", quote_bytes(&self.current_token));
                    self.parse_error(msg);
                    return;
                }
                b'\\' => escaped = true,
                c => self.current_token.push(c),
            }
        }
    }

    fn set_or_create_current_mf(&mut self) {
        self.current_is_summary_count = false;
        self.current_is_summary_sum = false;
        self.current_is_histogram_count = false;
        self.current_is_histogram_sum = false;
        if !self.scheme.is_valid_metric_name(&self.current_token) {
            let msg = format!("invalid metric name {}", quote_bytes(&self.current_token));
            self.parse_error(msg);
            return;
        }
        let name = String::from_utf8_lossy(&self.current_token).into_owned();
        if let Some(&i) = self.by_name.get(&name) {
            self.current_mf = Some(i);
            return;
        }
        let summary_name = summary_metric_name(&name);
        self.current_mf = self.by_name.get(summary_name).copied();
        if let Some(i) = self.current_mf
            && self.families[i].get_type() == MetricType::SUMMARY
        {
            if is_count(&name) {
                self.current_is_summary_count = true;
            }
            if is_sum(&name) {
                self.current_is_summary_sum = true;
            }
            return;
        }
        let histogram_name = histogram_metric_name(&name);
        self.current_mf = self.by_name.get(histogram_name).copied();
        if let Some(i) = self.current_mf {
            let t = self.families[i].get_type();
            if t == MetricType::HISTOGRAM || t == MetricType::GAUGE_HISTOGRAM {
                if is_count(&name) {
                    self.current_is_histogram_count = true;
                }
                if is_sum(&name) {
                    self.current_is_histogram_sum = true;
                }
                return;
            }
        }
        let idx = self.families.len();
        self.families.push(MetricFamily { name: name.clone(), ..Default::default() });
        self.by_name.insert(name, idx);
        self.current_mf = Some(idx);
    }
}

/// Go `normalizeHistogram`.
fn normalize_histogram(h: &mut Histogram) {
    let any_floats = h.get_sample_count_float() != 0.0 || h.bucket.iter().any(|b| b.get_cumulative_count_float() != 0.0);
    if !any_floats {
        return;
    }
    if h.get_sample_count_float() == 0.0 {
        h.sample_count_float = Some(h.get_sample_count() as f64);
        h.sample_count = None;
    }
    for b in &mut h.bucket {
        if b.get_cumulative_count_float() == 0.0 {
            b.cumulative_count_float = Some(b.get_cumulative_count() as f64);
            b.cumulative_count = None;
        }
    }
}
