//! Port of prometheus/common `expfmt.TextParser` (v0.72.1, `expfmt/text_parse.go`): parses the Prometheus text
//! format into metric families, with the same error texts. Used by `util::fmtutil` (promtool push metrics).
//!
//! The metric family types are owned forms of the client_model protobuf messages (`dto.MetricFamily` and
//! friends); optional protobuf fields are `Option`s where the parser leaves them unset.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io::{BufRead, BufReader, Read};

use model::labels::{BUCKET_LABEL, METRIC_NAME, ValidationScheme};

use crate::strconv::{go_f64_to_i64, parse_float as go_parse_float, parse_int, quote_bytes};

/// Go `model.QuantileLabel`.
const QUANTILE_LABEL: &str = "quantile";

/// Go `dto.MetricType` values.
pub mod metric_type {
    pub const COUNTER: i32 = 0;
    pub const GAUGE: i32 = 1;
    pub const SUMMARY: i32 = 2;
    pub const UNTYPED: i32 = 3;
    pub const HISTOGRAM: i32 = 4;
    pub const GAUGE_HISTOGRAM: i32 = 5;

    /// Go `MetricType.String()`: the enum name, or the number for unknown values.
    pub fn name(t: i32) -> String {
        match t {
            COUNTER => "COUNTER".to_string(),
            GAUGE => "GAUGE".to_string(),
            SUMMARY => "SUMMARY".to_string(),
            UNTYPED => "UNTYPED".to_string(),
            HISTOGRAM => "HISTOGRAM".to_string(),
            GAUGE_HISTOGRAM => "GAUGE_HISTOGRAM".to_string(),
            n => n.to_string(),
        }
    }

    /// Go `dto.MetricType_value`.
    pub fn value(name: &str) -> Option<i32> {
        Some(match name {
            "COUNTER" => COUNTER,
            "GAUGE" => GAUGE,
            "SUMMARY" => SUMMARY,
            "UNTYPED" => UNTYPED,
            "HISTOGRAM" => HISTOGRAM,
            "GAUGE_HISTOGRAM" => GAUGE_HISTOGRAM,
            _ => return None,
        })
    }
}

/// `dto.MetricFamily`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetricFamily {
    pub name: String,
    pub help: Option<String>,
    /// A [`metric_type`] value; `None` until a TYPE line or the first sample sets it.
    pub r#type: Option<i32>,
    pub metric: Vec<Metric>,
}

impl MetricFamily {
    /// Go `GetType()`: COUNTER when unset.
    pub fn get_type(&self) -> i32 {
        self.r#type.unwrap_or(metric_type::COUNTER)
    }
}

/// `dto.LabelPair`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LabelPair {
    pub name: String,
    pub value: String,
}

/// `dto.Gauge`, `dto.Counter` and `dto.Untyped` (the parser sets only the value).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Value {
    pub value: f64,
}

/// `dto.Quantile`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Quantile {
    pub quantile: f64,
    pub value: f64,
}

/// `dto.Summary`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Summary {
    pub sample_count: Option<u64>,
    pub sample_sum: Option<f64>,
    pub quantile: Vec<Quantile>,
}

/// `dto.Bucket`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Bucket {
    pub cumulative_count: Option<u64>,
    pub cumulative_count_float: Option<f64>,
    pub upper_bound: Option<f64>,
}

/// `dto.Histogram` (the classic fields).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Histogram {
    pub sample_count: Option<u64>,
    pub sample_count_float: Option<f64>,
    pub sample_sum: Option<f64>,
    pub bucket: Vec<Bucket>,
}

/// `dto.Metric`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Metric {
    pub label: Vec<LabelPair>,
    pub gauge: Option<Value>,
    pub counter: Option<Value>,
    pub summary: Option<Summary>,
    pub untyped: Option<Value>,
    pub histogram: Option<Histogram>,
    pub timestamp_ms: Option<i64>,
}

/// The error of [`TextParser::text_to_metric_families`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TextParseError {
    /// Go `ParseError`.
    Parse { line: usize, msg: String },
    /// Any other error (a read error of the input).
    Other(String),
}

impl fmt::Display for TextParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TextParseError::Parse { line, msg } => write!(f, "text format parsing error in line {line}: {msg}"),
            TextParseError::Other(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for TextParseError {}

/// Go `TextParser`: parses the simple and flat text-based exchange format.
#[derive(Clone, Copy, Debug)]
pub struct TextParser {
    scheme: ValidationScheme,
}

impl TextParser {
    /// Go `NewTextParser`.
    pub fn new(name_validation_scheme: ValidationScheme) -> TextParser {
        TextParser { scheme: name_validation_scheme }
    }

    /// Go `TextToMetricFamilies`: reads `input` and returns the metric families by name.
    ///
    /// Duplicate metrics and duplicate label names across lines are kept, metrics and label pairs are not
    /// sorted, as in Go. Go also returns the families parsed before an error; this returns only the error.
    pub fn text_to_metric_families(&self, input: impl Read) -> Result<BTreeMap<String, MetricFamily>, TextParseError> {
        let mut p = Run {
            scheme: self.scheme,
            families: BTreeMap::new(),
            buf: BufReader::new(input),
            err: None,
            line_count: 0,
            current_byte: 0,
            current_token: Vec::new(),
            current_mf: None,
            current_metric: Cur::None,
            current_label_pair: None,
            current_label_pairs: Vec::new(),
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
        };
        let mut next = Some(State::StartOfLine);
        while let Some(s) = next {
            next = p.step(s);
        }
        // Get rid of empty metric families.
        p.families.retain(|_, mf| !mf.metric.is_empty());
        // io.EOF now means a premature end of the input stream.
        if matches!(p.err, Some(Err::Eof)) {
            p.parse_error("unexpected end of input stream".to_string());
        }
        for (name, idx) in p.histograms.values() {
            if let Some(h) = p.families.get_mut(name).and_then(|mf| mf.metric.get_mut(*idx)).and_then(|m| m.histogram.as_mut()) {
                normalize_histogram(h);
            }
        }
        match p.err {
            None => Ok(p.families),
            Some(Err::Parse { line, msg }) => Err(TextParseError::Parse { line, msg }),
            Some(Err::Other(s)) => Err(TextParseError::Other(s)),
            Some(Err::Eof) => unreachable!(),
        }
    }
}

/// Go `normalizeHistogram`: makes all the buckets and the count of a histogram either float or integer.
fn normalize_histogram(h: &mut Histogram) {
    let any_floats = h.sample_count_float.unwrap_or(0.0) != 0.0
        || h.bucket.iter().any(|b| b.cumulative_count_float.unwrap_or(0.0) != 0.0);
    if !any_floats {
        return;
    }
    if h.sample_count_float.unwrap_or(0.0) == 0.0 {
        h.sample_count_float = Some(h.sample_count.unwrap_or(0) as f64);
        h.sample_count = None;
    }
    for b in &mut h.bucket {
        if b.cumulative_count_float.unwrap_or(0.0) == 0.0 {
            b.cumulative_count_float = Some(b.cumulative_count.unwrap_or(0) as f64);
            b.cumulative_count = None;
        }
    }
}

enum Err {
    Eof,
    Parse { line: usize, msg: String },
    Other(String),
}

#[derive(Clone, Copy)]
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

/// Go `currentMetric`: none, a new metric not yet added to the family, or a metric of the current family.
enum Cur {
    None,
    Pending(Metric),
    At(usize),
}

struct Run<R: Read> {
    scheme: ValidationScheme,
    families: BTreeMap<String, MetricFamily>,
    buf: BufReader<R>,
    err: Option<Err>,
    line_count: usize,
    current_byte: u8,
    current_token: Vec<u8>,
    current_mf: Option<String>,
    current_metric: Cur,
    /// The label pair being read, and its index in `current_label_pairs` when it was added there.
    current_label_pair: Option<(LabelPair, Option<usize>)>,
    current_label_pairs: Vec<LabelPair>,
    // Only for summaries and histograms.
    /// All labels including `__name__` but excluding `quantile`/`le`.
    current_labels: BTreeMap<String, String>,
    /// Go keys by `LabelsToSignature(currentLabels)`; this keys by the labels themselves (no hash collisions).
    summaries: HashMap<BTreeMap<String, String>, (String, usize)>,
    current_quantile: f64,
    histograms: HashMap<BTreeMap<String, String>, (String, usize)>,
    current_bucket: f64,
    current_is_summary_count: bool,
    current_is_summary_sum: bool,
    current_is_histogram_count: bool,
    current_is_histogram_sum: bool,
    current_metric_is_inside_braces: bool,
    current_metric_inside_braces_is_present: bool,
}

impl<R: Read> Run<R> {
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

    fn mf(&self) -> Option<&MetricFamily> {
        self.current_mf.as_ref().and_then(|n| self.families.get(n))
    }

    fn mf_mut(&mut self) -> &mut MetricFamily {
        let name = self.current_mf.as_ref().expect("current metric family");
        self.families.get_mut(name).expect("current metric family")
    }

    /// Go `p.currentMF.GetType()` (COUNTER for a nil family).
    fn mf_type(&self) -> i32 {
        self.mf().map_or(metric_type::COUNTER, |m| m.get_type())
    }

    /// Go `p.currentMF.GetName()` ("" for a nil family).
    fn mf_name(&self) -> String {
        self.mf().map_or(String::new(), |m| m.name.clone())
    }

    fn is_summary(&self) -> bool {
        self.mf_type() == metric_type::SUMMARY
    }

    fn is_histogram(&self) -> bool {
        let t = self.mf_type();
        t == metric_type::HISTOGRAM || t == metric_type::GAUGE_HISTOGRAM
    }

    fn metric_mut(&mut self) -> &mut Metric {
        match self.current_metric {
            Cur::Pending(ref mut m) => m,
            Cur::At(i) => &mut self.mf_mut().metric[i],
            Cur::None => panic!("no current metric"),
        }
    }

    fn token_string(&self) -> String {
        String::from_utf8_lossy(&self.current_token).into_owned()
    }

    fn read_byte(&mut self) -> Result<u8, Err> {
        loop {
            let b = match self.buf.fill_buf() {
                Ok(b) => b,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(Err::Other(e.to_string())),
            };
            let Some(&c) = b.first() else { return Err(Err::Eof) };
            self.buf.consume(1);
            return Ok(c);
        }
    }

    /// `p.currentByte, p.err = p.buf.ReadByte()`; true on success.
    fn next_byte(&mut self) -> bool {
        match self.read_byte() {
            Ok(c) => {
                self.current_byte = c;
                self.err = None;
                true
            }
            Err(e) => {
                // Go's ReadByte returns 0 with an error.
                self.current_byte = 0;
                self.err = Some(e);
                false
            }
        }
    }

    fn failed(&self) -> bool {
        self.err.is_some()
    }

    /// Go `startOfLine`.
    fn start_of_line(&mut self) -> Option<State> {
        self.line_count += 1;
        self.current_metric = Cur::None;
        self.current_metric_is_inside_braces = false;
        self.current_metric_inside_braces_is_present = false;
        self.skip_blank_tab();
        if self.failed() {
            // This is the only place that we expect to see io.EOF, which is not an error but the signal that we
            // are done.
            if matches!(self.err, Some(Err::Eof)) {
                self.err = None;
            }
            return None;
        }
        match self.current_byte {
            b'#' => Some(State::StartComment),
            // Empty line, start the next one.
            b'\n' => Some(State::StartOfLine),
            b'{' => {
                self.current_metric_is_inside_braces = true;
                Some(State::ReadingLabels)
            }
            _ => Some(State::ReadingMetricName),
        }
    }

    /// Go `startComment`.
    fn start_comment(&mut self) -> Option<State> {
        self.skip_blank_tab();
        if self.failed() {
            return None;
        }
        if self.current_byte == b'\n' {
            return Some(State::StartOfLine);
        }
        self.read_token_until_whitespace();
        if self.failed() {
            return None;
        }
        // If we have hit the end of line already, there is nothing left to do. This is not considered a syntax
        // error.
        if self.current_byte == b'\n' {
            return Some(State::StartOfLine);
        }
        let keyword = self.current_token.clone();
        if keyword != b"HELP" && keyword != b"TYPE" {
            // Generic comment, ignore by fast forwarding to end of line.
            while self.current_byte != b'\n' {
                if !self.next_byte() {
                    return None;
                }
            }
            return Some(State::StartOfLine);
        }
        // There is something. Next has to be a metric name.
        self.skip_blank_tab();
        if self.failed() {
            return None;
        }
        self.read_token_as_metric_name();
        if self.failed() {
            return None;
        }
        if self.current_byte == b'\n' {
            // At the end of the line already. Again, this is not considered a syntax error.
            return Some(State::StartOfLine);
        }
        if !is_blank_or_tab(self.current_byte) {
            self.parse_error("invalid metric name in comment".to_string());
            return None;
        }
        self.set_or_create_current_mf();
        if self.failed() {
            return None;
        }
        self.skip_blank_tab();
        if self.failed() {
            return None;
        }
        if self.current_byte == b'\n' {
            return Some(State::StartOfLine);
        }
        if keyword == b"HELP" { Some(State::ReadingHelp) } else { Some(State::ReadingType) }
    }

    /// Go `readingMetricName`.
    fn reading_metric_name(&mut self) -> Option<State> {
        self.read_token_as_metric_name();
        if self.failed() {
            return None;
        }
        if self.current_token.is_empty() {
            self.parse_error("invalid metric name".to_string());
            return None;
        }
        self.set_or_create_current_mf();
        if self.failed() {
            return None;
        }
        // Now is the time to fix the type if it hasn't happened yet.
        let mf = self.mf_mut();
        if mf.r#type.is_none() {
            mf.r#type = Some(metric_type::UNTYPED);
        }
        // Do not add the new metric to the family right now: a summary or histogram metric may exist already,
        // which we can only know after having read all the labels.
        self.current_metric = Cur::Pending(Metric::default());
        self.skip_blank_tab_if_current_blank_tab();
        if self.failed() {
            return None;
        }
        Some(State::ReadingLabels)
    }

    /// Go `readingLabels`.
    fn reading_labels(&mut self) -> Option<State> {
        // Summaries/histograms are special: reset the current labels, quantile and bucket before reading labels.
        if self.is_summary() || self.is_histogram() {
            self.current_labels = BTreeMap::new();
            self.current_labels.insert(METRIC_NAME.to_string(), self.mf_name());
            self.current_quantile = f64::NAN;
            self.current_bucket = f64::NAN;
        }
        if self.current_byte != b'{' {
            return Some(State::ReadingValue);
        }
        Some(State::StartLabelName)
    }

    fn append_label_pairs_to_metric(&mut self) {
        let pairs = std::mem::take(&mut self.current_label_pairs);
        self.metric_mut().label.extend(pairs);
    }

    /// Go `startLabelName`.
    fn start_label_name(&mut self) -> Option<State> {
        self.skip_blank_tab();
        if self.failed() {
            return None;
        }
        if self.current_byte == b'}' {
            if matches!(self.current_metric, Cur::None) {
                // The closing brace was reached before any metric name was read, e.g. for the input "{}".
                self.parse_error("invalid metric name".to_string());
                self.current_label_pairs.clear();
                return None;
            }
            self.append_label_pairs_to_metric();
            self.skip_blank_tab();
            if self.failed() {
                return None;
            }
            return Some(State::ReadingValue);
        }
        self.read_token_as_label_name();
        if self.failed() {
            return None;
        }
        if self.current_token.is_empty() {
            self.parse_error(format!("invalid label name for metric {}", go_quote(&self.mf_name())));
            return None;
        }
        self.skip_blank_tab_if_current_blank_tab();
        if self.failed() {
            return None;
        }
        if self.current_byte != b'=' {
            if self.current_metric_is_inside_braces {
                if self.current_metric_inside_braces_is_present {
                    self.parse_error(format!("multiple metric names for metric {}", go_quote(&self.mf_name())));
                    return None;
                }
                match self.current_byte {
                    b',' => {
                        self.set_or_create_current_mf();
                        if self.failed() {
                            return None;
                        }
                        let mf = self.mf_mut();
                        if mf.r#type.is_none() {
                            mf.r#type = Some(metric_type::UNTYPED);
                        }
                        self.current_metric = Cur::Pending(Metric::default());
                        self.current_metric_inside_braces_is_present = true;
                        return Some(State::StartLabelName);
                    }
                    b'}' => {
                        self.set_or_create_current_mf();
                        if self.failed() {
                            self.current_label_pairs.clear();
                            return None;
                        }
                        let mf = self.mf_mut();
                        if mf.r#type.is_none() {
                            mf.r#type = Some(metric_type::UNTYPED);
                        }
                        self.current_metric = Cur::Pending(Metric::default());
                        self.append_label_pairs_to_metric();
                        self.skip_blank_tab();
                        if self.failed() {
                            return None;
                        }
                        return Some(State::ReadingValue);
                    }
                    c => {
                        self.parse_error(format!("unexpected end of metric name {}", quote_byte(c)));
                        return None;
                    }
                }
            }
            self.parse_error(format!("expected '=' after label name, found {}", quote_byte(self.current_byte)));
            self.current_label_pairs.clear();
            return None;
        }
        let name = self.token_string();
        if name == METRIC_NAME {
            self.parse_error(format!("label name {} is reserved", go_quote(METRIC_NAME)));
            self.current_label_pairs.clear();
            return None;
        }
        if !(std::str::from_utf8(&self.current_token).is_ok() && self.scheme.is_valid_label_name(&name)) {
            self.parse_error(format!("invalid label name {}", go_quote(&name)));
            self.current_label_pairs.clear();
            return None;
        }
        // Special summary/histogram treatment. Don't add 'quantile' and 'le' labels to 'real' labels.
        let mut idx = None;
        if (!self.is_summary() || name != QUANTILE_LABEL) && (!self.is_histogram() || name != BUCKET_LABEL) {
            idx = Some(self.current_label_pairs.len());
            self.current_label_pairs.push(LabelPair { name: name.clone(), value: String::new() });
        }
        self.current_label_pair = Some((LabelPair { name, value: String::new() }, idx));
        // Check for duplicate label names.
        let mut seen = std::collections::HashSet::new();
        for l in &self.current_label_pairs {
            if !seen.insert(l.name.as_str()) {
                let msg = format!("duplicate label names for metric {}", go_quote(&self.mf_name()));
                self.parse_error(msg);
                self.current_label_pairs.clear();
                return None;
            }
        }
        Some(State::StartLabelValue)
    }

    /// Go `startLabelValue`.
    fn start_label_value(&mut self) -> Option<State> {
        self.skip_blank_tab();
        if self.failed() {
            return None;
        }
        if self.current_byte != b'"' {
            self.parse_error(format!("expected '\"' at start of label value, found {}", quote_byte(self.current_byte)));
            return None;
        }
        self.read_token_as_label_value();
        if self.failed() {
            return None;
        }
        let Ok(value) = String::from_utf8(self.current_token.clone()) else {
            self.parse_error(format!("invalid label value {}", quote_bytes(&self.current_token)));
            return None;
        };
        let (name, idx) = {
            let (pair, idx) = self.current_label_pair.as_mut().expect("current label pair");
            pair.value = value.clone();
            (pair.name.clone(), *idx)
        };
        if let Some(i) = idx {
            self.current_label_pairs[i].value = value.clone();
        }
        // Special treatment of summaries: quantile labels result in a dto.Quantile later; other labels are added
        // to the current labels for the signature.
        if self.is_summary() {
            if name == QUANTILE_LABEL {
                match parse_float(value.as_bytes()) {
                    Ok(q) => self.current_quantile = q,
                    Err(_) => {
                        // Go also leaves p.currentQuantile at 0.
                        self.current_quantile = 0.0;
                        self.parse_error(format!("expected float as value for 'quantile' label, got {}", go_quote(&value)));
                        self.current_label_pairs.clear();
                        return None;
                    }
                }
            } else {
                self.current_labels.insert(name.clone(), value.clone());
            }
        }
        // Similar special treatment of histograms.
        if self.is_histogram() {
            if name == BUCKET_LABEL {
                match parse_float(value.as_bytes()) {
                    Ok(b) => self.current_bucket = b,
                    Err(_) => {
                        self.current_bucket = 0.0;
                        self.parse_error(format!("expected float as value for 'le' label, got {}", go_quote(&value)));
                        return None;
                    }
                }
            } else {
                self.current_labels.insert(name, value.clone());
            }
        }
        self.skip_blank_tab();
        if self.failed() {
            return None;
        }
        match self.current_byte {
            b',' => Some(State::StartLabelName),
            b'}' => {
                if matches!(self.current_metric, Cur::None) {
                    self.parse_error("invalid metric name".to_string());
                    return None;
                }
                self.append_label_pairs_to_metric();
                self.skip_blank_tab();
                if self.failed() {
                    return None;
                }
                Some(State::ReadingValue)
            }
            _ => {
                self.parse_error(format!("unexpected end of label value {}", go_quote(&value)));
                self.current_label_pairs.clear();
                None
            }
        }
    }

    /// Adds the pending metric to the current family, or makes the summary or histogram with the same labels the
    /// current metric.
    fn place_current_metric(&mut self, summary: bool) {
        let pending = std::mem::replace(&mut self.current_metric, Cur::None);
        let Cur::Pending(m) = pending else {
            self.current_metric = pending;
            return;
        };
        let name = self.current_mf.clone().expect("current metric family");
        let map = if summary { &mut self.summaries } else { &mut self.histograms };
        if let Some((fam, idx)) = map.get(&self.current_labels)
            && *fam == name
        {
            self.current_metric = Cur::At(*idx);
            return;
        }
        let mf = self.families.get_mut(&name).expect("current metric family");
        mf.metric.push(m);
        let idx = mf.metric.len() - 1;
        let map = if summary { &mut self.summaries } else { &mut self.histograms };
        map.insert(self.current_labels.clone(), (name, idx));
        self.current_metric = Cur::At(idx);
    }

    /// Go `readingValue`.
    fn reading_value(&mut self) -> Option<State> {
        // We have read all the labels, so for a summary/histogram we can now find out if the metric exists.
        if self.is_summary() {
            self.place_current_metric(true);
        } else if self.is_histogram() {
            self.place_current_metric(false);
        } else if let Cur::Pending(m) = std::mem::replace(&mut self.current_metric, Cur::None) {
            let mf = self.mf_mut();
            mf.metric.push(m);
            self.current_metric = Cur::At(mf.metric.len() - 1);
        }
        self.read_token_until_whitespace();
        if self.failed() {
            return None;
        }
        let Ok(value) = parse_float(&self.current_token) else {
            self.parse_error(format!("expected float as value, got {}", quote_bytes(&self.current_token)));
            return None;
        };
        let t = self.mf_type();
        let name = self.mf_name();
        let (is_summary_count, is_summary_sum) = (self.current_is_summary_count, self.current_is_summary_sum);
        let (is_histogram_count, is_histogram_sum) = (self.current_is_histogram_count, self.current_is_histogram_sum);
        let (quantile, bucket) = (self.current_quantile, self.current_bucket);
        let m = self.metric_mut();
        match t {
            metric_type::COUNTER => m.counter = Some(Value { value }),
            metric_type::GAUGE => m.gauge = Some(Value { value }),
            metric_type::UNTYPED => m.untyped = Some(Value { value }),
            metric_type::SUMMARY => {
                let s = m.summary.get_or_insert_with(Summary::default);
                if is_summary_count {
                    s.sample_count = Some(go_f64_to_u64(value));
                } else if is_summary_sum {
                    s.sample_sum = Some(value);
                } else if !quantile.is_nan() {
                    s.quantile.push(Quantile { quantile, value });
                }
            }
            metric_type::HISTOGRAM | metric_type::GAUGE_HISTOGRAM => {
                let h = m.histogram.get_or_insert_with(Histogram::default);
                if is_histogram_count {
                    let u = go_f64_to_u64(value);
                    if value == u as f64 {
                        h.sample_count = Some(u);
                    } else {
                        if value < 0.0 {
                            self.parse_error(format!("negative count for histogram {}", go_quote(&name)));
                            return None;
                        }
                        h.sample_count_float = Some(value);
                    }
                } else if is_histogram_sum {
                    h.sample_sum = Some(value);
                } else if !bucket.is_nan() {
                    let mut b = Bucket { upper_bound: Some(bucket), ..Default::default() };
                    let u = go_f64_to_u64(value);
                    if value == u as f64 {
                        b.cumulative_count = Some(u);
                    } else {
                        if value < 0.0 {
                            self.parse_error(format!("negative bucket population for histogram {}", go_quote(&name)));
                            return None;
                        }
                        b.cumulative_count_float = Some(value);
                    }
                    h.bucket.push(b);
                }
            }
            _ => self.err = Some(Err::Other(format!("unexpected type for metric name {}", go_quote(&name)))),
        }
        if self.current_byte == b'\n' {
            return Some(State::StartOfLine);
        }
        Some(State::StartTimestamp)
    }

    /// Go `startTimestamp`.
    fn start_timestamp(&mut self) -> Option<State> {
        self.skip_blank_tab();
        if self.failed() {
            return None;
        }
        self.read_token_until_whitespace();
        if self.failed() {
            return None;
        }
        let Ok(timestamp) = parse_int(&self.current_token) else {
            self.parse_error(format!("expected integer as timestamp, got {}", quote_bytes(&self.current_token)));
            return None;
        };
        self.metric_mut().timestamp_ms = Some(timestamp);
        self.read_token_until_newline(false);
        if self.failed() {
            return None;
        }
        if !self.current_token.is_empty() {
            self.parse_error(format!("spurious string after timestamp: {}", quote_bytes(&self.current_token)));
            return None;
        }
        Some(State::StartOfLine)
    }

    /// Go `readingHelp`.
    fn reading_help(&mut self) -> Option<State> {
        if self.mf().is_some_and(|m| m.help.is_some()) {
            self.parse_error(format!("second HELP line for metric name {}", go_quote(&self.mf_name())));
            return None;
        }
        // Rest of line is the docstring.
        self.read_token_until_newline(true);
        if self.failed() {
            return None;
        }
        let help = self.token_string();
        self.mf_mut().help = Some(help);
        Some(State::StartOfLine)
    }

    /// Go `readingType`.
    fn reading_type(&mut self) -> Option<State> {
        if self.mf().is_some_and(|m| m.r#type.is_some()) {
            self.parse_error(format!(
                "second TYPE line for metric name {}, or TYPE reported after samples",
                go_quote(&self.mf_name())
            ));
            return None;
        }
        // Rest of line is the type.
        self.read_token_until_newline(false);
        if self.failed() {
            return None;
        }
        // Tolerate any combination of upper and lower case, and "gauge_histogram".
        let typ = go_to_upper(&self.current_token);
        let metric_type = match std::str::from_utf8(&typ).ok().and_then(metric_type::value) {
            Some(t) => t,
            // Also tolerate "gaugehistogram" (used by OpenMetrics).
            None if typ == b"GAUGEHISTOGRAM" => metric_type::GAUGE_HISTOGRAM,
            None => {
                self.parse_error(format!("unknown metric type {}", quote_bytes(&self.current_token)));
                return None;
            }
        };
        self.mf_mut().r#type = Some(metric_type);
        Some(State::StartOfLine)
    }

    /// Go `parseError`.
    fn parse_error(&mut self, msg: String) {
        self.err = Some(Err::Parse { line: self.line_count, msg });
    }

    /// Go `skipBlankTab`.
    fn skip_blank_tab(&mut self) {
        while self.next_byte() && is_blank_or_tab(self.current_byte) {}
    }

    /// Go `skipBlankTabIfCurrentBlankTab`.
    fn skip_blank_tab_if_current_blank_tab(&mut self) {
        if is_blank_or_tab(self.current_byte) {
            self.skip_blank_tab();
        }
    }

    /// Go `readTokenUntilWhitespace`.
    fn read_token_until_whitespace(&mut self) {
        self.current_token.clear();
        while !self.failed() && !is_blank_or_tab(self.current_byte) && self.current_byte != b'\n' {
            self.current_token.push(self.current_byte);
            self.next_byte();
        }
    }

    /// Go `readTokenUntilNewline`: with `recognize_escape_sequence`, `\\`, `\n` and `\"` are unescaped and other
    /// escape sequences are an error.
    fn read_token_until_newline(&mut self, recognize_escape_sequence: bool) {
        self.current_token.clear();
        let mut escaped = false;
        while !self.failed() {
            if recognize_escape_sequence && escaped {
                match self.current_byte {
                    b'\\' => self.current_token.push(b'\\'),
                    b'n' => self.current_token.push(b'\n'),
                    b'"' => self.current_token.push(b'"'),
                    c => {
                        self.parse_error(format!("invalid escape sequence '\\{}'", c as char));
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
            self.next_byte();
        }
    }

    /// Go `readTokenAsMetricName` and `readTokenAsLabelName`: a plain or quoted (UTF-8, escaped) name.
    fn read_token_as_name(&mut self, label: bool) {
        self.current_token.clear();
        let mut quoted = false;
        let mut escaped = false;
        let valid_start = if label { is_valid_label_name_start } else { is_valid_metric_name_start };
        if !valid_start(self.current_byte) {
            return;
        }
        while !self.failed() {
            if escaped {
                match self.current_byte {
                    b'\\' => self.current_token.push(b'\\'),
                    b'n' => self.current_token.push(b'\n'),
                    b'"' => self.current_token.push(b'"'),
                    c => {
                        self.parse_error(format!("invalid escape sequence '\\{}'", c as char));
                        return;
                    }
                }
                escaped = false;
            } else {
                match self.current_byte {
                    b'"' => {
                        quoted = !quoted;
                        if !quoted {
                            self.next_byte();
                            return;
                        }
                    }
                    b'\n' => {
                        let what = if label { "label name" } else { "metric name" };
                        self.parse_error(format!("{what} {} contains unescaped new-line", quote_bytes(&self.current_token)));
                        return;
                    }
                    b'\\' => escaped = true,
                    c => self.current_token.push(c),
                }
            }
            self.next_byte();
            let (valid, stop) = if label {
                (is_valid_label_name_continuation(self.current_byte, quoted), b'=')
            } else {
                (is_valid_metric_name_continuation(self.current_byte, quoted), b' ')
            };
            if !valid || (!quoted && self.current_byte == stop) {
                return;
            }
        }
    }

    fn read_token_as_metric_name(&mut self) {
        self.read_token_as_name(false)
    }

    fn read_token_as_label_name(&mut self) {
        self.read_token_as_name(true)
    }

    /// Go `readTokenAsLabelValue`: starts with a new byte, ends at the closing quote.
    fn read_token_as_label_value(&mut self) {
        self.current_token.clear();
        let mut escaped = false;
        loop {
            if !self.next_byte() {
                return;
            }
            if escaped {
                match self.current_byte {
                    c @ (b'"' | b'\\') => self.current_token.push(c),
                    b'n' => self.current_token.push(b'\n'),
                    c => {
                        self.parse_error(format!("invalid escape sequence '\\{}'", c as char));
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
                    self.parse_error(format!("label value {} contains unescaped new-line", quote_bytes(&self.current_token)));
                    return;
                }
                b'\\' => escaped = true,
                c => self.current_token.push(c),
            }
        }
    }

    /// Go `setOrCreateCurrentMF`.
    fn set_or_create_current_mf(&mut self) {
        self.current_is_summary_count = false;
        self.current_is_summary_sum = false;
        self.current_is_histogram_count = false;
        self.current_is_histogram_sum = false;
        let valid = std::str::from_utf8(&self.current_token).is_ok_and(|n| self.scheme.is_valid_metric_name(n));
        let name = self.token_string();
        if !valid {
            self.parse_error(format!("invalid metric name {}", quote_bytes(&self.current_token)));
            return;
        }
        if self.families.contains_key(&name) {
            self.current_mf = Some(name);
            return;
        }
        // Try out if this is a _sum or _count for a summary/histogram.
        let summary_name = summary_metric_name(&name);
        if let Some(mf) = self.families.get(summary_name)
            && mf.get_type() == metric_type::SUMMARY
        {
            self.current_is_summary_count = is_count(&name);
            self.current_is_summary_sum = is_sum(&name);
            self.current_mf = Some(summary_name.to_string());
            return;
        }
        let histogram_name = histogram_metric_name(&name);
        if let Some(mf) = self.families.get(histogram_name)
            && (mf.get_type() == metric_type::HISTOGRAM || mf.get_type() == metric_type::GAUGE_HISTOGRAM)
        {
            self.current_is_histogram_count = is_count(&name);
            self.current_is_histogram_sum = is_sum(&name);
            self.current_mf = Some(histogram_name.to_string());
            return;
        }
        self.families.insert(name.clone(), MetricFamily { name: name.clone(), ..Default::default() });
        self.current_mf = Some(name);
    }
}

/// Go `parseFloat`: `strconv.ParseFloat` without hex floats and underscores.
fn parse_float(s: &[u8]) -> Result<f64, ()> {
    if s.iter().any(|&c| c == b'p' || c == b'P' || c == b'_') {
        return Err(());
    }
    go_parse_float(s).map_err(|_| ())
}

/// Go `uint64(f)` on amd64.
fn go_f64_to_u64(f: f64) -> u64 {
    const TWO63: f64 = 9_223_372_036_854_775_808.0;
    if f < TWO63 { go_f64_to_i64(f) as u64 } else { (go_f64_to_i64(f - TWO63) as u64) ^ (1u64 << 63) }
}

/// Go `strings.ToUpper`: maps each rune to its single upper case rune (`unicode.ToUpper`); invalid UTF-8
/// becomes U+FFFD.
fn go_to_upper(b: &[u8]) -> Vec<u8> {
    if b.is_ascii() {
        return b.to_ascii_uppercase();
    }
    let mut out = String::with_capacity(b.len());
    for c in String::from_utf8_lossy(b).chars() {
        let mut up = c.to_uppercase();
        match (up.next(), up.next()) {
            (Some(u), None) => out.push(u),
            // Go's unicode.ToUpper has no multi-rune mappings; such runes stay as they are.
            _ => out.push(c),
        }
    }
    out.into_bytes()
}

fn go_quote(s: &str) -> String {
    model::unicode::quote(s)
}

/// Go `%q` of a byte: a single-quoted character literal (`strconv.QuoteRune(rune(b))`).
fn quote_byte(b: u8) -> String {
    let c = b as char;
    match c {
        '\'' => "'\\''".to_string(),
        '"' => "'\"'".to_string(),
        _ => {
            let q = model::unicode::quote(c.encode_utf8(&mut [0u8; 4]));
            format!("'{}'", &q[1..q.len() - 1])
        }
    }
}

fn is_valid_label_name_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b == b'"'
}

/// Go `isValidLabelNameContinuation`. Go's `utf8.ValidString(string(b))` converts the byte to a rune, which is
/// always valid, so any byte continues a quoted name.
fn is_valid_label_name_continuation(b: u8, quoted: bool) -> bool {
    is_valid_label_name_start(b) || b.is_ascii_digit() || quoted
}

fn is_valid_metric_name_start(b: u8) -> bool {
    is_valid_label_name_start(b) || b == b':'
}

fn is_valid_metric_name_continuation(b: u8, quoted: bool) -> bool {
    is_valid_label_name_continuation(b, quoted) || b == b':'
}

fn is_blank_or_tab(b: u8) -> bool {
    b == b' ' || b == b'\t'
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
