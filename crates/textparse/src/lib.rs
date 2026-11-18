//! Port of Prometheus `model/textparse`: exposition format parsers (Prometheus text 0.0.4/1.0.0, OpenMetrics
//! 0.0.1/1.0.0, OpenMetrics 2.0, protobuf) behind one [`Parser`] trait, the NHCB conversion wrapper, and the
//! content-type negotiation of [`new`].
//!
//! Scrape use, without allocation per sample:
//!
//! ```ignore
//! let (p, warn) = textparse::new(body, content_type, &opts);
//! let mut p = p.ok_or(warn)?;              // None: the scrape fails with `warn`.
//! let mut lset = Labels::default();         // reused: `labels()` overwrites its buffer in place
//! loop {
//!     match p.next() {
//!         Err(e) if e.is_eof() => break,
//!         Err(e) => return Err(e),
//!         Ok(Entry::Series) => { let (series, ts, v) = p.series(); /* cache lookup by `series` bytes */ }
//!         Ok(Entry::Histogram) => { let (series, ts, h, fh) = p.histogram(); }
//!         Ok(_) => {}
//!     }
//! }
//! ```

pub mod client_pb;
pub mod convertnhcb;
pub mod mime;
mod nhcbparse;
mod openmetrics2lex;
mod openmetrics2parse;
mod openmetricsparse;
mod promparse;
mod protobufparse;
pub mod strconv;
pub mod testing;
mod token;

use std::fmt;

use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram};
use model::labels::Labels;
use model::metadata::MetricType;

pub use nhcbparse::NHCBParser;
pub use openmetrics2parse::OpenMetrics2Parser;
pub use openmetricsparse::{OpenMetricsOptions, OpenMetricsParser};
pub use promparse::PromParser;
pub use protobufparse::ProtobufParser;

/// The type of a parsed entry (Go `textparse.Entry`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum Entry {
    Invalid = -1,
    Type = 0,
    Help = 1,
    /// A series with a simple float64 as value.
    Series = 2,
    Comment = 3,
    Unit = 4,
    /// A series with a native histogram as a value.
    Histogram = 5,
}

/// A parser error. `Eof` is Go's `io.EOF` (normal end of input); `Msg` carries Go's exact error text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Eof,
    Msg(String),
}

impl Error {
    pub fn is_eof(&self) -> bool {
        matches!(self, Error::Eof)
    }

    pub(crate) fn msg(s: impl Into<String>) -> Error {
        Error::Msg(s.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Eof => f.write_str("EOF"),
            Error::Msg(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for Error {}

/// Which concrete parser a `dyn Parser` is (Go type assertions in tests and callers).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParserKind {
    Prom,
    OpenMetrics,
    OpenMetrics2,
    Protobuf,
    Nhcb,
}

/// Parses samples from a byte slice in one exposition format (Go `textparse.Parser`).
///
/// Returned slices borrow the input or the parser and are valid until the next call of `next`.
pub trait Parser {
    /// The bytes of a series with a float64 value, the timestamp if set, and the value of the current sample.
    fn series(&self) -> (&[u8], Option<i64>, f64);

    /// The bytes of a series with a native histogram value, the timestamp if set, and the histogram: an integer
    /// `Histogram` or a `FloatHistogram`, the other one is `None`.
    fn histogram(&mut self) -> (&[u8], Option<i64>, Option<&Histogram>, Option<&FloatHistogram>);

    /// The metric name and help text of the current help entry.
    fn help(&mut self) -> (&[u8], &[u8]);

    /// The metric name and type of the current type entry (Go `Type`).
    fn typ(&self) -> (&[u8], MetricType);

    /// The metric name and unit of the current unit entry.
    fn unit(&self) -> (&[u8], &[u8]);

    /// The text of the current comment entry.
    fn comment(&self) -> &[u8];

    /// Writes the labels of the current sample into `l` (its buffer is reused when possible). The values of the
    /// "le" labels of classic histograms and "quantile" labels of summaries follow OpenMetrics float formatting.
    fn labels(&mut self, l: &mut Labels);

    /// Writes the next exemplar of the current sample into `e`; false when there are no more exemplars. Like Go,
    /// fields that the exemplar does not set (for example the timestamp) keep their previous values.
    fn exemplar(&mut self, e: &mut Exemplar) -> bool;

    /// The start timestamp in milliseconds of the current sample, 0 if unknown.
    fn start_timestamp(&mut self) -> i64;

    /// Advances to the next entry. Returns `Err(Error::Eof)` at the end of the input.
    fn next(&mut self) -> Result<Entry, Error>;

    /// The concrete parser type.
    fn kind(&self) -> ParserKind;
}

/// The value of the Content-Type version parameter that selects the OpenMetrics 2.0 parser.
pub const OPEN_METRICS2_VERSION: &str = "2.0.0";

/// Options for [`new`] (Go `textparse.ParserOptions`).
#[derive(Clone, Debug, Default)]
pub struct ParserOptions {
    /// Add `__type__` and `__unit__` labels.
    pub enable_type_and_unit_labels: bool,
    /// Ignore all native histogram parts (classic histograms can still be converted to NHCB).
    pub ignore_native_histograms: bool,
    /// Convert classic histograms to native histograms with custom buckets.
    pub convert_classic_histograms_to_nhcb: bool,
    /// Also output the classic series of a histogram that is also native or converted to NHCB.
    pub keep_classic_on_classic_and_native_histograms: bool,
    /// Skip `_created` series (OpenMetrics only); they are then only used for start timestamps.
    pub open_metrics_skip_st_series: bool,
    /// Enable the OpenMetrics 2.0 parser.
    pub enable_open_metrics2: bool,
    /// Media type to use when the Content-Type is missing, invalid or unsupported.
    pub fallback_content_type: String,
}

fn go_q(s: &str) -> String {
    model::unicode::quote(s)
}

/// Go `extractMediaType`: returns the media type and version to use, and an error to report (also when a
/// fallback type is used).
pub fn extract_media_type(
    content_type: &str,
    fallback_type: &str,
    enable_open_metrics2: bool,
) -> (String, String, Option<Error>) {
    if content_type.is_empty() {
        if fallback_type.is_empty() {
            return (
                String::new(),
                String::new(),
                Some(Error::msg(
                    "non-compliant scrape target sending blank Content-Type and no fallback_scrape_protocol specified for target",
                )),
            );
        }
        return (
            fallback_type.to_string(),
            String::new(),
            Some(Error::msg(format!(
                "non-compliant scrape target sending blank Content-Type, using fallback_scrape_protocol {}",
                go_q(fallback_type)
            ))),
        );
    }
    let (media_type, params) = match mime::parse_media_type(content_type) {
        Ok(v) => v,
        Err((_, e)) => {
            if fallback_type.is_empty() {
                return (
                    String::new(),
                    String::new(),
                    Some(Error::msg(format!(
                        "cannot parse Content-Type {} and no fallback_scrape_protocol for target\n{e}",
                        go_q(content_type)
                    ))),
                );
            }
            return (
                fallback_type.to_string(),
                String::new(),
                Some(Error::msg(format!(
                    "could not parse received Content-Type {}, using fallback_scrape_protocol {}\n{e}",
                    go_q(content_type),
                    go_q(fallback_type)
                ))),
            );
        }
    };
    let version = params.get("version").cloned().unwrap_or_default();
    if media_type == "application/openmetrics-text" && version == OPEN_METRICS2_VERSION && !enable_open_metrics2 {
        if fallback_type.is_empty() {
            return (
                String::new(),
                String::new(),
                Some(Error::msg(format!(
                    "received OpenMetrics 2.0 Content-Type {}, but the openmetrics2 feature flag is not enabled and no fallback_scrape_protocol specified for target",
                    go_q(content_type)
                ))),
            );
        }
        return (
            fallback_type.to_string(),
            String::new(),
            Some(Error::msg(format!(
                "received OpenMetrics 2.0 Content-Type {}, but the openmetrics2 feature flag is not enabled, using fallback_scrape_protocol {}",
                go_q(content_type),
                go_q(fallback_type)
            ))),
        );
    }
    match media_type.as_str() {
        "application/openmetrics-text" | "application/vnd.google.protobuf" | "text/plain" => {
            return (media_type, version, None);
        }
        _ => {}
    }
    if fallback_type.is_empty() {
        return (
            String::new(),
            String::new(),
            Some(Error::msg(format!(
                "received unsupported Content-Type {} and no fallback_scrape_protocol specified for target",
                go_q(content_type)
            ))),
        );
    }
    (
        fallback_type.to_string(),
        String::new(),
        Some(Error::msg(format!(
            "received unsupported Content-Type {}, using fallback_scrape_protocol {}",
            go_q(content_type),
            go_q(fallback_type)
        ))),
    )
}

/// Go `textparse.New`: a parser for `b` chosen by `content_type`. The parser is `None` when the scrape must
/// fail; the error may also be set together with a parser (a warning, for example when the fallback was used).
pub fn new<'a>(b: &'a [u8], content_type: &str, opts: &ParserOptions) -> (Option<Box<dyn Parser + 'a>>, Option<Error>) {
    let (media_type, version, err) =
        extract_media_type(content_type, &opts.fallback_content_type, opts.enable_open_metrics2);
    let base: Box<dyn Parser + 'a> = match media_type.as_str() {
        "application/openmetrics-text" => {
            if version == OPEN_METRICS2_VERSION {
                return (Some(Box::new(OpenMetrics2Parser::new(b, opts))), err);
            }
            Box::new(OpenMetricsParser::new(
                b,
                OpenMetricsOptions {
                    skip_st_series: opts.open_metrics_skip_st_series,
                    enable_type_and_unit_labels: opts.enable_type_and_unit_labels,
                },
            ))
        }
        "application/vnd.google.protobuf" => {
            return (
                Some(Box::new(ProtobufParser::new(
                    b,
                    opts.ignore_native_histograms,
                    opts.keep_classic_on_classic_and_native_histograms,
                    opts.convert_classic_histograms_to_nhcb,
                    opts.enable_type_and_unit_labels,
                ))),
                err,
            );
        }
        "text/plain" => Box::new(PromParser::new(b, opts.enable_type_and_unit_labels)),
        _ => return (None, err),
    };
    if opts.convert_classic_histograms_to_nhcb {
        return (
            Some(Box::new(NHCBParser::new(
                base,
                opts.keep_classic_on_classic_and_native_histograms,
                opts.open_metrics_skip_st_series,
            ))),
            err,
        );
    }
    (Some(base), err)
}

/// Go `normalizeFloatsInLabelValues`: rewrites `le` of histograms and `quantile` of summaries in OpenMetrics float
/// format into `scratch`; returns the value to use.
#[inline]
pub(crate) fn normalize_floats_in_label_values<'s>(
    t: MetricType,
    l: &str,
    v: &'s str,
    scratch: &'s mut String,
) -> &'s str {
    if (t == MetricType::Summary && l == "quantile") || (t == MetricType::Histogram && l == "le") {
        if let Ok(f) = strconv::parse_float(v.as_bytes()) {
            scratch.clear();
            model::labels::append_open_metrics_float(scratch, f);
            return scratch.as_str();
        }
    }
    v
}

/// Go `lvalReplacer` (`\"`, `\\`, `\n`) applied when `s` has a backslash; otherwise `s` itself.
#[inline]
pub(crate) fn unreplace<'s>(s: &'s [u8], scratch: &'s mut Vec<u8>) -> &'s [u8] {
    if memchr(b'\\', s).is_none() {
        return s;
    }
    replace_escapes(s, scratch, true);
    scratch.as_slice()
}

/// Go `strings.NewReplacer` semantics for the escape replacers: scans left to right, replacing `\\`, `\n` and
/// (when `quote`) `\"`; other bytes are copied.
pub(crate) fn replace_escapes(s: &[u8], out: &mut Vec<u8>, quote: bool) {
    out.clear();
    let mut i = 0;
    while i < s.len() {
        let c = s[i];
        if c == b'\\' && i + 1 < s.len() {
            match s[i + 1] {
                b'\\' => {
                    out.push(b'\\');
                    i += 2;
                    continue;
                }
                b'n' => {
                    out.push(b'\n');
                    i += 2;
                    continue;
                }
                b'"' if quote => {
                    out.push(b'"');
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }
        out.push(c);
        i += 1;
    }
}

#[inline]
pub(crate) fn memchr(c: u8, s: &[u8]) -> Option<usize> {
    s.iter().position(|&b| b == c)
}
