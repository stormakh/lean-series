//! Port of `openmetrics2parse.go`: the OpenMetrics 2.0 text format parser. The lexer (`openmetrics2lex.l`) is in
//! `openmetrics2lex.rs`.
//!
//! Composite values (summaries and classic histograms) are exploded into flat series that are served one by one
//! from a pending queue; their series bytes are built in a reused buffer, and labels are only built when
//! `labels` is called. Native histograms are built into reused `Histogram`/`FloatHistogram` values.

use std::cell::RefCell;

use model::exemplar::Exemplar;
use model::histogram::{CounterResetHint, FloatHistogram, Histogram, Span};
use model::labels::{append_open_metrics_float, Labels, ScratchBuilder};
use model::metadata::MetricType;

use crate::convertnhcb::TempHistogram;
use crate::openmetrics2lex::Om2Lexer;
use crate::promparse::{build_text_labels, lossy, LabelScratch};
use crate::strconv::{append_quote_bytes, format_f, format_g, go_f64_to_i64, parse_float, NumErrKind, NumError};
use crate::token::*;
use crate::{Entry, Error, Parser, ParserKind, ParserOptions};

const NONE: usize = usize::MAX;
const SEP: u8 = 0xff;

/// Go `%q` of a byte string.
fn q(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() + 2);
    append_quote_bytes(&mut s, b);
    s
}

/// One exploded flat series of a composite value (Go `pendingEntry`).
#[derive(Clone, Copy, Debug)]
pub(crate) struct PendingEntry {
    series_start: usize,
    series_end: usize,
    name_len: usize,
    inject_key: &'static str,
    inject_val: f64,
    val: f64,
}

/// The offsets and values of one exemplar (Go `om2Exemplar`).
#[derive(Clone, Copy, Debug)]
struct Om2Exemplar {
    offsets_start: usize,
    offsets_end: usize,
    val: f64,
    ts: i64,
}

type CompositeScratch = (Vec<PendingEntry>, Vec<u8>);

thread_local! {
    /// Go `om2CompositeScratchPool`: the pending queue and series buffer are handed back on EOF and reused by the
    /// next parser on the same thread, because the scrape loop creates a parser per scrape.
    static COMPOSITE_SCRATCH_POOL: RefCell<Vec<CompositeScratch>> = const { RefCell::new(Vec::new()) };
}

/// At most this many scratch buffers are kept per thread.
const POOL_MAX: usize = 4;

// Composite field indices (ordered as `Token::CompCount..=Token::CompQuantile`).
const F_COUNT: usize = 0;
const F_GCOUNT: usize = 1;
const F_SUM: usize = 2;
const F_GSUM: usize = 3;
const F_SCHEMA: usize = 4;
const F_ZERO_THRESHOLD: usize = 5;
const F_ZERO_COUNT: usize = 6;
const F_NEGATIVE_SPANS: usize = 7;
const F_NEGATIVE_BUCKETS: usize = 8;
const F_POSITIVE_SPANS: usize = 9;
const F_POSITIVE_BUCKETS: usize = 10;
const F_BUCKET: usize = 11;
const F_QUANTILE: usize = 12;
const NUM_COMP_FIELDS: usize = 13;

const HISTOGRAM_COMMON_MASK: u16 = (1 << F_SCHEMA)
    | (1 << F_ZERO_THRESHOLD)
    | (1 << F_ZERO_COUNT)
    | (1 << F_NEGATIVE_SPANS)
    | (1 << F_NEGATIVE_BUCKETS)
    | (1 << F_POSITIVE_SPANS)
    | (1 << F_POSITIVE_BUCKETS)
    | (1 << F_BUCKET);
const HISTOGRAM_ALLOWED_MASK: u16 = HISTOGRAM_COMMON_MASK | (1 << F_COUNT) | (1 << F_SUM);
const GAUGE_HISTOGRAM_ALLOWED_MASK: u16 = HISTOGRAM_COMMON_MASK | (1 << F_GCOUNT) | (1 << F_GSUM);
const SUMMARY_ALLOWED_MASK: u16 = (1 << F_COUNT) | (1 << F_SUM) | (1 << F_QUANTILE);

fn comp_field_index(t: Token) -> Option<usize> {
    Some(match t {
        Token::CompCount => F_COUNT,
        Token::CompGCount => F_GCOUNT,
        Token::CompSum => F_SUM,
        Token::CompGSum => F_GSUM,
        Token::CompSchema => F_SCHEMA,
        Token::CompZeroThreshold => F_ZERO_THRESHOLD,
        Token::CompZeroCount => F_ZERO_COUNT,
        Token::CompNegSpans => F_NEGATIVE_SPANS,
        Token::CompNegBuckets => F_NEGATIVE_BUCKETS,
        Token::CompPosSpans => F_POSITIVE_SPANS,
        Token::CompPosBuckets => F_POSITIVE_BUCKETS,
        Token::CompBucket => F_BUCKET,
        Token::CompQuantile => F_QUANTILE,
        _ => return None,
    })
}

/// The raw values of the fields of a composite value (Go `compositeFields`), as byte ranges of the input.
#[derive(Clone, Copy, Default)]
struct CompositeFields {
    fields: [(usize, usize); NUM_COMP_FIELDS],
    seen: u16,
}

impl CompositeFields {
    fn has(&self, field: usize) -> bool {
        self.seen & (1 << field) != 0
    }

    /// The raw bytes of `field`, empty when it is absent (Go's nil slice).
    fn raw<'b>(&self, b: &'b [u8], field: usize) -> &'b [u8] {
        let (s, e) = self.fields[field];
        &b[s..e]
    }

    fn get<'b>(&self, b: &'b [u8], field: usize) -> Option<&'b [u8]> {
        if self.has(field) {
            Some(self.raw(b, field))
        } else {
            None
        }
    }
}

/// Parses samples from a byte slice in the OpenMetrics 2.0 text exposition format (Go `openMetrics2Parser`).
/// Specification: <https://prometheus.io/docs/specs/om/open_metrics_spec_2_0/>
pub struct OpenMetrics2Parser<'a> {
    b: &'a [u8],
    l: Om2Lexer,
    builder: ScratchBuilder,

    // Metadata for the current metric family. Reset by `reset_on_family_change`.
    mtype: MetricType,
    unit: Vec<u8>,
    text: (usize, usize),
    help_buf: Vec<u8>,

    /// The current sample's series bytes in the input.
    series: (usize, usize),
    /// The last metric name seen by `reset_on_family_change` (Go `curFamilyName`).
    cur_family: (usize, usize),

    /// `[nameStart, nameEnd, k1Start, k1End, v1Start, v1End, ...]`, absolute offsets into the input.
    offsets: Vec<usize>,
    start: usize,

    val: f64,
    has_ts: bool,
    ts: i64,

    st: i64,
    has_st: bool,

    h: Histogram,
    fh: FloatHistogram,
    has_h: bool,
    has_fh: bool,

    exemplars: Vec<Om2Exemplar>,
    e_offsets: Vec<usize>,
    exemplar_idx: usize,

    pending: Vec<PendingEntry>,
    pending_idx: usize,
    series_buf: Vec<u8>,
    /// The parser holds a pooled composite scratch (Go `scratch != nil`).
    scratch: bool,

    enable_type_and_unit_labels: bool,
    ignore_native_histograms: bool,
    keep_classic_on_native_hist: bool,
    convert_classic_hist_to_nhcb: bool,
    tmp_nhcb: TempHistogram,

    sc: LabelScratch,
    float_buf: String,
}

impl<'a> OpenMetrics2Parser<'a> {
    /// Go `NewOpenMetrics2Parser`.
    pub fn new(b: &'a [u8], opts: &ParserOptions) -> OpenMetrics2Parser<'a> {
        OpenMetrics2Parser {
            b,
            l: Om2Lexer::default(),
            builder: ScratchBuilder::new(16),
            mtype: MetricType::Empty,
            unit: Vec::new(),
            text: (0, 0),
            help_buf: Vec::new(),
            series: (0, 0),
            cur_family: (0, 0),
            offsets: Vec::with_capacity(32),
            start: 0,
            val: 0.0,
            has_ts: false,
            ts: 0,
            st: 0,
            has_st: false,
            h: Histogram::default(),
            fh: FloatHistogram::default(),
            has_h: false,
            has_fh: false,
            exemplars: Vec::new(),
            e_offsets: Vec::new(),
            exemplar_idx: 0,
            pending: Vec::new(),
            pending_idx: 0,
            series_buf: Vec::new(),
            scratch: false,
            enable_type_and_unit_labels: opts.enable_type_and_unit_labels,
            ignore_native_histograms: opts.ignore_native_histograms,
            keep_classic_on_native_hist: opts.keep_classic_on_classic_and_native_histograms,
            convert_classic_hist_to_nhcb: opts.convert_classic_histograms_to_nhcb,
            tmp_nhcb: if opts.convert_classic_histograms_to_nhcb { TempHistogram::new() } else { TempHistogram::default() },
            sc: LabelScratch::default(),
            float_buf: String::new(),
        }
    }

    /// True when the pooled composite buffers were handed back (after EOF) and the parser holds none of them
    /// (Go test check of `scratch`, `pending` and `seriesBuf` being nil).
    #[doc(hidden)]
    pub fn composite_scratch_released(&self) -> bool {
        !self.scratch && self.pending.capacity() == 0 && self.series_buf.capacity() == 0
    }

    #[inline]
    fn next_token(&mut self) -> Token {
        self.l.lex(self.b)
    }

    /// The bytes of the current line so far (Go `p.l.b[p.start:p.l.i]`).
    #[inline]
    fn line(&self) -> &'a [u8] {
        &self.b[self.start..self.l.i]
    }

    /// The bytes of the current token (Go `p.l.buf()`).
    #[inline]
    fn buf(&self) -> &'a [u8] {
        &self.b[self.l.start..self.l.i]
    }

    fn parse_error(&self, exp: &str, got: Token) -> Error {
        let e = self.b.len().min(self.l.i + 1);
        if got == Token::Invalid && crate::memchr(b'\r', &self.b[self.l.start.min(e)..e]).is_some() {
            return parse_error(self.b, self.l.start, self.l.i, self.start, "unexpected carriage return", got);
        }
        parse_error(self.b, self.l.start, self.l.i, self.start, exp, got)
    }

    /// Go `parseFloat` (rejects `p`, `P` and `_`) with the error wrapped as `"%w<ctx>%q"` of the current line.
    fn text_float(&self, s: &[u8], ctx: &str) -> Result<f64, Error> {
        let r = if s.iter().any(|&c| c == b'p' || c == b'P' || c == b'_') {
            Err("unsupported character in float".to_string())
        } else {
            parse_float(s).map_err(|e| e.num_error(s).to_string())
        };
        r.map_err(|mut m| {
            m.push_str(ctx);
            append_quote_bytes(&mut m, self.line());
            Error::Msg(m)
        })
    }

    fn family_name(&self) -> &'a [u8] {
        &self.b[self.cur_family.0..self.cur_family.1]
    }

    /// Go `resetOnFamilyChange`.
    fn reset_on_family_change(&mut self, s: usize, e: usize) {
        if self.b[s..e] != *self.family_name() {
            self.mtype = MetricType::Unknown;
            self.unit.clear();
            self.cur_family = (s, e);
        }
    }

    /// Go `hasFamilyNameLabel`.
    fn has_family_name_label(&self) -> bool {
        let fam = self.family_name();
        let mut i = 2;
        while i < self.offsets.len() {
            if &self.b[self.offsets[i]..self.offsets[i + 1]] == fam {
                return true;
            }
            i += 4;
        }
        false
    }

    fn acquire_composite_scratch(&mut self) {
        if self.scratch {
            return;
        }
        self.scratch = true;
        match COMPOSITE_SCRATCH_POOL.with(|p| p.borrow_mut().pop()) {
            Some((pending, series_buf)) => {
                self.pending = pending;
                self.series_buf = series_buf;
            }
            None => {
                self.pending = Vec::with_capacity(16);
                self.series_buf = Vec::with_capacity(1024);
            }
        }
    }

    fn release_composite_scratch(&mut self) {
        if !self.scratch {
            return;
        }
        let mut pending = std::mem::take(&mut self.pending);
        let mut series_buf = std::mem::take(&mut self.series_buf);
        pending.clear();
        series_buf.clear();
        COMPOSITE_SCRATCH_POOL.with(|p| {
            let mut p = p.borrow_mut();
            if p.len() < POOL_MAX {
                p.push((pending, series_buf));
            }
        });
        self.scratch = false;
        self.pending_idx = 0;
    }

    /// Go `parseSeriesEndOfLine`.
    fn parse_series_end_of_line(&mut self, t: Token) -> Result<Entry, Error> {
        if self.offsets[0] == NONE {
            return Err(Error::Msg(format!("metric name not set while parsing: {}", q(self.line()))));
        }
        if t == Token::CompOpen {
            return self.parse_composite_value();
        }
        if t != Token::Value {
            return Err(self.parse_error("expected value after metric", t));
        }
        let mut raw = self.buf();
        if raw.len() > 1 && raw[0] == b' ' {
            raw = &raw[1..];
        }
        self.val = self.text_float(raw, " while parsing: ")?;
        if self.val.is_nan() {
            if self.mtype == MetricType::Counter {
                return Err(Error::Msg(format!(
                    "counter sample value must not be NaN while parsing: {}",
                    q(self.line())
                )));
            }
            self.val = model::value::normal_nan();
        }
        match self.mtype {
            MetricType::Histogram | MetricType::GaugeHistogram | MetricType::Summary => {
                return Err(Error::Msg(format!(
                    "composite value required for metric type {} while parsing: {}",
                    q(self.mtype.as_str().as_bytes()),
                    q(self.line())
                )));
            }
            MetricType::Info => {
                if self.val != 1.0 {
                    return Err(Error::Msg(format!(
                        "info sample value must be 1, got {} while parsing: {}",
                        format_g(self.val),
                        q(self.line())
                    )));
                }
            }
            MetricType::StateSet => {
                if self.val != 0.0 && self.val != 1.0 {
                    return Err(Error::Msg(format!(
                        "stateset sample value must be 0 or 1, got {} while parsing: {}",
                        format_g(self.val),
                        q(self.line())
                    )));
                }
                if !self.has_family_name_label() {
                    return Err(Error::Msg(format!(
                        "stateset sample must have a label matching the metric family name {} while parsing: {}",
                        q(self.family_name()),
                        q(self.line())
                    )));
                }
            }
            _ => {}
        }
        // Fast path: plain sample lines end immediately with a linebreak.
        let t2 = self.next_token();
        if t2 == Token::Linebreak {
            return Ok(Entry::Series);
        }
        self.parse_after_value(t2)?;
        Ok(Entry::Series)
    }

    /// Go `parseAfterValue`: `[tTimestamp] [tStartTimestamp] [*tComment exemplar] tLinebreak`.
    fn parse_after_value(&mut self, mut t: Token) -> Result<(), Error> {
        loop {
            match t {
                Token::Eof => return Err(Error::msg("data does not end with # EOF")),
                Token::Linebreak => return Ok(()),
                Token::Timestamp => {
                    if self.has_ts {
                        return Err(Error::Msg(format!("duplicate timestamp: {}", q(self.line()))));
                    }
                    if self.has_st {
                        return Err(Error::Msg(format!(
                            "timestamp must precede start timestamp: {}",
                            q(self.line())
                        )));
                    }
                    self.has_ts = true;
                    let ts = self.text_float(&self.buf()[1..], " while parsing: ")?;
                    if ts.is_nan() || ts.is_infinite() {
                        return Err(Error::Msg(format!("invalid timestamp {}", format_f(ts))));
                    }
                    self.ts = go_f64_to_i64(ts * 1000.0);
                }
                Token::StartTimestamp => {
                    if self.has_st {
                        return Err(Error::Msg(format!("duplicate start timestamp: {}", q(self.line()))));
                    }
                    // buf is " st@<float>"; skip the leading " st@" (4 bytes).
                    let raw = self.buf();
                    if raw.len() < 4 {
                        return Err(Error::Msg(format!("invalid start timestamp token {}", q(raw))));
                    }
                    let st = self.text_float(&raw[4..], " while parsing start timestamp: ")?;
                    if st.is_nan() || st.is_infinite() {
                        return Err(Error::Msg(format!("invalid start timestamp {}", format_f(st))));
                    }
                    self.st = go_f64_to_i64(st * 1000.0);
                    self.has_st = true;
                }
                Token::Comment => return self.parse_exemplars(),
                _ => return Err(self.parse_error("unexpected token after value", t)),
            }
            t = self.next_token();
        }
    }

    /// Go `parseExemplars`: one or more exemplars up to and including the linebreak.
    fn parse_exemplars(&mut self) -> Result<(), Error> {
        while !self.parse_single_exemplar()? {}
        Ok(())
    }

    /// Go `parseSingleExemplar`: true when the linebreak was read, false when another exemplar follows.
    fn parse_single_exemplar(&mut self) -> Result<bool, Error> {
        let offsets_start = self.e_offsets.len();
        let mut e_offsets = std::mem::take(&mut self.e_offsets);
        let r = self.parse_lvals(&mut e_offsets, true);
        self.e_offsets = e_offsets;
        r?;
        let offsets_end = self.e_offsets.len();

        let t = self.next_token();
        if t != Token::Value {
            return Err(self.parse_error("expected exemplar value", t));
        }
        let mut val = self.text_float(&self.buf()[1..], " while parsing exemplar value: ")?;
        if val.is_nan() {
            val = model::value::normal_nan();
        }

        match self.next_token() {
            Token::Eof => Err(Error::msg("data does not end with # EOF")),
            Token::Timestamp => {
                let ts = self.text_float(&self.buf()[1..], " while parsing exemplar timestamp: ")?;
                if ts.is_nan() || ts.is_infinite() {
                    return Err(Error::Msg(format!("invalid exemplar timestamp {}", format_f(ts))));
                }
                self.exemplars.push(Om2Exemplar { offsets_start, offsets_end, val, ts: go_f64_to_i64(ts * 1000.0) });
                match self.next_token() {
                    Token::Eof => Err(Error::msg("data does not end with # EOF")),
                    Token::Linebreak => Ok(true),
                    Token::Comment => Ok(false),
                    t3 => Err(self.parse_error("expected end of line or next exemplar", t3)),
                }
            }
            t2 => Err(self.parse_error("expected exemplar timestamp", t2)),
        }
    }

    /// Go `parseLVals`: parses `{k="v",...}` and appends byte offsets to `offsets`.
    fn parse_lvals(&mut self, offsets: &mut Vec<usize>, is_exemplar: bool) -> Result<(), Error> {
        let mut t = self.next_token();
        let mut first = true;
        loop {
            let is_first = first;
            first = false;
            let mut cur_start = self.l.start;
            let mut cur_i = self.l.i;
            let is_qstring = match t {
                Token::BraceClose => return Ok(()),
                Token::LName => false,
                Token::QString => true,
                _ => return Err(self.parse_error("expected label name", t)),
            };

            t = self.next_token();
            if is_qstring {
                if t == Token::Comma || t == Token::BraceClose {
                    if is_exemplar {
                        return Err(self.parse_error("expected label name", t));
                    }
                    if !is_first {
                        return Err(Error::msg("metric name must be the first item in the label set"));
                    }
                    if offsets[0] != NONE || offsets[1] != NONE {
                        return Err(Error::Msg(format!("metric name already set while parsing: {}", q(self.line()))));
                    }
                    offsets[0] = cur_start + 1;
                    offsets[1] = cur_i - 1;
                    if offsets[0] == offsets[1] {
                        return Err(Error::msg("metric name must not be empty"));
                    }
                    let name = &self.b[offsets[0]..offsets[1]];
                    if std::str::from_utf8(name).is_err() {
                        return Err(Error::Msg(format!("invalid UTF-8 metric name: {}", q(name))));
                    }
                    if t == Token::BraceClose {
                        return Ok(());
                    }
                    t = self.next_token();
                    continue;
                }
                // Quoted label name: strip the quotes and check that it is non-empty UTF-8.
                cur_start += 1;
                cur_i -= 1;
                if cur_start == cur_i {
                    return Err(Error::msg("label name must not be empty"));
                }
                let name = &self.b[cur_start..cur_i];
                if std::str::from_utf8(name).is_err() {
                    return Err(Error::Msg(format!("invalid UTF-8 label name: {}", q(name))));
                }
            }
            offsets.push(cur_start);
            offsets.push(cur_i);

            if t != Token::Equal {
                return Err(self.parse_error("expected equal", t));
            }
            let t2 = self.next_token();
            if t2 != Token::LValue {
                return Err(self.parse_error("expected label value", t2));
            }
            let v = self.buf();
            if std::str::from_utf8(v).is_err() {
                return Err(Error::Msg(format!("invalid UTF-8 label value: {}", q(v))));
            }
            offsets.push(self.l.start + 1);
            offsets.push(self.l.i - 1);

            t = self.next_token();
            if t == Token::Comma {
                t = self.next_token();
            } else if t != Token::BraceClose {
                return Err(self.parse_error("expected comma or brace close", t));
            }
        }
    }

    /// Go `parseCompositeValue`.
    fn parse_composite_value(&mut self) -> Result<Entry, Error> {
        match self.mtype {
            MetricType::Histogram | MetricType::GaugeHistogram => self.parse_histogram_composite(),
            MetricType::Summary => self.parse_summary_composite(),
            _ => Err(Error::Msg(format!(
                "composite value not supported for metric type {} while parsing: {}",
                q(self.mtype.as_str().as_bytes()),
                q(self.line())
            ))),
        }
    }

    /// Go `parseCompositeFields`.
    fn parse_composite_fields(&mut self, allowed_mask: u16) -> Result<CompositeFields, Error> {
        let mut cf = CompositeFields::default();
        let mut t = self.next_token();
        if t == Token::CompClose {
            return Ok(cf);
        }
        loop {
            let idx = match comp_field_index(t) {
                Some(idx) if allowed_mask & (1 << idx) != 0 => idx,
                _ => return Err(self.parse_error("unknown composite field", t)),
            };
            if cf.has(idx) {
                return Err(self.parse_error("duplicate composite field", t));
            }
            let t2 = self.next_token();
            if t2 != Token::Value {
                return Err(self.parse_error("expected composite value", t2));
            }
            cf.seen |= 1 << idx;
            cf.fields[idx] = (self.l.start, self.l.i);

            t = self.next_token();
            match t {
                Token::Comma => t = self.next_token(),
                Token::CompClose => return Ok(cf),
                _ => return Err(self.parse_error("expected comma or brace close in composite value", t)),
            }
        }
    }

    /// Go `parseHistogramFields`.
    fn parse_histogram_fields(&mut self, is_gauge: bool) -> Result<CompositeFields, Error> {
        let (allowed_mask, count_field, sum_field, count_key, sum_key) = if is_gauge {
            (GAUGE_HISTOGRAM_ALLOWED_MASK, F_GCOUNT, F_GSUM, "gcount", "gsum")
        } else {
            (HISTOGRAM_ALLOWED_MASK, F_COUNT, F_SUM, "count", "sum")
        };
        let cf = self.parse_composite_fields(allowed_mask)?;
        if !cf.has(count_field) {
            return Err(Error::Msg(format!("missing required field: {count_key}")));
        }
        if !cf.has(sum_field) {
            return Err(Error::Msg(format!("missing required field: {sum_key}")));
        }
        if cf.has(F_SCHEMA) {
            if !cf.has(F_ZERO_THRESHOLD) {
                return Err(Error::msg("missing required field: zero_threshold"));
            }
            if !cf.has(F_ZERO_COUNT) {
                return Err(Error::msg("missing required field: zero_count"));
            }
        } else if !cf.has(F_BUCKET) {
            return Err(Error::msg("missing required field: bucket"));
        }
        Ok(cf)
    }

    /// Go `parseSummaryFields`.
    fn parse_summary_fields(&mut self) -> Result<CompositeFields, Error> {
        let cf = self.parse_composite_fields(SUMMARY_ALLOWED_MASK)?;
        if !cf.has(F_COUNT) {
            return Err(Error::msg("missing required field: count"));
        }
        if !cf.has(F_SUM) {
            return Err(Error::msg("missing required field: sum"));
        }
        if !cf.has(F_QUANTILE) {
            return Err(Error::msg("missing required field: quantile"));
        }
        Ok(cf)
    }

    /// Consumes the rest of a composite line (timestamp, start timestamp, exemplars).
    fn parse_composite_line_end(&mut self) -> Result<(), Error> {
        let t2 = self.next_token();
        if t2 != Token::Linebreak {
            self.parse_after_value(t2)?;
        }
        Ok(())
    }

    /// Go `parseHistogramComposite`: a native histogram (`Entry::Histogram`) or classic histogram series.
    fn parse_histogram_composite(&mut self) -> Result<Entry, Error> {
        let is_gauge = self.mtype == MetricType::GaugeHistogram;
        let cf = self.parse_histogram_fields(is_gauge)?;
        self.parse_composite_line_end()?;

        // schema is mandatory for native histograms and absent from classic ones.
        let is_native = cf.has(F_SCHEMA);

        if is_native && !self.ignore_native_histograms {
            self.build_native_histogram(&cf, is_gauge)
                .map_err(|e| Error::Msg(format!("error parsing native histogram composite: {e}")))?;
            if cf.has(F_BUCKET) && self.keep_classic_on_native_hist {
                self.build_classic_histogram_pending(&cf, false)
                    .map_err(|e| Error::Msg(format!("error parsing classic histogram composite: {e}")))?;
                self.pending_idx = 0;
            }
            return Ok(Entry::Histogram);
        }

        if self.convert_classic_hist_to_nhcb {
            self.build_nhcb_histogram(&cf, is_native)
                .map_err(|e| Error::Msg(format!("error parsing classic histogram composite: {e}")))?;
            if self.keep_classic_on_native_hist {
                self.build_classic_histogram_pending(&cf, is_native)
                    .map_err(|e| Error::Msg(format!("error parsing classic histogram composite: {e}")))?;
                self.pending_idx = 0;
            }
            return Ok(Entry::Histogram);
        }

        // Classic histogram (or native histogram with ignore_native_histograms): explode into flat series.
        self.build_classic_histogram_pending(&cf, is_native)
            .map_err(|e| Error::Msg(format!("error parsing classic histogram composite: {e}")))?;
        self.serve_pending()
    }

    /// Go `parseSummaryComposite`.
    fn parse_summary_composite(&mut self) -> Result<Entry, Error> {
        let cf = self.parse_summary_fields()?;
        self.parse_composite_line_end()?;
        self.build_summary_pending(&cf).map_err(|e| Error::Msg(format!("error parsing summary composite: {e}")))?;
        self.serve_pending()
    }

    /// Go `servePending`.
    fn serve_pending(&mut self) -> Result<Entry, Error> {
        if self.pending.is_empty() {
            return Err(Error::msg("composite value produced no series"));
        }
        self.pending_idx = 1;
        Ok(Entry::Series)
    }

    /// Go `buildNativeHistogram`, building into the reused `h` or `fh`.
    fn build_native_histogram(&mut self, cf: &CompositeFields, is_gauge: bool) -> Result<(), String> {
        let b = self.b;
        let get_float = |field: usize| -> Result<Option<f64>, String> {
            match cf.get(b, field) {
                None => Ok(None),
                Some(v) => parse_float(v).map(Some).map_err(|e| e.num_error(v).to_string()),
            }
        };

        // schema is present: the caller only gets here when it is.
        let schema64 = go_parse_int(cf.raw(b, F_SCHEMA), 64).map_err(|e| format!("invalid schema: {e}"))?;
        if !(-4..=8).contains(&schema64) {
            return Err(format!("schema must be between -4 and 8, got {schema64}"));
        }

        let (count_field, sum_field, count_key, sum_key) =
            if is_gauge { (F_GCOUNT, F_GSUM, "gcount", "gsum") } else { (F_COUNT, F_SUM, "count", "sum") };
        let count = get_float(count_field)
            .map_err(|e| format!("invalid {count_key}: {e}"))?
            .ok_or_else(|| format!("missing required field: {count_key}"))?;
        if count < 0.0 {
            return Err(format!("{count_key} must not be negative, got {}", format_g(count)));
        }
        let sum = get_float(sum_field)
            .map_err(|e| format!("invalid {sum_key}: {e}"))?
            .ok_or_else(|| format!("missing required field: {sum_key}"))?;
        let zero_threshold = get_float(F_ZERO_THRESHOLD)
            .map_err(|e| format!("invalid zero_threshold: {e}"))?
            .ok_or_else(|| "missing required field: zero_threshold".to_string())?;
        if zero_threshold.is_nan() || zero_threshold.is_infinite() || zero_threshold < 0.0 {
            return Err(format!(
                "zero_threshold must be a non-negative, finite number, got {}",
                format_g(zero_threshold)
            ));
        }
        let zero_count = get_float(F_ZERO_COUNT)
            .map_err(|e| format!("invalid zero_count: {e}"))?
            .ok_or_else(|| "missing required field: zero_count".to_string())?;
        if zero_count < 0.0 {
            return Err(format!("zero_count must not be negative, got {}", format_g(zero_count)));
        }

        // A float histogram if count, zero_count or any bucket is not an integer (or has no uint64 form).
        let pos_buckets_raw = cf.raw(b, F_POSITIVE_BUCKETS);
        let neg_buckets_raw = cf.raw(b, F_NEGATIVE_BUCKETS);
        const MAX_U64_F: f64 = u64::MAX as f64;
        let is_float = count != count.trunc()
            || zero_count != zero_count.trunc()
            || count >= MAX_U64_F
            || zero_count >= MAX_U64_F
            || buckets_have_float(pos_buckets_raw)
            || buckets_have_float(neg_buckets_raw);

        let hint = if is_gauge { CounterResetHint::GaugeType } else { CounterResetHint::UnknownCounterReset };
        if is_float {
            let fh = &mut self.fh;
            self.has_fh = false;
            self.has_h = false;
            parse_spans(cf.raw(b, F_POSITIVE_SPANS), &mut fh.positive_spans)
                .map_err(|e| format!("invalid positive_spans: {e}"))?;
            parse_spans(cf.raw(b, F_NEGATIVE_SPANS), &mut fh.negative_spans)
                .map_err(|e| format!("invalid negative_spans: {e}"))?;
            parse_float_buckets(pos_buckets_raw, &mut fh.positive_buckets)
                .map_err(|e| format!("invalid positive_buckets: {e}"))?;
            parse_float_buckets(neg_buckets_raw, &mut fh.negative_buckets)
                .map_err(|e| format!("invalid negative_buckets: {e}"))?;
            fh.counter_reset_hint = hint;
            fh.schema = schema64 as i32;
            fh.zero_threshold = zero_threshold;
            fh.zero_count = zero_count;
            fh.count = count;
            fh.sum = sum;
            fh.custom_values = None;
            fh.validate().map_err(|e| format!("invalid float histogram: {e}"))?;
            self.has_fh = true;
            return Ok(());
        }

        let h = &mut self.h;
        self.has_fh = false;
        self.has_h = false;
        parse_spans(cf.raw(b, F_POSITIVE_SPANS), &mut h.positive_spans)
            .map_err(|e| format!("invalid positive_spans: {e}"))?;
        parse_spans(cf.raw(b, F_NEGATIVE_SPANS), &mut h.negative_spans)
            .map_err(|e| format!("invalid negative_spans: {e}"))?;
        parse_int_buckets(pos_buckets_raw, &mut h.positive_buckets)
            .map_err(|e| format!("invalid positive_buckets: {e}"))?;
        parse_int_buckets(neg_buckets_raw, &mut h.negative_buckets)
            .map_err(|e| format!("invalid negative_buckets: {e}"))?;
        absolute_to_int_deltas(&mut h.positive_buckets);
        absolute_to_int_deltas(&mut h.negative_buckets);
        h.counter_reset_hint = hint;
        h.schema = schema64 as i32;
        h.zero_threshold = zero_threshold;
        h.zero_count = zero_count as u64;
        h.count = count as u64;
        h.sum = sum;
        h.custom_values = None;
        h.validate().map_err(|e| format!("invalid histogram: {e}"))?;
        self.has_h = true;
        Ok(())
    }

    /// Go `buildClassicHistogramPending`: queues `_count`, `_sum` and `_bucket` series.
    fn build_classic_histogram_pending(&mut self, cf: &CompositeFields, is_native: bool) -> Result<(), String> {
        self.acquire_composite_scratch();
        let r = self.build_classic_histogram_pending_inner(cf, is_native);
        if r.is_err() {
            self.pending.clear();
        }
        r
    }

    fn build_classic_histogram_pending_inner(&mut self, cf: &CompositeFields, is_native: bool) -> Result<(), String> {
        let b = self.b;
        // GaugeHistogram samples with classic buckets expose count/sum as gcount/gsum.
        let gauge = self.mtype == MetricType::GaugeHistogram;
        let (count_field, sum_field, count_key, sum_key, count_suffix, sum_suffix) = if gauge {
            (F_GCOUNT, F_GSUM, "gcount", "gsum", "_gcount", "_gsum")
        } else {
            (F_COUNT, F_SUM, "count", "sum", "_count", "_sum")
        };

        let cv = cf.get(b, count_field).ok_or_else(|| format!("missing required field: {count_key}"))?;
        let count_val = strconv_float(cv).map_err(|e| format!("invalid {count_key}: {e}"))?;
        self.append_pending(count_suffix, "", 0.0, count_val);

        let sv = cf.get(b, sum_field).ok_or_else(|| format!("missing required field: {sum_key}"))?;
        let v = strconv_float(sv).map_err(|e| format!("invalid {sum_key}: {e}"))?;
        self.append_pending(sum_suffix, "", 0.0, v);

        // The spec requires a classic bucket list to include a +Inf threshold.
        let Some(bv) = cf.get(b, F_BUCKET) else {
            if !is_native {
                return Err("missing required field: bucket".to_string());
            }
            // Native-only histogram with ignore_native_histograms: the +Inf bucket holds the total count.
            self.append_pending("_bucket", "le", f64::INFINITY, count_val);
            return Ok(());
        };
        let mut has_pos_inf = false;
        for_each_bucket(bv, |lef, count| {
            // Classic histogram bucket values are counters and must not be negative; gauge histogram ones are
            // gauges, where the spec only discourages it.
            if count < 0.0 && !gauge {
                return Err(format!("invalid bucket: value must not be negative, got {}", format_g(count)));
            }
            if lef == f64::INFINITY {
                has_pos_inf = true;
            }
            self.append_pending("_bucket", "le", lef, count);
            Ok(())
        })?;
        if !has_pos_inf {
            return Err("classic histogram buckets must include a +Inf threshold".to_string());
        }
        Ok(())
    }

    /// Go `buildNHCBHistogram`.
    fn build_nhcb_histogram(&mut self, cf: &CompositeFields, is_native: bool) -> Result<(), String> {
        let b = self.b;
        let is_gauge = self.mtype == MetricType::GaugeHistogram;
        let (count_field, sum_field, count_key, sum_key) =
            if is_gauge { (F_GCOUNT, F_GSUM, "gcount", "gsum") } else { (F_COUNT, F_SUM, "count", "sum") };

        let cv = cf.get(b, count_field).ok_or_else(|| format!("missing required field: {count_key}"))?;
        let count = strconv_float(cv).map_err(|e| format!("invalid {count_key}: {e}"))?;
        let sv = cf.get(b, sum_field).ok_or_else(|| format!("missing required field: {sum_key}"))?;
        let sum = strconv_float(sv).map_err(|e| format!("invalid {sum_key}: {e}"))?;

        let tmp = &mut self.tmp_nhcb;
        tmp.reset();
        tmp.set_count(count)?;
        tmp.set_sum(sum)?;

        match cf.get(b, F_BUCKET) {
            None => {
                if !is_native {
                    return Err("missing required field: bucket".to_string());
                }
            }
            Some(bv) => {
                let mut has_pos_inf = false;
                for_each_bucket(bv, |lef, count| {
                    if lef == f64::INFINITY {
                        has_pos_inf = true;
                    }
                    tmp.set_bucket_count(lef, count)
                })?;
                if !has_pos_inf {
                    return Err("classic histogram buckets must include a +Inf threshold".to_string());
                }
            }
        }

        let (h, fh) = tmp.convert()?;
        self.has_h = false;
        self.has_fh = false;
        if let Some(mut h) = h {
            if is_gauge {
                h.counter_reset_hint = CounterResetHint::GaugeType;
            }
            self.h = h;
            self.has_h = true;
        } else if let Some(mut fh) = fh {
            if is_gauge {
                fh.counter_reset_hint = CounterResetHint::GaugeType;
            }
            self.fh = fh;
            self.has_fh = true;
        }
        Ok(())
    }

    /// Go `buildSummaryPending`: queues `_count`, `_sum` and quantile series.
    fn build_summary_pending(&mut self, cf: &CompositeFields) -> Result<(), String> {
        self.acquire_composite_scratch();
        let r = self.build_summary_pending_inner(cf);
        if r.is_err() {
            self.pending.clear();
        }
        r
    }

    fn build_summary_pending_inner(&mut self, cf: &CompositeFields) -> Result<(), String> {
        let b = self.b;
        let v = strconv_float(cf.raw(b, F_COUNT)).map_err(|e| format!("invalid count: {e}"))?;
        self.append_pending("_count", "", 0.0, v);
        let v = strconv_float(cf.raw(b, F_SUM)).map_err(|e| format!("invalid sum: {e}"))?;
        self.append_pending("_sum", "", 0.0, v);
        for_each_quantile(cf.raw(b, F_QUANTILE), |qf, val| {
            self.append_pending("", "quantile", qf, val);
        })
    }

    /// Go `appendPendingSeriesBytes`: appends the series bytes of an exploded entry to `series_buf` and queues it.
    fn append_pending(&mut self, suffix: &str, inject_key: &'static str, inject_val: f64, val: f64) {
        let b = self.b;
        let buf = &mut self.series_buf;
        let start = buf.len();
        buf.extend_from_slice(&b[self.offsets[0]..self.offsets[1]]);
        buf.extend_from_slice(suffix.as_bytes());
        let name_len = buf.len() - start;
        if self.enable_type_and_unit_labels {
            if self.mtype != MetricType::Empty && self.mtype != MetricType::Unknown {
                buf.push(SEP);
                buf.extend_from_slice(b"__type__");
                buf.push(SEP);
                buf.extend_from_slice(self.mtype.as_str().as_bytes());
            }
            if !self.unit.is_empty() {
                buf.push(SEP);
                buf.extend_from_slice(b"__unit__");
                buf.push(SEP);
                buf.extend_from_slice(&self.unit);
            }
        }
        let mut i = 2;
        while i < self.offsets.len() {
            buf.push(SEP);
            buf.extend_from_slice(&b[self.offsets[i]..self.offsets[i + 1]]);
            buf.push(SEP);
            buf.extend_from_slice(&b[self.offsets[i + 2]..self.offsets[i + 3]]);
            i += 4;
        }
        if !inject_key.is_empty() {
            buf.push(SEP);
            buf.extend_from_slice(inject_key.as_bytes());
            buf.push(SEP);
            self.float_buf.clear();
            append_open_metrics_float(&mut self.float_buf, inject_val);
            buf.extend_from_slice(self.float_buf.as_bytes());
        }
        let series_end = buf.len();
        self.pending.push(PendingEntry { series_start: start, series_end, name_len, inject_key, inject_val, val });
    }

    /// Labels of the current exploded composite entry, rebuilt from its series bytes (Go `Labels` pending path).
    fn pending_labels(&mut self, pe: PendingEntry, l: &mut Labels) {
        let s = &self.series_buf[pe.series_start..pe.series_end];
        let builder = &mut self.builder;
        let LabelScratch { mname, name, value, name_s, value_s, .. } = &mut self.sc;
        builder.reset();
        let type_set = self.mtype != MetricType::Empty && self.mtype != MetricType::Unknown;
        let unit_empty = self.unit.is_empty();
        let mut pos = pe.name_len;
        let metric_name_empty;
        {
            let metric_name = lossy(crate::unreplace(&s[..pe.name_len], mname), name_s);
            metric_name_empty = metric_name.is_empty();
            if self.enable_type_and_unit_labels {
                if type_set {
                    pos += 1 + "__type__".len() + 1 + self.mtype.as_str().len();
                }
                if !unit_empty {
                    pos += 1 + "__unit__".len() + 1 + self.unit.len();
                }
                if !metric_name_empty {
                    builder.add("__name__", metric_name);
                }
                if type_set {
                    builder.add("__type__", self.mtype.as_str());
                }
                if !unit_empty {
                    let unit = lossy(&self.unit, value_s);
                    builder.add("__unit__", unit);
                }
            } else {
                builder.add("__name__", metric_name);
            }
        }
        let mut i = 2;
        while i < self.offsets.len() {
            pos += 1; // Separator.
            let k_len = self.offsets[i + 1] - self.offsets[i];
            let label = lossy(crate::unreplace(&s[pos..pos + k_len], name), name_s);
            pos += k_len + 1; // Key and separator.
            let v_len = self.offsets[i + 3] - self.offsets[i + 2];
            let val = lossy(crate::unreplace(&s[pos..pos + v_len], value), value_s);
            pos += v_len;
            i += 4;
            if self.enable_type_and_unit_labels {
                let is_empty_for = match label {
                    "__name__" => metric_name_empty,
                    "__type__" => !type_set,
                    "__unit__" => unit_empty,
                    _ => true,
                };
                if !is_empty_for {
                    continue;
                }
            }
            builder.add(label, val);
        }
        if !pe.inject_key.is_empty() {
            pos += 1 + pe.inject_key.len() + 1;
            builder.add(pe.inject_key, lossy(&s[pos..], value_s));
        }
        builder.sort();
        builder.overwrite(l);
    }
}

impl Parser for OpenMetrics2Parser<'_> {
    fn series(&self) -> (&[u8], Option<i64>, f64) {
        let ts = if self.has_ts { Some(self.ts) } else { None };
        if self.pending_idx > 0 {
            let pe = self.pending[self.pending_idx - 1];
            return (&self.series_buf[pe.series_start..pe.series_end], ts, pe.val);
        }
        (&self.b[self.series.0..self.series.1], ts, self.val)
    }

    fn histogram(&mut self) -> (&[u8], Option<i64>, Option<&Histogram>, Option<&FloatHistogram>) {
        (
            &self.b[self.series.0..self.series.1],
            if self.has_ts { Some(self.ts) } else { None },
            if self.has_h { Some(&self.h) } else { None },
            if self.has_fh { Some(&self.fh) } else { None },
        )
    }

    fn help(&mut self) -> (&[u8], &[u8]) {
        let m = &self.b[self.offsets[0]..self.offsets[1]];
        let text = &self.b[self.text.0..self.text.1];
        if crate::memchr(b'\\', text).is_some() {
            crate::replace_escapes(text, &mut self.help_buf, true);
            return (m, &self.help_buf);
        }
        (m, text)
    }

    fn typ(&self) -> (&[u8], MetricType) {
        (&self.b[self.offsets[0]..self.offsets[1]], self.mtype)
    }

    fn unit(&self) -> (&[u8], &[u8]) {
        (&self.b[self.offsets[0]..self.offsets[1]], &self.unit)
    }

    fn comment(&self) -> &[u8] {
        &self.b[self.text.0..self.text.1]
    }

    fn labels(&mut self, l: &mut Labels) {
        if self.pending_idx > 0 {
            let pe = self.pending[self.pending_idx - 1];
            self.pending_labels(pe, l);
            return;
        }
        build_text_labels(
            self.b,
            &self.offsets,
            self.mtype,
            &self.unit,
            self.enable_type_and_unit_labels,
            &mut self.builder,
            &mut self.sc,
            l,
        );
    }

    fn exemplar(&mut self, e: &mut Exemplar) -> bool {
        if self.exemplars.is_empty() {
            return false;
        }
        let ex;
        if self.pending_idx > 0 {
            let idx = self.pending_idx - 1;
            let pe = self.pending[idx];
            if pe.inject_key != "le" {
                return false;
            }
            let upper = pe.inject_val;
            let has_lower = idx > 0 && self.pending[idx - 1].inject_key == "le";
            let lower = if has_lower { self.pending[idx - 1].inject_val } else { 0.0 };
            let is_pos_inf = upper == f64::INFINITY;
            let mut found = None;
            while self.exemplar_idx < self.exemplars.len() {
                let cand = self.exemplars[self.exemplar_idx];
                self.exemplar_idx += 1;
                if (cand.val <= upper && (!has_lower || cand.val > lower)) || (is_pos_inf && cand.val.is_nan()) {
                    found = Some(cand);
                    break;
                }
            }
            match found {
                Some(c) => ex = c,
                None => return false,
            }
        } else {
            if self.exemplar_idx >= self.exemplars.len() {
                return false;
            }
            ex = self.exemplars[self.exemplar_idx];
            self.exemplar_idx += 1;
        }

        e.value = ex.val;
        e.has_ts = true;
        e.ts = ex.ts;
        self.builder.reset();
        if ex.offsets_start < ex.offsets_end {
            let LabelScratch { name, value, name_s, value_s, .. } = &mut self.sc;
            let mut i = ex.offsets_start;
            while i < ex.offsets_end {
                let o = &self.e_offsets[i..i + 4];
                let n = lossy(crate::unreplace(&self.b[o[0]..o[1]], name), name_s);
                let v = lossy(crate::unreplace(&self.b[o[2]..o[3]], value), value_s);
                self.builder.add(n, v);
                i += 4;
            }
            self.builder.sort();
        }
        self.builder.overwrite(&mut e.labels);
        true
    }

    fn start_timestamp(&mut self) -> i64 {
        if self.has_st {
            self.st
        } else {
            0
        }
    }

    fn next(&mut self) -> Result<Entry, Error> {
        // Serve the pending entries of a composite value.
        if self.pending_idx < self.pending.len() {
            self.pending_idx += 1;
            self.exemplar_idx = 0;
            return Ok(Entry::Series);
        }
        if !self.pending.is_empty() {
            self.pending.clear();
            self.pending_idx = 0;
            self.series_buf.clear();
        }

        self.start = self.l.i;
        self.offsets.clear();
        if !self.exemplars.is_empty() {
            self.exemplars.clear();
            self.e_offsets.clear();
            self.exemplar_idx = 0;
        }
        self.has_ts = false;
        self.has_st = false;

        let t = self.next_token();
        match t {
            Token::EofWord => {
                if self.next_token() != Token::Eof {
                    return Err(Error::msg("unexpected data after # EOF"));
                }
                // All pending entries were served, so the composite buffers go back to the pool.
                self.release_composite_scratch();
                Err(Error::Eof)
            }
            Token::Eof => Err(Error::msg("data does not end with # EOF")),
            Token::Help | Token::Type | Token::Unit => {
                let t2 = self.next_token();
                if t2 != Token::MName {
                    return Err(self.parse_error(&format!("expected metric name after {}", t.as_str()), t2));
                }
                let (mut ms, mut me) = (self.l.start, self.l.i);
                if self.b[ms] == b'"' && self.b[me - 1] == b'"' {
                    ms += 1;
                    me -= 1;
                    if ms == me {
                        return Err(Error::msg("metric name must not be empty"));
                    }
                    if std::str::from_utf8(&self.b[ms..me]).is_err() {
                        return Err(Error::Msg(format!("invalid UTF-8 metric name: {}", q(&self.b[ms..me]))));
                    }
                }
                self.offsets.push(ms);
                self.offsets.push(me);
                self.reset_on_family_change(ms, me);

                if self.next_token() != Token::Text {
                    return Err(Error::Msg(format!("expected text in {}", t.as_str())));
                }
                self.text = if self.l.i - self.l.start > 1 { (self.l.start + 1, self.l.i - 1) } else { (0, 0) };
                let text = &self.b[self.text.0..self.text.1];
                if text.last() == Some(&b'\r') {
                    return Err(Error::Msg(format!("unexpected carriage return in {}: {}", t.as_str(), q(text))));
                }
                match t {
                    Token::Type => {
                        self.mtype = match text {
                            b"counter" => MetricType::Counter,
                            b"gauge" => MetricType::Gauge,
                            b"histogram" => MetricType::Histogram,
                            b"gaugehistogram" => MetricType::GaugeHistogram,
                            b"summary" => MetricType::Summary,
                            b"info" => {
                                if !self.family_name().ends_with(b"_info") {
                                    return Err(Error::Msg(format!(
                                        "info metric family name {} must end with _info",
                                        q(self.family_name())
                                    )));
                                }
                                MetricType::Info
                            }
                            b"stateset" => MetricType::StateSet,
                            b"unknown" => MetricType::Unknown,
                            _ => return Err(Error::Msg(format!("invalid metric type {}", q(text)))),
                        };
                    }
                    Token::Help => {
                        if std::str::from_utf8(text).is_err() {
                            return Err(Error::Msg(format!("help text {} is not a valid utf8 string", q(text))));
                        }
                        return Ok(Entry::Help);
                    }
                    _ => {
                        // OM2 only recommends the unit to be a suffix of the family name.
                        self.unit.clear();
                        self.unit.extend_from_slice(text);
                    }
                }
                if matches!(self.mtype, MetricType::Info | MetricType::StateSet) && !self.unit.is_empty() {
                    return Err(Error::Msg(format!(
                        "{} metric {} must have an empty unit, got {}",
                        self.mtype.as_str(),
                        q(self.family_name()),
                        q(&self.unit)
                    )));
                }
                Ok(if t == Token::Type { Entry::Type } else { Entry::Unit })
            }
            Token::BraceOpen => {
                // UTF-8 metric name inside braces.
                self.offsets.push(NONE);
                self.offsets.push(NONE);
                let mut offsets = std::mem::take(&mut self.offsets);
                let r = self.parse_lvals(&mut offsets, false);
                self.offsets = offsets;
                r?;
                if self.offsets[0] != NONE {
                    self.reset_on_family_change(self.offsets[0], self.offsets[1]);
                }
                self.series = (self.start, self.l.i);
                let t2 = self.next_token();
                self.parse_series_end_of_line(t2)
            }
            Token::MName => {
                self.offsets.push(self.start);
                self.offsets.push(self.l.i);
                self.reset_on_family_change(self.start, self.l.i);
                self.series = (self.start, self.l.i);
                let mut t2 = self.next_token();
                if t2 == Token::BraceOpen {
                    let mut offsets = std::mem::take(&mut self.offsets);
                    let r = self.parse_lvals(&mut offsets, false);
                    self.offsets = offsets;
                    r?;
                    self.series = (self.start, self.l.i);
                    t2 = self.next_token();
                }
                self.parse_series_end_of_line(t2)
            }
            _ => Err(self.parse_error("expected a valid start token", t)),
        }
    }

    fn kind(&self) -> ParserKind {
        ParserKind::OpenMetrics2
    }
}

/// Go `strconv.ParseFloat(s, 64)` with the error as text.
fn strconv_float(s: &[u8]) -> Result<f64, String> {
    parse_float(s).map_err(|e| e.num_error(s).to_string())
}

/// Go `strconv.ParseUint(s, 10, bit_size)`.
fn go_parse_uint_kind(s: &[u8], bit_size: u32) -> Result<u64, NumErrKind> {
    if s.is_empty() {
        return Err(NumErrKind::Syntax);
    }
    let max_val = if bit_size >= 64 { u64::MAX } else { (1u64 << bit_size) - 1 };
    const CUTOFF: u64 = u64::MAX / 10 + 1;
    let mut n: u64 = 0;
    for &c in s {
        if !c.is_ascii_digit() {
            return Err(NumErrKind::Syntax);
        }
        if n >= CUTOFF {
            return Err(NumErrKind::Range);
        }
        n *= 10;
        let n1 = n.wrapping_add((c - b'0') as u64);
        if n1 < n || n1 > max_val {
            return Err(NumErrKind::Range);
        }
        n = n1;
    }
    Ok(n)
}

fn go_parse_uint(s: &[u8], bit_size: u32) -> Result<u64, NumError> {
    go_parse_uint_kind(s, bit_size).map_err(|kind| NumError { func: "ParseUint", num: s.to_vec(), kind })
}

/// Go `strconv.ParseInt(s, 10, bit_size)`.
fn go_parse_int(s0: &[u8], bit_size: u32) -> Result<i64, NumError> {
    let err = |kind| NumError { func: "ParseInt", num: s0.to_vec(), kind };
    if s0.is_empty() {
        return Err(err(NumErrKind::Syntax));
    }
    let (neg, s) = match s0[0] {
        b'+' => (false, &s0[1..]),
        b'-' => (true, &s0[1..]),
        _ => (false, s0),
    };
    let un = go_parse_uint_kind(s, bit_size).map_err(err)?;
    let cutoff = 1u64 << (bit_size - 1);
    if (!neg && un >= cutoff) || (neg && un > cutoff) {
        return Err(err(NumErrKind::Range));
    }
    Ok(if neg { (un as i64).wrapping_neg() } else { un as i64 })
}

/// The first rune of `s` if it is valid UTF-8 (Go `DecodeRune` without `RuneError`).
fn first_char(s: &[u8]) -> Option<char> {
    s.utf8_chunks().next().and_then(|c| c.valid().chars().next())
}

/// The last rune of `s` if it is valid UTF-8 (Go `DecodeLastRune` without `RuneError`).
fn last_char(s: &[u8]) -> Option<char> {
    let mut i = s.len().checked_sub(1)?;
    while i > 0 && s.len() - i < 4 && s[i] & 0xc0 == 0x80 {
        i -= 1;
    }
    let mut it = std::str::from_utf8(&s[i..]).ok()?.chars();
    let c = it.next()?;
    if it.next().is_some() {
        return None;
    }
    Some(c)
}

/// Go `bytes.TrimSpace` / `strings.TrimSpace` (Unicode white space).
fn trim_space(mut s: &[u8]) -> &[u8] {
    while let Some(&c) = s.first() {
        if c < 0x80 {
            if !matches!(c, b'\t' | b'\n' | 0x0b | 0x0c | b'\r' | b' ') {
                break;
            }
            s = &s[1..];
        } else {
            match first_char(s) {
                Some(ch) if ch.is_whitespace() => s = &s[ch.len_utf8()..],
                _ => break,
            }
        }
    }
    while let Some(&c) = s.last() {
        if c < 0x80 {
            if !matches!(c, b'\t' | b'\n' | 0x0b | 0x0c | b'\r' | b' ') {
                break;
            }
            s = &s[..s.len() - 1];
        } else {
            match last_char(s) {
                Some(ch) if ch.is_whitespace() => s = &s[..s.len() - ch.len_utf8()],
                _ => break,
            }
        }
    }
    s
}

/// The inner part of a `[...]` list, or None for an empty list. `what` names the list in the error.
fn bracket_inner<'s>(b: &'s [u8], what: &str) -> Result<Option<&'s [u8]>, String> {
    let b = trim_space(b);
    if b.is_empty() || b == b"[]" {
        return Ok(None);
    }
    if b.len() < 2 || b[0] != b'[' || b[b.len() - 1] != b']' {
        return Err(format!("{what} must be wrapped in []: {}", q(b)));
    }
    Ok(Some(&b[1..b.len() - 1]))
}

/// Go `parseSpans`: `[offset:length,...]` into `out`.
fn parse_spans(b: &[u8], out: &mut Vec<Span>) -> Result<(), String> {
    out.clear();
    let Some(inner) = bracket_inner(b, "spans")? else { return Ok(()) };
    let inner = trim_space(inner);
    if inner.is_empty() {
        return Ok(());
    }
    for part in inner.split(|&c| c == b',') {
        let part = trim_space(part);
        if part.is_empty() {
            continue;
        }
        let Some(idx) = crate::memchr(b':', part) else {
            return Err(format!("span missing ':': {}", q(part)));
        };
        let (before, after) = (&part[..idx], &part[idx + 1..]);
        let offset = go_parse_int(trim_space(before), 32).map_err(|e| format!("invalid span offset {}: {e}", q(before)))?;
        let length =
            go_parse_uint(trim_space(after), 32).map_err(|e| format!("invalid span length {}: {e}", q(after)))?;
        out.push(Span { offset: offset as i32, length: length as u32 });
    }
    Ok(())
}

/// Go `bucketsHaveFloat`: OM2 formats floats with a decimal point or exponent.
fn buckets_have_float(b: &[u8]) -> bool {
    b.iter().any(|&c| matches!(c, b'.' | b'e' | b'E'))
}

/// Go `parseIntBuckets`: `[b1,b2,...]` into absolute counts in `out`.
fn parse_int_buckets(b: &[u8], out: &mut Vec<i64>) -> Result<(), String> {
    out.clear();
    let Some(inner) = bracket_inner(b, "buckets")? else { return Ok(()) };
    let inner = trim_space(inner);
    for part in inner.split(|&c| c == b',') {
        let part = trim_space(part);
        if part.is_empty() {
            continue;
        }
        let v = go_parse_int(part, 64).map_err(|e| format!("invalid bucket {}: {e}", q(part)))?;
        out.push(v);
    }
    Ok(())
}

/// Go `parseFloatBuckets`: `[b1,b2,...]` into `out`.
fn parse_float_buckets(b: &[u8], out: &mut Vec<f64>) -> Result<(), String> {
    out.clear();
    let Some(inner) = bracket_inner(b, "float buckets")? else { return Ok(()) };
    let inner = trim_space(inner);
    for part in inner.split(|&c| c == b',') {
        let part = trim_space(part);
        if part.is_empty() {
            continue;
        }
        let v = strconv_float(part).map_err(|e| format!("invalid float bucket {}: {e}", q(part)))?;
        out.push(v);
    }
    Ok(())
}

/// Go `absoluteToIntDeltas`.
fn absolute_to_int_deltas(abs: &mut [i64]) {
    for i in (1..abs.len()).rev() {
        abs[i] = abs[i].wrapping_sub(abs[i - 1]);
    }
}

/// Go `parseBuckets`: calls `f(le, count)` for each entry of `[1.0:3,2.0:7,+Inf:12]`. Parse errors are returned
/// as `"invalid bucket: ..."`; errors of `f` are returned unchanged.
fn for_each_bucket(s: &[u8], mut f: impl FnMut(f64, f64) -> Result<(), String>) -> Result<(), String> {
    let wrap = |e: String| format!("invalid bucket: {e}");
    let Some(inner) = bracket_inner(s, "bucket").map_err(wrap)? else { return Ok(()) };
    let mut prev_le = f64::NEG_INFINITY;
    let mut first_le = true;
    for part in inner.split(|&c| c == b',') {
        let part = trim_space(part);
        if part.is_empty() {
            continue;
        }
        let Some(idx) = part.iter().rposition(|&c| c == b':') else {
            return Err(wrap(format!("bucket missing ':': {}", q(part))));
        };
        let le = trim_space(&part[..idx]);
        let count = strconv_float(trim_space(&part[idx + 1..]))
            .map_err(|e| wrap(format!("invalid bucket count {}: {e}", q(&part[idx + 1..]))))?;
        let lef = strconv_float(le).map_err(|e| wrap(format!("invalid bucket threshold {}: {e}", q(le))))?;
        if lef.is_nan() {
            return Err(wrap(format!("bucket threshold must not be NaN: {}", q(part))));
        }
        // OM2 permits -Inf as the first threshold, so ordering is checked from the second one.
        if !first_le && lef <= prev_le {
            return Err(wrap(format!("classic histogram buckets must be sorted in increasing order: {}", q(part))));
        }
        first_le = false;
        prev_le = lef;
        f(lef, count)?;
    }
    Ok(())
}

/// Go `parseQuantiles`: calls `f(q, value)` for each entry of `[0.5:1.0,0.9:2.0]`. Errors are returned as
/// `"invalid quantile: ..."`.
fn for_each_quantile(s: &[u8], mut f: impl FnMut(f64, f64)) -> Result<(), String> {
    let wrap = |e: String| format!("invalid quantile: {e}");
    let Some(inner) = bracket_inner(s, "quantile").map_err(wrap)? else { return Ok(()) };
    let mut prev_q = f64::NEG_INFINITY;
    for part in inner.split(|&c| c == b',') {
        let part = trim_space(part);
        if part.is_empty() {
            continue;
        }
        let Some(idx) = crate::memchr(b':', part) else {
            return Err(wrap(format!("quantile missing ':': {}", q(part))));
        };
        let (before, after) = (&part[..idx], &part[idx + 1..]);
        let qs = trim_space(before);
        let val = strconv_float(trim_space(after))
            .map_err(|e| wrap(format!("invalid quantile value {}: {e}", q(after))))?;
        let qf = strconv_float(qs).map_err(|e| wrap(format!("invalid quantile {}: {e}", q(qs))))?;
        if qf.is_nan() {
            return Err(wrap(format!("quantile must not be NaN: {}", q(part))));
        }
        if !(0.0..=1.0).contains(&qf) {
            return Err(wrap(format!("quantile must be between 0.0 and 1.0, got {}", q(qs))));
        }
        if qf <= prev_q {
            return Err(wrap(format!("quantiles must be sorted in increasing order: {}", q(part))));
        }
        prev_q = qf;
        f(qf, val);
    }
    Ok(())
}
