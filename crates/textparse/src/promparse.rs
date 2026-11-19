//! Port of `promparse.go` and its lexer `promlex.l` (the golex DFA written by hand, rule for rule).

use std::borrow::Cow;

use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{Labels, ScratchBuilder};
use model::metadata::MetricType;

use crate::lexutil;
use crate::strconv::{self, append_quote_bytes};
use crate::token::*;
use crate::{Entry, Error, Parser, ParserKind};

const NONE: usize = usize::MAX;

#[inline(always)]
fn is_m(c: u8) -> bool {
    lexutil::is(c, lexutil::M)
}

#[inline(always)]
fn is_l(c: u8) -> bool {
    lexutil::is(c, lexutil::L)
}

#[inline(always)]
fn is_ws(c: u8) -> bool {
    c == b' ' || c == b'\t'
}

/// The Prometheus text lexer (Go `promlexer`). It holds only positions; the input is passed in.
#[derive(Default)]
pub(crate) struct PromLexer {
    pub(crate) i: usize,
    pub(crate) start: usize,
    pub(crate) state: u8,
    /// OR of the bytes of the quoted strings scanned since it was reset (>= 0x80: maybe not ASCII).
    pub(crate) hi: u8,
}

impl PromLexer {
    #[inline(always)]
    fn cur(&self, b: &[u8]) -> u8 {
        if self.i < b.len() { b[self.i] } else { 0 }
    }

    /// Go `next`: advances one byte; inside label values and help/comment text null bytes are skipped.
    #[inline(always)]
    fn next(&mut self, b: &[u8]) -> u8 {
        self.i += 1;
        if self.i >= b.len() {
            self.i = b.len();
            return 0;
        }
        let mut c = b[self.i];
        while c == 0 && matches!(self.state, S_LVALUE | S_META2 | S_COMMENT) {
            self.i += 1;
            if self.i >= b.len() {
                return 0;
            }
            c = b[self.i];
        }
        c
    }

    fn consume_comment(&mut self, b: &[u8]) -> Token {
        // Go: `for c := l.cur(); ; c = l.next()` until \0 (EOF) or \n. `next()` skips null bytes in sComment.
        match self.cur(b) {
            0 => return Token::Eof,
            b'\n' => {}
            _ => {
                let mut i = self.i + 1;
                if self.state == S_COMMENT {
                    i = lexutil::find(b, i, b'\n');
                } else {
                    while i < b.len() && b[i] != 0 && b[i] != b'\n' {
                        i += 1;
                    }
                }
                self.i = i.min(b.len());
                if self.i >= b.len() || b[self.i] == 0 {
                    return Token::Eof;
                }
            }
        }
        self.state = S_INIT;
        Token::Comment
    }

    #[inline]
    fn abort(&mut self, b: &[u8]) -> Token {
        if self.state == S_COMMENT {
            return self.consume_comment(b);
        }
        Token::Invalid
    }

    #[inline]
    fn whitespace(&mut self, b: &[u8]) -> Token {
        while is_ws(self.next(b)) {}
        Token::Whitespace
    }

    /// `\"(\\.|[^\\"])*\"` after the opening quote. Returns false on abort.
    #[inline]
    fn quoted(&mut self, b: &[u8]) -> bool {
        match lexutil::scan_quoted(b, self.i, self.state == S_LVALUE, true, &mut self.hi) {
            Ok(close) => {
                self.i = close;
                self.next(b);
                true
            }
            Err(abort) => {
                self.i = abort;
                false
            }
        }
    }

    /// Matches the literal `word` (first byte already matched) followed by `[\t ]+`.
    fn keyword(&mut self, b: &[u8], rest: &[u8]) -> bool {
        for &w in rest {
            if self.next(b) != w {
                return false;
            }
        }
        if !is_ws(self.next(b)) {
            return false;
        }
        while is_ws(self.next(b)) {}
        true
    }

    /// Go `Lex`.
    pub(crate) fn lex(&mut self, b: &[u8]) -> Token {
        if self.i >= b.len() {
            return Token::Eof;
        }
        let mut c = b[self.i];
        self.start = self.i;
        loop {
            match self.state {
                S_INIT => {
                    return match c {
                        b'#' => {
                            c = self.next(b);
                            if is_ws(c) {
                                loop {
                                    c = self.next(b);
                                    if !is_ws(c) {
                                        break;
                                    }
                                }
                                self.state = S_COMMENT;
                                continue;
                            }
                            self.consume_comment(b)
                        }
                        c if is_m(c) => {
                            self.i = lexutil::scan_while(b, self.i + 1, lexutil::MD);
                            self.state = S_VALUE;
                            Token::MName
                        }
                        b'\n' => {
                            self.next(b);
                            self.state = S_INIT;
                            Token::Linebreak
                        }
                        b'\t' | b' ' => self.whitespace(b),
                        0 => {
                            self.next(b);
                            Token::Eof
                        }
                        b'{' => {
                            self.next(b);
                            self.state = S_LABELS;
                            Token::BraceOpen
                        }
                        _ => self.abort(b),
                    };
                }
                S_COMMENT => {
                    return match c {
                        b'H' => {
                            if self.keyword(b, b"ELP") {
                                self.state = S_META1;
                                Token::Help
                            } else {
                                self.abort(b)
                            }
                        }
                        b'T' => {
                            if self.keyword(b, b"YPE") {
                                self.state = S_META1;
                                Token::Type
                            } else {
                                self.abort(b)
                            }
                        }
                        b'\t' | b' ' => self.whitespace(b),
                        _ => self.abort(b),
                    };
                }
                S_META1 => {
                    return match c {
                        b'"' => {
                            if self.quoted(b) {
                                self.state = S_META2;
                                Token::MName
                            } else {
                                self.abort(b)
                            }
                        }
                        c if is_m(c) => {
                            self.i = lexutil::scan_while(b, self.i + 1, lexutil::MD);
                            self.state = S_META2;
                            Token::MName
                        }
                        b'\t' | b' ' => self.whitespace(b),
                        _ => self.abort(b),
                    };
                }
                S_META2 => {
                    let text = match c {
                        b'\t' | b' ' => loop {
                            c = self.next(b);
                            if is_ws(c) {
                                continue;
                            }
                            if c != 0 && c != b'\n' {
                                break true;
                            }
                            break false;
                        },
                        0 | b'\n' => {
                            // {C}* matching the empty string.
                            self.state = S_INIT;
                            return Token::Text;
                        }
                        _ => true,
                    };
                    if !text {
                        return Token::Whitespace;
                    }
                    // {C}*: next() skips null bytes in sMeta2, so only \n or the end stops it.
                    self.i = lexutil::find(b, self.i + 1, b'\n');
                    self.state = S_INIT;
                    return Token::Text;
                }
                S_LABELS => {
                    return match c {
                        b'"' => {
                            if self.quoted(b) {
                                self.state = S_LABELS;
                                Token::QString
                            } else {
                                self.abort(b)
                            }
                        }
                        b',' => {
                            self.next(b);
                            Token::Comma
                        }
                        b'=' => {
                            self.next(b);
                            self.state = S_LVALUE;
                            Token::Equal
                        }
                        b'\t' | b' ' => self.whitespace(b),
                        b'}' => {
                            self.next(b);
                            self.state = S_VALUE;
                            Token::BraceClose
                        }
                        c if is_l(c) => {
                            self.i = lexutil::scan_while(b, self.i + 1, lexutil::LD);
                            Token::LName
                        }
                        _ => self.abort(b),
                    };
                }
                S_LVALUE => {
                    return match c {
                        b'"' => {
                            if self.quoted(b) {
                                self.state = S_LABELS;
                                Token::LValue
                            } else {
                                self.abort(b)
                            }
                        }
                        b'\t' | b' ' => self.whitespace(b),
                        _ => self.abort(b),
                    };
                }
                S_VALUE => {
                    return match c {
                        b'\t' | b' ' => self.whitespace(b),
                        b'{' => {
                            self.next(b);
                            self.state = S_LABELS;
                            Token::BraceOpen
                        }
                        0 | b'\n' => self.abort(b),
                        _ => {
                            // [^{ \t\n]+ (and not \0).
                            let mut i = self.i + 1;
                            while i < b.len() && !matches!(b[i], 0 | b'\t' | b'\n' | b' ' | b'{') {
                                i += 1;
                            }
                            self.i = i;
                            self.state = S_TIMESTAMP;
                            Token::Value
                        }
                    };
                }
                S_TIMESTAMP => {
                    return match c {
                        b'\n' => {
                            self.next(b);
                            self.state = S_INIT;
                            Token::Linebreak
                        }
                        b'\t' | b' ' => self.whitespace(b),
                        b'0'..=b'9' => {
                            self.i = lexutil::scan_while(b, self.i + 1, lexutil::D);
                            Token::Timestamp
                        }
                        _ => self.abort(b),
                    };
                }
                _ => panic!("invalid start condition {}", self.state),
            }
        }
    }
}

/// Reusable buffers for building label sets.
#[derive(Default)]
pub(crate) struct LabelScratch {
    pub(crate) mname: String,
    pub(crate) name: String,
    pub(crate) value: String,
    pub(crate) name_s: String,
    pub(crate) value_s: String,
    pub(crate) float: String,
}

/// UTF-8 view of `b`; invalid bytes are replaced with U+FFFD (Rust labels must be valid UTF-8).
#[inline]
pub(crate) fn lossy<'s>(b: &'s [u8], scratch: &'s mut String) -> &'s str {
    match std::str::from_utf8(b) {
        Ok(s) => s,
        Err(_) => {
            scratch.clear();
            scratch.push_str(&String::from_utf8_lossy(b));
            scratch.as_str()
        }
    }
}

/// The text of `b[a..z]`: a slice of the already validated `whole` (which starts at `b[s0]`) when possible,
/// else a lossy UTF-8 copy.
#[inline]
fn part<'s>(b: &'s [u8], whole: Option<&'s str>, s0: usize, a: usize, z: usize, scratch: &'s mut String) -> &'s str {
    if let Some(w) = whole
        && a >= s0
            && let Some(t) = w.get(a - s0..z - s0) {
                return t;
            }
    lossy(&b[a..z], scratch)
}

/// Go `lvalReplacer` on text: `\"`, `\\` and `\n` are replaced, left to right.
#[inline]
pub(crate) fn unreplace_str<'s>(s: &'s str, out: &'s mut String) -> &'s str {
    if !s.as_bytes().contains(&b'\\') {
        return s;
    }
    out.clear();
    let b = s.as_bytes();
    let mut last = 0;
    let mut i = 0;
    while i + 1 < b.len() {
        if b[i] == b'\\' {
            let r = match b[i + 1] {
                b'\\' => "\\",
                b'n' => "\n",
                b'"' => "\"",
                _ => {
                    i += 1;
                    continue;
                }
            };
            out.push_str(&s[last..i]);
            out.push_str(r);
            i += 2;
            last = i;
            continue;
        }
        i += 1;
    }
    out.push_str(&s[last..]);
    out.as_str()
}

/// Builds the labels of a text-format series (shared by the Prometheus and OpenMetrics parsers): metric name,
/// optional type/unit labels, then the label pairs from `offsets` (pairs of name and value ranges from index 2).
/// `series` is the byte range of the series; it is UTF-8 validated once and sliced for the parts.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_text_labels(
    b: &[u8],
    series: (usize, usize),
    offsets: &[usize],
    mtype: MetricType,
    unit: &[u8],
    enable_type_and_unit_labels: bool,
    builder: &mut ScratchBuilder,
    sc: &mut LabelScratch,
    l: &mut Labels,
) {
    builder.reset();
    let LabelScratch { mname, name, value, name_s, value_s, float } = sc;
    let whole = std::str::from_utf8(&b[series.0..series.1]).ok();
    let s0 = series.0;
    let type_set = mtype != MetricType::Empty && mtype != MetricType::Unknown;
    let metric_name_empty;
    let unit_empty;
    {
        let metric_name = unreplace_str(part(b, whole, s0, offsets[0], offsets[1], name_s), mname);
        let unit = lossy(unit, value_s);
        metric_name_empty = metric_name.is_empty();
        unit_empty = unit.is_empty();
        if enable_type_and_unit_labels {
            if !metric_name_empty {
                builder.add("__name__", metric_name);
            }
            if type_set {
                builder.add("__type__", mtype.as_str());
            }
            if !unit_empty {
                builder.add("__unit__", unit);
            }
        } else {
            builder.add("__name__", metric_name);
        }
    }
    let mut i = 2;
    while i < offsets.len() {
        let label = unreplace_str(part(b, whole, s0, offsets[i], offsets[i + 1], name_s), name);
        if enable_type_and_unit_labels {
            let is_empty_for = match label {
                "__name__" => metric_name_empty,
                "__type__" => !type_set,
                "__unit__" => unit_empty,
                _ => true,
            };
            if !is_empty_for {
                i += 4;
                continue;
            }
        }
        let v = unreplace_str(part(b, whole, s0, offsets[i + 2], offsets[i + 3], value_s), value);
        let v = crate::normalize_floats_in_label_values(mtype, label, v, float);
        builder.add(label, v);
        i += 4;
    }
    builder.sort();
    builder.overwrite(l);
}

/// Checks the label values in `offsets[from..]` (name and value pairs) in order, like Go does when it reads each
/// value token. On the first invalid one returns the Go error and the token range `(start, end)` of that value
/// (the quotes and, as the lexer skips them, the null bytes after the closing quote).
pub(crate) fn check_label_values(b: &[u8], offsets: &[usize], from: usize, skip_nulls: bool) -> Option<(Error, usize, usize)> {
    let mut i = from;
    while i + 3 < offsets.len() {
        let (vs, ve) = (offsets[i + 2], offsets[i + 3]);
        if std::str::from_utf8(&b[vs..ve]).is_err() {
            let start = vs - 1;
            let mut end = ve + 1;
            while skip_nulls && end < b.len() && b[end] == 0 {
                end += 1;
            }
            let mut m = String::from("invalid UTF-8 label value: ");
            append_quote_bytes(&mut m, &b[start..end]);
            return Some((Error::Msg(m), start, end));
        }
        i += 4;
    }
    None
}

/// Go `parseFloat` of the text parsers: rejects `p`, `P` and `_` (pre-Go 1.13 float formats only).
#[inline]
pub(crate) fn text_parse_float(s: &[u8], line: &[u8]) -> Result<f64, Error> {
    if s.iter().any(|&c| c == b'p' || c == b'P' || c == b'_') {
        let mut m = String::from("unsupported character in float while parsing: ");
        append_quote_bytes(&mut m, line);
        return Err(Error::Msg(m));
    }
    strconv::parse_float(s).map_err(|e| {
        let mut m = e.num_error(s).to_string();
        m.push_str(" while parsing: ");
        append_quote_bytes(&mut m, line);
        Error::Msg(m)
    })
}

/// Parses samples in the Prometheus text exposition format (Go `PromParser`).
pub struct PromParser<'a> {
    b: Cow<'a, [u8]>,
    l: PromLexer,
    builder: ScratchBuilder,
    series: (usize, usize),
    text: (usize, usize),
    help_buf: Vec<u8>,
    mtype: MetricType,
    val: f64,
    ts: i64,
    has_ts: bool,
    start: usize,
    offsets: Vec<usize>,
    sc: LabelScratch,
    enable_type_and_unit_labels: bool,
}

impl<'a> PromParser<'a> {
    /// Go `NewPromParser`. Input without a trailing newline is copied once to add it.
    pub fn new(b: &'a [u8], enable_type_and_unit_labels: bool) -> PromParser<'a> {
        let b = if b.last() == Some(&b'\n') {
            Cow::Borrowed(b)
        } else {
            let mut v = Vec::with_capacity(b.len() + 1);
            v.extend_from_slice(b);
            v.push(b'\n');
            Cow::Owned(v)
        };
        PromParser {
            b,
            l: PromLexer::default(),
            builder: ScratchBuilder::new(16),
            series: (0, 0),
            text: (0, 0),
            help_buf: Vec::new(),
            mtype: MetricType::Empty,
            val: 0.0,
            ts: 0,
            has_ts: false,
            start: 0,
            offsets: Vec::with_capacity(32),
            sc: LabelScratch::default(),
            enable_type_and_unit_labels,
        }
    }

    #[inline]
    fn next_token(&mut self) -> Token {
        loop {
            let t = self.l.lex(&self.b);
            if t != Token::Whitespace {
                return t;
            }
        }
    }

    fn parse_error(&self, exp: &str, got: Token) -> Error {
        parse_error(&self.b, self.l.start, self.l.i, self.start, exp, got)
    }

    /// Go `parseLVals`. Label values are UTF-8 checked together after the label set (one check of the whole
    /// set when it is valid); an invalid value gives Go's error and lexer position as if checked on reading it.
    fn parse_lvals(&mut self) -> Result<(), Error> {
        let from = self.offsets.len();
        self.l.hi = 0;
        let r = self.parse_lvals_tokens();
        if r.is_ok() && (self.l.hi < 0x80 || std::str::from_utf8(&self.b[self.start..self.l.i]).is_ok()) {
            return r;
        }
        if let Some((e, start, end)) = check_label_values(&self.b, &self.offsets, from, true) {
            self.l.start = start;
            self.l.i = end;
            self.l.state = S_LABELS;
            return Err(e);
        }
        r
    }

    fn parse_lvals_tokens(&mut self) -> Result<(), Error> {
        let mut t = self.next_token();
        loop {
            let mut cur_start = self.l.start;
            let mut cur_i = self.l.i;
            let is_qstring = match t {
                Token::BraceClose => return Ok(()),
                Token::LName => false,
                Token::QString => true,
                _ => return Err(self.parse_error("expected label name", t)),
            };
            t = self.next_token();
            if is_qstring && (t == Token::Comma || t == Token::BraceClose) {
                if self.offsets[0] != NONE || self.offsets[1] != NONE {
                    let mut m = String::from("metric name already set while parsing: ");
                    append_quote_bytes(&mut m, &self.b[self.start..self.l.i]);
                    return Err(Error::Msg(m));
                }
                self.offsets[0] = cur_start + 1;
                self.offsets[1] = cur_i - 1;
                if t == Token::BraceClose {
                    return Ok(());
                }
                t = self.next_token();
                continue;
            }
            if self.b[cur_start] == b'"' {
                cur_start += 1;
                cur_i -= 1;
            }
            self.offsets.push(cur_start);
            self.offsets.push(cur_i);
            if t != Token::Equal {
                return Err(self.parse_error("expected equal", t));
            }
            let t2 = self.next_token();
            if t2 != Token::LValue {
                return Err(self.parse_error("expected label value", t2));
            }
            // The value is checked for valid UTF-8 in `parse_lvals` (all values at once, same result as Go).
            self.offsets.push(self.l.start + 1);
            self.offsets.push(self.l.i - 1);
            t = self.next_token();
            if t == Token::Comma {
                t = self.next_token();
            }
        }
    }

    fn parse_metric_suffix(&mut self, t: Token) -> Result<Entry, Error> {
        if self.offsets[0] == NONE {
            let mut m = String::from("metric name not set while parsing: ");
            append_quote_bytes(&mut m, &self.b[self.start..self.l.i]);
            return Err(Error::Msg(m));
        }
        if t != Token::Value {
            return Err(self.parse_error("expected value after metric", t));
        }
        self.val = text_parse_float(&self.b[self.l.start..self.l.i], &self.b[self.start..self.l.i])?;
        if self.val.is_nan() {
            self.val = model::value::normal_nan();
        }
        self.has_ts = false;
        match self.next_token() {
            Token::Linebreak => {}
            Token::Timestamp => {
                self.has_ts = true;
                match strconv::parse_int(&self.b[self.l.start..self.l.i]) {
                    Ok(v) => self.ts = v,
                    Err(e) => {
                        let mut m = e.to_string();
                        m.push_str(" while parsing: ");
                        append_quote_bytes(&mut m, &self.b[self.start..self.l.i]);
                        return Err(Error::Msg(m));
                    }
                }
                let t2 = self.next_token();
                if t2 != Token::Linebreak {
                    return Err(self.parse_error("expected next entry after timestamp", t2));
                }
            }
            t => return Err(self.parse_error("expected timestamp or new record", t)),
        }
        Ok(Entry::Series)
    }
}

impl Parser for PromParser<'_> {
    fn series(&self) -> (&[u8], Option<i64>, f64) {
        let s = &self.b[self.series.0..self.series.1];
        (s, if self.has_ts { Some(self.ts) } else { None }, self.val)
    }

    fn histogram(&mut self) -> (&[u8], Option<i64>, Option<&Histogram>, Option<&FloatHistogram>) {
        (&[], None, None, None)
    }

    fn help(&mut self) -> (&[u8], &[u8]) {
        let m = &self.b[self.offsets[0]..self.offsets[1]];
        let text = &self.b[self.text.0..self.text.1];
        if crate::memchr(b'\\', text).is_some() {
            crate::replace_escapes(text, &mut self.help_buf, false);
            return (m, &self.help_buf);
        }
        (m, text)
    }

    fn typ(&self) -> (&[u8], MetricType) {
        (&self.b[self.offsets[0]..self.offsets[1]], self.mtype)
    }

    fn unit(&self) -> (&[u8], &[u8]) {
        (&[], &[])
    }

    fn comment(&self) -> &[u8] {
        &self.b[self.text.0..self.text.1]
    }

    fn labels(&mut self, l: &mut Labels) {
        build_text_labels(
            &self.b,
            self.series,
            &self.offsets,
            self.mtype,
            b"",
            self.enable_type_and_unit_labels,
            &mut self.builder,
            &mut self.sc,
            l,
        );
    }

    fn exemplar(&mut self, _e: &mut Exemplar) -> bool {
        false
    }

    fn start_timestamp(&mut self) -> i64 {
        0
    }

    fn next(&mut self) -> Result<Entry, Error> {
        loop {
            self.start = self.l.i;
            self.offsets.clear();
            let t = self.next_token();
            match t {
                Token::Eof => return Err(Error::Eof),
                Token::Linebreak => continue,
                Token::Help | Token::Type => {
                    let t2 = self.next_token();
                    if t2 != Token::MName {
                        return Err(self.parse_error(&format!("expected metric name after {}", t.as_str()), t2));
                    }
                    let (mut ms, mut me) = (self.l.start, self.l.i);
                    if self.b[ms] == b'"' && self.b[me - 1] == b'"' {
                        ms += 1;
                        me -= 1;
                    }
                    self.offsets.push(ms);
                    self.offsets.push(me);
                    let t2 = self.next_token();
                    if t2 != Token::Text {
                        return Err(Error::Msg(format!("expected text in {}, got {}", t.as_str(), t2.as_str())));
                    }
                    self.text = if self.l.i - self.l.start > 1 { (self.l.start + 1, self.l.i) } else { (0, 0) };
                    let text = &self.b[self.text.0..self.text.1];
                    if t == Token::Type {
                        self.mtype = match text {
                            b"counter" => MetricType::Counter,
                            b"gauge" => MetricType::Gauge,
                            b"histogram" => MetricType::Histogram,
                            b"summary" => MetricType::Summary,
                            b"untyped" => MetricType::Unknown,
                            _ => {
                                let mut m = String::from("invalid metric type ");
                                append_quote_bytes(&mut m, text);
                                return Err(Error::Msg(m));
                            }
                        };
                    } else if std::str::from_utf8(text).is_err() {
                        let mut m = String::from("help text ");
                        append_quote_bytes(&mut m, text);
                        m.push_str(" is not a valid utf8 string");
                        return Err(Error::Msg(m));
                    }
                    let t3 = self.next_token();
                    if t3 != Token::Linebreak {
                        return Err(self.parse_error("linebreak expected after metadata", t3));
                    }
                    return Ok(if t == Token::Help { Entry::Help } else { Entry::Type });
                }
                Token::Comment => {
                    self.text = (self.l.start, self.l.i);
                    let t2 = self.next_token();
                    if t2 != Token::Linebreak {
                        return Err(self.parse_error("linebreak expected after comment", t2));
                    }
                    return Ok(Entry::Comment);
                }
                Token::BraceOpen => {
                    if self.offsets.is_empty() {
                        self.offsets.push(NONE);
                        self.offsets.push(NONE);
                    }
                    self.parse_lvals()?;
                    self.series = (self.start, self.l.i);
                    let t2 = self.next_token();
                    return self.parse_metric_suffix(t2);
                }
                Token::MName => {
                    self.offsets.push(self.start);
                    self.offsets.push(self.l.i);
                    self.series = (self.start, self.l.i);
                    let mut t2 = self.next_token();
                    if t2 == Token::BraceOpen {
                        self.parse_lvals()?;
                        self.series = (self.start, self.l.i);
                        t2 = self.next_token();
                    }
                    return self.parse_metric_suffix(t2);
                }
                _ => return Err(self.parse_error("expected a valid start token", t)),
            }
        }
    }

    fn next_if_series(&mut self, series: &[u8]) -> Result<bool, Error> {
        let b = &self.b[..];
        let i = self.l.i;
        let end = i + series.len();
        if self.l.state != S_INIT || series.is_empty() || !is_m(series[0]) || end + 1 >= b.len() || b[i..end] != *series {
            return Ok(false);
        }
        // Lexing the same bytes from the start of a line gives the same tokens; they must end exactly at `end`.
        let name_end = lexutil::scan_while(series, 1, lexutil::MD);
        if name_end == series.len() {
            // A name alone: labels may still follow after whitespace; accept only a space and then the value.
            if b[end] != b' ' || matches!(b[end + 1], 0 | b' ' | b'\t' | b'\n' | b'{') {
                return Ok(false);
            }
        } else if series[name_end] != b'{' || series[series.len() - 1] != b'}' {
            return Ok(false);
        }
        // The state of `next` after the series (label offsets are not filled in).
        self.start = i;
        self.offsets.clear();
        self.offsets.push(i);
        self.offsets.push(i + name_end);
        self.series = (i, end);
        self.l.i = end;
        self.l.state = S_VALUE;
        let t = self.next_token();
        self.parse_metric_suffix(t)?;
        Ok(true)
    }

    fn kind(&self) -> ParserKind {
        ParserKind::Prom
    }
}
