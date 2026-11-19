//! Port of `openmetricsparse.go` and its lexer `openmetricslex.l` (the golex DFA written by hand; like golex it
//! never backtracks: a state without a transition aborts with an invalid token).

use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{xxhash, Labels, ScratchBuilder};
use model::metadata::MetricType;

use crate::promparse::{build_text_labels, check_label_values, lossy, text_parse_float, LabelScratch};
use crate::strconv::{append_quote_bytes, format_f, go_f64_to_i64};
use crate::lexutil;
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

/// `[^ \n]` as golex compiles it (also excluding \0).
#[inline(always)]
fn is_vc(c: u8) -> bool {
    !matches!(c, 0 | b'\n' | b' ')
}

/// The OpenMetrics lexer (Go `openMetricsLexer`).
#[derive(Clone, Copy, Default)]
pub(crate) struct OmLexer {
    pub(crate) i: usize,
    pub(crate) start: usize,
    pub(crate) state: u8,
    /// OR of the bytes of the quoted strings scanned since it was reset (>= 0x80: maybe not ASCII).
    pub(crate) hi: u8,
}

impl OmLexer {
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

    /// Quoted string after the opening quote. `newline` allows unescaped newlines. Returns false on abort.
    #[inline]
    fn quoted(&mut self, b: &[u8], newline: bool) -> bool {
        match lexutil::scan_quoted(b, self.i, self.state == S_LVALUE, newline, &mut self.hi) {
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

    /// Literal bytes after the first (already matched) one.
    #[inline]
    fn word(&mut self, b: &[u8], rest: &[u8]) -> bool {
        rest.iter().all(|&w| self.next(b) == w)
    }

    /// `{S}[^ \n]+` after the space was seen as `c`'s predecessor: the first byte must match.
    #[inline]
    fn space_value(&mut self, b: &[u8]) -> bool {
        if !is_vc(self.next(b)) {
            return false;
        }
        while is_vc(self.next(b)) {}
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
                S_INIT => match c {
                    b'#' => {
                        if self.next(b) != b' ' {
                            return Token::Invalid;
                        }
                        c = self.next(b);
                        self.state = S_COMMENT;
                        continue;
                    }
                    c if is_m(c) => {
                        self.i = lexutil::scan_while(b, self.i + 1, lexutil::MD);
                        self.state = S_VALUE;
                        return Token::MName;
                    }
                    b'{' => {
                        self.next(b);
                        self.state = S_LABELS;
                        return Token::BraceOpen;
                    }
                    _ => return Token::Invalid,
                },
                S_COMMENT => {
                    return match c {
                        b'E' => {
                            if !self.word(b, b"OF") {
                                return Token::Invalid;
                            }
                            if self.next(b) == b'\n' {
                                self.next(b);
                            }
                            self.state = S_INIT;
                            Token::EofWord
                        }
                        b'H' | b'T' | b'U' => {
                            let (rest, tok): (&[u8], Token) = match c {
                                b'H' => (b"ELP ", Token::Help),
                                b'T' => (b"YPE ", Token::Type),
                                _ => (b"NIT ", Token::Unit),
                            };
                            if !self.word(b, rest) {
                                return Token::Invalid;
                            }
                            self.next(b);
                            self.state = S_META1;
                            tok
                        }
                        _ => Token::Invalid,
                    };
                }
                S_META1 => {
                    return match c {
                        b'"' => {
                            if !self.quoted(b, true) {
                                return Token::Invalid;
                            }
                            self.state = S_META2;
                            Token::MName
                        }
                        c if is_m(c) => {
                            self.i = lexutil::scan_while(b, self.i + 1, lexutil::MD);
                            self.state = S_META2;
                            Token::MName
                        }
                        _ => Token::Invalid,
                    };
                }
                S_META2 => {
                    if c != b' ' {
                        return Token::Invalid;
                    }
                    // next() skips null bytes in sMeta2, so only \n or the end stops the text.
                    self.i = lexutil::find(b, self.i + 1, b'\n');
                    if self.i >= b.len() {
                        return Token::Invalid;
                    }
                    self.next(b);
                    self.state = S_INIT;
                    return Token::Text;
                }
                S_LABELS | S_EXEMPLAR => {
                    let ex = self.state == S_EXEMPLAR;
                    return match c {
                        b'"' => {
                            if !self.quoted(b, !ex) {
                                return Token::Invalid;
                            }
                            Token::QString
                        }
                        b',' => {
                            self.next(b);
                            Token::Comma
                        }
                        b'=' => {
                            self.next(b);
                            self.state = if ex { S_EVALUE } else { S_LVALUE };
                            Token::Equal
                        }
                        b'}' => {
                            self.next(b);
                            self.state = if ex { S_EVALUE } else { S_VALUE };
                            Token::BraceClose
                        }
                        c if is_l(c) => {
                            self.i = lexutil::scan_while(b, self.i + 1, lexutil::LD);
                            Token::LName
                        }
                        _ => Token::Invalid,
                    };
                }
                S_LVALUE => {
                    if c != b'"' || !self.quoted(b, false) {
                        return Token::Invalid;
                    }
                    self.state = S_LABELS;
                    return Token::LValue;
                }
                S_VALUE => {
                    return match c {
                        b' ' => {
                            if !self.space_value(b) {
                                return Token::Invalid;
                            }
                            self.state = S_TIMESTAMP;
                            Token::Value
                        }
                        b'{' => {
                            self.next(b);
                            self.state = S_LABELS;
                            Token::BraceOpen
                        }
                        _ => Token::Invalid,
                    };
                }
                S_TIMESTAMP => {
                    return match c {
                        b' ' => {
                            c = self.next(b);
                            if c == b'#' {
                                c = self.next(b);
                                if c == b' ' {
                                    if self.next(b) != b'{' {
                                        return Token::Invalid;
                                    }
                                    self.next(b);
                                    self.state = S_EXEMPLAR;
                                    return Token::Comment;
                                }
                                if is_vc(c) {
                                    while is_vc(self.next(b)) {}
                                }
                                return Token::Timestamp;
                            }
                            if !is_vc(c) {
                                return Token::Invalid;
                            }
                            while is_vc(self.next(b)) {}
                            Token::Timestamp
                        }
                        b'\n' => {
                            self.next(b);
                            self.state = S_INIT;
                            Token::Linebreak
                        }
                        _ => Token::Invalid,
                    };
                }
                S_EVALUE => {
                    return match c {
                        b' ' => {
                            if !self.space_value(b) {
                                return Token::Invalid;
                            }
                            self.state = S_ETIMESTAMP;
                            Token::Value
                        }
                        b'"' => {
                            if !self.quoted(b, false) {
                                return Token::Invalid;
                            }
                            self.state = S_EXEMPLAR;
                            Token::LValue
                        }
                        _ => Token::Invalid,
                    };
                }
                S_ETIMESTAMP => {
                    return match c {
                        b' ' => {
                            if !self.space_value(b) {
                                return Token::Invalid;
                            }
                            Token::Timestamp
                        }
                        b'\n' => {
                            self.next(b);
                            self.state = S_INIT;
                            Token::Linebreak
                        }
                        _ => Token::Invalid,
                    };
                }
                _ => panic!("invalid start condition {}", self.state),
            }
        }
    }
}

/// Options of [`OpenMetricsParser`] (Go `openMetricsParserOptions`).
#[derive(Clone, Copy, Debug, Default)]
pub struct OpenMetricsOptions {
    /// Do not expose `_created` lines as series; they are then only used by `start_timestamp`.
    pub skip_st_series: bool,
    /// Inject `__type__` and `__unit__` labels.
    pub enable_type_and_unit_labels: bool,
}

/// Parses the OpenMetrics 1.0 text format (Go `OpenMetricsParser`).
pub struct OpenMetricsParser<'a> {
    b: &'a [u8],
    l: OmLexer,
    builder: ScratchBuilder,
    series: (usize, usize),
    mf_name_len: usize,
    text: (usize, usize),
    help_buf: Vec<u8>,
    mtype: MetricType,
    unit: Vec<u8>,
    val: f64,
    ts: i64,
    has_ts: bool,
    start: usize,
    offsets: Vec<usize>,
    e_offsets: Vec<usize>,
    exemplar: (usize, usize),
    exemplar_val: f64,
    exemplar_ts: i64,
    has_exemplar_ts: bool,
    st: i64,
    st_hash_set: u64,
    ignore_exemplar: bool,
    skip_st_series: bool,
    enable_type_and_unit_labels: bool,
    sc: LabelScratch,
    hash_buf: Vec<u8>,
}

impl<'a> OpenMetricsParser<'a> {
    /// Go `NewOpenMetricsParser`.
    pub fn new(b: &'a [u8], opts: OpenMetricsOptions) -> OpenMetricsParser<'a> {
        OpenMetricsParser {
            b,
            l: OmLexer::default(),
            builder: ScratchBuilder::new(16),
            series: (0, 0),
            mf_name_len: 0,
            text: (0, 0),
            help_buf: Vec::new(),
            mtype: MetricType::Empty,
            unit: Vec::new(),
            val: 0.0,
            ts: 0,
            has_ts: false,
            start: 0,
            offsets: Vec::with_capacity(32),
            e_offsets: Vec::new(),
            exemplar: (0, 0),
            exemplar_val: 0.0,
            exemplar_ts: 0,
            has_exemplar_ts: false,
            st: 0,
            st_hash_set: 0,
            ignore_exemplar: false,
            skip_st_series: opts.skip_st_series,
            enable_type_and_unit_labels: opts.enable_type_and_unit_labels,
            sc: LabelScratch::default(),
            hash_buf: Vec::new(),
        }
    }

    #[inline]
    fn next_token(&mut self) -> Token {
        self.l.lex(self.b)
    }

    fn parse_error(&self, exp: &str, got: Token) -> Error {
        parse_error(self.b, self.l.start, self.l.i, self.start, exp, got)
    }

    /// Series bytes relative slicing (Go `p.series[x-p.start:y-p.start]`), clamped instead of panicking.
    fn series_slice(&self, from: usize, to: usize) -> &'a [u8] {
        let s = &self.b[self.series.0..self.series.1];
        let to = to.min(s.len());
        &s[from.min(to)..to]
    }

    fn series_hash(&mut self, mf_name: &[u8]) -> u64 {
        let mut buf = std::mem::take(&mut self.hash_buf);
        buf.clear();
        let mut i = 2;
        while i < self.offsets.len() {
            let label = &self.b[self.offsets[i]..self.offsets[i + 1]];
            if (self.mtype == MetricType::Summary && label == b"quantile")
                || (self.mtype == MetricType::Histogram && label == b"le")
            {
                i += 4;
                continue;
            }
            buf.extend_from_slice(label);
            buf.extend_from_slice(&self.b[self.offsets[i + 2]..self.offsets[i + 3]]);
            i += 4;
        }
        buf.extend_from_slice(mf_name);
        let h = xxhash::sum64(&buf);
        buf.clear();
        self.hash_buf = buf;
        h
    }

    fn reset_st_parse_values(&mut self) {
        self.st_hash_set = 0;
        self.skip_st_series = true;
    }

    fn is_created_series(&self) -> bool {
        let name = &self.b[self.offsets[0]..self.offsets[1]];
        type_requires_st(self.mtype) && name.len() >= 8 && name.ends_with(b"_created")
    }

    fn get_float_value(&self, t: Token, after: &str) -> Result<f64, Error> {
        if t != Token::Value {
            return Err(self.parse_error(&format!("expected value after {after}"), t));
        }
        let v = text_parse_float(&self.b[self.l.start + 1..self.l.i], &self.b[self.start..self.l.i])?;
        Ok(if v.is_nan() { model::value::normal_nan() } else { v })
    }

    fn parse_lvals(&mut self, is_exemplar: bool) -> Result<(), Error> {
        let mut offsets = std::mem::take(if is_exemplar { &mut self.e_offsets } else { &mut self.offsets });
        let from = if is_exemplar { 0 } else { offsets.len() };
        self.l.hi = 0;
        let mut r = self.parse_lvals_into(&mut offsets, is_exemplar);
        if (r.is_err() || (self.l.hi >= 0x80 && std::str::from_utf8(&self.b[self.start..self.l.i]).is_err()))
            && let Some((e, start, end)) = check_label_values(self.b, &offsets, from, !is_exemplar) {
                self.l.start = start;
                self.l.i = end;
                self.l.state = if is_exemplar { S_EXEMPLAR } else { S_LABELS };
                r = Err(e);
            }
        if r.is_err() {
            offsets.clear();
        }
        if is_exemplar {
            self.e_offsets = offsets;
        } else {
            self.offsets = offsets;
        }
        r
    }

    fn parse_lvals_into(&mut self, offsets: &mut Vec<usize>, is_exemplar: bool) -> Result<(), Error> {
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
                if is_exemplar {
                    return Err(self.parse_error("expected label name", t));
                }
                if offsets[0] != NONE || offsets[1] != NONE {
                    let mut m = String::from("metric name already set while parsing: ");
                    append_quote_bytes(&mut m, &self.b[self.start..self.l.i]);
                    return Err(Error::Msg(m));
                }
                offsets[0] = cur_start + 1;
                offsets[1] = cur_i - 1;
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
            offsets.push(cur_start);
            offsets.push(cur_i);
            if t != Token::Equal {
                return Err(self.parse_error("expected equal", t));
            }
            let t2 = self.next_token();
            if t2 != Token::LValue {
                return Err(self.parse_error("expected label value", t2));
            }
            // The value is checked for valid UTF-8 in `parse_lvals` (all values at once, same result as Go).
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

    fn parse_comment(&mut self) -> Result<(), Error> {
        if self.ignore_exemplar {
            loop {
                match self.next_token() {
                    Token::Linebreak => return Ok(()),
                    Token::Eof => return Err(Error::msg("data does not end with # EOF")),
                    // adapted: Go loops forever here because an invalid token does not advance the lexer.
                    Token::Invalid => return Err(Error::msg("data does not end with # EOF")),
                    _ => {}
                }
            }
        }
        self.parse_lvals(true)?;
        self.exemplar = (self.start, self.l.i);
        let t = self.next_token();
        self.exemplar_val = self.get_float_value(t, "exemplar labels")?;
        self.has_exemplar_ts = false;
        match self.next_token() {
            Token::Eof => return Err(Error::msg("data does not end with # EOF")),
            Token::Linebreak => {}
            Token::Timestamp => {
                self.has_exemplar_ts = true;
                let ts = text_parse_float(&self.b[self.l.start + 1..self.l.i], &self.b[self.start..self.l.i])?;
                if ts.is_nan() || ts.is_infinite() {
                    return Err(Error::Msg(format!("invalid exemplar timestamp {}", format_f(ts))));
                }
                self.exemplar_ts = go_f64_to_i64(ts * 1000.0);
                let t3 = self.next_token();
                if t3 != Token::Linebreak {
                    return Err(self.parse_error("expected next entry after exemplar timestamp", t3));
                }
            }
            t2 => return Err(self.parse_error("expected timestamp or comment", t2)),
        }
        Ok(())
    }

    fn parse_series_end_of_line(&mut self, t: Token) -> Result<(), Error> {
        if self.offsets[0] == NONE {
            let mut m = String::from("metric name not set while parsing: ");
            append_quote_bytes(&mut m, &self.b[self.start..self.l.i]);
            return Err(Error::Msg(m));
        }
        self.val = self.get_float_value(t, "metric")?;
        self.has_ts = false;
        match self.next_token() {
            Token::Eof => return Err(Error::msg("data does not end with # EOF")),
            Token::Linebreak => {}
            Token::Comment => self.parse_comment()?,
            Token::Timestamp => {
                self.has_ts = true;
                let ts = text_parse_float(&self.b[self.l.start + 1..self.l.i], &self.b[self.start..self.l.i])?;
                if ts.is_nan() || ts.is_infinite() {
                    return Err(Error::Msg(format!("invalid timestamp {}", format_f(ts))));
                }
                self.ts = go_f64_to_i64(ts * 1000.0);
                match self.next_token() {
                    Token::Linebreak => {}
                    Token::Comment => self.parse_comment()?,
                    t3 => return Err(self.parse_error("expected next entry after timestamp", t3)),
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// Go `typeRequiresST`.
fn type_requires_st(t: MetricType) -> bool {
    matches!(t, MetricType::Counter | MetricType::Summary | MetricType::Histogram)
}

impl Parser for OpenMetricsParser<'_> {
    fn series(&self) -> (&[u8], Option<i64>, f64) {
        (&self.b[self.series.0..self.series.1], if self.has_ts { Some(self.ts) } else { None }, self.val)
    }

    fn histogram(&mut self) -> (&[u8], Option<i64>, Option<&Histogram>, Option<&FloatHistogram>) {
        (&[], None, None, None)
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
        build_text_labels(
            self.b,
            self.series,
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
        if self.exemplar.1 <= self.exemplar.0 {
            return false;
        }
        e.value = self.exemplar_val;
        if self.has_exemplar_ts {
            e.has_ts = true;
            e.ts = self.exemplar_ts;
        }
        self.builder.reset();
        let mut i = 0;
        while i + 3 < self.e_offsets.len() {
            let o = &self.e_offsets[i..i + 4];
            let n = lossy(&self.b[o[0]..o[1]], &mut self.sc.name_s);
            let v = lossy(&self.b[o[2]..o[3]], &mut self.sc.value_s);
            self.builder.add(n, v);
            i += 4;
        }
        self.builder.sort();
        self.builder.overwrite(&mut e.labels);
        self.exemplar = (0, 0);
        true
    }

    fn start_timestamp(&mut self) -> i64 {
        if !type_requires_st(self.mtype) {
            self.st_hash_set = 0;
            return 0;
        }
        let s = &self.b[self.series.0..self.series.1];
        let o0 = self.offsets[0].wrapping_sub(self.start);
        let curr_name = if s.len() > 1 && s[0] == b'{' && s[1] == b'"' {
            self.series_slice(o0, self.mf_name_len + 2)
        } else {
            self.series_slice(o0, self.mf_name_len)
        };
        let curr_hash = self.series_hash(curr_name);
        if curr_hash == self.st_hash_set && self.st > 0 {
            return self.st;
        }

        let reset_lexer = self.l;
        let reset_start = self.start;
        let reset_mtype = self.mtype;
        self.skip_st_series = false;
        self.ignore_exemplar = true;

        let result = loop {
            match self.next() {
                Ok(Entry::Series) => {}
                _ => {
                    self.reset_st_parse_values();
                    break 0;
                }
            }
            let peeked = &self.b[self.offsets[0]..self.offsets[1]];
            if peeked.len() < 8 || !peeked.ends_with(b"_created") {
                continue;
            }
            let peeked_hash = self.series_hash(&peeked[..peeked.len() - 8]);
            if peeked_hash != curr_hash {
                self.reset_st_parse_values();
                break 0;
            }
            let st = go_f64_to_i64(self.val * 1000.0);
            self.st = st;
            self.st_hash_set = curr_hash;
            self.skip_st_series = true;
            break st;
        };

        self.l = reset_lexer;
        self.start = reset_start;
        self.mtype = reset_mtype;
        self.ignore_exemplar = false;
        result
    }

    fn next(&mut self) -> Result<Entry, Error> {
        loop {
            self.start = self.l.i;
            self.offsets.clear();
            if !self.ignore_exemplar {
                self.e_offsets.clear();
                self.exemplar = (0, 0);
                self.exemplar_val = 0.0;
                self.has_exemplar_ts = false;
            }
            let t = self.next_token();
            match t {
                Token::EofWord => {
                    if self.next_token() != Token::Eof {
                        return Err(Error::msg("unexpected data after # EOF"));
                    }
                    return Err(Error::Eof);
                }
                Token::Eof => return Err(Error::msg("data does not end with # EOF")),
                Token::Help | Token::Type | Token::Unit => {
                    let t2 = self.next_token();
                    if t2 != Token::MName {
                        return Err(self.parse_error(&format!("expected metric name after {}", t.as_str()), t2));
                    }
                    let (mut ms, mut me) = (self.l.start, self.l.i);
                    if self.b[ms] == b'"' && self.b[me - 1] == b'"' {
                        ms += 1;
                        me -= 1;
                    }
                    self.mf_name_len = me - ms;
                    self.offsets.push(ms);
                    self.offsets.push(me);
                    if self.next_token() != Token::Text {
                        return Err(Error::Msg(format!("expected text in {}", t.as_str())));
                    }
                    self.text =
                        if self.l.i - self.l.start > 1 { (self.l.start + 1, self.l.i - 1) } else { (0, 0) };
                    let text = &self.b[self.text.0..self.text.1];
                    match t {
                        Token::Type => {
                            self.mtype = match text {
                                b"counter" => MetricType::Counter,
                                b"gauge" => MetricType::Gauge,
                                b"histogram" => MetricType::Histogram,
                                b"gaugehistogram" => MetricType::GaugeHistogram,
                                b"summary" => MetricType::Summary,
                                b"info" => MetricType::Info,
                                b"stateset" => MetricType::StateSet,
                                b"unknown" => MetricType::Unknown,
                                _ => {
                                    let mut m = String::from("invalid metric type ");
                                    append_quote_bytes(&mut m, text);
                                    return Err(Error::Msg(m));
                                }
                            };
                            return Ok(Entry::Type);
                        }
                        Token::Help => {
                            if std::str::from_utf8(text).is_err() {
                                let mut m = String::from("help text ");
                                append_quote_bytes(&mut m, text);
                                m.push_str(" is not a valid utf8 string");
                                return Err(Error::Msg(m));
                            }
                            return Ok(Entry::Help);
                        }
                        _ => {
                            self.unit.clear();
                            self.unit.extend_from_slice(text);
                            let m = &self.b[self.offsets[0]..self.offsets[1]];
                            let u = &self.unit;
                            if !u.is_empty()
                                && (!m.ends_with(u) || m.len() < u.len() + 1 || self.b[self.offsets[1] - u.len() - 1] != b'_')
                            {
                                let mut e = String::from("unit ");
                                append_quote_bytes(&mut e, u);
                                e.push_str(" not a suffix of metric ");
                                append_quote_bytes(&mut e, m);
                                return Err(Error::Msg(e));
                            }
                            return Ok(Entry::Unit);
                        }
                    }
                }
                Token::BraceOpen => {
                    self.offsets.push(NONE);
                    self.offsets.push(NONE);
                    self.parse_lvals(false)?;
                    self.series = (self.start, self.l.i);
                    let t2 = self.next_token();
                    self.parse_series_end_of_line(t2)?;
                    if self.skip_st_series && self.is_created_series() {
                        continue;
                    }
                    return Ok(Entry::Series);
                }
                Token::MName => {
                    self.offsets.push(self.start);
                    self.offsets.push(self.l.i);
                    self.series = (self.start, self.l.i);
                    let mut t2 = self.next_token();
                    if t2 == Token::BraceOpen {
                        self.parse_lvals(false)?;
                        self.series = (self.start, self.l.i);
                        t2 = self.next_token();
                    }
                    self.parse_series_end_of_line(t2)?;
                    if self.skip_st_series && self.is_created_series() {
                        continue;
                    }
                    return Ok(Entry::Series);
                }
                _ => return Err(self.parse_error("expected a valid start token", t)),
            }
        }
    }

    fn next_if_series(&mut self, series: &[u8]) -> Result<bool, Error> {
        let b = self.b;
        let i = self.l.i;
        let end = i + series.len();
        if self.l.state != S_INIT
            || self.ignore_exemplar
            || series.is_empty()
            || !is_m(series[0])
            || end >= b.len()
            || b[i..end] != *series
        {
            return Ok(false);
        }
        // Lexing the same bytes from the start of a line gives the same tokens; they must end exactly at `end`.
        let name_end = lexutil::scan_while(series, 1, lexutil::MD);
        if name_end == series.len() {
            if lexutil::is(b[end], lexutil::MD) {
                return Ok(false);
            }
        } else if series[name_end] != b'{' || series[series.len() - 1] != b'}' {
            return Ok(false);
        }
        // `next` skips `_created` series of some types; leave those to it.
        let name = &series[..name_end];
        if self.skip_st_series && type_requires_st(self.mtype) && name.len() >= 8 && name.ends_with(b"_created") {
            return Ok(false);
        }
        // The state of `next` after the series (label offsets are not filled in).
        self.start = i;
        self.offsets.clear();
        self.e_offsets.clear();
        self.exemplar = (0, 0);
        self.exemplar_val = 0.0;
        self.has_exemplar_ts = false;
        self.offsets.push(i);
        self.offsets.push(i + name_end);
        self.series = (i, end);
        self.l.i = end;
        self.l.state = S_VALUE;
        let t = self.next_token();
        self.parse_series_end_of_line(t)?;
        Ok(true)
    }

    fn kind(&self) -> ParserKind {
        ParserKind::OpenMetrics
    }
}
