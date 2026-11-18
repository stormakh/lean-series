//! Port of `promql/parser/lex.go`: the PromQL lexer.

use std::borrow::Cow;
use std::fmt;

use super::generated::*;
use super::posrange::{Pos, PositionRange};
use super::quote::{go_quote, go_quote_rune, go_unicode_sharp};

/// A token type. The numbers are the goyacc token numbers of upstream, so error texts such as
/// `"<Item 57372>"` match Go.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct ItemType(pub u16);

impl ItemType {
    /// Whether the item is an arithmetic, comparison or set operator.
    pub fn is_operator(self) -> bool {
        self > operatorsStart && self < operatorsEnd
    }

    /// Whether the item is an aggregation operator.
    pub fn is_aggregator(self) -> bool {
        self > aggregatorsStart && self < aggregatorsEnd
    }

    /// Whether the item is an aggregator that takes a parameter.
    pub fn is_aggregator_with_param(self) -> bool {
        matches!(self, TOPK | BOTTOMK | COUNT_VALUES | QUANTILE | LIMITK | LIMIT_RATIO)
    }

    /// The experimental aggregators, enabled with `enable_experimental_functions`.
    pub fn is_experimental_aggregator(self) -> bool {
        matches!(self, LIMITK | LIMIT_RATIO)
    }

    /// Whether the item is a keyword.
    pub fn is_keyword(self) -> bool {
        self > keywordsStart && self < keywordsEnd
    }

    /// Whether the item is a comparison operator.
    pub fn is_comparison_operator(self) -> bool {
        matches!(self, EQLC | NEQ | LTE | LSS | GTE | GTR)
    }

    /// Whether the item is a set operator.
    pub fn is_set_operator(self) -> bool {
        matches!(self, LAND | LOR | LUNLESS)
    }

    /// The default string form (Go `ItemTypeStr`), if the type has one.
    pub fn type_str(self) -> Option<&'static str> {
        Some(match self {
            OPEN_HIST => "{{",
            CLOSE_HIST => "}}",
            LEFT_PAREN => "(",
            RIGHT_PAREN => ")",
            LEFT_BRACE => "{",
            RIGHT_BRACE => "}",
            LEFT_BRACKET => "[",
            RIGHT_BRACKET => "]",
            COMMA => ",",
            EQL => "=",
            COLON => ":",
            SEMICOLON => ";",
            BLANK => "_",
            TIMES => "x",
            SPACE => "<space>",
            SUB => "-",
            ADD => "+",
            MUL => "*",
            MOD => "%",
            DIV => "/",
            EQLC => "==",
            NEQ => "!=",
            LTE => "<=",
            LSS => "<",
            GTE => ">=",
            GTR => ">",
            TRIM_UPPER => "</",
            TRIM_LOWER => ">/",
            EQL_REGEX => "=~",
            NEQ_REGEX => "!~",
            POW => "^",
            AT => "@",
            _ => return KEYWORDS.iter().find(|(_, t)| *t == self).map(|(s, _)| *s),
        })
    }

    /// Go `ItemType.desc`.
    pub(crate) fn desc(self) -> String {
        match self {
            ERROR => "error".into(),
            EOF => "end of input".into(),
            COMMENT => "comment".into(),
            IDENTIFIER => "identifier".into(),
            METRIC_IDENTIFIER => "metric identifier".into(),
            STRING => "string".into(),
            NUMBER => "number".into(),
            DURATION => "duration".into(),
            _ => go_quote(&self.to_string()),
        }
    }
}

impl fmt::Display for ItemType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.type_str() {
            Some(s) => f.write_str(s),
            None => write!(f, "<Item {}>", self.0),
        }
    }
}

impl fmt::Debug for ItemType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self, self.0)
    }
}

/// All keywords of PromQL (Go `key`, without the special numbers).
pub(crate) static KEYWORDS: [(&str, ItemType); 33] = [
    ("and", LAND),
    ("or", LOR),
    ("unless", LUNLESS),
    ("atan2", ATAN2),
    ("sum", SUM),
    ("avg", AVG),
    ("count", COUNT),
    ("min", MIN),
    ("max", MAX),
    ("group", GROUP),
    ("stddev", STDDEV),
    ("stdvar", STDVAR),
    ("topk", TOPK),
    ("bottomk", BOTTOMK),
    ("count_values", COUNT_VALUES),
    ("quantile", QUANTILE),
    ("limitk", LIMITK),
    ("limit_ratio", LIMIT_RATIO),
    ("offset", OFFSET),
    ("smoothed", SMOOTHED),
    ("anchored", ANCHORED),
    ("by", BY),
    ("without", WITHOUT),
    ("on", ON),
    ("ignoring", IGNORING),
    ("group_left", GROUP_LEFT),
    ("group_right", GROUP_RIGHT),
    ("fill", FILL),
    ("fill_left", FILL_LEFT),
    ("fill_right", FILL_RIGHT),
    ("bool", BOOL),
    ("start", START),
    ("end", END),
];

static MORE_KEYWORDS: [(&str, ItemType); 4] =
    [("step", STEP), ("range", RANGE), ("max_of", MAX_OF), ("min_of", MIN_OF)];

/// Go `key[strings.ToLower(word)]`, including "inf" and "nan" as NUMBER. Only ASCII words match: a non-ASCII
/// word that lowercases to a keyword in Go (with U+212A KELVIN SIGN, for example) is rejected as a keyword by
/// every caller anyway.
fn lookup_key(word: &str) -> Option<ItemType> {
    if word.eq_ignore_ascii_case("inf") || word.eq_ignore_ascii_case("nan") {
        return Some(NUMBER);
    }
    lookup_lower(&KEYWORDS, word).or_else(|| lookup_lower(&MORE_KEYWORDS, word))
}

/// All keyword strings recognised by the lexer, including histogram descriptor keys and counter reset hints
/// (Go `Keywords`).
pub fn keywords() -> Vec<&'static str> {
    let mut v: Vec<&'static str> = KEYWORDS.iter().chain(MORE_KEYWORDS.iter()).map(|(s, _)| *s).collect();
    v.push("inf");
    v.push("nan");
    for (s, _) in HISTOGRAM_DESC.iter().chain(COUNTER_RESET_HINTS.iter()) {
        if !v.contains(s) {
            v.push(s);
        }
    }
    v
}

static HISTOGRAM_DESC: [(&str, ItemType); 11] = [
    ("sum", SUM_DESC),
    ("count", COUNT_DESC),
    ("schema", SCHEMA_DESC),
    ("offset", OFFSET_DESC),
    ("n_offset", NEGATIVE_OFFSET_DESC),
    ("buckets", BUCKETS_DESC),
    ("n_buckets", NEGATIVE_BUCKETS_DESC),
    ("z_bucket", ZERO_BUCKET_DESC),
    ("z_bucket_w", ZERO_BUCKET_WIDTH_DESC),
    ("custom_values", CUSTOM_VALUES_DESC),
    ("counter_reset_hint", COUNTER_RESET_HINT_DESC),
];

static COUNTER_RESET_HINTS: [(&str, ItemType); 4] = [
    ("unknown", UNKNOWN_COUNTER_RESET),
    ("reset", COUNTER_RESET),
    ("not_reset", NOT_COUNTER_RESET),
    ("gauge", GAUGE_TYPE),
];

fn lookup_lower(table: &[(&str, ItemType)], word: &str) -> Option<ItemType> {
    table.iter().find(|(s, _)| s.eq_ignore_ascii_case(word)).map(|(_, t)| *t)
}

/// A token or text string returned from the scanner.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Item<'a> {
    /// The type of this item.
    pub typ: ItemType,
    /// The starting position, in bytes, of this item in the input string.
    pub pos: Pos,
    /// The value of this item.
    pub val: Cow<'a, str>,
}

impl Item<'_> {
    /// The position range of the item.
    pub fn position_range(&self) -> PositionRange {
        PositionRange { start: self.pos, end: self.pos + self.val.len() as Pos }
    }

    /// Go `Item.desc`.
    pub(crate) fn desc(&self) -> String {
        if self.typ.type_str().is_some() {
            return self.to_string();
        }
        if self.typ == EOF {
            return self.typ.desc();
        }
        format!("{} {}", self.typ.desc(), self)
    }
}

impl fmt::Display for Item<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let t = self.typ;
        if t == EOF {
            f.write_str("EOF")
        } else if t == ERROR {
            f.write_str(&self.val)
        } else if t == IDENTIFIER || t == METRIC_IDENTIFIER {
            f.write_str(&go_quote(&self.val))
        } else if t.is_keyword() {
            write!(f, "<{}>", self.val)
        } else if t.is_operator() {
            write!(f, "<op:{}>", self.val)
        } else if t.is_aggregator() {
            write!(f, "<aggr:{}>", self.val)
        } else if self.val.len() > 10 {
            // Go `%.10q`: quote the first 10 runes.
            let end = self.val.char_indices().nth(10).map_or(self.val.len(), |(i, _)| i);
            write!(f, "{}...", go_quote(&self.val[..end]))
        } else {
            f.write_str(&go_quote(&self.val))
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum HistogramState {
    None,
    Open,
    Mul,
    Add,
    Sub,
}

/// The lexer states (Go state functions).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    Statements,
    Histogram,
    HistogramDescriptor,
    Buckets,
    InsideBraces,
    ValueSequence,
    Escape,
    String,
    RawString,
    Space,
    LineComment,
    Number,
    NumberOrDuration,
    Identifier,
    KeywordOrIdentifier,
    DurationExpr,
}

/// The state of the scanner.
pub struct Lexer<'a> {
    pub(crate) input: &'a str,
    state: Option<State>,
    pos: usize,
    pub(crate) start: usize,
    width: usize,
    last_pos: Pos,
    item: Item<'a>,
    scanned_item: bool,

    unquoted_utf8_names: bool,

    paren_depth: i32,
    brace_open: bool,
    bracket_open: bool,
    got_colon: bool,
    got_duration: bool,
    string_open: char,

    pub(crate) series_desc: bool,
    histogram_state: HistogramState,
}

#[inline]
fn is_space(r: Option<char>) -> bool {
    matches!(r, Some(' ' | '\t' | '\n' | '\r'))
}

#[inline]
fn is_end_of_line(r: Option<char>) -> bool {
    matches!(r, Some('\r' | '\n'))
}

#[inline]
fn is_alpha(r: Option<char>) -> bool {
    matches!(r, Some('_' | 'a'..='z' | 'A'..='Z'))
}

#[inline]
fn is_digit(r: Option<char>) -> bool {
    matches!(r, Some('0'..='9'))
}

#[inline]
fn is_alpha_numeric(r: Option<char>) -> bool {
    is_alpha(r) || is_digit(r)
}

/// Go `isSpace` on a byte (used by the parser).
#[inline]
pub(crate) fn is_space_byte(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}

fn is_duration_keyword_start_char(r: char) -> bool {
    matches!(model::unicode::to_lower(r), 's' | 'r' | 'm')
}

fn digit_val(ch: Option<char>) -> u32 {
    match ch {
        Some(c @ '0'..='9') => c as u32 - '0' as u32,
        Some(c @ 'a'..='f') => c as u32 - 'a' as u32 + 10,
        Some(c @ 'A'..='F') => c as u32 - 'A' as u32 + 10,
        _ => 16,
    }
}

fn contains(valid: &str, r: Option<char>) -> bool {
    r.is_some_and(|c| valid.contains(c))
}

impl<'a> Lexer<'a> {
    /// A new scanner for the input string (Go `Lex`).
    pub fn new(input: &'a str) -> Lexer<'a> {
        Lexer {
            input,
            state: Some(State::Statements),
            pos: 0,
            start: 0,
            width: 0,
            last_pos: 0,
            item: Item::default(),
            scanned_item: false,
            unquoted_utf8_names: false,
            paren_depth: 0,
            brace_open: false,
            bracket_open: false,
            got_colon: false,
            got_duration: false,
            string_open: '\0',
            series_desc: false,
            histogram_state: HistogramState::None,
        }
    }

    /// Sets whether a series description is lexed.
    pub fn with_series_desc(mut self, series_desc: bool) -> Self {
        self.series_desc = series_desc;
        self
    }

    /// Sets whether Unicode letters and non-leading dots are allowed in unquoted names.
    pub fn with_unquoted_utf8_names(mut self, enabled: bool) -> Self {
        self.unquoted_utf8_names = enabled;
        self
    }

    /// Whether the lexer has more items (Go `l.state != nil`).
    pub fn has_state(&self) -> bool {
        self.state.is_some()
    }

    fn next(&mut self) -> Option<char> {
        match self.input[self.pos..].chars().next() {
            None => {
                self.width = 0;
                None
            }
            Some(c) => {
                self.width = c.len_utf8();
                self.pos += self.width;
                Some(c)
            }
        }
    }

    fn peek(&mut self) -> Option<char> {
        let r = self.next();
        self.backup();
        r
    }

    #[inline]
    fn backup(&mut self) {
        self.pos -= self.width;
    }

    fn emit(&mut self, t: ItemType) {
        self.item = Item { typ: t, pos: self.start as Pos, val: Cow::Borrowed(&self.input[self.start..self.pos]) };
        self.start = self.pos;
        self.scanned_item = true;
    }

    fn ignore(&mut self) {
        self.start = self.pos;
    }

    fn accept(&mut self, valid: &str) -> bool {
        let r = self.next();
        if contains(valid, r) {
            return true;
        }
        self.backup();
        false
    }

    fn is(&mut self, valid: &str) -> bool {
        let r = self.peek();
        contains(valid, r)
    }

    fn accept_run(&mut self, valid: &str) {
        while {
            let r = self.next();
            contains(valid, r)
        } {}
        self.backup();
    }

    fn errorf(&mut self, msg: String) -> Option<State> {
        self.item = Item { typ: ERROR, pos: self.start as Pos, val: Cow::Owned(msg) };
        self.scanned_item = true;
        None
    }

    /// Scans the next item into `item` (Go `NextItem`).
    pub fn next_item(&mut self, item: &mut Item<'a>) {
        self.scanned_item = false;
        if self.state.is_some() {
            while !self.scanned_item {
                let st = self.state.unwrap_or(State::Statements);
                self.state = self.run(st);
            }
        } else {
            self.emit(EOF);
        }
        self.last_pos = self.item.pos;
        *item = std::mem::take(&mut self.item);
    }

    fn run(&mut self, st: State) -> Option<State> {
        match st {
            State::Statements => self.lex_statements(),
            State::Histogram => self.lex_histogram(),
            State::HistogramDescriptor => self.lex_histogram_descriptor(),
            State::Buckets => self.lex_buckets(),
            State::InsideBraces => self.lex_inside_braces(),
            State::ValueSequence => self.lex_value_sequence(),
            State::Escape => self.lex_escape(),
            State::String => self.lex_string(),
            State::RawString => self.lex_raw_string(),
            State::Space => self.lex_space(),
            State::LineComment => self.lex_line_comment(),
            State::Number => self.lex_number(),
            State::NumberOrDuration => self.lex_number_or_duration(),
            State::Identifier => self.lex_identifier(),
            State::KeywordOrIdentifier => self.lex_keyword_or_identifier(),
            State::DurationExpr => self.lex_duration_expr(),
        }
    }

    fn is_name_start(&self, r: Option<char>) -> bool {
        is_alpha(r) || (self.unquoted_utf8_names && r.is_some_and(|c| !c.is_ascii() && model::unicode::is_letter(c)))
    }

    fn is_name_continue(&self, r: Option<char>) -> bool {
        self.is_name_start(r) || is_digit(r) || (self.unquoted_utf8_names && r == Some('.'))
    }

    fn lex_statements(&mut self) -> Option<State> {
        if self.histogram_state != HistogramState::None {
            return Some(State::Histogram);
        }
        if self.brace_open {
            return Some(State::InsideBraces);
        }
        if self.input[self.pos..].starts_with('#') {
            return Some(State::LineComment);
        }

        let r = self.next();
        match r {
            None => {
                if self.paren_depth != 0 {
                    return self.errorf("unclosed left parenthesis".into());
                } else if self.bracket_open {
                    return self.errorf("unclosed left bracket".into());
                }
                self.emit(EOF);
                return None;
            }
            Some(',') => self.emit(COMMA),
            _ if is_space(r) => return Some(State::Space),
            Some('*') => self.emit(MUL),
            Some('/') => self.emit(DIV),
            Some('%') => self.emit(MOD),
            Some('+') => self.emit(ADD),
            Some('-') => self.emit(SUB),
            Some('^') => self.emit(POW),
            Some('=') => match self.peek() {
                Some('=') => {
                    self.next();
                    self.emit(EQLC);
                }
                Some('~') => {
                    return self.errorf(format!("unexpected character after '=': {}", go_quote_rune(Some('~'))));
                }
                _ => self.emit(EQL),
            },
            Some('!') => {
                let t = self.next();
                if t != Some('=') {
                    return self.errorf(format!("unexpected character after '!': {}", go_quote_rune(t)));
                }
                self.emit(NEQ);
            }
            Some('<') => {
                if self.peek() == Some('=') {
                    self.next();
                    self.emit(LTE);
                } else if self.peek() == Some('/') {
                    self.next();
                    self.emit(TRIM_UPPER);
                } else {
                    self.emit(LSS);
                }
            }
            Some('>') => {
                if self.peek() == Some('=') {
                    self.next();
                    self.emit(GTE);
                } else if self.peek() == Some('/') {
                    self.next();
                    self.emit(TRIM_LOWER);
                } else {
                    self.emit(GTR);
                }
            }
            _ if is_digit(r) || (r == Some('.') && is_digit(self.peek())) => {
                self.backup();
                return Some(State::NumberOrDuration);
            }
            Some(c @ ('"' | '\'')) => {
                self.string_open = c;
                return Some(State::String);
            }
            Some('`') => {
                self.string_open = '`';
                return Some(State::RawString);
            }
            Some(c) if self.is_name_start(r) || c == ':' => {
                if !self.bracket_open {
                    self.backup();
                    return Some(State::KeywordOrIdentifier);
                }
                if c == ':' {
                    if self.got_colon {
                        return self.errorf(format!("unexpected colon {}", go_quote_rune(r)));
                    }
                    self.emit(COLON);
                    self.got_colon = true;
                    return Some(State::Statements);
                } else if is_duration_keyword_start_char(c) && self.scan_duration_keyword() {
                    return Some(State::Statements);
                }
                return self.errorf(format!(
                    "unexpected character: {}, expected {}",
                    go_quote_rune(r),
                    go_quote_rune(Some(':'))
                ));
            }
            Some('(') => {
                self.emit(LEFT_PAREN);
                self.paren_depth += 1;
                return Some(State::Statements);
            }
            Some(')') => {
                self.emit(RIGHT_PAREN);
                self.paren_depth -= 1;
                if self.paren_depth < 0 {
                    return self.errorf(format!("unexpected right parenthesis {}", go_quote_rune(r)));
                }
                return Some(State::Statements);
            }
            Some('{') => {
                self.emit(LEFT_BRACE);
                self.brace_open = true;
                return Some(State::InsideBraces);
            }
            Some('[') => {
                if self.bracket_open {
                    return self.errorf(format!("unexpected left bracket {}", go_quote_rune(r)));
                }
                self.got_colon = false;
                self.got_duration = false;
                self.emit(LEFT_BRACKET);
                if is_space(self.peek()) {
                    self.skip_spaces();
                }
                self.bracket_open = true;
                return Some(State::DurationExpr);
            }
            Some(']') => {
                if !self.bracket_open {
                    return self.errorf(format!("unexpected right bracket {}", go_quote_rune(r)));
                }
                self.emit(RIGHT_BRACKET);
                self.bracket_open = false;
            }
            Some('@') => self.emit(AT),
            _ => return self.errorf(format!("unexpected character: {}", go_quote_rune(r))),
        }
        Some(State::Statements)
    }

    fn lex_histogram(&mut self) -> Option<State> {
        match self.histogram_state {
            HistogramState::Mul => {
                self.histogram_state = HistogramState::None;
                self.next();
                self.emit(TIMES);
                return Some(State::ValueSequence);
            }
            HistogramState::Add => {
                self.histogram_state = HistogramState::None;
                self.next();
                self.emit(ADD);
                return Some(State::ValueSequence);
            }
            HistogramState::Sub => {
                self.histogram_state = HistogramState::None;
                self.next();
                self.emit(SUB);
                return Some(State::ValueSequence);
            }
            _ => {}
        }

        if self.bracket_open {
            return Some(State::Buckets);
        }
        let r = self.next();
        match r {
            _ if is_space(r) => {
                self.emit(SPACE);
                Some(State::Space)
            }
            _ if is_alpha(r) => {
                self.backup();
                Some(State::HistogramDescriptor)
            }
            Some(':') => {
                self.emit(COLON);
                Some(State::Histogram)
            }
            Some('-') => {
                self.emit(SUB);
                Some(State::Histogram)
            }
            Some('x') => {
                self.emit(TIMES);
                Some(State::Number)
            }
            _ if is_digit(r) => {
                self.backup();
                Some(State::Number)
            }
            Some('[') => {
                self.bracket_open = true;
                self.got_colon = false;
                self.got_duration = false;
                self.emit(LEFT_BRACKET);
                Some(State::Buckets)
            }
            Some('}') if self.peek() == Some('}') => {
                self.next();
                self.emit(CLOSE_HIST);
                match self.peek() {
                    Some('x') => {
                        self.histogram_state = HistogramState::Mul;
                        Some(State::Histogram)
                    }
                    Some('+') => {
                        self.histogram_state = HistogramState::Add;
                        Some(State::Histogram)
                    }
                    Some('-') => {
                        self.histogram_state = HistogramState::Sub;
                        Some(State::Histogram)
                    }
                    _ => {
                        self.histogram_state = HistogramState::None;
                        Some(State::ValueSequence)
                    }
                }
            }
            _ => self.errorf(format!("histogram description incomplete unexpected: {}", go_quote_rune(r))),
        }
    }

    fn lex_histogram_descriptor(&mut self) -> Option<State> {
        loop {
            let r = self.next();
            if is_alpha(r) {
                continue;
            }
            self.backup();
            let word = &self.input[self.start..self.pos];
            if let Some(desc) = lookup_lower(&HISTOGRAM_DESC, word) {
                if self.peek() == Some(':') {
                    self.emit(desc);
                    return Some(State::Histogram);
                }
                self.errorf("missing `:` for histogram descriptor".into());
                break;
            }
            // Current word is Inf or NaN.
            if lookup_key(word) == Some(NUMBER) {
                self.emit(NUMBER);
                return Some(State::Histogram);
            }
            if let Some(desc) = lookup_lower(&COUNTER_RESET_HINTS, word) {
                self.emit(desc);
                return Some(State::Histogram);
            }
            let msg = format!("bad histogram descriptor found: {}", go_quote(word));
            self.errorf(msg);
            break;
        }
        Some(State::Statements)
    }

    fn lex_buckets(&mut self) -> Option<State> {
        let r = self.next();
        match r {
            _ if is_space(r) => {
                self.emit(SPACE);
                Some(State::Space)
            }
            Some('-') => {
                self.emit(SUB);
                Some(State::Number)
            }
            _ if is_digit(r) => {
                self.backup();
                Some(State::Number)
            }
            Some(']') => {
                self.bracket_open = false;
                self.emit(RIGHT_BRACKET);
                Some(State::Histogram)
            }
            _ if is_alpha(r) => {
                // Current word is Inf or NaN.
                let word = &self.input[self.start..self.pos];
                if lookup_key(word) == Some(NUMBER) {
                    self.emit(NUMBER);
                    return Some(State::Statements);
                }
                Some(State::Buckets)
            }
            _ => self.errorf(format!("invalid character in buckets description: {}", go_quote_rune(r))),
        }
    }

    fn lex_inside_braces(&mut self) -> Option<State> {
        if self.input[self.pos..].starts_with('#') {
            return Some(State::LineComment);
        }
        let r = self.next();
        match r {
            None => return self.errorf("unexpected end of input inside braces".into()),
            _ if is_space(r) => return Some(State::Space),
            _ if self.is_name_start(r) => {
                self.backup();
                return Some(State::Identifier);
            }
            Some(',') => self.emit(COMMA),
            Some(c @ ('"' | '\'')) => {
                self.string_open = c;
                return Some(State::String);
            }
            Some('`') => {
                self.string_open = '`';
                return Some(State::RawString);
            }
            Some('=') => {
                if self.next() == Some('~') {
                    self.emit(EQL_REGEX);
                } else {
                    self.backup();
                    self.emit(EQL);
                }
            }
            Some('!') => {
                let nr = self.next();
                match nr {
                    Some('~') => self.emit(NEQ_REGEX),
                    Some('=') => self.emit(NEQ),
                    _ => {
                        return self
                            .errorf(format!("unexpected character after '!' inside braces: {}", go_quote_rune(nr)));
                    }
                }
            }
            Some('{') => return self.errorf(format!("unexpected left brace {}", go_quote_rune(r))),
            Some('}') => {
                self.emit(RIGHT_BRACE);
                self.brace_open = false;
                if self.series_desc {
                    return Some(State::ValueSequence);
                }
                return Some(State::Statements);
            }
            _ => return self.errorf(format!("unexpected character inside braces: {}", go_quote_rune(r))),
        }
        Some(State::InsideBraces)
    }

    fn lex_value_sequence(&mut self) -> Option<State> {
        if self.histogram_state != HistogramState::None {
            return Some(State::Histogram);
        }
        let r = self.next();
        match r {
            None => return Some(State::Statements),
            Some('{') if self.peek() == Some('{') => {
                if self.histogram_state != HistogramState::None {
                    return self.errorf("unexpected histogram opening {{".into());
                }
                self.histogram_state = HistogramState::Open;
                self.next();
                self.emit(OPEN_HIST);
                return Some(State::Histogram);
            }
            _ if is_space(r) => {
                self.emit(SPACE);
                self.lex_space();
            }
            Some('+') => self.emit(ADD),
            Some('-') => self.emit(SUB),
            Some('x') => self.emit(TIMES),
            Some('_') => self.emit(BLANK),
            _ if is_digit(r) || (r == Some('.') && is_digit(self.peek())) => {
                self.backup();
                self.lex_number();
            }
            _ if is_alpha(r) => {
                self.backup();
                // We might lex invalid items here but this will be caught by the parser.
                return Some(State::KeywordOrIdentifier);
            }
            _ => return self.errorf(format!("unexpected character in series sequence: {}", go_quote_rune(r))),
        }
        Some(State::ValueSequence)
    }

    fn lex_escape(&mut self) -> Option<State> {
        let mut ch = self.next();
        let (mut n, base, max_val): (u32, u32, u32) = match ch {
            Some('a' | 'b' | 'f' | 'n' | 'r' | 't' | 'v' | '\\') => return Some(State::String),
            Some(c) if c == self.string_open => return Some(State::String),
            Some('0'..='7') => (3, 8, 255),
            Some('x') => {
                ch = self.next();
                (2, 16, 255)
            }
            Some('u') => {
                ch = self.next();
                (4, 16, 0x10FFFF)
            }
            Some('U') => {
                ch = self.next();
                (8, 16, 0x10FFFF)
            }
            None => {
                self.errorf("escape sequence not terminated".into());
                return Some(State::String);
            }
            Some(c) => {
                self.errorf(format!("unknown escape sequence {}", go_unicode_sharp(c)));
                return Some(State::String);
            }
        };

        let mut x: u32 = 0;
        while n > 0 {
            let d = digit_val(ch);
            if d >= base {
                match ch {
                    None => self.errorf("escape sequence not terminated".into()),
                    Some(c) => self.errorf(format!("illegal character {} in escape sequence", go_unicode_sharp(c))),
                };
                return Some(State::String);
            }
            x = x.wrapping_mul(base).wrapping_add(d);
            n -= 1;
            // Don't seek after last rune.
            if n > 0 {
                ch = self.next();
            }
        }

        if x > max_val || (0xD800..0xE000).contains(&x) {
            self.errorf("escape sequence is an invalid Unicode code point".into());
        }
        Some(State::String)
    }

    fn skip_spaces(&mut self) {
        while is_space(self.peek()) {
            self.next();
        }
        self.ignore();
    }

    fn lex_string(&mut self) -> Option<State> {
        loop {
            match self.next() {
                Some('\\') => return Some(State::Escape),
                Some('\u{FFFD}') => {
                    // Go's utf8.RuneError: invalid UTF-8 cannot occur in a Rust string, but a literal U+FFFD
                    // matches the same case in Go.
                    self.errorf("invalid UTF-8 rune".into());
                    return Some(State::String);
                }
                None | Some('\n') => return self.errorf("unterminated quoted string".into()),
                Some(c) if c == self.string_open => break,
                _ => {}
            }
        }
        self.emit(STRING);
        Some(State::Statements)
    }

    fn lex_raw_string(&mut self) -> Option<State> {
        loop {
            match self.next() {
                Some('\u{FFFD}') => {
                    self.errorf("invalid UTF-8 rune".into());
                    return Some(State::RawString);
                }
                None => {
                    self.errorf("unterminated raw string".into());
                    return Some(State::RawString);
                }
                Some(c) if c == self.string_open => break,
                _ => {}
            }
        }
        self.emit(STRING);
        Some(State::Statements)
    }

    fn lex_space(&mut self) -> Option<State> {
        while is_space(self.peek()) {
            self.next();
        }
        self.ignore();
        Some(State::Statements)
    }

    fn lex_line_comment(&mut self) -> Option<State> {
        self.pos += 1;
        let mut r = self.next();
        while !is_end_of_line(r) && r.is_some() {
            r = self.next();
        }
        self.backup();
        self.emit(COMMENT);
        Some(State::Statements)
    }

    fn lex_number(&mut self) -> Option<State> {
        if !self.scan_number() {
            let msg = format!("bad number syntax: {}", go_quote(&self.input[self.start..self.pos]));
            return self.errorf(msg);
        }
        self.emit(NUMBER);
        Some(State::Statements)
    }

    fn scan_duration_keyword(&mut self) -> bool {
        loop {
            let r = self.next();
            if is_alpha(r) {
                continue;
            }
            self.backup();
            let word = &self.input[self.start..self.pos];
            let tok = match lookup_key(word) {
                Some(t @ (STEP | RANGE | MAX_OF | MIN_OF)) => t,
                _ => return false,
            };
            self.emit(tok);
            return true;
        }
    }

    fn lex_number_or_duration(&mut self) -> Option<State> {
        if self.scan_number() {
            self.emit(NUMBER);
            return Some(State::Statements);
        }
        // Next two chars must be a valid unit and a non-alphanumeric.
        if self.accept_remaining_duration() {
            self.backup();
            self.emit(DURATION);
            return Some(State::Statements);
        }
        let msg = format!("bad number or duration syntax: {}", go_quote(&self.input[self.start..self.pos]));
        self.errorf(msg)
    }

    fn accept_remaining_duration(&mut self) -> bool {
        // Next two char must be a valid duration.
        if !self.accept("smhdwy") {
            return false;
        }
        // Support for ms. Bad units like hs, ys will be caught when we actually parse the duration.
        self.accept("s");
        // Next char can be another number then a unit.
        while self.accept("0123456789") {
            while self.accept("0123456789") {}
            // y is no longer in the list as it should always come first in durations.
            if !self.accept("smhdw") {
                return false;
            }
            // Support for ms. Bad units like hs, ys will be caught when we actually parse the duration.
            self.accept("s");
        }
        let r = self.next();
        !is_alpha_numeric(r)
    }

    /// Scans numbers of different formats. The scanned item is not necessarily a valid number; the parser
    /// catches that case.
    fn scan_number(&mut self) -> bool {
        let initial_pos = self.pos;
        let mut digit_pattern = "0123456789";
        // Disallow hexadecimal in series descriptions as the syntax is ambiguous.
        if !self.series_desc && self.accept("0") && self.accept("xX") {
            self.accept("_"); // eg., 0X_1FFFP-16 == 0.1249847412109375
            digit_pattern = "0123456789abcdefABCDEF";
        }
        const DOT: &str = ".";
        const EXPONENT: &str = "eE";
        const UNDERSCORE: &str = "_";
        const DOT_ANTI: &str = "_.";
        const EXPONENT_ANTI: &str = "._eE";
        const UNDERSCORE_ANTI: &str = "._eE";
        let hex = digit_pattern.len() > 10;
        let all: &str = if hex { "0123456789abcdefABCDEF._eE" } else { "0123456789._eE" };
        // All numbers follow the prefix: [.][d][d._eE]*
        self.accept(DOT);
        self.accept(digit_pattern);
        let mut dot_consumed = false;
        let mut exponent_consumed = false;
        while self.is(all) {
            // "." cannot repeat.
            if self.is(DOT) && dot_consumed {
                self.accept(DOT);
                return false;
            }
            // "eE" cannot repeat.
            if self.is(EXPONENT) && exponent_consumed {
                self.accept(EXPONENT);
                return false;
            }
            // Handle dots.
            if self.accept(DOT) {
                dot_consumed = true;
                if self.accept(DOT_ANTI) {
                    return false;
                }
                // Fractional hexadecimal literals are not allowed.
                if hex {
                    return false;
                }
                continue;
            }
            // Handle exponents.
            if self.accept(EXPONENT) {
                exponent_consumed = true;
                self.accept("+-");
                if self.accept(EXPONENT_ANTI) || self.peek().is_none() {
                    return false;
                }
                continue;
            }
            // Handle underscores.
            if self.accept(UNDERSCORE) {
                if self.accept(UNDERSCORE_ANTI) || self.peek().is_none() {
                    return false;
                }
                continue;
            }
            // Handle digits at the end since we already consumed before this loop.
            self.accept_run(digit_pattern);
        }
        // Empty string is not a valid number.
        if self.pos == initial_pos {
            return false;
        }
        // Next thing must not be alphanumeric unless it's the times token for series repetitions.
        let r = self.peek();
        (self.series_desc && r == Some('x')) || !is_alpha_numeric(r)
    }

    fn lex_identifier(&mut self) -> Option<State> {
        while {
            let r = self.next();
            self.is_name_continue(r)
        } {}
        self.backup();
        self.emit(IDENTIFIER);
        Some(State::Statements)
    }

    fn lex_keyword_or_identifier(&mut self) -> Option<State> {
        loop {
            let r = self.next();
            if self.is_name_continue(r) || r == Some(':') {
                continue;
            }
            self.backup();
            let word = &self.input[self.start..self.pos];
            // Unicode letters that lowercase to ASCII must retain their identity as names: lookup_key only
            // matches ASCII words.
            match lookup_key(word) {
                Some(kw) => {
                    // For fill/fill_left/fill_right, only treat as keyword if followed by '('. This allows
                    // using these as metric names (e.g., "fill + fill").
                    if matches!(kw, FILL | FILL_LEFT | FILL_RIGHT) && !self.peek_followed_by_left_paren() {
                        self.emit(IDENTIFIER);
                    } else {
                        self.emit(kw);
                    }
                }
                None if !word.contains(':') => self.emit(IDENTIFIER),
                None => self.emit(METRIC_IDENTIFIER),
            }
            break;
        }
        if self.series_desc && self.peek() != Some('{') {
            return Some(State::ValueSequence);
        }
        Some(State::Statements)
    }

    fn peek_followed_by_left_paren(&self) -> bool {
        for c in self.input[self.pos..].chars() {
            if !is_space(Some(c)) {
                return c == '(';
            }
        }
        false
    }

    fn lex_duration_expr(&mut self) -> Option<State> {
        let r = self.next();
        match r {
            None => self.errorf("unexpected end of input in duration expression".into()),
            Some(']') => {
                self.emit(RIGHT_BRACKET);
                self.bracket_open = false;
                self.got_colon = false;
                Some(State::Statements)
            }
            Some(':') => {
                self.emit(COLON);
                if !self.got_duration {
                    return self.errorf("unexpected colon before duration in duration expression".into());
                }
                if self.got_colon {
                    return self.errorf("unexpected repeated colon in duration expression".into());
                }
                self.got_colon = true;
                Some(State::DurationExpr)
            }
            Some('(') => {
                self.emit(LEFT_PAREN);
                self.paren_depth += 1;
                Some(State::DurationExpr)
            }
            Some(')') => {
                self.emit(RIGHT_PAREN);
                self.paren_depth -= 1;
                if self.paren_depth < 0 {
                    return self.errorf(format!("unexpected right parenthesis {}", go_quote_rune(r)));
                }
                Some(State::DurationExpr)
            }
            _ if is_space(r) => {
                self.skip_spaces();
                Some(State::DurationExpr)
            }
            Some('+') => {
                self.emit(ADD);
                Some(State::DurationExpr)
            }
            Some('-') => {
                self.emit(SUB);
                Some(State::DurationExpr)
            }
            Some('*') => {
                self.emit(MUL);
                Some(State::DurationExpr)
            }
            Some('/') => {
                self.emit(DIV);
                Some(State::DurationExpr)
            }
            Some('%') => {
                self.emit(MOD);
                Some(State::DurationExpr)
            }
            Some('^') => {
                self.emit(POW);
                Some(State::DurationExpr)
            }
            Some(',') => {
                self.emit(COMMA);
                Some(State::DurationExpr)
            }
            Some(c) if is_duration_keyword_start_char(c) => {
                if self.scan_duration_keyword() {
                    self.got_duration = true;
                    return Some(State::DurationExpr);
                }
                self.errorf(format!("unexpected character in duration expression: {}", go_quote_rune(r)))
            }
            _ if is_digit(r) || (r == Some('.') && is_digit(self.peek())) => {
                self.backup();
                self.got_duration = true;
                Some(State::NumberOrDuration)
            }
            _ => self.errorf(format!("unexpected character in duration expression: {}", go_quote_rune(r))),
        }
    }

    /// Finds the previous right parenthesis (Go `findPrevRightParen`). Used when the parser had to read ahead
    /// to find the next right parenthesis and lost track of the previous one.
    pub(crate) fn find_prev_right_paren(&self, fallback_pos: Pos) -> Pos {
        let input = self.input.as_bytes();
        let n = input.len() as Pos;
        if fallback_pos <= 0
            || fallback_pos > n
            || self.last_pos <= 0
            || self.last_pos >= n
            || input[self.last_pos as usize] != b')'
        {
            return fallback_pos;
        }
        let mut i = self.last_pos - 1;
        while i > 0 {
            let b = input[i as usize];
            if b == b')' {
                return i + 1;
            } else if !is_space_byte(b) {
                return fallback_pos;
            }
            i -= 1;
        }
        fallback_pos
    }
}

/// Whether a label name must be quoted when printed (Go `labelNameRequiresQuoting`).
pub(crate) fn label_name_requires_quoting(s: &str, unquoted_utf8_names: bool) -> bool {
    if !unquoted_utf8_names {
        return !model::labels::ValidationScheme::Legacy.is_valid_label_name(s);
    }
    if s.is_empty() {
        return true;
    }
    let l = Lexer::new("").with_unquoted_utf8_names(true);
    for (i, r) in s.chars().enumerate() {
        if (i == 0 && !l.is_name_start(Some(r))) || (i > 0 && !l.is_name_continue(Some(r))) {
            return true;
        }
    }
    false
}
