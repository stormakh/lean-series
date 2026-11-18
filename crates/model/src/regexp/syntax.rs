//! Port of `github.com/grafana/regexp/syntax` (the parser part): Go RE2 syntax parsed into the same tree Go builds
//! (literal strings merged, alternations factored, character classes as sorted rune ranges). `FastRegexMatcher`
//! inspects this tree, so its shape must match Go exactly. `Simplify` is not ported: Prometheus inspects the tree
//! straight from `Parse`, and matching is done by the `regex` crate on a translation of the tree.

use std::fmt;

use crate::unicode;

/// A parse error code (Go `syntax.ErrorCode`). The string is Go's message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    InternalError,
    InvalidCharClass,
    InvalidCharRange,
    InvalidEscape,
    InvalidNamedCapture,
    InvalidPerlOp,
    InvalidRepeatOp,
    InvalidRepeatSize,
    InvalidUtf8,
    MissingBracket,
    MissingParen,
    MissingRepeatArgument,
    TrailingBackslash,
    UnexpectedParen,
    NestingDepth,
    Large,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::InternalError => "regexp/syntax: internal error",
            ErrorCode::InvalidCharClass => "invalid character class",
            ErrorCode::InvalidCharRange => "invalid character class range",
            ErrorCode::InvalidEscape => "invalid escape sequence",
            ErrorCode::InvalidNamedCapture => "invalid named capture",
            ErrorCode::InvalidPerlOp => "invalid or unsupported Perl syntax",
            ErrorCode::InvalidRepeatOp => "invalid nested repetition operator",
            ErrorCode::InvalidRepeatSize => "invalid repeat count",
            ErrorCode::InvalidUtf8 => "invalid UTF-8",
            ErrorCode::MissingBracket => "missing closing ]",
            ErrorCode::MissingParen => "missing closing )",
            ErrorCode::MissingRepeatArgument => "missing argument to repetition operator",
            ErrorCode::TrailingBackslash => "trailing backslash at end of expression",
            ErrorCode::UnexpectedParen => "unexpected )",
            ErrorCode::NestingDepth => "expression nests too deeply",
            ErrorCode::Large => "expression too large",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A parse error with the offending expression (Go `*syntax.Error`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub code: ErrorCode,
    pub expr: String,
}

impl Error {
    fn new(code: ErrorCode, expr: &str) -> Error {
        Error { code, expr: expr.to_string() }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "error parsing regexp: {}: `{}`", self.code, self.expr)
    }
}

impl std::error::Error for Error {}

/// Parser flags (Go `syntax.Flags`).
pub type Flags = u16;
pub const FOLD_CASE: Flags = 1;
pub const LITERAL: Flags = 2;
pub const CLASS_NL: Flags = 4;
pub const DOT_NL: Flags = 8;
pub const ONE_LINE: Flags = 16;
pub const NON_GREEDY: Flags = 32;
pub const PERL_X: Flags = 64;
pub const UNICODE_GROUPS: Flags = 128;
pub const WAS_DOLLAR: Flags = 256;
pub const SIMPLE: Flags = 512;
pub const MATCH_NL: Flags = CLASS_NL | DOT_NL;
pub const PERL: Flags = CLASS_NL | ONE_LINE | PERL_X | UNICODE_GROUPS;
pub const POSIX: Flags = 0;

/// A regexp operator (Go `syntax.Op`). The order matters: it is Go's order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Op {
    NoMatch = 1,
    EmptyMatch,
    Literal,
    CharClass,
    AnyCharNotNL,
    AnyChar,
    BeginLine,
    EndLine,
    BeginText,
    EndText,
    WordBoundary,
    NoWordBoundary,
    Capture,
    Star,
    Plus,
    Quest,
    Repeat,
    Concat,
    Alternate,
    // Pseudo-ops for the parse stack.
    LeftParen = 128,
    VerticalBar,
}

/// A node of a regular expression syntax tree (Go `syntax.Regexp`). Runes are `i32` as in Go.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Regexp {
    pub op: Op,
    pub flags: Flags,
    pub sub: Vec<Regexp>,
    pub rune: Vec<i32>,
    pub min: i32,
    pub max: i32,
    pub cap: i32,
    pub name: String,
}

impl Regexp {
    pub fn new(op: Op) -> Regexp {
        Regexp { op, flags: 0, sub: Vec::new(), rune: Vec::new(), min: 0, max: 0, cap: 0, name: String::new() }
    }

    /// Go `Regexp.Equal` (the grafana fork version: fold flags of literals are not compared).
    pub fn equal(&self, y: &Regexp) -> bool {
        if self.op != y.op {
            return false;
        }
        match self.op {
            Op::EndText => {
                if self.flags & WAS_DOLLAR != y.flags & WAS_DOLLAR {
                    return false;
                }
            }
            Op::Literal | Op::CharClass => return self.rune == y.rune,
            Op::Alternate | Op::Concat => {
                return self.sub.len() == y.sub.len() && self.sub.iter().zip(&y.sub).all(|(a, b)| a.equal(b));
            }
            Op::Star | Op::Plus | Op::Quest => {
                if self.flags & NON_GREEDY != y.flags & NON_GREEDY || !self.sub[0].equal(&y.sub[0]) {
                    return false;
                }
            }
            Op::Repeat => {
                if self.flags & NON_GREEDY != y.flags & NON_GREEDY
                    || self.min != y.min
                    || self.max != y.max
                    || !self.sub[0].equal(&y.sub[0])
                {
                    return false;
                }
            }
            Op::Capture => {
                if self.cap != y.cap || self.name != y.name || !self.sub[0].equal(&y.sub[0]) {
                    return false;
                }
            }
            _ => {}
        }
        true
    }

    /// The maximum capture index in the tree.
    pub fn max_cap(&self) -> i32 {
        let mut m = 0;
        if self.op == Op::Capture {
            m = self.cap;
        }
        for s in &self.sub {
            m = m.max(s.max_cap());
        }
        m
    }

    /// The names of the capturing groups, indexed by capture number (index 0 is the whole match, "").
    pub fn cap_names(&self) -> Vec<String> {
        let mut names = vec![String::new(); self.max_cap() as usize + 1];
        fn walk(re: &Regexp, names: &mut Vec<String>) {
            if re.op == Op::Capture {
                names[re.cap as usize] = re.name.clone();
            }
            for s in &re.sub {
                walk(s, names);
            }
        }
        walk(self, &mut names);
        names
    }
}

const MAX_HEIGHT: usize = 1000;
const MAX_SIZE: i64 = (128 << 20) / 40;
const MAX_RUNES: usize = (128 << 20) / 4;
const MIN_FOLD: i32 = 0x0041;
const MAX_FOLD: i32 = 0x1e943;

/// Size and height limit tracking (Go `checkLimits`).
struct Limits {
    num_regexp: usize,
    num_runes: usize,
    repeats: i64,
    size_tracking: bool,
    height_tracking: bool,
}

impl Limits {
    fn check(&mut self, re: &Regexp, stack: &[Regexp]) -> Result<(), ErrorCode> {
        if self.num_runes > MAX_RUNES {
            return Err(ErrorCode::Large);
        }
        self.check_size(re, stack)?;
        self.check_height(re, stack)
    }

    fn check_size(&mut self, re: &Regexp, stack: &[Regexp]) -> Result<(), ErrorCode> {
        if !self.size_tracking {
            if self.repeats == 0 {
                self.repeats = 1;
            }
            if re.op == Op::Repeat {
                let mut n = re.max;
                if n == -1 {
                    n = re.min;
                }
                if n <= 0 {
                    n = 1;
                }
                if n as i64 > MAX_SIZE / self.repeats {
                    self.repeats = MAX_SIZE;
                } else {
                    self.repeats *= n as i64;
                }
            }
            if (self.num_regexp as i64) < MAX_SIZE / self.repeats {
                return Ok(());
            }
            self.size_tracking = true;
            for r in stack {
                if calc_size(r) > MAX_SIZE {
                    return Err(ErrorCode::Large);
                }
            }
        }
        if calc_size(re) > MAX_SIZE {
            return Err(ErrorCode::Large);
        }
        Ok(())
    }

    fn check_height(&mut self, re: &Regexp, stack: &[Regexp]) -> Result<(), ErrorCode> {
        if self.num_regexp < MAX_HEIGHT {
            return Ok(());
        }
        if !self.height_tracking {
            self.height_tracking = true;
            for r in stack {
                if calc_height(r, 0) > MAX_HEIGHT {
                    return Err(ErrorCode::NestingDepth);
                }
            }
        }
        if calc_height(re, 0) > MAX_HEIGHT {
            return Err(ErrorCode::NestingDepth);
        }
        Ok(())
    }
}

fn calc_size(re: &Regexp) -> i64 {
    let size: i64 = match re.op {
        Op::Literal => re.rune.len() as i64,
        Op::Capture | Op::Star => 2 + calc_size(&re.sub[0]),
        Op::Plus | Op::Quest => 1 + calc_size(&re.sub[0]),
        Op::Concat => re.sub.iter().map(calc_size).sum(),
        Op::Alternate => {
            let mut s: i64 = re.sub.iter().map(calc_size).sum();
            if re.sub.len() > 1 {
                s += re.sub.len() as i64 - 1;
            }
            s
        }
        Op::Repeat => {
            let sub = calc_size(&re.sub[0]);
            if re.max == -1 {
                if re.min == 0 { 2 + sub } else { 1 + re.min as i64 * sub }
            } else {
                re.max as i64 * sub + (re.max - re.min) as i64
            }
        }
        _ => 0,
    };
    size.max(1)
}

/// Height of the tree, stopping early once it is known to exceed the limit.
fn calc_height(re: &Regexp, depth: usize) -> usize {
    if depth > MAX_HEIGHT {
        return 1;
    }
    let mut h = 1;
    for s in &re.sub {
        h = h.max(1 + calc_height(s, depth + 1));
        if h > MAX_HEIGHT {
            break;
        }
    }
    h
}

struct Parser<'a> {
    flags: Flags,
    stack: Vec<Regexp>,
    num_cap: i32,
    whole: &'a str,
    lim: Limits,
}

/// Parses `s` with `flags` into a syntax tree (Go `syntax.Parse`).
pub fn parse(s: &str, flags: Flags) -> Result<Regexp, Error> {
    match parse_inner(s, flags) {
        Ok(r) => Ok(r),
        Err(PErr::Limit(code)) => Err(Error::new(code, s)),
        Err(PErr::Err(e)) => Err(e),
    }
}

enum PErr {
    Err(Error),
    Limit(ErrorCode),
}

impl From<ErrorCode> for PErr {
    fn from(c: ErrorCode) -> PErr {
        PErr::Limit(c)
    }
}

impl From<Error> for PErr {
    fn from(e: Error) -> PErr {
        PErr::Err(e)
    }
}

type PResult<T> = Result<T, PErr>;

fn perr(code: ErrorCode, expr: &str) -> PErr {
    PErr::Err(Error::new(code, expr))
}

/// Go `nextRune` on a valid UTF-8 str: an empty string gives U+FFFD and "" (as Go's DecodeRuneInString).
fn next_rune(s: &str) -> (i32, &str) {
    match s.chars().next() {
        Some(c) => (c as i32, &s[c.len_utf8()..]),
        None => (0xFFFD, ""),
    }
}

/// `before[:len(before)-len(after)]` for a suffix `after` of `before`.
fn cut<'a>(before: &'a str, after: &str) -> &'a str {
    &before[..before.len() - after.len()]
}

fn parse_inner(s: &str, flags: Flags) -> PResult<Regexp> {
    if flags & LITERAL != 0 {
        let mut re = Regexp::new(Op::Literal);
        re.flags = flags;
        re.rune = s.chars().map(|c| c as i32).collect();
        return Ok(re);
    }
    let mut p = Parser {
        flags,
        stack: Vec::new(),
        num_cap: 0,
        whole: s,
        lim: Limits { num_regexp: 0, num_runes: 0, repeats: 0, size_tracking: false, height_tracking: false },
    };
    let mut last_repeat: &str = "";
    let mut t = s;
    while !t.is_empty() {
        let mut repeat: &str = "";
        let b = t.as_bytes();
        match b[0] {
            b'(' => {
                if p.flags & PERL_X != 0 && b.len() >= 2 && b[1] == b'?' {
                    t = p.parse_perl_flags(t)?;
                } else {
                    p.num_cap += 1;
                    let cap = p.num_cap;
                    let i = p.op(Op::LeftParen)?.expect("left paren pushed");
                    p.stack[i].cap = cap;
                    t = &t[1..];
                }
            }
            b'|' => {
                p.parse_vertical_bar()?;
                t = &t[1..];
            }
            b')' => {
                p.parse_right_paren()?;
                t = &t[1..];
            }
            b'^' => {
                if p.flags & ONE_LINE != 0 {
                    p.op(Op::BeginText)?;
                } else {
                    p.op(Op::BeginLine)?;
                }
                t = &t[1..];
            }
            b'$' => {
                if p.flags & ONE_LINE != 0 {
                    if let Some(i) = p.op(Op::EndText)? {
                        p.stack[i].flags |= WAS_DOLLAR;
                    }
                } else {
                    p.op(Op::EndLine)?;
                }
                t = &t[1..];
            }
            b'.' => {
                if p.flags & DOT_NL != 0 {
                    p.op(Op::AnyChar)?;
                } else {
                    p.op(Op::AnyCharNotNL)?;
                }
                t = &t[1..];
            }
            b'[' => {
                t = p.parse_class(t)?;
            }
            b'*' | b'+' | b'?' => {
                let before = t;
                let op = match b[0] {
                    b'*' => Op::Star,
                    b'+' => Op::Plus,
                    _ => Op::Quest,
                };
                let after = &t[1..];
                let after = p.repeat(op, 0, 0, before, after, last_repeat)?;
                repeat = before;
                t = after;
            }
            b'{' => {
                let before = t;
                match p.parse_repeat(t) {
                    None => {
                        p.literal('{' as i32)?;
                        t = &t[1..];
                    }
                    Some((min, max, after)) => {
                        if min < 0 || min > 1000 || max > 1000 || (max >= 0 && min > max) {
                            return Err(perr(ErrorCode::InvalidRepeatSize, cut(before, after)));
                        }
                        let after = p.repeat(Op::Repeat, min, max, before, after, last_repeat)?;
                        repeat = before;
                        t = after;
                    }
                }
            }
            b'\\' => {
                t = p.parse_backslash(t)?;
            }
            _ => {
                let (c, rest) = next_rune(t);
                p.literal(c)?;
                t = rest;
            }
        }
        last_repeat = repeat;
    }
    p.concat()?;
    if p.swap_vertical_bar() {
        p.stack.pop();
    }
    p.alternate()?;
    if p.stack.len() != 1 {
        return Err(perr(ErrorCode::MissingParen, s));
    }
    Ok(p.stack.pop().unwrap())
}

impl<'a> Parser<'a> {
    fn new_regexp(&mut self, op: Op) -> Regexp {
        self.lim.num_regexp += 1;
        Regexp::new(op)
    }

    fn check_limits(&mut self, re: &Regexp) -> Result<(), ErrorCode> {
        self.lim.check(re, &self.stack)
    }

    fn check_limits_top(&mut self) -> Result<(), ErrorCode> {
        let n = self.stack.len();
        let (rest, top) = self.stack.split_at(n - 1);
        self.lim.check(&top[0], rest)
    }

    /// Pushes `re` onto the stack; returns its index, or None when it was merged into the literal below.
    fn push(&mut self, mut re: Regexp) -> PResult<Option<usize>> {
        self.lim.num_runes += re.rune.len();
        if re.op == Op::CharClass && re.rune.len() == 2 && re.rune[0] == re.rune[1] {
            // Single rune.
            if self.maybe_concat(re.rune[0], self.flags & !FOLD_CASE) {
                return Ok(None);
            }
            re.op = Op::Literal;
            re.rune.truncate(1);
            re.flags = self.flags & !FOLD_CASE;
        } else if (re.op == Op::CharClass
            && re.rune.len() == 4
            && re.rune[0] == re.rune[1]
            && re.rune[2] == re.rune[3]
            && unicode::simple_fold(re.rune[0]) == re.rune[2]
            && unicode::simple_fold(re.rune[2]) == re.rune[0])
            || (re.op == Op::CharClass
                && re.rune.len() == 2
                && re.rune[0] + 1 == re.rune[1]
                && unicode::simple_fold(re.rune[0]) == re.rune[1]
                && unicode::simple_fold(re.rune[1]) == re.rune[0])
        {
            // Case-insensitive rune like [Aa] or [Δδ].
            if self.maybe_concat(re.rune[0], self.flags | FOLD_CASE) {
                return Ok(None);
            }
            re.op = Op::Literal;
            re.rune.truncate(1);
            re.flags = self.flags | FOLD_CASE;
        } else {
            self.maybe_concat(-1, 0);
        }
        self.stack.push(re);
        self.check_limits_top()?;
        Ok(Some(self.stack.len() - 1))
    }

    fn maybe_concat(&mut self, r: i32, flags: Flags) -> bool {
        let n = self.stack.len();
        if n < 2 {
            return false;
        }
        {
            let (re2, re1) = (&self.stack[n - 2], &self.stack[n - 1]);
            if re1.op != Op::Literal || re2.op != Op::Literal || re1.flags & FOLD_CASE != re2.flags & FOLD_CASE {
                return false;
            }
        }
        let re1_runes = std::mem::take(&mut self.stack[n - 1].rune);
        self.stack[n - 2].rune.extend_from_slice(&re1_runes);
        if r >= 0 {
            let re1 = &mut self.stack[n - 1];
            re1.rune = vec![r];
            re1.flags = flags;
            return true;
        }
        self.stack.pop();
        false
    }

    fn literal(&mut self, mut r: i32) -> PResult<()> {
        let mut re = self.new_regexp(Op::Literal);
        re.flags = self.flags;
        if self.flags & FOLD_CASE != 0 {
            r = min_fold_rune(r);
        }
        re.rune = vec![r];
        self.push(re)?;
        Ok(())
    }

    fn op(&mut self, op: Op) -> PResult<Option<usize>> {
        let mut re = self.new_regexp(op);
        re.flags = self.flags;
        self.push(re)
    }

    fn repeat<'s>(
        &mut self,
        op: Op,
        min: i32,
        max: i32,
        before: &'s str,
        mut after: &'s str,
        last_repeat: &'s str,
    ) -> PResult<&'s str> {
        let mut flags = self.flags;
        if self.flags & PERL_X != 0 {
            if after.as_bytes().first() == Some(&b'?') {
                after = &after[1..];
                flags ^= NON_GREEDY;
            }
            if !last_repeat.is_empty() {
                return Err(perr(ErrorCode::InvalidRepeatOp, cut(last_repeat, after)));
            }
        }
        let n = self.stack.len();
        if n == 0 || self.stack[n - 1].op >= Op::LeftParen {
            return Err(perr(ErrorCode::MissingRepeatArgument, cut(before, after)));
        }
        let sub = self.stack.pop().unwrap();
        let mut re = self.new_regexp(op);
        re.min = min;
        re.max = max;
        re.flags = flags;
        re.sub = vec![sub];
        self.stack.push(re);
        self.check_limits_top()?;
        if op == Op::Repeat && (min >= 2 || max >= 2) && !repeat_is_valid(self.stack.last().unwrap(), 1000) {
            return Err(perr(ErrorCode::InvalidRepeatSize, cut(before, after)));
        }
        Ok(after)
    }

    fn concat(&mut self) -> PResult<Option<usize>> {
        self.maybe_concat(-1, 0);
        let mut i = self.stack.len();
        while i > 0 && self.stack[i - 1].op < Op::LeftParen {
            i -= 1;
        }
        let subs = self.stack.split_off(i);
        if subs.is_empty() {
            let re = self.new_regexp(Op::EmptyMatch);
            return self.push(re);
        }
        let re = self.collapse(subs, Op::Concat)?;
        self.push(re)
    }

    fn alternate(&mut self) -> PResult<Option<usize>> {
        let mut i = self.stack.len();
        while i > 0 && self.stack[i - 1].op < Op::LeftParen {
            i -= 1;
        }
        let mut subs = self.stack.split_off(i);
        if let Some(last) = subs.last_mut() {
            clean_alt(last);
        }
        if subs.is_empty() {
            let re = self.new_regexp(Op::NoMatch);
            return self.push(re);
        }
        let re = self.collapse(subs, Op::Alternate)?;
        self.push(re)
    }

    fn collapse(&mut self, subs: Vec<Regexp>, op: Op) -> PResult<Regexp> {
        if subs.len() == 1 {
            return Ok(subs.into_iter().next().unwrap());
        }
        let mut re = self.new_regexp(op);
        for sub in subs {
            if sub.op == op {
                re.sub.extend(sub.sub);
            } else {
                re.sub.push(sub);
            }
        }
        if op == Op::Alternate {
            let sub = std::mem::take(&mut re.sub);
            re.sub = self.factor(sub)?;
            if re.sub.len() == 1 {
                re = re.sub.pop().unwrap();
            }
        }
        Ok(re)
    }

    fn factor(&mut self, mut sub: Vec<Regexp>) -> PResult<Vec<Regexp>> {
        if sub.len() < 2 {
            return Ok(sub);
        }

        // Round 1: Factor out common literal prefixes.
        let mut str: Vec<i32> = Vec::new();
        let mut strflags: Flags = 0;
        let mut start = 0;
        let mut out: Vec<Regexp> = Vec::with_capacity(sub.len());
        let len = sub.len();
        for i in 0..=len {
            let mut istr: Vec<i32> = Vec::new();
            let mut iflags: Flags = 0;
            if i < len {
                let (s, f) = leading_string(&sub[i]);
                istr = s.to_vec();
                iflags = f;
                if iflags == strflags {
                    let mut same = 0;
                    while same < str.len() && same < istr.len() && str[same] == istr[same] {
                        same += 1;
                    }
                    if same > 0 {
                        str.truncate(same);
                        continue;
                    }
                }
            }
            if i == start {
            } else if i == start + 1 {
                out.push(take(&mut sub[start]));
            } else {
                let mut prefix = self.new_regexp(Op::Literal);
                prefix.flags = strflags;
                prefix.rune = str.clone();
                let mut run = Vec::with_capacity(i - start);
                for j in start..i {
                    let r = remove_leading_string(take(&mut sub[j]), str.len());
                    self.check_limits(&r)?;
                    run.push(r);
                }
                let suffix = self.collapse(run, Op::Alternate)?;
                let mut re = self.new_regexp(Op::Concat);
                re.sub = vec![prefix, suffix];
                out.push(re);
            }
            start = i;
            str = istr;
            strflags = iflags;
        }
        sub = out;

        // Round 2: Factor out common simple prefixes, just the first piece of each concatenation.
        let mut start = 0;
        let mut out: Vec<Regexp> = Vec::with_capacity(sub.len());
        let len = sub.len();
        let mut have_first = false;
        for i in 0..=len {
            let mut ifirst_some = false;
            if i < len {
                ifirst_some = leading_regexp(&sub[i]).is_some();
                if have_first && ifirst_some {
                    let first = leading_regexp(&sub[start]).unwrap();
                    let ifirst = leading_regexp(&sub[i]).unwrap();
                    if first.equal(ifirst)
                        && (is_char_class(first)
                            || (first.op == Op::Repeat && first.min == first.max && is_char_class(&first.sub[0])))
                    {
                        continue;
                    }
                }
            }
            if i == start {
            } else if i == start + 1 {
                out.push(take(&mut sub[start]));
            } else {
                let prefix = leading_regexp(&sub[start]).unwrap().clone();
                let mut run = Vec::with_capacity(i - start);
                for j in start..i {
                    let r = self.remove_leading_regexp(take(&mut sub[j]));
                    self.check_limits(&r)?;
                    run.push(r);
                }
                let suffix = self.collapse(run, Op::Alternate)?;
                let mut re = self.new_regexp(Op::Concat);
                re.sub = vec![prefix, suffix];
                out.push(re);
            }
            start = i;
            have_first = ifirst_some;
        }
        sub = out;

        // Round 3: Collapse runs of single literals into character classes.
        let mut start = 0;
        let mut out: Vec<Regexp> = Vec::with_capacity(sub.len());
        let len = sub.len();
        for i in 0..=len {
            if i < len && is_char_class(&sub[i]) {
                continue;
            }
            if i == start {
            } else if i == start + 1 {
                out.push(take(&mut sub[start]));
            } else {
                let mut max = start;
                for j in start + 1..i {
                    if sub[max].op < sub[j].op || (sub[max].op == sub[j].op && sub[max].rune.len() < sub[j].rune.len())
                    {
                        max = j;
                    }
                }
                sub.swap(start, max);
                let mut dst = take(&mut sub[start]);
                for s in &sub[start + 1..i] {
                    merge_char_class(&mut dst, s);
                }
                clean_alt(&mut dst);
                out.push(dst);
            }
            if i < len {
                out.push(take(&mut sub[i]));
            }
            start = i + 1;
        }
        sub = out;

        // Round 4: Collapse runs of empty matches into a single empty match.
        let mut out: Vec<Regexp> = Vec::with_capacity(sub.len());
        let len = sub.len();
        for i in 0..len {
            if i + 1 < len && sub[i].op == Op::EmptyMatch && sub[i + 1].op == Op::EmptyMatch {
                continue;
            }
            out.push(take(&mut sub[i]));
        }
        Ok(out)
    }

    fn remove_leading_regexp(&mut self, mut re: Regexp) -> Regexp {
        if re.op == Op::Concat && !re.sub.is_empty() {
            re.sub.remove(0);
            match re.sub.len() {
                0 => {
                    re.op = Op::EmptyMatch;
                    re.sub.clear();
                }
                1 => return re.sub.pop().unwrap(),
                _ => {}
            }
            return re;
        }
        self.new_regexp(Op::EmptyMatch)
    }

    fn parse_repeat<'s>(&self, s: &'s str) -> Option<(i32, i32, &'s str)> {
        let b = s.as_bytes();
        if b.first() != Some(&b'{') {
            return None;
        }
        let s = &s[1..];
        let (mut min, s) = parse_int(s)?;
        if s.is_empty() {
            return None;
        }
        let max;
        let mut s = s;
        if s.as_bytes()[0] != b',' {
            max = min;
        } else {
            s = &s[1..];
            if s.is_empty() {
                return None;
            }
            if s.as_bytes()[0] == b'}' {
                max = -1;
            } else {
                let (m, rest) = parse_int(s)?;
                s = rest;
                max = m;
                if max < 0 {
                    min = -1;
                }
            }
        }
        if s.as_bytes().first() != Some(&b'}') {
            return None;
        }
        Some((min, max, &s[1..]))
    }

    fn parse_perl_flags<'s>(&mut self, s: &'s str) -> PResult<&'s str> {
        let t = s;
        let tb = t.as_bytes();
        let starts_with_p = tb.len() > 4 && tb[2] == b'P' && tb[3] == b'<';
        let starts_with_name = tb.len() > 3 && tb[2] == b'<';
        if starts_with_p || starts_with_name {
            let expr_start = if starts_with_name { 3 } else { 4 };
            let end = match t.find('>') {
                Some(e) => e,
                None => return Err(perr(ErrorCode::InvalidNamedCapture, s)),
            };
            let capture = &t[..end + 1];
            let name = if expr_start <= end { &t[expr_start..end] } else { "" };
            if !is_valid_capture_name(name) {
                return Err(perr(ErrorCode::InvalidNamedCapture, capture));
            }
            self.num_cap += 1;
            let cap = self.num_cap;
            let i = self.op(Op::LeftParen)?.expect("left paren pushed");
            self.stack[i].cap = cap;
            self.stack[i].name = name.to_string();
            return Ok(&t[end + 1..]);
        }

        let mut t = &t[2..];
        let mut flags = self.flags;
        let mut sign = 1;
        let mut saw_flag = false;
        while !t.is_empty() {
            let (c, rest) = next_rune(t);
            t = rest;
            match char::from_u32(c as u32).unwrap_or('\u{FFFD}') {
                'i' => {
                    flags |= FOLD_CASE;
                    saw_flag = true;
                }
                'm' => {
                    flags &= !ONE_LINE;
                    saw_flag = true;
                }
                's' => {
                    flags |= DOT_NL;
                    saw_flag = true;
                }
                'U' => {
                    flags |= NON_GREEDY;
                    saw_flag = true;
                }
                '-' => {
                    if sign < 0 {
                        break;
                    }
                    sign = -1;
                    flags = !flags;
                    saw_flag = false;
                }
                ':' | ')' => {
                    if sign < 0 {
                        if !saw_flag {
                            break;
                        }
                        flags = !flags;
                    }
                    if c == ':' as i32 {
                        self.op(Op::LeftParen)?;
                    }
                    self.flags = flags;
                    return Ok(t);
                }
                _ => break,
            }
        }
        Err(perr(ErrorCode::InvalidPerlOp, cut(s, t)))
    }

    fn parse_vertical_bar(&mut self) -> PResult<()> {
        self.concat()?;
        if !self.swap_vertical_bar() {
            self.op(Op::VerticalBar)?;
        }
        Ok(())
    }

    fn swap_vertical_bar(&mut self) -> bool {
        let n = self.stack.len();
        if n >= 3
            && self.stack[n - 2].op == Op::VerticalBar
            && is_char_class(&self.stack[n - 1])
            && is_char_class(&self.stack[n - 3])
        {
            let mut re1 = self.stack.pop().unwrap();
            let re3 = &mut self.stack[n - 3];
            if re1.op > re3.op {
                std::mem::swap(&mut re1, re3);
            }
            merge_char_class(re3, &re1);
            return true;
        }
        if n >= 2 && self.stack[n - 2].op == Op::VerticalBar {
            if n >= 3 {
                clean_alt(&mut self.stack[n - 3]);
            }
            self.stack.swap(n - 2, n - 1);
            return true;
        }
        false
    }

    fn parse_right_paren(&mut self) -> PResult<()> {
        self.concat()?;
        if self.swap_vertical_bar() {
            self.stack.pop();
        }
        self.alternate()?;
        let n = self.stack.len();
        if n < 2 {
            return Err(perr(ErrorCode::UnexpectedParen, self.whole));
        }
        let re1 = self.stack.pop().unwrap();
        let mut re2 = self.stack.pop().unwrap();
        if re2.op != Op::LeftParen {
            return Err(perr(ErrorCode::UnexpectedParen, self.whole));
        }
        self.flags = re2.flags;
        if re2.cap == 0 {
            self.push(re1)?;
        } else {
            re2.op = Op::Capture;
            re2.sub = vec![re1];
            self.push(re2)?;
        }
        Ok(())
    }

    fn parse_backslash<'s>(&mut self, t: &'s str) -> PResult<&'s str> {
        let b = t.as_bytes();
        if self.flags & PERL_X != 0 && b.len() >= 2 {
            match b[1] {
                b'A' => {
                    self.op(Op::BeginText)?;
                    return Ok(&t[2..]);
                }
                b'b' => {
                    self.op(Op::WordBoundary)?;
                    return Ok(&t[2..]);
                }
                b'B' => {
                    self.op(Op::NoWordBoundary)?;
                    return Ok(&t[2..]);
                }
                b'C' => return Err(perr(ErrorCode::InvalidEscape, &t[..2])),
                b'Q' => {
                    let rest = &t[2..];
                    let (lit, after) = match rest.find("\\E") {
                        Some(i) => (&rest[..i], &rest[i + 2..]),
                        None => (rest, ""),
                    };
                    for c in lit.chars() {
                        self.literal(c as i32)?;
                    }
                    return Ok(after);
                }
                b'z' => {
                    self.op(Op::EndText)?;
                    return Ok(&t[2..]);
                }
                _ => {}
            }
        }
        let mut re = self.new_regexp(Op::CharClass);
        re.flags = self.flags;
        if b.len() >= 2 && (b[1] == b'p' || b[1] == b'P') {
            let mut r = Vec::new();
            if let Some(rest) = self.parse_unicode_class(t, &mut r)? {
                re.rune = r;
                self.push(re)?;
                return Ok(rest);
            }
        }
        let mut r = Vec::new();
        if let Some(rest) = self.parse_perl_class_escape(t, &mut r) {
            re.rune = r;
            self.push(re)?;
            return Ok(rest);
        }
        let (c, rest) = parse_escape(t)?;
        self.literal(c)?;
        Ok(rest)
    }

    fn parse_perl_class_escape<'s>(&mut self, s: &'s str, r: &mut Vec<i32>) -> Option<&'s str> {
        let b = s.as_bytes();
        if self.flags & PERL_X == 0 || b.len() < 2 || b[0] != b'\\' {
            return None;
        }
        let g = perl_group(b[1])?;
        self.append_group(r, g);
        Some(&s[2..])
    }

    fn parse_named_class<'s>(&mut self, s: &'s str, r: &mut Vec<i32>) -> PResult<Option<&'s str>> {
        let b = s.as_bytes();
        if b.len() < 2 || b[0] != b'[' || b[1] != b':' {
            return Ok(None);
        }
        let i = match s[2..].find(":]") {
            Some(i) => i + 2,
            None => return Ok(None),
        };
        let name = &s[..i + 2];
        let rest = &s[i + 2..];
        match posix_group(name) {
            Some(g) => {
                self.append_group(r, g);
                Ok(Some(rest))
            }
            None => Err(perr(ErrorCode::InvalidCharRange, name)),
        }
    }

    fn append_group(&mut self, r: &mut Vec<i32>, g: CharGroup) {
        if self.flags & FOLD_CASE == 0 {
            if g.sign < 0 {
                append_negated_class(r, g.class);
            } else {
                append_class(r, g.class);
            }
        } else {
            let mut tmp = Vec::new();
            append_folded_class(&mut tmp, g.class);
            clean_class(&mut tmp);
            if g.sign < 0 {
                append_negated_class(r, &tmp);
            } else {
                append_class(r, &tmp);
            }
        }
    }

    /// Parses a leading `\p{Han}`-style class. Returns the rest of the input, or None if there is none.
    fn parse_unicode_class<'s>(&mut self, s: &'s str, r: &mut Vec<i32>) -> PResult<Option<&'s str>> {
        let b = s.as_bytes();
        if self.flags & UNICODE_GROUPS == 0 || b.len() < 2 || b[0] != b'\\' || (b[1] != b'p' && b[1] != b'P') {
            return Ok(None);
        }
        let mut sign = if b[1] == b'P' { -1 } else { 1 };
        let t = &s[2..];
        let (c, t) = next_rune(t);
        let (seq, mut name, t) = if c != '{' as i32 {
            let seq = cut(s, t);
            (seq, &seq[2..], t)
        } else {
            let end = match s.find('}') {
                Some(e) => e,
                None => return Err(perr(ErrorCode::InvalidCharRange, s)),
            };
            (&s[..end + 1], &s[3..end], &s[end + 1..])
        };
        if name.as_bytes().first() == Some(&b'^') {
            sign = -sign;
            name = &name[1..];
        }
        let (tab, fold): (unicode::RangeTable, Option<unicode::RangeTable>) = if name == "Any" {
            (ANY_TABLE, Some(ANY_TABLE))
        } else if let Some(t) = unicode::category(name) {
            (t, unicode::fold_category(name))
        } else if let Some(t) = unicode::script(name) {
            (t, unicode::fold_script(name))
        } else {
            return Err(perr(ErrorCode::InvalidCharRange, seq));
        };
        match fold {
            Some(fold) if self.flags & FOLD_CASE != 0 => {
                let mut tmp = Vec::new();
                append_table(&mut tmp, tab);
                append_table(&mut tmp, fold);
                clean_class(&mut tmp);
                if sign > 0 {
                    append_class(r, &tmp);
                } else {
                    append_negated_class(r, &tmp);
                }
            }
            _ => {
                if sign > 0 {
                    append_table(r, tab);
                } else {
                    append_negated_table(r, tab);
                }
            }
        }
        Ok(Some(t))
    }

    fn parse_class<'s>(&mut self, s: &'s str) -> PResult<&'s str> {
        let mut t = &s[1..];
        let mut re = self.new_regexp(Op::CharClass);
        re.flags = self.flags;
        let mut class: Vec<i32> = Vec::new();
        let mut sign = 1;
        if t.as_bytes().first() == Some(&b'^') {
            sign = -1;
            t = &t[1..];
            if self.flags & CLASS_NL == 0 {
                class.push('\n' as i32);
                class.push('\n' as i32);
            }
        }
        let mut first = true;
        while t.is_empty() || t.as_bytes()[0] != b']' || first {
            let tb = t.as_bytes();
            if !tb.is_empty()
                && tb[0] == b'-'
                && self.flags & PERL_X == 0
                && !first
                && (tb.len() == 1 || tb[1] != b']')
            {
                let size = t[1..].chars().next().map_or(0, |c| c.len_utf8());
                return Err(perr(ErrorCode::InvalidCharRange, &t[..1 + size]));
            }
            first = false;

            if tb.len() > 2 && tb[0] == b'[' && tb[1] == b':' {
                if let Some(nt) = self.parse_named_class(t, &mut class)? {
                    t = nt;
                    continue;
                }
            }
            if let Some(nt) = self.parse_unicode_class(t, &mut class)? {
                t = nt;
                continue;
            }
            if let Some(nt) = self.parse_perl_class_escape(t, &mut class) {
                t = nt;
                continue;
            }
            let rng = t;
            let (lo, nt) = parse_class_char(t, s)?;
            t = nt;
            let mut hi = lo;
            let tb = t.as_bytes();
            if tb.len() >= 2 && tb[0] == b'-' && tb[1] != b']' {
                t = &t[1..];
                let (h, nt) = parse_class_char(t, s)?;
                t = nt;
                hi = h;
                if hi < lo {
                    return Err(perr(ErrorCode::InvalidCharRange, cut(rng, t)));
                }
            }
            if self.flags & FOLD_CASE == 0 {
                append_range(&mut class, lo, hi);
            } else {
                append_folded_range(&mut class, lo, hi);
            }
        }
        t = &t[1..];
        clean_class(&mut class);
        if sign < 0 {
            negate_class(&mut class);
        }
        re.rune = class;
        self.push(re)?;
        Ok(t)
    }
}

fn take(r: &mut Regexp) -> Regexp {
    std::mem::replace(r, Regexp::new(Op::EmptyMatch))
}

fn repeat_is_valid(re: &Regexp, mut n: i32) -> bool {
    if re.op == Op::Repeat {
        let mut m = re.max;
        if m == 0 {
            return true;
        }
        if m < 0 {
            m = re.min;
        }
        if m > n {
            return false;
        }
        if m > 0 {
            n /= m;
        }
    }
    re.sub.iter().all(|s| repeat_is_valid(s, n))
}

fn min_fold_rune(r: i32) -> i32 {
    if !(MIN_FOLD..=MAX_FOLD).contains(&r) {
        return r;
    }
    let mut m = r;
    let r0 = r;
    let mut r = unicode::simple_fold(r);
    while r != r0 {
        m = m.min(r);
        r = unicode::simple_fold(r);
    }
    m
}

fn clean_alt(re: &mut Regexp) {
    if re.op == Op::CharClass {
        clean_class(&mut re.rune);
        if re.rune.len() == 2 && re.rune[0] == 0 && re.rune[1] == unicode::MAX_RUNE {
            re.rune.clear();
            re.op = Op::AnyChar;
            return;
        }
        if re.rune.len() == 4
            && re.rune[0] == 0
            && re.rune[1] == '\n' as i32 - 1
            && re.rune[2] == '\n' as i32 + 1
            && re.rune[3] == unicode::MAX_RUNE
        {
            re.rune.clear();
            re.op = Op::AnyCharNotNL;
        }
    }
}

fn leading_string(re: &Regexp) -> (&[i32], Flags) {
    let re = if re.op == Op::Concat && !re.sub.is_empty() { &re.sub[0] } else { re };
    if re.op != Op::Literal {
        return (&[], 0);
    }
    (&re.rune, re.flags & FOLD_CASE)
}

fn remove_leading_string(mut re: Regexp, n: usize) -> Regexp {
    if re.op == Op::Concat && !re.sub.is_empty() {
        let s0 = remove_leading_string(take(&mut re.sub[0]), n);
        let empty = s0.op == Op::EmptyMatch;
        re.sub[0] = s0;
        if empty {
            match re.sub.len() {
                0 | 1 => {
                    re.op = Op::EmptyMatch;
                    re.sub.clear();
                }
                2 => return re.sub.pop().unwrap(),
                _ => {
                    re.sub.remove(0);
                }
            }
        }
        return re;
    }
    if re.op == Op::Literal {
        re.rune.drain(..n.min(re.rune.len()));
        if re.rune.is_empty() {
            re.op = Op::EmptyMatch;
        }
    }
    re
}

fn leading_regexp(re: &Regexp) -> Option<&Regexp> {
    if re.op == Op::EmptyMatch {
        return None;
    }
    if re.op == Op::Concat && !re.sub.is_empty() {
        let sub = &re.sub[0];
        if sub.op == Op::EmptyMatch {
            return None;
        }
        return Some(sub);
    }
    Some(re)
}

fn is_valid_capture_name(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(|c| c == b'_' || c.is_ascii_alphanumeric())
}

/// Go `parseInt`: a decimal integer without leading zeros; -1 when it is too big.
fn parse_int(s: &str) -> Option<(i32, &str)> {
    let b = s.as_bytes();
    if b.is_empty() || !b[0].is_ascii_digit() {
        return None;
    }
    if b.len() >= 2 && b[0] == b'0' && b[1].is_ascii_digit() {
        return None;
    }
    let mut i = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    let mut n: i64 = 0;
    for &c in &b[..i] {
        if n >= 100_000_000 {
            n = -1;
            break;
        }
        n = n * 10 + (c - b'0') as i64;
    }
    Some((n as i32, &s[i..]))
}

/// Whether `re` can be represented as a character class.
pub fn is_char_class(re: &Regexp) -> bool {
    (re.op == Op::Literal && re.rune.len() == 1)
        || re.op == Op::CharClass
        || re.op == Op::AnyCharNotNL
        || re.op == Op::AnyChar
}

fn match_rune(re: &Regexp, r: i32) -> bool {
    match re.op {
        Op::Literal => re.rune.len() == 1 && re.rune[0] == r,
        Op::CharClass => re.rune.chunks(2).any(|p| p[0] <= r && r <= p[1]),
        Op::AnyCharNotNL => r != '\n' as i32,
        Op::AnyChar => true,
        _ => false,
    }
}

fn merge_char_class(dst: &mut Regexp, src: &Regexp) {
    match dst.op {
        Op::AnyChar => {}
        Op::AnyCharNotNL => {
            if match_rune(src, '\n' as i32) {
                dst.op = Op::AnyChar;
            }
        }
        Op::CharClass => {
            if src.op == Op::Literal {
                append_literal(&mut dst.rune, src.rune[0], src.flags);
            } else {
                append_class(&mut dst.rune, &src.rune);
            }
        }
        Op::Literal => {
            if src.rune[0] == dst.rune[0] && src.flags == dst.flags {
                return;
            }
            dst.op = Op::CharClass;
            let d0 = dst.rune[0];
            dst.rune.clear();
            append_literal(&mut dst.rune, d0, dst.flags);
            append_literal(&mut dst.rune, src.rune[0], src.flags);
        }
        _ => {}
    }
}

/// Go `parseEscape`: parses an escape sequence at the start of `s` (which starts with a backslash).
fn parse_escape(s: &str) -> PResult<(i32, &str)> {
    let t = &s[1..];
    if t.is_empty() {
        return Err(perr(ErrorCode::TrailingBackslash, ""));
    }
    let (c, mut t) = next_rune(t);
    let ch = char::from_u32(c as u32).unwrap_or('\u{FFFD}');
    'sw: {
        match ch {
            '1'..='7' | '0' => {
                if ch != '0' && (t.is_empty() || !(b'0'..=b'7').contains(&t.as_bytes()[0])) {
                    break 'sw;
                }
                let mut r = c - '0' as i32;
                for _ in 1..3 {
                    let tb = t.as_bytes();
                    if tb.is_empty() || !(b'0'..=b'7').contains(&tb[0]) {
                        break;
                    }
                    r = r * 8 + (tb[0] - b'0') as i32;
                    t = &t[1..];
                }
                return Ok((r, t));
            }
            'x' => {
                if t.is_empty() {
                    break 'sw;
                }
                let (c, nt) = next_rune(t);
                t = nt;
                if c == '{' as i32 {
                    let mut nhex = 0;
                    let mut r: i32 = 0;
                    loop {
                        if t.is_empty() {
                            break 'sw;
                        }
                        let (c, nt) = next_rune(t);
                        t = nt;
                        if c == '}' as i32 {
                            break;
                        }
                        let v = unhex(c);
                        if v < 0 {
                            break 'sw;
                        }
                        r = r * 16 + v;
                        if r > unicode::MAX_RUNE {
                            break 'sw;
                        }
                        nhex += 1;
                    }
                    if nhex == 0 {
                        break 'sw;
                    }
                    return Ok((r, t));
                }
                let x = unhex(c);
                let (c, nt) = next_rune(t);
                t = nt;
                let y = unhex(c);
                if x < 0 || y < 0 {
                    break 'sw;
                }
                return Ok((x * 16 + y, t));
            }
            'a' => return Ok((7, t)),
            'f' => return Ok((12, t)),
            'n' => return Ok((10, t)),
            'r' => return Ok((13, t)),
            't' => return Ok((9, t)),
            'v' => return Ok((11, t)),
            _ => {
                if c < 0x80 && !(ch.is_ascii_alphanumeric()) {
                    return Ok((c, t));
                }
            }
        }
    }
    Err(perr(ErrorCode::InvalidEscape, cut(s, t)))
}

fn parse_class_char<'s>(s: &'s str, whole_class: &str) -> PResult<(i32, &'s str)> {
    if s.is_empty() {
        return Err(perr(ErrorCode::MissingBracket, whole_class));
    }
    if s.as_bytes()[0] == b'\\' {
        return parse_escape(s);
    }
    Ok(next_rune(s))
}

fn unhex(c: i32) -> i32 {
    match char::from_u32(c as u32) {
        Some(d @ '0'..='9') => d as i32 - '0' as i32,
        Some(d @ 'a'..='f') => d as i32 - 'a' as i32 + 10,
        Some(d @ 'A'..='F') => d as i32 - 'A' as i32 + 10,
        _ => -1,
    }
}

#[derive(Clone, Copy)]
struct CharGroup {
    sign: i32,
    class: &'static [i32],
}

const CODE_DIGIT: &[i32] = &[0x30, 0x39];
const CODE_SPACE: &[i32] = &[0x9, 0xa, 0xc, 0xd, 0x20, 0x20];
const CODE_WORD: &[i32] = &[0x30, 0x39, 0x41, 0x5a, 0x5f, 0x5f, 0x61, 0x7a];

fn perl_group(c: u8) -> Option<CharGroup> {
    let (sign, class) = match c {
        b'd' => (1, CODE_DIGIT),
        b'D' => (-1, CODE_DIGIT),
        b's' => (1, CODE_SPACE),
        b'S' => (-1, CODE_SPACE),
        b'w' => (1, CODE_WORD),
        b'W' => (-1, CODE_WORD),
        _ => return None,
    };
    Some(CharGroup { sign, class })
}

fn posix_group(name: &str) -> Option<CharGroup> {
    let (neg, base) = match name.strip_prefix("[:^") {
        Some(rest) => (true, rest),
        None => (false, name.strip_prefix("[:")?),
    };
    let class: &'static [i32] = match base {
        "alnum:]" => &[0x30, 0x39, 0x41, 0x5a, 0x61, 0x7a],
        "alpha:]" => &[0x41, 0x5a, 0x61, 0x7a],
        "ascii:]" => &[0x0, 0x7f],
        "blank:]" => &[0x9, 0x9, 0x20, 0x20],
        "cntrl:]" => &[0x0, 0x1f, 0x7f, 0x7f],
        "digit:]" => &[0x30, 0x39],
        "graph:]" => &[0x21, 0x7e],
        "lower:]" => &[0x61, 0x7a],
        "print:]" => &[0x20, 0x7e],
        "punct:]" => &[0x21, 0x2f, 0x3a, 0x40, 0x5b, 0x60, 0x7b, 0x7e],
        "space:]" => &[0x9, 0xd, 0x20, 0x20],
        "upper:]" => &[0x41, 0x5a],
        "word:]" => &[0x30, 0x39, 0x41, 0x5a, 0x5f, 0x5f, 0x61, 0x7a],
        "xdigit:]" => &[0x30, 0x39, 0x41, 0x46, 0x61, 0x66],
        _ => return None,
    };
    Some(CharGroup { sign: if neg { -1 } else { 1 }, class })
}

const ANY_TABLE: unicode::RangeTable = &[(0, 0xFFFF, 1), (0x10000, 0x10FFFF, 1)];

/// Sorts the ranges (pairs of elements of r), merges them, and eliminates duplicates.
pub fn clean_class(r: &mut Vec<i32>) {
    let mut pairs: Vec<(i32, i32)> = r.chunks(2).map(|p| (p[0], p[1])).collect();
    // Sort by lo increasing, hi decreasing to break ties.
    pairs.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
    r.clear();
    for (lo, hi) in pairs {
        if r.len() >= 2 {
            let w = r.len();
            if lo <= r[w - 1] + 1 {
                if hi > r[w - 1] {
                    r[w - 1] = hi;
                }
                continue;
            }
        }
        r.push(lo);
        r.push(hi);
    }
}

/// Whether `r` is in the (cleaned) class.
pub fn in_char_class(r: i32, class: &[i32]) -> bool {
    let (mut lo, mut hi) = (0, class.len() / 2);
    while lo < hi {
        let m = (lo + hi) / 2;
        if class[2 * m + 1] < r {
            lo = m + 1;
        } else {
            hi = m;
        }
    }
    lo < class.len() / 2 && class[2 * lo] <= r && r <= class[2 * lo + 1]
}

fn append_literal(r: &mut Vec<i32>, x: i32, flags: Flags) {
    if flags & FOLD_CASE != 0 {
        append_folded_range(r, x, x);
    } else {
        append_range(r, x, x);
    }
}

fn append_range(r: &mut Vec<i32>, lo: i32, hi: i32) {
    let n = r.len();
    let mut i = 2;
    while i <= 4 {
        if n >= i {
            let (rlo, rhi) = (r[n - i], r[n - i + 1]);
            if lo <= rhi + 1 && rlo <= hi + 1 {
                if lo < rlo {
                    r[n - i] = lo;
                }
                if hi > rhi {
                    r[n - i + 1] = hi;
                }
                return;
            }
        }
        i += 2;
    }
    r.push(lo);
    r.push(hi);
}

fn append_folded_range(r: &mut Vec<i32>, mut lo: i32, mut hi: i32) {
    if lo <= MIN_FOLD && hi >= MAX_FOLD {
        append_range(r, lo, hi);
        return;
    }
    if hi < MIN_FOLD || lo > MAX_FOLD {
        append_range(r, lo, hi);
        return;
    }
    if lo < MIN_FOLD {
        append_range(r, lo, MIN_FOLD - 1);
        lo = MIN_FOLD;
    }
    if hi > MAX_FOLD {
        append_range(r, MAX_FOLD + 1, hi);
        hi = MAX_FOLD;
    }
    for c in lo..=hi {
        append_range(r, c, c);
        let mut f = unicode::simple_fold(c);
        while f != c {
            append_range(r, f, f);
            f = unicode::simple_fold(f);
        }
    }
}

fn append_class(r: &mut Vec<i32>, x: &[i32]) {
    for p in x.chunks(2) {
        append_range(r, p[0], p[1]);
    }
}

fn append_folded_class(r: &mut Vec<i32>, x: &[i32]) {
    for p in x.chunks(2) {
        append_folded_range(r, p[0], p[1]);
    }
}

fn append_negated_class(r: &mut Vec<i32>, x: &[i32]) {
    let mut next_lo = 0;
    for p in x.chunks(2) {
        let (lo, hi) = (p[0], p[1]);
        if next_lo <= lo - 1 {
            append_range(r, next_lo, lo - 1);
        }
        next_lo = hi + 1;
    }
    if next_lo <= unicode::MAX_RUNE {
        append_range(r, next_lo, unicode::MAX_RUNE);
    }
}

fn append_table(r: &mut Vec<i32>, x: unicode::RangeTable) {
    for &(lo, hi, stride) in x {
        let (lo, hi, stride) = (lo as i32, hi as i32, stride as i32);
        if stride == 1 {
            append_range(r, lo, hi);
            continue;
        }
        let mut c = lo;
        while c <= hi {
            append_range(r, c, c);
            c += stride;
        }
    }
}

fn append_negated_table(r: &mut Vec<i32>, x: unicode::RangeTable) {
    let mut next_lo = 0;
    for &(lo, hi, stride) in x {
        let (lo, hi, stride) = (lo as i32, hi as i32, stride as i32);
        if stride == 1 {
            if next_lo <= lo - 1 {
                append_range(r, next_lo, lo - 1);
            }
            next_lo = hi + 1;
            continue;
        }
        let mut c = lo;
        while c <= hi {
            if next_lo <= c - 1 {
                append_range(r, next_lo, c - 1);
            }
            next_lo = c + 1;
            c += stride;
        }
    }
    if next_lo <= unicode::MAX_RUNE {
        append_range(r, next_lo, unicode::MAX_RUNE);
    }
}

fn negate_class(r: &mut Vec<i32>) {
    let mut next_lo = 0;
    let mut out = Vec::with_capacity(r.len() + 2);
    for p in r.chunks(2) {
        let (lo, hi) = (p[0], p[1]);
        if next_lo <= lo - 1 {
            out.push(next_lo);
            out.push(lo - 1);
        }
        next_lo = hi + 1;
    }
    if next_lo <= unicode::MAX_RUNE {
        out.push(next_lo);
        out.push(unicode::MAX_RUNE);
    }
    *r = out;
}
