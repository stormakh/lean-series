//! Port of Go `text/template/parse/lex.go`.

use model::unicode::{is_digit, is_letter, is_print};

use crate::gofmt::quote;

/// Token types (Go `itemType`); the order matters (`> Keyword` are keywords).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ItemType {
    Error,
    Bool,
    Char,
    CharConstant,
    Comment,
    Complex,
    Assign,
    Declare,
    Eof,
    Field,
    Identifier,
    LeftDelim,
    LeftParen,
    Number,
    Pipe,
    RawString,
    RightDelim,
    RightParen,
    Space,
    String,
    Text,
    Variable,
    Keyword,
    Block,
    Break,
    Continue,
    Dot,
    Define,
    Else,
    End,
    If,
    Nil,
    Range,
    Template,
    With,
}

/// A token (Go `item`).
#[derive(Clone, Debug)]
pub struct Item {
    pub typ: ItemType,
    pub pos: usize,
    pub val: String,
    pub line: usize,
}

impl std::fmt::Display for Item {
    /// Go `item.String`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.typ {
            ItemType::Eof => f.write_str("EOF"),
            ItemType::Error => f.write_str(&self.val),
            t if t > ItemType::Keyword => write!(f, "<{}>", self.val),
            _ => {
                if self.val.len() > 10 {
                    // %.10q: the first 10 runes, quoted.
                    let s: String = self.val.chars().take(10).collect();
                    write!(f, "{}...", quote(&s))
                } else {
                    f.write_str(&quote(&self.val))
                }
            }
        }
    }
}

fn keyword(word: &str) -> Option<ItemType> {
    Some(match word {
        "." => ItemType::Dot,
        "block" => ItemType::Block,
        "break" => ItemType::Break,
        "continue" => ItemType::Continue,
        "define" => ItemType::Define,
        "else" => ItemType::Else,
        "end" => ItemType::End,
        "if" => ItemType::If,
        "range" => ItemType::Range,
        "nil" => ItemType::Nil,
        "template" => ItemType::Template,
        "with" => ItemType::With,
        _ => return None,
    })
}

const LEFT_DELIM: &str = "{{";
const RIGHT_DELIM: &str = "}}";
const LEFT_COMMENT: &str = "/*";
const RIGHT_COMMENT: &str = "*/";
const SPACE_CHARS: &[char] = &[' ', '\t', '\r', '\n'];
const TRIM_MARKER_LEN: usize = 2;

/// Lexer options (Go `lexOptions`).
#[derive(Clone, Copy, Default)]
pub struct LexOptions {
    pub emit_comment: bool,
    pub break_ok: bool,
    pub continue_ok: bool,
}

#[derive(Clone, Copy)]
enum State {
    Text,
    LeftDelim,
    Comment,
    RightDelim,
    InsideAction,
    Space,
    Identifier,
    Field,
    Variable,
    Char,
    Number,
    Quote,
    RawQuote,
}

/// The lexer (Go `lexer`).
pub struct Lexer<'a> {
    input: &'a str,
    /// Set by `errorf`: Go empties the input so that the next item is EOF.
    end: usize,
    left_delim: String,
    right_delim: String,
    pos: usize,
    start: usize,
    at_eof: bool,
    paren_depth: i32,
    line: usize,
    start_line: usize,
    item: Option<Item>,
    inside_action: bool,
    pub options: LexOptions,
}

const EOF: Option<char> = None;

fn is_space(r: Option<char>) -> bool {
    matches!(r, Some(' ' | '\t' | '\r' | '\n'))
}

fn is_alpha_numeric(r: Option<char>) -> bool {
    match r {
        Some(c) => c == '_' || is_letter(c) || is_digit(c),
        None => false,
    }
}

fn has_left_trim_marker(s: &[u8]) -> bool {
    s.len() >= 2 && s[0] == b'-' && is_space(Some(s[1] as char))
}

fn has_right_trim_marker(s: &[u8]) -> bool {
    s.len() >= 2 && is_space(Some(s[0] as char)) && s[1] == b'-'
}

/// Go `%#U` of a rune.
fn sharp_u(r: Option<char>) -> String {
    match r {
        None => "U+FFFFFFFFFFFFFFFF".to_string(),
        Some(c) => {
            let mut s = format!("U+{:04X}", c as u32);
            if is_print(c) {
                s.push_str(" '");
                s.push(c);
                s.push('\'');
            }
            s
        }
    }
}

impl<'a> Lexer<'a> {
    /// Go `lex`.
    pub fn new(input: &'a str, left: &str, right: &str) -> Lexer<'a> {
        Lexer {
            input,
            end: input.len(),
            left_delim: if left.is_empty() { LEFT_DELIM.to_string() } else { left.to_string() },
            right_delim: if right.is_empty() { RIGHT_DELIM.to_string() } else { right.to_string() },
            pos: 0,
            start: 0,
            at_eof: false,
            paren_depth: 0,
            line: 1,
            start_line: 1,
            item: None,
            inside_action: false,
            options: LexOptions::default(),
        }
    }

    fn rest(&self) -> &str {
        &self.input[self.pos.min(self.end)..self.end]
    }

    fn next(&mut self) -> Option<char> {
        if self.pos >= self.end {
            self.at_eof = true;
            return EOF;
        }
        let r = self.input[self.pos..self.end].chars().next()?;
        self.pos += r.len_utf8();
        if r == '\n' {
            self.line += 1;
        }
        Some(r)
    }

    fn peek(&mut self) -> Option<char> {
        let r = self.next();
        self.backup();
        r
    }

    fn backup(&mut self) {
        if !self.at_eof && self.pos > 0 {
            if let Some(r) = self.input[..self.pos].chars().next_back() {
                self.pos -= r.len_utf8();
                if r == '\n' {
                    self.line -= 1;
                }
            }
        }
    }

    fn this_item(&mut self, t: ItemType) -> Item {
        let i = Item { typ: t, pos: self.start, val: self.input[self.start..self.pos].to_string(), line: self.start_line };
        self.start = self.pos;
        self.start_line = self.line;
        i
    }

    fn emit(&mut self, t: ItemType) -> Option<State> {
        let i = self.this_item(t);
        self.emit_item(i)
    }

    fn emit_item(&mut self, i: Item) -> Option<State> {
        self.item = Some(i);
        None
    }

    fn ignore(&mut self) {
        self.line += self.input[self.start..self.pos].matches('\n').count();
        self.start = self.pos;
        self.start_line = self.line;
    }

    fn accept(&mut self, valid: &str) -> bool {
        match self.next() {
            Some(c) if valid.contains(c) => true,
            _ => {
                self.backup();
                false
            }
        }
    }

    fn accept_run(&mut self, valid: &str) {
        loop {
            match self.next() {
                Some(c) if valid.contains(c) => {}
                _ => break,
            }
        }
        self.backup();
    }

    fn errorf(&mut self, msg: String) -> Option<State> {
        self.item = Some(Item { typ: ItemType::Error, pos: self.start, val: msg, line: self.start_line });
        self.start = 0;
        self.pos = 0;
        self.end = 0;
        None
    }

    /// Go `nextItem`.
    pub fn next_item(&mut self) -> Item {
        self.item = Some(Item { typ: ItemType::Eof, pos: self.pos, val: "EOF".to_string(), line: self.start_line });
        let mut state = if self.inside_action { State::InsideAction } else { State::Text };
        loop {
            let next = match state {
                State::Text => self.lex_text(),
                State::LeftDelim => self.lex_left_delim(),
                State::Comment => self.lex_comment(),
                State::RightDelim => self.lex_right_delim(),
                State::InsideAction => self.lex_inside_action(),
                State::Space => self.lex_space(),
                State::Identifier => self.lex_identifier(),
                State::Field => self.lex_field_or_variable(ItemType::Field),
                State::Variable => self.lex_variable(),
                State::Char => self.lex_char(),
                State::Number => self.lex_number(),
                State::Quote => self.lex_quote(),
                State::RawQuote => self.lex_raw_quote(),
            };
            match next {
                Some(s) => state = s,
                None => return self.item.take().unwrap_or(Item { typ: ItemType::Eof, pos: self.pos, val: "EOF".into(), line: self.line }),
            }
        }
    }

    fn lex_text(&mut self) -> Option<State> {
        let rest = &self.input[self.pos..self.end];
        if let Some(x) = rest.find(self.left_delim.as_str()) {
            if x > 0 {
                self.pos += x;
                let mut trim_length = 0;
                let delim_end = self.pos + self.left_delim.len();
                if has_left_trim_marker(&self.input.as_bytes()[delim_end..self.end]) {
                    let s = &self.input[self.start..self.pos];
                    trim_length = s.len() - s.trim_end_matches(SPACE_CHARS).len();
                }
                self.pos -= trim_length;
                self.line += self.input[self.start..self.pos].matches('\n').count();
                let i = self.this_item(ItemType::Text);
                self.pos += trim_length;
                self.ignore();
                if !i.val.is_empty() {
                    return self.emit_item(i);
                }
            }
            return Some(State::LeftDelim);
        }
        self.pos = self.end;
        if self.pos > self.start {
            self.line += self.input[self.start..self.pos].matches('\n').count();
            return self.emit(ItemType::Text);
        }
        self.emit(ItemType::Eof)
    }

    fn at_right_delim(&self) -> (bool, bool) {
        let rest = self.rest().as_bytes();
        if has_right_trim_marker(rest) && self.rest()[TRIM_MARKER_LEN..].starts_with(self.right_delim.as_str()) {
            return (true, true);
        }
        if self.rest().starts_with(self.right_delim.as_str()) {
            return (true, false);
        }
        (false, false)
    }

    fn lex_left_delim(&mut self) -> Option<State> {
        self.pos += self.left_delim.len();
        let trim_space = has_left_trim_marker(self.rest().as_bytes());
        let after_marker = if trim_space { TRIM_MARKER_LEN } else { 0 };
        if self.input[(self.pos + after_marker).min(self.end)..self.end].starts_with(LEFT_COMMENT) {
            self.pos += after_marker;
            self.ignore();
            return Some(State::Comment);
        }
        let i = self.this_item(ItemType::LeftDelim);
        self.inside_action = true;
        self.pos += after_marker;
        self.ignore();
        self.paren_depth = 0;
        self.emit_item(i)
    }

    fn lex_comment(&mut self) -> Option<State> {
        self.pos += LEFT_COMMENT.len();
        let Some(x) = self.rest().find(RIGHT_COMMENT) else {
            return self.errorf("unclosed comment".to_string());
        };
        self.pos += x + RIGHT_COMMENT.len();
        let (delim, trim_space) = self.at_right_delim();
        if !delim {
            return self.errorf("comment ends before closing delimiter".to_string());
        }
        self.line += self.input[self.start..self.pos].matches('\n').count();
        let i = self.this_item(ItemType::Comment);
        if trim_space {
            self.pos += TRIM_MARKER_LEN;
        }
        self.pos += self.right_delim.len();
        if trim_space {
            let rest = self.rest();
            self.pos += rest.len() - rest.trim_start_matches(SPACE_CHARS).len();
        }
        self.ignore();
        if self.options.emit_comment {
            return self.emit_item(i);
        }
        Some(State::Text)
    }

    fn lex_right_delim(&mut self) -> Option<State> {
        let (_, trim_space) = self.at_right_delim();
        if trim_space {
            self.pos += TRIM_MARKER_LEN;
            self.ignore();
        }
        self.pos += self.right_delim.len();
        let i = self.this_item(ItemType::RightDelim);
        if trim_space {
            let rest = self.rest();
            self.pos += rest.len() - rest.trim_start_matches(SPACE_CHARS).len();
            self.ignore();
        }
        self.inside_action = false;
        self.emit_item(i)
    }

    fn lex_inside_action(&mut self) -> Option<State> {
        let (delim, _) = self.at_right_delim();
        if delim {
            if self.paren_depth == 0 {
                return Some(State::RightDelim);
            }
            return self.errorf("unclosed left paren".to_string());
        }
        let r = self.next();
        match r {
            None => self.errorf("unclosed action".to_string()),
            _ if is_space(r) => {
                self.backup();
                Some(State::Space)
            }
            Some('=') => self.emit(ItemType::Assign),
            Some(':') => {
                if self.next() != Some('=') {
                    return self.errorf("expected :=".to_string());
                }
                self.emit(ItemType::Declare)
            }
            Some('|') => self.emit(ItemType::Pipe),
            Some('"') => Some(State::Quote),
            Some('`') => Some(State::RawQuote),
            Some('$') => Some(State::Variable),
            Some('\'') => Some(State::Char),
            Some('.') => {
                if self.pos < self.end {
                    let c = self.input.as_bytes()[self.pos];
                    if !c.is_ascii_digit() {
                        return Some(State::Field);
                    }
                }
                self.backup();
                Some(State::Number)
            }
            Some(c) if c == '+' || c == '-' || c.is_ascii_digit() => {
                self.backup();
                Some(State::Number)
            }
            _ if is_alpha_numeric(r) => {
                self.backup();
                Some(State::Identifier)
            }
            Some('(') => {
                self.paren_depth += 1;
                self.emit(ItemType::LeftParen)
            }
            Some(')') => {
                self.paren_depth -= 1;
                if self.paren_depth < 0 {
                    return self.errorf("unexpected right paren".to_string());
                }
                self.emit(ItemType::RightParen)
            }
            Some(c) if (c as u32) <= 0x7f && is_print(c) => self.emit(ItemType::Char),
            _ => {
                let msg = format!("unrecognized character in action: {}", sharp_u(r));
                self.errorf(msg)
            }
        }
    }

    fn lex_space(&mut self) -> Option<State> {
        let mut num_spaces = 0;
        loop {
            let r = self.peek();
            if !is_space(r) {
                break;
            }
            self.next();
            num_spaces += 1;
        }
        let b = self.input.as_bytes();
        if has_right_trim_marker(&b[self.pos - 1..self.end])
            && self.input[(self.pos - 1 + TRIM_MARKER_LEN).min(self.end)..self.end].starts_with(self.right_delim.as_str())
        {
            self.backup();
            if num_spaces == 1 {
                return Some(State::RightDelim);
            }
        }
        self.emit(ItemType::Space)
    }

    fn lex_identifier(&mut self) -> Option<State> {
        loop {
            let r = self.next();
            if is_alpha_numeric(r) {
                continue;
            }
            self.backup();
            let word = self.input[self.start..self.pos].to_string();
            if !self.at_terminator() {
                return self.errorf(format!("bad character {}", sharp_u(r)));
            }
            if let Some(item) = keyword(&word) {
                if item > ItemType::Keyword {
                    if (item == ItemType::Break && !self.options.break_ok)
                        || (item == ItemType::Continue && !self.options.continue_ok)
                    {
                        return self.emit(ItemType::Identifier);
                    }
                    return self.emit(item);
                }
            }
            if word.starts_with('.') {
                return self.emit(ItemType::Field);
            }
            if word == "true" || word == "false" {
                return self.emit(ItemType::Bool);
            }
            return self.emit(ItemType::Identifier);
        }
    }

    fn lex_variable(&mut self) -> Option<State> {
        if self.at_terminator() {
            return self.emit(ItemType::Variable);
        }
        self.lex_field_or_variable(ItemType::Variable)
    }

    fn lex_field_or_variable(&mut self, typ: ItemType) -> Option<State> {
        if self.at_terminator() {
            if typ == ItemType::Variable {
                return self.emit(ItemType::Variable);
            }
            return self.emit(ItemType::Dot);
        }
        let mut r;
        loop {
            r = self.next();
            if !is_alpha_numeric(r) {
                self.backup();
                break;
            }
        }
        if !self.at_terminator() {
            return self.errorf(format!("bad character {}", sharp_u(r)));
        }
        self.emit(typ)
    }

    fn at_terminator(&mut self) -> bool {
        let r = self.peek();
        if is_space(r) {
            return true;
        }
        match r {
            None | Some('.' | ',' | '|' | ':' | ')' | '(') => return true,
            _ => {}
        }
        self.rest().starts_with(self.right_delim.as_str())
    }

    fn lex_char(&mut self) -> Option<State> {
        loop {
            match self.next() {
                Some('\\') => {
                    let r = self.next();
                    if r.is_some() && r != Some('\n') {
                        continue;
                    }
                    return self.errorf("unterminated character constant".to_string());
                }
                None | Some('\n') => return self.errorf("unterminated character constant".to_string()),
                Some('\'') => break,
                _ => {}
            }
        }
        self.emit(ItemType::CharConstant)
    }

    fn lex_number(&mut self) -> Option<State> {
        if !self.scan_number() {
            let msg = format!("bad number syntax: {}", quote(&self.input[self.start..self.pos]));
            return self.errorf(msg);
        }
        let sign = self.peek();
        if sign == Some('+') || sign == Some('-') {
            if !self.scan_number() || self.input.as_bytes()[self.pos - 1] != b'i' {
                let msg = format!("bad number syntax: {}", quote(&self.input[self.start..self.pos]));
                return self.errorf(msg);
            }
            return self.emit(ItemType::Complex);
        }
        self.emit(ItemType::Number)
    }

    fn scan_number(&mut self) -> bool {
        self.accept("+-");
        let mut digits = "0123456789_";
        if self.accept("0") {
            if self.accept("xX") {
                digits = "0123456789abcdefABCDEF_";
            } else if self.accept("oO") {
                digits = "01234567_";
            } else if self.accept("bB") {
                digits = "01_";
            }
        }
        self.accept_run(digits);
        if self.accept(".") {
            self.accept_run(digits);
        }
        if digits.len() == 10 + 1 && self.accept("eE") {
            self.accept("+-");
            self.accept_run("0123456789_");
        }
        if digits.len() == 16 + 6 + 1 && self.accept("pP") {
            self.accept("+-");
            self.accept_run("0123456789_");
        }
        self.accept("i");
        if is_alpha_numeric(self.peek()) {
            self.next();
            return false;
        }
        true
    }

    fn lex_quote(&mut self) -> Option<State> {
        loop {
            match self.next() {
                Some('\\') => {
                    let r = self.next();
                    if r.is_some() && r != Some('\n') {
                        continue;
                    }
                    return self.errorf("unterminated quoted string".to_string());
                }
                None | Some('\n') => return self.errorf("unterminated quoted string".to_string()),
                Some('"') => break,
                _ => {}
            }
        }
        self.emit(ItemType::String)
    }

    fn lex_raw_quote(&mut self) -> Option<State> {
        loop {
            match self.next() {
                None => return self.errorf("unterminated raw quoted string".to_string()),
                Some('`') => break,
                _ => {}
            }
        }
        self.emit(ItemType::RawString)
    }
}
