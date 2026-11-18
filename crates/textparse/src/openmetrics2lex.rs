//! Port of `openmetrics2lex.l.go`: the golex DFA of the OpenMetrics 2.0 lexer, written by hand state by state.
//! Like golex it never backtracks: a state without a transition for the current byte either accepts its rule
//! (when the state accepts) or aborts with an invalid token. The comments name the golex states (`yystateN`).

use crate::token::*;

/// `[a-zA-Z_:]`.
#[inline(always)]
fn is_m(c: u8) -> bool {
    c == b':' || c == b'_' || c.is_ascii_alphabetic()
}

/// `[0-9:A-Za-z_]` (golex writes it as `'0'..=':'`).
#[inline(always)]
fn is_md(c: u8) -> bool {
    c == b':' || c == b'_' || c.is_ascii_alphanumeric()
}

/// `[a-zA-Z_]`.
#[inline(always)]
fn is_l(c: u8) -> bool {
    c == b'_' || c.is_ascii_alphabetic()
}

/// `[0-9A-Za-z_]`.
#[inline(always)]
fn is_ld(c: u8) -> bool {
    c == b'_' || c.is_ascii_alphanumeric()
}

/// `[^ \n]` as golex compiles it (`\x01-\t`, `\v-\x1f`, `!-\xff`: also excluding \0, which is the EOF byte).
#[inline(always)]
fn is_vc(c: u8) -> bool {
    !matches!(c, 0 | b'\n' | b' ')
}

/// `[^\n]` inside quotes after a backslash, and `{C}` (`\x01-\t`, `\v-\xff`).
#[inline(always)]
fn is_c(c: u8) -> bool {
    c != 0 && c != b'\n'
}

/// `[^,}\[\] \n\r]` (yystate196/197), and without `,` `}` `[` for the bracketed form (yystate198).
#[inline(always)]
fn is_comp_value(c: u8) -> bool {
    !matches!(c, 0 | b'\n' | b'\r' | b' ' | b',' | b'[' | b']' | b'}')
}

#[inline(always)]
fn is_comp_bracket(c: u8) -> bool {
    !matches!(c, 0 | b'\n' | b'\r' | b' ' | b']')
}

/// The composite field keywords of `sComposite` (yystate83..194). None is a prefix of another.
const KEYWORDS: [(&[u8], Token); 13] = [
    (b"count:", Token::CompCount),
    (b"gcount:", Token::CompGCount),
    (b"sum:", Token::CompSum),
    (b"gsum:", Token::CompGSum),
    (b"schema:", Token::CompSchema),
    (b"zero_threshold:", Token::CompZeroThreshold),
    (b"zero_count:", Token::CompZeroCount),
    (b"positive_spans:", Token::CompPosSpans),
    (b"positive_buckets:", Token::CompPosBuckets),
    (b"negative_spans:", Token::CompNegSpans),
    (b"negative_buckets:", Token::CompNegBuckets),
    (b"bucket:", Token::CompBucket),
    (b"quantile:", Token::CompQuantile),
];

/// The OpenMetrics 2.0 lexer (Go `openMetrics2Lexer`).
#[derive(Clone, Copy, Default)]
pub(crate) struct Om2Lexer {
    pub(crate) i: usize,
    pub(crate) start: usize,
    pub(crate) state: u8,
}

impl Om2Lexer {
    /// Go `next`: advances and returns the byte, 0 (`tEOF`) at the end. Null bytes are consumed in label values,
    /// exemplar label values, metadata text and comments.
    #[inline(always)]
    fn next(&mut self, b: &[u8]) -> u8 {
        self.i += 1;
        if self.i >= b.len() {
            self.i = b.len();
            return 0;
        }
        let mut c = b[self.i];
        while c == 0 && matches!(self.state, S_LVALUE | S_EVALUE | S_META2 | S_COMMENT) {
            self.i += 1;
            if self.i >= b.len() {
                return 0;
            }
            c = b[self.i];
        }
        c
    }

    /// Matches the bytes `rest` one by one (each mismatch aborts).
    #[inline]
    fn word(&mut self, b: &[u8], rest: &[u8]) -> bool {
        rest.iter().all(|&w| self.next(b) == w)
    }

    /// A quoted string after the opening quote: `(\\.|[^\\"])*"` (`newline`: unescaped `\n` allowed) or
    /// `(\\.|[^\\"\n])*"`. On success the closing quote is consumed and the byte after it is read.
    #[inline]
    fn quoted(&mut self, b: &[u8], newline: bool) -> bool {
        loop {
            match self.next(b) {
                b'"' => {
                    self.next(b);
                    return true;
                }
                b'\\' => {
                    if !is_c(self.next(b)) {
                        return false;
                    }
                }
                0 => return false,
                b'\n' if !newline => return false,
                _ => {}
            }
        }
    }

    /// `{S}[^ \n]+` after the space (the space is the current byte): false when the first byte does not match.
    #[inline]
    fn space_value(&mut self, b: &[u8]) -> bool {
        if !is_vc(self.next(b)) {
            return false;
        }
        while is_vc(self.next(b)) {}
        true
    }

    /// The keyword trie of `sComposite`, starting at byte `c0`.
    fn keyword(&mut self, b: &[u8], c0: u8) -> Token {
        let mut mask: u16 = 0;
        for (j, (kw, _)) in KEYWORDS.iter().enumerate() {
            if kw[0] == c0 {
                mask |= 1 << j;
            }
        }
        let mut k = 1;
        while mask != 0 {
            let c = self.next(b);
            let mut m2: u16 = 0;
            for (j, (kw, _)) in KEYWORDS.iter().enumerate() {
                if mask & (1 << j) != 0 && kw[k] == c {
                    m2 |= 1 << j;
                }
            }
            mask = m2;
            k += 1;
            if m2.count_ones() == 1 {
                let j = m2.trailing_zeros() as usize;
                if KEYWORDS[j].0.len() == k {
                    self.next(b);
                    self.state = S_COMP_VALUE;
                    return KEYWORDS[j].1;
                }
            }
        }
        Token::Invalid
    }

    /// `{S}[^ \n]+` and `{S}#{S}\{` of sTimestamp (yystate53..61) and sETimestamp (yystate78..82), after the
    /// space. `st` enables the `{S}st@[^ \n]+` rule of sTimestamp.
    fn timestamp(&mut self, b: &[u8], st: bool) -> Token {
        let c = self.next(b);
        if c == b'#' {
            // yystate55 / yystate80.
            let c = self.next(b);
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
        if st && c == b's' {
            // yystate58: "s", yystate59: "st", yystate60: "st@", yystate61: "st@x...".
            let mut c = self.next(b);
            if c == b't' {
                c = self.next(b);
                if c == b'@' {
                    c = self.next(b);
                    if is_vc(c) {
                        while is_vc(self.next(b)) {}
                        self.state = S_TIMESTAMP;
                        return Token::StartTimestamp;
                    }
                    return Token::Timestamp;
                }
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

    /// Go `Lex`.
    pub(crate) fn lex(&mut self, b: &[u8]) -> Token {
        if self.i >= b.len() {
            return Token::Eof;
        }
        let mut c = b[self.i];
        self.start = self.i;
        loop {
            match self.state {
                // yystart1.
                S_INIT => match c {
                    b'#' => {
                        if self.next(b) != b' ' {
                            return Token::Invalid;
                        }
                        // yystate3, rule 1: `#{S}` switches to sComment and goes on with the same token.
                        c = self.next(b);
                        self.state = S_COMMENT;
                        continue;
                    }
                    c if is_m(c) => {
                        while is_md(self.next(b)) {}
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
                // yystart6.
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
                // yystart26.
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
                            while is_md(self.next(b)) {}
                            self.state = S_META2;
                            Token::MName
                        }
                        _ => Token::Invalid,
                    };
                }
                // yystart31.
                S_META2 => {
                    if c != b' ' {
                        return Token::Invalid;
                    }
                    loop {
                        match self.next(b) {
                            b'\n' => {
                                self.next(b);
                                self.state = S_INIT;
                                return Token::Text;
                            }
                            0 => return Token::Invalid,
                            _ => {}
                        }
                    }
                }
                // yystart34 (sLabels) and yystart62 (sExemplar).
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
                            while is_ld(self.next(b)) {}
                            Token::LName
                        }
                        _ => Token::Invalid,
                    };
                }
                // yystart42.
                S_LVALUE => {
                    if c != b'"' || !self.quoted(b, false) {
                        return Token::Invalid;
                    }
                    self.state = S_LABELS;
                    return Token::LValue;
                }
                // yystart46.
                S_VALUE => {
                    return match c {
                        b' ' => {
                            // yystate47.
                            let c = self.next(b);
                            if c == b'{' {
                                self.next(b);
                                self.state = S_COMPOSITE;
                                return Token::CompOpen;
                            }
                            if !is_vc(c) {
                                return Token::Invalid;
                            }
                            while is_vc(self.next(b)) {}
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
                // yystart51.
                S_TIMESTAMP => {
                    return match c {
                        b' ' => self.timestamp(b, true),
                        b'\n' => {
                            self.next(b);
                            self.state = S_INIT;
                            Token::Linebreak
                        }
                        _ => Token::Invalid,
                    };
                }
                // yystart70.
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
                // yystart76.
                S_ETIMESTAMP => {
                    return match c {
                        b' ' => self.timestamp(b, false),
                        b'\n' => {
                            self.next(b);
                            self.state = S_INIT;
                            Token::Linebreak
                        }
                        _ => Token::Invalid,
                    };
                }
                // yystart83.
                S_COMPOSITE => {
                    return match c {
                        b',' => {
                            self.next(b);
                            Token::Comma
                        }
                        b'}' => {
                            self.next(b);
                            self.state = S_TIMESTAMP;
                            Token::CompClose
                        }
                        b'b' | b'c' | b'g' | b'n' | b'p' | b'q' | b's' | b'z' => self.keyword(b, c),
                        _ => Token::Invalid,
                    };
                }
                // yystart196.
                S_COMP_VALUE => {
                    if c == b'[' {
                        // yystate198, yystate199.
                        loop {
                            let c = self.next(b);
                            if c == b']' {
                                self.next(b);
                                self.state = S_COMPOSITE;
                                return Token::Value;
                            }
                            if !is_comp_bracket(c) {
                                return Token::Invalid;
                            }
                        }
                    }
                    if !is_comp_value(c) {
                        return Token::Invalid;
                    }
                    // yystate197.
                    while is_comp_value(self.next(b)) {}
                    self.state = S_COMPOSITE;
                    return Token::Value;
                }
                _ => panic!("invalid start condition {}", self.state),
            }
        }
    }
}
