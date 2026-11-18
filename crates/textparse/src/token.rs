//! Lexer tokens shared by the text parsers (Go `token`), with Go's `String()` names.

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Token {
    Invalid,
    Eof,
    Linebreak,
    Whitespace,
    Help,
    Type,
    Unit,
    EofWord,
    Text,
    Comment,
    #[allow(dead_code)]
    Blank,
    MName,
    QString,
    BraceOpen,
    BraceClose,
    LName,
    LValue,
    Comma,
    Equal,
    Timestamp,
    Value,
    StartTimestamp,
    CompOpen,
    CompClose,
    CompCount,
    CompGCount,
    CompSum,
    CompGSum,
    CompSchema,
    CompZeroThreshold,
    CompZeroCount,
    CompNegSpans,
    CompNegBuckets,
    CompPosSpans,
    CompPosBuckets,
    CompBucket,
    CompQuantile,
}

impl Token {
    /// Go `token.String()`.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Token::Invalid => "INVALID",
            Token::Eof => "EOF",
            Token::Linebreak => "LINEBREAK",
            Token::Whitespace => "WHITESPACE",
            Token::Help => "HELP",
            Token::Type => "TYPE",
            Token::Unit => "UNIT",
            Token::EofWord => "EOFWORD",
            Token::Text => "TEXT",
            Token::Comment => "COMMENT",
            Token::Blank => "BLANK",
            Token::MName => "MNAME",
            Token::QString => "QSTRING",
            Token::BraceOpen => "BOPEN",
            Token::BraceClose => "BCLOSE",
            Token::LName => "LNAME",
            Token::LValue => "LVALUE",
            Token::Equal => "EQUAL",
            Token::Comma => "COMMA",
            Token::Timestamp => "TIMESTAMP",
            Token::Value => "VALUE",
            Token::StartTimestamp => "STARTTIMESTAMP",
            Token::CompOpen => "COMPOPEN",
            Token::CompClose => "COMPCLOSE",
            Token::CompCount => "COMPCOUNT",
            Token::CompGCount => "COMPGCOUNT",
            Token::CompSum => "COMPSUM",
            Token::CompGSum => "COMPGSUM",
            Token::CompSchema => "COMPSCHEMA",
            Token::CompZeroThreshold => "COMPZEROTHRESHOLD",
            Token::CompZeroCount => "COMPZEROCOUNT",
            Token::CompNegSpans => "COMPNEGSPANS",
            Token::CompNegBuckets => "COMPNEGBUCKETS",
            Token::CompPosSpans => "COMPPOSSPANS",
            Token::CompPosBuckets => "COMPPOSBUCKETS",
            Token::CompBucket => "COMPBUCKET",
            Token::CompQuantile => "COMPQUANTILE",
        }
    }
}

// Lexer start conditions (Go `sInit`...). The OpenMetrics 2.0 lexer adds `S_COMPOSITE` and `S_COMP_VALUE`.
pub(crate) const S_INIT: u8 = 0;
pub(crate) const S_COMMENT: u8 = 1;
pub(crate) const S_META1: u8 = 2;
pub(crate) const S_META2: u8 = 3;
pub(crate) const S_LABELS: u8 = 4;
pub(crate) const S_LVALUE: u8 = 5;
pub(crate) const S_VALUE: u8 = 6;
pub(crate) const S_TIMESTAMP: u8 = 7;
pub(crate) const S_EXEMPLAR: u8 = 8;
pub(crate) const S_EVALUE: u8 = 9;
pub(crate) const S_ETIMESTAMP: u8 = 10;
#[allow(dead_code)]
pub(crate) const S_COMPOSITE: u8 = 11;
#[allow(dead_code)]
pub(crate) const S_COMP_VALUE: u8 = 12;

/// Go `parseError`: `"%s, got %q (%q) while parsing: %q"`.
pub(crate) fn parse_error(b: &[u8], tok_start: usize, i: usize, line_start: usize, exp: &str, got: Token) -> crate::Error {
    let e = b.len().min(i + 1);
    let mut s = String::with_capacity(exp.len() + 64);
    s.push_str(exp);
    s.push_str(", got ");
    crate::strconv::append_quote_bytes(&mut s, &b[tok_start.min(e)..e]);
    s.push_str(" (\"");
    s.push_str(got.as_str());
    s.push_str("\") while parsing: ");
    crate::strconv::append_quote_bytes(&mut s, &b[line_start.min(e)..e]);
    crate::Error::Msg(s)
}
