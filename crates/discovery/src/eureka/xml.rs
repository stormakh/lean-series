//! The part of Go's `encoding/xml` that the Eureka client needs: the strict tokenizer (`Decoder.Token`, with
//! Go's syntax error texts, entities, CDATA, comments, processing instructions and directives) and the
//! helpers that `Decoder.Decode` uses to fill structs (scalar values, attributes, `,innerxml`, skipping).
//!
//! Differs from Go: name space prefixes are not translated to their URLs (only the local name is used for
//! matching), and the check of XML name characters is a close approximation of Go's Unicode tables.

/// Go `xml.Name`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Name {
    pub space: String,
    pub local: String,
}

/// Go `xml.Attr`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Attr {
    pub name: Name,
    pub value: String,
}

/// Go `xml.StartElement`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StartElement {
    pub name: Name,
    pub attr: Vec<Attr>,
}

/// Go `xml.Token`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Token {
    Start(StartElement),
    End(Name),
    CharData(String),
    Comment(Vec<u8>),
    ProcInst(String, Vec<u8>),
    Directive(Vec<u8>),
}

/// Go `xml.Decoder` in strict mode over a byte slice.
pub struct Decoder<'a> {
    b: &'a [u8],
    pos: usize,
    line: usize,
    err: Option<String>,
    stack: Vec<Name>,
    need_close: Option<Name>,
}

const EOF: &str = "EOF";

impl<'a> Decoder<'a> {
    pub fn new(b: &'a [u8]) -> Decoder<'a> {
        Decoder { b, pos: 0, line: 1, err: None, stack: Vec::new(), need_close: None }
    }

    /// The offset of the next byte (Go `savedOffset`, for `,innerxml`).
    pub fn offset(&self) -> usize {
        self.pos
    }

    /// The raw input between two offsets.
    pub fn raw(&self, from: usize, to: usize) -> &'a [u8] {
        &self.b[from..to]
    }

    fn syntax_error(&self, msg: &str) -> String {
        format!("XML syntax error on line {}: {msg}", self.line)
    }

    fn fail<T>(&mut self, msg: &str) -> Option<T> {
        self.err = Some(self.syntax_error(msg));
        None
    }

    fn getc(&mut self) -> Option<u8> {
        if self.err.is_some() || self.pos >= self.b.len() {
            return None;
        }
        let c = self.b[self.pos];
        self.pos += 1;
        if c == b'\n' {
            self.line += 1;
        }
        Some(c)
    }

    /// Go `mustgetc`: EOF becomes "unexpected EOF".
    fn mustgetc(&mut self) -> Option<u8> {
        let c = self.getc();
        if c.is_none() && self.err.is_none() {
            self.err = Some(self.syntax_error("unexpected EOF"));
        }
        c
    }

    fn ungetc(&mut self, c: u8) {
        if c == b'\n' {
            self.line -= 1;
        }
        self.pos -= 1;
    }

    fn take_err(&mut self) -> String {
        if let Some(e) = &self.err {
            return e.clone();
        }
        EOF.to_string()
    }

    /// Go `Decoder.Token`: the next token, with end elements checked against the open elements.
    pub fn token(&mut self) -> Result<Token, String> {
        let t = match self.raw_token() {
            Some(t) => t,
            None => {
                if self.err.is_none() && !self.stack.is_empty() {
                    self.err = Some(self.syntax_error("unexpected EOF"));
                }
                return Err(self.take_err());
            }
        };
        match &t {
            Token::Start(s) => self.stack.push(s.name.clone()),
            Token::End(name) => {
                let Some(s) = self.stack.pop() else {
                    let e = self.syntax_error(&format!("unexpected end element </{}>", name.local));
                    self.err = Some(e.clone());
                    return Err(e);
                };
                if s.local != name.local {
                    let e = self.syntax_error(&format!("element <{}> closed by </{}>", s.local, name.local));
                    self.err = Some(e.clone());
                    return Err(e);
                }
                if s.space != name.space {
                    let ns = if s.space.is_empty() { "\"\"" } else { s.space.as_str() };
                    let e = self.syntax_error(&format!("element <{}> in space {} closed by </{}> in space {}", s.local, ns, name.local, name.space));
                    self.err = Some(e.clone());
                    return Err(e);
                }
            }
            _ => {}
        }
        Ok(t)
    }

    /// Go `Decoder.Skip`: reads tokens until the end of the element whose start was just read.
    pub fn skip(&mut self) -> Result<(), String> {
        let mut depth = 0usize;
        loop {
            match self.token()? {
                Token::Start(_) => depth += 1,
                Token::End(_) => {
                    if depth == 0 {
                        return Ok(());
                    }
                    depth -= 1;
                }
                _ => {}
            }
        }
    }

    /// The first start element (Go `Decode` with no start given). Returns "EOF" when there is none.
    pub fn first_start(&mut self) -> Result<StartElement, String> {
        loop {
            if let Token::Start(s) = self.token()? {
                return Ok(s);
            }
        }
    }

    /// The character data of an element whose start was just read (Go `unmarshal` into a scalar): text
    /// directly inside the element; child elements are skipped.
    pub fn chardata(&mut self) -> Result<String, String> {
        let mut data = String::new();
        loop {
            match self.token()? {
                Token::Start(_) => self.skip()?,
                Token::End(_) => return Ok(data),
                Token::CharData(t) => data.push_str(&t),
                _ => {}
            }
        }
    }

    /// The raw inner XML of an element whose start was just read (Go `,innerxml` on a struct with no other
    /// fields).
    pub fn inner_xml(&mut self) -> Result<String, String> {
        let start = self.offset();
        loop {
            let before = self.offset();
            match self.token()? {
                Token::Start(_) => self.skip()?,
                Token::End(_) => return Ok(String::from_utf8_lossy(self.raw(start, before)).into_owned()),
                _ => {}
            }
        }
    }

    fn raw_token(&mut self) -> Option<Token> {
        if self.err.is_some() {
            return None;
        }
        if let Some(n) = self.need_close.take() {
            return Some(Token::End(n));
        }
        let c = self.getc()?;
        if c != b'<' {
            self.ungetc(c);
            let data = self.text(None, false)?;
            return Some(Token::CharData(data));
        }
        let c = self.mustgetc()?;
        match c {
            b'/' => {
                let Some(name) = self.nsname() else {
                    return if self.err.is_some() { None } else { self.fail("expected element name after </") };
                };
                self.space();
                let c = self.mustgetc()?;
                if c != b'>' {
                    return self.fail(&format!("invalid characters between </{} and >", name.local));
                }
                Some(Token::End(name))
            }
            b'?' => {
                let Some(target) = self.name() else {
                    return if self.err.is_some() { None } else { self.fail("expected target name after <?") };
                };
                self.space();
                let mut data = Vec::new();
                let mut b0 = 0u8;
                loop {
                    let c = self.mustgetc()?;
                    data.push(c);
                    if b0 == b'?' && c == b'>' {
                        break;
                    }
                    b0 = c;
                }
                data.truncate(data.len() - 2);
                if target == "xml" {
                    let content = String::from_utf8_lossy(&data).into_owned();
                    let ver = proc_inst("version", &content);
                    if !ver.is_empty() && ver != "1.0" {
                        self.err = Some(format!("xml: unsupported version {}; only version 1.0 is supported", yaml::go_quote(ver)));
                        return None;
                    }
                    let enc = proc_inst("encoding", &content);
                    if !enc.is_empty() && !enc.eq_ignore_ascii_case("utf-8") {
                        self.err = Some(format!("xml: encoding {} declared but Decoder.CharsetReader is nil", yaml::go_quote(enc)));
                        return None;
                    }
                }
                Some(Token::ProcInst(target, data))
            }
            b'!' => {
                let c = self.mustgetc()?;
                match c {
                    b'-' => {
                        let c = self.mustgetc()?;
                        if c != b'-' {
                            return self.fail("invalid sequence <!- not part of <!--");
                        }
                        let mut data = Vec::new();
                        let (mut b0, mut b1) = (0u8, 0u8);
                        loop {
                            let c = self.mustgetc()?;
                            data.push(c);
                            if b0 == b'-' && b1 == b'-' {
                                if c != b'>' {
                                    return self.fail("invalid sequence \"--\" not allowed in comments");
                                }
                                break;
                            }
                            b0 = b1;
                            b1 = c;
                        }
                        data.truncate(data.len() - 3);
                        Some(Token::Comment(data))
                    }
                    b'[' => {
                        for &want in b"CDATA[" {
                            let c = self.mustgetc()?;
                            if c != want {
                                return self.fail("invalid <![ sequence");
                            }
                        }
                        let data = self.text(None, true)?;
                        Some(Token::CharData(data))
                    }
                    _ => self.directive(c),
                }
            }
            _ => {
                self.ungetc(c);
                let Some(name) = self.nsname() else {
                    return if self.err.is_some() { None } else { self.fail("expected element name after <") };
                };
                let mut attr = Vec::new();
                let mut empty = false;
                loop {
                    self.space();
                    let c = self.mustgetc()?;
                    if c == b'/' {
                        empty = true;
                        let c = self.mustgetc()?;
                        if c != b'>' {
                            return self.fail("expected /> in element");
                        }
                        break;
                    }
                    if c == b'>' {
                        break;
                    }
                    self.ungetc(c);
                    let Some(an) = self.nsname() else {
                        return if self.err.is_some() { None } else { self.fail("expected attribute name in element") };
                    };
                    self.space();
                    let c = self.mustgetc()?;
                    if c != b'=' {
                        return self.fail("attribute name without = in element");
                    }
                    self.space();
                    let q = self.mustgetc()?;
                    if q != b'"' && q != b'\'' {
                        return self.fail("unquoted or missing attribute value in element");
                    }
                    let value = self.text(Some(q), false)?;
                    attr.push(Attr { name: an, value });
                }
                if empty {
                    self.need_close = Some(name.clone());
                }
                Some(Token::Start(StartElement { name, attr }))
            }
        }
    }

    /// A directive (`<!DOCTYPE ...>`); quoted angle brackets do not count for nesting, comments become a space.
    fn directive(&mut self, first: u8) -> Option<Token> {
        let mut buf = vec![first];
        let mut inquote = 0u8;
        let mut depth = 0i32;
        // Go's `goto HandleB`: a byte to handle without the end check.
        let mut handle: Option<u8> = None;
        loop {
            let c = match handle.take() {
                Some(c) => c,
                None => {
                    let c = self.mustgetc()?;
                    if inquote == 0 && c == b'>' && depth == 0 {
                        break;
                    }
                    c
                }
            };
            buf.push(c);
            if c == inquote {
                inquote = 0;
            } else if inquote != 0 {
                // in quotes, no special action
            } else if c == b'\'' || c == b'"' {
                inquote = c;
            } else if c == b'>' {
                depth -= 1;
            } else if c == b'<' {
                // Look for <!-- to begin a comment.
                let s = b"!--";
                for i in 0..s.len() {
                    let b = self.mustgetc()?;
                    if b != s[i] {
                        buf.extend_from_slice(&s[..i]);
                        depth += 1;
                        handle = Some(b);
                        break;
                    }
                }
                if handle.is_some() {
                    continue;
                }
                buf.pop();
                let (mut b0, mut b1) = (0u8, 0u8);
                loop {
                    let b = self.mustgetc()?;
                    if b0 == b'-' && b1 == b'-' && b == b'>' {
                        break;
                    }
                    b0 = b1;
                    b1 = b;
                }
                buf.push(b' ');
            }
        }
        Some(Token::Directive(buf))
    }

    fn space(&mut self) {
        while let Some(c) = self.getc() {
            if !matches!(c, b' ' | b'\r' | b'\n' | b'\t') {
                self.ungetc(c);
                return;
            }
        }
    }

    /// Go `text`: character data up to '<' (or up to the quote, or to "]]>" in CDATA), with entities.
    fn text(&mut self, quote: Option<u8>, cdata: bool) -> Option<String> {
        let mut buf: Vec<u8> = Vec::new();
        let (mut b0, mut b1) = (0u8, 0u8);
        let mut trunc = 0;
        loop {
            let Some(c) = self.getc() else {
                if cdata {
                    if self.err.is_none() {
                        self.err = Some(self.syntax_error("unexpected EOF in CDATA section"));
                    }
                    return None;
                }
                if self.err.is_some() {
                    return None;
                }
                // EOF ends the text; the next read reports it.
                break;
            };
            if quote.is_none() && b0 == b']' && b1 == b']' && c == b'>' {
                if cdata {
                    trunc = 2;
                    break;
                }
                return self.fail("unescaped ]]> not in CDATA section");
            }
            if c == b'<' && !cdata {
                if quote.is_some() {
                    return self.fail("unescaped < inside quoted string");
                }
                self.ungetc(c);
                break;
            }
            if quote == Some(c) {
                break;
            }
            if c == b'&' && !cdata {
                let before = buf.len();
                buf.push(b'&');
                let mut c = self.mustgetc()?;
                let mut text: Option<String> = None;
                if c == b'#' {
                    buf.push(c);
                    c = self.mustgetc()?;
                    let mut base = 10;
                    if c == b'x' {
                        base = 16;
                        buf.push(c);
                        c = self.mustgetc()?;
                    }
                    let start = buf.len();
                    while c.is_ascii_digit() || base == 16 && c.is_ascii_hexdigit() {
                        buf.push(c);
                        c = self.mustgetc()?;
                    }
                    if c != b';' {
                        self.ungetc(c);
                    } else {
                        let s = String::from_utf8_lossy(&buf[start..]).into_owned();
                        buf.push(b';');
                        if let Ok(n) = u64::from_str_radix(&s, base)
                            && n <= 0x10FFFF
                        {
                            // Go string(rune(n)): surrogates become U+FFFD
                            text = Some(char::from_u32(n as u32).unwrap_or('\u{FFFD}').to_string());
                        }
                    }
                } else {
                    self.ungetc(c);
                    if !self.read_name(&mut buf) && self.err.is_some() {
                        return None;
                    }
                    let c = self.mustgetc()?;
                    if c != b';' {
                        self.ungetc(c);
                    } else {
                        let name = String::from_utf8_lossy(&buf[before + 1..]).into_owned();
                        buf.push(b';');
                        if is_name(name.as_bytes()) {
                            text = match name.as_str() {
                                "lt" => Some("<".into()),
                                "gt" => Some(">".into()),
                                "amp" => Some("&".into()),
                                "apos" => Some("'".into()),
                                "quot" => Some("\"".into()),
                                _ => None,
                            };
                        }
                    }
                }
                if let Some(t) = text {
                    buf.truncate(before);
                    buf.extend_from_slice(t.as_bytes());
                    b0 = 0;
                    b1 = 0;
                    continue;
                }
                let mut ent = String::from_utf8_lossy(&buf[before..]).into_owned();
                if !ent.ends_with(';') {
                    ent.push_str(" (no semicolon)");
                }
                return self.fail(&format!("invalid character entity {ent}"));
            }
            if c == b'\r' {
                buf.push(b'\n');
            } else if b1 == b'\r' && c == b'\n' {
                // \r\n: the \n was written already
            } else {
                buf.push(c);
            }
            b0 = b1;
            b1 = c;
        }
        buf.truncate(buf.len() - trunc);
        let s = match String::from_utf8(buf) {
            Ok(s) => s,
            Err(_) => return self.fail("invalid UTF-8"),
        };
        if let Some(r) = s.chars().find(|&r| !is_in_character_range(r)) {
            return self.fail(&format!("illegal character code U+{:04X}", r as u32));
        }
        Some(s)
    }

    /// Go `nsname`: "space:local".
    fn nsname(&mut self) -> Option<Name> {
        let s = self.name()?;
        if s.matches(':').count() > 1 {
            return None;
        }
        match s.split_once(':') {
            Some((space, local)) if !space.is_empty() && !local.is_empty() => Some(Name { space: space.to_string(), local: local.to_string() }),
            _ => Some(Name { space: String::new(), local: s }),
        }
    }

    /// Go `name`.
    fn name(&mut self) -> Option<String> {
        let mut buf = Vec::new();
        if !self.read_name(&mut buf) {
            return None;
        }
        if !is_name(&buf) {
            let msg = format!("invalid XML name: {}", String::from_utf8_lossy(&buf));
            return self.fail(&msg);
        }
        String::from_utf8(buf).ok()
    }

    /// Go `readName`: name bytes up to the first single-byte character that cannot be in a name.
    fn read_name(&mut self, buf: &mut Vec<u8>) -> bool {
        let Some(c) = self.mustgetc() else { return false };
        if c < 0x80 && !is_name_byte(c) {
            self.ungetc(c);
            return false;
        }
        buf.push(c);
        loop {
            let Some(c) = self.mustgetc() else { return false };
            if c < 0x80 && !is_name_byte(c) {
                self.ungetc(c);
                return true;
            }
            buf.push(c);
        }
    }
}

fn is_name_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, b'_' | b':' | b'.' | b'-')
}

/// Go `isName` (the `first` and `second` Unicode tables, approximated with Rust's character classes).
fn is_name(s: &[u8]) -> bool {
    let Ok(s) = std::str::from_utf8(s) else { return false };
    let mut it = s.chars();
    let Some(c) = it.next() else { return false };
    if !(c.is_alphabetic() || c == '_' || c == ':') {
        return false;
    }
    it.all(|c| c.is_alphanumeric() || matches!(c, '_' | ':' | '.' | '-' | '\u{B7}') || ('\u{300}'..='\u{36F}').contains(&c) || ('\u{203F}'..='\u{2040}').contains(&c))
}

/// Go `isInCharacterRange`.
fn is_in_character_range(r: char) -> bool {
    let r = r as u32;
    r == 0x09 || r == 0x0A || r == 0x0D || (0x20..=0xD7FF).contains(&r) || (0xE000..=0xFFFD).contains(&r) || (0x10000..=0x10FFFF).contains(&r)
}

/// Go `procInst`: the value of `param` in the content of a processing instruction.
fn proc_inst<'s>(param: &str, s: &'s str) -> &'s str {
    let param = format!("{param}=");
    let lenp = param.len();
    let mut i = 0;
    let mut sep = 0u8;
    while i < s.len() {
        let sub = &s[i..];
        let Some(k) = sub.find(&param) else { return "" };
        if lenp + k >= sub.len() {
            return "";
        }
        i += lenp + k + 1;
        let c = sub.as_bytes()[lenp + k];
        if c == b'\'' || c == b'"' {
            sep = c;
            break;
        }
    }
    if sep == 0 {
        return "";
    }
    match s[i..].find(sep as char) {
        Some(j) => &s[i..i + j],
        None => "",
    }
}

/// Go `copyValue` into an `int`: "" is 0; otherwise `strconv.ParseInt` of the trimmed text.
pub fn parse_int(s: &str) -> Result<i64, String> {
    if s.is_empty() {
        return Ok(0);
    }
    let t = s.trim();
    t.parse::<i64>().map_err(|e| {
        let why = match e.kind() {
            std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => "value out of range",
            _ => "invalid syntax",
        };
        format!("strconv.ParseInt: parsing {}: {why}", yaml::go_quote(t))
    })
}

/// Go `copyValue` into a `bool`: "" is false; otherwise `strconv.ParseBool` of the trimmed text.
pub fn parse_bool(s: &str) -> Result<bool, String> {
    if s.is_empty() {
        return Ok(false);
    }
    match s.trim() {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        t => Err(format!("strconv.ParseBool: parsing {}: invalid syntax", yaml::go_quote(t))),
    }
}
