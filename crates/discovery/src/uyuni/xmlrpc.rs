//! The subset of `github.com/kolo/xmlrpc` (with Go's `net/rpc` client and the token stream of Go's
//! `encoding/xml`) that the Uyuni discoverer uses: method calls with string, int and int-slice arguments, and
//! responses decoded into structs, slices, ints, strings and booleans, with the same error texts.
//!
//! Differs from Go: the per-client cookie jar keeps `name=value` pairs and sends them all back (no domain, path
//! or expiry rules). A `dateTime.iso8601` value always fails with the type mismatch error (no field has the time
//! type); Go first parses the time and reports a time parse error when it is malformed.
use std::sync::Arc;

// ---------------- values and schemas ----------------

/// The Go type a response is decoded into.
#[derive(Debug)]
pub enum Schema {
    Int,
    Str,
    Bool,
    /// Struct fields by `xmlrpc` tag name, in order.
    Struct(&'static [(&'static str, Schema)]),
    Slice(&'static Schema),
}

impl Schema {
    /// Go `reflect.Kind.String()`.
    fn kind(&self) -> &'static str {
        match self {
            Schema::Int => "int",
            Schema::Str => "string",
            Schema::Bool => "bool",
            Schema::Struct(_) => "struct",
            Schema::Slice(_) => "slice",
        }
    }
    /// The Go zero value.
    pub fn zero(&self) -> Value {
        match self {
            Schema::Int => Value::Int(0),
            Schema::Str => Value::Str(String::new()),
            Schema::Bool => Value::Bool(false),
            Schema::Struct(f) => Value::Struct(f.iter().map(|(_, s)| s.zero()).collect()),
            Schema::Slice(_) => Value::Slice(Vec::new()),
        }
    }
}

/// A decoded value of a `Schema` (struct fields in schema order).
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Int(i64),
    Str(String),
    Bool(bool),
    Struct(Vec<Value>),
    Slice(Vec<Value>),
}

impl Value {
    pub fn int(&self) -> i64 {
        match self {
            Value::Int(i) => *i,
            _ => 0,
        }
    }
    pub fn str(&self) -> &str {
        match self {
            Value::Str(s) => s,
            _ => "",
        }
    }
    pub fn bool(&self) -> bool {
        matches!(self, Value::Bool(true))
    }
    pub fn field(&self, i: usize) -> &Value {
        match self {
            Value::Struct(f) => &f[i],
            _ => panic!("xmlrpc: not a struct"),
        }
    }
    pub fn items(&self) -> &[Value] {
        match self {
            Value::Slice(v) => v,
            _ => &[],
        }
    }
}

/// A method call argument.
pub enum Arg<'a> {
    Str(&'a str),
    Int(i64),
    Ints(&'a [i64]),
}

// ---------------- encoder.go / request.go ----------------

/// Go `xml.Escape`.
fn xml_escape(s: &str, out: &mut String) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\t' => out.push_str("&#x9;"),
            '\n' => out.push_str("&#xA;"),
            '\r' => out.push_str("&#xD;"),
            c if (c as u32) < 0x20 || c == '\u{FFFE}' || c == '\u{FFFF}' => out.push('\u{FFFD}'),
            c => out.push(c),
        }
    }
}

fn encode_value(a: &Arg<'_>, out: &mut String) {
    out.push_str("<value>");
    match a {
        Arg::Str(s) => {
            out.push_str("<string>");
            xml_escape(s, out);
            out.push_str("</string>");
        }
        Arg::Int(i) => out.push_str(&format!("<int>{i}</int>")),
        Arg::Ints(v) => {
            out.push_str("<array><data>");
            for i in v.iter() {
                out.push_str(&format!("<value><int>{i}</int></value>"));
            }
            out.push_str("</data></array>");
        }
    }
    out.push_str("</value>");
}

/// Go `EncodeMethodCall`.
pub fn encode_method_call(method: &str, args: &[Arg<'_>]) -> String {
    let mut b = String::from(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    b.push_str(&format!("<methodCall><methodName>{method}</methodName>"));
    b.push_str("<params>");
    for a in args {
        b.push_str("<param>");
        encode_value(a, &mut b);
        b.push_str("</param>");
    }
    b.push_str("</params>");
    b.push_str("</methodCall>");
    b
}

// ---------------- encoding/xml tokens ----------------

#[derive(Debug, PartialEq)]
enum Tok {
    Start(String),
    End(String),
    Text(String),
    /// Comment, processing instruction or directive.
    Other,
}

const EOF: &str = "EOF";

struct XmlDecoder<'a> {
    b: &'a [u8],
    p: usize,
    line: usize,
    stack: Vec<String>,
    pending_end: bool,
}

fn is_name_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b':' || c == b'-' || c == b'.' || c >= 0x80
}

/// Go's `Name.Local` of a prefixed name.
fn local(name: &str) -> String {
    match name.find(':') {
        Some(i) if i >= 1 && i + 1 < name.len() => name[i + 1..].to_string(),
        _ => name.to_string(),
    }
}

impl<'a> XmlDecoder<'a> {
    fn new(b: &'a [u8]) -> XmlDecoder<'a> {
        XmlDecoder { b, p: 0, line: 1, stack: Vec::new(), pending_end: false }
    }

    fn syntax(&self, msg: &str) -> String {
        format!("XML syntax error on line {}: {msg}", self.line)
    }

    fn get(&mut self) -> Option<u8> {
        let c = *self.b.get(self.p)?;
        self.p += 1;
        if c == b'\n' {
            self.line += 1;
        }
        Some(c)
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.p).copied()
    }

    fn eof_err(&self) -> String {
        self.syntax("unexpected EOF")
    }

    fn space(&mut self) {
        while let Some(c) = self.peek() {
            if c == b' ' || c == b'\t' || c == b'\r' || c == b'\n' {
                self.get();
            } else {
                break;
            }
        }
    }

    fn name(&mut self) -> Option<String> {
        let s = self.p;
        while self.peek().is_some_and(is_name_byte) {
            self.get();
        }
        if self.p == s {
            return None;
        }
        Some(String::from_utf8_lossy(&self.b[s..self.p]).into_owned())
    }

    /// Text up to `<` (or EOF) with entities decoded and newlines normalized.
    fn text(&mut self, raw: &[u8]) -> Result<String, String> {
        let mut out: Vec<u8> = Vec::with_capacity(raw.len());
        let mut i = 0;
        while i < raw.len() {
            let c = raw[i];
            if c == b'\r' {
                out.push(b'\n');
                i += if raw.get(i + 1) == Some(&b'\n') { 2 } else { 1 };
                continue;
            }
            if c != b'&' {
                out.push(c);
                i += 1;
                continue;
            }
            let rest = &raw[i + 1..];
            let end = rest.iter().position(|&c| c == b';');
            let ent = match end {
                Some(e) => &rest[..e],
                None => {
                    let n = rest.iter().take_while(|c| is_name_byte(**c) || **c == b'#').count();
                    return Err(self.syntax(&format!("invalid character entity &{} (no semicolon)", String::from_utf8_lossy(&rest[..n]))));
                }
            };
            let s = String::from_utf8_lossy(ent).into_owned();
            let r: Option<char> = match s.as_str() {
                "lt" => Some('<'),
                "gt" => Some('>'),
                "amp" => Some('&'),
                "apos" => Some('\''),
                "quot" => Some('"'),
                _ => {
                    if let Some(h) = s.strip_prefix("#x") {
                        u32::from_str_radix(h, 16).ok().and_then(char::from_u32)
                    } else if let Some(d) = s.strip_prefix('#') {
                        d.parse::<u32>().ok().and_then(char::from_u32)
                    } else {
                        None
                    }
                }
            };
            match r {
                Some(ch) => {
                    let mut buf = [0u8; 4];
                    out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                }
                None => return Err(self.syntax(&format!("invalid character entity &{s};"))),
            }
            i += 1 + ent.len() + 1;
        }
        Ok(String::from_utf8_lossy(&out).into_owned())
    }

    /// Skips to just after `end`; false at EOF.
    fn until(&mut self, end: &[u8]) -> Option<usize> {
        let s = self.p;
        loop {
            if self.b[self.p..].starts_with(end) {
                let at = self.p;
                for _ in 0..end.len() {
                    self.get();
                }
                return Some(at - s);
            }
            self.get()?;
        }
    }

    fn proc_inst(&mut self) -> Result<Tok, String> {
        let target = self.name().unwrap_or_default();
        let s = self.p;
        let n = self.until(b"?>").ok_or_else(|| self.eof_err())?;
        if target == "xml" {
            let content = String::from_utf8_lossy(&self.b[s..s + n]).into_owned();
            let ver = proc_param("version", &content);
            if !ver.is_empty() && ver != "1.0" {
                return Err(format!("xml: unsupported version {}; only version 1.0 is supported", yaml::go_quote(&ver)));
            }
            let enc = proc_param("encoding", &content);
            if !enc.is_empty() && !enc.eq_ignore_ascii_case("utf-8") {
                return Err(format!("xml: encoding {} declared but Decoder.CharsetReader is nil", yaml::go_quote(&enc)));
            }
        }
        Ok(Tok::Other)
    }

    fn raw_token(&mut self) -> Result<Tok, String> {
        if self.pending_end {
            self.pending_end = false;
            let n = self.stack.pop().unwrap_or_default();
            return Ok(Tok::End(n));
        }
        let Some(c) = self.peek() else {
            return Err(if self.stack.is_empty() { EOF.to_string() } else { self.eof_err() });
        };
        if c != b'<' {
            let s = self.p;
            while self.peek().is_some_and(|c| c != b'<') {
                self.get();
            }
            let raw = self.b[s..self.p].to_vec();
            return Ok(Tok::Text(self.text(&raw)?));
        }
        self.get();
        match self.peek() {
            None => Err(self.eof_err()),
            Some(b'/') => {
                self.get();
                let name = self.name().ok_or_else(|| self.syntax("expected element name after </"))?;
                self.space();
                match self.get() {
                    Some(b'>') => {}
                    None => return Err(self.eof_err()),
                    Some(_) => return Err(self.syntax(&format!("invalid characters between </{name} and >"))),
                }
                let name = local(&name);
                match self.stack.last() {
                    None => Err(self.syntax(&format!("unexpected end element </{name}>"))),
                    Some(top) if *top != name => Err(self.syntax(&format!("element <{top}> closed by </{name}>"))),
                    Some(_) => {
                        self.stack.pop();
                        Ok(Tok::End(name))
                    }
                }
            }
            Some(b'?') => {
                self.get();
                self.proc_inst()
            }
            Some(b'!') => {
                self.get();
                if self.b[self.p..].starts_with(b"--") {
                    self.p += 2;
                    self.until(b"-->").ok_or_else(|| self.eof_err())?;
                    return Ok(Tok::Other);
                }
                if self.b[self.p..].starts_with(b"[CDATA[") {
                    self.p += 7;
                    let s = self.p;
                    let n = self.until(b"]]>").ok_or_else(|| self.eof_err())?;
                    let raw = &self.b[s..s + n];
                    let t = String::from_utf8_lossy(raw).replace("\r\n", "\n").replace('\r', "\n");
                    return Ok(Tok::Text(t));
                }
                // directive: up to the matching '>' outside quotes
                let mut depth = 0usize;
                let mut quote: Option<u8> = None;
                loop {
                    let c = self.get().ok_or_else(|| self.eof_err())?;
                    match quote {
                        Some(q) if c == q => quote = None,
                        Some(_) => {}
                        None => match c {
                            b'"' | b'\'' => quote = Some(c),
                            b'<' => depth += 1,
                            b'>' if depth == 0 => return Ok(Tok::Other),
                            b'>' => depth -= 1,
                            _ => {}
                        },
                    }
                }
            }
            Some(_) => {
                let name = self.name().ok_or_else(|| self.syntax("expected element name after <"))?;
                loop {
                    self.space();
                    match self.peek() {
                        None => return Err(self.eof_err()),
                        Some(b'/') => {
                            self.get();
                            if self.get() != Some(b'>') {
                                return Err(self.syntax("expected /> in element"));
                            }
                            self.pending_end = true;
                            break;
                        }
                        Some(b'>') => {
                            self.get();
                            break;
                        }
                        Some(_) => {
                            if self.name().is_none() {
                                return Err(self.syntax("expected attribute name in element"));
                            }
                            self.space();
                            if self.get() != Some(b'=') {
                                return Err(self.syntax("attribute name without = in element"));
                            }
                            self.space();
                            match self.get() {
                                Some(q @ (b'"' | b'\'')) => {
                                    let s = self.p;
                                    while self.peek().is_some_and(|c| c != q) {
                                        self.get();
                                    }
                                    if self.get().is_none() {
                                        return Err(self.eof_err());
                                    }
                                    let raw = self.b[s..self.p - 1].to_vec();
                                    self.text(&raw)?;
                                }
                                _ => return Err(self.syntax("unquoted or missing attribute value in element")),
                            }
                        }
                    }
                }
                let name = local(&name);
                self.stack.push(name.clone());
                Ok(Tok::Start(name))
            }
        }
    }

    /// Go `Decoder.Token`.
    fn token(&mut self) -> Result<Tok, String> {
        self.raw_token()
    }

    /// Go `Decoder.Skip`.
    fn skip(&mut self) -> Result<(), String> {
        loop {
            match self.token()? {
                Tok::Start(_) => self.skip()?,
                Tok::End(_) => return Ok(()),
                _ => {}
            }
        }
    }
}

/// Go `procInst`: the value of `param="..."` in a processing instruction.
fn proc_param(param: &str, s: &str) -> String {
    let Some(i) = s.find(param) else { return String::new() };
    let v = s[i + param.len()..].trim_start();
    let Some(v) = v.strip_prefix('=') else { return String::new() };
    let v = v.trim_start();
    let Some(q) = v.chars().next().filter(|c| *c == '"' || *c == '\'') else { return String::new() };
    let v = &v[1..];
    match v.find(q) {
        Some(j) => v[..j].to_string(),
        None => String::new(),
    }
}

// ---------------- decoder.go ----------------

const INVALID_XML: &str = "invalid xml";

fn check_type(s: &Schema, kind: &'static str) -> Result<(), String> {
    if s.kind() == kind {
        return Ok(());
    }
    Err(format!("error: type mismatch - can't unmarshal {} to {kind}", s.kind()))
}

/// Go `strconv.ParseBool`.
fn parse_bool(s: &str) -> Result<bool, String> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(format!("strconv.ParseBool: parsing {}: invalid syntax", yaml::go_quote(s))),
    }
}

/// Go `strconv.ParseInt(s, 10, 64)`.
fn parse_int(s: &str) -> Result<i64, String> {
    let body = s.strip_prefix(['+', '-']).unwrap_or(s);
    if body.is_empty() || !body.bytes().all(|c| c.is_ascii_digit()) {
        return Err(format!("strconv.ParseInt: parsing {}: invalid syntax", yaml::go_quote(s)));
    }
    s.parse::<i64>().map_err(|_| format!("strconv.ParseInt: parsing {}: value out of range", yaml::go_quote(s)))
}

impl XmlDecoder<'_> {
    /// Go `decoder.readTag`.
    fn read_tag(&mut self) -> Result<(String, String), String> {
        let name = loop {
            if let Tok::Start(n) = self.token()? {
                break n;
            }
        };
        let value = match self.token()? {
            Tok::Text(t) => t,
            _ => return Err(INVALID_XML.to_string()),
        };
        self.skip()?;
        Ok((name, value))
    }

    /// Go `decoder.decodeValue`.
    fn decode_value(&mut self, val: &mut Value, s: &Schema) -> Result<(), String> {
        let type_name = loop {
            match self.token()? {
                Tok::End(n) => return if n == "value" { Ok(()) } else { Err(INVALID_XML.to_string()) },
                Tok::Start(n) => break n,
                // Treat value data without type identifier as string
                Tok::Text(t) => {
                    let v = t.trim();
                    if !v.is_empty() {
                        check_type(s, "string")?;
                        *val = Value::Str(v.to_string());
                        return Ok(());
                    }
                }
                Tok::Other => {}
            }
        };
        match type_name.as_str() {
            "struct" => {
                check_type(s, "struct")?;
                let Schema::Struct(fields) = s else { unreachable!() };
                loop {
                    match self.token()? {
                        Tok::Start(n) => {
                            if n != "member" {
                                return Err(INVALID_XML.to_string());
                            }
                            let (tag_name, field_name) = self.read_tag()?;
                            if tag_name != "name" {
                                return Err(INVALID_XML.to_string());
                            }
                            if let Some(i) = fields.iter().position(|(n, _)| *n == field_name) {
                                loop {
                                    if let Tok::Start(n) = self.token()?
                                        && n == "value"
                                    {
                                        let Value::Struct(fv) = val else { unreachable!() };
                                        self.decode_value(&mut fv[i], &fields[i].1)?;
                                        // </value>
                                        self.skip()?;
                                        break;
                                    }
                                }
                            }
                            // </member>
                            self.skip()?;
                        }
                        Tok::End(_) => break,
                        _ => {}
                    }
                }
            }
            "array" => {
                check_type(s, "slice")?;
                let Schema::Slice(elem) = s else { unreachable!() };
                let mut slice: Vec<Value> = match val {
                    Value::Slice(v) => std::mem::take(v),
                    _ => Vec::new(),
                };
                loop {
                    match self.token()? {
                        Tok::Start(n) => {
                            let mut index = 0usize;
                            if n != "data" {
                                return Err(INVALID_XML.to_string());
                            }
                            loop {
                                match self.token()? {
                                    Tok::Start(tt) => {
                                        if tt != "value" {
                                            return Err(INVALID_XML.to_string());
                                        }
                                        if index < slice.len() {
                                            return Err("error: cannot write to non-pointer array element".to_string());
                                        }
                                        let mut v = elem.zero();
                                        self.decode_value(&mut v, elem)?;
                                        slice.push(v);
                                        // </value>
                                        self.skip()?;
                                        index += 1;
                                    }
                                    Tok::End(_) => {
                                        *val = Value::Slice(slice.clone());
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Tok::End(_) => break,
                        _ => {}
                    }
                }
            }
            _ => {
                let data = match self.token()? {
                    Tok::End(_) => return Ok(()),
                    Tok::Text(t) => t,
                    _ => return Err(INVALID_XML.to_string()),
                };
                match type_name.as_str() {
                    "int" | "i4" | "i8" => {
                        check_type(s, "int")?;
                        *val = Value::Int(parse_int(&data)?);
                    }
                    "string" | "base64" => {
                        check_type(s, "string")?;
                        *val = Value::Str(data);
                    }
                    "dateTime.iso8601" => {
                        return Err(format!("error: type mismatch error - can't decode {} to time", s.kind()));
                    }
                    "boolean" => {
                        let v = parse_bool(&data)?;
                        check_type(s, "bool")?;
                        *val = Value::Bool(v);
                    }
                    "double" => {
                        check_type(s, "float32")?;
                    }
                    _ => return Err("unsupported type".to_string()),
                }
                // </type>
                self.skip()?;
            }
        }
        Ok(())
    }
}

/// Go `unmarshal(data, v)`.
pub fn unmarshal(data: &[u8], s: &Schema) -> Result<Value, String> {
    let mut d = XmlDecoder::new(data);
    let mut v = s.zero();
    loop {
        if let Tok::Start(n) = d.token()?
            && n == "value"
        {
            d.decode_value(&mut v, s)?;
            break;
        }
    }
    // read until end of document
    match d.skip() {
        Err(e) if e != EOF => Err(e),
        _ => Ok(v),
    }
}

// ---------------- response.go ----------------

const FAULT_SCHEMA: Schema = Schema::Struct(&[("faultCode", Schema::Int), ("faultString", Schema::Str)]);

/// Go `faultRx.Match`: `<fault>(\s|\S)+</fault>`.
fn has_fault(b: &[u8]) -> bool {
    match b.windows(7).position(|w| w == b"<fault>") {
        Some(i) => b.windows(8).rposition(|w| w == b"</fault>").is_some_and(|j| j > i + 7),
        None => false,
    }
}

/// Go `Response.Err`.
fn response_err(b: &[u8]) -> Option<String> {
    if !has_fault(b) {
        return None;
    }
    match unmarshal(b, &FAULT_SCHEMA) {
        Err(e) => Some(e),
        Ok(f) => Some(format!("Fault({}): {}", f.field(0).int(), f.field(1).str())),
    }
}

// ---------------- client.go ----------------

/// Go `xmlrpc.Client` (an `rpc.Client` with the HTTP codec): one per refresh, with its own cookie jar.
pub struct Client {
    url: String,
    http: net::Client,
    cookies: Vec<(String, String)>,
}

impl Client {
    /// Go `NewClient`.
    pub fn new(url: &str, transport: Arc<dyn net::RoundTripper>) -> Result<Client, String> {
        yaml::url::parse(url)?;
        Ok(Client { url: url.to_string(), http: net::Client::new(transport), cookies: Vec::new() })
    }

    /// Go `rpc.Client.Call`: the error texts of net/rpc with the xmlrpc codec.
    pub fn call(&mut self, method: &str, args: &[Arg<'_>], reply: &Schema) -> Result<Value, String> {
        // WriteRequest
        let body = encode_method_call(method, args);
        let mut req = net::Request::new("POST", &self.url, body.into_bytes()).map_err(|e| e.to_string())?;
        req.header.set("Content-Type", "text/xml");
        if !self.cookies.is_empty() {
            let c: Vec<String> = self.cookies.iter().map(|(k, v)| format!("{k}={v}")).collect();
            req.header.set("Cookie", &c.join("; "));
        }
        let resp = self.http.do_request(req).map_err(|e| e.to_string())?;
        for sc in resp.header.values("Set-Cookie") {
            let nv = sc.split(';').next().unwrap_or("").trim();
            if let Some((n, v)) = nv.split_once('=') {
                let (n, v) = (n.trim().to_string(), v.trim().to_string());
                match self.cookies.iter_mut().find(|(k, _)| *k == n) {
                    Some(e) => e.1 = v,
                    None => self.cookies.push((n, v)),
                }
            }
        }
        // ReadResponseHeader: errors become rpc.ServerError
        if resp.status < 200 || resp.status >= 300 {
            return Err(format!("request error: bad status code - {}", resp.status));
        }
        if let Some(e) = response_err(&resp.body) {
            return Err(e);
        }
        // ReadResponseBody
        unmarshal(&resp.body, reply).map_err(|e| format!("reading body {e}"))
    }
}
