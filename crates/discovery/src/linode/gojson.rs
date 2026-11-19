//! The parts of Go `encoding/json` decoding that the puppetdb, linode and vultr providers need, with Go's error
//! texts: the syntax check (Go's scanner), and helpers that decode `json::Json` values into typed fields with
//! Go's `UnmarshalTypeError` messages ("json: cannot unmarshal string into Go struct field Instance.ram of type
//! int").
//!
//! Struct decoders loop over the object's keys in order and match field names case-insensitively, as Go does.
//! `null` leaves a field at its zero value, as Go does.
use std::fmt;

use json::Json;

/// A decoding error: a type mismatch (with Go's error context) or any other error text.
#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    Type { value: String, strukt: Option<String>, field: String, ty: String },
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Type { value, strukt, field, ty } => {
                if strukt.is_some() || !field.is_empty() {
                    write!(f, "json: cannot unmarshal {value} into Go struct field {}.{field} of type {ty}", strukt.as_deref().unwrap_or(""))
                } else {
                    write!(f, "json: cannot unmarshal {value} into Go value of type {ty}")
                }
            }
            Error::Other(s) => f.write_str(s),
        }
    }
}

impl From<Error> for String {
    fn from(e: Error) -> String {
        e.to_string()
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Go's decode error context: the struct being decoded and the JSON field names from the top value.
#[derive(Clone, Debug, Default)]
pub struct Ctx {
    pub strukt: Option<String>,
    pub stack: Vec<String>,
}

impl Ctx {
    /// The top-level value (no context).
    pub fn top() -> Ctx {
        Ctx::default()
    }
    /// The context of field `name` of struct `strukt` (Go type name without package).
    pub fn field(&self, strukt: &str, name: &str) -> Ctx {
        let mut stack = self.stack.clone();
        stack.push(name.to_string());
        Ctx { strukt: Some(strukt.to_string()), stack }
    }
    /// A type error for a value of kind `value` into Go type `ty`.
    pub fn err(&self, value: &str, ty: &str) -> Error {
        Error::Type { value: value.to_string(), strukt: self.strukt.clone(), field: self.stack.join("."), ty: ty.to_string() }
    }
    fn kind_err(&self, v: &Json, ty: &str) -> Error {
        self.err(kind(v), ty)
    }
}

/// Go's name of a JSON literal kind in type errors.
pub fn kind(v: &Json) -> &'static str {
    match v {
        Json::Null => "null",
        Json::Bool(_) => "bool",
        Json::Int(_) | Json::Float(_) => "number",
        Json::Str(_) => "string",
        Json::Arr(_) => "array",
        Json::Obj(_) => "object",
    }
}

/// Go `json.Unmarshal`'s first step: the syntax check, then the parse.
pub fn parse(b: &[u8]) -> Result<Json> {
    check_valid(b).map_err(Error::Other)?;
    Json::parse_bytes(b).map_err(|e| Error::Other(e.to_string()))
}

/// A Go `string` (or named string type `ty`).
pub fn string_ty(v: &Json, ctx: &Ctx, ty: &str) -> Result<String> {
    match v {
        Json::Null => Ok(String::new()),
        Json::Str(s) => Ok(s.clone()),
        _ => Err(ctx.kind_err(v, ty)),
    }
}

/// A Go `string`.
pub fn string(v: &Json, ctx: &Ctx) -> Result<String> {
    string_ty(v, ctx, "string")
}

/// A Go `int` (64 bits).
pub fn int(v: &Json, ctx: &Ctx) -> Result<i64> {
    match v {
        Json::Null => Ok(0),
        Json::Int(i) => Ok(*i),
        Json::Float(f) => {
            let mut s = String::new();
            json::write_f64(*f, &mut s);
            Err(ctx.err(&format!("number {s}"), "int"))
        }
        _ => Err(ctx.kind_err(v, "int")),
    }
}

/// A Go `bool`.
pub fn bool(v: &Json, ctx: &Ctx) -> Result<bool> {
    match v {
        Json::Null => Ok(false),
        Json::Bool(b) => Ok(*b),
        _ => Err(ctx.kind_err(v, "bool")),
    }
}

/// A Go `[]string`.
pub fn strings(v: &Json, ctx: &Ctx) -> Result<Vec<String>> {
    match v {
        Json::Null => Ok(Vec::new()),
        Json::Arr(a) => a.iter().map(|e| string(e, ctx)).collect(),
        _ => Err(ctx.kind_err(v, "[]string")),
    }
}

/// The elements of a Go slice of type `ty` (empty for null).
pub fn array<'a>(v: &'a Json, ctx: &Ctx, ty: &str) -> Result<&'a [Json]> {
    match v {
        Json::Null => Ok(&[]),
        Json::Arr(a) => Ok(a),
        _ => Err(ctx.kind_err(v, ty)),
    }
}

/// The members of a Go struct of type `ty` (None for null), in order.
pub fn object<'a>(v: &'a Json, ctx: &Ctx, ty: &str) -> Result<Option<&'a [(String, Json)]>> {
    match v {
        Json::Null => Ok(None),
        Json::Obj(o) => Ok(Some(o)),
        _ => Err(ctx.kind_err(v, ty)),
    }
}

/// Go's key match for a struct field (ASCII case folding).
pub fn key_is(key: &str, name: &str) -> bool {
    key.eq_ignore_ascii_case(name)
}

// ---------------- Go's scanner (encoding/json/scanner.go): the syntax errors ----------------

#[derive(Clone, Copy, PartialEq)]
enum Ps {
    ObjectKey,
    ObjectValue,
    ArrayValue,
}

#[derive(Clone, Copy, PartialEq)]
enum St {
    BeginValue,
    BeginValueOrEmpty,
    BeginStringOrEmpty,
    BeginString,
    EndValue,
    EndTop,
    InString,
    InStringEsc,
    InStringEscU(u8),
    Neg,
    S1,
    S0,
    Dot,
    Dot0,
    E,
    ESign,
    E0,
    Lit(&'static [u8], u8, &'static str),
}

struct Scanner {
    step: St,
    ps: Vec<Ps>,
    end_top: bool,
    err: Option<String>,
}

fn is_space(c: u8) -> bool {
    c == b' ' || c == b'\t' || c == b'\r' || c == b'\n'
}

fn quote_char(c: u8) -> String {
    match c {
        b'\'' => r"'\''".to_string(),
        b'"' => "'\"'".to_string(),
        _ => {
            // Go `string(c)` of a byte is the rune U+00XX.
            let q = yaml::go_quote(&char::from(c).to_string());
            format!("'{}'", &q[1..q.len() - 1])
        }
    }
}

const MAX_NESTING_DEPTH: usize = 10000;

impl Scanner {
    fn error(&mut self, c: u8, context: &str) -> bool {
        if self.err.is_none() {
            self.err = Some(format!("invalid character {} {}", quote_char(c), context));
        }
        false
    }

    fn push(&mut self, c: u8, p: Ps) -> bool {
        self.ps.push(p);
        if self.ps.len() <= MAX_NESTING_DEPTH {
            return true;
        }
        self.error(c, "exceeded max depth")
    }

    fn pop(&mut self) {
        self.ps.pop();
        if self.ps.is_empty() {
            self.step = St::EndTop;
            self.end_top = true;
        } else {
            self.step = St::EndValue;
        }
    }

    /// One byte; false on error.
    fn step(&mut self, c: u8) -> bool {
        let hex = |c: u8| c.is_ascii_hexdigit();
        match self.step {
            St::BeginValueOrEmpty => {
                if is_space(c) {
                    return true;
                }
                if c == b']' {
                    return self.end_value(c);
                }
                self.begin_value(c)
            }
            St::BeginValue => self.begin_value(c),
            St::BeginStringOrEmpty => {
                if is_space(c) {
                    return true;
                }
                if c == b'}' {
                    if let Some(l) = self.ps.last_mut() {
                        *l = Ps::ObjectValue;
                    }
                    return self.end_value(c);
                }
                self.begin_string(c)
            }
            St::BeginString => self.begin_string(c),
            St::EndValue => self.end_value(c),
            St::EndTop => {
                if !is_space(c) {
                    self.error(c, "after top-level value");
                    return false;
                }
                true
            }
            St::InString => {
                if c == b'"' {
                    self.step = St::EndValue;
                } else if c == b'\\' {
                    self.step = St::InStringEsc;
                } else if c < 0x20 {
                    return self.error(c, "in string literal");
                }
                true
            }
            St::InStringEsc => match c {
                b'b' | b'f' | b'n' | b'r' | b't' | b'\\' | b'/' | b'"' => {
                    self.step = St::InString;
                    true
                }
                b'u' => {
                    self.step = St::InStringEscU(0);
                    true
                }
                _ => self.error(c, "in string escape code"),
            },
            St::InStringEscU(n) => {
                if !hex(c) {
                    return self.error(c, "in \\u hexadecimal character escape");
                }
                self.step = if n == 3 { St::InString } else { St::InStringEscU(n + 1) };
                true
            }
            St::Neg => {
                if c == b'0' {
                    self.step = St::S0;
                } else if (b'1'..=b'9').contains(&c) {
                    self.step = St::S1;
                } else {
                    return self.error(c, "in numeric literal");
                }
                true
            }
            St::S1 => {
                if c.is_ascii_digit() {
                    return true;
                }
                self.state0(c)
            }
            St::S0 => self.state0(c),
            St::Dot => {
                if c.is_ascii_digit() {
                    self.step = St::Dot0;
                    return true;
                }
                self.error(c, "after decimal point in numeric literal")
            }
            St::Dot0 => {
                if c.is_ascii_digit() {
                    return true;
                }
                if c == b'e' || c == b'E' {
                    self.step = St::E;
                    return true;
                }
                self.end_value(c)
            }
            St::E => {
                if c == b'+' || c == b'-' {
                    self.step = St::ESign;
                    return true;
                }
                self.e_sign(c)
            }
            St::ESign => self.e_sign(c),
            St::E0 => {
                if c.is_ascii_digit() {
                    return true;
                }
                self.end_value(c)
            }
            St::Lit(word, i, name) => {
                if c == word[i as usize] {
                    self.step = if i as usize + 1 == word.len() { St::EndValue } else { St::Lit(word, i + 1, name) };
                    return true;
                }
                self.error(c, &format!("in literal {name} (expecting {})", quote_char(word[i as usize])))
            }
        }
    }

    fn state0(&mut self, c: u8) -> bool {
        if c == b'.' {
            self.step = St::Dot;
            return true;
        }
        if c == b'e' || c == b'E' {
            self.step = St::E;
            return true;
        }
        self.end_value(c)
    }

    fn e_sign(&mut self, c: u8) -> bool {
        if c.is_ascii_digit() {
            self.step = St::E0;
            return true;
        }
        self.error(c, "in exponent of numeric literal")
    }

    fn begin_value(&mut self, c: u8) -> bool {
        if is_space(c) {
            return true;
        }
        match c {
            b'{' => {
                self.step = St::BeginStringOrEmpty;
                self.push(c, Ps::ObjectKey)
            }
            b'[' => {
                self.step = St::BeginValueOrEmpty;
                self.push(c, Ps::ArrayValue)
            }
            b'"' => {
                self.step = St::InString;
                true
            }
            b'-' => {
                self.step = St::Neg;
                true
            }
            b'0' => {
                self.step = St::S0;
                true
            }
            b't' => {
                self.step = St::Lit(b"true", 1, "true");
                true
            }
            b'f' => {
                self.step = St::Lit(b"false", 1, "false");
                true
            }
            b'n' => {
                self.step = St::Lit(b"null", 1, "null");
                true
            }
            b'1'..=b'9' => {
                self.step = St::S1;
                true
            }
            _ => self.error(c, "looking for beginning of value"),
        }
    }

    fn begin_string(&mut self, c: u8) -> bool {
        if is_space(c) {
            return true;
        }
        if c == b'"' {
            self.step = St::InString;
            return true;
        }
        self.error(c, "looking for beginning of object key string")
    }

    fn end_value(&mut self, c: u8) -> bool {
        let Some(&ps) = self.ps.last() else {
            self.step = St::EndTop;
            self.end_top = true;
            return self.step(c);
        };
        if is_space(c) {
            self.step = St::EndValue;
            return true;
        }
        match ps {
            Ps::ObjectKey => {
                if c == b':' {
                    if let Some(l) = self.ps.last_mut() {
                        *l = Ps::ObjectValue;
                    }
                    self.step = St::BeginValue;
                    return true;
                }
                self.error(c, "after object key")
            }
            Ps::ObjectValue => {
                if c == b',' {
                    if let Some(l) = self.ps.last_mut() {
                        *l = Ps::ObjectKey;
                    }
                    self.step = St::BeginString;
                    return true;
                }
                if c == b'}' {
                    self.pop();
                    return true;
                }
                self.error(c, "after object key:value pair")
            }
            Ps::ArrayValue => {
                if c == b',' {
                    self.step = St::BeginValue;
                    return true;
                }
                if c == b']' {
                    self.pop();
                    return true;
                }
                self.error(c, "after array element")
            }
        }
    }
}

/// Go `checkValid`: the syntax error of `b` as Go's `SyntaxError.Error()`, if any.
pub fn check_valid(b: &[u8]) -> std::result::Result<(), String> {
    let mut s = Scanner { step: St::BeginValue, ps: Vec::new(), end_top: false, err: None };
    for &c in b {
        if !s.step(c) {
            return Err(s.err.unwrap_or_default());
        }
    }
    // eof
    if s.end_top {
        return Ok(());
    }
    s.step(b' ');
    if s.end_top {
        return Ok(());
    }
    Err(s.err.unwrap_or_else(|| "unexpected end of JSON input".to_string()))
}
