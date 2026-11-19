//! Go standard library behavior that the Consul and Nomad API clients need: `encoding/json` decoding into typed
//! values with Go's error texts, `fmt`'s `%v` of a decoded `interface{}`, `url.Values`, `strconv.ParseBool`, a Go
//! ticker, and an HTTP call that a `CancelToken` can abandon (Go `req.WithContext(ctx)`).
use std::sync::mpsc;
use std::time::{Duration, Instant};

use json::Json;

use crate::cancel::CancelToken;

/// A Go type that JSON is decoded into, for Go's type checks and error texts.
pub enum Ty {
    /// `string` (or a named string type such as `api.ServiceKind`).
    Str,
    /// `int` (64 bit).
    Int,
    Uint64,
    Bool,
    /// `interface{}`, or a type whose content Prometheus does not read and that is not checked here.
    Any,
    /// A slice of the element type; the Go type name ("[]string").
    Slice(&'static Ty, &'static str),
    /// A map with string keys; the Go type name ("map[string]string").
    Map(&'static Ty, &'static str),
    /// A struct or a pointer to a struct.
    Struct(&'static StructTy),
}

/// A Go struct: its name ("ServiceEntry"), its qualified type ("api.ServiceEntry") and its fields.
pub struct StructTy {
    pub name: &'static str,
    pub go_type: &'static str,
    pub fields: &'static [(&'static str, Ty)],
}

impl Ty {
    fn go_name(&self) -> &'static str {
        match self {
            Ty::Str => "string",
            Ty::Int => "int",
            Ty::Uint64 => "uint64",
            Ty::Bool => "bool",
            Ty::Any => "interface {}",
            Ty::Slice(_, n) | Ty::Map(_, n) => n,
            Ty::Struct(s) => s.go_type,
        }
    }
}

/// The JSON kind name in Go's `UnmarshalTypeError`.
fn kind(v: &Json) -> &'static str {
    model::labelset::json_kind(v)
}

/// Go's text of a number literal (the original literal is not kept by the parser).
fn number_text(v: &Json) -> String {
    let mut s = String::from("number ");
    match v {
        Json::Int(i) => json::write_i64(*i, &mut s),
        Json::Float(f) => json::write_f64(*f, &mut s),
        _ => {}
    }
    s
}

/// Go's `decodeState.errorContext`: the innermost struct and the field path.
#[derive(Default)]
struct ErrCtx {
    st: Option<&'static str>,
    stack: Vec<&'static str>,
}

impl ErrCtx {
    fn err(&self, value: &str, ty: &Ty) -> String {
        match self.st {
            Some(st) => format!("json: cannot unmarshal {value} into Go struct field {st}.{} of type {}", self.stack.join("."), ty.go_name()),
            None => format!("json: cannot unmarshal {value} into Go value of type {}", ty.go_name()),
        }
    }
}

/// Go's field lookup: an exact name match first, else a case-insensitive one.
fn find_field(s: &'static StructTy, k: &str) -> Option<&'static (&'static str, Ty)> {
    s.fields.iter().find(|(n, _)| *n == k).or_else(|| s.fields.iter().find(|(n, _)| n.eq_ignore_ascii_case(k) || n.to_lowercase() == k.to_lowercase()))
}

/// The first type error Go's decoder reports for `v` decoded into `ty` (fields in document order).
fn check(v: &Json, ty: &Ty, cx: &mut ErrCtx) -> Result<(), String> {
    match (ty, v) {
        // null leaves the Go value as it is (or sets a nil map, slice or pointer)
        (_, Json::Null) | (Ty::Any, _) => Ok(()),
        (Ty::Str, Json::Str(_)) | (Ty::Bool, Json::Bool(_)) => Ok(()),
        (Ty::Int, Json::Int(_)) => Ok(()),
        (Ty::Uint64, Json::Int(i)) if *i >= 0 => Ok(()),
        (Ty::Int | Ty::Uint64, Json::Int(_) | Json::Float(_)) => Err(cx.err(&number_text(v), ty)),
        (Ty::Slice(e, _), Json::Arr(a)) => {
            for x in a {
                check(x, e, cx)?;
            }
            Ok(())
        }
        (Ty::Map(e, _), Json::Obj(o)) => {
            for (_, x) in o {
                check(x, e, cx)?;
            }
            Ok(())
        }
        (Ty::Struct(s), Json::Obj(o)) => {
            for (k, x) in o {
                let Some((name, fty)) = find_field(s, k) else { continue };
                let (st, depth) = (cx.st, cx.stack.len());
                cx.st = Some(s.name);
                cx.stack.push(name);
                let r = check(x, fty, cx);
                cx.st = st;
                cx.stack.truncate(depth);
                r?;
            }
            Ok(())
        }
        _ => Err(cx.err(kind(v), ty)),
    }
}

/// Go `strconv.Quote` of one rune with single quotes, as the JSON scanner prints it.
fn quote_char(c: char) -> String {
    match c {
        '\'' => "'\\''".to_string(),
        '"' => "'\"'".to_string(),
        _ => {
            let q = yaml::go_quote(&c.to_string());
            format!("'{}'", &q[1..q.len() - 1])
        }
    }
}

/// Go's `encoding/json` scanner over the first value of a stream (Go `Decoder.readValue`): the end of that value,
/// or Go's error text ("unexpected EOF", "invalid character 'x' after object key", ...).
struct Scanner<'a> {
    b: &'a [u8],
    i: usize,
    depth: usize,
}

const UNEXPECTED_EOF: &str = "unexpected EOF";

fn invalid(c: u8, context: &str) -> String {
    format!("invalid character {} {context}", quote_char(c as char))
}

impl Scanner<'_> {
    fn skip_ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\r' | b'\n') {
            self.i += 1;
        }
    }
    fn peek(&self) -> Result<u8, String> {
        self.b.get(self.i).copied().ok_or_else(|| UNEXPECTED_EOF.to_string())
    }
    fn value(&mut self) -> Result<(), String> {
        self.skip_ws();
        let c = self.peek()?;
        match c {
            b'{' => {
                self.nest()?;
                self.i += 1;
                self.skip_ws();
                if self.peek()? == b'}' {
                    self.i += 1;
                    self.depth -= 1;
                    return Ok(());
                }
                loop {
                    let c = self.peek()?;
                    if c != b'"' {
                        return Err(invalid(c, "looking for beginning of object key string"));
                    }
                    self.string()?;
                    self.skip_ws();
                    let c = self.peek()?;
                    if c != b':' {
                        return Err(invalid(c, "after object key"));
                    }
                    self.i += 1;
                    self.value()?;
                    self.skip_ws();
                    match self.peek()? {
                        b',' => {
                            self.i += 1;
                            self.skip_ws();
                        }
                        b'}' => {
                            self.i += 1;
                            self.depth -= 1;
                            return Ok(());
                        }
                        c => return Err(invalid(c, "after object key:value pair")),
                    }
                }
            }
            b'[' => {
                self.nest()?;
                self.i += 1;
                self.skip_ws();
                if self.peek()? == b']' {
                    self.i += 1;
                    self.depth -= 1;
                    return Ok(());
                }
                loop {
                    self.value()?;
                    self.skip_ws();
                    match self.peek()? {
                        b',' => self.i += 1,
                        b']' => {
                            self.i += 1;
                            self.depth -= 1;
                            return Ok(());
                        }
                        c => return Err(invalid(c, "after array element")),
                    }
                }
            }
            b'"' => self.string(),
            b'-' | b'0'..=b'9' => self.number(),
            b't' => self.literal("true"),
            b'f' => self.literal("false"),
            b'n' => self.literal("null"),
            c => Err(invalid(c, "looking for beginning of value")),
        }
    }
    fn nest(&mut self) -> Result<(), String> {
        // Go allows 10000 levels; the value parser here allows json::MAX_DEPTH
        self.depth += 1;
        if self.depth > json::MAX_DEPTH {
            return Err("exceeded max depth".to_string());
        }
        Ok(())
    }
    fn string(&mut self) -> Result<(), String> {
        self.i += 1;
        loop {
            let c = self.peek()?;
            self.i += 1;
            match c {
                b'"' => return Ok(()),
                b'\\' => {
                    let e = self.peek()?;
                    self.i += 1;
                    match e {
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {}
                        b'u' => {
                            for _ in 0..4 {
                                let h = self.peek()?;
                                if !h.is_ascii_hexdigit() {
                                    return Err(invalid(h, "in \\u hexadecimal character escape"));
                                }
                                self.i += 1;
                            }
                        }
                        _ => return Err(invalid(e, "in string escape code")),
                    }
                }
                c if c < 0x20 => return Err(invalid(c, "in string literal")),
                _ => {}
            }
        }
    }
    fn digits(&mut self) {
        while self.i < self.b.len() && self.b[self.i].is_ascii_digit() {
            self.i += 1;
        }
    }
    fn number(&mut self) -> Result<(), String> {
        if self.b[self.i] == b'-' {
            self.i += 1;
            let c = self.peek()?;
            if !c.is_ascii_digit() {
                return Err(invalid(c, "in numeric literal"));
            }
        }
        if self.b[self.i] == b'0' {
            self.i += 1;
        } else {
            self.digits();
        }
        if self.b.get(self.i) == Some(&b'.') {
            self.i += 1;
            let c = self.peek()?;
            if !c.is_ascii_digit() {
                return Err(invalid(c, "after decimal point in numeric literal"));
            }
            self.digits();
        }
        if matches!(self.b.get(self.i), Some(b'e' | b'E')) {
            self.i += 1;
            if matches!(self.b.get(self.i), Some(b'+' | b'-')) {
                self.i += 1;
            }
            let c = self.peek()?;
            if !c.is_ascii_digit() {
                return Err(invalid(c, "in exponent of numeric literal"));
            }
            self.digits();
        }
        Ok(())
    }
    fn literal(&mut self, lit: &str) -> Result<(), String> {
        self.i += 1;
        for &want in &lit.as_bytes()[1..] {
            let c = self.peek()?;
            if c != want {
                return Err(invalid(c, &format!("in literal {lit} (expecting {})", quote_char(want as char))));
            }
            self.i += 1;
        }
        Ok(())
    }
}

/// Go `json.NewDecoder(body).Decode(&out)` with `out` of type `ty`: the first JSON value of the body (trailing
/// data is not read), checked against `ty`.
pub fn decode_body(body: &[u8], ty: &Ty) -> Result<Json, String> {
    if body.iter().all(|b| matches!(b, b' ' | b'\t' | b'\r' | b'\n')) {
        return Err("EOF".to_string());
    }
    let mut sc = Scanner { b: body, i: 0, depth: 0 };
    sc.value()?;
    let v = Json::parse_bytes(&body[..sc.i]).map_err(|e| e.to_string())?;
    check(&v, ty, &mut ErrCtx::default())?;
    Ok(v)
}

/// The value of a struct field in a decoded object, found as Go finds it (exact name first, else
/// case-insensitive; the last matching key wins). `None` for a missing field or null.
pub fn field<'a>(v: &'a Json, name: &str) -> Option<&'a Json> {
    let o = v.as_obj()?;
    let exact = o.iter().rev().find(|(k, _)| k == name);
    let found = exact.or_else(|| o.iter().rev().find(|(k, _)| k.eq_ignore_ascii_case(name) || k.to_lowercase() == name.to_lowercase()));
    found.map(|(_, v)| v).filter(|v| !v.is_null())
}

/// A string field (Go zero value "" when missing or null).
pub fn str_field(v: &Json, name: &str) -> String {
    field(v, name).and_then(Json::as_str).unwrap_or("").to_string()
}

/// An int field (0 when missing or null).
pub fn int_field(v: &Json, name: &str) -> i64 {
    field(v, name).and_then(Json::as_i64).unwrap_or(0)
}

/// A `[]string` field (null elements become "").
pub fn str_slice(v: Option<&Json>) -> Vec<String> {
    v.and_then(Json::as_arr).map(|a| a.iter().map(|s| s.as_str().unwrap_or("").to_string()).collect()).unwrap_or_default()
}

/// A `map[string]string` value as Go builds it (the last duplicate key wins; null values become "").
pub fn str_map(v: Option<&Json>) -> std::collections::BTreeMap<String, String> {
    let mut m = std::collections::BTreeMap::new();
    if let Some(o) = v.and_then(Json::as_obj) {
        for (k, x) in o {
            m.insert(k.clone(), x.as_str().unwrap_or("").to_string());
        }
    }
    m
}

/// Go `fmt.Sprintf("%v", x)` of a value decoded into `interface{}`.
pub fn fmt_v(v: Option<&Json>) -> String {
    let mut s = String::new();
    write_v(v.unwrap_or(&Json::Null), &mut s);
    s
}

fn write_v(v: &Json, s: &mut String) {
    match v {
        Json::Null => s.push_str("<nil>"),
        Json::Bool(b) => s.push_str(if *b { "true" } else { "false" }),
        Json::Int(i) => s.push_str(&yaml::format_float_g(*i as f64)),
        Json::Float(f) => s.push_str(&yaml::format_float_g(*f)),
        Json::Str(x) => s.push_str(x),
        Json::Arr(a) => {
            s.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    s.push(' ');
                }
                write_v(x, s);
            }
            s.push(']');
        }
        Json::Obj(o) => {
            // a Go map: the last duplicate key wins; fmt prints keys sorted
            let mut m: std::collections::BTreeMap<&str, &Json> = std::collections::BTreeMap::new();
            for (k, x) in o {
                m.insert(k, x);
            }
            s.push_str("map[");
            for (i, (k, x)) in m.iter().enumerate() {
                if i > 0 {
                    s.push(' ');
                }
                s.push_str(k);
                s.push(':');
                write_v(x, s);
            }
            s.push(']');
        }
    }
}

/// Go `url.Values` (insertion order per key; `encode` sorts by key).
#[derive(Clone, Debug, Default)]
pub struct Values(pub Vec<(String, String)>);

impl Values {
    /// Go `Values.Set`: replaces the values of `k`.
    pub fn set(&mut self, k: &str, v: &str) {
        match self.0.iter().position(|(n, _)| n == k) {
            Some(i) => {
                self.0[i].1 = v.to_string();
                let mut j = i + 1;
                while j < self.0.len() {
                    if self.0[j].0 == k {
                        self.0.remove(j);
                    } else {
                        j += 1;
                    }
                }
            }
            None => self.0.push((k.to_string(), v.to_string())),
        }
    }
    /// Go `Values.Add`.
    pub fn add(&mut self, k: &str, v: &str) {
        self.0.push((k.to_string(), v.to_string()));
    }
    /// Go `Values.Encode`.
    pub fn encode(&self) -> String {
        // values of one key must stay together in insertion order: a stable sort by key does that
        yaml::url::encode_query(&self.0)
    }
}

/// Go `strconv.ParseBool`.
pub fn parse_bool(s: &str) -> Option<bool> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// A Go `time.Ticker` read with `select { case <-ticker.C: case <-ctx.Done(): }`: ticks every interval from its
/// creation; one missed tick is kept (the channel buffer), later ones are dropped.
pub struct Ticker {
    interval: Duration,
    next: Instant,
}

impl Ticker {
    /// Go `time.NewTicker` (a zero interval, which Go rejects with a panic, ticks every nanosecond).
    pub fn new(interval: Duration) -> Ticker {
        let interval = interval.max(Duration::from_nanos(1));
        Ticker { interval, next: Instant::now() + interval }
    }
    /// Waits for the next tick. Returns true when `ctx` was cancelled first.
    pub fn wait(&mut self, ctx: &CancelToken) -> bool {
        let now = Instant::now();
        if self.next > now && ctx.wait_timeout(self.next - now) {
            return true;
        }
        let now = Instant::now();
        while self.next <= now {
            self.next += self.interval;
        }
        false
    }
}

enum Msg {
    Cancelled,
    Done(u64, Box<Result<net::Response, net::Error>>),
}

/// Sends HTTP requests that a cancel token can abandon, as a Go request with a context: when the token is
/// cancelled, `call` returns `Get "<url>": context canceled` at once. The request runs on its own thread, which
/// finishes in the background (at the latest at the client timeout).
pub struct Caller {
    tx: mpsc::Sender<Msg>,
    rx: mpsc::Receiver<Msg>,
    seq: u64,
    cancelled: bool,
}

impl Caller {
    pub fn new(ctx: &CancelToken) -> Caller {
        let (tx, rx) = mpsc::channel();
        let t = tx.clone();
        ctx.on_cancel(Box::new(move || {
            let _ = t.send(Msg::Cancelled);
        }));
        Caller { tx, rx, seq: 0, cancelled: false }
    }

    /// Go `client.Do(req.WithContext(ctx))`.
    pub fn call(&mut self, client: &net::Client, req: net::Request) -> Result<net::Response, net::Error> {
        let canceled = |req: &net::Request| {
            let op = req.method.chars().next().map(|c| c.to_string()).unwrap_or_default() + &req.method.get(1..).unwrap_or("").to_ascii_lowercase();
            // Go `url.Error` with the password stripped
            let mut u = req.url.clone();
            if let Some(ui) = u.user.as_mut()
                && ui.password_set
            {
                ui.password = "***".to_string();
            }
            net::Error::other(format!("{op} {}: context canceled", yaml::go_quote(&u.to_url_string())))
        };
        if self.cancelled {
            return Err(canceled(&req));
        }
        self.seq += 1;
        let id = self.seq;
        let tx = self.tx.clone();
        let c = client.clone();
        let r2 = req.clone();
        let spawned = std::thread::Builder::new().name("sd-http".to_string()).spawn(move || {
            let r = c.do_request(r2);
            let _ = tx.send(Msg::Done(id, Box::new(r)));
        });
        if spawned.is_err() {
            return client.do_request(req);
        }
        loop {
            match self.rx.recv() {
                Ok(Msg::Cancelled) => {
                    self.cancelled = true;
                    return Err(canceled(&req));
                }
                Ok(Msg::Done(i, r)) if i == id => return *r,
                Ok(Msg::Done(..)) => {}
                Err(_) => return Err(canceled(&req)),
            }
        }
    }
}

/// Go `strings.TrimSpace`.
pub fn trim_space(s: &str) -> &str {
    s.trim_matches(|c: char| c.is_whitespace())
}
