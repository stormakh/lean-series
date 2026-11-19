//! Go `encoding/json` decoding into typed structs, as far as SD providers need it, with Go's error texts.
//!
//! Providers describe the Go types they decode as static schemas ([`Ty`], [`StructTy`]) and call
//! [`unmarshal`] (Go `json.Unmarshal`) or [`decode_stream`] (Go `json.NewDecoder(r).Decode`). That returns the
//! parsed [`Json`] after checking it against the schema the way Go does:
//!
//! - syntax errors as Go words them ("unexpected end of JSON input", "invalid character 'x' looking for
//!   beginning of value", ...);
//! - type errors as `*json.UnmarshalTypeError`: "json: cannot unmarshal string into Go struct field
//!   Server.properties.name of type string" (struct name = the innermost enclosing struct, field = the JSON
//!   names from the root, slices add nothing), the first one wins and decoding goes on (Go `saveError`);
//! - errors of `UnmarshalText` (uuid) are saved like type errors; errors of `UnmarshalJSON` (time.Time) stop
//!   decoding and win over saved errors.
//!
//! Then read the fields with [`ptr`] (a Go pointer field: the last matching key, `None` for null) or [`val`]
//! (a value field: null leaves it unchanged). Object keys match field names exactly or case-insensitively, as
//! in Go. Unknown keys are ignored. Fields not in the schema are not checked.
//!
//! Differences from Go: the `json` crate keeps one value per repeated key (the last), so a type error in an
//! earlier duplicate is not seen; a number in an error text is printed from its parsed value ("number 1.5"),
//! not from the literal; syntax error offsets come from the `json` crate parser and can name a different
//! character than Go in some malformed inputs.
use json::Json;

/// A Go type, for checking a decoded value.
pub enum Ty {
    /// `string` (or `*string`).
    Str,
    /// A named string type (`type Type string`): its Go name.
    NamedStr(&'static str),
    Bool,
    /// An integer type: Go name ("int", "int32", "int64") and range.
    Int(&'static str, i64, i64),
    Float32,
    Float64,
    /// `interface{}`: anything.
    Any,
    /// `map[string]T`.
    Map(&'static Ty),
    /// `[]T` (or `*[]T`).
    Slice(&'static Ty),
    Struct(&'static StructTy),
    /// A type with `UnmarshalText` (Go type name, parser): JSON strings go to the parser, other values are
    /// type errors, null is a no-op.
    Text(&'static str, fn(&str) -> Result<(), String>),
    /// A type with `UnmarshalJSON` (time.Time): its error stops decoding.
    Unmarshaler(fn(&Json) -> Result<(), String>),
}

/// A Go struct: its type name with package ("ionoscloud.Server") and its JSON fields.
pub struct StructTy {
    pub go: &'static str,
    pub fields: &'static [(&'static str, Ty)],
}

pub const INT: Ty = Ty::Int("int", i64::MIN, i64::MAX);
pub const INT32: Ty = Ty::Int("int32", i32::MIN as i64, i32::MAX as i64);
pub const INT64: Ty = Ty::Int("int64", i64::MIN, i64::MAX);
pub const STRINGS: Ty = Ty::Slice(&Ty::Str);

impl Ty {
    /// Go `reflect.Type.String()`.
    pub fn name(&self) -> String {
        match self {
            Ty::Str => "string".into(),
            Ty::NamedStr(n) => (*n).into(),
            Ty::Bool => "bool".into(),
            Ty::Int(n, _, _) => (*n).into(),
            Ty::Float32 => "float32".into(),
            Ty::Float64 => "float64".into(),
            Ty::Any => "interface {}".into(),
            Ty::Map(e) => format!("map[string]{}", e.name()),
            Ty::Slice(e) => format!("[]{}", e.name()),
            Ty::Struct(s) => s.go.into(),
            Ty::Text(n, _) => (*n).into(),
            Ty::Unmarshaler(_) => "json.Unmarshaler".into(),
        }
    }
}

struct Ctx {
    strct: Option<&'static str>,
    stack: Vec<&'static str>,
    saved: Option<String>,
}

impl Ctx {
    fn save(&mut self, e: String) {
        if self.saved.is_none() {
            self.saved = Some(e);
        }
    }
    fn type_err(&mut self, value: String, ty: &Ty) {
        let e = if self.strct.is_some() || !self.stack.is_empty() {
            format!("json: cannot unmarshal {value} into Go struct field {}.{} of type {}", self.strct.unwrap_or(""), self.stack.join("."), ty.name())
        } else {
            format!("json: cannot unmarshal {value} into Go value of type {}", ty.name())
        };
        self.save(e);
    }
}

fn kind(v: &Json) -> &'static str {
    match v {
        Json::Null => "null",
        Json::Bool(_) => "bool",
        Json::Int(_) | Json::Float(_) => "number",
        Json::Str(_) => "string",
        Json::Arr(_) => "array",
        Json::Obj(_) => "object",
    }
}

fn num_lit(v: &Json) -> String {
    match v {
        Json::Int(i) => i.to_string(),
        other => other.stringify(),
    }
}

fn check(v: &Json, ty: &Ty, c: &mut Ctx) -> Result<(), String> {
    if let Ty::Unmarshaler(f) = ty {
        return f(v);
    }
    if v.is_null() {
        return Ok(());
    }
    match (ty, v) {
        (Ty::Any, _) => {}
        (Ty::Str | Ty::NamedStr(_), Json::Str(_)) | (Ty::Bool, Json::Bool(_)) | (Ty::Float64, Json::Int(_) | Json::Float(_)) => {}
        (Ty::Int(_, lo, hi), Json::Int(i)) => {
            if i < lo || i > hi {
                c.type_err(format!("number {i}"), ty);
            }
        }
        (Ty::Int(..), Json::Float(_)) => c.type_err(format!("number {}", num_lit(v)), ty),
        (Ty::Float32, Json::Int(_) | Json::Float(_)) => {
            let f = v.as_f64().unwrap_or(0.0);
            if f.abs() > f32::MAX as f64 {
                c.type_err(format!("number {}", num_lit(v)), ty);
            }
        }
        (Ty::Text(_, parse), Json::Str(s)) => {
            if let Err(e) = parse(s) {
                c.save(e);
            }
        }
        (Ty::Slice(e), Json::Arr(items)) => {
            for it in items {
                check(it, e, c)?;
            }
        }
        (Ty::Map(e), Json::Obj(pairs)) => {
            for (_, it) in pairs {
                check(it, e, c)?;
            }
        }
        (Ty::Struct(s), Json::Obj(pairs)) => {
            let outer = c.strct;
            for (k, it) in pairs {
                let Some((name, fty)) = find_field(s, k) else { continue };
                c.stack.push(name);
                c.strct = Some(short_name(s.go));
                let r = check(it, fty, c);
                c.stack.pop();
                c.strct = outer;
                r?;
            }
        }
        _ => c.type_err(kind(v).to_string(), ty),
    }
    Ok(())
}

fn short_name(go: &'static str) -> &'static str {
    go.rsplit('.').next().unwrap_or(go)
}

fn key_matches(key: &str, name: &str) -> bool {
    key == name || key.eq_ignore_ascii_case(name) || key.to_lowercase() == name.to_lowercase()
}

fn find_field(s: &'static StructTy, key: &str) -> Option<(&'static str, &'static Ty)> {
    s.fields.iter().find(|(n, _)| *n == key).or_else(|| s.fields.iter().find(|(n, _)| key_matches(key, n))).map(|(n, t)| (*n, t))
}

/// Checks a parsed value against `ty` (Go's decoding errors, see the module doc).
pub fn check_value(v: &Json, ty: &Ty) -> Result<(), String> {
    let mut c = Ctx { strct: None, stack: Vec::new(), saved: None };
    check(v, ty, &mut c)?;
    match c.saved {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Go `json.Unmarshal(b, &v)` for a `v` of type `ty`.
pub fn unmarshal(b: &[u8], ty: &Ty) -> Result<Json, String> {
    let v = Json::parse_bytes(b).map_err(|e| syntax_error(b, &e))?;
    check_value(&v, ty)?;
    Ok(v)
}

/// Go `json.NewDecoder(bytes.NewReader(b)).Decode(&v)`: the first JSON value; what follows it is not read.
/// Empty input is "EOF", a truncated value "unexpected EOF".
pub fn decode_stream(b: &[u8], ty: &Ty) -> Result<Json, String> {
    if b.iter().all(|c| c.is_ascii_whitespace()) {
        return Err("EOF".to_string());
    }
    let v = match Json::parse_bytes(b) {
        Ok(v) => v,
        Err(e) if e.msg == "trailing data after JSON value" => Json::parse_bytes(&b[..e.offset]).map_err(|e| syntax_error(b, &e))?,
        Err(e) => {
            let s = syntax_error(b, &e);
            return Err(if s == "unexpected end of JSON input" { "unexpected EOF".to_string() } else { s });
        }
    };
    check_value(&v, ty)?;
    Ok(v)
}

/// Go `quoteChar`.
fn quote_char(c: char) -> String {
    match c {
        '\'' => r"'\''".to_string(),
        '"' => r#"'"'"#.to_string(),
        _ => {
            let q = yaml::go_quote(&c.to_string());
            format!("'{}'", &q[1..q.len() - 1])
        }
    }
}

/// The index of the first byte that breaks the number grammar starting at `s`.
fn bad_number_byte(b: &[u8], s: usize) -> usize {
    let mut p = s;
    let digits = |p: &mut usize| {
        let st = *p;
        while *p < b.len() && b[*p].is_ascii_digit() {
            *p += 1;
        }
        *p > st
    };
    if p < b.len() && b[p] == b'-' {
        p += 1;
    }
    if p < b.len() && b[p] == b'0' {
        p += 1;
    } else if !digits(&mut p) {
        return p;
    }
    if p < b.len() && b[p] == b'.' {
        p += 1;
        if !digits(&mut p) {
            return p;
        }
    }
    if p < b.len() && (b[p] == b'e' || b[p] == b'E') {
        p += 1;
        if p < b.len() && (b[p] == b'+' || b[p] == b'-') {
            p += 1;
        }
        if !digits(&mut p) {
            return p;
        }
    }
    p
}

/// Go's `*json.SyntaxError` text for a parse error of the `json` crate.
pub fn syntax_error(b: &[u8], e: &json::JsonError) -> String {
    let mut off = e.offset;
    let ctx = match e.msg {
        "invalid character looking for beginning of value" => "looking for beginning of value",
        "expected ':' after object key" => "after object key",
        "expected ',' or '}' in object" => "after object key:value pair",
        "expected ',' or ']' in array" => "after array element",
        "expected string" => "looking for beginning of object key string",
        "trailing data after JSON value" => "after top-level value",
        "invalid escape" | "invalid \\u escape" => "in string escape code",
        "invalid number" => {
            off = bad_number_byte(b, off);
            "in numeric literal"
        }
        "JSON nested too deep" => return "exceeded max depth".to_string(),
        _ => "in string literal",
    };
    if off >= b.len() {
        return "unexpected end of JSON input".to_string();
    }
    let c = std::str::from_utf8(&b[off..]).ok().and_then(|s| s.chars().next()).unwrap_or(b[off] as char);
    format!("invalid character {} {ctx}", quote_char(c))
}

/// The value of a Go pointer field `name` of object `o`: the last matching key; `None` when absent or null.
pub fn ptr<'a>(o: &'a Json, name: &str) -> Option<&'a Json> {
    let pairs = o.as_obj()?;
    let exact = pairs.iter().rev().find(|(k, _)| k == name);
    let v = match exact {
        Some((_, v)) => Some(v),
        None => pairs.iter().rev().find(|(k, _)| key_matches(k, name)).map(|(_, v)| v),
    }?;
    if v.is_null() { None } else { Some(v) }
}

/// The value of a Go value field `name`: like [`ptr`] (null leaves the zero value).
pub fn val<'a>(o: &'a Json, name: &str) -> Option<&'a Json> {
    ptr(o, name)
}

/// A `string` field ("" when absent).
pub fn str_of(o: &Json, name: &str) -> String {
    val(o, name).and_then(Json::as_str).unwrap_or("").to_string()
}

/// A `*string` field.
pub fn opt_str(o: &Json, name: &str) -> Option<String> {
    ptr(o, name).and_then(Json::as_str).map(str::to_string)
}

/// An integer field (0 when absent).
pub fn int_of(o: &Json, name: &str) -> i64 {
    val(o, name).and_then(Json::as_i64).unwrap_or(0)
}

/// A `bool` field (false when absent).
pub fn bool_of(o: &Json, name: &str) -> bool {
    val(o, name).and_then(Json::as_bool).unwrap_or(false)
}

/// A `[]string` field (empty when absent).
pub fn strings_of(o: &Json, name: &str) -> Vec<String> {
    val(o, name).and_then(Json::as_arr).map(|a| a.iter().map(|v| v.as_str().unwrap_or("").to_string()).collect()).unwrap_or_default()
}

/// Go `time.Parse(time.RFC3339, s)` (`strict`: with the extra checks of `time.Time.UnmarshalJSON`), with Go's
/// `*time.ParseError` texts. Returns the Unix seconds.
pub fn parse_rfc3339(s: &str, strict: bool) -> Result<i64, String> {
    const LAYOUT: &str = "2006-01-02T15:04:05Z07:00";
    let q = yaml::go_quote;
    let elem_err = |rest: &str, elem: &str| format!("parsing time {} as {}: cannot parse {} as {}", q(s), q(LAYOUT), q(rest), q(elem));
    let msg_err = |m: &str| format!("parsing time {}{m}", q(s));
    let b = s.as_bytes();
    let digit = |i: usize| i < b.len() && b[i].is_ascii_digit();
    if !(0..4).all(digit) {
        return Err(elem_err(s, "2006"));
    }
    let year: i64 = s[0..4].parse().unwrap_or(0);
    let mut p = 4usize;
    let num2 = |p: &mut usize, sep: &str, elem: &str| -> Result<i64, String> {
        if !s[*p..].starts_with(sep) {
            return Err(elem_err(&s[*p..], sep));
        }
        *p += sep.len();
        if !(digit(*p) && digit(*p + 1)) {
            return Err(elem_err(&s[*p..], elem));
        }
        let v = (b[*p] - b'0') as i64 * 10 + (b[*p + 1] - b'0') as i64;
        *p += 2;
        Ok(v)
    };
    let month = num2(&mut p, "-", "01")?;
    if !(1..=12).contains(&month) {
        return Err(msg_err(": month out of range"));
    }
    let day = num2(&mut p, "-", "02")?;
    if !s[p..].starts_with('T') {
        return Err(elem_err(&s[p..], "T"));
    }
    p += 1;
    if !digit(p) {
        return Err(elem_err(&s[p..], "15"));
    }
    // Go's "15" takes one or two digits; the JSON form wants two.
    let hour = if digit(p + 1) {
        p += 2;
        (b[p - 2] - b'0') as i64 * 10 + (b[p - 1] - b'0') as i64
    } else {
        if strict {
            return Err(elem_err(&s[p..p + 1], "15"));
        }
        p += 1;
        (b[p - 1] - b'0') as i64
    };
    if hour >= 24 {
        return Err(msg_err(": hour out of range"));
    }
    let minute = num2(&mut p, ":", "04")?;
    if minute >= 60 {
        return Err(msg_err(": minute out of range"));
    }
    let second = num2(&mut p, ":", "05")?;
    if second >= 60 {
        return Err(msg_err(": second out of range"));
    }
    if p + 1 < b.len() && (b[p] == b'.' || b[p] == b',') && digit(p + 1) {
        if strict && b[p] == b',' {
            return Err(elem_err(",", "."));
        }
        p += 1;
        while digit(p) {
            p += 1;
        }
    }
    let mut offset = 0i64;
    if s[p..].starts_with('Z') {
        p += 1;
    } else {
        let r = &b[p..];
        if r.len() < 6 || r[3] != b':' || (r[0] != b'+' && r[0] != b'-') || !r[1..3].iter().all(u8::is_ascii_digit) || !r[4..6].iter().all(u8::is_ascii_digit) {
            return Err(elem_err(&s[p..], "Z07:00"));
        }
        let hh = ((r[1] - b'0') * 10 + (r[2] - b'0')) as i64;
        let mm = ((r[4] - b'0') * 10 + (r[5] - b'0')) as i64;
        if strict && hh >= 24 {
            return Err(msg_err(": timezone hour out of range"));
        }
        if strict && mm >= 60 {
            return Err(msg_err(": timezone minute out of range"));
        }
        if hh > 24 {
            return Err(msg_err(": time zone offset hour out of range"));
        }
        if mm > 60 {
            return Err(msg_err(": time zone offset minute out of range"));
        }
        offset = (hh * 3600 + mm * 60) * if r[0] == b'-' { -1 } else { 1 };
        p += 6;
    }
    if p < b.len() {
        return Err(msg_err(&format!(": extra text: {}", q(&s[p..]))));
    }
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let dim = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][(month - 1) as usize];
    if day < 1 || day > dim {
        return Err(msg_err(": day out of range"));
    }
    // days from civil (Howard Hinnant's algorithm)
    let (y, m) = if month <= 2 { (year - 1, month + 9) } else { (year, month - 3) };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Ok((era * 146097 + doe - 719468) * 86400 + hour * 3600 + minute * 60 + second - offset)
}

/// Go `time.Time.UnmarshalJSON` as a check (for [`Ty::Unmarshaler`]).
pub fn check_time(v: &Json) -> Result<(), String> {
    match v {
        Json::Null => Ok(()),
        Json::Str(s) => parse_rfc3339(s, true).map(|_| ()),
        _ => Err("Time.UnmarshalJSON: input is not a JSON string".to_string()),
    }
}
