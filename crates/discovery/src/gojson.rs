//! Go `encoding/json` decoding into typed fields, for the SD providers that port Go API clients
//! (hetzner, digitalocean, scaleway), plus Go's `net.IP` text form.
//!
//! The pattern: parse the body with [`parse`] (Go's syntax error texts), then walk the value with a [`Dec`]:
//! each struct is a loop over the object's pairs in document order that matches keys case-insensitively
//! (Go's rule) and decodes the fields the port needs. Type mismatches are kept and decoding goes on (Go's
//! `saveError`); the first one is returned by [`Dec::finish`]. Errors of custom unmarshalers (`UnmarshalJSON`,
//! `UnmarshalText`) stop decoding at once, as in Go.
//!
//! Error texts follow Go: `json: cannot unmarshal number into Go struct field Server.servers.name of type
//! string`, where `Server` is the innermost struct being decoded and `servers.name` the path of JSON keys.
//!
//! Differs from Go: only the fields a port decodes are type-checked (Go also fails on a bad type in an unused
//! field); a number in an error is printed from its parsed value (`1e+40`, Go prints the literal `1e40`).
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use json::Json;

pub(crate) type Obj = [(String, Json)];

/// Go's `json.Unmarshal` (`decoder` false) or `json.NewDecoder(r).Decode` (`decoder` true: the first value
/// only, trailing data ignored, "EOF"/"unexpected EOF" for short input) syntax checking.
pub(crate) fn parse(body: &[u8], decoder: bool) -> Result<Json, String> {
    match Json::parse_bytes(body) {
        Ok(v) => Ok(v),
        Err(e) if e.msg == "trailing data after JSON value" && decoder => {
            Json::parse_bytes(&body[..e.offset]).map_err(|e| syntax_error(body, &e, decoder))
        }
        Err(e) => Err(syntax_error(body, &e, decoder)),
    }
}

/// Go `quoteChar`.
fn quote_char(c: char) -> String {
    match c {
        '\'' => "'\\''".to_string(),
        '"' => "'\"'".to_string(),
        _ => {
            let q = json::quote(&c.to_string());
            format!("'{}'", &q[1..q.len() - 1])
        }
    }
}

fn syntax_error(body: &[u8], e: &json::JsonError, decoder: bool) -> String {
    let eof = if decoder {
        if body.iter().all(|b| b.is_ascii_whitespace()) {
            "EOF"
        } else {
            "unexpected EOF"
        }
    } else {
        "unexpected end of JSON input"
    };
    let rest = &body[e.offset.min(body.len())..];
    let c = match String::from_utf8_lossy(rest).chars().next() {
        Some(c) => c,
        None => return eof.to_string(),
    };
    let what = match e.msg {
        "unexpected end of JSON input" => return eof.to_string(),
        "expected ':' after object key" => "after object key",
        "expected ',' or '}' in object" => "after object key:value pair",
        "expected ',' or ']' in array" => "after array element",
        "expected string" => "looking for beginning of object key string",
        "trailing data after JSON value" => "after top-level value",
        "invalid number" => "in numeric literal",
        "invalid escape" | "invalid \\u escape" => "in string escape code",
        "unterminated string" => return eof.to_string(),
        _ => "looking for beginning of value",
    };
    format!("invalid character {} {what}", quote_char(c))
}

/// Go's name of a JSON value kind in type errors.
pub(crate) fn kind(v: &Json) -> &'static str {
    model::labelset::json_kind(v)
}

/// The value text Go puts in a numeric range error ("number 1.5").
fn number_text(v: &Json) -> String {
    match v {
        Json::Int(n) => n.to_string(),
        Json::Float(f) => {
            let mut s = String::new();
            json::write_f64(*f, &mut s);
            s
        }
        _ => String::new(),
    }
}

/// `path + "." + key` (the JSON key path in errors).
pub(crate) fn sub(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_string()
    } else {
        format!("{path}.{key}")
    }
}

/// Go's `UnmarshalTypeError` text.
pub(crate) fn type_error(value: &str, st: &str, path: &str, ty: &str) -> String {
    if st.is_empty() && path.is_empty() {
        format!("json: cannot unmarshal {value} into Go value of type {ty}")
    } else {
        format!("json: cannot unmarshal {value} into Go struct field {st}.{path} of type {ty}")
    }
}

/// A decode in progress: keeps the first type error (Go `d.savedError`).
#[derive(Default)]
pub(crate) struct Dec {
    saved: Option<String>,
}

impl Dec {
    pub(crate) fn new() -> Dec {
        Dec::default()
    }

    /// Records a type error (only the first is kept).
    pub(crate) fn mismatch(&mut self, v: &Json, st: &str, path: &str, ty: &str) {
        if self.saved.is_none() {
            self.saved = Some(type_error(kind(v), st, path, ty));
        }
    }

    /// The result of the decode: the first type error, if any.
    pub(crate) fn finish(self) -> Result<(), String> {
        match self.saved {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// An object for a struct of Go type `ty` (or a pointer to it): `None` for null (a nil pointer; a value
    /// struct stays zero) and for a type error.
    pub(crate) fn obj<'a>(
        &mut self,
        v: &'a Json,
        st: &str,
        path: &str,
        ty: &str,
    ) -> Option<&'a Obj> {
        match v {
            Json::Obj(o) => Some(o),
            Json::Null => None,
            _ => {
                self.mismatch(v, st, path, ty);
                None
            }
        }
    }

    /// The elements of a slice of Go type `ty`: `Some(&[])` for null (a nil slice), `None` for a type error
    /// (the slice keeps its value).
    pub(crate) fn arr<'a>(
        &mut self,
        v: &'a Json,
        st: &str,
        path: &str,
        ty: &str,
    ) -> Option<&'a [Json]> {
        match v {
            Json::Arr(a) => Some(a),
            Json::Null => Some(&[]),
            _ => {
                self.mismatch(v, st, path, ty);
                None
            }
        }
    }

    /// A Go `string` field (null leaves it unchanged).
    pub(crate) fn str(&mut self, v: &Json, dst: &mut String, st: &str, path: &str) {
        match v {
            Json::Str(s) => *dst = s.clone(),
            Json::Null => {}
            _ => self.mismatch(v, st, path, "string"),
        }
    }

    /// A Go `*string` field (null is nil).
    pub(crate) fn opt_str(&mut self, v: &Json, dst: &mut Option<String>, st: &str, path: &str) {
        match v {
            Json::Str(s) => *dst = Some(s.clone()),
            Json::Null => *dst = None,
            _ => self.mismatch(v, st, path, "string"),
        }
    }

    /// A Go `bool` field.
    pub(crate) fn bool(&mut self, v: &Json, dst: &mut bool, st: &str, path: &str) {
        match v {
            Json::Bool(b) => *dst = *b,
            Json::Null => {}
            _ => self.mismatch(v, st, path, "bool"),
        }
    }

    /// A Go integer field of type `ty` ("int", "int64", "int32", "uint32", "uint64").
    pub(crate) fn int(&mut self, v: &Json, dst: &mut i64, st: &str, path: &str, ty: &str) {
        let (lo, hi): (i128, i128) = match ty {
            "int32" => (i32::MIN as i128, i32::MAX as i128),
            "uint32" => (0, u32::MAX as i128),
            "uint64" => (0, u64::MAX as i128),
            _ => (i64::MIN as i128, i64::MAX as i128),
        };
        match v {
            Json::Int(n) if (*n as i128) >= lo && (*n as i128) <= hi => *dst = *n,
            Json::Int(_) | Json::Float(_) => {
                if self.saved.is_none() {
                    self.saved = Some(type_error(
                        &format!("number {}", number_text(v)),
                        st,
                        path,
                        ty,
                    ));
                }
            }
            Json::Null => {}
            _ => self.mismatch(v, st, path, ty),
        }
    }

    /// A Go `float32` field.
    pub(crate) fn f32(&mut self, v: &Json, dst: &mut f32, st: &str, path: &str) {
        match v {
            Json::Int(_) | Json::Float(_) => {
                let f = v.as_f64().unwrap_or(0.0);
                if f.abs() > f32::MAX as f64 {
                    if self.saved.is_none() {
                        self.saved = Some(type_error(
                            &format!("number {}", number_text(v)),
                            st,
                            path,
                            "float32",
                        ));
                    }
                } else {
                    *dst = f as f32;
                }
            }
            Json::Null => {}
            _ => self.mismatch(v, st, path, "float32"),
        }
    }

    /// A Go `[]string` field (null is nil; a bad element is a type error and becomes "").
    pub(crate) fn str_slice(&mut self, v: &Json, dst: &mut Vec<String>, st: &str, path: &str) {
        if let Some(a) = self.arr(v, st, path, "[]string") {
            let mut out = Vec::with_capacity(a.len());
            for e in a {
                let mut s = String::new();
                self.str(e, &mut s, st, path);
                out.push(s);
            }
            *dst = out;
        }
    }

    /// A Go `map[string]string` field (null is nil; entries are added to the map, as Go reuses it).
    pub(crate) fn str_map(
        &mut self,
        v: &Json,
        dst: &mut Vec<(String, String)>,
        st: &str,
        path: &str,
    ) {
        match v {
            Json::Obj(o) => {
                for (k, e) in o {
                    let mut s = String::new();
                    self.str(e, &mut s, st, path);
                    match dst.iter_mut().find(|(n, _)| n == k) {
                        Some(x) => x.1 = s,
                        None => dst.push((k.clone(), s)),
                    }
                }
            }
            Json::Null => dst.clear(),
            _ => self.mismatch(v, st, path, "map[string]string"),
        }
    }

    /// A Go `net.IP` field (`UnmarshalText`): null and "" are nil; a bad address stops decoding with
    /// "invalid IP address: <text>".
    pub(crate) fn ip(
        &mut self,
        v: &Json,
        dst: &mut Option<IpAddr>,
        st: &str,
        path: &str,
    ) -> Result<(), String> {
        match v {
            Json::Str(s) if s.is_empty() => *dst = None,
            Json::Str(s) => match parse_ip(s) {
                Some(ip) => *dst = Some(ip),
                None => return Err(format!("invalid IP address: {s}")),
            },
            Json::Null => *dst = None,
            _ => self.mismatch(v, st, path, "net.IP"),
        }
        Ok(())
    }
}

/// `json.Unmarshal(data, &s)` into a plain string, as custom `UnmarshalJSON` methods do: null is "", another
/// kind is an error without field context.
pub(crate) fn custom_string(v: &Json) -> Result<String, String> {
    match v {
        Json::Str(s) => Ok(s.clone()),
        Json::Null => Ok(String::new()),
        _ => Err(type_error(kind(v), "", "", "string")),
    }
}

/// Go `strings.EqualFold` for the ASCII field names used here (Go's key matching).
pub(crate) fn key_is(k: &str, name: &str) -> bool {
    k.eq_ignore_ascii_case(name)
}

/// Go `net.ParseIP`: dotted IPv4 or IPv6 text (no zone); `None` when invalid.
pub(crate) fn parse_ip(s: &str) -> Option<IpAddr> {
    if s.contains('%') {
        return None;
    }
    if s.contains(':') {
        s.parse::<Ipv6Addr>().ok().map(IpAddr::V6)
    } else {
        s.parse::<Ipv4Addr>().ok().map(IpAddr::V4)
    }
}

/// Go `IP.To4() != nil`.
pub(crate) fn is_ipv4(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(_) => true,
        IpAddr::V6(v6) => v6.to_ipv4_mapped().is_some(),
    }
}

/// Go `IP.String()` ("<nil>" for a nil IP; IPv4-mapped IPv6 prints as IPv4).
pub(crate) fn ip_string(ip: &Option<IpAddr>) -> String {
    match ip {
        None => "<nil>".to_string(),
        Some(IpAddr::V4(v4)) => v4.to_string(),
        Some(IpAddr::V6(v6)) => match v6.to_ipv4_mapped() {
            Some(v4) => v4.to_string(),
            None => v6.to_string(),
        },
    }
}

/// Go `net.ParseCIDR`: the address and the network (masked address, prefix length), or Go's error.
pub(crate) fn parse_cidr(s: &str) -> Result<(IpAddr, IpAddr, u8), String> {
    let bad = || format!("invalid CIDR address: {s}");
    let (addr, bits) = s.split_once('/').ok_or_else(bad)?;
    let ip = parse_ip(addr).ok_or_else(bad)?;
    if bits.is_empty()
        || bits.len() > 3
        || !bits.bytes().all(|b| b.is_ascii_digit())
        || (bits.len() > 1 && bits.starts_with('0'))
    {
        return Err(bad());
    }
    let n: u32 = bits.parse().map_err(|_| bad())?;
    // Go treats a dotted address as IPv4 (32 bits) and anything with ':' as IPv6 (128 bits).
    let v4 = !addr.contains(':');
    let max = if v4 { 32 } else { 128 };
    if n > max {
        return Err(bad());
    }
    let net = match ip {
        IpAddr::V4(a) if v4 => {
            let m = if n == 0 { 0 } else { u32::MAX << (32 - n) };
            IpAddr::V4(Ipv4Addr::from(u32::from(a) & m))
        }
        IpAddr::V4(a) => {
            let m = if n == 0 { 0 } else { u128::MAX << (128 - n) };
            IpAddr::V6(Ipv6Addr::from(u128::from(a.to_ipv6_mapped()) & m))
        }
        IpAddr::V6(a) => {
            let m = if n == 0 { 0 } else { u128::MAX << (128 - n) };
            IpAddr::V6(Ipv6Addr::from(u128::from(a) & m))
        }
    };
    Ok((ip, net, n as u8))
}

/// Go `IPNet.String()` of a parsed CIDR network ("<nil>" for the zero IPNet).
pub(crate) fn ipnet_string(net: &Option<(IpAddr, u8)>) -> String {
    match net {
        None => "<nil>".to_string(),
        Some((ip, n)) => match ip {
            IpAddr::V6(v6) if v6.to_ipv4_mapped().is_some() && *n >= 96 => {
                format!("{}/{}", ip_string(&Some(*ip)), n - 96)
            }
            _ => format!("{}/{}", ip_string(&Some(*ip)), n),
        },
    }
}

/// Go `strconv.Atoi` error text: `strconv.Atoi: parsing "x": invalid syntax` (or `value out of range`).
pub(crate) fn atoi(s: &str) -> Result<i64, String> {
    parse_int(s, "Atoi", 64)
}

/// Go `strconv.ParseInt(s, 10, bits)` with its error text (`fn_name` is "Atoi" or "ParseInt").
pub(crate) fn parse_int(s: &str, fn_name: &str, bits: u32) -> Result<i64, String> {
    let q = json::quote(s);
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("strconv.{fn_name}: parsing {q}: invalid syntax"));
    }
    let v: i128 = s.parse().unwrap_or(if s.starts_with('-') {
        i128::MIN
    } else {
        i128::MAX
    });
    let lim = 1i128 << (bits - 1);
    if v < -lim || v >= lim {
        return Err(format!(
            "strconv.{fn_name}: parsing {q}: value out of range"
        ));
    }
    Ok(v as i64)
}
