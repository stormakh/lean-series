//! Types that Prometheus configs share: model.Duration, units.Base2Bytes, config.Secret and config.URL.
use std::sync::atomic::{AtomicBool, Ordering};

use crate::decode::{Decoder, Error, Result, Unmarshal};
use crate::encode::Marshal;
use crate::node::{Kind, MAP_TAG, Node, SEQ_TAG};
use crate::url::GoUrl;
use crate::value::Value;

// ---------------- model.Duration ----------------

/// prometheus/common model.Duration, in nanoseconds (a Go time.Duration).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Duration(pub i64);

const MS: i64 = 1_000_000;

impl Duration {
    pub const fn from_millis(ms: i64) -> Duration {
        Duration(ms * MS)
    }
    pub const fn from_secs(s: i64) -> Duration {
        Duration(s * 1_000 * MS)
    }
    pub const fn millis(self) -> i64 {
        self.0 / MS
    }
    pub const fn nanos(self) -> i64 {
        self.0
    }
    /// As a std Duration (negative values give zero).
    pub fn to_std(self) -> std::time::Duration {
        std::time::Duration::from_nanos(self.0.max(0) as u64)
    }

    /// model.Duration.String: "0s", "1m", "1h30m", "90d"; years and weeks only when exact.
    pub fn go_string(&self) -> String {
        let mut ms = self.0 / MS;
        if ms == 0 {
            return "0s".to_string();
        }
        let mut sign = "";
        if ms < 0 {
            sign = "-";
            ms = -ms;
        }
        let mut r = String::new();
        let mut f = |unit: &str, mult: i64, exact: bool| {
            if exact && ms % mult != 0 {
                return;
            }
            let v = ms / mult;
            if v > 0 {
                r.push_str(&v.to_string());
                r.push_str(unit);
                ms -= v * mult;
            }
        };
        f("y", 1000 * 60 * 60 * 24 * 365, true);
        f("w", 1000 * 60 * 60 * 24 * 7, true);
        f("d", 1000 * 60 * 60 * 24, false);
        f("h", 1000 * 60 * 60, false);
        f("m", 1000 * 60, false);
        f("s", 1000, false);
        f("ms", 1, false);
        format!("{sign}{r}")
    }
}

impl std::fmt::Display for Duration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.go_string())
    }
}

/// model.ParseDuration: units y, w, d, h, m, s, ms from biggest to smallest; "0" without unit.
pub fn parse_duration(s: &str) -> std::result::Result<Duration, String> {
    match s {
        "0" => return Ok(Duration(0)),
        "" => return Err("empty duration string".to_string()),
        _ => {}
    }
    let orig = s;
    let invalid = || format!("not a valid duration string: {}", crate::decode::go_quote(orig));
    let b = s.as_bytes();
    let mut i = 0;
    let mut dur: u64 = 0;
    let mut last_pos = 0;
    while i < b.len() {
        if !b[i].is_ascii_digit() {
            return Err(invalid());
        }
        let st = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        let v: u64 = s[st..i].parse().map_err(|_| invalid())?;
        let ust = i;
        while i < b.len() && !b[i].is_ascii_digit() {
            i += 1;
        }
        if i == ust {
            return Err(invalid());
        }
        let u = &s[ust..i];
        let (pos, mult): (i32, u64) = match u {
            "ms" => (7, 1_000_000),
            "s" => (6, 1_000_000_000),
            "m" => (5, 60_000_000_000),
            "h" => (4, 3_600_000_000_000),
            "d" => (3, 86_400_000_000_000),
            "w" => (2, 604_800_000_000_000),
            "y" => (1, 31_536_000_000_000_000),
            _ => return Err(format!("unknown unit {} in duration {}", crate::decode::go_quote(u), crate::decode::go_quote(orig))),
        };
        if pos <= last_pos {
            return Err(invalid());
        }
        last_pos = pos;
        if v > (1u64 << 63) / mult {
            return Err("duration out of range".to_string());
        }
        dur += v * mult;
        if dur > (1u64 << 63) - 1 {
            return Err("duration out of range".to_string());
        }
    }
    Ok(Duration(dur as i64))
}

/// model.ParseDurationAllowNegative.
pub fn parse_duration_allow_negative(s: &str) -> std::result::Result<Duration, String> {
    match s.strip_prefix('-') {
        Some(rest) => parse_duration(rest).map(|d| Duration(-d.0)),
        None => parse_duration(s),
    }
}

impl Unmarshal for Duration {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> Result<bool> {
        d.custom(|d| {
            let mut s = String::new();
            d.inner(n, &mut s)?;
            *self = parse_duration(&s).map_err(Error::Msg)?;
            Ok(())
        })
    }
    fn go_type() -> String {
        "model.Duration".to_string()
    }
}
impl Marshal for Duration {
    fn marshal_yaml(&self) -> std::result::Result<Value, String> {
        Ok(Value::Str(self.go_string()))
    }
    fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

// ---------------- units.Base2Bytes ----------------

/// alecthomas/units Base2Bytes ("10MB", "1KiB").
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Base2Bytes(pub i64);

fn unit_value(u: &str, new_style: bool) -> Option<f64> {
    let s = if new_style { "iB" } else { "B" };
    let k = 1024f64;
    if u == "B" {
        return Some(1.0);
    }
    let p = u.strip_suffix(s)?;
    Some(match p {
        "K" => k,
        "M" => k * k,
        "G" => k * k * k,
        "T" => k * k * k * k,
        "P" => k * k * k * k * k,
        "E" => k * k * k * k * k * k,
        _ => return None,
    })
}

fn leading_int(s: &str) -> Option<(i64, &str)> {
    let b = s.as_bytes();
    let mut x: i64 = 0;
    let mut i = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        if x >= (i64::MAX - 10) / 10 {
            return None;
        }
        x = x * 10 + (b[i] - b'0') as i64;
        i += 1;
    }
    Some((x, &s[i..]))
}

fn parse_unit(s: &str, new_style: bool) -> std::result::Result<i64, String> {
    let orig = s;
    let mut s = s;
    let mut f = 0f64;
    let mut neg = false;
    if let Some(c) = s.as_bytes().first()
        && (*c == b'-' || *c == b'+') {
            neg = *c == b'-';
            s = &s[1..];
        }
    if s == "0" {
        return Ok(0);
    }
    if s.is_empty() {
        return Err(format!("units: invalid {orig}"));
    }
    while !s.is_empty() {
        let c = s.as_bytes()[0];
        if !(c == b'.' || c.is_ascii_digit()) {
            return Err(format!("units: invalid {orig}"));
        }
        let pl = s.len();
        let (x, rest) = leading_int(s).ok_or_else(|| format!("units: invalid {orig}"))?;
        s = rest;
        let mut g = x as f64;
        let pre = pl != s.len();
        let mut post = false;
        if s.starts_with('.') {
            s = &s[1..];
            let pl = s.len();
            let (x, rest) = leading_int(s).ok_or_else(|| format!("units: invalid {orig}"))?;
            s = rest;
            let mut scale = 1.0;
            for _ in 0..(pl - s.len()) {
                scale *= 10.0;
            }
            g += x as f64 / scale;
            post = pl != s.len();
        }
        if !pre && !post {
            return Err(format!("units: invalid {orig}"));
        }
        let i = s.bytes().position(|c| c == b'.' || c.is_ascii_digit()).unwrap_or(s.len());
        let u = &s[..i];
        s = &s[i..];
        let unit = unit_value(u, new_style).ok_or_else(|| format!("units: unknown unit {u} in {orig}"))?;
        f += g * unit;
    }
    if neg {
        f = -f;
    }
    if f < -(2f64.powi(63)) || f > 2f64.powi(63) - 1.0 {
        return Err("units: overflow parsing unit".to_string());
    }
    Ok(f as i64)
}

/// units.ParseBase2Bytes: "KiB" style first, then "KB" style (both base 1024).
pub fn parse_base2_bytes(s: &str) -> std::result::Result<Base2Bytes, String> {
    match parse_unit(s, true) {
        Ok(n) => Ok(Base2Bytes(n)),
        Err(_) => parse_unit(s, false).map(Base2Bytes),
    }
}

impl Base2Bytes {
    /// units.ToString(n, 1024, "iB", "B").
    pub fn go_string(&self) -> String {
        let units = ["", "K", "M", "G", "T", "P", "E"];
        let mut n = self.0;
        let mn = units.len();
        let mut out = vec![String::new(); mn];
        for (i, m) in units.iter().enumerate() {
            if n % 1024 != 0 || i == 0 && n == 0 {
                let s = if i == 0 { "B" } else { "iB" };
                out[mn - 1 - i] = format!("{}{}{}", n % 1024, m, s);
            }
            n /= 1024;
            if n == 0 {
                break;
            }
        }
        out.concat()
    }
}

impl Unmarshal for Base2Bytes {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> Result<bool> {
        match n.kind {
            // TextUnmarshaler: any scalar text goes to ParseBase2Bytes; its error stops decoding
            Kind::Scalar => {
                match parse_base2_bytes(&n.value) {
                    Ok(v) => *self = v,
                    Err(e) => return Err(Error::Msg(e)),
                }
                Ok(true)
            }
            Kind::Mapping => {
                d.terror(n, MAP_TAG, "units.Base2Bytes");
                Ok(false)
            }
            _ => {
                d.terror(n, SEQ_TAG, "units.Base2Bytes");
                Ok(false)
            }
        }
    }
    fn go_type() -> String {
        "units.Base2Bytes".to_string()
    }
}
impl Marshal for Base2Bytes {
    fn marshal_yaml(&self) -> std::result::Result<Value, String> {
        Ok(Value::Str(self.go_string()))
    }
    fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

// ---------------- config.Secret ----------------

/// When true, secrets marshal as their value (config.MarshalSecretValue).
pub static MARSHAL_SECRET_VALUE: AtomicBool = AtomicBool::new(false);

/// prometheus/common config.Secret: a string that marshals as "<secret>".
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct Secret(pub String);

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_empty() { "\"\"" } else { "\"<secret>\"" })
    }
}

impl Secret {
    pub fn new(s: impl Into<String>) -> Secret {
        Secret(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
impl From<&str> for Secret {
    fn from(s: &str) -> Self {
        Secret(s.to_string())
    }
}

/// Decodes a string-kind value with a custom Go type name in its type errors.
pub fn unmarshal_string_kind(out: &mut String, n: &Node, d: &mut Decoder, go_type: &str) -> Result<bool> {
    match n.kind {
        Kind::Scalar => {
            let (_, r) = d.scalar_tag(n)?;
            *out = match r {
                crate::resolve::Resolved::Str(s) => s,
                _ => n.value.clone(),
            };
            Ok(true)
        }
        Kind::Mapping => {
            d.terror(n, MAP_TAG, go_type);
            Ok(false)
        }
        _ => {
            d.terror(n, SEQ_TAG, go_type);
            Ok(false)
        }
    }
}

impl Unmarshal for Secret {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> Result<bool> {
        // type plain Secret; unmarshal((*plain)(s))
        unmarshal_string_kind(&mut self.0, n, d, "config.plain")
    }
    fn go_type() -> String {
        "config.Secret".to_string()
    }
}
impl Marshal for Secret {
    fn marshal_yaml(&self) -> std::result::Result<Value, String> {
        if MARSHAL_SECRET_VALUE.load(Ordering::Relaxed) {
            return Ok(Value::Str(self.0.clone()));
        }
        if !self.0.is_empty() {
            return Ok(Value::Str("<secret>".to_string()));
        }
        Ok(Value::Null)
    }
    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }
}

// ---------------- config.URL ----------------

/// prometheus/common config.URL: a Go *url.URL that may be nil.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Url(pub Option<GoUrl>);

impl Url {
    pub fn parse(s: &str) -> std::result::Result<Url, String> {
        crate::url::parse(s).map(|u| Url(Some(u)))
    }
    /// The URL with its password replaced by "xxxxx", "" for nil.
    pub fn redacted(&self) -> String {
        self.0.as_ref().map(|u| u.redacted()).unwrap_or_default()
    }
    /// Go `u.String()` ("" for nil).
    pub fn to_url_string(&self) -> String {
        self.0.as_ref().map(|u| u.to_url_string()).unwrap_or_default()
    }
    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }
}

impl Unmarshal for Url {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> Result<bool> {
        d.custom(|d| {
            let mut s = String::new();
            d.inner(n, &mut s)?;
            let u = crate::url::parse(&s).map_err(Error::Msg)?;
            self.0 = Some(u);
            Ok(())
        })
    }
    fn go_type() -> String {
        "config.URL".to_string()
    }
}
impl Marshal for Url {
    fn marshal_yaml(&self) -> std::result::Result<Value, String> {
        match &self.0 {
            Some(u) => Ok(Value::Str(u.redacted())),
            None => Ok(Value::Null),
        }
    }
    fn is_zero(&self) -> bool {
        self.0.is_none()
    }
}
