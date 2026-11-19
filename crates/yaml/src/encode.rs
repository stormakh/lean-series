//! Rust values to YAML text as go-yaml v2 `Marshal` writes them (encode.go): struct fields in declaration
//! order, Go maps sorted with go-yaml's key order, strings quoted only when a plain scalar would read back as
//! something else, multi-line strings as literal blocks.
use std::collections::BTreeMap;

use crate::decode::{MapKey, Struct};
use crate::emitter::{Emitter, Ev, Style};
use crate::resolve::plain_is_str;
use crate::value::{Value, sort_pairs};

pub trait Marshal {
    /// The value to write (go-yaml calls `MarshalYAML` first, then walks the result).
    fn marshal_yaml(&self) -> Result<Value, String>;
    /// Go `isZero`, for `omitempty`.
    fn is_zero(&self) -> bool;
}

/// Go `strconv.FormatFloat(f, 'g', -1, 64)`.
pub fn format_float_g(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "+Inf".to_string() } else { "-Inf".to_string() };
    }
    if f == 0.0 {
        return if f.is_sign_negative() { "-0".to_string() } else { "0".to_string() };
    }
    let neg = f < 0.0;
    let e = format!("{:e}", f.abs());
    let (mant, exp) = e.split_once('e').unwrap_or((&e, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let digits: String = mant.chars().filter(|c| *c != '.').collect();
    let nd = digits.len() as i32;
    let mut s = String::new();
    if neg {
        s.push('-');
    }
    // shortest 'g': %e when the exponent is < -4 or >= 6 (strconv uses eprec = 6 for the shortest form)
    if !(-4..6).contains(&exp) {
        exp_form(&mut s, &digits, exp);
        return s;
    }
    // %f with max(nd - dp, 0) decimals
    let dp = exp + 1;
    if dp <= 0 {
        s.push_str("0.");
        for _ in 0..(-dp) {
            s.push('0');
        }
        s.push_str(&digits);
    } else if dp >= nd {
        s.push_str(&digits);
        for _ in 0..(dp - nd) {
            s.push('0');
        }
    } else {
        s.push_str(&digits[..dp as usize]);
        s.push('.');
        s.push_str(&digits[dp as usize..]);
    }
    s
}

fn exp_form(s: &mut String, digits: &str, exp: i32) {
    s.push_str(&digits[..1]);
    if digits.len() > 1 {
        s.push('.');
        s.push_str(&digits[1..]);
    }
    s.push('e');
    s.push(if exp < 0 { '-' } else { '+' });
    let a = exp.unsigned_abs();
    if a < 10 {
        s.push('0');
    }
    s.push_str(&a.to_string());
}

fn to_ev(v: &Value) -> Ev {
    match v {
        Value::Null => Ev::Scalar("null".to_string(), Style::Plain),
        Value::Bool(b) => Ev::Scalar(if *b { "true" } else { "false" }.to_string(), Style::Plain),
        Value::Int(i) => Ev::Scalar(i.to_string(), Style::Plain),
        Value::Uint(u) => Ev::Scalar(u.to_string(), Style::Plain),
        Value::Float(f) => {
            let s = format_float_g(*f);
            let s = match s.as_str() {
                "+Inf" => ".inf".to_string(),
                "-Inf" => "-.inf".to_string(),
                "NaN" => ".nan".to_string(),
                _ => s,
            };
            Ev::Scalar(s, Style::Plain)
        }
        Value::Str(s) => {
            let style = if s.contains('\n') {
                Style::Literal
            } else if plain_is_str(s) {
                Style::Plain
            } else {
                Style::DoubleQuoted
            };
            Ev::Scalar(s.clone(), style)
        }
        Value::Seq(items) => Ev::Seq(items.iter().map(to_ev).collect()),
        Value::FlowSeq(items) => Ev::FlowSeq(items.iter().map(to_ev).collect()),
        Value::Map(pairs) => Ev::Map(pairs.iter().map(|(k, v)| (to_ev(k), to_ev(v))).collect()),
    }
}

/// Writes a value tree as one YAML document.
pub fn emit(v: &Value) -> String {
    Emitter::new().document(&to_ev(v))
}

/// go-yaml `Marshal`.
pub fn marshal<T: Marshal + ?Sized>(v: &T) -> Result<String, String> {
    Ok(emit(&v.marshal_yaml()?))
}

/// Marshals a `Struct` as a mapping of its fields (the default for structs without `MarshalYAML`).
pub fn marshal_struct<T: Struct + ?Sized>(v: &T) -> Result<Value, String> {
    let mut out = Vec::new();
    v.marshal_fields(&mut out)?;
    Ok(Value::Map(out))
}

/// Appends one struct field (used by `yaml_struct!`).
pub fn push_field<T: Marshal + ?Sized>(out: &mut Vec<(Value, Value)>, key: &str, v: &T, omitempty: bool) -> Result<(), String> {
    if omitempty && v.is_zero() {
        return Ok(());
    }
    out.push((Value::Str(key.to_string()), v.marshal_yaml()?));
    Ok(())
}

impl Marshal for Value {
    fn marshal_yaml(&self) -> Result<Value, String> {
        Ok(self.clone())
    }
    fn is_zero(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl Marshal for String {
    fn marshal_yaml(&self) -> Result<Value, String> {
        Ok(Value::Str(self.clone()))
    }
    fn is_zero(&self) -> bool {
        self.is_empty()
    }
}
impl Marshal for str {
    fn marshal_yaml(&self) -> Result<Value, String> {
        Ok(Value::Str(self.to_string()))
    }
    fn is_zero(&self) -> bool {
        self.is_empty()
    }
}
impl Marshal for bool {
    fn marshal_yaml(&self) -> Result<Value, String> {
        Ok(Value::Bool(*self))
    }
    fn is_zero(&self) -> bool {
        !*self
    }
}
macro_rules! num_marshal {
    ($($t:ty => $variant:ident as $cast:ty),*) => {$(
        impl Marshal for $t {
            fn marshal_yaml(&self) -> Result<Value, String> {
                Ok(Value::$variant(*self as $cast))
            }
            fn is_zero(&self) -> bool {
                *self == 0 as $t
            }
        }
    )*};
}
num_marshal!(i64 => Int as i64, i32 => Int as i64, u64 => Uint as u64, u32 => Uint as u64, usize => Uint as u64, f64 => Float as f64);

impl<T: Marshal> Marshal for Vec<T> {
    fn marshal_yaml(&self) -> Result<Value, String> {
        let mut out = Vec::with_capacity(self.len());
        for v in self {
            out.push(v.marshal_yaml()?);
        }
        Ok(Value::Seq(out))
    }
    fn is_zero(&self) -> bool {
        self.is_empty()
    }
}
impl<T: Marshal> Marshal for Option<T> {
    fn marshal_yaml(&self) -> Result<Value, String> {
        match self {
            Some(v) => v.marshal_yaml(),
            None => Ok(Value::Null),
        }
    }
    fn is_zero(&self) -> bool {
        self.is_none()
    }
}
impl<T: Marshal + ?Sized> Marshal for Box<T> {
    fn marshal_yaml(&self) -> Result<Value, String> {
        (**self).marshal_yaml()
    }
    fn is_zero(&self) -> bool {
        (**self).is_zero()
    }
}
impl<T: Marshal + ?Sized> Marshal for &T {
    fn marshal_yaml(&self) -> Result<Value, String> {
        (**self).marshal_yaml()
    }
    fn is_zero(&self) -> bool {
        (**self).is_zero()
    }
}
impl<K: MapKey + Marshal, V: Marshal> Marshal for BTreeMap<K, V> {
    fn marshal_yaml(&self) -> Result<Value, String> {
        let mut out = Vec::with_capacity(self.len());
        for (k, v) in self {
            out.push((k.marshal_yaml()?, v.marshal_yaml()?));
        }
        sort_pairs(&mut out);
        Ok(Value::Map(out))
    }
    fn is_zero(&self) -> bool {
        self.is_empty()
    }
}
