//! Node tree into Rust values with go-yaml v2 semantics (decode.go): type errors are collected and decoding goes
//! on ("yaml: unmarshal errors:\n  line N: ..."); errors returned by custom unmarshalers stop everything; strict
//! mode reports unknown fields, fields set twice and duplicate map keys.
//!
//! Rule for implementors: `Default` must be the Go zero value (a YAML null resets a value to it). Go defaults
//! that `UnmarshalYAML` sets (`*c = DefaultX`) belong in the type's `Unmarshal` impl.
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::node::*;
use crate::resolve::{Resolved, resolve, short_tag};
use crate::value::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// Type errors ("line 3: cannot unmarshal ..."), only bubbling out of `Decoder::inner` and the top level.
    Type(Vec<String>),
    /// Anything else, printed as is (a parse error or a custom unmarshaler's error).
    Msg(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Type(v) => write!(f, "yaml: unmarshal errors:\n  {}", v.join("\n  ")),
            Error::Msg(m) => f.write_str(m),
        }
    }
}
impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Msg(s)
    }
}
impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::Msg(s.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// A value that YAML can be decoded into. `unmarshal_yaml` is never called with a null node (the decoder
/// resets the value to `Default` instead, as go-yaml does) nor with an alias (it is followed first).
/// It returns Ok(false) when it recorded a type error for this node (the item is then dropped from a list).
pub trait Unmarshal {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> Result<bool>;
    /// Go type name used in "cannot unmarshal !!str `x` into <type>".
    fn go_type() -> String
    where
        Self: Sized;
}

/// A struct field, as an object so that a `Struct` can hand out its fields by key.
pub trait Field {
    fn decode_field(&mut self, n: &Node, d: &mut Decoder) -> Result<bool>;
}
impl<T: Unmarshal + Default> Field for T {
    fn decode_field(&mut self, n: &Node, d: &mut Decoder) -> Result<bool> {
        d.unmarshal(n, self)
    }
}

/// A Go struct: fields by YAML key (inline structs merged in), decoded and encoded in declaration order.
/// Usually written with the `yaml_struct!` macro.
pub trait Struct {
    /// Go type name in error messages, for example "config.plain" for a type decoded through the `plain` trick.
    fn type_name(&self) -> &'static str;
    fn has_field(&self, key: &str) -> bool;
    fn field(&mut self, key: &str) -> Option<&mut dyn Field>;
    fn marshal_fields(&self, out: &mut Vec<(Value, Value)>) -> std::result::Result<(), String>;
    /// Go `isZero` for the struct: every field is zero.
    fn fields_zero(&self) -> bool;
}

pub struct Decoder {
    pub strict: bool,
    terrors: Vec<String>,
    aliases: Vec<*const Node>,
    decode_count: usize,
    alias_count: usize,
    alias_depth: usize,
}

fn allowed_alias_ratio(decode_count: usize) -> f64 {
    const LOW: usize = 400_000;
    const HIGH: usize = 4_000_000;
    if decode_count <= LOW {
        0.99
    } else if decode_count >= HIGH {
        0.10
    } else {
        0.99 - 0.89 * ((decode_count - LOW) as f64 / (HIGH - LOW) as f64)
    }
}

/// Go `%#v` of a string: a double-quoted Go string literal.
pub fn go_quote(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('"');
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\t' => o.push_str("\\t"),
            '\x07' => o.push_str("\\a"),
            '\x08' => o.push_str("\\b"),
            '\x0c' => o.push_str("\\f"),
            '\x0b' => o.push_str("\\v"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => o.push_str(&format!("\\x{:02x}", c as u32)),
            c if !c.is_control() => o.push(c),
            c if (c as u32) < 0x10000 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push_str(&format!("\\U{:08x}", c as u32)),
        }
    }
    o.push('"');
    o
}

impl Decoder {
    pub fn new(strict: bool) -> Decoder {
        Decoder { strict, terrors: Vec::new(), aliases: Vec::new(), decode_count: 0, alias_count: 0, alias_depth: 0 }
    }

    /// Records a type error, as go-yaml `d.terror`.
    pub fn terror(&mut self, n: &Node, tag: &str, go_type: &str) {
        let tag = if !n.tag.is_empty() { n.tag.as_str() } else { tag };
        let mut value = String::new();
        if tag != SEQ_TAG && tag != MAP_TAG {
            if n.value.len() > 10 {
                value = format!(" `{}...`", String::from_utf8_lossy(&n.value.as_bytes()[..7]));
            } else {
                value = format!(" `{}`", n.value);
            }
        }
        self.terrors.push(format!("line {}: cannot unmarshal {}{} into {}", n.line, short_tag(tag), value, go_type));
    }

    /// Records a free-form type error ("line N: ...").
    pub fn push_type_error(&mut self, msg: String) {
        self.terrors.push(msg);
    }

    /// go-yaml `d.unmarshal`: follows aliases, resets the value on null, else calls its `Unmarshal`.
    pub fn unmarshal<T: Unmarshal + Default>(&mut self, n: &Node, out: &mut T) -> Result<bool> {
        self.count()?;
        if n.kind == Kind::Alias {
            return self.alias(n, |d, target| d.unmarshal(target, out));
        }
        // go-yaml skips custom unmarshalers for null-looking nodes and zeroes the value when the scalar resolves
        // to null; a quoted "null" or "~" is a string (passed on: types decoding it see the text)
        if n.is_null() && n.kind == Kind::Scalar && matches!(self.scalar_tag(n)?.1, Resolved::Null) {
            *out = T::default();
            return Ok(true);
        }
        out.unmarshal_yaml(n, self)
    }

    fn count(&mut self) -> Result<()> {
        self.decode_count += 1;
        if self.alias_depth > 0 {
            self.alias_count += 1;
        }
        if self.alias_count > 100 && self.decode_count > 1000 && self.alias_count as f64 / self.decode_count as f64 > allowed_alias_ratio(self.decode_count) {
            return Err(Error::Msg("yaml: document contains excessive aliasing".to_string()));
        }
        Ok(())
    }

    fn alias<R>(&mut self, n: &Node, f: impl FnOnce(&mut Decoder, &Node) -> Result<R>) -> Result<R> {
        let key = n as *const Node;
        if self.aliases.contains(&key) {
            return Err(Error::Msg(format!("yaml: anchor '{}' value contains itself", n.value)));
        }
        let target = n.alias.clone().ok_or_else(|| Error::Msg("yaml: unknown anchor referenced".to_string()))?;
        self.aliases.push(key);
        self.alias_depth += 1;
        let r = f(self, &target);
        self.alias_depth -= 1;
        self.aliases.retain(|a| *a != key);
        r
    }

    /// The `unmarshal` callback a Go `UnmarshalYAML` receives: decodes `n` into `out`, and returns the type errors
    /// found there as `Error::Type` (removed from the decoder, so the caller decides).
    pub fn inner<T: Unmarshal + Default>(&mut self, n: &Node, out: &mut T) -> Result<()> {
        let before = self.terrors.len();
        self.unmarshal(n, out)?;
        if self.terrors.len() > before {
            return Err(Error::Type(self.terrors.split_off(before)));
        }
        Ok(())
    }

    /// `unmarshal((*plain)(c))`: decodes `n` as the bare struct, without the type's own `Unmarshal`.
    pub fn inner_struct<T: Struct>(&mut self, n: &Node, out: &mut T) -> Result<()> {
        let before = self.terrors.len();
        self.decode_struct(n, out)?;
        if self.terrors.len() > before {
            return Err(Error::Type(self.terrors.split_off(before)));
        }
        Ok(())
    }

    /// Wraps a custom unmarshaler (go-yaml `callUnmarshaler`): type errors it returns are kept and decoding goes
    /// on (Ok(false)); any other error stops the whole decode.
    pub fn custom(&mut self, f: impl FnOnce(&mut Decoder) -> Result<()>) -> Result<bool> {
        match f(self) {
            Ok(()) => Ok(true),
            Err(Error::Type(v)) => {
                self.terrors.extend(v);
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    /// Decodes a mapping (or merge, alias) into a struct; a scalar or sequence is a type error.
    pub fn decode_struct<T: Struct>(&mut self, n: &Node, out: &mut T) -> Result<bool> {
        self.count()?;
        match n.kind {
            Kind::Alias => self.alias(n, |d, t| d.decode_struct(t, out)),
            Kind::Mapping => self.mapping_struct(n, out),
            Kind::Sequence => {
                self.terror(n, SEQ_TAG, out.type_name());
                Ok(false)
            }
            Kind::Scalar => {
                if n.is_null() {
                    // only reachable through `inner_struct`; go-yaml zeroes the value, which callers do not need
                    return Ok(true);
                }
                let (tag, _) = self.scalar_tag(n)?;
                self.terror(n, &tag, out.type_name());
                Ok(false)
            }
        }
    }

    fn mapping_struct<T: Struct>(&mut self, n: &Node, out: &mut T) -> Result<bool> {
        let mut done: Vec<String> = Vec::new();
        for (k, v) in n.pairs() {
            if k.is_merge() {
                self.merge(v, &mut |d, m| d.decode_struct(m, out).map(|_| ()))?;
                continue;
            }
            let mut name = String::new();
            if !self.unmarshal(k, &mut name)? {
                continue;
            }
            if out.has_field(&name) {
                if self.strict {
                    if done.contains(&name) {
                        self.terrors.push(format!("line {}: field {} already set in type {}", k.line, name, out.type_name()));
                        continue;
                    }
                    done.push(name.clone());
                }
                if let Some(f) = out.field(&name) {
                    f.decode_field(v, self)?;
                }
            } else if self.strict {
                self.terrors.push(format!("line {}: field {} not found in type {}", k.line, name, out.type_name()));
            }
        }
        Ok(true)
    }

    /// go-yaml `d.merge`: a mapping, an alias to one, or a list of them (later items lose).
    pub fn merge(&mut self, n: &Node, f: &mut dyn FnMut(&mut Decoder, &Node) -> Result<()>) -> Result<()> {
        let want_map = || Error::Msg("yaml: map merge requires map or sequence of maps as the value".to_string());
        let is_map_alias = |c: &Node| c.alias.as_ref().map(|a| a.kind == Kind::Mapping).unwrap_or(false);
        match n.kind {
            Kind::Mapping => f(self, n),
            Kind::Alias => {
                if !is_map_alias(n) {
                    return Err(want_map());
                }
                f(self, n)
            }
            Kind::Sequence => {
                for c in n.children.iter().rev() {
                    if c.kind == Kind::Alias {
                        if !is_map_alias(c) {
                            return Err(want_map());
                        }
                    } else if c.kind != Kind::Mapping {
                        return Err(want_map());
                    }
                    f(self, c)?;
                }
                Ok(())
            }
            Kind::Scalar => Err(want_map()),
        }
    }

    /// The tag and resolved value of a scalar, as go-yaml `d.scalar` computes them.
    pub fn scalar_tag(&mut self, n: &Node) -> Result<(String, Resolved)> {
        if n.tag.is_empty() && !n.implicit {
            return Ok((STR_TAG.to_string(), Resolved::Str(n.value.clone())));
        }
        let (tag, r) = resolve(&n.tag, &n.value).map_err(|e| Error::Msg(format!("yaml: {e}")))?;
        if tag == BINARY_TAG {
            let data = base64_decode(&n.value).ok_or_else(|| Error::Msg("yaml: !!binary value contains invalid base64 data".to_string()))?;
            return Ok((tag, Resolved::Str(String::from_utf8_lossy(&data).into_owned())));
        }
        Ok((tag, r))
    }

    /// Decodes a scalar node into a number-like value; `conv` maps the resolved value or returns None for a
    /// type error.
    fn scalar_into<T>(&mut self, n: &Node, go_type: &str, out: &mut T, conv: impl FnOnce(&Resolved) -> Option<T>) -> Result<bool> {
        match n.kind {
            Kind::Scalar => {
                let (tag, r) = self.scalar_tag(n)?;
                match conv(&r) {
                    Some(v) => {
                        *out = v;
                        Ok(true)
                    }
                    None => {
                        self.terror(n, &tag, go_type);
                        Ok(false)
                    }
                }
            }
            Kind::Mapping => {
                self.terror(n, MAP_TAG, go_type);
                Ok(false)
            }
            Kind::Sequence => {
                self.terror(n, SEQ_TAG, go_type);
                Ok(false)
            }
            Kind::Alias => unreachable!("aliases are followed before"),
        }
    }

    pub fn into_result(self) -> Result<()> {
        if self.terrors.is_empty() { Ok(()) } else { Err(Error::Type(self.terrors)) }
    }
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut buf = 0u32;
    let mut bits = 0;
    for c in s.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' | b'\n' | b'\r' | b' ' => continue,
            _ => return None,
        };
        buf = buf << 6 | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Some(out)
}

// ---------------- primitive impls ----------------

impl Unmarshal for String {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> Result<bool> {
        let raw = n.value.clone();
        d.scalar_into(n, "string", self, |r| match r {
            Resolved::Null => Some(String::new()),
            Resolved::Str(s) => Some(s.clone()),
            _ => Some(raw),
        })
    }
    fn go_type() -> String {
        "string".to_string()
    }
}

impl Unmarshal for bool {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> Result<bool> {
        d.scalar_into(n, "bool", self, |r| match r {
            Resolved::Bool(b) => Some(*b),
            _ => None,
        })
    }
    fn go_type() -> String {
        "bool".to_string()
    }
}

macro_rules! int_impl {
    ($t:ty, $name:expr) => {
        impl Unmarshal for $t {
            fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> Result<bool> {
                d.scalar_into(n, $name, self, |r| match *r {
                    Resolved::Int(i) => <$t>::try_from(i).ok(),
                    Resolved::Uint(u) => {
                        if u <= i64::MAX as u64 {
                            <$t>::try_from(u).ok()
                        } else {
                            None
                        }
                    }
                    Resolved::Float(f) => {
                        if f <= i64::MAX as f64 {
                            <$t>::try_from(f as i64).ok()
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
            }
            fn go_type() -> String {
                $name.to_string()
            }
        }
    };
}
int_impl!(i64, "int");
int_impl!(i32, "int32");

macro_rules! uint_impl {
    ($t:ty, $name:expr) => {
        impl Unmarshal for $t {
            fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> Result<bool> {
                d.scalar_into(n, $name, self, |r| match *r {
                    Resolved::Int(i) => {
                        if i >= 0 {
                            <$t>::try_from(i as u64).ok()
                        } else {
                            None
                        }
                    }
                    Resolved::Uint(u) => <$t>::try_from(u).ok(),
                    Resolved::Float(f) => {
                        if f <= u64::MAX as f64 {
                            <$t>::try_from(f as u64).ok()
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
            }
            fn go_type() -> String {
                $name.to_string()
            }
        }
    };
}
uint_impl!(u64, "uint64");
uint_impl!(u32, "uint32");
uint_impl!(usize, "uint");

impl Unmarshal for f64 {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> Result<bool> {
        d.scalar_into(n, "float64", self, |r| match *r {
            Resolved::Int(i) => Some(i as f64),
            Resolved::Uint(u) => Some(u as f64),
            Resolved::Float(f) => Some(f),
            _ => None,
        })
    }
    fn go_type() -> String {
        "float64".to_string()
    }
}

impl<T: Unmarshal + Default> Unmarshal for Vec<T> {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> Result<bool> {
        match n.kind {
            Kind::Sequence => {
                let mut out = Vec::with_capacity(n.children.len());
                for c in n.children.iter() {
                    let mut e = T::default();
                    if d.unmarshal(c, &mut e)? {
                        out.push(e);
                    }
                }
                *self = out;
                Ok(true)
            }
            Kind::Mapping => {
                d.terror(n, MAP_TAG, &Self::go_type());
                Ok(false)
            }
            _ => {
                let (tag, _) = d.scalar_tag(n)?;
                d.terror(n, &tag, &Self::go_type());
                Ok(false)
            }
        }
    }
    fn go_type() -> String {
        format!("[]{}", T::go_type())
    }
}

/// A Go pointer: null is nil, anything else allocates (or reuses) the value and decodes into it.
impl<T: Unmarshal + Default> Unmarshal for Option<T> {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> Result<bool> {
        let v = self.get_or_insert_with(T::default);
        v.unmarshal_yaml(n, d)
    }
    fn go_type() -> String {
        T::go_type()
    }
}

impl<T: Unmarshal + Default> Unmarshal for Box<T> {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> Result<bool> {
        (**self).unmarshal_yaml(n, d)
    }
    fn go_type() -> String {
        T::go_type()
    }
}

/// Map key types: what a YAML key decodes into.
pub trait MapKey: Unmarshal + Default + Ord + Clone {
    fn key_debug(&self) -> String;
}
impl MapKey for String {
    fn key_debug(&self) -> String {
        go_quote(self)
    }
}

impl<K: MapKey, V: Unmarshal + Default> Unmarshal for BTreeMap<K, V> {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> Result<bool> {
        match n.kind {
            Kind::Mapping => {
                d.mapping_into_map(n, self)?;
                Ok(true)
            }
            Kind::Sequence => {
                d.terror(n, SEQ_TAG, &Self::go_type());
                Ok(false)
            }
            _ => {
                let (tag, _) = d.scalar_tag(n)?;
                d.terror(n, &tag, &Self::go_type());
                Ok(false)
            }
        }
    }
    fn go_type() -> String {
        format!("map[{}]{}", K::go_type(), V::go_type())
    }
}

impl Decoder {
    /// go-yaml `d.mapping` into a Go map: existing entries stay; in strict mode a key set twice is an error.
    pub fn mapping_into_map<K: MapKey, V: Unmarshal + Default>(&mut self, n: &Node, out: &mut BTreeMap<K, V>) -> Result<()> {
        for (k, v) in n.pairs() {
            if k.is_merge() {
                self.merge(v, &mut |d, m| d.unmarshal(m, out).map(|_| ()))?;
                continue;
            }
            let mut key = K::default();
            if self.unmarshal(k, &mut key)? {
                let mut e = V::default();
                if self.unmarshal(v, &mut e)? {
                    self.set_map_index(v, out, key, e);
                }
            }
        }
        Ok(())
    }

    fn set_map_index<K: MapKey, V>(&mut self, n: &Node, out: &mut BTreeMap<K, V>, k: K, v: V) {
        if self.strict && out.contains_key(&k) {
            self.terrors.push(format!("line {}: key {} already set in map", n.line, k.key_debug()));
            return;
        }
        out.insert(k, v);
    }
}

// ---------------- interface{} ----------------

impl Unmarshal for Value {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> Result<bool> {
        match n.kind {
            Kind::Scalar => {
                let (tag, r) = d.scalar_tag(n)?;
                *self = match r {
                    Resolved::Null => Value::Null,
                    Resolved::Bool(b) => Value::Bool(b),
                    Resolved::Int(i) => Value::Int(i),
                    Resolved::Uint(u) => Value::Uint(u),
                    Resolved::Float(f) => Value::Float(f),
                    Resolved::Str(s) => Value::Str(s),
                    Resolved::Timestamp | Resolved::Merge => Value::Str(n.value.clone()),
                };
                let _ = tag;
                Ok(true)
            }
            Kind::Sequence => {
                let mut out = Vec::with_capacity(n.children.len());
                for c in n.children.iter() {
                    let mut e = Value::Null;
                    if d.unmarshal(c, &mut e)? {
                        out.push(e);
                    }
                }
                *self = Value::Seq(out);
                Ok(true)
            }
            Kind::Mapping => {
                let mut pairs: Vec<(Value, Value)> = Vec::new();
                d.mapping_into_pairs(n, &mut pairs)?;
                crate::value::sort_pairs(&mut pairs);
                *self = Value::Map(pairs);
                Ok(true)
            }
            Kind::Alias => unreachable!(),
        }
    }
    fn go_type() -> String {
        "interface {}".to_string()
    }
}

impl Decoder {
    fn mapping_into_pairs(&mut self, n: &Node, out: &mut Vec<(Value, Value)>) -> Result<()> {
        for (k, v) in n.pairs() {
            if k.is_merge() {
                self.merge(v, &mut |d, m| {
                    let m: &Node = if m.kind == Kind::Alias { return d.alias(m, |d, t| d.mapping_into_pairs(t, out)) } else { m };
                    d.mapping_into_pairs(m, out)
                })?;
                continue;
            }
            let mut key = Value::Null;
            if self.unmarshal(k, &mut key)? {
                if matches!(key, Value::Map(_) | Value::Seq(_)) {
                    return Err(Error::Msg(format!("yaml: invalid map key: {key:?}")));
                }
                let mut e = Value::Null;
                if self.unmarshal(v, &mut e)? {
                    if let Some(slot) = out.iter_mut().find(|p| p.0 == key) {
                        if self.strict {
                            self.terrors.push(format!("line {}: key {} already set in map", v.line, key.key_debug()));
                        } else {
                            slot.1 = e;
                        }
                    } else {
                        out.push((key, e));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Decodes the first document of `text` into `out` (go-yaml `Unmarshal`, or `UnmarshalStrict` when `strict`).
pub fn decode_into<T: Unmarshal + Default>(text: &str, out: &mut T, strict: bool) -> Result<()> {
    let root: Option<Rc<Node>> = crate::node::parse(text).map_err(Error::Msg)?;
    let mut d = Decoder::new(strict);
    if let Some(n) = root {
        d.unmarshal(&n, out)?;
    }
    d.into_result()
}
