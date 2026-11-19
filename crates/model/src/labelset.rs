//! prometheus/common `model.LabelSet`: a map of label names to values, as service discovery and configs use it
//! (Go map semantics; kept sorted by name).
use std::collections::BTreeMap;
use std::fmt;
use std::ops::{Deref, DerefMut};

use crate::labels::{self, Labels, ValidationScheme};
use crate::unicode;

/// `__address__`.
pub const ADDRESS_LABEL: &str = "__address__";
/// `__meta_`.
pub const META_LABEL_PREFIX: &str = "__meta_";

#[derive(Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LabelSet(pub BTreeMap<String, String>);

impl Deref for LabelSet {
    type Target = BTreeMap<String, String>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for LabelSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K: Into<String>, V: Into<String>> FromIterator<(K, V)> for LabelSet {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(it: I) -> Self {
        LabelSet(it.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
    }
}

impl LabelSet {
    pub fn new() -> LabelSet {
        LabelSet(BTreeMap::new())
    }
    /// A set from name/value pairs.
    pub fn from_pairs(p: &[(&str, &str)]) -> LabelSet {
        p.iter().map(|(k, v)| (*k, *v)).collect()
    }
    /// Go `Validate`: every name valid (UTF-8 scheme); values are always valid UTF-8 in Rust.
    pub fn validate(&self) -> Result<(), String> {
        for k in self.0.keys() {
            if !ValidationScheme::Utf8.is_valid_label_name(k) {
                return Err(format!("invalid name {}", unicode::quote(k)));
            }
        }
        Ok(())
    }
    /// Go `Merge`: a new set with `other` taking precedence.
    pub fn merge(&self, other: &LabelSet) -> LabelSet {
        let mut r = self.clone();
        for (k, v) in other.iter() {
            r.0.insert(k.clone(), v.clone());
        }
        r
    }
    /// Go `Fingerprint`: FNV-1a 64 over the sorted names and values, each followed by the separator byte 255.
    /// The empty set has the FNV offset basis.
    pub fn fingerprint(&self) -> u64 {
        const OFFSET64: u64 = 14695981039346656037;
        const PRIME64: u64 = 1099511628211;
        let add = |h: u64, b: &[u8]| b.iter().fold(h, |h, &c| (h ^ c as u64).wrapping_mul(PRIME64));
        let mut h = OFFSET64;
        for (k, v) in self.0.iter() {
            h = add(h, k.as_bytes());
            h = add(h, &[255]);
            h = add(h, v.as_bytes());
            h = add(h, &[255]);
        }
        h
    }

    /// As `labels.Labels`.
    pub fn to_labels(&self) -> Labels {
        labels::from_map(self.0.iter().map(|(k, v)| (k.as_str(), v.as_str())))
    }

    /// Go `MarshalJSON` (`map[string]string`, keys sorted).
    pub fn to_json(&self) -> json::Json {
        json::Json::Obj(self.0.iter().map(|(k, v)| (k.clone(), json::Json::Str(v.clone()))).collect())
    }

    /// Go `UnmarshalJSON`: an object of strings (null is an empty set); names must be valid.
    pub fn from_json(v: &json::Json) -> Result<LabelSet, String> {
        match v {
            json::Json::Null => Ok(LabelSet::new()),
            json::Json::Obj(o) => {
                let mut m = BTreeMap::new();
                for (k, v) in o {
                    match v {
                        json::Json::Str(s) => {
                            m.insert(k.clone(), s.clone());
                        }
                        json::Json::Null => {
                            m.entry(k.clone()).or_insert_with(String::new);
                        }
                        other => return Err(format!("json: cannot unmarshal {} into Go value of type model.LabelValue", json_kind(other))),
                    }
                }
                for k in m.keys() {
                    if !ValidationScheme::Utf8.is_valid_label_name(k) {
                        return Err(format!("{} is not a valid label name", unicode::quote(k)));
                    }
                }
                Ok(LabelSet(m))
            }
            other => Err(format!("json: cannot unmarshal {} into Go value of type map[model.LabelName]model.LabelValue", json_kind(other))),
        }
    }
}

/// The JSON kind name Go prints in unmarshal type errors.
pub fn json_kind(v: &json::Json) -> &'static str {
    match v {
        json::Json::Null => "null",
        json::Json::Bool(_) => "bool",
        json::Json::Int(_) | json::Json::Float(_) => "number",
        json::Json::Str(_) => "string",
        json::Json::Arr(_) => "array",
        json::Json::Obj(_) => "object",
    }
}

/// Go `LabelSet.String()`: `{a="1", b="2"}`.
impl fmt::Display for LabelSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::with_capacity(64);
        s.push('{');
        for (i, (k, v)) in self.0.iter().enumerate() {
            if i > 0 {
                s.push_str(", ");
            }
            s.push_str(k);
            s.push('=');
            s.push_str(&unicode::quote(v));
        }
        s.push('}');
        f.write_str(&s)
    }
}
impl fmt::Debug for LabelSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
