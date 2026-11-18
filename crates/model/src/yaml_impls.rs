//! go-yaml v2 decoding and encoding (crate `yaml`) of the model types that configs embed: `relabel.Config`,
//! `relabel.Regexp`, `relabel.Action`, `model.ValidationScheme`, `labels.Labels`, `model.LabelName(s)` and
//! `model.LabelSet`.
use std::collections::BTreeMap;

use yaml::{Decoder, Error as YError, Kind, Marshal, Node, Struct, Unmarshal, Value};

use crate::labels::{self, Labels, ValidationScheme};
use crate::labelset::LabelSet;
use crate::relabel::{self, Action, Config, Regexp};
use crate::unicode;

// ---------------- relabel.Action ----------------

impl Unmarshal for Action {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut s = String::new();
            d.inner(n, &mut s)?;
            *self = Action::parse(&s).map_err(YError::Msg)?;
            Ok(())
        })
    }
    fn go_type() -> String {
        "relabel.Action".to_string()
    }
}
impl Marshal for Action {
    fn marshal_yaml(&self) -> Result<Value, String> {
        Ok(Value::Str(self.as_str().to_string()))
    }
    fn is_zero(&self) -> bool {
        *self == Action::Unset
    }
}

// ---------------- relabel.Regexp ----------------

impl Unmarshal for Regexp {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut s = String::new();
            d.inner(n, &mut s)?;
            *self = Regexp::new(&s).map_err(|e| YError::Msg(e.to_string()))?;
            Ok(())
        })
    }
    fn go_type() -> String {
        "relabel.Regexp".to_string()
    }
}
impl Marshal for Regexp {
    fn marshal_yaml(&self) -> Result<Value, String> {
        match &self.0 {
            None => Ok(Value::Null),
            Some(_) => Ok(Value::Str(self.string().to_string())),
        }
    }
    /// Go `IsZeroer`: the default regexp object is omitted.
    fn is_zero(&self) -> bool {
        Regexp::is_zero(self)
    }
}

// ---------------- model.ValidationScheme ----------------

impl Unmarshal for ValidationScheme {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut s = String::new();
            d.inner(n, &mut s)?;
            self.set(&s).map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "model.ValidationScheme".to_string()
    }
}
impl Marshal for ValidationScheme {
    fn marshal_yaml(&self) -> Result<Value, String> {
        Ok(Value::Str(match self {
            ValidationScheme::Unset => String::new(),
            s => s.as_str().to_string(),
        }))
    }
    fn is_zero(&self) -> bool {
        *self == ValidationScheme::Unset
    }
}

// ---------------- labels.Labels ----------------

impl Unmarshal for Labels {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut m: BTreeMap<String, String> = BTreeMap::new();
            d.inner(n, &mut m)?;
            *self = labels::from_map(m);
            Ok(())
        })
    }
    fn go_type() -> String {
        "labels.Labels".to_string()
    }
}
impl Marshal for Labels {
    fn marshal_yaml(&self) -> Result<Value, String> {
        let mut m = BTreeMap::new();
        for (k, v) in self.iter() {
            m.insert(k.to_string(), v.to_string());
        }
        m.marshal_yaml()
    }
    fn is_zero(&self) -> bool {
        self.is_empty()
    }
}

// ---------------- model.LabelName, model.LabelNames ----------------

/// Go `model.LabelName` for YAML: a string checked with `IsValid` (the UTF-8 scheme: not empty).
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LabelName(pub String);

impl Unmarshal for LabelName {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut s = String::new();
            d.inner(n, &mut s)?;
            if !ValidationScheme::Utf8.is_valid_label_name(&s) {
                return Err(YError::Msg(format!("{} is not a valid label name", unicode::quote(&s))));
            }
            self.0 = s;
            Ok(())
        })
    }
    fn go_type() -> String {
        "model.LabelName".to_string()
    }
}
impl yaml::MapKey for LabelName {
    fn key_debug(&self) -> String {
        yaml::go_quote(&self.0)
    }
}
impl Marshal for LabelName {
    fn marshal_yaml(&self) -> Result<Value, String> {
        Ok(Value::Str(self.0.clone()))
    }
    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }
}

/// Go `model.LabelNames` (a list of checked label names); `None` is a nil slice.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LabelNames(pub Option<Vec<String>>);

impl Unmarshal for LabelNames {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        match n.kind {
            Kind::Sequence => {
                let mut out = Vec::with_capacity(n.children.len());
                for c in n.children.iter() {
                    let mut e = LabelName::default();
                    if d.unmarshal(c, &mut e)? {
                        out.push(e.0);
                    }
                }
                self.0 = Some(out);
                Ok(true)
            }
            Kind::Mapping => {
                d.terror(n, yaml::node::MAP_TAG, "model.LabelNames");
                Ok(false)
            }
            _ => {
                let (tag, _) = d.scalar_tag(n)?;
                d.terror(n, &tag, "model.LabelNames");
                Ok(false)
            }
        }
    }
    fn go_type() -> String {
        "model.LabelNames".to_string()
    }
}

// ---------------- model.LabelSet ----------------

impl Unmarshal for LabelSet {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        match n.kind {
            Kind::Mapping => {
                // a Go map: existing entries stay (go-yaml reuses a non-nil map)
                let mut m: BTreeMap<LabelName, String> = std::mem::take(&mut self.0).into_iter().map(|(k, v)| (LabelName(k), v)).collect();
                let r = d.mapping_into_map(n, &mut m);
                self.0 = m.into_iter().map(|(k, v)| (k.0, v)).collect();
                r?;
                Ok(true)
            }
            Kind::Sequence => {
                d.terror(n, yaml::node::SEQ_TAG, "model.LabelSet");
                Ok(false)
            }
            _ => {
                let (tag, _) = d.scalar_tag(n)?;
                d.terror(n, &tag, "model.LabelSet");
                Ok(false)
            }
        }
    }
    fn go_type() -> String {
        "model.LabelSet".to_string()
    }
}
impl Marshal for LabelSet {
    fn marshal_yaml(&self) -> Result<Value, String> {
        self.0.marshal_yaml()
    }
    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }
}

// ---------------- relabel.Config ----------------

impl Struct for Config {
    fn type_name(&self) -> &'static str {
        "relabel.plain"
    }
    fn has_field(&self, k: &str) -> bool {
        matches!(k, "source_labels" | "separator" | "regex" | "modulus" | "target_label" | "replacement" | "action")
    }
    fn field(&mut self, k: &str) -> Option<&mut dyn yaml::Field> {
        Some(match k {
            "separator" => &mut self.separator,
            "regex" => &mut self.regex,
            "modulus" => &mut self.modulus,
            "target_label" => &mut self.target_label,
            "replacement" => &mut self.replacement,
            "action" => &mut self.action,
            _ => return None,
        })
    }
    fn decode_key(&mut self, key: &str, n: &Node, d: &mut Decoder) -> Option<yaml::Result<bool>> {
        if key != "source_labels" {
            return None;
        }
        let mut ln = LabelNames(self.source_labels.take());
        let r = d.unmarshal(n, &mut ln);
        self.source_labels = ln.0;
        Some(r)
    }
    fn marshal_fields(&self, out: &mut Vec<(Value, Value)>) -> Result<(), String> {
        if let Some(sl) = &self.source_labels
            && !sl.is_empty()
        {
            out.push((Value::Str("source_labels".into()), Value::FlowSeq(sl.iter().map(|s| Value::Str(s.clone())).collect())));
        }
        yaml::push_field(out, "separator", &self.separator, false)?;
        yaml::push_field(out, "regex", &self.regex, true)?;
        yaml::push_field(out, "modulus", &self.modulus, true)?;
        yaml::push_field(out, "target_label", &self.target_label, true)?;
        yaml::push_field(out, "replacement", &self.replacement, false)?;
        yaml::push_field(out, "action", &self.action, true)?;
        Ok(())
    }
    fn fields_zero(&self) -> bool {
        self.source_labels.as_ref().is_none_or(|s| s.is_empty())
            && self.separator.is_empty()
            && self.regex.0.is_none()
            && self.modulus == 0
            && self.target_label.is_empty()
            && self.replacement.is_empty()
            && self.action == Action::Unset
    }
}

impl Unmarshal for Config {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = relabel::default_relabel_config();
            d.inner_struct(n, self)?;
            if self.regex.0.is_none() {
                self.regex = Regexp::must_new("");
            }
            Ok(())
        })
    }
    fn go_type() -> String {
        "relabel.plain".to_string()
    }
}
impl Marshal for Config {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        self.fields_zero()
    }
}
