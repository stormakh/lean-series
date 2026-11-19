//! Go `encoding/json` decoding into typed structs, for the JSON that the marathon and zookeeper providers read:
//! field names match exactly first and then case-insensitively, unknown fields are ignored, `null` leaves a
//! value as it is, maps merge, slices are replaced, and a type mismatch gives Go's `UnmarshalTypeError` text
//! ("json: cannot unmarshal string into Go struct field task.apps.tasks.ports of type uint32"). As in Go,
//! decoding goes on after a type error and the first error is returned.
//!
//! Differs from Go: syntax errors use the text of the `json` crate, and a number in an error is written back
//! from its parsed value (Go quotes the literal as written, for example `1.50`).
use std::collections::BTreeMap;

use json::Json;
use model::labelset::json_kind;

/// The decoding state: the first error and the error context (Go `errorContext`).
#[derive(Default)]
pub struct Ctx {
    err: Option<String>,
    struct_name: Option<&'static str>,
    stack: Vec<&'static str>,
}

impl Ctx {
    /// Go `UnmarshalTypeError` with the context added (Go `addErrorContext`).
    pub fn type_error(&mut self, value: &str, typ: &str) {
        if self.err.is_some() {
            return;
        }
        let msg = if self.struct_name.is_some() || !self.stack.is_empty() {
            format!("json: cannot unmarshal {value} into Go struct field {}.{} of type {typ}", self.struct_name.unwrap_or(""), self.stack.join("."))
        } else {
            format!("json: cannot unmarshal {value} into Go value of type {typ}")
        };
        self.err = Some(msg);
    }

    /// Enters field `field` of struct `name`; returns the state to restore with `leave`.
    pub fn enter(&mut self, name: &'static str, field: &'static str) -> (Option<&'static str>, usize) {
        let mark = (self.struct_name, self.stack.len());
        self.struct_name = Some(name);
        self.stack.push(field);
        mark
    }

    pub fn leave(&mut self, mark: (Option<&'static str>, usize)) {
        self.struct_name = mark.0;
        self.stack.truncate(mark.1);
    }
}

/// A Go type that `encoding/json` can decode into.
pub trait GoJson {
    /// Go `reflect.Type.String()` ("marathon.app", "[]uint32", "map[string]string").
    fn type_name() -> String;
    fn decode(&mut self, v: &Json, cx: &mut Ctx);
}

/// Go `json.Unmarshal(b, &v)`.
pub fn unmarshal<T: GoJson>(b: &[u8], v: &mut T) -> Result<(), String> {
    let j = Json::parse_bytes(b).map_err(|e| e.to_string())?;
    let mut cx = Ctx::default();
    v.decode(&j, &mut cx);
    match cx.err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// The text Go puts after "number " for a number that does not fit.
fn number_text(v: &Json) -> String {
    match v {
        Json::Int(i) => i.to_string(),
        Json::Float(f) => {
            let mut s = String::new();
            json::write_f64(*f, &mut s);
            s
        }
        _ => String::new(),
    }
}

impl GoJson for String {
    fn type_name() -> String {
        "string".to_string()
    }
    fn decode(&mut self, v: &Json, cx: &mut Ctx) {
        match v {
            Json::Null => {}
            Json::Str(s) => *self = s.clone(),
            other => cx.type_error(json_kind(other), "string"),
        }
    }
}

impl GoJson for bool {
    fn type_name() -> String {
        "bool".to_string()
    }
    fn decode(&mut self, v: &Json, cx: &mut Ctx) {
        match v {
            Json::Null => {}
            Json::Bool(b) => *self = *b,
            other => cx.type_error(json_kind(other), "bool"),
        }
    }
}

macro_rules! go_int {
    ($t:ty, $name:literal) => {
        impl GoJson for $t {
            fn type_name() -> String {
                $name.to_string()
            }
            fn decode(&mut self, v: &Json, cx: &mut Ctx) {
                match v {
                    Json::Null => {}
                    Json::Int(i) => match <$t>::try_from(*i) {
                        Ok(n) => *self = n,
                        Err(_) => cx.type_error(&format!("number {}", number_text(v)), $name),
                    },
                    Json::Float(_) => cx.type_error(&format!("number {}", number_text(v)), $name),
                    other => cx.type_error(json_kind(other), $name),
                }
            }
        }
    };
}
// Go `int` is 64 bits wide on the platforms Prometheus runs on.
go_int!(i64, "int");
go_int!(u32, "uint32");

impl<T: GoJson + Default> GoJson for Vec<T> {
    fn type_name() -> String {
        format!("[]{}", T::type_name())
    }
    fn decode(&mut self, v: &Json, cx: &mut Ctx) {
        match v {
            Json::Null => self.clear(),
            Json::Arr(a) => {
                self.clear();
                for item in a {
                    let mut e = T::default();
                    e.decode(item, cx);
                    self.push(e);
                }
            }
            other => cx.type_error(json_kind(other), &Self::type_name()),
        }
    }
}

impl<T: GoJson + Default> GoJson for BTreeMap<String, T> {
    fn type_name() -> String {
        format!("map[string]{}", T::type_name())
    }
    fn decode(&mut self, v: &Json, cx: &mut Ctx) {
        match v {
            Json::Null => {}
            Json::Obj(o) => {
                for (k, item) in o {
                    let mut e = T::default();
                    e.decode(item, cx);
                    self.insert(k.clone(), e);
                }
            }
            other => cx.type_error(json_kind(other), &Self::type_name()),
        }
    }
}

/// The index of the field for JSON key `k`: exact match first, then case-insensitive (Go `foldName`).
pub fn find_field(keys: &[&str], k: &str) -> Option<usize> {
    if let Some(i) = keys.iter().position(|f| *f == k) {
        return Some(i);
    }
    let fold = |s: &str| s.chars().flat_map(char::to_lowercase).collect::<String>();
    let kf = fold(k);
    keys.iter().position(|f| fold(f) == kf)
}

/// Implements [`GoJson`] for a struct: `go_json_struct!(Type, "pkg", "name", [field: "json key", ...])`.
/// The key is the `json` tag name, or the Go field name when the field has no tag.
#[macro_export]
macro_rules! go_json_struct {
    ($t:ty, $pkg:literal, $name:literal, [$($f:ident : $k:literal),* $(,)?]) => {
        impl $crate::marathon::gojson::GoJson for $t {
            fn type_name() -> String {
                concat!($pkg, ".", $name).to_string()
            }
            #[allow(unused_assignments)]
            fn decode(&mut self, v: &json::Json, cx: &mut $crate::marathon::gojson::Ctx) {
                const KEYS: &[&str] = &[$($k),*];
                match v {
                    json::Json::Null => {}
                    json::Json::Obj(o) => {
                        for (k, item) in o {
                            let Some(i) = $crate::marathon::gojson::find_field(KEYS, k) else { continue };
                            let mark = cx.enter($name, KEYS[i]);
                            let mut j = 0usize;
                            $(
                                if j == i {
                                    $crate::marathon::gojson::GoJson::decode(&mut self.$f, item, cx);
                                }
                                j += 1;
                            )*
                            cx.leave(mark);
                        }
                    }
                    other => cx.type_error(model::labelset::json_kind(other), concat!($pkg, ".", $name)),
                }
            }
        }
    };
}
