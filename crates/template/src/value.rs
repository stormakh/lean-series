//! Dynamic values for the template engine: the part of Go's `reflect` that `text/template` needs.
//!
//! Go templates work on any Go value through reflection. This crate models the values that Prometheus templates
//! see: basic types, strings and the `html/template` content types, slices, maps with string keys, structs,
//! pointers, `time.Time`, `time.Duration`, `*histogram.FloatHistogram` and functions. Every value knows its Go
//! type, because error messages, `%T`, function argument checks and comparisons depend on it.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use model::histogram::FloatHistogram;

use crate::gotime::GoTime;

/// The Go kinds of signed integers. `Duration`, `Month` and `Weekday` are the `time` package's named integer types
/// (they have a `String` method).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IntKind {
    Int,
    Int8,
    Int16,
    Int32,
    Int64,
    /// `time.Duration` (int64 nanoseconds).
    Duration,
    /// `time.Month` (int).
    Month,
    /// `time.Weekday` (int).
    Weekday,
}

/// The Go kinds of unsigned integers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UintKind {
    Uint,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Uintptr,
}

/// Go `string` and the named string types of `html/template` (content types that are known to be safe).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StrKind {
    String,
    /// `template.HTML`.
    Html,
    /// `template.CSS`.
    Css,
    /// `template.HTMLAttr`.
    HtmlAttr,
    /// `template.JS`.
    Js,
    /// `template.JSStr`.
    JsStr,
    /// `template.URL`.
    Url,
    /// `template.Srcset`.
    Srcset,
}

/// A Go struct type: its qualified name (such as `template.sample`) and its fields.
#[derive(Debug, PartialEq)]
pub struct StructType {
    pub name: String,
    pub fields: Vec<(String, Type)>,
}

impl StructType {
    /// The index of the field `name`.
    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|(n, _)| n == name)
    }
}

/// A function signature: parameter types (for a variadic function the last one is the element type of the
/// `...` parameter), and the result types (one value, or a value and `error`).
#[derive(Debug, PartialEq)]
pub struct Signature {
    pub args: Vec<Type>,
    pub variadic: bool,
    pub out: Type,
    pub returns_error: bool,
}

/// A Go type.
#[derive(Clone, Debug, PartialEq)]
pub enum Type {
    /// `interface {}` (`any`).
    Interface,
    /// `error`.
    Error,
    Bool,
    Int(IntKind),
    Uint(UintKind),
    Float64,
    Complex128,
    String(StrKind),
    /// A slice type: element type and an optional type name (such as `template.queryResult`).
    Slice(Arc<Type>, Option<Arc<str>>),
    /// A map type with string keys: element type and an optional type name.
    Map(Arc<Type>, Option<Arc<str>>),
    Ptr(Arc<Type>),
    Struct(Arc<StructType>),
    /// `time.Time`.
    Time,
    /// `histogram.FloatHistogram`.
    FloatHistogram,
    Func(Arc<Signature>),
    /// `reflect.Value`: the parameter type of builtins that take any value unconverted.
    ReflectValue,
}

/// The kind of a type (Go `reflect.Kind`, reduced).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Invalid,
    Bool,
    Int,
    Uint,
    Float,
    Complex,
    String,
    Slice,
    Map,
    Ptr,
    Struct,
    Func,
    Interface,
}

impl Type {
    /// The empty-interface slice `[]interface {}`.
    pub fn slice_of(elem: Type) -> Type {
        Type::Slice(Arc::new(elem), None)
    }

    /// `map[string]elem`.
    pub fn map_of(elem: Type) -> Type {
        Type::Map(Arc::new(elem), None)
    }

    /// `*elem`.
    pub fn ptr_to(elem: Type) -> Type {
        Type::Ptr(Arc::new(elem))
    }

    /// The reflect kind.
    pub fn kind(&self) -> Kind {
        match self {
            Type::Interface | Type::Error => Kind::Interface,
            Type::Bool => Kind::Bool,
            Type::Int(_) => Kind::Int,
            Type::Uint(_) => Kind::Uint,
            Type::Float64 => Kind::Float,
            Type::Complex128 => Kind::Complex,
            Type::String(_) => Kind::String,
            Type::Slice(..) => Kind::Slice,
            Type::Map(..) => Kind::Map,
            Type::Ptr(_) => Kind::Ptr,
            Type::Struct(_) | Type::Time | Type::FloatHistogram | Type::ReflectValue => Kind::Struct,
            Type::Func(_) => Kind::Func,
        }
    }

    /// Go `canBeNil`.
    pub fn can_be_nil(&self) -> bool {
        matches!(self.kind(), Kind::Func | Kind::Interface | Kind::Map | Kind::Ptr | Kind::Slice) || *self == Type::ReflectValue
    }

    /// Whether the type has a name (a defined type), which matters for assignability.
    fn is_named(&self) -> bool {
        match self {
            Type::Slice(_, n) | Type::Map(_, n) => n.is_some(),
            Type::Ptr(_) | Type::Func(_) => false,
            _ => true,
        }
    }

    /// Go `Type.AssignableTo`.
    pub fn assignable_to(&self, to: &Type) -> bool {
        if self == to {
            return true;
        }
        match to {
            Type::Interface => return true,
            Type::Error => return false,
            _ => {}
        }
        // Identical underlying types where at least one of them is not named.
        if self.is_named() && to.is_named() {
            return false;
        }
        match (self, to) {
            (Type::Slice(a, _), Type::Slice(b, _)) | (Type::Map(a, _), Type::Map(b, _)) => a == b,
            _ => false,
        }
    }

    /// Go `reflect.Zero(typ)`.
    pub fn zero(&self) -> Value {
        match self {
            Type::Interface | Type::Error | Type::Slice(..) | Type::Map(..) | Type::Ptr(_) | Type::Func(_) => {
                Value::Nil(self.clone())
            }
            Type::Bool => Value::Bool(false),
            Type::Int(k) => Value::Int(0, *k),
            Type::Uint(k) => Value::Uint(0, *k),
            Type::Float64 => Value::Float(0.0),
            Type::Complex128 => Value::Complex(0.0, 0.0),
            Type::String(k) => Value::String(Arc::from(""), *k),
            Type::Struct(st) => Value::Struct(Arc::new(StructValue {
                ty: st.clone(),
                fields: st.fields.iter().map(|(_, t)| t.zero()).collect(),
            })),
            Type::Time => Value::Time(GoTime::ZERO),
            Type::FloatHistogram => Value::Histogram(Arc::new(FloatHistogram::default())),
            Type::ReflectValue => Value::Invalid,
        }
    }

    /// The element type of a pointer, slice or map.
    pub fn elem(&self) -> Option<&Type> {
        match self {
            Type::Slice(e, _) | Type::Map(e, _) | Type::Ptr(e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for Type {
    /// Go `reflect.Type.String`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Interface => f.write_str("interface {}"),
            Type::Error => f.write_str("error"),
            Type::Bool => f.write_str("bool"),
            Type::Int(k) => f.write_str(match k {
                IntKind::Int => "int",
                IntKind::Int8 => "int8",
                IntKind::Int16 => "int16",
                IntKind::Int32 => "int32",
                IntKind::Int64 => "int64",
                IntKind::Duration => "time.Duration",
                IntKind::Month => "time.Month",
                IntKind::Weekday => "time.Weekday",
            }),
            Type::Uint(k) => f.write_str(match k {
                UintKind::Uint => "uint",
                UintKind::Uint8 => "uint8",
                UintKind::Uint16 => "uint16",
                UintKind::Uint32 => "uint32",
                UintKind::Uint64 => "uint64",
                UintKind::Uintptr => "uintptr",
            }),
            Type::Float64 => f.write_str("float64"),
            Type::Complex128 => f.write_str("complex128"),
            Type::String(k) => f.write_str(match k {
                StrKind::String => "string",
                StrKind::Html => "template.HTML",
                StrKind::Css => "template.CSS",
                StrKind::HtmlAttr => "template.HTMLAttr",
                StrKind::Js => "template.JS",
                StrKind::JsStr => "template.JSStr",
                StrKind::Url => "template.URL",
                StrKind::Srcset => "template.Srcset",
            }),
            Type::Slice(e, name) => match name {
                Some(n) => f.write_str(n),
                None => write!(f, "[]{}", e),
            },
            Type::Map(e, name) => match name {
                Some(n) => f.write_str(n),
                None => write!(f, "map[string]{}", e),
            },
            Type::Ptr(e) => write!(f, "*{}", e),
            Type::Struct(st) => f.write_str(&st.name),
            Type::Time => f.write_str("time.Time"),
            Type::FloatHistogram => f.write_str("histogram.FloatHistogram"),
            Type::ReflectValue => f.write_str("reflect.Value"),
            Type::Func(sig) => {
                f.write_str("func(")?;
                for (i, a) in sig.args.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    if sig.variadic && i == sig.args.len() - 1 {
                        write!(f, "...{}", a)?;
                    } else {
                        write!(f, "{}", a)?;
                    }
                }
                f.write_str(")")?;
                if sig.returns_error {
                    write!(f, " ({}, error)", sig.out)
                } else {
                    write!(f, " {}", sig.out)
                }
            }
        }
    }
}

/// The implementation of a function: takes the converted arguments, returns the result or the error text.
pub type FuncImpl = dyn Fn(&[Value]) -> Result<Value, String> + Send + Sync;

/// A function value (Go `func`), with its signature.
#[derive(Clone)]
pub struct Func {
    pub sig: Arc<Signature>,
    pub f: Arc<FuncImpl>,
}

impl Func {
    /// A function with fixed parameters `args` and one result of type `out` (and an `error` if `returns_error`).
    pub fn new(
        args: Vec<Type>,
        out: Type,
        returns_error: bool,
        f: impl Fn(&[Value]) -> Result<Value, String> + Send + Sync + 'static,
    ) -> Func {
        Func { sig: Arc::new(Signature { args, variadic: false, out, returns_error }), f: Arc::new(f) }
    }

    /// A variadic function: the last element of `args` is the element type of the `...` parameter.
    pub fn variadic(
        args: Vec<Type>,
        out: Type,
        returns_error: bool,
        f: impl Fn(&[Value]) -> Result<Value, String> + Send + Sync + 'static,
    ) -> Func {
        Func { sig: Arc::new(Signature { args, variadic: true, out, returns_error }), f: Arc::new(f) }
    }

    /// Go `reflect.Value.Call`.
    pub fn call(&self, args: &[Value]) -> Result<Value, String> {
        (self.f)(args)
    }
}

impl fmt::Debug for Func {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Func({})", Type::Func(self.sig.clone()))
    }
}

/// A slice value.
#[derive(Clone, Debug)]
pub struct SliceValue {
    /// The slice type.
    pub ty: Type,
    pub items: Vec<Value>,
}

/// A map value with string keys (iterated in sorted key order, as Go's `fmt` and `range` do).
#[derive(Clone, Debug)]
pub struct MapValue {
    /// The map type.
    pub ty: Type,
    pub entries: BTreeMap<String, Value>,
}

/// A struct value.
#[derive(Clone, Debug)]
pub struct StructValue {
    pub ty: Arc<StructType>,
    pub fields: Vec<Value>,
}

impl StructValue {
    /// The field `name`.
    pub fn field(&self, name: &str) -> Option<&Value> {
        self.ty.field_index(name).map(|i| &self.fields[i])
    }
}

/// A dynamic Go value (a `reflect.Value`).
#[derive(Clone, Debug, Default)]
pub enum Value {
    /// The zero `reflect.Value`: no value (Go's untyped `nil`, a missing map key).
    #[default]
    Invalid,
    /// A typed nil: a nil pointer, map, slice, function or interface of the given type.
    Nil(Type),
    Bool(bool),
    Int(i64, IntKind),
    Uint(u64, UintKind),
    Float(f64),
    Complex(f64, f64),
    String(Arc<str>, StrKind),
    Slice(Arc<SliceValue>),
    Map(Arc<MapValue>),
    Struct(Arc<StructValue>),
    /// A non-nil pointer to a value.
    Ptr(Arc<Value>),
    Time(GoTime),
    Histogram(Arc<FloatHistogram>),
    Func(Func),
    /// A non-nil value read from an `interface {}` slot (a map element, slice element or struct field of
    /// interface type): Go's reflect value of kind Interface. Its static type is `interface {}`.
    Iface(Arc<Value>),
}

impl Value {
    /// A Go `string`.
    pub fn string(s: impl Into<Arc<str>>) -> Value {
        Value::String(s.into(), StrKind::String)
    }

    /// A `template.HTML` (trusted HTML, not escaped by `html/template`).
    pub fn html(s: impl Into<Arc<str>>) -> Value {
        Value::String(s.into(), StrKind::Html)
    }

    /// A Go `int`.
    pub fn int(i: i64) -> Value {
        Value::Int(i, IntKind::Int)
    }

    /// A Go `float64`.
    pub fn float(f: f64) -> Value {
        Value::Float(f)
    }

    /// A slice of `elem` values.
    pub fn slice(elem: Type, items: Vec<Value>) -> Value {
        Value::Slice(Arc::new(SliceValue { ty: Type::slice_of(elem), items }))
    }

    /// A slice of the named or unnamed slice type `ty`.
    pub fn slice_typed(ty: Type, items: Vec<Value>) -> Value {
        Value::Slice(Arc::new(SliceValue { ty, items }))
    }

    /// A `map[string]elem`.
    pub fn map(elem: Type, entries: BTreeMap<String, Value>) -> Value {
        Value::Map(Arc::new(MapValue { ty: Type::map_of(elem), entries }))
    }

    /// A `map[string]string`.
    pub fn string_map<K: Into<String>, V: Into<Arc<str>>>(m: impl IntoIterator<Item = (K, V)>) -> Value {
        let entries = m.into_iter().map(|(k, v)| (k.into(), Value::string(v))).collect();
        Value::map(Type::String(StrKind::String), entries)
    }

    /// A pointer to `v`.
    pub fn ptr(v: Value) -> Value {
        Value::Ptr(Arc::new(v))
    }

    /// A struct of type `ty` with the given field values.
    pub fn new_struct(ty: Arc<StructType>, fields: Vec<Value>) -> Value {
        Value::Struct(Arc::new(StructValue { ty, fields }))
    }

    /// A `time.Duration` of `ns` nanoseconds.
    pub fn duration(ns: i64) -> Value {
        Value::Int(ns, IntKind::Duration)
    }

    /// Go `reflect.Value.IsValid`.
    #[inline]
    pub fn is_valid(&self) -> bool {
        !matches!(self, Value::Invalid)
    }

    /// The Go type (`None` for an invalid value).
    pub fn type_of(&self) -> Option<Type> {
        Some(match self {
            Value::Invalid => return None,
            Value::Nil(t) => t.clone(),
            Value::Bool(_) => Type::Bool,
            Value::Int(_, k) => Type::Int(*k),
            Value::Uint(_, k) => Type::Uint(*k),
            Value::Float(_) => Type::Float64,
            Value::Complex(..) => Type::Complex128,
            Value::String(_, k) => Type::String(*k),
            Value::Slice(s) => s.ty.clone(),
            Value::Map(m) => m.ty.clone(),
            Value::Struct(s) => Type::Struct(s.ty.clone()),
            Value::Ptr(v) => Type::ptr_to(v.type_of().unwrap_or(Type::Interface)),
            Value::Time(_) => Type::Time,
            Value::Histogram(_) => Type::FloatHistogram,
            Value::Func(f) => Type::Func(f.sig.clone()),
            Value::Iface(_) => Type::Interface,
        })
    }

    /// The Go type name (`reflect.Type.String`), `<nil>` for an invalid value.
    pub fn type_name(&self) -> String {
        match self.type_of() {
            Some(t) => t.to_string(),
            None => "<nil>".to_string(),
        }
    }

    /// The reflect kind.
    pub fn kind(&self) -> Kind {
        match self {
            Value::Invalid => Kind::Invalid,
            Value::Nil(t) => t.kind(),
            Value::Bool(_) => Kind::Bool,
            Value::Int(..) => Kind::Int,
            Value::Uint(..) => Kind::Uint,
            Value::Float(_) => Kind::Float,
            Value::Complex(..) => Kind::Complex,
            Value::String(..) => Kind::String,
            Value::Slice(_) => Kind::Slice,
            Value::Map(_) => Kind::Map,
            Value::Ptr(_) => Kind::Ptr,
            Value::Struct(_) | Value::Time(_) | Value::Histogram(_) => Kind::Struct,
            Value::Func(_) => Kind::Func,
            Value::Iface(_) => Kind::Interface,
        }
    }

    /// Whether the value is a nil pointer, map, slice, function or interface.
    pub fn is_nil(&self) -> bool {
        matches!(self, Value::Nil(_))
    }

    /// The string of a string-kind value.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s, _) => Some(s),
            _ => None,
        }
    }

    /// Go `reflect.Value.Len` for slices, maps and strings.
    pub fn len(&self) -> Option<usize> {
        match self {
            Value::Slice(s) => Some(s.items.len()),
            Value::Map(m) => Some(m.entries.len()),
            Value::String(s, _) => Some(s.len()),
            Value::Nil(t) if matches!(t.kind(), Kind::Slice | Kind::Map) => Some(0),
            _ => None,
        }
    }

    /// Whether `len` is 0.
    pub fn is_empty(&self) -> bool {
        self.len() == Some(0)
    }
}

/// Wraps a value read from a container slot of type `elem`: a missing value in an interface slot is a nil
/// interface.
pub(crate) fn from_slot(v: &Value, elem: &Type) -> Value {
    if elem.kind() != Kind::Interface {
        return v.clone();
    }
    match v {
        Value::Invalid => Value::Nil(elem.clone()),
        Value::Nil(t) if t.kind() == Kind::Interface => v.clone(),
        Value::Iface(_) => v.clone(),
        _ => Value::Iface(Arc::new(v.clone())),
    }
}

/// Go `indirect`: follows pointers; returns the value and whether a nil pointer or interface was found.
pub(crate) fn indirect(v: &Value) -> (Value, bool) {
    let mut v = v.clone();
    loop {
        match v {
            Value::Ptr(inner) | Value::Iface(inner) => v = (*inner).clone(),
            Value::Nil(ref t) if matches!(t.kind(), Kind::Ptr | Kind::Interface) => return (v, true),
            _ => return (v, false),
        }
    }
}

/// Go `indirectInterface`: a nil interface becomes the invalid value.
pub(crate) fn indirect_interface(v: &Value) -> Value {
    match v {
        Value::Nil(t) if t.kind() == Kind::Interface => Value::Invalid,
        Value::Iface(inner) => indirect_interface(inner),
        _ => v.clone(),
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Value {
        Value::Float(f)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Value {
        Value::Bool(b)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Value {
        Value::string(s)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Value {
        Value::string(s)
    }
}
