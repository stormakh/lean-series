//! Port of Go `text/template/funcs.go`: the builtin functions and the escapers.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::gofmt::{sprint, sprintf, sprintln};
use crate::value::{Func, IntKind, Kind, StrKind, Type, UintKind, Value, from_slot, indirect, indirect_interface};

fn rv() -> Type {
    Type::ReflectValue
}

fn string_t() -> Type {
    Type::String(StrKind::String)
}

fn unreachable_fn(_: &[Value]) -> Result<Value, String> {
    Err("unreachable".to_string())
}

/// The builtin functions (Go `builtins()`).
pub(crate) fn builtins() -> &'static HashMap<&'static str, Func> {
    static B: OnceLock<HashMap<&'static str, Func>> = OnceLock::new();
    B.get_or_init(|| {
        let mut m: HashMap<&'static str, Func> = HashMap::new();
        m.insert("and", Func::variadic(vec![rv(), rv()], rv(), false, unreachable_fn));
        m.insert("or", Func::variadic(vec![rv(), rv()], rv(), false, unreachable_fn));
        m.insert("call", Func::variadic(vec![rv(), rv()], rv(), false, unreachable_fn));
        m.insert(
            "html",
            Func::variadic(vec![Type::Interface], string_t(), false, |a| Ok(Value::string(html_escape_string(&eval_args(a))))),
        );
        m.insert(
            "js",
            Func::variadic(vec![Type::Interface], string_t(), false, |a| Ok(Value::string(js_escape_string(&eval_args(a))))),
        );
        m.insert(
            "urlquery",
            Func::variadic(vec![Type::Interface], string_t(), false, |a| Ok(Value::string(query_escape(&eval_args(a))))),
        );
        m.insert("index", Func::variadic(vec![rv(), rv()], rv(), true, |a| index(&a[0], &a[1..])));
        m.insert("slice", Func::variadic(vec![rv(), rv()], rv(), true, |a| slice(&a[0], &a[1..])));
        m.insert("len", Func::new(vec![rv()], Type::Int(IntKind::Int), true, |a| length(&a[0]).map(|n| Value::int(n as i64))));
        m.insert("not", Func::new(vec![rv()], Type::Bool, false, |a| Ok(Value::Bool(!truth(&a[0])))));
        m.insert("print", Func::variadic(vec![Type::Interface], string_t(), false, |a| Ok(Value::string(sprint(a)))));
        m.insert(
            "printf",
            Func::variadic(vec![string_t(), Type::Interface], string_t(), false, |a| {
                Ok(Value::string(sprintf(a[0].as_str().unwrap_or(""), &a[1..])))
            }),
        );
        m.insert("println", Func::variadic(vec![Type::Interface], string_t(), false, |a| Ok(Value::string(sprintln(a)))));
        m.insert("eq", Func::variadic(vec![rv(), rv()], Type::Bool, true, |a| eq(&a[0], &a[1..]).map(Value::Bool)));
        m.insert("ne", Func::new(vec![rv(), rv()], Type::Bool, true, |a| ne(&a[0], &a[1]).map(Value::Bool)));
        m.insert("lt", Func::new(vec![rv(), rv()], Type::Bool, true, |a| lt(&a[0], &a[1]).map(Value::Bool)));
        m.insert("le", Func::new(vec![rv(), rv()], Type::Bool, true, |a| le(&a[0], &a[1]).map(Value::Bool)));
        m.insert("gt", Func::new(vec![rv(), rv()], Type::Bool, true, |a| gt(&a[0], &a[1]).map(Value::Bool)));
        m.insert("ge", Func::new(vec![rv(), rv()], Type::Bool, true, |a| ge(&a[0], &a[1]).map(Value::Bool)));
        m
    })
}

/// Whether `name` is a builtin function.
pub fn is_builtin(name: &str) -> bool {
    builtins().contains_key(name)
}

/// Go `printableValue`: the value `fmt` prints for `v` (pointers followed), or `None` for functions.
pub(crate) fn printable_value(v: &Value) -> Option<Value> {
    let v = indirect_interface(v);
    let v = if v.kind() == Kind::Ptr { indirect(&v).0 } else { v };
    if !v.is_valid() {
        return Some(Value::string("<no value>"));
    }
    if v.kind() == Kind::Func {
        return None;
    }
    Some(v)
}

/// Go `evalArgs` (text/template): one string argument as is, else the printable values joined by `fmt.Sprint`.
pub(crate) fn eval_args(args: &[Value]) -> String {
    if args.len() == 1 {
        if let Value::String(s, StrKind::String) = &args[0] {
            return s.to_string();
        }
    }
    let a: Vec<Value> = args.iter().map(|v| printable_value(v).unwrap_or_else(|| v.clone())).collect();
    sprint(&a)
}

/// Go `prepareArg`.
pub(crate) fn prepare_arg(value: &Value, arg_type: &Type) -> Result<Value, String> {
    let value = if !value.is_valid() {
        if !arg_type.can_be_nil() {
            return Err(format!("value is nil; should be of type {}", arg_type));
        }
        arg_type.zero()
    } else {
        value.clone()
    };
    if *arg_type == Type::ReflectValue {
        return Ok(value);
    }
    let vt = value.type_of().unwrap_or(Type::Interface);
    if vt.assignable_to(arg_type) {
        return Ok(value);
    }
    if let (Some(c), true) = (convert_int(&value, arg_type), int_like(&vt) && int_like(arg_type)) {
        return Ok(c);
    }
    Err(format!("value has type {}; should be {}", vt, arg_type))
}

fn int_like(t: &Type) -> bool {
    matches!(t, Type::Int(_) | Type::Uint(_))
}

/// Converts an integer to another integer type (Go `Value.Convert` between integer kinds; truncating).
fn convert_int(v: &Value, to: &Type) -> Option<Value> {
    let bits: u64 = match v {
        Value::Int(i, _) => *i as u64,
        Value::Uint(u, _) => *u,
        _ => return None,
    };
    Some(match to {
        Type::Int(k) => Value::Int(
            match k {
                IntKind::Int8 => bits as i8 as i64,
                IntKind::Int16 => bits as i16 as i64,
                IntKind::Int32 => bits as i32 as i64,
                _ => bits as i64,
            },
            *k,
        ),
        Type::Uint(k) => Value::Uint(
            match k {
                UintKind::Uint8 => bits as u8 as u64,
                UintKind::Uint16 => bits as u16 as u64,
                UintKind::Uint32 => bits as u32 as u64,
                _ => bits,
            },
            *k,
        ),
        _ => return None,
    })
}

fn index_arg(index: &Value, cap: usize) -> Result<usize, String> {
    let x: i64 = match index {
        Value::Int(i, _) => *i,
        Value::Uint(u, _) => *u as i64,
        Value::Invalid => return Err("cannot index slice/array with nil".to_string()),
        _ => return Err(format!("cannot index slice/array with type {}", index.type_name())),
    };
    if x < 0 || x as u64 > cap as u64 {
        return Err(format!("index out of range: {}", x));
    }
    Ok(x as usize)
}

/// Go `index`.
pub(crate) fn index(item: &Value, indexes: &[Value]) -> Result<Value, String> {
    let mut item = indirect_interface(item);
    if !item.is_valid() {
        return Err("index of untyped nil".to_string());
    }
    for index in indexes {
        let index = indirect_interface(index);
        let (it, is_nil) = indirect(&item);
        if is_nil {
            return Err("index of nil pointer".to_string());
        }
        item = it;
        match &item {
            Value::Slice(s) => {
                let x = index_arg(&index, s.items.len())?;
                if x >= s.items.len() {
                    return Err(format!("reflect: slice index out of range"));
                }
                let elem = s.ty.elem().cloned().unwrap_or(Type::Interface);
                item = from_slot(&s.items[x], &elem);
            }
            Value::Nil(t) if t.kind() == Kind::Slice => {
                index_arg(&index, 0)?;
                return Err("reflect: slice index out of range".to_string());
            }
            Value::String(s, _) => {
                let x = index_arg(&index, s.len())?;
                if x >= s.len() {
                    return Err("reflect: string index out of range".to_string());
                }
                item = Value::Uint(s.as_bytes()[x] as u64, UintKind::Uint8);
            }
            Value::Map(m) => {
                let key = prepare_arg(&index, &Type::String(StrKind::String))?;
                let elem = m.ty.elem().cloned().unwrap_or(Type::Interface);
                item = match m.entries.get(key.as_str().unwrap_or("")) {
                    Some(v) => from_slot(v, &elem),
                    None => elem.zero(),
                };
            }
            Value::Nil(t) if t.kind() == Kind::Map => {
                prepare_arg(&index, &Type::String(StrKind::String))?;
                item = t.elem().cloned().unwrap_or(Type::Interface).zero();
            }
            _ => return Err(format!("can't index item of type {}", item.type_name())),
        }
    }
    Ok(item)
}

/// Go `slice`.
pub(crate) fn slice(item: &Value, indexes: &[Value]) -> Result<Value, String> {
    let item = indirect_interface(item);
    if !item.is_valid() {
        return Err("slice of untyped nil".to_string());
    }
    let (item, is_nil) = indirect(&item);
    if is_nil {
        return Err("slice of nil pointer".to_string());
    }
    if indexes.len() > 3 {
        return Err(format!("too many slice indexes: {}", indexes.len()));
    }
    let (cap, len) = match &item {
        Value::String(s, _) => {
            if indexes.len() == 3 {
                return Err("cannot 3-index slice a string".to_string());
            }
            (s.len(), s.len())
        }
        Value::Slice(s) => (s.items.len(), s.items.len()),
        Value::Nil(t) if t.kind() == Kind::Slice => (0, 0),
        _ => return Err(format!("can't slice item of type {}", item.type_name())),
    };
    let mut idx = [0usize, len, 0];
    for (i, index) in indexes.iter().enumerate() {
        idx[i] = index_arg(index, cap)?;
    }
    if idx[0] > idx[1] {
        return Err(format!("invalid slice index: {} > {}", idx[0], idx[1]));
    }
    if indexes.len() == 3 && idx[1] > idx[2] {
        return Err(format!("invalid slice index: {} > {}", idx[1], idx[2]));
    }
    Ok(match &item {
        Value::String(s, k) => match s.get(idx[0]..idx[1]) {
            Some(sub) => Value::String(Arc::from(sub), *k),
            None => Value::String(Arc::from(String::from_utf8_lossy(&s.as_bytes()[idx[0]..idx[1]]).as_ref()), *k),
        },
        Value::Slice(s) => Value::slice_typed(s.ty.clone(), s.items[idx[0]..idx[1]].to_vec()),
        other => other.clone(),
    })
}

/// Go `length`.
pub(crate) fn length(item: &Value) -> Result<usize, String> {
    let (item, is_nil) = indirect(item);
    if is_nil {
        return Err("len of nil pointer".to_string());
    }
    if !item.is_valid() {
        return Err("reflect: call of reflect.Value.Type on zero Value".to_string());
    }
    match item.len() {
        Some(n) => Ok(n),
        None => Err(format!("len of type {}", item.type_name())),
    }
}

/// Go `isTrue`: the truth of a value, and whether it has a meaningful truth value.
pub fn is_true(val: &Value) -> (bool, bool) {
    match val {
        Value::Invalid => (false, true),
        Value::Nil(t) => match t.kind() {
            Kind::Slice | Kind::Map | Kind::Ptr | Kind::Func | Kind::Interface => (false, true),
            _ => (false, false),
        },
        Value::Slice(s) => (!s.items.is_empty(), true),
        Value::Map(m) => (!m.entries.is_empty(), true),
        Value::String(s, _) => (!s.is_empty(), true),
        Value::Bool(b) => (*b, true),
        Value::Complex(re, im) => (*re != 0.0 || *im != 0.0, true),
        Value::Ptr(_) | Value::Func(_) | Value::Iface(_) => (true, true),
        Value::Int(i, _) => (*i != 0, true),
        Value::Float(f) => (*f != 0.0, true),
        Value::Uint(u, _) => (*u != 0, true),
        Value::Struct(_) | Value::Time(_) | Value::Histogram(_) => (true, true),
    }
}

/// Go `truth`.
pub(crate) fn truth(arg: &Value) -> bool {
    is_true(&indirect_interface(arg)).0
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BasicKind {
    Invalid,
    Bool,
    Complex,
    Int,
    Float,
    String,
    Uint,
}

const ERR_BAD_COMPARISON_TYPE: &str = "invalid type for comparison";
const ERR_NO_COMPARISON: &str = "missing argument for comparison";

fn basic_kind(v: &Value) -> Result<BasicKind, &'static str> {
    Ok(match v {
        Value::Bool(_) => BasicKind::Bool,
        Value::Int(..) => BasicKind::Int,
        Value::Uint(..) => BasicKind::Uint,
        Value::Float(_) => BasicKind::Float,
        Value::Complex(..) => BasicKind::Complex,
        Value::String(..) => BasicKind::String,
        _ => return Err(ERR_BAD_COMPARISON_TYPE),
    })
}

fn is_nil(v: &Value) -> bool {
    !v.is_valid() || v.is_nil()
}

fn int_of(v: &Value) -> i64 {
    match v {
        Value::Int(i, _) => *i,
        _ => 0,
    }
}

fn uint_of(v: &Value) -> u64 {
    match v {
        Value::Uint(u, _) => *u,
        _ => 0,
    }
}

fn float_of(v: &Value) -> f64 {
    match v {
        Value::Float(f) => *f,
        _ => 0.0,
    }
}

fn str_of(v: &Value) -> &str {
    v.as_str().unwrap_or("")
}

/// Go `==` on two comparable non-basic values (pointers by identity, structs by fields).
fn interface_eq(a: &Value, b: &Value) -> Result<bool, String> {
    Ok(match (a, b) {
        (Value::Ptr(x), Value::Ptr(y)) => Arc::ptr_eq(x, y),
        (Value::Time(x), Value::Time(y)) => x == y,
        (Value::Struct(x), Value::Struct(y)) => {
            if x.ty != y.ty {
                return Ok(false);
            }
            for (fa, fb) in x.fields.iter().zip(y.fields.iter()) {
                let eq = match (basic_kind(fa), basic_kind(fb)) {
                    (Ok(_), Ok(_)) => eq(fa, std::slice::from_ref(fb))?,
                    _ => interface_eq(fa, fb)?,
                };
                if !eq {
                    return Ok(false);
                }
            }
            true
        }
        (Value::Histogram(x), Value::Histogram(y)) => Arc::ptr_eq(x, y) || **x == **y,
        _ => false,
    })
}

fn comparable(v: &Value) -> bool {
    !matches!(v, Value::Slice(_) | Value::Map(_) | Value::Func(_))
}

/// Go `eq`.
pub(crate) fn eq(arg1: &Value, arg2: &[Value]) -> Result<bool, String> {
    let arg1 = indirect_interface(arg1);
    if arg2.is_empty() {
        return Err(ERR_NO_COMPARISON.to_string());
    }
    let k1 = basic_kind(&arg1).unwrap_or(BasicKind::Invalid);
    for arg in arg2 {
        let arg = indirect_interface(arg);
        let k2 = basic_kind(&arg).unwrap_or(BasicKind::Invalid);
        let mut truth = false;
        if k1 != k2 {
            match (k1, k2) {
                (BasicKind::Int, BasicKind::Uint) => truth = int_of(&arg1) >= 0 && int_of(&arg1) as u64 == uint_of(&arg),
                (BasicKind::Uint, BasicKind::Int) => truth = int_of(&arg) >= 0 && uint_of(&arg1) == int_of(&arg) as u64,
                _ => {
                    if arg1.is_valid() && arg.is_valid() {
                        return Err(format!(
                            "incompatible types for comparison: {} and {}",
                            arg1.type_name(),
                            arg.type_name()
                        ));
                    }
                }
            }
        } else {
            match k1 {
                BasicKind::Bool => truth = matches!((&arg1, &arg), (Value::Bool(a), Value::Bool(b)) if a == b),
                BasicKind::Complex => {
                    truth = matches!((&arg1, &arg), (Value::Complex(a, b), Value::Complex(c, d)) if a == c && b == d)
                }
                BasicKind::Float => truth = float_of(&arg1) == float_of(&arg),
                BasicKind::Int => truth = int_of(&arg1) == int_of(&arg),
                BasicKind::String => truth = str_of(&arg1) == str_of(&arg),
                BasicKind::Uint => truth = uint_of(&arg1) == uint_of(&arg),
                BasicKind::Invalid => {
                    let can_compare = arg1.kind() == arg.kind() || !arg1.is_valid() || !arg.is_valid();
                    if !can_compare {
                        return Err(format!(
                            "non-comparable types {}: {}, {}: {}",
                            crate::gofmt::sprint_one(&arg1),
                            arg1.type_name(),
                            arg.type_name(),
                            crate::gofmt::sprint_one(&arg)
                        ));
                    }
                    if is_nil(&arg1) || is_nil(&arg) {
                        truth = is_nil(&arg) == is_nil(&arg1);
                    } else {
                        if !comparable(&arg) {
                            return Err(format!(
                                "non-comparable type {}: {}",
                                crate::gofmt::sprint_one(&arg),
                                arg.type_name()
                            ));
                        }
                        truth = interface_eq(&arg1, &arg)?;
                    }
                }
            }
        }
        if truth {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Go `ne`.
pub(crate) fn ne(arg1: &Value, arg2: &Value) -> Result<bool, String> {
    eq(arg1, std::slice::from_ref(arg2)).map(|e| !e)
}

/// Go `lt`.
pub(crate) fn lt(arg1: &Value, arg2: &Value) -> Result<bool, String> {
    let arg1 = indirect_interface(arg1);
    let k1 = basic_kind(&arg1)?;
    let arg2 = indirect_interface(arg2);
    let k2 = basic_kind(&arg2)?;
    if k1 != k2 {
        return match (k1, k2) {
            (BasicKind::Int, BasicKind::Uint) => Ok(int_of(&arg1) < 0 || (int_of(&arg1) as u64) < uint_of(&arg2)),
            (BasicKind::Uint, BasicKind::Int) => Ok(int_of(&arg2) >= 0 && uint_of(&arg1) < int_of(&arg2) as u64),
            _ => Err(format!("incompatible types for comparison: {} and {}", arg1.type_name(), arg2.type_name())),
        };
    }
    match k1 {
        BasicKind::Bool | BasicKind::Complex => Err(ERR_BAD_COMPARISON_TYPE.to_string()),
        BasicKind::Float => Ok(float_of(&arg1) < float_of(&arg2)),
        BasicKind::Int => Ok(int_of(&arg1) < int_of(&arg2)),
        BasicKind::String => Ok(str_of(&arg1) < str_of(&arg2)),
        BasicKind::Uint => Ok(uint_of(&arg1) < uint_of(&arg2)),
        BasicKind::Invalid => Err(ERR_BAD_COMPARISON_TYPE.to_string()),
    }
}

/// Go `le`.
pub(crate) fn le(arg1: &Value, arg2: &Value) -> Result<bool, String> {
    let less = lt(arg1, arg2)?;
    if less {
        return Ok(true);
    }
    eq(arg1, std::slice::from_ref(arg2))
}

/// Go `gt`.
pub(crate) fn gt(arg1: &Value, arg2: &Value) -> Result<bool, String> {
    Ok(!le(arg1, arg2)?)
}

/// Go `ge`.
pub(crate) fn ge(arg1: &Value, arg2: &Value) -> Result<bool, String> {
    Ok(!lt(arg1, arg2)?)
}

/// Go `template.HTMLEscapeString`.
pub fn html_escape_string(s: &str) -> String {
    if !s.contains(['\'', '"', '&', '<', '>', '\0']) {
        return s.to_string();
    }
    let mut b = String::with_capacity(s.len() + 16);
    for c in s.chars() {
        match c {
            '\0' => b.push('\u{FFFD}'),
            '"' => b.push_str("&#34;"),
            '\'' => b.push_str("&#39;"),
            '&' => b.push_str("&amp;"),
            '<' => b.push_str("&lt;"),
            '>' => b.push_str("&gt;"),
            _ => b.push(c),
        }
    }
    b
}

fn js_is_special(r: char) -> bool {
    matches!(r, '\\' | '\'' | '"' | '<' | '>' | '&' | '=') || r < ' ' || (r as u32) >= 0x80
}

/// Go `template.JSEscapeString`.
pub fn js_escape_string(s: &str) -> String {
    if !s.chars().any(js_is_special) {
        return s.to_string();
    }
    const HEX: &[u8] = b"0123456789ABCDEF";
    let mut b = String::with_capacity(s.len() + 16);
    for c in s.chars() {
        if !js_is_special(c) {
            b.push(c);
            continue;
        }
        if (c as u32) < 0x80 {
            match c {
                '\\' => b.push_str("\\\\"),
                '\'' => b.push_str("\\'"),
                '"' => b.push_str("\\\""),
                '<' => b.push_str("\\u003C"),
                '>' => b.push_str("\\u003E"),
                '&' => b.push_str("\\u0026"),
                '=' => b.push_str("\\u003D"),
                _ => {
                    let v = c as u32;
                    b.push_str("\\u00");
                    b.push(HEX[(v >> 4) as usize] as char);
                    b.push(HEX[(v & 0xF) as usize] as char);
                }
            }
        } else if model::unicode::is_print(c) {
            b.push(c);
        } else {
            b.push_str(&format!("\\u{:04X}", c as u32));
        }
    }
    b
}

fn should_escape_query(c: u8) -> bool {
    !(c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.' | b'~'))
}

/// Go `url.QueryEscape`.
pub fn query_escape(s: &str) -> String {
    const HEX: &[u8] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len() + 8);
    for &c in s.as_bytes() {
        if c == b' ' {
            out.push('+');
        } else if should_escape_query(c) {
            out.push('%');
            out.push(HEX[(c >> 4) as usize] as char);
            out.push(HEX[(c & 15) as usize] as char);
        } else {
            out.push(c as char);
        }
    }
    out
}
