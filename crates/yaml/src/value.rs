//! A YAML value: what Go `interface{}` decodes into, and the tree that marshaling builds before emitting.
use crate::decode::go_quote;

#[derive(Debug, Clone, PartialEq, Default)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    Str(String),
    Seq(Vec<Value>),
    /// Pairs in emit order. Decoded Go maps are sorted with go-yaml's key order.
    Map(Vec<(Value, Value)>),
}

impl Value {
    /// Go `%#v` of the value as a map key.
    pub fn key_debug(&self) -> String {
        match self {
            Value::Null => "<nil>".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Uint(u) => format!("0x{u:x}"),
            Value::Float(f) => crate::encode::format_float_g(*f),
            Value::Str(s) => go_quote(s),
            other => format!("{other:?}"),
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Value of a key in a mapping.
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Map(p) => p.iter().find(|(k, _)| k.as_str() == Some(key)).map(|(_, v)| v),
            _ => None,
        }
    }
}

// reflect.Kind order used by go-yaml's key sorter for non-number kinds
fn kind_rank(v: &Value) -> u8 {
    match v {
        Value::Bool(_) => 1,
        Value::Int(_) => 2,
        Value::Uint(_) => 7,
        Value::Float(_) => 14,
        Value::Null => 20, // interface
        Value::Map(_) => 21,
        Value::Seq(_) => 23,
        Value::Str(_) => 24,
    }
}

fn key_float(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Uint(u) => Some(*u as f64),
        Value::Float(f) => Some(*f),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn num_less(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x < y,
        (Value::Uint(x), Value::Uint(y)) => x < y,
        (Value::Float(x), Value::Float(y)) => x < y,
        (Value::Bool(x), Value::Bool(y)) => !*x && *y,
        _ => false,
    }
}

/// go-yaml sorter.go `keyList.Less`: numbers by value, strings with digit runs compared as numbers and letters
/// before other characters.
pub fn key_less(a: &Value, b: &Value) -> bool {
    if let (Some(af), Some(bf)) = (key_float(a), key_float(b)) {
        if af != bf {
            return af < bf;
        }
        let (ak, bk) = (kind_rank(a), kind_rank(b));
        if ak != bk {
            return ak < bk;
        }
        return num_less(a, b);
    }
    match (a, b) {
        (Value::Str(x), Value::Str(y)) => str_less(x, y),
        _ => kind_rank(a) < kind_rank(b),
    }
}

/// The string part of `keyList.Less`.
pub fn str_less(a: &str, b: &str) -> bool {
    let ar: Vec<char> = a.chars().collect();
    let br: Vec<char> = b.chars().collect();
    let mut i = 0;
    while i < ar.len() && i < br.len() {
        if ar[i] == br[i] {
            i += 1;
            continue;
        }
        let al = ar[i].is_alphabetic();
        let bl = br[i].is_alphabetic();
        if al && bl {
            return ar[i] < br[i];
        }
        if al || bl {
            return bl;
        }
        let (mut an, mut bn): (i64, i64) = (0, 0);
        if ar[i] == '0' || br[i] == '0' {
            let mut j = i as isize - 1;
            while j >= 0 && ar[j as usize].is_ascii_digit() {
                if ar[j as usize] != '0' {
                    an = 1;
                    bn = 1;
                    break;
                }
                j -= 1;
            }
        }
        let mut ai = i;
        while ai < ar.len() && ar[ai].is_ascii_digit() {
            an = an.wrapping_mul(10).wrapping_add(ar[ai] as i64 - '0' as i64);
            ai += 1;
        }
        let mut bi = i;
        while bi < br.len() && br[bi].is_ascii_digit() {
            bn = bn.wrapping_mul(10).wrapping_add(br[bi] as i64 - '0' as i64);
            bi += 1;
        }
        if an != bn {
            return an < bn;
        }
        if ai != bi {
            return ai < bi;
        }
        return ar[i] < br[i];
    }
    ar.len() < br.len()
}

/// Sorts mapping pairs by go-yaml's key order (stable, as Go's sort.Sort is not but keys are unique).
pub fn sort_pairs(p: &mut [(Value, Value)]) {
    p.sort_by(|a, b| {
        if key_less(&a.0, &b.0) {
            std::cmp::Ordering::Less
        } else if key_less(&b.0, &a.0) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
}
