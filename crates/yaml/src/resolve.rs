//! Plain scalar resolution of go-yaml v2 (resolve.go): YAML 1.1 booleans (yes/no/on/off), null, ints with
//! 0x/0o/0b/leading-0 octal and underscores, floats, timestamps and the merge key.
use crate::node::*;

#[derive(Debug, Clone, PartialEq)]
pub enum Resolved {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    Str(String),
    /// A timestamp-looking plain scalar (decoded into strings and `any` as its text).
    Timestamp,
    Merge,
}

fn resolvable_tag(tag: &str) -> bool {
    matches!(tag, "" | STR_TAG | BOOL_TAG | INT_TAG | FLOAT_TAG | NULL_TAG | TIMESTAMP_TAG)
}

fn resolve_map(s: &str) -> Option<(&'static str, Resolved)> {
    Some(match s {
        "y" | "Y" | "yes" | "Yes" | "YES" | "true" | "True" | "TRUE" | "on" | "On" | "ON" => (BOOL_TAG, Resolved::Bool(true)),
        "n" | "N" | "no" | "No" | "NO" | "false" | "False" | "FALSE" | "off" | "Off" | "OFF" => (BOOL_TAG, Resolved::Bool(false)),
        "" | "~" | "null" | "Null" | "NULL" => (NULL_TAG, Resolved::Null),
        ".nan" | ".NaN" | ".NAN" => (FLOAT_TAG, Resolved::Float(f64::NAN)),
        ".inf" | ".Inf" | ".INF" | "+.inf" | "+.Inf" | "+.INF" => (FLOAT_TAG, Resolved::Float(f64::INFINITY)),
        "-.inf" | "-.Inf" | "-.INF" => (FLOAT_TAG, Resolved::Float(f64::NEG_INFINITY)),
        "<<" => (MERGE_TAG, Resolved::Merge),
        _ => return None,
    })
}

pub fn short_tag(tag: &str) -> String {
    match tag.strip_prefix("tag:yaml.org,2002:") {
        Some(s) => format!("!!{s}"),
        None => tag.to_string(),
    }
}

/// Go `strconv.ParseInt(s, 0, 64)`: sign, then 0x/0o/0b prefixes or a leading 0 for octal. `None` on
/// syntax error; `Err(())` on overflow (Go returns an error too, so callers try ParseUint).
pub fn parse_int_base0(s: &str) -> Option<i64> {
    let (neg, body) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let u = parse_uint_base0(body)?;
    if neg {
        if u > (i64::MAX as u64) + 1 {
            return None;
        }
        Some((u as i128).wrapping_neg() as i64)
    } else {
        if u > i64::MAX as u64 {
            return None;
        }
        Some(u as i64)
    }
}

/// Go `strconv.ParseUint(s, 0, 64)` (no sign allowed). Underscores only with a base prefix, as Go.
pub fn parse_uint_base0(s: &str) -> Option<u64> {
    if s.is_empty() {
        return None;
    }
    let b = s.as_bytes();
    let (base, digits, prefixed) = if b[0] == b'0' && b.len() > 1 {
        match b[1] {
            b'x' | b'X' => (16, &s[2..], true),
            b'o' | b'O' => (8, &s[2..], true),
            b'b' | b'B' => (2, &s[2..], true),
            _ => (8, &s[1..], true),
        }
    } else {
        (10, s, false)
    };
    if digits.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    let mut saw_digit = false;
    let mut last_us = false;
    for (i, c) in digits.bytes().enumerate() {
        if c == b'_' {
            if !prefixed || last_us {
                return None;
            }
            // Go allows '_' only between digits (or after the prefix)
            last_us = true;
            let _ = i;
            continue;
        }
        last_us = false;
        let d = match c {
            b'0'..=b'9' => (c - b'0') as u64,
            b'a'..=b'z' => (c - b'a' + 10) as u64,
            b'A'..=b'Z' => (c - b'A' + 10) as u64,
            _ => return None,
        };
        if d >= base {
            return None;
        }
        saw_digit = true;
        v = v.checked_mul(base)?.checked_add(d)?;
    }
    if !saw_digit || last_us {
        return None;
    }
    Some(v)
}

/// Go `strconv.ParseFloat(s, 64)` for the forms YAML hands it (decimal, exponent, inf/nan words, hex floats are
/// rare enough to be refused).
pub fn parse_go_float(s: &str) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    let body = s.trim_start_matches(['+', '-']);
    if s.len() - body.len() > 1 {
        return None;
    }
    let lower = body.to_ascii_lowercase();
    if lower == "inf" || lower == "infinity" || lower == "nan" {
        let v = if lower == "nan" { f64::NAN } else { f64::INFINITY };
        return Some(if s.starts_with('-') { -v } else { v });
    }
    // Rust accepts the same decimal syntax (digits, '.', exponent); Go also allows '_' only with a base prefix.
    if !body.bytes().all(|c| c.is_ascii_digit() || matches!(c, b'.' | b'e' | b'E' | b'+' | b'-')) {
        return None;
    }
    s.parse::<f64>().ok()
}

/// `^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$`
fn yaml_style_float(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    if i < b.len() && (b[i] == b'-' || b[i] == b'+') {
        i += 1;
    }
    let digits = |i: &mut usize| {
        let st = *i;
        while *i < b.len() && b[*i].is_ascii_digit() {
            *i += 1;
        }
        *i - st
    };
    if i < b.len() && b[i] == b'.' {
        i += 1;
        if digits(&mut i) == 0 {
            return false;
        }
    } else {
        if digits(&mut i) == 0 {
            return false;
        }
        if i < b.len() && b[i] == b'.' {
            i += 1;
            digits(&mut i);
        }
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        if i < b.len() && (b[i] == b'-' || b[i] == b'+') {
            i += 1;
        }
        if digits(&mut i) == 0 {
            return false;
        }
    }
    i == b.len()
}

/// go-yaml's parseTimestamp: four digits and '-', then one of the allowed layouts.
pub fn is_timestamp(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    if i != 4 || i == b.len() || b[i] != b'-' {
        return false;
    }
    // layouts: "2006-1-2T15:4:5.999999999Z07:00", "2006-1-2t15:4:5.999999999Z07:00", "2006-1-2 15:4:5.999999999", "2006-1-2"
    let mut p = 5;
    let num = |p: &mut usize, max: usize| -> Option<u32> {
        let st = *p;
        while *p < b.len() && b[*p].is_ascii_digit() && *p - st < max {
            *p += 1;
        }
        if *p == st { None } else { s[st..*p].parse().ok() }
    };
    let month = match num(&mut p, 2) {
        Some(m) if (1..=12).contains(&m) => m,
        _ => return false,
    };
    if p >= b.len() || b[p] != b'-' {
        return false;
    }
    p += 1;
    let day = match num(&mut p, 2) {
        Some(d) => d,
        None => return false,
    };
    let year: u32 = s[..4].parse().unwrap_or(0);
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let mdays = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if day < 1 || day > mdays[month as usize - 1] {
        return false;
    }
    if p == b.len() {
        return true;
    }
    let sep = b[p];
    if sep != b'T' && sep != b't' && sep != b' ' {
        return false;
    }
    p += 1;
    let hh = num(&mut p, 2);
    if hh.is_none_or(|h| h > 23) || p >= b.len() || b[p] != b':' {
        return false;
    }
    p += 1;
    if num(&mut p, 2).is_none_or(|m| m > 59) || p >= b.len() || b[p] != b':' {
        return false;
    }
    p += 1;
    if num(&mut p, 2).is_none_or(|s| s > 59) {
        return false;
    }
    if p < b.len() && b[p] == b'.' {
        p += 1;
        let st = p;
        while p < b.len() && b[p].is_ascii_digit() {
            p += 1;
        }
        if p == st {
            return false;
        }
    }
    if sep == b' ' {
        return p == b.len();
    }
    // zone: Z or ±hh:mm
    if p < b.len() && b[p] == b'Z' {
        return p + 1 == b.len();
    }
    if p < b.len() && (b[p] == b'+' || b[p] == b'-') {
        let z = &s[p + 1..];
        let zb = z.as_bytes();
        return zb.len() == 5 && zb[0].is_ascii_digit() && zb[1].is_ascii_digit() && zb[2] == b':' && zb[3].is_ascii_digit() && zb[4].is_ascii_digit();
    }
    false
}

/// go-yaml `resolve(tag, in)`. `Err` carries the fatal "cannot decode" message.
pub fn resolve(tag: &str, input: &str) -> Result<(String, Resolved), String> {
    if !resolvable_tag(tag) {
        return Ok((tag.to_string(), Resolved::Str(input.to_string())));
    }
    let (rtag, out) = resolve_inner(tag, input);
    match tag {
        "" | STR_TAG | BINARY_TAG => return Ok((rtag.to_string(), out)),
        t if t == rtag => return Ok((rtag.to_string(), out)),
        FLOAT_TAG => {
            if rtag == INT_TAG
                && let Resolved::Int(v) = out {
                    return Ok((FLOAT_TAG.to_string(), Resolved::Float(v as f64)));
                }
        }
        _ => {}
    }
    Err(format!("cannot decode {} `{}` as a {}", short_tag(rtag), input, short_tag(tag)))
}

fn resolve_inner(tag: &str, input: &str) -> (&'static str, Resolved) {
    let hint = match input.as_bytes().first() {
        None => b'N',
        Some(c) => match c {
            b'+' | b'-' => b'S',
            b'0'..=b'9' => b'D',
            b'y' | b'Y' | b'n' | b'N' | b't' | b'T' | b'f' | b'F' | b'o' | b'O' | b'~' => b'M',
            b'.' => b'.',
            _ => 0,
        },
    };
    if hint != 0 && tag != STR_TAG && tag != BINARY_TAG {
        if let Some((t, v)) = resolve_map(input) {
            return (t, v);
        }
        match hint {
            b'.' => {
                if let Some(f) = parse_go_float(input) {
                    return (FLOAT_TAG, Resolved::Float(f));
                }
            }
            b'D' | b'S' => {
                if (tag.is_empty() || tag == TIMESTAMP_TAG) && is_timestamp(input) {
                    return (TIMESTAMP_TAG, Resolved::Timestamp);
                }
                let plain = input.replace('_', "");
                if let Some(i) = parse_int_base0(&plain) {
                    return (INT_TAG, Resolved::Int(i));
                }
                if let Some(u) = parse_uint_base0(&plain) {
                    return (INT_TAG, Resolved::Uint(u));
                }
                if yaml_style_float(&plain)
                    && let Some(f) = parse_go_float(&plain) {
                        return (FLOAT_TAG, Resolved::Float(f));
                    }
                if let Some(rest) = plain.strip_prefix("0b") {
                    if let Some(u) = parse_bin(rest) {
                        if u <= i64::MAX as u64 {
                            return (INT_TAG, Resolved::Int(u as i64));
                        }
                        return (INT_TAG, Resolved::Uint(u));
                    }
                } else if let Some(rest) = plain.strip_prefix("-0b")
                    && let Some(u) = parse_bin(rest)
                        && u <= (i64::MAX as u64) + 1 {
                            return (INT_TAG, Resolved::Int((u as i128).wrapping_neg() as i64));
                        }
            }
            _ => {}
        }
    }
    (STR_TAG, Resolved::Str(input.to_string()))
}

fn parse_bin(s: &str) -> Option<u64> {
    if s.is_empty() || s.starts_with('+') || s.starts_with('-') {
        return None;
    }
    u64::from_str_radix(s, 2).ok()
}

/// True when `s` written as a plain scalar would read back as the same string (encode.go `stringv`).
pub fn plain_is_str(s: &str) -> bool {
    let (t, _) = resolve_inner("", s);
    t == STR_TAG && !is_base60_float(s)
}

/// `^[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+(?:\.[0-9_]*)?$`
pub fn is_base60_float(s: &str) -> bool {
    let b = s.as_bytes();
    if b.is_empty() {
        return false;
    }
    let c = b[0];
    if !(c == b'+' || c == b'-' || c.is_ascii_digit()) || !s.contains(':') {
        return false;
    }
    let mut i = 0;
    if b[i] == b'+' || b[i] == b'-' {
        i += 1;
    }
    if i >= b.len() || !b[i].is_ascii_digit() {
        return false;
    }
    i += 1;
    while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'_') {
        i += 1;
    }
    let mut groups = 0;
    while i < b.len() && b[i] == b':' {
        i += 1;
        // [0-5]?[0-9]
        if i + 1 < b.len() && (b'0'..=b'5').contains(&b[i]) && b[i + 1].is_ascii_digit() {
            i += 2;
        } else if i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        } else {
            return false;
        }
        groups += 1;
    }
    if groups == 0 {
        return false;
    }
    if i < b.len() && b[i] == b'.' {
        i += 1;
        while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'_') {
            i += 1;
        }
    }
    i == b.len()
}
