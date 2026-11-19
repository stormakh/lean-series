//! Go `strconv` parsing that the template parser needs: `newNumber` (ParseInt/ParseUint with base 0,
//! ParseFloat, complex constants), `Unquote` and `UnquoteChar`.

use super::lex::ItemType;
use super::node::NumberNode;
use crate::gofmt::quote;

const ERR_SYNTAX: &str = "invalid syntax";

/// Go `strconv.Unquote`.
pub fn unquote(s: &str) -> Result<String, &'static str> {
    let b = s.as_bytes();
    let n = b.len();
    if n < 2 {
        return Err(ERR_SYNTAX);
    }
    let q = b[0];
    if q != b[n - 1] {
        return Err(ERR_SYNTAX);
    }
    let inner = &s[1..n - 1];
    match q {
        b'`' => {
            if inner.contains('`') {
                return Err(ERR_SYNTAX);
            }
            if inner.contains('\r') {
                return Ok(inner.replace('\r', ""));
            }
            Ok(inner.to_string())
        }
        b'"' | b'\'' => {
            if q == b'\'' {
                // A single rune.
                let (r, tail) = unquote_char(inner, q)?;
                if !tail.is_empty() {
                    return Err(ERR_SYNTAX);
                }
                let mut out = String::new();
                push_rune(&mut out, r);
                return Ok(out);
            }
            if !inner.contains('\\') && !inner.contains('\n') {
                return Ok(inner.to_string());
            }
            let mut out = String::with_capacity(inner.len());
            let mut rest = inner;
            while !rest.is_empty() {
                let (r, tail) = unquote_char(rest, q)?;
                push_rune(&mut out, r);
                rest = tail;
            }
            Ok(out)
        }
        _ => Err(ERR_SYNTAX),
    }
}

fn push_rune(out: &mut String, r: u32) {
    out.push(char::from_u32(r).unwrap_or('\u{FFFD}'));
}

fn unhex(c: u8) -> Option<u32> {
    match c {
        b'0'..=b'9' => Some((c - b'0') as u32),
        b'a'..=b'f' => Some((c - b'a' + 10) as u32),
        b'A'..=b'F' => Some((c - b'A' + 10) as u32),
        _ => None,
    }
}

/// Go `strconv.UnquoteChar`: decodes the first character or escape of `s`. Byte escapes (`\x`, octal) that
/// are not valid runes become U+FFFD when the result is a string.
pub fn unquote_char(s: &str, quote: u8) -> Result<(u32, &str), &'static str> {
    let b = s.as_bytes();
    let Some(&c) = b.first() else { return Err(ERR_SYNTAX) };
    if c == quote && (quote == b'\'' || quote == b'"') {
        return Err(ERR_SYNTAX);
    }
    if c >= 0x80 {
        let ch = s.chars().next().ok_or(ERR_SYNTAX)?;
        return Ok((ch as u32, &s[ch.len_utf8()..]));
    }
    if c != b'\\' {
        return Ok((c as u32, &s[1..]));
    }
    if b.len() <= 1 {
        return Err(ERR_SYNTAX);
    }
    let c = b[1];
    let s = &s[2..];
    let v = match c {
        b'a' => 7,
        b'b' => 8,
        b'f' => 12,
        b'n' => 10,
        b'r' => 13,
        b't' => 9,
        b'v' => 11,
        b'x' | b'u' | b'U' => {
            let n = match c {
                b'x' => 2,
                b'u' => 4,
                _ => 8,
            };
            if s.len() < n {
                return Err(ERR_SYNTAX);
            }
            let mut v = 0u32;
            for &d in &s.as_bytes()[..n] {
                v = (v << 4) | unhex(d).ok_or(ERR_SYNTAX)?;
            }
            if c != b'x' && (v > 0x10FFFF || (0xD800..0xE000).contains(&v)) {
                return Err(ERR_SYNTAX);
            }
            return Ok((v, &s[n..]));
        }
        b'0'..=b'7' => {
            let mut v = (c - b'0') as u32;
            if s.len() < 2 {
                return Err(ERR_SYNTAX);
            }
            for &d in &s.as_bytes()[..2] {
                if !(b'0'..=b'7').contains(&d) {
                    return Err(ERR_SYNTAX);
                }
                v = (v << 3) | (d - b'0') as u32;
            }
            if v > 255 {
                return Err(ERR_SYNTAX);
            }
            return Ok((v, &s[2..]));
        }
        b'\\' => b'\\' as u32,
        b'\'' | b'"' => {
            if c != quote {
                return Err(ERR_SYNTAX);
            }
            c as u32
        }
        _ => return Err(ERR_SYNTAX),
    };
    Ok((v, s))
}

/// Go `underscoreOK`.
fn underscore_ok(s: &str) -> bool {
    let mut b = s.as_bytes();
    let mut saw = b'^';
    let mut i = 0;
    if !b.is_empty() && (b[0] == b'-' || b[0] == b'+') {
        b = &b[1..];
    }
    let mut hex = false;
    if b.len() >= 2 && b[0] == b'0' && matches!(b[1] | 0x20, b'b' | b'o' | b'x') {
        i = 2;
        saw = b'0';
        hex = b[1] | 0x20 == b'x';
    }
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_digit() || (hex && (b'a'..=b'f').contains(&(c | 0x20))) {
            saw = b'0';
            i += 1;
            continue;
        }
        if c == b'_' {
            if saw != b'0' {
                return false;
            }
            saw = b'_';
            i += 1;
            continue;
        }
        if saw == b'_' {
            return false;
        }
        saw = b'!';
        i += 1;
    }
    saw != b'_'
}

/// Go `strconv.ParseUint(s, 0, 64)`.
fn parse_uint0(s: &str) -> Option<u64> {
    if s.is_empty() {
        return None;
    }
    let s0 = s;
    let b = s.as_bytes();
    let (base, digits) = if b[0] == b'0' {
        if b.len() >= 3 && matches!(b[1] | 0x20, b'b' | b'o' | b'x') {
            (
                match b[1] | 0x20 {
                    b'b' => 2,
                    b'o' => 8,
                    _ => 16,
                },
                &s[2..],
            )
        } else {
            (8, &s[1..])
        }
    } else {
        (10, s)
    };
    let mut n: u64 = 0;
    let mut underscores = false;
    for c in digits.bytes() {
        if c == b'_' {
            underscores = true;
            continue;
        }
        let d = match c {
            b'0'..=b'9' => (c - b'0') as u64,
            b'a'..=b'z' => (c - b'a' + 10) as u64,
            b'A'..=b'Z' => (c - b'A' + 10) as u64,
            _ => return None,
        };
        if d >= base {
            return None;
        }
        n = n.checked_mul(base)?.checked_add(d)?;
    }
    if underscores && !underscore_ok(s0) {
        return None;
    }
    Some(n)
}

/// Go `strconv.ParseInt(s, 0, 64)`.
fn parse_int0(s: &str) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let (neg, body) = match s.as_bytes()[0] {
        b'+' => (false, &s[1..]),
        b'-' => (true, &s[1..]),
        _ => (false, s),
    };
    let un = parse_uint0(body)?;
    if body.contains('_') && !underscore_ok(s) {
        return None;
    }
    if !neg {
        if un > i64::MAX as u64 {
            return None;
        }
        Some(un as i64)
    } else {
        if un > 1u64 << 63 {
            return None;
        }
        Some((un as i64).wrapping_neg())
    }
}

/// Go `strconv.ParseFloat(s, 64)` for the number syntax the lexer accepts (no inf/nan).
pub fn parse_float(s: &str) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    if s.contains('_') && !underscore_ok(s) {
        return None;
    }
    let clean: String = s.chars().filter(|&c| c != '_').collect();
    let (neg, body) = match clean.as_bytes().first() {
        Some(b'+') => (false, &clean[1..]),
        Some(b'-') => (true, &clean[1..]),
        _ => (false, &clean[..]),
    };
    let lower = body.to_ascii_lowercase();
    if lower.starts_with("inf") || lower.starts_with("nan") || lower.is_empty() {
        return None;
    }
    let v = if lower.starts_with("0x") {
        parse_hex_float(&lower[2..])?
    } else {
        if !lower.bytes().all(|c| c.is_ascii_digit() || matches!(c, b'.' | b'e' | b'+' | b'-')) {
            return None;
        }
        let v: f64 = lower.parse().ok()?;
        if v.is_infinite() {
            return None; // Go: value out of range.
        }
        v
    };
    Some(if neg { -v } else { v })
}

fn parse_hex_float(s: &str) -> Option<f64> {
    let (mant, exp) = s.split_once('p')?;
    let (ip, fp) = mant.split_once('.').unwrap_or((mant, ""));
    if ip.is_empty() && fp.is_empty() {
        return None;
    }
    let mut v = 0f64;
    for c in ip.bytes().chain(fp.bytes()) {
        v = v * 16.0 + unhex(c)? as f64;
    }
    let e: i32 = exp.parse().ok()?;
    let r = v * 2f64.powi(e - 4 * fp.len() as i32);
    if r.is_infinite() {
        return None;
    }
    Some(r)
}

/// Go `float64(int64(f)) == f` (an out-of-range conversion never round-trips on amd64).
fn int_exact(f: f64) -> bool {
    (-9.223372036854775808e18..9.223372036854775808e18).contains(&f) && (f as i64) as f64 == f
}

/// Go `float64(uint64(f)) == f`.
fn uint_exact(f: f64) -> bool {
    (0.0..1.8446744073709551616e19).contains(&f.abs()) && f > -1.0 && (f as u64) as f64 == f
}

fn simplify_complex(n: &mut NumberNode) {
    n.is_float = n.complex128.1 == 0.0;
    if n.is_float {
        n.float64 = n.complex128.0;
        n.is_int = int_exact(n.float64);
        if n.is_int {
            n.int64 = n.float64 as i64;
        }
        n.is_uint = uint_exact(n.float64);
        if n.is_uint {
            n.uint64 = n.float64 as u64;
        }
    }
}

/// Go `Tree.newNumber`.
pub fn new_number(pos: usize, text: &str, typ: ItemType) -> Result<NumberNode, String> {
    let mut n = NumberNode { pos, text: text.to_string(), ..Default::default() };
    match typ {
        ItemType::CharConstant => {
            let (r, tail) = unquote_char(&text[1..], text.as_bytes()[0]).map_err(str::to_string)?;
            if tail != "'" {
                return Err(format!("malformed character constant: {}", text));
            }
            n.int64 = r as i64;
            n.is_int = true;
            n.uint64 = r as u64;
            n.is_uint = true;
            n.float64 = r as f64;
            n.is_float = true;
            return Ok(n);
        }
        ItemType::Complex => {
            // fmt.Sscan of "re+imi".
            let b = text.as_bytes();
            let mut split = None;
            for i in (1..b.len()).rev() {
                if (b[i] == b'+' || b[i] == b'-') && !matches!(b[i - 1] | 0x20, b'e' | b'p') {
                    split = Some(i);
                    break;
                }
            }
            let i = split.ok_or_else(|| "expected integer".to_string())?;
            let re = parse_float(&text[..i]).ok_or_else(|| "expected integer".to_string())?;
            let im_text = text[i..].strip_suffix('i').unwrap_or(&text[i..]);
            let im = parse_float(im_text).ok_or_else(|| "expected integer".to_string())?;
            n.complex128 = (re, im);
            n.is_complex = true;
            simplify_complex(&mut n);
            return Ok(n);
        }
        _ => {}
    }
    if let Some(stripped) = text.strip_suffix('i') {
        if let Some(f) = parse_float(stripped) {
            n.is_complex = true;
            n.complex128 = (0.0, f);
            simplify_complex(&mut n);
            return Ok(n);
        }
    }
    let u = parse_uint0(text);
    if let Some(u) = u {
        n.is_uint = true;
        n.uint64 = u;
    }
    if let Some(i) = parse_int0(text) {
        n.is_int = true;
        n.int64 = i;
        if i == 0 {
            n.is_uint = true;
            n.uint64 = u.unwrap_or(0);
        }
    }
    if n.is_int {
        n.is_float = true;
        n.float64 = n.int64 as f64;
    } else if n.is_uint {
        n.is_float = true;
        n.float64 = n.uint64 as f64;
    } else if let Some(f) = parse_float(text) {
        if !text.contains(['.', 'e', 'E', 'p', 'P']) {
            return Err(format!("integer overflow: {}", quote(text)));
        }
        n.is_float = true;
        n.float64 = f;
        if !n.is_int && int_exact(f) {
            n.is_int = true;
            n.int64 = f as i64;
        }
        if !n.is_uint && uint_exact(f) {
            n.is_uint = true;
            n.uint64 = f as u64;
        }
    }
    if !n.is_int && !n.is_uint && !n.is_float {
        return Err(format!("illegal number syntax: {}", quote(text)));
    }
    Ok(n)
}
