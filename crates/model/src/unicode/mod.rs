//! The parts of Go's `unicode`, `strings`, `strconv` and `golang.org/x/text/unicode/norm` packages that the
//! model code needs, with tables generated from the Go toolchain (see `gen/unicode_tables`), so that case
//! folding, case mapping, printability and normalization match Go exactly.

use std::borrow::Cow;

mod tables;

pub use tables::UNICODE_VERSION;

/// Maximum valid Unicode code point.
pub const MAX_RUNE: i32 = 0x10FFFF;
/// The Unicode replacement character.
pub const REPLACEMENT_CHAR: char = '\u{FFFD}';

/// A Go `unicode.RangeTable` flattened to `(lo, hi, stride)` triples (R16 entries first, then R32).
pub type RangeTable = &'static [(u32, u32, u32)];

fn find_table(list: &'static [(&'static str, RangeTable)], name: &str) -> Option<RangeTable> {
    list.binary_search_by(|(n, _)| (*n).cmp(name)).ok().map(|i| list[i].1)
}

/// Go `unicode.Categories[name]`.
pub fn category(name: &str) -> Option<RangeTable> {
    find_table(tables::CATEGORIES, name)
}

/// Go `unicode.Scripts[name]`.
pub fn script(name: &str) -> Option<RangeTable> {
    find_table(tables::SCRIPTS, name)
}

/// Go `unicode.FoldCategory[name]`.
pub fn fold_category(name: &str) -> Option<RangeTable> {
    find_table(tables::FOLD_CATEGORY, name)
}

/// Go `unicode.FoldScript[name]`.
pub fn fold_script(name: &str) -> Option<RangeTable> {
    find_table(tables::FOLD_SCRIPT, name)
}

/// Go `unicode.Is(table, r)`.
pub fn is_in(table: RangeTable, r: u32) -> bool {
    // Ranges are sorted by lo within the R16 and R32 parts, and all R16 ranges are below all R32 ranges.
    let i = table.partition_point(|&(_, hi, _)| hi < r);
    if let Some(&(lo, hi, stride)) = table.get(i) {
        return lo <= r && r <= hi && (stride == 1 || (r - lo) % stride == 0);
    }
    false
}

fn lookup_pair(t: &'static [(u32, u32)], r: u32) -> Option<u32> {
    t.binary_search_by(|&(k, _)| k.cmp(&r)).ok().map(|i| t[i].1)
}

/// Go `unicode.SimpleFold`: the next rune in the case folding orbit of `r`.
pub fn simple_fold(r: i32) -> i32 {
    if !(0..=MAX_RUNE).contains(&r) {
        return r;
    }
    match lookup_pair(tables::SIMPLE_FOLD, r as u32) {
        Some(t) => t as i32,
        None => r,
    }
}

/// Go `unicode.ToLower`.
pub fn to_lower(r: char) -> char {
    if r.is_ascii() {
        return r.to_ascii_lowercase();
    }
    lookup_pair(tables::TO_LOWER, r as u32).and_then(char::from_u32).unwrap_or(r)
}

/// Go `unicode.ToUpper`.
pub fn to_upper(r: char) -> char {
    if r.is_ascii() {
        return r.to_ascii_uppercase();
    }
    lookup_pair(tables::TO_UPPER, r as u32).and_then(char::from_u32).unwrap_or(r)
}

/// Go `unicode.IsPrint` (and `strconv.IsPrint`).
pub fn is_print(r: char) -> bool {
    let r = r as u32;
    if r < 0x80 {
        return (0x20..0x7f).contains(&r);
    }
    let t = tables::PRINT;
    let i = t.partition_point(|&(_, hi)| hi < r);
    t.get(i).is_some_and(|&(lo, hi)| lo <= r && r <= hi)
}

/// Go `unicode.IsLetter`.
pub fn is_letter(r: char) -> bool {
    if r.is_ascii() {
        return r.is_ascii_alphabetic();
    }
    is_in(category("L").expect("L table"), r as u32)
}

/// Go `unicode.IsDigit`.
pub fn is_digit(r: char) -> bool {
    if (r as u32) <= 0xFF {
        return r.is_ascii_digit();
    }
    is_in(category("Nd").expect("Nd table"), r as u32)
}

/// Go `strings.ToLower`.
pub fn str_to_lower(s: &str) -> Cow<'_, str> {
    if s.is_ascii() {
        if !s.bytes().any(|c| c.is_ascii_uppercase()) {
            return Cow::Borrowed(s);
        }
        return Cow::Owned(s.to_ascii_lowercase());
    }
    Cow::Owned(s.chars().map(to_lower).collect())
}

/// Go `strings.ToUpper`.
pub fn str_to_upper(s: &str) -> Cow<'_, str> {
    if s.is_ascii() {
        if !s.bytes().any(|c| c.is_ascii_lowercase()) {
            return Cow::Borrowed(s);
        }
        return Cow::Owned(s.to_ascii_uppercase());
    }
    Cow::Owned(s.chars().map(to_upper).collect())
}

/// Go `strings.EqualFold`: equality under Unicode simple case folding.
pub fn equal_fold(s: &str, t: &str) -> bool {
    let (sb, tb) = (s.as_bytes(), t.as_bytes());
    let mut i = 0;
    while i < sb.len() && i < tb.len() {
        let (mut sr, mut tr) = (sb[i], tb[i]);
        if (sr | tr) >= 0x80 {
            return equal_fold_unicode(&s[i..], &t[i..]);
        }
        i += 1;
        if tr == sr {
            continue;
        }
        if tr < sr {
            std::mem::swap(&mut tr, &mut sr);
        }
        if sr.is_ascii_uppercase() && tr == sr + b'a' - b'A' {
            continue;
        }
        return false;
    }
    sb.len() == tb.len()
}

fn equal_fold_unicode(s: &str, t: &str) -> bool {
    let mut tc = t.chars();
    for sr in s.chars() {
        let Some(tr) = tc.next() else { return false };
        let (mut sr, mut tr) = (sr as i32, tr as i32);
        if tr == sr {
            continue;
        }
        if tr < sr {
            std::mem::swap(&mut tr, &mut sr);
        }
        if tr < 0x80 {
            if (b'A' as i32..=b'Z' as i32).contains(&sr) && tr == sr + 32 {
                continue;
            }
            return false;
        }
        let mut r = simple_fold(sr);
        while r != sr && r < tr {
            r = simple_fold(r);
        }
        if r == tr {
            continue;
        }
        return false;
    }
    tc.next().is_none()
}

/// Canonical combining class of `r` (from x/text norm).
pub fn ccc(r: char) -> u8 {
    let r = r as u32;
    let t = tables::CCC;
    let i = t.partition_point(|&(_, hi, _)| hi < r);
    match t.get(i) {
        Some(&(lo, hi, c)) if lo <= r && r <= hi => c,
        _ => 0,
    }
}

const S_BASE: u32 = 0xAC00;
const L_BASE: u32 = 0x1100;
const V_BASE: u32 = 0x1161;
const T_BASE: u32 = 0x11A7;
const T_COUNT: u32 = 28;
const N_COUNT: u32 = 21 * 28;
const S_COUNT: u32 = 19 * N_COUNT;

/// Unicode Normalization Form KD (Go `norm.NFKD.String`).
pub fn nfkd(s: &str) -> String {
    let mut out: Vec<char> = Vec::with_capacity(s.len());
    for c in s.chars() {
        let u = c as u32;
        if (S_BASE..S_BASE + S_COUNT).contains(&u) {
            let si = u - S_BASE;
            out.push(char::from_u32(L_BASE + si / N_COUNT).unwrap());
            out.push(char::from_u32(V_BASE + (si % N_COUNT) / T_COUNT).unwrap());
            let t = si % T_COUNT;
            if t != 0 {
                out.push(char::from_u32(T_BASE + t).unwrap());
            }
            continue;
        }
        match tables::NFKD.binary_search_by(|&(k, _)| k.cmp(&u)) {
            Ok(i) => out.extend(tables::NFKD[i].1.iter().map(|&x| char::from_u32(x).unwrap())),
            Err(_) => out.push(c),
        }
    }
    // Canonical ordering: stable sort each run of non-starters by combining class.
    let mut i = 0;
    while i < out.len() {
        if ccc(out[i]) == 0 {
            i += 1;
            continue;
        }
        let start = i;
        while i < out.len() && ccc(out[i]) != 0 {
            i += 1;
        }
        out[start..i].sort_by_key(|&c| ccc(c));
    }
    out.into_iter().collect()
}

/// Go `strconv.AppendQuote`: appends a double-quoted Go string literal representing `s`.
pub fn append_quote(dst: &mut String, s: &str) {
    dst.push('"');
    for r in s.chars() {
        append_escaped_rune(dst, r);
    }
    dst.push('"');
}

/// Go `strconv.Quote`.
pub fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    append_quote(&mut out, s);
    out
}

fn append_escaped_rune(dst: &mut String, r: char) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    if r == '"' || r == '\\' {
        dst.push('\\');
        dst.push(r);
        return;
    }
    if is_print(r) {
        dst.push(r);
        return;
    }
    match r {
        '\x07' => dst.push_str("\\a"),
        '\x08' => dst.push_str("\\b"),
        '\x0c' => dst.push_str("\\f"),
        '\n' => dst.push_str("\\n"),
        '\r' => dst.push_str("\\r"),
        '\t' => dst.push_str("\\t"),
        '\x0b' => dst.push_str("\\v"),
        _ => {
            let u = r as u32;
            if u < 0x20 || u == 0x7f {
                dst.push_str("\\x");
                dst.push(HEX[(u >> 4) as usize & 0xF] as char);
                dst.push(HEX[u as usize & 0xF] as char);
            } else if u < 0x10000 {
                dst.push_str("\\u");
                for s in (0..4).rev() {
                    dst.push(HEX[(u >> (s * 4)) as usize & 0xF] as char);
                }
            } else {
                dst.push_str("\\U");
                for s in (0..8).rev() {
                    dst.push(HEX[(u >> (s * 4)) as usize & 0xF] as char);
                }
            }
        }
    }
}
