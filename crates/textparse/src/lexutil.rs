//! Fast scanning loops for the hand-written lexers. Each one is equal to a golex DFA self-loop that calls
//! `next()` per byte, for states where `next()` does not skip null bytes (or where skipping is handled here).

/// `[a-zA-Z_:]` (metric name start).
pub(crate) const M: u8 = 1;
/// `[a-zA-Z_:0-9]`.
pub(crate) const MD: u8 = 2;
/// `[a-zA-Z_]` (label name start).
pub(crate) const L: u8 = 4;
/// `[a-zA-Z_0-9]`.
pub(crate) const LD: u8 = 8;
/// `[0-9]`.
pub(crate) const D: u8 = 16;

const fn class_table() -> [u8; 256] {
    let mut t = [0u8; 256];
    let mut c = 0;
    while c < 256 {
        let b = c as u8;
        let alpha = b.is_ascii_alphabetic() || b == b'_';
        let digit = b.is_ascii_digit();
        let mut v = 0;
        if alpha || b == b':' {
            v |= M | MD;
        }
        if digit {
            v |= MD | LD | D;
        }
        if alpha {
            v |= L | LD;
        }
        t[c] = v;
        c += 1;
    }
    t
}

pub(crate) static CLASS: [u8; 256] = class_table();

#[inline(always)]
pub(crate) fn is(c: u8, class: u8) -> bool {
    CLASS[c as usize] & class != 0
}

/// Index of the first byte at or after `i` that is not in `class` (or `b.len()`).
#[inline(always)]
pub(crate) fn scan_while(b: &[u8], mut i: usize, class: u8) -> usize {
    while i < b.len() && CLASS[b[i] as usize] & class != 0 {
        i += 1;
    }
    i.min(b.len())
}

/// Index of the first `c` at or after `i` (or `b.len()`).
#[inline(always)]
pub(crate) fn find(b: &[u8], i: usize, c: u8) -> usize {
    if i >= b.len() {
        return b.len();
    }
    match b[i..].iter().position(|&x| x == c) {
        Some(p) => i + p,
        None => b.len(),
    }
}

/// `\"(\\.|[^\\"])*\"` (or with `[^\\"\n]` when `!allow_nl`) starting at the opening quote `b[open]`. With
/// `skip_nulls` null bytes are skipped as `next()` does in label values. Returns the index of the closing quote,
/// or the abort position (the offending byte, or `b.len()` at the end).
#[inline]
pub(crate) fn scan_quoted(b: &[u8], open: usize, skip_nulls: bool, allow_nl: bool) -> Result<usize, usize> {
    let n = b.len();
    let mut i = open + 1;
    loop {
        while i < n {
            let c = b[i];
            if c == b'"' || c == b'\\' || c == 0 || (c == b'\n' && !allow_nl) {
                break;
            }
            i += 1;
        }
        if i >= n {
            return Err(n);
        }
        match b[i] {
            b'"' => return Ok(i),
            b'\\' => {
                i += 1;
                if skip_nulls {
                    while i < n && b[i] == 0 {
                        i += 1;
                    }
                }
                if i >= n {
                    return Err(n);
                }
                if b[i] == 0 || b[i] == b'\n' {
                    return Err(i);
                }
                i += 1;
            }
            0 if skip_nulls => i += 1,
            _ => return Err(i),
        }
    }
}
