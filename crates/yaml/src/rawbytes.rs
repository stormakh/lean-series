//! Invalid UTF-8 in decoded strings. A Go string may hold any bytes (a `!!binary` scalar decoded into a
//! string keeps them, and Go then rejects them, for example as an invalid label name). Rust strings are UTF-8,
//! so each byte that is not part of valid UTF-8 is kept as the private-use char U+10FF00 + byte.
//! `is_valid_utf8` is Go's `utf8.ValidString` of the original bytes and the quoting functions
//! (`go_quote`, model's `unicode::quote`) print such a char as `\xNN`, as Go's `%q` prints the byte.
//! (A literal U+10FF00..U+10FFFF in the input reads the same way; those code points are private use.)

/// The char for raw byte 0.
pub const RAW_BASE: u32 = 0x10FF00;

/// The raw byte a char stands for, if it is one.
pub fn raw_byte(c: char) -> Option<u8> {
    let u = c as u32;
    if (RAW_BASE..=RAW_BASE + 0xFF).contains(&u) { Some((u - RAW_BASE) as u8) } else { None }
}

/// Bytes to a string, keeping invalid bytes as raw chars.
pub fn from_bytes(b: &[u8]) -> String {
    let mut out = String::with_capacity(b.len());
    let mut rest = b;
    loop {
        match std::str::from_utf8(rest) {
            Ok(s) => {
                out.push_str(s);
                return out;
            }
            Err(e) => {
                let good = e.valid_up_to();
                // valid_up_to is a char boundary of valid UTF-8
                out.push_str(std::str::from_utf8(&rest[..good]).unwrap_or_default());
                let bad = e.error_len().unwrap_or(rest.len() - good);
                for &x in &rest[good..good + bad] {
                    out.push(char::from_u32(RAW_BASE + x as u32).unwrap_or('\u{FFFD}'));
                }
                rest = &rest[good + bad..];
            }
        }
    }
}

/// Go `utf8.ValidString` of the bytes the string stands for: false if it holds a raw byte.
pub fn is_valid_utf8(s: &str) -> bool {
    // raw chars are encoded F4 8F BC..BF xx
    !s.as_bytes().contains(&0xF4) || !s.chars().any(|c| raw_byte(c).is_some())
}
