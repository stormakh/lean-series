//! Port of `util/strutil/quote.go`: `Unquote` for PromQL string literals (adapted from Go's `strconv`, with
//! single-quoted strings treated like double-quoted ones).

/// Go `ErrSyntax`: the value does not have the right syntax.
pub const ERR_SYNTAX: &str = "invalid syntax";

/// Go `Unquote`: interprets `s` as a single-quoted, double-quoted or backquoted PromQL string literal and returns
/// the bytes it quotes. Go strings may hold any bytes (`"\xff"` unquotes to the single byte 0xff), so this is
/// the exact form; see [`unquote`] for a Rust `String`.
pub fn unquote_bytes(s: &str) -> Result<Vec<u8>, &'static str> {
    let b = s.as_bytes();
    let n = b.len();
    if n < 2 {
        return Err(ERR_SYNTAX);
    }
    let quote = b[0];
    if quote != b[n - 1] {
        return Err(ERR_SYNTAX);
    }
    let inner = &b[1..n - 1];
    if quote == b'`' {
        if inner.contains(&b'`') {
            return Err(ERR_SYNTAX);
        }
        return Ok(inner.to_vec());
    }
    if quote != b'"' && quote != b'\'' {
        return Err(ERR_SYNTAX);
    }
    if inner.contains(&b'\n') {
        return Err(ERR_SYNTAX);
    }
    // Is it trivial? Avoid allocation.
    if !inner.contains(&b'\\') && !inner.contains(&quote) {
        return Ok(inner.to_vec());
    }
    let mut buf: Vec<u8> = Vec::with_capacity(3 * inner.len() / 2);
    let mut rest = inner;
    while !rest.is_empty() {
        let (c, multibyte, tail) = unquote_char(rest, quote)?;
        rest = tail;
        if c < 0x80 || !multibyte {
            buf.push(c as u8);
        } else {
            // Go utf8.EncodeRune writes U+FFFD for surrogates and invalid code points.
            let ch = char::from_u32(c).unwrap_or('\u{FFFD}');
            buf.extend_from_slice(ch.encode_utf8(&mut [0u8; 4]).as_bytes());
        }
    }
    Ok(buf)
}

/// Go `Unquote` into a Rust `String`. Escapes such as `\xff` that produce invalid UTF-8 are replaced with
/// U+FFFD (as `String::from_utf8_lossy` does).
pub fn unquote(s: &str) -> Result<String, &'static str> {
    unquote_checked(s).map(|(s, _)| s)
}

/// Like [`unquote`], also telling whether the Go string would not be valid UTF-8 (the result then holds U+FFFD
/// in place of the invalid bytes).
pub fn unquote_checked(s: &str) -> Result<(String, bool), &'static str> {
    let buf = unquote_bytes(s)?;
    match String::from_utf8(buf) {
        Ok(s) => Ok((s, false)),
        Err(e) => Ok((String::from_utf8_lossy(e.as_bytes()).into_owned(), true)),
    }
}

/// The bytes of a quoted string literal that is not valid UTF-8, or `None` when it is valid or does not unquote.
pub fn unquote_invalid_bytes(s: &str) -> Option<Vec<u8>> {
    match unquote_bytes(s) {
        Ok(b) if std::str::from_utf8(&b).is_err() => Some(b),
        _ => None,
    }
}

fn unhex(b: u8) -> Option<u32> {
    match b {
        b'0'..=b'9' => Some((b - b'0') as u32),
        b'a'..=b'f' => Some((b - b'a' + 10) as u32),
        b'A'..=b'F' => Some((b - b'A' + 10) as u32),
        _ => None,
    }
}

/// Go `unquoteChar`: decodes the first character or byte of the escaped string `s`. Returns the value, whether
/// it needs a multibyte UTF-8 form, and the rest of the input.
fn unquote_char(s: &[u8], quote: u8) -> Result<(u32, bool, &[u8]), &'static str> {
    let c = s[0];
    if c == quote && (quote == b'\'' || quote == b'"') {
        return Err(ERR_SYNTAX);
    }
    if c >= 0x80 {
        // The input is a Rust &str, so this is valid UTF-8.
        let st = std::str::from_utf8(&s[..s.len().min(4)]).or_else(|e| std::str::from_utf8(&s[..e.valid_up_to()]));
        let ch = st.ok().and_then(|t| t.chars().next()).unwrap_or('\u{FFFD}');
        return Ok((ch as u32, true, &s[ch.len_utf8()..]));
    }
    if c != b'\\' {
        return Ok((c as u32, false, &s[1..]));
    }
    // Hard case: c is backslash.
    if s.len() <= 1 {
        return Err(ERR_SYNTAX);
    }
    let c = s[1];
    let mut s = &s[2..];
    let mut multibyte = false;
    let value: u32 = match c {
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
            let mut v: u32 = 0;
            for &d in &s[..n] {
                v = (v << 4) | unhex(d).ok_or(ERR_SYNTAX)?;
            }
            s = &s[n..];
            if c != b'x' {
                if v > 0x10FFFF {
                    return Err(ERR_SYNTAX);
                }
                multibyte = true;
            }
            // 'x': a single byte, possibly not UTF-8.
            v
        }
        b'0'..=b'7' => {
            let mut v = (c - b'0') as u32;
            if s.len() < 2 {
                return Err(ERR_SYNTAX);
            }
            // One digit already; two more.
            for &d in &s[..2] {
                if !(b'0'..=b'7').contains(&d) {
                    return Err(ERR_SYNTAX);
                }
                v = (v << 3) | (d - b'0') as u32;
            }
            s = &s[2..];
            if v > 255 {
                return Err(ERR_SYNTAX);
            }
            v
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
    Ok((value, multibyte, s))
}
