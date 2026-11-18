//! Go `strconv` and `fmt` helpers that the parser's texts depend on, and `util/strutil.Unquote`.

use std::fmt::Write;

/// Go `strconv.Quote`.
pub fn go_quote(s: &str) -> String {
    model::unicode::quote(s)
}

/// Go `%q` of a rune (`strconv.QuoteRune`). `None` is Go's `eof` (-1), an invalid rune printed as U+FFFD.
pub fn go_quote_rune(r: Option<char>) -> String {
    let r = r.unwrap_or('\u{FFFD}');
    let mut out = String::with_capacity(8);
    out.push('\'');
    match r {
        '\'' | '\\' => {
            out.push('\\');
            out.push(r);
        }
        _ => {
            // Reuse the double-quote escaper and strip its quotes; '"' needs no escape inside single quotes.
            if r == '"' {
                out.push('"');
            } else {
                let q = model::unicode::quote(r.encode_utf8(&mut [0u8; 4]));
                out.push_str(&q[1..q.len() - 1]);
            }
        }
    }
    out.push('\'');
    out
}

/// Go `%#U` of a rune: `U+0041 'A'` (the character only when printable).
pub fn go_unicode_sharp(r: char) -> String {
    let mut s = format!("U+{:04X}", r as u32);
    if model::unicode::is_print(r) {
        let _ = write!(s, " '{}'", r);
    }
    s
}

/// The error of `unquote` (Go `strutil.ErrSyntax`).
pub const ERR_SYNTAX: &str = "invalid syntax";

/// Go `util/strutil.Unquote`: interprets a single-quoted, double-quoted or backquoted string literal.
///
/// Go strings may hold any bytes; a Rust `String` holds UTF-8. Escapes such as `\xff` that produce invalid
/// UTF-8 are replaced with U+FFFD (as `String::from_utf8_lossy` does).
pub fn unquote(s: &str) -> Result<String, &'static str> {
    let b = s.as_bytes();
    let n = b.len();
    if n < 2 {
        return Err(ERR_SYNTAX);
    }
    let quote = b[0];
    if quote != b[n - 1] {
        return Err(ERR_SYNTAX);
    }
    let inner = &s[1..n - 1];
    if quote == b'`' {
        if inner.contains('`') {
            return Err(ERR_SYNTAX);
        }
        return Ok(inner.to_string());
    }
    if quote != b'"' && quote != b'\'' {
        return Err(ERR_SYNTAX);
    }
    if inner.contains('\n') {
        return Err(ERR_SYNTAX);
    }
    if !inner.contains('\\') && !inner.as_bytes().contains(&quote) {
        return Ok(inner.to_string());
    }

    let mut buf: Vec<u8> = Vec::with_capacity(3 * inner.len() / 2);
    let mut rest = inner.as_bytes();
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
    match String::from_utf8(buf) {
        Ok(s) => Ok(s),
        Err(e) => Ok(String::from_utf8_lossy(e.as_bytes()).into_owned()),
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

fn unquote_char(s: &[u8], quote: u8) -> Result<(u32, bool, &[u8]), &'static str> {
    let c = s[0];
    if c == quote && (quote == b'\'' || quote == b'"') {
        return Err(ERR_SYNTAX);
    }
    if c >= 0x80 {
        // The input is valid UTF-8.
        let st = std::str::from_utf8(&s[..s.len().min(4)]).or_else(|e| std::str::from_utf8(&s[..e.valid_up_to()]));
        let ch = st.ok().and_then(|t| t.chars().next()).unwrap_or('\u{FFFD}');
        let size = ch.len_utf8();
        return Ok((ch as u32, true, &s[size..]));
    }
    if c != b'\\' {
        return Ok((c as u32, false, &s[1..]));
    }
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
            v
        }
        b'0'..=b'7' => {
            let mut v = (c - b'0') as u32;
            if s.len() < 2 {
                return Err(ERR_SYNTAX);
            }
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

/// The error of the Go `strconv` number parsers, formatted as Go's `*NumError`.
fn num_error(func: &str, s: &str, range: bool) -> String {
    format!(
        "strconv.{}: parsing {}: {}",
        func,
        go_quote(s),
        if range { "value out of range" } else { "invalid syntax" }
    )
}

fn underscore_ok(s: &[u8]) -> bool {
    let mut saw = b'^';
    let mut i = 0;
    let mut s = s;
    if !s.is_empty() && (s[0] == b'-' || s[0] == b'+') {
        s = &s[1..];
    }
    let mut hex = false;
    if s.len() >= 2 && s[0] == b'0' && matches!(s[1] | 0x20, b'b' | b'o' | b'x') {
        i = 2;
        saw = b'0';
        hex = (s[1] | 0x20) == b'x';
    }
    while i < s.len() {
        let c = s[i];
        if c.is_ascii_digit() || (hex && (b'a'..=b'f').contains(&(c | 0x20))) {
            saw = b'0';
        } else if c == b'_' {
            if saw != b'0' {
                return false;
            }
            saw = b'_';
        } else {
            if saw == b'_' {
                return false;
            }
            saw = b'!';
        }
        i += 1;
    }
    saw != b'_'
}

/// Go `strconv.ParseUint(s, base, 64)` for base 0 or 10. The error is Go's text.
pub(crate) fn parse_uint(s: &str, base0: bool) -> Result<u64, (u64, String)> {
    const FN: &str = "ParseUint";
    let b = s.as_bytes();
    if b.is_empty() {
        return Err((0, num_error(FN, s, false)));
    }
    let mut base: u64 = 10;
    let mut digits = b;
    if base0 && b[0] == b'0' {
        if b.len() >= 3 && (b[1] | 0x20) == b'b' {
            base = 2;
            digits = &b[2..];
        } else if b.len() >= 3 && (b[1] | 0x20) == b'o' {
            base = 8;
            digits = &b[2..];
        } else if b.len() >= 3 && (b[1] | 0x20) == b'x' {
            base = 16;
            digits = &b[2..];
        } else {
            base = 8;
            digits = &b[1..];
        }
    }
    let cutoff = u64::MAX / base + 1;
    let mut underscores = false;
    let mut n: u64 = 0;
    for &c in digits {
        let d = match c {
            b'_' if base0 => {
                underscores = true;
                continue;
            }
            b'0'..=b'9' => (c - b'0') as u64,
            _ if (b'a'..=b'z').contains(&(c | 0x20)) => ((c | 0x20) - b'a' + 10) as u64,
            _ => return Err((0, num_error(FN, s, false))),
        };
        if d >= base {
            return Err((0, num_error(FN, s, false)));
        }
        if n >= cutoff {
            return Err((u64::MAX, num_error(FN, s, true)));
        }
        n *= base;
        let n1 = n.wrapping_add(d);
        if n1 < n {
            return Err((u64::MAX, num_error(FN, s, true)));
        }
        n = n1;
    }
    if underscores && !underscore_ok(b) {
        return Err((0, num_error(FN, s, false)));
    }
    Ok(n)
}

/// Go `strconv.ParseInt(s, 0, 64)`; only success matters to the parser.
pub(crate) fn parse_int_base0(s: &str) -> Option<i64> {
    let (neg, rest) = match s.as_bytes().first() {
        Some(b'+') => (false, &s[1..]),
        Some(b'-') => (true, &s[1..]),
        _ => (false, s),
    };
    let un = parse_uint(rest, true).ok()?;
    let cutoff = 1u64 << 63;
    if !neg && un >= cutoff {
        return None;
    }
    if neg && un > cutoff {
        return None;
    }
    Some(if neg { (un as i64).wrapping_neg() } else { un as i64 })
}

/// Go `strconv.ParseFloat(s, 64)` for the number syntax that the lexer produces: decimal floats with
/// underscores, "Inf", "NaN", and hexadecimal integers (rejected by Go without a 'p' exponent). The error is
/// Go's text, with the value Go returns along with it (+-Inf on overflow).
pub(crate) fn parse_float(s: &str) -> Result<f64, (f64, String)> {
    const FN: &str = "ParseFloat";
    let b = s.as_bytes();
    // Go `special`: optional sign, "inf", "infinity" or "nan", case-insensitive, consuming the whole string.
    {
        let (sign, body) = match b.first() {
            Some(b'+') => (1.0, &s[1..]),
            Some(b'-') => (-1.0, &s[1..]),
            _ => (1.0, s),
        };
        if body.eq_ignore_ascii_case("inf") || body.eq_ignore_ascii_case("infinity") {
            return Ok(sign * f64::INFINITY);
        }
        if body.len() == s.len() && body.eq_ignore_ascii_case("nan") {
            return Ok(model::value::normal_nan());
        }
    }
    let mut i = 0;
    if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        i += 1;
    }
    if i + 2 < b.len() && b[i] == b'0' && (b[i + 1] | 0x20) == b'x' {
        // Hexadecimal floats need a 'p' exponent, which the lexer never produces.
        return Err((0.0, num_error(FN, s, false)));
    }
    // Validate the decimal syntax as Go readFloat does: digits, one dot, optional exponent with digits.
    let mut underscores = false;
    let mut saw_digits = false;
    let mut saw_dot = false;
    while i < b.len() {
        match b[i] {
            b'_' => underscores = true,
            b'.' if !saw_dot => saw_dot = true,
            b'0'..=b'9' => saw_digits = true,
            _ => break,
        }
        i += 1;
    }
    if !saw_digits {
        return Err((0.0, num_error(FN, s, false)));
    }
    if i < b.len() && (b[i] | 0x20) == b'e' {
        i += 1;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        if i >= b.len() || !b[i].is_ascii_digit() {
            return Err((0.0, num_error(FN, s, false)));
        }
        while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'_') {
            if b[i] == b'_' {
                underscores = true;
            }
            i += 1;
        }
    }
    if i != b.len() || (underscores && !underscore_ok(b)) {
        return Err((0.0, num_error(FN, s, false)));
    }
    let f: f64 = if underscores {
        s.replace('_', "").parse().map_err(|_| (0.0, num_error(FN, s, false)))?
    } else {
        s.parse().map_err(|_| (0.0, num_error(FN, s, false)))?
    };
    if f.is_infinite() {
        return Err((f, num_error(FN, s, true)));
    }
    Ok(f)
}
