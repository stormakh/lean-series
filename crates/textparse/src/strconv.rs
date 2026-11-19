//! Go `strconv` behavior that the parsers need: `ParseFloat` (exact syntax and rounding, including hex floats,
//! underscores, Inf/NaN spellings and range errors), `ParseInt` (base 10), `Quote` for byte strings that may hold
//! invalid UTF-8, and `%f` formatting for error messages.

use std::fmt;

/// The error kind of a failed number conversion (Go `strconv.ErrSyntax` / `strconv.ErrRange`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NumErrKind {
    Syntax,
    Range,
}

/// Go `*strconv.NumError`.
#[derive(Clone, Debug, PartialEq)]
pub struct NumError {
    pub func: &'static str,
    pub num: Vec<u8>,
    pub kind: NumErrKind,
}

impl fmt::Display for NumError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::with_capacity(self.num.len() + 48);
        s.push_str("strconv.");
        s.push_str(self.func);
        s.push_str(": parsing ");
        append_quote_bytes(&mut s, &self.num);
        s.push_str(match self.kind {
            NumErrKind::Syntax => ": invalid syntax",
            NumErrKind::Range => ": value out of range",
        });
        f.write_str(&s)
    }
}

impl std::error::Error for NumError {}

#[inline]
fn lower(c: u8) -> u8 {
    c | (b'x' - b'X')
}

fn common_prefix_len_ignore_case(s: &[u8], prefix: &[u8]) -> usize {
    let n = s.len().min(prefix.len());
    for i in 0..n {
        let mut c = s[i];
        if c.is_ascii_uppercase() {
            c += b'a' - b'A';
        }
        if c != prefix[i] {
            return i;
        }
    }
    n
}

/// Go `special`: Inf/Infinity/NaN with the exact accepted spellings. Returns the value and the bytes consumed.
fn special(s: &[u8]) -> Option<(f64, usize)> {
    if s.is_empty() {
        return None;
    }
    let mut sign = 1.0;
    let mut nsign = 0;
    let mut t = s;
    match s[0] {
        b'+' | b'-' | b'i' | b'I' => {
            if s[0] == b'+' || s[0] == b'-' {
                if s[0] == b'-' {
                    sign = -1.0;
                }
                nsign = 1;
                t = &s[1..];
            }
            let mut n = common_prefix_len_ignore_case(t, b"infinity");
            if 3 < n && n < 8 {
                n = 3;
            }
            if n == 3 || n == 8 {
                return Some((f64::INFINITY * sign, nsign + n));
            }
        }
        b'n' | b'N'
            if common_prefix_len_ignore_case(s, b"nan") == 3 => {
                return Some((f64::from_bits(0x7FF8000000000001), 3));
            }
        _ => {}
    }
    None
}

struct ReadFloat {
    mantissa: u64,
    exp: i64,
    neg: bool,
    trunc: bool,
    hex: bool,
    underscores: bool,
}

/// Go `readFloat`. Returns the parse result and the number of bytes consumed (None when not ok).
fn read_float(s: &[u8]) -> (Option<ReadFloat>, usize) {
    let mut i = 0;
    let mut r = ReadFloat { mantissa: 0, exp: 0, neg: false, trunc: false, hex: false, underscores: false };
    if i >= s.len() {
        return (None, i);
    }
    match s[i] {
        b'+' => i += 1,
        b'-' => {
            i += 1;
            r.neg = true;
        }
        _ => {}
    }
    let mut base: u64 = 10;
    let mut max_mant_digits = 19;
    let mut exp_char = b'e';
    if i + 2 < s.len() && s[i] == b'0' && lower(s[i + 1]) == b'x' {
        base = 16;
        max_mant_digits = 16;
        i += 2;
        exp_char = b'p';
        r.hex = true;
    }
    let mut sawdot = false;
    let mut sawdigits = false;
    let mut nd: i64 = 0;
    let mut nd_mant: i64 = 0;
    let mut dp: i64 = 0;
    while i < s.len() {
        let c = s[i];
        if c == b'_' {
            r.underscores = true;
        } else if c == b'.' {
            if sawdot {
                break;
            }
            sawdot = true;
            dp = nd;
        } else if c.is_ascii_digit() {
            sawdigits = true;
            if c == b'0' && nd == 0 {
                dp -= 1;
                i += 1;
                continue;
            }
            nd += 1;
            if nd_mant < max_mant_digits {
                r.mantissa = r.mantissa * base + (c - b'0') as u64;
                nd_mant += 1;
            } else if c != b'0' {
                r.trunc = true;
            }
        } else if base == 16 && (b'a'..=b'f').contains(&lower(c)) {
            sawdigits = true;
            nd += 1;
            if nd_mant < max_mant_digits {
                r.mantissa = r.mantissa * 16 + (lower(c) - b'a' + 10) as u64;
                nd_mant += 1;
            } else {
                r.trunc = true;
            }
        } else {
            break;
        }
        i += 1;
    }
    if !sawdigits {
        return (None, i);
    }
    if !sawdot {
        dp = nd;
    }
    if base == 16 {
        dp *= 4;
        nd_mant *= 4;
    }
    if i < s.len() && lower(s[i]) == exp_char {
        i += 1;
        if i >= s.len() {
            return (None, i);
        }
        let mut esign = 1;
        match s[i] {
            b'+' => i += 1,
            b'-' => {
                i += 1;
                esign = -1;
            }
            _ => {}
        }
        if i >= s.len() || !s[i].is_ascii_digit() {
            return (None, i);
        }
        let mut e: i64 = 0;
        while i < s.len() && (s[i].is_ascii_digit() || s[i] == b'_') {
            if s[i] == b'_' {
                r.underscores = true;
            } else if e < 10000 {
                e = e * 10 + (s[i] - b'0') as i64;
            }
            i += 1;
        }
        dp += e * esign;
    } else if base == 16 {
        return (None, i);
    }
    if r.mantissa != 0 {
        r.exp = dp - nd_mant;
    }
    if r.underscores && !underscore_ok(&s[..i]) {
        return (None, i);
    }
    (Some(r), i)
}

/// Go `underscoreOK`.
fn underscore_ok(mut s: &[u8]) -> bool {
    let mut saw = b'^';
    let mut i = 0;
    if !s.is_empty() && (s[0] == b'-' || s[0] == b'+') {
        s = &s[1..];
    }
    let mut hex = false;
    if s.len() >= 2 && s[0] == b'0' && matches!(lower(s[1]), b'b' | b'o' | b'x') {
        i = 2;
        saw = b'0';
        hex = lower(s[1]) == b'x';
    }
    while i < s.len() {
        let c = s[i];
        if c.is_ascii_digit() || hex && (b'a'..=b'f').contains(&lower(c)) {
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

const FLOAT64_POW10: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
    1e20, 1e21, 1e22,
];

/// Go `atof64exact`: exact conversion when mantissa and exponent are small.
#[inline]
fn atof64_exact(mantissa: u64, mut exp: i64, neg: bool) -> Option<f64> {
    if mantissa >> 52 != 0 {
        return None;
    }
    let mut f = mantissa as f64;
    if neg {
        f = -f;
    }
    if exp == 0 {
        return Some(f);
    }
    if exp > 0 && exp <= 15 + 22 {
        if exp > 22 {
            f *= FLOAT64_POW10[(exp - 22) as usize];
            exp = 22;
        }
        if !(-1e15..=1e15).contains(&f) {
            return None;
        }
        return Some(f * FLOAT64_POW10[exp as usize]);
    }
    if (-22..0).contains(&exp) {
        return Some(f / FLOAT64_POW10[(-exp) as usize]);
    }
    None
}

/// Go `atofHex` for float64.
fn atof_hex(mut mantissa: u64, mut exp: i64, neg: bool, trunc: bool) -> (f64, bool) {
    const MANTBITS: u32 = 52;
    const EXPBITS: u32 = 11;
    const BIAS: i64 = -1023;
    let max_exp: i64 = (1i64 << EXPBITS) + BIAS - 2;
    let min_exp: i64 = BIAS + 1;
    exp += MANTBITS as i64;
    while mantissa != 0 && mantissa >> (MANTBITS + 2) == 0 {
        mantissa <<= 1;
        exp -= 1;
    }
    if trunc {
        mantissa |= 1;
    }
    while mantissa >> (1 + MANTBITS + 2) != 0 {
        mantissa = (mantissa >> 1) | (mantissa & 1);
        exp += 1;
    }
    while mantissa > 1 && exp < min_exp - 2 {
        mantissa = (mantissa >> 1) | (mantissa & 1);
        exp += 1;
    }
    let mut round = mantissa & 3;
    mantissa >>= 2;
    round |= mantissa & 1;
    exp += 2;
    if round == 3 {
        mantissa += 1;
        if mantissa == 1 << (1 + MANTBITS) {
            mantissa >>= 1;
            exp += 1;
        }
    }
    if mantissa >> MANTBITS == 0 {
        exp = BIAS;
    }
    let mut range = false;
    if exp > max_exp {
        mantissa = 1 << MANTBITS;
        exp = max_exp + 1;
        range = true;
    }
    let mut bits = mantissa & ((1u64 << MANTBITS) - 1);
    bits |= (((exp - BIAS) as u64) & ((1u64 << EXPBITS) - 1)) << MANTBITS;
    if neg {
        bits |= 1u64 << MANTBITS << EXPBITS;
    }
    (f64::from_bits(bits), range)
}

/// Result of [`parse_float`] on failure: the kind and the value Go returns with the error (±Inf on range errors,
/// 0 on syntax errors).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FloatErr {
    pub kind: NumErrKind,
    pub value: f64,
}

impl FloatErr {
    /// The Go `*strconv.NumError` for input `s`.
    pub fn num_error(&self, s: &[u8]) -> NumError {
        NumError { func: "ParseFloat", num: s.to_vec(), kind: self.kind }
    }
}

const SYNTAX: FloatErr = FloatErr { kind: NumErrKind::Syntax, value: 0.0 };

/// Go `strconv.ParseFloat(s, 64)`, bit-exact (NaN is Go's `math.NaN()`).
pub fn parse_float(s: &[u8]) -> Result<f64, FloatErr> {
    if let Some((v, n)) = special(s) {
        return if n == s.len() { Ok(v) } else { Err(SYNTAX) };
    }
    let (r, n) = read_float(s);
    let Some(r) = r else { return Err(SYNTAX) };
    if n != s.len() {
        return Err(SYNTAX);
    }
    if r.hex {
        let (f, range) = atof_hex(r.mantissa, r.exp, r.neg, r.trunc);
        return if range { Err(FloatErr { kind: NumErrKind::Range, value: f }) } else { Ok(f) };
    }
    if !r.trunc
        && let Some(f) = atof64_exact(r.mantissa, r.exp, r.neg) {
            return Ok(f);
        }
    // Correctly rounded slow path (Go's Eisel-Lemire and decimal fallbacks are correctly rounded too).
    let f = if r.underscores {
        let mut buf = Vec::with_capacity(s.len());
        buf.extend(s.iter().copied().filter(|&c| c != b'_'));
        slow_parse(&buf)
    } else {
        slow_parse(s)
    };
    if f.is_infinite() {
        return Err(FloatErr { kind: NumErrKind::Range, value: f });
    }
    Ok(f)
}

fn slow_parse(s: &[u8]) -> f64 {
    // The syntax was validated by read_float; Rust's parser accepts the same decimal forms.
    std::str::from_utf8(s).ok().and_then(|t| t.parse::<f64>().ok()).unwrap_or(0.0)
}

/// Go `strconv.ParseInt(s, 10, 64)`.
pub fn parse_int(s0: &[u8]) -> Result<i64, NumError> {
    let err = |kind| NumError { func: "ParseInt", num: s0.to_vec(), kind };
    if s0.is_empty() {
        return Err(err(NumErrKind::Syntax));
    }
    let mut s = s0;
    let mut neg = false;
    if s[0] == b'+' {
        s = &s[1..];
    } else if s[0] == b'-' {
        neg = true;
        s = &s[1..];
    }
    // ParseUint(s, 10, 64).
    if s.is_empty() {
        return Err(err(NumErrKind::Syntax));
    }
    const CUTOFF: u64 = u64::MAX / 10 + 1;
    let mut n: u64 = 0;
    for &c in s {
        if !c.is_ascii_digit() {
            return Err(err(NumErrKind::Syntax));
        }
        if n >= CUTOFF {
            return Err(err(NumErrKind::Range));
        }
        n *= 10;
        let n1 = n.wrapping_add((c - b'0') as u64);
        if n1 < n {
            return Err(err(NumErrKind::Range));
        }
        n = n1;
    }
    let cutoff = 1u64 << 63;
    if !neg && n >= cutoff {
        return Err(err(NumErrKind::Range));
    }
    if neg && n > cutoff {
        return Err(err(NumErrKind::Range));
    }
    Ok(if neg { (n as i64).wrapping_neg() } else { n as i64 })
}

/// Go `strconv.Quote(string(b))`: invalid UTF-8 bytes print as `\xNN`.
pub fn quote_bytes(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() + 2);
    append_quote_bytes(&mut s, b);
    s
}

/// Appends Go `strconv.Quote(string(b))` to `dst`.
pub fn append_quote_bytes(dst: &mut String, b: &[u8]) {
    dst.push('"');
    for chunk in b.utf8_chunks() {
        let valid = chunk.valid();
        if !valid.is_empty() {
            let mut q = String::with_capacity(valid.len() + 2);
            model::unicode::append_quote(&mut q, valid);
            dst.push_str(&q[1..q.len() - 1]);
        }
        for &c in chunk.invalid() {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            dst.push_str("\\x");
            dst.push(HEX[(c >> 4) as usize] as char);
            dst.push(HEX[(c & 15) as usize] as char);
        }
    }
    dst.push('"');
}

/// Go `int64(f)` on amd64: truncation, and the "integer indefinite" value `math.MinInt64` for NaN and values
/// out of range (Rust's `as` saturates instead).
#[inline]
pub fn go_f64_to_i64(f: f64) -> i64 {
    if f.is_nan() || !(-9.223_372_036_854_776e18..9.223_372_036_854_776e18).contains(&f) {
        return i64::MIN;
    }
    f as i64
}

/// Go `fmt.Sprintf("%f", f)`.
pub fn format_f(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "+Inf".to_string() } else { "-Inf".to_string() };
    }
    format!("{f:.6}")
}

/// Go `fmt.Sprintf("%g", f)` (same as `strconv.FormatFloat(f, 'g', -1, 64)`).
pub fn format_g(f: f64) -> String {
    model::labels::format_float_g(f)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floats() {
        let ok = |s: &str, want: f64| {
            let got = parse_float(s.as_bytes()).unwrap_or_else(|e| panic!("{s}: {e:?}"));
            assert!(got.to_bits() == want.to_bits() || (got.is_nan() && want.is_nan()), "{s}: {got} != {want}");
        };
        let bad = |s: &str| assert!(parse_float(s.as_bytes()).is_err(), "{s} should fail");
        ok("1", 1.0);
        ok("-0", -0.0);
        ok("+1.5e3", 1500.0);
        ok(".5", 0.5);
        ok("5.", 5.0);
        ok("1_000", 1000.0);
        ok("0x1p-2", 0.25);
        ok("0x1.8p1", 3.0);
        ok("Inf", f64::INFINITY);
        ok("-infinity", f64::NEG_INFINITY);
        ok("+inf", f64::INFINITY);
        ok("NaN", f64::NAN);
        ok("1e-400", 0.0);
        ok("123456789012345678901234567890", 1.2345678901234568e29);
        ok("0.1", 0.1);
        ok("1.7976931348623157e308", f64::MAX);
        bad("+nan");
        bad("infin");
        bad("1e");
        bad(".");
        bad("0x1");
        bad("1__0");
        bad("_1");
        bad("1_");
        bad("");
        bad("1e400x");
        assert_eq!(parse_float(b"1e400").unwrap_err().kind, NumErrKind::Range);
        assert_eq!(parse_float(b"-1e400").unwrap_err().value, f64::NEG_INFINITY);
        assert_eq!(
            parse_float(b"a\xff").unwrap_err().num_error(b"a\xff").to_string(),
            "strconv.ParseFloat: parsing \"a\\xff\": invalid syntax"
        );
    }

    #[test]
    fn ints() {
        assert_eq!(parse_int(b"123").unwrap(), 123);
        assert_eq!(parse_int(b"-9223372036854775808").unwrap(), i64::MIN);
        assert_eq!(
            parse_int(b"9223372036854775808").unwrap_err().to_string(),
            "strconv.ParseInt: parsing \"9223372036854775808\": value out of range"
        );
    }
}
