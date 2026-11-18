//! Port of `model/labels/float.go`, plus Go `strconv.FormatFloat(f, 'g', -1, 64)`.

/// Appends `f` as Go `strconv.AppendFloat(dst, f, 'g', -1, 64)` does: shortest digits that read back the same,
/// exponent form when the decimal exponent is below -4 or at least 6 (Go uses precision 6 for this decision in
/// shortest mode), with a two-digit minimum exponent (`1e+06`). NaN and infinities print as `NaN`, `+Inf`, `-Inf`.
pub fn append_float_g(dst: &mut String, f: f64) {
    if f.is_nan() {
        dst.push_str("NaN");
        return;
    }
    if f.is_infinite() {
        dst.push_str(if f > 0.0 { "+Inf" } else { "-Inf" });
        return;
    }
    let mut buf = [0u8; 32];
    let s = {
        use std::io::Write;
        let mut c = std::io::Cursor::new(&mut buf[..]);
        let _ = write!(c, "{:e}", f);
        let n = c.position() as usize;
        &buf[..n]
    };
    // s is like "-1.2345e-7" or "1e6" or "0e0".
    let epos = s.iter().position(|&c| c == b'e').unwrap_or(s.len());
    let (mant, exp_s) = (&s[..epos], &s[epos + 1..]);
    let neg = mant.first() == Some(&b'-');
    let mut digits = [0u8; 24];
    let mut nd = 0;
    for &c in mant {
        if c.is_ascii_digit() {
            digits[nd] = c;
            nd += 1;
        }
    }
    let mut exp: i32 = 0;
    let mut eneg = false;
    for &c in exp_s {
        if c == b'-' {
            eneg = true;
        } else if c.is_ascii_digit() {
            exp = exp * 10 + (c - b'0') as i32;
        }
    }
    if eneg {
        exp = -exp;
    }
    let digits = &digits[..nd];
    let dp = exp + 1;
    if neg {
        dst.push('-');
    }
    let x = dp - 1;
    if x < -4 || x >= 6 {
        dst.push(digits[0] as char);
        if nd > 1 {
            dst.push('.');
            for &d in &digits[1..] {
                dst.push(d as char);
            }
        }
        dst.push('e');
        dst.push(if x < 0 { '-' } else { '+' });
        let ax = x.unsigned_abs();
        if ax < 10 {
            dst.push('0');
        }
        dst.push_str(itoa(ax, &mut [0u8; 12]));
        return;
    }
    let nd = nd as i32;
    if dp <= 0 {
        dst.push_str("0.");
        for _ in 0..-dp {
            dst.push('0');
        }
        for &d in digits {
            dst.push(d as char);
        }
    } else if dp >= nd {
        for &d in digits {
            dst.push(d as char);
        }
        for _ in 0..dp - nd {
            dst.push('0');
        }
    } else {
        for &d in &digits[..dp as usize] {
            dst.push(d as char);
        }
        dst.push('.');
        for &d in &digits[dp as usize..] {
            dst.push(d as char);
        }
    }
}

fn itoa(mut v: u32, buf: &mut [u8; 12]) -> &str {
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    std::str::from_utf8(&buf[i..]).unwrap_or("")
}

/// Go `strconv.FormatFloat(f, 'g', -1, 64)`.
pub fn format_float_g(f: f64) -> String {
    let mut s = String::with_capacity(24);
    append_float_g(&mut s, f);
    s
}

/// Works like the usual Go string formatting of a float but appends ".0" if the resulting number would otherwise
/// contain neither a "." nor an "e".
pub fn format_open_metrics_float(f: f64) -> String {
    let mut s = String::with_capacity(24);
    append_open_metrics_float(&mut s, f);
    s
}

/// Appends the OpenMetrics float representation of `f` to `dst`.
pub fn append_open_metrics_float(dst: &mut String, f: f64) {
    if f == 1.0 {
        dst.push_str("1.0");
        return;
    }
    if f == 0.0 {
        dst.push_str("0.0");
        return;
    }
    if f == -1.0 {
        dst.push_str("-1.0");
        return;
    }
    if f.is_nan() {
        dst.push_str("NaN");
        return;
    }
    if f.is_infinite() {
        dst.push_str(if f > 0.0 { "+Inf" } else { "-Inf" });
        return;
    }
    let start = dst.len();
    append_float_g(dst, f);
    if !dst[start..].bytes().any(|c| c == b'e' || c == b'.') {
        dst.push_str(".0");
    }
}
