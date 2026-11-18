//! Go `fmt` float verbs that histogram strings and error texts use (`%g`, `%v`, `%f`).

use std::fmt::Write;

/// Appends `f` as Go `fmt` `%g` (and `%v`) writes a float64: shortest round-trip digits, exponent form (at least two
/// exponent digits) when the decimal exponent is < -4 or >= 6; "+Inf", "-Inf", "NaN".
pub fn write_g(out: &mut String, f: f64) {
    if f.is_nan() {
        out.push_str("NaN");
        return;
    }
    if f.is_infinite() {
        out.push_str(if f > 0.0 { "+Inf" } else { "-Inf" });
        return;
    }
    if f == 0.0 {
        out.push_str(if f.is_sign_negative() { "-0" } else { "0" });
        return;
    }
    // Rust `{:e}` gives the shortest round-trip digits: "-1.2345e-7", "1e6".
    let mut buf = String::with_capacity(32);
    let _ = write!(buf, "{:e}", f);
    let (mant, exp) = buf.split_once('e').unwrap_or((&buf, "0"));
    let x: i32 = exp.parse().unwrap_or(0);
    let neg = mant.starts_with('-');
    let digits: String = mant.chars().filter(|c| c.is_ascii_digit()).collect();
    let nd = digits.len() as i32;
    // Go strconv 'g' with shortest precision ends with `if shortest { eprec = 6 }`.
    let eprec = 6;
    if neg {
        out.push('-');
    }
    if x < -4 || x >= eprec {
        out.push_str(&digits[..1]);
        if nd > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        if x < 0 {
            out.push('-');
        } else {
            out.push('+');
        }
        let ax = x.unsigned_abs();
        if ax < 10 {
            out.push('0');
        }
        let _ = write!(out, "{}", ax);
        return;
    }
    // Fixed notation with exactly the shortest digits.
    let dp = x + 1; // Position of the decimal point relative to the digits.
    if dp <= 0 {
        out.push_str("0.");
        for _ in 0..(-dp) {
            out.push('0');
        }
        out.push_str(&digits);
    } else if dp >= nd {
        out.push_str(&digits);
        for _ in 0..(dp - nd) {
            out.push('0');
        }
    } else {
        out.push_str(&digits[..dp as usize]);
        out.push('.');
        out.push_str(&digits[dp as usize..]);
    }
}

/// Go `%g` as a new string.
pub fn g(f: f64) -> String {
    let mut s = String::new();
    write_g(&mut s, f);
    s
}

/// Go `%f` (6 decimals; "+Inf", "-Inf", "NaN").
pub fn f6(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "+Inf".to_string() } else { "-Inf".to_string() };
    }
    format!("{:.6}", f)
}

/// Go `%g` of a `[]float64`: "[1 2.5 1e+07]".
pub fn g_slice(v: &[f64]) -> String {
    let mut s = String::from("[");
    for (i, f) in v.iter().enumerate() {
        if i > 0 {
            s.push(' ');
        }
        write_g(&mut s, *f);
    }
    s.push(']');
    s
}
