//! Durations as `prometheus/common/model` parses and prints them (`model.ParseDuration`,
//! `model.Duration.String`), and Go `time.Duration` helpers. Durations are `i64` nanoseconds.

use super::quote::go_quote;

/// Nanoseconds per millisecond.
pub const MILLISECOND: i64 = 1_000_000;
/// Nanoseconds per second.
pub const SECOND: i64 = 1_000_000_000;

/// Go `model.ParseDuration`: parses a duration such as `1h30m` into nanoseconds. Units must go from the biggest
/// to the smallest, a year is 365 days. The error text is Go's.
pub fn parse_duration(s: &str) -> Result<i64, String> {
    match s {
        "0" => return Ok(0),
        "" => return Err("empty duration string".to_string()),
        _ => {}
    }
    let invalid = || format!("not a valid duration string: {}", go_quote(s));
    let b = s.as_bytes();
    let mut i = 0;
    let mut dur: u64 = 0;
    let mut last_unit_pos = 0;
    while i < b.len() {
        if !b[i].is_ascii_digit() {
            return Err(invalid());
        }
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        let v: u64 = s[start..i].parse().map_err(|_| invalid())?;
        let ustart = i;
        while i < b.len() && !b[i].is_ascii_digit() {
            i += 1;
        }
        if i == ustart {
            return Err(invalid());
        }
        let u = &s[ustart..i];
        let (pos, mult): (i32, u64) = match u {
            "ms" => (7, 1_000_000),
            "s" => (6, 1_000_000_000),
            "m" => (5, 60 * 1_000_000_000),
            "h" => (4, 3600 * 1_000_000_000),
            "d" => (3, 24 * 3600 * 1_000_000_000),
            "w" => (2, 7 * 24 * 3600 * 1_000_000_000),
            "y" => (1, 365 * 24 * 3600 * 1_000_000_000),
            _ => return Err(format!("unknown unit {} in duration {}", go_quote(u), go_quote(s))),
        };
        if pos <= last_unit_pos {
            return Err(invalid());
        }
        last_unit_pos = pos;
        if v > (1u64 << 63) / mult {
            return Err("duration out of range".to_string());
        }
        dur = dur.wrapping_add(v * mult);
        if dur > (1u64 << 63) - 1 {
            return Err("duration out of range".to_string());
        }
    }
    Ok(dur as i64)
}

/// Go `model.Duration(d).String()` for `d` in nanoseconds: `1h30m`, `90d`, `0s`.
pub fn format_duration(d: i64) -> String {
    let mut out = String::new();
    write_duration(&mut out, d);
    out
}

/// Appends Go `model.Duration(d).String()`.
pub fn write_duration(out: &mut String, d: i64) {
    let mut ms = d / MILLISECOND;
    if ms == 0 {
        out.push_str("0s");
        return;
    }
    if ms < 0 {
        out.push('-');
        ms = ms.wrapping_neg();
    }
    let mut f = |unit: &str, mult: i64, exact: bool| {
        if exact && ms % mult != 0 {
            return;
        }
        let v = ms / mult;
        if v > 0 {
            out.push_str(&v.to_string());
            out.push_str(unit);
            ms -= v * mult;
        }
    };
    // Only format years and weeks if the remainder is zero, as it is often easier to read 90d than 12w6d.
    f("y", 1000 * 60 * 60 * 24 * 365, true);
    f("w", 1000 * 60 * 60 * 24 * 7, true);
    f("d", 1000 * 60 * 60 * 24, false);
    f("h", 1000 * 60 * 60, false);
    f("m", 1000 * 60, false);
    f("s", 1000, false);
    f("ms", 1, false);
}

/// Go `time.Duration.Seconds`.
pub fn duration_seconds(d: i64) -> f64 {
    let sec = d / SECOND;
    let nsec = d % SECOND;
    sec as f64 + nsec as f64 / 1e9
}

/// Go `int64(f)` conversion of a float as amd64 does it: out of range and NaN give `i64::MIN`.
pub fn go_f64_to_i64(f: f64) -> i64 {
    const TWO63: f64 = (1u64 << 63) as f64;
    if f.is_nan() || f >= TWO63 || f < -TWO63 { i64::MIN } else { f as i64 }
}

/// Go `time.Duration(math.Round(seconds * float64(time.Second)))`.
pub fn seconds_to_duration(seconds: f64) -> i64 {
    go_f64_to_i64((seconds * SECOND as f64).round())
}
