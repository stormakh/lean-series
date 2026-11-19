//! The parts of Go's `time` package the web API needs: an instant with nanoseconds (`time.Time`), RFC 3339
//! parsing and formatting as Go does it, `time.Duration` (`i64` nanoseconds) and its `String()`.
//!
//! Times are kept in UTC: formatting writes `Z`. Go's `time.Now()` is in the local zone; the API writes such times
//! (for example `lastScrape`) with the local offset. This port writes them in UTC (same instant).

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// Seconds from 0001-01-01 to 1970-01-01 (Go `unixToInternal`).
pub const UNIX_TO_INTERNAL: i64 = 62135596800;

/// Go `time.Duration`: nanoseconds.
pub type GoDuration = i64;

pub const NANOSECOND: GoDuration = 1;
pub const MICROSECOND: GoDuration = 1_000;
pub const MILLISECOND: GoDuration = 1_000_000;
pub const SECOND: GoDuration = 1_000_000_000;
pub const MINUTE: GoDuration = 60 * SECOND;
pub const HOUR: GoDuration = 60 * MINUTE;

/// An instant with nanosecond precision (Go `time.Time`). Equality is Go's `Equal` (same instant).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Time {
    /// Seconds since the Unix epoch.
    pub secs: i64,
    /// Nanoseconds in the second, 0..1e9.
    pub nsec: u32,
}

impl Default for Time {
    /// Go's zero time, 0001-01-01T00:00:00Z.
    fn default() -> Time {
        Time::ZERO
    }
}

impl fmt::Debug for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.format_rfc3339_nano())
    }
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.format_rfc3339_nano())
    }
}

impl Time {
    /// Go's zero `time.Time{}`.
    pub const ZERO: Time = Time { secs: -UNIX_TO_INTERNAL, nsec: 0 };

    /// Go `time.Unix(sec, nsec)`.
    pub fn unix(sec: i64, nsec: i64) -> Time {
        let mut sec = sec;
        let mut nsec = nsec;
        if !(0..1_000_000_000).contains(&nsec) {
            let n = nsec / 1_000_000_000;
            sec = sec.wrapping_add(n);
            nsec -= n * 1_000_000_000;
            if nsec < 0 {
                nsec += 1_000_000_000;
                sec = sec.wrapping_sub(1);
            }
        }
        Time { secs: sec, nsec: nsec as u32 }
    }

    /// Go `time.UnixMilli(ms)` (and `timestamp.Time`).
    pub fn from_millis(ms: i64) -> Time {
        Time::unix(ms.div_euclid(1000), ms.rem_euclid(1000) * 1_000_000)
    }

    /// Go `time.Now()`.
    pub fn now() -> Time {
        Time::from_system(SystemTime::now())
    }

    pub fn from_system(t: SystemTime) -> Time {
        match t.duration_since(UNIX_EPOCH) {
            Ok(d) => Time { secs: d.as_secs() as i64, nsec: d.subsec_nanos() },
            Err(e) => {
                let d = e.duration();
                Time::unix(-(d.as_secs() as i64), -(d.subsec_nanos() as i64))
            }
        }
    }

    /// Go `t.IsZero()`.
    pub fn is_zero(&self) -> bool {
        *self == Time::ZERO
    }

    /// Go `t.UnixMilli()` (and `timestamp.FromTime`), with Go's wrapping arithmetic.
    pub fn unix_milli(&self) -> i64 {
        self.secs.wrapping_mul(1000).wrapping_add((self.nsec / 1_000_000) as i64)
    }

    /// Go `t.UnixNano()`.
    pub fn unix_nano(&self) -> i64 {
        self.secs.wrapping_mul(1_000_000_000).wrapping_add(self.nsec as i64)
    }

    /// Go `t.Add(d)`.
    pub fn add(&self, d: GoDuration) -> Time {
        let dsec = d / 1_000_000_000;
        let mut nsec = self.nsec as i64 + d % 1_000_000_000;
        let mut sec = self.secs.wrapping_add(dsec);
        if nsec >= 1_000_000_000 {
            sec = sec.wrapping_add(1);
            nsec -= 1_000_000_000;
        } else if nsec < 0 {
            sec = sec.wrapping_sub(1);
            nsec += 1_000_000_000;
        }
        Time { secs: sec, nsec: nsec as u32 }
    }

    /// Go `t.Sub(u)`, saturating as Go does.
    pub fn sub(&self, u: &Time) -> GoDuration {
        let ds = (self.secs as i128 - u.secs as i128) * 1_000_000_000 + (self.nsec as i128 - u.nsec as i128);
        if ds > i64::MAX as i128 {
            i64::MAX
        } else if ds < i64::MIN as i128 {
            i64::MIN
        } else {
            ds as i64
        }
    }

    /// Go `t.Before(u)`.
    pub fn before(&self, u: &Time) -> bool {
        self < u
    }

    /// Go `t.After(u)`.
    pub fn after(&self, u: &Time) -> bool {
        self > u
    }

    /// Go `t.Format(time.RFC3339Nano)` for a UTC time.
    pub fn format_rfc3339_nano(&self) -> String {
        let mut s = String::with_capacity(32);
        self.write_rfc3339(&mut s, true);
        s
    }

    /// Go `t.Format(time.RFC3339)` for a UTC time.
    pub fn format_rfc3339(&self) -> String {
        let mut s = String::with_capacity(24);
        self.write_rfc3339(&mut s, false);
        s
    }

    /// Writes RFC 3339 (with the trimmed nanosecond fraction when `nano`), as Go formats a UTC time.
    pub fn write_rfc3339(&self, out: &mut String, nano: bool) {
        let days = self.secs.div_euclid(86400);
        let rem = self.secs.rem_euclid(86400);
        let (y, mo, d) = civil_from_days(days);
        if y < 0 {
            out.push('-');
        }
        let ya = y.unsigned_abs();
        let ys = ya.to_string();
        for _ in ys.len()..4 {
            out.push('0');
        }
        out.push_str(&ys);
        out.push('-');
        push2(out, mo as u32);
        out.push('-');
        push2(out, d as u32);
        out.push('T');
        push2(out, (rem / 3600) as u32);
        out.push(':');
        push2(out, (rem % 3600 / 60) as u32);
        out.push(':');
        push2(out, (rem % 60) as u32);
        if nano && self.nsec != 0 {
            let f = format!("{:09}", self.nsec);
            out.push('.');
            out.push_str(f.trim_end_matches('0'));
        }
        out.push('Z');
    }

    /// Writes the time as Go's `json.Marshal(time.Time)` does: a quoted RFC 3339 string with nanoseconds.
    pub fn write_json(&self, out: &mut String) {
        out.push('"');
        self.write_rfc3339(out, true);
        out.push('"');
    }

    /// Go `time.Parse(time.RFC3339Nano, s)` (and `time.RFC3339`, which accepts fractions too).
    pub fn parse_rfc3339(s: &str) -> Result<Time, String> {
        let err = || format!("parsing time {} as \"2006-01-02T15:04:05.999999999Z07:00\": cannot parse", go_quote(s));
        let b = s.as_bytes();
        if b.len() < 20 {
            return Err(err());
        }
        let num = |from: usize, n: usize| -> Option<i64> {
            let p = b.get(from..from + n)?;
            let mut v = 0i64;
            for c in p {
                if !c.is_ascii_digit() {
                    return None;
                }
                v = v * 10 + (c - b'0') as i64;
            }
            Some(v)
        };
        let year = num(0, 4).ok_or_else(err)?;
        if b[4] != b'-' || b[7] != b'-' || (b[10] != b'T' && b[10] != b't') || b[13] != b':' || b[16] != b':' {
            return Err(err());
        }
        let month = num(5, 2).ok_or_else(err)?;
        let day = num(8, 2).ok_or_else(err)?;
        let hour = num(11, 2).ok_or_else(err)?;
        let min = num(14, 2).ok_or_else(err)?;
        let sec = num(17, 2).ok_or_else(err)?;
        let mut i = 19;
        let mut nsec: i64 = 0;
        if i < b.len() && (b[i] == b'.' || b[i] == b',') {
            i += 1;
            let st = i;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            let digits = &s[st..i];
            if digits.is_empty() {
                return Err(err());
            }
            let mut n: i64 = 0;
            for (k, c) in digits.bytes().enumerate() {
                if k < 9 {
                    n = n * 10 + (c - b'0') as i64;
                }
            }
            for _ in digits.len()..9 {
                n *= 10;
            }
            nsec = n;
        }
        if i >= b.len() {
            return Err(err());
        }
        let offset: i64 = if b[i] == b'Z' || b[i] == b'z' {
            if i + 1 != b.len() {
                return Err(err());
            }
            0
        } else if b[i] == b'+' || b[i] == b'-' {
            if b.len() != i + 6 || b[i + 3] != b':' {
                return Err(err());
            }
            let oh = num(i + 1, 2).ok_or_else(err)?;
            let om = num(i + 4, 2).ok_or_else(err)?;
            if oh >= 24 || om >= 60 {
                return Err(err());
            }
            let o = oh * 3600 + om * 60;
            if b[i] == b'-' { -o } else { o }
        } else {
            return Err(err());
        };
        if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month as u32) || hour >= 24 || min >= 60 || sec >= 60 {
            return Err(err());
        }
        let days = days_from_civil(year, month as u32, day as u32);
        let secs = days * 86400 + hour * 3600 + min * 60 + sec - offset;
        Ok(Time { secs, nsec: nsec as u32 })
    }
}

fn days_in_month(y: i64, m: u32) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
    }
}

fn push2(s: &mut String, n: u32) {
    s.push((b'0' + (n / 10 % 10) as u8) as char);
    s.push((b'0' + (n % 10) as u8) as char);
}

/// (year, month 1-12, day 1-31) from days since 1970-01-01 (proleptic Gregorian, any year).
pub fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days as i128 + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    ((if m <= 2 { y + 1 } else { y }) as i64, m as i64, d as i64)
}

/// Days since 1970-01-01 from a civil date.
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let m = m as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Go `strconv.Quote` for the error texts (printable ASCII as is).
pub fn go_quote(s: &str) -> String {
    promql::parser::quote::go_quote(s)
}

/// Go `time.Duration.String()`.
pub fn duration_string(d: GoDuration) -> String {
    if d == i64::MIN {
        return "-2562047h47m16.854775808s".to_string();
    }
    if d < 0 {
        let mut s = String::from("-");
        s.push_str(&util::stats::go_duration_string(std::time::Duration::from_nanos((-d) as u64)));
        return s;
    }
    util::stats::go_duration_string(std::time::Duration::from_nanos(d as u64))
}

/// Go `d.Seconds()`.
pub fn duration_seconds(d: GoDuration) -> f64 {
    let sec = d / SECOND;
    let nsec = d % SECOND;
    sec as f64 + nsec as f64 / 1e9
}

/// Go `strconv.ParseFloat(s, 64)`: `Some` only when Go returns no error (decimal and special forms; out of range
/// values fail as in Go). Hexadecimal floats and underscores are not accepted.
pub fn go_parse_float(s: &str) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    let lower = s.to_ascii_lowercase();
    let body = lower.strip_prefix(['+', '-']).unwrap_or(&lower);
    match body {
        "inf" | "infinity" => {
            return Some(if lower.starts_with('-') { f64::NEG_INFINITY } else { f64::INFINITY });
        }
        "nan" => {
            return if lower.starts_with(['+', '-']) { None } else { Some(f64::NAN) };
        }
        _ => {}
    }
    // Rust accepts some forms Go does not ("inf" handled above); restrict to Go's decimal syntax.
    let bb = body.as_bytes();
    let mut i = 0;
    let mut digits = 0;
    while i < bb.len() && bb[i].is_ascii_digit() {
        i += 1;
        digits += 1;
    }
    if i < bb.len() && bb[i] == b'.' {
        i += 1;
        while i < bb.len() && bb[i].is_ascii_digit() {
            i += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return None;
    }
    if i < bb.len() && bb[i] == b'e' {
        i += 1;
        if i < bb.len() && (bb[i] == b'+' || bb[i] == b'-') {
            i += 1;
        }
        let st = i;
        while i < bb.len() && bb[i].is_ascii_digit() {
            i += 1;
        }
        if i == st {
            return None;
        }
    }
    if i != bb.len() {
        return None;
    }
    let v: f64 = s.parse().ok()?;
    if v.is_infinite() {
        return None;
    }
    Some(v)
}
