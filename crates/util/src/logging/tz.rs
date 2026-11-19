//! The local time zone as Go's `time.Local` finds it (`TZ`, else `/etc/localtime`), and Go's RFC 3339 time
//! formats. The log time stamps of Prometheus use the local zone.
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// A loaded TZif zone: transitions, their offsets, and the POSIX rule for times after the last transition.
#[derive(Debug, Default)]
pub struct Zone {
    trans: Vec<i64>,
    idx: Vec<u8>,
    /// (offset east of UTC in seconds, is DST)
    types: Vec<(i32, bool)>,
    rule: Option<Rule>,
}

#[derive(Debug, Clone, Copy)]
enum RuleKind {
    Julian,
    Doy,
    Month,
}

#[derive(Debug, Clone, Copy)]
struct RuleDate {
    kind: RuleKind,
    day: i32,
    week: i32,
    mon: i32,
    time: i32,
}

#[derive(Debug, Clone, Copy)]
struct Rule {
    std_off: i32,
    dst: Option<(i32, RuleDate, RuleDate)>,
}

impl Zone {
    /// UTC.
    pub fn utc() -> Zone {
        Zone::default()
    }

    /// Parses TZif data (versions 1 to 4).
    pub fn parse(d: &[u8]) -> Option<Zone> {
        if d.len() < 44 || &d[..4] != b"TZif" {
            return None;
        }
        let ver = d[4];
        let counts = |b: &[u8]| -> Option<[usize; 6]> {
            let mut c = [0usize; 6];
            for (i, v) in c.iter_mut().enumerate() {
                let p = 20 + i * 4;
                *v = u32::from_be_bytes(b.get(p..p + 4)?.try_into().ok()?) as usize;
            }
            Some(c)
        };
        // [isutcnt, isstdcnt, leapcnt, timecnt, typecnt, charcnt]
        let mut c = counts(d)?;
        let mut p = 44;
        let mut tsize = 4;
        if ver >= b'2' {
            p += c[3] * 4 + c[3] + c[4] * 6 + c[5] + c[2] * 8 + c[1] + c[0];
            let h = d.get(p..)?;
            if h.len() < 44 || &h[..4] != b"TZif" {
                return None;
            }
            c = counts(h)?;
            p += 44;
            tsize = 8;
        }
        let mut z = Zone::default();
        let n = c[3];
        let tr = d.get(p..p + n * tsize)?;
        for i in 0..n {
            let b = &tr[i * tsize..(i + 1) * tsize];
            z.trans.push(if tsize == 8 {
                i64::from_be_bytes(b.try_into().ok()?)
            } else {
                i32::from_be_bytes(b.try_into().ok()?) as i64
            });
        }
        p += n * tsize;
        z.idx = d.get(p..p + n)?.to_vec();
        p += n;
        let ty = d.get(p..p + c[4] * 6)?;
        for i in 0..c[4] {
            let b = &ty[i * 6..i * 6 + 6];
            z.types.push((i32::from_be_bytes([b[0], b[1], b[2], b[3]]), b[4] != 0));
        }
        if z.types.is_empty() || z.idx.iter().any(|&i| i as usize >= z.types.len()) {
            return None;
        }
        p += c[4] * 6 + c[5] + c[2] * (tsize + 4) + c[1] + c[0];
        if ver >= b'2' {
            if let Some(rest) = d.get(p..) {
                if rest.first() == Some(&b'\n') {
                    if let Some(end) = rest[1..].iter().position(|&b| b == b'\n') {
                        if let Ok(s) = std::str::from_utf8(&rest[1..1 + end]) {
                            z.rule = parse_rule(s);
                        }
                    }
                }
            }
        }
        Some(z)
    }

    /// Offset east of UTC in seconds at the Unix time `t`.
    pub fn offset(&self, t: i64) -> i32 {
        if self.trans.is_empty() || t >= *self.trans.last().unwrap_or(&i64::MAX) {
            if let Some(r) = &self.rule {
                return r.offset(t);
            }
        }
        if self.trans.is_empty() || t < self.trans[0] {
            // Go's lookupFirstZone: the first standard-time type.
            return match self.types.iter().find(|t| !t.1) {
                Some(t) => t.0,
                None => self.types.first().map(|t| t.0).unwrap_or(0),
            };
        }
        let i = self.trans.partition_point(|&x| x <= t) - 1;
        self.types[self.idx[i] as usize].0
    }
}

/// Parses a POSIX TZ string such as `EST5EDT,M3.2.0,M11.1.0` or `<-03>3`.
fn parse_rule(s: &str) -> Option<Rule> {
    let b = s.as_bytes();
    let mut i = 0;
    let name = |i: &mut usize| -> Option<()> {
        if b.get(*i) == Some(&b'<') {
            *i += b[*i..].iter().position(|&c| c == b'>')? + 1;
        } else {
            let st = *i;
            while *i < b.len() && b[*i].is_ascii_alphabetic() {
                *i += 1;
            }
            if *i - st < 3 {
                return None;
            }
        }
        Some(())
    };
    let num = |i: &mut usize| -> Option<i32> {
        let st = *i;
        let mut n: i32 = 0;
        while *i < b.len() && b[*i].is_ascii_digit() {
            n = n.checked_mul(10)?.checked_add((b[*i] - b'0') as i32)?;
            *i += 1;
        }
        if *i == st { None } else { Some(n) }
    };
    // [+-]hh[:mm[:ss]] in seconds
    let off = |i: &mut usize| -> Option<i32> {
        let mut neg = false;
        if let Some(&c) = b.get(*i) {
            if c == b'+' || c == b'-' {
                neg = c == b'-';
                *i += 1;
            }
        }
        let mut v = num(i)? * 3600;
        for mul in [60, 1] {
            if b.get(*i) != Some(&b':') {
                break;
            }
            *i += 1;
            v += num(i)? * mul;
        }
        Some(if neg { -v } else { v })
    };
    name(&mut i)?;
    let std_off = -off(&mut i)?;
    if i == b.len() {
        return Some(Rule { std_off, dst: None });
    }
    name(&mut i)?;
    let dst_off = if i < b.len() && b[i] != b',' { -off(&mut i)? } else { std_off + 3600 };
    let date = |i: &mut usize| -> Option<RuleDate> {
        let mut r = RuleDate { kind: RuleKind::Doy, day: 0, week: 0, mon: 0, time: 2 * 3600 };
        match b.get(*i)? {
            b'J' => {
                *i += 1;
                r.kind = RuleKind::Julian;
                r.day = num(i)?;
            }
            b'M' => {
                *i += 1;
                r.kind = RuleKind::Month;
                r.mon = num(i)?;
                if b.get(*i) != Some(&b'.') {
                    return None;
                }
                *i += 1;
                r.week = num(i)?;
                if b.get(*i) != Some(&b'.') {
                    return None;
                }
                *i += 1;
                r.day = num(i)?;
            }
            _ => r.day = num(i)?,
        }
        if b.get(*i) == Some(&b'/') {
            *i += 1;
            r.time = off(i)?;
        }
        Some(r)
    };
    let (start, end) = if i == b.len() {
        // Go's default: US rules.
        let d = |mon, week| RuleDate { kind: RuleKind::Month, day: 0, week, mon, time: 2 * 3600 };
        (d(3, 2), d(11, 1))
    } else {
        if b[i] != b',' {
            return None;
        }
        i += 1;
        let s = date(&mut i)?;
        if b.get(i) != Some(&b',') {
            return None;
        }
        i += 1;
        let e = date(&mut i)?;
        (s, e)
    };
    Some(Rule { std_off, dst: Some((dst_off, start, end)) })
}

fn is_leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

const DAYS_BEFORE: [i32; 13] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365];

/// Seconds from the start of the UTC year to the rule's transition (Go `tzruleTime`).
fn rule_time(year: i64, r: &RuleDate, off: i32) -> i64 {
    let leap = is_leap(year);
    let s: i64 = match r.kind {
        RuleKind::Julian => {
            let mut s = (r.day as i64 - 1) * 86400;
            if leap && r.day >= 60 {
                s += 86400;
            }
            s
        }
        RuleKind::Doy => r.day as i64 * 86400,
        RuleKind::Month => {
            let m1 = (r.mon + 9) % 12 + 1;
            let mut yy0 = year;
            if r.mon <= 2 {
                yy0 -= 1;
            }
            let yy1 = yy0.div_euclid(100);
            let yy2 = yy0.rem_euclid(100);
            let mut dow = ((26 * m1 as i64 - 2) / 10 + 1 + yy2 + yy2 / 4 + yy1 / 4 - 2 * yy1) % 7;
            if dow < 0 {
                dow += 7;
            }
            let mut d = r.day as i64 - dow;
            if d < 0 {
                d += 7;
            }
            let mon = r.mon.clamp(1, 12) as usize;
            let mut days_in = (DAYS_BEFORE[mon] - DAYS_BEFORE[mon - 1]) as i64;
            if mon == 2 && leap {
                days_in += 1;
            }
            for _ in 1..r.week {
                if d + 7 >= days_in {
                    break;
                }
                d += 7;
            }
            d += DAYS_BEFORE[mon - 1] as i64;
            if leap && r.mon > 2 {
                d += 1;
            }
            d * 86400
        }
    };
    s + r.time as i64 - off as i64
}

impl Rule {
    fn offset(&self, t: i64) -> i32 {
        let Some((dst_off, start, end)) = &self.dst else { return self.std_off };
        let days = t.div_euclid(86400);
        let (year, _, _) = civil_from_days(days);
        let ystart = days_from_civil(year, 1, 1) * 86400;
        let ysec = t - ystart;
        let mut s = rule_time(year, start, self.std_off);
        let mut e = rule_time(year, end, *dst_off);
        let (mut in_dst, mut out_dst) = (*dst_off, self.std_off);
        if e < s {
            std::mem::swap(&mut s, &mut e);
            std::mem::swap(&mut in_dst, &mut out_dst);
        }
        if ysec < s || ysec >= e { out_dst } else { in_dst }
    }
}

/// (year, month 1-12, day 1-31) from days since 1970-01-01.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 } as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn load_file(path: &str) -> Option<Zone> {
    Zone::parse(&std::fs::read(path).ok()?)
}

/// Go's `initLocal`: `TZ` unset reads `/etc/localtime`; `TZ=""` and `TZ=UTC` are UTC; `TZ=:Name`, `TZ=Name` and
/// `TZ=/path` load that zone. A zone that does not load is UTC.
fn load_local() -> Zone {
    let z = match std::env::var("TZ") {
        Err(_) => load_file("/etc/localtime"),
        Ok(tz) => {
            let tz = tz.strip_prefix(':').unwrap_or(&tz);
            if tz.starts_with('/') {
                load_file(tz)
            } else if tz.is_empty() || tz == "UTC" || tz.contains("..") {
                None
            } else {
                ["/usr/share/zoneinfo/", "/usr/share/lib/zoneinfo/", "/usr/lib/locale/TZ/", "/etc/zoneinfo/"]
                    .iter()
                    .find_map(|d| load_file(&format!("{d}{tz}")))
            }
        }
    };
    z.unwrap_or_default()
}

/// The local zone, loaded once.
pub fn local() -> &'static Zone {
    static Z: OnceLock<Zone> = OnceLock::new();
    Z.get_or_init(load_local)
}

fn push_num(out: &mut String, v: u64, width: usize) {
    let mut b = [b'0'; 20];
    let mut i = b.len();
    let mut v = v;
    while v > 0 || b.len() - i < width {
        i -= 1;
        b[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    out.push_str(std::str::from_utf8(&b[i..]).unwrap_or(""));
}

/// Number of fraction digits written by [`append_rfc3339`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Frac {
    /// Exactly three digits (`.000`), as the slog text handler writes.
    Millis,
    /// Up to nine digits with trailing zeros removed (Go `time.RFC3339Nano`).
    Nanos,
    /// No fraction (Go `time.RFC3339`).
    None,
}

/// Appends `t` in the zone `z` as Go's `2006-01-02T15:04:05[.frac]Z07:00`.
pub fn append_rfc3339(out: &mut String, t: SystemTime, z: &Zone, frac: Frac) {
    let (secs, nanos) = match t.duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as i64, d.subsec_nanos()),
        Err(e) => {
            let d = e.duration();
            if d.subsec_nanos() == 0 {
                (-(d.as_secs() as i64), 0)
            } else {
                (-(d.as_secs() as i64) - 1, 1_000_000_000 - d.subsec_nanos())
            }
        }
    };
    let off = z.offset(secs);
    let local = secs + off as i64;
    let (y, m, d) = civil_from_days(local.div_euclid(86400));
    let sod = local.rem_euclid(86400) as u64;
    if y < 0 {
        out.push('-');
    }
    push_num(out, y.unsigned_abs(), 4);
    out.push('-');
    push_num(out, m as u64, 2);
    out.push('-');
    push_num(out, d as u64, 2);
    out.push('T');
    push_num(out, sod / 3600, 2);
    out.push(':');
    push_num(out, sod / 60 % 60, 2);
    out.push(':');
    push_num(out, sod % 60, 2);
    match frac {
        Frac::Millis => {
            out.push('.');
            push_num(out, (nanos / 1_000_000) as u64, 3);
        }
        Frac::Nanos => {
            if nanos != 0 {
                let mut n = nanos;
                let mut w = 9;
                while n % 10 == 0 {
                    n /= 10;
                    w -= 1;
                }
                out.push('.');
                push_num(out, n as u64, w);
            }
        }
        Frac::None => {}
    }
    if off == 0 {
        out.push('Z');
    } else {
        out.push(if off < 0 { '-' } else { '+' });
        let a = off.unsigned_abs() as u64;
        push_num(out, a / 3600, 2);
        out.push(':');
        push_num(out, a / 60 % 60, 2);
    }
}
