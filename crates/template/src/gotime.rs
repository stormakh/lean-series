//! Go `time.Time` (UTC only), `time.Duration`, `time.Month` and `time.Weekday` as templates use them: the
//! `Format` layout engine, `String` methods and the methods templates call.

/// Nanoseconds per unit (Go `time.Nanosecond` ...).
pub const NANOSECOND: i64 = 1;
pub const MICROSECOND: i64 = 1000;
pub const MILLISECOND: i64 = 1_000_000;
pub const SECOND: i64 = 1_000_000_000;
pub const MINUTE: i64 = 60 * SECOND;
pub const HOUR: i64 = 60 * MINUTE;

/// Seconds from 0001-01-01 to 1970-01-01.
const UNIX_TO_INTERNAL: i64 = 62_135_596_800;

/// A point in time (Go `time.Time` in the UTC location): seconds since the Unix epoch and nanoseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GoTime {
    pub sec: i64,
    pub nsec: u32,
}

const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const DAYS: [&str; 7] = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

/// Go `time.Month.String`.
pub fn month_string(m: i64) -> String {
    if (1..=12).contains(&m) {
        MONTHS[(m - 1) as usize].to_string()
    } else {
        format!("%!Month({})", m)
    }
}

/// Go `time.Weekday.String`.
pub fn weekday_string(d: i64) -> String {
    if (0..7).contains(&d) {
        DAYS[d as usize].to_string()
    } else {
        format!("%!Weekday({})", d)
    }
}

/// Days since 1970-01-01 to (year, month, day) in the proleptic Gregorian calendar.
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// (year, month, day) to days since 1970-01-01.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn is_leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

impl GoTime {
    /// The zero time, 0001-01-01 00:00:00 UTC.
    pub const ZERO: GoTime = GoTime { sec: -UNIX_TO_INTERNAL, nsec: 0 };

    /// Go `time.Unix(0, ns).UTC()`.
    pub fn from_unix_nano(ns: i64) -> GoTime {
        GoTime { sec: ns.div_euclid(SECOND), nsec: ns.rem_euclid(SECOND) as u32 }
    }

    /// Go `time.UnixMilli(ms).UTC()`.
    pub fn from_unix_milli(ms: i64) -> GoTime {
        GoTime { sec: ms.div_euclid(1000), nsec: (ms.rem_euclid(1000) * MILLISECOND) as u32 }
    }

    fn days(&self) -> i64 {
        self.sec.div_euclid(86_400)
    }

    /// (year, month, day).
    pub fn date(&self) -> (i64, i64, i64) {
        civil_from_days(self.days())
    }

    /// (hour, minute, second).
    pub fn clock(&self) -> (i64, i64, i64) {
        let s = self.sec.rem_euclid(86_400);
        (s / 3600, s / 60 % 60, s % 60)
    }

    /// Day of the year, 1..=366.
    pub fn year_day(&self) -> i64 {
        let (y, _, _) = self.date();
        self.days() - days_from_civil(y, 1, 1) + 1
    }

    /// Day of the week, 0 is Sunday.
    pub fn weekday(&self) -> i64 {
        (self.days() + 4).rem_euclid(7)
    }

    /// Go `Unix`.
    pub fn unix(&self) -> i64 {
        self.sec
    }

    /// Go `UnixMilli`.
    pub fn unix_milli(&self) -> i64 {
        self.sec.wrapping_mul(1000).wrapping_add(self.nsec as i64 / MILLISECOND)
    }

    /// Go `UnixMicro`.
    pub fn unix_micro(&self) -> i64 {
        self.sec.wrapping_mul(1_000_000).wrapping_add(self.nsec as i64 / MICROSECOND)
    }

    /// Go `UnixNano`.
    pub fn unix_nano(&self) -> i64 {
        self.sec.wrapping_mul(SECOND).wrapping_add(self.nsec as i64)
    }

    /// Go `Add`.
    pub fn add(&self, d: i64) -> GoTime {
        let mut sec = self.sec.wrapping_add(d / SECOND);
        let mut nsec = self.nsec as i64 + d % SECOND;
        if nsec >= SECOND {
            sec += 1;
            nsec -= SECOND;
        } else if nsec < 0 {
            sec -= 1;
            nsec += SECOND;
        }
        GoTime { sec, nsec: nsec as u32 }
    }

    /// Go `Sub` (saturating at the extremes of Duration, as Go does).
    pub fn sub(&self, u: &GoTime) -> i64 {
        let d = (self.sec as i128 - u.sec as i128) * SECOND as i128 + (self.nsec as i128 - u.nsec as i128);
        d.clamp(i64::MIN as i128, i64::MAX as i128) as i64
    }

    /// Go `AddDate`.
    pub fn add_date(&self, years: i64, months: i64, days: i64) -> GoTime {
        let (y, m, d) = self.date();
        let (h, mi, s) = self.clock();
        let total_m = y * 12 + (m - 1) + years * 12 + months;
        let ny = total_m.div_euclid(12);
        let nm = total_m.rem_euclid(12) + 1;
        let day = days_from_civil(ny, nm, 1) + (d - 1) + days;
        GoTime { sec: day * 86_400 + h * 3600 + mi * 60 + s, nsec: self.nsec }
    }

    /// Go `IsZero`.
    pub fn is_zero(&self) -> bool {
        *self == GoTime::ZERO
    }

    /// Go `Truncate` (relative to the zero time, as Go does).
    pub fn truncate(&self, d: i64) -> GoTime {
        if d <= 0 {
            return *self;
        }
        let r = self.mod_since_zero(d);
        self.add(-r)
    }

    /// Go `Round`.
    pub fn round(&self, d: i64) -> GoTime {
        if d <= 0 {
            return *self;
        }
        let r = self.mod_since_zero(d);
        if r + r < d { self.add(-r) } else { self.add(d - r) }
    }

    fn mod_since_zero(&self, d: i64) -> i64 {
        let total = (self.sec as i128 + UNIX_TO_INTERNAL as i128) * SECOND as i128 + self.nsec as i128;
        total.rem_euclid(d as i128) as i64
    }

    /// Go `Format` (UTC location).
    pub fn format(&self, layout: &str) -> String {
        let mut b = String::with_capacity(layout.len() + 10);
        self.append_format(&mut b, layout);
        b
    }

    /// Go `String`: `2006-01-02 15:04:05.999999999 -0700 MST`.
    pub fn string(&self) -> String {
        self.format("2006-01-02 15:04:05.999999999 -0700 MST")
    }

    fn append_format(&self, b: &mut String, mut layout: &str) {
        let (year, month, day) = self.date();
        let yday = self.year_day();
        let (hour, min, sec) = self.clock();
        while !layout.is_empty() {
            let (prefix, std, suffix) = next_std_chunk(layout);
            b.push_str(prefix);
            let Some(std) = std else { break };
            layout = suffix;
            match std {
                Std::Year => {
                    append_int(b, year.abs() % 100, 2);
                }
                Std::LongYear => append_int(b, year, 4),
                Std::Month => b.push_str(&MONTHS[(month - 1) as usize][..3]),
                Std::LongMonth => b.push_str(MONTHS[(month - 1) as usize]),
                Std::NumMonth => append_int(b, month, 0),
                Std::ZeroMonth => append_int(b, month, 2),
                Std::WeekDay => b.push_str(&DAYS[self.weekday() as usize][..3]),
                Std::LongWeekDay => b.push_str(DAYS[self.weekday() as usize]),
                Std::Day => append_int(b, day, 0),
                Std::UnderDay => {
                    if day < 10 {
                        b.push(' ');
                    }
                    append_int(b, day, 0);
                }
                Std::ZeroDay => append_int(b, day, 2),
                Std::UnderYearDay => {
                    if yday < 100 {
                        b.push(' ');
                        if yday < 10 {
                            b.push(' ');
                        }
                    }
                    append_int(b, yday, 0);
                }
                Std::ZeroYearDay => append_int(b, yday, 3),
                Std::Hour => append_int(b, hour, 2),
                Std::Hour12 => {
                    let hr = if hour % 12 == 0 { 12 } else { hour % 12 };
                    append_int(b, hr, 0);
                }
                Std::ZeroHour12 => {
                    let hr = if hour % 12 == 0 { 12 } else { hour % 12 };
                    append_int(b, hr, 2);
                }
                Std::Minute => append_int(b, min, 0),
                Std::ZeroMinute => append_int(b, min, 2),
                Std::Second => append_int(b, sec, 0),
                Std::ZeroSecond => append_int(b, sec, 2),
                Std::PM => b.push_str(if hour >= 12 { "PM" } else { "AM" }),
                Std::Pm => b.push_str(if hour >= 12 { "pm" } else { "am" }),
                Std::Tz(tz) => {
                    // UTC: offset 0.
                    if tz.iso {
                        b.push('Z');
                    } else {
                        b.push_str("+00");
                        if tz.colon {
                            b.push(':');
                        }
                        if !tz.short {
                            b.push_str("00");
                        }
                        if tz.seconds {
                            if tz.colon {
                                b.push(':');
                            }
                            b.push_str("00");
                        }
                    }
                }
                Std::TzName => b.push_str("UTC"),
                Std::Frac { trim, n, sep } => append_nano(b, self.nsec as i64, trim, n, sep),
            }
        }
    }
}

#[derive(Clone, Copy)]
struct TzSpec {
    iso: bool,
    colon: bool,
    short: bool,
    seconds: bool,
}

#[derive(Clone, Copy)]
enum Std {
    LongMonth,
    Month,
    NumMonth,
    ZeroMonth,
    LongWeekDay,
    WeekDay,
    Day,
    UnderDay,
    ZeroDay,
    UnderYearDay,
    ZeroYearDay,
    Hour,
    Hour12,
    ZeroHour12,
    Minute,
    ZeroMinute,
    Second,
    ZeroSecond,
    LongYear,
    Year,
    PM,
    Pm,
    TzName,
    Tz(TzSpec),
    Frac { trim: bool, n: usize, sep: u8 },
}

fn starts_with_lower(s: &str) -> bool {
    s.as_bytes().first().is_some_and(|c| c.is_ascii_lowercase())
}

/// Go `nextStdChunk`.
fn next_std_chunk(layout: &str) -> (&str, Option<Std>, &str) {
    let l = layout.as_bytes();
    let n = l.len();
    let has = |i: usize, s: &str| layout[i..].starts_with(s);
    for i in 0..n {
        let c = l[i];
        match c {
            b'J' => {
                if has(i, "Jan") {
                    if has(i, "January") {
                        return (&layout[..i], Some(Std::LongMonth), &layout[i + 7..]);
                    }
                    if !starts_with_lower(&layout[i + 3..]) {
                        return (&layout[..i], Some(Std::Month), &layout[i + 3..]);
                    }
                }
            }
            b'M' => {
                if n >= i + 3 {
                    if has(i, "Mon") {
                        if has(i, "Monday") {
                            return (&layout[..i], Some(Std::LongWeekDay), &layout[i + 6..]);
                        }
                        if !starts_with_lower(&layout[i + 3..]) {
                            return (&layout[..i], Some(Std::WeekDay), &layout[i + 3..]);
                        }
                    }
                    if has(i, "MST") {
                        return (&layout[..i], Some(Std::TzName), &layout[i + 3..]);
                    }
                }
            }
            b'0' => {
                if n >= i + 2 && (b'1'..=b'6').contains(&l[i + 1]) {
                    let std = [Std::ZeroMonth, Std::ZeroDay, Std::ZeroHour12, Std::ZeroMinute, Std::ZeroSecond, Std::Year]
                        [(l[i + 1] - b'1') as usize];
                    return (&layout[..i], Some(std), &layout[i + 2..]);
                }
                if n >= i + 3 && l[i + 1] == b'0' && l[i + 2] == b'2' {
                    return (&layout[..i], Some(Std::ZeroYearDay), &layout[i + 3..]);
                }
            }
            b'1' => {
                if n >= i + 2 && l[i + 1] == b'5' {
                    return (&layout[..i], Some(Std::Hour), &layout[i + 2..]);
                }
                return (&layout[..i], Some(Std::NumMonth), &layout[i + 1..]);
            }
            b'2' => {
                if has(i, "2006") {
                    return (&layout[..i], Some(Std::LongYear), &layout[i + 4..]);
                }
                return (&layout[..i], Some(Std::Day), &layout[i + 1..]);
            }
            b'_' => {
                if n >= i + 2 && l[i + 1] == b'2' {
                    if n >= i + 5 && &layout[i + 1..i + 5] == "2006" {
                        return (&layout[..i + 1], Some(Std::LongYear), &layout[i + 5..]);
                    }
                    return (&layout[..i], Some(Std::UnderDay), &layout[i + 2..]);
                }
                if n >= i + 3 && l[i + 1] == b'_' && l[i + 2] == b'2' {
                    return (&layout[..i], Some(Std::UnderYearDay), &layout[i + 3..]);
                }
            }
            b'3' => return (&layout[..i], Some(Std::Hour12), &layout[i + 1..]),
            b'4' => return (&layout[..i], Some(Std::Minute), &layout[i + 1..]),
            b'5' => return (&layout[..i], Some(Std::Second), &layout[i + 1..]),
            b'P' => {
                if n >= i + 2 && l[i + 1] == b'M' {
                    return (&layout[..i], Some(Std::PM), &layout[i + 2..]);
                }
            }
            b'p' => {
                if n >= i + 2 && l[i + 1] == b'm' {
                    return (&layout[..i], Some(Std::Pm), &layout[i + 2..]);
                }
            }
            b'-' | b'Z' => {
                let iso = c == b'Z';
                let forms: [(&str, bool, bool, bool); 5] = [
                    ("070000", false, false, true),
                    ("07:00:00", true, false, true),
                    ("0700", false, false, false),
                    ("07:00", true, false, false),
                    ("07", false, true, false),
                ];
                for (f, colon, short, seconds) in forms {
                    if layout[i + 1..].starts_with(f) {
                        return (
                            &layout[..i],
                            Some(Std::Tz(TzSpec { iso, colon, short, seconds })),
                            &layout[i + 1 + f.len()..],
                        );
                    }
                }
            }
            b'.' | b',' => {
                if i + 1 < n && (l[i + 1] == b'0' || l[i + 1] == b'9') {
                    let ch = l[i + 1];
                    let mut j = i + 1;
                    while j < n && l[j] == ch {
                        j += 1;
                    }
                    if !(j < n && l[j].is_ascii_digit()) {
                        let std = Std::Frac { trim: ch == b'9', n: (j - (i + 1)) & 0xfff, sep: c };
                        return (&layout[..i], Some(std), &layout[j..]);
                    }
                }
            }
            _ => {}
        }
    }
    (layout, None, "")
}

/// Go `appendInt` (time package).
fn append_int(b: &mut String, x: i64, width: usize) {
    let u = if x < 0 {
        b.push('-');
        x.unsigned_abs()
    } else {
        x as u64
    };
    let s = u.to_string();
    for _ in s.len()..width {
        b.push('0');
    }
    b.push_str(&s);
}

fn append_nano(b: &mut String, nanosec: i64, trim: bool, n: usize, sep: u8) {
    if trim && (n == 0 || nanosec == 0) {
        return;
    }
    let mut s = String::with_capacity(10);
    s.push(sep as char);
    append_int(&mut s, nanosec, 9);
    if n < 9 {
        s.truncate(s.len() - 9 + n);
    }
    if trim {
        while s.ends_with('0') {
            s.pop();
        }
        if s.len() == 1 {
            s.pop();
        }
    }
    b.push_str(&s);
}

/// Go `time.Duration.String`.
pub fn duration_string(d: i64) -> String {
    let mut buf = [0u8; 32];
    let mut w = buf.len();
    let neg = d < 0;
    let mut u = d.unsigned_abs();
    if u < SECOND as u64 {
        let prec;
        w -= 1;
        buf[w] = b's';
        w -= 1;
        if u == 0 {
            buf[w] = b'0';
            return String::from_utf8_lossy(&buf[w..]).into_owned();
        } else if u < MICROSECOND as u64 {
            prec = 0;
            buf[w] = b'n';
        } else if u < MILLISECOND as u64 {
            prec = 3;
            // U+00B5 micro sign is 0xC2 0xB5.
            w -= 1;
            buf[w] = 0xC2;
            buf[w + 1] = 0xB5;
        } else {
            prec = 6;
            buf[w] = b'm';
        }
        (w, u) = fmt_frac(&mut buf, w, u, prec);
        w = fmt_int(&mut buf, w, u);
    } else {
        w -= 1;
        buf[w] = b's';
        (w, u) = fmt_frac(&mut buf, w, u, 9);
        w = fmt_int(&mut buf, w, u % 60);
        u /= 60;
        if u > 0 {
            w -= 1;
            buf[w] = b'm';
            w = fmt_int(&mut buf, w, u % 60);
            u /= 60;
            if u > 0 {
                w -= 1;
                buf[w] = b'h';
                w = fmt_int(&mut buf, w, u);
            }
        }
    }
    if neg {
        w -= 1;
        buf[w] = b'-';
    }
    String::from_utf8_lossy(&buf[w..]).into_owned()
}

fn fmt_frac(buf: &mut [u8], mut w: usize, mut v: u64, prec: usize) -> (usize, u64) {
    let mut print = false;
    for _ in 0..prec {
        let digit = v % 10;
        print = print || digit != 0;
        if print {
            w -= 1;
            buf[w] = digit as u8 + b'0';
        }
        v /= 10;
    }
    if print {
        w -= 1;
        buf[w] = b'.';
    }
    (w, v)
}

fn fmt_int(buf: &mut [u8], mut w: usize, mut v: u64) -> usize {
    if v == 0 {
        w -= 1;
        buf[w] = b'0';
    } else {
        while v > 0 {
            w -= 1;
            buf[w] = (v % 10) as u8 + b'0';
            v /= 10;
        }
    }
    w
}

/// Go `Duration.Round`.
pub fn duration_round(d: i64, m: i64) -> i64 {
    if m <= 0 {
        return d;
    }
    let mut r = d % m;
    if d < 0 {
        r = -r;
        if r + r < m {
            return d + r;
        }
        if let Some(d1) = (d - m).checked_add(r) {
            if d1 < d {
                return d1;
            }
        }
        return i64::MIN;
    }
    if r + r < m {
        return d - r;
    }
    if let Some(d1) = (d + m).checked_sub(r) {
        if d1 > d {
            return d1;
        }
    }
    i64::MAX
}

/// Go `Duration.Truncate`.
pub fn duration_truncate(d: i64, m: i64) -> i64 {
    if m <= 0 { d } else { d - d % m }
}

/// Whether `y` is a leap year (exported for tests of the calendar code).
pub fn is_leap_year(y: i64) -> bool {
    is_leap(y)
}
