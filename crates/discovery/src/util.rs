//! Small Go helpers the SD providers share.
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Go `strutil.SanitizeLabelName`: every character outside `[a-zA-Z0-9_]` becomes `_`.
pub fn sanitize_label_name(name: &str) -> String {
    name.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect()
}

/// Go `net.JoinHostPort`: "host:port", with brackets around a host that contains a colon (IPv6).
pub fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') { format!("[{host}]:{port}") } else { format!("{host}:{port}") }
}

/// Parses an RFC 3339 / ISO 8601 time ("2006-01-02T15:04:05Z", fractions and offsets allowed).
pub fn parse_rfc3339(s: &str) -> Option<SystemTime> {
    let s = s.trim();
    if s.len() < 19 || !s.is_char_boundary(19) {
        return None;
    }
    let num = |a: usize, b: usize| s.get(a..b).and_then(|x| x.parse::<i64>().ok());
    let (y, mo, d, h, mi, se) = (num(0, 4)?, num(5, 7)?, num(8, 10)?, num(11, 13)?, num(14, 16)?, num(17, 19)?);
    let mut rest = &s[19..];
    let mut nanos = 0u32;
    if let Some(r) = rest.strip_prefix('.') {
        let n = r.bytes().take_while(|c| c.is_ascii_digit()).count();
        let mut v: u64 = 0;
        for (i, c) in r[..n].bytes().take(9).enumerate() {
            v += (c - b'0') as u64 * 10u64.pow(8 - i as u32);
        }
        nanos = v as u32;
        rest = &r[n..];
    }
    let offset = match rest {
        "" | "Z" | "z" => 0,
        o if o.len() == 6 && (o.starts_with('+') || o.starts_with('-')) => {
            let hh: i64 = o[1..3].parse().ok()?;
            let mm: i64 = o[4..6].parse().ok()?;
            let v = hh * 3600 + mm * 60;
            if o.starts_with('-') { -v } else { v }
        }
        _ => return None,
    };
    // days from civil (Howard Hinnant)
    let yy = if mo <= 2 { y - 1 } else { y };
    let era = yy.div_euclid(400);
    let yoe = yy - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    let secs = days * 86400 + h * 3600 + mi * 60 + se - offset;
    if secs < 0 {
        return None;
    }
    Some(UNIX_EPOCH + Duration::new(secs as u64, nanos))
}

/// Go `t.UTC().Format(time.RFC3339)`: "2006-01-02T15:04:05Z" (fractional seconds dropped).
pub fn format_rfc3339(t: SystemTime) -> String {
    let secs = match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        Err(e) => {
            let d = e.duration();
            -(d.as_secs() as i64) - if d.subsec_nanos() > 0 { 1 } else { 0 }
        }
    };
    format_unix_rfc3339(secs)
}

/// `format_rfc3339` of Unix seconds.
pub fn format_unix_rfc3339(secs: i64) -> String {
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", rem / 3600, rem % 3600 / 60, rem % 60)
}

/// Go `t.Format(time.RFC3339)` of a time parsed from an RFC 3339 string: the parsed zone offset is kept ("Z" for
/// a zero offset) and fractional seconds are dropped, as for times the AWS SDKs decode.
pub fn reformat_rfc3339(s: &str) -> Option<String> {
    let t = parse_rfc3339(s)?;
    let s = s.trim();
    let tail = &s[s.len().saturating_sub(6)..];
    let offset: i64 = if (tail.starts_with('+') || tail.starts_with('-')) && tail.as_bytes().get(3) == Some(&b':') {
        let v = tail[1..3].parse::<i64>().ok()? * 3600 + tail[4..6].parse::<i64>().ok()? * 60;
        if tail.starts_with('-') { -v } else { v }
    } else {
        0
    };
    let secs = t.duration_since(UNIX_EPOCH).ok()?.as_secs() as i64;
    let local = format_unix_rfc3339(secs + offset);
    if offset == 0 {
        return Some(local);
    }
    let sign = if offset < 0 { '-' } else { '+' };
    let a = offset.abs();
    Some(format!("{}{sign}{:02}:{:02}", &local[..local.len() - 1], a / 3600, a % 3600 / 60))
}
