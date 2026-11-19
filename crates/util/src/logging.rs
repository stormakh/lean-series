//! Port of `github.com/prometheus/common/promslog` (the logger of the Prometheus binaries) on top of Go's
//! `log/slog` text and JSON handlers: a level that can change at run time ([`Level`]), the `logfmt` and `json`
//! formats ([`Format`]), and a [`Handler`] that writes one record per line.
//!
//! Records are `time=... level=INFO msg="..." key=value ...` (logfmt) or
//! `{"time":"...","level":"INFO","msg":"...","key":"value"}` (json), with slog's quoting rules.
//!
//! Differences from Go:
//! - The time is UTC (`2006-01-02T15:04:05.000Z`); Go prints the local time zone. This port has no time zone
//!   database.
//! - There is no `source=file.go:line` attribute (Rust has no cheap caller lookup that would name Go files).
//! - Attribute values are strings: in JSON every value is a JSON string (Go writes numbers and booleans bare).

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicI8, Ordering};

/// The level names that `--log.level` accepts (Go `LevelFlagOptions`).
pub const LEVEL_FLAG_OPTIONS: [&str; 4] = ["debug", "info", "warn", "error"];
/// The formats that `--log.format` accepts (Go `FormatFlagOptions`).
pub const FORMAT_FLAG_OPTIONS: [&str; 2] = ["logfmt", "json"];

/// The help of `--log.level` (Go `promslogflag.LevelFlagHelp`).
pub const LEVEL_FLAG_HELP: &str = "Only log messages with the given severity or above. One of: [debug, info, warn, error]";
/// The help of `--log.format` (Go `promslogflag.FormatFlagHelp`).
pub const FORMAT_FLAG_HELP: &str = "Output format of log messages. One of: [logfmt, json]";

/// A slog level (Go `slog.Level`: debug -4, info 0, warn 4, error 8).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SlogLevel {
    Debug = -4,
    Info = 0,
    Warn = 4,
    Error = 8,
}

impl SlogLevel {
    /// Go `slog.Level.String()`.
    pub fn as_str(self) -> &'static str {
        match self {
            SlogLevel::Debug => "DEBUG",
            SlogLevel::Info => "INFO",
            SlogLevel::Warn => "WARN",
            SlogLevel::Error => "ERROR",
        }
    }
    fn from_i8(v: i8) -> SlogLevel {
        match v {
            -4 => SlogLevel::Debug,
            4 => SlogLevel::Warn,
            8 => SlogLevel::Error,
            _ => SlogLevel::Info,
        }
    }
}

/// Go `promslog.Level`: a level that is safe to change while loggers use it. The default is info.
#[derive(Debug)]
pub struct Level(AtomicI8);

impl Default for Level {
    fn default() -> Self {
        Level(AtomicI8::new(SlogLevel::Info as i8))
    }
}

impl Level {
    /// Go `NewLevel`.
    pub fn new() -> Arc<Level> {
        Arc::new(Level::default())
    }
    /// Go `Level.Level()`.
    pub fn level(&self) -> SlogLevel {
        SlogLevel::from_i8(self.0.load(Ordering::Relaxed))
    }
    /// Go `Level.Set`: "debug", "info", "warn" or "error" (any case).
    pub fn set(&self, s: &str) -> Result<(), String> {
        let l = match s.to_lowercase().as_str() {
            "debug" => SlogLevel::Debug,
            "info" => SlogLevel::Info,
            "warn" => SlogLevel::Warn,
            "error" => SlogLevel::Error,
            _ => return Err(format!("unrecognized log level {s}")),
        };
        self.0.store(l as i8, Ordering::Relaxed);
        Ok(())
    }
    /// Go `slog.LevelVar.Set` with a level value.
    pub fn set_level(&self, l: SlogLevel) {
        self.0.store(l as i8, Ordering::Relaxed);
    }
    /// Whether a record at `l` is written.
    pub fn enabled(&self, l: SlogLevel) -> bool {
        l >= self.level()
    }
}

impl std::fmt::Display for Level {
    /// Go `Level.String()`: "debug", "info", "warn" or "error".
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.level() {
            SlogLevel::Debug => "debug",
            SlogLevel::Info => "info",
            SlogLevel::Warn => "warn",
            SlogLevel::Error => "error",
        })
    }
}

/// Go `promslog.Format`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Format {
    #[default]
    Logfmt,
    Json,
}

impl Format {
    /// Go `Format.Set`.
    pub fn parse(s: &str) -> Result<Format, String> {
        match s {
            "logfmt" => Ok(Format::Logfmt),
            "json" => Ok(Format::Json),
            _ => Err(format!("unrecognized log format {s}")),
        }
    }
}

/// Where records go.
pub type Writer = Arc<dyn Fn(&[u8]) + Send + Sync>;

/// Go `promslog.New(&Config{...})`: the slog handler with the level, the format and the writer (stderr when
/// `None`).
#[derive(Clone)]
pub struct Handler {
    pub level: Arc<Level>,
    pub format: Format,
    writer: Option<Writer>,
}

impl Handler {
    pub fn new(level: Arc<Level>, format: Format, writer: Option<Writer>) -> Handler {
        Handler { level, format, writer }
    }

    /// Writes one record when its level is enabled. `attrs` are the logger's attributes followed by the call's.
    pub fn log(&self, level: SlogLevel, msg: &str, attrs: &[(String, String)]) {
        if !self.level.enabled(level) {
            return;
        }
        let line = self.format_record(now_millis(), level, msg, attrs);
        match &self.writer {
            Some(w) => w(line.as_bytes()),
            None => {
                let mut e = std::io::stderr().lock();
                let _ = e.write_all(line.as_bytes());
            }
        }
    }

    /// The text of one record, with the newline.
    pub fn format_record(&self, ms: i64, level: SlogLevel, msg: &str, attrs: &[(String, String)]) -> String {
        let mut s = String::with_capacity(96 + msg.len());
        let time = format_time(ms);
        match self.format {
            Format::Logfmt => {
                s.push_str("time=");
                s.push_str(&time);
                s.push_str(" level=");
                s.push_str(level.as_str());
                s.push_str(" msg=");
                push_text_value(&mut s, msg);
                for (k, v) in attrs {
                    s.push(' ');
                    push_text_value(&mut s, &reserved_key(k));
                    s.push('=');
                    push_text_value(&mut s, v);
                }
            }
            Format::Json => {
                s.push_str("{\"time\":");
                json::write_str(&time, &mut s);
                s.push_str(",\"level\":");
                json::write_str(level.as_str(), &mut s);
                s.push_str(",\"msg\":");
                json::write_str(msg, &mut s);
                for (k, v) in attrs {
                    s.push(',');
                    json::write_str(&reserved_key(k), &mut s);
                    s.push(':');
                    json::write_str(v, &mut s);
                }
                s.push('}');
            }
        }
        s.push('\n');
        s
    }
}

/// promslog renames attributes that clash with the built-in keys.
fn reserved_key(k: &str) -> String {
    match k {
        "time" | "level" | "source" => format!("logged_{k}"),
        _ => k.to_string(),
    }
}

/// slog `needsQuoting`.
fn needs_quoting(s: &str) -> bool {
    if s.is_empty() {
        return true;
    }
    for c in s.chars() {
        if c == ' ' || c == '=' || c == '"' || c == '\u{FFFD}' || !is_go_print(c) {
            return true;
        }
    }
    false
}

/// Go `unicode.IsPrint` for the characters that matter in log lines.
fn is_go_print(c: char) -> bool {
    !(c.is_control() || c == '\u{2028}' || c == '\u{2029}' || (c.is_whitespace() && c != ' '))
}

/// slog text handler value: quoted with Go `strconv.Quote` when needed.
fn push_text_value(s: &mut String, v: &str) {
    if needs_quoting(v) {
        s.push_str(&go_quote(v));
    } else {
        s.push_str(v);
    }
}

/// Go `strconv.Quote`.
pub fn go_quote(v: &str) -> String {
    let mut out = String::with_capacity(v.len() + 2);
    out.push('"');
    for c in v.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\u{b}' => out.push_str("\\v"),
            c if is_go_print(c) => out.push(c),
            c if (c as u32) < 0x80 => out.push_str(&format!("\\x{:02x}", c as u32)),
            c if (c as u32) < 0x10000 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push_str(&format!("\\U{:08x}", c as u32)),
        }
    }
    out.push('"');
    out
}

fn now_millis() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// `2006-01-02T15:04:05.000Z` in UTC.
pub fn format_time(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let milli = ms.rem_euclid(1000);
    let days = secs.div_euclid(86400);
    let sod = secs.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{milli:03}Z", sod / 3600, (sod / 60) % 60, sod % 60)
}

/// Days since 1970-01-01 to (year, month, day) (H. Hinnant's algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Formats a duration like Go `time.Duration.String()` ("1h2m3.5s", "4.552015ms", "0s").
pub fn go_duration(d: std::time::Duration) -> String {
    let ns = d.as_nanos() as u128;
    if ns == 0 {
        return "0s".to_string();
    }
    if ns < 1_000 {
        return format!("{ns}ns");
    }
    let frac = |v: u128, unit: u128, digits: usize| -> String {
        let int = v / unit;
        let rem = v % unit;
        if rem == 0 {
            return int.to_string();
        }
        let s = format!("{rem:0digits$}");
        format!("{int}.{}", s.trim_end_matches('0'))
    };
    if ns < 1_000_000 {
        return format!("{}µs", frac(ns, 1_000, 3));
    }
    if ns < 1_000_000_000 {
        return format!("{}ms", frac(ns, 1_000_000, 6));
    }
    let total_s = ns / 1_000_000_000;
    let sub = ns % 1_000_000_000;
    let h = total_s / 3600;
    let m = (total_s / 60) % 60;
    let s = total_s % 60;
    let mut out = String::new();
    if h > 0 {
        out.push_str(&format!("{h}h"));
    }
    if h > 0 || m > 0 {
        out.push_str(&format!("{m}m"));
    }
    out.push_str(&frac(s as u128 * 1_000_000_000 + sub, 1_000_000_000, 9));
    out.push('s');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records() {
        let h = Handler::new(Level::new(), Format::Logfmt, None);
        let r = h.format_record(0, SlogLevel::Info, "Starting Prometheus Server", &[("mode".into(), "server".into()), ("version".into(), "(version=3.16.0)".into())]);
        assert_eq!(r, "time=1970-01-01T00:00:00.000Z level=INFO msg=\"Starting Prometheus Server\" mode=server version=(version=3.16.0)\n");
        let r = h.format_record(1500, SlogLevel::Warn, "x", &[("e".into(), String::new()), ("time".into(), "a b".into())]);
        assert_eq!(r, "time=1970-01-01T00:00:01.500Z level=WARN msg=x e=\"\" logged_time=\"a b\"\n");
        let h = Handler::new(Level::new(), Format::Json, None);
        let r = h.format_record(0, SlogLevel::Error, "m", &[("k".into(), "v".into())]);
        assert_eq!(r, "{\"time\":\"1970-01-01T00:00:00.000Z\",\"level\":\"ERROR\",\"msg\":\"m\",\"k\":\"v\"}\n");
    }

    #[test]
    fn levels_and_durations() {
        let l = Level::new();
        assert_eq!(l.to_string(), "info");
        assert!(l.set("DEBUG").is_ok());
        assert_eq!(l.to_string(), "debug");
        assert_eq!(l.set("foo").unwrap_err(), "unrecognized log level foo");
        assert_eq!(go_duration(std::time::Duration::from_nanos(4_552_015)), "4.552015ms");
        assert_eq!(go_duration(std::time::Duration::from_nanos(2_116)), "2.116µs");
        assert_eq!(go_duration(std::time::Duration::from_secs(3725)), "1h2m5s");
        assert_eq!(go_duration(std::time::Duration::from_millis(1500)), "1.5s");
        assert_eq!(format_time(1_763_571_784_537), "2025-11-19T17:03:04.537Z");
    }
}
