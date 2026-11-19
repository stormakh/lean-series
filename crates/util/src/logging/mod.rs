//! Port of `util/logging` and of the logger setup of `github.com/prometheus/common/promslog`: the one logger every
//! crate uses. It writes the same lines as Prometheus 3.16.0 (Go `log/slog` with promslog's options):
//!
//! ```text
//! time=2026-11-19T12:42:09.921-03:00 level=INFO source=main.go:939 msg="Starting Prometheus Server" mode=server
//! {"time":"2026-11-19T12:42:13.057701761-03:00","level":"INFO","source":"main.go:1793","msg":"updated GOGC","old":100}
//! ```
//!
//! Time is local (Go `time.Local`), with milliseconds in logfmt and up to nanoseconds in JSON. `source` is the
//! base name of the calling file and its line (`#[track_caller]`). Attribute keys `time`, `source` and `level`
//! become `logged_time`, `logged_source` and `logged_level`. Durations print as Go duration strings (`1m30s`).
//!
//! Cost: a disabled level is one atomic load and returns; no allocation. An enabled line is formatted into one
//! buffer and written with one `write_all` under a mutex.
//!
//! ```
//! use util::logging::{Config, Logger};
//! let logger = Logger::new(Config::default()); // info, logfmt, stderr
//! let l = logger.with(&[("component", &"tsdb")]);
//! l.warn("Deleting all segments newer than corrupted segment", &[("segment", &3)]);
//! ```
mod dedupe;
mod file;
pub mod tz;

pub use dedupe::dedupe;
pub use file::JsonFileLogger;

use std::fmt;
use std::io::Write;
use std::sync::atomic::{AtomicI8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// Log level with slog's numeric values (debug -4, info 0, warn 4, error 8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Level {
    Debug = -4,
    #[default]
    Info = 0,
    Warn = 4,
    Error = 8,
}

/// promslog `LevelFlagOptions`.
pub const LEVEL_FLAG_OPTIONS: [&str; 4] = ["debug", "info", "warn", "error"];
/// promslog `FormatFlagOptions`.
pub const FORMAT_FLAG_OPTIONS: [&str; 2] = ["logfmt", "json"];

impl Level {
    /// promslog `Level.Set`: case-insensitive `debug`, `info`, `warn`, `error`.
    pub fn parse(s: &str) -> Result<Level, String> {
        match s.to_ascii_lowercase().as_str() {
            "debug" => Ok(Level::Debug),
            "info" => Ok(Level::Info),
            "warn" => Ok(Level::Warn),
            "error" => Ok(Level::Error),
            _ => Err(format!("unrecognized log level {s}")),
        }
    }
    /// promslog `Level.String`: the lowercase name.
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Debug => "debug",
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
        }
    }
    /// slog's name in log lines (`INFO`).
    pub fn upper(self) -> &'static str {
        match self {
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
        }
    }
    fn from_i8(v: i8) -> Level {
        match v {
            i8::MIN..=-1 => Level::Debug,
            0..=3 => Level::Info,
            4..=7 => Level::Warn,
            _ => Level::Error,
        }
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Output format (promslog `Format`): `logfmt` (slog text handler) or `json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Format {
    #[default]
    Logfmt,
    Json,
}

impl Format {
    /// promslog `Format.Set`.
    pub fn parse(s: &str) -> Result<Format, String> {
        match s {
            "logfmt" => Ok(Format::Logfmt),
            "json" => Ok(Format::Json),
            _ => Err(format!("unrecognized log format {s}")),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Format::Logfmt => "logfmt",
            Format::Json => "json",
        }
    }
}

/// promslog `Config`. The writer defaults to stderr.
#[derive(Default)]
pub struct Config {
    pub level: Level,
    pub format: Format,
    pub writer: Option<Box<dyn Write + Send>>,
}

/// One attribute value. Build it through [`LogValue`].
#[derive(Clone, Copy)]
pub enum Value<'a> {
    Str(&'a str),
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
    /// Written as a Go duration string (`1m30s`), as promslog does.
    Duration(Duration),
    /// Errors and other values written with their `Display` text.
    Display(&'a dyn fmt::Display),
}

/// A value that can be logged. Implemented for strings, numbers, `bool`, `Duration`, `io::Error`, references to
/// these, and [`Disp`] (any `Display`, for example an error type).
pub trait LogValue {
    fn log_value(&self) -> Value<'_>;
}

/// One attribute: key and value.
pub type Attr<'a> = (&'a str, &'a dyn LogValue);

impl LogValue for str {
    fn log_value(&self) -> Value<'_> {
        Value::Str(self)
    }
}
impl LogValue for String {
    fn log_value(&self) -> Value<'_> {
        Value::Str(self)
    }
}
impl LogValue for Arc<str> {
    fn log_value(&self) -> Value<'_> {
        Value::Str(self)
    }
}
impl LogValue for Box<str> {
    fn log_value(&self) -> Value<'_> {
        Value::Str(self)
    }
}
impl<T: LogValue + ?Sized> LogValue for &T {
    fn log_value(&self) -> Value<'_> {
        (**self).log_value()
    }
}
macro_rules! int_value {
    ($v:ident, $($t:ty),*) => {$(
        impl LogValue for $t {
            fn log_value(&self) -> Value<'_> {
                Value::$v(*self as _)
            }
        }
    )*};
}
int_value!(I64, i8, i16, i32, i64, isize);
int_value!(U64, u8, u16, u32, u64, usize);
int_value!(F64, f32, f64);
impl LogValue for bool {
    fn log_value(&self) -> Value<'_> {
        Value::Bool(*self)
    }
}
impl LogValue for Duration {
    fn log_value(&self) -> Value<'_> {
        Value::Duration(*self)
    }
}
impl LogValue for std::io::Error {
    fn log_value(&self) -> Value<'_> {
        Value::Display(self)
    }
}
impl LogValue for dyn std::error::Error {
    fn log_value(&self) -> Value<'_> {
        Value::Display(self)
    }
}
impl LogValue for dyn std::error::Error + Send + Sync {
    fn log_value(&self) -> Value<'_> {
        Value::Display(self)
    }
}

/// Logs any `Display` value as its text: `("err", &Disp(&e))`.
pub struct Disp<'a>(pub &'a dyn fmt::Display);
impl LogValue for Disp<'_> {
    fn log_value(&self) -> Value<'_> {
        Value::Display(self.0)
    }
}

struct Core {
    level: AtomicI8,
    json: bool,
    out: Mutex<Box<dyn Write + Send>>,
}

/// A cheap, cloneable logger handle (slog `*slog.Logger` with a promslog handler). The default value discards
/// everything (promslog `NewNopLogger`).
#[derive(Clone, Default)]
pub struct Logger {
    core: Option<Arc<Core>>,
    /// Attributes from `with`, already rendered in the core's format, each with its leading separator.
    attrs: Option<Arc<str>>,
    dedupe: Option<Arc<dedupe::State>>,
}

impl fmt::Debug for Logger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Logger").field("level", &self.level()).finish()
    }
}

impl Logger {
    /// promslog `New`.
    pub fn new(cfg: Config) -> Logger {
        let out = cfg.writer.unwrap_or_else(|| Box::new(std::io::stderr()));
        Logger {
            core: Some(Arc::new(Core { level: AtomicI8::new(cfg.level as i8), json: cfg.format == Format::Json, out: Mutex::new(out) })),
            attrs: None,
            dedupe: None,
        }
    }

    /// promslog `NewNopLogger`: discards every line.
    pub fn nop() -> Logger {
        Logger::default()
    }

    /// The current minimum level (shared by all handles made from the same `new`).
    pub fn level(&self) -> Option<Level> {
        self.core.as_ref().map(|c| Level::from_i8(c.level.load(Ordering::Relaxed)))
    }

    /// Changes the minimum level for this logger and every handle made from it.
    pub fn set_level(&self, l: Level) {
        if let Some(c) = &self.core {
            c.level.store(l as i8, Ordering::Relaxed);
        }
    }

    /// True when a line at `l` would be written. Use it to skip expensive attribute values.
    #[inline]
    pub fn enabled(&self, l: Level) -> bool {
        match &self.core {
            Some(c) => l as i8 >= c.level.load(Ordering::Relaxed),
            None => false,
        }
    }

    /// slog `With`: a handle that adds `attrs` to every line, after the message and before the call's attributes.
    pub fn with(&self, attrs: &[Attr<'_>]) -> Logger {
        let Some(core) = &self.core else { return self.clone() };
        if attrs.is_empty() {
            return self.clone();
        }
        let mut s = String::with_capacity(64);
        if let Some(a) = &self.attrs {
            s.push_str(a);
        }
        for (k, v) in attrs {
            append_attr(&mut s, core.json, k, v.log_value());
        }
        Logger { core: self.core.clone(), attrs: Some(Arc::from(s)), dedupe: self.dedupe.clone() }
    }

    #[track_caller]
    #[inline]
    pub fn debug(&self, msg: &str, attrs: &[Attr<'_>]) {
        if self.enabled(Level::Debug) {
            self.write(Level::Debug, msg, attrs, std::panic::Location::caller());
        }
    }
    #[track_caller]
    #[inline]
    pub fn info(&self, msg: &str, attrs: &[Attr<'_>]) {
        if self.enabled(Level::Info) {
            self.write(Level::Info, msg, attrs, std::panic::Location::caller());
        }
    }
    #[track_caller]
    #[inline]
    pub fn warn(&self, msg: &str, attrs: &[Attr<'_>]) {
        if self.enabled(Level::Warn) {
            self.write(Level::Warn, msg, attrs, std::panic::Location::caller());
        }
    }
    #[track_caller]
    #[inline]
    pub fn error(&self, msg: &str, attrs: &[Attr<'_>]) {
        if self.enabled(Level::Error) {
            self.write(Level::Error, msg, attrs, std::panic::Location::caller());
        }
    }
    #[track_caller]
    #[inline]
    pub fn log(&self, level: Level, msg: &str, attrs: &[Attr<'_>]) {
        if self.enabled(level) {
            self.write(level, msg, attrs, std::panic::Location::caller());
        }
    }

    #[cold]
    fn write(&self, level: Level, msg: &str, attrs: &[Attr<'_>], loc: &std::panic::Location<'_>) {
        let Some(core) = &self.core else { return };
        if let Some(d) = &self.dedupe {
            if !d.admit(msg) {
                return;
            }
        }
        let line = format_line(core.json, SystemTime::now(), level, loc.file(), loc.line(), msg, self.attrs.as_deref(), attrs);
        let mut out = match core.out.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let _ = out.write_all(line.as_bytes());
    }
}

/// Formats one complete line (with the trailing newline). Public for tests and for writers that need the exact
/// text of a line.
#[allow(clippy::too_many_arguments)]
pub fn format_line(json: bool, t: SystemTime, level: Level, file: &str, line: u32, msg: &str, pre: Option<&str>, attrs: &[Attr<'_>]) -> String {
    let mut s = String::with_capacity(160);
    let base = file.rsplit(['/', '\\']).next().unwrap_or(file);
    if json {
        s.push_str("{\"time\":\"");
        tz::append_rfc3339(&mut s, t, tz::local(), tz::Frac::Nanos);
        s.push_str("\",\"level\":\"");
        s.push_str(level.upper());
        s.push_str("\",\"source\":");
        let mut src = String::with_capacity(base.len() + 8);
        src.push_str(base);
        src.push(':');
        push_u64(&mut src, line as u64);
        append_json_str(&mut s, &src);
        s.push_str(",\"msg\":");
        append_json_str(&mut s, msg);
    } else {
        s.push_str("time=");
        tz::append_rfc3339(&mut s, t, tz::local(), tz::Frac::Millis);
        s.push_str(" level=");
        s.push_str(level.upper());
        s.push_str(" source=");
        let mut src = String::with_capacity(base.len() + 8);
        src.push_str(base);
        src.push(':');
        push_u64(&mut src, line as u64);
        append_text_str(&mut s, &src);
        s.push_str(" msg=");
        append_text_str(&mut s, msg);
    }
    if let Some(p) = pre {
        s.push_str(p);
    }
    for (k, v) in attrs {
        append_attr(&mut s, json, k, v.log_value());
    }
    if json {
        s.push('}');
    }
    s.push('\n');
    s
}

fn push_u64(s: &mut String, v: u64) {
    let mut b = [0u8; 20];
    let mut i = b.len();
    let mut v = v;
    loop {
        i -= 1;
        b[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    s.push_str(std::str::from_utf8(&b[i..]).unwrap_or(""));
}

/// Appends `<sep>key=value` (logfmt) or `,"key":value` (JSON), with promslog's reserved-key renames.
fn append_attr(s: &mut String, json: bool, key: &str, v: Value<'_>) {
    let renamed;
    let key = match key {
        "time" | "source" | "level" => {
            renamed = format!("logged_{key}");
            renamed.as_str()
        }
        k => k,
    };
    if json {
        s.push(',');
        append_json_str(s, key);
        s.push(':');
        match v {
            Value::Str(x) => append_json_str(s, x),
            Value::I64(x) => {
                if x < 0 {
                    s.push('-');
                }
                push_u64(s, x.unsigned_abs());
            }
            Value::U64(x) => push_u64(s, x),
            Value::F64(x) => {
                if x.is_finite() {
                    json::write_f64(x, s);
                } else {
                    let t = if x.is_nan() {
                        "NaN"
                    } else if x > 0.0 {
                        "+Inf"
                    } else {
                        "-Inf"
                    };
                    append_json_str(s, &format!("!ERROR:json: unsupported value: {t}"));
                }
            }
            Value::Bool(x) => s.push_str(if x { "true" } else { "false" }),
            Value::Duration(d) => append_json_str(s, &go_duration(d)),
            Value::Display(d) => append_json_str(s, &d.to_string()),
        }
    } else {
        s.push(' ');
        append_text_str(s, key);
        s.push('=');
        match v {
            Value::Str(x) => append_text_str(s, x),
            Value::I64(x) => {
                if x < 0 {
                    s.push('-');
                }
                push_u64(s, x.unsigned_abs());
            }
            Value::U64(x) => push_u64(s, x),
            Value::F64(x) => model::labels::append_float_g(s, x),
            Value::Bool(x) => s.push_str(if x { "true" } else { "false" }),
            Value::Duration(d) => append_text_str(s, &go_duration(d)),
            Value::Display(d) => append_text_str(s, &d.to_string()),
        }
    }
}

/// slog's JSON string escaping (it does not escape `<`, `>`, `&`).
pub fn append_json_str(s: &mut String, v: &str) {
    s.push('"');
    let mut start = 0;
    for (i, c) in v.char_indices() {
        let esc: &str = match c {
            '"' => "\\\"",
            '\\' => "\\\\",
            '\n' => "\\n",
            '\r' => "\\r",
            '\t' => "\\t",
            '\u{2028}' => "\\u2028",
            '\u{2029}' => "\\u2029",
            c if (c as u32) < 0x20 => "",
            _ => continue,
        };
        s.push_str(&v[start..i]);
        if esc.is_empty() {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            s.push_str("\\u00");
            s.push(HEX[(c as usize) >> 4] as char);
            s.push(HEX[(c as usize) & 15] as char);
        } else {
            s.push_str(esc);
        }
        start = i + c.len_utf8();
    }
    s.push_str(&v[start..]);
    s.push('"');
}

/// slog text handler `needsQuoting`.
fn needs_quoting(v: &str) -> bool {
    if v.is_empty() {
        return true;
    }
    for c in v.chars() {
        if c.is_ascii() {
            if c != '\\' && (c == ' ' || c == '=' || c == '"' || (c as u32) < 0x20) {
                return true;
            }
        } else if c.is_whitespace() || !is_print(c) {
            return true;
        }
    }
    false
}

/// Close to Go's `unicode.IsPrint` for non-ASCII: not a control or format character and not a non-ASCII space.
fn is_print(c: char) -> bool {
    !(c.is_control()
        || c.is_whitespace()
        || matches!(c as u32, 0xAD | 0x600..=0x605 | 0x61C | 0x6DD | 0x70F | 0x200B..=0x200F | 0x202A..=0x202E | 0x2060..=0x2064 | 0x2066..=0x206F | 0xFEFF | 0xFFF9..=0xFFFB | 0xE000..=0xF8FF | 0xF0000..=0x10FFFF))
}

/// slog text value: as is, or Go `strconv.Quote` when it needs quoting.
pub fn append_text_str(s: &mut String, v: &str) {
    if !needs_quoting(v) {
        s.push_str(v);
        return;
    }
    go_quote(s, v);
}

/// Go `strconv.Quote` (printable non-ASCII is kept).
pub fn go_quote(s: &mut String, v: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    s.push('"');
    for c in v.chars() {
        match c {
            '"' => s.push_str("\\\""),
            '\\' => s.push_str("\\\\"),
            '\x07' => s.push_str("\\a"),
            '\x08' => s.push_str("\\b"),
            '\x0c' => s.push_str("\\f"),
            '\n' => s.push_str("\\n"),
            '\r' => s.push_str("\\r"),
            '\t' => s.push_str("\\t"),
            '\x0b' => s.push_str("\\v"),
            c if c == ' ' || (c.is_ascii() && !c.is_ascii_control()) => s.push(c),
            c if (c as u32) < 0x80 => {
                s.push_str("\\x");
                s.push(HEX[(c as usize) >> 4] as char);
                s.push(HEX[(c as usize) & 15] as char);
            }
            c if is_print(c) => s.push(c),
            c => {
                let n = c as u32;
                if n < 0x10000 {
                    s.push_str("\\u");
                    for sh in [12, 8, 4, 0] {
                        s.push(HEX[((n >> sh) & 15) as usize] as char);
                    }
                } else {
                    s.push_str("\\U");
                    for sh in [28, 24, 20, 16, 12, 8, 4, 0] {
                        s.push(HEX[((n >> sh) & 15) as usize] as char);
                    }
                }
            }
        }
    }
    s.push('"');
}

/// Go `time.Duration.String` (`1h2m3.5s`, `1.5ms`, `0s`). Durations above `i64::MAX` nanoseconds saturate.
pub fn go_duration(d: Duration) -> String {
    let u = d.as_nanos().min(i64::MAX as u128) as u64;
    let mut s = String::with_capacity(16);
    if u < 1_000_000_000 {
        if u == 0 {
            return "0s".to_string();
        }
        let (unit, div, prec) = if u < 1_000 {
            ("ns", 1u64, 0)
        } else if u < 1_000_000 {
            ("µs", 1_000, 3)
        } else {
            ("ms", 1_000_000, 6)
        };
        frac(&mut s, u / div, u % div, prec);
        s.push_str(unit);
        return s;
    }
    let secs = u / 1_000_000_000;
    let ns = u % 1_000_000_000;
    let h = secs / 3600;
    let m = secs / 60 % 60;
    if h > 0 {
        push_u64(&mut s, h);
        s.push('h');
    }
    if h > 0 || m > 0 {
        push_u64(&mut s, m);
        s.push('m');
    }
    frac(&mut s, secs % 60, ns, 9);
    s.push('s');
    s
}

/// `int.frac` with trailing zeros of the fraction removed (`prec` digits at most).
fn frac(s: &mut String, int: u64, mut f: u64, prec: u32) {
    push_u64(s, int);
    if f == 0 || prec == 0 {
        return;
    }
    let mut w = prec;
    while f % 10 == 0 {
        f /= 10;
        w -= 1;
    }
    s.push('.');
    let mut digits = [b'0'; 9];
    for i in (0..w as usize).rev() {
        digits[i] = b'0' + (f % 10) as u8;
        f /= 10;
    }
    s.push_str(std::str::from_utf8(&digits[..w as usize]).unwrap_or(""));
}

/// A `Write` target that keeps everything in memory, for tests and for capturing log output.
#[derive(Clone, Default)]
pub struct Buffer(Arc<Mutex<Vec<u8>>>);

impl Buffer {
    pub fn new() -> Buffer {
        Buffer::default()
    }
    /// Everything written so far.
    pub fn contents(&self) -> String {
        match self.0.lock() {
            Ok(g) => String::from_utf8_lossy(&g).into_owned(),
            Err(p) => String::from_utf8_lossy(&p.into_inner()).into_owned(),
        }
    }
}

impl Write for Buffer {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        match self.0.lock() {
            Ok(mut g) => g.extend_from_slice(b),
            Err(p) => p.into_inner().extend_from_slice(b),
        }
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
