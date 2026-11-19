//! The promtool logger: Go `promslog.New(&promslog.Config{})` (logfmt to stderr, level info).
//!
//! Differs from Go: no `source=file.go:line` attribute (there is no Go source location to name).

use std::io::Write;
use std::sync::Arc;

use discovery::log::{Level, Logger};

fn needs_quote(s: &str) -> bool {
    s.is_empty() || s.chars().any(|c| c == ' ' || c == '=' || c == '"' || c.is_control() || !c.is_ascii() && !c.is_alphanumeric())
}

fn fmt_value(s: &str) -> String {
    if needs_quote(s) { promql::parser::quote::go_quote(s) } else { s.to_string() }
}

/// The current time as promslog prints it: `2006-01-02T15:04:05.000Z07:00` in local time (UTC here).
fn now_string() -> String {
    let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    let ms = d.as_millis() as i64;
    let secs = ms.div_euclid(1000);
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    // Civil from days (Howard Hinnant).
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        ms.rem_euclid(1000)
    )
}

/// A logger that writes logfmt lines at level info and above to stderr.
pub fn stderr_logger() -> Logger {
    Logger::new(Arc::new(|level: Level, msg: &str, kv: &[(String, String)]| {
        if level < Level::Info {
            return;
        }
        let mut line = format!("time={} level={} msg={}", now_string(), level.as_str(), fmt_value(msg));
        for (k, v) in kv {
            line.push(' ');
            line.push_str(k);
            line.push('=');
            line.push_str(&fmt_value(v));
        }
        line.push('\n');
        let _ = std::io::stderr().write_all(line.as_bytes());
    }))
}
