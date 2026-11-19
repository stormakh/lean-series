//! The scrape failure log (Go `FailureLogger` and `util/logging.JSONFileLogger`): one JSON line per failed scrape,
//! in the format of promslog's JSON handler.

use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};

/// A logger for failed scrapes (Go `FailureLogger`: a `slog.Handler` that can be closed).
pub trait FailureLogger: Send + Sync {
    /// Logs an error record with the message.
    fn log_error(&self, msg: &str);
    /// A logger that adds the attribute to every record (Go `slog.New(l).With(k, v).Handler()`).
    fn with(&self, key: &str, value: &str) -> Arc<dyn FailureLogger>;
    /// Closes the underlying file.
    fn close(&self) -> Result<(), String>;
}

/// Go `logging.JSONFileLogger`.
pub struct JsonFileLogger {
    file: Arc<Mutex<Option<File>>>,
    attrs: Vec<(String, String)>,
}

impl JsonFileLogger {
    /// Go `NewJSONFileLogger`: None for an empty path.
    pub fn new(path: &str) -> Result<Option<Arc<dyn FailureLogger>>, String> {
        if path.is_empty() {
            return Ok(None);
        }
        let f = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .map_err(|e| format!("can't create json log file: open {}: {}", path, net::types::go_io_error(&e)))?;
        Ok(Some(Arc::new(JsonFileLogger { file: Arc::new(Mutex::new(Some(f))), attrs: Vec::new() })))
    }
}

fn json_str(out: &mut String, s: &str) {
    out.push_str(&json::Json::Str(s.to_string()).to_string());
}

/// The time as promslog writes it: RFC 3339 with milliseconds in local time ("2006-01-02T15:04:05.000Z07:00").
fn now_rfc3339_millis() -> String {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs() as i64;
    let ms = now.subsec_millis();
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{ms:03}Z", rem / 3600, rem % 3600 / 60, rem % 60)
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

impl FailureLogger for JsonFileLogger {
    fn log_error(&self, msg: &str) {
        let mut line = String::from("{\"time\":");
        json_str(&mut line, &now_rfc3339_millis());
        line.push_str(",\"level\":\"ERROR\",\"source\":\"scrape.go:0\",\"msg\":");
        json_str(&mut line, msg);
        for (k, v) in &self.attrs {
            line.push(',');
            json_str(&mut line, k);
            line.push(':');
            json_str(&mut line, v);
        }
        line.push_str("}\n");
        if let Some(f) = self.file.lock().unwrap().as_mut() {
            let _ = f.write_all(line.as_bytes());
        }
    }

    fn with(&self, key: &str, value: &str) -> Arc<dyn FailureLogger> {
        let mut attrs = self.attrs.clone();
        attrs.push((key.to_string(), value.to_string()));
        Arc::new(JsonFileLogger { file: self.file.clone(), attrs })
    }

    fn close(&self) -> Result<(), String> {
        self.file.lock().unwrap().take();
        Ok(())
    }
}
