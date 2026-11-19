//! A minimal structured logger (the part of `log/slog` discovery uses). The default logger drops everything;
//! the server installs a sink.
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Debug,
    Info,
    Warn,
    Error,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
        }
    }
}

/// Receives a record: level, message and key/value attributes (logger context first).
pub type Sink = Arc<dyn Fn(Level, &str, &[(String, String)]) + Send + Sync>;

#[derive(Clone, Default)]
pub struct Logger {
    sink: Option<Sink>,
    attrs: Vec<(String, String)>,
}

impl std::fmt::Debug for Logger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Logger").field("attrs", &self.attrs).finish()
    }
}

impl Logger {
    /// Go `promslog.NewNopLogger()`.
    pub fn nop() -> Logger {
        Logger::default()
    }
    pub fn new(sink: Sink) -> Logger {
        Logger { sink: Some(sink), attrs: Vec::new() }
    }
    /// Go `logger.With(k, v, ...)`.
    pub fn with(&self, kv: &[(&str, &str)]) -> Logger {
        let mut l = self.clone();
        l.attrs.extend(kv.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        l
    }
    pub fn log(&self, level: Level, msg: &str, kv: &[(&str, &str)]) {
        if let Some(s) = &self.sink {
            let mut a = self.attrs.clone();
            a.extend(kv.iter().map(|(k, v)| (k.to_string(), v.to_string())));
            s(level, msg, &a);
        }
    }
    pub fn debug(&self, msg: &str, kv: &[(&str, &str)]) {
        self.log(Level::Debug, msg, kv)
    }
    pub fn info(&self, msg: &str, kv: &[(&str, &str)]) {
        self.log(Level::Info, msg, kv)
    }
    pub fn warn(&self, msg: &str, kv: &[(&str, &str)]) {
        self.log(Level::Warn, msg, kv)
    }
    pub fn error(&self, msg: &str, kv: &[(&str, &str)]) {
        self.log(Level::Error, msg, kv)
    }
}
