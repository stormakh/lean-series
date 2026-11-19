//! Port of `util/logging/file.go`: a JSON logger that appends to a file (the query log and the scrape failure
//! log use it).
use super::{Config, Format, Logger};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::{Arc, Mutex};

/// Writes to the file until it is closed; writes after `close` fail like Go's writes to a closed file.
struct FileWriter(Arc<Mutex<Option<File>>>);

impl Write for FileWriter {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        let mut g = match self.0.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        match g.as_mut() {
            Some(f) => f.write(b),
            None => Err(std::io::Error::other("file already closed")),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Go `JSONFileLogger`: a JSON-format logger writing to a file opened with `O_APPEND|O_CREATE|O_WRONLY`.
pub struct JsonFileLogger {
    logger: Logger,
    path: String,
    file: Arc<Mutex<Option<File>>>,
}

/// The OS part of an I/O error as Go prints it ("no such file or directory").
pub(crate) fn go_io_error(e: &std::io::Error) -> String {
    let s = e.to_string();
    match s.find(" (os error ") {
        Some(i) => {
            let mut t = s[..i].to_string();
            if let Some(c) = t.get_mut(0..1) {
                c.make_ascii_lowercase();
            }
            t
        }
        None => s,
    }
}

impl JsonFileLogger {
    /// Go `NewJSONFileLogger`: `Ok(None)` for an empty path.
    pub fn new(path: &str) -> Result<Option<JsonFileLogger>, String> {
        if path.is_empty() {
            return Ok(None);
        }
        let f = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .map_err(|e| format!("can't create json log file: open {path}: {}", go_io_error(&e)))?;
        let file = Arc::new(Mutex::new(Some(f)));
        let logger = Logger::new(Config { format: Format::Json, writer: Some(Box::new(FileWriter(file.clone()))), ..Config::default() });
        Ok(Some(JsonFileLogger { logger, path: path.to_string(), file }))
    }

    /// The logger that writes to the file.
    pub fn logger(&self) -> &Logger {
        &self.logger
    }

    /// Closes the file. A second close fails with "close <path>: file already closed", as Go's `os.File.Close`.
    pub fn close(&self) -> Result<(), String> {
        let mut g = match self.file.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        match g.take() {
            Some(f) => {
                drop(f);
                Ok(())
            }
            None => Err(format!("close {}: file already closed", self.path)),
        }
    }
}
