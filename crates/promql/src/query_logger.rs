//! Port of `promql/query_logger.go`: the active query tracker, which writes running queries to
//! `queries.active` so that queries that crashed Prometheus are logged at the next start.
//!
//! Go maps the file into memory; here the tracker keeps the bytes in memory and writes every change through to
//! the file at the same offsets (`unsafe` is not allowed outside the TSDB). The file content is the same.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use storage::{Context, Error};

use crate::engine::QueryTracker;

/// The size of one entry in the file.
pub const ENTRY_SIZE: usize = 1000;

/// A writer that can sync (Go `syncWriter`).
pub trait SyncWriter: Write {
    fn sync(&mut self) -> io::Result<()>;
}

impl SyncWriter for File {
    fn sync(&mut self) -> io::Result<()> {
        self.sync_all()
    }
}

/// Go `allocateQueryLogFile`: writes `filesize` zero bytes and syncs.
pub fn allocate_query_log_file(file: &mut dyn SyncWriter, filesize: usize) -> io::Result<()> {
    let zeroes = vec![0u8; filesize.min(32 * 1024)];
    let mut remaining = filesize;
    while remaining > 0 {
        let w = &zeroes[..remaining.min(zeroes.len())];
        let n = file.write(w)?;
        if n != w.len() {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "short write"));
        }
        remaining -= n;
    }
    file.sync()
}

/// Go `parseBrokenJSON`: the queries of an unfinished log as a JSON array, if there are any.
pub fn parse_broken_json(broken: &[u8]) -> (String, bool) {
    let mut queries: Vec<u8> = broken.iter().copied().filter(|&b| b != 0).collect();
    if !queries.is_empty() {
        let n = queries.len();
        queries[n - 1] = b']';
    }
    if queries.len() <= 1 {
        return ("[]".to_string(), false);
    }
    (String::from_utf8_lossy(&queries).into_owned(), true)
}

/// Go `trimStringByBytes`: cuts `s` to at most `size` bytes at a UTF-8 character start.
pub fn trim_string_by_bytes(s: &[u8], mut size: usize) -> &[u8] {
    let mut trim = s.len();
    if size < s.len() {
        while size > 0 && (s[size] & 0xC0) == 0x80 {
            size -= 1;
        }
        trim = size;
    }
    &s[..trim]
}

fn json_entry(query: &[u8], timestamp: i64) -> Vec<u8> {
    let mut out = String::from("{\"query\":");
    json_write_str(&String::from_utf8_lossy(query), &mut out);
    out.push_str(",\"timestamp_sec\":");
    out.push_str(&timestamp.to_string());
    out.push('}');
    out.into_bytes()
}

/// Writes a JSON string like Go's encoding/json.
fn json_write_str(v: &str, out: &mut String) {
    use std::fmt::Write as _;
    out.push('"');
    // Fast path: printable ASCII without characters that need escaping (the usual query text).
    if v.bytes().all(|b| (0x20..0x7f).contains(&b) && !matches!(b, b'"' | b'\\' | b'<' | b'>' | b'&')) {
        out.push_str(v);
        out.push('"');
        return;
    }
    for c in v.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Go `newJSONEntry`.
pub fn new_json_entry(query: &[u8]) -> Vec<u8> {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
    let min = json_entry(b"", ts);
    let q = trim_string_by_bytes(query, ENTRY_SIZE - (min.len() + 1));
    json_entry(q, ts)
}

/// A file with an in-memory copy of its bytes; writes go to both (Go maps the file).
pub struct MappedFile {
    pub bytes: Vec<u8>,
    file: Option<File>,
}

impl MappedFile {
    /// In-memory bytes without a file.
    pub fn from_bytes(bytes: Vec<u8>) -> MappedFile {
        MappedFile { bytes, file: None }
    }

    /// Writes `data` at `at` in memory only (clipped to the size); the caller writes the file.
    fn write_mem(&mut self, at: usize, data: &[u8]) {
        let end = (at + data.len()).min(self.bytes.len());
        if at < end {
            self.bytes[at..end].copy_from_slice(&data[..end - at]);
        }
    }

    /// Writes `data` at `at` (clipped to the file size), like Go's `copy` into the mapping.
    pub fn write(&mut self, at: usize, data: &[u8]) {
        let end = (at + data.len()).min(self.bytes.len());
        if at >= end {
            return;
        }
        let n = end - at;
        self.bytes[at..end].copy_from_slice(&data[..n]);
        if let Some(f) = &self.file {
            let _ = f.write_at(&data[..n], at as u64);
        }
    }

    /// Go `mmappedFile.Close`.
    pub fn close(&mut self) -> io::Result<()> {
        if let Some(f) = self.file.take() {
            f.sync_all()?;
        }
        Ok(())
    }
}

/// Go `getMMappedFile`: creates the file with `filesize` zero bytes.
pub fn get_mapped_file(filename: &Path, filesize: usize) -> io::Result<MappedFile> {
    let mut file = OpenOptions::new().create(true).read(true).write(true).truncate(true).open(filename)?;
    allocate_query_log_file(&mut file, filesize)?;
    Ok(MappedFile { bytes: vec![0u8; filesize], file: Some(file) })
}

/// Go `logUnfinishedQueries` without the logger: the unfinished queries of the file, if any.
pub fn unfinished_queries(filename: &Path, filesize: usize) -> Option<String> {
    let mut f = File::open(filename).ok()?;
    let mut buf = vec![0u8; filesize];
    let mut n = 0;
    while n < buf.len() {
        match f.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(_) => return None,
        }
    }
    let (q, ok) = parse_broken_json(&buf);
    if ok { Some(q) } else { None }
}

/// Tracks active queries in a file (Go `ActiveQueryTracker`).
pub struct ActiveQueryTracker {
    state: Mutex<TrackerState>,
    free: Condvar,
    max_concurrent: usize,
    /// The file (a clone of the handle in `state`), written outside the lock: a slot belongs to one query
    /// between its insert and its delete, so positioned writes to it need no lock. One write per insert and
    /// per delete, each of the whole slot.
    out: Option<File>,
    closed: std::sync::atomic::AtomicBool,
}

struct TrackerState {
    file: MappedFile,
    free_indices: VecDeque<usize>,
}

impl ActiveQueryTracker {
    /// A tracker over in-memory bytes only (Go tests build the struct with a byte slice).
    pub fn with_bytes(bytes: Vec<u8>, max_concurrent: usize) -> ActiveQueryTracker {
        let t = ActiveQueryTracker {
            state: Mutex::new(TrackerState { file: MappedFile::from_bytes(bytes), free_indices: VecDeque::new() }),
            free: Condvar::new(),
            max_concurrent,
            out: None,
            closed: std::sync::atomic::AtomicBool::new(false),
        };
        t.generate_indices(max_concurrent);
        t
    }

    /// Go `NewActiveQueryTracker`: creates `<dir>/queries.active`, logging the queries of the last run that did
    /// not finish to stderr.
    pub fn new(local_storage_path: &Path, max_concurrent: usize) -> Result<ActiveQueryTracker, Error> {
        if let Err(e) = std::fs::create_dir_all(local_storage_path) {
            return Err(Error::msg(format!("create active query log directory: {}", e)));
        }
        let filename = local_storage_path.join("queries.active");
        let filesize = 1 + max_concurrent * ENTRY_SIZE;
        if let Some(queries) = unfinished_queries(&filename, filesize) {
            eprintln!("These queries didn't finish in prometheus' last run: {}", queries);
        }
        let mut file = match get_mapped_file(&filename, filesize) {
            Ok(x) => x,
            Err(e) => return Err(Error::msg(format!("create mmap-ed active query log: {}", e))),
        };
        file.write(0, b"[");
        let out = match file.file.as_ref().map(|f| f.try_clone()) {
            Some(Ok(f)) => Some(f),
            Some(Err(e)) => return Err(Error::msg(format!("create mmap-ed active query log: {}", e))),
            None => None,
        };
        let t = ActiveQueryTracker {
            state: Mutex::new(TrackerState { file, free_indices: VecDeque::new() }),
            free: Condvar::new(),
            max_concurrent,
            out,
            closed: std::sync::atomic::AtomicBool::new(false),
        };
        t.generate_indices(max_concurrent);
        Ok(t)
    }

    fn generate_indices(&self, max_concurrent: usize) {
        let mut st = self.state.lock().unwrap();
        for i in 0..max_concurrent {
            st.free_indices.push_back(1 + i * ENTRY_SIZE);
        }
    }

    /// Writes `data` at `at` (clipped to the file size) to the file, if any and not closed.
    fn write_file(&self, at: usize, data: &[u8]) {
        let Some(f) = &self.out else { return };
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        let len = 1 + self.max_concurrent * ENTRY_SIZE;
        let end = (at + data.len()).min(len);
        if at < end {
            let _ = f.write_at(&data[..end - at], at as u64);
        }
    }

    /// A copy of the file bytes.
    pub fn bytes(&self) -> Vec<u8> {
        self.state.lock().unwrap().file.bytes.clone()
    }
}

impl QueryTracker for ActiveQueryTracker {
    fn get_max_concurrent(&self) -> usize {
        self.max_concurrent
    }

    fn insert(&self, ctx: &Context, query: &str) -> Result<usize, Error> {
        let entry = new_json_entry(query.as_bytes());
        let mut st = self.state.lock().unwrap();
        loop {
            if let Some(i) = st.free_indices.pop_front() {
                st.file.write_mem(i, &entry);
                st.file.write_mem(i + ENTRY_SIZE - 1, b",");
                drop(st);
                // The same bytes as the two writes above (the rest of a free slot is zero).
                let mut slot = [0u8; ENTRY_SIZE];
                let n = entry.len().min(ENTRY_SIZE);
                slot[..n].copy_from_slice(&entry[..n]);
                slot[ENTRY_SIZE - 1] = b',';
                self.write_file(i, &slot);
                return Ok(i);
            }
            if let Some(e) = ctx.err() {
                return Err(e);
            }
            // Wake up regularly to notice a cancelled or expired context.
            st = self.free.wait_timeout(st, Duration::from_millis(10)).unwrap().0;
        }
    }

    fn delete(&self, insert_index: usize) {
        // Clear the slot in the file before it can be given out again.
        self.write_file(insert_index, &[0u8; ENTRY_SIZE]);
        let mut st = self.state.lock().unwrap();
        st.file.write_mem(insert_index, &[0u8; ENTRY_SIZE]);
        st.free_indices.push_back(insert_index);
        drop(st);
        self.free.notify_one();
    }

    fn close(&self) -> Result<(), Error> {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
        let mut st = self.state.lock().unwrap();
        if let Err(e) = st.file.close() {
            return Err(Error::msg(format!("close ActiveQueryTracker.closer: {}", e)));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// JSON file logger (Go util/logging JSONFileLogger with the promslog JSON format).

/// Logs queries as JSON lines to a file (Go `logging.JSONFileLogger`).
pub struct JsonFileLogger {
    file: Mutex<Option<File>>,
    path: String,
}

impl JsonFileLogger {
    /// Go `NewJSONFileLogger`: opens `path` for appending.
    pub fn new(path: &str) -> Result<JsonFileLogger, Error> {
        match OpenOptions::new().append(true).create(true).open(path) {
            Ok(f) => Ok(JsonFileLogger { file: Mutex::new(Some(f)), path: path.to_string() }),
            Err(e) => Err(Error::msg(format!("can't create json log file: {}", e))),
        }
    }
}

fn write_log_value(v: &crate::engine::LogValue, out: &mut String) {
    use crate::engine::LogValue;
    match v {
        LogValue::Str(s) => json::write_str(s, out),
        LogValue::Int(i) => json::write_i64(*i, out),
        LogValue::Float(f) => json::write_f64(*f, out),
        LogValue::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        LogValue::Map(m) => {
            // Go's JSON encoder writes map keys sorted.
            let mut m: Vec<&(String, LogValue)> = m.iter().collect();
            m.sort_by(|a, b| a.0.cmp(&b.0));
            out.push('{');
            for (i, (k, v)) in m.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                json::write_str(k, out);
                out.push(':');
                write_log_value(v, out);
            }
            out.push('}');
        }
    }
}

/// The JSON line of a logged query.
pub fn query_log_line(entry: &crate::engine::QueryLogEntry) -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let ms = now.as_millis() as i64;
    let mut out = String::from("{\"time\":");
    json::write_str(&crate::engine::format_date(ms), &mut out);
    out.push_str(",\"level\":\"INFO\",\"source\":\"engine.go:1125\",\"msg\":\"promql query logged\",\"params\":{");
    // Go marshals the params map with sorted keys.
    let mut params: Vec<&(String, crate::engine::LogValue)> = entry.params.iter().collect();
    params.sort_by(|a, b| a.0.cmp(&b.0));
    for (i, (k, v)) in params.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        json::write_str(k, &mut out);
        out.push(':');
        write_log_value(v, &mut out);
    }
    out.push('}');
    if let Some(e) = &entry.error {
        out.push_str(",\"error\":");
        json::write_str(&e.to_string(), &mut out);
    }
    out.push_str(",\"stats\":");
    entry.stats.write_json(&mut out);
    for (k, v) in &entry.origin {
        out.push(',');
        json::write_str(k, &mut out);
        out.push(':');
        write_log_value(v, &mut out);
    }
    out.push_str("}\n");
    out
}

impl crate::engine::QueryLogger for JsonFileLogger {
    fn log(&self, entry: &crate::engine::QueryLogEntry) {
        let line = query_log_line(entry);
        if let Some(f) = self.file.lock().unwrap().as_mut() {
            let _ = f.write_all(line.as_bytes());
        }
    }

    fn close(&self) -> Result<(), Error> {
        match self.file.lock().unwrap().take() {
            Some(_) => Ok(()),
            None => Err(Error::msg(format!("close {}: file already closed", self.path))),
        }
    }
}
