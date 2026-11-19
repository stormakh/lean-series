//! Port of `promql/query_logger.go`: the active query tracker, which writes running queries to
//! `queries.active` so that queries that crashed Prometheus are logged at the next start.
//!
//! As in Go, the file is mapped into memory (through `util::runtime::SharedMap`), so tracking a query costs no
//! system call.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
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
    let mut out = Vec::with_capacity(query.len() + query.len() / 8 + 48);
    out.extend_from_slice(b"{\"query\":");
    match std::str::from_utf8(query) {
        Ok(q) => json_write_str(q, &mut out),
        Err(_) => json_write_str(&String::from_utf8_lossy(query), &mut out),
    }
    out.extend_from_slice(b",\"timestamp_sec\":");
    out.extend_from_slice(timestamp.to_string().as_bytes());
    out.push(b'}');
    out
}

/// Writes a JSON string like Go's encoding/json.
/// Copies runs of plain bytes at once (query texts are mostly plain, but have quotes in label matchers).
fn json_write_str(v: &str, out: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let b = v.as_bytes();
    out.push(b'"');
    let mut start = 0;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let (esc, n): (&[u8], usize) = match c {
            b'"' => (b"\\\"", 1),
            b'\\' => (b"\\\\", 1),
            b'\n' => (b"\\n", 1),
            b'\r' => (b"\\r", 1),
            b'\t' => (b"\\t", 1),
            b'<' => (b"\\u003c", 1),
            b'>' => (b"\\u003e", 1),
            b'&' => (b"\\u0026", 1),
            // U+2028 and U+2029 (E2 80 A8 / E2 80 A9)
            0xE2 if i + 2 < b.len() && b[i + 1] == 0x80 && b[i + 2] == 0xA8 => (b"\\u2028", 3),
            0xE2 if i + 2 < b.len() && b[i + 1] == 0x80 && b[i + 2] == 0xA9 => (b"\\u2029", 3),
            _ if c < 0x20 => (b"", 1),
            _ => {
                i += 1;
                continue;
            }
        };
        out.extend_from_slice(&b[start..i]);
        if esc.is_empty() {
            out.extend_from_slice(b"\\u00");
            out.push(HEX[(c >> 4) as usize]);
            out.push(HEX[(c & 15) as usize]);
        } else {
            out.extend_from_slice(esc);
        }
        i += n;
        start = i;
    }
    out.extend_from_slice(&b[start..]);
    out.push(b'"');
}

/// Go `newJSONEntry`.
pub fn new_json_entry(query: &[u8]) -> Vec<u8> {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
    let min = json_entry(b"", ts);
    let q = trim_string_by_bytes(query, ENTRY_SIZE - (min.len() + 1));
    json_entry(q, ts)
}

/// The bytes of the active query file: memory only (tests), or the file mapped shared as Go does, so that a write
/// is a memory copy without a system call.
enum Mem {
    Bytes(Vec<u8>),
    Map(util::runtime::SharedMap),
    /// After `close` (Go unmaps the file; later writes are dropped here).
    Closed,
}

/// The active query file (Go `mmappedFile`).
pub struct MappedFile {
    mem: Mem,
}

impl MappedFile {
    /// In-memory bytes without a file.
    pub fn from_bytes(bytes: Vec<u8>) -> MappedFile {
        MappedFile { mem: Mem::Bytes(bytes) }
    }

    /// The current bytes (empty after `close`).
    pub fn bytes(&self) -> &[u8] {
        match &self.mem {
            Mem::Bytes(b) => b,
            Mem::Map(m) => m.as_slice(),
            Mem::Closed => &[],
        }
    }

    /// Writes `data` at `at` (clipped to the file size), like Go's `copy` into the mapping.
    pub fn write(&mut self, at: usize, data: &[u8]) {
        let b: &mut [u8] = match &mut self.mem {
            Mem::Bytes(b) => b,
            Mem::Map(m) => m.as_mut_slice(),
            Mem::Closed => return,
        };
        let end = (at + data.len()).min(b.len());
        if at < end {
            b[at..end].copy_from_slice(&data[..end - at]);
        }
    }

    /// Go `mmappedFile.Close`: writes the mapped pages to the file and unmaps it.
    pub fn close(&mut self) -> io::Result<()> {
        if let Mem::Map(m) = std::mem::replace(&mut self.mem, Mem::Closed) {
            m.sync()?;
        }
        Ok(())
    }
}

/// Go `getMMappedFile`: creates the file with `filesize` zero bytes and maps it.
pub fn get_mapped_file(filename: &Path, filesize: usize) -> io::Result<MappedFile> {
    let mut file = OpenOptions::new().create(true).read(true).write(true).truncate(true).open(filename)?;
    allocate_query_log_file(&mut file, filesize)?;
    let m = util::runtime::SharedMap::new(&file, filesize)?;
    Ok(MappedFile { mem: Mem::Map(m) })
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
}

struct TrackerState {
    file: MappedFile,
    free_indices: VecDeque<usize>,
    /// Inserts waiting for a free slot; `delete` wakes one only then (a futex wake is a system call).
    waiters: usize,
}

impl ActiveQueryTracker {
    /// A tracker over in-memory bytes only (Go tests build the struct with a byte slice).
    pub fn with_bytes(bytes: Vec<u8>, max_concurrent: usize) -> ActiveQueryTracker {
        let t = ActiveQueryTracker {
            state: Mutex::new(TrackerState { file: MappedFile::from_bytes(bytes), free_indices: VecDeque::new(), waiters: 0 }),
            free: Condvar::new(),
            max_concurrent,
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
        let t = ActiveQueryTracker {
            state: Mutex::new(TrackerState { file, free_indices: VecDeque::new(), waiters: 0 }),
            free: Condvar::new(),
            max_concurrent,
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

    /// A copy of the file bytes.
    pub fn bytes(&self) -> Vec<u8> {
        self.state.lock().unwrap().file.bytes().to_vec()
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
                st.file.write(i, &entry);
                st.file.write(i + ENTRY_SIZE - 1, b",");
                return Ok(i);
            }
            if let Some(e) = ctx.err() {
                return Err(e);
            }
            // Wake up regularly to notice a cancelled or expired context.
            st.waiters += 1;
            st = self.free.wait_timeout(st, Duration::from_millis(10)).unwrap().0;
            st.waiters -= 1;
        }
    }

    fn delete(&self, insert_index: usize) {
        let mut st = self.state.lock().unwrap();
        st.file.write(insert_index, &[0u8; ENTRY_SIZE]);
        st.free_indices.push_back(insert_index);
        let wake = st.waiters > 0;
        drop(st);
        if wake {
            self.free.notify_one();
        }
    }

    fn close(&self) -> Result<(), Error> {
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
