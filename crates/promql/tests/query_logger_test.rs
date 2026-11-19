//! Port of `promql/query_logger_test.go`.

use std::io::{self, Read, Write};

use model::regexp::Regexp;
use promql::QueryTracker;
use promql::query_logger::*;
use storage::Context;

struct QueryLogWriter {
    written: usize,
    err: Option<io::ErrorKind>,
}

impl Write for QueryLogWriter {
    fn write(&mut self, p: &[u8]) -> io::Result<usize> {
        if let Some(k) = self.err {
            return Err(io::Error::from(k));
        }
        self.written += p.len();
        Ok(p.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SyncWriter for QueryLogWriter {
    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct ShortQueryLogWriter;

impl Write for ShortQueryLogWriter {
    fn write(&mut self, p: &[u8]) -> io::Result<usize> {
        Ok(p.len() - 1)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SyncWriter for ShortQueryLogWriter {
    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct SyncErrorQueryLogWriter {
    inner: QueryLogWriter,
    sync_err: io::ErrorKind,
}

impl Write for SyncErrorQueryLogWriter {
    fn write(&mut self, p: &[u8]) -> io::Result<usize> {
        self.inner.write(p)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SyncWriter for SyncErrorQueryLogWriter {
    fn sync(&mut self) -> io::Result<()> {
        Err(io::Error::from(self.sync_err))
    }
}

fn matches(re: &str, have: &[u8]) -> bool {
    Regexp::compile(re).unwrap().is_match(std::str::from_utf8(have).unwrap())
}

#[test]
fn test_query_logging() {
    let query_logger = ActiveQueryTracker::with_bytes(vec![0u8; 4096], 4);
    let very_long_string = "MassiveQueryThatNeverEndsAndExceedsTwoHundredBytesWhichIsTheSizeOfEntrySizeAndShouldThusBeTruncatedAndIamJustGoingToRepeatTheSameCharactersAgainProbablyBecauseWeAreStillOnlyHalfWayDoneOrMaybeNotOrMaybe".repeat(5);
    let queries = ["TestQuery".to_string(), very_long_string.clone(), String::new(), "SpecialCharQuery{host=\"2132132\", id=123123}".to_string()];
    let trimmed = String::from_utf8(trim_string_by_bytes(very_long_string.as_bytes(), ENTRY_SIZE - 40).to_vec()).unwrap();
    let want = [
        r#"^{"query":"TestQuery","timestamp_sec":\d+}\x00*,$"#.to_string(),
        format!(r#"^{{"query":"{}","timestamp_sec":\d+}}\x00*,$"#, trimmed),
        r#"^{"query":"","timestamp_sec":\d+}\x00*,$"#.to_string(),
        r#"^{"query":"SpecialCharQuery{host=\\"2132132\\", id=123123}","timestamp_sec":\d+}\x00*,$"#.to_string(),
    ];
    for i in 0..4 {
        let start = 1 + i * ENTRY_SIZE;
        let end = start + ENTRY_SIZE;
        query_logger.insert(&Context::background(), &queries[i]).unwrap();
        let bytes = query_logger.bytes();
        assert!(matches(&want[i], &bytes[start..end]), "Query not written correctly: {}", queries[i]);
    }
    for i in 0..4 {
        query_logger.delete(1 + i * ENTRY_SIZE);
    }
    let bytes = query_logger.bytes();
    assert!(matches(r"^\x00+$", &bytes[1..1 + ENTRY_SIZE * 4]), "All queries not deleted properly. Want only null bytes \\x00");
}

#[test]
fn test_index_reuse() {
    let query_logger = ActiveQueryTracker::with_bytes(vec![0u8; 1 + 3 * ENTRY_SIZE], 3);
    let ctx = Context::background();
    query_logger.insert(&ctx, "TestQuery1").unwrap();
    query_logger.insert(&ctx, "TestQuery2").unwrap();
    query_logger.insert(&ctx, "TestQuery3").unwrap();
    query_logger.delete(1 + ENTRY_SIZE);
    query_logger.delete(1);
    query_logger.insert(&ctx, "ThisShouldBeInsertedAtIndex2").unwrap();
    query_logger.insert(&ctx, "ThisShouldBeInsertedAtIndex1").unwrap();
    let want = [
        r#"^{"query":"ThisShouldBeInsertedAtIndex1","timestamp_sec":\d+}\x00*,$"#,
        r#"^{"query":"ThisShouldBeInsertedAtIndex2","timestamp_sec":\d+}\x00*,$"#,
        r#"^{"query":"TestQuery3","timestamp_sec":\d+}\x00*,$"#,
    ];
    let bytes = query_logger.bytes();
    for i in 0..3 {
        let start = 1 + i * ENTRY_SIZE;
        let end = start + ENTRY_SIZE;
        assert!(matches(want[i], &bytes[start..end]), "Index not reused properly.");
    }
}

#[test]
fn test_m_map_file() {
    let dir = std::env::temp_dir().join(format!("promql-mmap-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let fpath = dir.join("mmappedFile");
    let data = b"ab";
    // adapted: Go copies into the mapping; here writes go through MappedFile, which mirrors them to the file.
    let mut f = get_mapped_file(&fpath, 2).unwrap();
    f.write(0, data);
    f.close().unwrap();
    let mut file = std::fs::File::open(&fpath).unwrap();
    let mut bytes = [0u8; 4];
    let n = file.read(&mut bytes).expect("Unexpected error while reading file.");
    assert_eq!(2, n);
    assert_eq!(&data[..], &bytes[..2], "Mmap failed");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_allocate_query_log_file() {
    let mut writer = QueryLogWriter { written: 0, err: None };
    allocate_query_log_file(&mut writer, 100_000).unwrap();
    assert_eq!(100_000, writer.written);
}

#[test]
fn test_allocate_query_log_file_returns_write_errors() {
    let err = allocate_query_log_file(&mut QueryLogWriter { written: 0, err: Some(io::ErrorKind::PermissionDenied) }, 1)
        .unwrap_err();
    assert_eq!(io::ErrorKind::PermissionDenied, err.kind());
    // Go io.ErrShortWrite.
    let err = allocate_query_log_file(&mut ShortQueryLogWriter, 1).unwrap_err();
    assert_eq!(io::ErrorKind::WriteZero, err.kind());
    assert_eq!("short write", err.to_string());
}

#[test]
fn test_allocate_query_log_file_returns_sync_error() {
    let mut w = SyncErrorQueryLogWriter { inner: QueryLogWriter { written: 0, err: None }, sync_err: io::ErrorKind::PermissionDenied };
    let err = allocate_query_log_file(&mut w, 1).unwrap_err();
    assert_eq!(io::ErrorKind::PermissionDenied, err.kind());
}

#[test]
fn test_new_active_query_tracker_returns_error() {
    let dir = std::env::temp_dir().join(format!("promql-aqt-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let local_storage_path = dir.join("not-a-directory");
    std::fs::write(&local_storage_path, b"").unwrap();
    let r = ActiveQueryTracker::new(&local_storage_path, 1);
    assert!(r.is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_trim_string_by_bytes() {
    let cases: [(&str, &[u8], usize, &[u8]); 4] = [
        ("normal ASCII string", b"hello", 3, b"hel"),
        ("no trimming needed", b"hi", 10, b"hi"),
        ("UTF-8 multibyte character boundary", "日本".as_bytes(), 4, "日".as_bytes()),
        ("invalid UTF-8 continuation-only bytes", &[0x80, 0x81, 0x82, 0x83, 0x84], 4, b""),
    ];
    for (name, input, size, expected) in cases {
        let r = std::panic::catch_unwind(|| trim_string_by_bytes(input, size).to_vec());
        assert!(r.is_ok(), "{}: panicked", name);
        assert_eq!(expected, r.unwrap().as_slice(), "{}", name);
    }
}

#[test]
fn test_parse_broken_json() {
    let cases: [(&[u8], bool, &str); 5] = [
        (b"", false, ""),
        (b"\x00\x00", false, ""),
        (b"\x00[\x00", false, ""),
        (b"\x00[]\x00", true, "[]"),
        (b"[\"up == 0\",\"rate(http_requests[2w]\"]\x00\x00\x00", true, "[\"up == 0\",\"rate(http_requests[2w]\"]"),
    ];
    for (b, ok, out) in cases {
        let (o, k) = parse_broken_json(b);
        assert_eq!(ok, k);
        if k {
            assert_eq!(out, o);
        }
    }
}
