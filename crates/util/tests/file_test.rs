//! Port of upstream/util/logging/file_test.go.
use util::logging::JsonFileLogger;

fn temp_file(name: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    let p = std::env::temp_dir().join(format!("logging{name}{}-{nanos}", std::process::id()));
    std::fs::File::create(&p).expect("create temp file");
    p.to_string_lossy().into_owned()
}

fn get_log_lines(name: &str) -> Vec<String> {
    let content = std::fs::read_to_string(name).expect("read log file");
    content.split('\n').filter(|l| !l.is_empty()).map(|l| l.to_string()).collect()
}

#[test]
fn test_json_file_logger_basic() {
    let f = temp_file("basic");

    let log_handler = JsonFileLogger::new(&f).expect("new");
    assert!(log_handler.is_some(), "logger handler can't be nil");
    let log_handler = log_handler.unwrap_or_else(|| unreachable!());

    let logger = log_handler.logger();
    logger.info("test", &[("hello", &"world")]);

    let r = get_log_lines(&f);
    assert_eq!(r.len(), 1, "expected 1 log line");
    // adapted: Go's `source` is a .go file; here it is this .rs file.
    let re = regex::Regex::new(r#"^\{"time":"[^"]+","level":"INFO","source":"\w+\.rs:\d+","msg":"test","hello":"world"\}"#).unwrap();
    assert!(re.is_match(&r[0]), "unexpected content: {r:?}");

    assert!(log_handler.close().is_ok());

    let err = log_handler.close();
    assert!(err.is_err());
    assert!(err.unwrap_err().ends_with("file already closed"), "file not closed");

    std::fs::remove_file(&f).expect("remove");
}

#[test]
fn test_json_file_logger_parallel() {
    let f = temp_file("parallel");

    let log_handler = JsonFileLogger::new(&f).expect("new");
    assert!(log_handler.is_some(), "logger handler can't be nil");
    let log_handler = log_handler.unwrap_or_else(|| unreachable!());

    let logger = log_handler.logger();
    logger.info("test", &[("hello", &"world")]);

    let log_handler2 = JsonFileLogger::new(&f).expect("new");
    assert!(log_handler2.is_some(), "logger handler can't be nil");
    let log_handler2 = log_handler2.unwrap_or_else(|| unreachable!());

    let logger2 = log_handler2.logger();
    logger2.info("test", &[("hello", &"world")]);

    assert!(log_handler.close().is_ok());

    let err = log_handler.close();
    assert!(err.is_err());
    assert!(err.unwrap_err().ends_with("file already closed"), "file not closed");

    assert!(log_handler2.close().is_ok());

    let err = log_handler2.close();
    assert!(err.is_err());
    assert!(err.unwrap_err().ends_with("file already closed"), "file not closed");

    std::fs::remove_file(&f).expect("remove");
}
