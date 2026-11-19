//! Port of upstream/util/logging/dedupe_test.go.
use std::time::Duration;
use util::logging::{Buffer, Config, Logger, dedupe};

fn non_empty_lines(s: &str) -> Vec<String> {
    s.split('\n').filter(|l| !l.is_empty()).map(|l| l.to_string()).collect()
}

#[test]
fn test_dedupe() {
    let buf = Buffer::new();
    let dlog = dedupe(&Logger::new(Config { writer: Some(Box::new(buf.clone())), ..Config::default() }), Duration::from_millis(100));

    // Log 10 times quickly, ensure they are deduped.
    for _ in 0..10 {
        dlog.info("test", &[("hello", &"world")]);
    }

    // Trim empty lines
    let lines = non_empty_lines(&buf.contents());
    assert_eq!(lines.len(), 1);

    // Wait, then log again, make sure it is logged.
    std::thread::sleep(Duration::from_millis(200));
    dlog.info("test", &[("hello", &"world")]);
    // Trim empty lines
    let lines = non_empty_lines(&buf.contents());
    assert_eq!(lines.len(), 2);
}

#[test]
fn test_dedupe_concurrent() {
    let dlog = dedupe(&Logger::new(Config::default()), Duration::from_millis(250));

    // adapted: Go checks require.NotPanics around starting two goroutines that log concurrently through `With`
    // handles; here the threads are joined and must not have panicked.
    let concurrent_write_func = || {
        let d1 = dlog.clone();
        let t1 = std::thread::spawn(move || {
            let dlog1 = d1.with(&[("writer", &1)]);
            for _ in 0..10 {
                dlog1.with(&[("foo", &"bar")]).info("test", &[("hello", &"world")]);
            }
        });
        let d2 = dlog.clone();
        let t2 = std::thread::spawn(move || {
            let dlog2 = d2.with(&[("writer", &2)]);
            for _ in 0..10 {
                dlog2.with(&[("foo", &"bar")]).info("test", &[("hello", &"world")]);
            }
        });
        vec![t1, t2]
    };

    for t in concurrent_write_func() {
        assert!(t.join().is_ok());
    }
}
