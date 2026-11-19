//! Port of `tsdb/tsdbutil/dir_locker_test.go`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use tsdb::tsdbutil::{test_dir_locker_usage, DirLocker};
use tsdb::Logger;

/// A unique empty directory for one test (Go `t.TempDir()`).
fn temp_dir(name: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let d = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "dir_locker_test-{name}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn test_lockfile() {
    let mut dirs = Vec::new();
    test_dir_locker_usage(
        || {
            let d = temp_dir("lockfile");
            dirs.push(d.clone());
            d
        },
        |data, create_lock| {
            let mut locker = DirLocker::new(data, "tsdbutil", Logger::nop()).unwrap();
            if create_lock {
                locker.lock().unwrap();
            }
            locker
        },
        |locker| locker,
        |mut locker| locker.release().unwrap(),
    );
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}
