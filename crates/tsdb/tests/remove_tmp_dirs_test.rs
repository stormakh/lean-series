//! Port of `tsdb/tsdbutil/remove_tmp_dirs_test.go`.

use std::fs::{self, DirEntry};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tsdb::tsdbutil::remove_tmp_dirs;
use tsdb::Logger;

/// A unique empty directory for one test (Go `t.TempDir()`); removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        static N: AtomicU64 = AtomicU64::new(0);
        let d = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "remove_tmp_dirs_test-{name}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        TempDir(d)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn is_dir(fi: &DirEntry) -> bool {
    fi.file_type().map(|t| t.is_dir()).unwrap_or(false)
}

fn name(fi: &DirEntry) -> String {
    fi.file_name().to_string_lossy().into_owned()
}

#[test]
fn test_remove_tmp_dirs() {
    struct Case {
        name: &'static str,
        is_tmp_dir: fn(&DirEntry) -> bool,
        setup: fn(&Path),
        /// Directories that should remain after cleanup.
        expected_dirs: Vec<&'static str>,
    }
    let tests = vec![
        Case {
            name: "remove directories with tmp prefix",
            is_tmp_dir: |fi| is_dir(fi) && name(fi).starts_with("tmp"),
            setup: |dir| {
                fs::create_dir(dir.join("tmpdir1")).unwrap();
                fs::create_dir(dir.join("tmpdir2")).unwrap();
                fs::create_dir(dir.join("normaldir")).unwrap();
            },
            expected_dirs: vec!["normaldir"],
        },
        Case {
            name: "remove directories with specific suffix",
            is_tmp_dir: |fi| is_dir(fi) && name(fi).ends_with(".tmp"),
            setup: |dir| {
                fs::create_dir(dir.join("data.tmp")).unwrap();
                fs::create_dir(dir.join("cache.tmp")).unwrap();
                fs::create_dir(dir.join("permanent")).unwrap();
            },
            expected_dirs: vec!["permanent"],
        },
        Case {
            name: "no temporary directories to remove",
            is_tmp_dir: |fi| is_dir(fi) && name(fi).starts_with("tmp"),
            setup: |dir| {
                fs::create_dir(dir.join("normaldir1")).unwrap();
                fs::create_dir(dir.join("normaldir2")).unwrap();
            },
            expected_dirs: vec!["normaldir1", "normaldir2"],
        },
        Case {
            name: "empty directory",
            is_tmp_dir: |fi| is_dir(fi) && name(fi).starts_with("tmp"),
            setup: |_| {}, // No setup needed - directory is empty.
            expected_dirs: vec![],
        },
        Case {
            name: "directory with files only (no directories)",
            is_tmp_dir: |fi| is_dir(fi) && name(fi).starts_with("tmp"),
            setup: |dir| {
                fs::write(dir.join("tmpfile1.txt"), b"test").unwrap();
                fs::write(dir.join("tmpfile2.txt"), b"test").unwrap();
            },
            expected_dirs: vec![],
        },
    ];

    for tt in tests {
        let test_dir = TempDir::new("remove");

        (tt.setup)(&test_dir.0);

        remove_tmp_dirs(&Logger::nop(), &test_dir.0, tt.is_tmp_dir).unwrap();

        // Get actual remaining directories.
        let mut actual_dirs: Vec<String> = Vec::new();
        for entry in fs::read_dir(&test_dir.0).unwrap() {
            let entry = entry.unwrap();
            if is_dir(&entry) {
                actual_dirs.push(name(&entry));
            }
        }

        // Go require.ElementsMatch: the same elements in any order.
        let mut expected: Vec<String> = tt.expected_dirs.iter().map(|s| s.to_string()).collect();
        expected.sort();
        actual_dirs.sort();
        assert_eq!(expected, actual_dirs, "{}: Remaining directories don't match expected", tt.name);
    }
}

#[test]
fn test_remove_tmp_dirs_non_existent_directory() {
    let test_dir = TempDir::new("nonexistent");
    let non_existent = test_dir.0.join("does_not_exist");

    remove_tmp_dirs(&Logger::nop(), &non_existent, |_| true).unwrap();
}
