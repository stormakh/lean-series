//! Port of `tsdb/tsdbutil/dir_locker.go` and `dir_locker_testutil.go`: the `lock` file of a data directory.

use std::path::{Component, Path, PathBuf};

use storage::Error;
use tsdb_format::fileutil::{self, Releaser};

use crate::metrics::Gauge;
use crate::Logger;

/// The gauge value when the lock file is disabled (Go `lockfileDisabled`).
pub const LOCKFILE_DISABLED: i64 = -1;
/// The gauge value when a lock file of a previous run was replaced (Go `lockfileReplaced`).
pub const LOCKFILE_REPLACED: i64 = 0;
/// The gauge value when the lock file was created cleanly (Go `lockfileCreatedCleanly`).
pub const LOCKFILE_CREATED_CLEANLY: i64 = 1;

/// Go `DirLocker`: holds the `flock` of `<dir>/lock`.
#[derive(Debug)]
pub struct DirLocker {
    logger: Logger,
    /// `prometheus_<subsystem>_clean_start`: -1: lockfile is disabled. 0: a lockfile from a previous execution
    /// was replaced. 1: lockfile creation was clean.
    pub created_cleanly: Gauge,
    metric_name: String,
    releaser: Option<Releaser>,
    path: PathBuf,
}

impl DirLocker {
    /// Go `NewDirLocker`: a locker for `<abs dir>/lock`; the gauge starts at -1 (disabled).
    pub fn new(dir: &Path, subsystem: &str, logger: Logger) -> Result<DirLocker, Error> {
        let created_cleanly = Gauge::new();
        created_cleanly.set(LOCKFILE_DISABLED as f64);
        let absdir = abs(dir).map_err(|e| Error::msg(crate::io_err_text(&e)))?;
        Ok(DirLocker {
            logger,
            created_cleanly,
            metric_name: format!("prometheus_{}_clean_start", subsystem),
            releaser: None,
            path: absdir.join("lock"),
        })
    }

    /// The name of the `created_cleanly` gauge (`prometheus_<subsystem>_clean_start`).
    pub fn metric_name(&self) -> &str {
        &self.metric_name
    }

    /// The path of the lock file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Go `Lock`: takes the lock; sets the gauge to 0 when the file already existed, else 1.
    pub fn lock(&mut self) -> Result<(), Error> {
        if self.releaser.is_some() {
            return Err(Error::msg("DB lock already obtained"));
        }

        if std::fs::metadata(&self.path).is_ok() {
            self.logger.warn(
                "A lockfile from a previous execution already existed. It was replaced",
                &[("file", &self.path.display())],
            );
            self.created_cleanly.set(LOCKFILE_REPLACED as f64);
        } else {
            self.created_cleanly.set(LOCKFILE_CREATED_CLEANLY as f64);
        }

        match fileutil::flock(&self.path).0 {
            Ok(r) => {
                self.releaser = Some(r);
                Ok(())
            }
            Err(e) => {
                // Go: the flock(2) error is a bare errno; an open error is an *os.PathError.
                let text = match e.raw_os_error() {
                    Some(c) if c == libc::EWOULDBLOCK || c == libc::EAGAIN => crate::io_err_text(&e),
                    _ => format!("open {}: {}", self.path.display(), crate::io_err_text(&e)),
                };
                let src = Error::msg(text);
                Err(Error::wrap("lock DB directory", src))
            }
        }
    }

    /// Go `Release`: unlocks and removes the lock file; does nothing when not locked.
    pub fn release(&mut self) -> Result<(), Error> {
        let Some(mut r) = self.releaser.take() else {
            return Ok(());
        };
        let releaser_err = r.release().err().map(|e| Error::msg(crate::io_err_text(&e)));
        let remove_err = std::fs::remove_file(&self.path)
            .err()
            .map(|e| Error::msg(format!("remove {}: {}", self.path.display(), crate::io_err_text(&e))));
        match Error::join([releaser_err, remove_err]) {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// Go `filepath.Abs`: joins relative paths to the working directory and cleans the result lexically.
fn abs(dir: &Path) -> std::io::Result<PathBuf> {
    let p = if dir.is_absolute() { dir.to_path_buf() } else { std::env::current_dir()?.join(dir) };
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                if out.parent().is_some() {
                    out.pop();
                }
            }
            c => out.push(c.as_os_str()),
        }
    }
    Ok(out)
}

/// Go `TestDirLockerUsage` (dir_locker_testutil.go): checks the gauge and the removal of the lock file for every
/// combination of "lock file already exists" and "lock file disabled". `temp_dir` gives a new empty directory,
/// `open` opens the user of the locker on it (taking the lock when its second argument is true), `locker` gets
/// its locker and `close` closes it (which must release the lock).
#[doc(hidden)]
pub fn test_dir_locker_usage<T>(
    mut temp_dir: impl FnMut() -> PathBuf,
    open: impl Fn(&Path, bool) -> T,
    locker: impl Fn(&T) -> &DirLocker,
    close: impl Fn(T),
) {
    struct Case {
        file_already_exists: bool,
        lock_file_disabled: bool,
        expected_value: i64,
    }
    let cases = [
        Case { file_already_exists: false, lock_file_disabled: false, expected_value: LOCKFILE_CREATED_CLEANLY },
        Case { file_already_exists: true, lock_file_disabled: false, expected_value: LOCKFILE_REPLACED },
        Case { file_already_exists: true, lock_file_disabled: true, expected_value: LOCKFILE_DISABLED },
        Case { file_already_exists: false, lock_file_disabled: true, expected_value: LOCKFILE_DISABLED },
    ];

    for c in cases {
        let tmpdir = temp_dir();

        // Test preconditions (file already exists + lockfile option).
        if c.file_already_exists {
            let tmp_locker = DirLocker::new(&tmpdir, "tsdb", Logger::nop()).expect("new dir locker");
            std::fs::write(tmp_locker.path(), b"").expect("write lock file");
        }

        let user = open(&tmpdir, !c.lock_file_disabled);
        let l = locker(&user);
        assert_eq!(
            c.expected_value as f64,
            l.created_cleanly.get(),
            "fileAlreadyExists:{} lockFileDisabled:{}",
            c.file_already_exists,
            c.lock_file_disabled
        );
        let path = l.path().to_path_buf();

        // Close the client. This should delete the lockfile.
        close(user);

        // Check that the lockfile is always deleted.
        if !c.lock_file_disabled {
            let err = std::fs::metadata(&path).expect_err("lockfile was not deleted");
            assert_eq!(err.kind(), std::io::ErrorKind::NotFound, "lockfile was not deleted");
        }
    }
}
