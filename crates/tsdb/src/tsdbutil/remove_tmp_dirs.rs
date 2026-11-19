//! Port of `tsdb/tsdbutil/remove_tmp_dirs.go`.

use std::fs::{self, DirEntry};
use std::io;
use std::path::Path;

use storage::Error;

use crate::Logger;

/// Go `RemoveTmpDirs`: removes (recursively) every entry of `dir` for which `is_tmp_dir` is true. A missing
/// `dir` is not an error; a failed removal is logged and skipped.
pub fn remove_tmp_dirs(l: &Logger, dir: &Path, is_tmp_dir: impl Fn(&DirEntry) -> bool) -> Result<(), Error> {
    let open_err = |e: io::Error| Error::msg(format!("open {}: {}", dir.display(), crate::io_err_text(&e)));
    let files = match fs::read_dir(dir) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(open_err(e)),
    };
    // Go `os.ReadDir` reads every entry first and returns them sorted by name.
    let mut entries = Vec::new();
    for f in files {
        entries.push(f.map_err(open_err)?);
    }
    entries.sort_by_key(|e| e.file_name());
    for f in entries {
        if is_tmp_dir(&f) {
            let p = dir.join(f.file_name());
            if let Err(err) = remove_all(&p) {
                let text = format!("unlinkat {}: {}", p.display(), crate::io_err_text(&err));
                l.error("failed to delete tmp dir", &[("dir", &p.display()), ("err", &text)]);
                continue;
            }
            l.info("Found and deleted tmp dir", &[("dir", &p.display())]);
        }
    }
    Ok(())
}

/// Go `os.RemoveAll`: removes a file or a directory tree; a missing path is not an error.
fn remove_all(p: &Path) -> io::Result<()> {
    let r = match fs::symlink_metadata(p) {
        Ok(m) if m.is_dir() => fs::remove_dir_all(p),
        Ok(_) => fs::remove_file(p),
        Err(e) => Err(e),
    };
    match r {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        r => r,
    }
}
