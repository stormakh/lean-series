//! Port of `tsdb/fileutil`: directory helpers, file locking, mmap, preallocation and syncing.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

mod direct_io;
pub use direct_io::*;

/// Go `DirSize`: the total size of the files under `dir`. Files removed during the walk are ignored.
pub fn dir_size(dir: &Path) -> io::Result<i64> {
    fn walk(p: &Path, size: &mut i64, root: bool) -> io::Result<()> {
        let md = match fs::symlink_metadata(p) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound && !root => return Ok(()),
            Err(e) => return Err(e),
        };
        if !md.is_dir() {
            *size += md.len() as i64;
            return Ok(());
        }
        let rd = match fs::read_dir(p) {
            Ok(rd) => rd,
            Err(e) if e.kind() == io::ErrorKind::NotFound && !root => return Ok(()),
            Err(e) => return Err(e),
        };
        let mut names: Vec<PathBuf> = rd.filter_map(|e| e.ok().map(|e| e.path())).collect();
        names.sort();
        for n in names {
            walk(&n, size, false)?;
        }
        Ok(())
    }
    let mut size = 0;
    walk(dir, &mut size, true)?;
    Ok(size)
}

/// Go `OpenDir`: opens a directory for syncing.
pub fn open_dir(path: &Path) -> io::Result<File> {
    File::open(path)
}

/// Go `CopyDirs`: copies all directories, subdirectories and files recursively, including empty folders.
pub fn copy_dirs(src: &Path, dest: &Path) -> io::Result<()> {
    fs::create_dir_all(dest)?;
    for rel in read_dirs(src)? {
        let dp = dest.join(&rel);
        let sp = src.join(&rel);
        let st = fs::metadata(&sp)?;
        // Empty directories are also created.
        if st.is_dir() {
            fs::create_dir_all(&dp)?;
            continue;
        }
        fs::write(&dp, fs::read(&sp)?)?;
    }
    Ok(())
}

/// Relative paths of all files and directories under `src`, in lexical walk order (Go `filepath.Walk`).
fn read_dirs(src: &Path) -> io::Result<Vec<PathBuf>> {
    fn walk(root: &Path, p: &Path, out: &mut Vec<PathBuf>) {
        if p != root {
            if let Ok(rel) = p.strip_prefix(root) {
                out.push(rel.to_path_buf());
            }
        }
        let Ok(md) = fs::symlink_metadata(p) else {
            return;
        };
        if !md.is_dir() {
            return;
        }
        let Ok(rd) = fs::read_dir(p) else { return };
        let mut names: Vec<PathBuf> = rd.filter_map(|e| e.ok().map(|e| e.path())).collect();
        names.sort();
        for n in names {
            walk(root, &n, out);
        }
    }
    let mut out = Vec::new();
    walk(src, src, &mut out);
    Ok(out)
}

/// Go `Rename`: renames a file and syncs the parent directory to persist the rename.
pub fn rename(from: &Path, to: &Path) -> io::Result<()> {
    fs::rename(from, to)?;
    let parent = to
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let pdir = open_dir(parent)?;
    pdir.sync_all()
}

/// Go `Replace`: moves a file or directory to a new location and deletes any previous data. Not atomic.
pub fn replace(from: &Path, to: &Path) -> io::Result<()> {
    // Remove destination only if it is a dir, otherwise leave it to rename as it replaces the destination file
    // and is atomic.
    if let Ok(md) = fs::metadata(to) {
        if md.is_dir() {
            fs::remove_dir_all(to)?;
        }
    }
    rename(from, to)
}

/// Go `Fdatasync`.
pub fn fdatasync(f: &File) -> io::Result<()> {
    f.sync_data()
}

/// Syncs the directory at `dir` (Go: `OpenDir` + `Sync` + `Close`).
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    open_dir(dir)?.sync_all()
}

// ---------------------------------------------------------------------------------------------------------------
// Locking.

/// Go `Releaser`: releases a file lock (also on drop).
#[derive(Debug)]
pub struct Releaser {
    f: Option<File>,
}

impl Releaser {
    /// Go `Release`: unlocks and closes the file.
    pub fn release(&mut self) -> io::Result<()> {
        if let Some(f) = self.f.take() {
            f.unlock()?;
        }
        Ok(())
    }
}

impl Drop for Releaser {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

/// Go `Flock`: locks the file `file_name` (exclusive, non-blocking `flock(2)` as Go does), creating it and its
/// directory when missing. Returns the releaser (or the error) and whether the file existed before.
pub fn flock(file_name: &Path) -> (io::Result<Releaser>, bool) {
    if let Some(dir) = file_name.parent().filter(|p| !p.as_os_str().is_empty()) {
        if let Err(e) = fs::create_dir_all(dir) {
            return (Err(e), false);
        }
    }
    let existed = fs::metadata(file_name).is_ok();
    let f = match OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(file_name)
    {
        Ok(f) => f,
        Err(e) => return (Err(e), existed),
    };
    let r = match f.try_lock() {
        Ok(()) => Ok(Releaser { f: Some(f) }),
        Err(fs::TryLockError::WouldBlock) => Err(io::Error::from_raw_os_error(libc::EWOULDBLOCK)),
        Err(fs::TryLockError::Error(e)) => Err(e),
    };
    (r, existed)
}

// ---------------------------------------------------------------------------------------------------------------
// Mmap.

/// Go `MmapFile`: a read-only shared mapping of a file.
#[derive(Debug)]
pub struct MmapFile {
    f: File,
    m: Option<memmap2::Mmap>,
}

impl MmapFile {
    /// Go `OpenMmapFile`.
    pub fn open(path: &Path) -> io::Result<MmapFile> {
        MmapFile::open_with_size(path, 0)
    }

    /// Go `OpenMmapFileWithSize`: maps the first `size` bytes, or the whole file when `size <= 0`.
    pub fn open_with_size(path: &Path, size: i64) -> io::Result<MmapFile> {
        let f = File::open(path).map_err(|e| wrap_io("try lock file", e))?;
        let size = if size <= 0 {
            f.metadata().map_err(|e| wrap_io("stat", e))?.len() as i64
        } else {
            size
        };
        let m = if size == 0 {
            // A zero length mapping is invalid; Go returns an error from mmap(2) (EINVAL).
            return Err(wrap_io(
                &format!("mmap, size {size}"),
                io::Error::from_raw_os_error(libc::EINVAL),
            ));
        } else {
            // SAFETY: the mapping is read-only and shared. Prometheus never truncates or rewrites the bytes of a
            // file while it is mapped for reading (segments are append-only; repair and truncation only happen
            // on files that are closed or not mapped), so the slice we hand out does not change under us.
            unsafe { memmap2::MmapOptions::new().len(size as usize).map(&f) }
                .map_err(|e| wrap_io(&format!("mmap, size {size}"), e))?
        };
        Ok(MmapFile { f, m: Some(m) })
    }

    /// Go `File()`.
    pub fn file(&self) -> &File {
        &self.f
    }

    /// Go `Bytes()`.
    pub fn bytes(&self) -> &[u8] {
        self.m.as_deref().unwrap_or(&[])
    }

    /// Go `Close()`: unmaps and closes (dropping does the same).
    pub fn close(self) -> io::Result<()> {
        Ok(())
    }
}

impl crate::encoding::ByteSlice for MmapFile {
    fn len(&self) -> usize {
        self.bytes().len()
    }
    fn range(&self, start: usize, end: usize) -> &[u8] {
        &self.bytes()[start..end]
    }
}

/// Wraps an I/O error with a Go style `"<msg>: <err>"` prefix, keeping its kind.
pub fn wrap_io(msg: &str, e: io::Error) -> io::Error {
    io::Error::new(e.kind(), format!("{msg}: {e}"))
}

// ---------------------------------------------------------------------------------------------------------------
// Preallocate.

/// Go `Preallocate`: tries to allocate space for the file. If the operation is unsupported, no error is
/// returned. With `extend_file` the file size changes; otherwise the size is kept (`FALLOC_FL_KEEP_SIZE`).
pub fn preallocate(f: &File, size_in_bytes: i64, extend_file: bool) -> io::Result<()> {
    if size_in_bytes == 0 {
        // fallocate will return EINVAL if length is 0; skip.
        return Ok(());
    }
    let mode = if extend_file {
        0
    } else {
        libc::FALLOC_FL_KEEP_SIZE
    };
    // SAFETY: fallocate on a valid, open file descriptor owned by `f`; no memory is passed.
    let r = unsafe { libc::fallocate(f.as_raw_fd(), mode, 0, size_in_bytes as libc::off_t) };
    if r == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    let errno = err.raw_os_error().unwrap_or(0);
    if extend_file {
        // Not supported or interrupted: fall back to truncate.
        if errno == libc::EOPNOTSUPP || errno == libc::EINTR {
            // Go `preallocExtendTrunc` compares against (current size + size_in_bytes), which is never smaller,
            // so it always truncates to size_in_bytes.
            return f.set_len(size_in_bytes as u64);
        }
        return Err(err);
    }
    if errno == libc::EOPNOTSUPP {
        return Ok(());
    }
    Err(err)
}
