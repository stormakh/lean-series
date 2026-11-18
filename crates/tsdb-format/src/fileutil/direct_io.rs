//! Port of `tsdb/fileutil/direct_io*.go` (Linux): `BufWriter`, a plain buffered writer and the Direct IO writer
//! that keeps all alignment restrictions.

use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;

/// The defaults are deliberately set higher to cover most setups. On Linux >= 6.1 statx(2) gives the exact
/// alignment restrictions.
const DEFAULT_ALIGNMENT: usize = 4096;
const DEFAULT_BUF_SIZE: usize = 4096;

/// Go `errWriterInvalid`.
pub const ERR_WRITER_INVALID: &str = "the last flush resulted in an unaligned offset, the writer can no longer ensure contiguous writes";
/// Go `errDirectIOUnsupported`.
pub const ERR_DIRECT_IO_UNSUPPORTED: &str = "direct IO is unsupported";
/// Go `errStatxNotSupported` (kept for reference; the Rust code uses `StatxErr::NotSupported`).
#[allow(dead_code)]
const ERR_STATX_NOT_SUPPORTED: &str =
    "the statx syscall with STATX_DIOALIGN is not supported. At least Linux kernel 6.1 is needed";

/// Go `BufWriter`: a buffered writer that can be reset to a new file.
pub trait BufWriter: Send {
    fn write(&mut self, p: &[u8]) -> io::Result<usize>;
    fn flush(&mut self) -> io::Result<()>;
    fn reset(&mut self, f: File) -> io::Result<()>;
    /// The file being written, if any.
    fn file(&self) -> Option<&File>;
    /// Gives back the file (after a flush by the caller).
    fn take_file(&mut self) -> Option<File>;
}

/// Go `writer`: a plain buffered writer (used when Direct IO is not enabled).
pub struct BufioWriter {
    buf: Vec<u8>,
    size: usize,
    f: Option<File>,
}

/// Go `NewBufioWriterWithSize`.
pub fn new_bufio_writer_with_size(f: File, size: usize) -> io::Result<Box<dyn BufWriter>> {
    let size = if size == 0 { DEFAULT_BUF_SIZE } else { size };
    Ok(Box::new(BufioWriter {
        buf: Vec::with_capacity(size),
        size,
        f: Some(f),
    }))
}

impl BufWriter for BufioWriter {
    fn write(&mut self, p: &[u8]) -> io::Result<usize> {
        if self.buf.len() + p.len() > self.size {
            self.flush()?;
        }
        if p.len() >= self.size {
            let f = self.f.as_mut().ok_or_else(|| io::Error::other("no file"))?;
            f.write_all(p)?;
        } else {
            self.buf.extend_from_slice(p);
        }
        Ok(p.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let f = self.f.as_mut().ok_or_else(|| io::Error::other("no file"))?;
        f.write_all(&self.buf)?;
        self.buf.clear();
        Ok(())
    }
    fn reset(&mut self, f: File) -> io::Result<()> {
        self.buf.clear();
        self.f = Some(f);
        Ok(())
    }
    fn file(&self) -> Option<&File> {
        self.f.as_ref()
    }
    fn take_file(&mut self) -> Option<File> {
        self.f.take()
    }
}

/// Go `NewDirectIOWriter`.
pub fn new_direct_io_writer(f: File, size: usize) -> io::Result<Box<dyn BufWriter>> {
    Ok(Box::new(DirectIoWriter::new(f, size)?))
}

/// Go `UncachedIOSupported`.
pub fn uncached_io_supported() -> bool {
    true
}

/// Go `directIORqmts`: alignment requirements for direct I/O, in bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectIoRqmts {
    /// The required alignment for memory buffer addresses.
    pub memory_align: usize,
    /// The required alignment for I/O segment lengths and file offsets.
    pub offset_align: usize,
}

fn default_direct_io_rqmts() -> DirectIoRqmts {
    DirectIoRqmts {
        memory_align: DEFAULT_ALIGNMENT,
        offset_align: DEFAULT_ALIGNMENT,
    }
}

/// An aligned heap block: `len` bytes starting at an address aligned to `memory_align`.
struct AlignedBlock {
    raw: Vec<u8>,
    start: usize,
    len: usize,
}

impl AlignedBlock {
    fn as_slice(&self) -> &[u8] {
        &self.raw[self.start..self.start + self.len]
    }
    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.raw[self.start..self.start + self.len]
    }
}

/// Go `directIOWriter`: a buffered writer for Direct IO that keeps all alignment restrictions. It handles files
/// whose initial offset is not aligned. If an explicit flush leaves an unaligned offset, the writer becomes
/// invalid, as it can no longer support contiguous writes.
pub struct DirectIoWriter {
    buf: AlignedBlock,
    n: usize,
    f: Option<File>,
    /// The number of bytes needed to reach the nearest aligned file offset (Direct IO is possible after that).
    offset_alignment_gap: usize,
    alignment_rqmts: DirectIoRqmts,
    err: Option<io::Error>,
    /// Public for tests (Go reads `w.invalid`).
    pub invalid: bool,
}

fn join(a: Option<io::Error>, b: Option<io::Error>) -> Option<io::Error> {
    match (a, b) {
        (None, b) => b,
        (a, None) => a,
        (Some(a), Some(b)) => Some(io::Error::new(a.kind(), format!("{a}\n{b}"))),
    }
}

fn clone_err(e: &io::Error) -> io::Error {
    io::Error::new(e.kind(), e.to_string())
}

impl DirectIoWriter {
    /// Go `newDirectIOWriter`.
    pub fn new(f: File, size: usize) -> io::Result<DirectIoWriter> {
        let rq = file_direct_io_rqmts(&f)?;
        let size = if size == 0 { DEFAULT_BUF_SIZE } else { size };
        if size % rq.offset_align != 0 {
            return Err(io::Error::other(format!(
                "size {size} should be a multiple of {}",
                rq.offset_align
            )));
        }
        let mut f = f;
        let gap = check_initial_unaligned_offset(&mut f, &rq)?;
        Ok(DirectIoWriter {
            buf: aligned_block_raw(size, &rq),
            n: 0,
            f: Some(f),
            offset_alignment_gap: gap,
            alignment_rqmts: rq,
            err: None,
            invalid: false,
        })
    }

    fn available(&self) -> usize {
        self.buf.len - self.n
    }

    fn buffered(&self) -> usize {
        self.n
    }

    /// Writes bytes from the buffer without Direct IO to fill the offset gap, then enables Direct IO.
    fn fill_initial_offset_gap(&mut self) {
        if self.n == 0 || self.offset_alignment_gap == 0 {
            return;
        }
        let bytes_to_align = self.n.min(self.offset_alignment_gap);
        let (n, mut err) = {
            let data = &self.buf.as_slice()[..bytes_to_align];
            let f = self.f.as_mut().expect("file");
            match f.write(data) {
                Ok(n) if n < bytes_to_align => (n, Some(io::Error::from(io::ErrorKind::WriteZero))),
                Ok(n) => (n, None),
                Err(e) => (0, Some(e)),
            }
        };
        if n > 0 {
            let cnt = self.n;
            self.buf.as_mut_slice().copy_within(n..cnt, 0);
            self.n -= n;
        }
        // If the file offset was aligned, enable Direct IO.
        self.offset_alignment_gap -= n;
        if self.offset_alignment_gap == 0 {
            let fd = self.f.as_ref().expect("file").as_raw_fd();
            err = join(err, enable_direct_io(fd).err());
        }
        self.err = join(self.err.take(), err);
    }

    /// Writes `p` (whose last `padding` bytes are zero padding) with Direct IO; returns the relevant bytes
    /// written.
    fn direct_io_write(
        f: &mut File,
        rq: &DirectIoRqmts,
        p: &[u8],
        padding: usize,
        invalid: &mut bool,
        err_acc: &mut Option<io::Error>,
    ) -> (usize, Option<io::Error>) {
        let mut relevant = p.len() - padding;
        let (n, mut err) = match f.write(p) {
            Ok(n) => (n, None),
            Err(e) => (0, Some(e)),
        };
        if n < relevant {
            relevant = n;
            if err.is_none() {
                err = Some(io::Error::from(io::ErrorKind::WriteZero));
            }
        } else if n > relevant {
            // Adjust the offset to discard the padding that was written.
            let written_padding = (n - relevant) as i64;
            if let Err(e) = f.seek(SeekFrom::Current(-written_padding)) {
                *err_acc = join(
                    err_acc.take(),
                    Some(io::Error::new(
                        e.kind(),
                        format!("seek to discard written padding {written_padding}: {e}"),
                    )),
                );
            }
        }
        if relevant % rq.offset_align != 0 {
            *invalid = true;
        }
        (relevant, err)
    }

    /// True when all Direct IO alignment restrictions are met for writing `p`.
    fn can_direct_io_write(&self, p: &[u8]) -> bool {
        is_aligned(p, &self.alignment_rqmts) && self.offset_alignment_gap == 0
    }

    /// Go `Write`.
    pub fn write(&mut self, mut p: &[u8]) -> io::Result<usize> {
        if self.invalid {
            return Err(io::Error::other(ERR_WRITER_INVALID));
        }
        let mut nn = 0;
        while p.len() > self.available() && self.err.is_none() {
            let n1;
            let mut n2 = 0;
            if self.buffered() == 0 && self.can_direct_io_write(p) {
                // Large write, empty buffer: write from p via Direct IO as the block and the offset are aligned.
                let rq = self.alignment_rqmts;
                let f = self.f.as_mut().expect("file");
                let mut acc = self.err.take();
                let (n, e) = Self::direct_io_write(f, &rq, p, 0, &mut self.invalid, &mut acc);
                n1 = n;
                // Go assigns: b.err = <result err> (the seek error accumulated in b.err is overwritten).
                let _ = acc;
                self.err = e;
            } else {
                let start = self.n;
                let avail = self.available();
                n1 = avail.min(p.len());
                self.buf.as_mut_slice()[start..start + n1].copy_from_slice(&p[..n1]);
                self.n += n1;
                if self.offset_alignment_gap != 0 {
                    self.fill_initial_offset_gap();
                    // Refill the buffer.
                    let start = self.n;
                    let rest = &p[n1..];
                    n2 = self.available().min(rest.len());
                    self.buf.as_mut_slice()[start..start + n2].copy_from_slice(&rest[..n2]);
                    self.n += n2;
                }
                if self.available() == 0 {
                    // Avoid flushing in case the second refill wasn't complete.
                    let e = self.flush_inner().err();
                    self.err = join(self.err.take(), e);
                }
            }
            nn += n1 + n2;
            p = &p[n1 + n2..];
        }
        if let Some(e) = &self.err {
            return Err(clone_err(e));
        }
        let start = self.n;
        let n = self.available().min(p.len());
        self.buf.as_mut_slice()[start..start + n].copy_from_slice(&p[..n]);
        self.n += n;
        nn += n;
        Ok(nn)
    }

    fn flush_inner(&mut self) -> io::Result<()> {
        if self.invalid {
            return Err(io::Error::other(ERR_WRITER_INVALID));
        }
        if let Some(e) = &self.err {
            return Err(clone_err(e));
        }
        if self.n == 0 {
            return Ok(());
        }
        // Ensure the segment length alignment restriction is met: round up to a multiple of offset_align and
        // add zero padding.
        let align = self.alignment_rqmts.offset_align;
        let mut u_offset = self.n;
        if u_offset % align != 0 {
            u_offset = (u_offset / align + 1) * align;
            let n = self.n;
            self.buf.as_mut_slice()[n..u_offset].fill(0);
        }
        let rq = self.alignment_rqmts;
        let pad = u_offset - self.n;
        let mut acc = self.err.take();
        let (n, e) = {
            let f = self.f.as_mut().expect("file");
            let data = &self.buf.raw[self.buf.start..self.buf.start + u_offset];
            Self::direct_io_write(f, &rq, data, pad, &mut self.invalid, &mut acc)
        };
        self.err = acc;
        if let Some(e) = e {
            if n > 0 && n < self.n {
                let cnt = self.n;
                self.buf.as_mut_slice().copy_within(n..cnt, 0);
            }
            self.n -= n;
            let ret = clone_err(&e);
            self.err = join(self.err.take(), Some(e));
            return Err(ret);
        }
        self.n = 0;
        Ok(())
    }

    /// Go `Flush`.
    pub fn flush(&mut self) -> io::Result<()> {
        if self.offset_alignment_gap != 0 {
            self.fill_initial_offset_gap();
            if let Some(e) = &self.err {
                return Err(clone_err(e));
            }
        }
        self.flush_inner()
    }

    /// Go `Reset`.
    pub fn reset(&mut self, mut f: File) -> io::Result<()> {
        let rq = file_direct_io_rqmts(&f)?;
        self.alignment_rqmts = rq;
        if self.buf.len == 0 {
            self.buf = aligned_block_raw(DEFAULT_BUF_SIZE, &rq);
        }
        let gap = check_initial_unaligned_offset(&mut f, &rq)?;
        self.offset_alignment_gap = gap;
        self.err = None;
        self.invalid = false;
        self.n = 0;
        self.f = Some(f);
        Ok(())
    }

    /// The file being written.
    pub fn file(&self) -> Option<&File> {
        self.f.as_ref()
    }
}

impl BufWriter for DirectIoWriter {
    fn write(&mut self, p: &[u8]) -> io::Result<usize> {
        DirectIoWriter::write(self, p)
    }
    fn flush(&mut self) -> io::Result<()> {
        DirectIoWriter::flush(self)
    }
    fn reset(&mut self, f: File) -> io::Result<()> {
        DirectIoWriter::reset(self, f)
    }
    fn file(&self) -> Option<&File> {
        self.f.as_ref()
    }
    fn take_file(&mut self) -> Option<File> {
        self.f.take()
    }
}

/// Go `fileDirectIORqmts`: alignment requirements via statx, with defaults when statx does not support them.
pub fn file_direct_io_rqmts(f: &File) -> io::Result<DirectIoRqmts> {
    let rq = match fetch_direct_io_rqmts_from_statx(f.as_raw_fd()) {
        Ok(rq) => rq,
        Err(StatxErr::NotSupported) => default_direct_io_rqmts(),
        Err(StatxErr::Other(e)) => return Err(e),
    };
    if rq.memory_align == 0 || rq.offset_align == 0 {
        return Err(io::Error::other(format!(
            "zero alignment requirement is not supported &{{memoryAlign:{} offsetAlign:{}}}",
            rq.memory_align, rq.offset_align
        )));
    }
    Ok(rq)
}

fn compute_alignment_offset(block: &[u8], alignment: usize) -> usize {
    if alignment == 0 {
        return 0;
    }
    if block.is_empty() {
        panic!("empty block not supported");
    }
    (block.as_ptr() as usize) & (alignment - 1)
}

/// Go `isAligned`: the length of the block is a multiple of offset_align and its address is aligned to
/// memory_align.
pub fn is_aligned(block: &[u8], rq: &DirectIoRqmts) -> bool {
    compute_alignment_offset(block, rq.memory_align) == 0 && block.len() % rq.offset_align == 0
}

fn aligned_block_raw(size: usize, rq: &DirectIoRqmts) -> AlignedBlock {
    if size == 0 || size % rq.offset_align != 0 {
        panic!(
            "size {size} should be > 0 and a multiple of offsetAlign={}",
            rq.offset_align
        );
    }
    if rq.memory_align == 0 {
        return AlignedBlock {
            raw: vec![0; size],
            start: 0,
            len: size,
        };
    }
    let raw = vec![0u8; size + rq.memory_align];
    let a = compute_alignment_offset(&raw, rq.memory_align);
    let start = if a == 0 { 0 } else { rq.memory_align - a };
    let b = AlignedBlock {
        raw,
        start,
        len: size,
    };
    if !is_aligned(b.as_slice(), rq) {
        // Assuming this to be rare, if not impossible.
        panic!("cannot create an aligned block");
    }
    b
}

/// Go `alignedBlock`: a zeroed block of `size` bytes whose address is aligned to `memory_align`. The returned
/// vector keeps the block at `[offset..offset + size]`; the second value is that offset.
pub fn aligned_block(size: usize, rq: &DirectIoRqmts) -> (Vec<u8>, usize) {
    let b = aligned_block_raw(size, rq);
    (b.raw, b.start)
}

/// Go `currentFileOffset`.
pub fn current_file_offset(f: &mut File) -> io::Result<usize> {
    f.stream_position()
        .map(|o| o as usize)
        .map_err(|e| io::Error::new(e.kind(), format!("cannot get the current offset: {e}")))
}

fn file_status_flags(fd: i32) -> io::Result<i32> {
    // SAFETY: fcntl(F_GETFL) on a file descriptor; no memory is passed.
    let r = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };
    if r < 0 {
        let e = io::Error::last_os_error();
        return Err(io::Error::new(
            e.kind(),
            format!("cannot get file status flags: {e}"),
        ));
    }
    Ok(r)
}

/// Go `enableDirectIO`: sets O_DIRECT on the file descriptor if needed.
pub fn enable_direct_io(fd: i32) -> io::Result<()> {
    let flag = file_status_flags(fd)?;
    if flag & libc::O_DIRECT == libc::O_DIRECT {
        return Ok(());
    }
    // SAFETY: fcntl(F_SETFL) on a file descriptor; no memory is passed.
    let r = unsafe { libc::fcntl(fd, libc::F_SETFL, flag | libc::O_DIRECT) };
    if r < 0 {
        let e = io::Error::last_os_error();
        return Err(io::Error::new(
            e.kind(),
            format!("cannot enable Direct IO: {e}"),
        ));
    }
    Ok(())
}

/// Go `checkInitialUnalignedOffset`: the gap between the current offset and the nearest aligned offset. If the
/// current offset is aligned, Direct IO is enabled on the file.
fn check_initial_unaligned_offset(f: &mut File, rq: &DirectIoRqmts) -> io::Result<usize> {
    let offset = current_file_offset(f)?;
    let alignment = rq.offset_align;
    let gap = (alignment - offset % alignment) % alignment;
    if gap == 0 {
        enable_direct_io(f.as_raw_fd())?;
    }
    Ok(gap)
}

enum StatxErr {
    NotSupported,
    Other(io::Error),
}

fn fetch_direct_io_rqmts_from_statx(fd: i32) -> Result<DirectIoRqmts, StatxErr> {
    // SAFETY: statx is plain old data; zeroed is a valid value.
    let mut st: libc::statx = unsafe { std::mem::zeroed() };
    let flags = libc::AT_SYMLINK_NOFOLLOW | libc::AT_EMPTY_PATH | libc::AT_STATX_DONT_SYNC;
    // SAFETY: the path is a valid NUL-terminated empty string and `st` is a valid, writable statx buffer.
    let r = unsafe { libc::statx(fd, c"".as_ptr(), flags, libc::STATX_DIOALIGN, &mut st) };
    if r != 0 {
        let e = io::Error::last_os_error();
        if e.raw_os_error() == Some(libc::ENOSYS) {
            return Err(StatxErr::NotSupported);
        }
        return Err(StatxErr::Other(io::Error::new(
            e.kind(),
            format!("statx failed on fd {fd}: {e}"),
        )));
    }
    if st.stx_mask & libc::STATX_DIOALIGN == 0 {
        return Err(StatxErr::NotSupported);
    }
    if st.stx_dio_mem_align == 0 || st.stx_dio_offset_align == 0 {
        return Err(StatxErr::Other(io::Error::other(format!(
            "{ERR_DIRECT_IO_UNSUPPORTED}: kernel may be old or the file may be on an unsupported FS"
        ))));
    }
    Ok(DirectIoRqmts {
        memory_align: st.stx_dio_mem_align as usize,
        offset_align: st.stx_dio_offset_align as usize,
    })
}
