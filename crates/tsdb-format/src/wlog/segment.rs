//! Segment files: naming, listing, opening, and the page-aligned buffered reader over many segments.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use super::{Error, PAGE_SIZE, Result};

/// Go `SegmentFile`: the file under a segment. `File` implements it; tests wrap it to inject faults.
pub trait SegmentFile: Read + Send {
    /// Go `io.Writer.Write`: writes all of `p` or returns how much was written together with the error.
    fn write_all_n(&mut self, p: &[u8]) -> (usize, io::Result<()>);
    /// Go `Sync`.
    fn sync(&mut self) -> io::Result<()>;
    /// Go `Stat().Size()`.
    fn size(&self) -> io::Result<i64>;
}

impl SegmentFile for File {
    fn write_all_n(&mut self, p: &[u8]) -> (usize, io::Result<()>) {
        let mut n = 0;
        while n < p.len() {
            match self.write(&p[n..]) {
                Ok(0) => {
                    return (
                        n,
                        Err(io::Error::new(io::ErrorKind::WriteZero, "short write")),
                    );
                }
                Ok(k) => n += k,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return (n, Err(e)),
            }
        }
        (n, Ok(()))
    }

    fn sync(&mut self) -> io::Result<()> {
        self.sync_all()
    }

    fn size(&self) -> io::Result<i64> {
        Ok(self.metadata()?.len() as i64)
    }
}

/// Go `Segment`: a segment file with its directory and index.
pub struct Segment {
    file: Box<dyn SegmentFile>,
    dir: PathBuf,
    i: i64,
}

impl Segment {
    /// Go `Index`.
    pub fn index(&self) -> i64 {
        self.i
    }

    /// Go `Dir`.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The underlying file.
    pub fn file_mut(&mut self) -> &mut dyn SegmentFile {
        &mut *self.file
    }

    /// Replaces the underlying file by `f(old)`. Tests use it to inject write failures.
    pub fn wrap_file(&mut self, f: impl FnOnce(Box<dyn SegmentFile>) -> Box<dyn SegmentFile>) {
        let old = std::mem::replace(&mut self.file, Box::new(NoFile));
        self.file = f(old);
    }

    pub(crate) fn write_all_n(&mut self, p: &[u8]) -> (usize, io::Result<()>) {
        self.file.write_all_n(p)
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync()
    }

    pub fn size(&self) -> io::Result<i64> {
        self.file.size()
    }

    /// Go `Close`. Dropping a segment closes it too.
    pub fn close(self) -> io::Result<()> {
        Ok(())
    }
}

impl Read for Segment {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

/// Placeholder file used only while `Segment::wrap_file` runs.
struct NoFile;

impl Read for NoFile {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Ok(0)
    }
}

impl SegmentFile for NoFile {
    fn write_all_n(&mut self, _: &[u8]) -> (usize, io::Result<()>) {
        (0, Err(io::Error::other("no file")))
    }
    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn size(&self) -> io::Result<i64> {
        Ok(0)
    }
}

/// Go `SegmentName`: builds a segment name for the directory.
pub fn segment_name(dir: &Path, i: i64) -> PathBuf {
    dir.join(format!("{i:08}"))
}

/// Go `OpenWriteSegment`: opens segment `k` in `dir`, ready for new appends. A torn last page is filled with
/// zeros.
pub fn open_write_segment(logger: &crate::record::Logger, dir: &Path, k: i64) -> Result<Segment> {
    let seg_name = segment_name(dir, k);
    let mut f = OpenOptions::new().append(true).open(&seg_name)?;
    let size = f.metadata()?.len() as usize;
    // If the last page is torn, fill it with zeros.
    // In case it was torn after all records were written successfully, this
    // will just pad the page and everything will be fine.
    // If it was torn mid-record, a full read (which the caller should do anyway
    // to ensure integrity) will detect it as a corruption by the end.
    let d = size % PAGE_SIZE;
    if d != 0 {
        logger.warn(
            "Last page of the wlog is torn, filling it with zeros",
            &[("segment", &seg_name.display())],
        );
        let (_, r) = f.write_all_n(&vec![0u8; PAGE_SIZE - d]);
        if let Err(e) = r {
            return Err(Error::from(e).wrap("zero-pad torn page"));
        }
    }
    Ok(Segment {
        file: Box::new(f),
        i: k,
        dir: dir.to_path_buf(),
    })
}

/// Go `CreateSegment`: creates a new segment `k` in `dir`.
pub fn create_segment(dir: &Path, k: i64) -> Result<Segment> {
    let f = OpenOptions::new()
        .append(true)
        .create(true)
        .open(segment_name(dir, k))?;
    Ok(Segment {
        file: Box::new(f),
        i: k,
        dir: dir.to_path_buf(),
    })
}

/// Go `OpenReadSegment`: opens the segment with the given file name.
pub fn open_read_segment(fn_: &Path) -> Result<Segment> {
    let k = fn_
        .file_name()
        .and_then(|s| s.to_str())
        .and_then(go_atoi)
        .ok_or_else(|| Error::msg("not a valid filename"))?;
    let f = File::open(fn_)?;
    let dir = fn_
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    Ok(Segment {
        file: Box::new(f),
        i: k,
        dir,
    })
}

/// Go `strconv.Atoi`: optional sign, then ASCII digits only.
pub(crate) fn go_atoi(s: &str) -> Option<i64> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// Go `segmentRef`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentRef {
    pub name: String,
    pub index: i64,
}

/// Go `listSegments`: the segment files of `dir` sorted by index; an error if they are not sequential.
pub fn list_segments(dir: &Path) -> Result<Vec<SegmentRef>> {
    let mut refs = Vec::new();
    for e in fs::read_dir(dir)? {
        let e = e?;
        let name = e.file_name();
        let Some(fn_) = name.to_str() else { continue };
        let Some(k) = go_atoi(fn_) else { continue };
        refs.push(SegmentRef {
            name: fn_.to_string(),
            index: k,
        });
    }
    refs.sort_by_key(|r| r.index);
    for w in refs.windows(2) {
        if w[0].index + 1 != w[1].index {
            return Err(Error::msg("segments are not sequential"));
        }
    }
    Ok(refs)
}

/// Go `Segments`: the range `[first, last]` of currently existing segments; `(-1, -1)` when there are none.
pub fn segments(wl_dir: &Path) -> Result<(i64, i64)> {
    let refs = list_segments(wl_dir)?;
    match (refs.first(), refs.last()) {
        (Some(f), Some(l)) => Ok((f.index, l.index)),
        _ => Ok((-1, -1)),
    }
}

/// Go `SegmentRange`: segments of a directory from `first` to `last`; `-1` leaves that end open.
#[derive(Clone, Debug)]
pub struct SegmentRange {
    pub dir: PathBuf,
    pub first: i64,
    pub last: i64,
}

/// Go `NewSegmentsReader`: a reader over all segments in the directory.
pub fn new_segments_reader(dir: &Path) -> Result<SegmentBufReader> {
    new_segments_range_reader(&[SegmentRange {
        dir: dir.to_path_buf(),
        first: -1,
        last: -1,
    }])
}

/// Go `NewSegmentsRangeReader`: a reader over the given WAL segment ranges.
pub fn new_segments_range_reader(sr: &[SegmentRange]) -> Result<SegmentBufReader> {
    let mut segs = Vec::new();
    for r in sr {
        let refs = list_segments(&r.dir)
            .map_err(|e| e.wrap(&format!("list segment in dir:{}", r.dir.display())))?;
        for s in refs {
            if r.first >= 0 && s.index < r.first {
                continue;
            }
            if r.last >= 0 && s.index > r.last {
                break;
            }
            let seg = open_read_segment(&r.dir.join(&s.name)).map_err(|e| {
                e.wrap(&format!(
                    "open segment:{} in dir:{}",
                    s.name,
                    r.dir.display()
                ))
            })?;
            segs.push(seg);
        }
    }
    Ok(new_segment_buf_reader(segs))
}

/// Go `segmentBufReader`: a buffered reader that reads in multiples of pages. It tracks segment and offset
/// for corruption reporting and pads the end of segments that are not page aligned with zeros.
pub struct SegmentBufReader {
    buf: Box<[u8]>,
    r: usize,
    w: usize,
    segs: Vec<Segment>,
    cur: usize,
    off: usize,
}

/// Go `NewSegmentBufReader`.
pub fn new_segment_buf_reader(segs: Vec<Segment>) -> SegmentBufReader {
    let buf = if segs.is_empty() {
        Box::default()
    } else {
        vec![0u8; 16 * PAGE_SIZE].into_boxed_slice()
    };
    SegmentBufReader {
        buf,
        r: 0,
        w: 0,
        segs,
        cur: 0,
        off: 0,
    }
}

/// Go `NewSegmentBufReaderWithOffset`: skips the first `offset` bytes of the first segment.
pub fn new_segment_buf_reader_with_offset(
    offset: usize,
    segs: Vec<Segment>,
) -> Result<SegmentBufReader> {
    let mut sbr = new_segment_buf_reader(segs);
    if offset == 0 || sbr.segs.is_empty() {
        return Ok(sbr);
    }
    // Go bufio.Reader.Discard: skips in the buffered first segment without counting into `off`.
    let mut left = offset;
    while left > 0 {
        if sbr.r == sbr.w && !sbr.fill()? {
            return Err(Error::Eof);
        }
        let k = left.min(sbr.w - sbr.r);
        sbr.r += k;
        left -= k;
    }
    Ok(sbr)
}

impl SegmentBufReader {
    /// The segment being read (Go `segs[cur]`), if any.
    pub fn current_segment(&self) -> Option<&Segment> {
        self.segs.get(self.cur)
    }

    /// Offset of read data into the current segment.
    pub fn offset(&self) -> usize {
        self.off
    }

    /// Go `Close`: closes all segments.
    pub fn close(self) -> io::Result<()> {
        Ok(())
    }

    /// Refills the buffer from the current segment; false at its end.
    fn fill(&mut self) -> io::Result<bool> {
        self.r = 0;
        self.w = 0;
        loop {
            match self.segs[self.cur].read(&mut self.buf) {
                Ok(n) => {
                    self.w = n;
                    return Ok(n > 0);
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
    }
}

impl Read for SegmentBufReader {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        if self.segs.is_empty() || b.is_empty() {
            return Ok(0);
        }
        loop {
            if self.r < self.w || self.fill()? {
                let n = b.len().min(self.w - self.r);
                b[..n].copy_from_slice(&self.buf[self.r..self.r + n]);
                self.r += n;
                self.off += n;
                return Ok(n);
            }

            // We hit EOF; fake out zero padding at the end of short segments, so we
            // don't increment curr too early and report the wrong segment as corrupt.
            if !self.off.is_multiple_of(PAGE_SIZE) {
                let i = b.len().min(PAGE_SIZE - self.off % PAGE_SIZE);
                b[..i].fill(0);
                self.off += i;
                return Ok(i);
            }

            // There is no more data left in the curr segment and there are no more
            // segments left. Return EOF.
            if self.cur + 1 >= self.segs.len() {
                return Ok(0);
            }

            // Move to next segment. Go returns (0, nil) here and lets the caller read again.
            self.cur += 1;
            self.off = 0;
            self.r = 0;
            self.w = 0;
        }
    }
}
