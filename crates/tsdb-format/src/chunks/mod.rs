//! Port of `tsdb/chunks`: chunk references, `Meta`, block chunk segment files (Writer/Reader), head chunk files
//! (`chunks_head`, ChunkDiskMapper), the chunk write queue and its queue.

mod block;
mod chunk_write_queue;
mod head_chunks;
mod queue;
mod samples;

pub use block::*;
pub use chunk_write_queue::*;
pub use head_chunks::*;
pub use queue::*;
pub use samples::*;

use chunkenc::Chunk;
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

// Segment header fields constants.

/// Go `MagicChunks`: 4 bytes at the head of a series file.
pub const MAGIC_CHUNKS: u32 = 0x85BD40DD;
/// Go `MagicChunksSize`: the size in bytes of MagicChunks.
pub const MAGIC_CHUNKS_SIZE: usize = 4;
/// Go `chunksFormatV1`.
pub const CHUNKS_FORMAT_V1: u8 = 1;
/// Go `ChunksFormatVersionSize`.
pub const CHUNKS_FORMAT_VERSION_SIZE: usize = 1;
const SEGMENT_HEADER_PADDING_SIZE: usize = 3;
/// Go `SegmentHeaderSize`: the total size of the header part.
pub const SEGMENT_HEADER_SIZE: usize =
    MAGIC_CHUNKS_SIZE + CHUNKS_FORMAT_VERSION_SIZE + SEGMENT_HEADER_PADDING_SIZE;

// Chunk fields constants.

/// Go `MaxChunkLengthFieldSize` (`binary.MaxVarintLen32`): the maximum size of the data length part.
pub const MAX_CHUNK_LENGTH_FIELD_SIZE: usize = 5;
/// Go `ChunkEncodingSize`: the size of the chunk encoding part.
pub const CHUNK_ENCODING_SIZE: usize = 1;
/// Go `crc32.Size`.
const CRC32_SIZE: usize = 4;

/// Go `ChunkRef`: a generic reference for reading chunk data (a `HeadChunkRef` or a `BlockChunkRef`).
pub type ChunkRef = u64;

/// Go `HeadSeriesRef`: refers to an in-memory series.
pub type HeadSeriesRef = u64;

/// Go `HeadChunkID`: refers to a specific chunk in a series (memSeries) in the Head.
pub type HeadChunkId = u64;

/// Go `HeadChunkRef`: packs a HeadSeriesRef (5 bytes) and a HeadChunkID (3 bytes) into a global 8 byte ID.
pub type HeadChunkRef = u64;

/// Go `NewHeadChunkRef`.
pub fn new_head_chunk_ref(hsr: HeadSeriesRef, chunk_id: HeadChunkId) -> HeadChunkRef {
    if hsr > (1 << 40) - 1 {
        panic!("series ID exceeds 5 bytes");
    }
    if chunk_id > (1 << 24) - 1 {
        panic!("chunk ID exceeds 3 bytes");
    }
    (hsr << 24) | chunk_id
}

/// Go `HeadChunkRef.Unpack`.
pub fn unpack_head_chunk_ref(p: HeadChunkRef) -> (HeadSeriesRef, HeadChunkId) {
    (p >> 24, (p << 40) >> 40)
}

/// Go `BlockChunkRef`: the upper 4 bytes are the segment index, the lower 4 bytes the offset in the segment.
pub type BlockChunkRef = u64;

/// Go `NewBlockChunkRef`.
pub fn new_block_chunk_ref(file_index: u64, file_offset: u64) -> BlockChunkRef {
    (file_index << 32) | file_offset
}

/// Go `BlockChunkRef.Unpack`: (segment index, chunk start).
pub fn unpack_block_chunk_ref(b: BlockChunkRef) -> (usize, usize) {
    ((b >> 32) as usize, ((b << 32) >> 32) as usize)
}

/// Go `Meta`: information about one or more chunks (shared with the storage crate).
pub use storage::chunks::Meta;

/// Go `chunks.Iterator`: iterates over the chunks of a single time series.
pub trait Iterator {
    /// The current meta. It depends on the implementation whether the chunk is populated or not.
    fn at(&self) -> &Meta;
    /// Advances the iterator by one.
    fn next(&mut self) -> bool;
    /// An optional error if `next` returned false.
    fn err(&self) -> Option<&Error>;
}

// ---------------------------------------------------------------------------------------------------------------
// Errors.

/// Errors of the chunks package. `Display` prints Go's error text.
#[derive(Debug)]
pub enum Error {
    /// Go `ErrChunkDiskMapperClosed`.
    Closed,
    /// Go `*CorruptionErr`.
    Corruption(Box<CorruptionErr>),
    /// An I/O error (Go `*os.PathError` and friends; the OS part of the text is Rust's).
    Io(io::Error),
    /// A chunk encoding error.
    Chunk(chunkenc::Error),
    /// Any other error text (Go `fmt.Errorf`/`errors.New`/`errors.Join`).
    Msg(String),
}

impl Error {
    pub fn msg(s: impl Into<String>) -> Error {
        Error::Msg(s.into())
    }

    /// Go `errors.As(err, &*CorruptionErr)`.
    pub fn as_corruption(&self) -> Option<&CorruptionErr> {
        match self {
            Error::Corruption(c) => Some(c),
            _ => None,
        }
    }

    /// Go `fmt.Errorf("<msg>: %w", err)`. I/O errors keep their kind.
    pub(crate) fn wrap(self, msg: &str) -> Error {
        match self {
            Error::Io(e) => Error::Io(crate::fileutil::wrap_io(msg, e)),
            e => Error::Msg(format!("{msg}: {e}")),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Closed => f.write_str("ChunkDiskMapper closed"),
            Error::Corruption(c) => fmt::Display::fmt(c, f),
            Error::Io(e) => fmt::Display::fmt(e, f),
            Error::Chunk(e) => fmt::Display::fmt(e, f),
            Error::Msg(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::Io(e)
    }
}

impl From<chunkenc::Error> for Error {
    fn from(e: chunkenc::Error) -> Error {
        Error::Chunk(e)
    }
}

impl From<CorruptionErr> for Error {
    fn from(e: CorruptionErr) -> Error {
        Error::Corruption(Box::new(e))
    }
}

/// Go `CorruptionErr`: returned when corruption is encountered in a head chunk file.
#[derive(Debug)]
pub struct CorruptionErr {
    pub dir: PathBuf,
    pub file_index: i64,
    pub err: Error,
}

impl fmt::Display for CorruptionErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "corruption in head chunk file {}: {}",
            segment_file(&self.dir, self.file_index).display(),
            self.err
        )
    }
}

/// Go `errors.Join`: `None` when all are `None`, the error itself when only one is set, else the texts joined
/// with newlines.
pub(crate) fn join_errors(errs: Vec<Option<Error>>) -> Option<Error> {
    let mut errs: Vec<Error> = errs.into_iter().flatten().collect();
    match errs.len() {
        0 => None,
        1 => errs.pop(),
        _ => Some(Error::Msg(
            errs.iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("\n"),
        )),
    }
}

pub(crate) fn err_invalid_size() -> Error {
    Error::msg("invalid size")
}

// ---------------------------------------------------------------------------------------------------------------
// CRC32 and segment files.

/// Go `checkCRC32`: checks that the CRC of `data` matches the big endian `sum`.
pub(crate) fn check_crc32(data: &[u8], sum: &[u8]) -> Result<(), Error> {
    let got = crate::encoding::crc32c(data);
    let want = u32::from_be_bytes([sum[0], sum[1], sum[2], sum[3]]);
    if got != want {
        return Err(Error::Msg(format!(
            "checksum mismatch expected:{want:x}, actual:{got:x}"
        )));
    }
    Ok(())
}

/// Go `segmentFile`: `<dir>/<index as %0.6d>` (for example `000001`, and `-000001` for -1).
pub fn segment_file(base_dir: &Path, index: i64) -> PathBuf {
    let name = if index < 0 {
        format!("-{:06}", index.unsigned_abs())
    } else {
        format!("{index:06}")
    };
    base_dir.join(name)
}

/// Go `strconv.ParseUint(name, 10, 64)`: the file names that count as segment files.
fn parse_seq(name: &std::ffi::OsStr) -> Option<u64> {
    let s = name.to_str()?;
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse::<u64>().ok()
}

/// The segment files of `dir` as (sequence, path), sorted by file name (Go `os.ReadDir` order).
fn read_seq_dir(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut names: Vec<std::ffi::OsString> = std::fs::read_dir(dir)?
        .map(|e| e.map(|e| e.file_name()))
        .collect::<io::Result<_>>()?;
    names.sort();
    Ok(names
        .into_iter()
        .filter_map(|n| parse_seq(&n).map(|s| (s, dir.join(n))))
        .collect())
}

/// Go `nextSequenceFile`: the path and number of the segment after the highest one in `dir`.
fn next_sequence_file(dir: &Path) -> io::Result<(PathBuf, u64)> {
    let i = read_seq_dir(dir)?
        .iter()
        .map(|(s, _)| *s)
        .max()
        .unwrap_or(0);
    Ok((segment_file(dir, (i + 1) as i64), i + 1))
}

/// Go `sequenceFiles`: the segment file paths of `dir` in name order.
fn sequence_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    Ok(read_seq_dir(dir)?.into_iter().map(|(_, p)| p).collect())
}

/// Go `cutSegmentFile`: creates the next segment file in `dir` and writes its header. The file content is not
/// synced. Returns (header size, file, path, sequence).
fn cut_segment_file(
    dir: &Path,
    dir_file: &File,
    magic_number: u32,
    chunks_format: u8,
    alloc_size: i64,
) -> Result<(usize, File, PathBuf, u64), Error> {
    let (p, seq) = next_sequence_file(dir).map_err(|e| Error::Io(e).wrap("next sequence file"))?;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&p)
        .map_err(|e| Error::Io(e).wrap("open segment file"))?;
    let res = (|| -> Result<usize, Error> {
        if alloc_size > 0 {
            crate::fileutil::preallocate(&f, alloc_size, true)
                .map_err(|e| Error::Io(e).wrap("preallocate"))?;
        }
        dir_file
            .sync_all()
            .map_err(|e| Error::Io(e).wrap("sync directory"))?;
        // Write header metadata.
        let mut metab = [0u8; SEGMENT_HEADER_SIZE];
        metab[..MAGIC_CHUNKS_SIZE].copy_from_slice(&magic_number.to_be_bytes());
        metab[4] = chunks_format;
        f.write_all(&metab)
            .map_err(|e| Error::Io(e).wrap("write header"))?;
        Ok(metab.len())
    })();
    match res {
        Ok(n) => Ok((n, f, p, seq)),
        Err(e) => {
            drop(f);
            let rm = std::fs::remove_file(&p)
                .err()
                .filter(|e| e.kind() != io::ErrorKind::NotFound);
            Err(join_errors(vec![Some(e), rm.map(Error::Io)]).unwrap())
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Buffered writer with Go `bufio.Writer` semantics, whose buffer is reused across files.

#[derive(Debug)]
struct BufWriter {
    buf: Vec<u8>,
    size: usize,
}

impl BufWriter {
    fn new(size: usize) -> BufWriter {
        BufWriter {
            buf: Vec::with_capacity(size),
            size,
        }
    }

    /// Go `Available`.
    fn available(&self) -> usize {
        self.size - self.buf.len()
    }

    /// Go `Buffered`.
    fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Go `Write`: returns the number of bytes accepted (buffered or written).
    fn write(&mut self, f: &mut File, mut p: &[u8]) -> (usize, Option<io::Error>) {
        let mut nn = 0;
        while p.len() > self.available() {
            let n;
            if self.buf.is_empty() {
                // Large write, empty buffer: write directly to avoid a copy.
                if let Err(e) = f.write_all(p) {
                    return (nn, Some(e));
                }
                n = p.len();
            } else {
                n = self.available();
                self.buf.extend_from_slice(&p[..n]);
                if let Err(e) = self.flush(f) {
                    return (nn + n, Some(e));
                }
            }
            nn += n;
            p = &p[n..];
        }
        self.buf.extend_from_slice(p);
        (nn + p.len(), None)
    }

    /// Go `Flush`.
    fn flush(&mut self, f: &mut File) -> io::Result<()> {
        if !self.buf.is_empty() {
            f.write_all(&self.buf)?;
            self.buf.clear();
        }
        Ok(())
    }
}
