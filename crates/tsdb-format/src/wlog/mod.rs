//! Port of `tsdb/wlog`: WAL and WBL segments, Reader, LiveReader, segment repair, checkpoints and the watcher.
//!
//! On-disk format (same bytes as Go Prometheus): segment files named `%08d` in the log directory, written in
//! 32 KiB pages. Each record is split into fragments that never cross a page; each fragment has a 7-byte header
//! `[type byte][len u16 BE][crc32c u32 BE]`. The type byte holds the fragment type in the low 3 bits, the Snappy
//! flag (0x08) and the zstd flag (0x10). A zero type byte pads the rest of the page.

mod checkpoint;
pub mod compression;
mod live_reader;
mod reader;
mod segment;
mod watcher;
mod wl;

pub use checkpoint::{
    CHECKPOINT_PREFIX, CHECKPOINT_TEMP_FILE_SUFFIX, CheckpointStats, FLUSH_THRESHOLD,
    METADATA_BATCH_SIZE, checkpoint, checkpoint_dir, delete_checkpoints, delete_temp_checkpoints,
    last_checkpoint, read_min_valid_time,
};
pub use live_reader::{LiveReader, LiveReaderMetrics};
pub use reader::Reader;
pub use segment::{
    Segment, SegmentBufReader, SegmentFile, SegmentRange, SegmentRef, create_segment,
    list_segments, new_segment_buf_reader, new_segment_buf_reader_with_offset,
    new_segments_range_reader, new_segments_reader, open_read_segment, open_write_segment,
    segment_name, segments,
};
pub use watcher::{READ_TIMEOUT, SegmentReadFn, Watcher, WatcherMetrics, WriteTo};
pub use wl::{WL, WlMetrics};

use std::fmt;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

/// Go `DefaultSegmentSize`: 128 MiB.
pub const DEFAULT_SEGMENT_SIZE: usize = 128 * 1024 * 1024;
/// Go `pageSize`: 32 KiB.
pub const PAGE_SIZE: usize = 32 * 1024;
/// Go `recordHeaderSize`.
pub const RECORD_HEADER_SIZE: usize = 7;
/// Go `WblDirName`.
pub const WBL_DIR_NAME: &str = "wbl";

// First byte of header format:
//
//	[3 bits unallocated] [1 bit zstd compression flag] [1 bit snappy compression flag] [3 bit record type ]
pub(crate) const SNAPPY_MASK: u8 = 1 << 3;
pub(crate) const ZSTD_MASK: u8 = 1 << 4;
pub(crate) const REC_TYPE_MASK: u8 = SNAPPY_MASK - 1;

/// Go `recType`: the fragment type in the low 3 bits of a fragment header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecType(pub u8);

impl RecType {
    /// Rest of page is empty.
    pub const PAGE_TERM: RecType = RecType(0);
    /// Full record.
    pub const FULL: RecType = RecType(1);
    /// First fragment of a record.
    pub const FIRST: RecType = RecType(2);
    /// Middle fragments of a record.
    pub const MIDDLE: RecType = RecType(3);
    /// Final fragment of a record.
    pub const LAST: RecType = RecType(4);

    /// Go `recTypeFromHeader`.
    pub fn from_header(header: u8) -> RecType {
        RecType(header & REC_TYPE_MASK)
    }
}

impl fmt::Display for RecType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match *self {
            RecType::PAGE_TERM => "zero",
            RecType::FULL => "full",
            RecType::FIRST => "first",
            RecType::MIDDLE => "middle",
            RecType::LAST => "last",
            _ => "<invalid>",
        })
    }
}

/// Go `validateRecord`.
pub(crate) fn validate_record(typ: RecType, i: usize) -> Result<()> {
    match typ {
        RecType::FULL => {
            if i != 0 {
                return Err(Error::msg("unexpected full record"));
            }
            Ok(())
        }
        RecType::FIRST => {
            if i != 0 {
                return Err(Error::msg("unexpected first record, dropping buffer"));
            }
            Ok(())
        }
        RecType::MIDDLE => {
            if i == 0 {
                return Err(Error::msg("unexpected middle record, dropping buffer"));
            }
            Ok(())
        }
        RecType::LAST => {
            if i == 0 {
                return Err(Error::msg("unexpected last record, dropping buffer"));
            }
            Ok(())
        }
        _ => Err(Error::Msg(format!("unexpected record type {}", typ.0))),
    }
}

/// Errors of the wlog package. `Display` prints Go's text, including `"<context>: <cause>"` wrapping.
#[derive(Debug, Clone)]
pub enum Error {
    /// An operating system I/O error. Its text is Rust's (Go adds the operation and path).
    Io(Arc<io::Error>),
    /// Go `io.EOF`.
    Eof,
    /// Go `io.ErrUnexpectedEOF`.
    UnexpectedEof,
    /// Go `errors.New` / `fmt.Errorf` without `%w`.
    Msg(String),
    /// Go `fmt.Errorf("<context>: %w", err)`.
    Wrap(String, Box<Error>),
    /// Go `*CorruptionErr`.
    Corruption(Box<CorruptionErr>),
    /// Go `record.ErrNotFound` ("not found"), returned by `last_checkpoint`.
    NotFound,
    /// Go `ErrIgnorable` ("ignore me"), used inside the watcher.
    Ignorable,
}

impl Error {
    pub fn msg(s: &str) -> Error {
        Error::Msg(s.to_string())
    }

    /// Go `fmt.Errorf("<ctx>: %w", self)`.
    pub fn wrap(self, ctx: &str) -> Error {
        Error::Wrap(ctx.to_string(), Box::new(self))
    }

    /// Go `errors.Is(err, io.EOF)`.
    pub fn is_eof(&self) -> bool {
        match self {
            Error::Eof => true,
            Error::Wrap(_, e) => e.is_eof(),
            Error::Corruption(c) => c.err.is_eof(),
            _ => false,
        }
    }

    /// Go `errors.Is(err, record.ErrNotFound)`.
    pub fn is_not_found(&self) -> bool {
        match self {
            Error::NotFound => true,
            Error::Wrap(_, e) => e.is_not_found(),
            _ => false,
        }
    }

    /// Go `errors.As(err, &cerr)` for `*CorruptionErr`.
    pub fn corruption(&self) -> Option<&CorruptionErr> {
        match self {
            Error::Corruption(c) => Some(c),
            Error::Wrap(_, e) => e.corruption(),
            _ => None,
        }
    }

    /// The I/O error at the end of the wrap chain, if any (Go `errors.Is(err, <io error>)`).
    pub fn io_error(&self) -> Option<&io::Error> {
        match self {
            Error::Io(e) => Some(e),
            Error::Wrap(_, e) => e.io_error(),
            Error::Corruption(c) => c.err.io_error(),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "{e}"),
            Error::Eof => f.write_str("EOF"),
            Error::UnexpectedEof => f.write_str("unexpected EOF"),
            Error::Msg(s) => f.write_str(s),
            Error::Wrap(ctx, e) => write!(f, "{ctx}: {e}"),
            Error::Corruption(c) => write!(f, "{c}"),
            Error::NotFound => f.write_str(crate::record::ERR_NOT_FOUND),
            Error::Ignorable => f.write_str("ignore me"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(&**e),
            Error::Wrap(_, e) => Some(&**e),
            Error::Corruption(c) => Some(&c.err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        match e.kind() {
            io::ErrorKind::UnexpectedEof => Error::UnexpectedEof,
            _ => Error::Io(Arc::new(e)),
        }
    }
}

impl From<Error> for io::Error {
    fn from(e: Error) -> io::Error {
        match e {
            Error::Io(e) => match Arc::try_unwrap(e) {
                Ok(e) => e,
                Err(e) => io::Error::new(e.kind(), e.to_string()),
            },
            Error::Eof | Error::UnexpectedEof => {
                io::Error::new(io::ErrorKind::UnexpectedEof, e.to_string())
            }
            e => io::Error::other(e),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Go `CorruptionErr`: returned when corruption is encountered.
#[derive(Debug, Clone)]
pub struct CorruptionErr {
    pub dir: PathBuf,
    /// Segment index; negative when the reader does not know it.
    pub segment: i64,
    pub offset: i64,
    pub err: Error,
}

impl fmt::Display for CorruptionErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.segment < 0 {
            return write!(f, "corruption after {} bytes: {}", self.offset, self.err);
        }
        write!(
            f,
            "corruption in segment {} at {}: {}",
            segment_name(&self.dir, self.segment).display(),
            self.offset,
            self.err
        )
    }
}

/// Go `WriteNotified`: notifies the watcher that data has been written so that it can read.
pub trait WriteNotified: Send + Sync {
    fn notify(&self);
}

/// Go `io.ReadFull`: `Eof` when nothing was read, `UnexpectedEof` when only part was read.
pub(crate) fn read_full<R: io::Read + ?Sized>(r: &mut R, buf: &mut [u8]) -> Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    if n == buf.len() {
        Ok(n)
    } else if n == 0 {
        Err(Error::Eof)
    } else {
        Err(Error::UnexpectedEof)
    }
}
