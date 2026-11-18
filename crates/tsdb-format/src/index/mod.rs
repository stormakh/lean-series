//! Port of `tsdb/index`: index v2 writer, reader (v1, v2, v3), postings and postings helpers.
//!
//! - [`Writer`] writes index format v2, byte-identical to Go for the same input.
//! - [`Reader`] reads v1, v2 and v3 index files over an mmapped file or an owned byte buffer, without copies on
//!   the query path (postings are big endian `u32` lists borrowed from the file).
//! - [`Postings`] is the iterator over sorted series references; [`intersect`], [`merge`], [`without`] and the
//!   other helpers combine them. [`MemPostings`] is the in-memory (head) postings index.

mod postings;
mod postingsstats;
mod reader;
mod writer;

pub use postings::*;
pub use postingsstats::*;
pub use reader::*;
pub use writer::*;

use std::fmt;
use std::sync::Arc;

use crate::encoding;

/// Go `MagicIndex`: 4 bytes at the head of an index file.
pub const MAGIC_INDEX: u32 = 0xBAAAD700;
/// Go `HeaderLen`: number of bytes reserved of index for header.
pub const HEADER_LEN: usize = 5;
/// Go `FormatV1`.
pub const FORMAT_V1: i32 = 1;
/// Go `FormatV2`.
pub const FORMAT_V2: i32 = 2;
/// Go `FormatV3`.
pub const FORMAT_V3: i32 = 3;

/// Go `indexFilename`.
pub const INDEX_FILENAME: &str = "index";
pub(crate) const SERIES_BYTE_ALIGN: u64 = 16;

/// Go `checkContextEveryNIterations` of this package: used in some tight loops to check if the context is done.
pub const CHECK_CONTEXT_EVERY_N_ITERATIONS: usize = 128;

/// Go `crc32.Size`.
pub(crate) const CRC32_SIZE: usize = 4;

/// The series reference type (Go `storage.SeriesRef`).
pub use crate::SeriesRef;

// ---------------------------------------------------------------------------------------------------------------
// Errors.

/// What kind of error this is, for `errors.Is` style checks against sentinel errors.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// Any error without a sentinel.
    Other,
    /// An error of the encoding package (`ErrInvalidSize`, `ErrInvalidChecksum`, invalid uvarint).
    Encoding(encoding::Error),
    /// A context error (`context.Canceled`: "context canceled").
    Context(&'static str),
    /// An I/O error of this kind.
    Io(std::io::ErrorKind),
    /// Go `ErrPostingsOffsetTableTooLarge`.
    PostingsOffsetTableTooLarge,
    /// Go `ErrIndexExceeds64GiB`.
    IndexExceeds64GiB,
    /// Go `ErrSymbolTableTooLarge`.
    SymbolTableTooLarge,
}

struct ErrorInner {
    text: String,
    kind: ErrorKind,
    cause: Option<Error>,
}

/// An index error: Go's error text (with `"...: %w"` wrapping) and the chain of wrapped errors, so that
/// [`Error::is`] works like Go's `errors.Is`. Cloning is cheap.
#[derive(Clone)]
pub struct Error(Arc<ErrorInner>);

impl Error {
    /// Go `errors.New(text)`. Each call makes a distinct error value: `is` matches only clones of it.
    pub fn new(text: impl Into<String>) -> Error {
        Error(Arc::new(ErrorInner {
            text: text.into(),
            kind: ErrorKind::Other,
            cause: None,
        }))
    }

    /// A sentinel error of `kind` with the text `text`.
    pub fn with_kind(kind: ErrorKind, text: impl Into<String>) -> Error {
        Error(Arc::new(ErrorInner {
            text: text.into(),
            kind,
            cause: None,
        }))
    }

    /// Go `fmt.Errorf("<prefix>: %w", cause)`.
    pub fn wrap(prefix: &str, cause: impl Into<Error>) -> Error {
        let cause = cause.into();
        Error(Arc::new(ErrorInner {
            text: format!("{prefix}: {cause}"),
            kind: ErrorKind::Other,
            cause: Some(cause),
        }))
    }

    /// An error with the full text `text` that wraps `cause` (Go `fmt.Errorf` with `%w` at any position).
    pub fn with_cause(text: impl Into<String>, cause: impl Into<Error>) -> Error {
        Error(Arc::new(ErrorInner {
            text: text.into(),
            kind: ErrorKind::Other,
            cause: Some(cause.into()),
        }))
    }

    /// A context error (Go `ctx.Err()`), for example "context canceled".
    pub fn context(text: &'static str) -> Error {
        Error::with_kind(ErrorKind::Context(text), text)
    }

    /// Go `context.Canceled`.
    pub fn canceled() -> Error {
        Error::context(crate::context::CANCELED)
    }

    /// Go `ErrPostingsOffsetTableTooLarge`.
    pub fn postings_offset_table_too_large() -> Error {
        Error::with_kind(
            ErrorKind::PostingsOffsetTableTooLarge,
            "length size exceeds 4 bytes",
        )
    }

    /// Go `ErrIndexExceeds64GiB`.
    pub fn index_exceeds_64gib() -> Error {
        Error::with_kind(ErrorKind::IndexExceeds64GiB, "exceeding max size of 64GiB")
    }

    /// Go `ErrSymbolTableTooLarge`.
    pub fn symbol_table_too_large() -> Error {
        Error::with_kind(
            ErrorKind::SymbolTableTooLarge,
            format!("symbol table size exceeds {} bytes", u32::MAX),
        )
    }

    /// The kind of this error (not of the errors it wraps).
    pub fn kind(&self) -> &ErrorKind {
        &self.0.kind
    }

    /// The wrapped error, if any (Go `errors.Unwrap`).
    pub fn cause(&self) -> Option<&Error> {
        self.0.cause.as_ref()
    }

    /// Go `errors.Is(self, target)`: true if this error or one it wraps is `target` (the same value, or the same
    /// sentinel kind).
    pub fn is(&self, target: &Error) -> bool {
        let mut cur = Some(self);
        while let Some(e) = cur {
            if Arc::ptr_eq(&e.0, &target.0)
                || (target.0.kind != ErrorKind::Other && e.0.kind == target.0.kind)
            {
                return true;
            }
            cur = e.cause();
        }
        false
    }

    /// True if this error or one it wraps has the kind `kind`.
    pub fn is_kind(&self, kind: &ErrorKind) -> bool {
        let mut cur = Some(self);
        while let Some(e) = cur {
            if &e.0.kind == kind {
                return true;
            }
            cur = e.cause();
        }
        false
    }

    /// True if this is (or wraps) a context error.
    pub fn is_context(&self) -> bool {
        let mut cur = Some(self);
        while let Some(e) = cur {
            if matches!(e.0.kind, ErrorKind::Context(_)) {
                return true;
            }
            cur = e.cause();
        }
        false
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.text)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0.text)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0
            .cause
            .as_ref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

impl From<encoding::Error> for Error {
    fn from(e: encoding::Error) -> Error {
        let text = e.to_string();
        Error::with_kind(ErrorKind::Encoding(e), text)
    }
}

impl From<&encoding::Error> for Error {
    fn from(e: &encoding::Error) -> Error {
        Error::from(e.clone())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::with_kind(ErrorKind::Io(e.kind()), e.to_string())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Context.

/// The part of Go's `context.Context` this package uses: `Err()`. Implemented by [`crate::context::Context`];
/// tests implement it to count calls (Go `testutil.MockContextErrAfter`).
// TODO(storage): use the shared context type after merge.
pub trait Ctx {
    /// Go `ctx.Err()`: `Some` once the context is done.
    fn err(&self) -> Option<Error>;
}

impl Ctx for crate::context::Context {
    fn err(&self) -> Option<Error> {
        crate::context::Context::err(self).map(Error::context)
    }
}

/// Go `context.Background()` as a `&dyn Ctx`.
pub fn background() -> &'static crate::context::Context {
    static BG: std::sync::OnceLock<crate::context::Context> = std::sync::OnceLock::new();
    BG.get_or_init(crate::context::Context::background)
}

// ---------------------------------------------------------------------------------------------------------------
// LabelHints.

/// Go `storage.LabelHints`: hints passed for label reads. Results are returned in natural (alphabetical) order.
// TODO(storage): use storage::LabelHints after merge.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LabelHints {
    /// Maximum number of results returned. Use a value of 0 to disable.
    pub limit: usize,
    /// Requires `limit` and selects the lexically smallest `limit` values.
    pub limit_smallest: bool,
}

impl LabelHints {
    /// Go `(*LabelHints).AllowsEarlyStop` (a `None` receiver is Go's nil).
    pub fn allows_early_stop(h: Option<&LabelHints>) -> bool {
        h.is_some_and(|h| h.limit > 0 && !h.limit_smallest)
    }

    /// Go `(*LabelHints).ApplyLimit`: reduces `values` to at most `limit` entries, keeping the lexically smallest
    /// ones (sorted) when `limit_smallest` is set, and otherwise the leading ones.
    pub fn apply_limit(h: Option<&LabelHints>, mut values: Vec<String>) -> Vec<String> {
        let Some(h) = h else { return values };
        if h.limit == 0 || values.len() <= h.limit {
            return values;
        }
        if h.limit_smallest {
            return smallest_n_values(values, h.limit);
        }
        values.truncate(h.limit);
        values
    }
}

/// Go `storage.smallestNValues`: the `n` lexically smallest values, sorted.
pub(crate) fn smallest_n_values<S: Ord>(mut values: Vec<S>, n: usize) -> Vec<S> {
    if n < values.len() {
        values.select_nth_unstable(n);
        values.truncate(n);
    }
    values.sort_unstable();
    values
}
