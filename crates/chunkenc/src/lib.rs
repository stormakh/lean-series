//! Chunk encodings (port of `tsdb/chunkenc`): XOR, histogram, float histogram, XOR2 and the start-timestamp
//! histogram variants. The byte output is identical to Prometheus 3.16.0 (the on-disk format).
//!
//! Design: chunks own their bytes. Appenders hold only the encoder state; every append call gets the chunk it
//! appends to (`app.append(&mut chunk, st, t, v)`), so a chunk can be read while its appender lives. Iterators
//! borrow the chunk bytes (`XorIterator<'a>` reads a `&'a [u8]`), so they also work on mmapped bytes without a
//! copy. The [`Chunk`] trait, the [`ChunkIterator`] enum and the [`Appender`] enum give dynamic dispatch over the
//! encodings (Go's interfaces).

pub mod bstream;
pub mod float_histogram;
pub mod histogram;
pub mod histogram_meta;
pub mod st;
pub mod varbit;
pub mod xor;
pub mod xor2;

pub use bstream::{Bstream, BstreamReader};
pub use float_histogram::*;
pub use histogram::*;
pub use histogram_meta::*;
pub use xor::*;
pub use xor2::*;

use model::histogram::{FloatHistogram, Histogram};
use std::any::Any;
use std::borrow::Cow;
use std::fmt;
use std::sync::Mutex;

/// The chunk encoding byte (Go `chunkenc.Encoding`).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Encoding(pub u8);

pub const ENC_NONE: Encoding = Encoding(0);
pub const ENC_XOR: Encoding = Encoding(1);
pub const ENC_HISTOGRAM: Encoding = Encoding(2);
pub const ENC_FLOAT_HISTOGRAM: Encoding = Encoding(3);
pub const ENC_XOR2: Encoding = Encoding(4);
pub const ENC_HISTOGRAM_ST: Encoding = Encoding(5);
pub const ENC_FLOAT_HISTOGRAM_ST: Encoding = Encoding(6);

impl Encoding {
    /// Go `Encoding.String()`.
    pub fn as_str(self) -> &'static str {
        match self.0 {
            0 => "none",
            1 => "XOR",
            2 => "histogram",
            3 => "floathistogram",
            4 => "XOR2",
            5 => "histogramST",
            6 => "floathistogramST",
            _ => "<unknown>",
        }
    }
}

impl fmt::Display for Encoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for Encoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Returns true for the encodings 1 to 6.
pub fn is_valid_encoding(e: Encoding) -> bool {
    matches!(e, ENC_XOR | ENC_HISTOGRAM | ENC_FLOAT_HISTOGRAM | ENC_XOR2 | ENC_HISTOGRAM_ST | ENC_FLOAT_HISTOGRAM_ST)
}

/// The maximum size an XOR chunk can be.
pub const MAX_BYTES_PER_XOR_CHUNK: usize = 1024;
/// Used for cutting new XOR chunks, assuming the next sample is maximally sized (19 bytes).
pub const MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND: usize = MAX_BYTES_PER_XOR_CHUNK - 19;
/// A size target for each histogram chunk.
pub const TARGET_BYTES_PER_HISTOGRAM_CHUNK: usize = 1024;
/// A minimum sample count for histogram chunks.
pub const MIN_SAMPLES_PER_HISTOGRAM_CHUNK: usize = 10;

pub(crate) const CHUNK_HEADER_SIZE: usize = 2;
pub(crate) const CHUNK_ALLOCATION_SIZE: usize = 128;
pub(crate) const CHUNK_COMPACT_CAPACITY_THRESHOLD: usize = 32;

pub(crate) const STALE_NAN: u64 = 0x7ff0000000000002;

#[inline(always)]
pub(crate) fn is_stale_nan(f: f64) -> bool {
    f.to_bits() == STALE_NAN
}

/// Shrinks the allocation of a chunk's bytes when it has much spare capacity (Go `Compact`).
pub(crate) fn compact_stream(stream: &mut Vec<u8>) {
    let l = stream.len();
    if stream.capacity() > l + CHUNK_COMPACT_CAPACITY_THRESHOLD {
        let buf = stream[..l].to_vec();
        *stream = buf;
    }
}

/// The type of the value at the iterator position.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum ValueType {
    /// No value at the current position.
    #[default]
    None = 0,
    /// A simple float, retrieved with `at`.
    Float = 1,
    /// A histogram, retrieved with `at_histogram` (`at_float_histogram` works, too).
    Histogram = 2,
    /// A floating-point histogram, retrieved with `at_float_histogram`.
    FloatHistogram = 3,
}

impl ValueType {
    /// Go `ValueType.String()`.
    pub fn as_str(self) -> &'static str {
        match self {
            ValueType::None => "none",
            ValueType::Float => "float",
            ValueType::Histogram => "histogram",
            ValueType::FloatHistogram => "floathistogram",
        }
    }

    /// The encoding for new chunks of this value type.
    pub fn chunk_encoding(self, use_xor2: bool, use_histogram_st: bool) -> Encoding {
        match self {
            ValueType::Float => {
                if use_xor2 {
                    ENC_XOR2
                } else {
                    ENC_XOR
                }
            }
            ValueType::Histogram => {
                if use_histogram_st {
                    ENC_HISTOGRAM_ST
                } else {
                    ENC_HISTOGRAM
                }
            }
            ValueType::FloatHistogram => {
                if use_histogram_st {
                    ENC_FLOAT_HISTOGRAM_ST
                } else {
                    ENC_FLOAT_HISTOGRAM
                }
            }
            ValueType::None => ENC_NONE,
        }
    }

    /// A new empty chunk for this value type.
    pub fn new_chunk(self, use_xor2: bool, use_histogram_st: bool) -> Result<Box<dyn Chunk>, Error> {
        new_empty_chunk(self.chunk_encoding(use_xor2, use_histogram_st))
    }
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether a chunk can continue to receive appends of another encoding (only the XOR float family).
pub fn compatible_values(a: Encoding, b: Encoding) -> bool {
    is_float_xor_encoding(a) && is_float_xor_encoding(b)
}

fn is_float_xor_encoding(e: Encoding) -> bool {
    e == ENC_XOR || e == ENC_XOR2
}

/// Errors of the chunk encodings.
#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    /// Go `io.EOF`.
    Eof,
    /// Go `io.ErrUnexpectedEOF`.
    UnexpectedEof,
    /// Go `binary` varint overflow.
    VarintOverflow,
    /// A varbit prefix that is not valid.
    InvalidBitPattern(u8),
    /// A histogram error (for example an unknown schema).
    Histogram(model::histogram::Error),
    /// Any other error text.
    Msg(Cow<'static, str>),
}

impl Error {
    pub fn msg(s: impl Into<Cow<'static, str>>) -> Error {
        Error::Msg(s.into())
    }
    /// Go `errors.Is(err, histogram.ErrX)`.
    pub fn is_histogram(&self, kind: model::histogram::ErrorKind) -> bool {
        matches!(self, Error::Histogram(e) if e.is(kind))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Eof => f.write_str("EOF"),
            Error::UnexpectedEof => f.write_str("unexpected EOF"),
            Error::VarintOverflow => f.write_str("binary: varint overflows a 64-bit integer"),
            Error::InvalidBitPattern(d) => write!(f, "invalid bit pattern {:b}", d),
            Error::Histogram(e) => write!(f, "{}", e),
            Error::Msg(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for Error {}

impl From<model::histogram::Error> for Error {
    fn from(e: model::histogram::Error) -> Error {
        Error::Histogram(e)
    }
}

pub(crate) fn invalid_encoding(e: Encoding) -> Error {
    Error::Msg(Cow::Owned(format!("invalid chunk encoding \"{}\"", e.as_str())))
}

/// Iterates over the samples of a chunk (Go `chunkenc.Iterator`).
pub trait Iterator {
    /// Advances the iterator by one and returns the type of the value at the new position (or `ValueType::None`
    /// if the iterator is exhausted).
    fn next(&mut self) -> ValueType;
    /// Advances to the first sample with a timestamp >= t. No effect if the current sample already qualifies.
    fn seek(&mut self, t: i64) -> ValueType;
    /// The current timestamp/value pair if the value is a float.
    fn at(&self) -> (i64, f64);
    /// The current histogram. `h` is reused when given (Go passes a non-nil histogram).
    fn at_histogram(&self, h: Option<Histogram>) -> (i64, Histogram);
    /// The current float histogram (also for integer histograms). `fh` is reused when given.
    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram);
    /// The current timestamp.
    fn at_t(&self) -> i64;
    /// The current start timestamp (0 if not implemented or not set).
    fn at_st(&self) -> i64;
    /// The current error; use only after `next` or `seek` returned `ValueType::None`.
    fn err(&self) -> Option<&Error>;
}

impl<T: Iterator + ?Sized> Iterator for Box<T> {
    fn next(&mut self) -> ValueType {
        (**self).next()
    }
    fn seek(&mut self, t: i64) -> ValueType {
        (**self).seek(t)
    }
    fn at(&self) -> (i64, f64) {
        (**self).at()
    }
    fn at_histogram(&self, h: Option<Histogram>) -> (i64, Histogram) {
        (**self).at_histogram(h)
    }
    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        (**self).at_float_histogram(fh)
    }
    fn at_t(&self) -> i64 {
        (**self).at_t()
    }
    fn at_st(&self) -> i64 {
        (**self).at_st()
    }
    fn err(&self) -> Option<&Error> {
        (**self).err()
    }
}

/// A chunk (Go `chunkenc.Chunk`).
pub trait Chunk: Any + Send + Sync + fmt::Debug {
    /// The underlying bytes of the chunk.
    fn bytes(&self) -> &[u8];
    /// The encoding of the chunk.
    fn encoding(&self) -> Encoding;
    /// An appender (state) to append samples to this chunk. Reads the chunk to restore the state.
    fn appender(&mut self) -> Result<Appender, Error>;
    /// An iterator over the chunk; `it` is reused when it is of the right kind.
    fn iterator<'a>(&'a self, it: Option<ChunkIterator<'a>>) -> ChunkIterator<'a>;
    /// The number of samples in the chunk.
    fn num_samples(&self) -> usize;
    /// Called when the chunk is expected to be complete; may shrink the allocation.
    fn compact(&mut self);
    /// Resets the chunk to the given bytes.
    fn reset(&mut self, stream: Vec<u8>);
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Dynamic iterator over any chunk encoding (enum dispatch, no allocation).
pub enum ChunkIterator<'a> {
    Nop(NopIterator),
    Xor(XorIterator<'a>),
    Xor2(Xor2Iterator<'a>),
    Histogram(HistogramIterator<'a>),
    FloatHistogram(FloatHistogramIterator<'a>),
    HistogramSt(HistogramStIterator<'a>),
    FloatHistogramSt(FloatHistogramStIterator<'a>),
    Other(Box<dyn Iterator + 'a>),
}

impl<'a> ChunkIterator<'a> {
    /// An iterator over chunk bytes of encoding `e` (no copy).
    pub fn new(e: Encoding, b: &'a [u8]) -> Result<ChunkIterator<'a>, Error> {
        Ok(match e {
            ENC_XOR => ChunkIterator::Xor(XorIterator::new(b)),
            ENC_XOR2 => ChunkIterator::Xor2(Xor2Iterator::new(b)),
            ENC_HISTOGRAM => ChunkIterator::Histogram(HistogramIterator::new(b)),
            ENC_FLOAT_HISTOGRAM => ChunkIterator::FloatHistogram(FloatHistogramIterator::new(b)),
            ENC_HISTOGRAM_ST => ChunkIterator::HistogramSt(HistogramStIterator::new(b)),
            ENC_FLOAT_HISTOGRAM_ST => ChunkIterator::FloatHistogramSt(FloatHistogramStIterator::new(b)),
            _ => return Err(invalid_encoding(e)),
        })
    }

    /// Resets the iterator to new bytes of encoding `e`, reusing its buffers when the kind matches.
    pub fn reset_to(self, e: Encoding, b: &'a [u8]) -> Result<ChunkIterator<'a>, Error> {
        Ok(match (self, e) {
            (ChunkIterator::Xor(mut it), ENC_XOR) => {
                it.reset(b);
                ChunkIterator::Xor(it)
            }
            (ChunkIterator::Xor2(mut it), ENC_XOR2) => {
                it.reset(b);
                ChunkIterator::Xor2(it)
            }
            (ChunkIterator::Histogram(mut it), ENC_HISTOGRAM) => {
                it.reset(b);
                ChunkIterator::Histogram(it)
            }
            (ChunkIterator::FloatHistogram(mut it), ENC_FLOAT_HISTOGRAM) => {
                it.reset(b);
                ChunkIterator::FloatHistogram(it)
            }
            (ChunkIterator::HistogramSt(mut it), ENC_HISTOGRAM_ST) => {
                it.reset(b);
                ChunkIterator::HistogramSt(it)
            }
            (ChunkIterator::FloatHistogramSt(mut it), ENC_FLOAT_HISTOGRAM_ST) => {
                it.reset(b);
                ChunkIterator::FloatHistogramSt(it)
            }
            (_, e) => ChunkIterator::new(e, b)?,
        })
    }
}

macro_rules! dispatch_it {
    ($self:expr, $it:ident => $e:expr) => {
        match $self {
            ChunkIterator::Nop($it) => $e,
            ChunkIterator::Xor($it) => $e,
            ChunkIterator::Xor2($it) => $e,
            ChunkIterator::Histogram($it) => $e,
            ChunkIterator::FloatHistogram($it) => $e,
            ChunkIterator::HistogramSt($it) => $e,
            ChunkIterator::FloatHistogramSt($it) => $e,
            ChunkIterator::Other($it) => $e,
        }
    };
}

impl Iterator for ChunkIterator<'_> {
    #[inline]
    fn next(&mut self) -> ValueType {
        dispatch_it!(self, it => it.next())
    }
    fn seek(&mut self, t: i64) -> ValueType {
        dispatch_it!(self, it => it.seek(t))
    }
    #[inline]
    fn at(&self) -> (i64, f64) {
        dispatch_it!(self, it => it.at())
    }
    fn at_histogram(&self, h: Option<Histogram>) -> (i64, Histogram) {
        dispatch_it!(self, it => it.at_histogram(h))
    }
    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        dispatch_it!(self, it => it.at_float_histogram(fh))
    }
    #[inline]
    fn at_t(&self) -> i64 {
        dispatch_it!(self, it => it.at_t())
    }
    fn at_st(&self) -> i64 {
        dispatch_it!(self, it => it.at_st())
    }
    fn err(&self) -> Option<&Error> {
        dispatch_it!(self, it => it.err())
    }
}

/// Dynamic appender over any chunk encoding. The methods take the chunk the appender belongs to and panic if it
/// has another concrete type.
#[derive(Clone, Debug)]
pub enum Appender {
    Xor(XorAppender),
    Xor2(Xor2Appender),
    Histogram(HistogramAppender),
    FloatHistogram(FloatHistogramAppender),
    HistogramSt(HistogramStAppender),
    FloatHistogramSt(FloatHistogramStAppender),
}

fn downcast<T: 'static>(c: &mut dyn Chunk) -> &mut T {
    match c.as_any_mut().downcast_mut::<T>() {
        Some(c) => c,
        None => panic!("appender used with a chunk of another type"),
    }
}

/// The result of appending a histogram: a new chunk with its appender (if one was cut or recoded) and whether
/// the new chunk is the recoded current chunk.
pub type AppendResult = (Option<(Box<dyn Chunk>, Appender)>, bool);

impl Appender {
    /// Appends a float sample. Panics if the chunk is full or not a float chunk.
    pub fn append(&mut self, c: &mut dyn Chunk, st: i64, t: i64, v: f64) {
        match self {
            Appender::Xor(a) => a.append(downcast::<XorChunk>(c), st, t, v),
            Appender::Xor2(a) => a.append(downcast::<Xor2Chunk>(c), st, t, v),
            Appender::Histogram(_) | Appender::HistogramSt(_) => panic!("appended a float sample to a histogram chunk"),
            Appender::FloatHistogram(_) | Appender::FloatHistogramSt(_) => {
                panic!("appended a float sample to a float histogram chunk")
            }
        }
    }

    /// The histogram appendable view of this appender (for `prev`).
    pub fn as_histogram_appendable(&self) -> Option<&dyn HistogramAppendable> {
        match self {
            Appender::Histogram(a) => Some(a),
            Appender::HistogramSt(a) => Some(a),
            _ => None,
        }
    }

    /// The float histogram appendable view of this appender (for `prev`).
    pub fn as_float_histogram_appendable(&self) -> Option<&dyn FloatHistogramAppendable> {
        match self {
            Appender::FloatHistogram(a) => Some(a),
            Appender::FloatHistogramSt(a) => Some(a),
            _ => None,
        }
    }

    /// Appends a histogram sample (Go `AppendHistogram`). `h` may be modified (layout adjustments), as in Go.
    pub fn append_histogram(
        &mut self,
        c: &mut dyn Chunk,
        prev: Option<&Appender>,
        st: i64,
        t: i64,
        h: &mut Histogram,
        append_only: bool,
    ) -> Result<AppendResult, Error> {
        let prev = prev.and_then(|p| p.as_histogram_appendable());
        match self {
            Appender::Histogram(a) => {
                let (n, r) = a.append_histogram(downcast::<HistogramChunk>(c), prev, st, t, h, append_only)?;
                Ok((n.map(|(c, a)| (Box::new(c) as Box<dyn Chunk>, Appender::Histogram(a))), r))
            }
            Appender::HistogramSt(a) => {
                let (n, r) = a.append_histogram(downcast::<HistogramStChunk>(c), prev, st, t, h, append_only)?;
                Ok((n.map(|(c, a)| (Box::new(c) as Box<dyn Chunk>, Appender::HistogramSt(a))), r))
            }
            Appender::Xor(_) | Appender::Xor2(_) => panic!("appended a histogram sample to a float chunk"),
            Appender::FloatHistogram(_) | Appender::FloatHistogramSt(_) => {
                panic!("appended a histogram sample to a float histogram chunk")
            }
        }
    }

    /// Appends a float histogram sample (Go `AppendFloatHistogram`). `h` may be modified, as in Go.
    pub fn append_float_histogram(
        &mut self,
        c: &mut dyn Chunk,
        prev: Option<&Appender>,
        st: i64,
        t: i64,
        h: &mut FloatHistogram,
        append_only: bool,
    ) -> Result<AppendResult, Error> {
        let prev = prev.and_then(|p| p.as_float_histogram_appendable());
        match self {
            Appender::FloatHistogram(a) => {
                let (n, r) = a.append_float_histogram(downcast::<FloatHistogramChunk>(c), prev, st, t, h, append_only)?;
                Ok((n.map(|(c, a)| (Box::new(c) as Box<dyn Chunk>, Appender::FloatHistogram(a))), r))
            }
            Appender::FloatHistogramSt(a) => {
                let (n, r) =
                    a.append_float_histogram(downcast::<FloatHistogramStChunk>(c), prev, st, t, h, append_only)?;
                Ok((n.map(|(c, a)| (Box::new(c) as Box<dyn Chunk>, Appender::FloatHistogramSt(a))), r))
            }
            Appender::Xor(_) | Appender::Xor2(_) => panic!("appended a float histogram sample to a float chunk"),
            Appender::Histogram(_) | Appender::HistogramSt(_) => {
                panic!("appended a float histogram sample to a histogram chunk")
            }
        }
    }
}

/// An iterator over fixed samples (Go `MockSeriesIterator`).
#[derive(Clone, Debug)]
pub struct MockSeriesIterator {
    start_timestamps: Vec<i64>,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    curr_index: isize,
}

pub fn mock_series_iterator(start_timestamps: Vec<i64>, timestamps: Vec<i64>, values: Vec<f64>) -> MockSeriesIterator {
    MockSeriesIterator { start_timestamps, timestamps, values, curr_index: -1 }
}

impl Iterator for MockSeriesIterator {
    fn next(&mut self) -> ValueType {
        if self.curr_index < self.timestamps.len() as isize - 1 {
            self.curr_index += 1;
            return ValueType::Float;
        }
        ValueType::None
    }
    fn seek(&mut self, _t: i64) -> ValueType {
        ValueType::None
    }
    fn at(&self) -> (i64, f64) {
        (self.timestamps[self.curr_index as usize], self.values[self.curr_index as usize])
    }
    fn at_histogram(&self, _h: Option<Histogram>) -> (i64, Histogram) {
        (i64::MIN, Histogram::default())
    }
    fn at_float_histogram(&self, _fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        (i64::MIN, FloatHistogram::default())
    }
    fn at_t(&self) -> i64 {
        self.timestamps[self.curr_index as usize]
    }
    fn at_st(&self) -> i64 {
        if self.start_timestamps.is_empty() {
            return 0;
        }
        self.start_timestamps[self.curr_index as usize]
    }
    fn err(&self) -> Option<&Error> {
        None
    }
}

/// An iterator without samples (Go `NewNopIterator`).
#[derive(Clone, Copy, Debug, Default)]
pub struct NopIterator;

pub fn new_nop_iterator() -> NopIterator {
    NopIterator
}

impl Iterator for NopIterator {
    fn next(&mut self) -> ValueType {
        ValueType::None
    }
    fn seek(&mut self, _t: i64) -> ValueType {
        ValueType::None
    }
    fn at(&self) -> (i64, f64) {
        (i64::MIN, 0.0)
    }
    fn at_histogram(&self, _h: Option<Histogram>) -> (i64, Histogram) {
        (i64::MIN, Histogram::default())
    }
    fn at_float_histogram(&self, _fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        (i64::MIN, FloatHistogram::default())
    }
    fn at_t(&self) -> i64 {
        i64::MIN
    }
    fn at_st(&self) -> i64 {
        0
    }
    fn err(&self) -> Option<&Error> {
        None
    }
}

/// Stores chunks so that they can be reused (Go `chunkenc.Pool`).
pub trait Pool: Send + Sync {
    fn put(&self, c: Box<dyn Chunk>) -> Result<(), Error>;
    fn get(&self, e: Encoding, b: Vec<u8>) -> Result<Box<dyn Chunk>, Error>;
}

/// The default pool: a free list per encoding.
#[derive(Default)]
pub struct DefaultPool {
    /// The free lists, indexed by encoding - 1 (public for tests only).
    #[doc(hidden)]
    pub free: [Mutex<Vec<Box<dyn Chunk>>>; 6],
}

/// Go `NewPool`.
pub fn new_pool() -> DefaultPool {
    DefaultPool::default()
}

const POOL_MAX: usize = 1024;

impl Pool for DefaultPool {
    fn get(&self, e: Encoding, b: Vec<u8>) -> Result<Box<dyn Chunk>, Error> {
        if !is_valid_encoding(e) {
            return Err(invalid_encoding(e));
        }
        let pooled = self.free[e.0 as usize - 1].lock().unwrap_or_else(|p| p.into_inner()).pop();
        let mut c = match pooled {
            Some(c) => c,
            None => new_empty_chunk(e)?,
        };
        c.reset(b);
        Ok(c)
    }

    fn put(&self, mut c: Box<dyn Chunk>) -> Result<(), Error> {
        let e = c.encoding();
        let ok = match e {
            ENC_XOR => c.as_any().is::<XorChunk>(),
            ENC_HISTOGRAM => c.as_any().is::<HistogramChunk>(),
            ENC_FLOAT_HISTOGRAM => c.as_any().is::<FloatHistogramChunk>(),
            ENC_XOR2 => c.as_any().is::<Xor2Chunk>(),
            ENC_HISTOGRAM_ST => c.as_any().is::<HistogramStChunk>(),
            ENC_FLOAT_HISTOGRAM_ST => c.as_any().is::<FloatHistogramStChunk>(),
            _ => return Err(invalid_encoding(e)),
        };
        if !ok {
            // Wrapped chunks: skip them.
            return Ok(());
        }
        c.reset(Vec::new());
        let mut free = self.free[e.0 as usize - 1].lock().unwrap_or_else(|p| p.into_inner());
        if free.len() < POOL_MAX {
            free.push(c);
        }
        Ok(())
    }
}

/// A chunk of encoding `e` over the given bytes (Go `FromData`).
pub fn from_data(e: Encoding, d: Vec<u8>) -> Result<Box<dyn Chunk>, Error> {
    let b = Bstream::new(d);
    Ok(match e {
        ENC_XOR => Box::new(XorChunk { b }),
        ENC_HISTOGRAM => Box::new(HistogramChunk::from_bstream(b)),
        ENC_FLOAT_HISTOGRAM => Box::new(FloatHistogramChunk::from_bstream(b)),
        ENC_XOR2 => Box::new(Xor2Chunk { b }),
        ENC_HISTOGRAM_ST => Box::new(HistogramStChunk::from_bstream(b)),
        ENC_FLOAT_HISTOGRAM_ST => Box::new(FloatHistogramStChunk::from_bstream(b)),
        _ => return Err(invalid_encoding(e)),
    })
}

/// A new empty chunk of encoding `e` (Go `NewEmptyChunk`).
pub fn new_empty_chunk(e: Encoding) -> Result<Box<dyn Chunk>, Error> {
    Ok(match e {
        ENC_XOR => Box::new(XorChunk::new()),
        ENC_HISTOGRAM => Box::new(HistogramChunk::new()),
        ENC_FLOAT_HISTOGRAM => Box::new(FloatHistogramChunk::new()),
        ENC_XOR2 => Box::new(Xor2Chunk::new()),
        ENC_HISTOGRAM_ST => Box::new(HistogramStChunk::new()),
        ENC_FLOAT_HISTOGRAM_ST => Box::new(FloatHistogramStChunk::new()),
        _ => return Err(invalid_encoding(e)),
    })
}

/// Implements the [`Chunk`] trait for a concrete chunk type with inherent methods of the same names.
macro_rules! impl_chunk {
    ($t:ty, $variant:ident) => {
        impl $crate::Chunk for $t {
            fn bytes(&self) -> &[u8] {
                <$t>::bytes(self)
            }
            fn encoding(&self) -> $crate::Encoding {
                <$t>::encoding(self)
            }
            fn appender(&mut self) -> Result<$crate::Appender, $crate::Error> {
                <$t>::appender(self).map($crate::Appender::$variant)
            }
            fn iterator<'a>(&'a self, it: Option<$crate::ChunkIterator<'a>>) -> $crate::ChunkIterator<'a> {
                match it {
                    Some($crate::ChunkIterator::$variant(mut it)) => {
                        it.reset(<$t>::bytes(self));
                        $crate::ChunkIterator::$variant(it)
                    }
                    _ => $crate::ChunkIterator::$variant(<$t>::iterator(self)),
                }
            }
            fn num_samples(&self) -> usize {
                <$t>::num_samples(self)
            }
            fn compact(&mut self) {
                <$t>::compact(self)
            }
            fn reset(&mut self, stream: Vec<u8>) {
                <$t>::reset(self, stream)
            }
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
                self
            }
        }
    };
}
pub(crate) use impl_chunk;
