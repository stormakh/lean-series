//! Errors of the storage layer (port of the error values in `storage/interface.go`, `storage/errors.go` and the
//! `AppendPartialError` of `storage/interface_append.go`).
//!
//! Go code compares errors with `errors.Is`, which follows `%w` wrapping and `errors.Join`. [`Error`] models the
//! same: sentinels (Go `errors.New` package variables) compare by address, wrapped errors keep their source, and
//! [`Error::is`] walks the chain. Every error text is the same string as Go's.

use std::any::Any;
use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use crate::annotations::AnnoError;

/// A Go error variable made with `errors.New` (or `fmt.Errorf("%w: ...", parent)` when `parent` is set).
/// Sentinels must be `static` items: `errors.Is` compares them by address, as Go compares pointers.
#[derive(Debug)]
pub struct Sentinel {
    pub msg: &'static str,
    /// The wrapped error (Go `Unwrap`), for sentinels such as `annotations.InvalidQuantileWarning` that wrap
    /// `annotations.PromQLWarning`.
    pub parent: Option<&'static Sentinel>,
}

impl Sentinel {
    pub const fn new(msg: &'static str) -> Sentinel {
        Sentinel { msg, parent: None }
    }
    pub const fn wrapping(msg: &'static str, parent: &'static Sentinel) -> Sentinel {
        Sentinel { msg, parent: Some(parent) }
    }
}

/// A storage error. Cheap to clone (heavy variants are behind `Arc`).
#[derive(Clone)]
pub enum Error {
    /// A sentinel error value (Go package-level `errors.New`).
    Sentinel(&'static Sentinel),
    /// Go `errDuplicateSampleForTimestamp`.
    DuplicateSample(DuplicateSampleError),
    /// Go `*AppendPartialError`.
    AppendPartial(Arc<AppendPartialError>),
    /// An error of a chunk encoding.
    Chunk(chunkenc::Error),
    /// Go `fmt.Errorf("...%w...", source)`: the full message and the wrapped error.
    Wrapped(Arc<(String, Error)>),
    /// Go `errors.Join`: the messages joined by newlines.
    Join(Arc<[Error]>),
    /// Go `errors.New(msg)` made at run time. Two `Msg` errors are the same error (`errors.Is`) only when they
    /// are clones of each other, as in Go.
    Msg(Arc<str>),
    /// A PromQL annotation (see the `annotations` module).
    Annotation(Arc<dyn AnnoError>),
    /// Any other error, for example of another crate. `downcast_ref` finds it.
    Other(Arc<dyn std::error::Error + Send + Sync>),
}

/// Go `ErrNotFound`.
pub static NOT_FOUND: Sentinel = Sentinel::new("not found");
pub const ERR_NOT_FOUND: Error = Error::Sentinel(&NOT_FOUND);
static COST_SAMPLING_UNSUPPORTED: Sentinel = Sentinel::new("bounded query cost sampling is unavailable");
/// The requested window cannot be sampled incrementally.
pub const ERR_COST_SAMPLING_UNSUPPORTED: Error = Error::Sentinel(&COST_SAMPLING_UNSUPPORTED);
static OUT_OF_ORDER_SAMPLE: Sentinel = Sentinel::new("out of order sample");
/// Out of order support is disabled and the sample is out of order.
pub const ERR_OUT_OF_ORDER_SAMPLE: Error = Error::Sentinel(&OUT_OF_ORDER_SAMPLE);
static OUT_OF_BOUNDS: Sentinel = Sentinel::new("out of bounds");
/// Out of order support is disabled and the sample is older than the min valid time for the append.
pub const ERR_OUT_OF_BOUNDS: Error = Error::Sentinel(&OUT_OF_BOUNDS);
static TOO_OLD_SAMPLE: Sentinel = Sentinel::new("too old sample");
/// Out of order support is enabled but the sample is outside the time window allowed.
pub const ERR_TOO_OLD_SAMPLE: Error = Error::Sentinel(&TOO_OLD_SAMPLE);
/// The sample has the same timestamp but a different value (Go `ErrDuplicateSampleForTimestamp`). Every
/// [`DuplicateSampleError`] `is` this error.
pub const ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP: Error = Error::DuplicateSample(DuplicateSampleError::ZERO);
static OUT_OF_ORDER_EXEMPLAR: Sentinel = Sentinel::new("out of order exemplar");
pub const ERR_OUT_OF_ORDER_EXEMPLAR: Error = Error::Sentinel(&OUT_OF_ORDER_EXEMPLAR);
static DUPLICATE_EXEMPLAR: Sentinel = Sentinel::new("duplicate exemplar");
pub const ERR_DUPLICATE_EXEMPLAR: Error = Error::Sentinel(&DUPLICATE_EXEMPLAR);
// "label length for exemplar exceeds maximum of %d UTF-8 characters" with exemplar.ExemplarMaxLabelSetLength = 128.
static EXEMPLAR_LABEL_LENGTH: Sentinel =
    Sentinel::new("label length for exemplar exceeds maximum of 128 UTF-8 characters");
pub const ERR_EXEMPLAR_LABEL_LENGTH: Error = Error::Sentinel(&EXEMPLAR_LABEL_LENGTH);
static EXEMPLARS_DISABLED: Sentinel =
    Sentinel::new("exemplar storage is disabled or max exemplars is less than or equal to 0");
pub const ERR_EXEMPLARS_DISABLED: Error = Error::Sentinel(&EXEMPLARS_DISABLED);
static NATIVE_HISTOGRAMS_DISABLED: Sentinel = Sentinel::new("native histograms are disabled");
pub const ERR_NATIVE_HISTOGRAMS_DISABLED: Error = Error::Sentinel(&NATIVE_HISTOGRAMS_DISABLED);
static OUT_OF_ORDER_ST: Sentinel = Sentinel::new("start timestamp out of order, ignoring");
/// A start timestamp older than a newer sample (appender v1 only).
pub const ERR_OUT_OF_ORDER_ST: Error = Error::Sentinel(&OUT_OF_ORDER_ST);
static ST_NEWER_THAN_SAMPLE: Sentinel = Sentinel::new("ST is newer or the same as sample's timestamp, ignoring");
pub const ERR_ST_NEWER_THAN_SAMPLE: Error = Error::Sentinel(&ST_NEWER_THAN_SAMPLE);

const _: () = assert!(model::exemplar::EXEMPLAR_MAX_LABEL_SET_LENGTH == 128);

impl Error {
    /// Go `errors.New(msg)` (a new error value).
    pub fn msg(msg: impl Into<Arc<str>>) -> Error {
        Error::Msg(msg.into())
    }

    /// Go `fmt.Errorf("<prefix>: %w", source)`.
    pub fn wrap(prefix: impl fmt::Display, source: Error) -> Error {
        Error::Wrapped(Arc::new((format!("{}: {}", prefix, source), source)))
    }

    /// Go `fmt.Errorf(format, ..., source)` where `msg` is the complete formatted message.
    pub fn wrapped(msg: String, source: Error) -> Error {
        Error::Wrapped(Arc::new((msg, source)))
    }

    /// Wraps any Rust error.
    pub fn other(e: impl std::error::Error + Send + Sync + 'static) -> Error {
        Error::Other(Arc::new(e))
    }

    /// Go `errors.Join(errs...)`: `None` when every error is `None`.
    pub fn join<I: IntoIterator<Item = Option<Error>>>(errs: I) -> Option<Error> {
        let v: Vec<Error> = errs.into_iter().flatten().collect();
        if v.is_empty() { None } else { Some(Error::Join(v.into())) }
    }

    /// The wrapped error (Go `errors.Unwrap`). Joined errors have more than one; see [`Error::is`].
    pub fn unwrap_once(&self) -> Option<&Error> {
        match self {
            Error::Wrapped(w) => Some(&w.1),
            Error::Annotation(a) => a.unwrap(),
            _ => None,
        }
    }

    fn identical(&self, target: &Error) -> bool {
        match (self, target) {
            (Error::Sentinel(a), Error::Sentinel(b)) => std::ptr::eq(*a, *b),
            // Go compares the struct values (`==`), so NaN values never compare equal.
            (Error::DuplicateSample(a), Error::DuplicateSample(b)) => a == b,
            (Error::AppendPartial(a), Error::AppendPartial(b)) => Arc::ptr_eq(a, b),
            (Error::Chunk(a), Error::Chunk(b)) => a == b,
            (Error::Wrapped(a), Error::Wrapped(b)) => Arc::ptr_eq(a, b),
            (Error::Join(a), Error::Join(b)) => Arc::ptr_eq(a, b),
            (Error::Msg(a), Error::Msg(b)) => Arc::ptr_eq(a, b),
            (Error::Annotation(a), Error::Annotation(b)) => {
                std::ptr::addr_eq(Arc::as_ptr(a), Arc::as_ptr(b))
            }
            (Error::Other(a), Error::Other(b)) => std::ptr::addr_eq(Arc::as_ptr(a), Arc::as_ptr(b)),
            _ => false,
        }
    }

    /// Go `errors.Is(self, target)`.
    pub fn is(&self, target: &Error) -> bool {
        if self.identical(target) {
            return true;
        }
        match self {
            Error::DuplicateSample(e) => {
                if e.is(target) {
                    return true;
                }
            }
            Error::AppendPartial(_) => {
                if matches!(target, Error::AppendPartial(_)) {
                    return true;
                }
            }
            _ => {}
        }
        match self {
            Error::Sentinel(s) => match s.parent {
                Some(p) => Error::Sentinel(p).is(target),
                None => false,
            },
            Error::Wrapped(w) => w.1.is(target),
            Error::Join(v) => v.iter().any(|e| e.is(target)),
            Error::Annotation(a) => a.unwrap().is_some_and(|e| e.is(target)),
            _ => false,
        }
    }

    /// Go `errors.As` for the `Other` variant: the first wrapped error of type `T`.
    pub fn downcast_ref<T: std::error::Error + 'static>(&self) -> Option<&T> {
        match self {
            Error::Other(e) => (&**e as &(dyn std::error::Error + 'static)).downcast_ref::<T>(),
            Error::Wrapped(w) => w.1.downcast_ref::<T>(),
            Error::Join(v) => v.iter().find_map(|e| e.downcast_ref::<T>()),
            _ => None,
        }
    }

    /// Go `errors.As(err, &*AppendPartialError)`.
    pub fn as_append_partial(&self) -> Option<&AppendPartialError> {
        match self {
            Error::AppendPartial(p) => Some(p),
            Error::Wrapped(w) => w.1.as_append_partial(),
            Error::Join(v) => v.iter().find_map(|e| e.as_append_partial()),
            _ => None,
        }
    }

    /// The annotation behind this error (Go `errors.As` to an annotation type).
    pub fn as_annotation(&self) -> Option<&dyn AnnoError> {
        match self {
            Error::Annotation(a) => Some(&**a),
            _ => None,
        }
    }

    /// Downcasts an annotation error to its concrete type.
    pub fn annotation_as<T: Any>(&self) -> Option<&T> {
        self.as_annotation().and_then(|a| (a as &dyn Any).downcast_ref::<T>())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Sentinel(s) => f.write_str(s.msg),
            Error::DuplicateSample(e) => fmt::Display::fmt(e, f),
            Error::AppendPartial(e) => fmt::Display::fmt(&**e, f),
            Error::Chunk(e) => fmt::Display::fmt(e, f),
            Error::Wrapped(w) => f.write_str(&w.0),
            Error::Join(v) => {
                for (i, e) in v.iter().enumerate() {
                    if i > 0 {
                        f.write_str("\n")?;
                    }
                    fmt::Display::fmt(e, f)?;
                }
                Ok(())
            }
            Error::Msg(s) => f.write_str(s),
            Error::Annotation(a) => f.write_str(&a.error("")),
            Error::Other(e) => fmt::Display::fmt(&**e, f),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error({:?})", self.to_string())
    }
}

impl std::error::Error for Error {}

/// Two errors are equal when they are the same kind of error with the same text (Go tests compare errors with
/// `reflect.DeepEqual`).
impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other) && self.to_string() == other.to_string()
    }
}

impl From<chunkenc::Error> for Error {
    fn from(e: chunkenc::Error) -> Error {
        Error::Chunk(e)
    }
}

impl From<&'static Sentinel> for Error {
    fn from(s: &'static Sentinel) -> Error {
        Error::Sentinel(s)
    }
}

impl From<model::histogram::Error> for Error {
    fn from(e: model::histogram::Error) -> Error {
        Error::Chunk(chunkenc::Error::Histogram(e))
    }
}

/// Converts a storage error to a chunk iterator error (iterators of `chunkenc` carry `chunkenc::Error`).
pub fn to_chunk_error(e: &Error) -> chunkenc::Error {
    match e {
        Error::Chunk(c) => c.clone(),
        other => chunkenc::Error::Msg(Cow::Owned(other.to_string())),
    }
}

/// Go `errDuplicateSampleForTimestamp`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DuplicateSampleError {
    pub timestamp: i64,
    pub existing: f64,
    pub existing_is_histogram: bool,
    pub new_value: f64,
}

impl DuplicateSampleError {
    /// The zero value (Go `ErrDuplicateSampleForTimestamp`).
    pub const ZERO: DuplicateSampleError =
        DuplicateSampleError { timestamp: 0, existing: 0.0, existing_is_histogram: false, new_value: 0.0 };

    /// Go `Is`: every duplicate sample error is `ErrDuplicateSampleForTimestamp`; otherwise the values must match.
    fn is(&self, target: &Error) -> bool {
        match target {
            Error::DuplicateSample(t) => *t == DuplicateSampleError::ZERO || self == t,
            _ => false,
        }
    }
}

impl fmt::Display for DuplicateSampleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.timestamp == 0 {
            return f.write_str("duplicate sample for timestamp");
        }
        if self.existing_is_histogram {
            return write!(
                f,
                "duplicate sample for timestamp {}; overrides not allowed: existing is a histogram, new value {}",
                self.timestamp,
                model::histogram::gofmt::g(self.new_value)
            );
        }
        write!(
            f,
            "duplicate sample for timestamp {}; overrides not allowed: existing {}, new value {}",
            self.timestamp,
            model::histogram::gofmt::g(self.existing),
            model::histogram::gofmt::g(self.new_value)
        )
    }
}

/// Go `NewDuplicateFloatErr`.
pub fn new_duplicate_float_err(t: i64, existing: f64, new_value: f64) -> Error {
    Error::DuplicateSample(DuplicateSampleError { timestamp: t, existing, existing_is_histogram: false, new_value })
}

/// Go `NewDuplicateHistogramToFloatErr`: a new float sample for the same timestamp as a previous histogram.
pub fn new_duplicate_histogram_to_float_err(t: i64, new_value: f64) -> Error {
    Error::DuplicateSample(DuplicateSampleError { timestamp: t, existing: 0.0, existing_is_histogram: true, new_value })
}

/// Why an exemplar was not written (Go `ExemplarError`).
#[derive(Clone, Debug, PartialEq)]
pub struct ExemplarError {
    /// The position of the exemplar in the slice given to the append call.
    pub index: usize,
    /// Why the exemplar was not written.
    pub err: Error,
}

impl ExemplarError {
    /// Go `errors.Is(exemplarError, target)` (it unwraps to `err`).
    pub fn is(&self, target: &Error) -> bool {
        self.err.is(target)
    }
}

impl fmt::Display for ExemplarError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.err, f)
    }
}

/// An append that wrote the sample, but not all of its exemplars (Go `AppendPartialError`).
///
/// Rust note: Go returns the series reference next to this error; here the error carries it in `series_ref`
/// (set by the appender), so that `Result<SeriesRef, Error>` loses nothing.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AppendPartialError {
    /// The reference of the series the sample was written to.
    pub series_ref: crate::SeriesRef,
    /// The exemplars that were not written. Several entries can share an index.
    pub exemplar_errors: Vec<ExemplarError>,
}

impl AppendPartialError {
    /// Go `ToError`: `None` if there are no exemplar errors.
    pub fn to_error(this: Option<&AppendPartialError>) -> Option<Error> {
        match this {
            Some(e) if !e.exemplar_errors.is_empty() => Some(Error::AppendPartial(Arc::new(e.clone()))),
            _ => None,
        }
    }

    /// Go `Handle`: if `err` is a partial error, its exemplar errors are added to `this` (allocated lazily) and
    /// `None` is returned; any other error is returned unchanged.
    pub fn handle(this: &mut Option<AppendPartialError>, err: Option<Error>) -> Option<Error> {
        let err = err?;
        let Some(p) = err.as_append_partial() else {
            return Some(err);
        };
        let e = this.get_or_insert_with(AppendPartialError::default);
        if e.series_ref == 0 {
            e.series_ref = p.series_ref;
        }
        e.exemplar_errors.extend(p.exemplar_errors.iter().cloned());
        None
    }
}

impl fmt::Display for AppendPartialError {
    /// The joined messages of the exemplar errors (Go `errors.Join`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, e) in self.exemplar_errors.iter().enumerate() {
            if i > 0 {
                f.write_str("\n")?;
            }
            fmt::Display::fmt(&e.err, f)?;
        }
        Ok(())
    }
}
