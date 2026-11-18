//! Native histograms (port of `model/histogram`): integer [`Histogram`], [`FloatHistogram`], spans, bucket
//! iterators, schemas (exponential -4..=8 and custom buckets -53), compaction, validation, resolution reduction,
//! arithmetic and the NHCB to classic conversion.

mod convert;
mod float_histogram;
pub mod gofmt;
#[allow(clippy::module_inception)]
mod histogram;
pub mod kahansum;
mod test_utils;

pub use convert::*;
pub use float_histogram::*;
pub use histogram::*;
pub use test_utils::*;

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

pub const EXPONENTIAL_SCHEMA_MAX: i32 = 8;
pub const EXPONENTIAL_SCHEMA_MAX_RESERVED: i32 = 52;
pub const EXPONENTIAL_SCHEMA_MIN: i32 = -4;
pub const EXPONENTIAL_SCHEMA_MIN_RESERVED: i32 = -9;
pub const CUSTOM_BUCKETS_SCHEMA: i32 = -53;

/// Custom bucket bounds. Interned: shared by reference and never mutated. `None` is Go's nil slice.
pub type CustomValues = Option<Arc<[f64]>>;

/// Makes [`CustomValues`] from a slice (`Some`, also for an empty slice, like Go's `[]float64{}`).
pub fn custom_values(v: &[f64]) -> CustomValues {
    Some(Arc::from(v))
}

/// CounterResetHint contains the known information about a counter reset, or alternatively that we are dealing
/// with a gauge histogram, where counter resets do not apply.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum CounterResetHint {
    /// We cannot say if this histogram signals a counter reset or not.
    #[default]
    UnknownCounterReset = 0,
    /// There was definitely a counter reset starting from this histogram.
    CounterReset = 1,
    /// There was definitely no counter reset with this histogram.
    NotCounterReset = 2,
    /// This is a gauge histogram, where counter resets do not happen.
    GaugeType = 3,
}

impl CounterResetHint {
    /// Converts the Go byte value. Values above 3 map to `GaugeType`'s neighbours as Go would keep the raw byte;
    /// only 0..=3 occur.
    pub fn from_u8(b: u8) -> CounterResetHint {
        match b {
            1 => CounterResetHint::CounterReset,
            2 => CounterResetHint::NotCounterReset,
            3 => CounterResetHint::GaugeType,
            _ => CounterResetHint::UnknownCounterReset,
        }
    }
}

/// A Span defines a continuous sequence of buckets.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Span {
    /// Gap to previous span (always positive), or starting index for the 1st span (which can be negative).
    pub offset: i32,
    /// Length of the span.
    pub length: u32,
}

impl Span {
    pub const fn new(offset: i32, length: u32) -> Span {
        Span { offset, length }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Errors.

/// The sentinel errors of the histogram package (`ErrHistogram...` in Go).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    HistogramCountNotBigEnough,
    HistogramCountMismatch,
    HistogramNegativeCount,
    HistogramNegativeBucketCount,
    HistogramSpanNegativeOffset,
    HistogramSpansBucketsMismatch,
    HistogramSpanIndexOutOfRange,
    HistogramCustomBucketsMismatch,
    HistogramCustomBucketsInvalid,
    HistogramCustomBucketsInfinite,
    HistogramCustomBucketsNaN,
    HistogramsIncompatibleSchema,
    HistogramCustomBucketsZeroCount,
    HistogramCustomBucketsZeroThresh,
    HistogramCustomBucketsNegSpans,
    HistogramCustomBucketsNegBuckets,
    HistogramExpSchemaCustomBounds,
    HistogramsInvalidSchema,
    HistogramsUnknownSchema,
}

impl ErrorKind {
    /// The error text of the sentinel.
    pub fn message(self) -> &'static str {
        match self {
            ErrorKind::HistogramCountNotBigEnough => {
                "histogram's observation count should be at least the number of observations found in the buckets"
            }
            ErrorKind::HistogramCountMismatch => {
                "histogram's observation count should equal the number of observations found in the buckets (in absence of NaN)"
            }
            ErrorKind::HistogramNegativeCount => "histogram's observation count is negative",
            ErrorKind::HistogramNegativeBucketCount => "histogram has a bucket whose observation count is negative",
            ErrorKind::HistogramSpanNegativeOffset => "histogram has a span whose offset is negative",
            ErrorKind::HistogramSpansBucketsMismatch => {
                "histogram spans specify different number of buckets than provided"
            }
            ErrorKind::HistogramSpanIndexOutOfRange => "histogram has a span whose bucket index is out of range",
            ErrorKind::HistogramCustomBucketsMismatch => "histogram custom bounds are too few",
            ErrorKind::HistogramCustomBucketsInvalid => "histogram custom bounds must be in strictly increasing order",
            ErrorKind::HistogramCustomBucketsInfinite => "histogram custom bounds must be finite",
            ErrorKind::HistogramCustomBucketsNaN => "histogram custom bounds must not be NaN",
            ErrorKind::HistogramsIncompatibleSchema => {
                "cannot apply this operation on histograms with a mix of exponential and custom bucket schemas"
            }
            ErrorKind::HistogramCustomBucketsZeroCount => "custom buckets: must have zero count of 0",
            ErrorKind::HistogramCustomBucketsZeroThresh => "custom buckets: must have zero threshold of 0",
            ErrorKind::HistogramCustomBucketsNegSpans => "custom buckets: must not have negative spans",
            ErrorKind::HistogramCustomBucketsNegBuckets => "custom buckets: must not have negative buckets",
            ErrorKind::HistogramExpSchemaCustomBounds => "histogram with exponential schema must not have custom bounds",
            ErrorKind::HistogramsInvalidSchema => {
                "histogram has an invalid schema, which must be between -4 and 8 for exponential buckets, or -53 for custom buckets"
            }
            ErrorKind::HistogramsUnknownSchema => {
                "histogram has an unknown schema, which must be between -9 and 52 for exponential buckets, or -53 for custom buckets"
            }
        }
    }
}

/// A histogram error. `kind` is the wrapped sentinel (Go `errors.Is`); an error with a kind is also a Go
/// `histogram.Error` for `errors.As`. Errors without a kind are plain Go errors (programming errors).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    kind: Option<ErrorKind>,
    msg: Cow<'static, str>,
}

impl Error {
    /// The sentinel error itself.
    pub fn new(kind: ErrorKind) -> Error {
        Error { kind: Some(kind), msg: Cow::Borrowed(kind.message()) }
    }
    /// `fmt.Errorf("<prefix>%w", sentinel)` style: `text` is the full text.
    pub fn wrapped(kind: ErrorKind, text: String) -> Error {
        Error { kind: Some(kind), msg: Cow::Owned(text) }
    }
    /// An error that wraps no sentinel (Go `errors.New`).
    pub fn plain(text: impl Into<Cow<'static, str>>) -> Error {
        Error { kind: None, msg: text.into() }
    }
    /// `fmt.Errorf("<prefix>%w", self)`.
    pub fn with_prefix(self, prefix: &str) -> Error {
        Error { kind: self.kind, msg: Cow::Owned(format!("{}{}", prefix, self.msg)) }
    }
    /// The wrapped sentinel, if any.
    pub fn kind(&self) -> Option<ErrorKind> {
        self.kind
    }
    /// Go `errors.Is(err, sentinel)`.
    pub fn is(&self, kind: ErrorKind) -> bool {
        self.kind == Some(kind)
    }
    /// Go `errors.As(err, &histogram.Error{})`.
    pub fn is_histogram_error(&self) -> bool {
        self.kind.is_some()
    }
    pub fn message(&self) -> &str {
        &self.msg
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for Error {}

/// Go `InvalidSchemaError`.
pub fn invalid_schema_error(s: i32) -> Error {
    Error::wrapped(ErrorKind::HistogramsInvalidSchema, format!("{}, got schema {}", ErrorKind::HistogramsInvalidSchema.message(), s))
}

/// Go `UnknownSchemaError`.
pub fn unknown_schema_error(s: i32) -> Error {
    Error::wrapped(ErrorKind::HistogramsUnknownSchema, format!("{}, got schema {}", ErrorKind::HistogramsUnknownSchema.message(), s))
}

pub fn is_custom_buckets_schema(s: i32) -> bool {
    s == CUSTOM_BUCKETS_SCHEMA
}

pub fn is_exponential_schema(s: i32) -> bool {
    (EXPONENTIAL_SCHEMA_MIN..=EXPONENTIAL_SCHEMA_MAX).contains(&s)
}

pub fn is_exponential_schema_reserved(s: i32) -> bool {
    (EXPONENTIAL_SCHEMA_MIN_RESERVED..=EXPONENTIAL_SCHEMA_MAX_RESERVED).contains(&s)
}

pub fn is_valid_schema(s: i32) -> bool {
    is_custom_buckets_schema(s) || is_exponential_schema(s)
}

/// Returns true if we know and accept the schema, but may need to reduce resolution to the nearest supported schema.
pub fn is_known_schema(s: i32) -> bool {
    is_custom_buckets_schema(s) || is_exponential_schema_reserved(s)
}

/// Compares histogram custom bucket bounds and returns true if all values match (by `==`).
pub fn custom_bucket_bounds_match(c1: &[f64], c2: &[f64]) -> bool {
    c1.len() == c2.len() && c1.iter().zip(c2).all(|(a, b)| a == b)
}

#[inline]
pub(crate) fn cv_slice(cv: &CustomValues) -> &[f64] {
    match cv {
        Some(a) => a,
        None => &[],
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Buckets and iterators.

/// The count type of a [`Bucket`]: u64 (Histogram) or f64 (FloatHistogram).
pub trait BucketCount: Copy + PartialEq + PartialOrd + Default + fmt::Debug {
    /// Go `%v`.
    fn write_v(self, out: &mut String);
    fn to_f64(self) -> f64;
}

impl BucketCount for u64 {
    fn write_v(self, out: &mut String) {
        use std::fmt::Write;
        let _ = write!(out, "{}", self);
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
}

impl BucketCount for f64 {
    fn write_v(self, out: &mut String) {
        gofmt::write_g(out, self);
    }
    fn to_f64(self) -> f64 {
        self
    }
}

/// The internal bucket count type: i64 deltas (Histogram) or f64 absolute counts (FloatHistogram). Integer
/// arithmetic wraps like Go's.
pub trait InternalBucketCount: Copy + PartialEq + PartialOrd + Default + fmt::Debug {
    const ZERO: Self;
    fn add(self, o: Self) -> Self;
    fn sub(self, o: Self) -> Self;
    fn neg(self) -> Self;
}

impl InternalBucketCount for i64 {
    const ZERO: i64 = 0;
    #[inline]
    fn add(self, o: i64) -> i64 {
        self.wrapping_add(o)
    }
    #[inline]
    fn sub(self, o: i64) -> i64 {
        self.wrapping_sub(o)
    }
    #[inline]
    fn neg(self) -> i64 {
        self.wrapping_neg()
    }
}

impl InternalBucketCount for f64 {
    const ZERO: f64 = 0.0;
    #[inline]
    fn add(self, o: f64) -> f64 {
        self + o
    }
    #[inline]
    fn sub(self, o: f64) -> f64 {
        self - o
    }
    #[inline]
    fn neg(self) -> f64 {
        -self
    }
}

/// A bucket with lower and upper limit and the absolute count of samples in the bucket. To represent cumulative
/// buckets, `lower` is -Inf and the count is cumulative.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Bucket<BC> {
    pub lower: f64,
    pub upper: f64,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
    pub count: BC,
    /// Index within schema. Irrelevant for the zero bucket.
    pub index: i32,
}

impl<BC: BucketCount> Bucket<BC> {
    /// Go `Bucket.String()`: "[lower,upper]:count" with '(' / ')' for non-inclusive bounds.
    pub fn write_to(&self, out: &mut String) {
        out.push(if self.lower_inclusive { '[' } else { '(' });
        gofmt::write_g(out, self.lower);
        out.push(',');
        gofmt::write_g(out, self.upper);
        out.push(if self.upper_inclusive { ']' } else { ')' });
        out.push(':');
        self.count.write_v(out);
    }

    /// Returns the fraction (between 0 and 1) of this bucket's width that lies at or below `v`, assuming `v` is
    /// strictly inside the bucket's finite bounds. Linear interpolation when `linear`, else exponential.
    pub fn fraction_below(&self, v: f64, linear: bool) -> f64 {
        if linear {
            return (v - self.lower) / (self.upper - self.lower);
        }
        let log_lower = self.lower.abs().log2();
        let log_upper = self.upper.abs().log2();
        let log_v = v.abs().log2();
        if v > 0.0 {
            return (log_v - log_lower) / (log_upper - log_lower);
        }
        1.0 - ((log_v - log_upper) / (log_lower - log_upper))
    }
}

impl<BC: BucketCount> fmt::Display for Bucket<BC> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::new();
        self.write_to(&mut s);
        f.write_str(&s)
    }
}

/// Bucket without bounds (cheaper to compute).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct StrippedBucket<BC> {
    pub count: BC,
    pub index: i32,
}

/// Iterates over the buckets of a histogram, returning decoded buckets (Go style: `next` then `at`).
pub trait BucketIterator<BC> {
    /// Advances the iterator by one.
    fn next(&mut self) -> bool;
    /// Returns the current bucket.
    fn at(&self) -> Bucket<BC>;
}

impl<BC, T: BucketIterator<BC> + ?Sized> BucketIterator<BC> for Box<T> {
    fn next(&mut self) -> bool {
        (**self).next()
    }
    fn at(&self) -> Bucket<BC> {
        (**self).at()
    }
}

/// Bounds of bucket `idx` in `schema` (Go `baseBucketIterator.at` without the count).
#[inline]
pub(crate) fn bucket_bounds(idx: i32, schema: i32, positive: bool, custom_values: &[f64]) -> (f64, f64, bool, bool) {
    let (lower, upper) = if positive {
        (get_bound(idx - 1, schema, custom_values), get_bound(idx, schema, custom_values))
    } else {
        (-get_bound(idx, schema, custom_values), -get_bound(idx - 1, schema, custom_values))
    };
    let (li, ui) = if is_custom_buckets_schema(schema) { (idx == 0, true) } else { (lower < 0.0, upper > 0.0) };
    (lower, upper, li, ui)
}

#[inline]
pub(crate) fn make_bucket<BC>(idx: i32, schema: i32, positive: bool, custom_values: &[f64], count: BC) -> Bucket<BC> {
    let (lower, upper, lower_inclusive, upper_inclusive) = bucket_bounds(idx, schema, positive, custom_values);
    Bucket { lower, upper, lower_inclusive, upper_inclusive, count, index: idx }
}

// ---------------------------------------------------------------------------------------------------------------
// Compaction.

/// Generic compaction used by `Histogram::compact` and `FloatHistogram::compact`. `delta_buckets` is true if the
/// buckets are deltas. `compensation` (Kahan compensation buckets, float histograms only) is processed in parallel.
/// Go's nil compensation slice is `None`.
pub fn compact_buckets<I: InternalBucketCount>(
    primary: &mut Vec<I>,
    mut compensation: Option<&mut Vec<f64>>,
    spans: &mut Vec<Span>,
    max_empty_buckets: usize,
    delta_buckets: bool,
) {
    if delta_buckets && compensation.is_some() {
        panic!("histogram type mismatch: deltaBuckets cannot be true when compensationBuckets is provided");
    } else if let Some(c) = &compensation
        && primary.len() != c.len()
    {
        panic!(
            "primary buckets layout ({:?}) mismatch against associated compensation buckets layout ({:?})",
            primary, c
        );
    }
    let max_empty = max_empty_buckets.min(i64::MAX as usize) as i64;
    // Fast path: nothing to do if there are no empty buckets, no small offsets and no zero-length spans.
    let mut nothing_to_do = true;
    let mut current = I::ZERO;
    for &b in primary.iter() {
        if delta_buckets {
            current = current.add(b);
        } else {
            current = b;
        }
        if current == I::ZERO {
            nothing_to_do = false;
            break;
        }
    }
    if nothing_to_do {
        for s in spans.iter() {
            if (s.offset as i64) <= max_empty || s.length == 0 {
                nothing_to_do = false;
                break;
            }
        }
        if nothing_to_do {
            return;
        }
    }

    let mut i_bucket: usize = 0;
    let mut i_span: usize = 0;
    let mut pos_in_span: u32 = 0;
    current = I::ZERO;

    // Merge spans with zero-offset to avoid special cases later.
    if spans.len() > 1 {
        for i in 1..spans.len() {
            let span = spans[i];
            if span.offset == 0 {
                spans[i_span].length = spans[i_span].length.wrapping_add(span.length);
                continue;
            }
            i_span += 1;
            if i != i_span {
                spans[i_span] = span;
            }
        }
        spans.truncate(i_span + 1);
        i_span = 0;
    }

    // Merge spans with zero-length to avoid special cases later.
    for i in 0..spans.len() {
        let span = spans[i];
        if span.length == 0 {
            if i + 1 < spans.len() {
                spans[i + 1].offset = spans[i + 1].offset.wrapping_add(span.offset);
            }
            continue;
        }
        if i != i_span {
            spans[i_span] = span;
        }
        i_span += 1;
    }
    spans.truncate(i_span);
    i_span = 0;

    // If all spans were zero-length, no buckets remain valid.
    if spans.is_empty() {
        if let Some(c) = compensation.as_deref_mut() {
            c.clear();
        }
        primary.clear();
        return;
    }

    // Cut out empty buckets from start and end of spans, no matter what. Also cut out empty buckets from the middle
    // of a span but only if there are more than max_empty_buckets consecutive empty buckets.
    while i_bucket < primary.len() && i_span < spans.len() {
        if delta_buckets {
            current = current.add(primary[i_bucket]);
        } else {
            current = primary[i_bucket];
        }
        // emptyBucketsHere.
        let n_empty = {
            let mut i: usize = 0;
            let mut abs = current;
            let mut comp = match compensation.as_deref() {
                Some(c) => c[i_bucket],
                None => 0.0,
            };
            while (i as u32).wrapping_add(pos_in_span) < spans[i_span].length && abs == I::ZERO && comp == 0.0 {
                i += 1;
                if i + i_bucket >= primary.len() {
                    break;
                }
                abs = primary[i + i_bucket];
                if let Some(c) = compensation.as_deref() {
                    comp = c[i + i_bucket];
                }
            }
            i
        };
        if n_empty > 0 {
            if pos_in_span > 0
                && (n_empty as i64) < (spans[i_span].length.wrapping_sub(pos_in_span) as i64)
                && (n_empty as i64) <= max_empty
            {
                // The empty buckets are in the middle of a span, and there are few enough to not bother.
                i_bucket += n_empty;
                if delta_buckets {
                    current = I::ZERO;
                }
                pos_in_span += n_empty as u32;
                continue;
            }
            // In all other cases, we cut out the empty buckets.
            if delta_buckets && i_bucket + n_empty < primary.len() {
                current = primary[i_bucket].neg();
                primary[i_bucket + n_empty] = primary[i_bucket + n_empty].add(primary[i_bucket]);
            }
            primary.drain(i_bucket..i_bucket + n_empty);
            if let Some(c) = compensation.as_deref_mut() {
                c.drain(i_bucket..i_bucket + n_empty);
            }
            if pos_in_span == 0 {
                // Start of span.
                if n_empty == spans[i_span].length as usize {
                    // The whole span is empty.
                    let offset = spans[i_span].offset;
                    spans.remove(i_span);
                    if spans.len() > i_span {
                        spans[i_span].offset =
                            spans[i_span].offset.wrapping_add(offset.wrapping_add(n_empty as i32));
                    }
                    continue;
                }
                spans[i_span].length -= n_empty as u32;
                spans[i_span].offset = spans[i_span].offset.wrapping_add(n_empty as i32);
                continue;
            }
            // It's in the middle or in the end of the span. Split the current span.
            let new_span = Span {
                offset: n_empty as i32,
                length: spans[i_span].length.wrapping_sub(pos_in_span).wrapping_sub(n_empty as u32),
            };
            spans[i_span].length = pos_in_span;
            // In any case, we have to split to the next span.
            i_span += 1;
            pos_in_span = 0;
            if new_span.length == 0 {
                // The span is empty, so we were already at the end of a span. Just adjust the next span's offset.
                if i_span < spans.len() {
                    spans[i_span].offset = spans[i_span].offset.wrapping_add(n_empty as i32);
                }
                continue;
            }
            // Insert the new span.
            spans.insert(i_span, new_span);
            continue;
        }
        i_bucket += 1;
        pos_in_span += 1;
        if pos_in_span >= spans[i_span].length {
            pos_in_span = 0;
            i_span += 1;
        }
    }
    if max_empty_buckets == 0 || primary.is_empty() {
        return;
    }

    // Finally, check if any offsets between spans are small enough to merge the spans.
    i_bucket = spans[0].length as usize;
    if delta_buckets {
        current = I::ZERO;
        for &b in &primary[..i_bucket] {
            current = current.add(b);
        }
    }
    i_span = 1;
    while i_span < spans.len() {
        if (spans[i_span].offset as i64) > max_empty {
            let l = spans[i_span].length as usize;
            if delta_buckets {
                for &b in &primary[i_bucket..i_bucket + l] {
                    current = current.add(b);
                }
            }
            i_bucket += l;
            i_span += 1;
            continue;
        }
        // Merge span with previous one and insert empty buckets.
        let offset = spans[i_span].offset as usize;
        let l = spans[i_span].length as usize;
        spans[i_span - 1].length = spans[i_span - 1].length.wrapping_add(offset as u32).wrapping_add(spans[i_span].length);
        spans.remove(i_span);
        insert_zeros(primary, i_bucket, offset, I::ZERO);
        if delta_buckets {
            // Go: new[iBucket] = -current (overwrites), new[iBucket+offset] += current.
            primary[i_bucket] = current.neg();
            primary[i_bucket + offset] = primary[i_bucket + offset].add(current);
        }
        if let Some(c) = compensation.as_deref_mut() {
            insert_zeros(c, i_bucket, offset, 0.0);
        }
        i_bucket += offset;
        // The buckets of the merged span are now part of the previous span.
        if delta_buckets {
            current = I::ZERO;
            for &b in &primary[i_bucket..i_bucket + l] {
                current = current.add(b);
            }
        }
        i_bucket += l;
    }
}

/// Inserts `n` copies of `zero` at position `at`.
fn insert_zeros<T: Copy>(v: &mut Vec<T>, at: usize, n: usize, zero: T) {
    let len = v.len();
    v.resize(len + n, zero);
    v.copy_within(at..len, at + n);
    for x in &mut v[at..at + n] {
        *x = zero;
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Validation helpers.

pub(crate) fn check_histogram_spans(spans: &[Span], num_buckets: usize) -> Result<(), Error> {
    let mut span_buckets: usize = 0;
    // Bucket indexes are i32, so track the index in i64 to detect offsets that add up past the i32 range.
    let mut end_idx: i64 = 0;
    for (n, span) in spans.iter().enumerate() {
        if n > 0 && span.offset < 0 {
            return Err(Error::wrapped(
                ErrorKind::HistogramSpanNegativeOffset,
                format!("span number {} with offset {}: {}", n + 1, span.offset, ErrorKind::HistogramSpanNegativeOffset.message()),
            ));
        } else if n == 0 && span.offset == i32::MIN {
            return Err(Error::wrapped(
                ErrorKind::HistogramSpanIndexOutOfRange,
                format!(
                    "span number {} starts at bucket index {}: {}",
                    n + 1,
                    span.offset,
                    ErrorKind::HistogramSpanIndexOutOfRange.message()
                ),
            ));
        }
        span_buckets += span.length as usize;
        end_idx += span.offset as i64 + span.length as i64;
        if end_idx - 1 > i32::MAX as i64 {
            return Err(Error::wrapped(
                ErrorKind::HistogramSpanIndexOutOfRange,
                format!(
                    "span number {} ends at bucket index {}: {}",
                    n + 1,
                    end_idx - 1,
                    ErrorKind::HistogramSpanIndexOutOfRange.message()
                ),
            ));
        }
    }
    if span_buckets != num_buckets {
        return Err(Error::wrapped(
            ErrorKind::HistogramSpansBucketsMismatch,
            format!(
                "spans need {} buckets, have {} buckets: {}",
                span_buckets,
                num_buckets,
                ErrorKind::HistogramSpansBucketsMismatch.message()
            ),
        ));
    }
    Ok(())
}

/// Integer buckets (deltas): sums absolute counts into `count`, errors on a negative absolute count.
pub(crate) fn check_histogram_buckets_int(buckets: &[i64], count: &mut u64, deltas: bool) -> Result<(), Error> {
    let mut last: i64 = 0;
    for (i, &b) in buckets.iter().enumerate() {
        let c = if deltas { last.wrapping_add(b) } else { b };
        if c < 0 {
            return Err(Error::wrapped(
                ErrorKind::HistogramNegativeBucketCount,
                format!("bucket number {} has observation count of {}: {}", i + 1, c, ErrorKind::HistogramNegativeBucketCount.message()),
            ));
        }
        last = c;
        *count = count.wrapping_add(c as u64);
    }
    Ok(())
}

/// Float buckets (absolute): sums counts into `count`, errors on a negative count.
pub(crate) fn check_histogram_buckets_float(buckets: &[f64], count: &mut f64, deltas: bool) -> Result<(), Error> {
    let mut last: f64 = 0.0;
    for (i, &b) in buckets.iter().enumerate() {
        let c = if deltas { last + b } else { b };
        if c < 0.0 {
            return Err(Error::wrapped(
                ErrorKind::HistogramNegativeBucketCount,
                format!(
                    "bucket number {} has observation count of {}: {}",
                    i + 1,
                    gofmt::g(c),
                    ErrorKind::HistogramNegativeBucketCount.message()
                ),
            ));
        }
        last = c;
        *count += c;
    }
    Ok(())
}

pub(crate) fn check_histogram_custom_bounds(bounds: &[f64], spans: &[Span], num_buckets: usize) -> Result<(), Error> {
    let mut prev = f64::NEG_INFINITY;
    for (i, &curr) in bounds.iter().enumerate() {
        if curr.is_nan() {
            return Err(Error::new(ErrorKind::HistogramCustomBucketsNaN));
        }
        if i > 0 && curr <= prev {
            return Err(Error::wrapped(
                ErrorKind::HistogramCustomBucketsInvalid,
                format!(
                    "previous bound is {} and current is {}: {}",
                    gofmt::f6(prev),
                    gofmt::f6(curr),
                    ErrorKind::HistogramCustomBucketsInvalid.message()
                ),
            ));
        }
        prev = curr;
    }
    if prev == f64::INFINITY {
        return Err(Error::wrapped(
            ErrorKind::HistogramCustomBucketsInfinite,
            format!("last +Inf bound must not be explicitly defined: {}", ErrorKind::HistogramCustomBucketsInfinite.message()),
        ));
    }

    let mut span_buckets: usize = 0;
    let mut total_span_length: i64 = 0;
    for (n, span) in spans.iter().enumerate() {
        if span.offset < 0 {
            return Err(Error::wrapped(
                ErrorKind::HistogramSpanNegativeOffset,
                format!("span number {} with offset {}: {}", n + 1, span.offset, ErrorKind::HistogramSpanNegativeOffset.message()),
            ));
        }
        span_buckets += span.length as usize;
        total_span_length += span.length as i64 + span.offset as i64;
    }
    if span_buckets != num_buckets {
        return Err(Error::wrapped(
            ErrorKind::HistogramSpansBucketsMismatch,
            format!(
                "spans need {} buckets, have {} buckets: {}",
                span_buckets,
                num_buckets,
                ErrorKind::HistogramSpansBucketsMismatch.message()
            ),
        ));
    }
    if (bounds.len() as i64 + 1) < total_span_length {
        return Err(Error::wrapped(
            ErrorKind::HistogramCustomBucketsMismatch,
            format!(
                "only {} custom bounds defined which is insufficient to cover total span length of {}: {}",
                bounds.len(),
                total_span_length,
                ErrorKind::HistogramCustomBucketsMismatch.message()
            ),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------------------------
// Bounds.

/// Upper bound of bucket `idx` (custom bounds for the custom buckets schema). Panics on an out-of-range custom index.
#[inline]
pub fn get_bound(idx: i32, schema: i32, custom_values: &[f64]) -> f64 {
    if is_custom_buckets_schema(schema) {
        let length = custom_values.len() as i32;
        if idx > length || idx < -1 {
            panic!("index {} out of bounds for custom bounds of length {}", idx, length);
        } else if idx == length {
            return f64::INFINITY;
        } else if idx == -1 {
            return f64::NEG_INFINITY;
        }
        return custom_values[idx as usize];
    }
    get_bound_exponential(idx, schema)
}

/// Go `math.Ldexp` (ported literally, including the rounding of denormal results).
#[inline]
#[doc(hidden)]
pub fn ldexp(frac: f64, exp: i64) -> f64 {
    if frac == 0.0 || frac.is_infinite() || frac.is_nan() {
        return frac;
    }
    // normalize
    let (frac, e) = if frac.abs() < f64::MIN_POSITIVE { (frac * (1u64 << 52) as f64, -52i64) } else { (frac, 0) };
    let mut exp = exp + e;
    let mut x = frac.to_bits();
    exp += ((x >> 52) & 0x7ff) as i64 - 1023;
    if exp < -1075 {
        return 0.0f64.copysign(frac); // Underflow.
    }
    if exp > 1023 {
        return if frac < 0.0 { f64::NEG_INFINITY } else { f64::INFINITY };
    }
    let mut m = 1.0f64;
    if exp < -1022 {
        // Denormal.
        exp += 53;
        m = 1.0 / (1u64 << 53) as f64;
    }
    x &= !(0x7ffu64 << 52);
    x |= ((exp + 1023) as u64) << 52;
    m * f64::from_bits(x)
}

/// Upper bound of bucket `idx` in an exponential schema.
pub fn get_bound_exponential(idx: i32, schema: i32) -> f64 {
    // The last bucket counting regular numbers has math.MaxFloat64 as its upper bound (see Go comments).
    if schema < 0 {
        let exp = (idx as i64) << (-schema);
        if exp == 1024 {
            return f64::MAX;
        }
        return ldexp(1.0, exp);
    }
    let frac_idx = idx & ((1 << schema) - 1);
    let frac = EXPONENTIAL_BOUNDS[schema as usize][frac_idx as usize];
    let exp = ((idx as i64) >> schema) + 1;
    if frac == 0.5 && exp == 1025 {
        return f64::MAX;
    }
    ldexp(frac, exp)
}

/// Bucket index in the target schema for bucket index `idx` in the original schema.
#[inline]
pub fn target_idx(idx: i32, origin_schema: i32, target_schema: i32) -> i32 {
    // Go's signed shift by 32 or more fills with the sign bit (0 or -1), as a shift by 31 does. The reserved
    // schemas (up to 52) reduced to 8 shift by up to 44.
    (idx.wrapping_sub(1) >> (origin_schema - target_schema).min(31)).wrapping_add(1)
}

// ---------------------------------------------------------------------------------------------------------------
// Resolution reduction.

/// Reduces spans/buckets from `origin_schema` to the lower `target_schema` in place (Go `reduceResolution` with
/// inplace=true). On error, both vectors are cleared (Go returns nil slices).
pub fn reduce_resolution_in_place<I: InternalBucketCount>(
    spans: &mut Vec<Span>,
    buckets: &mut Vec<I>,
    origin_schema: i32,
    target_schema: i32,
    delta_buckets: bool,
) -> Result<(), Error> {
    let r = reduce_resolution_impl(spans, buckets, origin_schema, target_schema, delta_buckets);
    if r.is_err() {
        spans.clear();
        buckets.clear();
    }
    r
}

fn reduce_resolution_impl<I: InternalBucketCount>(
    spans: &mut Vec<Span>,
    buckets: &mut Vec<I>,
    origin_schema: i32,
    target_schema: i32,
    delta_buckets: bool,
) -> Result<(), Error> {
    // Target slices never grow faster than origin slices are read, so writing in place is safe.
    let n_spans = spans.len();
    let n_buckets = buckets.len();
    let mut ts: usize = 0; // Number of target spans written.
    let mut tb: usize = 0; // Number of target buckets written.
    let mut bucket_idx: i32 = 0;
    let mut bucket_count_idx: usize = 0;
    let mut last_bucket_count = I::ZERO;
    let mut last_target_bucket_idx: i32 = 0;
    let mut last_target_bucket_count = I::ZERO;

    for n in 0..n_spans {
        let span = spans[n];
        if n > 0 && span.offset < 0 {
            return Err(Error::wrapped(
                ErrorKind::HistogramSpanNegativeOffset,
                format!("span number {} with offset {}: {}", n + 1, span.offset, ErrorKind::HistogramSpanNegativeOffset.message()),
            ));
        }
        bucket_idx = bucket_idx.wrapping_add(span.offset);
        for _ in 0..span.length {
            if bucket_count_idx >= n_buckets {
                return Err(Error::wrapped(
                    ErrorKind::HistogramSpansBucketsMismatch,
                    format!("have {} buckets but spans need more: {}", n_buckets, ErrorKind::HistogramSpansBucketsMismatch.message()),
                ));
            }
            let origin = buckets[bucket_count_idx];
            let target_bucket_idx = target_idx(bucket_idx, origin_schema, target_schema);
            if ts == 0 {
                spans[0] = Span { offset: target_bucket_idx, length: 1 };
                ts = 1;
                buckets[0] = origin;
                tb = 1;
                last_target_bucket_idx = target_bucket_idx;
                last_bucket_count = origin;
                last_target_bucket_count = origin;
            } else if last_target_bucket_idx == target_bucket_idx {
                if delta_buckets {
                    last_bucket_count = last_bucket_count.add(origin);
                    buckets[tb - 1] = buckets[tb - 1].add(last_bucket_count);
                    last_target_bucket_count = last_target_bucket_count.add(last_bucket_count);
                } else {
                    buckets[tb - 1] = buckets[tb - 1].add(origin);
                }
            } else if last_target_bucket_idx.wrapping_add(1) == target_bucket_idx {
                spans[ts - 1].length += 1;
                last_target_bucket_idx = last_target_bucket_idx.wrapping_add(1);
                if delta_buckets {
                    last_bucket_count = last_bucket_count.add(origin);
                    buckets[tb] = last_bucket_count.sub(last_target_bucket_count);
                    last_target_bucket_count = last_bucket_count;
                } else {
                    buckets[tb] = origin;
                }
                tb += 1;
            } else if last_target_bucket_idx.wrapping_add(1) < target_bucket_idx {
                spans[ts] = Span { offset: target_bucket_idx - last_target_bucket_idx - 1, length: 1 };
                ts += 1;
                last_target_bucket_idx = target_bucket_idx;
                if delta_buckets {
                    last_bucket_count = last_bucket_count.add(origin);
                    buckets[tb] = last_bucket_count.sub(last_target_bucket_count);
                    last_target_bucket_count = last_bucket_count;
                } else {
                    buckets[tb] = origin;
                }
                tb += 1;
            }
            bucket_idx = bucket_idx.wrapping_add(1);
            bucket_count_idx += 1;
        }
    }
    if bucket_count_idx != n_buckets {
        return Err(Error::wrapped(
            ErrorKind::HistogramSpansBucketsMismatch,
            format!(
                "spans need {} buckets, have {} buckets: {}",
                bucket_count_idx,
                n_buckets,
                ErrorKind::HistogramSpansBucketsMismatch.message()
            ),
        ));
    }
    spans.truncate(ts);
    buckets.truncate(tb);
    Ok(())
}

/// Go `reduceResolution` with inplace=false: returns new spans and buckets in the target schema.
pub fn reduce_resolution<I: InternalBucketCount>(
    origin_spans: &[Span],
    origin_buckets: &[I],
    origin_schema: i32,
    target_schema: i32,
    delta_buckets: bool,
) -> Result<(Vec<Span>, Vec<I>), Error> {
    let mut spans = origin_spans.to_vec();
    let mut buckets = origin_buckets.to_vec();
    reduce_resolution_in_place(&mut spans, &mut buckets, origin_schema, target_schema, delta_buckets)?;
    Ok((spans, buckets))
}

/// Like [`reduce_resolution`] but panics on error.
pub fn must_reduce_resolution<I: InternalBucketCount>(
    origin_spans: &[Span],
    origin_buckets: &[I],
    origin_schema: i32,
    target_schema: i32,
    delta_buckets: bool,
) -> (Vec<Span>, Vec<I>) {
    match reduce_resolution(origin_spans, origin_buckets, origin_schema, target_schema, delta_buckets) {
        Ok(r) => r,
        Err(e) => panic!("{}", e),
    }
}

/// Like [`reduce_resolution_in_place`] but panics on error.
pub fn must_reduce_resolution_in_place<I: InternalBucketCount>(
    spans: &mut Vec<Span>,
    buckets: &mut Vec<I>,
    origin_schema: i32,
    target_schema: i32,
    delta_buckets: bool,
) {
    if let Err(e) = reduce_resolution_in_place(spans, buckets, origin_schema, target_schema, delta_buckets) {
        panic!("{}", e);
    }
}

/// Go `clearIfNotNil` for a Vec: keeps the allocation, drops the content.
#[inline]
pub(crate) fn clear_vec<T>(v: &mut Vec<T>) {
    v.clear();
}

/// Copies `src` into `dst`, reusing `dst`'s allocation.
#[inline]
pub(crate) fn copy_into<T: Copy>(dst: &mut Vec<T>, src: &[T]) {
    dst.clear();
    dst.extend_from_slice(src);
}

/// Precalculated bucket bounds in the interval [0.5,1) in schema 0 to 8.
pub(crate) static EXPONENTIAL_BOUNDS: [&[f64]; 9] = [
    // Schema "0":
    &[0.5],
    // Schema 1:
    &[0.5, 0.7071067811865475],
    // Schema 2:
    &[0.5, 0.5946035575013605, 0.7071067811865475, 0.8408964152537144],
    // Schema 3:
    &[
        0.5, 0.5452538663326288, 0.5946035575013605, 0.6484197773255048,
        0.7071067811865475, 0.7711054127039704, 0.8408964152537144, 0.9170040432046711,
    ],
    // Schema 4:
    &[
        0.5, 0.5221368912137069, 0.5452538663326288, 0.5693943173783458,
        0.5946035575013605, 0.620928906036742, 0.6484197773255048, 0.6771277734684463,
        0.7071067811865475, 0.7384130729697496, 0.7711054127039704, 0.805245165974627,
        0.8408964152537144, 0.8781260801866495, 0.9170040432046711, 0.9576032806985735,
    ],
    // Schema 5:
    &[
        0.5, 0.5109485743270583, 0.5221368912137069, 0.5335702003384117,
        0.5452538663326288, 0.5571933712979462, 0.5693943173783458, 0.5818624293887887,
        0.5946035575013605, 0.6076236799902344, 0.620928906036742, 0.6345254785958666,
        0.6484197773255048, 0.6626183215798706, 0.6771277734684463, 0.6919549409819159,
        0.7071067811865475, 0.7225904034885232, 0.7384130729697496, 0.7545822137967112,
        0.7711054127039704, 0.7879904225539431, 0.805245165974627, 0.8228777390769823,
        0.8408964152537144, 0.8593096490612387, 0.8781260801866495, 0.8973545375015533,
        0.9170040432046711, 0.9370838170551498, 0.9576032806985735, 0.9785720620876999,
    ],
    // Schema 6:
    &[
        0.5, 0.5054446430258502, 0.5109485743270583, 0.5165124395106142,
        0.5221368912137069, 0.5278225891802786, 0.5335702003384117, 0.5393803988785598,
        0.5452538663326288, 0.5511912916539204, 0.5571933712979462, 0.5632608093041209,
        0.5693943173783458, 0.5755946149764913, 0.5818624293887887, 0.5881984958251406,
        0.5946035575013605, 0.6010783657263515, 0.6076236799902344, 0.6142402680534349,
        0.620928906036742, 0.6276903785123455, 0.6345254785958666, 0.6414350080393891,
        0.6484197773255048, 0.6554806057623822, 0.6626183215798706, 0.6698337620266515,
        0.6771277734684463, 0.6845012114872953, 0.6919549409819159, 0.6994898362691555,
        0.7071067811865475, 0.7148066691959849, 0.7225904034885232, 0.7304588970903234,
        0.7384130729697496, 0.7464538641456323, 0.7545822137967112, 0.762799075372269,
        0.7711054127039704, 0.7795022001189185, 0.7879904225539431, 0.7965710756711334,
        0.805245165974627, 0.8140137109286738, 0.8228777390769823, 0.8318382901633681,
        0.8408964152537144, 0.8500531768592616, 0.8593096490612387, 0.8686669176368529,
        0.8781260801866495, 0.8876882462632604, 0.8973545375015533, 0.9071260877501991,
        0.9170040432046711, 0.9269895625416926, 0.9370838170551498, 0.9472879907934827,
        0.9576032806985735, 0.9680308967461471, 0.9785720620876999, 0.9892280131939752,
    ],
    // Schema 7:
    &[
        0.5, 0.5027149505564014, 0.5054446430258502, 0.5081891574554764,
        0.5109485743270583, 0.5137229745593818, 0.5165124395106142, 0.5193170509806894,
        0.5221368912137069, 0.5249720429003435, 0.5278225891802786, 0.5306886136446309,
        0.5335702003384117, 0.5364674337629877, 0.5393803988785598, 0.5423091811066545,
        0.5452538663326288, 0.5482145409081883, 0.5511912916539204, 0.5541842058618393,
        0.5571933712979462, 0.5602188762048033, 0.5632608093041209, 0.5663192597993595,
        0.5693943173783458, 0.572486072215902, 0.5755946149764913, 0.5787200368168754,
        0.5818624293887887, 0.585021884841625, 0.5881984958251406, 0.5913923554921704,
        0.5946035575013605, 0.5978321960199137, 0.6010783657263515, 0.6043421618132907,
        0.6076236799902344, 0.6109230164863786, 0.6142402680534349, 0.6175755319684665,
        0.620928906036742, 0.6243004885946023, 0.6276903785123455, 0.6310986751971253,
        0.6345254785958666, 0.637970889198196, 0.6414350080393891, 0.6449179367033329,
        0.6484197773255048, 0.6519406325959679, 0.6554806057623822, 0.659039800633032,
        0.6626183215798706, 0.6662162735415805, 0.6698337620266515, 0.6734708931164728,
        0.6771277734684463, 0.6808045103191123, 0.6845012114872953, 0.688217985377265,
        0.6919549409819159, 0.6957121878859629, 0.6994898362691555, 0.7032879969095076,
        0.7071067811865475, 0.7109463010845827, 0.7148066691959849, 0.718687998724491,
        0.7225904034885232, 0.7265139979245261, 0.7304588970903234, 0.7344252166684908,
        0.7384130729697496, 0.7424225829363761, 0.7464538641456323, 0.7505070348132126,
        0.7545822137967112, 0.7586795205991071, 0.762799075372269, 0.7669409989204777,
        0.7711054127039704, 0.7752924388424999, 0.7795022001189185, 0.7837348199827764,
        0.7879904225539431, 0.7922691326262467, 0.7965710756711334, 0.8008963778413465,
        0.805245165974627, 0.8096175675974316, 0.8140137109286738, 0.8184337248834821,
        0.8228777390769823, 0.8273458838280969, 0.8318382901633681, 0.8363550898207981,
        0.8408964152537144, 0.8454623996346523, 0.8500531768592616, 0.8546688815502312,
        0.8593096490612387, 0.8639756154809185, 0.8686669176368529, 0.8733836930995842,
        0.8781260801866495, 0.8828942179666361, 0.8876882462632604, 0.8925083056594671,
        0.8973545375015533, 0.9022270839033115, 0.9071260877501991, 0.9120516927035263,
        0.9170040432046711, 0.9219832844793128, 0.9269895625416926, 0.9320230241988943,
        0.9370838170551498, 0.9421720895161669, 0.9472879907934827, 0.9524316709088368,
        0.9576032806985735, 0.9628029718180622, 0.9680308967461471, 0.9732872087896164,
        0.9785720620876999, 0.9838856116165875, 0.9892280131939752, 0.9945994234836328,
    ],
    // Schema 8:
    &[
        0.5, 0.5013556375251013, 0.5027149505564014, 0.5040779490592088,
        0.5054446430258502, 0.5068150424757447, 0.5081891574554764, 0.509566998038869,
        0.5109485743270583, 0.5123338964485679, 0.5137229745593818, 0.5151158188430205,
        0.5165124395106142, 0.5179128468009786, 0.5193170509806894, 0.520725062344158,
        0.5221368912137069, 0.5235525479396449, 0.5249720429003435, 0.526395386502313,
        0.5278225891802786, 0.5292536613972564, 0.5306886136446309, 0.5321274564422321,
        0.5335702003384117, 0.5350168559101208, 0.5364674337629877, 0.5379219445313954,
        0.5393803988785598, 0.5408428074966075, 0.5423091811066545, 0.5437795304588847,
        0.5452538663326288, 0.5467321995364429, 0.5482145409081883, 0.549700901315111,
        0.5511912916539204, 0.5526857228508706, 0.5541842058618393, 0.5556867516724088,
        0.5571933712979462, 0.5587040757836845, 0.5602188762048033, 0.5617377836665098,
        0.5632608093041209, 0.564787964283144, 0.5663192597993595, 0.5678547070789026,
        0.5693943173783458, 0.5709381019847808, 0.572486072215902, 0.5740382394200894,
        0.5755946149764913, 0.5771552102951081, 0.5787200368168754, 0.5802891060137493,
        0.5818624293887887, 0.5834400184762408, 0.585021884841625, 0.5866080400818185,
        0.5881984958251406, 0.5897932637314379, 0.5913923554921704, 0.5929957828304968,
        0.5946035575013605, 0.5962156912915756, 0.5978321960199137, 0.5994530835371903,
        0.6010783657263515, 0.6027080545025619, 0.6043421618132907, 0.6059806996384005,
        0.6076236799902344, 0.6092711149137041, 0.6109230164863786, 0.6125793968185725,
        0.6142402680534349, 0.6159056423670379, 0.6175755319684665, 0.6192499490999082,
        0.620928906036742, 0.622612415087629, 0.6243004885946023, 0.6259931389331581,
        0.6276903785123455, 0.6293922197748583, 0.6310986751971253, 0.6328097572894031,
        0.6345254785958666, 0.6362458516947014, 0.637970889198196, 0.6397006037528346,
        0.6414350080393891, 0.6431741147730128, 0.6449179367033329, 0.6466664866145447,
        0.6484197773255048, 0.6501778216898253, 0.6519406325959679, 0.6537082229673385,
        0.6554806057623822, 0.6572577939746774, 0.659039800633032, 0.6608266388015788,
        0.6626183215798706, 0.6644148621029772, 0.6662162735415805, 0.6680225691020727,
        0.6698337620266515, 0.6716498655934177, 0.6734708931164728, 0.6752968579460171,
        0.6771277734684463, 0.6789636531064505, 0.6808045103191123, 0.6826503586020058,
        0.6845012114872953, 0.6863570825438342, 0.688217985377265, 0.690083933630119,
        0.6919549409819159, 0.6938310211492645, 0.6957121878859629, 0.6975984549830999,
        0.6994898362691555, 0.7013863456101023, 0.7032879969095076, 0.7051948041086352,
        0.7071067811865475, 0.7090239421602076, 0.7109463010845827, 0.7128738720527471,
        0.7148066691959849, 0.7167447066838943, 0.718687998724491, 0.7206365595643126,
        0.7225904034885232, 0.7245495448210174, 0.7265139979245261, 0.7284837772007218,
        0.7304588970903234, 0.7324393720732029, 0.7344252166684908, 0.7364164454346837,
        0.7384130729697496, 0.7404151139112358, 0.7424225829363761, 0.7444354947621984,
        0.7464538641456323, 0.7484777058836176, 0.7505070348132126, 0.7525418658117031,
        0.7545822137967112, 0.7566280937263048, 0.7586795205991071, 0.7607365094544071,
        0.762799075372269, 0.7648672334736434, 0.7669409989204777, 0.7690203869158282,
        0.7711054127039704, 0.7731960915705107, 0.7752924388424999, 0.7773944698885442,
        0.7795022001189185, 0.7816156449856788, 0.7837348199827764, 0.7858597406461707,
        0.7879904225539431, 0.7901268813264122, 0.7922691326262467, 0.7944171921585818,
        0.7965710756711334, 0.7987307989543135, 0.8008963778413465, 0.8030678282083853,
        0.805245165974627, 0.8074284071024302, 0.8096175675974316, 0.8118126635086642,
        0.8140137109286738, 0.8162207259936375, 0.8184337248834821, 0.820652723822003,
        0.8228777390769823, 0.8251087869603088, 0.8273458838280969, 0.8295890460808079,
        0.8318382901633681, 0.8340936325652911, 0.8363550898207981, 0.8386226785089391,
        0.8408964152537144, 0.8431763167241966, 0.8454623996346523, 0.8477546807446661,
        0.8500531768592616, 0.8523579048290255, 0.8546688815502312, 0.8569861239649629,
        0.8593096490612387, 0.8616394738731368, 0.8639756154809185, 0.8663180910111553,
        0.8686669176368529, 0.871022112577578, 0.8733836930995842, 0.8757516765159389,
        0.8781260801866495, 0.8805069215187917, 0.8828942179666361, 0.8852879870317771,
        0.8876882462632604, 0.890095013257712, 0.8925083056594671, 0.8949281411607002,
        0.8973545375015533, 0.8997875124702672, 0.9022270839033115, 0.9046732696855155,
        0.9071260877501991, 0.909585556079304, 0.9120516927035263, 0.9145245157024483,
        0.9170040432046711, 0.9194902933879467, 0.9219832844793128, 0.9244830347552253,
        0.9269895625416926, 0.92950288621441, 0.9320230241988943, 0.9345499949706191,
        0.9370838170551498, 0.93962450902828, 0.9421720895161669, 0.9447265771954693,
        0.9472879907934827, 0.9498563490882775, 0.9524316709088368, 0.9550139751351947,
        0.9576032806985735, 0.9601996065815236, 0.9628029718180622, 0.9654133954938133,
        0.9680308967461471, 0.9706554947643201, 0.9732872087896164, 0.9759260581154889,
        0.9785720620876999, 0.9812252401044634, 0.9838856116165875, 0.9865531961276168,
        0.9892280131939752, 0.9919100824251095, 0.9945994234836328, 0.9972960560854698,
    ],
];
