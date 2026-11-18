//! Port of `storage/memoized_iterator.go`: an iterator that remembers the previous element.

use chunkenc::{AnyIterator, ChunkIterator, NopIterator, ValueType};
use model::histogram::FloatHistogram;

/// Wraps an iterator and remembers the previous element (Go `MemoizedSeriesIterator`). Integer histograms are
/// returned as float histograms: `seek` and `next` never return `ValueType::Histogram`. Deliberately not a
/// `ChunkIterator`.
pub struct MemoizedSeriesIterator<'a> {
    it: AnyIterator<'a>,
    delta: i64,
    last_time: i64,
    value_type: ValueType,
    // The previously returned value.
    prev_st: i64,
    prev_time: i64,
    prev_value: f64,
    prev_float_histogram: Option<FloatHistogram>,
    /// A spare float histogram, to reuse its allocation.
    spare: Option<FloatHistogram>,
}

/// Go `NewMemoizedEmptyIterator`.
pub fn new_memoized_empty_iterator<'a>(delta: i64) -> MemoizedSeriesIterator<'a> {
    new_memoized_iterator(AnyIterator::Nop(NopIterator), delta)
}

/// Go `NewMemoizedIterator`: remembers the values within `delta` before the current element.
pub fn new_memoized_iterator(it: AnyIterator<'_>, delta: i64) -> MemoizedSeriesIterator<'_> {
    let mut b = MemoizedSeriesIterator {
        it: AnyIterator::Nop(NopIterator),
        delta,
        last_time: i64::MIN,
        value_type: ValueType::None,
        prev_st: 0,
        prev_time: i64::MIN,
        prev_value: 0.0,
        prev_float_histogram: None,
        spare: None,
    };
    b.reset(it);
    b
}

impl<'a> MemoizedSeriesIterator<'a> {
    /// Reuses the wrapper with another iterator. Returns the previous iterator, so that it can be reused.
    pub fn reset(&mut self, it: AnyIterator<'a>) -> AnyIterator<'a> {
        let old = std::mem::replace(&mut self.it, it);
        self.prev_st = 0;
        self.last_time = i64::MIN;
        self.prev_time = i64::MIN;
        self.value_type = self.it.next();
        old
    }

    /// Go `PeekPrev`: the previous element `(st, t, v, fh)`, if any.
    pub fn peek_prev(&self) -> Option<(i64, i64, f64, Option<&FloatHistogram>)> {
        if self.prev_time == i64::MIN {
            return None;
        }
        Some((self.prev_st, self.prev_time, self.prev_value, self.prev_float_histogram.as_ref()))
    }

    /// Advances to the element at time t or later.
    pub fn seek(&mut self, t: i64) -> ValueType {
        let t0 = t - self.delta;
        if self.value_type != ValueType::None && t0 > self.last_time {
            // The seek went further than the delta: forget the previous element.
            self.prev_time = i64::MIN;
            self.value_type = self.it.seek(t0);
            match self.value_type {
                ValueType::None => return ValueType::None,
                ValueType::Histogram => self.value_type = ValueType::FloatHistogram,
                _ => {}
            }
            self.last_time = self.it.at_t();
        }
        if self.last_time >= t {
            return self.value_type;
        }
        while self.next() != ValueType::None {
            if self.last_time >= t {
                return self.value_type;
            }
        }
        ValueType::None
    }

    /// Advances to the next element. Does not check that the remembered element is within delta.
    pub fn next(&mut self) -> ValueType {
        // Remember the current element.
        match self.value_type {
            ValueType::None => return ValueType::None,
            ValueType::Float => {
                self.prev_st = self.it.at_st();
                (self.prev_time, self.prev_value) = self.it.at();
                if let Some(fh) = self.prev_float_histogram.take() {
                    self.spare = Some(fh);
                }
            }
            ValueType::Histogram | ValueType::FloatHistogram => {
                self.prev_value = 0.0;
                self.prev_st = self.it.at_st();
                let reuse = self.prev_float_histogram.take().or_else(|| self.spare.take());
                let (t, fh) = self.it.at_float_histogram(reuse);
                self.prev_time = t;
                self.prev_float_histogram = Some(fh);
            }
        }
        self.value_type = self.it.next();
        if self.value_type != ValueType::None {
            self.last_time = self.it.at_t();
        }
        if self.value_type == ValueType::Histogram {
            self.value_type = ValueType::FloatHistogram;
        }
        self.value_type
    }

    /// The current float element.
    pub fn at(&self) -> (i64, f64) {
        self.it.at()
    }

    /// The current float histogram element. `fh` is reused when given.
    pub fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        self.it.at_float_histogram(fh)
    }

    /// The start timestamp of the current element.
    pub fn at_st(&self) -> i64 {
        self.it.at_st()
    }

    /// The timestamp of the current element.
    pub fn at_t(&self) -> i64 {
        self.it.at_t()
    }

    /// The last error.
    pub fn err(&self) -> Option<&chunkenc::Error> {
        self.it.err()
    }
}
