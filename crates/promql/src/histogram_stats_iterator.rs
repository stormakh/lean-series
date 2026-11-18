//! Port of `promql/histogram_stats_iterator.go`: an iterator that returns histograms with only their count and
//! sum (and schema and counter reset hint) populated, with counter reset detection done on the fly.

use std::cell::RefCell;

use chunkenc::{AnyIterator, ChunkIterator, ValueType};
use model::histogram::{CounterResetHint, FloatHistogram, Histogram};

/// The state that `at_float_histogram` changes (Go mutates the iterator through a pointer receiver).
#[derive(Default)]
struct State {
    current: FloatHistogram,
    last: Option<FloatHistogram>,
    last_is_current: bool,
}

/// Go `HistogramStatsIterator`. `next` and `seek` never return `ValueType::Histogram`, but
/// `ValueType::FloatHistogram` instead; `at_histogram` must not be called (it panics).
pub struct HistogramStatsIterator<'a> {
    inner: AnyIterator<'a>,
    state: RefCell<State>,
}

impl<'a> HistogramStatsIterator<'a> {
    /// Go `NewHistogramStatsIterator`.
    pub fn new(it: AnyIterator<'a>) -> HistogramStatsIterator<'a> {
        HistogramStatsIterator { inner: it, state: RefCell::new(State::default()) }
    }

    /// Go `Reset`: resets the iterator for use with a new underlying iterator, reusing allocations.
    pub fn reset(&mut self, it: AnyIterator<'a>) {
        self.inner = it;
        let st = self.state.get_mut();
        st.last = None;
        st.last_is_current = false;
    }

    /// Go `getResetHint`.
    fn get_reset_hint(st: &State, hint: CounterResetHint) -> CounterResetHint {
        if hint != CounterResetHint::UnknownCounterReset {
            return hint;
        }
        match &st.last {
            // We don't know if there's a counter reset.
            None => CounterResetHint::UnknownCounterReset,
            Some(last) => {
                if st.current.detect_reset(last) {
                    CounterResetHint::CounterReset
                } else {
                    CounterResetHint::NotCounterReset
                }
            }
        }
    }
}

/// Go `populateFH`: a histogram with only the counter reset hint, schema, count and sum of `src`, written into
/// `fh` when given.
fn populate(src: &FloatHistogram, hint: CounterResetHint, fh: Option<FloatHistogram>) -> FloatHistogram {
    let h = FloatHistogram { counter_reset_hint: hint, schema: src.schema, count: src.count, sum: src.sum, ..Default::default() };
    match fh {
        None => h,
        Some(mut fh) => {
            h.copy_to(&mut fh);
            fh
        }
    }
}

impl ChunkIterator for HistogramStatsIterator<'_> {
    /// Relays to the underlying iterator, but changes a `Histogram` return into a `FloatHistogram` return.
    fn next(&mut self) -> ValueType {
        self.state.get_mut().last_is_current = false;
        match self.inner.next() {
            ValueType::Histogram => ValueType::FloatHistogram,
            vt => vt,
        }
    }

    /// Relays to the underlying iterator, but changes a `Histogram` return into a `FloatHistogram` return.
    fn seek(&mut self, t: i64) -> ValueType {
        // If the seek is going to move the iterator, we have to forget the last histogram and mark the current
        // one as not current anymore.
        if t > self.inner.at_t() {
            let st = self.state.get_mut();
            st.last = None;
            st.last_is_current = false;
        }
        match self.inner.seek(t) {
            ValueType::Histogram => ValueType::FloatHistogram,
            vt => vt,
        }
    }

    fn at(&self) -> (i64, f64) {
        self.inner.at()
    }

    fn at_histogram(&self, _h: Option<Histogram>) -> (i64, Histogram) {
        panic!("HistogramStatsIterator.AtHistogram must never be called")
    }

    /// The current timestamp and histogram with only count, sum, schema and counter reset hint. Detects counter
    /// resets on the fly: the hint is explicit (not unknown) if the previous sample was read with this iterator.
    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        let mut st = self.state.borrow_mut();
        if st.last_is_current {
            // Nothing changed since the last call. Return a copy of the stored last histogram rather than doing
            // counter reset detection again.
            let last = st.last.as_ref().expect("last histogram");
            return (self.inner.at_t(), populate(last, last.counter_reset_hint, fh));
        }
        let cur = std::mem::take(&mut st.current);
        let (t, cur) = self.inner.at_float_histogram(Some(cur));
        st.current = cur;
        if model::value::is_stale_nan(st.current.sum) {
            return (t, populate(&st.current, st.current.counter_reset_hint, fh));
        }
        let hint = Self::get_reset_hint(&st, st.current.counter_reset_hint);
        let out = populate(&st.current, hint, fh);
        // Go `setLastFromCurrent`: store a copy of current as last, with the hint calculated here.
        let st = &mut *st;
        match &mut st.last {
            None => st.last = Some(st.current.copy()),
            Some(last) => st.current.copy_to(last),
        }
        let last = st.last.as_mut().unwrap();
        last.counter_reset_hint = out.counter_reset_hint;
        st.last_is_current = true;
        (t, out)
    }

    fn at_t(&self) -> i64 {
        self.inner.at_t()
    }

    fn at_st(&self) -> i64 {
        self.inner.at_st()
    }

    fn err(&self) -> Option<&chunkenc::Error> {
        self.inner.err()
    }
}
