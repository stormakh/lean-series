//! Port of `promql/matrix_budget.go`: with the query-cost feature, matrix reads check cancellation while they
//! read from storage and stop as soon as the samples-read budget would be exceeded.
//!
//! Go's guard iterator panics from inside `At`/`Next`; here it records the failure in a state shared with the
//! evaluator and stops the reads (its `next` and `seek` return `None`). The evaluator then charges the unbilled
//! input and returns the error (see `Evaluator::guard_fail`).

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use chunkenc::{AnyIterator, ChunkIterator, ValueType};
use model::histogram::{FloatHistogram, Histogram};
use model::value::is_stale_nan;
use storage::{BufferedSeriesIterator, Context, Error, new_buffer};

use super::eval::{EvalResult, QueryBudget, add_saturating_i64};
use super::{ENV, PromqlError, context_done};
use crate::value::histogram_point_size;

/// Why a guarded read stopped.
#[derive(Clone, Debug)]
pub(crate) enum GuardFailure {
    Context(Error),
    Budget,
}

/// The state shared by a matrix iterator and its guard (Go `queryIterator` fields).
pub(crate) struct GuardState<'a> {
    pub ctx: Context,
    pub budget: &'a QueryBudget,
    pub mint_floats: Cell<i64>,
    pub mint_histograms: Cell<i64>,
    pub maxt: Cell<i64>,
    pub step: Cell<isize>,
    pub pending: Cell<i64>,
    pub read_base: Cell<i64>,
    pub project: Cell<bool>,
    pub failed: RefCell<Option<GuardFailure>>,
}

impl GuardState<'_> {
    pub fn begin_frame(&self, step: isize) {
        self.step.set(step);
        self.pending.set(0);
        self.project.set(false);
        self.read_base.set(self.budget.samples_read.get());
    }

    #[inline]
    fn check_context(&self) -> bool {
        if self.failed.borrow().is_some() {
            return false;
        }
        if let Err(e) = context_done(&self.ctx, ENV) {
            *self.failed.borrow_mut() = Some(GuardFailure::Context(e));
            return false;
        }
        true
    }

    fn observe(&self, t: i64, mint: i64, n: i64) {
        if t <= mint || t > self.maxt.get() {
            return;
        }
        self.pending.set(add_saturating_i64(self.pending.get(), n));
        let max = self.budget.max_samples_read;
        if max > 0 && add_saturating_i64(self.read_base.get(), self.pending.get()) > max && self.failed.borrow().is_none() {
            *self.failed.borrow_mut() = Some(GuardFailure::Budget);
        }
    }

    /// Go `flushPending`: the unbilled projected input to charge.
    pub fn unbilled(&self) -> i64 {
        if self.step.get() < 0 {
            return 0;
        }
        add_saturating_i64(self.read_base.get(), self.pending.get()) - self.budget.samples_read.get()
    }
}

/// Go `queryIterator`: checks cancellation around every read and projects input cost during matrix seeks.
pub(crate) struct QueryIterator<'a> {
    inner: AnyIterator<'a>,
    st: Rc<GuardState<'a>>,
}

impl ChunkIterator for QueryIterator<'_> {
    fn next(&mut self) -> ValueType {
        if !self.st.check_context() {
            return ValueType::None;
        }
        let vt = self.inner.next();
        if !self.st.check_context() {
            return ValueType::None;
        }
        vt
    }
    fn seek(&mut self, t: i64) -> ValueType {
        if !self.st.check_context() {
            return ValueType::None;
        }
        let vt = self.inner.seek(t);
        if !self.st.check_context() {
            return ValueType::None;
        }
        vt
    }
    fn at(&self) -> (i64, f64) {
        let (t, f) = self.inner.at();
        if self.st.project.get() && !is_stale_nan(f) {
            self.st.observe(t, self.st.mint_floats.get(), 1);
        }
        (t, f)
    }
    fn at_histogram(&self, h: Option<Histogram>) -> (i64, Histogram) {
        let (t, h) = self.inner.at_histogram(h);
        if self.st.project.get()
            && !is_stale_nan(h.sum)
            && t > self.st.mint_histograms.get()
            && t <= self.st.maxt.get()
        {
            let fh = h.to_float();
            self.st.observe(t, self.st.mint_histograms.get(), histogram_point_size(&fh) as i64);
        }
        (t, h)
    }
    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        let (t, h) = self.inner.at_float_histogram(fh);
        if self.st.project.get() && !is_stale_nan(h.sum) {
            self.st.observe(t, self.st.mint_histograms.get(), histogram_point_size(&h) as i64);
        }
        (t, h)
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

/// Go `matrixIterator`: a buffered iterator with an optional guard.
pub struct MatrixIter<'a> {
    pub(crate) buf: BufferedSeriesIterator<'a>,
    pub(crate) guard: Option<Rc<GuardState<'a>>>,
}

impl<'a> MatrixIter<'a> {
    pub(crate) fn new(ctx: &Context, budget: Option<&'a QueryBudget>, delta: i64) -> MatrixIter<'a> {
        MatrixIter {
            buf: new_buffer(delta),
            guard: budget.map(|b| {
                Rc::new(GuardState {
                    ctx: ctx.clone(),
                    budget: b,
                    mint_floats: Cell::new(0),
                    mint_histograms: Cell::new(0),
                    maxt: Cell::new(0),
                    step: Cell::new(-1),
                    pending: Cell::new(0),
                    read_base: Cell::new(0),
                    project: Cell::new(false),
                    failed: RefCell::new(None),
                })
            }),
        }
    }

    /// Go `Reset`.
    pub fn reset(&mut self, inner: AnyIterator<'a>) {
        match &self.guard {
            Some(g) => {
                g.begin_frame(-1);
                self.buf.reset(AnyIterator::Other(Box::new(QueryIterator { inner, st: g.clone() })));
            }
            None => {
                self.buf.reset(inner);
            }
        }
    }

    /// Go `seek`: seeks to `maxt`, projecting the input cost in `(mint, maxt]` with a guard.
    /// Go `beginFrame` (without the context check, which the evaluator's `guard_check` does).
    #[doc(hidden)]
    pub fn begin_frame(&mut self, step: isize) {
        if let Some(g) = &self.guard {
            g.begin_frame(step);
        }
    }

    /// The look-back buffer.
    #[doc(hidden)]
    pub fn buffer(&self) -> storage::SampleRingIterator<'_> {
        self.buf.buffer()
    }

    #[doc(hidden)]
    pub fn seek(&mut self, mint_floats: i64, mint_histograms: i64, maxt: i64) -> ValueType {
        match &self.guard {
            None => self.buf.seek(maxt),
            Some(g) => {
                g.mint_floats.set(mint_floats);
                g.mint_histograms.set(mint_histograms);
                g.maxt.set(maxt);
                g.project.set(g.step.get() >= 0);
                let vt = self.buf.seek(maxt);
                g.project.set(false);
                vt
            }
        }
    }

    pub fn reduce_delta(&mut self, delta: i64) -> bool {
        self.buf.reduce_delta(delta)
    }
}

/// The error of a failed guard, after charging the unbilled input (Go `flushPending` then the panic).
pub(crate) fn guard_error(
    g: &GuardState<'_>,
    failure: GuardFailure,
    consume: &mut dyn FnMut(i64, isize) -> EvalResult<()>,
) -> Error {
    let n = g.unbilled();
    if n > 0 {
        if let Err(e) = consume(n, g.step.get()) {
            return e;
        }
    }
    match failure {
        GuardFailure::Context(e) => e,
        GuardFailure::Budget => PromqlError::TooManySamplesRead(ENV.to_string()).err(),
    }
}
