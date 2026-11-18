//! Port of the parts of `util/teststorage/appender.go` that the storage tests use: `Appendable`, a mock of
//! `storage.Appendable` and `storage.AppendableV2` that records appended samples and injects errors. The chaining
//! (`Then`), `WithRefFn`, `SkipRecording` and the string helpers are not ported (no test here uses them).

#![allow(dead_code)]

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram};
use model::labels::Labels;
use model::metadata::Metadata;
use storage::*;

/// A combined test sample (Go `teststorage.Sample`).
#[derive(Clone, Debug, Default)]
pub struct Sample {
    pub mf: String,
    pub l: Labels,
    pub m: Metadata,
    pub st: i64,
    pub t: i64,
    pub v: f64,
    pub h: Option<Histogram>,
    pub fh: Option<FloatHistogram>,
    pub es: Vec<Exemplar>,
}

impl Sample {
    /// Go `Sample.Equals`.
    pub fn equals(&self, other: &Sample) -> bool {
        self.mf == other.mf
            && self.l == other.l
            && self.m.equals(&other.m)
            && self.st == other.st
            && self.t == other.t
            // Compare the bits, so NaN values which are exactly the same compare equal.
            && self.v.to_bits() == other.v.to_bits()
            && match (&self.h, &other.h) {
                (None, None) => true,
                (Some(a), Some(b)) => a.equals(b),
                _ => false,
            }
            && match (&self.fh, &other.fh) {
                (None, None) => true,
                (Some(a), Some(b)) => a.equals(b),
                _ => false,
            }
            && self.es.len() == other.es.len()
            && self.es.iter().zip(&other.es).all(|(a, b)| a.equals(b))
    }
}

/// Asserts that two sample lists are equal (Go `testutil.RequireEqual` on `[]Sample`).
pub fn require_equal(expected: &[Sample], got: &[Sample]) {
    assert!(
        expected.len() == got.len() && expected.iter().zip(got).all(|(a, b)| a.equals(b)),
        "samples differ:\nexpected: {:?}\ngot: {:?}",
        expected,
        got
    );
}

pub type AppendErrFn = Box<dyn Fn(&Labels) -> Option<Error> + Send + Sync>;

#[derive(Default)]
struct Recorded {
    pending_samples: Vec<Sample>,
    result_samples: Vec<Sample>,
    rolledback_samples: Vec<Sample>,
}

#[derive(Default)]
struct Inner {
    /// Injected on every Append, AppendHistogram and ST zero call.
    append_err_fn: Option<AppendErrFn>,
    /// Injected exemplar error.
    append_exemplars_error: Option<Error>,
    /// Injected commit error.
    commit_err: Option<Error>,
    mtx: Mutex<Recorded>,
    /// Guard against multi-appender use.
    open_appenders: AtomicI32,
}

/// A mock appendable (Go `teststorage.Appendable`). Clones share the recorded samples.
#[derive(Clone, Default)]
pub struct Appendable(Arc<Inner>);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// Go `NewAppendable`.
pub fn new_appendable() -> Appendable {
    Appendable::default()
}

impl Appendable {
    /// Go `WithErrs`: injects errors. Must be called before the appendable is shared.
    pub fn with_errs(
        mut self,
        append_err_fn: AppendErrFn,
        append_exemplars_error: Option<Error>,
        commit_err: Option<Error>,
    ) -> Appendable {
        let inner = Arc::get_mut(&mut self.0).expect("with_errs before sharing");
        inner.append_err_fn = Some(append_err_fn);
        inner.append_exemplars_error = append_exemplars_error;
        inner.commit_err = commit_err;
        self
    }

    fn copy(v: &[Sample]) -> Option<Vec<Sample>> {
        if v.is_empty() { None } else { Some(v.to_vec()) }
    }

    /// Go `PendingSamples`: samples appended without commit (`None` for none).
    pub fn pending_samples(&self) -> Option<Vec<Sample>> {
        Self::copy(&lock(&self.0.mtx).pending_samples)
    }

    /// Go `ResultSamples`: committed samples.
    pub fn result_samples(&self) -> Option<Vec<Sample>> {
        Self::copy(&lock(&self.0.mtx).result_samples)
    }

    /// Go `RolledbackSamples`.
    pub fn rolledback_samples(&self) -> Option<Vec<Sample>> {
        Self::copy(&lock(&self.0.mtx).rolledback_samples)
    }

    /// Go `computeOrCheckRef`.
    fn compute_or_check_ref(&self, r: SeriesRef, ls: &Labels) -> Result<SeriesRef, Error> {
        let h = ls.hash();
        if r == 0 {
            // The labels hash stands in for a unique series reference.
            return Ok(h);
        }
        if h != r {
            return Err(Error::msg(
                "teststorage.appender: found input ref not matching labels; potential bug in Appendable usage",
            ));
        }
        Ok(r)
    }

    fn check_append_err(&self, ls: &Labels) -> Option<Error> {
        self.0.append_err_fn.as_ref().and_then(|f| f(ls))
    }
}

fn err_closed_appender() -> Error {
    Error::msg("appender was already committed/rolledback")
}

/// Go `baseAppender`.
struct BaseAppender {
    err: Option<Error>,
    a: Appendable,
}

impl BaseAppender {
    fn new(a: &Appendable, concurrent_msg: &str) -> BaseAppender {
        let mut b = BaseAppender {
            err: None,
            a: a.clone(),
        };
        if a.0.open_appenders.fetch_add(1, Ordering::SeqCst) + 1 > 1 {
            b.err = Some(Error::msg(concurrent_msg.to_string()));
        }
        b
    }

    fn check_err(&self) -> Option<Error> {
        self.err.clone()
    }

    fn commit(&mut self) -> Option<Error> {
        if let Some(err) = self.check_err() {
            return Some(err);
        }
        let _dec = DecOnDrop(&self.a.0.open_appenders);
        if let Some(e) = &self.a.0.commit_err {
            return Some(e.clone());
        }
        let mut r = lock(&self.a.0.mtx);
        let pending = std::mem::take(&mut r.pending_samples);
        r.result_samples.extend(pending);
        self.err = Some(err_closed_appender());
        None
    }

    fn rollback(&mut self) -> Option<Error> {
        if let Some(err) = self.check_err() {
            return Some(err);
        }
        let _dec = DecOnDrop(&self.a.0.open_appenders);
        let mut r = lock(&self.a.0.mtx);
        let pending = std::mem::take(&mut r.pending_samples);
        r.rolledback_samples.extend(pending);
        self.err = Some(err_closed_appender());
        None
    }
}

/// Go `defer a.a.openAppenders.Dec()`.
struct DecOnDrop<'a>(&'a AtomicI32);

impl Drop for DecOnDrop<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Go `appender`.
struct TestAppender(BaseAppender);

impl storage::Appendable for Appendable {
    fn appender(&self, _ctx: &Context) -> Box<dyn Appender> {
        Box::new(TestAppender(BaseAppender::new(
            self,
            "teststorage.Appendable.Appender() concurrent use is not supported; attempted opening new Appender() without Commit/Rollback of the previous one. Extend the implementation if concurrent mock is needed",
        )))
    }
}

impl AppenderTransaction for TestAppender {
    fn commit(&mut self) -> Option<Error> {
        self.0.commit()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.0.rollback()
    }
}

impl Appender for TestAppender {
    fn append(&mut self, r: SeriesRef, ls: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        if let Some(err) = self.0.check_err() {
            return Err(err);
        }
        let a = &self.0.a;
        if let Some(err) = a.check_append_err(ls) {
            return Err(err);
        }
        lock(&a.0.mtx).pending_samples.push(Sample {
            l: ls.clone(),
            t,
            v,
            ..Default::default()
        });
        a.compute_or_check_ref(r, ls)
    }

    fn set_options(&mut self, _opts: Option<&AppendOptions>) {}

    fn append_exemplar(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        e: &Exemplar,
    ) -> Result<SeriesRef, Error> {
        if let Some(err) = self.0.check_err() {
            return Err(err);
        }
        let a = &self.0.a;
        if let Some(err) = &a.0.append_exemplars_error {
            return Err(err.clone());
        }
        let mut appended = false;
        {
            let mut rec = lock(&a.0.mtx);
            // Attach the exemplar to the last matching sample.
            if let Some(s) = rec.pending_samples.iter_mut().rev().find(|s| &s.l == l) {
                s.es.push(e.clone());
                appended = true;
            }
        }
        if !appended {
            return Err(Error::msg(format!(
                "teststorage.appender: exemplar appender without series; ref {}; l {}; exemplar: {:?}",
                r, l, e
            )));
        }
        a.compute_or_check_ref(r, l)
    }

    fn append_histogram(
        &mut self,
        r: SeriesRef,
        ls: &Labels,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        if let Some(err) = self.0.check_err() {
            return Err(err);
        }
        let a = &self.0.a;
        if let Some(err) = a.check_append_err(ls) {
            return Err(err);
        }
        lock(&a.0.mtx).pending_samples.push(Sample {
            l: ls.clone(),
            t,
            h: h.cloned(),
            fh: fh.cloned(),
            ..Default::default()
        });
        a.compute_or_check_ref(r, ls)
    }

    fn append_histogram_st_zero_sample(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        _t: i64,
        st: i64,
        h: Option<&Histogram>,
        _fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        if h.is_some() {
            return self.append_histogram(r, l, st, Some(&Histogram::default()), None);
        }
        self.append_histogram(r, l, st, None, Some(&FloatHistogram::default()))
    }

    fn update_metadata(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        m: &Metadata,
    ) -> Result<SeriesRef, Error> {
        if let Some(err) = self.0.check_err() {
            return Err(err);
        }
        let a = &self.0.a;
        let mut updated = false;
        {
            let mut rec = lock(&a.0.mtx);
            // Attach the metadata to the last matching sample.
            if let Some(s) = rec.pending_samples.iter_mut().rev().find(|s| &s.l == l) {
                s.m = m.clone();
                updated = true;
            }
        }
        if !updated {
            return Err(Error::msg(format!(
                "teststorage.appender: metadata update without series; ref {}; l {}; m: {:?}",
                r, l, m
            )));
        }
        a.compute_or_check_ref(r, l)
    }

    fn append_st_zero_sample(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        _t: i64,
        st: i64,
    ) -> Result<SeriesRef, Error> {
        self.append(r, l, st, 0.0)
    }
}

/// Go `appenderV2`.
struct TestAppenderV2(BaseAppender);

impl storage::AppendableV2 for Appendable {
    fn appender_v2(&self, _ctx: &Context) -> Box<dyn AppenderV2> {
        Box::new(TestAppenderV2(BaseAppender::new(
            self,
            "teststorage.Appendable.AppenderV2() concurrent use is not supported; attempted opening new AppenderV2() without Commit/Rollback of the previous one. Extend the implementation if concurrent mock is needed",
        )))
    }
}

impl AppenderTransaction for TestAppenderV2 {
    fn commit(&mut self) -> Option<Error> {
        self.0.commit()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.0.rollback()
    }
}

fn exemplar_errors(n: usize, err: &Error) -> Vec<ExemplarError> {
    (0..n)
        .map(|index| ExemplarError {
            index,
            err: err.clone(),
        })
        .collect()
}

impl AppenderV2 for TestAppenderV2 {
    fn append(
        &mut self,
        r: SeriesRef,
        ls: &Labels,
        st: i64,
        t: i64,
        v: f64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
        opts: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        if let Some(err) = self.0.check_err() {
            return Err(err);
        }
        let a = &self.0.a;
        if let Some(err) = a.check_append_err(ls) {
            return Err(err);
        }
        let mut partial: Option<AppendPartialError> = None;
        let mut es = Vec::new();
        if !opts.exemplars.is_empty() {
            match &a.0.append_exemplars_error {
                Some(err) => {
                    partial = Some(AppendPartialError {
                        series_ref: 0,
                        exemplar_errors: exemplar_errors(opts.exemplars.len(), err),
                    })
                }
                // The exemplars slice is unsafe for reuse.
                None => es = opts.exemplars.to_vec(),
            }
        }
        lock(&a.0.mtx).pending_samples.push(Sample {
            mf: opts.metric_family_name.to_string(),
            m: opts.metadata.cloned().unwrap_or_default(),
            l: ls.clone(),
            st,
            t,
            v,
            h: h.cloned(),
            fh: fh.cloned(),
            es,
        });
        let r = a.compute_or_check_ref(r, ls)?;
        match partial {
            Some(mut p) => {
                p.series_ref = r;
                Err(AppendPartialError::to_error(Some(&p)).expect("exemplar errors"))
            }
            None => Ok(r),
        }
    }

    fn as_exemplar_appender_v2(&mut self) -> Option<&mut dyn ExemplarAppenderV2> {
        Some(self)
    }
}

impl ExemplarAppenderV2 for TestAppenderV2 {
    fn append_exemplars(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        exemplars: &[Exemplar],
    ) -> Result<SeriesRef, Error> {
        if let Some(err) = self.0.check_err() {
            return Err(err);
        }
        let a = self.0.a.clone();
        let matches = |s: &Sample| (!l.is_empty() && &s.l == l) || (r != 0 && s.l.hash() == r);
        let mut pending_idx: Option<usize> = None;
        let mut matched_labels = Labels::default();
        {
            let rec = lock(&a.0.mtx);
            // Attach the exemplars to the last matching sample.
            if let Some(i) = rec.pending_samples.iter().rposition(matches) {
                pending_idx = Some(i);
                matched_labels = rec.pending_samples[i].l.clone();
            } else if let Some(s) = rec.result_samples.iter().rev().find(|s| matches(s)) {
                matched_labels = s.l.clone();
            }
        }
        if matched_labels.is_empty() {
            return Err(Error::wrapped(
                format!(
                    "teststorage.appenderV2: exemplars appended without series; ref {}; l {}; exemplars: {:?}: {}",
                    r, l, exemplars, ERR_NOT_FOUND
                ),
                ERR_NOT_FOUND,
            ));
        }

        let mut partial: Option<AppendPartialError> = None;
        let check_l = if l.is_empty() { &matched_labels } else { l };
        let r = a.compute_or_check_ref(r, check_l)?;

        if !exemplars.is_empty() {
            match &a.0.append_exemplars_error {
                Some(err) => {
                    let p = AppendPartialError {
                        series_ref: r,
                        exemplar_errors: exemplar_errors(exemplars.len(), err),
                    };
                    AppendPartialError::handle(
                        &mut partial,
                        AppendPartialError::to_error(Some(&p)),
                    );
                }
                None => {
                    // The exemplars slice is unsafe for reuse.
                    let es = exemplars.to_vec();
                    let mut rec = lock(&a.0.mtx);
                    match pending_idx {
                        Some(i) if i < rec.pending_samples.len() => {
                            rec.pending_samples[i].es.extend(es)
                        }
                        _ => rec.pending_samples.push(Sample {
                            l: matched_labels,
                            es,
                            ..Default::default()
                        }),
                    }
                }
            }
        }
        match partial {
            Some(mut p) if !p.exemplar_errors.is_empty() => {
                p.series_ref = r;
                Err(AppendPartialError::to_error(Some(&p)).expect("exemplar errors"))
            }
            _ => Ok(r),
        }
    }
}
