//! Port of `util/teststorage/appender.go`: an `Appendable` mock that records every sample appended through its
//! V1 and V2 appenders, with injectable errors and an optional chained appendable. The real-TSDB part of
//! `teststorage` (`teststorage.New`) lives with the TSDB.
//!
//! The mock is a cheap cloneable handle: clones share the recorded samples (Go passes `*Appendable`).

use std::fmt::{self, Write as _};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use model::exemplar::{self, Exemplar};
use model::histogram::gofmt::write_g;
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{self, Labels};
use model::metadata::{Metadata, MetricType};
use model::value::is_stale_nan;
use storage::{
    AppendOptions, AppendPartialError, AppendV2Options, Appendable as AppendableT, AppendableV2, Appender,
    AppenderTransaction, AppenderV2, Context, Error, ExemplarAppenderV2, ExemplarError, SeriesRef, Sentinel,
    ERR_NOT_FOUND,
};

/// A combined test sample for mocking `storage::AppenderV2` (Go `teststorage.Sample`).
#[derive(Clone, Default)]
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

fn exemplar_string(e: &Exemplar) -> String {
    let mut s = String::from("{");
    s.push_str(&e.labels.to_string());
    s.push(' ');
    write_g(&mut s, e.value);
    let _ = write!(s, " {} {}}}", e.ts, e.has_ts);
    s
}

/// Go `Sample.String`: roughly OpenMetrics 2.0, for readable test failures.
impl fmt::Display for Sample {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut b = String::new();
        if !self.m.help.is_empty() {
            b.push_str("HELP ");
            b.push_str(&self.m.help);
            b.push('\n');
        }
        if self.m.typ != MetricType::Unknown && self.m.typ != MetricType::Empty {
            b.push_str("type@");
            b.push_str(self.m.typ.as_str());
            b.push(' ');
        }
        if !self.m.unit.is_empty() {
            b.push_str("unit@");
            b.push_str(&self.m.unit);
            b.push(' ');
        }
        b.push_str(&self.l.to_string());
        b.push(' ');
        write_g(&mut b, self.v);
        if let Some(h) = &self.h {
            b.push(' ');
            b.push_str(&h.string());
        }
        if let Some(fh) = &self.fh {
            b.push(' ');
            b.push_str(&fh.string());
        }
        let _ = write!(b, " st@{} t@{}", self.st, self.t);
        if !self.es.is_empty() {
            b.push_str(" [");
            for (i, e) in self.es.iter().enumerate() {
                if i > 0 {
                    b.push(' ');
                }
                b.push_str(&exemplar_string(e));
            }
            b.push(']');
        }
        b.push('\n');
        f.write_str(&b)
    }
}

impl fmt::Debug for Sample {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

fn opt_eq<T>(a: &Option<T>, b: &Option<T>, eq: impl Fn(&T, &T) -> bool) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => eq(a, b),
        _ => false,
    }
}

impl Sample {
    /// Go `Sample.Equals`: floats compare by bits, so equal NaNs are equal.
    pub fn equals(&self, other: &Sample) -> bool {
        self.mf == other.mf
            && labels::equal(&self.l, &other.l)
            && self.m.equals(&other.m)
            && self.st == other.st
            && self.t == other.t
            && self.v.to_bits() == other.v.to_bits()
            && opt_eq(&self.h, &other.h, |a, b| a.equals(b))
            && opt_eq(&self.fh, &other.fh, |a, b| a.equals(b))
            && self.es.len() == other.es.len()
            && self.es.iter().zip(other.es.iter()).all(|(a, b)| a.equals(b))
    }

    /// Go `IsStale`: whether the sample is a staleness marker.
    pub fn is_stale(&self) -> bool {
        if let Some(fh) = &self.fh {
            return is_stale_nan(fh.sum);
        }
        if let Some(h) = &self.h {
            return is_stale_nan(h.sum);
        }
        is_stale_nan(self.v)
    }
}

impl PartialEq for Sample {
    fn eq(&self, other: &Sample) -> bool {
        self.equals(other)
    }
}

fn samples_equal(a: &[Sample], b: &[Sample]) -> bool {
    a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x.equals(y))
}

fn samples_string(s: &[Sample]) -> String {
    let mut out = String::from("[");
    for (i, x) in s.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(&x.to_string());
    }
    out.push(']');
    out
}

/// Go `RequireEqual`: compares samples with `Sample::equals`, ignoring the order of consecutive stale samples.
#[track_caller]
pub fn require_equal(expected: &[Sample], got: &[Sample]) {
    let expected = reorder_expected_for_staleness(expected, got);
    if !samples_equal(&expected, got) {
        panic!("samples not equal:\nexpected: {}\n     got: {}", samples_string(&expected), samples_string(got));
    }
}

/// Go `RequireEqual` with a message.
#[track_caller]
pub fn require_equal_msg(expected: &[Sample], got: &[Sample], msg: &str) {
    let expected = reorder_expected_for_staleness(expected, got);
    if !samples_equal(&expected, got) {
        panic!("{msg}: samples not equal:\nexpected: {}\n     got: {}", samples_string(&expected), samples_string(got));
    }
}

/// Go `RequireNotEqual`.
#[track_caller]
pub fn require_not_equal(expected: &[Sample], got: &[Sample]) {
    let expected = reorder_expected_for_staleness(expected, got);
    if samples_equal(&expected, got) {
        panic!("Equal, but expected not: \na: {}\nb: {}", samples_string(&expected), samples_string(got));
    }
}

/// Go `reorderExpectedForStaleness`.
pub fn reorder_expected_for_staleness(expected: &[Sample], got: &[Sample]) -> Vec<Sample> {
    if expected.len() != got.len() || !expected.iter().any(|s| s.is_stale()) {
        return expected.to_vec();
    }
    let mut result = expected.to_vec();
    for i in 0..result.len() {
        if !result[i].is_stale() || result[i].equals(&got[i]) {
            continue;
        }
        for j in i + 1..result.len() {
            if !result[j].is_stale() {
                break;
            }
            if result[j].equals(&got[i]) {
                result.swap(i, j);
                break;
            }
        }
    }
    result
}

type ErrFn = Arc<dyn Fn(&Labels) -> Option<Error> + Send + Sync>;
type RefFn = Arc<dyn Fn(&Labels) -> SeriesRef + Send + Sync>;

/// Both appendable kinds (Go `compatAppendable`).
pub trait CompatAppendable: AppendableT + AppendableV2 {}
impl<T: AppendableT + AppendableV2> CompatAppendable for T {}

#[derive(Default)]
struct Recorded {
    pending: Vec<Sample>,
    result: Vec<Sample>,
    rolledback: Vec<Sample>,
}

#[derive(Default)]
struct Inner {
    append_err_fn: Option<ErrFn>,
    ref_fn: Option<RefFn>,
    append_exemplars_error: Option<Error>,
    commit_err: Option<Error>,
    skip_recording: bool,
    open_appenders: AtomicI32,
    rec: Mutex<Recorded>,
    next: Option<Arc<dyn CompatAppendable>>,
}

/// A `storage::Appendable` and `storage::AppendableV2` mock (Go `teststorage.Appendable`). Opening a second
/// appender before the first is committed or rolled back gives an appender that fails every call.
#[derive(Clone, Default)]
pub struct Appendable(Arc<Mutex<Arc<Inner>>>);

static CLOSED_APPENDER: Sentinel = Sentinel::new("appender was already committed/rolledback");

impl Appendable {
    /// Go `NewAppendable`.
    pub fn new() -> Appendable {
        Appendable::default()
    }

    fn inner(&self) -> Arc<Inner> {
        self.0.lock().unwrap().clone()
    }

    fn update(&self, f: impl FnOnce(&mut Inner)) -> Appendable {
        {
            let mut g = self.0.lock().unwrap();
            let old = &**g;
            let mut n = Inner {
                append_err_fn: old.append_err_fn.clone(),
                ref_fn: old.ref_fn.clone(),
                append_exemplars_error: old.append_exemplars_error.clone(),
                commit_err: old.commit_err.clone(),
                skip_recording: old.skip_recording,
                open_appenders: AtomicI32::new(old.open_appenders.load(Ordering::SeqCst)),
                rec: Mutex::new(std::mem::take(&mut *old.rec.lock().unwrap())),
                next: old.next.clone(),
            };
            f(&mut n);
            *g = Arc::new(n);
        }
        self.clone()
    }

    /// Go `Then`: appenders also forward every call to an appender of `next`.
    pub fn then(&self, next: Arc<dyn CompatAppendable>) -> Appendable {
        self.update(|i| i.next = Some(next))
    }

    /// Go `WithErrs`.
    pub fn with_errs(
        &self,
        append_err_fn: Option<ErrFn>,
        append_exemplars_error: Option<Error>,
        commit_err: Option<Error>,
    ) -> Appendable {
        self.update(|i| {
            i.append_err_fn = append_err_fn;
            i.append_exemplars_error = append_exemplars_error;
            i.commit_err = commit_err;
        })
    }

    /// Go `WithRefFn`.
    pub fn with_ref_fn(&self, f: RefFn) -> Appendable {
        self.update(|i| i.ref_fn = Some(f))
    }

    /// Go `SkipRecording`.
    pub fn skip_recording(&self, skip: bool) -> Appendable {
        self.update(|i| i.skip_recording = skip)
    }

    /// The number of open appenders (Go `openAppenders.Load()`).
    pub fn open_appenders(&self) -> i32 {
        self.inner().open_appenders.load(Ordering::SeqCst)
    }

    /// Go `PendingSamples`: None when empty.
    pub fn pending_samples(&self) -> Vec<Sample> {
        self.inner().rec.lock().unwrap().pending.clone()
    }

    /// Go `ResultSamples`.
    pub fn result_samples(&self) -> Vec<Sample> {
        self.inner().rec.lock().unwrap().result.clone()
    }

    /// Go `RolledbackSamples`.
    pub fn rolledback_samples(&self) -> Vec<Sample> {
        self.inner().rec.lock().unwrap().rolledback.clone()
    }

    /// Go `ResultReset`.
    pub fn result_reset(&self) {
        let i = self.inner();
        let mut r = i.rec.lock().unwrap();
        r.pending.clear();
        r.result.clear();
        r.rolledback.clear();
    }

    /// Go `ResultMetadata`: committed samples that carry metadata, reduced to labels and metadata.
    pub fn result_metadata(&self) -> Vec<Sample> {
        let i = self.inner();
        let r = i.rec.lock().unwrap();
        r.result
            .iter()
            .filter(|s| !s.m.is_empty())
            .map(|s| Sample { l: s.l.clone(), m: s.m.clone(), ..Default::default() })
            .collect()
    }
}

impl fmt::Display for Appendable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let i = self.inner();
        let r = i.rec.lock().unwrap();
        f.write_str("committed:\n")?;
        for s in &r.result {
            write!(f, "\n{s}")?;
        }
        f.write_str("pending:\n")?;
        for s in &r.pending {
            write!(f, "\n{s}")?;
        }
        f.write_str("rolledback:\n")?;
        for s in &r.rolledback {
            write!(f, "\n{s}")?;
        }
        Ok(())
    }
}

impl Inner {
    fn compute_or_check_ref(&self, r: SeriesRef, ls: &Labels) -> Result<SeriesRef, Error> {
        if let Some(f) = &self.ref_fn {
            return Ok(f(ls));
        }
        let h = ls.hash();
        if r == 0 {
            return Ok(h);
        }
        if h != r {
            return Err(Error::msg(
                "teststorage.appender: found input ref not matching labels; potential bug in Appendable usage",
            ));
        }
        Ok(r)
    }
}

struct Base {
    err: Option<Error>,
    a: Arc<Inner>,
}

impl Base {
    fn check_err(&self) -> Result<(), Error> {
        match &self.err {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    fn commit(&mut self, next: Option<&mut dyn AppenderTransaction>) -> Option<Error> {
        if let Err(e) = self.check_err() {
            return Some(e);
        }
        if let Some(e) = &self.a.commit_err {
            self.a.open_appenders.fetch_sub(1, Ordering::SeqCst);
            return Some(e.clone());
        }
        {
            let mut r = self.a.rec.lock().unwrap();
            if !self.a.skip_recording {
                let p = std::mem::take(&mut r.pending);
                r.result.extend(p);
            }
        }
        self.err = Some(Error::Sentinel(&CLOSED_APPENDER));
        self.a.open_appenders.fetch_sub(1, Ordering::SeqCst);
        next.and_then(|n| n.commit())
    }

    fn rollback(&mut self, next: Option<&mut dyn AppenderTransaction>) -> Option<Error> {
        if let Err(e) = self.check_err() {
            return Some(e);
        }
        {
            let mut r = self.a.rec.lock().unwrap();
            if !self.a.skip_recording {
                let p = std::mem::take(&mut r.pending);
                r.rolledback.extend(p);
            }
        }
        self.err = Some(Error::Sentinel(&CLOSED_APPENDER));
        self.a.open_appenders.fetch_sub(1, Ordering::SeqCst);
        next.and_then(|n| n.rollback())
    }
}

/// The V1 appender of [`Appendable`].
pub struct TestAppender {
    base: Base,
    next: Option<Box<dyn Appender>>,
}

impl AppendableT for Appendable {
    fn appender(&self, ctx: &Context) -> Box<dyn Appender> {
        let a = self.inner();
        let mut ret = TestAppender { base: Base { err: None, a: a.clone() }, next: None };
        if a.open_appenders.fetch_add(1, Ordering::SeqCst) + 1 > 1 {
            ret.base.err = Some(Error::msg("teststorage.Appendable.Appender() concurrent use is not supported; attempted opening new Appender() without Commit/Rollback of the previous one. Extend the implementation if concurrent mock is needed"));
            return Box::new(ret);
        }
        if let Some(n) = &a.next {
            ret.next = Some(n.appender(ctx));
        }
        Box::new(ret)
    }
}

impl AppenderTransaction for TestAppender {
    fn commit(&mut self) -> Option<Error> {
        let next = self.next.as_mut().map(|n| &mut **n as &mut dyn AppenderTransaction);
        self.base.commit(next)
    }
    fn rollback(&mut self) -> Option<Error> {
        let next = self.next.as_mut().map(|n| &mut **n as &mut dyn AppenderTransaction);
        self.base.rollback(next)
    }
}

impl Appender for TestAppender {
    fn append(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        self.base.check_err()?;
        let a = &self.base.a;
        if let Some(f) = &a.append_err_fn
            && let Some(e) = f(l)
        {
            return Err(e);
        }
        if !a.skip_recording {
            a.rec.lock().unwrap().pending.push(Sample { l: l.clone(), t, v, ..Default::default() });
        }
        if let Some(n) = &mut self.next {
            return n.append(r, l, t, v);
        }
        a.compute_or_check_ref(r, l)
    }

    fn set_options(&mut self, _opts: Option<&AppendOptions>) {}

    fn append_exemplar(&mut self, r: SeriesRef, l: &Labels, e: &Exemplar) -> Result<SeriesRef, Error> {
        self.base.check_err()?;
        let a = &self.base.a;
        if let Some(err) = &a.append_exemplars_error {
            return Err(err.clone());
        }
        if !a.skip_recording {
            let mut appended = false;
            {
                let mut rec = a.rec.lock().unwrap();
                for s in rec.pending.iter_mut().rev() {
                    if labels::equal(l, &s.l) {
                        s.es.push(e.clone());
                        appended = true;
                        break;
                    }
                }
            }
            if !appended {
                return Err(Error::msg(format!(
                    "teststorage.appender: exemplar appender without series; ref {}; l {}; exemplar: {}",
                    r,
                    l,
                    exemplar_string(e)
                )));
            }
        }
        if let Some(n) = &mut self.next {
            return n.append_exemplar(r, l, e);
        }
        a.compute_or_check_ref(r, l)
    }

    fn append_histogram(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.base.check_err()?;
        let a = &self.base.a;
        if let Some(f) = &a.append_err_fn
            && let Some(e) = f(l)
        {
            return Err(e);
        }
        if !a.skip_recording {
            a.rec.lock().unwrap().pending.push(Sample {
                l: l.clone(),
                t,
                h: h.cloned(),
                fh: fh.cloned(),
                ..Default::default()
            });
        }
        if let Some(n) = &mut self.next {
            return n.append_histogram(r, l, t, h, fh);
        }
        a.compute_or_check_ref(r, l)
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

    fn update_metadata(&mut self, r: SeriesRef, l: &Labels, m: &Metadata) -> Result<SeriesRef, Error> {
        self.base.check_err()?;
        let a = &self.base.a;
        if !a.skip_recording {
            let mut updated = false;
            {
                let mut rec = a.rec.lock().unwrap();
                for s in rec.pending.iter_mut().rev() {
                    if labels::equal(l, &s.l) {
                        s.m = m.clone();
                        updated = true;
                        break;
                    }
                }
            }
            if !updated {
                return Err(Error::msg(format!(
                    "teststorage.appender: metadata update without series; ref {}; l {}; m: {{{} {} {}}}",
                    r,
                    l,
                    m.typ.as_str(),
                    m.unit,
                    m.help
                )));
            }
        }
        if let Some(n) = &mut self.next {
            return n.update_metadata(r, l, m);
        }
        a.compute_or_check_ref(r, l)
    }

    fn append_st_zero_sample(&mut self, r: SeriesRef, l: &Labels, _t: i64, st: i64) -> Result<SeriesRef, Error> {
        self.append(r, l, st, 0.0)
    }
}

/// The V2 appender of [`Appendable`].
pub struct TestAppenderV2 {
    base: Base,
    next: Option<Box<dyn AppenderV2>>,
}

impl AppendableV2 for Appendable {
    fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
        let a = self.inner();
        let mut ret = TestAppenderV2 { base: Base { err: None, a: a.clone() }, next: None };
        if a.open_appenders.fetch_add(1, Ordering::SeqCst) + 1 > 1 {
            ret.base.err = Some(Error::msg("teststorage.Appendable.AppenderV2() concurrent use is not supported; attempted opening new AppenderV2() without Commit/Rollback of the previous one. Extend the implementation if concurrent mock is needed"));
            return Box::new(ret);
        }
        if let Some(n) = &a.next {
            ret.next = Some(n.appender_v2(ctx));
        }
        Box::new(ret)
    }
}

impl AppenderTransaction for TestAppenderV2 {
    fn commit(&mut self) -> Option<Error> {
        let next = self.next.as_mut().map(|n| &mut **n as &mut dyn AppenderTransaction);
        self.base.commit(next)
    }
    fn rollback(&mut self) -> Option<Error> {
        let next = self.next.as_mut().map(|n| &mut **n as &mut dyn AppenderTransaction);
        self.base.rollback(next)
    }
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
        self.base.check_err()?;
        let a = self.base.a.clone();
        if let Some(f) = &a.append_err_fn
            && let Some(e) = f(ls)
        {
            return Err(e);
        }
        let mut partial: Option<AppendPartialError> = None;
        if !a.skip_recording {
            let mut es = Vec::new();
            if !opts.exemplars.is_empty() {
                if let Some(err) = &a.append_exemplars_error {
                    let ex: Vec<ExemplarError> = (0..opts.exemplars.len())
                        .map(|i| ExemplarError { index: i, err: err.clone() })
                        .collect();
                    partial = Some(AppendPartialError { series_ref: 0, exemplar_errors: ex });
                } else {
                    es = opts.exemplars.to_vec();
                }
            }
            a.rec.lock().unwrap().pending.push(Sample {
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
        }
        let r = if let Some(n) = &mut self.next {
            n.append(r, ls, st, t, v, h, fh, opts)?
        } else {
            a.compute_or_check_ref(r, ls)?
        };
        match partial {
            Some(mut p) => {
                p.series_ref = r;
                Err(Error::AppendPartial(Arc::new(p)))
            }
            None => Ok(r),
        }
    }

    fn as_exemplar_appender_v2(&mut self) -> Option<&mut dyn ExemplarAppenderV2> {
        Some(self)
    }
}

impl ExemplarAppenderV2 for TestAppenderV2 {
    fn append_exemplars(&mut self, r: SeriesRef, l: &Labels, exemplars: &[Exemplar]) -> Result<SeriesRef, Error> {
        self.base.check_err()?;
        let a = self.base.a.clone();
        let mut pending_idx: Option<usize> = None;
        let mut matched = Labels::default();
        if !a.skip_recording {
            let rec = a.rec.lock().unwrap();
            let m = |s: &Sample| (!l.is_empty() && labels::equal(l, &s.l)) || (r != 0 && s.l.hash() == r);
            if let Some(i) = rec.pending.iter().rposition(m) {
                pending_idx = Some(i);
                matched = rec.pending[i].l.clone();
            } else if let Some(s) = rec.result.iter().rev().find(|s| m(s)) {
                matched = s.l.clone();
            }
            drop(rec);
            if matched.is_empty() {
                let es: Vec<String> = exemplars.iter().map(exemplar_string).collect();
                return Err(Error::wrapped(
                    format!(
                        "teststorage.appenderV2: exemplars appended without series; ref {}; l {}; exemplars: [{}]: {}",
                        r,
                        l,
                        es.join(" "),
                        ERR_NOT_FOUND
                    ),
                    ERR_NOT_FOUND,
                ));
            }
        }
        let mut partial: Option<AppendPartialError> = None;
        let mut r = r;
        let next_ex = self.next.as_mut().and_then(|n| n.as_exemplar_appender_v2());
        if let Some(next) = next_ex {
            match next.append_exemplars(r, l, exemplars) {
                Ok(nr) => r = nr,
                Err(e) => {
                    if let Some(p) = e.as_append_partial() {
                        r = p.series_ref;
                    }
                    if let Some(e) = AppendPartialError::handle(&mut partial, Some(e)) {
                        return Err(e);
                    }
                }
            }
        } else {
            let check = if l.is_empty() { &matched } else { l };
            r = a.compute_or_check_ref(r, check)?;
        }
        if !a.skip_recording && !exemplars.is_empty() {
            if let Some(err) = &a.append_exemplars_error {
                let ex: Vec<ExemplarError> =
                    (0..exemplars.len()).map(|i| ExemplarError { index: i, err: err.clone() }).collect();
                let pe = AppendPartialError { series_ref: r, exemplar_errors: ex };
                let _ = AppendPartialError::handle(&mut partial, Some(Error::AppendPartial(Arc::new(pe))));
            } else {
                let mut rec = a.rec.lock().unwrap();
                match pending_idx {
                    Some(i) if i < rec.pending.len() => rec.pending[i].es.extend_from_slice(exemplars),
                    _ => rec.pending.push(Sample { l: matched.clone(), es: exemplars.to_vec(), ..Default::default() }),
                }
            }
        }
        if let Some(p) = &mut partial {
            p.series_ref = r;
        }
        match AppendPartialError::to_error(partial.as_ref()) {
            Some(e) => Err(e),
            None => Ok(r),
        }
    }
}

/// Exemplar comparison used for sorting (Go `exemplar.Compare`), re-exported for tests.
pub fn compare_exemplars(a: &Exemplar, b: &Exemplar) -> std::cmp::Ordering {
    exemplar::compare(a, b).cmp(&0)
}
