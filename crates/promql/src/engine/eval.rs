//! The evaluator (Go `evaluator`): evaluation of expression nodes over a range of steps.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;
use std::sync::Arc;

use chunkenc::{AnyIterator, ChunkIterator, ValueType as VT};
use model::histogram::{CounterResetHint, FloatHistogram};
use model::labels::{Builder, Labels, METRIC_NAME, ValidationScheme};
use model::schema::{self, METRIC_TYPE_LABEL, METRIC_UNIT_LABEL};
use model::value::is_stale_nan;
use storage::annotations as annos;
use storage::{
    Annotations, BoxSeriesSet, BufferedSeriesIterator, Context, Error, MemoizedSeriesIterator, Querier, SeriesArc,
    new_buffer, new_memoized_empty_iterator, new_memoized_iterator,
};
use util::stats::{QuerySamples, QueryTiming, SubqueryConsumer};

use super::*;
use crate::functions::{self, FunctionCall};
use crate::histogram_stats_iterator::HistogramStatsIterator;
use crate::parser::{
    AggregateExpr, Call, MatrixSelector, SubqueryExpr, VectorMatching, VectorSelector, COUNT_VALUES, LAND, LOR,
    LUNLESS, SUB,
};
use crate::quantile::MetricWithBuckets;

/// The maximum initial capacity of point slices (Go `maxPointsSliceSize`).
const MAX_POINTS_SLICE_SIZE: usize = 5000;

pub(crate) type EvalResult<T> = Result<T, Error>;

/// Converts a parser position range to an annotation position range.
#[inline]
pub(crate) fn pr(p: parser::PositionRange) -> storage::annotations::PositionRange {
    storage::annotations::PositionRange::new(p.start as i64, p.end as i64)
}

/// Go `durationMilliseconds` for nanosecond durations.
#[inline]
pub(crate) fn dur_ms(ns: i64) -> i64 {
    ns / 1_000_000
}

/// The shared budget of the query-cost feature (Go `queryBudget`).
#[derive(Debug, Default)]
pub struct QueryBudget {
    pub max_series: i64,
    pub max_samples_read: i64,
    pub series: Cell<i64>,
    pub samples_read: Cell<i64>,
}

/// Saturating addition (Go `addSaturatingInt64`).
pub(crate) fn add_saturating_i64(a: i64, b: i64) -> i64 {
    a.saturating_add(b)
}

enum SelectorSeries {
    Unexpanded(BoxSeriesSet),
    Expanded(Rc<Vec<SeriesArc>>),
}

/// Per-query state that Go keeps in the AST: selected series and `@`-adjusted offsets, keyed by node address.
#[derive(Default)]
pub struct QueryState {
    selectors: RefCell<HashMap<usize, SelectorSeries>>,
    offsets: RefCell<HashMap<usize, i64>>,
}

#[inline]
pub(crate) fn addr<T>(n: &T) -> usize {
    n as *const T as usize
}

impl QueryState {
    pub(crate) fn set_offset(&self, node: usize, off: i64) {
        self.offsets.borrow_mut().insert(node, off);
    }

    pub(crate) fn vs_offset(&self, vs: &VectorSelector) -> i64 {
        self.offsets.borrow().get(&addr(vs)).copied().unwrap_or(vs.original_offset)
    }

    pub(crate) fn subquery_offset(&self, sq: &SubqueryExpr) -> i64 {
        self.offsets.borrow().get(&addr(sq)).copied().unwrap_or(sq.original_offset)
    }

    pub(crate) fn set_unexpanded(&self, vs: &VectorSelector, set: BoxSeriesSet) {
        self.selectors.borrow_mut().insert(addr(vs), SelectorSeries::Unexpanded(set));
    }
}

/// Extra information about a series (Go `EvalSeriesHelper`).
#[derive(Clone, Copy, Debug, Default)]
pub struct EvalSeriesHelper {
    /// The ordinal of the join signature, used to match left-hand to right-hand side in binary operations.
    pub sig_ordinal: usize,
}

/// Information and caches for evaluating a single node across steps (Go `EvalNodeHelper`).
#[derive(Default)]
pub struct EvalNodeHelper {
    /// The evaluation timestamp.
    pub ts: i64,
    /// The output vector of the current step.
    pub out: Vector,
    /// The lookback delta of the query, in nanoseconds.
    pub lookback_delta: i64,
    /// The range of the matrix argument, in nanoseconds.
    pub range: i64,
    /// The offset of the matrix argument, in nanoseconds.
    pub offset: i64,
    pub anchored: bool,
    pub smoothed: bool,
    /// Sample start timestamps aligned with the matrix argument, if requested.
    pub start_timestamps: Option<StartTimestamps>,
    pub enable_delayed_name_removal: bool,

    // Caches of histogram_quantile, histogram_fraction and histogram_quantiles.
    pub(crate) signature_to_metric_with_buckets: HashMap<Vec<u8>, MetricWithBuckets>,
    pub(crate) native_histogram_samples: Vec<Sample>,
    pub(crate) quantile_strs: HashMap<u64, String>,
    pub(crate) signature_to_labels_with_quantile: HashMap<Vec<u8>, HashMap<u64, Labels>>,

    pub(crate) lb: Option<Builder>,
    pub(crate) lbl_buf: Vec<u8>,
    pub(crate) lbl_result_buf: Vec<u8>,

    // Binary vector matching.
    pub(crate) right_sigs: Vec<Option<Sample>>,
    pub(crate) sigs_present: Vec<bool>,
    pub(crate) matched_sigs: Vec<HashSet<u64>>,
    pub(crate) matched_sigs_present: Vec<bool>,
    pub(crate) result_metric: HashMap<Vec<u8>, Labels>,
    pub(crate) num_sigs: usize,

    // Info series matching.
    pub(crate) right_str_sigs: HashMap<String, Sample>,
}

impl EvalNodeHelper {
    pub(crate) fn new(lookback_delta: i64, enable_delayed_name_removal: bool) -> EvalNodeHelper {
        EvalNodeHelper { lookback_delta, enable_delayed_name_removal, ..Default::default() }
    }

    pub(crate) fn reset_sigs_present(&mut self) {
        if self.sigs_present.is_empty() {
            self.sigs_present = vec![false; self.num_sigs];
        } else {
            self.sigs_present.iter_mut().for_each(|b| *b = false);
        }
    }

    pub(crate) fn reset_matched_sigs_present(&mut self) {
        if self.matched_sigs_present.is_empty() {
            self.matched_sigs_present = vec![false; self.num_sigs];
        } else {
            self.matched_sigs_present.iter_mut().for_each(|b| *b = false);
        }
    }

    pub(crate) fn reset_right_sigs(&mut self) {
        if self.right_sigs.is_empty() {
            self.right_sigs = vec![None; self.num_sigs];
        } else {
            self.right_sigs.iter_mut().for_each(|s| *s = None);
        }
    }

    pub(crate) fn reset_matched_sigs(&mut self) {
        if self.matched_sigs.is_empty() {
            self.matched_sigs = vec![HashSet::new(); self.num_sigs];
        } else {
            self.matched_sigs.iter_mut().for_each(|s| s.clear());
        }
    }

    /// Go `resetBuilder`: the label builder, reset to `lbls`.
    pub(crate) fn reset_builder(&mut self, lbls: Labels) -> &mut Builder {
        match &mut self.lb {
            Some(b) => b.reset(lbls),
            None => self.lb = Some(Builder::new(lbls)),
        }
        self.lb.as_mut().unwrap()
    }
}

/// The range vector argument of a function: storage series or the result of a subquery.
pub struct MatrixArg {
    pub series: Rc<Vec<SeriesArc>>,
    /// Go `StorageSeries.series.DropName` of subquery results; empty for selectors.
    pub drop_names: Vec<bool>,
    /// The offset in nanoseconds.
    pub offset: i64,
    /// The range in nanoseconds.
    pub range: i64,
    pub timestamp: Option<i64>,
    pub anchored: bool,
    pub smoothed: bool,
}

/// Evaluates expressions over fixed timestamps (Go `evaluator`).
pub struct Evaluator<'a> {
    pub(crate) start_timestamp: i64,
    pub(crate) end_timestamp: i64,
    pub(crate) interval: i64,
    pub(crate) max_samples: i64,
    pub(crate) current_samples: i64,
    pub(crate) budget: Option<&'a QueryBudget>,
    /// The lookback delta in nanoseconds.
    pub(crate) lookback_delta: i64,
    pub(crate) samples_stats: QuerySamples,
    pub(crate) no_step_subquery_interval_fn: &'a (dyn Fn(i64) -> i64 + Send + Sync),
    pub(crate) enable_delayed_name_removal: bool,
    pub(crate) enable_type_and_unit_labels: bool,
    pub(crate) use_start_timestamps: bool,
    pub(crate) querier: &'a dyn Querier,
    pub(crate) ctx: &'a Context,
    pub(crate) state: &'a QueryState,
}

/// Go `ErrTooManySamples(env)`.
pub(crate) fn err_too_many_samples() -> Error {
    PromqlError::TooManySamples(ENV.to_string()).err()
}

pub(crate) fn errorf(msg: impl Into<String>) -> Error {
    Error::msg(msg.into())
}

/// Go `generateGroupingKey`.
pub(crate) fn generate_grouping_key(metric: &Labels, grouping: &[String], without: bool, buf: &mut Vec<u8>) -> u64 {
    if without {
        return metric.hash_without_labels(buf, grouping);
    }
    if grouping.is_empty() {
        return 0;
    }
    metric.hash_for_labels(buf, grouping)
}

/// Go `generateGroupingLabels`.
pub(crate) fn generate_grouping_labels(
    enh: &mut EvalNodeHelper,
    metric: &Labels,
    without: bool,
    grouping: &[String],
) -> Labels {
    if without {
        let lb = enh.reset_builder(metric.clone());
        lb.del_many(grouping);
        schema::Metadata::default().set_to_labels(lb);
        lb.labels()
    } else if !grouping.is_empty() {
        let lb = enh.reset_builder(metric.clone());
        lb.keep(grouping);
        lb.labels()
    } else {
        Labels::empty()
    }
}

/// Drops the metric name, type and unit labels (Go `DropReserved(schema.IsMetadataLabel)`).
#[inline]
pub(crate) fn drop_metadata(l: &Labels) -> Labels {
    l.drop_reserved(schema::is_metadata_label)
}

/// A series whose iterator only decodes histogram count and sum (Go `histogramStatsSeries`).
struct HistogramStatsSeries(SeriesArc);

impl storage::Series for HistogramStatsSeries {
    fn labels(&self) -> &Labels {
        self.0.labels()
    }
    fn iterator<'a>(&'a self, _reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        AnyIterator::Other(Box::new(HistogramStatsIterator::new(self.0.iterator(None))))
    }
}

/// Go `addToSeries`.
#[inline]
fn add_to_series(ss: &mut Series, ts: i64, f: f64, h: Option<HistogramRef>, num_steps: usize) {
    match h {
        None => {
            if ss.floats.capacity() == 0 {
                ss.floats.reserve_exact(num_steps.min(MAX_POINTS_SLICE_SIZE));
            }
            ss.floats.push(FPoint { t: ts, f });
        }
        Some(h) => {
            if ss.histograms.capacity() == 0 {
                ss.histograms.reserve_exact(num_steps.min(MAX_POINTS_SLICE_SIZE));
            }
            ss.histograms.push(HPoint { t: ts, h });
        }
    }
}

/// A matrix that is consumed step by step (Go slices `series.Floats[1:]`).
pub(crate) struct CursorMatrix {
    pub mat: Matrix,
    pub fpos: Vec<u32>,
    pub hpos: Vec<u32>,
}

impl CursorMatrix {
    pub fn new(mat: Matrix) -> CursorMatrix {
        let n = mat.len();
        CursorMatrix { mat, fpos: vec![0; n], hpos: vec![0; n] }
    }

    /// Go `nextValues`: the value of series `i` at `ts`, consuming it.
    #[inline]
    pub fn next_values(&mut self, i: usize, ts: i64) -> Option<(f64, Option<&HistogramRef>)> {
        let s = &self.mat[i];
        let fp = self.fpos[i] as usize;
        if fp < s.floats.len() && s.floats[fp].t == ts {
            self.fpos[i] += 1;
            return Some((s.floats[fp].f, None));
        }
        let hp = self.hpos[i] as usize;
        if hp < s.histograms.len() && s.histograms[hp].t == ts {
            self.hpos[i] += 1;
            return Some((0.0, Some(&s.histograms[hp].h)));
        }
        None
    }
}

impl<'a> Evaluator<'a> {
    #[inline]
    fn num_steps_total(&self) -> usize {
        ((self.end_timestamp - self.start_timestamp) / self.interval) as usize + 1
    }

    /// Go `numSteps`.
    pub(crate) fn num_steps(&self) -> usize {
        if self.interval <= 0 {
            return 1;
        }
        ((self.end_timestamp - self.start_timestamp) / self.interval) as usize + 1
    }

    #[inline]
    pub(crate) fn lookback_ms(&self) -> i64 {
        dur_ms(self.lookback_delta)
    }

    #[inline]
    pub(crate) fn check_ctx(&self, env: &str) -> EvalResult<()> {
        context_done(self.ctx, env)
    }

    #[inline]
    pub(crate) fn check_samples(&self) -> EvalResult<()> {
        if self.current_samples > self.max_samples {
            return Err(err_too_many_samples());
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn update_peak(&mut self) {
        let c = self.current_samples.max(0) as usize;
        self.samples_stats.update_peak(c);
    }

    pub(crate) fn consume_series(&self) -> EvalResult<()> {
        if let Some(b) = self.budget {
            b.series.set(b.series.get() + 1);
            if b.max_series > 0 && b.series.get() > b.max_series {
                return Err(PromqlError::TooManySeries(ENV.to_string()).err());
            }
        }
        Ok(())
    }

    pub(crate) fn consume_samples(&self, n: i64) -> EvalResult<()> {
        if let Some(b) = self.budget {
            b.samples_read.set(add_saturating_i64(b.samples_read.get(), n));
            if b.max_samples_read > 0 && b.samples_read.get() > b.max_samples_read {
                return Err(PromqlError::TooManySamplesRead(ENV.to_string()).err());
            }
        }
        Ok(())
    }

    /// Go `consumeMatrixSamples`. A negative step skips subquery output.
    pub(crate) fn consume_matrix_samples(&mut self, n: i64, step: isize) -> EvalResult<()> {
        if self.budget.is_none() || step < 0 {
            return Ok(());
        }
        self.samples_stats.increment_samples_read_at_step(step as usize, n);
        self.consume_samples(n)
    }

    /// A child evaluator with the same settings (Go creates `&evaluator{...}`).
    pub(crate) fn child(&self, start: i64, end: i64, interval: i64, samples_stats: QuerySamples) -> Evaluator<'a> {
        Evaluator {
            start_timestamp: start,
            end_timestamp: end,
            interval,
            max_samples: self.max_samples,
            current_samples: self.current_samples,
            budget: self.budget,
            lookback_delta: self.lookback_delta,
            samples_stats,
            no_step_subquery_interval_fn: self.no_step_subquery_interval_fn,
            enable_delayed_name_removal: self.enable_delayed_name_removal,
            enable_type_and_unit_labels: self.enable_type_and_unit_labels,
            use_start_timestamps: self.use_start_timestamps,
            querier: self.querier,
            ctx: self.ctx,
            state: self.state,
        }
    }

    pub(crate) fn new_enh(&self) -> EvalNodeHelper {
        EvalNodeHelper::new(self.lookback_delta, self.enable_delayed_name_removal)
    }

    /// Go `Eval`: evaluates the expression, turning panics into errors, and removes delayed metric names.
    pub fn eval_top(&mut self, expr: &Expr) -> (Option<Value>, Annotations, Option<Error>) {
        let res = catch_unwind(AssertUnwindSafe(|| {
            let (v, ws) = self.eval(expr)?;
            let v = if self.enable_delayed_name_removal { self.cleanup_metric_labels(v)? } else { v };
            Ok::<_, Error>((v, ws))
        }));
        match res {
            Ok(Ok((v, ws))) => (Some(v), ws, None),
            Ok(Err(e)) => match e.downcast_ref::<ErrWithWarnings>() {
                Some(w) => (None, w.warnings.clone(), Some(w.err.clone())),
                None => (None, Annotations::new(), Some(e)),
            },
            Err(p) => {
                let msg = p
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default();
                (None, Annotations::new(), Some(Error::msg(format!("unexpected error: {}", msg))))
            }
        }
    }

    /// Evaluates an expression that yields a matrix.
    pub(crate) fn eval_matrix(&mut self, expr: &Expr) -> EvalResult<(Matrix, Annotations)> {
        let (v, ws) = self.eval(expr)?;
        match v {
            Value::Matrix(m) => Ok((m, ws)),
            other => Err(errorf(format!("unexpected value type {}", other.value_type()))),
        }
    }

    // -------------------------------------------------------------------------------------------------------------
    // Series expansion.

    /// Go `checkAndExpandSeriesSet`.
    pub(crate) fn check_and_expand_series_set(
        &mut self,
        vs: &VectorSelector,
    ) -> EvalResult<(Rc<Vec<SeriesArc>>, Annotations)> {
        let key = addr(vs);
        let entry = self.state.selectors.borrow_mut().remove(&key);
        let set = match entry {
            Some(SelectorSeries::Expanded(s)) => {
                self.state.selectors.borrow_mut().insert(key, SelectorSeries::Expanded(s.clone()));
                return Ok((s, Annotations::new()));
            }
            Some(SelectorSeries::Unexpanded(set)) => Some(set),
            None => None,
        };
        let (mut series, ws, err) = match set {
            Some(set) => self.expand_series_set(set),
            None => (Vec::new(), Annotations::new(), None),
        };
        if vs.skip_histogram_buckets {
            for s in series.iter_mut() {
                *s = Arc::new(HistogramStatsSeries(s.clone()));
            }
        }
        let series = Rc::new(series);
        self.state.selectors.borrow_mut().insert(key, SelectorSeries::Expanded(series.clone()));
        match err {
            None => Ok((series, ws)),
            Some(e) => Err(Error::other(ErrWithWarnings { err: Error::wrap("expanding series", e), warnings: ws })),
        }
    }

    /// Go `expandSeriesSet`.
    fn expand_series_set(&mut self, mut it: BoxSeriesSet) -> (Vec<SeriesArc>, Annotations, Option<Error>) {
        let mut res = Vec::new();
        while it.next() {
            if let Some(e) = self.ctx.err() {
                return (Vec::new(), Annotations::new(), Some(e));
            }
            res.push(it.at().clone());
            self.samples_stats.increment_series(1);
            if let Err(e) = self.consume_series() {
                return (res, Annotations::new(), Some(e));
            }
        }
        (res, it.warnings(), it.err())
    }

    // -------------------------------------------------------------------------------------------------------------
    // The main evaluation.

    /// Go `eval`.
    pub fn eval(&mut self, expr: &Expr) -> EvalResult<(Value, Annotations)> {
        self.check_ctx("expression evaluation")?;
        if self.end_timestamp < self.start_timestamp {
            return Ok((Value::Matrix(Vec::new()), Annotations::new()));
        }
        match expr {
            Expr::Aggregate(e) => self.eval_aggregate(e),
            Expr::Call(e) => self.eval_call(e),
            Expr::Paren(e) => self.eval(&e.expr),
            Expr::Unary(e) => {
                let (mut mat, ws) = self.eval_matrix(&e.expr)?;
                if e.op == SUB {
                    for s in mat.iter_mut() {
                        if !self.enable_delayed_name_removal {
                            s.metric = drop_metadata(&s.metric);
                        }
                        s.drop_name = true;
                        for p in s.floats.iter_mut() {
                            p.f = -p.f;
                        }
                        for p in s.histograms.iter_mut() {
                            let mut h = (*p.h).clone();
                            h.mul(-1.0);
                            p.h = Arc::new(h);
                        }
                    }
                    if !self.enable_delayed_name_removal {
                        mat = self.merge_series_with_same_labelset(mat)?;
                    }
                }
                Ok((Value::Matrix(mat), ws))
            }
            Expr::Binary(e) => self.eval_binary(e),
            Expr::NumberLiteral(e) => {
                let v = e.val;
                let (m, ws) = self.range_eval(None, &[], |_, _, _, enh| {
                    enh.out.push(Sample { f: v, ..Default::default() });
                    Ok(Annotations::new())
                })?;
                Ok((Value::Matrix(m), ws))
            }
            Expr::StringLiteral(e) => {
                Ok((Value::String(StringValue { v: e.val.clone(), t: self.start_timestamp }), Annotations::new()))
            }
            Expr::VectorSelector(e) => {
                let (series, mut ws) = self.check_and_expand_series_set(e)?;
                let offset = self.state.vs_offset(e);
                if e.smoothed {
                    let (mat, a) = self.smooth_series(&series, offset, pr(e.pos_range))?;
                    ws.merge(a);
                    return Ok((Value::Matrix(mat), ws));
                }
                let mat = self.eval_series(&series, offset, false, None)?;
                Ok((Value::Matrix(mat), ws))
            }
            Expr::MatrixSelector(e) => {
                if self.start_timestamp != self.end_timestamp {
                    panic!("cannot do range evaluation of matrix selector");
                }
                let (m, ws) = self.matrix_selector(e)?;
                Ok((Value::Matrix(m), ws))
            }
            Expr::Subquery(e) => {
                let (res, child_stats, ws) = self.run_subquery(e)?;
                self.samples_stats.update_peak_from_subquery(&child_stats);
                self.samples_stats.merge_series_from_subquery(&child_stats);
                let consumer = SubqueryConsumer {
                    start: self.start_timestamp,
                    interval: self.interval,
                    num_steps: self.num_steps(),
                    ..Default::default()
                };
                self.samples_stats.merge_total_samples_from_subquery(&child_stats, &consumer);
                self.samples_stats.merge_samples_read_from_subquery(&child_stats, &consumer);
                Ok((res, ws))
            }
            Expr::StepInvariant(e) => self.eval_step_invariant(&e.expr),
            Expr::Duration(_) => panic!("unhandled expression of type: *parser.DurationExpr"),
        }
    }

    fn eval_step_invariant(&mut self, inner: &Expr) -> EvalResult<(Value, Annotations)> {
        let child_stats = self.samples_stats.new_child();
        let mut new_ev = self.child(self.start_timestamp, self.start_timestamp, self.interval, child_stats);
        let (res, ws) = new_ev.eval(inner)?;
        self.current_samples = new_ev.current_samples;
        self.samples_stats.update_peak_from_subquery(&new_ev.samples_stats);
        self.samples_stats.merge_series_from_subquery(&new_ev.samples_stats);
        let mut ts = self.start_timestamp;
        let mut step = 0;
        while ts <= self.end_timestamp {
            self.samples_stats.increment_samples_at_step(step, new_ev.samples_stats.total_samples);
            ts += self.interval;
            step += 1;
        }
        self.samples_stats.increment_samples_read_at_step(0, new_ev.samples_stats.samples_read);
        if matches!(inner, Expr::MatrixSelector(_) | Expr::Subquery(_)) {
            return Ok((res, ws));
        }
        let mut mat = match res {
            Value::Matrix(m) => m,
            _ => panic!("unexpected result in StepInvariantExpr evaluation"),
        };
        for s in mat.iter_mut() {
            if s.floats.len() + s.histograms.len() != 1 {
                panic!("unexpected number of samples");
            }
            let mut ts = self.start_timestamp + self.interval;
            while ts <= self.end_timestamp {
                if !s.floats.is_empty() {
                    let f = s.floats[0].f;
                    s.floats.push(FPoint { t: ts, f });
                    self.current_samples += 1;
                } else {
                    let p = HPoint { t: ts, h: s.histograms[0].h.clone() };
                    self.current_samples += p.size() as i64;
                    s.histograms.push(p);
                }
                self.check_samples()?;
                ts += self.interval;
            }
        }
        self.update_peak();
        Ok((Value::Matrix(mat), ws))
    }

    fn eval_aggregate(&mut self, e: &AggregateExpr) -> EvalResult<(Value, Annotations)> {
        let mut sorted_grouping: Vec<String> = e.grouping.clone();
        if e.without {
            sorted_grouping.insert(0, METRIC_UNIT_LABEL.to_string());
            sorted_grouping.insert(0, METRIC_TYPE_LABEL.to_string());
            sorted_grouping.insert(0, METRIC_NAME.to_string());
        }
        sorted_grouping.sort();
        let inner = e.expr.as_ref().expect("aggregation without expression");
        if e.op == COUNT_VALUES {
            let param = e.param.as_ref().expect("count_values without parameter");
            let value_label = match param {
                Expr::StringLiteral(s) => s.val.clone(),
                _ => return Err(errorf("count_values parameter must be a string")),
            };
            if !ValidationScheme::Utf8.is_valid_label_name(&value_label) {
                return Err(errorf(format!("invalid label name {}", param)));
            }
            if !e.without {
                sorted_grouping.push(value_label.clone());
                sorted_grouping.sort();
            }
            let (m, ws) = self.range_eval(None, &[Some(inner)], |_ev, v, _, enh| {
                aggregate::aggregation_count_values(e, &sorted_grouping, &value_label, &v[0], enh)
            })?;
            return Ok((Value::Matrix(m), ws));
        }

        let mut warnings = Annotations::new();
        let original_num_samples = self.current_samples;
        let (fp, ws) = aggregate::FParams::new(self, e.param.as_ref())?;
        warnings.merge(ws);
        let (input, ws) = self.eval_matrix(inner)?;
        warnings.merge(ws);
        let (result, ws) = self.range_eval_agg(e, &sorted_grouping, input, fp)?;
        warnings.merge(ws);
        self.current_samples = original_num_samples + matrix_total_samples(&result) as i64;
        self.update_peak();
        Ok((Value::Matrix(result), warnings))
    }

    fn eval_binary(&mut self, e: &parser::BinaryExpr) -> EvalResult<(Value, Annotations)> {
        let lt = e.lhs.value_type();
        let rt = e.rhs.value_type();
        let pos = pr(parser::Expr::position_range(&Expr::Binary(Box::new(e.clone()))));
        let res = match (lt, rt) {
            (ValueType::Scalar, ValueType::Scalar) => {
                let op = e.op;
                self.range_eval(None, &[Some(&e.lhs), Some(&e.rhs)], |_, v, _, enh| {
                    let val = scalar_binop(op, v[0][0].f, v[1][0].f);
                    enh.out.push(Sample { f: val, ..Default::default() });
                    Ok(Annotations::new())
                })?
            }
            (ValueType::Vector, ValueType::Vector) => {
                let matching = e.vector_matching.as_deref().cloned().unwrap_or_default();
                match e.op {
                    LAND => self.range_eval(Some(&matching), &[Some(&e.lhs), Some(&e.rhs)], |_, v, sh, enh| {
                        binop::vector_and(&v[0], &v[1], &matching, &sh[0], &sh[1], enh);
                        Ok(Annotations::new())
                    })?,
                    LOR => self.range_eval(Some(&matching), &[Some(&e.lhs), Some(&e.rhs)], |_, v, sh, enh| {
                        binop::vector_or(&v[0], &v[1], &matching, &sh[0], &sh[1], enh);
                        Ok(Annotations::new())
                    })?,
                    LUNLESS => self.range_eval(Some(&matching), &[Some(&e.lhs), Some(&e.rhs)], |_, v, sh, enh| {
                        binop::vector_unless(&v[0], &v[1], &matching, &sh[0], &sh[1], enh);
                        Ok(Annotations::new())
                    })?,
                    _ => self.range_eval(Some(&matching), &[Some(&e.lhs), Some(&e.rhs)], |ev, v, sh, enh| {
                        let r = binop::vector_binop(ev, e.op, &v[0], &v[1], &matching, e.return_bool, &sh[0], &sh[1], enh, pos)?;
                        binop::handle_vector_binop_error(r, e, pos)
                    })?,
                }
            }
            (ValueType::Vector, ValueType::Scalar) => {
                self.range_eval(None, &[Some(&e.lhs), Some(&e.rhs)], |ev, v, _, enh| {
                    let r = binop::vector_scalar_binop(ev, e.op, &v[0], v[1][0].f, false, e.return_bool, enh, pos);
                    binop::handle_vector_binop_error(r, e, pos)
                })?
            }
            (ValueType::Scalar, ValueType::Vector) => {
                self.range_eval(None, &[Some(&e.lhs), Some(&e.rhs)], |ev, v, _, enh| {
                    let r = binop::vector_scalar_binop(ev, e.op, &v[1], v[0][0].f, true, e.return_bool, enh, pos);
                    binop::handle_vector_binop_error(r, e, pos)
                })?
            }
            _ => panic!("unhandled expression of type: *parser.BinaryExpr"),
        };
        Ok((Value::Matrix(res.0), res.1))
    }

    // -------------------------------------------------------------------------------------------------------------
    // rangeEval.

    /// Go `rangeEval`: evaluates the expressions, then calls `f` for each step with their values at that step.
    /// `f` appends its output to `enh.out`. `None` and string expressions give empty vectors.
    pub(crate) fn range_eval<F>(
        &mut self,
        matching: Option<&VectorMatching>,
        exprs: &[Option<&Expr>],
        mut f: F,
    ) -> EvalResult<(Matrix, Annotations)>
    where
        F: FnMut(&mut Evaluator<'a>, &[Vector], &[Vec<EvalSeriesHelper>], &mut EvalNodeHelper) -> EvalResult<Annotations>,
    {
        let num_steps = self.num_steps_total();
        let original_num_samples = self.current_samples;
        let mut warnings = Annotations::new();
        let mut matrixes: Vec<CursorMatrix> = Vec::with_capacity(exprs.len());
        for e in exprs {
            match e {
                Some(e) if e.value_type() != ValueType::String => {
                    let (m, ws) = self.eval_matrix(e)?;
                    warnings.merge(ws);
                    matrixes.push(CursorMatrix::new(m));
                }
                _ => matrixes.push(CursorMatrix::new(Vec::new())),
            }
        }
        let mut vectors: Vec<Vector> = matrixes.iter().map(|m| Vec::with_capacity(m.mat.len())).collect();
        let biggest_len = matrixes.iter().map(|m| m.mat.len()).max().unwrap_or(0).max(1);
        let mut enh = self.new_enh();
        enh.out = Vec::with_capacity(biggest_len);

        let mut series_helpers: Vec<Vec<EvalSeriesHelper>> = Vec::new();
        let mut buf_helpers: Vec<Vec<EvalSeriesHelper>> = vec![Vec::new(); exprs.len()];
        if let Some(matching) = matching {
            let mut names: Vec<String> = matching.matching_labels.clone();
            if !matching.on {
                names.push(METRIC_NAME.to_string());
                names.push(METRIC_TYPE_LABEL.to_string());
                names.push(METRIC_UNIT_LABEL.to_string());
            }
            names.sort();
            let mut buf = Vec::with_capacity(1024);
            let mut sig_to_ord: HashMap<Vec<u8>, usize> = HashMap::new();
            for m in &matrixes {
                let mut sh = Vec::with_capacity(m.mat.len());
                for s in &m.mat {
                    if matching.on {
                        s.metric.bytes_with_labels(&mut buf, &names);
                    } else {
                        s.metric.bytes_without_labels(&mut buf, &names);
                    }
                    let ord = match sig_to_ord.get(buf.as_slice()) {
                        Some(&o) => o,
                        None => {
                            let o = enh.num_sigs;
                            sig_to_ord.insert(buf.clone(), o);
                            enh.num_sigs += 1;
                            o
                        }
                    };
                    sh.push(EvalSeriesHelper { sig_ordinal: ord });
                }
                series_helpers.push(sh);
            }
        }
        let use_signatures = matching.is_some();

        let mut out_series: Vec<(Series, i64)> = Vec::with_capacity(biggest_len);
        let mut out_index: HashMap<u64, usize> = HashMap::with_capacity(biggest_len);
        let mut temp_num_samples = self.current_samples;

        let mut ts = self.start_timestamp;
        while ts <= self.end_timestamp {
            self.check_ctx("expression evaluation")?;
            self.current_samples = temp_num_samples;
            for i in 0..matrixes.len() {
                let bh = if use_signatures { Some((&mut buf_helpers[i], &series_helpers[i])) } else { None };
                self.gather_vector(ts, &mut matrixes[i], &mut vectors[i], bh)?;
            }
            enh.ts = ts;
            enh.out.clear();
            let ws = f(self, &vectors, &buf_helpers, &mut enh)?;
            warnings.merge(ws);
            let result = &mut enh.out;

            let vec_num_samples = vector_total_samples(result) as i64;
            self.current_samples += vec_num_samples;
            temp_num_samples += vec_num_samples;
            self.update_peak();
            self.check_samples()?;

            if self.end_timestamp == self.start_timestamp {
                if !self.enable_delayed_name_removal && vector_contains_same_labelset(result) {
                    return Err(errorf("vector cannot contain metrics with the same labelset"));
                }
                let mut mat = Vec::with_capacity(result.len());
                for s in result.drain(..) {
                    match s.h {
                        None => mat.push(Series {
                            metric: s.metric,
                            floats: vec![FPoint { t: ts, f: s.f }],
                            histograms: Vec::new(),
                            drop_name: s.drop_name,
                        }),
                        Some(h) => mat.push(Series {
                            metric: s.metric,
                            floats: Vec::new(),
                            histograms: vec![HPoint { t: ts, h }],
                            drop_name: s.drop_name,
                        }),
                    }
                }
                self.current_samples = original_num_samples + matrix_total_samples(&mat) as i64;
                self.update_peak();
                return Ok((mat, warnings));
            }

            for sample in result.drain(..) {
                let h = sample.metric.hash();
                match out_index.get(&h) {
                    Some(&i) => {
                        let (ss, last) = &mut out_series[i];
                        if *last == ts {
                            return Err(errorf("vector cannot contain metrics with the same labelset"));
                        }
                        *last = ts;
                        add_to_series(ss, ts, sample.f, sample.h, num_steps);
                    }
                    None => {
                        let mut ss = Series::new(sample.metric);
                        ss.drop_name = sample.drop_name;
                        add_to_series(&mut ss, ts, sample.f, sample.h, num_steps);
                        out_index.insert(h, out_series.len());
                        out_series.push((ss, ts));
                    }
                }
            }
            ts += self.interval;
        }
        let mat: Matrix = out_series.into_iter().map(|(s, _)| s).collect();
        self.current_samples = original_num_samples + matrix_total_samples(&mat) as i64;
        self.update_peak();
        Ok((mat, warnings))
    }

    /// Go `gatherVector` without series helpers.
    pub(crate) fn gather_vector_pub(&mut self, ts: i64, input: &mut CursorMatrix, output: &mut Vector) -> EvalResult<()> {
        self.gather_vector(ts, input, output, None)
    }

    /// Go `expandSeriesSet`.
    pub(crate) fn expand_series_set_pub(&mut self, it: BoxSeriesSet) -> (Vec<SeriesArc>, Annotations, Option<Error>) {
        self.expand_series_set(it)
    }

    /// Go `gatherVector`.
    fn gather_vector(
        &mut self,
        ts: i64,
        input: &mut CursorMatrix,
        output: &mut Vector,
        mut helpers: Option<(&mut Vec<EvalSeriesHelper>, &Vec<EvalSeriesHelper>)>,
    ) -> EvalResult<()> {
        output.clear();
        if let Some((bh, _)) = &mut helpers {
            bh.clear();
        }
        for i in 0..input.mat.len() {
            let s = &input.mat[i];
            let fp = input.fpos[i] as usize;
            let hp = input.hpos[i] as usize;
            if fp < s.floats.len() && s.floats[fp].t == ts {
                output.push(Sample {
                    metric: s.metric.clone(),
                    f: s.floats[fp].f,
                    t: ts,
                    h: None,
                    drop_name: s.drop_name,
                });
                input.fpos[i] += 1;
            } else if hp < s.histograms.len() && s.histograms[hp].t == ts {
                output.push(Sample {
                    metric: s.metric.clone(),
                    f: 0.0,
                    t: ts,
                    h: Some(s.histograms[hp].h.clone()),
                    drop_name: s.drop_name,
                });
                input.hpos[i] += 1;
            } else {
                continue;
            }
            if let Some((bh, sh)) = &mut helpers {
                if !sh.is_empty() {
                    bh.push(sh[i]);
                }
            }
            self.current_samples += 1;
            self.check_samples()?;
        }
        self.update_peak();
        Ok(())
    }

    // -------------------------------------------------------------------------------------------------------------
    // Selectors.

    /// Go `vectorSelectorSingle`: the sample of the series at `ts` (with lookback), if any.
    #[inline]
    fn vector_selector_single(
        &self,
        it: &mut MemoizedSeriesIterator<'_>,
        offset_ns: i64,
        ts: i64,
    ) -> EvalResult<Option<(i64, i64, f64, Option<FloatHistogram>)>> {
        let ref_time = ts - dur_ms(offset_ns);
        let vt = it.seek(ref_time);
        let mut cur: Option<(i64, i64, f64, Option<FloatHistogram>)> = None;
        match vt {
            VT::None => {
                if let Some(e) = it.err() {
                    return Err(Error::Chunk(e.clone()));
                }
            }
            VT::Float => {
                let (t, v) = it.at();
                cur = Some((it.at_st(), t, v, None));
            }
            VT::FloatHistogram | VT::Histogram => {
                let st = it.at_st();
                let (t, h) = it.at_float_histogram(None);
                cur = Some((st, t, 0.0, Some(h)));
            }
        }
        let needs_prev = match &cur {
            None => true,
            Some((_, t, _, _)) => *t > ref_time,
        };
        if needs_prev {
            match it.peek_prev() {
                Some((st, t, v, h)) if t > ref_time - self.lookback_ms() => {
                    cur = Some((st, t, v, h.cloned()));
                }
                _ => return Ok(None),
            }
        }
        let c = cur.unwrap();
        if is_stale_nan(c.2) || c.3.as_ref().is_some_and(|h| is_stale_nan(h.sum)) {
            return Ok(None);
        }
        Ok(Some(c))
    }

    /// Go `evalSeries`: the values of the series at each step.
    pub(crate) fn eval_series(
        &mut self,
        series: &[SeriesArc],
        offset_ns: i64,
        record_orig_t: bool,
        at_timestamp: Option<i64>,
    ) -> EvalResult<Matrix> {
        let num_steps = self.num_steps_total();
        let mut mat = Vec::with_capacity(series.len());
        let mut it = new_memoized_empty_iterator(self.lookback_ms());
        for s in series.iter() {
            self.check_ctx("expression evaluation")?;
            it.reset(s.iterator(None));
            let mut ss = Series::new(s.labels().clone());
            let mut ts = self.start_timestamp;
            let mut step = 0usize;
            while ts <= self.end_timestamp {
                let lookup = at_timestamp.unwrap_or(ts);
                if let Some((_, orig_t, f, h)) = self.vector_selector_single(&mut it, offset_ns, lookup)? {
                    match h {
                        None => {
                            self.current_samples += 1;
                            self.samples_stats.increment_samples_at_step(step, 1);
                            self.samples_stats.increment_samples_read_at_step(step, 1);
                            self.consume_samples(1)?;
                            self.check_samples()?;
                            if ss.floats.capacity() == 0 {
                                ss.floats.reserve_exact(num_steps.min(MAX_POINTS_SLICE_SIZE));
                            }
                            let f = if record_orig_t { orig_t as f64 } else { f };
                            ss.floats.push(FPoint { t: ts, f });
                        }
                        Some(h) => {
                            if record_orig_t {
                                return Err(errorf(format!(
                                    "this should be an info metric, with float samples: {}",
                                    ss.metric
                                )));
                            }
                            let size = histogram_point_size(&h);
                            self.current_samples += size as i64;
                            self.samples_stats.increment_samples_at_step(step, size as i64);
                            self.samples_stats.increment_samples_read_at_step(step, size as i64);
                            self.consume_samples(size as i64)?;
                            self.check_samples()?;
                            if ss.histograms.capacity() == 0 {
                                ss.histograms.reserve_exact(num_steps.min(MAX_POINTS_SLICE_SIZE));
                            }
                            ss.histograms.push(HPoint { t: ts, h: Arc::new(h) });
                        }
                    }
                }
                ts += self.interval;
                step += 1;
            }
            if ss.floats.len() + ss.histograms.len() > 0 {
                mat.push(ss);
            }
        }
        self.update_peak();
        Ok(mat)
    }

    /// Go `smoothSeries`.
    fn smooth_series(
        &mut self,
        series: &[SeriesArc],
        offset_ns: i64,
        pos: storage::annotations::PositionRange,
    ) -> EvalResult<(Matrix, Annotations)> {
        let dur = self.end_timestamp - self.start_timestamp;
        let mut it = new_buffer(dur + 2 * self.lookback_ms());
        let off_ms = dur_ms(offset_ns);
        let lb = self.lookback_ms();
        let mut mat = Vec::with_capacity(series.len());
        let mut annos = Annotations::new();
        let mut floats: Vec<FPoint> = Vec::new();
        let mut hists: Vec<HPoint> = Vec::new();
        for s in series.iter() {
            let mut ss = Series::new(s.labels().clone());
            it.reset(s.iterator(None));
            floats.clear();
            hists.clear();
            let metric_name = s.labels().get(METRIC_NAME).to_string();
            let mut eval_ts = self.start_timestamp;
            while eval_ts <= self.end_timestamp {
                let data_ts = eval_ts - off_ms;
                let step = ((eval_ts - self.start_timestamp) / self.interval) as isize;
                self.matrix_iter_slice(&mut it, data_ts - lb, data_ts + lb, &mut floats, &mut hists, None, step)?;
                let next_ts = eval_ts + self.interval;
                if floats.is_empty() && hists.is_empty() {
                    eval_ts = next_ts;
                    continue;
                }
                if !hists.is_empty() && !floats.is_empty() {
                    annos.add(annos::new_mixed_floats_histograms_warning(&metric_name, pos));
                    eval_ts = next_ts;
                    continue;
                }
                if !hists.is_empty() {
                    let i = hists.partition_point(|p| p.t < data_ts);
                    if i < hists.len() && hists[i].t == data_ts {
                        ss.histograms.push(HPoint { h: Arc::new((*hists[i].h).clone()), t: eval_ts });
                    } else if i > 0 && i < hists.len() {
                        let (prev, next) = (&hists[i - 1], &hists[i]);
                        if prev.h.uses_custom_buckets() != next.h.uses_custom_buckets() {
                            annos.add(annos::new_mixed_exponential_custom_histograms_warning(&metric_name, pos));
                            eval_ts = next_ts;
                            continue;
                        }
                        let is_counter = prev.h.counter_reset_hint != CounterResetHint::GaugeType
                            || next.h.counter_reset_hint != CounterResetHint::GaugeType;
                        match crate::functions::interpolate_histograms(
                            &prev.h, prev.t, &next.h, next.t, data_ts, is_counter, &mut annos, pos,
                        ) {
                            Ok(h) => ss.histograms.push(HPoint { h: Arc::new(h), t: eval_ts }),
                            Err(e) => {
                                crate::functions::annos_from_interpolation_error(&mut annos, e, &metric_name, pos)?;
                                eval_ts = next_ts;
                                continue;
                            }
                        }
                    } else if i > 0 {
                        let mut h = (*hists[i - 1].h).clone();
                        h.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                        ss.histograms.push(HPoint { h: Arc::new(h), t: eval_ts });
                    }
                } else {
                    let i = floats.partition_point(|p| p.t < data_ts);
                    if i < floats.len() && floats[i].t == data_ts {
                        ss.floats.push(FPoint { f: floats[i].f, t: eval_ts });
                    } else if i > 0 && i < floats.len() {
                        let val = crate::functions::interpolate(floats[i - 1], floats[i], data_ts, false);
                        ss.floats.push(FPoint { f: val, t: eval_ts });
                    } else if i > 0 {
                        ss.floats.push(FPoint { f: floats[i - 1].f, t: eval_ts });
                    }
                }
                eval_ts = next_ts;
            }
            if ss.floats.len() + ss.histograms.len() > 0 {
                mat.push(ss);
            }
        }
        Ok((mat, annos))
    }

    /// Go `matrixSelector`: evaluates a matrix selector at the start time (instant queries only).
    fn matrix_selector(&mut self, node: &MatrixSelector) -> EvalResult<(Matrix, Annotations)> {
        let vs = node.vector_selector.as_vector_selector().expect("matrix selector without vector selector");
        let offset = dur_ms(self.state.vs_offset(vs));
        let mut maxt = self.start_timestamp - offset;
        let mut mint = maxt - dur_ms(node.range);
        let matrix_mint = mint;
        let matrix_maxt = maxt;
        let mut buffer_range = dur_ms(node.range);
        let mut retrieve_sts = false;
        if vs.anchored {
            buffer_range += self.lookback_ms();
            mint -= self.lookback_ms();
            retrieve_sts = true;
        } else if vs.smoothed {
            buffer_range += 2 * self.lookback_ms();
            mint -= self.lookback_ms();
            maxt += self.lookback_ms();
            retrieve_sts = true;
        }
        let (series, ws) = self.check_and_expand_series_set(vs)?;
        let mut it = new_buffer(buffer_range);
        let mut matrix = Vec::with_capacity(series.len());
        for s in series.iter() {
            self.check_ctx("expression evaluation")?;
            it.reset(s.iterator(None));
            let mut ss = Series::new(s.labels().clone());
            let mut sts = if self.use_start_timestamps && retrieve_sts { Some(StartTimestamps::default()) } else { None };
            self.matrix_iter_slice(&mut it, mint, maxt, &mut ss.floats, &mut ss.histograms, sts.as_mut(), 0)?;
            if vs.anchored || vs.smoothed {
                if !ss.histograms.is_empty() {
                    return Err(errorf(if vs.anchored {
                        "anchored modifier is not supported with histograms"
                    } else {
                        "smoothed modifier is not supported with histograms"
                    }));
                }
                let st = sts.as_ref().map(|s| s.floats.as_slice()).unwrap_or(&[]);
                ss.floats = crate::functions::extend_floats(&ss.floats, st, matrix_mint, matrix_maxt, mint, vs.smoothed);
            }
            let total = ss.floats.len() as i64 + total_hpoint_size(&ss.histograms) as i64;
            self.samples_stats.increment_samples_at_timestamp(self.start_timestamp, total);
            if self.budget.is_none() {
                self.samples_stats.increment_samples_read_at_timestamp(self.start_timestamp, total);
            }
            if total > 0 {
                matrix.push(ss);
            }
        }
        Ok((matrix, ws))
    }

    /// Go `matrixIterSlice`: fills `floats` and `histograms` with the points in `(mint, maxt]`, keeping the
    /// points of the previous step that are still in range.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn matrix_iter_slice(
        &mut self,
        it: &mut BufferedSeriesIterator<'_>,
        mint: i64,
        maxt: i64,
        floats: &mut Vec<FPoint>,
        histograms: &mut Vec<HPoint>,
        mut sts: Option<&mut StartTimestamps>,
        read_step: isize,
    ) -> EvalResult<()> {
        let mut mint_floats = mint;
        let mut mint_histograms = mint;

        if !floats.is_empty() && floats[floats.len() - 1].t > mint {
            let drop = floats.iter().position(|p| p.t > mint).unwrap_or(floats.len());
            self.current_samples -= drop as i64;
            floats.drain(..drop);
            mint_floats = floats[floats.len() - 1].t;
            if let Some(s) = sts.as_deref_mut() {
                let d = drop.min(s.floats.len());
                s.floats.drain(..d);
            }
        } else {
            self.current_samples -= floats.len() as i64;
            floats.clear();
            if let Some(s) = sts.as_deref_mut() {
                s.floats.clear();
            }
        }

        if !histograms.is_empty() && histograms[histograms.len() - 1].t > mint {
            let drop = histograms.iter().position(|p| p.t > mint).unwrap_or(histograms.len());
            let dropped_size = total_hpoint_size(&histograms[..drop]);
            histograms.drain(..drop);
            self.current_samples -= dropped_size as i64;
            mint_histograms = histograms[histograms.len() - 1].t;
            if let Some(s) = sts.as_deref_mut() {
                let d = drop.min(s.histograms.len());
                s.histograms.drain(..d);
            }
        } else {
            self.current_samples -= total_hpoint_size(histograms) as i64;
            histograms.clear();
            if let Some(s) = sts.as_deref_mut() {
                s.histograms.clear();
            }
        }

        if mint == maxt {
            return Ok(());
        }

        let sought = it.seek(maxt);
        if sought == VT::None {
            if let Some(e) = it.err() {
                return Err(Error::Chunk(e.clone()));
            }
        }

        {
            let mut buf = it.buffer();
            loop {
                match buf.next() {
                    VT::None => break,
                    VT::FloatHistogram | VT::Histogram => {
                        let t = buf.at_t();
                        if t > mint_histograms {
                            let stale = match buf.at_float_histogram_ref() {
                                Some(fh) => is_stale_nan(fh.sum),
                                None => buf.at_histogram().1.is_some_and(|h| is_stale_nan(h.sum)),
                            };
                            if stale {
                                continue;
                            }
                            let (_, fh) = buf.at_float_histogram(None);
                            let p = HPoint { t, h: Arc::new(fh) };
                            let size = p.size();
                            self.consume_matrix_samples(size as i64, read_step)?;
                            self.current_samples += size as i64;
                            self.check_samples()?;
                            histograms.push(p);
                            if let Some(s) = sts.as_deref_mut() {
                                s.histograms.push(buf.at_st());
                            }
                        }
                    }
                    VT::Float => {
                        let (t, f) = buf.at();
                        if is_stale_nan(f) {
                            continue;
                        }
                        if t > mint_floats {
                            self.consume_matrix_samples(1, read_step)?;
                            self.current_samples += 1;
                            self.check_samples()?;
                            if floats.capacity() == 0 {
                                floats.reserve(16);
                            }
                            floats.push(FPoint { t, f });
                            if let Some(s) = sts.as_deref_mut() {
                                s.floats.push(buf.at_st());
                            }
                        }
                    }
                }
            }
        }

        match sought {
            VT::FloatHistogram | VT::Histogram => {
                if it.at_t() == maxt {
                    let (t, fh) = it.at_float_histogram(None);
                    if !is_stale_nan(fh.sum) {
                        let p = HPoint { t, h: Arc::new(fh) };
                        let size = p.size();
                        self.consume_matrix_samples(size as i64, read_step)?;
                        self.current_samples += size as i64;
                        self.check_samples()?;
                        histograms.push(p);
                        if let Some(s) = sts.as_deref_mut() {
                            s.histograms.push(it.at_st());
                        }
                    }
                }
            }
            VT::Float => {
                let (t, f) = it.at();
                if t == maxt && !is_stale_nan(f) {
                    self.consume_matrix_samples(1, read_step)?;
                    self.current_samples += 1;
                    self.check_samples()?;
                    if floats.capacity() == 0 {
                        floats.reserve(16);
                    }
                    floats.push(FPoint { t, f });
                    if let Some(s) = sts.as_deref_mut() {
                        s.floats.push(it.at_st());
                    }
                }
            }
            VT::None => {}
        }
        self.update_peak();
        Ok(())
    }

    // -------------------------------------------------------------------------------------------------------------
    // Subqueries.

    /// Go `subqueryTimeRange`.
    pub(crate) fn subquery_time_range(&self, e: &SubqueryExpr) -> (i64, i64, i64) {
        let offset_ms = dur_ms(self.state.subquery_offset(e));
        let range_ms = dur_ms(e.range);
        let interval = if e.step != 0 { dur_ms(e.step) } else { (self.no_step_subquery_interval_fn)(range_ms) };
        let (start, end) = subquery_evaluation_times(
            self.start_timestamp,
            self.end_timestamp,
            self.interval,
            offset_ms,
            range_ms,
            interval,
            e.timestamp.is_some(),
        );
        (start, end, interval)
    }

    /// Go `runSubquery`.
    fn run_subquery(&mut self, e: &SubqueryExpr) -> EvalResult<(Value, QuerySamples, Annotations)> {
        let (start, end, interval) = self.subquery_time_range(e);
        let child_stats = QuerySamples::new_child_with_step_tracking(start, end, interval);
        let mut new_ev = self.child(start, end, interval, child_stats);
        if start != self.start_timestamp {
            set_offset_for_at_modifier(start, &e.expr, self.state);
        }
        let res = new_ev.eval(&e.expr);
        self.current_samples = new_ev.current_samples;
        let (v, ws) = res?;
        Ok((v, new_ev.samples_stats, ws))
    }

    /// Go `evalSubquery`: evaluates a subquery as the range vector argument of a function.
    fn eval_subquery(
        &mut self,
        subq: &SubqueryExpr,
        outer_offset: i64,
        outer_range: i64,
    ) -> EvalResult<(MatrixArg, i64, Annotations)> {
        let (val, child_stats, ws) = self.run_subquery(subq)?;
        self.samples_stats.update_peak_from_subquery(&child_stats);
        self.samples_stats.merge_series_from_subquery(&child_stats);
        let mut consumer = SubqueryConsumer {
            start: self.start_timestamp,
            interval: self.interval,
            num_steps: self.num_steps(),
            offset: outer_offset,
            range: outer_range,
            at_timestamp: subq.timestamp,
        };
        self.samples_stats.merge_total_samples_from_subquery(&child_stats, &consumer);
        if self.budget.is_some() {
            consumer.range = 0;
            if consumer.at_timestamp.is_some() {
                consumer.num_steps = 1;
            }
        }
        self.samples_stats.merge_samples_read_from_subquery(&child_stats, &consumer);
        let mat = match val {
            Value::Matrix(m) => m,
            _ => return Err(errorf("subquery did not return a matrix")),
        };
        let total = matrix_total_samples(&mat) as i64;
        let mut offset = self.state.subquery_offset(subq);
        if let Some(ts) = subq.timestamp {
            offset = subq.original_offset + (self.start_timestamp - ts) * 1_000_000;
        }
        let mut series: Vec<SeriesArc> = Vec::with_capacity(mat.len());
        let mut drop_names = Vec::with_capacity(mat.len());
        for mut s in mat {
            for hp in s.histograms.iter_mut() {
                if matches!(hp.h.counter_reset_hint, CounterResetHint::NotCounterReset | CounterResetHint::CounterReset) {
                    let mut h = (*hp.h).clone();
                    h.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                    hp.h = Arc::new(h);
                }
            }
            drop_names.push(s.drop_name);
            series.push(Arc::new(StorageSeries::new(s)));
        }
        Ok((
            MatrixArg {
                series: Rc::new(series),
                drop_names,
                offset,
                range: subq.range,
                timestamp: subq.timestamp,
                anchored: false,
                smoothed: false,
            },
            total,
            ws,
        ))
    }

    // -------------------------------------------------------------------------------------------------------------
    // Function calls.

    fn eval_call(&mut self, e: &Call) -> EvalResult<(Value, Annotations)> {
        let name: &str = &e.func.name;
        let call = functions::function_call(e.func.id);
        if name == "timestamp" || name == "start_timestamp" {
            if let Some(Expr::VectorSelector(vs)) = e.args.first() {
                let call = call.expect("timestamp function");
                let (m, ws) = self.range_eval_timestamp_function_over_vector_selector(vs, call, e)?;
                return Ok((Value::Matrix(m), ws));
            }
        }

        let mut warnings = Annotations::new();
        let mut matrix_arg_index = None;
        let mut subquery_arg: Option<(MatrixArg, i64)> = None;
        for (i, a) in e.args.iter().enumerate() {
            match a {
                Expr::MatrixSelector(_) => {
                    matrix_arg_index = Some(i);
                    break;
                }
                Expr::Subquery(subq) => {
                    matrix_arg_index = Some(i);
                    let (arg, total, ws) =
                        self.eval_subquery(subq, dur_ms(subq.original_offset), dur_ms(subq.range))?;
                    warnings.merge(ws);
                    subquery_arg = Some((arg, total));
                    break;
                }
                _ => {}
            }
        }

        match name {
            "label_replace" => return functions::eval_label_replace(self, &e.args),
            "label_join" => return functions::eval_label_join(self, &e.args),
            "info" => return crate::info::eval_info(self, &e.args),
            _ => {}
        }

        let call = match call {
            Some(c) => c,
            None => panic!("unexpected nil implementation for function {:?}", name),
        };

        if matches!(name, "sort" | "sort_desc" | "sort_by_label" | "sort_by_label_desc")
            && self.start_timestamp != self.end_timestamp
        {
            warnings.add(annos::new_sort_in_range_query_warning(pr(e.pos_range)));
        }

        let Some(matrix_arg_index) = matrix_arg_index else {
            let args: Vec<Option<&Expr>> = e.args.iter().map(Some).collect();
            let (m, ws) = self.range_eval(None, &args, |_, v, _, enh| call(v, &[], &e.args, enh))?;
            warnings.merge(ws);
            return Ok((Value::Matrix(m), warnings));
        };

        let from_subquery = subquery_arg.is_some();
        let res = self.eval_matrix_call(e, call, matrix_arg_index, subquery_arg.as_ref().map(|(a, _)| a), &mut warnings);
        if let Some((_, total)) = subquery_arg {
            // The subquery result takes space in memory; release it at the end.
            self.current_samples -= total;
        }
        let _ = from_subquery;
        res
    }

    fn eval_matrix_call(
        &mut self,
        e: &Call,
        call: FunctionCall,
        matrix_arg_index: usize,
        subquery_arg: Option<&MatrixArg>,
        warnings: &mut Annotations,
    ) -> EvalResult<(Value, Annotations)> {
        let name: &str = &e.func.name;
        let num_steps = self.num_steps_total();
        // Evaluate the non-matrix arguments.
        let mut eval_vals: Vec<Matrix> = Vec::with_capacity(e.args.len());
        for (i, a) in e.args.iter().enumerate() {
            if i != matrix_arg_index {
                let (m, ws) = self.eval_matrix(a)?;
                eval_vals.push(m);
                warnings.merge(ws);
            } else {
                eval_vals.push(Vec::new());
            }
        }

        let selector_arg;
        let arg: &MatrixArg = match subquery_arg {
            Some(a) => a,
            None => {
                let Expr::MatrixSelector(sel) = &e.args[matrix_arg_index] else { unreachable!() };
                let vs = sel.vector_selector.as_vector_selector().expect("vector selector");
                if vs.anchored && !functions::ANCHORED_SAFE_FUNCTIONS.contains(&name) {
                    return Err(errorf(format!(
                        "anchored modifier can only be used with: {} - not with {}",
                        functions::ANCHORED_SAFE_FUNCTIONS.join(", "),
                        name
                    )));
                }
                if vs.smoothed && !vs.anchored && !functions::SMOOTHED_SAFE_FUNCTIONS.contains(&name) {
                    return Err(errorf(format!(
                        "smoothed modifier can only be used with: {} - not with {}",
                        functions::SMOOTHED_SAFE_FUNCTIONS.join(", "),
                        name
                    )));
                }
                let r = self.check_and_expand_series_set(vs);
                let (series, ws) = match r {
                    Ok(x) => x,
                    Err(err) => {
                        return Err(match err.downcast_ref::<ErrWithWarnings>() {
                            Some(w) => {
                                let mut all = warnings.clone();
                                all.merge(w.warnings.clone());
                                Error::other(ErrWithWarnings { err: w.err.clone(), warnings: all })
                            }
                            None => err,
                        });
                    }
                };
                warnings.merge(ws);
                selector_arg = MatrixArg {
                    series,
                    drop_names: Vec::new(),
                    offset: self.state.vs_offset(vs),
                    range: sel.range,
                    timestamp: vs.timestamp,
                    anchored: vs.anchored,
                    smoothed: vs.smoothed,
                };
                &selector_arg
            }
        };
        let from_subquery = subquery_arg.is_some();

        let offset = dur_ms(arg.offset);
        let sel_range = dur_ms(arg.range);
        let lb = self.lookback_ms();
        let step_range = if arg.anchored {
            (sel_range + lb).min(self.interval)
        } else if arg.smoothed {
            (sel_range + 2 * lb).min(self.interval)
        } else {
            sel_range.min(self.interval)
        };
        let mut buffer_range = sel_range;
        if arg.anchored {
            buffer_range += lb;
        } else if arg.smoothed {
            buffer_range += 2 * lb;
        }

        let mut mat: Matrix = Vec::with_capacity(arg.series.len());
        let mut in_matrix = vec![Series::default()];
        let mut enh = self.new_enh();
        enh.out = Vec::with_capacity(1);
        enh.range = arg.range;
        enh.offset = arg.offset;
        enh.anchored = arg.anchored;
        enh.smoothed = arg.smoothed;
        let mut it = new_buffer(buffer_range);
        let use_sts = self.use_start_timestamps && matches!(name, "rate" | "irate" | "increase" | "resets");
        if use_sts {
            enh.start_timestamps = Some(StartTimestamps::default());
        }
        let drop_name = name != "last_over_time" && name != "first_over_time";
        let mut vector_vals: Vec<Vector> = vec![vec![Sample::default()]; e.args.len().saturating_sub(1)];
        let is_rate_or_increase = name == "rate" || name == "increase";

        for (i, s) in arg.series.iter().enumerate() {
            self.check_ctx("expression evaluation")?;
            {
                let m = &mut in_matrix[0];
                self.current_samples -= (m.floats.len() + total_hpoint_size(&m.histograms)) as i64;
                m.floats.clear();
                m.histograms.clear();
            }
            if let Some(st) = enh.start_timestamps.as_mut() {
                st.reset();
            }
            it.reset(s.iterator(None));
            let input_drop_name = arg.drop_names.get(i).copied().unwrap_or(false);
            let series_drop_name = drop_name || input_drop_name;
            let mut metric = s.labels().clone();
            if !self.enable_delayed_name_removal && series_drop_name {
                metric = drop_metadata(&metric);
            }
            let mut ss = Series { metric, drop_name: series_drop_name, ..Default::default() };
            in_matrix[0].metric = s.labels().clone();

            let mut ts = self.start_timestamp;
            let mut step: usize = 0;
            while ts <= self.end_timestamp {
                let mut counter = 0;
                for (j, ev) in eval_vals.iter().enumerate() {
                    if j != matrix_arg_index {
                        vector_vals[counter][0].f = ev[0].floats[step].f;
                        counter += 1;
                    }
                }
                let mut maxt = 0;
                let refetch = ts == self.start_timestamp || arg.timestamp.is_none();
                if refetch {
                    maxt = ts - offset;
                    let mut mint = maxt - sel_range;
                    if arg.anchored {
                        mint -= lb;
                    } else if arg.smoothed {
                        mint -= lb;
                        maxt += lb;
                    }
                    let read_step = if from_subquery { -1 } else { step as isize };
                    let m = &mut in_matrix[0];
                    let (floats, histograms) = (&mut m.floats, &mut m.histograms);
                    self.matrix_iter_slice(&mut it, mint, maxt, floats, histograms, enh.start_timestamps.as_mut(), read_step)?;
                }
                let m = &in_matrix[0];
                if m.floats.len() + m.histograms.len() == 0 {
                    ts += self.interval;
                    step += 1;
                    continue;
                }
                let full_window_count = (m.floats.len() + total_hpoint_size(&m.histograms)) as i64;
                let mut samples_read_count = 0;
                if self.budget.is_none() && refetch && !from_subquery {
                    samples_read_count = if step == 0 {
                        full_window_count
                    } else {
                        count_samples_after(&m.floats, &m.histograms, maxt - self.interval)
                    };
                }
                enh.ts = ts;
                enh.out.clear();
                let annos = call(&vector_vals, &in_matrix, &e.args, &mut enh)?;
                warnings.merge(annos);
                if !from_subquery {
                    self.samples_stats.increment_samples_at_step(step, full_window_count);
                }
                if samples_read_count > 0 {
                    self.samples_stats.increment_samples_read_at_step(step, samples_read_count);
                }
                if let Some(out) = enh.out.first_mut() {
                    match out.h.take() {
                        None => {
                            if ss.floats.capacity() == 0 {
                                ss.floats.reserve_exact(num_steps.min(MAX_POINTS_SLICE_SIZE));
                            }
                            ss.floats.push(FPoint { f: out.f, t: ts });
                        }
                        Some(h) => {
                            if ss.histograms.capacity() == 0 {
                                ss.histograms.reserve_exact(num_steps.min(MAX_POINTS_SLICE_SIZE));
                            }
                            ss.histograms.push(HPoint { h, t: ts });
                        }
                    }
                }
                it.reduce_delta(step_range);
                ts += self.interval;
                step += 1;
            }
            let hist_samples = total_hpoint_size(&ss.histograms);
            if ss.floats.len() + hist_samples > 0 {
                if self.current_samples + (ss.floats.len() + hist_samples) as i64 > self.max_samples {
                    return Err(err_too_many_samples());
                }
                self.current_samples += (ss.floats.len() + hist_samples) as i64;
                let has_floats = !ss.floats.is_empty();
                mat.push(ss);
                self.update_peak();
                if is_rate_or_increase {
                    self.possible_non_counter_info(&in_matrix[0].metric, has_floats, &e.args[0], warnings);
                }
            } else {
                self.update_peak();
            }
        }
        self.update_peak();
        {
            let m = &in_matrix[0];
            self.current_samples -= (m.floats.len() + total_hpoint_size(&m.histograms)) as i64;
        }

        if name == "absent_over_time" {
            let steps = num_steps;
            for s in &mat {
                if s.floats.len() + s.histograms.len() == steps {
                    return Ok((Value::Matrix(Vec::new()), std::mem::take(warnings)));
                }
            }
            let mut found: HashSet<i64> = HashSet::new();
            for (i, s) in mat.iter().enumerate() {
                for p in &s.floats {
                    found.insert(p.t);
                }
                for p in &s.histograms {
                    found.insert(p.t);
                }
                if i > 0 && found.len() == steps {
                    return Ok((Value::Matrix(Vec::new()), std::mem::take(warnings)));
                }
            }
            let mut newp = Vec::with_capacity(steps - found.len());
            let mut ts = self.start_timestamp;
            while ts <= self.end_timestamp {
                if !found.contains(&ts) {
                    newp.push(FPoint { t: ts, f: 1.0 });
                }
                ts += self.interval;
            }
            return Ok((
                Value::Matrix(vec![Series {
                    metric: crate::functions::create_labels_for_absent_function(&e.args[0]),
                    floats: newp,
                    histograms: Vec::new(),
                    drop_name,
                }]),
                std::mem::take(warnings),
            ));
        }

        if !self.enable_delayed_name_removal {
            mat = self.merge_series_with_same_labelset(mat)?;
        }
        Ok((Value::Matrix(mat), std::mem::take(warnings)))
    }

    fn possible_non_counter_info(&self, metric: &Labels, has_floats: bool, arg: &Expr, warnings: &mut Annotations) {
        let metric_name = metric.get(METRIC_NAME);
        if metric_name.is_empty() || !has_floats {
            return;
        }
        if self.enable_type_and_unit_labels {
            let type_label = metric.get(METRIC_TYPE_LABEL);
            if type_label != "counter" && type_label != "histogram" {
                warnings.add(annos::new_possible_non_counter_label_info(metric_name, type_label, pr(arg.position_range())));
            }
        } else if !metric_name.ends_with("_total")
            && !metric_name.ends_with("_sum")
            && !metric_name.ends_with("_count")
            && !metric_name.ends_with("_bucket")
        {
            warnings.add(annos::new_possible_non_counter_info(metric_name, pr(arg.position_range())));
        }
    }

    /// Go `rangeEvalTimestampFunctionOverVectorSelector`.
    fn range_eval_timestamp_function_over_vector_selector(
        &mut self,
        vs: &VectorSelector,
        call: FunctionCall,
        e: &Call,
    ) -> EvalResult<(Matrix, Annotations)> {
        let (series, ws) = self.check_and_expand_series_set(vs)?;
        let lb = self.lookback_ms();
        let mut iters: Vec<MemoizedSeriesIterator<'_>> =
            series.iter().map(|s| new_memoized_iterator(s.iterator(None), lb - 1)).collect();
        let propagate_sts = e.func.name == "start_timestamp" && self.use_start_timestamps;
        let base_offset = self.state.vs_offset(vs);
        let ws_cell = RefCell::new(ws);
        let (m, w) = self.range_eval(None, &[], |ev, _, _, enh| {
            let offset = match vs.timestamp {
                Some(t) => (enh.ts - t) * 1_000_000,
                None => base_offset,
            };
            let mut vec: Vector = Vec::with_capacity(series.len());
            let mut sts: Vec<i64> = Vec::new();
            if propagate_sts {
                sts = enh.start_timestamps.take().map(|s| s.floats).unwrap_or_default();
                sts.clear();
                sts.reserve(series.len());
            }
            for (i, s) in series.iter().enumerate() {
                if ev.budget.is_some() {
                    ev.check_ctx(ENV)?;
                }
                if let Some((st, t, _, _)) = ev.vector_selector_single(&mut iters[i], offset, enh.ts)? {
                    vec.push(Sample { metric: s.labels().clone(), t, ..Default::default() });
                    if propagate_sts {
                        sts.push(st);
                    }
                    ev.current_samples += 1;
                    ev.samples_stats.increment_samples_at_timestamp(enh.ts, 1);
                    ev.samples_stats.increment_samples_read_at_timestamp(enh.ts, 1);
                    ev.consume_samples(1)?;
                    ev.check_samples()?;
                }
            }
            if propagate_sts {
                enh.start_timestamps = Some(StartTimestamps { floats: sts, histograms: Vec::new() });
            } else if let Some(s) = enh.start_timestamps.as_mut() {
                s.floats.clear();
            }
            ev.update_peak();
            let annos = call(&[vec], &[], &e.args, enh)?;
            let mut w = ws_cell.borrow().clone();
            w.merge(annos);
            Ok(w)
        })?;
        Ok((m, w))
    }

    // -------------------------------------------------------------------------------------------------------------
    // Label cleanup.

    /// Go `cleanupMetricLabels`.
    fn cleanup_metric_labels(&mut self, v: Value) -> EvalResult<Value> {
        match v {
            Value::Matrix(mut mat) => {
                for s in mat.iter_mut() {
                    if s.drop_name {
                        s.metric = drop_metadata(&s.metric);
                    }
                }
                Ok(Value::Matrix(self.merge_series_with_same_labelset(mat)?))
            }
            Value::Vector(mut vec) => {
                for s in vec.iter_mut() {
                    if s.drop_name {
                        s.metric = drop_metadata(&s.metric);
                    }
                }
                if vector_contains_same_labelset(&vec) {
                    return Err(errorf("vector cannot contain metrics with the same labelset"));
                }
                Ok(Value::Vector(vec))
            }
            other => Ok(other),
        }
    }

    /// Go `mergeSeriesWithSameLabelset`.
    pub(crate) fn merge_series_with_same_labelset(&mut self, mat: Matrix) -> EvalResult<Matrix> {
        if mat.len() <= 1 || !matrix_contains_same_labelset(&mat) {
            return Ok(mat);
        }
        let mut order: Vec<u64> = Vec::new();
        let mut by_hash: HashMap<u64, Vec<usize>> = HashMap::new();
        for (i, s) in mat.iter().enumerate() {
            let h = s.metric.hash();
            by_hash
                .entry(h)
                .or_insert_with(|| {
                    order.push(h);
                    Vec::new()
                })
                .push(i);
        }
        let mut slots: Vec<Option<Series>> = mat.into_iter().map(Some).collect();
        let mut merged = Vec::with_capacity(order.len());
        for h in order {
            let idx = &by_hash[&h];
            let mut base = slots[idx[0]].take().unwrap();
            if idx.len() > 1 {
                for &j in &idx[1..] {
                    let s = slots[j].take().unwrap();
                    base.floats.extend(s.floats);
                    base.histograms.extend(s.histograms);
                }
                base.floats.sort_by_key(|p| p.t);
                base.histograms.sort_by_key(|p| p.t);
                for w in base.floats.windows(2) {
                    if w[0].t == w[1].t {
                        return Err(errorf("vector cannot contain metrics with the same labelset"));
                    }
                }
                for w in base.histograms.windows(2) {
                    if w[0].t == w[1].t {
                        return Err(errorf("vector cannot contain metrics with the same labelset"));
                    }
                }
                let (mut fi, mut hi) = (0, 0);
                while fi < base.floats.len() && hi < base.histograms.len() {
                    let (ft, ht) = (base.floats[fi].t, base.histograms[hi].t);
                    if ft == ht {
                        return Err(errorf("vector cannot contain metrics with the same labelset"));
                    } else if ft < ht {
                        fi += 1;
                    } else {
                        hi += 1;
                    }
                }
            }
            merged.push(base);
        }
        Ok(merged)
    }
}

// -----------------------------------------------------------------------------------------------------------------
// exec of an evaluation statement.

impl Query {
    /// Go `execEvalStmt`.
    pub(super) fn exec_eval_stmt(&mut self, ctx: &Context) -> Result<(Value, Annotations), (Error, Annotations)> {
        let engine = self.engine.clone();
        let ng = &*engine.inner;
        let budget = if ng.enable_query_cost {
            Some(QueryBudget {
                max_series: uint64_to_int64_limit(self.max_series),
                max_samples_read: uint64_to_int64_limit(self.max_samples_read),
                ..Default::default()
            })
        } else {
            None
        };
        let res = self.exec_eval_stmt_inner(ctx, budget.as_ref());
        if let Some(b) = &budget {
            self.sample_stats.total_series = b.series.get();
            self.sample_stats.samples_read = b.samples_read.get();
        }
        res
    }

    fn exec_eval_stmt_inner(
        &mut self,
        ctx: &Context,
        budget: Option<&QueryBudget>,
    ) -> Result<(Value, Annotations), (Error, Annotations)> {
        let engine = self.engine.clone();
        let ng = &*engine.inner;
        self.stats.start(QueryTiming::QueryPreparationTime);
        let (mint, maxt) = find_min_max_time(&self.stmt);
        let mut querier = match self.queryable.querier(mint, maxt) {
            Ok(q) => q,
            Err(e) => {
                self.stats.finish(QueryTiming::QueryPreparationTime);
                return Err((e, Annotations::new()));
            }
        };
        let state = QueryState::default();
        preprocess::populate_series(&*querier, &self.stmt, ctx, &state, &*ng.no_step_subquery_interval_fn);
        self.stats.finish(QueryTiming::QueryPreparationTime);

        set_offset_for_at_modifier(self.stmt.start, &self.stmt.expr, &state);
        self.stats.start(QueryTiming::InnerEvalTime);
        let s = &self.stmt;
        let instant = s.start == s.end && s.interval == 0;
        let (start, end, interval) = if instant { (s.start, s.start, 1) } else { (s.start, s.end, s.interval) };
        self.sample_stats.init_step_tracking(start, end, interval);
        let sample_stats = std::mem::take(&mut self.sample_stats);
        let mut ev = Evaluator {
            start_timestamp: start,
            end_timestamp: end,
            interval,
            max_samples: ng.max_samples_per_query as i64,
            current_samples: 0,
            budget,
            lookback_delta: s.lookback_delta.as_nanos() as i64,
            samples_stats: sample_stats,
            no_step_subquery_interval_fn: &*ng.no_step_subquery_interval_fn,
            enable_delayed_name_removal: ng.enable_delayed_name_removal,
            enable_type_and_unit_labels: ng.enable_type_and_unit_labels,
            use_start_timestamps: self.use_start_timestamps,
            querier: &*querier,
            ctx,
            state: &state,
        };
        let (val, warnings, err) = ev.eval_top(&s.expr);
        self.sample_stats = std::mem::take(&mut ev.samples_stats);
        drop(ev);
        drop(state);
        self.stats.finish(QueryTiming::InnerEvalTime);
        let _ = querier.close();
        if let Some(e) = err {
            return Err((e, warnings));
        }
        let val = val.unwrap();
        let expr_type = s.expr.value_type();
        if instant {
            let mat = match val {
                Value::Matrix(m) => m,
                Value::String(s) => return Ok((Value::String(s), warnings)),
                other => panic!("promql.Engine.exec: invalid expression type {}", other.value_type()),
            };
            return match expr_type {
                ValueType::Vector => {
                    let mut vector = Vec::with_capacity(mat.len());
                    for s in mat {
                        if let Some(h) = s.histograms.into_iter().next() {
                            vector.push(Sample { metric: s.metric, h: Some(h.h), t: start, f: 0.0, drop_name: s.drop_name });
                        } else {
                            vector.push(Sample { metric: s.metric, f: s.floats[0].f, t: start, h: None, drop_name: s.drop_name });
                        }
                    }
                    Ok((Value::Vector(vector), warnings))
                }
                ValueType::Scalar => Ok((Value::Scalar(Scalar { v: mat[0].floats[0].f, t: start }), warnings)),
                ValueType::Matrix => {
                    let mut mat = mat;
                    self.sort_matrix_result(&mut mat);
                    Ok((Value::Matrix(mat), warnings))
                }
                t => panic!("promql.Engine.exec: unexpected expression type {}", t),
            };
        }
        let mut mat = match val {
            Value::Matrix(m) => m,
            other => panic!("promql.Engine.exec: invalid expression type {}", other.value_type()),
        };
        if let Err(e) = context_done(ctx, "expression evaluation") {
            return Err((e, warnings));
        }
        self.sort_matrix_result(&mut mat);
        Ok((Value::Matrix(mat), warnings))
    }

    fn sort_matrix_result(&mut self, mat: &mut Matrix) {
        self.stats.start(QueryTiming::ResultSortTime);
        sort_matrix(mat);
        self.stats.finish(QueryTiming::ResultSortTime);
    }
}
