//! Aggregations (Go `rangeEvalAgg`, `aggregation`, `aggregationK`, `aggregationCountValues`, `fParams`).

use std::collections::HashMap;
use std::sync::Arc;

use model::histogram::{CounterResetHint, ErrorKind, FloatHistogram, kahansum};
use model::labels::{Labels, METRIC_NAME};
use storage::annotations::{self as annos, HistogramOperation};
use storage::{Annotations, Error};

use super::binop::is_histogram_error;
use super::eval::{
    CursorMatrix, EvalResult, errorf, generate_grouping_key, generate_grouping_labels, pr,
};
use super::{EvalNodeHelper, Evaluator, MAX_INT64, MIN_INT64};
use crate::gosort;
use crate::parser::{
    AVG, AggregateExpr, BOTTOMK, COUNT, Expr, GROUP, LIMIT_RATIO, LIMITK, MAX, MIN, QUANTILE, STDDEV, STDVAR, SUM,
    TOPK,
};
use crate::value::*;

/// The values of an aggregation parameter per step (Go `fParams`).
pub(crate) struct FParams {
    floats: Vec<FPoint>,
    pos: usize,
    const_value: f64,
    is_constant: bool,
    min_value: f64,
    max_value: f64,
    has_any_nan: bool,
}

impl FParams {
    /// Go `newFParams`.
    pub(crate) fn new(ev: &mut Evaluator<'_>, expr: Option<&Expr>) -> EvalResult<(FParams, Annotations)> {
        let empty = FParams {
            floats: Vec::new(),
            pos: 0,
            const_value: 0.0,
            is_constant: false,
            min_value: 0.0,
            max_value: 0.0,
            has_any_nan: false,
        };
        let Some(expr) = expr else {
            return Ok((empty, Annotations::new()));
        };
        let const_param = matches!(expr, Expr::NumberLiteral(_));
        let (val, ws) = ev.eval(expr)?;
        let mat = match val {
            Value::Matrix(m) if !m.is_empty() => m,
            _ => return Ok((empty, ws)),
        };
        let series = mat.into_iter().next().unwrap();
        let mut fp = FParams {
            floats: series.floats,
            pos: 0,
            const_value: 0.0,
            is_constant: const_param,
            min_value: f64::MAX,
            max_value: -f64::MAX,
            has_any_nan: false,
        };
        if const_param {
            fp.const_value = fp.floats[0].f;
            fp.min_value = fp.const_value;
            fp.max_value = fp.const_value;
            fp.has_any_nan = fp.const_value.is_nan();
            return Ok((fp, ws));
        }
        for v in &fp.floats {
            fp.max_value = crate::durations::go_max(fp.max_value, v.f);
            fp.min_value = crate::durations::go_min(fp.min_value, v.f);
            if v.f.is_nan() {
                fp.has_any_nan = true;
            }
        }
        Ok((fp, ws))
    }

    fn max(&self) -> f64 {
        self.max_value
    }
    fn min(&self) -> f64 {
        self.min_value
    }

    fn next(&mut self) -> f64 {
        if self.is_constant {
            return self.const_value;
        }
        if self.pos < self.floats.len() {
            let v = self.floats[self.pos].f;
            self.pos += 1;
            return v;
        }
        0.0
    }
}

/// An aggregation group (Go `groupedAggregation`).
#[derive(Default)]
struct GroupedAggregation {
    float_value: f64,
    float_mean: f64,
    float_kahan_c: f64,
    histogram_value: Option<FloatHistogram>,
    histogram_mean: Option<FloatHistogram>,
    histogram_kahan_c: Option<FloatHistogram>,
    group_count: f64,
    heap: Vec<Sample>,
    quantile_values: Vec<f64>,
    seen: bool,
    has_float: bool,
    has_histogram: bool,
    incompatible_histograms: bool,
    group_aggr_complete: bool,
    float_incremental_mean: bool,
    histogram_incremental_mean: bool,
    counter_reset_seen: bool,
    not_counter_reset_seen: bool,
    drop_name: bool,
}

/// Allows tests to replace the sampling of `limit_ratio` (Go `RatioSampler`).
pub trait RatioSampler: Send + Sync {
    fn sample_offset(&self, metric: &Labels) -> f64;
    fn add_ratio_sample(&self, ratio_limit: f64, sample: &Sample) -> bool {
        self.add_ratio_sample_with_offset(ratio_limit, self.sample_offset(&sample.metric))
    }
    fn add_ratio_sample_with_offset(&self, ratio_limit: f64, sample_offset: f64) -> bool;
}

/// Uses the hash of the labels as a deterministic offset in [0, 1) (Go `HashRatioSampler`).
#[derive(Clone, Copy, Debug, Default)]
pub struct HashRatioSampler;

impl RatioSampler for HashRatioSampler {
    fn sample_offset(&self, metric: &Labels) -> f64 {
        metric.hash() as f64 / u64::MAX as f64
    }

    fn add_ratio_sample_with_offset(&self, ratio_limit: f64, sample_offset: f64) -> bool {
        (ratio_limit >= 0.0 && sample_offset < ratio_limit) || (ratio_limit < 0.0 && sample_offset >= 1.0 + ratio_limit)
    }
}

fn by_value_less(a: &Sample, b: &Sample) -> bool {
    if a.f.is_nan() {
        return true;
    }
    a.f < b.f
}

fn by_reverse_value_less(a: &Sample, b: &Sample) -> bool {
    if a.f.is_nan() {
        return true;
    }
    a.f > b.f
}

/// Go `handleAggregationError`.
fn handle_aggregation_error(err: Error, e: &AggregateExpr, metric_name: &str, annos: &mut Annotations) -> EvalResult<()> {
    let pos = pr(e.expr.as_ref().unwrap().position_range());
    if is_histogram_error(&err, ErrorKind::HistogramsIncompatibleSchema) {
        annos.add(annos::new_mixed_exponential_custom_histograms_warning(metric_name, pos));
        return Ok(());
    }
    Err(err)
}

fn kahan_add(
    value: &mut FloatHistogram,
    h: &FloatHistogram,
    c: Option<FloatHistogram>,
) -> Result<(FloatHistogram, bool, bool), Error> {
    value.kahan_add(h, c).map_err(Error::from)
}

impl<'a> Evaluator<'a> {
    /// Go `rangeEvalAgg`.
    pub(crate) fn range_eval_agg(
        &mut self,
        agg: &AggregateExpr,
        sorted_grouping: &[String],
        input: Matrix,
        mut params: FParams,
    ) -> EvalResult<(Matrix, Annotations)> {
        let mut annos = Annotations::new();
        let mut enh = self.new_enh();
        let temp_num_samples = self.current_samples;
        let mut buf = Vec::with_capacity(1024);
        let mut group_to_result: HashMap<u64, usize> = HashMap::new();
        let mut series_to_result = Vec::with_capacity(input.len());
        let mut result: Matrix = Vec::new();
        let op = agg.op;
        let is_k = matches!(op, TOPK | BOTTOMK | LIMITK | LIMIT_RATIO);
        let mut group_count = 0usize;
        for s in &input {
            let key = generate_grouping_key(&s.metric, sorted_grouping, agg.without, &mut buf);
            let index = match group_to_result.get(&key) {
                Some(&i) => i,
                None => {
                    if !is_k {
                        let m = generate_grouping_labels(&mut enh, &s.metric, agg.without, sorted_grouping);
                        result.push(Series::new(m));
                    }
                    let i = group_count;
                    group_to_result.insert(key, i);
                    group_count += 1;
                    i
                }
            };
            series_to_result.push(index);
        }
        let mut groups: Vec<GroupedAggregation> = (0..group_count).map(|_| GroupedAggregation::default()).collect();
        let mut k_series: Vec<Series> = Vec::new();
        let mut k_index: HashMap<u64, usize> = HashMap::new();

        let param_pos = || pr(agg.param.as_ref().unwrap().position_range());
        match op {
            TOPK | BOTTOMK | LIMITK => {
                if params.max() < 1.0 {
                    return Ok((Vec::new(), annos));
                }
                if params.has_any_nan {
                    return Err(errorf("Parameter value is NaN"));
                }
                let p = params.min();
                if p <= MIN_INT64 {
                    return Err(errorf(format!("Scalar value {} underflows int64", go_v(p))));
                }
                let p = params.max();
                if p >= MAX_INT64 {
                    return Err(errorf(format!("Scalar value {} overflows int64", go_v(p))));
                }
            }
            LIMIT_RATIO => {
                if params.max() == 0.0 && params.min() == 0.0 {
                    return Ok((Vec::new(), annos));
                }
                if params.has_any_nan {
                    return Err(errorf("Ratio value is NaN"));
                }
                if params.max() > 1.0 {
                    annos.add(annos::new_invalid_ratio_warning(params.max(), 1.0, param_pos()));
                }
                if params.min() < -1.0 {
                    annos.add(annos::new_invalid_ratio_warning(params.min(), -1.0, param_pos()));
                }
            }
            QUANTILE => {
                if params.has_any_nan {
                    annos.add(annos::new_invalid_quantile_warning(f64::NAN, param_pos()));
                }
                if params.max() > 1.0 {
                    annos.add(annos::new_invalid_quantile_warning(params.max(), param_pos()));
                }
                if params.min() < 0.0 {
                    annos.add(annos::new_invalid_quantile_warning(params.min(), param_pos()));
                }
            }
            _ => {}
        }

        let mut input = CursorMatrix::new(input);
        let mut ts = self.start_timestamp;
        while ts <= self.end_timestamp {
            let fparam = params.next();
            self.check_ctx("expression evaluation")?;
            self.current_samples = temp_num_samples;
            enh.ts = ts;
            if is_k {
                let (mat, ws) = self.aggregation_k(agg, fparam, &mut input, &series_to_result, &mut groups, &mut enh, &mut k_series, &mut k_index)?;
                if self.start_timestamp == self.end_timestamp {
                    annos.merge(ws);
                    return Ok((mat, annos));
                }
                annos.merge(ws);
            } else {
                let ws = self.aggregation(agg, fparam, &mut input, &mut result, &series_to_result, &mut groups, &mut enh)?;
                annos.merge(ws);
            }
            self.check_samples()?;
            ts += self.interval;
        }
        if is_k {
            return Ok((k_series, annos));
        }
        result.retain(|s| !s.floats.is_empty() || !s.histograms.is_empty());
        Ok((result, annos))
    }

    /// Go `aggregation`: sum, avg, count, group, min, max, stdvar, stddev and quantile at one step.
    #[allow(clippy::too_many_arguments)]
    fn aggregation(
        &mut self,
        e: &AggregateExpr,
        q: f64,
        input: &mut CursorMatrix,
        output: &mut Matrix,
        series_to_result: &[usize],
        groups: &mut [GroupedAggregation],
        enh: &mut EvalNodeHelper,
    ) -> EvalResult<Annotations> {
        let op = e.op;
        let mut annos = Annotations::new();
        let expr_pos = pr(e.expr.as_ref().unwrap().position_range());
        for g in groups.iter_mut() {
            g.seen = false;
        }
        for si in 0..input.mat.len() {
            let drop_name_si = input.mat[si].drop_name;
            let Some((f, h)) = input.next_values(si, enh.ts) else {
                continue;
            };
            let h: Option<HistogramRef> = h.cloned();
            let group = &mut groups[series_to_result[si]];
            if !group.seen {
                *group = GroupedAggregation {
                    seen: true,
                    float_value: f,
                    float_mean: f,
                    group_count: 1.0,
                    drop_name: drop_name_si,
                    ..Default::default()
                };
                match op {
                    AVG | SUM => match &h {
                        None => group.has_float = true,
                        Some(h) => {
                            group.histogram_value = Some((**h).clone());
                            group.has_histogram = true;
                            match h.counter_reset_hint {
                                CounterResetHint::CounterReset => group.counter_reset_seen = true,
                                CounterResetHint::NotCounterReset => group.not_counter_reset_seen = true,
                                _ => {}
                            }
                        }
                    },
                    STDVAR | STDDEV => {
                        if h.is_some() {
                            group.seen = false;
                            let n = if op == STDVAR { "stdvar" } else { "stddev" };
                            annos.add(annos::new_histogram_ignored_in_aggregation_info(n, expr_pos));
                        } else if f.is_nan() || f.is_infinite() {
                            group.float_value = f64::NAN;
                        } else {
                            group.float_value = 0.0;
                        }
                    }
                    QUANTILE => {
                        if h.is_some() {
                            group.seen = false;
                            annos.add(annos::new_histogram_ignored_in_aggregation_info("quantile", expr_pos));
                        }
                        group.quantile_values = vec![f];
                    }
                    GROUP => group.float_value = 1.0,
                    MIN | MAX => {
                        if h.is_some() {
                            group.seen = false;
                            let n = if op == MIN { "min" } else { "max" };
                            annos.add(annos::new_histogram_ignored_in_aggregation_info(n, expr_pos));
                        }
                    }
                    _ => {}
                }
                continue;
            }
            if group.incompatible_histograms {
                continue;
            }
            if drop_name_si {
                group.drop_name = true;
            }
            let metric_name = || input_metric_name(&input.mat[si]);
            match op {
                SUM => match &h {
                    Some(h) => {
                        group.has_histogram = true;
                        if let Some(hv) = group.histogram_value.as_mut() {
                            match h.counter_reset_hint {
                                CounterResetHint::CounterReset => group.counter_reset_seen = true,
                                CounterResetHint::NotCounterReset => group.not_counter_reset_seen = true,
                                _ => {}
                            }
                            match kahan_add(hv, h, group.histogram_kahan_c.take()) {
                                Ok((c, _, reconciled)) => {
                                    group.histogram_kahan_c = Some(c);
                                    if reconciled {
                                        annos.add(annos::new_mismatched_custom_buckets_histograms_info(expr_pos, HistogramOperation::Agg));
                                    }
                                }
                                Err(err) => {
                                    handle_aggregation_error(err, e, &metric_name(), &mut annos)?;
                                    group.incompatible_histograms = true;
                                }
                            }
                        }
                    }
                    None => {
                        group.has_float = true;
                        let (v, c) = kahansum::inc(f, group.float_value, group.float_kahan_c);
                        group.float_value = v;
                        group.float_kahan_c = c;
                    }
                },
                AVG => {
                    group.group_count += 1.0;
                    match &h {
                        Some(h) => {
                            group.has_histogram = true;
                            if group.histogram_value.is_some() {
                                match h.counter_reset_hint {
                                    CounterResetHint::CounterReset => group.counter_reset_seen = true,
                                    CounterResetHint::NotCounterReset => group.not_counter_reset_seen = true,
                                    _ => {}
                                }
                                let mut done = false;
                                if !group.histogram_incremental_mean {
                                    let mut v = group.histogram_value.clone().unwrap();
                                    let c = group.histogram_kahan_c.clone();
                                    match kahan_add(&mut v, h, c) {
                                        Ok((c, _, reconciled)) => {
                                            if reconciled {
                                                annos.add(annos::new_mismatched_custom_buckets_histograms_info(expr_pos, HistogramOperation::Agg));
                                            }
                                            if !v.has_overflow() {
                                                group.histogram_value = Some(v);
                                                group.histogram_kahan_c = Some(c);
                                                done = true;
                                            } else {
                                                group.histogram_incremental_mean = true;
                                                let mut m = group.histogram_value.clone().unwrap();
                                                m.div(group.group_count - 1.0);
                                                group.histogram_mean = Some(m);
                                                if let Some(kc) = group.histogram_kahan_c.as_mut() {
                                                    kc.div(group.group_count - 1.0);
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            handle_aggregation_error(err, e, &metric_name(), &mut annos)?;
                                            group.incompatible_histograms = true;
                                            continue;
                                        }
                                    }
                                }
                                if !done {
                                    let q = (group.group_count - 1.0) / group.group_count;
                                    if let Some(kc) = group.histogram_kahan_c.as_mut() {
                                        kc.mul(q);
                                    }
                                    let mut to_add = (**h).clone();
                                    to_add.div(group.group_count);
                                    let mean = group.histogram_mean.as_mut().unwrap();
                                    mean.mul(q);
                                    match kahan_add(mean, &to_add, group.histogram_kahan_c.take()) {
                                        Ok((c, _, reconciled)) => {
                                            group.histogram_kahan_c = Some(c);
                                            if reconciled {
                                                annos.add(annos::new_mismatched_custom_buckets_histograms_info(expr_pos, HistogramOperation::Agg));
                                            }
                                        }
                                        Err(err) => {
                                            handle_aggregation_error(err, e, &metric_name(), &mut annos)?;
                                            group.incompatible_histograms = true;
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                        None => {
                            group.has_float = true;
                            let mut done = false;
                            if !group.float_incremental_mean {
                                let (nv, nc) = kahansum::inc(f, group.float_value, group.float_kahan_c);
                                if !nv.is_infinite() {
                                    group.float_value = nv;
                                    group.float_kahan_c = nc;
                                    done = true;
                                } else {
                                    group.float_incremental_mean = true;
                                    group.float_mean = group.float_value / (group.group_count - 1.0);
                                    group.float_kahan_c /= group.group_count - 1.0;
                                }
                            }
                            if !done {
                                let q = (group.group_count - 1.0) / group.group_count;
                                let (m, c) = kahansum::inc(f / group.group_count, q * group.float_mean, q * group.float_kahan_c);
                                group.float_mean = m;
                                group.float_kahan_c = c;
                            }
                        }
                    }
                }
                GROUP => {}
                MAX => {
                    if h.is_some() {
                        annos.add(annos::new_histogram_ignored_in_aggregation_info("max", expr_pos));
                        continue;
                    }
                    if group.float_value < f || group.float_value.is_nan() {
                        group.float_value = f;
                    }
                }
                MIN => {
                    if h.is_some() {
                        annos.add(annos::new_histogram_ignored_in_aggregation_info("min", expr_pos));
                        continue;
                    }
                    if group.float_value > f || group.float_value.is_nan() {
                        group.float_value = f;
                    }
                }
                COUNT => group.group_count += 1.0,
                STDVAR | STDDEV => {
                    if h.is_none() {
                        group.group_count += 1.0;
                        let delta = f - group.float_mean;
                        group.float_mean += delta / group.group_count;
                        group.float_value += delta * (f - group.float_mean);
                    } else {
                        let n = if op == STDVAR { "stdvar" } else { "stddev" };
                        annos.add(annos::new_histogram_ignored_in_aggregation_info(n, expr_pos));
                    }
                }
                QUANTILE => {
                    if h.is_some() {
                        annos.add(annos::new_histogram_ignored_in_aggregation_info("quantile", expr_pos));
                        continue;
                    }
                    group.quantile_values.push(f);
                }
                _ => panic!("expected aggregation operator but got {:?}", op.to_string()),
            }
        }

        let num_steps = ((self.end_timestamp - self.start_timestamp) / self.interval) as usize + 1;
        for (ri, aggr) in groups.iter_mut().enumerate() {
            if !aggr.seen {
                continue;
            }
            let mut out_h: Option<FloatHistogram> = None;
            match op {
                AVG => {
                    if aggr.has_float && aggr.has_histogram {
                        annos.add(annos::new_mixed_floats_histograms_agg_warning(expr_pos));
                        continue;
                    }
                    if aggr.incompatible_histograms {
                        continue;
                    } else if aggr.has_histogram {
                        let mut v = if aggr.histogram_incremental_mean {
                            let mut m = aggr.histogram_mean.take().unwrap();
                            if let Some(c) = &aggr.histogram_kahan_c {
                                let _ = m.add(c);
                            }
                            m
                        } else {
                            let mut v = aggr.histogram_value.take().unwrap();
                            v.div(aggr.group_count);
                            if let Some(c) = aggr.histogram_kahan_c.as_mut() {
                                c.div(aggr.group_count);
                                let _ = v.add(c);
                            }
                            v
                        };
                        v.compact(0);
                        out_h = Some(v);
                    } else if aggr.float_incremental_mean {
                        aggr.float_value = aggr.float_mean + aggr.float_kahan_c;
                    } else {
                        aggr.float_value = aggr.float_value / aggr.group_count + aggr.float_kahan_c / aggr.group_count;
                    }
                }
                COUNT => aggr.float_value = aggr.group_count,
                STDVAR => aggr.float_value /= aggr.group_count,
                STDDEV => aggr.float_value = (aggr.float_value / aggr.group_count).sqrt(),
                QUANTILE => aggr.float_value = crate::quantile::quantile(q, &mut aggr.quantile_values),
                SUM => {
                    if aggr.has_float && aggr.has_histogram {
                        annos.add(annos::new_mixed_floats_histograms_agg_warning(expr_pos));
                        continue;
                    }
                    if aggr.incompatible_histograms {
                        continue;
                    } else if aggr.has_histogram {
                        let mut v = aggr.histogram_value.take().unwrap();
                        if let Some(c) = &aggr.histogram_kahan_c {
                            let _ = v.add(c);
                        }
                        v.compact(0);
                        out_h = Some(v);
                    } else {
                        aggr.float_value += aggr.float_kahan_c;
                    }
                }
                _ => {}
            }
            if aggr.counter_reset_seen && aggr.not_counter_reset_seen {
                annos.add(annos::new_histogram_counter_reset_collision_warning(expr_pos, HistogramOperation::Agg));
            }
            let ss = &mut output[ri];
            match out_h {
                None => {
                    if ss.floats.capacity() == 0 {
                        ss.floats.reserve_exact(num_steps.min(5000));
                    }
                    ss.floats.push(FPoint { t: enh.ts, f: aggr.float_value });
                }
                Some(h) => {
                    if ss.histograms.capacity() == 0 {
                        ss.histograms.reserve_exact(num_steps.min(5000));
                    }
                    ss.histograms.push(HPoint { t: enh.ts, h: Arc::new(h) });
                }
            }
            ss.drop_name = aggr.drop_name;
        }
        Ok(annos)
    }

    /// Go `aggregationK`: topk, bottomk, limitk and limit_ratio at one step.
    #[allow(clippy::too_many_arguments)]
    fn aggregation_k(
        &mut self,
        e: &AggregateExpr,
        fparam: f64,
        input: &mut CursorMatrix,
        series_to_result: &[usize],
        groups: &mut [GroupedAggregation],
        enh: &mut EvalNodeHelper,
        k_series: &mut Vec<Series>,
        k_index: &mut HashMap<u64, usize>,
    ) -> EvalResult<(Matrix, Annotations)> {
        let op = e.op;
        let mut annos = Annotations::new();
        let mut groups_remaining = groups.len();
        for g in groups.iter_mut() {
            g.seen = false;
        }
        let pos = pr(e.pos_range);
        let sampler = HashRatioSampler;
        let n = input.mat.len();
        let advance_remaining = |input: &mut CursorMatrix, ts: i64, start: usize| {
            for i in start..input.mat.len() {
                let _ = input.next_values(i, ts);
            }
        };

        let mut si = 0;
        while si < n {
            let next = input.next_values(si, enh.ts).map(|(f, h)| (f, h.cloned()));
            let Some((f, h)) = next else {
                si += 1;
                continue;
            };
            let s = Sample {
                metric: input.mat[si].metric.clone(),
                f,
                h,
                t: 0,
                drop_name: input.mat[si].drop_name,
            };
            let mut k: i64 = 0;
            let mut r = 0.0;
            match op {
                TOPK | BOTTOMK | LIMITK => {
                    k = (fparam as i64).min(n as i64);
                    if k < 1 {
                        if enh.ts != self.end_timestamp {
                            advance_remaining(input, enh.ts, si + 1);
                        }
                        return Ok((Vec::new(), annos));
                    }
                }
                LIMIT_RATIO => {
                    if fparam == 0.0 {
                        if enh.ts != self.end_timestamp {
                            advance_remaining(input, enh.ts, si + 1);
                        }
                        return Ok((Vec::new(), annos));
                    }
                    r = if fparam < -1.0 {
                        -1.0
                    } else if fparam > 1.0 {
                        1.0
                    } else {
                        fparam
                    };
                }
                _ => {}
            }
            let group = &mut groups[series_to_result[si]];
            if !group.seen {
                match op {
                    LIMIT_RATIO => {
                        *group = GroupedAggregation { seen: true, ..Default::default() };
                        if sampler.add_ratio_sample(r, &s) {
                            gosort::heap_push(&mut group.heap, s, &mut by_value_less);
                        }
                    }
                    LIMITK => {
                        *group = GroupedAggregation { seen: true, ..Default::default() };
                        group.heap.reserve(k as usize);
                        group.heap.push(s);
                    }
                    TOPK | BOTTOMK => {
                        *group = GroupedAggregation { seen: true, ..Default::default() };
                        if s.h.is_some() {
                            group.seen = false;
                            let name = if op == TOPK { "topk" } else { "bottomk" };
                            annos.add(annos::new_histogram_ignored_in_aggregation_info(name, pos));
                        } else if op == TOPK {
                            gosort::heap_push(&mut group.heap, s, &mut by_value_less);
                        } else {
                            gosort::heap_push(&mut group.heap, s, &mut by_reverse_value_less);
                        }
                    }
                    _ => {}
                }
                si += 1;
                continue;
            }
            match op {
                TOPK => {
                    if s.h.is_some() {
                        annos.add(annos::new_histogram_ignored_in_aggregation_info("topk", pos));
                    } else if (group.heap.len() as i64) < k {
                        gosort::heap_push(&mut group.heap, s, &mut by_value_less);
                    } else if group.heap[0].f < s.f || (group.heap[0].f.is_nan() && !s.f.is_nan()) {
                        group.heap[0] = s;
                        if k > 1 {
                            gosort::heap_fix(&mut group.heap, 0, &mut by_value_less);
                        }
                    }
                }
                BOTTOMK => {
                    if s.h.is_some() {
                        annos.add(annos::new_histogram_ignored_in_aggregation_info("bottomk", pos));
                    } else if (group.heap.len() as i64) < k {
                        gosort::heap_push(&mut group.heap, s, &mut by_reverse_value_less);
                    } else if group.heap[0].f > s.f || (group.heap[0].f.is_nan() && !s.f.is_nan()) {
                        group.heap[0] = s;
                        if k > 1 {
                            gosort::heap_fix(&mut group.heap, 0, &mut by_reverse_value_less);
                        }
                    }
                }
                LIMITK => {
                    if (group.heap.len() as i64) < k {
                        gosort::heap_push(&mut group.heap, s, &mut by_value_less);
                    }
                    if !group.group_aggr_complete && group.heap.len() as i64 == k {
                        group.group_aggr_complete = true;
                        groups_remaining -= 1;
                        if groups_remaining == 0 {
                            if enh.ts != self.end_timestamp {
                                advance_remaining(input, enh.ts, si + 1);
                            }
                            break;
                        }
                    }
                }
                LIMIT_RATIO => {
                    if sampler.add_ratio_sample(r, &s) {
                        gosort::heap_push(&mut group.heap, s, &mut by_value_less);
                    }
                }
                _ => panic!("expected aggregation operator but got {:?}", op.to_string()),
            }
            si += 1;
        }

        let num_steps = ((self.end_timestamp - self.start_timestamp) / self.interval) as usize + 1;
        let instant = self.end_timestamp == self.start_timestamp;
        let mut mat: Matrix = if instant { Vec::with_capacity(groups.len()) } else { Vec::new() };
        let ts = enh.ts;
        let mut add = |s: Sample| {
            if instant {
                match s.h {
                    Some(h) => mat.push(Series {
                        metric: s.metric,
                        floats: Vec::new(),
                        histograms: vec![HPoint { t: ts, h }],
                        drop_name: s.drop_name,
                    }),
                    None => mat.push(Series {
                        metric: s.metric,
                        floats: vec![FPoint { t: ts, f: s.f }],
                        histograms: Vec::new(),
                        drop_name: s.drop_name,
                    }),
                }
            } else {
                let hash = s.metric.hash();
                let idx = match k_index.get(&hash) {
                    Some(&i) => i,
                    None => {
                        let mut ss = Series::new(s.metric.clone());
                        ss.drop_name = s.drop_name;
                        k_series.push(ss);
                        k_index.insert(hash, k_series.len() - 1);
                        k_series.len() - 1
                    }
                };
                let ss = &mut k_series[idx];
                match s.h {
                    None => {
                        if ss.floats.capacity() == 0 {
                            ss.floats.reserve_exact(num_steps.min(5000));
                        }
                        ss.floats.push(FPoint { t: ts, f: s.f });
                    }
                    Some(h) => {
                        if ss.histograms.capacity() == 0 {
                            ss.histograms.reserve_exact(num_steps.min(5000));
                        }
                        ss.histograms.push(HPoint { t: ts, h });
                    }
                }
            }
        };
        for aggr in groups.iter_mut() {
            if !aggr.seen {
                continue;
            }
            let mut heap = std::mem::take(&mut aggr.heap);
            match op {
                TOPK => {
                    if heap.len() > 1 {
                        gosort::sort(&mut heap, |a, b| by_value_less(b, a));
                    }
                }
                BOTTOMK => {
                    if heap.len() > 1 {
                        gosort::sort(&mut heap, |a, b| by_reverse_value_less(b, a));
                    }
                }
                _ => {}
            }
            for v in heap {
                add(v);
            }
        }
        Ok((mat, annos))
    }
}

fn input_metric_name(s: &Series) -> String {
    s.metric.get(METRIC_NAME).to_string()
}

/// Go `%v` of a float64.
fn go_v(f: f64) -> String {
    model::histogram::gofmt::g(f)
}

/// Go `aggregationCountValues`.
pub(crate) fn aggregation_count_values(
    e: &AggregateExpr,
    grouping: &[String],
    value_label: &str,
    vec: &[Sample],
    enh: &mut EvalNodeHelper,
) -> EvalResult<Annotations> {
    let mut order: Vec<u64> = Vec::new();
    let mut result: HashMap<u64, (Labels, usize)> = HashMap::new();
    let mut buf = Vec::new();
    for s in vec {
        let value = match &s.h {
            None => format_float(s.f),
            Some(h) => h.string(),
        };
        let lb = enh.reset_builder(s.metric.clone());
        lb.set(value_label, &value);
        let metric = lb.labels();
        let key = generate_grouping_key(&metric, grouping, e.without, &mut buf);
        match result.get_mut(&key) {
            Some(g) => g.1 += 1,
            None => {
                let l = generate_grouping_labels(enh, &metric, e.without, grouping);
                result.insert(key, (l, 1));
                order.push(key);
            }
        }
    }
    for k in order {
        let (l, c) = result.remove(&k).unwrap();
        enh.out.push(Sample { metric: l, f: c as f64, ..Default::default() });
    }
    Ok(Annotations::new())
}
