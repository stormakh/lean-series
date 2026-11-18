//! Port of `promql/functions.go`: the PromQL functions.
//!
//! The contract with the engine:
//! - A [`FunctionCall`] gets the instant vector arguments (`vals`; scalars are one-sample vectors, string
//!   arguments are empty vectors), the range vector argument (`matrix`, one series, empty for functions without
//!   one), the argument expressions, and the node helper. It appends its output to `enh.out` (cleared by the
//!   caller) and returns the annotations, or an error where Go panics. For range vector functions, `vals` holds
//!   the other arguments in order (Go `vectorVals`).
//! - For range vector functions, the range, offset and the anchored/smoothed flags of the matrix argument are in
//!   `enh.range`, `enh.offset` (nanoseconds), `enh.anchored`, `enh.smoothed` (Go reads them from `args[0]`,
//!   which may be a subquery here).

pub mod gomath;

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use model::histogram::{BucketIterator, CounterResetHint, ErrorKind, FloatHistogram};
use model::labels::{self, Builder, Labels, MatchType, BUCKET_LABEL, METRIC_NAME};
use model::regexp::Regexp;
use model::schema;
use storage::annotations::{self as annos, HistogramOperation, PositionRange as AnnoPos};
use storage::{Annotations, Error};

use crate::durations::{go_max, go_min};
use crate::engine::{EvalNodeHelper, Evaluator};
use crate::gosort;
use crate::parser::{self, Expr, FunctionId};
use crate::quantile::{self, Bucket, MetricWithBuckets, EXCLUDED_LABELS};
use crate::value::{FPoint, HPoint, Sample, Series, Value, Vector};

/// A function implementation (Go `FunctionCall`).
pub type FunctionCall =
    fn(vals: &[Vector], matrix: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error>;

/// The functions supported by PromQL (Go `FunctionCalls`); `None` for the functions the engine evaluates itself
/// (label_replace, label_join, info) or folds (start, end, range, step).
pub static FUNCTION_CALLS: [(&str, Option<FunctionCall>); 91] = [
    ("abs", Some(func_abs)),
    ("absent", Some(func_absent)),
    ("absent_over_time", Some(func_absent_over_time)),
    ("acos", Some(func_acos)),
    ("acosh", Some(func_acosh)),
    ("asin", Some(func_asin)),
    ("asinh", Some(func_asinh)),
    ("atan", Some(func_atan)),
    ("atanh", Some(func_atanh)),
    ("avg_over_time", Some(func_avg_over_time)),
    ("ceil", Some(func_ceil)),
    ("changes", Some(func_changes)),
    ("clamp", Some(func_clamp)),
    ("clamp_max", Some(func_clamp_max)),
    ("clamp_min", Some(func_clamp_min)),
    ("cos", Some(func_cos)),
    ("cosh", Some(func_cosh)),
    ("count_over_time", Some(func_count_over_time)),
    ("days_in_month", Some(func_days_in_month)),
    ("day_of_month", Some(func_day_of_month)),
    ("day_of_week", Some(func_day_of_week)),
    ("day_of_year", Some(func_day_of_year)),
    ("deg", Some(func_deg)),
    ("end", None), // Folded into NumberLiteral by foldQueryContextFunctions.
    ("delta", Some(func_delta)),
    ("deriv", Some(func_deriv)),
    ("exp", Some(func_exp)),
    ("first_over_time", Some(func_first_over_time)),
    ("floor", Some(func_floor)),
    ("histogram_avg", Some(func_histogram_avg)),
    ("histogram_count", Some(func_histogram_count)),
    ("histogram_fraction", Some(func_histogram_fraction)),
    ("histogram_quantile", Some(func_histogram_quantile)),
    ("histogram_quantiles", Some(func_histogram_quantiles)),
    ("histogram_sum", Some(func_histogram_sum)),
    ("histogram_stddev", Some(func_histogram_stddev)),
    ("histogram_stdvar", Some(func_histogram_stdvar)),
    ("double_exponential_smoothing", Some(func_double_exponential_smoothing)),
    ("hour", Some(func_hour)),
    ("idelta", Some(func_idelta)),
    ("increase", Some(func_increase)),
    ("info", None),
    ("integral", Some(func_integral)),
    ("irate", Some(func_irate)),
    ("max_of", Some(func_max_of)),
    ("label_replace", None), // evalLabelReplace not called via this map.
    ("label_join", None),    // evalLabelJoin not called via this map.
    ("min_of", Some(func_min_of)),
    ("ln", Some(func_ln)),
    ("log10", Some(func_log10)),
    ("log2", Some(func_log2)),
    ("last_over_time", Some(func_last_over_time)),
    ("mad_over_time", Some(func_mad_over_time)),
    ("max_over_time", Some(func_max_over_time)),
    ("min_over_time", Some(func_min_over_time)),
    ("ts_of_first_over_time", Some(func_ts_of_first_over_time)),
    ("ts_of_last_over_time", Some(func_ts_of_last_over_time)),
    ("ts_of_max_over_time", Some(func_ts_of_max_over_time)),
    ("ts_of_min_over_time", Some(func_ts_of_min_over_time)),
    ("minute", Some(func_minute)),
    ("month", Some(func_month)),
    ("pi", Some(func_pi)),
    ("predict_linear", Some(func_predict_linear)),
    ("present_over_time", Some(func_present_over_time)),
    ("quantile_over_time", Some(func_quantile_over_time)),
    ("rad", Some(func_rad)),
    ("range", None), // Folded into NumberLiteral by foldQueryContextFunctions.
    ("rate", Some(func_rate)),
    ("resets", Some(func_resets)),
    ("round", Some(func_round)),
    ("scalar", Some(func_scalar)),
    ("sgn", Some(func_sgn)),
    ("sin", Some(func_sin)),
    ("sinh", Some(func_sinh)),
    ("sort", Some(func_sort)),
    ("sort_desc", Some(func_sort_desc)),
    ("sort_by_label", Some(func_sort_by_label)),
    ("sort_by_label_desc", Some(func_sort_by_label_desc)),
    ("start", None), // Folded into NumberLiteral by foldQueryContextFunctions.
    ("start_timestamp", Some(func_start_timestamp)),
    ("step", None), // Folded into NumberLiteral by foldQueryContextFunctions.
    ("sqrt", Some(func_sqrt)),
    ("stddev_over_time", Some(func_stddev_over_time)),
    ("stdvar_over_time", Some(func_stdvar_over_time)),
    ("sum_over_time", Some(func_sum_over_time)),
    ("tan", Some(func_tan)),
    ("tanh", Some(func_tanh)),
    ("time", Some(func_time)),
    ("timestamp", Some(func_timestamp)),
    ("vector", Some(func_vector)),
    ("year", Some(func_year)),
];

/// The implementation of a built-in function, by id; `None` for functions the engine evaluates itself
/// (label_replace, label_join, info) or folds (start, end, range, step).
pub fn function_call(id: FunctionId) -> Option<FunctionCall> {
    static TABLE: OnceLock<Vec<Option<FunctionCall>>> = OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let n = parser::functions().len();
        (0..n)
            .map(|i| {
                let f = parser::function_by_id(FunctionId(i as u16))?;
                FUNCTION_CALLS.iter().find(|(name, _)| *name == f.name).and_then(|(_, c)| *c)
            })
            .collect()
    });
    table.get(id.0 as usize).copied().flatten()
}

/// Go `AtModifierUnsafeFunctions`.
pub fn is_at_modifier_unsafe(name: &str) -> bool {
    matches!(
        name,
        "days_in_month"
            | "day_of_month"
            | "day_of_week"
            | "day_of_year"
            | "end"
            | "hour"
            | "minute"
            | "month"
            | "year"
            | "predict_linear"
            | "range"
            | "start"
            | "step"
            | "time"
            | "timestamp"
    )
}

/// Go `AnchoredSafeFunctions`, sorted.
pub const ANCHORED_SAFE_FUNCTIONS: [&str; 5] = ["changes", "delta", "increase", "rate", "resets"];
/// Go `SmoothedSafeFunctions`, sorted.
pub const SMOOTHED_SAFE_FUNCTIONS: [&str; 3] = ["delta", "increase", "rate"];

// ---------------------------------------------------------------------------------------------------------------
// Small helpers.

/// Converts a parser position range to an annotation position range.
#[inline]
fn pr(p: parser::posrange::PositionRange) -> AnnoPos {
    AnnoPos::new(p.start as i64, p.end as i64)
}

/// Go `args[i].PositionRange()`. Go replaces a subquery argument with a matrix selector without positions
/// before calling the function, so a subquery argument has the position range {0, 0}.
#[inline]
fn arg_pos(args: &[Expr], i: usize) -> AnnoPos {
    match &args[i] {
        Expr::Subquery(_) => AnnoPos::new(0, 0),
        e => pr(e.position_range()),
    }
}

/// Go `args.PositionRange()` (`Expressions.PositionRange`).
fn args_pos(args: &[Expr]) -> AnnoPos {
    if args.is_empty() {
        return AnnoPos::new(-1, -1);
    }
    AnnoPos::new(arg_pos(args, 0).start, arg_pos(args, args.len() - 1).end)
}

/// Go `getMetricName`.
#[inline]
fn get_metric_name(metric: &Labels) -> &str {
    metric.get(METRIC_NAME)
}

/// Go `metric.DropReserved(schema.IsMetadataLabel)`.
#[inline]
fn drop_metadata(l: &Labels) -> Labels {
    l.drop_reserved(schema::is_metadata_label)
}

/// Go `durationMilliseconds` for nanoseconds.
#[inline]
fn dur_ms(ns: i64) -> i64 {
    ns / 1_000_000
}

/// Go `time.Duration.Seconds()`.
#[inline]
fn dur_seconds(ns: i64) -> f64 {
    let sec = ns / 1_000_000_000;
    let nsec = ns % 1_000_000_000;
    sec as f64 + nsec as f64 / 1e9
}

/// Go `stOrDefault`.
#[inline]
fn st_or_default(sts: &[i64], idx: usize, def: i64) -> i64 {
    if idx < sts.len() { sts[idx] } else { def }
}

/// Go `stringFromArg`: the value of a string literal argument.
fn string_from_arg(e: &Expr) -> Result<&str, Error> {
    match e {
        Expr::StringLiteral(s) => Ok(&s.val),
        other => Err(Error::msg(format!(
            "unexpected error: interface conversion: parser.Expr is {}, not *parser.StringLiteral",
            crate::durations::go_type_name(other)
        ))),
    }
}

/// Whether a histogram error is `histogram.ErrHistogramsIncompatibleSchema`.
fn is_incompatible_schema(e: &Error) -> bool {
    matches!(e, Error::Chunk(c) if c.is_histogram(ErrorKind::HistogramsIncompatibleSchema))
}

/// Go `fmt.Sprintf("%f", f)`.
fn go_fmt_f(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f > 0.0 { "+Inf".to_string() } else { "-Inf".to_string() }
    } else {
        format!("{:.6}", f)
    }
}

#[inline]
fn push_float(enh: &mut EvalNodeHelper, f: f64) {
    enh.out.push(Sample { f, ..Default::default() });
}

#[inline]
fn push_hist(enh: &mut EvalNodeHelper, h: FloatHistogram) {
    enh.out.push(Sample { h: Some(Arc::new(h)), ..Default::default() });
}

thread_local! {
    /// A reusable buffer of float values (quantile_over_time, mad_over_time).
    static FLOAT_BUF: RefCell<Vec<f64>> = const { RefCell::new(Vec::new()) };
}

// ---------------------------------------------------------------------------------------------------------------
// time, interpolation and rates.

/// === time() float64 ===
fn func_time(_: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    let f = enh.ts as f64 / 1000.0;
    push_float(enh, f);
    Ok(Annotations::new())
}

/// Go `pickOrInterpolateLeft`: the value at the left boundary of the range, and its start timestamp.
#[doc(hidden)]
pub fn pick_or_interpolate_left(
    floats: &[FPoint],
    start_timestamps: &[i64],
    first: usize,
    range_start: i64,
    lookback_start: i64,
    smoothed: bool,
    is_counter: bool,
) -> (f64, i64) {
    let mut first_dp = floats[first];
    let first_st = st_or_default(start_timestamps, first, 0);
    let mut returned_st = first_st;
    if returned_st > range_start {
        returned_st = 0;
    }

    if first_dp.t == range_start {
        return (first_dp.f, first_st);
    }

    if first_dp.t > range_start {
        // 'first' is meant to be the index of the most recent sample with T <= rangeStart. If that is not the
        // case, then there are no other samples before it.
        if !is_counter {
            return (first_dp.f, returned_st);
        }
        if !is_start_timestamp_reset_after(lookback_start, first_st, first_dp.t) {
            // Check if there's an ST reset in the visible lookback.
            return (first_dp.f, returned_st);
        }
        if is_start_timestamp_reset_after(range_start, first_st, first_dp.t) {
            // Check if there's an ST reset since the range start.
            return (0.0, returned_st);
        }
        if smoothed {
            return (interpolate(FPoint { t: first_st, f: 0.0 }, first_dp, range_start, is_counter), returned_st);
        }
        return (0.0, returned_st);
    }

    let mut next_dp = floats[first + 1];
    let next_st = st_or_default(start_timestamps, first + 1, 0);

    if !is_counter {
        // No adjustments.
    } else if is_start_timestamp_reset_after(range_start, next_st, next_dp.t) {
        // If there is an ST reset with 'nextST' > 'rangeStart', we adjust nextDP.
        next_dp = FPoint { t: next_st, f: 0.0 };
    } else if is_start_timestamp_reset(first_st, first_dp.t, next_st, next_dp.t) {
        // Otherwise if there is an ST reset in '(firstDP.T, rangeStart]', we adjust firstDP.
        first_dp = FPoint { t: next_st, f: 0.0 };
        returned_st = next_st;
    }

    if smoothed {
        return (interpolate(first_dp, next_dp, range_start, is_counter), returned_st);
    }
    (first_dp.f, returned_st)
}

/// Go `pickOrInterpolateRight`: the value at the right boundary of the range, and its start timestamp.
#[doc(hidden)]
pub fn pick_or_interpolate_right(
    floats: &[FPoint],
    start_timestamps: &[i64],
    last: usize,
    range_end: i64,
    lookback_start: i64,
    smoothed: bool,
    is_counter: bool,
) -> (f64, i64) {
    let mut last_dp = floats[last];
    let last_st = st_or_default(start_timestamps, last, 0);
    let mut returned_st = last_st;
    if returned_st > range_end {
        returned_st = 0;
    }

    if last_dp.t <= range_end {
        return (last_dp.f, returned_st);
    }

    if last == 0 {
        if !is_counter {
            return (last_dp.f, returned_st);
        }
        if !is_start_timestamp_reset_after(lookback_start, last_st, last_dp.t) {
            // Check if there's an ST reset in the visible lookback.
            return (last_dp.f, returned_st);
        }
        if is_start_timestamp_reset_after(range_end, last_st, last_dp.t) {
            // Check if there's an ST reset since the range end.
            return (0.0, returned_st);
        }
        if smoothed {
            return (interpolate(FPoint { t: last_st, f: 0.0 }, last_dp, range_end, is_counter), returned_st);
        }
        return (0.0, returned_st);
    }

    let mut prev_dp = floats[last - 1];
    let prev_st = st_or_default(start_timestamps, last - 1, 0);

    if !is_counter {
        // No adjustments.
    } else if is_start_timestamp_reset_after(range_end, last_st, last_dp.t) {
        // If there is an ST reset with 'lastST' > 'rangeEnd', we adjust lastDP.
        last_dp = FPoint { t: last_st, f: 0.0 };
        returned_st = prev_st;
    } else if is_start_timestamp_reset(prev_st, prev_dp.t, last_st, last_dp.t) {
        // Otherwise if there is an ST reset in '(prevDP.T, rangeEnd]', we adjust prevDP.
        prev_dp = FPoint { t: last_st, f: 0.0 };
    }

    if smoothed {
        return (interpolate(prev_dp, last_dp, range_end, is_counter), returned_st);
    }
    (prev_dp.f, returned_st)
}

/// Go `interpolate`: linear interpolation between two points. With `is_counter` and a counter reset, the counter
/// is modeled as starting from 0.
pub fn interpolate(p1: FPoint, p2: FPoint, t: i64, is_counter: bool) -> f64 {
    let mut y1 = p1.f;
    let y2 = p2.f;
    if is_counter && y2 < y1 {
        y1 = 0.0;
    }
    y1 + (y2 - y1) * (t - p1.t) as f64 / (p2.t - p1.t) as f64
}

/// Go `interpolateHistograms`: linear interpolation between two histogram points.
#[allow(clippy::too_many_arguments)]
pub fn interpolate_histograms(
    h1: &FloatHistogram,
    t1: i64,
    h2: &FloatHistogram,
    t2: i64,
    t: i64,
    is_counter: bool,
    annos: &mut Annotations,
    pos: AnnoPos,
) -> Result<FloatHistogram, Error> {
    if t == t1 {
        return Ok(h1.copy());
    }
    if t == t2 {
        return Ok(h2.copy());
    }
    let fraction = (t - t1) as f64 / (t2 - t1) as f64;

    if is_counter && h2.detect_reset(h1) {
        // Counter reset: model as starting from zero.
        let mut r = h2.copy();
        r.mul(fraction);
        return Ok(r);
    }

    // Result = H1 + (H2 - H1) * fraction.
    let mut result = h2.copy();
    let (_, nhcb_reconciled) = result.sub(h1)?;
    if nhcb_reconciled {
        annos.add(annos::new_mismatched_custom_buckets_histograms_info(pos, HistogramOperation::Sub));
    }
    result.mul(fraction);
    let (_, nhcb_reconciled) = result.add(h1)?;
    if nhcb_reconciled {
        annos.add(annos::new_mismatched_custom_buckets_histograms_info(pos, HistogramOperation::Add));
    }
    Ok(result)
}

/// Go `pickOrInterpolateLeftHistogram`.
fn pick_or_interpolate_left_histogram(
    hists: &[HPoint],
    first: usize,
    range_start: i64,
    smoothed: bool,
    is_counter: bool,
    annos: &mut Annotations,
    pos: AnnoPos,
) -> Result<FloatHistogram, Error> {
    if smoothed && hists[first].t < range_start {
        return interpolate_histograms(
            &hists[first].h,
            hists[first].t,
            &hists[first + 1].h,
            hists[first + 1].t,
            range_start,
            is_counter,
            annos,
            pos,
        );
    }
    Ok(hists[first].h.copy())
}

/// Go `pickOrInterpolateRightHistogram`.
fn pick_or_interpolate_right_histogram(
    hists: &[HPoint],
    last: usize,
    range_end: i64,
    smoothed: bool,
    is_counter: bool,
    annos: &mut Annotations,
    pos: AnnoPos,
) -> Result<FloatHistogram, Error> {
    if smoothed && last > 0 && hists[last].t > range_end {
        return interpolate_histograms(
            &hists[last - 1].h,
            hists[last - 1].t,
            &hists[last].h,
            hists[last].t,
            range_end,
            is_counter,
            annos,
            pos,
        );
    }
    Ok(hists[last].h.copy())
}

/// Go `correctForCounterResets`: the correction for counter resets (smoothed or anchored rates).
#[allow(clippy::too_many_arguments)]
fn correct_for_counter_resets(
    left: f64,
    right: f64,
    left_t: i64,
    right_t: i64,
    left_st: i64,
    right_st: i64,
    points: &[FPoint],
    start_timestamps: &[i64],
) -> f64 {
    let mut correction = 0.0;
    let (mut prev, mut prev_t, mut prev_st) = (left, left_t, left_st);
    for (i, p) in points.iter().enumerate() {
        let st = st_or_default(start_timestamps, i, 0);
        if p.f < prev || is_start_timestamp_reset(prev_st, prev_t, st, p.t) {
            correction += prev;
        }
        prev = p.f;
        prev_t = p.t;
        prev_st = st;
    }
    if right < prev || is_start_timestamp_reset(prev_st, prev_t, right_st, right_t) {
        correction += prev;
    }
    correction
}

/// Go `annosFromInterpolationError`: adds the annotation for an error of `interpolate_histograms`; any other
/// error than an incompatible schema fails the query (Go panics), so it is returned.
pub fn annos_from_interpolation_error(
    annos: &mut Annotations,
    err: Error,
    metric_name: &str,
    pos: AnnoPos,
) -> Result<(), Error> {
    if is_incompatible_schema(&err) {
        annos.add(annos::new_mixed_exponential_custom_histograms_warning(metric_name, pos));
        return Ok(());
    }
    // Any other error means a histogram has an invalid bucket layout. Fail the query.
    Err(err)
}

/// Go `addHistogramWithAnnotations`: adds `other` into `base`; false if the operation failed.
fn add_histogram_with_annotations(
    base: &mut FloatHistogram,
    other: &FloatHistogram,
    annos: &mut Annotations,
    metric_name: &str,
    pos: AnnoPos,
) -> Result<bool, Error> {
    match base.add(other) {
        Err(e) => {
            let e = Error::from(e);
            if is_incompatible_schema(&e) {
                annos.add(annos::new_mixed_exponential_custom_histograms_warning(metric_name, pos));
                return Ok(false);
            }
            Err(e)
        }
        Ok((_, nhcb_reconciled)) => {
            if nhcb_reconciled {
                annos.add(annos::new_mismatched_custom_buckets_histograms_info(pos, HistogramOperation::Add));
            }
            Ok(true)
        }
    }
}

/// Go `subHistogramWithAnnotations`: subtracts `other` from `base`; false if the operation failed.
fn sub_histogram_with_annotations(
    base: &mut FloatHistogram,
    other: &FloatHistogram,
    annos: &mut Annotations,
    metric_name: &str,
    pos: AnnoPos,
) -> Result<bool, Error> {
    match base.sub(other) {
        Err(e) => {
            let e = Error::from(e);
            if is_incompatible_schema(&e) {
                annos.add(annos::new_mixed_exponential_custom_histograms_warning(metric_name, pos));
                return Ok(false);
            }
            Err(e)
        }
        Ok((_, nhcb_reconciled)) => {
            if nhcb_reconciled {
                annos.add(annos::new_mismatched_custom_buckets_histograms_info(pos, HistogramOperation::Sub));
            }
            Ok(true)
        }
    }
}

/// Go `validateHistogramRange`.
fn validate_histogram_range(
    h: &[HPoint],
    is_counter: bool,
    annos: &mut Annotations,
    metric_name: &str,
    pos: AnnoPos,
) -> bool {
    let using_custom_buckets = h[0].h.uses_custom_buckets();
    for p in h {
        if p.h.uses_custom_buckets() != using_custom_buckets {
            annos.add(annos::new_mixed_exponential_custom_histograms_warning(metric_name, pos));
            return false;
        }
        if is_counter && p.h.counter_reset_hint == CounterResetHint::GaugeType {
            annos.add(annos::new_native_histogram_not_counter_warning(metric_name, pos));
        }
    }
    true
}

/// Go `correctForCounterResetsHistogram`.
#[allow(clippy::too_many_arguments)]
fn correct_for_counter_resets_histogram(
    h: &[HPoint],
    first_sample_index: usize,
    last_sample_index: usize,
    left: &FloatHistogram,
    right: &FloatHistogram,
    range_start: i64,
    smoothed: bool,
    metric_name: &str,
    pos: AnnoPos,
) -> Result<(Option<FloatHistogram>, Annotations, bool), Error> {
    // firstSampleIndex is represented by left, so the loop starts one beyond.
    let mut first = first_sample_index + 1;
    let mut prev: &FloatHistogram = left;
    if smoothed
        && h[first_sample_index].t < range_start
        && h[first_sample_index + 1].h.detect_reset(&h[first_sample_index].h)
    {
        // The left interpolation spanned the reset between h[firstSampleIndex] and h[firstSampleIndex+1].
        prev = &h[first_sample_index + 1].h;
        first += 1;
    }
    // lastSampleIndex is always excluded: right is either a direct copy of h[lastSampleIndex] or an
    // interpolation that inherits its CounterResetHint.
    let last = last_sample_index as isize - 1;

    if first as isize > last + 1 {
        return Ok((None, Annotations::new(), true));
    }

    let mut correction: Option<FloatHistogram> = None;
    let mut annos = Annotations::new();

    fn add_correction(
        correction: &mut Option<FloatHistogram>,
        h: &FloatHistogram,
        annos: &mut Annotations,
        metric_name: &str,
        pos: AnnoPos,
    ) -> Result<bool, Error> {
        match correction {
            None => {
                *correction = Some(h.copy());
                Ok(true)
            }
            Some(c) => add_histogram_with_annotations(c, h, annos, metric_name, pos),
        }
    }

    for p in &h[first..(last + 1) as usize] {
        if p.h.detect_reset(prev) && !add_correction(&mut correction, prev, &mut annos, metric_name, pos)? {
            return Ok((None, annos, false));
        }
        prev = &p.h;
    }
    if right.detect_reset(prev) && !add_correction(&mut correction, prev, &mut annos, metric_name, pos)? {
        return Ok((None, annos, false));
    }
    Ok((correction, annos, true))
}

/// Go `extendedRate`: anchored/smoothed rate/increase/delta of floats.
fn extended_rate(
    samples: &Series,
    _args: &[Expr],
    enh: &mut EvalNodeHelper,
    is_counter: bool,
    is_rate: bool,
) -> Result<Annotations, Error> {
    let f = &samples.floats;
    let mut last_sample_index = f.len() as isize - 1;
    let range_start = enh.ts - dur_ms(enh.range + enh.offset);
    let range_end = enh.ts - dur_ms(enh.offset);
    let lookback_start = range_start - dur_ms(enh.lookback_delta);
    let annos = Annotations::new();
    let smoothed = enh.smoothed;
    let start_timestamps: &[i64] = match &enh.start_timestamps {
        Some(sts) => &sts.floats,
        None => &[],
    };

    let n = last_sample_index as usize;
    let mut first_sample_index = (gosort::search(n, |i| f[i].t > range_start) as isize - 1).max(0);
    if smoothed {
        last_sample_index = gosort::search(n, |i| f[i].t >= range_end) as isize;
    }

    if f[last_sample_index as usize].t <= range_start {
        return Ok(annos);
    }
    let first_t = f[first_sample_index as usize].t;
    if smoothed && first_t > range_end {
        let first_st = st_or_default(start_timestamps, first_sample_index as usize, 0);
        if !is_counter
            || !is_start_timestamp_reset_after(lookback_start, first_st, first_t)
            || is_start_timestamp_reset_after(range_end, first_st, first_t)
        {
            // We cannot calculate a rate if there's no ST reset inside the lookback range, or if the reset is
            // after the range end.
            return Ok(annos);
        }
    }

    let (left, left_st) = pick_or_interpolate_left(
        f,
        start_timestamps,
        first_sample_index as usize,
        range_start,
        lookback_start,
        smoothed,
        is_counter,
    );
    let (right, right_st) = pick_or_interpolate_right(
        f,
        start_timestamps,
        last_sample_index as usize,
        range_end,
        lookback_start,
        smoothed,
        is_counter,
    );

    let mut result_float = right - left;

    if is_counter {
        // We only need to consider samples exactly within the range for counter resets correction.
        if f[first_sample_index as usize].t <= range_start {
            first_sample_index += 1;
        }
        if f[last_sample_index as usize].t >= range_end {
            last_sample_index -= 1;
        }
        let st_len = start_timestamps.len();
        let lo = st_len.min(first_sample_index as usize);
        let hi = st_len.min((last_sample_index + 1) as usize).max(lo);
        let sts = &start_timestamps[lo..hi];
        let fhi = ((last_sample_index + 1) as usize).max(first_sample_index as usize);
        result_float += correct_for_counter_resets(
            left,
            right,
            range_start,
            range_end,
            left_st,
            right_st,
            &f[first_sample_index as usize..fhi],
            sts,
        );
    }
    if is_rate {
        result_float /= dur_seconds(enh.range);
    }
    push_float(enh, result_float);
    Ok(annos)
}

/// Go `extendedHistogramRate`: anchored/smoothed rate/increase/delta of native histograms.
fn extended_histogram_rate(
    samples: &Series,
    args: &[Expr],
    enh: &mut EvalNodeHelper,
    is_counter: bool,
    is_rate: bool,
) -> Result<Annotations, Error> {
    let h = &samples.histograms;
    let mut last_sample_index = h.len() - 1;
    let range_start = enh.ts - dur_ms(enh.range + enh.offset);
    let range_end = enh.ts - dur_ms(enh.offset);
    let mut annos = Annotations::new();
    let metric_name = get_metric_name(&samples.metric);
    let pos = arg_pos(args, 0);
    let smoothed = enh.smoothed;

    let n = last_sample_index;
    let first_sample_index = (gosort::search(n, |i| h[i].t > range_start) as isize - 1).max(0) as usize;
    if smoothed {
        last_sample_index = gosort::search(n, |i| h[i].t >= range_end);
    }

    if h[last_sample_index].t <= range_start {
        return Ok(annos);
    }
    if smoothed && h[first_sample_index].t > range_end {
        return Ok(annos);
    }

    if !validate_histogram_range(&h[first_sample_index..last_sample_index + 1], is_counter, &mut annos, metric_name, pos) {
        return Ok(annos);
    }

    let left =
        match pick_or_interpolate_left_histogram(h, first_sample_index, range_start, smoothed, is_counter, &mut annos, pos)
        {
            Ok(l) => l,
            Err(e) => {
                annos_from_interpolation_error(&mut annos, e, metric_name, pos)?;
                return Ok(annos);
            }
        };
    let right =
        match pick_or_interpolate_right_histogram(h, last_sample_index, range_end, smoothed, is_counter, &mut annos, pos) {
            Ok(r) => r,
            Err(e) => {
                annos_from_interpolation_error(&mut annos, e, metric_name, pos)?;
                return Ok(annos);
            }
        };

    if !is_counter
        && (left.counter_reset_hint != CounterResetHint::GaugeType
            || right.counter_reset_hint != CounterResetHint::GaugeType)
    {
        annos.add(annos::new_native_histogram_not_gauge_warning(metric_name, pos));
    }

    // Copy right so that correctForCounterResetsHistogram can still call right.DetectReset without observing
    // mutations from subHistogramWithAnnotations.
    let mut result = right.copy();
    if !sub_histogram_with_annotations(&mut result, &left, &mut annos, metric_name, pos)? {
        return Ok(annos);
    }

    if is_counter {
        let (correction, new_annos, ok) = correct_for_counter_resets_histogram(
            h,
            first_sample_index,
            last_sample_index,
            &left,
            &right,
            range_start,
            smoothed,
            metric_name,
            pos,
        )?;
        annos.merge(new_annos);
        if !ok {
            return Ok(annos);
        }
        if let Some(c) = correction {
            if !add_histogram_with_annotations(&mut result, &c, &mut annos, metric_name, pos)? {
                return Ok(annos);
            }
        }
    }
    if is_rate {
        result.div(dur_seconds(enh.range));
    }
    result.counter_reset_hint = CounterResetHint::GaugeType;
    result.compact(0);
    push_hist(enh, result);
    Ok(annos)
}

/// Go `extrapolatedRate`: rate/increase/delta with extrapolation to the range boundaries.
fn extrapolated_rate(
    matrix: &[Series],
    args: &[Expr],
    enh: &mut EvalNodeHelper,
    is_counter: bool,
    is_rate: bool,
) -> Result<Annotations, Error> {
    let samples = &matrix[0];
    let mut annos = Annotations::new();
    if enh.anchored || enh.smoothed {
        if !samples.histograms.is_empty() && !samples.floats.is_empty() {
            annos.add(annos::new_mixed_floats_histograms_warning(get_metric_name(&samples.metric), arg_pos(args, 0)));
            return Ok(annos);
        }
        if !samples.histograms.is_empty() {
            return extended_histogram_rate(samples, args, enh, is_counter, is_rate);
        }
        if !samples.floats.is_empty() {
            return extended_rate(samples, args, enh, is_counter, is_rate);
        }
        // Not enough samples.
        return Ok(annos);
    }

    let range_start = enh.ts - dur_ms(enh.range + enh.offset);
    let range_end = enh.ts - dur_ms(enh.offset);
    let mut result_float = 0.0;
    let mut result_histogram: Option<FloatHistogram> = None;
    let (first_t, last_t);
    let num_samples_minus_one;
    let mut start_timestamps: &[i64] = &[];

    // Drop this vector element and return a warning if this rate window contains mixed float and histogram
    // samples.
    if !samples.histograms.is_empty() && !samples.floats.is_empty() {
        annos.add(annos::new_mixed_floats_histograms_warning(get_metric_name(&samples.metric), arg_pos(args, 0)));
        return Ok(annos);
    }

    if !samples.histograms.is_empty() {
        num_samples_minus_one = samples.histograms.len() - 1;
        first_t = samples.histograms[0].t;
        last_t = samples.histograms[num_samples_minus_one].t;
        if let Some(sts) = &enh.start_timestamps {
            start_timestamps = &sts.histograms;
        }
        if samples.histograms.len() > 1 {
            let (h, new_annos) =
                histogram_rate(&samples.histograms, start_timestamps, is_counter, &samples.metric, arg_pos(args, 0))?;
            annos.merge(new_annos);
            match h {
                None => return Ok(annos), // The histograms are not compatible with each other.
                Some(h) => result_histogram = Some(h),
            }
        }
    } else if !samples.floats.is_empty() {
        let floats = &samples.floats;
        num_samples_minus_one = floats.len() - 1;
        first_t = floats[0].t;
        last_t = floats[num_samples_minus_one].t;
        result_float = floats[num_samples_minus_one].f - floats[0].f;
        if is_counter {
            // Handle counter resets.
            if let Some(sts) = &enh.start_timestamps {
                start_timestamps = &sts.floats;
            }
            let mut overlap_detected = false;
            for i in 0..num_samples_minus_one {
                let prev_point = floats[i];
                let curr_point = floats[i + 1];
                // Warn once per series if start timestamp overlap detected.
                if !overlap_detected
                    && i + 1 < start_timestamps.len()
                    && check_start_time_overlap(start_timestamps[i], prev_point.t, start_timestamps[i + 1])
                {
                    annos.add(annos::new_start_time_overlap_warning(
                        get_metric_name(&samples.metric),
                        arg_pos(args, 0),
                    ));
                    overlap_detected = true;
                }
                if curr_point.f < prev_point.f
                    || (i + 1 < start_timestamps.len()
                        && is_start_timestamp_reset(
                            start_timestamps[i],
                            prev_point.t,
                            start_timestamps[i + 1],
                            curr_point.t,
                        ))
                {
                    result_float += prev_point.f;
                }
            }
        }
    } else {
        // TODO: add RangeTooShortWarning
        return Ok(annos);
    }

    // Duration between first/last samples and boundary of range.
    let mut duration_to_start = (first_t - range_start) as f64 / 1000.0;
    let mut duration_to_end = (range_end - last_t) as f64 / 1000.0;

    let mut sampled_interval = (last_t - first_t) as f64 / 1000.0;
    let mut average_duration_between_samples = 0.0;
    if num_samples_minus_one > 0 {
        average_duration_between_samples = sampled_interval / num_samples_minus_one as f64;
    }
    let extrapolation_threshold = average_duration_between_samples * 1.1;

    let sts = start_timestamps;
    if is_counter && !sts.is_empty() && sts[0] != 0 && sts[0] > range_start && sts[0] < first_t {
        // The ST of the first sample points inside the range: assume a zero-value sample at the time of ST.
        duration_to_start = 0.0;
        sampled_interval = (last_t - sts[0]) as f64 / 1000.0;
        if !samples.floats.is_empty() {
            result_float += samples.floats[0].f;
        } else if !samples.histograms.is_empty() {
            match &mut result_histogram {
                None => result_histogram = Some(samples.histograms[0].h.copy()),
                Some(rh) => {
                    if !add_histogram_with_annotations(
                        rh,
                        &samples.histograms[0].h,
                        &mut annos,
                        get_metric_name(&samples.metric),
                        arg_pos(args, 0),
                    )? {
                        return Ok(annos);
                    }
                }
            }
        }
    } else if num_samples_minus_one == 0 {
        // There's a single sample, and we do not have suitable ST to calculate the increase. Return nothing.
        return Ok(annos);
    } else {
        // If samples are close enough to the (lower or upper) boundary of the range, we extrapolate the rate all
        // the way to the boundary in question.
        if duration_to_start >= extrapolation_threshold {
            duration_to_start = average_duration_between_samples / 2.0;
        }
        if is_counter {
            // Counters cannot be negative. If the duration to the zero point is shorter than the
            // durationToStart, we take the zero point as the start of the series.
            let mut duration_to_zero = duration_to_start;
            if result_float > 0.0 && !samples.floats.is_empty() && samples.floats[0].f >= 0.0 {
                duration_to_zero = sampled_interval * (samples.floats[0].f / result_float);
            } else if let Some(rh) = &result_histogram {
                if rh.count > 0.0 && !samples.histograms.is_empty() && samples.histograms[0].h.count >= 0.0 {
                    duration_to_zero = sampled_interval * (samples.histograms[0].h.count / rh.count);
                }
            }
            if duration_to_zero < duration_to_start {
                duration_to_start = duration_to_zero;
            }
        }
    }

    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_duration_between_samples / 2.0;
    }

    let mut factor = 1.0;
    if sampled_interval != 0.0 {
        factor = (sampled_interval + duration_to_start + duration_to_end) / sampled_interval;
    }
    if is_rate {
        factor /= dur_seconds(enh.range);
    }
    match result_histogram {
        None => {
            result_float *= factor;
            push_float(enh, result_float);
        }
        Some(mut h) => {
            h.mul(factor);
            enh.out.push(Sample { f: result_float, h: Some(Arc::new(h)), ..Default::default() });
        }
    }
    Ok(annos)
}

/// Go `histogramRate`: requires at least two points. Returns `None` with a warning if the histograms are not
/// compatible.
#[doc(hidden)]
pub fn histogram_rate(
    points: &[HPoint],
    start_timestamps: &[i64],
    is_counter: bool,
    labels: &Labels,
    pos: AnnoPos,
) -> Result<(Option<FloatHistogram>, Annotations), Error> {
    let zero;
    let mut prev: &FloatHistogram = &points[0].h;
    let mut using_custom_buckets = prev.uses_custom_buckets();
    let last: &FloatHistogram = &points[points.len() - 1].h;
    let mut annos = Annotations::new();

    // We check for gauge type histograms in the loop below, but the loop below does not run on the first and last
    // point, so check the first and last point now.
    if is_counter
        && (prev.counter_reset_hint == CounterResetHint::GaugeType
            || last.counter_reset_hint == CounterResetHint::GaugeType)
    {
        annos.add(annos::new_native_histogram_not_counter_warning(get_metric_name(labels), pos));
    }

    // Null out the 1st sample if there is a counter reset between the 1st and 2nd.
    if is_counter {
        let second: &FloatHistogram = &points[1].h;
        if (start_timestamps.len() > 1
            && is_start_timestamp_reset(start_timestamps[0], points[0].t, start_timestamps[1], points[1].t))
            || second.detect_reset(prev)
        {
            zero = FloatHistogram { schema: second.schema, custom_values: second.custom_values.clone(), ..Default::default() };
            prev = &zero;
            using_custom_buckets = second.uses_custom_buckets();
        }
    }

    if last.uses_custom_buckets() != using_custom_buckets {
        annos.add(annos::new_mixed_exponential_custom_histograms_warning(get_metric_name(labels), pos));
        return Ok((None, annos));
    }

    // First iteration to find out the smallest relevant schema.
    let mut min_schema = last.schema.min(prev.schema);
    for curr_point in &points[1..points.len() - 1] {
        let curr = &curr_point.h;
        if !is_counter {
            continue;
        }
        if curr.counter_reset_hint == CounterResetHint::GaugeType {
            annos.add(annos::new_native_histogram_not_counter_warning(get_metric_name(labels), pos));
        }
        if curr.schema < min_schema {
            min_schema = curr.schema;
        }
        if curr.uses_custom_buckets() != using_custom_buckets {
            return Ok((
                None,
                Annotations::new().with(annos::new_mixed_exponential_custom_histograms_warning(
                    get_metric_name(labels),
                    pos,
                )),
            ));
        }
    }

    // An invalid bucket layout fails the query, as the result would otherwise be wrong.
    let mut h = last.copy_to_schema(min_schema)?;
    // This subtraction may deliberately include conflicting counter resets.
    match h.sub(prev) {
        Err(e) => {
            let e = Error::from(e);
            if is_incompatible_schema(&e) {
                return Ok((
                    None,
                    Annotations::new().with(annos::new_mixed_exponential_custom_histograms_warning(
                        get_metric_name(labels),
                        pos,
                    )),
                ));
            }
            return Err(e);
        }
        Ok((_, nhcb_reconciled)) => {
            if nhcb_reconciled {
                annos.add(annos::new_mismatched_custom_buckets_histograms_info(pos, HistogramOperation::Sub));
            }
        }
    }

    if is_counter {
        // Second iteration to deal with counter resets.
        let mut overlap_detected = false;
        for i in 0..points.len() - 1 {
            let curr_point = &points[i + 1];
            let curr: &FloatHistogram = &curr_point.h;
            // Warn once per series if start timestamp overlap detected.
            if !overlap_detected
                && i + 1 < start_timestamps.len()
                && check_start_time_overlap(start_timestamps[i], points[i].t, start_timestamps[i + 1])
            {
                annos.add(annos::new_start_time_overlap_warning(get_metric_name(labels), pos));
                overlap_detected = true;
            }
            // Check start timestamps first since it's potentially cheaper.
            if (i + 1 < start_timestamps.len()
                && is_start_timestamp_reset(start_timestamps[i], points[i].t, start_timestamps[i + 1], curr_point.t))
                || curr.detect_reset(prev)
            {
                match h.add(prev) {
                    Err(e) => {
                        let e = Error::from(e);
                        if is_incompatible_schema(&e) {
                            return Ok((
                                None,
                                Annotations::new().with(annos::new_mixed_exponential_custom_histograms_warning(
                                    get_metric_name(labels),
                                    pos,
                                )),
                            ));
                        }
                        return Err(e);
                    }
                    Ok((_, nhcb_reconciled)) => {
                        if nhcb_reconciled {
                            annos.add(annos::new_mismatched_custom_buckets_histograms_info(
                                pos,
                                HistogramOperation::Add,
                            ));
                        }
                    }
                }
            }
            prev = curr;
        }
    } else if points[0].h.counter_reset_hint != CounterResetHint::GaugeType
        || points[points.len() - 1].h.counter_reset_hint != CounterResetHint::GaugeType
    {
        annos.add(annos::new_native_histogram_not_gauge_warning(get_metric_name(labels), pos));
    }

    h.counter_reset_hint = CounterResetHint::GaugeType;
    h.compact(0);
    Ok((Some(h), annos))
}

/// Go `isStartTimestampReset`: whether there was a counter reset according to the start timestamps.
#[doc(hidden)]
pub fn is_start_timestamp_reset(
    prev_start_timestamp: i64,
    prev_timestamp: i64,
    curr_start_timestamp: i64,
    curr_timestamp: i64,
) -> bool {
    if prev_start_timestamp == curr_start_timestamp || curr_start_timestamp == 0 || curr_start_timestamp >= curr_timestamp
    {
        // No reset if start timestamp hasn't changed, if it is not set, if it is clearly invalid (ST > T), or if
        // it is OTel's unknown start time (ST == T).
        return false;
    }
    if curr_start_timestamp < prev_timestamp {
        return false;
    }
    if curr_start_timestamp > prev_timestamp {
        return true;
    }
    // The current start timestamp points to a previous datapoint: a reset for deltas, not for cumulative series
    // with unknown start timestamp. A previous start timestamp greater than the previous sample timestamp is
    // invalid; treat it as unknown to avoid a spurious reset.
    if prev_start_timestamp > prev_timestamp {
        return false;
    }
    prev_start_timestamp != 0 && prev_start_timestamp != prev_timestamp
}

/// Go `isStartTimestampResetAfter`: whether there is a start timestamp reset after time `t`.
#[doc(hidden)]
pub fn is_start_timestamp_reset_after(t: i64, start_timestamp: i64, timestamp: i64) -> bool {
    if start_timestamp <= t || timestamp <= t {
        return false;
    }
    is_start_timestamp_reset(t, t, start_timestamp, timestamp)
}

/// Go `checkStartTimeOverlap`.
#[doc(hidden)]
pub fn check_start_time_overlap(prev_start_timestamp: i64, prev_timestamp: i64, curr_start_timestamp: i64) -> bool {
    curr_start_timestamp != 0 && curr_start_timestamp < prev_timestamp && curr_start_timestamp != prev_start_timestamp
}

/// === delta(Matrix parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_delta(_: &[Vector], m: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    extrapolated_rate(m, args, enh, false, false)
}

/// === rate(node parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_rate(_: &[Vector], m: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    extrapolated_rate(m, args, enh, true, true)
}

/// === increase(node parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_increase(_: &[Vector], m: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    extrapolated_rate(m, args, enh, true, false)
}

/// === irate(node parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_irate(_: &[Vector], m: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    instant_value(m, args, enh, true)
}

/// === idelta(node model.ValMatrix) (Vector, Annotations) ===
fn func_idelta(_: &[Vector], m: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    instant_value(m, args, enh, false)
}

#[derive(Clone, Copy)]
struct SampleWithSt<'a> {
    t: i64,
    f: f64,
    h: Option<&'a Arc<FloatHistogram>>,
    st: i64,
}

/// Go `instantValue`: irate and idelta.
fn instant_value(vals: &[Series], args: &[Expr], enh: &mut EvalNodeHelper, is_rate: bool) -> Result<Annotations, Error> {
    let samples = &vals[0];
    let mut annos = Annotations::new();

    // No sense in trying to compute a rate without at least two points.
    if samples.floats.len() + samples.histograms.len() < 2 {
        return Ok(annos);
    }

    let empty = SampleWithSt { t: 0, f: 0.0, h: None, st: 0 };
    let mut ss = [empty; 2];
    let mut n = 0usize;
    let sts = enh.start_timestamps.as_ref();

    // Add the last 2 float samples if they exist.
    for i in samples.floats.len().saturating_sub(2)..samples.floats.len() {
        let mut s = SampleWithSt { t: samples.floats[i].t, f: samples.floats[i].f, h: None, st: 0 };
        if let Some(sts) = sts {
            if i < sts.floats.len() {
                s.st = sts.floats[i];
            }
        }
        ss[n] = s;
        n += 1;
    }

    // Add the last 2 histogram samples into their correct position if they exist.
    for i in samples.histograms.len().saturating_sub(2)..samples.histograms.len() {
        let mut s = SampleWithSt { t: samples.histograms[i].t, f: 0.0, h: Some(&samples.histograms[i].h), st: 0 };
        if let Some(sts) = sts {
            if i < sts.histograms.len() {
                s.st = sts.histograms[i];
            }
        }
        if n == 0 {
            ss[0] = s;
            n = 1;
        } else if n == 1 {
            if s.t < ss[0].t {
                ss[1] = ss[0];
                ss[0] = s;
            } else {
                ss[1] = s;
            }
            n = 2;
        } else if s.t < ss[0].t {
            // s is older than 1st, so discard it.
        } else if s.t > ss[1].t {
            // s is newest, so add it as 2nd and make the old 2nd the new 1st.
            ss[0] = ss[1];
            ss[1] = s;
        } else {
            // In all other cases, we just make s the new 1st.
            ss[0] = s;
        }
    }

    let sampled_interval = ss[1].t - ss[0].t;
    if sampled_interval == 0 {
        // Avoid dividing by 0.
        return Ok(annos);
    }
    let mut result_f = ss[1].f;
    let mut result_h: Option<FloatHistogram> = None;
    match (ss[0].h, ss[1].h) {
        (None, None) => {
            if !is_rate
                || !(ss[1].f < ss[0].f || is_start_timestamp_reset(ss[0].st, ss[0].t, ss[1].st, ss[1].t))
            {
                // Gauge, or counter without reset, or counter with NaN value.
                result_f = ss[1].f - ss[0].f;
            }
            // In case of a counter reset, we leave the result at its current value, which is already ss[1].
        }
        (Some(h0), Some(h1)) => {
            let mut rh = h1.copy();
            // irate should only be applied to counters.
            if is_rate
                && (h1.counter_reset_hint == CounterResetHint::GaugeType
                    || h0.counter_reset_hint == CounterResetHint::GaugeType)
            {
                annos.add(annos::new_native_histogram_not_counter_warning(
                    get_metric_name(&samples.metric),
                    args_pos(args),
                ));
            }
            // idelta should only be applied to gauges.
            if !is_rate
                && (h1.counter_reset_hint != CounterResetHint::GaugeType
                    || h0.counter_reset_hint != CounterResetHint::GaugeType)
            {
                annos.add(annos::new_native_histogram_not_gauge_warning(get_metric_name(&samples.metric), args_pos(args)));
            }
            if !is_rate || (!is_start_timestamp_reset(ss[0].st, ss[0].t, ss[1].st, ss[1].t) && !h1.detect_reset(h0)) {
                // This subtraction may deliberately include conflicting counter resets.
                match rh.sub(h0) {
                    Err(e) => {
                        let e = Error::from(e);
                        if is_incompatible_schema(&e) {
                            annos.add(annos::new_mixed_exponential_custom_histograms_warning(
                                get_metric_name(&samples.metric),
                                args_pos(args),
                            ));
                            return Ok(annos);
                        }
                        // Any other error means a histogram has an invalid bucket layout. Fail the query.
                        return Err(e);
                    }
                    Ok((_, nhcb_reconciled)) => {
                        if nhcb_reconciled {
                            annos.add(annos::new_mismatched_custom_buckets_histograms_info(
                                args_pos(args),
                                HistogramOperation::Sub,
                            ));
                        }
                    }
                }
            }
            rh.counter_reset_hint = CounterResetHint::GaugeType;
            rh.compact(0);
            result_h = Some(rh);
        }
        _ => {
            // Mix of a float and a histogram.
            annos.add(annos::new_mixed_floats_histograms_warning(get_metric_name(&samples.metric), args_pos(args)));
            return Ok(annos);
        }
    }

    if is_rate {
        // Convert to per-second.
        match &mut result_h {
            None => result_f /= sampled_interval as f64 / 1000.0,
            Some(h) => {
                h.div(sampled_interval as f64 / 1000.0);
            }
        }
    }
    let t = ss[1].t;
    enh.out.push(Sample { t, f: result_f, h: result_h.map(Arc::new), ..Default::default() });
    Ok(annos)
}

/// Go `calcTrendValue`: the trend value at index i (the slope of the trend).
fn calc_trend_value(i: usize, tf: f64, s0: f64, s1: f64, b: f64) -> f64 {
    if i == 0 {
        return b;
    }
    let x = tf * (s1 - s0);
    let y = (1.0 - tf) * b;
    x + y
}

/// Go `funcDoubleExponentialSmoothing`.
fn func_double_exponential_smoothing(
    vals: &[Vector],
    matrix: &[Series],
    args: &[Expr],
    enh: &mut EvalNodeHelper,
) -> Result<Annotations, Error> {
    if vals.len() < 2 || vals[0].is_empty() || vals[1].is_empty() || matrix.is_empty() {
        return Ok(Annotations::new());
    }
    let samples = &matrix[0];
    // The smoothing factor argument.
    let sf = vals[0][0].f;
    // The trend factor argument.
    let tf = vals[1][0].f;

    // Check that the input parameters are valid.
    if sf <= 0.0 || sf >= 1.0 {
        return Err(Error::msg(format!("invalid smoothing factor. Expected: 0 < sf < 1, got: {}", go_fmt_f(sf))));
    }
    if tf <= 0.0 || tf >= 1.0 {
        return Err(Error::msg(format!("invalid trend factor. Expected: 0 < tf < 1, got: {}", go_fmt_f(tf))));
    }

    let l = samples.floats.len();
    // Can't do the smoothing operation with less than two points.
    if l < 2 {
        // Annotate mix of float and histogram.
        if l == 1 && !samples.histograms.is_empty() {
            return Ok(Annotations::new().with(annos::new_histogram_ignored_in_mixed_range_info(
                get_metric_name(&samples.metric),
                arg_pos(args, 0),
            )));
        }
        return Ok(Annotations::new());
    }

    let mut s0 = 0.0;
    let mut s1 = samples.floats[0].f;
    let mut b = samples.floats[1].f - samples.floats[0].f;
    for i in 1..l {
        // Scale the raw value against the smoothing factor.
        let x = sf * samples.floats[i].f;
        // Scale the last smoothed value with the trend at this point.
        b = calc_trend_value(i - 1, tf, s0, s1, b);
        let y = (1.0 - sf) * (s1 + b);
        s0 = s1;
        s1 = x + y;
    }
    push_float(enh, s1);
    if !samples.histograms.is_empty() {
        return Ok(Annotations::new().with(annos::new_histogram_ignored_in_mixed_range_info(
            get_metric_name(&samples.metric),
            arg_pos(args, 0),
        )));
    }
    Ok(Annotations::new())
}

// ---------------------------------------------------------------------------------------------------------------
// Sorting, clamping, rounding.

/// Appends the float samples of `v` to `enh.out` (Go `filterFloats`).
fn push_floats(enh: &mut EvalNodeHelper, v: &[Sample]) {
    enh.out.extend(v.iter().filter(|s| s.h.is_none()).cloned());
}

/// === sort(node parser.ValueTypeVector) (Vector, Annotations) ===
fn func_sort(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    // NaN should sort to the bottom, so take descending sort with NaN first and reverse it.
    push_floats(enh, &vals[0]);
    gosort::sort(&mut enh.out, |a, b| b.f.is_nan() || b.f > a.f);
    Ok(Annotations::new())
}

/// === sortDesc(node parser.ValueTypeVector) (Vector, Annotations) ===
fn func_sort_desc(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    // NaN should sort to the bottom, so take ascending sort with NaN first and reverse it.
    push_floats(enh, &vals[0]);
    gosort::sort(&mut enh.out, |a, b| b.f.is_nan() || b.f < a.f);
    Ok(Annotations::new())
}

/// Go `natsort.Compare` (github.com/facette/natsort): whether `a` precedes `b` in natural order.
#[doc(hidden)]
pub fn natsort_compare(a: &str, b: &str) -> bool {
    let chunks_a = chunkify(a);
    let chunks_b = chunkify(b);
    let n_a = chunks_a.len();
    let n_b = chunks_b.len();
    for i in 0..n_a {
        if i >= n_b {
            return false;
        }
        let a_int = go_atoi(chunks_a[i]);
        let b_int = go_atoi(chunks_b[i]);
        // If both chunks are numeric, compare them as integers.
        if let (Some(ai), Some(bi)) = (a_int, b_int) {
            if ai == bi {
                if i == n_a - 1 {
                    // We reached the last chunk of A, thus B is greater than A.
                    return true;
                } else if i == n_b - 1 {
                    // We reached the last chunk of B, thus A is greater than B.
                    return false;
                }
                continue;
            }
            return ai < bi;
        }
        // So far both strings are equal, continue to next chunk.
        if chunks_a[i] == chunks_b[i] {
            if i == n_a - 1 {
                return true;
            } else if i == n_b - 1 {
                return false;
            }
            continue;
        }
        return chunks_a[i] < chunks_b[i];
    }
    false
}

/// Go natsort `chunkify`: the runs of ASCII digits and of other characters (regexp `(\d+|\D+)`).
fn chunkify(s: &str) -> Vec<&str> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut start = 0;
    for i in 1..=b.len() {
        if i == b.len() || b[i].is_ascii_digit() != b[start].is_ascii_digit() {
            out.push(&s[start..i]);
            start = i;
        }
    }
    out
}

/// Go `strconv.Atoi` for a run of ASCII digits: `None` on overflow.
fn go_atoi(s: &str) -> Option<i64> {
    if s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    s.parse::<i64>().ok()
}

fn sort_by_label(vals: &[Vector], args: &[Expr], enh: &mut EvalNodeHelper, desc: bool) -> Result<Annotations, Error> {
    let mut lbls: Vec<&str> = Vec::with_capacity(args.len().saturating_sub(1));
    for a in &args[1..] {
        lbls.push(string_from_arg(a)?);
    }
    enh.out.extend(vals[0].iter().cloned());
    gosort::sort_func(&mut enh.out, |a, b| {
        for label in &lbls {
            let lv1 = a.metric.get(label);
            let lv2 = b.metric.get(label);
            if lv1 == lv2 {
                continue;
            }
            let less = natsort_compare(lv1, lv2);
            return if less != desc { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater };
        }
        // If all labels provided as arguments were equal, sort by the full label set.
        let c = labels::compare(&a.metric, &b.metric);
        let c = if desc { -c } else { c };
        c.cmp(&0)
    });
    Ok(Annotations::new())
}

/// === sort_by_label(vector parser.ValueTypeVector, label parser.ValueTypeString...) (Vector, Annotations) ===
fn func_sort_by_label(vals: &[Vector], _: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    sort_by_label(vals, args, enh, false)
}

/// === sort_by_label_desc(vector parser.ValueTypeVector, label parser.ValueTypeString...) (Vector, Annotations) ===
fn func_sort_by_label_desc(
    vals: &[Vector],
    _: &[Series],
    args: &[Expr],
    enh: &mut EvalNodeHelper,
) -> Result<Annotations, Error> {
    sort_by_label(vals, args, enh, true)
}

/// Go `clamp`.
fn clamp(vec: &[Sample], min_val: f64, max_val: f64, enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    if max_val < min_val {
        return Ok(Annotations::new());
    }
    for el in vec {
        if el.h.is_some() {
            // Process only float samples.
            continue;
        }
        let metric = if !enh.enable_delayed_name_removal { drop_metadata(&el.metric) } else { el.metric.clone() };
        enh.out.push(Sample { metric, f: go_max(min_val, go_min(max_val, el.f)), drop_name: true, ..Default::default() });
    }
    Ok(Annotations::new())
}

/// === clamp(Vector parser.ValueTypeVector, min, max Scalar) (Vector, Annotations) ===
fn func_clamp(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    clamp(&vals[0], vals[1][0].f, vals[2][0].f, enh)
}

/// === clamp_max(Vector parser.ValueTypeVector, max Scalar) (Vector, Annotations) ===
fn func_clamp_max(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    clamp(&vals[0], f64::NEG_INFINITY, vals[1][0].f, enh)
}

/// === clamp_min(Vector parser.ValueTypeVector, min Scalar) (Vector, Annotations) ===
fn func_clamp_min(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    clamp(&vals[0], vals[1][0].f, f64::INFINITY, enh)
}

/// === round(Vector parser.ValueTypeVector, toNearest=1 Scalar) (Vector, Annotations) ===
fn func_round(vals: &[Vector], _: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    // round returns a number rounded to toNearest. Ties are solved by rounding up.
    let mut to_nearest = 1.0;
    if args.len() >= 2 {
        to_nearest = vals[1][0].f;
    }
    // Invert as it seems to cause fewer floating point accuracy issues.
    let to_nearest_inverse = 1.0 / to_nearest;
    simple_float_func(vals, enh, |f| (f * to_nearest_inverse + 0.5).floor() / to_nearest_inverse);
    Ok(Annotations::new())
}

/// === Scalar(node parser.ValueTypeVector) Scalar ===
fn func_scalar(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    let mut value = 0.0;
    let mut found = false;
    for s in &vals[0] {
        if s.h.is_none() {
            if found {
                // More than one float found, return NaN.
                push_float(enh, f64::NAN);
                return Ok(Annotations::new());
            }
            found = true;
            value = s.f;
        }
    }
    // Return the single float if found, otherwise return NaN.
    push_float(enh, if found { value } else { f64::NAN });
    Ok(Annotations::new())
}

// ---------------------------------------------------------------------------------------------------------------
// Functions over time.

/// Go `aggrOverTime`.
#[inline]
fn aggr_over_time(matrix: &[Series], enh: &mut EvalNodeHelper, aggr_fn: impl FnOnce(&Series) -> f64) {
    if let Some(el) = matrix.first() {
        let f = aggr_fn(el);
        push_float(enh, f);
    }
}

/// Tracks the counter reset hints of the histograms of an aggregation (Go `trackCounterReset`).
#[derive(Default)]
struct HistAggrTracker {
    counter_reset_seen: bool,
    not_counter_reset_seen: bool,
    nhcb_bounds_reconciled_seen: bool,
}

impl HistAggrTracker {
    fn track(&mut self, h: &FloatHistogram) {
        match h.counter_reset_hint {
            CounterResetHint::CounterReset => self.counter_reset_seen = true,
            CounterResetHint::NotCounterReset => self.not_counter_reset_seen = true,
            _ => {}
        }
    }

    /// The deferred annotations of Go.
    fn finish(&self, annos: &mut Annotations, pos: AnnoPos) {
        if self.counter_reset_seen && self.not_counter_reset_seen {
            annos.add(annos::new_histogram_counter_reset_collision_warning(pos, HistogramOperation::Agg));
        }
        if self.nhcb_bounds_reconciled_seen {
            annos.add(annos::new_mismatched_custom_buckets_histograms_info(pos, HistogramOperation::Agg));
        }
    }
}

/// The histogram average of avg_over_time.
fn avg_histograms(s: &Series, tr: &mut HistAggrTracker) -> Result<FloatHistogram, model::histogram::Error> {
    let mut sum = s.histograms[0].h.copy();
    let mut mean: Option<FloatHistogram> = None;
    let mut kahan_c: Option<FloatHistogram> = None;
    let mut count = 1.0;
    let mut incremental_mean = false;
    tr.track(&sum);
    for (i, h) in s.histograms[1..].iter().enumerate() {
        tr.track(&h.h);
        count = (i + 2) as f64;
        if !incremental_mean {
            let mut sum_copy = sum.copy();
            let c_copy = kahan_c.as_ref().map(|c| c.copy());
            let (c_copy, _, nhcb) = sum_copy.kahan_add(&h.h, c_copy)?;
            if nhcb {
                tr.nhcb_bounds_reconciled_seen = true;
            }
            if !sum_copy.has_overflow() {
                sum = sum_copy;
                kahan_c = Some(c_copy);
                continue;
            }
            incremental_mean = true;
            let mut m = sum.copy();
            m.div(count - 1.0);
            mean = Some(m);
            if let Some(c) = kahan_c.as_mut() {
                c.div(count - 1.0);
            }
        }
        let q = (count - 1.0) / count;
        if let Some(c) = kahan_c.as_mut() {
            c.mul(q);
        }
        let mut to_add = h.h.copy();
        to_add.div(count);
        let m = mean.as_mut().expect("mean");
        m.mul(q);
        let (c, _, nhcb) = m.kahan_add(&to_add, kahan_c.take())?;
        kahan_c = Some(c);
        if nhcb {
            tr.nhcb_bounds_reconciled_seen = true;
        }
    }
    if incremental_mean {
        let mut m = mean.expect("mean");
        if let Some(c) = kahan_c {
            m.add(&c)?;
        }
        return Ok(m);
    }
    if let Some(mut c) = kahan_c {
        sum.div(count);
        c.div(count);
        sum.add(&c)?;
        return Ok(sum);
    }
    sum.div(count);
    Ok(sum)
}

/// Handles the result of a histogram aggregation over time (Go `aggrHistOverTime` and its caller).
fn finish_hist_over_time(
    res: Result<FloatHistogram, model::histogram::Error>,
    tr: &HistAggrTracker,
    first_series: &Series,
    args: &[Expr],
    enh: &mut EvalNodeHelper,
) -> Result<Annotations, Error> {
    match res {
        Ok(h) => {
            let mut annos = Annotations::new();
            tr.finish(&mut annos, arg_pos(args, 0));
            push_hist(enh, h);
            Ok(annos)
        }
        Err(e) => {
            let e = Error::from(e);
            if is_incompatible_schema(&e) {
                return Ok(Annotations::new().with(annos::new_mixed_exponential_custom_histograms_warning(
                    get_metric_name(&first_series.metric),
                    arg_pos(args, 0),
                )));
            }
            // Fail the query on any other error rather than return a wrong result.
            Err(e)
        }
    }
}

/// === avg_over_time(Matrix parser.ValueTypeMatrix) (Vector, Annotations)  ===
fn func_avg_over_time(_: &[Vector], matrix: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    let Some(first_series) = matrix.first() else { return Ok(Annotations::new()) };
    if !first_series.floats.is_empty() && !first_series.histograms.is_empty() {
        return Ok(Annotations::new().with(annos::new_mixed_floats_histograms_warning(
            get_metric_name(&first_series.metric),
            arg_pos(args, 0),
        )));
    }
    if first_series.floats.is_empty() {
        // The passed values only contain histograms.
        let mut tr = HistAggrTracker::default();
        let res = avg_histograms(first_series, &mut tr);
        return finish_hist_over_time(res, &tr, first_series, args, enh);
    }
    aggr_over_time(matrix, enh, |s| {
        // Pre-set the 1st sample to start the loop with the 2nd.
        let (mut sum, mut count) = (s.floats[0].f, 1.0);
        let (mut mean, mut kahan_c) = (0.0, 0.0);
        let mut incremental_mean = false;
        for (i, f) in s.floats[1..].iter().enumerate() {
            count = (i + 2) as f64;
            if !incremental_mean {
                let (new_sum, new_c) = kahan_inc(f.f, sum, kahan_c);
                // Perform regular mean calculation as long as the sum doesn't overflow.
                if !new_sum.is_infinite() {
                    sum = new_sum;
                    kahan_c = new_c;
                    continue;
                }
                // Handle overflow by reverting to incremental calculation of the mean value.
                incremental_mean = true;
                mean = sum / (count - 1.0);
                kahan_c /= count - 1.0;
            }
            let q = (count - 1.0) / count;
            (mean, kahan_c) = kahan_inc(f.f / count, q * mean, q * kahan_c);
        }
        if incremental_mean {
            return mean + kahan_c;
        }
        sum / count + kahan_c / count
    });
    Ok(Annotations::new())
}

#[inline]
fn kahan_inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
    model::histogram::kahansum::inc(inc, sum, c)
}

/// === count_over_time(Matrix parser.ValueTypeMatrix) (Vector, Notes)  ===
fn func_count_over_time(_: &[Vector], matrix: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    aggr_over_time(matrix, enh, |s| (s.floats.len() + s.histograms.len()) as f64);
    Ok(Annotations::new())
}

/// === first_over_time(Matrix parser.ValueTypeMatrix) (Vector, Notes)  ===
fn func_first_over_time(_: &[Vector], matrix: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    let Some(el) = matrix.first() else { return Ok(Annotations::new()) };
    let f = el.floats.first().copied().unwrap_or_default();
    // If a float data point exists and is older than any histogram data points, return it.
    match el.histograms.first() {
        Some(h) if el.floats.is_empty() || h.t <= f.t => {
            enh.out.push(Sample { metric: el.metric.clone(), h: Some(Arc::new(h.h.copy())), ..Default::default() })
        }
        _ => enh.out.push(Sample { metric: el.metric.clone(), f: f.f, ..Default::default() }),
    }
    Ok(Annotations::new())
}

/// === last_over_time(Matrix parser.ValueTypeMatrix) (Vector, Notes)  ===
fn func_last_over_time(_: &[Vector], matrix: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    let Some(el) = matrix.first() else { return Ok(Annotations::new()) };
    let f = el.floats.last().copied().unwrap_or_default();
    match el.histograms.last() {
        Some(h) if el.floats.is_empty() || h.t >= f.t => {
            enh.out.push(Sample { metric: el.metric.clone(), h: Some(Arc::new(h.h.copy())), ..Default::default() })
        }
        _ => enh.out.push(Sample { metric: el.metric.clone(), f: f.f, ..Default::default() }),
    }
    Ok(Annotations::new())
}

/// === mad_over_time(Matrix parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_mad_over_time(_: &[Vector], matrix: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    let Some(samples) = matrix.first() else { return Ok(Annotations::new()) };
    let mut annos = Annotations::new();
    if samples.floats.is_empty() {
        return Ok(annos);
    }
    if !samples.histograms.is_empty() {
        annos.add(annos::new_histogram_ignored_in_mixed_range_info(get_metric_name(&samples.metric), arg_pos(args, 0)));
    }
    aggr_over_time(matrix, enh, |s| {
        FLOAT_BUF.with(|buf| {
            let mut values = buf.borrow_mut();
            values.clear();
            for f in &s.floats {
                // A NaN sample makes the median, and therefore the deviation, undefined.
                if f.f.is_nan() {
                    return f64::NAN;
                }
                values.push(f.f);
            }
            let median = quantile::quantile(0.5, &mut values);
            for v in values.iter_mut() {
                *v = (*v - median).abs();
            }
            quantile::quantile(0.5, &mut values)
        })
    });
    Ok(annos)
}

/// === ts_of_first_over_time(Matrix parser.ValueTypeMatrix) (Vector, Notes)  ===
fn func_ts_of_first_over_time(_: &[Vector], matrix: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    let Some(el) = matrix.first() else { return Ok(Annotations::new()) };
    let tf = el.floats.first().map_or(i64::MAX, |p| p.t);
    let th = el.histograms.first().map_or(i64::MAX, |p| p.t);
    enh.out.push(Sample { metric: el.metric.clone(), f: tf.min(th) as f64 / 1000.0, ..Default::default() });
    Ok(Annotations::new())
}

/// === ts_of_last_over_time(Matrix parser.ValueTypeMatrix) (Vector, Notes)  ===
fn func_ts_of_last_over_time(_: &[Vector], matrix: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    let Some(el) = matrix.first() else { return Ok(Annotations::new()) };
    let tf = el.floats.last().map_or(i64::MIN, |p| p.t);
    let th = el.histograms.last().map_or(i64::MIN, |p| p.t);
    enh.out.push(Sample { metric: el.metric.clone(), f: tf.max(th) as f64 / 1000.0, ..Default::default() });
    Ok(Annotations::new())
}

/// === ts_of_max_over_time(Matrix parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_ts_of_max_over_time(_: &[Vector], m: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    compare_over_time(m, args, enh, |cur, max_val| cur >= max_val || max_val.is_nan(), true)
}

/// === ts_of_min_over_time(Matrix parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_ts_of_min_over_time(_: &[Vector], m: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    compare_over_time(m, args, enh, |cur, max_val| cur <= max_val || max_val.is_nan(), true)
}

/// Go `compareOverTime`, used by max_over_time, min_over_time and the ts_of_ variants.
fn compare_over_time(
    matrix: &[Series],
    args: &[Expr],
    enh: &mut EvalNodeHelper,
    compare_fn: impl Fn(f64, f64) -> bool,
    return_timestamp: bool,
) -> Result<Annotations, Error> {
    let Some(samples) = matrix.first() else { return Ok(Annotations::new()) };
    let mut annos = Annotations::new();
    if samples.floats.is_empty() {
        return Ok(annos);
    }
    if !samples.histograms.is_empty() {
        annos.add(annos::new_histogram_ignored_in_mixed_range_info(get_metric_name(&samples.metric), arg_pos(args, 0)));
    }
    aggr_over_time(matrix, enh, |s| {
        let mut max_val = s.floats[0].f;
        let mut ts_of_max = s.floats[0].t;
        for f in &s.floats {
            if compare_fn(f.f, max_val) {
                max_val = f.f;
                ts_of_max = f.t;
            }
        }
        if return_timestamp {
            return ts_of_max as f64 / 1000.0;
        }
        max_val
    });
    Ok(annos)
}

/// === max_over_time(Matrix parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_max_over_time(_: &[Vector], m: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    compare_over_time(m, args, enh, |cur, max_val| cur > max_val || max_val.is_nan(), false)
}

/// === min_over_time(Matrix parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_min_over_time(_: &[Vector], m: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    compare_over_time(m, args, enh, |cur, max_val| cur < max_val || max_val.is_nan(), false)
}

/// The histogram sum of sum_over_time.
fn sum_histograms(s: &Series, tr: &mut HistAggrTracker) -> Result<FloatHistogram, model::histogram::Error> {
    let mut sum = s.histograms[0].h.copy();
    tr.track(&sum);
    let mut comp: Option<FloatHistogram> = None;
    for h in &s.histograms[1..] {
        tr.track(&h.h);
        let (c, _, nhcb) = sum.kahan_add(&h.h, comp.take())?;
        comp = Some(c);
        if nhcb {
            tr.nhcb_bounds_reconciled_seen = true;
        }
    }
    if let Some(c) = comp {
        let (_, nhcb) = sum.add(&c)?;
        if nhcb {
            tr.nhcb_bounds_reconciled_seen = true;
        }
    }
    Ok(sum)
}

/// === sum_over_time(Matrix parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_sum_over_time(_: &[Vector], matrix: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    let Some(first_series) = matrix.first() else { return Ok(Annotations::new()) };
    if !first_series.floats.is_empty() && !first_series.histograms.is_empty() {
        return Ok(Annotations::new().with(annos::new_mixed_floats_histograms_warning(
            get_metric_name(&first_series.metric),
            arg_pos(args, 0),
        )));
    }
    if first_series.floats.is_empty() {
        // The passed values only contain histograms.
        let mut tr = HistAggrTracker::default();
        let res = sum_histograms(first_series, &mut tr);
        return finish_hist_over_time(res, &tr, first_series, args, enh);
    }
    aggr_over_time(matrix, enh, |s| {
        let (mut sum, mut c) = (0.0, 0.0);
        for f in &s.floats {
            (sum, c) = kahan_inc(f.f, sum, c);
        }
        if sum.is_infinite() {
            return sum;
        }
        sum + c
    });
    Ok(Annotations::new())
}

const INTEGRAL_LEFT_POINT: i64 = 0;
const INTEGRAL_RIGHT_POINT: i64 = 1;
const INTEGRAL_TRAPEZOIDAL: i64 = 2;

/// === integral(Matrix parser.ValueTypeMatrix, strategy=2 Scalar) (Vector, Annotations) ===
fn func_integral(vals: &[Vector], matrix: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    let Some(samples) = matrix.first() else { return Ok(Annotations::new()) };
    let mut annos = Annotations::new();
    if samples.floats.is_empty() {
        return Ok(annos);
    }
    if !samples.histograms.is_empty() {
        annos.add(annos::new_histogram_ignored_in_mixed_range_info(get_metric_name(&samples.metric), arg_pos(args, 0)));
    }

    let mut strategy = INTEGRAL_TRAPEZOIDAL;
    if !vals.is_empty() && !vals[0].is_empty() {
        let strategy_arg = vals[0][0].f;
        if strategy_arg.is_nan()
            || strategy_arg.is_infinite()
            || strategy_arg.trunc() != strategy_arg
            || strategy_arg < INTEGRAL_LEFT_POINT as f64
            || strategy_arg > INTEGRAL_TRAPEZOIDAL as f64
        {
            annos.add(annos::new_invalid_integral_strategy_warning(strategy_arg, arg_pos(args, 1)));
        } else {
            strategy = strategy_arg as i64;
        }
    }

    let nan_as_zero = |v: f64| if v.is_nan() { 0.0 } else { v };

    aggr_over_time(matrix, enh, |s| {
        let (mut sum, mut c) = (0.0, 0.0);
        let mut prev = FPoint::default();
        for (i, f) in s.floats.iter().enumerate() {
            let (mut value, mut c_value) = (0.0, 0.0);
            // Treat NaN as zero, to let "neighboring" non-zero values handle it.
            let curr_val = nan_as_zero(f.f);
            let prev_val = nan_as_zero(prev.f);
            match strategy {
                // Left-point rectangle rule.
                INTEGRAL_LEFT_POINT => value = prev_val,
                // Right-point rectangle rule.
                INTEGRAL_RIGHT_POINT => value = curr_val,
                // Trapezoidal rule (default).
                _ => {
                    if prev_val != 0.0 || curr_val != 0.0 {
                        (value, c_value) = kahan_inc(curr_val, prev_val, 0.0);
                        if value.is_infinite() && !curr_val.is_infinite() && !prev_val.is_infinite() {
                            // Avoid overflow when averaging finite samples.
                            (value, c_value) = kahan_inc(curr_val / 2.0, prev_val / 2.0, 0.0);
                        } else {
                            value /= 2.0;
                            c_value /= 2.0;
                        }
                    }
                }
            }
            // Skip the first sample, aggregate non-zero values.
            if i > 0 && (value != 0.0 || c_value != 0.0) {
                let delta_t = (f.t - prev.t) as f64 / 1000.0;
                (sum, c) = kahan_inc(value * delta_t, sum, c + c_value * delta_t);
            }
            prev = *f;
        }
        if sum.is_infinite() {
            return sum;
        }
        sum + c
    });
    Ok(annos)
}

/// === quantile_over_time(Matrix parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_quantile_over_time(vals: &[Vector], matrix: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    if vals.is_empty() || vals[0].is_empty() || matrix.is_empty() {
        return Ok(Annotations::new());
    }
    let q = vals[0][0].f;
    let el = &matrix[0];
    if el.floats.is_empty() {
        return Ok(Annotations::new());
    }
    let mut annos = Annotations::new();
    if q.is_nan() || !(0.0..=1.0).contains(&q) {
        annos.add(annos::new_invalid_quantile_warning(q, arg_pos(args, 0)));
    }
    if !el.histograms.is_empty() {
        annos.add(annos::new_histogram_ignored_in_mixed_range_info(get_metric_name(&el.metric), arg_pos(args, 0)));
    }
    let r = FLOAT_BUF.with(|buf| {
        let mut values = buf.borrow_mut();
        values.clear();
        values.extend(el.floats.iter().map(|p| p.f));
        quantile::quantile(q, &mut values)
    });
    push_float(enh, r);
    Ok(annos)
}

/// Go `varianceOverTime`.
fn variance_over_time(
    matrix: &[Series],
    args: &[Expr],
    enh: &mut EvalNodeHelper,
    variance_to_result: Option<fn(f64) -> f64>,
) -> Result<Annotations, Error> {
    let Some(samples) = matrix.first() else { return Ok(Annotations::new()) };
    let mut annos = Annotations::new();
    if samples.floats.is_empty() {
        return Ok(annos);
    }
    if !samples.histograms.is_empty() {
        annos.add(annos::new_histogram_ignored_in_mixed_range_info(get_metric_name(&samples.metric), arg_pos(args, 0)));
    }
    aggr_over_time(matrix, enh, |s| {
        let mut count = 0.0;
        let (mut mean, mut c_mean) = (0.0, 0.0);
        let (mut aux, mut c_aux) = (0.0, 0.0);
        for f in &s.floats {
            count += 1.0;
            let delta = f.f - (mean + c_mean);
            (mean, c_mean) = kahan_inc(delta / count, mean, c_mean);
            (aux, c_aux) = kahan_inc(delta * (f.f - (mean + c_mean)), aux, c_aux);
        }
        let variance = (aux + c_aux) / count;
        match variance_to_result {
            None => variance,
            Some(f) => f(variance),
        }
    });
    Ok(annos)
}

/// === stddev_over_time(Matrix parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_stddev_over_time(_: &[Vector], m: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    variance_over_time(m, args, enh, Some(f64::sqrt))
}

/// === stdvar_over_time(Matrix parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_stdvar_over_time(_: &[Vector], m: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    variance_over_time(m, args, enh, None)
}

/// === absent(Vector parser.ValueTypeVector) (Vector, Annotations) ===
fn func_absent(vals: &[Vector], _: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    if !vals[0].is_empty() {
        return Ok(Annotations::new());
    }
    enh.out.push(Sample { metric: create_labels_for_absent_function(&args[0]), f: 1.0, ..Default::default() });
    Ok(Annotations::new())
}

/// === absent_over_time(Vector parser.ValueTypeMatrix) (Vector, Annotations) ===
/// Returns 1 if the matrix has at least one element; the engine post-processes the results.
fn func_absent_over_time(_: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    push_float(enh, 1.0);
    Ok(Annotations::new())
}

/// === present_over_time(Vector parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_present_over_time(_: &[Vector], m: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    aggr_over_time(m, enh, |_| 1.0);
    Ok(Annotations::new())
}

// ---------------------------------------------------------------------------------------------------------------
// Simple math functions.

/// Go `simpleFloatFunc`.
#[inline]
fn simple_float_func(vals: &[Vector], enh: &mut EvalNodeHelper, f: impl Fn(f64) -> f64) {
    let delayed = enh.enable_delayed_name_removal;
    for el in &vals[0] {
        if el.h.is_none() {
            // Process only float samples.
            let metric = if !delayed { drop_metadata(&el.metric) } else { el.metric.clone() };
            enh.out.push(Sample { metric, f: f(el.f), drop_name: true, ..Default::default() });
        }
    }
}

macro_rules! simple_funcs {
    ($($name:ident => $f:expr;)*) => {
        $(
            fn $name(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
                simple_float_func(vals, enh, $f);
                Ok(Annotations::new())
            }
        )*
    };
}

simple_funcs! {
    func_abs => f64::abs;
    func_ceil => f64::ceil;
    func_floor => f64::floor;
    func_exp => gomath::exp;
    func_sqrt => f64::sqrt;
    func_ln => gomath::log;
    func_log2 => gomath::log2;
    func_log10 => gomath::log10;
    func_sin => gomath::sin;
    func_cos => gomath::cos;
    func_tan => gomath::tan;
    func_asin => gomath::asin;
    func_acos => gomath::acos;
    func_atan => gomath::atan;
    func_sinh => gomath::sinh;
    func_cosh => gomath::cosh;
    func_tanh => gomath::tanh;
    func_asinh => gomath::asinh;
    func_acosh => gomath::acosh;
    func_atanh => gomath::atanh;
    func_rad => |v: f64| v * (std::f64::consts::PI / 180.0);
    func_deg => |v: f64| v * (180.0 / std::f64::consts::PI);
    func_sgn => |v: f64| if v < 0.0 { -1.0 } else if v > 0.0 { 1.0 } else { v };
}

/// === max_of(a, b parser.ValueTypeScalar) Scalar ===
fn func_max_of(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    push_float(enh, go_max(vals[0][0].f, vals[1][0].f));
    Ok(Annotations::new())
}

/// === min_of(a, b parser.ValueTypeScalar) Scalar ===
fn func_min_of(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    push_float(enh, go_min(vals[0][0].f, vals[1][0].f));
    Ok(Annotations::new())
}

/// === pi() Scalar ===
fn func_pi(_: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    push_float(enh, std::f64::consts::PI);
    Ok(Annotations::new())
}

/// === timestamp(Vector parser.ValueTypeVector) (Vector, Annotations) ===
fn func_timestamp(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    let delayed = enh.enable_delayed_name_removal;
    for el in &vals[0] {
        let metric = if !delayed { drop_metadata(&el.metric) } else { el.metric.clone() };
        enh.out.push(Sample { metric, f: el.t as f64 / 1000.0, drop_name: true, ..Default::default() });
    }
    Ok(Annotations::new())
}

/// === start_timestamp(Vector parser.ValueTypeVector) (Vector, Annotations) ===
fn func_start_timestamp(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    let delayed = enh.enable_delayed_name_removal;
    let sts = enh.start_timestamps.take();
    let floats: &[i64] = match &sts {
        Some(s) => &s.floats,
        None => &[],
    };
    for (i, el) in vals[0].iter().enumerate() {
        if i >= floats.len() {
            // Only return results if start timestamps slice is populated.
            continue;
        }
        let metric = if !delayed { drop_metadata(&el.metric) } else { el.metric.clone() };
        enh.out.push(Sample { metric, f: floats[i] as f64 / 1000.0, drop_name: true, ..Default::default() });
    }
    enh.start_timestamps = sts;
    Ok(Annotations::new())
}

/// Go `linearRegression`: least-square linear regression; returns the slope and the intercept at
/// `intercept_time`.
fn linear_regression(samples: &[FPoint], intercept_time: i64) -> (f64, f64) {
    let mut n = 0.0;
    let (mut sum_x, mut c_x) = (0.0, 0.0);
    let (mut sum_y, mut c_y) = (0.0, 0.0);
    let (mut sum_xy, mut c_xy) = (0.0, 0.0);
    let (mut sum_x2, mut c_x2) = (0.0, 0.0);
    let init_y = samples[0].f;
    let mut const_y = true;
    for (i, sample) in samples.iter().enumerate() {
        // Set constY to false if any new y values are encountered.
        if const_y && i > 0 && sample.f != init_y {
            const_y = false;
        }
        n += 1.0;
        let x = (sample.t - intercept_time) as f64 / 1e3;
        (sum_x, c_x) = kahan_inc(x, sum_x, c_x);
        (sum_y, c_y) = kahan_inc(sample.f, sum_y, c_y);
        (sum_xy, c_xy) = kahan_inc(x * sample.f, sum_xy, c_xy);
        (sum_x2, c_x2) = kahan_inc(x * x, sum_x2, c_x2);
    }
    if const_y {
        if init_y.is_infinite() {
            return (f64::NAN, f64::NAN);
        }
        return (0.0, init_y);
    }
    sum_x += c_x;
    sum_y += c_y;
    sum_xy += c_xy;
    sum_x2 += c_x2;

    let cov_xy = sum_xy - sum_x * sum_y / n;
    let var_x = sum_x2 - sum_x * sum_x / n;

    let slope = cov_xy / var_x;
    let intercept = sum_y / n - slope * sum_x / n;
    (slope, intercept)
}

/// === deriv(node parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_deriv(_: &[Vector], matrix: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    let Some(samples) = matrix.first() else { return Ok(Annotations::new()) };
    // No sense in trying to compute a derivative without at least two float points.
    if samples.floats.len() < 2 {
        // Annotate mix of float and histogram.
        if samples.floats.len() == 1 && !samples.histograms.is_empty() {
            return Ok(Annotations::new().with(annos::new_histogram_ignored_in_mixed_range_info(
                get_metric_name(&samples.metric),
                arg_pos(args, 0),
            )));
        }
        return Ok(Annotations::new());
    }
    // We pass in an arbitrary timestamp that is near the values in use to avoid floating point accuracy issues.
    let (slope, _) = linear_regression(&samples.floats, samples.floats[0].t);
    push_float(enh, slope);
    if !samples.histograms.is_empty() {
        return Ok(Annotations::new().with(annos::new_histogram_ignored_in_mixed_range_info(
            get_metric_name(&samples.metric),
            arg_pos(args, 0),
        )));
    }
    Ok(Annotations::new())
}

/// === predict_linear(node parser.ValueTypeMatrix, k parser.ValueTypeScalar) (Vector, Annotations) ===
fn func_predict_linear(vals: &[Vector], matrix: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    if vals.is_empty() || vals[0].is_empty() || matrix.is_empty() {
        return Ok(Annotations::new());
    }
    let samples = &matrix[0];
    let duration = vals[0][0].f;
    // No sense in trying to predict anything without at least two float points.
    if samples.floats.len() < 2 {
        if samples.floats.len() == 1 && !samples.histograms.is_empty() {
            return Ok(Annotations::new().with(annos::new_histogram_ignored_in_mixed_range_info(
                get_metric_name(&samples.metric),
                arg_pos(args, 0),
            )));
        }
        return Ok(Annotations::new());
    }
    let (slope, intercept) = linear_regression(&samples.floats, enh.ts);
    push_float(enh, slope * duration + intercept);
    if !samples.histograms.is_empty() {
        return Ok(Annotations::new().with(annos::new_histogram_ignored_in_mixed_range_info(
            get_metric_name(&samples.metric),
            arg_pos(args, 0),
        )));
    }
    Ok(Annotations::new())
}

// ---------------------------------------------------------------------------------------------------------------
// Histogram functions.

/// Go `simpleHistogramFunc`.
fn simple_histogram_func(vals: &[Vector], enh: &mut EvalNodeHelper, f: impl Fn(&FloatHistogram) -> f64) {
    let delayed = enh.enable_delayed_name_removal;
    for el in &vals[0] {
        if let Some(h) = &el.h {
            // Process only histogram samples.
            let metric = if !delayed { drop_metadata(&el.metric) } else { el.metric.clone() };
            enh.out.push(Sample { metric, f: f(h), drop_name: true, ..Default::default() });
        }
    }
}

/// === histogram_count(Vector parser.ValueTypeVector) (Vector, Annotations) ===
fn func_histogram_count(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    simple_histogram_func(vals, enh, |h| h.count);
    Ok(Annotations::new())
}

/// === histogram_sum(Vector parser.ValueTypeVector) (Vector, Annotations) ===
fn func_histogram_sum(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    simple_histogram_func(vals, enh, |h| h.sum);
    Ok(Annotations::new())
}

/// === histogram_avg(Vector parser.ValueTypeVector) (Vector, Annotations) ===
fn func_histogram_avg(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    simple_histogram_func(vals, enh, |h| h.sum / h.count);
    Ok(Annotations::new())
}

/// Go `histogramVariance`.
fn histogram_variance(vals: &[Vector], enh: &mut EvalNodeHelper, variance_to_result: Option<fn(f64) -> f64>) -> Result<Annotations, Error> {
    simple_histogram_func(vals, enh, |h| {
        let mean = h.sum / h.count;
        let (mut variance, mut c_variance) = (0.0, 0.0);
        let mut it = h.all_bucket_iterator();
        while it.next() {
            let bucket = it.at();
            if bucket.count == 0.0 {
                continue;
            }
            let val = if h.uses_custom_buckets() {
                // Use arithmetic mean in case of custom buckets.
                (bucket.upper + bucket.lower) / 2.0
            } else if bucket.lower <= 0.0 && bucket.upper >= 0.0 {
                // Use zero (effectively the arithmetic mean) in the zero bucket of a standard exponential
                // histogram.
                0.0
            } else {
                // Use geometric mean in case of standard exponential buckets.
                let v = (bucket.upper * bucket.lower).sqrt();
                if bucket.upper < 0.0 { -v } else { v }
            };
            let delta = val - mean;
            (variance, c_variance) = kahan_inc(bucket.count * delta * delta, variance, c_variance);
        }
        variance += c_variance;
        variance /= h.count;
        match variance_to_result {
            Some(f) => f(variance),
            None => variance,
        }
    });
    Ok(Annotations::new())
}

/// === histogram_stddev(Vector parser.ValueTypeVector) (Vector, Annotations)  ===
fn func_histogram_stddev(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    histogram_variance(vals, enh, Some(f64::sqrt))
}

/// === histogram_stdvar(Vector parser.ValueTypeVector) (Vector, Annotations) ===
fn func_histogram_stdvar(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    histogram_variance(vals, enh, None)
}

/// Go `strconv.ParseFloat(s, 64)` for bucket labels: `None` on error.
fn parse_bucket_bound(s: &str) -> Option<f64> {
    crate::parser::quote::parse_float(s).ok()
}

impl EvalNodeHelper {
    /// Go `resetHistograms`: prepares the histogram caches by splitting the vector into native and classic
    /// histograms.
    pub(crate) fn reset_histograms(&mut self, in_vec: &[Sample], arg: &Expr) -> Annotations {
        let mut annos = Annotations::new();
        let pos = pr(arg.position_range());
        for v in self.signature_to_metric_with_buckets.values_mut() {
            v.buckets.clear();
        }
        self.native_histogram_samples.clear();

        for sample in in_vec {
            // We are only looking for classic buckets here. Remember the histograms for later treatment.
            if sample.h.is_some() {
                self.native_histogram_samples.push(sample.clone());
                continue;
            }
            let bucket_label = sample.metric.get(BUCKET_LABEL);
            let upper_bound = match parse_bucket_bound(bucket_label) {
                Some(u) if !u.is_nan() => u,
                _ => {
                    let name = if self.enable_delayed_name_removal { sample.metric.get(METRIC_NAME) } else { "" };
                    annos.add(annos::new_bad_bucket_label_warning(name, bucket_label, pos));
                    continue;
                }
            };
            sample.metric.bytes_without_labels(&mut self.lbl_buf, &EXCLUDED_LABELS);
            let bucket = Bucket { upper_bound, count: sample.f };
            if let Some(mb) = self.signature_to_metric_with_buckets.get_mut(self.lbl_buf.as_slice()) {
                mb.buckets.push(bucket);
                continue;
            }
            let mut b = Builder::new(sample.metric.clone());
            b.del_many(&EXCLUDED_LABELS);
            let metric = b.labels();
            self.signature_to_metric_with_buckets
                .insert(self.lbl_buf.clone(), MetricWithBuckets { metric, buckets: vec![bucket] });
        }

        for idx in 0..self.native_histogram_samples.len() {
            // We have to reconstruct the exact same signature as above for a classic histogram, just ignoring any
            // le label.
            self.native_histogram_samples[idx].metric.bytes(&mut self.lbl_buf);
            let conflict = matches!(
                self.signature_to_metric_with_buckets.get(self.lbl_buf.as_slice()),
                Some(mb) if !mb.buckets.is_empty()
            );
            if conflict {
                // At this data point, we have classic histogram buckets and a native histogram with the same name
                // and labels. Do not evaluate anything.
                let s = &self.native_histogram_samples[idx];
                let name = if self.enable_delayed_name_removal { s.metric.get(METRIC_NAME) } else { "" };
                annos.add(annos::new_mixed_classic_native_histograms_warning(name, pos));
                self.signature_to_metric_with_buckets.remove(self.lbl_buf.as_slice());
                self.native_histogram_samples[idx].h = None;
            }
        }
        annos
    }
}

/// The key of a quantile in the caches (Go map[float64] keys: 0 and -0 are the same key).
#[inline]
fn quantile_key(q: f64) -> u64 {
    if q == 0.0 { 0 } else { q.to_bits() }
}

/// Go `getOrCreateLblsWithQuantile`.
fn get_or_create_lbls_with_quantile(
    cache: &mut HashMap<Vec<u8>, HashMap<u64, Labels>>,
    quantile_strs: &HashMap<u64, String>,
    lbl_buf: &mut Vec<u8>,
    lbls: &Labels,
    quantile_label: &str,
    q: f64,
) -> Labels {
    lbls.bytes(lbl_buf);
    if !cache.contains_key(lbl_buf.as_slice()) {
        cache.insert(lbl_buf.clone(), HashMap::with_capacity(quantile_strs.len()));
    }
    let cached = cache.get_mut(lbl_buf.as_slice()).expect("cached labels");
    let key = quantile_key(q);
    if !q.is_nan() {
        if let Some(l) = cached.get(&key) {
            return l.clone();
        }
    }
    let quantile_str: &str = if q.is_nan() {
        "NaN"
    } else {
        quantile_strs.get(&key).map(|s| s.as_str()).unwrap_or("")
    };
    let mut b = Builder::new(lbls.clone());
    b.set(quantile_label, quantile_str);
    let l = b.labels();
    cached.insert(key, l.clone());
    l
}

/// === histogram_fraction(lower, upper parser.ValueTypeScalar, Vector parser.ValueTypeVector) (Vector, Annotations) ===
fn func_histogram_fraction(vals: &[Vector], _: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    if vals.len() < 3 || vals[0].is_empty() || vals[1].is_empty() {
        return Ok(Annotations::new());
    }
    let lower = vals[0][0].f;
    let upper = vals[1][0].f;
    let in_vec = &vals[2];

    let mut annos = enh.reset_histograms(in_vec, &args[2]);
    let pos = pr(args[2].position_range());
    let delayed = enh.enable_delayed_name_removal;

    // Deal with the native histograms.
    for sample in &enh.native_histogram_samples {
        let Some(h) = &sample.h else {
            // Native histogram conflicts with classic histogram at the same timestamp, ignore.
            continue;
        };
        let metric = if !delayed { drop_metadata(&sample.metric) } else { sample.metric.clone() };
        let (hf, hf_annos) = quantile::histogram_fraction(lower, upper, h, get_metric_name(&metric), pos);
        annos.merge(hf_annos);
        enh.out.push(Sample { metric, f: hf, drop_name: true, ..Default::default() });
    }

    // Deal with classic histograms that have already been filtered for conflicting native histograms.
    for mb in enh.signature_to_metric_with_buckets.values_mut() {
        if mb.buckets.is_empty() {
            continue;
        }
        if !delayed {
            mb.metric = drop_metadata(&mb.metric);
        }
        enh.out.push(Sample {
            metric: mb.metric.clone(),
            f: quantile::bucket_fraction(lower, upper, &mut mb.buckets),
            drop_name: true,
            ..Default::default()
        });
    }
    Ok(annos)
}

/// === histogram_quantile(k parser.ValueTypeScalar, Vector parser.ValueTypeVector) (Vector, Annotations) ===
fn func_histogram_quantile(vals: &[Vector], _: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    if vals.len() < 2 || vals[0].is_empty() {
        return Ok(Annotations::new());
    }
    let q = vals[0][0].f;
    let in_vec = &vals[1];
    let mut annos = Annotations::new();

    if let Some(e) = validate_quantile(q, &args[0]) {
        annos.add(e);
    }
    annos.merge(enh.reset_histograms(in_vec, &args[1]));
    let pos = pr(args[1].position_range());
    let delayed = enh.enable_delayed_name_removal;

    // Deal with the native histograms.
    for sample in &enh.native_histogram_samples {
        let Some(h) = &sample.h else {
            // Native histogram conflicts with classic histogram at the same timestamp, ignore.
            continue;
        };
        let metric = if !delayed { drop_metadata(&sample.metric) } else { sample.metric.clone() };
        let (hq, hq_annos) = quantile::histogram_quantile(q, h, get_metric_name(&metric), pos);
        annos.merge(hq_annos);
        enh.out.push(Sample { metric, f: hq, drop_name: true, ..Default::default() });
    }

    // Deal with classic histograms that have already been filtered for conflicting native histograms.
    for mb in enh.signature_to_metric_with_buckets.values_mut() {
        if mb.buckets.is_empty() {
            continue;
        }
        let r = quantile::bucket_quantile(q, &mut mb.buckets);
        if r.forced_monotonic {
            let metric_name = if delayed { get_metric_name(&mb.metric) } else { "" };
            annos.add(annos::new_histogram_quantile_forced_monotonicity_info(
                metric_name,
                pos,
                enh.ts,
                r.min_bucket,
                r.max_bucket,
                r.max_diff,
            ));
        }
        if !delayed {
            mb.metric = drop_metadata(&mb.metric);
        }
        enh.out.push(Sample { metric: mb.metric.clone(), f: r.quantile, drop_name: true, ..Default::default() });
    }
    Ok(annos)
}

/// Go `validateQuantile`.
fn validate_quantile(q: f64, arg: &Expr) -> Option<Error> {
    if q.is_nan() || !(0.0..=1.0).contains(&q) {
        return Some(annos::new_invalid_quantile_warning(q, pr(arg.position_range())));
    }
    None
}

/// === histogram_quantiles(Vector parser.ValueTypeVector, label parser.ValueTypeString, q0 parser.ValueTypeScalar, qs parser.ValueTypeScalar...) (Vector, Annotations) ===
fn func_histogram_quantiles(vals: &[Vector], _: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    let in_vec = &vals[0];
    let quantile_label = string_from_arg(&args[1])?;
    let num_quantiles = vals.len().saturating_sub(2);
    let mut qs: Vec<f64> = Vec::with_capacity(num_quantiles);
    let mut annos = Annotations::new();

    for i in 2..vals.len() {
        let q = vals[i][0].f;
        if let Some(e) = validate_quantile(q, &args[i]) {
            annos.add(e);
        }
        let key = quantile_key(q);
        if !q.is_nan() && !enh.quantile_strs.contains_key(&key) {
            enh.quantile_strs.insert(key, labels::format_open_metrics_float(q));
        }
        qs.push(q);
    }

    annos.merge(enh.reset_histograms(in_vec, &args[0]));
    let pos = pr(args[0].position_range());
    let delayed = enh.enable_delayed_name_removal;
    let ts = enh.ts;

    let EvalNodeHelper {
        out,
        native_histogram_samples,
        signature_to_metric_with_buckets,
        quantile_strs,
        signature_to_labels_with_quantile,
        lbl_buf,
        ..
    } = enh;

    for &q in &qs {
        // Deal with the native histograms.
        for sample in native_histogram_samples.iter() {
            let Some(h) = &sample.h else {
                // Native histogram conflicts with classic histogram at the same timestamp, ignore.
                continue;
            };
            let metric = if !delayed { drop_metadata(&sample.metric) } else { sample.metric.clone() };
            let (hq, hq_annos) = quantile::histogram_quantile(q, h, metric.get(METRIC_NAME), pos);
            annos.merge(hq_annos);
            let metric = get_or_create_lbls_with_quantile(
                signature_to_labels_with_quantile,
                quantile_strs,
                lbl_buf,
                &metric,
                quantile_label,
                q,
            );
            out.push(Sample { metric, f: hq, drop_name: true, ..Default::default() });
        }

        // Deal with classic histograms that have already been filtered for conflicting native histograms.
        for mb in signature_to_metric_with_buckets.values_mut() {
            if mb.buckets.is_empty() {
                continue;
            }
            let r = quantile::bucket_quantile(q, &mut mb.buckets);
            if r.forced_monotonic {
                let metric_name = if delayed { get_metric_name(&mb.metric) } else { "" };
                annos.add(annos::new_histogram_quantile_forced_monotonicity_info(
                    metric_name,
                    pos,
                    ts,
                    r.min_bucket,
                    r.max_bucket,
                    r.max_diff,
                ));
            }
            if !delayed {
                mb.metric = drop_metadata(&mb.metric);
            }
            let metric = get_or_create_lbls_with_quantile(
                signature_to_labels_with_quantile,
                quantile_strs,
                lbl_buf,
                &mb.metric,
                quantile_label,
                q,
            );
            out.push(Sample { metric, f: r.quantile, drop_name: true, ..Default::default() });
        }
    }
    Ok(annos)
}

/// Go `pickFirstSampleIndices`: the start indices into the floats and histograms for anchored range processing.
/// Returns (firstFloat, firstHistogram, found).
fn pick_first_sample_indices(floats: &[FPoint], histograms: &[HPoint], enh: &EvalNodeHelper) -> (usize, usize, bool) {
    if !enh.anchored {
        return (0, 0, true);
    }
    let range_start = enh.ts - dur_ms(enh.range + enh.offset);

    // Index of the last float / histogram at or before the range start, or -1.
    let last_float_le = gosort::search(floats.len(), |i| floats[i].t > range_start) as isize - 1;
    let last_hist_le = gosort::search(histograms.len(), |i| histograms[i].t > range_start) as isize - 1;

    // Without a sample strictly after the range start there is nothing to measure.
    if last_float_le + 1 >= floats.len() as isize && last_hist_le + 1 >= histograms.len() as isize {
        return (0, 0, false);
    }

    if last_float_le < 0 && last_hist_le < 0 {
        // No anchor; every sample is after the range start, so include all.
        (0, 0, true)
    } else if last_hist_le < 0
        || (last_float_le >= 0 && floats[last_float_le as usize].t >= histograms[last_hist_le as usize].t)
    {
        // The anchor is a float; histograms at or before the range start precede it and are skipped.
        (last_float_le as usize, (last_hist_le + 1) as usize, true)
    } else {
        // The anchor is a histogram; floats at or before the range start precede it and are skipped.
        ((last_float_le + 1) as usize, last_hist_le as usize, true)
    }
}

/// === resets(Matrix parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_resets(_: &[Vector], matrix: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    let Some(series) = matrix.first() else { return Ok(Annotations::new()) };
    let floats = &series.floats;
    let histograms = &series.histograms;
    let mut resets = 0;
    if floats.is_empty() && histograms.is_empty() {
        return Ok(Annotations::new());
    }
    let (float_sts, histogram_sts): (&[i64], &[i64]) = match &enh.start_timestamps {
        Some(sts) => (&sts.floats, &sts.histograms),
        None => (&[], &[]),
    };
    let (first_float, first_histogram, found) = pick_first_sample_indices(floats, histograms, enh);
    if !found {
        return Ok(Annotations::new());
    }
    let (mut prev_t, mut prev_f, mut prev_h): (i64, f64, Option<&FloatHistogram>) = (0, 0.0, None);
    let mut cur_f = 0.0;
    let mut prev_st = 0i64;
    let (mut i_float, mut i_histogram) = (first_float, first_histogram);
    while i_float < floats.len() || i_histogram < histograms.len() {
        let mut cur_st = 0i64;
        let (cur_t, cur_h): (i64, Option<&FloatHistogram>);
        if i_histogram >= histograms.len() || (i_float < floats.len() && floats[i_float].t < histograms[i_histogram].t) {
            // Process a float sample if no histogram sample remains or its timestamp is earlier.
            cur_t = floats[i_float].t;
            cur_f = floats[i_float].f;
            cur_h = None;
            if i_float < float_sts.len() {
                cur_st = float_sts[i_float];
            }
            i_float += 1;
        } else if i_float >= floats.len() || (i_histogram < histograms.len() && floats[i_float].t > histograms[i_histogram].t) {
            // Process a histogram sample if no float sample remains or its timestamp is earlier.
            cur_t = histograms[i_histogram].t;
            cur_h = Some(&histograms[i_histogram].h);
            if i_histogram < histogram_sts.len() {
                cur_st = histogram_sts[i_histogram];
            }
            i_histogram += 1;
        } else {
            // A float and a histogram at the same timestamp (Go would loop forever).
            break;
        }
        // Skip the comparison for the first sample, just initialize prevSample.
        if i_float + i_histogram == 1 + first_float + first_histogram {
            (prev_t, prev_f, prev_h) = (cur_t, cur_f, cur_h);
            prev_st = cur_st;
            continue;
        }
        match (prev_h, cur_h) {
            (None, None) => {
                if cur_f < prev_f || is_start_timestamp_reset(prev_st, prev_t, cur_st, cur_t) {
                    resets += 1;
                }
            }
            (Some(ph), Some(ch)) => {
                if is_start_timestamp_reset(prev_st, prev_t, cur_st, cur_t) || ch.detect_reset(ph) {
                    resets += 1;
                }
            }
            _ => resets += 1,
        }
        (prev_t, prev_f, prev_h) = (cur_t, cur_f, cur_h);
        prev_st = cur_st;
    }
    push_float(enh, resets as f64);
    Ok(Annotations::new())
}

/// === changes(Matrix parser.ValueTypeMatrix) (Vector, Annotations) ===
fn func_changes(_: &[Vector], matrix: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    let Some(series) = matrix.first() else { return Ok(Annotations::new()) };
    let floats = &series.floats;
    let histograms = &series.histograms;
    let mut changes = 0;
    if floats.is_empty() && histograms.is_empty() {
        return Ok(Annotations::new());
    }
    let (first_float, first_histogram, found) = pick_first_sample_indices(floats, histograms, enh);
    if !found {
        return Ok(Annotations::new());
    }
    let (mut prev_f, mut prev_h): (f64, Option<&FloatHistogram>) = (0.0, None);
    let mut cur_f = 0.0;
    let (mut i_float, mut i_histogram) = (first_float, first_histogram);
    while i_float < floats.len() || i_histogram < histograms.len() {
        let cur_h: Option<&FloatHistogram>;
        if i_histogram >= histograms.len() || (i_float < floats.len() && floats[i_float].t < histograms[i_histogram].t) {
            cur_f = floats[i_float].f;
            cur_h = None;
            i_float += 1;
        } else if i_float >= floats.len() || (i_histogram < histograms.len() && floats[i_float].t > histograms[i_histogram].t) {
            cur_h = Some(&histograms[i_histogram].h);
            i_histogram += 1;
        } else {
            // A float and a histogram at the same timestamp (Go would loop forever).
            break;
        }
        // Skip the comparison for the first sample, just initialize prevSample.
        if i_float + i_histogram == 1 + first_float + first_histogram {
            (prev_f, prev_h) = (cur_f, cur_h);
            continue;
        }
        match (prev_h, cur_h) {
            (None, None) => {
                if cur_f != prev_f && !(cur_f.is_nan() && prev_f.is_nan()) {
                    changes += 1;
                }
            }
            (Some(ph), Some(ch)) => {
                if !ch.equals(ph) {
                    changes += 1;
                }
            }
            _ => changes += 1,
        }
        (prev_f, prev_h) = (cur_f, cur_h);
    }
    push_float(enh, changes as f64);
    Ok(Annotations::new())
}

/// Go `model.UTF8Validation.IsValidLabelName`.
#[inline]
fn is_valid_label_name(name: &str) -> bool {
    !name.is_empty()
}

/// Go `evalLabelReplace`: label_replace operates only on series; it does not look at timestamps or values.
pub(crate) fn eval_label_replace(ev: &mut Evaluator<'_>, args: &[Expr]) -> Result<(Value, Annotations), Error> {
    let dst = string_from_arg(&args[1])?;
    let repl = string_from_arg(&args[2])?;
    let src = string_from_arg(&args[3])?;
    let regex_str = string_from_arg(&args[4])?;

    let regex = match Regexp::compile(&format!("^(?s:{})$", regex_str)) {
        Ok(r) => r,
        Err(_) => return Err(Error::msg(format!("invalid regular expression in label_replace(): {}", regex_str))),
    };
    if !is_valid_label_name(dst) {
        return Err(Error::msg(format!("invalid destination label name in label_replace(): {}", dst)));
    }

    let (mut matrix, ws) = ev.eval_matrix(&args[0])?;
    let mut lb = Builder::new(Labels::empty());
    let mut res = String::new();
    for el in matrix.iter_mut() {
        let src_val = el.metric.get(src);
        if let Some(indexes) = regex.find_string_submatch_index(src_val) {
            // Only replace when regexp matches.
            res.clear();
            regex.expand_string(&mut res, repl, src_val, &indexes);
            lb.reset(el.metric.clone());
            lb.set(dst, &res);
            el.metric = lb.labels();
            if dst == METRIC_NAME {
                el.drop_name = false;
            }
        }
    }
    let m = ev.merge_series_with_same_labelset(matrix)?;
    Ok((Value::Matrix(m), ws))
}

/// === Vector(s Scalar) (Vector, Annotations) ===
fn func_vector(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    push_float(enh, vals[0][0].f);
    Ok(Annotations::new())
}

/// Go `evalLabelJoin`: label_join operates only on series; it does not look at timestamps or values.
pub(crate) fn eval_label_join(ev: &mut Evaluator<'_>, args: &[Expr]) -> Result<(Value, Annotations), Error> {
    let dst = string_from_arg(&args[1])?;
    let sep = string_from_arg(&args[2])?;
    let mut src_labels: Vec<&str> = Vec::with_capacity(args.len().saturating_sub(3));
    for a in &args[3..] {
        let src = string_from_arg(a)?;
        if !is_valid_label_name(src) {
            return Err(Error::msg(format!("invalid source label name in label_join(): {}", src)));
        }
        src_labels.push(src);
    }
    if !is_valid_label_name(dst) {
        return Err(Error::msg(format!("invalid destination label name in label_join(): {}", dst)));
    }

    let (mut matrix, ws) = ev.eval_matrix(&args[0])?;
    let mut lb = Builder::new(Labels::empty());
    let mut strval = String::new();
    for el in matrix.iter_mut() {
        strval.clear();
        for (i, src) in src_labels.iter().enumerate() {
            if i > 0 {
                strval.push_str(sep);
            }
            strval.push_str(el.metric.get(src));
        }
        lb.reset(el.metric.clone());
        lb.set(dst, &strval);
        el.metric = lb.labels();
        if dst == METRIC_NAME {
            el.drop_name = false;
        }
    }
    let m = ev.merge_series_with_same_labelset(matrix)?;
    Ok((Value::Matrix(m), ws))
}

// ---------------------------------------------------------------------------------------------------------------
// Date functions (Go `time` package arithmetic in UTC, with Go's wrapping integer behavior).

const SECONDS_PER_DAY: u64 = 86400;
const ABSOLUTE_YEARS: u64 = 292277022400;
const MARCH_THRU_DECEMBER: i64 = 306;
/// Go `unixToInternal + internalToAbsolute`.
const UNIX_TO_ABSOLUTE: i64 = 9223372028741760000;

/// The absolute seconds of a Unix time in UTC (Go `Time.absSec` of `time.Unix(sec, 0).UTC()`).
#[inline]
fn abs_sec(unix: i64) -> u64 {
    unix.wrapping_add(UNIX_TO_ABSOLUTE) as u64
}

/// Go `absDays.split`: (century, cyear, ayday).
fn abs_days_split(days: u64) -> (u64, i64, i64) {
    let d = days.wrapping_mul(4).wrapping_add(3);
    let century = d / 146097;
    let cd = ((d % 146097) as u32) | 3;
    let prod = 2939745u64 * cd as u64;
    let hi = (prod >> 32) as u32;
    let lo = prod as u32;
    (century, hi as i64, (lo / 2939745 / 4) as i64)
}

/// Go `absYday.split`: (month, mday).
fn ayday_split(ayday: i64) -> (i64, i64) {
    let d = 2141u32.wrapping_mul(ayday as u32).wrapping_add(197913);
    ((d >> 16) as i64, 1 + ((d & 0xFFFF) / 2141) as i64)
}

#[inline]
fn jan_feb(ayday: i64) -> i64 {
    if ayday >= MARCH_THRU_DECEMBER { 1 } else { 0 }
}

#[inline]
fn century_year(century: u64, cyear: i64, jan_feb: i64) -> i64 {
    (century.wrapping_mul(100).wrapping_sub(ABSOLUTE_YEARS) as i64).wrapping_add(cyear).wrapping_add(jan_feb)
}

#[inline]
fn century_leap(century: u64, cyear: i64) -> i64 {
    let y4ok = (cyear % 4 == 0) as i64;
    let y100ok = (cyear != 0) as i64;
    let y400ok = (century % 4 == 0) as i64;
    y4ok & (y100ok | y400ok)
}

/// Go `dateToAbsDays`.
fn date_to_abs_days(year: i64, month: i64, day: i64) -> u64 {
    let mut amonth = month as u32;
    let jan_feb: u32 = if amonth < 3 { 1 } else { 0 };
    amonth = amonth.wrapping_add(12 * jan_feb);
    let y = (year as u64).wrapping_sub(jan_feb as u64).wrapping_add(ABSOLUTE_YEARS);
    let ayday = 979u32.wrapping_mul(amonth).wrapping_sub(2919) >> 5;
    let century = y / 100;
    let cyear = (y % 100) as u32;
    let cday = 1461 * cyear / 4;
    let centurydays = 146097u64.wrapping_mul(century) / 4;
    centurydays.wrapping_add(((cday.wrapping_add(ayday)) as i64).wrapping_add(day).wrapping_sub(1) as u64)
}

fn go_year(abs: u64) -> i64 {
    let (century, cyear, ayday) = abs_days_split(abs / SECONDS_PER_DAY);
    century_year(century, cyear, jan_feb(ayday))
}

fn go_month(abs: u64) -> i64 {
    let (_, _, ayday) = abs_days_split(abs / SECONDS_PER_DAY);
    let (m, _) = ayday_split(ayday);
    m - jan_feb(ayday) * 12
}

fn go_day(abs: u64) -> i64 {
    let (_, _, ayday) = abs_days_split(abs / SECONDS_PER_DAY);
    ayday_split(ayday).1
}

fn go_weekday(abs: u64) -> i64 {
    ((abs / SECONDS_PER_DAY).wrapping_add(3) % 7) as i64
}

fn go_year_day(abs: u64) -> i64 {
    let (century, cyear, ayday) = abs_days_split(abs / SECONDS_PER_DAY);
    let jf = jan_feb(ayday);
    let leap = century_leap(century, cyear);
    ayday + (1 + 31 + 28) + (leap & !jf) - 365 * jf
}

fn go_days_in_month(abs: u64) -> i64 {
    // Go: 32 - time.Date(t.Year(), t.Month(), 32, 0, 0, 0, 0, time.UTC).Day().
    let days = date_to_abs_days(go_year(abs), go_month(abs), 32);
    let unix = (days as i64).wrapping_mul(SECONDS_PER_DAY as i64).wrapping_sub(UNIX_TO_ABSOLUTE);
    32 - go_day(abs_sec(unix))
}

/// Go `int64(f)` on amd64: NaN and out-of-range values give math.MinInt64.
#[inline]
fn go_f64_to_i64(f: f64) -> i64 {
    if (-9223372036854775808.0..9223372036854775808.0).contains(&f) { f as i64 } else { i64::MIN }
}

/// Go `dateWrapper`.
fn date_wrapper(vals: &[Vector], enh: &mut EvalNodeHelper, f: fn(u64) -> i64) -> Result<Annotations, Error> {
    if vals.is_empty() {
        let v = f(abs_sec(enh.ts / 1000)) as f64;
        push_float(enh, v);
        return Ok(Annotations::new());
    }
    let delayed = enh.enable_delayed_name_removal;
    for el in &vals[0] {
        if el.h.is_some() {
            // Ignore histogram sample.
            continue;
        }
        let t = abs_sec(go_f64_to_i64(el.f));
        let metric = if !delayed { drop_metadata(&el.metric) } else { el.metric.clone() };
        enh.out.push(Sample { metric, f: f(t) as f64, drop_name: true, ..Default::default() });
    }
    Ok(Annotations::new())
}

/// === days_in_month(v Vector) Scalar ===
fn func_days_in_month(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    date_wrapper(vals, enh, go_days_in_month)
}

/// === day_of_month(v Vector) Scalar ===
fn func_day_of_month(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    date_wrapper(vals, enh, go_day)
}

/// === day_of_week(v Vector) Scalar ===
fn func_day_of_week(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    date_wrapper(vals, enh, go_weekday)
}

/// === day_of_year(v Vector) Scalar ===
fn func_day_of_year(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    date_wrapper(vals, enh, go_year_day)
}

/// === hour(v Vector) Scalar ===
fn func_hour(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    date_wrapper(vals, enh, |abs| ((abs % SECONDS_PER_DAY) / 3600) as i64)
}

/// === minute(v Vector) Scalar ===
fn func_minute(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    date_wrapper(vals, enh, |abs| ((abs % 3600) / 60) as i64)
}

/// === month(v Vector) Scalar ===
fn func_month(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    date_wrapper(vals, enh, go_month)
}

/// === year(v Vector) Scalar ===
fn func_year(vals: &[Vector], _: &[Series], _: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error> {
    date_wrapper(vals, enh, go_year)
}

// ---------------------------------------------------------------------------------------------------------------
// Helpers the engine calls.

/// Go `createLabelsForAbsentFunction`: the labels that are uniquely and exactly matched in the expression.
pub fn create_labels_for_absent_function(expr: &Expr) -> Labels {
    let lm = match expr {
        Expr::VectorSelector(vs) => &vs.label_matchers,
        Expr::MatrixSelector(ms) => match ms.vector_selector.as_vector_selector() {
            Some(vs) => &vs.label_matchers,
            None => return Labels::empty(),
        },
        _ => return Labels::empty(),
    };
    let mut b = Builder::new(Labels::empty());
    // The 'has' set implements backwards-compatibility for historic behaviour: in
    // `absent(x{job="a",job="b",foo="bar"})` `job` is removed from the output.
    let mut has: Vec<&str> = Vec::with_capacity(lm.len());
    for ma in lm {
        if schema::is_metadata_label(&ma.name) {
            continue;
        }
        if ma.typ == MatchType::Equal && !has.contains(&ma.name.as_str()) {
            b.set(&ma.name, &ma.value);
            has.push(&ma.name);
        } else {
            b.del(&ma.name);
        }
    }
    b.labels()
}

/// Go `extendFloats` (in engine.go): extends the floats of an anchored or smoothed selector to `mint` and
/// `maxt`.
pub fn extend_floats(
    floats: &[FPoint],
    start_timestamps: &[i64],
    mint: i64,
    maxt: i64,
    lookback_start: i64,
    smoothed: bool,
) -> Vec<FPoint> {
    // Nothing to extend.
    if floats.is_empty() {
        return Vec::new();
    }
    let mut last_sample_index = floats.len() as isize - 1;
    let n = last_sample_index as usize;
    let mut first_sample_index = (gosort::search(n, |i| floats[i].t > mint) as isize - 1).max(0);
    if smoothed {
        last_sample_index = gosort::search(n, |i| floats[i].t >= maxt) as isize;
    }
    if floats[last_sample_index as usize].t <= mint {
        return Vec::new();
    }

    // TODO: detect if the sample is a counter, based on __type__ or metadata.
    let (left, _) = pick_or_interpolate_left(
        floats,
        start_timestamps,
        first_sample_index as usize,
        mint,
        lookback_start,
        smoothed,
        false,
    );
    let (right, _) = pick_or_interpolate_right(
        floats,
        start_timestamps,
        last_sample_index as usize,
        maxt,
        lookback_start,
        smoothed,
        false,
    );

    // Filter out samples at boundaries or outside the range.
    if floats[first_sample_index as usize].t <= mint {
        first_sample_index += 1;
    }
    if floats[last_sample_index as usize].t >= maxt {
        last_sample_index -= 1;
    }

    let count = (last_sample_index - first_sample_index + 1).max(0) as usize;
    let mut out = Vec::with_capacity(count + 2);
    out.push(FPoint { t: mint, f: left });
    if count > 0 {
        out.extend_from_slice(&floats[first_sample_index as usize..(last_sample_index + 1) as usize]);
    }
    out.push(FPoint { t: maxt, f: right });
    out
}
