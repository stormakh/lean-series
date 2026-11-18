//! Port of `promql/estimate.go`: an approximation of a query's storage input before execution.
//!
//! Units: timestamps are milliseconds (as the engine's), durations are nanoseconds (Go `time.Duration`, as the
//! AST's durations), so negative and extreme durations behave as in Go.
//!
//! Rust notes:
//! - Go finds the optional sampling capability with a type assertion `q.(storage.CostSamplingQueryable)`.
//!   Rust trait objects cannot do that, so the caller passes the capability as `sampling` (for a
//!   [`storage::Storage`], `storage.as_cost_sampling_queryable()`); `None` is a queryable without it.
//! - Go takes the `parser.Parser` interface; here it is the [`ExprParser`] trait, implemented by [`Parser`].
//! - Sample iterators report `chunkenc::Error`, which cannot carry a storage error. [`iterator_error`] turns the
//!   texts of Go's context errors back into [`ERR_CANCELED`] and [`ERR_DEADLINE_EXCEEDED`], so `errors.Is` checks
//!   on context errors keep working across that boundary.

use std::time::Duration;

use chunkenc::{ChunkIterator, ValueType as ChunkValueType};
use model::matcher::Matcher;
use model::value::is_stale_nan;
use storage::{
    Annotations, ChunkQueryable, Context, CostSamplingQueryable, ERR_CANCELED, ERR_COST_SAMPLING_UNSUPPORTED,
    ERR_DEADLINE_EXCEEDED, Error, Querier, Queryable, SampleAndChunkQueryable, SelectHints,
};

use crate::engine::{DEFAULT_LOOKBACK_DELTA, EvalStmt, get_time_ranges_for_selector, preprocess_expr,
    subquery_evaluation_times};
use crate::histogram_stats_iterator::HistogramStatsIterator;
use crate::parser::{self, Expr, Parser, ValueType, documented_type};

const NANOSECOND: i64 = 1;
const MILLISECOND: i64 = 1_000_000 * NANOSECOND;
const MINUTE: i64 = 60_000 * MILLISECOND;

/// Go `time.Duration.Milliseconds` (truncates toward zero).
#[inline]
fn ms(ns: i64) -> i64 {
    ns / MILLISECOND
}

/// Parses expressions (the part of Go's `parser.Parser` interface that `EstimateCost` uses).
pub trait ExprParser {
    fn parse_expr(&self, input: &str) -> Result<Expr, parser::Error>;
}

impl ExprParser for Parser {
    fn parse_expr(&self, input: &str) -> Result<Expr, parser::Error> {
        Parser::parse_expr(self, input)
    }
}

/// Approximates a query's storage input before execution (Go `CostEstimate`). Both values may overestimate or
/// underestimate the actual cost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CostEstimate {
    /// Estimated series reads, summed across explicit selectors. Series shared by selectors may be counted more
    /// than once. Runtime-dependent selections such as `info()` are omitted and produce an incomplete-estimate
    /// warning.
    pub series_touched: i64,
    /// Estimated input in the engine's sample units: one per float and `(FloatHistogram.size()+8)/16` per native
    /// histogram. Histogram size and sample density are measured from a bounded sample of the matching data.
    pub samples_read: i64,
}

/// One selector with the effective time window it reads (Go `selectorWindow`).
struct SelectorWindow<'a> {
    matchers: &'a [Matcher],
    mint: i64,
    maxt: i64,
    range_ms: i64,
    step_ms: i64,
    num_steps: i64,
    is_range: bool,
    skip_histogram_buckets: bool,
}

/// Go `EstimateCost`: approximates storage input without evaluating `expr`. The parser and evaluation options
/// must match the engine; non-positive `lookback_delta` and `subquery_default_step` use package defaults. Range
/// queries (`step > 0`) require a scalar or vector. `start` and `end` are milliseconds; `step`, `lookback_delta`,
/// `subquery_default_step` and `scrape_interval` are nanoseconds.
///
/// Series enumeration uses the "series" hint over preprocessed selector windows. `sampling` sources measure
/// density and point size incrementally, examining at most 50 series and, for density, 50 chunks per selector.
/// Unsupported sources or windows use `scrape_interval` and float-sized points with a warning. Arithmetic
/// saturates at `i64::MAX`. Errors return no partial estimate.
#[allow(clippy::too_many_arguments)]
pub fn estimate_cost(
    ctx: &Context,
    q: &dyn Queryable,
    sampling: Option<&dyn CostSamplingQueryable>,
    p: &dyn ExprParser,
    expr: &str,
    start: i64,
    end: i64,
    step: i64,
    lookback_delta: i64,
    subquery_default_step: i64,
    scrape_interval: i64,
) -> (CostEstimate, Annotations, Option<Error>) {
    let mut annos = Annotations::new();
    let res = estimate_cost_inner(
        ctx,
        q,
        sampling,
        p,
        expr,
        start,
        end,
        step,
        lookback_delta,
        subquery_default_step,
        scrape_interval,
        &mut annos,
    );
    let (estimate, err) = match res {
        Ok(e) => (e, None),
        Err(e) => (CostEstimate::default(), Some(e)),
    };
    match cost_estimation_error(ctx, err) {
        None => (estimate, annos, None),
        Some(err) => (CostEstimate::default(), annos, Some(err)),
    }
}

#[allow(clippy::too_many_arguments)]
fn estimate_cost_inner(
    ctx: &Context,
    q: &dyn Queryable,
    sampling: Option<&dyn CostSamplingQueryable>,
    p: &dyn ExprParser,
    expr: &str,
    start: i64,
    end: i64,
    step: i64,
    mut lookback_delta: i64,
    mut subquery_default_step: i64,
    scrape_interval: i64,
    annos: &mut Annotations,
) -> Result<CostEstimate, Error> {
    if let Some(err) = ctx.err() {
        return Err(err);
    }
    if lookback_delta <= 0 {
        lookback_delta = DEFAULT_LOOKBACK_DELTA.as_nanos() as i64;
    }
    if subquery_default_step <= 0 {
        subquery_default_step = DEFAULT_SUBQUERY_STEP;
    }

    let parsed = p.parse_expr(expr).map_err(Error::other);
    if let Some(context_err) = ctx.err() {
        return Err(join(context_err, parsed.err()));
    }
    let parsed = parsed?;
    if step > 0 && parsed.value_type() != ValueType::Vector && parsed.value_type() != ValueType::Scalar {
        return Err(Error::msg(format!(
            "invalid expression type \"{}\" for range query, must be Scalar or instant Vector",
            documented_type(parsed.value_type())
        )));
    }

    // Run the same preprocessing the engine runs before evaluating, so the estimator sees the expression the
    // engine would actually execute.
    let parsed = preprocess_expr(parsed, start, end, ms(step)).map_err(Error::msg);
    if let Some(context_err) = ctx.err() {
        return Err(join(context_err, parsed.err()));
    }
    let parsed = parsed?;

    let stmt = EvalStmt {
        expr: parsed,
        start,
        end,
        interval: ms(step),
        lookback_delta: Duration::from_nanos(lookback_delta as u64),
    };

    // Collect every selector together with the effective time window it reads, mirroring the engine's
    // getTimeRangesForSelector/populateSeries.
    let mut selectors: Vec<SelectorWindow<'_>> = Vec::new();
    let mut eval_range = 0i64;
    let mut incomplete = false;
    let _ = parser::inspect(&stmt.expr, |node, path| -> Result<(), ()> {
        if ctx.err().is_some() {
            return Err(());
        }
        match node {
            Expr::Call(n) => {
                if n.func.name == "info" {
                    incomplete = true;
                }
            }
            Expr::VectorSelector(n) => {
                for parent in path {
                    if let Expr::Call(call) = parent
                        && call.func.name == "info"
                        && call.args.len() > 1
                        && std::ptr::eq(&call.args[1], node)
                    {
                        return Ok(());
                    }
                }
                let (mint, maxt) = get_time_ranges_for_selector(&stmt, n, path, eval_range);
                let mut is_range = eval_range > 0;
                let (mut num_steps, step_ms) = selector_evaluation_steps(&stmt, path, subquery_default_step);
                if n.timestamp.is_some() && is_range {
                    // Range functions reuse an @ selector's window after their first step.
                    num_steps = num_steps.min(1);
                }
                let mut range_ms = ms(eval_range);
                if n.anchored {
                    range_ms = add_saturating_int64(range_ms, ms(lookback_delta));
                } else if n.smoothed {
                    // Smoothed vectors also consume a window, rather than one point.
                    range_ms = add_saturating_int64(range_ms, ms(lookback_delta));
                    range_ms = add_saturating_int64(range_ms, ms(lookback_delta));
                    is_range = true;
                }
                selectors.push(SelectorWindow {
                    matchers: &n.label_matchers,
                    mint,
                    maxt,
                    range_ms,
                    step_ms,
                    num_steps,
                    is_range,
                    skip_histogram_buckets: n.skip_histogram_buckets,
                });
                eval_range = 0;
            }
            Expr::MatrixSelector(n) => eval_range = n.range,
            _ => {}
        }
        Ok(())
    });
    if let Some(err) = ctx.err() {
        return Err(err);
    }

    if incomplete {
        annos.add(Error::msg("query cost estimate is incomplete: info() selects additional series at runtime"));
    }

    if selectors.is_empty() {
        return Ok(CostEstimate::default());
    }

    // Mirror the engine: open a single querier over the union of every selector's window.
    let (mut union_mint, mut union_maxt) = (selectors[0].mint, selectors[0].maxt);
    for sel in &selectors[1..] {
        union_mint = union_mint.min(sel.mint);
        union_maxt = union_maxt.max(sel.maxt);
    }

    let mut sampler: Option<&dyn SampleAndChunkQueryable> = None;
    if let Some(provider) = sampling {
        let (source, supported) = provider.cost_sampling_queryable();
        if supported {
            sampler = source;
        }
    }
    if let Some(err) = ctx.err() {
        return Err(err);
    }

    // The series count uses func "series" so storage can skip chunk decoding.
    let mut querier = q.querier(union_mint, union_maxt)?;
    let res = estimate_selectors(ctx, &*querier, sampler, &selectors, scrape_interval, annos);
    let close_err = querier.close();
    match (res, close_err) {
        (Ok(e), None) => Ok(e),
        (Ok(_), Some(c)) => Err(Error::Join(vec![c].into())),
        (Err(e), c) => Err(join_opt(e, c)),
    }
}

fn estimate_selectors(
    ctx: &Context,
    querier: &dyn Querier,
    sampler: Option<&dyn SampleAndChunkQueryable>,
    selectors: &[SelectorWindow<'_>],
    scrape_interval: i64,
    annos: &mut Annotations,
) -> Result<CostEstimate, Error> {
    let mut estimate = CostEstimate::default();
    for sel in selectors {
        let (series, sa, err) = count_series(ctx, querier, sel.mint, sel.maxt, sel.matchers);
        annos.merge(sa);
        if let Some(err) = err {
            return Err(err);
        }

        estimate.series_touched = add_saturating_int64(estimate.series_touched, series);

        // Sample the real window within a fixed chunk budget.
        let mut eff_interval = scrape_interval;
        let mut avg_point_cost = FALLBACK_AVG_POINT_COST;
        let mut density = SelectorDensity::default();
        match sampler {
            Some(sampler) if series > 0 => {
                let (d, sia, mut err) = sample_selector_density(ctx, sampler, sel.mint, sel.maxt, sel.matchers);
                density = d;
                annos.merge(sia);
                if let Some(e) = &err
                    && !sampling_unsupported(e)
                {
                    return Err(e.clone());
                }
                if err.is_none() {
                    let (cost, sca, e) = sample_avg_point_cost(
                        ctx,
                        sampler,
                        sel.mint,
                        sel.maxt,
                        series,
                        scrape_interval,
                        sel.skip_histogram_buckets,
                        sel.matchers,
                    );
                    avg_point_cost = cost;
                    annos.merge(sca);
                    if let Some(e) = &e
                        && !sampling_unsupported(e)
                    {
                        return Err(e.clone());
                    }
                    err = e;
                }
                if err.is_some() {
                    // A later opening can see a new unsupported layout. Discard both measurements rather than
                    // keeping a partially sampled estimate.
                    density = SelectorDensity::default();
                    avg_point_cost = FALLBACK_AVG_POINT_COST;
                    annos.add(err_cost_sampling_fallback());
                } else if density.interval_ms > 0.0 {
                    eff_interval = (density.interval_ms * MILLISECOND as f64) as i64;
                }
            }
            _ => {
                if series > 0 {
                    annos.add(err_cost_sampling_fallback());
                }
            }
        }

        // Estimate samples per series following the engine's incremental reads, then multiply by the series
        // count and scale by the measured per-point cost.
        let mut selector_samples = mul_saturating_int64(
            series,
            samples_per_series(sel.is_range, sel.range_ms, sel.step_ms, eff_interval, sel.num_steps),
        );
        if sel.is_range && density.complete_series > 0 {
            // A range selector cannot consume more points than exist in its storage window.
            let window_samples =
                scale_saturating_int64(series, density.samples as f64 / density.complete_series as f64);
            selector_samples = selector_samples.min(window_samples);
        }
        selector_samples = scale_saturating_int64(selector_samples, avg_point_cost);
        estimate.samples_read = add_saturating_int64(estimate.samples_read, selector_samples);
    }
    Ok(estimate)
}

static COST_SAMPLING_FALLBACK: storage::error::Sentinel = storage::error::Sentinel::new(
    "query cost estimate uses the configured scrape interval and float-sized points because bounded storage sampling is unavailable",
);

/// Go `errCostSamplingFallback`.
fn err_cost_sampling_fallback() -> Error {
    Error::Sentinel(&COST_SAMPLING_FALLBACK)
}

/// Go `errors.Join(a, b)` with a present `a`.
fn join(a: Error, b: Option<Error>) -> Error {
    match b {
        Some(b) => Error::Join(vec![a, b].into()),
        None => Error::Join(vec![a].into()),
    }
}

/// Go `errors.Join(err, closeErr)` with a present `err`: `err` itself when `close_err` is `None`.
fn join_opt(err: Error, close_err: Option<Error>) -> Error {
    match close_err {
        Some(c) => Error::Join(vec![err, c].into()),
        None => err,
    }
}

/// Go `costEstimationError`.
fn cost_estimation_error(ctx: &Context, err: Option<Error>) -> Option<Error> {
    if let Some(context_err) = ctx.err()
        && !err.as_ref().is_some_and(|e| e.is(&context_err))
    {
        return Some(join(context_err, err));
    }
    err
}

/// Go `samplingUnsupported`.
fn sampling_unsupported(err: &Error) -> bool {
    err.is(&ERR_COST_SAMPLING_UNSUPPORTED) && !err.is(&ERR_CANCELED) && !err.is(&ERR_DEADLINE_EXCEEDED)
}

/// Converts a sample iterator error to a storage error. Context errors come back as their sentinels (see the
/// module notes); other errors keep their text.
pub fn iterator_error(e: &chunkenc::Error) -> Error {
    if let chunkenc::Error::Msg(m) = e {
        if m == ERR_CANCELED.to_string().as_str() {
            return ERR_CANCELED;
        }
        if m == ERR_DEADLINE_EXCEEDED.to_string().as_str() {
            return ERR_DEADLINE_EXCEEDED;
        }
    }
    Error::from(e.clone())
}

/// Go `mulSaturatingInt64`: multiplies two non-negative values, saturating at `i64::MAX`.
pub fn mul_saturating_int64(a: i64, b: i64) -> i64 {
    if a == 0 || b == 0 {
        return 0;
    }
    if a > i64::MAX / b {
        return i64::MAX;
    }
    a * b
}

/// Go `addSaturatingInt64`: adds two non-negative values, saturating at `i64::MAX`.
pub fn add_saturating_int64(a: i64, b: i64) -> i64 {
    if a > i64::MAX - b {
        return i64::MAX;
    }
    a + b
}

/// Go `scaleSaturatingInt64`: multiplies a non-negative value by a non-negative factor, rounding to the nearest
/// integer and saturating at `i64::MAX`. A factor <= 0 leaves the value unchanged.
pub fn scale_saturating_int64(a: i64, factor: f64) -> i64 {
    if a == 0 {
        return 0;
    }
    if factor <= 0.0 {
        return a;
    }
    let scaled = (a as f64 * factor).round();
    if scaled >= i64::MAX as f64 {
        return i64::MAX;
    }
    scaled as i64
}

/// Go `histogramSampleLimit`: the most series whose points are sized per selector.
pub const HISTOGRAM_SAMPLE_LIMIT: i64 = 50;

/// Go `fallbackAvgPointCost`: the per-point cost when sampling finds no in-window points.
pub const FALLBACK_AVG_POINT_COST: f64 = 1.0;

/// Go `histogramSampleWindowMin` (nanoseconds).
pub const HISTOGRAM_SAMPLE_WINDOW_MIN: i64 = 5 * MINUTE;
/// Go `histogramSampleWindowMax` (nanoseconds).
pub const HISTOGRAM_SAMPLE_WINDOW_MAX: i64 = 30 * MINUTE;

/// Go `fallbackSampleWindow`: the narrow window ending at the selector's own `end_ms` that the samplers measure
/// over when the real window is too expensive. It spans eight scrape intervals (nanoseconds), clamped between
/// [`HISTOGRAM_SAMPLE_WINDOW_MIN`] and [`HISTOGRAM_SAMPLE_WINDOW_MAX`].
pub fn fallback_sample_window(end_ms: i64, scrape_interval: i64) -> (i64, i64) {
    // Clamp before multiplying so a very large configured interval cannot overflow.
    let window = HISTOGRAM_SAMPLE_WINDOW_MIN.max(0.max(scrape_interval.min(HISTOGRAM_SAMPLE_WINDOW_MAX / 8)) * 8);
    if end_ms < i64::MIN + ms(window) {
        return (i64::MIN, end_ms);
    }
    (end_ms - ms(window), end_ms)
}

/// Go `chunkSampleLimit`: the most chunks decoded per selector to measure density.
pub const CHUNK_SAMPLE_LIMIT: usize = 50;

/// Go `densitySeriesSampleLimit`: density sampling also bounds series without chunks.
pub const DENSITY_SERIES_SAMPLE_LIMIT: usize = 50;

/// Go `sampleAvgPointCost`: sizes the first usable point of at most [`HISTOGRAM_SAMPLE_LIMIT`] series. Small
/// populations use the selector's real window; larger ones a narrow window near its end.
#[allow(clippy::too_many_arguments)]
fn sample_avg_point_cost(
    ctx: &Context,
    q: &dyn Queryable,
    mint: i64,
    maxt: i64,
    series: i64,
    scrape_interval: i64,
    skip_histogram_buckets: bool,
    matchers: &[Matcher],
) -> (f64, Annotations, Option<Error>) {
    let mut annos = Annotations::new();
    let (cost, err) =
        sample_avg_point_cost_inner(ctx, q, mint, maxt, series, scrape_interval, skip_histogram_buckets, matchers, &mut annos);
    (cost, annos, cost_estimation_error(ctx, err))
}

#[allow(clippy::too_many_arguments)]
fn sample_avg_point_cost_inner(
    ctx: &Context,
    q: &dyn Queryable,
    mint: i64,
    maxt: i64,
    series: i64,
    scrape_interval: i64,
    skip_histogram_buckets: bool,
    matchers: &[Matcher],
    annos: &mut Annotations,
) -> (f64, Option<Error>) {
    if let Some(err) = ctx.err() {
        return (FALLBACK_AVG_POINT_COST, Some(err));
    }
    let (mut q_mint, mut q_maxt) = (mint, maxt);
    if series <= 0 || series > HISTOGRAM_SAMPLE_LIMIT {
        // Too many series (or the count is not yet known): use a narrow window near the selector's end.
        (q_mint, q_maxt) = fallback_sample_window(maxt, scrape_interval);
    } else if q_mint > q_maxt {
        // Guard against a real window with an inverted (overflowed) mint.
        q_mint = q_maxt;
    }

    let mut querier = match q.querier(q_mint, q_maxt) {
        Ok(q) => q,
        Err(e) => return (FALLBACK_AVG_POINT_COST, Some(e)),
    };
    let (cost, err) = sample_points(ctx, &*querier, q_mint, q_maxt, skip_histogram_buckets, matchers, annos);
    let close_err = querier.close();
    match (err, close_err) {
        (None, None) => (cost, None),
        (None, Some(c)) => (cost, Some(Error::Join(vec![c].into()))),
        (Some(e), c) => (cost, Some(join_opt(e, c))),
    }
}

fn sample_points(
    ctx: &Context,
    querier: &dyn Querier,
    q_mint: i64,
    q_maxt: i64,
    skip_histogram_buckets: bool,
    matchers: &[Matcher],
    annos: &mut Annotations,
) -> (f64, Option<Error>) {
    if let Some(err) = ctx.err() {
        return (FALLBACK_AVG_POINT_COST, Some(err));
    }
    // Select without func "series": here actual samples are decoded to size points.
    let hints = SelectHints { start: q_mint, end: q_maxt, ..Default::default() };
    let mut set = querier.select(ctx, false, Some(&hints), matchers);

    let mut sum = 0.0;
    let mut n = 0usize;
    let mut sampled = 0i64;
    while sampled < HISTOGRAM_SAMPLE_LIMIT && set.next() {
        if let Some(err) = ctx.err() {
            return (FALLBACK_AVG_POINT_COST, Some(err));
        }
        sampled += 1;

        let s = set.at();
        let (point, it_err) = if skip_histogram_buckets {
            let mut it = HistogramStatsIterator::new(s.iterator(None));
            (first_point_cost(&mut it), it.err().map(iterator_error))
        } else {
            let mut it = s.iterator(None);
            (first_point_cost(&mut it), it.err().map(iterator_error))
        };
        if let Some(err) = it_err {
            // Go returns only the set's warnings here.
            *annos = set.warnings();
            return (FALLBACK_AVG_POINT_COST, Some(err));
        }
        let Some(cost) = point else {
            continue;
        };
        sum += cost;
        n += 1;
    }

    annos.merge(set.warnings());
    if let Some(err) = set.err() {
        return (FALLBACK_AVG_POINT_COST, Some(err));
    }
    if n == 0 {
        // No in-window points were sampled: fall back to the documented default.
        return (FALLBACK_AVG_POINT_COST, None);
    }
    (sum / n as f64, None)
}

/// Go `firstPointCost`: the cost of the iterator's first point in the engine's sample units, or `None` when the
/// series has no usable first point (empty or a leading stale marker).
fn first_point_cost(it: &mut dyn ChunkIterator) -> Option<f64> {
    match it.next() {
        ChunkValueType::Float => {
            let (_, v) = it.at();
            if is_stale_nan(v) {
                return None;
            }
            Some(1.0)
        }
        ChunkValueType::Histogram => {
            let (_, h) = it.at_histogram(None);
            let fh = h.to_float();
            if is_stale_nan(fh.sum) {
                return None;
            }
            Some(((fh.size() + 8) / 16) as f64)
        }
        ChunkValueType::FloatHistogram => {
            let (_, fh) = it.at_float_histogram(None);
            if is_stale_nan(fh.sum) {
                return None;
            }
            Some(((fh.size() + 8) / 16) as f64)
        }
        _ => None,
    }
}

/// Go `selectorDensity`: a bounded sample of a selector's storage window. Only completely sampled series
/// contribute to `samples` and `complete_series`; partial series can contribute to `interval_ms`.
#[derive(Clone, Copy, Debug, Default)]
struct SelectorDensity {
    interval_ms: f64,
    samples: i64,
    complete_series: i64,
}

/// Go `sampleSelectorDensity`: examines at most [`CHUNK_SAMPLE_LIMIT`] chunks from the real selector window.
fn sample_selector_density(
    ctx: &Context,
    cq: &dyn ChunkQueryable,
    mint: i64,
    maxt: i64,
    matchers: &[Matcher],
) -> (SelectorDensity, Annotations, Option<Error>) {
    let mut annos = Annotations::new();
    let mut density = SelectorDensity::default();
    let err = sample_selector_density_inner(ctx, cq, mint, maxt, matchers, &mut density, &mut annos);
    (density, annos, cost_estimation_error(ctx, err))
}

fn sample_selector_density_inner(
    ctx: &Context,
    cq: &dyn ChunkQueryable,
    mint: i64,
    maxt: i64,
    matchers: &[Matcher],
    density: &mut SelectorDensity,
    annos: &mut Annotations,
) -> Option<Error> {
    if let Some(err) = ctx.err() {
        return Some(err);
    }
    let mut querier = match cq.chunk_querier(mint, maxt) {
        Ok(q) => q,
        Err(e) => return Some(e),
    };
    let err = (|| -> Option<Error> {
        if let Some(err) = ctx.err() {
            return Some(err);
        }
        let hints = SelectHints { start: mint, end: maxt, ..Default::default() };
        let mut set = querier.select(ctx, false, Some(&hints), matchers);
        let mut total_span_ms = 0.0f64;
        let mut total_gaps = 0i64;
        let mut read = 0usize;
        let mut series_read = 0usize;
        while read < CHUNK_SAMPLE_LIMIT && series_read < DENSITY_SERIES_SAMPLE_LIMIT && set.next() {
            if let Some(err) = ctx.err() {
                *annos = set.warnings();
                return Some(err);
            }
            series_read += 1;
            let mut points = 0i64;
            let mut last_max = 0i64;
            let mut have_max = false;
            let mut it = set.at().iterator(None);
            while read < CHUNK_SAMPLE_LIMIT && it.next() {
                if let Some(err) = ctx.err() {
                    *annos = set.warnings();
                    return Some(err);
                }
                read += 1;
                let meta = it.at();
                let Some(chunk) = &meta.chunk else {
                    continue;
                };
                if chunk.num_samples() == 0 {
                    continue;
                }
                let n = chunk.num_samples() as i64;
                points += n;
                if have_max && meta.min_time > last_max {
                    total_gaps += 1;
                    total_span_ms += meta.min_time as f64 - last_max as f64;
                }
                if n > 1 && meta.max_time > meta.min_time {
                    total_gaps += n - 1;
                    total_span_ms += meta.max_time as f64 - meta.min_time as f64;
                }
                last_max = meta.max_time;
                have_max = true;
            }
            if let Some(err) = it.err() {
                *annos = set.warnings();
                return Some(err);
            }
            if read < CHUNK_SAMPLE_LIMIT && points > 0 {
                density.samples += points;
                density.complete_series += 1;
            }
        }
        *annos = set.warnings();
        if let Some(err) = set.err() {
            return Some(err);
        }
        if total_gaps > 0 && total_span_ms > 0.0 {
            density.interval_ms = total_span_ms / total_gaps as f64;
        }
        None
    })();
    let close_err = querier.close();
    match (err, close_err) {
        (None, None) => None,
        (None, Some(c)) => Some(Error::Join(vec![c].into())),
        (Some(e), c) => Some(join_opt(e, c)),
    }
}

/// Go `countSeries`: enumerates matching series through the shared querier with the "series" hint. Enumeration
/// is not capped; cancellation returns an error rather than a partial count.
fn count_series(
    ctx: &Context,
    querier: &dyn Querier,
    mint: i64,
    maxt: i64,
    matchers: &[Matcher],
) -> (i64, Annotations, Option<Error>) {
    let (count, annos, err) = (|| {
        if let Some(err) = ctx.err() {
            return (0, Annotations::new(), Some(err));
        }
        let hints = SelectHints { start: mint, end: maxt, func: "series".to_string(), ..Default::default() };
        let mut set = querier.select(ctx, false, Some(&hints), matchers);
        let mut count = 0i64;
        while set.next() {
            if let Some(err) = ctx.err() {
                return (0, set.warnings(), Some(err));
            }
            count += 1;
        }
        let annos = set.warnings();
        if let Some(err) = set.err() {
            return (0, annos, Some(err));
        }
        (count, annos, None)
    })();
    (count, annos, cost_estimation_error(ctx, err))
}

/// Go `selectorEvaluationSteps`: follows the evaluator boundaries in `path` (subqueries and step-invariant
/// expressions) and returns the number of evaluation steps of a selector and their interval in milliseconds.
fn selector_evaluation_steps(stmt: &EvalStmt, path: &[&Expr], default_step: i64) -> (i64, i64) {
    let (mut start, mut end) = (stmt.start, stmt.end);
    let mut interval = stmt.interval.max(1);
    if stmt.interval <= 0 {
        end = start;
    }
    for node in path {
        match node {
            Expr::StepInvariant(_) => end = start,
            Expr::Subquery(n) => {
                let mut offset = ms(n.original_offset);
                if let Some(ts) = n.timestamp {
                    offset += start - ts;
                }
                let step = if n.step <= 0 { default_step } else { n.step };
                let subq_interval = ms(step).max(1);
                (start, end) = subquery_evaluation_times(
                    start,
                    end,
                    interval,
                    offset,
                    ms(n.range),
                    subq_interval,
                    n.timestamp.is_some(),
                );
                interval = subq_interval;
            }
            _ => {}
        }
    }
    if end < start {
        return (0, interval);
    }
    ((end - start) / interval + 1, interval)
}

/// Go `defaultSubqueryStep` (nanoseconds): the subquery resolution when the caller passes a non-positive one.
pub const DEFAULT_SUBQUERY_STEP: i64 = MINUTE;

/// Go `samplesPerSeries`: one full range window followed by incremental reads, or one sample per step for a
/// vector selector.
fn samples_per_series(is_range: bool, range_ms: i64, step_ms: i64, scrape_interval: i64, num_steps: i64) -> i64 {
    if num_steps <= 0 {
        return 0;
    }
    if !is_range {
        return num_steps;
    }
    let per_window = samples_per_window(range_ms, scrape_interval);
    if num_steps == 1 || step_ms <= 0 || ms(scrape_interval) <= 0 {
        return per_window;
    }
    let per_step = (step_ms as f64 / ms(scrape_interval) as f64).min(per_window as f64);
    add_saturating_int64(per_window, scale_saturating_int64(num_steps - 1, per_step))
}

/// Go `samplesPerWindow`: `floor(window/scrapeInterval)+1`, or 1 for a non-positive interval.
fn samples_per_window(mut window_ms: i64, scrape_interval: i64) -> i64 {
    if scrape_interval <= 0 {
        return 1;
    }
    if window_ms < 0 {
        window_ms = 0;
    }
    let interval_ms = ms(scrape_interval);
    if interval_ms <= 0 {
        return 1;
    }
    window_ms / interval_ms + 1
}
