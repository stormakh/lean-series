//! Port of `promql/quantile.go`: quantiles of classic and native histograms.

use model::histogram::{Bucket as HBucket, BucketIterator, FloatHistogram};
use model::labels::{Labels, BUCKET_LABEL};
use storage::annotations::{self as annos, PositionRange};
use storage::Annotations;

use crate::functions::gomath;
use crate::gosort;

/// The threshold for relative deltas between classic histogram buckets that histogram_quantile ignores
/// (Go `smallDeltaTolerance`).
pub const SMALL_DELTA_TOLERANCE: f64 = 1e-12;

/// The labels to exclude from the signature of quantiles (Go `excludedLabels`).
pub const EXCLUDED_LABELS: [&str; 1] = [BUCKET_LABEL];

/// A classic histogram bucket (Go `Bucket`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Bucket {
    pub upper_bound: f64,
    pub count: f64,
}

/// Go `Buckets`.
pub type Buckets = Vec<Bucket>;

/// A classic histogram: the metric without `le` and its buckets (Go `metricWithBuckets`).
#[derive(Clone, Debug, Default)]
pub struct MetricWithBuckets {
    pub metric: Labels,
    pub buckets: Buckets,
}

fn sort_buckets(buckets: &mut [Bucket]) {
    gosort::sort_func(buckets, |a, b| {
        // We don't expect the bucket boundary to be a NaN.
        if a.upper_bound < b.upper_bound {
            std::cmp::Ordering::Less
        } else if a.upper_bound > b.upper_bound {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
}

/// The result of [`bucket_quantile`]: the quantile and the extra info of
/// [`ensure_monotonic_and_ignore_small_deltas`].
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct BucketQuantileResult {
    pub quantile: f64,
    pub forced_monotonic: bool,
    pub fixed_precision: bool,
    pub min_bucket: f64,
    pub max_bucket: f64,
    pub max_diff: f64,
}

/// Go `BucketQuantile`: the quantile `q` of the classic histogram buckets (sorted in place by this function).
pub fn bucket_quantile(q: f64, buckets: &mut [Bucket]) -> BucketQuantileResult {
    let mut r = BucketQuantileResult::default();
    if q.is_nan() {
        r.quantile = f64::NAN;
        return r;
    }
    if q < 0.0 {
        r.quantile = f64::NEG_INFINITY;
        return r;
    }
    if q > 1.0 {
        r.quantile = f64::INFINITY;
        return r;
    }
    if buckets.is_empty() {
        r.quantile = f64::NAN;
        return r;
    }
    sort_buckets(buckets);
    if buckets[buckets.len() - 1].upper_bound != f64::INFINITY {
        r.quantile = f64::NAN;
        return r;
    }
    let n = coalesce_buckets(buckets);
    let buckets = &mut buckets[..n];
    let (forced, fixed, min_bucket, max_bucket, max_diff) =
        ensure_monotonic_and_ignore_small_deltas(buckets, SMALL_DELTA_TOLERANCE);
    r.forced_monotonic = forced;
    r.fixed_precision = fixed;
    r.min_bucket = min_bucket;
    r.max_bucket = max_bucket;
    r.max_diff = max_diff;

    if buckets.len() < 2 {
        r.quantile = f64::NAN;
        return r;
    }
    let observations = buckets[buckets.len() - 1].count;
    if observations == 0.0 {
        r.quantile = f64::NAN;
        return r;
    }
    let mut rank = q * observations;
    let b = gosort::search(buckets.len() - 1, |i| buckets[i].count >= rank);

    r.quantile = if b == buckets.len() - 1 {
        buckets[buckets.len() - 2].upper_bound
    } else if b == 0 && buckets[0].upper_bound <= 0.0 {
        buckets[0].upper_bound
    } else {
        let mut bucket_start = 0.0;
        let bucket_end = buckets[b].upper_bound;
        let mut count = buckets[b].count;
        if b > 0 {
            bucket_start = buckets[b - 1].upper_bound;
            count -= buckets[b - 1].count;
            rank -= buckets[b - 1].count;
        }
        bucket_start + (bucket_end - bucket_start) * (rank / count)
    };
    r
}

/// Go `HistogramQuantile`: the quantile `q` of the native histogram `h`. The annotations are about `h`, so `pos`
/// is the position of the expression `h` was derived from.
pub fn histogram_quantile(q: f64, h: &FloatHistogram, metric_name: &str, pos: PositionRange) -> (f64, Annotations) {
    let mut annos = Annotations::new();
    if q < 0.0 {
        return (f64::NEG_INFINITY, annos);
    }
    if q > 1.0 {
        return (f64::INFINITY, annos);
    }
    if h.count == 0.0 || q.is_nan() {
        return (f64::NAN, annos);
    }

    let forward = h.sum.is_nan() || q < 0.5;
    let mut it = if forward { h.all_bucket_iterator() } else { h.all_reverse_bucket_iterator() };
    let mut rank = if forward { q * h.count } else { (1.0 - q) * h.count };
    let mut bucket: HBucket<f64> = HBucket::default();
    let mut count = 0.0;

    while it.next() {
        bucket = it.at();
        if bucket.count == 0.0 {
            continue;
        }
        count += bucket.count;
        if count >= rank {
            break;
        }
    }
    if !h.uses_custom_buckets() && bucket.lower < 0.0 && bucket.upper > 0.0 {
        if h.negative_buckets.is_empty() && !h.positive_buckets.is_empty() {
            // The result is in the zero bucket and the histogram has only positive buckets. So we consider 0 to
            // be the lower bound.
            bucket.lower = 0.0;
        } else if h.positive_buckets.is_empty() && !h.negative_buckets.is_empty() {
            // The result is in the zero bucket and the histogram has only negative buckets. So we consider 0 to
            // be the upper bound.
            bucket.upper = 0.0;
        }
    } else if h.uses_custom_buckets() {
        if bucket.lower == f64::NEG_INFINITY {
            // First bucket, with lower bound -Inf.
            if bucket.upper <= 0.0 {
                return (bucket.upper, annos);
            }
            bucket.lower = 0.0;
        } else if bucket.upper == f64::INFINITY {
            // Last bucket, with upper bound +Inf.
            return (bucket.lower, annos);
        }
    }
    // Due to numerical inaccuracies, we could end up with a higher count than h.Count.
    if count > h.count {
        count = h.count;
    }
    // We could have hit the highest bucket without even reaching the rank (NaN observations).
    if count < rank {
        if h.sum.is_nan() {
            annos.add(annos::new_native_histogram_quantile_nan_result_info(metric_name, pos));
            return (f64::NAN, annos);
        }
        return (bucket.upper, annos);
    }

    if forward {
        rank -= count - bucket.count;
    } else {
        rank = count - rank;
    }
    if h.sum.is_nan() {
        // Detect if h.Count is greater than sum of buckets. Preserve the selected bucket for interpolation below.
        while it.next() {
            count += it.at().count;
        }
        if count < h.count {
            annos.add(annos::new_native_histogram_quantile_nan_skew_info(metric_name, pos));
        }
    }

    // The fraction of how far we are into the current bucket.
    let fraction = rank / bucket.count;

    // Return linear interpolation for custom buckets and for quantiles that end up in the zero bucket.
    if h.uses_custom_buckets() || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
        return (bucket.lower + (bucket.upper - bucket.lower) * fraction, annos);
    }

    // For exponential buckets, we interpolate on a logarithmic scale.
    let log_lower = gomath::log2(bucket.lower.abs());
    let log_upper = gomath::log2(bucket.upper.abs());
    if bucket.lower > 0.0 {
        // Positive bucket.
        return (gomath::exp2(log_lower + (log_upper - log_lower) * fraction), annos);
    }
    // Otherwise, we are in a negative bucket and have to mirror things.
    (-gomath::exp2(log_upper + (log_lower - log_upper) * (1.0 - fraction)), annos)
}

/// Go `HistogramFraction`: the fraction of observations of `h` between `lower` and `upper`.
pub fn histogram_fraction(
    lower: f64,
    upper: f64,
    h: &FloatHistogram,
    metric_name: &str,
    pos: PositionRange,
) -> (f64, Annotations) {
    let mut annos = Annotations::new();
    if h.count == 0.0 || lower.is_nan() || upper.is_nan() {
        return (f64::NAN, annos);
    }
    if lower >= upper {
        return (0.0, annos);
    }

    let (mut count, mut rank, mut lower_rank, mut upper_rank) = (0.0, 0.0, 0.0, 0.0);
    let (mut lower_set, mut upper_set) = (false, false);
    let mut it = h.all_bucket_iterator();
    let custom = h.uses_custom_buckets();
    while it.next() {
        let mut b = it.at();
        count += b.count;
        let mut zero_bucket = false;

        // Linear interpolation, used for custom buckets and the zero bucket.
        let interpolate_linearly = |b: &HBucket<f64>, rank: f64, v: f64| -> f64 {
            if b.lower == f64::NEG_INFINITY {
                return b.count;
            }
            rank + b.count * b.fraction_below(v, true)
        };
        // Interpolation on a logarithmic scale, a better fit for exponential bucketing.
        let interpolate_exponentially =
            |b: &HBucket<f64>, rank: f64, v: f64| -> f64 { rank + b.count * b.fraction_below(v, false) };

        if custom {
            // Custom buckets have no zero bucket. Only the first bucket has a lower bound of -Inf, and 0 is its
            // lower bound if its upper bound is positive.
            if b.lower == f64::NEG_INFINITY && b.upper > 0.0 {
                b.lower = 0.0;
            }
        } else if b.lower <= 0.0 && b.upper >= 0.0 {
            zero_bucket = true;
            if h.negative_buckets.is_empty() && !h.positive_buckets.is_empty() {
                b.lower = 0.0;
            } else if h.positive_buckets.is_empty() && !h.negative_buckets.is_empty() {
                b.upper = 0.0;
            }
        }
        if !lower_set && b.lower >= lower {
            // We have hit the lower value at the lower bucket boundary.
            lower_rank = rank;
            lower_set = true;
        }
        if !upper_set && b.lower >= upper {
            // We have hit the upper value at the lower bucket boundary.
            upper_rank = rank;
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        if !lower_set && b.lower < lower && b.upper > lower {
            // The lower value is in this bucket.
            lower_rank = if custom || zero_bucket {
                interpolate_linearly(&b, rank, lower)
            } else {
                interpolate_exponentially(&b, rank, lower)
            };
            lower_set = true;
        }
        if !upper_set && b.lower < upper && b.upper > upper {
            // The upper value is in this bucket.
            upper_rank = if custom || zero_bucket {
                interpolate_linearly(&b, rank, upper)
            } else {
                interpolate_exponentially(&b, rank, upper)
            };
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        rank += b.count;
    }
    if h.sum.is_nan() {
        // There might be NaN observations, so we need to adjust the count to only include non-NaN observations.
        while it.next() {
            count += it.at().count;
        }
        if count < h.count {
            annos.add(annos::new_native_histogram_fraction_nans_info(metric_name, pos));
        }
    } else {
        count = h.count;
    }

    if !lower_set || lower_rank > count {
        lower_rank = count;
    }
    if !upper_set || upper_rank > count {
        upper_rank = count;
    }
    ((upper_rank - lower_rank) / h.count, annos)
}

/// Go `BucketFraction`: [`histogram_fraction`] for classic histograms. Sorts `buckets` in place.
pub fn bucket_fraction(lower: f64, upper: f64, buckets: &mut [Bucket]) -> f64 {
    if buckets.is_empty() {
        return f64::NAN;
    }
    sort_buckets(buckets);
    if buckets[buckets.len() - 1].upper_bound != f64::INFINITY {
        return f64::NAN;
    }
    let n = coalesce_buckets(buckets);
    let buckets = &buckets[..n];

    let count = buckets[buckets.len() - 1].count;
    if count == 0.0 || lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let (mut rank, mut lower_rank, mut upper_rank) = (0.0, 0.0, 0.0);
    let (mut lower_set, mut upper_set) = (false, false);

    // If the upper bound of the first bucket is greater than 0, we assume we are dealing with positive buckets
    // only and lowerBound for the first bucket is set to 0; otherwise it is set to -Inf.
    let mut lower_bound = if buckets[0].upper_bound <= 0.0 { f64::NEG_INFINITY } else { 0.0 };

    for (i, b) in buckets.iter().enumerate() {
        if i > 0 {
            lower_bound = buckets[i - 1].upper_bound;
        }
        let upper_bound = b.upper_bound;

        let interpolate_linearly = |v: f64| -> f64 {
            if lower_bound == f64::NEG_INFINITY {
                return b.count;
            }
            rank + (b.count - rank) * (v - lower_bound) / (upper_bound - lower_bound)
        };

        if !lower_set && lower_bound >= lower {
            lower_rank = rank;
            lower_set = true;
        }
        if !upper_set && lower_bound >= upper {
            upper_rank = rank;
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        if !lower_set && lower_bound < lower && upper_bound > lower {
            lower_rank = interpolate_linearly(lower);
            lower_set = true;
        }
        if !upper_set && lower_bound < upper && upper_bound > upper {
            upper_rank = interpolate_linearly(upper);
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        rank = b.count;
    }
    if !lower_set || lower_rank > count {
        lower_rank = count;
    }
    if !upper_set || upper_rank > count {
        upper_rank = count;
    }
    (upper_rank - lower_rank) / count
}

/// Go `coalesceBuckets`: merges buckets with the same upper bound (the input must be sorted). Returns the new
/// length; the merged buckets are `buckets[..n]`.
pub fn coalesce_buckets(buckets: &mut [Bucket]) -> usize {
    let mut last = buckets[0];
    let mut i = 0;
    for j in 1..buckets.len() {
        let b = buckets[j];
        if b.upper_bound == last.upper_bound {
            last.count += b.count;
        } else {
            buckets[i] = last;
            last = b;
            i += 1;
        }
    }
    buckets[i] = last;
    i + 1
}

/// Go `ensureMonotonicAndIgnoreSmallDeltas`. Returns (forcedMonotonic, fixedPrecision, minBucket, maxBucket,
/// maxDiff).
pub fn ensure_monotonic_and_ignore_small_deltas(buckets: &mut [Bucket], tolerance: f64) -> (bool, bool, f64, f64, f64) {
    let (mut forced_monotonic, mut fixed_precision) = (false, false);
    let mut min_bucket = f64::INFINITY;
    let mut max_bucket = f64::NEG_INFINITY;
    let mut max_diff = 0.0;
    let mut prev = buckets[0].count;
    for i in 1..buckets.len() {
        let curr = buckets[i].count; // Assumed always positive.
        if curr == prev {
            // No correction needed if the counts are identical between buckets.
            continue;
        }
        if almost_equal(prev, curr, tolerance) {
            // Silently correct numerically insignificant differences from floating point precision errors,
            // regardless of direction. Do not update the 'prev' value as we are ignoring the difference.
            buckets[i].count = prev;
            fixed_precision = true;
            continue;
        }
        if curr < prev {
            // Force monotonicity by removing any decreases regardless of magnitude.
            buckets[i].count = prev;
            forced_monotonic = true;
            if buckets[i].upper_bound < min_bucket {
                min_bucket = buckets[i].upper_bound;
            }
            if buckets[i].upper_bound > max_bucket {
                max_bucket = buckets[i].upper_bound;
            }
            let diff = prev - curr;
            if diff > max_diff {
                max_diff = diff;
            }
            continue;
        }
        prev = curr;
    }
    (forced_monotonic, fixed_precision, min_bucket, max_bucket, max_diff)
}

/// Go `util/almost.Equal`: whether `a` and `b` are equal within the relative tolerance `epsilon`.
pub fn almost_equal(a: f64, b: f64, epsilon: f64) -> bool {
    const MIN_NORMAL: f64 = f64::MIN_POSITIVE; // Go: math.Float64frombits(0x0010000000000000).
    // StaleNaN is a special value that is used as staleness maker, and we don't want it to compare equal to any
    // other NaN.
    if model::value::is_stale_nan(a) || model::value::is_stale_nan(b) {
        return model::value::is_stale_nan(a) && model::value::is_stale_nan(b);
    }
    // NaN has no equality but for testing we still want to know whether both values are NaN.
    if a.is_nan() && b.is_nan() {
        return true;
    }
    // Cf. http://floating-point-gui.de/errors/comparison/
    if a == b {
        return true;
    }
    let abs_sum = a.abs() + b.abs();
    let diff = (a - b).abs();
    if a == 0.0 || b == 0.0 || abs_sum < MIN_NORMAL {
        return diff < epsilon * MIN_NORMAL;
    }
    diff / abs_sum.min(f64::MAX) < epsilon
}

/// Go `quantile`: the q-quantile of the values (used by the `quantile` aggregation and `quantile_over_time`).
/// Sorts `values` with Go's `sort.Sort` and `vectorByValueHeap.Less` (NaN first).
pub fn quantile(q: f64, values: &mut [f64]) -> f64 {
    if values.is_empty() || q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    gosort::sort(values, |a, b| a.is_nan() || a < b);

    let n = values.len() as f64;
    // When the quantile lies between two samples, we use a weighted average of the two samples.
    let rank = q * (n - 1.0);
    let lower_index = rank.floor().max(0.0);
    let upper_index = (n - 1.0).min(lower_index + 1.0);
    let weight = rank - rank.floor();
    if weight == 0.0 {
        return values[lower_index as usize];
    }
    values[lower_index as usize] * (1.0 - weight) + values[upper_index as usize] * weight
}
