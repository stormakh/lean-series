//! Port of `histograms.go`: OTLP exponential histograms to native histograms and explicit bucket histograms to
//! native histograms with custom buckets (NHCB).

use model::exemplar;
use model::histogram::{
    CUSTOM_BUCKETS_SCHEMA, CounterResetHint, EXPONENTIAL_SCHEMA_MAX, EXPONENTIAL_SCHEMA_MIN, Histogram, Span,
    custom_values,
};
use model::labels::METRIC_NAME;
use model::value::{STALE_NAN, stale_nan};
use storage::{Annotations, Context, Error};

use super::super::pdata::{AggregationTemporality, ExponentialHistogramDataPoint, HistogramDataPoint};
use super::annotations::{WarningCategory, new_categorized_warning};
use super::helper::{RESERVED_LABEL_NAMES, convert_time_stamp, go_f6};
use super::metrics_to_prw::{AppOpts, PrometheusConverter, Settings};

pub const DEFAULT_ZERO_THRESHOLD: f64 = 1e-128;

impl PrometheusConverter {
    /// Go `addExponentialHistogramDataPoints`: OTLP exponential histogram points as native histogram samples.
    pub fn add_exponential_histogram_data_points(
        &mut self,
        ctx: &Context,
        data_points: &[ExponentialHistogramDataPoint],
        settings: &Settings,
        temporality: AggregationTemporality,
        app_opts: &AppOpts,
    ) -> (Annotations, Result<(), Error>) {
        let mut annots = Annotations::new();
        for pt in data_points {
            if let Err(e) = self.every_n.check_context(ctx) {
                return (annots, Err(e));
            }
            let (hp, ws) = match exponential_to_native_histogram(pt, temporality) {
                Ok(r) => r,
                Err(e) => return (annots, Err(e)),
            };
            annots.merge(ws);
            let r = self.append_native_histogram(ctx, &pt.attributes, &pt.exemplars, pt.timestamp, pt.start_timestamp, &hp, settings, app_opts);
            if let Err(e) = r {
                return (annots, Err(e));
            }
        }
        (annots, Ok(()))
    }

    #[allow(clippy::too_many_arguments)]
    fn append_native_histogram(
        &mut self,
        ctx: &Context,
        attributes: &super::super::pdata::Map,
        exemplars: &[super::super::pdata::Exemplar],
        timestamp: u64,
        start_timestamp: u64,
        hp: &Histogram,
        settings: &Settings,
        app_opts: &AppOpts,
    ) -> Result<(), Error> {
        let lbls = self.create_attributes(
            attributes,
            settings,
            RESERVED_LABEL_NAMES,
            true,
            &app_opts.metadata,
            &[METRIC_NAME, &app_opts.metric_family_name],
        )?;
        let ts = convert_time_stamp(timestamp);
        let st = convert_time_stamp(start_timestamp);
        let mut exemplars = self.get_prom_exemplars(ctx, exemplars)?;
        // Appenders need exemplars sorted by timestamp.
        exemplars.sort_by(|a, b| exemplar::compare(a, b).cmp(&0));
        self.append(&lbls, st, ts, 0.0, Some(hp), app_opts, &exemplars)
    }

    /// Go `addCustomBucketsHistogramDataPoints`: OTLP explicit bucket histogram points as NHCB samples.
    pub fn add_custom_buckets_histogram_data_points(
        &mut self,
        ctx: &Context,
        data_points: &[HistogramDataPoint],
        settings: &Settings,
        temporality: AggregationTemporality,
        app_opts: &AppOpts,
    ) -> (Annotations, Result<(), Error>) {
        let mut annots = Annotations::new();
        for pt in data_points {
            if let Err(e) = self.every_n.check_context(ctx) {
                return (annots, Err(e));
            }
            let (hp, ws) = explicit_histogram_to_custom_buckets_histogram(pt, temporality);
            annots.merge(ws);
            let r = self.append_native_histogram(ctx, &pt.attributes, &pt.exemplars, pt.timestamp, pt.start_timestamp, &hp, settings, app_opts);
            if let Err(e) = r {
                return (annots, Err(e));
            }
        }
        (annots, Ok(()))
    }
}

/// The reset hint: unknown (always safe), or gauge for delta temporality to avoid needless chunk cuts.
fn reset_hint(temporality: AggregationTemporality) -> CounterResetHint {
    if temporality == AggregationTemporality::DELTA {
        CounterResetHint::GaugeType
    } else {
        CounterResetHint::UnknownCounterReset
    }
}

/// Go `exponentialToNativeHistogram`.
pub fn exponential_to_native_histogram(
    p: &ExponentialHistogramDataPoint,
    temporality: AggregationTemporality,
) -> Result<(Histogram, Annotations), Error> {
    let mut annots = Annotations::new();
    let mut scale = p.scale;
    if scale < EXPONENTIAL_SCHEMA_MIN {
        return Err(Error::msg(format!(
            "cannot convert exponential to native histogram. Scale must be >= {}, was {}",
            EXPONENTIAL_SCHEMA_MIN, scale
        )));
    }
    let mut scale_down = 0;
    if scale > EXPONENTIAL_SCHEMA_MAX {
        scale_down = scale - EXPONENTIAL_SCHEMA_MAX;
        scale = EXPONENTIAL_SCHEMA_MAX;
    }
    let (p_spans, p_deltas) = convert_buckets_layout(&p.positive.bucket_counts, p.positive.offset, scale_down, true);
    let (n_spans, n_deltas) = convert_buckets_layout(&p.negative.bucket_counts, p.negative.offset, scale_down, true);

    let mut h = Histogram {
        counter_reset_hint: reset_hint(temporality),
        schema: scale,
        // TODO upstream: use zero_threshold, if set.
        zero_threshold: DEFAULT_ZERO_THRESHOLD,
        zero_count: p.zero_count,
        positive_spans: p_spans,
        positive_buckets: p_deltas,
        negative_spans: n_spans,
        negative_buckets: n_deltas,
        ..Default::default()
    };
    if p.flags.no_recorded_value() {
        h.sum = stale_nan();
        h.count = STALE_NAN;
    } else {
        if let Some(s) = p.sum {
            h.sum = s;
        }
        h.count = p.count;
        if p.count == 0 && h.sum != 0.0 {
            annots.add(new_categorized_warning(
                WarningCategory::HistogramZeroCountNonZeroSum,
                format!("exponential histogram data point has zero count, but non-zero sum: {}", go_f6(h.sum)),
            ));
        }
    }
    Ok((h, annots))
}

/// Go `convertBucketsLayout`: dense OTLP bucket counts to sparse native histogram spans and deltas, merging
/// `2^scale_down` buckets into one. With `adjust_offset` (exponential histograms) the bucket index is shifted by
/// one, because OTLP bucket 0 is (1, base] while Prometheus bucket 0 is (base^-1, 1]. Spans are split at gaps of
/// more than two empty buckets (as client_golang's `makeBuckets`).
pub fn convert_buckets_layout(bucket_counts: &[u64], offset: i32, scale_down: i32, adjust_offset: bool) -> (Vec<Span>, Vec<i64>) {
    if bucket_counts.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let mut spans: Vec<Span> = Vec::new();
    let mut deltas: Vec<i64> = Vec::new();
    let mut count: i64 = 0;
    let mut prev_count: i64 = 0;

    // The index of the bucket being collected in count, and of the last bucket appended.
    let mut bucket_idx: i32 = (offset >> scale_down).wrapping_add(1);
    let mut last_idx: i32 = bucket_idx.wrapping_sub(1);

    let mut initial_offset = offset;
    if adjust_offset {
        initial_offset = (initial_offset >> scale_down).wrapping_add(1);
    }
    spans.push(Span { offset: initial_offset, length: 0 });

    fn append_delta(spans: &mut [Span], deltas: &mut Vec<i64>, prev_count: &mut i64, count: i64) {
        let last = spans.len() - 1;
        spans[last].length += 1;
        deltas.push(count.wrapping_sub(*prev_count));
        *prev_count = count;
    }

    let append_bucket = |spans: &mut Vec<Span>, deltas: &mut Vec<i64>, prev_count: &mut i64, last_idx: &mut i32, bucket_idx: i32, count: i64| {
        let gap = bucket_idx.wrapping_sub(*last_idx).wrapping_sub(1);
        if gap > 2 {
            // A gap of more than two buckets starts a new span.
            spans.push(Span { offset: gap, length: 0 });
        } else {
            // A small gap (or none): fill it with empty buckets.
            for _ in 0..gap {
                append_delta(spans, deltas, prev_count, 0);
            }
        }
        append_delta(spans, deltas, prev_count, count);
        *last_idx = bucket_idx;
    };

    for (i, &c) in bucket_counts.iter().enumerate() {
        let next_bucket_idx = ((i as i32).wrapping_add(offset) >> scale_down).wrapping_add(1);
        if bucket_idx == next_bucket_idx {
            // Not enough buckets collected to merge yet.
            count = count.wrapping_add(c as i64);
            continue;
        }
        // Empty buckets are not appended; the gap before the next bucket covers them.
        if count != 0 {
            append_bucket(&mut spans, &mut deltas, &mut prev_count, &mut last_idx, bucket_idx, count);
        }
        count = c as i64;
        bucket_idx = next_bucket_idx;
    }
    // Always append the last bucket.
    append_bucket(&mut spans, &mut deltas, &mut prev_count, &mut last_idx, bucket_idx, count);
    (spans, deltas)
}

/// Go `explicitHistogramToCustomBucketsHistogram`. OTLP explicit bounds map directly to custom values: both
/// have an implicit +Inf bucket above the last bound.
pub fn explicit_histogram_to_custom_buckets_histogram(
    p: &HistogramDataPoint,
    temporality: AggregationTemporality,
) -> (Histogram, Annotations) {
    let mut annots = Annotations::new();
    let buckets = &p.bucket_counts;
    let offset = get_bucket_offset(buckets);
    let (positive_spans, positive_deltas) = convert_buckets_layout(&buckets[offset..], offset as i32, 0, false);
    let mut h = Histogram {
        counter_reset_hint: reset_hint(temporality),
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans,
        positive_buckets: positive_deltas,
        // pdata's AsRaw gives a nil slice for no bounds.
        custom_values: if p.explicit_bounds.is_empty() { None } else { custom_values(&p.explicit_bounds) },
        ..Default::default()
    };
    if p.flags.no_recorded_value() {
        h.sum = stale_nan();
        h.count = STALE_NAN;
    } else {
        if let Some(s) = p.sum {
            h.sum = s;
        }
        h.count = p.count;
        if p.count == 0 && h.sum != 0.0 {
            annots.add(new_categorized_warning(
                WarningCategory::HistogramZeroCountNonZeroSum,
                format!("histogram data point has zero count, but non-zero sum: {}", go_f6(h.sum)),
            ));
        }
    }
    (h, annots)
}

/// Go `getBucketOffset`: the number of leading empty buckets.
pub fn get_bucket_offset(buckets: &[u64]) -> usize {
    buckets.iter().take_while(|&&b| b == 0).count()
}
