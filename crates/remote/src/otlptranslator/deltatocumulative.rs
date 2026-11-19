//! Port of the OpenTelemetry collector's `deltatocumulativeprocessor` (contrib v0.163.0) with its default
//! configuration (`max_stale` 5m, no stream limit), as the OTLP write handler uses it: delta sums, histograms
//! and exponential histograms are aggregated in memory per stream and passed on as cumulative.
//!
//! Differences from upstream: one mutex guards all streams (upstream locks per stream), stale streams are
//! removed lazily at the next request at most once a minute (upstream runs a background ticker), and the
//! processor's own telemetry is not kept.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::pdata::{
    AggregationTemporality, Buckets, ExponentialHistogramDataPoint, HistogramDataPoint, InstrumentationScope, Map,
    Metric, MetricData, Metrics, NumberDataPoint, NumberValue, Resource, Value,
};

/// Go `maxBuckets` of the exponential histogram adder.
const MAX_BUCKETS: usize = 160;

/// The processor's configuration (Go `Config`).
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// Streams not seen for this long are forgotten.
    pub max_stale: Duration,
    /// The maximum number of tracked streams; points of new streams over the limit are dropped.
    pub max_streams: usize,
}

impl Default for Config {
    /// Go `createDefaultConfig`.
    fn default() -> Config {
        Config { max_stale: Duration::from_secs(5 * 60), max_streams: usize::MAX }
    }
}

#[derive(Clone, Debug)]
enum Last {
    Num(NumberDataPoint),
    Hist(HistogramDataPoint),
    Expo(ExponentialHistogramDataPoint),
}

struct Stream {
    last: Last,
    seen: Instant,
}

struct State {
    streams: HashMap<Vec<u8>, Stream>,
    last_sweep: Instant,
}

/// Go `deltaToCumulativeProcessor`.
pub struct DeltaToCumulative {
    cfg: Config,
    state: Mutex<State>,
}

impl Default for DeltaToCumulative {
    fn default() -> DeltaToCumulative {
        DeltaToCumulative::new(Config::default())
    }
}

/// A point that cannot be aggregated (Go `delta.ErrOlderStart` and `delta.ErrOutOfOrder`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AggregateError {
    OlderStart { start: u64, sample: u64 },
    OutOfOrder { last: u64, sample: u64 },
}

impl DeltaToCumulative {
    pub fn new(cfg: Config) -> DeltaToCumulative {
        DeltaToCumulative { cfg, state: Mutex::new(State { streams: HashMap::new(), last_sweep: Instant::now() }) }
    }

    /// The number of tracked streams.
    pub fn streams(&self) -> usize {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).streams.len()
    }

    /// Go `ConsumeMetrics` up to the call of the next consumer: converts delta points in place to cumulative,
    /// drops points that cannot be aggregated and metrics, scopes and resources left empty. Returns false when no
    /// metric is left (Go then does not call the next consumer).
    pub fn process(&self, md: &mut Metrics) -> bool {
        let now = Instant::now();
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if now.duration_since(st.last_sweep) >= Duration::from_secs(60) {
            st.last_sweep = now;
            let max_stale = self.cfg.max_stale;
            st.streams.retain(|_, s| now.duration_since(s.seen) <= max_stale);
        }
        let max_streams = self.cfg.max_streams;
        let mut key = Vec::new();
        md.resource_metrics.retain_mut(|rm| {
            let resource = &rm.resource;
            rm.scope_metrics.retain_mut(|sm| {
                let scope = &sm.scope;
                sm.metrics.retain_mut(|m| {
                    let Some(temporality) = temporality(m) else { return true };
                    if temporality != AggregationTemporality::DELTA {
                        return true;
                    }
                    let metric_key = metric_identity(resource, scope, m);
                    let streams = &mut st.streams;
                    let remaining = match &mut m.data {
                        MetricData::Sum(s) => {
                            s.data_points.retain_mut(|dp| {
                                stream_key(&mut key, &metric_key, &dp.attributes);
                                keep_point(streams, &key, now, max_streams, |last| aggregate_num(last, dp))
                            });
                            s.aggregation_temporality = AggregationTemporality::CUMULATIVE;
                            s.data_points.len()
                        }
                        MetricData::Histogram(h) => {
                            h.data_points.retain_mut(|dp| {
                                stream_key(&mut key, &metric_key, &dp.attributes);
                                keep_point(streams, &key, now, max_streams, |last| aggregate_hist(last, dp))
                            });
                            h.aggregation_temporality = AggregationTemporality::CUMULATIVE;
                            h.data_points.len()
                        }
                        MetricData::ExponentialHistogram(h) => {
                            h.data_points.retain_mut(|dp| {
                                stream_key(&mut key, &metric_key, &dp.attributes);
                                keep_point(streams, &key, now, max_streams, |last| aggregate_expo(last, dp))
                            });
                            h.aggregation_temporality = AggregationTemporality::CUMULATIVE;
                            h.data_points.len()
                        }
                        _ => return true,
                    };
                    remaining > 0
                });
                !sm.metrics.is_empty()
            });
            !rm.scope_metrics.is_empty()
        });
        md.metric_count() > 0
    }
}

fn stream_key(key: &mut Vec<u8>, metric_key: &[u8], attrs: &Map) {
    key.clear();
    key.extend_from_slice(metric_key);
    write_map(key, attrs);
}

/// Aggregates one point of the stream `key` with `update`; returns whether to keep the point.
fn keep_point(
    streams: &mut HashMap<Vec<u8>, Stream>,
    key: &[u8],
    now: Instant,
    max_streams: usize,
    update: impl FnOnce(Option<&mut Last>) -> (bool, Option<Last>),
) -> bool {
    match streams.get_mut(key) {
        Some(s) => {
            s.seen = now;
            update(Some(&mut s.last)).0
        }
        None => {
            if streams.len() >= max_streams {
                return false;
            }
            let (keep, init) = update(None);
            if let Some(last) = init {
                streams.insert(key.to_vec(), Stream { last, seen: now });
            }
            keep
        }
    }
}

fn temporality(m: &Metric) -> Option<AggregationTemporality> {
    match &m.data {
        MetricData::Sum(s) => Some(s.aggregation_temporality),
        MetricData::Histogram(h) => Some(h.aggregation_temporality),
        MetricData::ExponentialHistogram(h) => Some(h.aggregation_temporality),
        _ => None,
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Stream identity (Go `identity.Stream`): resource attributes, scope name, version and attributes, metric name,
// unit, type, monotonicity and temporality, and the data point attributes. Maps are order-independent.

fn write_str(key: &mut Vec<u8>, s: &str) {
    key.extend_from_slice(&(s.len() as u64).to_le_bytes());
    key.extend_from_slice(s.as_bytes());
}

fn write_value(key: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Empty => key.push(0),
        Value::Str(s) => {
            key.push(1);
            write_str(key, s);
        }
        Value::Bool(b) => key.extend_from_slice(&[2, *b as u8]),
        Value::Int(i) => {
            key.push(3);
            key.extend_from_slice(&i.to_le_bytes());
        }
        Value::Double(f) => {
            key.push(4);
            key.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        Value::Map(m) => {
            key.push(5);
            write_map(key, m);
        }
        Value::Slice(items) => {
            key.push(6);
            key.extend_from_slice(&(items.len() as u64).to_le_bytes());
            for it in items {
                write_value(key, it);
            }
        }
        Value::Bytes(b) => {
            key.push(7);
            key.extend_from_slice(&(b.len() as u64).to_le_bytes());
            key.extend_from_slice(b);
        }
    }
}

fn write_map(key: &mut Vec<u8>, m: &Map) {
    let mut entries: Vec<&(String, Value)> = m.0.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    key.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for (k, v) in entries {
        write_str(key, k);
        write_value(key, v);
    }
}

fn metric_identity(resource: &Resource, scope: &InstrumentationScope, m: &Metric) -> Vec<u8> {
    let mut key = Vec::with_capacity(128);
    write_map(&mut key, &resource.attributes);
    write_str(&mut key, &scope.name);
    write_str(&mut key, &scope.version);
    write_map(&mut key, &scope.attributes);
    write_str(&mut key, &m.name);
    write_str(&mut key, &m.unit);
    let (ty, mono, temp) = match &m.data {
        MetricData::Sum(s) => (2u8, s.is_monotonic, s.aggregation_temporality.0),
        MetricData::Histogram(h) => (3, true, h.aggregation_temporality.0),
        MetricData::ExponentialHistogram(h) => (4, true, h.aggregation_temporality.0),
        _ => (0, false, 0),
    };
    key.push(ty);
    key.push(mono as u8);
    key.extend_from_slice(&temp.to_le_bytes());
    key
}

// ---------------------------------------------------------------------------------------------------------------
// Aggregation (Go `delta.Aggregate` with `data.Adder`).

/// The checks of Go `delta.Aggregate` before adding: start and order.
fn check(state_start: u64, state_ts: u64, dp_start: u64, dp_ts: u64) -> Result<(), AggregateError> {
    if dp_start < state_start {
        return Err(AggregateError::OlderStart { start: state_start, sample: dp_start });
    }
    if dp_ts <= state_ts {
        return Err(AggregateError::OutOfOrder { last: state_ts, sample: dp_ts });
    }
    Ok(())
}

/// Aggregates `dp` into the stream state and replaces `dp` by the state. Returns whether to keep the point and,
/// for a new stream, its initial state.
fn aggregate_num(last: Option<&mut Last>, dp: &mut NumberDataPoint) -> (bool, Option<Last>) {
    let Some(last) = last else {
        // A new stream (Go: the zero state, whose timestamp is 0, takes the point as it is).
        return (true, Some(Last::Num(dp.clone())));
    };
    let Last::Num(state) = last else { return (false, None) };
    if state.timestamp == 0 {
        *state = dp.clone();
        return (true, None);
    }
    if check(state.start_timestamp, state.timestamp, dp.start_timestamp, dp.timestamp).is_err() {
        return (false, None);
    }
    match dp.value {
        NumberValue::Double(v) => state.value = NumberValue::Double(state.value.double_value() + v),
        NumberValue::Int(v) => state.value = NumberValue::Int(state.value.int_value().wrapping_add(v)),
        NumberValue::Empty => {}
    }
    state.timestamp = dp.timestamp;
    *dp = state.clone();
    (true, None)
}

fn add_min_max(state_v: &mut Option<f64>, dp_v: Option<f64>, f: fn(f64, f64) -> f64) {
    *state_v = match (*state_v, dp_v) {
        (Some(a), Some(b)) => Some(f(a, b)),
        _ => None,
    };
}

fn go_min(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() { f64::NAN } else { a.min(b) }
}

fn go_max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() { f64::NAN } else { a.max(b) }
}

fn aggregate_hist(last: Option<&mut Last>, dp: &mut HistogramDataPoint) -> (bool, Option<Last>) {
    let Some(last) = last else {
        return (true, Some(Last::Hist(dp.clone())));
    };
    let Last::Hist(state) = last else { return (false, None) };
    if state.timestamp == 0 {
        *state = dp.clone();
        return (true, None);
    }
    if check(state.start_timestamp, state.timestamp, dp.start_timestamp, dp.timestamp).is_err() {
        return (false, None);
    }
    if state.explicit_bounds != dp.explicit_bounds {
        // Different bounds: the new point replaces the state.
        *state = dp.clone();
    } else {
        let n = state.bucket_counts.len().min(dp.bucket_counts.len());
        for i in 0..n {
            state.bucket_counts[i] = state.bucket_counts[i].wrapping_add(dp.bucket_counts[i]);
        }
        state.count = state.count.wrapping_add(dp.count);
        state.sum = match (state.sum, dp.sum) {
            (Some(a), Some(b)) => Some(a + b),
            _ => None,
        };
        add_min_max(&mut state.min, dp.min, go_min);
        add_min_max(&mut state.max, dp.max, go_max);
    }
    state.timestamp = dp.timestamp;
    *dp = state.clone();
    (true, None)
}

fn aggregate_expo(last: Option<&mut Last>, dp: &mut ExponentialHistogramDataPoint) -> (bool, Option<Last>) {
    let Some(last) = last else {
        return (true, Some(Last::Expo(dp.clone())));
    };
    let Last::Expo(state) = last else { return (false, None) };
    if state.timestamp == 0 {
        *state = dp.clone();
        return (true, None);
    }
    if check(state.start_timestamp, state.timestamp, dp.start_timestamp, dp.timestamp).is_err() {
        return (false, None);
    }
    add_exponential(state, dp);
    state.timestamp = dp.timestamp;
    *dp = state.clone();
    (true, None)
}

/// Go `Adder.Exponential`.
fn add_exponential(state: &mut ExponentialHistogramDataPoint, dp: &mut ExponentialHistogramDataPoint) {
    if state.scale != dp.scale {
        let (hi, lo) = if state.scale < dp.scale { (&mut *dp, &mut *state) } else { (&mut *state, &mut *dp) };
        let (from, to) = (hi.scale, lo.scale);
        downscale(&mut hi.positive, from, to);
        downscale(&mut hi.negative, from, to);
        hi.scale = lo.scale;
    }
    let from = state.scale;
    let to = limit(MAX_BUCKETS, from, &state.positive, &dp.positive)
        .min(limit(MAX_BUCKETS, from, &state.negative, &dp.negative));
    if from != to {
        downscale(&mut state.positive, from, to);
        downscale(&mut state.negative, from, to);
        downscale(&mut dp.positive, from, to);
        downscale(&mut dp.negative, from, to);
        state.scale = to;
        dp.scale = to;
    }
    if state.zero_threshold != dp.zero_threshold {
        if state.zero_threshold < dp.zero_threshold {
            let w = dp.zero_threshold;
            widen_zero(state, w);
        } else {
            let w = state.zero_threshold;
            widen_zero(dp, w);
        }
    }
    merge(&mut state.positive, &dp.positive);
    merge(&mut state.negative, &dp.negative);
    state.count = state.count.wrapping_add(dp.count);
    state.zero_count = state.zero_count.wrapping_add(dp.zero_count);
    state.sum = match (state.sum, dp.sum) {
        (Some(a), Some(b)) => Some(a + b),
        _ => None,
    };
    add_min_max(&mut state.min, dp.min, go_min);
    add_min_max(&mut state.max, dp.max, go_max);
}

// expo package: buckets in absolute bucket indexes.

fn lower(b: &Buckets) -> i64 {
    b.offset as i64
}

fn upper(b: &Buckets) -> i64 {
    b.bucket_counts.len() as i64 + b.offset as i64
}

fn abs(b: &Buckets, at: i64) -> u64 {
    let idx = at - lower(b);
    if idx >= 0 && (idx as usize) < b.bucket_counts.len() { b.bucket_counts[idx as usize] } else { 0 }
}

/// Go `expo.Downscale`.
fn downscale(bs: &mut Buckets, from: i32, to: i32) {
    if from == to {
        return;
    }
    assert!(from > to, "cannot upscale without introducing error ({from} -> {to})");
    let mut at = from;
    while at > to {
        collapse(bs);
        at -= 1;
    }
}

/// Go `expo.Collapse`: merges pairs of buckets (one scale down).
fn collapse(bs: &mut Buckets) {
    let counts = &mut bs.bucket_counts;
    let offset_was_odd = bs.offset % 2 != 0;
    let shift: i64 = if offset_was_odd { -1 } else { 0 };
    let mut size = counts.len() / 2;
    if counts.len() % 2 != 0 || offset_was_odd {
        size += 1;
    }
    if offset_was_odd {
        bs.offset -= 1;
    }
    bs.offset /= 2;
    if counts.is_empty() {
        return;
    }
    for i in 0..size {
        let k = i as i64 * 2 + shift;
        if i == 0 && k == -1 {
            counts[i] = counts[(k + 1) as usize];
            continue;
        }
        counts[i] = counts[k as usize];
        if ((k + 1) as usize) < counts.len() {
            counts[i] = counts[k as usize].wrapping_add(counts[(k + 1) as usize]);
        }
    }
    for c in counts.iter_mut().skip(size) {
        *c = 0;
    }
}

/// Go `expo.Limit`: the scale at which both bucket ranges fit in `max_buckets`.
fn limit(max_buckets: usize, mut scale: i32, a: &Buckets, b: &Buckets) -> i32 {
    let mut lo = lower(a).min(lower(b));
    let mut up = upper(a).max(upper(b));
    while lo < up && abs(a, lo) == 0 && abs(b, lo) == 0 {
        lo += 1;
    }
    while lo < up - 1 && abs(a, up - 1) == 0 && abs(b, up - 1) == 0 {
        up -= 1;
    }
    while up - lo > max_buckets as i64 {
        lo >>= 1;
        up = (up + 1) >> 1;
        scale -= 1;
    }
    scale
}

/// Go `expo.Merge`: adds `b` into `a`.
fn merge(a: &mut Buckets, b: &Buckets) {
    if b.bucket_counts.is_empty() {
        return;
    }
    if a.bucket_counts.is_empty() {
        *a = b.clone();
        return;
    }
    let mut lo = lower(a).min(lower(b));
    let mut up = upper(a).max(upper(b));
    while lo < up && abs(a, lo) == 0 && abs(b, lo) == 0 {
        lo += 1;
    }
    while lo < up - 1 && abs(a, up - 1) == 0 && abs(b, up - 1) == 0 {
        up -= 1;
    }
    let size = (up - lo).max(0) as usize;
    let counts: Vec<u64> = (0..size).map(|i| abs(a, lo + i as i64).wrapping_add(abs(b, lo + i as i64))).collect();
    a.offset = lo as i32;
    a.bucket_counts = counts;
}

/// Go `Scale.Idx`.
fn scale_idx(scale: i32, v: f64) -> i64 {
    let (frac, exp) = frexp(v);
    if frac == 0.5 {
        return (((exp - 1) as i64) << scale) - 1;
    }
    let scale_factor = std::f64::consts::LOG2_E * 2f64.powi(scale);
    (v.ln() * scale_factor).floor() as i64
}

/// Go `math.Frexp` for finite positive values.
fn frexp(v: f64) -> (f64, i32) {
    if v == 0.0 || !v.is_finite() {
        return (v, 0);
    }
    let bits = v.to_bits();
    let mut exp = ((bits >> 52) & 0x7ff) as i32;
    let (mut b, mut e_adj) = (bits, 0);
    if exp == 0 {
        // Subnormal: normalize.
        let n = v * (1u64 << 52) as f64;
        b = n.to_bits();
        exp = ((b >> 52) & 0x7ff) as i32;
        e_adj = -52;
    }
    let e = exp - 1022 + e_adj;
    let frac = f64::from_bits((b & !(0x7ffu64 << 52)) | (1022u64 << 52));
    (frac, e)
}

/// Go `expo.WidenZero`: moves buckets inside the new zero threshold into the zero bucket.
fn widen_zero(dp: &mut ExponentialHistogramDataPoint, width: f64) {
    if width == dp.zero_threshold {
        return;
    }
    assert!(width >= dp.zero_threshold, "min must be larger than current threshold");
    let scale = dp.scale;
    let zero = scale_idx(scale, width);
    for bs in [&mut dp.positive, &mut dp.negative] {
        let mut i = lower(bs);
        while i <= zero {
            dp.zero_count = dp.zero_count.wrapping_add(abs(bs, i));
            i += 1;
        }
        let lo = (zero + 1).min(upper(bs)).max(lower(bs));
        let first = (lo - lower(bs)) as usize;
        bs.bucket_counts.drain(..first);
        bs.offset = lo as i32;
    }
    // The upper bound of bucket `zero`.
    let inverse_factor = std::f64::consts::LN_2 * 2f64.powi(-scale);
    dp.zero_threshold = ((zero + 1) as f64 * inverse_factor).exp();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otlptranslator::pdata::AppendEmpty;

    #[test]
    fn sums_accumulate() {
        let d2c = DeltaToCumulative::default();
        let mut md = Metrics::new();
        let m = md.resource_metrics.append_empty().scope_metrics.append_empty().metrics.append_empty();
        m.name = "x".into();
        let s = m.set_empty_sum();
        s.aggregation_temporality = AggregationTemporality::DELTA;
        for i in 0..3 {
            let p = s.data_points.append_empty();
            p.set_int_value(i);
            p.timestamp = 1 + i as u64;
        }
        assert!(d2c.process(&mut md));
        let s = md.resource_metrics[0].scope_metrics[0].metrics[0].sum();
        assert_eq!(s.aggregation_temporality, AggregationTemporality::CUMULATIVE);
        let v: Vec<i64> = s.data_points.iter().map(|p| p.int_value()).collect();
        assert_eq!(v, vec![0, 1, 3]);
    }

    #[test]
    fn collapse_and_merge() {
        let mut b = Buckets { offset: 1, bucket_counts: vec![1, 2, 3, 4] };
        collapse(&mut b);
        assert_eq!(b, Buckets { offset: 0, bucket_counts: vec![1, 5, 4, 0] });
        let mut a = Buckets { offset: 0, bucket_counts: vec![1, 1] };
        merge(&mut a, &Buckets { offset: 1, bucket_counts: vec![2, 2] });
        assert_eq!(a, Buckets { offset: 0, bucket_counts: vec![1, 3, 2] });
        assert_eq!(frexp(8.0), (0.5, 4));
        assert_eq!(scale_idx(0, 4.0), 1);
    }
}
