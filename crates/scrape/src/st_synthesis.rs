//! Port of `scrape/st_synthesis.go`: start timestamp synthesis for cumulative metrics (counters, summaries,
//! histograms). The first sample of a series is dropped to set the reference point; later samples are reported
//! relative to it.

use model::histogram::{FloatHistogram, Histogram};

/// Go `floatSynthesis`.
#[derive(Clone, Debug, Default)]
pub struct FloatSynthesis {
    pub prev: f64,
    pub starting: f64,
}

/// Go `histogramSynthesis`: integer and float histograms, kept as float histograms.
#[derive(Clone, Debug, Default)]
pub struct HistogramSynthesis {
    pub prev: Option<FloatHistogram>,
    pub starting: Option<FloatHistogram>,
}

/// Go `stCache`.
#[derive(Clone, Debug, Default)]
pub struct StCache {
    pub f: Option<FloatSynthesis>,
    pub h: Option<HistogramSynthesis>,
    pub st: i64,
}

impl StCache {
    /// Go `synthesizeFloat`: the adjusted value, the start timestamp, and whether to skip the append.
    pub fn synthesize_float(&mut self, v: f64, t: i64) -> (f64, i64, bool) {
        let n = self.f.get_or_insert_with(FloatSynthesis::default);
        if self.st == 0 {
            n.prev = v;
            n.starting = v;
            self.st = t;
            return (v, self.st, true);
        }
        if v < n.prev {
            n.starting = 0.0;
            self.st = t - 1;
        }
        n.prev = v;
        (v - n.starting, self.st, false)
    }

    /// Go `synthesizeHistogram`: the adjusted histogram (None: unchanged), the start timestamp, and whether to
    /// skip the append.
    pub fn synthesize_histogram(&mut self, h: &Histogram, t: i64) -> (Option<Histogram>, i64, bool) {
        let n = self.h.get_or_insert_with(HistogramSynthesis::default);
        let curr = h.to_float();
        if self.st == 0 {
            n.prev = Some(curr.copy());
            n.starting = n.prev.clone();
            self.st = t;
            return (None, self.st, true);
        }
        if n.prev.as_ref().is_some_and(|p| curr.detect_reset(p)) {
            n.prev = Some(curr.copy());
            n.starting = None;
            self.st = t - 1;
            return (None, self.st, false);
        }
        n.prev = Some(curr.copy());
        let Some(starting) = &n.starting else {
            return (None, self.st, false);
        };
        let mut sub = curr;
        if let Err(e) = sub.sub(starting) {
            // Only an invalid bucket layout fails; appending an unadjusted sample would be wrong data.
            panic!("{e}");
        }
        sub.compact(0);
        let mut adjusted = Histogram {
            counter_reset_hint: h.counter_reset_hint,
            schema: sub.schema,
            zero_threshold: sub.zero_threshold,
            zero_count: sub.zero_count as u64,
            count: sub.count as u64,
            sum: sub.sum,
            custom_values: sub.custom_values.clone(),
            ..Default::default()
        };
        if !sub.positive_spans.is_empty() {
            adjusted.positive_spans = sub.positive_spans.clone();
            adjusted.positive_buckets = deltas(&sub.positive_buckets);
        }
        if !sub.negative_spans.is_empty() {
            adjusted.negative_spans = sub.negative_spans.clone();
            adjusted.negative_buckets = deltas(&sub.negative_buckets);
        }
        (Some(adjusted), self.st, false)
    }

    /// Go `synthesizeFloatHistogram`.
    pub fn synthesize_float_histogram(&mut self, fh: &FloatHistogram, t: i64) -> (Option<FloatHistogram>, i64, bool) {
        let n = self.h.get_or_insert_with(HistogramSynthesis::default);
        if self.st == 0 {
            n.prev = Some(fh.copy());
            n.starting = n.prev.clone();
            self.st = t;
            return (None, t, true);
        }
        if n.prev.as_ref().is_some_and(|p| fh.detect_reset(p)) {
            n.prev = Some(fh.copy());
            n.starting = None;
            self.st = t - 1;
            return (None, self.st, false);
        }
        n.prev = Some(fh.copy());
        let Some(starting) = &n.starting else {
            return (None, self.st, false);
        };
        let mut adjusted = fh.copy();
        if let Err(e) = adjusted.sub(starting) {
            panic!("{e}");
        }
        adjusted.compact(0);
        (Some(adjusted), self.st, false)
    }
}

/// Absolute bucket counts (as floats) to the delta encoding of integer histograms, with Go's uint64 wrapping.
fn deltas(abs: &[f64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(abs.len());
    let mut last: u64 = 0;
    for &v in abs {
        let a = v as u64;
        out.push(a.wrapping_sub(last) as i64);
        last = a;
    }
    out
}
