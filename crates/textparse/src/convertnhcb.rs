//! Port of `util/convertnhcb`: collects classic histogram series and converts them to a native histogram with
//! custom buckets (NHCB). Lives here because textparse needs it; the ledger lists its test under crate `util`.

use model::histogram::{custom_values, FloatHistogram, Histogram, Span, CUSTOM_BUCKETS_SCHEMA};
use model::labels::{Builder, Labels};

use crate::strconv::format_g;

pub const ERR_NEGATIVE_BUCKET_COUNT: &str = "bucket count must be non-negative";
pub const ERR_NAN_BUCKET: &str = "bucket boundary must not be NaN";
pub const ERR_NEGATIVE_COUNT: &str = "count must be non-negative";
pub const ERR_MISSING_COUNT: &str = "count must be provided when no buckets are present";
pub const ERR_COUNT_MISMATCH: &str = "count mismatch";
pub const ERR_COUNT_NOT_CUMULATIVE: &str = "count is not cumulative";

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct TempHistogramBucket {
    le: f64,
    count: f64,
}

/// Collects classic histogram samples incrementally (Go `TempHistogram`).
#[derive(Clone, Debug, Default)]
pub struct TempHistogram {
    buckets: Vec<TempHistogramBucket>,
    count: f64,
    sum: f64,
    err: Option<String>,
    has_count: bool,
}

impl TempHistogram {
    /// Go `NewTempHistogram`.
    pub fn new() -> TempHistogram {
        TempHistogram { buckets: Vec::with_capacity(10), ..Default::default() }
    }

    /// The first error seen, if any.
    pub fn err(&self) -> Option<&str> {
        self.err.as_deref()
    }

    pub fn reset(&mut self) {
        self.buckets.clear();
        self.count = 0.0;
        self.sum = 0.0;
        self.err = None;
        self.has_count = false;
    }

    fn fail(&mut self, e: String) -> Result<(), String> {
        self.err = Some(e.clone());
        Err(e)
    }

    pub fn set_bucket_count(&mut self, boundary: f64, count: f64) -> Result<(), String> {
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        if boundary.is_nan() {
            return self.fail(ERR_NAN_BUCKET.to_string());
        }
        if count < 0.0 {
            return self.fail(format!("{ERR_NEGATIVE_BUCKET_COUNT}: le={}, count={}", format_g(boundary), format_g(count)));
        }
        let n = self.buckets.len();
        if n == 0 {
            self.buckets.push(TempHistogramBucket { le: boundary, count });
        } else if self.buckets[n - 1].le < boundary {
            if count < self.buckets[n - 1].count {
                let prev = self.buckets[n - 1].count;
                return self.fail(format!("{ERR_COUNT_NOT_CUMULATIVE}: {} < {}", format_g(count), format_g(prev)));
            }
            self.buckets.push(TempHistogramBucket { le: boundary, count });
        } else if self.buckets[n - 1].le == boundary {
            // Duplicate sample.
        } else {
            let i = self.buckets.partition_point(|b| b.le < boundary);
            if self.buckets[i].le == boundary {
                return Ok(());
            }
            if i > 0 && count < self.buckets[i - 1].count {
                let prev = self.buckets[i - 1].count;
                return self.fail(format!("{ERR_COUNT_NOT_CUMULATIVE}: {} < {}", format_g(count), format_g(prev)));
            }
            if count > self.buckets[i].count {
                let next = self.buckets[i].count;
                return self.fail(format!("{ERR_COUNT_NOT_CUMULATIVE}: {} > {}", format_g(count), format_g(next)));
            }
            self.buckets.insert(i, TempHistogramBucket { le: boundary, count });
        }
        Ok(())
    }

    pub fn set_count(&mut self, count: f64) -> Result<(), String> {
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        if count < 0.0 {
            return self.fail(format!("{ERR_NEGATIVE_COUNT}: count={}", format_g(count)));
        }
        self.count = count;
        self.has_count = true;
        Ok(())
    }

    pub fn set_sum(&mut self, sum: f64) -> Result<(), String> {
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        self.sum = sum;
        Ok(())
    }

    /// Go `Convert` (value receiver: the receiver is not changed). Returns an integer or a float histogram.
    pub fn convert(&self) -> Result<(Option<Histogram>, Option<FloatHistogram>), String> {
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        let mut count = self.count;
        if !self.has_count {
            if self.buckets.is_empty() {
                return Err(ERR_MISSING_COUNT.to_string());
            }
            count = self.buckets[self.buckets.len() - 1].count;
        }
        let mut buckets: Vec<TempHistogramBucket> = Vec::with_capacity(self.buckets.len() + 1);
        buckets.extend_from_slice(&self.buckets);
        if buckets.is_empty() || buckets[buckets.len() - 1].le != f64::INFINITY {
            buckets.push(TempHistogramBucket { le: f64::INFINITY, count });
        }
        for b in &buckets {
            let int_count = b.count.round() as i64;
            if b.count != int_count as f64 {
                return convert_to_float_histogram(&buckets, count, self.sum);
            }
        }
        let int_count = count.round() as u64;
        if count != int_count as f64 {
            return convert_to_float_histogram(&buckets, count, self.sum);
        }
        convert_to_integer_histogram(&buckets, int_count, self.sum)
    }
}

fn convert_to_integer_histogram(
    buckets: &[TempHistogramBucket],
    count: u64,
    sum: f64,
) -> Result<(Option<Histogram>, Option<FloatHistogram>), String> {
    let mut rh = Histogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count,
        sum,
        positive_spans: vec![Span { offset: 0, length: buckets.len() as u32 }],
        positive_buckets: vec![0; buckets.len()],
        ..Default::default()
    };
    let mut cv = vec![0.0; buckets.len().saturating_sub(1)];
    let mut prev_count = 0i64;
    let mut prev_delta = 0i64;
    for (i, b) in buckets.iter().enumerate() {
        let delta = b.count as i64 - prev_count;
        rh.positive_buckets[i] = delta - prev_delta;
        prev_count = b.count as i64;
        prev_delta = delta;
        if b.le != f64::INFINITY {
            cv[i] = b.le;
        }
    }
    if buckets.len() > 1 {
        rh.custom_values = custom_values(&cv);
    }
    let last = buckets[buckets.len() - 1];
    if count != last.count as u64 {
        return Err(format!("{ERR_COUNT_MISMATCH}: count={count} != le={} count={}", format_g(last.le), format_g(last.count)));
    }
    rh.compact(2);
    if let Err(e) = rh.validate() {
        return Err(format!("invalid histogram: {}", e.message()));
    }
    Ok((Some(rh), None))
}

fn convert_to_float_histogram(
    buckets: &[TempHistogramBucket],
    count: f64,
    sum: f64,
) -> Result<(Option<Histogram>, Option<FloatHistogram>), String> {
    let mut rh = FloatHistogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count,
        sum,
        positive_spans: vec![Span { offset: 0, length: buckets.len() as u32 }],
        positive_buckets: vec![0.0; buckets.len()],
        ..Default::default()
    };
    let mut cv = vec![0.0; buckets.len().saturating_sub(1)];
    let mut prev_count = 0.0;
    for (i, b) in buckets.iter().enumerate() {
        rh.positive_buckets[i] = b.count - prev_count;
        prev_count = b.count;
        if b.le != f64::INFINITY {
            cv[i] = b.le;
        }
    }
    if buckets.len() > 1 {
        rh.custom_values = custom_values(&cv);
    }
    let last = buckets[buckets.len() - 1];
    if count != last.count {
        return Err(format!(
            "{ERR_COUNT_MISMATCH}: count={} != le={} count={}",
            format_g(count),
            format_g(last.le),
            format_g(last.count)
        ));
    }
    rh.compact(0);
    if let Err(e) = rh.validate() {
        return Err(format!("invalid float histogram: {}", e.message()));
    }
    Ok((None, Some(rh)))
}

/// Go `GetHistogramMetricBase`: the labels with `__name__` set to `name` and without `le`.
pub fn get_histogram_metric_base(m: &Labels, name: &str) -> Labels {
    let mut b = Builder::new(m.clone());
    b.set("__name__", name).del("le");
    b.labels()
}

/// Go `SuffixType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SuffixType {
    None,
    Bucket,
    Sum,
    Count,
}

/// Go `GetHistogramMetricBaseName`: removes `_bucket`, `_sum` or `_count` (not `_created`).
pub fn get_histogram_metric_base_name(s: &str) -> (SuffixType, &str) {
    if let Some(r) = s.strip_suffix("_bucket") {
        return (SuffixType::Bucket, r);
    }
    if let Some(r) = s.strip_suffix("_sum") {
        return (SuffixType::Sum, r);
    }
    if let Some(r) = s.strip_suffix("_count") {
        return (SuffixType::Count, r);
    }
    (SuffixType::None, s)
}
