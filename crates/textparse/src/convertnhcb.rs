//! Port of `util/convertnhcb`: converts classic histograms (`_bucket`, `_count`, `_sum` series) to native
//! histograms with custom buckets (NHCB). The code lives here because the parsers need it and textparse cannot
//! depend on `util`; `util::convertnhcb` re-exports it.

use std::sync::Arc;

use model::histogram::gofmt::g;
use model::histogram::{CUSTOM_BUCKETS_SCHEMA, FloatHistogram, Histogram, Span};
use model::labels::{self, Labels};

/// The errors of the conversion (Go's sentinel errors and their wrapped forms).
#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    /// Go `errNegativeBucketCount` with details.
    NegativeBucketCount(String),
    /// Go `errNaNBucket`.
    NaNBucket,
    /// Go `errNegativeCount` with details.
    NegativeCount(String),
    /// Go `errMissingCount`.
    MissingCount,
    /// Go `errCountMismatch` with details.
    CountMismatch(String),
    /// Go `errCountNotCumulative` with details.
    CountNotCumulative(String),
    /// Go `invalid histogram: %w` / `invalid float histogram: %w`.
    Invalid(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NegativeBucketCount(d) => write!(f, "bucket count must be non-negative: {}", d),
            Error::NaNBucket => f.write_str("bucket boundary must not be NaN"),
            Error::NegativeCount(d) => write!(f, "count must be non-negative: {}", d),
            Error::MissingCount => {
                f.write_str("count must be provided when no buckets are present")
            }
            Error::CountMismatch(d) => write!(f, "count mismatch: {}", d),
            Error::CountNotCumulative(d) => write!(f, "count is not cumulative: {}", d),
            Error::Invalid(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for Error {}

impl From<Error> for String {
    fn from(e: Error) -> String {
        e.to_string()
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct TempHistogramBucket {
    le: f64,
    count: f64,
}

/// Collects the parts of a classic histogram (Go `TempHistogram`).
#[derive(Clone, Debug, PartialEq)]
pub struct TempHistogram {
    buckets: Vec<TempHistogramBucket>,
    count: f64,
    sum: f64,
    err: Option<Error>,
    has_count: bool,
}

impl Default for TempHistogram {
    fn default() -> Self {
        TempHistogram::new()
    }
}

impl TempHistogram {
    /// Go `NewTempHistogram`.
    pub fn new() -> TempHistogram {
        TempHistogram {
            buckets: Vec::with_capacity(10),
            count: 0.0,
            sum: 0.0,
            err: None,
            has_count: false,
        }
    }

    /// Go `Err`.
    pub fn err(&self) -> Option<&Error> {
        self.err.as_ref()
    }

    /// Go `Reset`.
    pub fn reset(&mut self) {
        self.buckets.clear();
        self.count = 0.0;
        self.sum = 0.0;
        self.err = None;
        self.has_count = false;
    }

    /// Go `SetBucketCount`.
    pub fn set_bucket_count(&mut self, boundary: f64, count: f64) -> Result<(), Error> {
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        if boundary.is_nan() {
            self.err = Some(Error::NaNBucket);
            return Err(Error::NaNBucket);
        }
        if count < 0.0 {
            let e = Error::NegativeBucketCount(format!("le={}, count={}", g(boundary), g(count)));
            self.err = Some(e.clone());
            return Err(e);
        }
        let n = self.buckets.len();
        if n == 0 {
            self.buckets.push(TempHistogramBucket {
                le: boundary,
                count,
            });
        } else if self.buckets[n - 1].le < boundary {
            if count < self.buckets[n - 1].count {
                let e = Error::CountNotCumulative(format!(
                    "{} < {}",
                    g(count),
                    g(self.buckets[n - 1].count)
                ));
                self.err = Some(e.clone());
                return Err(e);
            }
            self.buckets.push(TempHistogramBucket {
                le: boundary,
                count,
            });
        } else if self.buckets[n - 1].le == boundary {
            // Ignore this, as it is a duplicate sample.
        } else {
            let i = self.buckets.partition_point(|b| b.le < boundary);
            if self.buckets[i].le == boundary {
                return Ok(());
            }
            if i > 0 && count < self.buckets[i - 1].count {
                let e = Error::CountNotCumulative(format!(
                    "{} < {}",
                    g(count),
                    g(self.buckets[i - 1].count)
                ));
                self.err = Some(e.clone());
                return Err(e);
            }
            if count > self.buckets[i].count {
                let e = Error::CountNotCumulative(format!(
                    "{} > {}",
                    g(count),
                    g(self.buckets[i].count)
                ));
                self.err = Some(e.clone());
                return Err(e);
            }
            self.buckets.insert(
                i,
                TempHistogramBucket {
                    le: boundary,
                    count,
                },
            );
        }
        Ok(())
    }

    /// Go `SetCount`.
    pub fn set_count(&mut self, count: f64) -> Result<(), Error> {
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        if count < 0.0 {
            let e = Error::NegativeCount(format!("count={}", g(count)));
            self.err = Some(e.clone());
            return Err(e);
        }
        self.count = count;
        self.has_count = true;
        Ok(())
    }

    /// Go `SetSum`.
    pub fn set_sum(&mut self, sum: f64) -> Result<(), Error> {
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        self.sum = sum;
        Ok(())
    }

    /// Go `Convert`: an integer histogram if all counts are integers, else a float histogram.
    pub fn convert(&self) -> Result<(Option<Histogram>, Option<FloatHistogram>), Error> {
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        // Go's value receiver works on a copy.
        let mut h = self.clone();
        if !h.has_count {
            if h.buckets.is_empty() {
                return Err(Error::MissingCount);
            }
            h.count = h.buckets[h.buckets.len() - 1].count;
            h.has_count = true;
        }
        if h.buckets.is_empty() || h.buckets[h.buckets.len() - 1].le != f64::INFINITY {
            h.buckets.push(TempHistogramBucket {
                le: f64::INFINITY,
                count: h.count,
            });
        }
        for b in &h.buckets {
            let int_count = b.count.round() as i64;
            if b.count != int_count as f64 {
                return h.convert_to_float_histogram();
            }
        }
        let int_count = h.count.round() as u64;
        if h.count != int_count as f64 {
            return h.convert_to_float_histogram();
        }
        h.convert_to_integer_histogram(int_count)
    }

    fn convert_to_integer_histogram(
        &self,
        count: u64,
    ) -> Result<(Option<Histogram>, Option<FloatHistogram>), Error> {
        let n = self.buckets.len();
        let mut rh = Histogram {
            schema: CUSTOM_BUCKETS_SCHEMA,
            count,
            sum: self.sum,
            positive_spans: vec![Span {
                offset: 0,
                length: n as u32,
            }],
            positive_buckets: vec![0; n],
            ..Default::default()
        };
        let mut cv = if n > 1 { Some(vec![0.0; n - 1]) } else { None };
        let mut prev_count = 0i64;
        let mut prev_delta = 0i64;
        for (i, b) in self.buckets.iter().enumerate() {
            let delta = b.count as i64 - prev_count;
            rh.positive_buckets[i] = delta - prev_delta;
            prev_count = b.count as i64;
            prev_delta = delta;
            if b.le != f64::INFINITY {
                if let Some(cv) = cv.as_mut() {
                    cv[i] = b.le;
                }
            }
        }
        rh.custom_values = cv.map(Arc::from);
        let last = self.buckets[n - 1];
        if count != last.count as u64 {
            return Err(Error::CountMismatch(format!(
                "count={} != le={} count={}",
                count,
                g(last.le),
                g(last.count)
            )));
        }
        rh.compact(2);
        if let Err(e) = rh.validate() {
            return Err(Error::Invalid(format!("invalid histogram: {}", e)));
        }
        Ok((Some(rh), None))
    }

    fn convert_to_float_histogram(
        &self,
    ) -> Result<(Option<Histogram>, Option<FloatHistogram>), Error> {
        let n = self.buckets.len();
        let mut rh = FloatHistogram {
            schema: CUSTOM_BUCKETS_SCHEMA,
            count: self.count,
            sum: self.sum,
            positive_spans: vec![Span {
                offset: 0,
                length: n as u32,
            }],
            positive_buckets: vec![0.0; n],
            ..Default::default()
        };
        let mut cv = if n > 1 { Some(vec![0.0; n - 1]) } else { None };
        let mut prev_count = 0.0;
        for (i, b) in self.buckets.iter().enumerate() {
            rh.positive_buckets[i] = b.count - prev_count;
            prev_count = b.count;
            if b.le != f64::INFINITY {
                if let Some(cv) = cv.as_mut() {
                    cv[i] = b.le;
                }
            }
        }
        rh.custom_values = cv.map(Arc::from);
        let last = self.buckets[n - 1];
        if self.count != last.count {
            return Err(Error::CountMismatch(format!(
                "count={} != le={} count={}",
                g(self.count),
                g(last.le),
                g(last.count)
            )));
        }
        rh.compact(0);
        if let Err(e) = rh.validate() {
            return Err(Error::Invalid(format!("invalid float histogram: {}", e)));
        }
        Ok((None, Some(rh)))
    }
}

/// Go `GetHistogramMetricBase`: the labels with the base metric name and without `le`.
pub fn get_histogram_metric_base(m: &Labels, name: &str) -> Labels {
    let mut b = labels::Builder::new(m.clone());
    b.set(labels::METRIC_NAME, name).del(labels::BUCKET_LABEL);
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

/// Go `GetHistogramMetricBaseName`.
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
