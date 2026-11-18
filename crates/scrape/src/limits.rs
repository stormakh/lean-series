//! The appender wrappers of `scrape/target.go` that enforce limits: time limit, sample limit, native
//! histogram bucket limit and maximum schema (Go `timeLimitAppender`, `limitAppender`, `bucketLimitAppender`,
//! `maxSchemaAppender` and their V2 forms).
//!
//! Go stacks one wrapper per limit. Here one struct applies the enabled limits in the same order as Go's
//! stack (outermost first: max schema, bucket limit, sample limit, time limit), without extra indirection.
//! [`LimitsV1::chain`] names the wrappers Go would stack.

use std::time::{Duration, SystemTime};

use model::exemplar::Exemplar;
use model::histogram::{
    EXPONENTIAL_SCHEMA_MAX, EXPONENTIAL_SCHEMA_MIN, FloatHistogram, Histogram, is_exponential_schema,
    is_exponential_schema_reserved,
};
use model::labels::Labels;
use model::metadata::Metadata;
use model::value::is_stale_nan;
use storage::{AppendOptions, AppendV2Options, Appender, AppenderV2, ERR_OUT_OF_BOUNDS, Error, SeriesRef, Sentinel};

static SAMPLE_LIMIT: Sentinel = Sentinel::new("sample limit exceeded");
static BUCKET_LIMIT: Sentinel = Sentinel::new("histogram bucket limit exceeded");
/// Go `errSampleLimit`.
pub const ERR_SAMPLE_LIMIT: Error = Error::Sentinel(&SAMPLE_LIMIT);
/// Go `errBucketLimit`.
pub const ERR_BUCKET_LIMIT: Error = Error::Sentinel(&BUCKET_LIMIT);

/// Go `maxAheadTime`.
pub const MAX_AHEAD_TIME: Duration = Duration::from_secs(600);

/// The limits of one scrape.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Samples later than this (ms) are out of bounds.
    pub max_time: i64,
    pub sample_limit: usize,
    pub bucket_limit: usize,
    pub max_schema: i32,
    /// Samples counted against the sample limit so far.
    pub i: usize,
}

impl Limits {
    /// Go `appenderWithLimits` (the limit settings; `now` is the wall clock).
    pub fn new(now: SystemTime, sample_limit: usize, bucket_limit: usize, max_schema: i32) -> Limits {
        Limits {
            max_time: model::timestamp::from_time(now + MAX_AHEAD_TIME),
            sample_limit,
            bucket_limit,
            max_schema,
            i: 0,
        }
    }

    /// The Go wrapper types, outermost first.
    pub fn chain(&self) -> Vec<&'static str> {
        let mut v = Vec::new();
        if self.max_schema < EXPONENTIAL_SCHEMA_MAX {
            v.push("maxSchemaAppender");
        }
        if self.bucket_limit > 0 {
            v.push("bucketLimitAppender");
        }
        if self.sample_limit > 0 {
            v.push("limitAppender");
        }
        v.push("timeLimitAppender");
        v
    }

    #[inline]
    fn count(&mut self) -> Result<(), Error> {
        if self.sample_limit > 0 {
            self.i += 1;
            if self.i > self.sample_limit {
                return Err(ERR_SAMPLE_LIMIT);
            }
        }
        Ok(())
    }

    /// The max schema and bucket limit steps for a histogram (modified in place).
    fn histogram_limits(&self, h: Option<&mut Histogram>, fh: Option<&mut FloatHistogram>) -> Result<(), Error> {
        let reserved = is_exponential_schema_reserved;
        let (mut h, mut fh) = (h, fh);
        if self.max_schema < EXPONENTIAL_SCHEMA_MAX {
            if let Some(h) = h.as_deref_mut()
                && reserved(h.schema)
                && h.schema > self.max_schema
            {
                h.reduce_resolution(self.max_schema)?;
            }
            if let Some(fh) = fh.as_deref_mut()
                && reserved(fh.schema)
                && fh.schema > self.max_schema
            {
                fh.reduce_resolution(self.max_schema)?;
            }
        }
        if self.bucket_limit > 0 {
            let limit = self.bucket_limit;
            if let Some(h) = h.as_deref_mut() {
                if h.positive_buckets.len() + h.negative_buckets.len() > limit && !is_exponential_schema(h.schema) {
                    return Err(ERR_BUCKET_LIMIT);
                }
                while h.positive_buckets.len() + h.negative_buckets.len() > limit {
                    if h.schema <= EXPONENTIAL_SCHEMA_MIN {
                        return Err(ERR_BUCKET_LIMIT);
                    }
                    h.reduce_resolution(h.schema - 1)?;
                }
            }
            if let Some(fh) = fh.as_deref_mut() {
                if fh.positive_buckets.len() + fh.negative_buckets.len() > limit && !is_exponential_schema(fh.schema) {
                    return Err(ERR_BUCKET_LIMIT);
                }
                while fh.positive_buckets.len() + fh.negative_buckets.len() > limit {
                    if fh.schema <= EXPONENTIAL_SCHEMA_MIN {
                        return Err(ERR_BUCKET_LIMIT);
                    }
                    fh.reduce_resolution(fh.schema - 1)?;
                }
            }
        }
        Ok(())
    }
}

/// A V1 appender with limits.
pub struct LimitsV1<'a> {
    pub app: &'a mut dyn Appender,
    pub limits: Limits,
}

impl LimitsV1<'_> {
    /// The Go wrapper types, outermost first.
    pub fn chain(&self) -> Vec<&'static str> {
        self.limits.chain()
    }

    /// Append with the sample and time limits (Go `limitAppender.Append` then `timeLimitAppender.Append`).
    #[inline]
    pub fn append(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        if r == 0 || !is_stale_nan(v) {
            self.limits.count()?;
        }
        if t > self.limits.max_time {
            return Err(ERR_OUT_OF_BOUNDS);
        }
        self.app.append(r, l, t, v)
    }

    /// AppendHistogram through the max schema, bucket limit and sample limit wrappers (Go's time limit wrapper
    /// does not check histograms).
    pub fn append_histogram(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        mut h: Option<&mut Histogram>,
        mut fh: Option<&mut FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.limits.histogram_limits(h.as_deref_mut(), fh.as_deref_mut())?;
        let stale_ok = h.as_deref().is_some_and(|h| !is_stale_nan(h.sum)) || fh.as_deref().is_some_and(|f| !is_stale_nan(f.sum));
        if r == 0 || stale_ok {
            self.limits.count()?;
        }
        self.app.append_histogram(r, l, t, h.as_deref(), fh.as_deref())
    }

    pub fn append_exemplar(&mut self, r: SeriesRef, l: &Labels, e: &Exemplar) -> Result<SeriesRef, Error> {
        self.app.append_exemplar(r, l, e)
    }
    pub fn append_st_zero_sample(&mut self, r: SeriesRef, l: &Labels, t: i64, st: i64) -> Result<SeriesRef, Error> {
        self.app.append_st_zero_sample(r, l, t, st)
    }
    pub fn append_histogram_st_zero_sample(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        st: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.app.append_histogram_st_zero_sample(r, l, t, st, h, fh)
    }
    pub fn update_metadata(&mut self, r: SeriesRef, l: &Labels, m: &Metadata) -> Result<SeriesRef, Error> {
        self.app.update_metadata(r, l, m)
    }
    pub fn set_options(&mut self, o: Option<&AppendOptions>) {
        self.app.set_options(o)
    }
}

/// A V2 appender with limits.
pub struct LimitsV2<'a> {
    pub app: &'a mut dyn AppenderV2,
    pub limits: Limits,
}

impl LimitsV2<'_> {
    /// The Go wrapper types, outermost first (V2 names).
    pub fn chain(&self) -> Vec<String> {
        self.limits.chain().into_iter().map(|s| format!("{s}V2")).collect()
    }

    /// Go's V2 wrapper stack: max schema, bucket limit, sample limit (on the float value), time limit.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub fn append(
        &mut self,
        r: SeriesRef,
        ls: &Labels,
        st: i64,
        t: i64,
        v: f64,
        mut h: Option<&mut Histogram>,
        mut fh: Option<&mut FloatHistogram>,
        opts: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        if h.is_some() || fh.is_some() {
            self.limits.histogram_limits(h.as_deref_mut(), fh.as_deref_mut())?;
        }
        if r == 0 || !is_stale_nan(v) {
            self.limits.count()?;
        }
        if t > self.limits.max_time {
            return Err(ERR_OUT_OF_BOUNDS);
        }
        self.app.append(r, ls, st, t, v, h.as_deref(), fh.as_deref(), opts)
    }
}
