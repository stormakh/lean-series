//! Port of `promql/histogram_stats_iterator.go`: an iterator that only decodes histogram count and sum.
//!
//! STUB: being ported.

use chunkenc::{AnyIterator, ChunkIterator, ValueType};
use model::histogram::{FloatHistogram, Histogram};

/// Go `HistogramStatsIterator`.
pub struct HistogramStatsIterator<'a> {
    inner: AnyIterator<'a>,
}

impl<'a> HistogramStatsIterator<'a> {
    /// Go `NewHistogramStatsIterator`.
    pub fn new(it: AnyIterator<'a>) -> HistogramStatsIterator<'a> {
        HistogramStatsIterator { inner: it }
    }
}

impl ChunkIterator for HistogramStatsIterator<'_> {
    fn next(&mut self) -> ValueType {
        self.inner.next()
    }
    fn seek(&mut self, t: i64) -> ValueType {
        self.inner.seek(t)
    }
    fn at(&self) -> (i64, f64) {
        self.inner.at()
    }
    fn at_histogram(&self, h: Option<Histogram>) -> (i64, Histogram) {
        self.inner.at_histogram(h)
    }
    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        self.inner.at_float_histogram(fh)
    }
    fn at_t(&self) -> i64 {
        self.inner.at_t()
    }
    fn at_st(&self) -> i64 {
        self.inner.at_st()
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        self.inner.err()
    }
}
