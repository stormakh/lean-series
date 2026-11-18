//! Port of `tsdb/record/buffers.go`: pools of reusable record buffers.

use std::sync::Mutex;

use super::{RefExemplar, RefFloatHistogramSample, RefHistogramSample, RefMetadata, RefSample, RefSeries};

/// A pool of empty vectors (Go `zeropool.Pool[[]T]`). Put clears the vector, which drops its elements, so
/// pooled buffers never retain labels or histograms (Go zeroes them for the same reason).
#[derive(Debug)]
struct Pool<T>(Mutex<Vec<Vec<T>>>);

impl<T> Default for Pool<T> {
    fn default() -> Self {
        Pool(Mutex::new(Vec::new()))
    }
}

impl<T> Pool<T> {
    #[inline]
    fn get(&self, capacity: usize) -> Vec<T> {
        match self.0.lock().unwrap_or_else(|e| e.into_inner()).pop() {
            Some(b) => b,
            None => Vec::with_capacity(capacity),
        }
    }

    #[inline]
    fn put(&self, mut b: Vec<T>) {
        b.clear();
        if b.capacity() == 0 {
            return;
        }
        self.0.lock().unwrap_or_else(|e| e.into_inner()).push(b);
    }
}

/// Go `BuffersPool`: pools of empty record buffers, safe for concurrent use.
#[derive(Debug, Default)]
pub struct BuffersPool {
    series: Pool<RefSeries>,
    samples: Pool<RefSample>,
    exemplars: Pool<RefExemplar>,
    histograms: Pool<RefHistogramSample>,
    float_histograms: Pool<RefFloatHistogramSample>,
    metadata: Pool<RefMetadata>,
}

impl BuffersPool {
    /// Go `NewBuffersPool`.
    pub fn new() -> BuffersPool {
        BuffersPool::default()
    }

    pub fn get_ref_series(&self, capacity: usize) -> Vec<RefSeries> {
        self.series.get(capacity)
    }

    pub fn put_ref_series(&self, b: Vec<RefSeries>) {
        self.series.put(b)
    }

    pub fn get_samples(&self, capacity: usize) -> Vec<RefSample> {
        self.samples.get(capacity)
    }

    pub fn put_samples(&self, b: Vec<RefSample>) {
        self.samples.put(b)
    }

    pub fn get_exemplars(&self, capacity: usize) -> Vec<RefExemplar> {
        self.exemplars.get(capacity)
    }

    pub fn put_exemplars(&self, b: Vec<RefExemplar>) {
        self.exemplars.put(b)
    }

    pub fn get_histograms(&self, capacity: usize) -> Vec<RefHistogramSample> {
        self.histograms.get(capacity)
    }

    pub fn put_histograms(&self, b: Vec<RefHistogramSample>) {
        self.histograms.put(b)
    }

    pub fn get_float_histograms(&self, capacity: usize) -> Vec<RefFloatHistogramSample> {
        self.float_histograms.get(capacity)
    }

    pub fn put_float_histograms(&self, b: Vec<RefFloatHistogramSample>) {
        self.float_histograms.put(b)
    }

    pub fn get_metadata(&self, capacity: usize) -> Vec<RefMetadata> {
        self.metadata.get(capacity)
    }

    pub fn put_metadata(&self, b: Vec<RefMetadata>) {
        self.metadata.put(b)
    }
}
