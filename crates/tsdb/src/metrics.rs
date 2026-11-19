//! Minimal self-instrumentation values (the parts of `client_golang` the TSDB uses): counters, gauges and
//! histograms as atomics, with upstream metric names kept next to them, so tests can read them (Go
//! `prom_testutil.ToFloat64`) and the web layer can expose them later. Nothing here allocates per update.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// A float value stored in an atomic.
#[derive(Debug, Default)]
pub struct AtomicF64(AtomicU64);

impl AtomicF64 {
    pub const fn new(v: f64) -> AtomicF64 {
        AtomicF64(AtomicU64::new(v.to_bits()))
    }
    #[inline]
    pub fn get(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }
    #[inline]
    pub fn set(&self, v: f64) {
        self.0.store(v.to_bits(), Ordering::Relaxed)
    }
    #[inline]
    pub fn add(&self, d: f64) {
        let mut cur = self.0.load(Ordering::Relaxed);
        loop {
            let new = (f64::from_bits(cur) + d).to_bits();
            match self.0.compare_exchange_weak(cur, new, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return,
                Err(x) => cur = x,
            }
        }
    }
}

/// A monotonic counter (Go `prometheus.Counter`).
#[derive(Debug, Default)]
pub struct Counter(AtomicF64);

impl Counter {
    pub const fn new() -> Counter {
        Counter(AtomicF64::new(0.0))
    }
    #[inline]
    pub fn inc(&self) {
        self.0.add(1.0)
    }
    #[inline]
    pub fn add(&self, v: f64) {
        self.0.add(v)
    }
    /// The current value (Go `prom_testutil.ToFloat64`).
    #[inline]
    pub fn get(&self) -> f64 {
        self.0.get()
    }
}

/// A gauge (Go `prometheus.Gauge`).
#[derive(Debug, Default)]
pub struct Gauge(AtomicF64);

impl Gauge {
    pub const fn new() -> Gauge {
        Gauge(AtomicF64::new(0.0))
    }
    #[inline]
    pub fn set(&self, v: f64) {
        self.0.set(v)
    }
    #[inline]
    pub fn inc(&self) {
        self.0.add(1.0)
    }
    #[inline]
    pub fn dec(&self) {
        self.0.add(-1.0)
    }
    #[inline]
    pub fn add(&self, v: f64) {
        self.0.add(v)
    }
    #[inline]
    pub fn sub(&self, v: f64) {
        self.0.add(-v)
    }
    #[inline]
    pub fn get(&self) -> f64 {
        self.0.get()
    }
}

/// A histogram with fixed buckets (Go `prometheus.Histogram`); also used for summaries, which upstream exposes
/// only as count and sum here.
#[derive(Debug, Default)]
pub struct Histogram {
    /// Upper bounds of the buckets (without +Inf).
    pub buckets: Vec<f64>,
    counts: Mutex<Vec<u64>>,
    count: AtomicU64,
    sum: AtomicF64,
}

impl Histogram {
    pub fn new(buckets: Vec<f64>) -> Histogram {
        let n = buckets.len();
        Histogram {
            buckets,
            counts: Mutex::new(vec![0; n]),
            count: AtomicU64::new(0),
            sum: AtomicF64::new(0.0),
        }
    }
    /// Go `prometheus.ExponentialBuckets(start, factor, count)`.
    pub fn exponential_buckets(start: f64, factor: f64, count: usize) -> Vec<f64> {
        let mut v = Vec::with_capacity(count);
        let mut x = start;
        for _ in 0..count {
            v.push(x);
            x *= factor;
        }
        v
    }
    /// Go `prometheus.LinearBuckets(start, width, count)`.
    pub fn linear_buckets(start: f64, width: f64, count: usize) -> Vec<f64> {
        (0..count).map(|i| start + width * i as f64).collect()
    }
    pub fn observe(&self, v: f64) {
        if !self.buckets.is_empty() {
            let i = self.buckets.partition_point(|b| *b < v);
            if i < self.buckets.len() {
                let mut c = self.counts.lock().unwrap_or_else(|p| p.into_inner());
                c[i] += 1;
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.add(v);
    }
    /// The number of observations (Go `SampleCount`).
    pub fn sample_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
    /// The sum of observations (Go `SampleSum`).
    pub fn sample_sum(&self) -> f64 {
        self.sum.get()
    }
    /// Cumulative bucket counts, as exposed.
    pub fn cumulative_counts(&self) -> Vec<u64> {
        let c = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        let mut acc = 0;
        c.iter()
            .map(|x| {
                acc += x;
                acc
            })
            .collect()
    }
}
