//! Test helpers (port of `model/histogram/test_utils.go`).

use super::*;

/// Generates `num_histograms` histograms with `num_buckets` buckets each.
pub fn generate_big_test_histograms(num_histograms: usize, num_buckets: usize) -> Vec<Histogram> {
    let num_spans = num_buckets / 10;
    let buckets_per_side = num_buckets / 2;
    let span_length = (buckets_per_side / num_spans) as u32;
    // Given all bucket deltas are 1, sum bucketsPerSide + 1.
    let observation_count = buckets_per_side as u64 * (1 + buckets_per_side as u64);

    (0..num_histograms)
        .map(|i| {
            let s = Span { offset: 1, length: span_length };
            Histogram {
                count: i as u64 + observation_count,
                zero_count: i as u64,
                zero_threshold: 1e-128,
                sum: 18.4 * (i + 1) as f64,
                schema: 2,
                negative_spans: vec![s; num_spans],
                positive_spans: vec![s; num_spans],
                negative_buckets: vec![1; buckets_per_side],
                positive_buckets: vec![1; buckets_per_side],
                ..Default::default()
            }
        })
        .collect()
}
