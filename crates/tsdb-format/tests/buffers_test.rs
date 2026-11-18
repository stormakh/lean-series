//! Port of `tsdb/record/buffers_test.go`.

use std::sync::Arc;

use model::histogram::{FloatHistogram, Histogram};
use tsdb_format::record::{BuffersPool, RefFloatHistogramSample, RefHistogramSample};

#[test]
fn test_buffers_pool_ptr_clear() {
    let pool = BuffersPool::new();

    let h_ptr = Arc::new(Histogram { schema: 1244124, ..Default::default() });
    let mut h = pool.get_histograms(1);
    h.push(RefHistogramSample { h: h_ptr.clone(), ..Default::default() });
    pool.put_histograms(h);

    let h2 = pool.get_histograms(1);
    assert!(h2.is_empty());
    assert_eq!(1, h2.capacity());
    // adapted: Go extends the slice to its capacity and checks that the stored pointer is nil. Rust cannot read
    // past the length; the pool must have dropped its reference, so the only one left is ours.
    assert_eq!(1, Arc::strong_count(&h_ptr));

    let fh_ptr = Arc::new(FloatHistogram { schema: 1244521, ..Default::default() });
    let mut fh = pool.get_float_histograms(1);
    fh.push(RefFloatHistogramSample { fh: fh_ptr.clone(), ..Default::default() });
    pool.put_float_histograms(fh);

    let fh2 = pool.get_float_histograms(1);
    assert!(fh2.is_empty());
    assert_eq!(1, fh2.capacity());
    // adapted: as above.
    assert_eq!(1, Arc::strong_count(&fh_ptr));
}
