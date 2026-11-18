//! Port of `storage/memoized_iterator_test.go`.

use chunkenc::ValueType;
use model::histogram::FloatHistogram;
use storage::tsdbutil::*;
use storage::*;

fn sample_eq(
    it: &MemoizedSeriesIterator<'_>,
    est: i64,
    ets: i64,
    ev: f64,
    efh: Option<&FloatHistogram>,
) {
    match efh {
        None => {
            let (ts, v) = it.at();
            assert_eq!(ts, ets, "At() timestamp mismatch");
            assert_eq!(v, ev, "At() value mismatch");
        }
        Some(efh) => {
            let (ts, fh) = it.at_float_histogram(None);
            assert_eq!(ts, ets, "AtFloatHistogram() timestamp mismatch");
            assert_eq!(&fh, efh, "AtFloatHistogram() histogram mismatch");
        }
    }
    assert_eq!(it.at_st(), est, "AtST() start timestamp mismatch");
    assert_eq!(it.at_t(), ets, "AtT() timestamp mismatch");
}

fn prev_sample_eq(
    it: &MemoizedSeriesIterator<'_>,
    est: i64,
    ets: i64,
    ev: f64,
    efh: Option<&FloatHistogram>,
    eok: bool,
) {
    // adapted: Go returns zero values with ok=false; Rust returns None.
    let (st, ts, v, fh, ok) = match it.peek_prev() {
        Some((st, ts, v, fh)) => (st, ts, v, fh, true),
        None => (0, 0, 0.0, None, false),
    };
    assert_eq!(st, est, "start timestamp mismatch");
    assert_eq!(ok, eok, "exist mismatch");
    assert_eq!(ts, ets, "timestamp mismatch");
    match efh {
        None => assert_eq!(v, ev, "value mismatch"),
        Some(efh) => assert_eq!(fh, Some(efh), "histogram mismatch"),
    }
}

#[test]
fn test_memoized_series_iterator() {
    let samples = vec![
        Sample::float(0, 1, 2.0),
        Sample::float(1, 2, 3.0),
        Sample::float(2, 3, 4.0),
        Sample::float(3, 4, 5.0),
        Sample::float(4, 5, 6.0),
        Sample::float(98, 99, 8.0),
        Sample::float(99, 100, 9.0),
        Sample::float(100, 101, 10.0),
        Sample::histogram(101, 102, generate_test_histogram(0)),
        Sample::histogram(102, 103, generate_test_histogram(1)),
        Sample::float_histogram(103, 104, generate_test_float_histogram(2)),
        Sample::float_histogram(104, 199, generate_test_float_histogram(3)),
        Sample::histogram(199, 200, generate_test_histogram(4)),
        Sample::float_histogram(298, 299, generate_test_float_histogram(5)),
        Sample::float(299, 300, 11.0),
        Sample::histogram(350, 399, generate_test_histogram(6)),
        Sample::float(399, 400, 12.0),
    ];
    let mut it = new_memoized_iterator(new_list_series_iterator(&samples), 2);

    assert_eq!(it.seek(-123), ValueType::Float, "seek failed");
    sample_eq(&it, 0, 1, 2.0, None);
    prev_sample_eq(&it, 0, 0, 0.0, None, false);

    assert_eq!(it.seek(5), ValueType::Float, "seek failed");
    sample_eq(&it, 4, 5, 6.0, None);
    prev_sample_eq(&it, 3, 4, 5.0, None, true);

    // Seek to a histogram sample with a previous float sample.
    assert_eq!(it.seek(102), ValueType::FloatHistogram, "seek failed");
    sample_eq(&it, 101, 102, 10.0, Some(&generate_test_float_histogram(0)));
    prev_sample_eq(&it, 100, 101, 10.0, None, true);

    // Attempt to seek backwards (no-op).
    assert_eq!(it.seek(50), ValueType::FloatHistogram, "seek failed");
    sample_eq(&it, 101, 102, 10.0, Some(&generate_test_float_histogram(0)));
    prev_sample_eq(&it, 100, 101, 10.0, None, true);

    // Seek to a float histogram sample with a previous histogram sample.
    assert_eq!(it.seek(104), ValueType::FloatHistogram, "seek failed");
    sample_eq(&it, 103, 104, 0.0, Some(&generate_test_float_histogram(2)));
    prev_sample_eq(
        &it,
        102,
        103,
        0.0,
        Some(&generate_test_float_histogram(1)),
        true,
    );

    // Seek to a float sample with a previous float histogram sample.
    assert_eq!(it.seek(300), ValueType::Float, "seek failed");
    sample_eq(&it, 299, 300, 11.0, None);
    prev_sample_eq(
        &it,
        298,
        299,
        0.0,
        Some(&generate_test_float_histogram(5)),
        true,
    );

    // Seek to a float sample with a previous histogram sample.
    assert_eq!(it.seek(400), ValueType::Float, "seek failed");
    sample_eq(&it, 399, 400, 12.0, None);
    prev_sample_eq(
        &it,
        350,
        399,
        0.0,
        Some(&generate_test_float_histogram(6)),
        true,
    );

    assert_eq!(
        it.seek(1024),
        ValueType::None,
        "seek succeeded unexpectedly"
    );
}
