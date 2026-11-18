//! Port of `promql/histogram_stats_iterator_test.go`.

use chunkenc::{AnyIterator, ChunkIterator, ValueType};
use model::histogram::{CounterResetHint, FloatHistogram, Histogram};
use model::value::{is_stale_nan, STALE_NAN};
use promql::histogram_stats_iterator::HistogramStatsIterator;
use storage::tsdbutil::generate_test_histogram_with_hint;

use CounterResetHint::{CounterReset, NotCounterReset, UnknownCounterReset};

/// Go `histogramIterator` of the test: returns integer histograms.
struct HistogramIterator<'a> {
    i: isize,
    histograms: &'a [Histogram],
}

impl ChunkIterator for HistogramIterator<'_> {
    fn next(&mut self) -> ValueType {
        self.i += 1;
        if (self.i as usize) < self.histograms.len() {
            return ValueType::Histogram;
        }
        ValueType::None
    }
    fn seek(&mut self, _: i64) -> ValueType {
        panic!("not implemented")
    }
    fn at(&self) -> (i64, f64) {
        panic!("not implemented")
    }
    fn at_histogram(&self, _: Option<Histogram>) -> (i64, Histogram) {
        (0, self.histograms[self.i as usize].clone())
    }
    fn at_float_histogram(&self, _: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        (0, self.histograms[self.i as usize].to_float())
    }
    fn at_t(&self) -> i64 {
        0
    }
    fn at_st(&self) -> i64 {
        0
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        None
    }
}

/// Go `newHistogramSeries(histograms).Iterator(nil)`.
fn series_iterator(histograms: &[Histogram]) -> AnyIterator<'_> {
    AnyIterator::Other(Box::new(HistogramIterator { i: -1, histograms }))
}

fn stale_histogram() -> Histogram {
    Histogram { sum: f64::from_bits(STALE_NAN), ..Default::default() }
}

#[test]
fn test_histogram_stats_decoding() {
    let h = generate_test_histogram_with_hint;
    let cases: Vec<(&str, Vec<Histogram>, Vec<CounterResetHint>)> = vec![
        (
            "unknown counter reset for later sample triggers detection",
            vec![h(0, NotCounterReset), h(1, UnknownCounterReset), h(2, CounterReset), h(2, UnknownCounterReset)],
            vec![NotCounterReset, NotCounterReset, CounterReset, NotCounterReset],
        ),
        (
            "unknown counter reset for first sample does not trigger detection",
            vec![h(0, UnknownCounterReset), h(1, UnknownCounterReset), h(2, CounterReset), h(2, UnknownCounterReset)],
            vec![UnknownCounterReset, NotCounterReset, CounterReset, NotCounterReset],
        ),
        (
            "stale sample before unknown reset hint",
            vec![h(0, NotCounterReset), h(1, UnknownCounterReset), stale_histogram(), h(1, UnknownCounterReset)],
            vec![NotCounterReset, NotCounterReset, UnknownCounterReset, NotCounterReset],
        ),
        ("unknown counter reset at the beginning", vec![h(1, UnknownCounterReset)], vec![UnknownCounterReset]),
        (
            "detect real counter reset",
            vec![h(2, UnknownCounterReset), h(1, UnknownCounterReset)],
            vec![UnknownCounterReset, CounterReset],
        ),
        (
            "detect real counter reset after stale NaN",
            vec![h(2, UnknownCounterReset), stale_histogram(), h(1, UnknownCounterReset)],
            vec![UnknownCounterReset, UnknownCounterReset, CounterReset],
        ),
    ];

    for (name, histograms, expected_hints) in &cases {
        let check = |stats_iterator: &mut HistogramStatsIterator<'_>| {
            let mut decoded_stats: Vec<FloatHistogram> = Vec::new();
            let mut typ = stats_iterator.next();
            while typ != ValueType::None {
                assert_eq!(ValueType::FloatHistogram, typ, "{}", name);
                let (t1, h1) = stats_iterator.at_float_histogram(None);
                // Call AtFloatHistogram again to check for idempotency.
                let (t2, h2) = stats_iterator.at_float_histogram(None);
                assert_eq!(t1, t2, "{}", name);
                assert!(h1.equals(&h2), "{}", name);
                decoded_stats.push(h1);
                typ = stats_iterator.next();
            }
            assert!(stats_iterator.err().is_none(), "{}", name);
            for i in 0..histograms.len() {
                assert_eq!(expected_hints[i], decoded_stats[i].counter_reset_hint, "{} [{}]", name, i);
                let fh = histograms[i].to_float();
                if is_stale_nan(fh.sum) {
                    assert!(is_stale_nan(decoded_stats[i].sum), "{} [{}]", name, i);
                    assert_eq!(0.0, decoded_stats[i].count, "{} [{}]", name, i);
                } else {
                    assert_eq!(fh.count, decoded_stats[i].count, "{} [{}]", name, i);
                    assert_eq!(fh.sum, decoded_stats[i].sum, "{} [{}]", name, i);
                }
            }
        };

        // Check that we get the expected results with a fresh iterator.
        let mut stats_iterator = HistogramStatsIterator::new(series_iterator(histograms));
        check(&mut stats_iterator);

        // Check that we get the same results if we reset and reuse that iterator.
        stats_iterator.reset(series_iterator(histograms));
        check(&mut stats_iterator);
    }
}

#[test]
fn test_histogram_stats_mixed_use() {
    let histograms = vec![
        generate_test_histogram_with_hint(2, UnknownCounterReset),
        generate_test_histogram_with_hint(4, UnknownCounterReset),
        generate_test_histogram_with_hint(0, UnknownCounterReset),
    ];

    let mut stats_iterator = HistogramStatsIterator::new(series_iterator(&histograms));

    let expected_hints = vec![UnknownCounterReset, NotCounterReset, CounterReset];
    // Note that statsIterator always returns float histograms.
    let mut actual_hints = vec![UnknownCounterReset; 3];
    let typ = stats_iterator.next();
    assert_eq!(ValueType::FloatHistogram, typ);
    let (_, h) = stats_iterator.at_float_histogram(None);
    actual_hints[0] = h.counter_reset_hint;
    let typ = stats_iterator.next();
    assert_eq!(ValueType::FloatHistogram, typ);
    let (_, h) = stats_iterator.at_float_histogram(None);
    // We call AtFloatHistogram here again "randomly" to check idempotency.
    let (_, h2) = stats_iterator.at_float_histogram(None);
    assert!(h.equals(&h2));
    actual_hints[1] = h.counter_reset_hint;
    let typ = stats_iterator.next();
    assert_eq!(ValueType::FloatHistogram, typ);
    let (_, fh) = stats_iterator.at_float_histogram(None);
    actual_hints[2] = fh.counter_reset_hint;

    assert_eq!(ValueType::None, stats_iterator.next());
    assert_eq!(expected_hints, actual_hints);
}
