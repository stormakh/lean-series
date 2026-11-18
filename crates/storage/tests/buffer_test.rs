//! Port of `storage/buffer_test.go`.

use std::cell::Cell;

use chunkenc::{AnyIterator, ChunkIterator, ValueType};
use model::histogram::{FloatHistogram, Histogram};
use storage::tsdbutil::*;
use storage::*;

/// A small deterministic generator (Go uses the global math/rand).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let x = self.0;
        (x ^ (x >> 33)).wrapping_mul(0xff51afd7ed558ccd) ^ (x >> 29)
    }
}

#[test]
fn test_sample_ring() {
    struct Case {
        input: Vec<i64>,
        delta: i64,
        size: usize,
    }
    let cases = [
        Case {
            input: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            delta: 2,
            size: 1,
        },
        Case {
            input: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            delta: 2,
            size: 2,
        },
        Case {
            input: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            delta: 7,
            size: 3,
        },
        Case {
            input: vec![1, 2, 3, 4, 5, 16, 17, 18, 19, 20],
            delta: 7,
            size: 1,
        },
        Case {
            input: vec![1, 2, 3, 4, 6],
            delta: 4,
            size: 4,
        },
    ];
    let mut rng = Rng(42);
    for c in &cases {
        let mut r = SampleRing::new(c.delta, c.size, ValueType::Float);

        let mut input = Vec::new();
        for &t in &c.input {
            // Randomize the start timestamp to make sure it does not affect the outcome.
            input.push(Sample::float(
                (rng.next() >> 1) as i64,
                t,
                (rng.next() % 100) as f64,
            ));
        }

        for (i, s) in input.iter().enumerate() {
            r.add(s);
            let buffered = r.samples();

            for sold in &input[..i] {
                let found = buffered
                    .iter()
                    .any(|bs| bs.t() == sold.t && bs.f() == sold.f);
                if found {
                    assert!(
                        sold.t >= s.t - c.delta,
                        "{}: unexpected sample {} in buffer; buffer {:?}",
                        i,
                        sold.t,
                        buffered
                    );
                } else {
                    assert!(
                        sold.t < s.t - c.delta,
                        "{}: expected sample {} to be in buffer but was not; buffer {:?}",
                        i,
                        sold.t,
                        buffered
                    );
                }
            }
        }
    }
}

#[test]
fn test_sample_ring_float_st() {
    let mut r = SampleRing::new(10, 5, ValueType::None);
    assert_eq!(r.buf_lens(), [0, 0, 0, 0]);

    r.add_f(FSample {
        st: 100,
        t: 11,
        f: 3.14,
    });
    let mut it = r.iterator();

    assert_eq!(it.next(), ValueType::Float);
    let (ts, f) = it.at();
    assert_eq!(ts, 11);
    assert_eq!(f, 3.14);
    assert_eq!(it.at_st(), 100);
    assert_eq!(it.next(), ValueType::None);
}

#[test]
fn test_sample_ring_mixed() {
    let h1 = generate_test_histogram(1);
    let h2 = generate_test_histogram(2);

    // With ValNone as the preferred type, nothing should be initialized.
    let mut r = SampleRing::new(10, 2, ValueType::None);
    assert_eq!(r.buf_lens(), [0, 0, 0, 0]);

    // But then mixed adds should work as expected.
    r.add_f(FSample {
        st: 10,
        t: 11,
        f: 3.14,
    });
    r.add_h(20, 21, &h1);

    let mut it = r.iterator();
    assert_eq!(it.next(), ValueType::Float);
    let (ts, f) = it.at();
    assert_eq!(ts, 11);
    assert_eq!(f, 3.14);
    assert_eq!(it.at_st(), 10);
    assert_eq!(it.next(), ValueType::Histogram);
    let (ts, h) = it.at_histogram();
    assert_eq!(ts, 21);
    assert_eq!(h, Some(&h1));
    assert_eq!(it.at_st(), 20);
    assert_eq!(it.next(), ValueType::None);

    r.reset();
    let mut it = r.iterator();
    assert_eq!(it.next(), ValueType::None);

    r.add_f(FSample {
        st: 30,
        t: 31,
        f: 4.2,
    });
    r.add_h(40, 41, &h2);

    let mut it = r.iterator();
    assert_eq!(it.next(), ValueType::Float);
    let (ts, f) = it.at();
    assert_eq!(ts, 31);
    assert_eq!(f, 4.2);
    assert_eq!(it.at_st(), 30);
    assert_eq!(it.next(), ValueType::Histogram);
    let (ts, h) = it.at_histogram();
    assert_eq!(ts, 41);
    assert_eq!(h, Some(&h2));
    assert_eq!(it.at_st(), 40);
    assert_eq!(it.next(), ValueType::None);
}

#[test]
fn test_sample_ring_at_float_histogram() {
    let fh1 = generate_test_float_histogram(1);
    let fh2 = generate_test_float_histogram(2);
    let h1 = generate_test_histogram(3);
    let h2 = generate_test_histogram(4);

    // With ValNone as the preferred type, nothing should be initialized.
    let mut r = SampleRing::new(10, 2, ValueType::None);
    assert_eq!(r.buf_lens(), [0, 0, 0, 0]);

    let mut fh: Option<FloatHistogram> = None;

    let mut it = r.iterator();
    assert_eq!(it.next(), ValueType::None);

    r.add_fh(10, 11, &fh1);
    r.add_fh(20, 21, &fh2);

    let mut it = r.iterator();
    assert_eq!(it.next(), ValueType::FloatHistogram);
    let (ts, got) = it.at_float_histogram(fh.take());
    assert_eq!(ts, 11);
    assert_eq!(got, fh1);
    fh = Some(got);
    assert_eq!(it.at_st(), 10);
    assert_eq!(it.next(), ValueType::FloatHistogram);
    let (ts, got) = it.at_float_histogram(fh.take());
    assert_eq!(ts, 21);
    assert_eq!(got, fh2);
    fh = Some(got);
    assert_eq!(it.at_st(), 20);
    assert_eq!(it.next(), ValueType::None);

    r.reset();
    let mut it = r.iterator();
    assert_eq!(it.next(), ValueType::None);

    r.add_h(30, 31, &h1);
    r.add_h(40, 41, &h2);

    let mut it = r.iterator();
    assert_eq!(it.next(), ValueType::Histogram);
    let (ts, h) = it.at_histogram();
    assert_eq!(ts, 31);
    assert_eq!(h, Some(&h1));
    assert_eq!(it.at_st(), 30);
    let (ts, got) = it.at_float_histogram(fh.take());
    assert_eq!(ts, 31);
    assert_eq!(got, h1.to_float());
    fh = Some(got);
    assert_eq!(it.at_st(), 30);
    assert_eq!(it.next(), ValueType::Histogram);
    let (ts, h) = it.at_histogram();
    assert_eq!(ts, 41);
    assert_eq!(h, Some(&h2));
    assert_eq!(it.at_st(), 40);
    let (ts, got) = it.at_float_histogram(fh.take());
    assert_eq!(ts, 41);
    assert_eq!(got, h2.to_float());
    assert_eq!(it.at_st(), 40);
    assert_eq!(it.next(), ValueType::None);
}

#[test]
fn test_buffered_series_iterator() {
    fn buffer_eq(it: &BufferedSeriesIterator<'_>, exp: &[FSample]) {
        let mut b = Vec::new();
        let mut bit = it.buffer();
        while bit.next() == ValueType::Float {
            let (t, f) = bit.at();
            let st = bit.at_st();
            b.push(FSample { st, t, f });
        }
        assert_eq!(b, exp, "buffer mismatch");
    }
    fn sample_eq(it: &BufferedSeriesIterator<'_>, est: i64, ets: i64, ev: f64) {
        let (ts, v) = it.at();
        let st = it.at_st();
        assert_eq!(st, est, "start timestamp mismatch");
        assert_eq!(ts, ets, "timestamp mismatch");
        assert_eq!(v, ev, "value mismatch");
    }
    fn prev_sample_eq(it: &BufferedSeriesIterator<'_>, est: i64, ets: i64, ev: f64, eok: bool) {
        // adapted: Go returns a zero sample with ok=false; Rust returns None.
        let (s, ok) = match it.peek_back(1) {
            Some(s) => (s, true),
            None => (Sample::default(), false),
        };
        assert_eq!(ok, eok, "exist mismatch");
        assert_eq!(s.st(), est, "start timestamp mismatch");
        assert_eq!(s.t(), ets, "timestamp mismatch");
        assert_eq!(s.f(), ev, "value mismatch");
    }

    let samples = vec![
        Sample::float(-1, 1, 2.0),
        Sample::float(1, 2, 3.0),
        Sample::float(2, 3, 4.0),
        Sample::float(3, 4, 5.0),
        Sample::float(3, 5, 6.0),
        Sample::float(50, 99, 8.0),
        Sample::float(99, 100, 9.0),
        Sample::float(100, 101, 10.0),
    ];
    let mut it = new_buffer_iterator(new_list_series_iterator(&samples), 2);

    assert_eq!(it.seek(-123), ValueType::Float, "seek failed");
    sample_eq(&it, -1, 1, 2.0);
    prev_sample_eq(&it, 0, 0, 0.0, false);
    buffer_eq(&it, &[]);

    assert_eq!(it.next(), ValueType::Float, "next failed");
    sample_eq(&it, 1, 2, 3.0);
    prev_sample_eq(&it, -1, 1, 2.0, true);
    buffer_eq(
        &it,
        &[FSample {
            st: -1,
            t: 1,
            f: 2.0,
        }],
    );

    assert_eq!(it.next(), ValueType::Float, "next failed");
    assert_eq!(it.next(), ValueType::Float, "next failed");
    assert_eq!(it.next(), ValueType::Float, "next failed");
    sample_eq(&it, 3, 5, 6.0);
    prev_sample_eq(&it, 3, 4, 5.0, true);
    buffer_eq(
        &it,
        &[
            FSample {
                st: 1,
                t: 2,
                f: 3.0,
            },
            FSample {
                st: 2,
                t: 3,
                f: 4.0,
            },
            FSample {
                st: 3,
                t: 4,
                f: 5.0,
            },
        ],
    );

    assert_eq!(it.seek(5), ValueType::Float, "seek failed");
    sample_eq(&it, 3, 5, 6.0);
    prev_sample_eq(&it, 3, 4, 5.0, true);
    buffer_eq(
        &it,
        &[
            FSample {
                st: 1,
                t: 2,
                f: 3.0,
            },
            FSample {
                st: 2,
                t: 3,
                f: 4.0,
            },
            FSample {
                st: 3,
                t: 4,
                f: 5.0,
            },
        ],
    );

    assert_eq!(it.seek(101), ValueType::Float, "seek failed");
    sample_eq(&it, 100, 101, 10.0);
    prev_sample_eq(&it, 99, 100, 9.0, true);
    buffer_eq(
        &it,
        &[
            FSample {
                st: 50,
                t: 99,
                f: 8.0,
            },
            FSample {
                st: 99,
                t: 100,
                f: 9.0,
            },
        ],
    );

    assert_eq!(it.next(), ValueType::None, "next succeeded unexpectedly");
    assert_eq!(
        it.seek(1024),
        ValueType::None,
        "seek succeeded unexpectedly"
    );
}

/// Go `mockSeriesIterator`.
struct MockSeriesIterator<'a> {
    seek: Box<dyn FnMut(i64) -> ValueType + 'a>,
    at: Box<dyn Fn() -> (i64, f64) + 'a>,
    next: Box<dyn FnMut() -> ValueType + 'a>,
}

impl ChunkIterator for MockSeriesIterator<'_> {
    fn seek(&mut self, t: i64) -> ValueType {
        (self.seek)(t)
    }
    fn at(&self) -> (i64, f64) {
        (self.at)()
    }
    fn next(&mut self) -> ValueType {
        (self.next)()
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        None
    }
    fn at_histogram(&self, _: Option<Histogram>) -> (i64, Histogram) {
        (0, Histogram::default()) // Not really mocked.
    }
    fn at_float_histogram(&self, _: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        (0, FloatHistogram::default()) // Not really mocked.
    }
    fn at_t(&self) -> i64 {
        0 // Not really mocked.
    }
    fn at_st(&self) -> i64 {
        0 // Not really mocked.
    }
}

/// At() should not be called once Next() returns false.
#[test]
fn test_buffered_series_iterator_no_bad_at() {
    let done = Cell::new(false);
    let m = MockSeriesIterator {
        seek: Box::new(|_| ValueType::None),
        at: Box::new(|| {
            assert!(!done.get(), "unexpectedly done");
            done.set(true);
            (0, 0.0)
        }),
        next: Box::new(|| {
            if done.get() {
                ValueType::None
            } else {
                ValueType::Float
            }
        }),
    };
    let mut it = new_buffer_iterator(AnyIterator::Other(Box::new(m)), 60);
    it.next();
    it.next();
}

#[test]
fn test_buffered_series_iterator_mixed_histograms() {
    let histograms = generate_test_histograms(2);
    let samples = vec![
        Sample::float_histogram(0, 1, histograms[0].to_float()),
        Sample::histogram(0, 2, histograms[1].clone()),
    ];
    let mut it = new_buffer_iterator(new_list_series_iterator(&samples), 2);

    assert_eq!(it.seek(3), ValueType::None);
    assert!(it.err().is_none());

    let mut buf = it.buffer();

    assert_eq!(buf.next(), ValueType::FloatHistogram);
    let (_, fh) = buf.at_float_histogram(None);
    assert_eq!(fh, histograms[0].to_float());

    assert_eq!(buf.next(), ValueType::Histogram);
    let (_, fh) = buf.at_float_histogram(None);
    assert_eq!(fh, histograms[1].to_float());
}

#[test]
fn test_buffered_series_iterator_mixed_floats_and_histograms() {
    let histograms = generate_test_histograms(5);
    let samples = vec![
        Sample::histogram(0, 1, histograms[0].copy()),
        Sample::float(0, 2, 2.0),
        Sample::histogram(0, 3, histograms[1].copy()),
        Sample::histogram(0, 4, histograms[2].copy()),
        Sample::float_histogram(0, 3, histograms[3].to_float()),
        Sample::float_histogram(0, 4, histograms[4].to_float()),
    ];
    let mut it = new_buffer_iterator(new_list_series_iterator_with_copy(&samples), 6);

    assert_eq!(it.seek(7), ValueType::None);
    assert!(it.err().is_none());

    let mut buf = it.buffer();

    assert_eq!(buf.next(), ValueType::Histogram);
    let (_, h0) = buf.at_histogram();
    assert_eq!(h0, Some(&histograms[0]));

    assert_eq!(buf.next(), ValueType::Float);
    let (_, v) = buf.at();
    assert_eq!(v, 2.0);

    assert_eq!(buf.next(), ValueType::Histogram);
    let (_, h1) = buf.at_histogram();
    assert_eq!(h1, Some(&histograms[1]));

    assert_eq!(buf.next(), ValueType::Histogram);
    let (_, h2) = buf.at_histogram();
    assert_eq!(h2, Some(&histograms[2]));

    assert_eq!(buf.next(), ValueType::FloatHistogram);
    let (_, h3) = buf.at_float_histogram(None);
    assert_eq!(h3, histograms[3].to_float());

    assert_eq!(buf.next(), ValueType::FloatHistogram);
    let (_, h4) = buf.at_float_histogram(None);
    assert_eq!(h4, histograms[4].to_float());

    // Test for the overwrite bug where the buffered histogram was reused between items in the buffer.
    assert_eq!(h0, Some(&histograms[0]));
    assert_eq!(h1, Some(&histograms[1]));
    assert_eq!(h2, Some(&histograms[2]));
    assert_eq!(h3, histograms[3].to_float());
    assert_eq!(h4, histograms[4].to_float());
}
