//! Port of `tsdb/chunkenc/histogram_meta_test.go`.

use chunkenc::{
    BucketIterator, Insert, XorValue, adjust_for_inserts, count_spans, expand_float_spans_and_buckets,
    expand_int_spans_and_buckets, expand_spans_both_ways, insert, read_histogram_chunk_layout,
    write_histogram_chunk_layout,
};
use chunkenc::{Bstream, BstreamReader};
use model::histogram::{CUSTOM_BUCKETS_SCHEMA, Span, ldexp};

fn sp(offset: i32, length: u32) -> Span {
    Span { offset, length }
}

/// Go `Insert{pos: p, num: n}` (bucketIdx 0).
fn ins(pos: i64, num: i64) -> Insert {
    Insert::new(pos, num, 0)
}

/// Go `Insert{pos: p, num: n, bucketIdx: i}`.
fn insi(pos: i64, num: i64, bucket_idx: i64) -> Insert {
    Insert::new(pos, num, bucket_idx)
}

// Example of a span layout and resulting bucket indices (_idx_ is used in this
// histogram, others are shown just for context):
//
//   spans      : [offset: 0, length: 2]                [offset 1, length 1]
//   bucket idx : _0_                _1_      2         [3]                  4 ...

#[test]
fn test_bucket_iterator() {
    struct Test {
        spans: Vec<Span>,
        idxs: Vec<i64>,
    }
    let tests = vec![
        Test { spans: vec![sp(0, 1)], idxs: vec![0] },
        Test { spans: vec![sp(0, 2), sp(1, 1)], idxs: vec![0, 1, 3] },
        Test {
            spans: vec![sp(100, 4), sp(8, 7), sp(0, 1)],
            idxs: vec![100, 101, 102, 103, 112, 113, 114, 115, 116, 117, 118, 119],
        },
        // The below 2 sets ore the ones described in expandFloatSpansAndBuckets's comments.
        Test { spans: vec![sp(0, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)], idxs: vec![0, 1, 4, 8, 9, 13, 15] },
        Test {
            spans: vec![sp(0, 3), sp(1, 1), sp(1, 4), sp(3, 3)],
            idxs: vec![0, 1, 2, 4, 6, 7, 8, 9, 13, 14, 15],
        },
    ];
    for test in &tests {
        let mut b = BucketIterator::new(&test.spans);
        let mut got = Vec::new();
        while let Some(v) = b.next() {
            got.push(v);
        }
        assert_eq!(test.idxs, got);
    }
}

#[test]
fn test_expand_spans_both_ways_and_insert() {
    struct Scenario {
        description: &'static str,
        spans_a: Vec<Span>,
        spans_b: Vec<Span>,
        f_inserts: Vec<Insert>,
        b_inserts: Vec<Insert>,
        buckets_in: Vec<i64>,
        buckets_out: Vec<i64>,
        merged_spans: Vec<Span>,
    }
    let scenarios = vec![
        Scenario {
            description: "single prepend at the beginning",
            spans_a: vec![sp(-10, 3)],
            spans_b: vec![sp(-11, 4)],
            f_inserts: vec![ins(0, 1)],
            b_inserts: vec![],
            buckets_in: vec![6, -3, 0],
            buckets_out: vec![0, 6, -3, 0],
            merged_spans: vec![sp(-11, 4)],
        },
        Scenario {
            description: "single append at the end",
            spans_a: vec![sp(-10, 3)],
            spans_b: vec![sp(-10, 4)],
            f_inserts: vec![ins(3, 1)],
            b_inserts: vec![],
            buckets_in: vec![6, -3, 0],
            buckets_out: vec![6, -3, 0, -3],
            merged_spans: vec![sp(-10, 4)],
        },
        Scenario {
            description: "double prepend at the beginning",
            spans_a: vec![sp(-10, 3)],
            spans_b: vec![sp(-12, 5)],
            f_inserts: vec![ins(0, 2)],
            b_inserts: vec![],
            buckets_in: vec![6, -3, 0],
            buckets_out: vec![0, 0, 6, -3, 0],
            merged_spans: vec![sp(-12, 5)],
        },
        Scenario {
            description: "double append at the end",
            spans_a: vec![sp(-10, 3)],
            spans_b: vec![sp(-10, 5)],
            f_inserts: vec![ins(3, 2)],
            b_inserts: vec![],
            buckets_in: vec![6, -3, 0],
            buckets_out: vec![6, -3, 0, -3, 0],
            merged_spans: vec![sp(-10, 5)],
        },
        Scenario {
            description: "double prepond at the beginning and double append at the end",
            spans_a: vec![sp(-10, 3)],
            spans_b: vec![sp(-12, 7)],
            f_inserts: vec![ins(0, 2), ins(3, 2)],
            b_inserts: vec![],
            buckets_in: vec![6, -3, 0],
            buckets_out: vec![0, 0, 6, -3, 0, -3, 0],
            merged_spans: vec![sp(-12, 7)],
        },
        Scenario {
            description: "single removal of bucket at the start",
            spans_a: vec![sp(-10, 4)],
            spans_b: vec![sp(-9, 3)],
            f_inserts: vec![],
            b_inserts: vec![ins(0, 1)],
            buckets_in: vec![1, 2, -1, 2],
            buckets_out: vec![1, 2, -1, 2],
            merged_spans: vec![sp(-10, 4)],
        },
        Scenario {
            description: "single removal of bucket in the middle",
            spans_a: vec![sp(-10, 4)],
            spans_b: vec![sp(-10, 2), sp(1, 1)],
            f_inserts: vec![],
            b_inserts: vec![ins(2, 1)],
            buckets_in: vec![1, 2, -1, 2],
            buckets_out: vec![1, 2, -1, 2],
            merged_spans: vec![sp(-10, 4)],
        },
        Scenario {
            description: "single removal of bucket at the end",
            spans_a: vec![sp(-10, 4)],
            spans_b: vec![sp(-10, 3)],
            f_inserts: vec![],
            b_inserts: vec![ins(3, 1)],
            merged_spans: vec![sp(-10, 4)],
            buckets_in: vec![1, 2, -1, 2],
            buckets_out: vec![1, 2, -1, 2],
        },
        Scenario {
            description: "as described in doc comment",
            spans_a: vec![sp(0, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)],
            spans_b: vec![sp(0, 3), sp(1, 1), sp(1, 4), sp(3, 3)],
            f_inserts: vec![ins(2, 1), ins(3, 2), ins(6, 1)],
            b_inserts: vec![],
            buckets_in: vec![6, -3, 0, -1, 2, 1, -4],
            buckets_out: vec![6, -3, -3, 3, -3, 0, 2, 2, 1, -5, 1],
            merged_spans: vec![sp(0, 3), sp(1, 1), sp(1, 4), sp(3, 3)],
        },
        Scenario {
            description: "both forward and backward inserts, complex case",
            spans_a: vec![sp(0, 2), sp(2, 1), sp(3, 2), sp(3, 1), sp(1, 1)],
            spans_b: vec![sp(1, 2), sp(1, 1), sp(1, 2), sp(1, 1), sp(4, 1)],
            f_inserts: vec![ins(2, 1), ins(3, 2), ins(6, 1)],
            b_inserts: vec![ins(0, 1), ins(5, 1), ins(6, 1), ins(7, 1)],
            buckets_in: vec![1, 2, -1, 2, 0, 3, 1],
            buckets_out: vec![1, 2, -3, 2, -2, 0, 4, 0, 3, -7, 8],
            merged_spans: vec![sp(0, 3), sp(1, 1), sp(1, 4), sp(3, 3)],
        },
        Scenario {
            description: "inserts with gaps",
            spans_a: vec![sp(-19, 2), sp(1, 2)],
            spans_b: vec![sp(-19, 1), sp(4, 1), sp(3, 1)],
            f_inserts: vec![ins(4, 2)],
            b_inserts: vec![ins(1, 3)],
            buckets_in: vec![1, 2, -1, 1],
            buckets_out: vec![1, 2, -1, 1, -3, 0],
            merged_spans: vec![sp(-19, 2), sp(1, 3), sp(3, 1)],
        },
    ];

    for s in &scenarios {
        let d = s.description;
        let (f_inserts, b_inserts, m) = expand_spans_both_ways(&s.spans_a, &s.spans_b);
        assert_eq!(s.f_inserts, f_inserts, "{d}");
        assert_eq!(s.b_inserts, b_inserts, "{d}");
        assert_eq!(s.merged_spans, m, "{d}");

        let mut got_buckets = vec![0i64; s.buckets_out.len()];
        insert(&s.buckets_in, &mut got_buckets, &f_inserts, true);
        assert_eq!(s.buckets_out, got_buckets, "{d}");

        let mut float_buckets_in = vec![0f64; s.buckets_in.len()];
        let mut last = s.buckets_in[0];
        float_buckets_in[0] = last as f64;
        for i in 1..float_buckets_in.len() {
            last += s.buckets_in[i];
            float_buckets_in[i] = last as f64;
        }
        let mut float_buckets_out = vec![0f64; s.buckets_out.len()];
        last = s.buckets_out[0];
        float_buckets_out[0] = last as f64;
        for i in 1..float_buckets_out.len() {
            last += s.buckets_out[i];
            float_buckets_out[i] = last as f64;
        }
        let mut got_float_buckets = vec![0f64; float_buckets_out.len()];
        insert(&float_buckets_in, &mut got_float_buckets, &f_inserts, false);
        assert_eq!(float_buckets_out, got_float_buckets, "{d}");
    }
}

#[test]
fn test_write_read_histogram_chunk_layout() {
    struct Layout {
        schema: i32,
        zero_threshold: f64,
        positive_spans: Vec<Span>,
        negative_spans: Vec<Span>,
        /// `None` is Go's nil slice.
        custom_values: Option<Vec<f64>>,
    }
    let layouts = vec![
        Layout {
            schema: 3,
            zero_threshold: 0.0,
            positive_spans: vec![sp(-4, 3), sp(2, 42)],
            negative_spans: vec![],
            custom_values: None,
        },
        Layout {
            schema: -2,
            zero_threshold: 2.938735877055719e-39, // Default value in client_golang.
            positive_spans: vec![],
            negative_spans: vec![sp(2, 5), sp(1, 34)],
            custom_values: None,
        },
        Layout {
            schema: 6,
            zero_threshold: 1024.0, // The largest power of two we can encode in one byte.
            positive_spans: vec![],
            negative_spans: vec![],
            custom_values: None,
        },
        Layout {
            schema: 6,
            zero_threshold: 1025.0,
            positive_spans: vec![sp(2, 5), sp(1, 34), sp(0, 0)], // Weird span.
            negative_spans: vec![sp(-345, 4545), sp(53645665, 345), sp(945995, 85848)],
            custom_values: None,
        },
        Layout {
            schema: 6,
            zero_threshold: 2048.0,
            positive_spans: vec![],
            negative_spans: vec![],
            custom_values: None,
        },
        Layout {
            schema: 0,
            zero_threshold: ldexp(0.5, -242), // The smallest power of two we can encode in one byte.
            positive_spans: vec![sp(-4, 3)],
            negative_spans: vec![sp(2, 5), sp(1, 34)],
            custom_values: None,
        },
        Layout {
            schema: 0,
            zero_threshold: ldexp(0.5, -243),
            positive_spans: vec![sp(-4, 3)],
            negative_spans: vec![sp(2, 5), sp(1, 34)],
            custom_values: None,
        },
        Layout {
            schema: 4,
            zero_threshold: 42.0, // Not a power of two.
            positive_spans: vec![],
            negative_spans: vec![],
            custom_values: None,
        },
        Layout {
            schema: CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            positive_spans: vec![sp(-4, 3), sp(2, 42)],
            negative_spans: vec![],
            custom_values: Some(vec![
                -5.0, -2.5, 0.0, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 255.0, 500.0, 1000.0, 50000.0,
                1e7,
            ]),
        },
        Layout {
            schema: CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            positive_spans: vec![sp(-4, 3), sp(2, 42)],
            negative_spans: vec![],
            custom_values: Some(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0,
            ]),
        },
        Layout {
            schema: CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            positive_spans: vec![sp(-4, 3), sp(2, 42)],
            negative_spans: vec![],
            custom_values: Some(vec![
                0.001, 0.002, 0.004, 0.008, 0.016, 0.032, 0.064, 0.128, 0.256, 0.512, 1.024, 2.048, 4.096, 8.192,
            ]),
        },
        Layout {
            schema: CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            positive_spans: vec![sp(-4, 3), sp(2, 42)],
            negative_spans: vec![],
            custom_values: Some(vec![1.001, 1.023, 2.01, 4.007, 4.095, 8.001, 8.19, 16.24]),
        },
    ];

    let mut bs = Bstream::default();

    for l in &layouts {
        write_histogram_chunk_layout(
            &mut bs,
            l.schema,
            l.zero_threshold,
            &l.positive_spans,
            &l.negative_spans,
            l.custom_values.as_deref().unwrap_or(&[]),
        );
    }

    let mut bsr = BstreamReader::new(bs.bytes());

    for want in &layouts {
        let got = read_histogram_chunk_layout(&mut bsr).expect("read layout");
        assert_eq!(want.schema, got.schema);
        assert_eq!(want.zero_threshold, got.zero_threshold);
        assert_eq!(want.positive_spans, got.positive_spans);
        assert_eq!(want.negative_spans, got.negative_spans);
        let got_custom = got.custom_values.as_deref().map(|s| s.to_vec());
        assert_eq!(want.custom_values, got_custom);
    }
}

#[test]
fn test_spans_from_bidirectional_compare_spans() {
    struct Case {
        s1: Vec<Span>,
        s2: Vec<Span>,
        exp: Vec<Span>,
    }
    let cases = vec![
        // All empty.
        Case { s1: vec![], s2: vec![], exp: vec![] },
        // Same spans.
        Case { s1: vec![], s2: vec![], exp: vec![] },
        Case {
            // Has the cases of
            // 1.  |----|        (partial overlap)
            //        |----|
            //
            // 2.       |-----|  (no gap but no overlap as well)
            //     |---|
            //
            // 3.  |----|        (complete overlap)
            //     |----|
            s1: vec![sp(0, 3), sp(3, 3), sp(5, 3)],
            s2: vec![sp(0, 2), sp(2, 2), sp(2, 3), sp(3, 3)],
            exp: vec![sp(0, 3), sp(1, 7), sp(3, 3)],
        },
        Case {
            // s1 is superset of s2.
            s1: vec![sp(0, 3), sp(3, 5), sp(3, 3)],
            s2: vec![sp(0, 2), sp(5, 3), sp(4, 3)],
            exp: vec![sp(0, 3), sp(3, 5), sp(3, 3)],
        },
        Case {
            // No overlaps but one span is side by side.
            s1: vec![sp(0, 3), sp(3, 3), sp(5, 3)],
            s2: vec![sp(3, 3), sp(4, 2)],
            exp: vec![sp(0, 9), sp(1, 2), sp(2, 3)],
        },
        Case {
            // No buckets in one of them.
            s1: vec![sp(0, 3), sp(3, 3), sp(5, 3)],
            s2: vec![],
            exp: vec![sp(0, 3), sp(3, 3), sp(5, 3)],
        },
        Case {
            // Zero length spans.
            s1: vec![
                sp(-5, 0),
                sp(2, 0),
                sp(3, 3),
                sp(1, 0),
                sp(2, 3),
                sp(2, 0),
                sp(2, 0),
                sp(1, 3),
                sp(4, 0),
                sp(5, 0),
            ],
            s2: vec![sp(0, 2), sp(2, 2), sp(1, 0), sp(1, 3), sp(3, 3)],
            exp: vec![sp(0, 3), sp(1, 7), sp(3, 3)],
        },
    ];

    for c in &cases {
        let s1c = c.s1.clone();
        let s2c = c.s2.clone();

        let (_, _, act) = expand_spans_both_ways(&c.s1, &c.s2);
        assert_eq!(c.exp, act);
        // Check that s1 and s2 are not modified.
        assert_eq!(s1c, c.s1);
        assert_eq!(s2c, c.s2);
        let (_, _, act) = expand_spans_both_ways(&c.s2, &c.s1);
        assert_eq!(c.exp, act);
    }
}

#[test]
fn test_expand_int_or_float_spans_and_buckets() {
    struct TestCase {
        spans_a: Vec<Span>,
        buckets_a: Vec<i64>,
        spans_b: Vec<Span>,
        buckets_b: Vec<i64>,

        expect_reset: bool,
        expect_forward_inserts: Vec<Insert>,
        expect_backward_inserts: Vec<Insert>,
        expect_merged_spans: Vec<Span>,
        expect_buckets_a: Vec<i64>,
        expect_buckets_b: Vec<i64>,
    }
    /// A reset case (Go leaves the expect* fields at their zero values).
    fn reset(spans_a: Vec<Span>, buckets_a: Vec<i64>, spans_b: Vec<Span>, buckets_b: Vec<i64>) -> TestCase {
        TestCase {
            spans_a,
            buckets_a,
            spans_b,
            buckets_b,
            expect_reset: true,
            expect_forward_inserts: vec![],
            expect_backward_inserts: vec![],
            expect_merged_spans: vec![],
            expect_buckets_a: vec![],
            expect_buckets_b: vec![],
        }
    }

    let test_cases: Vec<(&str, TestCase)> = vec![
        (
            "empty",
            TestCase {
                spans_a: vec![],
                buckets_a: vec![],
                spans_b: vec![],
                buckets_b: vec![],
                expect_reset: false,
                expect_forward_inserts: vec![],
                expect_backward_inserts: vec![],
                expect_merged_spans: vec![],
                expect_buckets_a: vec![],
                expect_buckets_b: vec![],
            },
        ),
        ("single bucket reset to none", reset(vec![sp(1, 1)], vec![1], vec![], vec![])),
        ("single bucket reset to lower", reset(vec![sp(1, 1)], vec![2], vec![sp(1, 1)], vec![1])),
        (
            "single bucket increase",
            TestCase {
                spans_a: vec![sp(1, 1)],
                buckets_a: vec![1],
                spans_b: vec![sp(1, 1)],
                buckets_b: vec![2],
                expect_reset: false,
                expect_forward_inserts: vec![],
                expect_backward_inserts: vec![],
                expect_merged_spans: vec![sp(1, 1)],
                expect_buckets_a: vec![1],
                expect_buckets_b: vec![2],
            },
        ),
        (
            "distinct new buckets and increase",
            // A:  ___1_____
            // B:  22_22___2
            // B': 22_22___2
            TestCase {
                spans_a: vec![sp(1, 1)],
                buckets_a: vec![1],
                spans_b: vec![sp(-2, 2), sp(1, 2), sp(3, 1)],
                buckets_b: vec![2, 0, 0, 0, 0],
                expect_reset: false,
                expect_forward_inserts: vec![insi(0, 2, -2), insi(1, 1, 2), insi(1, 1, 6)],
                expect_backward_inserts: vec![],
                expect_merged_spans: vec![sp(-2, 2), sp(1, 2), sp(3, 1)],
                expect_buckets_a: vec![0, 0, 1, -1, 0],
                expect_buckets_b: vec![2, 0, 0, 0, 0],
            },
        ),
        (
            "distinct new buckets but reset",
            // A: ___2_____
            // B: 11_11___1
            reset(vec![sp(1, 1)], vec![2], vec![sp(-2, 2), sp(1, 2), sp(3, 1)], vec![1, 0, 0, 0, 0]),
        ),
        (
            "distinct new buckets but missing",
            // A: ___2_____
            // B: 11__1___1
            reset(vec![sp(1, 1)], vec![2], vec![sp(-2, 2), sp(2, 1), sp(3, 1)], vec![1, 0, 0, 0]),
        ),
        (
            "distinct new buckets and missing an empty bucket",
            // A:  _0__
            // B:  ___1
            // B': _0_1
            TestCase {
                spans_a: vec![sp(1, 1)],
                buckets_a: vec![0],
                spans_b: vec![sp(3, 1)],
                buckets_b: vec![1],
                expect_reset: false,
                expect_forward_inserts: vec![insi(1, 1, 3)],
                expect_backward_inserts: vec![insi(0, 1, 1)],
                expect_merged_spans: vec![sp(1, 1), sp(1, 1)],
                expect_buckets_a: vec![0, 0],
                expect_buckets_b: vec![0, 1],
            },
        ),
        (
            "distinct new buckets and missing multiple empty buckets",
            // Idx: 01234567890123
            // A:   _000_00__0__00
            // B;   ________1_____
            // B':  _000_00_10__00
            TestCase {
                spans_a: vec![sp(1, 3), sp(1, 2), sp(2, 1), sp(2, 2)],
                buckets_a: vec![0, 0, 0, 0, 0, 0, 0, 0],
                spans_b: vec![sp(8, 1)],
                buckets_b: vec![1],
                expect_reset: false,
                expect_forward_inserts: vec![insi(5, 1, 8)],
                expect_backward_inserts: vec![insi(0, 3, 1), insi(0, 2, 5), insi(1, 1, 9), insi(1, 2, 12)],
                expect_merged_spans: vec![sp(1, 3), sp(1, 2), sp(1, 2), sp(2, 2)],
                expect_buckets_a: vec![0, 0, 0, 0, 0, 0, 0, 0, 0],
                expect_buckets_b: vec![0, 0, 0, 0, 0, 1, -1, 0, 0],
            },
        ),
        (
            "overlap new buckets and missing multiple empty buckets",
            // Idx: 01234567890123
            // A:   _000_00_10__00
            // B;   ________2_____
            // B':  _000_00_20__00
            TestCase {
                spans_a: vec![sp(1, 3), sp(1, 2), sp(1, 2), sp(2, 2)],
                buckets_a: vec![0, 0, 0, 0, 0, 1, -1, 0, 0],
                spans_b: vec![sp(8, 1)],
                buckets_b: vec![2],
                expect_reset: false,
                expect_forward_inserts: vec![],
                expect_backward_inserts: vec![insi(0, 3, 1), insi(0, 2, 5), insi(1, 1, 9), insi(1, 2, 12)],
                expect_merged_spans: vec![sp(1, 3), sp(1, 2), sp(1, 2), sp(2, 2)],
                expect_buckets_a: vec![0, 0, 0, 0, 0, 1, -1, 0, 0],
                expect_buckets_b: vec![0, 0, 0, 0, 0, 2, -2, 0, 0],
            },
        ),
        (
            "overlap new buckets and missing multiple empty buckets with 0 length/offset spans",
            // Idx: 01234567890123
            // A:   _000_00_10__00
            // B;   ________2_____
            // B':  _000_00_20__00
            TestCase {
                spans_a: vec![sp(1, 3), sp(1, 2), sp(1, 2), sp(1, 0), sp(1, 2)],
                buckets_a: vec![0, 0, 0, 0, 0, 1, -1, 0, 0],
                spans_b: vec![sp(1, 0), sp(7, 1)],
                buckets_b: vec![2],
                expect_reset: false,
                expect_forward_inserts: vec![],
                expect_backward_inserts: vec![insi(0, 3, 1), insi(0, 2, 5), insi(1, 1, 9), insi(1, 2, 12)],
                expect_merged_spans: vec![sp(1, 3), sp(1, 2), sp(1, 2), sp(2, 2)],
                expect_buckets_a: vec![0, 0, 0, 0, 0, 1, -1, 0, 0],
                expect_buckets_b: vec![0, 0, 0, 0, 0, 2, -2, 0, 0],
            },
        ),
        (
            "new empty buckets between filled buckets",
            // A:  11212332____1__1
            // B:  122323321__11__1
            // A': 112123320__01__1
            // B': 122323321__11__1
            TestCase {
                spans_a: vec![sp(-51, 8), sp(11, 1), sp(14, 1)],
                buckets_a: vec![1, 0, 1, -1, 1, 1, 0, -1, -1, 0],
                spans_b: vec![sp(-51, 9), sp(9, 2), sp(14, 1)],
                buckets_b: vec![1, 1, 0, 1, -1, 1, 0, -1, -1, 0, 0, 0],
                expect_reset: false,
                expect_forward_inserts: vec![insi(8, 1, -43), insi(8, 1, -33)],
                expect_backward_inserts: vec![],
                expect_merged_spans: vec![sp(-51, 9), sp(9, 2), sp(14, 1)],
                expect_buckets_a: vec![1, 0, 1, -1, 1, 1, 0, -1, -2, 0, 1, 0],
                // 1 0 1 -1 1 1 0 -1 -2 -2 1 0
                expect_buckets_b: vec![1, 1, 0, 1, -1, 1, 0, -1, -1, 0, 0, 0],
            },
        ),
        (
            "real example 1",
            // I-  6543210987654321
            // A:  0__2_______0__
            // B:  _0130_____00_0
            // A': 00020_____00_0
            // B': 00130_____00_0
            TestCase {
                spans_a: vec![sp(-16, 1), sp(2, 1), sp(7, 1)],
                buckets_a: vec![0, 2, -2],
                spans_b: vec![sp(-15, 4), sp(5, 2), sp(1, 1)],
                buckets_b: vec![0, 1, 2, -3, 0, 0, 0],
                expect_reset: false,
                expect_forward_inserts: vec![insi(1, 2, -15), insi(2, 1, -12), insi(2, 1, -6), insi(3, 1, -3)],
                expect_backward_inserts: vec![insi(0, 1, -16)],
                expect_merged_spans: vec![sp(-16, 5), sp(5, 2), sp(1, 1)],
                expect_buckets_a: vec![0, 0, 0, 2, -2, 0, 0, 0],
                expect_buckets_b: vec![0, 0, 1, 2, -3, 0, 0, 0],
            },
        ),
    ];

    fn make_float_buckets(input: &[i64]) -> Vec<f64> {
        let mut out = vec![0f64; input.len()];
        let mut absolute = 0f64;
        for (i, &v) in input.iter().enumerate() {
            absolute += v as f64;
            out[i] = absolute;
        }
        out
    }

    for (name, tc) in &test_cases {
        // Sanity check.
        assert_eq!(tc.buckets_a.len(), count_spans(&tc.spans_a), "{name}");
        assert_eq!(tc.buckets_b.len(), count_spans(&tc.spans_b), "{name}");
        assert_eq!(tc.expect_buckets_a.len(), count_spans(&tc.expect_merged_spans), "{name}");
        assert_eq!(tc.expect_buckets_b.len(), count_spans(&tc.expect_merged_spans), "{name}");

        // "integers".
        'integers: {
            let r = expand_int_spans_and_buckets(&tc.spans_a, &tc.spans_b, &tc.buckets_a, &tc.buckets_b);
            if tc.expect_reset {
                assert!(r.is_none(), "{name}/integers: expected reset");
                break 'integers;
            }
            let (f_inserts, b_inserts) = r.unwrap_or_else(|| panic!("{name}/integers: unexpected reset"));
            assert_eq!(tc.expect_forward_inserts, f_inserts, "{name}/integers: forward inserts");
            assert_eq!(tc.expect_backward_inserts, b_inserts, "{name}/integers: backward inserts");

            let got_bspans = adjust_for_inserts(&tc.spans_b, &b_inserts);
            assert_eq!(tc.expect_merged_spans, got_bspans, "{name}/integers");

            let mut got_abuckets = vec![0i64; tc.expect_buckets_a.len()];
            insert(&tc.buckets_a, &mut got_abuckets, &f_inserts, true);
            assert_eq!(tc.expect_buckets_a, got_abuckets, "{name}/integers");

            let mut got_bbuckets = vec![0i64; tc.expect_buckets_b.len()];
            insert(&tc.buckets_b, &mut got_bbuckets, &b_inserts, true);
            assert_eq!(tc.expect_buckets_b, got_bbuckets, "{name}/integers");
        }

        // "floats".
        'floats: {
            let mut a_xor_values = vec![XorValue::default(); tc.buckets_a.len()];
            let mut absolute = 0f64;
            for (i, &v) in tc.buckets_a.iter().enumerate() {
                absolute += v as f64;
                a_xor_values[i].value = absolute;
            }

            let b_float_buckets = make_float_buckets(&tc.buckets_b);

            let r = expand_float_spans_and_buckets(&tc.spans_a, &tc.spans_b, &a_xor_values, &b_float_buckets);
            if tc.expect_reset {
                assert!(r.is_none(), "{name}/floats: expected reset");
                break 'floats;
            }
            let (f_inserts, b_inserts) = r.unwrap_or_else(|| panic!("{name}/floats: unexpected reset"));
            assert_eq!(tc.expect_forward_inserts, f_inserts, "{name}/floats: forward inserts");
            assert_eq!(tc.expect_backward_inserts, b_inserts, "{name}/floats: backward inserts");

            let got_bspans = adjust_for_inserts(&tc.spans_b, &b_inserts);
            assert_eq!(tc.expect_merged_spans, got_bspans, "{name}/floats");

            let mut got_abuckets = vec![0f64; tc.expect_buckets_a.len()];
            insert(&make_float_buckets(&tc.buckets_a), &mut got_abuckets, &f_inserts, false);
            assert_eq!(make_float_buckets(&tc.expect_buckets_a), got_abuckets, "{name}/floats");

            let mut got_bbuckets = vec![0f64; tc.expect_buckets_b.len()];
            insert(&make_float_buckets(&tc.buckets_b), &mut got_bbuckets, &b_inserts, false);
            assert_eq!(make_float_buckets(&tc.expect_buckets_b), got_bbuckets, "{name}/floats");
        }
    }
}
