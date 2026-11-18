//! Port of `tsdb/index/postings_test.go`.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use model::labels::{self, Label};
use tsdb_format::SeriesRef;
use tsdb_format::index::{
    BigEndianPostings, BoxPostings, CHECK_CONTEXT_EVERY_N_ITERATIONS, Ctx, Error, LabelHints,
    MemPostings, Postings, PostingsWithIndex, PostingsWithIndexHeap, RemovedPostings,
    all_postings_key, background, empty_postings, err_postings, expand_postings,
    find_intersecting_postings, intersect, is_empty_postings_type, merge, new_list_postings,
    without,
};

/// Go `testutil.MockContextErrAfter`: a context whose `Err()` fails once it has been called `fail_after` times.
struct MockContextErrAfter {
    count: AtomicU64,
    fail_after: u64,
}

impl MockContextErrAfter {
    fn new(fail_after: u64) -> MockContextErrAfter {
        MockContextErrAfter {
            count: AtomicU64::new(0),
            fail_after,
        }
    }
    fn count(&self) -> u64 {
        self.count.load(Ordering::SeqCst)
    }
}

impl Ctx for MockContextErrAfter {
    fn err(&self) -> Option<Error> {
        self.count.fetch_add(1, Ordering::SeqCst);
        if self.count.load(Ordering::SeqCst) >= self.fail_after {
            return Some(Error::canceled());
        }
        None
    }
}

/// A small deterministic random generator (xorshift64*), standing in for Go's math/rand.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed.max(1))
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    /// Go `rand.Int31n(n)`.
    fn int31n(&mut self, n: u32) -> u32 {
        (self.next_u64() >> 33) as u32 % n
    }
}

fn seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1)
}

/// Go test helper `newListPostings`: panics if the list is not sorted.
fn nlp(list: &[SeriesRef]) -> BoxPostings<'static> {
    if !list.is_sorted() {
        panic!("newListPostings: list is not sorted");
    }
    Box::new(new_list_postings(list.to_vec()))
}

/// `Some(list)` is `newListPostings(list...)`, `None` is `EmptyPostings()`.
fn mk(p: &Option<Vec<SeriesRef>>) -> BoxPostings<'static> {
    match p {
        Some(l) => nlp(l),
        None => empty_postings(),
    }
}

fn lbls(ss: &[&str]) -> labels::Labels {
    labels::from_strings(ss)
}

#[test]
fn test_mem_postings_add_for() {
    let p = MemPostings::new();
    let (n, v) = all_postings_key();
    p.test_set_list(n, v, vec![1, 2, 3, 4, 6, 7, 8]);

    p.add_for(5, n, v);

    assert_eq!(Some(vec![1, 2, 3, 4, 5, 6, 7, 8]), p.test_list(n, v));
}

#[test]
fn test_mem_postings_ensure_order() {
    let p = MemPostings::new_unordered();
    let mut rng = Rng::new(seed());

    for i in 0..100 {
        let l: Vec<SeriesRef> = (0..100).map(|_| rng.next_u64()).collect();
        p.test_set_list("a", &i.to_string(), l);
    }

    p.ensure_order(0);

    for l in p.test_all_lists() {
        assert!(l.is_sorted(), "postings list {l:?} is not sorted");
    }
}

#[test]
fn test_intersect() {
    let a = || Some(vec![1, 2, 3]);
    let b = || Some(vec![2, 3, 4]);
    let e = || None;

    let cases: Vec<(Vec<Option<Vec<SeriesRef>>>, Option<Vec<SeriesRef>>)> = vec![
        (vec![], None),
        (vec![a(), b(), e()], None),
        (vec![b(), a(), e()], None),
        (vec![e(), b(), a()], None),
        (vec![e(), a(), b()], None),
        (vec![a(), e(), b()], None),
        (vec![b(), e(), a()], None),
        (vec![b(), e(), a(), a(), b(), a(), a(), a()], None),
        (
            vec![Some(vec![1, 2, 3, 4, 5]), Some(vec![6, 7, 8, 9, 10])],
            Some(vec![]),
        ),
        (
            vec![Some(vec![1, 2, 3, 4, 5]), Some(vec![4, 5, 6, 7, 8])],
            Some(vec![4, 5]),
        ),
        (
            vec![
                Some(vec![1, 2, 3, 4, 9, 10]),
                Some(vec![1, 4, 5, 6, 7, 8, 10, 11]),
            ],
            Some(vec![1, 4, 10]),
        ),
        (vec![Some(vec![1]), Some(vec![0, 1])], Some(vec![1])),
        (vec![Some(vec![1])], Some(vec![1])),
        (vec![Some(vec![1]), Some(vec![])], Some(vec![])),
        (vec![Some(vec![]), Some(vec![])], Some(vec![])),
    ];

    for (input, res) in cases {
        let mut res_p = mk(&res);
        let expected = expand_postings(&mut res_p).unwrap();

        let mut i = intersect(input.iter().map(mk).collect());

        if res.is_none() {
            assert!(is_empty_postings_type(&*i));
            continue;
        }

        assert!(
            !is_empty_postings_type(&*i),
            "intersect unexpected result: EmptyPostings sentinel"
        );

        let got = expand_postings(&mut i).unwrap();
        assert_eq!(expected, got);
    }
}

#[test]
fn test_multi_intersect() {
    let cases: Vec<(Vec<Vec<SeriesRef>>, Vec<SeriesRef>)> = vec![
        (
            vec![
                vec![1, 2, 3, 4, 5, 6, 1000, 1001],
                vec![2, 4, 5, 6, 7, 8, 999, 1001],
                vec![1, 2, 5, 6, 7, 8, 1001, 1200],
            ],
            vec![2, 5, 6, 1001],
        ),
        // One of the reproducible cases for: https://github.com/prometheus/prometheus/issues/2616
        // The initialisation of intersectPostings was moving the iterator forward prematurely making us miss
        // some postings.
        (vec![vec![1, 2], vec![1, 2], vec![1, 2], vec![2]], vec![2]),
    ];

    for (p, res) in cases {
        let ps: Vec<BoxPostings> = p.iter().map(|l| nlp(l)).collect();
        let got = expand_postings(&mut intersect(ps)).unwrap();
        assert_eq!(res, got);
    }
}

#[test]
fn test_multi_merge() {
    let i1 = nlp(&[1, 2, 3, 4, 5, 6, 1000, 1001]);
    let i2 = nlp(&[2, 4, 5, 6, 7, 8, 999, 1001]);
    let i3 = nlp(&[1, 2, 5, 6, 7, 8, 1001, 1200]);

    let res = expand_postings(&mut merge(vec![i1, i2, i3])).unwrap();
    assert_eq!(vec![1, 2, 3, 4, 5, 6, 7, 8, 999, 1000, 1001, 1200], res);
}

#[test]
fn test_merged_postings() {
    let cases: Vec<(Vec<Option<Vec<SeriesRef>>>, Option<Vec<SeriesRef>>)> = vec![
        (vec![], None),
        (vec![Some(vec![]), Some(vec![])], None),
        (vec![Some(vec![])], Some(vec![])),
        (vec![None, None, None, None], None),
        (
            vec![Some(vec![1, 2, 3, 4, 5]), Some(vec![6, 7, 8, 9, 10])],
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        ),
        (
            vec![Some(vec![1, 2, 3, 4, 5]), Some(vec![4, 5, 6, 7, 8])],
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8]),
        ),
        (
            vec![
                Some(vec![1, 2, 3, 4, 9, 10]),
                Some(vec![1, 4, 5, 6, 7, 8, 10, 11]),
            ],
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        ),
        (
            vec![
                Some(vec![1, 2, 3, 4, 9, 10]),
                None,
                Some(vec![1, 4, 5, 6, 7, 8, 10, 11]),
            ],
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        ),
        (vec![Some(vec![1, 2]), Some(vec![])], Some(vec![1, 2])),
        (vec![Some(vec![1, 2]), None], Some(vec![1, 2])),
    ];

    for (input, res) in cases {
        let mut res_p = mk(&res);
        let expected = expand_postings(&mut res_p).unwrap();

        let mut m = merge(input.iter().map(mk).collect());

        if res.is_none() {
            assert!(!m.next());
            continue;
        }

        assert!(
            !is_empty_postings_type(&*m),
            "merge unexpected result: EmptyPostings sentinel"
        );

        let got = expand_postings(&mut m).unwrap();
        assert_eq!(expected, got);
    }
}

struct SeekCase {
    a: Vec<SeriesRef>,
    b: Vec<SeriesRef>,
    seek: SeriesRef,
    success: bool,
    res: Vec<SeriesRef>,
}

fn sc(
    a: &[SeriesRef],
    b: &[SeriesRef],
    seek: SeriesRef,
    success: bool,
    res: &[SeriesRef],
) -> SeekCase {
    SeekCase {
        a: a.to_vec(),
        b: b.to_vec(),
        seek,
        success,
        res: res.to_vec(),
    }
}

#[test]
fn test_merged_postings_seek() {
    let cases = vec![
        sc(
            &[2, 3, 4, 5],
            &[6, 7, 8, 9, 10],
            1,
            true,
            &[2, 3, 4, 5, 6, 7, 8, 9, 10],
        ),
        sc(
            &[1, 2, 3, 4, 5],
            &[6, 7, 8, 9, 10],
            2,
            true,
            &[2, 3, 4, 5, 6, 7, 8, 9, 10],
        ),
        sc(&[1, 2, 3, 4, 5], &[4, 5, 6, 7, 8], 9, false, &[]),
        sc(
            &[1, 2, 3, 4, 9, 10],
            &[1, 4, 5, 6, 7, 8, 10, 11],
            10,
            true,
            &[10, 11],
        ),
    ];

    for c in cases {
        let a = nlp(&c.a);
        let b = nlp(&c.b);

        let mut p = merge(vec![a, b]);

        assert_eq!(c.success, p.seek(c.seek));

        // After seek(), at() should be called.
        if c.success {
            let start = p.at();
            let mut lst = expand_postings(&mut p).unwrap();
            lst.insert(0, start);
            assert_eq!(c.res, lst);
        }
    }
}

#[test]
fn test_removed_postings() {
    let cases: Vec<(Vec<SeriesRef>, Vec<SeriesRef>, Vec<SeriesRef>)> = vec![
        (vec![], vec![], vec![]),
        (vec![1, 2, 3, 4], vec![], vec![1, 2, 3, 4]),
        (vec![], vec![1, 2, 3, 4], vec![]),
        (
            vec![1, 2, 3, 4, 5],
            vec![6, 7, 8, 9, 10],
            vec![1, 2, 3, 4, 5],
        ),
        (vec![1, 2, 3, 4, 5], vec![4, 5, 6, 7, 8], vec![1, 2, 3]),
        (
            vec![1, 2, 3, 4, 9, 10],
            vec![1, 4, 5, 6, 7, 8, 10, 11],
            vec![2, 3, 9],
        ),
        (
            vec![1, 2, 3, 4, 9, 10],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            vec![],
        ),
    ];

    for (a, b, res) in cases {
        let a = nlp(&a);
        let b = nlp(&b);

        let got = expand_postings(&mut RemovedPostings::new(a, b)).unwrap();
        assert_eq!(res, got);
    }
}

#[test]
fn test_removed_next_stackoverflow() {
    let full: Vec<SeriesRef> = (0..10_000_000).collect();
    let remove: Vec<SeriesRef> = (0..10_000_000).collect();

    let flp = nlp(&full);
    let rlp = nlp(&remove);
    let mut rp = RemovedPostings::new(flp, rlp);
    let mut got_elem = false;
    while rp.next() {
        got_elem = true;
    }

    assert!(rp.err().is_none());
    assert!(!got_elem);
}

#[test]
fn test_removed_postings_seek() {
    let cases = vec![
        sc(&[2, 3, 4, 5], &[6, 7, 8, 9, 10], 1, true, &[2, 3, 4, 5]),
        sc(&[1, 2, 3, 4, 5], &[6, 7, 8, 9, 10], 2, true, &[2, 3, 4, 5]),
        sc(&[1, 2, 3, 4, 5], &[4, 5, 6, 7, 8], 9, false, &[]),
        sc(
            &[1, 2, 3, 4, 9, 10],
            &[1, 4, 5, 6, 7, 8, 10, 11],
            10,
            false,
            &[],
        ),
        sc(
            &[1, 2, 3, 4, 9, 10],
            &[1, 4, 5, 6, 7, 8, 11],
            4,
            true,
            &[9, 10],
        ),
        sc(
            &[1, 2, 3, 4, 9, 10],
            &[1, 4, 5, 6, 7, 8, 11],
            5,
            true,
            &[9, 10],
        ),
        sc(
            &[1, 2, 3, 4, 9, 10],
            &[1, 4, 5, 6, 7, 8, 11],
            10,
            true,
            &[10],
        ),
    ];

    for c in cases {
        let a = nlp(&c.a);
        let b = nlp(&c.b);

        let mut p = RemovedPostings::new(a, b);

        assert_eq!(c.success, p.seek(c.seek));

        // After seek(), at() should be called.
        if c.success {
            let start = p.at();
            let mut lst = expand_postings(&mut p).unwrap();
            lst.insert(0, start);
            assert_eq!(c.res, lst);
        }
    }
}

#[test]
fn test_big_endian() {
    let num = 1000;
    let mut rng = Rng::new(seed());
    // Mock a list as postings.
    let mut ls = vec![0u32; num];
    ls[0] = 2;
    for i in 1..num {
        ls[i] = ls[i - 1] + rng.int31n(25) + 2;
    }

    let mut be_lst = vec![0u8; num * 4];
    for i in 0..num {
        be_lst[i * 4..i * 4 + 4].copy_from_slice(&ls[i].to_be_bytes());
    }

    // Iteration.
    {
        let mut bep = BigEndianPostings::new(&be_lst);
        for &l in &ls {
            assert!(bep.next());
            assert_eq!(l as SeriesRef, bep.at());
        }
        assert!(!bep.next());
        assert!(bep.err().is_none());
    }

    // Seek.
    {
        let table: Vec<(u32, u32, bool)> = vec![
            (ls[0] - 1, ls[0], true),
            (ls[4], ls[4], true),
            (ls[500] - 1, ls[500], true),
            (ls[600] + 1, ls[601], true),
            (ls[600] + 1, ls[601], true),
            (ls[600] + 1, ls[601], true),
            (ls[0], ls[601], true),
            (ls[600], ls[601], true),
            (ls[999], ls[999], true),
            (ls[999] + 10, ls[999], false),
        ];

        let mut bep = BigEndianPostings::new(&be_lst);

        for (seek, val, found) in table {
            assert_eq!(found, bep.seek(seek as SeriesRef));
            assert_eq!(val as SeriesRef, bep.at());
            assert!(bep.err().is_none());
        }
    }
}

#[test]
fn test_intersect_with_merge() {
    // One of the reproducible cases for: https://github.com/prometheus/prometheus/issues/2616
    let a = nlp(&[21, 22, 23, 24, 25, 30]);

    let b = merge(vec![nlp(&[10, 20, 30]), nlp(&[15, 26, 30])]);

    let mut p = intersect(vec![a, b]);
    let res = expand_postings(&mut p).unwrap();

    assert_eq!(vec![30], res);
}

#[test]
fn test_without_postings() {
    let cases: Vec<(
        Option<Vec<SeriesRef>>,
        Option<Vec<SeriesRef>>,
        Option<Vec<SeriesRef>>,
    )> = vec![
        (None, None, None),
        (None, Some(vec![1, 2]), None),
        (Some(vec![1, 2]), None, Some(vec![1, 2])),
        (Some(vec![]), Some(vec![]), Some(vec![])),
        (Some(vec![1, 2, 3]), Some(vec![]), Some(vec![1, 2, 3])),
        (
            Some(vec![1, 2, 3]),
            Some(vec![4, 5, 6]),
            Some(vec![1, 2, 3]),
        ),
        (Some(vec![1, 2, 3]), Some(vec![3, 4, 5]), Some(vec![1, 2])),
    ];

    for (base, drop, res) in cases {
        let mut res_p = mk(&res);
        let expected = expand_postings(&mut res_p).unwrap();

        let mut w = without(mk(&base), mk(&drop));

        if res.is_none() {
            assert!(is_empty_postings_type(&*w));
            continue;
        }

        assert!(
            !is_empty_postings_type(&*w),
            "without unexpected result: EmptyPostings sentinel"
        );

        let got = expand_postings(&mut w).unwrap();
        assert_eq!(expected, got);
    }
}

#[test]
fn test_mem_postings_stats() {
    // Create a new MemPostings.
    let p = MemPostings::new();

    // Add some postings to the MemPostings.
    p.add(1, &lbls(&["label", "value1"]));
    p.add(1, &lbls(&["label", "value2"]));
    p.add(1, &lbls(&["label", "value3"]));
    p.add(2, &lbls(&["label", "value1"]));

    // Call the stats method to calculate the cardinality statistics, passing a fake calculation so we get the
    // same result regardless of the labels implementation.
    let stats = p.stats("label", 10, |name, value, n| {
        (name.len() + value.len()) as u64 * n
    });

    // Assert that the expected statistics were calculated.
    assert_eq!(2, stats.cardinality_metrics_stats[0].count);
    assert_eq!("value1", stats.cardinality_metrics_stats[0].name);

    assert_eq!(3, stats.cardinality_label_stats[0].count);
    assert_eq!("label", stats.cardinality_label_stats[0].name);

    assert_eq!(44, stats.label_value_stats[0].count);
    assert_eq!("label", stats.label_value_stats[0].name);

    assert_eq!(2, stats.label_value_pairs_stats[0].count);
    assert_eq!("label=value1", stats.label_value_pairs_stats[0].name);

    assert_eq!(3, stats.num_label_pairs);
}

#[test]
fn test_mem_postings_delete() {
    let p = MemPostings::new();
    p.add(1, &lbls(&["lbl1", "a"]));
    p.add(2, &lbls(&["lbl1", "b"]));
    p.add(3, &lbls(&["lbl2", "a"]));

    let (an, av) = all_postings_key();
    let mut before = p.postings(background(), an, &[av]);
    let deleted_refs: HashSet<SeriesRef> = [2].into_iter().collect();
    let affected_labels: HashSet<Label> = [Label::new("lbl1", "b")].into_iter().collect();
    p.delete(&deleted_refs, &affected_labels);
    let mut after = p.postings(background(), an, &[av]);

    // Make sure postings gotten before the delete have the old data when iterated over.
    let expanded = expand_postings(&mut before).unwrap();
    assert_eq!(vec![1, 2, 3], expanded);

    // Make sure postings gotten after the delete have the new data when iterated over.
    let expanded = expand_postings(&mut after).unwrap();
    assert_eq!(vec![1, 3], expanded);

    let mut deleted = p.postings(background(), "lbl1", &["b"]);
    let expanded = expand_postings(&mut deleted).unwrap();
    assert!(
        expanded.is_empty(),
        "expected empty postings, got {expanded:?}"
    );
}

/// Go `postingsFailingAfterNthCall`.
struct PostingsFailingAfterNthCall {
    ttl: i32,
    p: BoxPostings<'static>,
}

impl Postings for PostingsFailingAfterNthCall {
    fn seek(&mut self, v: SeriesRef) -> bool {
        self.ttl -= 1;
        if self.ttl <= 0 {
            return false;
        }
        self.p.seek(v)
    }
    fn next(&mut self) -> bool {
        self.ttl -= 1;
        if self.ttl <= 0 {
            return false;
        }
        self.p.next()
    }
    fn at(&self) -> SeriesRef {
        self.p.at()
    }
    fn err(&self) -> Option<Error> {
        if self.ttl <= 0 {
            return Some(Error::new("ttl exceeded"));
        }
        self.p.err()
    }
}

fn lp(l: &[SeriesRef]) -> BoxPostings<'static> {
    Box::new(new_list_postings(l.to_vec()))
}

#[test]
fn test_find_intersecting_postings() {
    // Multiple intersections.
    {
        let mut p = lp(&[10, 15, 20, 25, 30, 35, 40, 45, 50]);
        let candidates = vec![
            lp(&[7, 13, 14, 27]), // Does not intersect.
            lp(&[10, 20]),        // Does intersect.
            lp(&[29, 30, 31]),    // Does intersect.
            lp(&[29, 30, 31]),    // Does intersect (same again).
            lp(&[60]),            // Does not intersect.
            lp(&[45]),            // Does intersect.
            empty_postings(),     // Does not intersect.
        ];

        let mut indexes = find_intersecting_postings(&mut *p, candidates).unwrap();
        indexes.sort_unstable();
        assert_eq!(vec![1, 2, 3, 5], indexes);
    }

    // No intersections.
    {
        let mut p = lp(&[10, 15, 20, 25, 30, 35, 40, 45, 50]);
        let candidates = vec![
            lp(&[7, 13, 14, 27]), // Does not intersect.
            lp(&[60]),            // Does not intersect.
            empty_postings(),     // Does not intersect.
        ];

        let indexes = find_intersecting_postings(&mut *p, candidates).unwrap();
        assert!(indexes.is_empty());
    }

    // p is ErrPostings.
    {
        let mut p = err_postings(Error::canceled());
        let candidates = vec![lp(&[1])];
        assert!(find_intersecting_postings(&mut *p, candidates).is_err());
    }

    // One of the candidates is ErrPostings.
    {
        let mut p = lp(&[1]);
        let candidates = vec![lp(&[1]), err_postings(Error::canceled())];
        assert!(find_intersecting_postings(&mut *p, candidates).is_err());
    }

    // One of the candidates fails on nth call.
    {
        let mut p = lp(&[10, 20, 30, 40, 50, 60, 70]);
        let candidates: Vec<BoxPostings> = vec![
            lp(&[7, 13, 14, 27]),
            Box::new(PostingsFailingAfterNthCall {
                ttl: 2,
                p: lp(&[29, 30, 31, 40]),
            }),
        ];
        assert!(find_intersecting_postings(&mut *p, candidates).is_err());
    }

    // p fails on the nth call.
    {
        let mut p = PostingsFailingAfterNthCall {
            ttl: 2,
            p: lp(&[10, 20, 30, 40, 50, 60, 70]),
        };
        let candidates = vec![lp(&[7, 13, 14, 27]), lp(&[29, 30, 31, 40])];
        assert!(find_intersecting_postings(&mut p, candidates).is_err());
    }
}

fn heap3() -> PostingsWithIndexHeap<'static> {
    let mut h = PostingsWithIndexHeap(vec![
        PostingsWithIndex {
            index: 0,
            p: lp(&[10, 20, 30]),
            popped: false,
        },
        PostingsWithIndex {
            index: 1,
            p: lp(&[1, 5]),
            popped: false,
        },
        PostingsWithIndex {
            index: 2,
            p: lp(&[25, 50]),
            popped: false,
        },
    ]);
    for node in h.0.iter_mut() {
        node.p.next();
    }
    h.init();
    h
}

#[test]
fn test_postings_with_index_heap() {
    // seekHead.
    {
        let mut h = heap3();
        for expected in [1, 5, 10, 20, 25, 30, 50] {
            assert_eq!(expected, h.at());
            h.seek_head(h.at() + 1).unwrap();
        }
        assert!(h.empty());
    }

    // pop.
    {
        let mut h = heap3();
        for expected in [1, 5, 10, 20] {
            assert_eq!(expected, h.at());
            h.seek_head(h.at() + 1).unwrap();
        }
        assert_eq!(25, h.at());
        let node = h.pop();
        assert_eq!(2, node.index);
        assert_eq!(25, node.p.at());
    }
}

#[test]
fn test_list_postings() {
    // Empty list.
    {
        let mut p = new_list_postings(vec![]);
        assert_eq!(0, p.len());
        assert!(!p.next());
        assert!(!p.seek(10));
        assert!(!p.next());
        assert!(p.err().is_none());
        assert_eq!(0, p.len());
    }

    // One posting.
    {
        // next
        let mut p = new_list_postings(vec![10]);
        assert_eq!(1, p.len());
        assert!(p.next());
        assert_eq!(10, p.at());
        assert!(!p.next());
        assert!(p.err().is_none());
        assert_eq!(0, p.len());

        // seek less
        let mut p = new_list_postings(vec![10]);
        assert_eq!(1, p.len());
        assert!(p.seek(5));
        assert_eq!(10, p.at());
        assert!(p.seek(5));
        assert_eq!(10, p.at());
        assert!(!p.next());
        assert!(p.err().is_none());
        assert_eq!(0, p.len());

        // seek equal
        let mut p = new_list_postings(vec![10]);
        assert_eq!(1, p.len());
        assert!(p.seek(10));
        assert_eq!(10, p.at());
        assert!(!p.next());
        assert!(p.err().is_none());
        assert_eq!(0, p.len());

        // seek more
        let mut p = new_list_postings(vec![10]);
        assert_eq!(1, p.len());
        assert!(!p.seek(15));
        assert!(!p.next());
        assert!(p.err().is_none());
        assert_eq!(0, p.len());

        // seek after next
        let mut p = new_list_postings(vec![10]);
        assert_eq!(1, p.len());
        assert!(p.next());
        assert!(!p.seek(15));
        assert!(!p.next());
        assert!(p.err().is_none());
        assert_eq!(0, p.len());
    }

    // Multiple postings.
    {
        // next
        let mut p = new_list_postings(vec![10, 20]);
        assert_eq!(2, p.len());
        assert!(p.next());
        assert_eq!(10, p.at());
        assert!(p.next());
        assert_eq!(20, p.at());
        assert!(!p.next());
        assert!(p.err().is_none());
        assert_eq!(0, p.len());

        // seek
        let mut p = new_list_postings(vec![10, 20]);
        assert_eq!(2, p.len());
        assert!(p.seek(5));
        assert_eq!(10, p.at());
        assert!(p.seek(5));
        assert_eq!(10, p.at());
        assert!(p.seek(10));
        assert_eq!(10, p.at());
        assert!(p.next());
        assert_eq!(20, p.at());
        assert!(p.seek(10));
        assert_eq!(20, p.at());
        assert!(p.seek(20));
        assert_eq!(20, p.at());
        assert!(!p.next());
        assert!(p.err().is_none());
        assert_eq!(0, p.len());

        // seek lest than last
        let mut p = new_list_postings(vec![10, 20, 30, 40, 50]);
        assert_eq!(5, p.len());
        assert!(p.seek(45));
        assert_eq!(50, p.at());
        assert!(!p.next());
        assert_eq!(0, p.len());

        // seek exactly last
        let mut p = new_list_postings(vec![10, 20, 30, 40, 50]);
        assert_eq!(5, p.len());
        assert!(p.seek(50));
        assert_eq!(50, p.at());
        assert!(!p.next());
        assert_eq!(0, p.len());

        // seek more than last
        let mut p = new_list_postings(vec![10, 20, 30, 40, 50]);
        assert_eq!(5, p.len());
        assert!(!p.seek(60));
        assert!(!p.next());
        assert_eq!(0, p.len());
    }

    // Seek.
    for c in [2u64, 8, 9, 10] {
        let list: Vec<SeriesRef> = (0..c).map(|i| i * 10).collect();

        // All one by one.
        let mut p = new_list_postings(list.clone());
        for i in 0..c {
            assert!(p.seek(i * 10), "count={c}");
            assert_eq!(i * 10, p.at(), "count={c}");
        }
        assert!(!p.seek(c * 10), "count={c}");

        // Each one.
        for &r in &list {
            let mut p = new_list_postings(list.clone());
            assert!(p.seek(r), "count={c}");
            assert_eq!(r, p.at(), "count={c}");
        }
    }
}

#[test]
fn test_mem_postings_postings_for_label_matching() {
    let mp = MemPostings::new();
    mp.add(1, &lbls(&["foo", "1"]));
    mp.add(2, &lbls(&["foo", "2"]));
    mp.add(3, &lbls(&["foo", "3"]));
    mp.add(4, &lbls(&["foo", "4"]));

    let is_even = |v: &str| -> bool {
        let iv: i64 = v.parse().unwrap();
        iv % 2 == 0
    };

    let mut p = mp.postings_for_label_matching(background(), "foo", &is_even);
    assert!(p.err().is_none());
    let refs = expand_postings(&mut p).unwrap();
    assert_eq!(vec![2, 4], refs);
}

#[test]
fn test_mem_postings_postings_for_all_label_values() {
    let mp = MemPostings::new();
    mp.add(1, &lbls(&["foo", "1"]));
    mp.add(2, &lbls(&["foo", "2"]));
    mp.add(3, &lbls(&["foo", "3"]));
    mp.add(4, &lbls(&["foo", "4"]));

    let mut p = mp.postings_for_all_label_values(background(), "foo");
    assert!(p.err().is_none());
    let refs = expand_postings(&mut p).unwrap();
    // All postings for the label should be returned.
    assert_eq!(vec![1, 2, 3, 4], refs);
}

#[test]
fn test_mem_postings_postings_for_label_matching_honors_context_cancel() {
    let mem_p = MemPostings::new();
    let series_count = 10 * CHECK_CONTEXT_EVERY_N_ITERATIONS;
    for i in 1..=series_count {
        mem_p.add(i as SeriesRef, &lbls(&["__name__", &format!("{i:4}")]));
    }

    let fail_after = (series_count / 2 / CHECK_CONTEXT_EVERY_N_ITERATIONS) as u64;
    let ctx = MockContextErrAfter::new(fail_after);
    let p = mem_p.postings_for_label_matching(&ctx, "__name__", &|_| true);
    assert!(p.err().is_some());
    assert_eq!(fail_after + 1, ctx.count()); // Plus one for the err() call that puts the error in the result.
}

#[test]
fn test_mem_postings_label_values_limit_smallest() {
    let ctx = background();

    // Added in descending order.
    let p = MemPostings::new();
    let all = ["a", "b", "c", "d", "e"];
    for (i, v) in all.iter().enumerate().rev() {
        p.add(i as SeriesRef + 1, &lbls(&["lbl", v]));
    }

    let shared = p.test_lvs("lbl").unwrap();
    assert_eq!(
        vec!["e", "d", "c", "b", "a"],
        shared,
        "precondition: the shared value list is held in insertion order"
    );

    let h = |limit: usize, limit_smallest: bool| {
        Some(LabelHints {
            limit,
            limit_smallest,
        })
    };
    let cases: Vec<(&str, &str, Option<LabelHints>, Option<Vec<&str>>)> = vec![
        ("nil hints", "lbl", None, None),
        ("no limit", "lbl", h(0, true), None),
        ("limit below count", "lbl", h(2, true), Some(vec!["a", "b"])),
        ("limit of one", "lbl", h(1, true), Some(vec!["a"])),
        ("limit at count", "lbl", h(5, true), None),
        ("limit above count", "lbl", h(10, true), None),
        (
            "limit without flag",
            "lbl",
            h(2, false),
            Some(vec!["e", "d"]),
        ),
        ("unknown label name", "missing", h(2, true), None),
    ];
    for (name, label, hints, want) in cases {
        let mut got = p.label_values(ctx, label, hints.as_ref());
        if label == "missing" {
            // adapted: Go's nil slice is an empty Vec.
            assert!(got.is_empty(), "{name}");
        } else if let Some(want) = want {
            assert_eq!(want, got, "{name}");
        } else {
            // The whole set, in whatever order it is held.
            let mut sorted = got.clone();
            sorted.sort();
            assert_eq!(all.to_vec(), sorted, "{name}");
        }

        assert_eq!(
            Some(shared.clone()),
            p.test_lvs("lbl"),
            "{name}: the shared value list must not be mutated"
        );
        if !got.is_empty() {
            // A write through the result must not reach the shared array.
            got[0] = "zzz".to_string();
            assert_eq!(
                Some(shared.clone()),
                p.test_lvs("lbl"),
                "{name}: the result must not alias the shared value list"
            );
        }
    }

    // Delete rebuilds the value list from a map, so the order afterwards is arbitrary.
    p.delete(
        &[3].into_iter().collect(),
        &[Label::new("lbl", "c")].into_iter().collect(),
    );
    assert_eq!(
        vec!["a", "b", "d"],
        p.label_values(
            ctx,
            "lbl",
            Some(&LabelHints {
                limit: 3,
                limit_smallest: true
            })
        )
    );
}
