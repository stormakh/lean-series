//! Port of `tsdb/querier_test.go`. Tests that need the head run their body with `--cfg head` and are ignored
//! until the head is merged.

mod common;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use chunkenc::{AnyIterator, Chunk, ChunkIterator, ValueType};
use common::*;
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{self, Labels};
use model::matcher::{MatchType, Matcher};
use storage::tsdbutil as tu;
use storage::{
    BoxChunkSeriesSet, BoxSeriesSet, ChunkQuerier, Context, Error, LabelQuerier, Meta, MetaIterator, Querier, Sample,
    SelectHints,
};
use tsdb::querier::{
    BlockBaseQuerier, BlockChunkQuerier, BlockQuerier, DeletedIterator, PopulateWithDelChunkSeriesIterator,
    PopulateWithDelSeriesIterator,
};
use tsdb::{ChunkOrIterable, ChunkReader, IndexReader, Iterable, Ulid};
use tsdb_format::index;
use tsdb_format::tombstones::{self, Interval, Intervals, IntervalsExt, MemTombstones};

// ---------------------------------------------------------------------------------------------------------------
// Helpers.

fn m(t: MatchType, n: &str, v: &str) -> Matcher {
    Matcher::must_new(t, n, v)
}

fn sf(st: i64, t: i64, f: f64) -> Sample {
    Sample::float(st, t, f)
}

fn sh(st: i64, t: i64, h: Histogram) -> Sample {
    Sample::histogram(st, t, h)
}

fn sfh(st: i64, t: i64, fh: FloatHistogram) -> Sample {
    Sample::float_histogram(st, t, fh)
}

fn iv(mint: i64, maxt: i64) -> Interval {
    Interval::new(mint, maxt)
}

fn ivs_add(mut ivs: Intervals, n: Interval) -> Intervals {
    ivs.add(n);
    ivs
}

/// Go `assureChunkFromSamples`.
fn chk(s: Vec<Sample>) -> Meta {
    storage::chunks::chunk_from_samples(&s).unwrap()
}

fn ls(s: &[&str]) -> Labels {
    labels::from_strings(s)
}

/// A comparable view of a chunk meta (Go compares metas with `require.Equal`).
fn meta_view(c: &Meta) -> (u64, i64, i64, chunkenc::Encoding, Vec<u8>) {
    let ch = c.chunk();
    (c.chunk_ref, c.min_time, c.max_time, ch.encoding(), ch.bytes().to_vec())
}

/// Go `rmChunkRefs`.
fn rm_chunk_refs(chks: &mut [Meta]) {
    for c in chks {
        c.chunk_ref = 0;
    }
}

/// A list series set (Go `newMockSeriesSet`): expected series as labels and samples.
type ExpSeries = Vec<(Labels, Vec<Sample>)>;
/// Expected chunk series as labels and the samples of each chunk (Go `NewListChunkSeriesFromSamples`).
type ExpChunkSeries = Vec<(Labels, Vec<Vec<Sample>>)>;

struct SeriesSamples {
    lset: BTreeMap<String, String>,
    chunks: Vec<Vec<Sample>>,
}

fn lset(kv: &[(&str, &str)]) -> BTreeMap<String, String> {
    kv.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

/// Go `createIdxChkReaders`: index: labels -> postings -> chunk metas -> chunk ref; chunk reader: ref -> values.
fn create_idx_chk_readers(tc: &[SeriesSamples]) -> (Arc<MockIndex>, Arc<MockChunkReader>, i64, i64) {
    let mut chk_reader = MockChunkReader::default();
    let mut mi = new_mock_index();
    let mut postings: BTreeMap<(String, String), Vec<u64>> = BTreeMap::new();
    let (mut block_mint, mut block_maxt) = (i64::MAX, i64::MIN);
    let mut chunk_ref = 0u64;
    for (i, s) in tc.iter().enumerate() {
        let i = i as u64 + 1; // 0 is not a valid posting.
        let mut metas = Vec::with_capacity(s.chunks.len());
        for chk in &s.chunks {
            block_mint = block_mint.min(chk[0].t);
            block_maxt = block_maxt.max(chk[chk.len() - 1].t);
            metas.push(Meta { min_time: chk[0].t, max_time: chk[chk.len() - 1].t, chunk_ref, chunk: None });
            let chunk: Box<dyn Chunk> = if chk[0].fh.is_some() {
                let mut c = chunkenc::new_empty_chunk(chunkenc::ENC_FLOAT_HISTOGRAM).unwrap();
                let mut app = c.appender().unwrap();
                for smpl in chk {
                    let mut fh = smpl.fh.as_deref().expect("chunk can only contain one type of sample").clone();
                    app.append_float_histogram(&mut *c, None, 0, smpl.t, &mut fh, true).expect("chunk should be appendable");
                }
                c
            } else if chk[0].h.is_some() {
                let mut c = chunkenc::new_empty_chunk(chunkenc::ENC_HISTOGRAM).unwrap();
                let mut app = c.appender().unwrap();
                for smpl in chk {
                    let mut h = smpl.h.as_deref().expect("chunk can only contain one type of sample").clone();
                    app.append_histogram(&mut *c, None, 0, smpl.t, &mut h, true).expect("chunk should be appendable");
                }
                c
            } else {
                let mut c = chunkenc::new_empty_chunk(chunkenc::ENC_XOR).unwrap();
                let mut app = c.appender().unwrap();
                for smpl in chk {
                    assert!(smpl.h.is_none() && smpl.fh.is_none(), "chunk can only contain one type of sample");
                    app.append(&mut *c, 0, smpl.t, smpl.f);
                }
                c
            };
            chk_reader.0.insert(chunk_ref, Arc::from(chunk));
            chunk_ref += 1;
        }
        let l = labels::from_map(s.lset.clone());
        mi.add_series(i, l.clone(), &metas).unwrap();
        l.range(|n, v| postings.entry((n.to_string(), v.to_string())).or_default().push(i));
        postings.entry((String::new(), String::new())).or_default().push(i);
    }
    for ((n, v), refs) in postings {
        mi.write_postings(&n, &v, &mut index::new_list_postings(refs)).unwrap();
    }
    (Arc::new(mi), Arc::new(chk_reader), block_mint, block_maxt)
}

struct BlockQuerierTestCase {
    mint: i64,
    maxt: i64,
    ms: Vec<Matcher>,
    hints: Option<SelectHints>,
    exp: ExpSeries,
    exp_chks: ExpChunkSeries,
}

fn bq(mint: i64, maxt: i64, ms: Vec<Matcher>, exp: ExpSeries, exp_chks: ExpChunkSeries) -> BlockQuerierTestCase {
    BlockQuerierTestCase { mint, maxt, ms, hints: None, exp, exp_chks }
}

/// Go `testBlockQuerier`.
fn test_block_querier_case(
    c: &BlockQuerierTestCase,
    ir: Arc<dyn IndexReader>,
    cr: Arc<dyn ChunkReader>,
    stones: Arc<dyn tombstones::Reader>,
) {
    let ctx = Context::background();
    // Sample.
    {
        let mut q = BlockQuerier(BlockBaseQuerier::from_parts(
            Ulid::default(),
            ir.clone(),
            cr.clone(),
            stones.clone(),
            c.mint,
            c.maxt,
        ));
        {
            let mut res = q.select(&ctx, false, c.hints.as_ref(), &c.ms);
            let mut exp = c.exp.iter();
            loop {
                let e = exp.next();
                let rok = res.next();
                assert_eq!(e.is_some(), rok);
                let Some((el, es)) = e else {
                    assert!(res.warnings().is_empty());
                    break;
                };
                let sres = res.at().clone();
                assert_eq!(el, sres.labels());
                let mut it = sres.iterator(None);
                let smpl_res = storage::expand_samples(&mut it);
                assert_eq!(Ok(es.clone()), smpl_res);
            }
            assert!(res.err().is_none(), "{:?}", res.err());
        }
        assert!(q.close().is_none());
    }
    // Chunk.
    {
        let mut q =
            BlockChunkQuerier(BlockBaseQuerier::from_parts(Ulid::default(), ir, cr, stones, c.mint, c.maxt));
        {
            let mut res = q.select(&ctx, false, c.hints.as_ref(), &c.ms);
            let mut exp = c.exp_chks.iter();
            loop {
                let e = exp.next();
                let rok = res.next();
                assert_eq!(e.is_some(), rok);
                let Some((el, echks)) = e else {
                    assert!(res.warnings().is_empty());
                    break;
                };
                let sres = res.at().clone();
                assert_eq!(el, sres.labels());
                let mut it = sres.iterator(None);
                let chks_res = storage::expand_chunks(&mut *it).unwrap();
                assert_eq!(chks_res.len(), echks.len());
                let mut act = Vec::new();
                for c in &chks_res {
                    let mut it = c.chunk().iterator(None);
                    act.push(storage::expand_samples(&mut it).unwrap());
                }
                assert_eq!(echks, &act);
            }
            assert!(res.err().is_none(), "{:?}", res.err());
        }
        assert!(q.close().is_none());
    }
}

fn test_data() -> Vec<SeriesSamples> {
    vec![
        SeriesSamples {
            lset: lset(&[("a", "a")]),
            chunks: vec![
                vec![sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 4.0)],
                vec![sf(0, 5, 2.0), sf(0, 6, 3.0), sf(0, 7, 4.0)],
            ],
        },
        SeriesSamples {
            lset: lset(&[("a", "a"), ("b", "b")]),
            chunks: vec![vec![sf(0, 1, 1.0), sf(0, 2, 2.0), sf(0, 3, 3.0)], vec![sf(0, 5, 3.0), sf(0, 6, 6.0)]],
        },
        SeriesSamples {
            lset: lset(&[("b", "b")]),
            chunks: vec![
                vec![sf(0, 1, 3.0), sf(0, 2, 2.0), sf(0, 3, 6.0)],
                vec![sf(0, 5, 1.0), sf(0, 6, 7.0), sf(0, 7, 2.0)],
            ],
        },
    ]
}

/// Samples as `(t, f)` float samples with start time 0.
fn fl(v: &[(i64, f64)]) -> Vec<Sample> {
    v.iter().map(|&(t, f)| sf(0, t, f)).collect()
}

#[test]
fn test_block_querier() {
    let cases = vec![
        bq(0, 0, vec![], vec![], vec![]),
        bq(0, 0, vec![m(MatchType::Equal, "a", "a")], vec![], vec![]),
        bq(1, 0, vec![m(MatchType::Equal, "a", "a")], vec![], vec![]),
        bq(i64::MIN, i64::MAX, vec![m(MatchType::Equal, "a", "x")], vec![], vec![]),
        bq(
            i64::MIN,
            i64::MAX,
            vec![m(MatchType::Regexp, "a", ".*")],
            vec![
                (ls(&["a", "a"]), fl(&[(1, 2.), (2, 3.), (3, 4.), (5, 2.), (6, 3.), (7, 4.)])),
                (ls(&["a", "a", "b", "b"]), fl(&[(1, 1.), (2, 2.), (3, 3.), (5, 3.), (6, 6.)])),
                (ls(&["b", "b"]), fl(&[(1, 3.), (2, 2.), (3, 6.), (5, 1.), (6, 7.), (7, 2.)])),
            ],
            vec![
                (ls(&["a", "a"]), vec![fl(&[(1, 2.), (2, 3.), (3, 4.)]), fl(&[(5, 2.), (6, 3.), (7, 4.)])]),
                (ls(&["a", "a", "b", "b"]), vec![fl(&[(1, 1.), (2, 2.), (3, 3.)]), fl(&[(5, 3.), (6, 6.)])]),
                (ls(&["b", "b"]), vec![fl(&[(1, 3.), (2, 2.), (3, 6.)]), fl(&[(5, 1.), (6, 7.), (7, 2.)])]),
            ],
        ),
        bq(
            2,
            6,
            vec![m(MatchType::Equal, "a", "a")],
            vec![
                (ls(&["a", "a"]), fl(&[(2, 3.), (3, 4.), (5, 2.), (6, 3.)])),
                (ls(&["a", "a", "b", "b"]), fl(&[(2, 2.), (3, 3.), (5, 3.), (6, 6.)])),
            ],
            vec![
                (ls(&["a", "a"]), vec![fl(&[(2, 3.), (3, 4.)]), fl(&[(5, 2.), (6, 3.)])]),
                (ls(&["a", "a", "b", "b"]), vec![fl(&[(2, 2.), (3, 3.)]), fl(&[(5, 3.), (6, 6.)])]),
            ],
        ),
        // This test runs a query disabling trimming. All chunks containing at least 1 sample within the queried
        // time range will be returned.
        BlockQuerierTestCase {
            hints: Some(SelectHints { start: 2, end: 6, disable_trimming: true, ..Default::default() }),
            ..bq(
                2,
                6,
                vec![m(MatchType::Equal, "a", "a")],
                vec![
                    (ls(&["a", "a"]), fl(&[(1, 2.), (2, 3.), (3, 4.), (5, 2.), (6, 3.), (7, 4.)])),
                    (ls(&["a", "a", "b", "b"]), fl(&[(1, 1.), (2, 2.), (3, 3.), (5, 3.), (6, 6.)])),
                ],
                vec![
                    (ls(&["a", "a"]), vec![fl(&[(1, 2.), (2, 3.), (3, 4.)]), fl(&[(5, 2.), (6, 3.), (7, 4.)])]),
                    (ls(&["a", "a", "b", "b"]), vec![fl(&[(1, 1.), (2, 2.), (3, 3.)]), fl(&[(5, 3.), (6, 6.)])]),
                ],
            )
        },
        // This test runs a query disabling trimming. All chunks containing at least 1 sample within the queried
        // time range will be returned.
        BlockQuerierTestCase {
            hints: Some(SelectHints { start: 5, end: 6, disable_trimming: true, ..Default::default() }),
            ..bq(
                5,
                6,
                vec![m(MatchType::Equal, "a", "a")],
                vec![
                    (ls(&["a", "a"]), fl(&[(5, 2.), (6, 3.), (7, 4.)])),
                    (ls(&["a", "a", "b", "b"]), fl(&[(5, 3.), (6, 6.)])),
                ],
                vec![
                    (ls(&["a", "a"]), vec![fl(&[(5, 2.), (6, 3.), (7, 4.)])]),
                    (ls(&["a", "a", "b", "b"]), vec![fl(&[(5, 3.), (6, 6.)])]),
                ],
            )
        },
    ];
    for c in &cases {
        let (ir, cr, _, _) = create_idx_chk_readers(&test_data());
        test_block_querier_case(c, ir, cr, Arc::new(MemTombstones::new()));
    }
}

#[test]
fn test_block_base_querier_label_names_limit() {
    let mut ix = new_mock_index();
    let l = ls(&["aaa", "1", "bbb", "2", "ccc", "3"]);
    ix.add_series(1, l.clone(), &[]).unwrap();
    l.range(|n, v| ix.write_postings(n, v, &mut index::new_list_postings(vec![1])).unwrap());
    let q = BlockBaseQuerier::from_parts(
        Ulid::default(),
        Arc::new(ix),
        Arc::new(MockChunkReader::default()),
        Arc::new(MemTombstones::new()),
        0,
        0,
    );
    let ctx = Context::background();
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    let hint = |l| storage::LabelHints { limit: l, limit_smallest: false };
    // No limit.
    let (names, _, err) = q.label_names(&ctx, None, &[]);
    assert!(err.is_none());
    assert_eq!(s(&["aaa", "bbb", "ccc"]), names);
    // Limit applied.
    let (names, _, err) = q.label_names(&ctx, Some(&hint(2)), &[]);
    assert!(err.is_none());
    assert_eq!(s(&["aaa", "bbb"]), names);
    // Limit larger than result.
    let (names, _, err) = q.label_names(&ctx, Some(&hint(10)), &[]);
    assert!(err.is_none());
    assert_eq!(s(&["aaa", "bbb", "ccc"]), names);
    // Zero limit means no limit.
    let (names, _, err) = q.label_names(&ctx, Some(&hint(0)), &[]);
    assert!(err.is_none());
    assert_eq!(s(&["aaa", "bbb", "ccc"]), names);
}

#[test]
#[ignore = "needs head"]
fn test_block_querier_against_head_with_open_chunks() {
    #[cfg(head)]
    {
        use tsdb::head::{Head, HeadOptions, HeadStats, RangeHead};
        use tsdb::BlockReader;
        let cases = vec![
            bq(0, 0, vec![], vec![], vec![]),
            bq(0, 0, vec![m(MatchType::Equal, "a", "a")], vec![], vec![]),
            bq(1, 0, vec![m(MatchType::Equal, "a", "a")], vec![], vec![]),
            bq(i64::MIN, i64::MAX, vec![m(MatchType::Equal, "a", "x")], vec![], vec![]),
            bq(
                i64::MIN,
                i64::MAX,
                vec![m(MatchType::Regexp, "a", ".*")],
                vec![
                    (ls(&["a", "a"]), fl(&[(1, 2.), (2, 3.), (3, 4.), (5, 2.), (6, 3.), (7, 4.)])),
                    (ls(&["a", "a", "b", "b"]), fl(&[(1, 1.), (2, 2.), (3, 3.), (5, 3.), (6, 6.)])),
                    (ls(&["b", "b"]), fl(&[(1, 3.), (2, 2.), (3, 6.), (5, 1.), (6, 7.), (7, 2.)])),
                ],
                vec![
                    (ls(&["a", "a"]), vec![fl(&[(1, 2.), (2, 3.), (3, 4.), (5, 2.), (6, 3.), (7, 4.)])]),
                    (ls(&["a", "a", "b", "b"]), vec![fl(&[(1, 1.), (2, 2.), (3, 3.), (5, 3.), (6, 6.)])]),
                    (ls(&["b", "b"]), vec![fl(&[(1, 3.), (2, 2.), (3, 6.), (5, 1.), (6, 7.), (7, 2.)])]),
                ],
            ),
            bq(
                2,
                6,
                vec![m(MatchType::Equal, "a", "a")],
                vec![
                    (ls(&["a", "a"]), fl(&[(2, 3.), (3, 4.), (5, 2.), (6, 3.)])),
                    (ls(&["a", "a", "b", "b"]), fl(&[(2, 2.), (3, 3.), (5, 3.), (6, 6.)])),
                ],
                vec![
                    (ls(&["a", "a"]), vec![fl(&[(2, 3.), (3, 4.), (5, 2.), (6, 3.)])]),
                    (ls(&["a", "a", "b", "b"]), vec![fl(&[(2, 2.), (3, 3.), (5, 3.), (6, 6.)])]),
                ],
            ),
        ];
        for c in &cases {
            let opts = HeadOptions { chunk_range: 2 * 60 * 60 * 1000, ..HeadOptions::default() };
            let h = Head::new(None, None, opts, Arc::new(HeadStats::default())).unwrap();
            let mut app = h.appender(&Context::background());
            for s in test_data() {
                for chk in &s.chunks {
                    for sample in chk {
                        app.append(0, &labels::from_map(s.lset.clone()), sample.t, sample.f).unwrap();
                    }
                }
            }
            assert!(app.commit().is_none());
            let hr = RangeHead::new(h.clone(), c.mint, c.maxt);
            let ir: Arc<dyn IndexReader> = Arc::from(hr.index().unwrap());
            let cr: Arc<dyn ChunkReader> = Arc::from(hr.chunks().unwrap());
            test_block_querier_case(c, ir.clone(), cr.clone(), Arc::new(MemTombstones::new()));
            ir.close().unwrap();
            cr.close().unwrap();
            h.close().unwrap();
        }
    }
}

#[test]
fn test_block_querier_trimming_does_not_modify_original_tombstone_intervals() {
    let ctx = Context::background();
    let c = bq(
        2,
        6,
        vec![m(MatchType::Regexp, "a", "a")],
        vec![
            (ls(&["a", "a"]), fl(&[(3, 4.), (5, 2.), (6, 3.)])),
            (ls(&["a", "a", "b", "b"]), fl(&[(3, 3.), (5, 3.), (6, 6.)])),
        ],
        vec![
            (ls(&["a", "a"]), vec![fl(&[(3, 4.)]), fl(&[(5, 2.), (6, 3.)])]),
            (ls(&["a", "a", "b", "b"]), vec![fl(&[(3, 3.)]), fl(&[(5, 3.), (6, 6.)])]),
        ],
    );
    let (ir, cr, _, _) = create_idx_chk_readers(&test_data());
    let stones = Arc::new(MemTombstones::new());
    let mut p = ir.postings(&ctx, "a", &["a"]).unwrap();
    let refs = index::expand_postings(&mut *p).unwrap();
    drop(p);
    for &r in &refs {
        stones.add_interval(r, &[iv(1, 2)]);
    }
    test_block_querier_case(&c, ir, cr, stones.clone());
    for &r in &refs {
        let intervals = stones.get(r);
        // Without copy, the intervals could be [math.MinInt64, 2].
        assert_eq!(vec![iv(1, 2)], intervals);
    }
}

#[test]
fn test_block_querier_delete() {
    let stones: Arc<dyn tombstones::Reader> = Arc::new(MemTombstones::new_test_mem_tombstones(&[
        vec![iv(1, 3)],
        vec![iv(1, 3), iv(6, 10)],
        vec![iv(6, 10)],
    ]));
    let cases = vec![
        bq(0, 0, vec![], vec![], vec![]),
        bq(0, 0, vec![m(MatchType::Equal, "a", "a")], vec![], vec![]),
        bq(1, 0, vec![m(MatchType::Equal, "a", "a")], vec![], vec![]),
        bq(i64::MIN, i64::MAX, vec![m(MatchType::Equal, "a", "x")], vec![], vec![]),
        bq(
            i64::MIN,
            i64::MAX,
            vec![m(MatchType::Regexp, "a", ".*")],
            vec![
                (ls(&["a", "a"]), fl(&[(5, 2.), (6, 3.), (7, 4.)])),
                (ls(&["a", "a", "b", "b"]), fl(&[(5, 3.)])),
                (ls(&["b", "b"]), fl(&[(1, 3.), (2, 2.), (3, 6.), (5, 1.)])),
            ],
            vec![
                (ls(&["a", "a"]), vec![fl(&[(5, 2.), (6, 3.), (7, 4.)])]),
                (ls(&["a", "a", "b", "b"]), vec![fl(&[(5, 3.)])]),
                (ls(&["b", "b"]), vec![fl(&[(1, 3.), (2, 2.), (3, 6.)]), fl(&[(5, 1.)])]),
            ],
        ),
        bq(
            2,
            6,
            vec![m(MatchType::Equal, "a", "a")],
            vec![(ls(&["a", "a"]), fl(&[(5, 2.), (6, 3.)])), (ls(&["a", "a", "b", "b"]), fl(&[(5, 3.)]))],
            vec![
                (ls(&["a", "a"]), vec![fl(&[(5, 2.), (6, 3.)])]),
                (ls(&["a", "a", "b", "b"]), vec![fl(&[(5, 3.)])]),
            ],
        ),
    ];
    for c in &cases {
        let (ir, cr, _, _) = create_idx_chk_readers(&test_data());
        test_block_querier_case(c, ir, cr, stones.clone());
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Fake chunk readers.

/// Go `fakeChunksReader`.
#[derive(Default)]
struct FakeChunksReader {
    chks: HashMap<u64, Arc<dyn Chunk>>,
    iterables: HashMap<u64, Vec<Sample>>,
}

impl ChunkReader for FakeChunksReader {
    fn chunk_or_iterable(&self, meta: &Meta) -> Result<ChunkOrIterable, Error> {
        if let Some(c) = self.chks.get(&meta.chunk_ref) {
            return Ok(ChunkOrIterable::Chunk(c.clone()));
        }
        if let Some(s) = self.iterables.get(&meta.chunk_ref) {
            return Ok(ChunkOrIterable::Iterable(Box::new(MockIterable { s: s.clone() })));
        }
        Err(Error::msg(format!("chunk or iterable not found at ref {}", meta.chunk_ref)))
    }
    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// Go `createFakeReaderAndNotPopulatedChunks`.
fn create_fake_reader_and_not_populated_chunks(s: &[Vec<Sample>]) -> (FakeChunksReader, Vec<Meta>) {
    let mut f = FakeChunksReader::default();
    let mut chks = Vec::with_capacity(s.len());
    for (r, samples) in s.iter().enumerate() {
        let c = storage::chunks::chunk_from_samples(samples).unwrap();
        f.chks.insert(r as u64, c.chunk.clone().unwrap());
        chks.push(Meta { chunk_ref: r as u64, chunk: None, min_time: c.min_time, max_time: c.max_time });
    }
    (f, chks)
}

/// Go `createFakeReaderAndIterables`. Samples in each slice are assumed to be sorted.
fn create_fake_reader_and_iterables(s: &[Vec<Sample>]) -> (FakeChunksReader, Vec<Meta>) {
    let mut f = FakeChunksReader::default();
    let mut chks = Vec::with_capacity(s.len());
    for (r, samples) in s.iter().enumerate() {
        f.iterables.insert(r as u64, samples.clone());
        let (min_time, max_time) =
            if samples.is_empty() { (0, 0) } else { (samples[0].t, samples[samples.len() - 1].t) };
        chks.push(Meta { chunk_ref: r as u64, chunk: None, min_time, max_time });
    }
    (f, chks)
}

/// Go `mockIterable`.
struct MockIterable {
    s: Vec<Sample>,
}

impl Iterable for MockIterable {
    fn iterator<'a>(&'a self, _reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        AnyIterator::Other(Box::new(MockSampleIterator { s: &self.s, idx: -1 }))
    }
}

/// Go `mockSampleIterator`.
struct MockSampleIterator<'a> {
    s: &'a [Sample],
    idx: isize,
}

impl ChunkIterator for MockSampleIterator<'_> {
    fn seek(&mut self, t: i64) -> ValueType {
        while self.idx < self.s.len() as isize {
            if self.idx != -1 && self.s[self.idx as usize].t >= t {
                return self.s[self.idx as usize].typ();
            }
            self.idx += 1;
        }
        ValueType::None
    }
    fn at(&self) -> (i64, f64) {
        let s = &self.s[self.idx as usize];
        (s.t, s.f)
    }
    fn at_histogram(&self, _h: Option<Histogram>) -> (i64, Histogram) {
        let s = &self.s[self.idx as usize];
        (s.t, s.h.as_deref().cloned().unwrap_or_default())
    }
    fn at_float_histogram(&self, _fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        let s = &self.s[self.idx as usize];
        (s.t, s.fh.as_deref().cloned().unwrap_or_default())
    }
    fn at_t(&self) -> i64 {
        self.s[self.idx as usize].t
    }
    fn at_st(&self) -> i64 {
        self.s[self.idx as usize].st
    }
    fn next(&mut self) -> ValueType {
        if self.idx < self.s.len() as isize - 1 {
            self.idx += 1;
            return self.s[self.idx as usize].typ();
        }
        ValueType::None
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        None
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Populate with tombstones.

#[derive(Default)]
struct Case {
    name: &'static str,
    samples: Vec<Vec<Sample>>,
    expected: Vec<Sample>,
    expected_chks: Vec<Meta>,
    expected_min_max_times: Vec<(i64, i64)>,
    intervals: Intervals,
    // Seek being zero means do not test seek.
    seek: i64,
    seek_success: bool,
    // Set this to true if a sample slice will form multiple chunks.
    skip_chunk_test: bool,
    skip_iterable_test: bool,
}

fn check_chunk_results(tc: &Case, mut expanded: Vec<Meta>) {
    // We don't care about ref IDs for comparison, only chunk's samples matters.
    rm_chunk_refs(&mut expanded);
    let mut exp = tc.expected_chks.clone();
    rm_chunk_refs(&mut exp);
    let ev: Vec<_> = exp.iter().map(meta_view).collect();
    let av: Vec<_> = expanded.iter().map(meta_view).collect();
    assert_eq!(ev, av, "{}", tc.name);
    for (i, meta) in expanded.iter().enumerate() {
        assert_eq!(tc.expected_min_max_times[i].0, meta.min_time, "{}", tc.name);
        assert_eq!(tc.expected_min_max_times[i].1, meta.max_time, "{}", tc.name);
    }
}

#[test]
fn test_populate_with_tomb_series_iterators() {
    let cases: Vec<Case> = vec![
		Case {
			name:    "no chunk",
			samples: vec![],
		..Default::default() },
		Case {
			name:    "one empty chunk", // This should never happen.
			samples: vec![vec![]],

			expected_chks: vec![
				chk(vec![]),
			],
			expected_min_max_times: vec![(0, 0)],
			// iterables with no samples will return no chunks instead of empty chunks
			skip_iterable_test: true,
		..Default::default() },
		Case {
			name:    "one empty iterable",
			samples: vec![vec![]],

			// iterables with no samples will return no chunks
			expected_chks: vec![],
			skip_chunk_test: true,
		..Default::default() },
		Case {
			name:    "three empty chunks", // This should never happen.
			samples: vec![vec![], vec![], vec![]],

			expected_chks: vec![
				chk(vec![]),
				chk(vec![]),
				chk(vec![]),
			],
			expected_min_max_times: vec![(0, 0), (0, 0), (0, 0)],
			// iterables with no samples will return no chunks instead of empty chunks
			skip_iterable_test: true,
		..Default::default() },
		Case {
			name:    "three empty iterables",
			samples: vec![vec![], vec![], vec![]],

			// iterables with no samples will return no chunks
			expected_chks: vec![],
			skip_chunk_test: true,
		..Default::default() },
		Case {
			name: "one chunk",
			samples: vec![
				vec![sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0)],
			],

			expected: vec![
				sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0),
			],
			expected_chks: vec![
				chk(vec![
					sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0),
				]),
			],
			expected_min_max_times: vec![(1, 6)],
		..Default::default() },
		Case {
			name: "two full chunks",
			samples: vec![
				vec![sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0)],
				vec![sf(0, 7, 89.0), sf(0, 9, 8.0)],
			],

			expected: vec![
				sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0), sf(0, 7, 89.0), sf(0, 9, 8.0),
			],
			expected_chks: vec![
				chk(vec![
					sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0),
				]),
				chk(vec![
					sf(0, 7, 89.0), sf(0, 9, 8.0),
				]),
			],
			expected_min_max_times: vec![(1, 6), (7, 9)],
		..Default::default() },
		Case {
			name: "three full chunks",
			samples: vec![
				vec![sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0)],
				vec![sf(0, 7, 89.0), sf(0, 9, 8.0)],
				vec![sf(0, 10, 22.0), sf(0, 203, 3493.0)],
			],

			expected: vec![
				sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0), sf(0, 7, 89.0), sf(0, 9, 8.0), sf(0, 10, 22.0), sf(0, 203, 3493.0),
			],
			expected_chks: vec![
				chk(vec![
					sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0),
				]),
				chk(vec![
					sf(0, 7, 89.0), sf(0, 9, 8.0),
				]),
				chk(vec![
					sf(0, 10, 22.0), sf(0, 203, 3493.0),
				]),
			],
			expected_min_max_times: vec![(1, 6), (7, 9), (10, 203)],
		..Default::default() },
		// Seek cases.
		Case {
			name:    "three empty chunks and seek", // This should never happen.
			samples: vec![vec![], vec![], vec![]],
			seek:    1,

			seek_success: false,
		..Default::default() },
		Case {
			name: "two chunks and seek beyond chunks",
			samples: vec![
				vec![sf(0, 1, 2.0), sf(0, 3, 5.0), sf(0, 6, 1.0)],
				vec![sf(0, 7, 89.0), sf(0, 9, 8.0)],
			],
			seek: 10,

			seek_success: false,
		..Default::default() },
		Case {
			name: "two chunks and seek on middle of first chunk",
			samples: vec![
				vec![sf(0, 1, 2.0), sf(0, 3, 5.0), sf(0, 6, 1.0)],
				vec![sf(0, 7, 89.0), sf(0, 9, 8.0)],
			],
			seek: 2,

			seek_success: true,
			expected: vec![
				sf(0, 3, 5.0), sf(0, 6, 1.0), sf(0, 7, 89.0), sf(0, 9, 8.0),
			],
		..Default::default() },
		Case {
			name: "two chunks and seek before first chunk",
			samples: vec![
				vec![sf(0, 1, 2.0), sf(0, 3, 5.0), sf(0, 6, 1.0)],
				vec![sf(0, 7, 89.0), sf(0, 9, 8.0)],
			],
			seek: -32,

			seek_success: true,
			expected: vec![
				sf(0, 1, 2.0), sf(0, 3, 5.0), sf(0, 6, 1.0), sf(0, 7, 89.0), sf(0, 9, 8.0),
			],
		..Default::default() },
		// Deletion / Trim cases.
		Case {
			name:      "no chunk with deletion interval",
			samples:   vec![],
			intervals: vec![iv(20, 21)],
		..Default::default() },
		Case {
			name: "two chunks with trimmed first and last samples from edge chunks",
			samples: vec![
				vec![sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0)],
				vec![sf(0, 7, 89.0), sf(0, 9, 8.0)],
			],
			intervals: ivs_add(vec![iv(i64::MIN, 2)], iv(9, i64::MAX)),

			expected: vec![
				sf(0, 3, 5.0), sf(0, 6, 1.0), sf(0, 7, 89.0),
			],
			expected_chks: vec![
				chk(vec![
					sf(0, 3, 5.0), sf(0, 6, 1.0),
				]),
				chk(vec![
					sf(0, 7, 89.0),
				]),
			],
			expected_min_max_times: vec![(3, 6), (7, 7)],
		..Default::default() },
		Case {
			name: "two chunks with trimmed middle sample of first chunk",
			samples: vec![
				vec![sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0)],
				vec![sf(0, 7, 89.0), sf(0, 9, 8.0)],
			],
			intervals: vec![iv(2, 3)],

			expected: vec![
				sf(0, 1, 2.0), sf(0, 6, 1.0), sf(0, 7, 89.0), sf(0, 9, 8.0),
			],
			expected_chks: vec![
				chk(vec![
					sf(0, 1, 2.0), sf(0, 6, 1.0),
				]),
				chk(vec![
					sf(0, 7, 89.0), sf(0, 9, 8.0),
				]),
			],
			expected_min_max_times: vec![(1, 6), (7, 9)],
		..Default::default() },
		Case {
			name: "two chunks with deletion across two chunks",
			samples: vec![
				vec![sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0)],
				vec![sf(0, 7, 89.0), sf(0, 9, 8.0)],
			],
			intervals: vec![iv(6, 7)],

			expected: vec![
				sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 9, 8.0),
			],
			expected_chks: vec![
				chk(vec![
					sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0),
				]),
				chk(vec![
					sf(0, 9, 8.0),
				]),
			],
			expected_min_max_times: vec![(1, 3), (9, 9)],
		..Default::default() },
		Case {
			name: "two chunks with first chunk deleted",
			samples: vec![
				vec![sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0)],
				vec![sf(0, 7, 89.0), sf(0, 9, 8.0)],
			],
			intervals: vec![iv(1, 6)],

			expected: vec![
				sf(0, 7, 89.0), sf(0, 9, 8.0),
			],
			expected_chks: vec![
				chk(vec![
					sf(0, 7, 89.0), sf(0, 9, 8.0),
				]),
			],
			expected_min_max_times: vec![(7, 9)],
		..Default::default() },
		// Deletion with seek.
		Case {
			name: "two chunks with trimmed first and last samples from edge chunks, seek from middle of first chunk",
			samples: vec![
				vec![sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0)],
				vec![sf(0, 7, 89.0), sf(0, 9, 8.0)],
			],
			intervals: ivs_add(vec![iv(i64::MIN, 2)], iv(9, i64::MAX)),

			seek:        3,
			seek_success: true,
			expected: vec![
				sf(0, 3, 5.0), sf(0, 6, 1.0), sf(0, 7, 89.0),
			],
		..Default::default() },
		Case {
			name: "one chunk where all samples are trimmed",
			samples: vec![
				vec![sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0)],
				vec![sf(0, 7, 89.0), sf(0, 9, 8.0)],
			],
			intervals: ivs_add(vec![iv(i64::MIN, 3)], iv(4, i64::MAX)),

			expected: vec![],
			expected_chks: vec![],
		..Default::default() },
		Case {
			name: "one histogram chunk",
			samples: vec![
				vec![
					sh(0, 1, tu::generate_test_histogram(1)),
					sh(0, 2, tu::generate_test_histogram(2)),
					sh(0, 3, tu::generate_test_histogram(3)),
					sh(0, 6, tu::generate_test_histogram(6)),
				],
			],
			expected: vec![
				sh(0, 1, tu::generate_test_histogram(1)),
				sh(0, 2, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(2))),
				sh(0, 3, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(3))),
				sh(0, 6, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(6))),
			],
			expected_chks: vec![
				chk(vec![
					sh(0, 1, tu::generate_test_histogram(1)),
					sh(0, 2, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(2))),
					sh(0, 3, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(3))),
					sh(0, 6, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(6))),
				]),
			],
			expected_min_max_times: vec![(1, 6)],
		..Default::default() },
		Case {
			name: "one histogram chunk intersect with earlier deletion interval",
			samples: vec![
				vec![
					sh(0, 1, tu::generate_test_histogram(1)),
					sh(0, 2, tu::generate_test_histogram(2)),
					sh(0, 3, tu::generate_test_histogram(3)),
					sh(0, 6, tu::generate_test_histogram(6)),
				],
			],
			intervals: vec![iv(1, 2)],
			expected: vec![
				sh(0, 3, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(3))),
				sh(0, 6, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(6))),
			],
			expected_chks: vec![
				chk(vec![
					sh(0, 3, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(3))),
					sh(0, 6, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(6))),
				]),
			],
			expected_min_max_times: vec![(3, 6)],
		..Default::default() },
		Case {
			name: "one histogram chunk intersect with later deletion interval",
			samples: vec![
				vec![
					sh(0, 1, tu::generate_test_histogram(1)),
					sh(0, 2, tu::generate_test_histogram(2)),
					sh(0, 3, tu::generate_test_histogram(3)),
					sh(0, 6, tu::generate_test_histogram(6)),
				],
			],
			intervals: vec![iv(5, 20)],
			expected: vec![
				sh(0, 1, tu::generate_test_histogram(1)),
				sh(0, 2, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(2))),
				sh(0, 3, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(3))),
			],
			expected_chks: vec![
				chk(vec![
					sh(0, 1, tu::generate_test_histogram(1)),
					sh(0, 2, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(2))),
					sh(0, 3, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(3))),
				]),
			],
			expected_min_max_times: vec![(1, 3)],
		..Default::default() },
		Case {
			name: "one float histogram chunk",
			samples: vec![
				vec![
					sfh(0, 1, tu::generate_test_float_histogram(1)),
					sfh(0, 2, tu::generate_test_float_histogram(2)),
					sfh(0, 3, tu::generate_test_float_histogram(3)),
					sfh(0, 6, tu::generate_test_float_histogram(6)),
				],
			],
			expected: vec![
				sfh(0, 1, tu::generate_test_float_histogram(1)),
				sfh(0, 2, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(2))),
				sfh(0, 3, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(3))),
				sfh(0, 6, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(6))),
			],
			expected_chks: vec![
				chk(vec![
					sfh(0, 1, tu::generate_test_float_histogram(1)),
					sfh(0, 2, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(2))),
					sfh(0, 3, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(3))),
					sfh(0, 6, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(6))),
				]),
			],
			expected_min_max_times: vec![(1, 6)],
		..Default::default() },
		Case {
			name: "one float histogram chunk intersect with earlier deletion interval",
			samples: vec![
				vec![
					sfh(0, 1, tu::generate_test_float_histogram(1)),
					sfh(0, 2, tu::generate_test_float_histogram(2)),
					sfh(0, 3, tu::generate_test_float_histogram(3)),
					sfh(0, 6, tu::generate_test_float_histogram(6)),
				],
			],
			intervals: vec![iv(1, 2)],
			expected: vec![
				sfh(0, 3, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(3))),
				sfh(0, 6, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(6))),
			],
			expected_chks: vec![
				chk(vec![
					sfh(0, 3, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(3))),
					sfh(0, 6, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(6))),
				]),
			],
			expected_min_max_times: vec![(3, 6)],
		..Default::default() },
		Case {
			name: "one float histogram chunk intersect with later deletion interval",
			samples: vec![
				vec![
					sfh(0, 1, tu::generate_test_float_histogram(1)),
					sfh(0, 2, tu::generate_test_float_histogram(2)),
					sfh(0, 3, tu::generate_test_float_histogram(3)),
					sfh(0, 6, tu::generate_test_float_histogram(6)),
				],
			],
			intervals: vec![iv(5, 20)],
			expected: vec![
				sfh(0, 1, tu::generate_test_float_histogram(1)),
				sfh(0, 2, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(2))),
				sfh(0, 3, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(3))),
			],
			expected_chks: vec![
				chk(vec![
					sfh(0, 1, tu::generate_test_float_histogram(1)),
					sfh(0, 2, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(2))),
					sfh(0, 3, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(3))),
				]),
			],
			expected_min_max_times: vec![(1, 3)],
		..Default::default() },
		Case {
			name: "one gauge histogram chunk",
			samples: vec![
				vec![
					sh(0, 1, tu::generate_test_gauge_histogram(1)),
					sh(0, 2, tu::generate_test_gauge_histogram(2)),
					sh(0, 3, tu::generate_test_gauge_histogram(3)),
					sh(0, 6, tu::generate_test_gauge_histogram(6)),
				],
			],
			expected: vec![
				sh(0, 1, tu::generate_test_gauge_histogram(1)),
				sh(0, 2, tu::generate_test_gauge_histogram(2)),
				sh(0, 3, tu::generate_test_gauge_histogram(3)),
				sh(0, 6, tu::generate_test_gauge_histogram(6)),
			],
			expected_chks: vec![
				chk(vec![
					sh(0, 1, tu::generate_test_gauge_histogram(1)),
					sh(0, 2, tu::generate_test_gauge_histogram(2)),
					sh(0, 3, tu::generate_test_gauge_histogram(3)),
					sh(0, 6, tu::generate_test_gauge_histogram(6)),
				]),
			],
			expected_min_max_times: vec![(1, 6)],
		..Default::default() },
		Case {
			name: "one gauge histogram chunk intersect with earlier deletion interval",
			samples: vec![
				vec![
					sh(0, 1, tu::generate_test_gauge_histogram(1)),
					sh(0, 2, tu::generate_test_gauge_histogram(2)),
					sh(0, 3, tu::generate_test_gauge_histogram(3)),
					sh(0, 6, tu::generate_test_gauge_histogram(6)),
				],
			],
			intervals: vec![iv(1, 2)],
			expected: vec![
				sh(0, 3, tu::generate_test_gauge_histogram(3)),
				sh(0, 6, tu::generate_test_gauge_histogram(6)),
			],
			expected_chks: vec![
				chk(vec![
					sh(0, 3, tu::generate_test_gauge_histogram(3)),
					sh(0, 6, tu::generate_test_gauge_histogram(6)),
				]),
			],
			expected_min_max_times: vec![(3, 6)],
		..Default::default() },
		Case {
			name: "one gauge histogram chunk intersect with later deletion interval",
			samples: vec![
				vec![
					sh(0, 1, tu::generate_test_gauge_histogram(1)),
					sh(0, 2, tu::generate_test_gauge_histogram(2)),
					sh(0, 3, tu::generate_test_gauge_histogram(3)),
					sh(0, 6, tu::generate_test_gauge_histogram(6)),
				],
			],
			intervals: vec![iv(5, 20)],
			expected: vec![
				sh(0, 1, tu::generate_test_gauge_histogram(1)),
				sh(0, 2, tu::generate_test_gauge_histogram(2)),
				sh(0, 3, tu::generate_test_gauge_histogram(3)),
			],
			expected_chks: vec![
				chk(vec![
					sh(0, 1, tu::generate_test_gauge_histogram(1)),
					sh(0, 2, tu::generate_test_gauge_histogram(2)),
					sh(0, 3, tu::generate_test_gauge_histogram(3)),
				]),
			],
			expected_min_max_times: vec![(1, 3)],
		..Default::default() },
		Case {
			name: "one gauge float histogram",
			samples: vec![
				vec![
					sfh(0, 1, tu::generate_test_gauge_float_histogram(1)),
					sfh(0, 2, tu::generate_test_gauge_float_histogram(2)),
					sfh(0, 3, tu::generate_test_gauge_float_histogram(3)),
					sfh(0, 6, tu::generate_test_gauge_float_histogram(6)),
				],
			],
			expected: vec![
				sfh(0, 1, tu::generate_test_gauge_float_histogram(1)),
				sfh(0, 2, tu::generate_test_gauge_float_histogram(2)),
				sfh(0, 3, tu::generate_test_gauge_float_histogram(3)),
				sfh(0, 6, tu::generate_test_gauge_float_histogram(6)),
			],
			expected_chks: vec![
				chk(vec![
					sfh(0, 1, tu::generate_test_gauge_float_histogram(1)),
					sfh(0, 2, tu::generate_test_gauge_float_histogram(2)),
					sfh(0, 3, tu::generate_test_gauge_float_histogram(3)),
					sfh(0, 6, tu::generate_test_gauge_float_histogram(6)),
				]),
			],
			expected_min_max_times: vec![(1, 6)],
		..Default::default() },
		Case {
			name: "one gauge float histogram chunk intersect with earlier deletion interval",
			samples: vec![
				vec![
					sfh(0, 1, tu::generate_test_gauge_float_histogram(1)),
					sfh(0, 2, tu::generate_test_gauge_float_histogram(2)),
					sfh(0, 3, tu::generate_test_gauge_float_histogram(3)),
					sfh(0, 6, tu::generate_test_gauge_float_histogram(6)),
				],
			],
			intervals: vec![iv(1, 2)],
			expected: vec![
				sfh(0, 3, tu::generate_test_gauge_float_histogram(3)),
				sfh(0, 6, tu::generate_test_gauge_float_histogram(6)),
			],
			expected_chks: vec![
				chk(vec![
					sfh(0, 3, tu::generate_test_gauge_float_histogram(3)),
					sfh(0, 6, tu::generate_test_gauge_float_histogram(6)),
				]),
			],
			expected_min_max_times: vec![(3, 6)],
		..Default::default() },
		Case {
			name: "one gauge float histogram chunk intersect with later deletion interval",
			samples: vec![
				vec![
					sfh(0, 1, tu::generate_test_gauge_float_histogram(1)),
					sfh(0, 2, tu::generate_test_gauge_float_histogram(2)),
					sfh(0, 3, tu::generate_test_gauge_float_histogram(3)),
					sfh(0, 6, tu::generate_test_gauge_float_histogram(6)),
				],
			],
			intervals: vec![iv(5, 20)],
			expected: vec![
				sfh(0, 1, tu::generate_test_gauge_float_histogram(1)),
				sfh(0, 2, tu::generate_test_gauge_float_histogram(2)),
				sfh(0, 3, tu::generate_test_gauge_float_histogram(3)),
			],
			expected_chks: vec![
				chk(vec![
					sfh(0, 1, tu::generate_test_gauge_float_histogram(1)),
					sfh(0, 2, tu::generate_test_gauge_float_histogram(2)),
					sfh(0, 3, tu::generate_test_gauge_float_histogram(3)),
				]),
			],
			expected_min_max_times: vec![(1, 3)],
		..Default::default() },
		Case {
			name: "three full mixed chunks",
			samples: vec![
				vec![sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0)],
				vec![
					sh(0, 7, tu::generate_test_gauge_histogram(89)),
					sh(0, 9, tu::generate_test_gauge_histogram(8)),
				],
				vec![
					sfh(0, 10, tu::generate_test_gauge_float_histogram(22)),
					sfh(0, 203, tu::generate_test_gauge_float_histogram(3493)),
				],
			],

			expected: vec![
				sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0), sh(0, 7, tu::generate_test_gauge_histogram(89)), sh(0, 9, tu::generate_test_gauge_histogram(8)), sfh(0, 10, tu::generate_test_gauge_float_histogram(22)), sfh(0, 203, tu::generate_test_gauge_float_histogram(3493)),
			],
			expected_chks: vec![
				chk(vec![
					sf(0, 1, 2.0), sf(0, 2, 3.0), sf(0, 3, 5.0), sf(0, 6, 1.0),
				]),
				chk(vec![
					sh(0, 7, tu::generate_test_gauge_histogram(89)),
					sh(0, 9, tu::generate_test_gauge_histogram(8)),
				]),
				chk(vec![
					sfh(0, 10, tu::generate_test_gauge_float_histogram(22)),
					sfh(0, 203, tu::generate_test_gauge_float_histogram(3493)),
				]),
			],
			expected_min_max_times: vec![(1, 6), (7, 9), (10, 203)],
		..Default::default() },
		Case {
			name: "three full mixed chunks in different order",
			samples: vec![
				vec![
					sh(0, 7, tu::generate_test_gauge_histogram(89)),
					sh(0, 9, tu::generate_test_gauge_histogram(8)),
				],
				vec![sf(0, 11, 2.0), sf(0, 12, 3.0), sf(0, 13, 5.0), sf(0, 16, 1.0)],
				vec![
					sfh(0, 100, tu::generate_test_gauge_float_histogram(22)),
					sfh(0, 203, tu::generate_test_gauge_float_histogram(3493)),
				],
			],

			expected: vec![
				sh(0, 7, tu::generate_test_gauge_histogram(89)), sh(0, 9, tu::generate_test_gauge_histogram(8)), sf(0, 11, 2.0), sf(0, 12, 3.0), sf(0, 13, 5.0), sf(0, 16, 1.0), sfh(0, 100, tu::generate_test_gauge_float_histogram(22)), sfh(0, 203, tu::generate_test_gauge_float_histogram(3493)),
			],
			expected_chks: vec![
				chk(vec![
					sh(0, 7, tu::generate_test_gauge_histogram(89)),
					sh(0, 9, tu::generate_test_gauge_histogram(8)),
				]),
				chk(vec![
					sf(0, 11, 2.0), sf(0, 12, 3.0), sf(0, 13, 5.0), sf(0, 16, 1.0),
				]),
				chk(vec![
					sfh(0, 100, tu::generate_test_gauge_float_histogram(22)),
					sfh(0, 203, tu::generate_test_gauge_float_histogram(3493)),
				]),
			],
			expected_min_max_times: vec![(7, 9), (11, 16), (100, 203)],
		..Default::default() },
		Case {
			name: "three full mixed chunks in different order intersect with deletion interval",
			samples: vec![
				vec![
					sh(0, 7, tu::generate_test_gauge_histogram(89)),
					sh(0, 9, tu::generate_test_gauge_histogram(8)),
				],
				vec![sf(0, 11, 2.0), sf(0, 12, 3.0), sf(0, 13, 5.0), sf(0, 16, 1.0)],
				vec![
					sfh(0, 100, tu::generate_test_gauge_float_histogram(22)),
					sfh(0, 203, tu::generate_test_gauge_float_histogram(3493)),
				],
			],
			intervals: vec![iv(8, 11), iv(15, 150)],

			expected: vec![
				sh(0, 7, tu::generate_test_gauge_histogram(89)), sf(0, 12, 3.0), sf(0, 13, 5.0), sfh(0, 203, tu::generate_test_gauge_float_histogram(3493)),
			],
			expected_chks: vec![
				chk(vec![
					sh(0, 7, tu::generate_test_gauge_histogram(89)),
				]),
				chk(vec![
					sf(0, 12, 3.0), sf(0, 13, 5.0),
				]),
				chk(vec![
					sfh(0, 203, tu::generate_test_gauge_float_histogram(3493)),
				]),
			],
			expected_min_max_times: vec![(7, 7), (12, 13), (203, 203)],
		..Default::default() },
		Case {
			name: "three full mixed chunks overlapping",
			samples: vec![
				vec![
					sh(0, 7, tu::generate_test_gauge_histogram(89)),
					sh(0, 12, tu::generate_test_gauge_histogram(8)),
				],
				vec![sf(0, 11, 2.0), sf(0, 12, 3.0), sf(0, 13, 5.0), sf(0, 16, 1.0)],
				vec![
					sfh(0, 10, tu::generate_test_gauge_float_histogram(22)),
					sfh(0, 203, tu::generate_test_gauge_float_histogram(3493)),
				],
			],

			expected: vec![
				sh(0, 7, tu::generate_test_gauge_histogram(89)), sh(0, 12, tu::generate_test_gauge_histogram(8)), sf(0, 11, 2.0), sf(0, 12, 3.0), sf(0, 13, 5.0), sf(0, 16, 1.0), sfh(0, 10, tu::generate_test_gauge_float_histogram(22)), sfh(0, 203, tu::generate_test_gauge_float_histogram(3493)),
			],
			expected_chks: vec![
				chk(vec![
					sh(0, 7, tu::generate_test_gauge_histogram(89)),
					sh(0, 12, tu::generate_test_gauge_histogram(8)),
				]),
				chk(vec![
					sf(0, 11, 2.0), sf(0, 12, 3.0), sf(0, 13, 5.0), sf(0, 16, 1.0),
				]),
				chk(vec![
					sfh(0, 10, tu::generate_test_gauge_float_histogram(22)),
					sfh(0, 203, tu::generate_test_gauge_float_histogram(3493)),
				]),
			],
			expected_min_max_times: vec![(7, 12), (11, 16), (10, 203)],
		..Default::default() },
		Case {
			name: "int histogram iterables with counter resets",
			samples: vec![
				vec![
					sh(0, 7, tu::generate_test_histogram(8)),
					sh(0, 8, tu::generate_test_histogram(9)),
					// Counter reset should be detected when chunks are created from the iterable.
					sh(0, 12, tu::generate_test_histogram(5)),
					sh(0, 15, tu::generate_test_histogram(6)),
					sh(0, 16, tu::generate_test_histogram(7)),
					// Counter reset should be detected when chunks are created from the iterable.
					sh(0, 17, tu::generate_test_histogram(5)),
				],
				vec![
					sh(0, 18, tu::generate_test_histogram(6)),
					sh(0, 19, tu::generate_test_histogram(7)),
					// Counter reset should be detected when chunks are created from the iterable.
					sh(0, 20, tu::generate_test_histogram(5)),
					sh(0, 21, tu::generate_test_histogram(6)),
				],
			],

			expected: vec![
				sh(0, 7, tu::generate_test_histogram(8)),
				sh(0, 8, tu::generate_test_histogram(9)),
				sh(0, 12, tu::generate_test_histogram(5)),
				sh(0, 15, tu::generate_test_histogram(6)),
				sh(0, 16, tu::generate_test_histogram(7)),
				sh(0, 17, tu::generate_test_histogram(5)),
				sh(0, 18, tu::generate_test_histogram(6)),
				sh(0, 19, tu::generate_test_histogram(7)),
				sh(0, 20, tu::generate_test_histogram(5)),
				sh(0, 21, tu::generate_test_histogram(6)),
			],
			expected_chks: vec![
				chk(vec![
					sh(0, 7, tu::generate_test_histogram(8)),
					sh(0, 8, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(9))),
				]),
				chk(vec![
					sh(0, 12, tu::set_histogram_counter_reset(tu::generate_test_histogram(5))),
					sh(0, 15, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(6))),
					sh(0, 16, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(7))),
				]),
				chk(vec![
					sh(0, 17, tu::set_histogram_counter_reset(tu::generate_test_histogram(5))),
				]),
				chk(vec![
					sh(0, 18, tu::generate_test_histogram(6)),
					sh(0, 19, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(7))),
				]),
				chk(vec![
					sh(0, 20, tu::set_histogram_counter_reset(tu::generate_test_histogram(5))),
					sh(0, 21, tu::set_histogram_not_counter_reset(tu::generate_test_histogram(6))),
				]),
			],
			expected_min_max_times: vec![
				(7, 8),
				(12, 16),
				(17, 17),
				(18, 19),
				(20, 21),
			],

			// Skipping chunk test - can't create a single chunk for each
			// sample slice since there are counter resets in the middle of
			// the slices.
			skip_chunk_test: true,
		..Default::default() },
		Case {
			name: "float histogram iterables with counter resets",
			samples: vec![
				vec![
					sfh(0, 7, tu::generate_test_float_histogram(8)),
					sfh(0, 8, tu::generate_test_float_histogram(9)),
					// Counter reset should be detected when chunks are created from the iterable.
					sfh(0, 12, tu::generate_test_float_histogram(5)),
					sfh(0, 15, tu::generate_test_float_histogram(6)),
					sfh(0, 16, tu::generate_test_float_histogram(7)),
					// Counter reset should be detected when chunks are created from the iterable.
					sfh(0, 17, tu::generate_test_float_histogram(5)),
				],
				vec![
					sfh(0, 18, tu::generate_test_float_histogram(6)),
					sfh(0, 19, tu::generate_test_float_histogram(7)),
					// Counter reset should be detected when chunks are created from the iterable.
					sfh(0, 20, tu::generate_test_float_histogram(5)),
					sfh(0, 21, tu::generate_test_float_histogram(6)),
				],
			],

			expected: vec![
				sfh(0, 7, tu::generate_test_float_histogram(8)),
				sfh(0, 8, tu::generate_test_float_histogram(9)),
				sfh(0, 12, tu::generate_test_float_histogram(5)),
				sfh(0, 15, tu::generate_test_float_histogram(6)),
				sfh(0, 16, tu::generate_test_float_histogram(7)),
				sfh(0, 17, tu::generate_test_float_histogram(5)),
				sfh(0, 18, tu::generate_test_float_histogram(6)),
				sfh(0, 19, tu::generate_test_float_histogram(7)),
				sfh(0, 20, tu::generate_test_float_histogram(5)),
				sfh(0, 21, tu::generate_test_float_histogram(6)),
			],
			expected_chks: vec![
				chk(vec![
					sfh(0, 7, tu::generate_test_float_histogram(8)),
					sfh(0, 8, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(9))),
				]),
				chk(vec![
					sfh(0, 12, tu::set_float_histogram_counter_reset(tu::generate_test_float_histogram(5))),
					sfh(0, 15, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(6))),
					sfh(0, 16, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(7))),
				]),
				chk(vec![
					sfh(0, 17, tu::set_float_histogram_counter_reset(tu::generate_test_float_histogram(5))),
				]),
				chk(vec![
					sfh(0, 18, tu::generate_test_float_histogram(6)),
					sfh(0, 19, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(7))),
				]),
				chk(vec![
					sfh(0, 20, tu::set_float_histogram_counter_reset(tu::generate_test_float_histogram(5))),
					sfh(0, 21, tu::set_float_histogram_not_counter_reset(tu::generate_test_float_histogram(6))),
				]),
			],
			expected_min_max_times: vec![
				(7, 8),
				(12, 16),
				(17, 17),
				(18, 19),
				(20, 21),
			],

			// Skipping chunk test - can't create a single chunk for each
			// sample slice since there are counter resets in the middle of
			// the slices.
			skip_chunk_test: true,
		..Default::default() },
		Case {
			name: "iterables with mixed encodings and counter resets",
			samples: vec![
				vec![
					sh(0, 7, tu::generate_test_histogram(8)),
					sh(0, 8, tu::generate_test_histogram(9)),
					sfh(0, 9, tu::generate_test_float_histogram(10)),
					sfh(0, 10, tu::generate_test_float_histogram(11)),
					sfh(0, 11, tu::generate_test_float_histogram(12)),
					sf(0, 12, 13.0),
					sf(0, 13, 14.0),
					sh(0, 14, tu::generate_test_histogram(8)),
					// Counter reset should be detected when chunks are created from the iterable.
					sh(0, 15, tu::generate_test_histogram(7)),
				],
				vec![
					sh(0, 18, tu::generate_test_histogram(6)),
					sf(0, 19, 45.0),
				],
			],

			expected: vec![
				sh(0, 7, tu::generate_test_histogram(8)),
				sh(0, 8, tu::generate_test_histogram(9)),
				sfh(0, 9, tu::generate_test_float_histogram(10)),
				sfh(0, 10, tu::generate_test_float_histogram(11)),
				sfh(0, 11, tu::generate_test_float_histogram(12)),
				sf(0, 12, 13.0),
				sf(0, 13, 14.0),
				sh(0, 14, tu::generate_test_histogram(8)),
				sh(0, 15, tu::generate_test_histogram(7)),
				sh(0, 18, tu::generate_test_histogram(6)),
				sf(0, 19, 45.0),
			],
			expected_chks: vec![
				chk(vec![
					sh(0, 7, tu::generate_test_histogram(8)),
					sh(0, 8, tu::generate_test_histogram(9)),
				]),
				chk(vec![
					sfh(0, 9, tu::generate_test_float_histogram(10)),
					sfh(0, 10, tu::generate_test_float_histogram(11)),
					sfh(0, 11, tu::generate_test_float_histogram(12)),
				]),
				chk(vec![
					sf(0, 12, 13.0),
					sf(0, 13, 14.0),
				]),
				chk(vec![
					sh(0, 14, tu::generate_test_histogram(8)),
				]),
				chk(vec![
					sh(0, 15, tu::set_histogram_counter_reset(tu::generate_test_histogram(7))),
				]),
				chk(vec![
					sh(0, 18, tu::generate_test_histogram(6)),
				]),
				chk(vec![
					sf(0, 19, 45.0),
				]),
			],
			expected_min_max_times: vec![
				(7, 8),
				(9, 11),
				(12, 13),
				(14, 14),
				(15, 15),
				(18, 18),
				(19, 19),
			],

			skip_chunk_test: true,
		..Default::default() },

    ];
    for tc in &cases {
        // Sample.
        {
            // If the test case wants to skip the chunks test, it probably means you can't create valid chunks from
            // sample slices, therefore create iterables over the samples instead.
            let (f, chk_metas) = if tc.skip_chunk_test {
                create_fake_reader_and_iterables(&tc.samples)
            } else {
                create_fake_reader_and_not_populated_chunks(&tc.samples)
            };
            let mut it = PopulateWithDelSeriesIterator::new(Ulid::default(), &f, &chk_metas, &tc.intervals);
            let mut r = Vec::new();
            if tc.seek != 0 {
                assert_eq!(tc.seek_success, it.seek(tc.seek) == ValueType::Float, "{}", tc.name);
                // Next one should be noop.
                assert_eq!(tc.seek_success, it.seek(tc.seek) == ValueType::Float, "{}", tc.name);
                if tc.seek_success {
                    // After successful seek iterator is ready. Grab the value.
                    let (t, v) = it.at();
                    r.push(sf(0, t, v));
                }
            }
            r.extend(storage::expand_samples(&mut it).unwrap());
            assert_eq!(tc.expected, r, "{}", tc.name);
        }
        // Chunk.
        if !tc.skip_chunk_test {
            let (f, chk_metas) = create_fake_reader_and_not_populated_chunks(&tc.samples);
            let mut it = PopulateWithDelChunkSeriesIterator::new(Ulid::default(), &f, &chk_metas, &tc.intervals);
            // Chunk iterator does not have Seek method.
            if tc.seek == 0 {
                let expanded = storage::expand_chunks(&mut it).unwrap();
                check_chunk_results(tc, expanded);
            }
        }
        // Iterables.
        if !tc.skip_iterable_test {
            let (f, chk_metas) = create_fake_reader_and_iterables(&tc.samples);
            let mut it = PopulateWithDelChunkSeriesIterator::new(Ulid::default(), &f, &chk_metas, &tc.intervals);
            if tc.seek == 0 {
                let expanded = storage::expand_chunks(&mut it).unwrap();
                check_chunk_results(tc, expanded);
            }
        }
    }
}

/// Go `checkCurrVal`.
fn check_curr_val(val_type: ValueType, it: &PopulateWithDelSeriesIterator<'_>, expected_ts: i64, expected_value: i64) {
    match val_type {
        ValueType::Float => {
            let (ts, v) = it.at();
            assert_eq!(expected_ts, ts);
            assert_eq!(expected_value as f64, v);
        }
        ValueType::Histogram => {
            let (ts, mut h) = it.at_histogram(None);
            assert_eq!(expected_ts, ts);
            h.counter_reset_hint = model::histogram::CounterResetHint::UnknownCounterReset;
            assert_eq!(tu::generate_test_histogram(expected_value), h);
        }
        ValueType::FloatHistogram => {
            let (ts, mut h) = it.at_float_histogram(None);
            assert_eq!(expected_ts, ts);
            h.counter_reset_hint = model::histogram::CounterResetHint::UnknownCounterReset;
            assert_eq!(tu::generate_test_float_histogram(expected_value), h);
        }
        ValueType::None => panic!("unexpected value type"),
    }
}

type TypedCase = (&'static str, ValueType, Vec<Vec<Sample>>);

fn h(t: i64, n: i64) -> Sample {
    sh(0, t, tu::generate_test_histogram(n))
}

fn fhs(t: i64, n: i64) -> Sample {
    sfh(0, t, tu::generate_test_float_histogram(n))
}

// Regression for: https://github.com/prometheus/tsdb/pull/97
#[test]
fn test_populate_with_del_series_iterator_double_seek() {
    let cases: Vec<TypedCase> = vec![
        ("float", ValueType::Float, vec![vec![], fl(&[(1, 1.), (2, 2.), (3, 3.)]), fl(&[(4, 4.), (5, 5.)])]),
        ("histogram", ValueType::Histogram, vec![vec![], vec![h(1, 1), h(2, 2), h(3, 3)], vec![h(4, 4), h(5, 5)]]),
        (
            "float histogram",
            ValueType::FloatHistogram,
            vec![vec![], vec![fhs(1, 1), fhs(2, 2), fhs(3, 3)], vec![fhs(4, 4), fhs(5, 5)]],
        ),
    ];
    for (name, vt, chks) in cases {
        let (f, chk_metas) = create_fake_reader_and_not_populated_chunks(&chks);
        let mut it = PopulateWithDelSeriesIterator::new(Ulid::default(), &f, &chk_metas, &[]);
        assert_eq!(vt, it.seek(1), "{name}");
        assert_eq!(vt, it.seek(2), "{name}");
        assert_eq!(vt, it.seek(2), "{name}");
        check_curr_val(vt, &it, 2, 2);
        assert_eq!(0, chk_metas[0].min_time);
        assert_eq!(1, chk_metas[1].min_time);
        assert_eq!(4, chk_metas[2].min_time);
    }
}

// Regression when seeked chunks were still found via binary search and we always skipped to the end when seeking a
// value in the current chunk.
#[test]
fn test_populate_with_del_series_iterator_seek_in_current_chunk() {
    let cases: Vec<TypedCase> = vec![
        ("float", ValueType::Float, vec![vec![], fl(&[(1, 2.), (3, 4.), (5, 6.), (7, 8.)]), vec![]]),
        ("histogram", ValueType::Histogram, vec![vec![], vec![h(1, 2), h(3, 4), h(5, 6), h(7, 8)], vec![]]),
        (
            "float histogram",
            ValueType::FloatHistogram,
            vec![vec![], vec![fhs(1, 2), fhs(3, 4), fhs(5, 6), fhs(7, 8)], vec![]],
        ),
    ];
    for (name, vt, chks) in cases {
        let (f, chk_metas) = create_fake_reader_and_not_populated_chunks(&chks);
        let mut it = PopulateWithDelSeriesIterator::new(Ulid::default(), &f, &chk_metas, &[]);
        assert_eq!(vt, it.next(), "{name}");
        check_curr_val(vt, &it, 1, 2);
        assert_eq!(vt, it.seek(4), "{name}");
        check_curr_val(vt, &it, 5, 6);
        assert_eq!(0, chk_metas[0].min_time);
        assert_eq!(1, chk_metas[1].min_time);
        assert_eq!(0, chk_metas[2].min_time);
    }
}

#[test]
fn test_populate_with_del_series_iterator_seek_with_min_time() {
    let cases: Vec<TypedCase> = vec![
        ("float", ValueType::Float, vec![fl(&[(1, 6.), (5, 6.), (6, 8.)])]),
        ("histogram", ValueType::Histogram, vec![vec![h(1, 6), h(5, 6), h(6, 8)]]),
        ("float histogram", ValueType::FloatHistogram, vec![vec![fhs(1, 6), fhs(5, 6), fhs(6, 8)]]),
    ];
    for (name, vt, chks) in cases {
        let (f, chk_metas) = create_fake_reader_and_not_populated_chunks(&chks);
        let mut it = PopulateWithDelSeriesIterator::new(Ulid::default(), &f, &chk_metas, &[]);
        assert_eq!(ValueType::None, it.seek(7), "{name}");
        assert_eq!(vt, it.seek(3), "{name}");
        assert_eq!(1, chk_metas[0].min_time);
    }
}

// Regression when calling Next() with a time bounded to fit within two samples. Seek gets called and advances
// beyond the max time, which was just accepted as a valid sample.
#[test]
fn test_populate_with_del_series_iterator_next_with_min_time() {
    let cases: Vec<TypedCase> = vec![
        ("float", ValueType::Float, vec![fl(&[(1, 6.), (5, 6.), (7, 8.)])]),
        ("histogram", ValueType::Histogram, vec![vec![h(1, 6), h(5, 6), h(7, 8)]]),
        ("float histogram", ValueType::FloatHistogram, vec![vec![fhs(1, 6), fhs(5, 6), fhs(7, 8)]]),
    ];
    for (name, _vt, chks) in cases {
        let (f, chk_metas) = create_fake_reader_and_not_populated_chunks(&chks);
        let ivs = ivs_add(vec![iv(i64::MIN, 2)], iv(4, i64::MAX));
        let mut it = PopulateWithDelSeriesIterator::new(Ulid::default(), &f, &chk_metas, &ivs);
        assert_eq!(ValueType::None, it.next(), "{name}");
        assert_eq!(1, chk_metas[0].min_time);
    }
}

// Tests that ST (start time) values are correctly preserved when iterating through chunks with ST support.
#[test]
fn test_populate_with_del_series_iterator_with_st() {
    let samples_with_st = vec![vec![sf(100, 1000, 1.0), sf(200, 2000, 2.0), sf(300, 3000, 3.0)]];
    let samples_varying_st =
        vec![vec![sf(0, 1000, 1.0), sf(1500, 1500, 1.5), sf(1900, 2000, 2.0), sf(500, 3000, 3.0)]];
    let cases = vec![
        ("all samples have non-zero ST", samples_with_st.clone(), samples_with_st[0].clone()),
        ("samples with varying ST patterns", samples_varying_st.clone(), samples_varying_st[0].clone()),
    ];
    for (name, samples, expected) in cases {
        for iterables in [false, true] {
            let (f, chk_metas) = if iterables {
                create_fake_reader_and_iterables(&samples)
            } else {
                create_fake_reader_and_not_populated_chunks(&samples)
            };
            let mut it = PopulateWithDelSeriesIterator::new(Ulid::default(), &f, &chk_metas, &[]);
            let mut result = Vec::new();
            while it.next() != ValueType::None {
                let st = it.at_st();
                let (ts, v) = it.at();
                result.push(sf(st, ts, v));
            }
            assert!(it.err().is_none());
            assert_eq!(expected, result, "{name}");
        }
    }
}

// Tests that ST (start time) values are correctly preserved when re-encoding chunks with deletions.
#[test]
fn test_populate_with_del_chunk_series_iterator_with_st() {
    let samples_with_st =
        vec![sf(100, 1000, 1.0), sf(200, 2000, 2.0), sf(300, 3000, 3.0), sf(400, 4000, 4.0), sf(500, 5000, 5.0)];
    let samples_with_no_leading_st =
        vec![sf(0, 1000, 1.0), sf(0, 2000, 2.0), sf(0, 3000, 3.0), sf(400, 4000, 4.0), sf(500, 5000, 5.0)];
    let cases: Vec<(&str, Vec<Vec<Sample>>, Intervals, Vec<Sample>)> = vec![
        ("no deletions - ST preserved", vec![samples_with_st.clone()], vec![], samples_with_st.clone()),
        (
            "with deletions - ST preserved in remaining samples",
            vec![samples_with_st.clone()],
            // Delete samples at t=2000 and t=4000.
            vec![iv(2000, 2000), iv(4000, 4000)],
            vec![sf(100, 1000, 1.0), sf(300, 3000, 3.0), sf(500, 5000, 5.0)],
        ),
        (
            "delete first sample - ST preserved",
            vec![samples_with_st.clone()],
            vec![iv(1000, 1000)],
            vec![sf(200, 2000, 2.0), sf(300, 3000, 3.0), sf(400, 4000, 4.0), sf(500, 5000, 5.0)],
        ),
        (
            // This tests that populateCurrForSingleChunk can handle chunks that don't start with ST, but introduce
            // ST later.
            "delete first sample - ST late preserved",
            vec![samples_with_no_leading_st.clone()],
            vec![iv(1000, 1000)],
            vec![sf(0, 2000, 2.0), sf(0, 3000, 3.0), sf(400, 4000, 4.0), sf(500, 5000, 5.0)],
        ),
    ];
    for (name, samples, intervals, expected) in cases {
        for iterables in [false, true] {
            let (f, chk_metas) = if iterables {
                create_fake_reader_and_iterables(&samples)
            } else {
                create_fake_reader_and_not_populated_chunks(&samples)
            };
            let mut it = PopulateWithDelChunkSeriesIterator::new(Ulid::default(), &f, &chk_metas, &intervals);
            let mut result = Vec::new();
            while it.next() {
                let meta = it.at();
                let mut chk_it = meta.chunk().iterator(None);
                while chk_it.next() != ValueType::None {
                    let st = chk_it.at_st();
                    let (ts, v) = chk_it.at();
                    result.push(sf(st, ts, v));
                }
                assert!(chk_it.err().is_none());
            }
            assert!(it.err().is_none());
            assert_eq!(expected, result, "{name} iterables={iterables}");
        }
    }
}
