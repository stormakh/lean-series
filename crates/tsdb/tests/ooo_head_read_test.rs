//! Port of `tsdb/ooo_head_read_test.go` (the tests that need only the head).
//!
//! Not ported here yet (they need the DB, `db.rs`): TestOOOHeadChunkReader_Chunk,
//! TestOOOHeadChunkReader_Chunk_ConsistentQueryResponseDespiteOfHeadExpanding, TestOOOQueryAcrossChunkIDWrap.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use model::labels::{self, Labels, ScratchBuilder};
use model::matcher::{MatchType, Matcher};
use storage::{
    Annotations, BoxSearchResultSet, BoxSeriesSet, Context, LabelHints, LabelQuerier, LabelResult, Meta, Querier,
    SearchHints, SearchResult, Searcher, SelectHints, ERR_NOT_FOUND,
};
use tsdb::head::{MmappedChunk, OooHeadChunk, OOO_CHUNK_ID_MASK};
use tsdb::ooo_head::OooChunk;
use tsdb::ooo_head_read::{as_multi_meta, less_by_min_time_and_min_ref, HeadAndOOOIndexReader, HeadAndOOOQuerier, OooCompactionHead};
use tsdb::reader::IndexReader;
use tsdb_format::chunks::{new_head_chunk_ref, ChunkDiskMapperRef};
use tsdb_format::wlog::compression;

use common::{new_test_head, sample_type_scenarios};

#[derive(Clone, Copy, Debug)]
struct ChunkInterval {
    id: i64,
    mint: i64,
    maxt: i64,
}

fn ci(id: i64, mint: i64, maxt: i64) -> ChunkInterval {
    ChunkInterval { id, mint, maxt }
}

struct ExpChunk {
    c: ChunkInterval,
    m: Vec<ChunkInterval>,
}

fn ec(c: ChunkInterval, m: Vec<ChunkInterval>) -> ExpChunk {
    ExpChunk { c, m }
}

/// Go `permutateChunkIntervals`.
fn permutate_chunk_intervals(
    inp: &mut Vec<ChunkInterval>,
    mut out: Vec<Vec<ChunkInterval>>,
    left: usize,
    right: usize,
) -> Vec<Vec<ChunkInterval>> {
    if left == right {
        out.push(inp.clone());
        return out;
    }
    for i in left..=right {
        inp.swap(left, i);
        out = permutate_chunk_intervals(inp, out, left + 1, right);
        inp.swap(left, i);
    }
    out
}

/// A comparable form of a chunk meta: (ref, mint, maxt, metas of a multiMeta chunk).
type MetaKey = (u64, i64, i64, Option<Vec<(u64, i64, i64)>>);

fn meta_key(m: &Meta) -> MetaKey {
    let multi = m.chunk.as_ref().map(|c| {
        as_multi_meta(c)
            .expect("only multiMeta chunks expected")
            .metas
            .iter()
            .map(|x| {
                assert!(x.chunk.is_none());
                (x.chunk_ref, x.min_time, x.max_time)
            })
            .collect()
    });
    (m.chunk_ref, m.min_time, m.max_time, multi)
}

#[test]
fn test_ooo_head_index_reader_series() {
    let tests: Vec<(&str, i64, i64, Vec<ChunkInterval>, Vec<ExpChunk>)> = vec![
        ("Empty result and no error when head is empty", 0, 100, vec![], vec![]),
        ("If query interval is bigger than the existing chunks nothing is returned", 500, 700, vec![ci(0, 100, 400)], vec![]),
        ("If query interval is smaller than the existing chunks nothing is returned", 100, 400, vec![ci(0, 500, 700)], vec![]),
        (
            "If query interval exceeds the existing chunk, it is returned",
            100,
            400,
            vec![ci(0, 150, 350)],
            vec![ec(ci(0, 150, 350), vec![])],
        ),
        (
            "If chunk exceeds the query interval, it is returned",
            150,
            350,
            vec![ci(0, 100, 400)],
            vec![ec(ci(0, 100, 400), vec![])],
        ),
        (
            "Pairwise overlaps should return the references of the first of each pair",
            0,
            700,
            vec![ci(0, 100, 200), ci(1, 500, 600), ci(2, 150, 250), ci(3, 550, 650)],
            vec![
                ec(ci(0, 100, 250), vec![ci(0, 100, 200), ci(2, 150, 250)]),
                ec(ci(1, 500, 650), vec![ci(1, 500, 600), ci(3, 550, 650)]),
            ],
        ),
        (
            "If all chunks overlap, single big chunk is returned",
            0,
            700,
            vec![ci(0, 100, 200), ci(1, 200, 300), ci(2, 300, 400), ci(3, 400, 500)],
            vec![ec(ci(0, 100, 500), vec![ci(0, 100, 200), ci(1, 200, 300), ci(2, 300, 400), ci(3, 400, 500)])],
        ),
        (
            "If no chunks overlap, all chunks are returned",
            0,
            700,
            vec![ci(0, 100, 199), ci(1, 200, 299), ci(2, 300, 399), ci(3, 400, 499)],
            vec![
                ec(ci(0, 100, 199), vec![]),
                ec(ci(1, 200, 299), vec![]),
                ec(ci(2, 300, 399), vec![]),
                ec(ci(3, 400, 499), vec![]),
            ],
        ),
        (
            "Triplet with pairwise overlaps, query range covers all, and distractor extra chunk",
            0,
            400,
            vec![ci(0, 100, 200), ci(1, 150, 300), ci(2, 250, 350), ci(3, 450, 550)],
            vec![ec(ci(0, 100, 350), vec![ci(0, 100, 200), ci(1, 150, 300), ci(2, 250, 350)])],
        ),
        (
            "Query interval partially overlaps some chunks",
            100,
            400,
            vec![ci(0, 250, 500), ci(1, 0, 200), ci(2, 150, 300)],
            vec![ec(ci(1, 0, 500), vec![ci(1, 0, 200), ci(2, 150, 300), ci(0, 250, 500)])],
        ),
        (
            "A full overlap pair and disjointed triplet",
            0,
            900,
            vec![ci(0, 100, 300), ci(1, 770, 850), ci(2, 150, 250), ci(3, 650, 750), ci(4, 600, 800)],
            vec![
                ec(ci(0, 100, 300), vec![ci(0, 100, 300), ci(2, 150, 250)]),
                ec(ci(4, 600, 850), vec![ci(4, 600, 800), ci(3, 650, 750), ci(1, 770, 850)]),
            ],
        ),
        (
            "Query range covers 3 disjoint chunks",
            0,
            650,
            vec![ci(0, 100, 150), ci(1, 300, 350), ci(2, 200, 250)],
            vec![ec(ci(0, 100, 150), vec![]), ec(ci(2, 200, 250), vec![]), ec(ci(1, 300, 350), vec![])],
        ),
    ];

    let s1_lset = labels::from_strings(&["foo", "bar"]);
    let s1_id: u64 = 1;

    for (name, query_min_t, query_max_t, mut input, exp_chunks) in tests {
        let permutations = if input.is_empty() {
            vec![vec![]]
        } else {
            let n = input.len();
            permutate_chunk_intervals(&mut input, Vec::new(), 0, n - 1)
        };
        for (perm, intervals) in permutations.into_iter().enumerate() {
            for head_chunk in [false, true] {
                let ctx = format!("name={name}, permutation={perm}, headChunk={head_chunk}");
                let h = new_test_head("ooo-index-series", 1000, compression::Type::None, true);
                h.init(0).expect("init");
                let (s1, _) = h.get_or_create(s1_id, &s1_lset, false).expect("series");
                let mut intervals = intervals.clone();
                {
                    let mut g = s1.lock();
                    g.ooo_or_init();
                    let find_id = |id: i64| -> u64 {
                        for (r, c) in intervals.iter().enumerate() {
                            if c.id == id {
                                return new_head_chunk_ref(s1_id, g.ooo_head_chunk_id(r));
                            }
                        }
                        0
                    };
                    let exp: Vec<MetaKey> = exp_chunks
                        .iter()
                        .map(|e| {
                            let multi = if e.m.is_empty() {
                                None
                            } else {
                                Some(e.m.iter().map(|x| (find_id(x.id), x.mint, x.maxt)).collect())
                            };
                            (find_id(e.c.id), e.c.mint, e.c.maxt, multi)
                        })
                        .collect();
                    if head_chunk && !intervals.is_empty() {
                        // Put the last interval in the head chunk.
                        let last = *intervals.last().unwrap();
                        g.ooo_mut().unwrap().ooo_head_chunk = Some(OooHeadChunk {
                            chunk: OooChunk::new(),
                            min_time: last.mint,
                            max_time: last.maxt,
                        });
                        intervals.pop();
                    }
                    for ic in &intervals {
                        g.ooo_mut().unwrap().ooo_mmapped_chunks.push(MmappedChunk {
                            r#ref: ChunkDiskMapperRef(0),
                            num_samples: 0,
                            encoding: chunkenc::ENC_NONE,
                            min_time: ic.mint,
                            max_time: ic.maxt,
                        });
                    }
                    drop(g);

                    let ir = HeadAndOOOIndexReader::new(
                        h.head.clone(),
                        query_min_t,
                        query_min_t,
                        query_max_t,
                        ChunkDiskMapperRef(0),
                    );
                    let mut chks = Vec::new();
                    let mut b = ScratchBuilder::new(0);
                    ir.series(s1_id, &mut b, &mut chks).expect("series");
                    assert_eq!(s1_lset, b.labels(), "{ctx}");
                    let got: Vec<MetaKey> = chks.iter().map(meta_key).collect();
                    assert_eq!(exp, got, "{ctx}");

                    let err = ir.series(s1_id + 1, &mut b, &mut chks).err().expect("error");
                    assert!(err.is(&ERR_NOT_FOUND), "{ctx}");
                }
            }
        }
    }
}

#[test]
fn test_ooo_head_chunk_reader_label_values() {
    for (name, scenario) in sample_type_scenarios() {
        let chunk_range = 2000;
        let head = new_test_head("ooo-label-values", chunk_range, compression::Type::None, true);
        let ctx = Context::background();
        let mut app = head.appender(&ctx);
        // Add in-order samples.
        scenario.append(&mut *app, &labels::from_strings(&["foo", "bar1"]), 100, 1).0.expect("append");
        scenario.append(&mut *app, &labels::from_strings(&["foo", "bar2"]), 100, 2).0.expect("append");
        // Add ooo samples for those series.
        scenario.append(&mut *app, &labels::from_strings(&["foo", "bar1"]), 90, 1).0.expect("append");
        scenario.append(&mut *app, &labels::from_strings(&["foo", "bar2"]), 90, 2).0.expect("append");
        assert!(app.commit().is_none());

        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let cases = [
            ("LabelValues calls when ooo head has max query range", i64::MIN, i64::MAX),
            ("LabelValues calls with ooo head query range not overlapping in-order data", 90, 90),
            ("LabelValues calls with ooo head query range not overlapping out-of-order data", 100, 100),
        ];
        for (case, min_t, max_t) in cases {
            let oh = HeadAndOOOIndexReader::new(head.head.clone(), min_t, min_t, max_t, ChunkDiskMapperRef(0));
            let m = |t, v| Matcher::new(t, "foo", v).expect("matcher");
            let mut values = oh.label_values(&ctx, "foo", None, &[m(MatchType::Equal, "bar1")]).expect("values");
            values.sort();
            assert_eq!(s(&["bar1"]), values, "{name}/{case}");
            let mut values = oh.label_values(&ctx, "foo", None, &[m(MatchType::NotRegexp, "^bar.")]).expect("values");
            values.sort();
            assert_eq!(Vec::<String>::new(), values, "{name}/{case}");
            let mut values = oh.label_values(&ctx, "foo", None, &[m(MatchType::Regexp, "bar.")]).expect("values");
            values.sort();
            assert_eq!(s(&["bar1", "bar2"]), values, "{name}/{case}");
            let mut values = oh.label_values(&ctx, "foo", None, &[]).expect("values");
            values.sort();
            assert_eq!(s(&["bar1", "bar2"]), values, "{name}/{case}");
        }
    }
}

#[test]
fn test_sort_meta_by_min_time_and_min_ref() {
    let m = |r: u64, t: i64| Meta {
        chunk_ref: r,
        min_time: t,
        ..Default::default()
    };
    let tests = vec![
        ("chunks are ordered by min time", vec![m(0, 0), m(1, 1)], vec![m(0, 0), m(1, 1)]),
        ("if same mintime, lower reference goes first", vec![m(10, 0), m(5, 0)], vec![m(5, 0), m(10, 0)]),
    ];
    for (name, mut input, exp) in tests {
        input.sort_by(less_by_min_time_and_min_ref);
        let k = |v: &[Meta]| v.iter().map(|x| (x.chunk_ref, x.min_time, x.max_time)).collect::<Vec<_>>();
        assert_eq!(k(&exp), k(&input), "name={name}");
    }
}

/// Go `mockSearchQuerier`.
struct MockSearchQuerier {
    label_names: Vec<String>,
    label_values: Vec<String>,
    searcher: bool,
}

impl LabelQuerier for MockSearchQuerier {
    fn label_values(&self, _: &Context, _: &str, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (self.label_values.clone(), Annotations::new(), None)
    }
    fn label_names(&self, _: &Context, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (self.label_names.clone(), Annotations::new(), None)
    }
    fn close(&mut self) -> Option<storage::Error> {
        None
    }
}

impl Querier for MockSearchQuerier {
    fn select(&self, _: &Context, _: bool, _: Option<&SelectHints>, _: &[Matcher]) -> BoxSeriesSet {
        storage::empty_series_set()
    }
    fn as_searcher(&self) -> Option<&dyn Searcher> {
        if self.searcher { Some(self) } else { None }
    }
}

fn results(v: &[String]) -> Vec<SearchResult> {
    v.iter().map(|x| SearchResult { value: x.clone(), score: 1.0 }).collect()
}

impl Searcher for MockSearchQuerier {
    fn search_label_names<'a>(&'a self, _: &Context, _: Option<&SearchHints>, _: &[Matcher]) -> BoxSearchResultSet<'a> {
        storage::new_search_result_set_from_slice(results(&self.label_names), Annotations::new())
    }
    fn search_label_values<'a>(&'a self, _: &Context, _: &str, _: Option<&SearchHints>, _: &[Matcher]) -> BoxSearchResultSet<'a> {
        storage::new_search_result_set_from_slice(results(&self.label_values), Annotations::new())
    }
}

fn collect(mut rs: BoxSearchResultSet<'_>) -> Vec<String> {
    let mut out = Vec::new();
    while rs.next() {
        out.push(rs.at().value.clone());
    }
    assert!(rs.err().is_none());
    assert!(rs.close().is_none());
    out
}

#[test]
fn test_head_and_ooo_querier_search() {
    let ctx = Context::background();
    let h = new_test_head("ooo-search", 1000, compression::Type::None, true);
    let mk = |q: Option<storage::BoxQuerier>| {
        HeadAndOOOQuerier::new(0, 0, 0, h.head.clone(), h.ooo_iso.track_read_after(ChunkDiskMapperRef(0)), q)
    };
    // nil inner querier returns empty.
    {
        let mut q = mk(None);
        assert!(collect(q.search_label_names(&ctx, None, &[])).is_empty());
        assert!(collect(q.search_label_values(&ctx, "env", None, &[])).is_empty());
        q.close();
    }
    // delegates to inner searcher.
    {
        let inner = MockSearchQuerier {
            label_names: vec!["env".into(), "job".into()],
            label_values: vec!["prod".into(), "dev".into()],
            searcher: true,
        };
        let mut q = mk(Some(Box::new(inner)));
        assert_eq!(vec!["env".to_string(), "job".to_string()], collect(q.search_label_names(&ctx, None, &[])));
        assert_eq!(vec!["prod".to_string(), "dev".to_string()], collect(q.search_label_values(&ctx, "env", None, &[])));
        q.close();
    }
    // non-searcher inner querier returns empty.
    {
        let inner = MockSearchQuerier {
            label_names: vec![],
            label_values: vec![],
            searcher: false,
        };
        let mut q = mk(Some(Box::new(inner)));
        let mut rs = q.search_label_names(&ctx, None, &[]);
        assert!(!rs.next());
        assert!(rs.close().is_none());
        drop(rs);
        let mut rs = q.search_label_values(&ctx, "env", None, &[]);
        assert!(!rs.next());
        assert!(rs.close().is_none());
        drop(rs);
        q.close();
    }
}

#[test]
fn test_new_ooo_compaction_head_panic_releases_lock() {
    let h = new_test_head("ooo-compaction-panic", tsdb::head::DEFAULT_BLOCK_DURATION, compression::Type::None, true);
    h.init(0).expect("init");
    let ctx = Context::background();
    let lbls = labels::from_strings(&["__name__", "series"]);
    let mut app = h.appender(&ctx);
    let in_order_ts = 60 * 60 * 1000;
    app.append(0, &lbls, in_order_ts, 0.0).expect("append");
    // An out-of-order sample within the OOO window creates an OOO head chunk.
    app.append(0, &lbls, in_order_ts - 5 * 60 * 1000, 1.0).expect("append");
    assert!(app.commit().is_none());

    let s = h.series.get_by_hash(lbls.hash(), &lbls).expect("series");
    assert!(s.lock().ooo().is_some(), "expected an OOO head chunk");

    // Removing the chunk directory makes the chunk write fail, so handleChunkWriteError panics while the series
    // lock is held.
    std::fs::remove_dir_all(tsdb::head::mmapped_chunks_dir(h.dir.path())).expect("remove dir");
    let head = h.head.clone();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = OooCompactionHead::new(&Context::background(), head);
    }));
    assert!(res.is_err(), "expected a panic");
    assert!(s.test_try_lock(), "series lock leaked after panic");
    // Closing the mapper first makes the writes of Head.Close() fail with ErrChunkDiskMapperClosed, which does
    // not panic.
    let _ = h.chunk_disk_mapper.close();
}

#[test]
fn test_ooo_head_chunk_id_wrap() {
    let h = new_test_head("ooo-chunk-id-wrap", 1000, compression::Type::None, true);
    h.init(0).expect("init");
    let (s, _) = h.get_or_create(1, &labels::from_strings(&["a", "b"]), false).expect("series");
    let mut g = s.lock();
    g.ooo_or_init().first_ooo_chunk_id = OOO_CHUNK_ID_MASK - 5;
    for pos in 0..=10u64 {
        let id = g.ooo_head_chunk_id(pos as usize);
        let expected = (pos + OOO_CHUNK_ID_MASK - 5) % OOO_CHUNK_ID_MASK;
        assert_ne!(0, id & OOO_CHUNK_ID_MASK, "pos {pos}: OOO flag (bit 23) must be set");
        assert!(id < 1 << 24, "pos {pos}: chunk ID must fit in 24 bits");
        assert_eq!(expected, id & (OOO_CHUNK_ID_MASK - 1), "pos {pos}: lower 23 bits mismatch");
        let r = new_head_chunk_ref(s.r#ref, id);
        let (_, chunk_id, is_ooo) = tsdb::head::unpack_head_chunk_ref(r);
        assert!(is_ooo, "pos {pos}: round-trip must identify as OOO");
        assert_eq!(expected, chunk_id, "pos {pos}: round-trip chunkID mismatch");
    }
}

#[test]
fn test_ooo_chunk_resolve_after_wrap() {
    let h = new_test_head("ooo-chunk-resolve-wrap", 1000, compression::Type::None, true);
    h.init(0).expect("init");
    let (s, _) = h.get_or_create(1, &labels::from_strings(&["a", "b"]), false).expect("series");
    let mut g = s.lock();
    g.ooo_or_init().first_ooo_chunk_id = OOO_CHUNK_ID_MASK - 5;
    let mut raw_ids = Vec::new();
    for i in 0..10i64 {
        let r = h.chunk_disk_mapper.write_chunk(
            s.r#ref,
            i * 100,
            i * 100 + 99,
            Arc::new(chunkenc::XorChunk::new()),
            true,
            None,
        );
        g.ooo_mut().unwrap().ooo_mmapped_chunks.push(MmappedChunk {
            r#ref: r,
            num_samples: 0,
            encoding: chunkenc::ENC_XOR,
            min_time: i * 100,
            max_time: i * 100 + 99,
        });
        let id = g.ooo_head_chunk_id(i as usize);
        let (_, raw, _) = tsdb::head::unpack_head_chunk_ref(new_head_chunk_ref(s.r#ref, id));
        raw_ids.push(raw);
    }
    // Simulate truncation of the first 8 chunks.
    {
        let o = g.ooo_mut().unwrap();
        o.ooo_mmapped_chunks.drain(..8);
        o.first_ooo_chunk_id = (o.first_ooo_chunk_id + 8) % OOO_CHUNK_ID_MASK;
        assert_eq!(3, o.first_ooo_chunk_id);
    }
    // Surviving chunks (original positions 8, 9) must resolve.
    for pos in [8, 9] {
        g.ooo_chunk(raw_ids[pos], &h.chunk_disk_mapper)
            .unwrap_or_else(|e| panic!("pos {pos} should resolve after truncation: {e}"));
    }
    // Truncated chunks (original positions 0-7) must return ErrNotFound.
    for pos in 0..8 {
        let err = g.ooo_chunk(raw_ids[pos], &h.chunk_disk_mapper).err().expect("error");
        assert!(err.is(&ERR_NOT_FOUND), "pos {pos} should return ErrNotFound after truncation");
    }
}

#[test]
fn test_ooo_mmap_guard_prevents_aliasing() {
    // The guard in mmapCurrentOOOHeadChunk caps oooMmappedChunks at oooChunkIDMask - 1 entries.
    assert_eq!((1 << 23) - 1, OOO_CHUNK_ID_MASK - 1);
    let h = new_test_head("ooo-mmap-guard", 1000, compression::Type::None, true);
    h.init(0).expect("init");
    let (s, _) = h.get_or_create(1, &labels::from_strings(&["a", "b"]), false).expect("series");
    let mut g = s.lock();
    g.ooo_or_init().first_ooo_chunk_id = OOO_CHUNK_ID_MASK - 100;
    let mut seen: HashMap<u64, usize> = HashMap::new();
    for pos in 0..200 {
        let id = g.ooo_head_chunk_id(pos);
        assert!(id < 1 << 24, "chunk ID must fit in 24 bits");
        if let Some(prev) = seen.get(&id) {
            panic!("position {pos} produced same HeadChunkID as position {prev}");
        }
        seen.insert(id, pos);
    }
    let _ = Labels::empty();
}
