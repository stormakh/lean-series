//! Port of `tsdb/head_read_test.go`.
//!
//! Rust note: the head chunks of a series are a vector (oldest first), so Go's linked-list walk, the
//! pre-collected `headChunks` fast path and the head chunk reader cache do not exist. The tests of those
//! optimizations check the same observable behavior (which chunk an ID resolves to) and are marked `adapted`.

mod common;

use std::sync::Arc;

use chunkenc::{ChunkIterator, ValueType};
use model::labels::{self, Labels, ScratchBuilder};
use storage::{Context, Meta, ERR_NOT_FOUND};
use tsdb::head::{HeadOptions, MemSeries, MemSeriesInner, OOO_CHUNK_ID_MASK};
use tsdb::head_append::ChunkOpts;
use tsdb::ooo_head_read::HeadAndOOOIndexReader;
use tsdb::reader::IndexReader;
use tsdb_format::chunks::{new_head_chunk_ref, ChunkDiskMapper, ChunkDiskMapperRef, DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_QUEUE_SIZE};
use tsdb_format::index;
use tsdb_format::wlog::compression;

use common::{new_head_no_wal, new_test_head_with_options, TempDir};

const CHUNK_RANGE: i64 = 100;
const CHUNK_STEP: i64 = 5;

fn append_samples(s: &MemSeries, g: &mut MemSeriesInner, start: i64, end: i64, cdm: &ChunkDiskMapper) {
    let opts = ChunkOpts {
        chunk_disk_mapper: Some(cdm),
        chunk_range: CHUNK_RANGE,
        samples_per_chunk: tsdb::head::DEFAULT_SAMPLES_PER_CHUNK,
        ..Default::default()
    };
    let mut i = start;
    while i < end {
        let (ok, _) = g.append(0, i, i as f64, 0, &opts, &s.lset, s.r#ref);
        assert!(ok, "sample append failed");
        i += CHUNK_STEP;
    }
}

/// The structure checks of the setups: (mmapped chunks, head chunks, oldest head chunk min time, newest head
/// chunk max time).
fn check_layout(g: &MemSeriesInner, mmapped: usize, head: usize, oldest_min: i64, newest_max: i64) {
    assert_eq!(mmapped, g.mmapped_chunks.len(), "wrong number of mmappedChunks");
    assert_eq!(head, g.head_chunks.len(), "wrong number of headChunks");
    assert_eq!(oldest_min, g.head_chunks[0].min_time, "wrong minTime on last headChunks element");
    assert_eq!(newest_max, g.head_chunks.last().unwrap().max_time, "wrong maxTime on first headChunks element");
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Out {
    OpenHeadChunk,
    ClosedHeadChunk,
    MmappedChunk,
    Err,
}

type Setup = fn(&MemSeries, &mut MemSeriesInner, &ChunkDiskMapper);

fn setup_one(s: &MemSeries, g: &mut MemSeriesInner, cdm: &ChunkDiskMapper) {
    append_samples(s, g, 0, CHUNK_RANGE, cdm);
    check_layout(g, 0, 1, 0, CHUNK_RANGE - CHUNK_STEP);
}

fn setup_three_mmapped(s: &MemSeries, g: &mut MemSeriesInner, cdm: &ChunkDiskMapper) {
    append_samples(s, g, 0, CHUNK_RANGE * 4, cdm);
    g.mmap_chunks(cdm, s.r#ref);
    check_layout(g, 3, 1, CHUNK_RANGE * 3, CHUNK_RANGE * 4 - CHUNK_STEP);
}

fn setup_three_mmapped_no_head(s: &MemSeries, g: &mut MemSeriesInner, cdm: &ChunkDiskMapper) {
    setup_three_mmapped(s, g, cdm);
    g.head_chunks.clear();
}

fn setup_three_mmapped_closed_cdm(s: &MemSeries, g: &mut MemSeriesInner, cdm: &ChunkDiskMapper) {
    setup_three_mmapped(s, g, cdm);
    let _ = cdm.close();
}

fn setup_three_head(s: &MemSeries, g: &mut MemSeriesInner, cdm: &ChunkDiskMapper) {
    append_samples(s, g, 0, CHUNK_RANGE * 3, cdm);
    check_layout(g, 0, 3, 0, CHUNK_RANGE * 3 - CHUNK_STEP);
}

fn setup_three_and_three(s: &MemSeries, g: &mut MemSeriesInner, cdm: &ChunkDiskMapper) {
    setup_three_mmapped(s, g, cdm);
    append_samples(s, g, CHUNK_RANGE * 4, CHUNK_RANGE * 6, cdm);
    check_layout(g, 3, 3, CHUNK_RANGE * 3, CHUNK_RANGE * 6 - CHUNK_STEP);
}

fn new_cdm(dir: &TempDir) -> ChunkDiskMapper {
    ChunkDiskMapper::new(dir.path(), None, DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_QUEUE_SIZE).expect("cdm")
}

#[test]
fn test_mem_series_chunk() {
    let tests: Vec<(&str, Option<Setup>, u64, Out)> = vec![
        ("call ix=0 on empty memSeries", None, 0, Out::Err),
        ("call ix=1 on empty memSeries", None, 1, Out::Err),
        (
            "firstChunkID > ix",
            Some(|s, g, cdm| {
                setup_one(s, g, cdm);
                g.first_chunk_id = 5;
            }),
            1,
            Out::Err,
        ),
        ("call ix=0 on memSeries with no mmapped chunks", Some(setup_one), 0, Out::OpenHeadChunk),
        ("call ix=1 on memSeries with no mmapped chunks", Some(setup_one), 1, Out::Err),
        ("call ix=10 on memSeries with no mmapped chunks", Some(setup_one), 10, Out::Err),
        ("call ix=0 on memSeries with 3 mmapped chunks", Some(setup_three_mmapped), 0, Out::MmappedChunk),
        ("call ix=1 on memSeries with 3 mmapped chunks", Some(setup_three_mmapped), 1, Out::MmappedChunk),
        ("call ix=3 on memSeries with 3 mmapped chunks", Some(setup_three_mmapped), 3, Out::OpenHeadChunk),
        (
            "call ix=0 on memSeries with 3 mmapped chunks and no headChunk",
            Some(setup_three_mmapped_no_head),
            0,
            Out::MmappedChunk,
        ),
        (
            "call ix=2 on memSeries with 3 mmapped chunks and no headChunk",
            Some(setup_three_mmapped_no_head),
            2,
            Out::MmappedChunk,
        ),
        (
            "call ix=3 on memSeries with 3 mmapped chunks and no headChunk",
            Some(setup_three_mmapped_no_head),
            3,
            Out::Err,
        ),
        (
            "call ix=1 on memSeries with 3 mmapped chunks and closed ChunkDiskMapper",
            Some(setup_three_mmapped_closed_cdm),
            1,
            Out::Err,
        ),
        (
            "call ix=3 on memSeries with 3 mmapped chunks and closed ChunkDiskMapper",
            Some(setup_three_mmapped_closed_cdm),
            3,
            Out::OpenHeadChunk,
        ),
        ("call ix=0 on memSeries with 3 head chunks and no mmapped chunks", Some(setup_three_head), 0, Out::ClosedHeadChunk),
        ("call ix=1 on memSeries with 3 head chunks and no mmapped chunks", Some(setup_three_head), 1, Out::ClosedHeadChunk),
        ("call ix=10 on memSeries with 3 head chunks and no mmapped chunks", Some(setup_three_head), 10, Out::Err),
        ("call ix=0 on memSeries with 3 head chunks and 3 mmapped chunks", Some(setup_three_and_three), 0, Out::MmappedChunk),
        ("call ix=2 on memSeries with 3 head chunks and 3 mmapped chunks", Some(setup_three_and_three), 2, Out::MmappedChunk),
        ("call ix=3 on memSeries with 3 head chunks and 3 mmapped chunks", Some(setup_three_and_three), 3, Out::ClosedHeadChunk),
        ("call ix=5 on memSeries with 3 head chunks and 3 mmapped chunks", Some(setup_three_and_three), 5, Out::OpenHeadChunk),
        ("call ix=6 on memSeries with 3 head chunks and 3 mmapped chunks", Some(setup_three_and_three), 6, Out::Err),
        ("call ix=10 on memSeries with 3 head chunks and 3 mmapped chunks", Some(setup_three_and_three), 10, Out::Err),
    ];

    for (name, setup, input_id, expected) in tests {
        let dir = TempDir::named("mem-series-chunk");
        let cdm = new_cdm(&dir);
        let series = MemSeries::new(Labels::empty(), 1, 0, true, false);
        let mut g = series.lock();
        if let Some(setup) = setup {
            setup(&series, &mut g, &cdm);
        }
        let res = g.chunk(input_id, &cdm);
        match expected {
            Out::OpenHeadChunk => {
                let (_, head_chunk, is_open) = res.unwrap_or_else(|e| panic!("{name}: unexpected error {e}"));
                assert!(head_chunk, "{name}: expected a chunk with headChunk=true");
                assert!(is_open, "{name}: expected a chunk with isOpen=true");
            }
            Out::ClosedHeadChunk => {
                let (_, head_chunk, is_open) = res.unwrap_or_else(|e| panic!("{name}: unexpected error {e}"));
                assert!(head_chunk, "{name}: expected a chunk with headChunk=true");
                assert!(!is_open, "{name}: expected a chunk with isOpen=false");
            }
            Out::MmappedChunk => {
                let (_, head_chunk, _) = res.unwrap_or_else(|e| panic!("{name}: unexpected error {e}"));
                assert!(!head_chunk, "{name}: expected a chunk with headChunk=false");
            }
            Out::Err => {
                // Go also requires a nil chunk with the error; here the result holds either a chunk or an error.
                assert!(res.is_err(), "{name}: expected an error");
            }
        }
        drop(g);
        let _ = cdm.close();
    }

    // head chunk count mismatch.
    // adapted: Go's headChunkCount can drift from the linked list length; here the count is the length of the
    // head chunk vector and cannot drift. The same lookups on a series without chunks give ErrNotFound.
    {
        let s = MemSeries::new(Labels::empty(), 1, 0, true, false);
        let dir = TempDir::named("mem-series-chunk-mismatch");
        let cdm = new_cdm(&dir);
        let g = s.lock();
        let err = g.chunk(1, &cdm).err().expect("error");
        assert!(err.is(&ERR_NOT_FOUND));
        let err = g.chunk(0, &cdm).err().expect("error");
        assert!(err.is(&ERR_NOT_FOUND));
        drop(g);
        let _ = cdm.close();
    }
}

/// adapted: Go compares the linked-list fallback with the pre-collected slice fast path. Rust has only one
/// (indexed) lookup; this checks the same results: every ID resolves, head chunks are the same chunk on
/// repeated lookups, the open flag is set only for the newest, and an out-of-range ID gives ErrNotFound.
#[test]
fn test_mem_series_chunk_fast_path() {
    let dir = TempDir::named("mem-series-fast-path");
    let cdm = new_cdm(&dir);
    let series = MemSeries::new(Labels::empty(), 1, 0, true, false);
    let mut g = series.lock();

    // Build 3 mmapped + 3 head chunks.
    append_samples(&series, &mut g, 0, CHUNK_RANGE * 4, &cdm);
    g.mmap_chunks(&cdm, series.r#ref);
    assert_eq!(3, g.mmapped_chunks.len());
    assert_eq!(1, g.head_chunks.len());
    append_samples(&series, &mut g, CHUNK_RANGE * 4, CHUNK_RANGE * 6, &cdm);
    assert_eq!(3, g.head_chunks.len());
    assert_eq!(3, g.mmapped_chunks.len());

    let head_chunk_count = g.head_chunk_count() as usize;
    assert_eq!(3, head_chunk_count, "expected 3 head chunks");
    let total_chunks = g.mmapped_chunks.len() + head_chunk_count;
    for ix in 0..total_chunks {
        let (c1, hc1, open1) = g.chunk(ix as u64, &cdm).expect("chunk");
        let (c2, hc2, open2) = g.chunk(ix as u64, &cdm).expect("chunk");
        assert_eq!(hc1, hc2, "ix={ix}: headChunk mismatch");
        assert_eq!(open1, open2, "ix={ix}: isOpen mismatch");
        assert_eq!(open1, ix == total_chunks - 1, "ix={ix}: isOpen");
        if ix < g.mmapped_chunks.len() {
            assert_eq!(c1.min_time, c2.min_time, "ix={ix}: minTime mismatch");
            assert_eq!(c1.max_time, c2.max_time, "ix={ix}: maxTime mismatch");
        } else {
            assert!(Arc::ptr_eq(&c1.chunk, &c2.chunk), "ix={ix}: head chunk pointer mismatch");
            assert!(Arc::ptr_eq(&c1.chunk, &g.head_chunks[ix - 3].chunk), "ix={ix}: head chunk pointer mismatch");
        }
    }
    // Out-of-range ID must return ErrNotFound.
    let err = g.chunk(total_chunks as u64, &cdm).err().expect("error");
    assert!(err.is(&ERR_NOT_FOUND));
    drop(g);
    let _ = cdm.close();
}

#[test]
fn test_mem_series_chunk_resolve_after_wrap() {
    const NUM_CHUNKS: usize = 20;
    let dir = TempDir::named("mem-series-wrap");
    let cdm = new_cdm(&dir);
    let series = MemSeries::new(Labels::empty(), 1, 0, true, false);
    let mut g = series.lock();
    g.first_chunk_id = OOO_CHUNK_ID_MASK - 10;

    append_samples(&series, &mut g, 0, CHUNK_RANGE * NUM_CHUNKS as i64, &cdm);
    g.mmap_chunks(&cdm, series.r#ref);
    assert_eq!(NUM_CHUNKS - 1, g.mmapped_chunks.len(), "wrong number of mmapped chunks");
    assert_eq!(1, g.head_chunks.len(), "wrong number of head chunks");

    // Chunk i covers [i*chunkRange, (i+1)*chunkRange). Positions 10+ take IDs past the 23-bit boundary, so they
    // wrap back to 0, 1, 2, ...
    let ids: Vec<u64> = (0..NUM_CHUNKS).map(|i| g.head_chunk_id(i)).collect();
    for (i, id) in ids.iter().enumerate() {
        assert_eq!(0, id & OOO_CHUNK_ID_MASK, "pos {i}: ID must not set the OOO flag");
    }
    assert_eq!(OOO_CHUNK_ID_MASK - 1, ids[9], "pos 9 should hold the last ID before the boundary");
    assert_eq!(0, ids[10], "pos 10 should have wrapped to 0");

    let require_resolves = |g: &MemSeriesInner, first_live: usize| {
        for pos in 0..NUM_CHUNKS {
            let res = g.chunk(ids[pos], &cdm);
            if pos < first_live {
                assert!(res.err().expect("error").is(&ERR_NOT_FOUND), "pos {pos} was truncated and must not resolve");
                continue;
            }
            let (chk, _, _) = res.unwrap_or_else(|e| panic!("pos {pos} should resolve: {e}"));
            assert_eq!(pos as i64 * CHUNK_RANGE, chk.min_time, "pos {pos} resolved to the wrong chunk");
        }
    };
    require_resolves(&g, 0);

    // Truncate positions 0-3.
    let removed = g.truncate_chunks_before(CHUNK_RANGE * 4, ChunkDiskMapperRef(0));
    assert_eq!(4, removed);
    assert_eq!(OOO_CHUNK_ID_MASK - 6, g.first_chunk_id);
    require_resolves(&g, 4);

    // Truncate positions 4-11, which carries firstChunkID itself past 0.
    let removed = g.truncate_chunks_before(CHUNK_RANGE * 12, ChunkDiskMapperRef(0));
    assert_eq!(8, removed);
    assert_eq!(2, g.first_chunk_id);
    require_resolves(&g, 12);
    drop(g);
    let _ = cdm.close();
}

#[test]
fn test_head_index_reader_postings_for_label_matching() {
    let opts = HeadOptions {
        chunk_range: 1000,
        ..HeadOptions::default()
    };
    let h = new_head_no_wal("postings-for-label-matching", opts);
    let ctx = Context::background();
    let series = [
        labels::from_strings(&["n", "1"]),
        labels::from_strings(&["n", "1", "i", "a"]),
        labels::from_strings(&["n", "1", "i", "b"]),
        labels::from_strings(&["n", "2"]),
        labels::from_strings(&["n", "2.5"]),
    ];
    let mut app = h.appender(&ctx);
    for s in &series {
        let _ = app.append(0, s, 0, 0.0);
    }
    assert!(app.commit().is_none());
    let ir = h.index();

    let offset = 0u64;
    let cases: Vec<(&str, &str, fn(&str) -> bool, Vec<u64>)> = vec![
        ("n=1", "n", |v| v == "1", vec![offset + 1, offset + 2, offset + 3]),
        ("n=2", "n", |v| v == "2", vec![offset + 4]),
        ("missing label", "missing", |_| true, vec![]),
    ];
    for (name, label_name, m, exp) in cases {
        let mut p = ir.postings_for_label_matching(&ctx, label_name, &m);
        let srs = index::expand_postings(&mut p).expect("expand");
        assert_eq!(exp, srs, "{name}");
    }
    ir.close().expect("close");
}

/// Go `newCacheTestHead`: a head whose single series has at least three head chunks and no mmapped chunks.
fn new_cache_test_head(name: &str) -> (common::TestHead, Arc<MemSeries>, u64) {
    let opts = HeadOptions {
        chunk_range: 100,
        ..HeadOptions::default()
    };
    let h = new_test_head_with_options(name, compression::Type::None, opts);
    let ctx = Context::background();
    let mut app = h.appender(&ctx);
    let lbls = labels::from_strings(&["__name__", "test"]);
    let mut i = 0;
    while i < 500 {
        app.append(0, &lbls, i, i as f64).expect("append");
        i += 5;
    }
    assert!(app.commit().is_none());
    let s = h.series.get_by_id(1).expect("series");
    let (head_chunks_len, mmapped_len, newest_ref) = {
        let g = s.lock();
        let newest_cid = g.first_chunk_id + g.mmapped_chunks.len() as u64 + g.head_chunks.len() as u64 - 1;
        (g.head_chunks.len(), g.mmapped_chunks.len(), new_head_chunk_ref(s.r#ref, newest_cid))
    };
    assert!(head_chunks_len > 2, "need at least 3 head chunks for the test");
    assert_eq!(0, mmapped_len, "test requires a series with no mmapped chunks");
    (h, s, newest_ref)
}

fn first_ts(c: &Arc<dyn chunkenc::Chunk>) -> i64 {
    let mut it = c.iterator(None);
    assert_eq!(ValueType::Float, it.next());
    it.at().0
}

fn meta(r: u64) -> Meta {
    Meta {
        chunk_ref: r,
        ..Default::default()
    }
}

/// adapted: Go tests the head chunk reader cache (a slice of the linked list kept between lookups). There is
/// no cache in Rust (head chunks are indexed directly); the subtests check that lookups after the changes that
/// would invalidate the cache (mmap, truncation) return the right chunk, and that a closed reader keeps no
/// isolation state open.
#[test]
fn test_head_chunk_reader_cache() {
    // cache_hit: repeated lookups of an unchanged series return the same chunk.
    {
        let (h, _, newest_ref) = new_cache_test_head("cache-hit");
        let cr = h.chunks_range(0, 10000, None).expect("reader");
        let (c1, _) = cr.chunk(&meta(newest_ref)).expect("chunk");
        let (c2, _) = cr.chunk(&meta(newest_ref)).expect("chunk");
        assert_eq!(c1.bytes(), c2.bytes());
    }
    // invalidated_after_mmap.
    {
        let (h, s, mut newest_ref) = new_cache_test_head("cache-mmap");
        let (head_chunks_len_before, newest_chunk_min_time) = {
            let g = s.lock();
            (g.head_chunks.len(), g.head_chunks.last().unwrap().min_time)
        };
        let cr = h.chunks_range(0, 10000, None).expect("reader");
        let (chk1, _) = cr.chunk(&meta(newest_ref)).expect("chunk");
        assert_eq!(newest_chunk_min_time, first_ts(&chk1));
        let (head_chunks_len_after, newest_min_time_after) = {
            let mut g = s.lock();
            g.mmap_chunks(&h.chunk_disk_mapper, s.r#ref);
            let newest_cid = g.first_chunk_id + g.mmapped_chunks.len() as u64 + g.head_chunks.len() as u64 - 1;
            newest_ref = new_head_chunk_ref(s.r#ref, newest_cid);
            (g.head_chunks.len(), g.head_chunks.last().unwrap().min_time)
        };
        assert!(head_chunks_len_before > 1);
        assert_eq!(1, head_chunks_len_after, "after mmap, should have exactly 1 head chunk");
        assert_eq!(newest_chunk_min_time, newest_min_time_after, "newest chunk should be the remaining head chunk");
        let (chk2, _) = cr.chunk(&meta(newest_ref)).expect("chunk");
        assert_eq!(
            newest_chunk_min_time,
            first_ts(&chk2),
            "returned chunk should be the newest head chunk, not a stale cached entry"
        );
    }
    // invalidated_after_truncation.
    {
        let (h, s, newest_ref) = new_cache_test_head("cache-trunc");
        let (newest_chunk_min_time, first_chunk_id_before) = {
            let g = s.lock();
            (g.head_chunks.last().unwrap().min_time, g.first_chunk_id)
        };
        let cr = h.chunks_range(0, 10000, None).expect("reader");
        cr.chunk(&meta(newest_ref)).expect("chunk");
        let (mmapped_len_after, first_chunk_id_after) = {
            let mut g = s.lock();
            let oldest_max_time = g.head_chunks[0].max_time;
            g.truncate_chunks_before(oldest_max_time + 1, ChunkDiskMapperRef(0));
            (g.mmapped_chunks.len(), g.first_chunk_id)
        };
        assert_eq!(0, mmapped_len_after, "mmapped-chunk count must stay 0");
        assert!(first_chunk_id_after > first_chunk_id_before, "truncation should advance firstChunkID");
        let (chk, _) = cr.chunk(&meta(newest_ref)).expect("chunk");
        assert_eq!(
            newest_chunk_min_time,
            first_ts(&chk),
            "returned chunk should be the newest head chunk, not a stale cached entry"
        );
    }
    // cache_cap_release: a series with more than 256 head chunks and a small one both resolve.
    {
        let opts = HeadOptions {
            chunk_range: 1,
            ..HeadOptions::default()
        };
        let h = new_test_head_with_options("cache-cap", compression::Type::None, opts);
        let ctx = Context::background();
        let mut app = h.appender(&ctx);
        let big = labels::from_strings(&["__name__", "big"]);
        let small = labels::from_strings(&["__name__", "small"]);
        for i in 0..(256 + 10) {
            app.append(0, &big, i, i as f64).expect("append");
        }
        for i in 0..5 {
            app.append(0, &small, i, i as f64).expect("append");
        }
        assert!(app.commit().is_none());
        let big_series = h.series.get_by_hash(big.hash(), &big).expect("big");
        let small_series = h.series.get_by_hash(small.hash(), &small).expect("small");
        let cr = h.chunks_range(0, 10000, None).expect("reader");
        let big_ref = new_head_chunk_ref(big_series.r#ref, big_series.lock().first_chunk_id);
        cr.chunk(&meta(big_ref)).expect("chunk");
        let small_ref = new_head_chunk_ref(small_series.r#ref, small_series.lock().first_chunk_id);
        cr.chunk(&meta(small_ref)).expect("chunk");
    }
    // released_on_close: a closed reader keeps no isolation state open.
    {
        let (h, _, newest_ref) = new_cache_test_head("cache-close");
        let cr = h.chunks().expect("reader");
        cr.chunk(&meta(newest_ref)).expect("chunk");
        let mut open = 0;
        h.iso.traverse_open_reads(|_| {
            open += 1;
            true
        });
        assert_eq!(1, open);
        tsdb::reader::ChunkReader::close(&cr).expect("close");
        let mut open = 0;
        h.iso.traverse_open_reads(|_| {
            open += 1;
            true
        });
        assert_eq!(0, open);
    }
    // buffer_cap_release: Series() of a series with more than 256 head chunks.
    for ooo_reader in [false, true] {
        let opts = HeadOptions {
            chunk_range: 1,
            ..HeadOptions::default()
        };
        let h = new_head_no_wal("cache-buf", opts);
        let ctx = Context::background();
        let mut app = h.appender(&ctx);
        let lbls = labels::from_strings(&["__name__", if ooo_reader { "cap_test_ooo" } else { "cap_test" }]);
        for i in 0..(256 + 10) {
            app.append(0, &lbls, i, i as f64).expect("append");
        }
        assert!(app.commit().is_none());
        let s = h.series.get_by_id(1).expect("series");
        assert!(s.lock().head_chunks.len() > 256, "need >256 head chunks");
        let maxt = 256 + 10;
        let mut builder = ScratchBuilder::new(0);
        let mut chks = Vec::new();
        if ooo_reader {
            let ir = HeadAndOOOIndexReader::new(h.head.clone(), 0, 0, maxt, ChunkDiskMapperRef(0));
            ir.series(1, &mut builder, &mut chks).expect("series");
        } else {
            let ir = h.index_range(0, maxt);
            ir.series(1, &mut builder, &mut chks).expect("series");
        }
        assert!(chks.len() > 256);
    }
}
