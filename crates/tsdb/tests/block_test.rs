//! Port of `tsdb/block_test.go`.

#![allow(clippy::needless_update, clippy::type_complexity, clippy::unnecessary_to_owned, clippy::cloned_ref_to_slice_refs)]

mod common;

use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chunkenc::{ChunkIterator, ValueType};
use common::*;
use model::labels::{self, Labels};
use model::matcher::{MatchType, Matcher};
use storage::{Context, LabelHints, LabelQuerier, SearchHints, SeriesArc};
use tsdb::block::{chunk_dir, open_block};
use tsdb::compact::{Compactor, new_leveled_compactor};
use tsdb::querier::{new_block_base_querier, new_block_querier};
use tsdb::{BlockMeta, BlockMetaCompaction, BlockReader, IndexReader, Logger, read_meta_file, write_meta_file};
use tsdb_format::index;

fn m(t: MatchType, n: &str, v: &str) -> Matcher {
    Matcher::must_new(t, n, v)
}

fn sequence_files(dir: &Path) -> Vec<PathBuf> {
    let mut res: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().parse::<u64>().is_ok())
        .map(|e| e.path())
        .collect();
    res.sort();
    res
}

fn testdata(p: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../upstream/tsdb/testdata").join(p)
}

#[test]
fn test_block_meta_compaction_set_selected_series() {
    let mut bmc = BlockMetaCompaction::default();
    // Initially absent.
    assert!(!bmc.from_selected_series());
    // Set it once.
    bmc.set_selected_series();
    assert!(bmc.from_selected_series());
    assert_eq!(bmc.hints.len(), 1);
    // Idempotent: setting again does not duplicate the hint.
    bmc.set_selected_series();
    assert!(bmc.from_selected_series());
    assert_eq!(bmc.hints.len(), 1);
    // Independent of FromStaleSeries.
    assert!(!bmc.from_stale_series());
    bmc.set_stale_series();
    assert!(bmc.from_stale_series());
    assert!(bmc.from_selected_series());
    assert_eq!(bmc.hints.len(), 2);
}

#[test]
fn test_block_meta_must_never_be_version2() {
    let dir = TempDir::new();
    write_meta_file(dir.path(), &mut BlockMeta::default()).unwrap();
    let (meta, _) = read_meta_file(dir.path()).unwrap();
    assert_ne!(2, meta.version, "meta.json version must never be 2");
}

#[test]
fn test_set_compaction_failed() {
    let tmpdir = TempDir::new();
    let block_dir = create_block(tmpdir.path(), &gen_series(1, 1, 0, 1));
    let b = open_block(None, &block_dir, None, None).unwrap();
    assert!(!b.meta().compaction.failed);
    b.set_compaction_failed().unwrap();
    assert!(b.meta().compaction.failed);
    b.close().unwrap();

    let b = open_block(None, &block_dir, None, None).unwrap();
    assert!(b.meta().compaction.failed);
    b.close().unwrap();
}

// adapted: Go checks slog's LogValue and JSON encodings of *Block. Here a block prints as its ULID (the
// directory name), and a logger shows it as `block=<ulid>`.
#[test]
fn test_block_log_value() {
    let tmp = TempDir::new();
    let block_dir = create_block(tmp.path(), &gen_series(1, 1, 0, 10));
    let block = open_block(None, &block_dir, None, None).unwrap();
    let want = block_dir.file_name().unwrap().to_string_lossy().into_owned();
    assert_eq!(want, block.to_string());

    let out = Arc::new(Mutex::new(String::new()));
    let o = out.clone();
    let logger = Logger::new(move |l| o.lock().unwrap().push_str(l));
    logger.info("test", &[("block", &block)]);
    assert!(out.lock().unwrap().contains(&format!("block={want}")));
    block.close().unwrap();
}

#[test]
fn test_create_block() {
    let tmpdir = TempDir::new();
    let b = open_block(None, &create_block(tmpdir.path(), &gen_series(1, 1, 0, 10)), None, None).unwrap();
    b.close().unwrap();
}

#[test]
fn test_corrupted_chunk() {
    type CorrFn = Box<dyn Fn(&mut std::fs::File)>;
    let cases: Vec<(&str, CorrFn, Option<&str>, Option<&str>)> = vec![
        ("invalid header size", Box::new(|f| f.set_len(1).unwrap()), Some("invalid segment header in segment 0: invalid size"), None),
        (
            "invalid magic number",
            Box::new(|f| {
                f.seek(SeekFrom::Start(0)).unwrap();
                // Set invalid magic number.
                let n = f.write(&0u32.to_be_bytes()).unwrap();
                assert_eq!(tsdb_format::chunks::MAGIC_CHUNKS_SIZE, n);
            }),
            Some("invalid magic number 0"),
            None,
        ),
        (
            "invalid chunk format version",
            Box::new(|f| {
                f.seek(SeekFrom::Start(4)).unwrap();
                // Set invalid chunk format version.
                let n = f.write(&[0u8]).unwrap();
                assert_eq!(tsdb_format::chunks::CHUNKS_FORMAT_VERSION_SIZE, n);
            }),
            Some("invalid chunk format version 0"),
            None,
        ),
        (
            "chunk not enough bytes to read the chunk length",
            Box::new(|f| f.set_len(tsdb_format::chunks::SEGMENT_HEADER_SIZE as u64 + 1).unwrap()),
            None,
            Some("cannot populate chunk 8 from block 00000000000000000000000000: segment doesn't include enough bytes to read the chunk size data field - required:13, available:9"),
        ),
        (
            "chunk not enough bytes to read the data",
            Box::new(|f| {
                let size = f.metadata().unwrap().len();
                f.set_len(size - 1).unwrap();
            }),
            None,
            Some("cannot populate chunk 8 from block 00000000000000000000000000: segment doesn't include enough bytes to read the chunk - required:25, available:24"),
        ),
        (
            "checksum mismatch",
            Box::new(|f| {
                let size = f.metadata().unwrap().len();
                // Get the chunk data end offset.
                let chk_end_offset = size - 4;
                // Seek to the last byte of chunk data and modify it.
                f.seek(SeekFrom::Start(chk_end_offset - 1)).unwrap();
                assert_eq!(1, f.write(b"x").unwrap());
            }),
            None,
            Some("cannot populate chunk 8 from block 00000000000000000000000000: checksum mismatch expected:231bddcf, actual:d85ad10d"),
        ),
    ];
    for (name, corr, open_err, iter_err) in cases {
        let tmpdir = TempDir::new();
        let series = storage::new_list_series(labels::from_strings(&["a", "b"]), vec![fs(1, 1.0)]);
        let block_dir = create_block(tmpdir.path(), &[series]);
        let files = sequence_files(&chunk_dir(&block_dir));
        assert!(!files.is_empty(), "No chunk created.");

        let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&files[0]).unwrap();
        // Apply corruption function.
        corr(&mut f);
        drop(f);

        // Check open err.
        let b = open_block(None, &block_dir, None, None);
        if let Some(oe) = open_err {
            assert_eq!(oe, b.err().expect(name).to_string(), "{name}");
            continue;
        }
        let b = b.unwrap();
        {
            let mut querier = new_block_querier(&b, 0, 1).unwrap();
            {
                let mut set = querier.select(&Context::background(), false, None, &[m(MatchType::Equal, "a", "b")]);
                // Check chunk errors during iter time.
                assert!(set.next());
                let s = set.at().clone();
                let mut it = s.iterator(None);
                assert_eq!(ValueType::None, it.next());
                assert_eq!(iter_err.unwrap(), it.err().expect(name).to_string(), "{name}");
            }
            assert!(querier.close().is_none());
        }
        b.close().unwrap();
    }
}

#[test]
fn test_label_values_with_matchers() {
    let tmpdir = TempDir::new();
    let ctx = Context::background();
    let mut series_entries: Vec<SeriesArc> = Vec::new();
    for i in 0..100 {
        series_entries.push(storage::new_list_series(
            labels::from_strings(&["tens", &format!("value{}", i / 10), "unique", &format!("value{i}")]),
            vec![fs(100, 0.0)],
        ));
    }
    let block_dir = create_block(tmpdir.path(), &series_entries);
    assert!(!sequence_files(&chunk_dir(&block_dir)).is_empty(), "No chunk created.");

    let block = open_block(None, &block_dir, None, None).unwrap();
    let index_reader = block.index().unwrap();

    let mut unique_without_30s: Vec<String> =
        (0..100).filter(|i| i / 10 != 3).map(|i| format!("value{i}")).collect();
    unique_without_30s.sort();
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    let cases: Vec<(&str, &str, Vec<Matcher>, Vec<String>)> = vec![
        ("get tens based on unique id", "tens", vec![m(MatchType::Equal, "unique", "value35")], s(&["value3"])),
        (
            "get unique ids based on a ten",
            "unique",
            vec![m(MatchType::Equal, "tens", "value1")],
            s(&["value10", "value11", "value12", "value13", "value14", "value15", "value16", "value17", "value18", "value19"]),
        ),
        (
            "get tens by pattern matching on unique id",
            "tens",
            vec![m(MatchType::Regexp, "unique", "value[5-7]5")],
            s(&["value5", "value6", "value7"]),
        ),
        (
            "get tens by matching for presence of unique label",
            "tens",
            vec![m(MatchType::NotEqual, "unique", "")],
            s(&["value0", "value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9"]),
        ),
        (
            "get unique IDs based on tens not being equal to a certain value, while not empty",
            "unique",
            vec![m(MatchType::NotEqual, "tens", "value3"), m(MatchType::NotEqual, "tens", "")],
            unique_without_30s,
        ),
    ];
    for (name, label_name, matchers, expected) in cases {
        let actual = index_reader.sorted_label_values(&ctx, label_name, None, &matchers).unwrap();
        assert_eq!(expected, actual, "{name}");
        let mut actual = index_reader.label_values(&ctx, label_name, None, &matchers).unwrap();
        actual.sort();
        assert_eq!(expected, actual, "{name}");
    }
    index_reader.close().unwrap();
    drop(index_reader);
    block.close().unwrap();
}

#[test]
fn test_block_querier_returns_sorted_label_values() {
    let tmpdir = TempDir::new();
    let ctx = Context::background();
    let mut series_entries: Vec<SeriesArc> = Vec::new();
    for i in (1..=100).rev() {
        series_entries.push(storage::new_list_series(
            labels::from_strings(&["__name__", &format!("value{i}")]),
            vec![fs(100, 0.0)],
        ));
    }
    let block_dir = create_block(tmpdir.path(), &series_entries);
    let block = open_block(None, &block_dir, None, None).unwrap();
    let mut q = new_block_base_querier(&block, 0, 100).unwrap();
    let (res, _, err) = q.label_values(&ctx, "__name__", None, &[]);
    assert!(err.is_none());
    assert!(res.is_sorted());
    assert!(q.close().is_none());
    drop(q);
    block.close().unwrap();
}

#[test]
fn test_block_size() {
    let tmpdir = TempDir::new();
    // Create a block and compare the reported size vs actual disk size.
    let block_dir_init = create_block(tmpdir.path(), &gen_series(10, 1, 1, 100));
    let block_init = open_block(None, &block_dir_init, None, None).unwrap();
    let exp_size_init = block_init.size();
    let act_size_init = tsdb_format::fileutil::dir_size(block_init.dir()).unwrap();
    assert_eq!(exp_size_init, act_size_init);

    // Delete some series and check the sizes again.
    block_init.delete(&Context::background(), 1, 10, &[m(MatchType::Regexp, "", ".*")]).unwrap();
    let exp_after_delete = block_init.size();
    assert!(
        exp_after_delete > exp_size_init,
        "after a delete the block size should be bigger as the tombstone file should grow {exp_after_delete} > {exp_size_init}"
    );
    let act_after_delete = tsdb_format::fileutil::dir_size(&block_dir_init).unwrap();
    assert_eq!(exp_after_delete, act_after_delete, "after a delete reported block size doesn't match actual disk size");

    let c = new_leveled_compactor(&Context::background(), None, vec![0], None, None).unwrap();
    let after = c.compact(tmpdir.path(), &[block_init.dir().to_path_buf()], &[]).unwrap();
    assert_eq!(after.len(), 1);
    let block_after_compact = open_block(None, &tmpdir.path().join(after[0].to_string()), None, None).unwrap();
    let exp_after_compact = block_after_compact.size();
    let act_after_compact = tsdb_format::fileutil::dir_size(block_after_compact.dir()).unwrap();
    assert!(
        act_after_delete > act_after_compact,
        "after a delete and compaction the block size should be smaller {act_after_delete},{act_after_compact}"
    );
    assert_eq!(exp_after_compact, act_after_compact, "after a delete and compaction reported block size doesn't match actual disk size");
    block_after_compact.close().unwrap();
    block_init.close().unwrap();
}

fn sample_map(v: &[(&str, Vec<storage::Sample>)]) -> std::collections::BTreeMap<String, Vec<storage::Sample>> {
    v.iter().map(|(k, s)| (k.to_string(), s.clone())).collect()
}

#[test]
fn test_read_index_format_v1() {
    // The block here was produced at the commit 706602daed1487f7849990678b4ece4599745905 used in 2.0.0.
    let block = open_block(None, &testdata("index_format_v1"), None, None).unwrap();

    let q = new_block_querier(&block, 0, 1000).unwrap();
    assert_eq!(sample_map(&[(r#"{foo="bar"}"#, vec![fs(1, 2.0)])]), query(q, &[m(MatchType::Equal, "foo", "bar")]));

    let q = new_block_querier(&block, 0, 1000).unwrap();
    assert_eq!(
        sample_map(&[(r#"{foo="bar"}"#, vec![fs(1, 2.0)]), (r#"{foo="baz"}"#, vec![fs(3, 4.0)])]),
        query(q, &[m(MatchType::NotRegexp, "foo", "^.?$")])
    );
    block.close().unwrap();
}

#[test]
fn test_search_label_values_index_format_v1() {
    let ctx = Context::background();
    let block = open_block(None, &testdata("index_format_v1"), None, None).unwrap();
    assert_eq!(index::FORMAT_V1 as i64, block.meta().version, "fixture must exercise the FormatV1 branch");

    // The fixture holds "bar" values "0" to "99", so the lexically smallest are not the numerically smallest.
    let want_smallest: Vec<String> = ["0", "1", "10", "11", "12"].iter().map(|s| s.to_string()).collect();

    // Index reader keeps the smallest values.
    {
        let ir = block.index().unwrap();
        let all = ir.label_values(&ctx, "bar", None, &[]).unwrap();
        assert_eq!(all.len(), 100);
        assert!(!all.is_sorted(), "precondition: the map read is unordered");
        let got = ir.label_values(&ctx, "bar", Some(&LabelHints { limit: 5, limit_smallest: true }), &[]).unwrap();
        assert_eq!(want_smallest, got);
        ir.close().unwrap();
    }
    // Legacy limit still takes any subset.
    {
        let ir = block.index().unwrap();
        let got = ir.label_values(&ctx, "bar", Some(&LabelHints { limit: 5, limit_smallest: false }), &[]).unwrap();
        assert_eq!(got.len(), 5);
        ir.close().unwrap();
    }
    // Search returns the smallest values.
    {
        let mut q = new_block_querier(&block, 0, 1000).unwrap();
        {
            let mut rs = q.as_searcher().unwrap().search_label_values(
                &ctx,
                "bar",
                Some(&SearchHints { limit: 5, ..Default::default() }),
                &[],
            );
            let mut got = Vec::new();
            while rs.next() {
                got.push(rs.at().value.clone());
            }
            assert!(rs.err().is_none());
            assert_eq!(want_smallest, got);
            assert!(rs.close().is_none());
        }
        assert!(q.close().is_none());
    }
    block.close().unwrap();
}

#[test]
fn test_label_names_with_matchers() {
    let tmpdir = TempDir::new();
    let ctx = Context::background();
    let mut series_entries: Vec<SeriesArc> = Vec::new();
    let one = || vec![fs(100, 0.0)];
    for i in 0..100 {
        series_entries.push(storage::new_list_series(labels::from_strings(&["unique", &format!("value{i}")]), one()));
        if i % 10 == 0 {
            series_entries.push(storage::new_list_series(
                labels::from_strings(&["tens", &format!("value{}", i / 10), "unique", &format!("value{i}")]),
                one(),
            ));
        }
        if i % 20 == 0 {
            series_entries.push(storage::new_list_series(
                labels::from_strings(&[
                    "tens",
                    &format!("value{}", i / 10),
                    "twenties",
                    &format!("value{}", i / 20),
                    "unique",
                    &format!("value{i}"),
                ]),
                one(),
            ));
        }
    }
    let block_dir = create_block(tmpdir.path(), &series_entries);
    assert!(!sequence_files(&chunk_dir(&block_dir)).is_empty(), "No chunk created.");
    let block = open_block(None, &block_dir, None, None).unwrap();
    let index_reader = block.index().unwrap();

    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    let cases = vec![
        ("get with non-empty unique: all", vec![m(MatchType::NotEqual, "unique", "")], s(&["tens", "twenties", "unique"])),
        ("get with unique ending in 1: only unique", vec![m(MatchType::Regexp, "unique", "value.*1")], s(&["unique"])),
        ("get with unique = value20: all", vec![m(MatchType::Equal, "unique", "value20")], s(&["tens", "twenties", "unique"])),
        ("get tens = 1: unique & tens", vec![m(MatchType::Equal, "tens", "value1")], s(&["tens", "unique"])),
    ];
    for (name, matchers, expected) in cases {
        let actual = index_reader.label_names(&ctx, &matchers).unwrap();
        assert_eq!(expected, actual, "{name}");
    }
    index_reader.close().unwrap();
    drop(index_reader);
    block.close().unwrap();
}

#[test]
fn test_block_index_reader_postings_for_label_matching() {
    let tmp = TempDir::new();
    let series: Vec<Labels> = vec![
        labels::from_strings(&["n", "1"]),
        labels::from_strings(&["n", "1", "i", "a"]),
        labels::from_strings(&["n", "1", "i", "b"]),
        labels::from_strings(&["n", "2"]),
        labels::from_strings(&["n", "2.5"]),
    ];
    let entries: Vec<SeriesArc> =
        series.iter().map(|s| storage::new_list_series(s.clone(), vec![fs(100, 0.0)])).collect();
    let block_dir = create_block(tmp.path(), &entries);
    assert!(!sequence_files(&chunk_dir(&block_dir)).is_empty(), "No chunk created.");
    let block = open_block(None, &block_dir, None, None).unwrap();
    let ir = block.index().unwrap();
    test_postings_for_label_matching(&*ir, 2);
    ir.close().unwrap();
    drop(ir);
    block.close().unwrap();
}

fn test_postings_for_label_matching(ir: &dyn IndexReader, offset: u64) {
    let ctx = Context::background();
    let cases: Vec<(&str, &str, Box<dyn Fn(&str) -> bool + Sync>, Vec<u64>)> = vec![
        ("n=1", "n", Box::new(|v| v == "1"), vec![offset + 1, offset + 2, offset + 3]),
        ("n=2", "n", Box::new(|v| v == "2"), vec![offset + 4]),
        ("missing label", "missing", Box::new(|_| true), vec![]),
    ];
    for (name, label_name, matchf, exp) in cases {
        let mut p = ir.postings_for_label_matching(&ctx, label_name, &*matchf);
        let srs = index::expand_postings(&mut *p).unwrap();
        assert_eq!(exp, srs, "{name}");
    }
}

#[allow(unused)]
fn _unused(_: &BlockMeta) {}
