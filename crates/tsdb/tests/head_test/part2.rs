//! Port of `tsdb/head_test.go` lines 3806..7602.
//!
//! Tests that need the DB (`tsdb.Open`, `db.Compact`, `db.Querier`) wait for the DB port; see the list at the end.

use std::sync::Arc;

use chunkenc::{ChunkIterator, ValueType};
use model::exemplar::Exemplar;
use model::labels::{self, Labels, ScratchBuilder};
use model::matcher::{MatchType, Matcher};
use storage::{Appender, Context, SeriesRef};
use tsdb::head::{MemSeries, DEFAULT_SAMPLES_PER_CHUNK};
use tsdb::head_append::ChunkOpts;
use tsdb::reader::IndexReader;
use tsdb_format::chunks::{ChunkDiskMapper, DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_QUEUE_SIZE};
use tsdb_format::index::{self, BoxPostings};
use tsdb_format::wlog::compression;

use crate::common::{new_test_head, new_test_head_with_options, TempDir};

fn bg() -> Context {
    Context::background()
}

fn ls(kv: &[&str]) -> Labels {
    labels::from_strings(kv)
}

fn m(t: MatchType, n: &str, v: &str) -> Matcher {
    Matcher::must_new(t, n, v)
}

fn commit(app: &mut Box<dyn Appender>) {
    if let Some(e) = app.commit() {
        panic!("commit: {e}");
    }
}

#[test]
fn test_head_label_names_values_with_min_max_range() {
    let head = new_test_head("label-names-values", 1000, compression::Type::None, false);

    let first_series_timestamp: i64 = 100;
    let second_series_timestamp: i64 = 200;
    let last_series_timestamp: i64 = 300;
    let series_timestamps = [first_series_timestamp, second_series_timestamp, last_series_timestamp];
    let expected_label_names = ["a", "b", "c"];
    let expected_label_values = ["d", "e", "f"];
    let ctx = bg();

    let mut app = head.appender(&ctx);
    for (i, name) in expected_label_names.iter().enumerate() {
        app.append(0, &ls(&[name, expected_label_values[i]]), series_timestamps[i], 0.0).unwrap();
    }
    commit(&mut app);
    assert_eq!(first_series_timestamp, head.min_time());
    assert_eq!(last_series_timestamp, head.max_time());

    let names: Vec<String> = expected_label_names.iter().map(|s| s.to_string()).collect();
    let values: Vec<String> = expected_label_values.iter().map(|s| s.to_string()).collect();
    let cases: Vec<(&str, i64, i64, Vec<String>, Vec<String>)> = vec![
        ("maxt less than head min", head.max_time() - 10, head.min_time() - 10, vec![], vec![]),
        ("mint less than head max", head.max_time() + 10, head.min_time() + 10, vec![], vec![]),
        ("mint and maxt outside head", head.max_time() + 10, head.min_time() - 10, vec![], vec![]),
        ("mint and maxt within head", head.max_time() - 10, head.min_time() + 10, names.clone(), values.clone()),
    ];
    for (name, mint, maxt, expected_names, expected_values) in cases {
        let ir = head.index_range(mint, maxt);
        let actual_label_names = ir.label_names(&ctx, &[]).unwrap();
        assert_eq!(expected_names, actual_label_names, "{name}");
        if !expected_values.is_empty() {
            for (i, name) in expected_label_names.iter().enumerate() {
                let actual = ir.sorted_label_values(&ctx, name, None, &[]).unwrap();
                assert_eq!(vec![expected_values[i].clone()], actual, "{name}");
            }
        }
    }
}

#[test]
fn test_head_label_values_with_matchers() {
    let head = new_test_head("label-values-matchers", 1000, compression::Type::None, false);
    let ctx = bg();

    let mut app = head.appender(&ctx);
    for i in 0..100 {
        app.append(0, &ls(&["tens", &format!("value{}", i / 10), "unique", &format!("value{i}")]), 100, 0.0)
            .unwrap();
    }
    commit(&mut app);

    let mut unique_without30s: Vec<String> =
        (0..100).filter(|i| i / 10 != 3).map(|i| format!("value{i}")).collect();
    unique_without30s.sort();
    let sv = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let cases: Vec<(&str, &str, Vec<Matcher>, Vec<String>)> = vec![
        (
            "get tens based on unique id",
            "tens",
            vec![m(MatchType::Equal, "unique", "value35")],
            sv(&["value3"]),
        ),
        (
            "get unique ids based on a ten",
            "unique",
            vec![m(MatchType::Equal, "tens", "value1")],
            sv(&[
                "value10", "value11", "value12", "value13", "value14", "value15", "value16", "value17", "value18",
                "value19",
            ]),
        ),
        (
            "get tens by pattern matching on unique id",
            "tens",
            vec![m(MatchType::Regexp, "unique", "value[5-7]5")],
            sv(&["value5", "value6", "value7"]),
        ),
        (
            "get tens by matching for presence of unique label",
            "tens",
            vec![m(MatchType::NotEqual, "unique", "")],
            sv(&["value0", "value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9"]),
        ),
        (
            "get unique IDs based on tens not being equal to a certain value, while not empty",
            "unique",
            vec![m(MatchType::NotEqual, "tens", "value3"), m(MatchType::NotEqual, "tens", "")],
            unique_without30s,
        ),
    ];
    for (name, label_name, matchers, expected) in cases {
        let ir = head.index_range(0, 200);
        let actual = ir.sorted_label_values(&ctx, label_name, None, &matchers).unwrap();
        assert_eq!(expected, actual, "{name}");
        let mut actual = ir.label_values(&ctx, label_name, None, &matchers).unwrap();
        actual.sort();
        assert_eq!(expected, actual, "{name}");
    }
}

#[test]
fn test_head_label_names_with_matchers() {
    let head = new_test_head("label-names-matchers", 1000, compression::Type::None, false);
    let mut app = head.appender(&bg());
    for i in 0..100 {
        app.append(0, &ls(&["unique", &format!("value{i}")]), 100, 0.0).unwrap();
        if i % 10 == 0 {
            app.append(0, &ls(&["tens", &format!("value{}", i / 10), "unique", &format!("value{i}")]), 100, 0.0)
                .unwrap();
        }
        if i % 20 == 0 {
            app.append(
                0,
                &ls(&[
                    "tens",
                    &format!("value{}", i / 10),
                    "twenties",
                    &format!("value{}", i / 20),
                    "unique",
                    &format!("value{i}"),
                ]),
                100,
                0.0,
            )
            .unwrap();
        }
    }
    commit(&mut app);

    let cases: Vec<(&str, Vec<Matcher>, Vec<&str>)> = vec![
        ("get with non-empty unique: all", vec![m(MatchType::NotEqual, "unique", "")], vec!["tens", "twenties", "unique"]),
        ("get with unique ending in 1: only unique", vec![m(MatchType::Regexp, "unique", "value.*1")], vec!["unique"]),
        ("get with unique = value20: all", vec![m(MatchType::Equal, "unique", "value20")], vec!["tens", "twenties", "unique"]),
        ("get tens = 1: unique & tens", vec![m(MatchType::Equal, "tens", "value1")], vec!["tens", "unique"]),
    ];
    for (name, matchers, expected) in cases {
        let ir = head.index_range(0, 200);
        let actual = ir.label_names(&bg(), &matchers).unwrap();
        assert_eq!(expected, actual, "{name}");
    }
}

#[test]
fn test_head_sharded_postings() {
    let opts = tsdb::head::HeadOptions { chunk_range: 1000, enable_sharding: true, ..Default::default() };
    // Go newTestHeadDefaultOptions(1000, false): exemplars enabled with 10 max exemplars.
    let opts = tsdb::head::HeadOptions { enable_exemplar_storage: true, ..opts };
    opts.max_exemplars.store(10, std::sync::atomic::Ordering::Relaxed);
    let head = new_test_head_with_options("sharded-postings", compression::Type::None, opts);
    let ctx = bg();

    // Append some series.
    let mut app = head.appender(&ctx);
    for i in 0..100 {
        app.append(0, &ls(&["unique", &format!("value{i}"), "const", "1"]), 100, 0.0).unwrap();
    }
    commit(&mut app);

    let ir = head.index_range(0, 200);

    // List all postings for a given label value. This is what we expect to get in output from all shards.
    let mut p = ir.postings(&ctx, "const", &["1"]).unwrap();
    let mut expected: Vec<SeriesRef> = Vec::new();
    while p.next() {
        expected.push(p.at());
    }
    assert!(p.err().is_none());
    assert!(!expected.is_empty());
    drop(p);

    // Query the same postings for each shard.
    let shard_count = 4u64;
    let mut actual_shards: std::collections::HashMap<u64, Vec<SeriesRef>> = Default::default();
    let mut actual_postings: Vec<SeriesRef> = Vec::with_capacity(expected.len());
    for shard_index in 0..shard_count {
        let p = ir.postings(&ctx, "const", &["1"]).unwrap();
        let mut p = ir.sharded_postings(p, shard_index, shard_count);
        while p.next() {
            let r = p.at();
            actual_shards.entry(shard_index).or_default().push(r);
            actual_postings.push(r);
        }
        assert!(p.err().is_none());
    }

    // We expect the postings merged out of shards is the exact same of the non sharded ones.
    let mut a = expected.clone();
    a.sort();
    let mut b = actual_postings.clone();
    b.sort();
    assert_eq!(a, b);

    // We expect the series in each shard are the expected ones.
    for (shard_index, ids) in &actual_shards {
        for id in ids {
            let mut lbls = ScratchBuilder::default();
            let mut chks = Vec::new();
            ir.series(*id, &mut lbls, &mut chks).unwrap();
            assert_eq!(*shard_index, labels::stable_hash(&lbls.labels()) % shard_count);
        }
    }

    let err_expected = index::Error::new("postings error");
    for (name, refs) in [("empty", Vec::new()), ("non-empty", expected.clone())] {
        let ps: Vec<BoxPostings<'_>> =
            vec![Box::new(index::new_list_postings(refs)), index::err_postings(err_expected.clone())];
        let p = index::merge(ps);
        let mut p = ir.sharded_postings(p, 0, shard_count);
        assert!(!p.next(), "{name}");
        assert!(p.err().is_some_and(|e| e.is(&err_expected)), "{name}");
    }
}

#[test]
fn test_err_reuse_appender() {
    let head = new_test_head("err-reuse-appender", 1000, compression::Type::None, false);
    let l = ls(&["test", "test"]);

    let mut app = head.appender(&bg());
    app.append(0, &l, 0, 0.0).unwrap();
    assert!(app.commit().is_none());
    assert!(app.commit().is_some());
    assert!(app.rollback().is_some());

    let mut app = head.appender(&bg());
    app.append(0, &l, 1, 0.0).unwrap();
    assert!(app.rollback().is_none());
    assert!(app.rollback().is_some());
    assert!(app.commit().is_some());

    let mut app = head.appender(&bg());
    app.append(0, &l, 2, 0.0).unwrap();
    assert!(app.commit().is_none());
    assert!(app.rollback().is_some());
    assert!(app.commit().is_some());

    let mut app = head.appender(&bg());
    app.append(0, &l, 3, 0.0).unwrap();
    assert!(app.rollback().is_none());
    assert!(app.commit().is_some());
    assert!(app.rollback().is_some());
}

#[test]
fn test_head_mint_after_truncation() {
    let chunk_range = 2000;
    let head = new_test_head("mint-after-truncation", chunk_range, compression::Type::None, false);

    let mut app = head.appender(&bg());
    app.append(0, &ls(&["a", "b"]), 100, 100.0).unwrap();
    app.append(0, &ls(&["a", "b"]), 4000, 200.0).unwrap();
    app.append(0, &ls(&["a", "b"]), 8000, 300.0).unwrap();
    commit(&mut app);

    // Truncating outside the appendable window and actual mint being outside appendable window should leave mint
    // at the actual mint.
    head.truncate(3500).unwrap();
    assert_eq!(4000, head.min_time());
    assert_eq!(4000, head.min_valid_time());

    // After truncation outside the appendable window if the actual min time is in the appendable window then we
    // should leave mint at the start of appendable window.
    head.truncate(5000).unwrap();
    assert_eq!(head.appendable_min_valid_time(), head.min_time());
    assert_eq!(head.appendable_min_valid_time(), head.min_valid_time());

    // If the truncation time is inside the appendable window, then the min time should be the truncation time.
    head.truncate(7500).unwrap();
    assert_eq!(7500, head.min_time());
    assert_eq!(7500, head.min_valid_time());
}

#[test]
fn test_head_exemplars() {
    let chunk_range = 2000;
    let head = new_test_head("head-exemplars", chunk_range, compression::Type::None, false);
    let mut app = head.appender(&bg());

    let l = ls(&["trace_id", "123"]);
    // It is perfectly valid to add Exemplars before the current start time - histogram buckets that haven't been
    // update in a while could still be exported exemplars from an hour ago.
    let r = app.append(0, &ls(&["a", "b"]), 100, 100.0).unwrap();
    app.append_exemplar(r, &l, &Exemplar { labels: l.clone(), has_ts: true, ts: -1000, value: 1.0 }).unwrap();
    commit(&mut app);
}

#[test]
fn test_iterator_seek_into_buffer() {
    let dir = TempDir::new();
    // This is usually taken from the Head, but passing manually here.
    let cdm = ChunkDiskMapper::new(dir.path(), None, DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_QUEUE_SIZE).unwrap();
    let c_opts = ChunkOpts {
        chunk_disk_mapper: Some(&cdm),
        chunk_range: 500,
        samples_per_chunk: DEFAULT_SAMPLES_PER_CHUNK,
        ..Default::default()
    };

    let s = MemSeries::new(Labels::default(), 1, 0, false, false);
    let mut g = s.lock();
    for i in 0..7 {
        let (ok, _) = g.append(0, i, i as f64, 0, &c_opts, &s.lset, s.r#ref);
        assert!(ok, "sample append failed");
    }

    let (c, _, _) = g.chunk(0, &cdm).unwrap();
    let mut it = c.chunk.iterator(None);

    // First point.
    assert_eq!(ValueType::Float, it.seek(0));
    assert_eq!((0, 0.0), it.at());

    // Advance one point.
    assert_eq!(ValueType::Float, it.next());
    assert_eq!((1, 1.0), it.at());

    // Seeking an older timestamp shouldn't cause the iterator to go backwards.
    assert_eq!(ValueType::Float, it.seek(0));
    assert_eq!((1, 1.0), it.at());

    // Seek into the buffer.
    assert_eq!(ValueType::Float, it.seek(3));
    assert_eq!((3, 3.0), it.at());

    // Iterate through the rest of the buffer.
    for i in 4..7 {
        assert_eq!(ValueType::Float, it.next());
        assert_eq!((i, i as f64), it.at());
    }

    // Run out of elements in the iterator.
    assert_eq!(ValueType::None, it.next());
    assert_eq!(ValueType::None, it.seek(7));
    drop(it);
    drop(g);
    cdm.close().unwrap();
}

#[allow(dead_code)]
fn _unused(_: Arc<()>) {}
