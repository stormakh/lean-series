//! Port of `tsdb/head_test.go` lines 3806..7602.
//!
//! The tests that need the DB are at the end.

#![allow(clippy::type_complexity, clippy::let_and_return)]

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;

use chunkenc::{Chunk, ChunkIterator, CounterResetHeader, ValueType};
use model::exemplar::Exemplar;
use model::histogram::{CounterResetHint, FloatHistogram, Histogram};
use model::labels::{self, Labels, ScratchBuilder};
use model::matcher::{MatchType, Matcher};
use storage::tsdbutil as tu;
use storage::{Appender, Context, Error, Sample, SeriesRef};
use tsdb::head::{Head, HeadOptions, HeadSeriesRef, MemSeries, SeriesLifecycleCallback, DEFAULT_SAMPLES_PER_CHUNK};
use tsdb::head_append::ChunkOpts;
use tsdb::querier::new_block_querier;
use tsdb::reader::IndexReader;
use tsdb_format::chunks::{ChunkDiskMapper, DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_QUEUE_SIZE};
use tsdb_format::index::{self, BoxPostings};
use tsdb_format::tombstones::{Interval, Intervals, IntervalsExt, Stone};
use tsdb_format::wlog::{self, compression, WL};

use crate::common::{new_test_head, new_test_head_with_options, query, TempDir, TestHead};

fn bg() -> Context {
    Context::background()
}

/// Go `head.opts` shared by pointer between restarts: a copy of the options (atomics are read now).
fn clone_opts(o: &HeadOptions) -> HeadOptions {
    HeadOptions {
        max_exemplars: AtomicI64::new(o.max_exemplars.load(Ordering::SeqCst)),
        out_of_order_time_window: AtomicI64::new(o.out_of_order_time_window.load(Ordering::SeqCst)),
        out_of_order_cap_max: AtomicI64::new(o.out_of_order_cap_max.load(Ordering::SeqCst)),
        enable_st_storage: AtomicBool::new(o.enable_st_storage.load(Ordering::SeqCst)),
        float_chunk_encoding: AtomicU32::new(o.float_chunk_encoding.load(Ordering::SeqCst)),
        enable_histogram_st_encoding: AtomicBool::new(o.enable_histogram_st_encoding.load(Ordering::SeqCst)),
        chunk_range: o.chunk_range,
        chunk_dir_root: o.chunk_dir_root.clone(),
        chunk_pool: o.chunk_pool.clone(),
        chunk_write_buffer_size: o.chunk_write_buffer_size,
        chunk_write_queue_size: o.chunk_write_queue_size,
        samples_per_chunk: o.samples_per_chunk,
        stripe_size: o.stripe_size,
        series_callback: o.series_callback.clone(),
        enable_exemplar_storage: o.enable_exemplar_storage,
        enable_memory_snapshot_on_shutdown: AtomicBool::new(o.enable_memory_snapshot_on_shutdown.load(Ordering::SeqCst)),
        isolation_disabled: o.isolation_disabled,
        wal_replay_concurrency: o.wal_replay_concurrency,
        enable_sharding: o.enable_sharding,
        enable_st_as_zero_sample: o.enable_st_as_zero_sample,
        enable_metadata_wal_records: o.enable_metadata_wal_records,
        enable_fast_startup: o.enable_fast_startup,
        logger: o.logger.clone(),
    }
}

/// Go `newTestHeadDefaultOptions`.
fn default_test_opts(chunk_range: i64, ooo_enabled: bool) -> HeadOptions {
    let opts = HeadOptions { chunk_range, enable_exemplar_storage: true, ..Default::default() };
    opts.max_exemplars.store(10, Ordering::Relaxed);
    if ooo_enabled {
        opts.out_of_order_time_window.store(10 * 60 * 1000, Ordering::Relaxed);
    }
    opts
}

/// Go: `w, _ := wlog.NewSize(nil, nil, walDir, 32768, compression.None); head, _ = NewHead(nil, nil, w, nil,
/// opts, nil)`, followed by `head.Init(min_valid_time)` when given. The old head must be closed.
fn reopen(th: &mut TestHead, opts: HeadOptions, init: Option<i64>) -> Result<(), Error> {
    let wal = Arc::new(WL::new_size(th.dir.path().join("wal"), 32768, compression::Type::None).expect("wal"));
    th.head = Head::new(Some(wal.clone()), None, opts, None)?;
    th.wal = Some(wal);
    match init {
        Some(t) => th.head.init(t),
        None => Ok(()),
    }
}

/// Reopens the head with a copy of its own options and initializes it (requires success).
fn restart(th: &mut TestHead, min_valid_time: i64) {
    let opts = clone_opts(&th.head.opts);
    reopen(th, opts, Some(min_valid_time)).expect("restart head");
}

fn get_series(head: &Head, l: &Labels) -> Option<Arc<MemSeries>> {
    head.series.get_by_hash(l.hash(), l)
}

fn query_head(head: &Head, mint: i64, maxt: i64, ms: &[Matcher]) -> BTreeMap<String, Vec<Sample>> {
    query(new_block_querier(head, mint, maxt).expect("querier"), ms)
}

/// Go `compareSeries` (db_test.go).
fn compare_series(expected: &BTreeMap<String, Vec<Sample>>, actual: &BTreeMap<String, Vec<Sample>>) {
    assert_eq!(expected.len(), actual.len(), "number of series differs");
    for (key, exp_samples) in expected {
        let act_samples = actual.get(key).unwrap_or_else(|| panic!("expected series {key:?} not found"));
        assert_eq!(exp_samples.len(), act_samples.len(), "number of samples for series {key:?} differs");
        for (i, (e, a)) in exp_samples.iter().zip(act_samples).enumerate() {
            assert_eq!(e.t, a.t, "timestamp of sample {i} in series {key:?} differs");
            assert_eq!(e.typ(), a.typ(), "type of sample {i} in series {key:?} differs");
            match e.typ() {
                ValueType::Float => assert_eq!(e.f, a.f, "sample {i} in series {key:?} differs"),
                ValueType::Histogram => {
                    let mut eh = e.h.as_deref().unwrap().clone();
                    let ah = a.h.as_deref().unwrap();
                    if ah.counter_reset_hint == CounterResetHint::UnknownCounterReset {
                        // It is always safe to set the counter reset hint to UnknownCounterReset.
                        eh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                    }
                    assert_eq!(&eh, ah, "histogram sample {i} in series {key:?} differs");
                }
                ValueType::FloatHistogram => {
                    let mut efh = e.fh.as_deref().unwrap().clone();
                    let afh = a.fh.as_deref().unwrap();
                    if afh.counter_reset_hint == CounterResetHint::UnknownCounterReset {
                        efh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                    }
                    assert_eq!(&efh, afh, "float histogram sample {i} in series {key:?} differs");
                }
                ValueType::None => {}
            }
        }
    }
}

/// The counter reset header of a histogram chunk; panics unless the chunk has the expected Go type
/// (`chk.(*chunkenc.HistogramChunk)` and its float and start-timestamp variants).
fn counter_reset_header(c: &dyn Chunk, float: bool, st: bool) -> CounterResetHeader {
    let a = c.as_any();
    match (float, st) {
        (false, false) => a.downcast_ref::<chunkenc::HistogramChunk>().expect("HistogramChunk").get_counter_reset_header(),
        (false, true) => a.downcast_ref::<chunkenc::HistogramStChunk>().expect("HistogramSTChunk").get_counter_reset_header(),
        (true, false) => {
            a.downcast_ref::<chunkenc::FloatHistogramChunk>().expect("FloatHistogramChunk").get_counter_reset_header()
        }
        (true, true) => a
            .downcast_ref::<chunkenc::FloatHistogramStChunk>()
            .expect("FloatHistogramSTChunk")
            .get_counter_reset_header(),
    }
}

/// Go `countSeriesLifecycleCallback`.
#[derive(Default)]
struct CountSeriesLifecycleCallback {
    created: AtomicI64,
    deleted: AtomicI64,
}

impl SeriesLifecycleCallback for CountSeriesLifecycleCallback {
    fn pre_creation(&self, _: &Labels) -> Result<(), Error> {
        Ok(())
    }
    fn post_creation(&self, _: &Labels) {
        self.created.fetch_add(1, Ordering::SeqCst);
    }
    fn post_deletion(&self, s: &HashMap<HeadSeriesRef, Labels>) {
        self.deleted.fetch_add(s.len() as i64, Ordering::SeqCst);
    }
}

fn rand_u63() -> u64 {
    let mut b = [0u8; 8];
    crypto::random_bytes(&mut b);
    u64::from_le_bytes(b) >> 1
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

fn commit_ok(app: &mut Box<dyn Appender>) {
    if let Some(e) = app.commit() {
        panic!("commit: {e}");
    }
}

fn h_sample(t: i64, h: Histogram) -> Sample {
    Sample::histogram(0, t, h)
}

fn fh_sample(t: i64, fh: FloatHistogram) -> Sample {
    Sample::float_histogram(0, t, fh)
}

#[test]
fn test_append_histogram() {
    let l = ls(&["a", "b"]);
    for num_histograms in [1usize, 10, 150, 200, 250, 300] {
        let head = new_test_head("append-histogram", 1000, compression::Type::None, false);
        head.init(0).unwrap();
        let mut ingest_ts = 0i64;
        let mut app = head.appender(&bg());

        let mut exp_histograms = Vec::with_capacity(2 * num_histograms);
        // Counter integer histograms, then gauge integer histograms.
        for hs in [tu::generate_test_histograms(num_histograms), tu::generate_test_gauge_histograms(num_histograms)] {
            for h in hs {
                app.append_histogram(0, &l, ingest_ts, Some(&h), None).unwrap();
                exp_histograms.push(h_sample(ingest_ts, h));
                ingest_ts += 1;
                if ingest_ts % 50 == 0 {
                    commit_ok(&mut app);
                    app = head.appender(&bg());
                }
            }
        }

        let mut exp_float_histograms = Vec::with_capacity(2 * num_histograms);
        // Counter float histograms, then gauge float histograms.
        for fhs in
            [tu::generate_test_float_histograms(num_histograms), tu::generate_test_gauge_float_histograms(num_histograms)]
        {
            for fh in fhs {
                app.append_histogram(0, &l, ingest_ts, None, Some(&fh)).unwrap();
                exp_float_histograms.push(fh_sample(ingest_ts, fh));
                ingest_ts += 1;
                if ingest_ts % 50 == 0 {
                    commit_ok(&mut app);
                    app = head.appender(&bg());
                }
            }
        }
        commit_ok(&mut app);

        let mut q = new_block_querier(&**head, head.min_time(), head.max_time()).unwrap();
        let mut act_histograms = Vec::new();
        let mut act_float_histograms = Vec::new();
        {
            let mut ss = q.select(&bg(), false, None, &[m(MatchType::Equal, "a", "b")]);
            assert!(ss.next());
            let s = ss.at().clone();
            assert!(!ss.next());
            let mut it = s.iterator(None);
            loop {
                match it.next() {
                    ValueType::None => break,
                    ValueType::Histogram => {
                        let (ts, h) = it.at_histogram(None);
                        act_histograms.push(h_sample(ts, h));
                    }
                    ValueType::FloatHistogram => {
                        let (ts, fh) = it.at_float_histogram(None);
                        act_float_histograms.push(fh_sample(ts, fh));
                    }
                    ValueType::Float => {}
                }
            }
        }
        assert!(q.close().is_none());

        let wrap = |v: Vec<Sample>| BTreeMap::from([("dummy".to_string(), v)]);
        compare_series(&wrap(exp_histograms), &wrap(act_histograms));
        compare_series(&wrap(exp_float_histograms), &wrap(act_float_histograms));
    }
}

#[test]
fn test_histogram_in_wal_and_mmap_chunk() {
    let mut head = new_test_head("histogram-wal-mmap", 3000, compression::Type::None, false);
    head.init(0).unwrap();

    // Series with only histograms.
    let s1 = ls(&["a", "b1"]);
    let k1 = s1.to_string();
    let num_histograms = 300;
    let mut exp: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
    let mut ts = 0i64;
    for gauge in [true, false] {
        let mut app = head.appender(&bg());
        let hists =
            if gauge { tu::generate_test_gauge_histograms(num_histograms) } else { tu::generate_test_histograms(num_histograms) };
        for mut h in hists {
            h.negative_spans = h.positive_spans.clone();
            h.negative_buckets = h.positive_buckets.clone();
            app.append_histogram(0, &s1, ts, Some(&h), None).unwrap();
            exp.entry(k1.clone()).or_default().push(h_sample(ts, h));
            ts += 1;
            if ts % 5 == 0 {
                commit_ok(&mut app);
                app = head.appender(&bg());
            }
        }
        commit_ok(&mut app);
    }
    for gauge in [true, false] {
        let mut app = head.appender(&bg());
        let hists = if gauge {
            tu::generate_test_gauge_float_histograms(num_histograms)
        } else {
            tu::generate_test_float_histograms(num_histograms)
        };
        for mut h in hists {
            h.negative_spans = h.positive_spans.clone();
            h.negative_buckets = h.positive_buckets.clone();
            app.append_histogram(0, &s1, ts, None, Some(&h)).unwrap();
            exp.entry(k1.clone()).or_default().push(fh_sample(ts, h));
            ts += 1;
            if ts % 5 == 0 {
                commit_ok(&mut app);
                app = head.appender(&bg());
            }
        }
        commit_ok(&mut app);
        head.mmap_head_chunks();
    }

    // There should be 20 mmap chunks in s1.
    let ms = get_series(&head, &s1).unwrap();
    let (exp_mmap_chunks, exp_head_chunk_samples) = {
        let g = ms.lock();
        assert_eq!(25, g.mmapped_chunks.len());
        for mmap in &g.mmapped_chunks {
            assert!(mmap.num_samples > 0);
        }
        (g.mmapped_chunks.clone(), g.head_chunk().unwrap().chunk.num_samples())
    };
    assert!(exp_head_chunk_samples > 0);

    // Series with mix of histograms and float.
    let s2 = ls(&["a", "b2"]);
    let k2 = s2.to_string();
    ts = 0;
    for gauge in [true, false] {
        let mut app = head.appender(&bg());
        let hists = if gauge { tu::generate_test_gauge_histograms(100) } else { tu::generate_test_histograms(100) };
        for mut h in hists {
            ts += 1;
            h.negative_spans = h.positive_spans.clone();
            h.negative_buckets = h.positive_buckets.clone();
            app.append_histogram(0, &s2, ts, Some(&h), None).unwrap();
            let mut eh = h.clone();
            if !gauge && ts > 30 && (ts - 10) % 20 == 1 {
                // Need "unknown" hint after float sample.
                eh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            }
            exp.entry(k2.clone()).or_default().push(h_sample(ts, eh));
            if ts % 20 == 0 {
                commit_ok(&mut app);
                app = head.appender(&bg());
                // Add some float.
                for _ in 0..10 {
                    ts += 1;
                    app.append(0, &s2, ts, ts as f64).unwrap();
                    exp.entry(k2.clone()).or_default().push(Sample::float(0, ts, ts as f64));
                }
                commit_ok(&mut app);
                app = head.appender(&bg());
            }
        }
        commit_ok(&mut app);
    }
    for gauge in [true, false] {
        let mut app = head.appender(&bg());
        let hists =
            if gauge { tu::generate_test_gauge_float_histograms(100) } else { tu::generate_test_float_histograms(100) };
        for mut h in hists {
            ts += 1;
            h.negative_spans = h.positive_spans.clone();
            h.negative_buckets = h.positive_buckets.clone();
            app.append_histogram(0, &s2, ts, None, Some(&h)).unwrap();
            let mut eh = h.clone();
            if !gauge && ts > 30 && (ts - 10) % 20 == 1 {
                // Need "unknown" hint after float sample.
                eh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            }
            exp.entry(k2.clone()).or_default().push(fh_sample(ts, eh));
            if ts % 20 == 0 {
                commit_ok(&mut app);
                app = head.appender(&bg());
                // Add some float.
                for _ in 0..10 {
                    ts += 1;
                    app.append(0, &s2, ts, ts as f64).unwrap();
                    exp.entry(k2.clone()).or_default().push(Sample::float(0, ts, ts as f64));
                }
                commit_ok(&mut app);
                app = head.appender(&bg());
            }
        }
        commit_ok(&mut app);
    }

    // Restart head.
    head.close().unwrap();
    restart(&mut head, 0);

    // Checking contents of s1.
    let ms = get_series(&head, &s1).unwrap();
    {
        let g = ms.lock();
        assert_eq!(exp_mmap_chunks, g.mmapped_chunks);
        assert_eq!(exp_head_chunk_samples, g.head_chunk().unwrap().chunk.num_samples());
    }

    let test_query = |head: &Head| {
        let act = query_head(head, head.min_time(), head.max_time(), &[m(MatchType::Regexp, "a", "b.*")]);
        compare_series(&exp, &act);
    };
    test_query(&head);

    // Restart with no mmap chunks to test WAL replay.
    head.close().unwrap();
    std::fs::remove_dir_all(tsdb::head::mmapped_chunks_dir(&head.opts.chunk_dir_root)).unwrap();
    restart(&mut head, 0);
    test_query(&head);
}

const CHUNK_SNAPSHOT_PREFIX: &str = "chunk_snapshot.";
/// Go `chunkSnapshotRecordTypeSeries`.
const CHUNK_SNAPSHOT_RECORD_TYPE_SERIES: u8 = 1;

#[test]
fn test_chunk_snapshot() {
    for enable_st_storage in [false, true] {
        chunk_snapshot_case(enable_st_storage);
    }
}

fn chunk_snapshot_case(enable_st_storage: bool) {
    let mut head = new_test_head("chunk-snapshot", 120 * 4, compression::Type::None, false);

    let num_series = 10;
    let mut exp_series: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
    let mut exp_hist: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
    let mut exp_float_hist: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
    let mut exp_tombstones: BTreeMap<SeriesRef, Intervals> = BTreeMap::new();
    let mut exp_exemplars: Vec<(Labels, Exemplar)> = Vec::new();
    let histograms = tu::generate_test_gauge_histograms(481);
    let float_histogram = tu::generate_test_gauge_float_histograms(481);

    let add_exemplar = |exp_exemplars: &mut Vec<(Labels, Exemplar)>,
                        app: &mut Box<dyn Appender>,
                        r: SeriesRef,
                        lbls: &Labels,
                        ts: i64| {
        let e = Exemplar {
            labels: ls(&["trace_id", &rand_u63().to_string()]),
            value: crate::common::rand_f64(),
            ts,
            has_ts: false,
        };
        app.append_exemplar(r, lbls, &e).unwrap();
        exp_exemplars.push((lbls.clone(), e));
    };

    let check_replay = |head: &Head,
                        exp_series: &BTreeMap<String, Vec<Sample>>,
                        exp_hist: &BTreeMap<String, Vec<Sample>>,
                        exp_float_hist: &BTreeMap<String, Vec<Sample>>,
                        exp_tombstones: &BTreeMap<SeriesRef, Intervals>,
                        exp_exemplars: &[(Labels, Exemplar)]| {
        // checkSamples, checkHistograms, checkFloatHistograms.
        assert_eq!(*exp_series, query_head(head, i64::MIN, i64::MAX, &[m(MatchType::Regexp, "foo", "bar.*")]));
        assert_eq!(*exp_hist, query_head(head, i64::MIN, i64::MAX, &[m(MatchType::Regexp, "hist", "baz.*")]));
        assert_eq!(
            *exp_float_hist,
            query_head(head, i64::MIN, i64::MAX, &[m(MatchType::Regexp, "floathist", "bat.*")])
        );
        // checkTombstones. adapted: Go calls `actTombstones[ref].Add(itv)` and discards the result, so both of its
        // maps stay empty; here both maps are really filled, which checks more.
        let tr = head.tombstones();
        let mut act_tombstones: BTreeMap<SeriesRef, Intervals> = BTreeMap::new();
        tr.iter(&mut |r, itvs| {
            for itv in itvs {
                act_tombstones.entry(r).or_default().add(*itv);
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(*exp_tombstones, act_tombstones);
        // checkExemplars: verifies both existence of right exemplars and order of exemplars in the buffer.
        let mut act_exemplars: Vec<(Labels, Exemplar)> = Vec::new();
        head.exemplars()
            .iterate_exemplars(&mut |l, e| {
                act_exemplars.push((l.clone(), e.clone()));
                Ok(())
            })
            .unwrap();
        assert_eq!(exp_exemplars, &act_exemplars[..]);
    };

    let close_head_and_check_snapshot = |head: &Head, wlast: i64, woffset: i64| {
        head.close().unwrap();
        let (_, sidx, soffset) = tsdb::head_wal::last_chunk_snapshot(&head.opts.chunk_dir_root).unwrap();
        assert_eq!(wlast, sidx);
        assert_eq!(woffset, soffset);
    };

    let add_data = |head: &Head,
                    from: i64,
                    to: i64,
                    exp_series: &mut BTreeMap<String, Vec<Sample>>,
                    exp_hist: &mut BTreeMap<String, Vec<Sample>>,
                    exp_float_hist: &mut BTreeMap<String, Vec<Sample>>,
                    exp_exemplars: &mut Vec<(Labels, Exemplar)>| {
        let mut app = head.appender(&bg());
        for i in 1..=num_series {
            let lbls = ls(&["foo", &format!("bar{i}")]);
            let lbl_str = lbls.to_string();
            let lbls_hist = ls(&["hist", &format!("baz{i}")]);
            let lbls_hist_str = lbls_hist.to_string();
            let lbls_float_hist = ls(&["floathist", &format!("bat{i}")]);
            let lbls_float_hist_str = lbls_float_hist.to_string();

            // 240 samples should m-map at least 1 chunk.
            for ts in from..=to {
                let val = crate::common::rand_f64();
                exp_series.entry(lbl_str.clone()).or_default().push(Sample::float(0, ts, val));
                let r = app.append(0, &lbls, ts, val).unwrap();

                let hist = &histograms[ts as usize];
                exp_hist.entry(lbls_hist_str.clone()).or_default().push(h_sample(ts, hist.clone()));
                app.append_histogram(0, &lbls_hist, ts, Some(hist), None).unwrap();

                let float_hist = &float_histogram[ts as usize];
                exp_float_hist.entry(lbls_float_hist_str.clone()).or_default().push(fh_sample(ts, float_hist.clone()));
                app.append_histogram(0, &lbls_float_hist, ts, None, Some(float_hist)).unwrap();

                // Add an exemplar and to create multiple WAL records.
                if ts % 10 == 0 {
                    add_exemplar(exp_exemplars, &mut app, r, &lbls, ts);
                    commit_ok(&mut app);
                    app = head.appender(&bg());
                }
            }
        }
        commit_ok(&mut app);
    };

    let add_tombstones = |head: &Head, itvs: &[Interval], exp_tombstones: &mut BTreeMap<SeriesRef, Intervals>| {
        let enc = tsdb_format::record::Encoder { enable_st_storage };
        for i in 1..=num_series {
            let r = i as SeriesRef;
            for itv in itvs {
                exp_tombstones.entry(r).or_default().add(*itv);
            }
            head.tombstones.add_interval(r, itvs);
            let mut buf = Vec::new();
            enc.tombstones(&[Stone { r#ref: r, intervals: itvs.to_vec() }], &mut buf);
            head.wal().unwrap().log(&buf).unwrap();
        }
    };

    let last_segment_and_offset = |head: &Head| {
        let (wlast, mut woffset) = head.wal().unwrap().last_segment_and_offset().unwrap();
        if woffset != 0 && woffset < 32 * 1024 {
            // The page is always filled before taking the snapshot.
            woffset = 32 * 1024;
        }
        (wlast, woffset)
    };

    // Initial data that goes into snapshot. Add some initial samples with >=1 m-map chunk.
    add_data(&head, 1, 240, &mut exp_series, &mut exp_hist, &mut exp_float_hist, &mut exp_exemplars);
    // Add some tombstones.
    add_tombstones(
        &head,
        &[Interval { mint: 1234, maxt: 2345 }, Interval { mint: 3456, maxt: 4567 }],
        &mut exp_tombstones,
    );

    // These references should be the ones used for the snapshot.
    let (wlast, woffset) = last_segment_and_offset(&head);

    // Creating snapshot and verifying it.
    head.opts.enable_memory_snapshot_on_shutdown.store(true, Ordering::SeqCst);
    close_head_and_check_snapshot(&head, wlast, woffset); // This will create a snapshot.
    // Test the replay of snapshot.
    restart(&mut head, i64::MIN);
    check_replay(&head, &exp_series, &exp_hist, &exp_float_hist, &exp_tombstones, &exp_exemplars);

    // Additional data to only include in WAL and m-mapped chunks and not snapshot. This mimics having an old
    // snapshot on disk.
    add_data(&head, 241, 480, &mut exp_series, &mut exp_hist, &mut exp_float_hist, &mut exp_exemplars);
    // Add more tombstones.
    add_tombstones(
        &head,
        &[Interval { mint: 12345, maxt: 23456 }, Interval { mint: 34567, maxt: 45678 }],
        &mut exp_tombstones,
    );

    // Close Head and verify that new snapshot was not created.
    head.opts.enable_memory_snapshot_on_shutdown.store(false, Ordering::SeqCst);
    close_head_and_check_snapshot(&head, wlast, woffset); // This should not create a snapshot.
    // Test the replay of snapshot, m-map chunks, and WAL.
    head.opts.enable_memory_snapshot_on_shutdown.store(true, Ordering::SeqCst); // Enabled to read from snapshot.
    restart(&mut head, i64::MIN);
    check_replay(&head, &exp_series, &exp_hist, &exp_float_hist, &exp_tombstones, &exp_exemplars);

    // Creating another snapshot should delete the older snapshot and replay still works fine.
    let (wlast, woffset) = last_segment_and_offset(&head);

    // Close Head and verify that new snapshot was created.
    close_head_and_check_snapshot(&head, wlast, woffset);

    // Verify that there is only 1 snapshot.
    let mut snapshots = 0;
    for f in std::fs::read_dir(&head.opts.chunk_dir_root).unwrap() {
        let name = f.unwrap().file_name().to_string_lossy().to_string();
        if name.starts_with(CHUNK_SNAPSHOT_PREFIX) {
            snapshots += 1;
            assert_eq!(tsdb::head_wal::chunk_snapshot_dir(wlast, woffset), name);
        }
    }
    assert_eq!(1, snapshots);

    // Test the replay of snapshot.
    let opts = clone_opts(&head.opts);
    opts.enable_memory_snapshot_on_shutdown.store(true, Ordering::SeqCst); // Enabled to read from snapshot.
    // Disabling exemplars to check that it does not hard fail replay
    // https://github.com/prometheus/prometheus/issues/9437#issuecomment-933285870.
    let opts = HeadOptions { enable_exemplar_storage: false, ..opts };
    opts.max_exemplars.store(0, Ordering::SeqCst);
    exp_exemplars.clear();
    reopen(&mut head, opts, Some(i64::MIN)).unwrap();
    check_replay(&head, &exp_series, &exp_hist, &exp_float_hist, &exp_tombstones, &exp_exemplars);

    assert_eq!(0.0, head.metrics.snapshot_replay_error_total.get());

    head.opts.enable_memory_snapshot_on_shutdown.store(false, Ordering::SeqCst);
    head.close().unwrap();
}

fn corrupt_byte(path: &std::path::Path, off: u64) {
    use std::os::unix::fs::FileExt;
    let f = std::fs::OpenOptions::new().read(true).write(true).open(path).unwrap();
    f.write_all_at(&[0b1111_1111], off).unwrap();
}

#[test]
fn test_snapshot_error() {
    let mut head = new_test_head("snapshot-error", 120 * 4, compression::Type::None, false);

    // Add a sample.
    let mut app = head.appender(&bg());
    let lbls = ls(&["foo", "bar"]);
    app.append(0, &lbls, 99, 99.0).unwrap();

    // Add histograms
    let hist = tu::generate_test_gauge_histograms(1).remove(0);
    let float_hist = tu::generate_test_gauge_float_histograms(1).remove(0);
    let lbls_hist = ls(&["hist", "bar"]);
    let lbls_float_hist = ls(&["floathist", "bar"]);
    app.append_histogram(0, &lbls_hist, 99, Some(&hist), None).unwrap();
    app.append_histogram(0, &lbls_float_hist, 99, None, Some(&float_hist)).unwrap();
    commit_ok(&mut app);

    // Add some tombstones.
    let itvs = [Interval { mint: 1234, maxt: 2345 }, Interval { mint: 3456, maxt: 4567 }];
    head.tombstones.add_interval(1, &itvs);

    // Check existence of data.
    assert!(get_series(&head, &lbls).is_some());
    assert!(!head.tombstones.get(1).is_empty());

    head.opts.enable_memory_snapshot_on_shutdown.store(true, Ordering::SeqCst);
    head.close().unwrap(); // This will create a snapshot.

    // Remove the WAL so that we don't load from it.
    let wal_dir = head.wal().unwrap().dir().to_path_buf();
    std::fs::remove_dir_all(&wal_dir).unwrap();

    // Corrupt the snapshot.
    let (snap_dir, _, _) = tsdb::head_wal::last_chunk_snapshot(&head.opts.chunk_dir_root).unwrap();
    let mut files: Vec<_> = std::fs::read_dir(&snap_dir).unwrap().map(|e| e.unwrap().path()).collect();
    files.sort();
    // Create snapshot backup to be restored on future test cases.
    let snapshot_backup = std::fs::read(&files[0]).unwrap();
    corrupt_byte(&files[0], 18);

    // Create new Head which should replay this snapshot.
    restart(&mut head, i64::MIN);

    // There should be no series in the memory after snapshot error since WAL was removed.
    assert_eq!(1.0, head.metrics.snapshot_replay_error_total.get());
    assert_eq!(0, head.num_series());
    assert!(get_series(&head, &lbls).is_none());
    assert!(head.tombstones.get(1).is_empty());
    head.close().unwrap();

    // Test corruption in the middle of the snapshot.
    std::fs::write(&files[0], &snapshot_backup).unwrap();
    corrupt_byte(&files[0], 300);

    let c = Arc::new(CountSeriesLifecycleCallback::default());
    let opts = HeadOptions { series_callback: Some(c.clone()), ..clone_opts(&head.opts) };
    reopen(&mut head, opts, Some(i64::MIN)).unwrap();

    // There should be no series in the memory after snapshot error since WAL was removed.
    assert_eq!(1.0, head.metrics.snapshot_replay_error_total.get());
    assert!(get_series(&head, &lbls).is_none());
    assert_eq!(0, head.num_series());

    // Since the snapshot could replay certain series, we continue invoking the create hooks. In such instances, we
    // need to ensure that we also trigger the delete hooks when resetting the memory.
    assert_eq!(2, c.created.load(Ordering::SeqCst));
    assert_eq!(2, c.deleted.load(Ordering::SeqCst));

    assert_eq!(2.0, head.metrics.series_removed.get());
    assert_eq!(2.0, head.metrics.series_created.get());

    head.opts.enable_memory_snapshot_on_shutdown.store(false, Ordering::SeqCst);
    head.close().unwrap();
}

#[test]
fn test_snapshot_unknown_encoding_falls_back_to_wal() {
    let mut head = new_test_head("snapshot-unknown-encoding", 120 * 4, compression::Type::None, false);

    let float_hist = tu::generate_test_gauge_float_histograms(1).remove(0);
    let lbls_float_hist = ls(&["floathist", "bar"]);
    let lbls_float = ls(&["foo", "bar"]);

    let mut app = head.appender(&bg());
    app.append_histogram(0, &lbls_float_hist, 99, None, Some(&float_hist)).unwrap();
    app.append(0, &lbls_float, 99, 99.0).unwrap();
    commit_ok(&mut app);

    head.opts.enable_memory_snapshot_on_shutdown.store(true, Ordering::SeqCst);
    head.close().unwrap();

    // Find the snapshot and corrupt the encoding byte of the float histogram series.
    let (snap_dir, _, _) = tsdb::head_wal::last_chunk_snapshot(&head.opts.chunk_dir_root).unwrap();

    let sr = wlog::new_segments_reader(&snap_dir).unwrap();
    let mut r = wlog::Reader::new(sr);
    let mut rdec = tsdb_format::record::Decoder::new(tsdb::Logger::default());
    let mut records: Vec<Vec<u8>> = Vec::new();
    let mut mutated = false;
    while r.next() {
        let mut rec = r.record().to_vec();
        if rec[0] == CHUNK_SNAPSHOT_RECORD_TYPE_SERIES {
            let enc_pos = {
                let mut buf = tsdb_format::encoding::Decbuf::new(&rec);
                let _ = buf.byte(); // flag
                let _ = buf.be64(); // ref
                let lset = rdec.decode_labels(&mut buf);
                let _ = buf.be64int64(); // chunkRange
                if buf.uvarint() == 1 && lset.get("floathist") == "bar" {
                    let _ = buf.be64int64(); // minTime
                    let _ = buf.be64int64(); // maxTime
                    Some(rec.len() - buf.len())
                } else {
                    None
                }
            };
            if let Some(enc_pos) = enc_pos {
                assert_eq!(chunkenc::ENC_FLOAT_HISTOGRAM.0, rec[enc_pos], "expected float histogram encoding at computed offset");
                rec[enc_pos] = 0xFF;
                mutated = true;
            }
        }
        records.push(rec);
    }
    assert!(r.err().is_none());
    drop(r);
    assert!(mutated, "expected to find and corrupt the float histogram series record");

    // Rewrite the snapshot with the mutated records.
    for f in std::fs::read_dir(&snap_dir).unwrap() {
        std::fs::remove_file(f.unwrap().path()).unwrap();
    }
    let cp = WL::new(&snap_dir, compression::Type::None).unwrap();
    cp.log_batch(&records).unwrap();
    cp.close().unwrap();

    // Reload the head; snapshot should fail due to unknown encoding and fall back to WAL.
    restart(&mut head, i64::MIN);

    assert_eq!(1.0, head.metrics.snapshot_replay_error_total.get());
    assert_eq!(2, head.num_series(), "both series must be recovered from WAL");
    assert!(get_series(&head, &lbls_float).is_some());
    assert!(get_series(&head, &lbls_float_hist).is_some());

    head.opts.enable_memory_snapshot_on_shutdown.store(false, Ordering::SeqCst);
    head.close().unwrap();
}

#[test]
fn test_histogram_metrics() {
    let num_histograms = 10;
    let mut head = new_test_head("histogram-metrics", 1000, compression::Type::None, false);
    head.init(0).unwrap();

    let mut exp_h_samples = 0;
    for x in 0..5 {
        let l = ls(&["a", &format!("b{x}")]);
        for (i, h) in tu::generate_test_histograms(num_histograms).into_iter().enumerate() {
            let mut app = head.appender(&bg());
            app.append_histogram(0, &l, i as i64, Some(&h), None).unwrap();
            commit_ok(&mut app);
            exp_h_samples += 1;
        }
        for (i, fh) in tu::generate_test_float_histograms(num_histograms).into_iter().enumerate() {
            let mut app = head.appender(&bg());
            app.append_histogram(0, &l, (num_histograms + i) as i64, None, Some(&fh)).unwrap();
            commit_ok(&mut app);
            exp_h_samples += 1;
        }
    }

    assert_eq!(exp_h_samples as f64, head.metrics.samples_appended.with_label_values("histogram").get());

    head.close().unwrap();
    restart(&mut head, 0);

    assert_eq!(0.0, head.metrics.samples_appended.with_label_values("histogram").get()); // Counter reset.
}

#[test]
fn test_histogram_stale_sample() {
    // integer histogram
    histogram_stale_sample_helper(false);
    // float histogram
    histogram_stale_sample_helper(true);
}

#[derive(Clone, Debug, PartialEq)]
struct TimedHistogram {
    t: i64,
    h: Option<Histogram>,
    fh: Option<FloatHistogram>,
}

fn histogram_stale_sample_helper(float_histogram: bool) {
    let l = ls(&["a", "b"]);
    let num_histograms = 20;
    let head = new_test_head("histogram-stale", 100000, compression::Type::None, false);
    head.init(0).unwrap();

    let mut exp_histograms: Vec<TimedHistogram> = Vec::with_capacity(num_histograms);
    let stale = f64::from_bits(model::value::STALE_NAN);

    let test_query = |exp_histograms: &mut Vec<TimedHistogram>, num_stale: usize| {
        let mut q = new_block_querier(&**head, head.min_time(), head.max_time()).unwrap();
        let mut act_histograms: Vec<TimedHistogram> = Vec::new();
        {
            let mut ss = q.select(&bg(), false, None, &[m(MatchType::Equal, "a", "b")]);
            assert!(ss.next());
            let s = ss.at().clone();
            assert!(!ss.next());
            let mut it = s.iterator(None);
            loop {
                match it.next() {
                    ValueType::None => break,
                    ValueType::Histogram => {
                        let (t, h) = it.at_histogram(None);
                        act_histograms.push(TimedHistogram { t, h: Some(h), fh: None });
                    }
                    ValueType::FloatHistogram => {
                        let (t, fh) = it.at_float_histogram(None);
                        act_histograms.push(TimedHistogram { t, h: None, fh: Some(fh) });
                    }
                    ValueType::Float => {}
                }
            }
        }
        assert!(q.close().is_none());

        // We cannot compare StaleNAN with assert_eq, hence checking each histogram manually.
        assert_eq!(exp_histograms.len(), act_histograms.len());
        let mut act_num_stale = 0;
        for i in 0..exp_histograms.len() {
            let mut ah = act_histograms[i].clone();
            // Go: `eh` holds pointers into expHistograms, so the hint changes stick; the stale case copies first.
            let prev_unknown = i > 0 && {
                let prev = &exp_histograms[i - 1];
                if float_histogram {
                    prev.fh.as_ref().is_none_or(|p| model::value::is_stale_nan(p.sum))
                } else {
                    prev.h.as_ref().is_none_or(|p| model::value::is_stale_nan(p.sum))
                }
            };
            let eh = if float_histogram {
                if model::value::is_stale_nan(exp_histograms[i].fh.as_ref().unwrap().sum) {
                    act_num_stale += 1;
                    assert!(model::value::is_stale_nan(ah.fh.as_ref().unwrap().sum));
                    // To make the comparison work.
                    ah.fh.as_mut().unwrap().sum = 0.0;
                    let mut eh = exp_histograms[i].clone();
                    eh.fh.as_mut().unwrap().sum = 0.0;
                    eh
                } else {
                    if prev_unknown {
                        exp_histograms[i].fh.as_mut().unwrap().counter_reset_hint = CounterResetHint::UnknownCounterReset;
                    }
                    exp_histograms[i].clone()
                }
            } else if model::value::is_stale_nan(exp_histograms[i].h.as_ref().unwrap().sum) {
                act_num_stale += 1;
                assert!(model::value::is_stale_nan(ah.h.as_ref().unwrap().sum));
                // To make the comparison work.
                ah.h.as_mut().unwrap().sum = 0.0;
                let mut eh = exp_histograms[i].clone();
                eh.h.as_mut().unwrap().sum = 0.0;
                eh
            } else {
                if prev_unknown {
                    exp_histograms[i].h.as_mut().unwrap().counter_reset_hint = CounterResetHint::UnknownCounterReset;
                }
                exp_histograms[i].clone()
            };
            assert_eq!(eh, ah);
        }
        assert_eq!(num_stale, act_num_stale);
    };

    let push = |exp_histograms: &mut Vec<TimedHistogram>, app: &mut Box<dyn Appender>, h: Histogram| {
        let t = 100 * exp_histograms.len() as i64;
        if float_histogram {
            app.append_histogram(0, &l, t, None, Some(&h.to_float())).unwrap();
            exp_histograms.push(TimedHistogram { t, h: None, fh: Some(h.to_float()) });
        } else {
            app.append_histogram(0, &l, t, Some(&h), None).unwrap();
            exp_histograms.push(TimedHistogram { t, h: Some(h), fh: None });
        }
    };
    let push_stale = |exp_histograms: &mut Vec<TimedHistogram>, app: &mut Box<dyn Appender>| {
        // +1 so that delta-of-delta is not 0.
        let t = 100 * exp_histograms.len() as i64 + 1;
        app.append(0, &l, t, stale).unwrap();
        if float_histogram {
            exp_histograms.push(TimedHistogram { t, h: None, fh: Some(FloatHistogram { sum: stale, ..Default::default() }) });
        } else {
            exp_histograms.push(TimedHistogram { t, h: Some(Histogram { sum: stale, ..Default::default() }), fh: None });
        }
    };

    // Adding stale in the same appender.
    let mut app = head.appender(&bg());
    for h in tu::generate_test_histograms(num_histograms) {
        push(&mut exp_histograms, &mut app, h);
    }
    push_stale(&mut exp_histograms, &mut app);
    commit_ok(&mut app);

    // Only 1 chunk in the memory, no m-mapped chunk.
    {
        let s = get_series(&head, &l).unwrap();
        let g = s.lock();
        assert_eq!(1, g.head_chunks.len());
        assert!(g.mmapped_chunks.is_empty());
    }
    test_query(&mut exp_histograms, 1);

    // Adding stale in different appender and continuing series after a stale sample.
    let mut app = head.appender(&bg());
    for h in tu::generate_test_histograms(2 * num_histograms).into_iter().skip(num_histograms) {
        push(&mut exp_histograms, &mut app, h);
    }
    commit_ok(&mut app);

    let mut app = head.appender(&bg());
    push_stale(&mut exp_histograms, &mut app);
    commit_ok(&mut app);
    head.mmap_head_chunks();

    // Total 2 chunks, 1 m-mapped.
    {
        let s = get_series(&head, &l).unwrap();
        let g = s.lock();
        assert_eq!(1, g.head_chunks.len());
        assert_eq!(1, g.mmapped_chunks.len());
    }
    test_query(&mut exp_histograms, 2);
}

#[test]
fn test_histogram_counter_reset_header() {
    for float_histo in [true, false] {
        let l = ls(&["a", "b"]);
        let head = new_test_head("histogram-crh", 1000, compression::Type::None, false);
        head.init(0).unwrap();

        let mut ts = 0i64;
        let mut append_histogram = |h: &Histogram| {
            ts += 1;
            let mut app = head.appender(&bg());
            if float_histo {
                app.append_histogram(0, &l, ts, None, Some(&h.to_float())).unwrap();
            } else {
                app.append_histogram(0, &l, ts, Some(&h.clone()), None).unwrap();
            }
            commit_ok(&mut app);
        };

        let mut exp_headers: Vec<CounterResetHeader> = Vec::new();
        let mut check_exp_counter_reset_header = |new_headers: &[CounterResetHeader]| {
            exp_headers.extend_from_slice(new_headers);

            let (ms, _) = head.get_or_create(l.hash(), &l, false).unwrap();
            let mut g = ms.lock();
            g.mmap_chunks(&head.chunk_disk_mapper, ms.r#ref);
            assert_eq!(exp_headers.len() - 1, g.mmapped_chunks.len()); // One is the head chunk.

            for (i, mmap_chunk) in g.mmapped_chunks.iter().enumerate() {
                let chk = head.chunk_disk_mapper.chunk(mmap_chunk.r#ref).unwrap();
                assert_eq!(exp_headers[i], counter_reset_header(&*chk, float_histo, false));
            }
            assert_eq!(
                exp_headers[exp_headers.len() - 1],
                counter_reset_header(&*g.head_chunk().unwrap().chunk, float_histo, false)
            );
        };
        let head_chunk_count = || {
            let (ms, _) = head.get_or_create(l.hash(), &l, false).unwrap();
            let n = ms.lock().head_chunk_count();
            n
        };

        let mut h = tu::generate_test_histograms(1).remove(0);
        h.positive_buckets = vec![100, 1, 1, 1];
        h.negative_buckets = vec![100, 1, 1, 1];
        // Count = positive delta-decoded (100+101+102+103=406) + negative (406) + ZeroCount (2) = 814.
        h.count = 814;

        // First histogram is UnknownCounterReset.
        append_histogram(&h);
        check_exp_counter_reset_header(&[CounterResetHeader::UNKNOWN_COUNTER_RESET]);

        // Another normal histogram: increment a bucket and Count consistently.
        *h.positive_buckets.last_mut().unwrap() += 1;
        h.count += 1; // Count = 815.
        append_histogram(&h);
        check_exp_counter_reset_header(&[]);

        // Counter reset: decrement the same bucket and Count.
        *h.positive_buckets.last_mut().unwrap() -= 1;
        h.count -= 1; // Count = 814.
        append_histogram(&h);
        check_exp_counter_reset_header(&[CounterResetHeader::COUNTER_RESET]);

        // Add 2 non-counter reset histogram chunks.
        while head_chunk_count() < 3 {
            append_histogram(&h);
        }
        check_exp_counter_reset_header(&[CounterResetHeader::NOT_COUNTER_RESET, CounterResetHeader::NOT_COUNTER_RESET]);

        // Changing schema will cut a new chunk with unknown counter reset.
        h.schema += 1;
        append_histogram(&h);
        check_exp_counter_reset_header(&[CounterResetHeader::UNKNOWN_COUNTER_RESET]);

        // Changing schema will zero threshold a new chunk with unknown counter reset.
        h.zero_threshold += 0.01;
        append_histogram(&h);
        check_exp_counter_reset_header(&[CounterResetHeader::UNKNOWN_COUNTER_RESET]);

        // Counter reset by removing a positive bucket.
        h.positive_spans[1].length -= 1;
        h.positive_buckets.remove(0);
        // After removal: positive delta-decoded (1+2+3=6) + negative (406) + ZeroCount (2) = 414.
        h.count = 414;
        append_histogram(&h);
        check_exp_counter_reset_header(&[CounterResetHeader::COUNTER_RESET]);

        // Counter reset by removing a negative bucket.
        h.negative_spans[1].length -= 1;
        h.negative_buckets.remove(0);
        // After removal: positive (6) + negative delta-decoded (1+2+3=6) + ZeroCount (2) = 14.
        h.count = 14;
        append_histogram(&h);
        check_exp_counter_reset_header(&[CounterResetHeader::COUNTER_RESET]);

        // Add 2 non-counter reset histogram chunks. Just to have some non-counter reset chunks in between.
        while head_chunk_count() < 3 {
            append_histogram(&h);
        }
        check_exp_counter_reset_header(&[CounterResetHeader::NOT_COUNTER_RESET, CounterResetHeader::NOT_COUNTER_RESET]);

        // Counter reset with counter reset in a positive bucket.
        *h.positive_buckets.last_mut().unwrap() -= 1;
        // After: positive delta-decoded (1+2+2=5) + negative (6) + ZeroCount (2) = 13.
        h.count = 13;
        append_histogram(&h);
        check_exp_counter_reset_header(&[CounterResetHeader::COUNTER_RESET]);

        // Counter reset with counter reset in a negative bucket.
        *h.negative_buckets.last_mut().unwrap() -= 1;
        // After: positive (5) + negative delta-decoded (1+2+2=5) + ZeroCount (2) = 12.
        h.count = 12;
        append_histogram(&h);
        check_exp_counter_reset_header(&[CounterResetHeader::COUNTER_RESET]);
    }
}

#[test]
fn test_histogram_st_counter_reset_header_on_chunk_cut() {
    for float_histo in [false, true] {
        let l = ls(&["a", "b"]);
        let opts = default_test_opts(2, false);
        opts.float_chunk_encoding.store(chunkenc::ENC_XOR2.0 as u32, Ordering::SeqCst);
        opts.enable_histogram_st_encoding.store(true, Ordering::SeqCst);
        let head = new_test_head_with_options("histogram-st-crh", compression::Type::None, opts);
        head.init(0).unwrap();

        let append_histogram = |ts: i64, h: &Histogram| {
            let mut app = head.appender(&bg());
            if float_histo {
                app.append_histogram(0, &l, ts, None, Some(&h.to_float())).unwrap();
            } else {
                app.append_histogram(0, &l, ts, Some(&h.clone()), None).unwrap();
            }
            commit_ok(&mut app);
        };

        let h1 = tu::generate_test_histogram(0);
        let h2 = tu::generate_test_histogram(1);

        // Chunk range is 2, so appending at t=1 and t=2 cuts a new chunk on the second append.
        append_histogram(1, &h1);
        append_histogram(2, &h2);

        let (ms, _) = head.get_or_create(l.hash(), &l, false).unwrap();
        let g = ms.lock();
        assert!(g.head_chunks.len() >= 2);
        let prev = &g.head_chunks[g.head_chunks.len() - 2];
        let cur = g.head_chunk().unwrap();
        assert_eq!(CounterResetHeader::UNKNOWN_COUNTER_RESET, counter_reset_header(&*prev.chunk, float_histo, true));
        assert_eq!(CounterResetHeader::NOT_COUNTER_RESET, counter_reset_header(&*cur.chunk, float_histo, true));
    }
}

#[test]
fn test_ooo_histogram_counter_reset_headers() {
    for float_histo in [true, false] {
        let l = ls(&["a", "b"]);
        let head = new_test_head("ooo-histogram-crh", 1000, compression::Type::None, true);
        head.opts.out_of_order_cap_max.store(5, Ordering::SeqCst);
        head.init(0).unwrap();

        let append_histogram = |ts: i64, h: Histogram| {
            let mut app = head.appender(&bg());
            if float_histo {
                app.append_histogram(0, &l, ts, None, Some(&h.to_float())).unwrap();
            } else {
                app.append_histogram(0, &l, ts, Some(&h), None).unwrap();
            }
            commit_ok(&mut app);
        };

        // (header, mint, maxt, numSamples)
        type Exp = (CounterResetHeader, i64, i64, u16);
        let mut exp_chunks: Vec<Exp> = Vec::new();
        let mut check = |new_chunks: &[Exp]| {
            exp_chunks.extend_from_slice(new_chunks);
            let (ms, _) = head.get_or_create(l.hash(), &l, false).unwrap();
            let g = ms.lock();
            let chunks = g.ooo().map(|o| o.ooo_mmapped_chunks.clone()).unwrap_or_default();
            assert_eq!(exp_chunks.len(), chunks.len());
            for (i, mmap_chunk) in chunks.iter().enumerate() {
                let chk = head.chunk_disk_mapper.chunk(mmap_chunk.r#ref).unwrap();
                assert_eq!(exp_chunks[i].0, counter_reset_header(&*chk, float_histo, false));
                assert_eq!(exp_chunks[i].1, mmap_chunk.min_time);
                assert_eq!(exp_chunks[i].2, mmap_chunk.max_time);
                assert_eq!(exp_chunks[i].3, mmap_chunk.num_samples);
            }
        };
        let g = tu::generate_test_histogram;
        const U: CounterResetHeader = CounterResetHeader::UNKNOWN_COUNTER_RESET;
        const C: CounterResetHeader = CounterResetHeader::COUNTER_RESET;

        // Append an in-order histogram, so the rest of the samples can be detected as OOO.
        append_histogram(1000, g(1000));

        // OOO histogram
        for i in 1..=5 {
            append_histogram(100 + i, g(1000 + i));
        }
        // Nothing mmapped yet.
        check(&[]);

        // 6th observation (which triggers a head chunk mmapping).
        append_histogram(112, g(1002));

        // One mmapped chunk with (ts, val) [(101, 1001), (102, 1002), (103, 1003), (104, 1004), (105, 1005)].
        check(&[(U, 101, 105, 5)]);

        // Add more samples, there's a counter reset at ts 122.
        append_histogram(110, g(1001));
        append_histogram(124, g(904));
        append_histogram(123, g(903));
        append_histogram(122, g(902));

        // New samples not mmapped yet.
        check(&[]);

        // 11th observation (which triggers another head chunk mmapping).
        append_histogram(200, g(2000));

        // Two new mmapped chunks [(110, 1001), (112, 1002)], [(122, 902), (123, 903), (124, 904)].
        check(&[(U, 110, 112, 2), (C, 122, 124, 3)]);

        // Count is lower than previous sample at ts 200, and NotCounterReset is always ignored on append.
        append_histogram(205, tu::set_histogram_not_counter_reset(g(1000)));
        append_histogram(210, tu::set_histogram_counter_reset(g(2010)));
        append_histogram(220, g(2020));
        append_histogram(215, g(2005));

        // 16th observation (which triggers another head chunk mmapping).
        append_histogram(350, g(4000));

        // Four new mmapped chunks: [(200, 2000)] [(205, 1000)], [(210, 2010)], [(215, 2015), (220, 2020)]
        check(&[(U, 200, 200, 1), (C, 205, 205, 1), (C, 210, 210, 1), (C, 215, 220, 2)]);

        // Adding five more samples (21 in total), so another mmapped chunk is created.
        append_histogram(300, tu::set_histogram_counter_reset(g(3000)));
        for i in 1..=4 {
            append_histogram(300 + i, g(3000 + i));
        }

        // One mmapped chunk with (ts, val) [(300, 3000), (301, 3001), (302, 3002), (303, 3003), (350, 4000)].
        check(&[(C, 300, 350, 5)]);
    }
}

#[test]
fn test_append_histogram_error_does_not_set_pending_commit() {
    let cases: Vec<(&str, Option<Histogram>, Option<FloatHistogram>)> = vec![
        ("integer", Some(tu::generate_test_histogram(0)), None),
        ("float", None, Some(tu::generate_test_float_histogram(0))),
    ];
    for (name, h, fh) in cases {
        let head = new_test_head("append-hist-err-pending", 1000, compression::Type::None, false);
        head.init(0).unwrap();
        let lbls = ls(&["a", "b"]);

        // Seed an in-order sample so the series exists with maxTime=200.
        let mut app = head.appender(&bg());
        app.append_histogram(0, &lbls, 200, h.as_ref(), fh.as_ref()).unwrap();
        commit_ok(&mut app);

        let (ms, _) = head.get_or_create(lbls.hash(), &lbls, false).unwrap();
        let pc = ms.lock().has_pending_commit();
        assert!(!pc, "{name}: pending state should be cleared after a successful commit");

        // Attempt an out-of-order append: same series, earlier timestamp, OOO time window disabled.
        // appendableHistogram returns ErrOutOfOrderSample, so the sample is never recorded in the appender's batch
        // and Commit/Rollback never clears pending state for this series.
        let mut app = head.appender(&bg());
        let err = app.append_histogram(0, &lbls, 100, h.as_ref(), fh.as_ref()).unwrap_err();
        assert!(err.is(&storage::ERR_OUT_OF_ORDER_SAMPLE), "{name}: {err}");
        assert!(app.rollback().is_none());

        let pc = ms.lock().has_pending_commit();
        assert!(!pc, "{name}: pending state should remain clear after a failed AppendHistogram");
    }
}

/// A head in its own directory with options built by the test (Go `NewHead(nil, nil, wal, wbl, opts, nil)`).
fn own_head(dir: TempDir, wal: Option<Arc<WL>>, wbl: Option<Arc<WL>>, mut opts: HeadOptions) -> TestHead {
    opts.chunk_dir_root = dir.path().to_path_buf();
    let head = Head::new(wal.clone(), wbl, opts, None).expect("new head");
    TestHead { head, wal, dir }
}

fn new_wl(dir: &std::path::Path, name: &str, c: compression::Type) -> Arc<WL> {
    Arc::new(WL::new_size(dir.join(name), 32768, c).expect("wl"))
}

// Tests https://github.com/prometheus/prometheus/issues/9725.
#[test]
fn test_chunk_snapshot_replay_bug() {
    for enable_st_storage in [false, true] {
        let dir = TempDir::new();
        let wal = new_wl(dir.path(), "wal", compression::Type::Snappy);

        // Write few series records and samples such that the series references are not in order in the WAL for
        // status_code="200".
        let enc = tsdb_format::record::Encoder { enable_st_storage };
        for i in 1..=1000u64 {
            let r = if i <= 500 { i * 100 } else { (i - 500) * 50 };
            let series_rec = tsdb_format::record::RefSeries {
                r#ref: r,
                labels: ls(&["__name__", "request_duration", "status_code", "200", "foo", &format!("baz{}", rand_u63())]),
            };
            // Add a sample so that the series is not garbage collected.
            let samples_rec = tsdb_format::record::RefSample { r#ref: r, st: 0, t: 1000, v: 1000.0 };
            let mut buf = Vec::new();
            enc.series(&[series_rec], &mut buf);
            wal.log(&buf).unwrap();
            buf.clear();
            enc.samples(&[samples_rec], &mut buf);
            wal.log(&buf).unwrap();
        }

        // Write a corrupt snapshot to fail the replay on startup.
        let cpdir = dir.path().join(tsdb::head_wal::chunk_snapshot_dir(0, 100));
        std::fs::create_dir_all(&cpdir).unwrap();
        std::fs::write(cpdir.join("00000000"), [1u8, 5, 3, 5, 6, 7, 4, 2, 2]).unwrap();

        let opts = HeadOptions::default();
        opts.enable_memory_snapshot_on_shutdown.store(true, Ordering::SeqCst);
        let head = own_head(dir, Some(wal), None, opts);
        head.init(i64::MIN).unwrap();

        // Snapshot replay should error out.
        assert_eq!(1.0, head.metrics.snapshot_replay_error_total.get());

        // Querying `request_duration{status_code!="200"}` should return no series since all of them have
        // status_code="200".
        let series = query_head(
            &head,
            i64::MIN,
            i64::MAX,
            &[m(MatchType::Equal, "__name__", "request_duration"), m(MatchType::NotEqual, "status_code", "200")],
        );
        assert!(series.is_empty(), "there should be no series found");
        head.close().unwrap();
    }
}

#[test]
fn test_chunk_snapshot_taken_after_incomplete_snapshot() {
    let dir = TempDir::new();
    let wl_temp = new_wl(dir.path(), "wal", compression::Type::Snappy);

    // Write a snapshot with .tmp suffix. This used to fail taking any further snapshots or replay of snapshots.
    let cpdir = dir.path().join(tsdb::head_wal::chunk_snapshot_dir(0, 100) + ".tmp");
    std::fs::create_dir_all(&cpdir).unwrap();

    let opts = HeadOptions::default();
    opts.enable_memory_snapshot_on_shutdown.store(true, Ordering::SeqCst);
    let head = own_head(dir, Some(wl_temp), None, opts);
    head.init(i64::MIN).unwrap();

    assert_eq!(0.0, head.metrics.snapshot_replay_error_total.get());

    // Add some samples for the snapshot.
    let mut app = head.appender(&bg());
    app.append(0, &ls(&["foo", "bar"]), 10, 10.0).unwrap();
    commit_ok(&mut app);

    // Should not return any error for a successful snapshot.
    head.close().unwrap();

    // Verify the snapshot.
    let (name, idx, offset) = tsdb::head_wal::last_chunk_snapshot(head.dir.path()).unwrap();
    assert!(!name.as_os_str().is_empty());
    assert_eq!(0, idx);
    assert!(offset > 0);
}

const MINUTE: i64 = 60 * 1000;

/// Go `requireEqualSamples(..., requireEqualSamplesIgnoreCounterResets)`.
fn require_equal_samples_ignore_counter_resets(name: &str, expected: &[Sample], actual: &[Sample]) {
    assert_eq!(expected.len(), actual.len(), "Length not equal to expected for {name}");
    for (i, (e, a)) in expected.iter().zip(actual).enumerate() {
        assert_eq!(e.t, a.t, "Different timestamps for {name}[{i}]");
        assert_eq!(e.typ(), a.typ(), "Different types for {name}[{i}] at ts {}", e.t);
        if let Some(eh) = &e.h {
            let mut eh = (**eh).clone();
            let mut ah = a.h.as_deref().unwrap().clone();
            if eh.counter_reset_hint != CounterResetHint::GaugeType {
                eh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                ah.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            } else {
                assert_eq!(eh.counter_reset_hint, ah.counter_reset_hint, "Sample header doesn't match for {name}[{i}]");
            }
            assert_eq!(eh, ah, "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        } else if let Some(efh) = &e.fh {
            let mut efh = (**efh).clone();
            let mut afh = a.fh.as_deref().unwrap().clone();
            efh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            afh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            assert_eq!(efh, afh, "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        } else {
            assert_eq!(e.f, a.f, "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        }
    }
}

fn wal_wbl_head(dir: TempDir, opts: HeadOptions) -> TestHead {
    let wal = new_wl(dir.path(), "wal", compression::Type::Snappy);
    let wbl = new_wl(dir.path(), wlog::WBL_DIR_NAME, compression::Type::Snappy);
    own_head(dir, Some(wal), Some(wbl), opts)
}

/// Closes the head and opens a new one with WAL and WBL on the same directory and a copy of its options.
fn restart_wal_wbl(th: &mut TestHead, init: i64) {
    th.head.close().unwrap();
    let opts = clone_opts(&th.head.opts);
    let wal = new_wl(th.dir.path(), "wal", compression::Type::Snappy);
    let wbl = new_wl(th.dir.path(), wlog::WBL_DIR_NAME, compression::Type::Snappy);
    th.head = Head::new(Some(wal.clone()), Some(wbl), opts, None).expect("new head");
    th.wal = Some(wal);
    th.head.init(init).expect("init"); // Replay happens here.
}

/// TestWBLReplay checks the replay at a low level.
#[test]
fn test_wbl_replay() {
    for (name, scenario) in crate::common::sample_type_scenarios() {
        wbl_replay_case(name, scenario);
    }
}

fn wbl_replay_case(name: &str, scenario: crate::common::SampleTypeScenario) {
    let opts = HeadOptions { chunk_range: 1000, ..Default::default() };
    opts.out_of_order_time_window.store(30 * MINUTE, Ordering::SeqCst);
    let mut h = wal_wbl_head(TempDir::new(), opts);
    h.init(0).unwrap();

    let mut exp_ooo_samples: Vec<Sample> = Vec::new();
    let l = ls(&["foo", "bar"]);
    let mut append_sample = |h: &Head, mins: i64, is_ooo: bool| {
        let mut app = h.appender(&bg());
        let (r, s) = scenario.append(&mut *app, &l, mins * MINUTE, mins);
        r.unwrap();
        commit_ok(&mut app);
        if is_ooo {
            exp_ooo_samples.push(s);
        }
    };

    // In-order sample.
    append_sample(&h, 60, false);

    // Out of order samples.
    for mins in [40, 35, 50, 55, 59, 31] {
        append_sample(&h, mins, true);
    }

    // Check that Head's time ranges are set properly.
    assert_eq!(60 * MINUTE, h.min_time(), "{name}");
    assert_eq!(60 * MINUTE, h.max_time(), "{name}");
    assert_eq!(31 * MINUTE, h.min_ooo_time(), "{name}");
    assert_eq!(59 * MINUTE, h.max_ooo_time(), "{name}");

    // Restart head.
    restart_wal_wbl(&mut h, 0);

    // Get the ooo samples from the Head.
    let (ms, created) = h.get_or_create(l.hash(), &l, false).unwrap();
    assert!(!created);

    let chks = {
        let mut g = ms.lock();
        g.ooo_mut().unwrap().ooo_head_chunk.as_mut().unwrap().chunk.to_encoded_chunks(i64::MIN, i64::MAX, false, false).unwrap()
    };
    assert_eq!(1, chks.len());

    let mut it = chks[0].chunk.iterator(None);
    let act_ooo_samples = storage::expand_samples(&mut it).unwrap();

    // OOO chunk will be sorted. Hence sort the expected samples.
    exp_ooo_samples.sort_by_key(|s| s.t);

    // Ignoring counter resets prevents differences in counter reset headers from being factored in to the sample
    // comparison.
    require_equal_samples_ignore_counter_resets(&l.to_string(), &exp_ooo_samples, &act_ooo_samples);

    h.close().unwrap();
}

/// TestOOOMmapReplay checks the replay at a low level.
#[test]
fn test_ooo_mmap_replay() {
    for (name, scenario) in crate::common::sample_type_scenarios() {
        ooo_mmap_replay_case(name, scenario);
    }
}

fn ooo_mmap_replay_case(name: &str, scenario: crate::common::SampleTypeScenario) {
    let opts = HeadOptions { chunk_range: 1000, ..Default::default() };
    opts.out_of_order_cap_max.store(30, Ordering::SeqCst);
    opts.out_of_order_time_window.store(1000 * MINUTE, Ordering::SeqCst);
    let mut h = wal_wbl_head(TempDir::new(), opts);
    h.init(0).unwrap();

    let l = ls(&["foo", "bar"]);
    let append_sample = |h: &Head, mins: i64| {
        let mut app = h.appender(&bg());
        scenario.append(&mut *app, &l, mins * MINUTE, mins).0.unwrap();
        commit_ok(&mut app);
    };

    // In-order sample.
    append_sample(&h, 200);

    // Out of order samples. 92 samples to create 3 m-map chunks.
    for mins in 100..=191 {
        append_sample(&h, mins);
    }

    let check = |h: &Head| {
        let (ms, created) = h.get_or_create(l.hash(), &l, false).unwrap();
        assert!(!created, "{name}");
        let chunks = ms.lock().ooo().unwrap().ooo_mmapped_chunks.clone();
        // Verify that we can access the chunks without error.
        for m in &chunks {
            let chk = h.chunk_disk_mapper.chunk(m.r#ref).unwrap();
            assert_eq!(m.num_samples as usize, chk.num_samples(), "{name}");
        }
        chunks
    };
    let exp_mmap_chunks = check(&h);
    assert_eq!(3, exp_mmap_chunks.len(), "{name}");

    // Restart head.
    restart_wal_wbl(&mut h, 0);

    // Get the mmap chunks from the Head.
    let act_mmap_chunks = check(&h);
    assert_eq!(exp_mmap_chunks, act_mmap_chunks, "{name}");

    h.close().unwrap();
}

/// TestWALReplayMmapsChunks is a regression test ensuring that chunks are mmapped during WAL replay, not
/// accumulated in the in-memory list.
#[test]
fn test_wal_replay_mmaps_chunks() {
    type AppendFn = fn(&mut dyn Appender, &Labels, i64) -> Result<SeriesRef, Error>;
    let cases: Vec<(&str, AppendFn)> = vec![
        ("floats", |app, l, ts| app.append(0, l, ts, ts as f64)),
        ("histograms", |app, l, ts| app.append_histogram(0, l, ts, Some(&tu::generate_test_histogram(ts)), None)),
        ("float histograms", |app, l, ts| {
            app.append_histogram(0, l, ts, None, Some(&tu::generate_test_float_histogram(ts)))
        }),
    ];
    for (name, append) in cases {
        let dir = TempDir::new();
        let wal = new_wl(dir.path(), "wal", compression::Type::Snappy);
        let mut h = own_head(dir, Some(wal), None, HeadOptions { chunk_range: 1000, ..Default::default() });
        h.init(0).unwrap();

        let l = ls(&["foo", "bar"]);

        // Append 250 samples at 1-minute intervals. With ChunkRange=1000 and DefaultSamplesPerChunk=120, this
        // creates multiple chunks that should be mmapped during WAL replay.
        let mut app = h.appender(&bg());
        for i in 0..250 {
            append(&mut *app, &l, i * MINUTE).unwrap();
        }
        commit_ok(&mut app);

        h.close().unwrap();

        // Remove mmapped chunk files so WAL replay must recreate all chunks.
        std::fs::remove_dir_all(h.dir.path().join("chunks_head")).unwrap();

        // Reopen Head — WAL replay happens in Init.
        let wal = new_wl(h.dir.path(), "wal", compression::Type::Snappy);
        let opts = clone_opts(&h.opts);
        h.head = Head::new(Some(wal.clone()), None, opts, None).unwrap();
        h.wal = Some(wal);
        h.init(0).unwrap();

        let (ms, created) = h.get_or_create(l.hash(), &l, false).unwrap();
        assert!(!created, "{name}");

        let g = ms.lock();
        // Chunks must be mmapped during replay, not left in the head chunk list.
        assert!(!g.mmapped_chunks.is_empty(), "{name}: expected chunks to be mmapped during WAL replay");
        assert_eq!(1, g.head_chunks.len(), "{name}: expected only one head chunk after replay");

        // Verify each mmapped chunk is readable from disk.
        for m in &g.mmapped_chunks {
            let chk = h.chunk_disk_mapper.chunk(m.r#ref).unwrap();
            assert_eq!(m.num_samples as usize, chk.num_samples(), "{name}");
        }
        drop(g);

        h.close().unwrap();
    }
}

const UNSUPPORTED_MASK: u8 = 0b1000_0000;
const ENC_UNSUPPORTED_XOR: chunkenc::Encoding = chunkenc::Encoding(chunkenc::ENC_XOR.0 | UNSUPPORTED_MASK);

/// Go `unsupportedChunk`: a XOR chunk whose encoding is unknown.
#[derive(Debug)]
struct UnsupportedChunk(chunkenc::XorChunk);

impl Chunk for UnsupportedChunk {
    fn bytes(&self) -> &[u8] {
        Chunk::bytes(&self.0)
    }
    fn encoding(&self) -> chunkenc::Encoding {
        ENC_UNSUPPORTED_XOR
    }
    fn appender(&mut self) -> Result<chunkenc::Appender, chunkenc::Error> {
        Chunk::appender(&mut self.0)
    }
    fn iterator<'a>(&'a self, it: Option<chunkenc::AnyIterator<'a>>) -> chunkenc::AnyIterator<'a> {
        Chunk::iterator(&self.0, it)
    }
    fn num_samples(&self) -> usize {
        Chunk::num_samples(&self.0)
    }
    fn compact(&mut self) {
        Chunk::compact(&mut self.0)
    }
    fn reset(&mut self, stream: Vec<u8>) {
        Chunk::reset(&mut self.0, stream)
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[test]
fn test_head_init_discard_chunks_with_unsupported_encoding() {
    let mut h = new_test_head("discard-unsupported", 1000, compression::Type::None, false);
    h.init(0).unwrap();

    let series_labels = ls(&["a", "1"]);
    let mut app = h.appender(&bg());
    let mut series_ref = 0;
    for i in 0..400 {
        series_ref = app.append(0, &series_labels, i, i as f64).unwrap();
    }
    commit_ok(&mut app);
    assert!(h.metrics.chunks_created.get() > 1.0);

    // Make this chunk not overlap with the previous and the next.
    let uc: Arc<dyn Chunk> = Arc::new(UnsupportedChunk(chunkenc::XorChunk::new()));
    h.chunk_disk_mapper.write_chunk(series_ref, 500, 600, uc, false, Some(Box::new(|e| assert!(e.is_ok()))));

    let mut app = h.appender(&bg());
    for i in 700..1200 {
        app.append(0, &series_labels, i, i as f64).unwrap();
    }
    commit_ok(&mut app);
    assert!(h.metrics.chunks_created.get() > 4.0);

    let (series, created) = h.get_or_create(series_labels.hash(), &series_labels, false).unwrap();
    assert!(!created, "should already exist");
    let exp_chunks = {
        let mut g = series.lock();
        g.mmap_chunks(&h.chunk_disk_mapper, series.r#ref);
        g.mmapped_chunks.clone()
    };

    h.close().unwrap();
    restart(&mut h, 0);

    let (series, created) = h.get_or_create(series_labels.hash(), &series_labels, false).unwrap();
    assert!(!created, "should already exist");
    assert_eq!(exp_chunks, series.lock().mmapped_chunks);
}

fn corrupt_bytes(path: &std::path::Path, off: u64, b: &[u8]) {
    use std::os::unix::fs::FileExt;
    let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.write_all_at(b, off).unwrap();
}

// Tests https://github.com/prometheus/prometheus/issues/10277.
#[test]
fn test_mmap_panic_after_mmap_replay_corruption() {
    let dir = TempDir::new();
    let new_opts = || {
        let opts = HeadOptions {
            chunk_range: tsdb::head::DEFAULT_BLOCK_DURATION,
            enable_exemplar_storage: true,
            ..Default::default()
        };
        opts.max_exemplars.store(100000, Ordering::SeqCst); // Go config.DefaultExemplarsConfig.MaxExemplars.
        opts
    };
    let wal = new_wl(dir.path(), "wal", compression::Type::None);
    let mut h = own_head(dir, Some(wal), None, new_opts());
    h.init(0).unwrap();

    let mut last_ts = 0i64;
    let mut r: SeriesRef = 0;
    let lbls = ls(&["__name__", "testing", "foo", "bar"]);
    let mut add_chunks = |h: &Head| {
        let interval = tsdb::head::DEFAULT_BLOCK_DURATION / (4 * 120);
        let mut app = h.appender(&bg());
        for i in 0..250 {
            // Go ignores the append error here.
            if let Ok(nr) = app.append(r, &lbls, last_ts, last_ts as f64) {
                r = nr;
            }
            last_ts += interval;
            if i % 10 == 0 {
                commit_ok(&mut app);
                app = h.appender(&bg());
            }
        }
        commit_ok(&mut app);
    };

    add_chunks(&h);

    h.close().unwrap();
    let wal = new_wl(h.dir.path(), "wal", compression::Type::None);

    corrupt_bytes(&h.dir.path().join("chunks_head").join("000001"), 17, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);

    let mut opts = new_opts();
    opts.chunk_dir_root = h.dir.path().to_path_buf();
    h.head = Head::new(Some(wal.clone()), None, opts, None).unwrap();
    h.wal = Some(wal);
    h.init(0).unwrap();

    add_chunks(&h);

    h.close().unwrap();
}

// Tests https://github.com/prometheus/prometheus/issues/10277.
#[test]
fn test_replay_after_mmap_replay_error() {
    let dir = TempDir::new();
    let open_head = |dir: &std::path::Path| -> Arc<Head> {
        let wal = new_wl(dir, "wal", compression::Type::None);
        let opts = HeadOptions {
            chunk_range: tsdb::head::DEFAULT_BLOCK_DURATION,
            chunk_dir_root: dir.to_path_buf(),
            ..Default::default()
        };
        opts.enable_memory_snapshot_on_shutdown.store(true, Ordering::SeqCst);
        opts.max_exemplars.store(100000, Ordering::SeqCst); // Go config.DefaultExemplarsConfig.MaxExemplars.
        let h = Head::new(Some(wal), None, opts, None).unwrap();
        h.init(0).unwrap();
        h
    };

    let mut h = open_head(dir.path());

    let itvl = 15 * 1000;
    let mut last_ts = 0i64;
    let lbls = ls(&["__name__", "testing", "foo", "bar"]);
    let mut exp_samples: Vec<Sample> = Vec::new();
    let mut add_samples = |h: &Head, num_samples: usize| {
        let mut app = h.appender(&bg());
        let mut r: SeriesRef = 0;
        for i in 0..num_samples {
            r = app.append(r, &lbls, last_ts, last_ts as f64).unwrap();
            exp_samples.push(Sample::float(0, last_ts, last_ts as f64));
            last_ts += itvl;
            if i % 10 == 0 {
                commit_ok(&mut app);
                app = h.appender(&bg());
            }
        }
        commit_ok(&mut app);
    };

    // Creating multiple m-map files.
    for i in 0..5 {
        add_samples(&h, 250);
        h.close().unwrap();
        if i != 4 {
            // Don't open head for the last iteration.
            h = open_head(dir.path());
        }
    }

    let count_files = || std::fs::read_dir(dir.path().join("chunks_head")).unwrap().count();
    assert_eq!(5, count_files());

    // Corrupt a m-map file.
    corrupt_bytes(&dir.path().join("chunks_head").join("000002"), 17, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);

    h = open_head(dir.path());
    h.mmap_head_chunks();

    // There should be less m-map files due to corruption.
    assert_eq!(2, count_files());

    // Querying should not panic.
    let res = query_head(&h, 0, last_ts, &[m(MatchType::Equal, "__name__", "testing")]);
    assert_eq!(BTreeMap::from([(lbls.to_string(), exp_samples)]), res);

    h.close().unwrap();
}

#[test]
fn test_ooo_append_with_no_series() {
    for (name, scenario) in crate::common::sample_type_scenarios() {
        ooo_append_with_no_series_case(name, scenario);
    }
}

fn ooo_append_with_no_series_case(name: &str, scenario: crate::common::SampleTypeScenario) {
    let opts = HeadOptions::default();
    opts.out_of_order_cap_max.store(30, Ordering::SeqCst);
    opts.out_of_order_time_window.store(120 * MINUTE, Ordering::SeqCst);
    let h = wal_wbl_head(TempDir::new(), opts);
    h.init(0).unwrap();

    let append_sample = |lbls: &Labels, ts: i64| {
        let mut app = h.appender(&bg());
        scenario.append(&mut *app, lbls, ts * MINUTE, ts).0.unwrap();
        commit_ok(&mut app);
    };

    let verify_ooo_samples = |lbls: &Labels, exp_samples: usize| {
        let (ms, created) = h.get_or_create(lbls.hash(), lbls, false).unwrap();
        assert!(!created, "{name}");
        let g = ms.lock();
        assert!(g.head_chunks.is_empty(), "{name}");
        let ooo = g.ooo().expect("ooo");
        assert_eq!(exp_samples, ooo.ooo_head_chunk.as_ref().expect("ooo head chunk").chunk.num_samples(), "{name}");
    };

    let verify_in_order_samples = |lbls: &Labels, exp_samples: usize| {
        let (ms, created) = h.get_or_create(lbls.hash(), lbls, false).unwrap();
        assert!(!created, "{name}");
        let g = ms.lock();
        assert!(g.ooo().is_none(), "{name}");
        assert_eq!(exp_samples, g.head_chunk().expect("head chunk").chunk.num_samples(), "{name}");
    };

    let new_labels = |idx: i32| ls(&["foo", &idx.to_string()]);

    let s1 = new_labels(1);
    append_sample(&s1, 300); // At 300m.
    verify_in_order_samples(&s1, 1);

    // At 239m, the sample cannot be appended to in-order chunk since it is beyond the minValidTime. So it should go
    // in OOO chunk. Series does not exist for s2 yet.
    let s2 = new_labels(2);
    append_sample(&s2, 239); // OOO sample.
    verify_ooo_samples(&s2, 1);

    // Similar for 180m.
    let s3 = new_labels(3);
    append_sample(&s3, 180); // OOO sample.
    verify_ooo_samples(&s3, 1);

    // Now 179m is too old.
    let s4 = new_labels(4);
    let mut app = h.appender(&bg());
    let (r, _) = scenario.append(&mut *app, &s4, 179 * MINUTE, 179);
    assert_eq!(Some(storage::ERR_TOO_OLD_SAMPLE), r.err(), "{name}");
    assert!(app.rollback().is_none());
    verify_ooo_samples(&s3, 1);

    // Samples still go into in-order chunk for samples within appendable minValidTime.
    let s5 = new_labels(5);
    append_sample(&s5, 240);
    verify_in_order_samples(&s5, 1);
}

#[test]
fn test_head_min_ooo_time_update() {
    for (_, scenario) in crate::common::sample_type_scenarios() {
        if scenario.sample_type == "float" {
            head_min_ooo_time_update_case(scenario);
        }
    }
}

fn head_min_ooo_time_update_case(scenario: crate::common::SampleTypeScenario) {
    let opts = HeadOptions::default();
    opts.out_of_order_time_window.store(10 * MINUTE, Ordering::SeqCst);
    let h = wal_wbl_head(TempDir::new(), opts);
    h.init(0).unwrap();

    let append_sample = |ts: i64| {
        let mut app = h.appender(&bg());
        scenario.append(&mut *app, &ls(&["a", "b"]), ts * MINUTE, ts).0.unwrap();
        commit_ok(&mut app);
    };

    append_sample(300); // In-order sample.
    assert_eq!(i64::MAX, h.min_ooo_time());

    append_sample(295); // OOO sample.
    assert_eq!(295 * MINUTE, h.min_ooo_time());

    // Allowed window for OOO is >=290, which is before the earliest ooo sample 295, so it gets set to the lower
    // value.
    h.truncate_ooo(0, tsdb_format::chunks::ChunkDiskMapperRef(1)).unwrap();
    assert_eq!(290 * MINUTE, h.min_ooo_time());

    append_sample(310); // In-order sample.
    append_sample(305); // OOO sample.
    assert_eq!(290 * MINUTE, h.min_ooo_time());

    // Now the OOO sample 295 was not gc'ed yet. And allowed window for OOO is now >=300. So the lowest among them,
    // 295, is set as minOOOTime.
    h.truncate_ooo(0, tsdb_format::chunks::ChunkDiskMapperRef(2)).unwrap();
    assert_eq!(295 * MINUTE, h.min_ooo_time());
}

/// The records of a WAL (Go `readTestWAL`'s `[]any`).
#[derive(Clone, Debug, PartialEq)]
enum Rec {
    Series(Vec<tsdb_format::record::RefSeries>),
    Samples(Vec<tsdb_format::record::RefSample>),
    Histograms(Vec<tsdb_format::record::RefHistogramSample>),
    FloatHistograms(Vec<tsdb_format::record::RefFloatHistogramSample>),
    Tombstones(Vec<Stone>),
    Metadata(Vec<tsdb_format::record::RefMetadata>),
    Exemplars(Vec<tsdb_format::record::RefExemplar>),
}

/// Go `readTestWAL`.
fn read_test_wal(dir: &std::path::Path) -> Vec<Rec> {
    use tsdb_format::record::Type;
    let sr = wlog::new_segments_reader(dir).unwrap();
    let mut dec = tsdb_format::record::Decoder::new(tsdb::Logger::default());
    let mut r = wlog::Reader::new(sr);
    let mut recs = Vec::new();
    while r.next() {
        let rec = r.record();
        match dec.r#type(rec) {
            Type::Series => {
                let mut v = Vec::new();
                dec.series(rec, &mut v).unwrap();
                recs.push(Rec::Series(v));
            }
            Type::Samples | Type::SamplesV2 => {
                let mut v = Vec::new();
                dec.samples(rec, &mut v).unwrap();
                recs.push(Rec::Samples(v));
            }
            Type::HistogramSamples | Type::CustomBucketsHistogramSamples | Type::HistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.histogram_samples(rec, &mut v).unwrap();
                recs.push(Rec::Histograms(v));
            }
            Type::FloatHistogramSamples | Type::CustomBucketsFloatHistogramSamples | Type::FloatHistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.float_histogram_samples(rec, &mut v).unwrap();
                recs.push(Rec::FloatHistograms(v));
            }
            Type::Tombstones => {
                let mut v = Vec::new();
                dec.tombstones(rec, &mut v).unwrap();
                recs.push(Rec::Tombstones(v));
            }
            Type::Metadata => {
                let mut v = Vec::new();
                dec.metadata(rec, &mut v).unwrap();
                recs.push(Rec::Metadata(v));
            }
            Type::Exemplars => {
                let mut v = Vec::new();
                dec.exemplars(rec, &mut v).unwrap();
                recs.push(Rec::Exemplars(v));
            }
            Type::MinValidTime => {
                // Internal checkpoint bookkeeping, not WAL content callers care about.
                dec.min_valid_time(rec).unwrap();
            }
            t => panic!("unknown record type {t:?}"),
        }
    }
    assert!(r.err().is_none());
    recs
}

#[test]
fn test_gauge_histogram_wal_and_chunk_header() {
    gauge_histogram_wal_and_chunk_header(false);
}

#[test]
fn test_gauge_float_histogram_wal_and_chunk_header() {
    gauge_histogram_wal_and_chunk_header(true);
}

/// Go `TestGaugeHistogramWALAndChunkHeader` (`float` = false) and `TestGaugeFloatHistogramWALAndChunkHeader`.
fn gauge_histogram_wal_and_chunk_header(float: bool) {
    let l = ls(&["a", "b"]);
    let mut head = new_test_head("gauge-hist-wal", 1000, compression::Type::None, false);
    head.init(0).unwrap();

    let mut hists = tu::generate_test_gauge_histograms(5);
    let mut fhists = tu::generate_test_gauge_float_histograms(5);
    hists[0].counter_reset_hint = CounterResetHint::UnknownCounterReset;
    fhists[0].counter_reset_hint = CounterResetHint::UnknownCounterReset;
    hists[3].counter_reset_hint = CounterResetHint::UnknownCounterReset;
    fhists[3].counter_reset_hint = CounterResetHint::UnknownCounterReset;

    let mut ts = 0i64;
    let mut append_histogram = |head: &Head, i: usize| {
        ts += 1;
        let mut app = head.appender(&bg());
        if float {
            app.append_histogram(0, &l, ts, None, Some(&fhists[i].clone())).unwrap();
        } else {
            app.append_histogram(0, &l, ts, Some(&hists[i].clone()), None).unwrap();
        }
        commit_ok(&mut app);
    };
    for i in [0, 1, 2, 3, 3, 4] {
        append_histogram(&head, i);
    }

    let check_headers = |head: &Head| {
        head.mmap_head_chunks();
        let (ms, _) = head.get_or_create(l.hash(), &l, false).unwrap();
        let g = ms.lock();
        assert_eq!(3, g.mmapped_chunks.len());
        let exp_headers = if float {
            [
                CounterResetHeader::UNKNOWN_COUNTER_RESET,
                CounterResetHeader::GAUGE_TYPE,
                CounterResetHeader::UNKNOWN_COUNTER_RESET,
                CounterResetHeader::GAUGE_TYPE,
            ]
        } else {
            [
                CounterResetHeader::UNKNOWN_COUNTER_RESET,
                CounterResetHeader::GAUGE_TYPE,
                CounterResetHeader::NOT_COUNTER_RESET,
                CounterResetHeader::GAUGE_TYPE,
            ]
        };
        for (i, mmap_chunk) in g.mmapped_chunks.iter().enumerate() {
            let chk = head.chunk_disk_mapper.chunk(mmap_chunk.r#ref).unwrap();
            assert_eq!(exp_headers[i], counter_reset_header(&*chk, float, false));
        }
        assert_eq!(exp_headers[3], counter_reset_header(&*g.head_chunk().unwrap().chunk, float, false));
    };
    check_headers(&head);

    let recs = read_test_wal(head.wal().unwrap().dir());
    let mut exp = vec![Rec::Series(vec![tsdb_format::record::RefSeries { r#ref: 1, labels: ls(&["a", "b"]) }])];
    for (t, i) in [(1, 0), (2, 1), (3, 2), (4, 3), (5, 3), (6, 4)] {
        if float {
            exp.push(Rec::FloatHistograms(vec![tsdb_format::record::RefFloatHistogramSample {
                r#ref: 1,
                st: 0,
                t,
                fh: Arc::new(fhists[i].clone()),
            }]));
        } else {
            exp.push(Rec::Histograms(vec![tsdb_format::record::RefHistogramSample {
                r#ref: 1,
                st: 0,
                t,
                h: Arc::new(hists[i].clone()),
            }]));
        }
    }
    assert_eq!(exp, recs);

    // Restart Head without mmap chunks to expect the WAL replay to recognize gauge histograms.
    head.close().unwrap();
    std::fs::remove_dir_all(tsdb::head::mmapped_chunks_dir(&head.opts.chunk_dir_root)).unwrap();
    restart(&mut head, 0);

    check_headers(&head);
}

#[test]
fn test_snapshot_ahead_of_wal_error() {
    let mut head = new_test_head("snapshot-ahead-of-wal", 120 * 4, compression::Type::None, false);
    head.opts.enable_memory_snapshot_on_shutdown.store(true, Ordering::SeqCst);
    // Add a sample to fill WAL.
    let mut app = head.appender(&bg());
    app.append(0, &ls(&["foo", "bar"]), 10, 10.0).unwrap();
    commit_ok(&mut app);

    // Increment snapshot index to create sufficiently large difference.
    for _ in 0..2 {
        head.wal().unwrap().next_segment().unwrap();
    }
    head.close().unwrap(); // This will create a snapshot.

    let (_, idx, _) = tsdb::head_wal::last_chunk_snapshot(&head.opts.chunk_dir_root).unwrap();
    assert_eq!(2, idx);

    // Restart the WAL while keeping the old snapshot. The new head is created manually in this case in order to
    // keep using the same snapshot directory instead of a random one.
    let wal_dir = head.wal().unwrap().dir().to_path_buf();
    std::fs::remove_dir_all(&wal_dir).unwrap();
    head.opts.enable_memory_snapshot_on_shutdown.store(false, Ordering::SeqCst);
    let opts = clone_opts(&head.opts);
    reopen(&mut head, opts, None).unwrap();
    // Add a sample to fill WAL.
    let mut app = head.appender(&bg());
    app.append(0, &ls(&["foo", "bar"]), 10, 10.0).unwrap();
    commit_ok(&mut app);
    let (last_segment, _) = head.wal().unwrap().last_segment_and_offset().unwrap();
    assert_eq!(0, last_segment);
    head.close().unwrap();

    // New WAL is saved, but old snapshot still exists.
    let (_, idx, _) = tsdb::head_wal::last_chunk_snapshot(&head.opts.chunk_dir_root).unwrap();
    assert_eq!(2, idx);

    // Create new Head which should detect the incorrect index and delete the snapshot.
    head.opts.enable_memory_snapshot_on_shutdown.store(true, Ordering::SeqCst);
    restart(&mut head, i64::MIN);

    // Verify that snapshot directory does not exist anymore.
    let err = tsdb::head_wal::last_chunk_snapshot(&head.opts.chunk_dir_root).unwrap_err();
    assert!(err.is(&storage::ERR_NOT_FOUND), "{err}");
    assert_eq!(tsdb_format::record::ERR_NOT_FOUND, err.to_string());

    head.opts.enable_memory_snapshot_on_shutdown.store(false, Ordering::SeqCst);
    head.close().unwrap();
}

#[test]
fn test_cutting_new_head_chunks() {
    let ctx = bg();
    type FloatFn = Box<dyn Fn(usize) -> f64>;
    type HistFn = Box<dyn Fn(usize) -> Histogram>;
    let big = |n: usize, b: usize| -> HistFn {
        let hists = model::histogram::generate_big_test_histograms(n, b);
        Box::new(move |i| hists[i].clone())
    };
    let cases: Vec<(&str, usize, bool, Option<FloatFn>, Option<HistFn>, Vec<(usize, usize)>)> = vec![
        ("float samples", 180, false, Some(Box::new(|_| 1.0)), None, vec![(120, 46), (60, 32)]),
        (
            // Normally 120 samples would fit into a single chunk but these chunks violate the 1005 byte soft cap.
            "large float samples",
            120,
            true,
            // Flipping between these two make each sample val take at least 64 bits.
            Some(Box::new(|i| [f64::MAX, 0.0][i % 2])),
            None,
            vec![(99, 1008), (21, 219)],
        ),
        ("small histograms", 240, false, None, Some(big(240, 10)), vec![(120, 1087), (120, 1039)]),
        (
            "large histograms",
            240,
            false,
            None,
            Some(big(240, 100)),
            vec![(40, 896), (40, 899), (40, 896), (30, 690), (30, 691), (30, 694), (30, 693)],
        ),
        (
            // Really large histograms; each chunk can only contain a single histogram but we have a 10 sample
            // minimum per chunk.
            "really large histograms",
            11,
            false,
            None,
            Some(big(11, 100000)),
            vec![(10, 200103), (1, 87540)],
        ),
    ];
    for (name, num_total_samples, timestamp_jitter, float_val, hist_val, expected_chks) in cases {
        let h = new_test_head("cutting-new-head-chunks", tsdb::head::DEFAULT_BLOCK_DURATION, compression::Type::None, false);
        let mut a = h.appender(&bg());

        let mut ts = 10000i64;
        let lbls = ls(&["foo", "bar"]);
        let jitter = [0i64, 1]; // A bit of jitter to prevent dod=0.

        for i in 0..num_total_samples {
            if let Some(f) = &float_val {
                a.append(0, &lbls, ts, f(i)).unwrap();
            } else if let Some(hf) = &hist_val {
                a.append_histogram(0, &lbls, ts, Some(&hf(i)), None).unwrap();
            }
            ts += 60 * 1000;
            if timestamp_jitter {
                ts += jitter[i % jitter.len()];
            }
        }
        commit_ok(&mut a);

        let idx_reader = h.index();
        let chk_reader = h.chunks().unwrap();
        let mut p = idx_reader.postings(&ctx, "foo", &["bar"]).unwrap();
        let mut lbl_builder = ScratchBuilder::default();
        while p.next() {
            let s_ref = p.at();
            let mut chk_metas = Vec::new();
            idx_reader.series(s_ref, &mut lbl_builder, &mut chk_metas).unwrap();
            assert_eq!(expected_chks.len(), chk_metas.len(), "{name}");
            for (i, (num_samples, num_bytes)) in expected_chks.iter().enumerate() {
                let chk = match tsdb::ChunkReader::chunk_or_iterable(&chk_reader, &chk_metas[i]).unwrap() {
                    tsdb::ChunkOrIterable::Chunk(c) => c,
                    tsdb::ChunkOrIterable::Iterable(_) => panic!("{name}: unexpected iterable"),
                };
                assert_eq!(*num_samples, chk.num_samples(), "{name} chunk {i}");
                assert_eq!(*num_bytes, chk.bytes().len(), "{name} chunk {i}");
            }
        }
    }
}

/// TestHeadDetectsDuplicateSampleAtSizeLimit tests a regression where a duplicate sample is appended to the head,
/// right when the head chunk is at the size limit. The test adds all samples as duplicate, thus expecting that
/// the result has exactly half of the samples.
#[test]
fn test_head_detects_duplicate_sample_at_size_limit() {
    let num_samples = 1000;
    let base_ts = 1695209650i64;

    let h = new_test_head("duplicate-at-size-limit", tsdb::head::DEFAULT_BLOCK_DURATION, compression::Type::None, false);
    let mut a = h.appender(&bg());
    // Use the worst case scenario for the XOR encoding. Otherwise we hit the sample limit before the size limit.
    let vals = [f64::MAX, 0.0];
    for i in 0..num_samples {
        let ts = base_ts + (i / 2) as i64 * 10000;
        let _ = a.append(0, &ls(&["foo", "bar"]), ts, vals[(i / 2) % vals.len()]);
        commit_ok(&mut a);
        a = h.appender(&bg());
    }

    let index_reader = h.index();
    let mut chunks = Vec::new();
    let mut builder = ScratchBuilder::default();
    index_reader.series(1, &mut builder, &mut chunks).unwrap();

    let chunk_reader = h.chunks().unwrap();
    let mut stored_sample_count = 0;
    for chunk_meta in &chunks {
        match tsdb::ChunkReader::chunk_or_iterable(&chunk_reader, chunk_meta).unwrap() {
            tsdb::ChunkOrIterable::Chunk(c) => stored_sample_count += c.num_samples(),
            tsdb::ChunkOrIterable::Iterable(_) => panic!("unexpected iterable"),
        }
    }

    assert_eq!(num_samples / 2, stored_sample_count);
}

#[test]
fn test_wal_sample_and_exemplar_order() {
    let lbls = ls(&["foo", "bar"]);
    type AppendF = fn(&mut dyn Appender, &Labels, i64) -> Result<SeriesRef, Error>;
    let cases: Vec<(&str, AppendF, fn(&Rec) -> bool)> = vec![
        ("float sample", |app, l, ts| app.append(0, l, ts, 1.0), |r| matches!(r, Rec::Samples(_))),
        (
            "histogram sample",
            |app, l, ts| app.append_histogram(0, l, ts, Some(&tu::generate_test_histogram(1)), None),
            |r| matches!(r, Rec::Histograms(_)),
        ),
        (
            "float histogram sample",
            |app, l, ts| app.append_histogram(0, l, ts, None, Some(&tu::generate_test_float_histogram(1))),
            |r| matches!(r, Rec::FloatHistograms(_)),
        ),
    ];
    for (name, append_f, expected_type) in cases {
        let h = new_test_head("wal-sample-exemplar-order", 1000, compression::Type::None, false);

        let mut app = h.appender(&bg());
        let r = append_f(&mut *app, &lbls, 10).unwrap();
        let _ = app.append_exemplar(r, &lbls, &Exemplar { value: 1.0, ts: 5, ..Default::default() });
        let _ = app.commit();

        let recs = read_test_wal(h.wal.as_ref().unwrap().dir());
        assert_eq!(3, recs.len(), "{name}");
        assert!(matches!(recs[0], Rec::Series(_)), "{name}: expected first record to be a RefSeries");
        assert!(expected_type(&recs[1]), "{name}: unexpected second record {:?}", recs[1]);
        assert!(matches!(recs[2], Rec::Exemplars(_)), "{name}: expected third record to be a RefExemplar");
    }
}


// ---------------------------------------------------------------------------------------------------------------
// Tests that need the DB.

use crate::common::TestDb;

/// Go `Open(t.TempDir(), nil, nil, opts, nil)` with `t.Cleanup(db.Close)`.
fn open_db(opts: tsdb::db::Options) -> TestDb {
    let tmp = TempDir::new();
    let db = tsdb::db::open(tmp.path(), None, Some(opts), None).expect("open db");
    TestDb { db, tmp: Some(tmp) }
}

const DEFAULT_BLOCK_DURATION: i64 = tsdb::head::DEFAULT_BLOCK_DURATION;

#[test]
fn test_out_of_order_samples_metric() {
    for (_, scenario) in crate::common::sample_type_scenarios() {
        out_of_order_samples_metric_case(scenario, tsdb::db::default_options(), storage::ERR_OUT_OF_ORDER_SAMPLE);
    }
}

#[test]
fn test_out_of_order_samples_metric_native_histogram_ooo_disabled() {
    for (_, scenario) in crate::common::sample_type_scenarios() {
        if scenario.sample_type != "histogram" {
            continue;
        }
        let options = tsdb::db::Options { out_of_order_time_window: 0, ..tsdb::db::default_options() };
        out_of_order_samples_metric_case(scenario, options, storage::ERR_OUT_OF_ORDER_SAMPLE);
    }
}

fn out_of_order_samples_metric_case(
    scenario: crate::common::SampleTypeScenario,
    options: tsdb::db::Options,
    expect_out_of_order_error: Error,
) {
    let db = open_db(options);
    db.disable_compactions();
    let typ = scenario.sample_type;
    let l = ls(&["a", "b"]);
    let append_sample = |app: &mut Box<dyn Appender>, ts: i64| scenario.append(&mut **app, &l, ts, 99).0;
    let ooo = |db: &TestDb| db.head().metrics.out_of_order_samples.with_label_values(typ).get();
    let oob = |db: &TestDb| db.head().metrics.out_of_bound_samples.with_label_values(typ).get();
    let ctx = bg();

    let mut app = db.appender(&ctx);
    for i in 1..=5 {
        append_sample(&mut app, i).unwrap();
    }
    commit_ok(&mut app);

    // Test out of order metric.
    assert_eq!(0.0, ooo(&db));
    let mut app = db.appender(&ctx);
    assert_eq!(Some(expect_out_of_order_error.clone()), append_sample(&mut app, 2).err());
    assert_eq!(1.0, ooo(&db));
    assert_eq!(Some(expect_out_of_order_error.clone()), append_sample(&mut app, 3).err());
    assert_eq!(2.0, ooo(&db));
    assert_eq!(Some(expect_out_of_order_error.clone()), append_sample(&mut app, 4).err());
    assert_eq!(3.0, ooo(&db));
    commit_ok(&mut app);

    // Compact Head to test out of bound metric.
    let mut app = db.appender(&ctx);
    append_sample(&mut app, DEFAULT_BLOCK_DURATION * 2).unwrap();
    commit_ok(&mut app);

    assert_eq!(i64::MIN, db.head().min_valid_time());
    db.compact(&ctx).unwrap();
    assert!(db.head().min_valid_time() > 0);

    let mut app = db.appender(&ctx);
    assert_eq!(Some(storage::ERR_OUT_OF_BOUNDS), append_sample(&mut app, db.head().min_valid_time() - 2).err());
    assert_eq!(1.0, oob(&db));
    assert_eq!(Some(storage::ERR_OUT_OF_BOUNDS), append_sample(&mut app, db.head().min_valid_time() - 1).err());
    assert_eq!(2.0, oob(&db));
    commit_ok(&mut app);

    // Some more valid samples for out of order.
    let mut app = db.appender(&ctx);
    for i in 1..=5 {
        append_sample(&mut app, db.head().min_valid_time() + DEFAULT_BLOCK_DURATION + i).unwrap();
    }
    commit_ok(&mut app);

    // Test out of order metric.
    let mut app = db.appender(&ctx);
    for (d, exp) in [(2, 4.0), (3, 5.0), (4, 6.0)] {
        let r = append_sample(&mut app, db.head().min_valid_time() + DEFAULT_BLOCK_DURATION + d);
        assert_eq!(Some(expect_out_of_order_error.clone()), r.err());
        assert_eq!(exp, ooo(&db));
    }
    commit_ok(&mut app);
}

/// Appends samples spanning 1.5 block ranges (7 chunks with 15s scrape interval) of series {a="b"}; returns the
/// expected samples and the max time.
fn append_one_and_a_half_blocks(db: &TestDb) -> (Vec<Sample>, i64) {
    let mut app = db.appender(&bg());
    let mut r: SeriesRef = 0;
    let mut maxt = 0;
    let mut exp_samples = Vec::new();
    for i in 0..=120 * 7i64 {
        let ts = i * DEFAULT_BLOCK_DURATION / (4 * 120);
        r = app.append(r, &ls(&["a", "b"]), ts, i as f64).unwrap();
        maxt = ts;
        exp_samples.push(Sample::float(0, ts, i as f64));
    }
    commit_ok(&mut app);
    (exp_samples, maxt)
}

// Tests https://github.com/prometheus/prometheus/issues/8221.
#[test]
fn test_chunk_not_found_head_gc_race() {
    let db = crate::common::new_test_db(vec![]);
    db.disable_compactions();
    let (_, maxt) = append_one_and_a_half_blocks(&db);
    let mint = 0;

    // Get a querier before compaction (or when compaction is about to begin).
    let mut q = db.querier(mint, maxt).unwrap();

    // Query the compacted range and get the first series before compaction.
    {
        let mut ss = q.select(&bg(), true, None, &[m(MatchType::Equal, "a", "b")]);
        assert!(ss.next());
        let s = ss.at().clone();

        let db2 = db.db.clone();
        let wg = std::thread::spawn(move || {
            // Compacting head while the querier spans the compaction time.
            db2.compact(&bg()).unwrap();
            assert!(!db2.blocks().is_empty());
        });

        // Give enough time for compaction to finish. We expect it to be blocked until querier is closed.
        std::thread::sleep(std::time::Duration::from_secs(3));

        // Now consume after compaction when it's gone.
        {
            let mut it = s.iterator(None);
            while it.next() == ValueType::Float {
                let _ = it.at();
            }
            // It should error here without any fix for the mentioned issue.
            assert!(it.err().is_none());
        }
        while ss.next() {
            let s = ss.at().clone();
            let mut it = s.iterator(None);
            while it.next() == ValueType::Float {
                let _ = it.at();
            }
            assert!(it.err().is_none());
        }
        assert!(ss.err().is_none());
        drop(ss);
        assert!(q.close().is_none());
        wg.join().unwrap();
    }
}

// Tests https://github.com/prometheus/prometheus/issues/9079.
#[test]
fn test_data_missing_on_query_during_compaction() {
    let db = crate::common::new_test_db(vec![]);
    db.disable_compactions();
    let (exp_samples, maxt) = append_one_and_a_half_blocks(&db);

    // Get a querier before compaction (or when compaction is about to begin).
    let q = db.querier(0, maxt).unwrap();

    let db2 = db.db.clone();
    let wg = std::thread::spawn(move || {
        // Compacting head while the querier spans the compaction time.
        db2.compact(&bg()).unwrap();
        assert!(!db2.blocks().is_empty());
    });

    // Give enough time for compaction to finish. We expect it to be blocked until querier is closed.
    std::thread::sleep(std::time::Duration::from_secs(3));

    // Querying the querier that was got before compaction.
    let series = query(q, &[m(MatchType::Equal, "a", "b")]);
    assert_eq!(BTreeMap::from([("{a=\"b\"}".to_string(), exp_samples)]), series);

    wg.join().unwrap();
}

#[test]
fn test_is_querier_colliding_with_truncation() {
    let db = crate::common::new_test_db(vec![]);
    db.disable_compactions();

    let mut app = db.appender(&bg());
    let mut r: SeriesRef = 0;
    for i in 0..=3000i64 {
        r = app.append(r, &ls(&["a", "b"]), i, i as f64).unwrap();
    }
    commit_ok(&mut app);

    // This mocks truncation.
    db.head().mem_truncation_in_process.store(true, Ordering::SeqCst);
    db.head().last_memory_truncation_time.store(2000, Ordering::SeqCst);

    // Test that IsQuerierValid suggests correct querier ranges: (mint, maxt, expShouldClose, expGetNew, expNewMint).
    let cases: [(i64, i64, bool, bool, i64); 5] = [
        // Entirely below the truncation point: close without reopening, but newMint is still the truncation point
        // so callers (e.g. the OOO wrapper) clamp their in-order read to it.
        (-200, -100, true, false, 2000),
        (-200, 300, true, false, 2000),
        (100, 1900, true, false, 2000),
        // Straddles the truncation point: close and reopen at the truncation point.
        (1900, 2200, true, true, 2000),
        // At/above the truncation point: no collision.
        (2000, 2500, false, false, 0),
    ];
    for (mint, maxt, exp_should_close, exp_get_new, exp_new_mint) in cases {
        let (should_close, get_new, new_mint) = db.head().is_querier_colliding_with_truncation(mint, maxt);
        assert_eq!(exp_should_close, should_close, "mint={mint},maxt={maxt}");
        assert_eq!(exp_get_new, get_new, "mint={mint},maxt={maxt}");
        if should_close || get_new {
            assert_eq!(exp_new_mint, new_mint, "mint={mint},maxt={maxt}");
        }
    }
    db.head().mem_truncation_in_process.store(false, Ordering::SeqCst);
}

/// Starts `WaitForPendingReadersInTimeRange(mint, maxt)` in a thread; the flag is set once it returns.
fn spawn_wait(head: Arc<Head>, mint: i64, maxt: i64) -> (Arc<AtomicBool>, std::thread::JoinHandle<()>) {
    let wait_over = Arc::new(AtomicBool::new(false));
    let w = wait_over.clone();
    let h = std::thread::spawn(move || {
        head.wait_for_pending_readers_in_time_range(mint, maxt);
        w.store(true, Ordering::SeqCst);
    });
    (wait_over, h)
}

/// Waits up to `d` for the flag to be set.
fn wait_flag(f: &AtomicBool, d: std::time::Duration) -> bool {
    let start = std::time::Instant::now();
    while !f.load(Ordering::SeqCst) {
        if start.elapsed() > d {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    true
}

/// A querier or a chunk querier (both are Go `io.Closer`s and `storage.LabelQuerier`s).
enum AnyQuerier {
    Samples(storage::BoxQuerier),
    Chunks(storage::BoxChunkQuerier),
}

impl AnyQuerier {
    fn lq(&mut self) -> &mut dyn storage::LabelQuerier {
        match self {
            AnyQuerier::Samples(q) => &mut **q,
            AnyQuerier::Chunks(q) => &mut **q,
        }
    }
    fn close(&mut self) -> Option<Error> {
        self.lq().close()
    }
}

#[test]
fn test_wait_for_pending_readers_in_time_range() {
    let db = crate::common::new_test_db(vec![]);
    db.disable_compactions();

    let sample_ts = |i: i64| i * DEFAULT_BLOCK_DURATION / (4 * 120);
    let mut app = db.appender(&bg());
    let mut r: SeriesRef = 0;
    for i in 0..=3000i64 {
        r = app.append(r, &ls(&["a", "b"]), sample_ts(i), i as f64).unwrap();
    }
    commit_ok(&mut app);

    let (trunc_mint, trunc_maxt) = (1000i64, 2000i64);
    let cases = [
        (0, 500, false),     // Before truncation range.
        (500, 1500, true),   // Overlaps with truncation at the start.
        (1200, 1700, true),  // Within truncation range.
        (1800, 2500, true),  // Overlaps with truncation at the end.
        (2000, 2500, false), // After truncation range.
        (2100, 2500, false), // After truncation range.
    ];
    for (mint, maxt, should_wait) in cases {
        let name = format!("mint={mint},maxt={maxt},shouldWait={should_wait}");
        // adapted: Go uses synctest (fake time) to see whether the wait blocks; here real threads and short real
        // waits check the same: the wait must block while an overlapping querier is open and return (within the
        // 500ms poll interval) once it is closed, or return at once when there is no overlap.
        let check_waiting = |mut cl: AnyQuerier| {
            let (wait_over, h) = spawn_wait(db.head().clone(), trunc_mint, trunc_maxt);
            if should_wait {
                std::thread::sleep(std::time::Duration::from_millis(200));
                assert!(
                    !wait_over.load(Ordering::SeqCst),
                    "{name}: WaitForPendingReadersInTimeRange should block while overlapping querier is open"
                );
                assert!(cl.close().is_none());
                assert!(
                    wait_flag(&wait_over, std::time::Duration::from_secs(2)),
                    "{name}: WaitForPendingReadersInTimeRange should complete after querier is closed"
                );
            } else {
                assert!(
                    wait_flag(&wait_over, std::time::Duration::from_millis(400)),
                    "{name}: WaitForPendingReadersInTimeRange should return immediately when no overlap"
                );
                assert!(cl.close().is_none());
            }
            h.join().unwrap();
        };

        check_waiting(AnyQuerier::Samples(db.querier(mint, maxt).unwrap()));
        check_waiting(AnyQuerier::Chunks(db.chunk_querier(mint, maxt).unwrap()));
    }
}

/// TestQuerierDoesNotBlockHeadTruncation verifies that a querier opened during an in-progress head truncation does
/// not hold up the truncation for the range below the truncation point, for the plain in-order querier and the
/// head+OOO querier.
#[test]
fn test_querier_does_not_block_head_truncation() {
    const MAX_T: i64 = 3000;
    const TRUNC_TIME: i64 = 2000;

    // newDB builds a head with in-order samples spanning [0,maxT). When withOOO is set it also appends an OOO
    // range over the same period, which forces db.Querier down the head+OOO path.
    let new_db = |with_ooo: bool| {
        let db = open_db(tsdb::db::Options { out_of_order_time_window: MAX_T, ..tsdb::db::default_options() });
        db.disable_compactions();

        let mut app = db.appender(&bg());
        let mut r: SeriesRef = 0;
        for i in (0..MAX_T).step_by(100) {
            r = app.append(r, &ls(&["a", "b"]), i, i as f64).unwrap();
        }
        if with_ooo {
            for i in (50..MAX_T).step_by(100) {
                app.append(r, &ls(&["a", "b"]), i, i as f64).unwrap();
            }
        }
        commit_ok(&mut app);

        // Mock a head memory truncation in progress up to truncTime. At this point the block for [headMin,
        // truncTime) has already been written and reloaded, so any querier whose range falls below truncTime must
        // read from the block and must NOT pin the head.
        db.head().last_memory_truncation_time.store(TRUNC_TIME, Ordering::SeqCst);
        db.head().mem_truncation_in_process.store(true, Ordering::SeqCst);
        db
    };

    // adapted: Go uses synctest (fake time); here a real thread and a short real wait.
    let assert_does_not_block = |db: &TestDb, mut cl: AnyQuerier, name: &str| {
        let (wait_over, h) = spawn_wait(db.head().clone(), db.head().min_time(), TRUNC_TIME);
        // Let the thread run until it either finishes (no overlap) or blocks on sleep.
        let blocked = !wait_flag(&wait_over, std::time::Duration::from_millis(400));

        // Always release the querier and drain so the thread never leaks, regardless of outcome. Once the only
        // overlapping read is gone the wait must complete.
        assert!(cl.close().is_none());
        assert!(wait_flag(&wait_over, std::time::Duration::from_secs(2)), "{name}: wait must complete once the querier is closed");
        h.join().unwrap();

        assert!(
            !blocked,
            "{name}: a querier whose range is below the truncation point must not block head truncation: its \
             in-order isolation read must be clamped to the truncation point"
        );
    };

    // Both ranges read in-order data below the truncation point: one straddles the truncation point, the other is
    // entirely below it. Neither must pin the head.
    let ranges = [("straddling truncation point", 1000, 2500), ("entirely below truncation point", 500, 1500)];
    for with_ooo in [false, true] {
        let name = if with_ooo { "in-order+OOO" } else { "in-order" };
        let db = new_db(with_ooo);
        for (rname, mint, maxt) in ranges {
            let n = format!("{name}/{rname}");
            assert_does_not_block(&db, AnyQuerier::Samples(db.querier(mint, maxt).unwrap()), &n);
            assert_does_not_block(&db, AnyQuerier::Chunks(db.chunk_querier(mint, maxt).unwrap()), &n);
        }
        db.head().mem_truncation_in_process.store(false, Ordering::SeqCst);
    }
}

#[test]
fn test_query_ooo_head_during_truncate() {
    query_ooo_head_during_truncate(
        |db, min_t, max_t| AnyQuerier::Samples(db.querier(min_t, max_t).unwrap()),
        |q, min_t, _| {
            // Samples
            let AnyQuerier::Samples(q) = q else { panic!("not a querier") };
            let mut ss = q.select(&bg(), false, None, &[m(MatchType::Equal, "a", "b")]);
            assert!(ss.next());
            let s = ss.at().clone();
            assert!(!ss.next()); // One series.
            let mut it = s.iterator(None);
            assert_ne!(ValueType::None, it.next()); // Has some data.
            assert_eq!(min_t, it.at_t()); // It is an in-order sample.
            assert_ne!(ValueType::None, it.next()); // Has some data.
            assert_eq!(min_t + 50, it.at_t()); // it is an out-of-order sample.
            assert!(it.err().is_none());
        },
    );
}

#[test]
fn test_chunk_query_ooo_head_during_truncate() {
    query_ooo_head_during_truncate(
        |db, min_t, max_t| AnyQuerier::Chunks(db.chunk_querier(min_t, max_t).unwrap()),
        |q, min_t, _| {
            // Chunks
            let AnyQuerier::Chunks(q) = q else { panic!("not a chunk querier") };
            let mut ss = q.select(&bg(), false, None, &[m(MatchType::Equal, "a", "b")]);
            assert!(ss.next());
            let s = ss.at().clone();
            assert!(!ss.next()); // One series.
            let mut meta_it = s.iterator(None);
            assert!(meta_it.next());
            let meta = meta_it.at().clone();
            // Samples
            let chk = meta.chunk.expect("chunk");
            let mut it = chk.iterator(None);
            assert_ne!(ValueType::None, it.next()); // Has some data.
            assert_eq!(min_t, it.at_t()); // It is an in-order sample.
            assert_ne!(ValueType::None, it.next()); // Has some data.
            assert_eq!(min_t + 50, it.at_t()); // it is an out-of-order sample.
            assert!(it.err().is_none());
        },
    );
}

fn query_ooo_head_during_truncate(
    make_querier: fn(&TestDb, i64, i64) -> AnyQuerier,
    verify: fn(&mut AnyQuerier, i64, i64),
) {
    const MAX_T: i64 = 6000;
    let db = open_db(tsdb::db::Options {
        out_of_order_time_window: MAX_T,
        min_block_duration: MAX_T / 2, // So that head will compact up to 3000.
        ..tsdb::db::default_options()
    });
    db.disable_compactions();

    let mut app = db.appender(&bg());
    // Add in-order samples at every 100ms starting at 0ms.
    for i in (0..MAX_T).step_by(100) {
        app.append(0, &ls(&["a", "b"]), i, 0.0).unwrap();
    }
    // Add out-of-order samples at every 100ms starting at 50ms.
    for i in (50..MAX_T).step_by(100) {
        app.append(0, &ls(&["a", "b"]), i, 0.0).unwrap();
    }
    commit_ok(&mut app);

    // Go requireEqualOOOSamples.
    let m_ = &db.head().metrics.out_of_order_samples_appended;
    assert_eq!(
        (MAX_T / 100 - 1) as f64,
        m_.with_label_values("float").get() + m_.with_label_values("histogram").get(),
        "number of ooo appended samples mismatch"
    );

    // Synchronization points.
    let (allow_tx, allow_rx) = std::sync::mpsc::sync_channel::<()>(0);
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel::<()>(0);
    let (finished_tx, finished_rx) = std::sync::mpsc::sync_channel::<()>(1);

    let allow_tx = std::sync::Mutex::new(allow_tx);
    let started_rx = std::sync::Mutex::new(started_rx);
    *db.head().mem_truncation_call_back.lock().unwrap() = Some(Box::new(move || {
        // Compaction has started, let the query start and wait for it to actually start to simulate race condition.
        let _ = allow_tx.lock().unwrap().send(());
        let _ = started_rx.lock().unwrap().recv();
    }));

    let db2 = db.db.clone();
    let compaction = std::thread::spawn(move || {
        let _ = db2.compact(&bg()); // Compact and write blocks up to 3000 (maxtT/2).
        let _ = finished_tx.send(());
    });

    // Wait for the compaction to start.
    allow_rx.recv().unwrap();

    let mut q = make_querier(&db, 1500, 2500);
    started_tx.send(()).unwrap(); // Unblock the compaction.
    let ctx = bg();

    // Label names.
    let (res, annots, err) = q.lq().label_names(&ctx, None, &[m(MatchType::Equal, "a", "b")]);
    assert!(err.is_none());
    assert!(annots.is_empty());
    assert_eq!(vec!["a".to_string()], res);

    // Label values.
    let (res, annots, err) = q.lq().label_values(&ctx, "a", None, &[m(MatchType::Equal, "a", "b")]);
    assert!(err.is_none());
    assert!(annots.is_empty());
    assert_eq!(vec!["b".to_string()], res);

    verify(&mut q, 1500, 2500);

    // Cannot be deferred as the compaction waits for queries to close before finishing.
    assert!(q.close().is_none());

    finished_rx.recv().unwrap(); // Wait for compaction.
    compaction.join().unwrap();
    *db.head().mem_truncation_call_back.lock().unwrap() = None;
}

#[test]
fn test_appending_different_encoding_to_same_series() {
    let db = open_db(tsdb::db::default_options());
    db.disable_compactions();

    let hists = tu::generate_test_histograms(10);
    let float_hists = tu::generate_test_float_histograms(10);
    let lbls = ls(&["a", "b"]);

    let mut exp_result: Vec<Sample> = Vec::new();
    let check_exp_chunks = |count: usize| {
        let (ms, created) = db.head().get_or_create(lbls.hash(), &lbls, false).unwrap();
        assert!(!created);
        assert_eq!(count, ms.lock().head_chunks.len());
    };

    let h = |t: i64, i: usize| h_sample(t, hists[i].clone());
    let fh = |t: i64, i: usize| fh_sample(t, float_hists[i].clone());
    let f = |t: i64, v: f64| Sample::float(0, t, v);
    let ooo = Some(storage::ERR_OUT_OF_ORDER_SAMPLE);
    // (samples, expChunks, err)
    let appends: Vec<(Vec<Sample>, usize, Option<Error>)> = vec![
        (vec![h(100, 0)], 1, None),
        (vec![f(200, 2.0)], 2, None),
        (vec![fh(210, 0)], 3, None),
        (vec![h(220, 1)], 4, None),
        (vec![fh(230, 3)], 5, None),
        (vec![h(100, 2)], 0, ooo.clone()),
        (vec![h(300, 3)], 6, None),
        (vec![f(100, 2.0)], 0, ooo.clone()),
        (vec![fh(100, 4)], 0, ooo.clone()),
        // The three next tests all failed before #15177 was fixed.
        // Each of the three samples above creates a new chunk because the type changes.
        (vec![f(400, 4.0), h(500, 5), f(600, 6.0)], 9, None),
        // Again each sample creates a new chunk.
        (vec![h(700, 7), f(800, 8.0), h(900, 9)], 12, None),
        // Even changes between float and integer histogram create new chunks.
        (vec![fh(1000, 7), h(1100, 9)], 14, None),
    ];

    for (samples, exp_chunks, err) in appends {
        let mut app = db.appender(&bg());
        for s in &samples {
            let r = if s.h.is_some() || s.fh.is_some() {
                app.append_histogram(0, &lbls, s.t, s.h.as_deref(), s.fh.as_deref())
            } else {
                app.append(0, &lbls, s.t, s.f)
            };
            assert_eq!(err, r.err());
        }
        if err.is_none() {
            commit_ok(&mut app);
            exp_result.extend(samples);
            check_exp_chunks(exp_chunks);
        } else {
            assert!(app.rollback().is_none());
        }
    }
    for i in 1..exp_result.len() {
        let (prev_h, prev_fh) = (exp_result[i - 1].h.is_none(), exp_result[i - 1].fh.is_none());
        let s = &mut exp_result[i];
        if let Some(h) = s.h.as_mut().filter(|_| prev_h) {
            h.counter_reset_hint = CounterResetHint::UnknownCounterReset;
        } else if let Some(fh) = s.fh.as_mut().filter(|_| prev_fh) {
            fh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
        }
    }

    // Query back and expect same order of samples.
    let q = db.querier(i64::MIN, i64::MAX).unwrap();
    let series = query(q, &[m(MatchType::Equal, "a", "b")]);
    assert_eq!(BTreeMap::from([(lbls.to_string(), exp_result)]), series);
}
