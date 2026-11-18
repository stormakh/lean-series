//! Port of `tsdb/head_test.go` lines 3806..7602.
//!
//! Tests that need the DB (`tsdb.Open`, `db.Compact`, `db.Querier`) wait for the DB port; see the list at the end.

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
