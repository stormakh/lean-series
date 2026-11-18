//! Port of `tsdb/head_append_v2_test.go`, lines 1..2831.
//!
//! Tests that need the DB (`Open`, `newTestDB`) wait for `db.rs`: see the list at the end of this file.

use std::collections::BTreeMap;
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use chunkenc::{ChunkIterator, CounterResetHeader, FloatHistogramChunk, HistogramChunk, ValueType};
use model::exemplar::Exemplar;
use model::histogram::{CounterResetHint, FloatHistogram, Histogram};
use model::labels::{self, Labels};
use model::matcher::{MatchType, Matcher};
use model::value::{is_stale_nan, STALE_NAN};
use storage::tsdbutil as tu;
use storage::{
    AppendV2Options, AppenderV2, Context, Sample, SeriesArc, ERR_OUT_OF_BOUNDS, ERR_OUT_OF_ORDER_SAMPLE,
    ERR_TOO_OLD_SAMPLE,
};
use tsdb::head::{mmapped_chunks_dir, Head, HeadOptions, MemSeries};
use tsdb::querier::{new_block_querier, BlockBaseQuerier, BlockQuerier};
use tsdb::reader::IndexReader;
use tsdb_format::record::{self, RefSeries};
use tsdb_format::tombstones::{self, Interval, Intervals, MemTombstones};
use tsdb_format::wlog::{self, compression, WL};

use crate::common::{fs, new_test_head, query, TestHead};

// ---------------------------------------------------------------------------------------------------------------
// Helpers.

fn ctx() -> Context {
    Context::background()
}

fn ls(kv: &[&str]) -> Labels {
    labels::from_strings(kv)
}

fn m(t: MatchType, n: &str, v: &str) -> Matcher {
    Matcher::must_new(t, n, v)
}

fn ao() -> AppendV2Options<'static> {
    AppendV2Options::default()
}

/// Appends a float sample with `AOptions{}`.
fn app_f(app: &mut dyn AppenderV2, l: &Labels, t: i64, v: f64) -> Result<u64, storage::Error> {
    app.append(0, l, 0, t, v, None, None, &ao())
}

/// A reopened head that is closed on drop (Go `defer head.Close()`).
struct HeadGuard(Arc<Head>);

impl Deref for HeadGuard {
    type Target = Arc<Head>;
    fn deref(&self) -> &Arc<Head> {
        &self.0
    }
}

impl Drop for HeadGuard {
    fn drop(&mut self) {
        if !self.0.is_closed() {
            let _ = self.0.close();
        }
    }
}

/// A copy of the head options (Go copies the `HeadOptions` struct; here it holds atomics).
fn copy_opts(o: &HeadOptions) -> HeadOptions {
    HeadOptions {
        max_exemplars: AtomicI64::new(o.max_exemplars.load(Ordering::Relaxed)),
        out_of_order_time_window: AtomicI64::new(o.out_of_order_time_window.load(Ordering::Relaxed)),
        out_of_order_cap_max: AtomicI64::new(o.out_of_order_cap_max.load(Ordering::Relaxed)),
        enable_st_storage: AtomicBool::new(o.enable_st_storage.load(Ordering::Relaxed)),
        float_chunk_encoding: std::sync::atomic::AtomicU32::new(o.float_chunk_encoding.load(Ordering::Relaxed)),
        enable_histogram_st_encoding: AtomicBool::new(o.enable_histogram_st_encoding.load(Ordering::Relaxed)),
        chunk_range: o.chunk_range,
        chunk_dir_root: o.chunk_dir_root.clone(),
        chunk_pool: o.chunk_pool.clone(),
        chunk_write_buffer_size: o.chunk_write_buffer_size,
        chunk_write_queue_size: o.chunk_write_queue_size,
        samples_per_chunk: o.samples_per_chunk,
        stripe_size: o.stripe_size,
        series_callback: o.series_callback.clone(),
        enable_exemplar_storage: o.enable_exemplar_storage,
        enable_memory_snapshot_on_shutdown: o.enable_memory_snapshot_on_shutdown,
        isolation_disabled: o.isolation_disabled,
        wal_replay_concurrency: o.wal_replay_concurrency,
        enable_sharding: o.enable_sharding,
        enable_st_as_zero_sample: o.enable_st_as_zero_sample,
        enable_metadata_wal_records: o.enable_metadata_wal_records,
        enable_fast_startup: o.enable_fast_startup,
        logger: o.logger.clone(),
    }
}

/// Go `NewHead(nil, nil, w, nil, opts, nil)` followed by `Init(min_valid_time)`.
fn open_head(w: WL, opts: HeadOptions, min_valid_time: i64) -> HeadGuard {
    let h = HeadGuard(Head::new(Some(Arc::new(w)), None, opts, None).expect("new head"));
    h.init(min_valid_time).expect("init head");
    h
}

fn wal_dir(th: &TestHead) -> std::path::PathBuf {
    th.wal.as_ref().expect("wal").dir().to_path_buf()
}

/// A WAL record decoded by [`read_test_wal`] (Go returns `[]any`).
#[derive(Debug, PartialEq)]
enum Rec {
    Series(Vec<RefSeries>),
    Samples(Vec<record::RefSample>),
    Histograms(Vec<record::RefHistogramSample>),
    FloatHistograms(Vec<record::RefFloatHistogramSample>),
    Tombstones(Vec<tombstones::Stone>),
    Metadata(Vec<record::RefMetadata>),
    Exemplars(Vec<record::RefExemplar>),
}

/// Go `readTestWAL` (head_test.go).
fn read_test_wal(dir: &Path) -> Vec<Rec> {
    let sr = wlog::new_segments_reader(dir).expect("segments reader");
    let mut dec = record::Decoder::new(tsdb::Logger::nop());
    let mut r = wlog::Reader::new(sr);
    let mut recs = Vec::new();
    while r.next() {
        let rec = r.record();
        match dec.r#type(rec) {
            record::Type::Series => {
                let mut v = Vec::new();
                dec.series(rec, &mut v).unwrap();
                recs.push(Rec::Series(v));
            }
            record::Type::Samples | record::Type::SamplesV2 => {
                let mut v = Vec::new();
                dec.samples(rec, &mut v).unwrap();
                recs.push(Rec::Samples(v));
            }
            record::Type::HistogramSamples
            | record::Type::CustomBucketsHistogramSamples
            | record::Type::HistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.histogram_samples(rec, &mut v).unwrap();
                recs.push(Rec::Histograms(v));
            }
            record::Type::FloatHistogramSamples
            | record::Type::CustomBucketsFloatHistogramSamples
            | record::Type::FloatHistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.float_histogram_samples(rec, &mut v).unwrap();
                recs.push(Rec::FloatHistograms(v));
            }
            record::Type::Tombstones => {
                let mut v = Vec::new();
                dec.tombstones(rec, &mut v).unwrap();
                recs.push(Rec::Tombstones(v));
            }
            record::Type::Metadata => {
                let mut v = Vec::new();
                dec.metadata(rec, &mut v).unwrap();
                recs.push(Rec::Metadata(v));
            }
            record::Type::Exemplars => {
                let mut v = Vec::new();
                dec.exemplars(rec, &mut v).unwrap();
                recs.push(Rec::Exemplars(v));
            }
            record::Type::MinValidTime => {
                dec.min_valid_time(rec).unwrap();
            }
            t => panic!("unknown record type {t:?}"),
        }
    }
    assert!(r.err().is_none(), "{:?}", r.err());
    recs
}

/// Go `deletedSamples` (head_test.go).
fn deleted_samples(full: &[Sample], dranges: &[Interval]) -> Vec<Sample> {
    full.iter().filter(|s| !dranges.iter().any(|r| r.in_bounds(s.t))).cloned().collect()
}

/// Go `labels.Selector(ms).Matches(ls)`.
fn selector_matches(ms: &[Matcher], l: &Labels) -> bool {
    ms.iter().all(|m| m.matches(l.get(&m.name)))
}

/// Expands the samples of a series (Go `storage.ExpandSamples(s.Iterator(nil), newSample)`).
fn expand(s: &SeriesArc) -> Result<Vec<Sample>, storage::Error> {
    let mut it = s.iterator(None);
    storage::expand_samples(&mut it)
}

/// Go `requireEqualSamples` (testutil.go) without options.
fn require_equal_samples(name: &str, expected: &[Sample], actual: &[Sample]) {
    assert_eq!(expected.len(), actual.len(), "Length not equal to expected for {name}");
    for (i, (e, a)) in expected.iter().zip(actual).enumerate() {
        assert_eq!(e.t, a.t, "Different timestamps for {name}[{i}]");
        assert_eq!(sample_type(e), sample_type(a), "Different types for {name}[{i}] at ts {}", e.t);
        if let Some(eh) = &e.h {
            let ah = a.h.as_ref().unwrap();
            assert_eq!(eh.counter_reset_hint, ah.counter_reset_hint, "Sample header doesn't match for {name}[{i}]");
            assert_eq!(eh, ah, "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        } else if let Some(efh) = &e.fh {
            let afh = a.fh.as_ref().unwrap();
            assert_eq!(efh.counter_reset_hint, afh.counter_reset_hint, "Sample header doesn't match for {name}[{i}]");
            assert_eq!(efh, afh, "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        } else {
            assert_eq!(e.f.to_bits(), a.f.to_bits(), "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        }
    }
}

fn sample_type(s: &Sample) -> ValueType {
    if s.h.is_some() {
        ValueType::Histogram
    } else if s.fh.is_some() {
        ValueType::FloatHistogram
    } else {
        ValueType::Float
    }
}

/// Go `compareSeries` (db_test.go).
fn compare_series(expected: &BTreeMap<String, Vec<Sample>>, actual: &BTreeMap<String, Vec<Sample>>) {
    if expected.len() != actual.len() {
        assert_eq!(expected, actual, "number of series differs");
    }
    for (key, exp_samples) in expected {
        let Some(act_samples) = actual.get(key) else {
            panic!("expected series {key:?} not found");
        };
        if exp_samples.len() != act_samples.len() {
            assert_eq!(exp_samples, act_samples, "number of samples for series {key:?} differs");
        }
        for (i, (e, a)) in exp_samples.iter().zip(act_samples).enumerate() {
            assert_eq!(e.t, a.t, "timestamp of sample {i} in series {key:?} differs");
            assert_eq!(sample_type(e), sample_type(a), "type of sample {i} in series {key:?} differs");
            match sample_type(e) {
                ValueType::Float => assert_eq!(e.f, a.f, "sample {i} in series {key:?} differs"),
                ValueType::Histogram => {
                    let (eh, ah) = (e.h.as_deref().unwrap(), a.h.as_deref().unwrap());
                    let mut eh = eh.clone();
                    if ah.counter_reset_hint == CounterResetHint::UnknownCounterReset {
                        // It is always safe to set the counter reset hint to UnknownCounterReset.
                        eh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                    }
                    assert_eq!(&eh, ah, "histogram sample {i} in series {key:?} differs");
                }
                ValueType::FloatHistogram => {
                    let (efh, afh) = (e.fh.as_deref().unwrap(), a.fh.as_deref().unwrap());
                    let mut efh = efh.clone();
                    if afh.counter_reset_hint == CounterResetHint::UnknownCounterReset {
                        efh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                    }
                    assert_eq!(&efh, afh, "float histogram sample {i} in series {key:?} differs");
                }
                ValueType::None => unreachable!(),
            }
        }
    }
}

fn rand_f64() -> f64 {
    crate::common::rand_f64()
}

/// Go `rand.Int63n(n)`.
fn rand_i63n(n: i64) -> i64 {
    let mut b = [0u8; 8];
    crypto::random_bytes(&mut b);
    (u64::from_le_bytes(b) % n as u64) as i64
}

fn get_series(h: &Head, l: &Labels) -> Option<Arc<MemSeries>> {
    h.series.get_by_hash(l.hash(), l)
}

// ---------------------------------------------------------------------------------------------------------------
// Tests.

#[test]
fn test_head_appender_v2_wal_multi_ref() {
    let head = new_test_head("wal-multi-ref", 1000, compression::Type::None, false);
    head.init(0).unwrap();

    let mut app = head.appender_v2(&ctx());
    let ref1 = app_f(&mut *app, &ls(&["foo", "bar"]), 100, 1.0).unwrap();
    assert!(app.commit().is_none());
    assert_eq!(1.0, head.metrics.chunks_created.get());

    // Add another sample outside chunk range to mmap a chunk.
    let mut app = head.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["foo", "bar"]), 1500, 2.0).unwrap();
    assert!(app.commit().is_none());
    assert_eq!(2.0, head.metrics.chunks_created.get());

    head.truncate(1600).unwrap();

    let mut app = head.appender_v2(&ctx());
    let ref2 = app_f(&mut *app, &ls(&["foo", "bar"]), 1700, 3.0).unwrap();
    assert!(app.commit().is_none());
    assert_eq!(3.0, head.metrics.chunks_created.get());

    // Add another sample outside chunk range to mmap a chunk.
    let mut app = head.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["foo", "bar"]), 2000, 4.0).unwrap();
    assert!(app.commit().is_none());
    assert_eq!(4.0, head.metrics.chunks_created.get());

    assert_ne!(ref1, ref2, "Refs are the same");
    head.close().unwrap();

    let w = WL::new(wal_dir(&head), compression::Type::None).unwrap();
    let opts = HeadOptions {
        chunk_range: 1000,
        chunk_dir_root: head.opts.chunk_dir_root.clone(),
        ..HeadOptions::default()
    };
    let head2 = open_head(w, opts, 0);

    let q = new_block_querier(head2.as_ref(), 0, 2100).unwrap();
    let series = query(q, &[m(MatchType::Equal, "foo", "bar")]);
    // The samples before the new ref should be discarded since Head truncation happens only after compacting
    // the Head.
    let exp: BTreeMap<String, Vec<Sample>> =
        [(r#"{foo="bar"}"#.to_string(), vec![fs(1700, 3.0), fs(2000, 4.0)])].into_iter().collect();
    assert_eq!(exp, series);
}

#[test]
fn test_head_appender_v2_appenders_metrics() {
    let head = new_test_head("appenders-metrics", 1000, compression::Type::None, false);
    head.init(0).unwrap();

    // First rollback with no samples.
    let mut app = head.appender_v2(&ctx());
    assert_eq!(1.0, head.metrics.active_appenders.get());
    assert_eq!(1.0, head.metrics.appenders_created.get());
    assert!(app.rollback().is_none());
    assert_eq!(0.0, head.metrics.active_appenders.get());
    assert_eq!(1.0, head.metrics.appenders_created.get());

    // Then commit with no samples.
    let mut app = head.appender_v2(&ctx());
    assert!(app.commit().is_none());
    assert_eq!(0.0, head.metrics.active_appenders.get());
    assert_eq!(2.0, head.metrics.appenders_created.get());

    // Now rollback with one sample.
    let mut app = head.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["foo", "bar"]), 100, 1.0).unwrap();
    assert_eq!(1.0, head.metrics.active_appenders.get());
    assert_eq!(3.0, head.metrics.appenders_created.get());
    assert!(app.rollback().is_none());
    assert_eq!(0.0, head.metrics.active_appenders.get());
    assert_eq!(3.0, head.metrics.appenders_created.get());

    // Now commit with one sample.
    let mut app = head.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["foo", "bar"]), 100, 1.0).unwrap();
    assert!(app.commit().is_none());
    assert_eq!(0.0, head.metrics.active_appenders.get());
    assert_eq!(4.0, head.metrics.appenders_created.get());
}

#[test]
fn test_head_appender_v2_race_between_series_creation_and_gc() {
    let head = new_test_head("race-creation-gc", 1000, compression::Type::None, false);
    head.init(0).unwrap();

    const TOTAL_SERIES: usize = 100_000;
    let series: Vec<Labels> = (0..TOTAL_SERIES).map(|i| ls(&["foo", &i.to_string()])).collect();
    let done = AtomicBool::new(false);

    std::thread::scope(|s| {
        s.spawn(|| {
            let mut app = head.appender_v2(&ctx());
            for l in &series {
                if let Err(e) = app_f(&mut *app, l, 100, 1.0) {
                    done.store(true, Ordering::SeqCst);
                    panic!("Failed to append: {e}");
                }
            }
            if let Some(e) = app.commit() {
                done.store(true, Ordering::SeqCst);
                panic!("Failed to commit: {e}");
            }
            done.store(true, Ordering::SeqCst);
        });

        // Don't check the atomic.Bool on all iterations in order to perform more gc iterations and make the race
        // condition more likely.
        let mut i = 1;
        while i % 128 != 0 || !done.load(Ordering::SeqCst) {
            head.gc();
            i += 1;
        }
    });

    assert_eq!(TOTAL_SERIES, head.num_series() as usize);
}

#[test]
fn test_head_appender_v2_can_gc_series_created_without_samples() {
    type FinishTxn = fn(&mut dyn AppenderV2) -> Option<storage::Error>;
    let ops: [(&str, FinishTxn); 2] =
        [("after commit", |app| app.commit()), ("after rollback", |app| app.rollback())];
    for (op, finish_txn) in ops {
        let chunk_range = 3_600_000i64;
        let head = new_test_head("gc-without-samples", chunk_range, compression::Type::None, true);
        head.init(0).unwrap();

        let first_sample_time = 10 * chunk_range;
        {
            // Append first sample, it should init head max time to firstSampleTime.
            let mut app = head.appender_v2(&ctx());
            app_f(&mut *app, &ls(&["lbl", "ok"]), first_sample_time, 1.0).unwrap();
            assert!(app.commit().is_none());
            assert_eq!(1, head.num_series(), "{op}");
        }

        // Append a sample in a time range that is not covered by the chunk range, We would create series first
        // and then append no sample.
        let mut app = head.appender_v2(&ctx());
        let invalid_sample_time = first_sample_time - chunk_range;
        let err = app_f(&mut *app, &ls(&["foo", "bar"]), invalid_sample_time, 2.0).unwrap_err();
        // These are our assumptions: we're not testing them, we're just checking them to make debugging a failed
        // test easier if someone refactors the code and breaks these assumptions.
        assert!(
            !err.is(&ERR_OUT_OF_BOUNDS),
            "Failed to append sample shouldn't take the shortcut that returns storage.ErrOutOfBounds"
        );
        assert!(
            err.is(&ERR_TOO_OLD_SAMPLE),
            "Failed to append sample should return storage.ErrTooOldSample, because OOO window was enabled but this sample doesn't fall into it."
        );
        // Do commit or rollback, depending on what we're testing.
        assert!(finish_txn(&mut *app).is_none(), "{op}");

        // Garbage-collect, since we finished the transaction and series has no samples, it should be collectable.
        head.gc();
        assert_eq!(1, head.num_series(), "{op}");
    }
}

#[test]
fn test_head_appender_v2_delete_simple() {
    let build_smpls = |s: &[i64]| -> Vec<Sample> { s.iter().map(|&t| fs(t, t as f64)).collect() };
    let smpls_all = build_smpls(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    let lbls_default = ls(&["a", "b"]);
    let itv = |mint: i64, maxt: i64| Interval { mint, maxt };

    struct Case {
        dranges: Intervals,
        add_samples: Vec<Sample>, // Samples to add after delete.
        smpls_exp: Vec<Sample>,
    }
    let cases = vec![
        Case { dranges: vec![itv(0, 3)], add_samples: vec![], smpls_exp: build_smpls(&[4, 5, 6, 7, 8, 9]) },
        Case { dranges: vec![itv(1, 3)], add_samples: vec![], smpls_exp: build_smpls(&[0, 4, 5, 6, 7, 8, 9]) },
        Case { dranges: vec![itv(1, 3), itv(4, 7)], add_samples: vec![], smpls_exp: build_smpls(&[0, 8, 9]) },
        Case { dranges: vec![itv(1, 3), itv(4, 700)], add_samples: vec![], smpls_exp: build_smpls(&[0]) },
        // This case is to ensure that labels and symbols are deleted.
        Case { dranges: vec![itv(0, 9)], add_samples: vec![], smpls_exp: build_smpls(&[]) },
        Case {
            dranges: vec![itv(1, 3)],
            add_samples: build_smpls(&[11, 13, 15]),
            smpls_exp: build_smpls(&[0, 4, 5, 6, 7, 8, 9, 11, 13, 15]),
        },
        // After delete, the appended samples in the deleted range should be visible as the tombstones are clamped
        // to head min/max time.
        Case {
            dranges: vec![itv(7, 20)],
            add_samples: build_smpls(&[11, 13, 15]),
            smpls_exp: build_smpls(&[0, 1, 2, 3, 4, 5, 6, 11, 13, 15]),
        },
    ];

    for compress in [compression::Type::None, compression::Type::Snappy, compression::Type::Zstd] {
        for c in &cases {
            let head = new_test_head("delete-simple", 1000, compress, false);
            head.init(0).unwrap();

            let mut app = head.appender_v2(&ctx());
            for smpl in &smpls_all {
                app_f(&mut *app, &lbls_default, smpl.t, smpl.f).unwrap();
            }
            assert!(app.commit().is_none());

            // Delete the ranges.
            for r in &c.dranges {
                head.delete(&ctx(), r.mint, r.maxt, &[m(MatchType::Equal, "a", "b")]).unwrap();
            }

            // Add more samples.
            let mut app = head.appender_v2(&ctx());
            for smpl in &c.add_samples {
                app_f(&mut *app, &lbls_default, smpl.t, smpl.f).unwrap();
            }
            assert!(app.commit().is_none());

            // Compare the samples for both heads - before and after the reloadBlocks.
            // Use a new wal to ensure deleted samples are gone even after a reloadBlocks.
            let reloaded_w = WL::new(wal_dir(&head), compress).unwrap();
            let opts = HeadOptions {
                chunk_range: 1000,
                chunk_dir_root: reloaded_w.dir().to_path_buf(),
                ..HeadOptions::default()
            };
            let reloaded_head = open_head(reloaded_w, opts, 0);

            // Compare the query results for both heads - before and after the reloadBlocks.
            for h in [&head.head, &reloaded_head.0] {
                let mut q = new_block_querier(h.as_ref(), h.min_time(), h.max_time()).unwrap();
                let mut act = q.select(&ctx(), false, None, &[m(MatchType::Equal, "a", "b")]);
                assert!(q.close().is_none());
                let exp_series: Vec<SeriesArc> = vec![storage::new_list_series(lbls_default.clone(), c.smpls_exp.clone())];
                let mut exp = exp_series.iter();
                loop {
                    let e = exp.next();
                    let rok = act.next();
                    assert_eq!(e.is_some(), rok, "compress={compress:?}");
                    let Some(exp_series) = e else {
                        h.close().unwrap();
                        assert!(act.err().is_none());
                        assert!(act.warnings().is_empty());
                        break;
                    };
                    let act_series = act.at().clone();
                    assert_eq!(exp_series.labels(), act_series.labels());
                    let (smpl_exp, smpl_res) = (expand(exp_series), expand(&act_series));
                    assert_eq!(smpl_exp.as_ref().err(), smpl_res.as_ref().err());
                    assert_eq!(smpl_exp.unwrap(), smpl_res.unwrap());
                }
            }
        }
    }
}

#[test]
fn test_head_appender_v2_delete_until_curr_max() {
    let hb = new_test_head("delete-until-curr-max", 1_000_000, compression::Type::None, false);

    let num_samples = 10i64;
    let mut app = hb.appender_v2(&ctx());
    for i in 0..num_samples {
        app_f(&mut *app, &ls(&["a", "b"]), i, rand_f64()).unwrap();
    }
    assert!(app.commit().is_none());
    hb.delete(&ctx(), 0, 10000, &[m(MatchType::Equal, "a", "b")]).unwrap();

    // Test the series returns no samples. The series is cleared only after compaction.
    let q = new_block_querier(hb.head.as_ref(), 0, 100000).unwrap();
    {
        let mut res = q.select(&ctx(), false, None, &[m(MatchType::Equal, "a", "b")]);
        assert!(res.next(), "series is not present");
        let s = res.at().clone();
        let mut it = s.iterator(None);
        assert_eq!(ValueType::None, it.next(), "expected no samples");
        while res.next() {}
        assert!(res.err().is_none());
        assert!(res.warnings().is_empty());
    }

    // Add again and test for presence.
    let mut app = hb.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["a", "b"]), 11, 1.0).unwrap();
    assert!(app.commit().is_none());
    let q = new_block_querier(hb.head.as_ref(), 0, 100000).unwrap();
    let mut res = q.select(&ctx(), false, None, &[m(MatchType::Equal, "a", "b")]);
    assert!(res.next(), "series don't exist");
    let exps = res.at().clone();
    let res_samples = expand(&exps).unwrap();
    assert_eq!(vec![fs(11, 1.0)], res_samples);
    while res.next() {}
    assert!(res.err().is_none());
    assert!(res.warnings().is_empty());
}

#[test]
fn test_head_appender_v2_delete_samples_and_series_still_in_wal_after_checkpoint() {
    let num_samples = 10000i64;

    // Enough samples to cause a checkpoint.
    let hb = new_test_head("delete-still-in-wal", num_samples * 10, compression::Type::None, false);

    for i in 0..num_samples {
        let mut app = hb.appender_v2(&ctx());
        app_f(&mut *app, &ls(&["a", "b"]), i, 0.0).unwrap();
        assert!(app.commit().is_none());
    }
    hb.delete(&ctx(), 0, num_samples, &[m(MatchType::Equal, "a", "b")]).unwrap();
    hb.truncate(1).unwrap();
    hb.close().unwrap();

    // Confirm there's been a checkpoint.
    let (cdir, _) = wlog::last_checkpoint(&wal_dir(&hb)).unwrap();
    // Read in checkpoint and WAL.
    let mut recs = read_test_wal(&cdir);
    recs.extend(read_test_wal(&wal_dir(&hb)));

    let (mut series, mut samples, mut stones, mut metadata) = (0, 0, 0, 0);
    for rec in &recs {
        match rec {
            Rec::Series(_) => series += 1,
            Rec::Samples(_) => samples += 1,
            Rec::Tombstones(_) => stones += 1,
            Rec::Metadata(_) => metadata += 1,
            _ => panic!("unknown record type"),
        }
    }
    assert_eq!(1, series);
    assert_eq!(9999, samples);
    assert_eq!(1, stones);
    assert_eq!(0, metadata);
}

#[test]
fn test_head_appender_v2_delete_e2e() {
    let num_datapoints = 1000;
    let num_ranges = 1000;
    let time_interval = 2i64;
    // Create 8 series with 1000 data-points of different ranges, delete and run queries.
    let lbls: Vec<Labels> = vec![
        ls(&["a", "b", "instance", "localhost:9090", "job", "prometheus"]),
        ls(&["a", "b", "instance", "127.0.0.1:9090", "job", "prometheus"]),
        ls(&["a", "b", "instance", "127.0.0.1:9090", "job", "prom-k8s"]),
        ls(&["a", "b", "instance", "localhost:9090", "job", "prom-k8s"]),
        ls(&["a", "c", "instance", "localhost:9090", "job", "prometheus"]),
        ls(&["a", "c", "instance", "127.0.0.1:9090", "job", "prometheus"]),
        ls(&["a", "c", "instance", "127.0.0.1:9090", "job", "prom-k8s"]),
        ls(&["a", "c", "instance", "localhost:9090", "job", "prom-k8s"]),
    ];
    let mut series_map: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
    for l in &lbls {
        series_map.insert(l.to_string(), vec![]);
    }

    let hb = new_test_head("delete-e2e", 100000, compression::Type::None, false);

    let mut app = hb.appender_v2(&ctx());
    for l in &lbls {
        let mut series = Vec::new();
        let mut ts = rand_i63n(300);
        for _ in 0..num_datapoints {
            let v = rand_f64();
            app_f(&mut *app, l, ts, v).unwrap();
            series.push(fs(ts, v));
            ts += rand_i63n(time_interval) + 1;
        }
        series_map.insert(l.to_string(), series);
    }
    assert!(app.commit().is_none());
    // Delete a time-range from each-selector.
    let itv = |mint: i64, maxt: i64| Interval { mint, maxt };
    let dels: Vec<(Vec<Matcher>, Intervals)> = vec![
        (vec![m(MatchType::Equal, "a", "b")], vec![itv(300, 500), itv(600, 670)]),
        (
            vec![m(MatchType::Equal, "a", "b"), m(MatchType::Equal, "job", "prom-k8s")],
            vec![itv(300, 500), itv(100, 670)],
        ),
        (
            vec![
                m(MatchType::Equal, "a", "c"),
                m(MatchType::Equal, "instance", "localhost:9090"),
                m(MatchType::Equal, "job", "prometheus"),
            ],
            vec![itv(300, 400), itv(100, 6700)],
        ),
        // TODO: Add Regexp Matchers.
    ];
    for (ms, drange) in &dels {
        for r in drange {
            hb.delete(&ctx(), r.mint, r.maxt, ms).unwrap();
        }
        let mut matched: Vec<Labels> = lbls.iter().filter(|l| selector_matches(ms, l)).cloned().collect();
        matched.sort();
        for _ in 0..num_ranges {
            let mut q = new_block_querier(hb.head.as_ref(), 0, 100000).unwrap();
            {
                let mut ss = q.select(&ctx(), true, None, ms);
                // Build the mockSeriesSet.
                let mut matched_series: Vec<SeriesArc> = Vec::with_capacity(matched.len());
                for mm in &matched {
                    let smpls = deleted_samples(&series_map[&mm.to_string()], drange);
                    // Only append those series for which samples exist as mockSeriesSet doesn't skip series with
                    // no samples.
                    if !smpls.is_empty() {
                        matched_series.push(storage::new_list_series(mm.clone(), smpls));
                    }
                }
                let mut exp_ss = matched_series.iter();
                // Compare both SeriesSets.
                loop {
                    let e = exp_ss.next();
                    let mut rok = ss.next();
                    // Skip a series if iterator is empty.
                    if rok {
                        while ss.at().iterator(None).next() == ValueType::None {
                            rok = ss.next();
                            if !rok {
                                break;
                            }
                        }
                    }
                    assert_eq!(e.is_some(), rok);
                    let Some(sexp) = e else { break };
                    let sres = ss.at().clone();
                    assert_eq!(sexp.labels(), sres.labels());
                    let (smpl_exp, smpl_res) = (expand(sexp), expand(&sres));
                    assert_eq!(smpl_exp.as_ref().err(), smpl_res.as_ref().err());
                    require_equal_samples(&sexp.labels().to_string(), &smpl_exp.unwrap(), &smpl_res.unwrap());
                }
                assert!(ss.err().is_none());
                assert!(ss.warnings().is_empty());
            }
            assert!(q.close().is_none());
        }
    }
}

#[test]
fn test_head_appender_v2_uncommitted_samples_not_lost_on_truncate() {
    let h = new_test_head("uncommitted-not-lost", 1000, compression::Type::None, false);

    h.init_time(0);

    let mut app = h.head_appender_v2();
    let lset = ls(&["a", "1"]);
    app_f(&mut app, &lset, 2100, 1.0).unwrap();

    h.truncate(2000).unwrap();
    assert!(get_series(&h, &lset).is_some(), "series should not have been garbage collected");

    assert!(storage::AppenderTransaction::commit(&mut app).is_none());

    let mut q = new_block_querier(h.head.as_ref(), 1500, 2500).unwrap();
    {
        let mut ss = q.select(&ctx(), false, None, &[m(MatchType::Equal, "a", "1")]);
        assert!(ss.next());
        while ss.next() {}
        assert!(ss.err().is_none());
        assert!(ss.warnings().is_empty());
    }
    q.close();
}

#[test]
fn test_head_appender_v2_test_remove_series_after_rollback_and_truncate() {
    let h = new_test_head("remove-after-rollback", 1000, compression::Type::None, false);

    h.init_time(0);

    let mut app = h.head_appender_v2();
    let lset = ls(&["a", "1"]);
    app_f(&mut app, &lset, 2100, 1.0).unwrap();

    h.truncate(2000).unwrap();
    assert!(get_series(&h, &lset).is_some(), "series should not have been garbage collected");

    assert!(storage::AppenderTransaction::rollback(&mut app).is_none());

    let mut q = new_block_querier(h.head.as_ref(), 1500, 2500).unwrap();
    {
        let mut ss = q.select(&ctx(), false, None, &[m(MatchType::Equal, "a", "1")]);
        assert!(!ss.next());
        assert!(ss.warnings().is_empty());
    }
    assert!(q.close().is_none());

    // Truncate again, this time the series should be deleted.
    h.truncate(2050).unwrap();
    assert!(get_series(&h, &lset).is_none());
}

#[test]
fn test_head_appender_v2_log_rollback() {
    for compress in [compression::Type::None, compression::Type::Snappy, compression::Type::Zstd] {
        let h = new_test_head("log-rollback", 1000, compress, false);

        let mut app = h.appender_v2(&ctx());
        app_f(&mut *app, &ls(&["a", "b"]), 1, 2.0).unwrap();

        assert!(app.rollback().is_none());
        let recs = read_test_wal(&wal_dir(&h));

        assert_eq!(1, recs.len(), "compress={compress:?}");

        let Rec::Series(series) = &recs[0] else {
            panic!("expected series record but got {:?}", recs[0]);
        };
        assert_eq!(&vec![RefSeries { r#ref: 1, labels: ls(&["a", "b"]) }], series);
    }
}

#[test]
fn test_head_appender_v2_returns_sorted_label_values() {
    let h = new_test_head("sorted-label-values", 1000, compression::Type::None, false);

    h.init_time(0);

    let mut app = h.head_appender_v2();
    for i in (1..=100).rev() {
        for j in 0..10 {
            let lset = ls(&["__name__", &format!("metric_{i}"), "label", &format!("value_{j}")]);
            app_f(&mut app, &lset, 2100, 1.0).unwrap();
        }
    }

    let mut q = new_block_querier(h.head.as_ref(), 1500, 2500).unwrap();

    let (res, _, err) = q.label_values(&ctx(), "__name__", None, &[]);
    assert!(err.is_none());

    assert!(res.is_sorted());
    assert!(q.close().is_none());
    // adapted: Go never finishes the appender; here it is rolled back before the head closes.
    storage::AppenderTransaction::rollback(&mut app);
}

#[test]
fn test_head_appender_v2_new_wal_segment_on_truncate() {
    let h = new_test_head("new-wal-segment", 1000, compression::Type::None, false);
    let wal = wal_dir(&h);
    let add = |ts: i64| {
        let mut app = h.appender_v2(&ctx());
        app_f(&mut *app, &ls(&["a", "b"]), ts, 0.0).unwrap();
        assert!(app.commit().is_none());
    };

    add(0);
    let (_, last) = wlog::segments(&wal).unwrap();
    assert_eq!(0, last);

    add(1);
    h.truncate(1).unwrap();
    let (_, last) = wlog::segments(&wal).unwrap();
    assert_eq!(1, last);

    add(2);
    h.truncate(2).unwrap();
    let (_, last) = wlog::segments(&wal).unwrap();
    assert_eq!(2, last);
}

#[test]
fn test_head_appender_v2_append_duplicate_label_name() {
    let h = new_test_head("duplicate-label-name", 1000, compression::Type::None, false);

    let add = |l: Labels, label_name: &str| {
        let mut app = h.appender_v2(&ctx());
        let err = app_f(&mut *app, &l, 0, 0.0).unwrap_err();
        assert_eq!(format!(r#"label name "{label_name}" is not unique: invalid sample"#), err.to_string());
    };

    add(ls(&["a", "c", "a", "b"]), "a");
    add(ls(&["a", "c", "a", "c"]), "a");
    add(ls(&["__name__", "up", "job", "prometheus", "le", "500", "le", "400", "unit", "s"]), "le");
}

#[test]
fn test_head_appender_v2_mem_series_isolation() {
    // Put a series, select it. GC it and then access it.
    let last_value = |h: &Head, max_append_id: u64| -> i64 {
        let idx = h.index();

        let mut iso = h.iso.state(i64::MIN, i64::MAX);
        Arc::get_mut(&mut iso).expect("fresh isolation state").max_append_id = max_append_id;

        let chunks = h.chunks_range(i64::MIN, i64::MAX, Some(iso)).unwrap();
        // Hm.. here direct block chunk querier might be required?
        let mut querier = BlockQuerier(BlockBaseQuerier::from_parts(
            tsdb::head::head_ulid(),
            Arc::new(idx),
            Arc::new(chunks),
            Arc::new(MemTombstones::new()),
            0,
            10000,
        ));

        let mut result = -1;
        {
            let mut ss = storage::Querier::select(&querier, &ctx(), false, None, &[m(MatchType::Equal, "foo", "bar")]);
            // Go expandSeriesSet: float samples of each series.
            let mut series_set: Vec<Vec<Sample>> = Vec::new();
            while ss.next() {
                let s = ss.at().clone();
                let mut it = s.iterator(None);
                let mut samples = Vec::new();
                while it.next() == ValueType::Float {
                    let (t, v) = it.at();
                    samples.push(fs(t, v));
                }
                series_set.push(samples);
            }
            assert!(ss.err().is_none());
            assert!(ss.warnings().is_empty());
            if let Some(series) = series_set.first() {
                result = series[series.len() - 1].f as i64;
            }
        }
        storage::LabelQuerier::close(&mut querier);
        result
    };

    let add_samples = |h: &Head| -> i64 {
        let mut i = 1i64;
        while i <= 1000 {
            let mut app: Box<dyn AppenderV2> = if h.min_time() == i64::MAX {
                // To initialize bounds.
                // adapted: Go builds `&initAppenderV2{head: h}` directly; `Head::appender_v2` returns the same
                // init appender while the head is not initialized (it also counts the appender metrics).
                h.appender_v2(&ctx())
            } else {
                let mut a = h.head_appender_v2();
                a.base.cleanup_append_ids_below = 0;
                Box::new(a)
            };
            app_f(&mut *app, &ls(&["foo", "bar"]), i, i as f64).unwrap();
            assert!(app.commit().is_none());
            h.mmap_head_chunks();
            i += 1;
        }
        i
    };

    // Test isolation without restart of Head.
    let hb = new_test_head("mem-series-isolation", 1000, compression::Type::None, false);
    let mut i = add_samples(&hb);

    // Test simple cases in different chunks when no appendID cleanup has been performed.
    assert_eq!(10, last_value(&hb, 10));
    assert_eq!(130, last_value(&hb, 130));
    assert_eq!(160, last_value(&hb, 160));
    assert_eq!(240, last_value(&hb, 240));
    assert_eq!(500, last_value(&hb, 500));
    assert_eq!(750, last_value(&hb, 750));
    assert_eq!(995, last_value(&hb, 995));
    assert_eq!(999, last_value(&hb, 999));

    // Cleanup appendIDs below 500.
    let mut app = hb.head_appender_v2();
    app.base.cleanup_append_ids_below = 500;
    app_f(&mut app, &ls(&["foo", "bar"]), i, i as f64).unwrap();
    assert!(storage::AppenderTransaction::commit(&mut app).is_none());
    i += 1;

    // We should not get queries with a maxAppendID below 500 after the cleanup, but they only take the remaining
    // appendIDs into account.
    assert_eq!(499, last_value(&hb, 10));
    assert_eq!(499, last_value(&hb, 130));
    assert_eq!(499, last_value(&hb, 160));
    assert_eq!(499, last_value(&hb, 240));
    assert_eq!(500, last_value(&hb, 500));
    assert_eq!(995, last_value(&hb, 995));
    assert_eq!(999, last_value(&hb, 999));

    // Cleanup appendIDs below 1000, which means the sample buffer is the only thing with appendIDs.
    let mut app = hb.head_appender_v2();
    app.base.cleanup_append_ids_below = 1000;
    app_f(&mut app, &ls(&["foo", "bar"]), i, i as f64).unwrap();
    assert!(storage::AppenderTransaction::commit(&mut app).is_none());
    assert_eq!(999, last_value(&hb, 998));
    assert_eq!(999, last_value(&hb, 999));
    assert_eq!(1000, last_value(&hb, 1000));
    assert_eq!(1001, last_value(&hb, 1001));
    assert_eq!(1002, last_value(&hb, 1002));
    assert_eq!(1002, last_value(&hb, 1003));

    i += 1;
    // Cleanup appendIDs below 1001, but with a rollback.
    let mut app = hb.head_appender_v2();
    app.base.cleanup_append_ids_below = 1001;
    app_f(&mut app, &ls(&["foo", "bar"]), i, i as f64).unwrap();
    assert!(storage::AppenderTransaction::rollback(&mut app).is_none());
    assert_eq!(1000, last_value(&hb, 999));
    assert_eq!(1000, last_value(&hb, 1000));
    assert_eq!(1001, last_value(&hb, 1001));
    assert_eq!(1002, last_value(&hb, 1002));
    assert_eq!(1002, last_value(&hb, 1003));

    hb.close().unwrap();

    // Test isolation with restart of Head. This is to verify the num samples of chunks after m-map chunk replay.
    let hb = new_test_head("mem-series-isolation-restart", 1000, compression::Type::None, false);
    let mut i = add_samples(&hb);
    hb.close().unwrap();

    let wal = WL::new_size(wal_dir(&hb), 32768, compression::Type::None).unwrap();
    let opts = HeadOptions {
        chunk_range: 1000,
        chunk_dir_root: wal.dir().to_path_buf(),
        ..HeadOptions::default()
    };
    let hb = open_head(wal, opts, 0);

    // No appends after restarting. Hence all should return the last value.
    assert_eq!(1000, last_value(&hb, 10));
    assert_eq!(1000, last_value(&hb, 130));
    assert_eq!(1000, last_value(&hb, 160));
    assert_eq!(1000, last_value(&hb, 240));
    assert_eq!(1000, last_value(&hb, 500));

    // Cleanup appendIDs below 1000, which means the sample buffer is the only thing with appendIDs.
    let mut app = hb.head_appender_v2();
    let r = app_f(&mut app, &ls(&["foo", "bar"]), i, i as f64);
    i += 1;
    r.unwrap();
    assert!(storage::AppenderTransaction::commit(&mut app).is_none());
    assert_eq!(1001, last_value(&hb, 998));
    assert_eq!(1001, last_value(&hb, 999));
    assert_eq!(1001, last_value(&hb, 1000));
    assert_eq!(1001, last_value(&hb, 1001));
    assert_eq!(1001, last_value(&hb, 1002));
    assert_eq!(1001, last_value(&hb, 1003));

    // Cleanup appendIDs below 1002, but with a rollback.
    let mut app = hb.head_appender_v2();
    app_f(&mut app, &ls(&["foo", "bar"]), i, i as f64).unwrap();
    assert!(storage::AppenderTransaction::rollback(&mut app).is_none());
    assert_eq!(1001, last_value(&hb, 999));
    assert_eq!(1001, last_value(&hb, 1000));
    assert_eq!(1001, last_value(&hb, 1001));
    assert_eq!(1001, last_value(&hb, 1002));
    assert_eq!(1001, last_value(&hb, 1003));
}

#[test]
fn test_head_appender_v2_isolation_rollback() {
    // Rollback after a failed append and test if the low watermark has progressed anyway.
    let hb = new_test_head("isolation-rollback", 1000, compression::Type::None, false);

    let mut app = hb.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["foo", "bar"]), 0, 0.0).unwrap();
    assert!(app.commit().is_none());
    assert_eq!(1, hb.iso.low_watermark());

    let mut app = hb.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["foo", "bar"]), 1, 1.0).unwrap();
    assert!(app_f(&mut *app, &ls(&["foo", "bar", "foo", "baz"]), 2, 2.0).is_err());
    assert!(app.rollback().is_none());
    assert_eq!(2, hb.iso.low_watermark());

    let mut app = hb.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["foo", "bar"]), 3, 3.0).unwrap();
    assert!(app.commit().is_none());
    assert_eq!(3, hb.iso.low_watermark(), "Low watermark should proceed to 3 even if append #2 was rolled back.");
}

#[test]
fn test_head_appender_v2_isolation_low_watermark_monotonous() {
    let hb = new_test_head("isolation-lwm-monotonous", 1000, compression::Type::None, false);

    let mut app1 = hb.appender_v2(&ctx());
    app_f(&mut *app1, &ls(&["foo", "bar"]), 0, 0.0).unwrap();
    assert!(app1.commit().is_none());
    assert_eq!(1, hb.iso.low_watermark(), "Low watermark should by 1 after 1st append.");

    let mut app1 = hb.appender_v2(&ctx());
    app_f(&mut *app1, &ls(&["foo", "bar"]), 1, 1.0).unwrap();
    assert_eq!(2, hb.iso.low_watermark(), "Low watermark should be two, even if append is not committed yet.");

    let mut app2 = hb.appender_v2(&ctx());
    app_f(&mut *app2, &ls(&["foo", "baz"]), 1, 1.0).unwrap();
    assert!(app2.commit().is_none());
    assert_eq!(2, hb.iso.low_watermark(), "Low watermark should stay two because app1 is not committed yet.");

    let is = hb.iso.state(i64::MIN, i64::MAX);
    assert_eq!(2, hb.iso.low_watermark(), "After simulated read (iso state retrieved), low watermark should stay at 2.");

    assert!(app1.commit().is_none());
    assert_eq!(
        2,
        hb.iso.low_watermark(),
        "Even after app1 is committed, low watermark should stay at 2 because read is still ongoing."
    );

    is.close();
    assert_eq!(3, hb.iso.low_watermark(), "After read has finished (iso state closed), low watermark should jump to three.");
}

#[test]
fn test_head_appender_v2_isolation_without_add() {
    let hb = new_test_head("isolation-without-add", 1000, compression::Type::None, false);

    let mut app = hb.appender_v2(&ctx());
    assert!(app.commit().is_none());

    let mut app = hb.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["foo", "baz"]), 1, 1.0).unwrap();
    assert!(app.commit().is_none());

    assert_eq!(hb.iso.last_append_id(), hb.iso.low_watermark(), "High watermark should be equal to the low watermark");
}

/// TestHeadAppenderV2_HistogramErrorDoesNotSetPendingCommit is the V2 counterpart of
/// TestAppendHistogramErrorDoesNotSetPendingCommit.
#[test]
fn test_head_appender_v2_histogram_error_does_not_set_pending_commit() {
    let cases: Vec<(&str, Option<Histogram>, Option<FloatHistogram>)> = vec![
        ("integer", Some(tu::generate_test_histogram(0)), None),
        ("float", None, Some(tu::generate_test_float_histogram(0))),
    ];
    for (name, h, fh) in cases {
        let head = new_test_head("histogram-error-pending", 1000, compression::Type::None, false);
        head.init(0).unwrap();

        let lbls = ls(&["a", "b"]);

        let mut app = head.appender_v2(&ctx());
        app.append(0, &lbls, 0, 200, 0.0, h.as_ref(), fh.as_ref(), &ao()).unwrap();
        assert!(app.commit().is_none());

        let (ms, _) = head.get_or_create(lbls.hash(), &lbls, false).unwrap();
        let pc = ms.lock().has_pending_commit();
        assert!(!pc, "{name}: pending state should be cleared after a successful commit");

        // Out-of-order append for the same series, OOO window disabled.
        let mut app = head.appender_v2(&ctx());
        let err = app.append(0, &lbls, 0, 100, 0.0, h.as_ref(), fh.as_ref(), &ao()).unwrap_err();
        assert!(err.is(&ERR_OUT_OF_ORDER_SAMPLE), "{name}: {err}");
        assert!(app.rollback().is_none());

        let pc = ms.lock().has_pending_commit();
        assert!(!pc, "{name}: pending state should remain clear after a failed AppenderV2.Append");
    }
}

#[test]
fn test_head_label_names_values_with_min_max_range_appender_v2() {
    let head = new_test_head("label-names-values-range", 1000, compression::Type::None, false);

    const FIRST_SERIES_TIMESTAMP: i64 = 100;
    const SECOND_SERIES_TIMESTAMP: i64 = 200;
    const LAST_SERIES_TIMESTAMP: i64 = 300;
    let series_timestamps = [FIRST_SERIES_TIMESTAMP, SECOND_SERIES_TIMESTAMP, LAST_SERIES_TIMESTAMP];
    let expected_label_names = ["a", "b", "c"];
    let expected_label_values = ["d", "e", "f"];

    let mut app = head.appender_v2(&ctx());
    for (i, name) in expected_label_names.iter().enumerate() {
        app_f(&mut *app, &ls(&[name, expected_label_values[i]]), series_timestamps[i], 0.0).unwrap();
    }
    assert!(app.commit().is_none());
    assert_eq!(FIRST_SERIES_TIMESTAMP, head.min_time());
    assert_eq!(LAST_SERIES_TIMESTAMP, head.max_time());

    let empty: Vec<&str> = vec![];
    let test_cases: Vec<(&str, i64, i64, Vec<&str>, Vec<&str>)> = vec![
        ("maxt less than head min", head.max_time() - 10, head.min_time() - 10, empty.clone(), empty.clone()),
        ("mint less than head max", head.max_time() + 10, head.min_time() + 10, empty.clone(), empty.clone()),
        ("mint and maxt outside head", head.max_time() + 10, head.min_time() - 10, empty.clone(), empty.clone()),
        (
            "mint and maxt within head",
            head.max_time() - 10,
            head.min_time() + 10,
            expected_label_names.to_vec(),
            expected_label_values.to_vec(),
        ),
    ];

    for (name, mint, maxt, expected_names, expected_values) in test_cases {
        let head_idx_reader = head.index_range(mint, maxt);
        let actual_label_names = head_idx_reader.label_names(&ctx(), &[]).unwrap();
        assert_eq!(expected_names, actual_label_names, "{name}");
        if !expected_values.is_empty() {
            for (i, n) in expected_label_names.iter().enumerate() {
                let actual_label_value = head_idx_reader.sorted_label_values(&ctx(), n, None, &[]).unwrap();
                assert_eq!(vec![expected_values[i].to_string()], actual_label_value, "{name}");
            }
        }
    }
}

#[test]
fn test_head_appender_v2_err_reuse() {
    let head = new_test_head("err-reuse", 1000, compression::Type::None, false);

    let mut app = head.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["test", "test"]), 0, 0.0).unwrap();
    assert!(app.commit().is_none());
    assert!(app.commit().is_some());
    assert!(app.rollback().is_some());

    let mut app = head.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["test", "test"]), 1, 0.0).unwrap();
    assert!(app.rollback().is_none());
    assert!(app.rollback().is_some());
    assert!(app.commit().is_some());

    let mut app = head.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["test", "test"]), 2, 0.0).unwrap();
    assert!(app.commit().is_none());
    assert!(app.rollback().is_some());
    assert!(app.commit().is_some());

    let mut app = head.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["test", "test"]), 3, 0.0).unwrap();
    assert!(app.rollback().is_none());
    assert!(app.commit().is_some());
    assert!(app.rollback().is_some());
}

#[test]
fn test_head_appender_v2_min_time_after_truncation() {
    let chunk_range = 2000i64;
    let head = new_test_head("min-time-after-truncation", chunk_range, compression::Type::None, false);

    let mut app = head.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["a", "b"]), 100, 100.0).unwrap();
    app_f(&mut *app, &ls(&["a", "b"]), 4000, 200.0).unwrap();
    app_f(&mut *app, &ls(&["a", "b"]), 8000, 300.0).unwrap();
    assert!(app.commit().is_none());

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

    head.close().unwrap();
}

#[test]
fn test_head_appender_v2_append_exemplars() {
    let chunk_range = 2000i64;
    let head = new_test_head("append-exemplars", chunk_range, compression::Type::None, false);
    let mut app = head.appender_v2(&ctx());

    let l = ls(&["trace_id", "123"]);

    // It is perfectly valid to add Exemplars before the current start time - histogram buckets that haven't been
    // update in a while could still be exported exemplars from an hour ago.
    let ex = [Exemplar { labels: l, has_ts: true, ts: -1000, value: 1.0 }];
    let opts = AppendV2Options { exemplars: &ex, ..Default::default() };
    app.append(0, &ls(&["a", "b"]), 0, 100, 100.0, None, None, &opts).unwrap();
    assert!(app.commit().is_none());
    head.close().unwrap();
}
