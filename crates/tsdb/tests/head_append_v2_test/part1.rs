//! Port of `tsdb/head_append_v2_test.go`, lines 1..2831.
//!
//! (The tests with a DB are at the end of this file.)

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
        enable_memory_snapshot_on_shutdown: AtomicBool::new(o.enable_memory_snapshot_on_shutdown.load(Ordering::Relaxed)),
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
    #[allow(clippy::type_complexity)]
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

/// Reads all samples of the single series `{a="b"}` as histogram and float histogram samples (the loops of
/// TestHeadAppenderV2_Append_Histogram).
fn read_histograms(h: &Head, mint: i64, maxt: i64) -> (Vec<Sample>, Vec<Sample>) {
    let mut q = new_block_querier(h, mint, maxt).unwrap();
    let mut act_histograms = Vec::new();
    let mut act_float_histograms = Vec::new();
    {
        let mut ss = q.select(&ctx(), false, None, &[m(MatchType::Equal, "a", "b")]);
        assert!(ss.next());
        let s = ss.at().clone();
        assert!(!ss.next());
        let mut it = s.iterator(None);
        loop {
            match it.next() {
                ValueType::None => break,
                ValueType::Histogram => {
                    let (ts, h) = it.at_histogram(None);
                    act_histograms.push(Sample::histogram(0, ts, h));
                }
                ValueType::FloatHistogram => {
                    let (ts, fh) = it.at_float_histogram(None);
                    act_float_histograms.push(Sample::float_histogram(0, ts, fh));
                }
                ValueType::Float => {}
            }
        }
    }
    assert!(q.close().is_none());
    (act_histograms, act_float_histograms)
}

fn dummy(v: Vec<Sample>) -> BTreeMap<String, Vec<Sample>> {
    [("dummy".to_string(), v)].into_iter().collect()
}

#[test]
fn test_head_appender_v2_append_histogram() {
    let l = ls(&["a", "b"]);
    for num_histograms in [1usize, 10, 150, 200, 250, 300] {
        let head = new_test_head("append-histogram", 1000, compression::Type::None, false);
        head.init(0).unwrap();
        let mut ingest_ts = 0i64;
        let mut app = head.appender_v2(&ctx());

        let mut exp_histograms = Vec::with_capacity(2 * num_histograms);

        // Counter integer histograms, then gauge integer histograms.
        for hs in [tu::generate_test_histograms(num_histograms), tu::generate_test_gauge_histograms(num_histograms)] {
            for h in hs {
                app.append(0, &l, 0, ingest_ts, 0.0, Some(&h), None, &ao()).unwrap();
                exp_histograms.push(Sample::histogram(0, ingest_ts, h));
                ingest_ts += 1;
                if ingest_ts % 50 == 0 {
                    assert!(app.commit().is_none());
                    app = head.appender_v2(&ctx());
                }
            }
        }

        let mut exp_float_histograms = Vec::with_capacity(2 * num_histograms);

        // Counter float histograms, then gauge float histograms.
        for fhs in
            [tu::generate_test_float_histograms(num_histograms), tu::generate_test_gauge_float_histograms(num_histograms)]
        {
            for fh in fhs {
                app.append(0, &l, 0, ingest_ts, 0.0, None, Some(&fh), &ao()).unwrap();
                exp_float_histograms.push(Sample::float_histogram(0, ingest_ts, fh));
                ingest_ts += 1;
                if ingest_ts % 50 == 0 {
                    assert!(app.commit().is_none());
                    app = head.appender_v2(&ctx());
                }
            }
        }

        assert!(app.commit().is_none());

        let (act_histograms, act_float_histograms) = read_histograms(&head, head.min_time(), head.max_time());

        compare_series(&dummy(exp_histograms), &dummy(act_histograms));
        compare_series(&dummy(exp_float_histograms), &dummy(act_float_histograms));
    }
}

#[test]
fn test_histogram_in_wal_and_mmap_chunk_appender_v2() {
    let th = new_test_head("histogram-in-wal-and-mmap", 3000, compression::Type::None, false);
    let mut head = HeadGuard(th.head.clone());
    head.init(0).unwrap();

    // Series with only histograms.
    let s1 = ls(&["a", "b1"]);
    let k1 = s1.to_string();
    let num_histograms = 300;
    let mut exp: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
    let mut ts = 0i64;
    for gauge in [true, false] {
        let mut app = head.appender_v2(&ctx());
        let hists = if gauge {
            tu::generate_test_gauge_histograms(num_histograms)
        } else {
            tu::generate_test_histograms(num_histograms)
        };
        for mut h in hists {
            h.negative_spans = h.positive_spans.clone();
            h.negative_buckets = h.positive_buckets.clone();
            app.append(0, &s1, 0, ts, 0.0, Some(&h), None, &ao()).unwrap();
            exp.entry(k1.clone()).or_default().push(Sample::histogram(0, ts, h.copy()));
            ts += 1;
            if ts % 5 == 0 {
                assert!(app.commit().is_none());
                app = head.appender_v2(&ctx());
            }
        }
        assert!(app.commit().is_none());
    }
    for gauge in [true, false] {
        let mut app = head.appender_v2(&ctx());
        let hists = if gauge {
            tu::generate_test_gauge_float_histograms(num_histograms)
        } else {
            tu::generate_test_float_histograms(num_histograms)
        };
        for mut h in hists {
            h.negative_spans = h.positive_spans.clone();
            h.negative_buckets = h.positive_buckets.clone();
            app.append(0, &s1, 0, ts, 0.0, None, Some(&h), &ao()).unwrap();
            exp.entry(k1.clone()).or_default().push(Sample::float_histogram(0, ts, h.copy()));
            ts += 1;
            if ts % 5 == 0 {
                assert!(app.commit().is_none());
                app = head.appender_v2(&ctx());
            }
        }
        assert!(app.commit().is_none());
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
    // Commits, adds 10 float samples and commits again (the inner block of both histogram loops below).
    let add_floats =
        |head: &Head, app: &mut Box<dyn AppenderV2>, ts: &mut i64, exp: &mut BTreeMap<String, Vec<Sample>>| {
            assert!(app.commit().is_none());
            *app = head.appender_v2(&ctx());
            // Add some float.
            for _ in 0..10 {
                *ts += 1;
                app_f(&mut **app, &s2, *ts, *ts as f64).unwrap();
                exp.entry(k2.clone()).or_default().push(fs(*ts, *ts as f64));
            }
            assert!(app.commit().is_none());
            *app = head.appender_v2(&ctx());
        };
    for gauge in [true, false] {
        let mut app = head.appender_v2(&ctx());
        let hists = if gauge { tu::generate_test_gauge_histograms(100) } else { tu::generate_test_histograms(100) };
        for mut h in hists {
            ts += 1;
            h.negative_spans = h.positive_spans.clone();
            h.negative_buckets = h.positive_buckets.clone();
            app.append(0, &s2, 0, ts, 0.0, Some(&h), None, &ao()).unwrap();
            let mut eh = h.copy();
            if !gauge && ts > 30 && (ts - 10) % 20 == 1 {
                // Need "unknown" hint after float sample.
                eh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            }
            exp.entry(k2.clone()).or_default().push(Sample::histogram(0, ts, eh));
            if ts % 20 == 0 {
                add_floats(&head, &mut app, &mut ts, &mut exp);
            }
        }
        assert!(app.commit().is_none());
    }
    for gauge in [true, false] {
        let mut app = head.appender_v2(&ctx());
        let hists = if gauge {
            tu::generate_test_gauge_float_histograms(100)
        } else {
            tu::generate_test_float_histograms(100)
        };
        for mut h in hists {
            ts += 1;
            h.negative_spans = h.positive_spans.clone();
            h.negative_buckets = h.positive_buckets.clone();
            app.append(0, &s2, 0, ts, 0.0, None, Some(&h), &ao()).unwrap();
            let mut eh = h.copy();
            if !gauge && ts > 30 && (ts - 10) % 20 == 1 {
                // Need "unknown" hint after float sample.
                eh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            }
            exp.entry(k2.clone()).or_default().push(Sample::float_histogram(0, ts, eh));
            if ts % 20 == 0 {
                add_floats(&head, &mut app, &mut ts, &mut exp);
            }
        }
        assert!(app.commit().is_none());
    }

    // Restart head.
    let wal_dir = wal_dir(&th);
    head.close().unwrap();
    let start_head = |old: &Head| -> HeadGuard {
        let w = WL::new_size(&wal_dir, 32768, compression::Type::None).unwrap();
        open_head(w, copy_opts(&old.opts), 0)
    };
    head = start_head(&head);

    // Checking contents of s1.
    let ms = get_series(&head, &s1).unwrap();
    {
        let g = ms.lock();
        assert_eq!(exp_mmap_chunks, g.mmapped_chunks);
        assert_eq!(exp_head_chunk_samples, g.head_chunk().unwrap().chunk.num_samples());
    }

    let test_query = |head: &Head| {
        let q = new_block_querier(head, head.min_time(), head.max_time()).unwrap();
        let act = query(q, &[m(MatchType::Regexp, "a", "b.*")]);
        compare_series(&exp, &act);
    };
    test_query(&head);

    // Restart with no mmap chunks to test WAL replay.
    head.close().unwrap();
    std::fs::remove_dir_all(mmapped_chunks_dir(&head.opts.chunk_dir_root)).unwrap();
    head = start_head(&head);
    test_query(&head);
}

/// An exemplar with the labels of its series (Go `ex` in TestChunkSnapshot_AppenderV2).
#[derive(Clone, Debug, PartialEq)]
struct Ex {
    series_labels: Labels,
    e: Exemplar,
}

/// Go `rand.Int()`.
fn rand_int() -> i64 {
    let mut b = [0u8; 8];
    crypto::random_bytes(&mut b);
    (u64::from_le_bytes(b) >> 1) as i64
}

type SampleMap = BTreeMap<String, Vec<Sample>>;

#[test]
fn test_chunk_snapshot_appender_v2() {
    for enable_st_storage in [false, true] {
        let name = format!("enableSTStorage={enable_st_storage}");
        let th = new_test_head("chunk-snapshot", 120 * 4, compression::Type::None, false);
        let mut head = HeadGuard(th.head.clone());

        let num_series = 10;
        let mut exp_series = SampleMap::new();
        let mut exp_hist = SampleMap::new();
        let mut exp_float_hist = SampleMap::new();
        // Go adds the intervals with `expTombstones[ref].Add(itv)`, whose result (a new slice) is discarded, so the
        // expected (and actual) maps stay empty; ported as is.
        let exp_tombstones: BTreeMap<u64, Intervals> = BTreeMap::new();
        let mut exp_exemplars: Vec<Ex> = Vec::new();
        let histograms = tu::generate_test_gauge_histograms(481);
        let float_histogram = tu::generate_test_gauge_float_histograms(481);

        let new_exemplar = |exp_exemplars: &mut Vec<Ex>, lbls: &Labels, ts: i64| -> Exemplar {
            let e = Ex {
                series_labels: lbls.clone(),
                e: Exemplar {
                    labels: ls(&["trace_id", &rand_int().to_string()]),
                    value: rand_f64(),
                    ts,
                    has_ts: false,
                },
            };
            exp_exemplars.push(e.clone());
            e.e
        };

        let check_query = |head: &Head, lname: &str, re: &str, exp: &SampleMap| {
            let q = new_block_querier(head, i64::MIN, i64::MAX).unwrap();
            let series = query(q, &[m(MatchType::Regexp, lname, re)]);
            assert_eq!(exp, &series, "{name}");
        };
        let check_tombstones = |head: &Head| {
            let tr = tsdb::BlockReader::tombstones(head).unwrap();
            let act_tombstones: BTreeMap<u64, Intervals> = BTreeMap::new();
            tr.iter(&mut |_r, _itvs| Ok(())).unwrap();
            assert_eq!(exp_tombstones, act_tombstones, "{name}");
        };
        let check_exemplars = |head: &Head, exp_exemplars: &[Ex]| {
            let mut act_exemplars: Vec<Ex> = Vec::with_capacity(exp_exemplars.len());
            head.exemplars()
                .iterate_exemplars(&mut |series_labels, e| {
                    act_exemplars.push(Ex { series_labels: series_labels.clone(), e: e.clone() });
                    Ok(())
                })
                .unwrap();
            // Verifies both existence of right exemplars and order of exemplars in the buffer.
            assert_eq!(exp_exemplars, &act_exemplars[..], "{name}");
        };

        let wal = wal_dir(&th);
        let close_head_and_check_snapshot = |head: &Head, wlast: i64, woffset: i64| {
            head.close().unwrap();
            let (_, sidx, soffset) = tsdb::head_wal::last_chunk_snapshot(&head.opts.chunk_dir_root).unwrap();
            assert_eq!(wlast, sidx, "{name}");
            assert_eq!(woffset, soffset, "{name}");
        };
        let open_head_and_check_replay = |opts: HeadOptions,
                                          exp_series: &SampleMap,
                                          exp_hist: &SampleMap,
                                          exp_float_hist: &SampleMap,
                                          exp_exemplars: &[Ex]|
         -> HeadGuard {
            let w = WL::new_size(&wal, 32768, compression::Type::None).unwrap();
            let head = open_head(w, opts, i64::MIN);
            check_query(&head, "foo", "bar.*", exp_series);
            check_query(&head, "hist", "baz.*", exp_hist);
            check_query(&head, "floathist", "bat.*", exp_float_hist);
            check_tombstones(&head);
            check_exemplars(&head, exp_exemplars);
            head
        };

        // Appends the samples of [from, to] of all series, with an exemplar and a commit every 10 samples, and
        // logs tombstones (the two data blocks of the Go test).
        #[allow(clippy::too_many_arguments)]
        let add_data = |head: &Head,
                        from: i64,
                        to: i64,
                        itvs: Intervals,
                        exp_series: &mut SampleMap,
                        exp_hist: &mut SampleMap,
                        exp_float_hist: &mut SampleMap,
                        exp_exemplars: &mut Vec<Ex>| {
            let mut app = head.appender_v2(&ctx());
            for i in 1..=num_series {
                let lbls = ls(&["foo", &format!("bar{i}")]);
                let lbl_str = lbls.to_string();
                let lbls_hist = ls(&["hist", &format!("baz{i}")]);
                let lbls_hist_str = lbls_hist.to_string();
                let lbls_float_hist = ls(&["floathist", &format!("bat{i}")]);
                let lbls_float_hist_str = lbls_float_hist.to_string();

                // 240 samples should m-map at least 1 chunk.
                for ts in from..=to {
                    // Add an exemplar, but only to float sample.
                    let ex: Vec<Exemplar> =
                        if ts % 10 == 0 { vec![new_exemplar(exp_exemplars, &lbls, ts)] } else { vec![] };
                    let a_opts = AppendV2Options { exemplars: &ex, ..Default::default() };
                    let val = rand_f64();
                    exp_series.entry(lbl_str.clone()).or_default().push(fs(ts, val));
                    app.append(0, &lbls, 0, ts, val, None, None, &a_opts).unwrap();

                    let hist = &histograms[ts as usize];
                    exp_hist.entry(lbls_hist_str.clone()).or_default().push(Sample::histogram(0, ts, hist.clone()));
                    app.append(0, &lbls_hist, 0, ts, 0.0, Some(hist), None, &ao()).unwrap();

                    let float_hist = &float_histogram[ts as usize];
                    exp_float_hist
                        .entry(lbls_float_hist_str.clone())
                        .or_default()
                        .push(Sample::float_histogram(0, ts, float_hist.clone()));
                    app.append(0, &lbls_float_hist, 0, ts, 0.0, None, Some(float_hist), &ao()).unwrap();

                    // Create multiple WAL records (commit).
                    if ts % 10 == 0 {
                        assert!(app.commit().is_none());
                        app = head.appender_v2(&ctx());
                    }
                }
            }
            assert!(app.commit().is_none());

            // Add some tombstones.
            let enc = record::Encoder { enable_st_storage };
            for i in 1..=num_series {
                let r = i as u64;
                head.tombstones.add_interval(r, &itvs);
                let mut buf = Vec::new();
                enc.tombstones(&[tombstones::Stone { r#ref: r, intervals: itvs.clone() }], &mut buf);
                head.wal().unwrap().log(&buf).unwrap();
            }
        };

        let last_segment_and_offset = |head: &Head| -> (i64, i64) {
            let (wlast, mut woffset) = head.wal().unwrap().last_segment_and_offset().unwrap();
            if woffset != 0 && woffset < 32 * 1024 {
                // The page is always filled before taking the snapshot.
                woffset = 32 * 1024;
            }
            (wlast, woffset)
        };

        // Initial data that goes into snapshot. Add some initial samples with >=1 m-map chunk.
        add_data(
            &head,
            1,
            240,
            vec![Interval { mint: 1234, maxt: 2345 }, Interval { mint: 3456, maxt: 4567 }],
            &mut exp_series,
            &mut exp_hist,
            &mut exp_float_hist,
            &mut exp_exemplars,
        );

        // These references should be the ones used for the snapshot.
        let (wlast, woffset) = last_segment_and_offset(&head);

        {
            // Creating snapshot and verifying it.
            head.opts.enable_memory_snapshot_on_shutdown.store(true, Ordering::Relaxed);
            close_head_and_check_snapshot(&head, wlast, woffset); // This will create a snapshot.

            // Test the replay of snapshot.
            head = open_head_and_check_replay(
                copy_opts(&head.opts),
                &exp_series,
                &exp_hist,
                &exp_float_hist,
                &exp_exemplars,
            );
        }

        // Additional data to only include in WAL and m-mapped chunks and not snapshot. This mimics having an old
        // snapshot on disk.
        add_data(
            &head,
            241,
            480,
            vec![Interval { mint: 12345, maxt: 23456 }, Interval { mint: 34567, maxt: 45678 }],
            &mut exp_series,
            &mut exp_hist,
            &mut exp_float_hist,
            &mut exp_exemplars,
        );
        {
            // Close Head and verify that new snapshot was not created.
            head.opts.enable_memory_snapshot_on_shutdown.store(false, Ordering::Relaxed);
            close_head_and_check_snapshot(&head, wlast, woffset); // This should not create a snapshot.

            // Test the replay of snapshot, m-map chunks, and WAL.
            head.opts.enable_memory_snapshot_on_shutdown.store(true, Ordering::Relaxed); // Enabled to read from snapshot.
            head = open_head_and_check_replay(
                copy_opts(&head.opts),
                &exp_series,
                &exp_hist,
                &exp_float_hist,
                &exp_exemplars,
            );
        }

        // Creating another snapshot should delete the older snapshot and replay still works fine.
        let (wlast, woffset) = last_segment_and_offset(&head);

        {
            // Close Head and verify that new snapshot was created.
            close_head_and_check_snapshot(&head, wlast, woffset);

            // Verify that there is only 1 snapshot.
            let mut files: Vec<String> = std::fs::read_dir(&head.opts.chunk_dir_root)
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            files.sort();
            let mut snapshots = 0;
            for f in files.iter().rev() {
                if f.starts_with("chunk_snapshot.") {
                    snapshots += 1;
                    assert_eq!(tsdb::head_wal::chunk_snapshot_dir(wlast, woffset), *f, "{name}");
                }
            }
            assert_eq!(1, snapshots, "{name}");

            // Test the replay of snapshot.
            head.opts.enable_memory_snapshot_on_shutdown.store(true, Ordering::Relaxed); // Enabled to read from snapshot.

            // Disabling exemplars to check that it does not hard fail replay
            // https://github.com/prometheus/prometheus/issues/9437#issuecomment-933285870.
            let mut opts = copy_opts(&head.opts);
            opts.enable_exemplar_storage = false;
            opts.max_exemplars.store(0, Ordering::Relaxed);
            exp_exemplars.clear();

            head = open_head_and_check_replay(opts, &exp_series, &exp_hist, &exp_float_hist, &exp_exemplars);

            assert_eq!(0.0, head.metrics.snapshot_replay_error_total.get(), "{name}");
        }

        head.opts.enable_memory_snapshot_on_shutdown.store(false, Ordering::Relaxed);
        head.close().unwrap();
    }
}

/// Go `countSeriesLifecycleCallback` (head_test.go).
#[derive(Default)]
struct CountSeriesLifecycleCallback {
    created: AtomicI64,
    deleted: AtomicI64,
}

impl tsdb::head::SeriesLifecycleCallback for CountSeriesLifecycleCallback {
    fn pre_creation(&self, _: &Labels) -> Result<(), storage::Error> {
        Ok(())
    }
    fn post_creation(&self, _: &Labels) {
        self.created.fetch_add(1, Ordering::SeqCst);
    }
    fn post_deletion(&self, s: &std::collections::HashMap<u64, Labels>) {
        self.deleted.fetch_add(s.len() as i64, Ordering::SeqCst);
    }
}

fn write_at(path: &Path, buf: &[u8], off: u64) {
    use std::os::unix::fs::FileExt;
    let f = std::fs::OpenOptions::new().read(true).write(true).open(path).unwrap();
    f.write_all_at(buf, off).unwrap();
}

#[test]
fn test_snapshot_error_appender_v2() {
    let th = new_test_head("snapshot-error", 120 * 4, compression::Type::None, false);
    let mut head = HeadGuard(th.head.clone());

    // Add a sample.
    let mut app = head.appender_v2(&ctx());
    let lbls = ls(&["foo", "bar"]);
    app_f(&mut *app, &lbls, 99, 99.0).unwrap();

    // Add histograms.
    let hist = tu::generate_test_gauge_histograms(1).remove(0);
    let float_hist = tu::generate_test_gauge_float_histograms(1).remove(0);
    let lbls_hist = ls(&["hist", "bar"]);
    let lbls_float_hist = ls(&["floathist", "bar"]);

    app.append(0, &lbls_hist, 0, 99, 0.0, Some(&hist), None, &ao()).unwrap();
    app.append(0, &lbls_float_hist, 0, 99, 0.0, None, Some(&float_hist), &ao()).unwrap();

    assert!(app.commit().is_none());

    // Add some tombstones.
    let itvs = vec![Interval { mint: 1234, maxt: 2345 }, Interval { mint: 3456, maxt: 4567 }];
    head.tombstones.add_interval(1, &itvs);

    // Check existence of data.
    assert!(get_series(&head, &lbls).is_some());
    let tm = head.tombstones.get(1);
    assert!(!tm.is_empty());

    head.opts.enable_memory_snapshot_on_shutdown.store(true, Ordering::Relaxed);
    head.close().unwrap(); // This will create a snapshot.

    // Remove the WAL so that we don't load from it.
    let wal = wal_dir(&th);
    std::fs::remove_dir_all(&wal).unwrap();

    // Corrupt the snapshot.
    let (snap_dir, _, _) = tsdb::head_wal::last_chunk_snapshot(&head.opts.chunk_dir_root).unwrap();
    let mut files: Vec<_> = std::fs::read_dir(&snap_dir).unwrap().map(|e| e.unwrap().path()).collect();
    files.sort();
    // Create snapshot backup to be restored on future test cases.
    let snapshot_backup = std::fs::read(&files[0]).unwrap();
    write_at(&files[0], &[0b11111111], 18);

    // Create new Head which should replay this snapshot.
    let w = WL::new_size(&wal, 32768, compression::Type::None).unwrap();
    // adapted: Go passes a new Prometheus registry here (issue 9437, double registration); the Rust head has no
    // registry.
    head = open_head(w, copy_opts(&head.opts), i64::MIN);

    // There should be no series in the memory after snapshot error since WAL was removed.
    assert_eq!(1.0, head.metrics.snapshot_replay_error_total.get());
    assert_eq!(0, head.num_series());
    assert!(get_series(&head, &lbls).is_none());
    let tm = head.tombstones.get(1);
    assert!(tm.is_empty());
    head.close().unwrap();

    // Test corruption in the middle of the snapshot.
    write_at(&files[0], &snapshot_backup, 0);
    write_at(&files[0], &[0b11111111], 300);

    let c = Arc::new(CountSeriesLifecycleCallback::default());
    let mut opts = copy_opts(&head.opts);
    opts.series_callback = Some(c.clone());

    let w = WL::new_size(&wal, 32768, compression::Type::None).unwrap();
    head = open_head(w, opts, i64::MIN);

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

    head.opts.enable_memory_snapshot_on_shutdown.store(false, Ordering::Relaxed);
    head.close().unwrap();
}

#[test]
fn test_head_appender_v2_append_histogram_samples_appended_metric() {
    let num_histograms = 10usize;
    let th = new_test_head("histogram-samples-appended", 1000, compression::Type::None, false);
    let head = &th.head;
    head.init(0).unwrap();

    let mut exp_h_samples = 0;

    for x in 0..5 {
        let l = ls(&["a", &format!("b{x}")]);
        for (i, h) in tu::generate_test_histograms(num_histograms).iter().enumerate() {
            let mut app = head.appender_v2(&ctx());
            app.append(0, &l, 0, i as i64, 0.0, Some(h), None, &ao()).unwrap();
            assert!(app.commit().is_none());
            exp_h_samples += 1;
        }
        for (i, fh) in tu::generate_test_float_histograms(num_histograms).iter().enumerate() {
            let mut app = head.appender_v2(&ctx());
            app.append(0, &l, 0, (num_histograms + i) as i64, 0.0, None, Some(fh), &ao()).unwrap();
            assert!(app.commit().is_none());
            exp_h_samples += 1;
        }
    }

    assert_eq!(exp_h_samples as f64, head.metrics.samples_appended.with_label_values("histogram").get());

    head.close().unwrap();
    let w = WL::new_size(wal_dir(&th), 32768, compression::Type::None).unwrap();
    let head = open_head(w, copy_opts(&head.opts), 0);

    assert_eq!(0.0, head.metrics.samples_appended.with_label_values("histogram").get()); // Counter reset.
}

/// Go `timedHistogram` in testHeadAppenderV2AppendStaleHistogram.
#[derive(Clone, Debug, PartialEq)]
struct TimedHistogram {
    t: i64,
    h: Option<Histogram>,
    fh: Option<FloatHistogram>,
}

#[test]
fn test_head_appender_v2_append_stale_histogram() {
    // Subtests "integer histogram" and "float histogram".
    test_head_appender_v2_append_stale_histogram_impl(false);
    test_head_appender_v2_append_stale_histogram_impl(true);
}

fn test_head_appender_v2_append_stale_histogram_impl(float_histogram: bool) {
    let l = ls(&["a", "b"]);
    let num_histograms = 20usize;
    let head = new_test_head("stale-histogram", 100000, compression::Type::None, false);
    head.init(0).unwrap();

    let mut exp_histograms: Vec<TimedHistogram> = Vec::with_capacity(num_histograms);

    // Go mutates the expected histograms through their pointers (the counter reset hints), so the expectations are
    // taken mutably.
    let test_query = |exp_histograms: &mut Vec<TimedHistogram>, num_stale: usize| {
        let mut q = new_block_querier(head.head.as_ref(), head.min_time(), head.max_time()).unwrap();
        let mut act_histograms: Vec<TimedHistogram> = Vec::with_capacity(exp_histograms.len());
        {
            let mut ss = q.select(&ctx(), false, None, &[m(MatchType::Equal, "a", "b")]);
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

        // We cannot compare StaleNAN with require.Equal, hence checking each histogram manually.
        assert_eq!(exp_histograms.len(), act_histograms.len());
        let mut act_num_stale = 0;
        for i in 0..exp_histograms.len() {
            let mut ah = act_histograms[i].clone();
            let prev_unknown = i > 0 && {
                let prev = &exp_histograms[i - 1];
                if float_histogram {
                    prev.fh.as_ref().is_none_or(|p| is_stale_nan(p.sum))
                } else {
                    prev.h.as_ref().is_none_or(|p| is_stale_nan(p.sum))
                }
            };
            let eh_ref = &mut exp_histograms[i];
            let mut eh = eh_ref.clone();
            if float_histogram {
                if is_stale_nan(eh.fh.as_ref().unwrap().sum) {
                    act_num_stale += 1;
                    assert!(is_stale_nan(ah.fh.as_ref().unwrap().sum));
                    // To make require.Equal work.
                    ah.fh.as_mut().unwrap().sum = 0.0;
                    eh.fh.as_mut().unwrap().sum = 0.0;
                } else if prev_unknown {
                    eh_ref.fh.as_mut().unwrap().counter_reset_hint = CounterResetHint::UnknownCounterReset;
                    eh = eh_ref.clone();
                }
            } else if is_stale_nan(eh.h.as_ref().unwrap().sum) {
                act_num_stale += 1;
                assert!(is_stale_nan(ah.h.as_ref().unwrap().sum));
                // To make require.Equal work.
                ah.h.as_mut().unwrap().sum = 0.0;
                eh.h.as_mut().unwrap().sum = 0.0;
            } else if prev_unknown {
                eh_ref.h.as_mut().unwrap().counter_reset_hint = CounterResetHint::UnknownCounterReset;
                eh = eh_ref.clone();
            }
            assert_eq!(eh, ah, "float_histogram={float_histogram} i={i}");
        }
        assert_eq!(num_stale, act_num_stale);
    };

    let stale = f64::from_bits(STALE_NAN);
    let append_stale = |app: &mut dyn AppenderV2, exp_histograms: &mut Vec<TimedHistogram>| {
        // +1 so that delta-of-delta is not 0.
        let t = 100 * exp_histograms.len() as i64 + 1;
        app_f(app, &l, t, stale).unwrap();
        if float_histogram {
            exp_histograms.push(TimedHistogram {
                t,
                h: None,
                fh: Some(FloatHistogram { sum: stale, ..Default::default() }),
            });
        } else {
            exp_histograms.push(TimedHistogram {
                t,
                h: Some(Histogram { sum: stale, ..Default::default() }),
                fh: None,
            });
        }
    };
    let append_hists = |app: &mut dyn AppenderV2, exp_histograms: &mut Vec<TimedHistogram>, hs: &[Histogram]| {
        for h in hs {
            let t = 100 * exp_histograms.len() as i64;
            if float_histogram {
                app.append(0, &l, 0, t, 0.0, None, Some(&h.to_float()), &ao()).unwrap();
                exp_histograms.push(TimedHistogram { t, h: None, fh: Some(h.to_float()) });
            } else {
                app.append(0, &l, 0, t, 0.0, Some(h), None, &ao()).unwrap();
                exp_histograms.push(TimedHistogram { t, h: Some(h.clone()), fh: None });
            }
        }
    };

    // Adding stale in the same appender.
    let mut app = head.appender_v2(&ctx());
    append_hists(&mut *app, &mut exp_histograms, &tu::generate_test_histograms(num_histograms));
    append_stale(&mut *app, &mut exp_histograms);
    assert!(app.commit().is_none());

    // Only 1 chunk in the memory, no m-mapped chunk.
    let s = get_series(&head, &l).unwrap();
    {
        let g = s.lock();
        assert!(g.head_chunk().is_some());
        assert_eq!(1, g.head_chunks.len());
        assert!(g.mmapped_chunks.is_empty());
    }
    test_query(&mut exp_histograms, 1);

    // Adding stale in different appender and continuing series after a stale sample.
    let mut app = head.appender_v2(&ctx());
    append_hists(&mut *app, &mut exp_histograms, &tu::generate_test_histograms(2 * num_histograms)[num_histograms..]);
    assert!(app.commit().is_none());

    let mut app = head.appender_v2(&ctx());
    append_stale(&mut *app, &mut exp_histograms);
    assert!(app.commit().is_none());
    head.mmap_head_chunks();

    // Total 2 chunks, 1 m-mapped.
    let s = get_series(&head, &l).unwrap();
    {
        let g = s.lock();
        assert!(g.head_chunk().is_some());
        assert_eq!(1, g.head_chunks.len());
        assert_eq!(1, g.mmapped_chunks.len());
    }
    test_query(&mut exp_histograms, 2);
}

/// The counter reset header of a (float) histogram chunk.
fn chunk_counter_reset_header(chk: &dyn chunkenc::Chunk, float_histo: bool) -> CounterResetHeader {
    if float_histo {
        chk.as_any().downcast_ref::<FloatHistogramChunk>().expect("float histogram chunk").get_counter_reset_header()
    } else {
        chk.as_any().downcast_ref::<HistogramChunk>().expect("histogram chunk").get_counter_reset_header()
    }
}

#[test]
fn test_head_appender_v2_append_counter_reset_header() {
    for float_histo in [true, false] {
        let l = ls(&["a", "b"]);
        let head = new_test_head("counter-reset-header", 1000, compression::Type::None, false);
        head.init(0).unwrap();

        let mut ts = 0i64;
        let mut append_histogram = |h: &Histogram| {
            ts += 1;
            let mut app = head.appender_v2(&ctx());
            if float_histo {
                app.append(0, &l, 0, ts, 0.0, None, Some(&h.to_float()), &ao()).unwrap();
            } else {
                app.append(0, &l, 0, ts, 0.0, Some(&h.copy()), None, &ao()).unwrap();
            }
            assert!(app.commit().is_none());
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
                assert_eq!(exp_headers[i], chunk_counter_reset_header(&*chk, float_histo), "floatHistogram={float_histo}");
            }
            assert_eq!(
                exp_headers[exp_headers.len() - 1],
                chunk_counter_reset_header(&*g.head_chunk().unwrap().chunk, float_histo),
                "floatHistogram={float_histo}"
            );
        };
        let head_chunk_count = || {
            let (ms, _) = head.get_or_create(l.hash(), &l, false).unwrap();
            ms.lock().head_chunk_count()
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

/// Go `expOOOMmappedChunks`.
#[derive(Clone, Copy, Debug)]
struct ExpOooMmappedChunks {
    header: CounterResetHeader,
    mint: i64,
    maxt: i64,
    num_samples: u16,
}

#[test]
fn test_head_appender_v2_append_ooo_histogram_counter_reset_headers() {
    for float_histo in [true, false] {
        let l = ls(&["a", "b"]);
        let head = new_test_head("ooo-histogram-counter-reset", 1000, compression::Type::None, true);
        head.opts.out_of_order_cap_max.store(5, Ordering::Relaxed);
        head.init(0).unwrap();

        let append_histogram = |ts: i64, h: Histogram| {
            let mut app = head.appender_v2(&ctx());
            if float_histo {
                app.append(0, &l, 0, ts, 0.0, None, Some(&h.to_float()), &ao()).unwrap();
            } else {
                app.append(0, &l, 0, ts, 0.0, Some(&h.copy()), None, &ao()).unwrap();
            }
            assert!(app.commit().is_none());
        };

        let mut exp_chunks: Vec<ExpOooMmappedChunks> = Vec::new();
        let mut check_ooo_exp_counter_reset_header = |new_chunks: &[ExpOooMmappedChunks]| {
            exp_chunks.extend_from_slice(new_chunks);

            let (ms, _) = head.get_or_create(l.hash(), &l, false).unwrap();
            let g = ms.lock();
            let ooo_mmapped: &[tsdb::head::MmappedChunk] = g.ooo().map_or(&[], |o| &o.ooo_mmapped_chunks);
            assert_eq!(exp_chunks.len(), ooo_mmapped.len());

            for (i, mmap_chunk) in ooo_mmapped.iter().enumerate() {
                let chk = head.chunk_disk_mapper.chunk(mmap_chunk.r#ref).unwrap();
                assert_eq!(
                    exp_chunks[i].header,
                    chunk_counter_reset_header(&*chk, float_histo),
                    "floatHistogram={float_histo} {i}"
                );
                assert_eq!(exp_chunks[i].mint, mmap_chunk.min_time);
                assert_eq!(exp_chunks[i].maxt, mmap_chunk.max_time);
                assert_eq!(exp_chunks[i].num_samples, mmap_chunk.num_samples);
            }
        };
        let c = |header, mint, maxt, num_samples| ExpOooMmappedChunks { header, mint, maxt, num_samples };
        let g = tu::generate_test_histogram;

        // Append an in-order histogram, so the rest of the samples can be detected as OOO.
        append_histogram(1000, g(1000));

        // OOO histogram.
        for i in 1..=5 {
            append_histogram(100 + i, g(1000 + i));
        }
        // Nothing mmapped yet.
        check_ooo_exp_counter_reset_header(&[]);

        // 6th observation (which triggers a head chunk mmapping).
        append_histogram(112, g(1002));

        // One mmapped chunk with (ts, val) [(101, 1001), (102, 1002), (103, 1003), (104, 1004), (105, 1005)].
        check_ooo_exp_counter_reset_header(&[c(CounterResetHeader::UNKNOWN_COUNTER_RESET, 101, 105, 5)]);

        // Add more samples, there's a counter reset at ts 122.
        append_histogram(110, g(1001));
        append_histogram(124, g(904));
        append_histogram(123, g(903));
        append_histogram(122, g(902));

        // New samples not mmapped yet.
        check_ooo_exp_counter_reset_header(&[]);

        // 11th observation (which triggers another head chunk mmapping).
        append_histogram(200, g(2000));

        // Two new mmapped chunks [(110, 1001), (112, 1002)], [(122, 902), (123, 903), (124, 904)].
        check_ooo_exp_counter_reset_header(&[
            c(CounterResetHeader::UNKNOWN_COUNTER_RESET, 110, 112, 2),
            c(CounterResetHeader::COUNTER_RESET, 122, 124, 3),
        ]);

        // Count is lower than previous sample at ts 200, and NotCounterReset is always ignored on append.
        append_histogram(205, tu::set_histogram_not_counter_reset(g(1000)));

        append_histogram(210, tu::set_histogram_counter_reset(g(2010)));

        append_histogram(220, g(2020));

        append_histogram(215, g(2005));

        // 16th observation (which triggers another head chunk mmapping).
        append_histogram(350, g(4000));

        // Four new mmapped chunks: [(200, 2000)] [(205, 1000)], [(210, 2010)], [(215, 2015), (220, 2020)]
        check_ooo_exp_counter_reset_header(&[
            c(CounterResetHeader::UNKNOWN_COUNTER_RESET, 200, 200, 1),
            c(CounterResetHeader::COUNTER_RESET, 205, 205, 1),
            c(CounterResetHeader::COUNTER_RESET, 210, 210, 1),
            c(CounterResetHeader::COUNTER_RESET, 215, 220, 2),
        ]);

        // Adding five more samples (21 in total), so another mmapped chunk is created.
        append_histogram(300, tu::set_histogram_counter_reset(g(3000)));

        for i in 1..=4 {
            append_histogram(300 + i, g(3000 + i));
        }

        // One mmapped chunk with (ts, val) [(300, 3000), (301, 3001), (302, 3002), (303, 3003), (350, 4000)].
        check_ooo_exp_counter_reset_header(&[c(CounterResetHeader::COUNTER_RESET, 300, 350, 5)]);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Tests with a DB.

#[test]
fn test_head_appender_v2_append_out_of_order_samples_metric() {
    for (name, scenario) in crate::common::sample_type_scenarios() {
        let options = tsdb::db::default_options();
        test_head_appender_v2_out_of_order_samples_metric(name, scenario, options, &ERR_OUT_OF_ORDER_SAMPLE);
    }
}

#[test]
fn test_head_appender_v2_append_out_of_order_samples_metric_native_histogram_ooo_disabled() {
    for (name, scenario) in crate::common::sample_type_scenarios() {
        if scenario.sample_type != "histogram" {
            continue;
        }
        let mut options = tsdb::db::default_options();
        options.out_of_order_time_window = 0;
        test_head_appender_v2_out_of_order_samples_metric(name, scenario, options, &ERR_OUT_OF_ORDER_SAMPLE);
    }
}

fn test_head_appender_v2_out_of_order_samples_metric(
    name: &str,
    scenario: crate::common::SampleTypeScenario,
    options: tsdb::db::Options,
    expect_out_of_order_error: &storage::Error,
) {
    let dir = crate::common::TempDir::new();
    let db = tsdb::db::open(dir.path(), None, Some(options), None).unwrap();
    db.disable_compactions();
    let head = db.head().clone();
    let typ = scenario.sample_type;
    let ooo_samples = || head.metrics.out_of_order_samples.with_label_values(typ).get();
    let oob_samples = || head.metrics.out_of_bound_samples.with_label_values(typ).get();

    // TODO(bwplotka): Migrate to V2 natively.
    let append_sample = |app: &mut dyn storage::LimitedAppenderV1, ts: i64| -> Result<u64, storage::Error> {
        let s = (scenario.sample_func)(ts, 99);
        let l = ls(&["a", "b"]);
        if let Some(h) = &s.h {
            app.append_histogram(0, &l, ts, Some(h), None)
        } else if let Some(fh) = &s.fh {
            app.append_histogram(0, &l, ts, None, Some(fh))
        } else {
            app.append(0, &l, ts, s.f)
        }
    };
    let new_app = || storage::appender_v2_as_limited_v1(db.appender_v2(&ctx()));

    let mut app = new_app();
    for i in 1..=5 {
        append_sample(&mut *app, i).unwrap();
    }
    assert!(app.commit().is_none());

    // Test out of order metric.
    assert_eq!(0.0, ooo_samples(), "{name}");
    let mut app = new_app();
    assert_eq!(Some(expect_out_of_order_error), append_sample(&mut *app, 2).err().as_ref(), "{name}");
    assert_eq!(1.0, ooo_samples(), "{name}");

    assert_eq!(Some(expect_out_of_order_error), append_sample(&mut *app, 3).err().as_ref(), "{name}");
    assert_eq!(2.0, ooo_samples(), "{name}");

    assert_eq!(Some(expect_out_of_order_error), append_sample(&mut *app, 4).err().as_ref(), "{name}");
    assert_eq!(3.0, ooo_samples(), "{name}");
    assert!(app.commit().is_none());

    // Compact Head to test out of bound metric.
    let mut app = new_app();
    append_sample(&mut *app, crate::common::DEFAULT_BLOCK_DURATION * 2).unwrap();
    assert!(app.commit().is_none());

    assert_eq!(i64::MIN, head.min_valid_time(), "{name}");
    db.compact(&ctx()).unwrap();
    assert!(head.min_valid_time() > 0, "{name}");
    assert_eq!(0.0, oob_samples(), "{name}");

    let mut app = new_app();
    assert_eq!(Some(&ERR_OUT_OF_BOUNDS), append_sample(&mut *app, head.min_valid_time() - 2).err().as_ref());
    assert_eq!(1.0, oob_samples(), "{name}");

    assert_eq!(Some(&ERR_OUT_OF_BOUNDS), append_sample(&mut *app, head.min_valid_time() - 1).err().as_ref());
    assert_eq!(2.0, oob_samples(), "{name}");
    assert!(app.commit().is_none());

    // Some more valid samples for out of order.
    let mut app = new_app();
    for i in 1..=5 {
        append_sample(&mut *app, head.min_valid_time() + crate::common::DEFAULT_BLOCK_DURATION + i).unwrap();
    }
    assert!(app.commit().is_none());

    // Test out of order metric.
    let mut app = new_app();
    for (i, exp) in [(2, 4.0), (3, 5.0), (4, 6.0)] {
        let ts = head.min_valid_time() + crate::common::DEFAULT_BLOCK_DURATION + i;
        assert_eq!(Some(expect_out_of_order_error), append_sample(&mut *app, ts).err().as_ref(), "{name}");
        assert_eq!(exp, ooo_samples(), "{name}");
    }
    assert!(app.commit().is_none());
    db.close().unwrap();
}

/// Waits up to `timeout` for a value on `rx`, or for `done` to report a result first.
fn wait_or_done<T>(
    rx: &std::sync::mpsc::Receiver<()>,
    done: &std::sync::mpsc::Receiver<T>,
    timeout: std::time::Duration,
) -> Result<(), Option<T>> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if rx.try_recv().is_ok() {
            return Ok(());
        }
        if let Ok(v) = done.try_recv() {
            return Err(Some(v));
        }
        if std::time::Instant::now() > deadline {
            return Err(None);
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

/// Tests https://github.com/prometheus/prometheus/issues/9079.
#[test]
fn test_data_missing_on_query_during_compaction_appender_v2() {
    let db = crate::common::new_test_db(vec![]);
    db.disable_compactions();

    let mut app = db.appender_v2(&ctx());
    let mut r = 0u64;
    let (mint, mut maxt) = (0i64, 0i64);

    // Appends samples to span over 1.5 block ranges.
    let mut exp_samples = Vec::new();
    // 7 chunks with 15s scrape interval.
    for i in 0..=120 * 7i64 {
        let ts = i * crate::common::DEFAULT_BLOCK_DURATION / (4 * 120);
        r = app.append(r, &ls(&["a", "b"]), 0, ts, i as f64, None, None, &ao()).unwrap();
        maxt = ts;
        exp_samples.push(fs(ts, i as f64));
    }
    assert!(app.commit().is_none());

    // Get a querier before compaction (or when compaction is about to begin).
    let q = db.querier(mint, maxt).unwrap();

    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let started_tx = std::sync::Mutex::new(started_tx);
    *db.head().mem_truncation_call_back.lock().unwrap() = Some(Box::new(move || {
        let _ = started_tx.lock().unwrap().send(());
    }));

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let db2 = db.db.clone();
    let compactor = std::thread::spawn(move || {
        // Compacting head while the querier spans the compaction time.
        let _ = done_tx.send(db2.compact(&Context::background()));
    });

    match wait_or_done(&started_rx, &done_rx, std::time::Duration::from_secs(10)) {
        Ok(()) => {}
        Err(Some(res)) => {
            res.unwrap();
            panic!("compaction finished before head truncation started");
        }
        Err(None) => panic!("timed out waiting for head truncation to start"),
    }

    // Querying the querier that was got before compaction.
    let series = query(q, &[m(MatchType::Equal, "a", "b")]);
    let exp: BTreeMap<String, Vec<Sample>> = [(r#"{a="b"}"#.to_string(), exp_samples)].into_iter().collect();
    assert_eq!(exp, series);

    done_rx.recv().unwrap().unwrap();
    compactor.join().unwrap();
    assert!(!db.blocks().is_empty());
}

#[test]
fn test_is_querier_colliding_with_truncation_appender_v2() {
    let db = crate::common::new_test_db(vec![]);
    db.disable_compactions();

    let mut app = db.appender_v2(&ctx());
    let mut r = 0u64;
    for i in 0..=3000i64 {
        r = app.append(r, &ls(&["a", "b"]), 0, i, i as f64, None, None, &ao()).unwrap();
    }
    assert!(app.commit().is_none());

    // This mocks truncation.
    db.head().mem_truncation_in_process.store(true, Ordering::SeqCst);
    db.head().last_memory_truncation_time.store(2000, Ordering::SeqCst);

    // Test that IsQuerierValid suggests correct querier ranges.
    // (mint, maxt) for the querier, expShouldClose, expGetNew, expNewMint.
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
}

#[test]
fn test_wait_for_pending_readers_in_time_range_appender_v2() {
    let db = crate::common::new_test_db(vec![]);
    db.disable_compactions();

    let sample_ts = |i: i64| i * crate::common::DEFAULT_BLOCK_DURATION / (4 * 120);

    let mut app = db.appender_v2(&ctx());
    let mut r = 0u64;
    for i in 0..=3000i64 {
        r = app.append(r, &ls(&["a", "b"]), 0, sample_ts(i), i as f64, None, None, &ao()).unwrap();
    }
    assert!(app.commit().is_none());

    let (trunc_mint, trunc_maxt) = (1000i64, 2000i64);
    let cases: [(i64, i64, bool); 6] = [
        (0, 500, false),     // Before truncation range.
        (500, 1500, true),   // Overlaps with truncation at the start.
        (1200, 1700, true),  // Within truncation range.
        (1800, 2500, true),  // Overlaps with truncation at the end.
        (2000, 2500, false), // After truncation range.
        (2100, 2500, false), // After truncation range.
    ];
    for (mint, maxt, should_wait) in cases {
        let name = format!("mint={mint},maxt={maxt},shouldWait={should_wait}");
        // checkWaiting verifies WaitForPendingReadersInTimeRange behavior. The function should block while an
        // overlapping querier is open and return immediately when there's no overlap.
        // adapted: Go uses testing/synctest (fake time) to wait for the goroutine to block or finish; here a thread
        // runs the wait in real time: it must still run after 300ms while the querier is open, and finish within
        // the next poll interval (500ms) after the querier is closed.
        let check_waiting = |close: &mut dyn FnMut()| {
            let wait_over = Arc::new(AtomicBool::new(false));
            let (head, wo) = (db.head().clone(), wait_over.clone());
            let waiter = std::thread::spawn(move || {
                head.wait_for_pending_readers_in_time_range(trunc_mint, trunc_maxt);
                wo.store(true, Ordering::SeqCst);
            });
            std::thread::sleep(std::time::Duration::from_millis(300));
            if should_wait {
                assert!(
                    !wait_over.load(Ordering::SeqCst),
                    "{name}: WaitForPendingReadersInTimeRange should block while overlapping querier is open"
                );
                close();
                std::thread::sleep(std::time::Duration::from_secs(1));
                assert!(
                    wait_over.load(Ordering::SeqCst),
                    "{name}: WaitForPendingReadersInTimeRange should complete after querier is closed"
                );
            } else {
                assert!(
                    wait_over.load(Ordering::SeqCst),
                    "{name}: WaitForPendingReadersInTimeRange should return immediately when no overlap"
                );
                close();
            }
            waiter.join().unwrap();
        };

        let mut q = db.querier(mint, maxt).unwrap();
        check_waiting(&mut || assert!(q.close().is_none()));

        let mut cq = db.chunk_querier(mint, maxt).unwrap();
        check_waiting(&mut || assert!(cq.close().is_none()));
    }
}

#[test]
fn test_chunk_query_ooo_head_during_truncate_appender_v2() {
    const MAX_T: i64 = 6000;

    let dir = crate::common::TempDir::new();
    let mut opts = tsdb::db::default_options();
    opts.out_of_order_time_window = MAX_T;
    opts.min_block_duration = MAX_T / 2; // So that head will compact up to 3000.

    let db = tsdb::db::open(dir.path(), None, Some(opts), None).unwrap();
    db.disable_compactions();

    let r = 0u64;
    let mut app = db.appender_v2(&ctx());
    // Add in-order samples at every 100ms starting at 0ms.
    for i in (0..MAX_T).step_by(100) {
        app.append(r, &ls(&["a", "b"]), 0, i, 0.0, None, None, &ao()).unwrap();
    }
    // Add out-of-order samples at every 100ms starting at 50ms.
    for i in (50..MAX_T).step_by(100) {
        app.append(r, &ls(&["a", "b"]), 0, i, 0.0, None, None, &ao()).unwrap();
    }
    assert!(app.commit().is_none());

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
    let (allow_tx, started_rx) = (std::sync::Mutex::new(allow_tx), std::sync::Mutex::new(started_rx));
    *db.head().mem_truncation_call_back.lock().unwrap() = Some(Box::new(move || {
        // Compaction has started, let the query start and wait for it to actually start to simulate race
        // condition.
        allow_tx.lock().unwrap().send(()).unwrap();
        started_rx.lock().unwrap().recv().unwrap();
    }));

    let db2 = db.clone();
    let compactor = std::thread::spawn(move || {
        let _ = db2.compact(&Context::background()); // Compact and write blocks up to 3000 (maxtT/2).
    });

    // Wait for the compaction to start.
    allow_rx.recv().unwrap();

    let mut q = db.chunk_querier(1500, 2500).unwrap();
    started_tx.send(()).unwrap(); // Unblock the compaction.

    // Label names.
    let (res, annots, err) = q.label_names(&ctx(), None, &[m(MatchType::Equal, "a", "b")]);
    assert!(err.is_none(), "{err:?}");
    assert!(annots.is_empty());
    assert_eq!(vec!["a".to_string()], res);

    // Label values.
    let (res, annots, err) = q.label_values(&ctx(), "a", None, &[m(MatchType::Equal, "a", "b")]);
    assert!(err.is_none(), "{err:?}");
    assert!(annots.is_empty());
    assert_eq!(vec!["b".to_string()], res);

    // Verify: chunks.
    {
        let min_t = 1500;
        let mut ss = q.select(&ctx(), false, None, &[m(MatchType::Equal, "a", "b")]);
        assert!(ss.next());
        let s = ss.at().clone();
        assert!(!ss.next()); // One series.
        let mut meta_it = s.iterator(None);
        assert!(meta_it.next());
        let meta = meta_it.at().clone();
        // Samples.
        let chk = meta.chunk.expect("chunk");
        let mut it = chk.iterator(None);
        assert_ne!(ValueType::None, it.next()); // Has some data.
        assert_eq!(min_t, it.at_t()); // It is an in-order sample.
        assert_ne!(ValueType::None, it.next()); // Has some data.
        assert_eq!(min_t + 50, it.at_t()); // It is an out-of-order sample.
        assert!(it.err().is_none());
    }

    assert!(q.close().is_none()); // Cannot be deferred as the compaction waits for queries to close before finishing.

    compactor.join().unwrap(); // Wait for compaction.
    db.close().unwrap();
}
