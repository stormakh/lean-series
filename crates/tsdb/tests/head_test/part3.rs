//! Port of `tsdb/head_test.go`, lines 7603..end (part 3).
//!
//! Rust notes:
//! - `HeadOptions` is not `Clone` (it holds atomics). Go reuses `head.opts` when it reopens a head; here
//!   [`clone_opts`] copies the options.
//! - The head chunk count of a series is the length of its head chunk vector (Go keeps a separate atomic
//!   counter).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chunkenc::{ChunkIterator, ValueType, ENC_FLOAT_HISTOGRAM, ENC_FLOAT_HISTOGRAM_ST, ENC_HISTOGRAM, ENC_HISTOGRAM_ST, ENC_XOR, ENC_XOR2};
use model::exemplar::Exemplar;
use model::histogram::{CounterResetHint, FloatHistogram, Histogram, Span};
use model::labels::{self, Labels, ScratchBuilder};
use model::matcher::{MatchType, Matcher};
use model::value::stale_nan;
use storage::tsdbutil as tu;
use storage::{AppendV2Options, Context, Meta, Sample, SeriesRef};
use tsdb::head::{
    is_series_without_ooo, is_stale_series, Head, HeadOptions, MemSeries, MmappedChunk, NoopSeriesLifecycleCallback,
    StripeSeries, DEFAULT_SAMPLES_PER_CHUNK, OOO_CHUNK_ID_MASK,
};
use tsdb::head_wal::SeriesLifecycleState;
use tsdb::querier::new_block_querier;
use tsdb::{ChunkOrIterable, ChunkReader, IndexReader};
use tsdb_format::chunks::ChunkDiskMapperRef;
use tsdb_format::index::{new_list_postings, Postings};
use tsdb_format::record::{
    self, Decoder, Encoder, Logger, RefFloatHistogramSample, RefHistogramSample, RefSample, RefSeries,
};
use tsdb_format::tombstones::{Interval, Stone};
use tsdb_format::wlog::{self, compression, WL};

use crate::common::{self, fs, new_test_head_with_options, query, TempDir, TestHead, DEFAULT_BLOCK_DURATION};

// ---------------------------------------------------------------------------------------------------------------
// Helpers.

/// Go `newTestHeadDefaultOptions`.
fn new_test_head_default_options(chunk_range: i64, ooo_enabled: bool) -> HeadOptions {
    let opts = HeadOptions {
        chunk_range,
        enable_exemplar_storage: true,
        ..HeadOptions::default()
    };
    // Go config.DefaultExemplarsConfig.MaxExemplars.
    opts.max_exemplars.store(100000, Ordering::Relaxed);
    if ooo_enabled {
        opts.out_of_order_time_window.store(10 * 60 * 1000, Ordering::Relaxed);
    }
    opts
}

/// Go `newTestHead` (with the default options of this file).
fn new_test_head(name: &str, chunk_range: i64, compress: compression::Type, ooo: bool) -> TestHead {
    new_test_head_with_options(name, compress, new_test_head_default_options(chunk_range, ooo))
}

/// A copy of the head options (Go reuses the `*HeadOptions`).
fn clone_opts(o: &HeadOptions) -> HeadOptions {
    let n = HeadOptions {
        chunk_range: o.chunk_range,
        chunk_dir_root: o.chunk_dir_root.clone(),
        chunk_pool: o.chunk_pool.clone(),
        chunk_write_buffer_size: o.chunk_write_buffer_size,
        chunk_write_queue_size: o.chunk_write_queue_size,
        samples_per_chunk: o.samples_per_chunk,
        stripe_size: o.stripe_size,
        series_callback: o.series_callback.clone(),
        enable_exemplar_storage: o.enable_exemplar_storage,
        enable_memory_snapshot_on_shutdown: std::sync::atomic::AtomicBool::new(o.enable_memory_snapshot_on_shutdown.load(std::sync::atomic::Ordering::Relaxed)),
        isolation_disabled: o.isolation_disabled,
        wal_replay_concurrency: o.wal_replay_concurrency,
        enable_sharding: o.enable_sharding,
        enable_st_as_zero_sample: o.enable_st_as_zero_sample,
        enable_metadata_wal_records: o.enable_metadata_wal_records,
        enable_fast_startup: o.enable_fast_startup,
        logger: o.logger.clone(),
        ..HeadOptions::default()
    };
    n.max_exemplars.store(o.max_exemplars.load(Ordering::Relaxed), Ordering::Relaxed);
    n.out_of_order_time_window.store(o.out_of_order_time_window.load(Ordering::Relaxed), Ordering::Relaxed);
    n.out_of_order_cap_max.store(o.out_of_order_cap_max.load(Ordering::Relaxed), Ordering::Relaxed);
    n.enable_st_storage.store(o.enable_st_storage.load(Ordering::Relaxed), Ordering::Relaxed);
    n.float_chunk_encoding.store(o.float_chunk_encoding.load(Ordering::Relaxed), Ordering::Relaxed);
    n.enable_histogram_st_encoding.store(o.enable_histogram_st_encoding.load(Ordering::Relaxed), Ordering::Relaxed);
    n
}

/// Closes the head and opens a new one on the same WAL directory with `opts` (Go: `head.Close()`,
/// `wlog.NewSize(..., "wal", 32768, compression.None)`, `NewHead(nil, nil, wal, nil, opts, nil)`). Does not call
/// `init`.
fn reopen(th: &mut TestHead, opts: HeadOptions) {
    th.head.close().expect("close head");
    let wal = Arc::new(WL::new_size(th.dir.path().join("wal"), 32768, compression::Type::None).expect("wal"));
    th.head = Head::new(Some(wal.clone()), None, opts, None).expect("new head");
    th.wal = Some(wal);
}

/// Go `reopen` with `head.opts`.
fn reopen_same(th: &mut TestHead) {
    let opts = clone_opts(&th.head.opts);
    reopen(th, opts);
}

fn ctx() -> Context {
    Context::background()
}

fn lbl(ss: &[&str]) -> Labels {
    labels::from_strings(ss)
}

fn eq_matcher(n: &str, v: &str) -> Matcher {
    Matcher::new(MatchType::Equal, n, v).unwrap()
}

fn head_query(h: &Head, mint: i64, maxt: i64, n: &str, v: &str) -> BTreeMap<String, Vec<Sample>> {
    let q = new_block_querier(h, mint, maxt).expect("querier");
    query(q, &[eq_matcher(n, v)])
}

fn stale_hist(h: &Histogram) -> Histogram {
    let mut s = h.clone();
    s.sum = stale_nan();
    s
}

fn stale_fhist(fh: &FloatHistogram) -> FloatHistogram {
    let mut s = fh.clone();
    s.sum = stale_nan();
    s
}

/// A WAL record for [`populate_test_wl`] and [`read_test_wal`] (Go `[]any` of record slices).
#[derive(Clone, Debug, PartialEq)]
enum Rec {
    Series(Vec<RefSeries>),
    Samples(Vec<RefSample>),
    Tombstones(Vec<Stone>),
    Histograms(Vec<RefHistogramSample>),
    FloatHistograms(Vec<RefFloatHistogramSample>),
}

/// Go `populateTestWL`.
fn populate_test_wl(w: &WL, recs: &[Rec], enable_st_storage: bool) {
    let enc = Encoder { enable_st_storage };
    let mut buf = Vec::new();
    for r in recs {
        buf.clear();
        match r {
            Rec::Series(v) => enc.series(v, &mut buf),
            Rec::Samples(v) => enc.samples(v, &mut buf),
            Rec::Tombstones(v) => enc.tombstones(v, &mut buf),
            Rec::Histograms(v) => {
                let _ = enc.histogram_samples(v, &mut buf);
            }
            Rec::FloatHistograms(v) => {
                let _ = enc.float_histogram_samples(v, &mut buf);
            }
        }
        w.log(&buf).expect("log");
    }
}

/// Go `readTestWAL`.
fn read_test_wal(dir: &Path) -> Vec<Rec> {
    let sr = wlog::new_segments_reader(dir).expect("segments reader");
    let mut dec = Decoder::new(Logger::nop());
    let mut r = wlog::Reader::new(sr);
    let mut recs = Vec::new();
    while r.next() {
        let rec = r.record().to_vec();
        match dec.r#type(&rec) {
            record::Type::Series => {
                let mut v = Vec::new();
                dec.series(&rec, &mut v).expect("series");
                recs.push(Rec::Series(v));
            }
            record::Type::Samples | record::Type::SamplesV2 => {
                let mut v = Vec::new();
                dec.samples(&rec, &mut v).expect("samples");
                recs.push(Rec::Samples(v));
            }
            record::Type::HistogramSamples
            | record::Type::CustomBucketsHistogramSamples
            | record::Type::HistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.histogram_samples(&rec, &mut v).expect("histograms");
                recs.push(Rec::Histograms(v));
            }
            record::Type::FloatHistogramSamples
            | record::Type::CustomBucketsFloatHistogramSamples
            | record::Type::FloatHistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.float_histogram_samples(&rec, &mut v).expect("float histograms");
                recs.push(Rec::FloatHistograms(v));
            }
            record::Type::Tombstones => {
                let mut v = Vec::new();
                dec.tombstones(&rec, &mut v).expect("tombstones");
                recs.push(Rec::Tombstones(v));
            }
            record::Type::MinValidTime => {
                // Internal checkpoint bookkeeping.
                dec.min_valid_time(&rec).expect("min valid time");
            }
            t => panic!("unknown record type {:?}", t),
        }
    }
    assert!(r.err().is_none(), "{:?}", r.err());
    r.into_inner().close().expect("close segments reader");
    recs
}

/// The record types of all records in a WAL directory.
fn wal_record_types(dir: &Path) -> Vec<record::Type> {
    let sr = wlog::new_segments_reader(dir).expect("segments reader");
    let dec = Decoder::new(Logger::nop());
    let mut r = wlog::Reader::new(sr);
    let mut out = Vec::new();
    while r.next() {
        out.push(dec.r#type(r.record()));
    }
    assert!(r.err().is_none(), "{:?}", r.err());
    r.into_inner().close().expect("close segments reader");
    out
}

/// Go `requireEqualSamples` (testutil.go).
fn require_equal_samples(name: &str, expected: &[Sample], actual: &[Sample], ignore_counter_resets: bool) {
    assert_eq!(expected.len(), actual.len(), "Length not equal to expected for {name}");
    for (i, (e, a)) in expected.iter().zip(actual).enumerate() {
        assert_eq!(e.t, a.t, "Different timestamps for {name}[{i}]");
        let typ = |s: &Sample| {
            if s.h.is_some() {
                "histogram"
            } else if s.fh.is_some() {
                "floathistogram"
            } else {
                "float"
            }
        };
        assert_eq!(typ(e), typ(a), "Different types for {name}[{i}] at ts {}", e.t);
        if let Some(eh) = &e.h {
            let mut eh = (**eh).clone();
            let mut ah = (**a.h.as_ref().unwrap()).clone();
            if ignore_counter_resets && eh.counter_reset_hint != CounterResetHint::GaugeType {
                eh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                ah.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            } else {
                assert_eq!(eh.counter_reset_hint, ah.counter_reset_hint, "Sample header doesn't match for {name}[{i}]");
            }
            assert_eq!(eh, ah, "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        } else if let Some(efh) = &e.fh {
            let mut efh = (**efh).clone();
            let mut afh = (**a.fh.as_ref().unwrap()).clone();
            if ignore_counter_resets {
                efh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                afh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            } else {
                assert_eq!(efh.counter_reset_hint, afh.counter_reset_hint, "Sample header doesn't match for {name}[{i}]");
            }
            assert_eq!(efh, afh, "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        } else {
            assert_eq!(e.f.to_bits(), a.f.to_bits(), "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        }
    }
}

/// Go `requireEqualSeries` (testutil.go).
fn require_equal_series(
    expected: &BTreeMap<String, Vec<Sample>>,
    actual: &BTreeMap<String, Vec<Sample>>,
    ignore_counter_resets: bool,
) {
    for (name, e) in expected {
        let a = actual.get(name).unwrap_or_else(|| panic!("Expected series {name} not found"));
        require_equal_samples(name, e, a, ignore_counter_resets);
    }
    for name in actual.keys() {
        assert!(expected.contains_key(name), "Unexpected series {name}");
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Tests.

#[test]
fn test_head_compaction_while_append_and_commit_exemplar() {
    let h = new_test_head("compaction-exemplar", DEFAULT_BLOCK_DURATION, compression::Type::None, false);
    let mut app = h.appender(&ctx());
    let lbls = lbl(&["foo", "bar"]);
    let r = app.append(0, &lbls, 1, 1.0).expect("append");
    let _ = app.commit();
    // Not adding a sample here to trigger the fault.
    let mut app = h.appender(&ctx());
    app.append_exemplar(
        r,
        &lbls,
        &Exemplar {
            value: 1.0,
            ts: 20,
            ..Default::default()
        },
    )
    .expect("append exemplar");
    let _ = h.truncate(10);
    let _ = app.commit();
}

/// Go `labelsWithHashCollision`.
fn labels_with_hash_collision() -> (Labels, Labels) {
    // These two series have the same XXHash; thanks to https://github.com/pstibrany/labels_hash_collisions
    let mut ls1 = lbl(&["__name__", "metric", "lbl", "HFnEaGl"]);
    let mut ls2 = lbl(&["__name__", "metric", "lbl", "RqcXatm"]);
    if ls1.hash() != ls2.hash() {
        // These ones are the same when using -tags slicelabels
        ls1 = lbl(&["__name__", "metric", "lbl1", "value", "lbl2", "l6CQ5y"]);
        ls2 = lbl(&["__name__", "metric", "lbl1", "value", "lbl2", "v7uDlF"]);
    }
    if ls1.hash() != ls2.hash() {
        panic!("This code needs to be updated: find new labels with colliding hash values.");
    }
    (ls1, ls2)
}

/// Go `stripeSeriesWithCollidingSeries`.
fn stripe_series_with_colliding_series() -> (StripeSeries, Arc<MemSeries>, Arc<MemSeries>) {
    let (lbls1, lbls2) = labels_with_hash_collision();
    let ms1 = Arc::new(MemSeries::new(lbls1.clone(), 1, 0, true, false));
    let ms2 = Arc::new(MemSeries::new(lbls2.clone(), 2, 0, true, false));
    let hash = lbls1.hash();
    let s = StripeSeries::new(1, Arc::new(NoopSeriesLifecycleCallback));

    let (got, created) = s.set_unless_already_set(hash, &lbls1, ms1.clone());
    assert!(created);
    assert!(Arc::ptr_eq(&ms1, &got));

    // Add a conflicting series
    let (got, created) = s.set_unless_already_set(hash, &lbls2, ms2.clone());
    assert!(created);
    assert!(Arc::ptr_eq(&ms2, &got));

    (s, ms1, ms2)
}

#[test]
fn test_stripe_series_get_or_set() {
    let (s, ms1, ms2) = stripe_series_with_colliding_series();
    let hash = ms1.lset.hash();

    // Verify that we can get both of the series despite the hash collision
    let got = s.get_by_hash(hash, &ms1.lset).unwrap();
    assert!(Arc::ptr_eq(&ms1, &got));
    let got = s.get_by_hash(hash, &ms2.lset).unwrap();
    assert!(Arc::ptr_eq(&ms2, &got));
}

#[test]
fn test_stripe_series_gc() {
    // retains the oldest referenced file regardless of timestamp order
    {
        let lset = lbl(&["a", "1"]);
        let series = Arc::new(MemSeries::new(lset.clone(), 1, 0, false, false));
        // File numbers occupy the upper 32 bits of the disk reference.
        let mc = |r: u64, min_time: i64, max_time: i64| MmappedChunk {
            r#ref: ChunkDiskMapperRef(r),
            num_samples: 0,
            encoding: chunkenc::ENC_NONE,
            min_time,
            max_time,
        };
        series.lock().mmapped_chunks = vec![mc(3 << 32, 0, 50), mc(1 << 32, 100, 150), mc(2 << 32, 200, 250)];
        let s = StripeSeries::new(1, Arc::new(NoopSeriesLifecycleCallback));
        let (_, created) = s.set_unless_already_set(lset.hash(), &lset, series.clone());
        assert!(created);

        for (mint, min_file, remaining) in
            [(0i64, 1i64, 3usize), (51, 1, 2), (151, 2, 1), (251, i32::MAX as i64, 0)]
        {
            let res = s.gc(mint, ChunkDiskMapperRef(0));
            assert_eq!(min_file, res.min_mmap_file, "mint={mint}");
            assert_eq!(remaining, series.lock().mmapped_chunks.len());
        }
    }

    // marks collected series
    {
        let (s, ms1, ms2) = stripe_series_with_colliding_series();
        let hash = ms1.lset.hash();

        s.gc(0, ChunkDiskMapperRef(0));

        // Verify that we can get neither ms1 nor ms2 after garbage collection.
        assert!(s.get_by_hash(hash, &ms1.lset).is_none());
        assert!(s.get_by_hash(hash, &ms2.lset).is_none());
        assert!(ms1.lock().is_gced());
        assert!(ms2.lock().is_gced());
    }

    // collects a directly constructed series without samples or exemplars at mint<=0
    for mint in [-1i64, 0] {
        // adapted: Go builds `&memSeries{lset, ref: 1}` directly (no isolation ring); the Rust struct has private
        // fields, so it is built with `MemSeries::new` with isolation disabled, which sets no exemplar state.
        let lset = lbl(&["a", "1"]);
        let series = Arc::new(MemSeries::new(lset.clone(), 1, 0, true, false));
        let s = StripeSeries::new(1, Arc::new(NoopSeriesLifecycleCallback));
        let (_, created) = s.set_unless_already_set(lset.hash(), &lset, series);
        assert!(created);

        s.gc(mint, ChunkDiskMapperRef(0));

        assert!(
            s.get_by_hash(lset.hash(), &lset).is_none(),
            "a series with no chunks and no exemplar must be collected at mint={mint}, regardless of how it was constructed"
        );
    }

    // keeps series with zero or negative exemplar timestamps
    for ts in [-1i64, 0] {
        let lset = lbl(&["a", "1"]);
        let series = Arc::new(MemSeries::new(lset.clone(), 1, 0, true, false));
        let s = StripeSeries::new(1, Arc::new(NoopSeriesLifecycleCallback));
        let (_, created) = s.set_unless_already_set(lset.hash(), &lset, series.clone());
        assert!(created);

        series.update_exemplar_timestamp(ts);
        // An older exemplar must not shorten the lifetime of the series.
        series.update_exemplar_timestamp(ts - 1);
        s.gc(ts, ChunkDiskMapperRef(0));
        let got = s.get_by_id(series.r#ref).unwrap_or_else(|| panic!("exemplar timestamp={ts}"));
        assert!(Arc::ptr_eq(&series, &got), "exemplar timestamp={ts}");

        s.gc(ts + 1, ChunkDiskMapperRef(0));
        assert!(s.get_by_id(series.r#ref).is_none(), "exemplar timestamp={ts}");
    }

    // keeps series until all reservations are released
    {
        let lset = lbl(&["a", "1"]);
        let series = Arc::new(MemSeries::new(lset.clone(), 1, 0, false, false));
        {
            let mut g = series.lock();
            g.mark_pending_commit();
            g.mark_pending_commit();
            assert!(g.unmark_pending_commit());
            assert_eq!(1, g.pending_commit_count());
        }

        let s = StripeSeries::new(1, Arc::new(NoopSeriesLifecycleCallback));
        let (got, created) = s.set_unless_already_set(lset.hash(), &lset, series.clone());
        assert!(created);
        assert!(Arc::ptr_eq(&series, &got));

        let res = s.gc(0, ChunkDiskMapperRef(0));
        assert!(res.deleted.is_empty());
        assert!(Arc::ptr_eq(&series, &s.get_by_id(series.r#ref).unwrap()));

        assert!(series.lock().unmark_pending_commit());
        let res = s.gc(0, ChunkDiskMapperRef(0));
        assert!(res.deleted.contains(&series.r#ref));
        assert!(s.get_by_id(series.r#ref).is_none());
    }

    // underflow preserves garbage-collected flag
    {
        let series = MemSeries::new(lbl(&["a", "1"]), 1, 0, false, false);
        let mut g = series.lock();
        g.set_gced();
        assert!(!g.unmark_pending_commit());
        assert_eq!(0, g.pending_commit_count());
        assert!(g.is_gced());
    }
}

#[test]
fn test_postings_cardinality_stats() {
    // adapted: Go builds `&Head{postings: index.NewMemPostings()}`; here a head without WAL, whose postings are
    // empty at the start.
    let head = common::new_head_no_wal("postings-cardinality-stats", HeadOptions::default());
    head.postings.add(1, &lbl(&["__name__", "t", "n", "v1"]));
    head.postings.add(2, &lbl(&["__name__", "t", "n", "v2"]));

    let stats_for_metric_name = head.postings_cardinality_stats("__name__", 10);
    head.postings.add(3, &lbl(&["__name__", "t", "n", "v3"]));
    // Using cache.
    assert_eq!(*stats_for_metric_name, *head.postings_cardinality_stats("__name__", 10));

    let stats_for_some_label = head.postings_cardinality_stats("n", 10);
    // Cache should be evicted because of the change of label name.
    assert_ne!(*stats_for_metric_name, *stats_for_some_label);
    head.postings.add(4, &lbl(&["__name__", "t", "n", "v4"]));
    // Using cache.
    assert_eq!(*stats_for_some_label, *head.postings_cardinality_stats("n", 10));
    // Cache should be evicted because of the change of limit parameter.
    let stats_for_some_label1 = head.postings_cardinality_stats("n", 1);
    assert_ne!(*stats_for_some_label1, *stats_for_some_label);
    // Using cache.
    assert_eq!(*stats_for_some_label1, *head.postings_cardinality_stats("n", 1));
}

#[test]
fn test_head_appender_append_float_with_same_timestamp_as_previous_histogram() {
    let head = new_test_head("float-same-ts-hist", DEFAULT_BLOCK_DURATION, compression::Type::None, false);
    let ls = lbl(&["__name__", "test"]);
    {
        // Append a float 10.0 @ 1_000
        let mut app = head.appender(&ctx());
        app.append(0, &ls, 1_000, 10.0).expect("append");
        assert!(app.commit().is_none());
    }
    {
        // Append a float histogram @ 2_000
        let mut app = head.appender(&ctx());
        let h = tu::generate_test_histogram(1);
        app.append_histogram(0, &ls, 2_000, Some(&h), None).expect("append histogram");
        assert!(app.commit().is_none());
    }
    let mut app = head.appender(&ctx());
    let err = app.append(0, &ls, 2_000, 10.0).unwrap_err();
    assert!(err.is(&storage::new_duplicate_histogram_to_float_err(2_000, 10.0)), "{err}");
}

/// An appendable sample of the ST tests (Go `appendableSamples`).
#[derive(Clone, Default)]
struct AppSample {
    ts: i64,
    f: f64,
    h: Option<Histogram>,
    fh: Option<FloatHistogram>,
    st: i64,
}

#[test]
fn test_head_appender_append_st() {
    let mut test_histogram = tu::generate_test_histogram(1);
    test_histogram.counter_reset_hint = CounterResetHint::NotCounterReset;
    let mut test_float_histogram = tu::generate_test_float_histogram(1);
    test_float_histogram.counter_reset_hint = CounterResetHint::NotCounterReset;
    // TODO(beorn7): Once issue #15346 is fixed, the CounterResetHint of the following two zero histograms should
    // be histogram.CounterReset.
    let test_zero_histogram = Histogram {
        schema: test_histogram.schema,
        zero_threshold: test_histogram.zero_threshold,
        positive_spans: test_histogram.positive_spans.clone(),
        negative_spans: test_histogram.negative_spans.clone(),
        positive_buckets: vec![0, 0, 0, 0],
        negative_buckets: vec![0, 0, 0, 0],
        ..Default::default()
    };
    let test_zero_float_histogram = FloatHistogram {
        schema: test_float_histogram.schema,
        zero_threshold: test_float_histogram.zero_threshold,
        positive_spans: test_float_histogram.positive_spans.clone(),
        negative_spans: test_float_histogram.negative_spans.clone(),
        positive_buckets: vec![0.0, 0.0, 0.0, 0.0],
        negative_buckets: vec![0.0, 0.0, 0.0, 0.0],
        ..Default::default()
    };
    let fsam = |ts: i64, f: f64, st: i64| AppSample { ts, f, st, ..Default::default() };
    let hsam = |ts: i64, st: i64| AppSample {
        ts,
        h: Some(test_histogram.clone()),
        st,
        ..Default::default()
    };
    let fhsam = |ts: i64, st: i64| AppSample {
        ts,
        fh: Some(test_float_histogram.clone()),
        st,
        ..Default::default()
    };
    let eh = |t: i64, h: &Histogram| Sample::histogram(0, t, h.clone());
    let efh = |t: i64, h: &FloatHistogram| Sample::float_histogram(0, t, h.clone());
    let (th, tz, tfh, tzf) = (&test_histogram, &test_zero_histogram, &test_float_histogram, &test_zero_float_histogram);

    let cases: Vec<(&str, Vec<AppSample>, Vec<Sample>)> = vec![
        (
            "In order ct+normal sample/floatSample",
            vec![fsam(100, 10.0, 1), fsam(101, 10.0, 1)],
            vec![fs(1, 0.0), fs(100, 10.0), fs(101, 10.0)],
        ),
        (
            "In order ct+normal sample/histogram",
            vec![hsam(100, 1), hsam(101, 1)],
            vec![eh(1, tz), eh(100, th), eh(101, th)],
        ),
        (
            "In order ct+normal sample/floathistogram",
            vec![fhsam(100, 1), fhsam(101, 1)],
            vec![efh(1, tzf), efh(100, tfh), efh(101, tfh)],
        ),
        (
            "Consecutive appends with same st ignore st/floatSample",
            vec![fsam(100, 10.0, 1), fsam(101, 10.0, 1)],
            vec![fs(1, 0.0), fs(100, 10.0), fs(101, 10.0)],
        ),
        (
            "Consecutive appends with same st ignore st/histogram",
            vec![hsam(100, 1), hsam(101, 1)],
            vec![eh(1, tz), eh(100, th), eh(101, th)],
        ),
        (
            "Consecutive appends with same st ignore st/floathistogram",
            vec![fhsam(100, 1), fhsam(101, 1)],
            vec![efh(1, tzf), efh(100, tfh), efh(101, tfh)],
        ),
        (
            "Consecutive appends with newer st do not ignore st/floatSample",
            vec![fsam(100, 10.0, 1), fsam(102, 10.0, 101)],
            vec![fs(1, 0.0), fs(100, 10.0), fs(101, 0.0), fs(102, 10.0)],
        ),
        (
            "Consecutive appends with newer st do not ignore st/histogram",
            vec![hsam(100, 1), hsam(102, 101)],
            vec![eh(1, tz), eh(100, th), eh(101, tz), eh(102, th)],
        ),
        (
            "Consecutive appends with newer st do not ignore st/floathistogram",
            vec![fhsam(100, 1), fhsam(102, 101)],
            vec![efh(1, tzf), efh(100, tfh), efh(101, tzf), efh(102, tfh)],
        ),
        (
            "ST equals to previous sample timestamp is ignored/floatSample",
            vec![fsam(100, 10.0, 1), fsam(101, 10.0, 100)],
            vec![fs(1, 0.0), fs(100, 10.0), fs(101, 10.0)],
        ),
        (
            "ST equals to previous sample timestamp is ignored/histogram",
            vec![hsam(100, 1), hsam(101, 100)],
            vec![eh(1, tz), eh(100, th), eh(101, th)],
        ),
        (
            "ST equals to previous sample timestamp is ignored/floathistogram",
            vec![fhsam(100, 1), fhsam(101, 100)],
            vec![efh(1, tzf), efh(100, tfh), efh(101, tfh)],
        ),
    ];
    for (name, samples, expected) in cases {
        let h = new_test_head("append-st", DEFAULT_BLOCK_DURATION, compression::Type::None, false);
        let mut a = h.appender(&ctx());
        let lbls = lbl(&["foo", "bar"]);
        for s in &samples {
            // Append float if it's a float test case
            if s.f != 0.0 {
                a.append_st_zero_sample(0, &lbls, s.ts, s.st).unwrap_or_else(|e| panic!("{name}: {e}"));
                a.append(0, &lbls, s.ts, s.f).unwrap_or_else(|e| panic!("{name}: {e}"));
            }
            // Append histograms if it's a histogram test case
            if s.h.is_some() || s.fh.is_some() {
                let r = a
                    .append_histogram_st_zero_sample(0, &lbls, s.ts, s.st, s.h.as_ref(), s.fh.as_ref())
                    .unwrap_or_else(|e| panic!("{name}: {e}"));
                a.append_histogram(r, &lbls, s.ts, s.h.as_ref(), s.fh.as_ref())
                    .unwrap_or_else(|e| panic!("{name}: {e}"));
            }
        }
        assert!(a.commit().is_none(), "{name}");

        let result = head_query(&h, i64::MIN, i64::MAX, "foo", "bar");
        assert_eq!(Some(&expected), result.get(r#"{foo="bar"}"#), "{name}");
    }
}

#[test]
fn test_head_appender_append_histogram_st_zero_sample() {
    let gh = |ts: i64, st: i64| AppSample {
        ts,
        h: Some(tu::generate_test_histogram(1)),
        st,
        ..Default::default()
    };
    let gfh = |ts: i64, st: i64| AppSample {
        ts,
        fh: Some(tu::generate_test_float_histogram(1)),
        st,
        ..Default::default()
    };
    let cases: Vec<(&str, Vec<AppSample>, storage::Error)> = vec![
        (
            "integer histogram ST lower than minValidTime initiates ErrOutOfBounds",
            vec![gh(100, -1)],
            storage::ERR_OUT_OF_BOUNDS,
        ),
        (
            "float histograms ST lower than minValidTime initiates ErrOutOfBounds",
            vec![gfh(100, -1)],
            storage::ERR_OUT_OF_BOUNDS,
        ),
        (
            "integer histogram ST duplicates an existing sample",
            vec![gh(100, 0), gh(200, 100)],
            storage::ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP,
        ),
        (
            "float histogram ST duplicates an existing sample",
            vec![gfh(100, 0), gfh(200, 100)],
            storage::ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP,
        ),
        (
            "integer histogram ST is out of order",
            vec![gh(200, 0), gh(300, 100)],
            storage::ERR_OUT_OF_ORDER_ST,
        ),
        (
            "float histogram ST is out of order",
            vec![gfh(200, 0), gfh(300, 100)],
            storage::ERR_OUT_OF_ORDER_ST,
        ),
    ];
    for (name, samples, expected_error) in cases {
        let h = new_test_head("append-hist-st-zero", DEFAULT_BLOCK_DURATION, compression::Type::None, false);
        let lbls = lbl(&["foo", "bar"]);
        let mut r: SeriesRef = 0;
        for s in &samples {
            let mut a = h.appender(&ctx());
            if s.st != 0 {
                let res = a.append_histogram_st_zero_sample(r, &lbls, s.ts, s.st, s.h.as_ref(), s.fh.as_ref());
                match res {
                    Ok(x) => panic!("{name}: expected error {expected_error}, got ref {x}"),
                    Err(e) => {
                        assert!(e.is(&expected_error), "{name}: {e}");
                        // Go assigns the returned ref (0 on error).
                        r = 0;
                    }
                }
            }
            r = a.append_histogram(r, &lbls, s.ts, s.h.as_ref(), s.fh.as_ref()).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert!(a.commit().is_none(), "{name}");
        }
    }
}

#[test]
fn test_head_appender_append_st_zero_sample() {
    let fsam = |ts: i64, f: f64, st: i64| AppSample { ts, f, st, ..Default::default() };
    let cases: Vec<(&str, bool, Vec<AppSample>, storage::Error)> = vec![
        (
            "ST lower than minValidTime returns ErrOutOfBounds",
            false,
            vec![fsam(100, 1.0, -1)],
            storage::ERR_OUT_OF_BOUNDS,
        ),
        (
            "ST duplicates an existing sample",
            false,
            vec![fsam(100, 1.0, 0), fsam(200, 2.0, 100)],
            storage::ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP,
        ),
        (
            "ST is out of order when OOO is disabled",
            false,
            vec![fsam(200, 2.0, 0), fsam(300, 3.0, 100)],
            storage::ERR_OUT_OF_ORDER_SAMPLE,
        ),
        (
            "ST is out of order when OOO is enabled",
            true,
            vec![fsam(200, 2.0, 0), fsam(300, 3.0, 100)],
            storage::ERR_OUT_OF_ORDER_ST,
        ),
    ];
    for (name, ooo_enabled, samples, expected_error) in cases {
        let h = new_test_head("append-st-zero", DEFAULT_BLOCK_DURATION, compression::Type::None, ooo_enabled);
        let lbls = lbl(&["foo", "bar"]);
        let mut r: SeriesRef = 0;
        for s in &samples {
            let mut a = h.appender(&ctx());
            if s.st != 0 {
                match a.append_st_zero_sample(r, &lbls, s.ts, s.st) {
                    Ok(x) => panic!("{name}: expected error {expected_error}, got ref {x}"),
                    Err(e) => {
                        assert!(e.is(&expected_error), "{name}: {e}");
                        r = 0;
                    }
                }
            }
            r = a.append(r, &lbls, s.ts, s.f).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert!(a.commit().is_none(), "{name}");
        }
    }

    // rejected ST injected zero does not retain series
    {
        let h = new_test_head("append-st-zero-rejected", DEFAULT_BLOCK_DURATION, compression::Type::None, true);
        let lset = lbl(&["a", "1"]);

        let mut baseline = h.appender(&ctx());
        let r = baseline.append(0, &lset, 200, 2.0).expect("append");
        assert!(baseline.commit().is_none());

        let mut app = h.appender(&ctx());
        let err = app.append_st_zero_sample(r, &lset, 300, 100).unwrap_err();
        assert!(err.is(&storage::ERR_OUT_OF_ORDER_ST), "{err}");
        assert!(app.rollback().is_none());

        h.truncate(201).expect("truncate");
        assert_eq!(0, h.num_series(), "the rejected sample must not protect the collectable series");
        assert_eq!(0.0, h.metrics.pending_commit_underflow.get());
    }
}

#[test]
fn test_head_appender_pending_commit_underflow_is_reported_not_fatal() {
    let h = new_test_head("pending-underflow", DEFAULT_BLOCK_DURATION, compression::Type::None, false);
    let lset = lbl(&["a", "1"]);

    let mut app = h.appender(&ctx());
    let r = app.append(0, &lset, 100, 1.0).expect("append");

    // Drop both reservations the append took, so every release during Commit underflows: one for the queued
    // sample, one for the created series.
    let series = h.series.get_by_id(r).expect("series");
    {
        let mut g = series.lock();
        assert_eq!(2, g.pending_commit_count());
        assert!(g.unmark_pending_commit());
        assert!(g.unmark_pending_commit());
    }

    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| app.commit()));
    let res = res.expect("commit must not panic");
    assert!(res.is_none(), "{:?}", res);
    assert_eq!(2.0, h.metrics.pending_commit_underflow.get());

    {
        let g = series.lock();
        assert_eq!(0, g.pending_commit_count(), "the count must clamp rather than wrap");
        assert!(!g.is_gced(), "underflow must not set packed flag bits");
    }

    // The sample is still committed; the accounting bug must not lose data on its own.
    let mut exp = BTreeMap::new();
    exp.insert(r#"{a="1"}"#.to_string(), vec![fs(100, 1.0)]);
    assert_eq!(exp, head_query(&h, i64::MIN, i64::MAX, "a", "1"));
}

#[test]
fn test_head_compactable_does_not_compact_empty_head() {
    // Use a chunk range of 1 here so that if we attempted to determine if the head was compactable using default
    // values for min and max times, `Head.compactable()` would return true which is incorrect. This test verifies
    // that we short-circuit the check when the head has not yet had any samples added.
    let head = new_test_head("compactable-empty", 1, compression::Type::None, false);
    assert!(!head.compactable());
}

#[test]
fn test_head_append_histogram_and_commit_concurrency() {
    let h = tu::generate_test_histogram(1);
    let fh = tu::generate_test_float_histogram(1);
    for float in [false, true] {
        let name = if float { "float histogram" } else { "integer histogram" };
        // Go testHeadAppendHistogramAndCommitConcurrency.
        let head = new_test_head("hist-commit-concurrency", 1000, compression::Type::None, false);
        // How this works: Commit() should be atomic, thus one of the commits will be first and the other second.
        // The first commit will create a new series and write a sample. The second commit will see an exact
        // duplicate sample which it should ignore. Unless there's a race that causes the memSeries.lastHistogram
        // to be corrupt and fail the duplicate check.
        std::thread::scope(|sc| {
            for _ in 0..2 {
                let head = &head;
                let (h, fh) = (&h, &fh);
                sc.spawn(move || {
                    for i in 0..10000 {
                        let mut app = head.appender(&ctx());
                        let l = lbl(&["foo", "bar", "serial", &i.to_string()]);
                        let r = if float {
                            app.append_histogram(0, &l, 1, None, Some(fh))
                        } else {
                            app.append_histogram(0, &l, 1, Some(h), None)
                        };
                        r.unwrap_or_else(|e| panic!("{name}: {e}"));
                        assert!(app.commit().is_none(), "{name}");
                    }
                });
            }
        });
    }
}

fn append_float(head: &Head, lbls: &Labels, ts: i64, v: f64) {
    let mut app = head.appender(&ctx());
    app.append(0, lbls, ts, v).expect("append");
    assert!(app.commit().is_none());
}

fn append_hist(head: &Head, lbls: &Labels, ts: i64, h: &Histogram) {
    let mut app = head.appender(&ctx());
    app.append_histogram(0, lbls, ts, Some(h), None).expect("append histogram");
    assert!(app.commit().is_none());
}

fn append_fhist(head: &Head, lbls: &Labels, ts: i64, fh: &FloatHistogram) {
    let mut app = head.appender(&ctx());
    app.append_histogram(0, lbls, ts, None, Some(fh)).expect("append float histogram");
    assert!(app.commit().is_none());
}

#[test]
fn test_head_num_stale_series() {
    let mut head = new_test_head("num-stale-series", 1000, compression::Type::None, false);
    head.init(0).expect("init");

    // Initially, no series should be stale.
    assert_eq!(0, head.num_stale_series());

    let verify = |head: &Head, num_stale: u64, num_series: u64| {
        assert_eq!(num_stale, head.num_stale_series());
        assert_eq!(num_series, head.num_series());
    };
    let restart_and_verify = |th: &mut TestHead, num_stale: u64, num_series: u64| {
        verify(th, num_stale, num_series);
        reopen_same(th);
        th.init(0).expect("init");
        verify(th, num_stale, num_series);
    };

    // Create some series with normal samples.
    let series1 = lbl(&["name", "series1", "label", "value1"]);
    let series2 = lbl(&["name", "series2", "label", "value2"]);
    let series3 = lbl(&["name", "series3", "label", "value3"]);

    // Add normal samples to all series.
    append_float(&head, &series1, 100, 1.0);
    append_float(&head, &series2, 100, 2.0);
    append_float(&head, &series3, 100, 3.0);
    // Still no stale series.
    verify(&head, 0, 3);

    // Make series1 stale by appending a stale sample. Now we should have 1 stale series.
    append_float(&head, &series1, 200, stale_nan());
    verify(&head, 1, 3);

    // Make series2 stale as well.
    append_float(&head, &series2, 200, stale_nan());
    verify(&head, 2, 3);
    restart_and_verify(&mut head, 2, 3);

    // Add a non-stale sample to series1. It should not be counted as stale now.
    append_float(&head, &series1, 300, 10.0);
    verify(&head, 1, 3);
    restart_and_verify(&mut head, 1, 3);

    // Test that series3 doesn't become stale when we add another normal sample.
    append_float(&head, &series3, 200, 10.0);
    verify(&head, 1, 3);

    // Test histogram stale samples as well.
    let series4 = lbl(&["name", "series4", "type", "histogram"]);
    let h = tu::generate_test_histograms(1).remove(0);
    append_hist(&head, &series4, 100, &h);
    verify(&head, 1, 4);

    // Make histogram series stale.
    let stale_h = stale_hist(&h);
    append_hist(&head, &series4, 200, &stale_h);
    verify(&head, 2, 4);

    // Test float histogram stale samples.
    let series5 = lbl(&["name", "series5", "type", "float_histogram"]);
    let fh = tu::generate_test_float_histograms(1).remove(0);
    append_fhist(&head, &series5, 100, &fh);
    verify(&head, 2, 5);
    restart_and_verify(&mut head, 2, 5);

    // Make float histogram series stale.
    let stale_fh = stale_fhist(&fh);
    append_fhist(&head, &series5, 200, &stale_fh);
    verify(&head, 3, 5);

    // Make histogram sample non-stale and stale back again.
    append_hist(&head, &series4, 210, &h);
    verify(&head, 2, 5);
    append_hist(&head, &series4, 220, &stale_h);
    verify(&head, 3, 5);

    // Make float histogram sample non-stale and stale back again.
    append_fhist(&head, &series5, 210, &fh);
    verify(&head, 2, 5);
    append_fhist(&head, &series5, 220, &stale_fh);
    verify(&head, 3, 5);

    // Series 1 and 3 are not stale at this point. Add a new sample to series 1 and series 5, so after the GC and
    // removing series 2, 3, 4, we should be left with 1 stale and 1 non-stale series.
    append_float(&head, &series1, 400, 10.0);
    append_fhist(&head, &series5, 400, &stale_fh);
    restart_and_verify(&mut head, 3, 5);

    // This will test restarting with snapshot.
    head.opts.enable_memory_snapshot_on_shutdown.store(true, Ordering::Relaxed);
    restart_and_verify(&mut head, 3, 5);

    // Test garbage collection behavior - stale series should be decremented when GC'd.
    // Force a garbage collection by truncating old data.
    head.truncate(300).expect("truncate");

    // After truncation, run GC to collect old chunks/series.
    head.gc();

    // series 1 and series 5 are left.
    verify(&head, 1, 2);

    // Test creating a new series for each of float, histogram, float histogram that starts as stale.
    // This should be counted as stale.
    let series6 = lbl(&["name", "series6", "direct", "stale"]);
    let series7 = lbl(&["name", "series7", "direct", "stale"]);
    let series8 = lbl(&["name", "series8", "direct", "stale"]);
    append_float(&head, &series6, 400, stale_nan());
    verify(&head, 2, 3);
    append_hist(&head, &series7, 400, &stale_h);
    verify(&head, 3, 4);
    append_fhist(&head, &series8, 400, &stale_fh);
    verify(&head, 4, 5);
}

/// Go `staleSampleKind`.
#[derive(Clone, Copy, Debug, PartialEq)]
enum StaleKind {
    Float,
    Histogram,
    FloatHistogram,
}

impl std::fmt::Display for StaleKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            StaleKind::Float => "float",
            StaleKind::Histogram => "histogram",
            StaleKind::FloatHistogram => "float_histogram",
        })
    }
}

/// Go `appendStaleTestSample`.
fn append_stale_test_sample(head: &Head, kind: StaleKind, lbls: &Labels, ts: i64, stale: bool) {
    let mut app = head.appender(&ctx());
    let r = match kind {
        StaleKind::Float => {
            let v = if stale { stale_nan() } else { ts as f64 };
            app.append(0, lbls, ts, v)
        }
        StaleKind::Histogram => {
            let mut h = tu::generate_test_histogram(1);
            if stale {
                h.sum = stale_nan();
            }
            app.append_histogram(0, lbls, ts, Some(&h), None)
        }
        StaleKind::FloatHistogram => {
            let mut fh = tu::generate_test_float_histogram(1);
            if stale {
                fh.sum = stale_nan();
            }
            app.append_histogram(0, lbls, ts, None, Some(&fh))
        }
    };
    r.expect("append");
    assert!(app.commit().is_none());
}

#[test]
fn test_head_num_stale_series_mixed_type() {
    use StaleKind::*;
    let transitions = [
        (Float, Histogram),
        (Float, FloatHistogram),
        (Histogram, Float),
        (Histogram, FloatHistogram),
        (FloatHistogram, Float),
        (FloatHistogram, Histogram),
    ];
    for mode in ["live", "wal", "snapshot"] {
        for (stale, resolve) in transitions {
            let name = format!("TestHead_NumStaleSeries_MixedType/{mode}/{stale}_to_{resolve}");
            let opts = new_test_head_default_options(1000, false);
            let opts = opts;
            opts.enable_memory_snapshot_on_shutdown.store(mode == "snapshot", std::sync::atomic::Ordering::Relaxed);
            let mut head = new_test_head_with_options("num-stale-mixed", compression::Type::None, opts);
            head.init(0).expect("init");

            let lbls = lbl(&["name", &name]);
            // Going stale counts the series, resolving with a different type must uncount it.
            append_stale_test_sample(&head, stale, &lbls, 100, true);
            assert_eq!(1, head.num_stale_series(), "{name}");
            append_stale_test_sample(&head, resolve, &lbls, 200, false);
            assert_eq!(0, head.num_stale_series(), "{name}");

            if mode != "live" {
                reopen_same(&mut head);
                head.init(0).expect("init");
            }

            assert_eq!(1, head.num_series(), "{name}");
            assert_eq!(0, head.num_stale_series(), "{name}");
        }
    }
}

#[test]
fn test_head_num_stale_series_mixed_type_removal() {
    fn series_ref(head: &Head, lbls: &Labels) -> SeriesRef {
        head.series.get_by_hash(lbls.hash(), lbls).expect("series").r#ref
    }
    for (name, want_series, cross_type_survives) in [("gc", 1u64, false), ("truncate_stale_series", 2, true)] {
        let head = new_test_head("num-stale-mixed-removal", 1000, compression::Type::None, false);
        head.init(0).expect("init");

        // A series that went stale as a float and became non-stale as a histogram.
        let cross_type = lbl(&["name", "cross_type"]);
        append_stale_test_sample(&head, StaleKind::Float, &cross_type, 100, true);
        append_stale_test_sample(&head, StaleKind::Histogram, &cross_type, 200, false);
        // A genuinely stale series.
        let stale_only = lbl(&["name", "stale_only"]);
        append_stale_test_sample(&head, StaleKind::Histogram, &stale_only, 100, true);
        // A recent non-stale series, kept alive past the truncation point.
        let keeper = lbl(&["name", "keeper"]);
        append_stale_test_sample(&head, StaleKind::Float, &keeper, 5000, false);
        assert_eq!(1, head.num_stale_series(), "{name}");

        let cross_type_ref = series_ref(&head, &cross_type);
        let stale_only_ref = series_ref(&head, &stale_only);
        let keeper_ref = series_ref(&head, &keeper);
        let refs = vec![cross_type_ref, stale_only_ref, keeper_ref];
        let stale_refs = head.stale_series_refs_no_ooo_data(&ctx()).expect("stale refs");
        assert_eq!(vec![stale_only_ref], *stale_refs.sorted_by_ref, "{name}");
        if name == "gc" {
            head.truncate(1000).expect("truncate");
            head.gc();
        } else {
            head.truncate_stale_series(&refs, 1000, &head.snapshot_fingerprints(&refs, u64::MAX))
                .expect("truncate stale series");
        }

        assert_eq!(want_series, head.num_series(), "{name}");
        assert_eq!(0, head.num_stale_series(), "{name}");
        if cross_type_survives {
            assert!(
                head.series.get_by_hash(cross_type.hash(), &cross_type).is_some(),
                "cross-type non-stale series must survive"
            );
        } else {
            assert!(head.series.get_by_hash(cross_type.hash(), &cross_type).is_none(), "{name}");
        }
    }
}

#[test]
fn test_head_num_stale_series_wal_replay_ignores_rejected_cross_type_samples() {
    let stale = stale_nan();
    let hist = Arc::new(tu::generate_test_histogram(1));
    let stale_h = Arc::new(stale_hist(&hist));
    let float_hist = Arc::new(tu::generate_test_float_histogram(1));
    let rs = |t: i64, v: f64| RefSample { r#ref: 1, st: 0, t, v };

    let tests: Vec<(&str, Rec, Rec)> = vec![
        (
            "histogram_then_older_float",
            Rec::Histograms(vec![RefHistogramSample { r#ref: 1, st: 0, t: 200, h: stale_h.clone() }]),
            Rec::Samples(vec![rs(100, 1.0)]),
        ),
        (
            "float_then_older_histogram",
            Rec::Samples(vec![rs(200, stale)]),
            Rec::Histograms(vec![RefHistogramSample { r#ref: 1, st: 0, t: 100, h: hist.clone() }]),
        ),
        (
            "float_then_older_float_histogram",
            Rec::Samples(vec![rs(200, stale)]),
            Rec::FloatHistograms(vec![RefFloatHistogramSample { r#ref: 1, st: 0, t: 100, fh: float_hist.clone() }]),
        ),
    ];
    for (name, newer, older) in tests {
        let head = new_test_head("stale-wal-replay-rejected", 1000, compression::Type::None, false);
        populate_test_wl(
            head.wal.as_ref().unwrap(),
            &[
                Rec::Series(vec![RefSeries { r#ref: 1, labels: lbl(&["name", name]) }]),
                Rec::Samples(vec![rs(0, 1.0)]),
                newer,
                older,
            ],
            false,
        );

        head.init(0).expect("init");

        let series = head.series.get_by_id(1).expect("series");
        {
            let g = series.lock();
            assert_eq!(200, g.max_time(), "{name}");
            assert!(is_stale_series(&g), "{name}");
        }
        assert_eq!(1, head.num_series(), "{name}");
        assert_eq!(1, head.num_stale_series(), "{name}");
    }
}

#[test]
fn test_head_wal_replay_stale_marker_type_consistency() {
    for float_histogram in [false, true] {
        for mmapped in [false, true] {
            for st_storage in [false, true] {
                let name = format!("floatHistogram={float_histogram}/mmapped={mmapped}/stStorage={st_storage}");
                let opts = new_test_head_default_options(1000, false);
                if st_storage {
                    opts.enable_st_storage.store(true, Ordering::Relaxed);
                    opts.float_chunk_encoding.store(ENC_XOR2.0 as u32, Ordering::Relaxed);
                    opts.enable_histogram_st_encoding.store(true, Ordering::Relaxed);
                }
                let mut head = new_test_head_with_options("stale-marker-type", compression::Type::None, opts);
                head.init(0).expect("init");

                let lbls = lbl(&["foo", "bar"]);
                let want_type = if float_histogram { ValueType::FloatHistogram } else { ValueType::Histogram };

                // Establish the series as a native histogram.
                let mut app = head.appender(&ctx());
                if float_histogram {
                    app.append_histogram(0, &lbls, 100, None, Some(&tu::generate_test_float_histogram(1)))
                } else {
                    app.append_histogram(0, &lbls, 100, Some(&tu::generate_test_histogram(1)), None)
                }
                .expect("append");
                assert!(app.commit().is_none());

                // For the m-mapped case the marker crosses the chunk-range boundary so the histogram chunk becomes
                // a completed predecessor that gets m-mapped; on replay those histogram samples are skipped.
                let marker_t: i64 = if mmapped { 1000 } else { 200 };

                // Append the staleness marker in a separate appender so it is logged as a float record (converted
                // to a histogram marker for the chunk).
                let mut app = head.appender(&ctx());
                app.append(0, &lbls, marker_t, stale_nan()).expect("append");
                assert!(app.commit().is_none());

                if mmapped {
                    head.mmap_head_chunks();
                    let s = head.series.get_by_hash(lbls.hash(), &lbls).expect("series");
                    let n_mmapped = s.lock().mmapped_chunks.len();
                    assert_ne!(0, n_mmapped, "histogram chunk must be m-mapped for this case");
                }

                // Reopen, forcing WAL replay of the float staleness record.
                reopen_same(&mut head);
                head.init(0).expect("init");

                let mut q = new_block_querier(&**head, i64::MIN, i64::MAX).expect("querier");
                {
                    let mut ss = q.select(&ctx(), false, None, &[eq_matcher("foo", "bar")]);
                    assert!(ss.next(), "{name}");
                    let series = ss.at().clone();
                    let mut it = series.iterator(None);
                    let mut got_type = ValueType::None;
                    let mut found = false;
                    loop {
                        let vt = it.next();
                        if vt == ValueType::None {
                            break;
                        }
                        if it.at_t() == marker_t {
                            got_type = vt;
                            found = true;
                        }
                    }
                    assert!(it.err().is_none(), "{name}");
                    drop(it);
                    assert!(!ss.next(), "{name}");
                    assert!(ss.err().is_none(), "{name}");
                    assert!(ss.warnings().is_empty(), "{name}");
                    assert!(found, "staleness marker not found ({name})");
                    assert_eq!(
                        want_type, got_type,
                        "replayed staleness marker must keep the series' native histogram type ({name})"
                    );
                }
                assert!(q.close().is_none());
            }
        }
    }
}

#[test]
fn test_head_num_native_histogram_series_and_buckets() {
    let mut head = new_test_head("native-hist-counts", 1000, compression::Type::None, false);
    head.init(0).expect("init");

    // Initially, there are no native histogram series or buckets.
    assert_eq!(0, head.num_native_histogram_series());
    assert_eq!(0, head.num_native_histogram_buckets());

    let verify = |head: &Head, nhs: u64, nhb: u64, ns: u64| {
        assert_eq!(nhs, head.num_native_histogram_series());
        assert_eq!(nhb, head.num_native_histogram_buckets());
        assert_eq!(ns, head.num_series());
    };
    let restart_and_verify = |th: &mut TestHead, nhs: u64, nhb: u64, ns: u64| {
        verify(th, nhs, nhb, ns);
        reopen_same(th);
        th.init(0).expect("init");
        verify(th, nhs, nhb, ns);
    };

    // The standard test histograms carry 4 positive and 4 negative buckets each.
    let std_hist = tu::generate_test_histograms(3);
    assert_eq!(8, std_hist[0].positive_buckets.len() + std_hist[0].negative_buckets.len());
    let std_float_hist = tu::generate_test_float_histograms(3);
    assert_eq!(8, std_float_hist[0].positive_buckets.len() + std_float_hist[0].negative_buckets.len());
    // A histogram with a different (3) bucket count, to exercise bucket deltas.
    let three_bucket_hist = Histogram {
        // Buckets accumulate to 2, 3, 4, so the observation count is 9.
        count: 9,
        zero_threshold: 0.001,
        schema: 1,
        sum: 10.0,
        positive_spans: vec![Span { offset: 0, length: 3 }],
        positive_buckets: vec![2, 1, 1],
        ..Default::default()
    };

    let series1 = lbl(&["name", "series1"]);
    let series2 = lbl(&["name", "series2"]);

    // Plain float series never count as native histogram series.
    append_float(&head, &series1, 100, 1.0);
    verify(&head, 0, 0, 1);

    // A float series turning into a native histogram series is counted, and its buckets are added.
    append_hist(&head, &series1, 110, &std_hist[0]);
    verify(&head, 1, 8, 1);

    // Another native histogram sample with the same bucket count keeps the counts.
    append_hist(&head, &series1, 120, &std_hist[1]);
    verify(&head, 1, 8, 1);

    // A new series that starts directly as a float histogram is counted.
    append_fhist(&head, &series2, 100, &std_float_hist[0]);
    verify(&head, 2, 16, 2);

    restart_and_verify(&mut head, 2, 16, 2);

    // A native histogram sample with fewer buckets adjusts the bucket gauge by the delta (8 -> 3).
    append_hist(&head, &series1, 130, &three_bucket_hist);
    verify(&head, 2, 11, 2);

    // A float sample on a native histogram series removes it from both gauges.
    append_float(&head, &series1, 140, 5.0);
    verify(&head, 1, 8, 2);

    restart_and_verify(&mut head, 1, 8, 2);

    // A proper staleness marker (an empty histogram) keeps the series counted as a native histogram series but
    // drops its buckets to zero.
    let stale_float_hist = FloatHistogram {
        sum: stale_nan(),
        ..Default::default()
    };
    append_fhist(&head, &series2, 110, &stale_float_hist);
    verify(&head, 1, 0, 2);

    // A subsequent non-stale float histogram restores the bucket count.
    append_fhist(&head, &series2, 120, &std_float_hist[1]);
    verify(&head, 1, 8, 2);

    // This will test restarting with snapshot.
    head.opts.enable_memory_snapshot_on_shutdown.store(true, Ordering::Relaxed);
    restart_and_verify(&mut head, 1, 8, 2);

    // Garbage collection: a removed native histogram series decrements both gauges.
    // Keep series2 alive with a fresh sample, then truncate away the older series1.
    append_fhist(&head, &series2, 400, &std_float_hist[2]);
    head.truncate(300).expect("truncate");
    head.gc();
    // series1 (float, last sample at ts 140) is gone; series2 remains as a native histogram series with 8
    // buckets.
    verify(&head, 1, 8, 1);

    // A brand new series starting directly as a native histogram is counted.
    let series3 = lbl(&["name", "series3"]);
    append_hist(&head, &series3, 400, &std_hist[2]);
    verify(&head, 2, 16, 2);

    // GC of that native histogram series decrements the gauges again.
    append_fhist(&head, &series2, 500, &std_float_hist[0]);
    head.truncate(450).expect("truncate");
    head.gc();
    // series3 (last sample at ts 400) is removed, series2 remains.
    verify(&head, 1, 8, 1);
    drop(head);

    // stale payload buckets are counted
    type NewSample = fn() -> (Option<Histogram>, Option<FloatHistogram>);
    let variants: Vec<(&str, NewSample)> = vec![
        ("histogram", || (Some(tu::generate_test_histograms(1).remove(0)), None)),
        ("float_histogram", || (None, Some(tu::generate_test_float_histograms(1).remove(0)))),
        ("custom_bucket_histogram", || (Some(tu::generate_test_custom_buckets_histogram(1)), None)),
        ("custom_bucket_float_histogram", || (None, Some(tu::generate_test_custom_buckets_float_histogram(1)))),
    ];
    for appender_v2 in [false, true] {
        for snapshot in [false, true] {
            for (vname, new_sample) in &variants {
                let name = format!(
                    "TestHead_NumNativeHistogramSeriesAndBuckets/stale_payload_buckets_are_counted/appender_v2={appender_v2}/snapshot={snapshot}/{vname}"
                );
                let opts = new_test_head_default_options(1000, false);
                opts.enable_memory_snapshot_on_shutdown.store(snapshot, std::sync::atomic::Ordering::Relaxed);
                let mut test_head = new_test_head_with_options("stale-payload-buckets", compression::Type::None, opts);
                test_head.init(0).expect("init");

                let append_sample = |head: &Head, lbls: &Labels, ts: i64, v: f64, h: Option<&Histogram>, fh: Option<&FloatHistogram>| {
                    if appender_v2 {
                        let mut app = head.appender_v2(&ctx());
                        app.append(0, lbls, 0, ts, v, h, fh, &AppendV2Options::default()).expect("append");
                        assert!(app.commit().is_none());
                        return;
                    }
                    let mut app = head.appender(&ctx());
                    if h.is_some() || fh.is_some() {
                        app.append_histogram(0, lbls, ts, h, fh).expect("append histogram");
                    } else {
                        app.append(0, lbls, ts, v).expect("append");
                    }
                    assert!(app.commit().is_none());
                };

                let lbls = lbl(&["name", &name]);
                let (mut h, mut fh) = new_sample();
                let payload_buckets = match (&h, &fh) {
                    (Some(h), _) => h.positive_buckets.len() + h.negative_buckets.len(),
                    (_, Some(fh)) => fh.positive_buckets.len() + fh.negative_buckets.len(),
                    _ => unreachable!(),
                } as u64;
                assert!(payload_buckets > 0);

                if let Some(h) = &mut h {
                    h.sum = stale_nan();
                } else if let Some(fh) = &mut fh {
                    fh.sum = stale_nan();
                }
                append_sample(&test_head, &lbls, 100, 0.0, h.as_ref(), fh.as_ref());
                assert_eq!(1, test_head.num_stale_series(), "{name}");
                assert_eq!(1, test_head.num_native_histogram_series(), "{name}");
                assert_eq!(payload_buckets, test_head.num_native_histogram_buckets(), "{name}");

                reopen_same(&mut test_head);
                test_head.init(0).expect("init");

                assert_eq!(1, test_head.num_stale_series(), "{name}");
                assert_eq!(1, test_head.num_native_histogram_series(), "{name}");
                assert_eq!(payload_buckets, test_head.num_native_histogram_buckets(), "{name}");

                let series = test_head.series.get_by_hash(lbls.hash(), &lbls).expect("series");
                let stale_refs = vec![series.r#ref];
                test_head
                    .truncate_stale_series(&stale_refs, 100, &test_head.snapshot_fingerprints(&stale_refs, u64::MAX))
                    .expect("truncate stale series");
                assert_eq!(0, test_head.num_series(), "{name}");
                assert_eq!(0, test_head.num_stale_series(), "{name}");
                assert_eq!(0, test_head.num_native_histogram_series(), "{name}");
                assert_eq!(0, test_head.num_native_histogram_buckets(), "{name}");
            }
        }
    }
}

#[test]
fn test_head_filter_selected_series_and_sort_postings() {
    let head = new_test_head("filter-selected-series", 1000, compression::Type::None, false);
    head.init(0).expect("init");

    // Enable OOO so we can push an OOO sample.
    head.opts.out_of_order_time_window.store(1000, Ordering::Relaxed);

    let clean = lbl(&["name", "clean"]);
    let with_ooo = lbl(&["name", "with-ooo"]);

    let mut app = head.appender(&ctx());
    let clean_ref = app.append(0, &clean, 200, 1.0).expect("append");
    let with_ooo_ref = app.append(0, &with_ooo, 200, 2.0).expect("append");
    assert!(app.commit().is_none());

    // Push an OOO sample for the second series so its s.ooo becomes non-nil.
    let mut app = head.appender(&ctx());
    app.append(with_ooo_ref, &with_ooo, 100, 2.5).expect("append");
    assert!(app.commit().is_none());

    // A ref that does not resolve to any series in the head must be silently dropped.
    let bogus_ref: SeriesRef = u64::MAX;

    let mut p = new_list_postings(vec![clean_ref, with_ooo_ref, bogus_ref]);
    let kept = head
        .filter_series_and_sort_postings(&mut p, Some(&|_, g| is_series_without_ooo(g)))
        .expect("filter");
    assert_eq!(vec![clean_ref], *kept.sorted_by_ref);
    assert_eq!(vec![clean_ref], *kept.sorted_by_labels);
}

#[test]
fn test_histogram_staleness_conversion_metrics() {
    for float in [false, true] {
        let name = if float { "float_staleness_to_float_histogram" } else { "float_staleness_to_histogram" };
        let head = new_test_head("hist-staleness-metrics", 1000, compression::Type::None, false);
        let lbls = lbl(&["name", name]);

        // Step 1: Establish a series with histogram data
        let mut app = head.appender(&ctx());
        if float {
            app.append_histogram(0, &lbls, 1000, None, Some(&tu::generate_test_float_histograms(1)[0]))
        } else {
            app.append_histogram(0, &lbls, 1000, Some(&tu::generate_test_histograms(1)[0]), None)
        }
        .expect("append");
        assert!(app.commit().is_none());

        // Step 2: Add a float staleness marker
        let mut app = head.appender(&ctx());
        app.append(0, &lbls, 2000, stale_nan()).expect("append");
        assert!(app.commit().is_none());

        // Count what was actually stored by querying the series
        let mut q = new_block_querier(&**head, 0, 3000).expect("querier");
        let (mut actual_float, mut actual_hist) = (0, 0);
        {
            let mut ss = q.select(&ctx(), false, None, &[eq_matcher("name", name)]);
            assert!(ss.next());
            let series = ss.at().clone();
            let mut it = series.iterator(None);
            loop {
                match it.next() {
                    ValueType::None => break,
                    ValueType::Float => actual_float += 1,
                    ValueType::Histogram | ValueType::FloatHistogram => actual_hist += 1,
                }
            }
            assert!(it.err().is_none());
        }
        let _ = q.close();

        // Verify what was actually stored - should be 0 floats, 2 histograms (original + converted staleness
        // marker)
        assert_eq!(0, actual_float, "Should have 0 float samples stored");
        assert_eq!(2, actual_hist, "Should have 2 histogram samples: original + converted staleness marker");

        // The metrics should match what was actually stored
        assert_eq!(
            actual_float as f64,
            head.metrics.samples_appended.with_label_values("float").get(),
            "Float counter should match actual float samples stored"
        );
        assert_eq!(
            actual_hist as f64,
            head.metrics.samples_appended.with_label_values("histogram").get(),
            "Histogram counter should match actual histogram samples stored"
        );
    }
}

fn st_opts(base: HeadOptions, enable_st: bool) -> HeadOptions {
    base.enable_st_storage.store(enable_st, Ordering::Relaxed);
    let enc = if enable_st { ENC_XOR2 } else { ENC_XOR };
    base.float_chunk_encoding.store(enc.0 as u32, Ordering::Relaxed);
    base.enable_histogram_st_encoding.store(enable_st, Ordering::Relaxed);
    base
}

#[test]
fn test_head_appender_wal_encoder_enable_st_storage() {
    for enable_st in [false, true] {
        let opts = st_opts(new_test_head_default_options(DEFAULT_BLOCK_DURATION, false), enable_st);
        let h = new_test_head_with_options("wal-encoder-st", compression::Type::None, opts);

        let lbls = lbl(&["foo", "bar"]);
        let mut app = h.appender_v2(&ctx());
        for ts in 100..110i64 {
            app.append(0, &lbls, 0, ts, ts as f64, None, None, &AppendV2Options::default()).expect("append");
        }
        assert!(app.commit().is_none());
        h.close().expect("close");

        // Read WAL segments directly and check the sample record type.
        let mut found_sample_record = false;
        for rt in wal_record_types(h.wal.as_ref().unwrap().dir()) {
            match rt {
                record::Type::Samples => {
                    assert!(!enable_st, "WAL contains Samples (V1) record but EnableSTStorage=true, expected SamplesV2");
                    found_sample_record = true;
                }
                record::Type::SamplesV2 => {
                    assert!(enable_st, "WAL contains SamplesV2 record but EnableSTStorage=false, expected Samples (V1)");
                    found_sample_record = true;
                }
                _ => {}
            }
        }
        assert!(found_sample_record, "no sample record found in WAL");
    }
}

/// A head with a WAL and a WBL in `dir` (closed on drop).
fn head_with_wbl(dir: TempDir, opts: HeadOptions, compress: compression::Type) -> TestHead {
    let (head, wal) = open_head_with_wbl(dir.path(), opts, compress);
    TestHead { head, wal: Some(wal), dir }
}

fn open_head_with_wbl(dir: &Path, mut opts: HeadOptions, compress: compression::Type) -> (Arc<Head>, Arc<WL>) {
    let wal = Arc::new(WL::new_size(dir.join("wal"), 32768, compress).expect("wal"));
    let wbl = Arc::new(WL::new_size(dir.join(wlog::WBL_DIR_NAME), 32768, compress).expect("wbl"));
    opts.chunk_dir_root = dir.to_path_buf();
    let head = Head::new(Some(wal.clone()), Some(wbl), opts, None).expect("new head");
    (head, wal)
}

/// Reopens a head made by [`head_with_wbl`] with the same options.
fn reopen_with_wbl(mut th: TestHead, compress: compression::Type) -> TestHead {
    th.head.close().expect("close");
    let opts = clone_opts(&th.head.opts);
    let (head, wal) = open_head_with_wbl(th.dir.path(), opts, compress);
    th.head = head;
    th.wal = Some(wal);
    th
}

#[test]
fn test_head_appender_wbl_encoder_enable_st_storage() {
    for enable_st in [false, true] {
        let opts = HeadOptions {
            chunk_range: DEFAULT_BLOCK_DURATION,
            ..HeadOptions::default()
        };
        opts.out_of_order_time_window.store(60 * 60 * 1000, Ordering::Relaxed);
        let opts = st_opts(opts, enable_st);
        let h = head_with_wbl(TempDir::named("wbl-encoder-st"), opts, compression::Type::None);
        h.init(0).expect("init");

        let lbls = lbl(&["foo", "bar"]);

        // Append an in-order sample to establish head maxt.
        let mut app = h.appender_v2(&ctx());
        app.append(0, &lbls, 0, 200, 200.0, None, None, &AppendV2Options::default()).expect("append");
        assert!(app.commit().is_none());

        // Append OOO samples; these are written to the WBL.
        let mut app = h.appender_v2(&ctx());
        for ts in 100..110i64 {
            app.append(0, &lbls, 0, ts, ts as f64, None, None, &AppendV2Options::default()).expect("append");
        }
        assert!(app.commit().is_none());

        h.close().expect("close");

        // Read WBL segments directly and check the sample record type.
        let mut found_sample_record = false;
        for rt in wal_record_types(&h.dir.path().join(wlog::WBL_DIR_NAME)) {
            match rt {
                record::Type::Samples => {
                    assert!(!enable_st, "WBL contains Samples (V1) record but EnableSTStorage=true, expected SamplesV2");
                    found_sample_record = true;
                }
                record::Type::SamplesV2 => {
                    assert!(enable_st, "WBL contains SamplesV2 record but EnableSTStorage=false, expected Samples (V1)");
                    found_sample_record = true;
                }
                _ => {}
            }
        }
        assert!(found_sample_record, "no sample record found in WBL");
    }
}

/// The chunk metas and chunks of the single series `{foo="bar"}` of the head (Go: `h.Index()`, `h.Chunks()`,
/// `Postings`, `Series`, `ChunkOrIterable`). Panics if a chunk comes back as an iterable.
fn head_series_chunks(h: &Head) -> Vec<Arc<dyn chunkenc::Chunk>> {
    let idx_reader = h.index();
    let chk_reader = h.chunks().expect("chunks");
    let mut chks = Vec::new();
    {
        let mut p = idx_reader.postings(&ctx(), "foo", &["bar"]).expect("postings");
        let mut builder = ScratchBuilder::new(0);
        assert!(p.next());
        let s_ref = p.at();
        let mut metas: Vec<Meta> = Vec::new();
        idx_reader.series(s_ref, &mut builder, &mut metas).expect("series");
        for meta in &metas {
            match chk_reader.chunk_or_iterable(meta).expect("chunk") {
                ChunkOrIterable::Chunk(c) => chks.push(c),
                ChunkOrIterable::Iterable(_) => panic!("expected a chunk, got an iterable"),
            }
        }
    }
    idx_reader.close().expect("close index");
    chk_reader.close().expect("close chunks");
    chks
}

#[test]
fn test_head_appender_st_storage_disabled() {
    let samples = [(10i64, 100i64, 1.0f64), (20, 200, 2.0), (30, 300, 3.0)];

    let opts = new_test_head_default_options(DEFAULT_BLOCK_DURATION, false);
    opts.enable_st_storage.store(false, Ordering::Relaxed); // Explicitly disable ST storage.
    let h = new_test_head_with_options("st-storage-disabled", compression::Type::None, opts);

    let lbls = lbl(&["foo", "bar"]);
    let mut a = h.appender_v2(&ctx());
    for (st, ts, f) in samples {
        a.append(0, &lbls, st, ts, f, None, None, &AppendV2Options::default()).expect("append");
    }
    assert!(a.commit().is_none());

    for chk in head_series_chunks(&h) {
        let mut it = chk.iterator(None);
        while it.next() != ValueType::None {
            assert_eq!(0, it.at_st(), "ST should be 0 when EnableSTStorage is false");
        }
        assert!(it.err().is_none());
    }
}

type StAppend = fn(&mut dyn storage::AppenderV2, &Labels, i64, i64) -> (Sample, Result<SeriesRef, storage::Error>);

fn st_append_cases() -> Vec<(&'static str, StAppend)> {
    vec![
        ("float", |app, lbls, st, ts| {
            let r = app.append(0, lbls, st, ts, ts as f64, None, None, &AppendV2Options::default());
            (Sample::float(st, ts, ts as f64), r)
        }),
        ("histogram", |app, lbls, st, ts| {
            let mut h = tu::generate_test_histogram(ts);
            h.counter_reset_hint = CounterResetHint::NotCounterReset;
            let r = app.append(0, lbls, st, ts, 0.0, Some(&h), None, &AppendV2Options::default());
            (Sample::histogram(st, ts, h), r)
        }),
        ("float histogram", |app, lbls, st, ts| {
            let mut fh = tu::generate_test_float_histogram(ts);
            fh.counter_reset_hint = CounterResetHint::NotCounterReset;
            let r = app.append(0, lbls, st, ts, 0.0, None, Some(&fh), &AppendV2Options::default());
            (Sample::float_histogram(st, ts, fh), r)
        }),
    ]
}

#[test]
fn test_head_appender_st_storage_wal_replay() {
    const ST: i64 = 50;
    for (name, append) in st_append_cases() {
        let opts = st_opts(new_test_head_default_options(DEFAULT_BLOCK_DURATION, false), true);
        let mut h = new_test_head_with_options("st-storage-wal-replay", compression::Type::None, opts);

        let lbls = lbl(&["foo", "bar"]);
        let mut a = h.appender_v2(&ctx());
        let mut expected = Vec::new();
        for ts in 100..200i64 {
            let (s, r) = append(&mut *a, &lbls, ST, ts);
            r.unwrap_or_else(|e| panic!("{name}: {e}"));
            expected.push(s);
        }
        assert!(a.commit().is_none());

        // Reopen the head, triggering WAL replay.
        h.head.close().expect("close");
        let w = Arc::new(WL::new(h.wal.as_ref().unwrap().dir(), compression::Type::None).expect("wal"));
        let opts = clone_opts(&h.head.opts);
        h.head = Head::new(Some(w.clone()), None, opts, None).expect("new head");
        h.wal = Some(w);
        h.init(0).expect("init");

        let key = r#"{foo="bar"}"#;
        let got = head_query(&h, 100, 199, "foo", "bar");
        let mut exp = BTreeMap::new();
        exp.insert(key.to_string(), expected.clone());
        require_equal_series(&exp, &got, true);
        for (i, e) in expected.iter().enumerate() {
            assert_eq!(e.st, got[key][i].st, "ST mismatch at [{i}] ({name})");
        }
    }
}

#[test]
fn test_wal_replay_store_st_passed() {
    // Phase 1: write a WAL with EncXOR and no ST storage.
    let opts1 = new_test_head_default_options(DEFAULT_BLOCK_DURATION, false);
    opts1.float_chunk_encoding.store(ENC_XOR.0 as u32, Ordering::Relaxed);
    let mut h = new_test_head_with_options("wal-replay-store-st", compression::Type::None, opts1);

    let lbls = lbl(&["foo", "bar"]);
    let mut app = h.appender(&ctx());
    for ts in 1..=5i64 {
        app.append(0, &lbls, ts, ts as f64).expect("append");
    }
    assert!(app.commit().is_none());
    h.head.close().expect("close");

    // Phase 2: replay the WAL with EncXOR2 + ST storage enabled. If storeST were not passed during WAL replay,
    // the XOR chunk loaded from the chunk snapshot would be continued with EncXOR2 appends (CompatibleValues =
    // true, no cut), which would produce a chunk that cannot store start timestamps.
    let mut opts2 = new_test_head_default_options(DEFAULT_BLOCK_DURATION, false);
    opts2.float_chunk_encoding.store(ENC_XOR2.0 as u32, Ordering::Relaxed);
    opts2.enable_st_storage.store(true, Ordering::Relaxed);
    opts2.chunk_dir_root = h.head.opts.chunk_dir_root.clone();

    let w2 = Arc::new(WL::new(h.wal.as_ref().unwrap().dir(), compression::Type::None).expect("wal"));
    h.head = Head::new(Some(w2.clone()), None, opts2, None).expect("new head");
    h.wal = Some(w2);
    h.init(0).expect("init");

    // Append one post-replay sample with a start timestamp to confirm the active chunk is EncXOR2 (which can
    // store ST).
    let mut app2 = h.appender(&ctx());
    app2.append(0, &lbls, 10, 10.0).expect("append");
    assert!(app2.commit().is_none());

    let (ms, created) = h.get_or_create(lbls.hash(), &lbls, false).expect("get or create");
    assert!(!created);
    let g = ms.lock();
    let hc = g.head_chunk().expect("head chunk");
    assert_eq!(
        ENC_XOR2,
        hc.chunk.encoding(),
        "active chunk after WAL replay into EncXOR2+STStorage head must use EncXOR2"
    );
}

#[test]
fn test_head_appender_st_storage_wbl_replay() {
    const ST: i64 = 50;
    for (name, append) in st_append_cases() {
        let opts = HeadOptions {
            chunk_range: DEFAULT_BLOCK_DURATION,
            ..HeadOptions::default()
        };
        opts.out_of_order_time_window.store(60 * 60 * 1000, Ordering::Relaxed);
        let opts = st_opts(opts, true);
        let h = head_with_wbl(TempDir::named("st-storage-wbl-replay"), opts, compression::Type::None);
        h.init(0).expect("init");

        let lbls = lbl(&["foo", "bar"]);

        // Append an in-order sample to establish the head's maxt.
        let mut app = h.appender_v2(&ctx());
        append(&mut *app, &lbls, ST, 200).1.expect("append");
        assert!(app.commit().is_none());

        // Append OOO samples with non-zero ST; these go to the WBL. Use fewer than DefaultOutOfOrderCapMax (32)
        // samples so they all stay in the OOO head chunk (not mmap'd) and are exclusively recovered via WBL
        // replay.
        let mut app = h.appender_v2(&ctx());
        let mut expected = Vec::new();
        for ts in 100..120i64 {
            let (s, r) = append(&mut *app, &lbls, ST, ts);
            r.unwrap_or_else(|e| panic!("{name}: {e}"));
            expected.push(s);
        }
        assert!(app.commit().is_none());

        // Reopen the head, triggering WBL replay.
        let h2 = reopen_with_wbl(h, compression::Type::None);
        h2.init(0).expect("init");

        // Access the OOO head chunk directly and verify samples survived WBL replay.
        let (ms, created) = h2.get_or_create(lbls.hash(), &lbls, false).expect("get or create");
        assert!(!created);
        let chks = {
            let mut g = ms.lock();
            let ooo = g.ooo_mut().expect("ooo fields");
            let hc = ooo.ooo_head_chunk.as_mut().expect("ooo head chunk");
            hc.chunk.to_encoded_chunks(i64::MIN, i64::MAX, true, true).expect("encode")
        };
        assert_eq!(1, chks.len());

        // Go expandOOO.
        let mut got = Vec::new();
        let mut it = chks[0].chunk.iterator(None);
        loop {
            match it.next() {
                ValueType::None => break,
                ValueType::Float => {
                    let (t, v) = it.at();
                    got.push(Sample::float(it.at_st(), t, v));
                }
                ValueType::Histogram => {
                    let (t, h) = it.at_histogram(None);
                    got.push(Sample::histogram(it.at_st(), t, h));
                }
                ValueType::FloatHistogram => {
                    let (t, fh) = it.at_float_histogram(None);
                    got.push(Sample::float_histogram(it.at_st(), t, fh));
                }
            }
        }
        assert!(it.err().is_none(), "{name}");
        require_equal_samples("ooo", &expected, &got, true);
        for (i, e) in expected.iter().enumerate() {
            assert_eq!(e.st, got[i].st, "ST mismatch at [{i}] ({name})");
        }
    }
}

/// Phase 2 of the cross-encoding tests: reopen on the same WAL with new float encoding and ST storage, check
/// the replayed data and the encoding of the head chunk.
fn cross_encoding_reopen(h: &mut TestHead, enable_st: bool, want_enc: chunkenc::Encoding, msg: &str) {
    h.head.close().expect("close");
    let w = Arc::new(WL::new(h.wal.as_ref().unwrap().dir(), compression::Type::None).expect("wal"));
    let opts = clone_opts(&h.head.opts);
    opts.enable_st_storage.store(enable_st, Ordering::Relaxed);
    opts.float_chunk_encoding.store(want_enc.0 as u32, Ordering::Relaxed);
    h.head = Head::new(Some(w.clone()), None, opts, None).expect("new head");
    h.wal = Some(w);
    h.init(0).expect("init");

    // Data must survive the replay; ST values are 0.
    let got = head_query(h, 100, 109, "foo", "bar");
    let expected: Vec<Sample> = (100..110i64).map(|ts| Sample::float(0, ts, ts as f64)).collect();
    let mut exp = BTreeMap::new();
    exp.insert(r#"{foo="bar"}"#.to_string(), expected);
    assert_eq!(exp, got);

    // Verify the in-memory head chunk uses the replay-time encoding.
    let lbls = lbl(&["foo", "bar"]);
    let (ms, created) = h.get_or_create(lbls.hash(), &lbls, false).expect("get or create");
    assert!(!created);
    let g = ms.lock();
    assert_eq!(want_enc, g.head_chunk().expect("head chunk").chunk.encoding(), "{msg}");
}

#[test]
fn test_head_appender_st_storage_wal_replay_cross_encoding() {
    // Phase 1: write with EncXOR, no ST storage.
    let opts = new_test_head_default_options(DEFAULT_BLOCK_DURATION, false);
    opts.enable_st_storage.store(false, Ordering::Relaxed);
    opts.float_chunk_encoding.store(ENC_XOR.0 as u32, Ordering::Relaxed);
    let mut h = new_test_head_with_options("st-cross-encoding", compression::Type::None, opts);

    let lbls = lbl(&["foo", "bar"]);
    let mut a = h.appender(&ctx());
    for ts in 100..110i64 {
        a.append(0, &lbls, ts, ts as f64).expect("append");
    }
    assert!(a.commit().is_none());

    // Phase 2: reopen with EncXOR2+STStorage=true (migration to ST storage).
    cross_encoding_reopen(&mut h, true, ENC_XOR2, "chunks after replay into EncXOR2+STStorage head must use EncXOR2");
}

#[test]
fn test_head_appender_st_storage_wal_replay_cross_encoding_reverse() {
    // Phase 1: write with EncXOR2 and ST storage enabled.
    let opts = new_test_head_default_options(DEFAULT_BLOCK_DURATION, false);
    opts.enable_st_storage.store(true, Ordering::Relaxed);
    opts.float_chunk_encoding.store(ENC_XOR2.0 as u32, Ordering::Relaxed);
    let mut h = new_test_head_with_options("st-cross-encoding-rev", compression::Type::None, opts);

    let lbls = lbl(&["foo", "bar"]);
    let mut a = h.appender_v2(&ctx());
    for ts in 100..110i64 {
        a.append(0, &lbls, 50, ts, ts as f64, None, None, &AppendV2Options::default()).expect("append");
    }
    assert!(a.commit().is_none());

    // Phase 2: reopen with EncXOR and ST storage disabled (migration away from ST storage).
    cross_encoding_reopen(&mut h, false, ENC_XOR, "chunks after replay into EncXOR+no-ST head must use EncXOR");
}

#[test]
fn test_head_appender_st_storage_chunk_encoding() {
    let test_histogram = tu::generate_test_histogram(1);
    let test_float_histogram = tu::generate_test_float_histogram(1);
    let f = |st, ts, f| AppSample { ts, f, st, ..Default::default() };
    let hh = |st, ts| AppSample {
        ts,
        st,
        h: Some(test_histogram.clone()),
        ..Default::default()
    };
    let ff = |st, ts| AppSample {
        ts,
        st,
        fh: Some(test_float_histogram.clone()),
        ..Default::default()
    };
    let cases = vec![
        ("float samples", vec![f(10, 100, 1.0), f(20, 200, 2.0)], ENC_XOR2, ENC_XOR),
        ("histogram samples", vec![hh(10, 100), hh(20, 200)], ENC_HISTOGRAM_ST, ENC_HISTOGRAM),
        (
            "float histogram samples",
            vec![ff(10, 100), ff(20, 200)],
            ENC_FLOAT_HISTOGRAM_ST,
            ENC_FLOAT_HISTOGRAM,
        ),
    ];
    for (name, samples, st_encoding, regular_encoding) in cases {
        for enable_st in [false, true] {
            let name = format!("{name}/EnableSTStorage={enable_st}");
            let opts = st_opts(new_test_head_default_options(DEFAULT_BLOCK_DURATION, false), enable_st);
            let h = new_test_head_with_options("st-chunk-encoding", compression::Type::None, opts);

            let lbls = lbl(&["foo", "bar"]);
            let mut a = h.appender(&ctx());
            for s in &samples {
                if let Some(hist) = &s.h {
                    a.append_histogram_st_zero_sample(0, &lbls, s.ts, s.st, Some(hist), None).expect("st zero");
                    a.append_histogram(0, &lbls, s.ts, Some(hist), None).expect("append");
                } else if let Some(fh) = &s.fh {
                    a.append_histogram_st_zero_sample(0, &lbls, s.ts, s.st, None, Some(fh)).expect("st zero");
                    a.append_histogram(0, &lbls, s.ts, None, Some(fh)).expect("append");
                } else {
                    a.append_st_zero_sample(0, &lbls, s.ts, s.st).expect("st zero");
                    a.append(0, &lbls, s.ts, s.f).expect("append");
                }
            }
            assert!(a.commit().is_none());

            let chks = head_series_chunks(&h);
            assert!(!chks.is_empty(), "{name}");
            for chk in chks {
                let encoding = chk.encoding();
                if enable_st {
                    assert_eq!(st_encoding, encoding, "Expected ST-capable encoding when EnableSTStorage is true ({name})");
                } else {
                    assert_eq!(regular_encoding, encoding, "Expected regular encoding when EnableSTStorage is false ({name})");
                }
            }
        }
    }
}

/// (number of head chunks, encoding of the newest, encoding of the one before it).
fn head_chunk_encodings(h: &Head, lbls: &Labels) -> (usize, chunkenc::Encoding, Option<chunkenc::Encoding>) {
    let (ms, _) = h.get_or_create(lbls.hash(), lbls, false).expect("get or create");
    let g = ms.lock();
    let n = g.head_chunks.len();
    assert!(n > 0, "headChunks must not be nil");
    let prev = if n >= 2 { Some(g.head_chunks[n - 2].chunk.encoding()) } else { None };
    (n, g.head_chunks[n - 1].chunk.encoding(), prev)
}

fn append_range(h: &Head, lbls: &Labels, from: i64, to: i64) {
    let mut app = h.appender(&ctx());
    for ts in from..=to {
        app.append(0, lbls, ts, ts as f64).expect("append");
    }
    assert!(app.commit().is_none());
}

#[test]
fn test_float_chunk_encoding_switch_waits_for_next_chunk() {
    let set_enc = |h: &Head, e: chunkenc::Encoding| h.opts.float_chunk_encoding.store(e.0 as u32, Ordering::Relaxed);
    let lbls = lbl(&["foo", "bar"]);

    // without_st_storage
    {
        let opts = new_test_head_default_options(DEFAULT_BLOCK_DURATION, false);
        opts.float_chunk_encoding.store(ENC_XOR2.0 as u32, Ordering::Relaxed);
        let h = new_test_head_with_options("enc-switch", compression::Type::None, opts);
        h.init(0).expect("init");

        // Phase 1: Start appending while XOR2 is enabled.
        append_range(&h, &lbls, 1, 5);

        // Simulate a config reload that switches encoding to XOR.
        set_enc(&h, ENC_XOR);

        // A new appender picks up useXOR2=false, but the existing XOR2 chunk must not be cut — XOR and XOR2 are
        // compatible when ST is disabled.
        append_range(&h, &lbls, 6, 10);

        // The head chunk must still be XOR2: encoding change does not cut chunks.
        let (ms, created) = h.get_or_create(lbls.hash(), &lbls, false).expect("get or create");
        assert!(!created);
        drop(ms);
        let (n, enc, _) = head_chunk_encodings(&h, &lbls);
        assert_eq!(
            1, n,
            "chunk must not have been cut; only one chunk must exist after encoding switch without ST storage"
        );
        assert_eq!(ENC_XOR2, enc, "in-progress chunk must keep XOR2 encoding after encoding switch");

        // Phase 2: Fill the current chunk past its natural size limit so that a new chunk is started. The new
        // chunk must use the updated XOR encoding.
        append_range(&h, &lbls, 11, DEFAULT_SAMPLES_PER_CHUNK as i64 * 3);

        let (n, enc, _) = head_chunk_encodings(&h, &lbls);
        assert!(n >= 2, "at least one chunk cut must have occurred during phase 2");
        assert_eq!(ENC_XOR, enc, "new chunk after natural boundary must use XOR encoding");
    }

    // with_st_storage, and with_st_storage_xor_to_xor2
    for (from, to) in [(ENC_XOR2, ENC_XOR), (ENC_XOR, ENC_XOR2)] {
        let opts = new_test_head_default_options(DEFAULT_BLOCK_DURATION, false);
        opts.float_chunk_encoding.store(from.0 as u32, Ordering::Relaxed);
        opts.enable_st_storage.store(true, Ordering::Relaxed);
        let h = new_test_head_with_options("enc-switch-st", compression::Type::None, opts);
        h.init(0).expect("init");

        // Phase 1: Start appending with the first encoding.
        append_range(&h, &lbls, 1, 5);

        // Verify the in-progress chunk.
        let (ms, created) = h.get_or_create(lbls.hash(), &lbls, false).expect("get or create");
        assert!(!created);
        drop(ms);
        let (_, enc, _) = head_chunk_encodings(&h, &lbls);
        assert_eq!(from, enc, "chunk must be {from:?} before encoding switch");

        // Simulate a config reload that switches the encoding.
        set_enc(&h, to);

        // With ST storage enabled, XOR and XOR2 are NOT compatible. The first append after the encoding switch
        // must cut the current chunk and start a new one.
        let mut app2 = h.appender(&ctx());
        app2.append(0, &lbls, 6, 6.0).expect("append");
        assert!(app2.commit().is_none());

        let (n, enc, prev) = head_chunk_encodings(&h, &lbls);
        assert_eq!(to, enc, "encoding switch with ST storage must cut chunk immediately");
        assert!(n >= 2, "the old chunk must exist as a previous chunk after the cut");
        assert_eq!(Some(from), prev, "the evicted chunk must retain its original encoding");
    }

    // without_st_storage_xor_to_xor2
    {
        let opts = new_test_head_default_options(DEFAULT_BLOCK_DURATION, false);
        opts.float_chunk_encoding.store(ENC_XOR.0 as u32, Ordering::Relaxed);
        opts.enable_st_storage.store(false, Ordering::Relaxed);
        let h = new_test_head_with_options("enc-switch-x2", compression::Type::None, opts);
        h.init(0).expect("init");

        append_range(&h, &lbls, 1, 5);

        // Switch to XOR2.
        set_enc(&h, ENC_XOR2);

        append_range(&h, &lbls, 6, 10);

        // The head chunk must still be XOR: encoding change does not cut chunks when ST storage is disabled.
        let (ms, created) = h.get_or_create(lbls.hash(), &lbls, false).expect("get or create");
        assert!(!created);
        drop(ms);
        let (n, enc, _) = head_chunk_encodings(&h, &lbls);
        assert_eq!(
            1, n,
            "chunk must not have been cut; only one chunk must exist after encoding switch without ST storage"
        );
        assert_eq!(ENC_XOR, enc, "in-progress chunk must keep XOR encoding after encoding switch");
    }
}

#[test]
fn test_wal_replay_race_with_stale_series_compaction() {
    let mut opts = new_test_head_default_options(1000, false);
    // A small stripe size ensures many series share hash shards, increasing the likelihood that deleteSeriesByID
    // and getOrCreateWithOptionalID contend on the same shard during WAL replay.
    opts.stripe_size = 32;
    let mut head = new_test_head_with_options("wal-replay-race-stale", compression::Type::None, opts);
    head.init(0).expect("init");

    // Step 1: Create a batch of series and make them stale.
    const NUM_STALE_SERIES: usize = 500;
    let stale_lbls: Vec<Labels> =
        (0..NUM_STALE_SERIES).map(|i| lbl(&["__name__", "stale_metric", "i", &i.to_string()])).collect();
    for (i, l) in stale_lbls.iter().enumerate() {
        append_float(&head, l, 100, i as f64);
    }
    for l in &stale_lbls {
        append_float(&head, l, 200, stale_nan());
    }
    assert_eq!(NUM_STALE_SERIES as u64, head.num_stale_series());

    // Step 2: Truncate stale series. This removes them from the Head and writes tombstone records (with
    // Mint=MinInt64, Maxt=MaxInt64) to the WAL.
    let stale_refs: Vec<SeriesRef> =
        stale_lbls.iter().map(|l| head.series.get_by_hash(l.hash(), l).expect("series").r#ref).collect();
    head.truncate_stale_series(&stale_refs, 300, &head.snapshot_fingerprints(&stale_refs, u64::MAX))
        .expect("truncate stale series");
    assert_eq!(0, head.num_stale_series());
    assert_eq!(0, head.num_series());

    // Step 3: Add new series AFTER the truncation.
    const NUM_NEW_SERIES: usize = 500;
    for i in 0..NUM_NEW_SERIES {
        append_float(&head, &lbl(&["__name__", "new_metric", "i", &i.to_string()]), 300, i as f64);
    }
    assert_eq!(NUM_NEW_SERIES as u64, head.num_series());

    // Step 4: Close and re-open the Head to trigger WAL replay.
    reopen_same(&mut head);
    head.init(0).expect("init"); // Should not cause a race here.

    assert_eq!(0, head.num_stale_series());
    assert_eq!(NUM_NEW_SERIES as u64, head.num_series());
    head.close().expect("close");
}

#[test]
fn test_head_read_wal_tombstones_advance_last_series_id() {
    let cases = [
        ("interval tombstone", Interval { mint: 0, maxt: 100 }),
        ("full-range tombstone", Interval { mint: i64::MIN, maxt: i64::MAX }),
    ];
    for (name, iv) in cases {
        let entries = vec![
            Rec::Series(vec![RefSeries { r#ref: 10, labels: lbl(&["a", "1"]) }]),
            Rec::Samples(vec![RefSample { r#ref: 10, st: 0, t: 100, v: 1.0 }]),
            Rec::Tombstones(vec![Stone { r#ref: 42, intervals: vec![iv] }]),
        ];
        let head = new_test_head("tombstones-last-series-id", 1000, compression::Type::None, false);
        populate_test_wl(head.wal.as_ref().unwrap(), &entries, false);

        head.init(i64::MIN).expect("init");

        assert_eq!(42, head.last_series_id(), "{name}");
        head.close().expect("close");
    }
}

#[test]
fn test_stripe_series_unlink_hash() {
    let head = new_test_head("unlink-hash", 1000, compression::Type::None, false);

    let lbls = lbl(&["a", "1"]);
    let (s1, created) = head.get_or_create_with_optional_id(1, lbls.hash(), &lbls, false).expect("create");
    assert!(created);

    head.series.unlink_hash(lbls.hash(), s1.r#ref);

    // Gone from the hash index, still resolvable by ref.
    assert!(head.series.get_by_hash(lbls.hash(), &lbls).is_none());
    assert!(Arc::ptr_eq(&s1, &head.series.get_by_id(s1.r#ref).expect("by id")));

    // A series with the same labels isn't aliased onto s1.
    let (s2, created) = head.get_or_create_with_optional_id(2, lbls.hash(), &lbls, false).expect("create");
    assert!(created);
    assert_eq!(2, s2.r#ref);
    head.close().expect("close");
}

#[test]
fn test_head_wal_replay_full_range_tombstone_deletes_duplicate_ref() {
    let lbls = lbl(&["a", "1"]);
    let full = Interval { mint: i64::MIN, maxt: i64::MAX };
    let entries = vec![
        Rec::Series(vec![
            RefSeries { r#ref: 1, labels: lbls.clone() },
            // Duplicate record for the same series under a different ref; replay maps ref 2 onto ref 1.
            RefSeries { r#ref: 2, labels: lbls.clone() },
        ]),
        Rec::Samples(vec![RefSample { r#ref: 2, st: 0, t: 100, v: 1.0 }]),
        Rec::Tombstones(vec![Stone { r#ref: 2, intervals: vec![full] }]),
    ];

    let head = new_test_head("full-range-tombstone-dup", 1000, compression::Type::None, false);
    populate_test_wl(head.wal.as_ref().unwrap(), &entries, false);

    head.init(i64::MIN).expect("init");

    assert_eq!(0, head.num_series());
    head.close().expect("close");
}

#[test]
fn test_head_wal_checkpoint_full_range_tombstones() {
    let lbls1 = lbl(&["a", "1"]);
    let lbls2 = lbl(&["a", "2"]);
    let full_range = vec![Interval { mint: i64::MIN, maxt: i64::MAX }];
    let rs = |r: u64, t: i64, v: f64| RefSample { r#ref: r, st: 0, t, v };
    let wal_entries = vec![
        Rec::Series(vec![
            RefSeries { r#ref: 1, labels: lbls1.clone() },
            RefSeries { r#ref: 2, labels: lbls2.clone() },
        ]),
        Rec::Samples(vec![rs(1, 100, 1.0), rs(1, 500, 2.0), rs(2, 100, 3.0), rs(2, 200, 4.0)]),
        Rec::Tombstones(vec![
            Stone { r#ref: 1, intervals: full_range.clone() },
            Stone { r#ref: 2, intervals: full_range.clone() },
        ]),
    ];

    let head = new_test_head("wal-checkpoint-full-range", 1000, compression::Type::None, false);
    let w = head.wal.clone().unwrap();
    populate_test_wl(&w, &wal_entries, false);
    let (first, _) = wlog::segments(w.dir()).expect("segments");

    head.init(i64::MIN).expect("init");

    // Replay applied the tombstones: both series are deleted, and each ref has a WAL expiry at its max sample
    // time.
    assert_eq!(0, head.num_series());
    assert_eq!(Some(500), head.get_wal_expiry(1));
    assert_eq!(Some(200), head.get_wal_expiry(2));

    // Checkpoint with mint=250, between the two expiries. Each truncation creates a new segment, so attempt
    // truncations until a checkpoint is created.
    loop {
        head.last_wal_truncation_time.store(0, Ordering::SeqCst); // Reset so that it's always time to truncate the WAL.
        head.truncate_wal(250).expect("truncate wal");
        let (f, _) = wlog::segments(w.dir()).expect("segments");
        if f > first {
            break;
        }
    }

    // The WAL still holds a sample for ref 1 (t=500 >= mint), so its series record must be kept for the sample to
    // resolve on replay, and its tombstone with it so the replayed series is deleted again. Ref 2's samples all
    // age out, and its series record and tombstone are dropped with them.
    let expected = vec![
        Rec::Series(vec![RefSeries { r#ref: 1, labels: lbls1.clone() }]),
        Rec::Samples(vec![rs(1, 500, 2.0)]),
        Rec::Tombstones(vec![Stone { r#ref: 1, intervals: full_range.clone() }]),
    ];
    let (cp_dir, _) = wlog::last_checkpoint(w.dir()).expect("last checkpoint");
    let mut recs = read_test_wal(&cp_dir);
    recs.extend(read_test_wal(w.dir()));
    assert_eq!(expected, recs);

    // WAL expiries are cleaned up alongside the records they retain.
    assert_eq!(Some(500), head.get_wal_expiry(1), "ref 1 expiry must be retained (500 >= mint 250)");
    assert_eq!(None, head.get_wal_expiry(2), "ref 2 expiry must be cleared (200 < mint 250)");
    head.close().expect("close");
}

/// Parses `series_state.json` (Go `json.Unmarshal` into `SeriesLifecycleState`).
fn parse_series_state(b: &[u8]) -> Option<SeriesLifecycleState> {
    let j = json::Json::parse_bytes(b).ok()?;
    Some(SeriesLifecycleState {
        last_series_id: j.get("last_series_id").and_then(|v| v.as_i64()).unwrap_or(0) as u64,
        last_wal_segment: j.get("last_wal_segment").and_then(|v| v.as_i64()).unwrap_or(0),
        clean_shutdown: j.get("clean_shutdown").and_then(|v| v.as_bool()).unwrap_or(false),
    })
}

#[test]
fn test_head_fast_startup_state_file() {
    let mut opts = new_test_head_default_options(1000, false);
    // Enable the fast startup feature.
    opts.enable_fast_startup = true;

    let head = new_test_head_with_options("fast-startup-state", compression::Type::None, opts);
    head.init(0).expect("init");

    // Add a single sample to the Head.
    append_float(&head, &lbl(&["fast", "startup"]), 100, 1.0);

    let state_file_path = head.wal.as_ref().unwrap().dir().join("series_state.json");

    // Poll for the ticker (1s interval) to write the series state file rather than waiting a fixed amount of time.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut state;
    loop {
        if let Ok(b) = std::fs::read(&state_file_path)
            && let Some(s) = parse_series_state(&b)
        {
            state = s;
            if state.last_series_id == 1 {
                break;
            }
        }
        assert!(Instant::now() < deadline, "series_state.json should be written by the ticker");
        std::thread::sleep(Duration::from_millis(20));
    }

    // The ticker should write an unclean state.
    assert!(!state.clean_shutdown, "ticker should write CleanShutdown: false");
    assert_eq!(1, state.last_series_id, "LastSeriesID should be 1 after adding our sample");
    assert_eq!(0, state.last_wal_segment, "LastWALSegment should be 0 on a fresh WAL");

    // Perform a clean shutdown.
    head.close().expect("close");

    let b = std::fs::read(&state_file_path).expect("series_state.json should still exist after Close()");
    let state = parse_series_state(&b).expect("parse");

    // Calling head.Close() should put us in the clean state.
    assert!(state.clean_shutdown, "Close() should write CleanShutdown: true");
    assert_eq!(1, state.last_series_id, "LastSeriesID should remain 1");
    assert_eq!(0, state.last_wal_segment, "LastWALSegment should remain 0");
}

#[test]
fn test_head_read_series_state_file() {
    let opts = new_test_head_default_options(1000, false);
    let head = new_test_head_with_options("read-series-state", compression::Type::None, opts);

    // Fresh boot case.
    // Should return an os.ErrNotExist error (Go also returns a zero valued state).
    let err = head.read_series_state_file().expect_err("reading non-existent state file should return an error");
    // adapted: Go `os.IsNotExist(err)`; the Rust error carries the io::Error or its Go text.
    let not_exist = err.downcast_ref::<std::io::Error>().is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound)
        || err.to_string().ends_with("no such file or directory");
    assert!(not_exist, "error should be of type os.ErrNotExist: {err}");

    // Valid file case.
    let expected_state = SeriesLifecycleState {
        last_series_id: 42000,
        last_wal_segment: 5,
        clean_shutdown: true,
    };
    // Go json.Marshal of SeriesLifecycleState.
    let b = r#"{"last_series_id":42000,"last_wal_segment":5,"clean_shutdown":true}"#;
    std::fs::write(head.wal.as_ref().unwrap().dir().join("series_state.json"), b).expect("write");

    let state = head.read_series_state_file().expect("reading valid state file should not error");
    assert_eq!(expected_state, state, "read state should match written state");
}

#[test]
fn test_head_find_last_series_id() {
    let opts = new_test_head_default_options(1000, false);
    let head = new_test_head_with_options("find-last-series-id", compression::Type::None, opts);
    let w = head.wal.clone().unwrap();

    // Write Series A to first segment.
    append_float(&head, &lbl(&["metric", "A"]), 100, 1.0);

    // Force the WAL to cut a new segment file (second segment).
    w.next_segment().expect("next segment");

    // Write new sample for series A to second segment.
    append_float(&head, &lbl(&["metric", "A"]), 200, 2.0);

    // Get the current max segment number.
    let (first, last) = wlog::segments(w.dir()).expect("segments");

    let mut mock_state = SeriesLifecycleState {
        last_series_id: 1,
        last_wal_segment: first,
        clean_shutdown: false,
    };

    // Should return 1 as there is only 1 series created so far.
    let id = head.find_last_series_id(&mock_state, last).expect("find");
    assert_eq!(1, id, "Should find ID 1 as no new series were created in segment 2");

    // Write Series B to the second segment
    append_float(&head, &lbl(&["metric", "B"]), 300, 3.0);

    // Scanning both files should now return 2
    let id = head.find_last_series_id(&mock_state, last).expect("find");
    assert_eq!(2, id, "Should find ID 2 after new series was created in segment 2");

    // Simulate state file knowing about latest segment.
    mock_state.last_wal_segment = last;

    // Should return 2 as it should scan the last file and find series B.
    let id = head.find_last_series_id(&mock_state, last).expect("find");
    assert_eq!(2, id, "Should find ID 2 even when state file's last segment is the newest segment");
}

#[test]
fn test_ooo_restart_resets_first_ooo_chunk_id() {
    for (name, scenario) in common::sample_type_scenarios() {
        // Go testOOORestartResetsFirstOOOChunkID.
        let opts = HeadOptions {
            chunk_range: 1000,
            ..HeadOptions::default()
        };
        opts.out_of_order_cap_max.store(30, Ordering::Relaxed);
        opts.out_of_order_time_window.store(1000 * 60 * 1000, Ordering::Relaxed);
        let h = head_with_wbl(TempDir::named("ooo-restart-first-id"), opts, compression::Type::Snappy);
        h.init(0).expect("init");

        let l = lbl(&["foo", "bar"]);
        let append_sample = |h: &Head, mins: i64| {
            let mut app = h.appender(&ctx());
            let (r, _) = scenario.append(&mut *app, &l, mins * 60 * 1000, mins);
            r.unwrap_or_else(|e| panic!("{name}: {e}"));
            assert!(app.commit().is_none(), "{name}");
        };

        // In-order sample.
        append_sample(&h, 200);

        // OOO samples: 92 samples to create 3 mmapped chunks (cap=30).
        for mins in 100..=191 {
            append_sample(&h, mins);
        }

        let (ms, ok) = h.get_or_create(l.hash(), &l, false).expect("get or create");
        assert!(!ok);
        let exp_mmap_count = {
            let mut g = ms.lock();
            let ooo = g.ooo_mut().expect("ooo fields");
            assert!(!ooo.ooo_mmapped_chunks.is_empty(), "{name}");
            let n = ooo.ooo_mmapped_chunks.len();
            // Seed firstOOOChunkID to a large value.
            ooo.first_ooo_chunk_id = OOO_CHUNK_ID_MASK - 100;
            n
        };
        drop(ms);

        // Reopen.
        let h = reopen_with_wbl(h, compression::Type::Snappy);
        h.init(0).expect("init");

        let (ms, ok) = h.get_or_create(l.hash(), &l, false).expect("get or create");
        assert!(!ok);
        let g = ms.lock();
        let ooo = g.ooo().expect("ooo fields");

        // firstOOOChunkID resets to 0 after replay.
        assert_eq!(0, ooo.first_ooo_chunk_id, "{name}");

        // Verify mmapped chunks are still accessible.
        assert_eq!(exp_mmap_count, ooo.ooo_mmapped_chunks.len(), "{name}");
        for m in &ooo.ooo_mmapped_chunks {
            let chk = h.chunk_disk_mapper.chunk(m.r#ref).expect("chunk");
            assert_eq!(m.num_samples as usize, chk.num_samples(), "{name}");
        }
        drop(g);
        h.close().expect("close");
    }
}

/// The sum of the per-stripe mmap-ready counters (Go `mmapReadyTotal`).
fn mmap_ready_total(h: &Head) -> i32 {
    (0..h.series.size).map(|i| h.series.test_mmap_ready(i)).sum()
}

/// The number of series with at least two head chunks (Go `countReady`).
fn count_ready(h: &Head) -> i32 {
    let mut n = 0;
    h.series.for_each(|s| {
        if s.lock().head_chunk_count() >= 2 {
            n += 1;
        }
    });
    n
}

/// Go `getCount`: the head chunk count of the series.
fn head_chunk_count_of(h: &Head, lbls: &Labels) -> u32 {
    let s = h.series.get_by_hash(lbls.hash(), lbls).unwrap_or_else(|| panic!("series {lbls} not found"));
    s.lock().head_chunk_count()
}

/// The chunks of the head chunks of a series (to compare identity, Go `s.headChunks` pointer).
fn head_chunk_arcs(s: &MemSeries) -> Vec<Arc<dyn chunkenc::Chunk>> {
    s.lock().head_chunks.iter().map(|c| c.chunk.clone()).collect()
}

fn same_chunks(a: &[Arc<dyn chunkenc::Chunk>], b: &[Arc<dyn chunkenc::Chunk>]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| Arc::ptr_eq(x, y))
}

#[test]
fn test_head_mmap_head_chunks() {
    // Interval such that one chunk's worth of samples (DefaultSamplesPerChunk) spans DefaultBlockDuration/4.
    let interval = DEFAULT_BLOCK_DURATION / (4 * DEFAULT_SAMPLES_PER_CHUNK as i64);
    // Enough samples to cross the nextAt boundary multiple times, producing 3 head chunks per series.
    let chunk_cut_iterations = 2 * DEFAULT_SAMPLES_PER_CHUNK + 10;

    // basic
    {
        let h = new_test_head("mmap-basic", DEFAULT_BLOCK_DURATION, compression::Type::None, false);
        h.init(0).expect("init");

        let lbls_a = lbl(&["__name__", "seriesA"]);
        let lbls_b = lbl(&["__name__", "seriesB"]);
        let lbls_c = lbl(&["__name__", "seriesC"]);

        let mut ts = 0i64;

        // First chunk creation should set headChunkCount to 1.
        let mut app = h.appender(&ctx());
        app.append(0, &lbls_a, ts, 1.0).expect("append");
        assert!(app.commit().is_none());
        ts += interval;

        assert_eq!(1, head_chunk_count_of(&h, &lbls_a), "first chunk should set headChunkCount to 1");
        assert_eq!(0, count_ready(&h), "series with only a first chunk should not be ready");

        // Appending enough samples to trigger chunk cuts should update headChunkCount.
        let (mut ref_b, mut ref_c): (SeriesRef, SeriesRef) = (0, 0);
        let mut app = h.appender(&ctx());
        for _ in 0..chunk_cut_iterations {
            ref_b = app.append(ref_b, &lbls_b, ts, ts as f64).expect("append");
            ref_c = app.append(ref_c, &lbls_c, ts, ts as f64).expect("append");
            ts += interval;
        }
        assert!(app.commit().is_none());

        assert_eq!(2, count_ready(&h), "expected both series to be marked ready");
        // With ~2*DefaultSamplesPerChunk samples, we expect 3 head chunks.
        assert_eq!(3, head_chunk_count_of(&h, &lbls_b), "series B headChunkCount should reflect actual head chunk count");
        assert_eq!(3, head_chunk_count_of(&h, &lbls_c), "series C headChunkCount should reflect actual head chunk count");

        // mmapHeadChunks should reset headChunkCount to 1 (one head chunk remains).
        h.mmap_head_chunks();

        for lbls in [&lbls_b, &lbls_c] {
            let s = h.series.get_by_hash(lbls.hash(), lbls).unwrap_or_else(|| panic!("series {lbls} not found"));
            let g = s.lock();
            assert!(!g.head_chunks.is_empty(), "series {lbls} should have head chunks");
            assert_eq!(1, g.head_chunks.len(), "series {lbls} should not have prev mmapped");
            assert!(!g.mmapped_chunks.is_empty(), "series {lbls} should have mmapped chunks");
        }

        assert_eq!(1, head_chunk_count_of(&h, &lbls_b), "headChunkCount should be 1 after mmap");
        assert_eq!(1, head_chunk_count_of(&h, &lbls_c), "headChunkCount should be 1 after mmap");
        assert_eq!(0, count_ready(&h), "ready set should be empty after mmapHeadChunks");

        // A second call should be a no-op.
        let before = h.metrics.mmap_chunks_total.get();
        h.mmap_head_chunks();
        let after = h.metrics.mmap_chunks_total.get();
        assert_eq!(before, after, "second call should mmap 0 chunks");

        // Only newly ready series should be processed.
        let mut app = h.appender(&ctx());
        for _ in 0..chunk_cut_iterations {
            ref_b = app.append(ref_b, &lbls_b, ts, ts as f64).expect("append");
            ts += interval;
        }
        assert!(app.commit().is_none());

        assert_eq!(1, count_ready(&h), "only series B should be ready");
        assert_eq!(3, head_chunk_count_of(&h, &lbls_b), "series B headChunkCount should reflect new chunks");
        assert_eq!(1, head_chunk_count_of(&h, &lbls_c), "series C headChunkCount should be unchanged");

        let before = h.metrics.mmap_chunks_total.get();
        h.mmap_head_chunks();
        let after = h.metrics.mmap_chunks_total.get();
        assert!(after > before, "third call should mmap chunks from series B");
        assert_eq!(1, head_chunk_count_of(&h, &lbls_b), "series B headChunkCount should be 1 after mmap");
    }

    // panic releases locks
    //
    // Regression test for https://github.com/prometheus/prometheus/issues/17941: if mmapChunks panics (via
    // handleChunkWriteError) while mmapHeadChunks holds the stripe and series locks, those locks must still be
    // released.
    {
        let h = new_test_head("mmap-panic", DEFAULT_BLOCK_DURATION, compression::Type::None, false);
        h.init(0).expect("init");

        let lbls = lbl(&["__name__", "series"]);
        let mut ts = 0i64;
        let mut r: SeriesRef = 0;
        let mut app = h.appender(&ctx());
        for _ in 0..chunk_cut_iterations {
            r = app.append(r, &lbls, ts, ts as f64).expect("append");
            ts += interval;
        }
        assert!(app.commit().is_none());

        let s = h.series.get_by_hash(lbls.hash(), &lbls).expect("series");
        assert!(s.lock().head_chunk_count() >= 2, "need >=2 head chunks pending mmap");

        // Break the chunk write path in an OS agnostic way: dropping a segment file with a far higher sequence
        // number into the head chunks directory makes the next cut file's sequence differ from the one the chunk
        // reference was handed out for, which fails the write.
        let rogue = tsdb::head::mmapped_chunks_dir(&h.opts.chunk_dir_root).join("000100");
        std::fs::write(&rogue, b"").expect("write rogue file");

        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| h.mmap_head_chunks()));
        assert!(res.is_err(), "mmapHeadChunks must panic");

        // The locks held while mmapChunks panicked must have been released.
        assert!(s.test_try_lock(), "series lock leaked after panic");
        for i in 0..h.series.size {
            assert!(h.series.test_try_lock_shard(i), "stripe lock {i} leaked after panic");
        }

        // Go's cleanup closes the mapper first, so that Head.Close() -> mmapHeadChunks() does not panic again.
        let _ = h.chunk_disk_mapper.close();
        let _ = h.head.close();
    }

    // mmapHeadChunksInStripe releases the stripe lock before locking a series
    //
    // Regression test for https://github.com/prometheus/prometheus/issues/19445.
    {
        let h = new_test_head("mmap-stripe-lock", DEFAULT_BLOCK_DURATION, compression::Type::None, false);
        h.init(0).expect("init");

        let lbls = lbl(&["__name__", "series"]);
        let mut app = h.appender(&ctx());
        let (mut r, mut ts): (SeriesRef, i64) = (0, 0);
        for _ in 0..DEFAULT_SAMPLES_PER_CHUNK + 10 {
            r = app.append(r, &lbls, ts, ts as f64).expect("append");
            ts += interval;
        }
        assert!(app.commit().is_none());

        let series = h.series.get_by_id(r).expect("series");
        assert!(series.lock().head_chunk_count() >= 2, "series must be mmap-ready");
        let stripe = h.series.ref_stripe(r);

        // gcSeries: hold the series lock.
        let guard = series.lock();
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        std::thread::scope(|sc| {
            let head = &h;
            sc.spawn(move || {
                head.mmap_head_chunks_in_stripe(stripe);
                let _ = tx.send(());
            });
            if rx.recv_timeout(Duration::from_secs(1)).is_ok() {
                panic!("mmapHeadChunksInStripe returned without ever blocking on the series lock");
            }

            // If mmapHeadChunksInStripe still holds the stripe's lock, this try-lock (write) fails.
            let got_lock = h.series.test_try_lock_shard(stripe);

            // Unblock mmapHeadChunksInStripe and wait for it to finish before asserting.
            drop(guard);
            rx.recv_timeout(Duration::from_secs(5))
                .expect("mmapHeadChunksInStripe never completed after the series lock was released");

            assert!(
                got_lock,
                "mmapHeadChunksInStripe held the stripe's RLock while blocked on a series lock; this deadlocks against a concurrent gcSeries"
            );
        });
    }

    // gcSeries preserves head chunks for active readers after WAL replay
    {
        let dir = TempDir::new();
        let db = tsdb::db::open(dir.path(), None, Some(tsdb::db::default_options()), None).expect("open");
        db.disable_compactions();

        let mut app = db.appender(&ctx());
        let r = app.append(0, &lbl(&["__name__", "series"]), 100, 1.0).expect("append");
        assert!(app.commit().is_none());
        db.close().expect("close");
        drop(db);

        let db = tsdb::db::open(dir.path(), None, Some(tsdb::db::default_options()), None).expect("open");
        db.disable_compactions();
        let h = db.head().clone();
        let series = h.series.get_by_id(r).expect("series");
        {
            let g = series.lock();
            let txs = g.txs.as_ref().expect("isolation is enabled");
            assert_eq!(0, txs.tx_id_count);
            assert_eq!(0, txs.tx_ids.len(), "WAL replay must leave the transaction ring empty");
        }

        let rh = tsdb::head::RangeHead::new(h.clone(), 100, 100);
        let ir = tsdb::BlockReader::index(&rh).expect("index");
        let cr = tsdb::BlockReader::chunks(&rh).expect("chunks");
        let mut builder = ScratchBuilder::new(0);
        let mut metas: Vec<Meta> = Vec::new();
        ir.series(r, &mut builder, &mut metas).expect("series");
        assert_eq!(1, metas.len());
        let chk = match cr.chunk_or_iterable(&metas[0]).expect("chunk") {
            ChunkOrIterable::Chunk(c) => c,
            ChunkOrIterable::Iterable(_) => panic!("expected a chunk"),
        };

        // Retire the series directly to test the saved chunk independently of reader waiting.
        let deleted = h.gc_series(&[r], 100, |_, _| true);
        assert!(deleted.contains(&r));
        assert_eq!(0, h.num_series());
        assert!(h.series.get_by_id(r).is_none());

        let mut it = chk.iterator(None);
        assert_eq!(ValueType::Float, it.next());
        assert_eq!((100, 1.0), it.at());
        assert_eq!(ValueType::None, it.next());
        assert!(it.err().is_none());
        drop(it);

        ir.close().expect("close index");
        cr.close().expect("close chunks");
        db.close().expect("close");
    }

    // gcSeries preserves head chunks and a stale mmap is a no-op
    {
        let h = new_test_head("mmap-gc-series", DEFAULT_BLOCK_DURATION, compression::Type::None, false);
        h.init(0).expect("init");

        let lbls = lbl(&["__name__", "series"]);
        let mut app = h.appender(&ctx());
        let (mut r, mut ts): (SeriesRef, i64) = (0, 0);
        for _ in 0..DEFAULT_SAMPLES_PER_CHUNK + 10 {
            r = app.append(r, &lbls, ts, ts as f64).expect("append");
            ts += interval;
        }
        assert!(app.commit().is_none());

        let series = h.series.get_by_id(r).expect("series");
        let head_chunks_before = head_chunk_arcs(&series);
        let head_chunk_count_before = series.lock().head_chunk_count();
        let mmapped_chunks_before = series.lock().mmapped_chunks.len();
        assert!(head_chunk_count_before >= 2, "series must be mmap-ready");
        let stripe = h.series.ref_stripe(series.r#ref);
        assert_eq!(1, h.series.test_mmap_ready(stripe));

        let deleted = h.gc_series(&[r], i64::MAX, |_, _| true);
        assert!(deleted.contains(&r));
        assert!(h.series.get_by_id(r).is_none());
        assert!(series.lock().is_gced());
        assert!(same_chunks(&head_chunks_before, &head_chunk_arcs(&series)));
        assert_eq!(head_chunk_count_before, series.lock().head_chunk_count());
        assert_eq!(0, h.series.test_mmap_ready(stripe));

        assert_eq!(0, h.mmap_series_chunks(&series), "mmapSeriesChunks must be a no-op on an evicted series");
        assert!(same_chunks(&head_chunks_before, &head_chunk_arcs(&series)));
        assert_eq!(head_chunk_count_before, series.lock().head_chunk_count());
        assert_eq!(mmapped_chunks_before, series.lock().mmapped_chunks.len());
        assert_eq!(0, h.series.test_mmap_ready(stripe));
    }

    // ooo does not inflate count
    {
        let h = new_test_head("mmap-ooo-count", DEFAULT_BLOCK_DURATION, compression::Type::None, true);
        h.init(0).expect("init");

        let lbls = lbl(&["__name__", "test"]);
        let mut ts = 0i64;

        // Create enough in-order samples to get headChunkCount >= 2.
        let mut r: SeriesRef = 0;
        let mut app = h.appender(&ctx());
        for _ in 0..chunk_cut_iterations {
            r = app.append(r, &lbls, ts, ts as f64).expect("append");
            ts += interval;
        }
        assert!(app.commit().is_none());

        let s = h.series.get_by_hash(lbls.hash(), &lbls).expect("series");
        let count_before = s.lock().head_chunk_count();
        assert!(count_before >= 2, "need multiple head chunks for this test");

        // Append an OOO sample that creates an OOO chunk.
        let ooo_ts = ts - 5 * 60 * 1000; // Within the 10m OOO window.
        let mut app = h.appender(&ctx());
        app.append(0, &lbls, ooo_ts, 999.0).expect("append");
        assert!(app.commit().is_none());

        // headChunkCount must not change from an OOO insert.
        assert_eq!(count_before, s.lock().head_chunk_count(), "OOO insert should not inflate headChunkCount");
    }

    // ooo insert does not increment mmap ready
    type AppendOne = fn(&mut dyn storage::Appender, &Labels, i64) -> Result<SeriesRef, storage::Error>;
    let ooo_cases: Vec<(&str, AppendOne)> = vec![
        ("floats", |app, lbls, ts| app.append(0, lbls, ts, ts as f64)),
        ("histograms", |app, lbls, ts| {
            app.append_histogram(0, lbls, ts, Some(&tu::generate_test_histograms(1)[0]), None)
        }),
        ("float histograms", |app, lbls, ts| {
            app.append_histogram(0, lbls, ts, None, Some(&tu::generate_test_float_histograms(1)[0]))
        }),
    ];
    for (name, append_sample) in &ooo_cases {
        let h = new_test_head("mmap-ooo-ready", DEFAULT_BLOCK_DURATION, compression::Type::None, true);
        h.init(0).expect("init");

        // Create a series with one in-order sample.
        let lbls = lbl(&["__name__", "test"]);
        let mut ts = 0i64;
        let mut app = h.appender(&ctx());
        append_sample(&mut *app, &lbls, ts).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(app.commit().is_none());
        ts += interval;

        let s = h.series.get_by_hash(lbls.hash(), &lbls).expect("series");

        // Force the exact state required to trigger the bug: series at headChunkCount == 2 (mmap-ready) with
        // mmapReady already incremented.
        // adapted: the head chunk count is the length of the head chunk vector, so a second head chunk (a copy
        // of the first) is pushed instead of storing 2 into Go's counter.
        {
            let mut g = s.lock();
            let c = g.head_chunks[0].clone();
            g.head_chunks.push(c);
            assert_eq!(2, g.head_chunk_count());
        }
        h.series.inc_mmap_ready(s.r#ref);
        assert_eq!(1, mmap_ready_total(&h), "{name}");

        // Append an OOO sample. This creates an OOO chunk (not a regular head chunk), so mmapReady must stay
        // at 1.
        let ooo_ts = ts - 5 * 60 * 1000;
        let mut app = h.appender(&ctx());
        append_sample(&mut *app, &lbls, ooo_ts).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(app.commit().is_none());

        assert_eq!(1, mmap_ready_total(&h), "OOO chunk creation must not increment mmapReady ({name})");
    }

    // wal replay does not leak mmap ready
    {
        // Pre-generate varied histograms; reusing the same histogram every iteration compresses too well to
        // trigger chunk cuts.
        let histograms = tu::generate_test_histograms(chunk_cut_iterations);
        let float_histograms = tu::generate_test_float_histograms(chunk_cut_iterations);
        for kind in ["floats", "histograms", "float histograms"] {
            let h = new_test_head("mmap-wal-replay-ready", DEFAULT_BLOCK_DURATION, compression::Type::None, false);
            h.init(0).expect("init");

            // Build a series naturally to headChunkCount >= 2 via the normal appender path. This sets
            // mmapReady to 1.
            let lbls = lbl(&["__name__", "test"]);
            let mut ts = 0i64;
            let mut app = h.appender(&ctx());
            for i in 0..chunk_cut_iterations {
                match kind {
                    "floats" => app.append(0, &lbls, ts, ts as f64),
                    "histograms" => app.append_histogram(0, &lbls, ts, Some(&histograms[i]), None),
                    _ => app.append_histogram(0, &lbls, ts, None, Some(&float_histograms[i])),
                }
                .unwrap_or_else(|e| panic!("{kind}: {e}"));
                ts += interval;
            }
            assert!(app.commit().is_none());

            let s = h.series.get_by_hash(lbls.hash(), &lbls).expect("series");
            assert!(s.lock().head_chunk_count() >= 2, "{kind}");
            assert_eq!(1, mmap_ready_total(&h), "{kind}");

            // Invoke the WAL-replay-style helper with a sample timestamped past the next chunk boundary to force
            // a chunk cut.
            let (_, chunk_created) = {
                let mut g = s.lock();
                let t = ts + h.chunk_range();
                let o = tsdb::head_append::ChunkOpts {
                    chunk_disk_mapper: Some(&h.chunk_disk_mapper),
                    chunk_range: h.chunk_range(),
                    samples_per_chunk: h.opts.samples_per_chunk,
                    ..Default::default()
                };
                h.test_append_chunk_and_mmap(&s, &mut g, |g| match kind {
                    "floats" => g.append(0, t, 0.0, 0, &o, &s.lset, s.r#ref),
                    "histograms" => g.append_histogram(0, t, Arc::new(histograms[0].clone()), 0, &o, &s.lset, s.r#ref),
                    _ => g.append_float_histogram(0, t, Arc::new(float_histograms[0].clone()), 0, &o, &s.lset, s.r#ref),
                })
            };
            assert!(chunk_created, "test needs a chunk cut to be meaningful ({kind})");

            // After the chunk cut + mmap, headChunkCount == 1. The series is no longer mmap-ready, so mmapReady
            // must be 0.
            assert_eq!(0, mmap_ready_total(&h), "WAL-replay-style chunk cut must decrement mmapReady when prev >= 2 ({kind})");
        }
    }

    // counter consistency
    {
        let h = new_test_head("mmap-counter-consistency", DEFAULT_BLOCK_DURATION, compression::Type::None, false);
        h.init(0).expect("init");

        let require_counter_consistent = |msg: &str| {
            let actual = count_ready(&h);
            let counter = mmap_ready_total(&h);
            assert_eq!(actual, counter, "{msg}: mmapReady counter ({counter}) != actual ready count ({actual})");
        };

        let lbls_a = lbl(&["__name__", "seriesA"]);
        let lbls_b = lbl(&["__name__", "seriesB"]);
        let lbls_c = lbl(&["__name__", "seriesC"]);
        let lbls_hist = lbl(&["__name__", "seriesHist"]);
        let lbls_fhist = lbl(&["__name__", "seriesFHist"]);

        // Phase 1: Append enough samples to create multiple head chunks.
        let mut ts = 0i64;
        let (mut ref_a, mut ref_b, mut ref_c, mut ref_hist, mut ref_fhist): (SeriesRef, SeriesRef, SeriesRef, SeriesRef, SeriesRef) =
            (0, 0, 0, 0, 0);
        let histograms = tu::generate_test_histograms(chunk_cut_iterations);
        let float_histograms = tu::generate_test_float_histograms(chunk_cut_iterations);
        let mut app = h.appender(&ctx());
        for i in 0..chunk_cut_iterations {
            ref_a = app.append(ref_a, &lbls_a, ts, ts as f64).expect("append");
            ref_b = app.append(ref_b, &lbls_b, ts, ts as f64).expect("append");
            ref_c = app.append(ref_c, &lbls_c, ts, ts as f64).expect("append");
            ref_hist = app.append_histogram(ref_hist, &lbls_hist, ts, Some(&histograms[i]), None).expect("append");
            ref_fhist = app
                .append_histogram(ref_fhist, &lbls_fhist, ts, None, Some(&float_histograms[i]))
                .expect("append");
            ts += interval;
        }
        assert!(app.commit().is_none());
        require_counter_consistent("after initial appends with chunk cuts");
        assert_eq!(5, mmap_ready_total(&h), "all five series should be ready");

        // Phase 2: mmapHeadChunks should bring counter to 0.
        h.mmap_head_chunks();
        require_counter_consistent("after mmapHeadChunks");
        assert_eq!(0, mmap_ready_total(&h), "counter should be 0 after mmap");

        // Phase 3: Make only seriesA ready again.
        let append_a = |ref_a: &mut SeriesRef, ts: &mut i64| {
            let mut app = h.appender(&ctx());
            for _ in 0..chunk_cut_iterations {
                *ref_a = app.append(*ref_a, &lbls_a, *ts, *ts as f64).expect("append");
                *ts += interval;
            }
            assert!(app.commit().is_none());
        };
        append_a(&mut ref_a, &mut ts);
        require_counter_consistent("after making seriesA ready again");
        assert_eq!(1, mmap_ready_total(&h), "only seriesA should be ready");

        // Mmap to bring seriesA back to headChunkCount == 1, then append again so the next GC can test the
        // counter decrement path.
        h.mmap_head_chunks();
        require_counter_consistent("after second mmap");
        assert_eq!(0, mmap_ready_total(&h));

        // Make seriesA ready a third time, then GC at a time that truncates the mmapped chunks but not the new
        // head chunks.
        append_a(&mut ref_a, &mut ts);
        require_counter_consistent("after third round of appends");
        assert_eq!(1, mmap_ready_total(&h));

        // Set minTime to current ts so GC truncates old chunks.
        h.min_time.store(ts, Ordering::SeqCst);
        h.gc();
        require_counter_consistent("after GC truncation");

        // Phase 4: Stale-series deletion via truncateStaleSeries (gcStaleSeries path).
        let mut app = h.appender(&ctx());
        for _ in 0..chunk_cut_iterations {
            ref_b = app.append(ref_b, &lbls_b, ts, ts as f64).expect("append");
            ts += interval;
        }
        assert!(app.commit().is_none());

        // Mark seriesB stale so truncateStaleSeries will remove it.
        let mut app = h.appender(&ctx());
        app.append(ref_b, &lbls_b, ts, stale_nan()).expect("append");
        assert!(app.commit().is_none());
        ts += interval;

        let s_b = h.series.get_by_hash(lbls_b.hash(), &lbls_b).expect("series");
        assert!(
            s_b.lock().head_chunk_count() >= 2,
            "seriesB must be mmap-ready at deletion time for this test to be meaningful"
        );
        require_counter_consistent("before stale series deletion");
        let ready_before = mmap_ready_total(&h);

        let stale_refs = vec![s_b.r#ref];
        h.truncate_stale_series(&stale_refs, ts, &h.snapshot_fingerprints(&stale_refs, u64::MAX))
            .expect("truncate stale series");
        require_counter_consistent("after truncateStaleSeries");
        assert!(mmap_ready_total(&h) < ready_before, "counter should have decreased after deleting a ready series");

        // Phase 5: Verify mmapHeadChunks does not decrement when mmapChunks returns 0.
        let lbls_d = lbl(&["__name__", "seriesD"]);
        let mut app = h.appender(&ctx());
        app.append(0, &lbls_d, ts, ts as f64).expect("append");
        assert!(app.commit().is_none());

        let s_d = h.series.get_by_hash(lbls_d.hash(), &lbls_d).expect("series");
        assert_eq!(1, s_d.lock().head_chunk_count());
        assert_eq!(1, s_d.lock().head_chunks.len(), "only one head chunk, prev must be nil");
        let stripe_d = h.series.ref_stripe(s_d.r#ref);

        // Simulate the race: set mmapReady to 1 (as the increment site would).
        // adapted: Go also stores 2 into the series' headChunkCount while it has one head chunk; in Rust the
        // count is the head chunk vector length, so only the stripe counter is inflated. mmapChunks still finds
        // nothing to m-map and returns 0.
        let before_d = h.series.test_mmap_ready(stripe_d);
        h.series.inc_mmap_ready(s_d.r#ref);

        h.mmap_head_chunks();

        // mmapChunks returned 0. With the n > 0 guard, the counter stays incremented.
        assert_eq!(
            before_d + 1,
            h.series.test_mmap_ready(stripe_d),
            "mmapHeadChunks must not decrement when mmapChunks returns 0"
        );

        // Clean up: restore the real state.
        h.series.dec_mmap_ready(s_d.r#ref);
        require_counter_consistent("final state");
    }
}

#[test]
fn test_query_ooo_head_during_truncate_across_wrap() {
    const MAX_T: i64 = 6000;

    let mut opts = tsdb::db::default_options();
    opts.out_of_order_time_window = MAX_T;
    opts.min_block_duration = MAX_T / 2;

    let db = common::new_test_db(vec![common::with_opts(opts)]);
    db.disable_compactions();

    let l = lbl(&["a", "b"]);
    let mut app = db.appender(&ctx());

    // In-order samples.
    let r = app.append(0, &l, 0, 0.0).expect("append");
    let mut i = 100;
    while i < MAX_T {
        app.append(r, &l, i, i as f64).expect("append");
        i += 100;
    }
    // OOO samples interleaved between in-order ones.
    let mut i = 50;
    while i < MAX_T {
        app.append(r, &l, i, i as f64).expect("append");
        i += 100;
    }
    assert!(app.commit().is_none());

    // Seed firstOOOChunkID near the wrap boundary.
    let ms = db.head().series.get_by_id(r).expect("series");
    {
        let mut g = ms.lock();
        let ooo = g.ooo_mut().expect("ooo fields");
        ooo.first_ooo_chunk_id = OOO_CHUNK_ID_MASK - 1;
    }

    let count = |what: &str| {
        let mut q = db.querier(0, MAX_T).expect("querier");
        let mut n = 0;
        {
            let mut ss = q.select(&ctx(), false, None, &[eq_matcher("a", "b")]);
            assert!(ss.next());
            let series = ss.at().clone();
            let mut it = series.iterator(None);
            while it.next() != ValueType::None {
                n += 1;
            }
            assert!(it.err().is_none());
        }
        assert!(n > 0, "{what}-truncation query should return data");
        assert!(q.close().is_none());
    };

    // Pre-truncation query.
    count("pre");

    // Compact, which triggers truncation and wraps firstOOOChunkID.
    db.compact(&ctx()).expect("compact");

    // Post-truncation query.
    count("post");
}
