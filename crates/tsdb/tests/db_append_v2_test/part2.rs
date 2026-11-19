//! Port of `tsdb/db_append_v2_test.go` lines 4076..end.
//!
//! Subtests run one after the other in one Rust test; each prints its name first.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use chunkenc::Chunk;
use model::histogram::{CounterResetHint, FloatHistogram, Histogram, Span};
use model::labels::{self, Labels};
use model::matcher::{MatchType, Matcher};
use storage::tsdbutil as tu;
use storage::{AOptions, AppenderV2, ChunkQuerier, Context, Error, Meta, Sample, SeriesRef};
use tsdb::compact::Compactor;
use tsdb::db::{ApplyConfig, Options, TsdbConfig, DB};
use tsdb::head::{mmapped_chunks_dir, RangeHead};
use tsdb::meta::{BlockMeta, Ulid};
use tsdb::BlockReader;
use tsdb::querier::new_block_querier;
use tsdb_format::record::{self, Decoder};
use tsdb_format::wlog::{self, compression, WL};

use crate::common::{
    new_test_db, query, rand_f64, sample_type_scenarios, with_dir, with_opts, with_rngs, SampleTypeScenario, TempDir, TestDb,
    CUSTOM_BUCKETS_FLOAT_HISTOGRAM, CUSTOM_BUCKETS_INT_HISTOGRAM, FLOAT_HISTOGRAM, INT_HISTOGRAM,
};

/// Go `time.Minute.Milliseconds()`.
const MINUTE: i64 = 60 * 1000;
/// Go `time.Hour.Milliseconds()`.
const HOUR: i64 = 60 * MINUTE;

// ---------------------------------------------------------------------------------------------------------------
// Helpers (testutil.go, db_test.go).

fn ctx() -> Context {
    Context::background()
}

fn ls(kv: &[&str]) -> Labels {
    labels::from_strings(kv)
}

fn re(name: &str, value: &str) -> Matcher {
    Matcher::must_new(MatchType::Regexp, name, value)
}

fn eq(name: &str, value: &str) -> Matcher {
    Matcher::must_new(MatchType::Equal, name, value)
}

fn minutes(m: i64) -> i64 {
    m * MINUTE
}

fn opts_default() -> Options {
    tsdb::db::default_options()
}

/// Go `rand.Intn(n)`.
fn rand_intn(n: i64) -> i64 {
    ((rand_f64() * n as f64) as i64).min(n - 1)
}

/// Go `scenario.appendFunc(storage.AppenderV2AsLimitedV1(app), ...)`: the V1 call through the V2 appender with
/// start timestamp 0 and default options (what `AppenderV2AsLimitedV1` does).
fn scenario_append(
    sc: &SampleTypeScenario,
    app: &mut dyn AppenderV2,
    lbls: &Labels,
    ts: i64,
    value: i64,
) -> (Result<SeriesRef, Error>, Sample) {
    let s = (sc.sample_func)(ts, value);
    let o = AOptions::default();
    let r = if let Some(h) = &s.h {
        app.append(0, lbls, 0, ts, 0.0, Some(h), None, &o)
    } else if let Some(fh) = &s.fh {
        app.append(0, lbls, 0, ts, 0.0, None, Some(fh), &o)
    } else {
        app.append(0, lbls, 0, ts, s.f, None, None, &o)
    };
    (r, s)
}

fn commit(app: &mut Box<dyn AppenderV2>) {
    if let Some(e) = app.commit() {
        panic!("commit: {e}");
    }
}

fn sample_type_name(s: &Sample) -> &'static str {
    if s.h.is_some() {
        "histogram"
    } else if s.fh.is_some() {
        "floathistogram"
    } else {
        "float"
    }
}

/// Go `requireEqualSamples` (testutil.go) with its options as flags.
fn require_equal_samples_opts(
    name: &str,
    expected: &[Sample],
    actual: &[Sample],
    ignore_counter_resets: bool,
    in_use_bucket_compare: bool,
) {
    assert_eq!(expected.len(), actual.len(), "Length not equal to expected for {name}");
    for (i, (e, a)) in expected.iter().zip(actual).enumerate() {
        assert_eq!(e.t, a.t, "Different timestamps for {name}[{i}]");
        assert_eq!(sample_type_name(e), sample_type_name(a), "Different types for {name}[{i}] at ts {}", e.t);
        if let Some(eh) = &e.h {
            let mut eh = (**eh).clone();
            let mut ah = (**a.h.as_ref().unwrap()).clone();
            if ignore_counter_resets && eh.counter_reset_hint != CounterResetHint::GaugeType {
                eh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                ah.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            } else {
                assert_eq!(
                    eh.counter_reset_hint, ah.counter_reset_hint,
                    "Sample header doesn't match for {name}[{i}] at ts {}",
                    e.t
                );
            }
            if in_use_bucket_compare {
                eh.compact(0);
                ah.compact(0);
            }
            assert_eq!(eh, ah, "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        } else if let Some(efh) = &e.fh {
            let mut efh = (**efh).clone();
            let mut afh = (**a.fh.as_ref().unwrap()).clone();
            if ignore_counter_resets {
                efh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                afh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            } else {
                assert_eq!(
                    efh.counter_reset_hint, afh.counter_reset_hint,
                    "Sample header doesn't match for {name}[{i}] at ts {}",
                    e.t
                );
            }
            if in_use_bucket_compare {
                efh.compact(0);
                afh.compact(0);
            }
            assert_eq!(efh, afh, "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        } else {
            assert_eq!(e.f, a.f, "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        }
    }
}

fn require_equal_samples(name: &str, expected: &[Sample], actual: &[Sample], ignore_counter_resets: bool) {
    require_equal_samples_opts(name, expected, actual, ignore_counter_resets, false);
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

/// Go `requireEqualOOOSamples` (testutil.go).
fn require_equal_ooo_samples(expected_samples: usize, db: &DB) {
    let m = &db.head.metrics.out_of_order_samples_appended;
    assert_eq!(
        expected_samples as f64,
        m.float.get() + m.histogram.get(),
        "number of ooo appended samples mismatch"
    );
}

/// Go `queryChunks` (db_test.go). Closes the querier.
fn query_chunks(mut q: Box<dyn ChunkQuerier>, matchers: &[Matcher]) -> BTreeMap<String, Vec<Meta>> {
    let mut result = BTreeMap::new();
    {
        let mut ss = q.select(&ctx(), false, None, matchers);
        while ss.next() {
            let series = ss.at().clone();
            let mut chks = Vec::new();
            {
                let mut it = series.iterator(None);
                while it.next() {
                    chks.push(it.at().clone());
                }
                assert!(it.err().is_none(), "{:?}", it.err());
            }
            if chks.is_empty() {
                continue;
            }
            result.insert(series.labels().to_string(), chks);
        }
        assert!(ss.err().is_none(), "{:?}", ss.err());
        assert!(ss.warnings().is_empty());
    }
    assert!(q.close().is_none());
    result
}

fn expand_chunk(c: &dyn Chunk) -> Vec<Sample> {
    let mut it = c.iterator(None);
    storage::expand_samples(&mut it).expect("expand samples")
}

/// Go `queryAndExpandChunks` (db_test.go).
fn query_and_expand_chunks(q: Box<dyn ChunkQuerier>, matchers: &[Matcher]) -> BTreeMap<String, Vec<Vec<Sample>>> {
    query_chunks(q, matchers)
        .into_iter()
        .map(|(k, v)| (k, v.iter().map(|m| expand_chunk(m.chunk.as_deref().expect("chunk"))).collect()))
        .collect()
}

/// Go `copyWithCounterReset` (db_test.go).
fn copy_with_counter_reset(s: &Sample, hint: CounterResetHint) -> Sample {
    if let Some(h) = &s.h {
        let mut h = (**h).clone();
        h.counter_reset_hint = hint;
        return Sample::histogram(0, s.t, h);
    }
    let mut h = (**s.fh.as_ref().unwrap()).clone();
    h.counter_reset_hint = hint;
    Sample::float_histogram(0, s.t, h)
}

fn sort_by_t(v: &mut [Sample]) {
    v.sort_by_key(|s| s.t);
}

/// The number of samples in the OOO head chunk of the series (panics when there is none, as Go's nil pointer).
fn ooo_head_chunk_samples(db: &DB, lbls: &Labels) -> usize {
    let (ms, created) = db.head.get_or_create(lbls.hash(), lbls, false).unwrap();
    assert!(!created);
    let g = ms.lock();
    g.ooo().expect("ooo").ooo_head_chunk.as_ref().expect("ooo head chunk").chunk.num_samples()
}

/// Go `require.Nil(t, ms.ooo)`.
fn require_no_ooo(db: &DB, lbls: &Labels) {
    let (ms, created) = db.head.get_or_create(lbls.hash(), lbls, false).unwrap();
    assert!(!created);
    assert!(ms.lock().ooo().is_none());
}

// ---------------------------------------------------------------------------------------------------------------
// TestOOOCompactionWithNormalCompaction_AppendV2 / TestOOOCompactionWithDisabledWriteLog_AppendV2.

fn add_two_series(db: &DB, scenario: &SampleTypeScenario, s1: &Labels, s2: &Labels, from_mins: i64, to_mins: i64) {
    let mut app = db.appender_v2(&ctx());
    for m in from_mins..=to_mins {
        let ts = m * MINUTE;
        scenario_append(scenario, app.as_mut(), s1, ts, ts).0.unwrap();
        scenario_append(scenario, app.as_mut(), s2, ts, 2 * ts).0.unwrap();
    }
    commit(&mut app);
}

fn verify_block_two_series(
    block: &dyn tsdb::BlockReader,
    scenario: &SampleTypeScenario,
    s1: &Labels,
    s2: &Labels,
    from_mins: i64,
    to_mins: i64,
) {
    let mut s1s = Vec::new();
    let mut s2s = Vec::new();
    for m in from_mins..=to_mins {
        let ts = m * MINUTE;
        s1s.push((scenario.sample_func)(ts, ts));
        s2s.push((scenario.sample_func)(ts, 2 * ts));
    }
    let exp = BTreeMap::from([(s1.to_string(), s1s), (s2.to_string(), s2s)]);
    let q = new_block_querier(block, i64::MIN, i64::MAX).unwrap();
    let act = query(q, &[re("foo", "bar.*")]);
    require_equal_series(&exp, &act, true);
}

fn ooo_compaction_with_normal_compaction(scenario: &SampleTypeScenario, disable_wal: bool) {
    let mut opts = opts_default();
    opts.out_of_order_cap_max = 30;
    opts.out_of_order_time_window = 300 * MINUTE;
    if disable_wal {
        opts.wal_segment_size = -1; // disabled WAL and WBL
    }

    let db = new_test_db(vec![with_opts(opts)]);
    db.disable_compactions(); // We want to manually call it.

    let series1 = ls(&["foo", "bar1"]);
    let series2 = ls(&["foo", "bar2"]);

    // Add an in-order samples.
    add_two_series(&db, scenario, &series1, &series2, 250, 350);
    // Add ooo samples that will result into a single block.
    add_two_series(&db, scenario, &series1, &series2, 90, 110);

    // Checking that ooo chunk is not empty.
    for lbls in [&series1, &series2] {
        assert!(ooo_head_chunk_samples(&db, lbls) > 0);
    }

    // If the normal Head is not compacted, the OOO head compaction does not take place.
    db.compact(&ctx()).unwrap();
    assert!(db.blocks().is_empty());

    // Add more in-order samples in future that would trigger the compaction.
    add_two_series(&db, scenario, &series1, &series2, 400, 450);

    // No blocks before compaction.
    assert!(db.blocks().is_empty());

    // Compacts normal and OOO head.
    db.compact(&ctx()).unwrap();

    // 2 blocks exist now. [0, 120), [250, 360)
    let blocks = db.blocks();
    assert_eq!(2, blocks.len());
    assert_eq!(0, blocks[0].min_time());
    assert_eq!(120 * MINUTE, blocks[0].max_time());
    assert_eq!(250 * MINUTE, blocks[1].min_time());
    assert_eq!(360 * MINUTE, blocks[1].max_time());

    // Checking that ooo chunk is empty.
    for lbls in [&series1, &series2] {
        require_no_ooo(&db, lbls);
    }

    // Checking for expected data in the blocks.
    verify_block_two_series(&blocks[0], scenario, &series1, &series2, 90, 110);
    verify_block_two_series(&blocks[1], scenario, &series1, &series2, 250, 350);
}

#[test]
fn test_ooo_compaction_with_normal_compaction_append_v2() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        ooo_compaction_with_normal_compaction(&scenario, false);
    }
}

/// TestOOOCompactionWithDisabledWriteLog_AppendV2 tests the scenario where the TSDB is configured to not have wal
/// and wbl but its able to compact both the in-order and out-of-order head.
#[test]
fn test_ooo_compaction_with_disabled_write_log_append_v2() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        ooo_compaction_with_normal_compaction(&scenario, true);
    }
}

// ---------------------------------------------------------------------------------------------------------------

/// TestOOOQueryAfterRestartWithSnapshotAndRemovedWBL_AppendV2 tests the scenario where the WBL goes missing after
/// a restart while snapshot was enabled, but the query still returns the right data from the mmap chunks.
#[test]
fn test_ooo_query_after_restart_with_snapshot_and_removed_wbl_append_v2() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        ooo_query_after_restart_with_snapshot_and_removed_wbl(&scenario);
    }
}

fn ooo_query_after_restart_with_snapshot_and_removed_wbl(scenario: &SampleTypeScenario) {
    let mut opts = opts_default();
    opts.out_of_order_cap_max = 10;
    opts.out_of_order_time_window = 300 * MINUTE;
    opts.enable_memory_snapshot_on_shutdown = true;

    let mut db = new_test_db(vec![with_opts(opts)]);
    db.disable_compactions(); // We want to manually call it.
    let tmp = db.tmp.take().unwrap();

    let series1 = ls(&["foo", "bar1"]);
    let series2 = ls(&["foo", "bar2"]);

    // Add an in-order samples.
    add_two_series(&db, scenario, &series1, &series2, 250, 350);
    // Add ooo samples that will result into a single block.
    add_two_series(&db, scenario, &series1, &series2, 90, 110); // The sample 110 will not be in m-map chunks.

    // Checking that there are some ooo m-map chunks.
    for lbls in [&series1, &series2] {
        let (ms, created) = db.head.get_or_create(lbls.hash(), lbls, false).unwrap();
        assert!(!created);
        let g = ms.lock();
        let ooo = g.ooo().expect("ooo");
        assert_eq!(2, ooo.ooo_mmapped_chunks.len());
        assert!(ooo.ooo_head_chunk.is_some());
    }

    // Restart DB.
    db.close().unwrap();
    drop(db);

    // For some reason wbl goes missing.
    std::fs::remove_dir_all(tmp.path().join("wbl")).unwrap();

    // Go `newTestDB(t, withDir(db.Dir()))`: default options.
    let db = new_test_db(vec![with_dir(tmp.path())]);
    db.disable_compactions(); // We want to manually call it.

    // Check ooo m-map chunks again.
    for lbls in [&series1, &series2] {
        let (ms, created) = db.head.get_or_create(lbls.hash(), lbls, false).unwrap();
        assert!(!created);
        let g = ms.lock();
        let ooo = g.ooo().expect("ooo");
        assert_eq!(2, ooo.ooo_mmapped_chunks.len());
        assert_eq!(109 * MINUTE, ooo.ooo_mmapped_chunks[1].max_time);
        assert!(ooo.ooo_head_chunk.is_none()); // Because of missing wbl.
    }

    let verify_samples = |from_mins: i64, to_mins: i64| {
        let mut s1s = Vec::new();
        let mut s2s = Vec::new();
        for m in from_mins..=to_mins {
            let ts = m * MINUTE;
            s1s.push((scenario.sample_func)(ts, ts));
            s2s.push((scenario.sample_func)(ts, ts * 2));
        }
        let exp = BTreeMap::from([(series1.to_string(), s1s), (series2.to_string(), s2s)]);
        let q = db.querier(from_mins * MINUTE, to_mins * MINUTE).unwrap();
        let act = query(q, &[re("foo", "bar.*")]);
        require_equal_series(&exp, &act, true);
    };

    // Checking for expected ooo data from mmap chunks.
    verify_samples(90, 109);

    // Compaction should also work fine.
    assert!(db.blocks().is_empty());
    db.compact_ooo_head(&ctx()).unwrap();
    let blocks = db.blocks();
    assert_eq!(1, blocks.len()); // One block from OOO data.
    assert_eq!(0, blocks[0].min_time());
    assert_eq!(120 * MINUTE, blocks[0].max_time());

    // Checking that ooo chunk is empty in Head.
    for lbls in [&series1, &series2] {
        require_no_ooo(&db, lbls);
    }

    verify_samples(90, 109);
    drop(db);
    drop(tmp);
}

// ---------------------------------------------------------------------------------------------------------------
// TestQuerierOOOQuery_AppendV2 / TestChunkQuerierOOOQuery_AppendV2.

#[derive(Clone, Copy, PartialEq)]
enum OooKind {
    Float,
    IntHistogram,
    FloatHistogram,
    IntHistogramCounterResets,
    IntHistogramWithRecode,
}

/// Go `nBucketHistogram`.
fn n_bucket_histogram(n: i64) -> Histogram {
    let mut h = Histogram {
        count: n as u64,
        sum: n as f64,
        ..Default::default()
    };
    if n == 0 {
        return h;
    }
    h.positive_spans = vec![Span {
        offset: 0,
        length: n as u32,
    }];
    h.positive_buckets = vec![0; n as usize];
    h.positive_buckets[0] = 1;
    h
}

impl OooKind {
    fn append(self, app: &mut dyn AppenderV2, ts: i64, counter_reset: bool) -> Result<SeriesRef, Error> {
        let l = ls(&["foo", "bar1"]);
        let o = AOptions::default();
        match self {
            OooKind::Float => app.append(0, &l, 0, ts, ts as f64, None, None, &o),
            OooKind::IntHistogram => {
                let mut h = tu::generate_test_histogram(ts);
                if counter_reset {
                    h.counter_reset_hint = CounterResetHint::CounterReset;
                }
                app.append(0, &l, 0, ts, 0.0, Some(&h), None, &o)
            }
            OooKind::FloatHistogram => {
                let mut fh = tu::generate_test_float_histogram(ts);
                if counter_reset {
                    fh.counter_reset_hint = CounterResetHint::CounterReset;
                }
                app.append(0, &l, 0, ts, 0.0, None, Some(&fh), &o)
            }
            OooKind::IntHistogramCounterResets => {
                // Adding counter reset to all histograms means each histogram will have its own chunk.
                let mut h = tu::generate_test_histogram(ts);
                h.counter_reset_hint = CounterResetHint::CounterReset; // For this scenario, ignore the counterReset argument.
                app.append(0, &l, 0, ts, 0.0, Some(&h), None, &o)
            }
            OooKind::IntHistogramWithRecode => {
                // Histograms have increasing number of buckets so their chunks are recoded.
                let n = ts / MINUTE;
                app.append(0, &l, 0, ts, 0.0, Some(&n_bucket_histogram(n)), None, &o)
            }
        }
    }

    fn sample(self, ts: i64) -> Sample {
        match self {
            OooKind::Float => Sample::float(0, ts, ts as f64),
            OooKind::IntHistogram | OooKind::IntHistogramCounterResets => {
                Sample::histogram(0, ts, tu::generate_test_histogram(ts))
            }
            OooKind::FloatHistogram => Sample::float_histogram(0, ts, tu::generate_test_float_histogram(ts)),
            OooKind::IntHistogramWithRecode => Sample::histogram(0, ts, n_bucket_histogram(ts / MINUTE)),
        }
    }
}

struct SampleBatch {
    min_t: i64,
    max_t: i64,
    filter: fn(i64) -> bool,
    counter_reset: bool,
    is_ooo: bool,
}

struct OooQueryCase {
    name: &'static str,
    ooo_cap: i64,
    query_min_t: i64,
    query_max_t: i64,
    batches: Vec<SampleBatch>,
}

fn all(_: i64) -> bool {
    true
}
fn even(t: i64) -> bool {
    t % 2 == 0
}
fn odd(t: i64) -> bool {
    t % 2 == 1
}

fn b(min_t: i64, max_t: i64, filter: fn(i64) -> bool, counter_reset: bool, is_ooo: bool) -> SampleBatch {
    SampleBatch {
        min_t,
        max_t,
        filter,
        counter_reset,
        is_ooo,
    }
}

fn ooo_query_cases() -> Vec<OooQueryCase> {
    vec![
        OooQueryCase {
            name: "query interval covering ooomint and inordermaxt returns all ingested samples",
            ooo_cap: 30,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![b(minutes(100), minutes(200), all, false, false), b(minutes(0), minutes(99), all, false, true)],
        },
        OooQueryCase {
            name: "partial query interval returns only samples within interval",
            ooo_cap: 30,
            query_min_t: minutes(20),
            query_max_t: minutes(180),
            batches: vec![b(minutes(100), minutes(200), all, false, false), b(minutes(0), minutes(99), all, false, true)],
        },
        OooQueryCase {
            // In order: 100-200 normal. out of order first path: 0, 2, 4, ... 98 (no counter reset), second
            // pass: 1, 3, 5, ... 99 (with counter reset).
            name: "alternating OOO batches",
            ooo_cap: 0,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![
                b(minutes(100), minutes(200), all, false, false),
                b(minutes(0), minutes(99), even, false, true),
                b(minutes(0), minutes(99), odd, true, true),
            ],
        },
        OooQueryCase {
            name: "query overlapping inorder and ooo samples returns all ingested samples at the end of the interval",
            ooo_cap: 30,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![b(minutes(100), minutes(200), even, false, false), b(minutes(170), minutes(180), odd, false, true)],
        },
        OooQueryCase {
            name: "query overlapping inorder and ooo in-memory samples returns all ingested samples at the beginning of the interval",
            ooo_cap: 30,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![b(minutes(100), minutes(200), even, false, false), b(minutes(100), minutes(110), odd, false, true)],
        },
        OooQueryCase {
            name: "query inorder contain ooo mmapped samples returns all ingested samples at the beginning of the interval",
            ooo_cap: 5,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![
                b(minutes(100), minutes(200), even, false, false),
                // Append samples to fit in a single mmapped OOO chunk and fit inside the first in-order mmapped chunk.
                b(minutes(101), minutes(101 + (5 - 1) * 2), odd, false, true),
                // Append some more OOO samples to trigger mapping the OOO chunk, but use time 151 to not overlap with
                // in-order head chunk.
                b(minutes(191), minutes(193), odd, false, true),
            ],
        },
        OooQueryCase {
            name: "query overlapping inorder and ooo mmapped samples returns all ingested samples at the beginning of the interval",
            ooo_cap: 30,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![
                b(minutes(100), minutes(200), even, false, false),
                // Append samples to fit in a single mmapped OOO chunk and overlap the first in-order mmapped chunk.
                b(minutes(101), minutes(101 + (30 - 1) * 2), odd, false, true),
                // Append some more OOO samples to trigger mapping the OOO chunk, but use time 151 to not overlap with
                // in-order head chunk.
                b(minutes(191), minutes(193), odd, false, true),
            ],
        },
    ]
}

/// Go `addSample` of testQuerierOOOQueryAppendV2.
#[allow(clippy::too_many_arguments)]
fn ooo_add_sample(
    db: &DB,
    kind: OooKind,
    from_mins: i64,
    to_mins: i64,
    query_min_t: i64,
    query_max_t: i64,
    exp_samples: &mut Vec<Sample>,
    filter: fn(i64) -> bool,
    counter_reset: bool,
) -> usize {
    let mut app = db.appender_v2(&ctx());
    let mut total_appended = 0;
    let mut m = from_mins;
    while m <= to_mins {
        if filter(m / MINUTE) {
            let r = kind.append(app.as_mut(), m, counter_reset);
            if m >= query_min_t && m <= query_max_t {
                exp_samples.push(kind.sample(m));
            }
            r.unwrap();
            total_appended += 1;
        }
        m += MINUTE;
    }
    commit(&mut app);
    assert!(total_appended > 0); // Sanity check that filter is not too zealous.
    total_appended
}

/// Builds the DB of one case and appends its batches; returns the DB, the sorted expected samples and the
/// number of OOO samples.
fn ooo_query_setup(kind: OooKind, tc: &OooQueryCase, mut opts: Options) -> (TestDb, Vec<Sample>, usize) {
    opts.out_of_order_cap_max = tc.ooo_cap;
    let db = new_test_db(vec![with_opts(opts)]);
    db.disable_compactions();

    let mut exp_samples = Vec::new();
    let mut ooo_samples = 0;
    for batch in &tc.batches {
        let n = ooo_add_sample(
            &db,
            kind,
            batch.min_t,
            batch.max_t,
            tc.query_min_t,
            tc.query_max_t,
            &mut exp_samples,
            batch.filter,
            batch.counter_reset,
        );
        if batch.is_ooo {
            ooo_samples += n;
        }
    }
    sort_by_t(&mut exp_samples);
    (db, exp_samples, ooo_samples)
}

#[test]
fn test_querier_ooo_query_append_v2() {
    let scenarios = [
        ("float", OooKind::Float),
        ("integer histogram", OooKind::IntHistogram),
        ("float histogram", OooKind::FloatHistogram),
        ("integer histogram counter resets", OooKind::IntHistogramCounterResets),
    ];
    for (name, kind) in scenarios {
        let mut opts = opts_default();
        opts.out_of_order_time_window = 24 * HOUR;
        let series1 = ls(&["foo", "bar1"]);
        for tc in ooo_query_cases() {
            eprintln!("{name}/name={}", tc.name);
            let (db, exp_samples, ooo_samples) = ooo_query_setup(kind, &tc, opts.clone());

            let querier = db.querier(tc.query_min_t, tc.query_max_t).unwrap();
            let series_set = query(querier, &[eq("foo", "bar1")]);
            let got_samples = series_set.get(&series1.to_string()).expect("samples");
            assert_eq!(1, series_set.len());
            require_equal_samples(&series1.to_string(), &exp_samples, got_samples, true);
            require_equal_ooo_samples(ooo_samples, &db);
        }
    }
}

#[test]
fn test_chunk_querier_ooo_query_append_v2() {
    let scenarios = [
        ("float", OooKind::Float, false),
        ("integer histogram", OooKind::IntHistogram, false),
        ("float histogram", OooKind::FloatHistogram, false),
        ("integer histogram counter resets", OooKind::IntHistogramCounterResets, false),
        // Only check in-use buckets for this scenario. Recoding adds empty buckets.
        ("integer histogram with recode", OooKind::IntHistogramWithRecode, true),
    ];
    for (name, kind, check_in_use_buckets) in scenarios {
        let mut opts = opts_default();
        opts.out_of_order_cap_max = 30;
        opts.out_of_order_time_window = 24 * HOUR;
        let series1 = ls(&["foo", "bar1"]);
        for tc in ooo_query_cases() {
            eprintln!("{name}/name={}", tc.name);
            let (db, exp_samples, ooo_samples) = ooo_query_setup(kind, &tc, opts.clone());

            let querier = db.chunk_querier(tc.query_min_t, tc.query_max_t).unwrap();
            let chks = query_chunks(querier, &[eq("foo", "bar1")]);
            let s1_chks = chks.get(&series1.to_string()).expect("chunks");
            assert_eq!(1, chks.len());
            require_equal_ooo_samples(ooo_samples, &db);
            let mut got_samples = Vec::new();
            for chunk in s1_chks {
                let smpls = expand_chunk(chunk.chunk.as_deref().expect("chunk"));

                // Verify that no sample is outside the chunk's time range.
                for (i, s) in smpls.iter().enumerate() {
                    if i == 0 {
                        assert_eq!(chunk.min_time, s.t, "first sample {s:?} not at chunk min time {}", chunk.min_time);
                    } else if i == smpls.len() - 1 {
                        assert_eq!(chunk.max_time, s.t, "last sample {s:?} not at chunk max time {}", chunk.max_time);
                    } else {
                        assert!(s.t >= chunk.min_time, "sample {s:?} before chunk min time {}", chunk.min_time);
                        assert!(s.t <= chunk.max_time, "sample {s:?} after chunk max time {}", chunk.max_time);
                    }
                }
                got_samples.extend(smpls);
            }
            require_equal_samples_opts(&series1.to_string(), &exp_samples, &got_samples, true, check_in_use_buckets);
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------

/// TestOOONativeHistogramsWithCounterResets_AppendV2 verifies the counter reset headers for in-order and
/// out-of-order samples upon ingestion. Note that when the counter reset(s) occur in OOO samples, the header is set
/// to UnknownCounterReset rather than CounterReset. This is because with OOO native histogram samples, it cannot be
/// definitely determined if a counter reset occurred because the samples are not consecutive, and another sample
/// could potentially come in that would change the status of the header. In this case, the UnknownCounterReset
/// headers would be re-checked at query time and updated as needed. However, this test is checking the counter
/// reset headers at the time of storage.
#[test]
fn test_ooo_native_histograms_with_counter_resets_append_v2() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        if name == INT_HISTOGRAM || name == FLOAT_HISTOGRAM {
            ooo_native_histograms_with_counter_resets(&scenario);
        }
    }
}

struct ResetBatch {
    from: i64,
    until: i64,
    should_reset: fn(i64) -> bool,
    exp_counter_reset_hints: Vec<CounterResetHint>,
}

fn ooo_native_histograms_with_counter_resets(scenario: &SampleTypeScenario) {
    use CounterResetHint::{NotCounterReset as N, UnknownCounterReset as U};
    let mut opts = opts_default();
    opts.out_of_order_cap_max = 30;
    opts.out_of_order_time_window = 24 * HOUR;

    let lbls = ls(&["foo", "bar1"]);
    let no_reset: fn(i64) -> bool = |_| false;

    let rb = |from, until, should_reset, exp_counter_reset_hints| ResetBatch {
        from,
        until,
        should_reset,
        exp_counter_reset_hints,
    };
    let tests: Vec<(&str, i64, i64, Vec<ResetBatch>)> = vec![
        (
            "Counter reset within in-order samples",
            minutes(40),
            minutes(55),
            vec![
                // In-order samples
                rb(40, 50, |v| v == 45, vec![U, N, N, N, N, U, N, N, N, N]),
            ],
        ),
        (
            "Counter reset right at beginning of OOO samples",
            minutes(40),
            minutes(55),
            vec![
                // In-order samples
                rb(40, 45, no_reset, vec![U, N, N, N, N]),
                rb(50, 55, no_reset, vec![U, N, N, N, N]),
                // OOO samples
                rb(45, 50, |v| v == 45, vec![U, N, N, N, N]),
            ],
        ),
        (
            "Counter resets in both in-order and OOO samples",
            minutes(40),
            minutes(55),
            vec![
                // In-order samples
                rb(40, 45, |v| v == 44, vec![U, N, N, N, U]),
                rb(50, 55, no_reset, vec![U, N, N, N, N]),
                // OOO samples
                rb(45, 50, |v| v == 49, vec![U, N, N, N, U]),
            ],
        ),
    ];
    for (tc_name, query_min, query_max, batches) in tests {
        eprintln!("name={tc_name}");
        let db = new_test_db(vec![with_opts(opts.clone())]);
        db.disable_compactions();

        let mut app = db.appender_v2(&ctx());
        let mut exp_samples: BTreeMap<String, Vec<Sample>> = BTreeMap::new();

        for batch in &batches {
            let mut j = batch.from;
            for (smpl_idx, i) in (batch.from..batch.until).enumerate() {
                if (batch.should_reset)(i) {
                    j = 0;
                }
                let (r, mut s) = scenario_append(scenario, app.as_mut(), &lbls, minutes(i), j);
                r.unwrap();
                if let Some(h) = s.h.as_mut() {
                    h.counter_reset_hint = batch.exp_counter_reset_hints[smpl_idx];
                } else if let Some(fh) = s.fh.as_mut() {
                    fh.counter_reset_hint = batch.exp_counter_reset_hints[smpl_idx];
                }
                exp_samples.entry(lbls.to_string()).or_default().push(s);
                j += 1;
            }
        }
        commit(&mut app);

        for v in exp_samples.values_mut() {
            sort_by_t(v);
        }

        let querier = db.querier(query_min, query_max).unwrap();
        let series_set = query(querier, &[eq("foo", "bar1")]);
        assert!(series_set.contains_key(&lbls.to_string()));
        assert_eq!(1, series_set.len());
        require_equal_series(&exp_samples, &series_set, false);
    }
}

// ---------------------------------------------------------------------------------------------------------------

#[test]
fn test_ooo_interleaved_implicit_counter_resets_append_v2() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        ooo_interleaved_implicit_counter_resets(name, &scenario);
    }
}

fn interleaved_append(name: &str, app: &mut dyn AppenderV2, ts: i64, v: i64) -> Result<SeriesRef, Error> {
    let l = ls(&["foo", "bar1"]);
    let o = AOptions::default();
    let span = vec![Span { offset: 0, length: 1 }];
    let cv: model::histogram::CustomValues = Some(std::sync::Arc::from(&[1.0, 2.0, 3.0][..]));
    match name {
        INT_HISTOGRAM | CUSTOM_BUCKETS_INT_HISTOGRAM => {
            let mut h = Histogram {
                count: v as u64,
                sum: v as f64,
                positive_spans: span,
                positive_buckets: vec![v],
                ..Default::default()
            };
            if name == CUSTOM_BUCKETS_INT_HISTOGRAM {
                h.schema = -53;
                h.custom_values = cv;
            }
            app.append(0, &l, 0, ts, 0.0, Some(&h), None, &o)
        }
        FLOAT_HISTOGRAM | CUSTOM_BUCKETS_FLOAT_HISTOGRAM => {
            let mut fh = FloatHistogram {
                count: v as f64,
                sum: v as f64,
                positive_spans: span,
                positive_buckets: vec![v as f64],
                ..Default::default()
            };
            if name == CUSTOM_BUCKETS_FLOAT_HISTOGRAM {
                fh.schema = -53;
                fh.custom_values = cv;
            }
            app.append(0, &l, 0, ts, 0.0, None, Some(&fh), &o)
        }
        _ => unreachable!(),
    }
}

/// The hint and count of a histogram sample of the interleaved test.
fn hint_and_count(name: &str, s: &Sample) -> (CounterResetHint, i64) {
    match name {
        INT_HISTOGRAM | CUSTOM_BUCKETS_INT_HISTOGRAM => {
            let h = s.h.as_ref().expect("histogram");
            (h.counter_reset_hint, h.count as i64)
        }
        FLOAT_HISTOGRAM | CUSTOM_BUCKETS_FLOAT_HISTOGRAM => {
            let fh = s.fh.as_ref().expect("float histogram");
            (fh.counter_reset_hint, fh.count as i64)
        }
        _ => panic!("unexpected sample type {name}"),
    }
}

struct InterleavedCase {
    samples: Vec<(i64, i64)>,
    ooo_cap: i64,
    /// The expected samples with counter reset.
    expected_samples: Vec<(i64, i64, CounterResetHint)>,
    /// The expected counter reset hint for each chunk.
    expected_chunks: Vec<(CounterResetHint, usize)>,
}

fn ooo_interleaved_implicit_counter_resets(name: &str, scenario: &SampleTypeScenario) {
    use CounterResetHint::{NotCounterReset as N, UnknownCounterReset as U};
    if scenario.sample_type != "histogram" {
        return;
    }
    match name {
        INT_HISTOGRAM | FLOAT_HISTOGRAM | CUSTOM_BUCKETS_INT_HISTOGRAM | CUSTOM_BUCKETS_FLOAT_HISTOGRAM => {}
        _ => return, // gaugeIntHistogram, gaugeFloatHistogram.
    }

    // Not a sample, we're encoding an integer counter that we convert to a histogram with a single bucket.
    let cases: Vec<(&str, InterleavedCase)> = vec![
        (
            "counter reset in-order cleared by in-memory OOO chunk",
            InterleavedCase {
                samples: vec![
                    (1, 40), // New in In-order. I1.
                    (4, 30), // In-order counter reset. I2.
                    (2, 40), // New in OOO. O1.
                    (3, 10), // OOO counter reset. O2.
                ],
                ooo_cap: 30,
                // Expect all to be set to UnknownCounterReset because we switch between in-order and out-of-order
                // samples.
                expected_samples: vec![
                    (1, 40, U), // I1.
                    (2, 40, U), // O1.
                    (3, 10, U), // O2.
                    (4, 30, U), // I2. Counter reset cleared by iterator change.
                ],
                expected_chunks: vec![
                    (U, 1), // I1.
                    (U, 1), // O1.
                    (U, 1), // O2.
                    (U, 1), // I2.
                ],
            },
        ),
        (
            "counter reset in OOO mmapped chunk cleared by in-memory ooo chunk",
            InterleavedCase {
                samples: vec![
                    (8, 30), // In-order, new chunk. I1.
                    (1, 10), // OOO, new chunk (will be mmapped). MO1.
                    (2, 20), // OOO, no reset (will be mmapped). MO1.
                    (3, 30), // OOO, no reset (will be mmapped). MO1.
                    (5, 20), // OOO, reset (will be mmapped). MO2.
                    (6, 10), // OOO, reset (will be mmapped). MO3.
                    (7, 20), // OOO, no reset (will be mmapped). MO3.
                    (4, 10), // OOO, inserted into memory, triggers mmap. O1.
                ],
                ooo_cap: 6,
                expected_samples: vec![
                    (1, 10, U), // MO1.
                    (2, 20, N), // MO1.
                    (3, 30, N), // MO1.
                    (4, 10, U), // O1. Counter reset cleared by iterator change.
                    (5, 20, U), // MO2.
                    (6, 10, U), // MO3.
                    (7, 20, N), // MO3.
                    (8, 30, U), // I1.
                ],
                expected_chunks: vec![
                    (U, 3), // MO1.
                    (U, 1), // O1.
                    (U, 1), // MO2.
                    (U, 2), // MO3.
                    (U, 1), // I1.
                ],
            },
        ),
        (
            "counter reset in OOO mmapped chunk cleared by another OOO mmapped chunk",
            InterleavedCase {
                samples: vec![
                    (8, 100), // In-order, new chunk. I1.
                    (1, 50),  // OOO, new chunk (will be mmapped). MO1.
                    (5, 40),  // OOO, reset (will be mmapped). MO2.
                    (6, 50),  // OOO, no reset (will be mmapped). MO2.
                    (2, 10),  // OOO, new chunk no reset (will be mmapped). MO3.
                    (3, 20),  // OOO, no reset (will be mmapped). MO3.
                    (4, 30),  // OOO, no reset (will be mmapped). MO3.
                    (7, 60),  // OOO, no reset in memory. O1.
                ],
                ooo_cap: 3,
                expected_samples: vec![
                    (1, 50, U),  // MO1.
                    (2, 10, U),  // MO3.
                    (3, 20, N),  // MO3.
                    (4, 30, N),  // MO3.
                    (5, 40, U),  // MO2.
                    (6, 50, N),  // MO2.
                    (7, 60, U),  // O1.
                    (8, 100, U), // I1.
                ],
                expected_chunks: vec![
                    (U, 1), // MO1.
                    (U, 3), // MO3.
                    (U, 2), // MO2.
                    (U, 1), // O1.
                    (U, 1), // I1.
                ],
            },
        ),
    ];

    for (tc_name, tc) in cases {
        eprintln!("{name}/{tc_name}");
        let mut opts = opts_default();
        opts.out_of_order_cap_max = tc.ooo_cap;
        opts.out_of_order_time_window = 24 * HOUR;

        let db = new_test_db(vec![with_opts(opts)]);
        db.disable_compactions();

        let mut app = db.appender_v2(&ctx());
        for &(ts, v) in &tc.samples {
            interleaved_append(name, app.as_mut(), ts, v).unwrap();
        }
        commit(&mut app);

        // querier
        {
            let querier = db.querier(0, 10).unwrap();
            let series_set = query(querier, &[eq("foo", "bar1")]);
            assert_eq!(1, series_set.len());
            let samples = series_set.get("{foo=\"bar1\"}").expect("series");
            assert_eq!(tc.samples.len(), samples.len());
            assert_eq!(tc.expected_samples.len(), samples.len());

            // We expect all unknown counter resets because we clear the counter reset hint when we switch between
            // in-order and out-of-order samples.
            for (i, s) in samples.iter().enumerate() {
                let (hint, count) = hint_and_count(name, s);
                assert_eq!(tc.expected_samples[i].2, hint, "sample {i}");
                assert_eq!(tc.expected_samples[i].1, count, "sample {i}");
            }
        }

        // chunk-querier
        {
            let querier = db.chunk_querier(0, 10).unwrap();
            let chunk_set = query_and_expand_chunks(querier, &[eq("foo", "bar1")]);
            assert_eq!(1, chunk_set.len());
            let chunks = chunk_set.get("{foo=\"bar1\"}").expect("series");
            assert_eq!(tc.expected_chunks.len(), chunks.len());
            let mut idx = 0;
            for (i, samples) in chunks.iter().enumerate() {
                assert_eq!(tc.expected_chunks[i].1, samples.len());
                for (j, s) in samples.iter().enumerate() {
                    let expect_hint = if j > 0 { N } else { tc.expected_chunks[i].0 };
                    let (hint, count) = hint_and_count(name, s);
                    assert_eq!(expect_hint, hint, "sample {idx}");
                    assert_eq!(tc.expected_samples[idx].1, count, "sample {idx}");
                    idx += 1;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------

#[test]
fn test_ooo_append_and_query_append_v2() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        ooo_append_and_query(&scenario);
    }
}

fn ooo_append_and_query(scenario: &SampleTypeScenario) {
    let mut opts = opts_default();
    opts.out_of_order_cap_max = 30;
    opts.out_of_order_time_window = 4 * HOUR;

    let db = new_test_db(vec![with_opts(opts)]);
    db.disable_compactions();

    let s1 = ls(&["foo", "bar1"]);
    let s2 = ls(&["foo", "bar2"]);

    let mut appended_samples: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
    let mut total_samples = 0usize;
    let mut add_sample = |appended: &mut BTreeMap<String, Vec<Sample>>, lbls: &Labels, from_mins: i64, to_mins: i64, face_error: bool| {
        let mut app = db.appender_v2(&ctx());
        let key = lbls.to_string();
        let (from, to) = (minutes(from_mins), minutes(to_mins));
        let mut m = from;
        while m <= to {
            let val = rand_intn(1000);
            let (r, s) = scenario_append(scenario, app.as_mut(), lbls, m, val);
            if face_error {
                assert!(r.is_err());
            } else {
                r.unwrap();
                appended.entry(key.clone()).or_default().push(s);
                total_samples += 1;
            }
            m += MINUTE;
        }
        if face_error {
            assert!(app.rollback().is_none());
        } else {
            commit(&mut app);
        }
        total_samples
    };

    let test_query = |appended: &mut BTreeMap<String, Vec<Sample>>, total: usize, from: i64, to: i64| {
        let querier = db.querier(from, to).unwrap();
        let series_set = query(querier, &[re("foo", "bar.")]);
        for v in appended.values_mut() {
            sort_by_t(v);
        }
        let mut exp_samples: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
        for (k, samples) in appended.iter() {
            for s in samples {
                if s.t < from || s.t > to {
                    continue;
                }
                exp_samples.entry(k.clone()).or_default().push(s.clone());
            }
        }
        require_equal_series(&exp_samples, &series_set, true);
        require_equal_ooo_samples(total - 2, &db);
    };

    let verify_ooo_min_max_times = |exp_min: i64, exp_max: i64| {
        assert_eq!(minutes(exp_min), db.head.min_ooo_time());
        assert_eq!(minutes(exp_max), db.head.max_ooo_time());
    };

    let a = &mut appended_samples;
    // In-order samples.
    add_sample(a, &s1, 300, 300, false);
    let mut t = add_sample(a, &s2, 290, 290, false);
    assert_eq!(2.0, db.head.metrics.chunks_created.get());
    test_query(a, t, i64::MIN, i64::MAX);

    // Some ooo samples.
    add_sample(a, &s1, 250, 260, false);
    t = add_sample(a, &s2, 255, 265, false);
    verify_ooo_min_max_times(250, 265);
    test_query(a, t, i64::MIN, i64::MAX);
    test_query(a, t, minutes(250), minutes(265)); // Test querying ooo data time range.
    test_query(a, t, minutes(290), minutes(300)); // Test querying in-order data time range.
    test_query(a, t, minutes(250), minutes(300)); // Test querying the entire range.

    // Out of time window.
    add_sample(a, &s1, 59, 59, true);
    t = add_sample(a, &s2, 49, 49, true);
    verify_ooo_min_max_times(250, 265);
    test_query(a, t, i64::MIN, i64::MAX);

    // At the edge of time window, also it would be "out of bound" without the ooo support.
    t = add_sample(a, &s1, 60, 65, false);
    verify_ooo_min_max_times(60, 265);
    test_query(a, t, i64::MIN, i64::MAX);

    // This sample is not within the time window w.r.t. the head's maxt, but it is within the window w.r.t. the
    // series' maxt. But we consider only head's maxt.
    t = add_sample(a, &s2, 59, 59, true);
    verify_ooo_min_max_times(60, 265);
    test_query(a, t, i64::MIN, i64::MAX);

    // Now the sample is within time window w.r.t. the head's maxt.
    t = add_sample(a, &s2, 60, 65, false);
    verify_ooo_min_max_times(60, 265);
    test_query(a, t, i64::MIN, i64::MAX);

    // Out of time window again.
    add_sample(a, &s1, 59, 59, true);
    t = add_sample(a, &s2, 49, 49, true);
    test_query(a, t, i64::MIN, i64::MAX);

    // Generating some m-map chunks. The m-map chunks here are in such a way that when sorted w.r.t. mint, the last
    // chunk's maxt is not the overall maxt of the merged chunk. This tests a bug fixed in
    // https://github.com/grafana/mimir-prometheus/pull/238/.
    assert_eq!(4.0, db.head.metrics.chunks_created.get());
    t = add_sample(a, &s1, 180, 249, false);
    assert_eq!(6.0, db.head.metrics.chunks_created.get());
    verify_ooo_min_max_times(60, 265);
    test_query(a, t, i64::MIN, i64::MAX);
}

// ---------------------------------------------------------------------------------------------------------------

#[test]
fn test_ooo_disabled_append_v2() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        ooo_disabled(&scenario);
    }
}

fn ooo_disabled(scenario: &SampleTypeScenario) {
    let mut opts = opts_default();
    opts.out_of_order_time_window = 0;
    let db = new_test_db(vec![with_opts(opts)]);
    db.disable_compactions();

    let s1 = ls(&["foo", "bar1"]);
    let mut exp_samples: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
    let mut failed_samples = 0usize;

    let mut add_sample = |lbls: &Labels, from_mins: i64, to_mins: i64, face_error: bool| {
        let mut app = db.appender_v2(&ctx());
        let key = lbls.to_string();
        let (from, to) = (minutes(from_mins), minutes(to_mins));
        let mut m = from;
        while m <= to {
            let (r, _) = scenario_append(scenario, app.as_mut(), lbls, m, m);
            if face_error {
                assert!(r.is_err());
                failed_samples += 1;
            } else {
                r.unwrap();
                exp_samples.entry(key.clone()).or_default().push((scenario.sample_func)(m, m));
            }
            m += MINUTE;
        }
        if face_error {
            assert!(app.rollback().is_none());
        } else {
            commit(&mut app);
        }
    };

    add_sample(&s1, 300, 300, false); // In-order samples.
    add_sample(&s1, 250, 260, true); // Some ooo samples.
    add_sample(&s1, 59, 59, true); // Out of time window.
    add_sample(&s1, 60, 65, true); // At the edge of time window, also it would be "out of bound" without the ooo support.
    add_sample(&s1, 59, 59, true); // Out of time window again.
    add_sample(&s1, 301, 310, false); // More in-order samples.

    let querier = db.querier(i64::MIN, i64::MAX).unwrap();
    let series_set = query(querier, &[re("foo", "bar.")]);
    require_equal_series(&exp_samples, &series_set, true);
    require_equal_ooo_samples(0, &db);
    let m = &db.head.metrics;
    assert_eq!(
        failed_samples as f64,
        m.out_of_order_samples.with_label_values(scenario.sample_type).get()
            + m.out_of_bound_samples.with_label_values(scenario.sample_type).get(),
        "number of ooo/oob samples mismatch"
    );

    // Verifying that no OOO artifacts were generated.
    let err = std::fs::read_dir(db.dir().join(wlog::WBL_DIR_NAME)).unwrap_err();
    assert_eq!(std::io::ErrorKind::NotFound, err.kind());

    let (ms, created) = db.head.get_or_create(s1.hash(), &s1, false).unwrap();
    assert!(!created);
    assert!(ms.lock().ooo().is_none());
}

// ---------------------------------------------------------------------------------------------------------------

#[test]
fn test_wbl_and_mmap_replay_append_v2() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        wbl_and_mmap_replay(&scenario);
    }
}

fn copy_dirs(src: &Path, dst: &Path) {
    tsdb_format::fileutil::copy_dirs(src, dst).unwrap();
}

fn wbl_and_mmap_replay(scenario: &SampleTypeScenario) {
    let mut opts = opts_default();
    opts.out_of_order_cap_max = 30;
    opts.out_of_order_time_window = 4 * HOUR;

    let mut db = new_test_db(vec![with_opts(opts.clone())]);
    db.disable_compactions();
    let tmp = db.tmp.take().unwrap();
    let dir = tmp.path().to_path_buf();

    let s1 = ls(&["foo", "bar1"]);

    let mut exp_samples: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
    let add_sample = |db: &DB, exp: &mut BTreeMap<String, Vec<Sample>>, lbls: &Labels, from_mins: i64, to_mins: i64| {
        let mut app = db.appender_v2(&ctx());
        let key = lbls.to_string();
        let mut m = minutes(from_mins);
        while m <= minutes(to_mins) {
            let val = rand_intn(1000);
            let (r, s) = scenario_append(scenario, app.as_mut(), lbls, m, val);
            r.unwrap();
            exp.entry(key.clone()).or_default().push(s);
            m += MINUTE;
        }
        commit(&mut app);
    };

    let test_query = |db: &DB, exp: &mut BTreeMap<String, Vec<Sample>>| {
        let querier = db.querier(i64::MIN, i64::MAX).unwrap();
        let series_set = query(querier, &[re("foo", "bar.")]);
        for v in exp.values_mut() {
            sort_by_t(v);
        }
        require_equal_series(exp, &series_set, true);
    };

    // In-order samples.
    add_sample(&db, &mut exp_samples, &s1, 300, 300);
    assert_eq!(1.0, db.head.metrics.chunks_created.get());

    // Some ooo samples.
    add_sample(&db, &mut exp_samples, &s1, 250, 260);
    add_sample(&db, &mut exp_samples, &s1, 195, 249); // This creates some m-map chunks.
    assert_eq!(4.0, db.head.metrics.chunks_created.get());
    test_query(&db, &mut exp_samples);
    let (ooo_mint, ooo_maxt) = (minutes(195), minutes(260));

    // Collect the samples only present in the ooo m-map chunks.
    let mut s1_mmap_samples = Vec::new();
    {
        let (ms, created) = db.head.get_or_create(s1.hash(), &s1, false).unwrap();
        assert!(!created);
        let refs: Vec<_> = ms.lock().ooo().expect("ooo").ooo_mmapped_chunks.iter().map(|mc| mc.r#ref).collect();
        for r in refs {
            let chk = db.head.chunk_disk_mapper.chunk(r).unwrap();
            s1_mmap_samples.extend(expand_chunk(chk.as_ref()));
        }
    }
    assert!(!s1_mmap_samples.is_empty());

    // Making a copy of original state of WBL and Mmap files to use it later.
    let mmap_dir = mmapped_chunks_dir(&db.head.opts.chunk_dir_root);
    let wbl_dir = db.head.wbl().expect("wbl").dir().to_path_buf();
    db.close().unwrap();
    drop(db);

    let copies = TempDir::new();
    let original_wbl_dir = copies.path().join("original_wbl");
    let original_mmap_dir = copies.path().join("original_mmap");
    copy_dirs(&wbl_dir, &original_wbl_dir);
    copy_dirs(&mmap_dir, &original_mmap_dir);
    let reset_wbl_to_original = || {
        std::fs::remove_dir_all(&wbl_dir).unwrap();
        copy_dirs(&original_wbl_dir, &wbl_dir);
    };
    let reset_mmap_to_original = || {
        let _ = std::fs::remove_dir_all(&mmap_dir);
        copy_dirs(&original_mmap_dir, &mmap_dir);
    };

    let open = |opts: &Options| new_test_db(vec![with_dir(&dir), with_opts(opts.clone())]);

    {
        eprintln!("Restart DB with both WBL and M-map files for ooo data");
        let db = open(&opts);
        assert_eq!(ooo_mint, db.head.min_ooo_time());
        assert_eq!(ooo_maxt, db.head.max_ooo_time());
        test_query(&db, &mut exp_samples);
    }

    {
        eprintln!("Restart DB with only WBL for ooo data");
        std::fs::remove_dir_all(&mmap_dir).unwrap();

        let db = open(&opts);
        assert_eq!(ooo_mint, db.head.min_ooo_time());
        assert_eq!(ooo_maxt, db.head.max_ooo_time());
        test_query(&db, &mut exp_samples);
    }

    {
        eprintln!("Restart DB with only M-map files for ooo data");
        std::fs::remove_dir_all(&wbl_dir).unwrap();
        reset_mmap_to_original();

        let db = open(&opts);
        assert_eq!(ooo_mint, db.head.min_ooo_time());
        assert_eq!(ooo_maxt, db.head.max_ooo_time());
        let in_order_sample = exp_samples[&s1.to_string()].last().unwrap().clone();
        let mut exp = s1_mmap_samples.clone();
        exp.push(in_order_sample);
        test_query(&db, &mut BTreeMap::from([(s1.to_string(), exp)]));
    }

    {
        eprintln!("Restart DB with WBL+Mmap while increasing the OOOCapMax");
        reset_wbl_to_original();
        reset_mmap_to_original();

        opts.out_of_order_cap_max = 60;
        let db = open(&opts);
        assert_eq!(ooo_mint, db.head.min_ooo_time());
        assert_eq!(ooo_maxt, db.head.max_ooo_time());
        test_query(&db, &mut exp_samples);
    }

    {
        eprintln!("Restart DB with WBL+Mmap while decreasing the OOOCapMax");
        reset_mmap_to_original(); // We need to reset because new duplicate chunks can be written above.

        opts.out_of_order_cap_max = 10;
        let db = open(&opts);
        assert_eq!(ooo_mint, db.head.min_ooo_time());
        assert_eq!(ooo_maxt, db.head.max_ooo_time());
        test_query(&db, &mut exp_samples);
    }

    {
        eprintln!("Restart DB with WBL+Mmap while having no m-map markers in WBL");
        reset_mmap_to_original(); // We need to reset because new duplicate chunks can be written above.

        // Removing m-map markers in WBL by rewriting it.
        let new_wbl_tmp = TempDir::new();
        let new_wbl = WL::new(new_wbl_tmp.path().join("new_wbl"), compression::Type::None).unwrap();
        let sr = wlog::new_segments_reader(&original_wbl_dir).unwrap();
        let dec = Decoder::new(record::Logger::nop());
        let mut r = wlog::Reader::new(sr);
        let (mut markers, mut added_recs) = (0, 0);
        while r.next() {
            let rec = r.record();
            if dec.r#type(rec) == record::Type::MmapMarkers {
                markers += 1;
                continue;
            }
            added_recs += 1;
            new_wbl.log(rec).unwrap();
        }
        assert!(markers > 0);
        assert!(added_recs > 0);
        new_wbl.close().unwrap();
        r.into_inner().close().unwrap();
        std::fs::remove_dir_all(&wbl_dir).unwrap();
        std::fs::rename(new_wbl.dir(), &wbl_dir).unwrap();

        opts.out_of_order_cap_max = 30;
        let db = open(&opts);
        assert_eq!(ooo_mint, db.head.min_ooo_time());
        assert_eq!(ooo_maxt, db.head.max_ooo_time());
        test_query(&db, &mut exp_samples);
    }
    drop(tmp);
}

// ---------------------------------------------------------------------------------------------------------------

/// Go `addSample` of the two OOO histogram compaction tests: appends one histogram (with the hint) and returns
/// the sample (a copy of what was appended).
fn add_hist_sample(db: &DB, float_histogram: bool, ts_ms: i64, l: &Labels, val: i64, hint: CounterResetHint) -> Sample {
    let mut app = db.appender_v2(&ctx());
    let o = AOptions::default();
    if float_histogram {
        let mut h = tu::generate_test_float_histogram(val);
        h.counter_reset_hint = hint;
        app.append(0, l, 0, ts_ms, 0.0, None, Some(&h), &o).unwrap();
        commit(&mut app);
        return Sample::float_histogram(0, ts_ms, h);
    }
    let mut h = tu::generate_test_histogram(val);
    h.counter_reset_hint = hint;
    app.append(0, l, 0, ts_ms, 0.0, Some(&h), None, &o).unwrap();
    commit(&mut app);
    Sample::histogram(0, ts_ms, h)
}

/// The names of the entries of a directory, sorted (Go `os.ReadDir`).
fn read_dir_sorted(dir: &Path) -> Vec<(String, u64)> {
    let mut v: Vec<(String, u64)> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (e.file_name().to_string_lossy().into_owned(), e.metadata().unwrap().len())
        })
        .collect();
    v.sort();
    v
}

#[test]
fn test_ooo_histogram_compaction_with_counter_resets_append_v2() {
    use CounterResetHint::{CounterReset, NotCounterReset as N, UnknownCounterReset as U};
    for float_histogram in [false, true] {
        eprintln!("floatHistogram={float_histogram}");
        let mut opts = opts_default();
        opts.out_of_order_cap_max = 30;
        opts.out_of_order_time_window = 500 * MINUTE;

        let db = new_test_db(vec![with_opts(opts)]);
        db.disable_compactions(); // We want to manually call it.

        let series1 = ls(&["foo", "bar1"]);
        let series2 = ls(&["foo", "bar2"]);

        let (mut s1_pre, mut s2_pre, mut s1_post, mut s2_post) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let add = |ts: i64, l: &Labels, val: i64, hint| add_hist_sample(&db, float_histogram, ts * MINUTE, l, val, hint);
        let cr = copy_with_counter_reset;

        // Add an in-order sample to each series.
        let mut s = add(520, &series1, 1000000, U);
        s1_pre.push(s.clone());
        s1_post.push(s.clone());

        s = add(520, &series2, 1000000, U);
        s2_pre.push(s.clone());
        s2_post.push(s.clone());

        // Verify that the in-memory ooo chunk is empty.
        require_no_ooo(&db, &series1);
        require_no_ooo(&db, &series2);

        // Add samples for series1. There are three head chunks that will be created:
        // Chunk 1 - Samples between 100 - 440. One explicit counter reset at ts 250.
        // Chunk 2 - Samples between 105 - 395. Overlaps with Chunk 1. One detected counter reset at ts 165.
        // Chunk 3 - Samples between 480 - 509. All within one block boundary. One detected counter reset at 490.

        // Chunk 1.
        // First add 10 samples.
        for i in (100..200).step_by(10) {
            s = add(i, &series1, 100000 + i, U);
            // Before compaction, all the samples have UnknownCounterReset even though they've been added to the
            // same chunk. This is because they overlap with the samples from chunk two and when merging two chunks
            // on read, the header is set as unknown when the next sample is not in the same chunk as the previous
            // one.
            s1_pre.push(s.clone());
            // After compaction, samples from multiple mmapped chunks will be merged, so there won't be any
            // overlapping chunks. Therefore, most samples will have the NotCounterReset header.
            // 100 is the first sample in the first chunk in the blocks, so is still set to UnknownCounterReset.
            // 120 is a block boundary - after compaction, 120 will be the first sample in a chunk, so is still set
            // to UnknownCounterReset.
            if i > 100 && i != 120 {
                s = cr(&s, N);
            }
            s1_post.push(s.clone());
        }
        // Explicit counter reset - the counter reset header is set to CounterReset but the value is higher than for
        // the previous timestamp. Explicit counter reset headers are actually ignored though, so when reading the
        // sample back you actually get unknown/not counter reset. This is as the chainSampleIterator ignores
        // existing headers and sets the header as UnknownCounterReset if the next sample is not in the same chunk
        // as the previous one, and counter resets always create a new chunk.
        // This case has been added to document what's happening, though it might not be the ideal behavior.
        s = add(250, &series1, 100000 + 250, CounterReset);
        s1_pre.push(cr(&s, U));
        s1_post.push(cr(&s, N));

        // Add 19 more samples to complete a chunk.
        for i in (260..450).step_by(10) {
            s = add(i, &series1, 100000 + i, U);
            // The samples with timestamp less than 410 overlap with the samples from chunk 2, so before
            // compaction, they're all UnknownCounterReset. Samples greater than or equal to 410 don't overlap with
            // other chunks so they're always detected as NotCounterReset pre and post compaction.
            if i >= 410 {
                s = cr(&s, N);
            }
            s1_pre.push(s.clone());
            // 360 is a block boundary, so after compaction its header is still UnknownCounterReset.
            if i != 360 {
                s = cr(&s, N);
            }
            s1_post.push(s.clone());
        }

        // Chunk 2.
        // Add six OOO samples.
        for i in (105..165).step_by(10) {
            s = add(i, &series1, 100000 + i, U);
            // Samples overlap with chunk 1 so before compaction all headers are UnknownCounterReset.
            s1_pre.push(s.clone());
            s1_post.push(cr(&s, N));
        }

        // Add sample that will be detected as a counter reset.
        s = add(165, &series1, 100000, U);
        // Before compaction, sample has an UnknownCounterReset header due to the chainSampleIterator.
        s1_pre.push(s.clone());
        // After compaction, the sample's counter reset is still UnknownCounterReset as we cannot trust
        // CounterReset headers in chunks at the moment, so when reading the first sample in a chunk, its hint is
        // set to UnknownCounterReset.
        s1_post.push(s.clone());

        // Add 23 more samples to complete a chunk.
        for i in (175..405).step_by(10) {
            s = add(i, &series1, 100000 + i, U);
            // Samples between 205-255 overlap with chunk 1 so before compaction those samples will have the
            // UnknownCounterReset header.
            if (205..255).contains(&i) {
                s = cr(&s, N);
            }
            s1_pre.push(s.clone());
            // 245 is the first sample >= the block boundary at 240, so it's still UnknownCounterReset after
            // compaction.
            if i != 245 {
                s = cr(&s, N);
            } else {
                s = cr(&s, U);
            }
            s1_post.push(s.clone());
        }

        // Chunk 3.
        for i in 480..490 {
            s = add(i, &series1, 100000 + i, U);
            // No overlapping samples in other chunks, so all other samples will already be detected as
            // NotCounterReset before compaction.
            if i > 480 {
                s = cr(&s, N);
            }
            s1_pre.push(s.clone());
            // 480 is block boundary.
            if i == 480 {
                s = cr(&s, U);
            }
            s1_post.push(s.clone());
        }
        // Counter reset.
        s = add(490, &series1, 100000, U);
        s1_pre.push(s.clone());
        s1_post.push(s.clone());
        // Add some more samples after the counter reset.
        for i in 491..510 {
            s = add(i, &series1, 100000 + i, U);
            s = cr(&s, N);
            s1_pre.push(s.clone());
            s1_post.push(s.clone());
        }

        // Add samples for series2 - one chunk with one detected counter reset at 300.
        for i in (200..300).step_by(10) {
            s = add(i, &series2, 100000 + i, U);
            if i > 200 {
                s = cr(&s, N);
            }
            s2_pre.push(s.clone());
            if i == 240 {
                s = cr(&s, U);
            }
            s2_post.push(s.clone());
        }
        // Counter reset.
        s = add(300, &series2, 100000, U);
        s2_pre.push(s.clone());
        s2_post.push(s.clone());
        // Add some more samples after the counter reset.
        for i in (310..500).step_by(10) {
            let mut s = add(i, &series2, 100000 + i, U);
            s = cr(&s, N);
            s2_pre.push(s.clone());
            // 360 and 480 are block boundaries.
            if i == 360 || i == 480 {
                s = cr(&s, U);
            }
            s2_post.push(s);
        }

        // Sort samples (as OOO samples not added in time-order).
        sort_by_t(&mut s1_pre);
        sort_by_t(&mut s1_post);
        sort_by_t(&mut s2_pre);
        sort_by_t(&mut s2_post);

        let verify_db_samples = |s1s: &[Sample], s2s: &[Sample]| {
            let exp = BTreeMap::from([(series1.to_string(), s1s.to_vec()), (series2.to_string(), s2s.to_vec())]);
            let q = db.querier(i64::MIN, i64::MAX).unwrap();
            let act = query(q, &[re("foo", "bar.*")]);
            require_equal_series(&exp, &act, false);
        };

        // Verify DB samples before compaction.
        verify_db_samples(&s1_pre, &s2_pre);

        // Verify that the in-memory ooo chunk is not empty.
        assert!(ooo_head_chunk_samples(&db, &series1) > 0);
        assert!(ooo_head_chunk_samples(&db, &series2) > 0);

        // No blocks before compaction.
        assert!(db.blocks().is_empty());

        // There is a 0th WBL file.
        let wbl = db.head.wbl().expect("wbl");
        wbl.sync().unwrap(); // syncing to make sure wbl is flushed in windows
        let files = read_dir_sorted(wbl.dir());
        assert_eq!(1, files.len());
        assert_eq!("00000000", files[0].0);
        assert!(files[0].1 > 100);

        // OOO compaction happens here.
        db.compact_ooo_head(&ctx()).unwrap();

        // Check that blocks are created after compaction.
        assert_eq!(5, db.blocks().len());

        // Check samples after compaction.
        verify_db_samples(&s1_post, &s2_post);

        // 0th WBL file will be deleted and 1st will be the only present.
        let files = read_dir_sorted(wbl.dir());
        assert_eq!(1, files.len());
        assert_eq!("00000001", files[0].0);
        assert_eq!(0, files[0].1);

        // OOO stuff should not be present in the Head now.
        require_no_ooo(&db, &series1);
        require_no_ooo(&db, &series2);

        let verify_block_samples = |block: &tsdb::block::Block, from_mins: i64, to_mins: i64| {
            let pick = |v: &[Sample]| -> Vec<Sample> {
                let mut out = Vec::new();
                for s in v {
                    if s.t >= from_mins * MINUTE {
                        // Samples should be sorted, so break out of loop when we reach a timestamp that's too big.
                        if s.t > to_mins * MINUTE {
                            break;
                        }
                        out.push(s.clone());
                    }
                }
                out
            };
            let (s1s, s2s) = (pick(&s1_post), pick(&s2_post));
            let mut exp = BTreeMap::new();
            if !s1s.is_empty() {
                exp.insert(series1.to_string(), s1s);
            }
            if !s2s.is_empty() {
                exp.insert(series2.to_string(), s2s);
            }
            let q = new_block_querier(block, i64::MIN, i64::MAX).unwrap();
            let act = query(q, &[re("foo", "bar.*")]);
            require_equal_series(&exp, &act, false);
        };

        // Checking for expected data in the blocks.
        let blocks = db.blocks();
        verify_block_samples(&blocks[0], 100, 119);
        verify_block_samples(&blocks[1], 120, 239);
        verify_block_samples(&blocks[2], 240, 359);
        verify_block_samples(&blocks[3], 360, 479);
        verify_block_samples(&blocks[4], 480, 509);

        // There should be a single m-map file.
        let mmap_dir = mmapped_chunks_dir(&db.head.opts.chunk_dir_root);
        assert_eq!(1, read_dir_sorted(&mmap_dir).len());

        // Compact the in-order head and expect another block.
        // Since this is a forced compaction, this block is not aligned with 2h.
        db.compact_head(&RangeHead::new(db.head.clone(), 500 * MINUTE, 550 * MINUTE)).unwrap();
        let blocks = db.blocks();
        assert_eq!(6, blocks.len());
        verify_block_samples(&blocks[5], 520, 520);

        // Blocks created out of normal and OOO head now. But not merged.
        verify_db_samples(&s1_post, &s2_post);

        // The compaction also clears out the old m-map files. Including the file that has ooo chunks.
        let files = read_dir_sorted(&mmap_dir);
        assert_eq!(1, files.len());
        assert_eq!("000001", files[0].0);

        // This will merge overlapping block.
        db.compact(&ctx()).unwrap();

        let blocks = db.blocks();
        assert_eq!(5, blocks.len());
        verify_block_samples(&blocks[0], 100, 119);
        verify_block_samples(&blocks[1], 120, 239);
        verify_block_samples(&blocks[2], 240, 359);
        verify_block_samples(&blocks[3], 360, 479);
        verify_block_samples(&blocks[4], 480, 520); // Merged block.

        // Final state. Blocks from normal and OOO head are merged.
        verify_db_samples(&s1_post, &s2_post);
    }
}

#[test]
fn test_interleaved_in_order_and_ooo_histogram_compaction_with_counter_resets_append_v2() {
    use CounterResetHint::{NotCounterReset as N, UnknownCounterReset as U};
    for float_histogram in [false, true] {
        eprintln!("floatHistogram={float_histogram}");
        let mut opts = opts_default();
        opts.out_of_order_cap_max = 30;
        opts.out_of_order_time_window = 500 * MINUTE;

        let db = new_test_db(vec![with_opts(opts)]);
        db.disable_compactions(); // We want to manually call it.

        let series1 = ls(&["foo", "bar1"]);
        let add = |ts: i64, val: i64| add_hist_sample(&db, float_histogram, ts, &series1, val, U);

        let mut exp_samples = Vec::new();
        let s = add(0, 0);
        exp_samples.push(s);
        let s = add(1, 10);
        exp_samples.push(copy_with_counter_reset(&s, N));
        let s = add(3, 3);
        exp_samples.push(copy_with_counter_reset(&s, U));
        let s = add(2, 0);
        exp_samples.push(copy_with_counter_reset(&s, U));

        // Sort samples (as OOO samples not added in time-order).
        sort_by_t(&mut exp_samples);

        let verify_db_samples = |s1s: &[Sample]| {
            let exp = BTreeMap::from([(series1.to_string(), s1s.to_vec())]);
            let q = db.querier(i64::MIN, i64::MAX).unwrap();
            let act = query(q, &[re("foo", "bar.*")]);
            require_equal_series(&exp, &act, false);
        };

        // Verify DB samples before compaction.
        verify_db_samples(&exp_samples);

        db.compact_ooo_head(&ctx()).unwrap();

        // Check samples after OOO compaction.
        verify_db_samples(&exp_samples);

        // Checking for expected data in the blocks.
        // Check that blocks are created after compaction.
        assert_eq!(1, db.blocks().len());

        // Compact the in-order head and expect another block.
        // Since this is a forced compaction, this block is not aligned with 2h.
        db.compact_head(&RangeHead::new(db.head.clone(), 0, 3)).unwrap();
        assert_eq!(2, db.blocks().len());

        // Blocks created out of normal and OOO head now. But not merged.
        verify_db_samples(&exp_samples);

        // This will merge overlapping block.
        db.compact(&ctx()).unwrap();

        assert_eq!(1, db.blocks().len());

        // Final state. Blocks from normal and OOO head are merged.
        verify_db_samples(&exp_samples);
    }
}

// ---------------------------------------------------------------------------------------------------------------

/// Go `mockCompactorFailing` (db_test.go) as the OOO compaction tests use it: `max` is 0, so every `Write` fails
/// before it writes a block.
struct MockCompactorFailing;

impl Compactor for MockCompactorFailing {
    fn plan(&self, _: &Path) -> Result<Vec<PathBuf>, Error> {
        Ok(Vec::new())
    }
    fn write(&self, _: &Path, _: &dyn tsdb::BlockReader, _: i64, _: i64, _: Option<&BlockMeta>) -> Result<Vec<Ulid>, Error> {
        Err(Error::msg("the compactor already did the maximum allowed blocks so it is time to fail"))
    }
    fn compact(&self, _: &Path, _: &[PathBuf], _: &[tsdb::block::Block]) -> Result<Vec<Ulid>, Error> {
        Ok(Vec::new())
    }
}

/// A compactor of closures (Go `mockCompactorFn`).
struct MockCompactorFn {
    plan_fn: Box<dyn Fn() -> Result<Vec<PathBuf>, Error> + Send + Sync>,
    compact_fn: Box<dyn Fn() -> Result<Vec<Ulid>, Error> + Send + Sync>,
    write_fn: Box<dyn Fn() -> Result<Vec<Ulid>, Error> + Send + Sync>,
}

impl Compactor for MockCompactorFn {
    fn plan(&self, _: &Path) -> Result<Vec<PathBuf>, Error> {
        (self.plan_fn)()
    }
    fn write(&self, _: &Path, _: &dyn tsdb::BlockReader, _: i64, _: i64, _: Option<&BlockMeta>) -> Result<Vec<Ulid>, Error> {
        (self.write_fn)()
    }
    fn compact(&self, _: &Path, _: &[PathBuf], _: &[tsdb::block::Block]) -> Result<Vec<Ulid>, Error> {
        (self.compact_fn)()
    }
}

#[test]
fn test_ooo_compaction_failure_append_v2() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        ooo_compaction_failure(&scenario);
    }
}

fn ooo_compaction_failure(scenario: &SampleTypeScenario) {
    let mut opts = opts_default();
    opts.out_of_order_cap_max = 30;
    opts.out_of_order_time_window = 300 * MINUTE;
    let db = new_test_db(vec![with_opts(opts)]);
    db.disable_compactions(); // We want to manually call it.

    let series1 = ls(&["foo", "bar1"]);

    let add_sample = |from_mins: i64, to_mins: i64| {
        let mut app = db.appender_v2(&ctx());
        for m in from_mins..=to_mins {
            let ts = m * MINUTE;
            scenario_append(scenario, app.as_mut(), &series1, ts, ts).0.unwrap();
        }
        commit(&mut app);
    };

    // Add an in-order samples.
    add_sample(250, 350);

    // Add ooo samples that creates multiple chunks.
    add_sample(90, 310);

    // No blocks before compaction.
    assert!(db.blocks().is_empty());

    // There is a 0th WBL file.
    let wbl = db.head.wbl().expect("wbl");
    let verify_first_wbl_file_is_0 = |count: usize| {
        wbl.sync().unwrap(); // Syncing to make sure wbl is flushed in windows.
        let files = read_dir_sorted(wbl.dir());
        assert_eq!(count, files.len());
        assert_eq!("00000000", files[0].0);
        assert!(files[0].1 > 100);
    };
    verify_first_wbl_file_is_0(1);

    let mmap_dir = mmapped_chunks_dir(&db.head.opts.chunk_dir_root);
    let verify_mmap_files = |exp: &[&str]| {
        let files = read_dir_sorted(&mmap_dir);
        assert_eq!(exp.len(), files.len());
        for (i, f) in files.iter().enumerate() {
            assert_eq!(exp[i], f.0);
        }
    };

    verify_mmap_files(&["000001"]);

    // OOO compaction fails 5 times.
    let original_compactor = db.compactor.read().unwrap().clone();
    *db.compactor.write().unwrap() = Arc::new(MockCompactorFailing);
    for _ in 0..5 {
        assert!(db.compact_ooo_head(&ctx()).is_err());
    }
    assert!(db.blocks().is_empty());

    // M-map files don't change after failed compaction.
    verify_mmap_files(&["000001"]);

    // Because of 5 compaction attempts, there are 6 files now.
    verify_first_wbl_file_is_0(6);

    *db.compactor.write().unwrap() = original_compactor;
    db.compact_ooo_head(&ctx()).unwrap();
    // adapted: Go compares the `*Block` values; the blocks are compared by their metas.
    let old_blocks: Vec<BlockMeta> = db.blocks().iter().map(|b| b.meta()).collect();
    assert_eq!(3, db.blocks().len());

    // Check that the ooo chunks were removed.
    require_no_ooo(&db, &series1);

    // The failed compaction should not have left the ooo Head corrupted.
    // Hence, expect no new blocks with another OOO compaction call.
    db.compact_ooo_head(&ctx()).unwrap();
    assert_eq!(3, db.blocks().len());
    assert_eq!(old_blocks, db.blocks().iter().map(|b| b.meta()).collect::<Vec<_>>());

    // There should be a single m-map file.
    verify_mmap_files(&["000001"]);

    // All but last WBL file will be deleted.
    // 8 files in total (starting at 0) because of 7 compaction calls.
    let files = read_dir_sorted(wbl.dir());
    assert_eq!(1, files.len());
    assert_eq!("00000007", files[0].0);
    assert_eq!(0, files[0].1);

    let verify_samples = |block: &tsdb::block::Block, from_mins: i64, to_mins: i64| {
        let s1s: Vec<Sample> = (from_mins..=to_mins).map(|m| (scenario.sample_func)(m * MINUTE, m * MINUTE)).collect();
        let exp = BTreeMap::from([(series1.to_string(), s1s)]);
        let q = new_block_querier(block, i64::MIN, i64::MAX).unwrap();
        let act = query(q, &[re("foo", "bar.*")]);
        require_equal_series(&exp, &act, true);
    };

    // Checking for expected data in the blocks.
    let blocks = db.blocks();
    verify_samples(&blocks[0], 90, 119);
    verify_samples(&blocks[1], 120, 239);
    verify_samples(&blocks[2], 240, 310);

    // Compact the in-order head and expect another block.
    // Since this is a forced compaction, this block is not aligned with 2h.
    db.compact_head(&RangeHead::new(db.head.clone(), 250 * MINUTE, 350 * MINUTE)).unwrap();
    let blocks = db.blocks();
    assert_eq!(4, blocks.len()); // [0, 120), [120, 240), [240, 360), [250, 351)
    verify_samples(&blocks[3], 250, 350);

    // The compaction also clears out the old m-map files. Including the file that has ooo chunks.
    verify_mmap_files(&["000001"]);
}

// ---------------------------------------------------------------------------------------------------------------

#[test]
fn test_wbl_corruption_append_v2() {
    let mut opts = opts_default();
    opts.out_of_order_cap_max = 30;
    opts.out_of_order_time_window = 300 * MINUTE;

    let mut db = new_test_db(vec![with_opts(opts.clone())]);
    let tmp = db.tmp.take().unwrap();

    let series1 = ls(&["foo", "bar1"]);
    let mut all_samples = Vec::new();
    let mut exp_after_restart = Vec::new();
    let mut add_samples = |db: &DB, from_mins: i64, to_mins: i64, after_restart: bool| {
        let mut app = db.appender_v2(&ctx());
        for m in from_mins..=to_mins {
            let ts = m * MINUTE;
            app.append(0, &series1, 0, ts, ts as f64, None, None, &AOptions::default()).unwrap();
            all_samples.push(Sample::float(0, ts, ts as f64));
            if after_restart {
                exp_after_restart.push(Sample::float(0, ts, ts as f64));
            }
        }
        commit(&mut app);
    };

    // Add an in-order samples.
    add_samples(&db, 340, 350, true);

    // OOO samples.
    add_samples(&db, 90, 99, true);
    add_samples(&db, 100, 119, true);
    add_samples(&db, 120, 130, true);

    // Moving onto the second file.
    let wbl = db.head.wbl().expect("wbl");
    wbl.next_segment().unwrap();

    // More OOO samples.
    add_samples(&db, 200, 230, true);
    add_samples(&db, 240, 255, true);

    // We corrupt WBL after the sample at 255. So everything added later should be deleted after replay.

    // Checking where we corrupt it.
    wbl.sync().unwrap(); // Syncing to make sure wbl is flushed in windows.
    let files = read_dir_sorted(wbl.dir());
    assert_eq!(2, files.len());
    let corrupt_index = files[1].1 as usize;
    let corrupt_file_path = wbl.dir().join(&files[1].0);

    // Corrupt the WBL by adding a malformed record.
    wbl.log(&[record::Type::Samples as u8, 99, 9, 99, 9, 99, 9, 99]).unwrap();

    // More samples after the corruption point.
    add_samples(&db, 260, 280, false);
    add_samples(&db, 290, 300, false);

    // Another file.
    wbl.next_segment().unwrap();

    add_samples(&db, 310, 320, false);

    // Verifying that we have data after corruption point.
    wbl.sync().unwrap(); // Syncing to make sure wbl is flushed in windows.
    let files = read_dir_sorted(wbl.dir());
    assert_eq!(3, files.len());
    assert!(files[1].1 as usize > corrupt_index);
    assert!(files[0].1 > 100);
    assert!(files[2].1 > 100);

    let verify_samples = |db: &DB, exp_samples: &mut Vec<Sample>| {
        sort_by_t(exp_samples);
        let exp_res = BTreeMap::from([(series1.to_string(), exp_samples.clone())]);
        let q = db.querier(i64::MIN, i64::MAX).unwrap();
        let act_res = query(q, &[re("foo", "bar.*")]);
        assert_eq!(exp_res, act_res);
    };

    verify_samples(&db, &mut all_samples);

    db.close().unwrap();
    let wbl_dir = wbl.dir().to_path_buf();
    drop(wbl);
    drop(db);

    // We want everything to be replayed from the WBL. So we delete the m-map files.
    std::fs::remove_dir_all(mmapped_chunks_dir(tmp.path())).unwrap();

    // Restart does the replay and repair.
    let db = new_test_db(vec![with_dir(tmp.path()), with_opts(opts.clone())]);
    assert_eq!(1.0, db.head.metrics.wal_corruptions_total.get());
    assert!(exp_after_restart.len() < all_samples.len());
    verify_samples(&db, &mut exp_after_restart);

    // Verify that it did the repair on disk.
    let files = read_dir_sorted(&wbl_dir);
    assert_eq!(3, files.len());
    assert!(files[0].1 > 100);
    assert_eq!(0, files[2].1);
    assert_eq!(corrupt_file_path, wbl_dir.join(&files[1].0));

    // Verifying that everything after the corruption point is set to 0.
    let b = std::fs::read(&corrupt_file_path).unwrap();
    let sum: u64 = b[corrupt_index..].iter().map(|&v| v as u64).sum();
    assert_eq!(0, sum);

    // Another restart, everything normal with no repair.
    db.close().unwrap();
    drop(db);
    let db = new_test_db(vec![with_dir(tmp.path()), with_opts(opts)]);
    assert_eq!(0.0, db.head.metrics.wal_corruptions_total.get());
    verify_samples(&db, &mut exp_after_restart);
    drop(db);
    drop(tmp);
}

// ---------------------------------------------------------------------------------------------------------------

#[test]
fn test_ooo_mmap_corruption_append_v2() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        ooo_mmap_corruption(&scenario);
    }
}

fn ooo_mmap_corruption(scenario: &SampleTypeScenario) {
    let mut opts = opts_default();
    opts.out_of_order_cap_max = 10;
    opts.out_of_order_time_window = 300 * MINUTE;

    let mut db = new_test_db(vec![with_opts(opts.clone())]);
    let tmp = db.tmp.take().unwrap();

    let series1 = ls(&["foo", "bar1"]);
    let mut all_samples = Vec::new();
    let mut exp_in_mmap_chunks = Vec::new();
    let mut add_samples = |db: &DB, from_mins: i64, to_mins: i64, in_mmap_after_corruption: bool| {
        let mut app = db.appender_v2(&ctx());
        for m in from_mins..=to_mins {
            let ts = m * MINUTE;
            let (r, s) = scenario_append(scenario, app.as_mut(), &series1, ts, ts);
            r.unwrap();
            if in_mmap_after_corruption {
                exp_in_mmap_chunks.push(s.clone());
            }
            all_samples.push(s);
        }
        commit(&mut app);
    };

    // Add an in-order samples.
    add_samples(&db, 340, 350, true);

    // OOO samples.
    add_samples(&db, 90, 99, true);
    add_samples(&db, 100, 109, true);
    // This sample m-maps a chunk. But 120 goes into a new chunk.
    add_samples(&db, 120, 120, false);

    // Second m-map file. We will corrupt this file. Sample 120 goes into this new file.
    db.head.chunk_disk_mapper.cut_new_file();

    // More OOO samples.
    add_samples(&db, 200, 230, false);
    add_samples(&db, 240, 255, false);

    db.head.chunk_disk_mapper.cut_new_file();
    add_samples(&db, 260, 290, false);

    let verify_samples = |db: &DB, exp_samples: &mut Vec<Sample>| {
        sort_by_t(exp_samples);
        let exp_res = BTreeMap::from([(series1.to_string(), exp_samples.clone())]);
        let q = db.querier(i64::MIN, i64::MAX).unwrap();
        let act_res = query(q, &[re("foo", "bar.*")]);
        require_equal_series(&exp_res, &act_res, true);
    };

    verify_samples(&db, &mut all_samples);

    // Verifying existing files.
    let mmap_dir = mmapped_chunks_dir(&db.head.opts.chunk_dir_root);
    let files = read_dir_sorted(&mmap_dir);
    assert_eq!(3, files.len());

    // Corrupting the 2nd file.
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new().read(true).write(true).open(mmap_dir.join(&files[1].0)).unwrap();
        f.seek(SeekFrom::Start(20)).unwrap();
        f.write_all(&[99, 9, 99, 9, 99]).unwrap();
    }
    let first_file_name = files[0].0.clone();

    let wbl_dir = db.head.wbl().expect("wbl").dir().to_path_buf();
    db.close().unwrap();
    drop(db);

    // Moving OOO WBL to use it later.
    let wbl_tmp = TempDir::new();
    let wbl_dir_tmp = wbl_tmp.path().join("wbl_tmp");
    std::fs::rename(&wbl_dir, &wbl_dir_tmp).unwrap();

    // Restart does the replay and repair of m-map files.
    let db = new_test_db(vec![with_dir(tmp.path()), with_opts(opts.clone())]);
    assert_eq!(1.0, db.head.metrics.mmap_chunk_corruption_total.get());
    assert!(exp_in_mmap_chunks.len() < all_samples.len());

    // Since there is no WBL, only samples from m-map chunks comes in the query.
    verify_samples(&db, &mut exp_in_mmap_chunks);

    // Verify that it did the repair on disk. All files from the point of corruption should be deleted.
    let files = read_dir_sorted(&mmap_dir);
    assert_eq!(1, files.len());
    assert!(files[0].1 > 100);
    assert_eq!(first_file_name, files[0].0);

    // Another restart, everything normal with no repair.
    db.close().unwrap();
    drop(db);
    let db = new_test_db(vec![with_dir(tmp.path()), with_opts(opts.clone())]);
    assert_eq!(0.0, db.head.metrics.mmap_chunk_corruption_total.get());
    verify_samples(&db, &mut exp_in_mmap_chunks);

    // Restart again with the WBL, all samples should be present now.
    db.close().unwrap();
    drop(db);
    std::fs::remove_dir_all(&wbl_dir).unwrap();
    std::fs::rename(&wbl_dir_tmp, &wbl_dir).unwrap();
    let db = new_test_db(vec![with_dir(tmp.path()), with_opts(opts)]);
    verify_samples(&db, &mut all_samples);
    drop(db);
    drop(tmp);
}

// ---------------------------------------------------------------------------------------------------------------

#[test]
fn test_out_of_order_runtime_config_append_v2() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        out_of_order_runtime_config(&scenario);
    }
}

fn make_ooo_config(ooo_time_window: i64) -> ApplyConfig {
    ApplyConfig {
        tsdb: Some(TsdbConfig {
            out_of_order_time_window: ooo_time_window * MINUTE,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn out_of_order_runtime_config(scenario: &SampleTypeScenario) {
    let get_db = |ooo_time_window: i64| {
        let mut opts = opts_default();
        opts.out_of_order_time_window = ooo_time_window;
        let db = new_test_db(vec![with_opts(opts)]);
        db.disable_compactions();
        db
    };

    let series1 = ls(&["foo", "bar1"]);
    let add_samples = |db: &DB, from_mins: i64, to_mins: i64, success: bool, all: &mut Vec<Sample>| {
        let mut app = db.appender_v2(&ctx());
        for m in from_mins..=to_mins {
            let ts = m * MINUTE;
            let (r, s) = scenario_append(scenario, app.as_mut(), &series1, ts, ts);
            if success {
                r.unwrap();
                all.push(s);
            } else {
                assert!(r.is_err());
            }
        }
        commit(&mut app);
    };

    let verify_samples = |db: &DB, exp_samples: &mut Vec<Sample>| {
        sort_by_t(exp_samples);
        let exp_res = BTreeMap::from([(series1.to_string(), exp_samples.clone())]);
        let q = db.querier(i64::MIN, i64::MAX).unwrap();
        let act_res = query(q, &[re("foo", "bar.*")]);
        require_equal_series(&exp_res, &act_res, true);
    };

    let do_ooo_compaction = |db: &DB| {
        // WBL is not empty.
        let size = db.head.wbl().expect("wbl").size().unwrap();
        assert!(size > 0);

        assert!(db.blocks().is_empty());
        db.compact_ooo_head(&ctx()).unwrap();
        assert!(!db.blocks().is_empty());

        // WBL is empty.
        let size = db.head.wbl().expect("wbl").size().unwrap();
        assert_eq!(0, size);
    };

    let wbl_ptr = |db: &DB| db.head.wbl().map(|w| Arc::as_ptr(&w) as usize);

    {
        eprintln!("increase time window");
        let mut all = Vec::new();
        let db = get_db(30 * MINUTE);

        // In-order.
        add_samples(&db, 300, 310, true, &mut all);

        // OOO up to 30m old is success.
        add_samples(&db, 281, 290, true, &mut all);

        // OOO of 59m old fails.
        let mut s = Vec::new();
        add_samples(&db, 251, 260, false, &mut s);
        assert!(s.is_empty());
        verify_samples(&db, &mut all);

        let old_wbl_ptr = wbl_ptr(&db);

        // Increase time window and try adding again.
        db.apply_config(&make_ooo_config(60)).unwrap();
        add_samples(&db, 251, 260, true, &mut all);

        // WBL does not change.
        assert_eq!(old_wbl_ptr, wbl_ptr(&db));

        do_ooo_compaction(&db);
        verify_samples(&db, &mut all);
    }

    {
        eprintln!("decrease time window and increase again");
        let mut all = Vec::new();
        let db = get_db(60 * MINUTE);

        // In-order.
        add_samples(&db, 300, 310, true, &mut all);

        // OOO up to 59m old is success.
        add_samples(&db, 251, 260, true, &mut all);

        let old_wbl_ptr = wbl_ptr(&db);
        // Decrease time window.
        db.apply_config(&make_ooo_config(30)).unwrap();

        // OOO of 49m old fails.
        let mut s = Vec::new();
        add_samples(&db, 261, 270, false, &mut s);
        assert!(s.is_empty());

        // WBL does not change.
        assert_eq!(old_wbl_ptr, wbl_ptr(&db));

        verify_samples(&db, &mut all);

        // Increase time window again and check
        db.apply_config(&make_ooo_config(60)).unwrap();
        add_samples(&db, 261, 270, true, &mut all);
        verify_samples(&db, &mut all);

        // WBL does not change.
        assert_eq!(old_wbl_ptr, wbl_ptr(&db));

        do_ooo_compaction(&db);
        verify_samples(&db, &mut all);
    }

    {
        eprintln!("disabled to enabled");
        let mut all = Vec::new();
        let db = get_db(0);

        // In-order.
        add_samples(&db, 300, 310, true, &mut all);

        // OOO fails.
        let mut s = Vec::new();
        add_samples(&db, 251, 260, false, &mut s);
        assert!(s.is_empty());
        verify_samples(&db, &mut all);

        assert!(db.head.wbl().is_none());

        // Increase time window and try adding again.
        db.apply_config(&make_ooo_config(60)).unwrap();
        add_samples(&db, 251, 260, true, &mut all);

        // WBL gets created.
        assert!(db.head.wbl().is_some());

        verify_samples(&db, &mut all);

        // OOO compaction works now.
        do_ooo_compaction(&db);
        verify_samples(&db, &mut all);
    }

    {
        eprintln!("enabled to disabled");
        let mut all = Vec::new();
        let db = get_db(60 * MINUTE);

        // In-order.
        add_samples(&db, 300, 310, true, &mut all);

        // OOO up to 59m old is success.
        add_samples(&db, 251, 260, true, &mut all);

        let old_wbl_ptr = wbl_ptr(&db);
        // Time Window to 0, hence disabled.
        db.apply_config(&make_ooo_config(0)).unwrap();

        // OOO within old time window fails.
        let mut s = Vec::new();
        add_samples(&db, 290, 309, false, &mut s);
        assert!(s.is_empty());

        // WBL does not change and is not removed.
        assert_eq!(old_wbl_ptr, wbl_ptr(&db));

        verify_samples(&db, &mut all);

        // Compaction still works after disabling with WBL cleanup.
        do_ooo_compaction(&db);
        verify_samples(&db, &mut all);
    }

    {
        eprintln!("disabled to disabled");
        let mut all = Vec::new();
        let db = get_db(0);

        // In-order.
        add_samples(&db, 300, 310, true, &mut all);

        // OOO fails.
        let mut s = Vec::new();
        add_samples(&db, 290, 309, false, &mut s);
        assert!(s.is_empty());
        verify_samples(&db, &mut all);
        assert!(db.head.wbl().is_none());

        // Time window to 0.
        db.apply_config(&make_ooo_config(0)).unwrap();

        // OOO still fails.
        add_samples(&db, 290, 309, false, &mut s);
        assert!(s.is_empty());
        verify_samples(&db, &mut all);
        assert!(db.head.wbl().is_none());
    }
}

// ---------------------------------------------------------------------------------------------------------------

#[test]
fn test_no_gap_after_restart_with_ooo_append_v2() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        no_gap_after_restart_with_ooo(&scenario);
    }
}

fn no_gap_after_restart_with_ooo(scenario: &SampleTypeScenario) {
    let series1 = ls(&["foo", "bar1"]);
    let add_samples = |db: &DB, from_mins: i64, to_mins: i64, success: bool| {
        let mut app = db.appender_v2(&ctx());
        for m in from_mins..=to_mins {
            let ts = m * MINUTE;
            let (r, _) = scenario_append(scenario, app.as_mut(), &series1, ts, ts);
            if success {
                r.unwrap();
            } else {
                assert!(r.is_err());
            }
        }
        commit(&mut app);
    };

    let verify_samples = |db: &DB, from_mins: i64, to_mins: i64| {
        let exp: Vec<Sample> = (from_mins..=to_mins).map(|m| (scenario.sample_func)(m * MINUTE, m * MINUTE)).collect();
        let exp_res = BTreeMap::from([(series1.to_string(), exp)]);
        let q = db.querier(i64::MIN, i64::MAX).unwrap();
        let act_res = query(q, &[re("foo", "bar.*")]);
        require_equal_series(&exp_res, &act_res, true);
    };

    struct Case {
        in_order_mint: i64,
        in_order_maxt: i64,
        ooo_mint: i64,
        ooo_maxt: i64,
        // After compaction.
        block_ranges: Vec<[i64; 2]>,
        head_mint: i64,
        head_maxt: i64,
    }
    let cases = [
        Case {
            in_order_mint: 300,
            in_order_maxt: 490,
            ooo_mint: 489,
            ooo_maxt: 489,
            block_ranges: vec![[300, 360], [480, 600]],
            head_mint: 360,
            head_maxt: 490,
        },
        Case {
            in_order_mint: 300,
            in_order_maxt: 490,
            ooo_mint: 479,
            ooo_maxt: 479,
            block_ranges: vec![[300, 360], [360, 480]],
            head_mint: 360,
            head_maxt: 490,
        },
    ];

    for (i, c) in cases.iter().enumerate() {
        eprintln!("case={i}");
        let mut opts = opts_default();
        opts.out_of_order_time_window = 30 * MINUTE;
        let mut db = new_test_db(vec![with_opts(opts.clone())]);
        db.disable_compactions();
        let tmp = db.tmp.take().unwrap();

        // 3h10m=190m worth in-order data.
        add_samples(&db, c.in_order_mint, c.in_order_maxt, true);
        verify_samples(&db, c.in_order_mint, c.in_order_maxt);

        // One ooo samples.
        add_samples(&db, c.ooo_mint, c.ooo_maxt, true);
        verify_samples(&db, c.in_order_mint, c.in_order_maxt);

        // We get 2 blocks. 1 from OOO, 1 from in-order.
        db.compact(&ctx()).unwrap();
        let verify_block_ranges = |db: &DB| {
            let blocks = db.blocks();
            assert_eq!(c.block_ranges.len(), blocks.len());
            for (j, br) in c.block_ranges.iter().enumerate() {
                assert_eq!(br[0] * MINUTE, blocks[j].min_time());
                assert_eq!(br[1] * MINUTE, blocks[j].max_time());
            }
        };
        verify_block_ranges(&db);
        assert_eq!(c.head_mint * MINUTE, db.head.min_time());
        assert_eq!(c.head_maxt * MINUTE, db.head.max_time());

        // Restart and expect all samples to be present.
        db.close().unwrap();
        drop(db);

        let db = new_test_db(vec![with_dir(tmp.path()), with_opts(opts)]);
        db.disable_compactions();

        verify_block_ranges(&db);
        assert_eq!(c.head_mint * MINUTE, db.head.min_time());
        assert_eq!(c.head_maxt * MINUTE, db.head.max_time());
        verify_samples(&db, c.in_order_mint, c.in_order_maxt);
        drop(db);
        drop(tmp);
    }
}

// ---------------------------------------------------------------------------------------------------------------

/// Appends 290..300 in order and 250..260 out of order (Go `addSamples` of the next two tests).
fn add_in_order_and_ooo(db: &DB, scenario: &SampleTypeScenario, series1: &Labels, all: &mut Vec<Sample>) {
    for (from_mins, to_mins) in [(290, 300), (250, 260)] {
        let mut app = db.appender_v2(&ctx());
        for m in from_mins..=to_mins {
            let ts = m * MINUTE;
            let (r, s) = scenario_append(scenario, app.as_mut(), series1, ts, ts);
            r.unwrap();
            all.push(s);
        }
        commit(&mut app);
    }
}

#[test]
fn test_wbl_replay_after_ooo_disable_and_restart_append_v2() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        let mut opts = opts_default();
        opts.out_of_order_time_window = 60 * MINUTE;

        let mut db = new_test_db(vec![with_opts(opts.clone())]);
        let tmp = db.tmp.take().unwrap();

        let series1 = ls(&["foo", "bar1"]);
        let mut all_samples = Vec::new();
        // In-order samples, then OOO samples.
        add_in_order_and_ooo(&db, &scenario, &series1, &mut all_samples);

        let verify_samples = |db: &DB, exp_samples: &mut Vec<Sample>| {
            sort_by_t(exp_samples);
            let exp_res = BTreeMap::from([(series1.to_string(), exp_samples.clone())]);
            let q = db.querier(i64::MIN, i64::MAX).unwrap();
            let act_res = query(q, &[re("foo", "bar.*")]);
            require_equal_series(&exp_res, &act_res, true);
        };

        verify_samples(&db, &mut all_samples);

        // Restart DB with OOO disabled.
        db.close().unwrap();
        drop(db);

        opts.out_of_order_time_window = 0;
        let db = new_test_db(vec![with_dir(tmp.path()), with_opts(opts)]);

        // We can still query OOO samples when OOO is disabled.
        verify_samples(&db, &mut all_samples);
        drop(db);
        drop(tmp);
    }
}

#[test]
fn test_panic_on_apply_config_append_v2() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        let mut opts = opts_default();
        opts.out_of_order_time_window = 60 * MINUTE;

        let mut db = new_test_db(vec![with_opts(opts.clone())]);
        let tmp = db.tmp.take().unwrap();

        let series1 = ls(&["foo", "bar1"]);
        let mut all_samples = Vec::new();
        // In-order samples, then OOO samples.
        add_in_order_and_ooo(&db, &scenario, &series1, &mut all_samples);

        // Restart DB with OOO disabled.
        db.close().unwrap();
        drop(db);

        opts.out_of_order_time_window = 0;
        let db = new_test_db(vec![with_dir(tmp.path()), with_opts(opts)]);

        // ApplyConfig with OOO enabled and expect no panic.
        db.apply_config(&make_ooo_config(60)).unwrap();
        drop(db);
        drop(tmp);
    }
}

// ---------------------------------------------------------------------------------------------------------------

#[test]
fn test_histogram_append_and_query_append_v2() {
    eprintln!("integer histograms");
    histogram_append_and_query_helper(false);
    eprintln!("float histograms");
    histogram_append_and_query_helper(true);
}

fn histogram_append_and_query_helper(float_histogram: bool) {
    use CounterResetHint::{NotCounterReset as N, UnknownCounterReset as U};
    let db = new_test_db(vec![]);
    let minute = |m: i64| m * MINUTE;

    let append_histogram = |lbls: &Labels, ts_minute: i64, h: &Histogram, exp: &mut Vec<Sample>, exp_crh| {
        let mut app = db.appender_v2(&ctx());
        let r = if float_histogram {
            let r = app.append(0, lbls, 0, minute(ts_minute), 0.0, None, Some(&h.to_float()), &AOptions::default());
            let mut efh = h.to_float();
            efh.counter_reset_hint = exp_crh;
            exp.push(Sample::float_histogram(0, minute(ts_minute), efh));
            r
        } else {
            let r = app.append(0, lbls, 0, minute(ts_minute), 0.0, Some(h), None, &AOptions::default());
            let mut eh = h.clone();
            eh.counter_reset_hint = exp_crh;
            exp.push(Sample::histogram(0, minute(ts_minute), eh));
            r
        };
        r.unwrap();
        commit(&mut app);
    };
    let append_float = |lbls: &Labels, ts_minute: i64, val: f64, exp: &mut Vec<Sample>| {
        let mut app = db.appender_v2(&ctx());
        app.append(0, lbls, 0, minute(ts_minute), val, None, None, &AOptions::default()).unwrap();
        commit(&mut app);
        exp.push(Sample::float(0, minute(ts_minute), val));
    };

    let test_query = |name: &str, value: &str, exp: BTreeMap<String, Vec<Sample>>| {
        let q = db.querier(i64::MIN, i64::MAX).unwrap();
        let act = query(q, &[re(name, value)]);
        assert_eq!(exp, act);
    };

    let base_h = Histogram {
        count: 15,
        zero_count: 4,
        zero_threshold: 0.001,
        sum: 35.5,
        schema: 1,
        positive_spans: vec![Span { offset: 0, length: 2 }, Span { offset: 2, length: 2 }],
        positive_buckets: vec![1, 1, -1, 0],
        negative_spans: vec![Span { offset: 0, length: 1 }, Span { offset: 1, length: 2 }],
        negative_buckets: vec![1, 2, -1],
        ..Default::default()
    };

    let series1 = ls(&["foo", "bar1"]);
    let series2 = ls(&["foo", "bar2"]);
    let series3 = ls(&["foo", "bar3"]);
    let series4 = ls(&["foo", "bar4"]);
    let (mut exp1, mut exp2, mut exp3, mut exp4) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let m1 = |exp: &Vec<Sample>| BTreeMap::from([(series1.to_string(), exp.clone())]);

    // TODO(codesome): test everything for negative buckets as well.
    {
        eprintln!("series with only histograms");
        let mut h = base_h.clone(); // This is shared across all sub tests.

        append_histogram(&series1, 100, &h, &mut exp1, U);
        test_query("foo", "bar1", m1(&exp1));

        h.positive_buckets[0] += 1;
        h.negative_buckets[0] += 2;
        h.count += 10;
        append_histogram(&series1, 101, &h, &mut exp1, N);
        test_query("foo", "bar1", m1(&exp1));

        {
            eprintln!("changing schema");
            h.schema = 2;
            append_histogram(&series1, 102, &h, &mut exp1, U);
            test_query("foo", "bar1", m1(&exp1));

            // Schema back to old.
            h.schema = 1;
            append_histogram(&series1, 103, &h, &mut exp1, U);
            test_query("foo", "bar1", m1(&exp1));
        }

        {
            eprintln!("new buckets incoming");
            // In the previous unit test, during the last histogram append, we changed the schema and that caused
            // a new chunk creation. Because of the next append the layout of the last histogram will change because
            // the chunk will be re-encoded. So this forces us to modify the last histogram in exp1 so when we query
            // we get the expected results.
            let l = exp1.len();
            if float_histogram {
                let lh = exp1[l - 1].fh.as_mut().unwrap();
                lh.positive_spans[1].length += 1;
                lh.positive_buckets.push(0.0);
            } else {
                let lh = exp1[l - 1].h.as_mut().unwrap();
                lh.positive_spans[1].length += 1;
                lh.positive_buckets.push(-2); // -2 makes the last bucket 0.
            }

            // This histogram with new bucket at the end causes the re-encoding of the previous histogram.
            // Hence the previous histogram is recoded into this new layout.
            // But the query returns the histogram from the in-memory buffer, hence we don't see the recode here yet.
            h.positive_spans[1].length += 1;
            h.positive_buckets.push(1);
            h.count += 3;
            append_histogram(&series1, 104, &h, &mut exp1, N);
            test_query("foo", "bar1", m1(&exp1));

            // Because of the previous two histograms being on the active chunk, and the next append is only adding a
            // new bucket, the active chunk will be re-encoded to the new layout.
            let l = exp1.len();
            if float_histogram {
                let lh = exp1[l - 2].fh.as_mut().unwrap();
                lh.positive_spans[0].length += 1;
                lh.positive_spans[1].offset -= 1;
                lh.positive_buckets = vec![2.0, 3.0, 0.0, 2.0, 2.0, 0.0];

                let lh = exp1[l - 1].fh.as_mut().unwrap();
                lh.positive_spans[0].length += 1;
                lh.positive_spans[1].offset -= 1;
                lh.positive_buckets = vec![2.0, 3.0, 0.0, 2.0, 2.0, 3.0];
            } else {
                let lh = exp1[l - 2].h.as_mut().unwrap();
                lh.positive_spans[0].length += 1;
                lh.positive_spans[1].offset -= 1;
                lh.positive_buckets = vec![2, 1, -3, 2, 0, -2];

                let lh = exp1[l - 1].h.as_mut().unwrap();
                lh.positive_spans[0].length += 1;
                lh.positive_spans[1].offset -= 1;
                lh.positive_buckets = vec![2, 1, -3, 2, 0, 1];
            }

            // Now we add the new buckets in between. Empty bucket is again not present for the old histogram.
            h.positive_spans[0].length += 1;
            h.positive_spans[1].offset -= 1;
            h.count += 3;
            // {2, 1, -1, 0, 1} -> {2, 1, 0, -1, 0, 1}
            h.positive_buckets.insert(2, 0);
            append_histogram(&series1, 105, &h, &mut exp1, N);
            test_query("foo", "bar1", m1(&exp1));

            // We add 4 more histograms to clear out the buffer and see the re-encoded histograms.
            append_histogram(&series1, 106, &h, &mut exp1, N);
            append_histogram(&series1, 107, &h, &mut exp1, N);
            append_histogram(&series1, 108, &h, &mut exp1, N);
            append_histogram(&series1, 109, &h, &mut exp1, N);

            // Update the expected histograms to reflect the re-encoding.
            let l = exp1.len();
            if float_histogram {
                let spans = exp1[l - 1].fh.as_ref().unwrap().positive_spans.clone();
                let h7 = exp1[l - 7].fh.as_mut().unwrap();
                h7.positive_spans = spans.clone();
                h7.positive_buckets = vec![2.0, 3.0, 0.0, 2.0, 2.0, 0.0];

                let h6 = exp1[l - 6].fh.as_mut().unwrap();
                h6.positive_spans = spans;
                h6.positive_buckets = vec![2.0, 3.0, 0.0, 2.0, 2.0, 3.0];
            } else {
                let spans = exp1[l - 1].h.as_ref().unwrap().positive_spans.clone();
                let h7 = exp1[l - 7].h.as_mut().unwrap();
                h7.positive_spans = spans.clone();
                h7.positive_buckets = vec![2, 1, -3, 2, 0, -2]; // -3 and -2 are the empty buckets.

                let h6 = exp1[l - 6].h.as_mut().unwrap();
                h6.positive_spans = spans;
                h6.positive_buckets = vec![2, 1, -3, 2, 0, 1]; // -3 is the empty bucket.
            }

            test_query("foo", "bar1", m1(&exp1));
        }

        {
            eprintln!("buckets disappearing");
            h.positive_spans[1].length -= 1;
            h.positive_buckets.pop();
            h.count -= 3;
            append_histogram(&series1, 110, &h, &mut exp1, U);
            test_query("foo", "bar1", m1(&exp1));
        }
    }

    {
        eprintln!("series starting with float and then getting histograms");
        append_float(&series2, 100, 100.0, &mut exp2);
        append_float(&series2, 101, 101.0, &mut exp2);
        append_float(&series2, 102, 102.0, &mut exp2);
        test_query("foo", "bar2", BTreeMap::from([(series2.to_string(), exp2.clone())]));

        let h = base_h.clone();
        append_histogram(&series2, 103, &h, &mut exp2, U);
        append_histogram(&series2, 104, &h, &mut exp2, N);
        append_histogram(&series2, 105, &h, &mut exp2, N);
        test_query("foo", "bar2", BTreeMap::from([(series2.to_string(), exp2.clone())]));

        // Switching between float and histograms again.
        append_float(&series2, 106, 106.0, &mut exp2);
        append_float(&series2, 107, 107.0, &mut exp2);
        test_query("foo", "bar2", BTreeMap::from([(series2.to_string(), exp2.clone())]));

        append_histogram(&series2, 108, &h, &mut exp2, U);
        append_histogram(&series2, 109, &h, &mut exp2, N);
        test_query("foo", "bar2", BTreeMap::from([(series2.to_string(), exp2.clone())]));
    }

    {
        eprintln!("series starting with histogram and then getting float");
        let h = base_h.clone();
        append_histogram(&series3, 101, &h, &mut exp3, U);
        append_histogram(&series3, 102, &h, &mut exp3, N);
        append_histogram(&series3, 103, &h, &mut exp3, N);
        test_query("foo", "bar3", BTreeMap::from([(series3.to_string(), exp3.clone())]));

        append_float(&series3, 104, 100.0, &mut exp3);
        append_float(&series3, 105, 101.0, &mut exp3);
        append_float(&series3, 106, 102.0, &mut exp3);
        test_query("foo", "bar3", BTreeMap::from([(series3.to_string(), exp3.clone())]));

        // Switching between histogram and float again.
        append_histogram(&series3, 107, &h, &mut exp3, U);
        append_histogram(&series3, 108, &h, &mut exp3, N);
        test_query("foo", "bar3", BTreeMap::from([(series3.to_string(), exp3.clone())]));

        append_float(&series3, 109, 106.0, &mut exp3);
        append_float(&series3, 110, 107.0, &mut exp3);
        test_query("foo", "bar3", BTreeMap::from([(series3.to_string(), exp3.clone())]));
    }

    {
        eprintln!("query mix of histogram and float series");
        // A float only series.
        append_float(&series4, 100, 100.0, &mut exp4);
        append_float(&series4, 101, 101.0, &mut exp4);
        append_float(&series4, 102, 102.0, &mut exp4);

        test_query(
            "foo",
            "bar.*",
            BTreeMap::from([
                (series1.to_string(), exp1.clone()),
                (series2.to_string(), exp2.clone()),
                (series3.to_string(), exp3.clone()),
                (series4.to_string(), exp4.clone()),
            ]),
        );
    }
}

// ---------------------------------------------------------------------------------------------------------------

#[test]
fn test_ooo_native_histograms_settings_append_v2() {
    let h = Histogram {
        count: 9,
        zero_count: 4,
        zero_threshold: 0.001,
        sum: 35.5,
        schema: 1,
        positive_spans: vec![Span { offset: 0, length: 2 }, Span { offset: 2, length: 2 }],
        positive_buckets: vec![1, 1, -1, 0],
        ..Default::default()
    };

    let l = ls(&["foo", "bar"]);
    let o = AOptions::default();

    {
        eprintln!("Test OOO native histograms if OOO is disabled and Native Histograms is enabled");
        let mut opts = opts_default();
        opts.out_of_order_time_window = 0;
        let db = new_test_db(vec![with_opts(opts), with_rngs(&[100])]);

        let mut app = db.appender_v2(&ctx());
        app.append(0, &l, 0, 100, 0.0, Some(&h), None, &o).unwrap();
        app.append(0, &l, 0, 50, 0.0, Some(&h), None, &o).unwrap(); // The OOO sample is not detected until it is committed, so no error is returned
        commit(&mut app);

        let q = db.querier(i64::MIN, i64::MAX).unwrap();
        let act = query(q, &[eq("foo", "bar")]);
        assert_eq!(BTreeMap::from([(l.to_string(), vec![Sample::histogram(0, 100, h.clone())])]), act);
    }
    {
        eprintln!("Test OOO native histograms when both OOO and Native Histograms are enabled");
        let mut opts = opts_default();
        opts.out_of_order_time_window = 100;
        let db = new_test_db(vec![with_opts(opts), with_rngs(&[100])]);

        // Add in-order samples
        let mut app = db.appender_v2(&ctx());
        app.append(0, &l, 0, 200, 0.0, Some(&h), None, &o).unwrap();

        // Add OOO samples
        app.append(0, &l, 0, 100, 0.0, Some(&h), None, &o).unwrap();
        app.append(0, &l, 0, 150, 0.0, Some(&h), None, &o).unwrap();
        commit(&mut app);

        let q = db.querier(i64::MIN, i64::MAX).unwrap();
        let act = query(q, &[eq("foo", "bar")]);
        require_equal_series(
            &BTreeMap::from([(
                l.to_string(),
                vec![
                    Sample::histogram(0, 100, h.clone()),
                    Sample::histogram(0, 150, h.clone()),
                    Sample::histogram(0, 200, h.clone()),
                ],
            )]),
            &act,
            true,
        );
    }
}

// ---------------------------------------------------------------------------------------------------------------

/// TestChunkQuerierReadWriteRace_AppendV2 looks for any possible race between appending samples and reading
/// chunks because the head chunk that is being appended to can be read in parallel and we should be able to make
/// a copy of the chunk without worrying about the parallel write.
#[test]
fn test_chunk_querier_read_write_race_append_v2() {
    let db = new_test_db(vec![]);

    let lbls = ls(&["foo", "bar"]);

    let writer = {
        let db = db.db.clone();
        let lbls = lbls.clone();
        move || -> Option<Error> {
            std::thread::sleep(std::time::Duration::from_millis(5)); // Initial pause while readers start.
            let mut ts = 0i64;
            for _ in 0..500 {
                let mut app = db.appender_v2(&ctx());
                for _ in 0..10 {
                    ts += 1;
                    if let Err(e) = app.append(0, &lbls, 0, ts, (ts * 100) as f64, None, None, &AOptions::default()) {
                        return Some(e);
                    }
                }
                if let Some(e) = app.commit() {
                    return Some(e);
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            None
        }
    };

    let reader = || {
        let mut querier = db.chunk_querier(i64::MIN, i64::MAX).unwrap();
        {
            let mut ss = querier.select(&ctx(), false, None, &[eq("foo", "bar")]);
            while ss.next() {
                let cs = ss.at().clone();
                let mut it = cs.iterator(None);
                while it.next() {
                    let m = it.at();
                    let _ = m.chunk.as_ref().expect("chunk").bytes().to_vec(); // This copying of chunk bytes detects any race.
                }
            }
            assert!(ss.err().is_none(), "{:?}", ss.err());
        }
        assert!(querier.close().is_none());
    };

    let handle = std::thread::spawn(writer);
    loop {
        reader();
        if handle.is_finished() {
            break;
        }
    }
    let writer_err = handle.join().unwrap();
    assert!(writer_err.is_none(), "{writer_err:?}");
}

/// Regression test for https://github.com/prometheus/prometheus/pull/13754
#[test]
fn test_abort_block_compactions_append_v2() {
    // Create a test DB
    let db = new_test_db(vec![]);
    // It should NOT be compactable at the beginning of the test
    assert!(!db.head.compactable(), "head should NOT be compactable");

    // Track the number of compactions run inside db.compactBlocks()
    let compactions = Arc::new(AtomicUsize::new(0));

    // Use a mock compactor with custom Plan() implementation
    let head = db.head.clone();
    let c = compactions.clone();
    *db.compactor.write().unwrap() = Arc::new(MockCompactorFn {
        plan_fn: Box::new(move || {
            // On every Plan() run increment compactions. After 4 compactions update HEAD to make it compactable to
            // force an exit from db.compactBlocks() loop.
            let n = c.fetch_add(1, Ordering::SeqCst) + 1;
            if n > 3 {
                let chunk_range = head.chunk_range.load(Ordering::SeqCst);
                head.min_time.store(0, Ordering::SeqCst);
                head.max_time.store(chunk_range * 2, Ordering::SeqCst);
                assert!(head.compactable(), "head should be compactable");
            }
            // Our custom Plan() will always return something to compact.
            Ok(vec![PathBuf::from("1"), PathBuf::from("2"), PathBuf::from("3")])
        }),
        compact_fn: Box::new(|| Ok(Vec::new())),
        write_fn: Box::new(|| Ok(Vec::new())),
    });

    db.compact(&ctx()).unwrap();
    assert!(db.head.compactable(), "head should be compactable");
    assert_eq!(4, compactions.load(Ordering::SeqCst), "expected 4 compactions to be completed");
}

/// TestCompactHeadWithSTStorage_AppendV2 ensures that when EnableSTStorage is true, compacted blocks contain
/// ST-capable chunks that preserve start timestamps.
#[test]
fn test_compact_head_with_st_storage_append_v2() {
    #[derive(Clone, Copy)]
    enum Kind {
        Float,
        Histogram,
        FloatHistogram,
    }
    struct SampleCase {
        name: &'static str,
        lbls: Labels,
        expected_encoding: chunkenc::Encoding,
        kind: Kind,
    }
    // append wraps the Append() calls so they are uniform, and returns the appended sample so it can be used to
    // initialize the expectation slice.
    let append = |kind: Kind, app: &mut dyn AppenderV2, lbls: &Labels, ts: i64| -> (Sample, Result<SeriesRef, Error>) {
        let o = AOptions::default();
        match kind {
            Kind::Float => {
                let st = ts - 50;
                let r = app.append(0, lbls, st, ts, ts as f64, None, None, &o);
                (Sample::float(st, ts, ts as f64), r)
            }
            Kind::Histogram => {
                let st = ts - 60;
                let mut h = tu::generate_test_histogram(ts);
                h.counter_reset_hint = CounterResetHint::NotCounterReset;
                let r = app.append(0, lbls, st, ts, 0.0, Some(&h), None, &o);
                (Sample::histogram(st, ts, h), r)
            }
            Kind::FloatHistogram => {
                let st = ts - 70;
                let mut fh = tu::generate_test_float_histogram(ts);
                fh.counter_reset_hint = CounterResetHint::NotCounterReset;
                let r = app.append(0, lbls, st, ts, 0.0, None, Some(&fh), &o);
                (Sample::float_histogram(st, ts, fh), r)
            }
        }
    };

    let cases = [
        SampleCase {
            name: "float",
            lbls: ls(&["a", "b", "type", "float"]),
            expected_encoding: chunkenc::ENC_XOR2,
            kind: Kind::Float,
        },
        SampleCase {
            name: "histogram",
            lbls: ls(&["a", "b", "type", "histogram"]),
            expected_encoding: chunkenc::ENC_HISTOGRAM_ST,
            kind: Kind::Histogram,
        },
        SampleCase {
            name: "float histogram",
            lbls: ls(&["a", "b", "type", "float_histogram"]),
            expected_encoding: chunkenc::ENC_FLOAT_HISTOGRAM_ST,
            kind: Kind::FloatHistogram,
        },
    ];

    // Go `&Options{...}` literal.
    let opts = Options {
        retention_duration: 24 * 15 * HOUR,
        no_lockfile: true,
        min_block_duration: 2 * HOUR,
        max_block_duration: 2 * HOUR,
        wal_compression: compression::Type::Snappy,
        enable_st_storage: true,
        float_chunk_encoding: chunkenc::ENC_XOR2,
        enable_histogram_st_encoding: true,
        ..tsdb::db::zero_options()
    };
    let db = new_test_db(vec![with_opts(opts)]);
    let mut app = db.appender_v2(&ctx());

    let (mint, maxt) = (100i64, 200i64);
    let mut expected_samples: BTreeMap<&str, Vec<Sample>> = BTreeMap::new();
    for i in mint..maxt {
        for c in &cases {
            let (s, r) = append(c.kind, app.as_mut(), &c.lbls, i);
            r.unwrap();
            expected_samples.entry(c.name).or_default().push(s);
        }
    }
    commit(&mut app);

    db.compact_head(&RangeHead::new(db.head().clone(), mint, maxt - 1)).unwrap();
    let blocks = db.blocks();
    assert_eq!(1, blocks.len());
    let b = &blocks[0];

    let chunkr = b.chunks().unwrap();
    let indexr = b.index().unwrap();

    let mut chunk_count = 0;
    for c in &cases {
        eprintln!("{}", c.name);
        let mut p = indexr.postings(&ctx(), "type", &[c.lbls.get("type")]).unwrap();

        let mut actual_samples = Vec::new();
        while p.next() {
            let mut builder = labels::ScratchBuilder::new(0);
            let mut chks = Vec::new();
            indexr.series(p.at(), &mut builder, &mut chks).unwrap();

            for chk in &chks {
                let chunk = match chunkr.chunk_or_iterable(chk).unwrap() {
                    tsdb::ChunkOrIterable::Chunk(c) => c,
                    tsdb::ChunkOrIterable::Iterable(_) => panic!("iterable must be nil"),
                };
                assert_eq!(c.expected_encoding, chunk.encoding(), "unexpected chunk encoding, got {:?}", chunk.encoding());

                actual_samples.extend(expand_chunk(chunk.as_ref()));
                chunk_count += 1;
            }
        }
        assert!(p.err().is_none(), "{:?}", p.err());
        require_equal_samples(c.name, &expected_samples[c.name], &actual_samples, true);
    }

    assert!(chunk_count > 0, "expected at least one chunk");
}

#[test]
fn test_new_compactor_func_append_v2() {
    let mut opts = opts_default();
    let block1 = Ulid::from_parts(1, [0; 10]);
    let block2 = Ulid::from_parts(2, [0; 10]);
    opts.new_compactor_func = Some(Arc::new(move |_: &Context, _, _: &[i64], _, _: &Options| {
        let c: Arc<dyn Compactor> = Arc::new(MockCompactorFn {
            plan_fn: Box::new(move || Ok(vec![PathBuf::from(block1.to_string()), PathBuf::from(block2.to_string())])),
            compact_fn: Box::new(move || Ok(vec![block1])),
            write_fn: Box::new(move || Ok(vec![block2])),
        });
        Ok(c)
    }));
    let db = new_test_db(vec![with_opts(opts)]);

    let compactor = db.compactor.read().unwrap().clone();
    let plans = compactor.plan(Path::new("")).unwrap();
    assert_eq!(vec![PathBuf::from(block1.to_string()), PathBuf::from(block2.to_string())], plans);
    let ulids = compactor.compact(Path::new(""), &[], &[]).unwrap();
    assert_eq!(1, ulids.len());
    assert_eq!(block1, ulids[0]);
    // adapted: Go passes a nil BlockReader; Rust needs a reader, so the (unused) head is passed.
    let ulids = compactor.write(Path::new(""), &RangeHead::new(db.head.clone(), 0, 1), 0, 1, None).unwrap();
    assert_eq!(1, ulids.len());
    assert_eq!(block2, ulids[0]);
}

/// TestDBAppenderV2_STStorage_OutOfOrder verifies that ST storage works correctly when samples are appended out of
/// order and can be queried using ChunkQuerier.
#[test]
fn test_db_appender_v2_st_storage_out_of_order() {
    let mut test_histogram = tu::generate_test_histogram(1);
    test_histogram.counter_reset_hint = CounterResetHint::NotCounterReset;
    let f = |st, t, v| Sample::float(st, t, v);
    let hs = |st, t| Sample::histogram(st, t, test_histogram.clone());

    let test_cases = vec![
        (
            "Float samples out of order",
            vec![
                f(20, 200, 2.0), // Append second sample first.
                f(10, 100, 1.0), // Append first sample second (OOO).
                f(30, 300, 3.0), // Append third sample last.
                f(25, 250, 2.5), // Append middle sample (OOO).
            ],
            vec![f(10, 100, 1.0), f(20, 200, 2.0), f(25, 250, 2.5), f(30, 300, 3.0)],
        ),
        (
            "Histogram samples out of order",
            vec![
                hs(30, 300), // Append third sample first.
                hs(10, 100), // Append first sample second (OOO).
                hs(20, 200), // Append second sample last (OOO).
            ],
            vec![hs(10, 100), hs(20, 200), hs(30, 300)],
        ),
        (
            "Mixed float samples with same ST",
            vec![
                f(10, 200, 2.0),
                f(10, 100, 1.0), // OOO with same ST.
                f(10, 300, 3.0),
            ],
            vec![f(10, 100, 1.0), f(10, 200, 2.0), f(10, 300, 3.0)],
        ),
    ];

    for (name, append_samples, expected_samples) in test_cases {
        eprintln!("{name}");
        let mut opts = opts_default();
        opts.out_of_order_time_window = 300 * MINUTE;
        opts.enable_st_storage = true;
        opts.float_chunk_encoding = chunkenc::ENC_XOR2;
        opts.enable_histogram_st_encoding = true;
        let db = new_test_db(vec![with_opts(opts)]);
        db.disable_compactions();

        let lbls = ls(&["foo", "bar"]);

        for s in &append_samples {
            let mut app = db.appender_v2(&ctx());
            app.append(0, &lbls, s.st, s.t, s.f, s.h.as_deref(), s.fh.as_deref(), &AOptions::default())
                .expect("Appending OOO sample with ST should succeed");
            assert!(app.commit().is_none(), "Committing OOO sample with ST should succeed");
        }

        let mut querier = db.chunk_querier(i64::MIN, i64::MAX).unwrap();
        let mut actual_samples = Vec::new();
        {
            let mut ss = querier.select(&ctx(), false, None, &[eq("foo", "bar")]);
            assert!(ss.next(), "Should have series");
            let series = ss.at().clone();
            assert!(ss.err().is_none());
            assert!(!ss.next(), "Should have only one series");

            let mut chunk_it = series.iterator(None);
            while chunk_it.next() {
                let chk = chunk_it.at();
                actual_samples.extend(expand_chunk(chk.chunk.as_deref().expect("chunk")));
            }
            assert!(chunk_it.err().is_none());
        }
        assert!(querier.close().is_none());

        // Use requireEqualSamplesIgnoreCounterResets to ignore histogram counter reset hints.
        require_equal_samples(&lbls.to_string(), &expected_samples, &actual_samples, true);

        // Additionally verify ST values match expectations.
        assert_eq!(expected_samples.len(), actual_samples.len());
        for (i, expected) in expected_samples.iter().enumerate() {
            assert_eq!(expected.st, actual_samples[i].st, "Sample {i}: ST should match");
        }
    }
}
