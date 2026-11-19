//! Port of `tsdb/db_append_v2_test.go` lines 4076..end.
//!
//! Subtests run one after the other in one Rust test; each prints its name first.

use std::collections::BTreeMap;
use std::path::Path;

use chunkenc::Chunk;
use model::histogram::{CounterResetHint, FloatHistogram, Histogram, Span};
use model::labels::{self, Labels};
use model::matcher::{MatchType, Matcher};
use storage::tsdbutil as tu;
use storage::{AOptions, AppenderV2, ChunkQuerier, Context, Error, Meta, Sample, SeriesRef};
use tsdb::db::{Options, DB};
use tsdb::head::{mmapped_chunks_dir, RangeHead};
use tsdb::querier::new_block_querier;
use tsdb_format::record::{self, Decoder};
use tsdb_format::wlog::{self, compression, WL};

use crate::common::{
    new_test_db, query, rand_f64, sample_type_scenarios, with_dir, with_opts, SampleTypeScenario, TempDir, TestDb,
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
