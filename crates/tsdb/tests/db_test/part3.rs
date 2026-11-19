//! Port of `tsdb/db_test.go`, lines 6615..9966 (part 3): the out-of-order (OOO) DB tests, WBL and m-map
//! replay and corruption, OOO compaction, runtime OOO config, histogram append and query, and the chunk querier
//! read/write race.
//!
//! Rust notes:
//! - Go `t.Cleanup(db.Close)` of a DB opened in a subtest runs at the end of that subtest. Here a subtest is a
//!   block, and the [`TestDb`] it opens is dropped (closed) at the end of the block.
//! - Go keeps the `*histogram.Histogram` that the test passes to `Append` and mutates it before `Commit`; the
//!   head sees the mutated value. Where a test does that, the port makes the same change before the append
//!   (marked `adapted`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use chunkenc::ValueType;
use model::histogram::{self, CounterResetHint, FloatHistogram, Histogram, Span};
use model::labels::{self, Labels};
use model::matcher::{MatchType, Matcher};
use storage::tsdbutil as tu;
use storage::{Appender, BoxChunkQuerier, Context, Error, LimitedAppenderV1, Meta, Sample, SeriesArc, SeriesRef};
use tsdb::block::{open_block, Block};
use tsdb::compact::Compactor;
use tsdb::db::{ApplyConfig, Options, TsdbConfig, DB};
use tsdb::head::{mmapped_chunks_dir, RangeHead};
use tsdb::querier::new_block_querier;
use tsdb::{BlockMeta, BlockReader, Ulid};
use tsdb_format::fileutil;
use tsdb_format::record::{self, Decoder, Logger};
use tsdb_format::wlog::{self, compression, WL};

use crate::common::{
    create_block, fs, gen_histogram_and_float_series, gen_histogram_series, gen_series,
    gen_series_from_sample_generator, new_test_db, query, rand_f64, sample_type_scenarios, with_dir, with_opts,
    with_rngs, SampleTypeScenario, TempDir, TestDb, CUSTOM_BUCKETS_FLOAT_HISTOGRAM, CUSTOM_BUCKETS_INT_HISTOGRAM,
    FLOAT_HISTOGRAM, GAUGE_FLOAT_HISTOGRAM, GAUGE_INT_HISTOGRAM, INT_HISTOGRAM,
};

// ---------------------------------------------------------------------------------------------------------------
// Helpers.

const MINUTE: i64 = 60_000;

fn minutes(m: i64) -> i64 {
    m * MINUTE
}

fn ctx() -> Context {
    Context::background()
}

fn lbl(ss: &[&str]) -> Labels {
    labels::from_strings(ss)
}

fn meq(n: &str, v: &str) -> Matcher {
    Matcher::new(MatchType::Equal, n, v).unwrap()
}

fn mre(n: &str, v: &str) -> Matcher {
    Matcher::new(MatchType::Regexp, n, v).unwrap()
}

fn sort_by_t(v: &mut [Sample]) {
    v.sort_by_key(|s| s.t);
}

/// Go `rand.Intn(n)`.
fn rand_intn(n: i64) -> i64 {
    ((rand_f64() * n as f64) as i64).min(n - 1)
}

fn default_opts() -> Options {
    tsdb::db::default_options()
}

/// Go `newTestDB(t, withOpts(opts))` followed by `db.DisableCompactions()`.
fn open_db_no_compactions(opts: &Options) -> TestDb {
    let db = new_test_db(vec![with_opts(opts.clone())]);
    db.disable_compactions();
    db
}

/// Go `newTestDB(t, withDir(dir), withOpts(opts))`.
fn reopen_db(dir: &Path, opts: &Options) -> TestDb {
    new_test_db(vec![with_dir(dir), with_opts(opts.clone())])
}

/// Go `query(t, db.Querier(mint, maxt), ms...)`.
fn query_db(db: &DB, mint: i64, maxt: i64, ms: &[Matcher]) -> BTreeMap<String, Vec<Sample>> {
    query(db.querier(mint, maxt).expect("querier"), ms)
}

/// Go `query(t, NewBlockQuerier(b, mint, maxt), ms...)`.
fn query_block(b: &dyn BlockReader, mint: i64, maxt: i64, ms: &[Matcher]) -> BTreeMap<String, Vec<Sample>> {
    query(new_block_querier(b, mint, maxt).expect("block querier"), ms)
}

/// Commits and requires no error.
fn commit(app: &mut dyn Appender) {
    let e = app.commit();
    assert!(e.is_none(), "{e:?}");
}

/// Go `requireEqualSamples` (testutil.go) with the options `requireEqualSamplesIgnoreCounterResets` and
/// `requireEqualSamplesInUseBucketCompare`.
fn require_equal_samples(
    name: &str,
    expected: &[Sample],
    actual: &[Sample],
    ignore_counter_resets: bool,
    in_use_bucket_compare: bool,
) {
    assert_eq!(actual.len(), expected.len(), "Length not equal to expected for {name}");
    for (i, (e, a)) in expected.iter().zip(actual).enumerate() {
        assert_eq!(e.t, a.t, "Different timestamps for {name}[{i}]");
        assert_eq!(e.typ(), a.typ(), "Different types for {name}[{i}] at ts {}", e.t);
        if let Some(eh) = &e.h {
            let mut eh = (**eh).clone();
            let mut ah = a.h.as_deref().unwrap().clone();
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
            let mut afh = a.fh.as_deref().unwrap().clone();
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

/// Go `requireEqualSeries` (testutil.go).
fn require_equal_series(
    expected: &BTreeMap<String, Vec<Sample>>,
    actual: &BTreeMap<String, Vec<Sample>>,
    ignore_counter_resets: bool,
) {
    for (name, e) in expected {
        let a = actual.get(name).unwrap_or_else(|| panic!("Expected series {name} not found"));
        require_equal_samples(name, e, a, ignore_counter_resets, false);
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

/// Go `queryChunks` (db_test.go): runs a matcher query against the chunk querier and expands its chunk metas.
/// Closes the querier.
fn query_chunks(mut q: BoxChunkQuerier, ms: &[Matcher]) -> BTreeMap<String, Vec<Meta>> {
    let mut result = BTreeMap::new();
    {
        let mut ss = q.select(&ctx(), false, None, ms);
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

/// Go `queryAndExpandChunks` (db_test.go).
fn query_and_expand_chunks(q: BoxChunkQuerier, ms: &[Matcher]) -> BTreeMap<String, Vec<Vec<Sample>>> {
    query_chunks(q, ms)
        .into_iter()
        .map(|(k, v)| {
            let samples = v
                .iter()
                .map(|chk| {
                    let c = chk.chunk.as_ref().expect("chunk");
                    storage::expand_samples(&mut c.iterator(None)).unwrap()
                })
                .collect();
            (k, samples)
        })
        .collect()
}

/// Go `copyWithCounterReset` (db_test.go).
fn copy_with_counter_reset(s: &Sample, hint: CounterResetHint) -> Sample {
    if let Some(h) = &s.h {
        let mut h = (**h).clone();
        h.counter_reset_hint = hint;
        return Sample::histogram(0, s.t, h);
    }
    let mut fh = s.fh.as_deref().unwrap().clone();
    fh.counter_reset_hint = hint;
    Sample::float_histogram(0, s.t, fh)
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

/// Go `mockCompactorFailing` (db_test.go): creates a new empty block on every write and fails when it reached
/// the max allowed total.
struct MockCompactorFailing {
    blocks: Mutex<Vec<Block>>,
    max: usize,
}

impl Compactor for MockCompactorFailing {
    fn plan(&self, _: &Path) -> Result<Vec<PathBuf>, Error> {
        Ok(Vec::new())
    }

    fn write(&self, dest: &Path, _: &dyn BlockReader, _: i64, _: i64, _: Option<&BlockMeta>) -> Result<Vec<Ulid>, Error> {
        let mut blocks = self.blocks.lock().unwrap();
        if blocks.len() >= self.max {
            return Err(Error::msg("the compactor already did the maximum allowed blocks so it is time to fail"));
        }
        let block = open_block(None, &create_block(dest, &gen_series(1, 1, 0, 1)), None, None).unwrap();
        block.close().unwrap(); // Close block as we won't be using anywhere.
        blocks.push(block);

        // Now check that all expected blocks are actually persisted on disk.
        // This way we make sure that we have some blocks that are supposed to be removed.
        let expected: Vec<PathBuf> = blocks.iter().map(|b| dest.join(b.meta().ulid.to_string())).collect();
        let actual = tsdb::db::block_dirs(dest).unwrap();
        assert_eq!(expected, actual);

        Ok(vec![blocks.last().unwrap().meta().ulid])
    }

    fn compact(&self, _: &Path, _: &[PathBuf], _: &[Block]) -> Result<Vec<Ulid>, Error> {
        Ok(Vec::new())
    }
}

/// The names of the entries of a directory, sorted (Go `os.ReadDir`).
fn read_dir_names(dir: &Path) -> std::io::Result<Vec<String>> {
    let mut names: Vec<String> =
        std::fs::read_dir(dir)?.map(|e| e.map(|e| e.file_name().to_string_lossy().into_owned())).collect::<Result<_, _>>()?;
    names.sort();
    Ok(names)
}

/// Go `os.RemoveAll`: removes a directory tree; a missing path is not an error.
fn remove_all(p: impl AsRef<Path>) {
    match std::fs::remove_dir_all(p.as_ref()) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => panic!("remove {:?}: {e}", p.as_ref()),
        _ => {}
    }
}

/// The size of a file.
fn file_size(p: &Path) -> i64 {
    std::fs::metadata(p).unwrap().len() as i64
}

/// The WBL of the DB's head.
fn wbl(db: &DB) -> Arc<WL> {
    db.head.wbl().expect("wbl")
}

/// The OOO in-memory and m-mapped chunk state of a series: whether `ms.ooo` is nil.
fn ooo_is_nil(db: &DB, lbls: &Labels) -> bool {
    let (ms, created) = db.head.get_or_create(lbls.hash(), lbls, false).unwrap();
    assert!(!created);
    ms.lock().ooo().is_none()
}

/// Go `makeConfig` of `testOutOfOrderRuntimeConfig`: a config with the given OOO time window (ms).
fn ooo_config(window_ms: i64) -> ApplyConfig {
    ApplyConfig {
        tsdb: Some(TsdbConfig {
            out_of_order_time_window: window_ms,
            ..TsdbConfig::default()
        }),
        ..ApplyConfig::default()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// TestChunkQuerierOOOQuery.

/// Go `nBucketHistogram` of `TestChunkQuerierOOOQuery`.
fn n_bucket_histogram(n: i64) -> Histogram {
    let mut h = Histogram {
        count: n as u64,
        sum: n as f64,
        ..Histogram::default()
    };
    if n == 0 {
        h.positive_spans = Vec::new();
        h.positive_buckets = Vec::new();
        return h;
    }
    h.positive_spans = vec![Span { offset: 0, length: n as u32 }];
    h.positive_buckets = vec![0; n as usize];
    h.positive_buckets[0] = 1;
    h
}

type CqAppendFn = fn(&mut dyn Appender, i64, bool) -> Result<SeriesRef, Error>;

struct ChunkQuerierScenario {
    name: &'static str,
    append_func: CqAppendFn,
    sample_func: fn(i64) -> Sample,
    check_in_use_bucket: bool,
}

fn chunk_querier_scenarios() -> Vec<ChunkQuerierScenario> {
    vec![
        ChunkQuerierScenario {
            name: "float",
            append_func: |app, ts, _| app.append(0, &lbl(&["foo", "bar1"]), ts, ts as f64),
            sample_func: |ts| fs(ts, ts as f64),
            check_in_use_bucket: false,
        },
        ChunkQuerierScenario {
            name: "integer histogram",
            append_func: |app, ts, counter_reset| {
                let mut h = tu::generate_test_histogram(ts);
                if counter_reset {
                    h.counter_reset_hint = CounterResetHint::CounterReset;
                }
                app.append_histogram(0, &lbl(&["foo", "bar1"]), ts, Some(&h), None)
            },
            sample_func: |ts| Sample::histogram(0, ts, tu::generate_test_histogram(ts)),
            check_in_use_bucket: false,
        },
        ChunkQuerierScenario {
            name: "float histogram",
            append_func: |app, ts, counter_reset| {
                let mut fh = tu::generate_test_float_histogram(ts);
                if counter_reset {
                    fh.counter_reset_hint = CounterResetHint::CounterReset;
                }
                app.append_histogram(0, &lbl(&["foo", "bar1"]), ts, None, Some(&fh))
            },
            sample_func: |ts| Sample::float_histogram(0, ts, tu::generate_test_float_histogram(ts)),
            check_in_use_bucket: false,
        },
        ChunkQuerierScenario {
            name: "integer histogram counter resets",
            // Adding counter reset to all histograms means each histogram will have its own chunk.
            append_func: |app, ts, _| {
                let mut h = tu::generate_test_histogram(ts);
                h.counter_reset_hint = CounterResetHint::CounterReset; // For this scenario, ignore the counterReset argument.
                app.append_histogram(0, &lbl(&["foo", "bar1"]), ts, Some(&h), None)
            },
            sample_func: |ts| Sample::histogram(0, ts, tu::generate_test_histogram(ts)),
            check_in_use_bucket: false,
        },
        ChunkQuerierScenario {
            name: "integer histogram with recode",
            // Histograms have increasing number of buckets so their chunks are recoded.
            append_func: |app, ts, _| {
                let n = ts / MINUTE;
                app.append_histogram(0, &lbl(&["foo", "bar1"]), ts, Some(&n_bucket_histogram(n)), None)
            },
            sample_func: |ts| Sample::histogram(0, ts, n_bucket_histogram(ts / MINUTE)),
            // Only check in-use buckets for this scenario.
            // Recoding adds empty buckets.
            check_in_use_bucket: true,
        },
    ]
}

/// A batch of samples of the OOO query tests (Go `sampleBatch`).
struct SampleBatch {
    min_t: i64,
    max_t: i64,
    filter: fn(i64) -> bool,
    counter_reset: bool,
    is_ooo: bool,
}

fn batch(min_t: i64, max_t: i64, filter: fn(i64) -> bool, is_ooo: bool) -> SampleBatch {
    SampleBatch { min_t, max_t, filter, counter_reset: false, is_ooo }
}

/// A test case of the OOO query tests.
struct OooQueryCase {
    name: &'static str,
    ooo_cap: i64,
    query_min_t: i64,
    query_max_t: i64,
    batches: Vec<SampleBatch>,
}

fn ooo_query_cases() -> Vec<OooQueryCase> {
    let default_filter: fn(i64) -> bool = |_| true;
    let even: fn(i64) -> bool = |t| t % 2 == 0;
    let odd: fn(i64) -> bool = |t| t % 2 == 1;
    vec![
        OooQueryCase {
            name: "query interval covering ooomint and inordermaxt returns all ingested samples",
            ooo_cap: 30,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![
                batch(minutes(100), minutes(200), default_filter, false),
                batch(minutes(0), minutes(99), default_filter, true),
            ],
        },
        OooQueryCase {
            name: "partial query interval returns only samples within interval",
            ooo_cap: 30,
            query_min_t: minutes(20),
            query_max_t: minutes(180),
            batches: vec![
                batch(minutes(100), minutes(200), default_filter, false),
                batch(minutes(0), minutes(99), default_filter, true),
            ],
        },
        OooQueryCase {
            // In order: 100-200 normal. out of order first path: 0, 2, 4, ... 98 (no counter reset), second
            // pass: 1, 3, 5, ... 99 (with counter reset).
            name: "alternating OOO batches",
            ooo_cap: 0,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![
                batch(minutes(100), minutes(200), default_filter, false),
                batch(minutes(0), minutes(99), even, true),
                SampleBatch { min_t: minutes(0), max_t: minutes(99), filter: odd, counter_reset: true, is_ooo: true },
            ],
        },
        OooQueryCase {
            name: "query overlapping inorder and ooo samples returns all ingested samples at the end of the interval",
            ooo_cap: 30,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![batch(minutes(100), minutes(200), even, false), batch(minutes(170), minutes(180), odd, true)],
        },
        OooQueryCase {
            name: "query overlapping inorder and ooo in-memory samples returns all ingested samples at the beginning of the interval",
            ooo_cap: 30,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![batch(minutes(100), minutes(200), even, false), batch(minutes(100), minutes(110), odd, true)],
        },
        OooQueryCase {
            name: "query inorder contain ooo mmapped samples returns all ingested samples at the beginning of the interval",
            ooo_cap: 5,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![
                batch(minutes(100), minutes(200), even, false),
                // Append samples to fit in a single mmapped OOO chunk and fit inside the first in-order mmapped chunk.
                batch(minutes(101), minutes(101 + (5 - 1) * 2), odd, true),
                // Append some more OOO samples to trigger mapping the OOO chunk, but use time 151 to not overlap
                // with in-order head chunk.
                batch(minutes(191), minutes(193), odd, true),
            ],
        },
        OooQueryCase {
            name: "query overlapping inorder and ooo mmapped samples returns all ingested samples at the beginning of the interval",
            ooo_cap: 30,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![
                batch(minutes(100), minutes(200), even, false),
                // Append samples to fit in a single mmapped OOO chunk and overlap the first in-order mmapped chunk.
                batch(minutes(101), minutes(101 + (30 - 1) * 2), odd, true),
                // Append some more OOO samples to trigger mapping the OOO chunk, but use time 151 to not overlap
                // with in-order head chunk.
                batch(minutes(191), minutes(193), odd, true),
            ],
        },
    ]
}

/// Go `addSample` of `testChunkQuerierOOOQuery`.
#[allow(clippy::too_many_arguments)]
fn cq_add_sample(
    db: &DB,
    append_func: CqAppendFn,
    sample_func: fn(i64) -> Sample,
    from_mins: i64,
    to_mins: i64,
    query_min_t: i64,
    query_max_t: i64,
    exp_samples: &mut Vec<Sample>,
    filter: fn(i64) -> bool,
    counter_reset: bool,
) -> usize {
    let mut app = db.appender(&ctx());
    let mut total_appended = 0;
    let mut m = from_mins;
    while m <= to_mins {
        if filter(m / MINUTE) {
            let r = append_func(app.as_mut(), m, counter_reset);
            if m >= query_min_t && m <= query_max_t {
                exp_samples.push(sample_func(m));
            }
            r.unwrap();
            total_appended += 1;
        }
        m += MINUTE;
    }
    commit(app.as_mut());
    assert!(total_appended > 0); // Sanity check that filter is not too zealous.
    total_appended
}

/// Go `testChunkQuerierOOOQuery`.
fn run_chunk_querier_ooo_query(sc: &ChunkQuerierScenario) {
    let mut opts = default_opts();
    opts.out_of_order_cap_max = 30;
    opts.out_of_order_time_window = 24 * 60 * MINUTE;

    let series1 = lbl(&["foo", "bar1"]);

    for tc in ooo_query_cases() {
        eprintln!("name={}", tc.name);
        opts.out_of_order_cap_max = tc.ooo_cap;
        let db = open_db_no_compactions(&opts);

        let mut exp_samples = Vec::new();
        let mut ooo_samples = 0;
        for b in &tc.batches {
            let n = cq_add_sample(
                &db,
                sc.append_func,
                sc.sample_func,
                b.min_t,
                b.max_t,
                tc.query_min_t,
                tc.query_max_t,
                &mut exp_samples,
                b.filter,
                b.counter_reset,
            );
            if b.is_ooo {
                ooo_samples += n;
            }
        }
        sort_by_t(&mut exp_samples);

        let querier = db.chunk_querier(tc.query_min_t, tc.query_max_t).unwrap();
        let chks = query_chunks(querier, &[meq("foo", "bar1")]);
        assert!(chks.contains_key(&series1.to_string()));
        assert_eq!(1, chks.len());
        require_equal_ooo_samples(ooo_samples, &db);
        let mut got_samples = Vec::new();
        for chunk in &chks[&series1.to_string()] {
            let c = chunk.chunk.as_ref().expect("chunk");
            let mut it = c.iterator(None);
            let smpls = storage::expand_samples(&mut it).unwrap();

            // Verify that no sample is outside the chunk's time range.
            let n = smpls.len();
            for (i, s) in smpls.iter().enumerate() {
                if i == 0 {
                    assert_eq!(chunk.min_time, s.t, "first sample {s:?} not at chunk min time {}", chunk.min_time);
                } else if i == n - 1 {
                    assert_eq!(chunk.max_time, s.t, "last sample {s:?} not at chunk max time {}", chunk.max_time);
                } else {
                    assert!(s.t >= chunk.min_time, "sample {s:?} before chunk min time {}", chunk.min_time);
                    assert!(s.t <= chunk.max_time, "sample {s:?} after chunk max time {}", chunk.max_time);
                }
            }
            got_samples.extend(smpls);
        }
        require_equal_samples(&series1.to_string(), &exp_samples, &got_samples, true, sc.check_in_use_bucket);
    }
}

#[test]
fn test_chunk_querier_ooo_query() {
    for sc in chunk_querier_scenarios() {
        eprintln!("scenario {}", sc.name);
        run_chunk_querier_ooo_query(&sc);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// TestOOONativeHistogramsWithCounterResets.

struct CounterResetBatch {
    from: i64,
    until: i64,
    should_reset: fn(i64) -> bool,
    exp_counter_reset_hints: Vec<CounterResetHint>,
}

struct CounterResetCase {
    name: &'static str,
    query_min: i64,
    query_max: i64,
    batches: Vec<CounterResetBatch>,
}

fn crb(from: i64, until: i64, should_reset: fn(i64) -> bool, hints: Vec<CounterResetHint>) -> CounterResetBatch {
    CounterResetBatch { from, until, should_reset, exp_counter_reset_hints: hints }
}

/// Go `testOOONativeHistogramsWithCounterResets`.
fn run_ooo_native_histograms_with_counter_resets(scenario: &SampleTypeScenario) {
    use CounterResetHint::{NotCounterReset as N, UnknownCounterReset as U};
    let mut opts = default_opts();
    opts.out_of_order_cap_max = 30;
    opts.out_of_order_time_window = 24 * 60 * MINUTE;

    let default_reset: fn(i64) -> bool = |_| false;
    let lbls = lbl(&["foo", "bar1"]);

    let tests = vec![
        CounterResetCase {
            name: "Counter reset within in-order samples",
            query_min: minutes(40),
            query_max: minutes(55),
            batches: vec![
                // In-order samples
                crb(40, 50, |v| v == 45, vec![U, N, N, N, N, U, N, N, N, N]),
            ],
        },
        CounterResetCase {
            name: "Counter reset right at beginning of OOO samples",
            query_min: minutes(40),
            query_max: minutes(55),
            batches: vec![
                // In-order samples
                crb(40, 45, default_reset, vec![U, N, N, N, N]),
                crb(50, 55, default_reset, vec![U, N, N, N, N]),
                // OOO samples
                crb(45, 50, |v| v == 45, vec![U, N, N, N, N]),
            ],
        },
        CounterResetCase {
            name: "Counter resets in both in-order and OOO samples",
            query_min: minutes(40),
            query_max: minutes(55),
            batches: vec![
                // In-order samples
                crb(40, 45, |v| v == 44, vec![U, N, N, N, U]),
                crb(50, 55, default_reset, vec![U, N, N, N, N]),
                // OOO samples
                crb(45, 50, |v| v == 49, vec![U, N, N, N, U]),
            ],
        },
    ];

    for tc in tests {
        eprintln!("name={}", tc.name);
        let db = open_db_no_compactions(&opts);
        let mut app = db.appender(&ctx());
        let mut exp_samples: BTreeMap<String, Vec<Sample>> = BTreeMap::new();

        for b in &tc.batches {
            let mut j = b.from;
            for (smpl_idx, i) in (b.from..b.until).enumerate() {
                if (b.should_reset)(i) {
                    j = 0;
                }
                // adapted: Go sets the expected counter reset hint on the histogram it has just passed to the
                // appender, which keeps the pointer until Commit, so the head appends the changed histogram. Here
                // the hint is set before the append.
                let mut s = (scenario.sample_func)(minutes(i), j);
                if let Some(h) = s.h.as_mut() {
                    h.counter_reset_hint = b.exp_counter_reset_hints[smpl_idx];
                } else if let Some(fh) = s.fh.as_mut() {
                    fh.counter_reset_hint = b.exp_counter_reset_hints[smpl_idx];
                }
                app.append_histogram(0, &lbls, minutes(i), s.h.as_deref(), s.fh.as_deref()).unwrap();
                exp_samples.entry(lbls.to_string()).or_default().push(s);
                j += 1;
            }
        }
        commit(app.as_mut());

        for v in exp_samples.values_mut() {
            sort_by_t(v);
        }

        let series_set = query_db(&db, tc.query_min, tc.query_max, &[meq("foo", "bar1")]);
        assert!(series_set.contains_key(&lbls.to_string()));
        assert_eq!(1, series_set.len());
        require_equal_series(&exp_samples, &series_set, false);
    }
}

#[test]
fn test_ooo_native_histograms_with_counter_resets() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("scenario {name}");
        if name == INT_HISTOGRAM || name == FLOAT_HISTOGRAM {
            run_ooo_native_histograms_with_counter_resets(&scenario);
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// TestOOOInterleavedImplicitCounterResets.

/// The histogram that `testOOOInterleavedImplicitCounterResets` appends for scenario `name` (an integer counter
/// converted to a histogram with a single bucket).
fn interleaved_histogram(name: &str, v: i64) -> (Option<Histogram>, Option<FloatHistogram>) {
    let spans = vec![Span { offset: 0, length: 1 }];
    match name {
        INT_HISTOGRAM => (
            Some(Histogram {
                count: v as u64,
                sum: v as f64,
                positive_spans: spans,
                positive_buckets: vec![v],
                ..Histogram::default()
            }),
            None,
        ),
        FLOAT_HISTOGRAM => (
            None,
            Some(FloatHistogram {
                count: v as f64,
                sum: v as f64,
                positive_spans: spans,
                positive_buckets: vec![v as f64],
                ..FloatHistogram::default()
            }),
        ),
        CUSTOM_BUCKETS_INT_HISTOGRAM => (
            Some(Histogram {
                schema: -53,
                count: v as u64,
                sum: v as f64,
                positive_spans: spans,
                positive_buckets: vec![v],
                custom_values: histogram::custom_values(&[1.0, 2.0, 3.0]),
                ..Histogram::default()
            }),
            None,
        ),
        CUSTOM_BUCKETS_FLOAT_HISTOGRAM => (
            None,
            Some(FloatHistogram {
                schema: -53,
                count: v as f64,
                sum: v as f64,
                positive_spans: spans,
                positive_buckets: vec![v as f64],
                custom_values: histogram::custom_values(&[1.0, 2.0, 3.0]),
                ..FloatHistogram::default()
            }),
        ),
        _ => unreachable!(),
    }
}

struct InterleavedCase {
    name: &'static str,
    samples: Vec<(i64, i64)>,
    ooo_cap: i64,
    /// The expected samples with counter reset: (ts, v, hint).
    expected_samples: Vec<(i64, i64, CounterResetHint)>,
    /// The expected counter reset hint and size of each chunk.
    expected_chunks: Vec<(CounterResetHint, usize)>,
}

/// The hint and count of a histogram sample of scenario `name`.
fn hint_and_count(name: &str, s: &Sample) -> (CounterResetHint, i64) {
    match name {
        INT_HISTOGRAM | CUSTOM_BUCKETS_INT_HISTOGRAM => {
            let h = s.h.as_deref().expect("histogram");
            (h.counter_reset_hint, h.count as i64)
        }
        FLOAT_HISTOGRAM | CUSTOM_BUCKETS_FLOAT_HISTOGRAM => {
            let fh = s.fh.as_deref().expect("float histogram");
            (fh.counter_reset_hint, fh.count as i64)
        }
        _ => panic!("unexpected sample type {name}"),
    }
}

/// Go `testOOOInterleavedImplicitCounterResets`.
fn run_ooo_interleaved_implicit_counter_resets(name: &str, scenario: &SampleTypeScenario) {
    use CounterResetHint::{NotCounterReset as N, UnknownCounterReset as U};
    if scenario.sample_type != "histogram" {
        return;
    }
    if name == GAUGE_INT_HISTOGRAM || name == GAUGE_FLOAT_HISTOGRAM {
        return;
    }

    let cases = vec![
        InterleavedCase {
            name: "counter reset in-order cleared by in-memory OOO chunk",
            samples: vec![
                (1, 40), // New in In-order. I1.
                (4, 30), // In-order counter reset. I2.
                (2, 40), // New in OOO. O1.
                (3, 10), // OOO counter reset. O2.
            ],
            ooo_cap: 30,
            // Expect all to be set to UnknownCounterReset because we switch between
            // in-order and out-of-order samples.
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
        InterleavedCase {
            name: "counter reset in OOO mmapped chunk cleared by in-memory ooo chunk",
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
        InterleavedCase {
            name: "counter reset in OOO mmapped chunk cleared by another OOO mmapped chunk",
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
    ];

    for tc in cases {
        eprintln!("{}", tc.name);
        let mut opts = default_opts();
        opts.out_of_order_cap_max = tc.ooo_cap;
        opts.out_of_order_time_window = 24 * 60 * MINUTE;
        let db = open_db_no_compactions(&opts);

        let mut app = db.appender(&ctx());
        for &(ts, v) in &tc.samples {
            let (h, fh) = interleaved_histogram(name, v);
            app.append_histogram(0, &lbl(&["foo", "bar1"]), ts, h.as_ref(), fh.as_ref()).unwrap();
        }
        commit(app.as_mut());

        // querier
        {
            let series_set = query_db(&db, 0, 10, &[meq("foo", "bar1")]);
            assert_eq!(1, series_set.len());
            let samples = series_set.get("{foo=\"bar1\"}").expect("series");
            assert_eq!(tc.samples.len(), samples.len());
            assert_eq!(tc.expected_samples.len(), samples.len());

            // We expect all unknown counter resets because we clear the counter reset
            // hint when we switch between in-order and out-of-order samples.
            for (i, s) in samples.iter().enumerate() {
                let (hint, count) = hint_and_count(name, s);
                assert_eq!(tc.expected_samples[i].2, hint, "sample {i}");
                assert_eq!(tc.expected_samples[i].1, count, "sample {i}");
            }
        }

        // chunk-querier
        {
            let querier = db.chunk_querier(0, 10).unwrap();
            let chunk_set = query_and_expand_chunks(querier, &[meq("foo", "bar1")]);
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

#[test]
fn test_ooo_interleaved_implicit_counter_resets() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("scenario {name}");
        run_ooo_interleaved_implicit_counter_resets(name, &scenario);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// TestOOOAppendAndQuery.

/// The state of `testOOOAppendAndQuery`: the appended samples per series and their total.
struct OooAppendState<'a> {
    db: &'a DB,
    scenario: &'a SampleTypeScenario,
    appended_samples: BTreeMap<String, Vec<Sample>>,
    total_samples: usize,
}

impl OooAppendState<'_> {
    /// Go `addSample`.
    fn add_sample(&mut self, lbls: &Labels, from_mins: i64, to_mins: i64, face_error: bool) {
        let mut app = self.db.appender(&ctx());
        let key = lbls.to_string();
        let (from, to) = (minutes(from_mins), minutes(to_mins));
        let mut m = from;
        while m <= to {
            let val = rand_intn(1000);
            let (r, s) = self.scenario.append(app.as_mut(), lbls, m, val);
            if face_error {
                assert!(r.is_err());
            } else {
                r.unwrap();
                self.appended_samples.entry(key.clone()).or_default().push(s);
                self.total_samples += 1;
            }
            m += MINUTE;
        }
        if face_error {
            assert!(app.rollback().is_none());
        } else {
            commit(app.as_mut());
        }
    }

    /// Go `testQuery`.
    fn test_query(&mut self, from: i64, to: i64) {
        let series_set = query_db(self.db, from, to, &[mre("foo", "bar.")]);
        for v in self.appended_samples.values_mut() {
            sort_by_t(v);
        }
        let mut exp_samples: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
        for (k, samples) in &self.appended_samples {
            for s in samples {
                if s.t < from || s.t > to {
                    continue;
                }
                exp_samples.entry(k.clone()).or_default().push(s.clone());
            }
        }
        require_equal_series(&exp_samples, &series_set, true);
        require_equal_ooo_samples(self.total_samples - 2, self.db);
    }

    /// Go `verifyOOOMinMaxTimes`.
    fn verify_ooo_min_max_times(&self, exp_min: i64, exp_max: i64) {
        assert_eq!(minutes(exp_min), self.db.head.min_ooo_time());
        assert_eq!(minutes(exp_max), self.db.head.max_ooo_time());
    }
}

/// Go `testOOOAppendAndQuery`.
fn run_ooo_append_and_query(scenario: &SampleTypeScenario) {
    let mut opts = default_opts();
    opts.out_of_order_cap_max = 30;
    opts.out_of_order_time_window = 4 * 60 * MINUTE;

    let db = open_db_no_compactions(&opts);

    let s1 = lbl(&["foo", "bar1"]);
    let s2 = lbl(&["foo", "bar2"]);
    let mut st = OooAppendState { db: &db, scenario, appended_samples: BTreeMap::new(), total_samples: 0 };
    let chunks_created = || db.head.metrics.chunks_created.get();

    // In-order samples.
    st.add_sample(&s1, 300, 300, false);
    st.add_sample(&s2, 290, 290, false);
    assert_eq!(2.0, chunks_created());
    st.test_query(i64::MIN, i64::MAX);

    // Some ooo samples.
    st.add_sample(&s1, 250, 260, false);
    st.add_sample(&s2, 255, 265, false);
    st.verify_ooo_min_max_times(250, 265);
    st.test_query(i64::MIN, i64::MAX);
    st.test_query(minutes(250), minutes(265)); // Test querying ooo data time range.
    st.test_query(minutes(290), minutes(300)); // Test querying in-order data time range.
    st.test_query(minutes(250), minutes(300)); // Test querying the entire range.

    // Out of time window.
    st.add_sample(&s1, 59, 59, true);
    st.add_sample(&s2, 49, 49, true);
    st.verify_ooo_min_max_times(250, 265);
    st.test_query(i64::MIN, i64::MAX);

    // At the edge of time window, also it would be "out of bound" without the ooo support.
    st.add_sample(&s1, 60, 65, false);
    st.verify_ooo_min_max_times(60, 265);
    st.test_query(i64::MIN, i64::MAX);

    // This sample is not within the time window w.r.t. the head's maxt, but it is within the window
    // w.r.t. the series' maxt. But we consider only head's maxt.
    st.add_sample(&s2, 59, 59, true);
    st.verify_ooo_min_max_times(60, 265);
    st.test_query(i64::MIN, i64::MAX);

    // Now the sample is within time window w.r.t. the head's maxt.
    st.add_sample(&s2, 60, 65, false);
    st.verify_ooo_min_max_times(60, 265);
    st.test_query(i64::MIN, i64::MAX);

    // Out of time window again.
    st.add_sample(&s1, 59, 59, true);
    st.add_sample(&s2, 49, 49, true);
    st.test_query(i64::MIN, i64::MAX);

    // Generating some m-map chunks. The m-map chunks here are in such a way
    // that when sorted w.r.t. mint, the last chunk's maxt is not the overall maxt
    // of the merged chunk. This tests a bug fixed in https://github.com/grafana/mimir-prometheus/pull/238/.
    assert_eq!(4.0, chunks_created());
    st.add_sample(&s1, 180, 249, false);
    assert_eq!(6.0, chunks_created());
    st.verify_ooo_min_max_times(60, 265);
    st.test_query(i64::MIN, i64::MAX);
}

#[test]
fn test_ooo_append_and_query() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("scenario {name}");
        run_ooo_append_and_query(&scenario);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// TestOOODisabled.

/// Go `testOOODisabled`.
fn run_ooo_disabled(scenario: &SampleTypeScenario) {
    let mut opts = default_opts();
    opts.out_of_order_time_window = 0;
    let db = open_db_no_compactions(&opts);

    let s1 = lbl(&["foo", "bar1"]);
    let mut exp_samples: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
    let mut failed_samples = 0;

    let mut add_sample = |lbls: &Labels, from_mins: i64, to_mins: i64, face_error: bool| {
        let mut app = db.appender(&ctx());
        let key = lbls.to_string();
        let (from, to) = (minutes(from_mins), minutes(to_mins));
        let mut m = from;
        while m <= to {
            let (r, _) = scenario.append(app.as_mut(), lbls, m, m);
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
            commit(app.as_mut());
        }
    };

    add_sample(&s1, 300, 300, false); // In-order samples.
    add_sample(&s1, 250, 260, true); // Some ooo samples.
    add_sample(&s1, 59, 59, true); // Out of time window.
    add_sample(&s1, 60, 65, true); // At the edge of time window, also it would be "out of bound" without the ooo support.
    add_sample(&s1, 59, 59, true); // Out of time window again.
    add_sample(&s1, 301, 310, false); // More in-order samples.

    let series_set = query_db(&db, i64::MIN, i64::MAX, &[mre("foo", "bar.")]);
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
    let err = std::fs::read_dir(db.dir().join(wlog::WBL_DIR_NAME)).err().expect("no WBL dir");
    assert_eq!(std::io::ErrorKind::NotFound, err.kind());

    let (ms, created) = db.head.get_or_create(s1.hash(), &s1, false).unwrap();
    assert!(!created);
    assert!(ms.lock().ooo().is_none());
}

#[test]
fn test_ooo_disabled() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("scenario {name}");
        run_ooo_disabled(&scenario);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// TestOOOSampleLossOnWALCheckpointBeforeOOOCompaction.

// TestOOOSampleLossOnWALCheckpointBeforeOOOCompaction covers the loss of acknowledged out-of-order samples when a
// WAL checkpoint drops a series record that the WBL still references.
//
// A series that is evicted from the head and appended to again is recreated under a new reference, while the
// earlier reference's series record stays in the WAL. WAL replay collapses both onto a single in-memory series and
// holds that mapping in multiRef, which is never persisted. Out-of-order samples appended before such a replay are
// logged to the WBL under the newer reference, so they can only be resolved while the WAL still carries that
// reference's series record.
//
// An in-order head compaction checkpoints the WAL before out-of-order compaction persists the WBL. The checkpoint
// drops the newer reference, because it is not live in the head and its WAL expiry lies below the checkpoint's
// mint. If the process stops in that window the WBL holds the only copy of those samples, and they are skipped by
// the next replay.
#[test]
fn test_ooo_sample_loss_on_wal_checkpoint_before_ooo_compaction() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("scenario {name}");
        let evicted_time = 100 * MINUTE;
        let in_order_time = 300 * MINUTE;
        let ooo_time = 250 * MINUTE;

        let series = lbl(&["foo", "bar"]);
        let tmp = TempDir::new();
        let dir = tmp.path().to_path_buf();

        let open_db = || {
            let mut opts = default_opts();
            opts.wal_segment_size = 32 * 1024;
            opts.out_of_order_time_window = 300 * MINUTE;
            let db = reopen_db(&dir, &opts);
            db.disable_compactions(); // We want to call the compactions manually.
            db
        };

        let append_sample = |db: &DB, ts: i64| -> u64 {
            let mut app = db.appender(&ctx());
            let (r, _) = scenario.append(app.as_mut(), &series, ts, ts);
            let r = r.unwrap();
            commit(app.as_mut());
            r
        };

        // Appending large label values fills WAL segments, so that the in-order
        // compaction below has enough segments to checkpoint. Checkpointing only
        // considers the lower two thirds of the existing segments.
        let fill_wal_segments = |db: &DB| {
            let mut app = db.appender(&ctx());
            let padding = "x".repeat(8192);
            for i in 0..32 {
                let id = i.to_string();
                app.append(0, &lbl(&["foo", "pad", "id", &id, "padding", &padding]), in_order_time, i as f64).unwrap();
            }
            commit(app.as_mut());
        };

        let query_series = |db: &DB| query_db(db, i64::MIN, i64::MAX, &[meq("foo", "bar")]);

        // Compact the series into a block and evict it from the head, so that the
        // next append recreates it under a new series reference. The WAL is too
        // short to be checkpointed here, so the evicted reference's series record
        // survives in the WAL.
        let mut db = open_db();
        let evicted_ref = append_sample(&db, evicted_time);
        db.compact_head(&RangeHead::new(db.head.clone(), 0, evicted_time)).unwrap();
        assert_eq!(0, db.head.num_series(), "the series must be evicted from the head");

        // Recreate the series under a new reference and append an out-of-order
        // sample, which is written to the WBL under that new reference.
        let recreated_ref = append_sample(&db, in_order_time);
        assert_ne!(evicted_ref, recreated_ref, "the series must be recreated under a new reference");
        append_sample(&db, ooo_time);
        fill_wal_segments(&db);
        db.close().unwrap();

        // Replaying the WAL maps the newer reference onto the evicted one. The
        // mapping only exists in memory, and the out-of-order sample recovered from
        // the WBL does not extend the newer reference's WAL retention.
        db = open_db();
        let mut expected = BTreeMap::new();
        expected.insert(
            series.to_string(),
            vec![
                (scenario.sample_func)(evicted_time, evicted_time),
                (scenario.sample_func)(ooo_time, ooo_time),
                (scenario.sample_func)(in_order_time, in_order_time),
            ],
        );
        require_equal_series(&expected, &query_series(&db), true);

        // Only the WBL addresses the out-of-order sample, through a reference that
        // no longer identifies a series in the head. Its WAL expiry is the in-order
        // sample's timestamp, which the checkpoint below truncates past.
        assert!(db.head.series.get_by_id(recreated_ref).is_none(), "the recreated reference must not be live in the head");
        let keep_until = db.head.get_wal_expiry(recreated_ref).expect("WAL expiry");
        assert_eq!(in_order_time, keep_until);

        // Compacting the in-order head checkpoints the WAL. Out-of-order compaction
        // would persist the WBL afterwards, but is not reached here.
        db.compact_head(&RangeHead::new(db.head.clone(), db.head.min_time(), in_order_time)).unwrap();
        assert!(
            db.head.metrics.checkpoint_creation_total.get() > 0.0,
            "the in-order compaction must checkpoint the WAL"
        );
        db.close().unwrap();

        // The WBL is now the only copy of the out-of-order sample.
        db = open_db();
        require_equal_series(&expected, &query_series(&db), true);
        assert_eq!(
            0.0,
            db.head.metrics.wbl_replay_unknown_refs_total.get("series"),
            "WBL replay must not encounter unknown series references"
        );

        // Out-of-order compaction persists the WBL into a block and truncates it,
        // after which the reference the WBL used is no longer needed.
        db.compact_ooo_head(&ctx()).unwrap();
        assert!(
            !db.head.is_wbl_pinned_series_ref(recreated_ref),
            "the reference must be released once the WBL is truncated"
        );
        db.close().unwrap();

        db = open_db();
        require_equal_series(&expected, &query_series(&db), true);
        drop(db);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// TestWBLAndMmapReplay.

/// Go `testQuery` of `testWBLAndMmapReplay`.
fn wbl_mmap_test_query(db: &DB, exp: &mut BTreeMap<String, Vec<Sample>>) {
    let series_set = query_db(db, i64::MIN, i64::MAX, &[mre("foo", "bar.")]);
    for v in exp.values_mut() {
        sort_by_t(v);
    }
    require_equal_series(exp, &series_set, true);
}

/// Go `testWBLAndMmapReplay`.
fn run_wbl_and_mmap_replay(scenario: &SampleTypeScenario) {
    let mut opts = default_opts();
    opts.out_of_order_cap_max = 30;
    opts.out_of_order_time_window = 4 * 60 * MINUTE;

    let tmp = TempDir::new();
    let db_dir = tmp.path().to_path_buf();
    let db = reopen_db(&db_dir, &opts);
    db.disable_compactions();

    let s1 = lbl(&["foo", "bar1"]);
    let mut exp_samples: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
    let add_sample = |exp: &mut BTreeMap<String, Vec<Sample>>, lbls: &Labels, from_mins: i64, to_mins: i64| {
        let mut app = db.appender(&ctx());
        let key = lbls.to_string();
        let mut m = minutes(from_mins);
        while m <= minutes(to_mins) {
            let val = rand_intn(1000);
            let (r, s) = scenario.append(app.as_mut(), lbls, m, val);
            r.unwrap();
            exp.entry(key.clone()).or_default().push(s);
            m += MINUTE;
        }
        commit(app.as_mut());
    };

    // In-order samples.
    add_sample(&mut exp_samples, &s1, 300, 300);
    assert_eq!(1.0, db.head.metrics.chunks_created.get());

    // Some ooo samples.
    add_sample(&mut exp_samples, &s1, 250, 260);
    add_sample(&mut exp_samples, &s1, 195, 249); // This creates some m-map chunks.
    assert_eq!(4.0, db.head.metrics.chunks_created.get());
    wbl_mmap_test_query(&db, &mut exp_samples);
    let (ooo_mint, ooo_maxt) = (minutes(195), minutes(260));

    // Collect the samples only present in the ooo m-map chunks.
    let (ms, created) = db.head.get_or_create(s1.hash(), &s1, false).unwrap();
    assert!(!created);
    let mut s1_mmap_samples = Vec::new();
    {
        let g = ms.lock();
        for mc in &g.ooo().expect("ooo").ooo_mmapped_chunks {
            let chk = db.head.chunk_disk_mapper.chunk(mc.r#ref).unwrap();
            let mut it = chk.iterator(None);
            s1_mmap_samples.extend(storage::expand_samples(&mut it).unwrap());
        }
    }
    assert!(!s1_mmap_samples.is_empty());

    // Making a copy of original state of WBL and Mmap files to use it later.
    let mmap_dir = mmapped_chunks_dir(&db.head.opts.chunk_dir_root);
    let wbl_dir = wbl(&db).dir().to_path_buf();
    db.close().unwrap();
    drop(db);

    let tmp1 = TempDir::new();
    let tmp2 = TempDir::new();
    let original_wbl_dir = tmp1.path().join("original_wbl");
    let original_mmap_dir = tmp2.path().join("original_mmap");
    fileutil::copy_dirs(&wbl_dir, &original_wbl_dir).unwrap();
    fileutil::copy_dirs(&mmap_dir, &original_mmap_dir).unwrap();
    let reset_wbl_to_original = || {
        remove_all(&wbl_dir);
        fileutil::copy_dirs(&original_wbl_dir, &wbl_dir).unwrap();
    };
    let reset_mmap_to_original = || {
        remove_all(&mmap_dir);
        fileutil::copy_dirs(&original_mmap_dir, &mmap_dir).unwrap();
    };

    // Restart DB with both WBL and M-map files for ooo data.
    {
        let db = reopen_db(&db_dir, &opts);
        assert_eq!(ooo_mint, db.head.min_ooo_time());
        assert_eq!(ooo_maxt, db.head.max_ooo_time());
        wbl_mmap_test_query(&db, &mut exp_samples);
    }

    // Restart DB with only WBL for ooo data.
    {
        remove_all(&mmap_dir);
        let db = reopen_db(&db_dir, &opts);
        assert_eq!(ooo_mint, db.head.min_ooo_time());
        assert_eq!(ooo_maxt, db.head.max_ooo_time());
        wbl_mmap_test_query(&db, &mut exp_samples);
    }

    // Restart DB with only M-map files for ooo data.
    {
        remove_all(&wbl_dir);
        reset_mmap_to_original();
        let db = reopen_db(&db_dir, &opts);
        assert_eq!(ooo_mint, db.head.min_ooo_time());
        assert_eq!(ooo_maxt, db.head.max_ooo_time());
        let in_order_sample = exp_samples[&s1.to_string()].last().unwrap().clone();
        let mut exp = BTreeMap::new();
        let mut v = s1_mmap_samples.clone();
        v.push(in_order_sample);
        exp.insert(s1.to_string(), v);
        wbl_mmap_test_query(&db, &mut exp);
    }

    // Restart DB with WBL+Mmap while increasing the OOOCapMax.
    {
        reset_wbl_to_original();
        reset_mmap_to_original();
        opts.out_of_order_cap_max = 60;
        let db = reopen_db(&db_dir, &opts);
        assert_eq!(ooo_mint, db.head.min_ooo_time());
        assert_eq!(ooo_maxt, db.head.max_ooo_time());
        wbl_mmap_test_query(&db, &mut exp_samples);
    }

    // Restart DB with WBL+Mmap while decreasing the OOOCapMax.
    {
        reset_mmap_to_original(); // We need to reset because new duplicate chunks can be written above.
        opts.out_of_order_cap_max = 10;
        let db = reopen_db(&db_dir, &opts);
        assert_eq!(ooo_mint, db.head.min_ooo_time());
        assert_eq!(ooo_maxt, db.head.max_ooo_time());
        wbl_mmap_test_query(&db, &mut exp_samples);
    }

    // Restart DB with WBL+Mmap while having no m-map markers in WBL.
    {
        reset_mmap_to_original(); // We need to reset because new duplicate chunks can be written above.

        // Removing m-map markers in WBL by rewriting it.
        let tmp3 = TempDir::new();
        let new_wbl = WL::new(tmp3.path().join("new_wbl"), compression::Type::None).unwrap();
        let sr = wlog::new_segments_reader(&original_wbl_dir).unwrap();
        let dec = Decoder::new(Logger::nop());
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
        remove_all(&wbl_dir);
        std::fs::rename(new_wbl.dir(), &wbl_dir).unwrap();

        opts.out_of_order_cap_max = 30;
        let db = reopen_db(&db_dir, &opts);
        assert_eq!(ooo_mint, db.head.min_ooo_time());
        assert_eq!(ooo_maxt, db.head.max_ooo_time());
        wbl_mmap_test_query(&db, &mut exp_samples);
    }
}

#[test]
fn test_wbl_and_mmap_replay() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("scenario {name}");
        run_wbl_and_mmap_replay(&scenario);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// TestOOOHistogramCompactionWithCounterResets.

/// Go `addSample` of the histogram compaction tests: appends and commits one histogram sample (the timestamp is
/// already in ms) and returns a copy of it.
fn add_histogram_sample(db: &DB, ts_ms: i64, l: &Labels, val: i64, hint: CounterResetHint, float_histogram: bool) -> Sample {
    let mut app = db.appender(&ctx());
    if float_histogram {
        let mut h = tu::generate_test_float_histogram(val);
        h.counter_reset_hint = hint;
        app.append_histogram(0, l, ts_ms, None, Some(&h)).unwrap();
        commit(app.as_mut());
        return Sample::float_histogram(0, ts_ms, h);
    }
    let mut h = tu::generate_test_histogram(val);
    h.counter_reset_hint = hint;
    app.append_histogram(0, l, ts_ms, Some(&h), None).unwrap();
    commit(app.as_mut());
    Sample::histogram(0, ts_ms, h)
}

/// The names and sizes of the WBL segments of the DB, after a sync (Go `os.ReadDir(db.head.wbl.Dir())`).
fn wbl_files(db: &DB) -> Vec<(String, i64)> {
    let w = wbl(db);
    w.sync().unwrap(); // Syncing to make sure wbl is flushed in windows.
    read_dir_names(w.dir()).unwrap().into_iter().map(|n| {
        let size = file_size(&w.dir().join(&n));
        (n, size)
    }).collect()
}

/// The names of the m-map files of the DB's head.
fn mmap_files(db: &DB) -> Vec<String> {
    read_dir_names(&mmapped_chunks_dir(&db.head.opts.chunk_dir_root)).unwrap()
}

#[test]
fn test_ooo_histogram_compaction_with_counter_resets() {
    use CounterResetHint::{CounterReset as CR, NotCounterReset as N, UnknownCounterReset as U};
    for float_histogram in [false, true] {
        eprintln!("floatHistogram={float_histogram}");
        let mut opts = default_opts();
        opts.out_of_order_cap_max = 30;
        opts.out_of_order_time_window = 500 * MINUTE;

        let db = open_db_no_compactions(&opts); // We want to manually call it.

        let series1 = lbl(&["foo", "bar1"]);
        let series2 = lbl(&["foo", "bar2"]);

        let (mut s1_pre, mut s2_pre, mut s1_post, mut s2_post) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());

        let add_sample = |ts: i64, l: &Labels, val: i64, hint: CounterResetHint| {
            add_histogram_sample(&db, ts * MINUTE, l, val, hint, float_histogram)
        };

        // Add an in-order sample to each series.
        let s = add_sample(520, &series1, 1000000, U);
        s1_pre.push(s.clone());
        s1_post.push(s);

        let s = add_sample(520, &series2, 1000000, U);
        s2_pre.push(s.clone());
        s2_post.push(s);

        // Verify that the in-memory ooo chunk is empty.
        let check_empty_ooo_chunk = |lbls: &Labels| assert!(ooo_is_nil(&db, lbls));

        check_empty_ooo_chunk(&series1);
        check_empty_ooo_chunk(&series2);

        // Add samples for series1. There are three head chunks that will be created:
        // Chunk 1 - Samples between 100 - 440. One explicit counter reset at ts 250.
        // Chunk 2 - Samples between 105 - 395. Overlaps with Chunk 1. One detected counter reset at ts 165.
        // Chunk 3 - Samples between 480 - 509. All within one block boundary. One detected counter reset at 490.

        // Chunk 1.
        // First add 10 samples.
        for i in (100..200).step_by(10) {
            let mut s = add_sample(i, &series1, 100000 + i, U);
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
                s = copy_with_counter_reset(&s, N);
            }
            s1_post.push(s);
        }
        // Explicit counter reset - the counter reset header is set to CounterReset but the value is higher
        // than for the previous timestamp. Explicit counter reset headers are actually ignored though, so when
        // reading the sample back you actually get unknown/not counter reset. This is as the chainSampleIterator
        // ignores existing headers and sets the header as UnknownCounterReset if the next sample is not in the same
        // chunk as the previous one, and counter resets always create a new chunk.
        // This case has been added to document what's happening, though it might not be the ideal behavior.
        let s = add_sample(250, &series1, 100000 + 250, CR);
        s1_pre.push(copy_with_counter_reset(&s, U));
        s1_post.push(copy_with_counter_reset(&s, N));

        // Add 19 more samples to complete a chunk.
        for i in (260..450).step_by(10) {
            let mut s = add_sample(i, &series1, 100000 + i, U);
            // The samples with timestamp less than 410 overlap with the samples from chunk 2, so before compaction,
            // they're all UnknownCounterReset. Samples greater than or equal to 410 don't overlap with other chunks
            // so they're always detected as NotCounterReset pre and post compaction.
            if i >= 410 {
                s = copy_with_counter_reset(&s, N);
            }
            s1_pre.push(s.clone());
            //
            // 360 is a block boundary, so after compaction its header is still UnknownCounterReset.
            if i != 360 {
                s = copy_with_counter_reset(&s, N);
            }
            s1_post.push(s);
        }

        // Chunk 2.
        // Add six OOO samples.
        for i in (105..165).step_by(10) {
            let s = add_sample(i, &series1, 100000 + i, U);
            // Samples overlap with chunk 1 so before compaction all headers are UnknownCounterReset.
            s1_pre.push(s.clone());
            s1_post.push(copy_with_counter_reset(&s, N));
        }

        // Add sample that will be detected as a counter reset.
        let s = add_sample(165, &series1, 100000, U);
        // Before compaction, sample has an UnknownCounterReset header due to the chainSampleIterator.
        s1_pre.push(s.clone());
        // After compaction, the sample's counter reset is still UnknownCounterReset as we cannot trust
        // CounterReset headers in chunks at the moment, so when reading the first sample in a chunk, its hint is
        // set to UnknownCounterReset.
        s1_post.push(s);

        // Add 23 more samples to complete a chunk.
        for i in (175..405).step_by(10) {
            let mut s = add_sample(i, &series1, 100000 + i, U);
            // Samples between 205-255 overlap with chunk 1 so before compaction those samples will have the
            // UnknownCounterReset header.
            if (205..255).contains(&i) {
                s = copy_with_counter_reset(&s, N);
            }
            s1_pre.push(s.clone());
            // 245 is the first sample >= the block boundary at 240, so it's still UnknownCounterReset after
            // compaction.
            if i != 245 {
                s = copy_with_counter_reset(&s, N);
            } else {
                s = copy_with_counter_reset(&s, U);
            }
            s1_post.push(s);
        }

        // Chunk 3.
        for i in 480..490 {
            let mut s = add_sample(i, &series1, 100000 + i, U);
            // No overlapping samples in other chunks, so all other samples will already be detected as
            // NotCounterReset before compaction.
            if i > 480 {
                s = copy_with_counter_reset(&s, N);
            }
            s1_pre.push(s.clone());
            // 480 is block boundary.
            if i == 480 {
                s = copy_with_counter_reset(&s, U);
            }
            s1_post.push(s);
        }
        // Counter reset.
        let s = add_sample(490, &series1, 100000, U);
        s1_pre.push(s.clone());
        s1_post.push(s);
        // Add some more samples after the counter reset.
        for i in 491..510 {
            let s = add_sample(i, &series1, 100000 + i, U);
            let s = copy_with_counter_reset(&s, N);
            s1_pre.push(s.clone());
            s1_post.push(s);
        }

        // Add samples for series2 - one chunk with one detected counter reset at 300.
        for i in (200..300).step_by(10) {
            let mut s = add_sample(i, &series2, 100000 + i, U);
            if i > 200 {
                s = copy_with_counter_reset(&s, N);
            }
            s2_pre.push(s.clone());
            if i == 240 {
                s = copy_with_counter_reset(&s, U);
            }
            s2_post.push(s);
        }
        // Counter reset.
        let s = add_sample(300, &series2, 100000, U);
        s2_pre.push(s.clone());
        s2_post.push(s);
        // Add some more samples after the counter reset.
        for i in (310..500).step_by(10) {
            let s = add_sample(i, &series2, 100000 + i, U);
            let mut s = copy_with_counter_reset(&s, N);
            s2_pre.push(s.clone());
            // 360 and 480 are block boundaries.
            if i == 360 || i == 480 {
                s = copy_with_counter_reset(&s, U);
            }
            s2_post.push(s);
        }

        // Sort samples (as OOO samples not added in time-order).
        sort_by_t(&mut s1_pre);
        sort_by_t(&mut s1_post);
        sort_by_t(&mut s2_pre);
        sort_by_t(&mut s2_post);

        let verify_db_samples = |s1_samples: &[Sample], s2_samples: &[Sample]| {
            let mut exp_res = BTreeMap::new();
            exp_res.insert(series1.to_string(), s1_samples.to_vec());
            exp_res.insert(series2.to_string(), s2_samples.to_vec());
            let act_res = query_db(&db, i64::MIN, i64::MAX, &[mre("foo", "bar.*")]);
            require_equal_series(&exp_res, &act_res, false);
        };

        // Verify DB samples before compaction.
        verify_db_samples(&s1_pre, &s2_pre);

        // Verify that the in-memory ooo chunk is not empty.
        let check_non_empty_ooo_chunk = |lbls: &Labels| {
            let (ms, created) = db.head.get_or_create(lbls.hash(), lbls, false).unwrap();
            assert!(!created);
            let g = ms.lock();
            assert!(g.ooo().unwrap().ooo_head_chunk.as_ref().unwrap().chunk.num_samples() > 0);
        };

        check_non_empty_ooo_chunk(&series1);
        check_non_empty_ooo_chunk(&series2);

        // No blocks before compaction.
        assert!(db.blocks().is_empty());

        // There is a 0th WBL file.
        let files = wbl_files(&db);
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
        let files = wbl_files(&db);
        assert_eq!(1, files.len());
        assert_eq!("00000001", files[0].0);
        assert_eq!(0, files[0].1);

        // OOO stuff should not be present in the Head now.
        check_empty_ooo_chunk(&series1);
        check_empty_ooo_chunk(&series2);

        let verify_block_samples = |block: &Block, from_mins: i64, to_mins: i64| {
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
            let (series1_samples, series2_samples) = (pick(&s1_post), pick(&s2_post));
            let mut exp_res = BTreeMap::new();
            if !series1_samples.is_empty() {
                exp_res.insert(series1.to_string(), series1_samples);
            }
            if !series2_samples.is_empty() {
                exp_res.insert(series2.to_string(), series2_samples);
            }
            let act_res = query_block(block, i64::MIN, i64::MAX, &[mre("foo", "bar.*")]);
            require_equal_series(&exp_res, &act_res, false);
        };

        // Checking for expected data in the blocks.
        let blocks = db.blocks();
        verify_block_samples(&blocks[0], 100, 119);
        verify_block_samples(&blocks[1], 120, 239);
        verify_block_samples(&blocks[2], 240, 359);
        verify_block_samples(&blocks[3], 360, 479);
        verify_block_samples(&blocks[4], 480, 509);

        // There should be a single m-map file.
        assert_eq!(1, mmap_files(&db).len());

        // Compact the in-order head and expect another block.
        // Since this is a forced compaction, this block is not aligned with 2h.
        db.compact_head(&RangeHead::new(db.head.clone(), 500 * MINUTE, 550 * MINUTE)).unwrap();
        let blocks = db.blocks();
        assert_eq!(6, blocks.len());
        verify_block_samples(&blocks[5], 520, 520);

        // Blocks created out of normal and OOO head now. But not merged.
        verify_db_samples(&s1_post, &s2_post);

        // The compaction also clears out the old m-map files. Including
        // the file that has ooo chunks.
        assert_eq!(vec!["000001".to_string()], mmap_files(&db));

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
fn test_interleaved_in_order_and_ooo_histogram_compaction_with_counter_resets() {
    use CounterResetHint::{NotCounterReset as N, UnknownCounterReset as U};
    for float_histogram in [false, true] {
        eprintln!("floatHistogram={float_histogram}");
        let mut opts = default_opts();
        opts.out_of_order_cap_max = 30;
        opts.out_of_order_time_window = 500 * MINUTE;

        let db = open_db_no_compactions(&opts); // We want to manually call it.

        let series1 = lbl(&["foo", "bar1"]);
        let add_sample = |ts: i64, l: &Labels, val: i64| add_histogram_sample(&db, ts, l, val, U, float_histogram);

        let mut exp_samples = Vec::new();
        let s = add_sample(0, &series1, 0);
        exp_samples.push(s);
        let s = add_sample(1, &series1, 10);
        exp_samples.push(copy_with_counter_reset(&s, N));
        let s = add_sample(3, &series1, 3);
        exp_samples.push(copy_with_counter_reset(&s, U));
        let s = add_sample(2, &series1, 0);
        exp_samples.push(copy_with_counter_reset(&s, U));

        // Sort samples (as OOO samples not added in time-order).
        sort_by_t(&mut exp_samples);

        let verify_db_samples = |s1_samples: &[Sample]| {
            let mut exp_res = BTreeMap::new();
            exp_res.insert(series1.to_string(), s1_samples.to_vec());
            let act_res = query_db(&db, i64::MIN, i64::MAX, &[mre("foo", "bar.*")]);
            require_equal_series(&exp_res, &act_res, false);
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
// TestOOOCompactionFailure.

/// Go `testOOOCompactionFailure`.
fn run_ooo_compaction_failure(scenario: &SampleTypeScenario) {
    let mut opts = default_opts();
    opts.out_of_order_cap_max = 30;
    opts.out_of_order_time_window = 300 * MINUTE;
    let db = open_db_no_compactions(&opts); // We want to manually call it.

    let series1 = lbl(&["foo", "bar1"]);

    let add_sample = |from_mins: i64, to_mins: i64| {
        let mut app = db.appender(&ctx());
        for m in from_mins..=to_mins {
            let ts = m * MINUTE;
            scenario.append(app.as_mut(), &series1, ts, ts).0.unwrap();
        }
        commit(app.as_mut());
    };

    // Add an in-order samples.
    add_sample(250, 350);

    // Add ooo samples that creates multiple chunks.
    add_sample(90, 310);

    // No blocks before compaction.
    assert!(db.blocks().is_empty());

    // There is a 0th WBL file.
    let verify_first_wbl_file_is_0 = |count: usize| {
        let files = wbl_files(&db);
        assert_eq!(count, files.len());
        assert_eq!("00000000", files[0].0);
        assert!(files[0].1 > 100);
    };
    verify_first_wbl_file_is_0(1);

    let verify_mmap_files = |exp: &[&str]| {
        let files = mmap_files(&db);
        assert_eq!(exp, files.iter().map(|s| s.as_str()).collect::<Vec<_>>().as_slice());
    };

    verify_mmap_files(&["000001"]);

    // OOO compaction fails 5 times.
    let original_compactor = db.compactor.read().unwrap().clone();
    *db.compactor.write().unwrap() = Arc::new(MockCompactorFailing { blocks: Mutex::new(Vec::new()), max: 0 });
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
    let old_blocks = db.blocks();
    assert_eq!(3, db.blocks().len());

    // Check that the ooo chunks were removed.
    assert!(ooo_is_nil(&db, &series1));

    // The failed compaction should not have left the ooo Head corrupted.
    // Hence, expect no new blocks with another OOO compaction call.
    db.compact_ooo_head(&ctx()).unwrap();
    assert_eq!(3, db.blocks().len());
    // adapted: Go compares the `[]*Block` slices; here the blocks' metas and directories.
    let ident = |bs: &[Block]| bs.iter().map(|b| (b.meta(), b.dir().to_path_buf())).collect::<Vec<_>>();
    assert_eq!(ident(&old_blocks), ident(&db.blocks()));

    // There should be a single m-map file.
    verify_mmap_files(&["000001"]);

    // All but last WBL file will be deleted.
    // 8 files in total (starting at 0) because of 7 compaction calls.
    let files = wbl_files(&db);
    assert_eq!(1, files.len());
    assert_eq!("00000007", files[0].0);
    assert_eq!(0, files[0].1);

    let verify_samples = |block: &Block, from_mins: i64, to_mins: i64| {
        let series1_samples: Vec<Sample> =
            (from_mins..=to_mins).map(|m| (scenario.sample_func)(m * MINUTE, m * MINUTE)).collect();
        let mut exp_res = BTreeMap::new();
        exp_res.insert(series1.to_string(), series1_samples);
        let act_res = query_block(block, i64::MIN, i64::MAX, &[mre("foo", "bar.*")]);
        require_equal_series(&exp_res, &act_res, true);
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

    // The compaction also clears out the old m-map files. Including
    // the file that has ooo chunks.
    verify_mmap_files(&["000001"]);
}

#[test]
fn test_ooo_compaction_failure() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("scenario {name}");
        run_ooo_compaction_failure(&scenario);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// TestWBLCorruption.

#[test]
fn test_wbl_corruption() {
    let mut opts = default_opts();
    opts.out_of_order_cap_max = 30;
    opts.out_of_order_time_window = 300 * MINUTE;

    let tmp = TempDir::new();
    let mut db = reopen_db(tmp.path(), &opts);

    let series1 = lbl(&["foo", "bar1"]);
    let mut all_samples = Vec::new();
    let mut exp_after_restart = Vec::new();
    let mut add_samples = |db: &DB, from_mins: i64, to_mins: i64, after_restart: bool| {
        let mut app = db.appender(&ctx());
        for m in from_mins..=to_mins {
            let ts = m * MINUTE;
            app.append(0, &series1, ts, ts as f64).unwrap();
            all_samples.push(fs(ts, ts as f64));
            if after_restart {
                exp_after_restart.push(fs(ts, ts as f64));
            }
        }
        commit(app.as_mut());
    };

    // Add an in-order samples.
    add_samples(&db, 340, 350, true);

    // OOO samples.
    add_samples(&db, 90, 99, true);
    add_samples(&db, 100, 119, true);
    add_samples(&db, 120, 130, true);

    // Moving onto the second file.
    wbl(&db).next_segment().unwrap();

    // More OOO samples.
    add_samples(&db, 200, 230, true);
    add_samples(&db, 240, 255, true);

    // We corrupt WBL after the sample at 255. So everything added later
    // should be deleted after replay.

    let wbl_dir = wbl(&db).dir().to_path_buf();
    // segmentSize returns the on-disk size of the named WBL segment.
    let segment_size = |name: &str| file_size(&wbl_dir.join(name));

    // Checking where we corrupt it.
    wbl(&db).sync().unwrap(); // Syncing to make sure wbl is flushed in windows.
    let files = read_dir_names(&wbl_dir).unwrap();
    assert_eq!(2, files.len());
    let corrupt_index = segment_size(&files[1]);
    let corrupt_file_path = wbl_dir.join(&files[1]);

    // Corrupt the WBL by adding a malformed record.
    wbl(&db).log(&[record::Type::Samples as u8, 99, 9, 99, 9, 99, 9, 99]).unwrap();

    // More samples after the corruption point.
    add_samples(&db, 260, 280, false);
    add_samples(&db, 290, 300, false);

    // Another file.
    wbl(&db).next_segment().unwrap();

    add_samples(&db, 310, 320, false);

    // Verifying that we have data after corruption point.
    wbl(&db).sync().unwrap(); // Syncing to make sure wbl is flushed in windows.
    let files = read_dir_names(&wbl_dir).unwrap();
    assert_eq!(3, files.len());
    assert!(segment_size(&files[1]) > corrupt_index);
    assert!(segment_size(&files[0]) > 100);
    assert!(segment_size(&files[2]) > 100);

    let verify_samples = |db: &DB, exp_samples: &mut Vec<Sample>| {
        sort_by_t(exp_samples);
        let mut exp_res = BTreeMap::new();
        exp_res.insert(series1.to_string(), exp_samples.clone());
        let act_res = query_db(db, i64::MIN, i64::MAX, &[mre("foo", "bar.*")]);
        assert_eq!(exp_res, act_res);
    };

    drop(add_samples);
    verify_samples(&db, &mut all_samples);

    db.close().unwrap();

    // We want everything to be replayed from the WBL. So we delete the m-map files.
    remove_all(mmapped_chunks_dir(&db.head.opts.chunk_dir_root));

    // Restart does the replay and repair.
    db = reopen_db(tmp.path(), &opts);
    assert_eq!(1.0, db.head.metrics.wal_corruptions_total.get());
    assert!(exp_after_restart.len() < all_samples.len());
    verify_samples(&db, &mut exp_after_restart);

    // Verify that it did the repair on disk.
    let files = read_dir_names(&wbl_dir).unwrap();
    assert_eq!(3, files.len());
    assert!(segment_size(&files[0]) > 100);
    assert_eq!(0, segment_size(&files[2]));
    assert_eq!(corrupt_file_path, wbl_dir.join(&files[1]));

    // Verifying that everything after the corruption point is set to 0.
    let b = std::fs::read(&corrupt_file_path).unwrap();
    let sum: i64 = b[corrupt_index as usize..].iter().map(|&v| v as i64).sum();
    assert_eq!(0, sum);

    // Another restart, everything normal with no repair.
    db.close().unwrap();
    db = reopen_db(tmp.path(), &opts);
    assert_eq!(0.0, db.head.metrics.wal_corruptions_total.get());
    verify_samples(&db, &mut exp_after_restart);
    drop(db);
}

// ---------------------------------------------------------------------------------------------------------------
// TestOOOMmapCorruption.

/// Go `testOOOMmapCorruption`.
fn run_ooo_mmap_corruption(scenario: &SampleTypeScenario) {
    use std::os::unix::fs::FileExt;

    let mut opts = default_opts();
    opts.out_of_order_cap_max = 10;
    opts.out_of_order_time_window = 300 * MINUTE;

    let tmp = TempDir::new();
    let mut db = reopen_db(tmp.path(), &opts);

    let series1 = lbl(&["foo", "bar1"]);
    let mut all_samples = Vec::new();
    let mut exp_in_mmap_chunks = Vec::new();
    let mut add_samples = |db: &DB, from_mins: i64, to_mins: i64, in_mmap_after_corruption: bool| {
        let mut app = db.appender(&ctx());
        for m in from_mins..=to_mins {
            let ts = m * MINUTE;
            let (r, s) = scenario.append(app.as_mut(), &series1, ts, ts);
            r.unwrap();
            all_samples.push(s.clone());
            if in_mmap_after_corruption {
                exp_in_mmap_chunks.push(s);
            }
        }
        commit(app.as_mut());
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
    drop(add_samples);

    let verify_samples = |db: &DB, exp_samples: &mut Vec<Sample>| {
        sort_by_t(exp_samples);
        let mut exp_res = BTreeMap::new();
        exp_res.insert(series1.to_string(), exp_samples.clone());
        let act_res = query_db(db, i64::MIN, i64::MAX, &[mre("foo", "bar.*")]);
        require_equal_series(&exp_res, &act_res, true);
    };

    verify_samples(&db, &mut all_samples);

    // Verifying existing files.
    let mmap_dir = mmapped_chunks_dir(&db.head.opts.chunk_dir_root);
    let files = read_dir_names(&mmap_dir).unwrap();
    assert_eq!(3, files.len());

    // Corrupting the 2nd file.
    let f = std::fs::OpenOptions::new().read(true).write(true).open(mmap_dir.join(&files[1])).unwrap();
    f.write_at(&[99, 9, 99, 9, 99], 20).unwrap();
    drop(f);
    let first_file_name = files[0].clone();

    db.close().unwrap();

    // Moving OOO WBL to use it later.
    let wbl_dir = wbl(&db).dir().to_path_buf();
    let tmp2 = TempDir::new();
    let wbl_dir_tmp = tmp2.path().join("wbl_tmp");
    std::fs::rename(&wbl_dir, &wbl_dir_tmp).unwrap();

    // Restart does the replay and repair of m-map files.
    db = reopen_db(tmp.path(), &opts);
    assert_eq!(1.0, db.head.metrics.mmap_chunk_corruption_total.get());
    assert!(exp_in_mmap_chunks.len() < all_samples.len());

    // Since there is no WBL, only samples from m-map chunks comes in the query.
    verify_samples(&db, &mut exp_in_mmap_chunks);

    // Verify that it did the repair on disk. All files from the point of corruption
    // should be deleted.
    let files = read_dir_names(&mmap_dir).unwrap();
    assert_eq!(1, files.len());
    assert!(file_size(&mmap_dir.join(&files[0])) > 100);
    assert_eq!(first_file_name, files[0]);

    // Another restart, everything normal with no repair.
    db.close().unwrap();
    db = reopen_db(tmp.path(), &opts);
    assert_eq!(0.0, db.head.metrics.mmap_chunk_corruption_total.get());
    verify_samples(&db, &mut exp_in_mmap_chunks);

    // Restart again with the WBL, all samples should be present now.
    db.close().unwrap();
    remove_all(&wbl_dir);
    std::fs::rename(&wbl_dir_tmp, &wbl_dir).unwrap();
    db = reopen_db(tmp.path(), &opts);
    verify_samples(&db, &mut all_samples);
    drop(db);
}

#[test]
fn test_ooo_mmap_corruption() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("scenario {name}");
        run_ooo_mmap_corruption(&scenario);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// TestOutOfOrderRuntimeConfig.

/// Go `addSamples` of `testOutOfOrderRuntimeConfig`.
fn rc_add_samples(
    scenario: &SampleTypeScenario,
    db: &DB,
    from_mins: i64,
    to_mins: i64,
    success: bool,
    all_samples: &mut Vec<Sample>,
) {
    let series1 = lbl(&["foo", "bar1"]);
    let mut app = db.appender(&ctx());
    for m in from_mins..=to_mins {
        let ts = m * MINUTE;
        let (r, s) = scenario.append(app.as_mut(), &series1, ts, ts);
        if success {
            r.unwrap();
            all_samples.push(s);
        } else {
            assert!(r.is_err());
        }
    }
    commit(app.as_mut());
}

/// Go `verifySamples` of the OOO tests that query the whole DB with `foo=~"bar.*"` and ignore counter resets.
fn verify_db_samples_sorted(db: &DB, exp_samples: &mut [Sample]) {
    sort_by_t(exp_samples);
    let mut exp_res = BTreeMap::new();
    exp_res.insert(lbl(&["foo", "bar1"]).to_string(), exp_samples.to_vec());
    let act_res = query_db(db, i64::MIN, i64::MAX, &[mre("foo", "bar.*")]);
    require_equal_series(&exp_res, &act_res, true);
}

/// Go `testOutOfOrderRuntimeConfig`.
fn run_out_of_order_runtime_config(scenario: &SampleTypeScenario) {
    let get_db = |ooo_time_window: i64| {
        let mut opts = default_opts();
        opts.out_of_order_time_window = ooo_time_window;
        open_db_no_compactions(&opts)
    };
    let make_config = |ooo_time_window: i64| ooo_config(ooo_time_window * MINUTE);
    let add_samples = |db: &DB, from: i64, to: i64, success: bool, all: &mut Vec<Sample>| {
        rc_add_samples(scenario, db, from, to, success, all)
    };

    let do_ooo_compaction = |db: &DB| {
        // WBL is not empty.
        let size = wbl(db).size().unwrap();
        assert!(size > 0);

        assert!(db.blocks().is_empty());
        db.compact_ooo_head(&ctx()).unwrap();
        assert!(!db.blocks().is_empty());

        // WBL is empty.
        let size = wbl(db).size().unwrap();
        assert_eq!(0, size);
    };

    // increase time window
    {
        let mut all_samples = Vec::new();
        let db = get_db(30 * MINUTE);

        // In-order.
        add_samples(&db, 300, 310, true, &mut all_samples);

        // OOO up to 30m old is success.
        add_samples(&db, 281, 290, true, &mut all_samples);

        // OOO of 59m old fails.
        let mut s = Vec::new();
        add_samples(&db, 251, 260, false, &mut s);
        assert!(s.is_empty());
        verify_db_samples_sorted(&db, &mut all_samples);

        let old_wbl = wbl(&db);

        // Increase time window and try adding again.
        db.apply_config(&make_config(60)).unwrap();
        add_samples(&db, 251, 260, true, &mut all_samples);

        // WBL does not change.
        assert!(Arc::ptr_eq(&old_wbl, &wbl(&db)));

        do_ooo_compaction(&db);
        verify_db_samples_sorted(&db, &mut all_samples);
    }

    // decrease time window and increase again
    {
        let mut all_samples = Vec::new();
        let db = get_db(60 * MINUTE);

        // In-order.
        add_samples(&db, 300, 310, true, &mut all_samples);

        // OOO up to 59m old is success.
        add_samples(&db, 251, 260, true, &mut all_samples);

        let old_wbl = wbl(&db);
        // Decrease time window.
        db.apply_config(&make_config(30)).unwrap();

        // OOO of 49m old fails.
        let mut s = Vec::new();
        add_samples(&db, 261, 270, false, &mut s);
        assert!(s.is_empty());

        // WBL does not change.
        assert!(Arc::ptr_eq(&old_wbl, &wbl(&db)));

        verify_db_samples_sorted(&db, &mut all_samples);

        // Increase time window again and check
        db.apply_config(&make_config(60)).unwrap();
        add_samples(&db, 261, 270, true, &mut all_samples);
        verify_db_samples_sorted(&db, &mut all_samples);

        // WBL does not change.
        assert!(Arc::ptr_eq(&old_wbl, &wbl(&db)));

        do_ooo_compaction(&db);
        verify_db_samples_sorted(&db, &mut all_samples);
    }

    // disabled to enabled
    {
        let mut all_samples = Vec::new();
        let db = get_db(0);

        // In-order.
        add_samples(&db, 300, 310, true, &mut all_samples);

        // OOO fails.
        let mut s = Vec::new();
        add_samples(&db, 251, 260, false, &mut s);
        assert!(s.is_empty());
        verify_db_samples_sorted(&db, &mut all_samples);

        assert!(db.head.wbl().is_none());

        // Increase time window and try adding again.
        db.apply_config(&make_config(60)).unwrap();
        add_samples(&db, 251, 260, true, &mut all_samples);

        // WBL gets created.
        assert!(db.head.wbl().is_some());

        verify_db_samples_sorted(&db, &mut all_samples);

        // OOO compaction works now.
        do_ooo_compaction(&db);
        verify_db_samples_sorted(&db, &mut all_samples);
    }

    // enabled to disabled
    {
        let mut all_samples = Vec::new();
        let db = get_db(60 * MINUTE);

        // In-order.
        add_samples(&db, 300, 310, true, &mut all_samples);

        // OOO up to 59m old is success.
        add_samples(&db, 251, 260, true, &mut all_samples);

        let old_wbl = wbl(&db);
        // Time Window to 0, hence disabled.
        db.apply_config(&make_config(0)).unwrap();

        // OOO within old time window fails.
        let mut s = Vec::new();
        add_samples(&db, 290, 309, false, &mut s);
        assert!(s.is_empty());

        // WBL does not change and is not removed.
        assert!(Arc::ptr_eq(&old_wbl, &wbl(&db)));

        verify_db_samples_sorted(&db, &mut all_samples);

        // Compaction still works after disabling with WBL cleanup.
        do_ooo_compaction(&db);
        verify_db_samples_sorted(&db, &mut all_samples);
    }

    // disabled to disabled
    {
        let mut all_samples = Vec::new();
        let db = get_db(0);

        // In-order.
        add_samples(&db, 300, 310, true, &mut all_samples);

        // OOO fails.
        let mut s = Vec::new();
        add_samples(&db, 290, 309, false, &mut s);
        assert!(s.is_empty());
        verify_db_samples_sorted(&db, &mut all_samples);
        assert!(db.head.wbl().is_none());

        // Time window to 0.
        db.apply_config(&make_config(0)).unwrap();

        // OOO still fails.
        let mut s = Vec::new();
        add_samples(&db, 290, 309, false, &mut s);
        assert!(s.is_empty());
        verify_db_samples_sorted(&db, &mut all_samples);
        assert!(db.head.wbl().is_none());
    }
}

#[test]
fn test_out_of_order_runtime_config() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("scenario {name}");
        run_out_of_order_runtime_config(&scenario);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// TestNoGapAfterRestartWithOOO.

/// Go `testNoGapAfterRestartWithOOO`.
fn run_no_gap_after_restart_with_ooo(scenario: &SampleTypeScenario) {
    let series1 = lbl(&["foo", "bar1"]);
    let add_samples = |db: &DB, from_mins: i64, to_mins: i64, success: bool| {
        let mut app = db.appender(&ctx());
        for m in from_mins..=to_mins {
            let ts = m * MINUTE;
            let (r, _) = scenario.append(app.as_mut(), &series1, ts, ts);
            if success {
                r.unwrap();
            } else {
                assert!(r.is_err());
            }
        }
        commit(app.as_mut());
    };

    let verify_samples = |db: &DB, from_mins: i64, to_mins: i64| {
        let exp_samples: Vec<Sample> =
            (from_mins..=to_mins).map(|m| (scenario.sample_func)(m * MINUTE, m * MINUTE)).collect();
        let mut exp_res = BTreeMap::new();
        exp_res.insert(series1.to_string(), exp_samples);
        let act_res = query_db(db, i64::MIN, i64::MAX, &[mre("foo", "bar.*")]);
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
    let cases = vec![
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
        let mut opts = default_opts();
        opts.out_of_order_time_window = 30 * MINUTE;
        let tmp = TempDir::new();
        let mut db = reopen_db(tmp.path(), &opts);
        db.disable_compactions();

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

        db = reopen_db(tmp.path(), &opts);
        db.disable_compactions();

        verify_block_ranges(&db);
        assert_eq!(c.head_mint * MINUTE, db.head.min_time());
        assert_eq!(c.head_maxt * MINUTE, db.head.max_time());
        verify_samples(&db, c.in_order_mint, c.in_order_maxt);
        drop(db);
    }
}

#[test]
fn test_no_gap_after_restart_with_ooo() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("scenario {name}");
        run_no_gap_after_restart_with_ooo(&scenario);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// TestWblReplayAfterOOODisableAndRestart and TestPanicOnApplyConfig.

/// Go `testWblReplayAfterOOODisableAndRestart`.
fn run_wbl_replay_after_ooo_disable_and_restart(scenario: &SampleTypeScenario) {
    let mut opts = default_opts();
    opts.out_of_order_time_window = 60 * MINUTE;

    let tmp = TempDir::new();
    let mut db = reopen_db(tmp.path(), &opts);

    let mut all_samples = Vec::new();
    // In-order samples.
    rc_add_samples(scenario, &db, 290, 300, true, &mut all_samples);
    // OOO samples.
    rc_add_samples(scenario, &db, 250, 260, true, &mut all_samples);

    verify_db_samples_sorted(&db, &mut all_samples);

    // Restart DB with OOO disabled.
    db.close().unwrap();

    opts.out_of_order_time_window = 0;
    db = reopen_db(tmp.path(), &opts);

    // We can still query OOO samples when OOO is disabled.
    verify_db_samples_sorted(&db, &mut all_samples);
    drop(db);
}

#[test]
fn test_wbl_replay_after_ooo_disable_and_restart() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("scenario {name}");
        run_wbl_replay_after_ooo_disable_and_restart(&scenario);
    }
}

/// Go `testPanicOnApplyConfig`.
fn run_panic_on_apply_config(scenario: &SampleTypeScenario) {
    let mut opts = default_opts();
    opts.out_of_order_time_window = 60 * MINUTE;

    let tmp = TempDir::new();
    let mut db = reopen_db(tmp.path(), &opts);

    let mut all_samples = Vec::new();
    // In-order samples.
    rc_add_samples(scenario, &db, 290, 300, true, &mut all_samples);
    // OOO samples.
    rc_add_samples(scenario, &db, 250, 260, true, &mut all_samples);

    // Restart DB with OOO disabled.
    db.close().unwrap();

    opts.out_of_order_time_window = 0;
    db = reopen_db(tmp.path(), &opts);

    // ApplyConfig with OOO enabled and expect no panic.
    db.apply_config(&ooo_config(60 * MINUTE)).unwrap();
    drop(db);
}

#[test]
fn test_panic_on_apply_config() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("scenario {name}");
        run_panic_on_apply_config(&scenario);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// TestDiskFillingUpAfterDisablingOOO.

/// A V1 appender as `storage.LimitedAppenderV1` (in Go a `storage.Appender` is one).
struct V1Limited(Box<dyn Appender>);

impl storage::AppenderTransaction for V1Limited {
    fn commit(&mut self) -> Option<Error> {
        self.0.commit()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.0.rollback()
    }
}

impl LimitedAppenderV1 for V1Limited {
    fn append(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        self.0.append(r, l, t, v)
    }
    fn append_histogram(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.0.append_histogram(r, l, t, h, fh)
    }
}

/// Go `scenario.appendFunc` through a `storage.LimitedAppenderV1`.
fn scenario_append_limited(
    sc: &SampleTypeScenario,
    app: &mut dyn LimitedAppenderV1,
    lbls: &Labels,
    ts: i64,
    value: i64,
) -> (Result<SeriesRef, Error>, Sample) {
    let s = (sc.sample_func)(ts, value);
    let r = if s.h.is_some() || s.fh.is_some() {
        app.append_histogram(0, lbls, ts, s.h.as_deref(), s.fh.as_deref())
    } else {
        app.append(0, lbls, ts, s.f)
    };
    (r, s)
}

/// Go `testDiskFillingUpAfterDisablingOOO`.
fn run_disk_filling_up_after_disabling_ooo(scenario: &SampleTypeScenario, app_v2: bool) {
    let mut opts = default_opts();
    opts.out_of_order_time_window = 60 * MINUTE;
    // Use lower SamplesPerChunk and OutOfOrderCapMax so we need fewer samples
    // to fill chunks, reducing the overall test time significantly
    // (important for slow CI like i386 which can be 60x+ slower).
    opts.samples_per_chunk = 15;
    opts.out_of_order_cap_max = 5;
    // Reduce the chunk segment size from the 512MB default: the test only writes
    // ~80 samples so 1MB is sufficient and avoids large file pre-allocations
    // during compaction on slow or constrained CI environments.
    opts.max_block_chunk_segment_size = 1024 * 1024;

    let tmp = TempDir::new();
    let mut db = reopen_db(tmp.path(), &opts);
    db.disable_compactions();

    let series1 = lbl(&["foo", "bar1"]);

    // Use step of 5 minutes to reduce sample count while preserving time ranges
    // needed for compaction triggers. This reduces total samples from ~411 to ~83.
    let add_samples = |db: &DB, from_mins: i64, to_mins: i64| {
        let mut app: Box<dyn LimitedAppenderV1> = if app_v2 {
            storage::appender_v2_as_limited_v1(db.appender_v2(&ctx()))
        } else {
            Box::new(V1Limited(db.appender(&ctx())))
        };
        let mut m = from_mins;
        while m <= to_mins {
            let ts = m * MINUTE;
            scenario_append_limited(scenario, app.as_mut(), &series1, ts, ts).0.unwrap();
            m += 5;
        }
        let e = app.commit();
        assert!(e.is_none(), "{e:?}");
    };

    // In-order samples.
    add_samples(&db, 290, 300);
    // OOO samples.
    add_samples(&db, 250, 299);

    // Restart DB with OOO disabled.
    db.close().unwrap();

    opts.out_of_order_time_window = 0;
    db = reopen_db(tmp.path(), &opts);
    db.disable_compactions();

    let ms = db.head.series.get_by_hash(series1.hash(), &series1).expect("series");
    assert!(
        !ms.lock().ooo().expect("ooo").ooo_mmapped_chunks.is_empty(),
        "OOO mmap chunk was not replayed"
    );

    let check_mmap_file_contents = |db: &DB, contains: &[&str], not_contains: &[&str]| {
        let fnames = mmap_files(db);
        for f in contains {
            assert!(fnames.iter().any(|n| n == f), "{fnames:?} should contain {f}");
        }
        for f in not_contains {
            assert!(!fnames.iter().any(|n| n == f), "{fnames:?} should not contain {f}");
        }
    };

    // Add in-order samples until ready for compaction.
    add_samples(&db, 301, 500);

    // Check that m-map files gets deleted properly after compactions.

    db.head.mmap_head_chunks();
    check_mmap_file_contents(&db, &["000001", "000002"], &[]);

    db.compact(&ctx()).unwrap();

    check_mmap_file_contents(&db, &["000002"], &["000001"]);
    assert!(ms.lock().ooo().is_none(), "OOO mmap chunk was not compacted");

    add_samples(&db, 501, 650);
    db.head.mmap_head_chunks();
    check_mmap_file_contents(&db, &["000002", "000003"], &["000001"]);

    db.compact(&ctx()).unwrap();
    check_mmap_file_contents(&db, &[], &["000001", "000002", "000003"]);

    // Verify that WBL is empty.
    let files = read_dir_names(wbl(&db).dir()).unwrap();
    assert_eq!(1, files.len()); // Last empty file after compaction.
    assert_eq!(0, file_size(&wbl(&db).dir().join(&files[0])));
    drop(db);
}

#[test]
fn test_disk_filling_up_after_disabling_ooo() {
    for app_v2 in [true, false] {
        for (name, scenario) in sample_type_scenarios() {
            eprintln!("sample={name}/appV2={app_v2}");
            run_disk_filling_up_after_disabling_ooo(&scenario, app_v2);
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// TestHistogramAppendAndQuery.

/// The integer histogram of a sample (Go `s.H()`), mutable.
fn hmut(s: &mut Sample) -> &mut Histogram {
    s.h.as_deref_mut().expect("histogram")
}

/// The float histogram of a sample (Go `s.FH()`), mutable.
fn fhmut(s: &mut Sample) -> &mut FloatHistogram {
    s.fh.as_deref_mut().expect("float histogram")
}

/// Go `testHistogramAppendAndQueryHelper`.
fn run_histogram_append_and_query(float_histogram: bool) {
    use CounterResetHint::{NotCounterReset as N, UnknownCounterReset as U};
    let db = new_test_db(vec![]);
    let minute = |m: i64| m * MINUTE;

    let append_histogram = |lbls: &Labels, ts_minute: i64, h: &Histogram, exp: &mut Vec<Sample>, exp_crh: CounterResetHint| {
        let mut app = db.appender(&ctx());
        let r = if float_histogram {
            let r = app.append_histogram(0, lbls, minute(ts_minute), None, Some(&h.to_float()));
            let mut efh = h.to_float();
            efh.counter_reset_hint = exp_crh;
            exp.push(Sample::float_histogram(0, minute(ts_minute), efh));
            r
        } else {
            let r = app.append_histogram(0, lbls, minute(ts_minute), Some(&h.copy()), None);
            let mut eh = h.copy();
            eh.counter_reset_hint = exp_crh;
            exp.push(Sample::histogram(0, minute(ts_minute), eh));
            r
        };
        r.unwrap();
        commit(app.as_mut());
    };
    let append_float = |lbls: &Labels, ts_minute: i64, val: f64, exp: &mut Vec<Sample>| {
        let mut app = db.appender(&ctx());
        app.append(0, lbls, minute(ts_minute), val).unwrap();
        commit(app.as_mut());
        exp.push(fs(minute(ts_minute), val));
    };

    let test_query = |name: &str, value: &str, exp: BTreeMap<String, Vec<Sample>>| {
        let act = query_db(&db, i64::MIN, i64::MAX, &[mre(name, value)]);
        assert_eq!(exp, act);
    };
    let one = |l: &Labels, exp: &[Sample]| {
        let mut m = BTreeMap::new();
        m.insert(l.to_string(), exp.to_vec());
        m
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
        ..Histogram::default()
    };

    let series1 = lbl(&["foo", "bar1"]);
    let series2 = lbl(&["foo", "bar2"]);
    let series3 = lbl(&["foo", "bar3"]);
    let series4 = lbl(&["foo", "bar4"]);
    let (mut exp1, mut exp2, mut exp3, mut exp4) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());

    // TODO(codesome): test everything for negative buckets as well.
    // series with only histograms
    {
        let mut h = base_h.copy(); // This is shared across all sub tests.

        append_histogram(&series1, 100, &h, &mut exp1, U);
        test_query("foo", "bar1", one(&series1, &exp1));

        h.positive_buckets[0] += 1;
        h.negative_buckets[0] += 2;
        h.count += 10;
        append_histogram(&series1, 101, &h, &mut exp1, N);
        test_query("foo", "bar1", one(&series1, &exp1));

        // changing schema
        {
            h.schema = 2;
            append_histogram(&series1, 102, &h, &mut exp1, U);
            test_query("foo", "bar1", one(&series1, &exp1));

            // Schema back to old.
            h.schema = 1;
            append_histogram(&series1, 103, &h, &mut exp1, U);
            test_query("foo", "bar1", one(&series1, &exp1));
        }

        // new buckets incoming
        {
            // In the previous unit test, during the last histogram append, we
            // changed the schema and  that caused a new chunk creation. Because
            // of the next append the layout of the last histogram will change
            // because the chunk will be re-encoded. So this forces us to modify
            // the last histogram in exp1 so when we query we get the expected
            // results.
            let l = exp1.len();
            if float_histogram {
                let lh = fhmut(&mut exp1[l - 1]);
                lh.positive_spans[1].length += 1;
                lh.positive_buckets.push(0.0);
            } else {
                let lh = hmut(&mut exp1[l - 1]);
                lh.positive_spans[1].length += 1;
                lh.positive_buckets.push(-2); // -2 makes the last bucket 0.
            }

            // This histogram with new bucket at the end causes the re-encoding of the previous histogram.
            // Hence the previous histogram is recoded into this new layout.
            // But the query returns the histogram from the in-memory buffer, hence we don't see the recode here
            // yet.
            h.positive_spans[1].length += 1;
            h.positive_buckets.push(1);
            h.count += 3;
            append_histogram(&series1, 104, &h, &mut exp1, N);
            test_query("foo", "bar1", one(&series1, &exp1));

            // Because of the previous two histograms being on the active chunk,
            // and the next append is only adding a new bucket, the active chunk
            // will be re-encoded to the new layout.
            let l = exp1.len();
            if float_histogram {
                let lh = fhmut(&mut exp1[l - 2]);
                lh.positive_spans[0].length += 1;
                lh.positive_spans[1].offset -= 1;
                lh.positive_buckets = vec![2.0, 3.0, 0.0, 2.0, 2.0, 0.0];

                let lh = fhmut(&mut exp1[l - 1]);
                lh.positive_spans[0].length += 1;
                lh.positive_spans[1].offset -= 1;
                lh.positive_buckets = vec![2.0, 3.0, 0.0, 2.0, 2.0, 3.0];
            } else {
                let lh = hmut(&mut exp1[l - 2]);
                lh.positive_spans[0].length += 1;
                lh.positive_spans[1].offset -= 1;
                lh.positive_buckets = vec![2, 1, -3, 2, 0, -2];

                let lh = hmut(&mut exp1[l - 1]);
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
            test_query("foo", "bar1", one(&series1, &exp1));

            // We add 4 more histograms to clear out the buffer and see the re-encoded histograms.
            append_histogram(&series1, 106, &h, &mut exp1, N);
            append_histogram(&series1, 107, &h, &mut exp1, N);
            append_histogram(&series1, 108, &h, &mut exp1, N);
            append_histogram(&series1, 109, &h, &mut exp1, N);

            // Update the expected histograms to reflect the re-encoding.
            let l = exp1.len();
            if float_histogram {
                let spans = exp1[l - 1].fh.as_ref().unwrap().positive_spans.clone();
                let h7 = fhmut(&mut exp1[l - 7]);
                h7.positive_spans = spans.clone();
                h7.positive_buckets = vec![2.0, 3.0, 0.0, 2.0, 2.0, 0.0];

                let h6 = fhmut(&mut exp1[l - 6]);
                h6.positive_spans = spans;
                h6.positive_buckets = vec![2.0, 3.0, 0.0, 2.0, 2.0, 3.0];
            } else {
                let spans = exp1[l - 1].h.as_ref().unwrap().positive_spans.clone();
                let h7 = hmut(&mut exp1[l - 7]);
                h7.positive_spans = spans.clone();
                h7.positive_buckets = vec![2, 1, -3, 2, 0, -2]; // -3 and -2 are the empty buckets.

                let h6 = hmut(&mut exp1[l - 6]);
                h6.positive_spans = spans;
                h6.positive_buckets = vec![2, 1, -3, 2, 0, 1]; // -3 is the empty bucket.
            }

            test_query("foo", "bar1", one(&series1, &exp1));
        }

        // buckets disappearing
        {
            h.positive_spans[1].length -= 1;
            h.positive_buckets.pop();
            h.count -= 3;
            append_histogram(&series1, 110, &h, &mut exp1, U);
            test_query("foo", "bar1", one(&series1, &exp1));
        }
    }

    // series starting with float and then getting histograms
    {
        append_float(&series2, 100, 100.0, &mut exp2);
        append_float(&series2, 101, 101.0, &mut exp2);
        append_float(&series2, 102, 102.0, &mut exp2);
        test_query("foo", "bar2", one(&series2, &exp2));

        let h = base_h.copy();
        append_histogram(&series2, 103, &h, &mut exp2, U);
        append_histogram(&series2, 104, &h, &mut exp2, N);
        append_histogram(&series2, 105, &h, &mut exp2, N);
        test_query("foo", "bar2", one(&series2, &exp2));

        // Switching between float and histograms again.
        append_float(&series2, 106, 106.0, &mut exp2);
        append_float(&series2, 107, 107.0, &mut exp2);
        test_query("foo", "bar2", one(&series2, &exp2));

        append_histogram(&series2, 108, &h, &mut exp2, U);
        append_histogram(&series2, 109, &h, &mut exp2, N);
        test_query("foo", "bar2", one(&series2, &exp2));
    }

    // series starting with histogram and then getting float
    {
        let h = base_h.copy();
        append_histogram(&series3, 101, &h, &mut exp3, U);
        append_histogram(&series3, 102, &h, &mut exp3, N);
        append_histogram(&series3, 103, &h, &mut exp3, N);
        test_query("foo", "bar3", one(&series3, &exp3));

        append_float(&series3, 104, 100.0, &mut exp3);
        append_float(&series3, 105, 101.0, &mut exp3);
        append_float(&series3, 106, 102.0, &mut exp3);
        test_query("foo", "bar3", one(&series3, &exp3));

        // Switching between histogram and float again.
        append_histogram(&series3, 107, &h, &mut exp3, U);
        append_histogram(&series3, 108, &h, &mut exp3, N);
        test_query("foo", "bar3", one(&series3, &exp3));

        append_float(&series3, 109, 106.0, &mut exp3);
        append_float(&series3, 110, 107.0, &mut exp3);
        test_query("foo", "bar3", one(&series3, &exp3));
    }

    // query mix of histogram and float series
    {
        // A float only series.
        append_float(&series4, 100, 100.0, &mut exp4);
        append_float(&series4, 101, 101.0, &mut exp4);
        append_float(&series4, 102, 102.0, &mut exp4);

        let mut exp = BTreeMap::new();
        exp.insert(series1.to_string(), exp1.clone());
        exp.insert(series2.to_string(), exp2.clone());
        exp.insert(series3.to_string(), exp3.clone());
        exp.insert(series4.to_string(), exp4.clone());
        test_query("foo", "bar.*", exp);
    }
}

#[test]
fn test_histogram_append_and_query() {
    eprintln!("integer histograms");
    run_histogram_append_and_query(false);
    eprintln!("float histograms");
    run_histogram_append_and_query(true);
}

// ---------------------------------------------------------------------------------------------------------------
// TestQueryHistogramFromBlocksWithCompaction.

/// Go `testBlockQuerying` of `TestQueryHistogramFromBlocksWithCompaction`.
fn test_block_querying(block_series: &[Vec<SeriesArc>]) {
    let db = new_test_db(vec![with_opts(default_opts())]);

    let mut exp: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
    for series in block_series {
        create_block(db.dir(), series);

        for s in series {
            let lbls = s.labels().to_string();
            let slice = exp.entry(lbls).or_default();
            let mut it = s.iterator(None);
            let smpls = storage::expand_samples(&mut it).unwrap();
            slice.extend(smpls);
            sort_by_t(slice);
        }
    }

    assert!(db.blocks().is_empty());
    db.reload().unwrap();
    assert_eq!(block_series.len(), db.blocks().len());

    let res = query_db(&db, i64::MIN, i64::MAX, &[mre("__name__", ".*")]);
    compare_series(&exp, &res);

    // Compact all the blocks together and query again.
    let blocks = db.blocks();
    let block_dirs: Vec<PathBuf> = blocks.iter().map(|b| b.dir().to_path_buf()).collect();
    let compactor = db.compactor.read().unwrap().clone();
    let ids = compactor.compact(db.dir(), &block_dirs, &blocks).unwrap();
    assert_eq!(1, ids.len());
    drop(blocks);
    db.reload().unwrap();
    assert_eq!(1, db.blocks().len());

    let res = query_db(&db, i64::MIN, i64::MAX, &[mre("__name__", ".*")]);

    // After compaction, we do not require "unknown" counter resets
    // due to origin from different overlapping chunks anymore.
    for ss in exp.values_mut() {
        for i in 1..ss.len() {
            let prev = ss[i - 1].typ();
            let s = &mut ss[i];
            if s.typ() == ValueType::Histogram
                && prev == ValueType::Histogram
                && hmut(s).counter_reset_hint == CounterResetHint::UnknownCounterReset
            {
                hmut(s).counter_reset_hint = CounterResetHint::NotCounterReset;
            }
            if s.typ() == ValueType::FloatHistogram
                && prev == ValueType::FloatHistogram
                && fhmut(s).counter_reset_hint == CounterResetHint::UnknownCounterReset
            {
                fhmut(s).counter_reset_hint = CounterResetHint::NotCounterReset;
            }
        }
    }
    compare_series(&exp, &res);
}

#[test]
fn test_query_histogram_from_blocks_with_compaction() {
    let minute = |m: i64| m * MINUTE;
    let float_gen = |ts: i64| fs(ts, rand_f64());

    for float_histogram in [false, true] {
        eprintln!("floatHistogram={float_histogram}");
        // serial blocks with only histograms
        test_block_querying(&[
            gen_histogram_series(10, 5, minute(0), minute(119), minute(1), float_histogram),
            gen_histogram_series(10, 5, minute(120), minute(239), minute(1), float_histogram),
            gen_histogram_series(10, 5, minute(240), minute(359), minute(1), float_histogram),
        ]);

        // serial blocks with either histograms or floats in a block and not both
        test_block_querying(&[
            gen_histogram_series(10, 5, minute(0), minute(119), minute(1), float_histogram),
            gen_series_from_sample_generator(10, 5, minute(120), minute(239), minute(1), float_gen),
            gen_histogram_series(10, 5, minute(240), minute(359), minute(1), float_histogram),
        ]);

        // serial blocks with mix of histograms and float64
        test_block_querying(&[
            gen_histogram_and_float_series(10, 5, minute(0), minute(60), minute(1), float_histogram),
            gen_histogram_series(10, 5, minute(61), minute(120), minute(1), float_histogram),
            gen_histogram_and_float_series(10, 5, minute(121), minute(180), minute(1), float_histogram),
            gen_series_from_sample_generator(10, 5, minute(181), minute(240), minute(1), float_gen),
        ]);

        // overlapping blocks with only histograms
        test_block_querying(&[
            gen_histogram_series(10, 5, minute(0), minute(120), minute(3), float_histogram),
            gen_histogram_series(10, 5, minute(1), minute(120), minute(3), float_histogram),
            gen_histogram_series(10, 5, minute(2), minute(120), minute(3), float_histogram),
        ]);

        // overlapping blocks with only histograms and only float in a series
        test_block_querying(&[
            gen_histogram_series(10, 5, minute(0), minute(120), minute(3), float_histogram),
            gen_series_from_sample_generator(10, 5, minute(1), minute(120), minute(3), float_gen),
            gen_histogram_series(10, 5, minute(2), minute(120), minute(3), float_histogram),
        ]);

        // overlapping blocks with mix of histograms and float64
        test_block_querying(&[
            gen_histogram_and_float_series(10, 5, minute(0), minute(60), minute(3), float_histogram),
            gen_histogram_series(10, 5, minute(46), minute(100), minute(3), float_histogram),
            gen_histogram_and_float_series(10, 5, minute(89), minute(140), minute(3), float_histogram),
            gen_series_from_sample_generator(10, 5, minute(126), minute(200), minute(3), float_gen),
        ]);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// TestOOONativeHistogramsSettings.

#[test]
fn test_ooo_native_histograms_settings() {
    let h = Histogram {
        count: 9,
        zero_count: 4,
        zero_threshold: 0.001,
        sum: 35.5,
        schema: 1,
        positive_spans: vec![Span { offset: 0, length: 2 }, Span { offset: 2, length: 2 }],
        positive_buckets: vec![1, 1, -1, 0],
        ..Histogram::default()
    };

    let l = lbl(&["foo", "bar"]);

    // Test OOO native histograms if OOO is disabled and Native Histograms is enabled
    {
        let mut opts = default_opts();
        opts.out_of_order_time_window = 0;
        let db = new_test_db(vec![with_opts(opts), with_rngs(&[100])]);

        let mut app = db.appender(&ctx());
        app.append_histogram(0, &l, 100, Some(&h), None).unwrap();

        // The OOO sample is not detected until it is committed, so no error is returned
        app.append_histogram(0, &l, 50, Some(&h), None).unwrap();

        commit(app.as_mut());

        let act = query_db(&db, i64::MIN, i64::MAX, &[meq("foo", "bar")]);
        let mut exp = BTreeMap::new();
        exp.insert(l.to_string(), vec![Sample::histogram(0, 100, h.clone())]);
        assert_eq!(exp, act);
    }
    // Test OOO native histograms when both OOO and Native Histograms are enabled
    {
        let mut opts = default_opts();
        opts.out_of_order_time_window = 100;
        let db = new_test_db(vec![with_opts(opts), with_rngs(&[100])]);

        // Add in-order samples
        let mut app = db.appender(&ctx());
        app.append_histogram(0, &l, 200, Some(&h), None).unwrap();

        // Add OOO samples
        app.append_histogram(0, &l, 100, Some(&h), None).unwrap();
        app.append_histogram(0, &l, 150, Some(&h), None).unwrap();

        commit(app.as_mut());

        let act = query_db(&db, i64::MIN, i64::MAX, &[meq("foo", "bar")]);
        let mut exp = BTreeMap::new();
        exp.insert(
            l.to_string(),
            vec![
                Sample::histogram(0, 100, h.clone()),
                Sample::histogram(0, 150, h.clone()),
                Sample::histogram(0, 200, h.clone()),
            ],
        );
        require_equal_series(&exp, &act, true);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// TestChunkQuerierReadWriteRace.

// TestChunkQuerierReadWriteRace looks for any possible race between appending
// samples and reading chunks because the head chunk that is being appended to
// can be read in parallel and we should be able to make a copy of the chunk without
// worrying about the parallel write.
#[test]
fn test_chunk_querier_read_write_race() {
    let db = new_test_db(vec![]);

    let lbls = lbl(&["foo", "bar"]);

    let writer = {
        let db = db.db.clone();
        let lbls = lbls.clone();
        move || -> Result<(), Error> {
            std::thread::sleep(std::time::Duration::from_millis(5)); // Initial pause while readers start.
            let mut ts = 0i64;
            for _ in 0..500 {
                let mut app = db.appender(&ctx());
                for _ in 0..10 {
                    ts += 1;
                    app.append(0, &lbls, ts, (ts * 100) as f64)?;
                }
                if let Some(e) = app.commit() {
                    return Err(e);
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Ok(())
        }
    };

    let reader = || {
        let mut querier = db.chunk_querier(i64::MIN, i64::MAX).unwrap();
        {
            let mut ss = querier.select(&ctx(), false, None, &[meq("foo", "bar")]);
            while ss.next() {
                let cs = ss.at().clone();
                let mut it = cs.iterator(None);
                while it.next() {
                    let m = it.at();
                    // This copying of chunk bytes detects any race.
                    let _ = m.chunk.as_ref().expect("chunk").bytes().to_vec();
                }
            }
            assert!(ss.err().is_none(), "{:?}", ss.err());
        }
        assert!(querier.close().is_none());
    };

    let done = Arc::new(AtomicBool::new(false));
    let handle = {
        let done = done.clone();
        std::thread::spawn(move || {
            let r = writer();
            done.store(true, Ordering::SeqCst);
            r
        })
    };

    loop {
        reader();
        if done.load(Ordering::SeqCst) {
            break;
        }
    }

    handle.join().unwrap().unwrap();
}
