//! Port of `tsdb/db_test.go`, lines 9967..end (part 4).
//!
//! Rust notes:
//! - Go's package-level `compactHeadViewBeforeEvictTestingCallback` is a field of the DB here
//!   (`db.compact_head_view_before_evict_testing_callback`), called once and cleared.
//! - Go `newTestDB` + reopen with `Open`: the reopened DB replaces `TestDb::db`, so the drop closes it.
//! - Go holds a series mutex in the test goroutine and unlocks it from the compaction hook. A Rust mutex guard
//!   cannot move into the hook, so a helper thread holds the lock until the hook tells it to release it.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use model::histogram::{CounterResetHint, FloatHistogram, Histogram};
use model::labels::{self, Labels, METRIC_NAME};
use model::matcher::{MatchType, Matcher};
use model::value::{is_stale_nan, stale_nan};
use storage::tsdbutil as tu;
use storage::{Appender, AppenderV2, AppendV2Options, Context, Error, Sample, SeriesArc, SeriesRef};
use tsdb::block::{open_block, Block};
use tsdb::compact::Compactor;
use tsdb::db::{self, Options, DB};
use tsdb::head::{Head, HeadSeriesRef, MemSeries, SeriesLifecycleCallback, OOO_CHUNK_ID_MASK};
use tsdb::head::RangeHead;
use tsdb::querier::{new_block_chunk_querier, new_block_querier};
use tsdb::{read_meta_file, write_meta_file, BlockMeta, BlockMetaCompaction, BlockReader, Logger, Ulid};
use tsdb_format::record::{self, Decoder, RefSeries};
use tsdb_format::wlog::{self, compression};

use crate::common::{
    create_block, gen_series, new_test_db, query, query_without_replacing_nans, sample_type_scenarios, with_opts,
    with_rngs, SampleTypeScenario, TempDir, TestDb,
};

// ---------------------------------------------------------------------------------------------------------------
// Helpers.

fn bg() -> Context {
    Context::background()
}

fn ls(kv: &[&str]) -> Labels {
    labels::from_strings(kv)
}

fn eq(name: &str, value: &str) -> Matcher {
    Matcher::must_new(MatchType::Equal, name, value)
}

fn re(name: &str, value: &str) -> Matcher {
    Matcher::must_new(MatchType::Regexp, name, value)
}

fn fsample(t: i64, f: f64) -> Sample {
    Sample::float(0, t, f)
}

fn ok(e: Option<Error>) {
    if let Some(e) = e {
        panic!("unexpected error: {e}");
    }
}

/// Go `DefaultOptions()` with `MinBlockDuration` and `MaxBlockDuration` set.
fn opts_block(min: i64, max: i64) -> Options {
    Options { min_block_duration: min, max_block_duration: max, ..Options::default() }
}

/// Go `Open(dir, nil, nil, opts, nil)`.
fn reopen(dir: &Path, opts: &Options) -> Arc<DB> {
    db::open(dir, None, Some(opts.clone()), None).unwrap()
}

/// Closes the DB of `t` and replaces it with a DB reopened in the same directory (Go `db.Close()` + `Open`).
fn restart(t: &mut TestDb, opts: &Options) {
    t.db.close().unwrap();
    let dir = t.db.dir().to_path_buf();
    t.db = reopen(&dir, opts);
}

/// Sets the testing hook that runs after the block write and before eviction.
fn set_before_evict_hook(db: &DB, f: impl FnOnce() + Send + 'static) {
    *db.compact_head_view_before_evict_testing_callback.lock().unwrap() = Some(Box::new(f));
}

fn clear_before_evict_hook(db: &DB) {
    *db.compact_head_view_before_evict_testing_callback.lock().unwrap() = None;
}

fn series_by_id(h: &Head, r: SeriesRef) -> Arc<MemSeries> {
    h.series.get_by_id(r as HeadSeriesRef).expect("series")
}

/// Go `mockCompactorFn`.
struct MockCompactorFn {
    plan_fn: Mutex<Box<dyn FnMut() -> Result<Vec<PathBuf>, Error> + Send>>,
    compact_fn: Mutex<Box<dyn FnMut() -> Result<Vec<Ulid>, Error> + Send>>,
    write_fn: Mutex<Box<dyn FnMut() -> Result<Vec<Ulid>, Error> + Send>>,
}

impl Compactor for MockCompactorFn {
    fn plan(&self, _: &Path) -> Result<Vec<PathBuf>, Error> {
        (self.plan_fn.lock().unwrap())()
    }
    fn write(&self, _: &Path, _: &dyn BlockReader, _: i64, _: i64, _: Option<&BlockMeta>) -> Result<Vec<Ulid>, Error> {
        (self.write_fn.lock().unwrap())()
    }
    fn compact(&self, _: &Path, _: &[PathBuf], _: &[Block]) -> Result<Vec<Ulid>, Error> {
        (self.compact_fn.lock().unwrap())()
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

/// The series records of Go `readTestWAL` (the other record types are decoded and dropped).
fn read_test_wal_series(dir: &Path) -> Vec<Vec<RefSeries>> {
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
                recs.push(v);
            }
            record::Type::Samples | record::Type::SamplesV2 => {
                let mut v = Vec::new();
                dec.samples(&rec, &mut v).expect("samples");
            }
            record::Type::HistogramSamples
            | record::Type::CustomBucketsHistogramSamples
            | record::Type::HistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.histogram_samples(&rec, &mut v).expect("histograms");
            }
            record::Type::FloatHistogramSamples
            | record::Type::CustomBucketsFloatHistogramSamples
            | record::Type::FloatHistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.float_histogram_samples(&rec, &mut v).expect("float histograms");
            }
            record::Type::Tombstones => {
                let mut v = Vec::new();
                dec.tombstones(&rec, &mut v).expect("tombstones");
            }
            record::Type::MinValidTime => {
                dec.min_valid_time(&rec).expect("min valid time");
            }
            t => panic!("unknown record type {:?}", t),
        }
    }
    assert!(r.err().is_none(), "{:?}", r.err());
    r.into_inner().close().expect("close segments reader");
    recs
}

fn checkpoint_has_series(wal_dir: &Path, r: SeriesRef) -> bool {
    let (cp, _) = wlog::last_checkpoint(wal_dir).expect("a checkpoint must have been produced");
    read_test_wal_series(&cp).iter().flatten().any(|s| s.r#ref as SeriesRef == r)
}

fn wal_dir(db: &DB) -> PathBuf {
    db.head().wal().expect("wal").dir().to_path_buf()
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

fn one(name: &str, samples: Vec<Sample>) -> BTreeMap<String, Vec<Sample>> {
    BTreeMap::from([(name.to_string(), samples)])
}

/// Holds the lock of a series in a helper thread until `release` is called (Go `series.Lock()` in the test
/// goroutine and `Unlock()` in the compaction hook).
struct HeldLock {
    release: Mutex<Option<mpsc::Sender<()>>>,
    handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl HeldLock {
    fn hold(s: Arc<MemSeries>) -> Arc<HeldLock> {
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let handle = std::thread::spawn(move || {
            let g = s.lock();
            locked_tx.send(()).unwrap();
            let _ = release_rx.recv();
            drop(g);
        });
        locked_rx.recv().unwrap();
        Arc::new(HeldLock { release: Mutex::new(Some(release_tx)), handle: Mutex::new(Some(handle)) })
    }

    /// Go `unlockOnce.Do(blocker.Unlock)`.
    fn unlock(&self) {
        if let Some(tx) = self.release.lock().unwrap().take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.handle.lock().unwrap().take() {
            h.join().unwrap();
        }
    }
}

/// Go `require.Eventually`.
fn eventually(timeout: Duration, tick: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    loop {
        if f() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("condition never satisfied");
        }
        std::thread::sleep(tick);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Tests.

// Regression test for https://github.com/prometheus/prometheus/pull/13754
#[test]
fn test_abort_block_compactions() {
    // Create a test DB
    let db = new_test_db(vec![]);
    // It should NOT be compactable at the beginning of the test
    assert!(!db.head().compactable(), "head should NOT be compactable");

    // Track the number of compactions run inside db.compactBlocks()
    let compactions = Arc::new(AtomicUsize::new(0));

    // Use a mock compactor with custom Plan() implementation
    let head = db.head().clone();
    let c = compactions.clone();
    *db.compactor.write().unwrap() = Arc::new(MockCompactorFn {
        plan_fn: Mutex::new(Box::new(move || {
            // On every Plan() run increment compactions. After 4 compactions update HEAD to make it compactable
            // to force an exit from db.compactBlocks() loop.
            let n = c.fetch_add(1, Ordering::SeqCst) + 1;
            if n > 3 {
                let chunk_range = head.chunk_range.load(Ordering::SeqCst);
                head.min_time.store(0, Ordering::SeqCst);
                head.max_time.store(chunk_range * 2, Ordering::SeqCst);
                assert!(head.compactable(), "head should be compactable");
            }
            // Our custom Plan() will always return something to compact.
            Ok(vec![PathBuf::from("1"), PathBuf::from("2"), PathBuf::from("3")])
        })),
        compact_fn: Mutex::new(Box::new(|| Ok(Vec::new()))),
        write_fn: Mutex::new(Box::new(|| Ok(Vec::new()))),
    });

    db.compact(&bg()).unwrap();
    assert!(db.head().compactable(), "head should be compactable");
    assert_eq!(4, compactions.load(Ordering::SeqCst), "expected 4 compactions to be completed");
}

#[test]
fn test_new_compactor_func() {
    let mut opts = Options::default();
    let block1 = Ulid::from_parts(1, [0; 10]);
    let block2 = Ulid::from_parts(2, [0; 10]);
    opts.new_compactor_func = Some(Arc::new(move |_, _, _, _, _| {
        Ok(Arc::new(MockCompactorFn {
            plan_fn: Mutex::new(Box::new(move || {
                Ok(vec![PathBuf::from(block1.to_string()), PathBuf::from(block2.to_string())])
            })),
            compact_fn: Mutex::new(Box::new(move || Ok(vec![block1]))),
            write_fn: Mutex::new(Box::new(move || Ok(vec![block2]))),
        }) as Arc<dyn Compactor>)
    }));
    let db = new_test_db(vec![with_opts(opts)]);

    let compactor = db.compactor.read().unwrap().clone();
    let plans = compactor.plan(Path::new("")).unwrap();
    assert_eq!(vec![PathBuf::from(block1.to_string()), PathBuf::from(block2.to_string())], plans);
    let ulids = compactor.compact(Path::new(""), &[], &[]).unwrap();
    assert_eq!(1, ulids.len());
    assert_eq!(block1, ulids[0]);
    let empty = common_empty_block();
    let ulids = compactor.write(Path::new(""), &empty, 0, 1, None).unwrap();
    assert_eq!(1, ulids.len());
    assert_eq!(block2, ulids[0]);
}

/// Go passes a nil `BlockReader`; the mock does not read it.
fn common_empty_block() -> crate::common::MemBlock {
    crate::common::mem_block_from_series(&[])
}

#[test]
fn test_block_querier_and_block_chunk_querier() {
    let mut opts = Options::default();
    opts.block_querier_func = Some(Arc::new(|b: &dyn BlockReader, mint, maxt| {
        // Only block with hints can be queried.
        if !b.meta().compaction.hints.is_empty() {
            return new_block_querier(b, mint, maxt);
        }
        Ok(storage::noop_querier())
    }));
    opts.block_chunk_querier_func = Some(Arc::new(|b: &dyn BlockReader, mint, maxt| {
        // Only level 4 compaction block can be queried.
        if b.meta().compaction.level == 4 {
            return new_block_chunk_querier(b, mint, maxt);
        }
        Ok(storage::noop_chunked_querier())
    }));

    let db = new_test_db(vec![with_opts(opts)]);

    let metas = [
        BlockMetaCompaction { hints: vec!["test-hint".to_string()], ..Default::default() },
        BlockMetaCompaction { level: 4, ..Default::default() },
    ];
    for (i, m) in metas.iter().enumerate() {
        // Include blockID into series to identify which block got touched.
        let serieses = vec![storage::new_list_series(
            labels::from_map(BTreeMap::from([
                ("block".to_string(), format!("block-{i}")),
                (METRIC_NAME.to_string(), "test_metric".to_string()),
            ])),
            vec![fsample(0, 1.0)],
        )];
        let block_dir = create_block(db.dir(), &serieses);
        let b = open_block(Some(db.logger.clone()), &block_dir, Some(db.chunk_pool.clone()), None).unwrap();

        // Overwrite meta.json with compaction section for testing purpose.
        let mut meta = b.meta();
        meta.compaction = m.clone();
        write_meta_file(&block_dir, &mut meta).unwrap();
        b.close().unwrap();
    }
    db.reload_blocks().unwrap();
    assert_eq!(2, db.blocks().len());

    let matcher = eq(METRIC_NAME, "test_metric");
    let mut querier = db.querier(0, 500).unwrap();
    {
        let mut series_set = querier.select(&bg(), false, None, std::slice::from_ref(&matcher));
        let mut count = 0;
        let mut lbls = Labels::empty();
        while series_set.next() {
            count += 1;
            lbls = series_set.at().labels().clone();
        }
        assert!(series_set.err().is_none());
        assert_eq!(1, count);
        // Make sure only block-0 is queried.
        assert_eq!("block-0", lbls.get("block"));
    }
    let _ = querier.close();

    let mut chunk_querier = db.chunk_querier(0, 500).unwrap();
    {
        let mut css = chunk_querier.select(&bg(), false, None, std::slice::from_ref(&matcher));
        let mut count = 0;
        // Reset lbls variable.
        let mut lbls = Labels::empty();
        while css.next() {
            count += 1;
            lbls = css.at().labels().clone();
        }
        assert!(css.err().is_none());
        assert_eq!(1, count);
        // Make sure only block-1 is queried.
        assert_eq!("block-1", lbls.get("block"));
    }
    let _ = chunk_querier.close();
}

#[test]
fn test_generate_compaction_delay() {
    let assert_delay = |delay: Duration, expected_max_percent_delay: i64| {
        // Expect to generate a delay up to MaxPercentDelay of the head chunk range
        assert!(delay <= Duration::from_millis((60000 * expected_max_percent_delay / 100) as u64));
    };

    let mut opts = Options::default();
    let cases = [1, 10, 60, 100];

    opts.enable_delayed_compaction = true;

    for c in cases {
        opts.compaction_delay_max_percent = c;
        let db = new_test_db(vec![with_opts(opts.clone()), with_rngs(&[60000])]);

        // The offset is generated and changed while opening.
        assert_delay(db.opts.read().unwrap().compaction_delay, c as i64);

        for _ in 0..1000 {
            assert_delay(db.generate_compaction_delay(), c as i64);
        }
    }
}

// TestBlockClosingBlockedDuringRemoteRead ensures that a TSDB Block is not closed while it is being queried
// through remote read. This is a regression test for https://github.com/prometheus/prometheus/issues/14422.
//
// adapted: the remote read handler (storage/remote) is not part of the tsdb crate. The Go handler opens a
// querier on the DB, selects all series and streams them, closing the querier when the stream ends; the test
// pauses the stream at its first write. Here the test does the same steps directly: it opens a querier, selects
// `{__name__=~".*"}`, reads the first series (the stream started) and pauses, then checks that closing the
// blocks blocks until the "stream" is finished and the querier is closed.
#[test]
fn test_block_closing_blocked_during_remote_read() {
    let dir = TempDir::new();

    create_block(dir.path(), &gen_series(2, 1, 0, 10));

    // Not using newTestDB as db.Close is expected to return error.
    let db = db::open(dir.path(), None, None, None).unwrap();

    let matcher = Matcher::new(MatchType::Regexp, "__name__", ".*").unwrap();

    let (read_done_tx, read_done_rx) = mpsc::channel::<()>();
    let (write_started_tx, write_started_rx) = mpsc::channel::<()>();
    let (write_blocked_tx, write_blocked_rx) = mpsc::channel::<()>();
    let qdb = db.clone();
    let reader = std::thread::spawn(move || {
        let mut q = qdb.querier(0, 10).unwrap();
        {
            let mut ss = q.select(&bg(), false, None, &[matcher]);
            let mut first = true;
            while ss.next() {
                let s = ss.at().clone();
                let mut it = s.iterator(None);
                let _ = storage::expand_samples(&mut it).unwrap();
                if first {
                    // Signal that writing has started, then block until the test resumes it.
                    first = false;
                    write_started_tx.send(()).unwrap();
                    let _ = write_blocked_rx.recv();
                }
            }
            assert!(ss.err().is_none());
        }
        assert!(q.close().is_none());
        read_done_tx.send(()).unwrap();
    });

    // Wait for the read API to start streaming data.
    write_started_rx.recv().unwrap();

    // Try to close the queried block.
    let (block_closed_tx, block_closed_rx) = mpsc::channel::<()>();
    let blocks = db.blocks();
    std::thread::spawn(move || {
        for block in blocks {
            let _ = block.close();
        }
        let _ = block_closed_tx.send(());
    });

    // Closing the queried block should block. Wait a little bit to make sure of that.
    std::thread::sleep(Duration::from_millis(100));
    assert!(read_done_rx.try_recv().is_err(), "read API should still be streaming data.");
    assert!(block_closed_rx.try_recv().is_err(), "Block shouldn't get closed while being queried.");

    // Resume the read API data streaming.
    drop(write_blocked_tx);
    read_done_rx.recv().unwrap();
    reader.join().unwrap();

    // The block should be no longer needed and closing it should end.
    // adapted: 10ms in Go; a thread wake-up can take longer on a loaded machine.
    assert!(block_closed_rx.recv_timeout(Duration::from_millis(100)).is_ok(), "Closing the block timed out.");
    let _ = db.close();
}

#[test]
fn test_block_reload_interval() {
    struct Case {
        name: &'static str,
        opts: Options,
        expected_interval: Duration,
        verify_reloads: bool,
        expected_reloads: f64,
    }
    let cases = vec![
        Case {
            name: "zero interval with custom options",
            opts: Options { retention_duration: 60 * 60 * 1000, ..db::zero_options() },
            expected_interval: Duration::from_secs(60),
            verify_reloads: false,
            expected_reloads: 0.0,
        },
        Case {
            name: "extremely small interval",
            opts: Options { block_reload_interval: Duration::from_millis(1), ..db::zero_options() },
            expected_interval: Duration::from_secs(1),
            verify_reloads: true,
            expected_reloads: 5.0,
        },
        Case {
            name: "one second interval",
            opts: Options { block_reload_interval: Duration::from_secs(1), ..db::zero_options() },
            expected_interval: Duration::from_secs(1),
            verify_reloads: true,
            expected_reloads: 5.0,
        },
    ];
    // Go runs the cases in parallel.
    std::thread::scope(|s| {
        for c in cases {
            s.spawn(move || {
                let db = new_test_db(vec![with_opts(c.opts)]);
                assert_eq!(c.expected_interval, db.opts.read().unwrap().block_reload_interval, "{}", c.name);
                if !c.verify_reloads {
                    return;
                }
                assert_eq!(1.0, db.metrics.reloads.get(), "there should be one initial reload");
                eventually(Duration::from_secs(5), Duration::from_millis(100), || {
                    db.metrics.reloads.get() == c.expected_reloads
                });
            });
        }
    });
}

#[test]
fn test_stale_series_compaction() {
    let opts = opts_block(1000, 1000);
    let mut db = new_test_db(vec![with_opts(opts)]);
    db.disable_compactions();

    let num_series_per_category = 1;
    let (mut non_stale_series, mut stale_series) = (Vec::new(), Vec::new());
    let (mut non_stale_hist, mut stale_hist) = (Vec::new(), Vec::new());
    let (mut non_stale_fhist, mut stale_fhist) = (Vec::new(), Vec::new());
    let (mut stale_series_crossing_boundary, mut stale_hist_crossing_boundary, mut stale_fhist_crossing_boundary) =
        (Vec::new(), Vec::new(), Vec::new());
    for i in 0..num_series_per_category {
        let n = |base: usize| ls(&["name", &format!("series{}", base + i)]);
        non_stale_series.push(n(1000));
        non_stale_hist.push(n(2000));
        non_stale_fhist.push(n(3000));

        stale_series.push(n(4000));
        stale_hist.push(n(5000));
        stale_fhist.push(n(6000));

        stale_series_crossing_boundary.push(n(7000));
        stale_hist_crossing_boundary.push(n(8000));
        stale_fhist_crossing_boundary.push(n(9000));
    }

    let v = 10.0;
    let stale_v = stale_nan();
    let h = tu::generate_test_histograms(1).remove(0);
    let fh = tu::generate_test_float_histograms(1).remove(0);
    let stale_h = Histogram { sum: stale_v, ..Histogram::default() };
    let stale_fh = FloatHistogram { sum: stale_v, ..FloatHistogram::default() };

    let add_normal_samples = |db: &DB, ts: i64, float_series: &[Labels], hist_series: &[Labels], fh_series: &[Labels]| {
        let mut app = db.appender(&bg());
        for i in 0..float_series.len() {
            app.append(0, &float_series[i], ts, v).unwrap();
            app.append_histogram(0, &hist_series[i], ts, Some(&h), None).unwrap();
            app.append_histogram(0, &fh_series[i], ts, None, Some(&fh)).unwrap();
        }
        ok(app.commit());
    };
    let add_stale_samples = |db: &DB, ts: i64, float_series: &[Labels], hist_series: &[Labels], fh_series: &[Labels]| {
        let mut app = db.appender(&bg());
        for i in 0..float_series.len() {
            app.append(0, &float_series[i], ts, stale_v).unwrap();
            app.append_histogram(0, &hist_series[i], ts, Some(&stale_h), None).unwrap();
            app.append_histogram(0, &fh_series[i], ts, None, Some(&stale_fh)).unwrap();
        }
        ok(app.commit());
    };

    // Normal sample for all.
    add_normal_samples(&db, 100, &non_stale_series, &non_stale_hist, &non_stale_fhist);
    add_normal_samples(&db, 100, &stale_series, &stale_hist, &stale_fhist);

    // Stale sample for the stale series. Normal sample for the non-stale series.
    add_normal_samples(&db, 200, &non_stale_series, &non_stale_hist, &non_stale_fhist);
    add_stale_samples(&db, 200, &stale_series, &stale_hist, &stale_fhist);

    // Normal samples for the non-stale series later
    add_normal_samples(&db, 300, &non_stale_series, &non_stale_hist, &non_stale_fhist);

    assert_eq!(6 * num_series_per_category as u64, db.head().num_series());
    assert_eq!(3 * num_series_per_category as u64, db.head().num_stale_series());

    // Series crossing block boundary and gets stale.
    let (a, b, c) = (&stale_series_crossing_boundary, &stale_hist_crossing_boundary, &stale_fhist_crossing_boundary);
    add_normal_samples(&db, 300, a, b, c);
    add_normal_samples(&db, 700, a, b, c);
    add_normal_samples(&db, 1100, a, b, c);
    add_stale_samples(&db, 1200, a, b, c);

    db.compact_stale_head().unwrap();

    assert_eq!(3 * num_series_per_category as u64, db.head().num_series());
    assert_eq!(0, db.head().num_stale_series());

    assert_eq!(2, db.blocks().len());
    let m = db.blocks()[0].meta();
    assert_eq!(0, m.min_time);
    assert_eq!(1000, m.max_time);
    assert!(m.compaction.from_stale_series(), "stale series info not found in block meta");
    let m = db.blocks()[1].meta();
    assert_eq!(1000, m.min_time);
    assert_eq!(2000, m.max_time);
    assert!(m.compaction.from_stale_series(), "stale series info not found in block meta");

    // To make sure that Head is not truncated based on stale series block.
    db.reload().unwrap();

    let mut non_first_h = h.clone();
    non_first_h.counter_reset_hint = CounterResetHint::NotCounterReset;
    let mut non_first_fh = fh.clone();
    non_first_fh.counter_reset_hint = CounterResetHint::NotCounterReset;

    let hs = |t: i64, h: &Histogram| Sample::histogram(0, t, h.clone());
    let fhs = |t: i64, fh: &FloatHistogram| Sample::float_histogram(0, t, fh.clone());
    let key = |l: &Labels| format!(r#"{{name="{}"}}"#, l.get("name"));

    // Verify head block.
    let verify_head_block = |db: &DB| {
        assert_eq!(3, db.head().num_series());
        assert_eq!(0, db.head().num_stale_series());

        let mut exp_head_query = BTreeMap::new();
        for i in 0..num_series_per_category {
            exp_head_query
                .insert(key(&non_stale_series[i]), vec![fsample(100, v), fsample(200, v), fsample(300, v)]);
            exp_head_query
                .insert(key(&non_stale_hist[i]), vec![hs(100, &h), hs(200, &non_first_h), hs(300, &non_first_h)]);
            exp_head_query.insert(
                key(&non_stale_fhist[i]),
                vec![fhs(100, &fh), fhs(200, &non_first_fh), fhs(300, &non_first_fh)],
            );
        }

        let rh = RangeHead::new(db.head().clone(), 0, 300);
        let querier = new_block_querier(&rh, 0, 300).unwrap();
        let series_set = query(querier, &[re("name", "series.*")]);
        assert_eq!(exp_head_query, series_set);
    };

    verify_head_block(&db);

    // Verify blocks from stale series.
    {
        let mut exp_block_query = BTreeMap::new();
        for i in 0..num_series_per_category {
            exp_block_query.insert(key(&stale_series[i]), vec![fsample(100, v), fsample(200, stale_v)]);
            exp_block_query.insert(key(&stale_hist[i]), vec![hs(100, &h), hs(200, &stale_h)]);
            exp_block_query.insert(key(&stale_fhist[i]), vec![fhs(100, &fh), fhs(200, &stale_fh)]);
            exp_block_query.insert(
                key(&stale_series_crossing_boundary[i]),
                vec![fsample(300, v), fsample(700, v), fsample(1100, v), fsample(1200, stale_v)],
            );
            exp_block_query.insert(
                key(&stale_hist_crossing_boundary[i]),
                vec![hs(300, &h), hs(700, &non_first_h), hs(1100, &h), hs(1200, &stale_h)],
            );
            exp_block_query.insert(
                key(&stale_fhist_crossing_boundary[i]),
                vec![fhs(300, &fh), fhs(700, &non_first_fh), fhs(1100, &fh), fhs(1200, &stale_fh)],
            );
        }

        let blocks = db.blocks();
        let querier = new_block_querier(&blocks[0], 0, 1000).unwrap();
        let mut series_set = query_without_replacing_nans(querier, &[re("name", "series.*")]);

        let querier = new_block_querier(&blocks[1], 1000, 2000).unwrap();
        let series_set2 = query_without_replacing_nans(querier, &[re("name", "series.*")]);
        for (k, v) in series_set2 {
            series_set.entry(k).or_default().extend(v);
        }

        assert_eq!(exp_block_query.len(), series_set.len());

        // Compare all the samples except the stale value that needs special handling.
        for category in [
            &stale_series,
            &stale_hist,
            &stale_fhist,
            &stale_series_crossing_boundary,
            &stale_hist_crossing_boundary,
            &stale_fhist_crossing_boundary,
        ] {
            for i in 0..num_series_per_category {
                let series_key = key(&category[i]);
                let samples = &exp_block_query[&series_key];
                let act_samples =
                    series_set.get(&series_key).unwrap_or_else(|| panic!("series not found in result {series_key}"));
                assert_eq!(samples.len(), act_samples.len());

                for j in 0..samples.len() - 1 {
                    assert_eq!(samples[j], act_samples[j]);
                }

                let l = samples.len() - 1;
                assert_eq!(samples[l].t, act_samples[l].t);
                if samples[l].h.is_none() && samples[l].fh.is_none() && is_stale_nan(samples[l].f) {
                    assert!(is_stale_nan(act_samples[l].f));
                } else if samples[l].h.is_some() {
                    assert!(is_stale_nan(act_samples[l].h.as_ref().unwrap().sum));
                } else {
                    assert!(is_stale_nan(act_samples[l].fh.as_ref().unwrap().sum));
                }
            }
        }
    }

    {
        // Restart DB and verify that stale series were discarded from WAL replay.
        let opts = db.opts.read().unwrap().clone();
        restart(&mut db, &opts);

        verify_head_block(&db);
    }
}

#[test]
fn test_lifecycle_callback_invariant_after_wal_replay_with_full_tombstones() {
    const NUM_SERIES: usize = 6;
    const DELETED: usize = NUM_SERIES / 2;

    let mut opts = opts_block(1000, 1000);
    opts.series_lifecycle_callback = Some(Arc::new(CountSeriesLifecycleCallback::default()));

    let tmp = TempDir::new();
    let db = db::open(tmp.path(), None, Some(opts.clone()), None).unwrap();
    db.disable_compactions();

    let all_series: Vec<Labels> = (0..NUM_SERIES).map(|i| ls(&["name", &format!("series{i}")])).collect();

    let mut app = db.appender(&bg());
    for lset in &all_series {
        app.append(0, lset, 100, 1.0).unwrap();
    }
    ok(app.commit());

    let stale_v = stale_nan();
    let mut app = db.appender(&bg());
    for (i, lset) in all_series.iter().enumerate() {
        let v = if i < DELETED { stale_v } else { i as f64 };
        app.append(0, lset, 200, v).unwrap();
    }
    ok(app.commit());

    assert_eq!(NUM_SERIES as u64, db.head().num_series());
    assert_eq!(DELETED as u64, db.head().num_stale_series());

    db.compact_stale_head().unwrap();
    assert_eq!((NUM_SERIES - DELETED) as u64, db.head().num_series());
    assert_eq!(0, db.head().num_stale_series());

    let dir = db.dir().to_path_buf();
    db.close().unwrap();

    let replay_callback = Arc::new(CountSeriesLifecycleCallback::default());
    opts.series_lifecycle_callback = Some(replay_callback.clone());
    let db = db::open(&dir, None, Some(opts), None).unwrap();

    assert_eq!(
        db.head().num_series() as i64,
        replay_callback.created.load(Ordering::SeqCst) - replay_callback.deleted.load(Ordering::SeqCst)
    );
    db.close().unwrap();
}

// TestStaleSeriesResurrectionOverlapNotMerged drives the #18379 data-loss path end to end using only DB-level
// operations: a series goes stale and gets a real from-stale block via CompactStaleHead, is evicted, is
// resurrected by a backfilled sample, then head compaction produces a normal block overlapping the stale block.
// Segregation keeps the two overlapping blocks unmerged, so the stale block does not advance
// inOrderBlocksMaxTime and no in-order data is dropped across the reload.
#[test]
fn test_stale_series_resurrection_overlap_not_merged() {
    const CHUNK_RANGE: i64 = 1000;
    let db = new_test_db(vec![with_opts(opts_block(CHUNK_RANGE, CHUNK_RANGE))]);
    db.disable_compactions();

    let stale_v = stale_nan();
    let resur = ls(&["name", "resurrected"]);
    let keep = ls(&["name", "in_order_kept"]);

    // resur receives a normal sample then a stale-NaN marker, so it becomes stale. keep is an ordinary in-order
    // series; its sample at t=700 lives only in the head/WAL and is never covered by an in-order block, so if
    // the WAL-replay cutoff (inOrderBlocksMaxTime) advances past 700 the reload drops it.
    let mut app = db.appender(&bg());
    app.append(0, &resur, 100, 1.0).unwrap();
    app.append(0, &keep, 100, 7.0).unwrap();
    ok(app.commit());

    let mut app = db.appender(&bg());
    app.append(0, &resur, 200, stale_v).unwrap(); // marks resur as stale.
    app.append(0, &keep, 700, 42.0).unwrap(); // WAL-only in-order sample, no block covers it.
    ok(app.commit());

    assert_eq!(2, db.head().num_series());
    assert_eq!(1, db.head().num_stale_series());

    // CompactStaleHead writes a real from-stale-series block covering resur over [0,1000) and evicts resur from
    // the head. keep stays in the head.
    db.compact_stale_head().unwrap();
    assert_eq!(1, db.head().num_series(), "only keep must remain in the head");
    assert_eq!(0, db.head().num_stale_series());

    assert_eq!(1, db.blocks().len());
    let stale_meta = db.blocks()[0].meta();
    assert!(
        stale_meta.compaction.from_stale_series(),
        "block from CompactStaleHead must carry the from-stale-series hint"
    );
    assert_eq!(0, stale_meta.min_time);
    assert_eq!(1000, stale_meta.max_time);
    let stale_ulid = stale_meta.ulid;

    // Resurrect resur in the head with a backfilled sample at t=300, which lands inside the [0,1000) range
    // already owned by the stale block. t=300 is within the head's appendable window (head MaxTime is 700, so
    // the cutoff is 200).
    let mut app = db.appender(&bg());
    app.append(0, &resur, 300, 9.0).unwrap();
    ok(app.commit());
    assert_eq!(2, db.head().num_series(), "resur must be resurrected in the head");
    assert_eq!(0, db.head().num_stale_series());

    // Head-compact the range [0,500) into a NORMAL (non-stale) block. Its range [0,500) overlaps the stale
    // block's [0,1000) but stays below the WAL-only sample at t=700, which remains in the head.
    db.compact_head(&RangeHead::new(db.head().clone(), 0, 499)).unwrap();

    // One stale block [0,1000) and one normal block [0,500) now overlap on disk.
    let mut normal_ulid = Ulid::default();
    for b in db.blocks() {
        let m = b.meta();
        if m.ulid == stale_ulid {
            continue;
        }
        normal_ulid = m.ulid;
        assert!(!m.compaction.from_stale_series(), "head-compacted block must not be tagged from-stale-series");
        assert_eq!(500, m.max_time);
    }
    assert_ne!(Ulid::default(), normal_ulid, "the head-compacted normal block must exist");

    // Run the background compaction + reload path. The reload truncates the head based on inOrderBlocksMaxTime.
    db.reload_blocks().unwrap();
    db.compact_blocks(&db.cmtx.lock().unwrap()).unwrap();
    db.reload().unwrap();

    // (a) Segregation holds: the stale block and the overlapping normal block must both still exist as separate
    // blocks. Pre-fix they were merged into one.
    let (mut have_stale, mut have_normal) = (false, false);
    for b in db.blocks() {
        let m = b.meta();
        if m.ulid == stale_ulid {
            have_stale = true;
            assert!(m.compaction.from_stale_series(), "stale block must keep its hint");
        } else if m.ulid == normal_ulid {
            have_normal = true;
        }
    }
    assert!(have_stale, "stale block must not be merged away");
    assert!(have_normal, "overlapping normal block must not be merged away");

    // (b) The stale block must not have advanced the WAL-replay cutoff. The only in-order block is the normal
    // [0,500) block, so the cutoff must be 500, which is below the WAL-only sample at t=700.
    let maxt = db.in_order_blocks_max_time();
    assert!(maxt.is_some());
    let maxt = maxt.unwrap();
    assert_eq!(500, maxt, "inOrderBlocksMaxTime must reflect only the non-stale block");
    assert!(maxt < 700, "the stale block must not advance the WAL-replay cutoff past the WAL-only sample");

    // (c) No in-order data is dropped: the WAL-only sample at t=700, which is past every in-order block, must
    // survive the reload. Querying the head over (500,1000] isolates it from samples also retained in the
    // [0,500) block.
    let rh = RangeHead::new(db.head().clone(), 501, 1000);
    let querier = new_block_querier(&rh, 501, 1000).unwrap();
    let got = query(querier, &[eq("name", "in_order_kept")]);
    assert_eq!(one(r#"{name="in_order_kept"}"#, vec![fsample(700, 42.0)]), got);
}

// TestCompactBlocksLoopDoesNotStarveStaleBlocks drives the real db.compactBlocks() loop with both non-stale and
// stale compactable work present at once and checks that the loop drains both classes and that the
// from-stale-series hint survives the real merge loop.
#[test]
fn test_compact_blocks_loop_does_not_starve_stale_blocks() {
    // MinBlockDuration 1000 with MaxBlockDuration 3000 yields compactor ranges ExponentialBlockRanges(1000, 10, 3)
    // capped at 3000 = [1000, 3000]. We build groups that fill the 3000-wide second range so selectDirs picks
    // them.
    let db = new_test_db(vec![with_opts(opts_block(1000, 3000))]);
    db.disable_compactions();

    // oneSeriesBlock builds a single-series block spanning [mint, mint+1000) (createBlock sets maxt to the last
    // sample time + 1).
    let one_series_block = |name: &str, mint: i64| -> Vec<SeriesArc> {
        vec![storage::new_list_series(ls(&["name", name]), vec![fsample(mint, 1.0), fsample(mint + 999, 1.0)])]
    };

    // Non-stale blocks filling the 3000-wide range [0,3000), plus a fresh non-stale block at [7000,8000) so the
    // [0,3000) range is no longer the most recent non-stale block (planClass excludes the most recent block of
    // each class).
    let non_stale_dirs = [
        create_block(db.dir(), &one_series_block("ns_0", 0)),
        create_block(db.dir(), &one_series_block("ns_1", 1000)),
        create_block(db.dir(), &one_series_block("ns_2", 2000)),
        create_block(db.dir(), &one_series_block("ns_fresh", 7000)),
    ];

    // From-stale-series blocks filling the 3000-wide range [3000,6000), plus a fresh stale block at [6000,7000)
    // so the [3000,6000) range is no longer the most recent stale block.
    let stale_dirs = [
        create_block(db.dir(), &one_series_block("st_0", 3000)),
        create_block(db.dir(), &one_series_block("st_1", 4000)),
        create_block(db.dir(), &one_series_block("st_2", 5000)),
        create_block(db.dir(), &one_series_block("st_fresh", 6000)),
    ];

    // Tag the stale-class blocks with the from-stale-series hint, as CompactStaleHead does.
    for dir in &stale_dirs {
        let (mut meta, _) = read_meta_file(dir).unwrap();
        meta.compaction.set_stale_series();
        write_meta_file(dir, &mut meta).unwrap();
    }

    db.reload_blocks().unwrap();
    assert_eq!(
        non_stale_dirs.len() + stale_dirs.len(),
        db.blocks().len(),
        "all eight source blocks must be loaded before compaction"
    );

    // Record the original source ULIDs per class BEFORE compaction.
    let original_non_stale: HashSet<Ulid> =
        non_stale_dirs.iter().map(|d| read_meta_file(d).unwrap().0.ulid).collect();
    let original_stale: HashSet<Ulid> = stale_dirs.iter().map(|d| read_meta_file(d).unwrap().0.ulid).collect();

    // Drive the real loop. It must compact the non-stale group first and, once non-stale work is exhausted, also
    // compact the stale group, then exit.
    db.compact_blocks(&db.cmtx.lock().unwrap()).unwrap();
    db.reload_blocks().unwrap();

    let mut merged_non_stale: Option<BlockMeta> = None;
    let mut merged_stale: Option<BlockMeta> = None;
    for b in db.blocks() {
        let meta = b.meta();

        let mut has_non_stale_source = false;
        let mut has_stale_source = false;
        for src in &meta.compaction.sources {
            if original_non_stale.contains(src) {
                has_non_stale_source = true;
            }
            if original_stale.contains(src) {
                has_stale_source = true;
            }
        }
        // Sanity check: no merged block mixes the two classes.
        assert!(!(has_non_stale_source && has_stale_source), "block {} mixes non-stale and stale sources", meta.ulid);

        // Identify the two merged products (blocks built from more than one original source).
        if meta.compaction.sources.len() > 1 {
            if has_stale_source {
                assert!(merged_stale.is_none(), "expected a single merged stale block");
                merged_stale = Some(meta);
            } else if has_non_stale_source {
                assert!(merged_non_stale.is_none(), "expected a single merged non-stale block");
                merged_non_stale = Some(meta);
            }
        }
    }

    // The non-stale group was merged (proving non-stale work ran).
    let merged_non_stale = merged_non_stale.expect("the three non-stale blocks must be merged");
    assert_eq!(
        3,
        merged_non_stale.compaction.sources.len(),
        "the merged non-stale block must cover the three non-stale source blocks"
    );
    assert!(
        !merged_non_stale.compaction.from_stale_series(),
        "the merged non-stale block must NOT carry the from-stale-series hint"
    );

    // The stale group was ALSO merged, and the merge product still carries the from-stale-series hint.
    let merged_stale = merged_stale.expect("the three stale blocks must also be merged: stale work must not be starved");
    assert_eq!(3, merged_stale.compaction.sources.len(), "the merged stale block must cover the three stale source blocks");
    assert!(merged_stale.compaction.from_stale_series(), "the merged stale block must keep the from-stale-series hint");
}

// TestStaleSeriesCompactionExcludesOutOfOrderSeries verifies that a stale series which also has out-of-order
// data is excluded from the stale-series block and that the produced block is not tagged from-out-of-order.
#[test]
fn test_stale_series_compaction_excludes_out_of_order_series() {
    let mut opts = opts_block(1000, 1000);
    opts.out_of_order_time_window = 1000;
    let db = new_test_db(vec![with_opts(opts)]);
    db.disable_compactions();

    let v = 10.0;
    let stale_v = stale_nan();
    let series_x = ls(&["name", "in_order_stale"]);
    let series_y = ls(&["name", "ooo_stale"]);

    // In-order samples for both X and Y, then a stale NaN for both, marking them stale.
    let mut app = db.appender(&bg());
    app.append(0, &series_x, 100, v).unwrap();
    app.append(0, &series_y, 100, v).unwrap();
    ok(app.commit());

    let mut app = db.appender(&bg());
    app.append(0, &series_x, 200, stale_v).unwrap();
    app.append(0, &series_y, 200, stale_v).unwrap();
    ok(app.commit());

    // Both series are stale now.
    assert_eq!(2, db.head().num_series());
    assert_eq!(2, db.head().num_stale_series());

    // Give series Y an out-of-order sample, which sets s.ooo != nil. The out-of-order append takes the OOO path
    // and does not touch lastValue, so Y remains stale.
    let mut app = db.appender(&bg());
    app.append(0, &series_y, 50, v).unwrap();
    ok(app.commit());

    // Y is still counted as stale; only its out-of-order data should keep it out of the stale block.
    assert_eq!(2, db.head().num_stale_series());

    db.compact_stale_head().unwrap();

    // A from-stale-series block was produced.
    assert_eq!(1, db.blocks().len());
    let meta = db.blocks()[0].meta();
    assert!(meta.compaction.from_stale_series(), "produced block must carry the from-stale-series hint");
    assert!(!meta.compaction.from_out_of_order(), "stale-series block must not be tagged from-out-of-order");

    // The block must contain the in-order stale series X but NOT the out-of-order stale series Y.
    let querier = new_block_querier(&db.blocks()[0], i64::MIN, i64::MAX).unwrap();
    let got = query_without_replacing_nans(querier, &[re("name", ".*")]);
    assert!(got.contains_key(r#"{name="in_order_stale"}"#), "in-order stale series X must be in the stale block");
    assert!(
        !got.contains_key(r#"{name="ooo_stale"}"#),
        "out-of-order stale series Y must be excluded from the stale block"
    );
}

// TestStaleSeriesCompactionWithZeroSeries verifies that CompactStaleHead handles an empty head (0 series)
// gracefully. This is a regression test for issue #17949.
#[test]
fn test_stale_series_compaction_with_zero_series() {
    let db = new_test_db(vec![with_opts(opts_block(1000, 1000))]);
    db.disable_compactions();

    // Verify the head is empty.
    assert_eq!(0, db.head().num_series());
    assert_eq!(0, db.head().num_stale_series());

    // CompactStaleHead should handle zero series gracefully (no panic, no error).
    db.compact_stale_head().unwrap();

    // Should still have no blocks since there was nothing to compact.
    assert!(db.blocks().is_empty());
}

// TestCompactStaleHead_LateStaleAppendSurvives verifies that a stale series is not evicted when another sample is
// appended after CompactStaleHead writes the block but before eviction, even if the new sample is also a stale
// marker.
#[test]
fn test_compact_stale_head_late_stale_append_survives() {
    const CHUNK_RANGE: i64 = 1000;
    let db = new_test_db(vec![with_opts(opts_block(CHUNK_RANGE, CHUNK_RANGE))]);
    db.disable_compactions();

    let stale_v = stale_nan();

    // Filler series keeps head.MaxTime at 700, ensuring the late append at t=400 remains within
    // appendableMinValidTime() = max(700-500, 0) = 200.
    let filler = ls(&["name", "filler"]);
    let sel = ls(&["name", "stale-selected"]);

    let mut app = db.appender(&bg());
    app.append(0, &filler, 100, 0.1).unwrap();
    app.append(0, &filler, 700, 0.7).unwrap();
    let sel_ref = app.append(0, &sel, 100, 10.0).unwrap();
    app.append(sel_ref, &sel, 200, stale_v).unwrap();
    ok(app.commit());

    assert_eq!(700, db.head().max_time());
    assert_eq!(1, db.head().num_stale_series());

    // The hook fires after the block is written but before eviction.
    const LATE_T: i64 = 400;
    let hook_err: Arc<Mutex<Option<Error>>> = Arc::default();
    {
        let (d, he, sel) = (db.db.clone(), hook_err.clone(), sel.clone());
        set_before_evict_hook(&db, move || {
            let mut hook_app = d.appender(&bg());
            if let Err(e) = hook_app.append(sel_ref, &sel, LATE_T, stale_v) {
                *he.lock().unwrap() = Some(e);
                return;
            }
            *he.lock().unwrap() = hook_app.commit();
        });
    }

    db.compact_stale_head().unwrap();
    clear_before_evict_hook(&db);
    assert!(hook_err.lock().unwrap().is_none(), "the late stale append/commit inside the hook must itself succeed");

    // A block is written regardless; what the fingerprint protects is eviction.
    assert_eq!(1, db.blocks().len());
    assert_eq!(2, db.head().num_series(), "filler + sel: sel must survive eviction");
}

// TestCompactStaleHead_EvictedSeriesRecordKeptInCheckpoint verifies that after CompactStaleHead evicts a stale
// series, the series's label record is retained in the next WAL checkpoint while the WAL still holds sample
// records referencing its ref.
#[test]
fn test_compact_stale_head_evicted_series_record_kept_in_checkpoint() {
    const CHUNK_RANGE: i64 = 1000;
    let db = new_test_db(vec![with_opts(opts_block(CHUNK_RANGE, CHUNK_RANGE))]);
    db.disable_compactions();

    let stale_v = stale_nan();

    let sel = ls(&["name", "selected"]);
    let mut app = db.appender(&bg());
    let sel_ref = app.append(0, &sel, 100, 1.0).unwrap();
    app.append(sel_ref, &sel, 200, stale_v).unwrap(); // marks sel as stale
    ok(app.commit());
    assert_eq!(1, db.head().num_stale_series(), "sel must be the only stale series");

    // Evict sel via gcSeries → sets walExpiries[selRef] = head.MaxTime() (200).
    db.compact_stale_head().unwrap();
    assert!(db.head().get_wal_expiry(sel_ref as HeadSeriesRef).is_some(), "walExpiry must be recorded for the evicted ref");

    let truncate_mint = 150;
    let wd = wal_dir(&db);

    // Each truncateWAL call rolls to a new WAL segment; loop until a checkpoint is actually produced.
    for _ in 0..10 {
        db.head().last_wal_truncation_time.store(0, Ordering::SeqCst); // force re-truncation each iteration
        db.head().truncate_wal(truncate_mint).unwrap();
        if wlog::last_checkpoint(&wd).is_ok() {
            break;
        }
    }

    assert!(
        checkpoint_has_series(&wd, sel_ref),
        "the evicted stale series's record must remain in the checkpoint while the WAL still holds sample records referencing this ref"
    );
}

// TestCompactStaleHead_ChunkBoundarySampleNotLost verifies that CompactStaleHead preserves a sample whose
// timestamp lands exactly on a chunk-range boundary.
#[test]
fn test_compact_stale_head_chunk_boundary_sample_not_lost() {
    const CHUNK_RANGE: i64 = 1000;
    let db = new_test_db(vec![with_opts(opts_block(CHUNK_RANGE, CHUNK_RANGE))]);
    db.disable_compactions();

    let stale_v = stale_nan();

    let sel = ls(&["name", "selected"]);
    let mut app = db.appender(&bg());
    let sel_ref = app.append(0, &sel, 500, 1.0).unwrap();
    app.append(sel_ref, &sel, CHUNK_RANGE, stale_v).unwrap(); // T == chunkRange, marks sel stale
    ok(app.commit());

    assert_eq!(500, db.head().min_time(), "test precondition");
    assert_eq!(CHUNK_RANGE, db.head().max_time(), "test precondition");
    assert_eq!(1, db.head().num_stale_series(), "sel must be the only stale series");

    db.compact_stale_head().unwrap();

    let q = db.querier(0, 2 * CHUNK_RANGE).unwrap();
    // Use the no-replacement variant so the stale-NaN bit pattern survives into the result.
    let series_set = query_without_replacing_nans(q, &[eq("name", "selected")]);
    let actual = series_set.get(r#"{name="selected"}"#).cloned().unwrap_or_default();

    assert_eq!(
        2,
        actual.len(),
        "both samples must remain queryable; if the boundary sample is missing the loop in compactHeadViewLocked exited one iteration too early and the slice covering [chunkRange, 2*chunkRange-1] was never produced as a block"
    );
    assert_eq!(500, actual[0].t, "first sample timestamp");
    assert_eq!(1.0, actual[0].f, "first sample value");
    assert_eq!(CHUNK_RANGE, actual[1].t, "boundary sample timestamp");
    assert!(is_stale_nan(actual[1].f), "boundary sample must remain the stale-NaN marker");
}

// TestCompactSelectedSeries_SingleTimestampHeadIsEvicted exercises the case where the head holds a single
// timestamp, i.e., MinTime == MaxTime.
#[test]
fn test_compact_selected_series_single_timestamp_head_is_evicted() {
    const CHUNK_RANGE: i64 = 1000;
    let db = new_test_db(vec![with_opts(opts_block(CHUNK_RANGE, CHUNK_RANGE))]);
    db.disable_compactions();

    let sel = ls(&["name", "single"]);
    let mut app = db.appender(&bg());
    let r = app.append(0, &sel, CHUNK_RANGE, 1.0).unwrap();
    ok(app.commit());

    assert_eq!(CHUNK_RANGE, db.head().min_time(), "test precondition: single-timestamp head");
    assert_eq!(CHUNK_RANGE, db.head().max_time(), "test precondition: single-timestamp head");
    assert_eq!(1, db.head().num_series());

    db.compact_selected_series(&[r]).unwrap();

    assert_eq!(0, db.head().num_series(), "single-timestamp series must be evicted after its sample is persisted to a block");
    assert_eq!(1, db.blocks().len(), "exactly one block should be produced for the single chunk-range slice");
}

// TestCompactSelectedSeries verifies the happy path of CompactSelectedSeries.
#[test]
fn test_compact_selected_series() {
    let db = new_test_db(vec![with_opts(opts_block(1000, 1000))]);
    db.disable_compactions();

    let selected1 = ls(&["name", "selected1"]);
    let selected2 = ls(&["name", "selected2"]);
    let kept = ls(&["name", "kept"]);

    let mut app = db.appender(&bg());
    let s1 = app.append(0, &selected1, 100, 1.0).unwrap();
    let s2 = app.append(0, &selected2, 200, 2.0).unwrap();
    app.append(0, &kept, 300, 3.0).unwrap();
    ok(app.commit());

    assert_eq!(3, db.head().num_series());
    assert_eq!(0, db.head().num_stale_series(), "no series carry stale-NaN markers");

    let head_min_time_before = db.head().min_time();

    db.compact_selected_series(&[s1, s2]).unwrap();

    // The two selected series should have been evicted; the third remains.
    assert_eq!(1, db.head().num_series());
    // numStaleSeries must not have been touched: the evicted series weren't stale.
    assert_eq!(0, db.head().num_stale_series());
    // HeadMinTime must not have advanced.
    assert_eq!(head_min_time_before, db.head().min_time());

    // A block carrying the selected-series hint must have been produced.
    assert_eq!(1, db.blocks().len());
    let bm = db.blocks()[0].meta();
    assert!(bm.compaction.from_selected_series(), "selected-series hint not found in block meta");

    // Selected-series compaction metrics should reflect one successful run.
    assert_eq!(1.0, db.metrics.selected_series_compactions_triggered.get());
    assert_eq!(0.0, db.metrics.selected_series_compactions_failed.get());
}

// TestCompactSelectedSeries_UnsortedDuplicateRefs verifies that CompactSelectedSeries tolerates an input slice
// that is unsorted and contains duplicate refs.
#[test]
fn test_compact_selected_series_unsorted_duplicate_refs() {
    let db = new_test_db(vec![with_opts(opts_block(1000, 1000))]);
    db.disable_compactions();

    let mut app = db.appender(&bg());
    let s1 = app.append(0, &ls(&["name", "selected1"]), 100, 1.0).unwrap();
    let s2 = app.append(0, &ls(&["name", "selected2"]), 200, 2.0).unwrap();
    let s3 = app.append(0, &ls(&["name", "selected3"]), 300, 3.0).unwrap();
    ok(app.commit());
    assert_eq!(3, db.head().num_series());

    // Refs are allocated monotonically, so reversing the natural order guarantees the input is unsorted;
    // inserting each ref twice exercises de-duplication.
    let refs = [s3, s1, s2, s1, s3, s2];
    db.compact_selected_series(&refs)
        .expect("compaction must succeed even when the caller passes unsorted, duplicate refs");

    assert_eq!(0, db.head().num_series(), "each deduplicated series must be evicted exactly once");
    assert_eq!(1, db.blocks().len(), "duplicates must not produce extra blocks");
}

// TestCompactSelectedSeries_EmptyRefs verifies that passing nil or an empty slice is a no-op that does not write
// any block and does not bump the triggered counter.
#[test]
fn test_compact_selected_series_empty_refs() {
    let db = new_test_db(vec![]);
    db.disable_compactions();

    // adapted: Go passes nil and an empty slice; both are an empty slice in Rust.
    db.compact_selected_series(&[]).unwrap();
    db.compact_selected_series(&Vec::new()).unwrap();
    assert!(db.blocks().is_empty());
    assert_eq!(0.0, db.metrics.selected_series_compactions_triggered.get());
}

// TestCompactSelectedSeries_MultipleChunkRanges verifies that one block is produced per chunk range when a
// selected series spans multiple ranges, and each block carries the selected-series hint.
#[test]
fn test_compact_selected_series_multiple_chunk_ranges() {
    let db = new_test_db(vec![with_opts(opts_block(1000, 1000))]);
    db.disable_compactions();

    let sel = ls(&["name", "spanning"]);
    let mut app = db.appender(&bg());
    let mut r = 0;
    // Span samples across two chunk ranges (0-999 and 1000-1999).
    let mut ts = 100;
    while ts < 1500 {
        r = app.append(r, &sel, ts, ts as f64).unwrap();
        ts += 100;
    }
    ok(app.commit());
    assert_eq!(1, db.head().num_series());

    db.compact_selected_series(&[r]).unwrap();
    assert_eq!(0, db.head().num_series());

    assert_eq!(2, db.blocks().len());
    for b in db.blocks() {
        assert!(b.meta().compaction.from_selected_series(), "all selected-series blocks must carry the hint");
    }
}

// TestCompactSelectedSeries_DecrementsNumStaleSeriesWhenStaleSeriesCompacted verifies that evicting a stale
// series via CompactSelectedSeries keeps Head.numStaleSeries in sync.
#[test]
fn test_compact_selected_series_decrements_num_stale_series_when_stale_series_compacted() {
    let db = new_test_db(vec![with_opts(opts_block(1000, 1000))]);
    db.disable_compactions();

    let stale_v = stale_nan();
    let stale = ls(&["name", "stale"]);
    let non_stale1 = ls(&["name", "non-stale-1"]);
    let non_stale2 = ls(&["name", "non-stale-2"]);

    // Stale series: a normal sample followed by a stale-NaN.
    let mut app = db.appender(&bg());
    let stale_ref = app.append(0, &stale, 100, 1.0).unwrap();
    let stale_ref = app.append(stale_ref, &stale, 200, stale_v).unwrap();
    // Non-stale series: ordinary samples only.
    let non_stale_ref1 = app.append(0, &non_stale1, 100, 2.0).unwrap();
    let non_stale_ref2 = app.append(0, &non_stale2, 100, 3.0).unwrap();
    ok(app.commit());

    assert_eq!(3, db.head().num_series());
    assert_eq!(1, db.head().num_stale_series());

    // Evict only a non-stale serie.
    db.compact_selected_series(&[non_stale_ref1]).unwrap();

    // The stale series remains, and the stale counter must not have been touched.
    assert_eq!(2, db.head().num_series());
    assert_eq!(1, db.head().num_stale_series(), "numStaleSeries must not be decremented when evicting non-stale series");

    // Evict a stale serie and a non-stale serie.
    db.compact_selected_series(&[stale_ref, non_stale_ref2]).unwrap();

    // The stale series is removed, and the stale counter must be decremented.
    assert_eq!(0, db.head().num_series());
    assert_eq!(
        0,
        db.head().num_stale_series(),
        "numStaleSeries must be decremented when CompactSelectedSeries evicts a stale serie"
    );
}

// TestCompactSelectedSeries_SkipsSeriesWithOOOData verifies that CompactSelectedSeries skips any ref whose series
// carries out-of-order data.
#[test]
fn test_compact_selected_series_skips_series_with_ooo_data() {
    let mut opts = opts_block(1000, 1000);
    opts.out_of_order_time_window = 1000; // enable OOO ingestion
    let db = new_test_db(vec![with_opts(opts)]);
    db.disable_compactions();

    let with_ooo = ls(&["name", "with-ooo"]);
    let without_ooo = ls(&["name", "without-ooo"]);

    // Both series receive in-order samples at two timestamps.
    let mut app = db.appender(&bg());
    let with_ooo_ref = app.append(0, &with_ooo, 100, 1.0).unwrap();
    let without_ooo_ref = app.append(0, &without_ooo, 100, 2.0).unwrap();
    app.append(with_ooo_ref, &with_ooo, 200, 1.1).unwrap();
    app.append(without_ooo_ref, &without_ooo, 200, 2.1).unwrap();
    ok(app.commit());

    // Push a single OOO sample for one of the series, so its s.ooo field becomes non-nil.
    let mut app = db.appender(&bg());
    app.append(with_ooo_ref, &with_ooo, 50, 1.5).unwrap();
    ok(app.commit());

    assert_eq!(2, db.head().num_series());

    db.compact_selected_series(&[with_ooo_ref, without_ooo_ref]).unwrap();

    // One ref was skipped (OOO), one was compacted.
    assert_eq!(1, db.head().num_series());
    assert_eq!(1.0, db.metrics.selected_series_compactions_triggered.get());
    assert_eq!(0.0, db.metrics.selected_series_compactions_failed.get());

    assert_eq!(1, db.blocks().len());
    assert!(db.blocks()[0].meta().compaction.from_selected_series());
}

// TestCompactSelectedSeries_HintPropagation verifies that a block carrying the FromSelectedSeries hint
// propagates that hint to a new block written from it via the compactor's Write call.
#[test]
fn test_compact_selected_series_hint_propagation() {
    let db = new_test_db(vec![with_opts(opts_block(1000, 1000))]);
    db.disable_compactions();

    // Produce a selected-series block.
    let sel = ls(&["name", "selected"]);
    let mut app = db.appender(&bg());
    let r = app.append(0, &sel, 100, 1.0).unwrap();
    ok(app.commit());
    db.compact_selected_series(&[r]).unwrap();

    assert_eq!(1, db.blocks().len());
    let source_block = db.blocks()[0].clone();
    let source_meta = source_block.meta();
    assert!(source_meta.compaction.from_selected_series());

    // Re-write the block via compactor.Write, passing the source meta as base.
    let dest_dir = TempDir::new();
    let compactor = db.compactor.read().unwrap().clone();
    let uids = compactor
        .write(dest_dir.path(), &source_block, source_meta.min_time, source_meta.max_time + 1, Some(&source_meta))
        .unwrap();
    assert_eq!(1, uids.len());

    let (new_meta, _) = read_meta_file(&dest_dir.path().join(uids[0].to_string())).unwrap();
    assert!(new_meta.compaction.from_selected_series(), "FromSelectedSeries hint must propagate to a compacted block");
}

// TestInOrderBlocksMaxTime_ExcludesSelectedSeriesBlocks verifies that blocks tagged with the FromSelectedSeries
// compaction hint are excluded from the in-order maxt computation.
#[test]
fn test_in_order_blocks_max_time_excludes_selected_series_blocks() {
    let db = new_test_db(vec![with_opts(opts_block(1000, 1000))]);
    db.disable_compactions();

    let sel = ls(&["name", "selected"]);
    let mut app = db.appender(&bg());
    let r = app.append(0, &sel, 100, 1.0).unwrap();
    ok(app.commit());

    db.compact_selected_series(&[r]).unwrap();
    assert_eq!(1, db.blocks().len());
    assert!(db.blocks()[0].meta().compaction.from_selected_series());

    // With only a selected-series block on disk, inOrderBlocksMaxTime must report no in-order block.
    assert!(db.in_order_blocks_max_time().is_none(), "selected-series block must be excluded from inOrderBlocksMaxTime");
}

// TestDBOpen_MinValidTime pins down the max() in open() between the min valid time computed from on-disk blocks
// (inOrderBlocksMaxTime) and the one persisted in the WAL's checkpoint (wlog.ReadMinValidTime).
#[test]
fn test_db_open_min_valid_time() {
    // newDBDirWithBlock creates a fresh DB directory containing a single in-order block, and returns that block's
    // actual on-disk maxt.
    let new_db_dir_with_block = || -> (TempDir, i64) {
        let dir = TempDir::new();
        let block_dir = create_block(dir.path(), &gen_series(1, 1, 0, 5000));
        let (meta, _) = read_meta_file(&block_dir).unwrap();
        (dir, meta.max_time)
    };

    // writeMinValidTimeCheckpoint writes a standalone checkpoint carrying mint as its persisted min valid time,
    // into a WAL that open() will then pick up as db's own.
    let write_min_valid_time_checkpoint = |dir: &Path, mint: i64| {
        let w = wlog::WL::new(dir.join("wal"), compression::Type::None).unwrap();
        wlog::checkpoint(&w, 0, 1000, &|_| true, mint, false, true).unwrap();
        w.close().unwrap();
    };

    // corruptLastCheckpoint flips a byte in the checkpoint's first record, which is always the min valid time
    // record, making wlog.ReadMinValidTime fail instead of returning ok == false.
    let corrupt_last_checkpoint = |dir: &Path| {
        use std::io::{Seek, SeekFrom, Write};
        let (cp_dir, _) = wlog::last_checkpoint(&dir.join("wal")).unwrap();
        let mut f = std::fs::OpenOptions::new().write(true).open(wlog::segment_name(&cp_dir, 0)).unwrap();
        f.seek(SeekFrom::Start(1)).unwrap();
        f.write_all(&[42]).unwrap();
    };

    const WARN_MSG: &str = "Failed to read persisted min valid time";

    type Setup<'a> = Box<dyn Fn(&Path) + 'a>;
    let cases: Vec<(&str, Setup, fn(i64) -> i64, bool)> = vec![
        (
            "stored wins when higher than the blocks-derived value",
            Box::new(|dir: &Path| write_min_valid_time_checkpoint(dir, 999999)),
            |_| 999999,
            false,
        ),
        (
            "blocks win when higher than the stored value",
            Box::new(|dir: &Path| write_min_valid_time_checkpoint(dir, 1)),
            |m| m,
            false,
        ),
        ("no record falls back to the blocks-derived value", Box::new(|_: &Path| {}), |m| m, false),
        (
            "corrupt checkpoint falls back to the blocks-derived value and warns",
            Box::new(|dir: &Path| {
                write_min_valid_time_checkpoint(dir, 999999);
                corrupt_last_checkpoint(dir);
            }),
            |m| m,
            true,
        ),
    ];
    for (name, setup, want_min_valid, want_warn) in cases {
        let (dir, block_max_time) = new_db_dir_with_block();
        setup(dir.path());

        let logs = Arc::new(Mutex::new(String::new()));
        let l2 = logs.clone();
        let logger = Logger::new(move |s: &str| {
            let mut g = l2.lock().unwrap();
            g.push_str(s);
            g.push('\n');
        });
        let db = db::open(dir.path(), Some(logger), Some(Options::default()), None).unwrap();

        assert_eq!(want_min_valid(block_max_time), db.head().min_valid_time.load(Ordering::SeqCst), "{name}");
        let logs = logs.lock().unwrap().clone();
        if want_warn {
            assert!(logs.contains(WARN_MSG), "{name}: {logs}");
        } else {
            assert!(!logs.contains(WARN_MSG), "{name}: {logs}");
        }
        db.close().unwrap();
    }
}

/// `query` of the series `{name="selected"}` over [0, maxt].
fn query_selected(d: &DB, maxt: i64) -> Vec<Sample> {
    let q = d.querier(0, maxt).unwrap();
    let mut series_set = query(q, &[eq("name", "selected")]);
    series_set.remove(r#"{name="selected"}"#).unwrap_or_default()
}

// TestCompactSelectedSeries_LateAppendDuringCompactionSurvivesRestart verifies that a sample appended after the
// block write starts but before eviction is not lost.
#[test]
fn test_compact_selected_series_late_append_during_compaction_survives_restart() {
    const CHUNK_RANGE: i64 = 1000;
    let opts = opts_block(CHUNK_RANGE, CHUNK_RANGE);
    let mut db = new_test_db(vec![with_opts(opts.clone())]);
    db.disable_compactions();

    // Filler series keeps head.MaxTime at 700, ensuring the late append at t=400 remains within
    // appendableMinValidTime() = max(700-500, 0)=200, and is accepted.
    let filler = ls(&["name", "filler"]);
    let sel = ls(&["name", "selected"]);

    let mut app = db.appender(&bg());
    app.append(0, &filler, 100, 0.1).unwrap();
    app.append(0, &filler, 700, 0.7).unwrap();
    let sel_ref = app.append(0, &sel, 100, 10.0).unwrap();
    app.append(sel_ref, &sel, 200, 20.0).unwrap();
    ok(app.commit());

    assert_eq!(700, db.head().max_time());
    let appendable_min_valid = db.head().appendable_min_valid_time_checked().unwrap_or(0);
    assert!(appendable_min_valid <= 400, "appendableMinValidTime must allow t=400");

    const LATE_T: i64 = 400;
    const LATE_V: f64 = 40.0;

    // The hook runs after the block has been written and reloaded, but before eviction.
    let hook_err: Arc<Mutex<Option<Error>>> = Arc::default();
    {
        let (d, he, sel) = (db.db.clone(), hook_err.clone(), sel.clone());
        set_before_evict_hook(&db, move || {
            let mut hook_app = d.appender(&bg());
            if let Err(e) = hook_app.append(sel_ref, &sel, LATE_T, LATE_V) {
                *he.lock().unwrap() = Some(e);
                return;
            }
            *he.lock().unwrap() = hook_app.commit();
        });
    }

    db.compact_selected_series(&[sel_ref]).unwrap();
    clear_before_evict_hook(&db);
    assert!(hook_err.lock().unwrap().is_none(), "the late append/commit inside the hook must itself succeed");

    assert_eq!(1, db.blocks().len());

    // sel survives regardless of isolation, so all three samples remain queryable.
    let expected_samples = vec![fsample(100, 10.0), fsample(200, 20.0), fsample(LATE_T, LATE_V)];

    assert_eq!(2, db.head().num_series(), "filler + sel: sel must survive eviction");

    let before_restart = query_selected(&db, CHUNK_RANGE);
    assert_eq!(expected_samples, before_restart, "all three samples must be visible before restart");

    // Verify the same data is visible after a restart driven by WAL replay.
    restart(&mut db, &opts);

    let after_restart = query_selected(&db, CHUNK_RANGE);
    assert_eq!(expected_samples, after_restart, "all three samples must survive restart");
}

// TestCompactSelectedSeries_RestartDoesNotReplayOrphanedSampleAsUnknownSeriesRef checks that WAL replay, after a
// restart, does not report an unknown series reference for a sample left behind by CompactSelectedSeries.
#[test]
fn test_compact_selected_series_restart_does_not_replay_orphaned_sample_as_unknown_series_ref() {
    const CHUNK_RANGE: i64 = 1000;
    let opts = opts_block(CHUNK_RANGE, CHUNK_RANGE);
    let mut db = new_test_db(vec![with_opts(opts.clone())]);
    db.disable_compactions();

    let sel = ls(&["name", "selected"]);
    let mut app = db.appender(&bg());
    let sel_ref = app.append(0, &sel, 100, 1.0).unwrap();
    ok(app.commit());

    // Roll the WAL well past the segment that declared sel.
    const PRE_ROLL_SEGMENTS: usize = 30;
    for _ in 0..PRE_ROLL_SEGMENTS {
        db.head().wal().unwrap().next_segment().unwrap();
    }

    let mut app = db.appender(&bg());
    app.append(sel_ref, &sel, 1200, 2.0).unwrap();
    ok(app.commit());
    assert_eq!(1200, db.head().max_time());

    db.compact_selected_series(&[sel_ref]).unwrap();
    assert_eq!(0, db.head().num_series(), "sel must be evicted from the head");
    assert_eq!(2, db.blocks().len(), "one selected-series block per chunk range");
    for b in db.blocks() {
        assert!(b.meta().compaction.from_selected_series(), "block must carry the selected-series hint");
    }

    let keep_until = db.head().get_wal_expiry(sel_ref as HeadSeriesRef);
    assert!(keep_until.is_some(), "walExpiry must be recorded for the evicted ref");
    assert_eq!(1200, keep_until.unwrap());

    let wd = wal_dir(&db);
    // forceCheckpointAt repeatedly retries truncateWAL with the given mint until it actually produces a new
    // checkpoint.
    let force_checkpoint_at = |db: &DB, mint: i64| {
        let last_idx = match wlog::last_checkpoint(&wd) {
            Err(wlog::Error::NotFound) => -1,
            Err(e) => panic!("{e}"),
            Ok((_, idx)) => idx,
        };
        for _ in 0..10 {
            db.head().last_wal_truncation_time.store(0, Ordering::SeqCst);
            db.head().truncate_wal(mint).unwrap();
            if let Ok((_, idx)) = wlog::last_checkpoint(&wd) {
                if idx > last_idx {
                    return;
                }
            }
        }
        panic!("no new checkpoint produced for mint={mint}");
    };

    // mint stays within the expiry: sel's record must survive.
    force_checkpoint_at(&db, 150);
    assert!(checkpoint_has_series(&wd, sel_ref), "sel's record must be carried forward while still within its walExpiry");

    // mint now exceeds the expiry: sel's record is finally dropped.
    force_checkpoint_at(&db, 1201);
    assert!(!checkpoint_has_series(&wd, sel_ref), "sel's record must be dropped once its walExpiry has passed");

    restart(&mut db, &opts);

    let unknown_samples = db.head().metrics.wal_replay_unknown_refs_total.get("samples");
    assert_eq!(
        0.0, unknown_samples,
        "WAL replay must not report unknown series references for sel's leftover sample; its data is safely in a selected-series block, but a too-low minValidTime let replay try to match it against a series record that no longer exists"
    );
}

// TestCompactSelectedSeries_OOOAppendDuringCompactionSurvives verifies that a series is not evicted when an
// out-of-order sample transitions it from s.ooo == nil to non-nil during compaction.
#[test]
fn test_compact_selected_series_ooo_append_during_compaction_survives() {
    const CHUNK_RANGE: i64 = 1000;
    let mut opts = opts_block(CHUNK_RANGE, CHUNK_RANGE);
    opts.out_of_order_time_window = CHUNK_RANGE; // enable OOO ingestion
    let db = new_test_db(vec![with_opts(opts)]);
    db.disable_compactions();

    let filler = ls(&["name", "filler"]);
    let sel = ls(&["name", "selected"]);

    let mut app = db.appender(&bg());
    app.append(0, &filler, 100, 0.1).unwrap();
    app.append(0, &filler, 700, 0.7).unwrap();
    let sel_ref = app.append(0, &sel, 100, 10.0).unwrap();
    app.append(sel_ref, &sel, 200, 20.0).unwrap();
    ok(app.commit());

    assert_eq!(2, db.head().num_series());

    // The hook fires after the block is written but before eviction. This sample, earlier than sel's max (200)
    // but within the OOO window, flips sel's s.ooo from nil to non-nil.
    const OOO_T: i64 = 150;
    const OOO_V: f64 = 15.0;
    let hook_err: Arc<Mutex<Option<Error>>> = Arc::default();
    {
        let (d, he, sel) = (db.db.clone(), hook_err.clone(), sel.clone());
        set_before_evict_hook(&db, move || {
            let mut hook_app = d.appender(&bg());
            if let Err(e) = hook_app.append(sel_ref, &sel, OOO_T, OOO_V) {
                *he.lock().unwrap() = Some(e);
                return;
            }
            *he.lock().unwrap() = hook_app.commit();
        });
    }

    db.compact_selected_series(&[sel_ref]).unwrap();
    clear_before_evict_hook(&db);
    assert!(hook_err.lock().unwrap().is_none(), "the OOO append/commit inside the hook must itself succeed");

    assert_eq!(2, db.head().num_series(), "filler + sel: sel must survive eviction");

    assert_eq!(
        vec![fsample(100, 10.0), fsample(OOO_T, OOO_V), fsample(200, 20.0)],
        query_selected(&db, CHUNK_RANGE),
        "all three samples, including the OOO one, must be visible"
    );
}

// TestCompactSelectedSeries_OpenAppenderCommittingDuringCompaction verifies that a sample committed while
// compaction is in progress is not lost.
#[test]
fn test_compact_selected_series_open_appender_committing_during_compaction() {
    const CHUNK_RANGE: i64 = 1000;
    let opts = opts_block(CHUNK_RANGE, CHUNK_RANGE);
    let mut db = new_test_db(vec![with_opts(opts.clone())]);
    db.disable_compactions();

    let filler = ls(&["name", "filler"]);
    let sel = ls(&["name", "selected"]);

    let mut baseline = db.appender(&bg());
    baseline.append(0, &filler, 100, 0.1).unwrap();
    baseline.append(0, &filler, 700, 0.7).unwrap();
    let sel_ref = baseline.append(0, &sel, 100, 10.0).unwrap();
    baseline.append(sel_ref, &sel, 200, 20.0).unwrap();
    ok(baseline.commit());

    assert_eq!(700, db.head().max_time());

    const LATE_T: i64 = 400;
    const LATE_V: f64 = 40.0;

    // Open the appender BEFORE CompactSelectedSeries.
    let mut late = db.appender(&bg());
    late.append(sel_ref, &sel, LATE_T, LATE_V).unwrap();

    // Commit the pre-opened appender after the block is written but before eviction runs.
    let hook_err: Arc<Mutex<Option<Error>>> = Arc::default();
    {
        let he = hook_err.clone();
        set_before_evict_hook(&db, move || {
            *he.lock().unwrap() = late.commit();
        });
    }

    db.compact_selected_series(&[sel_ref]).unwrap();
    clear_before_evict_hook(&db);
    assert!(hook_err.lock().unwrap().is_none(), "the late commit inside the hook must itself succeed");

    assert_eq!(1, db.blocks().len());
    assert_eq!(
        2,
        db.head().num_series(),
        "sel must survive eviction: its late commit changed the fingerprint taken before the block was written"
    );

    let expected = vec![fsample(100, 10.0), fsample(200, 20.0), fsample(LATE_T, LATE_V)];
    assert_eq!(
        expected,
        query_selected(&db, CHUNK_RANGE),
        "all three samples must be visible: the block holds t=100,200 and the head retains the late t=400"
    );

    restart(&mut db, &opts);
    assert_eq!(
        expected,
        query_selected(&db, CHUNK_RANGE),
        "the late sample must survive restart, proving no tombstone was written for sel"
    );
}

/// Go `testAppender` of `TestCompactSelectedSeries_AppenderEvictionRaces`.
enum TestAppender {
    V1(Box<dyn Appender>),
    V2(Box<dyn AppenderV2>),
}

impl TestAppender {
    fn new(v2: bool, h: &Head) -> TestAppender {
        if v2 {
            TestAppender::V2(h.appender_v2(&bg()))
        } else {
            TestAppender::V1(h.appender(&bg()))
        }
    }
    fn append(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        match self {
            TestAppender::V1(a) => a.append(r, l, t, v),
            TestAppender::V2(a) => a.append(r, l, 0, t, v, None, None, &AppendV2Options::default()),
        }
    }
    fn commit(&mut self) -> Option<Error> {
        match self {
            TestAppender::V1(a) => a.commit(),
            TestAppender::V2(a) => a.commit(),
        }
    }
    fn rollback(&mut self) -> Option<Error> {
        match self {
            TestAppender::V1(a) => a.rollback(),
            TestAppender::V2(a) => a.rollback(),
        }
    }
}

/// Queries `{series=<value>}` over all time and checks the samples.
fn query_series(d: &DB, value: &str, lset: &Labels, expected: &[Sample], stage: &str) {
    let q = d.querier(i64::MIN, i64::MAX).unwrap();
    let result = query(q, &[eq("series", value)]);
    assert_eq!(Some(&expected.to_vec()), result.get(&lset.to_string()), "{stage}");
}

// TestCompactSelectedSeries_AppenderEvictionRaces verifies that selected-series eviction remains safe while
// appenders are resolving or updating a series.
#[test]
fn test_compact_selected_series_appender_eviction_races() {
    let appenders = [("v1", false), ("v2", true)];

    // retries after series evicted: an appender that resolves a series just before eviction unlinks it must
    // retry against a live series instead of appending into the evicted one.
    for (name, v2) in appenders {
        let opts = opts_block(1000, 1000);
        let mut db = new_test_db(vec![with_opts(opts.clone())]);
        db.disable_compactions();
        let h = db.head().clone();

        let lset = ls(&["series", "resolved-before-gc"]);
        let mut baseline = db.appender(&bg());
        let old_ref = baseline.append(0, &lset, 100, 1.0).unwrap();
        ok(baseline.commit());
        let old_series = series_by_id(&h, old_ref);

        let mut pending = TestAppender::new(v2, &h);
        let (lookup_done_tx, lookup_done_rx) = mpsc::channel::<()>();
        let (resume_tx, resume_rx) = mpsc::channel::<()>();
        let lookup_done_tx = Mutex::new(Some(lookup_done_tx));
        let resume_rx = Mutex::new(resume_rx);
        // Pause after the appender resolves oldSeries but before it can append, leaving it with a pointer that
        // GC will unlink.
        let os = old_series.clone();
        h.test_set_after_series_lookup(Some(Arc::new(move |s: Option<&Arc<MemSeries>>| {
            if !s.is_some_and(|s| Arc::ptr_eq(s, &os)) {
                return;
            }
            if let Some(tx) = lookup_done_tx.lock().unwrap().take() {
                let _ = tx.send(());
                let _ = resume_rx.lock().unwrap().recv();
            }
        })));

        let (append_done_tx, append_done_rx) = mpsc::channel();
        let l2 = lset.clone();
        let appender_thread = std::thread::spawn(move || {
            let r = pending.append(old_ref, &l2, 200, 2.0);
            let _ = append_done_tx.send(r.err());
            pending
        });
        if lookup_done_rx.recv().is_err() {
            let append_err = append_done_rx.recv().unwrap();
            let mut pending = appender_thread.join().unwrap();
            ok(pending.rollback());
            panic!("{name}: append completed before reaching the series lookup hook: {append_err:?}");
        }

        let compact_err = db.compact_selected_series(&[old_ref]);
        drop(resume_tx);
        let append_err = append_done_rx.recv().unwrap();
        let mut pending = appender_thread.join().unwrap();
        h.test_set_after_series_lookup(None);
        compact_err.unwrap();
        assert_eq!(1, db.blocks().len(), "{name}: selected-series compaction must produce a block");
        assert!(append_err.is_none(), "{name}: {append_err:?}");
        ok(pending.commit());

        let expected = [fsample(100, 1.0), fsample(200, 2.0)];
        query_series(&db, "resolved-before-gc", &lset, &expected, &format!("{name}: before WAL restart"));

        restart(&mut db, &opts);
        db.disable_compactions();
        query_series(&db, "resolved-before-gc", &lset, &expected, &format!("{name}: after WAL restart"));
    }

    // overlapping appenders keep series: eviction must keep a series while any appender still has an
    // uncommitted sample for it. A second appender closing in the meantime must not release the first one's
    // protection.
    for (name, v2) in appenders {
        for closer in ["commit", "rollback"] {
            let opts = opts_block(1000, 1000);
            let mut db = new_test_db(vec![with_opts(opts.clone())]);
            db.disable_compactions();
            let h = db.head().clone();

            let lset = ls(&["series", "overlapping-appenders"]);
            let mut baseline = db.appender(&bg());
            let r = baseline.append(0, &lset, 100, 1.0).unwrap();
            ok(baseline.commit());

            // Leave the newer sample uncommitted so the series remains eligible for deletion by maxTime but must
            // still be protected from GC.
            let mut pending = TestAppender::new(v2, &h);
            pending.append(r, &lset, 200, 2.0).unwrap();

            // Use a duplicate so closing this appender exercises pending-state bookkeeping without advancing the
            // series timestamp and masking the bug.
            let mut interfering = TestAppender::new(v2, &h);
            interfering.append(r, &lset, 100, 1.0).unwrap();
            if closer == "commit" {
                ok(interfering.commit());
            } else {
                ok(interfering.rollback());
            }

            db.compact_selected_series(&[r]).unwrap();
            assert_eq!(1, db.blocks().len(), "{name}/{closer}: selected-series compaction must produce a block");
            ok(pending.commit());

            let expected = [fsample(100, 1.0), fsample(200, 2.0)];
            query_series(&db, "overlapping-appenders", &lset, &expected, &format!("{name}/{closer}: before WAL restart"));

            restart(&mut db, &opts);
            db.disable_compactions();
            query_series(&db, "overlapping-appenders", &lset, &expected, &format!("{name}/{closer}: after WAL restart"));
        }
    }
}

/// The common body of the tests that check that a hinted block is not merged with a regular block (Go
/// `TestSelectedBlockNotMergedWithNonSelectedBlock`, `TestStaleBlockNotMergedWithNonStaleBlock`).
fn hinted_block_not_merged(regular_name: &str, hinted_name: &str, set_hint: fn(&mut BlockMetaCompaction), msg: &str) {
    let db = new_test_db(vec![with_opts(opts_block(1000, 1000))]);
    db.disable_compactions();

    // WAL-only in-order sample at t=700. There is no in-order block covering t=700; it lives only in the
    // head/WAL. If inOrderBlocksMaxTime advances past 700, reload() truncates the head and this sample is lost.
    let wal_series = ls(&["name", "wal_only"]);
    let mut app = db.appender(&bg());
    app.append(0, &wal_series, 700, 42.0).unwrap();
    ok(app.commit());

    // Two on-disk blocks sharing time range [0,1000) so the planner sees them as overlapping: one regular
    // (maxt 500) and one hinted (maxt 1000).
    create_block(
        db.dir(),
        &[storage::new_list_series(ls(&["name", regular_name]), vec![fsample(0, 1.0), fsample(499, 1.0)])],
    );
    let hinted_dir = create_block(
        db.dir(),
        &[storage::new_list_series(ls(&["name", hinted_name]), vec![fsample(0, 1.0), fsample(999, 1.0)])],
    );

    // Tag the hinted block.
    let (mut hinted_meta, _) = read_meta_file(&hinted_dir).unwrap();
    set_hint(&mut hinted_meta.compaction);
    write_meta_file(&hinted_dir, &mut hinted_meta).unwrap();
    assert_eq!(1000, hinted_meta.max_time);

    // Merge blocks the same way the background compactor does, then reload (which truncates the head based on
    // inOrderBlocksMaxTime).
    db.reload_blocks().unwrap();
    db.compact_blocks(&db.cmtx.lock().unwrap()).unwrap();
    db.reload().unwrap();

    // The hinted block must not have advanced the WAL-replay cutoff past the WAL-only sample at t=700.
    let maxt = db.in_order_blocks_max_time();
    assert!(maxt.is_some());
    let maxt = maxt.unwrap();
    assert!(maxt < 700, "{msg} (got maxt={maxt})");

    // The WAL-only in-order sample must survive the reload.
    let rh = RangeHead::new(db.head().clone(), 0, 1000);
    let querier = new_block_querier(&rh, 0, 1000).unwrap();
    let got = query(querier, &[eq("name", "wal_only")]);
    assert_eq!(one(r#"{name="wal_only"}"#, vec![fsample(700, 42.0)]), got);
}

// TestSelectedBlockNotMergedWithNonSelectedBlock reproduces the data-loss path that occurs when a
// from-selected-series block is merged with a non-selected block by ordinary leveled compaction.
#[test]
fn test_selected_block_not_merged_with_non_selected_block() {
    hinted_block_not_merged(
        "non_selected",
        "selected",
        |c| c.set_selected_series(),
        "inOrderBlocksMaxTime advanced past the WAL-only sample; a from-selected-series block was merged with a non-selected block and the hint was dropped",
    );
}

// TestStaleBlockNotMergedWithNonStaleBlock reproduces the #18379 data-loss path end to end.
#[test]
fn test_stale_block_not_merged_with_non_stale_block() {
    hinted_block_not_merged(
        "non_stale",
        "stale",
        |c| c.set_stale_series(),
        "inOrderBlocksMaxTime advanced past the WAL-only sample; a stale block was merged with a non-stale block",
    );
}

// TestOutOfOrderBlockMergePreservesHint drives the from-out-of-order analogue of
// TestStaleBlockNotMergedWithNonStaleBlock end to end.
#[test]
fn test_out_of_order_block_merge_preserves_hint() {
    let db = new_test_db(vec![with_opts(opts_block(1000, 1000))]);
    db.disable_compactions();

    // WAL-only in-order sample at t=700.
    let wal_series = ls(&["name", "wal_only"]);
    let mut app = db.appender(&bg());
    app.append(0, &wal_series, 700, 42.0).unwrap();
    ok(app.commit());

    // Two on-disk blocks sharing time range [0,1000), both tagged from-out-of-order.
    let ooo_dir1 = create_block(
        db.dir(),
        &[storage::new_list_series(ls(&["name", "ooo_a"]), vec![fsample(0, 1.0), fsample(499, 1.0)])],
    );
    let ooo_dir2 = create_block(
        db.dir(),
        &[storage::new_list_series(ls(&["name", "ooo_b"]), vec![fsample(0, 1.0), fsample(999, 1.0)])],
    );

    // Tag both blocks with the from-out-of-order hint, as out-of-order head compaction does.
    for dir in [&ooo_dir1, &ooo_dir2] {
        let (mut meta, _) = read_meta_file(dir).unwrap();
        meta.compaction.set_out_of_order();
        write_meta_file(dir, &mut meta).unwrap();
    }

    // Merge blocks the same way the background compactor does, then reload.
    db.reload_blocks().unwrap();
    db.compact_blocks(&db.cmtx.lock().unwrap()).unwrap();
    db.reload().unwrap();

    // The two source blocks must have been merged into a single block.
    let blocks = db.blocks();
    assert_eq!(1, blocks.len(), "the two overlapping out-of-order blocks must be merged into one");

    // (a) The merged all-out-of-order block must keep the from-out-of-order hint.
    let merged = blocks[0].meta();
    assert!(merged.compaction.from_out_of_order(), "merged all-out-of-order block must keep the from-out-of-order hint");
    assert_eq!(1000, merged.max_time, "merged block must cover the full out-of-order range");

    // (b) inOrderBlocksMaxTime must NOT be advanced by the out-of-order-only block.
    let maxt = db.in_order_blocks_max_time();
    assert!(maxt.is_none(), "out-of-order-only data must not advance inOrderBlocksMaxTime, got maxt={maxt:?}");

    // The WAL-only in-order sample must survive the reload.
    let rh = RangeHead::new(db.head().clone(), 0, 1000);
    let querier = new_block_querier(&rh, 0, 1000).unwrap();
    let got = query(querier, &[eq("name", "wal_only")]);
    assert_eq!(one(r#"{name="wal_only"}"#, vec![fsample(700, 42.0)]), got);
}

// TestCompactSelectedSeries_ChunkBoundarySampleNotLost verifies that CompactSelectedSeries preserves a sample
// whose timestamp lands exactly on a chunk-range boundary.
#[test]
fn test_compact_selected_series_chunk_boundary_sample_not_lost() {
    const CHUNK_RANGE: i64 = 1000;
    let db = new_test_db(vec![with_opts(opts_block(CHUNK_RANGE, CHUNK_RANGE))]);
    db.disable_compactions();

    let sel = ls(&["name", "selected"]);
    let mut app = db.appender(&bg());
    let sel_ref = app.append(0, &sel, 500, 1.0).unwrap();
    app.append(sel_ref, &sel, CHUNK_RANGE, 2.0).unwrap(); // T == chunkRange, on the boundary
    ok(app.commit());

    assert_eq!(500, db.head().min_time(), "test precondition");
    assert_eq!(CHUNK_RANGE, db.head().max_time(), "test precondition");

    db.compact_selected_series(&[sel_ref]).unwrap();

    let actual = query_selected(&db, 2 * CHUNK_RANGE);
    let expected = vec![fsample(500, 1.0), fsample(CHUNK_RANGE, 2.0)];
    assert_eq!(
        expected, actual,
        "the sample at T=chunkRange (the chunk-range boundary) must be captured in a block. With the loop bound `mint < maxt`, the iteration that would have produced a block over [chunkRange, 2*chunkRange-1] never runs, so the boundary sample is lost when sel is evicted."
    );
}

// TestCompactSelectedSeries_SparseSelectedAcrossWideHead verifies that when the selected series only covers a
// small part of the head's time range, CompactSelectedSeries does not write empty blocks for the chunk-range
// slices that have no selected-series data.
#[test]
fn test_compact_selected_series_sparse_selected_across_wide_head() {
    const CHUNK_RANGE: i64 = 1000;
    let db = new_test_db(vec![with_opts(opts_block(CHUNK_RANGE, CHUNK_RANGE))]);
    db.disable_compactions();

    let sel = ls(&["name", "selected"]);
    let filler = ls(&["name", "filler"]);

    // sel covers [100, 200]; filler extends head.MaxTime to 10500.
    let mut app = db.appender(&bg());
    let sel_ref = app.append(0, &sel, 100, 1.0).unwrap();
    app.append(sel_ref, &sel, 200, 2.0).unwrap();
    app.append(0, &filler, 10500, 100.0).unwrap();
    ok(app.commit());

    assert_eq!(100, db.head().min_time());
    assert_eq!(10500, db.head().max_time());

    db.compact_selected_series(&[sel_ref]).unwrap();

    let blocks = db.blocks();
    assert_eq!(
        1,
        blocks.len(),
        "exactly one block must be produced — the slice covering [0, 1000) where sel has data; the other chunk-range slices yield empty views and must not create blocks"
    );

    let m = blocks[0].meta();
    assert!(m.compaction.from_selected_series(), "block must carry the selected-series hint");
    assert_eq!(0, m.min_time, "block covers chunk-range slice [0, 1000)");
    assert_eq!(CHUNK_RANGE, m.max_time, "block covers chunk-range slice [0, 1000)");
    assert_eq!(1, m.stats.num_series, "block must contain exactly one series (sel)");
    assert_eq!(2, m.stats.num_samples, "block must contain exactly two samples (sel's t=100 and t=200)");

    // Also assert via the on-disk directory listing that no extra block dirs were left over from the empty
    // iterations.
    let mut block_dir_count = 0;
    for e in std::fs::read_dir(db.dir()).unwrap() {
        let e = e.unwrap();
        if !e.file_type().unwrap().is_dir() {
            continue;
        }
        // Block directories are ULID-named (26 chars).
        if e.file_name().len() == 26 {
            block_dir_count += 1;
        }
    }
    assert_eq!(
        1, block_dir_count,
        "exactly one block directory must exist on disk; empty-view iterations must not leave block directories behind"
    );
}

// TestCompactSelectedSeries_EvictedSeriesRecordKeptInCheckpoint verifies that after CompactSelectedSeries evicts
// a series, the series's label record is retained in the next WAL checkpoint while the WAL still holds sample
// records referencing its ref.
#[test]
fn test_compact_selected_series_evicted_series_record_kept_in_checkpoint() {
    const CHUNK_RANGE: i64 = 1000;
    let db = new_test_db(vec![with_opts(opts_block(CHUNK_RANGE, CHUNK_RANGE))]);
    db.disable_compactions();

    let sel = ls(&["name", "selected"]);
    let mut app = db.appender(&bg());
    let sel_ref = app.append(0, &sel, 100, 1.0).unwrap();
    app.append(sel_ref, &sel, 200, 2.0).unwrap();
    ok(app.commit());

    // Evict sel via gcSeries → sets walExpiries[selRef] = head.MaxTime() (200).
    db.compact_selected_series(&[sel_ref]).unwrap();
    assert!(db.head().get_wal_expiry(sel_ref as HeadSeriesRef).is_some(), "walExpiry must be recorded for the evicted ref");

    let truncate_mint = 150;
    let wd = wal_dir(&db);
    for _ in 0..10 {
        db.head().last_wal_truncation_time.store(0, Ordering::SeqCst); // force re-truncation each iteration
        db.head().truncate_wal(truncate_mint).unwrap();
        if wlog::last_checkpoint(&wd).is_ok() {
            break;
        }
    }

    assert!(
        checkpoint_has_series(&wd, sel_ref),
        "the evicted series's record must remain in the checkpoint while the WAL still holds sample records referencing this ref"
    );
}

#[test]
fn test_beyond_size_retention_with_percentage() {
    // adapted: Go builds `Block` values with only `numBytesChunks` (1024) and a ULID set. A Rust `Block` is always
    // backed by files, so the test writes identical blocks to disk (same series, so the same size S each) and
    // sets the disk size to 100 * S: the 10% limit then holds exactly 10 blocks, as in Go (10 * 1024 of 102400).
    let tmp = TempDir::new();
    let series = gen_series(1, 1, 0, 10);
    let blocks_dir = tmp.path().join("blocks");
    let mut blocks = Vec::new();
    for _ in 0..11 {
        let dir = create_block(&blocks_dir, &series);
        blocks.push(open_block(None, &dir, None, None).unwrap());
    }
    let block_size = blocks[0].size();
    assert!(blocks.iter().all(|b| b.size() == block_size));
    let disk_size = 100 * block_size as u64;

    let mut opts = Options::default();
    opts.max_percentage = 10.0;
    opts.fs_size_func = Some(Arc::new(move |_: &Path| disk_size));

    let db = new_test_db(vec![with_opts(opts.clone())]);
    assert_eq!(0, db.head().size());

    let last = blocks.pop().unwrap();
    assert_eq!(opts.max_percentage as usize, blocks.len());

    let deletable = db::beyond_size_retention(&db, &blocks);
    assert!(deletable.is_empty());

    let ulid = last.ulid();
    blocks.push(last);

    let deletable = db::beyond_size_retention(&db, &blocks);
    assert_eq!(1, deletable.len());
    assert!(deletable.contains(&ulid));
    for b in blocks {
        b.close().unwrap();
    }
}

fn minutes(m: i64) -> i64 {
    m * 60 * 1000
}

#[test]
fn test_ooo_compaction_across_chunk_id_wrap() {
    for (name, scenario) in sample_type_scenarios() {
        ooo_compaction_across_chunk_id_wrap(name, scenario);
    }
}

fn ooo_compaction_across_chunk_id_wrap(name: &str, scenario: SampleTypeScenario) {
    let mut opts = Options::default();
    opts.out_of_order_cap_max = 5;
    opts.out_of_order_time_window = 4 * 60 * 60 * 1000;

    let db = new_test_db(vec![with_opts(opts)]);
    db.disable_compactions();

    let l = ls(&["l", "v1"]);

    let mut app = db.appender(&bg());

    // In-order sample.
    let (r, s) = scenario.append(app.as_mut(), &l, minutes(200), 200);
    let r = r.unwrap();
    let mut exp_samples = vec![s];

    // 15 OOO samples to create 3 mmapped chunks (cap=5).
    for i in 1..=15 {
        let (res, s) = scenario.append(app.as_mut(), &l, minutes(i), i);
        res.unwrap();
        exp_samples.push(s);
    }
    ok(app.commit());

    // Seed firstOOOChunkID near the wrap boundary.
    let ms = series_by_id(db.head(), r);
    {
        let mut g = ms.lock();
        let ooo = g.ooo_mut().expect("ooo");
        ooo.first_ooo_chunk_id = OOO_CHUNK_ID_MASK - 1;
    }

    // More OOO data whose chunk IDs cross the boundary.
    let mut app = db.appender(&bg());
    for i in 16..=30 {
        let (res, s) = scenario.append(app.as_mut(), &l, minutes(i), i);
        res.unwrap();
        exp_samples.push(s);
    }
    ok(app.commit());

    // Compact.
    db.compact(&bg()).unwrap();

    // Query and verify all data is present.
    let querier = db.querier(0, minutes(300)).unwrap();
    let series_set = query(querier, &[eq("l", "v1")]);

    exp_samples.sort_by_key(|s| s.t);
    let _ = name;
    require_equal_series(&one(&l.to_string(), exp_samples), &series_set, true);
}

// TestInOrderCompactionAcrossChunkIDWrap verifies that queries and head compaction work on a series whose
// in-order chunk IDs wrap past the 23-bit boundary.
#[test]
fn test_in_order_compaction_across_chunk_id_wrap() {
    for (name, scenario) in sample_type_scenarios() {
        in_order_compaction_across_chunk_id_wrap(name, scenario);
    }
}

fn in_order_compaction_across_chunk_id_wrap(name: &str, scenario: SampleTypeScenario) {
    const CHUNK_RANGE: i64 = 100;
    const MAX_T: i64 = 500;

    let db = new_test_db(vec![with_rngs(&[CHUNK_RANGE])]);
    db.disable_compactions();

    let l = ls(&["l", "v1"]);

    // Create the series, then seed firstChunkID at the top of the 23-bit ID space.
    let mut app = db.appender(&bg());
    let (r, _) = scenario.append(app.as_mut(), &l, 0, 0);
    let r = r.unwrap();
    ok(app.commit());

    let first_chunk_id_seed = OOO_CHUNK_ID_MASK - 1;
    let ms = series_by_id(db.head(), r);
    ms.lock().first_chunk_id = first_chunk_id_seed;

    let newest_chunk_id = || {
        let g = ms.lock();
        g.head_chunk_id(g.mmapped_chunks.len() + g.head_chunks.len() - 1)
    };
    let newest_before_appends = newest_chunk_id();

    let mut exp_samples = vec![(scenario.sample_func)(0, 0)];
    let mut app = db.appender(&bg());
    let mut ts = 10;
    while ts < MAX_T {
        let (res, s) = scenario.append(app.as_mut(), &l, ts, ts);
        res.unwrap();
        exp_samples.push(s);
        ts += 10;
    }
    ok(app.commit());
    exp_samples.sort_by_key(|s| s.t);

    assert!(newest_chunk_id() < newest_before_appends, "{name}: chunk IDs should have wrapped past the boundary");

    let matcher = eq("l", "v1");
    let expected = one(&l.to_string(), exp_samples);

    // Queries must resolve the wrapped chunk IDs while the data is in the head.
    let querier = db.querier(0, MAX_T).unwrap();
    require_equal_series(&expected, &query(querier, std::slice::from_ref(&matcher)), true);

    // Head compaction reads every chunk through the same wrapped IDs, then drops the compacted ones via
    // truncateChunksBefore, wrapping firstChunkID too.
    db.compact(&bg()).unwrap();

    let first_chunk_id_after = ms.lock().first_chunk_id;
    assert!(first_chunk_id_after < first_chunk_id_seed, "{name}: firstChunkID should have wrapped past 0");

    // The compacted block must contain every sample.
    let querier = db.querier(0, MAX_T).unwrap();
    require_equal_series(&expected, &query(querier, std::slice::from_ref(&matcher)), true);
}

// TestCompactionSurvivesChunkRollAndMmap verifies that a series receiving a sample that cuts a new chunk,
// immediately followed by mmap of the now-inactive previous chunk, is still correctly retained by
// selected-series and stale-series compaction.
#[test]
fn test_compaction_survives_chunk_roll_and_mmap() {
    for stale in [false, true] {
        let name = if stale { "stale" } else { "selected" };
        let db = new_test_db(vec![with_opts(opts_block(1000, 1000))]);
        db.disable_compactions();
        let sel = ls(&["name", "selected"]);
        let v = if stale { stale_nan() } else { 1.0 };
        let mut app = db.appender(&bg());
        app.append(0, &ls(&["name", "filler"]), 1400, 1.0).unwrap();
        let r = app.append(0, &sel, 900, v).unwrap();
        ok(app.commit());
        let refs = vec![r];
        let before = db.head().snapshot_fingerprints(&refs, u64::MAX);
        let fingerprint_changed = Arc::new(Mutex::new(None));
        {
            let (d, sel, refs, fc) = (db.db.clone(), sel.clone(), refs.clone(), fingerprint_changed.clone());
            set_before_evict_hook(&db, move || {
                let mut app = d.appender(&bg());
                app.append(r, &sel, 1000, v).unwrap();
                ok(app.commit());
                d.force_head_mmap();
                // The chunk roll and mmap alone must not make the fingerprint look unchanged.
                *fc.lock().unwrap() = Some(before != d.head().snapshot_fingerprints(&refs, u64::MAX));
            });
        }
        if stale {
            db.compact_stale_head().unwrap();
        } else {
            db.compact_selected_series(&refs).unwrap();
        }
        clear_before_evict_hook(&db);
        assert_eq!(
            Some(true),
            *fingerprint_changed.lock().unwrap(),
            "{name}: the fingerprint must reflect the append across a chunk roll and mmap"
        );
        let q = db.querier(0, 2000).unwrap();
        let got = query(q, &[eq("name", "selected")]);
        assert_eq!(
            2,
            got.get(r#"{name="selected"}"#).map_or(0, |v| v.len()),
            "{name}: The sample at 1000 must survive compaction."
        );
    }
}

/// The common setup of the tests where a commit pauses after it mutated the selected series: returns the DB,
/// the selected series ref, the selected series, the held lock of the filler series and the commit thread.
fn pause_commit_after_selected_series() -> (TestDb, SeriesRef, Arc<MemSeries>, Arc<HeldLock>, std::thread::JoinHandle<Option<Error>>) {
    let db = new_test_db(vec![with_opts(opts_block(1000, 1000))]);
    db.disable_compactions();
    let sel = ls(&["name", "selected"]);
    let filler = ls(&["name", "filler"]);
    let mut app = db.appender(&bg());
    let r = app.append(0, &sel, 100, 1.0).unwrap();
    let filler_ref = app.append(0, &filler, 700, 1.0).unwrap();
    ok(app.commit());
    let mut app = db.appender(&bg());
    app.append(r, &sel, 400, 2.0).unwrap();
    app.append(filler_ref, &filler, 700, 1.0).unwrap();

    // Pause Commit on its second series after it mutates the selected series.
    let blocker = HeldLock::hold(series_by_id(db.head(), filler_ref));
    let done = std::thread::spawn(move || app.commit());
    let series = series_by_id(db.head(), r);
    eventually(Duration::from_secs(5), Duration::from_millis(1), || {
        let g = series.lock();
        g.max_time() == 400 && !g.has_pending_commit()
    });
    (db, r, series, blocker, done)
}

// TestCompactSelectedSeries_SurvivesMutationFromStillOpenTransaction verifies that a sample survives compaction
// even when it's already in the chunk before the fingerprint snapshot is taken, as long as its transaction was
// still open (not isolation-closed) at that point.
#[test]
fn test_compact_selected_series_survives_mutation_from_still_open_transaction() {
    // Go skips this test when isolation is disabled by default (a build tag); it is enabled here.
    let (db, r, _series, blocker, done) = pause_commit_after_selected_series();

    let commit_err: Arc<Mutex<Option<Option<Error>>>> = Arc::default();
    {
        let (b, ce) = (blocker.clone(), commit_err.clone());
        set_before_evict_hook(&db, move || {
            b.unlock();
            *ce.lock().unwrap() = Some(done.join().unwrap());
        });
    }
    let res = db.compact_selected_series(&[r]);
    blocker.unlock();
    clear_before_evict_hook(&db);
    res.unwrap();
    assert!(matches!(*commit_err.lock().unwrap(), Some(None)), "commit: {:?}", commit_err.lock().unwrap());
    let q = db.querier(0, 1000).unwrap();
    let got = query(q, &[eq("name", "selected")]);
    assert_eq!(2, got.get(r#"{name="selected"}"#).map_or(0, |v| v.len()), "The sample at 400 must survive compaction.");
}

// TestCompactSelectedSeries_SurvivesEvidenceErasedByUnrelatedCleanup verifies that a sample survives compaction
// even when the one piece of evidence that would have proven it's missing from the block gets erased before the
// eviction check ever looks at it.
#[test]
fn test_compact_selected_series_survives_evidence_erased_by_unrelated_cleanup() {
    // Go skips this test when isolation is disabled by default (a build tag); it is enabled here.
    let (db, r, series, blocker, done) = pause_commit_after_selected_series();

    let hook_result: Arc<Mutex<Option<(Option<Error>, Option<Error>, u32)>>> = Arc::default();
    {
        let (b, hr, d) = (blocker.clone(), hook_result.clone(), db.db.clone());
        set_before_evict_hook(&db, move || {
            b.unlock();
            let commit_err = done.join().unwrap();
            // A duplicate sample changes no chunks but runs append-ID cleanup.
            let mut cleanup = d.appender(&bg());
            cleanup.append(r, &ls(&["name", "selected"]), 400, 2.0).unwrap();
            let cleanup_err = cleanup.commit();
            let tx_count = series.lock().txs.as_ref().map_or(0, |t| t.tx_id_count);
            *hr.lock().unwrap() = Some((commit_err, cleanup_err, tx_count));
        });
    }
    let res = db.compact_selected_series(&[r]);
    blocker.unlock();
    clear_before_evict_hook(&db);
    res.unwrap();
    let (commit_err, cleanup_err, tx_count) = hook_result.lock().unwrap().take().expect("hook ran");
    assert!(commit_err.is_none(), "{commit_err:?}");
    assert!(cleanup_err.is_none(), "{cleanup_err:?}");
    assert_eq!(0, tx_count);
    let q = db.querier(0, 1000).unwrap();
    let got = query(q, &[eq("name", "selected")]);
    assert_eq!(2, got.get(r#"{name="selected"}"#).map_or(0, |v| v.len()), "The sample at 400 must survive compaction.");
}

// TestCompactionSurvivesLateAppendWithErasedWatermarkEvidence verifies that a sample appended after the
// fingerprint snapshot survives compaction even when a later, unrelated duplicate write erases the only watermark
// evidence that it happened.
#[test]
fn test_compaction_survives_late_append_with_erased_watermark_evidence() {
    for stale in [false, true] {
        let name = if stale { "stale" } else { "selected" };
        let opts = opts_block(1000, 1000);
        let mut db = new_test_db(vec![with_opts(opts.clone())]);
        db.disable_compactions();
        let sel = ls(&["name", "selected"]);
        let v = if stale { stale_nan() } else { 2.0 };
        let mut app = db.appender(&bg());
        let r = app.append(0, &sel, 100, 1.0).unwrap();
        app.append(r, &sel, 200, v).unwrap();
        app.append(0, &ls(&["name", "filler"]), 700, 1.0).unwrap();
        ok(app.commit());

        {
            let (d, sel) = (db.db.clone(), sel.clone());
            set_before_evict_hook(&db, move || {
                // Both transactions start after the snapshot and block generation.
                for _ in 0..2 {
                    let mut app = d.appender(&bg());
                    app.append(r, &sel, 400, v).unwrap();
                    ok(app.commit());
                }
            });
        }
        if stale {
            db.compact_stale_head().unwrap();
        } else {
            db.compact_selected_series(&[r]).unwrap();
        }
        clear_before_evict_hook(&db);
        let q = db.querier(0, 1000).unwrap();
        let got = query(q, &[eq("name", "selected")]);
        let n = got.get(&sel.to_string()).map_or(0, |v| v.len());
        let before_restart_ok = n == 3;
        restart(&mut db, &opts);
        let q = db.querier(0, 1000).unwrap();
        let got = query(q, &[eq("name", "selected")]);
        assert!(before_restart_ok, "{name}: Before restart: want 3 samples, got {n}");
        assert_eq!(3, got.get(&sel.to_string()).map_or(0, |v| v.len()), "{name}: The sample at 400 must survive restart.");
    }
}
