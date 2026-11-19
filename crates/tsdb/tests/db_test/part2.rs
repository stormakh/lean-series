//! Port of `tsdb/db_test.go`, lines 3399..6614.
//!
//! Rust notes:
//! - Go goroutines are threads; `require.Eventually` is a polling loop with the same timeout and interval.
//! - Go's `t.TempDir()` with a reopened DB: the test keeps the temporary directory in a [`TempDir`] and opens the
//!   DB on it with `with_dir`, so that the directory outlives the first DB.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chunkenc::{ChunkIterator, ValueType};
use model::exemplar::{Exemplar, QueryResult};
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{self, Labels};
use model::matcher::{MatchType, Matcher};
use storage::{AppendV2Options, Appender, AppenderV2, Context, Error, Sample, SelectHints, SeriesRef};
use tsdb::db::{DB, Options, default_options};
use tsdb::BlockReader;
use tsdb::head::RangeHead;
use tsdb_format::wlog::{self, WL, compression};

use crate::common::*;

fn ls(kv: &[&str]) -> Labels {
    labels::from_strings(kv)
}

fn eq(name: &str, value: &str) -> Matcher {
    Matcher::must_new(MatchType::Equal, name, value)
}

fn ctx() -> Context {
    Context::background()
}

fn stale_nan() -> f64 {
    f64::from_bits(model::value::STALE_NAN)
}

fn wal_of(db: &DB) -> Arc<WL> {
    db.head().wal().expect("wal").clone()
}

/// Go `require.Eventually`.
fn eventually(mut cond: impl FnMut() -> bool, wait: Duration, tick: Duration, msg: &str) {
    let deadline = Instant::now() + wait;
    loop {
        if cond() {
            return;
        }
        assert!(Instant::now() < deadline, "{msg}");
        std::thread::sleep(tick);
    }
}

/// Go `&Options{...}` literals: the zero options (unset fields are zero in Go).
fn zero_opts() -> Options {
    tsdb::db::zero_options()
}

#[test]
fn test_range_for_timestamp() {
    let tests = [((0, 5), 5), ((1, 5), 5), ((5, 5), 10), ((6, 5), 10), ((13, 5), 15), ((95, 5), 100)];
    for ((t, width), expected) in tests {
        assert_eq!(expected, tsdb::db::range_for_timestamp(t, width));
    }
}

/// TestChunkReader_ConcurrentReads checks that the chunk result can be read concurrently.
#[test]
fn test_chunk_reader_concurrent_reads() {
    let mut chks: Vec<_> = (1..=5)
        .map(|v| storage::chunks::chunk_from_samples(&[Sample::float(0, 1, v as f64)]).unwrap())
        .collect();

    let temp_dir = TempDir::new();
    let mut chunkw = tsdb_format::chunks::new_writer(temp_dir.path(), &[]).unwrap();
    chunkw.write_chunks(&mut chks).unwrap();
    chunkw.close().unwrap();

    let r = tsdb_format::chunks::new_dir_reader(temp_dir.path(), None).unwrap();
    for chk in &chks {
        std::thread::scope(|s| {
            for _ in 0..100 {
                let (r, chk) = (&r, chk);
                s.spawn(move || {
                    // adapted: the Rust block chunk reader has no iterable result (it is always nil in Go).
                    let chk_act = r.chunk_or_iterable(chk).unwrap();
                    assert_eq!(chk.chunk.as_ref().unwrap().bytes(), chk_act.bytes());
                });
            }
        });
    }
    r.close().unwrap();
}

/// Go `deleteNonBlocks`.
fn delete_non_blocks(db_dir: &Path) -> Result<(), String> {
    for d in std::fs::read_dir(db_dir).map_err(|e| e.to_string())? {
        let d = d.map_err(|e| e.to_string())?;
        let is_dir = d.file_type().map_err(|e| e.to_string())?.is_dir();
        if !tsdb::compact::is_block_dir(&d.file_name().to_string_lossy(), is_dir) {
            let p = d.path();
            if is_dir { std::fs::remove_dir_all(&p) } else { std::fs::remove_file(&p) }.map_err(|e| e.to_string())?;
        }
    }
    for d in std::fs::read_dir(db_dir).map_err(|e| e.to_string())? {
        let d = d.map_err(|e| e.to_string())?;
        let is_dir = d.file_type().map_err(|e| e.to_string())?.is_dir();
        if !tsdb::compact::is_block_dir(&d.file_name().to_string_lossy(), is_dir) {
            return Err(format!(
                "root folder:{} still hase non block directory:{}",
                db_dir.display(),
                d.file_name().to_string_lossy()
            ));
        }
    }
    Ok(())
}

/// TestCompactHead ensures that the head compaction creates a block that is ready for loading and does not
/// cause data loss.
#[test]
fn test_compact_head() {
    let opts = || Options {
        retention_duration: 15 * 24 * 3600 * 1000,
        no_lockfile: true,
        min_block_duration: 2 * 3600 * 1000,
        max_block_duration: 2 * 3600 * 1000,
        wal_compression: compression::Type::Snappy,
        ..zero_opts()
    };
    let dir = TempDir::new();
    let db = new_test_db(vec![with_dir(dir.path()), with_opts(opts())]);
    let mut app = db.appender(&ctx());
    let mut exp_samples = Vec::new();
    let maxt = 100i64;
    for i in 0..maxt {
        let val = rand_f64();
        app.append(0, &ls(&["a", "b"]), i, val).unwrap();
        exp_samples.push((i, val));
    }
    assert!(app.commit().is_none());

    // Compact the Head to create a new block.
    db.compact_head(&RangeHead::new(db.head().clone(), 0, maxt - 1)).unwrap();
    db.close().unwrap();
    drop(db);

    // Delete everything but the new block and reopen the db to query it to ensure it includes the head data.
    delete_non_blocks(dir.path()).unwrap();
    let db = new_test_db(vec![with_dir(dir.path()), with_opts(opts())]);
    assert_eq!(1, db.blocks().len());
    assert_eq!(maxt, db.head().min_time());
    let mut querier = db.querier(0, maxt - 1).unwrap();
    let mut act_samples = Vec::new();
    {
        let mut series_set = querier.select(&ctx(), false, None, &[eq("a", "b")]);
        while series_set.next() {
            let s = series_set.at().clone();
            let mut it = s.iterator(None);
            while it.next() == ValueType::Float {
                act_samples.push(it.at());
            }
            assert!(it.err().is_none());
        }
        assert_eq!(exp_samples, act_samples);
        assert!(series_set.err().is_none());
    }
    assert!(querier.close().is_none());
    db.close().unwrap();
}

/// TestCompactHeadWithDeletion tests https://github.com/prometheus/prometheus/issues/11585.
#[test]
fn test_compact_head_with_deletion() {
    let db = new_test_db(vec![]);
    let mut app = db.appender(&ctx());
    app.append(0, &ls(&["a", "b"]), 10, rand_f64()).unwrap();
    assert!(app.commit().is_none());

    db.delete(&ctx(), 0, 100, &[eq("a", "b")]).unwrap();

    // This recreates the bug.
    db.compact_head(&RangeHead::new(db.head().clone(), 0, 100)).unwrap();
}

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

#[test]
fn test_open_various_block_states() {
    let tmp = TempDir::new();
    let tmp_dir = tmp.path().to_path_buf();

    let mut expected_loaded_dirs = HashSet::new();
    let mut expected_removed_dirs = HashSet::new();
    let mut expected_ignored_dirs = HashSet::new();

    {
        // Ok blocks; should be loaded.
        expected_loaded_dirs.insert(create_block(&tmp_dir, &gen_series(10, 2, 0, 10)));
        expected_loaded_dirs.insert(create_block(&tmp_dir, &gen_series(10, 2, 10, 20)));
    }
    {
        // Block to repair; should be repaired & loaded.
        let db_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../upstream/tsdb/testdata/repair_index_version/01BZJ9WJQPWHGNC2W4J9TA62KC");
        let out_dir = tmp_dir.join("01BZJ9WJQPWHGNC2W4J9TA62KC");
        expected_loaded_dirs.insert(out_dir.clone());

        std::fs::create_dir(&out_dir).unwrap();
        tsdb_format::fileutil::copy_dirs(&db_dir, &out_dir).unwrap();
        // adapted: Go touches the chunks dir in the (read-only here) testdata block before the copy; the port
        // creates it in the copy, which gives the same block.
        std::fs::create_dir_all(out_dir.join("chunks")).unwrap();
    }
    {
        // Missing meta.json; should be ignored and only logged.
        let dir = create_block(&tmp_dir, &gen_series(10, 2, 20, 30));
        std::fs::remove_file(dir.join(tsdb::META_FILENAME)).unwrap();
        expected_ignored_dirs.insert(dir);
    }
    {
        let suffixed = |dir: &Path, s: &str| PathBuf::from(format!("{}{}", path_str(dir), s));
        // Tmp blocks during creation; those should be removed on start.
        let dir = create_block(&tmp_dir, &gen_series(10, 2, 30, 40));
        let to = suffixed(&dir, tsdb::db::TMP_FOR_CREATION_BLOCK_DIR_SUFFIX);
        tsdb_format::fileutil::replace(&dir, &to).unwrap();
        expected_removed_dirs.insert(to);

        // Tmp blocks during deletion; those should be removed on start.
        let dir = create_block(&tmp_dir, &gen_series(10, 2, 40, 50));
        let to = suffixed(&dir, tsdb::db::TMP_FOR_DELETION_BLOCK_DIR_SUFFIX);
        tsdb_format::fileutil::replace(&dir, &to).unwrap();
        expected_removed_dirs.insert(to);

        // Pre-2.21 tmp blocks; those should be removed on start.
        let dir = create_block(&tmp_dir, &gen_series(10, 2, 50, 60));
        let to = suffixed(&dir, tsdb::db::TMP_LEGACY);
        tsdb_format::fileutil::replace(&dir, &to).unwrap();
        expected_removed_dirs.insert(to);
    }
    {
        // One ok block; but two should be replaced.
        let dir = create_block(&tmp_dir, &gen_series(10, 2, 50, 60));
        expected_loaded_dirs.insert(dir.clone());

        let (mut m, _) = tsdb::read_meta_file(&dir).unwrap();

        let compacted = create_block(&tmp_dir, &gen_series(10, 2, 50, 55));
        expected_removed_dirs.insert(compacted.clone());

        let desc = |ulid| tsdb::BlockDesc { ulid, min_time: 0, max_time: 0 };
        let compacted_ulid = tsdb::Ulid::parse(&compacted.file_name().unwrap().to_string_lossy()).unwrap();
        m.compaction.parents.push(desc(compacted_ulid));
        m.compaction.parents.push(desc(tsdb::Ulid::from_parts(1, [0; 10])));
        m.compaction.parents.push(desc(tsdb::Ulid::from_parts(123, [0; 10])));

        // Regression test: Already removed parent can be still in list, which was causing Open errors.
        m.compaction.parents.push(desc(compacted_ulid));
        m.compaction.parents.push(desc(compacted_ulid));
        tsdb::write_meta_file(&dir, &mut m).unwrap();
    }
    let tmp_checkpoint_dir = tmp_dir.join("wal/checkpoint.00000001.tmp");
    std::fs::create_dir_all(&tmp_checkpoint_dir).unwrap();
    let tmp_chunk_snapshot_dir = tmp_dir.join(format!("{}0000.00000001.tmp", tsdb::db::CHUNK_SNAPSHOT_PREFIX));
    std::fs::create_dir_all(&tmp_chunk_snapshot_dir).unwrap();

    let mut opts = default_options();
    opts.retention_duration = 0;
    let db = new_test_db(vec![with_dir(&tmp_dir), with_opts(opts)]);
    let loaded_blocks = db.blocks();

    let mut loaded = 0;
    for l in &loaded_blocks {
        let p = tmp_dir.join(l.meta().ulid.to_string());
        assert!(expected_loaded_dirs.contains(&p), "unexpected block {} was loaded", l.meta().ulid);
        loaded += 1;
    }
    assert_eq!(expected_loaded_dirs.len(), loaded);
    drop(loaded_blocks);
    db.close().unwrap();

    let mut ignored = 0;
    for f in std::fs::read_dir(&tmp_dir).unwrap() {
        let p = tmp_dir.join(f.unwrap().file_name());
        assert!(!expected_removed_dirs.contains(&p), "expected {} to be removed, but still exists", p.display());
        if expected_ignored_dirs.contains(&p) {
            ignored += 1;
        }
    }
    assert_eq!(expected_ignored_dirs.len(), ignored);
    assert!(!tmp_checkpoint_dir.exists());
    assert!(!tmp_chunk_snapshot_dir.exists());
}

// ---------------------------------------------------------------------------------------------------------------
// Exemplar compaction tests.

/// Go `exemplarCompactionAppender` / `newExemplarCompactionAppender`: a V1 appender, or a V2 appender used through
/// `AppenderV2AsLimitedV1` with its `ExemplarAppenderV2`.
enum ExApp {
    V1(Box<dyn Appender>),
    V2(Box<dyn AppenderV2>),
}

impl ExApp {
    fn new(db: &DB, app_v2: bool) -> ExApp {
        if app_v2 { ExApp::V2(db.appender_v2(&ctx())) } else { ExApp::V1(db.appender(&ctx())) }
    }
    fn append(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        match self {
            ExApp::V1(a) => a.append(r, l, t, v),
            ExApp::V2(a) => a.append(r, l, 0, t, v, None, None, &AppendV2Options::default()),
        }
    }
    fn append_histogram(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        match self {
            ExApp::V1(a) => a.append_histogram(r, l, t, h, fh),
            ExApp::V2(a) => a.append(r, l, 0, t, 0.0, h, fh, &AppendV2Options::default()),
        }
    }
    fn append_exemplar(&mut self, r: SeriesRef, l: &Labels, e: &Exemplar) -> Result<SeriesRef, Error> {
        match self {
            ExApp::V1(a) => a.append_exemplar(r, l, e),
            ExApp::V2(a) => {
                let ex = a.as_exemplar_appender_v2().expect("ExemplarAppenderV2");
                ex.append_exemplars(r, l, std::slice::from_ref(e))
            }
        }
    }
    fn commit(&mut self) -> Option<Error> {
        match self {
            ExApp::V1(a) => a.commit(),
            ExApp::V2(a) => a.commit(),
        }
    }
}

fn exemplar_select(db: &DB, end: i64, m: &Matcher) -> Vec<QueryResult> {
    let q = db.exemplar_querier(&ctx()).unwrap();
    q.select(0, end, &[vec![m.clone()]]).unwrap()
}

fn unknown_exemplar_refs(db: &DB) -> f64 {
    db.head().metrics.wal_replay_unknown_refs_total.get("exemplars")
}

/// TestExemplarAfterLastSampleSurvivesCompaction verifies that an exemplar newer than its series's last sample
/// survives ordinary, selected, and stale compaction, WAL cleanup, and subsequent compaction after restart.
#[test]
fn test_exemplar_after_last_sample_survives_compaction() {
    for app_v2 in [false, true] {
        for compaction in ["ordinary", "selected", "stale"] {
            for enable_snapshot in [false, true] {
                let name = format!("{compaction}/snapshot={enable_snapshot}/appV2={app_v2}");
                const BLOCK_RANGE: i64 = 1000;
                let opts = || {
                    let mut opts = default_options();
                    opts.min_block_duration = BLOCK_RANGE;
                    opts.max_block_duration = BLOCK_RANGE;
                    opts.enable_exemplar_storage = true;
                    opts.max_exemplars = 10;
                    opts.enable_memory_snapshot_on_shutdown = enable_snapshot;
                    opts
                };
                let dir = TempDir::new();
                let db = new_test_db(vec![with_dir(dir.path()), with_opts(opts())]);
                db.disable_compactions();

                let series_labels = ls(&["name", "exemplar_only"]);
                let mut app = ExApp::new(&db, app_v2);
                let r = app.append(0, &series_labels, 100, 1.0).unwrap();
                if compaction == "stale" {
                    app.append(r, &series_labels, 200, stale_nan()).unwrap();
                }
                assert!(app.commit().is_none());

                // Leave the series record in an old segment and the exemplar in a recent one.
                const EXEMPLAR_SEGMENT: i64 = 8;
                for _ in 0..EXEMPLAR_SEGMENT {
                    wal_of(&db).next_segment().unwrap();
                }
                let e = Exemplar { labels: ls(&["trace_id", "abc123"]), value: 1.0, ts: 10 * BLOCK_RANGE, has_ts: false };
                let mut app = ExApp::new(&db, app_v2);
                app.append_exemplar(r, &series_labels, &e).unwrap();
                assert!(app.commit().is_none());

                // Selected and stale compaction persist this series before unrelated sample ingestion advances the
                // head. Its exemplar is not in the block. Ordinary compaction is triggered by the unrelated ingestion
                // below.
                if compaction != "ordinary" {
                    match compaction {
                        "selected" => db.compact_selected_series(&[r]).unwrap(),
                        _ => db.compact_stale_head().unwrap(),
                    }
                    assert_eq!(1, db.blocks().len(), "{name}");
                }

                let active_labels = ls(&["name", "active"]);
                let mut app = ExApp::new(&db, app_v2);
                let active_ref = app.append(0, &active_labels, BLOCK_RANGE * 3 / 2 + 101, 1.0).unwrap();
                assert!(app.commit().is_none());
                db.compact(&ctx()).unwrap();
                if compaction == "ordinary" {
                    assert_eq!(1, db.blocks().len(), "{name}");
                }
                {
                    let series = db.head().series.get_by_id(r).expect("series");
                    let s = series.lock();
                    assert!(s.head_chunks.is_empty(), "{name}");
                    assert!(s.app.is_none(), "the retained identity must not keep the compacted chunk alive");
                }
                let (_, first_checkpoint) = wlog::last_checkpoint(wal_of(&db).dir()).unwrap();

                // Advance beyond the expiry that sample-only eviction would assign. Without exemplar protection, WAL
                // cleanup drops the series record while its exemplar remains in a segment that still needs replay.
                let mut app = ExApp::new(&db, app_v2);
                app.append(active_ref, &active_labels, BLOCK_RANGE * 5 / 2 + 101, 2.0).unwrap();
                app.append(active_ref, &active_labels, BLOCK_RANGE * 7 / 2 + 101, 3.0).unwrap();
                assert!(app.commit().is_none());
                db.compact(&ctx()).unwrap();
                if compaction == "ordinary" {
                    assert_eq!(2, db.blocks().len(), "{name}");
                }
                let (_, second_checkpoint) = wlog::last_checkpoint(wal_of(&db).dir()).unwrap();
                assert!(second_checkpoint > first_checkpoint, "{name}");
                assert!(second_checkpoint < EXEMPLAR_SEGMENT, "the exemplar segment must still await replay");
                assert!(db.head().min_time() < e.ts, "{name}");

                let matcher = eq("name", "exemplar_only");
                let expected = vec![QueryResult { series_labels: series_labels.clone(), exemplars: vec![e.clone()] }];
                assert_eq!(expected, exemplar_select(&db, e.ts, &matcher), "{name}");

                db.close().unwrap();
                drop(db);
                let reopened = new_test_db(vec![with_dir(dir.path()), with_opts(opts())]);
                reopened.disable_compactions();
                assert_eq!(
                    expected,
                    exemplar_select(&reopened, e.ts, &matcher),
                    "the exemplar must survive {compaction} compaction and restart"
                );
                assert_eq!(0.0, unknown_exemplar_refs(&reopened), "{name}");

                // Replay must restore the exemplar's eviction protection, so another compaction and restart cannot
                // lose an exemplar that survived the first restart.
                let mut app = ExApp::new(&reopened, app_v2);
                app.append(0, &active_labels, BLOCK_RANGE * 11 / 2 + 101, 4.0).unwrap();
                assert!(app.commit().is_none());
                reopened.compact(&ctx()).unwrap();
                reopened.close().unwrap();
                drop(reopened);
                let reopened = new_test_db(vec![with_dir(dir.path()), with_opts(opts())]);
                reopened.disable_compactions();
                assert_eq!(
                    expected,
                    exemplar_select(&reopened, e.ts, &matcher),
                    "replayed exemplars must survive subsequent compaction and restart"
                );
                assert_eq!(0.0, unknown_exemplar_refs(&reopened), "{name}");
            }
        }
    }
}

/// TestExemplarBeforeLastSampleSurvivesSeriesCompaction verifies that selected and stale compaction preserve
/// exemplars until the global replay cutoff passes them, even when their timestamps precede the samples being
/// compacted.
#[test]
fn test_exemplar_before_last_sample_survives_series_compaction() {
    for app_v2 in [false, true] {
        for stale in [false, true] {
            for cutoff in [100i64, 500] {
                let name = format!("stale={stale}/cutoff={cutoff}/appV2={app_v2}");
                const BLOCK_RANGE: i64 = 1000;
                let opts = || {
                    let mut opts = default_options();
                    opts.min_block_duration = BLOCK_RANGE;
                    opts.max_block_duration = BLOCK_RANGE;
                    opts.enable_exemplar_storage = true;
                    opts.max_exemplars = 10;
                    opts
                };
                let dir = TempDir::new();
                let db = new_test_db(vec![with_dir(dir.path()), with_opts(opts())]);
                db.disable_compactions();

                let series_labels = ls(&["name", "exemplar_series"]);
                let mut app = ExApp::new(&db, app_v2);
                let r = app.append(0, &series_labels, 100, 1.0).unwrap();
                let sample_value = if stale { stale_nan() } else { 2.0 };
                app.append(r, &series_labels, 900, sample_value).unwrap();
                let e = Exemplar { labels: ls(&["trace_id", "abc123"]), value: 1.0, ts: 500, has_ts: false };
                app.append_exemplar(r, &series_labels, &e).unwrap();
                assert!(app.commit().is_none());
                db.head().set_min_valid_time(cutoff);

                if stale {
                    db.compact_stale_head().unwrap();
                } else {
                    db.compact_selected_series(&[r]).unwrap();
                }
                assert_eq!(1, db.blocks().len(), "{name}");
                assert_eq!(1, db.head().num_series(), "the exemplar still needs its series identity");

                db.close().unwrap();
                drop(db);
                let reopened = new_test_db(vec![with_dir(dir.path()), with_opts(opts())]);
                reopened.disable_compactions();
                let matcher = eq("name", "exemplar_series");
                assert_eq!(
                    vec![QueryResult { series_labels: series_labels.clone(), exemplars: vec![e.clone()] }],
                    exemplar_select(&reopened, e.ts, &matcher),
                    "{name}"
                );
                assert_eq!(0.0, unknown_exemplar_refs(&reopened), "{name}");

                // Once replay can skip the exemplar, it no longer prevents eviction.
                reopened.head().set_min_valid_time(e.ts + 1);
                if stale {
                    reopened.compact_stale_head().unwrap();
                } else {
                    reopened.compact_selected_series(&[r]).unwrap();
                }
                assert_eq!(0, reopened.head().num_series(), "{name}");
            }
        }
    }
}

/// TestSeriesCompactionReclaimsChunksWithExemplars verifies that selected and stale compaction release persisted
/// chunks and their appenders while retaining exemplar recovery state. Concurrent appends must prevent chunk
/// reclamation. WAL replay may restore the samples, which a subsequent compaction can reclaim again.
#[test]
fn test_series_compaction_reclaims_chunks_with_exemplars() {
    for app_v2 in [false, true] {
        for compaction in ["selected", "stale"] {
            for sample_type in ["float", "histogram", "float histogram"] {
                for scenario in ["unchanged", "mmapped", "in-order append", "out-of-order append"] {
                    series_compaction_reclaims_chunks_with_exemplars(app_v2, compaction, sample_type, scenario);
                }
            }
        }
    }
}

fn series_compaction_reclaims_chunks_with_exemplars(app_v2: bool, compaction: &str, sample_type: &str, scenario: &str) {
    let name = format!("{compaction}/{sample_type}/{scenario}/appV2={app_v2}");
    const BLOCK_RANGE: i64 = 1000;
    let opts = || {
        let mut opts = default_options();
        opts.min_block_duration = BLOCK_RANGE;
        opts.max_block_duration = BLOCK_RANGE;
        opts.enable_exemplar_storage = true;
        opts.max_exemplars = 10;
        opts.out_of_order_time_window = BLOCK_RANGE;
        opts
    };
    let dir = TempDir::new();
    let db = new_test_db(vec![with_dir(dir.path()), with_opts(opts())]);
    db.disable_compactions();
    let series_labels = ls(&["name", "exemplar_series"]);
    let mut app = ExApp::new(&db, app_v2);
    // A float followed by histograms creates an older chunk while leaving a histogram appender on the current
    // chunk.
    let r = app.append(0, &series_labels, 50, 0.0).unwrap();
    for ts in [100i64, 900] {
        let stale = compaction == "stale" && ts == 900;
        let v = if stale { stale_nan() } else { 2.0 };
        match sample_type {
            "histogram" => {
                let h = if stale {
                    Histogram { sum: v, ..Default::default() }
                } else {
                    storage::tsdbutil::generate_test_histogram(ts)
                };
                app.append_histogram(r, &series_labels, ts, Some(&h), None)
            }
            "float histogram" => {
                let h = if stale {
                    FloatHistogram { sum: v, ..Default::default() }
                } else {
                    storage::tsdbutil::generate_test_float_histogram(ts)
                };
                app.append_histogram(r, &series_labels, ts, None, Some(&h))
            }
            _ => app.append(r, &series_labels, ts, v),
        }
        .unwrap();
    }
    // Keep concurrent samples within the appendable time range and below maxt, so timestamp checks alone cannot
    // protect them.
    app.append(0, &ls(&["name", "active"]), 1100, 1.0).unwrap();
    let e = Exemplar { labels: ls(&["trace_id", "abc123"]), value: 1.0, ts: 500, has_ts: false };
    app.append_exemplar(r, &series_labels, &e).unwrap();
    assert!(app.commit().is_none());
    if scenario == "mmapped" {
        db.force_head_mmap();
    }

    let series = db.head().series.get_by_id(r).expect("series");
    let chunk_count = {
        let s = series.lock();
        let n = s.head_chunk_count() as usize + s.mmapped_chunks.len();
        if scenario == "mmapped" && sample_type != "float" {
            assert!(!s.mmapped_chunks.is_empty(), "{name}");
        }
        n
    };
    assert!(chunk_count > 0, "{name}");
    let removed_before = db.head().metrics.chunks_removed.get();
    db.head().set_min_valid_time(100);

    let expected_times = Arc::new(Mutex::new(vec![50i64, 100, 900]));
    let concurrent_append = scenario == "in-order append" || scenario == "out-of-order append";
    if concurrent_append {
        let (db2, et, sl, scenario, compaction) =
            (db.db.clone(), expected_times.clone(), series_labels.clone(), scenario.to_string(), compaction.to_string());
        *db.compact_head_view_before_evict_testing_callback.lock().unwrap() = Some(Box::new(move || {
            let mut app = ExApp::new(&db2, app_v2);
            let (mut ts, mut v) = (950i64, 3.0);
            if scenario == "out-of-order append" {
                ts = 850;
                *et.lock().unwrap() = vec![50, 100, 850, 900];
            } else {
                et.lock().unwrap().push(ts);
                if compaction == "stale" {
                    v = stale_nan();
                }
            }
            app.append(r, &sl, ts, v).unwrap();
            assert!(app.commit().is_none());
        }));
    }
    let compact = |d: &DB| {
        if compaction == "stale" {
            d.compact_stale_head().unwrap();
        } else {
            d.compact_selected_series(&[r]).unwrap();
        }
    };
    compact(&db);
    assert_eq!(1, db.blocks().len(), "{name}");
    assert_eq!(2, db.head().num_series(), "{name}");
    assert!(Arc::ptr_eq(&series, &db.head().series.get_by_id(r).unwrap()), "{name}");
    if concurrent_append {
        assert!(series.lock().head_chunk_count() > 0, "unpersisted samples must keep their chunks");
        assert_eq!(removed_before, db.head().metrics.chunks_removed.get(), "{name}");
    } else {
        {
            let s = series.lock();
            assert_eq!(0, s.head_chunk_count(), "{name}");
            assert!(s.mmapped_chunks.is_empty(), "{name}");
            assert!(s.app.is_none(), "the appender must not retain the compacted chunk");
        }
        assert_eq!(removed_before + chunk_count as f64, db.head().metrics.chunks_removed.get(), "{name}");
        assert_eq!(1.0, db.head().metrics.chunks.get(), "only the active series retains a chunk");
        // Compacting the retained identity again must neither write another block nor count the removed chunks
        // twice.
        compact(&db);
        assert_eq!(1, db.blocks().len(), "{name}");
        assert_eq!(removed_before + chunk_count as f64, db.head().metrics.chunks_removed.get(), "{name}");
    }
    // Go `t.Cleanup(func() { compactHeadViewBeforeEvictTestingCallback = nil })`.
    *db.compact_head_view_before_evict_testing_callback.lock().unwrap() = None;

    let matcher = eq("name", "exemplar_series");
    let check_data = |d: &DB| {
        assert_eq!(
            vec![QueryResult { series_labels: series_labels.clone(), exemplars: vec![e.clone()] }],
            exemplar_select(d, e.ts, &matcher),
            "{name}"
        );
        let sq = d.querier(0, BLOCK_RANGE).unwrap();
        let res = query(sq, std::slice::from_ref(&matcher));
        let samples = res.get(&series_labels.to_string()).cloned().unwrap_or_default();
        let et = expected_times.lock().unwrap();
        assert_eq!(et.len(), samples.len(), "{name}");
        for (i, ts) in et.iter().enumerate() {
            assert_eq!(*ts, samples[i].t(), "{name}");
        }
    };
    check_data(&db);
    drop(series);
    db.close().unwrap();
    drop(db);
    let reopened = new_test_db(vec![with_dir(dir.path()), with_opts(opts())]);
    reopened.disable_compactions();
    check_data(&reopened);
    assert_eq!(0.0, unknown_exemplar_refs(&reopened), "{name}");
    if !concurrent_append {
        // Reclamation is local to the running head: WAL replay restores these samples until the global replay
        // cutoff passes them.
        assert!(reopened.head().series.get_by_id(r).unwrap().lock().head_chunk_count() > 0, "{name}");
        compact(&reopened);
        assert_eq!(0, reopened.head().series.get_by_id(r).unwrap().lock().head_chunk_count(), "{name}");
        check_data(&reopened);
    }
}
