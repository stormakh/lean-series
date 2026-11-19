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

#[test]
fn test_one_checkpoint_per_compact_call() {
    let block_range = 1000i64;
    let opts = || Options {
        retention_duration: block_range * 1000,
        no_lockfile: true,
        min_block_duration: block_range,
        max_block_duration: block_range,
        ..zero_opts()
    };

    let dir = TempDir::new();
    let db = new_test_db(vec![with_dir(dir.path()), with_opts(opts())]);
    db.disable_compactions();

    // Case 1: Lot's of uncompacted data in Head.

    let lbls = ls(&["foo_d", "choco_bar"]);
    // Append samples spanning 59 block ranges.
    let mut app = db.appender(&ctx());
    for i in 0..60i64 {
        app.append(0, &lbls, block_range * i, rand_f64()).unwrap();
        app.append(0, &lbls, block_range * i + block_range / 2, rand_f64()).unwrap();
        // Rotate the WAL file so that there is >3 files for checkpoint to happen.
        wal_of(&db).next_segment().unwrap();
    }
    assert!(app.commit().is_none());

    // Check the existing WAL files.
    let wal_dir = wal_of(&db).dir().to_path_buf();
    assert_eq!((0, 60), wlog::segments(&wal_dir).unwrap());

    let metrics = &db.head().metrics;
    assert_eq!(0.0, metrics.checkpoint_creation_total.get());
    db.compact(&ctx()).unwrap();
    assert_eq!(1.0, metrics.checkpoint_creation_total.get());

    // As the data spans for 59 blocks, 58 go to disk and 1 remains in Head.
    assert_eq!(58, db.blocks().len());
    // Though WAL was truncated only once, head should be truncated after each compaction.
    assert_eq!(58.0, metrics.head_truncate_total.get());

    // The compaction should have only truncated first 2/3 of WAL (while also rotating the files).
    assert_eq!((40, 61), wlog::segments(&wal_dir).unwrap());

    // The first checkpoint would be for first 2/3rd of WAL, hence till 39. That should be the last checkpoint.
    assert_eq!(39, wlog::last_checkpoint(&wal_dir).unwrap().1);

    // Case 2: Old blocks on disk. The above blocks will act as old blocks.

    // Creating a block to cover the data in the Head so that Head will skip the data during replay and start
    // fresh.
    let blocks = db.blocks();
    let new_block_mint = blocks[blocks.len() - 1].meta().max_time;
    let new_block_maxt = db.head().max_time() + 1;
    drop(blocks);
    db.close().unwrap();
    drop(db);

    create_block(dir.path(), &gen_series(1, 1, new_block_mint, new_block_maxt));

    let db = new_test_db(vec![with_dir(dir.path()), with_opts(opts())]);
    db.disable_compactions();

    // 1 block more.
    assert_eq!(59, db.blocks().len());
    // No series in Head because of this new block.
    assert_eq!(0, db.head().num_series());

    // Adding sample way into the future.
    let mut app = db.appender(&ctx());
    app.append(0, &lbls, block_range * 120, rand_f64()).unwrap();
    assert!(app.commit().is_none());

    // The mint of head is the last block maxt, that means the gap between mint and maxt of Head is too large.
    // This will trigger many compactions.
    assert_eq!(new_block_maxt, db.head().min_time());

    // Another WAL file was rotated.
    assert_eq!((40, 62), wlog::segments(&wal_dir).unwrap());

    let metrics = &db.head().metrics;
    assert_eq!(0.0, metrics.checkpoint_creation_total.get());
    db.compact(&ctx()).unwrap();
    assert_eq!(1.0, metrics.checkpoint_creation_total.get());

    // No new blocks should be created as there was not data in between the new samples and the blocks.
    assert_eq!(59, db.blocks().len());

    // The compaction should have only truncated first 2/3 of WAL (while also rotating the files).
    assert_eq!((55, 63), wlog::segments(&wal_dir).unwrap());

    // The first checkpoint would be for first 2/3rd of WAL, hence till 54. That should be the last checkpoint.
    assert_eq!(54, wlog::last_checkpoint(&wal_dir).unwrap().1);
}

#[test]
fn test_no_panic_on_tsdb_open_error() {
    let tmpdir = TempDir::new();

    // Taking the lock will cause a TSDB startup error.
    let mut l = tsdb::tsdbutil::DirLocker::new(tmpdir.path(), "tsdb", tsdb::Logger::nop()).unwrap();
    l.lock().unwrap();

    assert!(tsdb::db::open(tmpdir.path(), None, Some(default_options()), None).is_err());

    l.release().unwrap();
}

/// The names of the threads of this process.
fn thread_names() -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|e| std::fs::read_to_string(e.ok()?.path().join("comm")).ok())
        .map(|s| s.trim().to_string())
        .collect();
    names.sort();
    names
}

#[test]
fn test_no_goroutine_leak_on_tsdb_open_error() {
    // adapted: Go checks with goleak that no goroutine remains. The port checks the threads of the process
    // (/proc/self/task) before and after the failed open. Other tests run threads in parallel, so the test runs
    // itself again in a child process that runs only this test, on one test thread.
    const CHILD: &str = "DB_TEST_NO_GOROUTINE_LEAK_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "part2::test_no_goroutine_leak_on_tsdb_open_error", "--test-threads=1", "--nocapture"])
            .env(CHILD, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(out.status.success(), "{stdout}{}", String::from_utf8_lossy(&out.stderr));
        assert!(stdout.contains("1 passed"), "{stdout}");
        return;
    }

    let dir = TempDir::new();

    // Create a WAL directory with a valid segment so that the WAL opens.
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let w = WL::new_size(&wal_dir, 32768, compression::Type::None).unwrap();
    w.close().unwrap();
    drop(w);

    // Create a corrupt chunks_head file with an invalid magic number. This will cause the chunk disk mapper to
    // fail, which in turn causes the head to fail.
    let chunks_dir = dir.path().join("chunks_head");
    std::fs::create_dir_all(&chunks_dir).unwrap();
    std::fs::write(chunks_dir.join("000001"), [0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x00, 0x00, 0x00]).unwrap();

    let mut opts = default_options();
    opts.head_chunks_write_queue_size = 1000;

    let before = thread_names();
    assert!(tsdb::db::open(dir.path(), None, Some(opts), None).is_err());

    // Verify that no threads were leaked.
    eventually(
        || thread_names() == before,
        Duration::from_secs(5),
        Duration::from_millis(10),
        &format!("leaked threads: before {before:?}, after {:?}", thread_names()),
    );
}

#[test]
fn test_lockfile() {
    let dirs = Mutex::new(Vec::new());
    tsdb::tsdbutil::test_dir_locker_usage(
        || {
            let d = TempDir::new();
            let p = d.path().to_path_buf();
            dirs.lock().unwrap().push(d);
            p
        },
        |data, create_lock| {
            let mut opts = default_options();
            opts.no_lockfile = !create_lock;

            // Create the DB. This should create lockfile and its metrics.
            let db = tsdb::db::open(data, None, Some(opts), None).unwrap();
            // adapted: the DB keeps its locker behind a mutex; the test takes it out to give a reference to it and
            // puts it back before closing the DB, which releases it.
            let l = db.locker.lock().unwrap().take().unwrap();
            (db, l)
        },
        |(_, l)| l,
        |(db, l)| {
            *db.locker.lock().unwrap() = Some(l);
            db.close().unwrap();
        },
    );
}

/// Go `testQuerierShouldNotPanicIfHeadChunkIsTruncatedWhileReadingQueriedChunks` and its chunk querier variant
/// (shared setup).
fn head_chunk_truncated_setup(db: &DB, num_series: usize, interval: i64) -> Vec<Labels> {
    // Generate the metrics we're going to append.
    let metrics: Vec<Labels> = (0..num_series).map(|i| ls(&[labels::METRIC_NAME, &format!("test_{i}")])).collect();

    // Push 1 sample every 15s for 2x the block duration period.
    let mut ts = 0i64;
    while ts < 2 * DEFAULT_BLOCK_DURATION {
        let mut app = db.appender(&ctx());
        for metric in &metrics {
            app.append(0, metric, ts, ts as f64).unwrap();
        }
        assert!(app.commit().is_none());
        ts += interval;
    }

    // Compact the TSDB head for the first time. We expect the head chunks file has been cut.
    db.compact(&ctx()).unwrap();
    assert_eq!(1.0, db.head().metrics.head_truncate_total.get());

    // Push more samples for another 1x block duration period.
    while ts < 3 * DEFAULT_BLOCK_DURATION {
        let mut app = db.appender(&ctx());
        for metric in &metrics {
            app.append(0, metric, ts, ts as f64).unwrap();
        }
        assert!(app.commit().is_none());
        ts += interval;
    }
    metrics
}

/// Go's memory stress before reading the chunks.
fn stress_memory() {
    const NUM_STRESS_ITERATIONS: usize = 10000;
    const MIN_STRESS_ALLOCATION_BYTES: usize = 128 * 1024;
    const MAX_STRESS_ALLOCATION_BYTES: usize = 512 * 1024;
    // adapted: Go grows one buffer (reset every 1000 iterations) and relies on its GC to release the chunk
    // memory. Rust frees memory deterministically; the port allocates and touches the same sizes, without
    // keeping more than one allocation alive.
    let mut sum = 0u64;
    for _ in 0..NUM_STRESS_ITERATIONS {
        let n = MIN_STRESS_ALLOCATION_BYTES
            + (rand_f64() * (MAX_STRESS_ALLOCATION_BYTES - MIN_STRESS_ALLOCATION_BYTES) as f64) as usize;
        let buf = std::hint::black_box(vec![1u8; n]);
        sum += buf[n - 1] as u64;
    }
    std::hint::black_box(sum);
}

#[test]
#[ignore = "skipped upstream (t.Skip); the second db.Compact waits forever for the open querier"]
fn test_querier_should_not_panic_if_head_chunk_is_truncated_while_reading_queried_chunks() {
    // Go skips this test ("TODO: investigate why process crash in CI"). It cannot pass in Go either: the second
    // `db.Compact` truncates the head, which waits (`WaitForPendingReadersInTimeRange`) for the querier over
    // [0, MaxInt64] that is still open, so it never returns. The port keeps the test body and the skip.
    const NUM_RUNS: usize = 5;
    for _ in 1..=NUM_RUNS {
        const NUM_SERIES: usize = 1000;
        let db = new_test_db(vec![]);
        // Disable compactions so we can control it.
        db.disable_compactions();
        let interval = 15 * 1000i64;
        head_chunk_truncated_setup(&db, NUM_SERIES, interval);

        // At this point we expect 2 mmap-ed head chunks.

        // Get a querier and make sure it's closed only once the test is over.
        let mut querier = db.querier(0, i64::MAX).unwrap();

        // Query back all series.
        let hints = SelectHints { start: 0, end: i64::MAX, step: interval, ..Default::default() };
        let mut series_set =
            querier.select(&ctx(), true, Some(&hints), &[Matcher::must_new(MatchType::Regexp, labels::METRIC_NAME, ".+")]);

        // Fetch samples iterators from all series.
        let mut series = Vec::new();
        while series_set.next() {
            series.push(series_set.at().clone());
        }
        assert!(series_set.err().is_none());
        assert_eq!(NUM_SERIES, series.len());
        let mut iterators = Vec::new();
        for s in &series {
            // Get the iterator and call Next() so that we're sure the chunk is loaded.
            let mut it = s.iterator(None);
            it.next();
            it.at();
            iterators.push(it);
        }

        // Compact the TSDB head again.
        db.compact(&ctx()).unwrap();
        assert_eq!(2.0, db.head().metrics.head_truncate_total.get());

        // At this point we expect 1 head chunk has been deleted.
        stress_memory();

        // Iterate samples.
        let mut sum = 0.0;
        let mut first_err = None;
        for it in &mut iterators {
            while it.next() == ValueType::Float {
                sum += it.at().1;
            }
            if let Some(e) = it.err() {
                first_err = Some(e);
            }
        }
        std::hint::black_box(sum);

        // After having iterated all samples we also want to be sure no error occurred or the "cannot populate
        // chunk XXX: not found" error occurred.
        if let Some(e) = first_err {
            assert!(e.to_string().contains("cannot populate chunk"), "{e}");
        }
        drop(iterators);
        drop(series);
        drop(series_set);
        assert!(querier.close().is_none());
    }
}

#[test]
#[ignore = "skipped upstream (t.Skip); the second db.Compact waits forever for the open querier"]
fn test_chunk_querier_should_not_panic_if_head_chunk_is_truncated_while_reading_queried_chunks() {
    // Go skips this test ("TODO: investigate why process crash in CI"). It cannot pass in Go either: the second
    // `db.Compact` truncates the head, which waits (`WaitForPendingReadersInTimeRange`) for the querier over
    // [0, MaxInt64] that is still open, so it never returns. The port keeps the test body and the skip.
    const NUM_RUNS: usize = 5;
    for _ in 1..=NUM_RUNS {
        const NUM_SERIES: usize = 1000;
        let db = new_test_db(vec![]);
        // Disable compactions so we can control it.
        db.disable_compactions();
        let interval = 15 * 1000i64;
        head_chunk_truncated_setup(&db, NUM_SERIES, interval);

        // Get a querier and make sure it's closed only once the test is over.
        let mut querier = db.chunk_querier(0, i64::MAX).unwrap();

        // Query back all series.
        let hints = SelectHints { start: 0, end: i64::MAX, step: interval, ..Default::default() };
        let mut series_set =
            querier.select(&ctx(), true, Some(&hints), &[Matcher::must_new(MatchType::Regexp, labels::METRIC_NAME, ".+")]);

        // Iterate all series and get their chunks.
        let mut chunks = Vec::new();
        let mut actual_series = 0;
        while series_set.next() {
            actual_series += 1;
            let s = series_set.at().clone();
            let mut it = s.iterator(None);
            while it.next() {
                chunks.push(it.at().chunk.clone().expect("chunk"));
            }
        }
        assert!(series_set.err().is_none());
        assert_eq!(NUM_SERIES, actual_series);

        // Compact the TSDB head again.
        db.compact(&ctx()).unwrap();
        assert_eq!(2.0, db.head().metrics.head_truncate_total.get());

        // At this point we expect 1 head chunk has been deleted.
        stress_memory();

        // Iterate chunks and read their bytes slice (Go computes their CRC32), to make sure the memory of the
        // bytes is still valid.
        let mut sum = 0u64;
        for chunk in &chunks {
            sum = chunk.bytes().iter().fold(sum, |a, b| a.wrapping_mul(31).wrapping_add(*b as u64));
        }
        std::hint::black_box(sum);
        drop(series_set);
        assert!(querier.close().is_none());
    }
}

/// The shared setup of the three `TestQuerierShouldNotFailIfOOOCompactionOccurs...` tests: returns the DB, the
/// number of samples written and the interval.
fn ooo_compaction_querier_setup() -> (TestDb, usize, i64) {
    let mut opts = default_options();
    opts.out_of_order_time_window = 3 * DEFAULT_BLOCK_DURATION;
    let db = new_test_db(vec![with_opts(opts)]);

    // Disable compactions so we can control it.
    db.disable_compactions();

    let metric = ls(&[labels::METRIC_NAME, "test_metric"]);
    let interval = 15 * 1000i64;
    let mut ts = 0i64;
    let mut samples_written = 0;

    // Capture the first timestamp - this will be the timestamp of the OOO sample we'll append below.
    let ooo_ts = ts;
    ts += interval;

    // Push samples after the OOO sample we'll write below.
    while ts < 10 * interval {
        let mut app = db.appender(&ctx());
        app.append(0, &metric, ts, ts as f64).unwrap();
        assert!(app.commit().is_none());
        samples_written += 1;
        ts += interval;
    }

    // Push a single OOO sample.
    let mut app = db.appender(&ctx());
    app.append(0, &metric, ooo_ts, ts as f64).unwrap();
    assert!(app.commit().is_none());
    samples_written += 1;
    (db, samples_written, interval)
}

/// Starts the OOO head compaction in a thread; returns its completion flag and result channel.
fn start_ooo_compaction(db: &TestDb) -> (Arc<AtomicBool>, std::sync::mpsc::Receiver<Result<(), Error>>) {
    let complete = Arc::new(AtomicBool::new(false));
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let (db2, c2) = (db.db.clone(), complete.clone());
    std::thread::spawn(move || {
        let r = db2.compact_ooo_head(&ctx());
        tx.send(r).unwrap();
        c2.store(true, Ordering::SeqCst);
    });
    (complete, rx)
}

fn select_test_metric(q: &dyn storage::ChunkQuerier, interval: i64) -> storage::BoxChunkSeriesSet {
    let hints = SelectHints { start: 0, end: i64::MAX, step: interval, ..Default::default() };
    q.select(&ctx(), true, Some(&hints), &[eq(labels::METRIC_NAME, "test_metric")])
}

fn collect_chunk_series(mut ss: storage::BoxChunkSeriesSet) -> Vec<storage::ChunkSeriesArc> {
    let mut series = Vec::new();
    while ss.next() {
        series.push(ss.at().clone());
    }
    assert!(ss.err().is_none());
    series
}

fn count_chunk_samples(it: &mut storage::BoxMetaIterator<'_>) -> usize {
    let mut samples_read = 0;
    while it.next() {
        samples_read += it.at().chunk.as_ref().unwrap().num_samples();
    }
    assert!(it.err().is_none());
    samples_read
}

#[test]
fn test_querier_should_not_fail_if_ooo_compaction_occurs_after_retrieving_querier() {
    let (db, samples_written, interval) = ooo_compaction_querier_setup();

    // Get a querier.
    let mut querier_created_before_compaction = db.chunk_querier(0, i64::MAX).unwrap();

    // Start OOO head compaction.
    let (compaction_complete, compaction_err) = start_ooo_compaction(&db);

    // Wait until CompactOOOHead has written the OOO block, reloaded, and published lastGarbageCollectedMmapRef
    // before creating the second querier below.
    eventually(
        || {
            compaction_complete.load(Ordering::SeqCst)
                || db.mtx.read().unwrap().last_garbage_collected_mmap_ref.0 != 0
        },
        Duration::from_secs(60),
        Duration::from_millis(10),
        "CompactOOOHead did not reach the reader-wait phase",
    );
    if compaction_complete.load(Ordering::SeqCst) {
        compaction_err.recv().unwrap().unwrap();
    }
    // The compaction must still be waiting for querierCreatedBeforeCompaction to be closed.
    assert!(
        !compaction_complete.load(Ordering::SeqCst),
        "compaction completed before reading chunks or closing querier created before compaction"
    );

    // Get another querier. This one should only use the compacted blocks from disk and ignore the chunks that
    // will be garbage collected.
    let mut querier_created_after_compaction = db.chunk_querier(0, i64::MAX).unwrap();

    let test_querier = |q: &dyn storage::ChunkQuerier| {
        // Query back the series.
        let series = collect_chunk_series(select_test_metric(q, interval));
        assert_eq!(1, series.len());
        let mut iterator = series[0].iterator(None);
        // Check that we can still successfully read all samples.
        assert_eq!(samples_written, count_chunk_samples(&mut iterator));
    };

    test_querier(querier_created_before_compaction.as_ref());

    assert!(
        !compaction_complete.load(Ordering::SeqCst),
        "compaction completed before closing querier created before compaction"
    );
    assert!(querier_created_before_compaction.close().is_none());
    eventually(
        || compaction_complete.load(Ordering::SeqCst),
        Duration::from_secs(60),
        Duration::from_millis(10),
        "compaction should complete after querier created before compaction was closed, and not wait for querier \
         created after compaction",
    );
    compaction_err.recv().unwrap().unwrap();
    assert_eq!(1.0, db.head().metrics.chunks_removed.get());

    // Use the querier created after compaction and confirm it returns the expected results without error.
    test_querier(querier_created_after_compaction.as_ref());
    assert!(querier_created_after_compaction.close().is_none());
}

#[test]
fn test_querier_should_not_fail_if_ooo_compaction_occurs_after_selecting() {
    let (db, samples_written, interval) = ooo_compaction_querier_setup();

    // Get a querier.
    let mut querier = db.chunk_querier(0, i64::MAX).unwrap();

    // Query back the series.
    let series_set = select_test_metric(querier.as_ref(), interval);

    // Start OOO head compaction.
    let (compaction_complete, compaction_err) = start_ooo_compaction(&db);

    // Give CompactOOOHead time to start work. If it does not wait for the querier to be closed, then the query
    // will return incorrect results or fail.
    std::thread::sleep(Duration::from_secs(1));
    assert!(!compaction_complete.load(Ordering::SeqCst), "compaction completed before reading chunks or closing querier");

    // Collect the iterator for the series.
    let series = collect_chunk_series(series_set);
    assert_eq!(1, series.len());
    let mut iterator = series[0].iterator(None);

    // Check that we can still successfully read all samples.
    assert_eq!(samples_written, count_chunk_samples(&mut iterator));

    assert!(!compaction_complete.load(Ordering::SeqCst), "compaction completed before closing querier");
    drop(iterator);
    assert!(querier.close().is_none());
    eventually(
        || compaction_complete.load(Ordering::SeqCst),
        Duration::from_secs(60),
        Duration::from_millis(10),
        "compaction should complete after querier was closed",
    );
    compaction_err.recv().unwrap().unwrap();
    assert_eq!(1.0, db.head().metrics.chunks_removed.get());
}

#[test]
fn test_querier_should_not_fail_if_ooo_compaction_occurs_after_retrieving_iterators() {
    let (db, samples_written, interval) = ooo_compaction_querier_setup();

    // Get a querier.
    let mut querier = db.chunk_querier(0, i64::MAX).unwrap();

    // Query back the series.
    let series_set = select_test_metric(querier.as_ref(), interval);

    // Collect the iterator for the series.
    let series = collect_chunk_series(series_set);
    assert_eq!(1, series.len());
    let mut iterator = series[0].iterator(None);

    // Start OOO head compaction.
    let (compaction_complete, compaction_err) = start_ooo_compaction(&db);

    // Give CompactOOOHead time to start work.
    std::thread::sleep(Duration::from_secs(1));
    assert!(!compaction_complete.load(Ordering::SeqCst), "compaction completed before reading chunks or closing querier");

    // Check that we can still successfully read all samples.
    assert_eq!(samples_written, count_chunk_samples(&mut iterator));

    assert!(!compaction_complete.load(Ordering::SeqCst), "compaction completed before closing querier");
    drop(iterator);
    assert!(querier.close().is_none());
    eventually(
        || compaction_complete.load(Ordering::SeqCst),
        Duration::from_secs(60),
        Duration::from_millis(10),
        "compaction should complete after querier was closed",
    );
    compaction_err.recv().unwrap().unwrap();
    assert_eq!(1.0, db.head().metrics.chunks_removed.get());
}

// ---------------------------------------------------------------------------------------------------------------
// TestOOOWALWrite.

use tsdb_format::chunks::ChunkDiskMapperRef;
use tsdb_format::record::{
    self, Decoder, RefFloatHistogramSample, RefHistogramSample, RefMetadata, RefMmapMarker, RefSample, RefSeries,
};

/// A decoded WAL record (Go `[]any` of record slices).
#[derive(Debug, PartialEq)]
enum Rec {
    Series(Vec<RefSeries>),
    Samples(Vec<RefSample>),
    MmapMarkers(Vec<RefMmapMarker>),
    Histograms(Vec<RefHistogramSample>),
    FloatHistograms(Vec<RefFloatHistogramSample>),
}

fn minutes(m: i64) -> i64 {
    m * 60 * 1000
}

/// The sample kind of a TestOOOWALWrite scenario.
#[derive(Clone, Copy)]
enum WalKind {
    Float,
    H(fn(i64) -> Histogram),
    Fh(fn(i64) -> FloatHistogram),
}

impl WalKind {
    /// A samples record of (ref, minutes) pairs.
    fn rec(self, s: &[(u64, i64)]) -> Rec {
        match self {
            WalKind::Float => Rec::Samples(
                s.iter().map(|&(r, m)| RefSample { r#ref: r, st: 0, t: minutes(m), v: m as f64 }).collect(),
            ),
            WalKind::H(g) => Rec::Histograms(
                s.iter().map(|&(r, m)| RefHistogramSample { r#ref: r, st: 0, t: minutes(m), h: Arc::new(g(m)) }).collect(),
            ),
            WalKind::Fh(g) => Rec::FloatHistograms(
                s.iter()
                    .map(|&(r, m)| RefFloatHistogramSample { r#ref: r, st: 0, t: minutes(m), fh: Arc::new(g(m)) })
                    .collect(),
            ),
        }
    }

    fn append(self, app: &mut dyn Appender, l: &Labels, mins: i64) {
        match self {
            WalKind::Float => app.append(0, l, minutes(mins), mins as f64),
            WalKind::H(g) => app.append_histogram(0, l, minutes(mins), Some(&g(mins)), None),
            WalKind::Fh(g) => app.append_histogram(0, l, minutes(mins), None, Some(&g(mins))),
        }
        .unwrap();
    }
}

fn marker(r: u64, mmap_ref: u64) -> Rec {
    Rec::MmapMarkers(vec![RefMmapMarker { r#ref: r, mmap_ref: ChunkDiskMapperRef(mmap_ref) }])
}

#[test]
fn test_ooowal_write() {
    use storage::tsdbutil as tu;
    const B: u64 = 0x100000000;
    // (name, kind, the three m-map refs (beyond the first one) taken from the Go test run).
    let scenarios: [(&str, WalKind, [u64; 3]); 5] = [
        ("float", WalKind::Float, [58, 107, 156]),
        ("integer histogram", WalKind::H(tu::generate_test_histogram), [89, 172, 257]),
        ("float histogram", WalKind::Fh(tu::generate_test_float_histogram), [177, 348, 521]),
        ("custom buckets histogram", WalKind::H(tu::generate_test_custom_buckets_histogram), [82, 160, 239]),
        (
            "custom buckets float histogram",
            WalKind::Fh(tu::generate_test_custom_buckets_float_histogram),
            [134, 263, 393],
        ),
    ];
    let s1 = ls(&["l", "v1"]);
    let s2 = ls(&["l", "v2"]);
    for (name, k, o) in scenarios {
        // The MmapRef in these are not hand calculated, and instead taken from the test run. What is important here
        // is the order of records, and that MmapRef increases for each record.
        let expected_ooo_records = vec![
            marker(1, 0),
            k.rec(&[(1, 40)]),
            marker(2, 0),
            k.rec(&[(2, 42)]),
            k.rec(&[(2, 45), (1, 35)]),
            marker(1, B + 8), // 3rd sample, hence m-mapped.
            k.rec(&[(1, 36), (1, 37)]),
            marker(1, B + o[0]), // 3rd sample, hence m-mapped.
            k.rec(&[(1, 50)]),   // Does not contain the in-order sample here.
            // Single commit but multiple OOO records.
            marker(2, B + o[1]),
            k.rec(&[(2, 50), (2, 51)]),
            marker(2, B + o[2]),
            k.rec(&[(2, 52), (2, 53)]),
        ];
        let expected_in_o_records = vec![
            Rec::Series(vec![RefSeries { r#ref: 1, labels: s1.clone() }, RefSeries { r#ref: 2, labels: s2.clone() }]),
            k.rec(&[(1, 60), (2, 60)]),
            k.rec(&[(1, 40)]),
            k.rec(&[(2, 42)]),
            k.rec(&[(2, 45), (1, 35), (1, 36), (1, 37)]),
            k.rec(&[(1, 50), (2, 65)]), // Contains both in-order and ooo sample.
            k.rec(&[(2, 50), (2, 51), (2, 52), (2, 53)]),
        ];
        ooowal_write(name, k, expected_ooo_records, expected_in_o_records);
    }
}

/// Go `getRecords` of testOOOWALWrite: fails on records that are not series, samples or m-map markers.
fn get_wal_records(wal_dir: &Path, name: &str) -> Vec<Rec> {
    let sr = wlog::new_segments_reader(wal_dir).unwrap();
    let mut r = wlog::Reader::new(sr);
    let mut dec = Decoder::new(tsdb::Logger::nop());
    let mut records = Vec::new();
    while r.next() {
        let rec = r.record().to_vec();
        match dec.r#type(&rec) {
            record::Type::Series => {
                let mut v = Vec::new();
                dec.series(&rec, &mut v).unwrap();
                records.push(Rec::Series(v));
            }
            record::Type::Samples | record::Type::SamplesV2 => {
                let mut v = Vec::new();
                dec.samples(&rec, &mut v).unwrap();
                records.push(Rec::Samples(v));
            }
            record::Type::MmapMarkers => {
                let mut v = Vec::new();
                dec.mmap_markers(&rec, &mut v).unwrap();
                records.push(Rec::MmapMarkers(v));
            }
            record::Type::HistogramSamples
            | record::Type::CustomBucketsHistogramSamples
            | record::Type::HistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.histogram_samples(&rec, &mut v).unwrap();
                records.push(Rec::Histograms(v));
            }
            record::Type::FloatHistogramSamples
            | record::Type::CustomBucketsFloatHistogramSamples
            | record::Type::FloatHistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.float_histogram_samples(&rec, &mut v).unwrap();
                records.push(Rec::FloatHistograms(v));
            }
            typ => panic!("{name}: got a WAL record that is not series or samples: {typ:?}"),
        }
    }
    assert!(r.err().is_none(), "{:?}", r.err());
    r.into_inner().close().unwrap();
    records
}

fn ooowal_write(name: &str, k: WalKind, expected_ooo_records: Vec<Rec>, expected_in_o_records: Vec<Rec>) {
    let mut opts = default_options();
    opts.out_of_order_cap_max = 2;
    opts.out_of_order_time_window = minutes(30);
    let db = new_test_db(vec![with_opts(opts)]);

    let (s1, s2) = (ls(&["l", "v1"]), ls(&["l", "v2"]));
    let commit = |samples: &[(&Labels, i64)]| {
        let mut app = db.appender(&ctx());
        for (l, m) in samples {
            k.append(app.as_mut(), l, *m);
        }
        assert!(app.commit().is_none());
    };

    // Ingest sample at 1h.
    commit(&[(&s1, 60), (&s2, 60)]);
    // OOO for s1.
    commit(&[(&s1, 40)]);
    // OOO for s2.
    commit(&[(&s2, 42)]);
    // OOO for both s1 and s2 in the same commit (36 m-maps).
    commit(&[(&s2, 45), (&s1, 35), (&s1, 36), (&s1, 37)]);
    // OOO for s1 but not for s2 in the same commit (50 m-maps).
    commit(&[(&s1, 50), (&s2, 65)]);
    // Single commit has 2 times m-mapping and more samples after m-map (50 and 52 m-map).
    commit(&[(&s2, 50), (&s2, 51), (&s2, 52), (&s2, 53)]);

    // The normal WAL.
    assert_eq!(expected_in_o_records, get_wal_records(&db.dir().join("wal"), name), "{name}");
    // The WBL.
    assert_eq!(expected_ooo_records, get_wal_records(&db.dir().join(wlog::WBL_DIR_NAME), name), "{name}");
}

/// Tests https://github.com/prometheus/prometheus/issues/10291#issuecomment-1044373110.
#[test]
fn test_db_panic_on_mmapping_head_chunk() {
    let dir = TempDir::new();
    let db = new_test_db(vec![with_dir(dir.path())]);
    db.disable_compactions();

    // Choosing scrape interval of 45s to have chunk larger than 1h.
    let itvl = 45 * 1000i64;

    let mut last_ts = 0i64;
    let mut add_samples = |db: &DB, num_samples: i64| {
        let mut app = db.appender(&ctx());
        let mut r: SeriesRef = 0;
        let lbls = ls(&["__name__", "testing", "foo", "bar"]);
        for i in 0..num_samples {
            r = app.append(r, &lbls, last_ts, last_ts as f64).unwrap();
            last_ts += itvl;
            if i % 10 == 0 {
                assert!(app.commit().is_none());
                app = db.appender(&ctx());
            }
        }
        assert!(app.commit().is_none());
    };

    // Ingest samples up to 2h50m to make the head "about to compact".
    add_samples(&db, 170 * 60 * 1000 / itvl);

    assert!(db.blocks().is_empty());
    db.compact(&ctx()).unwrap();
    assert!(db.blocks().is_empty());

    // Restarting.
    db.close().unwrap();
    drop(db);

    let db = new_test_db(vec![with_dir(dir.path())]);
    db.disable_compactions();

    // Ingest samples up to 20m more to make the head compact.
    add_samples(&db, 20 * 60 * 1000 / itvl);

    assert!(db.blocks().is_empty());
    db.compact(&ctx()).unwrap();
    assert_eq!(1, db.blocks().len());

    // More samples to m-map and panic.
    add_samples(&db, 120 * 60 * 1000 / itvl);

    db.close().unwrap();
}

// ---------------------------------------------------------------------------------------------------------------
// Metadata.

use model::metadata::{Metadata, MetricType};

fn md(typ: MetricType, unit: &str, help: &str) -> Metadata {
    Metadata { typ, unit: unit.to_string(), help: help.to_string() }
}

fn ref_md(r: u64, m: &Metadata) -> RefMetadata {
    RefMetadata { r#ref: r, r#type: record::get_metric_type(m.typ), unit: m.unit.clone(), help: m.help.clone() }
}

/// Go `readTestWAL` (head_test.go), keeping only what the metadata tests read.
fn read_test_wal_metadata(dir: &Path) -> Vec<Vec<RefMetadata>> {
    let sr = wlog::new_segments_reader(dir).expect("segments reader");
    let dec = Decoder::new(tsdb::Logger::nop());
    let mut r = wlog::Reader::new(sr);
    let mut out = Vec::new();
    while r.next() {
        let rec = r.record().to_vec();
        if dec.r#type(&rec) == record::Type::Metadata {
            let mut v = Vec::new();
            dec.metadata(&rec, &mut v).expect("metadata");
            out.push(v);
        }
    }
    assert!(r.err().is_none(), "{:?}", r.err());
    out
}

fn update_metadata(app: &mut dyn Appender, s: &Labels, m: &Metadata) {
    app.update_metadata(0, s, m).unwrap();
}

#[test]
fn test_metadata_in_wal() {
    let db = new_test_db(vec![]);

    // Add some series so we can append metadata to them.
    let mut app = db.appender(&ctx());
    let (s1, s2, s3, s4) = (ls(&["a", "b"]), ls(&["c", "d"]), ls(&["e", "f"]), ls(&["g", "h"]));
    for s in [&s1, &s2, &s3, &s4] {
        app.append(0, s, 0, 0.0).unwrap();
    }
    assert!(app.commit().is_none());

    // Add a first round of metadata to the first three series.
    let m1 = md(MetricType::Gauge, "unit_1", "help_1");
    let m2 = md(MetricType::Gauge, "unit_2", "help_2");
    let m3 = md(MetricType::Gauge, "unit_3", "help_3");
    let mut app = db.appender(&ctx());
    update_metadata(app.as_mut(), &s1, &m1);
    update_metadata(app.as_mut(), &s2, &m2);
    update_metadata(app.as_mut(), &s3, &m3);
    assert!(app.commit().is_none());

    // Add a replicated metadata entry to the first series, a completely new metadata entry for the fourth series,
    // and a changed metadata entry to the second series.
    let m4 = md(MetricType::Counter, "unit_4", "help_4");
    let m5 = md(MetricType::Counter, "unit_5", "help_5");
    let mut app = db.appender(&ctx());
    update_metadata(app.as_mut(), &s1, &m1);
    update_metadata(app.as_mut(), &s4, &m4);
    update_metadata(app.as_mut(), &s2, &m5);
    assert!(app.commit().is_none());

    // Read the WAL to see if the disk storage format is correct.
    let got_metadata_blocks = read_test_wal_metadata(&db.dir().join("wal"));
    let expected_metadata = [ref_md(1, &m1), ref_md(2, &m2), ref_md(3, &m3), ref_md(4, &m4), ref_md(2, &m5)];
    assert_eq!(2, got_metadata_blocks.len());
    assert_eq!(&expected_metadata[..3], &got_metadata_blocks[0][..]);
    assert_eq!(&expected_metadata[3..], &got_metadata_blocks[1][..]);
}

#[test]
fn test_metadata_checkpointing_only_keeps_latest_entry() {
    for enable_st_storage in [false, true] {
        let num_samples = 10000i64;
        let hb = new_test_head("metadata-checkpoint", num_samples * 10, compression::Type::None, false);
        let w = hb.wal.clone().unwrap();

        // Add some series so we can append metadata to them.
        let mut app = hb.appender(&ctx());
        let (s1, s2, s3, s4) = (ls(&["a", "b"]), ls(&["c", "d"]), ls(&["e", "f"]), ls(&["g", "h"]));
        for s in [&s1, &s2, &s3, &s4] {
            app.append(0, s, 0, 0.0).unwrap();
        }
        assert!(app.commit().is_none());

        // Add a first round of metadata to the first three series.
        let m1 = md(MetricType::Gauge, "unit_1", "help_1");
        let m2 = md(MetricType::Gauge, "unit_2", "help_2");
        let m3 = md(MetricType::Gauge, "unit_3", "help_3");
        let m4 = md(MetricType::Gauge, "unit_4", "help_4");
        let mut app = hb.appender(&ctx());
        update_metadata(app.as_mut(), &s1, &m1);
        update_metadata(app.as_mut(), &s2, &m2);
        update_metadata(app.as_mut(), &s3, &m3);
        update_metadata(app.as_mut(), &s4, &m4);
        assert!(app.commit().is_none());

        // Update metadata for first series.
        let m5 = md(MetricType::Counter, "unit_5", "help_5");
        let mut app = hb.appender(&ctx());
        update_metadata(app.as_mut(), &s1, &m5);
        assert!(app.commit().is_none());

        // Switch back-and-forth metadata for second series. Since it ended on a new metadata record, we expect a
        // single new entry.
        let m6 = md(MetricType::Counter, "unit_6", "help_6");
        for m in [&m6, &m2, &m6, &m2, &m6] {
            let mut app = hb.appender(&ctx());
            update_metadata(app.as_mut(), &s2, m);
            assert!(app.commit().is_none());
        }

        // Let's create a checkpoint.
        let (first, last) = wlog::segments(w.dir()).unwrap();
        let keep = |id: u64| id != 3;
        wlog::checkpoint(&w, first, last - 1, &keep, 0, enable_st_storage, false).unwrap();

        // Confirm there's been a checkpoint.
        let (cdir, _) = wlog::last_checkpoint(w.dir()).unwrap();

        // Read in checkpoint and WAL.
        let got_metadata_blocks = read_test_wal_metadata(&cdir);

        // There should only be 1 metadata block present, with only the latest metadata kept around.
        let want_metadata = vec![ref_md(1, &m5), ref_md(2, &m6), ref_md(4, &m4)];
        assert_eq!(1, got_metadata_blocks.len(), "enableSTStorage={enable_st_storage}");
        assert_eq!(3, got_metadata_blocks[0].len(), "enableSTStorage={enable_st_storage}");
        let mut got_metadata_block = got_metadata_blocks[0].clone();
        got_metadata_block.sort_by_key(|m| m.r#ref);
        assert_eq!(want_metadata, got_metadata_block, "enableSTStorage={enable_st_storage}");
        hb.close().unwrap();
    }
}

fn series_meta(db: &DB, s: &Labels) -> Option<Metadata> {
    let ms = db.head().series.get_by_hash(s.hash(), s).expect("series");
    let inner = ms.lock();
    inner.meta().filter(|h| !model::metadata::is_zero_handle(h)).map(|h| h.value().clone())
}

#[test]
fn test_metadata_assert_in_memory_data() {
    let dir = TempDir::new();
    let db = new_test_db(vec![with_dir(dir.path())]);

    // Add some series so we can append metadata to them.
    let mut app = db.appender(&ctx());
    let (s1, s2, s3, s4) = (ls(&["a", "b"]), ls(&["c", "d"]), ls(&["e", "f"]), ls(&["g", "h"]));
    for s in [&s1, &s2, &s3, &s4] {
        app.append(0, s, 0, 0.0).unwrap();
    }
    assert!(app.commit().is_none());

    // Add a first round of metadata to the first three series. The in-memory data held in the db Head should hold
    // the metadata.
    let m1 = md(MetricType::Gauge, "unit_1", "help_1");
    let m2 = md(MetricType::Gauge, "unit_2", "help_2");
    let m3 = md(MetricType::Gauge, "unit_3", "help_3");
    let mut app = db.appender(&ctx());
    update_metadata(app.as_mut(), &s1, &m1);
    update_metadata(app.as_mut(), &s2, &m2);
    update_metadata(app.as_mut(), &s3, &m3);
    assert!(app.commit().is_none());

    assert_eq!(Some(m1.clone()), series_meta(&db, &s1));
    assert_eq!(Some(m2.clone()), series_meta(&db, &s2));
    assert_eq!(Some(m3.clone()), series_meta(&db, &s3));
    // Go `metadata.IsZeroHandle(series4.meta)`.
    assert_eq!(None, series_meta(&db, &s4));

    // Add a replicated metadata entry to the first series, a changed metadata entry to the second series, and a
    // completely new metadata entry for the fourth series. The in-memory data held in the db Head should be
    // correctly updated.
    let m4 = md(MetricType::Counter, "unit_4", "help_4");
    let m5 = md(MetricType::Counter, "unit_5", "help_5");
    let mut app = db.appender(&ctx());
    update_metadata(app.as_mut(), &s1, &m1);
    update_metadata(app.as_mut(), &s4, &m4);
    update_metadata(app.as_mut(), &s2, &m5);
    assert!(app.commit().is_none());

    assert_eq!(Some(m1.clone()), series_meta(&db, &s1));
    assert_eq!(Some(m5.clone()), series_meta(&db, &s2));
    assert_eq!(Some(m3.clone()), series_meta(&db, &s3));
    assert_eq!(Some(m4.clone()), series_meta(&db, &s4));

    db.close().unwrap();
    drop(db);

    // Reopen the DB, replaying the WAL. The Head must have been replayed correctly in memory.
    let db = new_test_db(vec![with_dir(dir.path())]);
    wal_of(&db).size().unwrap();

    assert_eq!(Some(m1), series_meta(&db, &s1));
    assert_eq!(Some(m5), series_meta(&db, &s2));
    assert_eq!(Some(m3), series_meta(&db, &s3));
    assert_eq!(Some(m4), series_meta(&db, &s4));
}

// ---------------------------------------------------------------------------------------------------------------
// Sample helpers (testutil.go).

/// Go `requireEqualOOOSamples`.
fn require_equal_ooo_samples(expected_samples: usize, db: &DB) {
    let m = &db.head().metrics.out_of_order_samples_appended;
    assert_eq!(
        expected_samples as f64,
        m.with_label_values("float").get() + m.with_label_values("histogram").get(),
        "number of ooo appended samples mismatch"
    );
}

/// Go `requireEqualSamples` (testutil.go).
fn require_equal_samples(name: &str, expected: &[Sample], actual: &[Sample], ignore_counter_resets: bool) {
    use model::histogram::CounterResetHint;
    assert_eq!(expected.len(), actual.len(), "Length not equal to expected for {name}");
    for (i, (e, a)) in expected.iter().zip(actual).enumerate() {
        assert_eq!(e.t, a.t, "Different timestamps for {name}[{i}]");
        assert_eq!(e.typ(), a.typ(), "Different types for {name}[{i}] at ts {}", e.t);
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
        require_equal_samples(name, e, a, ignore_counter_resets);
    }
    for name in actual.keys() {
        assert!(expected.contains_key(name), "Unexpected series {name}");
    }
}

/// Go `queryChunks` (db_test.go). Closes the querier.
fn query_chunks(mut q: storage::BoxChunkQuerier, matchers: &[Matcher]) -> BTreeMap<String, Vec<storage::Meta>> {
    let mut result = BTreeMap::new();
    {
        let mut ss = q.select(&ctx(), false, None, matchers);
        while ss.next() {
            let series = ss.at().clone();
            let mut chks = Vec::new();
            let mut it = series.iterator(None);
            while it.next() {
                chks.push(it.at().clone());
            }
            assert!(it.err().is_none());
            if chks.is_empty() {
                continue;
            }
            result.insert(series.labels().to_string(), chks);
        }
        assert!(ss.err().is_none());
        assert!(ss.warnings().is_empty());
    }
    assert!(q.close().is_none());
    result
}

/// TestMultipleEncodingsCommitOrder mainly serves to demonstrate when happens when committing a batch of samples
/// for the same series when there are multiple encodings.
#[test]
fn test_multiple_encodings_commit_order() {
    let mut opts = default_options();
    opts.out_of_order_cap_max = 30;
    opts.out_of_order_time_window = 24 * 3600 * 1000;

    let db = new_test_db(vec![with_opts(opts)]);
    db.disable_compactions();

    let series1 = ls(&["foo", "bar1"]);
    let add_sample = |app: &mut dyn Appender, ts: i64, val_type: ValueType| -> Sample {
        match val_type {
            ValueType::Float => {
                app.append(0, &ls(&["foo", "bar1"]), ts, ts as f64).unwrap();
                Sample::float(0, ts, ts as f64)
            }
            ValueType::Histogram => {
                let h = storage::tsdbutil::generate_test_histogram(ts);
                app.append_histogram(0, &ls(&["foo", "bar1"]), ts, Some(&h), None).unwrap();
                Sample::histogram(0, ts, h)
            }
            _ => {
                let fh = storage::tsdbutil::generate_test_float_histogram(ts);
                app.append_histogram(0, &ls(&["foo", "bar1"]), ts, None, Some(&fh)).unwrap();
                Sample::float_histogram(0, ts, fh)
            }
        }
    };

    let verify_samples = |min_t: i64, max_t: i64, exp_samples: &[Sample], ooo_count: usize| {
        require_equal_ooo_samples(ooo_count, &db);

        // Verify samples querier.
        let querier = db.querier(min_t, max_t).unwrap();
        let series_set = query(querier, &[eq("foo", "bar1")]);
        assert_eq!(1, series_set.len());
        let got_samples = &series_set[&series1.to_string()];
        require_equal_samples(&series1.to_string(), exp_samples, got_samples, true);

        // Verify chunks querier.
        let chunk_querier = db.chunk_querier(min_t, max_t).unwrap();
        let chks = query_chunks(chunk_querier, &[eq("foo", "bar1")]);
        assert!(chks.contains_key(&series1.to_string()));
        assert_eq!(1, chks.len());
        let mut got_chunk_samples = Vec::new();
        for chunk in &chks[&series1.to_string()] {
            let c = chunk.chunk.as_ref().unwrap();
            let mut it = c.iterator(None);
            got_chunk_samples.extend(storage::expand_samples(&mut it).unwrap());
            assert!(it.err().is_none());
        }
        require_equal_samples(&series1.to_string(), exp_samples, &got_chunk_samples, true);
    };

    let mut exp_samples = Vec::new();

    // Append samples with different encoding types and then commit them at once.
    let mut app = db.appender(&ctx());
    for i in 100..105 {
        exp_samples.push(add_sample(app.as_mut(), i, ValueType::Float));
    }
    for i in 110..120 {
        exp_samples.push(add_sample(app.as_mut(), i, ValueType::Histogram));
    }
    for i in 120..130 {
        exp_samples.push(add_sample(app.as_mut(), i, ValueType::FloatHistogram));
    }
    for i in 140..150 {
        exp_samples.push(add_sample(app.as_mut(), i, ValueType::FloatHistogram));
    }
    // These samples will be marked as out-of-order.
    for i in 130..135 {
        exp_samples.push(add_sample(app.as_mut(), i, ValueType::Float));
    }
    assert!(app.commit().is_none());

    exp_samples.sort_by_key(|s| s.t);

    // oooCount = 5 for the samples 130 to 134.
    verify_samples(100, 150, &exp_samples, 5);

    // Append and commit some in-order histograms by themselves.
    let mut app = db.appender(&ctx());
    for i in 150..160 {
        exp_samples.push(add_sample(app.as_mut(), i, ValueType::Histogram));
    }
    assert!(app.commit().is_none());

    // oooCount remains at 5.
    verify_samples(100, 160, &exp_samples, 5);

    // Append and commit samples for all encoding types. This time all samples will be treated as OOO because
    // samples with newer timestamps have already been committed.
    let mut app = db.appender(&ctx());
    for i in 50..55 {
        exp_samples.push(add_sample(app.as_mut(), i, ValueType::Float));
    }
    for i in 60..70 {
        exp_samples.push(add_sample(app.as_mut(), i, ValueType::Histogram));
    }
    for i in 70..75 {
        exp_samples.push(add_sample(app.as_mut(), i, ValueType::Float));
    }
    for i in 80..90 {
        exp_samples.push(add_sample(app.as_mut(), i, ValueType::FloatHistogram));
    }
    assert!(app.commit().is_none());

    // Sort samples again because OOO samples have been added.
    exp_samples.sort_by_key(|s| s.t);

    // oooCount = 35 as we've added 30 more OOO samples.
    verify_samples(50, 160, &exp_samples, 35);
}

// ---------------------------------------------------------------------------------------------------------------
// OOO compaction.

/// Adds the samples of the OOO compaction tests: for every minute in [from_mins, to_mins], series1 gets value ts
/// and series2 gets 2*ts.
fn ooo_add_samples(db: &DB, scenario: &SampleTypeScenario, from_mins: i64, to_mins: i64) {
    let (series1, series2) = (ls(&["foo", "bar1"]), ls(&["foo", "bar2"]));
    let mut app = db.appender(&ctx());
    for m in from_mins..=to_mins {
        let ts = minutes(m);
        scenario.append(app.as_mut(), &series1, ts, ts).0.unwrap();
        scenario.append(app.as_mut(), &series2, ts, 2 * ts).0.unwrap();
    }
    assert!(app.commit().is_none());
}

/// The expected series of the OOO compaction tests for the given minute ranges.
fn ooo_expected(scenario: &SampleTypeScenario, ranges: &[(i64, i64)]) -> BTreeMap<String, Vec<Sample>> {
    let (mut s1, mut s2) = (Vec::new(), Vec::new());
    for &(from_mins, to_mins) in ranges {
        for m in from_mins..=to_mins {
            let ts = minutes(m);
            s1.push((scenario.sample_func)(ts, ts));
            s2.push((scenario.sample_func)(ts, 2 * ts));
        }
    }
    BTreeMap::from([(ls(&["foo", "bar1"]).to_string(), s1), (ls(&["foo", "bar2"]).to_string(), s2)])
}

fn foo_bar_regexp() -> Matcher {
    Matcher::must_new(MatchType::Regexp, "foo", "bar.*")
}

/// Go `verifySamples` of the OOO compaction tests: checks the data of one block.
fn ooo_verify_block(scenario: &SampleTypeScenario, block: &tsdb::block::Block, from_mins: i64, to_mins: i64) {
    let exp_res = ooo_expected(scenario, &[(from_mins, to_mins)]);
    let q = tsdb::querier::new_block_querier(block, i64::MIN, i64::MAX).unwrap();
    let act_res = query(q, &[foo_bar_regexp()]);
    require_equal_series(&exp_res, &act_res, true);
}

/// The OOO fields of a series: (has OOO fields, OOO head chunk samples, OOO m-mapped chunks).
fn ooo_state(db: &DB, lbls: &Labels) -> (bool, Option<usize>, usize) {
    let (ms, created) = db.head().get_or_create(lbls.hash(), lbls, false).unwrap();
    assert!(!created);
    let s = ms.lock();
    match s.ooo() {
        None => (false, None, 0),
        Some(o) => (true, o.ooo_head_chunk.as_ref().map(|c| c.chunk.num_samples()), o.ooo_mmapped_chunks.len()),
    }
}

fn wbl_files(db: &DB) -> Vec<(String, u64)> {
    let wbl = db.head().wbl().expect("wbl");
    let mut files: Vec<(String, u64)> = std::fs::read_dir(wbl.dir())
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (e.file_name().to_string_lossy().into_owned(), e.metadata().unwrap().len())
        })
        .collect();
    files.sort();
    files
}

fn dir_names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> =
        std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect();
    names.sort();
    names
}

// TODO(codesome): test more samples incoming once compaction has started. To verify new samples after the start
// are not included in this compaction.
#[test]
fn test_ooo_compaction() {
    for (name, scenario) in sample_type_scenarios() {
        ooo_compaction(name, scenario, false);
        ooo_compaction(&format!("{name}+extra"), scenario, true);
    }
}

fn ooo_compaction(name: &str, scenario: SampleTypeScenario, add_extra_samples: bool) {
    let mut opts = default_options();
    opts.out_of_order_cap_max = 30;
    opts.out_of_order_time_window = minutes(300);
    let db = new_test_db(vec![with_opts(opts)]);
    db.disable_compactions(); // We want to manually call it.

    let (series1, series2) = (ls(&["foo", "bar1"]), ls(&["foo", "bar2"]));

    // Add an in-order samples.
    ooo_add_samples(&db, &scenario, 250, 300);

    // Verify that the in-memory ooo chunk is empty.
    let check_empty_ooo_chunk = |l: &Labels| assert!(!ooo_state(&db, l).0, "{name}");
    check_empty_ooo_chunk(&series1);
    check_empty_ooo_chunk(&series2);

    // Add ooo samples that creates multiple chunks. 90 to 300 spans across 3 block ranges: [0, 120), [120, 240),
    // [240, 360).
    ooo_add_samples(&db, &scenario, 90, 300);
    // Adding same samples to create overlapping chunks. Since the active chunk won't start at 90 again, all the
    // new chunks will have different time ranges than the previous chunks.
    ooo_add_samples(&db, &scenario, 90, 300);

    let highest = Arc::new(std::sync::atomic::AtomicI64::new(300));

    let verify_db_samples = || {
        let h = highest.load(Ordering::SeqCst);
        let exp_res = ooo_expected(&scenario, &[(90, 119), (120, 239), (240, h)]);
        let q = db.querier(i64::MIN, i64::MAX).unwrap();
        let act_res = query(q, &[foo_bar_regexp()]);
        require_equal_series(&exp_res, &act_res, true);
    };

    verify_db_samples(); // Before any compaction.

    // Verify that the in-memory ooo chunk is not empty.
    for l in [&series1, &series2] {
        let (_, head_samples, mmapped) = ooo_state(&db, l);
        assert!(head_samples.unwrap() > 0, "{name}");
        assert_eq!(13, mmapped, "{name}"); // 7 original, 6 duplicate.
    }

    // No blocks before compaction.
    assert!(db.blocks().is_empty(), "{name}");

    // There is a 0th WBL file.
    db.head().wbl().unwrap().sync().unwrap();
    let files = wbl_files(&db);
    assert_eq!(1, files.len(), "{name}");
    assert_eq!("00000000", files[0].0);
    assert!(files[0].1 > 100, "{name}");

    if add_extra_samples {
        let db2 = db.db.clone();
        *db.compact_ooo_head_testing_callback.lock().unwrap() = Some(Box::new(move || {
            ooo_add_samples(&db2, &scenario, 90, 120); // Back in time, to generate a new OOO chunk.
            ooo_add_samples(&db2, &scenario, 300, 330); // Now some samples after the previous highest timestamp.
            ooo_add_samples(&db2, &scenario, 300, 330); // Repeat to generate an OOO chunk at these timestamps.
        }));
        highest.store(330, Ordering::SeqCst);
    }

    // OOO compaction happens here.
    db.compact_ooo_head(&ctx()).unwrap();

    // 3 blocks exist now. [0, 120), [120, 240), [240, 360)
    assert_eq!(3, db.blocks().len(), "{name}");

    verify_db_samples(); // Blocks created out of OOO head now.

    // 0th WBL file will be deleted and 1st will be the only present.
    let files = wbl_files(&db);
    assert_eq!(1, files.len(), "{name}");
    assert_eq!("00000001", files[0].0);

    if !add_extra_samples {
        assert_eq!(0, files[0].1, "{name}");
        // OOO stuff should not be present in the Head now.
        check_empty_ooo_chunk(&series1);
        check_empty_ooo_chunk(&series2);
    }

    // Checking for expected data in the blocks.
    let blocks = db.blocks();
    ooo_verify_block(&scenario, &blocks[0], 90, 119);
    ooo_verify_block(&scenario, &blocks[1], 120, 239);
    ooo_verify_block(&scenario, &blocks[2], 240, 299);
    drop(blocks);

    // There should be a single m-map file.
    let mmap_dir = tsdb::head::mmapped_chunks_dir(&db.head().opts.chunk_dir_root);
    assert_eq!(1, dir_names(&mmap_dir).len(), "{name}");

    // Compact the in-order head and expect another block. Since this is a forced compaction, this block is not
    // aligned with 2h.
    db.compact_head(&RangeHead::new(db.head().clone(), minutes(250), minutes(350))).unwrap();
    let blocks = db.blocks();
    assert_eq!(4, blocks.len(), "{name}"); // [0, 120), [120, 240), [240, 360), [250, 351)
    let highest_v = highest.load(Ordering::SeqCst);
    ooo_verify_block(&scenario, &blocks[3], 250, highest_v);
    drop(blocks);

    verify_db_samples(); // Blocks created out of normal and OOO head now. But not merged.

    // The compaction also clears out the old m-map files. Including the file that has ooo chunks.
    assert_eq!(vec!["000001".to_string()], dir_names(&mmap_dir), "{name}");

    // This will merge overlapping block.
    db.compact(&ctx()).unwrap();

    let blocks = db.blocks();
    assert_eq!(3, blocks.len(), "{name}"); // [0, 120), [120, 240), [240, 360)
    ooo_verify_block(&scenario, &blocks[0], 90, 119);
    ooo_verify_block(&scenario, &blocks[1], 120, 239);
    ooo_verify_block(&scenario, &blocks[2], 240, highest_v); // Merged block.
    drop(blocks);

    verify_db_samples(); // Final state. Blocks from normal and OOO head are merged.
}

/// TestOOOCompactionWithNormalCompaction tests if OOO compaction is performed when the normal head's compaction
/// is done.
#[test]
fn test_ooo_compaction_with_normal_compaction() {
    for (name, scenario) in sample_type_scenarios() {
        ooo_compaction_with_normal_compaction(name, scenario, false);
    }
}

/// TestOOOCompactionWithDisabledWriteLog tests the scenario where the TSDB is configured to not have wal and wbl
/// but its able to compact both the in-order and out-of-order head.
#[test]
fn test_ooo_compaction_with_disabled_write_log() {
    for (name, scenario) in sample_type_scenarios() {
        ooo_compaction_with_normal_compaction(name, scenario, true);
    }
}

/// Go `testOOOCompactionWithNormalCompaction` and `testOOOCompactionWithDisabledWriteLog` (the same test with the
/// WAL and WBL disabled).
fn ooo_compaction_with_normal_compaction(name: &str, scenario: SampleTypeScenario, disable_wal: bool) {
    let mut opts = default_options();
    opts.out_of_order_cap_max = 30;
    opts.out_of_order_time_window = minutes(300);
    if disable_wal {
        opts.wal_segment_size = -1; // disabled WAL and WBL
    }

    let db = new_test_db(vec![with_opts(opts)]);
    db.disable_compactions(); // We want to manually call it.

    let (series1, series2) = (ls(&["foo", "bar1"]), ls(&["foo", "bar2"]));

    // Add an in-order samples.
    ooo_add_samples(&db, &scenario, 250, 350);

    // Add ooo samples that will result into a single block.
    ooo_add_samples(&db, &scenario, 90, 110);

    // Checking that ooo chunk is not empty.
    for l in [&series1, &series2] {
        assert!(ooo_state(&db, l).1.unwrap() > 0, "{name}");
    }

    // If the normal Head is not compacted, the OOO head compaction does not take place.
    db.compact(&ctx()).unwrap();
    assert!(db.blocks().is_empty(), "{name}");

    // Add more in-order samples in future that would trigger the compaction.
    ooo_add_samples(&db, &scenario, 400, 450);

    // No blocks before compaction.
    assert!(db.blocks().is_empty(), "{name}");

    // Compacts normal and OOO head.
    db.compact(&ctx()).unwrap();

    // 2 blocks exist now. [0, 120), [250, 360)
    let blocks = db.blocks();
    assert_eq!(2, blocks.len(), "{name}");
    assert_eq!(0, blocks[0].min_time());
    assert_eq!(minutes(120), blocks[0].max_time());
    assert_eq!(minutes(250), blocks[1].min_time());
    assert_eq!(minutes(360), blocks[1].max_time());

    // Checking that ooo chunk is empty.
    for l in [&series1, &series2] {
        assert!(!ooo_state(&db, l).0, "{name}");
    }

    // Checking for expected data in the blocks.
    ooo_verify_block(&scenario, &blocks[0], 90, 110);
    ooo_verify_block(&scenario, &blocks[1], 250, 350);
}

/// TestOOOQueryAfterRestartWithSnapshotAndRemovedWBL tests the scenario where the WBL goes missing after a restart
/// while snapshot was enabled, but the query still returns the right data from the mmap chunks.
#[test]
fn test_ooo_query_after_restart_with_snapshot_and_removed_wbl() {
    for (name, scenario) in sample_type_scenarios() {
        ooo_query_after_restart_with_snapshot_and_removed_wbl(name, scenario);
    }
}

fn ooo_query_after_restart_with_snapshot_and_removed_wbl(name: &str, scenario: SampleTypeScenario) {
    let mut opts = default_options();
    opts.out_of_order_cap_max = 10;
    opts.out_of_order_time_window = minutes(300);
    opts.enable_memory_snapshot_on_shutdown = true;

    let dir = TempDir::new();
    let db = new_test_db(vec![with_dir(dir.path()), with_opts(opts)]);
    db.disable_compactions(); // We want to manually call it.

    let (series1, series2) = (ls(&["foo", "bar1"]), ls(&["foo", "bar2"]));

    // Add an in-order samples.
    ooo_add_samples(&db, &scenario, 250, 350);

    // Add ooo samples that will result into a single block.
    ooo_add_samples(&db, &scenario, 90, 110); // The sample 110 will not be in m-map chunks.

    // Checking that there are some ooo m-map chunks.
    for l in [&series1, &series2] {
        let (_, head, mmapped) = ooo_state(&db, l);
        assert_eq!(2, mmapped, "{name}");
        assert!(head.is_some(), "{name}");
    }

    // Restart DB.
    db.close().unwrap();
    drop(db);

    // For some reason wbl goes missing.
    std::fs::remove_dir_all(dir.path().join("wbl")).unwrap();

    let db = new_test_db(vec![with_dir(dir.path())]);
    db.disable_compactions(); // We want to manually call it.

    // Check ooo m-map chunks again.
    for l in [&series1, &series2] {
        let (ms, created) = db.head().get_or_create(l.hash(), l, false).unwrap();
        assert!(!created);
        let s = ms.lock();
        let o = s.ooo().expect("ooo");
        assert_eq!(2, o.ooo_mmapped_chunks.len(), "{name}");
        assert_eq!(minutes(109), o.ooo_mmapped_chunks[1].max_time, "{name}");
        assert!(o.ooo_head_chunk.is_none(), "{name}"); // Because of missing wbl.
    }

    let verify_samples = |from_mins: i64, to_mins: i64| {
        let exp_res = ooo_expected(&scenario, &[(from_mins, to_mins)]);
        let q = db.querier(minutes(from_mins), minutes(to_mins)).unwrap();
        let act_res = query(q, &[foo_bar_regexp()]);
        require_equal_series(&exp_res, &act_res, true);
    };

    // Checking for expected ooo data from mmap chunks.
    verify_samples(90, 109);

    // Compaction should also work fine.
    assert!(db.blocks().is_empty(), "{name}");
    db.compact_ooo_head(&ctx()).unwrap();
    let blocks = db.blocks();
    assert_eq!(1, blocks.len(), "{name}"); // One block from OOO data.
    assert_eq!(0, blocks[0].min_time());
    assert_eq!(minutes(120), blocks[0].max_time());
    drop(blocks);

    // Checking that ooo chunk is empty in Head.
    for l in [&series1, &series2] {
        assert!(!ooo_state(&db, l).0, "{name}");
    }

    verify_samples(90, 109);
}

// ---------------------------------------------------------------------------------------------------------------
// TestQuerierOOOQuery.

type OooAppendFunc = fn(&mut dyn Appender, i64, bool) -> Result<SeriesRef, Error>;
type OooSampleFunc = fn(i64) -> Sample;

#[test]
fn test_querier_ooo_query() {
    use model::histogram::CounterResetHint;
    use storage::tsdbutil as tu;
    let scenarios: [(&str, OooAppendFunc, OooSampleFunc); 4] = [
        (
            "float",
            |app, ts, _| app.append(0, &ls(&["foo", "bar1"]), ts, ts as f64),
            |ts| Sample::float(0, ts, ts as f64),
        ),
        (
            "integer histogram",
            |app, ts, counter_reset| {
                let mut h = tu::generate_test_histogram(ts);
                if counter_reset {
                    h.counter_reset_hint = CounterResetHint::CounterReset;
                }
                app.append_histogram(0, &ls(&["foo", "bar1"]), ts, Some(&h), None)
            },
            |ts| Sample::histogram(0, ts, tu::generate_test_histogram(ts)),
        ),
        (
            "float histogram",
            |app, ts, counter_reset| {
                let mut fh = tu::generate_test_float_histogram(ts);
                if counter_reset {
                    fh.counter_reset_hint = CounterResetHint::CounterReset;
                }
                app.append_histogram(0, &ls(&["foo", "bar1"]), ts, None, Some(&fh))
            },
            |ts| Sample::float_histogram(0, ts, tu::generate_test_float_histogram(ts)),
        ),
        (
            "integer histogram counter resets",
            // Adding counter reset to all histograms means each histogram will have its own chunk.
            |app, ts, _| {
                let mut h = tu::generate_test_histogram(ts);
                h.counter_reset_hint = CounterResetHint::CounterReset; // For this scenario, ignore the counterReset argument.
                app.append_histogram(0, &ls(&["foo", "bar1"]), ts, Some(&h), None)
            },
            |ts| Sample::histogram(0, ts, tu::generate_test_histogram(ts)),
        ),
    ];
    for (name, append_func, sample_func) in scenarios {
        querier_ooo_query(name, append_func, sample_func);
    }
}

/// A sample batch of testQuerierOOOQuery.
struct SampleBatch {
    min_t: i64,
    max_t: i64,
    filter: fn(i64) -> bool,
    counter_reset: bool,
    is_ooo: bool,
}

/// A test case of testQuerierOOOQuery.
struct OooQueryCase {
    name: &'static str,
    ooo_cap: i64,
    query_min_t: i64,
    query_max_t: i64,
    batches: Vec<SampleBatch>,
}

fn default_filter_func(_: i64) -> bool {
    true
}

fn even(t: i64) -> bool {
    t % 2 == 0
}

fn odd(t: i64) -> bool {
    t % 2 == 1
}

fn batch(min_t: i64, max_t: i64, filter: fn(i64) -> bool, is_ooo: bool) -> SampleBatch {
    SampleBatch { min_t, max_t, filter, counter_reset: false, is_ooo }
}

/// The test cases of testQuerierOOOQuery.
fn ooo_query_cases() -> Vec<OooQueryCase> {
    vec![
        OooQueryCase {
            name: "query interval covering ooomint and inordermaxt returns all ingested samples",
            ooo_cap: 30,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![
                batch(minutes(100), minutes(200), default_filter_func, false),
                batch(minutes(0), minutes(99), default_filter_func, true),
            ],
        },
        OooQueryCase {
            name: "partial query interval returns only samples within interval",
            ooo_cap: 30,
            query_min_t: minutes(20),
            query_max_t: minutes(180),
            batches: vec![
                batch(minutes(100), minutes(200), default_filter_func, false),
                batch(minutes(0), minutes(99), default_filter_func, true),
            ],
        },
        OooQueryCase {
            // In order: 100-200 normal. out of order first path: 0, 2, 4, ... 98 (no counter reset), second pass: 1,
            // 3, 5, ... 99 (with counter reset).
            name: "alternating OOO batches",
            ooo_cap: 0,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![
                batch(minutes(100), minutes(200), default_filter_func, false),
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
            name: "query overlapping inorder and ooo in-memory samples returns all ingested samples at the beginning of \
                   the interval",
            ooo_cap: 30,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![batch(minutes(100), minutes(200), even, false), batch(minutes(100), minutes(110), odd, true)],
        },
        OooQueryCase {
            name: "query inorder contain ooo mmapped samples returns all ingested samples at the beginning of the \
                   interval",
            ooo_cap: 5,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![
                batch(minutes(100), minutes(200), even, false),
                // Append samples to fit in a single mmapped OOO chunk and fit inside the first in-order mmapped chunk.
                batch(minutes(101), minutes(101 + (5 - 1) * 2), odd, true),
                // Append some more OOO samples to trigger mapping the OOO chunk, but use time 151 to not overlap with
                // in-order head chunk.
                batch(minutes(191), minutes(193), odd, true),
            ],
        },
        OooQueryCase {
            name: "query overlapping inorder and ooo mmapped samples returns all ingested samples at the beginning of \
                   the interval",
            ooo_cap: 30,
            query_min_t: minutes(0),
            query_max_t: minutes(200),
            batches: vec![
                batch(minutes(100), minutes(200), even, false),
                // Append samples to fit in a single mmapped OOO chunk and overlap the first in-order mmapped chunk.
                batch(minutes(101), minutes(101 + (30 - 1) * 2), odd, true),
                // Append some more OOO samples to trigger mapping the OOO chunk, but use time 151 to not overlap with
                // in-order head chunk.
                batch(minutes(191), minutes(193), odd, true),
            ],
        },
    ]
}

/// Go `addSample` of testQuerierOOOQuery.
#[allow(clippy::too_many_arguments)]
fn ooo_query_add_sample(
    db: &DB,
    append_func: OooAppendFunc,
    sample_func: OooSampleFunc,
    b: &SampleBatch,
    query_min_t: i64,
    query_max_t: i64,
    exp_samples: &mut Vec<Sample>,
) -> usize {
    let mut app = db.appender(&ctx());
    let mut total_appended = 0;
    let mut m = b.min_t;
    while m <= b.max_t {
        if (b.filter)(m / minutes(1)) {
            let r = append_func(app.as_mut(), m, b.counter_reset);
            if m >= query_min_t && m <= query_max_t {
                exp_samples.push(sample_func(m));
            }
            r.unwrap();
            total_appended += 1;
        }
        m += minutes(1);
    }
    assert!(app.commit().is_none());
    assert!(total_appended > 0); // Sanity check that filter is not too zealous.
    total_appended
}

fn querier_ooo_query(scenario: &str, append_func: OooAppendFunc, sample_func: OooSampleFunc) {
    let series1 = ls(&["foo", "bar1"]);
    for tc in ooo_query_cases() {
        let name = format!("{scenario}/name={}", tc.name);
        let mut opts = default_options();
        opts.out_of_order_time_window = 24 * 3600 * 1000;
        opts.out_of_order_cap_max = tc.ooo_cap;
        let db = new_test_db(vec![with_opts(opts)]);
        db.disable_compactions();

        let mut exp_samples = Vec::new();
        let mut ooo_samples = 0;
        for b in &tc.batches {
            let appended =
                ooo_query_add_sample(&db, append_func, sample_func, b, tc.query_min_t, tc.query_max_t, &mut exp_samples);
            if b.is_ooo {
                ooo_samples += appended;
            }
        }

        exp_samples.sort_by_key(|s| s.t);

        let querier = db.querier(tc.query_min_t, tc.query_max_t).unwrap();
        let series_set = query(querier, &[eq("foo", "bar1")]);
        let got_samples = series_set.get(&series1.to_string()).unwrap_or_else(|| panic!("{name}: no samples"));
        assert_eq!(1, series_set.len(), "{name}");
        require_equal_samples(&name, &exp_samples, got_samples, true);
        require_equal_ooo_samples(ooo_samples, &db);
    }
}
