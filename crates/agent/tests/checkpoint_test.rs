//! Port of `tsdb/agent/checkpoint_test.go`. The benchmarks (BenchmarkCheckpoint) are not ported.

mod common;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use agent::{Db, Logger, MemSeries, default_options};
use common::*;
use model::labels;
use model::metadata::{self, Metadata, MetricType};
use storage::{AppendV2Options, AppenderTransaction, AppenderV2};
use tsdb_format::wlog;

/// The series of a replayed DB (Go keeps the `*stripeSeries` after Close).
struct SeriesState {
    size: usize,
    by_ref: HashMap<u64, Arc<MemSeries>>,
}

fn open_db_and_do(is_in_mem_checkpoint: bool, storage_dir: &Path, f: impl FnOnce(Db)) {
    let mut opts = default_options();
    opts.enable_metadata_wal_records = true;
    opts.checkpoint_from_in_memory_series = is_in_mem_checkpoint;
    opts.wal_segment_size = WAL_SEGMENT_SIZE; // Set minimum size to get more segments for checkpoint.

    let db = agent::open(Logger::nop(), None, storage_dir, Some(opts)).expect("Open");
    f(db);
}

/// WAL replay of a checkpoint written by wlog::checkpoint and by agent::checkpoint gives the same series.
#[test]
fn test_checkpoint_replay_compatibility() {
    let test_name = "TestCheckpointReplayCompatibility";

    // Prepare samples and labels that will be written into the appender.
    let samples = gen_checkpoint_test_samples(test_name, 3, 3, 300);

    let append_data = |db: &Db| {
        let mut app = db.appender_v2();
        const FLUSH_EVERY: usize = 1000;
        let mut n = 0usize;
        let maybe_flush = |app: &mut agent::AppenderV2, n: &mut usize| {
            if *n < FLUSH_EVERY {
                return;
            }
            assert!(app.commit().is_none());
            *app = db.appender_v2();
            *n = 0;
        };

        let m = Metadata {
            typ: MetricType::Gauge,
            unit: "bytes".into(),
            help: "test help".into(),
        };
        for (i, lset) in samples.datapoint_labels.iter().enumerate() {
            for (j, sample) in samples.datapoint_samples.iter().enumerate() {
                let mut st = sample[0].t();
                if i % 3 != 0 {
                    st += j as i64 + 1;
                }
                let sf = sample[0].f();

                let mut aopts = AppendV2Options::default();
                if j == 0 && i % 2 == 0 {
                    aopts.metadata = Some(&m);
                }

                // Replay doesn't include exemplars, thus don't include them to remove them from the assertion.
                app.append(0, lset, 0, st, sf, None, None, &aopts).unwrap_or_else(|e| panic!("L: {i}; S: {j}: {e}"));
                n += 1;
                maybe_flush(&mut app, &mut n);
            }
        }

        for (i, lset) in samples.histogram_labels.iter().enumerate() {
            for (j, h) in samples.histogram_samples[i].iter().enumerate() {
                app.append(0, lset, 0, j as i64 + 1, 0.0, Some(h), None, &AppendV2Options::default()).unwrap();
                n += 1;
                maybe_flush(&mut app, &mut n);
            }
        }
        assert!(app.commit().is_none());
    };

    let collect = |db: &Db| SeriesState {
        size: db.series.size,
        by_ref: db.series.all_by_ref().into_iter().collect(),
    };

    // Write and replay for the old wlog::checkpoint. The WAL is expected in a "wal" subdirectory.
    let tmp1 = TempDir::new();
    let wlog_state_root = tmp1.path().join("state-wlog");
    let wlog_wal_dir = wlog_state_root.join("wal");
    std::fs::create_dir_all(&wlog_wal_dir).unwrap();

    open_db_and_do(false, &wlog_state_root, |db| {
        append_data(&db);
        // Trigger a checkpoint with mint=1 so series with i%3 == 0 (lastTs=0) are garbage-collected into
        // db.deleted while remaining in the checkpoint.
        db.truncate(1).expect("db.truncate");
        db.close().unwrap();
    });
    assert_checkpoint_exists(&wlog_wal_dir, 1);

    // Restore the database from the checkpoint.
    let mut wlog_after_series = None;
    open_db_and_do(true, &wlog_state_root, |db| {
        wlog_after_series = Some(collect(&db));
        db.close().unwrap();
    });

    // Write and replay using agent::checkpoint.
    let tmp2 = TempDir::new();
    let agent_state_root = tmp2.path().join("state-agent");
    let agent_wal_dir = agent_state_root.join("wal");
    std::fs::create_dir_all(&agent_wal_dir).unwrap();

    open_db_and_do(true, &agent_state_root, |db| {
        append_data(&db);
        db.truncate(1).expect("db.truncate");
        db.close().unwrap();
    });
    assert_checkpoint_exists(&agent_wal_dir, 1);

    let mut agent_after_series = None;
    open_db_and_do(true, &agent_state_root, |db| {
        agent_after_series = Some(collect(&db));
        db.close().unwrap();
    });

    require_stripe_series_equal(&wlog_after_series.unwrap(), &agent_after_series.unwrap());
}

/// Go `requireStripeSeriesEqual`: the same refs, and for each the same labels (by content), lastTs and
/// metadata.
fn require_stripe_series_equal(want: &SeriesState, got: &SeriesState) {
    assert_eq!(want.size, got.size, "stripeSeries size mismatch");
    assert_eq!(want.by_ref.len(), got.by_ref.len(), "series count mismatch");

    let mut metadata_count = 0;
    for (r, w) in &want.by_ref {
        let g = got.by_ref.get(r).unwrap_or_else(|| panic!("ref {r} present in wlog path, missing in agent path"));
        assert!(
            labels::equal(&w.lset, &g.lset),
            "ref {r} labels mismatch: wlog={} agent={}",
            w.lset,
            g.lset
        );
        assert_eq!(w.last_sample_timestamp(), g.last_sample_timestamp(), "ref {r} lastTs mismatch");
        assert_eq!(w.metadata(), g.metadata(), "ref {r} metadata mismatch");
        if !metadata::is_zero_handle(&g.metadata()) {
            metadata_count += 1;
        }
    }
    assert_ne!(0, metadata_count, "expected non-nil metadata on replayed series");
}

fn assert_checkpoint_exists(wal_dir: &Path, checkpoint_id: i64) {
    let d = wlog::checkpoint_dir(wal_dir, checkpoint_id);
    match std::fs::metadata(&d) {
        Ok(v) => assert!(v.is_dir(), "checkpoint should be a dir"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            panic!("checkpoint doesn't exists in WAL dir {:?}", wal_dir)
        }
        Err(e) => panic!("can't stat checkpoint dir {e}"),
    }
}
