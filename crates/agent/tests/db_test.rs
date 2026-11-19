//! Port of `tsdb/agent/db_test.go`.

mod common;

use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent::{
    AppenderBase, AppenderV1, DbInner, ERR_UNSUPPORTED, Logger, Options, default_options, validate_options,
    wal_truncation_time,
};
use common::*;
use model::exemplar::Exemplar;
use model::labels;
use model::metadata::{self, Metadata, MetricType};
use storage::{AppendV2Options, Appender, AppenderTransaction, AppenderV2, ChunkQueryable, Context, ExemplarQueryable, Queryable};
use tsdb_format::record::{self, Encoder, RefMetadata, RefSample, RefSeries};
use tsdb_format::wlog::WL;

/// Appends the five groups of series used by the V1 tests (floats, histograms, custom bucket histograms,
/// float histograms, custom bucket float histograms), each `num_series` series with the label prefix `name`.
///
/// `float_t`: the timestamp of the float samples (None: Go `chunks.GenerateSamples(0, 1)[0].T()`, i.e. 0).
/// `hist_t`: the timestamp of the histograms (None: the index of the histogram).
/// `exemplars`: also append an exemplar per float sample (as TestCommit does).
/// `commit_each`: commit after each series (as the truncate tests do).
#[allow(clippy::too_many_arguments)]
fn append_groups_v1(
    app: &mut AppenderV1,
    name: &str,
    num_series: usize,
    num_datapoints: usize,
    num_histograms: usize,
    float_t: Option<i64>,
    hist_t: Option<i64>,
    exemplars: bool,
    commit_each: bool,
) {
    for lset in labels_for_test(name, num_series) {
        for i in 0..num_datapoints {
            let sample = storage::chunks::generate_samples(0, 1);
            let t = float_t.unwrap_or(sample[0].t());
            let v = if float_t.is_some() { 0.0 } else { sample[0].f() };
            let r = app.append(0, &lset, t, v).unwrap();
            if exemplars {
                let e = Exemplar {
                    labels: lset.clone(),
                    ts: sample[0].t() + i as i64,
                    value: sample[0].f(),
                    has_ts: true,
                };
                app.append_exemplar(r, &lset, &e).unwrap();
            }
        }
        if commit_each {
            assert!(app.commit().is_none());
        }
    }
    let ht = |i: usize| hist_t.unwrap_or(i as i64);
    for lset in labels_for_test(&format!("{name}_histogram"), num_series) {
        let hs = storage::tsdbutil::generate_test_histograms(num_histograms);
        for (i, h) in hs.iter().enumerate() {
            app.append_histogram(0, &lset, ht(i), Some(h), None).unwrap();
        }
        if commit_each {
            assert!(app.commit().is_none());
        }
    }
    for lset in labels_for_test(&format!("{name}_custom_buckets_histogram"), num_series) {
        let hs = storage::tsdbutil::generate_test_custom_buckets_histograms(num_histograms);
        for (i, h) in hs.iter().enumerate() {
            app.append_histogram(0, &lset, ht(i), Some(h), None).unwrap();
        }
        if commit_each {
            assert!(app.commit().is_none());
        }
    }
    for lset in labels_for_test(&format!("{name}_float_histogram"), num_series) {
        let fhs = storage::tsdbutil::generate_test_float_histograms(num_histograms);
        for (i, fh) in fhs.iter().enumerate() {
            app.append_histogram(0, &lset, ht(i), None, Some(fh)).unwrap();
        }
        if commit_each {
            assert!(app.commit().is_none());
        }
    }
    for lset in labels_for_test(&format!("{name}_custom_buckets_float_histogram"), num_series) {
        let fhs = storage::tsdbutil::generate_test_custom_buckets_float_histograms(num_histograms);
        for (i, fh) in fhs.iter().enumerate() {
            app.append_histogram(0, &lset, ht(i), None, Some(fh)).unwrap();
        }
        if commit_each {
            assert!(app.commit().is_none());
        }
    }
}

#[test]
fn test_appender_buffer_reset_after_wal_write_error() {
    type Prepare = fn(&mut AppenderBase);
    type Log = fn(&mut AppenderBase) -> Result<(), storage::Error>;
    let tests: [(&str, Prepare, Log); 2] = [
        (
            "log",
            |a| {
                a.pending_samples.push(RefSample {
                    r#ref: 1,
                    t: 1,
                    v: 1.0,
                    ..Default::default()
                })
            },
            |a| a.log(),
        ),
        (
            "logSeries",
            |a| {
                a.pending_series.push(RefSeries {
                    r#ref: 1,
                    labels: labels::from_strings(&["__name__", "test_metric"]),
                })
            },
            |a| a.log_series(),
        ),
    ];
    for (name, prepare, log) in tests {
        let dir = TempDir::new();
        let wal = WL::new(dir.path(), default_options().wal_compression).unwrap();
        wal.close().unwrap();

        let db = Arc::new(DbInner::new(Logger::nop(), default_options(), None, wal));
        let mut app = AppenderBase::new(db.clone());
        prepare(&mut app);

        assert!(log(&mut app).is_err(), "{name}");

        // adapted: Go retries up to 100 times because a sync.Pool may drop buffers; the Rust pool keeps them.
        let buf = db.buf_pool.lock().unwrap().pop();
        let buf = buf.unwrap_or_else(|| panic!("{name}: no buffer returned from pool"));
        assert!(buf.is_empty(), "{name}");
    }
}

/// Concurrent appends for the same label set produce exactly one series in memory and one series record in
/// the WAL.
#[test]
fn test_concurrent_append_same_labels() {
    let mut opts = default_options();
    opts.out_of_order_time_window = i64::MAX;
    let db = Arc::new(create_test_agent_db(opts));
    let lset = labels::from_strings(&["__name__", "test_metric"]);

    const N: usize = 100;
    let start = Arc::new(Barrier::new(N));
    let mut hs = Vec::new();
    for _ in 0..N {
        let (db, start, lset) = (db.clone(), start.clone(), lset.clone());
        hs.push(std::thread::spawn(move || {
            let mut app = db.appender_v2();
            start.wait();
            app.append(0, &lset, 0, 1000, 1.0, None, None, &AppendV2Options::default()).unwrap();
            assert!(app.commit().is_none());
        }));
    }
    for h in hs {
        h.join().unwrap();
    }

    let total: usize = (0..db.series.size).map(|i| db.series.read(i).series.len()).sum();
    assert_eq!(1, total);
    db.close().unwrap();

    let wal_series = count_wal(db.wal.dir()).series;
    assert_eq!(1, wal_series);
}

#[test]
fn test_unsupported_functions() {
    let s = create_test_agent_db(default_options());

    // Querier.
    match s.querier(0, 0) {
        Err(e) => assert!(e.is(&ERR_UNSUPPORTED) && e.to_string() == ERR_UNSUPPORTED.to_string()),
        Ok(_) => panic!("expected ErrUnsupported"),
    }
    // ChunkQuerier.
    match s.chunk_querier(0, 0) {
        Err(e) => assert!(e.is(&ERR_UNSUPPORTED) && e.to_string() == ERR_UNSUPPORTED.to_string()),
        Ok(_) => panic!("expected ErrUnsupported"),
    }
    // ExemplarQuerier.
    match s.exemplar_querier(&Context::background()) {
        Err(e) => assert!(e.is(&ERR_UNSUPPORTED) && e.to_string() == ERR_UNSUPPORTED.to_string()),
        Ok(_) => panic!("expected ErrUnsupported"),
    }
    s.close().unwrap();
}

#[test]
fn test_commit() {
    const NUM_DATAPOINTS: usize = 1000;
    const NUM_HISTOGRAMS: usize = 100;
    const NUM_SERIES: usize = 8;

    let s = create_test_agent_db(default_options());
    let mut app = s.appender();
    append_groups_v1(&mut app, "TestCommit", NUM_SERIES, NUM_DATAPOINTS, NUM_HISTOGRAMS, None, None, true, false);

    assert!(app.commit().is_none());
    s.close().unwrap();

    // Read records from WAL and check for expected count of series, samples, and exemplars.
    let c = count_wal(s.wal.dir());
    assert_eq!(NUM_SERIES * 5, c.series, "unexpected number of series");
    assert_eq!(NUM_SERIES * NUM_DATAPOINTS, c.samples, "unexpected number of samples");
    assert_eq!(NUM_SERIES * NUM_DATAPOINTS, c.exemplars, "unexpected number of exemplars");
    assert_eq!(NUM_SERIES * NUM_HISTOGRAMS * 2, c.histograms, "unexpected number of histograms");
    assert_eq!(NUM_SERIES * NUM_HISTOGRAMS * 2, c.float_histograms, "unexpected number of float histograms");

    // Check that we can get another appender after this - see https://github.com/prometheus/prometheus/issues/17800.
    let _ = s.appender();
}

#[test]
fn test_rollback() {
    const NUM_DATAPOINTS: usize = 1000;
    const NUM_HISTOGRAMS: usize = 100;
    const NUM_SERIES: usize = 8;

    let s = create_test_agent_db(default_options());
    let mut app = s.appender();
    append_groups_v1(&mut app, "TestRollback", NUM_SERIES, NUM_DATAPOINTS, NUM_HISTOGRAMS, None, None, false, false);

    // Do a rollback, which should clear uncommitted data. A followup call to commit should persist nothing to
    // the WAL.
    assert!(app.rollback().is_none());
    assert!(app.commit().is_none());
    s.close().unwrap();

    let c = count_wal(s.wal.dir());
    // Check that only series get stored after calling Rollback.
    assert_eq!(NUM_SERIES * 5, c.series, "series should have been written to WAL");
    assert_eq!(0, c.samples, "samples should not have been written to WAL");
    assert_eq!(0, c.exemplars, "exemplars should not have been written to WAL");
    assert_eq!(0, c.histograms, "histograms should not have been written to WAL");
    assert_eq!(0, c.float_histograms, "float histograms should not have been written to WAL");
}

#[test]
fn test_full_truncate_wal() {
    const NUM_DATAPOINTS: usize = 1000;
    const NUM_HISTOGRAMS: usize = 100;
    const NUM_SERIES: usize = 800;
    const LAST_TS: i64 = 500;

    let mut opts = default_options();
    opts.truncate_frequency = Duration::from_secs(2 * 60);

    let s = create_test_agent_db(opts);
    let mut app = s.appender();
    append_groups_v1(
        &mut app,
        "TestFullTruncateWAL",
        NUM_SERIES,
        NUM_DATAPOINTS,
        NUM_HISTOGRAMS,
        Some(LAST_TS),
        Some(LAST_TS),
        false,
        true,
    );

    // Truncate WAL with mint to GC all the samples.
    let _ = s.truncate(LAST_TS + 1);

    assert_eq!(
        (NUM_SERIES * 5) as f64,
        s.metrics.num_wal_series_pending_deletion.get(),
        "agent wal truncate mismatch of deleted series count"
    );
    s.close().unwrap();
}

#[test]
fn test_partial_truncate_wal() {
    const NUM_DATAPOINTS: usize = 1000;
    const NUM_SERIES: usize = 800;

    let s = create_test_agent_db(default_options());
    let mut app = s.appender();

    // Create first batch of 800 series with 1000 data-points with a fixed lastTs as 500.
    let mut last_ts: i64 = 500;
    batch_v1(&mut app, "TestPartialTruncateWAL", "batch-1", NUM_SERIES, NUM_DATAPOINTS, last_ts);

    // Create second batch of 800 series with 1000 data-points with a fixed lastTs as 600.
    last_ts = 600;
    batch_v1(&mut app, "TestPartialTruncateWAL", "batch-2", NUM_SERIES, NUM_DATAPOINTS, last_ts);

    // Truncate WAL with mint to GC only the first batch of 800 series and retaining 2nd batch of 800 series.
    let _ = s.truncate(last_ts - 1);

    // adapted: Go also checks that the gauge family has exactly one metric (it is a plain gauge here).
    assert_eq!(
        (NUM_SERIES * 5) as f64,
        s.metrics.num_wal_series_pending_deletion.get(),
        "agent wal truncate mismatch of deleted series count"
    );
    s.close().unwrap();
}

/// The batches of TestPartialTruncateWAL: names `<test>batch-N` for floats and `<test>_<kind>_batch-N` for
/// the histogram kinds, every series committed separately, all samples at `last_ts`.
fn batch_v1(app: &mut AppenderV1, test: &str, batch: &str, num_series: usize, num_datapoints: usize, last_ts: i64) {
    for lset in labels_for_test(&format!("{test}{batch}"), num_series) {
        for _ in 0..num_datapoints {
            app.append(0, &lset, last_ts, 0.0).unwrap();
        }
        assert!(app.commit().is_none());
    }
    for lset in labels_for_test(&format!("{test}_histogram_{batch}"), num_series) {
        for h in storage::tsdbutil::generate_test_histograms(num_datapoints) {
            app.append_histogram(0, &lset, last_ts, Some(&h), None).unwrap();
        }
        assert!(app.commit().is_none());
    }
    for lset in labels_for_test(&format!("{test}_custom_buckets_histogram_{batch}"), num_series) {
        for h in storage::tsdbutil::generate_test_custom_buckets_histograms(num_datapoints) {
            app.append_histogram(0, &lset, last_ts, Some(&h), None).unwrap();
        }
        assert!(app.commit().is_none());
    }
    for lset in labels_for_test(&format!("{test}_float_histogram_{batch}"), num_series) {
        for fh in storage::tsdbutil::generate_test_float_histograms(num_datapoints) {
            app.append_histogram(0, &lset, last_ts, None, Some(&fh)).unwrap();
        }
        assert!(app.commit().is_none());
    }
    for lset in labels_for_test(&format!("{test}_custom_buckets_float_histogram_{batch}"), num_series) {
        for fh in storage::tsdbutil::generate_test_custom_buckets_float_histograms(num_datapoints) {
            app.append_histogram(0, &lset, last_ts, None, Some(&fh)).unwrap();
        }
        assert!(app.commit().is_none());
    }
}

#[test]
fn test_wal_replay() {
    for unknown_record in [false, true] {
        const NUM_DATAPOINTS: usize = 1000;
        const NUM_HISTOGRAMS: usize = 100;
        const NUM_SERIES: usize = 8;
        const LAST_TS: i64 = 500;

        let s = create_test_agent_db(default_options());
        if unknown_record {
            // Valid records following an unknown type must still be replayed.
            s.wal.log(&[255, 1, 2, 3]).unwrap();
        }
        let mut app = s.appender();
        let name = format!("TestWALReplay/unknownRecord={unknown_record}");
        append_groups_v1(
            &mut app,
            &name,
            NUM_SERIES,
            NUM_DATAPOINTS,
            NUM_HISTOGRAMS,
            Some(LAST_TS),
            Some(LAST_TS),
            false,
            false,
        );
        assert!(app.commit().is_none());
        s.close().unwrap();

        // s.wal.dir() is the /wal subdirectory of the original storage path.
        let storage_dir = s.wal.dir().parent().unwrap().to_path_buf();

        // Replay twice to ensure unknown records do not trigger destructive WAL repair.
        for _ in 0..2 {
            let replay_storage = agent::open(s.logger.clone(), None, &storage_dir, Some(s.opts.clone()))
                .unwrap_or_else(|e| panic!("unable to create storage for the agent: {e}"));

            // Check if all the series are retrieved back from the WAL.
            assert_eq!(
                (NUM_SERIES * 5) as f64,
                replay_storage.metrics.num_active_series.get(),
                "agent wal replay mismatch of active series count"
            );

            // Check if lastTs of the samples retrieved from the WAL is retained.
            for v in replay_storage.series.all_by_ref().values() {
                assert_eq!(v.last_sample_timestamp(), LAST_TS);
            }
            replay_storage.close().unwrap();
        }
    }
}

#[test]
fn test_wal_truncation_time() {
    let opts = default_options();
    let now_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let hour = 60 * 60 * 1000;
    for (name, sent, want) in [
        ("current", now_ts, now_ts - opts.min_wal_time),
        ("lagging", now_ts - hour, now_ts - hour - opts.min_wal_time),
        ("stalled", 0, now_ts - opts.max_wal_time),
        ("future", now_ts + 5 * hour, now_ts - opts.min_wal_time),
    ] {
        assert_eq!(want, wal_truncation_time(&opts, sent, now_ts), "{name}");
    }
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
            let db = agent::open(Logger::nop(), None, data, Some(opts)).unwrap();
            // adapted: the DB keeps its locker behind a mutex; the test takes it out to give a reference to it
            // and puts it back before closing the DB, which releases it.
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

#[test]
fn test_validate_options() {
    // Apply defaults to zero values.
    let opts = validate_options(Some(Options::default()));
    assert_eq!(default_options(), opts);

    // Defaults are already valid.
    assert_eq!(default_options(), validate_options(None));

    // MaxWALTime should not be lower than TruncateFrequency.
    let opts = validate_options(Some(Options {
        max_wal_time: 60 * 60 * 1000,
        truncate_frequency: Duration::from_secs(2 * 60 * 60),
        ..Default::default()
    }));
    assert_eq!(2 * 60 * 60 * 1000, opts.max_wal_time);
}

#[test]
fn test_duplicate_series_refs_by_hash() {
    for in_mem_checkpoint in [false, true] {
        let name = format!("CheckpointFromInMemorySeries={in_mem_checkpoint}");
        let db_dir = TempDir::new();
        let mut opts = default_options();
        opts.enable_metadata_wal_records = true;
        opts.checkpoint_from_in_memory_series = in_mem_checkpoint;
        let db = agent::open(Logger::nop(), None, db_dir.path(), Some(opts.clone())).unwrap();

        let mut app = db.appender_v2();

        let initial_meta = Metadata {
            typ: MetricType::Gauge,
            unit: "bytes".into(),
            help: "initial metadata".into(),
        };
        let updated_meta = Metadata {
            typ: MetricType::Counter,
            unit: "seconds".into(),
            help: "updated metadata".into(),
        };

        let metric_names = ["foo", "bar", "baz", "blerg"];
        let mut original_series_refs = Vec::new();
        for metric_name in metric_names {
            let lbls = labels::from_map([("__name__", metric_name)]);
            let r = app.append(0, &lbls, 0, 0, 10.0, None, None, &AppendV2Options::default()).unwrap();
            original_series_refs.push(r);
            let o = AppendV2Options {
                metadata: Some(&initial_meta),
                ..Default::default()
            };
            let r2 = app.append(r, &lbls, 0, 10, 100.0, None, None, &o).unwrap();
            assert_eq!(r, r2);
        }
        assert!(app.commit().is_none());

        // Forcefully create a bunch of new segments to force a truncation.
        for _ in 0..3 {
            db.wal.next_segment_sync().unwrap();
        }
        // No series should be deleted yet.
        assert!(db.deleted.lock().unwrap().is_empty(), "{name}");

        // Truncate at 1 ms higher than the highest timestamp.
        db.truncate(11).unwrap();

        // The original SeriesRefs should be considered deleted.
        for r in &original_series_refs {
            assert!(db.series.get_by_id(*r).is_none(), "{name}");
            assert!(db.deleted.lock().unwrap().contains_key(r), "{name}");
        }

        let mut duplicate_series_refs = Vec::new();
        for (i, metric_name) in metric_names.iter().enumerate() {
            let lbls = labels::from_map([("__name__", *metric_name)]);
            // Leave i==0 without a new metadata record in the segment so replay must recover initialMeta from the
            // deleted series record in the checkpoint.
            let mut aopts = AppendV2Options::default();
            if i > 0 {
                aopts.metadata = Some(&updated_meta);
            }
            let r = app.append(0, &lbls, 0, 20, 10.0, None, None, &aopts).unwrap();
            duplicate_series_refs.push(r);
        }
        assert!(app.commit().is_none());

        // Write a raw metadata record for a non-existent series ref to exercise the nil series skip on replay.
        let mut buf = Vec::new();
        Encoder::default().metadata(
            &[RefMetadata {
                r#ref: 9999,
                r#type: record::get_metric_type(MetricType::Gauge),
                unit: "bytes".into(),
                help: "orphan metadata".into(),
            }],
            &mut buf,
        );
        db.wal.log(&buf).unwrap();

        // The duplicate SeriesRefs should be in series.
        for r in &duplicate_series_refs {
            assert!(db.series.get_by_id(*r).is_some(), "{name}");
        }

        // Close the WAL before we have a chance to remove the original RefIDs.
        drop(app);
        db.close().unwrap();
        drop(db);

        let db = agent::open(Logger::nop(), None, db_dir.path(), Some(opts.clone())).unwrap();

        // The original SeriesRefs should be in series with initialMeta (for i==0, recovered from the deleted
        // series in the checkpoint) or updatedMeta (for i>0, remapped from duplicateSeriesRefs).
        for (i, r) in original_series_refs.iter().enumerate() {
            let s = db.series.get_by_id(*r).unwrap_or_else(|| panic!("{name}: series {r}"));
            if i == 0 {
                assert_eq!(&initial_meta, s.metadata().value(), "{name}");
            } else {
                assert_eq!(&updated_meta, s.metadata().value(), "{name}");
            }
            assert!(!db.deleted.lock().unwrap().contains_key(r), "{name}");
        }

        // The duplicated SeriesRefs should be considered deleted.
        for (i, r) in duplicate_series_refs.iter().enumerate() {
            assert!(db.series.get_by_id(*r).is_none(), "{name}");
            let deleted = db.deleted.lock().unwrap();
            assert!(deleted.contains_key(r), "{name}");
            if in_mem_checkpoint && i > 0 {
                assert_eq!(&updated_meta, deleted[r].meta.value(), "{name}");
            }
        }
        db.close().unwrap();
    }
}

fn no_metadata_records(recs: &[Rec], msg: &str) {
    assert!(!recs.is_empty());
    for rec in recs {
        assert!(!matches!(rec, Rec::Metadata(_)), "{msg}");
    }
}

#[test]
fn test_update_metadata() {
    // Feature disabled.
    {
        let mut opts = default_options();
        opts.enable_metadata_wal_records = false;
        let s = create_test_agent_db(opts);

        let mut app = s.appender();
        let lbls = labels::from_strings(&["__name__", "m1"]);
        let r = app.append(0, &lbls, 1000, 1.0).unwrap();

        let m = Metadata {
            typ: MetricType::Gauge,
            unit: "bytes".into(),
            help: "help".into(),
        };
        app.update_metadata(r, &lbls, &m).unwrap();
        assert!(app.commit().is_none());

        // WAL should not contain any metadata records.
        no_metadata_records(
            &read_test_wal(s.wal.dir()),
            "unexpected metadata record in WAL when feature is disabled",
        );
    }

    // Empty metadata.
    {
        let mut opts = default_options();
        opts.enable_metadata_wal_records = true;
        let s = create_test_agent_db(opts);

        let mut app = s.appender();
        let lbls = labels::from_strings(&["__name__", "m1"]);
        let r = app.append(0, &lbls, 1000, 1.0).unwrap();

        let meta_ref = app.update_metadata(r, &lbls, &Metadata::default()).unwrap();
        assert_eq!(0, meta_ref);
        assert!(app.commit().is_none());

        let mem_s = s.series.get_by_id(r).unwrap();
        assert!(metadata::is_zero_handle(&mem_s.metadata()));

        no_metadata_records(&read_test_wal(s.wal.dir()), "unexpected metadata record in WAL for empty metadata");
    }

    // Unknown series.
    {
        let mut opts = default_options();
        opts.enable_metadata_wal_records = true;
        let s = create_test_agent_db(opts);

        let mut app = s.appender();
        let m = Metadata {
            typ: MetricType::Gauge,
            unit: "bytes".into(),
            help: "help".into(),
        };
        let err = app.update_metadata(0, &labels::from_strings(&["__name__", "unknown"]), &m).unwrap_err();
        assert!(err.to_string().contains("unknown series"));
    }

    // Metadata logged, deduplicated, and updated.
    {
        let mut opts = default_options();
        opts.enable_metadata_wal_records = true;
        let s = create_test_agent_db(opts);

        let s1 = labels::from_strings(&["__name__", "m1"]);
        let s2 = labels::from_strings(&["__name__", "m2"]);

        let m1 = Metadata {
            typ: MetricType::Gauge,
            unit: "seconds".into(),
            help: "help 1".into(),
        };
        let m2 = Metadata {
            typ: MetricType::Counter,
            unit: "bytes".into(),
            help: "help 2".into(),
        };

        // First transaction: create series and update metadata.
        let mut app = s.appender();
        let ref1 = app.append(0, &s1, 1000, 1.0).unwrap();
        let ref2 = app.append(0, &s2, 1000, 2.0).unwrap();

        let got_ref1 = app.update_metadata(ref1, &s1, &m1).unwrap();
        assert_eq!(ref1, got_ref1);
        // Pass 0 as ref to exercise the GetByHash fallback for an existing series.
        let got_ref2 = app.update_metadata(0, &s2, &m2).unwrap();
        assert_eq!(ref2, got_ref2);
        assert!(app.commit().is_none());

        // Check in-memory metadata.
        let mem_s1 = s.series.get_by_id(ref1).unwrap();
        assert_eq!(&m1, mem_s1.metadata().value());
        let mem_s2 = s.series.get_by_id(ref2).unwrap();
        assert_eq!(&m2, mem_s2.metadata().value());

        // Second transaction: append same metadata (dedup) for s1, new metadata for s2.
        let m2_updated = Metadata {
            typ: MetricType::Counter,
            unit: "bytes".into(),
            help: "help 2 updated".into(),
        };
        let mut app = s.appender();
        app.append(ref1, &s1, 2000, 1.5).unwrap();
        app.update_metadata(ref1, &s1, &m1).unwrap();
        app.append(ref2, &s2, 2000, 2.5).unwrap();
        app.update_metadata(ref2, &s2, &m2_updated).unwrap();
        assert!(app.commit().is_none());

        // Verify in-memory updated metadata.
        assert_eq!(&m2_updated, mem_s2.metadata().value());

        // Verify WAL content: expected 2 metadata blocks (first has m1, m2; second has only m2Updated).
        let got_meta = metadata_records(&read_test_wal(s.wal.dir()));
        assert_eq!(2, got_meta.len());
        let rm = |r: u64, m: &Metadata| RefMetadata {
            r#ref: r,
            r#type: record::get_metric_type(m.typ),
            unit: m.unit.clone(),
            help: m.help.clone(),
        };
        assert_eq!(vec![rm(ref1, &m1), rm(ref2, &m2)], got_meta[0]);
        assert_eq!(vec![rm(ref2, &m2_updated)], got_meta[1]);
    }

    // Rollback does not update metadata.
    {
        let mut opts = default_options();
        opts.enable_metadata_wal_records = true;
        let s = create_test_agent_db(opts);

        let s1 = labels::from_strings(&["__name__", "m1"]);
        let m1 = Metadata {
            typ: MetricType::Gauge,
            unit: "seconds".into(),
            help: "help 1".into(),
        };

        let mut app = s.appender();
        let ref1 = app.append(0, &s1, 1000, 1.0).unwrap();
        app.update_metadata(ref1, &s1, &m1).unwrap();
        assert!(app.commit().is_none());

        // Start second transaction and rollback.
        let m1_new = Metadata {
            typ: MetricType::Gauge,
            unit: "minutes".into(),
            help: "help 1 changed".into(),
        };
        let mut app = s.appender();
        app.update_metadata(ref1, &s1, &m1_new).unwrap();
        assert!(app.rollback().is_none());

        // Verify in-memory metadata was NOT changed.
        let mem_s1 = s.series.get_by_id(ref1).unwrap();
        assert_eq!(&m1, mem_s1.metadata().value());
    }
}

#[test]
fn test_wal_replay_metadata() {
    let mut opts = default_options();
    opts.enable_metadata_wal_records = true;
    let dir = TempDir::new();

    let db = agent::open(Logger::nop(), None, dir.path(), Some(opts.clone())).unwrap();

    let s1 = labels::from_strings(&["__name__", "cpu_usage"]);
    let s2 = labels::from_strings(&["__name__", "mem_bytes"]);

    let m1 = Metadata {
        typ: MetricType::Gauge,
        unit: "percent".into(),
        help: "CPU usage percentage".into(),
    };
    let m2 = Metadata {
        typ: MetricType::Gauge,
        unit: "bytes".into(),
        help: "Memory used in bytes".into(),
    };

    let mut app = db.appender_v2();
    let o1 = AppendV2Options {
        metadata: Some(&m1),
        ..Default::default()
    };
    let ref1 = app.append(0, &s1, 0, 1000, 50.0, None, None, &o1).unwrap();
    let o2 = AppendV2Options {
        metadata: Some(&m2),
        ..Default::default()
    };
    let ref2 = app.append(0, &s2, 0, 1000, 1024.0, None, None, &o2).unwrap();
    assert!(app.commit().is_none());
    db.close().unwrap();

    // Reopen DB with EnableMetadataWALRecords=true and verify metadata is restored during WAL replay.
    let db2 = agent::open(Logger::nop(), None, dir.path(), Some(opts)).unwrap();
    assert_eq!(&m1, db2.series.get_by_id(ref1).unwrap().metadata().value());
    assert_eq!(&m2, db2.series.get_by_id(ref2).unwrap().metadata().value());
    db2.close().unwrap();

    // Reopen DB with EnableMetadataWALRecords=false and verify existing metadata in WAL is still restored.
    let mut opts_disabled = default_options();
    opts_disabled.enable_metadata_wal_records = false;
    let db3 = agent::open(Logger::nop(), None, dir.path(), Some(opts_disabled)).unwrap();
    assert_eq!(&m1, db3.series.get_by_id(ref1).unwrap().metadata().value());
    assert_eq!(&m2, db3.series.get_by_id(ref2).unwrap().metadata().value());
    db3.close().unwrap();
}
