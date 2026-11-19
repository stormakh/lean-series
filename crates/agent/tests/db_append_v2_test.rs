//! Port of `tsdb/agent/db_append_v2_test.go`.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use agent::{AppenderV2, Logger, default_options, deleted_series_iter};
use common::*;
use model::exemplar::Exemplar;
use model::histogram::{CounterResetHint, FloatHistogram, Histogram};
use model::labels::{self, Labels};
use model::metadata::{self, Metadata, MetricType};
use storage::{
    AppendV2Options, AppenderTransaction, AppenderV2 as _, ERR_EXEMPLAR_LABEL_LENGTH, ERR_NOT_FOUND,
    ERR_OUT_OF_ORDER_SAMPLE, Error, ExemplarAppenderV2,
};
use tsdb::head::{ERR_INVALID_EXEMPLAR, ERR_INVALID_SAMPLE};
use tsdb_format::record::{self, RefMetadata, Type};
use tsdb_format::wlog;

fn no_opts() -> AppendV2Options<'static> {
    AppendV2Options::default()
}

fn append(app: &mut AppenderV2, r: u64, ls: &Labels, st: i64, t: i64, v: f64) -> Result<u64, Error> {
    app.append(r, ls, st, t, v, None, None, &no_opts())
}

fn append_h(app: &mut AppenderV2, ls: &Labels, st: i64, t: i64, h: &Histogram) -> Result<u64, Error> {
    app.append(0, ls, st, t, 0.0, Some(h), None, &no_opts())
}

fn append_fh(app: &mut AppenderV2, ls: &Labels, st: i64, t: i64, fh: &FloatHistogram) -> Result<u64, Error> {
    app.append(0, ls, st, t, 0.0, None, Some(fh), &no_opts())
}

fn with_meta(m: &Metadata) -> AppendV2Options<'_> {
    AppendV2Options {
        metadata: Some(m),
        ..Default::default()
    }
}

fn meta(typ: MetricType, unit: &str, help: &str) -> Metadata {
    Metadata {
        typ,
        unit: unit.into(),
        help: help.into(),
    }
}

fn ref_meta(r: u64, m: &Metadata) -> RefMetadata {
    RefMetadata {
        r#ref: r,
        r#type: record::get_metric_type(m.typ),
        unit: m.unit.clone(),
        help: m.help.clone(),
    }
}

#[test]
fn test_db_invalid_series_append_v2() {
    let s = create_test_agent_db(default_options());
    let mut app = s.appender_v2();

    // Samples.
    let err = append(&mut app, 0, &Labels::default(), 0, 0, 0.0).unwrap_err();
    assert!(err.is(&ERR_INVALID_SAMPLE), "should reject empty labels");
    let err = append(&mut app, 0, &labels::from_strings(&["a", "1", "a", "2"]), 0, 0, 0.0).unwrap_err();
    assert!(err.is(&ERR_INVALID_SAMPLE), "should reject duplicate labels");

    // Histograms.
    let h = &storage::tsdbutil::generate_test_histograms(1)[0];
    let err = append_h(&mut app, &Labels::default(), 0, 0, h).unwrap_err();
    assert!(err.is(&ERR_INVALID_SAMPLE), "should reject empty labels");
    let err = append_h(&mut app, &labels::from_strings(&["a", "1", "a", "2"]), 0, 0, h).unwrap_err();
    assert!(err.is(&ERR_INVALID_SAMPLE), "should reject duplicate labels");

    // Exemplars.
    let e = Exemplar {
        labels: labels::from_strings(&["a", "1", "a", "2"]),
        ..Default::default()
    };
    let ex = [e];
    let o = AppendV2Options {
        exemplars: &ex,
        ..Default::default()
    };
    let err = app.append(0, &labels::from_strings(&["a", "1"]), 0, 0, 0.0, None, None, &o).unwrap_err();
    let part_err = err.as_append_partial().expect("AppendPartialError");
    assert_eq!(1, part_err.exemplar_errors.len());
    assert!(part_err.exemplar_errors[0].is(&ERR_INVALID_EXEMPLAR), "should reject duplicate labels");

    let e = Exemplar {
        labels: labels::from_strings(&[
            "a_somewhat_long_trace_id",
            "nYJSNtFrFTY37VR7mHzEE/LIDt7cdAQcuOzFajgmLDAdBSRHYPDzrxhMA4zz7el8naI/AoXFv9/e/G0vcETcIoNUi3OieeLfaIRQci2oa",
        ]),
        ..Default::default()
    };
    let ex = [
        Exemplar {
            labels: labels::from_strings(&["a", "1"]),
            ..Default::default()
        },
        e,
    ];
    let o = AppendV2Options {
        exemplars: &ex,
        ..Default::default()
    };
    let err = app.append(0, &labels::from_strings(&["a", "2"]), 0, 0, 0.0, None, None, &o).unwrap_err();
    let part_err = err.as_append_partial().expect("AppendPartialError");
    assert_eq!(1, part_err.exemplar_errors.len());
    assert!(part_err.exemplar_errors[0].is(&ERR_EXEMPLAR_LABEL_LENGTH), "should reject too long label length");
    assert_eq!(1, part_err.exemplar_errors[0].index, "should report the position of the rejected exemplar");

    // Inverse check.
    let ex = [Exemplar {
        labels: labels::from_strings(&["a", "1"]),
        value: 20.0,
        ts: 10,
        has_ts: true,
    }];
    let o = AppendV2Options {
        exemplars: &ex,
        ..Default::default()
    };
    app.append(0, &labels::from_strings(&["a", "1"]), 0, 0, 0.0, None, None, &o)
        .expect("should not reject valid exemplars");
}

const V1_TYPES: [Type; 5] = [
    Type::Samples,
    Type::HistogramSamples,
    Type::CustomBucketsHistogramSamples,
    Type::FloatHistogramSamples,
    Type::CustomBucketsFloatHistogramSamples,
];
const V2_TYPES: [Type; 3] = [Type::SamplesV2, Type::HistogramSamplesV2, Type::FloatHistogramSamplesV2];

/// Appender commit.
#[test]
fn test_commit_append_v2() {
    const NUM_DATAPOINTS: usize = 1000;
    const NUM_HISTOGRAMS: usize = 100;
    const NUM_SERIES: usize = 8;
    for enable_st_storage in [false, true] {
        let name = format!("TestCommit_AppendV2/enableSTStorage={enable_st_storage}");
        let mut opts = default_options();
        opts.enable_st_storage = enable_st_storage;
        let s = create_test_agent_db(opts);

        let mut expected_sample_sts = Vec::new();
        let mut expected_histogram_sts = Vec::new();
        let mut expected_float_histogram_sts = Vec::new();

        let mut app = s.appender_v2();
        for lset in labels_for_test(&name, NUM_SERIES) {
            for i in 0..NUM_DATAPOINTS {
                let sample = storage::chunks::generate_samples(0, 1);
                let st = i as i64 + 1234;
                let ex = [Exemplar {
                    labels: lset.clone(),
                    ts: sample[0].t() + i as i64 + 2000,
                    value: sample[0].f(),
                    has_ts: true,
                }];
                let o = AppendV2Options {
                    exemplars: &ex,
                    ..Default::default()
                };
                app.append(0, &lset, st, sample[0].t() + 2000, sample[0].f(), None, None, &o).unwrap();
                if enable_st_storage {
                    expected_sample_sts.push(st);
                }
            }
        }
        for lset in labels_for_test(&format!("{name}_histogram"), NUM_SERIES) {
            for (i, h) in storage::tsdbutil::generate_test_histograms(NUM_HISTOGRAMS).iter().enumerate() {
                let st = i as i64 + 2234;
                append_h(&mut app, &lset, st, i as i64 + 2000, h).unwrap();
                if enable_st_storage {
                    expected_histogram_sts.push(st);
                }
            }
        }
        for lset in labels_for_test(&format!("{name}_custom_buckets_histogram"), NUM_SERIES) {
            for (i, h) in storage::tsdbutil::generate_test_custom_buckets_histograms(NUM_HISTOGRAMS).iter().enumerate()
            {
                let st = i as i64 + 3234;
                append_h(&mut app, &lset, st, i as i64 + 2000, h).unwrap();
                if enable_st_storage {
                    expected_histogram_sts.push(st);
                }
            }
        }
        for lset in labels_for_test(&format!("{name}_float_histogram"), NUM_SERIES) {
            for (i, fh) in storage::tsdbutil::generate_test_float_histograms(NUM_HISTOGRAMS).iter().enumerate() {
                let st = i as i64 + 4234;
                append_fh(&mut app, &lset, st, i as i64 + 2000, fh).unwrap();
                if enable_st_storage {
                    expected_float_histogram_sts.push(st);
                }
            }
        }
        for lset in labels_for_test(&format!("{name}_custom_buckets_float_histogram"), NUM_SERIES) {
            for (i, fh) in
                storage::tsdbutil::generate_test_custom_buckets_float_histograms(NUM_HISTOGRAMS).iter().enumerate()
            {
                let st = i as i64 + 5234;
                append_fh(&mut app, &lset, st, i as i64 + 2000, fh).unwrap();
                if enable_st_storage {
                    expected_float_histogram_sts.push(st);
                }
            }
        }

        assert!(app.commit().is_none());
        s.close().unwrap();

        let c = count_wal(s.wal.dir());
        for t in &c.types {
            if enable_st_storage {
                assert!(!V1_TYPES.contains(t), "{name}: Got V1 Samples when ST enabled");
            } else {
                assert!(!V2_TYPES.contains(t), "{name}: Got V2 Samples when ST disabled");
            }
        }

        // Check that the WAL contained the same number of committed series/samples/exemplars.
        assert_eq!(NUM_SERIES * 5, c.series, "unexpected number of series");
        assert_eq!(NUM_SERIES * NUM_DATAPOINTS, c.samples, "unexpected number of samples");
        assert_eq!(expected_sample_sts, c.sample_sts, "unexpected STs received");
        assert_eq!(NUM_SERIES * NUM_DATAPOINTS, c.exemplars, "unexpected number of exemplars");
        assert_eq!(NUM_SERIES * NUM_HISTOGRAMS * 2, c.histograms, "unexpected number of histograms");
        assert_eq!(NUM_SERIES * NUM_HISTOGRAMS * 2, c.float_histograms, "unexpected number of float histograms");
        assert_eq!(expected_histogram_sts, c.histogram_sts, "unexpected histogram STs received");
        assert_eq!(expected_float_histogram_sts, c.float_histogram_sts, "unexpected float histogram STs received");

        // Check that we can still create an AppenderV2.
        // Regression test against https://github.com/prometheus/prometheus/issues/17800.
        let _ = s.appender_v2();
    }
}

#[test]
fn test_rollback_append_v2() {
    const NUM_DATAPOINTS: usize = 1000;
    const NUM_HISTOGRAMS: usize = 100;
    const NUM_SERIES: usize = 8;

    for enable_st_storage in [false, true] {
        let name = "TestRollbackAppendV2";
        let mut opts = default_options();
        opts.enable_st_storage = enable_st_storage;
        let s = create_test_agent_db(opts);
        let mut app = s.appender_v2();

        for lset in labels_for_test(name, NUM_SERIES) {
            for i in 0..NUM_DATAPOINTS {
                let sample = storage::chunks::generate_samples(0, 1);
                append(&mut app, 0, &lset, i as i64, sample[0].t() + 2000, sample[0].f()).unwrap();
            }
        }
        for lset in labels_for_test(&format!("{name}_histogram"), NUM_SERIES) {
            for (i, h) in storage::tsdbutil::generate_test_histograms(NUM_HISTOGRAMS).iter().enumerate() {
                append_h(&mut app, &lset, i as i64, i as i64 + 2000, h).unwrap();
            }
        }
        for lset in labels_for_test(&format!("{name}_custom_buckets_histogram"), NUM_SERIES) {
            for (i, h) in storage::tsdbutil::generate_test_custom_buckets_histograms(NUM_HISTOGRAMS).iter().enumerate()
            {
                append_h(&mut app, &lset, i as i64, i as i64 + 2000, h).unwrap();
            }
        }
        for lset in labels_for_test(&format!("{name}_float_histogram"), NUM_SERIES) {
            for (i, fh) in storage::tsdbutil::generate_test_float_histograms(NUM_HISTOGRAMS).iter().enumerate() {
                append_fh(&mut app, &lset, i as i64, i as i64 + 2000, fh).unwrap();
            }
        }
        for lset in labels_for_test(&format!("{name}_custom_buckets_float_histogram"), NUM_SERIES) {
            for (i, fh) in
                storage::tsdbutil::generate_test_custom_buckets_float_histograms(NUM_HISTOGRAMS).iter().enumerate()
            {
                append_fh(&mut app, &lset, i as i64, i as i64 + 2000, fh).unwrap();
            }
        }

        // Do a rollback, which should clear uncommitted data. A followup call to commit should persist nothing
        // to the WAL.
        assert!(app.rollback().is_none());
        assert!(app.commit().is_none());
        s.close().unwrap();

        let c = count_wal(s.wal.dir());
        for t in &c.types {
            assert!(!matches!(t, Type::Samples | Type::SamplesV2), "should not have found samples");
            assert!(*t != Type::Exemplars, "should not have found exemplars");
            assert!(
                !matches!(
                    t,
                    Type::HistogramSamples
                        | Type::CustomBucketsHistogramSamples
                        | Type::FloatHistogramSamples
                        | Type::CustomBucketsFloatHistogramSamples
                        | Type::HistogramSamplesV2
                        | Type::FloatHistogramSamplesV2
                ),
                "should not have found histograms"
            );
        }
        // Check that only series get stored after calling Rollback.
        assert_eq!(NUM_SERIES * 5, c.series, "series should have been written to WAL");
    }
}

/// The five groups of the truncate and replay tests: every sample at `last_ts`, `num_series` series per group.
#[allow(clippy::too_many_arguments)]
fn append_groups_v2(
    app: &mut AppenderV2,
    name: &str,
    sep: &str,
    suffix: &str,
    num_series: usize,
    num_datapoints: usize,
    num_histograms: usize,
    last_ts: i64,
    commit_each: bool,
) {
    let commit = |app: &mut AppenderV2| {
        if commit_each {
            assert!(app.commit().is_none());
        }
    };
    for lset in labels_for_test(&format!("{name}{sep}{suffix}"), num_series) {
        for _ in 0..num_datapoints {
            append(app, 0, &lset, 0, last_ts, 0.0).unwrap();
        }
        commit(app);
    }
    let sfx = |k: &str| {
        if suffix.is_empty() { format!("{name}_{k}") } else { format!("{name}_{k}_{suffix}") }
    };
    for lset in labels_for_test(&sfx("histogram"), num_series) {
        for h in storage::tsdbutil::generate_test_histograms(num_histograms) {
            append_h(app, &lset, 0, last_ts, &h).unwrap();
        }
        commit(app);
    }
    for lset in labels_for_test(&sfx("custom_buckets_histogram"), num_series) {
        for h in storage::tsdbutil::generate_test_custom_buckets_histograms(num_histograms) {
            append_h(app, &lset, 0, last_ts, &h).unwrap();
        }
        commit(app);
    }
    for lset in labels_for_test(&sfx("float_histogram"), num_series) {
        for fh in storage::tsdbutil::generate_test_float_histograms(num_histograms) {
            append_fh(app, &lset, 0, last_ts, &fh).unwrap();
        }
        commit(app);
    }
    for lset in labels_for_test(&sfx("custom_buckets_float_histogram"), num_series) {
        for fh in storage::tsdbutil::generate_test_custom_buckets_float_histograms(num_histograms) {
            append_fh(app, &lset, 0, last_ts, &fh).unwrap();
        }
        commit(app);
    }
}

#[test]
fn test_full_truncate_wal_append_v2() {
    const NUM_DATAPOINTS: usize = 1000;
    const NUM_HISTOGRAMS: usize = 100;
    const NUM_SERIES: usize = 800;
    const LAST_TS: i64 = 500;

    let mut opts = default_options();
    opts.truncate_frequency = Duration::from_secs(2 * 60);
    let s = create_test_agent_db(opts);
    let mut app = s.appender_v2();
    append_groups_v2(
        &mut app,
        "TestFullTruncateWAL_AppendV2",
        "",
        "",
        NUM_SERIES,
        NUM_DATAPOINTS,
        NUM_HISTOGRAMS,
        LAST_TS,
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
fn test_partial_truncate_wal_append_v2() {
    const NUM_DATAPOINTS: usize = 1000;
    const NUM_SERIES: usize = 800;
    let name = "TestPartialTruncateWAL_AppendV2";

    let s = create_test_agent_db(default_options());
    let mut app = s.appender_v2();

    // Create first batch of 800 series with 1000 data-points with a fixed lastTs as 500.
    let mut last_ts: i64 = 500;
    append_groups_v2(&mut app, name, "", "batch-1", NUM_SERIES, NUM_DATAPOINTS, NUM_DATAPOINTS, last_ts, true);

    // Create second batch of 800 series with 1000 data-points with a fixed lastTs as 600.
    last_ts = 600;
    append_groups_v2(&mut app, name, "", "batch-2", NUM_SERIES, NUM_DATAPOINTS, NUM_DATAPOINTS, last_ts, true);

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

#[test]
fn test_wal_replay_append_v2() {
    const NUM_DATAPOINTS: usize = 1000;
    const NUM_HISTOGRAMS: usize = 100;
    const NUM_SERIES: usize = 8;
    const LAST_TS: i64 = 500;

    for unknown_record in [false, true] {
        let s = create_test_agent_db(default_options());
        if unknown_record {
            // Valid records following an unknown type must still be replayed.
            s.wal.log(&[255, 1, 2, 3]).unwrap();
        }
        let mut app = s.appender_v2();
        let name = format!("TestWALReplay_AppendV2/unknownRecord={unknown_record}");
        append_groups_v2(&mut app, &name, "", "", NUM_SERIES, NUM_DATAPOINTS, NUM_HISTOGRAMS, LAST_TS, false);
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
fn test_existing_wal_next_ref_append_v2() {
    let db_dir = TempDir::new();
    let db = agent::open(Logger::nop(), None, db_dir.path(), Some(default_options())).unwrap();

    let series_count = 10;

    // Append <seriesCount> series.
    let mut app = db.appender_v2();
    for i in 0..series_count {
        let lset = labels::from_strings(&["__name__", &format!("series_{i}")]);
        append(&mut app, 0, &lset, 0, 0, 100.0).unwrap();
    }

    let histogram_count = 10;
    let histograms = storage::tsdbutil::generate_test_histograms(histogram_count);
    // Append <histogramCount> series.
    for (i, h) in histograms.iter().enumerate() {
        let lset = labels::from_strings(&["__name__", &format!("histogram_{i}")]);
        append_h(&mut app, &lset, 0, 0, h).unwrap();
    }
    assert!(app.commit().is_none());

    // Truncate the WAL to force creation of a new segment.
    db.truncate(0).unwrap();
    drop(app);
    db.close().unwrap();
    drop(db);

    // Create a new storage and see what nextRef is initialized to.
    let db = agent::open(Logger::nop(), None, db_dir.path(), Some(default_options())).unwrap();
    assert_eq!(
        (series_count + histogram_count) as u64,
        db.next_ref.load(std::sync::atomic::Ordering::SeqCst),
        "nextRef should be equal to the number of series written across the entire WAL"
    );
    db.close().unwrap();
}

#[test]
fn test_storage_duplicate_exemplars_ignored_append_v2() {
    let s = create_test_agent_db(default_options());
    let mut app = s.appender_v2();

    // Write a few exemplars to our appender and call Commit(). If the labels, value or timestamp are different
    // than the last exemplar, then a new one should be appended; otherwise, it should be skipped.
    let ex = |l: &str, v: &str, value: f64, ts: i64| Exemplar {
        labels: labels::from_strings(&[l, v]),
        value,
        ts,
        has_ts: true,
    };
    let e1 = ex("a", "1", 20.0, 10);
    let e2 = ex("b", "2", 20.0, 10);
    let e3 = ex("b", "2", 42.0, 10);
    let e4 = ex("b", "2", 42.0, 25);

    let exs = [e1.clone(), e1, e2.clone(), e2.clone(), e2, e3.clone(), e3, e4.clone(), e4];
    let o = AppendV2Options {
        exemplars: &exs,
        ..Default::default()
    };
    app.append(0, &labels::from_strings(&["a", "1"]), 0, 0, 0.0, None, None, &o)
        .expect("should not reject valid series");
    assert!(app.commit().is_none());

    // Read back what was written to the WAL.
    // We had 9 calls to AppendExemplar but only 4 of those should have gotten through.
    assert_eq!(4, count_wal(s.wal.dir()).exemplars);
    s.close().unwrap();
}

#[test]
fn test_storage_append_exemplars_append_v2() {
    let s = create_test_agent_db(default_options());

    let mut app = s.appender_v2();
    assert!(app.as_exemplar_appender_v2().is_some());

    // Unknown series should return storage.ErrNotFound.
    let err = app
        .append_exemplars(
            0,
            &labels::from_strings(&["a", "unknown"]),
            &[Exemplar {
                labels: labels::from_strings(&["id", "1"]),
                value: 1.0,
                ts: 10,
                has_ts: false,
            }],
        )
        .unwrap_err();
    assert!(err.is(&ERR_NOT_FOUND));

    let lset = labels::from_strings(&["a", "1"]);
    let r = append(&mut app, 0, &lset, 0, 10, 1.0).unwrap();

    // Empty exemplars slice should still validate series and return SeriesRef.
    let ret_ref = app.append_exemplars(r, &lset, &[]).unwrap();
    assert_eq!(r, ret_ref);

    let e1 = Exemplar {
        labels: labels::from_strings(&["id", "1"]),
        value: 20.0,
        ts: 10,
        has_ts: true,
    };
    let e2 = Exemplar {
        labels: labels::from_strings(&["id", "2"]),
        value: 42.0,
        ts: 25,
        has_ts: true,
    };

    // Append via ref.
    let ret_ref = app.append_exemplars(r, &Labels::default(), &[e1.clone(), e1]).unwrap();
    assert_eq!(r, ret_ref);

    // Append via labels lookup (with empty label value that gets canonicalized).
    let ret_ref = app.append_exemplars(0, &labels::from_strings(&["a", "1", "empty", ""]), &[e2]).unwrap();
    assert_eq!(r, ret_ref);

    assert!(app.commit().is_none());

    assert_eq!(2, count_wal(s.wal.dir()).exemplars);
    s.close().unwrap();
}

/// The float and histogram groups of TestDBAllowOOOSamples_AppendV2. `float_name` is the label prefix of the
/// float series (Go reuses the `_histogram` prefix in the second phase).
fn append_ooo_groups(
    app: &mut AppenderV2,
    name: &str,
    float_name: &str,
    num_series: usize,
    range: std::ops::Range<i64>,
    offset: i64,
    num_histograms: usize,
) {
    for lset in labels_for_test(float_name, num_series) {
        for i in range.clone() {
            let ex = [Exemplar {
                labels: lset.clone(),
                ts: i * 2,
                value: i as f64,
                has_ts: true,
            }];
            let o = AppendV2Options {
                exemplars: &ex,
                ..Default::default()
            };
            app.append(0, &lset, 0, i, i as f64, None, None, &o).unwrap();
        }
    }
    let idx = |i: i64| (i - offset) as usize;
    for lset in labels_for_test(&format!("{name}_histogram"), num_series) {
        let hs = storage::tsdbutil::generate_test_histograms(num_histograms);
        for i in range.clone() {
            append_h(app, &lset, 0, i, &hs[idx(i)]).unwrap();
        }
    }
    for lset in labels_for_test(&format!("{name}_custom_buckets_histogram"), num_series) {
        let hs = storage::tsdbutil::generate_test_custom_buckets_histograms(num_histograms);
        for i in range.clone() {
            append_h(app, &lset, 0, i, &hs[idx(i)]).unwrap();
        }
    }
    for lset in labels_for_test(&format!("{name}_float_histogram"), num_series) {
        let fhs = storage::tsdbutil::generate_test_float_histograms(num_histograms);
        for i in range.clone() {
            append_fh(app, &lset, 0, i, &fhs[idx(i)]).unwrap();
        }
    }
    for lset in labels_for_test(&format!("{name}_custom_buckets_float_histogram"), num_series) {
        let fhs = storage::tsdbutil::generate_test_custom_buckets_float_histograms(num_histograms);
        for i in range.clone() {
            append_fh(app, &lset, 0, i, &fhs[idx(i)]).unwrap();
        }
    }
}

#[test]
fn test_db_allow_ooo_samples_append_v2() {
    const NUM_DATAPOINTS: i64 = 5;
    const NUM_HISTOGRAMS: usize = 5;
    const NUM_SERIES: usize = 4;
    const OFFSET: i64 = 100;
    let name = "TestDBAllowOOOSamples_AppendV2";

    let mut opts = default_options();
    opts.out_of_order_time_window = i64::MAX;
    let s = create_test_agent_db(opts);
    let mut app = s.appender_v2();

    // Let's add some samples in the [offset, offset+numDatapoints) range.
    append_ooo_groups(&mut app, name, name, NUM_SERIES, OFFSET..NUM_DATAPOINTS + OFFSET, OFFSET, NUM_HISTOGRAMS);

    assert!(app.commit().is_none());
    assert_eq!(
        20.0,
        s.metrics.total_appended_samples.float.get(),
        "agent wal mismatch of total appended samples"
    );
    assert_eq!(
        80.0,
        s.metrics.total_appended_samples.histogram.get(),
        "agent wal mismatch of total appended histograms"
    );
    drop(app);
    s.close().unwrap();

    // s.wal.dir() is the /wal subdirectory of the original storage path.
    let storage_dir = s.wal.dir().parent().unwrap().to_path_buf();

    // Replay the storage so that the lastTs for each series is recorded.
    let db = agent::open(s.logger.clone(), None, &storage_dir, Some(s.opts.clone()))
        .unwrap_or_else(|e| panic!("unable to create storage for the agent: {e}"));

    let mut app = db.appender_v2();

    // Now the lastTs will have been recorded successfully. Let's try appending twice as many OOO samples in the
    // [0, numDatapoints) range.
    append_ooo_groups(
        &mut app,
        name,
        &format!("{name}_histogram"),
        NUM_SERIES * 2,
        0..NUM_DATAPOINTS,
        0,
        NUM_HISTOGRAMS,
    );

    assert!(app.commit().is_none());
    assert_eq!(
        40.0,
        db.metrics.total_appended_samples.float.get(),
        "agent wal mismatch of total appended samples"
    );
    assert_eq!(
        160.0,
        db.metrics.total_appended_samples.histogram.get(),
        "agent wal mismatch of total appended histograms"
    );
    db.close().unwrap();
}

#[test]
fn test_db_out_of_order_time_window_append_v2() {
    let tc: [(i64, i64, i64, Option<Error>); 5] = [
        (0, 100, 101, None),
        (0, 100, 100, Some(ERR_OUT_OF_ORDER_SAMPLE)),
        (0, 100, 99, Some(ERR_OUT_OF_ORDER_SAMPLE)),
        (100, 100, 1, None),
        (100, 100, 0, Some(ERR_OUT_OF_ORDER_SAMPLE)),
    ];

    for (out_of_order_time_window, first_ts, second_ts, expected_error) in tc {
        let name = format!(
            "outOfOrderTimeWindow={out_of_order_time_window}, firstTs={first_ts}, secondTs={second_ts}, expectedError={}",
            expected_error.as_ref().map_or("%!s(<nil>)".to_string(), |e| e.to_string())
        );
        let check = |r: Result<u64, Error>| match (&expected_error, r) {
            (None, r) => assert!(r.is_ok(), "{name}: {r:?}"),
            (Some(want), Err(e)) => assert!(e.is(want), "{name}: {e}"),
            (Some(_), Ok(_)) => panic!("{name}: expected an error"),
        };

        let mut opts = default_options();
        opts.out_of_order_time_window = out_of_order_time_window;
        let s = create_test_agent_db(opts);
        let mut app = s.appender_v2();

        let lset = labels_for_test(&format!("{name}_histogram"), 1).remove(0);
        let h = &storage::tsdbutil::generate_test_histograms(1)[0];
        append_h(&mut app, &lset, 0, first_ts, h).unwrap();
        assert!(app.commit().is_none());
        check(append_h(&mut app, &lset, 0, second_ts, h));

        let lset = labels_for_test(&name, 1).remove(0);
        append(&mut app, 0, &lset, 0, first_ts, 0.0).unwrap();
        assert!(app.commit().is_none());
        check(append(&mut app, 0, &lset, 0, second_ts, 0.0));

        let expected_appended_samples = if expected_error.is_some() { 1.0 } else { 2.0 };
        assert_eq!(
            expected_appended_samples,
            s.metrics.total_appended_samples.float.get(),
            "agent wal mismatch of total appended samples"
        );
        assert_eq!(
            expected_appended_samples,
            s.metrics.total_appended_samples.histogram.get(),
            "agent wal mismatch of total appended histograms"
        );
        drop(app);
        s.close().unwrap();
    }
}

/// Replaces TestDBStartTimestampSamplesIngestion.
#[test]
fn test_db_enable_st_zero_injection_append_v2() {
    struct AppendableSample {
        st: i64,
        t: i64,
        v: f64,
        lbls: Labels,
        h: Option<Histogram>,
    }

    let test_histograms = storage::tsdbutil::generate_test_histograms(2);
    let zero_histogram = Histogram {
        // The STZeroSample represents a counter reset by definition.
        counter_reset_hint: CounterResetHint::CounterReset,
        // Replicate other fields to avoid needless chunk creation.
        schema: test_histograms[0].schema,
        zero_threshold: test_histograms[0].zero_threshold,
        custom_values: test_histograms[0].custom_values.clone(),
        ..Default::default()
    };

    let def_lbls = labels_for_test("TestDB_EnableSTZeroInjection_AppendV2", 1).remove(0);
    let inp = |t: i64, st: i64, v: f64, h: Option<&Histogram>| AppendableSample {
        st,
        t,
        v,
        lbls: def_lbls.clone(),
        h: h.cloned(),
    };
    let fs = |t: i64, f: f64| WalSample {
        t,
        f,
        lbls: def_lbls.clone(),
        r#ref: 1,
        ..Default::default()
    };
    let hs = |t: i64, h: &Histogram| WalSample {
        t,
        h: Some(Arc::new(h.clone())),
        lbls: def_lbls.clone(),
        r#ref: 1,
        ..Default::default()
    };

    let test_cases: Vec<(&str, Vec<AppendableSample>, Vec<WalSample>)> = vec![
        (
            "in order ct+normal sample/floatSamples",
            vec![inp(100, 1, 10.0, None), inp(101, 1, 10.0, None)],
            vec![fs(1, 0.0), fs(100, 10.0), fs(101, 10.0)],
        ),
        (
            "ST+float && ST+histogram samples",
            vec![inp(100, 30, 20.0, None), inp(300, 230, 0.0, Some(&test_histograms[0]))],
            vec![fs(30, 0.0), fs(100, 20.0), hs(230, &zero_histogram), hs(300, &test_histograms[0])],
        ),
        (
            "ST+float && ST+histogram samples with error; should be ignored",
            vec![
                // Invalid ST.
                inp(100, 100, 10.0, None),
                // Invalid ST histogram.
                inp(300, 300, 0.0, Some(&test_histograms[0])),
            ],
            vec![fs(100, 10.0), hs(300, &test_histograms[0])],
        ),
        (
            "In order ct+normal sample/histogram",
            vec![inp(100, 1, 0.0, Some(&test_histograms[0])), inp(101, 1, 0.0, Some(&test_histograms[1]))],
            vec![hs(1, &zero_histogram), hs(100, &test_histograms[0]), hs(101, &test_histograms[1])],
        ),
        (
            "ct+normal then OOO sample/float",
            vec![
                inp(60_000, 40_000, 10.0, None),
                inp(120_000, 40_000, 10.0, None),
                inp(180_000, 40_000, 10.0, None),
                inp(50_000, 40_000, 10.0, None),
            ],
            vec![
                fs(40_000, 0.0),
                fs(60_000, 10.0),
                fs(120_000, 10.0),
                fs(180_000, 10.0),
                fs(50_000, 10.0), // OOO sample.
            ],
        ),
    ];

    for (name, input_samples, expected_samples) in test_cases {
        let mut opts = default_options();
        opts.out_of_order_time_window = 360_000;
        opts.enable_st_as_zero_sample = true;
        let s = create_test_agent_db(opts);

        for sample in &input_samples {
            // Simulate one sample per series logic we have in all our ingestion paths in Prometheus.
            let mut app = s.appender_v2();
            app.append(0, &sample.lbls, sample.st, sample.t, sample.v, sample.h.as_ref(), None, &no_opts())
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            assert!(app.commit().is_none(), "{name}");
        }

        // Close the DB to ensure all data is flushed to the WAL.
        s.close().unwrap();

        // adapted: Go then checks that the gathered registry has no prometheus_agent_out_of_order_samples_total
        // family; that check is vacuous upstream because Close unregisters every agent metric. There is no
        // registry here, and the counter is not asserted (the OOO case does count one out-of-order sample at
        // commit).

        let got = read_wal_samples(s.wal.dir());
        assert_eq!(expected_samples, got, "{name}");
    }
}

#[test]
fn test_metadata_in_wal_appender_v2() {
    // Feature disabled.
    {
        let mut opts = default_options();
        opts.enable_metadata_wal_records = false;
        let s = create_test_agent_db(opts);

        let mut app = s.appender_v2();
        let lbls = labels::from_strings(&["a", "b"]);
        let m = meta(MetricType::Gauge, "unit_1", "help_1");
        app.append(0, &lbls, 0, 1000, 1.0, None, None, &with_meta(&m)).unwrap();
        assert!(app.commit().is_none());

        let recs = read_test_wal(s.wal.dir());
        assert!(!recs.is_empty());
        for rec in &recs {
            assert!(
                !matches!(rec, Rec::Metadata(_)),
                "unexpected metadata record in WAL when feature is disabled"
            );
        }
    }

    // Stale sample skips metadata.
    {
        let mut opts = default_options();
        opts.enable_metadata_wal_records = true;
        let s = create_test_agent_db(opts);

        let mut app = s.appender_v2();
        let lbls = labels::from_strings(&["a", "b"]);
        let m = meta(MetricType::Gauge, "unit_1", "help_1");
        app.append(0, &lbls, 0, 1000, model::value::stale_nan(), None, None, &with_meta(&m)).unwrap();
        assert!(app.commit().is_none());

        let recs = read_test_wal(s.wal.dir());
        assert!(!recs.is_empty());
        for rec in &recs {
            assert!(!matches!(rec, Rec::Metadata(_)), "stale sample append should not write metadata to WAL");
        }
    }

    // Metadata logged and deduplicated.
    {
        let mut opts = default_options();
        opts.enable_metadata_wal_records = true;
        let s = create_test_agent_db(opts);

        // Add some series so we can attach metadata to them.
        let s1 = labels::from_strings(&["a", "b"]);
        let s2 = labels::from_strings(&["c", "d"]);
        let s3 = labels::from_strings(&["e", "f"]);
        let s4 = labels::from_strings(&["g", "h"]);

        // Add a first round of metadata to the first three series.
        let m1 = meta(MetricType::Gauge, "unit_1", "help_1");
        let m2 = meta(MetricType::Gauge, "unit_2", "help_2");
        let m3 = meta(MetricType::Gauge, "unit_3", "help_3");

        let mut app = s.appender_v2();
        let mut ts = 0i64;
        app.append(0, &s1, 0, ts, 0.0, None, None, &with_meta(&m1)).unwrap();
        app.append(0, &s2, 0, ts, 0.0, None, None, &with_meta(&m2)).unwrap();
        app.append(0, &s3, 0, ts, 0.0, None, None, &with_meta(&m3)).unwrap();
        app.append(0, &s4, 0, ts, 0.0, None, None, &no_opts()).unwrap();
        assert!(app.commit().is_none());

        // Add a replicated metadata entry to the first series (deduplication check), a completely new metadata
        // entry for the fourth series, and a changed metadata entry to the second series.
        let m4 = meta(MetricType::Counter, "unit_4", "help_4");
        let m5 = meta(MetricType::Counter, "unit_5", "help_5");
        let mut app = s.appender_v2();
        ts += 1;
        app.append(0, &s1, 0, ts, 0.0, None, None, &with_meta(&m1)).unwrap();
        app.append(0, &s4, 0, ts, 0.0, None, None, &with_meta(&m4)).unwrap();
        app.append(0, &s2, 0, ts, 0.0, None, None, &with_meta(&m5)).unwrap();
        assert!(app.commit().is_none());

        // Read the WAL to see if the disk storage format is correct.
        let got_metadata_blocks = metadata_records(&read_test_wal(s.wal.dir()));

        let expected_metadata =
            [ref_meta(1, &m1), ref_meta(2, &m2), ref_meta(3, &m3), ref_meta(4, &m4), ref_meta(2, &m5)];
        assert_eq!(2, got_metadata_blocks.len());
        assert_eq!(&expected_metadata[..3], &got_metadata_blocks[0][..]);
        assert_eq!(&expected_metadata[3..], &got_metadata_blocks[1][..]);
    }
}

#[test]
fn test_metadata_assert_in_memory_data_appender_v2() {
    let mut opts = default_options();
    opts.enable_metadata_wal_records = true;
    let s = create_test_agent_db(opts);

    let s1 = labels::from_strings(&["a", "b"]);
    let s2 = labels::from_strings(&["c", "d"]);
    let s3 = labels::from_strings(&["e", "f"]);
    let s4 = labels::from_strings(&["g", "h"]);

    let m1 = meta(MetricType::Gauge, "unit_1", "help_1");
    let m2 = meta(MetricType::Gauge, "unit_2", "help_2");
    let m3 = meta(MetricType::Gauge, "unit_3", "help_3");

    let mut app = s.appender_v2();
    let mut ts = 0i64;
    app.append(0, &s1, 0, ts, 0.0, None, None, &with_meta(&m1)).unwrap();
    app.append(0, &s2, 0, ts, 0.0, None, None, &with_meta(&m2)).unwrap();
    app.append(0, &s3, 0, ts, 0.0, None, None, &with_meta(&m3)).unwrap();
    app.append(0, &s4, 0, ts, 0.0, None, None, &no_opts()).unwrap();
    assert!(app.commit().is_none());

    let series1 = s.series.get_by_hash(s1.hash(), &s1).unwrap();
    let series2 = s.series.get_by_hash(s2.hash(), &s2).unwrap();
    let series3 = s.series.get_by_hash(s3.hash(), &s3).unwrap();
    let series4 = s.series.get_by_hash(s4.hash(), &s4).unwrap();
    assert_eq!(&m1, series1.metadata().value());
    assert_eq!(&m2, series2.metadata().value());
    assert_eq!(&m3, series3.metadata().value());
    assert!(metadata::is_zero_handle(&series4.metadata()));

    // Update metadata.
    let m4 = meta(MetricType::Counter, "unit_4", "help_4");
    let m5 = meta(MetricType::Counter, "unit_5", "help_5");
    let mut app = s.appender_v2();
    ts += 1;
    app.append(0, &s1, 0, ts, 0.0, None, None, &with_meta(&m1)).unwrap();
    app.append(0, &s4, 0, ts, 0.0, None, None, &with_meta(&m4)).unwrap();
    app.append(0, &s2, 0, ts, 0.0, None, None, &with_meta(&m5)).unwrap();
    assert!(app.commit().is_none());

    assert_eq!(&m1, series1.metadata().value());
    assert_eq!(&m5, series2.metadata().value());
    assert_eq!(&m3, series3.metadata().value());
    assert_eq!(&m4, series4.metadata().value());
}

#[test]
fn test_metadata_checkpointing_appender_v2() {
    for in_mem_checkpoint in [false, true] {
        let name = format!("CheckpointFromInMemorySeries={in_mem_checkpoint}");
        let mut opts = default_options();
        opts.enable_metadata_wal_records = true;
        opts.checkpoint_from_in_memory_series = in_mem_checkpoint;
        opts.wal_segment_size = WAL_SEGMENT_SIZE;

        let s = create_test_agent_db(opts.clone());

        let s1 = labels::from_strings(&["a", "b"]);
        let s2 = labels::from_strings(&["c", "d"]);
        let s3 = labels::from_strings(&["e", "f"]);
        let s4 = labels::from_strings(&["g", "h"]);

        let m1 = meta(MetricType::Gauge, "unit_1", "help_1");
        let m2 = meta(MetricType::Gauge, "unit_2", "help_2");
        let m3 = meta(MetricType::Gauge, "unit_3", "help_3");
        let m4 = meta(MetricType::Gauge, "unit_4", "help_4");

        let mut app = s.appender_v2();
        let mut ts = 0i64;
        app.append(0, &s1, 0, ts, 0.0, None, None, &with_meta(&m1)).unwrap();
        app.append(0, &s2, 0, ts, 0.0, None, None, &with_meta(&m2)).unwrap();
        app.append(0, &s3, 0, ts, 0.0, None, None, &with_meta(&m3)).unwrap();
        app.append(0, &s4, 0, ts, 0.0, None, None, &with_meta(&m4)).unwrap();
        assert!(app.commit().is_none());

        // Update metadata for first series.
        let m5 = meta(MetricType::Counter, "unit_5", "help_5");
        let mut app = s.appender_v2();
        ts += 1;
        app.append(0, &s1, 0, ts, 0.0, None, None, &with_meta(&m5)).unwrap();
        assert!(app.commit().is_none());

        // Switch back-and-forth metadata for second series. Since it ended on a new metadata record, we expect m6.
        let m6 = meta(MetricType::Counter, "unit_6", "help_6");

        let mut app = s.appender_v2();
        ts += 1;
        app.append(0, &s2, 0, ts, 0.0, None, None, &with_meta(&m6)).unwrap();
        assert!(app.commit().is_none());

        let mut app = s.appender_v2();
        ts += 1;
        app.append(0, &s2, 0, ts, 0.0, None, None, &with_meta(&m2)).unwrap();
        assert!(app.commit().is_none());

        let mut app = s.appender_v2();
        ts += 1;
        app.append(0, &s2, 0, ts, 0.0, None, None, &with_meta(&m6)).unwrap();
        app.append(0, &s4, 0, ts, 0.0, None, None, &with_meta(&m4)).unwrap();
        assert!(app.commit().is_none());

        // GC series 3 (lastTs=0) in segment 0 so its lastSegment is 0 (will be dropped at checkpoint 0).
        s.gc(1);

        // Advance to segment 1 and GC series 4 (lastTs=4) while keeping series 1 and 2 alive.
        s.wal.next_segment_sync().unwrap();
        let mut app = s.appender_v2();
        ts += 1;
        app.append(0, &s1, 0, ts, 0.0, None, None, &with_meta(&m5)).unwrap();
        app.append(0, &s2, 0, ts, 0.0, None, None, &with_meta(&m6)).unwrap();
        assert!(app.commit().is_none());

        // GC series 4 in segment 1 so its lastSegment is 1 > 0 (must be retained as deletedSeries in checkpoint 0).
        s.gc(ts);

        // Recreate series 4 in segment 1 with updated metadata m5 before checkpointing at segment 0, so series 4
        // is present in both activeSeries (Ref 5, m5) and deletedSeries (Ref 4, m4).
        let mut app = s.appender_v2();
        app.append(0, &s4, 0, ts, 0.0, None, None, &with_meta(&m5)).unwrap();
        assert!(app.commit().is_none());

        // Create a checkpoint at segment 0 using the configured checkpoint strategy.
        let (first, _) = wlog::segments(s.wal.dir()).unwrap();

        if in_mem_checkpoint {
            let deleted = s.deleted.lock().unwrap();
            agent::checkpoint(&Logger::nop(), &s.wal, 0, 1000, s.series.all_series(), deleted_series_iter(&deleted, 0))
                .unwrap();
        } else {
            let keep = s.keep_series_in_wal_checkpoint_fn(0);
            wlog::checkpoint(&s.wal, first, 0, &keep, 0, false, false).unwrap();
        }

        // Confirm checkpoint exists.
        let (cdir, _) = wlog::last_checkpoint(s.wal.dir()).unwrap();

        // Read checkpoint records.
        let mut got_metadata: Vec<RefMetadata> =
            metadata_records(&read_test_wal(&cdir)).into_iter().flatten().collect();
        got_metadata.sort_by_key(|m| m.r#ref);

        // Both checkpoint strategies keep active series 1 and 2, drop series 3 (deleted at segment 0 <= 0), and
        // retain deleted series 4 (deleted at segment 1 > 0) along with its metadata. The in-memory checkpoint
        // also includes the recreated active series 4 (Ref 5) with m5.
        let mut want_metadata = vec![ref_meta(1, &m5), ref_meta(2, &m6), ref_meta(4, &m4)];
        if in_mem_checkpoint {
            want_metadata.push(ref_meta(5, &m5));
        }
        assert_eq!(want_metadata, got_metadata, "{name}");

        if in_mem_checkpoint {
            // Replaying the in-memory checkpoint alone must keep the recreated active series 4's newer metadata
            // (m5) rather than overwriting it with deleted Ref 4's stale metadata (m4).
            let replayed = create_test_agent_db(opts.clone());
            let sr = wlog::new_segments_reader(&cdir).unwrap();
            let mut r = wlog::Reader::new(sr);
            replayed.load_wal(&mut r, &mut HashMap::new(), 0).unwrap();
            r.into_inner().close().unwrap();
            let replayed_s4 = replayed.series.get_by_hash(s4.hash(), &s4).expect("series 4");
            assert_eq!(&m5, replayed_s4.metadata().value(), "{name}");
            replayed.close().unwrap();
        }
        s.close().unwrap();
    }
}
