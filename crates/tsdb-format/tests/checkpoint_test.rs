//! Port of `tsdb/wlog/checkpoint_test.go`.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use model::histogram::{CUSTOM_BUCKETS_SCHEMA, FloatHistogram, Histogram, Span, custom_values};
use model::labels::from_strings;
use tsdb_format::chunks::HeadSeriesRef;
use tsdb_format::record::{
    self, Decoder, Encoder, Logger, RefExemplar, RefFloatHistogramSample, RefHistogramSample,
    RefMetadata, RefSample, RefSeries,
};
use tsdb_format::tombstones::{Interval, Stone};
use tsdb_format::wlog::compression::Type;
use tsdb_format::wlog::{
    Error, FLUSH_THRESHOLD, METADATA_BATCH_SIZE, Reader, WL, checkpoint, checkpoint_dir,
    create_segment, delete_checkpoints, delete_temp_checkpoints, last_checkpoint,
    new_segments_reader, read_min_valid_time, segments,
};

fn temp_dir(name: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let d = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "checkpoint_test-{name}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

/// Go `os.ReadDir` names (sorted by file name).
fn read_dir_names(dir: &Path, only_dirs: bool) -> Vec<String> {
    let mut v: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| !only_dirs || e.file_type().unwrap().is_dir())
        .map(|e| e.file_name().into_string().unwrap())
        .collect();
    v.sort();
    v
}

fn enc_bytes(f: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut b = Vec::new();
    f(&mut b);
    b
}

fn dec() -> Decoder {
    Decoder::new(Logger::nop())
}

fn keep_all(_: HeadSeriesRef) -> bool {
    true
}

#[test]
fn test_last_checkpoint() {
    let dir = temp_dir("last_checkpoint");

    let err = last_checkpoint(&dir).unwrap_err();
    assert!(matches!(err, Error::NotFound), "{err}");
    assert_eq!(record::ERR_NOT_FOUND, err.to_string());

    fs::create_dir_all(dir.join("checkpoint.0000")).unwrap();
    let (s, k) = last_checkpoint(&dir).unwrap();
    assert_eq!(dir.join("checkpoint.0000"), s);
    assert_eq!(0, k);

    fs::create_dir_all(dir.join("checkpoint.xyz")).unwrap();
    let (s, k) = last_checkpoint(&dir).unwrap();
    assert_eq!(dir.join("checkpoint.0000"), s);
    assert_eq!(0, k);

    fs::create_dir_all(dir.join("checkpoint.1")).unwrap();
    let (s, k) = last_checkpoint(&dir).unwrap();
    assert_eq!(dir.join("checkpoint.1"), s);
    assert_eq!(1, k);

    fs::create_dir_all(dir.join("checkpoint.1000")).unwrap();
    let (s, k) = last_checkpoint(&dir).unwrap();
    assert_eq!(dir.join("checkpoint.1000"), s);
    assert_eq!(1000, k);

    fs::create_dir_all(dir.join("checkpoint.99999999")).unwrap();
    let (s, k) = last_checkpoint(&dir).unwrap();
    assert_eq!(dir.join("checkpoint.99999999"), s);
    assert_eq!(99999999, k);

    fs::create_dir_all(dir.join("checkpoint.100000000")).unwrap();
    let (s, k) = last_checkpoint(&dir).unwrap();
    assert_eq!(dir.join("checkpoint.100000000"), s);
    assert_eq!(100000000, k);
}

#[test]
fn test_delete_checkpoints() {
    let dir = temp_dir("delete_checkpoints");

    delete_checkpoints(&dir, 0).unwrap();

    fs::create_dir_all(dir.join("checkpoint.00")).unwrap();
    fs::create_dir_all(dir.join("checkpoint.01")).unwrap();
    fs::create_dir_all(dir.join("checkpoint.02")).unwrap();
    fs::create_dir_all(dir.join("checkpoint.03")).unwrap();

    delete_checkpoints(&dir, 2).unwrap();

    assert_eq!(
        vec!["checkpoint.02", "checkpoint.03"],
        read_dir_names(&dir, false)
    );

    fs::create_dir_all(dir.join("checkpoint.99999999")).unwrap();
    fs::create_dir_all(dir.join("checkpoint.100000000")).unwrap();
    fs::create_dir_all(dir.join("checkpoint.100000001")).unwrap();

    delete_checkpoints(&dir, 100000000).unwrap();

    assert_eq!(
        vec!["checkpoint.100000000", "checkpoint.100000001"],
        read_dir_names(&dir, false)
    );
}

#[test]
fn test_read_min_valid_time_no_checkpoint_yet() {
    let dir = temp_dir("read_min_valid_time_no_checkpoint_yet");

    let mint = read_min_valid_time(&dir).unwrap();
    assert!(
        mint.is_none(),
        "a WAL with no checkpoint yet must have no recoverable min valid time"
    );
    assert_eq!(0, mint.unwrap_or_default());
}

#[test]
fn test_read_min_valid_time_after_checkpoint() {
    let dir = temp_dir("read_min_valid_time_after_checkpoint");
    let w = WL::new(&dir, Type::None).unwrap();

    checkpoint(&w, 0, 1000, &keep_all, 100, false, true).unwrap();

    let mint = read_min_valid_time(&dir).unwrap();
    assert!(mint.is_some());
    assert_eq!(Some(100), mint);
    w.close().unwrap();
}

#[test]
fn test_read_min_valid_time_reflects_only_latest_checkpoint() {
    let dir = temp_dir("read_min_valid_time_reflects_only_latest_checkpoint");
    let w = WL::new(&dir, Type::None).unwrap();

    checkpoint(&w, 0, 1000, &keep_all, 100, false, true).unwrap();

    checkpoint(&w, 1001, 2000, &keep_all, 200, false, true).unwrap();

    // The second checkpoint's own mint must win, and the first checkpoint's carried-forward
    // copy must not have been kept alongside it (there must be exactly one such record).
    let mint = read_min_valid_time(&dir).unwrap();
    assert!(mint.is_some());
    assert_eq!(Some(200), mint);

    let (checkpoint_dir, _) = last_checkpoint(&dir).unwrap();
    let sr = new_segments_reader(&checkpoint_dir).unwrap();
    let dec = dec();
    let mut r = Reader::new(sr);
    let mut min_valid_time_records = 0;
    while r.next() {
        if dec.r#type(r.record()) == record::Type::MinValidTime {
            min_valid_time_records += 1;
        }
    }
    assert!(r.err().is_none());
    assert_eq!(
        1, min_valid_time_records,
        "a checkpoint must carry exactly one min valid time record"
    );
    w.close().unwrap();
}

/// TestCheckpoint_MinValidTimeNeverRegressesAcrossRestarts covers the case a restart
/// introduces: a later checkpoint, in a later process, can legitimately be asked to persist a
/// mint lower than what an earlier checkpoint already recorded. The persisted value must
/// still never regress.
#[test]
fn test_checkpoint_min_valid_time_never_regresses_across_restarts() {
    let dir = temp_dir("checkpoint_min_valid_time_never_regresses_across_restarts");
    let w = WL::new(&dir, Type::None).unwrap();

    checkpoint(&w, 0, 1000, &keep_all, 5000, false, true).unwrap();

    let mint = read_min_valid_time(&dir).unwrap();
    assert!(mint.is_some());
    assert_eq!(Some(5000), mint);

    // A restart happened here: this next checkpoint is free to use a mint lower than 5000.
    checkpoint(&w, 1001, 2000, &keep_all, 100, false, true).unwrap();

    let mint = read_min_valid_time(&dir).unwrap();
    assert!(mint.is_some());
    assert_eq!(
        Some(5000),
        mint,
        "the persisted min valid time must never regress, even across a restart"
    );
    w.close().unwrap();
}

#[test]
fn test_read_min_valid_time_older_checkpoint_without_record() {
    let dir = temp_dir("read_min_valid_time_older_checkpoint_without_record");

    // Simulate a checkpoint written before this record existed: its first (and only) record
    // is a Series record, not a MinValidTime one.
    let w = WL::new(dir.join("checkpoint.0000"), Type::None).unwrap();
    let enc = Encoder::default();
    w.log(&enc_bytes(|b| {
        enc.series(
            &[RefSeries {
                r#ref: 0,
                labels: from_strings(&["a", "b"]),
            }],
            b,
        )
    }))
    .unwrap();
    w.close().unwrap();

    let mint = read_min_valid_time(&dir).unwrap();
    assert!(
        mint.is_none(),
        "an older checkpoint with no min valid time record must not be mistaken for one recording 0"
    );
    assert_eq!(0, mint.unwrap_or_default());
}

fn span(offset: i32, length: u32) -> Span {
    Span { offset, length }
}

#[test]
fn test_checkpoint() {
    let make_histogram = |i: i64| -> Histogram {
        Histogram {
            count: 5 + (i * 4) as u64,
            zero_count: 2 + i as u64,
            zero_threshold: 0.001,
            sum: 18.4 * (i + 1) as f64,
            schema: 1,
            positive_spans: vec![span(0, 2), span(1, 2)],
            positive_buckets: vec![i + 1, 1, -1, 0],
            ..Default::default()
        }
    };
    let make_custom_bucket_histogram = |i: i64| -> Histogram {
        Histogram {
            count: 5 + (i * 4) as u64,
            zero_count: 2 + i as u64,
            zero_threshold: 0.001,
            sum: 18.4 * (i + 1) as f64,
            schema: -53,
            positive_spans: vec![span(0, 2), span(1, 2)],
            custom_values: custom_values(&[0.0, 1.0, 2.0, 3.0, 4.0]),
            ..Default::default()
        }
    };
    let make_float_histogram = |i: i64| -> FloatHistogram {
        FloatHistogram {
            count: 5.0 + (i * 4) as f64,
            zero_count: 2.0 + i as f64,
            zero_threshold: 0.001,
            sum: 18.4 * (i + 1) as f64,
            schema: 1,
            positive_spans: vec![span(0, 2), span(1, 2)],
            positive_buckets: vec![(i + 1) as f64, 1.0, -1.0, 0.0],
            ..Default::default()
        }
    };
    let make_custom_bucket_float_histogram = |i: i64| -> FloatHistogram {
        FloatHistogram {
            count: 5.0 + (i * 4) as f64,
            zero_count: 2.0 + i as f64,
            zero_threshold: 0.001,
            sum: 18.4 * (i + 1) as f64,
            schema: -53,
            positive_spans: vec![span(0, 2), span(1, 2)],
            custom_values: custom_values(&[0.0, 1.0, 2.0, 3.0, 4.0]),
            ..Default::default()
        }
    };

    for enable_st_storage in [false, true] {
        for compress in Type::ALL {
            let name = format!("compress={compress},stStorage={enable_st_storage}");
            let dir = temp_dir("checkpoint");

            let enc = Encoder { enable_st_storage };
            // Create a dummy segment to bump the initial number.
            let seg = create_segment(&dir, 100).unwrap();
            seg.close().unwrap();

            // Manually create checkpoint for 99 and earlier.
            let w = WL::new(dir.join("checkpoint.0099"), compress).unwrap();

            // Add some data we expect to be around later.
            w.log(&enc_bytes(|b| {
                enc.series(
                    &[
                        RefSeries {
                            r#ref: 0,
                            labels: from_strings(&["a", "b", "c", "0"]),
                        },
                        RefSeries {
                            r#ref: 1,
                            labels: from_strings(&["a", "b", "c", "1"]),
                        },
                    ],
                    b,
                )
            }))
            .unwrap();
            // Log an unknown record, that might have come from a future Prometheus version.
            w.log(&[255]).unwrap();
            w.close().unwrap();

            // Start a WAL and write records to it as usual.
            let w = WL::new_size(&dir, 128 * 1024, compress).unwrap();

            let (mut samples_in_wal, mut histograms_in_wal, mut float_histograms_in_wal) =
                (0usize, 0usize, 0usize);
            let mut last: i64 = 0;
            let mut i: i64 = 0;
            loop {
                let (_, n) = segments(w.dir()).unwrap();
                if n >= 106 {
                    break;
                }
                // Write some series initially.
                if i == 0 {
                    let b = enc_bytes(|b| {
                        enc.series(
                            &[
                                RefSeries {
                                    r#ref: 2,
                                    labels: from_strings(&["a", "b", "c", "2"]),
                                },
                                RefSeries {
                                    r#ref: 3,
                                    labels: from_strings(&["a", "b", "c", "3"]),
                                },
                                RefSeries {
                                    r#ref: 4,
                                    labels: from_strings(&["a", "b", "c", "4"]),
                                },
                                RefSeries {
                                    r#ref: 5,
                                    labels: from_strings(&["a", "b", "c", "5"]),
                                },
                            ],
                            b,
                        )
                    });
                    w.log(&b).unwrap();

                    let md = |r: HeadSeriesRef| RefMetadata {
                        r#ref: r,
                        r#type: 0,
                        unit: "unit".to_string(),
                        help: "help".to_string(),
                    };
                    let b = enc_bytes(|b| enc.metadata(&[md(2), md(3), md(4), md(5)], b));
                    w.log(&b).unwrap();
                }
                // Write samples until the WAL has enough segments.
                // Make them have drifting timestamps within a record to see that they
                // get filtered properly.
                // Start times are ignored at encoding time if start time storage is
                // disabled.
                let samples = [
                    RefSample {
                        r#ref: 0,
                        st: last - 1,
                        t: last,
                        v: i as f64,
                    },
                    RefSample {
                        r#ref: 1,
                        st: last + 9999,
                        t: last + 10000,
                        v: i as f64,
                    },
                    RefSample {
                        r#ref: 2,
                        st: last + 19999,
                        t: last + 20000,
                        v: i as f64,
                    },
                    RefSample {
                        r#ref: 3,
                        st: last + 29999,
                        t: last + 30000,
                        v: i as f64,
                    },
                ];
                w.log(&enc_bytes(|b| enc.samples(&samples, b))).unwrap();
                samples_in_wal += 4;
                let h = Arc::new(make_histogram(i));
                let hs = |h: &Arc<Histogram>| {
                    vec![
                        RefHistogramSample {
                            r#ref: 0,
                            st: last - 1,
                            t: last,
                            h: h.clone(),
                        },
                        RefHistogramSample {
                            r#ref: 1,
                            st: last + 9999,
                            t: last + 10000,
                            h: h.clone(),
                        },
                        RefHistogramSample {
                            r#ref: 2,
                            st: last + 19999,
                            t: last + 20000,
                            h: h.clone(),
                        },
                        RefHistogramSample {
                            r#ref: 3,
                            st: last + 29999,
                            t: last + 30000,
                            h: h.clone(),
                        },
                    ]
                };
                let histograms = hs(&h);
                w.log(&enc_bytes(|b| {
                    enc.histogram_samples(&histograms, b);
                }))
                .unwrap();
                histograms_in_wal += 4;
                let cbh = Arc::new(make_custom_bucket_histogram(i));
                let custom_bucket_histograms = hs(&cbh);
                w.log(&enc_bytes(|b| {
                    enc.custom_buckets_histogram_samples(&custom_bucket_histograms, b)
                }))
                .unwrap();
                histograms_in_wal += 4;
                let fhs = |fh: &Arc<FloatHistogram>| {
                    vec![
                        RefFloatHistogramSample {
                            r#ref: 0,
                            st: last - 1,
                            t: last,
                            fh: fh.clone(),
                        },
                        RefFloatHistogramSample {
                            r#ref: 1,
                            st: last + 9999,
                            t: last + 10000,
                            fh: fh.clone(),
                        },
                        RefFloatHistogramSample {
                            r#ref: 2,
                            st: last + 19999,
                            t: last + 20000,
                            fh: fh.clone(),
                        },
                        RefFloatHistogramSample {
                            r#ref: 3,
                            st: last + 29999,
                            t: last + 30000,
                            fh: fh.clone(),
                        },
                    ]
                };
                let fh = Arc::new(make_float_histogram(i));
                let float_histograms = fhs(&fh);
                w.log(&enc_bytes(|b| {
                    enc.float_histogram_samples(&float_histograms, b);
                }))
                .unwrap();
                float_histograms_in_wal += 4;
                let cbfh = Arc::new(make_custom_bucket_float_histogram(i));
                let custom_bucket_float_histograms = fhs(&cbfh);
                w.log(&enc_bytes(|b| {
                    enc.custom_buckets_float_histogram_samples(&custom_bucket_float_histograms, b)
                }))
                .unwrap();
                float_histograms_in_wal += 4;

                let b = enc_bytes(|b| {
                    enc.exemplars(
                        &[RefExemplar {
                            r#ref: 1,
                            t: last,
                            v: i as f64,
                            labels: from_strings(&["trace_id", &format!("trace-{i}")]),
                        }],
                        b,
                    )
                });
                w.log(&b).unwrap();

                // Write changing metadata for each series. In the end, only the latest
                // version should end up in the checkpoint.
                let md = |r: HeadSeriesRef| RefMetadata {
                    r#ref: r,
                    r#type: 0,
                    unit: last.to_string(),
                    help: last.to_string(),
                };
                let b = enc_bytes(|b| enc.metadata(&[md(0), md(1), md(2), md(3)], b));
                w.log(&b).unwrap();

                last += 100;
                i += 1;
            }
            w.close().unwrap();

            let stats = checkpoint(
                &w,
                100,
                106,
                &|x| x % 2 == 0,
                last / 2,
                enable_st_storage,
                false,
            )
            .unwrap();
            w.truncate(107).unwrap();
            delete_checkpoints(w.dir(), 106).unwrap();
            assert_eq!(
                histograms_in_wal + float_histograms_in_wal + samples_in_wal,
                stats.total_samples,
                "{name}"
            );
            assert!(stats.dropped_samples > 0, "{name}");

            // Only the new checkpoint should be left.
            let files = read_dir_names(&dir, false);
            assert_eq!(files.len(), 1, "{name}");
            assert_eq!("checkpoint.00000106", files[0], "{name}");

            let sr = new_segments_reader(&dir.join("checkpoint.00000106")).unwrap();

            let mut dec = dec();
            let mut series: Vec<RefSeries> = Vec::new();
            let mut metadata: Vec<RefMetadata> = Vec::new();
            let mut r = Reader::new(sr);

            let (
                mut samples_in_checkpoint,
                mut histograms_in_checkpoint,
                mut float_histograms_in_checkpoint,
            ) = (0usize, 0usize, 0usize);
            while r.next() {
                let rec = r.record();

                match dec.r#type(rec) {
                    record::Type::Series => {
                        dec.series(rec, &mut series).unwrap();
                    }
                    record::Type::Samples | record::Type::SamplesV2 => {
                        let mut samples = Vec::new();
                        dec.samples(rec, &mut samples).unwrap();
                        for s in &samples {
                            assert!(s.t >= last / 2, "{name}: sample with wrong timestamp");
                            if enable_st_storage {
                                assert_eq!(
                                    s.t - 1,
                                    s.st,
                                    "{name}: sample with wrong start timestamp"
                                );
                            } else {
                                // Start times should have not survived the round trip.
                                assert_eq!(
                                    0, s.st,
                                    "{name}: sample should not have start timestamp"
                                );
                            }
                        }
                        samples_in_checkpoint += samples.len();
                    }
                    record::Type::HistogramSamples
                    | record::Type::CustomBucketsHistogramSamples
                    | record::Type::HistogramSamplesV2 => {
                        let mut histograms = Vec::new();
                        dec.histogram_samples(rec, &mut histograms).unwrap();
                        for h in &histograms {
                            assert!(h.t >= last / 2, "{name}: histogram with wrong timestamp");
                            if enable_st_storage {
                                assert_eq!(
                                    h.t - 1,
                                    h.st,
                                    "{name}: histogram with wrong start timestamp"
                                );
                            } else {
                                assert_eq!(
                                    0, h.st,
                                    "{name}: histogram should not have start timestamp"
                                );
                            }
                        }
                        histograms_in_checkpoint += histograms.len();
                    }
                    record::Type::FloatHistogramSamples
                    | record::Type::CustomBucketsFloatHistogramSamples
                    | record::Type::FloatHistogramSamplesV2 => {
                        let mut float_histograms = Vec::new();
                        dec.float_histogram_samples(rec, &mut float_histograms)
                            .unwrap();
                        for h in &float_histograms {
                            assert!(
                                h.t >= last / 2,
                                "{name}: float histogram with wrong timestamp"
                            );
                            if enable_st_storage {
                                assert_eq!(
                                    h.t - 1,
                                    h.st,
                                    "{name}: float histogram with wrong start timestamp"
                                );
                            } else {
                                assert_eq!(
                                    0, h.st,
                                    "{name}: float histogram should not have start timestamp"
                                );
                            }
                        }
                        float_histograms_in_checkpoint += float_histograms.len();
                    }
                    record::Type::Exemplars => {
                        let mut exemplars = Vec::new();
                        dec.exemplars(rec, &mut exemplars).unwrap();
                        for e in &exemplars {
                            assert!(e.t >= last / 2, "{name}: exemplar with wrong timestamp");
                        }
                    }
                    record::Type::Metadata => {
                        dec.metadata(rec, &mut metadata).unwrap();
                    }
                    _ => {}
                }
            }
            assert!(r.err().is_none(), "{name}: {:?}", r.err());
            // Making sure we replayed some samples. We expect >50% samples to be still present.
            let ratio = |a: usize, b: usize| a as f64 / b as f64;
            assert!(ratio(samples_in_checkpoint, samples_in_wal) > 0.5, "{name}");
            assert!(ratio(samples_in_checkpoint, samples_in_wal) < 0.8, "{name}");
            assert!(
                ratio(histograms_in_checkpoint, histograms_in_wal) > 0.5,
                "{name}"
            );
            assert!(
                ratio(histograms_in_checkpoint, histograms_in_wal) < 0.8,
                "{name}"
            );
            assert!(
                ratio(float_histograms_in_checkpoint, float_histograms_in_wal) > 0.5,
                "{name}"
            );
            assert!(
                ratio(float_histograms_in_checkpoint, float_histograms_in_wal) < 0.8,
                "{name}"
            );

            let expected_ref_series = vec![
                RefSeries {
                    r#ref: 0,
                    labels: from_strings(&["a", "b", "c", "0"]),
                },
                RefSeries {
                    r#ref: 2,
                    labels: from_strings(&["a", "b", "c", "2"]),
                },
                RefSeries {
                    r#ref: 4,
                    labels: from_strings(&["a", "b", "c", "4"]),
                },
            ];
            assert_eq!(expected_ref_series, series, "{name}");

            let lm = (last - 100).to_string();
            let expected_ref_metadata = vec![
                RefMetadata {
                    r#ref: 0,
                    r#type: 0,
                    unit: lm.clone(),
                    help: lm.clone(),
                },
                RefMetadata {
                    r#ref: 2,
                    r#type: 0,
                    unit: lm.clone(),
                    help: lm.clone(),
                },
                RefMetadata {
                    r#ref: 4,
                    r#type: 0,
                    unit: "unit".to_string(),
                    help: "help".to_string(),
                },
            ];
            metadata.sort_by_key(|m| m.r#ref);
            assert_eq!(expected_ref_metadata, metadata, "{name}");
            fs::remove_dir_all(&dir).unwrap();
        }
    }
}

/// TestCheckpointMetadataAcrossBatches checkpoints more series than a single
/// metadata record covers, so the metadata is written as several records.
#[test]
fn test_checkpoint_metadata_across_batches() {
    let num_series = 2 * METADATA_BATCH_SIZE + 1;

    let dir = temp_dir("checkpoint_metadata_across_batches");
    let enc = Encoder::default();

    let w = WL::new_size(&dir, 8 * 1024 * 1024, Type::None).unwrap();

    let mut expected = Vec::with_capacity(num_series);
    for i in 0..num_series {
        let r = i as HeadSeriesRef;
        let m = RefMetadata {
            r#ref: r,
            r#type: 0,
            unit: "seconds".to_string(),
            help: "x".repeat(FLUSH_THRESHOLD / METADATA_BATCH_SIZE) + &i.to_string(),
        };
        w.log_batch(&[
            enc_bytes(|b| {
                enc.series(
                    &[RefSeries {
                        r#ref: r,
                        labels: from_strings(&["a", &i.to_string()]),
                    }],
                    b,
                )
            }),
            enc_bytes(|b| enc.metadata(std::slice::from_ref(&m), b)),
        ])
        .unwrap();
        expected.push(m);
    }
    let (first, last) = segments(&dir).unwrap();
    w.close().unwrap();

    let stats = checkpoint(&w, first, last, &keep_all, 0, false, false).unwrap();
    assert_eq!(num_series, stats.total_metadata);

    let sr = new_segments_reader(&checkpoint_dir(&dir, last)).unwrap();

    let dec = dec();
    let mut got: Vec<RefMetadata> = Vec::new();
    let mut batch_sizes: Vec<usize> = Vec::new();
    let mut r = Reader::new(sr);
    while r.next() {
        let rec = r.record();
        if dec.r#type(rec) != record::Type::Metadata {
            continue;
        }
        let mut batch = Vec::new();
        dec.metadata(rec, &mut batch).unwrap();
        batch_sizes.push(batch.len());
        got.extend(batch);
    }
    assert!(r.err().is_none());
    assert_eq!(
        vec![METADATA_BATCH_SIZE, METADATA_BATCH_SIZE, 1],
        batch_sizes
    );

    got.sort_by_key(|m| m.r#ref);
    assert_eq!(expected, got);
}

/// TestCheckpoint_Tombstones verifies tombstone retention. A tombstone is dropped
/// together with its series record, or once all its intervals age out of the WAL.
#[test]
fn test_checkpoint_tombstones() {
    let dir = temp_dir("checkpoint_tombstones");

    let seg = create_segment(&dir, 0).unwrap();
    seg.close().unwrap();

    let w = WL::new_size(&dir, 128 * 1024, Type::None).unwrap();
    let enc = Encoder::default();
    let full_range = vec![Interval {
        mint: i64::MIN,
        maxt: i64::MAX,
    }];
    let iv = |mint, maxt| vec![Interval { mint, maxt }];
    w.log(&enc_bytes(|b| {
        enc.tombstones(
            &[
                // Full-range tombstones: kept iff keep(ref).
                Stone {
                    r#ref: 1,
                    intervals: full_range.clone(),
                },
                Stone {
                    r#ref: 2,
                    intervals: full_range.clone(),
                },
                // Finite intervals with keep(ref) = true: kept iff they extend past mint.
                Stone {
                    r#ref: 3,
                    intervals: iv(0, 100),
                },
                Stone {
                    r#ref: 4,
                    intervals: iv(0, 5),
                },
                // Dropped because keep(ref) is false, even though their intervals extend past mint.
                Stone {
                    r#ref: 5,
                    intervals: iv(1000, i64::MAX),
                },
                Stone {
                    r#ref: 6,
                    intervals: iv(0, 100),
                },
            ],
            b,
        )
    }))
    .unwrap();
    let (first, last) = segments(w.dir()).unwrap();
    w.next_segment().unwrap();

    checkpoint(
        &w,
        first,
        last,
        &|id| id == 2 || id == 3 || id == 4,
        10,
        false,
        false,
    )
    .unwrap();
    w.close().unwrap();

    let (cp_dir, _) = last_checkpoint(w.dir()).unwrap();
    let sr = new_segments_reader(&cp_dir).unwrap();

    let dec = dec();
    let mut r = Reader::new(sr);
    let mut stones: Vec<Stone> = Vec::new();
    while r.next() {
        let rec = r.record();
        if dec.r#type(rec) == record::Type::Tombstones {
            dec.tombstones(rec, &mut stones).unwrap();
        }
    }
    assert!(r.err().is_none());

    let expected = vec![
        // Refs 1, 5, and 6 are dropped by keep(ref); ref 4 aged out.
        Stone {
            r#ref: 2,
            intervals: full_range.clone(),
        },
        Stone {
            r#ref: 3,
            intervals: iv(0, 100),
        },
    ];
    assert_eq!(expected, stones);
}

/// TestCheckpointV2HistogramsToV1 verifies that when a WAL contains V2 histogram
/// records (where exponential and custom-bucket histograms are interleaved in a
/// single record) and Checkpoint is asked to re-encode them using the V1 encoder,
/// the custom-bucket histograms returned as leftover by the V1 encoder are not
/// dropped. They must be re-encoded as a separate CustomBuckets(Float)Histogram
/// record in the checkpoint.
#[test]
fn test_checkpoint_v2_histograms_to_v1() {
    let exp_h = Arc::new(Histogram {
        count: 5,
        zero_count: 2,
        zero_threshold: 0.001,
        sum: 18.4,
        schema: 1,
        positive_spans: vec![span(0, 2), span(1, 2)],
        positive_buckets: vec![1, 1, -1, 0],
        ..Default::default()
    });
    let cb_h = Arc::new(Histogram {
        count: 5,
        zero_count: 2,
        zero_threshold: 0.001,
        sum: 18.4,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![span(0, 2), span(1, 2)],
        positive_buckets: vec![1, 1, -1, 0],
        custom_values: custom_values(&[0.0, 1.0, 2.0, 3.0, 4.0]),
        ..Default::default()
    });

    let dir = temp_dir("checkpoint_v2_histograms_to_v1");

    let enc_v2 = Encoder {
        enable_st_storage: true,
    };
    let w = WL::new_size(&dir, 128 * 1024, Type::None).unwrap();

    w.log(&enc_bytes(|b| {
        enc_v2.series(
            &[
                RefSeries {
                    r#ref: 0,
                    labels: from_strings(&["a", "b", "c", "exp0"]),
                },
                RefSeries {
                    r#ref: 1,
                    labels: from_strings(&["a", "b", "c", "cb1"]),
                },
                RefSeries {
                    r#ref: 2,
                    labels: from_strings(&["a", "b", "c", "exp2"]),
                },
                RefSeries {
                    r#ref: 3,
                    labels: from_strings(&["a", "b", "c", "cb3"]),
                },
            ],
            b,
        )
    }))
    .unwrap();

    // V2 encoder writes exp and custom-bucket histograms into a single
    // HistogramSamplesV2 record (interleaved). Verify there is no leftover.
    let hist_samples = vec![
        RefHistogramSample {
            r#ref: 0,
            st: 0,
            t: 1000,
            h: exp_h.clone(),
        },
        RefHistogramSample {
            r#ref: 1,
            st: 0,
            t: 1000,
            h: cb_h.clone(),
        },
        RefHistogramSample {
            r#ref: 0,
            st: 0,
            t: 2000,
            h: exp_h.clone(),
        },
        RefHistogramSample {
            r#ref: 1,
            st: 0,
            t: 2000,
            h: cb_h.clone(),
        },
    ];
    let mut hist_rec = Vec::new();
    let leftover = enc_v2.histogram_samples(&hist_samples, &mut hist_rec);
    assert!(leftover.is_empty(), "v2 encoder must not return leftover");
    w.log(&hist_rec).unwrap();

    let float_hist_samples: Vec<RefFloatHistogramSample> = hist_samples
        .iter()
        .map(|h| RefFloatHistogramSample {
            r#ref: h.r#ref + 2, // float series live at refs 2 and 3.
            st: 0,
            t: h.t,
            fh: Arc::new(h.h.to_float()),
        })
        .collect();
    let mut float_hist_rec = Vec::new();
    let float_leftover = enc_v2.float_histogram_samples(&float_hist_samples, &mut float_hist_rec);
    assert!(
        float_leftover.is_empty(),
        "v2 encoder must not return leftover"
    );
    w.log(&float_hist_rec).unwrap();

    w.close().unwrap();

    let (_, last) = segments(w.dir()).unwrap();

    // Re-open so Checkpoint can take a read lock on the directory.
    let w = WL::new_size(&dir, 128 * 1024, Type::None).unwrap();

    // Run Checkpoint with V1 encoding (enableSTStorage=false) to force the
    // V1 leftover path in checkpoint.go.
    let stats = checkpoint(&w, 0, last, &keep_all, 0, false, false).unwrap();
    assert_eq!(
        hist_samples.len() + float_hist_samples.len(),
        stats.total_samples
    );
    assert_eq!(
        0, stats.dropped_samples,
        "no histogram samples should be dropped"
    );

    let cp_dir = checkpoint_dir(w.dir(), last);
    let sr = new_segments_reader(&cp_dir).unwrap();

    let dec = dec();
    let mut r = Reader::new(sr);

    // For each V1 record type we expect to see exactly 2 samples for a
    // specific series ref, with the matching UsesCustomBuckets flag.
    struct Expectation {
        r#ref: HeadSeriesRef,
        uses_cb: bool,
        seen: usize,
        samples: usize,
    }
    let e = |r, uses_cb| Expectation {
        r#ref: r,
        uses_cb,
        seen: 0,
        samples: 0,
    };
    let mut expects: HashMap<record::Type, Expectation> = HashMap::from([
        (record::Type::HistogramSamples, e(0, false)),
        (record::Type::CustomBucketsHistogramSamples, e(1, true)),
        (record::Type::FloatHistogramSamples, e(2, false)),
        (record::Type::CustomBucketsFloatHistogramSamples, e(3, true)),
    ]);

    while r.next() {
        let rec = r.record();
        let typ = dec.r#type(rec);
        let ok = expects.contains_key(&typ);
        match typ {
            record::Type::HistogramSamples | record::Type::CustomBucketsHistogramSamples => {
                let mut hs = Vec::new();
                dec.histogram_samples(rec, &mut hs).unwrap();
                let exp = expects.get_mut(&typ).unwrap();
                exp.seen += 1;
                exp.samples += hs.len();
                for h in &hs {
                    assert_eq!(exp.r#ref, h.r#ref);
                    assert_eq!(exp.uses_cb, h.h.uses_custom_buckets());
                }
            }
            record::Type::FloatHistogramSamples
            | record::Type::CustomBucketsFloatHistogramSamples => {
                let mut fhs = Vec::new();
                dec.float_histogram_samples(rec, &mut fhs).unwrap();
                let exp = expects.get_mut(&typ).unwrap();
                exp.seen += 1;
                exp.samples += fhs.len();
                for h in &fhs {
                    assert_eq!(exp.r#ref, h.r#ref);
                    assert_eq!(exp.uses_cb, h.fh.uses_custom_buckets());
                }
            }
            record::Type::HistogramSamplesV2 | record::Type::FloatHistogramSamplesV2 => {
                panic!("unexpected V2 record in V1 checkpoint: {typ}");
            }
            _ => {
                assert!(!ok, "unhandled expected type {typ}");
            }
        }
    }
    assert!(r.err().is_none());

    for (typ, exp) in &expects {
        assert!(exp.seen > 0, "expected record type {typ} in checkpoint");
        assert_eq!(2, exp.samples, "expected 2 samples in record type {typ}");
    }
    w.close().unwrap();
}

/// Go `filepath.Walk`: every directory under `dir`, including `dir`.
fn walk_dirs(dir: &Path, out: &mut Vec<PathBuf>) {
    out.push(dir.to_path_buf());
    for e in fs::read_dir(dir).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_dir() {
            walk_dirs(&e.path(), out);
        }
    }
}

#[test]
fn test_checkpoint_no_tmp_folder_after_error() {
    for enable_st_storage in [false, true] {
        // Create a new wlog with invalid data.
        let dir = temp_dir("checkpoint_no_tmp_folder_after_error");
        let w = WL::new_size(&dir, 64 * 1024, Type::None).unwrap();
        let enc = Encoder { enable_st_storage };
        w.log(&enc_bytes(|b| {
            enc.series(
                &[RefSeries {
                    r#ref: 0,
                    labels: from_strings(&["a", "b", "c", "2"]),
                }],
                b,
            )
        }))
        .unwrap();
        w.close().unwrap();

        // Corrupt data.
        let f = OpenOptions::new()
            .write(true)
            .open(w.dir().join("00000000"))
            .unwrap();
        f.write_all_at(&[42], 1).unwrap();
        drop(f);

        // Run the checkpoint and since the wlog contains corrupt data this should return an error.
        // adapted: Go passes a nil keep func (never called before the corruption error); Rust has no nil
        // function, so this one fails the test if it is called.
        let r = checkpoint(
            &w,
            0,
            1,
            &|_| panic!("keep must not be called"),
            0,
            enable_st_storage,
            false,
        );
        assert!(r.is_err(), "enableSTStorage={enable_st_storage}");

        // Walk the wlog dir to make sure there are no tmp folder left behind after the error.
        let mut dirs = Vec::new();
        walk_dirs(w.dir(), &mut dirs);
        for d in dirs {
            let name = d.file_name().unwrap().to_str().unwrap();
            assert!(
                !name.ends_with(".tmp"),
                "wlog dir contains temporary folder:{name}"
            );
        }
    }
}

#[test]
fn test_checkpoint_deletes_temporary_checkpoints() {
    let dir = temp_dir("checkpoint_deletes_temporary_checkpoints");

    // Create one tmp checkpoint directory
    fs::create_dir_all(dir.join("checkpoint.00001000.tmp")).unwrap();

    let w = WL::new(&dir, Type::None).unwrap();

    checkpoint(&w, 0, 1000, &keep_all, 1000, false, false).unwrap();

    assert_eq!(vec!["checkpoint.00001000"], read_dir_names(&dir, true));
    w.close().unwrap();
}

#[test]
fn test_delete_temp_checkpoints() {
    let test_cases: Vec<(&str, Vec<&str>, Vec<&str>)> = vec![
        ("no tmp checkpoints", vec![], vec![]),
        (
            "one tmp checkpoint",
            vec!["checkpoint.00001000.tmp"],
            vec![],
        ),
        (
            "many tmp checkpoints",
            vec![
                "checkpoint.00000001.tmp",
                "checkpoint.00001000.tmp",
                "checkpoint.00002000.tmp",
            ],
            vec![],
        ),
        (
            "mix of tmp and regular checkpoints",
            vec![
                "checkpoint.00000001",
                "checkpoint.00000001.tmp",
                "checkpoint.00001000.tmp",
                "checkpoint.00002000",
            ],
            vec!["checkpoint.00000001", "checkpoint.00002000"],
        ),
    ];

    for (name, checkpoint_directories_to_create, expected_directories) in test_cases {
        let dir = temp_dir("delete_temp_checkpoints");
        for f in checkpoint_directories_to_create {
            fs::create_dir_all(dir.join(f)).unwrap();
        }

        delete_temp_checkpoints(&dir).unwrap();

        assert_eq!(expected_directories, read_dir_names(&dir, true), "{name}");
    }
}
