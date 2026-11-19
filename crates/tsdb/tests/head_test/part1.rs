//! Port of `tsdb/head_test.go`, lines 1..3805.
//!
//! Rust notes:
//! - The head chunks of a series are a vector (oldest first), not a linked list, and the head chunk count is the
//!   vector length (it cannot drift). Checks of the list links and of a drifted counter are marked `adapted`.
//! - Go `errgroup` and channels are threads with `std::sync::mpsc` channels; a canceled context is an atomic flag.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use chunkenc::{ChunkIterator, ValueType};
use model::exemplar::Exemplar;
use model::labels::{self, Labels, ScratchBuilder};
use model::matcher::{MatchType, Matcher};
use storage::{AppendV2Options, Appender, Context, Error, Sample, SeriesRef, SeriesSet};
use tsdb::head::{Head, HeadOptions, MemChunk, MemSeries, MmappedChunk, OOO_CHUNK_ID_MASK, OooHeadChunk};
use tsdb::head_append::{ChunkOpts, compute_chunk_end_time};
use tsdb::ooo_head::OooChunk;
use tsdb::querier::{BlockBaseQuerier, BlockQuerier, new_block_querier};
use tsdb_format::chunks::{ChunkDiskMapper, ChunkDiskMapperRef, DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_QUEUE_SIZE};
use tsdb_format::index::{StringIter, expand_postings};
use tsdb_format::record::{
    self, Decoder, Encoder, RefExemplar, RefFloatHistogramSample, RefHistogramSample, RefMetadata, RefMmapMarker,
    RefSample, RefSeries,
};
use tsdb_format::tombstones::{Interval, Intervals, MemTombstones, Stone};
use tsdb_format::wlog::{self, WL, compression};

use crate::common::{DEFAULT_BLOCK_DURATION, TempDir, TestHead, fs, new_test_head_with_options, query};

/// Go `defaultIsolationDisabled` (false without the `isolation_disabled` build tag).
const DEFAULT_ISOLATION_DISABLED: bool = false;

/// Go `config.DefaultExemplarsConfig.MaxExemplars`.
const DEFAULT_MAX_EXEMPLARS: i64 = 100000;

const COMPRESSIONS: [compression::Type; 3] = [
    compression::Type::None,
    compression::Type::Snappy,
    compression::Type::Zstd,
];

fn ls(kv: &[&str]) -> Labels {
    labels::from_strings(kv)
}

fn eq(name: &str, value: &str) -> Matcher {
    Matcher::must_new(MatchType::Equal, name, value)
}

fn ctx() -> Context {
    Context::background()
}

/// Go `newTestHeadDefaultOptions`.
fn new_test_head_default_options(chunk_range: i64, ooo_enabled: bool) -> HeadOptions {
    let opts = HeadOptions {
        chunk_range,
        enable_exemplar_storage: true,
        ..HeadOptions::default()
    };
    opts.max_exemplars.store(DEFAULT_MAX_EXEMPLARS, Ordering::Relaxed);
    if ooo_enabled {
        opts.out_of_order_time_window.store(10 * 60 * 1000, Ordering::Relaxed);
    }
    opts
}

/// Go `newTestHead`.
fn new_test_head(chunk_range: i64, compress: compression::Type, ooo_enabled: bool) -> TestHead {
    new_test_head_with_options(
        "head_test",
        compress,
        new_test_head_default_options(chunk_range, ooo_enabled),
    )
}

fn wal_of(h: &TestHead) -> Arc<WL> {
    h.wal.clone().expect("test head has a WAL")
}

fn commit(app: &mut dyn Appender) {
    if let Some(e) = app.commit() {
        panic!("commit: {e}");
    }
}

fn rollback(app: &mut dyn Appender) {
    if let Some(e) = app.rollback() {
        panic!("rollback: {e}");
    }
}

/// Reopens a head on the WAL directory `dir` with Go `DefaultHeadOptions` and the given chunk range.
fn reopen_head(
    wal_dir: &std::path::Path,
    compress: compression::Type,
    chunk_range: i64,
    chunk_dir_root: &std::path::Path,
) -> Arc<Head> {
    let w = Arc::new(WL::new(wal_dir, compress).expect("open wal"));
    let opts = HeadOptions {
        chunk_range,
        chunk_dir_root: chunk_dir_root.to_path_buf(),
        ..HeadOptions::default()
    };
    Head::new(Some(w), None, opts, None).expect("new head")
}

fn new_cdm(dir: &TempDir) -> ChunkDiskMapper {
    ChunkDiskMapper::new(dir.path(), None, DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_QUEUE_SIZE)
        .expect("chunk disk mapper")
}

/// Go `handleChunkWriteError`.
fn handle_chunk_write_error() -> Option<tsdb_format::chunks::WriteCallback> {
    Some(Box::new(|r: Result<(), tsdb_format::chunks::Error>| {
        if let Err(e) = r
            && !matches!(e, tsdb_format::chunks::Error::Closed)
        {
            panic!("{}", e);
        }
    }))
}

// ---------------------------------------------------------------------------------------------------------------
// WAL records (Go `[]any` of record slices).

#[allow(dead_code)] // Go populateTestWL handles all record types.
#[derive(Clone, Debug, PartialEq)]
enum Rec {
    Series(Vec<RefSeries>),
    Samples(Vec<RefSample>),
    Tombstones(Vec<Stone>),
    Exemplars(Vec<RefExemplar>),
    Histograms(Vec<RefHistogramSample>),
    FloatHistograms(Vec<RefFloatHistogramSample>),
    MmapMarkers(Vec<RefMmapMarker>),
    Metadata(Vec<RefMetadata>),
}

fn rs(r: u64, kv: &[&str]) -> RefSeries {
    RefSeries {
        r#ref: r,
        labels: ls(kv),
    }
}

fn rsmp(r: u64, t: i64, v: f64) -> RefSample {
    RefSample { r#ref: r, st: 0, t, v }
}

fn stone(r: u64, mint: i64, maxt: i64) -> Stone {
    Stone {
        r#ref: r,
        intervals: vec![Interval { mint, maxt }],
    }
}

fn rex(r: u64, t: i64, v: f64, kv: &[&str]) -> RefExemplar {
    RefExemplar {
        r#ref: r,
        t,
        v,
        labels: ls(kv),
    }
}

fn rh(r: u64, t: i64) -> RefHistogramSample {
    RefHistogramSample {
        r#ref: r,
        st: 0,
        t,
        h: Arc::default(),
    }
}

fn rfh(r: u64, t: i64) -> RefFloatHistogramSample {
    RefFloatHistogramSample {
        r#ref: r,
        st: 0,
        t,
        fh: Arc::default(),
    }
}

/// Go `populateTestWL`.
fn populate_test_wl(w: &WL, recs: &[Rec], enable_st_storage: bool) {
    let enc = Encoder { enable_st_storage };
    let mut buf = Vec::new();
    for r in recs {
        buf.clear();
        match r {
            Rec::Series(v) => enc.series(v, &mut buf),
            Rec::Samples(v) => enc.samples(v, &mut buf),
            Rec::Tombstones(v) => enc.tombstones(v, &mut buf),
            Rec::Exemplars(v) => enc.exemplars(v, &mut buf),
            Rec::Histograms(v) => {
                let _ = enc.histogram_samples(v, &mut buf);
            }
            Rec::FloatHistograms(v) => {
                let _ = enc.float_histogram_samples(v, &mut buf);
            }
            Rec::MmapMarkers(v) => enc.mmap_markers(v, &mut buf),
            Rec::Metadata(v) => enc.metadata(v, &mut buf),
        }
        w.log(&buf).expect("log record");
    }
}

/// Go `readTestWAL`.
fn read_test_wal(dir: &std::path::Path) -> Vec<Rec> {
    let sr = wlog::new_segments_reader(dir).expect("segments reader");
    let mut dec = Decoder::new(tsdb::Logger::nop());
    let mut r = wlog::Reader::new(sr);
    let mut recs = Vec::new();
    while r.next() {
        let rec = r.record().to_vec();
        match dec.r#type(&rec) {
            record::Type::Series => {
                let mut v = Vec::new();
                dec.series(&rec, &mut v).expect("series");
                recs.push(Rec::Series(v));
            }
            record::Type::Samples | record::Type::SamplesV2 => {
                let mut v = Vec::new();
                dec.samples(&rec, &mut v).expect("samples");
                recs.push(Rec::Samples(v));
            }
            record::Type::HistogramSamples
            | record::Type::CustomBucketsHistogramSamples
            | record::Type::HistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.histogram_samples(&rec, &mut v).expect("histograms");
                recs.push(Rec::Histograms(v));
            }
            record::Type::FloatHistogramSamples
            | record::Type::CustomBucketsFloatHistogramSamples
            | record::Type::FloatHistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.float_histogram_samples(&rec, &mut v).expect("float histograms");
                recs.push(Rec::FloatHistograms(v));
            }
            record::Type::Tombstones => {
                let mut v = Vec::new();
                dec.tombstones(&rec, &mut v).expect("tombstones");
                recs.push(Rec::Tombstones(v));
            }
            record::Type::Metadata => {
                let mut v = Vec::new();
                dec.metadata(&rec, &mut v).expect("metadata");
                recs.push(Rec::Metadata(v));
            }
            record::Type::Exemplars => {
                let mut v = Vec::new();
                dec.exemplars(&rec, &mut v).expect("exemplars");
                recs.push(Rec::Exemplars(v));
            }
            record::Type::MinValidTime => {
                // Internal checkpoint bookkeeping, not WAL content callers of readTestWAL care about.
                dec.min_valid_time(&rec).expect("min valid time");
            }
            t => panic!("unknown record type {t:?}"),
        }
    }
    assert!(r.err().is_none(), "{:?}", r.err());
    recs
}

fn expand_floats(it: &mut dyn ChunkIterator) -> Vec<Sample> {
    let mut x = Vec::new();
    while it.next() == ValueType::Float {
        let (t, v) = it.at();
        x.push(fs(t, v));
    }
    assert!(it.err().is_none());
    x
}

// ---------------------------------------------------------------------------------------------------------------

#[test]
fn test_head_init_appender_race_err_out_of_bounds() {
    let head = new_test_head(DEFAULT_BLOCK_DURATION, compression::Type::None, false);
    head.init(0).unwrap();

    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let append_cycles = 100;

    let canceled = AtomicBool::new(false);
    let barrier = std::sync::Barrier::new(100);
    let first_err: Mutex<Option<String>> = Mutex::new(None);
    std::thread::scope(|sc| {
        for i in 0..100 {
            let (head, canceled, barrier, first_err) = (&head, &canceled, &barrier, &first_err);
            sc.spawn(move || {
                let mut appends = 0;
                barrier.wait();
                let res: Result<(), String> = (|| {
                    while !canceled.load(Ordering::SeqCst) && appends < append_cycles {
                        appends += 1;
                        let mut app = head.appender(&ctx());
                        if let Err(e) = app.append(0, &ls(&["__name__", &i.to_string()]), ts, ts as f64) {
                            return Err(format!("error when appending to head: {e}"));
                        }
                        if let Some(e) = app.rollback() {
                            return Err(e.to_string());
                        }
                    }
                    Ok(())
                })();
                if let Err(e) = res {
                    canceled.store(true, Ordering::SeqCst);
                    first_err.lock().unwrap().get_or_insert(e);
                }
            });
        }
    });
    assert_eq!(None, first_err.into_inner().unwrap());
}

/// TestHead_HighConcurrencyReadAndWrite generates 1000 series with a step of 15s and fills a whole block with
/// samples, while concurrently querying them and verifying the results.
#[test]
fn test_head_high_concurrency_read_and_write() {
    for app_v2 in [false, true] {
        let head = new_test_head(DEFAULT_BLOCK_DURATION, compression::Type::None, false);

        let series_cnt = 1000usize;
        let read_concurrency = 2usize;
        let write_concurrency = 10usize;
        let start_ts = DEFAULT_BLOCK_DURATION as u64; // Start at the second block relative to the unix epoch.
        let qry_range = 5 * 60 * 1000u64;
        let step = 15 * 1000u64;
        let end_ts = start_ts + DEFAULT_BLOCK_DURATION as u64;

        let label_sets: Vec<Labels> = (0..series_cnt).map(|i| ls(&["seriesId", &i.to_string()])).collect();
        head.init(0).unwrap();

        let canceled = AtomicBool::new(false);
        let first_err: Mutex<Option<String>> = Mutex::new(None);
        let fail = |e: String| {
            canceled.store(true, Ordering::SeqCst);
            first_err.lock().unwrap().get_or_insert(e);
        };

        // One channel for each write worker (unbuffered, like the Go channels).
        let mut writer_tx = Vec::new();
        let mut writer_rx = Vec::new();
        for _ in 0..write_concurrency {
            let (tx, rx) = mpsc::sync_channel::<u64>(0);
            writer_tx.push(tx);
            writer_rx.push(rx);
        }
        let (reader_tx, reader_rx) = mpsc::sync_channel::<u64>(0);
        let reader_rx = Arc::new(Mutex::new(reader_rx));
        let worker_ready = std::sync::Barrier::new(write_concurrency + read_concurrency + 1);

        std::thread::scope(|sc| {
            // Start the write workers.
            for (worker_id, rx) in writer_rx.into_iter().enumerate() {
                let (head, label_sets, canceled, fail, worker_ready) =
                    (&head, &label_sets, &canceled, &fail, &worker_ready);
                sc.spawn(move || {
                    let n = series_cnt / write_concurrency;
                    let worker_label_sets = &label_sets[n * worker_id..n * (worker_id + 1)];
                    worker_ready.wait();
                    while !canceled.load(Ordering::SeqCst) {
                        let Ok(ts) = rx.recv() else { return };
                        let r: Result<(), String> = if app_v2 {
                            let mut app = head.appender_v2(&ctx());
                            let mut res = Ok(());
                            for l in worker_label_sets {
                                // We also use the timestamp as the sample value.
                                if let Err(e) =
                                    app.append(0, l, 0, ts as i64, ts as f64, None, None, &AppendV2Options::default())
                                {
                                    res = Err(format!("error when appending (V2) to head: {e}"));
                                    break;
                                }
                            }
                            res.and_then(|_| app.commit().map_or(Ok(()), |e| Err(e.to_string())))
                        } else {
                            let mut app = head.appender(&ctx());
                            let mut res = Ok(());
                            for l in worker_label_sets {
                                if let Err(e) = app.append(0, l, ts as i64, ts as f64) {
                                    res = Err(format!("error when appending to head: {e}"));
                                    break;
                                }
                            }
                            res.and_then(|_| app.commit().map_or(Ok(()), |e| Err(e.to_string())))
                        };
                        if let Err(e) = r {
                            fail(e);
                            return;
                        }
                    }
                });
            }

            // Start the read workers.
            for worker_id in 0..read_concurrency {
                let (head, label_sets, canceled, fail, worker_ready, reader_rx) =
                    (&head, &label_sets, &canceled, &fail, &worker_ready, reader_rx.clone());
                sc.spawn(move || {
                    let mut query_series_ref = (series_cnt / read_concurrency) * worker_id;
                    worker_ready.wait();
                    while !canceled.load(Ordering::SeqCst) {
                        let r = reader_rx.lock().unwrap().recv();
                        let Ok(ts) = r else { return };

                        query_series_ref = (query_series_ref + 1) % series_cnt;
                        let lbls = &label_sets[query_series_ref];
                        let mut lbl = (String::new(), String::new());
                        lbls.range(|n, v| lbl = (n.to_string(), v.to_string()));
                        let q = match new_block_querier(&***head, (ts - qry_range) as i64, ts as i64) {
                            Ok(q) => q,
                            Err(e) => {
                                fail(e.to_string());
                                return;
                            }
                        };
                        let samples = query(q, &[eq(&lbl.0, &lbl.1)]);
                        let res: Result<(), String> = (|| {
                            if samples.len() != 1 {
                                return Err(format!("expected 1 series, got {}", samples.len()));
                            }
                            let series = lbls.to_string();
                            let expect_sample_cnt = qry_range / step + 1;
                            let got = samples.get(&series).map_or(0, |s| s.len());
                            if expect_sample_cnt != got as u64 {
                                return Err(format!("expected {expect_sample_cnt} samples, got {got}"));
                            }
                            for (idx, s) in samples[&series].iter().enumerate() {
                                let expected = ts - qry_range + idx as u64 * step;
                                if s.t != expected as i64 {
                                    return Err(format!("expected sample {idx} to have ts {expected}, got {}", s.t));
                                }
                                if s.f != expected as f64 {
                                    return Err(format!("expected sample {idx} to have value {expected}, got {}", s.f));
                                }
                            }
                            Ok(())
                        })();
                        if let Err(e) = res {
                            fail(e);
                            return;
                        }
                    }
                });
            }
            drop(reader_rx);

            // The coordinator.
            let (canceled, worker_ready) = (&canceled, &worker_ready);
            sc.spawn(move || {
                let mut curr_ts = start_ts;
                worker_ready.wait();
                'outer: while !canceled.load(Ordering::SeqCst) {
                    // Send the current timestamp to each of the writers.
                    for ch in &writer_tx {
                        if ch.send(curr_ts).is_err() {
                            break 'outer;
                        }
                    }
                    // Once data for at least <qryRange> has been ingested, send the current timestamp to the readers.
                    if curr_ts > start_ts + qry_range && reader_tx.send(curr_ts - step).is_err() {
                        break;
                    }
                    curr_ts += step;
                    if curr_ts > end_ts {
                        break;
                    }
                }
                // End of the test: dropping the senders closes all channels to stop the workers.
                drop(writer_tx);
                drop(reader_tx);
            });
        });
        assert_eq!(None, first_err.into_inner().unwrap(), "appV2={app_v2}");
    }
}

#[test]
fn test_head_read_wal() {
    for enable_st_storage in [false, true] {
        for compress in COMPRESSIONS {
            let entries = vec![
                Rec::Series(vec![rs(10, &["a", "1"]), rs(11, &["a", "2"]), rs(100, &["a", "3"])]),
                Rec::Samples(vec![rsmp(0, 99, 1.0), rsmp(10, 100, 2.0), rsmp(100, 100, 3.0)]),
                Rec::Series(vec![
                    rs(50, &["a", "4"]),
                    // This series has two refs pointing to it.
                    rs(101, &["a", "3"]),
                ]),
                Rec::Samples(vec![
                    rsmp(10, 101, 5.0),
                    rsmp(50, 101, 6.0),
                    // Sample for duplicate series record.
                    rsmp(101, 101, 7.0),
                ]),
                Rec::Tombstones(vec![
                    stone(0, 99, 101),
                    // Tombstone for duplicate series record.
                    stone(101, 0, 100),
                ]),
                Rec::Exemplars(vec![
                    rex(10, 100, 1.0, &["trace_id", "asdf"]),
                    // Exemplar for duplicate series record.
                    rex(101, 101, 7.0, &["trace_id", "zxcv"]),
                ]),
                Rec::Metadata(vec![
                    // Metadata for duplicate series record.
                    RefMetadata {
                        r#ref: 101,
                        r#type: record::MetricType::Counter as u8,
                        unit: "foo".into(),
                        help: "total foo".into(),
                    },
                ]),
            ];
            let name = format!("compress={},stStorage={}", compress.as_str(), enable_st_storage);

            let head = new_test_head(1000, compress, false);
            populate_test_wl(&wal_of(&head), &entries, enable_st_storage);

            head.init(i64::MIN).unwrap();
            assert_eq!(101, head.last_series_id.load(Ordering::SeqCst), "{name}");

            let s10 = head.series.get_by_id(10);
            let s11 = head.series.get_by_id(11);
            let s50 = head.series.get_by_id(50);
            let s100 = head.series.get_by_id(100);
            let s101 = head.series.get_by_id(101);

            assert_eq!(ls(&["a", "1"]), s10.as_ref().expect("s10").lset, "{name}");
            assert!(
                s11.is_none(),
                "{name}: series without samples should be garbage collected at head.Init()"
            );
            assert_eq!(ls(&["a", "4"]), s50.as_ref().expect("s50").lset, "{name}");
            assert_eq!(ls(&["a", "3"]), s100.as_ref().expect("s100").lset, "{name}");

            // Duplicate series record should not be written to the head.
            assert!(s101.is_none(), "{name}");
            // But it should have a WAL expiry set.
            assert_eq!(Some(101), head.get_wal_expiry(101), "{name}");
            // Only the duplicate series record should have a WAL expiry set.
            assert_eq!(None, head.get_wal_expiry(50), "{name}");

            // Verify samples and exemplar for series 10.
            let (c, _, _) = s10.as_ref().unwrap().lock().chunk(0, &head.chunk_disk_mapper).unwrap();
            assert_eq!(
                vec![fs(100, 2.0), fs(101, 5.0)],
                expand_floats(&mut c.chunk.iterator(None)),
                "{name}"
            );

            let q = head.exemplar_querier(&ctx()).unwrap();
            let e = q.select(0, 1000, &[vec![eq("a", "1")]]).unwrap();
            assert!(!e.is_empty(), "{name}");
            assert!(!e[0].exemplars.is_empty(), "{name}");
            assert!(
                Exemplar {
                    ts: 100,
                    value: 1.0,
                    labels: ls(&["trace_id", "asdf"]),
                    has_ts: false
                }
                .equals(&e[0].exemplars[0]),
                "{name}"
            );

            // Verify samples for series 50.
            let (c, _, _) = s50.as_ref().unwrap().lock().chunk(0, &head.chunk_disk_mapper).unwrap();
            assert_eq!(vec![fs(101, 6.0)], expand_floats(&mut c.chunk.iterator(None)), "{name}");

            // Verify records for series 100 and its duplicate, series 101. The samples before the new series
            // record should be discarded since a duplicate record is only possible when old samples were compacted.
            let (c, _, _) = s100.as_ref().unwrap().lock().chunk(0, &head.chunk_disk_mapper).unwrap();
            assert_eq!(vec![fs(101, 7.0)], expand_floats(&mut c.chunk.iterator(None)), "{name}");

            let q = head.exemplar_querier(&ctx()).unwrap();
            let e = q.select(0, 1000, &[vec![eq("a", "3")]]).unwrap();
            assert!(!e.is_empty(), "{name}");
            assert!(!e[0].exemplars.is_empty(), "{name}");
            assert!(
                Exemplar {
                    ts: 101,
                    value: 7.0,
                    labels: ls(&["trace_id", "zxcv"]),
                    has_ts: false
                }
                .equals(&e[0].exemplars[0]),
                "{name}"
            );

            let s100 = s100.unwrap();
            let g = s100.lock();
            let meta = g.meta().expect("s100 metadata");
            assert!(!model::metadata::is_zero_handle(meta), "{name}");
            assert_eq!("foo", meta.value().unit, "{name}");
            assert_eq!("total foo", meta.value().help, "{name}");
            drop(g);

            let intervals = head.tombstones.get(s100.r#ref);
            assert_eq!(vec![Interval { mint: 0, maxt: 100 }], intervals, "{name}");
        }
    }
}

#[test]
fn test_head_wal_multi_ref() {
    let mut head = new_test_head(1000, compression::Type::None, false);
    let w = wal_of(&head);
    head.init(0).unwrap();

    let mut app = head.appender(&ctx());
    let ref1 = app.append(0, &ls(&["foo", "bar"]), 100, 1.0).unwrap();
    commit(&mut *app);
    assert_eq!(1.0, head.metrics.chunks_created.get());

    // Add another sample outside chunk range to mmap a chunk.
    let mut app = head.appender(&ctx());
    app.append(0, &ls(&["foo", "bar"]), 1500, 2.0).unwrap();
    commit(&mut *app);
    assert_eq!(2.0, head.metrics.chunks_created.get());

    head.truncate(1600).unwrap();

    let mut app = head.appender(&ctx());
    let ref2 = app.append(0, &ls(&["foo", "bar"]), 1700, 3.0).unwrap();
    commit(&mut *app);
    assert_eq!(3.0, head.metrics.chunks_created.get());

    // Add another sample outside chunk range to mmap a chunk.
    let mut app = head.appender(&ctx());
    app.append(0, &ls(&["foo", "bar"]), 2000, 4.0).unwrap();
    commit(&mut *app);
    assert_eq!(4.0, head.metrics.chunks_created.get());

    assert_ne!(ref1, ref2, "Refs are the same");
    head.close().unwrap();

    let chunk_dir_root = head.opts.chunk_dir_root.clone();
    head.head = reopen_head(w.dir(), compression::Type::None, 1000, &chunk_dir_root);
    head.init(0).unwrap();

    let q = new_block_querier(&*head.head, 0, 2100).unwrap();
    let series = query(q, &[eq("foo", "bar")]);
    // The samples before the new ref should be discarded since Head truncation happens only after compacting
    // the Head.
    let mut exp = BTreeMap::new();
    exp.insert(r#"{foo="bar"}"#.to_string(), vec![fs(1700, 3.0), fs(2000, 4.0)]);
    assert_eq!(exp, series);
    head.close().unwrap();
}

/// Regression test for https://github.com/prometheus/prometheus/issues/10884: the chunks gauge must not go
/// negative after the WAL replay of a stale series deleted by `truncateStaleSeries` and re-created.
#[test]
fn test_head_wal_multi_ref_stale_deletion_chunk_gauge_not_negative() {
    let mut head = new_test_head(1000, compression::Type::None, false);
    let w = wal_of(&head);
    head.init(0).unwrap();

    let lset = ls(&["foo", "bar"]);
    let append_sample = |head: &Head, ts: i64, v: f64| -> SeriesRef {
        let mut app = head.appender(&ctx());
        let r = app.append(0, &lset, ts, v).unwrap();
        commit(&mut *app);
        r
    };

    // Append samples across chunk boundaries to give ref1 3 m-mapped chunks + 1 active head chunk.
    let ref1 = append_sample(&head, 100, 1.0); // nextAt=1000
    append_sample(&head, 1200, 2.0); // >=1000 -> chunk 2, nextAt=2000
    append_sample(&head, 2300, 3.0); // >=2000 -> chunk 3, nextAt=3000
    append_sample(&head, 3400, 4.0); // >=3000 -> chunk 4 (active head)

    // Mark ref1 as stale.
    append_sample(&head, 3500, f64::from_bits(model::value::STALE_NAN));

    // Truncate stale series: removes ref1 from the head and writes a [MinInt64, MaxInt64] tombstone record to
    // the WAL.
    let stale_refs = vec![ref1];
    let fps = head.snapshot_fingerprints(&stale_refs, u64::MAX);
    head.truncate_stale_series(&stale_refs, 3500, &fps).unwrap();

    // Append a single sample with the same labels to create ref2.
    let ref2 = append_sample(&head, 5000, 5.0);
    assert_ne!(ref1, ref2, "refs must differ after stale truncation and recreation");

    head.close().unwrap();

    // Reopen the head to trigger WAL replay.
    let chunk_dir_root = head.opts.chunk_dir_root.clone();
    head.head = reopen_head(w.dir(), compression::Type::None, 1000, &chunk_dir_root);
    head.init(0).unwrap();

    let chunks_gauge = head.metrics.chunks.get();
    assert!(
        chunks_gauge >= 0.0,
        "prometheus_tsdb_head_chunks gauge must not be negative after WAL replay"
    );
    head.close().unwrap();
}

#[test]
fn test_head_wal_checkpoint_multi_ref() {
    struct Case {
        name: &'static str,
        wal_entries: Vec<Rec>,
        expected_wal_expiry: i64,
        wal_truncate_min_t: i64,
        expected_wal_entries: Vec<Rec>,
    }
    let a1 = || Rec::Series(vec![rs(1, &["a", "1"]), rs(2, &["a", "1"])]);
    let cases = vec![
        Case {
            name: "Samples only; keep needed duplicate series record",
            wal_entries: vec![
                a1(),
                Rec::Samples(vec![rsmp(1, 100, 1.0), rsmp(2, 200, 2.0), rsmp(2, 500, 3.0)]),
            ],
            expected_wal_expiry: 500,
            wal_truncate_min_t: 500,
            expected_wal_entries: vec![a1(), Rec::Samples(vec![rsmp(2, 500, 3.0)])],
        },
        Case {
            name: "Tombstones only; keep needed duplicate series record",
            wal_entries: vec![
                a1(),
                Rec::Tombstones(vec![stone(1, 0, 100), stone(2, 0, 200), stone(2, 0, 500)]),
            ],
            expected_wal_expiry: 500,
            wal_truncate_min_t: 500,
            expected_wal_entries: vec![a1(), Rec::Tombstones(vec![stone(2, 0, 500)])],
        },
        Case {
            name: "Exemplars only; keep needed duplicate series record",
            wal_entries: vec![
                a1(),
                Rec::Exemplars(vec![
                    rex(1, 100, 1.0, &["trace_id", "asdf"]),
                    rex(2, 200, 2.0, &["trace_id", "asdf"]),
                    rex(2, 500, 3.0, &["trace_id", "asdf"]),
                ]),
            ],
            expected_wal_expiry: 500,
            wal_truncate_min_t: 500,
            expected_wal_entries: vec![a1(), Rec::Exemplars(vec![rex(2, 500, 3.0, &["trace_id", "asdf"])])],
        },
        Case {
            name: "Histograms only; keep needed duplicate series record",
            wal_entries: vec![a1(), Rec::Histograms(vec![rh(1, 100), rh(2, 200), rh(2, 500)])],
            expected_wal_expiry: 500,
            wal_truncate_min_t: 500,
            expected_wal_entries: vec![a1(), Rec::Histograms(vec![rh(2, 500)])],
        },
        Case {
            name: "Float histograms only; keep needed duplicate series record",
            wal_entries: vec![a1(), Rec::FloatHistograms(vec![rfh(1, 100), rfh(2, 200), rfh(2, 500)])],
            expected_wal_expiry: 500,
            wal_truncate_min_t: 500,
            expected_wal_entries: vec![a1(), Rec::FloatHistograms(vec![rfh(2, 500)])],
        },
        Case {
            name: "All record types; keep needed duplicate series record until last record",
            // Series with 2 refs and samples for both.
            wal_entries: vec![
                a1(),
                Rec::Samples(vec![rsmp(2, 500, 3.0)]),
                Rec::Tombstones(vec![stone(2, 0, 500)]),
                Rec::Exemplars(vec![rex(2, 800, 2.0, &["trace_id", "asdf"])]),
                Rec::Histograms(vec![rh(2, 500)]),
                Rec::FloatHistograms(vec![rfh(2, 500)]),
            ],
            expected_wal_expiry: 800,
            wal_truncate_min_t: 700,
            expected_wal_entries: vec![a1(), Rec::Exemplars(vec![rex(2, 800, 2.0, &["trace_id", "asdf"])])],
        },
        Case {
            name: "All record types; drop expired duplicate series record",
            // Series with 2 refs and samples for both.
            wal_entries: vec![
                a1(),
                Rec::Samples(vec![rsmp(2, 500, 2.0), rsmp(1, 900, 3.0)]),
                Rec::Tombstones(vec![stone(2, 0, 750)]),
                Rec::Exemplars(vec![rex(2, 800, 2.0, &["trace_id", "asdf"])]),
                Rec::Histograms(vec![rh(2, 600)]),
                Rec::FloatHistograms(vec![rfh(2, 700)]),
            ],
            expected_wal_expiry: 800,
            wal_truncate_min_t: 900,
            expected_wal_entries: vec![
                Rec::Series(vec![rs(1, &["a", "1"])]),
                Rec::Samples(vec![rsmp(1, 900, 3.0)]),
            ],
        },
    ];

    for enable_st_storage in [false, true] {
        for tc in &cases {
            let name = format!("{},stStorage={}", tc.name, enable_st_storage);
            let h = new_test_head(1000, compression::Type::None, false);
            let w = wal_of(&h);
            populate_test_wl(&w, &tc.wal_entries, enable_st_storage);
            let (first, _) = wlog::segments(w.dir()).unwrap();

            h.init(0).unwrap();

            assert_eq!(Some(tc.expected_wal_expiry), h.get_wal_expiry(2), "{name}");

            // Each truncation creates a new segment, so attempt truncations until a checkpoint is created.
            loop {
                h.last_wal_truncation_time.store(0, Ordering::SeqCst); // Reset so that it's always time to truncate the WAL.
                h.truncate_wal(tc.wal_truncate_min_t).unwrap();
                let (f, _) = wlog::segments(w.dir()).unwrap();
                if f > first {
                    break;
                }
            }

            // Read test WAL, checkpoint first.
            let (checkpoint_dir, _) = wlog::last_checkpoint(w.dir()).unwrap();
            let mut recs = read_test_wal(&checkpoint_dir);
            recs.extend(read_test_wal(w.dir()));

            assert_eq!(tc.expected_wal_entries, recs, "{name}");
        }
    }
}

#[test]
fn test_head_keep_series_in_wal_checkpoint() {
    let existing_ref: u64 = 1;
    let existing_lbls = ls(&["foo", "bar"]);
    let keep_until: i64 = 10;

    type Prepare = Box<dyn Fn(&Head)>;
    let lb = existing_lbls.clone();
    let cases: Vec<(&str, Option<Prepare>, i64, bool)> = vec![
        (
            "keep series still in the head",
            Some(Box::new(move |h: &Head| {
                h.get_or_create_with_optional_id(existing_ref, lb.hash(), &lb, false)
                    .unwrap();
            })),
            0,
            true,
        ),
        ("keep series with keepUntil > mint", None, keep_until - 1, true),
        ("keep series with keepUntil = mint", None, keep_until, true),
        ("drop series with keepUntil < mint", None, keep_until + 1, false),
        (
            // The WBL addresses this series through a reference that WAL replay mapped onto another one, so mint
            // cannot express how long the record is needed.
            "keep series that the WBL references, past mint",
            Some(Box::new(move |h: &Head| {
                h.pin_wbl_series_refs(&HashSet::from([existing_ref]));
            })),
            keep_until + 1,
            true,
        ),
        (
            "drop series once the WBL reference is released",
            Some(Box::new(move |h: &Head| {
                h.pin_wbl_series_refs(&HashSet::from([existing_ref]));
                h.release_wbl_pinned_series_refs();
            })),
            keep_until + 1,
            false,
        ),
    ];

    for (name, prepare, mint, expected) in cases {
        let h = new_test_head(1000, compression::Type::None, false);
        match prepare {
            Some(p) => p(&h),
            None => h.update_wal_expiry(existing_ref, keep_until),
        }
        let keep = h.keep_series_in_wal_checkpoint_fn(mint);
        assert_eq!(expected, keep(existing_ref), "{name}");
    }
}

#[test]
fn test_head_appenders_metrics() {
    let head = new_test_head(1000, compression::Type::None, false);
    head.init(0).unwrap();

    // First rollback with no samples.
    let mut app = head.appender(&ctx());
    assert_eq!(1.0, head.metrics.active_appenders.get());
    assert_eq!(1.0, head.metrics.appenders_created.get());
    rollback(&mut *app);
    assert_eq!(0.0, head.metrics.active_appenders.get());
    assert_eq!(1.0, head.metrics.appenders_created.get());

    // Then commit with no samples.
    let mut app = head.appender(&ctx());
    commit(&mut *app);
    assert_eq!(0.0, head.metrics.active_appenders.get());
    assert_eq!(2.0, head.metrics.appenders_created.get());

    // Now rollback with one sample.
    let mut app = head.appender(&ctx());
    app.append(0, &ls(&["foo", "bar"]), 100, 1.0).unwrap();
    assert_eq!(1.0, head.metrics.active_appenders.get());
    assert_eq!(3.0, head.metrics.appenders_created.get());
    rollback(&mut *app);
    assert_eq!(0.0, head.metrics.active_appenders.get());
    assert_eq!(3.0, head.metrics.appenders_created.get());

    // Now commit with one sample.
    let mut app = head.appender(&ctx());
    app.append(0, &ls(&["foo", "bar"]), 100, 1.0).unwrap();
    commit(&mut *app);
    assert_eq!(0.0, head.metrics.active_appenders.get());
    assert_eq!(4.0, head.metrics.appenders_created.get());
}

#[test]
fn test_head_race_between_series_creation_and_gc() {
    let head = new_test_head(1000, compression::Type::None, false);
    head.init(0).unwrap();

    const TOTAL_SERIES: usize = 100_000;
    let series: Vec<Labels> = (0..TOTAL_SERIES).map(|i| ls(&["foo", &i.to_string()])).collect();
    let done = AtomicBool::new(false);
    let err: Mutex<Option<String>> = Mutex::new(None);

    std::thread::scope(|sc| {
        sc.spawn(|| {
            let mut app = head.appender(&ctx());
            for s in &series {
                if let Err(e) = app.append(0, s, 100, 1.0) {
                    *err.lock().unwrap() = Some(format!("Failed to append: {e}"));
                    break;
                }
            }
            if let Some(e) = app.commit() {
                *err.lock().unwrap() = Some(format!("Failed to commit: {e}"));
            }
            done.store(true, Ordering::SeqCst);
        });

        // Don't check the flag on all iterations in order to perform more gc iterations and make the race
        // condition more likely.
        let mut i = 1u64;
        while i % 128 != 0 || !done.load(Ordering::SeqCst) {
            head.gc();
            i += 1;
        }
    });
    assert_eq!(None, err.into_inner().unwrap());
    assert_eq!(TOTAL_SERIES, head.num_series() as usize);
}

/// Covers appends to a series that already exists in the head racing with gc():
/// https://github.com/prometheus/prometheus/issues/8365.
#[test]
fn test_head_race_between_existing_series_append_and_gc() {
    const TOTAL_SERIES: usize = 10_000;
    const ROUNDS: i64 = 40;
    const APPENDERS: usize = 16;
    const FIRST_TS: i64 = 1_000_000;
    const TS_STEP: i64 = 10_000;

    let head = new_test_head(1000, compression::Type::None, false);
    head.init(0).unwrap();

    let series: Vec<Labels> = (0..TOTAL_SERIES).map(|i| ls(&["foo", &i.to_string()])).collect();
    let refs: Vec<Mutex<SeriesRef>> = (0..TOTAL_SERIES).map(|_| Mutex::new(0)).collect();

    let mut app = head.appender(&ctx());
    for (i, s) in series.iter().enumerate() {
        *refs[i].lock().unwrap() = app.append(0, s, FIRST_TS, 1.0).unwrap();
    }
    commit(&mut *app);
    assert_eq!(TOTAL_SERIES, head.num_series() as usize);

    let err: Mutex<Option<String>> = Mutex::new(None);
    for round in 1..=ROUNDS {
        let ts = FIRST_TS + round * TS_STEP;

        // Emulate the head truncation that makes existing series collectable in production.
        head.min_time.store(ts, Ordering::SeqCst);
        head.min_valid_time.store(ts, Ordering::SeqCst);

        let done = AtomicBool::new(false);
        let running = std::sync::atomic::AtomicUsize::new(APPENDERS);
        std::thread::scope(|sc| {
            for a in 0..APPENDERS {
                let (head, series, refs, err, running, done) = (&head, &series, &refs, &err, &running, &done);
                sc.spawn(move || {
                    let mut app = head.appender(&ctx());
                    let mut i = a;
                    while i < TOTAL_SERIES {
                        // Append by reference, like the scrape loop does for series it has seen before.
                        let r = *refs[i].lock().unwrap();
                        match app.append(r, &series[i], ts, 1.0) {
                            Ok(r) => *refs[i].lock().unwrap() = r,
                            Err(e) => {
                                *err.lock().unwrap() = Some(format!("Failed to append: {e}"));
                                break;
                            }
                        }
                        i += APPENDERS;
                    }
                    if let Some(e) = app.commit() {
                        *err.lock().unwrap() = Some(format!("Failed to commit: {e}"));
                    }
                    if running.fetch_sub(1, Ordering::SeqCst) == 1 {
                        done.store(true, Ordering::SeqCst);
                    }
                });
            }

            let mut i = 1u64;
            while i % 4 != 0 || !done.load(Ordering::SeqCst) {
                head.gc();
                i += 1;
            }
        });
        assert_eq!(None, *err.lock().unwrap());

        // Every Append() reported success, so no sample may have been dropped.
        assert_eq!(TOTAL_SERIES, head.num_series() as usize, "round {round}");
    }

    for lset in &series {
        let s = head.series.get_by_hash(lset.hash(), lset);
        let s = s.unwrap_or_else(|| panic!("series {lset} is not in the head"));
        assert!(!s.lock().head_chunks.is_empty(), "series {lset} holds no sample");
    }
}

#[test]
fn test_head_can_garbagecollect_series_created_without_samples() {
    let ops: [(&str, fn(&mut dyn Appender) -> Option<Error>); 2] = [
        ("after commit", |app| app.commit()),
        ("after rollback", |app| app.rollback()),
    ];
    for (op, finish_txn) in ops {
        let chunk_range = 60 * 60 * 1000;
        let head = new_test_head(chunk_range, compression::Type::None, true);
        head.init(0).unwrap();

        let first_sample_time = 10 * chunk_range;
        {
            // Append first sample, it should init head max time to firstSampleTime.
            let mut app = head.appender(&ctx());
            app.append(0, &ls(&["lbl", "ok"]), first_sample_time, 1.0).unwrap();
            commit(&mut *app);
            assert_eq!(1, head.num_series(), "{op}");
        }

        // Append a sample in a time range that is not covered by the chunk range: we would create series first
        // and then append no sample.
        let mut app = head.appender(&ctx());
        let invalid_sample_time = first_sample_time - chunk_range;
        let err = app
            .append(0, &ls(&["foo", "bar"]), invalid_sample_time, 2.0)
            .unwrap_err();
        // These are our assumptions, checked to make debugging a failed test easier.
        assert!(
            !err.is(&storage::ERR_OUT_OF_BOUNDS),
            "{op}: failed append shouldn't take the ErrOutOfBounds shortcut"
        );
        assert!(
            err.is(&storage::ERR_TOO_OLD_SAMPLE),
            "{op}: failed append should return ErrTooOldSample, got {err}"
        );
        // Do commit or rollback, depending on what we're testing.
        assert!(finish_txn(&mut *app).is_none(), "{op}");

        // Garbage-collect, since we finished the transaction and series has no samples, it should be collectable.
        head.gc();
        assert_eq!(1, head.num_series(), "{op}");
    }
}

#[test]
fn test_head_unknown_wal_record() {
    let head = new_test_head(1000, compression::Type::None, false);
    let _ = wal_of(&head).log(&[255, 42]);
    head.init(0).unwrap();
}

fn mm(min_time: i64, max_time: i64) -> MmappedChunk {
    MmappedChunk {
        r#ref: ChunkDiskMapperRef(0),
        num_samples: 0,
        encoding: chunkenc::Encoding(0),
        min_time,
        max_time,
    }
}

fn expand(p: tsdb_format::index::BoxPostings<'static>) -> Vec<SeriesRef> {
    let mut p = p;
    expand_postings(&mut *p).unwrap()
}

#[test]
fn test_head_truncate() {
    let h = new_test_head(1000, compression::Type::None, false);
    h.init_time(0);
    let c = ctx();

    let (s1, _) = h.get_or_create(1, &ls(&["a", "1", "b", "1"]), false).unwrap();
    let (s2, _) = h.get_or_create(2, &ls(&["a", "2", "b", "1"]), false).unwrap();
    let (s3, _) = h.get_or_create(3, &ls(&["a", "1", "b", "2"]), false).unwrap();
    let (s4, _) = h.get_or_create(4, &ls(&["a", "2", "b", "2", "c", "1"]), false).unwrap();

    s1.lock().mmapped_chunks = vec![mm(0, 999), mm(1000, 1999), mm(2000, 2999)];
    s2.lock().mmapped_chunks = vec![mm(1000, 1999), mm(2000, 2999), mm(3000, 3999)];
    s3.lock().mmapped_chunks = vec![mm(0, 999), mm(1000, 1999)];
    s4.lock().mmapped_chunks = vec![];

    // Truncation need not be aligned.
    h.truncate(1).unwrap();
    h.truncate(2000).unwrap();

    assert_eq!(
        vec![mm(2000, 2999)],
        h.series.get_by_id(s1.r#ref).unwrap().lock().mmapped_chunks
    );
    assert_eq!(
        vec![mm(2000, 2999), mm(3000, 3999)],
        h.series.get_by_id(s2.r#ref).unwrap().lock().mmapped_chunks
    );
    assert!(h.series.get_by_id(s3.r#ref).is_none());
    assert!(h.series.get_by_id(s4.r#ref).is_none());

    let postings_a1 = expand(h.postings.postings(&c, "a", &["1"]));
    let postings_a2 = expand(h.postings.postings(&c, "a", &["2"]));
    let postings_b1 = expand(h.postings.postings(&c, "b", &["1"]));
    let postings_b2 = expand(h.postings.postings(&c, "b", &["2"]));
    let postings_c1 = expand(h.postings.postings(&c, "c", &["1"]));
    let postings_all = expand(h.postings.postings(&c, "", &[""]));

    assert_eq!(vec![s1.r#ref], postings_a1);
    assert_eq!(vec![s2.r#ref], postings_a2);
    assert_eq!(vec![s1.r#ref, s2.r#ref], postings_b1);
    assert_eq!(vec![s1.r#ref, s2.r#ref], postings_all);
    assert!(postings_b2.is_empty());
    assert!(postings_c1.is_empty());

    let mut iter = h.postings.symbols();
    let mut symbols = Vec::new();
    while iter.next() {
        symbols.push(iter.at().to_string());
    }
    assert_eq!(vec!["" /* from 'all' postings list */, "1", "2", "a", "b"], symbols);

    let mut values: BTreeMap<String, HashSet<String>> = BTreeMap::new();
    for name in h.postings.label_names() {
        let ss = values.entry(name.clone()).or_default();
        for value in h.postings.label_values(&c, &name, None) {
            ss.insert(value);
        }
    }
    let mut exp: BTreeMap<String, HashSet<String>> = BTreeMap::new();
    exp.insert("a".into(), HashSet::from(["1".to_string(), "2".to_string()]));
    exp.insert("b".into(), HashSet::from(["1".to_string()]));
    assert_eq!(exp, values);
}

/// Validate various behaviors brought on by firstChunkID accounting for garbage collected chunks.
#[test]
fn test_mem_series_truncate_chunks() {
    let dir = TempDir::new();
    // This is usually taken from the Head, but passing manually here.
    let cdm = new_cdm(&dir);
    let c_opts = ChunkOpts {
        chunk_disk_mapper: Some(&cdm),
        chunk_range: 2000,
        samples_per_chunk: tsdb::head::DEFAULT_SAMPLES_PER_CHUNK,
        ..Default::default()
    };

    let s = MemSeries::new(ls(&["a", "b"]), 1, 0, DEFAULT_ISOLATION_DISABLED, false);
    let mut g = s.lock();

    let mut i = 0;
    while i < 4000 {
        let (ok, _) = g.append(0, i, i as f64, 0, &c_opts, &s.lset, s.r#ref);
        assert!(ok, "sample append failed");
        i += 5;
    }
    g.mmap_chunks(&cdm, s.r#ref);

    // Check that truncate removes half of the chunks and afterwards that the ID of the last chunk still gives us
    // the same chunk afterwards.
    let count_before = g.mmapped_chunks.len() + 1; // +1 for the head chunk.
    let last_id = g.head_chunk_id(count_before - 1);
    let (last_chunk, _, _) = g.chunk(last_id, &cdm).unwrap();

    let (_chk, _, _) = g.chunk(0, &cdm).unwrap();

    g.truncate_chunks_before(2000, ChunkDiskMapperRef(0));

    assert_eq!(2000, g.mmapped_chunks[0].min_time);
    let err = g.chunk(0, &cdm).unwrap_err();
    assert_eq!(storage::ERR_NOT_FOUND, err, "first chunks not gone");
    assert_eq!(count_before / 2, g.mmapped_chunks.len() + 1); // +1 for the head chunk.
    let (chk, _, _) = g.chunk(last_id, &cdm).unwrap();
    // adapted: Go compares the memChunk structs; here the time range and the chunk bytes.
    assert_eq!(last_chunk.min_time, chk.min_time);
    assert_eq!(last_chunk.max_time, chk.max_time);
    assert_eq!(last_chunk.chunk.bytes(), chk.chunk.bytes());
    drop(g);
    cdm.close().unwrap();
}

#[test]
fn test_mem_series_truncate_chunks_scenarios() {
    const CHUNK_RANGE: i64 = 100;
    const CHUNK_STEP: i64 = 5;

    struct Tc {
        name: &'static str,
        head_chunks: usize,
        mmapped_chunks: usize,
        truncate_before: i64,
        expected_truncated: usize,
        expected_head: usize,
        expected_mmap: usize,
        expected_first_chunk_id: u64,
    }
    let d = |name| Tc {
        name,
        head_chunks: 0,
        mmapped_chunks: 0,
        truncate_before: 0,
        expected_truncated: 0,
        expected_head: 0,
        expected_mmap: 0,
        expected_first_chunk_id: 0,
    };
    let tests = vec![
        Tc {
            truncate_before: CHUNK_RANGE * 10,
            ..d("empty memSeries")
        },
        Tc {
            head_chunks: 1,
            expected_head: 1,
            ..d("single head chunk, not truncated")
        },
        Tc {
            head_chunks: 1,
            truncate_before: CHUNK_RANGE,
            expected_truncated: 1,
            expected_head: 0,
            expected_first_chunk_id: 1,
            ..d("single head chunk, truncated")
        },
        Tc {
            head_chunks: 2,
            expected_head: 2,
            ..d("2 head chunks, not truncated")
        },
        Tc {
            head_chunks: 2,
            truncate_before: CHUNK_RANGE,
            expected_truncated: 1,
            expected_head: 1,
            expected_first_chunk_id: 1,
            ..d("2 head chunks, first truncated")
        },
        Tc {
            head_chunks: 2,
            truncate_before: CHUNK_RANGE * 2,
            expected_truncated: 2,
            expected_head: 0,
            expected_first_chunk_id: 2,
            ..d("2 head chunks, everything truncated")
        },
        Tc {
            head_chunks: 0,
            mmapped_chunks: 3,
            truncate_before: CHUNK_RANGE * 2,
            expected_truncated: 2,
            expected_head: 0,
            expected_mmap: 1,
            expected_first_chunk_id: 2,
            ..d("no head chunks, 3 mmap chunks, second mmap truncated")
        },
        Tc {
            head_chunks: 1,
            mmapped_chunks: 1,
            expected_head: 1,
            expected_mmap: 1,
            ..d("single head chunk, single mmap chunk, not truncated")
        },
        Tc {
            head_chunks: 1,
            mmapped_chunks: 1,
            truncate_before: CHUNK_RANGE,
            expected_truncated: 1,
            expected_head: 1,
            expected_mmap: 0,
            expected_first_chunk_id: 1,
            ..d("single head chunk, single mmap chunk, mmap truncated")
        },
        Tc {
            head_chunks: 5,
            mmapped_chunks: 5,
            truncate_before: CHUNK_RANGE * 7,
            expected_truncated: 7,
            expected_head: 3,
            expected_mmap: 0,
            expected_first_chunk_id: 7,
            ..d("5 head chunk, 5 mmap chunk, third head truncated")
        },
        Tc {
            head_chunks: 2,
            mmapped_chunks: 3,
            truncate_before: CHUNK_RANGE * 2,
            expected_truncated: 2,
            expected_head: 2,
            expected_mmap: 1,
            expected_first_chunk_id: 2,
            ..d("2 head chunks, 3 mmap chunks, second mmap truncated")
        },
    ];

    for tc in tests {
        let name = tc.name;
        let dir = TempDir::new();
        let cdm = new_cdm(&dir);

        let series = MemSeries::new(labels::empty_labels(), 1, 0, true, false);
        let mut g = series.lock();

        let c_opts = ChunkOpts {
            chunk_disk_mapper: Some(&cdm),
            chunk_range: CHUNK_RANGE,
            samples_per_chunk: tsdb::head::DEFAULT_SAMPLES_PER_CHUNK,
            ..Default::default()
        };

        let mut head_start = 0i64;
        if tc.mmapped_chunks > 0 {
            head_start = (tc.mmapped_chunks as i64 + 1) * CHUNK_RANGE;
            let mut i = 0;
            while i < (tc.mmapped_chunks as i64 + 1) * CHUNK_RANGE {
                let (ok, _) = g.append(0, i, i as f64, 0, &c_opts, &series.lset, series.r#ref);
                assert!(ok, "{name}: sample append failed");
                i += CHUNK_STEP;
            }
            g.mmap_chunks(&cdm, series.r#ref);
        }

        if tc.head_chunks == 0 {
            g.head_chunks.clear();
        } else {
            let mut i = head_start;
            while i < CHUNK_RANGE * (tc.mmapped_chunks + tc.head_chunks) as i64 {
                let (ok, _) = g.append(0, i, i as f64, 0, &c_opts, &series.lset, series.r#ref);
                assert!(ok, "{name}: sample append failed: {i}");
                i += CHUNK_STEP;
            }
        }

        if tc.head_chunks > 0 {
            assert_eq!(
                tc.head_chunks,
                g.head_chunks.len(),
                "{name}: wrong number of head chunks"
            );
        } else {
            assert!(g.head_chunks.is_empty(), "{name}: head chunk is present");
        }
        assert_eq!(
            tc.mmapped_chunks,
            g.mmapped_chunks.len(),
            "{name}: wrong number of mmapped chunks"
        );

        // adapted: Go sets headChunkCount here (series.append bypasses observeChunkCreated); the Rust count is
        // the length of the head chunk vector.
        assert_eq!(tc.head_chunks as u32, g.head_chunk_count(), "{name}");

        let truncated = g.truncate_chunks_before(tc.truncate_before, ChunkDiskMapperRef(0));
        assert_eq!(
            tc.expected_truncated, truncated,
            "{name}: wrong number of truncated chunks returned"
        );
        assert_eq!(
            tc.expected_head as u32,
            g.head_chunk_count(),
            "{name}: wrong headChunkCount after truncation"
        );

        assert_eq!(
            tc.expected_mmap,
            g.mmapped_chunks.len(),
            "{name}: wrong number of mmappedChunks after truncation"
        );

        if tc.expected_head > 0 {
            assert_eq!(
                tc.expected_head,
                g.head_chunks.len(),
                "{name}: wrong number of head chunks after truncation"
            );
            // adapted: Go checks that the oldest head chunk has no `prev` link; the vector has no links.
        } else {
            assert!(
                g.head_chunks.is_empty(),
                "{name}: headChunks should be empty after truncation"
            );
        }

        if !g.head_chunks.is_empty() || !g.mmapped_chunks.is_empty() {
            assert!(
                g.max_time() >= tc.truncate_before,
                "{name}: wrong value of series.maxTime() after truncation"
            );
        } else {
            assert_eq!(
                i64::MIN,
                g.max_time(),
                "{name}: wrong value of series.maxTime() after truncation"
            );
        }

        assert_eq!(
            tc.expected_first_chunk_id, g.first_chunk_id,
            "{name}: wrong firstChunkID after truncation"
        );
        drop(g);
        cdm.close().unwrap();
    }

    // "head chunk count mismatch": truncateChunksBefore must advance firstChunkID by the actual number of removed
    // chunks.
    // adapted: the Rust head chunk count is the vector length and cannot drift from the list; the check keeps
    // the same chunks, truncation and expected values.
    {
        let s = MemSeries::new(labels::empty_labels(), 1, 0, true, false);
        let mut g = s.lock();
        // Go `buildHeadChunksLight(5)`: chunks span [0,999]..[4000,4999].
        for i in 0..5i64 {
            g.head_chunks.push(MemChunk {
                chunk: Arc::new(chunkenc::XorChunk::new()),
                min_time: i * 1000,
                max_time: i * 1000 + 999,
            });
        }
        // mint=2000 drops the two oldest.
        let removed = g.truncate_chunks_before(2000, ChunkDiskMapperRef(0));
        assert_eq!(2, removed);
        assert_eq!(2, g.first_chunk_id);
        assert_eq!(3, g.head_chunks.len());
        assert_eq!(3, g.head_chunk_count(), "truncation re-syncs the counter from the walk");
    }
}

#[test]
fn test_ooo_truncate_chunks_before_wrap() {
    struct Tc {
        name: &'static str,
        first_ooo_chunk_id: u64,
        num_ooo_chunks: usize,
        has_ooo_head_chunk: bool,
        truncate_n: usize,
        exp_first_ooo_chunk_id: u64,
        exp_ooo_nil: bool,
        exp_ooo_mmapped_len: usize,
    }
    let tests = vec![
        Tc {
            name: "wrap past zero with remaining chunks",
            first_ooo_chunk_id: OOO_CHUNK_ID_MASK - 3,
            num_ooo_chunks: 5,
            has_ooo_head_chunk: false,
            truncate_n: 4,
            exp_first_ooo_chunk_id: 1,
            exp_ooo_nil: false,
            exp_ooo_mmapped_len: 1,
        },
        Tc {
            name: "truncate all OOO chunks, no head chunk, ooo becomes nil",
            first_ooo_chunk_id: OOO_CHUNK_ID_MASK - 3,
            num_ooo_chunks: 5,
            has_ooo_head_chunk: false,
            truncate_n: 5,
            exp_first_ooo_chunk_id: 0,
            exp_ooo_nil: true,
            exp_ooo_mmapped_len: 0,
        },
        Tc {
            name: "truncate all mmapped, head chunk keeps ooo alive",
            first_ooo_chunk_id: OOO_CHUNK_ID_MASK - 1,
            num_ooo_chunks: 3,
            has_ooo_head_chunk: true,
            truncate_n: 3,
            exp_first_ooo_chunk_id: 2,
            exp_ooo_nil: false,
            exp_ooo_mmapped_len: 0,
        },
    ];

    for tc in tests {
        let name = tc.name;
        let dir = TempDir::new();
        let cdm = new_cdm(&dir);

        let series = MemSeries::new(labels::empty_labels(), 1, 0, true, false);
        let mut g = series.lock();
        g.ooo_or_init().first_ooo_chunk_id = tc.first_ooo_chunk_id;

        let mut refs = Vec::with_capacity(tc.num_ooo_chunks);
        for i in 0..tc.num_ooo_chunks as i64 {
            let r = cdm.write_chunk(
                series.r#ref,
                i * 100,
                i * 100 + 99,
                Arc::new(chunkenc::XorChunk::new()),
                true,
                handle_chunk_write_error(),
            );
            refs.push(r);
            g.ooo_or_init().ooo_mmapped_chunks.push(MmappedChunk {
                r#ref: r,
                num_samples: 0,
                encoding: chunkenc::Encoding(0),
                min_time: i * 100,
                max_time: i * 100 + 99,
            });
        }

        if tc.has_ooo_head_chunk {
            g.ooo_or_init().ooo_head_chunk = Some(OooHeadChunk {
                chunk: OooChunk::new(),
                min_time: tc.num_ooo_chunks as i64 * 100,
                max_time: tc.num_ooo_chunks as i64 * 100 + 99,
            });
        }

        let min_ooo_mmap_ref = refs[tc.truncate_n - 1];
        g.truncate_chunks_before(0, min_ooo_mmap_ref);

        if tc.exp_ooo_nil {
            assert!(
                g.ooo().is_none(),
                "{name}: ooo should be nil after truncating all chunks"
            );
        } else {
            let ooo = g.ooo().unwrap_or_else(|| panic!("{name}: ooo should not be nil"));
            assert_eq!(
                tc.exp_first_ooo_chunk_id, ooo.first_ooo_chunk_id,
                "{name}: firstOOOChunkID mismatch after wrap"
            );
            assert_eq!(
                tc.exp_ooo_mmapped_len,
                ooo.ooo_mmapped_chunks.len(),
                "{name}: oooMmappedChunks length mismatch"
            );
        }
        drop(g);
        cdm.close().unwrap();
    }
}

/// adapted: Go stores `oooChunkIDMask - 1` into headChunkCount; the Rust count is the number of chunks of the
/// series, so the series gets `oooChunkIDMask - 1` (placeholder) m-mapped chunks instead.
fn fill_chunk_id_space(g: &mut tsdb::head::MemSeriesInner) {
    g.mmapped_chunks = vec![mm(0, 0); (OOO_CHUNK_ID_MASK - 1) as usize];
}

/// Go `require.Panics`; the panic must be the chunk ID space check of pushHeadChunk.
fn assert_id_space_panic(r: std::thread::Result<()>) {
    let p = r.expect_err("expected a panic");
    let msg = p
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default();
    assert!(msg.contains("too many in-order head chunks"), "unexpected panic: {msg}");
}

#[test]
fn test_push_head_chunk_panics_at_id_space_bound() {
    let s = MemSeries::new(ls(&["a", "b"]), 1, 0, true, false);
    let mut g = s.lock();
    fill_chunk_id_space(&mut g);

    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        g.push_head_chunk(
            MemChunk {
                chunk: Arc::new(chunkenc::XorChunk::new()),
                min_time: 0,
                max_time: 0,
            },
            &s.lset,
            s.r#ref,
        );
    }));
    assert_id_space_panic(r);
}

#[test]
fn test_append_histogram_layout_change_panics_at_id_space_bound() {
    let head = new_test_head(1000, compression::Type::None, false);
    let l = ls(&["l", "v1"]);

    let mut app = head.appender(&ctx());
    let h = storage::tsdbutil::generate_test_histogram(0);
    app.append_histogram(0, &l, 0, Some(&h), None).unwrap();
    commit(&mut *app);

    let (ms, _) = head.get_or_create(l.hash(), &l, false).unwrap();
    {
        let mut g = ms.lock();
        // adapted: see fill_chunk_id_space. The m-mapped placeholders end before the head chunk.
        g.mmapped_chunks = vec![mm(-1, -1); (OOO_CHUNK_ID_MASK - 1 - g.head_chunks.len() as u64) as usize];
    }

    let mut h2 = h.clone();
    h2.schema += 1;
    let head2 = head.head.clone();
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let mut app = head2.appender(&ctx());
        let _ = app.append_histogram(0, &l, 1, Some(&h2), None);
        let _ = app.commit();
    }));
    assert_id_space_panic(r);
}

#[test]
fn test_head_delete_series_without_samples() {
    for enable_st_storage in [false, true] {
        for compress in COMPRESSIONS {
            let entries = vec![
                Rec::Series(vec![rs(10, &["a", "1"])]),
                Rec::Samples(vec![]),
                Rec::Series(vec![rs(50, &["a", "2"])]),
                Rec::Samples(vec![rsmp(50, 80, 1.0), rsmp(50, 90, 1.0)]),
            ];
            let head = new_test_head(1000, compress, false);
            populate_test_wl(&wal_of(&head), &entries, enable_st_storage);

            head.init(i64::MIN).unwrap();

            head.delete(&ctx(), 0, 100, &[eq("a", "1")]).unwrap();
        }
    }
}

/// Compares a series set with the expected series (Go compares with a `mockSeriesSet`).
fn check_series_set(exp: &[(Labels, Vec<Sample>)], act: &mut dyn SeriesSet, name: &str) {
    let mut i = 0;
    loop {
        let eok = i < exp.len();
        let rok = act.next();
        assert_eq!(eok, rok, "{name}");
        if !eok {
            break;
        }
        let s = act.at().clone();
        assert_eq!(exp[i].0, *s.labels(), "{name}");
        let smpl_res = storage::expand_samples(&mut s.iterator(None));
        let smpl_exp = storage::expand_samples(&mut storage::new_list_series_iterator(&exp[i].1));
        assert_eq!(smpl_exp, smpl_res, "{name}");
        i += 1;
    }
}

#[test]
fn test_head_delete_simple() {
    let build_smpls = |s: &[i64]| -> Vec<Sample> { s.iter().map(|&t| fs(t, t as f64)).collect() };
    let smpls_all = build_smpls(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    let lbls_default = ls(&["a", "b"]);

    struct Case {
        dranges: Intervals,
        add_samples: Vec<Sample>,
        smpls_exp: Vec<Sample>,
    }
    let iv = |mint, maxt| Interval { mint, maxt };
    let cases = vec![
        Case {
            dranges: vec![iv(0, 3)],
            add_samples: vec![],
            smpls_exp: build_smpls(&[4, 5, 6, 7, 8, 9]),
        },
        Case {
            dranges: vec![iv(1, 3)],
            add_samples: vec![],
            smpls_exp: build_smpls(&[0, 4, 5, 6, 7, 8, 9]),
        },
        Case {
            dranges: vec![iv(1, 3), iv(4, 7)],
            add_samples: vec![],
            smpls_exp: build_smpls(&[0, 8, 9]),
        },
        Case {
            dranges: vec![iv(1, 3), iv(4, 700)],
            add_samples: vec![],
            smpls_exp: build_smpls(&[0]),
        },
        // This case is to ensure that labels and symbols are deleted.
        Case {
            dranges: vec![iv(0, 9)],
            add_samples: vec![],
            smpls_exp: build_smpls(&[]),
        },
        Case {
            dranges: vec![iv(1, 3)],
            add_samples: build_smpls(&[11, 13, 15]),
            smpls_exp: build_smpls(&[0, 4, 5, 6, 7, 8, 9, 11, 13, 15]),
        },
        // After delete, the appended samples in the deleted range should be visible as the tombstones are clamped
        // to head min/max time.
        Case {
            dranges: vec![iv(7, 20)],
            add_samples: build_smpls(&[11, 13, 15]),
            smpls_exp: build_smpls(&[0, 1, 2, 3, 4, 5, 6, 11, 13, 15]),
        },
    ];

    for compress in COMPRESSIONS {
        for (ci, c) in cases.iter().enumerate() {
            let name = format!("compress={},case={ci}", compress.as_str());
            let head = new_test_head(1000, compress, false);
            let w = wal_of(&head);
            head.init(0).unwrap();

            let mut app = head.appender(&ctx());
            for s in &smpls_all {
                app.append(0, &lbls_default, s.t, s.f).unwrap();
            }
            commit(&mut *app);

            // Delete the ranges.
            for r in &c.dranges {
                head.delete(&ctx(), r.mint, r.maxt, &[eq("a", "b")]).unwrap();
            }

            // Add more samples.
            let mut app = head.appender(&ctx());
            for s in &c.add_samples {
                app.append(0, &lbls_default, s.t, s.f).unwrap();
            }
            commit(&mut *app);

            // Compare the samples for both heads - before and after the reloadBlocks. Use a new wal to ensure
            // deleted samples are gone even after a reloadBlocks.
            let reloaded_head = reopen_head(w.dir(), compress, 1000, w.dir());
            reloaded_head.init(0).unwrap();

            let exp: Vec<(Labels, Vec<Sample>)> = vec![(lbls_default.clone(), c.smpls_exp.clone())];
            for h in [&head.head, &reloaded_head] {
                let mut q = new_block_querier(&**h, h.min_time(), h.max_time()).unwrap();
                let mut act = q.select(&ctx(), false, None, &[eq("a", "b")]);
                assert!(q.close().is_none());
                check_series_set(&exp, &mut *act, &name);
                h.close().unwrap();
                assert!(act.err().is_none(), "{name}");
                assert!(act.warnings().is_empty(), "{name}");
            }
        }
    }
}

#[test]
fn test_delete_until_cur_max() {
    let hb = new_test_head(1000000, compression::Type::None, false);

    let num_samples = 10i64;
    let mut app = hb.appender(&ctx());
    for i in 0..num_samples {
        let v = crate::common::rand_f64();
        app.append(0, &ls(&["a", "b"]), i, v).unwrap();
    }
    commit(&mut *app);
    hb.delete(&ctx(), 0, 10000, &[eq("a", "b")]).unwrap();

    // Test the series returns no samples. The series is cleared only after compaction.
    let q = new_block_querier(&*hb.head, 0, 100000).unwrap();
    let mut res = q.select(&ctx(), false, None, &[eq("a", "b")]);
    assert!(res.next(), "series is not present");
    let s = res.at().clone();
    let mut it = s.iterator(None);
    assert_eq!(ValueType::None, it.next(), "expected no samples");
    while res.next() {}
    assert!(res.err().is_none());
    assert!(res.warnings().is_empty());

    // Add again and test for presence.
    let mut app = hb.appender(&ctx());
    app.append(0, &ls(&["a", "b"]), 11, 1.0).unwrap();
    commit(&mut *app);
    let q = new_block_querier(&*hb.head, 0, 100000).unwrap();
    let mut res = q.select(&ctx(), false, None, &[eq("a", "b")]);
    assert!(res.next(), "series don't exist");
    let exps = res.at().clone();
    let res_samples = storage::expand_samples(&mut exps.iterator(None)).unwrap();
    assert_eq!(vec![fs(11, 1.0)], res_samples);
    while res.next() {}
    assert!(res.err().is_none());
    assert!(res.warnings().is_empty());
}

#[test]
fn test_deleted_samples_and_series_still_in_wal_after_checkpoint() {
    let num_samples = 10000i64;

    // Enough samples to cause a checkpoint.
    let hb = new_test_head(num_samples * 10, compression::Type::None, false);
    let w = wal_of(&hb);

    for i in 0..num_samples {
        let mut app = hb.appender(&ctx());
        app.append(0, &ls(&["a", "b"]), i, 0.0).unwrap();
        commit(&mut *app);
    }
    hb.delete(&ctx(), 0, num_samples, &[eq("a", "b")]).unwrap();
    hb.truncate(1).unwrap();
    hb.close().unwrap();

    // Confirm there's been a checkpoint.
    let (cdir, _) = wlog::last_checkpoint(w.dir()).unwrap();
    // Read in checkpoint and WAL.
    let mut recs = read_test_wal(&cdir);
    recs.extend(read_test_wal(w.dir()));

    let (mut series, mut samples, mut stones, mut metadata) = (0, 0, 0, 0);
    for rec in &recs {
        match rec {
            Rec::Series(_) => series += 1,
            Rec::Samples(_) => samples += 1,
            Rec::Tombstones(_) => stones += 1,
            Rec::Metadata(_) => metadata += 1,
            r => panic!("unknown record type {r:?}"),
        }
    }
    assert_eq!(1, series);
    assert_eq!(9999, samples);
    assert_eq!(1, stones);
    assert_eq!(0, metadata);
}

/// Go `deletedSamples`.
fn deleted_samples(full: &[Sample], dranges: &Intervals) -> Vec<Sample> {
    full.iter()
        .filter(|s| !dranges.iter().any(|r| r.in_bounds(s.t)))
        .cloned()
        .collect()
}

/// Go `requireEqualSamples` for float samples.
fn require_equal_float_samples(name: &str, expected: &[Sample], actual: &[Sample]) {
    assert_eq!(expected.len(), actual.len(), "Length not equal to expected for {name}");
    for (i, (e, a)) in expected.iter().zip(actual).enumerate() {
        assert_eq!(e.t, a.t, "Different timestamps for {name}[{i}]");
        assert_eq!(
            e.h.is_some(),
            a.h.is_some(),
            "Different types for {name}[{i}] at ts {}",
            e.t
        );
        assert_eq!(
            e.fh.is_some(),
            a.fh.is_some(),
            "Different types for {name}[{i}] at ts {}",
            e.t
        );
        assert_eq!(e.f, a.f, "Sample doesn't match for {name}[{i}] at ts {}", e.t);
    }
}

#[test]
fn test_delete_e2e() {
    let num_datapoints = 1000;
    let num_ranges = 1000;
    let time_interval = 2i64;
    // Create 8 series with 1000 data-points of different ranges, delete and run queries.
    let lbls: Vec<Labels> = vec![
        ls(&["a", "b", "instance", "localhost:9090", "job", "prometheus"]),
        ls(&["a", "b", "instance", "127.0.0.1:9090", "job", "prometheus"]),
        ls(&["a", "b", "instance", "127.0.0.1:9090", "job", "prom-k8s"]),
        ls(&["a", "b", "instance", "localhost:9090", "job", "prom-k8s"]),
        ls(&["a", "c", "instance", "localhost:9090", "job", "prometheus"]),
        ls(&["a", "c", "instance", "127.0.0.1:9090", "job", "prometheus"]),
        ls(&["a", "c", "instance", "127.0.0.1:9090", "job", "prom-k8s"]),
        ls(&["a", "c", "instance", "localhost:9090", "job", "prom-k8s"]),
    ];
    let mut series_map: BTreeMap<String, Vec<Sample>> = BTreeMap::new();

    let hb = new_test_head(100000, compression::Type::None, false);

    let rand_i63n = |n: i64| -> i64 {
        let mut b = [0u8; 8];
        crypto::random_bytes(&mut b);
        (u64::from_le_bytes(b) % n as u64) as i64
    };

    let mut app = hb.appender(&ctx());
    for l in &lbls {
        let mut series = Vec::new();
        let mut ts = rand_i63n(300);
        for _ in 0..num_datapoints {
            let v = crate::common::rand_f64();
            app.append(0, l, ts, v).unwrap();
            series.push(fs(ts, v));
            ts += rand_i63n(time_interval) + 1;
        }
        series_map.insert(l.to_string(), series);
    }
    commit(&mut *app);

    // Delete a time-range from each-selector.
    let iv = |mint, maxt| Interval { mint, maxt };
    let dels: Vec<(Vec<Matcher>, Intervals)> = vec![
        (vec![eq("a", "b")], vec![iv(300, 500), iv(600, 670)]),
        (
            vec![eq("a", "b"), eq("job", "prom-k8s")],
            vec![iv(300, 500), iv(100, 670)],
        ),
        (
            vec![eq("a", "c"), eq("instance", "localhost:9090"), eq("job", "prometheus")],
            vec![iv(300, 400), iv(100, 6700)],
        ),
        // TODO: Add Regexp Matchers.
    ];
    for (ms, drange) in &dels {
        for r in drange {
            hb.delete(&ctx(), r.mint, r.maxt, ms).unwrap();
        }
        let mut matched: Vec<Labels> = lbls
            .iter()
            .filter(|l| ms.iter().all(|m| m.matches(l.get(&m.name))))
            .cloned()
            .collect();
        matched.sort_by(|a, b| labels::compare(a, b).cmp(&0));
        for _ in 0..num_ranges {
            let mut q = new_block_querier(&*hb.head, 0, 100000).unwrap();
            let mut ss = q.select(&ctx(), true, None, ms);
            // Build the expected series.
            let mut matched_series: Vec<(Labels, Vec<Sample>)> = Vec::new();
            for m in &matched {
                let smpls = deleted_samples(&series_map[&m.to_string()], drange);
                // Only append those series for which samples exist, as the expected set doesn't skip series with
                // no samples.
                if !smpls.is_empty() {
                    matched_series.push((m.clone(), smpls));
                }
            }
            // Compare both SeriesSets.
            let mut i = 0;
            loop {
                let eok = i < matched_series.len();
                let mut rok = ss.next();
                // Skip a series if iterator is empty.
                if rok {
                    while ss.at().iterator(None).next() == ValueType::None {
                        rok = ss.next();
                        if !rok {
                            break;
                        }
                    }
                }
                assert_eq!(eok, rok);
                if !eok {
                    break;
                }
                let (exp_l, exp_s) = &matched_series[i];
                let sres = ss.at().clone();
                assert_eq!(*exp_l, *sres.labels());
                let smpl_exp = storage::expand_samples(&mut storage::new_list_series_iterator(exp_s));
                let smpl_res = storage::expand_samples(&mut sres.iterator(None));
                assert_eq!(smpl_exp.is_err(), smpl_res.is_err());
                require_equal_float_samples(&exp_l.to_string(), &smpl_exp.unwrap(), &smpl_res.unwrap());
                i += 1;
            }
            assert!(ss.err().is_none());
            assert!(ss.warnings().is_empty());
            assert!(q.close().is_none());
        }
    }
}

#[test]
fn test_compute_chunk_end_time() {
    let cases: Vec<(&str, i64, i64, i64, f64, i64)> = vec![
        ("exactly 1/4 full, even increment", 0, 250, 1000, 4.0, 1000),
        ("exactly 1/4 full, uneven increment", 100, 200, 1000, 4.0, 550),
        ("decimal ratio to full", 5000, 5110, 10000, 4.2, 5500),
        // Case where we fit floored 0 chunks. Must catch division by 0 and default to maximum time.
        ("fit floored 0 chunks", 0, 500, 1000, 4.0, 1000),
        // Catch division by zero for cur == start. Strictly not a possible case.
        ("cur == start", 100, 100, 1000, 4.0, 104),
    ];
    for (name, start, cur, max, ratio_to_full, res) in cases {
        let got = compute_chunk_end_time(start, cur, max, ratio_to_full);
        assert_eq!(res, got, "{name} (start: {start}, cur: {cur}, max: {max})");
    }
}

type StFunc = fn(i64) -> i64;

fn st_scenarios() -> Vec<(&'static str, bool, StFunc)> {
    vec![
        ("useXOR2=false st=0", false, |_| 0),
        ("useXOR2=true st=0", true, |_| 0),
        ("useXOR2=true st=ts", true, |ts| ts),
        ("useXOR2=true st=ts-100", true, |ts| ts - 100),
        ("useXOR2=false st=ts (st ignored)", false, |ts| ts),
    ]
}

/// TestMemSeries_append tests float appending with various useXOR2/st combinations.
#[test]
fn test_mem_series_append() {
    for (name, use_xor2, st_func) in st_scenarios() {
        test_mem_series_append_scenario(name, use_xor2, st_func);
    }
}

fn test_mem_series_append_scenario(name: &str, use_xor2: bool, st_func: StFunc) {
    let dir = TempDir::new();
    let cdm = new_cdm(&dir);
    let c_opts = ChunkOpts {
        chunk_disk_mapper: Some(&cdm),
        chunk_range: 500,
        samples_per_chunk: tsdb::head::DEFAULT_SAMPLES_PER_CHUNK,
        use_xor2,
        ..Default::default()
    };

    let s = MemSeries::new(Labels::default(), 1, 0, DEFAULT_ISOLATION_DISABLED, false);
    let mut g = s.lock();
    let (lset, sref) = (&s.lset, s.r#ref);

    // Add first two samples at the very end of a chunk range and the next two on and after it. New chunk must
    // correctly be cut at 1000.
    let (ok, chunk_created) = g.append(st_func(998), 998, 1.0, 0, &c_opts, lset, sref);
    assert!(ok, "{name}: append failed");
    assert!(chunk_created, "{name}: first sample created chunk");

    let (ok, chunk_created) = g.append(st_func(999), 999, 2.0, 0, &c_opts, lset, sref);
    assert!(ok, "{name}: append failed");
    assert!(!chunk_created, "{name}: second sample should use same chunk");
    g.mmap_chunks(&cdm, sref);

    let (ok, chunk_created) = g.append(st_func(1000), 1000, 3.0, 0, &c_opts, lset, sref);
    assert!(ok, "{name}: append failed");
    assert!(chunk_created, "{name}: expected new chunk on boundary");

    let (ok, chunk_created) = g.append(st_func(1001), 1001, 4.0, 0, &c_opts, lset, sref);
    assert!(ok, "{name}: append failed");
    assert!(!chunk_created, "{name}: second sample should use same chunk");

    g.mmap_chunks(&cdm, sref);
    assert_eq!(
        1,
        g.mmapped_chunks.len(),
        "{name}: there should be only 1 mmapped chunk"
    );
    assert_eq!(998, g.mmapped_chunks[0].min_time, "{name}: wrong chunk range");
    assert_eq!(999, g.mmapped_chunks[0].max_time, "{name}: wrong chunk range");
    assert_eq!(1000, g.head_chunk().unwrap().min_time, "{name}: wrong chunk range");
    assert_eq!(1001, g.head_chunk().unwrap().max_time, "{name}: wrong chunk range");

    // Fill the range [1000,2000) with many samples. Intermediate chunks should be cut at approximately 120
    // samples per chunk.
    for i in 1..1000i64 {
        let ts = 1001 + i;
        let (ok, _) = g.append(st_func(ts), ts, i as f64, 0, &c_opts, lset, sref);
        assert!(ok, "{name}: append failed");
    }
    g.mmap_chunks(&cdm, sref);

    assert!(g.mmapped_chunks.len() + 1 > 7, "{name}: expected intermediate chunks");

    // All chunks but the first and last should now be moderately full.
    for (i, c) in g.mmapped_chunks[1..].iter().enumerate() {
        let chk = cdm.chunk(c.r#ref).unwrap();
        assert!(
            chk.num_samples() > 100,
            "{name}: unexpected small chunk {i} of length {}",
            chk.num_samples()
        );
    }
    drop(g);
    cdm.close().unwrap();
}

/// TestMemSeries_appendHistogram tests histogram appending with various useXOR2/st combinations.
#[test]
fn test_mem_series_append_histogram() {
    for (name, use_xor2, st_func) in st_scenarios() {
        test_mem_series_append_histogram_scenario(name, use_xor2, st_func);
    }
}

fn test_mem_series_append_histogram_scenario(name: &str, use_xor2: bool, st_func: StFunc) {
    let dir = TempDir::new();
    let cdm = new_cdm(&dir);
    let c_opts = ChunkOpts {
        chunk_disk_mapper: Some(&cdm),
        chunk_range: 1000,
        samples_per_chunk: tsdb::head::DEFAULT_SAMPLES_PER_CHUNK,
        use_xor2,
        ..Default::default()
    };

    let s = MemSeries::new(Labels::default(), 1, 0, DEFAULT_ISOLATION_DISABLED, false);
    let mut g = s.lock();
    let (lset, sref) = (&s.lset, s.r#ref);

    let histograms = storage::tsdbutil::generate_test_histograms(4);
    let mut histogram_with_one_more_bucket = histograms[3].clone();
    histogram_with_one_more_bucket.count += 1;
    histogram_with_one_more_bucket.sum += 1.23;
    histogram_with_one_more_bucket.positive_spans[1].length = 3;
    histogram_with_one_more_bucket.positive_buckets.push(1);

    // Add first two samples at the very end of a chunk range and the next two on and after it. New chunk must
    // correctly be cut at 1000.
    let (ok, chunk_created) = g.append_histogram(
        st_func(998),
        998,
        Arc::new(histograms[0].clone()),
        0,
        &c_opts,
        lset,
        sref,
    );
    assert!(ok, "{name}: append failed");
    assert!(chunk_created, "{name}: first sample created chunk");

    let (ok, chunk_created) = g.append_histogram(
        st_func(999),
        999,
        Arc::new(histograms[1].clone()),
        0,
        &c_opts,
        lset,
        sref,
    );
    assert!(ok, "{name}: append failed");
    assert!(!chunk_created, "{name}: second sample should use same chunk");

    let (ok, chunk_created) = g.append_histogram(
        st_func(1000),
        1000,
        Arc::new(histograms[2].clone()),
        0,
        &c_opts,
        lset,
        sref,
    );
    assert!(ok, "{name}: append failed");
    assert!(chunk_created, "{name}: expected new chunk on boundary");

    let (ok, chunk_created) = g.append_histogram(
        st_func(1001),
        1001,
        Arc::new(histograms[3].clone()),
        0,
        &c_opts,
        lset,
        sref,
    );
    assert!(ok, "{name}: append failed");
    assert!(!chunk_created, "{name}: second sample should use same chunk");

    g.mmap_chunks(&cdm, sref);
    assert_eq!(
        1,
        g.mmapped_chunks.len(),
        "{name}: there should be only 1 mmapped chunk"
    );
    assert_eq!(998, g.mmapped_chunks[0].min_time, "{name}: wrong chunk range");
    assert_eq!(999, g.mmapped_chunks[0].max_time, "{name}: wrong chunk range");
    assert_eq!(1000, g.head_chunk().unwrap().min_time, "{name}: wrong chunk range");
    assert_eq!(1001, g.head_chunk().unwrap().max_time, "{name}: wrong chunk range");

    let (ok, chunk_created) = g.append_histogram(
        st_func(1002),
        1002,
        Arc::new(histogram_with_one_more_bucket),
        0,
        &c_opts,
        lset,
        sref,
    );
    assert!(ok, "{name}: append failed");
    assert!(!chunk_created, "{name}: third sample should trigger a re-encoded chunk");

    g.mmap_chunks(&cdm, sref);
    assert_eq!(
        1,
        g.mmapped_chunks.len(),
        "{name}: there should be only 1 mmapped chunk"
    );
    assert_eq!(998, g.mmapped_chunks[0].min_time, "{name}: wrong chunk range");
    assert_eq!(999, g.mmapped_chunks[0].max_time, "{name}: wrong chunk range");
    assert_eq!(1000, g.head_chunk().unwrap().min_time, "{name}: wrong chunk range");
    assert_eq!(1002, g.head_chunk().unwrap().max_time, "{name}: wrong chunk range");
    drop(g);
    cdm.close().unwrap();
}

#[test]
fn test_mem_series_append_at_variable_rate() {
    const SAMPLES_PER_CHUNK: usize = 120;
    let dir = TempDir::new();
    let cdm = new_cdm(&dir);
    let c_opts = ChunkOpts {
        chunk_disk_mapper: Some(&cdm),
        chunk_range: DEFAULT_BLOCK_DURATION,
        samples_per_chunk: SAMPLES_PER_CHUNK,
        ..Default::default()
    };

    let s = MemSeries::new(Labels::default(), 1, 0, DEFAULT_ISOLATION_DISABLED, false);
    let mut g = s.lock();
    let (lset, sref) = (&s.lset, s.r#ref);

    // At this slow rate, we will fill the chunk in two block durations.
    let slow_rate = (DEFAULT_BLOCK_DURATION * 2) / SAMPLES_PER_CHUNK as i64;

    let mut next_ts = 0i64;
    let mut total_appended_samples = 0usize;
    for i in 0..SAMPLES_PER_CHUNK / 4 {
        let (ok, _) = g.append(0, next_ts, i as f64, 0, &c_opts, lset, sref);
        assert!(ok, "slow sample {i} was not appended");
        next_ts += slow_rate;
        total_appended_samples += 1;
    }
    assert_eq!(
        DEFAULT_BLOCK_DURATION, g.next_at,
        "after appending a samplesPerChunk/4 samples at a slow rate, we should aim to cut a new block at the default block duration"
    );

    // Suddenly, the rate increases and we receive a sample every millisecond.
    for i in 0..u16::MAX as usize {
        let (ok, _) = g.append(0, next_ts, i as f64, 0, &c_opts, lset, sref);
        assert!(ok, "quick sample {i} was not appended");
        next_ts += 1;
        total_appended_samples += 1;
    }
    let (ok, chunk_created) = g.append(0, DEFAULT_BLOCK_DURATION, 0.0, 0, &c_opts, lset, sref);
    assert!(ok, "new chunk sample was not appended");
    assert!(
        chunk_created,
        "sample at block duration timestamp should create a new chunk"
    );

    g.mmap_chunks(&cdm, sref);
    let mut total_samples_in_chunks = 0usize;
    for (i, c) in g.mmapped_chunks.iter().enumerate() {
        total_samples_in_chunks += c.num_samples as usize;
        assert!(
            c.num_samples <= (2 * SAMPLES_PER_CHUNK) as u16,
            "mmapped chunk {i} has more than {} samples",
            2 * SAMPLES_PER_CHUNK
        );
    }
    assert_eq!(
        total_appended_samples,
        total_samples_in_chunks,
        "wrong number of samples in {} mmapped chunks",
        g.mmapped_chunks.len()
    );
    drop(g);
    cdm.close().unwrap();
}

/// The shared setup of TestGCChunkAccess and TestGCSeriesAccess: a series with two chunks.
fn gc_access_setup(h: &TestHead) -> Arc<MemSeries> {
    const CHUNK_RANGE: i64 = 1000;
    let c_opts = ChunkOpts {
        chunk_disk_mapper: Some(&h.chunk_disk_mapper),
        chunk_range: CHUNK_RANGE,
        samples_per_chunk: tsdb::head::DEFAULT_SAMPLES_PER_CHUNK,
        ..Default::default()
    };

    h.init_time(0);

    let (s, _) = h.get_or_create(1, &ls(&["a", "1"]), false).unwrap();
    let mut g = s.lock();
    let (lset, sref) = (&s.lset, s.r#ref);

    // Appending 2 samples for the first chunk.
    let (ok, chunk_created) = g.append(0, 0, 0.0, 0, &c_opts, lset, sref);
    assert!(ok, "series append failed");
    assert!(chunk_created, "chunks was not created");
    let (ok, chunk_created) = g.append(0, 999, 999.0, 0, &c_opts, lset, sref);
    assert!(ok, "series append failed");
    assert!(!chunk_created, "chunks was created");

    // A new chunks should be created here as it's beyond the chunk range.
    let (ok, chunk_created) = g.append(0, 1000, 1000.0, 0, &c_opts, lset, sref);
    assert!(ok, "series append failed");
    assert!(chunk_created, "chunks was not created");
    let (ok, chunk_created) = g.append(0, 1999, 1999.0, 0, &c_opts, lset, sref);
    assert!(ok, "series append failed");
    assert!(!chunk_created, "chunks was created");
    drop(g);
    s
}

#[test]
fn test_gc_chunk_access() {
    // Put a chunk, select it. GC it and then access it.
    let h = new_test_head(1000, compression::Type::None, false);
    let _s = gc_access_setup(&h);

    let idx = h.index_range(0, 1500);
    let mut chnks = Vec::new();
    let mut builder = ScratchBuilder::default();
    tsdb::IndexReader::series(&idx, 1, &mut builder, &mut chnks).unwrap();

    assert_eq!(ls(&["a", "1"]), builder.labels());
    assert_eq!(2, chnks.len());

    let cr = h.chunks_range(0, 1500, None).unwrap();
    tsdb::ChunkReader::chunk_or_iterable(&cr, &chnks[0]).unwrap();
    tsdb::ChunkReader::chunk_or_iterable(&cr, &chnks[1]).unwrap();

    h.truncate(1500).unwrap(); // Remove a chunk.

    assert_eq!(
        storage::ERR_NOT_FOUND,
        tsdb::ChunkReader::chunk_or_iterable(&cr, &chnks[0]).err().unwrap()
    );
    tsdb::ChunkReader::chunk_or_iterable(&cr, &chnks[1]).unwrap();
}

#[test]
fn test_gc_series_access() {
    // Put a series, select it. GC it and then access it.
    let h = new_test_head(1000, compression::Type::None, false);
    let _s = gc_access_setup(&h);

    let idx = h.index_range(0, 2000);
    let mut chunks = Vec::new();
    let mut builder = ScratchBuilder::default();
    tsdb::IndexReader::series(&idx, 1, &mut builder, &mut chunks).unwrap();

    assert_eq!(ls(&["a", "1"]), builder.labels());
    assert_eq!(2, chunks.len());

    let cr = h.chunks_range(0, 2000, None).unwrap();
    tsdb::ChunkReader::chunk_or_iterable(&cr, &chunks[0]).unwrap();
    tsdb::ChunkReader::chunk_or_iterable(&cr, &chunks[1]).unwrap();

    h.truncate(2000).unwrap(); // Remove the series.

    assert!(h.series.get_by_id(1).is_none());

    assert_eq!(
        storage::ERR_NOT_FOUND,
        tsdb::ChunkReader::chunk_or_iterable(&cr, &chunks[0]).err().unwrap()
    );
    assert_eq!(
        storage::ERR_NOT_FOUND,
        tsdb::ChunkReader::chunk_or_iterable(&cr, &chunks[1]).err().unwrap()
    );
}

#[test]
fn test_uncommitted_samples_not_lost_on_truncate() {
    let h = new_test_head(1000, compression::Type::None, false);
    h.init_time(0);

    let mut app = h.head_appender();
    let lset = ls(&["a", "1"]);
    app.append(0, &lset, 2100, 1.0).unwrap();

    h.truncate(2000).unwrap();
    assert!(
        h.series.get_by_hash(lset.hash(), &lset).is_some(),
        "series should not have been garbage collected"
    );

    commit(&mut app);

    let mut q = new_block_querier(&*h.head, 1500, 2500).unwrap();
    let mut ss = q.select(&ctx(), false, None, &[eq("a", "1")]);
    assert!(ss.next());
    while ss.next() {}
    assert!(ss.err().is_none());
    assert!(ss.warnings().is_empty());
    drop(ss);
    q.close();
}

#[test]
fn test_remove_series_after_rollback_and_truncate() {
    let h = new_test_head(1000, compression::Type::None, false);
    h.init_time(0);

    let mut app = h.head_appender();
    let lset = ls(&["a", "1"]);
    app.append(0, &lset, 2100, 1.0).unwrap();

    h.truncate(2000).unwrap();
    assert!(
        h.series.get_by_hash(lset.hash(), &lset).is_some(),
        "series should not have been garbage collected"
    );

    rollback(&mut app);

    let mut q = new_block_querier(&*h.head, 1500, 2500).unwrap();
    let mut ss = q.select(&ctx(), false, None, &[eq("a", "1")]);
    assert!(!ss.next());
    assert!(ss.warnings().is_empty());
    drop(ss);
    assert!(q.close().is_none());

    // Truncate again, this time the series should be deleted.
    h.truncate(2050).unwrap();
    assert!(h.series.get_by_hash(lset.hash(), &lset).is_none());
}

#[test]
fn test_head_log_rollback() {
    for compress in COMPRESSIONS {
        let h = new_test_head(1000, compress, false);
        let w = wal_of(&h);

        let mut app = h.appender(&ctx());
        app.append(0, &ls(&["a", "b"]), 1, 2.0).unwrap();

        rollback(&mut *app);
        let recs = read_test_wal(w.dir());

        assert_eq!(1, recs.len(), "compress={}", compress.as_str());
        match &recs[0] {
            Rec::Series(series) => assert_eq!(&vec![rs(1, &["a", "b"])], series),
            r => panic!("expected series record but got {r:?}"),
        }
    }
}

#[test]
fn test_head_returns_sorted_label_values() {
    let h = new_test_head(1000, compression::Type::None, false);
    h.init_time(0);

    let mut app = h.head_appender();
    for i in (1..=100).rev() {
        for j in 0..10 {
            let lset = ls(&["__name__", &format!("metric_{i}"), "label", &format!("value_{j}")]);
            app.append(0, &lset, 2100, 1.0).unwrap();
        }
    }

    let mut q = new_block_querier(&*h.head, 1500, 2500).unwrap();
    let (res, _, err) = q.label_values(&ctx(), "__name__", None, &[]);
    assert!(err.is_none());
    assert!(res.is_sorted());
    assert!(q.close().is_none());
}

/// Go `errors.As(err, &*wlog.CorruptionErr)`, also through an `errLoadWbl` wrapper.
fn is_corruption_err(err: &Error) -> bool {
    if err.downcast_ref::<wlog::Error>().and_then(|e| e.corruption()).is_some() {
        return true;
    }
    err.downcast_ref::<tsdb::head_wal::ErrLoadWbl>()
        .is_some_and(|e| is_corruption_err(&e.0))
}

/// Counts the intact records of the log in `dir`.
fn count_records(dir: &std::path::Path) -> usize {
    let sr = wlog::new_segments_reader(dir).unwrap();
    let mut r = wlog::Reader::new(sr);
    let mut act_rec = 0;
    while r.next() {
        act_rec += 1;
    }
    assert!(r.err().is_none(), "{:?}", r.err());
    act_rec
}

/// TestWalRepair_DecodingError ensures that a repair is run for an error when decoding a record.
#[test]
fn test_wal_repair_decoding_error() {
    for enable_st_storage in [false, true] {
        let enc = Encoder { enable_st_storage };
        let mut series = Vec::new();
        enc.series(&[rs(1, &["a", "b"])], &mut series);
        let mut samples = Vec::new();
        enc.samples(&[rsmp(0, 99, 1.0)], &mut samples);
        let mut tstones = Vec::new();
        enc.tombstones(
            &[Stone {
                r#ref: 1,
                intervals: vec![],
            }],
            &mut tstones,
        );
        // (name, record, total records, expected intact records); the corruption keeps the first 3 bytes.
        let tests: Vec<(&str, Vec<u8>, usize, usize)> = vec![
            ("decode_series", series, 9, 5),
            ("decode_samples", samples, 9, 5),
            ("decode_tombstone", tstones, 9, 5),
        ];
        for (name, rec, total_recs, exp_recs) in &tests {
            for compress in COMPRESSIONS {
                let name = format!("{name},compress={},stStorage={enable_st_storage}", compress.as_str());
                let dir = TempDir::new();

                // Fill the wal and corrupt it.
                {
                    let w = Arc::new(WL::new(dir.path().join("wal"), compress).unwrap());
                    for i in 1..=*total_recs {
                        // At this point insert a corrupted record.
                        if i - 1 == *exp_recs {
                            // adapted: Go `rec[:3]` slices into the spare (zeroed) capacity of the record
                            // buffer when the record is shorter (the tombstone record without intervals is 1
                            // byte); pad with zeros the same way.
                            let mut corr = rec.clone();
                            corr.resize(3, 0);
                            w.log(&corr[..3]).unwrap();
                            continue;
                        }
                        w.log(rec).unwrap();
                    }

                    let opts = HeadOptions {
                        chunk_range: 1,
                        chunk_dir_root: w.dir().to_path_buf(),
                        ..HeadOptions::default()
                    };
                    let h = Head::new(Some(w), None, opts, None).unwrap();
                    assert_eq!(0.0, h.metrics.wal_corruptions_total.get(), "{name}");
                    let init_err = h.init(i64::MIN).expect_err("init should fail");
                    assert!(
                        is_corruption_err(&init_err),
                        "{name}: reading the wal didn't return corruption error: {init_err}"
                    );
                    h.close().unwrap(); // Head will close the wal as well.
                }

                // Open the db to trigger a repair.
                {
                    let db = tsdb::db::open(dir.path(), None, Some(tsdb::db::default_options()), None).unwrap();
                    assert_eq!(1.0, db.head().metrics.wal_corruptions_total.get(), "{name}");
                    db.close().unwrap();
                }

                // Read the wal content after the repair.
                assert_eq!(
                    *exp_recs,
                    count_records(&dir.path().join("wal")),
                    "{name}: Wrong number of intact records"
                );
            }
        }
    }
}

/// TestWblRepair_DecodingError ensures that a repair is run for an error when decoding a record.
#[test]
fn test_wbl_repair_decoding_error() {
    for enable_st_storage in [false, true] {
        let name = format!("enableSTStorage={enable_st_storage}");
        let enc = Encoder { enable_st_storage };
        let mut rec = Vec::new();
        enc.samples(&[rsmp(0, 99, 1.0)], &mut rec);
        let total_recs = 9;
        let exp_recs = 5;
        let dir = TempDir::new();

        // Fill the wbl and corrupt it.
        {
            let wal = Arc::new(WL::new(dir.path().join("wal"), compression::Type::None).unwrap());
            let wbl = Arc::new(WL::new(dir.path().join("wbl"), compression::Type::None).unwrap());

            for i in 1..=total_recs {
                // At this point insert a corrupted record.
                if i - 1 == exp_recs {
                    wbl.log(&rec[..3]).unwrap();
                    continue;
                }
                wbl.log(&rec).unwrap();
            }

            let opts = HeadOptions {
                chunk_range: 1,
                chunk_dir_root: wal.dir().to_path_buf(),
                ..HeadOptions::default()
            };
            opts.out_of_order_cap_max.store(30, Ordering::Relaxed);
            opts.out_of_order_time_window.store(1000 * 60 * 1000, Ordering::Relaxed);
            let h = Head::new(Some(wal), Some(wbl), opts, None).unwrap();
            assert_eq!(0.0, h.metrics.wal_corruptions_total.get(), "{name}");
            let init_err = h.init(i64::MIN).expect_err("init should fail");

            // Wbl errors are wrapped into errLoadWbl, make sure we can unwrap it.
            assert!(
                init_err.downcast_ref::<tsdb::head_wal::ErrLoadWbl>().is_some(),
                "{name}: {init_err}"
            );
            assert!(
                is_corruption_err(&init_err),
                "{name}: reading the wal didn't return corruption error: {init_err}"
            );
            h.close().unwrap(); // Head will close the wal as well.
        }

        // Open the db to trigger a repair.
        {
            let db = tsdb::db::open(dir.path(), None, Some(tsdb::db::default_options()), None).unwrap();
            assert_eq!(1.0, db.head().metrics.wal_corruptions_total.get(), "{name}");
            db.close().unwrap();
        }

        // Read the wbl content after the repair.
        assert_eq!(
            exp_recs,
            count_records(&dir.path().join("wbl")),
            "{name}: Wrong number of intact records"
        );
    }
}

/// Creating a checkpoint over a corrupted WAL segment increments prometheus_tsdb_wal_corruptions_total.
#[test]
fn test_head_truncate_wal_increments_corruption_metric_on_corrupted_segment() {
    let h = new_test_head(1000, compression::Type::None, false);
    let w = wal_of(&h);

    let enc = Encoder::default();
    let mut series_rec = Vec::new();
    enc.series(&[rs(1, &["a", "b"])], &mut series_rec);

    // Create several WAL segments so that truncateWAL has a range to checkpoint.
    const NUM_SEGMENTS: usize = 4;
    for _ in 0..NUM_SEGMENTS {
        w.log(&series_rec).unwrap();
        w.next_segment_sync().unwrap();
    }

    // Replay the intact WAL.
    h.init(0).unwrap();
    assert_eq!(0.0, h.metrics.wal_corruptions_total.get());

    // Corrupt segment 0 on disk by flipping a byte inside the first record's data.
    let seg_file = wlog::segment_name(w.dir(), 0);
    {
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new().write(true).open(&seg_file).unwrap();
        f.write_at(&[0xff], 7).unwrap();
    }

    // Trigger a checkpoint via WAL truncation.
    h.last_wal_truncation_time.store(0, Ordering::SeqCst);
    let err = h.truncate_wal(100).unwrap_err();
    assert!(
        err.downcast_ref::<wlog::Error>().and_then(|e| e.corruption()).is_some(),
        "checkpoint creation should report a WAL corruption error, got {err}"
    );
    assert_eq!(1.0, h.metrics.wal_corruptions_total.get());
}

#[test]
fn test_head_read_writer_repair() {
    let dir = TempDir::new();
    const CHUNK_RANGE: i64 = 1000;
    let wal_dir = dir.path().join("wal");
    let chunks_dir = tsdb::head::mmapped_chunks_dir(dir.path());

    // Fill the chunk segments and corrupt it.
    {
        let w = Arc::new(WL::new(&wal_dir, compression::Type::None).unwrap());
        let opts = HeadOptions {
            chunk_range: CHUNK_RANGE,
            chunk_dir_root: dir.path().to_path_buf(),
            // We need to set this option so that we use the async queue. Upstream prometheus uses the queue
            // directly.
            chunk_write_queue_size: 1,
            ..HeadOptions::default()
        };
        let h = Head::new(Some(w), None, opts, None).unwrap();
        assert_eq!(0.0, h.metrics.mmap_chunk_corruption_total.get());
        h.init(i64::MIN).unwrap();

        let c_opts = ChunkOpts {
            chunk_disk_mapper: Some(&h.chunk_disk_mapper),
            chunk_range: CHUNK_RANGE,
            samples_per_chunk: tsdb::head::DEFAULT_SAMPLES_PER_CHUNK,
            ..Default::default()
        };

        let (s, created) = h.get_or_create(1, &ls(&["a", "1"]), false).unwrap();
        assert!(created, "series was not created");

        for i in 0..7i64 {
            let mut g = s.lock();
            let (ok, chunk_created) = g.append(
                0,
                i * CHUNK_RANGE,
                (i * CHUNK_RANGE) as f64,
                0,
                &c_opts,
                &s.lset,
                s.r#ref,
            );
            assert!(ok, "series append failed");
            assert!(chunk_created, "chunk was not created");
            let (ok, chunk_created) = g.append(
                0,
                i * CHUNK_RANGE + CHUNK_RANGE - 1,
                (i * CHUNK_RANGE) as f64,
                0,
                &c_opts,
                &s.lset,
                s.r#ref,
            );
            assert!(ok, "series append failed");
            assert!(!chunk_created, "chunk was created");
            h.chunk_disk_mapper.cut_new_file();
            g.mmap_chunks(&h.chunk_disk_mapper, s.r#ref);
        }
        h.close().unwrap();

        // Verify that there are 6 segment files. It should only be 6 because the last call to .CutNewFile() won't
        // take effect without another chunk being written.
        let mut files: Vec<_> = std::fs::read_dir(&chunks_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        files.sort();
        assert_eq!(6, files.len());

        // Corrupt the 4th file by writing a random byte to series ref.
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new().write(true).open(&files[3]).unwrap();
        let n = f
            .write_at(&[67, 88], (tsdb_format::chunks::HEAD_CHUNK_FILE_HEADER_SIZE + 2) as u64)
            .unwrap();
        assert_eq!(2, n);
    }

    // Open the db to trigger a repair.
    {
        let db = tsdb::db::open(dir.path(), None, Some(tsdb::db::default_options()), None).unwrap();
        assert_eq!(1.0, db.head().metrics.mmap_chunk_corruption_total.get());
        db.close().unwrap();
    }

    // Verify that there are 3 segment files after the repair. The segments from the corrupt segment should be
    // removed.
    assert_eq!(3, std::fs::read_dir(&chunks_dir).unwrap().count());
}

#[test]
fn test_new_wal_segment_on_truncate() {
    let h = new_test_head(1000, compression::Type::None, false);
    let wal = wal_of(&h);
    let add = |ts: i64| {
        let mut app = h.appender(&ctx());
        app.append(0, &ls(&["a", "b"]), ts, 0.0).unwrap();
        commit(&mut *app);
    };

    add(0);
    let (_, last) = wlog::segments(wal.dir()).unwrap();
    assert_eq!(0, last);

    add(1);
    h.truncate(1).unwrap();
    let (_, last) = wlog::segments(wal.dir()).unwrap();
    assert_eq!(1, last);

    add(2);
    h.truncate(2).unwrap();
    let (_, last) = wlog::segments(wal.dir()).unwrap();
    assert_eq!(2, last);
}

#[test]
fn test_add_duplicate_label_name() {
    let h = new_test_head(1000, compression::Type::None, false);

    let add = |lbls: Labels, label_name: &str| {
        let mut app = h.appender(&ctx());
        let err = app.append(0, &lbls, 0, 0.0).unwrap_err();
        assert_eq!(
            format!(r#"label name "{label_name}" is not unique: invalid sample"#),
            err.to_string()
        );
    };

    add(ls(&["a", "c", "a", "b"]), "a");
    add(ls(&["a", "c", "a", "c"]), "a");
    add(
        ls(&[
            "__name__",
            "up",
            "job",
            "prometheus",
            "le",
            "500",
            "le",
            "400",
            "unit",
            "s",
        ]),
        "le",
    );
}

/// Go `expandSeriesSet`: (labels, float samples by labels string).
fn expand_series_set(ss: &mut dyn SeriesSet) -> (Vec<Labels>, BTreeMap<String, Vec<Sample>>) {
    let mut result_labels = Vec::new();
    let mut result_samples = BTreeMap::new();
    while ss.next() {
        let series = ss.at().clone();
        let samples = expand_floats(&mut series.iterator(None));
        result_labels.push(series.labels().clone());
        result_samples.insert(series.labels().to_string(), samples);
    }
    assert!(ss.err().is_none());
    assert!(ss.warnings().is_empty());
    (result_labels, result_samples)
}

#[test]
fn test_mem_series_isolation() {
    if DEFAULT_ISOLATION_DISABLED {
        return; // Skipping test since tsdb isolation is disabled.
    }

    let last_value = |h: &Arc<Head>, max_append_id: u64| -> i64 {
        let idx = h.index();

        let mut iso = h.iso.state(i64::MIN, i64::MAX);
        Arc::get_mut(&mut iso).expect("unshared isolation state").max_append_id = max_append_id;

        let chunks = h.chunks_range(i64::MIN, i64::MAX, Some(iso)).unwrap();
        let mut querier = BlockQuerier(BlockBaseQuerier::from_parts(
            tsdb::head::head_ulid(),
            Arc::new(idx),
            Arc::new(chunks),
            Arc::new(MemTombstones::new()),
            0,
            10000,
        ));

        let mut ss = storage::Querier::select(&querier, &ctx(), false, None, &[eq("foo", "bar")]);
        let (_, series_set) = expand_series_set(&mut *ss);
        drop(ss);
        storage::LabelQuerier::close(&mut querier);

        if let Some(series) = series_set.values().next() {
            return series[series.len() - 1].f as i64;
        }
        -1
    };

    let add_samples = |h: &Arc<Head>| -> i64 {
        let mut i = 1i64;
        while i <= 1000 {
            // To initialize bounds.
            let mut app: Box<dyn Appender> = if h.min_time() == i64::MAX {
                // adapted: Go builds an `initAppender` directly; `Appender()` returns one on an uninitialized head.
                h.appender(&ctx())
            } else {
                let mut a = h.head_appender();
                a.base.cleanup_append_ids_below = 0;
                Box::new(a)
            };
            app.append(0, &ls(&["foo", "bar"]), i, i as f64).unwrap();
            commit(&mut *app);
            h.mmap_head_chunks();
            i += 1;
        }
        i
    };

    // Test isolation without restart of Head.
    let hb = new_test_head(1000, compression::Type::None, false);
    let mut i = add_samples(&hb.head);

    // Test simple cases in different chunks when no appendID cleanup has been performed.
    assert_eq!(10, last_value(&hb.head, 10));
    assert_eq!(130, last_value(&hb.head, 130));
    assert_eq!(160, last_value(&hb.head, 160));
    assert_eq!(240, last_value(&hb.head, 240));
    assert_eq!(500, last_value(&hb.head, 500));
    assert_eq!(750, last_value(&hb.head, 750));
    assert_eq!(995, last_value(&hb.head, 995));
    assert_eq!(999, last_value(&hb.head, 999));

    // Cleanup appendIDs below 500.
    let mut app = hb.head_appender();
    app.base.cleanup_append_ids_below = 500;
    app.append(0, &ls(&["foo", "bar"]), i, i as f64).unwrap();
    commit(&mut app);
    i += 1;

    // We should not get queries with a maxAppendID below 500 after the cleanup, but they only take the remaining
    // appendIDs into account.
    assert_eq!(499, last_value(&hb.head, 10));
    assert_eq!(499, last_value(&hb.head, 130));
    assert_eq!(499, last_value(&hb.head, 160));
    assert_eq!(499, last_value(&hb.head, 240));
    assert_eq!(500, last_value(&hb.head, 500));
    assert_eq!(995, last_value(&hb.head, 995));
    assert_eq!(999, last_value(&hb.head, 999));

    // Cleanup appendIDs below 1000, which means the sample buffer is the only thing with appendIDs.
    let mut app = hb.head_appender();
    app.base.cleanup_append_ids_below = 1000;
    app.append(0, &ls(&["foo", "bar"]), i, i as f64).unwrap();
    commit(&mut app);
    assert_eq!(999, last_value(&hb.head, 998));
    assert_eq!(999, last_value(&hb.head, 999));
    assert_eq!(1000, last_value(&hb.head, 1000));
    assert_eq!(1001, last_value(&hb.head, 1001));
    assert_eq!(1002, last_value(&hb.head, 1002));
    assert_eq!(1002, last_value(&hb.head, 1003));

    i += 1;
    // Cleanup appendIDs below 1001, but with a rollback.
    let mut app = hb.head_appender();
    app.base.cleanup_append_ids_below = 1001;
    app.append(0, &ls(&["foo", "bar"]), i, i as f64).unwrap();
    rollback(&mut app);
    assert_eq!(1000, last_value(&hb.head, 999));
    assert_eq!(1000, last_value(&hb.head, 1000));
    assert_eq!(1001, last_value(&hb.head, 1001));
    assert_eq!(1002, last_value(&hb.head, 1002));
    assert_eq!(1002, last_value(&hb.head, 1003));

    hb.close().unwrap();

    // Test isolation with restart of Head. This is to verify the num samples of chunks after m-map chunk replay.
    let mut hb = new_test_head(1000, compression::Type::None, false);
    let w = wal_of(&hb);
    let mut i = add_samples(&hb.head);
    hb.close().unwrap();

    let wal = Arc::new(WL::new_size(w.dir(), 32768, compression::Type::None).unwrap());
    let opts = HeadOptions {
        chunk_range: 1000,
        chunk_dir_root: wal.dir().to_path_buf(),
        ..HeadOptions::default()
    };
    hb.head = Head::new(Some(wal), None, opts, None).unwrap();
    hb.init(0).unwrap();

    // No appends after restarting. Hence all should return the last value.
    assert_eq!(1000, last_value(&hb.head, 10));
    assert_eq!(1000, last_value(&hb.head, 130));
    assert_eq!(1000, last_value(&hb.head, 160));
    assert_eq!(1000, last_value(&hb.head, 240));
    assert_eq!(1000, last_value(&hb.head, 500));

    // Cleanup appendIDs below 1000, which means the sample buffer is the only thing with appendIDs.
    let mut app = hb.head_appender();
    let r = app.append(0, &ls(&["foo", "bar"]), i, i as f64);
    i += 1;
    r.unwrap();
    commit(&mut app);
    assert_eq!(1001, last_value(&hb.head, 998));
    assert_eq!(1001, last_value(&hb.head, 999));
    assert_eq!(1001, last_value(&hb.head, 1000));
    assert_eq!(1001, last_value(&hb.head, 1001));
    assert_eq!(1001, last_value(&hb.head, 1002));
    assert_eq!(1001, last_value(&hb.head, 1003));

    // Cleanup appendIDs below 1002, but with a rollback.
    let mut app = hb.head_appender();
    app.append(0, &ls(&["foo", "bar"]), i, i as f64).unwrap();
    rollback(&mut app);
    assert_eq!(1001, last_value(&hb.head, 999));
    assert_eq!(1001, last_value(&hb.head, 1000));
    assert_eq!(1001, last_value(&hb.head, 1001));
    assert_eq!(1001, last_value(&hb.head, 1002));
    assert_eq!(1001, last_value(&hb.head, 1003));
    hb.close().unwrap();
}

#[test]
fn test_isolation_rollback() {
    if DEFAULT_ISOLATION_DISABLED {
        return; // Skipping test since tsdb isolation is disabled.
    }

    // Rollback after a failed append and test if the low watermark has progressed anyway.
    let hb = new_test_head(1000, compression::Type::None, false);

    let mut app = hb.appender(&ctx());
    app.append(0, &ls(&["foo", "bar"]), 0, 0.0).unwrap();
    commit(&mut *app);
    assert_eq!(1, hb.iso.low_watermark());

    let mut app = hb.appender(&ctx());
    app.append(0, &ls(&["foo", "bar"]), 1, 1.0).unwrap();
    assert!(app.append(0, &ls(&["foo", "bar", "foo", "baz"]), 2, 2.0).is_err());
    rollback(&mut *app);
    assert_eq!(2, hb.iso.low_watermark());

    let mut app = hb.appender(&ctx());
    app.append(0, &ls(&["foo", "bar"]), 3, 3.0).unwrap();
    commit(&mut *app);
    assert_eq!(
        3,
        hb.iso.low_watermark(),
        "Low watermark should proceed to 3 even if append #2 was rolled back."
    );
}

#[test]
fn test_isolation_low_watermark_monotonous() {
    if DEFAULT_ISOLATION_DISABLED {
        return; // Skipping test since tsdb isolation is disabled.
    }

    let hb = new_test_head(1000, compression::Type::None, false);

    let mut app1 = hb.appender(&ctx());
    app1.append(0, &ls(&["foo", "bar"]), 0, 0.0).unwrap();
    commit(&mut *app1);
    assert_eq!(1, hb.iso.low_watermark(), "Low watermark should by 1 after 1st append.");

    let mut app1 = hb.appender(&ctx());
    app1.append(0, &ls(&["foo", "bar"]), 1, 1.0).unwrap();
    assert_eq!(
        2,
        hb.iso.low_watermark(),
        "Low watermark should be two, even if append is not committed yet."
    );

    let mut app2 = hb.appender(&ctx());
    app2.append(0, &ls(&["foo", "baz"]), 1, 1.0).unwrap();
    commit(&mut *app2);
    assert_eq!(
        2,
        hb.iso.low_watermark(),
        "Low watermark should stay two because app1 is not committed yet."
    );

    let is = hb.iso.state(i64::MIN, i64::MAX);
    assert_eq!(
        2,
        hb.iso.low_watermark(),
        "After simulated read (iso state retrieved), low watermark should stay at 2."
    );

    commit(&mut *app1);
    assert_eq!(
        2,
        hb.iso.low_watermark(),
        "Even after app1 is committed, low watermark should stay at 2 because read is still ongoing."
    );

    is.close();
    assert_eq!(
        3,
        hb.iso.low_watermark(),
        "After read has finished (iso state closed), low watermark should jump to three."
    );
}

#[test]
fn test_isolation_append_id_zero_is_noop() {
    if DEFAULT_ISOLATION_DISABLED {
        return; // Skipping test since tsdb isolation is disabled.
    }

    let h = new_test_head(1000, compression::Type::None, false);
    h.init_time(0);

    let c_opts = ChunkOpts {
        chunk_disk_mapper: Some(&h.chunk_disk_mapper),
        chunk_range: h.chunk_range(),
        samples_per_chunk: tsdb::head::DEFAULT_SAMPLES_PER_CHUNK,
        ..Default::default()
    };

    let (s, _) = h.get_or_create(1, &ls(&["a", "1"]), false).unwrap();
    let mut g = s.lock();
    let (ok, _) = g.append(0, 0, 0.0, 0, &c_opts, &s.lset, s.r#ref);
    assert!(ok, "Series append failed.");
    assert_eq!(
        0,
        g.txs.as_ref().unwrap().tx_id_count,
        "Series should not have an appendID after append with appendID=0."
    );
}

#[test]
fn test_head_series_chunk_race() {
    for _ in 0..100 {
        test_head_series_chunk_race_once();
    }
}

/// Go `testHeadSeriesChunkRace`.
fn test_head_series_chunk_race_once() {
    let h = new_test_head(1000, compression::Type::None, false);
    h.init(0).unwrap();
    let mut app = h.appender(&ctx());

    let s2 = app.append(0, &ls(&["foo2", "bar"]), 5, 0.0).unwrap();
    for ts in 6..11 {
        app.append(s2, &labels::empty_labels(), ts, 0.0).unwrap();
    }
    commit(&mut *app);

    let matcher = eq("", "");
    let mut q = new_block_querier(&*h.head, 18, 22).unwrap();

    std::thread::scope(|sc| {
        let h2 = &h;
        let t = sc.spawn(move || {
            h2.update_min_max_time(20, 25);
            h2.gc();
        });
        let mut ss = q.select(&ctx(), false, None, std::slice::from_ref(&matcher));
        while ss.next() {}
        assert!(ss.err().is_none());
        t.join().unwrap();
    });
    q.close();
}

#[test]
fn test_isolation_without_add() {
    if DEFAULT_ISOLATION_DISABLED {
        return; // Skipping test since tsdb isolation is disabled.
    }

    let hb = new_test_head(1000, compression::Type::None, false);

    let mut app = hb.appender(&ctx());
    commit(&mut *app);

    let mut app = hb.appender(&ctx());
    app.append(0, &ls(&["foo", "baz"]), 1, 1.0).unwrap();
    commit(&mut *app);

    assert_eq!(
        hb.iso.last_append_id(),
        hb.iso.low_watermark(),
        "High watermark should be equal to the low watermark"
    );
}

/// TestIsolationSeek checks that seeking does not expose head samples committed after the querier was created.
#[test]
fn test_isolation_seek() {
    if DEFAULT_ISOLATION_DISABLED {
        return; // Skipping test since tsdb isolation is disabled.
    }

    let db = crate::common::new_test_db(vec![]);
    let lbls = ls(&["foo", "bar"]);
    let append_range = |from: i64, to: i64| {
        let mut app = db.appender(&ctx());
        for ts in from..=to {
            app.append(0, &lbls, ts, ts as f64).unwrap();
        }
        commit(&mut *app);
    };
    let timestamps = |from: i64, to: i64| -> Vec<i64> { (from..=to).collect() };

    append_range(1, 10);
    // A querier starting after the chunk's first sample trims the chunk.
    let mut queriers: Vec<(&str, i64, storage::BoxQuerier)> = Vec::new();
    for (name, mint) in [("untrimmed", 0i64), ("trimmed", 3)] {
        let q = db.querier(mint, 100).unwrap();
        queriers.push((name, mint, q));
    }
    // The queriers must not see samples committed after they were created, although these share a head chunk
    // with the ones they do see.
    append_range(11, 20);

    for (qname, qmint, q) in &queriers {
        // (name, seek (zero means advancing with Next only), want)
        let cases: Vec<(&str, i64, Vec<i64>)> = vec![
            ("next", 0, timestamps((*qmint).max(1), 10)),
            ("seek to a visible sample", 5, timestamps(5, 10)),
            ("seek past the visible samples", 15, vec![]),
        ];
        for (tname, seek, want) in cases {
            let name = format!("{qname}/{tname}");
            let mut ss = q.select(&ctx(), false, None, &[eq("foo", "bar")]);
            assert!(ss.next(), "{name}");
            let s = ss.at().clone();
            let mut it = s.iterator(None);
            let mut vt = it.next();
            if seek != 0 {
                vt = it.seek(seek);
            }
            let mut got = Vec::new();
            while vt == ValueType::Float {
                got.push(it.at_t());
                vt = it.next();
            }
            assert!(it.err().is_none(), "{name}");
            assert_eq!(want, got, "{name}");
            drop(it);
            assert!(!ss.next(), "{name}");
            assert!(ss.err().is_none(), "{name}");
        }
    }
    for (_, _, mut q) in queriers {
        assert!(q.close().is_none());
    }
}
