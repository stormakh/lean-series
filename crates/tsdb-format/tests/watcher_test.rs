//! Port of `tsdb/wlog/watcher_test.go`.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use model::histogram::{Histogram, Span, custom_values};
use model::labels::from_strings;
use tsdb_format::chunks::HeadSeriesRef;
use tsdb_format::record::{
    Encoder, MetricType, RefExemplar, RefFloatHistogramSample, RefHistogramSample, RefMetadata,
    RefSample, RefSeries,
};
use tsdb_format::wlog::compression::Type;
use tsdb_format::wlog::{
    PAGE_SIZE, READ_TIMEOUT, WL, Watcher, WatcherMetrics, WriteTo, checkpoint, last_checkpoint,
    segment_name, segments,
};

fn temp_dir(name: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let d = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "watcher_test-{name}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

/// Go `math/rand.Intn` (any random values work).
fn rand_intn(n: usize) -> usize {
    static SEED: AtomicU64 = AtomicU64::new(0x2545_F491_4F6C_DD1D);
    let mut x = SEED.load(Ordering::Relaxed);
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    SEED.store(x, Ordering::Relaxed);
    (x % n as u64) as usize
}

const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_RETRIES: usize = 100;

/// Go `retry`: executes `f` n times at each interval until it returns true.
fn retry(interval: Duration, n: usize, f: impl Fn() -> bool) {
    for _ in 0..=n {
        if f() {
            return;
        }
        std::thread::sleep(interval);
    }
    eprintln!("function returned false");
}

/// testify `require.Eventually`: `f` must return true within `wait_for`, checked every `tick`.
fn eventually(wait_for: Duration, tick: Duration, f: impl Fn() -> bool, msg: &str) {
    let deadline = Instant::now() + wait_for;
    loop {
        std::thread::sleep(tick);
        if f() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("condition never satisfied: {msg}");
        }
    }
}

/// Go `timestamp.FromTime`.
fn from_time(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

#[derive(Default)]
struct MockState {
    series_stored: Vec<RefSeries>,
    metadata_stored: Vec<RefMetadata>,
    samples_appended: Vec<RefSample>,
    exemplars_appended: Vec<RefExemplar>,
    histograms_appended: Vec<RefHistogramSample>,
    float_histograms_appended: Vec<RefFloatHistogramSample>,

    series_stores: usize,
    metadata_stores: usize,
    sample_appends: usize,
    exemplar_appends: usize,
    histogram_appends: usize,
    float_histograms_appends: usize,

    series_segment_indexes: HashMap<HeadSeriesRef, i64>,
}

struct WriteToMock {
    mu: Mutex<MockState>,
    // If nonzero, delay reads with a short sleep.
    delay: Duration,
}

impl WriteToMock {
    fn new(delay: Duration) -> Arc<WriteToMock> {
        Arc::new(WriteToMock {
            mu: Mutex::new(MockState::default()),
            delay,
        })
    }

    fn check_num_series(&self) -> usize {
        self.mu.lock().unwrap().series_segment_indexes.len()
    }
}

impl WriteTo for WriteToMock {
    fn append(&self, s: &[RefSample]) -> bool {
        let mut m = self.mu.lock().unwrap();
        m.sample_appends += 1;
        m.samples_appended.extend_from_slice(s);
        std::thread::sleep(self.delay);
        true
    }

    fn append_exemplars(&self, e: &[RefExemplar]) -> bool {
        let mut m = self.mu.lock().unwrap();
        std::thread::sleep(self.delay);
        m.exemplar_appends += 1;
        m.exemplars_appended.extend_from_slice(e);
        true
    }

    fn append_histograms(&self, h: &[RefHistogramSample]) -> bool {
        let mut m = self.mu.lock().unwrap();
        std::thread::sleep(self.delay);
        m.histogram_appends += 1;
        m.histograms_appended.extend_from_slice(h);
        true
    }

    fn append_float_histograms(&self, fh: &[RefFloatHistogramSample]) -> bool {
        let mut m = self.mu.lock().unwrap();
        std::thread::sleep(self.delay);
        m.float_histograms_appends += 1;
        m.float_histograms_appended.extend_from_slice(fh);
        true
    }

    fn store_series(&self, series: &[RefSeries], index: i64) {
        let mut m = self.mu.lock().unwrap();
        m.series_stores += 1;
        m.series_stored.extend_from_slice(series);
        for s in series {
            m.series_segment_indexes.insert(s.r#ref, index);
        }
        std::thread::sleep(self.delay);
    }

    fn store_metadata(&self, meta: &[RefMetadata]) {
        let mut m = self.mu.lock().unwrap();
        m.metadata_stores += 1;
        m.metadata_stored.extend_from_slice(meta);
        std::thread::sleep(self.delay);
    }

    fn update_series_segment(&self, series: &[RefSeries], index: i64) {
        let mut m = self.mu.lock().unwrap();
        for s in series {
            m.series_segment_indexes.insert(s.r#ref, index);
        }
    }

    fn series_reset(&self, index: i64) {
        // Check for series that are in segments older than the checkpoint
        // that were not also present in the checkpoint.
        let mut m = self.mu.lock().unwrap();
        m.series_segment_indexes.retain(|_, v| *v >= index);
    }
}

/// Go `testwal.Records`.
#[derive(Default)]
struct Records {
    series: Vec<RefSeries>,
    samples: Vec<RefSample>,
    histograms: Vec<RefHistogramSample>,
    float_histograms: Vec<RefFloatHistogramSample>,
    exemplars: Vec<RefExemplar>,
    metadata: Vec<RefMetadata>,
}

/// Go `testwal.RecordsCase` (the fields TestWatcher_Tail uses).
struct RecordsCase<'a> {
    series: usize,
    samples_per_series: usize,
    histograms_per_series: usize,
    float_histograms_per_series: usize,
    exemplars_per_series: usize,
    ref_padding: usize,
    ts_fn: &'a dyn Fn(usize, usize) -> i64,
    histogram_fn: Option<&'a dyn Fn(usize) -> Histogram>,
    no_st: bool,
}

/// Go `testwal.newTestHist`.
fn new_test_hist(i: usize) -> Histogram {
    Histogram {
        schema: 2,
        zero_threshold: 1e-128,
        zero_count: 0,
        count: 2,
        sum: 0.0,
        positive_spans: vec![Span {
            offset: 0,
            length: 1,
        }],
        positive_buckets: vec![i as i64 + 1],
        negative_spans: vec![Span {
            offset: 0,
            length: 1,
        }],
        negative_buckets: vec![-(i as i64) - 1],
        ..Default::default()
    }
}

/// Go `testwal.GenerateRecords`.
fn generate_records(c: RecordsCase) -> Records {
    let mut ret = Records::default();
    let st_for_ts = |ts: i64| if c.no_st { 0 } else { ts - 1 };
    for i in 0..c.series {
        let r = c.ref_padding + i;
        let hr = r as HeadSeriesRef;
        ret.series.push(RefSeries {
            r#ref: hr,
            labels: from_strings(&["__name__", &format!("test_metric_{r}")]),
        });
        ret.metadata.push(RefMetadata {
            r#ref: hr,
            r#type: MetricType::Counter as u8,
            unit: "unit text".to_string(),
            help: format!("help text for {r}"),
        });
        for j in 0..c.samples_per_series {
            let ts = (c.ts_fn)(r, j);
            ret.samples.push(RefSample {
                r#ref: hr,
                st: st_for_ts(ts),
                t: ts,
                v: r as f64,
            });
        }
        let h = Arc::new(match c.histogram_fn {
            Some(f) => f(r),
            None => new_test_hist(r),
        });
        for j in 0..c.histograms_per_series {
            let ts = (c.ts_fn)(r, j);
            ret.histograms.push(RefHistogramSample {
                r#ref: hr,
                st: st_for_ts(ts),
                t: ts,
                h: h.clone(),
            });
        }
        for j in 0..c.float_histograms_per_series {
            let ts = (c.ts_fn)(r, j);
            ret.float_histograms.push(RefFloatHistogramSample {
                r#ref: hr,
                st: st_for_ts(ts),
                t: ts,
                fh: Arc::new(h.to_float()),
            });
        }
        for j in 0..c.exemplars_per_series {
            ret.exemplars.push(RefExemplar {
                r#ref: hr,
                t: (c.ts_fn)(r, j),
                v: r as f64,
                labels: from_strings(&["trace_id", &format!("trace-{r}")]),
            });
        }
    }
    ret
}

fn enc_bytes(f: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut b = Vec::new();
    f(&mut b);
    b
}

fn series_rec(enc: &Encoder, r: usize, name: &str) -> Vec<u8> {
    enc_bytes(|b| {
        enc.series(
            &[RefSeries {
                r#ref: r as HeadSeriesRef,
                labels: from_strings(&["__name__", name]),
            }],
            b,
        )
    })
}

fn sample_rec(enc: &Encoder, r: usize, t: i64, v: f64) -> Vec<u8> {
    enc_bytes(|b| {
        enc.samples(
            &[RefSample {
                r#ref: r as HeadSeriesRef,
                st: 0,
                t,
                v,
            }],
            b,
        )
    })
}

fn keep_all(_: HeadSeriesRef) -> bool {
    true
}

// adapted: Go shares one WatcherMetrics registered in prometheus.DefaultRegisterer (wMetrics) across all
// watchers; here each watcher gets its own metrics (there is no registry yet).

#[test]
fn test_watcher_tail() {
    let batches = 3;
    let series_per_batch = 100;
    let exemplars_per_series = 2;
    for enable_st_storage in [false, true] {
        for compress in Type::ALL {
            let name = format!("compress={compress}/stStorage={enable_st_storage}");
            let now = SystemTime::now();
            let dir = temp_dir("watcher_tail");
            let wdir = dir.join("wal");
            let enc = Encoder { enable_st_storage };
            fs::create_dir(&wdir).unwrap();

            // Generate test records that represents batches of records data.
            // "batch" simulates a single scrape or RW/OTLP receive message.
            // Watcher does not inspect the data other than watching start timestamp, so records
            // does not need any certain shape.
            let ts = from_time(now + Duration::from_secs(1));
            let ts_fn = move |_: usize, _: usize| ts;
            let cb_hist = |r: usize| Histogram {
                schema: -53,
                zero_threshold: 1e-128,
                zero_count: 0,
                count: 2,
                sum: 0.0,
                positive_spans: vec![Span {
                    offset: 0,
                    length: 1,
                }],
                custom_values: custom_values(&[r as f64 + 2.0]),
                ..Default::default()
            };
            let mut records = Vec::new();
            let mut cb_histogram_records = Vec::new();
            for i in 0..batches {
                records.push(generate_records(RecordsCase {
                    no_st: !enable_st_storage,
                    ref_padding: i * series_per_batch,
                    ts_fn: &ts_fn,
                    series: series_per_batch,
                    samples_per_series: 10,
                    histograms_per_series: 5,
                    float_histograms_per_series: 5,
                    exemplars_per_series,
                    histogram_fn: None,
                }));
                cb_histogram_records.push(generate_records(RecordsCase {
                    no_st: !enable_st_storage,
                    ref_padding: i * series_per_batch,
                    ts_fn: &ts_fn,
                    series: series_per_batch,
                    samples_per_series: 0,
                    histograms_per_series: 5,
                    float_histograms_per_series: 5,
                    exemplars_per_series: 0,
                    histogram_fn: Some(&cb_hist),
                }));
            }

            // Create WAL for writing.
            let w = WL::new_size(&wdir, 128 * PAGE_SIZE, compress).unwrap();

            // Start watcher to that reads into a mock.
            let wt = WriteToMock::new(Duration::ZERO);
            let watcher = Watcher::new(
                Some(WatcherMetrics::new()),
                None,
                tsdb_format::record::Logger::nop(),
                "test",
                wt.clone(),
                &dir,
                true,
                true,
                true,
                None,
            );
            // Update the time because we just created samples around "now" time and watcher
            // only starts watching after that time.
            watcher.set_start_time(now);
            // Start spins up watcher loop in a thread.
            watcher.start();

            // Write to WAL like append commit would do, while watcher is tailing.

            // Write first a few samples before the start time, we don't expect those to be appended.
            w.log(&enc_bytes(|b| {
                enc.samples(
                    &[
                        RefSample {
                            r#ref: 1,
                            st: 0,
                            t: from_time(now),
                            v: 123.0,
                        },
                        RefSample {
                            r#ref: 2,
                            st: 0,
                            t: from_time(now),
                            v: 123.1,
                        },
                    ],
                    b,
                )
            }))
            .unwrap();

            for i in 0..batches {
                // Similar order as tsdb/head_appender.go.headAppenderBase.log
                w.log(&enc_bytes(|b| enc.series(&records[i].series, b)))
                    .unwrap();
                w.log(&enc_bytes(|b| enc.metadata(&records[i].metadata, b)))
                    .unwrap();
                w.log(&enc_bytes(|b| enc.samples(&records[i].samples, b)))
                    .unwrap();

                let mut hs = Vec::new();
                let cb_hs = enc.histogram_samples(&records[i].histograms, &mut hs);
                assert!(cb_hs.is_empty(), "{name}");
                w.log(&hs).unwrap();
                let mut fhs = Vec::new();
                let cb_fhs = enc.float_histogram_samples(&records[i].float_histograms, &mut fhs);
                assert!(cb_fhs.is_empty(), "{name}");
                w.log(&fhs).unwrap();
                w.log(&enc_bytes(|b| {
                    enc.custom_buckets_histogram_samples(&cb_histogram_records[i].histograms, b)
                }))
                .unwrap();
                w.log(&enc_bytes(|b| {
                    enc.custom_buckets_float_histogram_samples(
                        &cb_histogram_records[i].float_histograms,
                        b,
                    )
                }))
                .unwrap();

                w.log(&enc_bytes(|b| enc.exemplars(&records[i].exemplars, b)))
                    .unwrap();

                // Ping watcher for faster test. Watcher is checking for segment changes or 15s timeout.
                watcher.notify();
            }

            // Wait for watcher to lead all.
            eventually(
                Duration::from_secs(120),
                Duration::from_secs(1),
                || {
                    // Exemplars are logged as the last one, so assert on those.
                    wt.mu.lock().unwrap().exemplar_appends >= batches
                },
                &name,
            );

            {
                let m = wt.mu.lock().unwrap();

                assert_eq!(batches, m.series_stores, "{name}");
                assert_eq!(batches, m.metadata_stores, "{name}");
                assert_eq!(batches, m.sample_appends, "{name}");
                assert_eq!(2 * batches, m.histogram_appends, "{name}");
                assert_eq!(2 * batches, m.float_histograms_appends, "{name}");
                assert_eq!(batches, m.exemplar_appends, "{name}");

                for i in 0..batches {
                    let sector = records[i].series.len();
                    assert_eq!(
                        records[i].series,
                        m.series_stored[i * sector..(i + 1) * sector],
                        "{name} {i}"
                    );
                    let sector = records[i].metadata.len();
                    assert_eq!(
                        records[i].metadata,
                        m.metadata_stored[i * sector..(i + 1) * sector],
                        "{name} {i}"
                    );
                    let sector = records[i].samples.len();
                    assert_eq!(
                        records[i].samples,
                        m.samples_appended[i * sector..(i + 1) * sector],
                        "{name} {i}"
                    );

                    let sector =
                        records[i].histograms.len() + cb_histogram_records[i].histograms.len();
                    let nh = records[i].histograms.len();
                    assert_eq!(
                        records[i].histograms,
                        m.histograms_appended[i * sector..i * sector + nh],
                        "{name} {i}"
                    );
                    assert_eq!(
                        cb_histogram_records[i].histograms,
                        m.histograms_appended[i * sector + nh..(i + 1) * sector],
                        "{name}"
                    );
                    let sector = records[i].float_histograms.len()
                        + cb_histogram_records[i].float_histograms.len();
                    let nfh = records[i].float_histograms.len();
                    assert_eq!(
                        records[i].float_histograms,
                        m.float_histograms_appended[i * sector..i * sector + nfh],
                        "{name}"
                    );
                    assert_eq!(
                        cb_histogram_records[i].float_histograms,
                        m.float_histograms_appended[i * sector + nfh..(i + 1) * sector],
                        "{name}"
                    );

                    let sector = records[i].exemplars.len();
                    assert_eq!(
                        records[i].exemplars,
                        m.exemplars_appended[i * sector..(i + 1) * sector],
                        "{name}"
                    );
                }
            }
            watcher.stop();
            w.close().unwrap();
        }
    }
}

#[test]
fn test_read_to_end_no_checkpoint() {
    let page_size = 32 * 1024;
    let series_count = 10;
    let samples_count = 250;

    for enable_st_storage in [false, true] {
        for compress in Type::ALL {
            let name = format!("compress={compress},stStorage={enable_st_storage}");
            let dir = temp_dir("read_to_end_no_checkpoint");
            let wdir = dir.join("wal");
            fs::create_dir(&wdir).unwrap();

            let w = WL::new_size(&wdir, 128 * page_size, compress).unwrap();

            let mut recs: Vec<Vec<u8>> = Vec::new();

            let enc = Encoder { enable_st_storage };

            for i in 0..series_count {
                recs.push(series_rec(&enc, i, &format!("metric_{i}")));
                for j in 0..samples_count {
                    recs.push(sample_rec(&enc, j, i as i64, i as f64));

                    // Randomly batch up records.
                    if rand_intn(4) < 3 {
                        w.log_batch(&recs).unwrap();
                        recs.clear();
                    }
                }
            }
            w.log_batch(&recs).unwrap();
            segments(w.dir()).unwrap();

            let wt = WriteToMock::new(Duration::ZERO);
            let watcher = Watcher::new(
                Some(WatcherMetrics::new()),
                None,
                tsdb_format::record::Logger::nop(),
                "",
                wt.clone(),
                &dir,
                false,
                false,
                false,
                None,
            );
            // Go: overwriteReadTimeout(t, time.Second).
            watcher.set_read_timeout(Duration::from_secs(1));
            watcher.start();

            let expected = series_count;
            eventually(
                Duration::from_secs(20),
                Duration::from_secs(1),
                || wt.check_num_series() == expected,
                &name,
            );
            watcher.stop();
            w.close().unwrap();
        }
    }
}

#[test]
fn test_read_to_end_with_checkpoint() {
    let segment_size = 32 * 1024;
    // We need something similar to this # of series and samples
    // in order to get enough segments for us to checkpoint.
    let series_count = 10;
    let samples_count = 250;

    for enable_st_storage in [false, true] {
        for compress in Type::ALL {
            let name = format!("compress={compress},stStorage={enable_st_storage}");
            let dir = temp_dir("read_to_end_with_checkpoint");

            let wdir = dir.join("wal");
            fs::create_dir(&wdir).unwrap();

            let enc = Encoder { enable_st_storage };
            let w = WL::new_size(&wdir, segment_size, compress).unwrap();

            // Write to the initial segment then checkpoint.
            for i in 0..series_count {
                let r = i + 100;
                w.log(&series_rec(&enc, r, &format!("metric_{i}"))).unwrap();
                // Add in an unknown record type, which should be ignored.
                w.log(&[255]).unwrap();

                for _ in 0..samples_count {
                    let inner = rand_intn(r + 1);
                    w.log(&sample_rec(&enc, inner, i as i64, i as f64)).unwrap();
                }
            }

            let _ = checkpoint(&w, 0, 1, &keep_all, 0, enable_st_storage, true);
            let _ = w.truncate(1);

            // Write more records after checkpointing.
            for i in 0..series_count {
                w.log(&series_rec(&enc, i, &format!("metric_{i}"))).unwrap();

                for j in 0..samples_count {
                    w.log(&sample_rec(&enc, j, i as i64, i as f64)).unwrap();
                }
            }

            segments(w.dir()).unwrap();
            let wt = WriteToMock::new(Duration::ZERO);
            let watcher = Watcher::new(
                Some(WatcherMetrics::new()),
                None,
                tsdb_format::record::Logger::nop(),
                "",
                wt.clone(),
                &dir,
                false,
                false,
                false,
                None,
            );
            watcher.set_read_timeout(Duration::from_secs(1));
            watcher.start();

            let expected = series_count * 2;

            eventually(
                Duration::from_secs(10),
                Duration::from_secs(1),
                || wt.check_num_series() == expected,
                &name,
            );
            watcher.stop();
            w.close().unwrap();
        }
    }
}

#[test]
fn test_read_checkpoint() {
    let page_size = 32 * 1024;
    let series_count = 10;
    let samples_count = 250;

    for enable_st_storage in [false, true] {
        for compress in Type::ALL {
            let name = format!("compress={compress},stStorage={enable_st_storage}");
            let dir = temp_dir("read_checkpoint");

            let wdir = dir.join("wal");
            fs::create_dir(&wdir).unwrap();

            fs::File::create(segment_name(&wdir, 30)).unwrap();

            let enc = Encoder { enable_st_storage };
            let w = WL::new_size(&wdir, 128 * page_size, compress).unwrap();

            // Write to the initial segment then checkpoint.
            for i in 0..series_count {
                let r = i + 100;
                w.log(&series_rec(&enc, r, &format!("metric_{i}"))).unwrap();

                for _ in 0..samples_count {
                    let inner = rand_intn(r + 1);
                    w.log(&sample_rec(&enc, inner, i as i64, i as f64)).unwrap();
                }
            }
            w.next_segment_sync().unwrap();
            checkpoint(&w, 30, 31, &keep_all, 0, enable_st_storage, true).unwrap();
            w.truncate(32).unwrap();

            // Start read after checkpoint, no more data written.
            segments(w.dir()).unwrap();

            let wt = WriteToMock::new(Duration::ZERO);
            let watcher = Watcher::new(
                Some(WatcherMetrics::new()),
                None,
                tsdb_format::record::Logger::nop(),
                "",
                wt.clone(),
                &dir,
                false,
                false,
                false,
                None,
            );
            watcher.start();

            let expected_series = series_count;
            retry(DEFAULT_RETRY_INTERVAL, DEFAULT_RETRIES, || {
                wt.check_num_series() >= expected_series
            });
            watcher.stop();
            assert_eq!(expected_series, wt.check_num_series(), "{name}");
            w.close().unwrap();
        }
    }
}

#[test]
fn test_read_checkpoint_multiple_segments() {
    let page_size = 32 * 1024;

    let segments_n = 1;
    let series_count = 40;
    let samples_count = 500;

    for enable_st_storage in [false, true] {
        for compress in Type::ALL {
            let name = format!("compress={compress},stStorage={enable_st_storage}");
            let dir = temp_dir("read_checkpoint_multiple_segments");

            let wdir = dir.join("wal");
            fs::create_dir(&wdir).unwrap();

            let enc = Encoder { enable_st_storage };
            let w = WL::new_size(&wdir, page_size, compress).unwrap();

            // Write a bunch of data.
            for i in 0..segments_n {
                for j in 0..series_count {
                    let r = j + (i * 100);
                    w.log(&series_rec(&enc, r, &format!("metric_{i}"))).unwrap();

                    for _ in 0..samples_count {
                        let inner = rand_intn(r + 1);
                        w.log(&sample_rec(&enc, inner, i as i64, i as f64)).unwrap();
                    }
                }
            }
            w.close().unwrap();

            // At this point we should have at least 6 segments, lets create a checkpoint dir of the first 5.
            let checkpoint_dir = wdir.join("checkpoint.000004");
            fs::create_dir(&checkpoint_dir).unwrap();
            for i in 0..=4 {
                fs::rename(segment_name(&wdir, i), segment_name(&checkpoint_dir, i)).unwrap();
            }

            let wt = WriteToMock::new(Duration::ZERO);
            let watcher = Watcher::new(
                Some(WatcherMetrics::new()),
                None,
                tsdb_format::record::Logger::nop(),
                "",
                wt.clone(),
                &dir,
                false,
                false,
                false,
                None,
            );
            watcher.set_max_segment(-1);

            // Set the Watcher's metrics so they're not nil pointers.
            watcher.set_metrics();

            let (last_checkpoint, _) = last_checkpoint(watcher.wal_dir()).unwrap();

            watcher
                .read_checkpoint(&last_checkpoint, Watcher::read_segment)
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }
}

#[test]
fn test_checkpoint_series_reset() {
    let segment_size = 64 * 1024;
    // We need something similar to this # of series and samples
    // in order to get enough segments for us to checkpoint.
    let series_count = 30;
    let samples_count = 700;
    let test_cases = [
        (Type::None, false, 24usize),
        (Type::Snappy, false, 23),
        (Type::None, true, 20),
        (Type::Snappy, true, 20),
    ];

    let dir = temp_dir("checkpoint_series_reset");
    for (compress, enable_st_storage, segments_n) in test_cases {
        let name = format!("compress={compress},stStorage={enable_st_storage}");
        let subdir = dir.join(format!("{compress}-{enable_st_storage}"));
        fs::create_dir_all(&subdir).unwrap();
        let wdir = subdir.join("wal");
        fs::create_dir_all(&wdir).unwrap();

        let enc = Encoder { enable_st_storage };
        let w = WL::new_size(&wdir, segment_size, compress).unwrap();

        // Write to the initial segment, then checkpoint later.
        for i in 0..series_count {
            let r = i + 100;
            w.log(&series_rec(&enc, r, &format!("metric_{i}"))).unwrap();

            for _ in 0..samples_count {
                let inner = rand_intn(r + 1);
                w.log(&sample_rec(&enc, inner, i as i64, i as f64)).unwrap();
            }
        }

        segments(w.dir()).unwrap();

        let wt = WriteToMock::new(Duration::ZERO);
        let watcher = Watcher::new(
            Some(WatcherMetrics::new()),
            None,
            tsdb_format::record::Logger::nop(),
            "",
            wt.clone(),
            &subdir,
            false,
            false,
            false,
            None,
        );
        watcher.set_read_timeout(Duration::from_secs(1));
        watcher.set_max_segment(-1);
        watcher.start();

        let expected = series_count;
        retry(DEFAULT_RETRY_INTERVAL, DEFAULT_RETRIES, || {
            wt.check_num_series() >= expected
        });
        eventually(
            Duration::from_secs(10),
            Duration::from_secs(1),
            || wt.check_num_series() == series_count,
            &name,
        );

        checkpoint(&w, 2, 4, &keep_all, 0, true, false).unwrap();

        w.truncate(5).unwrap();

        let (_, cpi) = last_checkpoint(&wdir).unwrap();
        // When current segment is at or behind the checkpoint, garbage_collect_series
        // must skip the checkpoint without marking it as already processed.
        watcher.garbage_collect_series(cpi).unwrap();
        watcher.garbage_collect_series(cpi + 1).unwrap();

        watcher.stop();
        // If you modify the checkpoint and truncate segment #'s run the test to see how
        // many series records you end up with and change the last Equals check accordingly
        // or modify the Equals to Assert(len(wt.seriesLabels) < seriesCount*10)
        eventually(
            Duration::from_secs(20),
            Duration::from_secs(1),
            || wt.check_num_series() == segments_n,
            &format!("{name}: got {} series", wt.check_num_series()),
        );
        w.close().unwrap();
    }
}

#[test]
fn test_run_startup_time() {
    let page_size = 32 * 1024;
    let segments_n = 20;
    let series_count = 40;
    let samples_count = 500;

    for enable_st_storage in [false, true] {
        for compress in Type::ALL {
            let name = format!("compress={compress},stStorage={enable_st_storage}");
            let dir = temp_dir("run_startup_time");

            let wdir = dir.join("wal");
            fs::create_dir(&wdir).unwrap();

            let enc = Encoder { enable_st_storage };
            let w = WL::new_size(&wdir, page_size, compress).unwrap();

            for i in 0..segments_n {
                for j in 0..series_count {
                    let r = j + (i * 100);
                    w.log(&series_rec(&enc, r, &format!("metric_{i}"))).unwrap();

                    for _ in 0..samples_count {
                        let inner = rand_intn(r + 1);
                        w.log(&sample_rec(&enc, inner, i as i64, i as f64)).unwrap();
                    }
                }
            }
            w.close().unwrap();

            let wt = WriteToMock::new(Duration::ZERO);
            let watcher = Watcher::new(
                Some(WatcherMetrics::new()),
                None,
                tsdb_format::record::Logger::nop(),
                "",
                wt.clone(),
                &dir,
                false,
                false,
                false,
                None,
            );
            watcher.set_max_segment(segments_n as i64);

            watcher.set_metrics();
            let start_time = Instant::now();

            let err = watcher.run();
            assert!(start_time.elapsed() < READ_TIMEOUT, "{name}");
            err.unwrap();
        }
    }
}

fn generate_wal_records(
    w: &WL,
    segment: usize,
    series_count: usize,
    samples_count: usize,
    enable_st_storage: bool,
) -> tsdb_format::wlog::Result<()> {
    let enc = Encoder { enable_st_storage };
    for j in 0..series_count {
        let r = j + (segment * 100);
        w.log(&series_rec(&enc, r, &format!("metric_{segment}")))?;

        for _ in 0..samples_count {
            let inner = rand_intn(r + 1);
            w.log(&sample_rec(&enc, inner, segment as i64, segment as f64))?;
        }
    }
    Ok(())
}

#[test]
fn test_run_avoid_notify_when_behind() {
    let segment_size = PAGE_SIZE; // Smallest allowed segment size.
    let segments_to_write = 5;
    let segments_to_read = segments_to_write - 1;
    let series_count = 10;
    let samples_count = 50;

    for enable_st_storage in [false, true] {
        for compress in Type::ALL {
            let name = format!("compress={compress},stStorage={enable_st_storage}");
            let dir = temp_dir("run_avoid_notify_when_behind");

            let wdir = dir.join("wal");
            fs::create_dir(&wdir).unwrap();

            let w = WL::new_size(&wdir, segment_size, compress).unwrap();
            // Write to 00000000, the watcher will read series from it.
            generate_wal_records(&w, 0, series_count, samples_count, enable_st_storage).unwrap();
            // Create 00000001, the watcher will tail it once started.
            let _ = w.next_segment();

            // Set up the watcher and run it in the background.
            let wt = WriteToMock::new(Duration::from_millis(1));
            let watcher = Watcher::new(
                Some(WatcherMetrics::new()),
                None,
                tsdb_format::record::Logger::nop(),
                "",
                wt.clone(),
                &dir,
                false,
                false,
                false,
                None,
            );
            watcher.set_metrics();
            watcher.set_max_segment(segments_to_read as i64);

            let wr = watcher.clone();
            let g = std::thread::spawn(move || -> Result<(), String> {
                let start_time = Instant::now();
                wr.run().map_err(|e| e.to_string())?;
                // If the watcher was to wait for readTicker to read every new segment, it would need
                // readTimeout * segmentsToRead.
                let d = start_time.elapsed();
                if d > READ_TIMEOUT {
                    return Err(format!(
                        "watcher ran for {d:?}, it shouldn't rely on readTicker={READ_TIMEOUT:?} to read the new segments"
                    ));
                }
                Ok(())
            });

            // The watcher went through 00000000 and is tailing the next one.
            retry(DEFAULT_RETRY_INTERVAL, DEFAULT_RETRIES, || {
                wt.check_num_series() == series_count
            });

            // In the meantime, add some new segments in bulk.
            // We should end up with segmentsToWrite + 1 segments now.
            for i in 1..segments_to_write {
                generate_wal_records(&w, i, series_count, samples_count, enable_st_storage)
                    .unwrap();
                let _ = w.next_segment();
            }

            // Wait for the watcher.
            g.join().unwrap().unwrap_or_else(|e| panic!("{name}: {e}"));

            // All series and samples were read.
            assert_eq!(
                (segments_to_read + 1) * series_count,
                wt.check_num_series(),
                "{name}"
            ); // Series from 00000000 are also read.
            assert_eq!(
                wt.mu.lock().unwrap().samples_appended.len(),
                segments_to_read * series_count * samples_count,
                "{name}"
            );
            w.close().unwrap();
        }
    }
}
