//! Port of `watcher.go`: tails the TSDB WAL for remote write and sends what it reads to a `WriteTo`.
//!
//! Go runs the watcher in a goroutine with tickers and channels. Here it runs in one plain thread; a mutex and
//! a condition variable stand in for the `quit` and `readNotify` channels, and deadlines stand in for the
//! tickers (same periods as upstream: 5s checkpoint check, 100ms segment check, 15s read timeout).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::segment::{go_atoi, list_segments, open_read_segment, segment_name, segments};
use super::{Error, LiveReader, LiveReaderMetrics, Result, Segment, last_checkpoint};
use crate::record::{
    self, BuffersPool, Decoder, Logger, RefExemplar, RefFloatHistogramSample, RefHistogramSample,
    RefMetadata, RefSample, RefSeries,
};

/// Go `checkpointPeriod`.
const CHECKPOINT_PERIOD: Duration = Duration::from_secs(5);
/// Go `segmentCheckPeriod`.
const SEGMENT_CHECK_PERIOD: Duration = Duration::from_millis(100);
/// Go `readTimeout` (a package variable that tests overwrite; here `Watcher::set_read_timeout`).
pub const READ_TIMEOUT: Duration = Duration::from_secs(15);
/// The wait between two `run` attempts in the watcher loop.
const RETRY_PERIOD: Duration = Duration::from_secs(5);

/// Go `WriteTo`: used by the Watcher to send the samples it has read from the WAL on to somewhere else.
///
/// Implementations must be safe for use from several threads. The slices are only valid during the call;
/// copy what must be kept.
pub trait WriteTo: Send + Sync {
    /// `append` and the other `append_*` methods should block until the samples are fully accepted, e.g.
    /// enqueued in memory. Once returned, the WAL Watcher will not attempt to pass that data again.
    fn append(&self, samples: &[RefSample]) -> bool;
    fn append_exemplars(&self, exemplars: &[RefExemplar]) -> bool;
    fn append_histograms(&self, histograms: &[RefHistogramSample]) -> bool;
    fn append_float_histograms(&self, float_histograms: &[RefFloatHistogramSample]) -> bool;
    fn store_series(&self, series: &[RefSeries], index: i64);
    fn store_metadata(&self, metadata: &[RefMetadata]);

    /// `update_series_segment` and `series_reset` are intended for garbage collection:
    /// first `update_series_segment` is called on all current series, then `series_reset`.
    fn update_series_segment(&self, series: &[RefSeries], index: i64);
    /// Allows the deletion of all series created in a segment lower than the argument.
    fn series_reset(&self, index: i64);
}

/// Go `WatcherMetrics`, as plain atomics for one watcher (the Go metrics carry the watcher name in the
/// `consumer` label).
// TODO(metrics): register with the metrics registry once it exists.
#[derive(Debug, Default)]
pub struct WatcherMetrics {
    /// `prometheus_wal_watcher_records_read_total{consumer,type}`, indexed by record type byte (`Unknown` at 0).
    pub records_read: [AtomicU64; 16],
    /// `prometheus_wal_watcher_record_decode_failures_total{consumer}`.
    pub record_decode_fails: AtomicU64,
    /// `prometheus_wal_watcher_samples_sent_pre_tailing_total{consumer}`.
    pub samples_sent_pre_tailing: AtomicU64,
    /// `prometheus_wal_watcher_current_segment{consumer}`.
    pub current_segment: AtomicI64,
    /// `prometheus_wal_watcher_notifications_skipped_total{consumer}`.
    pub notifications_skipped: AtomicU64,
}

impl WatcherMetrics {
    /// Go `NewWatcherMetrics`.
    pub fn new() -> Arc<WatcherMetrics> {
        Arc::new(WatcherMetrics::default())
    }

    /// `records_read_total` for one record type.
    pub fn records_read(&self, t: record::Type) -> u64 {
        self.records_read[type_index(t)].load(Ordering::Relaxed)
    }
}

fn type_index(t: record::Type) -> usize {
    match t {
        record::Type::Unknown => 0,
        t => (t as u8 as usize) & 15,
    }
}

#[derive(Default)]
struct Signal {
    /// The watcher waits in `watch` and accepts a notification (Go: blocked in `select` on `readNotify`).
    waiting: bool,
    notified: bool,
}

/// What a `segment_read_fn` reads: Go `(*Watcher).readSegment` or `(*Watcher).readSegmentForGC`.
pub type SegmentReadFn = fn(&Watcher, &mut LiveReader<Segment>, i64, bool) -> Result<()>;

/// Go `Watcher`: watches the TSDB WAL for a given `WriteTo`. Create it with `Watcher::new`, which returns an
/// `Arc` so the background thread can share it.
pub struct Watcher {
    name: String,
    writer: Arc<dyn WriteTo>,
    record_buf: Arc<BuffersPool>,
    wal_dir: PathBuf,
    last_checkpoint: Mutex<PathBuf>,
    send_exemplars: bool,
    send_histograms: bool,
    send_metadata: bool,
    metrics: Option<Arc<WatcherMetrics>>,
    reader_metrics: Option<Arc<LiveReaderMetrics>>,

    start: Mutex<(SystemTime, i64)>, // start time and the same as a Prometheus timestamp
    send_samples: AtomicBool,

    read_timeout: Mutex<Duration>,
    quit: AtomicBool,
    signal: Mutex<Signal>,
    cond: Condvar,
    gc_running: AtomicBool,
    handle: Mutex<Option<JoinHandle<()>>>,

    /// For testing, stop when we hit this segment (-1: never).
    max_segment: AtomicI64,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Go `timestamp.FromTime`: milliseconds since the Unix epoch.
fn timestamp_from_time(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64 * 1000 + d.subsec_nanos() as i64 / 1_000_000,
        Err(e) => {
            let d = e.duration();
            -(d.as_secs() as i64 * 1000 + d.subsec_nanos().div_ceil(1_000_000) as i64)
        }
    }
}

impl Watcher {
    /// Go `NewWatcher`: a watcher of `<dir>/wal` that sends to `writer`. `record_buf` defaults to a new pool.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metrics: Option<Arc<WatcherMetrics>>,
        reader_metrics: Option<Arc<LiveReaderMetrics>>,
        name: &str,
        writer: Arc<dyn WriteTo>,
        dir: &Path,
        send_exemplars: bool,
        send_histograms: bool,
        send_metadata: bool,
        record_buf: Option<Arc<BuffersPool>>,
    ) -> Arc<Watcher> {
        Arc::new(Watcher {
            name: name.to_string(),
            writer,
            record_buf: record_buf.unwrap_or_else(|| Arc::new(BuffersPool::new())),
            wal_dir: dir.join("wal"),
            last_checkpoint: Mutex::new(PathBuf::new()),
            send_exemplars,
            send_histograms,
            send_metadata,
            metrics,
            reader_metrics,
            start: Mutex::new((UNIX_EPOCH, 0)),
            send_samples: AtomicBool::new(false),
            read_timeout: Mutex::new(READ_TIMEOUT),
            quit: AtomicBool::new(false),
            signal: Mutex::new(Signal::default()),
            cond: Condvar::new(),
            gc_running: AtomicBool::new(false),
            handle: Mutex::new(None),
            max_segment: AtomicI64::new(-1),
        })
    }

    /// The watcher name (Go `name`, the `consumer` metric label).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The watched WAL directory (`<dir>/wal`).
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    /// The metrics of this watcher, if any.
    pub fn metrics(&self) -> Option<&WatcherMetrics> {
        self.metrics.as_deref()
    }

    /// Go `MaxSegment`: for testing, stop when this segment is reached (-1: never).
    pub fn set_max_segment(&self, s: i64) {
        self.max_segment.store(s, Ordering::Relaxed);
    }

    /// Go `readTimeout`: how long to wait without notifications before reading anyway.
    pub fn set_read_timeout(&self, d: Duration) {
        *lock(&self.read_timeout) = d;
    }

    fn is_closed(&self) -> bool {
        self.quit.load(Ordering::Relaxed)
    }

    /// Go `Notify`: wakes the watcher to read new data. A notification that arrives while the watcher is not
    /// waiting for one is skipped (the watcher reads until EOF for each notification it receives).
    pub fn notify(&self) {
        let mut s = lock(&self.signal);
        if s.waiting && !s.notified {
            s.notified = true;
            drop(s);
            self.cond.notify_all();
            return;
        }
        drop(s);
        if let Some(m) = &self.metrics {
            m.notifications_skipped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Go `SetMetrics`. The metrics are bound at construction here, so this only exists for parity.
    pub fn set_metrics(&self) {}

    /// Go `Start`: runs the watcher loop in a background thread.
    pub fn start(self: &Arc<Self>) {
        self.set_metrics();
        // TODO(log): logger.Info("Starting WAL watcher", "queue", w.name)
        let w = self.clone();
        let h = std::thread::Builder::new()
            .name(format!("wal-watcher-{}", self.name))
            .spawn(move || w.run_loop())
            .expect("spawn WAL watcher thread");
        *lock(&self.handle) = Some(h);
    }

    /// Go `Stop`: stops the watcher and waits for its thread to end.
    pub fn stop(&self) {
        self.quit.store(true, Ordering::Relaxed);
        {
            let _s = lock(&self.signal);
            self.cond.notify_all();
        }
        if let Some(h) = lock(&self.handle).take() {
            let _ = h.join();
        }
        // TODO(metrics): Go deletes this consumer's label values here.
        // TODO(log): logger.Info("WAL watcher stopped", "queue", w.name)
    }

    /// Waits until `deadline`, a quit, or (with `accept_notify`) a notification. True if notified.
    fn wait(&self, deadline: Instant, accept_notify: bool) -> bool {
        let mut s = lock(&self.signal);
        s.waiting = accept_notify;
        loop {
            if self.is_closed() {
                break;
            }
            if s.notified {
                s.notified = false;
                s.waiting = false;
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            s = self
                .cond
                .wait_timeout(s, deadline - now)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        s.waiting = false;
        false
    }

    fn run_loop(self: Arc<Self>) {
        // We may encounter failures processing the WAL; we should wait and retry.
        while !self.is_closed() {
            self.set_start_time(SystemTime::now());
            if let Err(_e) = self.run() {
                // TODO(log): logger.Error("error tailing WAL", "err", err)
            }
            self.wait(Instant::now() + RETRY_PERIOD, false);
        }
    }

    /// Go `Run`: tails the WAL until `stop` is called or an error case is hit.
    pub fn run(self: &Arc<Self>) -> Result<()> {
        let (_, last_segment) = segments(&self.wal_dir).map_err(|e| e.wrap("Segments"))?;

        // We want to ensure this is false across iterations since
        // run will be called again if there was a failure to read the WAL.
        self.send_samples.store(false, Ordering::Relaxed);

        // TODO(log): logger.Info("Replaying WAL", "queue", w.name)

        // Backfill from the checkpoint first if it exists.
        let (last_checkpoint, checkpoint_index) = match last_checkpoint(&self.wal_dir) {
            Ok((d, i)) => {
                self.read_checkpoint(&d, Watcher::read_segment)
                    .map_err(|e| e.wrap("readCheckpoint"))?;
                (d, i)
            }
            Err(Error::NotFound) => (PathBuf::new(), 0),
            Err(e) => return Err(e.wrap("tsdb.LastCheckpoint")),
        };
        *lock(&self.last_checkpoint) = last_checkpoint;

        let mut current_segment = self.find_segment_for_index(checkpoint_index)?;

        while !self.is_closed() {
            if let Some(m) = &self.metrics {
                m.current_segment.store(current_segment, Ordering::Relaxed);
            }

            // On start, after reading the existing WAL for series records, we have a pointer to what is the
            // latest segment. On subsequent calls to this function, current_segment will have been incremented
            // and we should open that segment.
            match self.watch(current_segment, current_segment < last_segment) {
                Err(Error::Ignorable) | Ok(()) => {}
                Err(e) => return Err(e),
            }

            // For testing: stop when you hit a specific segment.
            if current_segment == self.max_segment.load(Ordering::Relaxed) {
                return Ok(());
            }

            current_segment += 1;
        }
        Ok(())
    }

    /// Go `findSegmentForIndex`: the first segment greater than or equal to `index`.
    fn find_segment_for_index(&self, index: i64) -> Result<i64> {
        let refs = list_segments(&self.wal_dir)?;
        for r in refs {
            if r.index >= index {
                return Ok(r.index);
            }
        }
        Err(Error::msg("failed to find segment for index"))
    }

    fn read_and_handle_error(
        &self,
        r: &mut LiveReader<Segment>,
        segment_num: i64,
        only_series: bool,
        size: i64,
    ) -> Result<()> {
        let err = self.read_segment(r, segment_num, only_series);

        // Ignore all errors reading to end of segment whilst replaying the WAL.
        if only_series {
            if let Err(e) = &err
                && !e.is_eof()
            {
                // TODO(log): logger.Warn("Ignoring error reading to end of segment, may have dropped data", ...)
            } else if r.offset() != size {
                // TODO(log): logger.Warn("Expected to have read whole segment, may have dropped data", ...)
            }
            return Err(Error::Ignorable);
        }

        // Otherwise, when we are tailing, non-EOFs are fatal.
        match err {
            Err(e) if !e.is_eof() => Err(e),
            _ => Ok(()),
        }
    }

    /// Go `watch`. With `only_series` false the reader is on a segment that is still being written; with true it
    /// is a full segment that is replayed on start to cache the series records.
    fn watch(self: &Arc<Self>, segment_num: i64, only_series: bool) -> Result<()> {
        let segment = open_read_segment(&segment_name(&self.wal_dir, segment_num))?;
        let mut reader = LiveReader::new(self.reader_metrics.clone(), segment);

        let size = i64::MAX;
        if only_series {
            let size = get_segment_size(&self.wal_dir, segment_num)
                .map_err(|e| e.wrap("getSegmentSize"))?;
            return self.read_and_handle_error(&mut reader, segment_num, only_series, size);
        }

        let now = Instant::now();
        let mut next_checkpoint = now + CHECKPOINT_PERIOD;
        let mut next_segment_check = now + SEGMENT_CHECK_PERIOD;
        let mut next_read = now + *lock(&self.read_timeout);

        loop {
            let deadline = next_checkpoint.min(next_segment_check).min(next_read);
            let notified = self.wait(deadline, true);
            if self.is_closed() {
                return Ok(());
            }

            if notified {
                self.read_and_handle_error(&mut reader, segment_num, only_series, size)?;
                // Reset the read timer so we don't read too often.
                next_read = Instant::now() + *lock(&self.read_timeout);
                continue;
            }

            let now = Instant::now();
            if now >= next_checkpoint {
                next_checkpoint = advance(next_checkpoint, CHECKPOINT_PERIOD, now);
                // Periodically check if there is a new checkpoint so we can garbage
                // collect labels. As this is considered an optimisation, we ignore
                // errors during checkpoint processing. Doing the process asynchronously
                // allows the current WAL segment to be processed while reading the
                // checkpoint.
                if !self.gc_running.swap(true, Ordering::AcqRel) {
                    let w = self.clone();
                    std::thread::spawn(move || {
                        if let Err(_e) = w.garbage_collect_series(segment_num) {
                            // TODO(log): logger.Warn("Error process checkpoint", "err", err)
                        }
                        w.gc_running.store(false, Ordering::Release);
                    });
                }
                // Else: currently doing a garbage collect, try again later.
            }

            // If a newer segment is produced, read the current one until the end and move on.
            if now >= next_segment_check {
                next_segment_check = advance(next_segment_check, SEGMENT_CHECK_PERIOD, now);
                let (_, last) = segments(&self.wal_dir).map_err(|e| e.wrap("Segments"))?;
                if last > segment_num {
                    return self.read_and_handle_error(&mut reader, segment_num, only_series, size);
                }
            }

            // We haven't read due to a notification in quite some time, try reading anyways.
            if now >= next_read {
                // TODO(log): logger.Debug("Watcher is reading the WAL due to timeout, ...")
                self.read_and_handle_error(&mut reader, segment_num, only_series, size)?;
                // Reset the read timer so we don't read too often.
                next_read = Instant::now() + *lock(&self.read_timeout);
            }
        }
    }

    /// Go `garbageCollectSeries`: reads a new checkpoint, if there is one, to update the series segments and
    /// reset older series in the writer.
    pub fn garbage_collect_series(&self, segment_num: i64) -> Result<()> {
        let dir = match last_checkpoint(&self.wal_dir) {
            Ok((d, _)) => d,
            Err(Error::NotFound) => PathBuf::new(),
            Err(e) => return Err(e.wrap("tsdb.LastCheckpoint")),
        };

        if dir.as_os_str().is_empty() || dir == *lock(&self.last_checkpoint) {
            return Ok(());
        }

        let index =
            checkpoint_num(&dir).map_err(|e| e.wrap("error parsing checkpoint filename"))?;

        if index >= segment_num {
            // TODO(log): logger.Debug("Current segment is behind the checkpoint, skipping reading of checkpoint")
            return Ok(());
        }

        // TODO(log): logger.Debug("New checkpoint detected", "new", dir, "currentSegment", segmentNum)

        self.read_checkpoint(&dir, Watcher::read_segment_for_gc)
            .map_err(|e| e.wrap("readCheckpoint"))?;
        *lock(&self.last_checkpoint) = dir;

        // Clear series with a checkpoint or segment index # lower than the checkpoint we just read.
        self.writer.series_reset(index);
        Ok(())
    }

    fn count_read(&self, t: record::Type) {
        if let Some(m) = &self.metrics {
            m.records_read[type_index(t)].fetch_add(1, Ordering::Relaxed);
        }
    }

    fn count_decode_fail(&self) {
        if let Some(m) = &self.metrics {
            m.record_decode_fails.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn mark_sending(&self) {
        if !self.send_samples.load(Ordering::Relaxed) {
            self.send_samples.store(true, Ordering::Relaxed);
            // TODO(log): logger.Info("Done replaying WAL", "duration", time.Since(w.startTime))
        }
    }

    /// Go `readSegment`: reads records from `r` and passes them to the writer. Returns `Err(Eof)` at the end of
    /// the data written so far (the common case when tailing).
    pub fn read_segment(
        &self,
        r: &mut LiveReader<Segment>,
        segment_num: i64,
        only_series: bool,
    ) -> Result<()> {
        let mut series = self.record_buf.get_ref_series(512);
        let mut samples = self.record_buf.get_samples(512);
        let mut exemplars = self.record_buf.get_exemplars(512);
        let mut histograms = self.record_buf.get_histograms(512);
        let mut float_histograms = self.record_buf.get_float_histograms(512);
        let mut metadata = self.record_buf.get_metadata(512);

        let res = self.read_segment_inner(
            r,
            segment_num,
            only_series,
            &mut series,
            &mut samples,
            &mut exemplars,
            &mut histograms,
            &mut float_histograms,
            &mut metadata,
        );

        self.record_buf.put_ref_series(series);
        self.record_buf.put_samples(samples);
        self.record_buf.put_exemplars(exemplars);
        self.record_buf.put_histograms(histograms);
        self.record_buf.put_float_histograms(float_histograms);
        self.record_buf.put_metadata(metadata);
        res
    }

    #[allow(clippy::too_many_arguments)]
    fn read_segment_inner(
        &self,
        r: &mut LiveReader<Segment>,
        segment_num: i64,
        only_series: bool,
        series: &mut Vec<RefSeries>,
        samples: &mut Vec<RefSample>,
        exemplars: &mut Vec<RefExemplar>,
        histograms: &mut Vec<RefHistogramSample>,
        float_histograms: &mut Vec<RefFloatHistogramSample>,
        metadata: &mut Vec<RefMetadata>,
    ) -> Result<()> {
        let start_timestamp = lock(&self.start).1;
        let mut dec = Decoder::new(Logger::nop()); // One decoder per WAL segment.
        while r.next() && !self.is_closed() {
            let rec = r.record();
            let typ = dec.r#type(rec);
            self.count_read(typ);

            match typ {
                record::Type::Series => {
                    series.clear();
                    if let Err(e) = dec.series(rec, series) {
                        self.count_decode_fail();
                        return Err(rec_err(e));
                    }
                    self.writer.store_series(series, segment_num);
                }
                record::Type::Samples | record::Type::SamplesV2 => {
                    // When only caching series we can ignore any samples records we see.
                    // This speeds up replay of the WAL by > 10x.
                    if only_series {
                        continue;
                    }
                    samples.clear();
                    if let Err(e) = dec.samples(rec, samples) {
                        self.count_decode_fail();
                        return Err(rec_err(e));
                    }
                    samples.retain(|s| s.t > start_timestamp);
                    if !samples.is_empty() {
                        self.mark_sending();
                        self.writer.append(samples);
                    }
                }
                record::Type::Exemplars => {
                    // Skip if experimental "exemplars over remote write" is not enabled.
                    if !self.send_exemplars || only_series {
                        continue;
                    }
                    exemplars.clear();
                    if let Err(e) = dec.exemplars(rec, exemplars) {
                        self.count_decode_fail();
                        return Err(rec_err(e));
                    }
                    self.writer.append_exemplars(exemplars);
                }
                record::Type::HistogramSamples
                | record::Type::CustomBucketsHistogramSamples
                | record::Type::HistogramSamplesV2 => {
                    // Skip if "native histograms over remote write" is not enabled.
                    if !self.send_histograms || only_series {
                        continue;
                    }
                    histograms.clear();
                    if let Err(e) = dec.histogram_samples(rec, histograms) {
                        self.count_decode_fail();
                        return Err(rec_err(e));
                    }
                    histograms.retain(|h| h.t > start_timestamp);
                    if !histograms.is_empty() {
                        self.mark_sending();
                        self.writer.append_histograms(histograms);
                    }
                }
                record::Type::FloatHistogramSamples
                | record::Type::CustomBucketsFloatHistogramSamples
                | record::Type::FloatHistogramSamplesV2 => {
                    if !self.send_histograms || only_series {
                        continue;
                    }
                    float_histograms.clear();
                    if let Err(e) = dec.float_histogram_samples(rec, float_histograms) {
                        self.count_decode_fail();
                        return Err(rec_err(e));
                    }
                    float_histograms.retain(|fh| fh.t > start_timestamp);
                    if !float_histograms.is_empty() {
                        self.mark_sending();
                        self.writer.append_float_histograms(float_histograms);
                    }
                }
                record::Type::Metadata => {
                    if !self.send_metadata {
                        continue;
                    }
                    metadata.clear();
                    if let Err(e) = dec.metadata(rec, metadata) {
                        self.count_decode_fail();
                        return Err(rec_err(e));
                    }
                    self.writer.store_metadata(metadata);
                }
                record::Type::Unknown => {
                    // Could be corruption, or reading from a WAL from a newer Prometheus.
                    self.count_decode_fail();
                }
                _ => {
                    // We're not interested in other types of records.
                }
            }
        }
        // NOTE: Err(Eof) is a common case when tailing.
        // Don't wrap error, callers are expected to handle EOF and wrap accordingly.
        reader_result(r)
    }

    /// Go `readSegmentForGC`: goes through all series in a segment updating the segment number, so older
    /// series can be deleted.
    pub fn read_segment_for_gc(
        &self,
        r: &mut LiveReader<Segment>,
        segment_num: i64,
        _: bool,
    ) -> Result<()> {
        let mut series = self.record_buf.get_ref_series(512);
        let res = (|| {
            let mut dec = Decoder::new(Logger::nop());
            while r.next() && !self.is_closed() {
                let rec = r.record();
                let typ = dec.r#type(rec);
                self.count_read(typ);

                match typ {
                    record::Type::Series => {
                        series.clear();
                        if let Err(e) = dec.series(rec, &mut series) {
                            self.count_decode_fail();
                            return Err(rec_err(e));
                        }
                        self.writer.update_series_segment(&series, segment_num);
                    }
                    record::Type::Unknown => {
                        // Could be corruption, or reading from a WAL from a newer Prometheus.
                        self.count_decode_fail();
                    }
                    _ => {
                        // We're only interested in series.
                    }
                }
            }
            reader_result(r)
        })();
        self.record_buf.put_ref_series(series);
        res
    }

    /// Go `SetStartTime`.
    pub fn set_start_time(&self, t: SystemTime) {
        *lock(&self.start) = (t, timestamp_from_time(t));
    }

    /// Go `readCheckpoint`: reads all the series records from a checkpoint directory.
    pub fn read_checkpoint(&self, checkpoint_dir: &Path, read_fn: SegmentReadFn) -> Result<()> {
        // TODO(log): logger.Debug("Reading checkpoint", "dir", checkpointDir)
        let index = checkpoint_num(checkpoint_dir).map_err(|e| e.wrap("checkpointNum"))?;

        // Ensure we read the whole contents of every segment in the checkpoint dir.
        let segs = list_segments(checkpoint_dir)
            .map_err(|e| e.wrap("unable to get segments checkpoint dir"))?;
        for seg_ref in segs {
            let size = get_segment_size(checkpoint_dir, seg_ref.index)
                .map_err(|e| e.wrap("getSegmentSize"))?;

            let sr = open_read_segment(&segment_name(checkpoint_dir, seg_ref.index))
                .map_err(|e| e.wrap("unable to open segment"))?;

            let mut r = LiveReader::new(self.reader_metrics.clone(), sr);
            let res = read_fn(self, &mut r, index, true);
            if let Err(e) = res
                && !e.is_eof()
            {
                return Err(e.wrap(&format!("readSegment {index}")));
            }

            if r.offset() != size {
                return Err(Error::Msg(format!(
                    "readCheckpoint wasn't able to read all data from the checkpoint {}/{:08}, size: {}, totalRead: {}",
                    checkpoint_dir.display(),
                    seg_ref.index,
                    size,
                    r.offset()
                )));
            }
        }

        // TODO(log): logger.Debug("Read series references from checkpoint", "checkpoint", checkpointDir)
        Ok(())
    }
}

/// The next tick of a Go ticker: ticks that were missed are dropped.
fn advance(next: Instant, period: Duration, now: Instant) -> Instant {
    let n = next + period;
    if n <= now { now + period } else { n }
}

fn reader_result(r: &LiveReader<Segment>) -> Result<()> {
    match r.err() {
        Some(e) => Err(e.clone()),
        None => Ok(()),
    }
}

fn rec_err(e: record::Error) -> Error {
    Error::Msg(e.to_string())
}

/// Go `checkpointNum`: the index of a checkpoint directory named `checkpoint.NNN`.
fn checkpoint_num(dir: &Path) -> Result<i64> {
    // Checkpoint dir names are in the format checkpoint.000001
    // dir may contain a hidden directory, so only check the base directory
    let base = dir.file_name().and_then(|s| s.to_str()).unwrap_or("");
    let chunks: Vec<&str> = base.split('.').collect();
    if chunks.len() != 2 {
        return Err(Error::Msg(format!(
            "invalid checkpoint dir string: {}",
            dir.display()
        )));
    }
    go_atoi(chunks[1])
        .ok_or_else(|| Error::Msg(format!("invalid checkpoint dir string: {}", dir.display())))
}

/// Go `getSegmentSize`.
fn get_segment_size(dir: &Path, index: i64) -> Result<i64> {
    Ok(fs::metadata(segment_name(dir, index))?.len() as i64)
}

impl super::WriteNotified for Watcher {
    fn notify(&self) {
        Watcher::notify(self);
    }
}
