//! Go `WL`: a write log that stores records in segment files (the WAL and the WBL).

use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::Instant;

use super::compression::{EncodeBuffer, Type};
use super::segment::{Segment, create_segment, list_segments, segment_name, segments};
use super::{
    DEFAULT_SEGMENT_SIZE, Error, PAGE_SIZE, RECORD_HEADER_SIZE, Reader, RecType, Result,
    SNAPPY_MASK, WBL_DIR_NAME, WriteNotified, ZSTD_MASK,
};
use crate::encoding::crc32c;
use crate::fileutil;
use crate::record::Logger;

/// Go `page`: an in-memory buffer used to batch disk writes.
/// Records bigger than the page size are split and flushed separately.
/// A flush is triggered when a single record doesn't fit the page size or
/// when the next record can't fit in the remaining free page space.
struct Page {
    alloc: usize,
    flushed: usize,
    buf: Box<[u8]>,
}

impl Page {
    fn new() -> Page {
        Page {
            alloc: 0,
            flushed: 0,
            buf: vec![0u8; PAGE_SIZE].into_boxed_slice(),
        }
    }

    fn remaining(&self) -> usize {
        PAGE_SIZE - self.alloc
    }

    fn full(&self) -> bool {
        PAGE_SIZE - self.alloc < RECORD_HEADER_SIZE
    }

    fn reset(&mut self) {
        self.buf.fill(0);
        self.alloc = 0;
        self.flushed = 0;
    }
}

/// Go `wlMetrics`, as plain atomics. Names are the upstream metric names after the prefix
/// `prometheus_tsdb_wal_` (or `prometheus_tsdb_out_of_order_wbl_` for a directory named `wbl`).
/// The server exposes them (with `WL::size` as `storage_size_bytes`) in its TSDB collector.
#[derive(Debug, Default)]
pub struct WlMetrics {
    /// The metric name prefix.
    pub prefix: &'static str,
    /// `fsync_duration_seconds` (summary): number of observations.
    pub fsync_duration_count: AtomicU64,
    /// `fsync_duration_seconds` (summary): sum in nanoseconds.
    pub fsync_duration_sum_nanos: AtomicU64,
    /// `page_flushes_total`.
    pub page_flushes: AtomicU64,
    /// `completed_pages_total`.
    pub page_completions: AtomicU64,
    /// `truncations_failed_total`.
    pub truncate_fail: AtomicU64,
    /// `truncations_total`.
    pub truncate_total: AtomicU64,
    /// `segment_current` (gauge).
    pub current_segment: AtomicI64,
    /// `writes_failed_total`.
    pub writes_failed: AtomicU64,
    /// `record_part_writes_total`.
    pub record_part_writes: AtomicU64,
    /// `record_parts_bytes_written_total`.
    pub record_part_bytes: AtomicU64,
    /// `record_bytes_saved_total{compression="<type>"}`.
    pub record_bytes_saved: AtomicU64,
    // `storage_size_bytes` (gauge func) is `WL::size`.
}

impl WlMetrics {
    fn new(dir: &Path) -> WlMetrics {
        let prefix = if dir.file_name().is_some_and(|n| n == WBL_DIR_NAME) {
            "prometheus_tsdb_out_of_order_wbl_"
        } else {
            "prometheus_tsdb_wal_"
        };
        WlMetrics {
            prefix,
            ..Default::default()
        }
    }

    fn observe_fsync(&self, start: Instant) {
        self.fsync_duration_count.fetch_add(1, Ordering::Relaxed);
        self.fsync_duration_sum_nanos
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
}

struct Core {
    dir: PathBuf,
    segment_size: usize,
    segment: Option<Segment>, // Active segment.
    done_pages: usize,        // Pages written to the segment.
    page: Page,               // Active page.
    closed: bool,             // To allow calling close() more than once without blocking.
    compress: Type,
    /// Go `actorc`: background fsync and close of previous segments.
    pending: Vec<JoinHandle<()>>,
    metrics: Arc<WlMetrics>,
    logger: Logger,
}

struct State {
    core: Core,
    c_enc: EncodeBuffer,
}

/// Go `WL`: a write log that stores records in segment files.
/// It must be read from start to end once before logging new data.
/// If an error occurs during read, the repair procedure must be called
/// before it's safe to do further writes.
///
/// Segments are written to in pages of 32KB, with records possibly split
/// across page boundaries.
/// Records are never split across segments to allow full segments to be
/// safely truncated. It also ensures that torn writes never corrupt records
/// beyond the most recent segment.
pub struct WL {
    dir: PathBuf,
    compress: Type,
    state: Mutex<State>,
    metrics: Arc<WlMetrics>,
    write_notified: Mutex<Option<Arc<dyn WriteNotified>>>,
    logger: Logger,
}

fn segment_err() -> Error {
    Error::msg("wlog has no active segment")
}

impl WL {
    /// Go `New`: a new WAL over the given directory with the default segment size (128 MiB).
    pub fn new(dir: impl AsRef<Path>, compress: Type) -> Result<WL> {
        WL::new_size(dir, DEFAULT_SEGMENT_SIZE, compress)
    }

    /// Go `NewSize`: a new write log over the given directory. New segments are created with the given size.
    /// It always starts a new segment after the last existing one.
    pub fn new_size(dir: impl AsRef<Path>, segment_size: usize, compress: Type) -> Result<WL> {
        let dir = dir.as_ref().to_path_buf();
        if !segment_size.is_multiple_of(PAGE_SIZE) {
            return Err(Error::msg("invalid segment size"));
        }
        fs::create_dir_all(&dir).map_err(|e| Error::from(e).wrap("create dir"))?;
        let metrics = Arc::new(WlMetrics::new(&dir));
        let mut core = Core {
            dir: dir.clone(),
            segment_size,
            segment: None,
            done_pages: 0,
            page: Page::new(),
            closed: false,
            compress,
            pending: Vec::new(),
            metrics: metrics.clone(),
            logger: Logger::nop(),
        };

        let (_, last) = segments(&dir).map_err(|e| e.wrap("get segment range"))?;

        // Index of the Segment we want to open and write to.
        // If some segments already exist create one with a higher index than the last segment.
        let write_segment_index = if last != -1 { last + 1 } else { 0 };

        let segment = create_segment(&dir, write_segment_index)?;
        core.set_segment(segment)?;

        Ok(WL {
            dir,
            compress,
            state: Mutex::new(State {
                core,
                c_enc: EncodeBuffer::new(),
            }),
            metrics,
            write_notified: Mutex::new(None),
            logger: Logger::nop(),
        })
    }

    /// Go `Open`: an existing WAL, only for reading and truncation (no active segment).
    pub fn open(dir: impl AsRef<Path>) -> Result<WL> {
        let dir = dir.as_ref().to_path_buf();
        let metrics = Arc::new(WlMetrics::new(&dir));
        let core = Core {
            dir: dir.clone(),
            segment_size: DEFAULT_SEGMENT_SIZE,
            segment: None,
            done_pages: 0,
            page: Page::new(),
            closed: false,
            compress: Type::None,
            pending: Vec::new(),
            metrics: metrics.clone(),
            logger: Logger::nop(),
        };
        Ok(WL {
            dir,
            compress: Type::None,
            state: Mutex::new(State {
                core,
                c_enc: EncodeBuffer::new(),
            }),
            metrics,
            write_notified: Mutex::new(None),
            logger: Logger::nop(),
        })
    }

    /// Sets the logger (Go passes it to `NewSize` and `Open`; the default discards everything).
    pub fn with_logger(mut self, logger: Logger) -> WL {
        self.state.get_mut().unwrap_or_else(|e| e.into_inner()).core.logger = logger.clone();
        self.logger = logger;
        self
    }

    /// The logger of the write log.
    pub fn logger(&self) -> &Logger {
        &self.logger
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Go `CompressionType`.
    pub fn compression_type(&self) -> Type {
        self.compress
    }

    /// Go `Dir`.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The metrics of this log.
    pub fn metrics(&self) -> &WlMetrics {
        &self.metrics
    }

    /// Go `SetWriteNotified`.
    pub fn set_write_notified(&self, wn: Option<Arc<dyn WriteNotified>>) {
        *self
            .write_notified
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = wn;
    }

    /// Go `WriteNotified` field.
    pub fn write_notified(&self) -> Option<Arc<dyn WriteNotified>> {
        self.write_notified
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Index of the active segment (Go `w.segment.Index()`), if any.
    pub fn segment_index(&self) -> Option<i64> {
        self.lock().core.segment.as_ref().map(Segment::index)
    }

    /// Pages written to the active segment (Go `w.donePages`).
    pub fn done_pages(&self) -> usize {
        self.lock().core.done_pages
    }

    /// Runs `f` on the active segment. Tests use it to replace the segment file.
    pub fn with_active_segment<T>(&self, f: impl FnOnce(&mut Segment) -> T) -> Option<T> {
        self.lock().core.segment.as_mut().map(f)
    }

    /// Go `Repair`: attempts to repair the WAL based on the error. It discards all data after the corruption.
    pub fn repair(&self, orig_err: &Error) -> Result<()> {
        // We could probably have a mode that only discards torn records right around
        // the corruption to preserve as data much as possible.
        // But that's not generally applicable if the records have any kind of causality.
        // Maybe as an extra mode in the future if mid-WAL corruptions become
        // a frequent concern.
        let Some(cerr) = orig_err.corruption() else {
            return Err(orig_err.clone().wrap("cannot handle error"));
        };
        if cerr.segment < 0 {
            return Err(Error::msg("corruption error does not specify position"));
        }
        self.logger.warn(
            "Starting corruption repair",
            &[("segment", &cerr.segment), ("offset", &cerr.offset)],
        );

        let mut st = self.lock();
        let State { core, c_enc } = &mut *st;

        // All segments behind the corruption can no longer be used.
        let segs = list_segments(&self.dir).map_err(|e| e.wrap("list segments"))?;
        self.logger.warn(
            "Deleting all segments newer than corrupted segment",
            &[("segment", &cerr.segment)],
        );

        for s in &segs {
            if core
                .segment
                .as_ref()
                .is_some_and(|seg| seg.index() == s.index)
            {
                // The active segment needs to be removed,
                // close it first (Windows!). Can be closed safely
                // as we set the current segment to repaired file
                // below.
                if let Some(seg) = core.segment.take() {
                    seg.close()
                        .map_err(|e| Error::from(e).wrap("close active segment"))?;
                }
            }
            if s.index <= cerr.segment {
                continue;
            }
            fs::remove_file(self.dir.join(&s.name))
                .map_err(|e| Error::from(e).wrap(&format!("delete segment:{}", s.index)))?;
        }
        // Regardless of the corruption offset, no record reaches into the previous segment.
        // So we can safely repair the WAL by removing the segment and re-inserting all
        // its records up to the corruption.
        self.logger
            .warn("Rewrite corrupted segment", &[("segment", &cerr.segment)]);

        let fn_ = segment_name(&self.dir, cerr.segment);
        let mut tmpfn = fn_.clone().into_os_string();
        tmpfn.push(".repair");
        let tmpfn = PathBuf::from(tmpfn);

        fileutil::rename(&fn_, &tmpfn)?;
        // Create a clean segment and make it the active one.
        let s = create_segment(&self.dir, cerr.segment)?;
        core.set_segment(s)?;

        let f = File::open(&tmpfn).map_err(|e| Error::from(e).wrap("open segment"))?;
        let mut r = Reader::new(BufReader::new(f));

        while r.next() {
            // Add records only up to the where the error was.
            if r.offset() >= cerr.offset {
                break;
            }
            let rec = r.record();
            core.log(c_enc, rec, true).map_err(|e| {
                core.metrics.writes_failed.fetch_add(1, Ordering::Relaxed);
                e.wrap("insert record")
            })?;
        }
        // We expect an error here from r.err(), so nothing to handle.

        // We need to pad to the end of the last page in the repaired segment.
        core.flush_page(true)
            .map_err(|e| e.wrap("flush page in repair"))?;

        drop(r);
        fs::remove_file(&tmpfn).map_err(|e| Error::from(e).wrap("delete corrupted segment"))?;

        // Explicitly close the segment we just repaired.
        if let Some(s) = core.segment.take() {
            let _ = s.close();
        }

        // We always want to start writing to a new Segment rather than an existing
        // Segment, which is handled by new_size, but earlier in repair we're deleting
        // all segments that come after the corrupted Segment. Recreate a new Segment here.
        let s = create_segment(&self.dir, cerr.segment + 1)?;
        core.set_segment(s)
    }

    /// Go `NextSegment`: creates the next segment and closes the previous one asynchronously.
    /// It returns the index of the new segment.
    pub fn next_segment(&self) -> Result<i64> {
        self.lock().core.next_segment(true)
    }

    /// Go `NextSegmentSync`: creates the next segment and closes the previous one synchronously.
    pub fn next_segment_sync(&self) -> Result<i64> {
        self.lock().core.next_segment(false)
    }

    /// Go `Log` with one record.
    pub fn log(&self, rec: &[u8]) -> Result<()> {
        let mut st = self.lock();
        let State { core, c_enc } = &mut *st;
        core.log(c_enc, rec, true).inspect_err(|_| {
            core.metrics.writes_failed.fetch_add(1, Ordering::Relaxed);
        })
    }

    /// Go `Log`: writes the records into the log. Multiple records can be passed at once to reduce writes and
    /// increase throughput.
    pub fn log_batch<T: AsRef<[u8]>>(&self, recs: &[T]) -> Result<()> {
        let mut st = self.lock();
        let State { core, c_enc } = &mut *st;
        // Callers could just implement their own list record format but adding
        // a bit of extra logic here frees them from that overhead.
        for (i, r) in recs.iter().enumerate() {
            if let Err(e) = core.log(c_enc, r.as_ref(), i == recs.len() - 1) {
                core.metrics.writes_failed.fetch_add(1, Ordering::Relaxed);
                return Err(e);
            }
        }
        Ok(())
    }

    /// Go `LastSegmentAndOffset`: the last segment number of the WAL and the offset in that file up to which
    /// the segment has been filled.
    pub fn last_segment_and_offset(&self) -> Result<(i64, i64)> {
        let st = self.lock();
        let (_, seg) = segments(&self.dir)?;
        let offset = (st.core.done_pages * PAGE_SIZE + st.core.page.alloc) as i64;
        Ok((seg, offset))
    }

    /// Go `Truncate`: drops all segments before `i`.
    pub fn truncate(&self, i: i64) -> Result<()> {
        self.metrics.truncate_total.fetch_add(1, Ordering::Relaxed);
        let r = (|| -> Result<()> {
            let refs = list_segments(&self.dir)?;
            for r in refs {
                if r.index >= i {
                    break;
                }
                fs::remove_file(self.dir.join(&r.name))?;
            }
            Ok(())
        })();
        if r.is_err() {
            self.metrics.truncate_fail.fetch_add(1, Ordering::Relaxed);
        }
        r
    }

    /// Go `Sync`: forces a file sync on the current segment.
    pub fn sync(&self) -> Result<()> {
        let mut st = self.lock();
        let metrics = st.core.metrics.clone();
        let seg = st.core.segment.as_mut().ok_or_else(segment_err)?;
        fsync(&metrics, seg)
    }

    /// Go `Close`: flushes all writes and closes the active segment. A second call returns an error.
    pub fn close(&self) -> Result<()> {
        let mut st = self.lock();
        let core = &mut st.core;
        if core.closed {
            return Err(Error::msg("wlog already closed"));
        }
        if core.segment.is_none() {
            core.closed = true;
            return Ok(());
        }

        // Flush the last page and zero out all its remaining size.
        // We must not flush an empty page as it would falsely signal
        // the segment is done if we start writing to it again after opening.
        if core.page.alloc > 0 {
            core.flush_page(true)?;
        }

        for h in core.pending.drain(..) {
            let _ = h.join();
        }

        if let Some(mut seg) = core.segment.take() {
            if let Err(e) = fsync(&core.metrics, &mut seg) {
                self.logger.error("sync previous segment", &[("err", &e)]);
            }
            if let Err(e) = seg.close() {
                self.logger.error("close previous segment", &[("err", &e)]);
            }
        }
        core.closed = true;
        Ok(())
    }

    /// Go `Size`: the size of all files under the log directory.
    pub fn size(&self) -> Result<i64> {
        Ok(fileutil::dir_size(&self.dir)?)
    }
}

fn fsync(metrics: &WlMetrics, seg: &mut Segment) -> Result<()> {
    let start = Instant::now();
    let r = seg.sync();
    metrics.observe_fsync(start);
    Ok(r?)
}

impl Core {
    fn set_segment(&mut self, segment: Segment) -> Result<()> {
        // Correctly initialize done_pages.
        let size = segment.size()?;
        self.done_pages = size as usize / PAGE_SIZE;
        self.metrics
            .current_segment
            .store(segment.index(), Ordering::Relaxed);
        self.segment = Some(segment);
        Ok(())
    }

    fn pages_per_segment(&self) -> usize {
        self.segment_size / PAGE_SIZE
    }

    /// Go `nextSegment`: creates the next segment and closes the previous one.
    fn next_segment(&mut self, async_close: bool) -> Result<i64> {
        if self.closed {
            return Err(Error::msg("wlog is closed"));
        }
        let cur = self.segment.as_ref().ok_or_else(segment_err)?.index();

        // Only flush the current page if it actually holds data.
        if self.page.alloc > 0 {
            self.flush_page(true)?;
        }
        let next =
            create_segment(&self.dir, cur + 1).map_err(|e| e.wrap("create new segment file"))?;
        let next_index = next.index();
        let prev = self.segment.take();
        self.set_segment(next)?;

        // Don't block further writes by fsyncing the last segment.
        if let Some(mut prev) = prev {
            let metrics = self.metrics.clone();
            let logger = self.logger.clone();
            let f = move || {
                if let Err(e) = fsync(&metrics, &mut prev) {
                    logger.error("sync previous segment", &[("err", &e)]);
                }
                if let Err(e) = prev.close() {
                    logger.error("close previous segment", &[("err", &e)]);
                }
            };
            if async_close {
                self.pending.retain(|h| !h.is_finished());
                self.pending.push(std::thread::spawn(f));
            } else {
                f();
            }
        }
        Ok(next_index)
    }

    /// Go `flushPage`: writes the new contents of the page to disk. If no more records will fit into the page,
    /// the remaining bytes are set to zero and a new page is started. With `force_clear` this happens
    /// regardless of how many bytes are left in the page.
    fn flush_page(&mut self, force_clear: bool) -> Result<()> {
        self.metrics.page_flushes.fetch_add(1, Ordering::Relaxed);

        let p = &mut self.page;
        let should_clear = force_clear || p.full();

        // No more data will fit into the page or an implicit clear.
        // Enqueue and clear it.
        if should_clear {
            p.alloc = PAGE_SIZE; // Write till end of page.
        }

        let seg = self.segment.as_mut().ok_or_else(segment_err)?;
        let (n, r) = seg.write_all_n(&p.buf[p.flushed..p.alloc]);
        p.flushed += n;
        r?;

        // We flushed an entire page, prepare a new one.
        if should_clear {
            p.reset();
            self.done_pages += 1;
            self.metrics
                .page_completions
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Go `log`: writes `rec` to the log and forces a flush of the current page if it is the final record of
    /// a batch, the record is bigger than the page size, or the current page is full.
    fn log(&mut self, c_enc: &mut EncodeBuffer, rec: &[u8], final_: bool) -> Result<()> {
        // When the last page flush failed the page will remain full.
        // When the page is full, need to flush it before trying to add more records to it.
        if self.page.full() {
            self.flush_page(true)?;
        }

        // Compress the record before calculating if a new segment is needed.
        let mut final_compression = self.compress;
        let mut enc = c_enc.encode(self.compress, rec)?;
        if self.compress != Type::None {
            let mut saved_bytes = rec.len() as i64 - enc.len() as i64;

            // Even if the compression was applied, skip it, if there's no benefit
            // in the WAL record size (we have a choice). For small records e.g. snappy
            // compression can yield larger records than the uncompressed.
            if saved_bytes <= 0 {
                enc = rec;
                final_compression = Type::None;
                saved_bytes = 0;
            }
            self.metrics
                .record_bytes_saved
                .fetch_add(saved_bytes as u64, Ordering::Relaxed);
        }

        // If the record is too big to fit within the active page in the current
        // segment, terminate the active segment and advance to the next one.
        // This ensures that records do not cross segment boundaries.
        let mut left = self.page.remaining() as i64 - RECORD_HEADER_SIZE as i64; // Free space in the active page.
        left += (PAGE_SIZE - RECORD_HEADER_SIZE) as i64
            * (self.pages_per_segment() as i64 - self.done_pages as i64 - 1); // Free pages in the active segment.

        if enc.len() as i64 > left {
            self.next_segment(true)?;
        }

        let compression_bits = match final_compression {
            Type::None => 0,
            Type::Snappy => SNAPPY_MASK,
            Type::Zstd => ZSTD_MASK,
        };

        // Populate as many pages as necessary to fit the record.
        // Be careful to always do one pass to ensure we write zero-length records.
        let mut i = 0;
        while i == 0 || !enc.is_empty() {
            let p = &mut self.page;

            // Find how much of the record we can fit into the page.
            let l = enc.len().min((PAGE_SIZE - p.alloc) - RECORD_HEADER_SIZE);
            let part = &enc[..l];
            let typ = if i == 0 && part.len() == enc.len() {
                RecType::FULL
            } else if part.len() == enc.len() {
                RecType::LAST
            } else if i == 0 {
                RecType::FIRST
            } else {
                RecType::MIDDLE
            };

            let buf = &mut p.buf[p.alloc..];
            buf[0] = typ.0 | compression_bits;
            buf[1..3].copy_from_slice(&(part.len() as u16).to_be_bytes());
            buf[3..7].copy_from_slice(&crc32c(part).to_be_bytes());
            buf[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + l].copy_from_slice(part);
            p.alloc += l + RECORD_HEADER_SIZE;

            self.metrics
                .record_part_writes
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .record_part_bytes
                .fetch_add((l + RECORD_HEADER_SIZE) as u64, Ordering::Relaxed);

            if self.page.full() {
                // TODO (upstream) When the flushing fails at this point and the record has not been
                // fully written to the buffer, we end up with a corrupted WAL because some part of the
                // record have been written to the buffer, while the rest of the record will be discarded.
                self.flush_page(true)?;
            }
            enc = &enc[l..];
            i += 1;
        }

        // If it's the final record of the batch and the page is not empty, flush it.
        if final_ && self.page.alloc > 0 {
            self.flush_page(false)?;
        }
        Ok(())
    }
}
