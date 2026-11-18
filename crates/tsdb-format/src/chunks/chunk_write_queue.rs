//! Port of `tsdb/chunks/chunk_write_queue.go`: a queue for writing chunks to disk without blocking the caller.
//! Go's worker goroutine is a plain thread.

use super::{ChunkDiskMapperRef, Error, HeadSeriesRef, WriteJobQueue};
use chunkenc::Chunk;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Minimum recorded peak since the last shrinking of the chunk ref map to shrink it again.
const CHUNK_REF_MAP_SHRINK_THRESHOLD: usize = 1000;

/// Minimum interval between shrinking of the chunk ref map.
const CHUNK_REF_MAP_MIN_SHRINK_INTERVAL: Duration = Duration::from_secs(10 * 60);

/// Maximum size of segment used by the job queue (number of elements).
const MAX_CHUNK_QUEUE_SEGMENT_SIZE: usize = 8192;

/// Called with the result of a chunk write.
pub type WriteCallback = Box<dyn FnOnce(Result<(), Error>) + Send>;

/// Go `writeChunkF`: writes a chunk (series ref, mint, maxt, chunk, ref, is OOO, cut file). It is dynamic to
/// allow mocking in tests.
pub type WriteChunkF = Arc<
    dyn Fn(
            HeadSeriesRef,
            i64,
            i64,
            Option<&Arc<dyn Chunk>>,
            ChunkDiskMapperRef,
            bool,
            bool,
        ) -> Result<(), Error>
        + Send
        + Sync,
>;

/// Go `chunkWriteJob`.
#[derive(Default)]
pub struct ChunkWriteJob {
    pub cut_file: bool,
    pub series_ref: HeadSeriesRef,
    pub mint: i64,
    pub maxt: i64,
    pub chk: Option<Arc<dyn Chunk>>,
    pub r#ref: ChunkDiskMapperRef,
    pub is_ooo: bool,
    pub callback: Option<WriteCallback>,
}

impl std::fmt::Debug for ChunkWriteJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkWriteJob")
            .field("cut_file", &self.cut_file)
            .field("series_ref", &self.series_ref)
            .field("mint", &self.mint)
            .field("maxt", &self.maxt)
            .field("ref", &self.r#ref)
            .field("is_ooo", &self.is_ooo)
            .finish()
    }
}

struct RefMap {
    map: HashMap<ChunkDiskMapperRef, Option<Arc<dyn Chunk>>>,
    /// Largest size the map has grown to since the last time we shrank it.
    peak_size: usize,
    /// When the map has been shrunk the last time.
    last_shrink: Instant,
}

struct Shared {
    jobs: WriteJobQueue,
    chunk_ref_map: RwLock<RefMap>,
    write_chunk: Mutex<WriteChunkF>,
    // Go counter `prometheus_tsdb_chunk_write_queue_operations_total{operation=...}`.
    adds: AtomicU64,
    gets: AtomicU64,
    completed: AtomicU64,
    shrink: AtomicU64,
}

/// Go `chunkWriteQueue`: chunks that shall be written are added to the queue, which a worker thread consumes.
/// Adding is non-blocking as long as the queue is not full.
pub struct ChunkWriteQueue {
    shared: Arc<Shared>,
    /// Go `isRunningMtx` + `isRunning`. It also serializes adding jobs so that the chunk ref map never grows
    /// beyond the queue size + 1.
    is_running: Mutex<bool>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

/// The operation counts of a chunk write queue (Go metric `prometheus_tsdb_chunk_write_queue_operations_total`
/// with label `operation`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ChunkWriteQueueOps {
    pub add: u64,
    pub get: u64,
    pub complete: u64,
    pub shrink: u64,
}

impl ChunkWriteQueue {
    /// Go `newChunkWriteQueue`: starts the worker thread.
    pub fn new(size: usize, write_chunk: WriteChunkF) -> ChunkWriteQueue {
        let segment_size = size.min(MAX_CHUNK_QUEUE_SEGMENT_SIZE);
        let shared = Arc::new(Shared {
            jobs: WriteJobQueue::new(size, segment_size),
            chunk_ref_map: RwLock::new(RefMap {
                map: HashMap::new(),
                peak_size: 0,
                last_shrink: Instant::now(),
            }),
            write_chunk: Mutex::new(write_chunk),
            adds: AtomicU64::new(0),
            gets: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            shrink: AtomicU64::new(0),
        });
        let q = ChunkWriteQueue {
            shared,
            is_running: Mutex::new(false),
            worker: Mutex::new(None),
        };
        q.start();
        q
    }

    fn start(&self) {
        let shared = self.shared.clone();
        let h = std::thread::Builder::new()
            .name("chunk-write-queue".into())
            .spawn(move || {
                while let Some(job) = shared.jobs.pop() {
                    shared.process_job(job);
                }
            })
            .expect("spawn chunk write queue worker");
        *self.worker.lock().unwrap_or_else(|p| p.into_inner()) = Some(h);
        *self.is_running.lock().unwrap_or_else(|p| p.into_inner()) = true;
    }

    /// Go `addJob`.
    pub fn add_job(&self, job: ChunkWriteJob) -> Result<(), Error> {
        self.try_add_job(job).map_err(|(e, _)| e)
    }

    /// Like `add_job`, but hands the job back on failure (so that its callback can be called).
    pub(crate) fn try_add_job(&self, job: ChunkWriteJob) -> Result<(), (Error, ChunkWriteJob)> {
        let running = self.is_running.lock().unwrap_or_else(|p| p.into_inner());
        if !*running {
            return Err((Error::msg("queue is not running"), job));
        }
        let r = job.r#ref;
        {
            let mut m = self
                .shared
                .chunk_ref_map
                .write()
                .unwrap_or_else(|p| p.into_inner());
            m.map.insert(r, job.chk.clone());
            // Keep track of the peak usage of the map.
            if m.map.len() > m.peak_size {
                m.peak_size = m.map.len();
            }
        }
        if let Err(job) = self.shared.jobs.push(job) {
            self.shared
                .chunk_ref_map
                .write()
                .unwrap_or_else(|p| p.into_inner())
                .map
                .remove(&r);
            return Err((Error::msg("queue is closed"), job));
        }
        drop(running);
        self.shared.adds.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Go `get`: the chunk of a job that is queued or being written.
    pub fn get(&self, r: ChunkDiskMapperRef) -> Option<Arc<dyn Chunk>> {
        let m = self
            .shared
            .chunk_ref_map
            .read()
            .unwrap_or_else(|p| p.into_inner());
        match m.map.get(&r) {
            Some(c) => {
                self.shared.gets.fetch_add(1, Ordering::Relaxed);
                c.clone()
            }
            None => None,
        }
    }

    /// Go `stop`: closes the queue and waits until the worker has written all queued jobs.
    pub fn stop(&self) {
        let mut running = self.is_running.lock().unwrap_or_else(|p| p.into_inner());
        if !*running {
            return;
        }
        *running = false;
        self.shared.jobs.close();
        if let Some(h) = self.worker.lock().unwrap_or_else(|p| p.into_inner()).take() {
            let _ = h.join();
        }
    }

    /// Go `queueIsEmpty`.
    pub fn queue_is_empty(&self) -> bool {
        self.queue_size() == 0
    }

    /// Go `queueIsFull`: when the queue is full and blocked on the writer, the chunk ref map has one more job
    /// than the queue size because one job is being processed.
    pub fn queue_is_full(&self) -> bool {
        self.queue_size() == self.shared.jobs.max_size() + 1
    }

    /// Go `queueSize`: the jobs that are queued or being processed.
    pub fn queue_size(&self) -> usize {
        self.shared
            .chunk_ref_map
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .map
            .len()
    }

    /// The operation counters.
    pub fn ops(&self) -> ChunkWriteQueueOps {
        let s = &self.shared;
        ChunkWriteQueueOps {
            add: s.adds.load(Ordering::Relaxed),
            get: s.gets.load(Ordering::Relaxed),
            complete: s.completed.load(Ordering::Relaxed),
            shrink: s.shrink.load(Ordering::Relaxed),
        }
    }

    /// The function the worker writes chunks with (Go field `writeChunk`).
    pub fn write_chunk_fn(&self) -> WriteChunkF {
        self.shared
            .write_chunk
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Replaces the function the worker writes chunks with (tests use it to add delays).
    pub fn set_write_chunk_fn(&self, f: WriteChunkF) {
        *self
            .shared
            .write_chunk
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = f;
    }
}

impl Drop for ChunkWriteQueue {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Shared {
    /// Go `processJob`.
    fn process_job(&self, job: ChunkWriteJob) {
        let f = self
            .write_chunk
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let res = f(
            job.series_ref,
            job.mint,
            job.maxt,
            job.chk.as_ref(),
            job.r#ref,
            job.is_ooo,
            job.cut_file,
        );
        if let Some(cb) = job.callback {
            cb(res);
        }
        let mut m = self
            .chunk_ref_map
            .write()
            .unwrap_or_else(|p| p.into_inner());
        m.map.remove(&job.r#ref);
        self.completed.fetch_add(1, Ordering::Relaxed);
        Self::shrink_chunk_ref_map(&mut m, &self.shrink);
    }

    /// Go `shrinkChunkRefMap`: releases the memory of the map once it is empty, has grown past the threshold
    /// and the minimum interval has passed.
    fn shrink_chunk_ref_map(m: &mut RefMap, shrink: &AtomicU64) {
        if !m.map.is_empty() {
            // Can't shrink it while there is data in it.
            return;
        }
        if m.peak_size < CHUNK_REF_MAP_SHRINK_THRESHOLD {
            // Not shrinking it because it has not grown to the minimum threshold yet.
            return;
        }
        let now = Instant::now();
        if now.duration_since(m.last_shrink) < CHUNK_REF_MAP_MIN_SHRINK_INTERVAL {
            // Not shrinking it because the minimum duration between shrink-events has not passed yet.
            return;
        }
        // Re-initialize the map to half of the peak size it has grown to since the last re-init event.
        m.map = HashMap::with_capacity(m.peak_size / 2);
        m.peak_size = 0;
        m.last_shrink = now;
        shrink.fetch_add(1, Ordering::Relaxed);
    }
}
