//! Port of `tsdb/chunks/queue.go`: a bounded blocking queue of chunk write jobs, stored in segments of equal
//! size so that an empty queue uses little memory.

use super::ChunkWriteJob;
use std::collections::VecDeque;
use std::sync::{Condvar, Mutex, MutexGuard};

/// Go `writeJobQueue`: like a buffered channel of `ChunkWriteJob`, but it manages its own buffers to avoid
/// using a lot of memory when it is empty. Segments that are fully read are dropped.
#[derive(Debug)]
pub struct WriteJobQueue {
    max_size: usize,
    segment_size: usize,
    /// Protects all the state.
    mtx: Mutex<QueueState>,
    /// Signalled when something is pushed into the queue.
    pushed: Condvar,
    /// Signalled when something is popped from the queue.
    popped: Condvar,
}

#[derive(Debug, Default)]
struct QueueState {
    /// The segments, first to last.
    segments: VecDeque<Segment>,
    /// Total size of the queue.
    size: usize,
    /// After closing the queue, nothing can be pushed to it.
    closed: bool,
}

#[derive(Debug)]
struct Segment {
    /// The jobs; its capacity is the segment size, slots before `next_read` are taken (default jobs).
    segment: Vec<ChunkWriteJob>,
    /// Index of the next read in this segment (the next write is `segment.len()`).
    next_read: usize,
}

/// One segment as seen by tests: (segment size, next read, next write).
#[doc(hidden)]
pub type SegmentInfo = (usize, usize, usize);

impl WriteJobQueue {
    /// Go `newWriteJobQueue`. Panics with "invalid queue" when a size is 0.
    pub fn new(max_size: usize, segment_size: usize) -> WriteJobQueue {
        if max_size == 0 || segment_size == 0 {
            panic!("invalid queue");
        }
        WriteJobQueue {
            max_size,
            segment_size,
            mtx: Mutex::new(QueueState::default()),
            pushed: Condvar::new(),
            popped: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, QueueState> {
        self.mtx.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The maximum number of queued jobs.
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Go `close`: nothing can be pushed afterwards; blocked callers are woken.
    pub fn close(&self) {
        let mut q = self.lock();
        q.closed = true;
        // Unblock all blocked threads.
        self.pushed.notify_all();
        self.popped.notify_all();
        drop(q);
    }

    /// Go `push`: blocks until there is space in the queue, then adds the job. If the queue is closed or gets
    /// closed while waiting, the job is handed back (Go returns false).
    pub fn push(&self, job: ChunkWriteJob) -> Result<(), ChunkWriteJob> {
        let mut q = self.lock();
        // Wait until queue has more space or is closed.
        while !q.closed && q.size >= self.max_size {
            q = self.popped.wait(q).unwrap_or_else(|p| p.into_inner());
        }
        if q.closed {
            return Err(job);
        }
        // Check if the last segment has more space for writing, and create a new one if not.
        let need_new = match q.segments.back() {
            None => true,
            Some(s) => s.segment.len() >= self.segment_size,
        };
        if need_new {
            q.segments.push_back(Segment {
                segment: Vec::with_capacity(self.segment_size),
                next_read: 0,
            });
        }
        q.segments.back_mut().unwrap().segment.push(job);
        q.size += 1;
        self.pushed.notify_one();
        Ok(())
    }

    /// Go `pop`: the first job. If the queue is empty, blocks until there is a job or the queue is closed
    /// (`None`). After close, the remaining jobs are still returned first.
    pub fn pop(&self) -> Option<ChunkWriteJob> {
        let mut q = self.lock();
        // Wait until something is pushed to the queue, or queue is closed.
        while q.size == 0 {
            if q.closed {
                return None;
            }
            q = self.pushed.wait(q).unwrap_or_else(|p| p.into_inner());
        }
        let first = q.segments.front_mut().unwrap();
        // Take the element, leaving a cleared one behind.
        let res = std::mem::take(&mut first.segment[first.next_read]);
        first.next_read += 1;
        // If we have read all possible elements from the first segment, we can drop it.
        let drop_first = first.next_read >= self.segment_size;
        q.size -= 1;
        if drop_first {
            q.segments.pop_front();
        }
        self.popped.notify_one();
        Some(res)
    }

    /// Go `length`: the number of jobs in the queue.
    pub fn length(&self) -> usize {
        self.lock().size
    }

    /// The segments (first to last) and the size, for the invariant checks of the tests.
    #[doc(hidden)]
    pub fn debug_segments(&self) -> (Vec<SegmentInfo>, usize) {
        let q = self.lock();
        (
            q.segments
                .iter()
                .map(|s| (self.segment_size, s.next_read, s.segment.len()))
                .collect(),
            q.size,
        )
    }
}
