//! Port of `tsdb/chunks/queue_test.go`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tsdb_format::chunks::{ChunkWriteJob, HeadSeriesRef, WriteJobQueue};

/// A small deterministic random source (splitmix64); `int()` is a non-negative int63 as Go's `rand.Int()`.
struct Rand(u64);

impl Rand {
    fn int(&mut self) -> usize {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        ((z ^ (z >> 31)) >> 1) as usize
    }
}

fn job(series_ref: HeadSeriesRef) -> ChunkWriteJob {
    ChunkWriteJob {
        series_ref,
        ..Default::default()
    }
}

/// Go `(*writeJobQueue).assertInvariants`.
fn assert_invariants(q: &WriteJobQueue) {
    let (segments, size) = q.debug_segments();
    let mut total_size = 0;
    let n = segments.len();
    for (i, &(segment_len, next_read, next_write)) in segments.iter().enumerate() {
        let is_first = i == 0;
        let is_last = i == n - 1;
        // The segment exists (Go: require.NotNil(s.segment)).
        assert!(segment_len > 0);

        // Next read index is lower or equal than next write index (we cannot past written jobs).
        assert!(next_read <= next_write);

        // Number of unread elements in this segment.
        total_size += next_write - next_read;

        // First segment can be partially read, other segments were not read yet.
        if !is_first {
            assert_eq!(0, next_read);
        }

        // If first shard is empty (everything was read from it already), it must have extra capacity for
        // additional elements, otherwise it would have been removed.
        if is_first && next_read == next_write {
            assert!(next_write < segment_len);
        }

        // Segments in the middle are full.
        if !is_first && !is_last {
            assert_eq!(segment_len, next_write);
        }
        // Last segment must have at least one element, or we wouldn't have created it.
        assert!(next_write > 0);
    }
    assert_eq!(size, total_size);
}

#[test]
fn test_queue_push_pop_single_goroutine() {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    println!("seed: {seed}");
    let mut r = Rand(seed);

    const MAX_SIZE: usize = 500;
    const MAX_ITERS: usize = 50;

    for max_count in 1..MAX_SIZE {
        let queue = WriteJobQueue::new(max_count, 1 + (r.int() % max_count));

        let mut elements = 0; // Total elements in the queue.
        let mut last_write_id = 0u64;
        let mut last_read_id = 0u64;

        for _ in 0..MAX_ITERS {
            if elements < max_count {
                let mut to_write = r.int() % (max_count - elements);
                if to_write == 0 {
                    to_write = 1;
                }
                for _ in 0..to_write {
                    last_write_id += 1;
                    assert!(queue.push(job(last_write_id)).is_ok());
                    elements += 1;
                }
            }

            if elements > 0 {
                let mut to_read = r.int() % elements;
                if to_read == 0 {
                    to_read = 1;
                }
                for _ in 0..to_read {
                    last_read_id += 1;
                    let j = queue.pop();
                    assert!(j.is_some());
                    assert_eq!(last_read_id, j.unwrap().series_ref);
                    elements -= 1;
                }
            }

            assert_eq!(elements, queue.length());
            assert_invariants(&queue);
        }
    }
}

#[test]
fn test_queue_push_blocks_on_full_queue() {
    let queue = Arc::new(WriteJobQueue::new(5, 5));

    let (push_time_tx, push_time_rx) = mpsc::channel::<Instant>();
    let q = queue.clone();
    let h = thread::spawn(move || {
        assert!(q.push(job(1)).is_ok());
        assert!(q.push(job(2)).is_ok());
        assert!(q.push(job(3)).is_ok());
        assert!(q.push(job(4)).is_ok());
        assert!(q.push(job(5)).is_ok());
        push_time_tx.send(Instant::now()).unwrap();
        // This will block.
        assert!(q.push(job(6)).is_ok());
        push_time_tx.send(Instant::now()).unwrap();
    });

    let time_before_push = push_time_rx.recv().unwrap();

    let delay = Duration::from_millis(100);
    match push_time_rx.recv_timeout(delay) {
        Err(mpsc::RecvTimeoutError::Timeout) => {} // ok
        _ => panic!("didn't expect another push to proceed"),
    }

    let pop_time = Instant::now();
    let j = queue.pop();
    assert!(j.is_some());
    assert_eq!(1, j.unwrap().series_ref);

    let time_after_push = push_time_rx.recv().unwrap();

    assert!(time_after_push >= pop_time);
    assert!(time_after_push.duration_since(time_before_push) >= delay);
    h.join().unwrap();
}

#[test]
fn test_queue_pop_blocks_on_empty_queue() {
    let queue = Arc::new(WriteJobQueue::new(5, 5));

    let (pop_time_tx, pop_time_rx) = mpsc::channel::<Instant>();
    let q = queue.clone();
    let h = thread::spawn(move || {
        let j = q.pop();
        assert!(j.is_some());
        assert_eq!(1, j.unwrap().series_ref);

        pop_time_tx.send(Instant::now()).unwrap();

        // This will block.
        let j = q.pop();
        assert!(j.is_some());
        assert_eq!(2, j.unwrap().series_ref);

        pop_time_tx.send(Instant::now()).unwrap();
    });

    let _ = queue.push(job(1));

    let time_before_pop = pop_time_rx.recv().unwrap();

    let delay = Duration::from_millis(100);
    match pop_time_rx.recv_timeout(delay) {
        Err(mpsc::RecvTimeoutError::Timeout) => {} // ok
        _ => panic!("didn't expect another pop to proceed"),
    }

    let push_time = Instant::now();
    assert!(queue.push(job(2)).is_ok());

    let time_after_pop = pop_time_rx.recv().unwrap();

    assert!(time_after_pop >= push_time);
    assert!(time_after_pop.duration_since(time_before_pop) > delay);
    h.join().unwrap();
}

#[test]
fn test_queue_pop_unblocks_on_close() {
    let queue = Arc::new(WriteJobQueue::new(5, 5));

    let (pop_time_tx, pop_time_rx) = mpsc::channel::<Instant>();
    let q = queue.clone();
    let h = thread::spawn(move || {
        let j = q.pop();
        assert!(j.is_some());
        assert_eq!(1, j.unwrap().series_ref);

        pop_time_tx.send(Instant::now()).unwrap();

        // This will block until queue is closed.
        let j = q.pop();
        assert!(j.is_none());

        pop_time_tx.send(Instant::now()).unwrap();
    });

    let _ = queue.push(job(1));

    let time_before_pop = pop_time_rx.recv().unwrap();

    let delay = Duration::from_millis(100);
    match pop_time_rx.recv_timeout(delay) {
        Err(mpsc::RecvTimeoutError::Timeout) => {} // ok
        _ => panic!("didn't expect another pop to proceed"),
    }

    let close_time = Instant::now();
    queue.close();

    let time_after_pop = pop_time_rx.recv().unwrap();

    assert!(time_after_pop >= close_time);
    assert!(time_after_pop.duration_since(time_before_pop) >= delay);
    h.join().unwrap();
}

#[test]
fn test_queue_pop_after_close_returns_all_elements() {
    const COUNT: u64 = 10;

    let queue = WriteJobQueue::new(COUNT as usize, COUNT as usize);

    for i in 0..COUNT {
        assert!(queue.push(job(i)).is_ok());
    }

    // Close the queue before popping all elements.
    queue.close();

    // No more pushing allowed after close.
    assert!(queue.push(job(11111)).is_err());

    // Verify that we can still read all pushed elements.
    for i in 0..COUNT {
        let j = queue.pop();
        assert!(j.is_some());
        assert_eq!(i, j.unwrap().series_ref);
    }

    assert!(queue.pop().is_none());
}

#[test]
fn test_queue_push_pop_many_goroutines() {
    const READ_GOROUTINES: usize = 5;
    const WRITE_GOROUTINES: usize = 10;
    const WRITES: usize = 500;

    let queue = Arc::new(WriteJobQueue::new(1024, 64));

    // Reading threads.
    let refs: Arc<Mutex<HashMap<HeadSeriesRef, bool>>> = Arc::new(Mutex::new(HashMap::new()));

    let mut readers = Vec::new();
    for _ in 0..READ_GOROUTINES {
        let q = queue.clone();
        let refs = refs.clone();
        readers.push(thread::spawn(move || {
            while let Some(j) = q.pop() {
                refs.lock().unwrap().insert(j.series_ref, true);
            }
        }));
    }

    let id = Arc::new(AtomicU64::new(0));

    let mut writers = Vec::new();
    for _ in 0..WRITE_GOROUTINES {
        let q = queue.clone();
        let id = id.clone();
        writers.push(thread::spawn(move || {
            for _ in 0..WRITES {
                let r = id.fetch_add(1, Ordering::SeqCst) + 1;
                assert!(q.push(job(r)).is_ok());
            }
        }));
    }

    // Wait until all writes are done.
    for w in writers {
        w.join().unwrap();
    }

    // Close the queue and wait for reading to be done.
    queue.close();
    for r in readers {
        r.join().unwrap();
    }

    // Check if we have all expected values.
    assert_eq!(WRITE_GOROUTINES * WRITES, refs.lock().unwrap().len());
}

#[test]
fn test_queue_segment_is_kept_even_if_empty() {
    let queue = WriteJobQueue::new(1024, 64);

    assert!(queue.push(job(1)).is_ok());
    assert!(queue.pop().is_some());

    let (segments, _) = queue.debug_segments();
    // Go: queue.first is not nil, first.nextRead == 1, first.nextWrite == 1.
    assert!(!segments.is_empty());
    assert_eq!(1, segments[0].1);
    assert_eq!(1, segments[0].2);
}
