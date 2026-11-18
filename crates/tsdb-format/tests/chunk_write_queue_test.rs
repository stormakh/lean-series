//! Port of `tsdb/chunks/chunk_write_queue_test.go`.

use chunkenc::{Chunk, XorChunk};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tsdb_format::chunks::{
    ChunkDiskMapperRef, ChunkWriteJob, ChunkWriteQueue, Error, HeadSeriesRef, WriteChunkF,
};

/// Go `sync.WaitGroup`.
#[derive(Default)]
struct WaitGroup {
    n: Mutex<i64>,
    cv: Condvar,
}

impl WaitGroup {
    fn add(&self, d: i64) {
        let mut n = self.n.lock().unwrap();
        *n += d;
        if *n == 0 {
            self.cv.notify_all();
        }
    }
    fn done(&self) {
        self.add(-1);
    }
    fn wait(&self) {
        let mut n = self.n.lock().unwrap();
        while *n != 0 {
            n = self.cv.wait(n).unwrap();
        }
    }
}

/// Go `require.Eventually`.
fn eventually(cond: impl Fn() -> bool, wait_for: Duration, tick: Duration) -> bool {
    let deadline = Instant::now() + wait_for;
    loop {
        thread::sleep(tick);
        if cond() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
    }
}

#[test]
fn test_chunk_write_queue_getting_chunk_from_queue() {
    let block_writer_wg = Arc::new(WaitGroup::default());
    block_writer_wg.add(1);

    // blocking_chunk_writer blocks until block_writer_wg is done.
    let wg = block_writer_wg.clone();
    let blocking_chunk_writer: WriteChunkF = Arc::new(move |_, _, _, _, _, _, _| {
        wg.wait();
        Ok(())
    });

    let q = ChunkWriteQueue::new(1000, blocking_chunk_writer);

    let test_chunk: Arc<dyn Chunk> = Arc::new(XorChunk::new());
    let r = ChunkDiskMapperRef::default();
    let job = ChunkWriteJob {
        chk: Some(test_chunk.clone()),
        r#ref: r,
        ..Default::default()
    };
    assert!(q.add_job(job).is_ok());

    // Retrieve chunk from the queue.
    let got_chunk = q.get(r).expect("chunk in queue");
    // adapted: Go compares the chunk interface values (same pointer); here the Arc must be the same chunk.
    assert!(Arc::ptr_eq(&test_chunk, &got_chunk));
    assert_eq!(test_chunk.bytes(), got_chunk.bytes());

    // Go defers: blockWriterWg.Done() then q.stop().
    block_writer_wg.done();
    q.stop();
}

#[test]
fn test_chunk_write_queue_writing_through_queue() {
    type Got = (
        HeadSeriesRef,
        i64,
        i64,
        Option<Arc<dyn Chunk>>,
        ChunkDiskMapperRef,
        bool,
    );
    let got: Arc<Mutex<Got>> = Arc::new(Mutex::new((
        0,
        0,
        0,
        None,
        ChunkDiskMapperRef::default(),
        false,
    )));

    let g = got.clone();
    let blocking_chunk_writer: WriteChunkF =
        Arc::new(move |series_ref, mint, maxt, chunk, r, _, cut_file| {
            *g.lock().unwrap() = (series_ref, mint, maxt, chunk.cloned(), r, cut_file);
            Ok(())
        });

    let q = ChunkWriteQueue::new(1000, blocking_chunk_writer);

    let series_ref: HeadSeriesRef = 1;
    let (mint, maxt) = (2i64, 3i64);
    let chunk: Arc<dyn Chunk> = Arc::new(XorChunk::new());
    let r = ChunkDiskMapperRef::new(321, 123);
    let cut_file = true;
    let (tx, rx) = mpsc::channel::<()>();
    assert!(
        q.add_job(ChunkWriteJob {
            series_ref,
            mint,
            maxt,
            chk: Some(chunk.clone()),
            r#ref: r,
            cut_file,
            callback: Some(Box::new(move |_| {
                tx.send(()).unwrap();
            })),
            ..Default::default()
        })
        .is_ok()
    );
    rx.recv().unwrap();

    // Compare whether the write function has received all job attributes correctly.
    let g = got.lock().unwrap();
    assert_eq!(series_ref, g.0);
    assert_eq!(mint, g.1);
    assert_eq!(maxt, g.2);
    assert!(Arc::ptr_eq(&chunk, g.3.as_ref().unwrap()));
    assert_eq!(r, g.4);
    assert_eq!(cut_file, g.5);
    drop(g);
    q.stop();
}

#[test]
fn test_chunk_write_queue_wrapping_around_size_limit() {
    let size_limit = 100usize;
    // Go: a buffered channel of size_limit. Unblocking a writer is a token; closing the channel unblocks all.
    struct Tokens {
        n: Mutex<(usize, bool)>,
        cv: Condvar,
    }
    let unblock = Arc::new(Tokens {
        n: Mutex::new((0, false)),
        cv: Condvar::new(),
    });

    // blocking_chunk_writer blocks until the unblock channel returns a value.
    let u = unblock.clone();
    let blocking_chunk_writer: WriteChunkF = Arc::new(move |_, _, _, _, _, _, _| {
        let mut s = u.n.lock().unwrap();
        while s.0 == 0 && !s.1 {
            s = u.cv.wait(s).unwrap();
        }
        if s.0 > 0 {
            s.0 -= 1;
        }
        Ok(())
    });

    let q = Arc::new(ChunkWriteQueue::new(size_limit, blocking_chunk_writer));

    let chunk_ref = Arc::new(Mutex::new(ChunkDiskMapperRef::default()));
    let callback_wg = Arc::new(WaitGroup::default());
    let add_chunk = {
        let q = q.clone();
        let chunk_ref = chunk_ref.clone();
        let callback_wg = callback_wg.clone();
        move || {
            callback_wg.add(1);
            let mut r = chunk_ref.lock().unwrap();
            let wg = callback_wg.clone();
            assert!(
                q.add_job(ChunkWriteJob {
                    r#ref: *r,
                    callback: Some(Box::new(move |_| wg.done())),
                    ..Default::default()
                })
                .is_ok()
            );
            r.0 += 1;
        }
    };

    let unblock_chunk_writer = || {
        let mut s = unblock.n.lock().unwrap();
        s.0 += 1;
        unblock.cv.notify_all();
    };

    // Fill the queue to the middle of the size limit.
    for _ in 0..size_limit / 2 {
        add_chunk();
    }

    // Consume the jobs.
    for _ in 0..size_limit / 2 {
        unblock_chunk_writer();
    }

    // Add jobs until the queue is full. Note that one more job than <size_limit> can be added because one will
    // be processed by the worker already and it will block on the chunk write function.
    for _ in 0..size_limit + 1 {
        add_chunk();
    }

    // The queue should be full.
    assert!(q.queue_is_full());

    // Adding another job should block as long as no job from the queue gets consumed.
    let added_job = Arc::new(AtomicBool::new(false));
    let h = {
        let added_job = added_job.clone();
        let add_chunk = add_chunk.clone();
        thread::spawn(move || {
            add_chunk();
            added_job.store(true, Ordering::SeqCst);
        })
    };

    // Wait for 10ms while the adding of a new job is blocked.
    thread::sleep(Duration::from_millis(10));
    assert!(!added_job.load(Ordering::SeqCst));

    // Consume one job from the queue.
    unblock_chunk_writer();

    // Wait until the job has been added to the queue.
    assert!(eventually(
        || added_job.load(Ordering::SeqCst),
        Duration::from_secs(1),
        Duration::from_millis(10)
    ));
    h.join().unwrap();

    // The queue should be full again.
    assert!(q.queue_is_full());

    // Consume <size_limit>+1 jobs from the queue. To drain the queue we need to consume <size_limit>+1 jobs
    // because 1 job is already in the state of being processed.
    for _ in 0..size_limit + 1 {
        assert!(!q.queue_is_empty());
        unblock_chunk_writer();
    }

    // Wait until all jobs have been processed.
    callback_wg.wait();

    assert!(eventually(
        || q.queue_is_empty(),
        Duration::from_millis(500),
        Duration::from_millis(50)
    ));

    // Go defers: close(unblockChunkWriterCh) then q.stop().
    {
        let mut s = unblock.n.lock().unwrap();
        s.1 = true;
        unblock.cv.notify_all();
    }
    q.stop();
}

#[test]
fn test_chunk_write_queue_handler_error_via_callback() {
    let chunk_writer: WriteChunkF = Arc::new(|_, _, _, _, _, _, _| Err(Error::msg("test error")));

    let (tx, rx) = mpsc::channel::<Result<(), Error>>();
    let q = ChunkWriteQueue::new(1, chunk_writer);

    let job = ChunkWriteJob {
        callback: Some(Box::new(move |err| {
            tx.send(err).unwrap();
        })),
        ..Default::default()
    };
    assert!(q.add_job(job).is_ok());

    let got_error = rx.recv().unwrap();

    // adapted: Go compares the error values; here the error is the same variant with the same text.
    match got_error {
        Err(Error::Msg(s)) => assert_eq!("test error", s),
        other => panic!("unexpected result {other:?}"),
    }
    q.stop();
}
