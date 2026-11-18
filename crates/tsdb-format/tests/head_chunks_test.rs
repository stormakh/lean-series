//! Port of `tsdb/chunks/head_chunks_test.go`.
//!
//! Go's `TestMain` runs every test twice: with the chunk write queue disabled (size 0) and with a queue of
//! size 1e6. Here every test loops over both sizes.

use chunkenc::{Chunk, Encoding, XorChunk};
use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;
use tsdb_format::chunks::{
    CHUNK_ENCODING_SIZE, CHUNKS_FORMAT_V1, CRC_SIZE, ChunkDiskMapper, ChunkDiskMapperRef,
    DEFAULT_WRITE_BUFFER_SIZE, Error, HEAD_CHUNK_FILE_HEADER_SIZE, HeadSeriesRef,
    MAGIC_CHUNKS_SIZE, MAGIC_HEAD_CHUNKS, MAX_HEAD_CHUNK_META_SIZE, MINT_MAXT_SIZE,
    SERIES_REF_SIZE, WriteChunkF, segment_file,
};
use tsdb_format::encoding::Crc32;

const WRITE_QUEUE_SIZES: [usize; 2] = [0, 1_000_000];

/// A unique empty directory for one test (Go `t.TempDir()`).
fn temp_dir(name: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let d = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "head_chunks_test-{name}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// A small random source (splitmix64), seeded from the clock as Go's global `math/rand`.
struct Rand(Cell<u64>);

impl Rand {
    fn new() -> Rand {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        Rand(Cell::new(seed))
    }
    fn u64(&self) -> u64 {
        let s = self.0.get().wrapping_add(0x9E3779B97F4A7C15);
        self.0.set(s);
        let mut z = s;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    /// Go `rand.Int63`.
    fn int63(&self) -> i64 {
        (self.u64() >> 1) as i64
    }
    /// Go `rand.Int`.
    fn int(&self) -> usize {
        self.int63() as usize
    }
    /// Go `rand.Intn`.
    fn intn(&self, n: usize) -> usize {
        self.int() % n
    }
    /// Go `rand.Float64`.
    fn float64(&self) -> f64 {
        (self.u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn create_chunk_disk_mapper(
    dir: Option<&Path>,
    write_queue_size: usize,
    name: &str,
) -> ChunkDiskMapper {
    let dir = match dir {
        Some(d) => d.to_path_buf(),
        None => temp_dir(name),
    };
    let hrw = ChunkDiskMapper::new(
        &dir,
        Some(Arc::new(chunkenc::new_pool())),
        DEFAULT_WRITE_BUFFER_SIZE,
        write_queue_size,
    )
    .unwrap();
    assert!(!hrw.test_file_maxt_set());
    hrw.iterate_all_chunks(|_, _, _, _, _, _, _| Ok(()))
        .unwrap();
    assert!(hrw.test_file_maxt_set());
    hrw
}

fn random_chunk(r: &Rand) -> Arc<dyn Chunk> {
    let mut chunk = XorChunk::new();
    let length = r.int() % 120;
    let mut app = chunk.appender().unwrap();
    for _ in 0..length {
        app.append(&mut chunk, 0, r.int63(), r.float64());
    }
    Arc::new(chunk)
}

/// Writes a chunk and waits for the write callback; returns the callback result.
fn write_and_wait(
    hrw: &ChunkDiskMapper,
    series_ref: HeadSeriesRef,
    mint: i64,
    maxt: i64,
    chunk: Arc<dyn Chunk>,
    is_ooo: bool,
) -> (ChunkDiskMapperRef, Result<(), Error>) {
    let (tx, rx) = mpsc::channel();
    let r = hrw.write_chunk(
        series_ref,
        mint,
        maxt,
        chunk,
        is_ooo,
        Some(Box::new(move |err| {
            tx.send(err).unwrap();
        })),
    );
    (r, rx.recv().unwrap())
}

struct Created {
    series_ref: HeadSeriesRef,
    chunk_ref: ChunkDiskMapperRef,
    mint: i64,
    maxt: i64,
    chunk: Arc<dyn Chunk>,
    is_ooo: bool,
}

fn create_chunk(idx: usize, hrw: &ChunkDiskMapper, r: &Rand) -> Created {
    let series_ref = r.int63() as HeadSeriesRef;
    let mint = (idx * 1000 + 1) as i64;
    let maxt = ((idx + 1) * 1000) as i64;
    let chunk = random_chunk(r);
    let is_ooo = r.intn(2) == 0;
    let (chunk_ref, err) = write_and_wait(hrw, series_ref, mint, maxt, chunk.clone(), is_ooo);
    // adapted: Go checks a variable that is always nil here; the real callback error is checked instead.
    err.unwrap();
    Created {
        series_ref,
        chunk_ref,
        mint,
        maxt,
        chunk,
        is_ooo,
    }
}

fn put_uvarint(buf: &mut [u8], mut x: u64) -> usize {
    let mut i = 0;
    while x >= 0x80 {
        buf[i] = (x as u8) | 0x80;
        x >>= 7;
        i += 1;
    }
    buf[i] = x as u8;
    i + 1
}

/// Go `verifyFiles` of the truncate tests.
fn verify_files(hrw: &ChunkDiskMapper, remaining_files: &[usize]) {
    let files: Vec<_> = std::fs::read_dir(hrw.dir()).unwrap().collect();
    assert_eq!(remaining_files.len(), files.len(), "files on disk");
    let mmapped = hrw.test_mmapped_chunk_files();
    assert_eq!(
        remaining_files.len(),
        mmapped.len(),
        "hrw.mmappedChunkFiles"
    );
    // adapted: Go also checks `len(hrw.closers)`; here the mapping is the closer, stored in the same map, so the
    // count above covers it.
    for i in remaining_files {
        assert!(
            mmapped.contains(i),
            "remaining file {i} not in hrw.mmappedChunkFiles"
        );
    }
}

#[test]
fn test_chunk_disk_mapper_write_chunk_chunk_iterate_chunks() {
    for wqs in WRITE_QUEUE_SIZES {
        let r = Rand::new();
        let mut hrw = create_chunk_disk_mapper(None, wqs, "write_iterate");

        let mut expected_bytes: Vec<u8> = Vec::new();
        let mut next_chunk_offset = HEAD_CHUNK_FILE_HEADER_SIZE as u64;
        let mut chk_crc32 = Crc32::new();

        struct Expected {
            series_ref: HeadSeriesRef,
            chunk_ref: ChunkDiskMapperRef,
            maxt: i64,
            num_samples: u16,
            chunk: Arc<dyn Chunk>,
            is_ooo: bool,
        }
        let mut expected_data: Vec<Expected> = Vec::new();

        let mut buf = [0u8; MAX_HEAD_CHUNK_META_SIZE];
        let mut total_chunks = 0;
        let mut first_file_name: Option<PathBuf> = None;
        while hrw.test_cur_file_sequence() < 3 || hrw.test_chk_writer_buffered() == 0 {
            let mut add_chunks = |num_chunks: usize| {
                for _ in 0..num_chunks {
                    let c = create_chunk(total_chunks, &hrw, &r);
                    total_chunks += 1;
                    expected_data.push(Expected {
                        series_ref: c.series_ref,
                        chunk_ref: c.chunk_ref,
                        maxt: c.maxt,
                        num_samples: c.chunk.num_samples() as u16,
                        chunk: c.chunk.clone(),
                        is_ooo: c.is_ooo,
                    });

                    if hrw.test_cur_file_sequence() != 1 {
                        // We are checking for bytes written only for the first file.
                        continue;
                    }

                    // Calculating expected bytes written on disk for first file.
                    first_file_name = hrw.test_cur_file_name();
                    assert_eq!(ChunkDiskMapperRef::new(1, next_chunk_offset), c.chunk_ref);

                    let mut bytes_written = 0;
                    chk_crc32.reset();

                    buf[bytes_written..bytes_written + 8]
                        .copy_from_slice(&c.series_ref.to_be_bytes());
                    bytes_written += SERIES_REF_SIZE;
                    buf[bytes_written..bytes_written + 8]
                        .copy_from_slice(&(c.mint as u64).to_be_bytes());
                    bytes_written += MINT_MAXT_SIZE;
                    buf[bytes_written..bytes_written + 8]
                        .copy_from_slice(&(c.maxt as u64).to_be_bytes());
                    bytes_written += MINT_MAXT_SIZE;
                    let mut enc = c.chunk.encoding();
                    if c.is_ooo {
                        enc = ChunkDiskMapper::apply_out_of_order_mask(enc);
                    }
                    buf[bytes_written] = enc.0;
                    bytes_written += CHUNK_ENCODING_SIZE;
                    let n = put_uvarint(&mut buf[bytes_written..], c.chunk.bytes().len() as u64);
                    bytes_written += n;

                    expected_bytes.extend_from_slice(&buf[..bytes_written]);
                    chk_crc32.write(&buf[..bytes_written]);
                    expected_bytes.extend_from_slice(c.chunk.bytes());
                    chk_crc32.write(c.chunk.bytes());

                    chk_crc32.sum(&mut expected_bytes);

                    // += seriesRef, mint, maxt, encoding, chunk data len, chunk data, CRC.
                    next_chunk_offset +=
                        (SERIES_REF_SIZE + 2 * MINT_MAXT_SIZE + CHUNK_ENCODING_SIZE) as u64
                            + n as u64
                            + c.chunk.bytes().len() as u64
                            + CRC_SIZE as u64;
                }
            };
            add_chunks(100);
            hrw.cut_new_file();
            add_chunks(10); // For chunks in in-memory buffer.
        }

        // Checking on-disk bytes for the first file.
        let mmapped = hrw.test_mmapped_chunk_files();
        assert_eq!(
            3,
            mmapped.len(),
            "expected 3 mmapped files, got {}",
            mmapped.len()
        );
        // adapted: Go checks len(hrw.closers) == len(hrw.mmappedChunkFiles); they are one map here.

        let actual_bytes = std::fs::read(first_file_name.unwrap()).unwrap();

        // Check header of the segment file.
        assert_eq!(
            MAGIC_HEAD_CHUNKS,
            u32::from_be_bytes(actual_bytes[0..MAGIC_CHUNKS_SIZE].try_into().unwrap())
        );
        assert_eq!(CHUNKS_FORMAT_V1, actual_bytes[MAGIC_CHUNKS_SIZE]);

        // Remaining chunk data.
        let file_end = HEAD_CHUNK_FILE_HEADER_SIZE + expected_bytes.len();
        assert_eq!(
            expected_bytes,
            actual_bytes[HEAD_CHUNK_FILE_HEADER_SIZE..file_end]
        );

        // Testing reading of chunks.
        for exp in &expected_data {
            let act_chunk = hrw.chunk(exp.chunk_ref).unwrap();
            assert_eq!(exp.chunk.bytes(), act_chunk.bytes());
        }

        // Testing IterateAllChunks method.
        let dir = hrw.dir().to_path_buf();
        hrw.close().unwrap();
        hrw = create_chunk_disk_mapper(Some(&dir), wqs, "");

        let mut idx = 0;
        hrw.iterate_all_chunks(
            |series_ref: HeadSeriesRef,
             chunk_ref: ChunkDiskMapperRef,
             _,
             maxt: i64,
             num_samples: u16,
             _: Encoding,
             is_ooo: bool| {
                let exp_data = &expected_data[idx];
                assert_eq!(exp_data.series_ref, series_ref);
                assert_eq!(exp_data.chunk_ref, chunk_ref);
                assert_eq!(exp_data.maxt, maxt);
                assert_eq!(exp_data.maxt, maxt);
                assert_eq!(exp_data.num_samples, num_samples);
                assert_eq!(exp_data.is_ooo, is_ooo);

                let act_chunk = hrw.chunk(exp_data.chunk_ref).unwrap();
                assert_eq!(exp_data.chunk.bytes(), act_chunk.bytes());

                idx += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(expected_data.len(), idx);

        hrw.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Tests that truncation happens properly based on the file number, that the active file is not deleted even
/// if the number makes it eligible, and that a non-empty current file leads to another file after truncation.
#[test]
fn test_chunk_disk_mapper_truncate() {
    for wqs in WRITE_QUEUE_SIZES {
        let r = Rand::new();
        let mut hrw = create_chunk_disk_mapper(None, wqs, "truncate");

        let mut time_range = 0i64;
        let mut add_chunk = |hrw: &ChunkDiskMapper| {
            let step = 100;
            let (mint, maxt) = (time_range + 1, time_range + step - 1);
            let (_, err) = write_and_wait(hrw, 1, mint, maxt, random_chunk(&r), false);
            err.unwrap();
            time_range += step;
        };

        // Create segments 1 to 7.
        for _ in 1..=7 {
            hrw.cut_new_file();
            add_chunk(&hrw);
        }
        verify_files(&hrw, &[1, 2, 3, 4, 5, 6, 7]);

        // Truncating files.
        hrw.truncate(3).unwrap();

        // Add a chunk to trigger cutting of new file.
        add_chunk(&hrw);

        verify_files(&hrw, &[3, 4, 5, 6, 7, 8]);

        let dir = hrw.dir().to_path_buf();
        hrw.close().unwrap();

        // Restarted.
        hrw = create_chunk_disk_mapper(Some(&dir), wqs, "");

        verify_files(&hrw, &[3, 4, 5, 6, 7, 8]);
        // New file is created after restart even if last file was empty.
        add_chunk(&hrw);
        verify_files(&hrw, &[3, 4, 5, 6, 7, 8, 9]);

        // Truncating files after restart.
        hrw.truncate(6).unwrap();
        verify_files(&hrw, &[6, 7, 8, 9]);

        // Truncating a second time without adding a chunk shouldn't create a new file.
        hrw.truncate(6).unwrap();
        verify_files(&hrw, &[6, 7, 8, 9]);

        // Add a chunk to trigger cutting of new file.
        add_chunk(&hrw);

        verify_files(&hrw, &[6, 7, 8, 9, 10]);

        // Truncation by file number.
        hrw.truncate(8).unwrap();
        verify_files(&hrw, &[8, 9, 10]);

        // Truncating till current time should not delete the current active file.
        hrw.truncate(10).unwrap();

        // Add a chunk to trigger cutting of new file.
        add_chunk(&hrw);

        verify_files(&hrw, &[10, 11]); // One file is the previously active file and one currently created.

        hrw.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Tests that truncation doesn't poke holes into the file sequence, even if there are empty files in between
/// non-empty files (https://github.com/prometheus/prometheus/issues/7412).
#[test]
fn test_chunk_disk_mapper_truncate_preserves_file_sequence() {
    for wqs in WRITE_QUEUE_SIZES {
        let r = Rand::new();
        let mut hrw = create_chunk_disk_mapper(None, wqs, "truncate_preserves");
        let mut time_range = 0i64;

        let mut add_chunk = |hrw: &ChunkDiskMapper| {
            let step = 100;
            let (mint, maxt) = (time_range + 1, time_range + step - 1);
            let (_, err) = write_and_wait(hrw, 1, mint, maxt, random_chunk(&r), false);
            err.unwrap();
            time_range += step;
        };

        let empty_file = |hrw: &ChunkDiskMapper| {
            hrw.test_cut().unwrap();
            hrw.test_evtl_pos_to_new_file();
        };

        // Go nonEmptyFile() is emptyFile() followed by addChunk().
        add_chunk(&hrw); // 1. Created with the first chunk.
        empty_file(&hrw); // 2. nonEmptyFile()
        add_chunk(&hrw);
        empty_file(&hrw); // 3. nonEmptyFile()
        add_chunk(&hrw);
        empty_file(&hrw); // 4. emptyFile()
        empty_file(&hrw); // 5. nonEmptyFile()
        add_chunk(&hrw);
        empty_file(&hrw); // 6. emptyFile()

        verify_files(&hrw, &[1, 2, 3, 4, 5, 6]);

        // Truncating files till 2. It should not delete anything after 3 (inclusive) though files 4 and 6 are
        // empty.
        hrw.truncate(3).unwrap();
        verify_files(&hrw, &[3, 4, 5, 6]);

        // Add chunk, so file 6 is not empty anymore.
        add_chunk(&hrw);
        verify_files(&hrw, &[3, 4, 5, 6]);

        // Truncating till file 3 should also delete file 4, because it is empty.
        hrw.truncate(5).unwrap();
        add_chunk(&hrw);
        verify_files(&hrw, &[5, 6, 7]);

        let dir = hrw.dir().to_path_buf();
        hrw.close().unwrap();
        // Restarting checks for unsequential files.
        hrw = create_chunk_disk_mapper(Some(&dir), wqs, "");
        verify_files(&hrw, &[5, 6, 7]);

        hrw.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn test_chunk_disk_mapper_truncate_write_queue_race_condition() {
    for wqs in WRITE_QUEUE_SIZES {
        let r = Rand::new();
        let hrw = create_chunk_disk_mapper(None, wqs, "truncate_race");

        // This test should only run when the queue is enabled.
        let Some(q) = hrw.test_write_queue() else {
            let dir = hrw.dir().to_path_buf();
            hrw.close().unwrap();
            let _ = std::fs::remove_dir_all(&dir);
            continue;
        };

        // Add an artificial delay in the write function to easily trigger the race condition.
        let orig_write_chunk = q.write_chunk_fn();
        let delayed: WriteChunkF =
            Arc::new(move |series_ref, mint, maxt, chk, r, is_ooo, cut_file| {
                std::thread::sleep(Duration::from_millis(100));
                orig_write_chunk(series_ref, mint, maxt, chk, r, is_ooo, cut_file)
            });
        q.set_write_chunk_fn(delayed);

        let (tx, rx) = mpsc::channel::<Result<(), Error>>();

        // Write a chunk. Since the queue is enabled, the chunk will be written asynchronously (with the
        // artificial delay).
        let tx1 = tx.clone();
        let r1 = hrw.write_chunk(
            1,
            0,
            10,
            random_chunk(&r),
            false,
            Some(Box::new(move |err| tx1.send(err).unwrap())),
        );

        let (seq, _) = r1.unpack();
        assert_eq!(1, seq);

        // Truncate, simulating that all chunks from segment files before 1 can be dropped.
        hrw.truncate(1).unwrap();

        // Request to cut a new file when writing the next chunk. If there's a race condition, cutting a new file
        // will allow us to detect there's actually an issue with the sequence number (because it's checked when
        // a new segment file is created).
        hrw.cut_new_file();

        // Write another chunk. This will cut a new file.
        let r2 = hrw.write_chunk(
            1,
            0,
            10,
            random_chunk(&r),
            false,
            Some(Box::new(move |err| tx.send(err).unwrap())),
        );

        let (seq, _) = r2.unpack();
        assert_eq!(2, seq);

        // Go: wg.Wait() for both callbacks, each requiring no error.
        rx.recv().unwrap().unwrap();
        rx.recv().unwrap().unwrap();

        let dir = hrw.dir().to_path_buf();
        hrw.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Tests https://github.com/prometheus/prometheus/issues/7753.
#[test]
fn test_head_read_writer_truncate_after_failed_iterate_chunks() {
    for wqs in WRITE_QUEUE_SIZES {
        let r = Rand::new();
        let mut hrw = create_chunk_disk_mapper(None, wqs, "truncate_after_failed_iterate");

        // Write a chunk to iterate on it later.
        let (_, err) = write_and_wait(&hrw, 1, 0, 1000, random_chunk(&r), false);
        err.unwrap();

        let dir = hrw.dir().to_path_buf();
        hrw.close().unwrap();

        // Restarting to recreate https://github.com/prometheus/prometheus/issues/7753.
        hrw = create_chunk_disk_mapper(Some(&dir), wqs, "");

        // Forcefully failing IterateAllChunks.
        assert!(
            hrw.iterate_all_chunks(|_, _, _, _, _, _, _| Err(Error::msg("random error")))
                .is_err()
        );

        // Truncation call should not return error after IterateAllChunks fails.
        hrw.truncate(2000).unwrap();

        hrw.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Covers more than empty files; the name is kept for history.
#[test]
fn test_head_read_writer_read_repair_on_empty_last_file() {
    for wqs in WRITE_QUEUE_SIZES {
        let r = Rand::new();
        let hrw = create_chunk_disk_mapper(None, wqs, "read_repair");

        let mut time_range = 0i64;
        let mut add_chunk = |hrw: &ChunkDiskMapper| {
            let step = 100;
            let (mint, maxt) = (time_range + 1, time_range + step - 1);
            let (_, err) = write_and_wait(hrw, 1, mint, maxt, random_chunk(&r), false);
            err.unwrap();
            time_range += step;
        };

        add_chunk(&hrw); // 1. Created with the first chunk.
        hrw.cut_new_file(); // 2.
        add_chunk(&hrw);
        hrw.cut_new_file(); // 3.
        add_chunk(&hrw);

        let mmapped = hrw.test_mmapped_chunk_files();
        assert_eq!(3, mmapped.len());
        let last_file = mmapped.iter().copied().max().unwrap_or(0);
        assert_eq!(3, last_file);
        let dir = hrw.dir().to_path_buf();
        hrw.close().unwrap();
        drop(hrw);

        let write_corrupt_last_file = |b: &[u8]| {
            use std::io::Write;
            let fname = segment_file(&dir, (last_file + 1) as i64);
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&fname)
                .unwrap();
            f.write_all(b).unwrap();
            f.sync_all().unwrap();
            let stat = f.metadata().unwrap();
            assert_eq!(b.len() as u64, stat.len());
        };

        let check_repair = || {
            // Open chunk disk mapper again, corrupt file should be removed.
            let hrw = create_chunk_disk_mapper(Some(&dir), wqs, "");

            // Removed from memory.
            let mmapped = hrw.test_mmapped_chunk_files();
            assert_eq!(3, mmapped.len());
            for idx in mmapped {
                assert!(
                    idx <= last_file,
                    "file index is bigger than previous last file"
                );
            }

            // Removed even from disk.
            let files: Vec<_> = std::fs::read_dir(&dir)
                .unwrap()
                .map(|e| e.unwrap())
                .collect();
            assert_eq!(3, files.len());
            for fi in files {
                let seq: u64 = fi.file_name().to_str().unwrap().parse().unwrap();
                assert!(
                    seq <= last_file as u64,
                    "file index on disk is bigger than previous last file"
                );
            }

            hrw.close().unwrap();
        };

        // Write an empty last file mimicking an abrupt shutdown on file creation.
        write_corrupt_last_file(&[]);
        // Removes empty last file.
        check_repair();

        // Write another empty last file with 0 bytes.
        write_corrupt_last_file(&[0, 0, 0, 0, 0, 0, 0, 0]);
        // Removes the 0 filled last file.
        check_repair();

        // Write another corrupt file with less than 4 bytes (size of magic number).
        write_corrupt_last_file(&[1, 2]);
        // Removes the partial file.
        check_repair();

        // Check that it does not delete a valid last file.
        check_repair();

        let _ = std::fs::remove_dir_all(&dir);
    }
}
