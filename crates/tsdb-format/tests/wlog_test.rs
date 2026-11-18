//! Port of `tsdb/wlog/wlog_test.go`.

use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tsdb_format::fileutil;
use tsdb_format::wlog::compression::Type;
use tsdb_format::wlog::{
    PAGE_SIZE, RECORD_HEADER_SIZE, Reader, RecType, SegmentFile, WL, list_segments,
    new_segment_buf_reader, new_segments_reader, open_read_segment, segment_name, segments,
};

fn temp_dir(name: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let d = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "wlog_test-{name}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

/// Random bytes (Go `crypto/rand.Read` in the tests; any random content works).
fn rand_fill(b: &mut [u8]) {
    static SEED: AtomicU64 = AtomicU64::new(0);
    let mut x = SEED.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
        | 1;
    for c in b.iter_mut() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *c = x as u8;
    }
}

// adapted: Go's TestMain runs goleak.VerifyTestMain; Rust has no goroutines. The background segment close
// threads are joined by WL::close.

/// TestWALRepair_ReadingError ensures that a repair is run for an error
/// when reading a record.
#[test]
fn test_wal_repair_reading_error() {
    type CorrFunc = fn(&mut fs::File);
    let tests: Vec<(&str, i64, CorrFunc, usize)> = vec![
        (
            "torn_last_record",
            2,
            |f| {
                f.seek(SeekFrom::Start(PAGE_SIZE as u64 * 2)).unwrap();
                f.write_all(&[RecType::FIRST.0]).unwrap();
            },
            8,
        ),
        // Ensures that the page buffer is big enough to fit
        // an entire page size without panicking.
        // https://github.com/prometheus/tsdb/pull/414
        (
            "bad_header",
            1,
            |f| {
                f.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
                f.write_all(&[RecType::PAGE_TERM.0]).unwrap();
            },
            4,
        ),
        (
            "bad_fragment_sequence",
            1,
            |f| {
                f.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
                f.write_all(&[RecType::LAST.0]).unwrap();
            },
            4,
        ),
        (
            "bad_fragment_flag",
            1,
            |f| {
                f.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
                f.write_all(&[123]).unwrap();
            },
            4,
        ),
        (
            "bad_checksum",
            1,
            |f| {
                f.seek(SeekFrom::Start(PAGE_SIZE as u64 + 4)).unwrap();
                f.write_all(&[0]).unwrap();
            },
            4,
        ),
        (
            "bad_length",
            1,
            |f| {
                f.seek(SeekFrom::Start(PAGE_SIZE as u64 + 2)).unwrap();
                f.write_all(&[0]).unwrap();
            },
            4,
        ),
        (
            "bad_content",
            1,
            |f| {
                f.seek(SeekFrom::Start(PAGE_SIZE as u64 + 100)).unwrap();
                f.write_all(b"beef").unwrap();
            },
            4,
        ),
    ];
    for (name, corr_sgm, corr_func, intact_recs) in tests {
        let dir = temp_dir(name);

        // We create 3 segments with 3 records each and
        // then corrupt a given record in a given segment.
        // As a result we want a repaired WAL with given intact records.
        let seg_size = 3 * PAGE_SIZE;
        let w = WL::new_size(&dir, seg_size, Type::None).unwrap();

        let mut records: Vec<Vec<u8>> = Vec::new();

        for i in 1..=9 {
            let mut b = vec![0u8; PAGE_SIZE - RECORD_HEADER_SIZE];
            b[0] = i as u8;
            w.log(&b).unwrap();
            records.push(b);
        }
        let (first, last) = segments(w.dir()).unwrap();
        assert_eq!(
            3,
            1 + last - first,
            "{name}: wlog creation didn't result in expected number of segments"
        );

        w.close().unwrap();

        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(segment_name(&dir, corr_sgm))
            .unwrap();

        // Apply corruption function.
        corr_func(&mut f);

        drop(f);

        let w = WL::new_size(&dir, seg_size, Type::None).unwrap();

        let (first, last) = segments(w.dir()).unwrap();

        // Backfill segments from the most recent checkpoint onwards.
        for i in first..=last {
            let s = open_read_segment(&segment_name(w.dir(), i)).unwrap();

            let sr = new_segment_buf_reader(vec![s]);
            let mut r = Reader::new(sr);
            while r.next() {}

            // No corruption in this segment.
            let Some(err) = r.err() else { continue };
            w.repair(&err).unwrap();
            break;
        }

        let sr = new_segments_reader(&dir).unwrap();
        let mut r = Reader::new(sr);

        let mut result: Vec<Vec<u8>> = Vec::new();
        while r.next() {
            result.push(r.record().to_vec());
        }
        assert!(r.err().is_none(), "{name}: {:?}", r.err());
        assert_eq!(
            result.len(),
            intact_recs,
            "{name}: Wrong number of intact records"
        );

        for (i, r) in result.iter().enumerate() {
            assert!(
                records[i] == *r,
                "{name}: record {i} diverges: want {:x?}, got {:x?}",
                &records[i][..10],
                &r[..10]
            );
        }

        // Make sure there is a new 0 size Segment after the corrupted Segment.
        let (_, last) = segments(w.dir()).unwrap();
        assert_eq!(corr_sgm + 1, last, "{name}");
        let fi = fs::metadata(segment_name(&dir, last)).unwrap();
        assert_eq!(0, fi.len(), "{name}");
        w.close().unwrap();
    }
}

/// TestCorruptAndCarryOn writes a multi-segment WAL; corrupts the first segment and
/// ensures that an error during reading that segment are correctly repaired before
/// moving to write more records to the WAL.
#[test]
fn test_corrupt_and_carry_on() {
    let dir = temp_dir("corrupt_and_carry_on");

    let segment_size = PAGE_SIZE * 3;
    let record_size = (PAGE_SIZE / 3) - RECORD_HEADER_SIZE;

    // Produce a WAL with a two segments of 3 pages with 3 records each,
    // so when we truncate the file we're guaranteed to split a record.
    {
        let w = WL::new_size(&dir, segment_size, Type::None).unwrap();

        for _ in 0..18 {
            let mut buf = vec![0u8; record_size];
            rand_fill(&mut buf);
            w.log(&buf).unwrap();
        }

        w.close().unwrap();
    }

    // Check all the segments are the correct size.
    {
        let segments = list_segments(&dir).unwrap();
        for segment in segments {
            let f = fs::File::open(dir.join(format!("{:08}", segment.index))).unwrap();
            let fi = f.metadata().unwrap();
            assert_eq!(
                segment_size as u64,
                fi.len(),
                "segment {} size",
                segment.index
            );
        }
    }

    // Truncate the first file, splitting the middle record in the second
    // page in half, leaving 4 valid records.
    {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.join(format!("{:08}", 0)))
            .unwrap();
        let fi = f.metadata().unwrap();
        assert_eq!(segment_size as u64, fi.len());

        f.set_len((segment_size / 2) as u64).unwrap();
    }

    // Now try and repair this WAL, and write 5 more records to it.
    {
        let sr = new_segments_reader(&dir).unwrap();

        let mut reader = Reader::new(sr);
        let mut i = 0;
        while i < 4 && reader.next() {
            assert_eq!(reader.record().len(), record_size);
            i += 1;
        }
        assert_eq!(4, i, "not enough records");
        assert!(!reader.next(), "unexpected record");

        let corruption_err = reader.err();
        assert!(corruption_err.is_some());
        let corruption_err = corruption_err.unwrap();

        reader.into_inner().close().unwrap();

        let w = WL::new_size(&dir, segment_size, Type::None).unwrap();

        w.repair(&corruption_err).unwrap();

        // Ensure that we have a completely clean slate after repairing.
        assert_eq!(Some(1), w.segment_index()); // We corrupted segment 0.
        assert_eq!(0, w.done_pages());

        for _ in 0..5 {
            let mut buf = vec![0u8; record_size];
            rand_fill(&mut buf);
            w.log(&buf).unwrap();
        }

        w.close().unwrap();
    }

    // Replay the WAL. Should get 9 records.
    {
        let sr = new_segments_reader(&dir).unwrap();

        let mut reader = Reader::new(sr);
        let mut i = 0;
        while i < 9 && reader.next() {
            assert_eq!(reader.record().len(), record_size);
            i += 1;
        }
        assert_eq!(9, i, "wrong number of records");
        assert!(!reader.next(), "unexpected record");
        assert!(reader.err().is_none());
    }
}

/// TestClose ensures that calling Close more than once doesn't panic and doesn't block.
#[test]
fn test_close() {
    let dir = temp_dir("close");
    let w = WL::new_size(&dir, PAGE_SIZE, Type::None).unwrap();
    w.close().unwrap();
    assert!(w.close().is_err());
}

#[test]
fn test_segment_metric() {
    let segment_size = PAGE_SIZE;
    let record_size = (PAGE_SIZE / 2) - RECORD_HEADER_SIZE;

    let dir = temp_dir("segment_metric");
    let w = WL::new_size(&dir, segment_size, Type::None).unwrap();

    let initial_segment = w.metrics().current_segment.load(Ordering::Relaxed);

    // Write 3 records, each of which is half the segment size, meaning we should rotate to the next segment.
    for _ in 0..3 {
        let mut buf = vec![0u8; record_size];
        rand_fill(&mut buf);
        w.log(&buf).unwrap();
    }
    assert_eq!(
        initial_segment + 1,
        w.metrics().current_segment.load(Ordering::Relaxed),
        "segment metric did not increment after segment rotation"
    );
    w.close().unwrap();
}

#[test]
fn test_compression() {
    let bootstrap = |compressed: Type| -> PathBuf {
        let segment_size = PAGE_SIZE;
        let record_size = (PAGE_SIZE / 2) - RECORD_HEADER_SIZE;
        let records = 100;

        let dir_path = temp_dir(&format!("compression-{compressed}"));

        let w = WL::new_size(&dir_path, segment_size, compressed).unwrap();

        let buf = vec![0u8; record_size];
        for _ in 0..records {
            w.log(&buf).unwrap();
        }
        w.close().unwrap();

        dir_path
    };

    let mut tmp_dirs = Vec::with_capacity(3);

    let dir_un_compressed = bootstrap(Type::None);
    tmp_dirs.push(dir_un_compressed.clone());

    for compression_type in [Type::Snappy, Type::Zstd] {
        let dir_compressed = bootstrap(compression_type);
        tmp_dirs.push(dir_compressed.clone());

        let uncompressed_size = fileutil::dir_size(&dir_un_compressed).unwrap();
        let compressed_size = fileutil::dir_size(&dir_compressed).unwrap();

        assert!(
            uncompressed_size as f64 * 0.75 > compressed_size as f64,
            "Compressing zeroes should save at least 25% space - uncompressedSize: {uncompressed_size}, compressedSize: {compressed_size}"
        );
    }
    for dir in tmp_dirs {
        fs::remove_dir_all(dir).unwrap();
    }
}

struct FaultySegmentFile {
    inner: Box<dyn SegmentFile>,
    written: usize,
    write_failure_after: i64,
}

impl Read for FaultySegmentFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl SegmentFile for FaultySegmentFile {
    fn write_all_n(&mut self, p: &[u8]) -> (usize, io::Result<()>) {
        if self.write_failure_after >= 0
            && (self.write_failure_after as usize) < self.written + p.len()
        {
            let mut partial_len = self.write_failure_after - self.written as i64;
            if partial_len <= 0 || partial_len as usize >= p.len() {
                partial_len = 1;
            }

            // Inject failure.
            let (n, _) = self.inner.write_all_n(&p[..partial_len as usize]);
            self.written += n;
            self.write_failure_after = -1;

            // Go io.ErrShortWrite.
            return (
                n,
                Err(io::Error::new(io::ErrorKind::WriteZero, "short write")),
            );
        }

        // Proxy the write to the underlying file.
        let (n, r) = self.inner.write_all_n(p);
        self.written += n;
        (n, r)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.inner.sync()
    }

    fn size(&self) -> io::Result<i64> {
        self.inner.size()
    }
}

#[test]
fn test_log_partial_write() {
    let segment_size = PAGE_SIZE * 2;
    let record: &[u8] = &[1, 2, 3, 4, 5];

    let tests: Vec<(&str, usize, usize)> = vec![
        ("partial write when logging first record in a page", 10, 1),
        (
            "partial write when logging record in the middle of a page",
            10,
            3,
        ),
        (
            "partial write when logging last record of a page",
            (PAGE_SIZE / (RECORD_HEADER_SIZE + record.len())) + 10,
            PAGE_SIZE / (RECORD_HEADER_SIZE + record.len()),
        ),
        // TODO the current implementation suffers this:
        // "partial write when logging a record overlapping two pages": {
        //	numRecords:   (pageSize / (recordHeaderSize + len(record))) + 10,
        //	faultyRecord: pageSize/(recordHeaderSize+len(record)) + 1,
        // },
    ];

    for (test_name, num_records, faulty_record) in tests {
        let dir_path = temp_dir("log_partial_write");

        let w = WL::new_size(&dir_path, segment_size, Type::None).unwrap();

        // Replace the underlying segment file with a mocked one that injects a failure.
        w.with_active_segment(|s| {
            s.wrap_file(|inner| {
                Box::new(FaultySegmentFile {
                    inner,
                    written: 0,
                    write_failure_after: (((RECORD_HEADER_SIZE + record.len())
                        * (faulty_record - 1))
                        + 2) as i64,
                })
            })
        })
        .unwrap();

        for i in 1..=num_records {
            let r = w.log(record);
            if i == faulty_record {
                let err = r.expect_err(test_name);
                let ioe = err.io_error().expect(test_name);
                assert_eq!(ioe.kind(), io::ErrorKind::WriteZero, "{test_name}");
                assert_eq!(ioe.to_string(), "short write", "{test_name}");
            } else {
                r.unwrap();
            }
        }

        w.close().unwrap();

        // Read it back. We expect no corruption.
        let s = open_read_segment(&segment_name(&dir_path, 0)).unwrap();

        let mut r = Reader::new(new_segment_buf_reader(vec![s]));
        for _ in 0..num_records {
            assert!(r.next(), "{test_name}");
            assert!(r.err().is_none(), "{test_name}");
            assert_eq!(record, r.record(), "{test_name}");
        }
        assert!(!r.next(), "{test_name}");
        assert!(r.err().is_none(), "{test_name}");
        r.into_inner().close().unwrap();
    }
}

#[test]
fn test_unregister_metrics() {
    // adapted: Go registers the WL metrics in one registry twice and checks that Close unregisters them (no
    // duplicate registration panic). There is no registry yet (TODO(metrics)); the closest check is that two
    // logs can be created and closed one after the other, each with its own metrics under the WAL prefix.
    for i in 0..2 {
        let wl = WL::new(temp_dir(&format!("unregister_metrics-{i}")), Type::None).unwrap();
        assert_eq!("prometheus_tsdb_wal_", wl.metrics().prefix);
        wl.close().unwrap();
    }
}
