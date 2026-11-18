//! Port of `tsdb/wlog/reader_test.go`.

use std::fs::{self, OpenOptions};
use std::io::{Cursor, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use tsdb_format::encoding::crc32c;
use tsdb_format::wlog::compression::Type;
use tsdb_format::wlog::{
    Error, LiveReader, LiveReaderMetrics, PAGE_SIZE, RECORD_HEADER_SIZE, Reader, RecType, WL,
    list_segments, open_read_segment, segment_name, segments,
};

fn temp_dir(name: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let d = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "reader_test-{name}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

/// A small xorshift generator (Go `crypto/rand` in the tests; any random values work).
struct Rng(u64);

impl Rng {
    fn new() -> Rng {
        static SEED: AtomicU64 = AtomicU64::new(0);
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        Rng((t ^ SEED.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    /// Go `rand.Int(rand.Reader, big.NewInt(n))`: uniform in [0, n).
    fn int(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn fill(&mut self, b: &mut [u8]) {
        for c in b.iter_mut() {
            *c = self.next() as u8;
        }
    }
}

/// Go `reader` interface over Reader and LiveReader.
trait TestReader {
    fn next(&mut self) -> bool;
    fn err(&self) -> Option<Error>;
    fn record(&self) -> &[u8];
}

impl<R: Read + 'static> TestReader for Reader<R> {
    fn next(&mut self) -> bool {
        Reader::next(self)
    }
    fn err(&self) -> Option<Error> {
        Reader::err(self)
    }
    fn record(&self) -> &[u8] {
        Reader::record(self)
    }
}

impl<R: Read> TestReader for LiveReader<R> {
    fn next(&mut self) -> bool {
        LiveReader::next(self)
    }
    fn err(&self) -> Option<Error> {
        LiveReader::err(self).cloned()
    }
    fn record(&self) -> &[u8] {
        LiveReader::record(self)
    }
}

type Constructor = fn(Box<dyn Read>) -> Box<dyn TestReader>;

fn reader_constructors() -> Vec<(&'static str, Constructor)> {
    vec![
        ("Reader", |r| Box::new(Reader::new(r))),
        ("LiveReader", |r| {
            let mut lr = LiveReader::new(Some(LiveReaderMetrics::new()), r);
            lr.set_eof_non_err(true);
            Box::new(lr)
        }),
    ]
}

struct Rec {
    t: RecType,
    b: Vec<u8>,
}

struct Case {
    t: Vec<Rec>,
    exp: Vec<Vec<u8>>,
    fail: bool,
}

fn data() -> Vec<u8> {
    vec![0u8; 100000]
}

fn rec(t: RecType, b: &[u8]) -> Rec {
    Rec { t, b: b.to_vec() }
}

fn test_reader_cases() -> Vec<Case> {
    let data = data();
    let mut page_term_nonzero = vec![0u8; PAGE_SIZE - RECORD_HEADER_SIZE - 102];
    page_term_nonzero.push(1);
    vec![
        // Sequence of valid records.
        Case {
            t: vec![
                rec(RecType::FULL, &data[0..200]),
                rec(RecType::FIRST, &data[200..300]),
                rec(RecType::LAST, &data[300..400]),
                rec(RecType::FIRST, &data[400..800]),
                rec(RecType::MIDDLE, &data[800..900]),
                rec(
                    RecType::PAGE_TERM,
                    &vec![0u8; PAGE_SIZE - 900 - RECORD_HEADER_SIZE * 5 - 1],
                ), // exactly lines up with page boundary.
                rec(RecType::LAST, &data[900..900]),
                rec(RecType::FIRST, &data[900..1000]),
                rec(RecType::MIDDLE, &data[1000..1200]),
                rec(RecType::MIDDLE, &data[1200..30000]),
                rec(RecType::MIDDLE, &data[30000..30001]),
                rec(RecType::MIDDLE, &data[30001..30001]),
                rec(RecType::LAST, &data[30001..32000]),
            ],
            exp: vec![
                data[0..200].to_vec(),
                data[200..400].to_vec(),
                data[400..900].to_vec(),
                data[900..32000].to_vec(),
            ],
            fail: false,
        },
        // Exactly at the limit of one page minus the header size
        Case {
            t: vec![rec(RecType::FULL, &data[0..PAGE_SIZE - RECORD_HEADER_SIZE])],
            exp: vec![data[..PAGE_SIZE - RECORD_HEADER_SIZE].to_vec()],
            fail: false,
        },
        // More than a full page, this exceeds our buffer and can never happen
        // when written by the WAL.
        Case {
            t: vec![rec(RecType::FULL, &data[0..PAGE_SIZE + 1])],
            exp: vec![],
            fail: true,
        },
        // Two records the together are too big for a page.
        // NB currently the non-live reader succeeds on this. I think this is a bug.
        // but we've seen it in production.
        Case {
            t: vec![
                rec(RecType::FULL, &data[..PAGE_SIZE / 2]),
                rec(RecType::FULL, &data[..PAGE_SIZE / 2]),
            ],
            exp: vec![
                data[..PAGE_SIZE / 2].to_vec(),
                data[..PAGE_SIZE / 2].to_vec(),
            ],
            fail: false,
        },
        // Invalid orders of record types.
        Case {
            t: vec![rec(RecType::MIDDLE, &data[..200])],
            exp: vec![],
            fail: true,
        },
        Case {
            t: vec![rec(RecType::LAST, &data[..200])],
            exp: vec![],
            fail: true,
        },
        Case {
            t: vec![
                rec(RecType::FIRST, &data[..200]),
                rec(RecType::FULL, &data[200..400]),
            ],
            exp: vec![],
            fail: true,
        },
        Case {
            t: vec![
                rec(RecType::FIRST, &data[..100]),
                rec(RecType::MIDDLE, &data[100..200]),
                rec(RecType::FULL, &data[200..400]),
            ],
            exp: vec![],
            fail: true,
        },
        // Non-zero data after page termination.
        Case {
            t: vec![
                rec(RecType::FULL, &data[..100]),
                rec(RecType::PAGE_TERM, &page_term_nonzero),
            ],
            exp: vec![data[..100].to_vec()],
            fail: true,
        },
    ]
}

fn encoded_record(t: RecType, b: &[u8]) -> Vec<u8> {
    if t == RecType::PAGE_TERM {
        let mut r = vec![0u8];
        r.extend_from_slice(b);
        return r;
    }
    let mut r = vec![0u8; RECORD_HEADER_SIZE];
    r[0] = t.0;
    r[1..3].copy_from_slice(&(b.len() as u16).to_be_bytes());
    r[3..7].copy_from_slice(&crc32c(b).to_be_bytes());
    r.extend_from_slice(b);
    r
}

/// TestReader feeds the reader a stream of encoded records with different types.
#[test]
fn test_reader() {
    for (name, f) in reader_constructors() {
        for (i, c) in test_reader_cases().iter().enumerate() {
            let mut buf = Vec::new();
            for r in &c.t {
                buf.extend_from_slice(&encoded_record(r.t, &r.b));
            }
            let mut r = f(Box::new(Cursor::new(buf)));

            let mut j = 0;
            while r.next() {
                let rec = r.record();

                assert!(
                    j < c.exp.len(),
                    "{name}/{i}: received more records than expected"
                );
                assert_eq!(
                    c.exp[j], rec,
                    "{name}/{i}: Bytes within record did not match expected Bytes"
                );
                j += 1;
            }
            if !c.fail {
                assert!(r.err().is_none(), "{name}/{i}: {:?}", r.err());
            } else {
                assert!(r.err().is_some(), "{name}/{i}");
            }
        }
    }
}

#[test]
fn test_reader_live() {
    for (i, case) in test_reader_cases().into_iter().enumerate() {
        let dir = temp_dir("reader_live");
        let path = dir.join("TestReader_Live");
        let mut write_fd = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        let recs: Vec<Vec<u8>> = case.t.iter().map(|r| encoded_record(r.t, &r.b)).collect();
        let writer = std::thread::spawn(move || {
            for rec in recs {
                write_fd.write_all(&rec).unwrap();
                std::thread::yield_now();
            }
        });

        // Read from a second FD on the same file.
        let read_fd = fs::File::open(&path).unwrap();
        let mut reader = LiveReader::new(Some(LiveReaderMetrics::new()), read_fd);
        for exp in &case.exp {
            while !reader.next() {
                assert!(
                    matches!(reader.err(), Some(Error::Eof)),
                    "{i}: expect EOF, got: {:?}",
                    reader.err()
                );
                std::thread::yield_now();
            }

            let actual = reader.record();
            assert_eq!(exp.as_slice(), actual, "{i}: read wrong record");
        }

        writer.join().unwrap();
        assert!(!reader.next(), "{i}: unexpected record");
        if case.fail {
            assert!(reader.err().is_some(), "{i}");
        }
        fs::remove_dir_all(&dir).unwrap();
    }
}

const FUZZ_LEN: usize = 500;

fn generate_random_entries(
    w: &WL,
    records: &mpsc::SyncSender<Vec<u8>>,
) -> tsdb_format::wlog::Result<()> {
    let mut rng = Rng::new();
    let mut recs: Vec<Vec<u8>> = Vec::new();
    for i in 0..FUZZ_LEN {
        let sz = match i % 5 {
            0 | 1 => 50,
            2 | 3 => PAGE_SIZE as u64,
            _ => PAGE_SIZE as u64 * 8,
        };
        let n = rng.int(sz);
        let mut rec = vec![0u8; n as usize];
        rng.fill(&mut rec);

        records.send(rec.clone()).unwrap();

        // Randomly batch up records.
        recs.push(rec);
        let n = rng.int(4);
        if n < 3 {
            w.log_batch(&recs)?;
            recs.clear();
        }
    }
    w.log_batch(&recs)
}

/// Go `allSegments`: one reader over all segment files of `dir` in order.
fn all_segments(dir: &Path) -> Box<dyn Read> {
    let seg = list_segments(dir).unwrap();
    let mut reader: Box<dyn Read> = Box::new(std::io::empty());
    for r in seg {
        let f = fs::File::open(dir.join(&r.name)).unwrap();
        reader = Box::new(reader.chain(f));
    }
    reader
}

#[test]
fn test_reader_fuzz() {
    for (name, f) in reader_constructors() {
        for compress in Type::ALL {
            let dir = temp_dir("reader_fuzz");

            let w = WL::new_size(&dir, 128 * PAGE_SIZE, compress).unwrap();

            // Buffering required as we're not reading concurrently.
            let (tx, input) = mpsc::sync_channel(FUZZ_LEN);
            generate_random_entries(&w, &tx).unwrap();
            drop(tx);

            w.close().unwrap();

            let sr = all_segments(w.dir());

            let mut reader = f(sr);
            for expected in input.iter() {
                assert!(
                    reader.next(),
                    "{name},compress={compress}: expected record: {:?}",
                    reader.err()
                );
                let r = reader.record();
                // Expected value may come as nil or empty slice, so it requires special comparison.
                if expected.is_empty() {
                    assert!(r.is_empty(), "{name},compress={compress}");
                } else {
                    assert_eq!(
                        expected.as_slice(),
                        r,
                        "{name},compress={compress}: read wrong record"
                    );
                }
            }
            assert!(
                !reader.next(),
                "{name},compress={compress}: unexpected record"
            );
            fs::remove_dir_all(&dir).unwrap();
        }
    }
}

#[test]
fn test_reader_fuzz_live() {
    for compress in Type::ALL {
        let dir = temp_dir("reader_fuzz_live");

        let w = Arc::new(WL::new_size(&dir, 128 * PAGE_SIZE, compress).unwrap());

        // In the background, generate a stream of random records and write them
        // to the WAL.
        let (tx, input) = mpsc::sync_channel::<Vec<u8>>(FUZZ_LEN / 10); // buffering required as we sometimes batch WAL writes.
        let (done_tx, done) = mpsc::channel::<()>();
        let wg = w.clone();
        let writer = std::thread::spawn(move || {
            generate_random_entries(&wg, &tx).unwrap();
            std::thread::sleep(Duration::from_millis(100));
            drop(done_tx);
        });

        // Tail the WAL and compare the results.
        let (m, _) = segments(w.dir()).unwrap();

        let seg = open_read_segment(&segment_name(&dir, m)).unwrap();

        let mut r = LiveReader::new(None, seg);

        let read_segment = |r: &mut LiveReader<tsdb_format::wlog::Segment>| {
            while r.next() {
                let rec = r.record();
                let expected = input.recv();
                assert!(expected.is_ok(), "compress={compress}: unexpected record");
                let expected = expected.unwrap();
                // Expected value may come as nil or empty slice, so it requires special comparison.
                if expected.is_empty() {
                    assert!(rec.is_empty());
                } else {
                    assert_eq!(
                        expected.as_slice(),
                        rec,
                        "compress={compress}: record does not match expected"
                    );
                }
            }
            assert!(
                matches!(r.err(), Some(Error::Eof)),
                "compress={compress}: expected EOF, got: {:?}",
                r.err()
            );
        };

        // adapted: Go selects over a 100ms segment ticker, a 10ms read ticker and the done channel. Here one
        // loop sleeps 10ms per turn and runs the segment check once 100ms have passed.
        let segment_every = Duration::from_millis(100);
        let mut next_segment_check = Instant::now() + segment_every;
        loop {
            std::thread::sleep(Duration::from_millis(10));
            if matches!(done.try_recv(), Err(mpsc::TryRecvError::Disconnected)) {
                read_segment(&mut r);
                break;
            }
            if Instant::now() >= next_segment_check {
                next_segment_check += segment_every;
                // check if new segments exist
                let (_, last) = segments(w.dir()).unwrap();
                let seg_i = r.get_ref().index();
                if last <= seg_i {
                    continue;
                }

                // read to end of segment.
                read_segment(&mut r);

                let fi = fs::metadata(segment_name(&dir, seg_i)).unwrap();
                assert_eq!(
                    r.offset(),
                    fi.len() as i64,
                    "expected to have read whole segment, but read {} of {}",
                    r.offset(),
                    fi.len()
                );

                let seg = open_read_segment(&segment_name(&dir, seg_i + 1)).unwrap();
                r = LiveReader::new(None, seg);
                continue;
            }
            read_segment(&mut r);
        }

        assert!(matches!(r.err(), Some(Error::Eof)), "expected EOF");
        writer.join().unwrap();
        w.close().unwrap();
        fs::remove_dir_all(&dir).unwrap();
    }
}

#[test]
fn test_live_reader_corrupt_short_file() {
    // Write a corrupt WAL segment, there is one record of pageSize in length,
    // but the segment is only half written.
    let dir = temp_dir("live_reader_corrupt_short_file");

    let w = WL::new_size(&dir, PAGE_SIZE, Type::None).unwrap();

    let mut rec = vec![0u8; PAGE_SIZE - RECORD_HEADER_SIZE];
    Rng::new().fill(&mut rec);

    w.log(&rec).unwrap();

    w.close().unwrap();

    let segment_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join("00000000"))
        .unwrap();

    segment_file.set_len((PAGE_SIZE / 2) as u64).unwrap();

    drop(segment_file);

    // Try and LiveReader it.
    let (m, _) = segments(w.dir()).unwrap();

    let seg = open_read_segment(&segment_name(&dir, m)).unwrap();

    let mut r = LiveReader::new(None, seg);
    assert!(!r.next(), "expected no records");
    assert!(
        matches!(r.err(), Some(Error::Eof)),
        "expected error, got: {:?}",
        r.err()
    );
}

#[test]
fn test_live_reader_corrupt_record_too_long_and_short() {
    // Write a corrupt WAL segment, when record len > page size.
    let dir = temp_dir("live_reader_corrupt_record_too_long_and_short");

    let w = WL::new_size(&dir, PAGE_SIZE * 2, Type::None).unwrap();

    let mut rec = vec![0u8; PAGE_SIZE - RECORD_HEADER_SIZE];
    Rng::new().fill(&mut rec);

    w.log(&rec).unwrap();

    w.close().unwrap();

    let segment_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join("00000000"))
        .unwrap();

    // Override the record length
    let mut buf = [0u8; 3];
    buf[0] = RecType::FULL.0;
    buf[1..3].copy_from_slice(&0xFFFFu16.to_be_bytes());
    segment_file.write_all_at(&buf, 0).unwrap();

    drop(segment_file);

    // Try and LiveReader it.
    let (m, _) = segments(w.dir()).unwrap();

    let seg = open_read_segment(&segment_name(&dir, m)).unwrap();

    let mut r = LiveReader::new(Some(LiveReaderMetrics::new()), seg);
    assert!(!r.next(), "expected no records");
    assert_eq!(
        r.err().map(|e| e.to_string()).as_deref(),
        Some("record length greater than a single page: 65542 > 32768"),
        "expected error, got: {:?}",
        r.err()
    );
}

#[test]
fn test_reader_data() {
    let dir = std::env::var("WALDIR").unwrap_or_default();
    if dir.is_empty() {
        return;
    }

    for (name, f) in reader_constructors() {
        let w = WL::new(&dir, Type::Snappy).unwrap();

        let sr = all_segments(Path::new(&dir));

        let mut reader = f(sr);
        while reader.next() {}
        assert!(reader.err().is_none(), "{name}: {:?}", reader.err());

        // Go calls w.Repair(reader.Err()) with a nil error here and requires no error.
        let err = reader.err().unwrap_or(Error::msg("nil"));
        w.repair(&err).unwrap();
    }
}
