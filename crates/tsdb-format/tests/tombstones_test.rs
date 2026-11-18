//! Port of `tsdb/tombstones/tombstones_test.go`.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use tsdb_format::record::Logger;
use tsdb_format::tombstones::{Interval, Intervals, IntervalsExt, MemTombstones, read_tombstones, write_file};

// adapted: Go uses math/rand; a small xorshift seeded from the clock gives the same kind of arbitrary input.
struct Rng(u64);

impl Rng {
    fn new() -> Rng {
        Rng(now_nanos() as u64 | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    /// Go `rand.Int63n(n)`.
    fn int63n(&mut self, n: i64) -> i64 {
        ((self.next() >> 1) % n as u64) as i64
    }
}

fn now_nanos() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64
}

fn temp_dir(name: &str) -> PathBuf {
    let d = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "tombstones-{name}-{}-{}",
        std::process::id(),
        now_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn test_write_and_readback_tombstones() {
    let tmpdir = temp_dir("readback");
    let mut rng = Rng::new();

    let mut r: u64 = 0;

    let stones = MemTombstones::new();
    // Generate the tombstones.
    for _ in 0..100 {
        r += rng.int63n(10) as u64 + 1;
        let num_ranges = rng.int63n(5) + 1;
        let mut dranges: Intervals = Vec::with_capacity(num_ranges as usize);
        let mut mint = rng.int63n(now_nanos());
        for _ in 0..num_ranges {
            dranges.add(Interval::new(mint, mint + rng.int63n(1000)));
            mint += rng.int63n(1000) + 1;
        }
        stones.add_interval(r, &dranges);
    }

    write_file(&Logger::nop(), &tmpdir, &stones).unwrap();

    let (restr, _) = read_tombstones(&tmpdir).unwrap();

    // Compare the two readers.
    assert_eq!(stones, restr);
    std::fs::remove_dir_all(&tmpdir).unwrap();
}

#[test]
fn test_deleting_tombstones() {
    let stones = MemTombstones::new();
    let mut rng = Rng::new();

    let r: u64 = 42;
    let mint = rng.int63n(now_nanos());
    let mut dranges: Intervals = Vec::with_capacity(1);
    dranges.add(Interval::new(mint, mint + rng.int63n(1000)));
    stones.add_interval(r, &dranges);
    stones.add_interval(43, &dranges);

    let intervals = stones.get(r);
    assert_eq!(intervals, dranges);

    stones.delete_tombstones(&HashSet::from([r]));

    let intervals = stones.get(r);
    assert!(intervals.is_empty());
}

#[test]
fn test_tombstones_get_with_copy() {
    let stones = MemTombstones::new();
    stones.add_interval(1, &[Interval::new(1, 2), Interval::new(7, 8), Interval::new(11, 12)]);

    let mut intervals0 = stones.get(1);
    assert_eq!(vec![Interval::new(1, 2), Interval::new(7, 8), Interval::new(11, 12)], intervals0);
    // adapted: Go's Add returns a slice that aliases intervals0 ("Original slice changed"); Rust's add changes
    // intervals0 in place, and intervals1 is its value afterwards.
    intervals0.add(Interval::new(4, 6));
    let intervals1 = intervals0.clone();
    assert_eq!(vec![Interval::new(1, 2), Interval::new(4, 8), Interval::new(11, 12)], intervals0); // Original slice changed.
    assert_eq!(vec![Interval::new(1, 2), Interval::new(4, 8), Interval::new(11, 12)], intervals1);

    let intervals2 = stones.get(1);
    assert_eq!(vec![Interval::new(1, 2), Interval::new(7, 8), Interval::new(11, 12)], intervals2);
}

#[test]
fn test_truncate_before() {
    let iv = Interval::new;
    let cases: Vec<(Intervals, i64, Intervals)> = vec![
        (vec![iv(1, 2), iv(4, 10), iv(12, 100)], 3, vec![iv(4, 10), iv(12, 100)]),
        (vec![iv(1, 2), iv(4, 10), iv(12, 100), iv(200, 1000)], 900, vec![iv(200, 1000)]),
        (vec![iv(1, 2), iv(4, 10), iv(12, 100), iv(200, 1000)], 2000, vec![]),
        (
            vec![iv(1, 2), iv(4, 10), iv(12, 100), iv(200, 1000)],
            0,
            vec![iv(1, 2), iv(4, 10), iv(12, 100), iv(200, 1000)],
        ),
    ];
    for (before, before_t, after) in cases {
        let r = 42;
        let stones = MemTombstones::new();
        stones.add_interval(r, &before);

        stones.truncate_before(before_t);
        let ts = stones.get(r);
        assert_eq!(after, ts);
    }
}

#[test]
fn test_adding_new_intervals() {
    let iv = Interval::new;
    let (min, max) = (i64::MIN, i64::MAX);
    let cases: Vec<(Intervals, Interval, Intervals)> = vec![
        (vec![], iv(1, 2), vec![iv(1, 2)]),
        (vec![iv(1, 2)], iv(1, 2), vec![iv(1, 2)]),
        (vec![iv(1, 4), iv(6, 6)], iv(5, 6), vec![iv(1, 6)]),
        (vec![iv(1, 10), iv(12, 20), iv(25, 30)], iv(21, 25), vec![iv(1, 10), iv(12, 30)]),
        (vec![iv(1, 10), iv(12, 20), iv(25, 30)], iv(22, 23), vec![iv(1, 10), iv(12, 20), iv(22, 23), iv(25, 30)]),
        (vec![iv(1, 2), iv(3, 5), iv(7, 7)], iv(6, 7), vec![iv(1, 2), iv(3, 7)]),
        (vec![iv(1, 10), iv(12, 20), iv(25, 30)], iv(18, 23), vec![iv(1, 10), iv(12, 23), iv(25, 30)]),
        (vec![iv(1, 10), iv(12, 20), iv(25, 30)], iv(9, 23), vec![iv(1, 23), iv(25, 30)]),
        (vec![iv(1, 10), iv(12, 20), iv(25, 30)], iv(9, 230), vec![iv(1, 230)]),
        (vec![iv(5, 10), iv(12, 20), iv(25, 30)], iv(1, 4), vec![iv(1, 10), iv(12, 20), iv(25, 30)]),
        (vec![iv(5, 10), iv(12, 20), iv(25, 30)], iv(11, 14), vec![iv(5, 20), iv(25, 30)]),
        (vec![iv(5, 10), iv(12, 20), iv(25, 30)], iv(1, 3), vec![iv(1, 3), iv(5, 10), iv(12, 20), iv(25, 30)]),
        (vec![iv(5, 10), iv(12, 20), iv(25, 30)], iv(35, 40), vec![iv(5, 10), iv(12, 20), iv(25, 30), iv(35, 40)]),
        (vec![], iv(min, 2), vec![iv(min, 2)]),
        (vec![iv(min, 2)], iv(9, max), vec![iv(min, 2), iv(9, max)]),
        (vec![iv(9, max)], iv(min, 2), vec![iv(min, 2), iv(9, max)]),
        (vec![iv(9, max)], iv(min, 10), vec![iv(min, max)]),
        (vec![iv(9, 10)], iv(min, 7), vec![iv(min, 7), iv(9, 10)]),
        (vec![iv(9, 10)], iv(12, max), vec![iv(9, 10), iv(12, max)]),
        (vec![iv(9, 10)], iv(min, 8), vec![iv(min, 10)]),
        (vec![iv(9, 10)], iv(11, max), vec![iv(9, max)]),
    ];

    for (mut exist, new, exp) in cases {
        let before = exist.clone();
        exist.add(new);
        assert_eq!(exp, exist, "exist={before:?} new={new:?}");
    }
}

// TestMemTombstonesConcurrency to make sure they are safe to access from different goroutines.
#[test]
fn test_mem_tombstones_concurrency() {
    let tomb = Arc::new(MemTombstones::new());
    let total_runs: u64 = 100;

    let t1 = {
        let tomb = tomb.clone();
        thread::spawn(move || {
            for x in 0..total_runs {
                tomb.add_interval(x, &[Interval::new(x as i64, x as i64)]);
            }
        })
    };
    let t2 = {
        let tomb = tomb.clone();
        thread::spawn(move || {
            for x in 0..total_runs {
                // adapted: Go checks that Get returns no error; MemTombstones::get cannot fail, so check through
                // the Reader trait instead.
                let r = tsdb_format::tombstones::Reader::get(&*tomb, x);
                assert!(r.is_ok());
            }
        })
    };
    t1.join().unwrap();
    t2.join().unwrap();
}
