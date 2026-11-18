//! Port of `tsdb/tombstones`: tombstone file format, MemTombstones and intervals.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;
use std::sync::RwLock;

use crate::SeriesRef;
use crate::encoding::{self, Crc32, Decbuf, put_uvarint, put_varint};
use crate::fileutil;
use crate::record::Logger;

/// Go `TombstonesFilename`.
pub const TOMBSTONES_FILENAME: &str = "tombstones";

/// Go `MagicTombstone`: 4 bytes at the head of a tombstone file.
pub const MAGIC_TOMBSTONE: u32 = 0x0130_BA30;

const TOMBSTONE_FORMAT_V1: u8 = 1;
const TOMBSTONE_FORMAT_VERSION_SIZE: usize = 1;
const TOMBSTONES_HEADER_SIZE: usize = 5;
const TOMBSTONES_CRC_SIZE: usize = 4;

/// Errors of this package. `Display` is Go's text (I/O errors use Rust's OS error text).
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    /// A bare `tsdb/encoding` error.
    Encoding(encoding::Error),
    /// Go `fmt.Errorf("<prefix>: %w", err)`.
    Wrapped(&'static str, Box<Error>),
    /// Go `errors.New` / `fmt.Errorf` without `%w`.
    Msg(String),
}

impl Error {
    /// Go `errors.Is(err, e)` for an encoding sentinel.
    pub fn is(&self, e: &encoding::Error) -> bool {
        match self {
            Error::Encoding(x) => x == e,
            Error::Wrapped(_, inner) => inner.is(e),
            _ => false,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "{e}"),
            Error::Encoding(e) => write!(f, "{e}"),
            Error::Wrapped(p, e) => write!(f, "{p}: {e}"),
            Error::Msg(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::Io(e)
    }
}

/// Go `Reader`: gives access to tombstone intervals by series reference.
pub trait Reader: Send + Sync {
    /// Returns deletion intervals for the series with the given reference (a copy).
    fn get(&self, r: SeriesRef) -> Result<Intervals, Error>;

    /// Calls `f` for each encountered interval set.
    fn iter(&self, f: &mut dyn FnMut(SeriesRef, &Intervals) -> Result<(), Error>) -> Result<(), Error>;

    /// Returns the total count of tombstones.
    fn total(&self) -> u64;

    /// Closes any underlying resources.
    fn close(&self) -> Result<(), Error>;
}

/// Go `WriteFile`: writes the tombstones of `tr` to `dir/tombstones` (through a `.tmp` file, synced and then
/// renamed) and returns the file size.
pub fn write_file(logger: &Logger, dir: &Path, tr: &dyn Reader) -> Result<i64, Error> {
    let path = dir.join(TOMBSTONES_FILENAME);
    let mut tmp = path.clone().into_os_string();
    tmp.push(".tmp");
    let tmp = std::path::PathBuf::from(tmp);

    let r = write_tmp(&tmp, tr);
    let r = match r {
        Ok(size) => fileutil::replace(&tmp, &path).map(|()| size).map_err(Error::Io),
        Err(e) => Err(e),
    };
    // Go removes the tmp file in a deferred call (a no-op after the rename).
    if let Err(e) = fs::remove_file(&tmp) {
        if e.kind() != io::ErrorKind::NotFound {
            logger.error("remove tmp file", &[("err", &e)]);
        }
    }
    r
}

fn write_tmp(tmp: &Path, tr: &dyn Reader) -> Result<i64, Error> {
    let mut f = File::create(tmp)?;
    let mut size = 0usize;

    // Write the meta.
    f.write_all(&MAGIC_TOMBSTONE.to_be_bytes())?;
    size += 4;

    let bytes = encode(tr).map_err(|e| Error::Wrapped("encoding tombstones", Box::new(e)))?;

    // Ignore first byte which is the format type. We do this for compatibility.
    let mut hash = Crc32::new();
    hash.write(&bytes[TOMBSTONE_FORMAT_VERSION_SIZE..]);

    f.write_all(&bytes).map_err(|e| Error::Wrapped("writing tombstones", Box::new(Error::Io(e))))?;
    size += bytes.len();

    let mut sum = Vec::with_capacity(4);
    hash.sum(&mut sum);
    f.write_all(&sum)?;
    size += sum.len();

    f.sync_all()?;
    drop(f);
    Ok(size as i64)
}

/// Go `Encode`: encodes the tombstones from the reader, without magic number or checksum.
pub fn encode(tr: &dyn Reader) -> Result<Vec<u8>, Error> {
    let mut buf = vec![TOMBSTONE_FORMAT_V1];
    tr.iter(&mut |r, ivs| {
        for iv in ivs {
            put_uvarint(&mut buf, r);
            put_varint(&mut buf, iv.mint);
            put_varint(&mut buf, iv.maxt);
        }
        Ok(())
    })?;
    Ok(buf)
}

/// Go `Decode`: decodes tombstones encoded with [`encode`].
pub fn decode(b: &[u8]) -> Result<MemTombstones, Error> {
    let mut d = Decbuf::new(b);
    let flag = d.byte();
    if flag != TOMBSTONE_FORMAT_V1 {
        return Err(Error::Msg(format!("invalid tombstone format {flag:x}")));
    }
    if let Some(e) = d.e {
        return Err(Error::Encoding(e));
    }

    let stones = MemTombstones::new();
    {
        let mut m = stones.write();
        while !d.b.is_empty() {
            let k = d.uvarint64();
            let mint = d.varint64();
            let maxt = d.varint64();
            if let Some(e) = d.e {
                return Err(Error::Encoding(e));
            }
            m.entry(k).or_default().add(Interval { mint, maxt });
        }
    }
    Ok(stones)
}

/// Go `Stone`: the series and time ranges that are deleted.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Stone {
    pub r#ref: SeriesRef,
    pub intervals: Intervals,
}

/// Go `ReadTombstones`: reads `dir/tombstones`; a missing file gives empty tombstones. Returns the tombstones
/// and the file size.
pub fn read_tombstones(dir: &Path) -> Result<(MemTombstones, i64), Error> {
    let b = match fs::read(dir.join(TOMBSTONES_FILENAME)) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((MemTombstones::new(), 0)),
        Err(e) => return Err(Error::Io(e)),
    };

    if b.len() < TOMBSTONES_HEADER_SIZE {
        return Err(Error::Wrapped("tombstones header", Box::new(Error::Encoding(encoding::Error::InvalidSize))));
    }

    let mut d = Decbuf::new(&b[..b.len() - TOMBSTONES_CRC_SIZE]);
    let mg = d.be32();
    if mg != MAGIC_TOMBSTONE {
        return Err(Error::Msg(format!("invalid magic number {mg:x}")));
    }

    // Verify checksum. Ignore first byte which is the format type.
    let mut hash = Crc32::new();
    hash.write(&d.get()[TOMBSTONE_FORMAT_VERSION_SIZE..]);
    let t = &b[b.len() - TOMBSTONES_CRC_SIZE..];
    if u32::from_be_bytes([t[0], t[1], t[2], t[3]]) != hash.sum32() {
        return Err(Error::Msg("checksum did not match".to_string()));
    }

    if let Some(e) = d.e {
        return Err(Error::Encoding(e));
    }

    let stones = decode(d.get())?;
    Ok((stones, b.len() as i64))
}

/// Go `MemTombstones`: in-memory tombstones that allow adding new intervals. Safe for concurrent use.
/// Iteration is in series reference order (Go iterates a map in random order).
#[derive(Debug, Default)]
pub struct MemTombstones {
    intvl_groups: RwLock<BTreeMap<SeriesRef, Intervals>>,
}

impl MemTombstones {
    /// Go `NewMemTombstones`.
    pub fn new() -> MemTombstones {
        MemTombstones::default()
    }

    /// Go `NewTestMemTombstones`: series `i + 1` gets `intervals[i]`.
    pub fn new_test_mem_tombstones(intervals: &[Intervals]) -> MemTombstones {
        let ret = MemTombstones::new();
        for (i, group) in intervals.iter().enumerate() {
            for &iv in group {
                ret.add_interval((i + 1) as SeriesRef, &[iv]);
            }
        }
        ret
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<SeriesRef, Intervals>> {
        self.intvl_groups.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<SeriesRef, Intervals>> {
        self.intvl_groups.write().unwrap_or_else(|e| e.into_inner())
    }

    /// Go `Get`: a copy of the intervals of `r` (empty if none).
    pub fn get(&self, r: SeriesRef) -> Intervals {
        self.read().get(&r).cloned().unwrap_or_default()
    }

    /// Go `DeleteTombstones`.
    pub fn delete_tombstones<'a>(&self, refs: impl IntoIterator<Item = &'a SeriesRef>) {
        let mut m = self.write();
        for r in refs {
            m.remove(r);
        }
    }

    /// Go `TruncateBefore`: drops intervals that end before `before_t`.
    pub fn truncate_before(&self, before_t: i64) {
        let mut m = self.write();
        m.retain(|_, ivs| {
            // Index of the first interval to keep.
            let mut i = ivs.len();
            while i > 0 && before_t <= ivs[i - 1].maxt {
                i -= 1;
            }
            if i == ivs.len() {
                return false;
            }
            if i > 0 {
                // Go copies into a new slice of exact size.
                *ivs = ivs[i..].to_vec();
            }
            true
        });
    }

    /// Go `Iter`.
    pub fn iter(&self, f: &mut dyn FnMut(SeriesRef, &Intervals) -> Result<(), Error>) -> Result<(), Error> {
        let m = self.read();
        for (&r, ivs) in m.iter() {
            f(r, ivs)?;
        }
        Ok(())
    }

    /// Go `Total`.
    pub fn total(&self) -> u64 {
        self.read().values().map(|ivs| ivs.len() as u64).sum()
    }

    /// Go `AddInterval`: adds intervals to the series `r`.
    pub fn add_interval(&self, r: SeriesRef, itvs: &[Interval]) {
        let mut m = self.write();
        let e = m.entry(r).or_default();
        for &itv in itvs {
            e.add(itv);
        }
    }
}

impl PartialEq for MemTombstones {
    fn eq(&self, o: &MemTombstones) -> bool {
        if std::ptr::eq(self, o) {
            return true;
        }
        *self.read() == *o.read()
    }
}

impl Reader for MemTombstones {
    fn get(&self, r: SeriesRef) -> Result<Intervals, Error> {
        Ok(MemTombstones::get(self, r))
    }
    fn iter(&self, f: &mut dyn FnMut(SeriesRef, &Intervals) -> Result<(), Error>) -> Result<(), Error> {
        MemTombstones::iter(self, f)
    }
    fn total(&self) -> u64 {
        MemTombstones::total(self)
    }
    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// Go `Interval`: a single closed time interval `[mint, maxt]`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Interval {
    pub mint: i64,
    pub maxt: i64,
}

impl Interval {
    pub const fn new(mint: i64, maxt: i64) -> Interval {
        Interval { mint, maxt }
    }

    /// Go `InBounds`.
    #[inline]
    pub fn in_bounds(&self, t: i64) -> bool {
        t >= self.mint && t <= self.maxt
    }

    /// Go `IsSubrange`.
    pub fn is_subrange(&self, dranges: &[Interval]) -> bool {
        dranges.iter().any(|r| r.in_bounds(self.mint) && r.in_bounds(self.maxt))
    }
}

/// Go `Intervals`: a set of increasing and non-overlapping time intervals.
pub type Intervals = Vec<Interval>;

/// Methods of Go `Intervals`.
pub trait IntervalsExt {
    /// Go `Intervals.Add`: adds the new time range to the existing (sorted) ones, merging overlapping and
    /// adjacent intervals. Changes the intervals in place (Go returns the new slice, which may alias).
    fn add(&mut self, n: Interval);
}

impl IntervalsExt for Intervals {
    fn add(&mut self, n: Interval) {
        let len = self.len();
        if len == 0 {
            self.push(n);
            return;
        }
        // Find min and max indexes of intervals that overlap with the new interval. Intervals are closed
        // [t1, t2] and t is discrete, so if neighbour intervals are 1 step difference to the new one, we can
        // merge those together.
        let mut mini = 0;
        if n.mint != i64::MIN {
            // Avoid overflow.
            mini = self.partition_point(|iv| iv.maxt < n.mint - 1);
            if mini == len {
                self.push(n);
                return;
            }
        }

        // Go starts from len(in) here, which indexes past the end when mini > 0 (a panic in Go); the number of
        // intervals from mini on is the correct value.
        let mut maxi = len - mini;
        if n.maxt != i64::MAX {
            // Avoid overflow.
            maxi = self[mini..].partition_point(|iv| iv.mint <= n.maxt + 1);
            if maxi == 0 {
                self.insert(mini, n);
                return;
            }
        }

        if n.mint < self[mini].mint {
            self[mini].mint = n.mint;
        }
        self[mini].maxt = n.maxt.max(self[maxi + mini - 1].maxt);
        self.drain(mini + 1..maxi + mini);
    }
}
