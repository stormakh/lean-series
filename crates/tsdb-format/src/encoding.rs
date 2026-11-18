//! Port of `tsdb/encoding`: `Encbuf` and `Decbuf` (big endian integers, varints, CRC32 Castagnoli) and an
//! in-house CRC32C (slicing-by-8, same results as Go's `crc32.MakeTable(crc32.Castagnoli)`).

use std::fmt;

/// Go `binary.MaxVarintLen64`.
pub const MAX_VARINT_LEN64: usize = 10;
/// Go `binary.MaxVarintLen32`.
pub const MAX_VARINT_LEN32: usize = 5;

/// Errors of the encoding package. The `Display` text is Go's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Go `ErrInvalidSize`: "invalid size".
    InvalidSize,
    /// Go `ErrInvalidChecksum`: "invalid checksum".
    InvalidChecksum,
    /// Go `fmt.Errorf("invalid uvarint %d", n)`.
    InvalidUvarint(i64),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::InvalidSize => f.write_str("invalid size"),
            Error::InvalidChecksum => f.write_str("invalid checksum"),
            Error::InvalidUvarint(n) => write!(f, "invalid uvarint {n}"),
        }
    }
}

impl std::error::Error for Error {}

// ---------------------------------------------------------------------------------------------------------------
// CRC32 Castagnoli.

const CASTAGNOLI_REFLECTED: u32 = 0x82F6_3B78;

const fn make_tables() -> [[u32; 256]; 8] {
    let mut t = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ CASTAGNOLI_REFLECTED
            } else {
                crc >> 1
            };
            j += 1;
        }
        t[0][i] = crc;
        i += 1;
    }
    let mut i = 0;
    while i < 256 {
        let mut crc = t[0][i];
        let mut k = 1;
        while k < 8 {
            crc = t[0][(crc & 0xff) as usize] ^ (crc >> 8);
            t[k][i] = crc;
            k += 1;
        }
        i += 1;
    }
    t
}

static TABLES: [[u32; 256]; 8] = make_tables();

/// Updates a CRC32C (Castagnoli) checksum with `p` (Go `crc32.Update(crc, castagnoliTable, p)`).
pub fn crc32c_update(crc: u32, p: &[u8]) -> u32 {
    let t = &TABLES;
    let mut crc = !crc;
    let mut chunks = p.chunks_exact(8);
    for c in &mut chunks {
        let lo = crc ^ u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        let hi = u32::from_le_bytes([c[4], c[5], c[6], c[7]]);
        crc = t[7][(lo & 0xff) as usize]
            ^ t[6][((lo >> 8) & 0xff) as usize]
            ^ t[5][((lo >> 16) & 0xff) as usize]
            ^ t[4][(lo >> 24) as usize]
            ^ t[3][(hi & 0xff) as usize]
            ^ t[2][((hi >> 8) & 0xff) as usize]
            ^ t[1][((hi >> 16) & 0xff) as usize]
            ^ t[0][(hi >> 24) as usize];
    }
    for &b in chunks.remainder() {
        crc = t[0][((crc as u8) ^ b) as usize] ^ (crc >> 8);
    }
    !crc
}

/// The CRC32C (Castagnoli) checksum of `p` (Go `crc32.Checksum(p, castagnoliTable)`).
pub fn crc32c(p: &[u8]) -> u32 {
    crc32c_update(0, p)
}

/// A streaming CRC32C hasher (Go `crc32.New(castagnoliTable)`, a `hash.Hash32`).
#[derive(Clone, Copy, Debug, Default)]
pub struct Crc32 {
    crc: u32,
}

impl Crc32 {
    pub fn new() -> Crc32 {
        Crc32 { crc: 0 }
    }
    pub fn reset(&mut self) {
        self.crc = 0;
    }
    pub fn write(&mut self, p: &[u8]) {
        self.crc = crc32c_update(self.crc, p);
    }
    /// Go `Sum32`.
    pub fn sum32(&self) -> u32 {
        self.crc
    }
    /// Go `Sum(b)`: appends the big endian checksum to `b`.
    pub fn sum(&self, b: &mut Vec<u8>) {
        b.extend_from_slice(&self.crc.to_be_bytes());
    }
}

impl std::io::Write for Crc32 {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Crc32::write(self, buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Varints (Go encoding/binary).

/// Appends `x` as a uvarint (Go `binary.AppendUvarint`).
#[inline]
pub fn put_uvarint(b: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        b.push(x as u8 | 0x80);
        x >>= 7;
    }
    b.push(x as u8);
}

/// Appends `x` as a zig-zag varint (Go `binary.AppendVarint`).
#[inline]
pub fn put_varint(b: &mut Vec<u8>, x: i64) {
    let ux = ((x as u64) << 1) ^ ((x >> 63) as u64);
    put_uvarint(b, ux);
}

/// Go `binary.Uvarint` (same as `github.com/dennwc/varint.Uvarint`): returns the value and the number of bytes
/// read; `n == 0` if the buffer is too small, `n < 0` on overflow (`-n` bytes read).
#[inline]
pub fn uvarint(buf: &[u8]) -> (u64, isize) {
    let mut x: u64 = 0;
    let mut s: u32 = 0;
    for (i, &b) in buf.iter().enumerate() {
        if i == MAX_VARINT_LEN64 {
            return (0, -((i + 1) as isize)); // overflow
        }
        if b < 0x80 {
            if i == MAX_VARINT_LEN64 - 1 && b > 1 {
                return (0, -((i + 1) as isize)); // overflow
            }
            return (x | (b as u64) << s, (i + 1) as isize);
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;
    }
    (0, 0)
}

/// Go `binary.Varint`.
#[inline]
pub fn varint(buf: &[u8]) -> (i64, isize) {
    let (ux, n) = uvarint(buf);
    let mut x = (ux >> 1) as i64;
    if ux & 1 != 0 {
        x = !x;
    }
    (x, n)
}

/// The number of bytes `x` takes as a uvarint.
#[inline]
pub fn uvarint_size(mut x: u64) -> usize {
    let mut n = 1;
    while x >= 0x80 {
        x >>= 7;
        n += 1;
    }
    n
}

// ---------------------------------------------------------------------------------------------------------------
// Encbuf.

/// Go `Encbuf`: populates a byte slice with various types.
#[derive(Clone, Debug, Default)]
pub struct Encbuf {
    pub b: Vec<u8>,
}

impl Encbuf {
    pub fn new() -> Encbuf {
        Encbuf { b: Vec::new() }
    }
    pub fn with_capacity(n: usize) -> Encbuf {
        Encbuf {
            b: Vec::with_capacity(n),
        }
    }
    #[inline]
    pub fn reset(&mut self) {
        self.b.clear();
    }
    #[inline]
    pub fn get(&self) -> &[u8] {
        &self.b
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.b.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.b.is_empty()
    }
    #[inline]
    pub fn put_string(&mut self, s: &str) {
        self.b.extend_from_slice(s.as_bytes());
    }
    #[inline]
    pub fn put_byte(&mut self, c: u8) {
        self.b.push(c);
    }
    #[inline]
    pub fn put_bytes(&mut self, b: &[u8]) {
        self.b.extend_from_slice(b);
    }
    #[inline]
    pub fn put_be32int(&mut self, x: i64) {
        self.put_be32(x as u32);
    }
    #[inline]
    pub fn put_uvarint32(&mut self, x: u32) {
        self.put_uvarint64(x as u64);
    }
    #[inline]
    pub fn put_be64int64(&mut self, x: i64) {
        self.put_be64(x as u64);
    }
    /// Go `PutUvarint(x int)`: negative values wrap to `uint64` as in Go.
    #[inline]
    pub fn put_uvarint(&mut self, x: i64) {
        self.put_uvarint64(x as u64);
    }
    #[inline]
    pub fn put_be32(&mut self, x: u32) {
        self.b.extend_from_slice(&x.to_be_bytes());
    }
    #[inline]
    pub fn put_be64(&mut self, x: u64) {
        self.b.extend_from_slice(&x.to_be_bytes());
    }
    #[inline]
    pub fn put_be_float64(&mut self, x: f64) {
        self.put_be64(x.to_bits());
    }
    #[inline]
    pub fn put_uvarint64(&mut self, x: u64) {
        put_uvarint(&mut self.b, x);
    }
    #[inline]
    pub fn put_varint64(&mut self, x: i64) {
        put_varint(&mut self.b, x);
    }
    /// Writes a string prefixed by its uvarint length in bytes.
    #[inline]
    pub fn put_uvarint_str(&mut self, s: &str) {
        self.put_uvarint64(s.len() as u64);
        self.put_string(s);
    }
    /// Writes a byte slice prefixed by its uvarint length.
    #[inline]
    pub fn put_uvarint_bytes(&mut self, b: &[u8]) {
        self.put_uvarint64(b.len() as u64);
        self.put_bytes(b);
    }
    /// Go `PutHash`: appends the CRC32C of the current contents (big endian).
    pub fn put_hash(&mut self, h: &mut Crc32) {
        h.reset();
        self.write_to_hash(h);
        self.put_hash_sum(h);
    }
    /// Go `WriteToHash`.
    pub fn write_to_hash(&self, h: &mut Crc32) {
        h.write(&self.b);
    }
    /// Go `PutHashSum`.
    pub fn put_hash_sum(&mut self, h: &Crc32) {
        h.sum(&mut self.b);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// ByteSlice and Decbuf.

/// Go `encoding.ByteSlice`: an abstract byte slice (a plain slice or an mmapped file).
pub trait ByteSlice {
    fn len(&self) -> usize;
    fn range(&self, start: usize, end: usize) -> &[u8];
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl ByteSlice for [u8] {
    fn len(&self) -> usize {
        <[u8]>::len(self)
    }
    fn range(&self, start: usize, end: usize) -> &[u8] {
        &self[start..end]
    }
}

impl ByteSlice for Vec<u8> {
    fn len(&self) -> usize {
        Vec::len(self)
    }
    fn range(&self, start: usize, end: usize) -> &[u8] {
        &self[start..end]
    }
}

impl<T: ByteSlice + ?Sized> ByteSlice for &T {
    fn len(&self) -> usize {
        (**self).len()
    }
    fn range(&self, start: usize, end: usize) -> &[u8] {
        (**self).range(start, end)
    }
}

/// Go `Decbuf`: safe methods to extract data from a byte slice, with bounds checks. Several values can be read
/// without checking for errors; check `err()` before using any of them.
#[derive(Clone, Debug, Default)]
pub struct Decbuf<'a> {
    pub b: &'a [u8],
    pub e: Option<Error>,
}

impl<'a> Decbuf<'a> {
    /// A decoding buffer over `b`.
    #[inline]
    pub fn new(b: &'a [u8]) -> Decbuf<'a> {
        Decbuf { b, e: None }
    }

    /// A decoding buffer in the error state `e`.
    #[inline]
    pub fn with_err(e: Error) -> Decbuf<'a> {
        Decbuf { b: &[], e: Some(e) }
    }

    /// Go `NewDecbufAt`: expects 4 bytes of big endian length at `off`, then the contents and a CRC32C.
    /// `check_crc == false` is Go's nil table (no check).
    pub fn new_at<B: ByteSlice + ?Sized>(bs: &'a B, off: usize, check_crc: bool) -> Decbuf<'a> {
        if bs.len() < off + 4 {
            return Decbuf::with_err(Error::InvalidSize);
        }
        let b = bs.range(off, off + 4);
        let l = u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize;
        if bs.len() < off + 4 + l + 4 {
            return Decbuf::with_err(Error::InvalidSize);
        }
        let b = bs.range(off + 4, off + 4 + l + 4);
        let dec = Decbuf::new(&b[..b.len() - 4]);
        if check_crc {
            let t = &b[b.len() - 4..];
            let exp = u32::from_be_bytes([t[0], t[1], t[2], t[3]]);
            if dec.crc32() != exp {
                return Decbuf::with_err(Error::InvalidChecksum);
            }
        }
        dec
    }

    /// Go `NewDecbufUvarintAt`: expects a uvarint length at `off`, then the contents and a CRC32C.
    pub fn new_uvarint_at<B: ByteSlice + ?Sized>(bs: &'a B, off: usize) -> Decbuf<'a> {
        // We never have to access this method at the far end of the byte slice. Thus just checking against the
        // MaxVarintLen32 is sufficient.
        if bs.len() < off + MAX_VARINT_LEN32 {
            return Decbuf::with_err(Error::InvalidSize);
        }
        let b = bs.range(off, off + MAX_VARINT_LEN32);
        let (l, n) = uvarint(b);
        if n <= 0 || n > MAX_VARINT_LEN32 as isize {
            return Decbuf::with_err(Error::InvalidUvarint(n as i64));
        }
        let n = n as usize;
        let l = l as usize;
        if bs.len() < off + n + l + 4 {
            return Decbuf::with_err(Error::InvalidSize);
        }
        let b = bs.range(off + n, off + n + l + 4);
        let dec = Decbuf::new(&b[..b.len() - 4]);
        let t = &b[b.len() - 4..];
        if dec.crc32() != u32::from_be_bytes([t[0], t[1], t[2], t[3]]) {
            return Decbuf::with_err(Error::InvalidChecksum);
        }
        dec
    }

    /// Go `NewDecbufRaw`: a decoding buffer of the first `length` bytes.
    pub fn new_raw<B: ByteSlice + ?Sized>(bs: &'a B, length: usize) -> Decbuf<'a> {
        if bs.len() < length {
            return Decbuf::with_err(Error::InvalidSize);
        }
        Decbuf::new(bs.range(0, length))
    }

    /// Go `Uvarint() int`.
    #[inline]
    pub fn uvarint(&mut self) -> i64 {
        self.uvarint64() as i64
    }
    #[inline]
    pub fn uvarint32(&mut self) -> u32 {
        self.uvarint64() as u32
    }
    /// Go `Be32int() int`.
    #[inline]
    pub fn be32int(&mut self) -> i64 {
        self.be32() as i64
    }
    #[inline]
    pub fn be64int64(&mut self) -> i64 {
        self.be64() as i64
    }

    /// The CRC32C over the remaining bytes.
    #[inline]
    pub fn crc32(&self) -> u32 {
        crc32c(self.b)
    }

    pub fn skip(&mut self, l: usize) {
        if self.b.len() < l {
            self.e = Some(Error::InvalidSize);
            return;
        }
        self.b = &self.b[l..];
    }

    /// Go `UvarintStr`. Invalid UTF-8 is replaced lossily only if present; label data written by Prometheus is
    /// valid UTF-8. Use [`Decbuf::uvarint_bytes`] to avoid the allocation.
    pub fn uvarint_str(&mut self) -> String {
        String::from_utf8_lossy(self.uvarint_bytes()).into_owned()
    }

    /// Go `UvarintBytes`: a slice into the underlying data (no copy).
    #[inline]
    pub fn uvarint_bytes(&mut self) -> &'a [u8] {
        let l = self.uvarint64();
        if self.e.is_some() {
            return &[];
        }
        if (self.b.len() as u64) < l {
            self.e = Some(Error::InvalidSize);
            return &[];
        }
        let (s, rest) = self.b.split_at(l as usize);
        self.b = rest;
        s
    }

    #[inline]
    pub fn varint64(&mut self) -> i64 {
        if self.e.is_some() {
            return 0;
        }
        // Decode as unsigned first, since that's what the varint library implements.
        let (ux, n) = uvarint(self.b);
        if n < 1 {
            self.e = Some(Error::InvalidSize);
            return 0;
        }
        let mut x = (ux >> 1) as i64;
        if ux & 1 != 0 {
            x = !x;
        }
        self.b = &self.b[n as usize..];
        x
    }

    #[inline]
    pub fn uvarint64(&mut self) -> u64 {
        if self.e.is_some() {
            return 0;
        }
        let (x, n) = uvarint(self.b);
        if n < 1 {
            self.e = Some(Error::InvalidSize);
            return 0;
        }
        self.b = &self.b[n as usize..];
        x
    }

    #[inline]
    pub fn be64(&mut self) -> u64 {
        if self.e.is_some() {
            return 0;
        }
        if self.b.len() < 8 {
            self.e = Some(Error::InvalidSize);
            return 0;
        }
        let x = u64::from_be_bytes(self.b[..8].try_into().unwrap());
        self.b = &self.b[8..];
        x
    }

    #[inline]
    pub fn be64_float64(&mut self) -> f64 {
        f64::from_bits(self.be64())
    }

    #[inline]
    pub fn be32(&mut self) -> u32 {
        if self.e.is_some() {
            return 0;
        }
        if self.b.len() < 4 {
            self.e = Some(Error::InvalidSize);
            return 0;
        }
        let x = u32::from_be_bytes(self.b[..4].try_into().unwrap());
        self.b = &self.b[4..];
        x
    }

    #[inline]
    pub fn byte(&mut self) -> u8 {
        if self.e.is_some() {
            return 0;
        }
        if self.b.is_empty() {
            self.e = Some(Error::InvalidSize);
            return 0;
        }
        let x = self.b[0];
        self.b = &self.b[1..];
        x
    }

    pub fn consume_padding(&mut self) {
        if self.e.is_some() {
            return;
        }
        while self.b.len() > 1 && self.b[0] == 0 {
            self.b = &self.b[1..];
        }
        if self.b.is_empty() {
            self.e = Some(Error::InvalidSize);
        }
    }

    #[inline]
    pub fn err(&self) -> Option<&Error> {
        self.e.as_ref()
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.b.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.b.is_empty()
    }
    #[inline]
    pub fn get(&self) -> &'a [u8] {
        self.b
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_known_values() {
        // RFC 3720 / iSCSI test vectors.
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
        assert_eq!(crc32c(&[0xffu8; 32]), 0x62A8_AB43);
        let inc: Vec<u8> = (0..32).collect();
        assert_eq!(crc32c(&inc), 0x46DD_794E);
        // Streaming equals one shot at every split point.
        let data: Vec<u8> = (0..1000u32).map(|i| (i * 7 + 3) as u8).collect();
        for split in 0..data.len() {
            assert_eq!(
                crc32c_update(crc32c(&data[..split]), &data[split..]),
                crc32c(&data)
            );
        }
    }

    #[test]
    fn varints_match_go() {
        let mut b = Vec::new();
        for x in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            b.clear();
            put_uvarint(&mut b, x);
            assert_eq!(b.len(), uvarint_size(x));
            assert_eq!(uvarint(&b), (x, b.len() as isize));
        }
        for x in [0i64, -1, 1, -64, 64, i64::MIN, i64::MAX] {
            b.clear();
            put_varint(&mut b, x);
            assert_eq!(varint(&b), (x, b.len() as isize));
        }
        assert_eq!(uvarint(&[0x80]), (0, 0));
        assert_eq!(uvarint(&[0xff; 11]), (0, -11));
        assert_eq!(
            uvarint(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02]),
            (0, -10)
        );
    }
}
