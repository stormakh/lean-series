//! Bit stream writer and reader (port of `tsdb/chunkenc/bstream.go`).

use crate::Error;

/// Go `uint64(1)<<n - 1` with Go shift semantics (n >= 64 gives all ones).
#[inline(always)]
pub(crate) fn mask(n: u8) -> u64 {
    if n >= 64 { u64::MAX } else { (1u64 << n) - 1 }
}

/// Go `x >> n` for unsigned x (n >= 64 gives 0).
#[inline(always)]
pub(crate) fn shr(x: u64, n: u8) -> u64 {
    if n >= 64 { 0 } else { x >> n }
}

/// Go `x << n` for unsigned x (n >= 64 gives 0).
#[inline(always)]
pub(crate) fn shl(x: u64, n: u8) -> u64 {
    if n >= 64 { 0 } else { x << n }
}

/// A bit stream writer. `count` is how many right-most bits are available for writing in the last byte.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bstream {
    pub stream: Vec<u8>,
    pub count: u8,
}

impl Bstream {
    pub fn new(stream: Vec<u8>) -> Bstream {
        Bstream { stream, count: 0 }
    }

    pub fn reset(&mut self, stream: Vec<u8>) {
        self.stream = stream;
        self.count = 0;
    }

    /// Appends a byte. A full buffer grows by a quarter (at least 32 bytes) instead of doubling, so open head
    /// chunks keep little unused capacity; amortized growth stays linear in the chunk size.
    #[inline]
    fn push(&mut self, b: u8) {
        if self.stream.len() == self.stream.capacity() {
            self.stream.reserve_exact((self.stream.len() / 4).max(32));
        }
        self.stream.push(b);
    }

    #[inline]
    pub fn bytes(&self) -> &[u8] {
        &self.stream
    }

    #[inline]
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.stream
    }

    #[inline]
    pub fn write_bit(&mut self, bit: bool) {
        if self.count == 0 {
            self.push(0);
            self.count = 8;
        }
        let i = self.stream.len() - 1;
        if bit {
            self.stream[i] |= 1 << (self.count - 1);
        }
        self.count -= 1;
    }

    #[inline]
    pub fn write_byte(&mut self, byt: u8) {
        if self.count == 0 {
            self.push(byt);
            return;
        }
        let i = self.stream.len() - 1;
        // Complete the last byte with the leftmost count bits from byt.
        self.stream[i] |= byt >> (8 - self.count);
        // Write the remainder, if any.
        self.push(((byt as u16) << self.count) as u8);
    }

    #[inline]
    pub fn write_bits(&mut self, u: u64, nbits: i32) {
        let mut nbits = nbits;
        let mut u = shl(u, (64 - nbits) as u8);
        while nbits >= 8 {
            let byt = (u >> 56) as u8;
            self.write_byte(byt);
            u <<= 8;
            nbits -= 8;
        }
        while nbits > 0 {
            self.write_bit((u >> 63) == 1);
            u <<= 1;
            nbits -= 1;
        }
    }

    /// Like `write_bits` but writes whole bytes at once. Produces identical output.
    #[inline]
    pub fn write_bits_fast(&mut self, u: u64, nbits: i32) {
        let mut nbits = nbits;
        let mut u = shl(u, (64 - nbits) as u8);
        // If the last byte is partial, fill its remaining bits first.
        if self.count > 0 {
            let free = self.count as i32;
            let last = self.stream.len() - 1;
            self.stream[last] |= (u >> (64 - free)) as u8;
            if nbits < free {
                self.count = (free - nbits) as u8;
                return;
            }
            u = shl(u, free as u8);
            nbits -= free;
            self.count = 0;
        }
        while nbits >= 8 {
            self.push((u >> 56) as u8);
            u <<= 8;
            nbits -= 8;
        }
        if nbits > 0 {
            self.push((u >> 56) as u8);
            self.count = (8 - nbits) as u8;
        }
    }
}

/// A bit stream reader over a byte slice.
#[derive(Clone, Debug, Default)]
pub struct BstreamReader<'a> {
    pub(crate) stream: &'a [u8],
    /// The offset from which to read the next byte from the stream.
    pub(crate) stream_offset: usize,
    /// The current buffer, filled from the stream, containing up to 8 bytes from which bits are read.
    pub(crate) buffer: u64,
    /// The number of right-most bits valid to read (from left) in the current 8 byte buffer.
    pub(crate) valid: u8,
    /// A copy of the last byte of the stream.
    pub(crate) last: u8,
}

impl<'a> BstreamReader<'a> {
    pub fn new(b: &'a [u8]) -> BstreamReader<'a> {
        // The last byte of the stream can be updated later, so we take a copy.
        let last = b.last().copied().unwrap_or(0);
        BstreamReader { stream: b, stream_offset: 0, buffer: 0, valid: 0, last }
    }

    /// The number of valid bits left in the current buffer.
    #[inline]
    pub fn valid(&self) -> u8 {
        self.valid
    }

    #[inline]
    pub fn read_bit(&mut self) -> Option<bool> {
        if self.valid == 0 && !self.load_next_buffer(1) {
            return None;
        }
        self.read_bit_fast()
    }

    /// Reads a bit from the buffer only; None if the buffer is empty.
    #[inline(always)]
    pub fn read_bit_fast(&mut self) -> Option<bool> {
        if self.valid == 0 {
            return None;
        }
        self.valid -= 1;
        Some((self.buffer & (1u64 << self.valid)) != 0)
    }

    /// `read_bit_fast` falling back to `read_bit`.
    #[inline(always)]
    pub fn read_bit_any(&mut self) -> Option<bool> {
        match self.read_bit_fast() {
            Some(b) => Some(b),
            None => self.read_bit(),
        }
    }

    #[inline]
    pub fn read_bits(&mut self, nbits: u8) -> Option<u64> {
        let mut nbits = nbits;
        if self.valid == 0 && !self.load_next_buffer(nbits) {
            return None;
        }
        if nbits <= self.valid {
            return self.read_bits_fast(nbits);
        }
        // Read all remaining valid bits from the current buffer and a part from the next one.
        let bitmask = mask(self.valid);
        nbits -= self.valid;
        let mut v = shl(self.buffer & bitmask, nbits);
        self.valid = 0;
        if !self.load_next_buffer(nbits) {
            return None;
        }
        let bitmask = mask(nbits);
        v |= shr(self.buffer, self.valid.wrapping_sub(nbits)) & bitmask;
        self.valid = self.valid.wrapping_sub(nbits);
        Some(v)
    }

    #[inline(always)]
    pub fn read_bits_fast(&mut self, nbits: u8) -> Option<u64> {
        if nbits > self.valid {
            return None;
        }
        let bitmask = mask(nbits);
        self.valid -= nbits;
        Some(shr(self.buffer, self.valid) & bitmask)
    }

    /// `read_bits_fast` falling back to `read_bits`.
    #[inline(always)]
    pub fn read_bits_any(&mut self, nbits: u8) -> Option<u64> {
        match self.read_bits_fast(nbits) {
            Some(v) => Some(v),
            None => self.read_bits(nbits),
        }
    }

    #[inline]
    pub fn read_byte(&mut self) -> Option<u8> {
        self.read_bits(8).map(|v| v as u8)
    }

    /// Reads the XOR2 control prefix from the buffer only (cases 0-3). None if not decidable from the buffer.
    #[inline(always)]
    pub fn read_xor2_control_fast(&mut self) -> Option<u8> {
        if self.valid < 4 {
            return None;
        }
        let top4 = ((self.buffer >> (self.valid - 4)) & 0xf) as u8;
        if top4 < 8 {
            self.valid -= 1;
            return Some(0);
        }
        if top4 < 12 {
            self.valid -= 2;
            return Some(1);
        }
        if top4 < 14 {
            self.valid -= 3;
            return Some(2);
        }
        if top4 == 14 {
            self.valid -= 4;
            return Some(3);
        }
        None
    }

    /// Reads the XOR2 control prefix (cases 0-5).
    pub fn read_xor2_control(&mut self) -> Option<u8> {
        if self.valid >= 4 {
            let top4 = ((self.buffer >> (self.valid - 4)) & 0xf) as u8;
            if top4 < 8 {
                self.valid -= 1;
                return Some(0);
            }
            if top4 < 12 {
                self.valid -= 2;
                return Some(1);
            }
            if top4 < 14 {
                self.valid -= 3;
                return Some(2);
            }
            if top4 == 14 {
                self.valid -= 4;
                return Some(3);
            }
            // '1111': need fifth bit to distinguish cases 4 and 5.
            if self.valid >= 5 {
                let bit4 = ((self.buffer >> (self.valid - 5)) & 1) as u8;
                self.valid -= 5;
                return Some(4 + bit4);
            }
            // Fifth bit spans a buffer boundary.
            self.valid -= 4;
            let bit4 = self.read_bit()?;
            return Some(if bit4 { 5 } else { 4 });
        }
        // Slow path: bits may span buffer boundaries, read one at a time.
        if !self.read_bit()? {
            return Some(0);
        }
        if !self.read_bit()? {
            return Some(1);
        }
        if !self.read_bit()? {
            return Some(2);
        }
        if !self.read_bit()? {
            return Some(3);
        }
        if !self.read_bit()? {
            return Some(4);
        }
        Some(5)
    }

    /// The bstream's own uvarint reader (not Go's binary.ReadUvarint): EOF errors pass through unchanged, and
    /// more than 10 bytes give "unexpected EOF".
    pub fn read_uvarint(&mut self) -> Result<u64, Error> {
        let mut x: u64 = 0;
        let mut s: u32 = 0;
        for _ in 0..10 {
            let byt = match self.read_byte() {
                Some(b) => b,
                None => return Err(Error::Eof),
            };
            if byt < 0x80 {
                return Ok(x | (byt as u64).checked_shl(s).unwrap_or(0));
            }
            x |= ((byt & 0x7f) as u64).checked_shl(s).unwrap_or(0);
            s += 7;
        }
        Err(Error::UnexpectedEof)
    }

    pub fn read_varint(&mut self) -> Result<i64, Error> {
        let ux = self.read_uvarint()?;
        let mut x = (ux >> 1) as i64;
        if ux & 1 != 0 {
            x = !x;
        }
        Ok(x)
    }

    /// Go `binary.ReadUvarint` over this reader as an io.ByteReader.
    pub fn go_read_uvarint(&mut self) -> Result<u64, Error> {
        let mut x: u64 = 0;
        let mut s: u32 = 0;
        for i in 0..10 {
            let b = match self.read_byte() {
                Some(b) => b,
                None => return Err(if i > 0 { Error::UnexpectedEof } else { Error::Eof }),
            };
            if b < 0x80 {
                if i == 9 && b > 1 {
                    return Err(Error::VarintOverflow);
                }
                return Ok(x | (b as u64).checked_shl(s).unwrap_or(0));
            }
            x |= ((b & 0x7f) as u64).checked_shl(s).unwrap_or(0);
            s += 7;
        }
        Err(Error::VarintOverflow)
    }

    /// Go `binary.ReadVarint` over this reader.
    pub fn go_read_varint(&mut self) -> Result<i64, Error> {
        let ux = self.go_read_uvarint()?;
        let mut x = (ux >> 1) as i64;
        if ux & 1 != 0 {
            x = !x;
        }
        Ok(x)
    }

    #[inline]
    pub(crate) fn load_next_buffer(&mut self, nbits: u8) -> bool {
        if self.stream_offset >= self.stream.len() {
            return false;
        }
        // More than 8 bytes left (most common case): never reads the very last byte of the stream.
        if self.stream_offset + 8 < self.stream.len() {
            let mut a = [0u8; 8];
            a.copy_from_slice(&self.stream[self.stream_offset..self.stream_offset + 8]);
            self.buffer = u64::from_be_bytes(a);
            self.stream_offset += 8;
            self.valid = 64;
            return true;
        }
        // 8 or less bytes left in the stream.
        let mut nbytes = (nbits / 8) as usize + 1;
        if self.stream_offset + nbytes > self.stream.len() {
            nbytes = self.stream.len() - self.stream_offset;
        }
        let mut buffer: u64 = 0;
        let mut skip = 0;
        if self.stream_offset + nbytes == self.stream.len() {
            // There can be concurrent writes happening on the very last byte of the stream, so use the copy.
            buffer |= self.last as u64;
            skip = 1;
        }
        for i in 0..nbytes - skip {
            buffer |= (self.stream[self.stream_offset + i] as u64) << (8 * (nbytes - i - 1));
        }
        self.buffer = buffer;
        self.stream_offset += nbytes;
        self.valid = (nbytes * 8) as u8;
        true
    }
}
