//! XOR float chunk (port of `tsdb/chunkenc/xor.go`), the Gorilla encoding.

use crate::bstream::{Bstream, BstreamReader, mask, shl, shr};
use crate::varbit::bit_range;
use crate::ChunkIterator as _;
use crate::{CHUNK_ALLOCATION_SIZE, CHUNK_HEADER_SIZE, ENC_XOR, Encoding, Error, ValueType};
use model::histogram::{FloatHistogram, Histogram};

/// Appends Go `binary.PutUvarint` bytes of `x` to the stream (byte by byte, as Go's writeByte loop).
#[inline]
pub(crate) fn write_uvarint(b: &mut Bstream, mut x: u64) {
    while x >= 0x80 {
        b.write_byte((x as u8) | 0x80);
        x >>= 7;
    }
    b.write_byte(x as u8);
}

/// Appends Go `binary.PutVarint` bytes of `x`.
#[inline]
pub(crate) fn write_varint(b: &mut Bstream, x: i64) {
    let mut ux = (x as u64) << 1;
    if x < 0 {
        ux = !ux;
    }
    write_uvarint(b, ux);
}

#[inline(always)]
pub(crate) fn read_u16(b: &[u8]) -> u16 {
    u16::from_be_bytes([b[0], b[1]])
}

#[inline(always)]
pub(crate) fn put_u16(b: &mut [u8], v: u16) {
    b[..2].copy_from_slice(&v.to_be_bytes());
}

/// An XOR chunk holding float samples.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct XorChunk {
    pub b: Bstream,
}

impl XorChunk {
    /// A new empty chunk (Go `NewXORChunk`).
    pub fn new() -> XorChunk {
        let mut stream = Vec::with_capacity(CHUNK_ALLOCATION_SIZE);
        stream.resize(CHUNK_HEADER_SIZE, 0);
        XorChunk { b: Bstream { stream, count: 0 } }
    }

    pub fn reset(&mut self, stream: Vec<u8>) {
        self.b.reset(stream);
    }

    pub fn encoding(&self) -> Encoding {
        ENC_XOR
    }

    pub fn bytes(&self) -> &[u8] {
        self.b.bytes()
    }

    pub fn num_samples(&self) -> usize {
        read_u16(self.b.bytes()) as usize
    }

    pub fn compact(&mut self) {
        crate::compact_stream(&mut self.b.stream);
    }

    /// An appender whose state continues after the last sample of the chunk.
    pub fn appender(&self) -> Result<XorAppender, Error> {
        if self.b.stream.len() == CHUNK_HEADER_SIZE {
            return Ok(XorAppender { t: i64::MIN, v: 0.0, t_delta: 0, leading: 0xff, trailing: 0 });
        }
        let mut it = self.iterator();
        while it.next() != ValueType::None {}
        if let Some(e) = it.err {
            return Err(e);
        }
        Ok(XorAppender { t: it.t, v: it.val, t_delta: it.t_delta, leading: it.leading, trailing: it.trailing })
    }

    pub fn iterator(&self) -> XorIterator<'_> {
        XorIterator::new(self.b.bytes())
    }
}

crate::impl_chunk!(XorChunk, Xor);

/// The encoder state of an XOR chunk.
#[derive(Clone, Debug, PartialEq)]
pub struct XorAppender {
    pub(crate) t: i64,
    pub(crate) v: f64,
    pub(crate) t_delta: u64,
    pub(crate) leading: u8,
    pub(crate) trailing: u8,
}

impl XorAppender {
    /// Appends a sample to `c`. Panics if the chunk has 65535 samples.
    #[inline]
    pub fn append(&mut self, c: &mut XorChunk, _st: i64, t: i64, v: f64) {
        let b = &mut c.b;
        let mut t_delta: u64 = 0;
        let num = read_u16(b.bytes());
        match num {
            0 => {
                write_varint(b, t);
                b.write_bits(v.to_bits(), 64);
            }
            1 => {
                t_delta = t.wrapping_sub(self.t) as u64;
                write_uvarint(b, t_delta);
                xor_write(b, v, self.v, &mut self.leading, &mut self.trailing);
            }
            u16::MAX => panic!("chunk capacity exceeded"),
            _ => {
                t_delta = t.wrapping_sub(self.t) as u64;
                let dod = t_delta.wrapping_sub(self.t_delta) as i64;
                if dod == 0 {
                    b.write_bit(false);
                } else if bit_range(dod, 14) {
                    b.write_byte((0b10 << 6) | (((dod >> 8) as u8) & ((1 << 6) - 1)));
                    b.write_byte(dod as u8);
                } else if bit_range(dod, 17) {
                    b.write_bits(0b110, 3);
                    b.write_bits(dod as u64, 17);
                } else if bit_range(dod, 20) {
                    b.write_bits(0b1110, 4);
                    b.write_bits(dod as u64, 20);
                } else {
                    b.write_bits(0b1111, 4);
                    b.write_bits(dod as u64, 64);
                }
                xor_write(b, v, self.v, &mut self.leading, &mut self.trailing);
            }
        }
        self.t = t;
        self.v = v;
        put_u16(b.bytes_mut(), num + 1);
        self.t_delta = t_delta;
    }
}

/// Iterator over an XOR chunk.
#[derive(Clone, Debug)]
pub struct XorIterator<'a> {
    pub(crate) br: BstreamReader<'a>,
    num_total: u16,
    num_read: u16,
    pub(crate) t: i64,
    pub(crate) val: f64,
    pub(crate) leading: u8,
    pub(crate) trailing: u8,
    pub(crate) t_delta: u64,
    pub(crate) err: Option<Error>,
}

impl<'a> XorIterator<'a> {
    /// An iterator over the bytes of an XOR chunk (including the 2 byte header).
    pub fn new(b: &'a [u8]) -> XorIterator<'a> {
        XorIterator {
            br: BstreamReader::new(&b[CHUNK_HEADER_SIZE..]),
            num_total: read_u16(b),
            num_read: 0,
            t: i64::MIN,
            val: 0.0,
            leading: 0,
            trailing: 0,
            t_delta: 0,
            err: None,
        }
    }

    /// Resets the iterator to new chunk bytes.
    pub fn reset(&mut self, b: &'a [u8]) {
        self.br = BstreamReader::new(&b[CHUNK_HEADER_SIZE..]);
        self.num_total = read_u16(b);
        self.num_read = 0;
        self.t = 0;
        self.val = 0.0;
        self.leading = 0;
        self.trailing = 0;
        self.t_delta = 0;
        self.err = None;
    }

    #[inline]
    fn read_value(&mut self) -> ValueType {
        let br = &mut self.br;
        if br.valid >= 1 {
            if (br.buffer >> (br.valid - 1)) & 1 == 0 {
                br.valid -= 1;
                self.num_read += 1;
                return ValueType::Float;
            }
            if self.leading != 0xff {
                let mbits = 64u8.wrapping_sub(self.leading).wrapping_sub(self.trailing);
                if br.valid as u16 >= 2 + mbits as u16 && (br.buffer >> (br.valid - 2)) & 1 == 0 {
                    br.valid -= 2; // Consume both control bits.
                    let data_mask = mask(mbits);
                    let bits = shr(br.buffer, br.valid - mbits) & data_mask;
                    br.valid -= mbits;
                    let vbits = self.val.to_bits() ^ shl(bits, self.trailing);
                    self.val = f64::from_bits(vbits);
                    self.num_read += 1;
                    return ValueType::Float;
                }
            }
        }
        if let Err(e) = xor_read(&mut self.br, &mut self.val, &mut self.leading, &mut self.trailing) {
            self.err = Some(e);
            return ValueType::None;
        }
        self.num_read += 1;
        ValueType::Float
    }
}

impl crate::ChunkIterator for XorIterator<'_> {
    fn seek(&mut self, t: i64) -> ValueType {
        if self.err.is_some() {
            return ValueType::None;
        }
        while t > self.t || self.num_read == 0 {
            if self.next() == ValueType::None {
                return ValueType::None;
            }
        }
        ValueType::Float
    }

    #[inline]
    fn at(&self) -> (i64, f64) {
        (self.t, self.val)
    }

    fn at_histogram(&self, _h: Option<Histogram>) -> (i64, Histogram) {
        panic!("cannot call xorIterator.AtHistogram");
    }

    fn at_float_histogram(&self, _fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        panic!("cannot call xorIterator.AtFloatHistogram");
    }

    #[inline]
    fn at_t(&self) -> i64 {
        self.t
    }

    fn at_st(&self) -> i64 {
        0
    }

    fn err(&self) -> Option<&Error> {
        self.err.as_ref()
    }

    #[inline]
    fn next(&mut self) -> ValueType {
        if self.err.is_some() || self.num_read == self.num_total {
            return ValueType::None;
        }
        if self.num_read == 0 {
            let t = match self.br.go_read_varint() {
                Ok(t) => t,
                Err(e) => {
                    self.err = Some(e);
                    return ValueType::None;
                }
            };
            let v = match self.br.read_bits(64) {
                Some(v) => v,
                None => {
                    self.err = Some(Error::Eof);
                    return ValueType::None;
                }
            };
            self.t = t;
            self.val = f64::from_bits(v);
            self.num_read += 1;
            return ValueType::Float;
        }
        if self.num_read == 1 {
            let t_delta = match self.br.go_read_uvarint() {
                Ok(t) => t,
                Err(e) => {
                    self.err = Some(e);
                    return ValueType::None;
                }
            };
            self.t_delta = t_delta;
            self.t = self.t.wrapping_add(self.t_delta as i64);
            return self.read_value();
        }

        // Fast path: dod == 0 (and possibly an unchanged value) from the buffer.
        if self.br.valid >= 1 && (self.br.buffer >> (self.br.valid - 1)) & 1 == 0 {
            self.br.valid -= 1;
            self.t = self.t.wrapping_add(self.t_delta as i64);
            if self.br.valid >= 1 && (self.br.buffer >> (self.br.valid - 1)) & 1 == 0 {
                self.br.valid -= 1;
                self.num_read += 1;
                return ValueType::Float;
            }
            return self.read_value();
        }

        let mut d: u8 = 0;
        for _ in 0..4 {
            d <<= 1;
            match self.br.read_bit_any() {
                None => {
                    self.err = Some(Error::Eof);
                    return ValueType::None;
                }
                Some(false) => break,
                Some(true) => d |= 1,
            }
        }
        let mut sz: u8 = 0;
        let mut dod: i64 = 0;
        match d {
            0b10 => sz = 14,
            0b110 => sz = 17,
            0b1110 => sz = 20,
            0b1111 => match self.br.read_bits(64) {
                Some(bits) => dod = bits as i64,
                None => {
                    self.err = Some(Error::Eof);
                    return ValueType::None;
                }
            },
            _ => {}
        }
        if sz != 0 {
            let mut bits = match self.br.read_bits_any(sz) {
                Some(b) => b,
                None => {
                    self.err = Some(Error::Eof);
                    return ValueType::None;
                }
            };
            if bits > (1u64 << (sz - 1)) {
                bits = bits.wrapping_sub(1u64 << sz);
            }
            dod = bits as i64;
        }
        self.t_delta = (self.t_delta as i64).wrapping_add(dod) as u64;
        self.t = self.t.wrapping_add(self.t_delta as i64);
        self.read_value()
    }
}

/// Writes the XOR of `new_value` with `current_value` (Gorilla value encoding).
#[inline]
pub fn xor_write(b: &mut Bstream, new_value: f64, current_value: f64, leading: &mut u8, trailing: &mut u8) {
    let delta = new_value.to_bits() ^ current_value.to_bits();
    if delta == 0 {
        b.write_bit(false);
        return;
    }
    b.write_bit(true);

    let mut new_leading = delta.leading_zeros() as u8;
    let new_trailing = delta.trailing_zeros() as u8;
    // Clamp number of leading zeros to avoid overflow when encoding.
    if new_leading >= 32 {
        new_leading = 31;
    }
    if *leading != 0xff && new_leading >= *leading && new_trailing >= *trailing {
        b.write_bit(false);
        b.write_bits(shr(delta, *trailing), 64 - *leading as i32 - *trailing as i32);
        return;
    }
    *leading = new_leading;
    *trailing = new_trailing;
    b.write_bit(true);
    b.write_bits(new_leading as u64, 5);
    // Note that if new_leading == new_trailing == 0, sigbits is 64, which is written as 0 in 6 bits (overflow).
    let sigbits = 64 - new_leading - new_trailing;
    b.write_bits(sigbits as u64, 6);
    b.write_bits(shr(delta, new_trailing), sigbits as i32);
}

/// Reads an XOR encoded value into `value`.
#[inline]
pub fn xor_read(br: &mut BstreamReader<'_>, value: &mut f64, leading: &mut u8, trailing: &mut u8) -> Result<(), Error> {
    let bit = br.read_bit_any().ok_or(Error::Eof)?;
    if !bit {
        return Ok(());
    }
    let bit = br.read_bit_any().ok_or(Error::Eof)?;
    let new_trailing;
    let mbits;
    if !bit {
        new_trailing = *trailing;
        mbits = 64u8.wrapping_sub(*leading).wrapping_sub(*trailing);
    } else {
        let new_leading = br.read_bits_any(5).ok_or(Error::Eof)? as u8;
        let mut mb = br.read_bits_any(6).ok_or(Error::Eof)? as u8;
        // 0 significant bits here means we overflowed and we actually need 64.
        if mb == 0 {
            mb = 64;
        }
        mbits = mb;
        new_trailing = 64u8.wrapping_sub(new_leading).wrapping_sub(mbits);
        *leading = new_leading;
        *trailing = new_trailing;
    }
    let bits = br.read_bits_any(mbits).ok_or(Error::Eof)?;
    let vbits = value.to_bits() ^ shl(bits, new_trailing);
    *value = f64::from_bits(vbits);
    Ok(())
}
