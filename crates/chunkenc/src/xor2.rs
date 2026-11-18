//! XOR2 float chunk (port of `tsdb/chunkenc/xor2.go`): XOR encoding with joint timestamp+value control bits,
//! byte-packed dod encoding, and an extra header byte after the sample count for optional start timestamps (ST).
//!
//! Control prefix for samples >= 2:
//!
//! ```text
//! 0     -> dod=0 AND value unchanged              (1 bit)
//! 10    -> dod=0, value changed                   (2 bits, then value encoding)
//! 110   -> dod!=0, 13-bit signed [-4096, 4095]    (prefix+dod packed into 2 bytes)
//! 1110  -> dod!=0, 20-bit signed [-524288, 524287] (prefix+dod packed into 3 bytes)
//! 11110 -> dod!=0, 64-bit escape                  (5+64 bits, then value encoding)
//! 11111 -> dod=0, stale NaN                       (5 bits, no value field)
//! ```
//!
//! Value encoding for the dod!=0 cases: `0` unchanged, `10` reuse window, `110` new window, `111` stale NaN.
//! Value encoding for the dod=0, value-changed case: `0` reuse window, `1` new window.
//!
//! ST header byte (at `b[CHUNK_HEADER_SIZE]`): bit 7 = firstSTKnown (ST of the first sample is in the stream),
//! bits 6-0 = firstSTChangeOn (sample index where the first ST change begins). Without ST the header stays 0 and
//! the chunk has no extra bits. With ST, the ST delta (prevT - st) follows each sample's joint encoding.

use crate::bstream::{Bstream, BstreamReader, mask, shl, shr};
use crate::varbit::{put_varbit_int_fast, read_varbit_int};
use crate::xor::{put_u16, read_u16, write_uvarint, write_varint};
use crate::Iterator as _;
use crate::{CHUNK_ALLOCATION_SIZE, CHUNK_HEADER_SIZE, ENC_XOR2, Encoding, Error, STALE_NAN, ValueType, is_stale_nan};
use model::histogram::{FloatHistogram, Histogram};

const CHUNK_ST_HEADER_SIZE: usize = 1;

/// Go `maxFirstSTChangeOn` (st.go).
const MAX_FIRST_ST_CHANGE_ON: u16 = 0x7F;

/// Go `writeHeaderFirstSTKnown` (st.go).
#[inline]
fn write_header_first_st_known(b: &mut [u8]) {
    b[0] = 0x80;
}

/// Go `writeHeaderFirstSTChangeOn` (st.go).
#[inline]
fn write_header_first_st_change_on(b: &mut [u8], first_st_change_on: u16) {
    if first_st_change_on > MAX_FIRST_ST_CHANGE_ON {
        // This should never happen, would cause corruption (ST already skipped but shouldn't).
        return;
    }
    b[0] |= first_st_change_on as u8;
}

/// Go `readSTHeader` (st.go).
#[inline]
fn read_st_header(b: &[u8]) -> (bool, u8) {
    (b[0] & 0x80 != 0, b[0] & 0x7F)
}

/// An XOR2 chunk holding float samples with optional start timestamps.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Xor2Chunk {
    pub b: Bstream,
}

impl Xor2Chunk {
    /// A new empty chunk (Go `NewXOR2Chunk`).
    pub fn new() -> Xor2Chunk {
        let mut stream = Vec::with_capacity(CHUNK_ALLOCATION_SIZE);
        stream.resize(CHUNK_HEADER_SIZE + CHUNK_ST_HEADER_SIZE, 0);
        Xor2Chunk { b: Bstream { stream, count: 0 } }
    }

    pub fn from_bstream(b: Bstream) -> Xor2Chunk {
        Xor2Chunk { b }
    }

    pub fn reset(&mut self, stream: Vec<u8>) {
        self.b.reset(stream);
    }

    pub fn encoding(&self) -> Encoding {
        ENC_XOR2
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

    /// An appender whose state continues after the last sample of the chunk. Like Go, it also sets the chunk's
    /// write position (`c.b.count = it.br.valid`).
    pub fn appender(&mut self) -> Result<Xor2Appender, Error> {
        if self.b.stream.len() == CHUNK_HEADER_SIZE + CHUNK_ST_HEADER_SIZE {
            return Ok(Xor2Appender {
                st: 0,
                t: i64::MIN,
                v: 0.0,
                t_delta: 0,
                st_diff: 0,
                leading: 0xff,
                trailing: 0,
                num_total: 0,
                first_st_change_on: 0,
                first_st_known: false,
            });
        }
        let mut it = self.iterator();
        while it.next() != ValueType::None {}
        if let Some(e) = it.err {
            return Err(e);
        }
        let valid = it.br.valid;
        let a = Xor2Appender {
            st: it.st,
            t: it.t,
            v: it.baseline_v,
            t_delta: it.t_delta,
            st_diff: it.st_diff,
            leading: it.leading,
            trailing: it.trailing,
            num_total: read_u16(self.b.bytes()),
            first_st_change_on: it.first_st_change_on as u16,
            first_st_known: it.first_st_known,
        };
        // Set the bit position for continuing writes: the reader knows how many bits of the last byte are unread.
        self.b.count = valid;
        Ok(a)
    }

    pub fn iterator(&self) -> Xor2Iterator<'_> {
        Xor2Iterator::new(self.b.bytes())
    }
}

crate::impl_chunk!(Xor2Chunk, Xor2);

/// The encoder state of an XOR2 chunk.
#[derive(Clone, Debug, PartialEq)]
pub struct Xor2Appender {
    st: i64,
    t: i64,
    v: f64,
    t_delta: u64,
    /// prevT - st for the previous sample.
    st_diff: i64,
    leading: u8,
    trailing: u8,
    num_total: u16,
    first_st_change_on: u16,
    first_st_known: bool,
}

/// Writes the ST delta of the active-ST fast path when it follows a T/V `0` bit, fused with that bit (Go inlines
/// this switch twice).
#[inline(always)]
fn write_zero_and_st_delta(b: &mut Bstream, delta_st_diff: i64) {
    let u = delta_st_diff as u64;
    if delta_st_diff == 0 {
        b.write_bit(false);
        b.write_bit(false);
    } else if (-3..=4).contains(&delta_st_diff) {
        // 0 (T/V) + 5-bit ST = 6 bits.
        b.write_bits_fast((0b10 << 3) | (u & 0x7), 6);
    } else if (-31..=32).contains(&delta_st_diff) {
        // 0 (T/V) + 9-bit ST = 10 bits.
        b.write_bits_fast((0b110 << 6) | (u & 0x3F), 10);
    } else if (-255..=256).contains(&delta_st_diff) {
        // 0 (T/V) + 13-bit ST = 14 bits.
        b.write_bits_fast((0b1110 << 9) | (u & 0x1FF), 14);
    } else {
        b.write_bit(false);
        put_varbit_int_fast(b, delta_st_diff);
    }
}

impl Xor2Appender {
    /// Appends a sample to `c`. Panics if the chunk has 65535 samples.
    #[inline]
    pub fn append(&mut self, c: &mut Xor2Chunk, st: i64, t: i64, v: f64) {
        let b = &mut c.b;
        let mut t_delta: u64 = 0;
        let mut st_diff: i64 = 0;

        match self.num_total {
            0 => {
                write_varint(b, t);
                b.write_bits_fast(v.to_bits(), 64);
                if st != 0 {
                    write_varint(b, t.wrapping_sub(st));
                    self.first_st_known = true;
                    write_header_first_st_known(&mut b.bytes_mut()[CHUNK_HEADER_SIZE..]);
                }
            }
            1 => {
                t_delta = t.wrapping_sub(self.t) as u64;
                write_uvarint(b, t_delta);
                self.write_v_delta(b, v);
                if st != self.st {
                    st_diff = self.t.wrapping_sub(st);
                    self.first_st_change_on = 1;
                    write_header_first_st_change_on(&mut b.bytes_mut()[CHUNK_HEADER_SIZE..], 1);
                    put_varbit_int_fast(b, st_diff);
                }
            }
            u16::MAX => panic!("chunk capacity exceeded"),
            _ => {
                t_delta = t.wrapping_sub(self.t) as u64;
                let dod = t_delta.wrapping_sub(self.t_delta) as i64;

                // Fast path: no new ST data to write for this sample.
                // Covers: ST never seen (st=0 always), or ST recorded initially but unchanged.
                // Must use the slow path at MAX_FIRST_ST_CHANGE_ON so the header remains valid even if ST changes
                // on a later sample (index > MAX_FIRST_ST_CHANGE_ON).
                if self.first_st_change_on == 0 && st == self.st && self.num_total != MAX_FIRST_ST_CHANGE_ON {
                    let vbits = v.to_bits();
                    if dod == 0 && vbits == self.v.to_bits() {
                        // Unchanged value and timestamp: a single 0 bit. self.v stays correct.
                        b.write_bit(false);
                    } else if (-(1 << 12)..=(1 << 12) - 1).contains(&dod) && vbits == self.v.to_bits() {
                        // 13-bit dod, value unchanged: both bytes and the zero value bit inline.
                        b.write_byte(0b110_00000 | (((dod as u64) >> 8) as u8 & 0x1F));
                        b.write_byte(dod as u64 as u8);
                        b.write_bit(false);
                    } else {
                        self.encode_joint(b, dod, v);
                        if !is_stale_nan(v) {
                            self.v = v;
                        }
                    }
                    self.t = t;
                    self.t_delta = t_delta;
                    self.num_total += 1;
                    put_u16(b.bytes_mut(), self.num_total);
                    return;
                }

                // Active-ST fast path: first_st_change_on is set, so every sample needs a per-sample ST delta.
                if self.first_st_change_on > 0 {
                    let new_st_diff = self.t.wrapping_sub(st);
                    let delta_st_diff = new_st_diff.wrapping_sub(self.st_diff);
                    let vbits = v.to_bits();
                    if dod == 0 && vbits == self.v.to_bits() {
                        // T/V: single 0 bit (dod=0, value unchanged), fused with the ST delta.
                        write_zero_and_st_delta(b, delta_st_diff);
                    } else if (-(1 << 12)..=(1 << 12) - 1).contains(&dod) && vbits == self.v.to_bits() {
                        b.write_byte(0b110_00000 | (((dod as u64) >> 8) as u8 & 0x1F));
                        b.write_byte(dod as u64 as u8);
                        // T/V ends with a 0 bit (value unchanged), fused with the ST delta.
                        write_zero_and_st_delta(b, delta_st_diff);
                    } else {
                        self.encode_joint(b, dod, v);
                        if !is_stale_nan(v) {
                            self.v = v;
                        }
                        let u = delta_st_diff as u64;
                        if delta_st_diff == 0 {
                            b.write_bit(false);
                        } else if (-3..=4).contains(&delta_st_diff) {
                            b.write_bits_fast((0b10 << 3) | (u & 0x7), 5);
                        } else if (-31..=32).contains(&delta_st_diff) {
                            b.write_bits_fast((0b110 << 6) | (u & 0x3F), 9);
                        } else if (-255..=256).contains(&delta_st_diff) {
                            b.write_bits_fast((0b1110 << 9) | (u & 0x1FF), 13);
                        } else {
                            put_varbit_int_fast(b, delta_st_diff);
                        }
                    }
                    self.st_diff = new_st_diff;
                    self.st = st;
                    self.t = t;
                    self.t_delta = t_delta;
                    self.num_total += 1;
                    put_u16(b.bytes_mut(), self.num_total);
                    return;
                }

                // Full slow path: first_st_change_on == 0 and ST may be initialised here.
                self.encode_joint(b, dod, v);

                if st != self.st || self.num_total == MAX_FIRST_ST_CHANGE_ON {
                    // First ST change: record prevT - st.
                    st_diff = self.t.wrapping_sub(st);
                    self.first_st_change_on = self.num_total;
                    write_header_first_st_change_on(&mut b.bytes_mut()[CHUNK_HEADER_SIZE..], self.num_total);
                    put_varbit_int_fast(b, st_diff);
                }
            }
        }

        self.st = st;
        self.t = t;
        if !is_stale_nan(v) {
            self.v = v;
        }
        self.t_delta = t_delta;
        self.st_diff = st_diff;
        self.num_total += 1;
        put_u16(b.bytes_mut(), self.num_total);
    }

    /// Writes the XOR2 joint timestamp+value control sequence for samples >= 2.
    #[inline]
    fn encode_joint(&mut self, b: &mut Bstream, dod: i64, v: f64) {
        if dod == 0 {
            if is_stale_nan(v) {
                b.write_bits_fast(0b11111, 5);
                return;
            }
            let vbits = v.to_bits() ^ self.v.to_bits();
            if vbits == 0 {
                b.write_bit(false);
                return;
            }
            b.write_bits_fast(0b10, 2);
            self.write_v_delta_known_non_zero(b, vbits);
            return;
        }

        let u = dod as u64;
        if (-(1 << 12)..=(1 << 12) - 1).contains(&dod) {
            // 13-bit dod: prefix `110` packed with top 5 bits -> 2 bytes total.
            b.write_byte(0b110_00000 | ((u >> 8) as u8 & 0x1F));
            b.write_byte(u as u8);
        } else if (-(1 << 19)..=(1 << 19) - 1).contains(&dod) {
            // 20-bit dod: prefix `1110` packed with top 4 bits -> 3 bytes total.
            b.write_byte(0b1110_0000 | ((u >> 16) as u8 & 0x0F));
            b.write_byte((u >> 8) as u8);
            b.write_byte(u as u8);
        } else {
            // 64-bit escape (rare): `11110`.
            b.write_bits_fast(0b11110, 5);
            b.write_bits_fast(u, 64);
        }
        // Inline the most common value-unchanged case.
        if v.to_bits() == self.v.to_bits() {
            b.write_bit(false);
        } else {
            self.write_v_delta(b, v);
        }
    }

    /// Encodes the value delta for the dod!=0 case.
    #[inline]
    fn write_v_delta(&mut self, b: &mut Bstream, v: f64) {
        if is_stale_nan(v) {
            b.write_bits_fast(0b111, 3);
            return;
        }

        let delta = v.to_bits() ^ self.v.to_bits();
        if delta == 0 {
            b.write_bit(false);
            return;
        }

        let mut new_leading = delta.leading_zeros() as u8;
        let mut new_trailing = delta.trailing_zeros() as u8;
        if new_leading >= 32 {
            new_leading = 31;
        }

        // A new window costs 12 extra bits: one control bit and 11 window bits. Reuse has prefix 10; replacement
        // has prefix 110 plus leading(5) and sigbits(6). Choose the lower cost for this delta and reuse on a tie.
        if new_leading >= self.leading
            && new_trailing >= self.trailing
            && new_leading.wrapping_add(new_trailing)
                <= self.leading.wrapping_add(self.trailing).wrapping_add(12)
        {
            b.write_bits_fast(0b10, 2);
            b.write_bits_fast(shr(delta, self.trailing), 64 - self.leading as i32 - self.trailing as i32);
            return;
        }

        // When widening, retain one or both narrower window boundaries if the union adds at most half a
        // replacement header's cost in payload bits.
        if self.leading != 0xff && (new_leading < self.leading || new_trailing < self.trailing) {
            let union_leading = new_leading.min(self.leading);
            let union_trailing = new_trailing.min(self.trailing);
            if new_leading
                .wrapping_add(new_trailing)
                .wrapping_sub(union_leading)
                .wrapping_sub(union_trailing)
                <= 6
            {
                new_leading = union_leading;
                new_trailing = union_trailing;
            }
        }

        self.leading = new_leading;
        self.trailing = new_trailing;

        b.write_bits_fast(0b110, 3);
        b.write_bits_fast(new_leading as u64, 5);
        let sigbits = 64u8.wrapping_sub(new_leading).wrapping_sub(new_trailing);
        b.write_bits_fast(sigbits as u64, 6);
        b.write_bits_fast(shr(delta, new_trailing), sigbits as i32);
    }

    /// Encodes a non-zero value XOR delta for the dod=0, value-changed case: `0` reuse window, `1` new window.
    #[inline]
    fn write_v_delta_known_non_zero(&mut self, b: &mut Bstream, delta: u64) {
        let mut new_leading = delta.leading_zeros() as u8;
        let mut new_trailing = delta.trailing_zeros() as u8;
        if new_leading >= 32 {
            new_leading = 31;
        }

        // Both controls cost one bit, so a new window costs 11 extra window bits.
        if new_leading >= self.leading
            && new_trailing >= self.trailing
            && new_leading.wrapping_add(new_trailing)
                <= self.leading.wrapping_add(self.trailing).wrapping_add(11)
        {
            b.write_bit(false);
            b.write_bits_fast(shr(delta, self.trailing), 64 - self.leading as i32 - self.trailing as i32);
            return;
        }

        // Retain nearby old boundaries when widening (five extra payload bits allowed).
        if self.leading != 0xff && (new_leading < self.leading || new_trailing < self.trailing) {
            let union_leading = new_leading.min(self.leading);
            let union_trailing = new_trailing.min(self.trailing);
            if new_leading
                .wrapping_add(new_trailing)
                .wrapping_sub(union_leading)
                .wrapping_sub(union_trailing)
                <= 5
            {
                new_leading = union_leading;
                new_trailing = union_trailing;
            }
        }

        self.leading = new_leading;
        self.trailing = new_trailing;

        b.write_bit(true);
        b.write_bits_fast(new_leading as u64, 5);
        let sigbits = 64u8.wrapping_sub(new_leading).wrapping_sub(new_trailing);
        b.write_bits_fast(sigbits as u64, 6);
        b.write_bits_fast(shr(delta, new_trailing), sigbits as i32);
    }
}

/// Iterator over an XOR2 chunk. The fields are public (hidden) only for the ported Go tests.
#[derive(Clone, Debug, Default)]
pub struct Xor2Iterator<'a> {
    #[doc(hidden)]
    pub br: BstreamReader<'a>,
    num_total: u16,
    num_read: u16,

    first_st_known: bool,
    first_st_change_on: u8,

    #[doc(hidden)]
    pub leading: u8,
    #[doc(hidden)]
    pub trailing: u8,

    st: i64,
    t: i64,
    #[doc(hidden)]
    pub val: f64,

    t_delta: u64,
    /// Accumulated prevT - st.
    st_diff: i64,
    err: Option<Error>,

    /// Last non-stale value for the XOR baseline.
    #[doc(hidden)]
    pub baseline_v: f64,
}

#[inline(always)]
fn stale_nan() -> f64 {
    f64::from_bits(STALE_NAN)
}

impl<'a> Xor2Iterator<'a> {
    /// An iterator over the bytes of an XOR2 chunk (including the 3 header bytes).
    pub fn new(b: &'a [u8]) -> Xor2Iterator<'a> {
        let mut it = Xor2Iterator::default();
        it.reset(b);
        it
    }

    /// Resets the iterator to new chunk bytes.
    pub fn reset(&mut self, b: &'a [u8]) {
        self.br = BstreamReader::new(&b[CHUNK_HEADER_SIZE + CHUNK_ST_HEADER_SIZE..]);
        self.num_total = read_u16(b);
        let (known, change_on) = read_st_header(&b[CHUNK_HEADER_SIZE..]);
        self.first_st_known = known;
        self.first_st_change_on = change_on;

        self.num_read = 0;
        self.st = 0;
        self.t = 0;
        self.val = 0.0;
        self.leading = 0;
        self.trailing = 0;
        self.t_delta = 0;
        self.st_diff = 0;
        self.baseline_v = 0.0;
        self.err = None;
    }

    #[inline(always)]
    fn fail(&mut self, e: Error) -> ValueType {
        self.err = Some(e);
        ValueType::None
    }

    /// Reads `n` bits from the buffer if there are enough, else through `read_bits`.
    #[inline(always)]
    fn read_n(&mut self, n: u8) -> Result<u64, Error> {
        let br = &mut self.br;
        if br.valid >= n {
            br.valid -= n;
            Ok(shr(br.buffer, br.valid) & mask(n))
        } else {
            br.read_bits(n).ok_or(Error::Eof)
        }
    }

    /// Reads one bit from the buffer if there is one, else through `read_bit`.
    #[inline(always)]
    fn read_one(&mut self) -> Result<bool, Error> {
        let br = &mut self.br;
        if br.valid > 0 {
            br.valid -= 1;
            Ok(br.buffer & (1u64 << br.valid) != 0)
        } else {
            br.read_bit().ok_or(Error::Eof)
        }
    }

    /// Reads a signed dod of width `w` bits and updates `t_delta` and `t`.
    #[inline]
    fn read_dod(&mut self, w: u8) -> Result<(), Error> {
        let mut b = self.read_n(w)?;
        if w < 64 && b >= (1u64 << (w - 1)) {
            b = b.wrapping_sub(1u64 << w);
        }
        self.t_delta = (self.t_delta as i64).wrapping_add(b as i64) as u64;
        self.t = self.t.wrapping_add(self.t_delta as i64);
        Ok(())
    }

    /// Applies `value_bits << trailing` to the baseline value.
    #[inline(always)]
    fn apply_xor(&mut self, value_bits: u64) {
        let vbits = self.baseline_v.to_bits() ^ shl(value_bits, self.trailing);
        self.val = f64::from_bits(vbits);
        self.baseline_v = self.val;
    }

    /// Reads the XOR2 value encoding for the dod!=0 case: `0` unchanged, `10` reuse window, `110` new window,
    /// `111` stale NaN.
    #[doc(hidden)]
    #[inline]
    pub fn decode_value(&mut self) -> Result<(), Error> {
        // Fast path: 3 bits available, read the full control prefix in one shot.
        if self.br.valid >= 3 {
            let ctrl = (self.br.buffer >> (self.br.valid - 3)) & 0x7;
            if ctrl & 0x4 == 0 {
                // `0xx`: value unchanged, consume 1 bit.
                self.br.valid -= 1;
                self.val = self.baseline_v;
                return Ok(());
            }
            if ctrl & 0x6 == 0x4 {
                // `10x`: reuse previous leading/trailing window, consume 2 bits.
                self.br.valid -= 2;
                let sz = (64 - self.leading as i32 - self.trailing as i32) as u8;
                let value_bits = self.read_n(sz)?;
                self.apply_xor(value_bits);
                return Ok(());
            }
            // `11x`: consume 3 bits.
            self.br.valid -= 3;
            if ctrl == 0x6 {
                // `110`: new leading/trailing window.
                return self.decode_new_leading_trailing();
            }
            // `111`: stale NaN.
            self.val = stale_nan();
            return Ok(());
        }

        // Slow path: fewer than 3 bits buffered (rare, only near buffer refills).
        if !self.read_one()? {
            // `0` -> value unchanged.
            self.val = self.baseline_v;
            return Ok(());
        }
        if !self.read_one()? {
            // `10` -> reuse previous leading/trailing window.
            let sz = (64 - self.leading as i32 - self.trailing as i32) as u8;
            let value_bits = self.read_n(sz)?;
            self.apply_xor(value_bits);
            return Ok(());
        }
        if !self.read_one()? {
            // `110` -> new leading/trailing window.
            return self.decode_new_leading_trailing();
        }
        // `111` -> stale NaN.
        self.val = stale_nan();
        Ok(())
    }

    /// Reads the XOR2 value encoding for the dod=0, value-changed case: `0` reuse window, `1` new window.
    #[doc(hidden)]
    #[inline]
    pub fn decode_value_known_non_zero(&mut self) -> Result<(), Error> {
        let sz = (64 - self.leading as i32 - self.trailing as i32) as u8;
        // Fast path: the 1-bit control and the sz-bit value in a single buffer operation.
        if self.br.valid >= 1u8.wrapping_add(sz) {
            let ctrl_bit = (self.br.buffer >> (self.br.valid - 1)) & 1;
            if ctrl_bit == 0 {
                // `0`: reuse previous leading/trailing window.
                self.br.valid = self.br.valid.wrapping_sub(1u8.wrapping_add(sz));
                let value_bits = shr(self.br.buffer, self.br.valid) & mask(sz);
                self.apply_xor(value_bits);
                return Ok(());
            }
            // `1`: new leading/trailing window.
            self.br.valid -= 1;
            return self.decode_new_leading_trailing();
        }

        // Slow path: control bit then value bits separately.
        if !self.read_one()? {
            let value_bits = self.read_n(sz)?;
            self.apply_xor(value_bits);
            return Ok(());
        }
        self.decode_new_leading_trailing()
    }

    /// Reads a new leading/sigbits/value triple and updates leading, trailing, val and baseline_v.
    #[doc(hidden)]
    #[inline]
    pub fn decode_new_leading_trailing(&mut self) -> Result<(), Error> {
        let (new_leading, mut sigbits);
        // Fast path: leading (5 bits) and sigbits (6 bits) together as 11 bits.
        if self.br.valid >= 11 {
            let val = (self.br.buffer >> (self.br.valid - 11)) & 0x7ff;
            self.br.valid -= 11;
            new_leading = val >> 6;
            sigbits = val & 0x3f;
        } else {
            new_leading = self.br.read_bits(5).ok_or(Error::Eof)?;
            sigbits = self.br.read_bits(6).ok_or(Error::Eof)?;
        }

        self.leading = new_leading as u8;
        if sigbits == 0 {
            sigbits = 64;
        }
        self.trailing = 64u8.wrapping_sub(self.leading).wrapping_sub(sigbits as u8);

        let value_bits = self.read_n(sigbits as u8)?;
        self.apply_xor(value_bits);
        Ok(())
    }

    /// Reads the optional ST data of sample n >= 2.
    #[inline]
    fn read_st(&mut self, saved_num_read: u16, prev_t: i64) -> Result<(), Error> {
        let sdod = read_varbit_int(&mut self.br)?;
        if saved_num_read == self.first_st_change_on as u16 {
            self.st_diff = sdod;
        } else {
            self.st_diff = self.st_diff.wrapping_add(sdod);
        }
        self.st = prev_t.wrapping_sub(self.st_diff);
        Ok(())
    }
}

impl crate::Iterator for Xor2Iterator<'_> {
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
        panic!("cannot call xor2Iterator.AtHistogram");
    }

    fn at_float_histogram(&self, _fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        panic!("cannot call xor2Iterator.AtFloatHistogram");
    }

    #[inline]
    fn at_t(&self) -> i64 {
        self.t
    }

    #[inline]
    fn at_st(&self) -> i64 {
        self.st
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
            let t = match self.br.read_varint() {
                Ok(t) => t,
                Err(e) => return self.fail(e),
            };
            let v = match self.br.read_bits(64) {
                Some(v) => v,
                None => return self.fail(Error::Eof),
            };
            self.t = t;
            self.val = f64::from_bits(v);
            if !is_stale_nan(self.val) {
                self.baseline_v = self.val;
            }
            // Optional ST for sample 0.
            if self.first_st_known {
                let st_diff = match self.br.read_varint() {
                    Ok(d) => d,
                    Err(e) => return self.fail(e),
                };
                self.st = t.wrapping_sub(st_diff);
            }
            self.num_read += 1;
            return ValueType::Float;
        }

        if self.num_read == 1 {
            let t_delta = match self.br.read_uvarint() {
                Ok(d) => d,
                Err(e) => return self.fail(e),
            };
            let prev_t = self.t;
            self.t_delta = t_delta;
            self.t = self.t.wrapping_add(t_delta as i64);
            if let Err(e) = self.decode_value() {
                return self.fail(e);
            }
            // Optional ST delta for sample 1.
            if self.first_st_change_on == 1 {
                let sdod = match read_varbit_int(&mut self.br) {
                    Ok(d) => d,
                    Err(e) => return self.fail(e),
                };
                self.st_diff = sdod;
                self.st = prev_t.wrapping_sub(sdod);
            }
            self.num_read += 1;
            return ValueType::Float;
        }

        // Sample N >= 2: read joint XOR2 control, then optional ST data.
        let prev_t = self.t;
        let saved_num_read = self.num_read;

        let ctrl = match self.br.read_xor2_control_fast() {
            Some(c) => c,
            None => match self.br.read_xor2_control() {
                Some(c) => c,
                None => return self.fail(Error::Eof),
            },
        };

        let res = match ctrl {
            0 => {
                // dod=0, value unchanged.
                self.t = self.t.wrapping_add(self.t_delta as i64);
                self.val = self.baseline_v;
                Ok(())
            }
            1 => {
                // dod=0, value changed.
                self.t = self.t.wrapping_add(self.t_delta as i64);
                self.decode_value_known_non_zero()
            }
            // 13-bit dod.
            2 => self.read_dod(13).and_then(|_| self.decode_value()),
            // 20-bit dod.
            3 => self.read_dod(20).and_then(|_| self.decode_value()),
            // 64-bit escape.
            4 => self.read_dod(64).and_then(|_| self.decode_value()),
            _ => {
                // dod=0, stale NaN.
                self.t = self.t.wrapping_add(self.t_delta as i64);
                self.val = stale_nan();
                Ok(())
            }
        };
        if let Err(e) = res {
            return self.fail(e);
        }

        // Optional ST data after the joint timestamp+value encoding. The ST delta was encoded as (prevT - st),
        // using the previous sample's t.
        if self.first_st_change_on > 0 && saved_num_read >= self.first_st_change_on as u16 {
            if let Err(e) = self.read_st(saved_num_read, prev_t) {
                return self.fail(e);
            }
        }

        self.num_read += 1;
        ValueType::Float
    }
}
