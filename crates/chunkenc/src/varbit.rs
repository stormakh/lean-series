//! Variable bit-width integer encoding (port of `tsdb/chunkenc/varbit.go`).

use crate::Error;
use crate::bstream::{Bstream, BstreamReader};

/// Returns true if `x` can be represented with `nbits` bits (-(2^(nbits-1)-1) <= x <= 2^(nbits-1)).
#[inline(always)]
pub fn bit_range(x: i64, nbits: u8) -> bool {
    let h = 1i64 << (nbits - 1);
    -(h - 1) <= x && x <= h
}

#[inline(always)]
fn bit_range_uint(x: u64, nbits: u32) -> bool {
    x.leading_zeros() >= 64 - nbits
}

/// Writes `val` with the varbit int encoding (bit by bit).
pub fn put_varbit_int(b: &mut Bstream, val: i64) {
    let u = val as u64;
    if val == 0 {
        b.write_bit(false);
    } else if bit_range(val, 3) {
        b.write_bits(0b10, 2);
        b.write_bits(u, 3);
    } else if bit_range(val, 6) {
        b.write_bits(0b110, 3);
        b.write_bits(u, 6);
    } else if bit_range(val, 9) {
        b.write_bits(0b1110, 4);
        b.write_bits(u, 9);
    } else if bit_range(val, 12) {
        b.write_bits(0b11110, 5);
        b.write_bits(u, 12);
    } else if bit_range(val, 18) {
        b.write_bits(0b111110, 6);
        b.write_bits(u, 18);
    } else if bit_range(val, 25) {
        b.write_bits(0b1111110, 7);
        b.write_bits(u, 25);
    } else if bit_range(val, 56) {
        b.write_bits(0b11111110, 8);
        b.write_bits(u, 56);
    } else {
        b.write_bits(0b11111111, 8);
        b.write_bits(u, 64);
    }
}

/// Same output as `put_varbit_int`, written with whole-byte writes.
pub fn put_varbit_int_fast(b: &mut Bstream, val: i64) {
    let u = val as u64;
    if val == 0 {
        b.write_bit(false);
    } else if bit_range(val, 3) {
        b.write_bits_fast((0b10 << 3) | (u & 0x7), 5);
    } else if bit_range(val, 6) {
        b.write_bits_fast((0b110 << 6) | (u & 0x3F), 9);
    } else if bit_range(val, 9) {
        b.write_bits_fast((0b1110 << 9) | (u & 0x1FF), 13);
    } else if bit_range(val, 12) {
        b.write_bits_fast((0b11110 << 12) | (u & 0xFFF), 17);
    } else if bit_range(val, 18) {
        b.write_bits_fast((0b111110 << 18) | (u & 0x3FFFF), 24);
    } else if bit_range(val, 25) {
        b.write_bits_fast((0b1111110 << 25) | (u & 0x1FFFFFF), 32);
    } else if bit_range(val, 56) {
        b.write_bits_fast((0b11111110u64 << 56) | (u & 0xFFFFFFFFFFFFFF), 64);
    } else {
        b.write_bits_fast(0b11111111, 8);
        b.write_bits_fast(u, 64);
    }
}

#[inline(always)]
fn read_prefix(b: &mut BstreamReader<'_>) -> Result<u8, Error> {
    let mut d: u8 = 0;
    for _ in 0..8 {
        d <<= 1;
        match b.read_bit_any() {
            None => return Err(Error::Eof),
            Some(false) => break,
            Some(true) => d |= 1,
        }
    }
    Ok(d)
}

#[inline(always)]
fn prefix_size(d: u8) -> Result<u8, Error> {
    Ok(match d {
        0b0 => 0,
        0b10 => 3,
        0b110 => 6,
        0b1110 => 9,
        0b11110 => 12,
        0b111110 => 18,
        0b1111110 => 25,
        0b11111110 => 56,
        0b11111111 => 64,
        _ => return Err(Error::InvalidBitPattern(d)),
    })
}

/// Reads a varbit int.
#[inline]
pub fn read_varbit_int(b: &mut BstreamReader<'_>) -> Result<i64, Error> {
    let d = read_prefix(b)?;
    let sz = prefix_size(d)?;
    if sz == 64 {
        // Do not use fast because it's very unlikely it will succeed.
        return b.read_bits(64).map(|v| v as i64).ok_or(Error::Eof);
    }
    if sz == 0 {
        return Ok(0);
    }
    let mut bits = b.read_bits_any(sz).ok_or(Error::Eof)?;
    if bits > (1u64 << (sz - 1)) {
        bits = bits.wrapping_sub(1u64 << sz);
    }
    Ok(bits as i64)
}

/// Writes `val` with the varbit uint encoding.
pub fn put_varbit_uint(b: &mut Bstream, val: u64) {
    if val == 0 {
        b.write_bit(false);
    } else if bit_range_uint(val, 3) {
        b.write_bits(0b10, 2);
        b.write_bits(val, 3);
    } else if bit_range_uint(val, 6) {
        b.write_bits(0b110, 3);
        b.write_bits(val, 6);
    } else if bit_range_uint(val, 9) {
        b.write_bits(0b1110, 4);
        b.write_bits(val, 9);
    } else if bit_range_uint(val, 12) {
        b.write_bits(0b11110, 5);
        b.write_bits(val, 12);
    } else if bit_range_uint(val, 18) {
        b.write_bits(0b111110, 6);
        b.write_bits(val, 18);
    } else if bit_range_uint(val, 25) {
        b.write_bits(0b1111110, 7);
        b.write_bits(val, 25);
    } else if bit_range_uint(val, 56) {
        b.write_bits(0b11111110, 8);
        b.write_bits(val, 56);
    } else {
        b.write_bits(0b11111111, 8);
        b.write_bits(val, 64);
    }
}

/// Reads a varbit uint.
#[inline]
pub fn read_varbit_uint(b: &mut BstreamReader<'_>) -> Result<u64, Error> {
    let d = read_prefix(b)?;
    let sz = prefix_size(d)?;
    if sz == 64 {
        return b.read_bits(64).ok_or(Error::Eof);
    }
    if sz == 0 {
        return Ok(0);
    }
    b.read_bits_any(sz).ok_or(Error::Eof)
}
