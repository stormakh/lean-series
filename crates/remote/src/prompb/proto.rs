//! Protobuf wire helpers that follow the gogo-generated `Marshal`/`Unmarshal` code of `prompb`.
//!
//! Encoding writes fields in field-number order, skips proto3 zero scalars (`!= 0`, so `-0.0` is skipped
//! as in Go), packs repeated scalars, and always writes non-nullable sub-messages. The output is
//! byte-identical to gogo's `Marshal`.
//!
//! Decoding accepts packed and unpacked repeated scalars, skips unknown fields (gogo keeps them in
//! `XXX_unrecognized`; nothing in Prometheus reads them back), and returns Go's error texts.

use std::borrow::Cow;
use std::fmt;

pub const ERR_INVALID_LENGTH: &str = "proto: negative length found during unmarshaling";
pub const ERR_INT_OVERFLOW: &str = "proto: integer overflow";
pub const ERR_UNEXPECTED_END_OF_GROUP: &str = "proto: unexpected end of group";
pub const ERR_UNEXPECTED_EOF: &str = "unexpected EOF";

/// A protobuf decoding error with Go's text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodeError(pub Cow<'static, str>);

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DecodeError {}

impl DecodeError {
    pub fn eof() -> DecodeError {
        DecodeError(Cow::Borrowed(ERR_UNEXPECTED_EOF))
    }
    fn overflow() -> DecodeError {
        DecodeError(Cow::Borrowed(ERR_INT_OVERFLOW))
    }
    fn invalid_length() -> DecodeError {
        DecodeError(Cow::Borrowed(ERR_INVALID_LENGTH))
    }
    pub fn wrong_wire_type(wt: u8, field: &str) -> DecodeError {
        DecodeError(Cow::Owned(format!("proto: wrong wireType = {wt} for field {field}")))
    }
}

pub type Result<T> = std::result::Result<T, DecodeError>;

// ---------------------------------------------------------------- encoding

/// Number of bytes of `v` as a varint (gogo `sov`).
#[inline]
pub fn sov(v: u64) -> usize {
    ((64 - (v | 1).leading_zeros() as usize) + 6) / 7
}

/// Zigzag size (gogo `soz`).
#[inline]
pub fn soz(v: i64) -> usize {
    sov(((v << 1) ^ (v >> 63)) as u64)
}

#[inline]
pub fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

#[inline]
pub fn put_tag(buf: &mut Vec<u8>, field: u32, wire_type: u8) {
    put_varint(buf, ((field as u64) << 3) | wire_type as u64);
}

#[inline]
pub fn tag_len(field: u32) -> usize {
    sov((field as u64) << 3)
}

/// A varint field, skipped when zero.
#[inline]
pub fn put_uint_field(buf: &mut Vec<u8>, field: u32, v: u64) {
    if v != 0 {
        put_tag(buf, field, 0);
        put_varint(buf, v);
    }
}

#[inline]
pub fn uint_field_len(field: u32, v: u64) -> usize {
    if v != 0 { tag_len(field) + sov(v) } else { 0 }
}

/// An int64/int32/enum field (negative values take 10 bytes), skipped when zero.
#[inline]
pub fn put_int_field(buf: &mut Vec<u8>, field: u32, v: i64) {
    put_uint_field(buf, field, v as u64)
}

#[inline]
pub fn int_field_len(field: u32, v: i64) -> usize {
    uint_field_len(field, v as u64)
}

/// A sint32/sint64 field (zigzag), skipped when zero.
#[inline]
pub fn put_sint_field(buf: &mut Vec<u8>, field: u32, v: i64) {
    if v != 0 {
        put_tag(buf, field, 0);
        put_varint(buf, ((v << 1) ^ (v >> 63)) as u64);
    }
}

#[inline]
pub fn sint_field_len(field: u32, v: i64) -> usize {
    if v != 0 { tag_len(field) + soz(v) } else { 0 }
}

/// A sint32 field. Go zigzags a sint32 in 32 bits: `(uint32(v)<<1)^uint32(v>>31)`.
#[inline]
pub fn put_sint32_field(buf: &mut Vec<u8>, field: u32, v: i32) {
    if v != 0 {
        put_tag(buf, field, 0);
        put_varint(buf, (((v as u32) << 1) ^ ((v >> 31) as u32)) as u64);
    }
}

#[inline]
pub fn sint32_field_len(field: u32, v: i32) -> usize {
    if v != 0 { tag_len(field) + sov((((v as u32) << 1) ^ ((v >> 31) as u32)) as u64) } else { 0 }
}

/// A bool field, skipped when false.
#[inline]
pub fn put_bool_field(buf: &mut Vec<u8>, field: u32, v: bool) {
    if v {
        put_tag(buf, field, 0);
        buf.push(1);
    }
}

#[inline]
pub fn bool_field_len(field: u32, v: bool) -> usize {
    if v { tag_len(field) + 1 } else { 0 }
}

/// A double field, skipped when `v == 0` (so `-0.0` is skipped too, as gogo does; NaN is written).
#[inline]
pub fn put_double_field(buf: &mut Vec<u8>, field: u32, v: f64) {
    if v != 0.0 {
        put_tag(buf, field, 1);
        buf.extend_from_slice(&v.to_bits().to_le_bytes());
    }
}

#[inline]
pub fn double_field_len(field: u32, v: f64) -> usize {
    if v != 0.0 { tag_len(field) + 8 } else { 0 }
}

/// A double that is always written (oneof members).
#[inline]
pub fn put_double_always(buf: &mut Vec<u8>, field: u32, v: f64) {
    put_tag(buf, field, 1);
    buf.extend_from_slice(&v.to_bits().to_le_bytes());
}

/// A varint that is always written (oneof members).
#[inline]
pub fn put_uint_always(buf: &mut Vec<u8>, field: u32, v: u64) {
    put_tag(buf, field, 0);
    put_varint(buf, v);
}

/// A string or bytes field, skipped when empty.
#[inline]
pub fn put_bytes_field(buf: &mut Vec<u8>, field: u32, v: &[u8]) {
    if !v.is_empty() {
        put_tag(buf, field, 2);
        put_varint(buf, v.len() as u64);
        buf.extend_from_slice(v);
    }
}

#[inline]
pub fn bytes_field_len(field: u32, len: usize) -> usize {
    if len > 0 { tag_len(field) + sov(len as u64) + len } else { 0 }
}

/// A string or bytes element of a repeated field: always written, even when empty.
#[inline]
pub fn put_bytes_always(buf: &mut Vec<u8>, field: u32, v: &[u8]) {
    put_tag(buf, field, 2);
    put_varint(buf, v.len() as u64);
    buf.extend_from_slice(v);
}

#[inline]
pub fn bytes_always_len(field: u32, len: usize) -> usize {
    tag_len(field) + sov(len as u64) + len
}

/// Tag and length prefix of an embedded message of `len` bytes.
#[inline]
pub fn put_message_header(buf: &mut Vec<u8>, field: u32, len: usize) {
    put_tag(buf, field, 2);
    put_varint(buf, len as u64);
}

#[inline]
pub fn message_len(field: u32, len: usize) -> usize {
    tag_len(field) + sov(len as u64) + len
}

/// Packed doubles, skipped when empty.
pub fn put_packed_doubles(buf: &mut Vec<u8>, field: u32, v: &[f64]) {
    if v.is_empty() {
        return;
    }
    put_tag(buf, field, 2);
    put_varint(buf, (v.len() * 8) as u64);
    for f in v {
        buf.extend_from_slice(&f.to_bits().to_le_bytes());
    }
}

#[inline]
pub fn packed_doubles_len(field: u32, n: usize) -> usize {
    if n > 0 { tag_len(field) + sov((n * 8) as u64) + n * 8 } else { 0 }
}

/// Packed sint64 (zigzag), skipped when empty.
pub fn put_packed_sint64(buf: &mut Vec<u8>, field: u32, v: &[i64]) {
    if v.is_empty() {
        return;
    }
    let n: usize = v.iter().map(|&x| soz(x)).sum();
    put_tag(buf, field, 2);
    put_varint(buf, n as u64);
    for &x in v {
        put_varint(buf, ((x << 1) ^ (x >> 63)) as u64);
    }
}

pub fn packed_sint64_len(field: u32, v: &[i64]) -> usize {
    if v.is_empty() {
        return 0;
    }
    let n: usize = v.iter().map(|&x| soz(x)).sum();
    tag_len(field) + sov(n as u64) + n
}

/// Packed uint32, skipped when empty.
pub fn put_packed_uint32(buf: &mut Vec<u8>, field: u32, v: &[u32]) {
    if v.is_empty() {
        return;
    }
    let n: usize = v.iter().map(|&x| sov(x as u64)).sum();
    put_tag(buf, field, 2);
    put_varint(buf, n as u64);
    for &x in v {
        put_varint(buf, x as u64);
    }
}

pub fn packed_uint32_len(field: u32, v: &[u32]) -> usize {
    if v.is_empty() {
        return 0;
    }
    let n: usize = v.iter().map(|&x| sov(x as u64)).sum();
    tag_len(field) + sov(n as u64) + n
}

/// Packed enums (int32 as varint), skipped when empty.
pub fn put_packed_int32(buf: &mut Vec<u8>, field: u32, v: &[i32]) {
    if v.is_empty() {
        return;
    }
    let n: usize = v.iter().map(|&x| sov(x as i64 as u64)).sum();
    put_tag(buf, field, 2);
    put_varint(buf, n as u64);
    for &x in v {
        put_varint(buf, x as i64 as u64);
    }
}

pub fn packed_int32_len(field: u32, v: &[i32]) -> usize {
    if v.is_empty() {
        return 0;
    }
    let n: usize = v.iter().map(|&x| sov(x as i64 as u64)).sum();
    tag_len(field) + sov(n as u64) + n
}

// ---------------------------------------------------------------- decoding

/// A protobuf reader over one message.
pub struct Decoder<'a> {
    pub b: &'a [u8],
    pub i: usize,
}

impl<'a> Decoder<'a> {
    #[inline]
    pub fn new(b: &'a [u8]) -> Decoder<'a> {
        Decoder { b, i: 0 }
    }

    #[inline]
    pub fn done(&self) -> bool {
        self.i >= self.b.len()
    }

    #[inline]
    pub fn varint(&mut self) -> Result<u64> {
        let mut v: u64 = 0;
        let mut shift = 0u32;
        loop {
            if shift >= 64 {
                return Err(DecodeError::overflow());
            }
            let Some(&b) = self.b.get(self.i) else {
                return Err(DecodeError::eof());
            };
            self.i += 1;
            v |= ((b & 0x7f) as u64) << shift;
            if b < 0x80 {
                return Ok(v);
            }
            shift += 7;
        }
    }

    /// Reads a field key. `msg` is the Go message name used in the error texts.
    #[inline]
    pub fn key(&mut self, msg: &str) -> Result<(u32, u8, usize)> {
        let pre = self.i;
        let wire = self.varint()?;
        let field = (wire >> 3) as i32;
        let wt = (wire & 7) as u8;
        if wt == 4 {
            return Err(DecodeError(Cow::Owned(format!("proto: {msg}: wiretype end group for non-group"))));
        }
        if field <= 0 {
            return Err(DecodeError(Cow::Owned(format!("proto: {msg}: illegal tag {field} (wire type {wire})"))));
        }
        Ok((field as u32, wt, pre))
    }

    /// A length-delimited payload (gogo's `msglen`/`postIndex` checks).
    #[inline]
    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.varint()? as i64;
        if len < 0 {
            return Err(DecodeError::invalid_length());
        }
        let post = self.i as i64 + len;
        if post < 0 {
            return Err(DecodeError::invalid_length());
        }
        if post as usize > self.b.len() {
            return Err(DecodeError::eof());
        }
        let s = &self.b[self.i..post as usize];
        self.i = post as usize;
        Ok(s)
    }

    #[inline]
    pub fn string(&mut self) -> Result<String> {
        Ok(bytes_to_string(self.bytes()?))
    }

    #[inline]
    pub fn fixed64(&mut self) -> Result<u64> {
        if self.i + 8 > self.b.len() {
            return Err(DecodeError::eof());
        }
        let mut a = [0u8; 8];
        a.copy_from_slice(&self.b[self.i..self.i + 8]);
        self.i += 8;
        Ok(u64::from_le_bytes(a))
    }

    #[inline]
    pub fn double(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.fixed64()?))
    }

    /// Skips the field that starts at `pre` (gogo's `default:` branch with `skip<File>`).
    pub fn skip(&mut self, pre: usize) -> Result<()> {
        self.i = pre;
        let n = skip_field(&self.b[self.i..])?;
        if self.i + n > self.b.len() {
            return Err(DecodeError::eof());
        }
        self.i += n;
        Ok(())
    }

    /// Checks the wire type of a known field.
    #[inline]
    pub fn expect(&self, wt: u8, want: u8, field: &str) -> Result<()> {
        if wt != want { Err(DecodeError::wrong_wire_type(wt, field)) } else { Ok(()) }
    }

    /// A repeated varint field, packed (wire type 2) or not (wire type 0).
    pub fn repeated_varint(&mut self, wt: u8, field: &str, mut f: impl FnMut(u64)) -> Result<()> {
        if wt == 0 {
            f(self.varint()?);
            Ok(())
        } else if wt == 2 {
            let b = self.bytes()?;
            let mut d = Decoder::new(b);
            while !d.done() {
                f(d.varint()?);
            }
            Ok(())
        } else {
            Err(DecodeError::wrong_wire_type(wt, field))
        }
    }

    /// A repeated double field, packed (wire type 2) or not (wire type 1).
    pub fn repeated_double(&mut self, wt: u8, field: &str, out: &mut Vec<f64>) -> Result<()> {
        if wt == 1 {
            out.push(self.double()?);
            Ok(())
        } else if wt == 2 {
            let b = self.bytes()?;
            if out.is_empty() {
                out.reserve(b.len() / 8);
            }
            let mut d = Decoder::new(b);
            while !d.done() {
                out.push(d.double()?);
            }
            Ok(())
        } else {
            Err(DecodeError::wrong_wire_type(wt, field))
        }
    }
}

/// Go's `string(b)`. Go strings may hold invalid UTF-8; Rust strings may not, so invalid bytes become U+FFFD.
#[inline]
pub fn bytes_to_string(b: &[u8]) -> String {
    match std::str::from_utf8(b) {
        Ok(s) => s.to_string(),
        Err(_) => String::from_utf8_lossy(b).into_owned(),
    }
}

/// gogo `skip<File>`: the length of the field at the start of `b`.
pub fn skip_field(b: &[u8]) -> Result<usize> {
    let l = b.len();
    let mut i = 0usize;
    let mut depth = 0i32;
    while i < l {
        let mut d = Decoder { b, i };
        let wire = d.varint()?;
        i = d.i;
        let wt = wire & 7;
        match wt {
            0 => {
                let mut shift = 0;
                loop {
                    if shift >= 64 {
                        return Err(DecodeError::overflow());
                    }
                    if i >= l {
                        return Err(DecodeError::eof());
                    }
                    i += 1;
                    if b[i - 1] < 0x80 {
                        break;
                    }
                    shift += 7;
                }
            }
            1 => i += 8,
            2 => {
                let mut d = Decoder { b, i };
                let len = d.varint()? as i64;
                i = d.i;
                if len < 0 {
                    return Err(DecodeError::invalid_length());
                }
                i += len as usize;
            }
            3 => depth += 1,
            4 => {
                if depth == 0 {
                    return Err(DecodeError(Cow::Borrowed(ERR_UNEXPECTED_END_OF_GROUP)));
                }
                depth -= 1;
            }
            5 => i += 4,
            _ => return Err(DecodeError(Cow::Owned(format!("proto: illegal wireType {wt}")))),
        }
        if depth == 0 {
            return Ok(i);
        }
    }
    Err(DecodeError::eof())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sov_matches_go() {
        assert_eq!(sov(0), 1);
        assert_eq!(sov(127), 1);
        assert_eq!(sov(128), 2);
        assert_eq!(sov(u64::MAX), 10);
    }
}
