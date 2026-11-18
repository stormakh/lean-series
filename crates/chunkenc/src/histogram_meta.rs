//! Histogram chunk layout and bucket insert helpers (port of `tsdb/chunkenc/histogram_meta.go`).

use crate::Error;
use crate::bstream::{Bstream, BstreamReader};
use crate::histogram::CounterResetHeader;
use crate::varbit::{put_varbit_int, put_varbit_uint, read_varbit_int, read_varbit_uint};
use model::histogram::{CounterResetHint, Span, is_custom_buckets_schema};
use std::sync::Arc;

/// Writes the layout (zero threshold, schema, spans, custom bounds) of the first histogram of a chunk.
pub fn write_histogram_chunk_layout(
    b: &mut Bstream,
    schema: i32,
    zero_threshold: f64,
    positive_spans: &[Span],
    negative_spans: &[Span],
    custom_values: &[f64],
) {
    put_zero_threshold(b, zero_threshold);
    put_varbit_int(b, schema as i64);
    put_histogram_chunk_layout_spans(b, positive_spans);
    put_histogram_chunk_layout_spans(b, negative_spans);
    if is_custom_buckets_schema(schema) {
        put_histogram_chunk_layout_custom_bounds(b, custom_values);
    }
}

/// The decoded chunk layout.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HistogramChunkLayout {
    pub schema: i32,
    pub zero_threshold: f64,
    pub positive_spans: Vec<Span>,
    pub negative_spans: Vec<Span>,
    /// `None` when the chunk has no custom bounds (Go nil).
    pub custom_values: Option<Arc<[f64]>>,
}

/// Reads the chunk layout into `l` (reusing its span allocations).
pub fn read_histogram_chunk_layout_into(b: &mut BstreamReader<'_>, l: &mut HistogramChunkLayout) -> Result<(), Error> {
    l.zero_threshold = read_zero_threshold(b)?;
    l.schema = read_varbit_int(b)? as i32;
    read_histogram_chunk_layout_spans(b, &mut l.positive_spans)?;
    read_histogram_chunk_layout_spans(b, &mut l.negative_spans)?;
    l.custom_values = None;
    if is_custom_buckets_schema(l.schema) {
        l.custom_values = read_histogram_chunk_layout_custom_bounds(b)?;
    }
    Ok(())
}

/// Reads the chunk layout.
pub fn read_histogram_chunk_layout(b: &mut BstreamReader<'_>) -> Result<HistogramChunkLayout, Error> {
    let mut l = HistogramChunkLayout::default();
    read_histogram_chunk_layout_into(b, &mut l)?;
    Ok(l)
}

fn put_histogram_chunk_layout_spans(b: &mut Bstream, spans: &[Span]) {
    put_varbit_uint(b, spans.len() as u64);
    for s in spans {
        put_varbit_uint(b, s.length as u64);
        put_varbit_int(b, s.offset as i64);
    }
}

/// Reads spans into `spans` (cleared first). On error `spans` is cleared (Go returns nil).
pub fn read_histogram_chunk_layout_spans(b: &mut BstreamReader<'_>, spans: &mut Vec<Span>) -> Result<(), Error> {
    spans.clear();
    let r = (|| {
        let num = read_varbit_uint(b)?;
        for _ in 0..num as i64 {
            let length = read_varbit_uint(b)?;
            let offset = read_varbit_int(b)?;
            spans.push(Span { length: length as u32, offset: offset as i32 });
        }
        Ok(())
    })();
    if r.is_err() {
        spans.clear();
    }
    r
}

fn put_histogram_chunk_layout_custom_bounds(b: &mut Bstream, custom_values: &[f64]) {
    put_varbit_uint(b, custom_values.len() as u64);
    for &bound in custom_values {
        put_custom_bound(b, bound);
    }
}

fn read_histogram_chunk_layout_custom_bounds(b: &mut BstreamReader<'_>) -> Result<Option<Arc<[f64]>>, Error> {
    let num = read_varbit_uint(b)?;
    let mut cv: Vec<f64> = Vec::new();
    for _ in 0..num as i64 {
        cv.push(read_custom_bound(b)?);
    }
    Ok(if cv.is_empty() { None } else { Some(Arc::from(cv)) })
}

/// Go `math.Frexp`.
pub(crate) fn frexp(f: f64) -> (f64, i32) {
    if f == 0.0 || f.is_infinite() || f.is_nan() {
        return (f, 0);
    }
    let (mut f, mut exp) = (f, 0i32);
    if f.abs() < f64::MIN_POSITIVE {
        f *= (1u64 << 52) as f64;
        exp = -52;
    }
    let mut x = f.to_bits();
    exp += ((x >> 52) & 0x7ff) as i32 - 1023 + 1;
    x &= !(0x7ffu64 << 52);
    x |= ((-1i64 + 1023) as u64) << 52;
    (f64::from_bits(x), exp)
}

/// Writes the zero threshold: 0 as byte 0, 2^-243..2^10 powers of two in one byte, anything else as 255 + 64 bits.
pub fn put_zero_threshold(b: &mut Bstream, threshold: f64) {
    if threshold == 0.0 {
        b.write_byte(0);
        return;
    }
    let (frac, exp) = frexp(threshold);
    if frac != 0.5 || !(-242..=11).contains(&exp) {
        b.write_byte(255);
        b.write_bits(threshold.to_bits(), 64);
        return;
    }
    b.write_byte((exp + 243) as u8);
}

pub fn read_zero_threshold(br: &mut BstreamReader<'_>) -> Result<f64, Error> {
    let b = br.read_byte().ok_or(Error::Eof)?;
    match b {
        0 => Ok(0.0),
        255 => Ok(f64::from_bits(br.read_bits(64).ok_or(Error::Eof)?)),
        _ => Ok(model::histogram::ldexp(0.5, b as i64 - 243)),
    }
}

/// Go `math.Round` (half away from zero) is Rust's `f64::round`.
fn is_whole_when_multiplied(f: f64) -> bool {
    let i = go_float_to_uint((f * 1000.0).round());
    let out = i as f64 / 1000.0;
    f == out
}

/// Go `uint(f)` for the values this code sees (amd64 semantics for out-of-range values: CVTTSD2SI based).
fn go_float_to_uint(f: f64) -> u64 {
    // On amd64, Go converts float64 to uint64 via: if f < 2^63 { int64(f) } else { int64(f - 2^63) ^ (1<<63) }.
    // int64() of NaN/out of range gives 0x8000000000000000.
    let two63 = 9223372036854775808.0f64;
    if f < two63 {
        cvttsd2si(f) as u64
    } else {
        (cvttsd2si(f - two63) as u64) ^ (1u64 << 63)
    }
}

fn cvttsd2si(f: f64) -> i64 {
    if f.is_nan() || f >= 9223372036854775808.0 || f < -9223372036854775808.0 { i64::MIN } else { f as i64 }
}

/// Writes a custom bucket bound: values that are whole multiples of 0.001 in [0, 33554.43] as a varbit uint of
/// bound*1000+1, anything else as a 0 bit and the 64 float bits.
pub fn put_custom_bound(b: &mut Bstream, f: f64) {
    let tf = f * 1000.0;
    // tf < 0 or tf > 33554430 (2^25 - 2) is not whole; also NaN goes to the float path.
    if tf < 0.0 || tf > 33554430.0 || !is_whole_when_multiplied(f) {
        b.write_bit(false);
        b.write_bits(f.to_bits(), 64);
        return;
    }
    put_varbit_uint(b, go_float_to_uint(tf.round()).wrapping_add(1));
}

pub fn read_custom_bound(br: &mut BstreamReader<'_>) -> Result<f64, Error> {
    let b = read_varbit_uint(br)?;
    match b {
        0 => Ok(f64::from_bits(br.read_bits(64).ok_or(Error::Eof)?)),
        _ => Ok((b - 1) as f64 / 1000.0),
    }
}

/// Iterates over the bucket indices described by spans.
#[derive(Clone, Debug)]
pub struct BucketIterator<'a> {
    spans: &'a [Span],
    span: usize,
    bucket: i64,
    idx: i64,
}

impl<'a> BucketIterator<'a> {
    pub fn new(spans: &'a [Span]) -> BucketIterator<'a> {
        let mut b = BucketIterator { spans, span: 0, bucket: -1, idx: -1 };
        if !spans.is_empty() {
            b.idx += spans[0].offset as i64;
        }
        b
    }

    /// The next bucket index, or None.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<i64> {
        if self.span >= self.spans.len() {
            return None;
        }
        if self.bucket < self.spans[self.span].length as i64 - 1 {
            // Try to move within same span.
            self.bucket += 1;
            self.idx += 1;
            return Some(self.idx);
        }
        while self.span < self.spans.len() - 1 {
            // Try to move from one span to the next.
            self.span += 1;
            self.idx += (self.spans[self.span].offset.wrapping_add(1)) as i64;
            self.bucket = 0;
            if self.spans[self.span].length == 0 {
                self.idx -= 1;
                continue;
            }
            return Some(self.idx);
        }
        None
    }
}

/// An insert of `num` empty buckets at position `pos` of the bucket slice (and `bucket_idx`, the bucket index of
/// the first inserted bucket, where known).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Insert {
    pub pos: i64,
    pub num: i64,
    pub bucket_idx: i64,
}

impl Insert {
    pub const fn new(pos: i64, num: i64, bucket_idx: i64) -> Insert {
        Insert { pos, num, bucket_idx }
    }
}

fn add_bucket(merged: &mut Vec<Span>, last_bucket: &mut i64, b: i64) {
    let mut offset = b - *last_bucket - 1;
    if offset == 0 && !merged.is_empty() {
        merged.last_mut().unwrap().length += 1;
    } else {
        if merged.is_empty() {
            offset += 1;
        }
        merged.push(Span { offset: offset as i32, length: 1 });
    }
    *last_bucket = b;
}

/// Compares two span layouts and returns the forward inserts (into a), the backward inserts (into b) and the
/// merged spans of both (used for gauge histograms).
pub fn expand_spans_both_ways(a: &[Span], b: &[Span]) -> (Vec<Insert>, Vec<Insert>, Vec<Span>) {
    let mut ai = BucketIterator::new(a);
    let mut bi = BucketIterator::new(b);
    let mut f_inserts: Vec<Insert> = Vec::new();
    let mut b_inserts: Vec<Insert> = Vec::new();
    let mut merged: Vec<Span> = Vec::new();
    let mut last_bucket: i64 = 0;
    let mut f_inter = Insert::default();
    let mut b_inter = Insert::default();

    let mut av = ai.next();
    let mut bv = bi.next();
    loop {
        match (av, bv) {
            (Some(a), Some(b)) => {
                if a == b {
                    if f_inter.num > 0 {
                        f_inserts.push(f_inter);
                        f_inter.num = 0;
                    }
                    if b_inter.num > 0 {
                        b_inserts.push(b_inter);
                        b_inter.num = 0;
                    }
                    add_bucket(&mut merged, &mut last_bucket, a);
                    av = ai.next();
                    bv = bi.next();
                    f_inter.pos += 1;
                    b_inter.pos += 1;
                } else if a < b {
                    b_inter.num += 1;
                    if f_inter.num > 0 {
                        f_inserts.push(f_inter);
                        f_inter.num = 0;
                    }
                    add_bucket(&mut merged, &mut last_bucket, a);
                    f_inter.pos += 1;
                    av = ai.next();
                } else {
                    f_inter.num += 1;
                    if b_inter.num > 0 {
                        b_inserts.push(b_inter);
                        b_inter.num = 0;
                    }
                    add_bucket(&mut merged, &mut last_bucket, b);
                    b_inter.pos += 1;
                    bv = bi.next();
                }
            }
            (Some(a), None) => {
                b_inter.num += 1;
                add_bucket(&mut merged, &mut last_bucket, a);
                av = ai.next();
            }
            (None, Some(b)) => {
                f_inter.num += 1;
                add_bucket(&mut merged, &mut last_bucket, b);
                bv = bi.next();
            }
            (None, None) => {
                if f_inter.num > 0 {
                    f_inserts.push(f_inter);
                }
                if b_inter.num > 0 {
                    b_inserts.push(b_inter);
                }
                break;
            }
        }
    }
    (f_inserts, b_inserts, merged)
}

/// Bucket value types for [`insert`].
pub trait BucketValue: Copy + Default + PartialEq + std::fmt::Debug {
    fn add(self, o: Self) -> Self;
    fn neg(self) -> Self;
}

impl BucketValue for i64 {
    fn add(self, o: i64) -> i64 {
        self.wrapping_add(o)
    }
    fn neg(self) -> i64 {
        self.wrapping_neg()
    }
}

impl BucketValue for f64 {
    fn add(self, o: f64) -> f64 {
        self + o
    }
    fn neg(self) -> f64 {
        -self
    }
}

/// Copies `input` into `out` (which must be large enough) and inserts empty buckets as described by `inserts`.
/// With `deltas`, the values are delta encoded and the inserts keep the following deltas correct.
pub fn insert<BV: BucketValue>(input: &[BV], out: &mut [BV], inserts: &[Insert], deltas: bool) {
    let mut oi = 0usize;
    let mut v = BV::default();
    let mut ii = 0usize;
    for (i, &d) in input.iter().enumerate() {
        if ii >= inserts.len() || i as i64 != inserts[ii].pos {
            out[oi] = d;
            oi += 1;
            v = v.add(d);
            continue;
        }
        let mut first_insert = true;
        while ii < inserts.len() && i as i64 == inserts[ii].pos {
            if deltas && first_insert {
                out[oi] = v.neg();
                first_insert = false;
            } else {
                out[oi] = BV::default();
            }
            oi += 1;
            for _ in 1..inserts[ii].num {
                out[oi] = BV::default();
                oi += 1;
            }
            ii += 1;
        }
        if deltas {
            out[oi] = d.add(v);
        } else {
            out[oi] = d;
        }
        oi += 1;
        v = v.add(d);
    }
    while ii < inserts.len() {
        if (inserts[ii].pos as usize) < input.len() {
            panic!("leftover inserts must be after the current buckets");
        }
        out[oi] = if deltas { v.neg() } else { BV::default() };
        oi += 1;
        for _ in 1..inserts[ii].num {
            out[oi] = BV::default();
            oi += 1;
        }
        ii += 1;
        v = BV::default();
    }
}

/// The counter reset hint of the sample number `num_read` in a chunk with header `crh`.
pub fn counter_reset_hint(crh: CounterResetHeader, num_read: u16) -> CounterResetHint {
    if crh == CounterResetHeader::GAUGE_TYPE {
        CounterResetHint::GaugeType
    } else if num_read > 1 {
        // In a counter histogram chunk, there will not be any counter resets after the first histogram.
        CounterResetHint::NotCounterReset
    } else {
        // Sample 1 (or 0 before reading): the chunk header says if it is a counter reset. Return unknown so
        // that the counter reset detection of PromQL looks at the previous sample.
        CounterResetHint::UnknownCounterReset
    }
}

/// Returns the spans with the buckets of `inserts` added.
pub fn adjust_for_inserts(spans: &[Span], inserts: &[Insert]) -> Vec<Span> {
    if inserts.is_empty() {
        return spans.to_vec();
    }
    let mut it = BucketIterator::new(spans);
    let mut merged: Vec<Span> = Vec::new();
    let mut last_bucket: i64 = 0;
    let mut i = 0usize;
    let mut insert_idx = inserts[0].bucket_idx;
    let mut insert_num = inserts[0].num;

    let consume_insert = |i: &mut usize, insert_idx: &mut i64, insert_num: &mut i64| {
        *insert_num -= 1;
        if *insert_num == 0 {
            *i += 1;
            if *i < inserts.len() {
                *insert_idx = inserts[*i].bucket_idx;
                *insert_num = inserts[*i].num;
            }
        } else {
            *insert_idx += 1;
        }
    };

    let mut bucket = it.next();
    while let Some(bk) = bucket {
        if i < inserts.len() && insert_idx < bk {
            add_bucket(&mut merged, &mut last_bucket, insert_idx);
            consume_insert(&mut i, &mut insert_idx, &mut insert_num);
        } else {
            add_bucket(&mut merged, &mut last_bucket, bk);
            bucket = it.next();
        }
    }
    while i < inserts.len() {
        add_bucket(&mut merged, &mut last_bucket, insert_idx);
        consume_insert(&mut i, &mut insert_idx, &mut insert_num);
    }
    merged
}
