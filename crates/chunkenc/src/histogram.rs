//! Integer histogram chunks (port of `tsdb/chunkenc/histogram.go` and `histogram_st.go`).
//!
//! `HistogramChunkT<false>` is the histogram encoding (2), `HistogramChunkT<true>` the histogram encoding with
//! start timestamps (5). They share all code; the ST variant has another header layout (sample count masked to 14
//! bits, counter reset bits in byte 0, ST header in byte 2) and encodes the ST after each sample.

use crate::Iterator as _;
use crate::bstream::{Bstream, BstreamReader};
use crate::histogram_meta::*;
use crate::st::{HISTOGRAM_ST_SAMPLE_COUNT_MASK, StDecoder, StEncoder, read_st_header};
use crate::varbit::{put_varbit_int, put_varbit_uint, read_varbit_int, read_varbit_uint};
use crate::xor::{put_u16, read_u16, xor_read, xor_write};
use crate::{CHUNK_ALLOCATION_SIZE, ENC_HISTOGRAM, ENC_HISTOGRAM_ST, Encoding, Error, ValueType, is_stale_nan};
use model::histogram::{
    CounterResetHint, CustomValues, EXPONENTIAL_SCHEMA_MAX, EXPONENTIAL_SCHEMA_MAX_RESERVED, FloatHistogram, Histogram,
    Span, custom_bucket_bounds_match, is_custom_buckets_schema, is_known_schema, unknown_schema_error,
};

/// The counter reset bits of a histogram chunk header.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct CounterResetHeader(pub u8);

impl CounterResetHeader {
    /// There was definitely a counter reset between this and the previous chunk.
    pub const COUNTER_RESET: CounterResetHeader = CounterResetHeader(0b1000_0000);
    /// There was definitely no counter reset between this and the previous chunk.
    pub const NOT_COUNTER_RESET: CounterResetHeader = CounterResetHeader(0b0100_0000);
    /// The chunk holds a gauge histogram.
    pub const GAUGE_TYPE: CounterResetHeader = CounterResetHeader(0b1100_0000);
    /// Unknown if there was a counter reset (also the default for chunks before the header existed).
    pub const UNKNOWN_COUNTER_RESET: CounterResetHeader = CounterResetHeader(0);
}

impl std::fmt::Debug for CounterResetHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            CounterResetHeader::COUNTER_RESET => f.write_str("CounterReset"),
            CounterResetHeader::NOT_COUNTER_RESET => f.write_str("NotCounterReset"),
            CounterResetHeader::GAUGE_TYPE => f.write_str("GaugeType"),
            CounterResetHeader::UNKNOWN_COUNTER_RESET => f.write_str("UnknownCounterReset"),
            CounterResetHeader(b) => write!(f, "CounterResetHeader({:#010b})", b),
        }
    }
}

/// Mask of the counter reset bits in the header byte.
pub const COUNTER_RESET_HEADER_MASK: u8 = 0b1100_0000;
pub(crate) const HISTOGRAM_FLAG_POS: usize = 2;
/// Size of the header of histogram chunks.
pub const HISTOGRAM_HEADER_SIZE: usize = 3;

/// Go `CounterResetHintToHeader`.
pub fn counter_reset_hint_to_header(hint: CounterResetHint) -> CounterResetHeader {
    match hint {
        CounterResetHint::CounterReset => CounterResetHeader::COUNTER_RESET,
        CounterResetHint::NotCounterReset => CounterResetHeader::NOT_COUNTER_RESET,
        CounterResetHint::GaugeType => CounterResetHeader::GAUGE_TYPE,
        CounterResetHint::UnknownCounterReset => CounterResetHeader::UNKNOWN_COUNTER_RESET,
    }
}

// Header accessors for both layouts (shared with the float histogram chunks).

#[inline]
pub(crate) fn header_num_samples<const ST: bool>(b: &[u8]) -> u16 {
    if ST { read_u16(b) & HISTOGRAM_ST_SAMPLE_COUNT_MASK } else { read_u16(b) }
}

#[inline]
pub(crate) fn header_counter_reset<const ST: bool>(b: &[u8]) -> CounterResetHeader {
    let pos = if ST { 0 } else { HISTOGRAM_FLAG_POS };
    CounterResetHeader(b[pos] & COUNTER_RESET_HEADER_MASK)
}

#[inline]
pub(crate) fn set_header_counter_reset<const ST: bool>(b: &mut [u8], cr: CounterResetHeader) {
    let pos = if ST { 0 } else { HISTOGRAM_FLAG_POS };
    b[pos] = (b[pos] & !COUNTER_RESET_HEADER_MASK) | (cr.0 & COUNTER_RESET_HEADER_MASK);
}

#[inline]
pub(crate) fn set_header_num_samples<const ST: bool>(b: &mut [u8], num: usize) {
    if ST {
        let cr_bits = (b[0] & COUNTER_RESET_HEADER_MASK) as u16;
        put_u16(b, (cr_bits << 8) | ((num as u16) & HISTOGRAM_ST_SAMPLE_COUNT_MASK));
    } else {
        put_u16(b, num as u16);
    }
}

pub(crate) fn count_spans(spans: &[Span]) -> usize {
    spans.iter().map(|s| s.length as usize).sum()
}

pub(crate) fn new_histogram_stream() -> Vec<u8> {
    let mut stream = Vec::with_capacity(CHUNK_ALLOCATION_SIZE);
    stream.resize(HISTOGRAM_HEADER_SIZE, 0);
    stream
}

/// A chunk holding integer histograms. `ST` selects the start timestamp variant.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HistogramChunkT<const ST: bool> {
    pub b: Bstream,
}

/// The histogram chunk (encoding 2).
pub type HistogramChunk = HistogramChunkT<false>;
/// The histogram chunk with start timestamps (encoding 5).
pub type HistogramStChunk = HistogramChunkT<true>;

impl<const ST: bool> HistogramChunkT<ST> {
    /// A new empty chunk (Go `NewHistogramChunk` / `NewHistogramSTChunk`).
    pub fn new() -> Self {
        HistogramChunkT { b: Bstream { stream: new_histogram_stream(), count: 0 } }
    }

    pub fn from_bstream(b: Bstream) -> Self {
        HistogramChunkT { b }
    }

    pub fn reset(&mut self, stream: Vec<u8>) {
        self.b.reset(stream);
    }

    pub fn encoding(&self) -> Encoding {
        if ST { ENC_HISTOGRAM_ST } else { ENC_HISTOGRAM }
    }

    pub fn bytes(&self) -> &[u8] {
        self.b.bytes()
    }

    pub fn num_samples(&self) -> usize {
        header_num_samples::<ST>(self.b.bytes()) as usize
    }

    /// The counter reset bits of the chunk header.
    pub fn get_counter_reset_header(&self) -> CounterResetHeader {
        header_counter_reset::<ST>(self.b.bytes())
    }

    pub fn compact(&mut self) {
        crate::compact_stream(&mut self.b.stream);
    }

    /// An appender whose state continues after the last sample of the chunk.
    pub fn appender(&mut self) -> Result<HistogramAppenderT<ST>, Error> {
        if self.b.stream.len() == HISTOGRAM_HEADER_SIZE {
            return Ok(HistogramAppenderT::new_empty(self.b.bytes()));
        }
        let mut it = HistogramIteratorT::<ST>::new(self.b.bytes());
        while it.next() == ValueType::Histogram {}
        if let Some(e) = it.err {
            return Err(e);
        }
        let valid = it.br.valid;
        let num_samples = header_num_samples::<ST>(self.b.bytes());
        let crh = header_counter_reset::<ST>(self.b.bytes());
        let a = HistogramAppenderT {
            schema: it.layout.schema,
            z_threshold: it.layout.zero_threshold,
            p_spans: it.layout.positive_spans,
            n_spans: it.layout.negative_spans,
            custom_values: it.layout.custom_values,
            t: it.t,
            cnt: it.cnt,
            z_cnt: it.z_cnt,
            t_delta: it.t_delta,
            cnt_delta: it.cnt_delta,
            z_cnt_delta: it.z_cnt_delta,
            p_buckets: it.p_buckets,
            n_buckets: it.n_buckets,
            p_buckets_delta: it.p_buckets_delta,
            n_buckets_delta: it.n_buckets_delta,
            sum: it.sum,
            leading: it.leading,
            trailing: it.trailing,
            num_samples,
            crh,
            st_enc: StEncoder {
                st: it.st_dec.st,
                st_diff: it.st_dec.st_diff,
                first_st_known: it.st_dec.first_st_known,
                first_st_change_on: it.st_dec.first_st_change_on,
            },
        };
        if ST {
            // Go: c.b.count = it.br.valid.
            self.b.count = valid;
        }
        Ok(a)
    }

    pub fn iterator(&self) -> HistogramIteratorT<'_, ST> {
        HistogramIteratorT::new(self.b.bytes())
    }
}

crate::impl_chunk!(HistogramChunkT<false>, Histogram);
crate::impl_chunk!(HistogramChunkT<true>, HistogramSt);

/// The result of `appendable`: forward positive and negative inserts, backward positive and negative inserts,
/// okToAppend and the counter reset header to use.
pub type HistogramAppendableResult = (Vec<Insert>, Vec<Insert>, Vec<Insert>, Vec<Insert>, bool, CounterResetHeader);

/// The result of `appendable_gauge`: forward and backward inserts, merged positive and negative spans, okToAppend.
pub type AppendableGaugeResult = (Vec<Insert>, Vec<Insert>, Vec<Insert>, Vec<Insert>, Vec<Span>, Vec<Span>, bool);

/// An appender that can tell how a histogram fits after its last sample (Go `histogramAppendable`).
pub trait HistogramAppendable {
    fn appendable(&self, h: &Histogram) -> HistogramAppendableResult;
}

/// The encoder state of a histogram chunk.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HistogramAppenderT<const ST: bool> {
    pub(crate) schema: i32,
    pub(crate) z_threshold: f64,
    pub(crate) p_spans: Vec<Span>,
    pub(crate) n_spans: Vec<Span>,
    pub(crate) custom_values: CustomValues,
    pub(crate) t: i64,
    pub(crate) cnt: u64,
    pub(crate) z_cnt: u64,
    pub(crate) t_delta: i64,
    pub(crate) cnt_delta: i64,
    pub(crate) z_cnt_delta: i64,
    pub(crate) p_buckets: Vec<i64>,
    pub(crate) n_buckets: Vec<i64>,
    pub(crate) p_buckets_delta: Vec<i64>,
    pub(crate) n_buckets_delta: Vec<i64>,
    pub(crate) sum: f64,
    pub(crate) leading: u8,
    pub(crate) trailing: u8,
    /// Mirror of the sample count in the chunk header (so that `appendable` works without the chunk).
    pub(crate) num_samples: u16,
    /// Mirror of the counter reset bits in the chunk header.
    pub(crate) crh: CounterResetHeader,
    pub(crate) st_enc: StEncoder,
}

/// The appender of the histogram chunk.
pub type HistogramAppender = HistogramAppenderT<false>;
/// The appender of the histogram chunk with start timestamps.
pub type HistogramStAppender = HistogramAppenderT<true>;

impl<const ST: bool> HistogramAppendable for HistogramAppenderT<ST> {
    fn appendable(&self, h: &Histogram) -> HistogramAppendableResult {
        HistogramAppenderT::appendable(self, h)
    }
}

impl<const ST: bool> HistogramAppenderT<ST> {
    fn new_empty(b: &[u8]) -> Self {
        HistogramAppenderT {
            t: i64::MIN,
            leading: 0xff,
            num_samples: header_num_samples::<ST>(b),
            crh: header_counter_reset::<ST>(b),
            ..Default::default()
        }
    }

    /// The counter reset bits of the chunk header.
    pub fn get_counter_reset_header(&self) -> CounterResetHeader {
        self.crh
    }

    /// Sets the counter reset bits of the chunk header.
    pub fn set_counter_reset_header(&mut self, c: &mut HistogramChunkT<ST>, cr: CounterResetHeader) {
        set_header_counter_reset::<ST>(c.b.bytes_mut(), cr);
        self.crh = CounterResetHeader(cr.0 & COUNTER_RESET_HEADER_MASK);
    }

    /// The number of samples in the chunk.
    pub fn num_samples(&self) -> usize {
        self.num_samples as usize
    }

    fn set_num_samples(&mut self, c: &mut HistogramChunkT<ST>, num: usize) {
        set_header_num_samples::<ST>(c.b.bytes_mut(), num);
        self.num_samples = if ST { (num as u16) & HISTOGRAM_ST_SAMPLE_COUNT_MASK } else { num as u16 };
    }

    /// Checks if `h` can be appended without a new chunk (Go `appendable`). See the Go documentation for the rules.
    pub fn appendable(&self, h: &Histogram) -> HistogramAppendableResult {
        let none = |ok: bool, crh: CounterResetHeader| (Vec::new(), Vec::new(), Vec::new(), Vec::new(), ok, crh);
        if self.num_samples > 0 && self.crh == CounterResetHeader::GAUGE_TYPE {
            return none(false, CounterResetHeader::NOT_COUNTER_RESET);
        }
        if h.counter_reset_hint == CounterResetHint::CounterReset {
            return none(false, CounterResetHeader::COUNTER_RESET);
        }
        if is_stale_nan(h.sum) {
            // This is a stale sample whose buckets and spans don't matter.
            return none(true, CounterResetHeader::NOT_COUNTER_RESET);
        }
        if is_stale_nan(self.sum) {
            // If the last sample was stale, then we can only accept stale samples in this chunk.
            return none(false, CounterResetHeader::UNKNOWN_COUNTER_RESET);
        }
        if h.count < self.cnt {
            return none(false, CounterResetHeader::COUNTER_RESET);
        }
        if h.schema != self.schema || h.zero_threshold != self.z_threshold {
            return none(false, CounterResetHeader::UNKNOWN_COUNTER_RESET);
        }
        if is_custom_buckets_schema(h.schema)
            && !custom_bucket_bounds_match(h.custom_values_slice(), model_cv(&self.custom_values))
        {
            return none(false, CounterResetHeader::COUNTER_RESET);
        }
        if h.zero_count < self.z_cnt {
            return none(false, CounterResetHeader::COUNTER_RESET);
        }
        let (pf, pb) =
            match expand_int_spans_and_buckets(&self.p_spans, &h.positive_spans, &self.p_buckets, &h.positive_buckets) {
                Some(r) => r,
                None => return none(false, CounterResetHeader::COUNTER_RESET),
            };
        let (nf, nb) =
            match expand_int_spans_and_buckets(&self.n_spans, &h.negative_spans, &self.n_buckets, &h.negative_buckets) {
                Some(r) => r,
                None => return (pf, Vec::new(), pb, Vec::new(), false, CounterResetHeader::COUNTER_RESET),
            };
        (pf, nf, pb, nb, true, CounterResetHeader::NOT_COUNTER_RESET)
    }

    /// Like `appendable` for gauge histograms (Go `appendableGauge`).
    pub fn appendable_gauge(&self, h: &Histogram) -> AppendableGaugeResult {
        let none = |ok: bool| (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), ok);
        if self.num_samples > 0 && self.crh != CounterResetHeader::GAUGE_TYPE {
            return none(false);
        }
        if is_stale_nan(h.sum) {
            return none(true);
        }
        if is_stale_nan(self.sum) {
            return none(false);
        }
        if h.schema != self.schema || h.zero_threshold != self.z_threshold {
            return none(false);
        }
        if is_custom_buckets_schema(h.schema)
            && !custom_bucket_bounds_match(h.custom_values_slice(), model_cv(&self.custom_values))
        {
            return none(false);
        }
        let (pf, pb, ps) = expand_spans_both_ways(&self.p_spans, &h.positive_spans);
        let (nf, nb, ns) = expand_spans_both_ways(&self.n_spans, &h.negative_spans);
        (pf, nf, pb, nb, ps, ns, true)
    }

    /// Writes one histogram (Go `appendHistogram`); returns num + 1.
    fn append_histogram_raw(&mut self, b: &mut Bstream, num: usize, t: i64, h: &Histogram) -> usize {
        let stale;
        let h = if is_stale_nan(h.sum) {
            // Emptying out other fields to write no buckets, and an empty layout in case of first histogram in
            // the chunk.
            stale = Histogram { sum: h.sum, ..Default::default() };
            &stale
        } else {
            h
        };
        let (mut t_delta, mut cnt_delta, mut z_cnt_delta) = (0i64, 0i64, 0i64);
        if num == 0 {
            // The first append gets the privilege to dictate the layout but it's also responsible for encoding it
            // into the chunk!
            write_histogram_chunk_layout(
                b,
                h.schema,
                h.zero_threshold,
                &h.positive_spans,
                &h.negative_spans,
                h.custom_values_slice(),
            );
            self.schema = h.schema;
            self.z_threshold = h.zero_threshold;
            self.p_spans.clear();
            self.p_spans.extend_from_slice(&h.positive_spans);
            self.n_spans.clear();
            self.n_spans.extend_from_slice(&h.negative_spans);
            self.custom_values = if h.custom_values_slice().is_empty() { None } else { h.custom_values.clone() };
            let (np, nn) = (count_spans(&h.positive_spans), count_spans(&h.negative_spans));
            self.p_buckets.clear();
            self.p_buckets.resize(np, 0);
            self.p_buckets_delta.clear();
            self.p_buckets_delta.resize(np, 0);
            self.n_buckets.clear();
            self.n_buckets.resize(nn, 0);
            self.n_buckets_delta.clear();
            self.n_buckets_delta.resize(nn, 0);

            // Now store the actual data.
            put_varbit_int(b, t);
            put_varbit_uint(b, h.count);
            put_varbit_uint(b, h.zero_count);
            b.write_bits(h.sum.to_bits(), 64);
            for &v in &h.positive_buckets {
                put_varbit_int(b, v);
            }
            for &v in &h.negative_buckets {
                put_varbit_int(b, v);
            }
        } else {
            // The case for the 2nd sample and beyond.
            t_delta = t.wrapping_sub(self.t);
            cnt_delta = (h.count as i64).wrapping_sub(self.cnt as i64);
            z_cnt_delta = (h.zero_count as i64).wrapping_sub(self.z_cnt as i64);

            let t_dod = t_delta.wrapping_sub(self.t_delta);
            let mut cnt_dod = cnt_delta.wrapping_sub(self.cnt_delta);
            let mut z_cnt_dod = z_cnt_delta.wrapping_sub(self.z_cnt_delta);
            if is_stale_nan(h.sum) {
                cnt_dod = 0;
                z_cnt_dod = 0;
            }
            put_varbit_int(b, t_dod);
            put_varbit_int(b, cnt_dod);
            put_varbit_int(b, z_cnt_dod);

            xor_write(b, h.sum, self.sum, &mut self.leading, &mut self.trailing);

            for (i, &v) in h.positive_buckets.iter().enumerate() {
                let delta = v.wrapping_sub(self.p_buckets[i]);
                let dod = delta.wrapping_sub(self.p_buckets_delta[i]);
                put_varbit_int(b, dod);
                self.p_buckets_delta[i] = delta;
            }
            for (i, &v) in h.negative_buckets.iter().enumerate() {
                let delta = v.wrapping_sub(self.n_buckets[i]);
                let dod = delta.wrapping_sub(self.n_buckets_delta[i]);
                put_varbit_int(b, dod);
                self.n_buckets_delta[i] = delta;
            }
        }

        self.t = t;
        self.cnt = h.count;
        self.z_cnt = h.zero_count;
        self.t_delta = t_delta;
        self.cnt_delta = cnt_delta;
        self.z_cnt_delta = z_cnt_delta;

        let n = self.p_buckets.len().min(h.positive_buckets.len());
        self.p_buckets[..n].copy_from_slice(&h.positive_buckets[..n]);
        let n = self.n_buckets.len().min(h.negative_buckets.len());
        self.n_buckets[..n].copy_from_slice(&h.negative_buckets[..n]);
        // Note that the bucket deltas were already updated above.
        self.sum = h.sum;
        num + 1
    }

    /// Writes one histogram and, for the ST variant, its start timestamp.
    fn append_one(&mut self, c: &mut HistogramChunkT<ST>, num: usize, st: i64, t: i64, h: &Histogram) -> usize {
        let prev_t = self.t;
        let new_num = self.append_histogram_raw(&mut c.b, num, t, h);
        if ST {
            let cur_t = self.t;
            self.st_enc.encode(&mut c.b, new_num as u16, cur_t, prev_t, st);
        }
        new_num
    }

    /// Recodes the chunk with new inserts and spans (Go `recode` / `recodeST`); returns the new chunk and its
    /// appender.
    fn recode(
        &self,
        c: &HistogramChunkT<ST>,
        positive_inserts: &[Insert],
        negative_inserts: &[Insert],
        positive_spans: &[Span],
        negative_spans: &[Span],
    ) -> (HistogramChunkT<ST>, HistogramAppenderT<ST>) {
        // This is a very slow path, but it should only happen if the chunk is getting a new layout.
        let byts = c.b.bytes();
        let mut it = HistogramIteratorT::<ST>::new(byts);
        let mut hc = HistogramChunkT::<ST>::new();
        let mut happ = match hc.appender() {
            Ok(a) => a,
            Err(e) => panic!("{}", e),
        };
        let (np, nn) = (count_spans(positive_spans), count_spans(negative_spans));
        let mut num = happ.num_samples();
        while it.next() == ValueType::Histogram {
            let (t_old, mut h_old) = it.at_histogram(None);
            let st_old = it.at_st();
            h_old.positive_spans = positive_spans.to_vec();
            h_old.negative_spans = negative_spans.to_vec();
            if !positive_inserts.is_empty() {
                let mut out = vec![0i64; np];
                insert(&h_old.positive_buckets, &mut out, positive_inserts, true);
                h_old.positive_buckets = out;
            }
            if !negative_inserts.is_empty() {
                let mut out = vec![0i64; nn];
                insert(&h_old.negative_buckets, &mut out, negative_inserts, true);
                h_old.negative_buckets = out;
            }
            num = happ.append_one(&mut hc, num, st_old, t_old, &h_old);
        }
        happ.set_num_samples(&mut hc, num);
        happ.set_counter_reset_header(&mut hc, header_counter_reset::<ST>(byts));
        (hc, happ)
    }

    /// Adds backward inserts (empty buckets) to `h` (Go `recodeHistogram`).
    fn recode_histogram(h: &mut Histogram, p_backward: &[Insert], n_backward: &[Insert]) {
        if !p_backward.is_empty() {
            let mut out = vec![0i64; count_spans(&h.positive_spans)];
            insert(&h.positive_buckets, &mut out, p_backward, true);
            h.positive_buckets = out;
        }
        if !n_backward.is_empty() {
            let mut out = vec![0i64; count_spans(&h.negative_spans)];
            insert(&h.negative_buckets, &mut out, n_backward, true);
            h.negative_buckets = out;
        }
    }

    fn new_chunk_with(st: i64, t: i64, h: &Histogram, crh: CounterResetHeader) -> (HistogramChunkT<ST>, Self) {
        let mut new_chunk = HistogramChunkT::<ST>::new();
        let mut happ = match new_chunk.appender() {
            Ok(a) => a,
            Err(e) => panic!("{}", e),
        };
        happ.set_counter_reset_header(&mut new_chunk, crh);
        let n = happ.append_one(&mut new_chunk, 0, st, t, h);
        happ.set_num_samples(&mut new_chunk, n);
        (new_chunk, happ)
    }

    /// Appends a histogram to chunk `c` (Go `AppendHistogram`). Returns a new chunk with its appender when the
    /// sample needs a new or recoded chunk, and whether the new chunk is a recoding of `c`. `h` may get new
    /// spans and buckets (backward inserts), as in Go. `prev` (the appender of the previous chunk) is only used
    /// for the first sample, to set the counter reset header.
    #[allow(clippy::type_complexity)]
    pub fn append_histogram(
        &mut self,
        c: &mut HistogramChunkT<ST>,
        prev: Option<&dyn HistogramAppendable>,
        st: i64,
        t: i64,
        h: &mut Histogram,
        append_only: bool,
    ) -> Result<(Option<(HistogramChunkT<ST>, HistogramAppenderT<ST>)>, bool), Error> {
        let num_samples = self.num_samples();
        let max = if ST { HISTOGRAM_ST_SAMPLE_COUNT_MASK as usize } else { u16::MAX as usize };
        if num_samples == max {
            panic!("chunk capacity exceeded");
        }

        if num_samples == 0 {
            // The first append gets the privilege to dictate the layout but it's also responsible for encoding it
            // into the chunk!
            let n = self.append_one(c, num_samples, st, t, h);
            self.set_num_samples(c, n);
            if h.counter_reset_hint == CounterResetHint::GaugeType {
                self.set_counter_reset_header(c, CounterResetHeader::GAUGE_TYPE);
                return Ok((None, false));
            }
            if h.counter_reset_hint == CounterResetHint::CounterReset {
                self.set_counter_reset_header(c, CounterResetHeader::COUNTER_RESET);
            } else if let Some(p) = prev {
                // If this is not the first chunk in the series, check if there was a counter reset.
                let (_, _, _, _, _, counter_reset) = p.appendable(h);
                self.set_counter_reset_header(c, counter_reset);
            }
            return Ok((None, false));
        }

        // Adding counter-like histogram.
        if h.counter_reset_hint != CounterResetHint::GaugeType {
            let (pf, nf, pb, nb, ok_to_append, counter_reset_hint) = self.appendable(h);
            if !ok_to_append || counter_reset_hint != CounterResetHeader::NOT_COUNTER_RESET {
                if append_only {
                    if counter_reset_hint == CounterResetHeader::COUNTER_RESET {
                        return Err(Error::msg("histogram counter reset"));
                    }
                    return Err(Error::msg("histogram schema change"));
                }
                return Ok((Some(Self::new_chunk_with(st, t, h, counter_reset_hint)), false));
            }
            if !pb.is_empty() || !nb.is_empty() {
                // The histogram needs to be expanded to have the extra empty buckets of the chunk.
                if pf.is_empty() && nf.is_empty() {
                    // No new chunks from the histogram, so the spans of the appender can accommodate the new
                    // buckets. However, we need to make a copy in case the input is sharing spans from an iterator.
                    h.positive_spans = self.p_spans.clone();
                    h.negative_spans = self.n_spans.clone();
                } else {
                    // Spans need pre-adjusting to accommodate the new buckets.
                    h.positive_spans = adjust_for_inserts(&h.positive_spans, &pb);
                    h.negative_spans = adjust_for_inserts(&h.negative_spans, &nb);
                }
                Self::recode_histogram(h, &pb, &nb);
            }
            // If there are new buckets in h, we need to recode the current chunk.
            if !pf.is_empty() || !nf.is_empty() {
                if append_only {
                    return Err(Error::msg(format!(
                        "histogram layout change with {} positive and {} negative forwards inserts",
                        pf.len(),
                        nf.len()
                    )));
                }
                let (mut chk, mut happ) = self.recode(c, &pf, &nf, &h.positive_spans, &h.negative_spans);
                let n = happ.num_samples();
                let n = happ.append_one(&mut chk, n, st, t, h);
                happ.set_num_samples(&mut chk, n);
                return Ok((Some((chk, happ)), true));
            }
            // In all other cases, we can append without recoding.
            let n = self.append_one(c, num_samples, st, t, h);
            self.set_num_samples(c, n);
            return Ok((None, false));
        }

        // Adding gauge histogram.
        let (pf, nf, pb, nb, p_merged, n_merged, ok_to_append) = self.appendable_gauge(h);
        if !ok_to_append {
            if append_only {
                return Err(Error::msg("gauge histogram schema change"));
            }
            return Ok((Some(Self::new_chunk_with(st, t, h, CounterResetHeader::GAUGE_TYPE)), false));
        }
        if pb.len() + nb.len() > 0 {
            if append_only {
                return Err(Error::msg(format!(
                    "gauge histogram layout change with {} positive and {} negative backwards inserts",
                    pb.len(),
                    nb.len()
                )));
            }
            h.positive_spans = p_merged;
            h.negative_spans = n_merged;
            Self::recode_histogram(h, &pb, &nb);
        }
        if !pf.is_empty() || !nf.is_empty() {
            if append_only {
                return Err(Error::msg(format!(
                    "gauge histogram layout change with {} positive and {} negative forwards inserts",
                    pf.len(),
                    nf.len()
                )));
            }
            let (mut chk, mut happ) = self.recode(c, &pf, &nf, &h.positive_spans, &h.negative_spans);
            let n = happ.num_samples();
            let n = happ.append_one(&mut chk, n, st, t, h);
            happ.set_num_samples(&mut chk, n);
            return Ok((Some((chk, happ)), true));
        }
        let n = self.append_one(c, num_samples, st, t, h);
        self.set_num_samples(c, n);
        Ok((None, false))
    }
}

#[inline]
pub(crate) fn model_cv(cv: &CustomValues) -> &[f64] {
    match cv {
        Some(a) => a,
        None => &[],
    }
}

/// Compares the spans and buckets of the last sample (a) with a new sample (b). Returns the inserts to make a
/// and b match, or None if b has a bucket missing that is populated in a or a bucket count went down (a reset).
pub fn expand_int_spans_and_buckets(
    a: &[Span],
    b: &[Span],
    a_buckets: &[i64],
    b_buckets: &[i64],
) -> Option<(Vec<Insert>, Vec<Insert>)> {
    let mut ai = BucketIterator::new(a);
    let mut bi = BucketIterator::new(b);
    let mut a_inserts: Vec<Insert> = Vec::new(); // To insert into buckets of a, for missing buckets in b.
    let mut b_inserts: Vec<Insert> = Vec::new(); // To insert into buckets of b, for missing empty buckets in a.
    let mut a_inter = Insert::default();
    let mut b_inter = Insert::default();

    let mut a_idx = ai.next();
    let mut b_idx = bi.next();
    let (mut a_count, mut b_count) = (0i64, 0i64);
    let (mut a_count_idx, mut b_count_idx) = (0usize, 0usize);
    if a_idx.is_some() {
        a_count = a_buckets[a_count_idx];
    }
    if b_idx.is_some() {
        b_count = b_buckets[b_count_idx];
    }

    fn add_insert(inserts: &mut Vec<Insert>, insert: &mut Insert, other_idx: i64) {
        if insert.num == 0 {
            // First insert.
            insert.bucket_idx = other_idx;
        } else if insert.bucket_idx + insert.num != other_idx {
            // Insert is not continuous from previous insert.
            inserts.push(*insert);
            insert.num = 0;
            insert.bucket_idx = other_idx;
        }
        insert.num += 1;
    }

    macro_rules! advance_a {
        () => {{
            if a_inter.num > 0 {
                a_inserts.push(a_inter);
                a_inter.num = 0;
            }
            a_idx = ai.next();
            a_inter.pos += 1;
            a_count_idx += 1;
            if a_idx.is_some() {
                a_count = a_count.wrapping_add(a_buckets[a_count_idx]);
            }
        }};
    }
    macro_rules! advance_b {
        () => {{
            if b_inter.num > 0 {
                b_inserts.push(b_inter);
                b_inter.num = 0;
            }
            b_idx = bi.next();
            b_inter.pos += 1;
            b_count_idx += 1;
            if b_idx.is_some() {
                b_count = b_count.wrapping_add(b_buckets[b_count_idx]);
            }
        }};
    }

    loop {
        match (a_idx, b_idx) {
            (Some(av), Some(bv)) => {
                if av == bv {
                    // Both have an identical bucket index. Bucket counts can't go down.
                    if a_count > b_count {
                        return None;
                    }
                    advance_a!();
                    advance_b!();
                } else if av < bv {
                    // b misses a bucket index that is in a. This is ok if the count in a is 0.
                    if a_count == 0 {
                        add_insert(&mut b_inserts, &mut b_inter, av);
                        advance_a!();
                        continue;
                    }
                    return None;
                } else {
                    // a misses a value that is in b. Forward b and recompare.
                    add_insert(&mut a_inserts, &mut a_inter, bv);
                    advance_b!();
                }
            }
            (Some(av), None) => {
                // b misses a value that is in a.
                if a_count == 0 {
                    add_insert(&mut b_inserts, &mut b_inter, av);
                    advance_a!();
                    continue;
                }
                return None;
            }
            (None, Some(bv)) => {
                // a misses a value that is in b. Forward b and recompare.
                add_insert(&mut a_inserts, &mut a_inter, bv);
                advance_b!();
            }
            (None, None) => {
                // Both iterators ran out. We're done.
                if a_inter.num > 0 {
                    a_inserts.push(a_inter);
                }
                if b_inter.num > 0 {
                    b_inserts.push(b_inter);
                }
                break;
            }
        }
    }
    Some((a_inserts, b_inserts))
}

/// Iterator over a histogram chunk.
#[derive(Clone, Debug)]
pub struct HistogramIteratorT<'a, const ST: bool> {
    pub(crate) br: BstreamReader<'a>,
    num_total: u16,
    num_read: u16,
    counter_reset_header: CounterResetHeader,
    pub(crate) layout: HistogramChunkLayout,
    pub(crate) t: i64,
    pub(crate) cnt: u64,
    pub(crate) z_cnt: u64,
    pub(crate) t_delta: i64,
    pub(crate) cnt_delta: i64,
    pub(crate) z_cnt_delta: i64,
    /// Bucket values as delta encoded in Go (first absolute, then deltas between buckets).
    pub(crate) p_buckets: Vec<i64>,
    pub(crate) n_buckets: Vec<i64>,
    pub(crate) p_buckets_delta: Vec<i64>,
    pub(crate) n_buckets_delta: Vec<i64>,
    pub(crate) sum: f64,
    pub(crate) leading: u8,
    pub(crate) trailing: u8,
    pub(crate) err: Option<Error>,
    pub(crate) st_dec: StDecoder,
}

/// The iterator of the histogram chunk.
pub type HistogramIterator<'a> = HistogramIteratorT<'a, false>;
/// The iterator of the histogram chunk with start timestamps.
pub type HistogramStIterator<'a> = HistogramIteratorT<'a, true>;

impl<'a, const ST: bool> HistogramIteratorT<'a, ST> {
    /// An iterator over the bytes of a histogram chunk (including the header).
    pub fn new(b: &'a [u8]) -> Self {
        let mut it = HistogramIteratorT {
            br: BstreamReader::new(&b[HISTOGRAM_HEADER_SIZE..]),
            num_total: header_num_samples::<ST>(b),
            num_read: 0,
            counter_reset_header: header_counter_reset::<ST>(b),
            layout: HistogramChunkLayout::default(),
            t: i64::MIN,
            cnt: 0,
            z_cnt: 0,
            t_delta: 0,
            cnt_delta: 0,
            z_cnt_delta: 0,
            p_buckets: Vec::new(),
            n_buckets: Vec::new(),
            p_buckets_delta: Vec::new(),
            n_buckets_delta: Vec::new(),
            sum: 0.0,
            leading: 0,
            trailing: 0,
            err: None,
            st_dec: StDecoder::default(),
        };
        if ST {
            let (known, change_on) = read_st_header(&b[HISTOGRAM_HEADER_SIZE - 1..]);
            it.st_dec.first_st_known = known;
            it.st_dec.first_st_change_on = change_on;
        }
        it
    }

    /// Resets the iterator to new chunk bytes, keeping its allocations.
    pub fn reset(&mut self, b: &'a [u8]) {
        self.st_dec = StDecoder::default();
        if ST {
            let (known, change_on) = read_st_header(&b[HISTOGRAM_HEADER_SIZE - 1..]);
            self.st_dec.first_st_known = known;
            self.st_dec.first_st_change_on = change_on;
        }
        self.br = BstreamReader::new(&b[HISTOGRAM_HEADER_SIZE..]);
        self.num_total = header_num_samples::<ST>(b);
        self.num_read = 0;
        self.counter_reset_header = header_counter_reset::<ST>(b);
        self.t = 0;
        self.cnt = 0;
        self.z_cnt = 0;
        self.t_delta = 0;
        self.cnt_delta = 0;
        self.z_cnt_delta = 0;
        self.p_buckets.clear();
        self.n_buckets.clear();
        self.p_buckets_delta.clear();
        self.n_buckets_delta.clear();
        self.sum = 0.0;
        self.leading = 0;
        self.trailing = 0;
        self.err = None;
        self.layout.custom_values = None;
    }

    #[inline]
    fn fail(&mut self, e: Error) -> ValueType {
        self.err = Some(e);
        ValueType::None
    }

    fn next_base(&mut self) -> ValueType {
        if self.err.is_some() || self.num_read == self.num_total {
            return ValueType::None;
        }

        if self.num_read == 0 {
            // The first read is responsible for reading the chunk layout and for initializing fields that depend
            // on it.
            if let Err(e) = read_histogram_chunk_layout_into(&mut self.br, &mut self.layout) {
                return self.fail(e);
            }
            if !is_known_schema(self.layout.schema) {
                return self.fail(Error::Histogram(unknown_schema_error(self.layout.schema)));
            }
            let (np, nn) = (count_spans(&self.layout.positive_spans), count_spans(&self.layout.negative_spans));
            self.p_buckets.resize(self.p_buckets.len() + np, 0);
            self.p_buckets_delta.resize(self.p_buckets_delta.len() + np, 0);
            self.n_buckets.resize(self.n_buckets.len() + nn, 0);
            self.n_buckets_delta.resize(self.n_buckets_delta.len() + nn, 0);

            // Now read the actual data.
            match read_varbit_int(&mut self.br) {
                Ok(v) => self.t = v,
                Err(e) => return self.fail(e),
            }
            match read_varbit_uint(&mut self.br) {
                Ok(v) => self.cnt = v,
                Err(e) => return self.fail(e),
            }
            match read_varbit_uint(&mut self.br) {
                Ok(v) => self.z_cnt = v,
                Err(e) => return self.fail(e),
            }
            match self.br.read_bits(64) {
                Some(v) => self.sum = f64::from_bits(v),
                None => return self.fail(Error::Eof),
            }
            for i in 0..self.p_buckets.len() {
                match read_varbit_int(&mut self.br) {
                    Ok(v) => self.p_buckets[i] = v,
                    Err(e) => return self.fail(e),
                }
            }
            for i in 0..self.n_buckets.len() {
                match read_varbit_int(&mut self.br) {
                    Ok(v) => self.n_buckets[i] = v,
                    Err(e) => return self.fail(e),
                }
            }
            self.num_read += 1;
            return ValueType::Histogram;
        }

        let t_dod = match read_varbit_int(&mut self.br) {
            Ok(v) => v,
            Err(e) => return self.fail(e),
        };
        self.t_delta = self.t_delta.wrapping_add(t_dod);
        self.t = self.t.wrapping_add(self.t_delta);

        let cnt_dod = match read_varbit_int(&mut self.br) {
            Ok(v) => v,
            Err(e) => return self.fail(e),
        };
        self.cnt_delta = self.cnt_delta.wrapping_add(cnt_dod);
        self.cnt = (self.cnt as i64).wrapping_add(self.cnt_delta) as u64;

        let zcnt_dod = match read_varbit_int(&mut self.br) {
            Ok(v) => v,
            Err(e) => return self.fail(e),
        };
        self.z_cnt_delta = self.z_cnt_delta.wrapping_add(zcnt_dod);
        self.z_cnt = (self.z_cnt as i64).wrapping_add(self.z_cnt_delta) as u64;

        if let Err(e) = xor_read(&mut self.br, &mut self.sum, &mut self.leading, &mut self.trailing) {
            return self.fail(e);
        }

        if is_stale_nan(self.sum) {
            self.num_read += 1;
            return ValueType::Histogram;
        }

        for i in 0..self.p_buckets.len() {
            let dod = match read_varbit_int(&mut self.br) {
                Ok(v) => v,
                Err(e) => return self.fail(e),
            };
            self.p_buckets_delta[i] = self.p_buckets_delta[i].wrapping_add(dod);
            self.p_buckets[i] = self.p_buckets[i].wrapping_add(self.p_buckets_delta[i]);
        }
        for i in 0..self.n_buckets.len() {
            let dod = match read_varbit_int(&mut self.br) {
                Ok(v) => v,
                Err(e) => return self.fail(e),
            };
            self.n_buckets_delta[i] = self.n_buckets_delta[i].wrapping_add(dod);
            self.n_buckets[i] = self.n_buckets[i].wrapping_add(self.n_buckets_delta[i]);
        }
        self.num_read += 1;
        ValueType::Histogram
    }
}

/// Copies `src` into `dst` reusing `dst`'s allocation.
#[inline]
pub(crate) fn copy_vec<T: Copy>(dst: &mut Vec<T>, src: &[T]) {
    dst.clear();
    dst.extend_from_slice(src);
}

/// Resets a reused histogram to `{Sum: sum}` (Go returns a new `&Histogram{Sum: sum}` for stale samples).
pub(crate) fn stale_histogram(h: Option<Histogram>, sum: f64) -> Histogram {
    let mut h = h.unwrap_or_default();
    h.counter_reset_hint = CounterResetHint::UnknownCounterReset;
    h.schema = 0;
    h.zero_threshold = 0.0;
    h.zero_count = 0;
    h.count = 0;
    h.sum = sum;
    h.positive_spans.clear();
    h.negative_spans.clear();
    h.positive_buckets.clear();
    h.negative_buckets.clear();
    h.custom_values = None;
    h
}

pub(crate) fn stale_float_histogram(fh: Option<FloatHistogram>, sum: f64) -> FloatHistogram {
    let mut fh = fh.unwrap_or_default();
    fh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
    fh.schema = 0;
    fh.zero_threshold = 0.0;
    fh.zero_count = 0.0;
    fh.count = 0.0;
    fh.sum = sum;
    fh.positive_spans.clear();
    fh.negative_spans.clear();
    fh.positive_buckets.clear();
    fh.negative_buckets.clear();
    fh.custom_values = None;
    fh
}

impl<const ST: bool> crate::Iterator for HistogramIteratorT<'_, ST> {
    fn next(&mut self) -> ValueType {
        if !ST {
            return self.next_base();
        }
        let prev_t = self.t;
        let vt = self.next_base();
        if vt == ValueType::None {
            return ValueType::None;
        }
        let (num_read, t) = (self.num_read, self.t);
        if let Err(e) = self.st_dec.decode(&mut self.br, num_read, t, prev_t) {
            self.err = Some(e);
            return ValueType::None;
        }
        vt
    }

    fn seek(&mut self, t: i64) -> ValueType {
        if self.err.is_some() {
            return ValueType::None;
        }
        while t > self.t || self.num_read == 0 {
            if self.next() == ValueType::None {
                return ValueType::None;
            }
        }
        ValueType::Histogram
    }

    fn at(&self) -> (i64, f64) {
        panic!("cannot call histogramIterator.At");
    }

    fn at_histogram(&self, h: Option<Histogram>) -> (i64, Histogram) {
        if is_stale_nan(self.sum) {
            return (self.t, stale_histogram(h, self.sum));
        }
        let mut h = h.unwrap_or_default();
        h.counter_reset_hint = counter_reset_hint(self.counter_reset_header, self.num_read);
        h.schema = self.layout.schema;
        h.zero_threshold = self.layout.zero_threshold;
        h.zero_count = self.z_cnt;
        h.count = self.cnt;
        h.sum = self.sum;
        copy_vec(&mut h.positive_spans, &self.layout.positive_spans);
        copy_vec(&mut h.negative_spans, &self.layout.negative_spans);
        copy_vec(&mut h.positive_buckets, &self.p_buckets);
        copy_vec(&mut h.negative_buckets, &self.n_buckets);
        h.custom_values = self.layout.custom_values.clone();
        if h.schema > EXPONENTIAL_SCHEMA_MAX && h.schema <= EXPONENTIAL_SCHEMA_MAX_RESERVED {
            if let Err(e) = h.reduce_resolution(EXPONENTIAL_SCHEMA_MAX) {
                // With the checks done in Next(), this should never happen.
                panic!("{}", e);
            }
        }
        (self.t, h)
    }

    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        if is_stale_nan(self.sum) {
            return (self.t, stale_float_histogram(fh, self.sum));
        }
        // Go: a new float histogram gets the absolute counts accumulated as int64 (computed in Next), a reused
        // one accumulates as float64.
        let reused = fh.is_some();
        let mut fh = fh.unwrap_or_default();
        fh.counter_reset_hint = counter_reset_hint(self.counter_reset_header, self.num_read);
        fh.schema = self.layout.schema;
        fh.zero_threshold = self.layout.zero_threshold;
        fh.zero_count = self.z_cnt as f64;
        fh.count = self.cnt as f64;
        fh.sum = self.sum;
        copy_vec(&mut fh.positive_spans, &self.layout.positive_spans);
        copy_vec(&mut fh.negative_spans, &self.layout.negative_spans);
        fill_float_buckets(&mut fh.positive_buckets, &self.p_buckets, reused);
        fill_float_buckets(&mut fh.negative_buckets, &self.n_buckets, reused);
        fh.custom_values = self.layout.custom_values.clone();
        if fh.schema > EXPONENTIAL_SCHEMA_MAX && fh.schema <= EXPONENTIAL_SCHEMA_MAX_RESERVED {
            if let Err(e) = fh.reduce_resolution(EXPONENTIAL_SCHEMA_MAX) {
                panic!("{}", e);
            }
        }
        (self.t, fh)
    }

    #[inline]
    fn at_t(&self) -> i64 {
        self.t
    }

    fn at_st(&self) -> i64 {
        if ST { self.st_dec.st } else { 0 }
    }

    fn err(&self) -> Option<&Error> {
        self.err.as_ref()
    }
}

fn fill_float_buckets(dst: &mut Vec<f64>, deltas: &[i64], float_accumulation: bool) {
    dst.clear();
    if float_accumulation {
        let mut cur = 0.0f64;
        for &d in deltas {
            cur += d as f64;
            dst.push(cur);
        }
    } else {
        let mut cur = 0i64;
        for &d in deltas {
            cur = cur.wrapping_add(d);
            dst.push(cur as f64);
        }
    }
}
