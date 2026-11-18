//! Float histogram chunks (port of `tsdb/chunkenc/float_histogram.go` and `float_histogram_st.go`).
//!
//! `FloatHistogramChunkT<false>` is encoding 3, `FloatHistogramChunkT<true>` encoding 6 (with start timestamps).

use crate::Iterator as _;
use crate::bstream::{Bstream, BstreamReader};
use crate::histogram::{
    CounterResetHeader, HISTOGRAM_HEADER_SIZE, copy_vec, count_spans, header_counter_reset, header_num_samples,
    model_cv, new_histogram_stream, set_header_counter_reset, set_header_num_samples, stale_float_histogram,
};
use crate::histogram_meta::*;
use crate::st::{HISTOGRAM_ST_SAMPLE_COUNT_MASK, StDecoder, StEncoder, read_st_header};
use crate::varbit::{put_varbit_int, read_varbit_int};
use crate::xor::{xor_read, xor_write};
use crate::{ENC_FLOAT_HISTOGRAM, ENC_FLOAT_HISTOGRAM_ST, Encoding, Error, ValueType, is_stale_nan};
use model::histogram::{
    CounterResetHint, CustomValues, EXPONENTIAL_SCHEMA_MAX, EXPONENTIAL_SCHEMA_MAX_RESERVED, FloatHistogram, Histogram,
    Span, custom_bucket_bounds_match, is_custom_buckets_schema, is_known_schema, unknown_schema_error,
};

/// A value with its XOR window.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct XorValue {
    pub value: f64,
    pub leading: u8,
    pub trailing: u8,
}

impl XorValue {
    const NEW: XorValue = XorValue { value: 0.0, leading: 0xff, trailing: 0 };
}

/// A chunk holding float histograms. `ST` selects the start timestamp variant.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FloatHistogramChunkT<const ST: bool> {
    pub b: Bstream,
}

/// The float histogram chunk (encoding 3).
pub type FloatHistogramChunk = FloatHistogramChunkT<false>;
/// The float histogram chunk with start timestamps (encoding 6).
pub type FloatHistogramStChunk = FloatHistogramChunkT<true>;

impl<const ST: bool> FloatHistogramChunkT<ST> {
    /// A new empty chunk.
    pub fn new() -> Self {
        FloatHistogramChunkT { b: Bstream { stream: new_histogram_stream(), count: 0 } }
    }

    pub fn from_bstream(b: Bstream) -> Self {
        FloatHistogramChunkT { b }
    }

    pub fn reset(&mut self, stream: Vec<u8>) {
        self.b.reset(stream);
    }

    pub fn encoding(&self) -> Encoding {
        if ST { ENC_FLOAT_HISTOGRAM_ST } else { ENC_FLOAT_HISTOGRAM }
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
    pub fn appender(&mut self) -> Result<FloatHistogramAppenderT<ST>, Error> {
        if self.b.stream.len() == HISTOGRAM_HEADER_SIZE {
            return Ok(FloatHistogramAppenderT::new_empty(self.b.bytes()));
        }
        let mut it = FloatHistogramIteratorT::<ST>::new(self.b.bytes());
        while it.next() == ValueType::FloatHistogram {}
        if let Some(e) = it.err {
            return Err(e);
        }
        let valid = it.br.valid;
        let p_buckets = (0..it.p_buckets.len())
            .map(|i| XorValue { value: it.p_buckets[i], leading: it.p_leading[i], trailing: it.p_trailing[i] })
            .collect();
        let n_buckets = (0..it.n_buckets.len())
            .map(|i| XorValue { value: it.n_buckets[i], leading: it.n_leading[i], trailing: it.n_trailing[i] })
            .collect();
        let a = FloatHistogramAppenderT {
            schema: it.layout.schema,
            z_threshold: it.layout.zero_threshold,
            p_spans: it.layout.positive_spans,
            n_spans: it.layout.negative_spans,
            custom_values: it.layout.custom_values,
            t: it.t,
            t_delta: it.t_delta,
            cnt: it.cnt,
            z_cnt: it.z_cnt,
            sum: it.sum,
            p_buckets,
            n_buckets,
            num_samples: header_num_samples::<ST>(self.b.bytes()),
            crh: header_counter_reset::<ST>(self.b.bytes()),
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

    pub fn iterator(&self) -> FloatHistogramIteratorT<'_, ST> {
        FloatHistogramIteratorT::new(self.b.bytes())
    }
}

crate::impl_chunk!(FloatHistogramChunkT<false>, FloatHistogram);
crate::impl_chunk!(FloatHistogramChunkT<true>, FloatHistogramSt);

/// The result of float `appendable`: forward positive and negative inserts, backward positive and negative inserts,
/// okToAppend, counterReset.
pub type FloatHistogramAppendableResult = (Vec<Insert>, Vec<Insert>, Vec<Insert>, Vec<Insert>, bool, bool);

/// An appender that can tell how a float histogram fits after its last sample (Go `floatHistogramAppendable`).
pub trait FloatHistogramAppendable {
    fn appendable(&self, h: &FloatHistogram) -> FloatHistogramAppendableResult;
}

/// The encoder state of a float histogram chunk.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FloatHistogramAppenderT<const ST: bool> {
    pub(crate) schema: i32,
    pub(crate) z_threshold: f64,
    pub(crate) p_spans: Vec<Span>,
    pub(crate) n_spans: Vec<Span>,
    pub(crate) custom_values: CustomValues,
    pub(crate) t: i64,
    pub(crate) t_delta: i64,
    pub(crate) sum: XorValue,
    pub(crate) cnt: XorValue,
    pub(crate) z_cnt: XorValue,
    pub(crate) p_buckets: Vec<XorValue>,
    pub(crate) n_buckets: Vec<XorValue>,
    /// Mirror of the sample count in the chunk header.
    pub(crate) num_samples: u16,
    /// Mirror of the counter reset bits in the chunk header.
    pub(crate) crh: CounterResetHeader,
    pub(crate) st_enc: StEncoder,
}

/// The appender of the float histogram chunk.
pub type FloatHistogramAppender = FloatHistogramAppenderT<false>;
/// The appender of the float histogram chunk with start timestamps.
pub type FloatHistogramStAppender = FloatHistogramAppenderT<true>;

impl<const ST: bool> FloatHistogramAppendable for FloatHistogramAppenderT<ST> {
    fn appendable(&self, h: &FloatHistogram) -> FloatHistogramAppendableResult {
        FloatHistogramAppenderT::appendable(self, h)
    }
}

impl<const ST: bool> FloatHistogramAppenderT<ST> {
    fn new_empty(b: &[u8]) -> Self {
        FloatHistogramAppenderT {
            t: i64::MIN,
            sum: XorValue::NEW,
            cnt: XorValue::NEW,
            z_cnt: XorValue::NEW,
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
    pub fn set_counter_reset_header(&mut self, c: &mut FloatHistogramChunkT<ST>, cr: CounterResetHeader) {
        set_header_counter_reset::<ST>(c.b.bytes_mut(), cr);
        self.crh = CounterResetHeader(cr.0 & crate::histogram::COUNTER_RESET_HEADER_MASK);
    }

    /// The number of samples in the chunk.
    pub fn num_samples(&self) -> usize {
        self.num_samples as usize
    }

    fn set_num_samples(&mut self, c: &mut FloatHistogramChunkT<ST>, num: usize) {
        set_header_num_samples::<ST>(c.b.bytes_mut(), num);
        self.num_samples = if ST { (num as u16) & HISTOGRAM_ST_SAMPLE_COUNT_MASK } else { num as u16 };
    }

    /// Checks if `h` can be appended without a new chunk (Go `appendable`).
    pub fn appendable(&self, h: &FloatHistogram) -> FloatHistogramAppendableResult {
        let none = |ok: bool, cr: bool| (Vec::new(), Vec::new(), Vec::new(), Vec::new(), ok, cr);
        if self.num_samples > 0 && self.crh == CounterResetHeader::GAUGE_TYPE {
            return none(false, false);
        }
        if h.counter_reset_hint == CounterResetHint::CounterReset {
            return none(false, true);
        }
        if is_stale_nan(h.sum) {
            return none(true, false);
        }
        if is_stale_nan(self.sum.value) {
            return none(false, false);
        }
        if h.count < self.cnt.value {
            return none(false, true);
        }
        if h.schema != self.schema || h.zero_threshold != self.z_threshold {
            return none(false, false);
        }
        if is_custom_buckets_schema(h.schema)
            && !custom_bucket_bounds_match(h.custom_values_slice(), model_cv(&self.custom_values))
        {
            return none(false, true);
        }
        if h.zero_count < self.z_cnt.value {
            return none(false, true);
        }
        let (pf, pb) =
            match expand_float_spans_and_buckets(&self.p_spans, &h.positive_spans, &self.p_buckets, &h.positive_buckets) {
                Some(r) => r,
                None => return none(false, true),
            };
        let (nf, nb) =
            match expand_float_spans_and_buckets(&self.n_spans, &h.negative_spans, &self.n_buckets, &h.negative_buckets) {
                Some(r) => r,
                None => return (pf, Vec::new(), pb, Vec::new(), false, true),
            };
        (pf, nf, pb, nb, true, false)
    }

    /// Like `appendable` for gauge histograms (Go `appendableGauge`).
    pub fn appendable_gauge(&self, h: &FloatHistogram) -> crate::histogram::AppendableGaugeResult {
        let none = |ok: bool| (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), ok);
        if self.num_samples > 0 && self.crh != CounterResetHeader::GAUGE_TYPE {
            return none(false);
        }
        if is_stale_nan(h.sum) {
            return none(true);
        }
        if is_stale_nan(self.sum.value) {
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

    /// Writes one float histogram (Go `appendFloatHistogram`); returns num + 1.
    fn append_float_histogram_raw(&mut self, b: &mut Bstream, num: usize, t: i64, h: &FloatHistogram) -> usize {
        let stale;
        let h = if is_stale_nan(h.sum) {
            stale = FloatHistogram { sum: h.sum, ..Default::default() };
            &stale
        } else {
            h
        };
        let mut t_delta = 0i64;
        if num == 0 {
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
            copy_vec(&mut self.p_spans, &h.positive_spans);
            copy_vec(&mut self.n_spans, &h.negative_spans);
            self.custom_values = if h.custom_values_slice().is_empty() { None } else { h.custom_values.clone() };
            let (np, nn) = (count_spans(&h.positive_spans), count_spans(&h.negative_spans));
            self.p_buckets.clear();
            for i in 0..np {
                self.p_buckets.push(XorValue { value: h.positive_buckets[i], leading: 0xff, trailing: 0 });
            }
            self.n_buckets.clear();
            for i in 0..nn {
                self.n_buckets.push(XorValue { value: h.negative_buckets[i], leading: 0xff, trailing: 0 });
            }

            put_varbit_int(b, t);
            b.write_bits(h.count.to_bits(), 64);
            b.write_bits(h.zero_count.to_bits(), 64);
            b.write_bits(h.sum.to_bits(), 64);
            self.cnt.value = h.count;
            self.z_cnt.value = h.zero_count;
            self.sum.value = h.sum;
            for &v in &h.positive_buckets {
                b.write_bits(v.to_bits(), 64);
            }
            for &v in &h.negative_buckets {
                b.write_bits(v.to_bits(), 64);
            }
        } else {
            t_delta = t.wrapping_sub(self.t);
            let t_dod = t_delta.wrapping_sub(self.t_delta);
            put_varbit_int(b, t_dod);

            write_xor_value(b, &mut self.cnt, h.count);
            write_xor_value(b, &mut self.z_cnt, h.zero_count);
            write_xor_value(b, &mut self.sum, h.sum);
            for (i, &v) in h.positive_buckets.iter().enumerate() {
                write_xor_value(b, &mut self.p_buckets[i], v);
            }
            for (i, &v) in h.negative_buckets.iter().enumerate() {
                write_xor_value(b, &mut self.n_buckets[i], v);
            }
        }
        self.t = t;
        self.t_delta = t_delta;
        num + 1
    }

    fn append_one(&mut self, c: &mut FloatHistogramChunkT<ST>, num: usize, st: i64, t: i64, h: &FloatHistogram) -> usize {
        let prev_t = self.t;
        let new_num = self.append_float_histogram_raw(&mut c.b, num, t, h);
        if ST {
            let cur_t = self.t;
            self.st_enc.encode(&mut c.b, new_num as u16, cur_t, prev_t, st);
        }
        new_num
    }

    fn recode(
        &self,
        c: &FloatHistogramChunkT<ST>,
        positive_inserts: &[Insert],
        negative_inserts: &[Insert],
        positive_spans: &[Span],
        negative_spans: &[Span],
    ) -> (FloatHistogramChunkT<ST>, FloatHistogramAppenderT<ST>) {
        let byts = c.b.bytes();
        let mut it = FloatHistogramIteratorT::<ST>::new(byts);
        let mut hc = FloatHistogramChunkT::<ST>::new();
        let mut happ = match hc.appender() {
            Ok(a) => a,
            Err(e) => panic!("{}", e),
        };
        let (np, nn) = (count_spans(positive_spans), count_spans(negative_spans));
        let mut num = happ.num_samples();
        while it.next() == ValueType::FloatHistogram {
            let (t_old, mut h_old) = it.at_float_histogram(None);
            let st_old = it.at_st();
            h_old.positive_spans = positive_spans.to_vec();
            h_old.negative_spans = negative_spans.to_vec();
            if !positive_inserts.is_empty() {
                let mut out = vec![0f64; np];
                insert(&h_old.positive_buckets, &mut out, positive_inserts, false);
                h_old.positive_buckets = out;
            }
            if !negative_inserts.is_empty() {
                let mut out = vec![0f64; nn];
                insert(&h_old.negative_buckets, &mut out, negative_inserts, false);
                h_old.negative_buckets = out;
            }
            num = happ.append_one(&mut hc, num, st_old, t_old, &h_old);
        }
        happ.set_num_samples(&mut hc, num);
        happ.set_counter_reset_header(&mut hc, header_counter_reset::<ST>(byts));
        (hc, happ)
    }

    fn recode_histogram(fh: &mut FloatHistogram, p_backward: &[Insert], n_backward: &[Insert]) {
        if !p_backward.is_empty() {
            let mut out = vec![0f64; count_spans(&fh.positive_spans)];
            insert(&fh.positive_buckets, &mut out, p_backward, false);
            fh.positive_buckets = out;
        }
        if !n_backward.is_empty() {
            let mut out = vec![0f64; count_spans(&fh.negative_spans)];
            insert(&fh.negative_buckets, &mut out, n_backward, false);
            fh.negative_buckets = out;
        }
    }

    fn new_chunk_with(st: i64, t: i64, h: &FloatHistogram, crh: Option<CounterResetHeader>) -> (FloatHistogramChunkT<ST>, Self) {
        let mut new_chunk = FloatHistogramChunkT::<ST>::new();
        let mut happ = match new_chunk.appender() {
            Ok(a) => a,
            Err(e) => panic!("{}", e),
        };
        if let Some(crh) = crh {
            happ.set_counter_reset_header(&mut new_chunk, crh);
        }
        let n = happ.append_one(&mut new_chunk, 0, st, t, h);
        happ.set_num_samples(&mut new_chunk, n);
        (new_chunk, happ)
    }

    /// Appends a float histogram to chunk `c` (Go `AppendFloatHistogram`). See `HistogramAppenderT::append_histogram`.
    #[allow(clippy::type_complexity)]
    pub fn append_float_histogram(
        &mut self,
        c: &mut FloatHistogramChunkT<ST>,
        prev: Option<&dyn FloatHistogramAppendable>,
        st: i64,
        t: i64,
        h: &mut FloatHistogram,
        append_only: bool,
    ) -> Result<(Option<(FloatHistogramChunkT<ST>, FloatHistogramAppenderT<ST>)>, bool), Error> {
        let num_samples = self.num_samples();
        let max = if ST { HISTOGRAM_ST_SAMPLE_COUNT_MASK as usize } else { u16::MAX as usize };
        if num_samples == max {
            panic!("chunk capacity exceeded");
        }

        if num_samples == 0 {
            let n = self.append_one(c, num_samples, st, t, h);
            self.set_num_samples(c, n);
            if h.counter_reset_hint == CounterResetHint::GaugeType {
                self.set_counter_reset_header(c, CounterResetHeader::GAUGE_TYPE);
                return Ok((None, false));
            }
            if h.counter_reset_hint == CounterResetHint::CounterReset {
                self.set_counter_reset_header(c, CounterResetHeader::COUNTER_RESET);
            } else if let Some(p) = prev {
                let (_, _, _, _, _, counter_reset) = p.appendable(h);
                if counter_reset {
                    self.set_counter_reset_header(c, CounterResetHeader::COUNTER_RESET);
                } else {
                    self.set_counter_reset_header(c, CounterResetHeader::NOT_COUNTER_RESET);
                }
            }
            return Ok((None, false));
        }

        if h.counter_reset_hint != CounterResetHint::GaugeType {
            let (pf, nf, pb, nb, ok_to_append, counter_reset) = self.appendable(h);
            if !ok_to_append || counter_reset {
                if append_only {
                    if counter_reset {
                        return Err(Error::msg("float histogram counter reset"));
                    }
                    return Err(Error::msg("float histogram schema change"));
                }
                let crh = if counter_reset { Some(CounterResetHeader::COUNTER_RESET) } else { None };
                return Ok((Some(Self::new_chunk_with(st, t, h, crh)), false));
            }
            if !pb.is_empty() || !nb.is_empty() {
                if pf.is_empty() && nf.is_empty() {
                    h.positive_spans = self.p_spans.clone();
                    h.negative_spans = self.n_spans.clone();
                } else {
                    h.positive_spans = adjust_for_inserts(&h.positive_spans, &pb);
                    h.negative_spans = adjust_for_inserts(&h.negative_spans, &nb);
                }
                Self::recode_histogram(h, &pb, &nb);
            }
            if !pf.is_empty() || !nf.is_empty() {
                if append_only {
                    return Err(Error::msg(format!(
                        "float histogram layout change with {} positive and {} negative forwards inserts",
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
            return Ok((None, false));
        }

        let (pf, nf, pb, nb, p_merged, n_merged, ok_to_append) = self.appendable_gauge(h);
        if !ok_to_append {
            if append_only {
                return Err(Error::msg("float gauge histogram schema change"));
            }
            return Ok((Some(Self::new_chunk_with(st, t, h, Some(CounterResetHeader::GAUGE_TYPE))), false));
        }
        if pb.len() + nb.len() > 0 {
            if append_only {
                return Err(Error::msg(format!(
                    "float gauge histogram layout change with {} positive and {} negative backwards inserts",
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
                    "float gauge histogram layout change with {} positive and {} negative forwards inserts",
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
fn write_xor_value(b: &mut Bstream, old: &mut XorValue, v: f64) {
    xor_write(b, v, old.value, &mut old.leading, &mut old.trailing);
    old.value = v;
}

/// Like `expand_int_spans_and_buckets` for absolute float buckets.
pub fn expand_float_spans_and_buckets(
    a: &[Span],
    b: &[Span],
    a_buckets: &[XorValue],
    b_buckets: &[f64],
) -> Option<(Vec<Insert>, Vec<Insert>)> {
    let mut ai = BucketIterator::new(a);
    let mut bi = BucketIterator::new(b);
    let mut a_inserts: Vec<Insert> = Vec::new();
    let mut b_inserts: Vec<Insert> = Vec::new();
    let mut a_inter = Insert::default();
    let mut b_inter = Insert::default();
    let mut a_idx = ai.next();
    let mut b_idx = bi.next();
    let (mut a_count, mut b_count) = (0f64, 0f64);
    let (mut a_count_idx, mut b_count_idx) = (0usize, 0usize);
    if a_idx.is_some() {
        a_count = a_buckets[a_count_idx].value;
    }
    if b_idx.is_some() {
        b_count = b_buckets[b_count_idx];
    }

    fn add_insert(inserts: &mut Vec<Insert>, insert: &mut Insert, other_idx: i64) {
        if insert.num == 0 {
            insert.bucket_idx = other_idx;
        } else if insert.bucket_idx + insert.num != other_idx {
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
                a_count = a_buckets[a_count_idx].value;
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
                b_count = b_buckets[b_count_idx];
            }
        }};
    }

    loop {
        match (a_idx, b_idx) {
            (Some(av), Some(bv)) => {
                if av == bv {
                    if a_count > b_count {
                        return None;
                    }
                    advance_a!();
                    advance_b!();
                } else if av < bv {
                    if a_count == 0.0 {
                        add_insert(&mut b_inserts, &mut b_inter, av);
                        advance_a!();
                        continue;
                    }
                    return None;
                } else {
                    add_insert(&mut a_inserts, &mut a_inter, bv);
                    advance_b!();
                }
            }
            (Some(av), None) => {
                if a_count == 0.0 {
                    add_insert(&mut b_inserts, &mut b_inter, av);
                    advance_a!();
                    continue;
                }
                return None;
            }
            (None, Some(bv)) => {
                add_insert(&mut a_inserts, &mut a_inter, bv);
                advance_b!();
            }
            (None, None) => {
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

/// Iterator over a float histogram chunk.
#[derive(Clone, Debug)]
pub struct FloatHistogramIteratorT<'a, const ST: bool> {
    pub(crate) br: BstreamReader<'a>,
    num_total: u16,
    num_read: u16,
    counter_reset_header: CounterResetHeader,
    pub(crate) layout: HistogramChunkLayout,
    pub(crate) t: i64,
    pub(crate) t_delta: i64,
    pub(crate) sum: XorValue,
    pub(crate) cnt: XorValue,
    pub(crate) z_cnt: XorValue,
    pub(crate) p_buckets: Vec<f64>,
    pub(crate) n_buckets: Vec<f64>,
    pub(crate) p_leading: Vec<u8>,
    pub(crate) n_leading: Vec<u8>,
    pub(crate) p_trailing: Vec<u8>,
    pub(crate) n_trailing: Vec<u8>,
    pub(crate) err: Option<Error>,
    pub(crate) st_dec: StDecoder,
}

/// The iterator of the float histogram chunk.
pub type FloatHistogramIterator<'a> = FloatHistogramIteratorT<'a, false>;
/// The iterator of the float histogram chunk with start timestamps.
pub type FloatHistogramStIterator<'a> = FloatHistogramIteratorT<'a, true>;

impl<'a, const ST: bool> FloatHistogramIteratorT<'a, ST> {
    /// An iterator over the bytes of a float histogram chunk (including the header).
    pub fn new(b: &'a [u8]) -> Self {
        let mut it = FloatHistogramIteratorT {
            br: BstreamReader::new(&b[HISTOGRAM_HEADER_SIZE..]),
            num_total: header_num_samples::<ST>(b),
            num_read: 0,
            counter_reset_header: header_counter_reset::<ST>(b),
            layout: HistogramChunkLayout::default(),
            t: i64::MIN,
            t_delta: 0,
            sum: XorValue::default(),
            cnt: XorValue::default(),
            z_cnt: XorValue::default(),
            p_buckets: Vec::new(),
            n_buckets: Vec::new(),
            p_leading: Vec::new(),
            n_leading: Vec::new(),
            p_trailing: Vec::new(),
            n_trailing: Vec::new(),
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
        self.t_delta = 0;
        self.cnt = XorValue::default();
        self.z_cnt = XorValue::default();
        self.sum = XorValue::default();
        self.p_buckets.clear();
        self.n_buckets.clear();
        self.p_leading.clear();
        self.p_trailing.clear();
        self.n_leading.clear();
        self.n_trailing.clear();
        self.err = None;
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
            if let Err(e) = read_histogram_chunk_layout_into(&mut self.br, &mut self.layout) {
                return self.fail(e);
            }
            if !is_known_schema(self.layout.schema) {
                return self.fail(Error::Histogram(unknown_schema_error(self.layout.schema)));
            }
            let (np, nn) = (count_spans(&self.layout.positive_spans), count_spans(&self.layout.negative_spans));
            self.p_buckets.resize(self.p_buckets.len() + np, 0.0);
            self.p_leading.resize(self.p_leading.len() + np, 0);
            self.p_trailing.resize(self.p_trailing.len() + np, 0);
            self.n_buckets.resize(self.n_buckets.len() + nn, 0.0);
            self.n_leading.resize(self.n_leading.len() + nn, 0);
            self.n_trailing.resize(self.n_trailing.len() + nn, 0);

            match read_varbit_int(&mut self.br) {
                Ok(v) => self.t = v,
                Err(e) => return self.fail(e),
            }
            match self.br.read_bits(64) {
                Some(v) => self.cnt.value = f64::from_bits(v),
                None => return self.fail(Error::Eof),
            }
            match self.br.read_bits(64) {
                Some(v) => self.z_cnt.value = f64::from_bits(v),
                None => return self.fail(Error::Eof),
            }
            match self.br.read_bits(64) {
                Some(v) => self.sum.value = f64::from_bits(v),
                None => return self.fail(Error::Eof),
            }
            for i in 0..self.p_buckets.len() {
                match self.br.read_bits(64) {
                    Some(v) => self.p_buckets[i] = f64::from_bits(v),
                    None => return self.fail(Error::Eof),
                }
            }
            for i in 0..self.n_buckets.len() {
                match self.br.read_bits(64) {
                    Some(v) => self.n_buckets[i] = f64::from_bits(v),
                    None => return self.fail(Error::Eof),
                }
            }
            self.num_read += 1;
            return ValueType::FloatHistogram;
        }

        let t_dod = match read_varbit_int(&mut self.br) {
            Ok(v) => v,
            Err(e) => return self.fail(e),
        };
        self.t_delta = self.t_delta.wrapping_add(t_dod);
        self.t = self.t.wrapping_add(self.t_delta);

        if let Err(e) = xor_read(&mut self.br, &mut self.cnt.value, &mut self.cnt.leading, &mut self.cnt.trailing) {
            return self.fail(e);
        }
        if let Err(e) = xor_read(&mut self.br, &mut self.z_cnt.value, &mut self.z_cnt.leading, &mut self.z_cnt.trailing) {
            return self.fail(e);
        }
        if let Err(e) = xor_read(&mut self.br, &mut self.sum.value, &mut self.sum.leading, &mut self.sum.trailing) {
            return self.fail(e);
        }
        if is_stale_nan(self.sum.value) {
            self.num_read += 1;
            return ValueType::FloatHistogram;
        }
        for i in 0..self.p_buckets.len() {
            if let Err(e) =
                xor_read(&mut self.br, &mut self.p_buckets[i], &mut self.p_leading[i], &mut self.p_trailing[i])
            {
                return self.fail(e);
            }
        }
        for i in 0..self.n_buckets.len() {
            if let Err(e) =
                xor_read(&mut self.br, &mut self.n_buckets[i], &mut self.n_leading[i], &mut self.n_trailing[i])
            {
                return self.fail(e);
            }
        }
        self.num_read += 1;
        ValueType::FloatHistogram
    }
}

impl<const ST: bool> crate::Iterator for FloatHistogramIteratorT<'_, ST> {
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
        ValueType::FloatHistogram
    }

    fn at(&self) -> (i64, f64) {
        panic!("cannot call floatHistogramIterator.At");
    }

    fn at_histogram(&self, _h: Option<Histogram>) -> (i64, Histogram) {
        panic!("cannot call floatHistogramIterator.AtHistogram");
    }

    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        if is_stale_nan(self.sum.value) {
            return (self.t, stale_float_histogram(fh, self.sum.value));
        }
        let mut fh = fh.unwrap_or_default();
        fh.counter_reset_hint = counter_reset_hint(self.counter_reset_header, self.num_read);
        fh.schema = self.layout.schema;
        fh.zero_threshold = self.layout.zero_threshold;
        fh.zero_count = self.z_cnt.value;
        fh.count = self.cnt.value;
        fh.sum = self.sum.value;
        copy_vec(&mut fh.positive_spans, &self.layout.positive_spans);
        copy_vec(&mut fh.negative_spans, &self.layout.negative_spans);
        copy_vec(&mut fh.positive_buckets, &self.p_buckets);
        copy_vec(&mut fh.negative_buckets, &self.n_buckets);
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
