//! Port of `storage/series.go`: list series and iterators, conversions between series sets and chunk series
//! sets, and the series to chunks encoder.

use std::borrow::Cow;
use std::sync::Arc;

use chunkenc::{AnyIterator, Chunk, ChunkIterator, Encoding, ValueType};
use model::histogram::{FloatHistogram, Histogram};
use model::labels::Labels;

use crate::annotations::Annotations;
use crate::chunks::{BoxMetaIterator, Meta, MetaIterator, Sample, chunk_from_samples};
use crate::error::Error;
use crate::interface::*;
use crate::merge::ChainedSeries;

/// A series made of labels, data and a function that iterates the data (Go `SeriesEntry`, whose iterator
/// function is a closure). The function gets the data and the iterator to reuse.
pub struct SeriesEntry<D> {
    pub lset: Labels,
    pub data: D,
    pub sample_iterator_fn: for<'a> fn(&'a D, Option<AnyIterator<'a>>) -> AnyIterator<'a>,
}

impl<D: Send + Sync> Series for SeriesEntry<D> {
    fn labels(&self) -> &Labels {
        &self.lset
    }
    fn iterator<'a>(&'a self, reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        (self.sample_iterator_fn)(&self.data, reuse)
    }
}

/// A chunk series made of labels, data and a function that iterates the chunks (Go `ChunkSeriesEntry`).
pub struct ChunkSeriesEntry<D> {
    pub lset: Labels,
    pub data: D,
    pub chunk_iterator_fn: for<'a> fn(&'a D, Option<BoxMetaIterator<'a>>) -> BoxMetaIterator<'a>,
}

impl<D: Send + Sync> ChunkSeries for ChunkSeriesEntry<D> {
    fn labels(&self) -> &Labels {
        &self.lset
    }
    fn iterator<'a>(&'a self, reuse: Option<BoxMetaIterator<'a>>) -> BoxMetaIterator<'a> {
        (self.chunk_iterator_fn)(&self.data, reuse)
    }
}

// ---------------------------------------------------------------------------------------------------------------
// List series.

/// A series over a list of samples (Go `NewListSeries`).
pub struct ListSeries {
    lset: Labels,
    samples: Vec<Sample>,
}

impl ListSeries {
    pub fn new(lset: Labels, samples: Vec<Sample>) -> ListSeries {
        ListSeries { lset, samples }
    }
    pub fn samples(&self) -> &[Sample] {
        &self.samples
    }
}

/// Go `NewListSeries`.
pub fn new_list_series(lset: Labels, samples: Vec<Sample>) -> SeriesArc {
    Arc::new(ListSeries::new(lset, samples))
}

impl Series for ListSeries {
    fn labels(&self) -> &Labels {
        &self.lset
    }
    fn iterator<'a>(&'a self, _reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        AnyIterator::Other(Box::new(ListSeriesIterator::new(&self.samples)))
    }
}

/// Iterates over a list of samples (Go `listSeriesIterator`). Histograms are copied into the histogram given to
/// `at_histogram` (so it also serves as Go's `listSeriesIteratorWithCopy`).
#[derive(Clone, Debug)]
pub struct ListSeriesIterator<'a> {
    samples: &'a [Sample],
    idx: isize,
}

impl<'a> ListSeriesIterator<'a> {
    pub fn new(samples: &'a [Sample]) -> ListSeriesIterator<'a> {
        ListSeriesIterator { samples, idx: -1 }
    }

    /// Go `Reset`.
    pub fn reset(&mut self, samples: &'a [Sample]) {
        self.samples = samples;
        self.idx = -1;
    }

    fn cur(&self) -> &'a Sample {
        &self.samples[self.idx as usize]
    }
}

/// Go `NewListSeriesIterator`.
pub fn new_list_series_iterator(samples: &[Sample]) -> AnyIterator<'_> {
    AnyIterator::Other(Box::new(ListSeriesIterator::new(samples)))
}

/// Go `NewListSeriesIteratorWithCopy`.
pub fn new_list_series_iterator_with_copy(samples: &[Sample]) -> AnyIterator<'_> {
    new_list_series_iterator(samples)
}

impl ChunkIterator for ListSeriesIterator<'_> {
    fn next(&mut self) -> ValueType {
        self.idx += 1;
        if self.idx as usize >= self.samples.len() {
            return ValueType::None;
        }
        self.cur().typ()
    }

    fn seek(&mut self, t: i64) -> ValueType {
        if self.idx == -1 {
            self.idx = 0;
        }
        if self.idx as usize >= self.samples.len() {
            return ValueType::None;
        }
        // No-op check.
        let s = self.cur();
        if s.t >= t {
            return s.typ();
        }
        // Binary search between the current position and the end.
        let start = self.idx as usize;
        self.idx += self.samples[start..].partition_point(|s| s.t < t) as isize;
        if self.idx as usize >= self.samples.len() {
            return ValueType::None;
        }
        self.cur().typ()
    }

    fn at(&self) -> (i64, f64) {
        let s = self.cur();
        (s.t, s.f)
    }

    fn at_histogram(&self, h: Option<Histogram>) -> (i64, Histogram) {
        let s = self.cur();
        match (s.h.as_deref(), h) {
            (Some(sh), Some(mut h)) => {
                sh.copy_to(&mut h);
                (s.t, h)
            }
            (Some(sh), None) => (s.t, sh.clone()),
            (None, h) => (s.t, h.unwrap_or_default()),
        }
    }

    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        let s = self.cur();
        if let Some(sfh) = s.fh.as_deref() {
            return match fh {
                Some(mut fh) => {
                    sfh.copy_to(&mut fh);
                    (s.t, fh)
                }
                None => (s.t, sfh.clone()),
            };
        }
        if let Some(sh) = s.h.as_deref() {
            // Go hSample.FH(): the histogram as a float histogram.
            let mut fh = fh.unwrap_or_default();
            sh.to_float_into(&mut fh);
            return (s.t, fh);
        }
        (s.t, fh.unwrap_or_default())
    }

    fn at_t(&self) -> i64 {
        self.cur().t
    }

    fn at_st(&self) -> i64 {
        self.cur().st
    }

    fn err(&self) -> Option<&chunkenc::Error> {
        None
    }
}

/// Iterates over a list of chunk metas (Go `listChunkSeriesIterator`).
pub struct ListChunkSeriesIterator<'a> {
    chks: Cow<'a, [Meta]>,
    idx: isize,
}

impl<'a> ListChunkSeriesIterator<'a> {
    pub fn new(chks: Cow<'a, [Meta]>) -> ListChunkSeriesIterator<'a> {
        ListChunkSeriesIterator { chks, idx: -1 }
    }

    /// Go `Reset`.
    pub fn reset(&mut self, chks: Cow<'a, [Meta]>) {
        self.chks = chks;
        self.idx = -1;
    }
}

/// Go `NewListChunkSeriesIterator`.
pub fn new_list_chunk_series_iterator(chks: Vec<Meta>) -> BoxMetaIterator<'static> {
    Box::new(ListChunkSeriesIterator::new(Cow::Owned(chks)))
}

impl MetaIterator for ListChunkSeriesIterator<'_> {
    fn next(&mut self) -> bool {
        self.idx += 1;
        (self.idx as usize) < self.chks.len()
    }
    fn at(&self) -> &Meta {
        &self.chks[self.idx as usize]
    }
    fn err(&self) -> Option<Error> {
        None
    }
}

/// A chunk iterator that only has an error (Go `errChunksIterator`).
pub struct ErrChunksIterator {
    pub err: Option<Error>,
    meta: Meta,
}

impl ErrChunksIterator {
    pub fn new(err: Error) -> ErrChunksIterator {
        ErrChunksIterator {
            err: Some(err),
            meta: Meta::default(),
        }
    }
}

impl MetaIterator for ErrChunksIterator {
    fn next(&mut self) -> bool {
        false
    }
    fn at(&self) -> &Meta {
        &self.meta
    }
    fn err(&self) -> Option<Error> {
        self.err.clone()
    }
}

/// A chunk series over chunks made from lists of samples, one chunk per list (Go
/// `NewListChunkSeriesFromSamples`). Inefficient: for tests.
pub struct ListChunkSeries {
    lset: Labels,
    chunks: Result<Vec<Meta>, Error>,
}

/// Go `NewListChunkSeriesFromSamples`.
pub fn new_list_chunk_series_from_samples(lset: Labels, samples: &[&[Sample]]) -> ChunkSeriesArc {
    Arc::new(ListChunkSeries::from_samples(lset, samples))
}

impl ListChunkSeries {
    pub fn from_samples(lset: Labels, samples: &[&[Sample]]) -> ListChunkSeries {
        let mut chks = Vec::with_capacity(samples.len());
        for s in samples {
            match chunk_from_samples(s) {
                Ok(m) => chks.push(m),
                Err(e) => {
                    return ListChunkSeries {
                        lset,
                        chunks: Err(e),
                    };
                }
            }
        }
        ListChunkSeries {
            lset,
            chunks: Ok(chks),
        }
    }

    /// A chunk series over the given metas.
    pub fn from_metas(lset: Labels, chunks: Vec<Meta>) -> ListChunkSeries {
        ListChunkSeries {
            lset,
            chunks: Ok(chunks),
        }
    }
}

impl ChunkSeries for ListChunkSeries {
    fn labels(&self) -> &Labels {
        &self.lset
    }
    fn iterator<'a>(&'a self, _reuse: Option<BoxMetaIterator<'a>>) -> BoxMetaIterator<'a> {
        match &self.chunks {
            Ok(c) => Box::new(ListChunkSeriesIterator::new(Cow::Borrowed(c))),
            Err(e) => Box::new(ErrChunksIterator::new(e.clone())),
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Conversions.

/// Decodes a chunk series set into a series set, one chunk after the other (Go `chunkSetToSeriesSet`).
pub struct ChunkSetToSeriesSet {
    set: BoxChunkSeriesSet,
    chk_iter_err: Option<Error>,
    cur: Option<SeriesArc>,
}

/// Go `NewSeriesSetFromChunkSeriesSet`.
pub fn new_series_set_from_chunk_series_set(chk: BoxChunkSeriesSet) -> BoxSeriesSet {
    Box::new(ChunkSetToSeriesSet {
        set: chk,
        chk_iter_err: None,
        cur: None,
    })
}

impl SeriesSet for ChunkSetToSeriesSet {
    fn next(&mut self) -> bool {
        if self.err().is_some() || !self.set.next() {
            return false;
        }
        let cs = self.set.at().clone();
        let mut same_series_chunks: Vec<SeriesArc> = Vec::new();
        {
            let mut iter = cs.iterator(None);
            while iter.next() {
                same_series_chunks.push(Arc::new(ChunkToSeriesDecoder::new(
                    cs.labels().clone(),
                    iter.at().clone(),
                )));
            }
            if let Some(err) = iter.err() {
                self.chk_iter_err = Some(err);
                return false;
            }
        }
        // A series composed of the chunks of the same series.
        self.cur = Some(Arc::new(ChainedSeries::new(
            cs.labels().clone(),
            same_series_chunks,
        )));
        true
    }
    fn at(&self) -> &SeriesArc {
        self.cur.as_ref().expect("At called before Next")
    }
    fn err(&self) -> Option<Error> {
        if let Some(e) = &self.chk_iter_err {
            return Some(e.clone());
        }
        self.set.err()
    }
    fn warnings(&self) -> Annotations {
        self.set.warnings()
    }
}

/// A series over the samples of one chunk (Go `newChunkToSeriesDecoder`).
pub struct ChunkToSeriesDecoder {
    lset: Labels,
    meta: Meta,
}

impl ChunkToSeriesDecoder {
    pub fn new(lset: Labels, meta: Meta) -> ChunkToSeriesDecoder {
        ChunkToSeriesDecoder { lset, meta }
    }
}

impl Series for ChunkToSeriesDecoder {
    fn labels(&self) -> &Labels {
        &self.lset
    }
    fn iterator<'a>(&'a self, reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        self.meta.chunk().iterator(reuse)
    }
}

/// Encodes a series set into a chunk series set (Go `seriesSetToChunkSet`).
pub struct SeriesSetToChunkSet {
    set: BoxSeriesSet,
    cur: Option<ChunkSeriesArc>,
}

/// Go `NewSeriesSetToChunkSet`.
pub fn new_series_set_to_chunk_set(chk: BoxSeriesSet) -> BoxChunkSeriesSet {
    Box::new(SeriesSetToChunkSet {
        set: chk,
        cur: None,
    })
}

impl ChunkSeriesSet for SeriesSetToChunkSet {
    fn next(&mut self) -> bool {
        if self.err().is_some() || !self.set.next() {
            return false;
        }
        self.cur = Some(new_series_to_chunk_encoder(self.set.at().clone()));
        true
    }
    fn at(&self) -> &ChunkSeriesArc {
        self.cur.as_ref().expect("At called before Next")
    }
    fn err(&self) -> Option<Error> {
        self.set.err()
    }
    fn warnings(&self) -> Annotations {
        self.set.warnings()
    }
}

/// Encodes the samples of a series into chunks of at most 120 samples (Go `seriesToChunkEncoder`).
pub struct SeriesToChunkEncoder {
    series: SeriesArc,
    /// The encoding for float samples. Samples with a start timestamp always use XOR2.
    float_encoding: Encoding,
}

/// Go `seriesToChunkEncoderSplit`.
pub const SERIES_TO_CHUNK_ENCODER_SPLIT: usize = 120;

/// Go `NewSeriesToChunkEncoder`.
pub fn new_series_to_chunk_encoder(series: SeriesArc) -> ChunkSeriesArc {
    new_series_to_chunk_encoder_with_float_encoding(series, chunkenc::ENC_XOR)
}

/// Go `NewSeriesToChunkEncoderWithFloatEncoding`: float samples use `float_encoding` (XOR or XOR2; anything else
/// is XOR).
pub fn new_series_to_chunk_encoder_with_float_encoding(
    series: SeriesArc,
    float_encoding: Encoding,
) -> ChunkSeriesArc {
    Arc::new(SeriesToChunkEncoder {
        series,
        float_encoding,
    })
}

impl ChunkSeries for SeriesToChunkEncoder {
    fn labels(&self) -> &Labels {
        self.series.labels()
    }
    fn iterator<'a>(&'a self, _reuse: Option<BoxMetaIterator<'a>>) -> BoxMetaIterator<'a> {
        match series_to_chunks(&*self.series, self.float_encoding) {
            Ok(chks) => new_list_chunk_series_iterator(chks),
            Err(e) => Box::new(ErrChunksIterator::new(e)),
        }
    }
}

fn append_chunk(chks: &mut Vec<Meta>, mint: i64, maxt: i64, chk: Option<Box<dyn Chunk>>) {
    if let Some(chk) = chk {
        chks.push(Meta {
            chunk_ref: 0,
            chunk: Some(Arc::from(chk)),
            min_time: mint,
            max_time: maxt,
        });
    }
}

/// The chunks of a series, cut at 120 samples and at value type changes (the body of Go
/// `seriesToChunkEncoder.Iterator`).
pub fn series_to_chunks(series: &dyn Series, float_encoding: Encoding) -> Result<Vec<Meta>, Error> {
    let mut chk: Option<Box<dyn Chunk>> = None;
    let mut app: Option<chunkenc::Appender> = None;
    let mut mint = i64::MAX;
    let mut maxt = i64::MIN;
    let mut chks = Vec::new();
    let mut i = 0usize;
    let mut series_iter = series.iterator(None);
    let mut last_type = ValueType::None;
    let mut h = Histogram::default();
    let mut fh = FloatHistogram::default();
    loop {
        let typ = series_iter.next();
        if typ == ValueType::None {
            break;
        }
        let st = series_iter.at_st();
        let has_st = st != 0;
        let desired = typ.chunk_encoding(has_st || float_encoding == chunkenc::ENC_XOR2, has_st);
        let mut cut = typ != last_type || i >= SERIES_TO_CHUNK_ENCODER_SPLIT;
        if !cut {
            let cur = chk.as_ref().expect("open chunk").encoding();
            if cur != desired {
                // XOR and XOR2 are append-compatible, so a change of the desired float encoding alone does not cut
                // the chunk, unless the sample has a start timestamp, which an open XOR chunk cannot store.
                cut = !chunkenc::compatible_values(cur, desired)
                    || (has_st && cur == chunkenc::ENC_XOR);
            }
        }
        if cut {
            append_chunk(&mut chks, mint, maxt, chk.take());
            let mut c = chunkenc::new_empty_chunk(desired)?;
            app = Some(c.appender()?);
            chk = Some(c);
            mint = i64::MAX;
            i = 0;
        }
        last_type = typ;

        let t;
        let a = app.as_mut().expect("appender");
        let c = chk.as_deref_mut().expect("chunk");
        match typ {
            ValueType::Float => {
                let (tt, v) = series_iter.at();
                t = tt;
                a.append(c, st, t, v);
            }
            ValueType::Histogram => {
                let (tt, hh) = series_iter.at_histogram(Some(std::mem::take(&mut h)));
                t = tt;
                h = hh;
                let (new_chk, recoded) = a.append_histogram(c, None, st, t, &mut h, false)?;
                if let Some((nc, na)) = new_chk {
                    if !recoded {
                        append_chunk(&mut chks, mint, maxt, chk.take());
                        mint = i64::MAX;
                        i = 0;
                    }
                    chk = Some(nc);
                    app = Some(na);
                }
            }
            ValueType::FloatHistogram => {
                let (tt, ff) = series_iter.at_float_histogram(Some(std::mem::take(&mut fh)));
                t = tt;
                fh = ff;
                let (new_chk, recoded) =
                    a.append_float_histogram(c, None, st, t, &mut fh, false)?;
                if let Some((nc, na)) = new_chk {
                    if !recoded {
                        append_chunk(&mut chks, mint, maxt, chk.take());
                        mint = i64::MAX;
                        i = 0;
                    }
                    chk = Some(nc);
                    app = Some(na);
                }
            }
            ValueType::None => unreachable!(),
        }
        maxt = t;
        if mint == i64::MAX {
            mint = t;
        }
        i += 1;
    }
    if let Some(e) = series_iter.err() {
        return Err(Error::Chunk(e.clone()));
    }
    append_chunk(&mut chks, mint, maxt, chk);
    Ok(chks)
}

/// Go `ExpandSamples`: all samples of the iterator. NaN float values are replaced by -42, so that samples
/// compare equal in tests.
pub fn expand_samples(iter: &mut dyn ChunkIterator) -> Result<Vec<Sample>, Error> {
    expand(iter, true)
}

/// Go `ExpandSamplesWithoutReplacingNaNs`.
pub fn expand_samples_without_replacing_nans(
    iter: &mut dyn ChunkIterator,
) -> Result<Vec<Sample>, Error> {
    expand(iter, false)
}

fn expand(iter: &mut dyn ChunkIterator, replace_nan: bool) -> Result<Vec<Sample>, Error> {
    let mut result = Vec::new();
    loop {
        match iter.next() {
            ValueType::None => {
                return match iter.err() {
                    Some(e) => Err(Error::Chunk(e.clone())),
                    None => Ok(result),
                };
            }
            ValueType::Float => {
                let (t, mut f) = iter.at();
                let st = iter.at_st();
                // NaNs cannot be compared normally, so substitute another value.
                if replace_nan && f.is_nan() {
                    f = -42.0;
                }
                result.push(Sample::float(st, t, f));
            }
            ValueType::Histogram => {
                let (t, h) = iter.at_histogram(None);
                let st = iter.at_st();
                result.push(Sample::histogram(st, t, h));
            }
            ValueType::FloatHistogram => {
                let (t, fh) = iter.at_float_histogram(None);
                let st = iter.at_st();
                result.push(Sample::float_histogram(st, t, fh));
            }
        }
    }
}

/// Go `ExpandChunks`: all chunks of the iterator.
pub fn expand_chunks(iter: &mut dyn MetaIterator) -> Result<Vec<Meta>, Error> {
    let mut result = Vec::new();
    while iter.next() {
        result.push(iter.at().clone());
    }
    match iter.err() {
        Some(e) => Err(e),
        None => Ok(result),
    }
}
