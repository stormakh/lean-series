//! Appending to the head (port of `tsdb/head_append.go`): the V1 appender, the series append and insert
//! (out-of-order) logic, chunk cutting, commit (WAL and WBL records) and rollback.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use chunkenc::{Chunk, Encoding, ValueType};
use model::exemplar::Exemplar;
use model::histogram::{CounterResetHint, FloatHistogram, Histogram};
use model::labels::Labels;
use model::metadata::{self, Metadata};
use model::value::is_stale_nan;
use storage::{
    AppendOptions, Appender, AppenderTransaction, Context, Error, GetRef, SeriesRef, ERR_DUPLICATE_EXEMPLAR,
    ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP, ERR_EXEMPLARS_DISABLED, ERR_OUT_OF_BOUNDS, ERR_OUT_OF_ORDER_EXEMPLAR,
    ERR_OUT_OF_ORDER_SAMPLE, ERR_OUT_OF_ORDER_ST, ERR_ST_NEWER_THAN_SAMPLE, ERR_TOO_OLD_SAMPLE,
};
use tsdb_format::chunks::{ChunkDiskMapper, ChunkDiskMapperRef};
use tsdb_format::record::{
    self, RefExemplar, RefFloatHistogramSample, RefHistogramSample, RefMetadata, RefMmapMarker, RefSample, RefSeries,
};

use crate::head::{
    handle_chunk_write_error, wlog_err, Head, HeadSeriesRef, MemChunk, MemSeries, MemSeriesInner, MmappedChunk,
    OooHeadChunk, SeriesAppender, U64Map, ERR_APPENDER_CLOSED, ERR_INVALID_SAMPLE, OOO_CHUNK_ID_MASK,
    SAMPLE_METRIC_TYPE_FLOAT, SAMPLE_METRIC_TYPE_HISTOGRAM,
};
use crate::ooo_head::OooChunk;
use crate::Logger;

/// Go `rangeForTimestamp`: the upper bound of the chunk range `t` is in.
#[inline]
pub fn range_for_timestamp(t: i64, width: i64) -> i64 {
    (t / width) * width + width
}

/// Go `computeChunkEndTime`: estimates the end timestamp of a chunk, assuming that the time range is
/// 1/`ratio_to_full` full and samples keep arriving at the same rate.
pub fn compute_chunk_end_time(start: i64, cur: i64, max_t: i64, ratio_to_full: f64) -> i64 {
    let n = (max_t - start) as f64 / ((cur - start + 1) as f64 * ratio_to_full);
    if n <= 1.0 {
        return max_t;
    }
    (start as f64 + (max_t - start) as f64 / n.floor()) as i64
}

/// Go `chunkOpts`: chunk-level options for appending to a series.
#[derive(Clone, Copy)]
pub struct ChunkOpts<'a> {
    pub chunk_disk_mapper: Option<&'a ChunkDiskMapper>,
    pub chunk_range: i64,
    pub samples_per_chunk: usize,
    /// Selects XOR2 encoding for float chunks.
    pub use_xor2: bool,
    /// Selects the ST-capable encodings for histogram chunks.
    pub use_histogram_st: bool,
    /// Whether start-timestamp storage is enabled.
    pub store_st: bool,
}

impl Default for ChunkOpts<'_> {
    fn default() -> Self {
        ChunkOpts {
            chunk_disk_mapper: None,
            chunk_range: crate::head::DEFAULT_BLOCK_DURATION,
            samples_per_chunk: crate::head::DEFAULT_SAMPLES_PER_CHUNK,
            use_xor2: false,
            use_histogram_st: false,
            store_st: false,
        }
    }
}

/// The result of an appendable check: whether the sample is out of order, the out-of-order delta, and the
/// error (Go returns all three).
pub type Appendable = (bool, i64, Option<Error>);

fn appendable_tail(t: i64, head_maxt: i64, min_valid_time: i64, ooo_time_window: i64) -> Appendable {
    // The sample cannot go in the in-order chunk. Check if it can go in the out-of-order chunk.
    if ooo_time_window > 0 && t >= head_maxt.wrapping_sub(ooo_time_window) {
        return (true, head_maxt.wrapping_sub(t), None);
    }
    // The sample cannot go in both in-order and out-of-order chunk.
    if ooo_time_window > 0 {
        return (true, head_maxt.wrapping_sub(t), Some(ERR_TOO_OLD_SAMPLE));
    }
    if t < min_valid_time {
        return (false, head_maxt.wrapping_sub(t), Some(ERR_OUT_OF_BOUNDS));
    }
    (false, head_maxt.wrapping_sub(t), Some(ERR_OUT_OF_ORDER_SAMPLE))
}

impl MemSeriesInner {
    /// Go `appendable`: whether the float sample can be appended in order (false, no error), belongs to the
    /// out-of-order chunk (true, no error), or cannot be handled (error).
    pub fn appendable(&self, t: i64, v: f64, head_maxt: i64, min_valid_time: i64, ooo_time_window: i64) -> Appendable {
        // Check if we can append in the in-order chunk.
        if t >= min_valid_time {
            if self.head_chunks.is_empty() {
                // The series has no sample and was freshly created.
                return (false, 0, None);
            }
            let ms_maxt = self.max_time();
            if t > ms_maxt {
                return (false, 0, None);
            }
            if t == ms_maxt {
                // We are allowing exact duplicates as we can encounter them in valid cases like federation and
                // erroring out at that time would be extremely noisy. This only checks against the latest
                // in-order sample.
                if self.last_histogram_value().is_some() || self.last_float_histogram_value().is_some() {
                    return (false, 0, Some(storage::new_duplicate_histogram_to_float_err(t, v)));
                }
                if self.last_value.to_bits() != v.to_bits() {
                    return (false, 0, Some(storage::new_duplicate_float_err(t, self.last_value, v)));
                }
                // Sample is identical (ts + value) with most current (highest ts) sample in sampleBuf.
                return (false, 0, None);
            }
        }
        appendable_tail(t, head_maxt, min_valid_time, ooo_time_window)
    }

    /// Go `appendableHistogram`.
    pub fn appendable_histogram(
        &self,
        t: i64,
        h: &Histogram,
        head_maxt: i64,
        min_valid_time: i64,
        ooo_time_window: i64,
    ) -> Appendable {
        if t >= min_valid_time {
            if self.head_chunks.is_empty() {
                return (false, 0, None);
            }
            let ms_maxt = self.max_time();
            if t > ms_maxt {
                return (false, 0, None);
            }
            if t == ms_maxt {
                if !self.last_histogram_value().is_some_and(|l| h.equals(l)) {
                    return (false, 0, Some(ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP));
                }
                return (false, 0, None);
            }
        }
        appendable_tail(t, head_maxt, min_valid_time, ooo_time_window)
    }

    /// Go `appendableFloatHistogram`.
    pub fn appendable_float_histogram(
        &self,
        t: i64,
        fh: &FloatHistogram,
        head_maxt: i64,
        min_valid_time: i64,
        ooo_time_window: i64,
    ) -> Appendable {
        if t >= min_valid_time {
            if self.head_chunks.is_empty() {
                return (false, 0, None);
            }
            let ms_maxt = self.max_time();
            if t > ms_maxt {
                return (false, 0, None);
            }
            if t == ms_maxt {
                if !self.last_float_histogram_value().is_some_and(|l| fh.equals(l)) {
                    return (false, 0, Some(ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP));
                }
                return (false, 0, None);
            }
        }
        appendable_tail(t, head_maxt, min_valid_time, ooo_time_window)
    }

    /// Go `append`: adds the sample (t, v). `append_id` is for isolation (0: no isolation for this append).
    /// Returns (sample in order, chunk created).
    pub fn append(&mut self, st: i64, t: i64, v: f64, append_id: u64, o: &ChunkOpts<'_>, lset: &Labels, sref: HeadSeriesRef) -> (bool, bool) {
        let enc = ValueType::Float.chunk_encoding(o.use_xor2, o.use_histogram_st);
        let (in_order, chunk_created, _) = self.append_preprocessor(t, enc, o, lset, sref);
        if !in_order {
            return (false, chunk_created);
        }
        let c = self.head_chunks.last_mut().expect("head chunk");
        self.app.append(c.chunk_mut(), st, t, v);
        c.max_time = t;
        self.last_value = v;
        self.set_last_histogram_values(None, None);
        if append_id > 0
            && let Some(txs) = self.txs.as_mut()
        {
            txs.add(append_id);
        }
        (true, chunk_created)
    }

    /// Go `appendHistogram`. In case of recoding the existing chunk, a new chunk replaces the old one and
    /// chunk created is false (to keep the meaning of the chunk metrics).
    #[allow(clippy::too_many_arguments)]
    pub fn append_histogram(
        &mut self,
        st: i64,
        t: i64,
        mut h: Arc<Histogram>,
        append_id: u64,
        o: &ChunkOpts<'_>,
        lset: &Labels,
        sref: HeadSeriesRef,
    ) -> (bool, bool) {
        // Head controls the execution of recoding, so that we own the proper chunk reference afterwards and mmap
        // used up chunks.
        let enc = ValueType::Histogram.chunk_encoding(o.use_xor2, o.use_histogram_st);
        let (in_order, chunk_created, prev_app) = self.histograms_append_preprocessor(t, enc, o, lset, sref);
        if !in_order {
            return (false, chunk_created);
        }
        // Ignore the previous appender if we continue the current chunk.
        let prev_app = if chunk_created { prev_app } else { None };
        let c = self.head_chunks.last_mut().expect("head chunk");
        let SeriesAppender::Other(app) = &mut self.app else {
            panic!("histogram append without a histogram appender")
        };
        let prev = prev_app.as_ref().and_then(|p| p.as_appender());
        let res = app.append_histogram(c.chunk_mut(), prev, st, t, Arc::make_mut(&mut h), false);
        let new_chunk = match res {
            Ok((n, recoded)) => n.map(|x| (x, recoded)),
            Err(_) => None,
        };
        self.set_last_histogram_values(Some(h), None);
        self.finish_histogram_append(t, append_id, new_chunk, chunk_created, o, lset, sref)
    }

    /// Go `appendFloatHistogram`.
    #[allow(clippy::too_many_arguments)]
    pub fn append_float_histogram(
        &mut self,
        st: i64,
        t: i64,
        mut fh: Arc<FloatHistogram>,
        append_id: u64,
        o: &ChunkOpts<'_>,
        lset: &Labels,
        sref: HeadSeriesRef,
    ) -> (bool, bool) {
        let enc = ValueType::FloatHistogram.chunk_encoding(o.use_xor2, o.use_histogram_st);
        let (in_order, chunk_created, prev_app) = self.histograms_append_preprocessor(t, enc, o, lset, sref);
        if !in_order {
            return (false, chunk_created);
        }
        let prev_app = if chunk_created { prev_app } else { None };
        let c = self.head_chunks.last_mut().expect("head chunk");
        let SeriesAppender::Other(app) = &mut self.app else {
            panic!("float histogram append without a float histogram appender")
        };
        let prev = prev_app.as_ref().and_then(|p| p.as_appender());
        let res = app.append_float_histogram(c.chunk_mut(), prev, st, t, Arc::make_mut(&mut fh), false);
        let new_chunk = match res {
            Ok((n, recoded)) => n.map(|x| (x, recoded)),
            Err(_) => None,
        };
        self.set_last_histogram_values(None, Some(fh));
        self.finish_histogram_append(t, append_id, new_chunk, chunk_created, o, lset, sref)
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_histogram_append(
        &mut self,
        t: i64,
        append_id: u64,
        new_chunk: Option<((Box<dyn Chunk>, chunkenc::Appender), bool)>,
        chunk_created: bool,
        o: &ChunkOpts<'_>,
        lset: &Labels,
        sref: HeadSeriesRef,
    ) -> (bool, bool) {
        if append_id > 0
            && let Some(txs) = self.txs.as_mut()
        {
            txs.add(append_id);
        }
        let Some(((nc, napp), recoded)) = new_chunk else {
            // Sample was appended to existing chunk or is the first sample in a new chunk.
            self.head_chunks.last_mut().expect("head chunk").max_time = t;
            return (true, chunk_created);
        };
        self.app = SeriesAppender::from_appender(napp);
        if recoded {
            // The appender needed to recode the chunk.
            let c = self.head_chunks.last_mut().expect("head chunk");
            c.max_time = t;
            c.chunk = Arc::from(nc);
            return (true, false);
        }
        self.push_head_chunk(
            MemChunk {
                chunk: Arc::from(nc),
                min_time: t,
                max_time: t,
            },
            lset,
            sref,
        );
        self.next_at = range_for_timestamp(t, o.chunk_range);
        (true, true)
    }

    /// Go `appendPreprocessor`: cuts new XOR chunks based on the number of samples, with a soft cap in bytes.
    /// Returns (sample in order, chunk created, the appender replaced by a cut).
    pub(crate) fn append_preprocessor(
        &mut self,
        t: i64,
        e: Encoding,
        o: &ChunkOpts<'_>,
        lset: &Labels,
        sref: HeadSeriesRef,
    ) -> (bool, bool, Option<SeriesAppender>) {
        let mut chunk_created = false;
        let mut prev = None;
        if self.head_chunks.is_empty() {
            if let Some(l) = self.mmapped_chunks.last()
                && l.max_time >= t
            {
                // Out of order sample. Sample timestamp is already in the mmapped chunks, so ignore it.
                return (false, false, None);
            }
            // There is no head chunk in this series yet, create the first chunk for the sample.
            prev = Some(self.cut_new_head_chunk(t, e, o.chunk_range, lset, sref));
            chunk_created = true;
        }
        let c = self.head_chunks.last().expect("head chunk");
        // Out of order sample.
        if c.max_time >= t {
            return (false, chunk_created, prev);
        }
        // Check the chunk size, unless we just created it and if the chunk is too large, cut a new one.
        if !chunk_created && c.chunk.bytes().len() > chunkenc::MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND {
            prev = Some(self.cut_new_head_chunk(t, e, o.chunk_range, lset, sref));
            chunk_created = true;
        }
        // XOR and XOR2 are compatible float encodings when ST storage is disabled; with ST storage they must not
        // be mixed within a single chunk.
        let ce = self.head_chunks.last().expect("head chunk").chunk.encoding();
        if ce != e && (!chunkenc::compatible_values(ce, e) || o.store_st) {
            prev = Some(self.cut_new_head_chunk(t, e, o.chunk_range, lset, sref));
            chunk_created = true;
        }
        let c = self.head_chunks.last_mut().expect("head chunk");
        let num_samples = c.chunk.num_samples();
        if num_samples == 0 {
            // It could be the new chunk created after reading the chunk snapshot, hence we fix the minTime of
            // the chunk here.
            c.min_time = t;
            self.next_at = range_for_timestamp(c.min_time, o.chunk_range);
        }
        // If we reach 25% of a chunk's desired sample count, predict an end time for this chunk that will try to
        // make samples equally distributed within the remaining chunks in the current chunk range.
        if num_samples == o.samples_per_chunk / 4 {
            self.next_at = compute_chunk_end_time(c.min_time, c.max_time, self.next_at, 4.0);
        }
        // If numSamples > samplesPerChunk*2 then our previous prediction was invalid, most likely because
        // samples rate has changed and now they are arriving more frequently.
        if t >= self.next_at || num_samples >= o.samples_per_chunk * 2 {
            prev = Some(self.cut_new_head_chunk(t, e, o.chunk_range, lset, sref));
            chunk_created = true;
        }
        (true, chunk_created, prev)
    }

    /// Go `histogramsAppendPreprocessor`: cuts new histogram chunks based on their size in bytes.
    pub(crate) fn histograms_append_preprocessor(
        &mut self,
        t: i64,
        e: Encoding,
        o: &ChunkOpts<'_>,
        lset: &Labels,
        sref: HeadSeriesRef,
    ) -> (bool, bool, Option<SeriesAppender>) {
        let mut chunk_created = false;
        let mut prev = None;
        if self.head_chunks.is_empty() {
            if let Some(l) = self.mmapped_chunks.last()
                && l.max_time >= t
            {
                return (false, false, None);
            }
            prev = Some(self.cut_new_head_chunk(t, e, o.chunk_range, lset, sref));
            chunk_created = true;
        }
        if self.head_chunks.last().expect("head chunk").max_time >= t {
            return (false, chunk_created, prev);
        }
        if self.head_chunks.last().expect("head chunk").chunk.encoding() != e {
            // The chunk encoding expected by this append is different than the head chunk's encoding. So we cut
            // a new chunk with the expected encoding.
            prev = Some(self.cut_new_head_chunk(t, e, o.chunk_range, lset, sref));
            chunk_created = true;
        }
        let target_bytes = chunkenc::TARGET_BYTES_PER_HISTOGRAM_CHUNK;
        let (num_samples, num_bytes) = {
            let c = self.head_chunks.last().expect("head chunk");
            (c.chunk.num_samples(), c.chunk.bytes().len())
        };
        if num_samples == 0 {
            let c = self.head_chunks.last_mut().expect("head chunk");
            c.min_time = t;
            self.next_at = range_for_timestamp(t, o.chunk_range);
        }
        let (c_min, c_max) = {
            let c = self.head_chunks.last().expect("head chunk");
            (c.min_time, c.max_time)
        };
        // Below, we will enforce chunkenc.MinSamplesPerHistogramChunk. There are, however, two cases that
        // supersede it: the current chunk range ending before it is satisfied, and s.nextAt set while loading a
        // chunk snapshot with the intent that a new chunk be cut on the next append.
        let next_chunk_range_start = if self.has_computed_histogram_chunk_end_time() {
            range_for_timestamp(c_min, o.chunk_range)
        } else {
            self.next_at
        };
        // If we reach 25% of a chunk's desired maximum size, predict an end time for this chunk.
        if !self.has_computed_histogram_chunk_end_time() && num_bytes >= target_bytes / 4 {
            let ratio_to_full = target_bytes as f64 / num_bytes as f64;
            self.next_at = compute_chunk_end_time(c_min, c_max, self.next_at, ratio_to_full);
            self.set_computed_histogram_chunk_end_time(true);
        }
        // If numBytes > targetBytes*2 then our previous prediction was invalid.
        if (t >= self.next_at || num_bytes >= target_bytes * 2)
            && (num_samples >= chunkenc::MIN_SAMPLES_PER_HISTOGRAM_CHUNK || t >= next_chunk_range_start)
        {
            prev = Some(self.cut_new_head_chunk(t, e, o.chunk_range, lset, sref));
            chunk_created = true;
        }
        // The new chunk will also need a new computed end time.
        if chunk_created {
            self.set_computed_histogram_chunk_end_time(false);
        }
        (true, chunk_created, prev)
    }

    /// Go `cutNewHeadChunk`: cuts a new head chunk; returns the replaced appender.
    pub fn cut_new_head_chunk(
        &mut self,
        mint: i64,
        e: Encoding,
        chunk_range: i64,
        lset: &Labels,
        sref: HeadSeriesRef,
    ) -> SeriesAppender {
        let mut chunk: Box<dyn Chunk> = if chunkenc::is_valid_encoding(e) {
            chunkenc::new_empty_chunk(e).expect("valid encoding") // This should never fail.
        } else {
            Box::new(chunkenc::XorChunk::new())
        };
        let app = chunk.appender().expect("appender of an empty chunk");
        // When cutting a new head chunk the previous ones are kept until they are m-mapped.
        self.push_head_chunk(
            MemChunk {
                chunk: Arc::from(chunk),
                min_time: mint,
                max_time: i64::MIN,
            },
            lset,
            sref,
        );
        // Set upper bound on when the next chunk must be started. An earlier timestamp may be chosen
        // dynamically at a later point.
        self.next_at = range_for_timestamp(mint, chunk_range);
        std::mem::replace(&mut self.app, SeriesAppender::from_appender(app))
    }

    /// Go `insert`: like append, except it inserts. Used for OOO samples. Returns (inserted, chunk created,
    /// m-map references of the chunks written to disk).
    #[allow(clippy::too_many_arguments)]
    pub fn insert(
        &mut self,
        st: i64,
        t: i64,
        v: f64,
        h: Option<Box<Histogram>>,
        fh: Option<Box<FloatHistogram>>,
        o: &ChunkOpts<'_>,
        ooo_cap_max: i64,
        logger: &Logger,
        lset: &Labels,
        sref: HeadSeriesRef,
    ) -> (bool, bool, Vec<ChunkDiskMapperRef>) {
        let mut chunk_created = false;
        let mut mmap_refs = Vec::new();
        let need_cut = match self.ooo_or_init().ooo_head_chunk.as_ref() {
            None => true,
            Some(c) => c.chunk.num_samples() as i64 == ooo_cap_max,
        };
        if need_cut {
            // Note: If no new samples come in then we rely on compaction to clean up stale in-memory OOO chunks.
            mmap_refs = self.cut_new_ooo_head_chunk(t, o, logger, lset, sref);
            chunk_created = true;
        }
        let c = self
            .ooo_mut()
            .and_then(|o| o.ooo_head_chunk.as_mut())
            .expect("ooo head chunk");
        let ok = c.chunk.insert(st, t, v, h, fh);
        if ok {
            if chunk_created || t < c.min_time {
                c.min_time = t;
            }
            if chunk_created || t > c.max_time {
                c.max_time = t;
            }
        }
        (ok, chunk_created, mmap_refs)
    }

    /// Go `cutNewOOOHeadChunk`: cuts a new OOO chunk and m-maps the old one.
    pub fn cut_new_ooo_head_chunk(
        &mut self,
        mint: i64,
        o: &ChunkOpts<'_>,
        logger: &Logger,
        lset: &Labels,
        sref: HeadSeriesRef,
    ) -> Vec<ChunkDiskMapperRef> {
        let refs = self.mmap_current_ooo_head_chunk(o, logger, lset, sref);
        self.ooo_or_init().ooo_head_chunk = Some(OooHeadChunk {
            chunk: OooChunk::new(),
            min_time: mint,
            max_time: i64::MIN,
        });
        refs
    }

    /// Go `mmapCurrentOOOHeadChunk`.
    pub fn mmap_current_ooo_head_chunk(
        &mut self,
        o: &ChunkOpts<'_>,
        logger: &Logger,
        lset: &Labels,
        sref: HeadSeriesRef,
    ) -> Vec<ChunkDiskMapperRef> {
        let Some(ooo) = self.ooo_mut() else {
            return Vec::new();
        };
        let Some(hc) = ooo.ooo_head_chunk.as_mut() else {
            // OOO is not enabled or there is no head chunk, so nothing to m-map here.
            return Vec::new();
        };
        let chks = match hc.chunk.to_encoded_chunks(i64::MIN, i64::MAX, o.use_xor2, o.use_histogram_st) {
            Ok(c) => c,
            Err(e) => {
                handle_chunk_write_error(Err(tsdb_format::chunks::Error::Chunk(e)));
                return Vec::new();
            }
        };
        let cdm = o.chunk_disk_mapper.expect("chunk disk mapper");
        let mut chunk_refs = Vec::with_capacity(chks.len());
        for memchunk in chks {
            if ooo.ooo_mmapped_chunks.len() >= (OOO_CHUNK_ID_MASK - 1) as usize {
                logger.error("Too many OOO chunks, dropping data", &[("series", lset)]);
                break;
            }
            let num_samples = memchunk.chunk.num_samples() as u16;
            let encoding = memchunk.chunk.encoding();
            let chunk_ref = cdm.write_chunk(
                sref,
                memchunk.min_time,
                memchunk.max_time,
                memchunk.chunk,
                true,
                Some(Box::new(handle_chunk_write_error)),
            );
            chunk_refs.push(chunk_ref);
            ooo.ooo_mmapped_chunks.push(MmappedChunk {
                r#ref: chunk_ref,
                num_samples,
                encoding,
                min_time: memchunk.min_time,
                max_time: memchunk.max_time,
            });
        }
        ooo.ooo_head_chunk = None;
        chunk_refs
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Appenders.

/// Go `sampleType`: sample types to distinguish for append batching.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SampleType {
    /// The sample type does not matter.
    #[default]
    None,
    Float,
    Histogram,
    CustomBucketHistogram,
    FloatHistogram,
    CustomBucketFloatHistogram,
}

/// Go `exemplarWithSeriesRef`.
#[derive(Clone, Debug)]
pub struct ExemplarWithSeriesRef {
    pub r#ref: SeriesRef,
    pub exemplar: Exemplar,
}

/// Go `appendBatch`: appended data where every series receives samples of only one type.
#[derive(Default)]
pub struct AppendBatch {
    pub floats: Vec<RefSample>,
    pub float_series: Vec<Arc<MemSeries>>,
    pub histograms: Vec<RefHistogramSample>,
    pub histogram_series: Vec<Arc<MemSeries>>,
    pub float_histograms: Vec<RefFloatHistogramSample>,
    pub float_histogram_series: Vec<Arc<MemSeries>>,
    pub metadata: Vec<RefMetadata>,
    pub metadata_series: Vec<Arc<MemSeries>>,
    pub exemplars: Vec<ExemplarWithSeriesRef>,
}

/// Go `headAppenderBase`: the state shared by the V1 and V2 appenders.
pub struct HeadAppenderBase {
    pub(crate) head: Arc<Head>,
    /// No samples below this timestamp are allowed.
    pub(crate) min_valid_time: i64,
    /// Tracked here to not take the lock for every sample appended.
    pub(crate) head_maxt: i64,
    /// The same for the entire append.
    pub(crate) ooo_time_window: i64,
    /// New series records held by this appender.
    pub(crate) series_refs: Vec<RefSeries>,
    /// New series held by this appender (same indexes as `series_refs`).
    pub(crate) series: Vec<Arc<MemSeries>>,
    pub(crate) batches: Vec<AppendBatch>,
    /// Which (one) sample type each series holds in the most recent batch.
    pub(crate) types_in_batch: U64Map<SampleType>,
    pub(crate) append_id: u64,
    pub(crate) cleanup_append_ids_below: u64,
    pub(crate) closed: bool,
    pub(crate) store_st: bool,
    pub(crate) use_xor2: bool,
    pub(crate) use_histogram_st: bool,
}

/// Go `appenderCommitContext`.
struct AppenderCommitContext<'a> {
    floats_appended: i64,
    histograms_appended: i64,
    ooo_floats_accepted: i64,
    ooo_histogram_accepted: i64,
    float_ooo_rejected: i64,
    histo_ooo_rejected: i64,
    float_too_old_rejected: i64,
    histo_too_old_rejected: i64,
    float_oob_rejected: i64,
    histo_oob_rejected: i64,
    in_order_mint: i64,
    in_order_maxt: i64,
    append_chunk_opts: ChunkOpts<'a>,
    ooo_min_t: i64,
    ooo_max_t: i64,
    wbl_samples: Vec<RefSample>,
    wbl_histograms: Vec<RefHistogramSample>,
    wbl_float_histograms: Vec<RefFloatHistogramSample>,
    ooo_mmap_markers: Option<HashMap<HeadSeriesRef, Vec<ChunkDiskMapperRef>>>,
    ooo_mmap_markers_count: usize,
    ooo_records: Vec<Vec<u8>>,
    ooo_cap_max: i64,
    ooo_enc: record::Encoder,
}

impl AppenderCommitContext<'_> {
    /// Go `collectOOORecords`.
    fn collect_ooo_records(&mut self, has_wbl: bool) {
        if !has_wbl {
            // WBL is not enabled. So no need to collect.
            self.wbl_samples.clear();
            self.wbl_histograms.clear();
            self.wbl_float_histograms.clear();
            self.ooo_mmap_markers = None;
            self.ooo_mmap_markers_count = 0;
            return;
        }
        // The m-map happens before adding a new sample. So we collect the m-map markers first, and then samples.
        if let Some(m) = self.ooo_mmap_markers.take() {
            let mut markers = Vec::with_capacity(self.ooo_mmap_markers_count);
            for (r, mmap_refs) in m {
                for mmap_ref in mmap_refs {
                    markers.push(RefMmapMarker { r#ref: r, mmap_ref });
                }
            }
            let mut b = Vec::new();
            self.ooo_enc.mmap_markers(&markers, &mut b);
            self.ooo_records.push(b);
        }
        if !self.wbl_samples.is_empty() {
            let mut b = Vec::new();
            self.ooo_enc.samples(&self.wbl_samples, &mut b);
            self.ooo_records.push(b);
        }
        if !self.wbl_histograms.is_empty() {
            let mut b = Vec::new();
            let custom = self.ooo_enc.histogram_samples(&self.wbl_histograms, &mut b);
            if !b.is_empty() {
                self.ooo_records.push(b);
            }
            if !custom.is_empty() {
                let mut b = Vec::new();
                self.ooo_enc.custom_buckets_histogram_samples(&custom, &mut b);
                self.ooo_records.push(b);
            }
        }
        if !self.wbl_float_histograms.is_empty() {
            let mut b = Vec::new();
            let custom = self.ooo_enc.float_histogram_samples(&self.wbl_float_histograms, &mut b);
            if !b.is_empty() {
                self.ooo_records.push(b);
            }
            if !custom.is_empty() {
                let mut b = Vec::new();
                self.ooo_enc.custom_buckets_float_histogram_samples(&custom, &mut b);
                self.ooo_records.push(b);
            }
        }
        self.wbl_samples.clear();
        self.wbl_histograms.clear();
        self.wbl_float_histograms.clear();
        self.ooo_mmap_markers = None;
    }

    /// The marker bookkeeping after an OOO insert created a chunk.
    fn on_ooo_chunk_created(&mut self, sref: HeadSeriesRef, mmap_refs: Vec<ChunkDiskMapperRef>, has_wbl: bool) {
        let flush = match self.ooo_mmap_markers.as_ref().and_then(|m| m.get(&sref)) {
            // No markers collected for these samples yet: first flush the samples before setting this marker.
            None => true,
            // We have already m-mapped a chunk for this series in the same Commit(): add the samples and markers
            // seen till now to the WBL records before m-mapping again.
            Some(_) => true,
        };
        if flush {
            self.collect_ooo_records(has_wbl);
        }
        let m = self.ooo_mmap_markers.get_or_insert_with(HashMap::new);
        if !mmap_refs.is_empty() {
            self.ooo_mmap_markers_count += mmap_refs.len();
            m.insert(sref, mmap_refs);
        } else {
            // No chunk was written to disk, so we need to set an initial marker for this series.
            m.insert(sref, vec![ChunkDiskMapperRef(0)]);
            self.ooo_mmap_markers_count += 1;
        }
    }
}

/// Go `handleAppendableError`.
fn handle_appendable_error(err: &Error, appended: &mut i64, ooo_rejected: &mut i64, oob_rejected: &mut i64, too_old_rejected: &mut i64) {
    *appended -= 1;
    if err.is(&ERR_OUT_OF_ORDER_SAMPLE) {
        *ooo_rejected += 1;
    } else if err.is(&ERR_OUT_OF_BOUNDS) {
        *oob_rejected += 1;
    } else if err.is(&ERR_TOO_OLD_SAMPLE) {
        *too_old_rejected += 1;
    }
}

impl HeadAppenderBase {
    pub(crate) fn new(head: Arc<Head>) -> HeadAppenderBase {
        let min_valid_time = head.appendable_min_valid_time();
        // Every appender gets an ID that is cleared upon commit/rollback.
        let (append_id, cleanup_append_ids_below) = head.iso.new_append_id(min_valid_time);
        HeadAppenderBase {
            min_valid_time,
            head_maxt: head.max_time(),
            ooo_time_window: head.opts.out_of_order_time_window.load(Ordering::Relaxed),
            series_refs: Vec::new(),
            series: Vec::new(),
            batches: Vec::new(),
            types_in_batch: U64Map::default(),
            append_id,
            cleanup_append_ids_below,
            closed: false,
            store_st: head.opts.enable_st_storage.load(Ordering::Relaxed),
            use_xor2: head.opts.use_xor2_float_encoding(),
            use_histogram_st: head.opts.enable_histogram_st_encoding.load(Ordering::Relaxed),
            head,
        }
    }

    /// Go `getOrCreate`: validates the labels and gets or creates the series.
    pub(crate) fn get_or_create(&mut self, lset: &Labels) -> Result<(Arc<MemSeries>, bool), Error> {
        // Ensure no empty labels have gotten through.
        let lset = lset.without_empty();
        if lset.is_empty() {
            return Err(Error::wrap("empty labelset", ERR_INVALID_SAMPLE));
        }
        if let Some(l) = lset.has_duplicate_label_names() {
            return Err(Error::wrap(format!("label name \"{}\" is not unique", l), ERR_INVALID_SAMPLE));
        }
        let (s, created) = self.head.get_or_create(lset.hash(), &lset, true)?;
        if created {
            self.series_refs.push(RefSeries {
                r#ref: s.r#ref,
                labels: lset,
            });
            self.series.push(s.clone());
        }
        Ok((s, created))
    }

    /// Go `lockForAppend`: locks `s` (or, if GC removed it from the head index meanwhile, a live series for the
    /// same labels) and runs `f` on it. Returns the series that was locked and the result of `f`.
    pub(crate) fn lock_for_append<R>(
        &mut self,
        mut s: Arc<MemSeries>,
        f: impl FnOnce(&Arc<MemSeries>, &mut MemSeriesInner) -> R,
    ) -> Result<(Arc<MemSeries>, R), Error> {
        loop {
            {
                let mut g = s.lock();
                if !g.is_gced() {
                    let r = f(&s, &mut g);
                    drop(g);
                    return Ok((s, r));
                }
            }
            let lset = s.lset.clone();
            s = self.get_or_create(&lset)?.0;
        }
    }

    /// Go `getCurrentBatch`: the current batch if it fits the sample type for the series, else a new one.
    pub(crate) fn get_current_batch(&mut self, st: SampleType, s: HeadSeriesRef) -> &mut AppendBatch {
        let new_batch = |this: &mut HeadAppenderBase| {
            this.types_in_batch.clear();
            match st {
                SampleType::Histogram
                | SampleType::FloatHistogram
                | SampleType::CustomBucketHistogram
                | SampleType::CustomBucketFloatHistogram => {
                    // We only record histogram sample types in the map. Floats are implicit.
                    this.types_in_batch.insert(s, st);
                }
                _ => {}
            }
            this.batches.push(AppendBatch::default());
        };
        if self.batches.is_empty() {
            // First batch ever. Create it.
            new_batch(self);
        } else if st != SampleType::None {
            match self.types_in_batch.get(&s).copied() {
                Some(prev) if prev == st => {}
                None if st == SampleType::Float => {}
                _ if st == SampleType::Float => new_batch(self),
                None => {
                    // A new series of some histogram type, or some histogram type being appended to an old
                    // float series. Histograms after floats are fine.
                    self.types_in_batch.insert(s, st);
                }
                Some(_) => new_batch(self),
            }
        }
        self.batches.last_mut().expect("batch")
    }

    /// Go `GetRef`.
    pub fn get_ref(&self, lset: &Labels, hash: u64) -> (SeriesRef, Labels) {
        match self.head.series.get_by_hash(hash, lset) {
            None => (0, Labels::empty()),
            // Returned labels must be suitable to pass to Append().
            Some(s) => (s.r#ref, s.lset.clone()),
        }
    }

    /// Go `log`: writes all the appender's data to the WAL.
    pub(crate) fn log(&self) -> Result<(), Error> {
        let Some(wal) = &self.head.wal else {
            return Ok(());
        };
        let enc = record::Encoder {
            enable_st_storage: self.store_st,
        };
        let mut buf: Vec<u8> = Vec::new();
        let log = |buf: &[u8], what: &str| wal.log(buf).map_err(|e| Error::wrap(what, wlog_err(e)));
        if !self.series_refs.is_empty() {
            enc.series(&self.series_refs, &mut buf);
            log(&buf, "log series")?;
        }
        for b in &self.batches {
            if !b.metadata.is_empty() {
                buf.clear();
                enc.metadata(&b.metadata, &mut buf);
                log(&buf, "log metadata")?;
            }
            // It's important to do (float) Samples before histogram samples to end up with the correct order.
            if !b.floats.is_empty() {
                buf.clear();
                enc.samples(&b.floats, &mut buf);
                log(&buf, "log samples")?;
            }
            if !b.histograms.is_empty() {
                buf.clear();
                let custom = enc.histogram_samples(&b.histograms, &mut buf);
                if !buf.is_empty() {
                    log(&buf, "log histograms")?;
                }
                if !custom.is_empty() {
                    buf.clear();
                    enc.custom_buckets_histogram_samples(&custom, &mut buf);
                    log(&buf, "log custom buckets histograms")?;
                }
            }
            if !b.float_histograms.is_empty() {
                buf.clear();
                let custom = enc.float_histogram_samples(&b.float_histograms, &mut buf);
                if !buf.is_empty() {
                    log(&buf, "log float histograms")?;
                }
                if !custom.is_empty() {
                    buf.clear();
                    enc.custom_buckets_float_histogram_samples(&custom, &mut buf);
                    log(&buf, "log custom buckets float histograms")?;
                }
            }
            // Exemplars should be logged after samples, otherwise remote write could send the exemplars before
            // the samples.
            if !b.exemplars.is_empty() {
                buf.clear();
                enc.exemplars(&exemplars_for_encoding(&b.exemplars), &mut buf);
                log(&buf, "log exemplars")?;
            }
        }
        Ok(())
    }

    /// Go `commitExemplars`.
    fn commit_exemplars(&self, b: &AppendBatch) {
        if b.exemplars.is_empty() {
            return;
        }
        let es = self.head.exemplars();
        for e in &b.exemplars {
            let Some(s) = self.head.series.get_by_id(e.r#ref) else {
                // The series was truncated between AppendExemplar and Commit.
                continue;
            };
            // We don't instrument exemplar appends here, all is instrumented by storage.
            if let Err(err) = es.add_exemplar(&s.lset, &e.exemplar) {
                if err.is(&ERR_OUT_OF_ORDER_EXEMPLAR) {
                    continue;
                }
                self.head.logger.debug("Unknown error while adding exemplar", &[("err", &err)]);
            }
        }
    }

    /// Go `releasePendingCommit`. Must be called with the series lock held.
    pub(crate) fn release_pending_commit(&self, series: &MemSeries, s: &mut MemSeriesInner) {
        if s.unmark_pending_commit() {
            return;
        }
        self.head.metrics.pending_commit_underflow.inc();
        self.head.logger.error(
            "Tried to release a pending commit that was not held",
            &[("series", &series.lset)],
        );
    }

    /// Go `releaseCreatedSeriesReservations`.
    fn release_created_series_reservations(&self) {
        for s in &self.series {
            let mut g = s.lock();
            self.release_pending_commit(s, &mut g);
        }
    }

    /// Go `commitFloats`.
    fn commit_floats(&self, b: &mut AppendBatch, acc: &mut AppenderCommitContext<'_>, has_wbl: bool) {
        let floats = std::mem::take(&mut b.floats);
        let float_series = std::mem::take(&mut b.float_series);
        for (s, series) in floats.iter().zip(float_series.iter()) {
            let mut g = series.lock();
            if is_stale_nan(s.v) {
                // A float staleness marker for a series whose last sample is a histogram is converted to the
                // matching histogram staleness marker and committed with the histograms of this batch.
                if g.last_histogram_value().is_some() {
                    b.histograms.push(RefHistogramSample {
                        r#ref: series.r#ref,
                        st: s.st,
                        t: s.t,
                        h: Arc::new(Histogram {
                            sum: s.v,
                            ..Default::default()
                        }),
                    });
                    b.histogram_series.push(series.clone());
                    // This sample was counted as a float but is now a histogram.
                    acc.floats_appended -= 1;
                    acc.histograms_appended += 1;
                    continue;
                }
                if g.last_float_histogram_value().is_some() {
                    b.float_histograms.push(RefFloatHistogramSample {
                        r#ref: series.r#ref,
                        st: s.st,
                        t: s.t,
                        fh: Arc::new(FloatHistogram {
                            sum: s.v,
                            ..Default::default()
                        }),
                    });
                    b.float_histogram_series.push(series.clone());
                    acc.floats_appended -= 1;
                    acc.histograms_appended += 1;
                    continue;
                }
            }
            let (ooo_sample, _, err) =
                g.appendable(s.t, s.v, self.head_maxt, self.min_valid_time, self.ooo_time_window);
            if let Some(err) = &err {
                handle_appendable_error(
                    err,
                    &mut acc.floats_appended,
                    &mut acc.float_ooo_rejected,
                    &mut acc.float_oob_rejected,
                    &mut acc.float_too_old_rejected,
                );
            }
            let prev_head_chunk_count = g.head_chunk_count();
            let mut chunk_created = false;
            if err.is_some() {
                // Do nothing here.
            } else if ooo_sample {
                // Sample is OOO and OOO handling is enabled and the delta is within the OOO tolerance.
                let (ok, created, mmap_refs) = g.insert(
                    s.st,
                    s.t,
                    s.v,
                    None,
                    None,
                    &acc.append_chunk_opts,
                    acc.ooo_cap_max,
                    &self.head.logger,
                    &series.lset,
                    series.r#ref,
                );
                chunk_created = created;
                if created {
                    acc.on_ooo_chunk_created(series.r#ref, mmap_refs, has_wbl);
                }
                if ok {
                    acc.wbl_samples.push(*s);
                    acc.ooo_min_t = acc.ooo_min_t.min(s.t);
                    acc.ooo_max_t = acc.ooo_max_t.max(s.t);
                    acc.ooo_floats_accepted += 1;
                } else {
                    // Sample is an exact duplicate of the last sample.
                    acc.floats_appended -= 1;
                }
            } else {
                let (was_stale, was_histogram, old_buckets) = g.sample_state();
                let is_stale = is_stale_nan(s.v);
                let (ok, created) =
                    g.append(s.st, s.t, s.v, self.append_id, &acc.append_chunk_opts, &series.lset, series.r#ref);
                chunk_created = created;
                if ok {
                    acc.in_order_mint = acc.in_order_mint.min(s.t);
                    acc.in_order_maxt = acc.in_order_maxt.max(s.t);
                    self.head.update_stale_series_metric_on_append(was_stale, is_stale);
                    if was_histogram {
                        self.head.update_native_histogram_metrics_on_append(true, false, old_buckets, 0);
                    }
                } else {
                    // The sample is an exact duplicate, and should be silently dropped.
                    acc.floats_appended -= 1;
                }
            }
            if chunk_created {
                self.head.on_chunk_created(series.r#ref, prev_head_chunk_count, g.head_chunk_count());
            }
            g.cleanup_append_ids_below(self.cleanup_append_ids_below);
            self.release_pending_commit(series, &mut g);
        }
    }

    /// Go `commitHistograms`.
    fn commit_histograms(&self, b: &mut AppendBatch, acc: &mut AppenderCommitContext<'_>, has_wbl: bool) {
        let histograms = std::mem::take(&mut b.histograms);
        let histogram_series = std::mem::take(&mut b.histogram_series);
        for (s, series) in histograms.into_iter().zip(histogram_series.iter()) {
            let mut g = series.lock();
            let (ooo_sample, _, err) =
                g.appendable_histogram(s.t, &s.h, self.head_maxt, self.min_valid_time, self.ooo_time_window);
            if let Some(err) = &err {
                handle_appendable_error(
                    err,
                    &mut acc.histograms_appended,
                    &mut acc.histo_ooo_rejected,
                    &mut acc.histo_oob_rejected,
                    &mut acc.histo_too_old_rejected,
                );
            }
            let prev_head_chunk_count = g.head_chunk_count();
            let mut chunk_created = false;
            if err.is_some() {
            } else if ooo_sample {
                let (ok, created, mmap_refs) = g.insert(
                    s.st,
                    s.t,
                    0.0,
                    Some(Box::new((*s.h).clone())),
                    None,
                    &acc.append_chunk_opts,
                    acc.ooo_cap_max,
                    &self.head.logger,
                    &series.lset,
                    series.r#ref,
                );
                chunk_created = created;
                if created {
                    acc.on_ooo_chunk_created(series.r#ref, mmap_refs, has_wbl);
                }
                if ok {
                    acc.ooo_min_t = acc.ooo_min_t.min(s.t);
                    acc.ooo_max_t = acc.ooo_max_t.max(s.t);
                    acc.wbl_histograms.push(s);
                    acc.ooo_histogram_accepted += 1;
                } else {
                    acc.histograms_appended -= 1;
                }
            } else {
                let (was_stale, was_histogram, old_buckets) = g.sample_state();
                let is_stale = is_stale_nan(s.h.sum);
                let new_buckets = s.h.positive_buckets.len() + s.h.negative_buckets.len();
                let (ok, created) = g.append_histogram(
                    s.st,
                    s.t,
                    s.h,
                    self.append_id,
                    &acc.append_chunk_opts,
                    &series.lset,
                    series.r#ref,
                );
                chunk_created = created;
                if ok {
                    acc.in_order_mint = acc.in_order_mint.min(s.t);
                    acc.in_order_maxt = acc.in_order_maxt.max(s.t);
                    self.head.update_stale_series_metric_on_append(was_stale, is_stale);
                    self.head
                        .update_native_histogram_metrics_on_append(was_histogram, true, old_buckets, new_buckets);
                } else {
                    acc.histograms_appended -= 1;
                    acc.histo_ooo_rejected += 1;
                }
            }
            if chunk_created {
                self.head.on_chunk_created(series.r#ref, prev_head_chunk_count, g.head_chunk_count());
            }
            g.cleanup_append_ids_below(self.cleanup_append_ids_below);
            self.release_pending_commit(series, &mut g);
        }
    }

    /// Go `commitFloatHistograms`.
    fn commit_float_histograms(&self, b: &mut AppendBatch, acc: &mut AppenderCommitContext<'_>, has_wbl: bool) {
        let histograms = std::mem::take(&mut b.float_histograms);
        let histogram_series = std::mem::take(&mut b.float_histogram_series);
        for (s, series) in histograms.into_iter().zip(histogram_series.iter()) {
            let mut g = series.lock();
            let (ooo_sample, _, err) =
                g.appendable_float_histogram(s.t, &s.fh, self.head_maxt, self.min_valid_time, self.ooo_time_window);
            if let Some(err) = &err {
                handle_appendable_error(
                    err,
                    &mut acc.histograms_appended,
                    &mut acc.histo_ooo_rejected,
                    &mut acc.histo_oob_rejected,
                    &mut acc.histo_too_old_rejected,
                );
            }
            let prev_head_chunk_count = g.head_chunk_count();
            let mut chunk_created = false;
            if err.is_some() {
            } else if ooo_sample {
                let (ok, created, mmap_refs) = g.insert(
                    s.st,
                    s.t,
                    0.0,
                    None,
                    Some(Box::new((*s.fh).clone())),
                    &acc.append_chunk_opts,
                    acc.ooo_cap_max,
                    &self.head.logger,
                    &series.lset,
                    series.r#ref,
                );
                chunk_created = created;
                if created {
                    acc.on_ooo_chunk_created(series.r#ref, mmap_refs, has_wbl);
                }
                if ok {
                    acc.ooo_min_t = acc.ooo_min_t.min(s.t);
                    acc.ooo_max_t = acc.ooo_max_t.max(s.t);
                    acc.wbl_float_histograms.push(s);
                    acc.ooo_histogram_accepted += 1;
                } else {
                    acc.histograms_appended -= 1;
                }
            } else {
                let (was_stale, was_histogram, old_buckets) = g.sample_state();
                let is_stale = is_stale_nan(s.fh.sum);
                let new_buckets = s.fh.positive_buckets.len() + s.fh.negative_buckets.len();
                let (ok, created) = g.append_float_histogram(
                    s.st,
                    s.t,
                    s.fh,
                    self.append_id,
                    &acc.append_chunk_opts,
                    &series.lset,
                    series.r#ref,
                );
                chunk_created = created;
                if ok {
                    acc.in_order_mint = acc.in_order_mint.min(s.t);
                    acc.in_order_maxt = acc.in_order_maxt.max(s.t);
                    self.head.update_stale_series_metric_on_append(was_stale, is_stale);
                    self.head
                        .update_native_histogram_metrics_on_append(was_histogram, true, old_buckets, new_buckets);
                } else {
                    acc.histograms_appended -= 1;
                    acc.histo_ooo_rejected += 1;
                }
            }
            if chunk_created {
                self.head.on_chunk_created(series.r#ref, prev_head_chunk_count, g.head_chunk_count());
            }
            g.cleanup_append_ids_below(self.cleanup_append_ids_below);
            self.release_pending_commit(series, &mut g);
        }
    }

    /// Go `commitMetadata`.
    fn commit_metadata(b: &AppendBatch) {
        for (m, series) in b.metadata.iter().zip(b.metadata_series.iter()) {
            let mut g = series.lock();
            g.ext_mut().meta = metadata::Handle::make(Metadata {
                typ: record::to_metric_type(m.r#type),
                unit: m.unit.clone(),
                help: m.help.clone(),
            });
        }
    }

    fn finish(&mut self) {
        self.series_refs = Vec::new();
        self.series = Vec::new();
        self.types_in_batch = U64Map::default();
        self.closed = true;
    }

    /// Go `Commit`: writes to the WAL and adds the data to the head.
    pub fn commit(&mut self) -> Result<(), Error> {
        if self.closed {
            return Err(ERR_APPENDER_CLOSED);
        }
        let h = self.head.clone();
        if let Err(e) = self.log() {
            let _ = self.rollback(); // Most likely the same error will happen again.
            return Err(Error::wrap("write to WAL", e));
        }
        if let Some(wn) = h.write_notified() {
            wn.notify();
        }
        let mut acc = AppenderCommitContext {
            floats_appended: 0,
            histograms_appended: 0,
            ooo_floats_accepted: 0,
            ooo_histogram_accepted: 0,
            float_ooo_rejected: 0,
            histo_ooo_rejected: 0,
            float_too_old_rejected: 0,
            histo_too_old_rejected: 0,
            float_oob_rejected: 0,
            histo_oob_rejected: 0,
            in_order_mint: i64::MAX,
            in_order_maxt: i64::MIN,
            append_chunk_opts: ChunkOpts {
                chunk_disk_mapper: Some(&h.chunk_disk_mapper),
                chunk_range: h.chunk_range(),
                samples_per_chunk: h.opts.samples_per_chunk,
                use_xor2: self.use_xor2,
                use_histogram_st: self.use_histogram_st,
                store_st: self.store_st,
            },
            ooo_min_t: i64::MAX,
            ooo_max_t: i64::MIN,
            wbl_samples: Vec::new(),
            wbl_histograms: Vec::new(),
            wbl_float_histograms: Vec::new(),
            ooo_mmap_markers: None,
            ooo_mmap_markers_count: 0,
            ooo_records: Vec::new(),
            ooo_cap_max: h.opts.out_of_order_cap_max.load(Ordering::Relaxed),
            ooo_enc: record::Encoder {
                enable_st_storage: self.store_st,
            },
        };
        let wbl = h.wbl();
        let has_wbl = wbl.is_some();
        for b in &self.batches {
            acc.floats_appended += b.floats.len() as i64;
            acc.histograms_appended += (b.histograms.len() + b.float_histograms.len()) as i64;
            self.commit_exemplars(b);
        }
        let mut batches = std::mem::take(&mut self.batches);
        for b in batches.iter_mut() {
            // Do not change the order of these calls. We depend on it for correct commit order of samples and
            // for the staleness marker handling.
            self.commit_floats(b, &mut acc, has_wbl);
            self.commit_histograms(b, &mut acc, has_wbl);
            self.commit_float_histograms(b, &mut acc, has_wbl);
            Self::commit_metadata(b);
        }
        drop(batches);
        // Release the reservations that protected newly indexed series before their first sample was queued.
        self.release_created_series_reservations();

        let m = &h.metrics;
        m.out_of_order_samples.with_label_values(SAMPLE_METRIC_TYPE_FLOAT).add(acc.float_ooo_rejected as f64);
        m.out_of_order_samples.with_label_values(SAMPLE_METRIC_TYPE_HISTOGRAM).add(acc.histo_ooo_rejected as f64);
        m.out_of_bound_samples.with_label_values(SAMPLE_METRIC_TYPE_FLOAT).add(acc.float_oob_rejected as f64);
        m.too_old_samples.with_label_values(SAMPLE_METRIC_TYPE_FLOAT).add(acc.float_too_old_rejected as f64);
        m.samples_appended.with_label_values(SAMPLE_METRIC_TYPE_FLOAT).add(acc.floats_appended as f64);
        m.samples_appended.with_label_values(SAMPLE_METRIC_TYPE_HISTOGRAM).add(acc.histograms_appended as f64);
        m.out_of_order_samples_appended
            .with_label_values(SAMPLE_METRIC_TYPE_FLOAT)
            .add(acc.ooo_floats_accepted as f64);
        m.out_of_order_samples_appended
            .with_label_values(SAMPLE_METRIC_TYPE_HISTOGRAM)
            .add(acc.ooo_histogram_accepted as f64);
        h.update_min_max_time(acc.in_order_mint, acc.in_order_maxt);
        h.update_min_ooo_max_ooo_time(acc.ooo_min_t, acc.ooo_max_t);

        acc.collect_ooo_records(has_wbl);
        if let Some(wbl) = wbl
            && !acc.ooo_records.is_empty()
            && let Err(e) = wbl.log_batch(&acc.ooo_records)
        {
            // WBL logging of ooo samples is best effort here since the samples are already in memory.
            h.logger.error("Failed to log out of order samples into the WAL", &[("err", &e)]);
        }
        h.iso.close_append(self.append_id);
        h.metrics.active_appenders.dec();
        self.finish();
        Ok(())
    }

    /// Go `Rollback`: removes the samples and exemplars and writes any new series to the WAL.
    pub fn rollback(&mut self) -> Result<(), Error> {
        if self.closed {
            return Err(ERR_APPENDER_CLOSED);
        }
        let h = self.head.clone();
        let batches = std::mem::take(&mut self.batches);
        for b in &batches {
            for series in b.float_series.iter().chain(&b.histogram_series).chain(&b.float_histogram_series) {
                let mut g = series.lock();
                g.cleanup_append_ids_below(self.cleanup_append_ids_below);
                self.release_pending_commit(series, &mut g);
            }
        }
        drop(batches);
        // Series are created in the head memory regardless of rollback. Thus we have to log them to the WAL in
        // any case.
        let res = self.log();
        self.release_created_series_reservations();
        h.iso.close_append(self.append_id);
        h.metrics.active_appenders.dec();
        self.finish();
        res
    }
}

/// Go `exemplarsForEncoding`.
fn exemplars_for_encoding(es: &[ExemplarWithSeriesRef]) -> Vec<RefExemplar> {
    es.iter()
        .map(|e| RefExemplar {
            r#ref: e.r#ref,
            t: e.exemplar.ts,
            v: e.exemplar.value,
            labels: e.exemplar.labels.clone(),
        })
        .collect()
}

fn opt_err(r: Result<(), Error>) -> Option<Error> {
    r.err()
}

/// Go `headAppender`: the V1 appender of the head.
pub struct HeadAppender {
    pub base: HeadAppenderBase,
    hints: Option<AppendOptions>,
}

impl HeadAppender {
    pub(crate) fn new(head: Arc<Head>) -> HeadAppender {
        HeadAppender {
            base: HeadAppenderBase::new(head),
            hints: None,
        }
    }

    /// Go `appendID` (tests).
    pub fn append_id(&self) -> u64 {
        self.base.append_id
    }

    /// Go `minValidTime` (tests).
    pub fn min_valid_time(&self) -> i64 {
        self.base.min_valid_time
    }
}

impl AppenderTransaction for HeadAppender {
    fn commit(&mut self) -> Option<Error> {
        opt_err(self.base.commit())
    }
    fn rollback(&mut self) -> Option<Error> {
        opt_err(self.base.rollback())
    }
}

impl GetRef for HeadAppender {
    fn get_ref(&mut self, lset: &Labels, hash: u64) -> (SeriesRef, Labels) {
        self.base.get_ref(lset, hash)
    }
}

fn zero_histogram(h: &Histogram) -> Histogram {
    Histogram {
        // The STZeroSample represents a counter reset by definition.
        counter_reset_hint: CounterResetHint::CounterReset,
        // Replicate other fields to avoid needless chunk creation.
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        custom_values: h.custom_values.clone(),
        ..Default::default()
    }
}

fn zero_float_histogram(fh: &FloatHistogram) -> FloatHistogram {
    FloatHistogram {
        counter_reset_hint: CounterResetHint::CounterReset,
        schema: fh.schema,
        zero_threshold: fh.zero_threshold,
        custom_values: fh.custom_values.clone(),
        ..Default::default()
    }
}

pub(crate) fn histogram_sample_type(h: &Histogram) -> SampleType {
    if h.uses_custom_buckets() {
        SampleType::CustomBucketHistogram
    } else {
        SampleType::Histogram
    }
}

pub(crate) fn float_histogram_sample_type(fh: &FloatHistogram) -> SampleType {
    if fh.uses_custom_buckets() {
        SampleType::CustomBucketFloatHistogram
    } else {
        SampleType::FloatHistogram
    }
}

impl Appender for HeadAppender {
    fn append(&mut self, r: SeriesRef, lset: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        let a = &mut self.base;
        // Fail fast if OOO is disabled and the sample is out of bounds.
        if a.ooo_time_window == 0 && t < a.min_valid_time {
            a.head.metrics.out_of_bound_samples.with_label_values(SAMPLE_METRIC_TYPE_FLOAT).inc();
            return Err(ERR_OUT_OF_BOUNDS);
        }
        let mut s = a.head.series.get_by_id(r);
        a.head.run_test_after_series_lookup(s.as_ref());
        if s.is_none() {
            s = Some(a.get_or_create(lset)?.0);
        }
        let s = s.expect("series");
        if is_stale_nan(v) {
            // If we have added a sample before with this same appender, we can check the previously used type
            // and turn a stale float sample into a stale histogram sample or stale float histogram sample.
            match a.types_in_batch.get(&s.r#ref).copied().unwrap_or_default() {
                SampleType::Histogram | SampleType::CustomBucketHistogram => {
                    let h = Histogram {
                        sum: v,
                        ..Default::default()
                    };
                    return self.append_histogram(r, lset, t, Some(&h), None);
                }
                SampleType::FloatHistogram | SampleType::CustomBucketFloatHistogram => {
                    let fh = FloatHistogram {
                        sum: v,
                        ..Default::default()
                    };
                    return self.append_histogram(r, lset, t, None, Some(&fh));
                }
                _ => {}
            }
        }
        let (head_maxt, min_valid_time, ooo_time_window) = (a.head_maxt, a.min_valid_time, a.ooo_time_window);
        let discard_ooo = self.hints.is_some_and(|h| h.discard_out_of_order);
        let (s, (discarded, delta, err)) = a.lock_for_append(s, |_, g| {
            let (is_ooo, delta, err) = g.appendable(t, v, head_maxt, min_valid_time, ooo_time_window);
            if err.is_none() {
                if is_ooo && discard_ooo {
                    return (true, delta, None);
                }
                g.mark_pending_commit();
            }
            (false, delta, err)
        })?;
        if discarded {
            a.head.metrics.out_of_order_samples.with_label_values(SAMPLE_METRIC_TYPE_FLOAT).inc();
            return Err(ERR_OUT_OF_ORDER_SAMPLE);
        }
        if delta > 0 {
            a.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
        }
        if let Some(err) = err {
            if err.is(&ERR_OUT_OF_ORDER_SAMPLE) {
                a.head.metrics.out_of_order_samples.with_label_values(SAMPLE_METRIC_TYPE_FLOAT).inc();
            } else if err.is(&ERR_TOO_OLD_SAMPLE) {
                a.head.metrics.too_old_samples.with_label_values(SAMPLE_METRIC_TYPE_FLOAT).inc();
            }
            return Err(err);
        }
        let sref = s.r#ref;
        let b = a.get_current_batch(SampleType::Float, sref);
        b.floats.push(RefSample {
            r#ref: sref,
            st: 0,
            t,
            v,
        });
        b.float_series.push(s);
        Ok(sref)
    }

    fn set_options(&mut self, opts: Option<&AppendOptions>) {
        self.hints = opts.copied();
    }

    /// Go `AppendExemplar`: assumes the series ref already exists.
    fn append_exemplar(&mut self, r: SeriesRef, lset: &Labels, e: &Exemplar) -> Result<SeriesRef, Error> {
        let a = &mut self.base;
        // Check if exemplar storage is enabled.
        if !a.head.opts.enable_exemplar_storage || a.head.opts.max_exemplars.load(Ordering::Relaxed) <= 0 {
            return Ok(0);
        }
        let mut r = r;
        let mut s = a.head.series.get_by_id(r);
        if s.is_none() {
            s = a.head.series.get_by_hash(lset.hash(), lset);
            if let Some(s) = &s {
                r = s.r#ref;
            }
        }
        let Some(s) = s else {
            return Err(Error::msg(format!(
                "unknown HeadSeriesRef when trying to add exemplar: {}",
                r
            )));
        };
        // Ensure no empty labels have gotten through.
        let mut e = e.clone();
        e.labels = e.labels.without_empty();
        if let Err(err) = a.head.exemplars().validate_exemplar(&s.lset, &e) {
            if err.is(&ERR_DUPLICATE_EXEMPLAR) || err.is(&ERR_EXEMPLARS_DISABLED) {
                // Duplicate, don't return an error but don't accept the exemplar.
                return Ok(0);
            }
            return Err(err);
        }
        let b = a.get_current_batch(SampleType::None, r);
        // Protect the series from ordinary compaction before the exemplar is committed.
        s.update_exemplar_timestamp(e.ts);
        b.exemplars.push(ExemplarWithSeriesRef { r#ref: r, exemplar: e });
        Ok(s.r#ref)
    }

    fn append_histogram(
        &mut self,
        r: SeriesRef,
        lset: &Labels,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        let a = &mut self.base;
        // Fail fast if OOO is disabled and the sample is out of bounds.
        if a.ooo_time_window == 0 && t < a.min_valid_time {
            a.head.metrics.out_of_bound_samples.with_label_values(SAMPLE_METRIC_TYPE_HISTOGRAM).inc();
            return Err(ERR_OUT_OF_BOUNDS);
        }
        if let Some(h) = h {
            h.validate()?;
        }
        if let Some(fh) = fh {
            fh.validate()?;
        }
        let mut s = a.head.series.get_by_id(r);
        if s.is_none() {
            s = Some(a.get_or_create(lset)?.0);
        }
        let mut s = s.expect("series");
        let (head_maxt, min_valid_time, ooo_time_window) = (a.head_maxt, a.min_valid_time, a.ooo_time_window);
        if let Some(h) = h {
            let (ls, (_, delta, err)) = a.lock_for_append(s, |_, g| {
                let res = g.appendable_histogram(t, h, head_maxt, min_valid_time, ooo_time_window);
                if res.2.is_none() {
                    g.mark_pending_commit();
                }
                res
            })?;
            s = ls;
            if delta > 0 {
                a.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
            }
            if let Some(err) = err {
                count_histogram_append_error(&a.head, &err);
                return Err(err);
            }
            let b = a.get_current_batch(histogram_sample_type(h), s.r#ref);
            b.histograms.push(RefHistogramSample {
                r#ref: s.r#ref,
                st: 0,
                t,
                h: Arc::new(h.clone()),
            });
            b.histogram_series.push(s.clone());
        } else if let Some(fh) = fh {
            let (ls, (_, delta, err)) = a.lock_for_append(s, |_, g| {
                let res = g.appendable_float_histogram(t, fh, head_maxt, min_valid_time, ooo_time_window);
                if res.2.is_none() {
                    g.mark_pending_commit();
                }
                res
            })?;
            s = ls;
            if delta > 0 {
                a.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
            }
            if let Some(err) = err {
                count_histogram_append_error(&a.head, &err);
                return Err(err);
            }
            let b = a.get_current_batch(float_histogram_sample_type(fh), s.r#ref);
            b.float_histograms.push(RefFloatHistogramSample {
                r#ref: s.r#ref,
                st: 0,
                t,
                fh: Arc::new(fh.clone()),
            });
            b.float_histogram_series.push(s.clone());
        }
        Ok(s.r#ref)
    }

    fn append_histogram_st_zero_sample(
        &mut self,
        r: SeriesRef,
        lset: &Labels,
        t: i64,
        st: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        if st >= t {
            return Err(ERR_ST_NEWER_THAN_SAMPLE);
        }
        let a = &mut self.base;
        let mut s = a.head.series.get_by_id(r);
        if s.is_none() {
            s = Some(a.get_or_create(lset)?.0);
        }
        let mut s = s.expect("series");
        let (head_maxt, min_valid_time, ooo_time_window) = (a.head_maxt, a.min_valid_time, a.ooo_time_window);
        if let Some(h) = h {
            let zero = zero_histogram(h);
            // For STZeroSamples OOO is not allowed.
            let (ls, res) = a.lock_for_append(s, |_, g| {
                let (is_ooo, _, err) = g.appendable_histogram(st, &zero, head_maxt, min_valid_time, ooo_time_window);
                if let Some(err) = err {
                    if err.is(&ERR_OUT_OF_ORDER_SAMPLE) {
                        return Err(ERR_OUT_OF_ORDER_ST);
                    }
                    return Err(err);
                }
                // OOO is not allowed because after the first scrape, ST will be the same for most (if not all)
                // future samples.
                if is_ooo {
                    return Err(ERR_OUT_OF_ORDER_ST);
                }
                g.mark_pending_commit();
                Ok(())
            })?;
            res?;
            s = ls;
            let b = a.get_current_batch(histogram_sample_type(h), s.r#ref);
            b.histograms.push(RefHistogramSample {
                r#ref: s.r#ref,
                st: 0,
                t: st,
                h: Arc::new(zero),
            });
            b.histogram_series.push(s.clone());
        } else if let Some(fh) = fh {
            let zero = zero_float_histogram(fh);
            let (ls, res) = a.lock_for_append(s, |_, g| {
                let (is_ooo, _, err) =
                    g.appendable_float_histogram(st, &zero, head_maxt, min_valid_time, ooo_time_window);
                if let Some(err) = err {
                    if err.is(&ERR_OUT_OF_ORDER_SAMPLE) {
                        return Err(ERR_OUT_OF_ORDER_ST);
                    }
                    return Err(err);
                }
                if is_ooo {
                    return Err(ERR_OUT_OF_ORDER_ST);
                }
                g.mark_pending_commit();
                Ok(())
            })?;
            res?;
            s = ls;
            let b = a.get_current_batch(float_histogram_sample_type(fh), s.r#ref);
            b.float_histograms.push(RefFloatHistogramSample {
                r#ref: s.r#ref,
                st: 0,
                t: st,
                fh: Arc::new(zero),
            });
            b.float_histogram_series.push(s.clone());
        }
        Ok(s.r#ref)
    }

    /// Go `UpdateMetadata`: assumes the series ref already exists.
    fn update_metadata(&mut self, r: SeriesRef, lset: &Labels, meta: &Metadata) -> Result<SeriesRef, Error> {
        let a = &mut self.base;
        let mut r = r;
        let mut s = a.head.series.get_by_id(r);
        if s.is_none() {
            s = a.head.series.get_by_hash(lset.hash(), lset);
            if let Some(s) = &s {
                r = s.r#ref;
            }
        }
        let Some(s) = s else {
            return Err(Error::msg(format!(
                "unknown series when trying to add metadata with HeadSeriesRef: {} and labels: {}",
                r, lset
            )));
        };
        let has_new_metadata = {
            let g = s.lock();
            match g.meta() {
                Some(m) if !metadata::is_zero_handle(m) => m.value() != meta,
                _ => true,
            }
        };
        if has_new_metadata {
            let b = a.get_current_batch(SampleType::None, s.r#ref);
            b.metadata.push(RefMetadata {
                r#ref: s.r#ref,
                r#type: record::get_metric_type(meta.typ),
                unit: meta.unit.clone(),
                help: meta.help.clone(),
            });
            b.metadata_series.push(s);
        }
        Ok(r)
    }

    /// Go `AppendSTZeroSample`: appends a synthetic zero sample at `st`.
    fn append_st_zero_sample(&mut self, r: SeriesRef, lset: &Labels, t: i64, st: i64) -> Result<SeriesRef, Error> {
        if st >= t {
            return Err(ERR_ST_NEWER_THAN_SAMPLE);
        }
        let a = &mut self.base;
        let mut s = a.head.series.get_by_id(r);
        if s.is_none() {
            s = Some(a.get_or_create(lset)?.0);
        }
        let s = s.expect("series");
        let (head_maxt, min_valid_time, ooo_time_window) = (a.head_maxt, a.min_valid_time, a.ooo_time_window);
        // Check if ST wouldn't be OOO vs samples we already might have for this series.
        let (s, res) = a.lock_for_append(s, |_, g| {
            let (is_ooo, _, err) = g.appendable(st, 0.0, head_maxt, min_valid_time, ooo_time_window);
            if let Some(err) = err {
                return Err((0, err));
            }
            if is_ooo {
                return Err((1, ERR_OUT_OF_ORDER_ST));
            }
            g.mark_pending_commit();
            Ok(())
        })?;
        if let Err((kind, err)) = res {
            if kind == 1 {
                // Go returns the series reference with ErrOutOfOrderST.
                return Err(err);
            }
            return Err(err);
        }
        let b = a.get_current_batch(SampleType::Float, s.r#ref);
        b.floats.push(RefSample {
            r#ref: s.r#ref,
            st: 0,
            t: st,
            v: 0.0,
        });
        let sref = s.r#ref;
        b.float_series.push(s);
        Ok(sref)
    }

    fn as_get_ref(&mut self) -> Option<&mut dyn GetRef> {
        Some(self)
    }
}

pub(crate) fn count_histogram_append_error(head: &Head, err: &Error) {
    if err.is(&ERR_OUT_OF_ORDER_SAMPLE) {
        head.metrics.out_of_order_samples.with_label_values(SAMPLE_METRIC_TYPE_HISTOGRAM).inc();
    } else if err.is(&ERR_TOO_OLD_SAMPLE) {
        head.metrics.too_old_samples.with_label_values(SAMPLE_METRIC_TYPE_HISTOGRAM).inc();
    }
}

/// Go `initAppender`: initializes the time bounds of the head upon the first sample it receives.
pub struct InitAppender {
    app: Option<HeadAppender>,
    head: Arc<Head>,
    hints: Option<AppendOptions>,
}

impl InitAppender {
    fn app(&mut self, t: Option<i64>) -> &mut HeadAppender {
        if self.app.is_none() {
            if let Some(t) = t {
                self.head.init_time(t);
            }
            let mut a = HeadAppender::new(self.head.clone());
            a.hints = self.hints;
            self.app = Some(a);
        }
        self.app.as_mut().expect("appender")
    }
}

impl AppenderTransaction for InitAppender {
    fn commit(&mut self) -> Option<Error> {
        match self.app.as_mut() {
            None => {
                self.head.metrics.active_appenders.dec();
                None
            }
            Some(a) => a.commit(),
        }
    }
    fn rollback(&mut self) -> Option<Error> {
        match self.app.as_mut() {
            None => {
                self.head.metrics.active_appenders.dec();
                None
            }
            Some(a) => a.rollback(),
        }
    }
}

impl GetRef for InitAppender {
    fn get_ref(&mut self, lset: &Labels, hash: u64) -> (SeriesRef, Labels) {
        match self.app.as_mut() {
            Some(a) => a.get_ref(lset, hash),
            None => (0, Labels::empty()),
        }
    }
}

impl Appender for InitAppender {
    fn append(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        self.app(Some(t)).append(r, l, t, v)
    }
    fn set_options(&mut self, opts: Option<&AppendOptions>) {
        self.hints = opts.copied();
        if let Some(a) = self.app.as_mut() {
            a.set_options(opts);
        }
    }
    fn append_exemplar(&mut self, r: SeriesRef, l: &Labels, e: &Exemplar) -> Result<SeriesRef, Error> {
        // Check if exemplar storage is enabled.
        if !self.head.opts.enable_exemplar_storage || self.head.opts.max_exemplars.load(Ordering::Relaxed) <= 0 {
            return Ok(0);
        }
        // We should never reach here given we would call Append before AppendExemplar and we probably want to
        // always base head/WAL min time on sample times.
        self.app(Some(e.ts)).append_exemplar(r, l, e)
    }
    fn append_histogram(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.app(Some(t)).append_histogram(r, l, t, h, fh)
    }
    fn append_histogram_st_zero_sample(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        st: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.app(Some(t)).append_histogram_st_zero_sample(r, l, t, st, h, fh)
    }
    fn update_metadata(&mut self, r: SeriesRef, l: &Labels, m: &Metadata) -> Result<SeriesRef, Error> {
        self.app(None).update_metadata(r, l, m)
    }
    fn append_st_zero_sample(&mut self, r: SeriesRef, l: &Labels, t: i64, st: i64) -> Result<SeriesRef, Error> {
        self.app(Some(t)).append_st_zero_sample(r, l, t, st)
    }
    fn as_get_ref(&mut self) -> Option<&mut dyn GetRef> {
        Some(self)
    }
}

impl Head {
    /// Go `initTime`: initializes a head with the first timestamp (only for a completely fresh head with an
    /// empty WAL).
    pub(crate) fn init_time(&self, t: i64) {
        // maxTime must be set before minTime, because initialized() keys off minTime.
        if self
            .max_time
            .compare_exchange(i64::MIN, t, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            // Another thread already won the init race. Wait until it also sets minTime.
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
            while self.min_time.load(Ordering::SeqCst) == i64::MAX {
                if std::time::Instant::now() >= deadline {
                    self.logger.warn("initTime timeout waiting for minTime initialization", &[]);
                    return;
                }
                std::thread::yield_now();
            }
            return;
        }
        let _ = self
            .min_time
            .compare_exchange(i64::MAX, t, Ordering::SeqCst, Ordering::SeqCst);
    }

    /// Go `Appender`: a new appender on the head.
    pub fn appender(&self, _ctx: &Context) -> Box<dyn Appender> {
        self.metrics.active_appenders.inc();
        self.metrics.appenders_created.inc();
        // The head cache might not have a starting point yet. The init appender picks up the first appended
        // timestamp as the base.
        if !self.initialized() {
            return Box::new(InitAppender {
                app: None,
                head: self.arc(),
                hints: None,
            });
        }
        Box::new(self.head_appender())
    }

    /// Go `appender`: the head appender itself (the head must be initialized).
    pub fn head_appender(&self) -> HeadAppender {
        HeadAppender::new(self.arc())
    }

    /// Go `appendableMinValidTime`: the minimum valid timestamp for appends.
    pub fn appendable_min_valid_time(&self) -> i64 {
        // This boundary ensures that no samples will be added to the compaction window.
        let cw_end = self.max_time().wrapping_sub(self.chunk_range() / 2);
        // This boundary ensures that we avoid overlapping timeframes from one block to the next.
        let min_valid = self.min_valid_time.load(Ordering::SeqCst);
        cw_end.max(min_valid)
    }

    /// Go `AppendableMinValidTime`: `None` if the head is not initialized.
    pub fn appendable_min_valid_time_checked(&self) -> Option<i64> {
        if !self.initialized() {
            return None;
        }
        Some(self.appendable_min_valid_time())
    }
}

impl storage::Appendable for Head {
    fn appender(&self, ctx: &Context) -> Box<dyn Appender> {
        Head::appender(self, ctx)
    }
}
