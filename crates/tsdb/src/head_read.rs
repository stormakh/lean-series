//! Reading the head (port of `tsdb/head_read.go`): the head index reader, the head chunk reader with
//! isolation, the stop iterator and the selected-series index reader.

use std::collections::HashSet;
use std::sync::Arc;

use chunkenc::{AnyIterator, Chunk, ChunkIterator, Encoding, ValueType};
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{Labels, ScratchBuilder};
use model::matcher::Matcher;
use storage::{Context, Error, LabelHints, Meta, SeriesRef, ERR_NOT_FOUND};
use tsdb_format::chunks::{new_head_chunk_ref, ChunkDiskMapper};
use tsdb_format::index::{self, BoxPostings, ListPostings, Postings, StringIter};

use crate::head::{
    chunks_err, wrap_chunk_id, Head, HeadChunkId, HeadSeriesRef, MemChunk, MemSeries, MemSeriesInner, SeriesRefs,
    unpack_head_chunk_ref,
};
use crate::isolation::IsolationState;
use crate::reader::{ChunkOrIterable, ChunkReader, IndexReader};

/// Go `checkContextEveryNIterations`.
pub(crate) const CHECK_CONTEXT_EVERY_N_ITERATIONS: usize = 100;

/// Converts an index error to a storage error, keeping context errors recognizable.
pub(crate) fn index_err(e: index::Error) -> Error {
    if e.is_context() {
        let s = e.to_string();
        if s.contains("deadline exceeded") {
            return storage::ERR_DEADLINE_EXCEEDED;
        }
        return storage::ERR_CANCELED;
    }
    Error::other(e)
}

impl Head {
    /// Go `ExemplarQuerier`.
    pub fn exemplar_querier(&self, ctx: &Context) -> Result<Box<dyn storage::ExemplarQuerier>, Error> {
        self.exemplars().exemplar_querier(ctx)
    }

    /// Go `Index`: an index reader against the head.
    pub fn index(&self) -> HeadIndexReader {
        self.index_range(i64::MIN, i64::MAX)
    }

    /// Go `indexRange`.
    pub fn index_range(&self, mut mint: i64, maxt: i64) -> HeadIndexReader {
        let hmin = self.min_time();
        if hmin > mint {
            mint = hmin;
        }
        HeadIndexReader {
            head: self.arc(),
            mint,
            maxt,
        }
    }

    /// Go `Chunks`: a chunk reader against the head (with an isolation state over all time).
    pub fn chunks(&self) -> Result<HeadChunkReader, Error> {
        self.chunks_range(i64::MIN, i64::MAX, Some(self.iso.state(i64::MIN, i64::MAX)))
    }

    /// Go `chunksRange`.
    pub fn chunks_range(
        &self,
        mut mint: i64,
        maxt: i64,
        is: Option<Arc<IsolationState>>,
    ) -> Result<HeadChunkReader, Error> {
        if self.is_closed() {
            if let Some(is) = is {
                is.close();
            }
            return Err(Error::msg("can't read from a closed head"));
        }
        let hmin = self.min_time();
        if hmin > mint {
            mint = hmin;
        }
        Ok(HeadChunkReader {
            head: self.arc(),
            mint,
            maxt,
            iso_state: is,
        })
    }

    /// Go `selectedSeriesIndex`.
    pub fn selected_series_index(&self, mint: i64, maxt: i64, refs: SeriesRefs) -> HeadSelectedSeriesIndexReader {
        HeadSelectedSeriesIndexReader {
            inner: self.index_range(mint, maxt),
            selected_series_refs: refs,
        }
    }

    /// Go `filterSeriesAndSortPostings`: the series of `p` that exist in the head (and satisfy `keep`, called
    /// under the series lock), in postings order and in label order.
    pub fn filter_series_and_sort_postings(
        &self,
        p: &mut dyn Postings,
        keep: Option<&dyn Fn(&MemSeries, &MemSeriesInner) -> bool>,
    ) -> Result<SeriesRefs, Error> {
        let mut sorted_by_ref = Vec::new();
        let mut kept: Vec<Arc<MemSeries>> = Vec::new();
        let mut not_found = 0usize;
        while p.next() {
            let r = p.at();
            let Some(s) = self.series.get_by_id(r) else {
                not_found += 1;
                continue;
            };
            if let Some(keep) = keep {
                let ok = {
                    let g = s.lock();
                    keep(&s, &g)
                };
                if !ok {
                    continue;
                }
            }
            sorted_by_ref.push(r);
            kept.push(s);
        }
        if not_found > 0 {
            self.logger.debug("Looked up series not found", &[("count", &not_found)]);
        }
        if let Some(e) = p.err() {
            return Err(Error::wrap("expand postings", index_err(e)));
        }
        kept.sort_by(|a, b| model::labels::compare(&a.lset, &b.lset).cmp(&0));
        let sorted_by_labels = kept.iter().map(|s| s.r#ref).collect();
        Ok(SeriesRefs {
            sorted_by_ref: Arc::new(sorted_by_ref),
            sorted_by_labels: Arc::new(sorted_by_labels),
        })
    }

    /// Go `staleSeriesRefsNoOOOData`: the stale series that have no out-of-order data.
    pub fn stale_series_refs_no_ooo_data(&self, ctx: &Context) -> Result<SeriesRefs, Error> {
        let (k, v) = index::all_postings_key();
        let mut p = self.postings.postings(ctx, k, &[v]);
        self.filter_series_and_sort_postings(
            &mut p,
            Some(&|_, s| crate::head::is_series_without_ooo(s) && crate::head::is_stale_series(s)),
        )
    }

    /// Go `chunkFromSeries`. Call with the series locked (`g`).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn chunk_from_series(
        &self,
        s: &Arc<MemSeries>,
        g: &MemSeriesInner,
        cid: HeadChunkId,
        is_ooo: bool,
        mint: i64,
        maxt: i64,
        iso_state: Option<&Arc<IsolationState>>,
    ) -> Result<(Arc<dyn Chunk>, i64), Error> {
        if is_ooo {
            return g.ooo_chunk(cid, &self.chunk_disk_mapper);
        }
        let (c, head_chunk, is_open) = g.chunk(cid, &self.chunk_disk_mapper)?;
        // This means that the chunk is outside the specified range.
        if !c.overlaps_closed_interval(mint, maxt) {
            return Err(ERR_NOT_FOUND);
        }
        let max_time = c.max_time;
        let chk = if head_chunk && is_open {
            // The open chunk is appended to while it is read: take a copy of its bytes (Go does so when asked
            // to; here it is always done, because the open chunk is never shared).
            let enc = c.chunk.encoding();
            let b = c.chunk.bytes().to_vec();
            drop(c);
            let copied = match &self.opts.chunk_pool {
                Some(p) => p.get(enc, b),
                None => chunkenc::from_data(enc, b),
            }
            .map_err(Error::Chunk)?;
            Arc::from(copied)
        } else {
            c.chunk
        };
        Ok((
            Arc::new(SafeHeadChunk {
                chunk: chk,
                s: s.clone(),
                cid,
                iso_state: iso_state.cloned(),
            }),
            max_time,
        ))
    }
}

/// Go `appendSeriesChunks`: appends the chunk metas of the series in [mint, maxt] to `chks`.
pub fn append_series_chunks(s: &MemSeriesInner, sref: HeadSeriesRef, mint: i64, maxt: i64, chks: &mut Vec<Meta>) {
    for (i, c) in s.mmapped_chunks.iter().enumerate() {
        // Do not expose chunks that are outside of the specified range.
        if !c.overlaps_closed_interval(mint, maxt) {
            continue;
        }
        chks.push(Meta {
            chunk_ref: new_head_chunk_ref(sref, s.head_chunk_id(i)),
            chunk: None,
            min_time: c.min_time,
            max_time: c.max_time,
        });
    }
    let n = s.head_chunks.len();
    let base = s.mmapped_chunks.len();
    for (i, chk) in s.head_chunks.iter().enumerate() {
        if !chk.overlaps_closed_interval(mint, maxt) {
            continue;
        }
        // The newest chunk is open.
        let max_time = if i == n - 1 { i64::MAX } else { chk.max_time };
        chks.push(Meta {
            chunk_ref: new_head_chunk_ref(sref, s.head_chunk_id(base + i)),
            chunk: None,
            min_time: chk.min_time,
            max_time,
        });
    }
}

impl MemSeriesInner {
    /// Go `memSeries.chunk`: the chunk for the chunk ID, from memory or from the m-mapped files. Returns the
    /// chunk, whether it is a head chunk (else m-mapped), and whether it is the open chunk.
    pub fn chunk(&self, id: HeadChunkId, cdm: &ChunkDiskMapper) -> Result<(MemChunk, bool, bool), Error> {
        // ix is the chunk's oldest-first position in the logical sequence.
        let mut ix = wrap_chunk_id(id.wrapping_sub(self.first_chunk_id)) as usize;
        if ix < self.mmapped_chunks.len() {
            let m = &self.mmapped_chunks[ix];
            let chk = match cdm.chunk(m.r#ref) {
                Ok(c) => c,
                Err(e) => {
                    if let tsdb_format::chunks::Error::Corruption(_) = e {
                        panic!("{}", e);
                    }
                    return Err(chunks_err(e));
                }
            };
            return Ok((
                MemChunk {
                    chunk: chk,
                    min_time: m.min_time,
                    max_time: m.max_time,
                },
                false,
                false,
            ));
        }
        ix -= self.mmapped_chunks.len();
        if ix >= self.head_chunks.len() {
            return Err(ERR_NOT_FOUND);
        }
        Ok((self.head_chunks[ix].clone(), true, ix == self.head_chunks.len() - 1))
    }

    /// Go `oooChunk`: the m-mapped OOO chunk for the chunk ID and its max time. Never the head OOO chunk.
    pub fn ooo_chunk(&self, id: HeadChunkId, cdm: &ChunkDiskMapper) -> Result<(Arc<dyn Chunk>, i64), Error> {
        let Some(ooo) = self.ooo() else {
            return Err(ERR_NOT_FOUND);
        };
        let ix = wrap_chunk_id(id.wrapping_sub(ooo.first_ooo_chunk_id)) as usize;
        if ix >= ooo.ooo_mmapped_chunks.len() {
            return Err(ERR_NOT_FOUND);
        }
        let m = &ooo.ooo_mmapped_chunks[ix];
        let chk = cdm.chunk(m.r#ref).map_err(chunks_err)?;
        Ok((chk, m.max_time))
    }

    /// The isolation part of Go `memSeries.iterator`: how many of the `num_samples` samples of chunk `id` the
    /// isolation state allows to read.
    pub fn iterator_stop_after(&self, id: HeadChunkId, num_samples: usize, iso_state: Option<&IsolationState>) -> usize {
        let mut stop_after = num_samples;
        let Some(iso) = iso_state else {
            return stop_after;
        };
        if iso.isolation_disabled() {
            return stop_after;
        }
        let mut ix = wrap_chunk_id(id.wrapping_sub(self.first_chunk_id)) as i64;
        let mut total_samples = 0i64; // Total samples in this series.
        let mut previous_samples = 0i64; // Samples before this chunk.
        for (j, d) in self.mmapped_chunks.iter().enumerate() {
            total_samples += d.num_samples as i64;
            if (j as i64) < ix {
                previous_samples += d.num_samples as i64;
            }
        }
        if !self.head_chunks.is_empty() {
            // Reset ix, so its base is 0 (oldest non-mmapped head chunk).
            ix -= self.mmapped_chunks.len() as i64;
            for (j, elem) in self.head_chunks.iter().enumerate() {
                let chk_samples = elem.chunk.num_samples() as i64;
                total_samples += chk_samples;
                if (j as i64) < ix {
                    previous_samples += chk_samples;
                }
            }
        }
        let Some(txs) = &self.txs else {
            return stop_after;
        };
        // Removing the extra transactionIDs that are relevant for samples that come after this chunk, from the
        // total transactionIDs.
        let append_ids_to_consider =
            txs.tx_id_count as i64 - (total_samples - (previous_samples + num_samples as i64));
        // Iterate over the appendIDs, find the first one that the isolation state says not to return.
        let mut it = txs.iterator();
        let mut index = 0i64;
        while index < append_ids_to_consider {
            let append_id = it.at();
            if append_id <= iso.max_append_id && !iso.is_incomplete(append_id) {
                it.next();
                index += 1;
                continue;
            }
            // Stopped in a previous chunk.
            stop_after = (num_samples as i64 - (append_ids_to_consider - index)).max(0) as usize;
            break;
        }
        stop_after
    }
}

/// Go `safeHeadChunk`: a head chunk whose iterator applies the isolation of the read under the series lock.
pub struct SafeHeadChunk {
    pub chunk: Arc<dyn Chunk>,
    s: Arc<MemSeries>,
    cid: HeadChunkId,
    iso_state: Option<Arc<IsolationState>>,
}

impl std::fmt::Debug for SafeHeadChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SafeHeadChunk").field("chunk", &self.chunk).field("cid", &self.cid).finish()
    }
}

impl Chunk for SafeHeadChunk {
    fn bytes(&self) -> &[u8] {
        self.chunk.bytes()
    }
    fn encoding(&self) -> Encoding {
        self.chunk.encoding()
    }
    fn appender(&mut self) -> Result<chunkenc::Appender, chunkenc::Error> {
        match Arc::get_mut(&mut self.chunk) {
            Some(c) => c.appender(),
            None => Err(chunkenc::Error::msg("head chunk is shared")),
        }
    }
    fn iterator<'a>(&'a self, it: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        let num_samples = self.chunk.num_samples();
        let stop_after = {
            let g = self.s.lock();
            g.iterator_stop_after(self.cid, num_samples, self.iso_state.as_deref())
        };
        if stop_after == 0 {
            return AnyIterator::Nop(chunkenc::new_nop_iterator());
        }
        if stop_after == num_samples {
            return self.chunk.iterator(it);
        }
        AnyIterator::Other(Box::new(StopIterator::new(self.chunk.iterator(None), stop_after)))
    }
    fn num_samples(&self) -> usize {
        self.chunk.num_samples()
    }
    fn compact(&mut self) {}
    fn reset(&mut self, stream: Vec<u8>) {
        if let Some(c) = Arc::get_mut(&mut self.chunk) {
            c.reset(stream);
        }
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Go `stopIterator`: returns only the first `stop_after` values of the wrapped iterator.
pub struct StopIterator<'a> {
    inner: AnyIterator<'a>,
    i: i64,
    stop_after: i64,
    /// The type of the current value, or None before the first and after the last one.
    value_type: ValueType,
}

impl<'a> StopIterator<'a> {
    /// Go `makeStopIterator`.
    pub fn new(inner: AnyIterator<'a>, stop_after: usize) -> StopIterator<'a> {
        StopIterator {
            inner,
            i: -1,
            stop_after: stop_after as i64,
            value_type: ValueType::None,
        }
    }
}

impl ChunkIterator for StopIterator<'_> {
    fn next(&mut self) -> ValueType {
        if self.i + 1 >= self.stop_after {
            self.value_type = ValueType::None;
            return ValueType::None;
        }
        self.i += 1;
        self.value_type = self.inner.next();
        self.value_type
    }
    /// Advances with next, since the wrapped iterator's seek would not stop after `stop_after` values.
    fn seek(&mut self, t: i64) -> ValueType {
        while self.value_type == ValueType::None || self.inner.at_t() < t {
            if self.next() == ValueType::None {
                return ValueType::None;
            }
        }
        self.value_type
    }
    fn at(&self) -> (i64, f64) {
        self.inner.at()
    }
    fn at_histogram(&self, h: Option<Histogram>) -> (i64, Histogram) {
        self.inner.at_histogram(h)
    }
    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        self.inner.at_float_histogram(fh)
    }
    fn at_t(&self) -> i64 {
        self.inner.at_t()
    }
    fn at_st(&self) -> i64 {
        self.inner.at_st()
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        self.inner.err()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Index reader.

/// Go `headIndexReader`: index reading for the head block.
#[derive(Clone)]
pub struct HeadIndexReader {
    pub(crate) head: Arc<Head>,
    pub mint: i64,
    pub maxt: i64,
}

impl HeadIndexReader {
    pub fn new(head: Arc<Head>, mint: i64, maxt: i64) -> HeadIndexReader {
        HeadIndexReader { head, mint, maxt }
    }

    pub fn head(&self) -> &Arc<Head> {
        &self.head
    }

    /// The series lookup shared with the out-of-order index reader.
    pub(crate) fn series_with(
        &self,
        r: SeriesRef,
        builder: &mut ScratchBuilder,
        chks: &mut Vec<Meta>,
        f: impl FnOnce(&Arc<MemSeries>, &mut MemSeriesInner, &mut Vec<Meta>),
    ) -> Result<(), Error> {
        let Some(s) = self.head.series.get_by_id(r) else {
            self.head.metrics.series_not_found.inc();
            return Err(ERR_NOT_FOUND);
        };
        builder.assign(s.lset.clone());
        let mut g = s.lock();
        chks.clear();
        f(&s, &mut g, chks);
        Ok(())
    }
}

/// Go `headIndexReader.SortedPostings`.
pub(crate) fn head_sorted_postings<'a>(head: &Head, mut p: BoxPostings<'a>) -> BoxPostings<'a> {
    let mut series: Vec<Arc<MemSeries>> = Vec::with_capacity(128);
    let mut not_found = 0usize;
    // Fetch all the series only once.
    while p.next() {
        match head.series.get_by_id(p.at()) {
            None => not_found += 1,
            Some(s) => series.push(s),
        }
    }
    if not_found > 0 {
        head.logger.debug("Looked up series not found", &[("count", &not_found)]);
    }
    if let Some(e) = p.err() {
        return index::err_postings(index::Error::wrap("expand postings", e));
    }
    series.sort_by(|a, b| model::labels::compare(&a.lset, &b.lset).cmp(&0));
    // Convert back to list.
    let ep: Vec<SeriesRef> = series.iter().map(|s| s.r#ref).collect();
    Box::new(index::new_list_postings(ep))
}

/// Go `headIndexReader.ShardedPostings`.
pub(crate) fn head_sharded_postings<'a>(
    head: &Head,
    mut p: BoxPostings<'a>,
    shard_index: u64,
    shard_count: u64,
) -> BoxPostings<'a> {
    if !head.opts.enable_sharding {
        return index::err_postings(index::Error::new("sharding is disabled"));
    }
    let mut out = Vec::with_capacity(128);
    let mut not_found = 0usize;
    while p.next() {
        let Some(s) = head.series.get_by_id(p.at()) else {
            not_found += 1;
            continue;
        };
        // Check if the series belong to the shard.
        if s.shard_hash % shard_count != shard_index {
            continue;
        }
        out.push(s.r#ref);
    }
    if not_found > 0 {
        head.logger.debug("Looked up series not found", &[("count", &not_found)]);
    }
    if let Some(e) = p.err() {
        return index::err_postings(index::Error::wrap("expand postings", e));
    }
    Box::new(index::new_list_postings(out))
}

/// Go `headIndexReader.LabelNamesFor`.
pub(crate) fn head_label_names_for(head: &Head, ctx: &Context, series: &mut dyn Postings) -> Result<Vec<String>, Error> {
    let mut names: HashSet<String> = HashSet::new();
    let mut i = 0usize;
    while series.next() {
        i += 1;
        if i % CHECK_CONTEXT_EVERY_N_ITERATIONS == 0
            && let Some(e) = ctx.err()
        {
            return Err(e);
        }
        let Some(ms) = head.series.get_by_id(series.at()) else {
            // Series not found, this happens during compaction, when series was garbage collected after the
            // caller got the series IDs.
            continue;
        };
        for (n, _) in ms.lset.iter() {
            if !names.contains(n) {
                names.insert(n.to_string());
            }
        }
    }
    if let Some(e) = series.err() {
        return Err(index_err(e));
    }
    let mut names: Vec<String> = names.into_iter().collect();
    names.sort();
    Ok(names)
}

impl IndexReader for HeadIndexReader {
    fn symbols(&self) -> Box<dyn StringIter + '_> {
        Box::new(self.head.postings.symbols())
    }

    fn sorted_label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> Result<Vec<String>, Error> {
        let mut values = self.label_values(ctx, name, hints, matchers)?;
        values.sort();
        Ok(values)
    }

    fn label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> Result<Vec<String>, Error> {
        if self.maxt < self.head.min_time() || self.mint > self.head.max_time() {
            return Ok(Vec::new());
        }
        if matchers.is_empty() {
            return Ok(self.head.postings.label_values(ctx, name, hints));
        }
        crate::querier::label_values_with_matchers(ctx, self, name, hints, matchers)
    }

    fn postings(&self, ctx: &Context, name: &str, values: &[&str]) -> Result<BoxPostings<'_>, Error> {
        Ok(self.head.postings.postings(ctx, name, values))
    }

    fn postings_for_label_matching(
        &self,
        ctx: &Context,
        name: &str,
        m: &(dyn Fn(&str) -> bool + Sync),
    ) -> BoxPostings<'_> {
        self.head.postings.postings_for_label_matching(ctx, name, &|v| m(v))
    }

    fn postings_for_all_label_values(&self, ctx: &Context, name: &str) -> BoxPostings<'_> {
        self.head.postings.postings_for_all_label_values(ctx, name)
    }

    fn sorted_postings<'a>(&'a self, p: BoxPostings<'a>) -> BoxPostings<'a> {
        head_sorted_postings(&self.head, p)
    }

    fn sharded_postings<'a>(&'a self, p: BoxPostings<'a>, shard_index: u64, shard_count: u64) -> BoxPostings<'a> {
        head_sharded_postings(&self.head, p, shard_index, shard_count)
    }

    fn series(&self, r: SeriesRef, builder: &mut ScratchBuilder, chks: &mut Vec<Meta>) -> Result<(), Error> {
        let (mint, maxt) = (self.mint, self.maxt);
        self.series_with(r, builder, chks, |s, g, chks| append_series_chunks(g, s.r#ref, mint, maxt, chks))
    }

    fn label_names(&self, ctx: &Context, matchers: &[Matcher]) -> Result<Vec<String>, Error> {
        if self.maxt < self.head.min_time() || self.mint > self.head.max_time() {
            return Ok(Vec::new());
        }
        if matchers.is_empty() {
            let mut names = self.head.postings.label_names();
            names.sort();
            return Ok(names);
        }
        crate::querier::label_names_with_matchers(ctx, self, matchers)
    }

    fn label_names_for(&self, ctx: &Context, postings: &mut dyn Postings) -> Result<Vec<String>, Error> {
        head_label_names_for(&self.head, ctx, postings)
    }

    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// Go `headSelectedSeriesIndexReader`: the head index restricted to a list of series references, for
/// compaction.
pub struct HeadSelectedSeriesIndexReader {
    pub inner: HeadIndexReader,
    pub selected_series_refs: SeriesRefs,
}

impl HeadSelectedSeriesIndexReader {
    fn selected(&self) -> BoxPostings<'static> {
        Box::new(ListPostings::from_shared(self.selected_series_refs.sorted_by_ref.clone()))
    }
}

impl IndexReader for HeadSelectedSeriesIndexReader {
    fn symbols(&self) -> Box<dyn StringIter + '_> {
        self.inner.symbols()
    }
    fn sorted_label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> Result<Vec<String>, Error> {
        self.inner.sorted_label_values(ctx, name, hints, matchers)
    }
    fn label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> Result<Vec<String>, Error> {
        self.inner.label_values(ctx, name, hints, matchers)
    }
    fn postings(&self, ctx: &Context, name: &str, values: &[&str]) -> Result<BoxPostings<'_>, Error> {
        if self.selected_series_refs.sorted_by_ref.is_empty() {
            return Ok(index::empty_postings());
        }
        // Only expected to be called during compaction with the AllPostingsKey; the result goes to
        // sorted_postings.
        let (k, v) = index::all_postings_key();
        if name == k && values.len() == 1 && values[0] == v {
            return Ok(self.selected());
        }
        Ok(index::intersect(vec![self.selected(), self.inner.head.postings.postings(ctx, name, values)]))
    }
    fn postings_for_label_matching(
        &self,
        ctx: &Context,
        name: &str,
        m: &(dyn Fn(&str) -> bool + Sync),
    ) -> BoxPostings<'_> {
        index::intersect(vec![self.selected(), self.inner.postings_for_label_matching(ctx, name, m)])
    }
    fn postings_for_all_label_values(&self, ctx: &Context, name: &str) -> BoxPostings<'_> {
        index::intersect(vec![self.selected(), self.inner.postings_for_all_label_values(ctx, name)])
    }
    /// Go swaps the marker postings of `postings(AllPostingsKey)` for the pre-sorted list. Here the postings
    /// are expanded and compared with the selected list instead (Rust cannot type-switch on boxed postings).
    fn sorted_postings<'a>(&'a self, mut p: BoxPostings<'a>) -> BoxPostings<'a> {
        let list = match index::expand_postings(&mut p) {
            Ok(l) => l,
            Err(e) => return index::err_postings(index::Error::wrap("expand postings", e)),
        };
        if list.as_slice() == self.selected_series_refs.sorted_by_ref.as_slice() {
            return Box::new(ListPostings::from_shared(self.selected_series_refs.sorted_by_labels.clone()));
        }
        head_sorted_postings(&self.inner.head, Box::new(index::new_list_postings(list)))
    }
    fn sharded_postings<'a>(&'a self, p: BoxPostings<'a>, shard_index: u64, shard_count: u64) -> BoxPostings<'a> {
        self.inner.sharded_postings(p, shard_index, shard_count)
    }
    fn series(&self, r: SeriesRef, builder: &mut ScratchBuilder, chks: &mut Vec<Meta>) -> Result<(), Error> {
        self.inner.series(r, builder, chks)
    }
    fn label_names(&self, ctx: &Context, matchers: &[Matcher]) -> Result<Vec<String>, Error> {
        self.inner.label_names(ctx, matchers)
    }
    fn label_names_for(&self, ctx: &Context, postings: &mut dyn Postings) -> Result<Vec<String>, Error> {
        self.inner.label_names_for(ctx, postings)
    }
    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Chunk reader.

/// Go `headChunkReader`: chunk reading for the head block.
pub struct HeadChunkReader {
    pub(crate) head: Arc<Head>,
    pub mint: i64,
    pub maxt: i64,
    pub(crate) iso_state: Option<Arc<IsolationState>>,
}

impl HeadChunkReader {
    /// Go `chunk`: the chunk for the meta and its max time.
    pub fn chunk(&self, meta: &Meta) -> Result<(Arc<dyn Chunk>, i64), Error> {
        let (sid, cid, is_ooo) = unpack_head_chunk_ref(meta.chunk_ref);
        // This means that the series has been garbage collected.
        let Some(s) = self.head.series.get_by_id(sid) else {
            return Err(ERR_NOT_FOUND);
        };
        let g = s.lock();
        self.head
            .chunk_from_series(&s, &g, cid, is_ooo, self.mint, self.maxt, self.iso_state.as_ref())
    }

    /// The isolation state of the reader.
    pub fn iso_state(&self) -> Option<&Arc<IsolationState>> {
        self.iso_state.as_ref()
    }
}

impl ChunkReader for HeadChunkReader {
    fn chunk_or_iterable(&self, meta: &Meta) -> Result<ChunkOrIterable, Error> {
        Ok(ChunkOrIterable::Chunk(self.chunk(meta)?.0))
    }
    fn chunk_or_iterable_with_copy(&self, meta: &Meta) -> Option<Result<(ChunkOrIterable, i64), Error>> {
        Some(self.chunk(meta).map(|(c, maxt)| (ChunkOrIterable::Chunk(c), maxt)))
    }
    fn close(&self) -> Result<(), Error> {
        if let Some(is) = &self.iso_state {
            is.close();
        }
        Ok(())
    }
}

impl Drop for HeadChunkReader {
    fn drop(&mut self) {
        if let Some(is) = &self.iso_state {
            is.close();
        }
    }
}

/// Labels of a series (helper for tests).
#[doc(hidden)]
pub fn test_series_labels(head: &Head, r: SeriesRef) -> Option<Labels> {
    head.series.get_by_id(r).map(|s| s.lset.clone())
}
