//! Reading in-order and out-of-order head data together, and the out-of-order compaction head (port of
//! `tsdb/ooo_head_read.go`).

use std::cmp::Ordering as CmpOrdering;
use std::sync::Arc;

use chunkenc::{AnyIterator, Chunk, Encoding};
use model::labels::ScratchBuilder;
use model::matcher::Matcher;
use storage::{
    Annotations, BoxChunkSeriesSet, BoxQuerier, BoxChunkQuerier, BoxSearchResultSet, BoxSeriesSet, ChunkQuerier,
    Context, Error, LabelHints, LabelQuerier, LabelResult, Meta, Querier, SearchHints, Searcher, SelectHints,
    SeriesRef, ERR_NOT_FOUND,
};
use tsdb_format::chunks::{new_head_chunk_ref, ChunkDiskMapperRef};
use tsdb_format::index::{self, BoxPostings, ListPostings, Postings, StringIter};
use tsdb_format::tombstones::{self, MemTombstones};

use crate::head::{
    handle_chunk_write_error, ooo_compaction_head_ulid, range_head_ulid, unpack_head_chunk_ref, Head,
    HeadSeriesRef, MemSeriesInner, OOO_CHUNK_ID_MASK,
};
use crate::head_append::ChunkOpts;
use crate::head_read::{
    append_series_chunks, head_label_names_for, head_sharded_postings, head_sorted_postings, HeadChunkReader,
    HeadIndexReader,
};
use crate::meta::{BlockMeta, BlockStats};
use crate::ooo_isolation::OooIsolationState;
use crate::reader::{BlockReader, ChunkOrIterable, ChunkReader, IndexReader, Iterable};

/// Go `HeadAndOOOIndexReader`: reads the head index, returning both the in-order and the out-of-order chunks
/// of each series.
pub struct HeadAndOOOIndexReader {
    pub inner: HeadIndexReader,
    pub ino_mint: i64,
    pub last_garbage_collected_mmap_ref: ChunkDiskMapperRef,
}

impl HeadAndOOOIndexReader {
    /// Go `NewHeadAndOOOIndexReader`.
    pub fn new(
        head: Arc<Head>,
        ino_mint: i64,
        mint: i64,
        maxt: i64,
        last_garbage_collected_mmap_ref: ChunkDiskMapperRef,
    ) -> HeadAndOOOIndexReader {
        HeadAndOOOIndexReader {
            inner: HeadIndexReader::new(head, mint, maxt),
            ino_mint,
            last_garbage_collected_mmap_ref,
        }
    }
}

/// Go `mergedOOOChunks`: the iterables of overlapping chunks.
pub struct MergedOooChunks {
    pub chunk_iterables: Vec<Arc<dyn Chunk>>,
}

impl Iterable for MergedOooChunks {
    fn iterator<'a>(&'a self, reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        let its = self.chunk_iterables.iter().map(|c| c.iterator(None)).collect();
        storage::chain_sample_iterator_from_iterators(reuse, its)
    }
}

/// Go `multiMeta`: a fake chunk that carries a set of metas inside `Meta::chunk`.
#[derive(Debug, Default)]
pub struct MultiMeta {
    pub metas: Vec<Meta>,
}

impl Chunk for MultiMeta {
    fn bytes(&self) -> &[u8] {
        &[]
    }
    fn encoding(&self) -> Encoding {
        chunkenc::ENC_NONE
    }
    fn appender(&mut self) -> Result<chunkenc::Appender, chunkenc::Error> {
        Err(chunkenc::Error::msg("multiMeta has no appender"))
    }
    fn iterator<'a>(&'a self, _it: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        AnyIterator::Nop(chunkenc::new_nop_iterator())
    }
    fn num_samples(&self) -> usize {
        0
    }
    fn compact(&mut self) {}
    fn reset(&mut self, _stream: Vec<u8>) {}
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// The metas of a `MultiMeta` chunk, if `c` is one.
pub fn as_multi_meta(c: &Arc<dyn Chunk>) -> Option<&MultiMeta> {
    c.as_any().downcast_ref::<MultiMeta>()
}

/// Go `lessByMinTimeAndMinRef`.
pub fn less_by_min_time_and_min_ref(a: &Meta, b: &Meta) -> CmpOrdering {
    a.min_time.cmp(&b.min_time).then(a.chunk_ref.cmp(&b.chunk_ref))
}

/// Go `getOOOSeriesChunks`: chunk metas of the out-of-order (and optionally in-order) data of the series, with
/// overlapping chunks merged into `MultiMeta` metas. `last_garbage_collected_mmap_ref` excludes chunks at or
/// before it (0 disables the check); a non-zero `max_mmap_ref` limits the m-mapped chunks and excludes the OOO
/// head chunk.
#[allow(clippy::too_many_arguments)]
pub fn get_ooo_series_chunks(
    s: &mut MemSeriesInner,
    sref: HeadSeriesRef,
    use_xor2: bool,
    use_histogram_st: bool,
    mint: i64,
    maxt: i64,
    last_garbage_collected_mmap_ref: ChunkDiskMapperRef,
    #[allow(dead_code)] // Go keeps it but does not read it either.
    max_mmap_ref: ChunkDiskMapperRef,
    include_in_order: bool,
    ino_mint: i64,
    chks: &mut Vec<Meta>,
) {
    let n_mmapped = s.ooo().map_or(0, |o| o.ooo_mmapped_chunks.len());
    let mut tmp_chks: Vec<Meta> = Vec::with_capacity(n_mmapped);
    let head_chunk_ref = new_head_chunk_ref(sref, s.ooo_head_chunk_id(n_mmapped));
    let mut ids = Vec::new();
    for i in (0..n_mmapped).rev() {
        ids.push(new_head_chunk_ref(sref, s.ooo_head_chunk_id(i)));
    }
    let Some(ooo) = s.ooo_mut() else {
        return;
    };
    // Collect all chunks that overlap the query range.
    if let Some(c) = ooo.ooo_head_chunk.as_mut()
        && c.overlaps_closed_interval(mint, maxt)
        && max_mmap_ref.0 == 0
    {
        if !c.chunk.samples.is_empty() {
            // Empty samples happens in tests, at least.
            let encoded = match c.chunk.to_encoded_chunks(c.min_time, c.max_time, use_xor2, use_histogram_st) {
                Ok(e) => e,
                Err(e) => {
                    handle_chunk_write_error(Err(tsdb_format::chunks::Error::Chunk(e)));
                    return;
                }
            };
            for chk in encoded {
                tmp_chks.push(Meta {
                    chunk_ref: head_chunk_ref,
                    chunk: Some(chk.chunk),
                    min_time: chk.min_time,
                    max_time: chk.max_time,
                });
            }
        } else {
            tmp_chks.push(Meta {
                chunk_ref: head_chunk_ref,
                chunk: None,
                min_time: c.min_time,
                max_time: c.max_time,
            });
        }
    }
    for (k, c) in ooo.ooo_mmapped_chunks.iter().rev().enumerate() {
        if c.overlaps_closed_interval(mint, maxt)
            && (max_mmap_ref.0 == 0 || max_mmap_ref.greater_than_or_equal_to(c.r#ref))
            && (last_garbage_collected_mmap_ref.0 == 0 || c.r#ref.greater_than(last_garbage_collected_mmap_ref))
        {
            tmp_chks.push(Meta {
                chunk_ref: ids[k],
                chunk: None,
                min_time: c.min_time,
                max_time: c.max_time,
            });
        }
    }
    if include_in_order {
        append_series_chunks(s, sref, ino_mint, maxt, &mut tmp_chks);
    }
    // There is nothing to do if we did not collect any chunk.
    if tmp_chks.is_empty() {
        return;
    }
    // Next we want to sort all the collected chunks by min time so we can find those that overlap.
    tmp_chks.sort_by(less_by_min_time_and_min_ref);
    // Next we want to iterate the sorted collected chunks and return composites for chunks that overlap with
    // others.
    let mut it = tmp_chks.into_iter();
    let mut to_be_merged = it.next().expect("non-empty");
    let mut merged: Option<Vec<Meta>> = None;
    for c in it {
        if c.min_time > to_be_merged.max_time {
            // This chunk doesn't overlap. Send current toBeMerged to output and start a new one.
            if let Some(metas) = merged.take() {
                to_be_merged.chunk = Some(Arc::new(MultiMeta { metas }));
            }
            chks.push(to_be_merged);
            to_be_merged = c;
        } else {
            // Merge this chunk with existing toBeMerged.
            let max = to_be_merged.max_time.max(c.max_time);
            match merged.as_mut() {
                Some(m) => m.push(c),
                None => merged = Some(vec![to_be_merged.clone(), c]),
            }
            to_be_merged.max_time = max;
        }
    }
    if let Some(metas) = merged.take() {
        to_be_merged.chunk = Some(Arc::new(MultiMeta { metas }));
    }
    chks.push(to_be_merged);
}

impl IndexReader for HeadAndOOOIndexReader {
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
        // Go's embedded headIndexReader.SortedLabelValues calls its own LabelValues.
        self.inner.sorted_label_values(ctx, name, hints, matchers)
    }
    /// Labels within either the in-order range or the OOO range.
    fn label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> Result<Vec<String>, Error> {
        let h = &self.inner.head;
        let (mint, maxt) = (self.inner.mint, self.inner.maxt);
        if maxt < h.min_time() && maxt < h.min_ooo_time() || mint > h.max_time() && mint > h.max_ooo_time() {
            return Ok(Vec::new());
        }
        if matchers.is_empty() {
            return Ok(h.postings.label_values(ctx, name, hints));
        }
        crate::querier::label_values_with_matchers(ctx, self, name, hints, matchers)
    }
    fn postings(&self, ctx: &Context, name: &str, values: &[&str]) -> Result<BoxPostings<'_>, Error> {
        self.inner.postings(ctx, name, values)
    }
    fn postings_for_label_matching(
        &self,
        ctx: &Context,
        name: &str,
        m: &(dyn Fn(&str) -> bool + Sync),
    ) -> BoxPostings<'_> {
        self.inner.postings_for_label_matching(ctx, name, m)
    }
    fn postings_for_all_label_values(&self, ctx: &Context, name: &str) -> BoxPostings<'_> {
        self.inner.postings_for_all_label_values(ctx, name)
    }
    fn sorted_postings<'a>(&'a self, p: BoxPostings<'a>) -> BoxPostings<'a> {
        self.inner.sorted_postings(p)
    }
    fn sharded_postings<'a>(&'a self, p: BoxPostings<'a>, shard_index: u64, shard_count: u64) -> BoxPostings<'a> {
        self.inner.sharded_postings(p, shard_index, shard_count)
    }
    fn series(&self, r: SeriesRef, builder: &mut ScratchBuilder, chks: &mut Vec<Meta>) -> Result<(), Error> {
        let h = self.inner.head.clone();
        let (mint, maxt, ino_mint, gc_ref) = (self.inner.mint, self.inner.maxt, self.ino_mint, self.last_garbage_collected_mmap_ref);
        let use_xor2 = h.opts.use_xor2_float_encoding();
        let use_hst = h.opts.enable_histogram_st_encoding.load(std::sync::atomic::Ordering::Relaxed);
        self.inner.series_with(r, builder, chks, |s, g, chks| {
            if g.ooo().is_some() {
                get_ooo_series_chunks(
                    g,
                    s.r#ref,
                    use_xor2,
                    use_hst,
                    mint,
                    maxt,
                    gc_ref,
                    ChunkDiskMapperRef(0),
                    true,
                    ino_mint,
                    chks,
                );
            } else {
                append_series_chunks(g, s.r#ref, ino_mint, maxt, chks);
            }
        })
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

/// Go `HeadAndOOOChunkReader`: reads both the in-order and the out-of-order chunks of the head.
pub struct HeadAndOOOChunkReader {
    head: Arc<Head>,
    pub mint: i64,
    pub maxt: i64,
    /// If None, only read OOO chunks.
    cr: Option<HeadChunkReader>,
    #[allow(dead_code)] // Go keeps it but does not read it either.
    max_mmap_ref: ChunkDiskMapperRef,
    ooo_iso_state: Option<Arc<OooIsolationState>>,
}

impl HeadAndOOOChunkReader {
    /// Go `NewHeadAndOOOChunkReader`.
    pub fn new(
        head: Arc<Head>,
        mint: i64,
        maxt: i64,
        cr: Option<HeadChunkReader>,
        ooo_iso_state: Option<Arc<OooIsolationState>>,
        max_mmap_ref: ChunkDiskMapperRef,
    ) -> HeadAndOOOChunkReader {
        HeadAndOOOChunkReader {
            head,
            mint,
            maxt,
            cr,
            max_mmap_ref,
            ooo_iso_state,
        }
    }

    /// Go `chunkOrIterable`: (chunk or iterable, max time).
    pub fn chunk_or_iterable_inner(&self, meta: &Meta) -> Result<(ChunkOrIterable, i64), Error> {
        let (sid, cid, is_ooo) = unpack_head_chunk_ref(meta.chunk_ref);
        let Some(s) = self.head.series.get_by_id(sid) else {
            // This means that the series has been garbage collected.
            return Err(ERR_NOT_FOUND);
        };
        let iso_state = self.cr.as_ref().and_then(|c| c.iso_state());
        let g = s.lock();
        let Some(chunk) = &meta.chunk else {
            let (c, maxt) =
                self.head.chunk_from_series(&s, &g, cid, is_ooo, meta.min_time, meta.max_time, iso_state)?;
            return Ok((ChunkOrIterable::Chunk(c), maxt));
        };
        let Some(mm) = as_multi_meta(chunk) else {
            // Complete chunk was supplied.
            return Ok((ChunkOrIterable::Chunk(chunk.clone()), meta.max_time));
        };
        // We have a composite meta: construct a composite iterable.
        let mut mc = MergedOooChunks {
            chunk_iterables: Vec::with_capacity(mm.metas.len()),
        };
        for m in &mm.metas {
            match &m.chunk {
                Some(c) => mc.chunk_iterables.push(c.clone()),
                None => {
                    let (_, cid, is_ooo) = unpack_head_chunk_ref(m.chunk_ref);
                    let (iterable, _) = self
                        .head
                        .chunk_from_series(&s, &g, cid, is_ooo, m.min_time, m.max_time, iso_state)
                        .map_err(|e| Error::wrap("invalid head chunk", e))?;
                    mc.chunk_iterables.push(iterable);
                }
            }
        }
        Ok((ChunkOrIterable::Iterable(Box::new(mc)), meta.max_time))
    }
}

impl ChunkReader for HeadAndOOOChunkReader {
    fn chunk_or_iterable(&self, meta: &Meta) -> Result<ChunkOrIterable, Error> {
        self.chunk_or_iterable_inner(meta).map(|x| x.0)
    }
    fn chunk_or_iterable_with_copy(&self, meta: &Meta) -> Option<Result<(ChunkOrIterable, i64), Error>> {
        Some(self.chunk_or_iterable_inner(meta))
    }
    fn close(&self) -> Result<(), Error> {
        let res = match &self.cr {
            Some(cr) => cr.close(),
            None => Ok(()),
        };
        if let Some(s) = &self.ooo_iso_state {
            s.close();
        }
        res
    }
}

impl Drop for HeadAndOOOChunkReader {
    fn drop(&mut self) {
        if let Some(s) = &self.ooo_iso_state {
            s.close();
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// OOO compaction head.

/// Go `OOOCompactionHead`.
#[derive(Clone)]
pub struct OooCompactionHead {
    head: Arc<Head>,
    last_mmap_ref: ChunkDiskMapperRef,
    last_wbl_file: i64,
    postings: Arc<Vec<SeriesRef>>,
    chunk_range: i64,
    /// Among all the compactable chunks.
    mint: i64,
    maxt: i64,
}

impl OooCompactionHead {
    /// Go `NewOOOCompactionHead`: m-maps all the in-memory OOO chunks, computes the time range of the OOO data,
    /// stores the postings of the series with OOO data and cuts a new WBL segment.
    pub fn new(ctx: &Context, head: Arc<Head>) -> Result<OooCompactionHead, Error> {
        let mut ch = OooCompactionHead {
            chunk_range: head.chunk_range(),
            head: head.clone(),
            last_mmap_ref: ChunkDiskMapperRef(0),
            last_wbl_file: 0,
            postings: Arc::new(Vec::new()),
            mint: i64::MAX,
            maxt: i64::MIN,
        };
        if let Some(wbl) = head.wbl() {
            ch.last_wbl_file = wbl.next_segment_sync().map_err(crate::head::wlog_err)?;
        }
        let hr = HeadIndexReader::new(head.clone(), ch.mint, ch.maxt);
        let (n, v) = index::all_postings_key();
        let p = hr.postings(ctx, n, &[v])?;
        let mut p = hr.sorted_postings(p);
        let (mut last_seq, mut last_off) = (0usize, 0usize);
        let mut postings = Vec::new();
        while p.next() {
            let series_ref = p.at();
            let Some(ms) = head.series.get_by_id(series_ref) else {
                continue;
            };
            ch.mmap_ooo_series_chunk(&ms, series_ref, &mut last_seq, &mut last_off, &mut postings);
        }
        ch.postings = Arc::new(postings);
        Ok(ch)
    }

    /// Go `mmapOOOSeriesChunk`: m-maps the OOO head chunk of one series and tracks the ranges.
    fn mmap_ooo_series_chunk(
        &mut self,
        ms: &crate::head::MemSeries,
        series_ref: SeriesRef,
        last_seq: &mut usize,
        last_off: &mut usize,
        postings: &mut Vec<SeriesRef>,
    ) {
        let head = &self.head;
        let mut g = ms.lock();
        if g.ooo().is_none() {
            return;
        }
        let opts = ChunkOpts {
            chunk_disk_mapper: Some(&head.chunk_disk_mapper),
            use_xor2: head.opts.use_xor2_float_encoding(),
            use_histogram_st: head.opts.enable_histogram_st_encoding.load(std::sync::atomic::Ordering::Relaxed),
            ..Default::default()
        };
        let mut mmap_refs = g.mmap_current_ooo_head_chunk(&opts, &head.logger, &ms.lset, ms.r#ref);
        let ooo = g.ooo().expect("ooo fields");
        if mmap_refs.is_empty()
            && let Some(l) = ooo.ooo_mmapped_chunks.last()
        {
            // Nothing was m-mapped. So take the mmapRef from the existing slice if it exists.
            mmap_refs = vec![l.r#ref];
        }
        let last_mmap_ref = mmap_refs.last().copied().unwrap_or(ChunkDiskMapperRef(0));
        let (seq, off) = last_mmap_ref.unpack();
        if seq > *last_seq || (seq == *last_seq && off > *last_off) {
            self.last_mmap_ref = last_mmap_ref;
            *last_seq = seq;
            *last_off = off;
        }
        if !ooo.ooo_mmapped_chunks.is_empty() {
            postings.push(series_ref);
            for c in &ooo.ooo_mmapped_chunks {
                if c.min_time < self.mint {
                    self.mint = c.min_time;
                }
                if c.max_time > self.maxt {
                    self.maxt = c.max_time;
                }
            }
        }
    }

    /// Go `CloneForTimeRange`: only the BlockReader methods are valid on the clone.
    pub fn clone_for_time_range(&self, mint: i64, maxt: i64) -> OooCompactionHead {
        OooCompactionHead {
            head: self.head.clone(),
            last_mmap_ref: self.last_mmap_ref,
            last_wbl_file: 0,
            postings: self.postings.clone(),
            chunk_range: self.chunk_range,
            mint,
            maxt,
        }
    }

    pub fn min_time(&self) -> i64 {
        self.mint
    }
    pub fn max_time(&self) -> i64 {
        self.maxt
    }
    pub fn chunk_range(&self) -> i64 {
        self.chunk_range
    }
    pub fn last_mmap_ref(&self) -> ChunkDiskMapperRef {
        self.last_mmap_ref
    }
    pub fn last_wbl_file(&self) -> i64 {
        self.last_wbl_file
    }
    pub fn postings_list(&self) -> &[SeriesRef] {
        &self.postings
    }
}

impl BlockReader for OooCompactionHead {
    fn index(&self) -> Result<Box<dyn IndexReader>, Error> {
        Ok(Box::new(OooCompactionHeadIndexReader { ch: self.clone() }))
    }
    fn chunks(&self) -> Result<Box<dyn ChunkReader>, Error> {
        Ok(Box::new(HeadAndOOOChunkReader::new(
            self.head.clone(),
            self.mint,
            self.maxt,
            None,
            None,
            self.last_mmap_ref,
        )))
    }
    fn tombstones(&self) -> Result<Arc<dyn tombstones::Reader>, Error> {
        Ok(Arc::new(MemTombstones::new()))
    }
    fn meta(&self) -> BlockMeta {
        BlockMeta {
            min_time: self.mint,
            max_time: self.maxt,
            ulid: ooo_compaction_head_ulid(),
            stats: BlockStats {
                num_series: self.postings.len() as u64,
                ..Default::default()
            },
            ..Default::default()
        }
    }
    fn size(&self) -> i64 {
        0
    }
}

/// Go `OOOCompactionHeadIndexReader`.
pub struct OooCompactionHeadIndexReader {
    ch: OooCompactionHead,
}

impl OooCompactionHeadIndexReader {
    /// Go `NewOOOCompactionHeadIndexReader`.
    pub fn new(ch: OooCompactionHead) -> OooCompactionHeadIndexReader {
        OooCompactionHeadIndexReader { ch }
    }

    fn hr(&self) -> HeadIndexReader {
        HeadIndexReader::new(self.ch.head.clone(), self.ch.mint, self.ch.maxt)
    }
}

impl IndexReader for OooCompactionHeadIndexReader {
    fn symbols(&self) -> Box<dyn StringIter + '_> {
        Box::new(self.ch.head.postings.symbols())
    }
    fn sorted_label_values(&self, _: &Context, _: &str, _: Option<&LabelHints>, _: &[Matcher]) -> Result<Vec<String>, Error> {
        Err(Error::msg("not implemented"))
    }
    fn label_values(&self, _: &Context, _: &str, _: Option<&LabelHints>, _: &[Matcher]) -> Result<Vec<String>, Error> {
        Err(Error::msg("not implemented"))
    }
    fn postings(&self, _ctx: &Context, name: &str, values: &[&str]) -> Result<BoxPostings<'_>, Error> {
        let (n, v) = index::all_postings_key();
        if name != n || values.len() != 1 || values[0] != v {
            return Err(Error::msg("only AllPostingsKey is supported"));
        }
        Ok(Box::new(ListPostings::from_shared(self.ch.postings.clone())))
    }
    fn postings_for_label_matching(&self, _: &Context, _: &str, _: &(dyn Fn(&str) -> bool + Sync)) -> BoxPostings<'_> {
        index::err_postings(index::Error::new("not supported"))
    }
    fn postings_for_all_label_values(&self, _: &Context, _: &str) -> BoxPostings<'_> {
        index::err_postings(index::Error::new("not supported"))
    }
    fn sorted_postings<'a>(&'a self, p: BoxPostings<'a>) -> BoxPostings<'a> {
        // This will already be sorted from the postings() call above.
        p
    }
    fn sharded_postings<'a>(&'a self, p: BoxPostings<'a>, shard_index: u64, shard_count: u64) -> BoxPostings<'a> {
        head_sharded_postings(&self.ch.head, p, shard_index, shard_count)
    }
    fn series(&self, r: SeriesRef, builder: &mut ScratchBuilder, chks: &mut Vec<Meta>) -> Result<(), Error> {
        let h = &self.ch.head;
        let Some(s) = h.series.get_by_id(r) else {
            h.metrics.series_not_found.inc();
            return Err(ERR_NOT_FOUND);
        };
        builder.assign(s.lset.clone());
        let mut g = s.lock();
        chks.clear();
        if g.ooo().is_none() {
            return Ok(());
        }
        get_ooo_series_chunks(
            &mut g,
            s.r#ref,
            h.opts.use_xor2_float_encoding(),
            h.opts.enable_histogram_st_encoding.load(std::sync::atomic::Ordering::Relaxed),
            self.ch.mint,
            self.ch.maxt,
            ChunkDiskMapperRef(0),
            self.ch.last_mmap_ref,
            false,
            0,
            chks,
        );
        Ok(())
    }
    fn label_names(&self, _: &Context, _: &[Matcher]) -> Result<Vec<String>, Error> {
        Err(Error::msg("not implemented"))
    }
    fn label_names_for(&self, _: &Context, _: &mut dyn Postings) -> Result<Vec<String>, Error> {
        Err(Error::msg("not implemented"))
    }
    fn close(&self) -> Result<(), Error> {
        let _ = self.hr();
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Queriers.

fn new_head_chunk_reader(head: &Arc<Head>, ino_mint: i64, mint: i64, maxt: i64) -> HeadChunkReader {
    HeadChunkReader {
        head: head.clone(),
        mint,
        maxt,
        iso_state: Some(head.iso.state(ino_mint, maxt)),
    }
}

/// Go `HeadAndOOOQuerier`: queries both the head and the out-of-order head.
pub struct HeadAndOOOQuerier {
    mint: i64,
    maxt: i64,
    head: Arc<Head>,
    index: Arc<dyn IndexReader>,
    chunkr: Arc<dyn ChunkReader>,
    /// Used for label names and values; None if the head was truncated in the meantime.
    querier: Option<BoxQuerier>,
}

impl HeadAndOOOQuerier {
    /// Go `NewHeadAndOOOQuerier`.
    pub fn new(
        ino_mint: i64,
        mint: i64,
        maxt: i64,
        head: Arc<Head>,
        ooo_iso_state: Arc<OooIsolationState>,
        querier: Option<BoxQuerier>,
    ) -> HeadAndOOOQuerier {
        let cr = new_head_chunk_reader(&head, ino_mint, mint, maxt);
        let min_ref = ooo_iso_state.min_ref;
        HeadAndOOOQuerier {
            mint,
            maxt,
            index: Arc::new(HeadAndOOOIndexReader::new(head.clone(), ino_mint, mint, maxt, min_ref)),
            chunkr: Arc::new(HeadAndOOOChunkReader::new(
                head.clone(),
                mint,
                maxt,
                Some(cr),
                Some(ooo_iso_state),
                ChunkDiskMapperRef(0),
            )),
            head,
            querier,
        }
    }
}

impl LabelQuerier for HeadAndOOOQuerier {
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, matchers: &[Matcher]) -> LabelResult {
        match &self.querier {
            None => (Vec::new(), Annotations::new(), None),
            Some(q) => q.label_values(ctx, name, hints, matchers),
        }
    }
    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, matchers: &[Matcher]) -> LabelResult {
        match &self.querier {
            None => (Vec::new(), Annotations::new(), None),
            Some(q) => q.label_names(ctx, hints, matchers),
        }
    }
    fn close(&mut self) -> Option<Error> {
        let err = self.chunkr.close().err();
        match self.querier.as_mut() {
            None => err,
            Some(q) => Error::join([err, q.close()]),
        }
    }
}

impl Querier for HeadAndOOOQuerier {
    fn select(&self, ctx: &Context, sort_series: bool, hints: Option<&SelectHints>, matchers: &[Matcher]) -> BoxSeriesSet {
        crate::querier::select_series_set(
            ctx,
            sort_series,
            hints,
            matchers,
            self.index.clone(),
            self.chunkr.clone(),
            self.head.tombstones.clone(),
            self.mint,
            self.maxt,
        )
    }
    fn as_searcher(&self) -> Option<&dyn Searcher> {
        Some(self)
    }
}

impl Searcher for HeadAndOOOQuerier {
    fn search_label_names<'a>(&'a self, ctx: &Context, hints: Option<&SearchHints>, matchers: &[Matcher]) -> BoxSearchResultSet<'a> {
        match self.querier.as_ref().and_then(|q| q.as_searcher()) {
            Some(s) => s.search_label_names(ctx, hints, matchers),
            None => storage::empty_search_result_set(),
        }
    }
    fn search_label_values<'a>(
        &'a self,
        ctx: &Context,
        name: &str,
        hints: Option<&SearchHints>,
        matchers: &[Matcher],
    ) -> BoxSearchResultSet<'a> {
        match self.querier.as_ref().and_then(|q| q.as_searcher()) {
            Some(s) => s.search_label_values(ctx, name, hints, matchers),
            None => storage::empty_search_result_set(),
        }
    }
}

/// Go `HeadAndOOOChunkQuerier`.
pub struct HeadAndOOOChunkQuerier {
    mint: i64,
    maxt: i64,
    head: Arc<Head>,
    index: Arc<dyn IndexReader>,
    chunkr: Arc<dyn ChunkReader>,
    querier: Option<BoxChunkQuerier>,
}

impl HeadAndOOOChunkQuerier {
    /// Go `NewHeadAndOOOChunkQuerier`.
    pub fn new(
        ino_mint: i64,
        mint: i64,
        maxt: i64,
        head: Arc<Head>,
        ooo_iso_state: Arc<OooIsolationState>,
        querier: Option<BoxChunkQuerier>,
    ) -> HeadAndOOOChunkQuerier {
        let cr = new_head_chunk_reader(&head, ino_mint, mint, maxt);
        let min_ref = ooo_iso_state.min_ref;
        HeadAndOOOChunkQuerier {
            mint,
            maxt,
            index: Arc::new(HeadAndOOOIndexReader::new(head.clone(), ino_mint, mint, maxt, min_ref)),
            chunkr: Arc::new(HeadAndOOOChunkReader::new(
                head.clone(),
                mint,
                maxt,
                Some(cr),
                Some(ooo_iso_state),
                ChunkDiskMapperRef(0),
            )),
            head,
            querier,
        }
    }
}

impl LabelQuerier for HeadAndOOOChunkQuerier {
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, matchers: &[Matcher]) -> LabelResult {
        match &self.querier {
            None => (Vec::new(), Annotations::new(), None),
            Some(q) => q.label_values(ctx, name, hints, matchers),
        }
    }
    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, matchers: &[Matcher]) -> LabelResult {
        match &self.querier {
            None => (Vec::new(), Annotations::new(), None),
            Some(q) => q.label_names(ctx, hints, matchers),
        }
    }
    fn close(&mut self) -> Option<Error> {
        let err = self.chunkr.close().err();
        match self.querier.as_mut() {
            None => err,
            Some(q) => Error::join([err, q.close()]),
        }
    }
}

impl ChunkQuerier for HeadAndOOOChunkQuerier {
    fn select(
        &self,
        ctx: &Context,
        sort_series: bool,
        hints: Option<&SelectHints>,
        matchers: &[Matcher],
    ) -> BoxChunkSeriesSet {
        crate::querier::select_chunk_series_set(
            ctx,
            sort_series,
            hints,
            matchers,
            range_head_ulid(),
            self.index.clone(),
            self.chunkr.clone(),
            self.head.tombstones.clone(),
            self.mint,
            self.maxt,
        )
    }
}

/// Whether the chunk ID has the OOO flag (tests).
#[doc(hidden)]
pub fn test_is_ooo_chunk_id(id: u64) -> bool {
    id & OOO_CHUNK_ID_MASK != 0
}

/// Label sort helper shared with tests.
#[doc(hidden)]
pub fn test_head_sorted_postings(head: &Head, p: Vec<SeriesRef>) -> Vec<SeriesRef> {
    let mut p = head_sorted_postings(head, Box::new(index::new_list_postings(p)));
    index::expand_postings(&mut p).unwrap_or_default()
}

/// Head label names for postings (shared with tests).
#[doc(hidden)]
pub fn test_head_label_names_for(head: &Head, ctx: &Context, p: Vec<SeriesRef>) -> Result<Vec<String>, Error> {
    head_label_names_for(head, ctx, &mut index::new_list_postings(p))
}
