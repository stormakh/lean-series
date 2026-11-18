//! Block querier, postings for matchers and series sets (port of `tsdb/querier.go`).
//!
//! Ownership (Rust notes):
//! - A querier owns the readers of its block (`Arc`s; closed with the querier, released on drop).
//! - A series set iterates its postings lazily, as Go does: [`OwnedPostings`] keeps the index reader next to the
//!   postings that borrow it, so no postings list is expanded into memory.
//! - Series hold the chunk metas of one series and an `Arc` of the chunk reader. Their sample iterator keeps the
//!   current chunk (an `Arc`) next to the decoder that reads it ([`OwningIter`]), and reuses the decoder from
//!   chunk to chunk, so there is no allocation per sample and none per chunk for block chunks.

use std::sync::Arc;

use chunkenc::{AnyIterator, Chunk, ChunkIterator, ValueType};
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{Labels, ScratchBuilder};
use model::matcher::{MatchType, Matcher};
use storage::{
    Annotations, BoxChunkQuerier, BoxChunkSeriesSet, BoxMetaIterator, BoxQuerier, BoxSearchResultSet,
    BoxSeriesSet, ChunkQuerier, ChunkSeries, ChunkSeriesArc, ChunkSeriesSet, Context, Error, LabelHints,
    LabelQuerier, LabelResult, Meta, MetaIterator, Querier, SearchHints, Searcher, SelectHints, Series, SeriesArc,
    SeriesSet,
};
use tsdb_format::index::{self, BoxPostings, Postings, StringIter};
use tsdb_format::tombstones::{self, Interval, Intervals, IntervalsExt};

use crate::meta::Ulid;
use crate::reader::{BlockReader, ChunkOrIterable, ChunkReader, IndexReader, Iterable};
use crate::{index_err, to_index_err, tomb_err};

/// Go `checkContextEveryNIterations`: used in some tight loops to check if the context is done.
pub const CHECK_CONTEXT_EVERY_N_ITERATIONS: usize = 100;

// ---------------------------------------------------------------------------------------------------------------
// Queriers.

/// Go `blockBaseQuerier`: the readers of a block and a time range.
pub struct BlockBaseQuerier {
    block_id: Ulid,
    index: Arc<dyn IndexReader>,
    chunks: Arc<dyn ChunkReader>,
    tombstones: Arc<dyn tombstones::Reader>,
    closed: bool,
    mint: i64,
    maxt: i64,
}

/// Go `newBlockBaseQuerier`.
pub fn new_block_base_querier(b: &dyn BlockReader, mint: i64, maxt: i64) -> Result<BlockBaseQuerier, Error> {
    let indexr = b.index().map_err(|e| Error::wrap("open index reader", e))?;
    let chunkr = match b.chunks() {
        Ok(c) => c,
        Err(e) => {
            let _ = indexr.close();
            return Err(Error::wrap("open chunk reader", e));
        }
    };
    let tombsr = match b.tombstones() {
        Ok(t) => t,
        Err(e) => {
            let _ = indexr.close();
            let _ = chunkr.close();
            return Err(Error::wrap("open tombstone reader", e));
        }
    };
    Ok(BlockBaseQuerier {
        block_id: b.meta().ulid,
        mint,
        maxt,
        index: Arc::from(indexr),
        chunks: Arc::from(chunkr),
        tombstones: tombsr,
        closed: false,
    })
}

impl BlockBaseQuerier {
    /// A querier over the given readers (Go `&blockBaseQuerier{...}` literal, used by tests and the DB).
    pub fn from_parts(
        block_id: Ulid,
        index: Arc<dyn IndexReader>,
        chunks: Arc<dyn ChunkReader>,
        tombstones: Arc<dyn tombstones::Reader>,
        mint: i64,
        maxt: i64,
    ) -> BlockBaseQuerier {
        BlockBaseQuerier { block_id, index, chunks, tombstones, closed: false, mint, maxt }
    }

    /// The ULID of the block.
    pub fn block_id(&self) -> Ulid {
        self.block_id
    }
    /// The index reader.
    pub fn index(&self) -> &Arc<dyn IndexReader> {
        &self.index
    }
    /// The chunk reader.
    pub fn chunks(&self) -> &Arc<dyn ChunkReader> {
        &self.chunks
    }
    /// The tombstones reader.
    pub fn tombstones(&self) -> &Arc<dyn tombstones::Reader> {
        &self.tombstones
    }
    /// The time range [mint, maxt].
    pub fn range(&self) -> (i64, i64) {
        (self.mint, self.maxt)
    }

    fn label_values_impl(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, ms: &[Matcher]) -> LabelResult {
        match self.index.sorted_label_values(ctx, name, hints, ms) {
            Ok(v) => (v, Annotations::new(), None),
            Err(e) => (Vec::new(), Annotations::new(), Some(e)),
        }
    }

    fn label_names_impl(&self, ctx: &Context, hints: Option<&LabelHints>, ms: &[Matcher]) -> LabelResult {
        let mut res = match self.index.label_names(ctx, ms) {
            Ok(v) => v,
            Err(e) => return (Vec::new(), Annotations::new(), Some(e)),
        };
        if let Some(h) = hints
            && h.limit > 0
            && res.len() as i64 > h.limit
        {
            res.truncate(h.limit as usize);
        }
        (res, Annotations::new(), None)
    }

    fn close_impl(&mut self) -> Option<Error> {
        if self.closed {
            return Some(Error::msg("block querier already closed"));
        }
        let errs = [
            self.index.close().err(),
            self.chunks.close().err(),
            self.tombstones.close().err().map(tomb_err),
        ];
        self.closed = true;
        Error::join(errs)
    }
}

impl Searcher for BlockBaseQuerier {
    fn search_label_names<'a>(
        &'a self,
        ctx: &Context,
        hints: Option<&SearchHints>,
        matchers: &[Matcher],
    ) -> BoxSearchResultSet<'a> {
        match self.index.label_names(ctx, matchers) {
            Ok(names) => storage::new_search_result_set_from_slice(
                storage::apply_search_hints(&names, hints),
                Annotations::new(),
            ),
            Err(e) => storage::err_search_result_set(e, Vec::new()),
        }
    }

    fn search_label_values<'a>(
        &'a self,
        ctx: &Context,
        name: &str,
        hints: Option<&SearchHints>,
        matchers: &[Matcher],
    ) -> BoxSearchResultSet<'a> {
        let default = SearchHints::default();
        let hints = hints.unwrap_or(&default);
        // Limit pushdown is only correct when natural (ascending) index order is preserved all the way to the
        // output and no filtering discards values ahead of the limit. LimitSmallest is also set to true to
        // ensure we obtain the smallest N results not just N results.
        let mut label_hints = LabelHints::default();
        if hints.order_by == storage::Ordering::ByValueAsc && hints.filter.is_none() {
            label_hints.limit = hints.limit;
            label_hints.limit_smallest = true;
        }
        // ApplySearchHints needs the values ascending by value. OrderByScore relies on the score calculated by
        // the filter.
        let res = if hints.order_by == storage::Ordering::ByScoreDesc && hints.filter.is_some() {
            self.index.label_values(ctx, name, Some(&label_hints), matchers)
        } else {
            self.index.sorted_label_values(ctx, name, Some(&label_hints), matchers)
        };
        match res {
            Ok(values) => storage::new_search_result_set_from_slice(
                storage::apply_search_hints(&values, Some(hints)),
                Annotations::new(),
            ),
            Err(e) => storage::err_search_result_set(e, Vec::new()),
        }
    }
}

impl LabelQuerier for BlockBaseQuerier {
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, ms: &[Matcher]) -> LabelResult {
        self.label_values_impl(ctx, name, hints, ms)
    }
    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, ms: &[Matcher]) -> LabelResult {
        self.label_names_impl(ctx, hints, ms)
    }
    fn close(&mut self) -> Option<Error> {
        self.close_impl()
    }
}

/// Go `blockQuerier`.
pub struct BlockQuerier(pub BlockBaseQuerier);

/// Go `NewBlockQuerier`: a querier over [mint, maxt] of the block. Takes the readers of `b` (they are owned by
/// the querier and closed with it).
pub fn new_block_querier(b: &dyn BlockReader, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
    Ok(Box::new(BlockQuerier(new_block_base_querier(b, mint, maxt)?)))
}

impl LabelQuerier for BlockQuerier {
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, ms: &[Matcher]) -> LabelResult {
        self.0.label_values_impl(ctx, name, hints, ms)
    }
    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, ms: &[Matcher]) -> LabelResult {
        self.0.label_names_impl(ctx, hints, ms)
    }
    fn close(&mut self) -> Option<Error> {
        self.0.close_impl()
    }
}

impl Querier for BlockQuerier {
    fn select(&self, ctx: &Context, sort_series: bool, hints: Option<&SelectHints>, ms: &[Matcher]) -> BoxSeriesSet {
        let q = &self.0;
        select_series_set(
            ctx,
            sort_series,
            hints,
            ms,
            q.index.clone(),
            q.chunks.clone(),
            q.tombstones.clone(),
            q.mint,
            q.maxt,
        )
    }
    fn as_searcher(&self) -> Option<&dyn Searcher> {
        Some(&self.0)
    }
}

/// Go `blockChunkQuerier`.
pub struct BlockChunkQuerier(pub BlockBaseQuerier);

/// Go `NewBlockChunkQuerier`.
pub fn new_block_chunk_querier(b: &dyn BlockReader, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
    Ok(Box::new(BlockChunkQuerier(new_block_base_querier(b, mint, maxt)?)))
}

impl LabelQuerier for BlockChunkQuerier {
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, ms: &[Matcher]) -> LabelResult {
        self.0.label_values_impl(ctx, name, hints, ms)
    }
    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, ms: &[Matcher]) -> LabelResult {
        self.0.label_names_impl(ctx, hints, ms)
    }
    fn close(&mut self) -> Option<Error> {
        self.0.close_impl()
    }
}

impl ChunkQuerier for BlockChunkQuerier {
    fn select(
        &self,
        ctx: &Context,
        sort_series: bool,
        hints: Option<&SelectHints>,
        ms: &[Matcher],
    ) -> BoxChunkSeriesSet {
        let q = &self.0;
        select_chunk_series_set(
            ctx,
            sort_series,
            hints,
            ms,
            q.block_id,
            q.index.clone(),
            q.chunks.clone(),
            q.tombstones.clone(),
            q.mint,
            q.maxt,
        )
    }
    fn as_searcher(&self) -> Option<&dyn Searcher> {
        Some(&self.0)
    }
}

/// Go `selectSeriesSet`: the series of `index` that match `ms`, over [mint, maxt], with tombstones applied.
#[allow(clippy::too_many_arguments)]
pub fn select_series_set(
    ctx: &Context,
    sort_series: bool,
    hints: Option<&SelectHints>,
    ms: &[Matcher],
    index: Arc<dyn IndexReader>,
    chunks: Arc<dyn ChunkReader>,
    tombstones: Arc<dyn tombstones::Reader>,
    mut mint: i64,
    mut maxt: i64,
) -> BoxSeriesSet {
    let mut disable_trimming = false;
    if let Some(h) = hints
        && h.step > 0
    {
        chunks.enable_chunk_cache();
    }
    let p = match OwnedPostings::new(index, |ix| select_postings(ctx, ix, sort_series, hints, ms)) {
        Ok(p) => p,
        Err(e) => return storage::err_series_set(e),
    };
    let mut chunks = chunks;
    if let Some(h) = hints {
        mint = h.start;
        maxt = h.end;
        disable_trimming = h.disable_trimming;
        if h.func == "series" {
            // When you're only looking up metadata (for example series API), you don't need to load any chunks.
            chunks = Arc::new(NopChunkReader::new());
        }
    }
    Box::new(BlockSeriesSet(
        BlockBaseSeriesSet::new(Ulid::default(), p, chunks, tombstones, mint, maxt, disable_trimming),
        None,
    ))
}

fn select_postings<'a>(
    ctx: &Context,
    ix: &'a dyn IndexReader,
    sort_series: bool,
    hints: Option<&SelectHints>,
    ms: &[Matcher],
) -> Result<BoxPostings<'a>, Error> {
    let mut p = postings_for_matchers(ctx, ix, ms)?;
    if let Some(h) = hints
        && h.shard_count > 0
    {
        p = ix.sharded_postings(p, h.shard_index, h.shard_count);
    }
    if sort_series {
        p = ix.sorted_postings(p);
    }
    Ok(p)
}

/// Go `selectChunkSeriesSet`.
#[allow(clippy::too_many_arguments)]
pub fn select_chunk_series_set(
    ctx: &Context,
    sort_series: bool,
    hints: Option<&SelectHints>,
    ms: &[Matcher],
    block_id: Ulid,
    index: Arc<dyn IndexReader>,
    chunks: Arc<dyn ChunkReader>,
    tombstones: Arc<dyn tombstones::Reader>,
    mut mint: i64,
    mut maxt: i64,
) -> BoxChunkSeriesSet {
    let mut disable_trimming = false;
    if let Some(h) = hints {
        if h.step > 0 {
            chunks.enable_chunk_cache();
        }
        mint = h.start;
        maxt = h.end;
        disable_trimming = h.disable_trimming;
    }
    let p = match OwnedPostings::new(index, |ix| select_postings(ctx, ix, sort_series, hints, ms)) {
        Ok(p) => p,
        Err(e) => return storage::err_chunk_series_set(e),
    };
    new_block_chunk_series_set(block_id, p, chunks, tombstones, mint, maxt, disable_trimming)
}

// ---------------------------------------------------------------------------------------------------------------
// Postings for matchers.

/// Go `PostingsForMatchers`: the postings of the series that match all matchers, with every upstream
/// optimization. The resulting postings are not ordered by series.
pub fn postings_for_matchers<'a>(
    ctx: &Context,
    ix: &'a dyn IndexReader,
    ms: &[Matcher],
) -> Result<BoxPostings<'a>, Error> {
    if ms.len() == 1 && ms[0].name.is_empty() && ms[0].value.is_empty() {
        let (k, v) = index::all_postings_key();
        return ix.postings(ctx, k, &[v]);
    }

    let mut its: Vec<BoxPostings<'a>> = Vec::new();
    let mut not_its: Vec<BoxPostings<'a>> = Vec::new();
    // See which label must be non-empty. Optimization for case like {l=~".", l!="1"}.
    let label_must_be_set = |name: &str| ms.iter().any(|m| m.name == name && !m.matches(""));
    let is_subtracting_matcher = |m: &Matcher| {
        if !label_must_be_set(&m.name) {
            return true;
        }
        matches!(m.typ, MatchType::NotEqual | MatchType::NotRegexp) && m.matches("")
    };
    let mut has_subtracting = false;
    let mut has_intersecting = false;
    let mut sorted: Vec<(&Matcher, bool)> = Vec::with_capacity(ms.len());
    for m in ms {
        let sub = is_subtracting_matcher(m);
        if sub {
            has_subtracting = true;
        } else {
            has_intersecting = true;
        }
        sorted.push((m, sub));
    }

    if has_subtracting && !has_intersecting {
        // If there's nothing to subtract from, add in everything and remove the notIts later. We prefer to get
        // AllPostings so that the base of subtraction (i.e. allPostings) doesn't include series that may be added
        // to the index reader during this function call.
        let (k, v) = index::all_postings_key();
        its.push(ix.postings(ctx, k, &[v])?);
    }

    // Sort matchers to have the intersecting matchers first. This way the base for subtraction is smaller and
    // there is no chance that the set we subtract from contains postings of series that didn't exist when we
    // constructed the set we subtract by.
    sorted.sort_by_key(|&(_, sub)| sub);

    for (m, _) in sorted {
        if let Some(e) = ctx.err() {
            return Err(e);
        }
        let typ = m.typ;
        if m.name.is_empty() && m.value.is_empty() {
            // We already handled the case at the top of the function, and it is unexpected to get all postings
            // again here.
            return Err(Error::msg("unexpected all postings"));
        } else if typ == MatchType::Regexp && m.value == ".*" {
            // .* regexp matches any string: do nothing.
        } else if typ == MatchType::NotRegexp && m.value == ".*" {
            return Ok(index::empty_postings());
        } else if typ == MatchType::Regexp && m.value == ".+" {
            // .+ regexp matches any non-empty string: get postings for all label values.
            let it = ix.postings_for_all_label_values(ctx, &m.name);
            if index::is_empty_postings_type(&*it) {
                return Ok(index::empty_postings());
            }
            its.push(it);
        } else if typ == MatchType::NotRegexp && m.value == ".+" {
            // .+ regexp matches any non-empty string: get postings for all label values and remove them.
            not_its.push(ix.postings_for_all_label_values(ctx, &m.name));
        } else if label_must_be_set(&m.name) {
            // If this matcher must be non-empty, we can be smarter.
            let matches_empty = m.matches("");
            let is_not = matches!(typ, MatchType::NotEqual | MatchType::NotRegexp);
            if is_not && matches_empty {
                // l!="foo": if the label can't be empty and is a Not and the inner matcher doesn't match empty,
                // then subtract it out at the end.
                let inverse = m.inverse().map_err(|e| Error::msg(e.to_string()))?;
                not_its.push(postings_for_matcher(ctx, ix, &inverse)?);
            } else if is_not && !matches_empty {
                // l!="": if the label can't be empty and is a Not, but the inner matcher can be empty we need to
                // use inversePostingsForMatcher.
                let inverse = m.inverse().map_err(|e| Error::msg(e.to_string()))?;
                let it = inverse_postings_for_matcher(ctx, ix, &inverse)?;
                if index::is_empty_postings_type(&*it) {
                    return Ok(index::empty_postings());
                }
                its.push(it);
            } else {
                // l="a", l=~"a|b", l=~"a.b", etc. Non-Not matcher, use normal postingsForMatcher.
                let it = postings_for_matcher(ctx, ix, m)?;
                if index::is_empty_postings_type(&*it) {
                    return Ok(index::empty_postings());
                }
                its.push(it);
            }
        } else {
            // l="": if the matchers for a labelname selects an empty value, it selects all the series which don't
            // have the label name set too. See https://github.com/prometheus/prometheus/issues/3575.
            not_its.push(inverse_postings_for_matcher(ctx, ix, m)?);
        }
    }

    let mut it = index::intersect(its);
    for n in not_its {
        it = index::without(it, n);
    }
    Ok(it)
}

/// Go `postingsForMatcher`. It does not return postings for missing labels.
pub fn postings_for_matcher<'a>(ctx: &Context, ix: &'a dyn IndexReader, m: &Matcher) -> Result<BoxPostings<'a>, Error> {
    // Fast-path for equal matching.
    if m.typ == MatchType::Equal {
        return ix.postings(ctx, &m.name, &[m.value.as_str()]);
    }
    // Fast-path for set matching.
    if m.typ == MatchType::Regexp {
        let set_matches = m.set_matches();
        if !set_matches.is_empty() {
            let vals: Vec<&str> = set_matches.iter().map(|s| s.as_str()).collect();
            return ix.postings(ctx, &m.name, &vals);
        }
    }
    let it = ix.postings_for_label_matching(ctx, &m.name, &|s| m.matches(s));
    match it.err() {
        Some(e) => Err(index_err(e)),
        None => Ok(it),
    }
}

/// Go `inversePostingsForMatcher`: the postings for the series with the label name set but not matching the
/// matcher.
pub fn inverse_postings_for_matcher<'a>(
    ctx: &Context,
    ix: &'a dyn IndexReader,
    m: &Matcher,
) -> Result<BoxPostings<'a>, Error> {
    // Fast-path for MatchNotRegexp matching. Inverse of a MatchNotRegexp is MatchRegexp (double negation).
    // Fast-path for set matching.
    if m.typ == MatchType::NotRegexp {
        let set_matches = m.set_matches();
        if !set_matches.is_empty() {
            let vals: Vec<&str> = set_matches.iter().map(|s| s.as_str()).collect();
            return ix.postings(ctx, &m.name, &vals);
        }
    }
    // Fast-path for MatchNotEqual matching. Inverse of a MatchNotEqual is MatchEqual (double negation).
    if m.typ == MatchType::NotEqual {
        return ix.postings(ctx, &m.name, &[m.value.as_str()]);
    }
    // If the matcher being inverted is =~"" or ="", we just want all the values.
    let it = if m.value.is_empty() && matches!(m.typ, MatchType::Regexp | MatchType::Equal) {
        ix.postings_for_all_label_values(ctx, &m.name)
    } else {
        ix.postings_for_label_matching(ctx, &m.name, &|s| !m.matches(s))
    };
    match it.err() {
        Some(e) => Err(index_err(e)),
        None => Ok(it),
    }
}

/// Go `labelValuesWithMatchers`.
pub fn label_values_with_matchers(
    ctx: &Context,
    r: &dyn IndexReader,
    name: &str,
    hints: Option<&LabelHints>,
    matchers: &[Matcher],
) -> Result<Vec<String>, Error> {
    // Limit is applied at the end, after filtering.
    let mut all_values = r
        .label_values(ctx, name, None, &[])
        .map_err(|e| Error::wrap(format_args!("fetching values of label {name}"), e))?;

    // If we have a matcher for the label name, we can filter out values that don't match before we fetch
    // postings. This is especially useful for labels with many values, e.g. __name__ with a selector like
    // {__name__="xyz"}.
    let mut has_matchers_for_other_labels = false;
    for m in matchers {
        if m.name != name {
            has_matchers_for_other_labels = true;
            continue;
        }
        let mut count = 1usize;
        let mut cancelled = None;
        all_values.retain(|v| {
            if cancelled.is_some() {
                return false;
            }
            if count.is_multiple_of(CHECK_CONTEXT_EVERY_N_ITERATIONS)
                && let Some(e) = ctx.err()
            {
                cancelled = Some(e);
                return false;
            }
            count += 1;
            m.matches(v)
        });
        if let Some(e) = cancelled {
            return Err(e);
        }
    }

    if all_values.is_empty() {
        return Ok(Vec::new());
    }

    // If we don't have any matchers for other labels, then we're done.
    if !has_matchers_for_other_labels {
        return Ok(index::apply_limit(hints, all_values));
    }

    let mut p = postings_for_matchers(ctx, r, matchers).map_err(|e| Error::wrap("fetching postings for matchers", e))?;

    let mut values_postings = Vec::with_capacity(all_values.len());
    for value in &all_values {
        let vp = r.postings(ctx, name, &[value.as_str()]).map_err(|e| {
            Error::wrap(format_args!("fetching postings for {}={}", name, model::unicode::quote(value)), e)
        })?;
        values_postings.push(vp);
    }
    let indexes = index::find_intersecting_postings(&mut *p, values_postings)
        .map_err(|e| Error::wrap("intersecting postings", index_err(e)))?;

    // FindIntersectingPostings returns the indexes ordered by series reference, not by value, so stopping at the
    // limit returns an arbitrary subset. A caller asking for the smallest values has to see all of them first.
    let early_limit = LabelHints::allows_early_stop(hints);
    let mut values = Vec::with_capacity(indexes.len());
    for idx in indexes {
        values.push(std::mem::take(&mut all_values[idx]));
        if early_limit && values.len() as i64 >= hints.map_or(0, |h| h.limit) {
            break;
        }
    }
    // A no-op unless a limit needs the smallest of what we collected.
    Ok(index::apply_limit(hints, values))
}

/// Go `labelNamesWithMatchers`.
pub fn label_names_with_matchers(ctx: &Context, r: &dyn IndexReader, matchers: &[Matcher]) -> Result<Vec<String>, Error> {
    let mut p = postings_for_matchers(ctx, r, matchers)?;
    r.label_names_for(ctx, &mut *p)
}

// ---------------------------------------------------------------------------------------------------------------
// Owned postings.

/// Postings together with the index reader they borrow from, so that a series set can iterate them lazily after
/// the call that made them returned.
pub struct OwnedPostings {
    // Field order matters: `p` borrows from `ix` and is dropped first.
    p: BoxPostings<'static>,
    ix: Arc<dyn IndexReader>,
}

impl OwnedPostings {
    /// Makes postings with `f` from the reader and keeps the reader with them.
    pub fn new<F>(ix: Arc<dyn IndexReader>, f: F) -> Result<OwnedPostings, Error>
    where
        F: for<'a> FnOnce(&'a dyn IndexReader) -> Result<BoxPostings<'a>, Error>,
    {
        let p = f(&*ix)?;
        // SAFETY: `p` borrows only the reader behind the `Arc` (the closure gets nothing else with that lifetime).
        // The reader lives at a fixed heap address for as long as the `Arc` lives, the `Arc` is stored next to the
        // postings and dropped after them (field order), and the reader is only used through shared references.
        let p: BoxPostings<'static> = unsafe { std::mem::transmute::<BoxPostings<'_>, BoxPostings<'static>>(p) };
        Ok(OwnedPostings { p, ix })
    }

    /// Postings without an index reader behind them (for example `index::empty_postings()`), over `ix`.
    pub fn from_static(ix: Arc<dyn IndexReader>, p: BoxPostings<'static>) -> OwnedPostings {
        OwnedPostings { p, ix }
    }

    /// The index reader.
    pub fn index(&self) -> &dyn IndexReader {
        &*self.ix
    }
}

impl Postings for OwnedPostings {
    #[inline]
    fn next(&mut self) -> bool {
        self.p.next()
    }
    #[inline]
    fn seek(&mut self, v: storage::SeriesRef) -> bool {
        self.p.seek(v)
    }
    #[inline]
    fn at(&self) -> storage::SeriesRef {
        self.p.at()
    }
    fn err(&self) -> Option<index::Error> {
        self.p.err()
    }
    fn is_empty_postings(&self) -> bool {
        self.p.is_empty_postings()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Series sets.

/// Go `blockBaseSeriesSet`: iterates over all series of one block that the postings select. Iterated series are
/// trimmed with the given min and max time and with the tombstones.
struct BlockBaseSeriesSet {
    block_id: Ulid,
    p: OwnedPostings,
    chunks: Arc<dyn ChunkReader>,
    tombstones: Arc<dyn tombstones::Reader>,
    mint: i64,
    maxt: i64,
    disable_trimming: bool,

    buf_chks: Vec<Meta>,
    builder: ScratchBuilder,
    err: Option<Error>,
}

/// Go `seriesData`: the data of the current series.
struct SeriesData {
    chks: Vec<Meta>,
    intervals: Intervals,
    labels: Labels,
}

impl BlockBaseSeriesSet {
    fn new(
        block_id: Ulid,
        p: OwnedPostings,
        chunks: Arc<dyn ChunkReader>,
        tombstones: Arc<dyn tombstones::Reader>,
        mint: i64,
        maxt: i64,
        disable_trimming: bool,
    ) -> BlockBaseSeriesSet {
        BlockBaseSeriesSet {
            block_id,
            p,
            chunks,
            tombstones,
            mint,
            maxt,
            disable_trimming,
            buf_chks: Vec::new(),
            builder: ScratchBuilder::default(),
            err: None,
        }
    }

    fn next(&mut self) -> Option<SeriesData> {
        while self.p.next() {
            let r = self.p.at();
            if let Err(e) = self.p.index().series(r, &mut self.builder, &mut self.buf_chks) {
                // Postings may be stale. Skip if no underlying series exists.
                if e.is(&storage::ERR_NOT_FOUND) {
                    continue;
                }
                self.err = Some(Error::wrap(format_args!("get series {r}"), e));
                return None;
            }
            if self.buf_chks.is_empty() {
                continue;
            }
            let mut intervals = match self.tombstones.get(r) {
                Ok(iv) => iv,
                Err(e) => {
                    self.err = Some(Error::wrap("get tombstones", tomb_err(e)));
                    return None;
                }
            };

            // NOTE:
            // * block time range is half-open: [meta.MinTime, meta.MaxTime).
            // * chunks are both closed: [chk.MinTime, chk.MaxTime].
            // * requested time ranges are closed: [req.Start, req.End].
            let (mut trim_front, mut trim_back) = (false, false);

            // Count those in range to size allocation (roughly - ignoring tombstones).
            let n_chks = self
                .buf_chks
                .iter()
                .filter(|c| c.max_time >= self.mint && c.min_time <= self.maxt)
                .count();
            let mut chks = Vec::with_capacity(n_chks);

            // Prefilter chunks and pick those which are not entirely deleted or totally outside of the requested
            // range.
            for chk in &self.buf_chks {
                if chk.max_time < self.mint || chk.min_time > self.maxt {
                    continue;
                }
                if Interval::new(chk.min_time, chk.max_time).is_subrange(&intervals) {
                    continue;
                }
                chks.push(chk.clone());
                // If still not entirely deleted, check if trim is needed based on requested time range.
                if !self.disable_trimming {
                    if chk.min_time < self.mint {
                        trim_front = true;
                    }
                    if chk.max_time > self.maxt {
                        trim_back = true;
                    }
                }
            }
            if chks.is_empty() {
                continue;
            }
            if trim_front {
                intervals.add(Interval::new(i64::MIN, self.mint - 1));
            }
            if trim_back {
                intervals.add(Interval::new(self.maxt + 1, i64::MAX));
            }
            return Some(SeriesData { chks, intervals, labels: self.builder.labels() });
        }
        None
    }

    fn err(&self) -> Option<Error> {
        if let Some(e) = &self.err {
            return Some(e.clone());
        }
        self.p.err().map(index_err)
    }
}

/// Go `blockSeriesSet`: sorted, populated series with applied tombstones. Series with all deleted chunks are
/// still present as series with no samples. Samples are trimmed to the requested min and max time.
struct BlockSeriesSet(BlockBaseSeriesSet, Option<SeriesArc>);

impl SeriesSet for BlockSeriesSet {
    fn next(&mut self) -> bool {
        match self.0.next() {
            Some(d) => {
                self.1 = Some(Arc::new(BlockSeriesEntry {
                    chunks: self.0.chunks.clone(),
                    block_id: self.0.block_id,
                    data: d,
                }));
                true
            }
            None => false,
        }
    }
    fn at(&self) -> &SeriesArc {
        self.1.as_ref().expect("At called before Next")
    }
    fn err(&self) -> Option<Error> {
        self.0.err()
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

/// Go `newBlockSeriesSet`.
pub fn new_block_series_set(
    p: OwnedPostings,
    chunks: Arc<dyn ChunkReader>,
    tombstones: Arc<dyn tombstones::Reader>,
    mint: i64,
    maxt: i64,
    disable_trimming: bool,
) -> BoxSeriesSet {
    Box::new(BlockSeriesSet(
        BlockBaseSeriesSet::new(Ulid::default(), p, chunks, tombstones, mint, maxt, disable_trimming),
        None,
    ))
}

/// Go `blockChunkSeriesSet`: sorted, populated chunk series with applied tombstones. Chunks are trimmed to the
/// requested [min, max] (keeping samples with min and max timestamps).
struct BlockChunkSeriesSet(BlockBaseSeriesSet, Option<ChunkSeriesArc>);

/// Go `NewBlockChunkSeriesSet`.
pub fn new_block_chunk_series_set(
    id: Ulid,
    p: OwnedPostings,
    chunks: Arc<dyn ChunkReader>,
    tombstones: Arc<dyn tombstones::Reader>,
    mint: i64,
    maxt: i64,
    disable_trimming: bool,
) -> BoxChunkSeriesSet {
    Box::new(BlockChunkSeriesSet(
        BlockBaseSeriesSet::new(id, p, chunks, tombstones, mint, maxt, disable_trimming),
        None,
    ))
}

impl ChunkSeriesSet for BlockChunkSeriesSet {
    fn next(&mut self) -> bool {
        match self.0.next() {
            Some(d) => {
                self.1 = Some(Arc::new(ChunkSeriesEntry {
                    chunks: self.0.chunks.clone(),
                    block_id: self.0.block_id,
                    data: d,
                }));
                true
            }
            None => false,
        }
    }
    fn at(&self) -> &ChunkSeriesArc {
        self.1.as_ref().expect("At called before Next")
    }
    fn err(&self) -> Option<Error> {
        self.0.err()
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

/// Go `blockSeriesEntry`.
struct BlockSeriesEntry {
    chunks: Arc<dyn ChunkReader>,
    block_id: Ulid,
    data: SeriesData,
}

impl Series for BlockSeriesEntry {
    fn labels(&self) -> &Labels {
        &self.data.labels
    }
    fn iterator<'a>(&'a self, _reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        AnyIterator::Other(Box::new(PopulateWithDelSeriesIterator::new(
            self.block_id,
            &*self.chunks,
            &self.data.chks,
            &self.data.intervals,
        )))
    }
}

/// Go `chunkSeriesEntry`.
struct ChunkSeriesEntry {
    chunks: Arc<dyn ChunkReader>,
    block_id: Ulid,
    data: SeriesData,
}

impl ChunkSeries for ChunkSeriesEntry {
    fn labels(&self) -> &Labels {
        &self.data.labels
    }
    fn iterator<'a>(&'a self, _reuse: Option<BoxMetaIterator<'a>>) -> BoxMetaIterator<'a> {
        Box::new(PopulateWithDelChunkSeriesIterator::new(
            self.block_id,
            &*self.chunks,
            &self.data.chks,
            &self.data.intervals,
        ))
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Owning iterator.

// The sources are only held (the iterator borrows them), never read.
#[allow(dead_code)]
enum IterSrc {
    Chunk(Arc<dyn Chunk>),
    Iterable(Arc<dyn Iterable>),
}

/// A sample iterator together with the chunk or iterable it reads, so that both can live in one struct. Setting
/// a new source reuses the decoder of the previous chunk (no allocation for chunks of the same encoding).
#[derive(Default)]
pub struct OwningIter {
    // Field order matters: `it` borrows from `src` and is dropped first.
    it: Option<AnyIterator<'static>>,
    src: Option<IterSrc>,
}

impl OwningIter {
    /// Iterates the samples of `c` from the start.
    pub fn set_chunk(&mut self, c: Arc<dyn Chunk>) {
        let reuse = self.it.take();
        // SAFETY: the chunk is behind an `Arc`, so its address is fixed while the `Arc` lives; the `Arc` is stored
        // in `self.src` right below and kept until the iterator is dropped or pointed at another source. The
        // reused iterator still borrows the previous source, which is alive during the call (it is dropped only
        // when `self.src` is replaced); `Chunk::iterator` resets a reused iterator to the new bytes.
        let r: &'static dyn Chunk = unsafe { &*Arc::as_ptr(&c) };
        self.it = Some(r.iterator(reuse));
        self.src = Some(IterSrc::Chunk(c));
    }

    /// Iterates the samples of the iterable `i` from the start.
    pub fn set_iterable(&mut self, i: Arc<dyn Iterable>) {
        self.it = None;
        // SAFETY: as in `set_chunk`; no iterator is reused, so the new one borrows only `i`.
        let r: &'static dyn Iterable = unsafe { &*Arc::as_ptr(&i) };
        self.it = Some(r.iterator(None));
        self.src = Some(IterSrc::Iterable(i));
    }

    #[inline]
    fn get(&self) -> Option<&AnyIterator<'static>> {
        self.it.as_ref()
    }
}

impl ChunkIterator for OwningIter {
    #[inline]
    fn next(&mut self) -> ValueType {
        match &mut self.it {
            Some(it) => it.next(),
            None => ValueType::None,
        }
    }
    fn seek(&mut self, t: i64) -> ValueType {
        match &mut self.it {
            Some(it) => it.seek(t),
            None => ValueType::None,
        }
    }
    #[inline]
    fn at(&self) -> (i64, f64) {
        self.get().map_or((i64::MIN, 0.0), |it| it.at())
    }
    fn at_histogram(&self, h: Option<Histogram>) -> (i64, Histogram) {
        match self.get() {
            Some(it) => it.at_histogram(h),
            None => (i64::MIN, h.unwrap_or_default()),
        }
    }
    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        match self.get() {
            Some(it) => it.at_float_histogram(fh),
            None => (i64::MIN, fh.unwrap_or_default()),
        }
    }
    #[inline]
    fn at_t(&self) -> i64 {
        self.get().map_or(i64::MIN, |it| it.at_t())
    }
    fn at_st(&self) -> i64 {
        self.get().map_or(0, |it| it.at_st())
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        self.get().and_then(|it| it.err())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Populate with deletes.

/// Go `populateWithDelGenericSeriesIterator`: iterates over the given chunk metas, trimming chunks by the
/// tombstone intervals. Chunks that would be fully removed by the intervals must be filtered out before.
///
/// On each iteration `curr_meta` is available. If `curr_del_iter` is set, the chunk in `curr_meta` is invalid
/// and a chunk rewrite is needed, for which `buf_iter` should be used.
struct PopulateWithDelGeneric<'a> {
    block_id: Ulid,
    cr: &'a dyn ChunkReader,
    // Expected to be sorted by min time and to belong to one series. One meta may refer to several chunks; the
    // chunk reader then gives an iterable.
    metas: &'a [Meta],
    i: isize,
    err: Option<Error>,
    buf_iter: DeletedIterator<OwningIter>,
    intervals: &'a [Interval],
    curr_del_iter: bool,
    // The current meta. Its chunk is set to the chunk from the chunk reader, if it gave one.
    curr_meta: Meta,
}

impl<'a> PopulateWithDelGeneric<'a> {
    fn new(block_id: Ulid, cr: &'a dyn ChunkReader, metas: &'a [Meta], intervals: &'a [Interval]) -> Self {
        PopulateWithDelGeneric {
            block_id,
            cr,
            metas,
            i: -1,
            err: None,
            buf_iter: DeletedIterator::new(OwningIter::default(), Vec::new()),
            intervals,
            curr_del_iter: false,
            curr_meta: Meta::default(),
        }
    }

    /// If `copy_head_chunk` is true, the head chunk (the in-memory chunk of the TSDB) is deep copied to avoid
    /// races between reads and copying chunk bytes. If the deletion intervals overlap the head chunk, it is not
    /// copied, as it is re-encoded later anyway.
    fn next(&mut self, copy_head_chunk: bool) -> bool {
        if self.err.is_some() || self.i >= self.metas.len() as isize - 1 {
            return false;
        }
        self.i += 1;
        self.curr_meta = self.metas[self.i as usize].clone();

        self.buf_iter.reset_intervals();
        for iv in self.intervals {
            if self.curr_meta.overlaps_closed_interval(iv.mint, iv.maxt) {
                self.buf_iter.intervals.add(*iv);
            }
        }

        let res = if copy_head_chunk && self.buf_iter.intervals.is_empty() {
            // ChunkOrIterableWithCopy will copy the head chunk, if it can.
            match self.cr.chunk_or_iterable_with_copy(&self.curr_meta) {
                Some(Ok((c, maxt))) => {
                    if matches!(c, ChunkOrIterable::Chunk(_)) {
                        // For the in-memory head chunk the index reader sets maxt as MaxInt64. We fix it here.
                        self.curr_meta.max_time = maxt;
                    }
                    Ok(c)
                }
                Some(Err(e)) => Err(e),
                None => self.cr.chunk_or_iterable(&self.curr_meta),
            }
        } else {
            self.cr.chunk_or_iterable(&self.curr_meta)
        };

        match res {
            Err(e) => {
                self.curr_meta.chunk = None;
                self.err = Some(Error::wrap(
                    format_args!("cannot populate chunk {} from block {}", self.curr_meta.chunk_ref, self.block_id),
                    e,
                ));
                false
            }
            Ok(ChunkOrIterable::Chunk(c)) => {
                // Use the single chunk if possible.
                self.curr_meta.chunk = Some(c.clone());
                if self.buf_iter.intervals.is_empty() {
                    // If there is no overlap with deletion intervals and a single chunk is returned, we can take
                    // chunk as it is.
                    self.curr_del_iter = false;
                    return true;
                }
                // Otherwise we need to iterate over the samples in the single chunk and create new chunks.
                self.buf_iter.iter.set_chunk(c);
                self.curr_del_iter = true;
                true
            }
            Ok(ChunkOrIterable::Iterable(it)) => {
                // Otherwise, use the iterable to create an iterator.
                self.curr_meta.chunk = None;
                self.buf_iter.iter.set_iterable(Arc::from(it));
                self.curr_del_iter = true;
                true
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Curr {
    None,
    Del,
    Plain,
}

/// Go `populateWithDelSeriesIterator`: iterates over the samples of one series.
pub struct PopulateWithDelSeriesIterator<'a> {
    g: PopulateWithDelGeneric<'a>,
    plain: OwningIter,
    curr: Curr,
    err: Option<chunkenc::Error>,
}

impl<'a> PopulateWithDelSeriesIterator<'a> {
    /// Go `&populateWithDelSeriesIterator{}` followed by `reset(blockID, cr, chks, intervals)`.
    pub fn new(block_id: Ulid, cr: &'a dyn ChunkReader, chks: &'a [Meta], intervals: &'a [Interval]) -> Self {
        PopulateWithDelSeriesIterator {
            g: PopulateWithDelGeneric::new(block_id, cr, chks, intervals),
            plain: OwningIter::default(),
            curr: Curr::None,
            err: None,
        }
    }

    #[inline]
    fn cur_next(&mut self) -> ValueType {
        match self.curr {
            Curr::Del => self.g.buf_iter.next(),
            Curr::Plain => self.plain.next(),
            Curr::None => ValueType::None,
        }
    }

    #[inline]
    fn cur(&self) -> &dyn ChunkIterator {
        match self.curr {
            Curr::Del => &self.g.buf_iter,
            _ => &self.plain,
        }
    }

    fn cur_seek(&mut self, t: i64) -> ValueType {
        match self.curr {
            Curr::Del => self.g.buf_iter.seek(t),
            Curr::Plain => self.plain.seek(t),
            Curr::None => ValueType::None,
        }
    }

    fn sync_err(&mut self) {
        if self.err.is_none()
            && let Some(e) = &self.g.err
        {
            self.err = Some(storage::to_chunk_error(e));
        }
    }

    /// The error of the chunk reader, as a storage error (Go `p.populateWithDelGenericSeriesIterator.Err()`).
    pub fn populate_err(&self) -> Option<&Error> {
        self.g.err.as_ref()
    }
}

impl ChunkIterator for PopulateWithDelSeriesIterator<'_> {
    fn next(&mut self) -> ValueType {
        let vt = self.cur_next();
        if vt != ValueType::None {
            return vt;
        }
        while self.g.next(false) {
            if self.g.curr_del_iter {
                self.curr = Curr::Del;
            } else {
                let c = self.g.curr_meta.chunk.clone().expect("chunk set");
                self.plain.set_chunk(c);
                self.curr = Curr::Plain;
            }
            let vt = self.cur_next();
            if vt != ValueType::None {
                return vt;
            }
        }
        self.sync_err();
        ValueType::None
    }

    fn seek(&mut self, t: i64) -> ValueType {
        let vt = self.cur_seek(t);
        if vt != ValueType::None {
            return vt;
        }
        while self.next() != ValueType::None {
            let vt = self.cur_seek(t);
            if vt != ValueType::None {
                return vt;
            }
        }
        ValueType::None
    }

    #[inline]
    fn at(&self) -> (i64, f64) {
        self.cur().at()
    }
    fn at_histogram(&self, h: Option<Histogram>) -> (i64, Histogram) {
        self.cur().at_histogram(h)
    }
    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        self.cur().at_float_histogram(fh)
    }
    #[inline]
    fn at_t(&self) -> i64 {
        self.cur().at_t()
    }
    fn at_st(&self) -> i64 {
        self.cur().at_st()
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        if let Some(e) = &self.err {
            return Some(e);
        }
        if self.curr != Curr::None {
            return self.cur().err();
        }
        None
    }
}

/// Go `populateWithDelChunkSeriesIterator`: iterates over the chunks of one series, re-encoding chunks that have
/// deleted samples.
pub struct PopulateWithDelChunkSeriesIterator<'a> {
    g: PopulateWithDelGeneric<'a>,
    // The current meta with its chunk set. It always maps to a single chunk (unlike `g.curr_meta`, which can refer
    // to several).
    curr_meta_with_chunk: Meta,
    // The chunks made from the iterable of the chunk reader (with deleted samples removed).
    chunks_from_iterable: Vec<Meta>,
    chunks_from_iterable_idx: isize,
    h: Histogram,
    fh: FloatHistogram,
}

impl<'a> PopulateWithDelChunkSeriesIterator<'a> {
    /// Go `&populateWithDelChunkSeriesIterator{}` followed by `reset(blockID, cr, chks, intervals)`.
    pub fn new(block_id: Ulid, cr: &'a dyn ChunkReader, chks: &'a [Meta], intervals: &'a [Interval]) -> Self {
        PopulateWithDelChunkSeriesIterator {
            g: PopulateWithDelGeneric::new(block_id, cr, chks, intervals),
            curr_meta_with_chunk: Meta::default(),
            chunks_from_iterable: Vec::new(),
            chunks_from_iterable_idx: -1,
            h: Histogram::default(),
            fh: FloatHistogram::default(),
        }
    }

    /// Go `populateCurrForSingleChunk`: sets `curr_meta_with_chunk`. Called when the samples of the deletion
    /// iterator form only one chunk.
    fn populate_curr_for_single_chunk(&mut self) -> bool {
        let it = &mut self.g.buf_iter;
        let value_type = it.next();
        if value_type == ValueType::None {
            if let Some(e) = it.err() {
                self.g.err = Some(Error::wrap("iterate chunk while re-encoding", Error::Chunk(e.clone())));
            }
            return false;
        }
        self.curr_meta_with_chunk.min_time = it.at_t();

        // Re-encode the chunk if iterator is provided. This means that it has some samples to be deleted or chunk
        // is opened.
        let enc = self.g.curr_meta.chunk.as_ref().expect("chunk set").encoding();
        let mut new_chunk = match chunkenc::new_empty_chunk(enc) {
            Ok(c) => c,
            Err(e) => {
                self.g.err = Some(Error::wrap("create new chunk while re-encoding", Error::Chunk(e)));
                return false;
            }
        };
        let mut app = match new_chunk.appender() {
            Ok(a) => a,
            Err(e) => {
                self.g.err = Some(Error::wrap("create appender while re-encoding", Error::Chunk(e)));
                return false;
            }
        };

        let mut t = 0i64;
        let mut err: Option<Error> = None;
        let mut vt = value_type;
        while vt != ValueType::None {
            if vt != value_type {
                err = Some(Error::msg(format!("found value type {vt} in chunk with {value_type}")));
                break;
            }
            let st = it.at_st();
            match vt {
                ValueType::Float => {
                    let (tt, v) = it.at();
                    t = tt;
                    app.append(&mut *new_chunk, st, t, v);
                }
                ValueType::Histogram => {
                    let (tt, h) = it.at_histogram(Some(std::mem::take(&mut self.h)));
                    t = tt;
                    self.h = h;
                    if let Err(e) = app.append_histogram(&mut *new_chunk, None, st, t, &mut self.h, true) {
                        err = Some(Error::Chunk(e));
                        break;
                    }
                }
                ValueType::FloatHistogram => {
                    let (tt, fh) = it.at_float_histogram(Some(std::mem::take(&mut self.fh)));
                    t = tt;
                    self.fh = fh;
                    if let Err(e) = app.append_float_histogram(&mut *new_chunk, None, st, t, &mut self.fh, true) {
                        err = Some(Error::Chunk(e));
                        break;
                    }
                }
                ValueType::None => {
                    err = Some(Error::msg(format!(
                        "populateCurrForSingleChunk: value type {value_type} unsupported"
                    )));
                    break;
                }
            }
            vt = it.next();
        }

        if let Some(e) = err {
            self.g.err = Some(Error::wrap("iterate chunk while re-encoding", e));
            return false;
        }
        if let Some(e) = it.err() {
            self.g.err = Some(Error::wrap("iterate chunk while re-encoding", Error::Chunk(e.clone())));
            return false;
        }
        self.curr_meta_with_chunk.chunk = Some(Arc::from(new_chunk));
        self.curr_meta_with_chunk.max_time = t;
        true
    }

    /// Go `populateChunksFromIterable`: reads the samples of the deletion iterator to create
    /// `chunks_from_iterable`, and sets `curr_meta_with_chunk` to the first chunk.
    fn populate_chunks_from_iterable(&mut self) -> bool {
        self.chunks_from_iterable.clear();
        self.chunks_from_iterable_idx = -1;

        let it = &mut self.g.buf_iter;
        let first_value_type = it.next();
        if first_value_type == ValueType::None {
            if let Some(e) = it.err() {
                self.g.err = Some(Error::wrap(
                    "populateChunksFromIterable: no samples could be read",
                    Error::Chunk(e.clone()),
                ));
            }
            return false;
        }

        let mut t = 0i64;
        let mut cmint = 0i64;
        let mut cmaxt = 0i64;
        let mut current_chunk: Option<Box<dyn Chunk>> = None;
        let mut app: Option<chunkenc::Appender> = None;
        let mut err: Option<Error> = None;
        let mut prev_value_type = ValueType::None;
        let mut has_ts = false;

        let mut current_value_type = first_value_type;
        while current_value_type != ValueType::None {
            // Check if the encoding has changed (i.e. we need to create a new chunk as chunks can't have multiple
            // encoding types). For the first sample, the following condition will always be true. Also if we need
            // to store the start time (ST), but the current chunk is not capable.
            let st = it.at_st();
            let need_ts = st != 0;
            let mut cut_new_chunk = current_value_type != prev_value_type || (!has_ts && need_ts);
            if !cut_new_chunk {
                let c = current_chunk.as_deref().expect("current chunk");
                let chunk_bytes = c.bytes().len();
                cut_new_chunk = match current_value_type {
                    // In the TSDB head we also take into account the number of samples, but here we want to keep
                    // it simple and consistent with histograms. Also the size limit is checked before sample limit
                    // in the head as well.
                    ValueType::Float => chunk_bytes > chunkenc::MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND,
                    ValueType::Histogram | ValueType::FloatHistogram => {
                        chunk_bytes > chunkenc::TARGET_BYTES_PER_HISTOGRAM_CHUNK
                            && c.num_samples() > chunkenc::MIN_SAMPLES_PER_HISTOGRAM_CHUNK
                    }
                    ValueType::None => false,
                };
            }
            if cut_new_chunk {
                if prev_value_type != ValueType::None
                    && let Some(c) = current_chunk.take()
                {
                    self.chunks_from_iterable.push(Meta {
                        chunk_ref: 0,
                        chunk: Some(Arc::from(c)),
                        min_time: cmint,
                        max_time: cmaxt,
                    });
                }
                cmint = it.at_t();
                let mut c = match current_value_type.new_chunk(need_ts, need_ts) {
                    Ok(c) => c,
                    Err(e) => {
                        err = Some(Error::Chunk(e));
                        break;
                    }
                };
                match c.appender() {
                    Ok(a) => app = Some(a),
                    Err(e) => {
                        err = Some(Error::Chunk(e));
                        break;
                    }
                }
                current_chunk = Some(c);
                has_ts = need_ts;
            }

            let a = app.as_mut().expect("appender");
            let c = current_chunk.as_deref_mut().expect("current chunk");
            let res = match current_value_type {
                ValueType::Float => {
                    let (tt, v) = it.at();
                    t = tt;
                    a.append(c, st, t, v);
                    Ok((None, false))
                }
                ValueType::Histogram => {
                    let (tt, h) = it.at_histogram(Some(std::mem::take(&mut self.h)));
                    t = tt;
                    self.h = h;
                    // No need to set prevApp as AppendHistogram will set the counter reset header for the
                    // appender that's returned.
                    a.append_histogram(c, None, st, t, &mut self.h, false)
                }
                ValueType::FloatHistogram => {
                    let (tt, fh) = it.at_float_histogram(Some(std::mem::take(&mut self.fh)));
                    t = tt;
                    self.fh = fh;
                    a.append_float_histogram(c, None, st, t, &mut self.fh, false)
                }
                ValueType::None => Ok((None, false)),
            };
            match res {
                Err(e) => {
                    err = Some(Error::Chunk(e));
                    break;
                }
                Ok((Some((new_chunk, new_app)), recoded)) => {
                    if !recoded {
                        if let Some(c) = current_chunk.take() {
                            self.chunks_from_iterable.push(Meta {
                                chunk_ref: 0,
                                chunk: Some(Arc::from(c)),
                                min_time: cmint,
                                max_time: cmaxt,
                            });
                        }
                        cmint = t;
                    }
                    current_chunk = Some(new_chunk);
                    app = Some(new_app);
                }
                Ok((None, _)) => {}
            }
            cmaxt = t;
            prev_value_type = current_value_type;
            current_value_type = it.next();
        }

        if let Some(e) = err {
            self.g.err = Some(Error::wrap("populateChunksFromIterable: error when writing new chunks", e));
            return false;
        }
        if let Some(e) = it.err() {
            self.g.err = Some(Error::wrap(
                "populateChunksFromIterable: currDelIter error when writing new chunks",
                Error::Chunk(e.clone()),
            ));
            return false;
        }
        if prev_value_type != ValueType::None
            && let Some(c) = current_chunk.take()
        {
            self.chunks_from_iterable.push(Meta {
                chunk_ref: 0,
                chunk: Some(Arc::from(c)),
                min_time: cmint,
                max_time: cmaxt,
            });
        }
        if self.chunks_from_iterable.is_empty() {
            return false;
        }
        self.curr_meta_with_chunk = self.chunks_from_iterable[0].clone();
        self.chunks_from_iterable_idx = 0;
        true
    }
}

impl MetaIterator for PopulateWithDelChunkSeriesIterator<'_> {
    fn next(&mut self) -> bool {
        if self.g.curr_meta.chunk.is_none() {
            // If we've been creating chunks from the iterable, check if there are any more chunks to iterate
            // through.
            if self.chunks_from_iterable_idx < self.chunks_from_iterable.len() as isize - 1 {
                self.chunks_from_iterable_idx += 1;
                self.curr_meta_with_chunk = self.chunks_from_iterable[self.chunks_from_iterable_idx as usize].clone();
                return true;
            }
        }

        // Move to the next chunk/deletion iterator. This is a loop as if the current deletion iterator returns no
        // samples (which means a chunk won't be created), there still might be more samples/chunks from the rest
        // of the metas.
        while self.g.next(true) {
            if !self.g.curr_del_iter {
                self.curr_meta_with_chunk = self.g.curr_meta.clone();
                return true;
            }
            if self.g.curr_meta.chunk.is_some() {
                // If the chunk reader returned a chunk, the samples of the deletion iterator will only form one
                // chunk, as the only change it might make is deleting some samples.
                if self.populate_curr_for_single_chunk() {
                    return true;
                }
            } else {
                // If the chunk reader returned an iterable, multiple chunks may be created from its samples.
                if self.populate_chunks_from_iterable() {
                    return true;
                }
            }
        }
        false
    }

    fn at(&self) -> &Meta {
        &self.curr_meta_with_chunk
    }

    fn err(&self) -> Option<Error> {
        self.g.err.clone()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Merged string iterator.

/// Go `mergedStringIter`: merges two sorted string iterators on demand (symbols of several blocks).
pub struct MergedStringIter<'a> {
    a: Box<dyn StringIter + 'a>,
    b: Box<dyn StringIter + 'a>,
    aok: bool,
    bok: bool,
    cur: String,
    err: Option<index::Error>,
}

/// Go `NewMergedStringIter`.
pub fn new_merged_string_iter<'a>(
    mut a: Box<dyn StringIter + 'a>,
    mut b: Box<dyn StringIter + 'a>,
) -> Box<dyn StringIter + 'a> {
    let aok = a.next();
    let bok = b.next();
    Box::new(MergedStringIter { a, b, aok, bok, cur: String::new(), err: None })
}

impl StringIter for MergedStringIter<'_> {
    fn next(&mut self) -> bool {
        if (!self.aok && !self.bok) || self.err.is_some() {
            return false;
        }
        self.cur.clear();
        if !self.aok {
            self.cur.push_str(self.b.at());
            self.bok = self.b.next();
            self.err = self.b.err();
        } else if !self.bok {
            self.cur.push_str(self.a.at());
            self.aok = self.a.next();
            self.err = self.a.err();
        } else if self.b.at() > self.a.at() {
            self.cur.push_str(self.a.at());
            self.aok = self.a.next();
            self.err = self.a.err();
        } else if self.a.at() > self.b.at() {
            self.cur.push_str(self.b.at());
            self.bok = self.b.next();
            self.err = self.b.err();
        } else {
            // Equal.
            self.cur.push_str(self.b.at());
            self.aok = self.a.next();
            self.err = self.a.err();
            self.bok = self.b.next();
            if self.err.is_none() {
                self.err = self.b.err();
            }
        }
        true
    }
    fn at(&self) -> &str {
        &self.cur
    }
    fn err(&self) -> Option<index::Error> {
        self.err.clone()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Deleted iterator.

/// Go `DeletedIterator`: wraps a sample iterator and skips the samples in the deletion intervals. The intervals
/// must be sorted by min time; the iterator drops intervals from the front as it moves past them. To reuse it, set
/// both `iter` and `intervals`; no other state needs resetting.
pub struct DeletedIterator<I> {
    /// The wrapped iterator.
    pub iter: I,
    /// The deletion intervals, sorted by min time in ascending order.
    pub intervals: Intervals,
}

impl<I: ChunkIterator> DeletedIterator<I> {
    /// Go `&DeletedIterator{Iter: iter, Intervals: intervals}`.
    pub fn new(iter: I, intervals: Intervals) -> DeletedIterator<I> {
        DeletedIterator { iter, intervals }
    }

    fn reset_intervals(&mut self) {
        self.intervals.clear();
    }
}

impl<I: ChunkIterator> ChunkIterator for DeletedIterator<I> {
    fn next(&mut self) -> ValueType {
        'outer: loop {
            let vt = self.iter.next();
            if vt == ValueType::None {
                return ValueType::None;
            }
            let ts = self.iter.at_t();
            while let Some(tr) = self.intervals.first().copied() {
                if tr.in_bounds(ts) {
                    continue 'outer;
                }
                if ts <= tr.maxt {
                    return vt;
                }
                // The interval list is short (the tombstones of one chunk), so dropping the front is cheap.
                self.intervals.remove(0);
            }
            return vt;
        }
    }

    fn seek(&mut self, t: i64) -> ValueType {
        if self.iter.err().is_some() {
            return ValueType::None;
        }
        let vt = self.iter.seek(t);
        if vt == ValueType::None {
            return ValueType::None;
        }
        // Now double check if the entry falls into a deleted interval.
        let ts = self.iter.at_t();
        while let Some(itv) = self.intervals.first().copied() {
            if ts < itv.mint {
                return vt;
            }
            if ts > itv.maxt {
                self.intervals.remove(0);
                continue;
            }
            // We're in the middle of an interval, we can now call Next().
            return self.next();
        }
        // The timestamp is greater than all the deleted intervals.
        vt
    }

    #[inline]
    fn at(&self) -> (i64, f64) {
        self.iter.at()
    }
    fn at_histogram(&self, h: Option<Histogram>) -> (i64, Histogram) {
        self.iter.at_histogram(h)
    }
    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        self.iter.at_float_histogram(fh)
    }
    #[inline]
    fn at_t(&self) -> i64 {
        self.iter.at_t()
    }
    fn at_st(&self) -> i64 {
        self.iter.at_st()
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        self.iter.err()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Nop chunk reader.

/// Go `nopChunkReader`: gives an empty XOR chunk for every meta.
pub struct NopChunkReader {
    empty_chunk: Arc<dyn Chunk>,
}

impl NopChunkReader {
    /// Go `newNopChunkReader`.
    pub fn new() -> NopChunkReader {
        NopChunkReader { empty_chunk: Arc::new(chunkenc::XorChunk::new()) }
    }
}

impl Default for NopChunkReader {
    fn default() -> Self {
        NopChunkReader::new()
    }
}

impl ChunkReader for NopChunkReader {
    fn chunk_or_iterable(&self, _meta: &Meta) -> Result<ChunkOrIterable, Error> {
        Ok(ChunkOrIterable::Chunk(self.empty_chunk.clone()))
    }
    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// Converts an index error into a postings error with the same text (used by callers that build postings from
/// storage errors, Go `index.ErrPostings(err)`).
pub fn err_postings<'a>(e: &Error) -> BoxPostings<'a> {
    index::err_postings(to_index_err(e))
}
