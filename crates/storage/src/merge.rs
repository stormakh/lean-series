//! Port of `storage/merge.go`: merge queriers, the merging series set, the chained series merge with its sample
//! iterator, and the compacting and concatenating chunk series mergers.

use std::sync::Arc;

use chunkenc::{AnyIterator, ChunkIterator, Encoding, ValueType};
use model::histogram::{CounterResetHint, FloatHistogram, Histogram};
use model::labels::{self, Labels};
use model::matcher::Matcher;

use crate::annotations::Annotations;
use crate::chunks::{BoxMetaIterator, Meta, MetaIterator};
use crate::context::Context;
use crate::error::Error;
use crate::generic::*;
use crate::heap;
use crate::interface::*;
use crate::lazy::{ErrorOnlySeriesSet, LazyGenericSeriesSet};
use crate::noop::{NoopChunkQuerier, NoopQuerier};
use crate::secondary::SecondaryQuerier;
use crate::series::{ChunkToSeriesDecoder, ListChunkSeriesIterator, series_to_chunks};

/// Merges series with the same labels into one series; must handle time overlaps (Go
/// `VerticalSeriesMergeFunc`).
pub type VerticalSeriesMergeFunc = Arc<dyn Fn(Vec<SeriesArc>) -> SeriesArc + Send + Sync>;

/// Merges chunk series with the same labels into one chunk series (Go `VerticalChunkSeriesMergeFunc`). How
/// chunks are merged (sorted, re-encoded) is up to the implementation.
pub type VerticalChunkSeriesMergeFunc = Arc<dyn Fn(Vec<ChunkSeriesArc>) -> ChunkSeriesArc + Send + Sync>;

type MergeFn<T> = Arc<dyn Fn(Vec<T>) -> T + Send + Sync>;

/// Go `mergeGenericQuerier`.
pub(crate) struct MergeGenericQuerier<T> {
    queriers: Vec<Box<dyn GenericQuerier<T>>>,
    /// Used when series from different queriers have the same labels.
    merge_fn: MergeFn<T>,
    /// Run the selects on threads (with secondaries, which are usually remote).
    concurrent_select: bool,
}

/// Go `NewMergeQuerier`: a querier that merges the results of primary and secondary queriers. Overlapping series
/// are merged with `merge_fn`. Secondary failures become warnings. The result is a [`Searcher`] when every
/// primary is one.
pub fn new_merge_querier(
    primaries: Vec<BoxQuerier>,
    secondaries: Vec<BoxQuerier>,
    merge_fn: VerticalSeriesMergeFunc,
) -> BoxQuerier {
    let mut primaries: Vec<BoxQuerier> = primaries.into_iter().filter(|q| !q.is_noop()).collect();
    let mut secondaries: Vec<BoxQuerier> = secondaries.into_iter().filter(|q| !q.is_noop()).collect();
    match (primaries.len(), secondaries.len()) {
        (0, 0) => return Box::new(NoopQuerier),
        (1, 0) => return primaries.pop().expect("one primary"),
        (0, 1) => {
            let s = secondaries.pop().expect("one secondary");
            return Box::new(QuerierAdapter {
                g: Box::new(SecondaryQuerier::new(Box::new(PlainQuerier(s)))),
                supports_search: true,
            });
        }
        _ => {}
    }
    let supports_search = primaries.iter().all(|q| q.as_searcher().is_some());
    let concurrent_select = !secondaries.is_empty();
    let mut queriers: Vec<Box<dyn GenericQuerier<SeriesArc>>> = Vec::with_capacity(primaries.len() + secondaries.len());
    for q in primaries {
        queriers.push(Box::new(PlainQuerier(q)));
    }
    for q in secondaries {
        queriers.push(Box::new(SecondaryQuerier::new(Box::new(PlainQuerier(q)))));
    }
    Box::new(QuerierAdapter {
        g: Box::new(MergeGenericQuerier { queriers, merge_fn: merge_fn as MergeFn<SeriesArc>, concurrent_select }),
        supports_search,
    })
}

/// Go `NewMergeChunkQuerier`: like [`new_merge_querier`] for chunk queriers.
pub fn new_merge_chunk_querier(
    primaries: Vec<BoxChunkQuerier>,
    secondaries: Vec<BoxChunkQuerier>,
    merge_fn: VerticalChunkSeriesMergeFunc,
) -> BoxChunkQuerier {
    let mut primaries: Vec<BoxChunkQuerier> = primaries.into_iter().filter(|q| !q.is_noop()).collect();
    let mut secondaries: Vec<BoxChunkQuerier> = secondaries.into_iter().filter(|q| !q.is_noop()).collect();
    match (primaries.len(), secondaries.len()) {
        (0, 0) => return Box::new(NoopChunkQuerier),
        (1, 0) => return primaries.pop().expect("one primary"),
        (0, 1) => {
            let s = secondaries.pop().expect("one secondary");
            return Box::new(ChunkQuerierAdapter { g: Box::new(SecondaryQuerier::new(Box::new(PlainQuerier(s)))) });
        }
        _ => {}
    }
    let concurrent_select = !secondaries.is_empty();
    let mut queriers: Vec<Box<dyn GenericQuerier<ChunkSeriesArc>>> =
        Vec::with_capacity(primaries.len() + secondaries.len());
    for q in primaries {
        queriers.push(Box::new(PlainQuerier(q)));
    }
    for q in secondaries {
        queriers.push(Box::new(SecondaryQuerier::new(Box::new(PlainQuerier(q)))));
    }
    Box::new(ChunkQuerierAdapter {
        g: Box::new(MergeGenericQuerier {
            queriers,
            merge_fn: merge_fn as MergeFn<ChunkSeriesArc>,
            concurrent_select,
        }),
    })
}

impl<T: GenericItem> GenericQuerier<T> for MergeGenericQuerier<T> {
    fn select(&self, ctx: &Context, _sort: bool, hints: Option<&SelectHints>, matchers: &[Matcher]) -> GBox<T> {
        let limit = hints.map_or(0, |h| h.limit);
        // The merge needs sorted sets.
        let series_sets: Vec<GBox<T>> = if !self.concurrent_select || self.queriers.len() < 2 {
            self.queriers.iter().map(|q| q.select(ctx, true, hints, matchers)).collect()
        } else {
            std::thread::scope(|s| {
                let handles: Vec<_> = self.queriers[1..]
                    .iter()
                    .map(|q| s.spawn(move || q.select(ctx, true, hints, matchers)))
                    .collect();
                let mut sets = vec![self.queriers[0].select(ctx, true, hints, matchers)];
                for h in handles {
                    match h.join() {
                        Ok(set) => sets.push(set),
                        Err(p) => std::panic::resume_unwind(p),
                    }
                }
                sets
            })
        };
        let merge_fn = self.merge_fn.clone();
        Box::new(LazyGenericSeriesSet::new(Box::new(move || {
            let mut s = new_generic_merge_series_set(series_sets, limit, merge_fn);
            let ok = s.next();
            (s, ok)
        })))
    }

    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        let (res, ws, err) = merge_results(&self.queriers, hints, &|q| q.label_values(ctx, name, hints, m));
        if let Some(err) = err {
            return (
                Vec::new(),
                Annotations::new(),
                Some(Error::wrap(format!("LabelValues() from merge generic querier for label {}", name), err)),
            );
        }
        (res, ws, None)
    }

    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        let (res, ws, err) = merge_results(&self.queriers, hints, &|q| q.label_names(ctx, hints, m));
        if let Some(err) = err {
            return (Vec::new(), Annotations::new(), Some(Error::wrap("LabelNames() from merge generic querier", err)));
        }
        (res, ws, None)
    }

    fn close(&mut self) -> Option<Error> {
        Error::join(self.queriers.iter_mut().map(|q| q.close()))
    }

    fn searcher(&self) -> Option<&dyn Searcher> {
        None
    }

    fn collect_searchers<'a>(&'a self, out: &mut Vec<&'a dyn Searcher>) {
        for q in &self.queriers {
            q.collect_searchers(out);
        }
    }

    fn children(&self) -> Vec<&dyn Querier> {
        self.queriers.iter().flat_map(|q| q.children()).collect()
    }
}

type LabelFn<'f, T> = dyn Fn(&dyn GenericQuerier<T>) -> LabelResult + 'f;

/// Go `mergeResults`: a merge sort of the results of the queriers, split in halves.
fn merge_results<T>(lq: &[Box<dyn GenericQuerier<T>>], hints: Option<&LabelHints>, f: &LabelFn<'_, T>) -> LabelResult {
    if lq.is_empty() {
        return (Vec::new(), Annotations::new(), None);
    }
    if lq.len() == 1 {
        return f(&*lq[0]);
    }
    let (a, b) = lq.split_at(lq.len() / 2);
    let mut ws = Annotations::new();
    let (s1, w, err) = merge_results(a, hints, f);
    ws.merge(w);
    if err.is_some() {
        return (Vec::new(), ws, err);
    }
    let (s2, w, err) = merge_results(b, hints, f);
    ws.merge(w);
    if err.is_some() {
        return (Vec::new(), ws, err);
    }
    let s1 = truncate_to_limit(s1, hints);
    let s2 = truncate_to_limit(s2, hints);
    let merged = merge_strings(s1, s2);
    (truncate_to_limit(merged, hints), ws, None)
}

/// Go `mergeStrings`: merges two sorted lists, dropping duplicates between them.
pub fn merge_strings(a: Vec<String>, b: Vec<String>) -> Vec<String> {
    let maxl = a.len().max(b.len());
    let mut res = Vec::with_capacity(maxl * 10 / 9);
    let mut a = a.into_iter().peekable();
    let mut b = b.into_iter().peekable();
    while let (Some(x), Some(y)) = (a.peek(), b.peek()) {
        match x.cmp(y) {
            std::cmp::Ordering::Equal => {
                res.push(a.next().expect("peeked"));
                b.next();
            }
            std::cmp::Ordering::Less => res.push(a.next().expect("peeked")),
            std::cmp::Ordering::Greater => res.push(b.next().expect("peeked")),
        }
    }
    res.extend(a);
    res.extend(b);
    res
}

fn truncate_to_limit(mut s: Vec<String>, hints: Option<&LabelHints>) -> Vec<String> {
    if let Some(h) = hints
        && h.limit > 0
        && s.len() as i64 > h.limit
    {
        s.truncate(h.limit as usize);
    }
    s
}

/// Go `NewMergeSeriesSet`: merges sorted series sets; series with equal labels are merged with `merge_fn`.
/// `limit` > 0 limits the number of series.
pub fn new_merge_series_set(sets: Vec<BoxSeriesSet>, limit: i64, merge_fn: VerticalSeriesMergeFunc) -> BoxSeriesSet {
    let generic: Vec<GBox<SeriesArc>> = sets.into_iter().map(|s| Box::new(s) as GBox<SeriesArc>).collect();
    Box::new(SeriesSetAdapter(new_generic_merge_series_set(generic, limit, merge_fn)))
}

/// Go `NewMergeChunkSeriesSet`.
pub fn new_merge_chunk_series_set(
    sets: Vec<BoxChunkSeriesSet>,
    limit: i64,
    merge_fn: VerticalChunkSeriesMergeFunc,
) -> BoxChunkSeriesSet {
    let generic: Vec<GBox<ChunkSeriesArc>> = sets.into_iter().map(|s| Box::new(s) as GBox<ChunkSeriesArc>).collect();
    Box::new(ChunkSeriesSetAdapter(new_generic_merge_series_set(generic, limit, merge_fn)))
}

/// Go `genericMergeSeriesSet`.
struct GenericMergeSeriesSet<T> {
    merge_fn: MergeFn<T>,
    heap: Vec<usize>,
    sets: Vec<GBox<T>>,
    current_sets: Vec<usize>,
    series_limit: i64,
    merged_series: i64,
    /// The merged series when more than one set has the current labels.
    merged: Option<T>,
}

fn set_less<T: GenericItem>(sets: &[GBox<T>], a: usize, b: usize) -> bool {
    labels::compare(sets[a].at().labels(), sets[b].at().labels()) < 0
}

/// Go `newGenericMergeSeriesSet`: each set must be sorted by labels.
fn new_generic_merge_series_set<T: GenericItem>(mut sets: Vec<GBox<T>>, limit: i64, merge_fn: MergeFn<T>) -> GBox<T> {
    if sets.len() == 1 {
        return sets.pop().expect("one set");
    }
    // Pre-advance the sets, so we can look at the labels under the cursor.
    let mut h = Vec::with_capacity(sets.len());
    for i in 0..sets.len() {
        if sets[i].next() {
            heap::push(&mut h, i, |a, b| set_less(&sets, *a, *b));
        }
        if let Some(err) = sets[i].err() {
            return Box::new(ErrorOnlySeriesSet(err));
        }
    }
    Box::new(GenericMergeSeriesSet {
        merge_fn,
        heap: h,
        sets,
        current_sets: Vec::new(),
        series_limit: limit,
        merged_series: 0,
        merged: None,
    })
}

impl<T: GenericItem> GenericSet<T> for GenericMergeSeriesSet<T> {
    fn next(&mut self) -> bool {
        if self.series_limit > 0 && self.merged_series >= self.series_limit {
            return false;
        }
        let sets = &mut self.sets;
        loop {
            // Advance the current sets; those not exhausted go back into the heap.
            for &i in &self.current_sets {
                if sets[i].next() {
                    heap::push(&mut self.heap, i, |a, b| set_less(sets, *a, *b));
                }
            }
            if self.heap.is_empty() {
                return false;
            }
            // Pop the sets with labels equal to the smallest.
            self.current_sets.clear();
            let first = heap::pop(&mut self.heap, |a, b| set_less(sets, *a, *b));
            self.current_sets.push(first);
            while let Some(&top) = self.heap.first() {
                if sets[top].at().labels() != sets[first].at().labels() {
                    break;
                }
                let i = heap::pop(&mut self.heap, |a, b| set_less(sets, *a, *b));
                self.current_sets.push(i);
            }
            if !self.current_sets.is_empty() {
                break;
            }
        }
        self.merged = if self.current_sets.len() > 1 {
            let series: Vec<T> = self.current_sets.iter().map(|&i| sets[i].at().clone()).collect();
            Some((self.merge_fn)(series))
        } else {
            None
        };
        self.merged_series += 1;
        true
    }

    fn at(&self) -> &T {
        if self.current_sets.len() == 1 {
            return self.sets[self.current_sets[0]].at();
        }
        self.merged.as_ref().expect("At called before Next")
    }

    fn err(&self) -> Option<Error> {
        self.sets.iter().find_map(|s| s.err())
    }

    fn warnings(&self) -> Annotations {
        let mut ws = Annotations::new();
        for s in &self.sets {
            ws.merge(s.warnings());
        }
        ws
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Chained series merge.

/// The series made by [`chained_series_merge`].
pub struct ChainedSeries {
    lset: Labels,
    series: Vec<SeriesArc>,
}

/// Go `ChainedSeriesMerge`: one series from several series with the same labels, by chaining samples. On equal
/// timestamps one sample is kept and the others are dropped. Rust note: Go returns nil for no series; this
/// returns a series without samples and with empty labels.
pub fn chained_series_merge(series: Vec<SeriesArc>) -> SeriesArc {
    let lset = series.first().map(|s| s.labels().clone()).unwrap_or_default();
    Arc::new(ChainedSeries { lset, series })
}

impl ChainedSeries {
    /// A chained series with the given labels (used where Go builds a `SeriesEntry` around the chain).
    pub fn new(lset: Labels, series: Vec<SeriesArc>) -> ChainedSeries {
        ChainedSeries { lset, series }
    }
}

impl Series for ChainedSeries {
    fn labels(&self) -> &Labels {
        &self.lset
    }
    fn iterator<'a>(&'a self, reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        chain_sample_iterator_from_series(reuse, &self.series)
    }
}

/// Iterates over the samples of several iterators of one series in timestamp order (Go `chainSampleIterator`).
/// Of samples with the same timestamp, one is kept. Optimized for series that do not overlap.
pub struct ChainSampleIterator<'a> {
    iterators: Vec<AnyIterator<'a>>,
    h: Vec<usize>,
    h_init: bool,
    curr: Option<usize>,
    last_t: i64,
    /// Whether the previous and the current sample are direct neighbours in the same base iterator.
    consecutive: bool,
    err: Option<chunkenc::Error>,
}

/// Go `ChainSampleIteratorFromSeries`.
pub fn chain_sample_iterator_from_series<'a>(_reuse: Option<AnyIterator<'a>>, series: &'a [SeriesArc]) -> AnyIterator<'a> {
    let iterators = series.iter().map(|s| s.iterator(None)).collect();
    AnyIterator::Other(Box::new(ChainSampleIterator::new(iterators)))
}

/// Go `ChainSampleIteratorFromIterators`.
pub fn chain_sample_iterator_from_iterators<'a>(
    _reuse: Option<AnyIterator<'a>>,
    iterators: Vec<AnyIterator<'a>>,
) -> AnyIterator<'a> {
    AnyIterator::Other(Box::new(ChainSampleIterator::new(iterators)))
}

impl<'a> ChainSampleIterator<'a> {
    pub fn new(iterators: Vec<AnyIterator<'a>>) -> ChainSampleIterator<'a> {
        ChainSampleIterator {
            iterators,
            h: Vec::new(),
            h_init: false,
            curr: None,
            last_t: i64::MIN,
            consecutive: false,
            err: None,
        }
    }

    /// Returns `ValueType::None` and records the joined errors of the iterators (Go `Err()`).
    fn none(&mut self) -> ValueType {
        let errs: Vec<&chunkenc::Error> = self.iterators.iter().filter_map(|it| it.err()).collect();
        self.err = match errs.len() {
            0 => None,
            1 => Some(errs[0].clone()),
            _ => Some(chunkenc::Error::Msg(
                errs.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("\n").into(),
            )),
        };
        ValueType::None
    }

    fn cur(&self, what: &str) -> &AnyIterator<'a> {
        match self.curr {
            Some(c) => &self.iterators[c],
            None => panic!("chainSampleIterator.{} called before first .Next or after .Next returned false.", what),
        }
    }
}

impl ChunkIterator for ChainSampleIterator<'_> {
    fn seek(&mut self, t: i64) -> ValueType {
        // No-op check.
        if let Some(c) = self.curr
            && self.last_t >= t
        {
            let lt = self.last_t;
            return self.iterators[c].seek(lt);
        }
        // Callers of Seek are usually not interested in consecutiveness.
        self.consecutive = false;
        self.h.clear();
        self.h_init = true;
        let its = &mut self.iterators;
        for i in 0..its.len() {
            if its[i].seek(t) == ValueType::None {
                if its[i].err().is_some() {
                    // If any iterator reports an error, abort.
                    return self.none();
                }
                continue;
            }
            heap::push(&mut self.h, i, |a, b| its[*a].at_t() < its[*b].at_t());
        }
        if !self.h.is_empty() {
            let c = heap::pop(&mut self.h, |a, b| its[*a].at_t() < its[*b].at_t());
            self.curr = Some(c);
            self.last_t = its[c].at_t();
            let lt = self.last_t;
            return its[c].seek(lt);
        }
        self.curr = None;
        self.none()
    }

    fn at(&self) -> (i64, f64) {
        self.cur("At").at()
    }

    fn at_histogram(&self, h: Option<Histogram>) -> (i64, Histogram) {
        let (t, mut h) = self.cur("AtHistogram").at_histogram(h);
        // If the sample is not consecutive with the previous one, counter resets are not known anymore.
        if !self.consecutive && h.counter_reset_hint != CounterResetHint::GaugeType {
            h.counter_reset_hint = CounterResetHint::UnknownCounterReset;
        }
        (t, h)
    }

    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        let (t, mut fh) = self.cur("AtFloatHistogram").at_float_histogram(fh);
        if !self.consecutive && fh.counter_reset_hint != CounterResetHint::GaugeType {
            fh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
        }
        (t, fh)
    }

    fn at_t(&self) -> i64 {
        self.cur("AtT").at_t()
    }

    fn at_st(&self) -> i64 {
        self.cur("AtST").at_st()
    }

    fn next(&mut self) -> ValueType {
        let mut curr_t: i64;
        let mut curr_vt;
        let mut iterator_changed = false;
        let its = &mut self.iterators;
        if !self.h_init {
            iterator_changed = true;
            self.h_init = true;
            self.h.clear();
            if its.is_empty() {
                return self.none();
            }
            // curr.next() is called first below, so not here.
            self.curr = Some(0);
            for i in 1..its.len() {
                if its[i].next() == ValueType::None {
                    if its[i].err().is_some() {
                        // If any iterator reports an error, abort; an error of its[0] is handled below.
                        return self.none();
                    }
                } else {
                    heap::push(&mut self.h, i, |a, b| its[*a].at_t() < its[*b].at_t());
                }
            }
        }
        let Some(mut c) = self.curr else {
            return self.none();
        };
        loop {
            curr_vt = its[c].next();
            if curr_vt == ValueType::None {
                if its[c].err().is_some() {
                    return self.none();
                }
                if self.h.is_empty() {
                    // No iterator left.
                    self.curr = None;
                    return self.none();
                }
            } else {
                curr_t = its[c].at_t();
                if curr_t == self.last_t {
                    // Drop the sample with the same timestamp.
                    continue;
                }
                if self.h.is_empty() {
                    // The current iterator is the only one left.
                    break;
                }
                let next_t = its[self.h[0]].at_t();
                if curr_t < next_t {
                    break;
                }
                // The current iterator does not hold the smallest timestamp.
                heap::push(&mut self.h, c, |a, b| its[*a].at_t() < its[*b].at_t());
            }
            c = heap::pop(&mut self.h, |a, b| its[*a].at_t() < its[*b].at_t());
            self.curr = Some(c);
            iterator_changed = true;
            curr_t = its[c].at_t();
            curr_vt = its[c].seek(curr_t);
            if curr_t != self.last_t {
                break;
            }
        }
        self.consecutive = !iterator_changed;
        self.last_t = curr_t;
        curr_vt
    }

    fn err(&self) -> Option<&chunkenc::Error> {
        self.err.as_ref()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Chunk series mergers.

/// The float encoding for re-encoded chunks, read once per merged series.
pub type FloatEncodingFn = Arc<dyn Fn() -> Encoding + Send + Sync>;

/// Go `NewCompactingChunkSeriesMerger`: merges chunk series into one; overlapping chunks are compacted into
/// time-ordered chunks, merging their samples with `merge_fn`.
pub fn new_compacting_chunk_series_merger(merge_fn: VerticalSeriesMergeFunc) -> VerticalChunkSeriesMergeFunc {
    new_compacting_chunk_series_merger_with_float_encoding(merge_fn, None)
}

/// Go `NewCompactingChunkSeriesMergerWithFloatEncoding`: re-encoded float chunks use `float_encoding()` (XOR
/// when `None`); samples with a start timestamp always use XOR2.
pub fn new_compacting_chunk_series_merger_with_float_encoding(
    merge_fn: VerticalSeriesMergeFunc,
    float_encoding: Option<FloatEncodingFn>,
) -> VerticalChunkSeriesMergeFunc {
    Arc::new(move |series: Vec<ChunkSeriesArc>| -> ChunkSeriesArc {
        let lset = series.first().map(|s| s.labels().clone()).unwrap_or_default();
        Arc::new(CompactingChunkSeries {
            lset,
            series,
            merge_fn: merge_fn.clone(),
            float_encoding: float_encoding.clone(),
        })
    })
}

struct CompactingChunkSeries {
    lset: Labels,
    series: Vec<ChunkSeriesArc>,
    merge_fn: VerticalSeriesMergeFunc,
    float_encoding: Option<FloatEncodingFn>,
}

impl ChunkSeries for CompactingChunkSeries {
    fn labels(&self) -> &Labels {
        &self.lset
    }
    fn iterator<'a>(&'a self, _reuse: Option<BoxMetaIterator<'a>>) -> BoxMetaIterator<'a> {
        let iterators: Vec<BoxMetaIterator<'a>> = self.series.iter().map(|s| s.iterator(None)).collect();
        let enc = match &self.float_encoding {
            Some(f) => f(),
            None => chunkenc::ENC_XOR,
        };
        let n = iterators.len();
        Box::new(CompactChunkIterator {
            merge_fn: self.merge_fn.clone(),
            iterators: iterators.into_iter().map(Some).collect(),
            n_orig: n,
            float_encoding: enc,
            h: Vec::new(),
            h_init: false,
            err: None,
            curr: Meta::default(),
        })
    }
}

/// Go `compactChunkIterator`: compacts the chunks of several iterators of one series. Overlapping chunks are
/// decoded, merged with the series merge function and encoded again into one bigger chunk.
struct CompactChunkIterator<'a> {
    merge_fn: VerticalSeriesMergeFunc,
    /// The original iterators, then the iterators over re-encoded chunks (dropped when exhausted).
    iterators: Vec<Option<BoxMetaIterator<'a>>>,
    n_orig: usize,
    float_encoding: Encoding,
    h: Vec<usize>,
    h_init: bool,
    err: Option<Error>,
    curr: Meta,
}

fn chunk_less(its: &[Option<BoxMetaIterator<'_>>], a: usize, b: usize) -> bool {
    let at = its[a].as_ref().expect("live iterator").at();
    let bt = its[b].as_ref().expect("live iterator").at();
    if at.min_time == bt.min_time {
        return at.max_time < bt.max_time;
    }
    at.min_time < bt.min_time
}

impl CompactChunkIterator<'_> {
    /// Advances iterator `i`, pushing it back into the heap if it has a chunk.
    fn advance(&mut self, i: usize) {
        let it = self.iterators[i].as_mut().expect("live iterator");
        if it.next() {
            let its = &self.iterators;
            heap::push(&mut self.h, i, |a, b| chunk_less(its, *a, *b));
        } else if i >= self.n_orig {
            // A list iterator over re-encoded chunks: it never fails, so free it.
            self.iterators[i] = None;
        }
    }
}

impl MetaIterator for CompactChunkIterator<'_> {
    fn at(&self) -> &Meta {
        &self.curr
    }

    fn next(&mut self) -> bool {
        if !self.h_init {
            self.h_init = true;
            for i in 0..self.iterators.len() {
                if self.iterators[i].as_mut().expect("live iterator").next() {
                    let its = &self.iterators;
                    heap::push(&mut self.h, i, |a, b| chunk_less(its, *a, *b));
                }
            }
        }
        if self.h.is_empty() {
            return false;
        }

        let i = {
            let its = &self.iterators;
            heap::pop(&mut self.h, |a, b| chunk_less(its, *a, *b))
        };
        self.curr = self.iterators[i].as_ref().expect("live iterator").at().clone();
        self.advance(i);

        let mut overlapping: Vec<SeriesArc> = Vec::new();
        let mut o_max_time = self.curr.max_time;
        let mut prev = self.curr.clone();
        // Detect overlaps to compact. Identical chunks are deduplicated on the fly.
        while let Some(&top) = self.h.first() {
            let next = self.iterators[top].as_ref().expect("live iterator").at();
            if next.min_time > o_max_time {
                // No overlap with the current one.
                break;
            }
            if next.min_time != prev.min_time
                || next.max_time != prev.max_time
                || next.chunk().bytes() != prev.chunk().bytes()
            {
                // The series is the same, so the labels do not matter here.
                overlapping.push(Arc::new(ChunkToSeriesDecoder::new(Labels::default(), next.clone())));
                if next.max_time > o_max_time {
                    o_max_time = next.max_time;
                }
                prev = next.clone();
            }
            let i = {
                let its = &self.iterators;
                heap::pop(&mut self.h, |a, b| chunk_less(its, *a, *b))
            };
            self.advance(i);
        }
        if overlapping.is_empty() {
            return true;
        }

        // Add the current chunk, which is not in the overlap list yet.
        overlapping.push(Arc::new(ChunkToSeriesDecoder::new(Labels::default(), self.curr.clone())));
        let merged = (self.merge_fn)(overlapping);
        let chks = match series_to_chunks(&*merged, self.float_encoding) {
            Ok(c) => c,
            Err(e) => {
                self.err = Some(e);
                return false;
            }
        };
        let mut iter = ListChunkSeriesIterator::new(chks.into());
        if !iter.next() {
            panic!("unexpected seriesToChunkEncoder lack of iterations");
        }
        self.curr = iter.at().clone();
        if iter.next() {
            let idx = self.iterators.len();
            self.iterators.push(Some(Box::new(iter)));
            let its = &self.iterators;
            heap::push(&mut self.h, idx, |a, b| chunk_less(its, *a, *b));
        }
        true
    }

    fn err(&self) -> Option<Error> {
        let errs = self.iterators[..self.n_orig].iter().map(|it| it.as_ref().and_then(|it| it.err()));
        Error::join(errs.chain(std::iter::once(self.err.clone())))
    }
}

/// Go `NewConcatenatingChunkSeriesMerger`: concatenates the chunks of the series. The result may overlap and
/// be unsorted.
pub fn new_concatenating_chunk_series_merger() -> VerticalChunkSeriesMergeFunc {
    Arc::new(|series: Vec<ChunkSeriesArc>| -> ChunkSeriesArc {
        let lset = series.first().map(|s| s.labels().clone()).unwrap_or_default();
        Arc::new(ConcatenatingChunkSeries { lset, series })
    })
}

struct ConcatenatingChunkSeries {
    lset: Labels,
    series: Vec<ChunkSeriesArc>,
}

impl ChunkSeries for ConcatenatingChunkSeries {
    fn labels(&self) -> &Labels {
        &self.lset
    }
    fn iterator<'a>(&'a self, _reuse: Option<BoxMetaIterator<'a>>) -> BoxMetaIterator<'a> {
        Box::new(ConcatenatingChunkIterator::new(self.series.iter().map(|s| s.iterator(None)).collect()))
    }
}

/// Go `concatenatingChunkIterator`.
pub struct ConcatenatingChunkIterator<'a> {
    iterators: Vec<BoxMetaIterator<'a>>,
    idx: usize,
    curr: Meta,
}

impl<'a> ConcatenatingChunkIterator<'a> {
    pub fn new(iterators: Vec<BoxMetaIterator<'a>>) -> Self {
        ConcatenatingChunkIterator { iterators, idx: 0, curr: Meta::default() }
    }
}

impl MetaIterator for ConcatenatingChunkIterator<'_> {
    fn at(&self) -> &Meta {
        &self.curr
    }

    fn next(&mut self) -> bool {
        while self.idx < self.iterators.len() {
            let it = &mut self.iterators[self.idx];
            if it.next() {
                self.curr = it.at().clone();
                return true;
            }
            if it.err().is_some() {
                return false;
            }
            self.idx += 1;
        }
        false
    }

    fn err(&self) -> Option<Error> {
        Error::join(self.iterators.iter().map(|it| it.err()))
    }
}
