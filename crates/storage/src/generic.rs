//! Port of `storage/generic.go`: the generic series set and querier adapters that let one merge implementation
//! serve samples and chunks, and the search helpers (`ApplySearchHints`, search result sets and their merge).

use std::cmp::Ordering as CmpOrdering;

use model::labels::Labels;
use model::matcher::Matcher;

use crate::annotations::Annotations;
use crate::context::Context;
use crate::error::Error;
use crate::interface::*;

// ---------------------------------------------------------------------------------------------------------------
// Generic sets and queriers (internal).

/// A series or a chunk series (the item of a generic set).
pub(crate) trait GenericItem: Clone + Send + Sync + 'static {
    fn labels(&self) -> &Labels;
}

impl GenericItem for SeriesArc {
    fn labels(&self) -> &Labels {
        (**self).labels()
    }
}

impl GenericItem for ChunkSeriesArc {
    fn labels(&self) -> &Labels {
        (**self).labels()
    }
}

/// Go `genericSeriesSet`.
pub(crate) trait GenericSet<T>: Send {
    fn next(&mut self) -> bool;
    fn at(&self) -> &T;
    fn err(&self) -> Option<Error>;
    fn warnings(&self) -> Annotations;
}

pub(crate) type GBox<T> = Box<dyn GenericSet<T>>;

impl GenericSet<SeriesArc> for BoxSeriesSet {
    fn next(&mut self) -> bool {
        (**self).next()
    }
    fn at(&self) -> &SeriesArc {
        (**self).at()
    }
    fn err(&self) -> Option<Error> {
        (**self).err()
    }
    fn warnings(&self) -> Annotations {
        (**self).warnings()
    }
}

impl GenericSet<ChunkSeriesArc> for BoxChunkSeriesSet {
    fn next(&mut self) -> bool {
        (**self).next()
    }
    fn at(&self) -> &ChunkSeriesArc {
        (**self).at()
    }
    fn err(&self) -> Option<Error> {
        (**self).err()
    }
    fn warnings(&self) -> Annotations {
        (**self).warnings()
    }
}

/// Go `seriesSetAdapter`: a generic set as a `SeriesSet`.
pub(crate) struct SeriesSetAdapter(pub GBox<SeriesArc>);

impl SeriesSet for SeriesSetAdapter {
    fn next(&mut self) -> bool {
        self.0.next()
    }
    fn at(&self) -> &SeriesArc {
        self.0.at()
    }
    fn err(&self) -> Option<Error> {
        self.0.err()
    }
    fn warnings(&self) -> Annotations {
        self.0.warnings()
    }
}

/// Go `chunkSeriesSetAdapter`: a generic set as a `ChunkSeriesSet`.
pub(crate) struct ChunkSeriesSetAdapter(pub GBox<ChunkSeriesArc>);

impl ChunkSeriesSet for ChunkSeriesSetAdapter {
    fn next(&mut self) -> bool {
        self.0.next()
    }
    fn at(&self) -> &ChunkSeriesArc {
        self.0.at()
    }
    fn err(&self) -> Option<Error> {
        self.0.err()
    }
    fn warnings(&self) -> Annotations {
        self.0.warnings()
    }
}

/// Go `genericQuerier`.
pub(crate) trait GenericQuerier<T>: Send + Sync {
    fn select(&self, ctx: &Context, sort_series: bool, hints: Option<&SelectHints>, matchers: &[Matcher]) -> GBox<T>;
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, matchers: &[Matcher])
    -> LabelResult;
    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, matchers: &[Matcher]) -> LabelResult;
    fn close(&mut self) -> Option<Error>;
    /// Go `searcherFromGenericQuerier`.
    fn searcher(&self) -> Option<&dyn Searcher>;
    /// Go `collectSearchers`.
    fn collect_searchers<'a>(&'a self, out: &mut Vec<&'a dyn Searcher>) {
        if let Some(s) = self.searcher() {
            out.push(s);
        }
    }
    /// The sample queriers inside (for tests).
    fn children(&self) -> Vec<&dyn Querier>;
}

/// Go `genericQuerierAdapter`: a plain querier or chunk querier as a generic querier.
pub(crate) struct PlainQuerier<Q>(pub Q);

impl GenericQuerier<SeriesArc> for PlainQuerier<BoxQuerier> {
    fn select(&self, ctx: &Context, sort: bool, hints: Option<&SelectHints>, matchers: &[Matcher]) -> GBox<SeriesArc> {
        Box::new(self.0.select(ctx, sort, hints, matchers))
    }
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.0.label_values(ctx, name, hints, m)
    }
    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.0.label_names(ctx, hints, m)
    }
    fn close(&mut self) -> Option<Error> {
        self.0.close()
    }
    fn searcher(&self) -> Option<&dyn Searcher> {
        self.0.as_searcher()
    }
    fn children(&self) -> Vec<&dyn Querier> {
        vec![&*self.0]
    }
}

impl GenericQuerier<ChunkSeriesArc> for PlainQuerier<BoxChunkQuerier> {
    fn select(
        &self,
        ctx: &Context,
        sort: bool,
        hints: Option<&SelectHints>,
        matchers: &[Matcher],
    ) -> GBox<ChunkSeriesArc> {
        Box::new(self.0.select(ctx, sort, hints, matchers))
    }
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.0.label_values(ctx, name, hints, m)
    }
    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.0.label_names(ctx, hints, m)
    }
    fn close(&mut self) -> Option<Error> {
        self.0.close()
    }
    fn searcher(&self) -> Option<&dyn Searcher> {
        self.0.as_searcher()
    }
    fn children(&self) -> Vec<&dyn Querier> {
        Vec::new()
    }
}

/// Go `querierAdapter` and `searchQuerierAdapter`: a generic querier as a `Querier`.
pub(crate) struct QuerierAdapter {
    pub g: Box<dyn GenericQuerier<SeriesArc>>,
    pub supports_search: bool,
}

impl LabelQuerier for QuerierAdapter {
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.g.label_values(ctx, name, hints, m)
    }
    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.g.label_names(ctx, hints, m)
    }
    fn close(&mut self) -> Option<Error> {
        self.g.close()
    }
}

impl Querier for QuerierAdapter {
    fn select(&self, ctx: &Context, sort: bool, hints: Option<&SelectHints>, matchers: &[Matcher]) -> BoxSeriesSet {
        Box::new(SeriesSetAdapter(self.g.select(ctx, sort, hints, matchers)))
    }
    fn as_searcher(&self) -> Option<&dyn Searcher> {
        if self.supports_search { Some(self) } else { None }
    }
    fn children(&self) -> Vec<&dyn Querier> {
        self.g.children()
    }
}

impl Searcher for QuerierAdapter {
    fn search_label_names<'a>(
        &'a self,
        ctx: &Context,
        hints: Option<&SearchHints>,
        matchers: &[Matcher],
    ) -> BoxSearchResultSet<'a> {
        let mut searchers = Vec::new();
        self.g.collect_searchers(&mut searchers);
        let (ctx, h, m) = (ctx.clone(), hints.cloned(), matchers.to_vec());
        merge_search_sets(hints, move |s| s.search_label_names(&ctx, h.as_ref(), &m), searchers)
    }
    fn search_label_values<'a>(
        &'a self,
        ctx: &Context,
        name: &str,
        hints: Option<&SearchHints>,
        matchers: &[Matcher],
    ) -> BoxSearchResultSet<'a> {
        let mut searchers = Vec::new();
        self.g.collect_searchers(&mut searchers);
        let (ctx, name, h, m) = (ctx.clone(), name.to_string(), hints.cloned(), matchers.to_vec());
        merge_search_sets(hints, move |s| s.search_label_values(&ctx, &name, h.as_ref(), &m), searchers)
    }
}

/// Go `chunkQuerierAdapter`: a generic querier as a `ChunkQuerier`.
pub(crate) struct ChunkQuerierAdapter {
    pub g: Box<dyn GenericQuerier<ChunkSeriesArc>>,
}

impl LabelQuerier for ChunkQuerierAdapter {
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.g.label_values(ctx, name, hints, m)
    }
    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.g.label_names(ctx, hints, m)
    }
    fn close(&mut self) -> Option<Error> {
        self.g.close()
    }
}

impl ChunkQuerier for ChunkQuerierAdapter {
    fn select(&self, ctx: &Context, sort: bool, hints: Option<&SelectHints>, m: &[Matcher]) -> BoxChunkSeriesSet {
        Box::new(ChunkSeriesSetAdapter(self.g.select(ctx, sort, hints, m)))
    }
}

/// Go `noopGenericSeriesSet`.
pub(crate) struct NoopGenericSet;

impl<T> GenericSet<T> for NoopGenericSet {
    fn next(&mut self) -> bool {
        false
    }
    fn at(&self) -> &T {
        panic!("At called on a no-op series set")
    }
    fn err(&self) -> Option<Error> {
        None
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Search result sets.

static EMPTY_RESULT: SearchResult = SearchResult { value: String::new(), score: 0.0 };

/// Go `emptySearchResultSet` and `errSearchResultSet`.
struct ErrSearchResultSet {
    err: Option<Error>,
    warnings: Annotations,
}

impl SearchResultSet for ErrSearchResultSet {
    fn next(&mut self) -> bool {
        false
    }
    fn at(&self) -> &SearchResult {
        &EMPTY_RESULT
    }
    fn warnings(&self) -> Annotations {
        self.warnings.clone()
    }
    fn err(&self) -> Option<Error> {
        self.err.clone()
    }
    fn close(&mut self) -> Option<Error> {
        None
    }
}

/// Go `EmptySearchResultSet`.
pub fn empty_search_result_set<'a>() -> BoxSearchResultSet<'a> {
    Box::new(ErrSearchResultSet { err: None, warnings: Annotations::new() })
}

/// Go `ErrSearchResultSet`: a set that fails with `err` at once; the warnings are merged and exposed.
pub fn err_search_result_set<'a>(err: Error, warnings: Vec<Annotations>) -> BoxSearchResultSet<'a> {
    let mut warns = Annotations::new();
    for w in warnings {
        warns.merge(w);
    }
    Box::new(ErrSearchResultSet { err: Some(err), warnings: warns })
}

/// Go `sliceSearchResultSet` and `errAfterSliceSet`.
struct SliceSearchResultSet {
    results: Vec<SearchResult>,
    warnings: Annotations,
    idx: isize,
    err: Option<Error>,
}

impl SearchResultSet for SliceSearchResultSet {
    fn next(&mut self) -> bool {
        self.idx += 1;
        (self.idx as usize) < self.results.len()
    }
    fn at(&self) -> &SearchResult {
        &self.results[self.idx as usize]
    }
    fn warnings(&self) -> Annotations {
        self.warnings.clone()
    }
    fn err(&self) -> Option<Error> {
        if self.idx >= self.results.len() as isize { self.err.clone() } else { None }
    }
    fn close(&mut self) -> Option<Error> {
        None
    }
}

/// Go `NewSearchResultSetFromSlice`.
pub fn new_search_result_set_from_slice<'a>(results: Vec<SearchResult>, warns: Annotations) -> BoxSearchResultSet<'a> {
    Box::new(SliceSearchResultSet { results, warnings: warns, idx: -1, err: None })
}

/// Go `NewSearchResultSetFromSliceAndError`: iterates `results`, then reports `err` once exhausted.
pub fn new_search_result_set_from_slice_and_error<'a>(
    results: Vec<SearchResult>,
    warns: Annotations,
    err: Option<Error>,
) -> BoxSearchResultSet<'a> {
    Box::new(SliceSearchResultSet { results, warnings: warns, idx: -1, err })
}

/// Go `minLinearAllocCap`.
pub const MIN_LINEAR_ALLOC_CAP: usize = 256;

/// Go `ApplySearchHints`: filters, orders and limits `values` (sorted ascending) into scored results.
pub fn apply_search_hints(values: &[String], hints: Option<&SearchHints>) -> Vec<SearchResult> {
    let default = SearchHints::default();
    let hints = hints.unwrap_or(&default);
    let Some(filter) = hints.filter.as_deref() else {
        return apply_search_hints_no_filter(values, hints);
    };
    if hints.limit > 0 {
        match hints.order_by {
            Ordering::ByScoreDesc => return top_k_by_score(values, filter, hints.limit as usize),
            Ordering::ByValueDesc => return reverse_filter_early_exit(values, filter, hints.limit as usize),
            Ordering::ByValueAsc => {}
        }
    }
    apply_search_hints_linear(values, hints, filter)
}

fn reverse_filter_early_exit(values: &[String], filter: &dyn Filter, limit: usize) -> Vec<SearchResult> {
    let mut results = Vec::with_capacity(limit.min(values.len()));
    for v in values.iter().rev() {
        if results.len() >= limit {
            break;
        }
        let (accepted, score) = filter.accept(v);
        if !accepted {
            continue;
        }
        results.push(SearchResult { value: v.clone(), score });
    }
    results
}

fn apply_search_hints_no_filter(values: &[String], hints: &SearchHints) -> Vec<SearchResult> {
    let mut n = values.len();
    if hints.limit > 0 && (hints.limit as usize) < n {
        n = hints.limit as usize;
    }
    let mut results = Vec::with_capacity(n);
    if hints.order_by == Ordering::ByValueDesc {
        for v in values.iter().rev().take(n) {
            results.push(SearchResult { value: v.clone(), score: 1.0 });
        }
        return results;
    }
    for v in &values[..n] {
        results.push(SearchResult { value: v.clone(), score: 1.0 });
    }
    results
}

fn apply_search_hints_linear(values: &[String], hints: &SearchHints, filter: &dyn Filter) -> Vec<SearchResult> {
    let mut results = Vec::with_capacity(linear_result_cap(values.len() as i64, hints.limit) as usize);
    let early_exit = hints.order_by == Ordering::ByValueAsc && hints.limit > 0;
    for v in values {
        let (accepted, score) = filter.accept(v);
        if !accepted {
            continue;
        }
        results.push(SearchResult { value: v.clone(), score });
        if early_exit && results.len() as i64 >= hints.limit {
            break;
        }
    }
    match hints.order_by {
        Ordering::ByValueDesc => results.reverse(),
        Ordering::ByScoreDesc => {
            let cmp = compare_search_results(Ordering::ByScoreDesc);
            // Go slices.SortFunc is not stable, but the order is total, so the result is the same.
            results.sort_by(cmp);
        }
        Ordering::ByValueAsc => {}
    }
    if hints.limit > 0 && results.len() as i64 > hints.limit {
        results.truncate(hints.limit as usize);
    }
    results
}

/// Go `linearResultCap`: the capacity hint for the linear path.
pub fn linear_result_cap(num_values: i64, limit: i64) -> i64 {
    if limit <= 0 {
        return num_values;
    }
    // Go `2 * limit` wraps on overflow; then the small floor is used.
    let alloc_cap = limit.wrapping_mul(2);
    if alloc_cap < limit {
        return MIN_LINEAR_ALLOC_CAP as i64;
    }
    let mut alloc_cap = alloc_cap.max(MIN_LINEAR_ALLOC_CAP as i64);
    if alloc_cap > num_values {
        alloc_cap = num_values;
    }
    alloc_cap
}

/// Go `smallestNValues`: the `n` smallest values in ascending order (0 < n < len(values)), with a bounded
/// max-heap.
pub(crate) fn smallest_n_values(values: &[String], n: usize) -> Vec<String> {
    let mut h: Vec<String> = values[..n].to_vec();
    for i in (0..h.len() / 2).rev() {
        sift_down_max(&mut h, i);
    }
    for v in &values[n..] {
        if *v < h[0] {
            h[0].clone_from(v);
            sift_down_max(&mut h, 0);
        }
    }
    h.sort();
    h
}

fn sift_down_max(h: &mut [String], mut i: usize) {
    let n = h.len();
    loop {
        let left = 2 * i + 1;
        if left >= n {
            return;
        }
        let mut largest = left;
        let right = left + 1;
        if right < n && h[right] > h[left] {
            largest = right;
        }
        if h[largest] <= h[i] {
            return;
        }
        h.swap(i, largest);
        i = largest;
    }
}

/// Go `cmp.Compare` for floats: NaN is less than any other value and equal to NaN.
fn go_cmp_f64(a: f64, b: f64) -> CmpOrdering {
    let (an, bn) = (a.is_nan(), b.is_nan());
    if an {
        return if bn { CmpOrdering::Equal } else { CmpOrdering::Less };
    }
    if bn {
        return CmpOrdering::Greater;
    }
    a.partial_cmp(&b).unwrap_or(CmpOrdering::Equal)
}

/// Go `compareSearchResults`: the total order for an ordering.
pub fn compare_search_results(o: Ordering) -> fn(&SearchResult, &SearchResult) -> CmpOrdering {
    match o {
        Ordering::ByValueDesc => |a, b| b.value.cmp(&a.value),
        Ordering::ByScoreDesc => |a, b| go_cmp_f64(b.score, a.score).then_with(|| a.value.cmp(&b.value)),
        Ordering::ByValueAsc => |a, b| a.value.cmp(&b.value),
    }
}

/// Go `searchTopKHeap.less`: the lighter entry (lower score, or higher value on ties) sits at the root.
fn topk_less(a: &SearchResult, b: &SearchResult) -> bool {
    if a.score != b.score {
        return a.score < b.score;
    }
    a.value > b.value
}

fn topk_sift_up(h: &mut [SearchResult], mut i: usize) {
    while i > 0 {
        let parent = (i - 1) / 2;
        if !topk_less(&h[i], &h[parent]) {
            return;
        }
        h.swap(i, parent);
        i = parent;
    }
}

fn topk_sift_down(h: &mut [SearchResult], mut i: usize) {
    let n = h.len();
    loop {
        let left = 2 * i + 1;
        if left >= n {
            return;
        }
        let mut smallest = left;
        let right = left + 1;
        if right < n && topk_less(&h[right], &h[left]) {
            smallest = right;
        }
        if !topk_less(&h[smallest], &h[i]) {
            return;
        }
        h.swap(i, smallest);
        i = smallest;
    }
}

/// Go `topKByScore`: the best `limit` matches under (score desc, value asc).
fn top_k_by_score(values: &[String], filter: &dyn Filter, limit: usize) -> Vec<SearchResult> {
    let mut h: Vec<SearchResult> = Vec::with_capacity(limit.min(values.len()));
    for v in values {
        let (accepted, score) = filter.accept(v);
        if !accepted {
            continue;
        }
        if h.len() < limit {
            h.push(SearchResult { value: v.clone(), score });
            let n = h.len() - 1;
            topk_sift_up(&mut h, n);
            continue;
        }
        let worst = &h[0];
        if score < worst.score {
            continue;
        }
        if score > worst.score || *v < worst.value {
            h[0] = SearchResult { value: v.clone(), score };
            topk_sift_down(&mut h, 0);
        }
    }
    // Popping gives the worst first; fill from the tail so the result is best first.
    let mut out = vec![SearchResult::default(); h.len()];
    for i in (0..out.len()).rev() {
        let n = h.len() - 1;
        h.swap(0, n);
        let r = h.pop().expect("non-empty heap");
        if !h.is_empty() {
            topk_sift_down(&mut h, 0);
        }
        out[i] = r;
    }
    out
}

/// Go `mergeSearchSets`: merges the results of several searchers (lazily created) into one stream.
pub(crate) fn merge_search_sets<'a, F>(
    hints: Option<&SearchHints>,
    f: F,
    searchers: Vec<&'a dyn Searcher>,
) -> BoxSearchResultSet<'a>
where
    F: Fn(&'a dyn Searcher) -> BoxSearchResultSet<'a> + Clone + 'a,
{
    if searchers.is_empty() {
        return empty_search_result_set();
    }
    let sets: Vec<BoxSearchResultSet<'a>> = searchers
        .into_iter()
        .map(|s| {
            let f = f.clone();
            new_lazy_search_result_set(move || f(s))
        })
        .collect();
    let (order, limit) = hints.map_or((Ordering::ByValueAsc, 0), |h| (h.order_by, h.limit));
    pairwise_merge_search_sets(sets, order, limit)
}

/// Go `MergeSearchResultSets`: merges sets that are already in the order of `hints` (ascending by value by
/// default). The returned set owns the inputs.
pub fn merge_search_result_sets<'a>(
    sets: Vec<BoxSearchResultSet<'a>>,
    hints: Option<&SearchHints>,
) -> BoxSearchResultSet<'a> {
    let (order, limit) = hints.map_or((Ordering::ByValueAsc, 0), |h| (h.order_by, h.limit));
    pairwise_merge_search_sets(sets, order, limit)
}

fn pairwise_merge_search_sets<'a>(
    mut sets: Vec<BoxSearchResultSet<'a>>,
    order: Ordering,
    limit: i64,
) -> BoxSearchResultSet<'a> {
    match sets.len() {
        0 => empty_search_result_set(),
        1 => {
            let s = sets.pop().expect("one set");
            if limit > 0 { Box::new(LimitSearchResultSet { rs: s, limit, emitted: 0 }) } else { s }
        }
        n => {
            let right = sets.split_off(n / 2);
            let left = pairwise_merge_search_sets(sets, order, limit);
            let right = pairwise_merge_search_sets(right, order, limit);
            Box::new(MergingSearchResultSet::new(left, right, order, limit))
        }
    }
}

/// Go `NewLazySearchResultSet`: calls `init` on the first use.
pub fn new_lazy_search_result_set<'a, F>(init: F) -> BoxSearchResultSet<'a>
where
    F: FnOnce() -> BoxSearchResultSet<'a> + 'a,
{
    Box::new(LazySearchResultSet { init: Some(Box::new(init)), rs: None })
}

type SearchInit<'a> = Box<dyn FnOnce() -> BoxSearchResultSet<'a> + 'a>;

struct LazySearchResultSet<'a> {
    init: Option<SearchInit<'a>>,
    rs: Option<BoxSearchResultSet<'a>>,
}

impl SearchResultSet for LazySearchResultSet<'_> {
    fn next(&mut self) -> bool {
        if self.rs.is_none() {
            let init = self.init.take().expect("lazy search set init");
            self.rs = Some(init());
        }
        self.rs.as_mut().expect("initialized").next()
    }
    fn at(&self) -> &SearchResult {
        match &self.rs {
            Some(rs) => rs.at(),
            None => &EMPTY_RESULT,
        }
    }
    fn warnings(&self) -> Annotations {
        self.rs.as_ref().map(|rs| rs.warnings()).unwrap_or_default()
    }
    fn err(&self) -> Option<Error> {
        self.rs.as_ref().and_then(|rs| rs.err())
    }
    fn close(&mut self) -> Option<Error> {
        self.rs.as_mut().and_then(|rs| rs.close())
    }
}

struct LimitSearchResultSet<'a> {
    rs: BoxSearchResultSet<'a>,
    limit: i64,
    emitted: i64,
}

impl SearchResultSet for LimitSearchResultSet<'_> {
    fn next(&mut self) -> bool {
        if self.limit > 0 && self.emitted >= self.limit {
            return false;
        }
        if self.rs.next() {
            self.emitted += 1;
            return true;
        }
        false
    }
    fn at(&self) -> &SearchResult {
        self.rs.at()
    }
    fn warnings(&self) -> Annotations {
        self.rs.warnings()
    }
    fn err(&self) -> Option<Error> {
        self.rs.err()
    }
    fn close(&mut self) -> Option<Error> {
        self.rs.close()
    }
}

/// Go `mergingSearchResultSet`: merges two sorted sets; equal entries collapse (the higher score wins under
/// value orderings). A side that fails stops, the other side is drained, and `err` joins both errors.
struct MergingSearchResultSet<'a> {
    a: BoxSearchResultSet<'a>,
    b: BoxSearchResultSet<'a>,
    cmp_fn: fn(&SearchResult, &SearchResult) -> CmpOrdering,
    value_order: bool,
    limit: i64,
    emitted: i64,
    curr: SearchResult,
    a_val: SearchResult,
    b_val: SearchResult,
    a_ok: bool,
    b_ok: bool,
    a_init: bool,
    b_init: bool,
    done: bool,
}

impl<'a> MergingSearchResultSet<'a> {
    fn new(a: BoxSearchResultSet<'a>, b: BoxSearchResultSet<'a>, order: Ordering, limit: i64) -> Self {
        MergingSearchResultSet {
            a,
            b,
            cmp_fn: compare_search_results(order),
            value_order: matches!(order, Ordering::ByValueAsc | Ordering::ByValueDesc),
            limit,
            emitted: 0,
            curr: SearchResult::default(),
            a_val: SearchResult::default(),
            b_val: SearchResult::default(),
            a_ok: false,
            b_ok: false,
            a_init: false,
            b_init: false,
            done: false,
        }
    }

    fn advance_a(&mut self) {
        self.a_ok = self.a.next();
        if self.a_ok {
            self.a_val.clone_from(self.a.at());
        }
    }

    fn advance_b(&mut self) {
        self.b_ok = self.b.next();
        if self.b_ok {
            self.b_val.clone_from(self.b.at());
        }
    }
}

impl SearchResultSet for MergingSearchResultSet<'_> {
    fn next(&mut self) -> bool {
        if self.done {
            return false;
        }
        if self.limit > 0 && self.emitted >= self.limit {
            self.done = true;
            return false;
        }
        if !self.a_init {
            self.advance_a();
            self.a_init = true;
        }
        if !self.b_init {
            self.advance_b();
            self.b_init = true;
        }
        match (self.a_ok, self.b_ok) {
            (false, false) => {
                self.done = true;
                return false;
            }
            (false, true) => {
                std::mem::swap(&mut self.curr, &mut self.b_val);
                self.advance_b();
            }
            (true, false) => {
                std::mem::swap(&mut self.curr, &mut self.a_val);
                self.advance_a();
            }
            (true, true) => match (self.cmp_fn)(&self.a_val, &self.b_val) {
                CmpOrdering::Less => {
                    std::mem::swap(&mut self.curr, &mut self.a_val);
                    self.advance_a();
                }
                CmpOrdering::Greater => {
                    std::mem::swap(&mut self.curr, &mut self.b_val);
                    self.advance_b();
                }
                CmpOrdering::Equal => {
                    if self.value_order && self.b_val.score > self.a_val.score {
                        std::mem::swap(&mut self.curr, &mut self.b_val);
                    } else {
                        std::mem::swap(&mut self.curr, &mut self.a_val);
                    }
                    self.advance_a();
                    self.advance_b();
                }
            },
        }
        self.emitted += 1;
        true
    }
    fn at(&self) -> &SearchResult {
        &self.curr
    }
    fn warnings(&self) -> Annotations {
        let mut ws = self.a.warnings();
        ws.merge(self.b.warnings());
        ws
    }
    fn err(&self) -> Option<Error> {
        Error::join([self.a.err(), self.b.err()])
    }
    fn close(&mut self) -> Option<Error> {
        let ea = self.a.close();
        let eb = self.b.close();
        Error::join([ea, eb])
    }
}
