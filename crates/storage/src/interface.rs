//! The storage interfaces (port of `storage/interface.go` and `storage/interface_append.go`).
//!
//! Ownership model (Rust notes):
//! - Queriers, series sets and series are owned values (`Box`/`Arc`, `'static`, `Send + Sync`), so PromQL can keep
//!   series after the set moved on, and merge queriers can run selects on threads.
//! - Sample iterators borrow their series: `series.iterator(reuse)` returns a `chunkenc::AnyIterator<'a>` that lives
//!   no longer than `&'a series`. Chunk encodings are enum variants (no allocation, reusable); other iterators are
//!   `AnyIterator::Other(Box<..>)`.
//! - Errors are [`Error`] values; methods that may fail with a partial result return it next to the error, as Go.

use std::borrow::Cow;
use std::sync::Arc;

use chunkenc::AnyIterator;
use model::exemplar::{Exemplar, QueryResult};
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{self, Labels};
use model::matcher::Matcher;
use model::metadata::Metadata;

use crate::annotations::Annotations;
use crate::chunks::BoxMetaIterator;
use crate::context::Context;
use crate::error::Error;

/// A generic series reference (Go `storage.SeriesRef`): a head series reference, a block series reference or
/// another implementation's own.
pub type SeriesRef = u64;

/// A shared series (Go `storage.Series` interface value).
pub type SeriesArc = Arc<dyn Series>;
/// A shared chunk series (Go `storage.ChunkSeries` interface value).
pub type ChunkSeriesArc = Arc<dyn ChunkSeries>;

// ---------------------------------------------------------------------------------------------------------------
// Series.

/// A single time series that allows iterating over its samples (Go `storage.Series`).
pub trait Series: Send + Sync {
    /// The complete set of labels identifying the series.
    fn labels(&self) -> &Labels;
    /// An iterator over the samples. `reuse` is an iterator to reuse, if the implementation can.
    fn iterator<'a>(&'a self, reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a>;
}

/// A single time series that allows iterating over its chunks (Go `storage.ChunkSeries`).
pub trait ChunkSeries: Send + Sync {
    /// The complete set of labels identifying the series.
    fn labels(&self) -> &Labels;
    /// An iterator over the (potentially overlapping) chunks of the series, sorted by min time. `reuse` is an
    /// iterator to reuse, if the implementation can.
    fn iterator<'a>(&'a self, reuse: Option<BoxMetaIterator<'a>>) -> BoxMetaIterator<'a>;
}

/// A set of series (Go `storage.SeriesSet`). Use: `while set.next() { let s = set.at(); ... }`, then `err()` and
/// `warnings()`.
pub trait SeriesSet: Send {
    fn next(&mut self) -> bool;
    /// The current series. Panics when `next` has not returned true. The series stays usable after `next`.
    fn at(&self) -> &SeriesArc;
    /// The error that iteration failed with. When an error occurs, the set cannot continue.
    fn err(&self) -> Option<Error>;
    /// Warnings for the whole set; possible without an error.
    fn warnings(&self) -> Annotations;
}

/// A set of chunk series (Go `storage.ChunkSeriesSet`).
pub trait ChunkSeriesSet: Send {
    fn next(&mut self) -> bool;
    /// The current chunk series. Panics when `next` has not returned true.
    fn at(&self) -> &ChunkSeriesArc;
    fn err(&self) -> Option<Error>;
    fn warnings(&self) -> Annotations;
}

/// A boxed series set.
pub type BoxSeriesSet = Box<dyn SeriesSet>;
/// A boxed chunk series set.
pub type BoxChunkSeriesSet = Box<dyn ChunkSeriesSet>;

// ---------------------------------------------------------------------------------------------------------------
// Hints.

/// Hints for a select (Go `storage.SelectHints`). Implementations may use them.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SelectHints {
    /// Start time in milliseconds for this select.
    pub start: i64,
    /// End time in milliseconds for this select.
    pub end: i64,
    /// Maximum number of results returned; 0 disables the limit.
    pub limit: i64,
    /// Query step size in milliseconds.
    pub step: i64,
    /// The surrounding function or aggregation.
    pub func: String,
    /// The label names used in an aggregation.
    pub grouping: Vec<String>,
    /// Whether the aggregation is `by` (else `without`).
    pub by: bool,
    /// The range of a range vector selector in milliseconds.
    pub range: i64,
    /// The number of shards to split series into (0 disables sharding).
    pub shard_count: u64,
    /// The shard to return, from 0 to `shard_count - 1`. Series are sharded by labels stable hash.
    pub shard_index: u64,
    /// Disables trimming chunks to [start, end]; results may then contain samples outside the range.
    pub disable_trimming: bool,
    /// Projection hints: the minimum labels to fetch (see upstream for the `__series_hash__` contract).
    pub projection_labels: Vec<String>,
    /// Whether `projection_labels` are included (else excluded).
    pub projection_include: bool,
}

/// Hints for label reads (Go `storage.LabelHints`). Results are in natural (alphabetical) order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LabelHints {
    /// Maximum number of results; 0 or less disables the limit.
    pub limit: i64,
    /// With `limit`: select the lexically smallest values of the full set.
    pub limit_smallest: bool,
}

impl LabelHints {
    /// Go `AllowsEarlyStop`: whether a read may stop once it holds `limit` values.
    pub fn allows_early_stop(h: Option<&LabelHints>) -> bool {
        matches!(h, Some(h) if h.limit > 0 && !h.limit_smallest)
    }

    /// Go `ApplyLimit`: at most `limit` values, the smallest ones with `limit_smallest`, else the leading ones.
    /// The bool tells whether the result is a new allocation (`Cow::Owned`).
    pub fn apply_limit<'a>(h: Option<&LabelHints>, values: &'a [String]) -> (Cow<'a, [String]>, bool) {
        let Some(h) = h else {
            return (Cow::Borrowed(values), false);
        };
        if h.limit <= 0 || values.len() as i64 <= h.limit {
            return (Cow::Borrowed(values), false);
        }
        if h.limit_smallest {
            return (Cow::Owned(crate::generic::smallest_n_values(values, h.limit as usize)), true);
        }
        (Cow::Borrowed(&values[..h.limit as usize]), false)
    }
}

/// Decides whether a value is in the results, and its relevance score in [0, 1] (Go `storage.Filter`).
pub trait Filter: Send + Sync {
    fn accept(&self, value: &str) -> (bool, f64);
}

/// The order of search results (Go `storage.Ordering`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Ordering {
    /// Ascending by value; the natural index order.
    #[default]
    ByValueAsc,
    /// Descending by value.
    ByValueDesc,
    /// Descending by score, ties ascending by value.
    ByScoreDesc,
}

/// Hints for search operations (Go `storage.SearchHints`).
#[derive(Clone, Default)]
pub struct SearchHints {
    /// Which values to include and their scores. `None` accepts all values with score 1.0.
    pub filter: Option<Arc<dyn Filter>>,
    /// Maximum number of results; 0 disables the limit.
    pub limit: i64,
    pub order_by: Ordering,
}

/// One search result (Go `storage.SearchResult`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchResult {
    /// The label name or label value.
    pub value: String,
    /// The relevance in [0, 1]; 1.0 is a perfect match.
    pub score: f64,
}

/// An iterator over search results (Go `storage.SearchResultSet`). Call `close` when done.
pub trait SearchResultSet {
    fn next(&mut self) -> bool;
    /// The current result; only after `next` returned true.
    fn at(&self) -> &SearchResult;
    fn warnings(&self) -> Annotations;
    fn err(&self) -> Option<Error>;
    fn close(&mut self) -> Option<Error>;
}

/// A boxed search result set that may borrow the querier it came from.
pub type BoxSearchResultSet<'a> = Box<dyn SearchResultSet + 'a>;

/// Search with relevance scores (Go `storage.Searcher`). For a value and filter, the score must be the same in
/// every implementation.
pub trait Searcher: Send + Sync {
    fn search_label_names<'a>(
        &'a self,
        ctx: &Context,
        hints: Option<&SearchHints>,
        matchers: &[Matcher],
    ) -> BoxSearchResultSet<'a>;
    fn search_label_values<'a>(
        &'a self,
        ctx: &Context,
        name: &str,
        hints: Option<&SearchHints>,
        matchers: &[Matcher],
    ) -> BoxSearchResultSet<'a>;
}

// ---------------------------------------------------------------------------------------------------------------
// Queriers.

/// The result of a label read: values, warnings, and the error (Go returns all three).
pub type LabelResult = (Vec<String>, Annotations, Option<Error>);

/// Querying access over labels (Go `storage.LabelQuerier`).
pub trait LabelQuerier: Send + Sync {
    /// All potential values for a label name, sorted. With matchers, only values of matching series.
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, matchers: &[Matcher])
    -> LabelResult;
    /// All unique label names, sorted. With matchers, only names of matching series.
    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, matchers: &[Matcher]) -> LabelResult;
    /// Releases the resources of the querier.
    fn close(&mut self) -> Option<Error>;
}

/// Querying access over time series data of a fixed time range (Go `storage.Querier`).
pub trait Querier: LabelQuerier {
    /// The series that match the matchers. Results are not checked against the matchers. `sort_series` asks for
    /// series sorted by labels (prefer not to ask).
    fn select(&self, ctx: &Context, sort_series: bool, hints: Option<&SelectHints>, matchers: &[Matcher])
    -> BoxSeriesSet;

    /// The search capability (Go type assertion `q.(storage.Searcher)`).
    fn as_searcher(&self) -> Option<&dyn Searcher> {
        None
    }

    /// Whether this is the no-op querier (Go type check against `noopQuerier`).
    #[doc(hidden)]
    fn is_noop(&self) -> bool {
        false
    }

    /// The queriers inside this one, for tests that inspect merge trees (Go tests use type switches).
    #[doc(hidden)]
    fn children(&self) -> Vec<&dyn Querier> {
        Vec::new()
    }
}

/// Querying access over chunks of a fixed time range (Go `storage.ChunkQuerier`).
pub trait ChunkQuerier: LabelQuerier {
    fn select(&self, ctx: &Context, sort_series: bool, hints: Option<&SelectHints>, matchers: &[Matcher])
    -> BoxChunkSeriesSet;

    /// The search capability (Go type assertion `q.(storage.Searcher)`).
    fn as_searcher(&self) -> Option<&dyn Searcher> {
        None
    }

    #[doc(hidden)]
    fn is_noop(&self) -> bool {
        false
    }
}

/// A boxed querier.
pub type BoxQuerier = Box<dyn Querier>;
/// A boxed chunk querier.
pub type BoxChunkQuerier = Box<dyn ChunkQuerier>;

/// Handles queries against a storage (Go `storage.Queryable`).
pub trait Queryable: Send + Sync {
    /// A new querier over the inclusive time range [mint, maxt] in milliseconds.
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error>;
}

/// Handles chunk queries against a storage (Go `storage.ChunkQueryable`).
pub trait ChunkQueryable: Send + Sync {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error>;
}

/// Samples and chunks (Go `storage.SampleAndChunkQueryable`).
pub trait SampleAndChunkQueryable: Queryable + ChunkQueryable {}
impl<T: Queryable + ChunkQueryable + ?Sized> SampleAndChunkQueryable for T {}

/// Optional incremental density and point-size sampling (Go `storage.CostSamplingQueryable`). Returns the
/// source and whether sampling is supported; `(None, true)` means a definitely empty backend.
pub trait CostSamplingQueryable {
    fn cost_sampling_queryable(&self) -> (Option<&dyn SampleAndChunkQueryable>, bool);
}

/// A queryable made from a function (Go `storage.QueryableFunc`).
pub struct QueryableFunc<F>(pub F);

impl<F: Fn(i64, i64) -> Result<BoxQuerier, Error> + Send + Sync> Queryable for QueryableFunc<F> {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        (self.0)(mint, maxt)
    }
}

/// Exemplar queries (Go `storage.ExemplarQueryable`).
pub trait ExemplarQueryable: Send + Sync {
    fn exemplar_querier(&self, ctx: &Context) -> Result<Box<dyn ExemplarQuerier>, Error>;
}

/// Reads exemplars (Go `storage.ExemplarQuerier`).
pub trait ExemplarQuerier {
    /// All exemplars that match the matchers: an intersection within one matcher list, a union between lists.
    fn select(&self, start: i64, end: i64, matchers: &[Vec<Matcher>]) -> Result<Vec<QueryResult>, Error>;
}

/// Exemplar storage (Go `storage.ExemplarStorage`).
pub trait ExemplarStorage: ExemplarQueryable + ExemplarAppender {}

// ---------------------------------------------------------------------------------------------------------------
// Appenders.

/// Creates appenders (Go `storage.Appendable`).
pub trait Appendable: Send + Sync {
    fn appender(&self, ctx: &Context) -> Box<dyn Appender>;
}

/// Creates V2 appenders (Go `storage.AppendableV2`).
pub trait AppendableV2: Send + Sync {
    fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2>;
}

/// A storage that ingests samples and answers queries (Go `storage.Storage`). All methods are thread-safe.
pub trait Storage: SampleAndChunkQueryable + Appendable + AppendableV2 {
    /// The oldest timestamp stored.
    fn start_time(&self) -> Result<i64, Error>;
    /// Closes the storage and its resources.
    fn close(&self) -> Option<Error>;
    /// The optional cost sampling capability (Go type assertion).
    fn as_cost_sampling_queryable(&self) -> Option<&dyn CostSamplingQueryable> {
        None
    }
}

/// Options for V1 appenders (Go `storage.AppendOptions`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AppendOptions {
    /// Reject out-of-order samples even if out-of-order is enabled in the TSDB.
    pub discard_out_of_order: bool,
}

/// Commit or roll back (Go `storage.AppenderTransaction`). An appender must not be used after either.
pub trait AppenderTransaction {
    /// Submits the collected samples. On error, rolls back everything as `rollback` would.
    fn commit(&mut self) -> Option<Error>;
    /// Rolls back all changes of this appender.
    fn rollback(&mut self) -> Option<Error>;
}

/// Batched appends (Go `storage.Appender`, with `ExemplarAppender`, `HistogramAppender`, `MetadataUpdater` and
/// `StartTimestampAppender`). Not thread-safe. The order of samples is kept within each series.
///
/// Every append takes an optional series reference (0 = unknown) and returns a reference for later appends.
pub trait Appender: AppenderTransaction + Send {
    /// Adds a float sample.
    fn append(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error>;
    /// Sets append options such as discarding out-of-order samples.
    fn set_options(&mut self, opts: Option<&AppendOptions>);
    /// Adds an exemplar for the series.
    fn append_exemplar(&mut self, r: SeriesRef, l: &Labels, e: &Exemplar) -> Result<SeriesRef, Error>;
    /// Adds a histogram (`h`) or a float histogram (`fh`).
    fn append_histogram(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error>;
    /// Adds a synthetic zero histogram at the start timestamp `st`, before the histogram at `t`.
    fn append_histogram_st_zero_sample(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        st: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error>;
    /// Updates the metadata of a series. Fails if the series does not exist.
    fn update_metadata(&mut self, r: SeriesRef, l: &Labels, m: &Metadata) -> Result<SeriesRef, Error>;
    /// Adds a synthetic zero sample at the start timestamp `st`, before the sample at `t`.
    fn append_st_zero_sample(&mut self, r: SeriesRef, l: &Labels, t: i64, st: i64) -> Result<SeriesRef, Error>;

    /// The optional `GetRef` capability (Go type assertion).
    fn as_get_ref(&mut self) -> Option<&mut dyn GetRef> {
        None
    }
}

/// Adds exemplars (Go `storage.ExemplarAppender`).
pub trait ExemplarAppender {
    fn append_exemplar(&mut self, r: SeriesRef, l: &Labels, e: &Exemplar) -> Result<SeriesRef, Error>;
}

/// Gives the reference of a series (Go `storage.GetRef`): 0 if unknown, with labels that will not be copied
/// again. `hash` is the hash of `lset`.
pub trait GetRef {
    fn get_ref(&mut self, lset: &Labels, hash: u64) -> (SeriesRef, Labels);
}

/// Optional data for [`AppenderV2::append`] (Go `storage.AppendV2Options`). The borrowed data lives only for
/// the call.
#[derive(Clone, Copy, Debug, Default)]
pub struct AppendV2Options<'a> {
    /// The metric family name of the series, if the caller knows it.
    pub metric_family_name: &'a str,
    /// The metadata of the series. Must be set (if known) even when it did not change.
    pub metadata: Option<&'a Metadata>,
    /// Exemplars, sorted by timestamp. Duplicate exemplar errors must be ignored by implementations.
    pub exemplars: &'a [Exemplar],
    /// Reject this append with `ERR_OUT_OF_ORDER_SAMPLE` if it is out of order.
    pub reject_out_of_order: bool,
}

/// Go `storage.AOptions`.
pub type AOptions<'a> = AppendV2Options<'a>;

/// Appends all sample types (Go `storage.AppenderV2`). Not thread-safe. The order of samples is kept within
/// each series.
pub trait AppenderV2: AppenderTransaction + Send {
    /// Appends a sample with its exemplars, metadata and start timestamp `st` (0 = unknown). Exactly one of the
    /// values is used: `fh` if set, else `h` if set, else `v`. The append of the sample is attempted even if
    /// metadata, exemplar or start timestamp appends fail; failed exemplars give an `AppendPartialError` (which
    /// carries the series reference). On success the reference of the series is returned.
    #[allow(clippy::too_many_arguments)]
    fn append(
        &mut self,
        r: SeriesRef,
        ls: &Labels,
        st: i64,
        t: i64,
        v: f64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
        opts: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error>;

    /// The optional exemplar append capability (Go type assertion to `ExemplarAppenderV2`).
    fn as_exemplar_appender_v2(&mut self) -> Option<&mut dyn ExemplarAppenderV2> {
        None
    }

    /// The implementation's type name, for error messages (Go `%T`).
    fn type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

/// Appends exemplars apart from samples (Go `storage.ExemplarAppenderV2`). They are part of the transaction.
pub trait ExemplarAppenderV2 {
    /// Appends exemplars (sorted by timestamp) for an existing series; `ERR_NOT_FOUND` if it does not exist.
    /// Every exemplar is attempted; failures give an `AppendPartialError`.
    fn append_exemplars(&mut self, r: SeriesRef, ls: &Labels, exemplars: &[Exemplar]) -> Result<SeriesRef, Error>;
}

/// An appender of only float and histogram samples (Go `storage.LimitedAppenderV1`), for migration to V2.
pub trait LimitedAppenderV1: AppenderTransaction {
    fn append(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error>;
    fn append_histogram(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error>;
}

/// Go `AppenderV2AsLimitedV1`.
pub fn appender_v2_as_limited_v1(app: Box<dyn AppenderV2>) -> Box<dyn LimitedAppenderV1> {
    Box::new(LimitedAppenderV1Adapter(app))
}

struct LimitedAppenderV1Adapter(Box<dyn AppenderV2>);

impl AppenderTransaction for LimitedAppenderV1Adapter {
    fn commit(&mut self) -> Option<Error> {
        self.0.commit()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.0.rollback()
    }
}

impl LimitedAppenderV1 for LimitedAppenderV1Adapter {
    fn append(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        self.0.append(r, l, 0, t, v, None, None, &AppendV2Options::default())
    }
    fn append_histogram(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.0.append(r, l, 0, t, 0.0, h, fh, &AppendV2Options::default())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Simple sets and mocks.

/// A series set without series, or with only an error (Go `errSeriesSet`).
pub struct ErrSeriesSet(pub Option<Error>);

impl SeriesSet for ErrSeriesSet {
    fn next(&mut self) -> bool {
        false
    }
    fn at(&self) -> &SeriesArc {
        panic!("At called on an empty series set")
    }
    fn err(&self) -> Option<Error> {
        self.0.clone()
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

/// Go `EmptySeriesSet`.
pub fn empty_series_set() -> BoxSeriesSet {
    Box::new(ErrSeriesSet(None))
}

/// Go `ErrSeriesSet`.
pub fn err_series_set(err: Error) -> BoxSeriesSet {
    Box::new(ErrSeriesSet(Some(err)))
}

/// A chunk series set without series, or with only an error (Go `errChunkSeriesSet`).
pub struct ErrChunkSeriesSet(pub Option<Error>);

impl ChunkSeriesSet for ErrChunkSeriesSet {
    fn next(&mut self) -> bool {
        false
    }
    fn at(&self) -> &ChunkSeriesArc {
        panic!("At called on an empty chunk series set")
    }
    fn err(&self) -> Option<Error> {
        self.0.clone()
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

/// Go `EmptyChunkSeriesSet`.
pub fn empty_chunk_series_set() -> BoxChunkSeriesSet {
    Box::new(ErrChunkSeriesSet(None))
}

/// Go `ErrChunkSeriesSet`.
pub fn err_chunk_series_set(err: Error) -> BoxChunkSeriesSet {
    Box::new(ErrChunkSeriesSet(Some(err)))
}

/// A mock series set that always returns the same series (Go `TestSeriesSet`).
pub struct TestSeriesSet(pub SeriesArc);

impl SeriesSet for TestSeriesSet {
    fn next(&mut self) -> bool {
        true
    }
    fn at(&self) -> &SeriesArc {
        &self.0
    }
    fn err(&self) -> Option<Error> {
        None
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

/// Go `TestSeriesSet`.
pub fn test_series_set(series: SeriesArc) -> BoxSeriesSet {
    Box::new(TestSeriesSet(series))
}

/// A series with fixed samples (Go `MockSeries`). Start timestamps are optional (empty for none).
pub struct MockSeries {
    start_timestamps: Vec<i64>,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    labels: Labels,
}

/// Go `MockSeries`.
pub fn mock_series<S: AsRef<str>>(
    start_timestamps: Vec<i64>,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    label_set: &[S],
) -> SeriesArc {
    Arc::new(MockSeries { start_timestamps, timestamps, values, labels: labels::from_strings(label_set) })
}

impl Series for MockSeries {
    fn labels(&self) -> &Labels {
        &self.labels
    }
    fn iterator<'a>(&'a self, _reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        AnyIterator::Other(Box::new(chunkenc::mock_series_iterator(
            self.start_timestamps.clone(),
            self.timestamps.clone(),
            self.values.clone(),
        )))
    }
}

/// A querier whose select calls a function (Go `MockQuerier`).
pub struct MockQuerier {
    #[allow(clippy::type_complexity)]
    pub select_mock_function: Box<dyn Fn(bool, Option<&SelectHints>, &[Matcher]) -> BoxSeriesSet + Send + Sync>,
}

impl LabelQuerier for MockQuerier {
    fn label_values(&self, _: &Context, _: &str, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), None)
    }
    fn label_names(&self, _: &Context, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), None)
    }
    fn close(&mut self) -> Option<Error> {
        None
    }
}

impl Querier for MockQuerier {
    fn select(&self, _: &Context, sort_series: bool, hints: Option<&SelectHints>, matchers: &[Matcher]) -> BoxSeriesSet {
        (self.select_mock_function)(sort_series, hints, matchers)
    }
}

/// A queryable that returns a fixed querier (Go `MockQueryable`). Each call gives the querier made by `make`.
pub struct MockQueryable<F>(pub F);

impl<F: Fn() -> BoxQuerier + Send + Sync> Queryable for MockQueryable<F> {
    fn querier(&self, _: i64, _: i64) -> Result<BoxQuerier, Error> {
        Ok((self.0)())
    }
}
