//! Port of `storage/noop.go`: queriers and sets that do nothing.

use model::matcher::Matcher;

use crate::annotations::Annotations;
use crate::context::Context;
use crate::error::Error;
use crate::generic::empty_search_result_set;
use crate::interface::*;

/// Go `noopQuerier`.
pub struct NoopQuerier;

/// Go `NoopQuerier()`.
pub fn noop_querier() -> BoxQuerier {
    Box::new(NoopQuerier)
}

impl LabelQuerier for NoopQuerier {
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

impl Querier for NoopQuerier {
    fn select(&self, _: &Context, _: bool, _: Option<&SelectHints>, _: &[Matcher]) -> BoxSeriesSet {
        noop_series_set()
    }
    fn as_searcher(&self) -> Option<&dyn Searcher> {
        Some(self)
    }
    fn is_noop(&self) -> bool {
        true
    }
}

impl Searcher for NoopQuerier {
    fn search_label_names<'a>(&'a self, _: &Context, _: Option<&SearchHints>, _: &[Matcher]) -> BoxSearchResultSet<'a> {
        empty_search_result_set()
    }
    fn search_label_values<'a>(
        &'a self,
        _: &Context,
        _: &str,
        _: Option<&SearchHints>,
        _: &[Matcher],
    ) -> BoxSearchResultSet<'a> {
        empty_search_result_set()
    }
}

/// Go `noopChunkQuerier`.
pub struct NoopChunkQuerier;

/// Go `NoopChunkedQuerier()`.
pub fn noop_chunked_querier() -> BoxChunkQuerier {
    Box::new(NoopChunkQuerier)
}

impl LabelQuerier for NoopChunkQuerier {
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

impl ChunkQuerier for NoopChunkQuerier {
    fn select(&self, _: &Context, _: bool, _: Option<&SelectHints>, _: &[Matcher]) -> BoxChunkSeriesSet {
        noop_chunked_series_set()
    }
    fn is_noop(&self) -> bool {
        true
    }
}

/// Go `NoopSeriesSet()`.
pub fn noop_series_set() -> BoxSeriesSet {
    Box::new(ErrSeriesSet(None))
}

/// Go `NoopChunkedSeriesSet()`.
pub fn noop_chunked_series_set() -> BoxChunkSeriesSet {
    Box::new(ErrChunkSeriesSet(None))
}
