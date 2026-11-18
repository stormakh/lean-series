//! Port of Prometheus `storage/` (without `storage/remote`): the contract between PromQL and the TSDB.
//!
//! - [`interface`]: `Queryable`/`Querier`, `ChunkQueryable`/`ChunkQuerier`, `Series`/`SeriesSet`,
//!   `ChunkSeries`/`ChunkSeriesSet`, hints, `Appender` and `AppenderV2`, `Storage`.
//! - [`merge`], [`fanout`]: merge queriers and series sets, the chained and compacting mergers, fanout storage.
//! - [`buffer`], [`memoized`], [`series`]: iterators and series helpers.
//! - [`annotations`]: query warnings and infos (port of `util/annotations`).
//! - [`chunks`]: chunk metas, chunk iterators and samples (the parts of `tsdb/chunks` that storage needs).
//! - [`context`]: cancellation and deadlines (Go `context.Context`).
//! - [`error`]: the storage errors with Go's texts and `errors.Is` semantics.

pub mod annotations;
pub mod buffer;
pub mod chunks;
pub mod context;
pub mod error;
pub mod fanout;
pub mod generic;
mod heap;
pub mod interface;
mod lazy;
pub mod memoized;
pub mod merge;
pub mod noop;
mod secondary;
pub mod series;
pub mod tsdbutil;

pub use annotations::Annotations;
pub use buffer::*;
pub use chunks::{BoxMetaIterator, ChunkRef, Meta, MetaIterator, Sample};
pub use context::{CancelToken, Context, ERR_CANCELED, ERR_DEADLINE_EXCEEDED};
pub use error::*;
pub use fanout::*;
pub use generic::{
    apply_search_hints, compare_search_results, empty_search_result_set, err_search_result_set, linear_result_cap,
    merge_search_result_sets, new_lazy_search_result_set, new_search_result_set_from_slice,
    new_search_result_set_from_slice_and_error, MIN_LINEAR_ALLOC_CAP,
};
pub use interface::*;
pub use memoized::*;
pub use merge::*;
pub use noop::*;
pub use secondary::ERR_SECONDARY_SEARCH_UNSUPPORTED;
pub use series::*;
