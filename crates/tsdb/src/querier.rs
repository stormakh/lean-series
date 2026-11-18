//! Block querier, postings for matchers and series sets (port of `tsdb/querier.go`).
//!
//! API fixed by the lead (the head's out-of-order queriers and the DB use it); the bodies are filled by the
//! querier port.

use std::sync::Arc;

use model::matcher::Matcher;
use storage::{BoxChunkQuerier, BoxChunkSeriesSet, BoxQuerier, BoxSeriesSet, Context, Error, LabelHints, SelectHints};
use tsdb_format::index::BoxPostings;
use tsdb_format::tombstones;

use crate::meta::Ulid;
use crate::reader::{BlockReader, ChunkReader, IndexReader};

/// Go `NewBlockQuerier`: a querier over [mint, maxt] of the block. Takes the readers of `b` (they are owned by
/// the querier and closed with it).
pub fn new_block_querier(b: &dyn BlockReader, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
    let _ = (b, mint, maxt);
    todo!("querier port")
}

/// Go `NewBlockChunkQuerier`.
pub fn new_block_chunk_querier(b: &dyn BlockReader, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
    let _ = (b, mint, maxt);
    todo!("querier port")
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
    mint: i64,
    maxt: i64,
) -> BoxSeriesSet {
    let _ = (ctx, sort_series, hints, ms, index, chunks, tombstones, mint, maxt);
    todo!("querier port")
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
    mint: i64,
    maxt: i64,
) -> BoxChunkSeriesSet {
    let _ = (ctx, sort_series, hints, ms, block_id, index, chunks, tombstones, mint, maxt);
    todo!("querier port")
}

/// Go `PostingsForMatchers`: the postings of the series that match all matchers, with every upstream
/// optimization.
pub fn postings_for_matchers<'a>(
    ctx: &Context,
    ix: &'a dyn IndexReader,
    ms: &[Matcher],
) -> Result<BoxPostings<'a>, Error> {
    let _ = (ctx, ix, ms);
    todo!("querier port")
}

/// Go `labelValuesWithMatchers`.
pub fn label_values_with_matchers(
    ctx: &Context,
    r: &dyn IndexReader,
    name: &str,
    hints: Option<&LabelHints>,
    matchers: &[Matcher],
) -> Result<Vec<String>, Error> {
    let _ = (ctx, r, name, hints, matchers);
    todo!("querier port")
}

/// Go `labelNamesWithMatchers`.
pub fn label_names_with_matchers(ctx: &Context, r: &dyn IndexReader, matchers: &[Matcher]) -> Result<Vec<String>, Error> {
    let _ = (ctx, r, matchers);
    todo!("querier port")
}
