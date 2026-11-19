//! Port of `tsdb/query_cost.go`: a queryable restricted to incremental native readers, for bounded query cost
//! sampling.
//!
//! Rust note: Go's `costSamplingQueryable` holds the `*DB` and checks the pinned queriers with a type assertion
//! (`queriers[0].(*blockChunkQuerier)`). Here the DB gives its pinned queriers as [`PinnedChunkQuerier`] values
//! (native block queriers are typed), through the function given to [`CostSamplingQueryable`]
//! (Go `db.blockChunkQuerierForRange`).

use storage::{BoxChunkQuerier, BoxQuerier, ChunkQueryable, Error, Queryable};

use crate::querier::{BlockBaseQuerier, BlockChunkQuerier, BlockQuerier};

/// A chunk querier pinned by the DB for a time range: a native block (or in-order head) chunk querier, or any
/// other kind (for example one that merges out-of-order data).
pub enum PinnedChunkQuerier {
    Native(BlockChunkQuerier),
    Other(BoxChunkQuerier),
}

impl PinnedChunkQuerier {
    pub(crate) fn close(&mut self) -> Option<Error> {
        use storage::LabelQuerier;
        match self {
            PinnedChunkQuerier::Native(q) => q.close(),
            PinnedChunkQuerier::Other(q) => q.close(),
        }
    }
}

/// Go `costSamplingQueryable`: `f` is Go `db.blockChunkQuerierForRange`.
pub struct CostSamplingQueryable<F>(pub F);

impl<F> CostSamplingQueryable<F>
where
    F: Fn(i64, i64) -> Result<Vec<PinnedChunkQuerier>, Error> + Send + Sync,
{
    /// Go `blockQuerier`: the single native querier of the range (`None` when there is no data), or
    /// `ERR_COST_SAMPLING_UNSUPPORTED` when the pinned layout is anything else.
    fn block_querier(&self, mint: i64, maxt: i64) -> Result<Option<BlockBaseQuerier>, Error> {
        let mut queriers = (self.0)(mint, maxt)?;
        if queriers.is_empty() {
            return Ok(None);
        }
        if queriers.len() == 1 && matches!(queriers[0], PinnedChunkQuerier::Native(_)) {
            // Transfer ownership of the pinned readers to the returned querier.
            if let Some(PinnedChunkQuerier::Native(q)) = queriers.pop() {
                return Ok(Some(q.0));
            }
        }
        // Check the actual pinned layout, rather than mutable configuration or block metadata. OOO iterables and
        // merged readers can encode a whole window eagerly.
        let errs: Vec<Option<Error>> = queriers.iter_mut().map(|q| q.close()).collect();
        if let Some(e) = Error::join(errs) {
            return Err(Error::wrap("close unsupported cost sampling readers", e));
        }
        Err(storage::ERR_COST_SAMPLING_UNSUPPORTED)
    }
}

impl<F> Queryable for CostSamplingQueryable<F>
where
    F: Fn(i64, i64) -> Result<Vec<PinnedChunkQuerier>, Error> + Send + Sync,
{
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        match self.block_querier(mint, maxt)? {
            None => Ok(storage::noop_querier()),
            Some(base) => Ok(Box::new(BlockQuerier(base))),
        }
    }
}

impl<F> ChunkQueryable for CostSamplingQueryable<F>
where
    F: Fn(i64, i64) -> Result<Vec<PinnedChunkQuerier>, Error> + Send + Sync,
{
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        match self.block_querier(mint, maxt)? {
            None => Ok(storage::noop_chunked_querier()),
            Some(base) => Ok(Box::new(BlockChunkQuerier(base))),
        }
    }
}
