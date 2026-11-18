//! The reader and writer interfaces of a block (port of the interfaces at the top of `tsdb/block.go`). The head,
//! the range head, persisted blocks and the out-of-order head readers implement them; the querier and the
//! compactor use them.
//!
//! Rust notes:
//! - Readers are owned values (`Box<dyn ...>`, `'static`), so a querier can keep them for its whole life. A reader
//!   that must keep its block open (Go's pending readers) holds what it needs (an `Arc`) and releases it in
//!   `close` (and on drop).
//! - Postings borrow the reader they came from (`BoxPostings<'_>`). Code that must outlive that borrow (a series
//!   set returned by a querier) expands the postings into a list first, or keeps the reader next to them.
//! - All errors are [`storage::Error`]; Go's error texts are kept.

use std::sync::Arc;

use chunkenc::{AnyIterator, Chunk};
use model::labels::{Labels, ScratchBuilder};
use model::matcher::Matcher;
use storage::{Context, Error, LabelHints, Meta, SeriesRef};
use tsdb_format::index::{BoxPostings, Postings, StringIter};
use tsdb_format::tombstones;

use crate::meta::BlockMeta;

/// Go `IndexWriter`: serializes the index for a block of series data. Methods must be called in this order.
pub trait IndexWriter: Send {
    /// Registers a single symbol. Symbols must be registered in sorted order.
    fn add_symbol(&mut self, sym: &str) -> Result<(), Error>;
    /// Adds a series and the references of its chunks. Series must be added in increasing label order.
    fn add_series(&mut self, r: SeriesRef, l: &Labels, chunks: &[Meta]) -> Result<(), Error>;
    /// Writes any finalization and closes the resources of the writer.
    fn close(&mut self) -> Result<(), Error>;
}

/// Go `IndexReader`: read access to the index of a block.
pub trait IndexReader: Send + Sync {
    /// An iterator over the sorted symbols that may occur in labels and indices.
    fn symbols(&self) -> Box<dyn StringIter + '_>;

    /// Sorted possible label values for `name`, of the series that match `matchers` (all with none).
    fn sorted_label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> Result<Vec<String>, Error>;

    /// Possible label values, which may not be sorted.
    fn label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> Result<Vec<String>, Error>;

    /// The postings of the series that have label `name` with any of `values`. Found IDs need not point to a
    /// valid series (for example during garbage collection).
    fn postings(&self, ctx: &Context, name: &str, values: &[&str]) -> Result<BoxPostings<'_>, Error>;

    /// Sorted postings of the series that have label `name` with a value for which `m` returns true. Empty if
    /// none match.
    fn postings_for_label_matching(
        &self,
        ctx: &Context,
        name: &str,
        m: &(dyn Fn(&str) -> bool + Sync),
    ) -> BoxPostings<'_>;

    /// Sorted postings of all series that have a label `name`.
    fn postings_for_all_label_values(&self, ctx: &Context, name: &str) -> BoxPostings<'_>;

    /// The postings reordered so that they are sorted by the label sets of the series.
    fn sorted_postings<'a>(&'a self, p: BoxPostings<'a>) -> BoxPostings<'a>;

    /// The postings of shard `shard_index` out of `shard_count`, by a stable hash of the series labels.
    fn sharded_postings<'a>(&'a self, p: BoxPostings<'a>, shard_index: u64, shard_count: u64) -> BoxPostings<'a>;

    /// Writes the labels of the series into `builder` and its chunk metas into `chks` (cleared first).
    /// `storage::ERR_NOT_FOUND` if the reference does not resolve to a known series.
    fn series(&self, r: SeriesRef, builder: &mut ScratchBuilder, chks: &mut Vec<Meta>) -> Result<(), Error>;

    /// All unique label names in sorted order, of the series that match `matchers` (all with none).
    fn label_names(&self, ctx: &Context, matchers: &[Matcher]) -> Result<Vec<String>, Error>;

    /// The sorted label names of the series referred to by the postings.
    fn label_names_for(&self, ctx: &Context, postings: &mut dyn Postings) -> Result<Vec<String>, Error>;

    /// Releases the resources of the reader.
    fn close(&self) -> Result<(), Error>;
}

/// Go `ChunkWriter`: serializes the chunks of a block.
pub trait ChunkWriter: Send {
    /// Writes the chunks (whose `chunk` must be set) and sets their `chunk_ref`.
    fn write_chunks(&mut self, chunks: &mut [Meta]) -> Result<(), Error>;
    /// Writes any finalization and closes the writer.
    fn close(&mut self) -> Result<(), Error>;
}

/// Go `chunkenc.Iterable`: something that gives a sample iterator.
pub trait Iterable: Send + Sync {
    fn iterator<'a>(&'a self, reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a>;
}

/// The result of [`ChunkReader::chunk_or_iterable`]: exactly one of the two is set.
pub enum ChunkOrIterable {
    Chunk(Arc<dyn Chunk>),
    Iterable(Box<dyn Iterable>),
}

/// Go `ChunkReader`: read access to the chunk data of a block.
pub trait ChunkReader: Send + Sync {
    /// The data for `meta`: a chunk when the meta maps to an existing chunk that needs no change, else an
    /// iterable (for example a subset of the samples of a head chunk, or the merge of out-of-order chunks).
    fn chunk_or_iterable(&self, meta: &Meta) -> Result<ChunkOrIterable, Error>;
    /// Go `ChunkReaderWithCopy.ChunkOrIterableWithCopy`: `Some` for readers that support it, with a copy of an
    /// open head chunk (or an iterable) and the real max time of the chunk.
    fn chunk_or_iterable_with_copy(&self, meta: &Meta) -> Option<Result<(ChunkOrIterable, i64), Error>> {
        let _ = meta;
        None
    }
    /// Go `chunkCacheEnabler.EnableChunkCache`.
    fn enable_chunk_cache(&self) {}
    /// Releases the resources of the reader.
    fn close(&self) -> Result<(), Error>;
}

/// Go `BlockReader`: read access to a data block (a persisted block, the head, a range of the head, ...).
pub trait BlockReader: Send + Sync {
    /// An index reader over the block's data.
    fn index(&self) -> Result<Box<dyn IndexReader>, Error>;
    /// A chunk reader over the block's data.
    fn chunks(&self) -> Result<Box<dyn ChunkReader>, Error>;
    /// The deletion intervals of the block.
    fn tombstones(&self) -> Result<Arc<dyn tombstones::Reader>, Error>;
    /// Meta information about the block.
    fn meta(&self) -> BlockMeta;
    /// The number of bytes the block takes on disk.
    fn size(&self) -> i64;
}
