//! Port of `tsdb/chunks`: chunk references, `Meta`, block chunk segment files (Writer/Reader), head chunk files
//! (`chunks_head`, ChunkDiskMapper), the chunk write queue and its queue.

use chunkenc::Chunk;

/// Go `ChunkRef`: a generic reference for reading chunk data (a `HeadChunkRef` or a `BlockChunkRef`).
pub type ChunkRef = u64;

/// Go `HeadSeriesRef`: refers to an in-memory series.
pub type HeadSeriesRef = u64;

/// Go `HeadChunkID`: refers to a specific chunk in a series (memSeries) in the Head.
pub type HeadChunkId = u64;

/// Go `HeadChunkRef`: packs a HeadSeriesRef (5 bytes) and a HeadChunkID (3 bytes) into a global 8 byte ID.
pub type HeadChunkRef = u64;

/// Go `NewHeadChunkRef`.
pub fn new_head_chunk_ref(hsr: HeadSeriesRef, chunk_id: HeadChunkId) -> HeadChunkRef {
    if hsr > (1 << 40) - 1 {
        panic!("series ID exceeds 5 bytes");
    }
    if chunk_id > (1 << 24) - 1 {
        panic!("chunk ID exceeds 3 bytes");
    }
    (hsr << 24) | chunk_id
}

/// Go `HeadChunkRef.Unpack`.
pub fn unpack_head_chunk_ref(p: HeadChunkRef) -> (HeadSeriesRef, HeadChunkId) {
    (p >> 24, (p << 40) >> 40)
}

/// Go `BlockChunkRef`: the upper 4 bytes are the segment index, the lower 4 bytes the offset in the segment.
pub type BlockChunkRef = u64;

/// Go `NewBlockChunkRef`.
pub fn new_block_chunk_ref(file_index: u64, file_offset: u64) -> BlockChunkRef {
    (file_index << 32) | file_offset
}

/// Go `BlockChunkRef.Unpack`: (segment index, chunk start).
pub fn unpack_block_chunk_ref(b: BlockChunkRef) -> (usize, usize) {
    ((b >> 32) as usize, ((b << 32) >> 32) as usize)
}

/// Go `Meta`: information about one or more chunks. `chunk` holds the data, or is `None` and `ref` is used
/// to read it. When `max_time == i64::MAX` the chunk is still open.
#[derive(Debug, Default)]
pub struct Meta {
    pub r#ref: ChunkRef,
    pub chunk: Option<Box<dyn Chunk>>,
    /// Time range the data covers.
    pub min_time: i64,
    pub max_time: i64,
}

impl Meta {
    /// Go `OverlapsClosedInterval`: true if the chunk overlaps [mint, maxt].
    pub fn overlaps_closed_interval(&self, mint: i64, maxt: i64) -> bool {
        self.min_time <= maxt && mint <= self.max_time
    }
}
